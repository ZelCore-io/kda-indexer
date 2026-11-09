//! `measure` command: record sizes and compression variants (spec §3.10 row 1, B7, B12).
//!
//! Input is one or two `--records-out` files from `dump`. Every size is computed on the real
//! record bytes; nothing is estimated except where a line says so.
//!
//! Method notes (also printed with the table):
//! - B/tx is bytes per USER tx. Blocked layouts hold coinbase and user records in TxKey order.
//!   Per block, the coinbase records are priced as a block of their own and the user records get
//!   the remainder (marginal cost given the coinbase context); the user part is split between
//!   legacy and non-legacy txs by raw record bytes.
//! - "in-sample" trains the dictionary on up to 20,000 records of the same input (the plan's
//!   definition; optimistic on small inputs). "hold-out" trains on half of the input (alternate
//!   256-record folds) and compresses the other half, then sums both halves (pessimistic: half
//!   the training data). The truth for a production dictionary lies between the two.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use kdx_types::record::{decode_block, decode_tx, encode_tx};
use rayon::prelude::*;
use serde_json::{json, Value};
use zstd::bulk::Compressor;

use super::frames::{read_frames, KIND_BLOCK, KIND_COINBASE, KIND_USER, KIND_USER_LEGACY};

const DICT_SIZES: [usize; 4] = [64 * 1024, 32 * 1024, 16 * 1024, 8 * 1024];
const MAX_TRAIN: usize = 20_000;
const FOLD: usize = 256;
/// §3.10 row 1 band, B/tx.
const ROW1_LO: f64 = 497.0;
const ROW1_HI: f64 = 625.0;
const USER_TXS_MAINNET: f64 = 200.8e6;
const ROW1_GB: (f64, f64) = (99.8, 125.5);
const TOTAL_GB_HIGH: f64 = 330.5;
const BLOCKS_MAINNET: f64 = 139.3e6;

pub struct MeasureArgs {
    pub records: PathBuf,
    pub records_b: Option<PathBuf>,
    pub out: PathBuf,
}

struct Rec {
    kind: u8,
    bytes: Vec<u8>,
}

fn class(kind: u8) -> usize {
    match kind {
        KIND_COINBASE => 0,
        KIND_USER => 1,
        _ => 2,
    }
}

#[derive(Clone, Copy, Default, Debug)]
struct Sizes {
    /// Compressed (or raw) bytes attributed to coinbase, user, user-legacy.
    by_class: [f64; 3],
}

impl Sizes {
    fn total(&self) -> f64 {
        self.by_class.iter().sum()
    }
    fn user(&self) -> f64 {
        self.by_class[1] + self.by_class[2]
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Lay {
    Raw,
    PerRecord,
    Blocks(usize),
}

enum Dicts {
    None,
    InSample,
    HoldOut,
    Fixed(Vec<u8>),
}

/// Train a dictionary on up to `MAX_TRAIN` records sampled uniformly; falls back to smaller
/// dictionary sizes when the input is too small for 64 KB.
fn train(samples: &[&[u8]]) -> Option<Vec<u8>> {
    if samples.len() < 32 {
        return None;
    }
    let step = (samples.len() as f64 / MAX_TRAIN as f64).max(1.0);
    let picked: Vec<&[u8]> = (0..samples.len().min(MAX_TRAIN))
        .map(|i| samples[(i as f64 * step) as usize])
        .collect();
    DICT_SIZES
        .iter()
        .find_map(|&sz| zstd::dict::from_samples(&picked, sz).ok())
}

fn make_comp(level: i32, dict: &Option<Vec<u8>>) -> Compressor<'static> {
    match dict {
        Some(d) => Compressor::with_dictionary(level, d),
        None => Compressor::new(level),
    }
    .expect("zstd compressor")
}

/// Sizes of `recs` under a layout. The dictionary (if any) is trained on `recs` itself.
fn eval(recs: &[Rec], lay: Lay, level: i32, dicts: &Dicts) -> Sizes {
    let mut s = Sizes::default();
    if lay == Lay::Raw {
        for r in recs {
            s.by_class[class(r.kind)] += r.bytes.len() as f64;
        }
        return s;
    }
    let all: Vec<&[u8]> = recs.iter().map(|r| r.bytes.as_slice()).collect();
    // dict_for[f] = dictionary used to compress units of fold f.
    let dict_for: [Option<Vec<u8>>; 2] = match dicts {
        Dicts::None => [None, None],
        Dicts::InSample => {
            let d = train(&all);
            [d.clone(), d]
        }
        Dicts::Fixed(d) => [Some(d.clone()), Some(d.clone())],
        Dicts::HoldOut => {
            let part = |f: usize| -> Vec<&[u8]> {
                recs.iter()
                    .enumerate()
                    .filter(|(i, _)| (i / FOLD) % 2 == f)
                    .map(|(_, r)| r.bytes.as_slice())
                    .collect()
            };
            // Units of fold 0 are compressed with the dictionary trained on fold 1.
            [train(&part(1)), train(&part(0))]
        }
    };
    let n = match lay {
        Lay::PerRecord => 1,
        Lay::Blocks(n) => n,
        Lay::Raw => unreachable!(),
    };
    let units: Vec<&[Rec]> = recs.chunks(n).collect();
    let parts: Vec<[f64; 3]> = units
        .par_iter()
        .enumerate()
        .map_init(
            || {
                [
                    make_comp(level, &dict_for[0]),
                    make_comp(level, &dict_for[1]),
                ]
            },
            |comps, (ui, unit)| {
                let fold = (ui * n / FOLD) % 2;
                let pack = |sel: &dyn Fn(&Rec) -> bool| {
                    let mut buf = Vec::new();
                    if let Lay::Blocks(_) = lay {
                        let mut off = 0u32;
                        for r in unit.iter().filter(|r| sel(r)) {
                            buf.extend_from_slice(&off.to_le_bytes());
                            off += r.bytes.len() as u32;
                        }
                    }
                    for r in unit.iter().filter(|r| sel(r)) {
                        buf.extend_from_slice(&r.bytes);
                    }
                    buf
                };
                let mut raw = [0f64; 3];
                for r in unit.iter() {
                    raw[class(r.kind)] += r.bytes.len() as f64;
                }
                let all = comps[fold]
                    .compress(&pack(&|_| true))
                    .expect("zstd compress")
                    .len() as f64;
                if n == 1 {
                    let mut out = [0f64; 3];
                    out[class(unit[0].kind)] = all;
                    return out;
                }
                // Marginal attribution: the coinbase records are priced on their own, the
                // user records get the rest of the block (their cost given the coinbase
                // context), split between the two user classes by raw bytes.
                let cb = if raw[0] > 0.0 {
                    comps[fold]
                        .compress(&pack(&|r| r.kind == KIND_COINBASE))
                        .expect("zstd compress")
                        .len() as f64
                } else {
                    0.0
                };
                let user = (all - cb).max(0.0);
                let ru = raw[1] + raw[2];
                if ru == 0.0 {
                    return [all, 0.0, 0.0];
                }
                [cb, user * raw[1] / ru, user * raw[2] / ru]
            },
        )
        .collect();
    for p in parts {
        for (acc, v) in s.by_class.iter_mut().zip(p) {
            *acc += v;
        }
    }
    s
}

struct Variant {
    name: &'static str,
    lay: Lay,
    level: i32,
    dict: bool,
}

const VARIANTS: [Variant; 7] = [
    Variant {
        name: "raw",
        lay: Lay::Raw,
        level: 0,
        dict: false,
    },
    Variant {
        name: "zstd-19 per record",
        lay: Lay::PerRecord,
        level: 19,
        dict: false,
    },
    Variant {
        name: "zstd-19 blocks of 64",
        lay: Lay::Blocks(64),
        level: 19,
        dict: false,
    },
    Variant {
        name: "dict64K per record (19)",
        lay: Lay::PerRecord,
        level: 19,
        dict: true,
    },
    Variant {
        name: "dict64K blocks of 64 (19) [chosen]",
        lay: Lay::Blocks(64),
        level: 19,
        dict: true,
    },
    Variant {
        name: "dict64K blocks of 64 (9)",
        lay: Lay::Blocks(64),
        level: 9,
        dict: true,
    },
    Variant {
        name: "dict64K blocks of 128 (19)",
        lay: Lay::Blocks(128),
        level: 19,
        dict: true,
    },
];
const CHOSEN: usize = 4;

fn per(x: f64, n: usize) -> f64 {
    if n == 0 {
        0.0
    } else {
        x / n as f64
    }
}

fn r1(x: f64) -> f64 {
    (x * 10.0).round() / 10.0
}

struct Loaded {
    name: String,
    blocks: Vec<Rec>,
    txs: Vec<Rec>,
    n_user: usize,
    n_legacy: usize,
    n_coinbase: usize,
    /// chain -> (user txs, legacy)
    by_chain: BTreeMap<u8, (usize, usize)>,
    /// 100k band -> (user txs, legacy)
    by_band: BTreeMap<u64, (usize, usize)>,
    avg_adjacents: f64,
}

fn load(path: &Path) -> anyhow::Result<Loaded> {
    let mut l = Loaded {
        name: path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default(),
        blocks: vec![],
        txs: vec![],
        n_user: 0,
        n_legacy: 0,
        n_coinbase: 0,
        by_chain: BTreeMap::new(),
        by_band: BTreeMap::new(),
        avg_adjacents: 0.0,
    };
    let mut adj = 0usize;
    for (kind, bytes) in read_frames(path)? {
        match kind {
            KIND_BLOCK => {
                adj += decode_block(&bytes)
                    .context("block record")?
                    .adjacents
                    .len();
                l.blocks.push(Rec { kind, bytes });
            }
            KIND_COINBASE => {
                l.n_coinbase += 1;
                l.txs.push(Rec { kind, bytes });
            }
            KIND_USER | KIND_USER_LEGACY => {
                let r = decode_tx(&bytes).context("tx record")?;
                l.n_user += 1;
                let legacy = kind == KIND_USER_LEGACY;
                l.n_legacy += legacy as usize;
                let c = l.by_chain.entry(r.key.chain()).or_default();
                c.0 += 1;
                c.1 += legacy as usize;
                let b = l
                    .by_band
                    .entry(r.key.height() as u64 / 100_000 * 100_000)
                    .or_default();
                b.0 += 1;
                b.1 += legacy as usize;
                l.txs.push(Rec { kind, bytes });
            }
            k => bail!("{}: unknown frame kind {k}", path.display()),
        }
    }
    l.avg_adjacents = per(adj as f64, l.blocks.len());
    if l.n_user == 0 {
        bail!("{}: no user tx records", path.display());
    }
    Ok(l)
}

/// Copy of the tx stream with `cmd` removed from every user record (B12).
fn without_cmd(txs: &[Rec]) -> anyhow::Result<Vec<Rec>> {
    txs.iter()
        .map(|r| {
            if r.kind == KIND_COINBASE {
                return Ok(Rec {
                    kind: r.kind,
                    bytes: r.bytes.clone(),
                });
            }
            let mut t = decode_tx(&r.bytes)?;
            t.cmd = None;
            let mut b = Vec::new();
            encode_tx(&t, &mut b);
            Ok(Rec {
                kind: r.kind,
                bytes: b,
            })
        })
        .collect()
}

fn pass(bpt: f64) -> bool {
    (ROW1_LO * 0.7..=ROW1_HI * 1.3).contains(&bpt)
}

fn analyze(l: &Loaded, baseline: f64, text: &mut String) -> anyhow::Result<Value> {
    use std::fmt::Write as _;
    let nu = l.n_user;
    let chosen = &VARIANTS[CHOSEN];
    let mut variants = vec![];
    let _ = writeln!(text, "\n== {} ==", l.name);
    let _ = writeln!(
        text,
        "blocks {}  coinbase {}  user txs {}  legacy-cmd user txs {} ({:.1}%)",
        l.blocks.len(),
        l.n_coinbase,
        nu,
        l.n_legacy,
        100.0 * l.n_legacy as f64 / nu as f64
    );
    let _ = writeln!(
        text,
        "{:<38} {:>12} {:>12}   (user B/tx)",
        "variant", "in-sample", "hold-out"
    );
    let mut chosen_in = Sizes::default();
    let mut chosen_ho = Sizes::default();
    for (i, v) in VARIANTS.iter().enumerate() {
        let (a, b) = if v.dict {
            (
                eval(&l.txs, v.lay, v.level, &Dicts::InSample),
                eval(&l.txs, v.lay, v.level, &Dicts::HoldOut),
            )
        } else {
            let s = eval(&l.txs, v.lay, v.level, &Dicts::None);
            (s, s)
        };
        if i == CHOSEN {
            chosen_in = a;
            chosen_ho = b;
        }
        let _ = writeln!(
            text,
            "{:<38} {:>12.1} {:>12.1}",
            v.name,
            per(a.user(), nu),
            per(b.user(), nu)
        );
        variants.push(json!({
            "name": v.name,
            "user_b_per_tx_in_sample": r1(per(a.user(), nu)),
            "user_b_per_tx_hold_out": r1(per(b.user(), nu)),
            "coinbase_b_per_record_in_sample": r1(per(a.by_class[0], l.n_coinbase)),
        }));
    }

    // Legacy vs rest, chosen layout.
    let n_ok = nu - l.n_legacy;
    let split = |s: &Sizes| (per(s.by_class[1], n_ok), per(s.by_class[2], l.n_legacy));
    let (ok_in, leg_in) = split(&chosen_in);
    let (ok_ho, leg_ho) = split(&chosen_ho);
    let _ = writeln!(
        text,
        "chosen layout, B/tx by class: non-legacy {ok_in:.1} / {ok_ho:.1} (n {n_ok}), legacy cmd {leg_in:.1} / {leg_ho:.1} (n {})  [in-sample / hold-out]",
        l.n_legacy
    );

    // B12: same layout without cmd.
    let nocmd = without_cmd(&l.txs)?;
    let nc_in = eval(&nocmd, chosen.lay, chosen.level, &Dicts::InSample);
    let nc_ho = eval(&nocmd, chosen.lay, chosen.level, &Dicts::HoldOut);
    let ov = |full: f64, nc: f64| full - nc;
    let ov_in = ov(per(chosen_in.user(), nu), per(nc_in.user(), nu));
    let ov_ho = ov(per(chosen_ho.user(), nu), per(nc_ho.user(), nu));
    let _ = writeln!(
        text,
        "B12 cmd overhead (full - no-cmd): {ov_in:.1} B/tx in-sample, {ov_ho:.1} hold-out; no-cmd B/tx {:.1} / {:.1}",
        per(nc_in.user(), nu),
        per(nc_ho.user(), nu)
    );
    let _ = writeln!(
        text,
        "B12 cmd overhead by class (in-sample): non-legacy {:.1}, legacy {:.1}",
        per(chosen_in.by_class[1] - nc_in.by_class[1], n_ok),
        per(chosen_in.by_class[2] - nc_in.by_class[2], l.n_legacy)
    );
    let vs = per(chosen_in.user(), nu) - baseline;
    let vs_ho = per(chosen_ho.user(), nu) - baseline;
    let _ = writeln!(
        text,
        "versus study baseline {baseline:.0} B (no-cmd record): full B/tx - baseline = {vs:.1} in-sample, {vs_ho:.1} hold-out (estimate was 80-150)"
    );

    // Exit check against row 1.
    let bpt_in = per(chosen_in.user(), nu);
    let bpt_ho = per(chosen_ho.user(), nu);
    let verdict = |b: f64| if pass(b) { "PASS" } else { "FAIL" };
    let _ = writeln!(
        text,
        "EXIT CHECK row 1 (band {ROW1_LO}-{ROW1_HI} B, +-30% = {:.0}-{:.0}): chosen {bpt_in:.1} B/tx {} (in-sample); {bpt_ho:.1} {} (hold-out)",
        ROW1_LO * 0.7,
        ROW1_HI * 1.3,
        verdict(bpt_in),
        verdict(bpt_ho)
    );
    let gb = |b: f64| b * USER_TXS_MAINNET / 1e9;
    let adj_high = |b: f64| TOTAL_GB_HIGH + 1.25 * (gb(b) - ROW1_GB.1);
    let flag = adj_high(bpt_in).max(adj_high(bpt_ho)) > 400.0;
    let _ = writeln!(
        text,
        "extrapolation: {bpt_in:.1} B x 200.8M = {:.1} GB in-sample, {:.1} GB hold-out (row 1: {}-{} GB); §3.10 high end with this row 1: {:.1} / {:.1} GB{}",
        gb(bpt_in),
        gb(bpt_ho),
        ROW1_GB.0,
        ROW1_GB.1,
        adj_high(bpt_in),
        adj_high(bpt_ho),
        if flag { "  -> REPORT TO OWNER (passes ~400 GB)" } else { "" }
    );

    // Block records + attributed coinbase vs row 2.
    let blk = |d: Dicts| eval(&l.blocks, Lay::Blocks(64), 19, &d).total();
    let (bin, bho) = (blk(Dicts::InSample), blk(Dicts::HoldOut));
    let nb = l.blocks.len();
    let cb_in = chosen_in.by_class[0];
    let cb_ho = chosen_ho.by_class[0];
    let per_block_in = per(bin + cb_in, nb);
    let per_block_ho = per(bho + cb_ho, nb);
    let adj_saving = l.avg_adjacents * 27.0;
    let _ = writeln!(
        text,
        "B/block (block record + coinbase, dict blocks of 64): {per_block_in:.1} in-sample / {per_block_ho:.1} hold-out (row 2 band 150-250; adjacents stored as full hashes here: est. {:.1} / {:.1} with 5-byte references, avg {:.1} adjacents)",
        per_block_in - adj_saving,
        per_block_ho - adj_saving,
        l.avg_adjacents
    );

    // Legacy share.
    let _ = writeln!(text, "legacy cmd hash mismatch share per chain (user txs):");
    let mut chains = vec![];
    for (c, (n, lg)) in &l.by_chain {
        let _ = writeln!(
            text,
            "  chain {c:>2}: {lg} of {n} ({:.1}%)",
            100.0 * *lg as f64 / *n as f64
        );
        chains.push(json!({"chain": c, "user_txs": n, "legacy": lg}));
    }
    let mut bands = vec![];
    for (b, (n, lg)) in &l.by_band {
        let _ = writeln!(
            text,
            "  band {b:>9}: {lg} of {n} ({:.1}%)",
            100.0 * *lg as f64 / *n as f64
        );
        bands.push(json!({"band": b, "user_txs": n, "legacy": lg}));
    }

    Ok(json!({
        "name": l.name,
        "blocks": nb,
        "coinbase_records": l.n_coinbase,
        "user_txs": nu,
        "legacy_user_txs": l.n_legacy,
        "legacy_share": r1(100.0 * l.n_legacy as f64 / nu as f64),
        "variants": variants,
        "chosen": chosen.name,
        "chosen_user_b_per_tx": {"in_sample": r1(bpt_in), "hold_out": r1(bpt_ho)},
        "chosen_by_class_b_per_tx": {
            "non_legacy": {"in_sample": r1(ok_in), "hold_out": r1(ok_ho), "n": n_ok},
            "legacy_cmd": {"in_sample": r1(leg_in), "hold_out": r1(leg_ho), "n": l.n_legacy},
        },
        "b12_cmd_overhead_b_per_tx": {
            "in_sample": r1(ov_in), "hold_out": r1(ov_ho),
            "no_cmd_in_sample": r1(per(nc_in.user(), nu)),
            "non_legacy_in_sample": r1(per(chosen_in.by_class[1] - nc_in.by_class[1], n_ok)),
            "legacy_in_sample": r1(per(chosen_in.by_class[2] - nc_in.by_class[2], l.n_legacy)),
        },
        "vs_study_baseline": {"baseline": baseline, "in_sample": r1(vs), "hold_out": r1(vs_ho)},
        "exit_check": {
            "band": [ROW1_LO, ROW1_HI], "tolerance_range": [ROW1_LO * 0.7, ROW1_HI * 1.3],
            "in_sample": verdict(bpt_in), "hold_out": verdict(bpt_ho),
        },
        "extrapolation_gb": {
            "in_sample": r1(gb(bpt_in)), "hold_out": r1(gb(bpt_ho)),
            "high_end_total_in_sample": r1(adj_high(bpt_in)), "high_end_total_hold_out": r1(adj_high(bpt_ho)),
            "report_to_owner": flag,
        },
        "b_per_block": {
            "in_sample": r1(per_block_in), "hold_out": r1(per_block_ho),
            "est_with_5b_adjacent_refs_in_sample": r1(per_block_in - adj_saving),
            "block_record_total_blocks_extrapolation_gb_in_sample": r1(per_block_in * BLOCKS_MAINNET / 1e9),
            "avg_adjacents": r1(l.avg_adjacents),
        },
        "legacy_by_chain": chains,
        "legacy_by_band": bands,
    }))
}

pub fn run(args: MeasureArgs) -> anyhow::Result<()> {
    let a = load(&args.records)?;
    let b = args.records_b.as_deref().map(load).transpose()?;
    let mut text = String::new();
    text.push_str(
        "Method: B/tx = bytes per user tx; in blocked layouts coinbase records are priced as their own block and user records get the remainder; \
in-sample = dictionary trained on the same input (<=20,000 records), hold-out = trained on alternate 256-record folds, applied to the other half.\n",
    );
    let ra = analyze(&a, 475.0, &mut text)?;
    let rb = match &b {
        Some(b) => Some(analyze(b, 417.0, &mut text)?),
        None => None,
    };

    // B7 cross-era dictionary.
    let mut cross = Value::Null;
    if let Some(b) = &b {
        use std::fmt::Write as _;
        let chosen = &VARIANTS[CHOSEN];
        let train_on =
            |l: &Loaded| train(&l.txs.iter().map(|r| r.bytes.as_slice()).collect::<Vec<_>>());
        let (da, db) = (train_on(&a), train_on(b));
        let bpt = |l: &Loaded, d: &Option<Vec<u8>>| match d {
            Some(d) => per(
                eval(&l.txs, chosen.lay, chosen.level, &Dicts::Fixed(d.clone())).user(),
                l.n_user,
            ),
            None => f64::NAN,
        };
        let own = |l: &Loaded| {
            (
                per(
                    eval(&l.txs, chosen.lay, chosen.level, &Dicts::InSample).user(),
                    l.n_user,
                ),
                per(
                    eval(&l.txs, chosen.lay, chosen.level, &Dicts::HoldOut).user(),
                    l.n_user,
                ),
            )
        };
        let (a_own, b_own) = (own(&a), own(b));
        let b_with_a = bpt(b, &da);
        let a_with_b = bpt(&a, &db);
        let _ = writeln!(
            text,
            "\n== B7 cross-era dictionary (chosen layout, user B/tx) =="
        );
        let _ = writeln!(
            text,
            "{} with dictionary trained on {}: {b_with_a:.1}  vs own in-sample {:.1} (delta {:+.1}), own hold-out {:.1} (delta {:+.1})",
            b.name, a.name, b_own.0, b_with_a - b_own.0, b_own.1, b_with_a - b_own.1
        );
        let _ = writeln!(
            text,
            "{} with dictionary trained on {}: {a_with_b:.1}  vs own in-sample {:.1} (delta {:+.1}), own hold-out {:.1} (delta {:+.1})",
            a.name, b.name, a_own.0, a_with_b - a_own.0, a_own.1, a_with_b - a_own.1
        );
        cross = json!({
            "b_with_a_dict": {"b_per_tx": r1(b_with_a), "own_in_sample": r1(b_own.0), "own_hold_out": r1(b_own.1)},
            "a_with_b_dict": {"b_per_tx": r1(a_with_b), "own_in_sample": r1(a_own.0), "own_hold_out": r1(a_own.1)},
        });
    }
    print!("{text}");
    if let Some(d) = args.out.parent() {
        std::fs::create_dir_all(d)?;
    }
    let report = json!({"records": ra, "records_b": rb, "cross_era_dictionary": cross});
    std::fs::write(&args.out, serde_json::to_vec_pretty(&report)?)?;
    eprintln!("report written to {}", args.out.display());
    Ok(())
}
