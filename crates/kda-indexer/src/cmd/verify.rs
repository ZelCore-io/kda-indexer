//! `verify merkle` (B8, read-only): recompute the payload Merkle roots of stored blocks from
//! their records (leaves re-rendered through `render.rs`, so the storage round trip is
//! checked too) and compare them with the stored `transactionsHash`, `outputsHash` and
//! `payloadHash` and with the record's `merkle_verified` bit.
//!
//! Classes: `ok` (verified, bit set), `ok_unflagged` (verified, bit false), `bad_flagged`
//! (mismatch, bit set: corruption), `bad_legacy` / `bad_other` (mismatch, bit false; legacy =
//! some tx has `cmd_reencoded`), `errors` (a record that does not render). Exit 1 on any
//! `bad_flagged`, `bad_other` or `errors`.
//!
//! The hot store is opened read-only and a fresh read txn is taken per chunk of heights (or of
//! samples), never one for the whole walk. Blocks come from the hot store first, then from the
//! frozen segments (as `inspect`).

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use kdx_decode::merkle::check_records;
use kdx_decode::MerkleResult;
use kdx_hot::tables::block_key;
use kdx_hot::{HotDict, HotStore, RoTxn, Stage, Table, Txn};
use kdx_segment::{Segments, SLOT_WIDTH};
use kdx_types::record::{decode_block, decode_tx, BlockRecord, TxRecord};
use kdx_types::{MemDict, TxKey};
use rand::RngExt;
use serde::Serialize;

use super::hot::hot_dir;
use super::segments::segments_dir;

/// Heights per read txn (`--all`).
const CHUNK_HEIGHTS: u32 = 1_000;
/// Samples per read txn (`--sample`).
const CHUNK_SAMPLES: usize = 1_000;
/// Bad blocks listed in the output.
const LIST_MAX: usize = 100;

pub struct MerkleArgs {
    pub data_dir: PathBuf,
    pub from: Option<u32>,
    pub to: Option<u32>,
    pub chains: Option<Vec<u8>>,
    /// `None` with `all = false` means the default sample.
    pub sample: Option<u64>,
    pub all: bool,
    pub json: bool,
}

pub const DEFAULT_SAMPLE: u64 = 1_000;

#[derive(Default, Serialize)]
struct Tally {
    from: u32,
    to: u32,
    mode: String,
    blocks: u64,
    ok: u64,
    ok_unflagged: u64,
    bad_flagged: u64,
    bad_legacy: u64,
    bad_other: u64,
    errors: u64,
    /// (chain, height) pairs with no stored block (chain not yet started, outside the store).
    absent: u64,
    hot: u64,
    frozen: u64,
    /// First `LIST_MAX` blocks of the bad classes and errors.
    bad: Vec<Bad>,
}

#[derive(Serialize)]
struct Bad {
    chain: u8,
    height: u32,
    class: &'static str,
    detail: String,
}

impl Tally {
    fn failed(&self) -> bool {
        self.bad_flagged + self.bad_other + self.errors > 0
    }

    fn note(&mut self, chain: u8, height: u32, class: &'static str, detail: String) {
        if self.bad.len() < LIST_MAX {
            self.bad.push(Bad {
                chain,
                height,
                class,
                detail,
            });
        }
    }
}

struct Ctx {
    hs: HotStore,
    seg: Segments,
    dict: Arc<MemDict>,
    chains: Vec<u8>,
}

impl Ctx {
    fn reload_dict(&mut self) -> anyhow::Result<()> {
        let txn = self.hs.ro()?;
        self.dict = Arc::new(HotDict::load(&txn)?.mem().clone());
        Ok(())
    }
}

/// The block and its tx records (coinbase first): hot first, then frozen.
fn load(
    txn: &RoTxn,
    seg: &Segments,
    chain: u8,
    height: u32,
) -> anyhow::Result<Option<(BlockRecord, Vec<TxRecord>, &'static str)>> {
    if let Some(v) = txn.get(Table::Headers, &block_key(chain, height))? {
        let b = decode_block(&v).context("hot block record")?;
        let start = TxKey::new(height, chain, 0).to_be_bytes();
        let rows = txn.scan(Table::TailTxs, &start, |k| k.get(..5) == Some(&start[..5]))?;
        let txs = rows
            .iter()
            .map(|(_, v)| decode_tx(v).context("hot tx record"))
            .collect::<anyhow::Result<Vec<_>>>()?;
        return Ok(Some((b, txs, "hot")));
    }
    let Some(b) = seg.block(chain, height)? else {
        return Ok(None);
    };
    let range = seg
        .block_txnums(chain, height)
        .with_context(|| format!("chain {chain} height {height}: frozen block without txs"))?;
    let mut txs = Vec::with_capacity((range.end - range.start) as usize);
    for t in range {
        txs.push(seg.tx(t)?.with_context(|| format!("txnum {t} missing"))?);
    }
    Ok(Some((b, txs, "frozen")))
}

fn check_one(cx: &mut Ctx, txn: &RoTxn, t: &mut Tally, chain: u8, height: u32) {
    let (b, txs, src) = match load(txn, &cx.seg, chain, height) {
        Ok(Some(x)) => x,
        Ok(None) => {
            t.absent += 1;
            return;
        }
        Err(e) => {
            t.blocks += 1;
            t.errors += 1;
            t.note(chain, height, "error", format!("{e:#}"));
            return;
        }
    };
    t.blocks += 1;
    if src == "hot" {
        t.hot += 1;
    } else {
        t.frozen += 1;
    }
    let mut r = check_records(&b, &txs, cx.dict.as_ref());
    if r.is_err() {
        // A name interned after the dictionary was loaded (tail moved on): reload once.
        if cx.reload_dict().is_ok() {
            r = check_records(&b, &txs, cx.dict.as_ref());
        }
    }
    let flagged = b.merkle_verified;
    match r {
        Ok(MerkleResult::Verified) if flagged => t.ok += 1,
        Ok(MerkleResult::Verified) => t.ok_unflagged += 1,
        Ok(MerkleResult::Mismatch { which, legacy }) => {
            let class = if flagged {
                t.bad_flagged += 1;
                "bad_flagged"
            } else if legacy {
                t.bad_legacy += 1;
                "bad_legacy"
            } else {
                t.bad_other += 1;
                "bad_other"
            };
            t.note(
                chain,
                height,
                class,
                format!("{} ({src}, hash {})", which.as_str(), b.hash),
            );
        }
        Err(e) => {
            t.errors += 1;
            t.note(chain, height, "error", format!("render ({src}): {e}"));
        }
    }
}

/// Lowest and highest stored heights over `chains` (hot `Headers` and the segments).
fn bounds(txn: &RoTxn, seg: &Segments, chains: &[u8]) -> anyhow::Result<Option<(u32, u32)>> {
    let mut lo: Option<u32> = seg.manifest.entries.first().map(|e| e.start);
    let mut hi: Option<u32> = seg.frontier().checked_sub(1).filter(|_| lo.is_some());
    for &c in chains {
        txn.walk_keys(Table::Headers, &[c], |k| {
            if k.first() == Some(&c) {
                if let Some(h) = k.get(1..5).and_then(|b| b.try_into().ok()) {
                    let h = u32::from_be_bytes(h);
                    lo = Some(lo.map_or(h, |l| l.min(h)));
                }
            }
            false
        })?;
        if let Some((h, _)) = kdx_hot::checkpoint(txn, Stage::Tail, c)? {
            hi = Some(hi.map_or(h, |x| x.max(h)));
        }
    }
    Ok(lo.zip(hi))
}

pub fn merkle(args: MerkleArgs) -> anyhow::Result<()> {
    let data_dir = std::fs::canonicalize(&args.data_dir)
        .with_context(|| format!("data dir {}", args.data_dir.display()))?;
    let hs = HotStore::open_ro(&hot_dir(&data_dir))?;
    let chains: Vec<u8> = match &args.chains {
        Some(c) => c.clone(),
        None => (0..SLOT_WIDTH as u8).collect(),
    };
    let (dict, stored) = {
        let txn = hs.ro()?;
        let dict = Arc::new(HotDict::load(&txn)?.mem().clone());
        let seg = Segments::open(&segments_dir(&data_dir), dict.clone())?;
        let b = bounds(&txn, &seg, &chains)?;
        (dict, b)
    };
    let seg = Segments::open(&segments_dir(&data_dir), dict.clone())?;
    let Some((slo, shi)) = stored else {
        anyhow::bail!("no stored blocks in {}", data_dir.display());
    };
    let from = args.from.unwrap_or(slo).max(slo);
    let to = args.to.unwrap_or(shi).min(shi);
    anyhow::ensure!(from <= to, "empty height range {from}..={to}");
    let mut cx = Ctx {
        hs,
        seg,
        dict,
        chains,
    };
    let mut t = Tally {
        from,
        to,
        ..Default::default()
    };
    if args.all {
        t.mode = "all".into();
        let mut a = from;
        loop {
            let b = a.saturating_add(CHUNK_HEIGHTS - 1).min(to);
            let txn = cx.hs.ro()?;
            for h in a..=b {
                for i in 0..cx.chains.len() {
                    let c = cx.chains[i];
                    check_one(&mut cx, &txn, &mut t, c, h);
                }
            }
            drop(txn);
            if b == to {
                break;
            }
            a = b + 1;
        }
    } else {
        let n = args.sample.unwrap_or(DEFAULT_SAMPLE);
        t.mode = format!("sample {n}");
        let mut rng = rand::rng();
        let mut picks: Vec<(u32, u8)> = (0..n)
            .map(|_| {
                (
                    rng.random_range(from..=to),
                    cx.chains[rng.random_range(0..cx.chains.len())],
                )
            })
            .collect();
        picks.sort_unstable();
        for chunk in picks.chunks(CHUNK_SAMPLES) {
            let txn = cx.hs.ro()?;
            for &(h, c) in chunk {
                check_one(&mut cx, &txn, &mut t, c, h);
            }
        }
    }
    report(&t, args.json)?;
    if t.failed() {
        anyhow::bail!(
            "verify merkle: {} bad_flagged, {} bad_other, {} errors",
            t.bad_flagged,
            t.bad_other,
            t.errors
        );
    }
    Ok(())
}

fn report(t: &Tally, json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(t)?);
        return Ok(());
    }
    println!("heights       {}..={} ({})", t.from, t.to, t.mode);
    println!(
        "blocks        {} (hot {}, frozen {}; absent {})",
        t.blocks, t.hot, t.frozen, t.absent
    );
    println!("ok            {}", t.ok);
    println!("ok_unflagged  {}", t.ok_unflagged);
    println!("bad_flagged   {}", t.bad_flagged);
    println!("bad_legacy    {}", t.bad_legacy);
    println!("bad_other     {}", t.bad_other);
    println!("errors        {}", t.errors);
    for b in &t.bad {
        println!(
            "  {} chain {} height {}: {}",
            b.class, b.chain, b.height, b.detail
        );
    }
    Ok(())
}
