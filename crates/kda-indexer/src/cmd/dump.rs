//! `dump` command: fetch a bounded height range, decode, verify, build records with one shared
//! `MemDict`, encode, decode back and check the round trip (Task 5 definition).
//!
//! Heights are processed in slabs of `SLAB` heights: every chain of the slab is fetched
//! concurrently (headers, then payloads in batches), then the blocks are handled in
//! (height, chain) order so dictionary ids and the records file are in TxKey order.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::{bail, Context};
use kdx_decode::{
    build_records, compare_json, decode_block, render_cmd, render_header, render_miner,
    render_output, render_tx, verify_block, verify_linkage, Header,
};
use kdx_node::{ChainGraph, NodeClient};
use kdx_types::hash::b64url_decode;
use kdx_types::record::{
    decode_block as decode_block_rec, decode_tx, encode_block, encode_tx, Sig, TxRecord,
};
use kdx_types::{Dict, DictKind, MemDict};
use serde::Serialize;
use serde_json::{json, Value};

use super::frames::{dict_path, put_frame, KIND_BLOCK, KIND_COINBASE, KIND_USER, KIND_USER_LEGACY};
use crate::config::Config;

const SLAB: u64 = 100;
const BAND: u64 = 100_000;

pub struct DumpArgs {
    pub from: String,
    pub to: String,
    pub chains: Option<Vec<u8>>,
    pub json: bool,
    pub records_out: Option<PathBuf>,
    pub summary_out: Option<PathBuf>,
}

#[derive(Default, Serialize)]
struct Band {
    user_txs: u64,
    legacy_cmd_hash_mismatch: u64,
}

#[derive(Default, Serialize)]
struct Summary {
    from: u64,
    to: u64,
    blocks: u64,
    user_txs: u64,
    coinbase_records: u64,
    cont_txs: u64,
    max_events_in_tx: usize,
    max_cont_cmd_bytes: usize,
    other_sigs: u64,
    records_with_extra: u64,
    round_trip_failures: u64,
    verify_failures: u64,
    decode_failures: u64,
    fetch_failures: u64,
    legacy_cmd_hash_mismatch: u64,
    block_record_bytes: u64,
    coinbase_record_bytes: u64,
    user_record_bytes: u64,
    dict_sizes: BTreeMap<String, usize>,
    /// `chain -> band start height -> counts`.
    legacy_by_chain_band: BTreeMap<String, BTreeMap<String, Band>>,
    requests: u64,
}

struct Ctx {
    sum: Summary,
    dict: MemDict,
    prev: BTreeMap<u8, Header>,
    json: bool,
    out: Option<BufWriter<File>>,
}

impl Ctx {
    fn fail_rt(&mut self, chain: u8, height: u64, pos: usize, msg: &str) {
        self.sum.round_trip_failures += 1;
        eprintln!("ROUND-TRIP FAIL chain {chain} height {height} pos {pos}: {msg}");
    }
}

/// Strict comparison: same key order and number text (Value equality ignores key order).
fn same_text(a: &Value, b: &Value) -> bool {
    let (x, y) = (a.to_string(), b.to_string());
    x == y
}

fn parse_height(s: &str, tip: Option<u64>) -> anyhow::Result<u64> {
    if let Some(rest) = s.strip_prefix("tip") {
        let tip = tip.context("no tip available")?;
        if rest.is_empty() {
            return Ok(tip);
        }
        let n: u64 = rest
            .strip_prefix('-')
            .context("expected tip-N")?
            .parse()
            .context("expected tip-N")?;
        return tip.checked_sub(n).context("tip-N below zero");
    }
    s.parse().with_context(|| format!("bad height {s:?}"))
}

type Fetched = Vec<(Value, Value)>;

/// Headers come from `header/branch` with the current cut's hash as `upper`, so only the
/// canonical branch is returned (`header?minheight&maxheight` also returns orphaned siblings).
async fn fetch_chain(
    nc: &NodeClient,
    chain: u8,
    cut_hash: &str,
    lo: u64,
    hi: u64,
) -> anyhow::Result<Fetched> {
    let page = nc
        .header_branch(chain, &[], &[cut_hash.to_owned()], Some(lo), Some(hi))
        .await?;
    if page.next.is_some() {
        bail!("chain {chain} {lo}..{hi}: header/branch page is not complete (reduce SLAB)");
    }
    let mut headers = page.items;
    headers.sort_by_key(|h| h.get("height").and_then(Value::as_u64).unwrap_or(0));
    let want = (hi - lo + 1) as usize;
    if headers.len() != want {
        bail!(
            "chain {chain} {lo}..{hi}: node returned {} headers, expected {want}",
            headers.len()
        );
    }
    let hashes: Vec<String> = headers
        .iter()
        .map(|h| {
            h.get("payloadHash")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .context("header without payloadHash")
        })
        .collect::<anyhow::Result<_>>()?;
    let payloads = nc.payloads_with_outputs(chain, &hashes).await?;
    headers
        .into_iter()
        .zip(hashes)
        .map(|(h, ph)| {
            let p = payloads.get(&ph).cloned().context("payload missing")?;
            Ok((h, p))
        })
        .collect()
}

pub async fn run(cfg: &Config, args: DumpArgs) -> anyhow::Result<()> {
    let nc = super::node_client(cfg)?;
    nc.set_batch_limit(cfg.ingest.batch_size);
    let info = nc.info().await?;
    if info.chainweb_version != cfg.network {
        bail!(
            "node network {:?} != config network {:?}",
            info.chainweb_version,
            cfg.network
        );
    }
    let graph = ChainGraph::from_info(&info);
    let cut = nc.cut().await?;
    let tip = cut.hashes.values().map(|e| e.height).min();
    let from = parse_height(&args.from, tip)?;
    let to = parse_height(&args.to, tip)?;
    if from > to {
        bail!("--from {from} > --to {to}");
    }
    eprintln!("dump: heights {from}..={to} (tip {tip:?})");

    let out = match &args.records_out {
        Some(p) => {
            if let Some(d) = p.parent() {
                std::fs::create_dir_all(d)?;
            }
            Some(BufWriter::new(
                File::create(p).with_context(|| format!("creating {}", p.display()))?,
            ))
        }
        None => None,
    };
    let mut cx = Ctx {
        sum: Summary {
            from,
            to,
            ..Default::default()
        },
        dict: MemDict::new(),
        prev: BTreeMap::new(),
        json: args.json,
        out,
    };

    let mut lo = from;
    while lo <= to {
        let hi = (lo + SLAB - 1).min(to);
        // Chains and their height sub-range inside this slab.
        let mut ranges: BTreeMap<u8, (u64, u64)> = BTreeMap::new();
        for h in lo..=hi {
            for &c in graph.chains_at(h) {
                if args.chains.as_ref().is_some_and(|f| !f.contains(&c)) {
                    continue;
                }
                let e = ranges.entry(c).or_insert((h, h));
                e.1 = h;
            }
        }
        let futs = ranges.iter().map(|(&c, &(a, b))| {
            let nc = nc.clone();
            let cut_hash = cut.hashes[&c].hash.clone();
            async move { (c, fetch_chain(&nc, c, &cut_hash, a, b).await) }
        });
        let results = futures::future::join_all(futs).await;
        let mut per_height: BTreeMap<(u64, u8), (Value, Value)> = BTreeMap::new();
        for (c, r) in results {
            match r {
                Ok(items) => {
                    let (a, _) = ranges[&c];
                    for (i, it) in items.into_iter().enumerate() {
                        per_height.insert((a + i as u64, c), it);
                    }
                }
                Err(e) => {
                    cx.sum.fetch_failures += 1;
                    eprintln!("FETCH FAIL chain {c} slab {lo}..{hi}: {e:#}");
                    cx.prev.remove(&c);
                }
            }
        }
        for ((h, c), (hdr, payload)) in per_height {
            handle_block(&mut cx, c, h, &hdr, &payload)?;
        }
        lo = hi + 1;
    }

    let Ctx {
        mut sum, dict, out, ..
    } = cx;
    if let Some(mut w) = out {
        w.flush()?;
    }
    if let Some(p) = &args.records_out {
        let dp = dict_path(p);
        std::fs::write(&dp, serde_json::to_vec(&dict)?)?;
        // The dictionary file must reload to the same tables.
        let back: MemDict = serde_json::from_slice(&std::fs::read(&dp)?)?;
        for k in DictKind::ALL {
            for id in 0..dict.len(k) as u32 {
                if back.name(k, id) != dict.name(k, id) {
                    sum.round_trip_failures += 1;
                    eprintln!("ROUND-TRIP FAIL dict.json {k:?} id {id}");
                    break;
                }
            }
        }
    }
    for k in DictKind::ALL {
        sum.dict_sizes.insert(format!("{k:?}"), dict.len(k));
    }
    sum.requests = nc.stats().iter().map(|r| r.count).sum();
    print_summary(&sum, args.json);
    if let Some(p) = &args.summary_out {
        if let Some(d) = p.parent() {
            std::fs::create_dir_all(d)?;
        }
        std::fs::write(p, serde_json::to_vec_pretty(&sum)?)?;
    }
    let failures =
        sum.round_trip_failures + sum.verify_failures + sum.decode_failures + sum.fetch_failures;
    if failures > 0 {
        bail!("{failures} failure(s)");
    }
    Ok(())
}

fn handle_block(
    cx: &mut Ctx,
    chain: u8,
    height: u64,
    hdr: &Value,
    payload: &Value,
) -> anyhow::Result<()> {
    let b = match decode_block(hdr, payload) {
        Ok(b) => b,
        Err(e) => {
            cx.sum.decode_failures += 1;
            eprintln!("DECODE FAIL chain {chain} height {height}: {e}");
            cx.prev.remove(&chain);
            return Ok(());
        }
    };
    if b.header.chain_id != chain || b.header.height != height {
        cx.sum.decode_failures += 1;
        eprintln!(
            "DECODE FAIL chain {chain} height {height}: header says chain {} height {}",
            b.header.chain_id, b.header.height
        );
        return Ok(());
    }
    let report = match verify_block(&b, &b.header.payload_hash) {
        Ok(r) => r,
        Err(e) => {
            cx.sum.verify_failures += 1;
            eprintln!("VERIFY FAIL chain {chain} height {height}: {e}");
            Default::default()
        }
    };
    if let Some(prev) = cx.prev.get(&chain) {
        if let Err(e) = verify_linkage(prev, &b.header) {
            cx.sum.verify_failures += 1;
            eprintln!("VERIFY FAIL chain {chain} height {height}: {e}");
        }
    }
    cx.prev.insert(chain, b.header.clone());

    let (br, trs) = build_records(&b, &mut cx.dict);
    cx.sum.blocks += 1;

    // Block record round trip.
    let mut buf = Vec::new();
    encode_block(&br, &mut buf);
    cx.sum.block_record_bytes += buf.len() as u64;
    match decode_block_rec(&buf) {
        Ok(br2) => {
            if br2 != br {
                cx.fail_rt(chain, height, 0, "block record differs after decode");
            }
            if let Some(d) = compare_json(hdr, &render_header(&br2)) {
                cx.fail_rt(chain, height, 0, &format!("header: {d}"));
            }
            match render_miner(&br2, &cx.dict) {
                Ok(m) if same_text(&m, &b.miner) => {}
                Ok(_) => cx.fail_rt(chain, height, 0, "minerData differs"),
                Err(e) => cx.fail_rt(chain, height, 0, &format!("minerData render: {e}")),
            }
        }
        Err(e) => cx.fail_rt(chain, height, 0, &format!("block record decode: {e}")),
    }
    if let Some(w) = &mut cx.out {
        put_frame(w, KIND_BLOCK, &buf)?;
    }

    let band = height / BAND * BAND;
    for (i, r) in trs.iter().enumerate() {
        let legacy = i > 0 && report.cmd_hash_mismatch.contains(&i);
        let mut buf = Vec::new();
        encode_tx(r, &mut buf);
        let kind = match (i, legacy) {
            (0, _) => KIND_COINBASE,
            (_, false) => KIND_USER,
            (_, true) => KIND_USER_LEGACY,
        };
        if i == 0 {
            cx.sum.coinbase_records += 1;
            cx.sum.coinbase_record_bytes += buf.len() as u64;
        } else {
            cx.sum.user_txs += 1;
            cx.sum.user_record_bytes += buf.len() as u64;
            let e = cx
                .sum
                .legacy_by_chain_band
                .entry(chain.to_string())
                .or_default()
                .entry(band.to_string())
                .or_default();
            e.user_txs += 1;
            if legacy {
                e.legacy_cmd_hash_mismatch += 1;
                cx.sum.legacy_cmd_hash_mismatch += 1;
            }
        }
        check_tx(cx, &b, payload, chain, height, i, r, &buf);
        if cx.json {
            println!(
                "{}",
                json!({
                    "txkey": r.key.0,
                    "chain": chain,
                    "height": height,
                    "pos": i,
                    "reqKey": match (&r.request_key, &r.cb_req_key) {
                        (Some(h), _) => Some(h.to_b64url()),
                        (None, Some(c)) => Some(c.to_wire()),
                        _ => None,
                    },
                    "status": if r.status_ok { "success" } else { "failure" },
                    "events": r.events.len(),
                    "bytes": buf.len(),
                })
            );
        }
        if let Some(w) = &mut cx.out {
            put_frame(w, kind, &buf)?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn check_tx(
    cx: &mut Ctx,
    b: &kdx_decode::DecodedBlock,
    payload: &Value,
    chain: u8,
    height: u64,
    i: usize,
    r: &TxRecord,
    buf: &[u8],
) {
    cx.sum.max_events_in_tx = cx.sum.max_events_in_tx.max(r.events.len());
    cx.sum.other_sigs += r.sigs.iter().filter(|s| matches!(s, Sig::Other(_))).count() as u64;
    let r2 = match decode_tx(buf) {
        Ok(r2) => r2,
        Err(e) => {
            cx.fail_rt(chain, height, i, &format!("record decode: {e}"));
            return;
        }
    };
    if r2 != *r {
        cx.fail_rt(chain, height, i, "record differs after decode");
    }
    if r2.extra.is_some() {
        cx.sum.records_with_extra += 1;
    }
    let orig_out = if i == 0 {
        &b.coinbase
    } else {
        &b.txs[i - 1].output
    };
    match render_output(&r2, &cx.dict) {
        Ok(ro) => {
            if let Some(d) = compare_json(orig_out, &ro) {
                cx.fail_rt(chain, height, i, &format!("output: {d}"));
            }
        }
        Err(e) => cx.fail_rt(chain, height, i, &format!("output render: {e}")),
    }
    if i == 0 {
        return;
    }
    let t = &b.txs[i - 1];
    if t.cmd_parsed.pointer("/payload/cont").is_some() {
        cx.sum.cont_txs += 1;
        cx.sum.max_cont_cmd_bytes = cx.sum.max_cont_cmd_bytes.max(t.cmd.len());
    }
    if render_cmd(&r2) != Some(t.cmd.as_bytes()) {
        cx.fail_rt(chain, height, i, "cmd bytes differ");
    }
    let orig_tx = payload
        .pointer(&format!("/transactions/{}/0", i - 1))
        .and_then(Value::as_str)
        .and_then(|s| b64url_decode(s).ok())
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    match (orig_tx, render_tx(&r2, &cx.dict)) {
        (Some(o), Ok(Some(rt))) => {
            if let Some(d) = compare_json(&o, &rt) {
                cx.fail_rt(chain, height, i, &format!("tx object: {d}"));
            }
        }
        (None, _) => cx.fail_rt(chain, height, i, "original tx object unreadable"),
        (_, Ok(None)) => cx.fail_rt(chain, height, i, "tx object not rendered"),
        (_, Err(e)) => cx.fail_rt(chain, height, i, &format!("tx render: {e}")),
    }
}

fn print_summary(s: &Summary, to_stderr: bool) {
    let mut o = String::new();
    use std::fmt::Write as _;
    let _ = writeln!(o, "heights            {}..={}", s.from, s.to);
    let _ = writeln!(o, "blocks             {}", s.blocks);
    let _ = writeln!(o, "user txs           {}", s.user_txs);
    let _ = writeln!(o, "coinbase records   {}", s.coinbase_records);
    let _ = writeln!(
        o,
        "cont txs           {} (max cmd {} B)",
        s.cont_txs, s.max_cont_cmd_bytes
    );
    let _ = writeln!(o, "max events in tx   {}", s.max_events_in_tx);
    let _ = writeln!(o, "Other-sigs         {}", s.other_sigs);
    let _ = writeln!(o, "records with extra {}", s.records_with_extra);
    let _ = writeln!(
        o,
        "record bytes       blocks {} coinbase {} user {}",
        s.block_record_bytes, s.coinbase_record_bytes, s.user_record_bytes
    );
    let _ = writeln!(o, "dict sizes         {:?}", s.dict_sizes);
    let _ = writeln!(o, "requests           {}", s.requests);
    let _ = writeln!(
        o,
        "legacy cmd hash mismatch (soft): {} of {} user txs",
        s.legacy_cmd_hash_mismatch, s.user_txs
    );
    for (chain, bands) in &s.legacy_by_chain_band {
        for (band, c) in bands {
            if c.legacy_cmd_hash_mismatch > 0 {
                let _ = writeln!(
                    o,
                    "  chain {chain:>2} band {band:>9}: {} of {} ({:.1}%)",
                    c.legacy_cmd_hash_mismatch,
                    c.user_txs,
                    100.0 * c.legacy_cmd_hash_mismatch as f64 / c.user_txs as f64
                );
            }
        }
    }
    let _ = writeln!(o, "round-trip failures {}", s.round_trip_failures);
    let _ = writeln!(o, "verify failures     {}", s.verify_failures);
    let _ = writeln!(o, "decode failures     {}", s.decode_failures);
    let _ = writeln!(o, "fetch failures      {}", s.fetch_failures);
    if to_stderr {
        eprint!("{o}");
    } else {
        print!("{o}");
    }
}
