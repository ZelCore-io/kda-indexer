//! Read-only measurements for audit PR-1 (F-14 limits, F-01 history). Opens the hot store
//! read-only (`HotStore::open_ro`, like `reconcile-balances --status`: no data-dir lock, every
//! read in a short txn) and the frozen segments without filters; writes nothing.
//!
//! ```text
//! audit_pr1_scan hot  <data dir>   # F-14: per-module Balances rows, stored guard sizes
//! audit_pr1_scan f01  <data dir>   # F-01: frozen ranges holding an affected event
//! ```
//!
//! F-01 counts: an *affected event* is a 4-param `TRANSFER_XCHAIN` / `TRANSFER_XCHAIN_RECD`
//! (amount in `p[2]`, chain id string in `p[3]`) with a side that is an indexable account but
//! not a principal (the pre-fix `event_accounts` posted only principals for these). A tx whose
//! `acct` account set grows under the fixed rule is a *changed tx*; a range holding one needs
//! `rebuild-range` to complete its history.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use kdx_decode::build::{
    account_ok, acct_transfer_accounts, event_accounts, is_principal, parse_event,
    transfer_amount_ok,
};
use kdx_hot::{balances, HotDict, HotStore, Table, Txn};
use kdx_segment::reader::Segments;
use kdx_types::{DictKind, MemDict};
use serde_json::{json, Value};

const CHUNK: usize = 50_000;

fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let (mode, dir) = match (a.get(1), a.get(2)) {
        (Some(m), Some(d)) => (m.as_str(), Path::new(d)),
        _ => anyhow::bail!("usage: audit_pr1_scan hot|f01 <data dir>"),
    };
    let hs = HotStore::open_ro(&dir.join("hot"))?;
    let dict = HotDict::load(&hs.ro()?)?.mem().clone();
    match mode {
        "hot" => hot(&hs, &dict),
        "f01" => f01(&dir.join("segments"), dict),
        _ => anyhow::bail!("mode must be hot or f01"),
    }
}

/// Every row of `table`, in chunks of fresh read txns.
fn each(hs: &HotStore, table: Table, mut f: impl FnMut(&[u8], &[u8])) -> anyhow::Result<u64> {
    let mut after: Option<Vec<u8>> = None;
    let mut n = 0u64;
    loop {
        let rows = hs
            .ro()?
            .scan_chunk(table, after.as_deref(), CHUNK, |_| true)?;
        for (k, v) in &rows {
            f(k, v);
            n += 1;
        }
        match rows.last() {
            Some((k, _)) if rows.len() == CHUNK => after = Some(k.clone()),
            _ => return Ok(n),
        }
    }
}

fn hot(hs: &HotStore, dict: &MemDict) -> anyhow::Result<()> {
    let modules = dict.names(DictKind::Module);
    let coin = modules.iter().position(|m| m == "coin").map(|i| i as u32);
    let mut per: BTreeMap<u32, u64> = BTreeMap::new();
    let rows = each(hs, Table::Balances, |k, _| {
        if let Some((_, m, _)) = balances::parse_bal_key(k) {
            *per.entry(m).or_insert(0) += 1;
        }
    })?;
    let mut top: Vec<(u64, u32)> = per
        .iter()
        .filter(|(m, _)| Some(**m) != coin)
        .map(|(m, n)| (*n, *m))
        .collect();
    top.sort_unstable_by(|a, b| b.cmp(a));
    let name = |m: u32| modules.get(m as usize).cloned().unwrap_or_default();
    let (mut max_guard, mut over_cap, mut max_keys, mut max_key_len) = (0usize, 0u64, 0usize, 0);
    let (mut long_keys, mut long_guards) = (0u64, 0u64);
    let mut long_kinds: BTreeMap<String, u64> = BTreeMap::new();
    let mut sizes = [0u64; 5]; // <=256, <=1024, <=4096, <=16384, more
    let guards = each(hs, Table::Guards, |_, v| {
        max_guard = max_guard.max(v.len());
        over_cap += u64::from(v.len() > 4096);
        sizes[[256, 1024, 4096, 16384]
            .iter()
            .position(|l| v.len() <= *l)
            .unwrap_or(4)] += 1;
        if let Ok(g) = serde_json::from_slice::<Value>(v) {
            if let Some(Value::Array(keys)) = g.get("keys") {
                max_keys = max_keys.max(keys.len());
                let mut long = false;
                for k in keys.iter().filter_map(Value::as_str) {
                    max_key_len = max_key_len.max(k.len());
                    if k.len() > 128 {
                        long = true;
                        long_keys += 1;
                        // The key's scheme prefix (`WEBAUTHN-`, ...) or its length class.
                        let kind = k.split_once('-').map_or_else(
                            || format!("no-prefix len {}", k.len() / 16 * 16),
                            |(p, _)| format!("{p}- len {}", k.len() / 16 * 16),
                        );
                        *long_kinds.entry(kind).or_insert(0) += 1;
                    }
                }
                long_guards += u64::from(long);
            }
        }
    })?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "balance_rows": rows,
            "coin_rows": coin.and_then(|c| per.get(&c)),
            "modules_with_rows": per.len(),
            "largest_non_coin_modules": top.iter().take(15)
                .map(|(n, m)| json!({"module": name(*m), "rows": n})).collect::<Vec<_>>(),
            "guards": guards,
            "largest_guard_bytes": max_guard,
            "guards_over_4096": over_cap,
            "guard_size_buckets_le256_le1k_le4k_le16k_more": sizes,
            "largest_keyset_keys": max_keys,
            "longest_key_bytes": max_key_len,
            "keys_over_128_bytes": long_keys,
            "guards_with_a_key_over_128_bytes": long_guards,
            "long_key_kinds": long_kinds,
        }))?
    );
    Ok(())
}

/// The pre-fix `event_accounts` (the cross-chain arm could never run).
fn old_event_accounts<'a>(e: &'a Value, out: &mut Vec<&'a str>) {
    fn principals<'a>(v: &'a Value, out: &mut Vec<&'a str>) {
        match v {
            Value::String(s) if is_principal(s) => out.push(s),
            Value::Array(a) => a.iter().for_each(|x| principals(x, out)),
            Value::Object(m) => m.values().for_each(|x| principals(x, out)),
            _ => {}
        }
    }
    let Some(params) = e.get("params") else {
        return;
    };
    if let (Some(ev), Some(p)) = (parse_event(e), params.as_array()) {
        if let Some((from, to)) = acct_transfer_accounts(&ev) {
            out.extend([from, to]);
        } else if p.len() == 4 && matches!(p[3], Value::Number(_) | Value::Object(_)) {
            let sides = match ev.name {
                "TRANSFER" if p[0].is_string() => Some((&p[1], &p[2])),
                "TRANSFER_XCHAIN" | "TRANSFER_XCHAIN_RECD" => Some((&p[0], &p[1])),
                _ => None,
            };
            if let Some((Some(a), Some(b))) = sides.map(|(a, b)| (a.as_str(), b.as_str())) {
                out.extend([a, b]);
            }
        }
    }
    principals(params, out);
}

fn is_xchain(name: &str) -> bool {
    name == "TRANSFER_XCHAIN" || name == "TRANSFER_XCHAIN_RECD"
}

/// An affected event (see the module docs).
fn affected(e: &Value) -> bool {
    let (Some(ev), Some(p)) = (parse_event(e), e.get("params").and_then(Value::as_array)) else {
        return false;
    };
    is_xchain(ev.name)
        && p.len() == 4
        && transfer_amount_ok(&p[2])
        && p[3].is_string()
        && p[..2]
            .iter()
            .filter_map(Value::as_str)
            .any(|s| account_ok(s) && !is_principal(s))
}

#[derive(Default)]
struct RangeCount {
    events: u64,
    txs_with_event: u64,
    changed_txs: u64,
    new_names: u64,
}

fn f01(segdir: &Path, dict: MemDict) -> anyhow::Result<()> {
    let xchain: HashSet<u32> = dict
        .names(DictKind::EventName)
        .iter()
        .enumerate()
        .filter(|(_, n)| n.rsplit_once('.').is_some_and(|(_, e)| is_xchain(e)))
        .map(|(i, _)| i as u32)
        .collect();
    let dict = Arc::new(dict);
    let segs = Segments::open_without_filters(segdir, dict.clone())?;
    let entries = segs.manifest.entries.clone();
    let (mut rendered_raw, mut errors, mut txs) = (0u64, 0u64, 0u64);
    let mut out = Vec::new();
    let mut tot = RangeCount::default();
    let mut names = BTreeSet::new();
    for (i, r) in entries.iter().enumerate() {
        let mut c = RangeCount::default();
        for t in r.first_txnum..r.end_txnum() {
            let Some(key) = segs.txkey(t) else { continue };
            if key.is_coinbase() {
                continue;
            }
            let Some(rec) = (match segs.tx(t) {
                Ok(x) => x,
                Err(e) => {
                    errors += 1;
                    eprintln!("txnum {t}: {e:#}");
                    continue;
                }
            }) else {
                continue;
            };
            txs += 1;
            let typed = rec.events.iter().any(|e| xchain.contains(&e.name));
            if !typed && rec.raw_output.is_none() {
                continue;
            }
            rendered_raw += u64::from(!typed);
            let o = kdx_decode::render_output(&rec, dict.as_ref())
                .with_context(|| format!("render txnum {t}"))?;
            let evs = o
                .get("events")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let n = evs.iter().filter(|e| affected(e)).count() as u64;
            if n == 0 {
                continue;
            }
            c.events += n;
            c.txs_with_event += 1;
            let (mut old, mut new) = (Vec::new(), Vec::new());
            for e in &evs {
                old_event_accounts(e, &mut old);
                event_accounts(e, &mut new);
            }
            let old: HashSet<&str> = old.into_iter().filter(|s| account_ok(s)).collect();
            let added: BTreeSet<&str> = new
                .into_iter()
                .filter(|s| account_ok(s) && !old.contains(s))
                .collect();
            if !added.is_empty() {
                c.changed_txs += 1;
                c.new_names += added.len() as u64;
                names.extend(added.iter().map(|s| (*s).to_owned()));
            }
        }
        if c.events > 0 {
            out.push(
                json!({"range": i, "kind": format!("{:?}", r.kind), "heights": [r.start, r.end],
                "events": c.events, "txs_with_event": c.txs_with_event,
                "changed_txs": c.changed_txs, "new_postings": c.new_names}),
            );
        }
        tot.events += c.events;
        tot.txs_with_event += c.txs_with_event;
        tot.changed_txs += c.changed_txs;
        tot.new_names += c.new_names;
        eprintln!(
            "range {i}/{} [{}, {}) events {}",
            entries.len(),
            r.start,
            r.end,
            c.events
        );
    }
    let changed_ranges = out
        .iter()
        .filter(|r| r["changed_txs"].as_u64() > Some(0))
        .count();
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "frozen_ranges": entries.len(),
            "frozen_user_txs": txs,
            "ranges_with_affected_event": out.len(),
            "ranges_whose_postings_change": changed_ranges,
            "affected_events": tot.events,
            "txs_with_affected_event": tot.txs_with_event,
            "changed_txs": tot.changed_txs,
            "new_postings": tot.new_names,
            "distinct_new_accounts": names.len(),
            "raw_output_txs_rendered": rendered_raw,
            "read_errors": errors,
            "ranges": out,
        }))?
    );
    Ok(())
}
