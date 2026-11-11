//! Hidden `inspect view-check`: Review Focus 1 and 2 check. Loops `Store::view` beside a
//! running indexer (read-only open) and, per iteration, walks the whole posting list of a
//! busy entity (the `coin` module's events), requiring strictly descending keys (a tx seen
//! twice = a duplicate), resolves sampled keys by TxKey and by request key, and re-walks the
//! page after a cursor taken in an earlier iteration (it must survive freezes and merges,
//! or answer "invalidated by reorg"). Prints a JSON summary; exit 1 on any duplicate,
//! order break, lookup miss or error.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use kdx_api::{ApiError, Cursor, PostingQuery, Store};
use kdx_decode::index::evt_module_entity;
use kdx_decode::Family;
use kdx_hot::HotStore;
use kdx_types::{Dict, DictKind, TxKey};
use rand::RngExt;

#[derive(Default, Debug, serde::Serialize)]
struct Tally {
    iterations: u64,
    keys_walked: u64,
    duplicates: u64,
    order_breaks: u64,
    tx_misses: u64,
    rk_misses: u64,
    rk_checked: u64,
    count_drops: u64,
    cursor_checks: u64,
    cursor_mismatches: u64,
    cursor_reorged: u64,
    errors: u64,
    max_view_ms: u64,
    max_walk_ms: u64,
    seg_frontiers_seen: Vec<u32>,
    hot_frontiers_seen: Vec<u32>,
}

/// Below this height mainnet blocks carry no events (a coarse switch for the dev check).
const EVENTS_FROM: u32 = 1_200_000;

pub fn run(data_dir: &Path, seconds: u64) -> anyhow::Result<()> {
    let data_dir = std::fs::canonicalize(data_dir)?;
    let hot = Arc::new(HotStore::open_ro(&data_dir.join("hot"))?);
    let store = Arc::new(Store::open(hot, &data_dir)?);
    let end = Instant::now() + Duration::from_secs(seconds);
    let mut t = Tally::default();
    let mut rng = rand::rng();
    let mut last_count = 0u64;
    // (cursor, the keys that followed it when taken)
    let mut saved: Option<(Cursor, Vec<TxKey>)> = None;
    while Instant::now() < end {
        t.iterations += 1;
        let t0 = Instant::now();
        let view = match store.view(Instant::now() + Duration::from_secs(30)) {
            Ok(v) => v,
            Err(e) => {
                t.errors += 1;
                eprintln!("view: {e:?}");
                continue;
            }
        };
        t.max_view_ms = t.max_view_ms.max(t0.elapsed().as_millis() as u64);
        if t.seg_frontiers_seen.last() != Some(&view.seg_frontier) {
            t.seg_frontiers_seen.push(view.seg_frontier);
        }
        if t.hot_frontiers_seen.last() != Some(&view.hot_frontier) {
            t.hot_frontiers_seen.push(view.hot_frontier);
        }
        // The `coin` module's events; early history has none, then account 0 (a miner).
        let (fam, ent) = match view.dict().find(DictKind::Module, "coin") {
            Some(coin) if view.seg_frontier > EVENTS_FROM || view.hot_floor() > EVENTS_FROM => {
                (Family::Evt, evt_module_entity(coin))
            }
            _ => (Family::Acct, 0),
        };
        let res = (|| -> Result<(), ApiError> {
            let w0 = Instant::now();
            let mut keys = Vec::new();
            for k in view.postings(fam, ent, PostingQuery::default())? {
                keys.push(k?);
            }
            t.max_walk_ms = t.max_walk_ms.max(w0.elapsed().as_millis() as u64);
            t.keys_walked += keys.len() as u64;
            for w in keys.windows(2) {
                if w[0] == w[1] {
                    t.duplicates += 1;
                } else if w[0] < w[1] {
                    t.order_breaks += 1;
                }
            }
            if (keys.len() as u64) < last_count {
                t.count_drops += 1;
            }
            last_count = keys.len() as u64;
            for _ in 0..20.min(keys.len()) {
                let k = keys[rng.random_range(0..keys.len())];
                match view.tx(k)? {
                    None => t.tx_misses += 1,
                    Some(r) => {
                        if let Some(rk) = r.request_key {
                            t.rk_checked += 1;
                            match view.tx_by_rk(&rk)? {
                                Some((k2, _)) if k2 == k => {}
                                _ => t.rk_misses += 1,
                            }
                        }
                    }
                }
            }
            // Cursor: re-walk after a cursor saved earlier; the keys below it must be the
            // ones seen then (new blocks only arrive above it).
            if let Some((c, after)) = &saved {
                t.cursor_checks += 1;
                match c.check(&view) {
                    Err(ApiError::BadRequest(_)) => t.cursor_reorged += 1,
                    Err(e) => return Err(e),
                    Ok(()) => {
                        let q = PostingQuery {
                            before: Some(c.key),
                            ..Default::default()
                        };
                        let got: Vec<TxKey> = view
                            .postings(fam, ent, q)?
                            .take(after.len())
                            .collect::<Result<_, _>>()?;
                        if &got != after {
                            t.cursor_mismatches += 1;
                            eprintln!("cursor at {:?}: {} keys differ", c.key, after.len());
                        }
                    }
                }
            }
            if t.iterations % 50 == 1 && keys.len() > 200 {
                // A cursor in the newest part (hot until frozen).
                let i = 50;
                let c = Cursor::new(&view, keys[i], 0)?;
                saved = Some((c, keys[i + 1..(i + 151).min(keys.len())].to_vec()));
            }
            Ok(())
        })();
        if let Err(e) = res {
            t.errors += 1;
            eprintln!("iteration {}: {e:?}", t.iterations);
        }
        drop(view);
        if t.iterations % 100 == 0 {
            eprintln!("{}", serde_json::to_string(&t)?);
        }
    }
    println!("{}", serde_json::to_string_pretty(&t).context("summary")?);
    let bad =
        t.duplicates + t.order_breaks + t.tx_misses + t.rk_misses + t.cursor_mismatches + t.errors;
    if bad > 0 {
        anyhow::bail!("view-check found {bad} problems");
    }
    Ok(())
}
