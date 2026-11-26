//! `kda-indexer reconcile-counts` (PR #48 part 2): build the stored per-key row counts
//! (`kdx_hot::rowcount`) from every record, frozen and hot, and set the readiness marker.
//!
//! - Offline: the caller holds the data-dir lock, so neither the tail nor the freezer moves.
//! - The frozen TxNums below the frontier are cut into slices that `threads` workers read and
//!   count in parallel (the PR #40 history-walk layout: one `Segments` shared by the workers,
//!   one dictionary snapshot), each slice into its own tally merged into one; the hot `TailTxs` rows at or above the
//!   frontier are counted here. The tallies are merged and replace the table.
//! - Idempotent: the table is rebuilt from scratch on each run. The marker is cleared first
//!   and set last, so a run that stops halfway leaves the counts untrusted (readers walk).
//! - A record whose output does not render is counted nowhere and leaves the marker unset
//!   (the walk fails on that tx too; stored counts would hide it).

use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use kdx_hot::rowcount::{self, Tally};
use kdx_hot::{HotDict, HotStore, Table, Txn};
use kdx_segment::Segments;
use kdx_types::record::decode_tx;
use kdx_types::{MemDict, TxKey};
use serde::Serialize;

/// Frozen TxNums one worker takes at a time.
pub const SLICE_TXS: u64 = 1 << 16;
/// Rows written per write txn when the table is replaced.
pub const WRITE_CHUNK: usize = 1 << 20;

#[derive(Debug, Clone, Default, Serialize)]
pub struct Report {
    pub frozen_txs: u64,
    pub hot_txs: u64,
    /// Records whose output does not render (the marker stays unset when any).
    pub unrendered: u64,
    /// Rows of the rebuilt table.
    pub keys: u64,
    /// `--check`: rows whose stored value differed from the rebuilt one (missing or extra
    /// rows included). Nothing is written in that mode.
    pub differing: Option<u64>,
    pub threads: usize,
    pub count_secs: f64,
    pub write_secs: f64,
    /// Records counted per second (frozen and hot, counting only).
    pub rate: f64,
    pub ready: bool,
}

/// Count every record into a [`Tally`]: the frozen part on `threads` workers, then the hot
/// part. `stop` is polled between slices.
pub fn tally(
    segdir: &Path,
    hot: &HotStore,
    threads: usize,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<(Tally, Report)> {
    let t0 = Instant::now();
    let ro = hot.ro()?;
    let snap: Arc<MemDict> = Arc::new(HotDict::load(&ro)?.mem().clone());
    let frontier = ro.frozen_frontier()?;
    let segs = Segments::open_without_filters(segdir, snap.clone())?;
    let start = segs.manifest.entries.first().map_or(0, |e| e.first_txnum);
    let end = segs
        .last_txnum_below_height(frontier)
        .map_or(start, |t| t + 1);
    let threads = threads.max(1);
    let slices = (end.saturating_sub(start)).div_ceil(SLICE_TXS) as usize;
    let next = AtomicUsize::new(0);
    let (frozen, unrendered) = (AtomicU64::new(0), AtomicU64::new(0));
    // Each slice is counted into its own small tally and merged into `all` under the lock,
    // so the peak memory is the merged table plus one slice per worker.
    let all = std::sync::Mutex::new(Tally::default());
    std::thread::scope(|sc| -> Result<()> {
        let mut hs = Vec::new();
        for _ in 0..threads.min(slices.max(1)) {
            let (segs, snap, next, frozen, unrendered, all) =
                (&segs, &*snap, &next, &frozen, &unrendered, &all);
            hs.push(sc.spawn(move || -> Result<()> {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= slices || stop() {
                        return Ok(());
                    }
                    let a = start + i as u64 * SLICE_TXS;
                    let b = end.min(a + SLICE_TXS);
                    let (mut t, mut n, mut bad) = (Tally::default(), 0u64, 0u64);
                    for txnum in a..b {
                        let Some(key) = segs.txkey(txnum) else {
                            continue;
                        };
                        if key.height() >= frontier {
                            continue;
                        }
                        let r = segs
                            .tx(txnum)?
                            .with_context(|| format!("frozen TxNum {txnum} has no record"))?;
                        n += 1;
                        if !t.add(&r, snap) {
                            bad += 1;
                        }
                    }
                    frozen.fetch_add(n, Ordering::Relaxed);
                    unrendered.fetch_add(bad, Ordering::Relaxed);
                    all.lock().unwrap_or_else(|e| e.into_inner()).merge(t);
                }
            }));
        }
        for h in hs {
            h.join().expect("count worker panicked")?;
        }
        Ok(())
    })?;
    let mut total = all.into_inner().unwrap_or_else(|e| e.into_inner());
    anyhow::ensure!(!stop(), "stopped");
    let mut rep = Report {
        threads,
        frozen_txs: frozen.into_inner(),
        unrendered: unrendered.into_inner(),
        ..Report::default()
    };
    let from = TxKey::new(frontier, 0, 0).to_be_bytes();
    for (k, v) in ro.scan(Table::TailTxs, &from, |_| true)? {
        let r = decode_tx(&v).with_context(|| format!("hot record {k:?}"))?;
        if r.key.height() < frontier {
            continue;
        }
        rep.hot_txs += 1;
        if !total.add(&r, &*snap) {
            rep.unrendered += 1;
        }
    }
    drop(ro);
    rep.keys = total.0.len() as u64;
    rep.count_secs = t0.elapsed().as_secs_f64();
    rep.rate = (rep.frozen_txs + rep.hot_txs) as f64 / rep.count_secs.max(1e-9);
    Ok((total, rep))
}

/// Rebuild `RowCounts` and set the marker (or, with `check`, only compare). The caller holds
/// the data-dir lock.
pub fn reconcile(
    segdir: &Path,
    hot: &HotStore,
    threads: usize,
    check: bool,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Report> {
    let (t, mut rep) = tally(segdir, hot, threads, stop)?;
    if check {
        let stored = rowcount::read_all(&hot.ro()?)?;
        let mut diff =
            t.0.iter()
                .filter(|(k, v)| stored.0.get(k) != Some(v))
                .count() as u64;
        diff += stored.0.keys().filter(|k| !t.0.contains_key(k)).count() as u64;
        rep.differing = Some(diff);
        rep.ready = rowcount::ready(&hot.ro()?)?;
        return Ok(rep);
    }
    let t1 = Instant::now();
    let next_txnum = {
        let mut txn = hot.rw()?;
        rowcount::clear_ready(&mut txn)?;
        rowcount::clear_table(&mut txn)?;
        let n = txn.next_txnum()?;
        txn.commit()?;
        n
    };
    let mut rows: Vec<_> = t.0.into_iter().collect();
    rows.sort_unstable_by_key(|(k, _)| *k);
    for chunk in rows.chunks(WRITE_CHUNK) {
        let mut txn = hot.rw()?;
        for ((kind, e, c), v) in chunk {
            txn.put(
                Table::RowCounts,
                &rowcount::key(*kind, *e, *c),
                &rowcount::encode_val(*v),
            )?;
        }
        txn.commit()?;
    }
    if rep.unrendered == 0 {
        let mut txn = hot.rw()?;
        rowcount::set_ready(&mut txn, next_txnum)?;
        txn.commit()?;
        rep.ready = true;
    }
    rep.write_secs = t1.elapsed().as_secs_f64();
    Ok(rep)
}
