//! `kda-indexer reconcile-holders`: build the module holders table (`kdx_hot::holders`) of an
//! existing store from every record, frozen and hot, and set its readiness marker.
//!
//! - A new store needs none of this: backfill and the tail keep the table from block 0. This is
//!   the backfill for a store built before the table existed (and after `rebuild-range` or a
//!   rollback to an older build, which clear the marker).
//! - Offline (the caller holds the data-dir lock), like `reconcile-counts`: the frozen TxNums
//!   are cut into slices that `threads` workers read in parallel, each into its own tally merged
//!   into one; then the hot records are read in one snapshot. The table is replaced and the
//!   marker and per-chain stamps are set last, so a run stopped anywhere leaves it unserved.
//! - Memory: one map entry per holder row (about 1.2M on mainnet today, 9 B key + 8 B value plus
//!   map overhead: about 60–100 MB).
//! - `--check` builds the tally and compares it with the stored table (read-only): the
//!   "backfill equals ingest" check on a live store.
//! - A record whose output does not render is counted in `unrendered` and leaves the marker
//!   unset (its transfers cannot be read).

use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use kdx_hot::holders::{self, Holder, Tally};
use kdx_hot::{HotDict, HotStore, RoTxn, Table, Txn};
use kdx_segment::Segments;
use kdx_types::record::{decode_tx, TxRecord};
use kdx_types::{MemDict, TxKey};
use serde::Serialize;

/// Frozen TxNums one worker takes at a time.
pub const SLICE_TXS: u64 = 1 << 16;
/// Holder rows written per write txn when the table is replaced.
pub const WRITE_CHUNK: usize = 1 << 18;

#[derive(Debug, Clone, Default, Serialize)]
pub struct Report {
    pub frozen_txs: u64,
    pub hot_txs: u64,
    pub unrendered: u64,
    /// Holder rows of the rebuilt table.
    pub rows: u64,
    /// `--check`: rows that differ between the walk and the stored table (both directions).
    pub differing: Option<u64>,
    pub threads: usize,
    pub walk_secs: f64,
    pub write_secs: f64,
    pub ready: bool,
}

fn add_record(t: &mut Tally, r: &TxRecord, dict: &MemDict) -> bool {
    match kdx_decode::rowcount::holder_sides(r, dict) {
        Ok(sides) => {
            for (a, m) in sides {
                t.add(a, m, r.key);
            }
            true
        }
        Err(_) => false,
    }
}

fn walk_frozen(
    segs: &Segments,
    snap: &MemDict,
    txnums: Range<u64>,
    heights: Range<u32>,
    threads: usize,
    stop: &(dyn Fn() -> bool + Sync),
    total: &mut Tally,
) -> Result<(u64, u64)> {
    let slices = txnums.end.saturating_sub(txnums.start).div_ceil(SLICE_TXS) as usize;
    let next = AtomicUsize::new(0);
    let (seen, bad) = (AtomicU64::new(0), AtomicU64::new(0));
    let all = std::sync::Mutex::new(std::mem::take(total));
    std::thread::scope(|sc| -> Result<()> {
        let mut hs = Vec::new();
        for _ in 0..threads.min(slices.max(1)) {
            let (next, seen, bad, all, txnums, heights) =
                (&next, &seen, &bad, &all, &txnums, &heights);
            hs.push(sc.spawn(move || -> Result<()> {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= slices || stop() {
                        return Ok(());
                    }
                    let a = txnums.start + i as u64 * SLICE_TXS;
                    let b = txnums.end.min(a + SLICE_TXS);
                    let (mut t, mut n, mut nb) = (Tally::default(), 0u64, 0u64);
                    for txnum in a..b {
                        let Some(key) = segs.txkey(txnum) else {
                            continue;
                        };
                        if !heights.contains(&key.height()) {
                            continue;
                        }
                        let r = segs
                            .tx(txnum)?
                            .with_context(|| format!("frozen TxNum {txnum} has no record"))?;
                        n += 1;
                        if !add_record(&mut t, &r, snap) {
                            nb += 1;
                        }
                    }
                    seen.fetch_add(n, Ordering::Relaxed);
                    bad.fetch_add(nb, Ordering::Relaxed);
                    all.lock().unwrap_or_else(|e| e.into_inner()).merge(t);
                }
            }));
        }
        for h in hs {
            h.join().expect("holders worker panicked")?;
        }
        Ok(())
    })?;
    *total = all.into_inner().unwrap_or_else(|e| e.into_inner());
    anyhow::ensure!(!stop(), "stopped");
    Ok((seen.into_inner(), bad.into_inner()))
}

fn frozen_end(segs: &Segments, start: u64, frontier: u32) -> u64 {
    segs.last_txnum_below_height(frontier)
        .map_or(start, |t| t + 1)
}

/// Every record's first transfers: the frozen part with no hot txn open, then the heights
/// frozen meanwhile and the hot part in one snapshot (returned, still open).
pub fn tally(
    segdir: &Path,
    hot: &HotStore,
    threads: usize,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<(Tally, Report, RoTxn)> {
    let t0 = Instant::now();
    let threads = threads.max(1);
    let (snap, frontier) = {
        let ro = hot.ro()?;
        let snap: Arc<MemDict> = Arc::new(HotDict::load(&ro)?.mem().clone());
        (snap, ro.frozen_frontier()?)
    };
    let mut total = Tally::default();
    let mut rep = Report {
        threads,
        ..Report::default()
    };
    let (start, end) = {
        let segs = Segments::open_without_filters(segdir, snap.clone())?;
        let start = segs.manifest.entries.first().map_or(0, |e| e.first_txnum);
        let end = frozen_end(&segs, start, frontier);
        let (n, bad) = walk_frozen(
            &segs,
            &snap,
            start..end,
            0..frontier,
            threads,
            stop,
            &mut total,
        )?;
        rep.frozen_txs += n;
        rep.unrendered += bad;
        (start, end)
    };
    let ro = hot.ro()?;
    let snap: Arc<MemDict> = Arc::new(HotDict::load(&ro)?.mem().clone());
    let now = ro.frozen_frontier()?;
    anyhow::ensure!(
        now >= frontier,
        "the frozen frontier went back from {frontier} to {now}"
    );
    if now > frontier {
        let segs = Segments::open_without_filters(segdir, snap.clone())?;
        let end2 = frozen_end(&segs, start, now);
        let (n, bad) = walk_frozen(
            &segs,
            &snap,
            end..end2,
            frontier..now,
            threads,
            stop,
            &mut total,
        )?;
        rep.frozen_txs += n;
        rep.unrendered += bad;
    }
    let from = TxKey::new(now, 0, 0).to_be_bytes();
    for (k, v) in ro.scan(Table::TailTxs, &from, |_| true)? {
        let r = decode_tx(&v).with_context(|| format!("hot record {k:?}"))?;
        if r.key.height() < now {
            continue;
        }
        rep.hot_txs += 1;
        if !add_record(&mut total, &r, &snap) {
            rep.unrendered += 1;
        }
    }
    rep.rows = total.len() as u64;
    rep.walk_secs = t0.elapsed().as_secs_f64();
    Ok((total, rep, ro))
}

/// Rows in one sorted list and not the other.
pub fn differing(a: &[Holder], b: &[Holder]) -> u64 {
    let key = |h: &Holder| holders::hold_key(h.module, h.chain, h.first, h.account);
    let (mut i, mut j, mut d) = (0usize, 0usize, 0u64);
    while i < a.len() || j < b.len() {
        match (a.get(i), b.get(j)) {
            (Some(x), Some(y)) if key(x) == key(y) => {
                i += 1;
                j += 1;
            }
            (Some(x), Some(y)) if key(x) < key(y) => {
                d += 1;
                i += 1;
            }
            (Some(_), Some(_)) | (None, Some(_)) => {
                d += 1;
                j += 1;
            }
            (Some(_), None) => {
                d += 1;
                i += 1;
            }
            (None, None) => break,
        }
    }
    d
}

/// Startup check of `run`: a set marker whose table does not follow the tips (a build without
/// the table moved them) is cleared and logged. Nothing is written on a read-only store.
pub fn startup_guard(hot: &HotStore) -> Result<Option<String>> {
    if hot.is_read_only() {
        return Ok(None);
    }
    let mut txn = hot.rw()?;
    let why = holders::guard(&mut txn)?;
    if why.is_some() {
        txn.commit()?;
    }
    Ok(why)
}

/// Rebuild the holders table and set its marker (or, with `check`, only compare). The caller
/// holds the data-dir lock (not for `check`, which writes nothing).
pub fn reconcile(
    segdir: &Path,
    hot: &HotStore,
    threads: usize,
    check: bool,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Report> {
    if !check {
        let mut txn = hot.rw()?;
        holders::clear_ready(&mut txn)?;
        txn.commit()?;
    }
    let (t, mut rep, ro) = tally(segdir, hot, threads, stop)?;
    let want = t.into_sorted();
    if check {
        let have = holders::read_all(&ro)?;
        rep.differing = Some(differing(&want, &have));
        rep.ready = holders::ready(&ro)?;
        return Ok(rep);
    }
    drop(ro);
    let t1 = Instant::now();
    let next_txnum = {
        let mut txn = hot.rw()?;
        holders::clear_ready(&mut txn)?;
        holders::clear_table(&mut txn)?;
        let n = txn.next_txnum()?;
        txn.commit()?;
        n
    };
    for chunk in want.chunks(WRITE_CHUNK) {
        let mut t = Tally::default();
        for h in chunk {
            t.add(h.account, h.module, h.first);
        }
        let mut txn = hot.rw()?;
        holders::add_tally(&mut txn, &t)?;
        txn.commit()?;
    }
    // A partial store (`run --start-height`, or a first range above height 0) misses the
    // holders below its start: the table is rebuilt but not marked ready, as the chain
    // counters in `row_counts::reconcile`.
    let partial = hot.ro()?.meta(crate::freezer::META_PARTIAL_FROM)?.is_some()
        || kdx_segment::manifest::Manifest::load(segdir)?
            .and_then(|m| m.entries.first().map(|e| e.start > 0))
            .unwrap_or(false);
    if rep.unrendered == 0 && !partial {
        let mut txn = hot.rw()?;
        holders::set_ready(&mut txn, next_txnum)?;
        holders::stamp_all(&mut txn)?;
        holders::stamp_txnum(&mut txn)?;
        txn.commit()?;
        rep.ready = true;
    }
    rep.write_secs = t1.elapsed().as_secs_f64();
    Ok(rep)
}
