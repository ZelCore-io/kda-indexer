//! `kda-indexer reconcile-counts` (PR #48 part 2): build the stored per-key row counts
//! (`kdx_hot::rowcount`) from every record, frozen and hot, and set the readiness marker.
//!
//! - Offline: the caller holds the data-dir lock, so neither the tail nor the freezer moves.
//! - The frozen TxNums below the frontier are cut into slices that `threads` workers read and
//!   count in parallel (the PR #40 history-walk layout: one `Segments` shared by the workers,
//!   one dictionary snapshot), each slice into its own tally merged into one; the hot `TailTxs` rows at or above the
//!   frontier are counted here. The tallies are merged and replace the table.
//! - Idempotent: the table is rebuilt from scratch on each run. The marker is cleared before
//!   counting starts and set last (with every chain's `row_counts_at`), so a run that stops
//!   at any point leaves the counts untrusted (readers walk).
//! - No hot read txn is held over the frozen walk (it reads segments and a dictionary
//!   snapshot). One later snapshot reads the frozen heights the freezer moved meanwhile, the
//!   hot records and, for `--check`, the stored table: both sides of the compare come from
//!   it, so `differing` is 0 on a healthy store even beside the running service.
//! - Memory: the tally keeps one map per kind; the write and the compare turn one kind at a
//!   time into a sorted run (that kind's map and run coexist: the peak is per kind), and the
//!   compare streams the stored table in chunks inside one hot read txn.
//! - A record whose output does not render is counted nowhere and leaves the marker unset
//!   (the walk fails on that tx too; stored counts would hide it).
//! - The same pass builds the per-chain counters (`kdx_hot::chaincount`: user txs and gas in
//!   KDA, served by `networkInfo.countersOfEachChain`), replaces that table and sets its own
//!   marker (`chain_counters_ready`) in the txn that sets the row counts' marker and stamps.
//!   It needs no rendering, so unrendered records do not hold it back; only a sum past
//!   `u128` KDA does.

use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use kdx_decode::rowcount::CountKind;
use kdx_hot::chaincount;
use kdx_hot::rowcount::{self, Tally};
use kdx_hot::{HotDict, HotStore, RoTxn, Table, Txn};
use kdx_segment::Segments;
use kdx_types::record::decode_tx;
use kdx_types::{MemDict, TxKey};
use serde::Serialize;
use std::ops::Range;

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
    /// User txs counted into the per-chain counters (coinbase excluded).
    pub chain_txs: u64,
    /// User txs whose `meta.gasPrice` is missing or malformed (counted, 0 KDA of gas).
    pub unpriced: u64,
    /// `--check`: per-chain counter rows that differ from the rebuilt ones.
    pub chain_counters_differing: Option<u64>,
    /// Whether the per-chain counters may answer after the run.
    pub chain_counters_ready: bool,
}

/// What the walk builds: the row counts and the per-chain counters.
#[derive(Debug, Default)]
pub struct Both {
    pub rows: Tally,
    pub chains: chaincount::Tally,
}

impl Both {
    /// Add `r`; `false` when its output does not render (the chain counters still take it).
    fn add(&mut self, r: &kdx_types::record::TxRecord, dict: &dyn kdx_types::Dict) -> bool {
        self.chains.add(r);
        self.rows.add(r, dict)
    }

    fn merge(&mut self, o: Both) {
        self.rows.merge(o.rows);
        self.chains.merge(o.chains);
    }
}

/// Count the frozen records with TxNum in `txnums` and height in `heights` into `total`, on
/// `threads` workers. Returns (records, unrendered).
fn count_frozen(
    segs: &Segments,
    snap: &MemDict,
    txnums: Range<u64>,
    heights: Range<u32>,
    threads: usize,
    stop: &(dyn Fn() -> bool + Sync),
    total: &mut Both,
) -> Result<(u64, u64)> {
    let slices = (txnums.end.saturating_sub(txnums.start)).div_ceil(SLICE_TXS) as usize;
    let next = AtomicUsize::new(0);
    let (frozen, unrendered) = (AtomicU64::new(0), AtomicU64::new(0));
    // Each slice is counted into its own small tally and merged into `all` under the lock,
    // so the peak memory is the merged table plus one slice per worker.
    let all = std::sync::Mutex::new(std::mem::take(total));
    std::thread::scope(|sc| -> Result<()> {
        let mut hs = Vec::new();
        for _ in 0..threads.min(slices.max(1)) {
            let (next, frozen, unrendered, all, txnums, heights) =
                (&next, &frozen, &unrendered, &all, &txnums, &heights);
            hs.push(sc.spawn(move || -> Result<()> {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= slices || stop() {
                        return Ok(());
                    }
                    let a = txnums.start + i as u64 * SLICE_TXS;
                    let b = txnums.end.min(a + SLICE_TXS);
                    let (mut t, mut n, mut bad) = (Both::default(), 0u64, 0u64);
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
    *total = all.into_inner().unwrap_or_else(|e| e.into_inner());
    anyhow::ensure!(!stop(), "stopped");
    Ok((frozen.into_inner(), unrendered.into_inner()))
}

/// TxNum end of the frozen records below height `frontier`.
fn frozen_end(segs: &Segments, start: u64, frontier: u32) -> u64 {
    segs.last_txnum_below_height(frontier)
        .map_or(start, |t| t + 1)
}

/// Count every record into a [`Tally`]: the frozen part on `threads` workers with no hot
/// txn open, then, in one snapshot (returned, still open), the heights frozen meanwhile and
/// the hot part. `stop` is polled between slices.
fn tally_in(
    segdir: &Path,
    hot: &HotStore,
    threads: usize,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<(Both, Report, RoTxn)> {
    let t0 = Instant::now();
    let threads = threads.max(1);
    let (snap, frontier) = {
        let ro = hot.ro()?;
        let snap: Arc<MemDict> = Arc::new(HotDict::load(&ro)?.mem().clone());
        (snap, ro.frozen_frontier()?)
    };
    let segs = Segments::open_without_filters(segdir, snap.clone())?;
    let start = segs.manifest.entries.first().map_or(0, |e| e.first_txnum);
    let end = frozen_end(&segs, start, frontier);
    drop(segs);
    let mut total = Both::default();
    let (frozen, unrendered) = count_frozen_at(
        segdir,
        &snap,
        start..end,
        0..frontier,
        threads,
        stop,
        &mut total,
    )?;
    let mut rep = Report {
        threads,
        frozen_txs: frozen,
        unrendered,
        ..Report::default()
    };

    // One snapshot for the rest (and the caller's compare).
    let ro = hot.ro()?;
    let snap: Arc<MemDict> = Arc::new(HotDict::load(&ro)?.mem().clone());
    let now = ro.frozen_frontier()?;
    anyhow::ensure!(
        now >= frontier,
        "the frozen frontier went back from {frontier} to {now}"
    );
    if now > frontier {
        // Frozen by the running service while the walk ran.
        let segs = Segments::open_without_filters(segdir, snap.clone())?;
        let end2 = frozen_end(&segs, start, now);
        let (n, bad) = count_frozen(
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
        if !total.add(&r, &*snap) {
            rep.unrendered += 1;
        }
    }
    rep.keys = total.rows.len() as u64;
    rep.chain_txs = total.chains.chains.values().map(|c| c.user_txs).sum();
    rep.unpriced = total.chains.unpriced;
    rep.count_secs = t0.elapsed().as_secs_f64();
    rep.rate = (rep.frozen_txs + rep.hot_txs) as f64 / rep.count_secs.max(1e-9);
    Ok((total, rep, ro))
}

fn count_frozen_at(
    segdir: &Path,
    snap: &Arc<MemDict>,
    txnums: Range<u64>,
    heights: Range<u32>,
    threads: usize,
    stop: &(dyn Fn() -> bool + Sync),
    total: &mut Both,
) -> Result<(u64, u64)> {
    let segs = Segments::open_without_filters(segdir, snap.clone())?;
    count_frozen(&segs, snap, txnums, heights, threads, stop, total)
}

/// Count every record into a [`Tally`] (see `tally_in`; the snapshot is closed).
pub fn tally(
    segdir: &Path,
    hot: &HotStore,
    threads: usize,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<(Tally, Report)> {
    let (t, rep, _ro) = tally_in(segdir, hot, threads, stop)?;
    Ok((t.rows, rep))
}

/// [`tally`] with the per-chain counters.
pub fn tally_both(
    segdir: &Path,
    hot: &HotStore,
    threads: usize,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<(Both, Report)> {
    let (t, rep, _ro) = tally_in(segdir, hot, threads, stop)?;
    Ok((t, rep))
}

/// Clear the readiness marker now (a run stopped anywhere leaves it cleared, review M-4).
fn clear_marker(hot: &HotStore) -> Result<()> {
    let mut txn = hot.rw()?;
    rowcount::clear_ready(&mut txn)?;
    chaincount::clear_ready(&mut txn)?;
    txn.commit()
}

/// Startup check of `run` and the tail (review I-1): a set marker whose counts do not follow
/// every chain's hot tip (a build without stored counts ran on this data dir) is cleared, and
/// the reason logged. Returns it. Nothing is written on a read-only store.
pub fn startup_guard(hot: &HotStore) -> Result<Option<String>> {
    if hot.is_read_only() {
        return Ok(None);
    }
    let mut txn = hot.rw()?;
    let why = rowcount::guard(&mut txn)?;
    let why = chaincount::guard(&mut txn)?.or(why);
    if why.is_some() {
        txn.commit()?;
    }
    Ok(why)
}

/// Rebuild `RowCounts` and set the marker (or, with `check`, only compare). The caller holds
/// the data-dir lock (not for `check`, which writes nothing).
pub fn reconcile(
    segdir: &Path,
    hot: &HotStore,
    threads: usize,
    check: bool,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Report> {
    if !check {
        clear_marker(hot)?;
    }
    let (
        Both {
            rows: mut t,
            chains,
        },
        mut rep,
        ro,
    ) = tally_in(segdir, hot, threads, stop)?;
    if check {
        let mut diff = rowcount::unknown_kind_rows(&ro)?;
        for k in CountKind::ALL {
            let run = t.take_sorted(k);
            diff += rowcount::differing(&ro, k, &run)?;
        }
        rep.differing = Some(diff);
        rep.ready = rowcount::ready(&ro)?;
        rep.chain_counters_differing = Some(chaincount::differing(&ro, &chains)?);
        rep.chain_counters_ready = chaincount::ready(&ro)?;
        return Ok(rep);
    }
    drop(ro);
    let t1 = Instant::now();
    let next_txnum = {
        let mut txn = hot.rw()?;
        rowcount::clear_ready(&mut txn)?;
        rowcount::clear_table(&mut txn)?;
        chaincount::clear_ready(&mut txn)?;
        let n = txn.next_txnum()?;
        txn.commit()?;
        n
    };
    // Kinds in key order, each written sorted in chunks; one kind's run alive at a time.
    for k in CountKind::ALL {
        let run = t.take_sorted(k);
        for chunk in run.chunks(WRITE_CHUNK) {
            let mut txn = hot.rw()?;
            for ((e, c), v) in chunk {
                txn.put(
                    Table::RowCounts,
                    &rowcount::key(k, *e, *c),
                    &rowcount::encode_val(*v),
                )?;
            }
            txn.commit()?;
        }
    }
    // One txn: the per-chain counters (twenty rows), both markers and the stamps.
    let mut txn = hot.rw()?;
    chaincount::replace(&mut txn, &chains)?;
    // A partial store (from a height above 0) has no all-time counters to serve.
    let partial = txn.meta(crate::freezer::META_PARTIAL_FROM)?.is_some();
    if !chains.overflow && !partial {
        chaincount::set_ready(&mut txn, next_txnum)?;
        chaincount::stamp_all(&mut txn)?;
        rep.chain_counters_ready = true;
    }
    if rep.unrendered == 0 {
        rowcount::set_ready(&mut txn, next_txnum)?;
        rep.ready = true;
    }
    if rep.ready {
        rowcount::stamp_all(&mut txn)?;
    }
    txn.commit()?;
    rep.write_secs = t1.elapsed().as_secs_f64();
    Ok(rep)
}
