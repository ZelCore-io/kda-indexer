//! Tail stage 7 (spec §4.1, plan Task 8): code search docs for new exec txs. Runs only in the
//! writer process (`run`, `tail`; B-5), on its own blocking thread.
//!
//! - No `search/manifest.json`: build the index first (`search rebuild`, mode from config) in
//!   the background; ingest goes on meanwhile.
//! - Then per chain, every block from `built_through_height + 1` (minus a rewind margin at
//!   startup) up to the chain's tail checkpoint, read from the hot store at or above the
//!   frozen frontier and from the segments below it: each exec tx whose body hash the index
//!   does not hold becomes a doc with `first_h = last_h = height` (fallback mode applies).
//! - A reorg is noticed by the hash of the last blocks read (a ring per chain): the chain is
//!   re-read from the first block that changed. Docs are never deleted; a body of a reorged
//!   block is harmless (expansion finds no tx).
//! - Periodic rebuild (`rebuild_every_secs`, `rebuild_window_heights`; R-P5P9-9 #5): a full
//!   rebuild into `search.new/` on its own thread while the stage keeps tailing the live
//!   index, in the live index's mode (or the configured explicit one), with the configured
//!   writer heap. Its swap takes the swap lock, so it never lands inside a stage commit, and
//!   the stage's next commit or `built_at` check reopens on the new index.
//! - Docs are buffered; every `commit_secs` (or at `COMMIT_BYTES` of buffered code) they are
//!   written and committed, merges run, then `built_through_height` goes into the manifest,
//!   all under the swap lock and only while `search/` still holds this stage's index (same
//!   `built_at`). A rebuild that swapped `search/` makes the stage drop its buffer and reopen:
//!   tantivy writes by path, so a commit after the swap would land in the new index.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use kdx_hot::tables::block_key;
use kdx_hot::{checkpoint, Stage, Table, Txn};
use kdx_search::manifest::dir_bytes;
use kdx_search::{
    exec_code, search_dir, BuildOpts, Doc, Mode, ModeSel, SearchManifest, SearchWriter,
};
use kdx_segment::Segments;
use kdx_types::record::{decode_block, decode_tx, TxRecord};
use kdx_types::{blake2b_128, Hash32, MemDict, TxKey};
use tokio_util::sync::CancellationToken;

use crate::Ctx;

#[derive(Debug, Clone)]
pub struct SearchStageOpts {
    pub data_dir: PathBuf,
    pub mode: ModeSel,
    pub commit_secs: u64,
    pub build_threads: usize,
    pub heap_mb: usize,
    /// Periodic rebuild: seconds since the index was built (0 = off).
    pub rebuild_every_secs: u64,
    /// Periodic rebuild: upper expansion window in heights (0 = off).
    pub rebuild_window_heights: u32,
}

/// Blocks per chain per step (one hot read txn each).
const STEP_BLOCKS: u32 = 200;
/// Block hashes remembered per chain for reorg detection.
const RING: usize = 256;
/// Heights re-read below `built_through_height` at startup (a reorg after the rebuild's hot
/// pass; existing bodies are skipped by hash).
const STARTUP_REWIND: u32 = 128;
const POLL: Duration = Duration::from_secs(1);
/// First retry delay after a failure; doubles per consecutive failure up to `RETRY_MAX` (a
/// rebuild that keeps failing must not re-run its census every minute).
const RETRY: Duration = Duration::from_secs(60);
const RETRY_MAX: Duration = Duration::from_secs(3600);
/// The stage's writer heap (one thread).
const STAGE_HEAP_MB: usize = 64;
/// Buffered code bytes that force a commit before `commit_secs`.
const COMMIT_BYTES: usize = 32 << 20;
/// How often the stage checks whether a periodic rebuild is due (and the least time between
/// two attempts).
const REBUILD_CHECK: Duration = Duration::from_secs(600);
const REBUILD_RETRY: Duration = Duration::from_secs(3600);

/// A periodic rebuild running on its own thread.
struct Periodic {
    flag: Arc<AtomicBool>,
    handle: std::thread::JoinHandle<Result<kdx_search::RebuildReport>>,
}

impl Periodic {
    fn finish(self) {
        match self.handle.join() {
            Ok(Ok(rep)) => match serde_json::to_string(&rep) {
                Ok(r) => tracing::info!(report = %r, "search: periodic rebuild done"),
                Err(_) => tracing::info!("search: periodic rebuild done"),
            },
            Ok(Err(e)) => tracing::warn!(
                error = format!("{e:#}"),
                "search: periodic rebuild failed (retried later)"
            ),
            Err(_) => tracing::error!("search: periodic rebuild panicked"),
        }
    }
}

/// The rebuild's mode: an explicit `full` or `fallback` with a threshold from the config, else
/// the live index's (no projection run).
fn periodic_mode(cfg: ModeSel, live: Mode) -> ModeSel {
    match (cfg, live) {
        (ModeSel::Full, _) => ModeSel::Full,
        (ModeSel::Fallback(l), _) if l > 0 => ModeSel::Fallback(l),
        (_, Mode::Full) => ModeSel::Full,
        (_, Mode::Fallback { min_len }) => ModeSel::Fallback(min_len.max(1)),
    }
}

/// Start a periodic rebuild when one is due and none ran in the last `REBUILD_RETRY`.
fn maybe_start_periodic(
    ctx: &Arc<Ctx>,
    o: &SearchStageOpts,
    m: &SearchManifest,
    last_try: &mut Option<Instant>,
) -> Result<Option<Periodic>> {
    if o.rebuild_every_secs == 0 && o.rebuild_window_heights == 0 {
        return Ok(None);
    }
    if last_try.is_some_and(|t| t.elapsed() < REBUILD_RETRY) {
        return Ok(None);
    }
    let tip = kdx_search::census::hot_tip(&ctx.hot)?;
    let now = kdx_search::manifest::now_secs();
    if !kdx_search::build::rebuild_due(m, now, tip, o.rebuild_every_secs, o.rebuild_window_heights)
    {
        return Ok(None);
    }
    *last_try = Some(Instant::now());
    let flag = Arc::new(AtomicBool::new(false));
    let bo = BuildOpts {
        data_dir: o.data_dir.clone(),
        mode: periodic_mode(o.mode, m.mode()),
        threads: o.build_threads,
        heap_mb: o.heap_mb,
        trial_docs: 1_000_000,
        stop: flag.clone(),
    };
    tracing::info!(
        built_at = m.built_at,
        rebuilt_through = m.rebuilt_through_height,
        tip,
        "search: periodic rebuild starting"
    );
    let c = ctx.clone();
    let handle = std::thread::Builder::new()
        .name("kdx-search-rebuild".into())
        .spawn(move || kdx_search::rebuild(&bo, &c.hot))?;
    Ok(Some(Periodic { flag, handle }))
}

/// Runs until `stop`; failures are logged and retried (search never stops ingest).
pub async fn run(ctx: Arc<Ctx>, o: SearchStageOpts, stop: CancellationToken) -> Result<()> {
    let mut delay = RETRY;
    loop {
        let (c, o2, s) = (ctx.clone(), o.clone(), stop.clone());
        let t0 = Instant::now();
        let r = tokio::task::spawn_blocking(move || run_blocking(&c, &o2, &s)).await;
        // A stage that ran for a while before failing starts the back-off again.
        if t0.elapsed() > RETRY_MAX {
            delay = RETRY;
        }
        let secs = delay.as_secs();
        match r {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => {
                tracing::error!(
                    error = format!("{e:#}"),
                    retry_secs = secs,
                    "search stage failed"
                )
            }
            Err(e) => tracing::error!(error = %e, retry_secs = secs, "search stage panicked"),
        }
        tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            _ = tokio::time::sleep(delay) => {}
        }
        delay = (delay * 2).min(RETRY_MAX);
    }
}

fn run_blocking(ctx: &Arc<Ctx>, o: &SearchStageOpts, stop: &CancellationToken) -> Result<()> {
    let dir = search_dir(&o.data_dir);
    if SearchManifest::load(&dir)?.is_none() {
        let flag = Arc::new(AtomicBool::new(false));
        let bo = BuildOpts {
            data_dir: o.data_dir.clone(),
            mode: o.mode,
            threads: o.build_threads,
            heap_mb: o.heap_mb,
            trial_docs: 1_000_000,
            stop: flag.clone(),
        };
        tracing::info!("search: no index yet, rebuilding in the background (B-5)");
        let watcher = {
            let (stop, flag) = (stop.clone(), flag.clone());
            std::thread::spawn(move || {
                while !stop.is_cancelled() && !flag.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(POLL);
                }
                flag.store(true, std::sync::atomic::Ordering::Relaxed);
            })
        };
        let r = kdx_search::rebuild(&bo, &ctx.hot);
        flag.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = watcher.join();
        if stop.is_cancelled() {
            return Ok(());
        }
        let rep = r?;
        tracing::info!(report = %serde_json::to_string(&rep)?, "search: rebuild done");
    }
    let mut periodic: Option<Periodic> = None;
    let mut last_try: Option<Instant> = None;
    let r = tail_loop(ctx, o, stop, &mut periodic, &mut last_try);
    if let Some(p) = periodic.take() {
        p.flag.store(true, std::sync::atomic::Ordering::Relaxed);
        p.finish();
    }
    r
}

fn tail_loop(
    ctx: &Arc<Ctx>,
    o: &SearchStageOpts,
    stop: &CancellationToken,
    periodic: &mut Option<Periodic>,
    last_try: &mut Option<Instant>,
) -> Result<()> {
    let mut checked = Instant::now();
    loop {
        let mut st = StageState::open(ctx, o)?;
        tracing::info!(
            built_through = st.m.built_through_height,
            docs = st.m.docs,
            mode = %st.m.mode,
            "search stage: tailing"
        );
        loop {
            if stop.is_cancelled() {
                st.commit()?;
                return Ok(());
            }
            if periodic.as_ref().is_some_and(|p| p.handle.is_finished()) {
                if let Some(p) = periodic.take() {
                    p.finish();
                }
            }
            if periodic.is_none() && checked.elapsed() >= REBUILD_CHECK {
                checked = Instant::now();
                *periodic = maybe_start_periodic(ctx, o, &st.m, last_try)?;
            }
            if st.swapped()? {
                tracing::info!("search stage: index rebuilt elsewhere, reopening");
                break;
            }
            let n = st.step(ctx)?;
            if !st.maybe_commit()? {
                tracing::info!("search stage: index rebuilt meanwhile, reopening");
                break;
            }
            if n == 0 {
                std::thread::sleep(POLL);
            }
        }
    }
}

#[derive(Default)]
struct ChainState {
    next: u32,
    ring: VecDeque<(u32, Hash32)>,
}

struct StageState {
    data_dir: PathBuf,
    dir: PathBuf,
    segdir: PathBuf,
    w: SearchWriter,
    m: SearchManifest,
    mode: Mode,
    seg: Option<Segments>,
    chains: Vec<ChainState>,
    commit_every: Duration,
    last_commit: Instant,
    dirty: bool,
}

impl StageState {
    fn open(ctx: &Ctx, o: &SearchStageOpts) -> Result<StageState> {
        let dir = search_dir(&o.data_dir);
        let m = SearchManifest::load(&dir)?.context("search/manifest.json vanished")?;
        let w = SearchWriter::open(&dir, STAGE_HEAP_MB)?;
        let start = (m.built_through_height + 1).saturating_sub(STARTUP_REWIND);
        let chains = (0..kdx_segment::SLOT_WIDTH)
            .map(|_| ChainState {
                next: start,
                ring: VecDeque::new(),
            })
            .collect();
        Ok(StageState {
            data_dir: o.data_dir.clone(),
            dir,
            segdir: ctx.segdir.clone(),
            mode: m.mode(),
            w,
            m,
            seg: None,
            chains,
            commit_every: Duration::from_secs(o.commit_secs.max(1)),
            last_commit: Instant::now(),
            dirty: false,
        })
    }

    /// `search/` now holds another rebuild's index.
    fn swapped(&self) -> Result<bool> {
        Ok(match SearchManifest::load(&self.dir) {
            Ok(Some(m)) => m.built_at != self.m.built_at,
            // Mid-swap: wait for the next step.
            Ok(None) => false,
            Err(e) => return Err(e),
        })
    }

    /// Segments covering `h` (opened or refreshed when the frontier is at or below `h`).
    fn segments_for(&mut self, h: u32) -> Result<&Segments> {
        let fresh = match self.seg.take() {
            Some(s) if s.frontier() > h => s,
            Some(s) => s.refresh()?.unwrap_or(s),
            None => Segments::open(&self.segdir, Arc::new(MemDict::new()))?,
        };
        anyhow::ensure!(
            fresh.frontier() > h,
            "height {h} is below the hot frozen frontier but not in the segments (frontier {})",
            fresh.frontier()
        );
        Ok(self.seg.insert(fresh))
    }

    /// Lowest height the store holds below the hot frozen frontier `frontier` (the first
    /// segment range's start; `frontier` when there is none: a store started at a height).
    fn floor(&mut self, frontier: u32) -> Result<u32> {
        let s = match self.seg.take() {
            Some(s) => s.refresh()?.unwrap_or(s),
            None => Segments::open(&self.segdir, Arc::new(MemDict::new()))?,
        };
        let lo = s
            .manifest
            .entries
            .first()
            .map_or(frontier, |e| e.start.min(frontier));
        self.seg = Some(s);
        Ok(lo)
    }

    fn consider(&mut self, rec: &TxRecord, h: u32) -> Result<()> {
        let Some(code) = exec_code(rec) else {
            return Ok(());
        };
        if !self.mode.admits(&code) {
            return Ok(());
        }
        let hash = blake2b_128(code.as_bytes());
        let d = Doc {
            hash,
            code,
            first_h: u64::from(h),
            last_h: u64::from(h),
        };
        if self.w.add(&d)? {
            self.dirty = true;
        }
        Ok(())
    }

    /// One pass over every chain; returns the blocks read.
    fn step(&mut self, ctx: &Ctx) -> Result<u32> {
        let mut n = 0;
        for c in 0..kdx_segment::SLOT_WIDTH as u8 {
            let txn = ctx.hot.ro()?;
            let Some((tip, _)) = checkpoint(&txn, Stage::Tail, c)? else {
                continue;
            };
            let frontier = txn.frozen_frontier()?;
            // Reorg: drop remembered blocks the hot store no longer has.
            while let Some(&(h, hash)) = self.chains[c as usize].ring.back() {
                if h < frontier {
                    break;
                }
                let same = match txn.get(Table::Headers, &block_key(c, h))? {
                    Some(v) => decode_block(&v)?.hash == hash,
                    None => false,
                };
                if same {
                    break;
                }
                let st = &mut self.chains[c as usize];
                st.ring.pop_back();
                st.next = st.next.min(h);
            }
            if self.chains[c as usize].next < frontier {
                let lo = self.floor(frontier)?;
                let st = &mut self.chains[c as usize];
                st.next = st.next.max(lo);
            }
            let from = self.chains[c as usize].next;
            let end = tip.min(from.saturating_add(STEP_BLOCKS - 1));
            for h in from..=end {
                let hash = if h >= frontier {
                    let Some(v) = txn.get(Table::Headers, &block_key(c, h))? else {
                        break;
                    };
                    let hash = decode_block(&v)?.hash;
                    let k0 = TxKey::new(h, c, 0);
                    let rows = txn.scan(Table::TailTxs, &k0.to_be_bytes(), |k| {
                        k.try_into()
                            .map(TxKey::from_be_bytes)
                            .is_ok_and(|t| t.height() == h && t.chain() == c)
                    })?;
                    for (_, v) in rows {
                        self.consider(&decode_tx(&v)?, h)?;
                    }
                    Some(hash)
                } else {
                    let seg = self.segments_for(h)?;
                    let hash = seg.block_hash(c, h)?;
                    let recs = match seg.block_txnums(c, h) {
                        Some(r) => r
                            .map(|t| seg.tx(t)?.context("frozen tx missing"))
                            .collect::<Result<Vec<_>>>()?,
                        None => Vec::new(),
                    };
                    for r in &recs {
                        self.consider(r, h)?;
                    }
                    hash
                };
                let st = &mut self.chains[c as usize];
                if let Some(x) = hash {
                    st.ring.push_back((h, x));
                    if st.ring.len() > RING {
                        st.ring.pop_front();
                    }
                }
                st.next = h + 1;
                n += 1;
            }
        }
        Ok(n)
    }

    /// False when a rebuild swapped `search/` (see `commit`).
    fn maybe_commit(&mut self) -> Result<bool> {
        if self.last_commit.elapsed() >= self.commit_every || self.w.pending_bytes() >= COMMIT_BYTES
        {
            return self.commit();
        }
        Ok(true)
    }

    /// Under the swap lock and only while `search/` holds this stage's index: the buffered
    /// docs, then the manifest's `built_through_height` (the lowest height every chain has
    /// been read through). False, with nothing written, when a rebuild swapped `search/`.
    fn commit(&mut self) -> Result<bool> {
        self.last_commit = Instant::now();
        let through = self
            .chains
            .iter()
            .map(|c| c.next.saturating_sub(1))
            .min()
            .unwrap_or(0);
        if !self.dirty && through <= self.m.built_through_height && self.m.docs == self.w.num_docs()
        {
            return Ok(true);
        }
        let (data_dir, built_at) = (self.data_dir.clone(), self.m.built_at);
        let done = kdx_search::build::with_current(&data_dir, built_at, || {
            if self.dirty {
                self.w.commit()?;
                self.dirty = false;
            }
            self.m.built_through_height = self.m.built_through_height.max(through);
            self.m.docs = self.w.num_docs();
            self.m.bytes = dir_bytes(&self.dir)?;
            self.m.commit(&self.dir)
        })?;
        Ok(done.is_some())
    }
}
