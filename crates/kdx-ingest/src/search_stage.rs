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
//! - Commit every `commit_secs` (docs, then `built_through_height` in the manifest, under the
//!   rebuild lock). A rebuild that swapped `search/` (new `built_at`) makes the stage reopen.

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
}

/// Blocks per chain per step (one hot read txn each).
const STEP_BLOCKS: u32 = 200;
/// Block hashes remembered per chain for reorg detection.
const RING: usize = 256;
/// Heights re-read below `built_through_height` at startup (a reorg after the rebuild's hot
/// pass; existing bodies are skipped by hash).
const STARTUP_REWIND: u32 = 128;
const POLL: Duration = Duration::from_secs(1);
const RETRY: Duration = Duration::from_secs(60);
/// The stage's writer heap (one thread).
const STAGE_HEAP_MB: usize = 64;

/// Runs until `stop`; failures are logged and retried (search never stops ingest).
pub async fn run(ctx: Arc<Ctx>, o: SearchStageOpts, stop: CancellationToken) -> Result<()> {
    loop {
        let (c, o2, s) = (ctx.clone(), o.clone(), stop.clone());
        let r = tokio::task::spawn_blocking(move || run_blocking(&c, &o2, &s)).await;
        match r {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => {
                tracing::error!(
                    error = format!("{e:#}"),
                    "search stage failed; retrying in 60 s"
                )
            }
            Err(e) => tracing::error!(error = %e, "search stage panicked; retrying in 60 s"),
        }
        tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            _ = tokio::time::sleep(RETRY) => {}
        }
    }
}

fn run_blocking(ctx: &Ctx, o: &SearchStageOpts, stop: &CancellationToken) -> Result<()> {
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
            if st.swapped()? {
                tracing::info!("search stage: index rebuilt elsewhere, reopening");
                break;
            }
            let n = st.step(ctx)?;
            st.maybe_commit()?;
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

    fn maybe_commit(&mut self) -> Result<()> {
        if self.last_commit.elapsed() >= self.commit_every {
            self.commit()?;
        }
        Ok(())
    }

    /// Docs first, then (under the rebuild lock, unless a rebuild holds it) the manifest's
    /// `built_through_height`: the lowest height every chain has been read through.
    fn commit(&mut self) -> Result<()> {
        self.last_commit = Instant::now();
        if self.dirty {
            self.w.commit()?;
            self.dirty = false;
        }
        let through = self
            .chains
            .iter()
            .map(|c| c.next.saturating_sub(1))
            .min()
            .unwrap_or(0);
        if through <= self.m.built_through_height && self.m.docs == self.w.num_docs() {
            return Ok(());
        }
        let Ok(_lock) = kdx_search::build::lock(&self.data_dir) else {
            return Ok(());
        };
        if self.swapped()? {
            return Ok(());
        }
        self.m.built_through_height = self.m.built_through_height.max(through);
        self.m.docs = self.w.num_docs();
        self.m.bytes = dir_bytes(&self.dir)?;
        self.m.commit(&self.dir)
    }
}
