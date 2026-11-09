//! Backfill: canonical headers + bodies → raw spool (ETL) → decode/index in (height, chain)
//! order → `RangeWriter` → commit. Ranges fetch concurrently; build, intern and commit happen
//! strictly in range order.
//!
//! Commit order (builder call, `docs/format.md` "Write ordering"):
//! 1. `RangeWriter::finish`: files renamed into `segments/`, directory fsynced.
//! 2. One MDBX txn: the range's new dictionary names, `Headers` checkpoint per chain,
//!    `next_txnum`, `frozen_frontier`.
//! 3. `Manifest::commit`.
//!
//! The manifest is the source of truth. On start ([`resume`]) the `etl/` spool is cleared,
//! files and temp dirs the manifest does not list are deleted, and a hot frontier that
//! differs from the manifest's is reset to it (checkpoints re-derived from the last range).
//! Names interned by a range that never reached the manifest stay; the rebuild finds them
//! again in the same order, so ids stay deterministic.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use kdx_hot::checkpoint::clear_checkpoint;
use kdx_hot::dict;
use kdx_hot::{set_checkpoint, Stage, Txn};
use kdx_hot::{HotDict, Table};
use kdx_segment::files::{file_name, range_file_names, FileKind};
use kdx_segment::manifest::MANIFEST;
use kdx_segment::{
    Manifest, RangeEntry, RangeInfo, RangeKind, RangeReader, RangeWriter, WriteOpts,
};
use kdx_types::Hash32;
use serde::Serialize;

use crate::canon::{HeaderMode, WalkTiming, Walker};
use crate::ctx::Ctx;
use crate::disk;
use crate::etl::{self, Collector};
use crate::fetch;
use crate::pipeline::{self, parse_spool_key, spool_key};

/// Heights per header/body fetch task of one chain.
const SLAB: u64 = 1000;
/// Concurrent slab tasks per range (the client's semaphore and rate limit still apply).
const SLAB_TASKS: usize = 16;
/// Blocks decoded per rayon batch.
const DECODE_BATCH: usize = 1024;
/// Estimated writer memory for a busy 100,000-height range (run B: 1–1.5 GB).
const WRITER_MB_PER_100K: u64 = 1536;

/// One work unit: a range, or a step at the tail end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unit {
    pub kind: RangeKind,
    pub start: u32,
    /// Exclusive.
    pub end: u32,
}

/// Ranges aligned to `range` heights; steps of `step` heights where a full range does not
/// fit (unaligned start or the tail end). A partial step is never written.
pub fn plan_units(start: u32, end: u32, range: u32, step: u32) -> Result<Vec<Unit>> {
    if !start.is_multiple_of(step) {
        bail!("backfill start {start} is not a multiple of backfill.step_heights {step}");
    }
    let mut v = Vec::new();
    let mut s = start;
    loop {
        if s.is_multiple_of(range) && s.checked_add(range).is_some_and(|e| e <= end) {
            v.push(Unit {
                kind: RangeKind::Range,
                start: s,
                end: s + range,
            });
            s += range;
        } else if s.checked_add(step).is_some_and(|e| e <= end) {
            v.push(Unit {
                kind: RangeKind::Step,
                start: s,
                end: s + step,
            });
            s += step;
        } else {
            return Ok(v);
        }
    }
}

/// Last block hash per chain, the linkage anchor of the next range.
pub type Prev = BTreeMap<u8, (u32, Hash32)>;

#[derive(Debug, Clone, Default, Serialize)]
pub struct ResumeInfo {
    pub frontier: u32,
    pub next_txnum: u64,
    pub removed_files: Vec<String>,
    pub hot_reset: bool,
    /// Dictionary names of an uncommitted range dropped on resume.
    pub dict_names_removed: u64,
}

/// Delete every file or dir in `segments/` the manifest does not list (temp dirs, files of a
/// range or step that never reached the manifest, files of merged-away steps).
pub fn remove_unlisted(ctx: &Ctx, m: &Manifest) -> Result<Vec<String>> {
    let net = &ctx.opts.network;
    let mut keep: BTreeSet<String> = BTreeSet::new();
    keep.insert(MANIFEST.to_string());
    for e in &m.entries {
        keep.extend(range_file_names(net, e.start, e.end));
    }
    let mut removed = Vec::new();
    for ent in fs::read_dir(&ctx.segdir)? {
        let ent = ent?;
        let name = ent.file_name().to_string_lossy().into_owned();
        if keep.contains(&name) {
            continue;
        }
        let p = ent.path();
        if p.is_dir() {
            fs::remove_dir_all(&p)
        } else {
            fs::remove_file(&p)
        }
        .with_context(|| format!("remove {}", p.display()))?;
        removed.push(name);
    }
    if !removed.is_empty() {
        kdx_segment::files::fsync_dir(&ctx.segdir)?;
    }
    Ok(removed)
}

/// Reconcile the stores before a backfill: clear `etl/`, delete what the manifest does not
/// list, reset the hot frontier to the manifest's, re-derive `Headers` checkpoints.
pub fn resume(ctx: &Ctx) -> Result<(Manifest, Prev, ResumeInfo)> {
    let net = &ctx.opts.network;
    etl::clear_dir(&ctx.etl)?;
    let m = Manifest::load(&ctx.segdir)?.unwrap_or_else(|| Manifest::new(net));
    if &m.network != net {
        bail!("segments hold network {:?}, config says {net:?}", m.network);
    }
    let mut info = ResumeInfo {
        frontier: m.frontier,
        next_txnum: m.next_txnum,
        removed_files: remove_unlisted(ctx, &m)?,
        ..Default::default()
    };

    // Linkage anchors from the last committed range.
    let mut prev = Prev::new();
    if let Some(last) = m.entries.last() {
        let r = RangeReader::open(&ctx.segdir, last, net)?;
        let h = last.end - 1;
        for &c in ctx.graph.chains_at(u64::from(h)) {
            let slot = r.slot(c, h).context("last height slot")?;
            let hash = r
                .block_hash(slot)?
                .with_context(|| format!("segments miss block chain {c} height {h}"))?;
            prev.insert(c, (h, hash));
        }
    }

    let txn = ctx.hot.ro()?;
    let hot_ahead = txn.frozen_frontier()? > m.frontier;
    let no_tail = txn.count(Table::TailTxs)? == 0 && txn.count(Table::Headers)? == 0;
    let base_lens = match txn.meta(&dict::lens_key(m.frontier))? {
        Some(v) => Some(dict::decode_lens(&v)?),
        None if m.entries.is_empty() => Some(vec![0; kdx_types::DictKind::ALL.len()]),
        None => None,
    };
    let mut stale = txn.frozen_frontier()? != m.frontier || txn.next_txnum()? != m.next_txnum;
    for c in 0..kdx_segment::SLOT_WIDTH as u8 {
        let want = prev.get(&c).copied();
        if kdx_hot::checkpoint(&txn, Stage::Headers, c)? != want {
            stale = true;
        }
    }
    drop(txn);
    if stale {
        let mut txn = ctx.hot.rw()?;
        txn.set_frozen_frontier(m.frontier)?;
        txn.set_next_txnum(m.next_txnum)?;
        for c in 0..kdx_segment::SLOT_WIDTH as u8 {
            match prev.get(&c) {
                Some(&(h, hash)) => set_checkpoint(&mut txn, Stage::Headers, c, h, hash)?,
                None => {
                    clear_checkpoint(&mut txn, Stage::Headers, c)?;
                }
            }
        }
        // Names interned by a range that reached MDBX but not the manifest: nothing
        // committed references them, so drop them and the rebuild interns them again in
        // the same order (byte-identical files). Never with tail data present.
        if hot_ahead && no_tail {
            if let Some(lens) = &base_lens {
                info.dict_names_removed = dict::truncate(&mut txn, lens)?;
            }
        }
        txn.commit()?;
        info.hot_reset = true;
        *ctx.dict.lock().unwrap_or_else(|e| e.into_inner()) = HotDict::load(&ctx.hot.ro()?)?;
    }
    Ok((m, prev, info))
}

/// Memory plan: how many ranges fetch ahead and the per-range spool / posting budgets.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct MemPlan {
    pub in_flight: u32,
    pub spool_mb: u64,
    pub writer_est_mb: u64,
    pub postings_mem_entries: usize,
    pub cap_mb: u64,
}

/// `(in_flight + 1) × spool + writer ≤ cap`: `in_flight` ranges fetching while one builds.
pub fn mem_plan(
    ranges_in_flight: u32,
    range_heights: u64,
    etl_mem_mb: u64,
    cap_mb: u64,
) -> MemPlan {
    let writer_est_mb = (WRITER_MB_PER_100K * range_heights / 100_000).max(128);
    let mut spool_mb = etl_mem_mb.max(16);
    let budget = cap_mb.saturating_sub(writer_est_mb);
    let mut in_flight = ranges_in_flight.max(1);
    while in_flight > 1 && (u64::from(in_flight) + 1) * spool_mb > budget {
        in_flight -= 1;
    }
    if 2 * spool_mb > budget {
        spool_mb = (budget / 2).max(16);
    }
    // Postings are about half of the writer estimate: 5 families × 16 B per entry.
    let postings_mem_entries = ((writer_est_mb << 20) / 2 / (5 * 16))
        .clamp(1 << 16, kdx_segment::writer::ETL_MEM_ENTRIES as u64)
        as usize;
    MemPlan {
        in_flight,
        spool_mb,
        writer_est_mb,
        postings_mem_entries,
        cap_mb,
    }
}

struct Fetched {
    coll: Collector,
    blocks: u64,
    secs: f64,
}

/// Fetch every canonical block of `u` into a raw spool under `etl/raw-<start>`.
async fn fetch_unit(
    ctx: Arc<Ctx>,
    walker: Arc<Walker>,
    u: Unit,
    spool_bytes: usize,
) -> Result<Fetched> {
    let t = Instant::now();
    let dir = ctx.etl.join(format!("raw-{:09}", u.start));
    let coll = Collector::new(&dir, "raw", spool_bytes)?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<(Vec<u8>, Vec<u8>)>>(8);
    let sink = std::thread::spawn(move || -> Result<Collector> {
        let mut coll = coll;
        while let Some(batch) = rx.blocking_recv() {
            for (k, v) in batch {
                coll.push(k, v)?;
            }
        }
        Ok(coll)
    });

    let mut slabs = Vec::new();
    let mut spans: BTreeMap<u8, (u64, u64)> = BTreeMap::new();
    for h in u64::from(u.start)..u64::from(u.end) {
        for &c in ctx.graph.chains_at(h) {
            spans.entry(c).or_insert((h, h)).1 = h;
        }
    }
    for (&c, &(a, b)) in &spans {
        let mut lo = a;
        while lo <= b {
            let hi = (lo + SLAB - 1).min(b);
            slabs.push((c, lo, hi));
            lo = hi + 1;
        }
    }
    let results = futures::stream::iter(slabs.into_iter().map(|(c, lo, hi)| {
        let ctx = ctx.clone();
        let walker = walker.clone();
        let tx = tx.clone();
        async move {
            let hs = walker.canonical_headers(&ctx.node, c, lo, hi).await?;
            let items = fetch::bodies(&ctx.node, c, hs).await?;
            let mut batch = Vec::with_capacity(items.len());
            for (i, it) in items.into_iter().enumerate() {
                let h = u32::try_from(lo + i as u64)?;
                batch.push((spool_key(h, c).to_vec(), serde_json::to_vec(&it)?));
            }
            let n = batch.len() as u64;
            tx.send(batch)
                .await
                .map_err(|_| anyhow::anyhow!("spool closed"))?;
            anyhow::Ok(n)
        }
    }))
    .buffer_unordered(SLAB_TASKS)
    .collect::<Vec<_>>()
    .await;
    drop(tx);
    let coll = tokio::task::spawn_blocking(move || sink.join())
        .await?
        .map_err(|_| anyhow::anyhow!("spool thread panicked"))??;
    let mut blocks = 0;
    for r in results {
        blocks += r.with_context(|| format!("fetch {}..{}", u.start, u.end))?;
    }
    Ok(Fetched {
        coll,
        blocks,
        secs: t.elapsed().as_secs_f64(),
    })
}

/// One line of `backfill-stats.jsonl` (B10 input).
#[derive(Debug, Clone, Serialize)]
pub struct RangeStats {
    pub kind: &'static str,
    pub start: u32,
    pub end: u32,
    pub blocks: u64,
    pub txs: u64,
    pub cmd_reencoded: u64,
    pub fetch_secs: f64,
    pub decode_index_secs: f64,
    pub finish_secs: f64,
    pub commit_secs: f64,
    /// Time since the previous range was committed (pipeline throughput).
    pub wall_secs: f64,
    pub blocks_per_s: f64,
    pub txs_per_s: f64,
    pub fetch_blocks_per_s: f64,
    pub build_blocks_per_s: f64,
    pub spool_runs: usize,
    pub spool_mb: f64,
    pub bytes: BTreeMap<String, u64>,
    pub bytes_per_tx: BTreeMap<String, f64>,
    pub peak_rss_mb: u64,
    pub rss_mb: Option<u64>,
    pub header_mode: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub from: u32,
    pub end: u32,
    pub resume: ResumeInfo,
    pub mem: MemPlan,
    pub ranges: usize,
    pub blocks: u64,
    pub txs: u64,
    pub secs: f64,
    pub header_mode: &'static str,
    pub header_timing: WalkTiming,
    pub peak_rss_mb: u64,
}

/// Debug hook for crash tests: `KDX_DEBUG_ABORT_AFTER=finish|hot` aborts the process after
/// that commit step of the range starting at `KDX_DEBUG_ABORT_AT` (any range when unset).
fn debug_abort(step: &str, start: u32) {
    if std::env::var("KDX_DEBUG_ABORT_AFTER").ok().as_deref() == Some(step) {
        let at = std::env::var("KDX_DEBUG_ABORT_AT")
            .ok()
            .and_then(|s| s.parse::<u32>().ok());
        if at.is_none_or(|a| a == start) {
            eprintln!("KDX_DEBUG_ABORT_AFTER={step}: aborting at range {start}");
            std::process::abort();
        }
    }
}

/// Backfill `from..=to` (clamped to the final tip and to whole steps), resuming at the
/// manifest frontier.
pub async fn run(ctx: Arc<Ctx>, from: u32, to: u32, mode: HeaderMode) -> Result<Summary> {
    let t0 = Instant::now();
    let o = ctx.opts.clone();
    let (mut manifest, mut prev, resume_info) = {
        let c = ctx.clone();
        tokio::task::spawn_blocking(move || resume(&c)).await??
    };
    if resume_info.hot_reset || !resume_info.removed_files.is_empty() {
        tracing::warn!(
            removed = resume_info.removed_files.len(),
            hot_reset = resume_info.hot_reset,
            frontier = manifest.frontier,
            "resume: reset to the manifest"
        );
    }
    let fresh = manifest.entries.is_empty();
    let start = if fresh {
        from
    } else if from > manifest.frontier {
        bail!(
            "--from {from} is above the segment frontier {}: ranges must stay contiguous",
            manifest.frontier
        );
    } else {
        manifest.frontier
    };

    let cut = ctx.node.cut().await?;
    let min_tip = cut
        .hashes
        .values()
        .map(|e| e.height)
        .min()
        .context("empty cut")?;
    let final_tip = min_tip.saturating_sub(o.finality_depth);
    let end = (u64::from(to) + 1).min(final_tip + 1);
    let end = u32::try_from(end)?;
    let units = plan_units(
        start,
        end.max(start),
        u32::try_from(o.range_heights)?,
        u32::try_from(o.step_heights)?,
    )?;
    let planned_blocks: u64 = units
        .iter()
        .flat_map(|u| u.start..u.end)
        .map(|h| ctx.graph.chains_at(u64::from(h)).len() as u64)
        .sum();
    let mem = mem_plan(
        o.ranges_in_flight,
        o.range_heights,
        o.etl_mem_mb,
        o.mem_cap_mb,
    );
    tracing::info!(
        data_dir = %o.data_dir.display(),
        start,
        end = units.last().map(|u| u.end).unwrap_or(start),
        final_tip,
        units = units.len(),
        planned_blocks,
        in_flight = mem.in_flight,
        spool_mb = mem.spool_mb,
        writer_est_mb = mem.writer_est_mb,
        "backfill: plan"
    );

    let walker = Arc::new(Walker::new(mode, cut));
    let spool_bytes = (mem.spool_mb << 20) as usize;
    let mut pending: VecDeque<(Unit, tokio::task::JoinHandle<Result<Fetched>>)> = VecDeque::new();
    let mut next = 0usize;
    let spawn = |u: Unit| tokio::spawn(fetch_unit(ctx.clone(), walker.clone(), u, spool_bytes));
    let stats_path = o.data_dir.join("backfill-stats.jsonl");
    let mut last_commit = Instant::now();
    let (mut tot_blocks, mut tot_txs) = (0u64, 0u64);

    for _ in 0..units.len() {
        while pending.len() < mem.in_flight as usize && next < units.len() {
            pending.push_back((units[next], spawn(units[next])));
            next += 1;
        }
        let (u, h) = pending.pop_front().expect("pending unit");
        let fetched = h.await??;
        // Keep `in_flight` ranges fetching while this one builds.
        if next < units.len() {
            pending.push_back((units[next], spawn(units[next])));
            next += 1;
        }
        let c = ctx.clone();
        let p = std::mem::take(&mut prev);
        let first_txnum = manifest.next_txnum;
        let level = if u.kind == RangeKind::Range {
            o.range_level
        } else {
            o.step_level
        };
        let mut wopts = WriteOpts::new(level, o.dict_samples);
        wopts.postings_mem_entries = mem.postings_mem_entries;
        let link_free = fresh && u.start == start;
        let fetch_secs = fetched.secs;
        let fetch_blocks = fetched.blocks;
        let spool_runs = fetched.coll.runs();
        let spool_mb = fetched.coll.spilled_bytes() as f64 / (1 << 20) as f64;
        let m = manifest.clone();
        let (entry, p, timing) = tokio::task::spawn_blocking(move || {
            build_and_commit(&c, u, fetched.coll, p, first_txnum, wopts, link_free, m)
        })
        .await??;
        prev = p;
        manifest.push(entry.clone())?;

        let info = RangeInfo::load(&ctx.segdir.join(file_name(
            &o.network,
            FileKind::Range,
            u.start,
            u.end,
        )))?;
        let wall = last_commit.elapsed().as_secs_f64();
        last_commit = Instant::now();
        let mut bytes: BTreeMap<String, u64> = BTreeMap::new();
        for (name, f) in &info.files {
            let kind = FileKind::ALL
                .iter()
                .find(|k| name.starts_with(&format!("{}-{}-", o.network, k.name())))
                .map(|k| k.name())
                .unwrap_or("other");
            *bytes.entry(kind.to_string()).or_default() += f.bytes;
        }
        let txs = entry.tx_count.max(1) as f64;
        let st = RangeStats {
            kind: u.kind.name(),
            start: u.start,
            end: u.end,
            blocks: entry.block_count,
            txs: entry.tx_count,
            cmd_reencoded: info.cmd_reencoded,
            fetch_secs,
            decode_index_secs: timing.0,
            finish_secs: timing.1,
            commit_secs: timing.2,
            wall_secs: wall,
            blocks_per_s: entry.block_count as f64 / wall,
            txs_per_s: entry.tx_count as f64 / wall,
            fetch_blocks_per_s: fetch_blocks as f64 / fetch_secs.max(1e-9),
            build_blocks_per_s: entry.block_count as f64 / (timing.0 + timing.1).max(1e-9),
            spool_runs,
            spool_mb,
            bytes_per_tx: bytes
                .iter()
                .map(|(k, b)| (k.clone(), *b as f64 / txs))
                .collect(),
            bytes,
            peak_rss_mb: disk::peak_rss_mb(),
            rss_mb: disk::rss_mb(),
            header_mode: walker.mode(),
        };
        tot_blocks += entry.block_count;
        tot_txs += entry.tx_count;
        tracing::info!(
            range = format!("{}..{}", u.start, u.end),
            kind = st.kind,
            blocks = st.blocks,
            txs = st.txs,
            fetch_s = format!("{:.1}", st.fetch_secs),
            decode_index_s = format!("{:.1}", st.decode_index_secs),
            finish_s = format!("{:.1}", st.finish_secs),
            commit_s = format!("{:.2}", st.commit_secs),
            blocks_per_s = format!("{:.0}", st.blocks_per_s),
            txs_per_s = format!("{:.0}", st.txs_per_s),
            peak_rss_mb = st.peak_rss_mb,
            header_mode = st.header_mode,
            "backfill: range committed"
        );
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stats_path)?;
        writeln!(f, "{}", serde_json::to_string(&st)?)?;
    }
    Ok(Summary {
        from: start,
        end: manifest.frontier,
        resume: resume_info,
        mem,
        ranges: units.len(),
        blocks: tot_blocks,
        txs: tot_txs,
        secs: t0.elapsed().as_secs_f64(),
        header_mode: walker.mode(),
        header_timing: walker.timing(),
        peak_rss_mb: disk::peak_rss_mb(),
    })
}

/// Decode, index and write one unit, then commit it (writer finish → MDBX → manifest).
/// Returns the entry, the new linkage anchors and (decode+index, finish, commit) seconds.
#[allow(clippy::too_many_arguments)]
fn build_and_commit(
    ctx: &Ctx,
    u: Unit,
    coll: Collector,
    mut prev: Prev,
    first_txnum: u64,
    wopts: WriteOpts,
    link_free: bool,
    mut manifest: Manifest,
) -> Result<(RangeEntry, Prev, (f64, f64, f64))> {
    disk::ensure_free(&ctx.segdir, ctx.opts.min_free_gb)?;
    let net = &ctx.opts.network;
    let expected: u64 = (u.start..u.end)
        .map(|h| ctx.graph.chains_at(u64::from(h)).len() as u64)
        .sum();
    if coll.len() != expected {
        bail!(
            "range {}..{}: spooled {} blocks, expected {expected}",
            u.start,
            u.end,
            coll.len()
        );
    }
    let t = Instant::now();
    let mut w = RangeWriter::create(
        &ctx.segdir,
        &ctx.etl,
        net,
        u.kind,
        u.start,
        u.end,
        first_txnum,
        wopts,
    )?;
    let mut dict = ctx.dict.lock().unwrap_or_else(|e| e.into_inner());
    let mut it = coll.into_sorted()?;
    let mut batch: Vec<(u32, u8, Vec<u8>)> = Vec::with_capacity(DECODE_BATCH);
    loop {
        batch.clear();
        for r in it.by_ref() {
            let (k, v) = r?;
            let (h, c) = parse_spool_key(&k)?;
            batch.push((h, c, v));
            if batch.len() == DECODE_BATCH {
                break;
            }
        }
        if batch.is_empty() {
            break;
        }
        let prepared = pipeline::prepare_batch(&batch)?;
        for p in &prepared {
            let hd = &p.0.header;
            let (c, h) = (hd.chain_id, hd.height as u32);
            match prev.get(&c) {
                Some(&(ph, phash)) => {
                    if ph + 1 != h || hd.parent != phash {
                        bail!(
                            "linkage chain {c} height {h}: parent {} does not follow stored {ph} {}",
                            hd.parent,
                            phash
                        );
                    }
                }
                None => {
                    // Allowed only at the start of a fresh store or at a chain's first height.
                    let existed = h > 0 && ctx.graph.chains_at(u64::from(h) - 1).contains(&c);
                    if existed && !(link_free && h == u.start) {
                        bail!("linkage chain {c} height {h}: no stored parent");
                    }
                }
            }
            let ib = pipeline::index(p, &mut *dict)?;
            w.push(&ib)?;
            prev.insert(c, (h, hd.hash));
        }
    }
    drop(it);
    let _ = fs::remove_dir_all(ctx.etl.join(format!("raw-{:09}", u.start)));
    let last = u.end - 1;
    if let Some((c, (h, _))) = prev.iter().find(|(_, (h, _))| *h != last) {
        bail!("chain {c} ends at {h}, range ends at {last}");
    }
    let decode_secs = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let entry = w.finish()?;
    let finish_secs = t.elapsed().as_secs_f64();
    debug_abort("finish", u.start);

    let t = Instant::now();
    let mut txn = ctx.hot.rw()?;
    dict.persist(&mut txn)?;
    for (&c, &(h, hash)) in &prev {
        if h == last {
            set_checkpoint(&mut txn, Stage::Headers, c, h, hash)?;
        }
    }
    txn.set_next_txnum(entry.end_txnum())?;
    txn.set_frozen_frontier(entry.end)?;
    txn.set_meta(&dict::lens_key(entry.end), &dict::encode_lens(&dict))?;
    txn.commit()?;
    debug_abort("hot", u.start);
    manifest.push(entry.clone())?;
    manifest.commit(&ctx.segdir)?;
    let commit_secs = t.elapsed().as_secs_f64();
    drop(dict);
    Ok((entry, prev, (decode_secs, finish_secs, commit_secs)))
}
