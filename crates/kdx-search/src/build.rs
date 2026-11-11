//! `search rebuild` and the S-1 projection (`--dry-run`), plan Task 8.
//!
//! Pass 0 is the census. Pass 1 walks the ranges oldest to newest on `build_threads`: for
//! each census key whose first range is this range, the first tx of its `code` posting list
//! in the range is decoded and its body becomes one doc (`first_h` = that tx's height,
//! `last_h` = the last height of the key's last census range). Reading one tx per key in
//! TxNum order (one row decompressed at most once) gives the same docs as walking every exec
//! tx of the range, except that two bodies sharing an 8-byte prefix and first seen in the same
//! range yield one doc (about n²/2⁶⁵ for n bodies). The hot store goes last, every exec tx
//! checked (per-pass `HashSet` of full hashes). Read-only on hot and segments, with a fresh
//! hot read txn per chunk; it writes only `search.new/` (and `search.dryrun/` for the trial).

use std::collections::HashSet;
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use kdx_decode::index::code_entity;
use kdx_decode::Family;
use kdx_hot::{checkpoint, HotStore, Stage, Table, Txn};
use kdx_segment::blockfile::{row_record, BlockFile};
use kdx_segment::files::fsync_dir;
use kdx_segment::{RangeReader, Segments};
use kdx_types::record::decode_tx;
use kdx_types::{blake2b_128, MemDict, TxKey};
use rayon::prelude::*;
use serde::Serialize;
use tantivy::IndexWriter;

use crate::census::{distinct_code_keys, Census};
use crate::manifest::{dir_bytes, now_secs, SearchManifest, FORMAT};
use crate::schema::{create, open, to_doc, writer, Doc, Fields};
use crate::{exec_code, is_module, new_dir, search_dir, Mode, ModeSel, TANTIVY_VERSION};

/// S-1: full mode when the projection is at most this.
pub const FULL_LIMIT_BYTES: u64 = 40_000_000_000;
/// S-1: the fallback threshold is the smallest length whose projection is at most this.
pub const FALLBACK_TARGET_BYTES: u64 = 30_000_000_000;
/// One body in `SAMPLE_EVERY` per range for the projection (1%).
const SAMPLE_EVERY: usize = 100;
/// Hot `TailTxs` rows per read txn.
const HOT_CHUNK: usize = 10_000;

pub struct BuildOpts {
    pub data_dir: PathBuf,
    pub mode: ModeSel,
    pub threads: usize,
    pub heap_mb: usize,
    /// Docs of the dry run's trial index (bytes per doc).
    pub trial_docs: u64,
    /// Set to stop a rebuild (`run` shutting down): `search.new/` is removed.
    pub stop: Arc<AtomicBool>,
}

/// S-1 projection. `bytes_per_code_byte` comes from the trial index; bodies of the 1% sample
/// scale to the census count. The per-byte model charges each doc's fixed overhead in
/// proportion to the trial's average length, so for a fallback set (long bodies) it
/// over-estimates.
#[derive(Debug, Clone, Serialize)]
pub struct Estimate {
    pub distinct: u64,
    pub ranges: usize,
    pub hot_keys_first_in_hot: u64,
    pub sample_bodies: u64,
    pub sample_avg_code_len: f64,
    pub sample_module_share: f64,
    pub trial_docs: u64,
    pub trial_code_bytes: u64,
    pub trial_bytes: u64,
    pub bytes_per_doc: f64,
    pub bytes_per_code_byte: f64,
    pub projected_full_bytes: u64,
    /// `full` or `fallback`.
    pub decision: String,
    pub fallback_min_len: u32,
    pub projected_bytes: u64,
    /// Fallback only: projected bytes of the module/interface bodies alone.
    pub projected_module_bytes: u64,
    pub secs: f64,
}

impl Estimate {
    pub fn mode(&self) -> Mode {
        if self.decision == "fallback" {
            Mode::Fallback {
                min_len: self.fallback_min_len,
            }
        } else {
            Mode::Full
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RebuildReport {
    pub manifest: SearchManifest,
    pub estimate: Option<Estimate>,
    pub census_keys: u64,
    pub skipped_by_mode: u64,
    pub secs: f64,
}

// Progress for `run`'s status line: ranges done of total while a rebuild runs.
static ACTIVE: AtomicBool = AtomicBool::new(false);
static DONE: AtomicU32 = AtomicU32::new(0);
static TOTAL: AtomicU32 = AtomicU32::new(0);

/// `(ranges done, ranges total)` of a rebuild running in this process.
pub fn progress() -> Option<(u32, u32)> {
    ACTIVE
        .load(Ordering::Relaxed)
        .then(|| (DONE.load(Ordering::Relaxed), TOTAL.load(Ordering::Relaxed)))
}

struct ActiveGuard;
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        ACTIVE.store(false, Ordering::Relaxed);
    }
}

/// Decompressed-row cache over one `txs.seg` (records read in TxNum order).
struct RowCache<'a> {
    f: &'a BlockFile,
    row: usize,
    buf: Vec<u8>,
}

impl<'a> RowCache<'a> {
    fn new(f: &'a BlockFile) -> Self {
        RowCache {
            f,
            row: usize::MAX,
            buf: Vec::new(),
        }
    }
    fn record(&mut self, i: u64) -> Result<&[u8]> {
        let br = u64::from(self.f.header.block_records);
        let r = (i / br) as usize;
        if r != self.row {
            self.buf = self.f.row(r)?;
            self.row = r;
        }
        row_record(&self.buf, (i % br) as usize)
    }
}

/// First relative TxNum of every census key whose first range is `r` (every `every`-th such
/// key), ascending.
fn first_rels(rr: &RangeReader, c: &Census, r: u16, every: usize) -> Result<Vec<u32>> {
    let p = &rr.postings[Family::Code as usize];
    let mut out = Vec::new();
    let mut j = 0usize;
    for i in 0..p.entities() {
        if c.get(p.entity_at(i)).map(|x| x.0) != Some(r) {
            continue;
        }
        j += 1;
        if !(j - 1).is_multiple_of(every) {
            continue;
        }
        if let Some(&rel) = p.list_at(i)?.first() {
            out.push(rel);
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

/// Bodies of range `r` for its census keys, in TxNum order: `(doc, admitted by mode)`.
/// `f` returns false to stop early.
fn range_bodies(
    rr: &RangeReader,
    c: &Census,
    r: u16,
    every: usize,
    stop: &AtomicBool,
    mut f: impl FnMut(Doc) -> Result<bool>,
) -> Result<()> {
    let mut cache = RowCache::new(&rr.txs);
    for rel in first_rels(rr, c, r, every)? {
        if stop.load(Ordering::Relaxed) {
            bail!("search rebuild stopped");
        }
        let rec = decode_tx(cache.record(u64::from(rel))?)?;
        let Some(code) = exec_code(&rec) else {
            continue;
        };
        let hash = blake2b_128(code.as_bytes());
        let Some((first, last)) = c.get(code_entity(&hash)) else {
            bail!(
                "range {}..{} rel {rel}: body prefix is not in the census",
                rr.info.start,
                rr.info.end
            );
        };
        debug_assert_eq!(first, r);
        let h = rr
            .txkey_rel(u64::from(rel))
            .map_or(rr.info.start, |k| k.height());
        let d = Doc {
            hash,
            code,
            first_h: u64::from(h),
            last_h: u64::from(c.last_h(last).max(h)),
        };
        if !f(d)? {
            break;
        }
    }
    Ok(())
}

fn segments_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("segments")
}

fn open_segments(data_dir: &Path) -> Result<Segments> {
    Segments::open(&segments_dir(data_dir), Arc::new(MemDict::new()))
}

/// The S-1 projection: census, 1% body sample per range, trial index of the first
/// `trial_docs` bodies (in `search.dryrun/`, removed afterwards).
pub fn estimate(data_dir: &Path, seg: &Segments, c: &Census, o: &BuildOpts) -> Result<Estimate> {
    let t0 = Instant::now();
    // 1% sample of distinct bodies per range.
    let (mut n, mut sum, mut modules) = (0u64, 0u64, 0u64);
    let mut lens: Vec<(u32, bool)> = Vec::new();
    for (r, rr) in seg.ranges.iter().enumerate() {
        range_bodies(rr, c, r as u16, SAMPLE_EVERY, &o.stop, |d| {
            let m = is_module(&d.code);
            n += 1;
            sum += d.code.len() as u64;
            modules += u64::from(m);
            lens.push((u32::try_from(d.code.len()).unwrap_or(u32::MAX), m));
            Ok(true)
        })?;
    }
    // Trial index.
    let dir = data_dir.join("search.dryrun");
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    let trial = (|| -> Result<(u64, u64, u64)> {
        let (index, f) = create(&dir)?;
        let w = writer(&index, o.threads, o.heap_mb)?;
        let (mut docs, mut code_bytes) = (0u64, 0u64);
        for (r, rr) in seg.ranges.iter().enumerate() {
            if docs >= o.trial_docs {
                break;
            }
            range_bodies(rr, c, r as u16, 1, &o.stop, |d| {
                code_bytes += d.code.len() as u64;
                w.add_document(to_doc(&f, &d))?;
                docs += 1;
                Ok(docs < o.trial_docs)
            })?;
        }
        let mut w = w;
        w.commit()?;
        w.wait_merging_threads()?;
        Ok((docs, code_bytes, dir_bytes(&dir)?))
    })();
    let _ = std::fs::remove_dir_all(&dir);
    let (trial_docs, trial_code_bytes, trial_bytes) = trial?;
    let k = if trial_code_bytes > 0 {
        trial_bytes as f64 / trial_code_bytes as f64
    } else {
        0.0
    };
    let distinct = c.len() as u64;
    let scale = if n > 0 {
        distinct as f64 / n as f64
    } else {
        0.0
    };
    let proj = |code_bytes: u64| (k * scale * code_bytes as f64) as u64;
    let projected_full_bytes = proj(sum);
    let module_bytes: u64 = lens.iter().filter(|x| x.1).map(|x| u64::from(x.0)).sum();
    let (decision, min_len, projected) = if projected_full_bytes <= FULL_LIMIT_BYTES {
        ("full", 0u32, projected_full_bytes)
    } else {
        // Smallest threshold L (a sampled non-module length) whose projection fits.
        let mut other: Vec<u32> = lens.iter().filter(|x| !x.1).map(|x| x.0).collect();
        other.sort_unstable();
        let mut suffix = vec![0u64; other.len() + 1];
        for i in (0..other.len()).rev() {
            suffix[i] = suffix[i + 1] + u64::from(other[i]);
        }
        let mut pick = (
            other.last().map_or(1, |&l| l.saturating_add(1)),
            proj(module_bytes),
        );
        for (i, &l) in other.iter().enumerate() {
            // Equal lengths: the first index of a run holds the full suffix.
            if i > 0 && other[i - 1] == l {
                continue;
            }
            let p = proj(module_bytes + suffix[i]);
            if p <= FALLBACK_TARGET_BYTES {
                pick = (l, p);
                break;
            }
        }
        ("fallback", pick.0, pick.1)
    };
    Ok(Estimate {
        distinct,
        ranges: seg.ranges.len(),
        hot_keys_first_in_hot: c.count_first(c.hot_idx) as u64,
        sample_bodies: n,
        sample_avg_code_len: if n > 0 { sum as f64 / n as f64 } else { 0.0 },
        sample_module_share: if n > 0 {
            modules as f64 / n as f64
        } else {
            0.0
        },
        trial_docs,
        trial_code_bytes,
        trial_bytes,
        bytes_per_doc: if trial_docs > 0 {
            trial_bytes as f64 / trial_docs as f64
        } else {
            0.0
        },
        bytes_per_code_byte: k,
        projected_full_bytes,
        decision: decision.into(),
        fallback_min_len: min_len,
        projected_bytes: projected,
        projected_module_bytes: proj(module_bytes),
        secs: t0.elapsed().as_secs_f64(),
    })
}

/// Lock against a second rebuild of the same data dir (`run`'s and the CLI's), held for the
/// whole rebuild.
pub fn lock(data_dir: &Path) -> Result<File> {
    let p = data_dir.join("search.lock");
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&p)
        .with_context(|| format!("open {}", p.display()))?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(TryLockError::WouldBlock) => {
            bail!("another search rebuild of this data dir is running")
        }
        Err(TryLockError::Error(e)) => Err(e).with_context(|| format!("lock {}", p.display())),
    }
}

/// The swap lock (`search.swap.lock`, blocking): held by [`swap_in`] around its renames, by
/// the tail stage around every write into `search/` ([`with_current`]) and by `snapshot`
/// while it reads and links the live files. Every holder is short (one stage commit, two
/// renames, one set of links).
pub fn swap_lock(data_dir: &Path) -> Result<File> {
    let p = data_dir.join("search.swap.lock");
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&p)
        .with_context(|| format!("open {}", p.display()))?;
    f.lock().with_context(|| format!("lock {}", p.display()))?;
    Ok(f)
}

/// `f` under the swap lock, only while `search/` holds the index built at `built_at`; None
/// (and `f` not run) once a rebuild swapped in another one. A tantivy writer opened before a
/// swap still writes by path, so without this check its files, `meta.json` and
/// `.managed.json` would land in the new index.
pub fn with_current<T>(
    data_dir: &Path,
    built_at: u64,
    f: impl FnOnce() -> Result<T>,
) -> Result<Option<T>> {
    let _g = swap_lock(data_dir)?;
    match SearchManifest::load(&search_dir(data_dir))? {
        Some(m) if m.built_at == built_at => f().map(Some),
        _ => Ok(None),
    }
}

/// Hot pass: every exec tx of `TailTxs` whose body is not first seen in a segment range.
/// Returns `built_through_height`: the lowest tail checkpoint when the frozen frontier stayed
/// at the census's, else the census frontier − 1 (blocks frozen meanwhile are in neither
/// pass; the tail stage catches up from there).
fn hot_pass(
    hot: &HotStore,
    c: &Census,
    mode: Mode,
    stop: &AtomicBool,
    skipped: &AtomicU64,
    mut add: impl FnMut(Doc) -> Result<()>,
) -> Result<u32> {
    let floor = c.frontier.saturating_sub(1);
    let (mut moved, min_cp) = {
        let txn = hot.ro()?;
        let mut m: Option<u32> = None;
        for ch in 0..kdx_segment::SLOT_WIDTH as u8 {
            if let Some((h, _)) = checkpoint(&txn, Stage::Tail, ch)? {
                m = Some(m.map_or(h, |x| x.min(h)));
            }
        }
        (txn.frozen_frontier()? != c.frontier, m)
    };
    let mut seen: HashSet<[u8; 16]> = HashSet::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        if stop.load(Ordering::Relaxed) {
            bail!("search rebuild stopped");
        }
        let txn = hot.ro()?;
        moved |= txn.frozen_frontier()? != c.frontier;
        let rows = txn.scan_chunk(Table::TailTxs, after.as_deref(), HOT_CHUNK, |_| true)?;
        drop(txn);
        for (k, v) in &rows {
            let rec = decode_tx(v)?;
            let Some(code) = exec_code(&rec) else {
                continue;
            };
            let hash = blake2b_128(code.as_bytes());
            if matches!(c.get(code_entity(&hash)), Some((first, _)) if first < c.hot_idx) {
                continue;
            }
            if !seen.insert(hash) {
                continue;
            }
            if !mode.admits(&code) {
                skipped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let h = TxKey::from_be_bytes(k[..].try_into().context("TailTxs key")?).height();
            add(Doc {
                hash,
                code,
                first_h: u64::from(h),
                last_h: u64::from(c.last_h(c.hot_idx).max(h)),
            })?;
        }
        if rows.len() < HOT_CHUNK {
            break;
        }
        after = rows.last().map(|r| r.0.clone());
    }
    Ok(match (moved, min_cp) {
        (false, Some(m)) => m.max(floor),
        _ => floor,
    })
}

/// `rebuilt_through_height` of a rebuild over census `c`: the last height below the census's
/// frozen frontier. Every segment below it was read by the census, so a doc's `last_h` is
/// exact up to there. Hot heights are not: pass 1 runs for hours after the census's hot walk,
/// a body can recur in a hot block meanwhile, and neither the hot pass nor the tail stage
/// adds a doc for a body the index already holds. Readers keep those heights in their upper
/// window, also after a later freeze moves them into a segment. (The first P6 cut used
/// `built_through_height` here, the lowest tail checkpoint at the hot pass: recurrences
/// between the census and the hot pass were lost after the next freeze.)
pub fn rebuilt_through(c: &Census) -> u32 {
    c.frontier.saturating_sub(1)
}

/// Census, projection (`auto`, or `fallback` with no threshold), pass 1, hot pass, then
/// `search.new/` replaces `search/`. `hot` is the caller's environment (`run` shares its own).
pub fn rebuild(o: &BuildOpts, hot: &HotStore) -> Result<RebuildReport> {
    let t0 = Instant::now();
    let _lock = lock(&o.data_dir)?;
    let seg = open_segments(&o.data_dir)?;
    let c = distinct_code_keys(&seg, hot)?;
    tracing::info!(
        distinct = c.len(),
        ranges = seg.ranges.len(),
        frontier = c.frontier,
        "search rebuild: census"
    );
    let (mode, est) = match o.mode {
        ModeSel::Full => (Mode::Full, None),
        ModeSel::Fallback(l) if l > 0 => (Mode::Fallback { min_len: l }, None),
        _ => {
            let e = estimate(&o.data_dir, &seg, &c, o)?;
            tracing::info!(estimate = %serde_json::to_string(&e)?, "search rebuild: S-1 projection");
            (e.mode(), Some(e))
        }
    };
    let new = new_dir(&o.data_dir);
    if new.exists() {
        std::fs::remove_dir_all(&new)?;
    }
    let r = build_into(o, hot, &seg, &c, mode, &new);
    let (built_through, skipped) = match r {
        Ok(x) => x,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&new);
            return Err(e);
        }
    };
    let (index, _) = open(&new)?;
    let docs = index.reader()?.searcher().num_docs();
    let m = SearchManifest {
        format: FORMAT,
        tantivy: TANTIVY_VERSION.into(),
        mode: mode.name().into(),
        fallback_min_len: mode.min_len(),
        built_through_height: built_through,
        rebuilt_through_height: Some(rebuilt_through(&c)),
        docs,
        bytes: dir_bytes(&new)?,
        built_at: now_secs(),
    };
    drop(index);
    m.commit(&new)?;
    swap_in(&o.data_dir)?;
    Ok(RebuildReport {
        manifest: m,
        estimate: est,
        census_keys: c.len() as u64,
        skipped_by_mode: skipped,
        secs: t0.elapsed().as_secs_f64(),
    })
}

fn build_into(
    o: &BuildOpts,
    hot: &HotStore,
    seg: &Segments,
    c: &Census,
    mode: Mode,
    new: &Path,
) -> Result<(u32, u64)> {
    let (index, f) = create(new)?;
    let w = writer(&index, o.threads, o.heap_mb)?;
    let skipped = AtomicU64::new(0);
    let _g = ActiveGuard;
    DONE.store(0, Ordering::Relaxed);
    TOTAL.store(seg.ranges.len() as u32 + 1, Ordering::Relaxed);
    ACTIVE.store(true, Ordering::Relaxed);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(o.threads.max(1))
        .thread_name(|i| format!("kdx-search-{i}"))
        .build()?;
    let add = |w: &IndexWriter, f: &Fields, d: Doc| -> Result<bool> {
        if mode.admits(&d.code) {
            w.add_document(to_doc(f, &d))?;
        } else {
            skipped.fetch_add(1, Ordering::Relaxed);
        }
        Ok(true)
    };
    pool.install(|| {
        seg.ranges
            .par_iter()
            .enumerate()
            .try_for_each(|(r, rr)| -> Result<()> {
                range_bodies(rr, c, r as u16, 1, &o.stop, |d| add(&w, &f, d))?;
                DONE.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
    })?;
    let bt = hot_pass(hot, c, mode, &o.stop, &skipped, |d| {
        w.add_document(to_doc(&f, &d))?;
        Ok(())
    })?;
    DONE.fetch_add(1, Ordering::Relaxed);
    let mut w = w;
    w.commit()?;
    w.wait_merging_threads()?;
    Ok((bt, skipped.load(Ordering::Relaxed)))
}

/// `search.new/` becomes `search/` (the old index is removed). A crash between the renames
/// leaves no `search/manifest.json`, so the next `run` rebuilds. Under the swap lock: no
/// stage commit and no snapshot read runs across the renames.
pub(crate) fn swap_in(data_dir: &Path) -> Result<()> {
    let _g = swap_lock(data_dir)?;
    let (live, new, old) = (
        search_dir(data_dir),
        new_dir(data_dir),
        data_dir.join("search.old"),
    );
    if old.exists() {
        std::fs::remove_dir_all(&old)?;
    }
    if live.exists() {
        std::fs::rename(&live, &old)?;
    }
    std::fs::rename(&new, &live)?;
    fsync_dir(data_dir)?;
    if old.exists() {
        std::fs::remove_dir_all(&old)?;
    }
    Ok(())
}

/// `--dry-run`: census and projection only.
pub fn dry_run(o: &BuildOpts, hot: &HotStore) -> Result<Estimate> {
    let seg = open_segments(&o.data_dir)?;
    let c = distinct_code_keys(&seg, hot)?;
    estimate(&o.data_dir, &seg, &c, o)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use kdx_decode::{Family, IndexedBlock, TxIndex};
    use kdx_hot::{Geometry, HotDict, HotStore};
    use kdx_types::like::Like;
    use kdx_types::record::{BlockRecord, TxRecord};
    use kdx_types::{blake2b_128, Hash32, TxKey};

    use super::{
        build_into, open_segments, rebuilt_through, swap_in, swap_lock, with_current, BuildOpts,
    };
    use crate::census::distinct_code_keys;
    use crate::manifest::{SearchManifest, FORMAT};
    use crate::schema::{create, to_doc, writer, Doc, SearchWriter};
    use crate::{new_dir, search_dir, Mode, ModeSel, Searcher};

    pub(crate) fn exec_block(chain: u8, height: u32, codes: &[&str]) -> IndexedBlock {
        let txs: Vec<TxRecord> = codes
            .iter()
            .enumerate()
            .map(|(i, c)| TxRecord {
                key: TxKey::new(height, chain, i as u16 + 1),
                request_key: Some(Hash32([(height % 251) as u8 + i as u8; 32])),
                cmd: Some(
                    serde_json::to_vec(&serde_json::json!({"payload": {"exec": {"code": c}}}))
                        .unwrap(),
                ),
                ..TxRecord::default()
            })
            .collect();
        let idx = txs
            .iter()
            .zip(codes)
            .map(|(r, c)| {
                let h = blake2b_128(c.as_bytes());
                TxIndex {
                    key: r.key,
                    postings: vec![(Family::Code, kdx_decode::index::code_entity(&h))],
                    pact_id: None,
                    code_hash: Some(h),
                }
            })
            .collect();
        IndexedBlock {
            block: BlockRecord {
                hash: Hash32([height as u8; 32]),
                chain,
                height: u64::from(height),
                ..BlockRecord::default()
            },
            txs,
            idx,
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        }
    }

    pub(crate) fn apply(hot: &HotStore, blocks: &[IndexedBlock]) {
        let mut w = hot.rw().unwrap();
        let mut dict = HotDict::load(&w).unwrap();
        for b in blocks {
            kdx_hot::tail::apply(&mut w, b, &mut dict).unwrap();
        }
        w.commit().unwrap();
    }

    pub(crate) fn hot_store(dir: &std::path::Path) -> HotStore {
        let g = Geometry {
            max_gb: 1,
            growth_mb: 16,
        };
        HotStore::open(&dir.join("hot"), g).unwrap()
    }

    /// I-1: a body that recurs in a hot block between the census and the hot pass keeps the
    /// doc's census `last_h`. `rebuilt_through_height` stays below the recurrence, so the
    /// readers' upper window (`min(rebuilt_through + 1, hot floor)`) still covers it after the
    /// next freeze moves the hot floor above it; the hot pass's checkpoint would not.
    #[test]
    fn recurrence_after_the_census_stays_in_the_upper_window() {
        let d = tempfile::tempdir().unwrap();
        let hot = hot_store(d.path());
        let body = "(free.recurring.body \"x\")";
        apply(
            &hot,
            &[
                exec_block(0, 10, &[body]),
                exec_block(0, 11, &["(other.one)"]),
                exec_block(0, 12, &["(other.two)"]),
            ],
        );
        let seg = open_segments(d.path()).unwrap();
        let c = distinct_code_keys(&seg, &hot).unwrap();
        // Pass 1 runs; meanwhile the body recurs at 15.
        apply(
            &hot,
            &(13..=16)
                .map(|h| exec_block(0, h, &[if h == 15 { body } else { "(other.three)" }]))
                .collect::<Vec<_>>(),
        );
        let o = BuildOpts {
            data_dir: d.path().to_path_buf(),
            mode: ModeSel::Full,
            threads: 1,
            heap_mb: 32,
            trial_docs: 0,
            stop: Arc::new(AtomicBool::new(false)),
        };
        let new = d.path().join("search.new");
        let (built_through, _) = build_into(&o, &hot, &seg, &c, Mode::Full, &new).unwrap();
        let rt = rebuilt_through(&c);
        SearchManifest {
            format: FORMAT,
            tantivy: crate::TANTIVY_VERSION.into(),
            mode: "full".into(),
            fallback_min_len: 0,
            built_through_height: built_through,
            rebuilt_through_height: Some(rt),
            docs: 0,
            bytes: 0,
            built_at: 1,
        }
        .commit(&new)
        .unwrap();
        let s = Searcher::open(&new, 100).unwrap().unwrap();
        let far = Instant::now() + Duration::from_secs(10);
        let hits = s
            .candidates(&Like::new("recurring.body"), far)
            .unwrap()
            .hashes;
        assert_eq!(hits.len(), 1);
        let (_, first_h, last_h) = hits[0];
        // The doc knows heights 10..=12 only.
        assert_eq!((first_h, last_h), (10, 12));
        // The hot pass's checkpoint covers 15: as the upper bound of the exact part it would
        // drop 15 from both windows once a freeze lifts the hot floor above 16.
        assert!(built_through >= 15);
        let hot_floor_after_freeze = 17;
        let upper_from = (rt + 1).min(hot_floor_after_freeze);
        assert!(upper_from <= 15, "upper window from {upper_from} misses 15");
    }

    fn doc(code: &str) -> Doc {
        Doc {
            hash: blake2b_128(code.as_bytes()),
            code: code.into(),
            first_h: 1,
            last_h: 1,
        }
    }

    /// An index of `codes` with its manifest in `dir`.
    fn build_index(dir: &std::path::Path, codes: &[&str], built_at: u64) {
        let (index, f) = create(dir).unwrap();
        let mut w = writer(&index, 1, 16).unwrap();
        for c in codes {
            w.add_document(to_doc(&f, &doc(c))).unwrap();
        }
        w.commit().unwrap();
        w.wait_merging_threads().unwrap();
        SearchManifest {
            format: FORMAT,
            tantivy: crate::TANTIVY_VERSION.into(),
            mode: "full".into(),
            fallback_min_len: 0,
            built_through_height: 1,
            rebuilt_through_height: Some(1),
            docs: codes.len() as u64,
            bytes: 0,
            built_at,
        }
        .commit(dir)
        .unwrap();
    }

    fn count(data: &std::path::Path, pat: &str) -> usize {
        let s = Searcher::open(&search_dir(data), 100).unwrap().unwrap();
        let far = Instant::now() + Duration::from_secs(10);
        s.candidates(&Like::new(pat), far).unwrap().hashes.len()
    }

    /// I-2: a rebuild that swaps `search/` while the stage holds docs never gets the stage's
    /// writes. A swap requested during a stage commit waits for it; a stage commit after the
    /// swap writes nothing.
    #[test]
    fn a_swap_and_a_stage_commit_never_interleave() {
        let d = tempfile::tempdir().unwrap();
        let data = d.path().to_path_buf();
        build_index(&search_dir(&data), &["(a.one)"], 1);
        let mut w = SearchWriter::open(&search_dir(&data), 16).unwrap();
        assert!(w.add(&doc("(a.two)")).unwrap());
        build_index(&new_dir(&data), &["(b.one)", "(b.two)", "(b.three)"], 2);
        // The stage is in a commit: the swap waits for it.
        let g = swap_lock(&data).unwrap();
        let data2 = data.clone();
        let t = std::thread::spawn(move || swap_in(&data2).unwrap());
        std::thread::sleep(Duration::from_millis(200));
        let live = SearchManifest::load(&search_dir(&data)).unwrap().unwrap();
        assert_eq!(live.built_at, 1, "swapped during the stage commit");
        w.commit().unwrap();
        drop(g);
        t.join().unwrap();
        // The next stage commit sees the new index and writes nothing into it.
        assert!(w.add(&doc("(a.three)")).unwrap());
        let meta = std::fs::read(search_dir(&data).join("meta.json")).unwrap();
        let managed = std::fs::read(search_dir(&data).join(".managed.json")).ok();
        assert!(with_current(&data, 1, || w.commit()).unwrap().is_none());
        drop(w);
        assert_eq!(
            meta,
            std::fs::read(search_dir(&data).join("meta.json")).unwrap()
        );
        assert_eq!(
            managed,
            std::fs::read(search_dir(&data).join(".managed.json")).ok()
        );
        assert_eq!(count(&data, "(b."), 3);
        assert_eq!(count(&data, "(a."), 0);
        // A writer on the new index commits through `with_current`, merges included.
        let mut w = SearchWriter::open(&search_dir(&data), 16).unwrap();
        for i in 0..12 {
            assert!(w.add(&doc(&format!("(b.tail-{i})"))).unwrap());
            assert!(with_current(&data, 2, || w.commit()).unwrap().is_some());
        }
        assert_eq!(count(&data, "(b."), 15);
        assert_eq!(w.num_docs(), 15);
    }
}
