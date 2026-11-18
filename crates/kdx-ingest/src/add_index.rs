//! `segments add-index` (FIX-2): add `sndr.pst` and `kf.fil` to the ranges and steps of a
//! store written before FIX-2, then the hot sender backfill. Offline: the caller holds the
//! data-dir lock; this holds `segments.swap.lock` exclusively for the whole run.
//!
//! Per manifest entry that lacks either file, is listed but missing (a rollback's old binary
//! deleted it), or whose key filter is not trusted:
//! 1. walk `txs.seg` row by row, in order: the sender id of every tx (the dictionary is only
//!    read; a sender that passes `account_ok` but has no id fails the entry) and the
//!    request-key prefixes; walk `blocks.seg` likewise for the block-hash prefixes;
//! 2. write the missing files and a new `range.toml` (the old one plus `[postings.sndr]` and
//!    the files' size and blake3) into `segments/rebuild.<start>/`, exactly as the writer
//!    would have written them;
//! 3. check: every `sndr` list is within the `acct` list of the same entity, every key passes
//!    the written filter, the key counts equal the range's;
//! 4. commit with `swap`: journal, `finish` over the new names (`range.toml` last), remove.
//!    Only renames: hard-linked copies of the old files keep their inodes.
//!
//! Resumable: a finished entry has both files; a crash after the journal is finished by
//! `swap::recover` (every writer start, and this command's start); an uncommitted rebuild dir
//! is dropped. Entries run in parallel on a bounded pool, each independent.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{bail, ensure, Context, Result};
use kdx_decode::index::indexed_sender;
use kdx_decode::Family;
use kdx_hot::HotStore;
use kdx_segment::blockfile::{row_len, row_record};
use kdx_segment::files::{blake3_file, file_name, fsync_dir, write_file, FileKind};
use kdx_segment::filter::{KeyFilter, KeyFilters};
use kdx_segment::postings::{Postings, PostingsWriter};
use kdx_segment::writer::{FileStat, PostingStat};
use kdx_segment::{swap, Manifest, RangeEntry, RangeReader};
use kdx_types::record::decode_tx;
use kdx_types::{Dict, DictKind};
use serde::Serialize;

use crate::freezer::crash_at;

/// Which files to add (`--only`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Only {
    Both,
    Sndr,
    Filter,
}

impl Only {
    pub fn parse(s: Option<&str>) -> Result<Only> {
        match s {
            None => Ok(Only::Both),
            Some("sndr") => Ok(Only::Sndr),
            Some("filter") => Ok(Only::Filter),
            Some(o) => bail!("--only {o:?}: expected sndr or filter"),
        }
    }
    fn sndr(self) -> bool {
        self != Only::Filter
    }
    fn filter(self) -> bool {
        self != Only::Sndr
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Opts {
    pub jobs: usize,
    pub only: Only,
    pub dry_run: bool,
}

/// One entry's line of the report.
#[derive(Debug, Clone, Default, Serialize)]
pub struct EntryReport {
    pub kind: &'static str,
    pub start: u32,
    pub end: u32,
    pub txs: u64,
    /// Files built (`sndr`, `kf`).
    pub built: Vec<&'static str>,
    /// Txs with an indexed sender (each has one `sndr` posting).
    pub senders: u64,
    pub sndr_entities: u64,
    pub sndr_bytes: u64,
    pub filter_bytes: u64,
    pub seconds: f64,
    pub swapped: bool,
    pub error: Option<String>,
}

/// The hot sender backfill's part of the report.
#[derive(Debug, Clone, Default, Serialize)]
pub struct HotReport {
    /// `Meta sndr_hot` was already 1 (the pass still runs: `inserted` is what it repaired).
    pub already_ready: bool,
    pub txs: u64,
    pub senders: u64,
    pub inserted: u64,
    pub undo_rows: u64,
    pub seconds: f64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Totals {
    pub entries: usize,
    pub built: usize,
    pub skipped: usize,
    pub failed: usize,
    pub txs: u64,
    pub senders: u64,
    pub sndr_bytes: u64,
    pub filter_bytes: u64,
    pub seconds: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub dry_run: bool,
    pub jobs: usize,
    pub only: Only,
    /// One line per entry that needed work.
    pub entries: Vec<EntryReport>,
    /// `None` with `--only filter`.
    pub hot: Option<HotReport>,
    pub totals: Totals,
}

impl Report {
    pub fn ok(&self) -> bool {
        self.totals.failed == 0 && self.hot.as_ref().is_none_or(|h| h.error.is_none())
    }
}

/// What an entry needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Need {
    sndr: bool,
    filter: bool,
}

/// What `e` lacks: `sndr.pst` not listed or listed but missing; `kf.fil` not listed, missing
/// or not trusted (the reader keeps no filter then).
fn need(r: &RangeReader, only: Only) -> Need {
    Need {
        sndr: only.sndr() && r.sndr.is_none(),
        filter: only.filter() && r.filter.is_none(),
    }
}

/// Run `segments add-index` over `segdir` (the caller holds the data-dir lock); then the hot
/// sender backfill on `hot` (unless `--only filter`). `--dry-run` builds and checks every
/// entry in a scratch dir under `<data dir>/etl` and removes it; it takes no swap lock, runs
/// no recovery, swaps nothing and only reads the hot store (which may be opened read-only).
pub fn run(segdir: &Path, hot: &HotStore, opts: Opts) -> Result<Report> {
    let t0 = Instant::now();
    let _swap = if opts.dry_run {
        None
    } else {
        let l = swap::swap_lock_or_wait(segdir, true, &mut || {
            tracing::info!(
                "add-index: waiting for segments.swap.lock (a snapshot or a manual `search \
                 rebuild` holds it)"
            )
        })?;
        for l in swap::recover(segdir)? {
            tracing::warn!("add-index: {l}");
        }
        Some(l)
    };
    let dict = {
        let txn = hot.ro()?;
        kdx_hot::HotDict::load(&txn)?
    };
    let manifest = Manifest::load(segdir)?;
    let mut work: Vec<RangeEntry> = Vec::new();
    let mut skipped = 0usize;
    if let Some(m) = &manifest {
        for e in &m.entries {
            let r = RangeReader::open(segdir, e, &m.network)
                .with_context(|| format!("open {} {}..{}", e.kind.name(), e.start, e.end))?;
            let n = need(&r, opts.only);
            if n.sndr || n.filter {
                work.push(e.clone());
            } else {
                skipped += 1;
            }
        }
    }
    let total_txs: u64 = work.iter().map(|e| e.tx_count).sum();
    tracing::info!(
        entries = work.len(),
        skipped,
        txs = total_txs,
        jobs = opts.jobs,
        dry_run = opts.dry_run,
        "add-index: start"
    );
    let entries = match &manifest {
        Some(m) => run_entries(segdir, &m.network, &work, &dict, opts, total_txs),
        None => Vec::new(),
    };
    let hot_rep = opts.only.sndr().then(|| hot_pass(hot, &dict, opts.dry_run));
    let mut totals = Totals {
        entries: entries.len() + skipped,
        skipped,
        ..Default::default()
    };
    for e in &entries {
        if e.error.is_some() {
            totals.failed += 1;
        } else {
            totals.built += 1;
        }
        totals.txs += e.txs;
        totals.senders += e.senders;
        totals.sndr_bytes += e.sndr_bytes;
        totals.filter_bytes += e.filter_bytes;
    }
    totals.seconds = t0.elapsed().as_secs_f64();
    Ok(Report {
        dry_run: opts.dry_run,
        jobs: opts.jobs,
        only: opts.only,
        entries,
        hot: hot_rep,
        totals,
    })
}

/// The entries on `jobs` threads, in manifest order as threads free up. A progress line per
/// entry; the ETA is the first finished entry's seconds per tx × the txs left ÷ jobs.
fn run_entries(
    segdir: &Path,
    net: &str,
    work: &[RangeEntry],
    dict: &(impl Dict + Sync),
    opts: Opts,
    total_txs: u64,
) -> Vec<EntryReport> {
    let next = AtomicUsize::new(0);
    let out: Mutex<Vec<(usize, EntryReport)>> = Mutex::new(Vec::new());
    // (txs done, seconds per tx of the first finished entry)
    let progress: Mutex<(u64, Option<f64>)> = Mutex::new((0, None));
    let jobs = opts.jobs.clamp(1, work.len().max(1));
    std::thread::scope(|s| {
        for _ in 0..jobs {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some(e) = work.get(i) else {
                    break;
                };
                let rep = add_entry(segdir, net, e, dict, opts.only, opts.dry_run);
                let rep = match rep {
                    Ok(r) => r,
                    Err(err) => {
                        tracing::error!(
                            start = e.start,
                            error = %format!("{err:#}"),
                            "add-index: entry failed, nothing swapped"
                        );
                        EntryReport {
                            kind: e.kind.name(),
                            start: e.start,
                            end: e.end,
                            txs: e.tx_count,
                            error: Some(format!("{err:#}")),
                            ..Default::default()
                        }
                    }
                };
                {
                    let mut p = progress.lock().unwrap_or_else(|x| x.into_inner());
                    p.0 += e.tx_count;
                    if p.1.is_none() && rep.error.is_none() && rep.txs > 0 {
                        p.1 = Some(rep.seconds / rep.txs as f64);
                    }
                    let left = total_txs.saturating_sub(p.0);
                    let eta = p.1.map(|r| r * left as f64 / jobs as f64);
                    let done = out.lock().map(|o| o.len() + 1).unwrap_or(0);
                    tracing::info!(
                        "add-index: {done}/{} {} {}..{}: {} txs, {} senders, built {:?} in {:.1} s{}; {left} txs left, eta {}",
                        work.len(),
                        e.kind.name(),
                        e.start,
                        e.end,
                        rep.txs,
                        rep.senders,
                        rep.built,
                        rep.seconds,
                        if rep.error.is_some() { " (FAILED)" } else { "" },
                        eta.map(|s| format!("{s:.0} s")).unwrap_or_else(|| "-".into())
                    );
                }
                out.lock().unwrap_or_else(|x| x.into_inner()).push((i, rep));
            });
        }
    });
    let mut v = out.into_inner().unwrap_or_else(|x| x.into_inner());
    v.sort_by_key(|(i, _)| *i);
    v.into_iter().map(|(_, r)| r).collect()
}

/// Where an entry is built: the swap's rebuild dir, or for a dry run a scratch dir under
/// `<data dir>/etl` that never takes part in a swap.
fn work_dir(segdir: &Path, start: u32, dry_run: bool) -> PathBuf {
    if dry_run {
        segdir
            .parent()
            .unwrap_or(segdir)
            .join("etl")
            .join(format!("add-index-dry.{start:09}"))
    } else {
        swap::rebuild_dir(segdir, start)
    }
}

fn remove_work(segdir: &Path, start: u32, dry_run: bool) -> Result<()> {
    if dry_run {
        let d = work_dir(segdir, start, true);
        if d.exists() {
            std::fs::remove_dir_all(&d)?;
        }
        Ok(())
    } else {
        swap::remove(segdir, start)
    }
}

/// Add the missing optional files to one entry and swap them in (see the module docs). An
/// error leaves the entry as it was (its rebuild dir removed). Public for tests.
pub fn add_entry(
    segdir: &Path,
    net: &str,
    e: &RangeEntry,
    dict: &impl Dict,
    only: Only,
    dry_run: bool,
) -> Result<EntryReport> {
    let t0 = Instant::now();
    // A committed swap's rebuild dir holds the files readers already take: never rebuild
    // over it (M2). `run` recovers pending swaps at startup, so this is a guard.
    if swap::pending(segdir, e.start) {
        bail!(
            "a committed swap of this entry is pending (the next writer start or add-index \
             run finishes it)"
        );
    }
    remove_work(segdir, e.start, dry_run)?;
    let dir = work_dir(segdir, e.start, dry_run);
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let built = build(segdir, &dir, net, e, dict, only);
    let mut rep = match built {
        Ok(r) => r,
        Err(err) => {
            let _ = remove_work(segdir, e.start, dry_run);
            return Err(err);
        }
    };
    if dry_run {
        remove_work(segdir, e.start, true)?;
        rep.seconds = t0.elapsed().as_secs_f64();
        return Ok(rep);
    }
    let names: Vec<String> = rep
        .built
        .iter()
        .map(|k| {
            file_name(
                net,
                if *k == "sndr" {
                    FileKind::Sndr
                } else {
                    FileKind::KeyFilter
                },
                e.start,
                e.end,
            )
        })
        .chain([file_name(net, FileKind::Range, e.start, e.end)])
        .collect();
    crash_at("add-index-before-journal");
    rep.swapped = true;
    swap::commit_journal(segdir, e.start, &serde_json::to_vec_pretty(&rep)?)?;
    // Past the journal the swap is committed: a failure from here on is reported as such
    // (M1), never as "nothing swapped". The next writer start or add-index run finishes it.
    let fin = (|| -> Result<()> {
        swap::finish(segdir, e.start, &names, &|| crash_at("add-index-mid-swap"))?;
        crash_at("add-index-before-cleanup");
        swap::remove(segdir, e.start)
    })();
    if let Err(err) = fin {
        tracing::error!(
            start = e.start,
            error = %format!("{err:#}"),
            "add-index: swap committed but not finished; the next writer start or add-index \
             run completes it"
        );
        rep.error = Some(format!("swap committed, not finished: {err:#}"));
    }
    rep.seconds = t0.elapsed().as_secs_f64();
    Ok(rep)
}

/// Steps 1-3 into `dir`: the missing files and the new `range.toml`.
fn build(
    segdir: &Path,
    dir: &Path,
    net: &str,
    e: &RangeEntry,
    dict: &impl Dict,
    only: Only,
) -> Result<EntryReport> {
    let r = RangeReader::open(segdir, e, net)?;
    let n = need(&r, only);
    let mut rep = EntryReport {
        kind: e.kind.name(),
        start: e.start,
        end: e.end,
        txs: e.tx_count,
        ..Default::default()
    };
    if !n.sndr && !n.filter {
        bail!("nothing to add");
    }
    let fname = |k: FileKind| file_name(net, k, e.start, e.end);

    // 1. txs.seg, row by row: (AccountId << 32 | rel) per sender, request-key prefixes.
    let mut pairs: Vec<u64> = Vec::new();
    let mut rk: Vec<u64> = Vec::new();
    let mut rel = 0u64;
    for row_i in 0..r.txs.rows() {
        let row = r.txs.row(row_i)?;
        for i in 0..row_len(&row)? {
            let rec =
                decode_tx(row_record(&row, i)?).with_context(|| format!("tx record {rel}"))?;
            if n.filter {
                if let Some(k) = &rec.request_key {
                    rk.push(kdx_segment::filter::key_of(k));
                }
            }
            if n.sndr {
                let sender = indexed_sender(&rec)
                    .map_err(|err| anyhow::anyhow!("cmd of {:?}: {err:?}", rec.key))?;
                if let Some(s) = sender {
                    let id = dict.find(DictKind::Account, &s).with_context(|| {
                        format!("sender of {:?} is not in the dictionary", rec.key)
                    })?;
                    pairs.push(u64::from(id) << 32 | rel);
                }
            }
            rel += 1;
            kdx_types::heartbeat::beat(kdx_types::heartbeat::Component::Ingest);
        }
    }
    ensure!(
        rel == e.tx_count,
        "txs.seg holds {rel} records, the manifest {}",
        e.tx_count
    );
    let mut info = r.info.clone();

    if n.sndr {
        // 2./3. sndr.pst, as the writer writes it: entities ascending, each list ascending.
        pairs.sort_unstable();
        rep.senders = pairs.len() as u64;
        let path = dir.join(fname(FileKind::Sndr));
        let mut pw = PostingsWriter::create(&path, Family::Sndr, e.tx_count)?;
        let acct = &r.postings[Family::Acct as usize];
        let mut i = 0;
        while i < pairs.len() {
            let ent = pairs[i] >> 32;
            let j = i + pairs[i..].partition_point(|p| p >> 32 == ent);
            let list: Vec<u32> = pairs[i..j].iter().map(|p| *p as u32).collect();
            within_acct(acct, ent, &list, e.first_txnum)?;
            pw.push(ent, &list)?;
            i = j;
        }
        drop(pairs);
        let (entities, entries) = pw.finish()?;
        ensure!(
            entries == rep.senders,
            "sndr.pst holds {entries} of {} postings",
            rep.senders
        );
        Postings::open(&path, e.first_txnum).context("written sndr.pst")?;
        rep.sndr_entities = entities;
        rep.sndr_bytes = std::fs::metadata(&path)?.len();
        info.postings.insert(
            Family::Sndr.name().to_string(),
            PostingStat { entities, entries },
        );
        info.files.insert(
            fname(FileKind::Sndr),
            FileStat {
                bytes: rep.sndr_bytes,
                blake3: blake3_file(&path)?,
            },
        );
        rep.built.push("sndr");
    }

    if n.filter {
        // blocks.seg, row by row: the block-hash prefixes.
        let mut bh: Vec<u64> = Vec::with_capacity(e.block_count as usize);
        for row_i in 0..r.blocks.rows() {
            let row = r.blocks.row(row_i)?;
            for i in 0..row_len(&row)? {
                let h = kdx_segment::adj::record_block_hash(row_record(&row, i)?)?;
                bh.push(kdx_segment::filter::key_of(&h));
            }
        }
        ensure!(
            bh.len() as u64 == info.bh_keys && rk.len() as u64 == info.rk_keys,
            "{} block hashes and {} request keys, range.toml {} and {}",
            bh.len(),
            rk.len(),
            info.bh_keys,
            info.rk_keys
        );
        let filters = KeyFilters {
            bh: KeyFilter::build(&mut bh)?,
            rk: KeyFilter::build(&mut rk)?,
        };
        let bytes = filters.to_bytes();
        drop(filters);
        // The self-check on the bytes as written: every key passes the decoded filter.
        let back = KeyFilters::from_bytes(&bytes)?;
        ensure!(
            bh.iter().all(|k| back.bh.contains(*k)) && rk.iter().all(|k| back.rk.contains(*k)),
            "key filter self-check failed"
        );
        let path = dir.join(fname(FileKind::KeyFilter));
        write_file(&path, &bytes)?;
        rep.filter_bytes = bytes.len() as u64;
        info.files.insert(
            fname(FileKind::KeyFilter),
            FileStat {
                bytes: rep.filter_bytes,
                blake3: blake3_file(&path)?,
            },
        );
        rep.built.push("kf");
    }
    drop(r);
    write_file(
        &dir.join(fname(FileKind::Range)),
        info.to_toml()?.as_bytes(),
    )?;
    fsync_dir(dir)?;
    Ok(rep)
}

/// `list` (relative TxNums, ascending) is within the `acct` list of `ent` (absolute TxNums,
/// descending from the reader): a merge walk.
fn within_acct(acct: &Postings, ent: u64, list: &[u32], first: u64) -> Result<()> {
    let have: Vec<u64> = acct.list(ent).map(|it| it.collect()).unwrap_or_default();
    let mut it = have.iter().rev().peekable();
    for &rel in list {
        let want = first + u64::from(rel);
        while it.peek().is_some_and(|&&x| x < want) {
            it.next();
        }
        if it.peek() != Some(&&want) {
            bail!("sndr ⊄ acct: entity {ent}, TxNum {want} has no acct posting");
        }
    }
    Ok(())
}

/// The hot sender backfill (`kdx_hot::sndr::backfill`), one write txn. It runs even when
/// `Meta sndr_hot` is already 1 (FIX-2 fix 2): it is idempotent and takes seconds, and a tail
/// an older binary extended after a rollback without `hot drop-family sndr` still says 1
/// while its newer rows lack their `sndr` postings. A dry run only counts the tail's txs.
fn hot_pass(hot: &HotStore, dict: &kdx_hot::HotDict, dry_run: bool) -> HotReport {
    let t0 = Instant::now();
    let mut rep = HotReport::default();
    let r = (|| -> Result<()> {
        rep.already_ready = kdx_hot::sndr::hot_ready(&hot.ro()?)?;
        if dry_run {
            // Read-only (the hot store may be opened read-only): the tail txs to walk.
            use kdx_hot::Txn;
            rep.txs = hot.ro()?.count(kdx_hot::Table::TailTxs)? as u64;
            return Ok(());
        }
        let mut txn = hot.rw()?;
        let st = kdx_hot::sndr::backfill(&mut txn, dict)?;
        txn.commit()?;
        rep.txs = st.txs;
        rep.senders = st.senders;
        rep.inserted = st.inserted;
        rep.undo_rows = st.undo_rows;
        Ok(())
    })();
    if let Err(e) = r {
        tracing::error!(error = %format!("{e:#}"), "add-index: hot sender backfill failed");
        rep.error = Some(format!("{e:#}"));
    }
    rep.seconds = t0.elapsed().as_secs_f64();
    tracing::info!(
        already_ready = rep.already_ready,
        txs = rep.txs,
        senders = rep.senders,
        inserted = rep.inserted,
        seconds = rep.seconds,
        "add-index: hot sender backfill"
    );
    rep
}

/// Entries of `report` by start (tests).
pub fn by_start(report: &Report) -> BTreeMap<u32, &EntryReport> {
    report.entries.iter().map(|e| (e.start, e)).collect()
}
