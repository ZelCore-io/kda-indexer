//! `snapshot --out DIR`: a consistent copy of a data dir that can run beside a live `run`.
//!
//! Order (plan R-8): begin a fresh read txn T on the hot store, then read
//! `segments/manifest.json`, then check that T sees every dictionary name the manifest's
//! segments refer to (frontier at or below T's frozen frontier, or `dict_lens@<frontier>` in
//! T, the check `Store::view` makes), then `copy_compact(T)` the hot store and drop T, then
//! link (or copy) every file the manifest references with no txn held, then write the
//! manifest bytes. T is older than the manifest, so the manifest frontier is at or above the
//! frozen frontier T sees and the hot copy can only hold duplicate rows below the frontier,
//! which freezer recovery resolves at `run`. When the name check fails (a backfill range or a
//! freeze committed between T and the manifest read), T and the manifest are taken again after
//! a short pause. When a merge deletes a step file before it is linked, T, the manifest and
//! the hot copy are taken again; files linked earlier stay valid (immutable, named by range).
//! T is never reused across manifest reads.
//!
//! Then `search/` (P6): its `manifest.json` and tantivy's `meta.json` are read once and the
//! files of the segments `meta.json` lists are linked (or copied); a file a tantivy commit or
//! merge removed first makes the step start over (the step holds the search swap lock, so no
//! rebuild swap, stage commit or merge runs meanwhile). The search manifest is read before the
//! index, so its `built_through_height` never runs ahead of the docs (the copy's tail stage
//! re-reads from there; bodies already present are skipped).
//!
//! Never copied: `etl/`, `LOCK`, `merkle-unverified.jsonl`, `search.new/`, anything else
//! outside `segments/`, `hot/` and `search/`. The snapshot is a data dir (`serve --data-dir
//! DIR`, `run`).
//! On a refusal nothing is created; on a later failure what this run created is removed.
//!
//! The snapshot runs as the data dir's owner (the service user). Root is refused unless it
//! owns the whole tree (the data dir, `segments/`, `hot/`, `search/` and an existing `--out`;
//! [`refuse_root`]); otherwise root only pre-creates the empty `--out` (`install -d`) for the
//! owner's run. A listed file is placed only when it is a regular file: a symlink at its name
//! is refused, never followed ([`place`]). The manifest's `network` is a plain name
//! (`Manifest::validate`), so a listed name never leaves `segments/`.

use std::collections::HashSet;
use std::fs;
use std::io::{self, ErrorKind};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use kdx_hot::env::meta;
use kdx_hot::{dict, HotStore, RoTxn, Stage, Table, Txn};
use kdx_segment::files::{fsync_dir, replace_file};
use kdx_segment::Manifest;
use rustix::fs::{Mode, OFlags};
use rustix::io::Errno;
use serde_json::json;

use super::hot::hot_dir;
use super::segments::segments_dir;

/// Manifest re-reads when a merge removes files under the snapshot.
const MAX_ATTEMPTS: usize = 5;
/// Manifest re-reads while T cannot see the names of the manifest's newest segments (the
/// freezer's hot txn follows its manifest commit; a backfill commits names first).
const MAX_NAME_WAITS: usize = 30;
/// Pause before such a re-read.
const NAME_WAIT_PAUSE: Duration = Duration::from_secs(1);

pub struct SnapshotArgs {
    pub data_dir: PathBuf,
    pub out: PathBuf,
    /// Copy segment files instead of hard-linking them (another filesystem).
    pub copy: bool,
}

#[derive(Debug, Default)]
pub struct SnapshotReport {
    pub files: usize,
    pub segment_bytes: u64,
    pub hot_bytes: u64,
    pub linked: bool,
    pub attempts: usize,
    pub search_files: usize,
    pub search_bytes: u64,
    pub secs: f64,
}

/// Where `snapshot_with` calls its hook (argument two: the manifest read's number).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Point {
    /// T has begun; the manifest is not read yet.
    Txn,
    /// The manifest was read; nothing of this read is copied or linked yet.
    Manifest,
    /// The segment swap lock is held (shared), before the first manifest read (m14).
    SwapLocked,
}

pub fn run(a: SnapshotArgs) -> anyhow::Result<()> {
    let r = snapshot(&a)?;
    println!(
        "snapshot {}: {} segment files {} ({} B), hot {} B, search {} files ({} B), {} manifest read(s), {:.1} s",
        a.out.display(),
        r.files,
        if r.linked { "linked" } else { "copied" },
        r.segment_bytes,
        r.hot_bytes,
        r.search_files,
        r.search_bytes,
        r.attempts,
        r.secs
    );
    Ok(())
}

pub fn snapshot(a: &SnapshotArgs) -> anyhow::Result<SnapshotReport> {
    snapshot_as(rustix::process::geteuid().as_raw(), a)
}

/// [`snapshot`] run with the effective uid `euid` (FV-2: tests pass 0 without being root).
fn snapshot_as(euid: u32, a: &SnapshotArgs) -> anyhow::Result<SnapshotReport> {
    // Debug hook `KDX_DEBUG_SLEEP_AT=snapshot-after-manifest:<secs>`: widen the window between
    // the manifest read and the links (a merge can delete step files in it).
    let sleep = std::env::var("KDX_DEBUG_SLEEP_AT")
        .ok()
        .and_then(|v| {
            let (p, n) = v.split_once(':')?;
            (p == "snapshot-after-manifest").then(|| n.parse::<u64>().ok())?
        })
        .map(Duration::from_secs);
    // Before anything is opened or created (the hot store open creates its lock file).
    let owner = fs::metadata(&a.data_dir)
        .with_context(|| format!("stat {}", a.data_dir.display()))?
        .uid();
    let below = if euid == 0 {
        tree_owners(a)?
    } else {
        Vec::new()
    };
    refuse_root(euid, owner, &below)?;
    let hs = HotStore::open_ro(&hot_dir(&a.data_dir))?;
    snapshot_with_as(euid, a, &hs, NAME_WAIT_PAUSE, &mut |p, _| {
        if let (Point::Manifest, Some(d)) = (p, sleep) {
            std::thread::sleep(d);
        }
    })
}

/// [`snapshot_with_as`] run with the process's effective uid.
#[cfg(test)]
fn snapshot_with(
    a: &SnapshotArgs,
    hs: &HotStore,
    pause: Duration,
    hook: &mut dyn FnMut(Point, usize),
) -> anyhow::Result<SnapshotReport> {
    snapshot_with_as(rustix::process::geteuid().as_raw(), a, hs, pause, hook)
}

/// `snapshot` of the hot store `hs` (opened on the data dir's `hot/`), with a hook at each
/// [`Point`] and the pause before a re-read for missing names; `euid` is the effective uid
/// ([`prepare_out`]).
fn snapshot_with_as(
    euid: u32,
    a: &SnapshotArgs,
    hs: &HotStore,
    pause: Duration,
    hook: &mut dyn FnMut(Point, usize),
) -> anyhow::Result<SnapshotReport> {
    let created = prepare_out(&a.out, &a.data_dir, euid)?;
    let r = snapshot_into(a, hs, pause, hook);
    if r.is_err() {
        remove_created(&a.out, created.as_deref());
    }
    r
}

/// Refuses a snapshot run as root (`euid` 0) unless root owns the whole tree it reads and
/// writes by path: the data dir (`data_owner`: the uid owning it) and each of `below` (path,
/// owner; [`tree_owners`]). The service user owns the data dir and may be compromised (the
/// API runs as it); root reading and writing by path in a tree that user controls could be
/// pointed at any file on the host. The snapshot runs as the data dir's owner; root only
/// creates the empty `--out` for it first. The refusal names every path root does not own.
fn refuse_root(euid: u32, data_owner: u32, below: &[(PathBuf, u32)]) -> anyhow::Result<()> {
    if euid != 0 {
        return Ok(());
    }
    let others: Vec<String> = below
        .iter()
        .filter(|(_, uid)| *uid != 0)
        .map(|(p, uid)| format!("{} (uid {uid})", p.display()))
        .collect();
    if data_owner != 0 {
        let also = if others.is_empty() {
            String::new()
        } else {
            format!(" (not root-owned either: {})", others.join(", "))
        };
        bail!(
            "snapshot refuses to run as root on a data dir owned by uid {data_owner}{also}: run \
             it as the data dir's owner (sudo -u kda-indexer kda-indexer ... snapshot --out \
             DIR, or sudo -u '#{data_owner}' ...); when that user cannot create DIR, create it \
             empty as root first (install -d -o kda-indexer -g kda-indexer -m 0750 DIR)"
        );
    }
    if !others.is_empty() {
        bail!(
            "snapshot refuses to run as root: it runs as root only on a data dir whose whole \
             tree root owns, and these are not root-owned: {}; run it as the data dir's owner",
            others.join(", ")
        );
    }
    Ok(())
}

/// FV-3: the owners of what a root run reads and writes by path besides the data dir itself:
/// `segments/`, `hot/`, `search/`, and `--out` when it exists (even empty). A symlink counts
/// with its own owner and, when that is root, its target's. Missing paths are left out.
fn tree_owners(a: &SnapshotArgs) -> anyhow::Result<Vec<(PathBuf, u32)>> {
    let mut v = Vec::new();
    for p in [
        segments_dir(&a.data_dir),
        hot_dir(&a.data_dir),
        kdx_search::search_dir(&a.data_dir),
        a.out.clone(),
    ] {
        match owner_uid(&p) {
            Ok(uid) => v.push((p, uid)),
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("stat {}", p.display())),
        }
    }
    Ok(v)
}

/// The owner of `p` itself: a symlink counts with its own owner and, when that is root, its
/// target's.
fn owner_uid(p: &Path) -> io::Result<u32> {
    let m = fs::symlink_metadata(p)?;
    let mut uid = m.uid();
    if uid == 0 && m.file_type().is_symlink() {
        if let Ok(t) = fs::metadata(p) {
            uid = t.uid();
        }
    }
    Ok(uid)
}

/// Refuses an `--out` inside the data dir or not empty before creating anything, then creates
/// it. Returns the topmost directory this created (None: `--out` existed, empty). Run as root
/// (`euid` 0), an existing `--out` is accepted only when root owns it (Review PRE-F M-1: the
/// owner is read where the directory is accepted, so one created after [`tree_owners`] ran is
/// refused too).
fn prepare_out(out: &Path, data_dir: &Path, euid: u32) -> anyhow::Result<Option<PathBuf>> {
    let data_abs =
        fs::canonicalize(data_dir).with_context(|| format!("resolve {}", data_dir.display()))?;
    let out_abs = resolve(out)?;
    if out_abs.starts_with(&data_abs) {
        bail!("--out must not be inside the data dir");
    }
    match fs::read_dir(out) {
        Ok(mut d) => {
            if euid == 0 {
                let uid = owner_uid(out).with_context(|| format!("stat {}", out.display()))?;
                if uid != 0 {
                    bail!(
                        "snapshot refuses to run as root: {} (uid {uid}) is not root-owned; \
                         run it as the data dir's owner",
                        out.display()
                    );
                }
            }
            if d.next().is_some() {
                bail!("{} is not empty", out.display());
            }
            return Ok(None);
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("read {}", out.display())),
    }
    // The missing directories, topmost first.
    let mut missing = Vec::new();
    let mut p = out_abs.as_path();
    while fs::symlink_metadata(p).is_err() {
        missing.push(p.to_path_buf());
        p = p.parent().context("--out has no existing ancestor")?;
    }
    missing.reverse();
    fs::create_dir_all(out).with_context(|| format!("create {}", out.display()))?;
    for d in &missing {
        if let Some(parent) = d.parent() {
            fsync_dir(parent)?;
        }
    }
    Ok(missing.into_iter().next())
}

/// Absolute form of `p` with symlinks of its existing part resolved (the rest may not exist).
fn resolve(p: &Path) -> anyhow::Result<PathBuf> {
    let abs = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()?.join(p)
    };
    let mut rest = Vec::new();
    let mut base = abs.as_path();
    while fs::symlink_metadata(base).is_err() {
        match base.components().next_back() {
            Some(Component::Normal(n)) => rest.push(n.to_os_string()),
            _ => bail!("cannot resolve {}", p.display()),
        }
        base = base
            .parent()
            .with_context(|| format!("cannot resolve {}", p.display()))?;
    }
    let mut r = fs::canonicalize(base).with_context(|| format!("resolve {}", base.display()))?;
    r.extend(rest.iter().rev());
    Ok(r)
}

/// After a failure: removes the directories this run created, or (an `--out` that existed
/// empty) everything inside it. Best effort; the original error is what the caller sees.
fn remove_created(out: &Path, created: Option<&Path>) {
    let r = match created {
        Some(top) => fs::remove_dir_all(top),
        None => fs::read_dir(out).and_then(|d| {
            for e in d {
                let p = e?.path();
                if p.is_dir() {
                    fs::remove_dir_all(&p)?;
                } else {
                    fs::remove_file(&p)?;
                }
            }
            Ok(())
        }),
    };
    if let Err(e) = r {
        tracing::warn!(out = %out.display(), "snapshot: cleanup after a failure: {e}");
    }
}

/// Whether T sees every dictionary name that segments up to `frontier` refer to: the frozen
/// frontier T sees covers it (a backfill commits names and frontier before the manifest; the
/// tail interned the names of every block the freezer later froze), or `dict_lens@frontier`
/// is in T (written in the MDBX commit that holds those names).
fn names_visible(txn: &RoTxn, frontier: u32) -> anyhow::Result<bool> {
    Ok(frontier <= txn.frozen_frontier()? || txn.meta(&dict::lens_key(frontier))?.is_some())
}

/// What `SNAPSHOT.json` records from T.
struct HotInfo {
    network: Option<String>,
    frozen_frontier: Option<u32>,
    next_txnum: Option<u64>,
    tail: serde_json::Map<String, serde_json::Value>,
}

fn hot_info(txn: &RoTxn) -> anyhow::Result<HotInfo> {
    let u32_of = |k: &str| -> Option<u32> {
        let v = txn.get(Table::Meta, k.as_bytes()).ok()??;
        Some(u32::from_be_bytes(v.as_slice().try_into().ok()?))
    };
    let u64_of = |k: &str| -> Option<u64> {
        let v = txn.get(Table::Meta, k.as_bytes()).ok()??;
        Some(u64::from_be_bytes(v.as_slice().try_into().ok()?))
    };
    let network = txn
        .get(Table::Meta, meta::NETWORK.as_bytes())?
        .map(|v| String::from_utf8_lossy(&v).into_owned());
    let mut tail = serde_json::Map::new();
    for (k, v) in txn.scan(Table::StageCheckpoint, &[Stage::Tail as u8], |k| {
        k.first() == Some(&(Stage::Tail as u8))
    })? {
        if let (Some(c), Some(h)) = (k.get(1), v.get(..4)) {
            tail.insert(
                c.to_string(),
                json!(u32::from_be_bytes(h.try_into().unwrap_or_default())),
            );
        }
    }
    Ok(HotInfo {
        network,
        frozen_frontier: u32_of(meta::FROZEN_FRONTIER),
        next_txnum: u64_of(meta::NEXT_TXNUM),
        tail,
    })
}

fn snapshot_into(
    a: &SnapshotArgs,
    hs: &HotStore,
    pause: Duration,
    hook: &mut dyn FnMut(Point, usize),
) -> anyhow::Result<SnapshotReport> {
    let t0 = Instant::now();
    let src_seg = segments_dir(&a.data_dir);
    let out_seg = a.out.join("segments");
    let out_hot = a.out.join("hot");
    fs::create_dir_all(&out_seg)?;
    if !a.copy && fs::metadata(&src_seg)?.dev() != fs::metadata(&out_seg)?.dev() {
        bail!(
            "{} is on another filesystem than {}: hard links are impossible, use --copy",
            a.out.display(),
            src_seg.display()
        );
    }

    let mut rep = SnapshotReport {
        linked: !a.copy,
        ..Default::default()
    };
    let mut have: HashSet<PathBuf> = HashSet::new();
    let (mut merge_reads, mut name_waits) = (0usize, 0usize);
    // m14: no `rebuild-range` swap runs while segment files are placed, so a range's files
    // all come from one side of its swap (a data dir we cannot lock still gets `resolve`).
    let swap_guard = lock_or_warn(
        kdx_segment::swap::swap_lock(&src_seg, false),
        "placing segment files",
    )?;
    hook(Point::SwapLocked, 0);
    let (manifest_bytes, manifest, info) = loop {
        rep.attempts += 1;
        // A fresh T before every manifest read.
        let txn = hs.ro()?;
        hook(Point::Txn, rep.attempts);
        let bytes = match fs::read(src_seg.join("manifest.json")) {
            Ok(b) => Some(b),
            Err(e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => return Err(e).context("read manifest.json"),
        };
        let m = bytes.as_deref().map(Manifest::parse).transpose()?;
        hook(Point::Manifest, rep.attempts);
        if let Some(m) = &m {
            if !names_visible(&txn, m.frontier)? {
                drop(txn);
                name_waits += 1;
                if name_waits >= MAX_NAME_WAITS {
                    bail!(
                        "the manifest (frontier {}) kept listing segments whose dictionary names \
                         the hot store had not committed ({MAX_NAME_WAITS} reads): a backfill \
                         or a freeze is committing, try again",
                        m.frontier
                    );
                }
                std::thread::sleep(pause);
                continue;
            }
        }
        // The hot copy from T, then T is dropped: the segment files are placed with no txn
        // held (a `--copy` of every file can take a long time).
        if out_hot.exists() {
            fs::remove_dir_all(&out_hot)?;
        }
        hs.copy_compact(&txn, &out_hot)?;
        let info = hot_info(&txn)?;
        drop(txn);
        let mut missing = false;
        if let Some(m) = &m {
            for f in m.files() {
                if have.contains(&f) {
                    continue;
                }
                let src = kdx_segment::swap::resolve(&src_seg, &f.to_string_lossy());
                match place(&src, &out_seg.join(&f), a.copy) {
                    Ok(n) => {
                        rep.segment_bytes += n;
                        have.insert(f);
                    }
                    Err(e) if e.kind() == ErrorKind::NotFound => missing = true,
                    Err(e) => return Err(e).with_context(|| format!("place {}", f.display())),
                }
            }
        }
        if !missing {
            drop(swap_guard);
            break (bytes, m, info);
        }
        merge_reads += 1;
        if merge_reads >= MAX_ATTEMPTS {
            bail!("segment files kept changing under the snapshot ({MAX_ATTEMPTS} manifest reads)");
        }
    };
    // Files of an earlier manifest read that the final one no longer lists.
    let keep: HashSet<PathBuf> = manifest.iter().flat_map(Manifest::files).collect();
    for f in have.difference(&keep) {
        fs::remove_file(out_seg.join(f))?;
    }
    rep.files = keep.len();
    if let Some(b) = &manifest_bytes {
        replace_file(&out_seg.join("manifest.json"), b)?;
    }
    fsync_dir(&out_seg)?;
    rep.hot_bytes = fs::metadata(out_hot.join("mdbx.dat"))?.len();
    fsync_dir(&out_hot)?;
    let search = snapshot_search(
        &kdx_search::search_dir(&a.data_dir),
        &kdx_search::search_dir(&a.out),
        a.copy,
    )?;
    if let Some((n, b, _)) = &search {
        (rep.search_files, rep.search_bytes) = (*n, *b);
    }

    let network = info
        .network
        .or_else(|| manifest.as_ref().map(|m| m.network.clone()));
    let snap = json!({
        "format": {"hot": kdx_hot::FORMAT_VERSION, "segments": kdx_segment::manifest::FORMAT_VERSION},
        "network": network,
        "frozen_frontier": manifest.as_ref().map_or(0, |m| m.frontier),
        "hot_frozen_frontier": info.frozen_frontier,
        "next_txnum": info.next_txnum,
        "tail_checkpoints": info.tail,
        "manifest_blake3": manifest_bytes.as_deref().map(|b| blake3::hash(b).to_hex().to_string()),
        "search_built_through_height": search.as_ref().map(|s| s.2),
        "binary_version": env!("CARGO_PKG_VERSION"),
        "created_at": utc_iso(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()),
    });
    replace_file(
        &a.out.join("SNAPSHOT.json"),
        serde_json::to_string_pretty(&snap)?.as_bytes(),
    )?;
    fsync_dir(&a.out)?;
    rep.secs = t0.elapsed().as_secs_f64();
    Ok(rep)
}

/// A swap lock as taken (`r`), for the step `doing`. FV-1: a symlink planted at the lock
/// path (`ELOOP` anywhere in the cause chain; the open never follows it) fails the snapshot,
/// since without the lock a swap already in progress could pair files from both sides. Any
/// other cause (a read-only data dir where the lock file cannot be created, say) is logged and
/// the step runs on without the lock: no writer can run there either.
fn lock_or_warn(r: anyhow::Result<fs::File>, doing: &str) -> anyhow::Result<Option<fs::File>> {
    match r {
        Ok(f) => Ok(Some(f)),
        Err(e) if is_eloop(&e) => Err(e).context(format!(
            "snapshot: a symlink is planted at a swap lock path; {doing} without that lock is \
             refused (remove the symlink as the data dir's owner)"
        )),
        Err(e) => {
            tracing::warn!("snapshot: {doing} without the swap lock: {e:#}");
            Ok(None)
        }
    }
}

/// Whether `e`'s cause chain holds `ELOOP` (40 on Linux, 62 on macOS): an `O_NOFOLLOW` open
/// met a symlink. (`io::ErrorKind::FilesystemLoop` is not stable, so the raw errno is read.)
fn is_eloop(e: &anyhow::Error) -> bool {
    e.chain()
        .filter_map(|c| c.downcast_ref::<io::Error>())
        .any(|c| c.raw_os_error() == Some(Errno::LOOP.raw_os_error()))
}

/// `search/` into `out` (see the module doc): `(files, bytes, built_through_height)`, None
/// when the source has no built index.
fn snapshot_search(
    src: &Path,
    out: &Path,
    copy: bool,
) -> anyhow::Result<Option<(usize, u64, u32)>> {
    // No rebuild swap, stage commit or merge runs while the files are read and linked (a
    // source we cannot lock still gets the re-read checks of `live`).
    let _swap = match src.parent() {
        Some(d) => lock_or_warn(kdx_search::build::swap_lock(d), "copying search/")?,
        None => None,
    };
    for _ in 0..MAX_ATTEMPTS {
        let sf = match kdx_search::snapshot::live(src)? {
            kdx_search::snapshot::Live::None => return Ok(None),
            kdx_search::snapshot::Live::Changed => continue,
            kdx_search::snapshot::Live::Files(f) => f,
        };
        let m: kdx_search::SearchManifest =
            serde_json::from_slice(&sf.manifest).context("parse search/manifest.json")?;
        if out.exists() {
            fs::remove_dir_all(out)?;
        }
        fs::create_dir_all(out)?;
        let mut bytes = 0u64;
        let mut missing = false;
        for f in &sf.files {
            match place(&src.join(f), &out.join(f), copy) {
                Ok(n) => bytes += n,
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    missing = true;
                    break;
                }
                Err(e) => return Err(e).with_context(|| format!("place search/{f}")),
            }
        }
        if missing {
            continue;
        }
        replace_file(&out.join("meta.json"), &sf.meta)?;
        replace_file(&out.join(".managed.json"), &serde_json::to_vec(&sf.files)?)?;
        replace_file(&out.join("manifest.json"), &sf.manifest)?;
        fsync_dir(out)?;
        return Ok(Some((sf.files.len(), bytes, m.built_through_height)));
    }
    bail!("search/ kept changing under the snapshot ({MAX_ATTEMPTS} reads)")
}

/// Hard-link `src` to `dst`, or copy it durably; returns its size. `src` must be a regular
/// file: a symbolic link (or anything else) at its name is refused, and neither mode reads
/// through one. The copy opens `src` with `O_NOFOLLOW` and checks the open descriptor, and
/// creates `dst` with `O_CREAT | O_EXCL | O_NOFOLLOW`. The hard link is made without
/// following a symlink (`linkat` with no flags) and must be the inode checked before it.
fn place(src: &Path, dst: &Path, copy: bool) -> io::Result<u64> {
    if !copy {
        let m = fs::symlink_metadata(src)?;
        regular_file(src, &m)?;
        match fs::hard_link(src, dst) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::AlreadyExists => return Ok(0),
            Err(e) => return Err(e),
        }
        // `src` swapped for a symlink after the check: the link is that symlink.
        let l = fs::symlink_metadata(dst)?;
        if (l.dev(), l.ino()) != (m.dev(), m.ino()) {
            let _ = fs::remove_file(dst);
            return Err(io::Error::other(format!(
                "{} changed while it was linked",
                src.display()
            )));
        }
        return Ok(m.len());
    }
    // NONBLOCK and NOCTTY: a FIFO or a terminal at `src` neither blocks the open nor becomes
    // the controlling tty; the descriptor check below refuses it.
    let from = rustix::fs::open(
        src,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::NOCTTY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|e| match e {
        Errno::LOOP => io::Error::other(format!(
            "{} is a symbolic link: the snapshot never follows one",
            src.display()
        )),
        e => io::Error::from(e),
    })?;
    let mut from = fs::File::from(from);
    regular_file(src, &from.metadata()?)?;
    let to = rustix::fs::open(
        dst,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP | Mode::ROTH | Mode::WOTH,
    )?;
    let mut to = fs::File::from(to);
    let n = io::copy(&mut from, &mut to)?;
    to.sync_all()?;
    Ok(n)
}

/// Refuses anything but a regular file at `path` (`m`: its metadata, not following a link).
fn regular_file(path: &Path, m: &fs::Metadata) -> io::Result<()> {
    let t = m.file_type();
    if t.is_file() {
        return Ok(());
    }
    let what = if t.is_symlink() {
        "a symbolic link: the snapshot never follows one"
    } else if t.is_dir() {
        "a directory, not a regular file"
    } else {
        "not a regular file"
    };
    Err(io::Error::other(format!("{} is {what}", path.display())))
}

/// `YYYY-MM-DDTHH:MM:SSZ` of unix seconds (civil-from-days, proleptic Gregorian).
fn utc_iso(secs: u64) -> String {
    let (days, rem) = ((secs / 86_400) as i64, secs % 86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::time::Duration;

    use kdx_hot::{dict, HotStore, Txn};
    use kdx_hot::{Geometry, Table};
    use kdx_segment::manifest::{RangeEntry, RangeKind};
    use kdx_segment::Manifest;

    use super::{snapshot_with, utc_iso, Point, SnapshotArgs, MAX_NAME_WAITS};

    fn entry(start: u32, end: u32, first_txnum: u64) -> RangeEntry {
        RangeEntry {
            kind: RangeKind::Step,
            start,
            end,
            first_txnum,
            tx_count: 10,
            block_count: 10,
        }
    }

    fn put(dir: &Path, e: &RangeEntry) {
        for f in kdx_segment::files::range_file_names("net", e.start, e.end) {
            fs::write(dir.join(f), b"x").unwrap();
        }
    }

    /// One MDBX commit as a backfill range makes it: a name, the frozen frontier and
    /// `dict_lens@frontier`. Runs on its own thread (the caller may hold a read txn).
    fn commit_names(hs: &HotStore, frontier: u32, name: &[u8]) {
        std::thread::scope(|s| {
            s.spawn(|| {
                let mut t = hs.rw().unwrap();
                t.set_meta("test_name", name).unwrap();
                t.set_frozen_frontier(frontier).unwrap();
                t.set_meta(&dict::lens_key(frontier), &[0u8; 20]).unwrap();
                t.commit().unwrap();
            })
            .join()
            .unwrap();
        });
    }

    /// A merge replaces step files between the manifest read and the links: the snapshot
    /// reads the manifest again, links the merged range, drops the files of the old read and
    /// writes the new manifest bytes.
    #[test]
    fn snapshot_waits_for_a_segment_swap_in_progress() {
        // m14: a snapshot placing files across a `rebuild-range` commit point could pair old
        // and new files of one range. The swap holds `segments.swap.lock` exclusively; the
        // snapshot takes it shared before reading the manifest, so it waits.
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        let seg = data.join("segments");
        fs::create_dir_all(&seg).unwrap();
        let hs = HotStore::open(&data.join("hot"), Geometry::default()).unwrap();
        commit_names(&hs, 20, b"a");
        let a = entry(0, 20, 0);
        let mut m = Manifest::new("net");
        m.push(a.clone()).unwrap();
        m.commit(&seg).unwrap();
        put(&seg, &a);
        let swap = kdx_segment::swap::swap_lock(&seg, true).unwrap();
        let locked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let args = SnapshotArgs {
            data_dir: data.clone(),
            out: tmp.path().join("out"),
            copy: false,
        };
        let rep = std::thread::scope(|sc| {
            let l = locked.clone();
            let h = sc.spawn(|| {
                snapshot_with(&args, &hs, Duration::ZERO, &mut move |p, _| {
                    if p == Point::SwapLocked {
                        l.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                })
            });
            std::thread::sleep(Duration::from_millis(300));
            assert!(
                !locked.load(std::sync::atomic::Ordering::SeqCst),
                "the snapshot did not wait for the swap"
            );
            drop(swap);
            h.join().unwrap()
        })
        .unwrap();
        assert!(locked.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(rep.attempts, 1);
    }

    #[test]
    fn merge_between_manifest_read_and_links() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        let seg = data.join("segments");
        fs::create_dir_all(&seg).unwrap();
        let hs = HotStore::open(&data.join("hot"), Geometry::default()).unwrap();
        commit_names(&hs, 40, b"a");
        let (a, b, c) = (entry(0, 20, 0), entry(20, 40, 10), entry(0, 40, 0));
        let c = RangeEntry {
            tx_count: 20,
            kind: RangeKind::Range,
            ..c
        };
        let mut m1 = Manifest::new("net");
        m1.push(a.clone()).unwrap();
        m1.push(b.clone()).unwrap();
        m1.commit(&seg).unwrap();
        put(&seg, &a);
        put(&seg, &b);
        let mut m2 = Manifest::new("net");
        m2.push(c.clone()).unwrap();
        let out = tmp.path().join("out");
        let args = SnapshotArgs {
            data_dir: data.clone(),
            out: out.clone(),
            copy: false,
        };
        let mut reads = 0;
        let rep = snapshot_with(&args, &hs, Duration::ZERO, &mut |p, n| {
            if p != Point::Manifest {
                return;
            }
            reads = n;
            if n == 1 {
                // The merge: new range files, new manifest, then the step files go away
                // (after a was linked, b is gone before it is).
                put(&seg, &c);
                m2.commit(&seg).unwrap();
                for f in kdx_segment::files::range_file_names("net", b.start, b.end) {
                    fs::remove_file(seg.join(f)).unwrap();
                }
            }
        })
        .unwrap();
        assert_eq!(reads, 2);
        assert_eq!(rep.attempts, 2);
        let m = Manifest::load(&out.join("segments")).unwrap().unwrap();
        assert_eq!(m, m2);
        let mut have: Vec<String> = fs::read_dir(out.join("segments"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        have.sort();
        let mut want = kdx_segment::files::range_file_names("net", 0, 40);
        want.push("manifest.json".into());
        want.sort();
        assert_eq!(have, want);
        assert!(out.join("hot/mdbx.dat").is_file());
        assert!(out.join("SNAPSHOT.json").is_file());
        // A second snapshot into the non-empty dir is refused and leaves it as it was.
        assert!(snapshot_with(&args, &hs, Duration::ZERO, &mut |_, _| {}).is_err());
        assert!(out.join("SNAPSHOT.json").is_file());
    }

    /// I2: a backfill range commits its names after T began and its manifest before the
    /// manifest read. That read is refused (T cannot see the names), nothing is copied from
    /// T, and T plus the manifest are taken again; the hot copy then holds the names.
    #[test]
    fn manifest_newer_than_names_in_txn_is_reread() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        let seg = data.join("segments");
        fs::create_dir_all(&seg).unwrap();
        let hs = HotStore::open(&data.join("hot"), Geometry::default()).unwrap();
        commit_names(&hs, 20, b"old");
        let (a, b) = (entry(0, 20, 0), entry(20, 40, 10));
        let mut m1 = Manifest::new("net");
        m1.push(a.clone()).unwrap();
        m1.commit(&seg).unwrap();
        put(&seg, &a);
        let mut m2 = m1.clone();
        m2.push(b.clone()).unwrap();
        let out = tmp.path().join("out");
        let args = SnapshotArgs {
            data_dir: data.clone(),
            out: out.clone(),
            copy: false,
        };
        let mut seen = Vec::new();
        let rep = snapshot_with(&args, &hs, Duration::ZERO, &mut |p, n| {
            seen.push((p, n, out.join("hot/mdbx.dat").exists()));
            if (p, n) == (Point::Txn, 1) {
                // The backfill: MDBX (names, frontier 40, dict_lens@40), files, manifest.
                commit_names(&hs, 40, b"new");
                put(&seg, &b);
                m2.commit(&seg).unwrap();
            }
        })
        .unwrap();
        assert_eq!(rep.attempts, 2);
        // No hot copy was taken from the first T.
        assert_eq!(
            seen,
            vec![
                (Point::SwapLocked, 0, false),
                (Point::Txn, 1, false),
                (Point::Manifest, 1, false),
                (Point::Txn, 2, false),
                (Point::Manifest, 2, false),
            ]
        );
        assert_eq!(Manifest::load(&out.join("segments")).unwrap().unwrap(), m2);
        let copy = HotStore::open_ro(&out.join("hot")).unwrap();
        let t = copy.ro().unwrap();
        assert_eq!(t.frozen_frontier().unwrap(), 40);
        assert_eq!(
            t.get(Table::Meta, b"test_name").unwrap().as_deref(),
            Some(&b"new"[..])
        );
    }

    /// I2 and M5: a manifest whose names never reach the hot store is refused after
    /// `MAX_NAME_WAITS` reads; the snapshot leaves nothing behind (a directory it created is
    /// removed, an existing empty one is emptied). An `--out` inside the data dir is refused
    /// before anything is created.
    #[test]
    fn refusals_leave_nothing_behind() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        let seg = data.join("segments");
        fs::create_dir_all(&seg).unwrap();
        let hs = HotStore::open(&data.join("hot"), Geometry::default()).unwrap();
        let a = entry(0, 20, 0);
        let mut m = Manifest::new("net");
        m.push(a.clone()).unwrap();
        m.commit(&seg).unwrap();
        put(&seg, &a);
        let run = |out: &Path| {
            let args = SnapshotArgs {
                data_dir: data.clone(),
                out: out.to_path_buf(),
                copy: false,
            };
            let mut reads = 0;
            let e = snapshot_with(&args, &hs, Duration::ZERO, &mut |p, _| {
                reads += usize::from(p == Point::Manifest);
            })
            .unwrap_err();
            (e.to_string(), reads)
        };
        let out = tmp.path().join("new/out");
        let (e, reads) = run(&out);
        assert!(e.contains("dictionary names"), "{e}");
        assert_eq!(reads, MAX_NAME_WAITS);
        assert!(!tmp.path().join("new").exists());
        let empty = tmp.path().join("empty");
        fs::create_dir(&empty).unwrap();
        run(&empty);
        assert_eq!(fs::read_dir(&empty).unwrap().count(), 0);
        let inside = seg.join("x");
        let args = SnapshotArgs {
            data_dir: data.clone(),
            out: inside.clone(),
            copy: false,
        };
        let e = snapshot_with(&args, &hs, Duration::ZERO, &mut |_, _| {}).unwrap_err();
        assert!(e.to_string().contains("inside the data dir"), "{e}");
        assert!(!inside.exists());
    }

    #[test]
    fn iso_dates() {
        assert_eq!(utc_iso(0), "1970-01-01T00:00:00Z");
        assert_eq!(utc_iso(1_700_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(utc_iso(1_709_164_800), "2024-02-29T00:00:00Z");
    }

    /// Root may not snapshot a data dir it does not own (the service user's): the run would
    /// read and write as root in a tree that user controls. Root on a root-owned data dir and
    /// any other user run.
    #[test]
    fn root_is_refused_on_a_data_dir_it_does_not_own() {
        let e = super::refuse_root(0, 1001, &[]).unwrap_err().to_string();
        assert!(e.contains("sudo -u kda-indexer"), "{e}");
        assert!(e.contains("uid 1001"), "{e}");
        assert!(e.contains("install -d"), "{e}");
        super::refuse_root(0, 0, &[]).unwrap();
        super::refuse_root(1001, 1001, &[]).unwrap();
        super::refuse_root(1002, 1001, &[]).unwrap();
    }

    /// FV-3: root on a root-owned data dir is refused when `segments/` or the existing
    /// `--out` is not root-owned; the refusal names them. Other users are never refused.
    #[test]
    fn root_is_refused_below_a_root_owned_data_dir_it_does_not_own() {
        let (seg, out) = (
            std::path::PathBuf::from("/d/segments"),
            std::path::PathBuf::from("/o"),
        );
        for (below, bad) in [
            (
                vec![(seg.clone(), 1001), (out.clone(), 0)],
                "/d/segments (uid 1001)",
            ),
            (vec![(seg.clone(), 0), (out.clone(), 1001)], "/o (uid 1001)"),
        ] {
            let e = super::refuse_root(0, 0, &below).unwrap_err().to_string();
            assert!(e.contains("whole tree root owns"), "{e}");
            assert!(e.contains(bad), "{e}");
        }
        super::refuse_root(0, 0, &[(seg.clone(), 0), (out.clone(), 0)]).unwrap();
        super::refuse_root(1001, 1001, &[(seg, 5), (out, 6)]).unwrap();
    }

    /// FV-2: the root refusal at its call site. Run as euid 0 on a data dir root does not own
    /// (the test user's; chowned away from root when the tests run as root), the snapshot
    /// fails with the refusal before it opens or creates anything: no `<data>/hot/`, no
    /// `--out`, no `segments.swap.lock`.
    #[test]
    fn root_is_refused_before_anything_is_created() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        fs::create_dir_all(data.join("segments")).unwrap();
        if rustix::process::geteuid().is_root() {
            std::os::unix::fs::chown(&data, Some(65534), None).unwrap();
        }
        let out = tmp.path().join("out");
        let args = SnapshotArgs {
            data_dir: data.clone(),
            out: out.clone(),
            copy: false,
        };
        let e = format!("{:#}", super::snapshot_as(0, &args).unwrap_err());
        assert!(e.contains("refuses to run as root"), "{e}");
        assert!(!data.join("hot").exists());
        assert!(!out.exists());
        assert!(fs::symlink_metadata(data.join("segments.swap.lock")).is_err());
    }

    /// FV-3: as root the snapshot runs only on a data dir whose whole tree root owns. Run as
    /// euid 0 on a tree another uid owns (the test user's), the refusal names `segments/` and
    /// the existing `--out` next to the data dir, and the run leaves `--out` empty and
    /// creates nothing in the data dir. When the tests run as root, the data dir stays
    /// root-owned and only `segments/` and `--out` go to 65534, so the second refusal (the
    /// whole-tree rule) runs end to end; otherwise the data dir is another uid's and the
    /// first one (which also lists them) does.
    #[test]
    fn root_names_every_path_of_the_tree_it_does_not_own() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        let seg = data.join("segments");
        let out = tmp.path().join("out");
        fs::create_dir_all(&seg).unwrap();
        fs::create_dir(&out).unwrap();
        if rustix::process::geteuid().is_root() {
            for p in [&seg, &out] {
                std::os::unix::fs::chown(p, Some(65534), None).unwrap();
            }
        }
        let args = SnapshotArgs {
            data_dir: data.clone(),
            out: out.clone(),
            copy: false,
        };
        let e = format!("{:#}", super::snapshot_as(0, &args).unwrap_err());
        assert!(e.contains("refuses to run as root"), "{e}");
        assert!(e.contains(&format!("{} (uid", seg.display())), "{e}");
        assert!(e.contains(&format!("{} (uid", out.display())), "{e}");
        if rustix::process::geteuid().is_root() {
            assert!(e.contains("whole tree root owns"), "{e}");
        }
        assert_eq!(fs::read_dir(&out).unwrap().count(), 0);
        assert!(!data.join("hot").exists());
        assert!(fs::symlink_metadata(data.join("segments.swap.lock")).is_err());
    }

    /// FV-3 (Review PRE-F M-2): `hot/` and `search/` are part of the tree a root run owns: a
    /// root-owned data dir whose `hot/` or `search/` another uid owns is refused, naming it
    /// (the test user's own directory, chowned to 65534 when the tests run as root).
    #[test]
    fn root_is_refused_when_hot_or_search_is_not_root_owned() {
        for sub in ["hot", "search"] {
            let tmp = tempfile::tempdir().unwrap();
            let data = tmp.path().join("data");
            fs::create_dir_all(data.join(sub)).unwrap();
            let args = SnapshotArgs {
                data_dir: data.clone(),
                out: tmp.path().join("out"),
                copy: false,
            };
            if rustix::process::geteuid().is_root() {
                std::os::unix::fs::chown(data.join(sub), Some(65534), None).unwrap();
            }
            let owners = super::tree_owners(&args).unwrap();
            let listed = owners
                .iter()
                .find(|(p, _)| p == &data.join(sub))
                .unwrap_or_else(|| panic!("{sub} is not in tree_owners: {owners:?}"));
            assert_ne!(listed.1, 0, "{sub}");
            let mut below = owners;
            below.retain(|(p, _)| p == &data.join(sub));
            let e = super::refuse_root(0, 0, &below).unwrap_err().to_string();
            assert!(e.contains("whole tree root owns"), "{e}");
            assert!(e.contains(&data.join(sub).display().to_string()), "{e}");
        }
    }

    /// FV-3 (Review PRE-F M-1): as root an existing `--out`, even an empty one, is accepted
    /// only when root owns it; the owner is read where the directory is accepted, so one
    /// created after the early owner check is refused too. Other users, and a `--out` that
    /// does not exist yet, are unaffected.
    #[test]
    fn root_accepts_an_existing_empty_out_only_when_root_owns_it() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        fs::create_dir_all(&data).unwrap();
        let out = tmp.path().join("out");
        fs::create_dir(&out).unwrap();
        let is_root = rustix::process::geteuid().is_root();
        if is_root {
            std::os::unix::fs::chown(&out, Some(65534), None).unwrap();
        }
        let e = format!("{:#}", super::prepare_out(&out, &data, 0).unwrap_err());
        assert!(e.contains("refuses to run as root"), "{e}");
        assert!(e.contains("is not root-owned"), "{e}");
        assert_eq!(fs::read_dir(&out).unwrap().count(), 0);
        // Another euid is not refused on the owner.
        assert!(super::prepare_out(&out, &data, 1001).unwrap().is_none());
        // A missing `--out` is created as before, even under euid 0.
        let fresh = tmp.path().join("fresh");
        assert!(super::prepare_out(&fresh, &data, 0).unwrap().is_some());
        // Root owning an existing empty `--out` is accepted.
        if is_root {
            std::os::unix::fs::chown(&out, Some(0), None).unwrap();
            assert!(super::prepare_out(&out, &data, 0).unwrap().is_none());
        }
    }

    /// A data dir with one listed range `0..20` whose names the hot store sees, and the
    /// snapshot arguments for `out`.
    fn store_with_one_range(data: &Path, out: &Path, copy: bool) -> (HotStore, SnapshotArgs) {
        let seg = data.join("segments");
        fs::create_dir_all(&seg).unwrap();
        let hs = HotStore::open(&data.join("hot"), Geometry::default()).unwrap();
        commit_names(&hs, 20, b"a");
        let a = entry(0, 20, 0);
        let mut m = Manifest::new("net");
        m.push(a.clone()).unwrap();
        m.commit(&seg).unwrap();
        put(&seg, &a);
        let args = SnapshotArgs {
            data_dir: data.to_path_buf(),
            out: out.to_path_buf(),
            copy,
        };
        (hs, args)
    }

    /// Every regular file under `dir` (empty when it does not exist).
    fn files_under(dir: &Path) -> Vec<std::path::PathBuf> {
        let mut v = Vec::new();
        let mut todo = vec![dir.to_path_buf()];
        while let Some(d) = todo.pop() {
            let Ok(rd) = fs::read_dir(&d) else { continue };
            for e in rd {
                let p = e.unwrap().path();
                let t = fs::symlink_metadata(&p).unwrap().file_type();
                if t.is_dir() {
                    todo.push(p);
                } else if t.is_file() {
                    v.push(p);
                }
            }
        }
        v
    }

    /// A manifest whose `network` climbs out of `segments/` (`../../x`) would make the
    /// snapshot read `<data>/../x-*` and write `<out>/../x-*`, outside `--out`. The manifest
    /// read refuses it, before any segment file is placed, and the run leaves nothing.
    #[test]
    fn a_traversal_network_is_refused_before_anything_is_placed() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        let out = tmp.path().join("o/out");
        let (hs, args) = store_with_one_range(&data, &out, false);
        let seg = data.join("segments");
        // The hostile manifest, written past `Manifest::commit`'s own check.
        let mut v: serde_json::Value =
            serde_json::from_slice(&fs::read(seg.join("manifest.json")).unwrap()).unwrap();
        v["network"] = "../../x".into();
        fs::write(seg.join("manifest.json"), serde_json::to_vec(&v).unwrap()).unwrap();
        // Its files exist where the traversal points (`<tmp>/x-*`).
        for f in kdx_segment::files::range_file_names("../../x", 0, 20) {
            fs::write(seg.join(f), b"x").unwrap();
        }
        let e = snapshot_with(&args, &hs, Duration::ZERO, &mut |_, _| {}).unwrap_err();
        assert!(format!("{e:#}").contains("network"), "{e:#}");
        // `<out>/segments/../../x-*` is `<tmp>/o/x-*`: nothing was placed there, and the
        // directories the run created (`o`) are gone.
        assert!(!tmp.path().join("o").exists());
    }

    /// A listed segment file swapped for a symlink to a file outside the data dir: `--copy`
    /// refuses it without reading through the link, the hard-link mode refuses it too, and
    /// no copy of the target's bytes is left anywhere under `--out`.
    #[test]
    fn a_symlinked_segment_file_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        let secret = tmp.path().join("outside/secret");
        fs::create_dir_all(secret.parent().unwrap()).unwrap();
        fs::write(&secret, b"SECRET").unwrap();
        for copy in [true, false] {
            let out = tmp.path().join(format!("out-{copy}"));
            let (hs, args) = store_with_one_range(&data, &out, copy);
            let seg = data.join("segments");
            let victim = seg.join(&kdx_segment::files::range_file_names("net", 0, 20)[0]);
            fs::remove_file(&victim).unwrap();
            std::os::unix::fs::symlink(&secret, &victim).unwrap();
            let e = snapshot_with(&args, &hs, Duration::ZERO, &mut |_, _| {}).unwrap_err();
            assert!(
                format!("{e:#}").contains("symbolic link"),
                "copy {copy}: {e:#}"
            );
            assert!(!out.exists(), "copy {copy}");
            for f in files_under(tmp.path()) {
                if f != secret {
                    assert_ne!(fs::read(&f).unwrap(), b"SECRET", "{}", f.display());
                }
            }
            drop(hs);
            fs::remove_dir_all(&data).unwrap();
        }
        // `place` alone: a symlink, a directory and a dangling symlink, in both modes, and
        // nothing is created at the destination.
        let src = tmp.path().join("src");
        fs::create_dir(&src).unwrap();
        std::os::unix::fs::symlink(&secret, src.join("ln")).unwrap();
        std::os::unix::fs::symlink(tmp.path().join("none"), src.join("dangling")).unwrap();
        fs::create_dir(src.join("dir")).unwrap();
        let dst = tmp.path().join("dst");
        fs::create_dir(&dst).unwrap();
        for copy in [true, false] {
            for name in ["ln", "dangling", "dir"] {
                let e = super::place(&src.join(name), &dst.join(name), copy).unwrap_err();
                assert_ne!(e.kind(), std::io::ErrorKind::NotFound, "{name} copy {copy}");
                assert!(
                    fs::symlink_metadata(dst.join(name)).is_err(),
                    "{name} copy {copy}"
                );
            }
        }
        // A regular file is still placed in both modes.
        fs::write(src.join("ok"), b"ok").unwrap();
        assert_eq!(
            super::place(&src.join("ok"), &dst.join("ok1"), true).unwrap(),
            2
        );
        assert_eq!(
            super::place(&src.join("ok"), &dst.join("ok2"), false).unwrap(),
            2
        );
        assert_eq!(fs::read(dst.join("ok1")).unwrap(), b"ok");
        assert_eq!(fs::read(dst.join("ok2")).unwrap(), b"ok");
    }

    /// FV-1: a symlink planted at either swap lock path of the data dir is not followed (the
    /// target is never created) and fails the snapshot: without the lock a swap already in
    /// progress could pair files from both sides. The run leaves nothing under `--out`.
    #[test]
    fn planted_lock_symlinks_are_not_followed() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        for lock in ["segments.swap.lock", "search.swap.lock"] {
            let data = tmp.path().join(format!("data-{lock}"));
            let out = tmp.path().join(format!("out-{lock}"));
            let (hs, args) = store_with_one_range(&data, &out, false);
            std::os::unix::fs::symlink(outside.join(lock), data.join(lock)).unwrap();
            let e = snapshot_with(&args, &hs, Duration::ZERO, &mut |_, _| {}).unwrap_err();
            let e = format!("{e:#}");
            assert!(e.contains("symlink"), "{lock}: {e}");
            assert!(e.contains(lock), "{lock}: {e}");
            assert_eq!(fs::read_dir(&outside).unwrap().count(), 0, "{lock}");
            assert!(!out.exists(), "{lock}");
        }
    }

    /// FV-1: any other cause of a lock-open failure keeps warn-and-run, so a data dir where
    /// the lock files cannot be created can still be snapshotted: a directory at each lock
    /// path (`EISDIR`, also as root), and a read-only data dir (`EACCES`; root creates the
    /// lock files anyway).
    #[test]
    fn an_unopenable_lock_that_is_no_symlink_warns_and_runs() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        let (hs, args) = store_with_one_range(&data, &tmp.path().join("out-dirs"), false);
        let locks = ["segments.swap.lock", "search.swap.lock"];
        for lock in locks {
            fs::create_dir(data.join(lock)).unwrap();
        }
        snapshot_with(&args, &hs, Duration::ZERO, &mut |_, _| {}).unwrap();
        assert!(args.out.join("SNAPSHOT.json").is_file());
        for lock in locks {
            fs::remove_dir(data.join(lock)).unwrap();
        }
        let args = SnapshotArgs {
            out: tmp.path().join("out-ro"),
            ..args
        };
        fs::set_permissions(&data, fs::Permissions::from_mode(0o555)).unwrap();
        let r = snapshot_with(&args, &hs, Duration::ZERO, &mut |_, _| {});
        fs::set_permissions(&data, fs::Permissions::from_mode(0o755)).unwrap();
        r.unwrap();
        assert!(args.out.join("SNAPSHOT.json").is_file());
        if !rustix::process::geteuid().is_root() {
            for lock in locks {
                assert!(fs::symlink_metadata(data.join(lock)).is_err(), "{lock}");
            }
        }
    }
}
