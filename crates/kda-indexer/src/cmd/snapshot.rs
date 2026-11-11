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
//! Never copied: `etl/`, `LOCK`, `merkle-unverified.jsonl`, anything else outside
//! `segments/` and `hot/`. The snapshot is a data dir (`serve --data-dir DIR`, `run`).
//! On a refusal nothing is created; on a later failure what this run created is removed.

use std::collections::HashSet;
use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use kdx_hot::env::meta;
use kdx_hot::{dict, HotStore, RoTxn, Stage, Table, Txn};
use kdx_segment::files::{fsync_dir, replace_file};
use kdx_segment::Manifest;
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
    pub secs: f64,
}

/// Where `snapshot_with` calls its hook (argument two: the manifest read's number).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Point {
    /// T has begun; the manifest is not read yet.
    Txn,
    /// The manifest was read; nothing of this read is copied or linked yet.
    Manifest,
}

pub fn run(a: SnapshotArgs) -> anyhow::Result<()> {
    let r = snapshot(&a)?;
    println!(
        "snapshot {}: {} segment files {} ({} B), hot {} B, {} manifest read(s), {:.1} s",
        a.out.display(),
        r.files,
        if r.linked { "linked" } else { "copied" },
        r.segment_bytes,
        r.hot_bytes,
        r.attempts,
        r.secs
    );
    Ok(())
}

pub fn snapshot(a: &SnapshotArgs) -> anyhow::Result<SnapshotReport> {
    // Debug hook `KDX_DEBUG_SLEEP_AT=snapshot-after-manifest:<secs>`: widen the window between
    // the manifest read and the links (a merge can delete step files in it).
    let sleep = std::env::var("KDX_DEBUG_SLEEP_AT")
        .ok()
        .and_then(|v| {
            let (p, n) = v.split_once(':')?;
            (p == "snapshot-after-manifest").then(|| n.parse::<u64>().ok())?
        })
        .map(Duration::from_secs);
    let hs = HotStore::open_ro(&hot_dir(&a.data_dir))?;
    snapshot_with(a, &hs, NAME_WAIT_PAUSE, &mut |p, _| {
        if let (Point::Manifest, Some(d)) = (p, sleep) {
            std::thread::sleep(d);
        }
    })
}

/// `snapshot` of the hot store `hs` (opened on the data dir's `hot/`), with a hook at each
/// [`Point`] and the pause before a re-read for missing names.
fn snapshot_with(
    a: &SnapshotArgs,
    hs: &HotStore,
    pause: Duration,
    hook: &mut dyn FnMut(Point, usize),
) -> anyhow::Result<SnapshotReport> {
    let created = prepare_out(&a.out, &a.data_dir)?;
    let r = snapshot_into(a, hs, pause, hook);
    if r.is_err() {
        remove_created(&a.out, created.as_deref());
    }
    r
}

/// Refuses an `--out` inside the data dir or not empty before creating anything, then creates
/// it. Returns the topmost directory this created (None: `--out` existed, empty).
fn prepare_out(out: &Path, data_dir: &Path) -> anyhow::Result<Option<PathBuf>> {
    let data_abs =
        fs::canonicalize(data_dir).with_context(|| format!("resolve {}", data_dir.display()))?;
    let out_abs = resolve(out)?;
    if out_abs.starts_with(&data_abs) {
        bail!("--out must not be inside the data dir");
    }
    match fs::read_dir(out) {
        Ok(mut d) => {
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
                match place(&src_seg.join(&f), &out_seg.join(&f), a.copy) {
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

/// Hard-link `src` to `dst`, or copy it durably; returns its size.
fn place(src: &Path, dst: &Path, copy: bool) -> std::io::Result<u64> {
    if !copy {
        let n = fs::metadata(src)?.len();
        return match fs::hard_link(src, dst) {
            Ok(()) => Ok(n),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(0),
            Err(e) => Err(e),
        };
    }
    let n = fs::copy(src, dst)?;
    fs::File::open(dst)?.sync_all()?;
    Ok(n)
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
}
