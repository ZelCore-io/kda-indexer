//! `snapshot --out DIR`: a consistent copy of a data dir that can run beside a live `run`.
//!
//! Order (plan R-8): open the hot store read-only and begin a read txn T, then read
//! `segments/manifest.json`, then link (or copy) every file it references, then copy the
//! manifest bytes, then `copy_compact(T)` the hot store. T is older than the manifest, so
//! the manifest frontier is at or above the frozen frontier T sees and the hot copy can
//! only hold duplicate rows below the frontier, which freezer recovery resolves at `run`.
//! When a merge deletes a step file between the manifest read and the link, the manifest is
//! read again and the missing links are redone against it.
//!
//! Never copied: `etl/`, `LOCK`, `merkle-unverified.jsonl`, anything else outside
//! `segments/` and `hot/`. The snapshot is a data dir (`serve --data-dir DIR`, `run`).

use std::collections::HashSet;
use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use kdx_hot::env::meta;
use kdx_hot::{HotStore, Stage, Table, Txn};
use kdx_segment::files::{fsync_dir, replace_file};
use kdx_segment::Manifest;
use serde_json::json;

use super::hot::hot_dir;
use super::segments::segments_dir;

/// Manifest re-reads when a merge removes files under the snapshot.
const MAX_ATTEMPTS: usize = 5;

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
        .map(std::time::Duration::from_secs);
    snapshot_with(a, &mut |_| {
        if let Some(d) = sleep {
            std::thread::sleep(d);
        }
    })
}

/// `snapshot` with a hook called after each manifest read (argument: the read's number),
/// before its files are linked.
fn snapshot_with(
    a: &SnapshotArgs,
    after_manifest_read: &mut dyn FnMut(usize),
) -> anyhow::Result<SnapshotReport> {
    let t0 = Instant::now();
    let src_seg = segments_dir(&a.data_dir);
    let src_hot = hot_dir(&a.data_dir);
    match fs::read_dir(&a.out) {
        Ok(mut d) => {
            if d.next().is_some() {
                bail!("{} is not empty", a.out.display());
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("read {}", a.out.display())),
    }
    fs::create_dir_all(&a.out).with_context(|| format!("create {}", a.out.display()))?;
    let (out_abs, data_abs) = (fs::canonicalize(&a.out)?, fs::canonicalize(&a.data_dir)?);
    if out_abs.starts_with(&data_abs) {
        bail!("--out must not be inside the data dir");
    }
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

    // T first, then the manifest.
    let hs = HotStore::open_ro(&src_hot)?;
    let txn = hs.ro()?;
    let mut rep = SnapshotReport {
        linked: !a.copy,
        ..Default::default()
    };
    let mut have: HashSet<PathBuf> = HashSet::new();
    let (manifest_bytes, manifest) = loop {
        rep.attempts += 1;
        let bytes = match fs::read(src_seg.join("manifest.json")) {
            Ok(b) => Some(b),
            Err(e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => return Err(e).context("read manifest.json"),
        };
        let m = bytes.as_deref().map(Manifest::parse).transpose()?;
        after_manifest_read(rep.attempts);
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
            break (bytes, m);
        }
        if rep.attempts >= MAX_ATTEMPTS {
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

    hs.copy_compact(&txn, &out_hot)?;
    rep.hot_bytes = fs::metadata(out_hot.join("mdbx.dat"))?.len();
    fsync_dir(&out_hot)?;

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
        .map(|v| String::from_utf8_lossy(&v).into_owned())
        .or_else(|| manifest.as_ref().map(|m| m.network.clone()));
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
    let info = json!({
        "format": {"hot": kdx_hot::FORMAT_VERSION, "segments": kdx_segment::manifest::FORMAT_VERSION},
        "network": network,
        "frozen_frontier": manifest.as_ref().map_or(0, |m| m.frontier),
        "hot_frozen_frontier": u32_of(meta::FROZEN_FRONTIER),
        "next_txnum": u64_of(meta::NEXT_TXNUM),
        "tail_checkpoints": tail,
        "manifest_blake3": manifest_bytes.as_deref().map(|b| blake3::hash(b).to_hex().to_string()),
        "binary_version": env!("CARGO_PKG_VERSION"),
        "created_at": utc_iso(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()),
    });
    drop(txn);
    replace_file(
        &a.out.join("SNAPSHOT.json"),
        serde_json::to_string_pretty(&info)?.as_bytes(),
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

    use kdx_hot::{Geometry, HotStore};
    use kdx_segment::manifest::{RangeEntry, RangeKind};
    use kdx_segment::Manifest;

    use super::{snapshot_with, utc_iso, SnapshotArgs};

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
        drop(hs);
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
        let rep = snapshot_with(&args, &mut |n| {
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
        // A second snapshot into the non-empty dir is refused.
        assert!(snapshot_with(&args, &mut |_| {}).is_err());
    }

    #[test]
    fn iso_dates() {
        assert_eq!(utc_iso(0), "1970-01-01T00:00:00Z");
        assert_eq!(utc_iso(1_700_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(utc_iso(1_709_164_800), "2024-02-29T00:00:00Z");
    }
}
