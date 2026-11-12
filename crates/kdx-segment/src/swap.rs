//! Crash-safe replacement of one range's files (`rebuild-range`, T-4).
//!
//! The rebuilt files have the same names as the files they replace, so they are written to
//! `segments/rebuild.<start>/` first. The commit point is the journal
//! `rebuild.<start>/rebuild.json` (temp + rename). Before it exists, the rebuild dir is
//! ignored and removed by [`recover`]. Once it exists, the rebuilt set is the range: readers
//! take every file from the rebuild dir ([`resolve`]) while the swap puts each one over the
//! old file in `segments/` (hard link + atomic rename), and the journal is removed only
//! after the whole set is in place. A crash at any point leaves a store that reads either
//! the old set or the new one, never a mix. [`recover`] (run by every writer at startup)
//! finishes a committed swap and removes an uncommitted one.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::files::{fsync_dir, open_lock_file, replace_file};

/// Journal file name inside a rebuild dir.
pub const JOURNAL: &str = "rebuild.json";

/// `segments/rebuild.<start:09>`.
pub fn rebuild_dir(segdir: &Path, start: u32) -> PathBuf {
    segdir.join(format!("rebuild.{start:09}"))
}

/// Range start encoded in a segment file name (`{network}-{kind}-{start}-{end}.{ext}`).
fn start_of(name: &str) -> Option<u32> {
    let stem = name.rsplit_once('.')?.0;
    let mut it = stem.rsplit('-');
    let _end = it.next()?;
    it.next()?.parse().ok()
}

/// Path of a segment file, honouring a committed, not yet finished swap of its range.
pub fn resolve(segdir: &Path, name: &str) -> PathBuf {
    if let Some(start) = start_of(name) {
        let d = rebuild_dir(segdir, start);
        if d.join(JOURNAL).exists() {
            let p = d.join(name);
            if p.exists() {
                return p;
            }
        }
    }
    segdir.join(name)
}

/// True when a committed swap of the range starting at `start` is pending.
pub fn pending(segdir: &Path, start: u32) -> bool {
    rebuild_dir(segdir, start).join(JOURNAL).exists()
}

/// `<data dir>/segments.swap.lock`. `rebuild-range` holds it exclusively while it recovers
/// or swaps a range; `snapshot` holds it shared while it places segment files, so a snapshot
/// never pairs files from both sides of a swap (m14). It sits outside `segments/`, so
/// `remove_unlisted` never deletes it and a snapshot never copies it. Blocks until granted.
/// A symlink at the lock path is refused, never followed ([`open_lock_file`]).
pub fn swap_lock(segdir: &Path, exclusive: bool) -> Result<fs::File> {
    let p = segdir.parent().unwrap_or(segdir).join("segments.swap.lock");
    let f = open_lock_file(&p)?;
    if exclusive { f.lock() } else { f.lock_shared() }
        .with_context(|| format!("lock {}", p.display()))?;
    Ok(f)
}

/// Commit point: write the journal (temp + rename + dir fsync).
pub fn commit_journal(segdir: &Path, start: u32, journal: &[u8]) -> Result<()> {
    replace_file(&rebuild_dir(segdir, start).join(JOURNAL), journal)
}

/// Put every rebuilt file over the old one: hard link it to `segments/.swap-<name>`, then
/// rename that over `<name>` (atomic per file). The rebuild dir keeps its files, so while
/// the journal exists readers take the whole new set from there (a container's side files
/// are found next to its main file). `mid` runs after the first file (crash-test hook).
/// Idempotent.
pub fn finish(segdir: &Path, start: u32, names: &[String], mid: &dyn Fn()) -> Result<()> {
    let d = rebuild_dir(segdir, start);
    for (i, n) in names.iter().enumerate() {
        let tmp = segdir.join(format!(".swap-{n}"));
        let _ = fs::remove_file(&tmp);
        fs::hard_link(d.join(n), &tmp).with_context(|| format!("link {n}"))?;
        fs::rename(&tmp, segdir.join(n)).with_context(|| format!("swap {n}"))?;
        // When `<name>` already is this inode (a finish run again by `recover`), POSIX
        // `rename` does nothing and both names stay: drop the `.swap-` link (m8).
        let _ = fs::remove_file(&tmp);
        if i == 0 {
            fsync_dir(segdir)?;
            mid();
        }
    }
    fsync_dir(segdir)?;
    Ok(())
}

/// Remove a rebuild dir. The journal goes first: after a finished swap `segments/` holds
/// the whole new set, so readers stop looking into the rebuild dir before its files go (a
/// crash in between leaves an uncommitted dir that [`recover`] drops).
pub fn remove(segdir: &Path, start: u32) -> Result<()> {
    let d = rebuild_dir(segdir, start);
    if !d.exists() {
        return Ok(());
    }
    if d.join(JOURNAL).exists() {
        fs::remove_file(d.join(JOURNAL))?;
        fsync_dir(&d)?;
    }
    fs::remove_dir_all(&d)?;
    fsync_dir(segdir)?;
    Ok(())
}

/// Names of the files in a committed rebuild dir (journal excluded), `range.toml` last.
fn rebuilt_names(d: &Path) -> Result<Vec<String>> {
    let mut v: Vec<String> = fs::read_dir(d)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n != JOURNAL && !n.ends_with(".tmp"))
        .collect();
    v.sort_by_key(|n| (n.ends_with(".toml"), n.clone()));
    Ok(v)
}

/// Startup recovery for every `rebuild.*` dir: finish a committed swap, drop the rest.
/// Returns one line per dir handled.
pub fn recover(segdir: &Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    if !segdir.exists() {
        return Ok(out);
    }
    for e in fs::read_dir(segdir)? {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        if let Some(n) = name.strip_prefix(".swap-") {
            // A link of an interrupted swap; `finish` makes it again.
            fs::remove_file(e.path()).with_context(|| format!("remove .swap-{n}"))?;
            continue;
        }
        let Some(start) = name
            .strip_prefix("rebuild.")
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        if pending(segdir, start) {
            let names = rebuilt_names(&e.path())?;
            finish(segdir, start, &names, &|| {})?;
            out.push(format!("{name}: finished a committed swap"));
        } else {
            out.push(format!("{name}: removed an uncommitted rebuild"));
        }
        remove(segdir, start)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_is_parsed_from_file_names() {
        assert_eq!(start_of("mainnet01-txs-000000100-000000200.seg"), Some(100));
        assert_eq!(start_of("a-b-range-000000007-000000009.toml"), Some(7));
        assert_eq!(start_of("manifest.json"), None);
    }

    #[test]
    fn resolve_prefers_a_committed_rebuild_and_recover_finishes_it() {
        let t = tempfile::tempdir().unwrap();
        let seg = t.path();
        let n = "net-txs-000000010-000000020.seg";
        fs::write(seg.join(n), b"old").unwrap();
        let d = rebuild_dir(seg, 10);
        fs::create_dir_all(&d).unwrap();
        fs::write(d.join(n), b"new").unwrap();
        // Uncommitted: ignored, and recover drops it.
        assert_eq!(resolve(seg, n), seg.join(n));
        commit_journal(seg, 10, b"{}").unwrap();
        assert_eq!(resolve(seg, n), d.join(n));
        fs::write(seg.join(format!(".swap-{n}")), b"stale").unwrap();
        let lines = recover(seg).unwrap();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert_eq!(fs::read(seg.join(n)).unwrap(), b"new");
        assert!(!d.exists());
        assert!(!seg.join(format!(".swap-{n}")).exists());

        fs::create_dir_all(&d).unwrap();
        fs::write(d.join(n), b"newer").unwrap();
        recover(seg).unwrap();
        assert_eq!(fs::read(seg.join(n)).unwrap(), b"new");
        assert!(!d.exists());
    }

    #[test]
    fn finish_run_again_after_a_finished_swap_leaves_no_swap_links() {
        // m8 (T9): a crash after `finish` but before `remove` makes `recover` finish again.
        // `<name>` is then the same inode as the rebuilt file, so `rename(.swap-n, n)` is a
        // no-op and the `.swap-` link stayed, holding the old inode's space.
        let t = tempfile::tempdir().unwrap();
        let seg = t.path().join("segments");
        fs::create_dir_all(&seg).unwrap();
        let names: Vec<String> = [
            "net-txs-000000010-000000020.seg",
            "net-range-000000010-000000020.toml",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let d = rebuild_dir(&seg, 10);
        fs::create_dir_all(&d).unwrap();
        for n in &names {
            fs::write(seg.join(n), b"old").unwrap();
            fs::write(d.join(n), b"new").unwrap();
        }
        commit_journal(&seg, 10, b"{}").unwrap();
        finish(&seg, 10, &names, &|| {}).unwrap();
        let lines = recover(&seg).unwrap();
        assert_eq!(lines.len(), 1, "{lines:?}");
        let left: Vec<String> = fs::read_dir(&seg)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".swap-") || n.starts_with("rebuild."))
            .collect();
        assert!(left.is_empty(), "{left:?}");
        for n in &names {
            assert_eq!(fs::read(seg.join(n)).unwrap(), b"new");
        }
    }

    #[test]
    fn swap_lock_excludes_shared_holders() {
        let t = tempfile::tempdir().unwrap();
        let seg = t.path().join("segments");
        fs::create_dir_all(&seg).unwrap();
        let ex = swap_lock(&seg, true).unwrap();
        let p = t.path().join("segments.swap.lock");
        let other = fs::OpenOptions::new().write(true).open(&p).unwrap();
        assert!(matches!(
            other.try_lock_shared(),
            Err(fs::TryLockError::WouldBlock)
        ));
        drop(ex);
        let a = swap_lock(&seg, false).unwrap();
        other.try_lock_shared().unwrap();
        assert!(matches!(
            other.try_lock(),
            Err(fs::TryLockError::WouldBlock)
        ));
        drop(a);
    }

    /// A symlink planted at `segments.swap.lock` (to a file that does not exist, or to one
    /// that does) is refused: the target is neither created nor opened.
    #[test]
    fn swap_lock_never_follows_a_symlink() {
        let t = tempfile::tempdir().unwrap();
        let seg = t.path().join("segments");
        fs::create_dir_all(&seg).unwrap();
        let target = t.path().join("outside");
        std::os::unix::fs::symlink(&target, t.path().join("segments.swap.lock")).unwrap();
        for exclusive in [true, false] {
            assert!(swap_lock(&seg, exclusive).is_err());
            assert!(!target.exists());
        }
        // An existing target is not opened (following the link would take this shared lock).
        fs::write(&target, b"keep").unwrap();
        let held = fs::File::open(&target).unwrap();
        held.lock_shared().unwrap();
        assert!(swap_lock(&seg, false).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"keep");
    }
}
