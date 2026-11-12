//! Giving a finished snapshot to the data dir's owner (5E) without following a symlink.
//!
//! Run as root, `snapshot` gives what it wrote to the service user, who may write somewhere
//! in or above the copy meanwhile. A chown by path follows symlinks in every component but
//! the last, so a directory swapped for a symlink between a scan and the chown would send the
//! change elsewhere (to a file under `/etc`, say). Here no path reaches a chown: the root is
//! opened with `O_DIRECTORY | O_NOFOLLOW`, each entry is looked at with `fstatat(dirfd, name,
//! AT_SYMLINK_NOFOLLOW)`, opened with `openat(dirfd, name, O_NOFOLLOW ..)` (plus `O_DIRECTORY`
//! for a directory), checked again with `fstat` on that descriptor (same inode, same kind) and
//! changed with `fchown` on it. Symlinks are never followed or changed; a regular file with
//! more than one link (shared with the source data dir) and anything that is neither a
//! regular file nor a directory are left alone.
//!
//! Directories are changed after everything in them (post-order), so the new owner never
//! holds a directory the walk is still working in. A directory with an entry that could not
//! be changed keeps its owner, and so do the directories above it: a failure never leaves a
//! directory given with something under it that is not.

use std::ffi::{CStr, CString, OsStr};
use std::fs::File;
use std::io;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::fchown;
use std::path::{Path, PathBuf};

use rustix::fs::{fstat, openat, statat, AtFlags, Dir, FileType, Mode, OFlags, Stat};
use rustix::io::Errno;

/// A directory, never through a symlink in the last component.
const DIR_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
/// A regular file, only to `fstat` and `fchown` it: never through a symlink, and an entry
/// swapped for a FIFO or a terminal neither blocks the open nor becomes the controlling tty.
const FILE_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK)
    .union(OFlags::NOCTTY)
    .union(OFlags::CLOEXEC);

/// What [`tree`] did, by path relative to its root (the root itself is the empty path).
#[derive(Debug, Default)]
pub(super) struct Handoff {
    /// Entries given to the new owner, in the order they were changed.
    pub changed: Vec<PathBuf>,
    /// Entries left as they were, with why.
    pub failed: Vec<(PathBuf, io::Error)>,
}

/// Opens the directory `path` for [`tree`] or [`file_in`]; a symlink as its last component
/// is refused.
pub(super) fn open_dir(path: &Path) -> io::Result<File> {
    let fd = rustix::fs::open(path, DIR_FLAGS, Mode::empty())?;
    Ok(File::from(fd))
}

/// Gives the directory `root` and the directories and single-link regular files under it to
/// `uid:gid`, each directory after everything in it (see the module doc). `before` runs
/// before each change with the entry's path.
pub(super) fn tree(root: &File, uid: u32, gid: u32, before: &mut dyn FnMut(&Path)) -> Handoff {
    let mut w = Walk {
        uid,
        gid,
        before,
        done: Handoff::default(),
    };
    w.dir(root.as_fd(), Path::new(""));
    w.done
}

/// Gives the entry `name` of the directory `dir` to `uid:gid` when it is a regular file with
/// one link, checked on its own descriptor. Ok(false): absent or anything else, left alone.
pub(super) fn file_in(dir: &File, name: &str, uid: u32, gid: u32) -> io::Result<bool> {
    match open_entry(dir.as_fd(), &CString::new(name)?)? {
        Some((fd, Kind::File)) => {
            fchown(&fd, Some(uid), Some(gid))?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// What a handoff changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Dir,
    /// A regular file with one link.
    File,
}

fn kind(st: &Stat) -> Option<Kind> {
    match FileType::from_raw_mode(st.st_mode) {
        FileType::Directory => Some(Kind::Dir),
        FileType::RegularFile if st.st_nlink <= 1 => Some(Kind::File),
        _ => None,
    }
}

/// The entry `name` of `dir`, looked at without following a symlink and, when it is a
/// [`Kind`], opened as that and checked on the new descriptor: the same inode, still that
/// kind. None: absent, or anything else (left alone).
fn open_entry(dir: BorrowedFd<'_>, name: &CStr) -> io::Result<Option<(OwnedFd, Kind)>> {
    let st = match statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(st) => st,
        Err(Errno::NOENT) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let Some(k) = kind(&st) else {
        return Ok(None);
    };
    let flags = match k {
        Kind::Dir => DIR_FLAGS,
        Kind::File => FILE_FLAGS,
    };
    let fd = match openat(dir, name, flags, Mode::empty()) {
        Ok(fd) => fd,
        Err(Errno::NOENT) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let now = fstat(&fd)?;
    if (now.st_dev, now.st_ino) != (st.st_dev, st.st_ino) || kind(&now) != Some(k) {
        return Err(io::Error::other("replaced while being handed over"));
    }
    Ok(Some((fd, k)))
}

/// The names in the directory `fd` but `.` and `..`, sorted. Read through a descriptor of
/// their own, closed before this returns.
fn names(fd: BorrowedFd<'_>) -> io::Result<Vec<CString>> {
    let mut v = Vec::new();
    for e in Dir::read_from(fd)? {
        let e = e?;
        let n = e.file_name();
        if n.to_bytes() != b"." && n.to_bytes() != b".." {
            v.push(n.to_owned());
        }
    }
    v.sort();
    Ok(v)
}

struct Walk<'a> {
    uid: u32,
    gid: u32,
    before: &'a mut dyn FnMut(&Path),
    done: Handoff,
}

impl Walk<'_> {
    /// Everything in the directory `fd` (at `rel`), then the directory itself unless
    /// something in it was not given. Whether the directory was given.
    fn dir(&mut self, fd: BorrowedFd<'_>, rel: &Path) -> bool {
        let names = match names(fd) {
            Ok(n) => n,
            Err(e) => return self.fail(rel, e),
        };
        let mut all = true;
        for name in names {
            let at = rel.join(OsStr::from_bytes(name.to_bytes()));
            all &= match open_entry(fd, &name) {
                Ok(None) => true,
                Ok(Some((e, Kind::File))) => self.give(e.as_fd(), &at),
                Ok(Some((e, Kind::Dir))) => self.dir(e.as_fd(), &at),
                Err(e) => self.fail(&at, e),
            };
        }
        if !all {
            return self.fail(rel, io::Error::other("an entry in it was not given"));
        }
        self.give(fd, rel)
    }

    /// `fchown` of the open entry `fd` (at `rel`). Whether it was given.
    fn give(&mut self, fd: BorrowedFd<'_>, rel: &Path) -> bool {
        (self.before)(rel);
        match fchown(fd, Some(self.uid), Some(self.gid)) {
            Ok(()) => {
                self.done.changed.push(rel.to_path_buf());
                true
            }
            Err(e) => self.fail(rel, e),
        }
    }

    fn fail(&mut self, rel: &Path, e: io::Error) -> bool {
        self.done.failed.push((rel.to_path_buf(), e));
        false
    }
}

/// Tests: this user, the group its new files get in `dir`, and another group it may give its
/// own files to (the same group when it is in no other; a change then shows in no gid, only
/// in what [`tree`] reports).
#[cfg(test)]
pub(super) fn test_ids(dir: &Path) -> (u32, u32, u32) {
    let uid = rustix::process::geteuid().as_raw();
    let from = rustix::process::getegid().as_raw();
    // New entries take their directory's group on some systems (BSD): start `dir` with ours.
    std::os::unix::fs::lchown(dir, None, Some(from)).unwrap();
    let to = if uid == 0 {
        from ^ 1
    } else {
        rustix::process::getgroups()
            .unwrap_or_default()
            .into_iter()
            .map(|g| g.as_raw())
            .find(|&g| g != from)
            .unwrap_or(from)
    };
    (uid, from, to)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{symlink, MetadataExt};
    use std::path::{Path, PathBuf};

    use super::{file_in, open_dir, test_ids, tree, Handoff};

    struct Fx {
        tmp: tempfile::TempDir,
        uid: u32,
        from: u32,
        to: u32,
    }

    fn fx() -> Fx {
        let tmp = tempfile::tempdir().unwrap();
        let (uid, from, to) = test_ids(tmp.path());
        Fx { tmp, uid, from, to }
    }

    impl Fx {
        fn p(&self, rel: &str) -> PathBuf {
            self.tmp.path().join(rel)
        }

        fn walk(&self, root: &str, before: &mut dyn FnMut(&Path)) -> Handoff {
            tree(&open_dir(&self.p(root)).unwrap(), self.uid, self.to, before)
        }
    }

    fn gid(p: &Path) -> u32 {
        fs::symlink_metadata(p).unwrap().gid()
    }

    fn rels(v: &[&str]) -> Vec<PathBuf> {
        v.iter().map(PathBuf::from).collect()
    }

    /// Symlinks in the copy, to a directory and to a file outside it, are neither followed
    /// nor changed, and their targets are untouched.
    #[test]
    fn symlinks_out_of_the_tree_are_not_followed() {
        let f = fx();
        fs::create_dir_all(f.p("out/a")).unwrap();
        fs::create_dir(f.p("outside")).unwrap();
        fs::write(f.p("out/a/f"), b"x").unwrap();
        fs::write(f.p("outside/secret"), b"s").unwrap();
        symlink(f.p("outside"), f.p("out/dir_ln")).unwrap();
        symlink(f.p("outside/secret"), f.p("out/a/file_ln")).unwrap();
        let mut seen = Vec::new();
        let h = f.walk("out", &mut |p| seen.push(p.to_path_buf()));
        assert!(h.failed.is_empty(), "{:?}", h.failed);
        assert_eq!(h.changed, rels(&["a/f", "a", ""]));
        assert_eq!(seen, h.changed);
        for p in ["outside", "outside/secret", "out/dir_ln", "out/a/file_ln"] {
            assert_eq!(gid(&f.p(p)), f.from, "{p}");
        }
        for p in ["out", "out/a", "out/a/f"] {
            assert_eq!(gid(&f.p(p)), f.to, "{p}");
        }
    }

    /// A file hard-linked with the source data dir keeps its owner; the rest is given.
    #[test]
    fn hard_linked_files_are_skipped() {
        let f = fx();
        fs::create_dir(f.p("out")).unwrap();
        fs::write(f.p("src.seg"), b"x").unwrap();
        fs::hard_link(f.p("src.seg"), f.p("out/linked.seg")).unwrap();
        fs::write(f.p("out/own"), b"y").unwrap();
        let h = f.walk("out", &mut |_| {});
        assert!(h.failed.is_empty(), "{:?}", h.failed);
        assert_eq!(h.changed, rels(&["own", ""]));
        assert_eq!(gid(&f.p("src.seg")), f.from);
        assert_eq!(gid(&f.p("out/own")), f.to);
    }

    /// Post-order: every directory is changed after everything under it, the root last.
    #[test]
    fn directories_after_their_contents() {
        let f = fx();
        fs::create_dir_all(f.p("out/a/b")).unwrap();
        for p in ["out/a/b/x", "out/a/y", "out/z"] {
            fs::write(f.p(p), b"x").unwrap();
        }
        let mut seen = Vec::new();
        let h = f.walk("out", &mut |p| seen.push(p.to_path_buf()));
        assert!(h.failed.is_empty(), "{:?}", h.failed);
        assert_eq!(h.changed, rels(&["a/b/x", "a/b", "a/y", "a", "z", ""]));
        assert_eq!(seen, h.changed);
        for (i, d) in h.changed.iter().enumerate() {
            assert!(
                !h.changed[i + 1..].iter().any(|later| later.starts_with(d)),
                "{} changed before an entry under it",
                d.display()
            );
        }
    }

    /// The race of the finding: while the walk is inside `a`, `a` is moved away and a
    /// symlink to a directory outside the copy takes its name. The changes still reach the
    /// directory the walk opened (now `a.moved`), never the symlink's target.
    #[test]
    fn directory_swapped_for_a_symlink_mid_walk() {
        let f = fx();
        fs::create_dir_all(f.p("out/a")).unwrap();
        fs::create_dir(f.p("outside")).unwrap();
        for p in ["out/a/f", "out/a/g", "outside/f"] {
            fs::write(f.p(p), b"x").unwrap();
        }
        let mut swapped = false;
        let h = f.walk("out", &mut |p| {
            if p.starts_with("a") && !swapped {
                fs::rename(f.p("out/a"), f.p("out/a.moved")).unwrap();
                symlink(f.p("outside"), f.p("out/a")).unwrap();
                swapped = true;
            }
        });
        assert!(swapped);
        assert!(h.failed.is_empty(), "{:?}", h.failed);
        assert_eq!(h.changed, rels(&["a/f", "a/g", "a", ""]));
        for p in ["outside", "outside/f", "out/a"] {
            assert_eq!(gid(&f.p(p)), f.from, "{p}");
        }
        for p in ["out/a.moved", "out/a.moved/f", "out/a.moved/g"] {
            assert_eq!(gid(&f.p(p)), f.to, "{p}");
        }
    }

    /// An entry that cannot be read fails, and the directories above it keep their owner;
    /// its siblings are still given.
    #[test]
    fn a_failure_keeps_the_directories_above_it() {
        use std::os::unix::fs::PermissionsExt;
        let f = fx();
        if f.uid == 0 {
            return; // root reads a mode-000 directory
        }
        fs::create_dir_all(f.p("out/a/locked")).unwrap();
        fs::write(f.p("out/a/f"), b"x").unwrap();
        fs::write(f.p("out/z"), b"x").unwrap();
        fs::set_permissions(f.p("out/a/locked"), fs::Permissions::from_mode(0o000)).unwrap();
        let h = f.walk("out", &mut |_| {});
        fs::set_permissions(f.p("out/a/locked"), fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(h.changed, rels(&["a/f", "z"]));
        let failed: Vec<_> = h.failed.iter().map(|(p, _)| p.clone()).collect();
        assert_eq!(failed, rels(&["a/locked", "a", ""]));
        for p in ["out", "out/a", "out/a/locked"] {
            assert_eq!(gid(&f.p(p)), f.from, "{p}");
        }
    }

    /// A lock file is given only as a regular file with one link; a symlink of that name
    /// (target untouched), a hard link, a directory or nothing are left alone.
    #[test]
    fn lock_file_only_as_a_single_link_regular_file() {
        let f = fx();
        fs::create_dir(f.p("data")).unwrap();
        fs::write(f.p("secret"), b"s").unwrap();
        fs::write(f.p("data/plain.lock"), b"").unwrap();
        symlink(f.p("secret"), f.p("data/sym.lock")).unwrap();
        fs::hard_link(f.p("secret"), f.p("data/hard.lock")).unwrap();
        fs::create_dir(f.p("data/dir.lock")).unwrap();
        let d = open_dir(&f.p("data")).unwrap();
        let give = |n: &str| file_in(&d, n, f.uid, f.to).unwrap();
        assert!(give("plain.lock"));
        for n in ["sym.lock", "hard.lock", "dir.lock", "missing.lock"] {
            assert!(!give(n), "{n}");
        }
        assert_eq!(gid(&f.p("data/plain.lock")), f.to);
        for p in ["secret", "data/sym.lock", "data/dir.lock", "data"] {
            assert_eq!(gid(&f.p(p)), f.from, "{p}");
        }
        // The root of a walk: a symlink as its last component is refused.
        symlink(f.p("data"), f.p("data_ln")).unwrap();
        assert!(open_dir(&f.p("data_ln")).is_err());
    }
}
