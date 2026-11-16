//! File naming and durable (Ceph-aware) writes, spec §3.4.
//!
//! Names: `{network}-{file}-{start:09}-{end:09}.{ext}`, `end` exclusive. `txs` and `blocks`
//! are nippy-jar containers: the data file `.seg` plus its `.off` (offsets) and `.conf`
//! (config + user header) side files.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Write buffer for segment files (spec §3.4: 16 MB buffered writer).
pub const WRITE_BUF: usize = 16 << 20;

/// One logical file of a range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Txs,
    Blocks,
    Keys,
    Rk,
    Bh,
    Acct,
    Xfer,
    Pk,
    Evt,
    Code,
    /// `pact` postings (format v2).
    Pact,
    Range,
}

impl FileKind {
    pub const ALL: [FileKind; 12] = [
        FileKind::Txs,
        FileKind::Blocks,
        FileKind::Keys,
        FileKind::Rk,
        FileKind::Bh,
        FileKind::Acct,
        FileKind::Xfer,
        FileKind::Pk,
        FileKind::Evt,
        FileKind::Code,
        FileKind::Pact,
        FileKind::Range,
    ];
    pub fn name(self) -> &'static str {
        match self {
            FileKind::Txs => "txs",
            FileKind::Blocks => "blocks",
            FileKind::Keys => "keys",
            FileKind::Rk => "rk",
            FileKind::Bh => "bh",
            FileKind::Acct => "acct",
            FileKind::Xfer => "xfer",
            FileKind::Pk => "pk",
            FileKind::Evt => "evt",
            FileKind::Code => "code",
            FileKind::Pact => "pact",
            FileKind::Range => "range",
        }
    }
    pub fn ext(self) -> &'static str {
        match self {
            FileKind::Txs | FileKind::Blocks => "seg",
            FileKind::Keys => "ef",
            FileKind::Rk | FileKind::Bh => "idx",
            FileKind::Range => "toml",
            _ => "pst",
        }
    }
    /// Posting file of a family.
    pub fn of_family(f: kdx_decode::Family) -> FileKind {
        match f {
            kdx_decode::Family::Xfer => FileKind::Xfer,
            kdx_decode::Family::Acct => FileKind::Acct,
            kdx_decode::Family::Pk => FileKind::Pk,
            kdx_decode::Family::Evt => FileKind::Evt,
            kdx_decode::Family::Code => FileKind::Code,
            kdx_decode::Family::Pact => FileKind::Pact,
        }
    }
}

/// `{network}-{file}-{start:09}-{end:09}.{ext}`.
pub fn file_name(network: &str, kind: FileKind, start: u32, end: u32) -> String {
    format!(
        "{network}-{}-{start:09}-{end:09}.{}",
        kind.name(),
        kind.ext()
    )
}

/// Every on-disk file name of a range, side files of the nippy-jar containers included.
pub fn range_file_names(network: &str, start: u32, end: u32) -> Vec<String> {
    let mut v = Vec::new();
    for k in FileKind::ALL {
        let n = file_name(network, k, start, end);
        if matches!(k, FileKind::Txs | FileKind::Blocks) {
            let stem = n.trim_end_matches(".seg").to_string();
            v.push(format!("{stem}.off"));
            v.push(format!("{stem}.conf"));
        }
        v.push(n);
    }
    v
}

/// Temp directory a range is built in (same filesystem, renamed file by file).
pub fn tmp_dir(dir: &Path, network: &str, start: u32, end: u32) -> PathBuf {
    dir.join(format!(".tmp-{network}-{start:09}-{end:09}"))
}

/// Buffered writer that is flushed and `fsync`ed once by [`DurableFile::finish`].
pub struct DurableFile {
    w: BufWriter<File>,
    path: PathBuf,
}

impl DurableFile {
    pub fn create(path: &Path) -> Result<DurableFile> {
        let f = File::create(path).with_context(|| format!("create {}", path.display()))?;
        Ok(DurableFile {
            w: BufWriter::with_capacity(WRITE_BUF, f),
            path: path.to_path_buf(),
        })
    }
    pub fn finish(self) -> Result<()> {
        let f = self
            .w
            .into_inner()
            .map_err(|e| anyhow::anyhow!("flush {}: {}", self.path.display(), e.error()))?;
        f.sync_all()
            .with_context(|| format!("fsync {}", self.path.display()))?;
        Ok(())
    }
}

impl Write for DurableFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.w.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

/// Write a whole file durably: buffered write, one fsync.
pub fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = DurableFile::create(path)?;
    f.write_all(bytes)?;
    f.finish()
}

/// Opens (creating when missing) the lock file `path` for writing, never through a symlink:
/// `O_CREAT | O_NOFOLLOW`, so a symlink planted at `path` makes the open fail (`ELOOP`)
/// instead of creating or opening its target. The file stays between runs, so no `O_EXCL`.
pub fn open_lock_file(path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("open {} (not following a symlink)", path.display()))
}

/// `fsync` a directory (makes renames durable).
pub fn fsync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("fsync dir {}", dir.display()))
}

/// Atomically replace `path` with `bytes`: temp name, fsync, rename, fsync dir.
pub fn replace_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    write_file(&tmp, bytes)?;
    fs::rename(&tmp, path).with_context(|| format!("rename to {}", path.display()))?;
    fsync_dir(path.parent().unwrap_or(Path::new(".")))
}

/// blake3 of a file, hex.
pub fn blake3_file(path: &Path) -> Result<String> {
    let mut h = blake3::Hasher::new();
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    h.update_reader(f)
        .with_context(|| format!("read {}", path.display()))?;
    Ok(h.finalize().to_hex().to_string())
}

/// Delete every file of a range and its temp dir (used before rebuilding a range that is not
/// in the manifest). Missing files are ignored.
pub fn remove_range_files(dir: &Path, network: &str, start: u32, end: u32) -> Result<()> {
    for n in range_file_names(network, start, end) {
        match fs::remove_file(dir.join(&n)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("remove {n}")),
        }
    }
    let t = tmp_dir(dir, network, start, end);
    if t.exists() {
        fs::remove_dir_all(&t).with_context(|| format!("remove {}", t.display()))?;
    }
    fsync_dir(dir)
}
