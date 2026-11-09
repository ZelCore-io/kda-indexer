//! Small external-sort collector (the role reth-etl plays in reth; written here because
//! reth-etl drags in the reth/alloy tree). Byte-string key/value pairs are buffered up to a
//! memory budget, spilled as sorted runs (zstd level 1) into a directory, and read back in
//! key order through a k-way merge. Equal keys come back in push order. Run files are
//! deleted when the merge iterator is dropped; [`clear_dir`] removes leftovers on resume.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Per-entry bookkeeping counted against the budget on top of key and value bytes.
const ENTRY_OVERHEAD: usize = 64;

pub struct Collector {
    dir: PathBuf,
    name: String,
    buf: Vec<(Vec<u8>, Vec<u8>)>,
    buf_bytes: usize,
    limit: usize,
    runs: Vec<PathBuf>,
    len: u64,
    spilled_bytes: u64,
}

impl Collector {
    /// `limit_bytes` = buffered key+value bytes before a sorted run is spilled into `dir`.
    pub fn new(dir: &Path, name: &str, limit_bytes: usize) -> Result<Collector> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        Ok(Collector {
            dir: dir.to_path_buf(),
            name: name.to_string(),
            buf: Vec::new(),
            buf_bytes: 0,
            limit: limit_bytes.max(1 << 20),
            runs: Vec::new(),
            len: 0,
            spilled_bytes: 0,
        })
    }

    pub fn push(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.buf_bytes += key.len() + value.len() + ENTRY_OVERHEAD;
        self.buf.push((key, value));
        self.len += 1;
        if self.buf_bytes >= self.limit {
            self.spill()?;
        }
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Sorted runs written so far.
    pub fn runs(&self) -> usize {
        self.runs.len()
    }

    /// Compressed bytes spilled so far.
    pub fn spilled_bytes(&self) -> u64 {
        self.spilled_bytes
    }

    fn spill(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        // Stable: equal keys keep push order inside a run.
        self.buf.sort_by(|a, b| a.0.cmp(&b.0));
        let p = self
            .dir
            .join(format!("{}-{:05}.run", self.name, self.runs.len()));
        let f = File::create(&p).with_context(|| format!("create {}", p.display()))?;
        let mut w = zstd::Encoder::new(BufWriter::with_capacity(1 << 20, f), 1)?;
        for (k, v) in self.buf.drain(..) {
            w.write_all(&(k.len() as u32).to_le_bytes())?;
            w.write_all(&(v.len() as u32).to_le_bytes())?;
            w.write_all(&k)?;
            w.write_all(&v)?;
        }
        let mut bw = w.finish()?;
        bw.flush()?;
        self.spilled_bytes += fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
        self.buf = Vec::new();
        self.buf_bytes = 0;
        self.runs.push(p);
        Ok(())
    }

    /// All pairs in key order. With no spilled run the buffer is sorted in memory.
    pub fn into_sorted(mut self) -> Result<MergeIter> {
        if self.runs.is_empty() {
            self.buf.sort_by(|a, b| a.0.cmp(&b.0));
            return Ok(MergeIter {
                mem: std::mem::take(&mut self.buf).into_iter(),
                readers: Vec::new(),
                heap: BinaryHeap::new(),
                runs: Vec::new(),
            });
        }
        self.spill()?;
        let mut readers = Vec::with_capacity(self.runs.len());
        let mut heap = BinaryHeap::with_capacity(self.runs.len());
        for (i, p) in self.runs.iter().enumerate() {
            let f = File::open(p).with_context(|| format!("open {}", p.display()))?;
            let mut r = RunReader {
                z: zstd::Decoder::with_buffer(BufReader::with_capacity(1 << 20, f))?,
            };
            if let Some((k, v)) = r.next()? {
                heap.push(Head {
                    key: k,
                    val: v,
                    run: i,
                });
            }
            readers.push(r);
        }
        Ok(MergeIter {
            mem: Vec::new().into_iter(),
            readers,
            heap,
            runs: std::mem::take(&mut self.runs),
        })
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        for p in &self.runs {
            let _ = fs::remove_file(p);
        }
    }
}

struct RunReader {
    z: zstd::Decoder<'static, BufReader<File>>,
}

impl RunReader {
    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut lens = [0u8; 8];
        match self.z.read_exact(&mut lens) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let kl = u32::from_le_bytes(lens[..4].try_into().unwrap()) as usize;
        let vl = u32::from_le_bytes(lens[4..].try_into().unwrap()) as usize;
        let mut k = vec![0u8; kl];
        let mut v = vec![0u8; vl];
        self.z
            .read_exact(&mut k)
            .context("etl run: truncated key")?;
        self.z
            .read_exact(&mut v)
            .context("etl run: truncated value")?;
        Ok(Some((k, v)))
    }
}

struct Head {
    key: Vec<u8>,
    val: Vec<u8>,
    run: usize,
}

impl PartialEq for Head {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Head {
    /// Reversed: `BinaryHeap` is a max-heap and we want the smallest (key, run) first.
    fn cmp(&self, o: &Self) -> Ordering {
        o.key.cmp(&self.key).then(o.run.cmp(&self.run))
    }
}

/// K-way merge over the spilled runs (or the in-memory buffer when nothing spilled).
pub struct MergeIter {
    mem: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
    readers: Vec<RunReader>,
    heap: BinaryHeap<Head>,
    runs: Vec<PathBuf>,
}

impl MergeIter {
    fn next_inner(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.readers.is_empty() {
            return Ok(self.mem.next());
        }
        let Some(h) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some((k, v)) = self.readers[h.run].next()? {
            self.heap.push(Head {
                key: k,
                val: v,
                run: h.run,
            });
        }
        Ok(Some((h.key, h.val)))
    }
}

impl Iterator for MergeIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner().transpose()
    }
}

impl Drop for MergeIter {
    fn drop(&mut self) {
        self.readers.clear();
        for p in &self.runs {
            let _ = fs::remove_file(p);
        }
    }
}

/// Remove everything inside `dir` (resume: spools of an interrupted run are never reused).
pub fn clear_dir(dir: &Path) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for e in fs::read_dir(dir)? {
        let p = e?.path();
        if p.is_dir() {
            fs::remove_dir_all(&p)
        } else {
            fs::remove_file(&p)
        }
        .with_context(|| format!("remove {}", p.display()))?;
    }
    Ok(())
}
