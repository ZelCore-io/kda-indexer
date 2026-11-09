//! Small external sorter for `u128` values (posting entries `entity << 64 | rel_txnum`):
//! sorted in memory up to a budget, spilled as sorted runs to the range's `etl/` dir, k-way
//! merged on read. Duplicates are dropped by the merge.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

pub struct SortedU128 {
    dir: PathBuf,
    name: String,
    buf: Vec<u128>,
    limit: usize,
    runs: Vec<PathBuf>,
}

impl SortedU128 {
    /// `limit` = entries kept in memory before a run is spilled.
    pub fn new(dir: &Path, name: &str, limit: usize) -> SortedU128 {
        SortedU128 {
            dir: dir.to_path_buf(),
            name: name.to_string(),
            buf: Vec::new(),
            limit: limit.max(1024),
            runs: Vec::new(),
        }
    }

    pub fn push(&mut self, v: u128) -> Result<()> {
        self.buf.push(v);
        if self.buf.len() >= self.limit {
            self.spill()?;
        }
        Ok(())
    }

    fn spill(&mut self) -> Result<()> {
        self.buf.sort_unstable();
        self.buf.dedup();
        let p = self
            .dir
            .join(format!("{}-{:04}.run", self.name, self.runs.len()));
        let mut w = BufWriter::with_capacity(
            1 << 20,
            File::create(&p).with_context(|| format!("create {}", p.display()))?,
        );
        for v in &self.buf {
            w.write_all(&v.to_le_bytes())?;
        }
        w.flush()?;
        self.buf.clear();
        self.runs.push(p);
        Ok(())
    }

    /// Sorted, deduplicated values. Run files are deleted when the iterator is dropped.
    pub fn into_sorted(mut self) -> Result<SortedIter> {
        self.buf.sort_unstable();
        self.buf.dedup();
        let mut readers = Vec::new();
        for p in &self.runs {
            readers.push(BufReader::with_capacity(1 << 20, File::open(p)?));
        }
        let mut it = SortedIter {
            mem: std::mem::take(&mut self.buf).into_iter(),
            mem_head: None,
            readers,
            heap: BinaryHeap::new(),
            runs: std::mem::take(&mut self.runs),
            last: None,
        };
        it.mem_head = it.mem.next();
        for i in 0..it.readers.len() {
            if let Some(v) = read_one(&mut it.readers[i])? {
                it.heap.push(Reverse((v, i)));
            }
        }
        Ok(it)
    }
}

fn read_one(r: &mut BufReader<File>) -> Result<Option<u128>> {
    let mut b = [0u8; 16];
    match r.read_exact(&mut b) {
        Ok(()) => Ok(Some(u128::from_le_bytes(b))),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub struct SortedIter {
    mem: std::vec::IntoIter<u128>,
    mem_head: Option<u128>,
    readers: Vec<BufReader<File>>,
    heap: BinaryHeap<Reverse<(u128, usize)>>,
    runs: Vec<PathBuf>,
    last: Option<u128>,
}

impl SortedIter {
    fn next_raw(&mut self) -> Result<Option<u128>> {
        let h = self.heap.peek().map(|Reverse((v, _))| *v);
        match (self.mem_head, h) {
            (None, None) => Ok(None),
            (Some(m), hv) if hv.is_none_or(|x| m <= x) => {
                self.mem_head = self.mem.next();
                Ok(Some(m))
            }
            _ => {
                let Reverse((v, i)) = self.heap.pop().expect("peeked");
                if let Some(n) = read_one(&mut self.readers[i])? {
                    self.heap.push(Reverse((n, i)));
                }
                Ok(Some(v))
            }
        }
    }
}

impl Iterator for SortedIter {
    type Item = Result<u128>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.next_raw() {
                Ok(Some(v)) if Some(v) == self.last => continue,
                Ok(Some(v)) => {
                    self.last = Some(v);
                    return Some(Ok(v));
                }
                Ok(None) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl Drop for SortedIter {
    fn drop(&mut self) {
        self.readers.clear();
        for p in &self.runs {
            let _ = std::fs::remove_file(p);
        }
    }
}
