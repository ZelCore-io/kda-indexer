//! `txs.seg` / `blocks.seg`: nippy-jar container, one column, one row = one zstd frame
//! holding up to `block_records` (64) records. The range's trained dictionary and the
//! counts live in the jar's user header (`.conf`).
//!
//! Decompressed row: `u32 LE n`, `n + 1` × `u32 LE` record offsets relative to the end of
//! the offset table, then the record bytes back to back (record i = `[off[i], off[i+1])`).

use std::path::Path;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use reth_nippy_jar::{DataReader, NippyJar, NippyJarWriter};
use serde::{Deserialize, Serialize};
use zstd::dict::{DecoderDictionary, EncoderDictionary};

/// Rows compressed in parallel per batch.
const BATCH_ROWS: usize = 256;

/// Id of a zstd dictionary (magic `0xEC30A437`, then the id, both LE); 0 = none/raw.
pub fn dict_id(d: &[u8]) -> u32 {
    if d.len() >= 8 && d[..4] == 0xEC30_A437u32.to_le_bytes() {
        u32::from_le_bytes(d[4..8].try_into().expect("4"))
    } else {
        0
    }
}

/// User header of a segment jar.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SegHeader {
    /// `txs` or `blocks`.
    pub kind: String,
    pub records: u64,
    pub block_records: u32,
    pub level: i32,
    /// zstd dictionary (empty = compressed without a dictionary).
    pub dict: Vec<u8>,
    /// zstd dictionary id (0 when none).
    pub dict_id: u32,
    /// Largest decompressed row, bytes.
    pub max_raw_row: u64,
    /// `blocks` only: one bit per BlockOrd slot of the range (LSB first), set when the block
    /// exists; record i is the i-th set slot.
    pub present: Vec<u8>,
    /// `blocks` only: number of slots (`(end - start) × slot_width`).
    pub slots: u64,
}

pub struct BlockFileWriter {
    w: NippyJarWriter<SegHeader>,
    cdict: Option<EncoderDictionary<'static>>,
    level: i32,
    block_records: usize,
    cur: Vec<Vec<u8>>,
    pending: Vec<Vec<u8>>,
    records: u64,
    max_raw_row: u64,
    compressed: u64,
}

fn pack_row(recs: &[Vec<u8>]) -> Vec<u8> {
    let total: usize = recs.iter().map(|r| r.len()).sum();
    let mut out = Vec::with_capacity(4 + 4 * (recs.len() + 1) + total);
    out.extend_from_slice(&(recs.len() as u32).to_le_bytes());
    let mut off = 0u32;
    out.extend_from_slice(&off.to_le_bytes());
    for r in recs {
        off += r.len() as u32;
        out.extend_from_slice(&off.to_le_bytes());
    }
    for r in recs {
        out.extend_from_slice(r);
    }
    out
}

/// Record `i` of a decompressed row.
pub fn row_record(row: &[u8], i: usize) -> Result<&[u8]> {
    let n = u32::from_le_bytes(row.get(..4).context("short row")?.try_into()?) as usize;
    if i >= n {
        bail!("record {i} past row length {n}");
    }
    let tab = 4 + 4 * (n + 1);
    let at = |k: usize| -> Result<usize> {
        Ok(u32::from_le_bytes(
            row.get(4 + 4 * k..8 + 4 * k)
                .context("short row")?
                .try_into()?,
        ) as usize)
    };
    let (a, b) = (at(i)?, at(i + 1)?);
    row.get(tab + a..tab + b).context("record past row end")
}

/// Number of records in a decompressed row.
pub fn row_len(row: &[u8]) -> Result<usize> {
    Ok(u32::from_le_bytes(row.get(..4).context("short row")?.try_into()?) as usize)
}

impl BlockFileWriter {
    pub fn create(
        path: &Path,
        kind: &str,
        dict: Vec<u8>,
        level: i32,
        block_records: u32,
    ) -> Result<Self> {
        let dict_id = dict_id(&dict);
        let cdict = (!dict.is_empty()).then(|| EncoderDictionary::copy(&dict, level));
        let header = SegHeader {
            kind: kind.to_string(),
            block_records,
            level,
            dict,
            dict_id,
            ..Default::default()
        };
        let jar = NippyJar::new(1, path, header);
        let w = NippyJarWriter::new(jar)
            .with_context(|| format!("nippy-jar create {}", path.display()))?;
        Ok(BlockFileWriter {
            w,
            cdict,
            level,
            block_records: block_records as usize,
            cur: Vec::with_capacity(block_records as usize),
            pending: Vec::new(),
            records: 0,
            max_raw_row: 0,
            compressed: 0,
        })
    }

    pub fn push(&mut self, rec: Vec<u8>) -> Result<()> {
        self.cur.push(rec);
        self.records += 1;
        if self.cur.len() == self.block_records {
            let row = pack_row(&self.cur);
            self.cur.clear();
            self.pending.push(row);
            if self.pending.len() >= BATCH_ROWS {
                self.flush_batch()?;
            }
        }
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.pending);
        let level = self.level;
        let cdict = self.cdict.as_ref();
        let out: Vec<std::io::Result<Vec<u8>>> = rows
            .par_iter()
            .map_init(
                || match cdict {
                    Some(d) => zstd::bulk::Compressor::with_prepared_dictionary(d),
                    None => zstd::bulk::Compressor::new(level),
                },
                |c, raw| match c {
                    Ok(c) => c.compress(raw),
                    Err(e) => Err(std::io::Error::other(e.to_string())),
                },
            )
            .collect();
        for (raw, z) in rows.iter().zip(out) {
            let z = z.context("zstd compress")?;
            self.max_raw_row = self.max_raw_row.max(raw.len() as u64);
            self.compressed += z.len() as u64;
            self.w
                .append_column(Some(Ok::<_, Box<dyn std::error::Error + Send + Sync>>(&z)))?;
        }
        kdx_types::heartbeat::beat(kdx_types::heartbeat::Component::Ingest);
        Ok(())
    }

    /// Flush the last partial row, write header and offsets, fsync (nippy-jar commit).
    /// `present`/`slots` are set for `blocks`. Returns (records, compressed bytes).
    pub fn finish(mut self, present: Vec<u8>, slots: u64) -> Result<(u64, u64)> {
        if !self.cur.is_empty() {
            let row = pack_row(&self.cur);
            self.cur.clear();
            self.pending.push(row);
        }
        self.flush_batch()?;
        let h = self.w.user_header_mut();
        h.records = self.records;
        h.max_raw_row = self.max_raw_row;
        h.present = present;
        h.slots = slots;
        self.w.commit()?;
        Ok((self.records, self.compressed))
    }
}

/// Read side: mmapped data + offsets, prepared decoder dictionary.
pub struct BlockFile {
    data: DataReader,
    pub header: SegHeader,
    ddict: Option<DecoderDictionary<'static>>,
    rows: usize,
}

impl BlockFile {
    pub fn open(path: &Path) -> Result<BlockFile> {
        let jar = NippyJar::<SegHeader>::load(path)
            .with_context(|| format!("nippy-jar load {}", path.display()))?;
        let header = jar.user_header().clone();
        let rows = jar.rows();
        let data = DataReader::new(path).with_context(|| format!("mmap {}", path.display()))?;
        let ddict = (!header.dict.is_empty()).then(|| DecoderDictionary::copy(&header.dict));
        if header.block_records == 0 {
            bail!("{}: block_records 0", path.display());
        }
        Ok(BlockFile {
            data,
            header,
            ddict,
            rows,
        })
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn records(&self) -> u64 {
        self.header.records
    }

    /// Decompressed row `r`.
    pub fn row(&self, r: usize) -> Result<Vec<u8>> {
        if r >= self.rows {
            bail!("row {r} past {}", self.rows);
        }
        let a = self.data.offset(r)? as usize;
        let b = self.data.offset(r + 1)? as usize;
        let z = self.data.data(a..b);
        let cap = self.header.max_raw_row as usize;
        let mut d = match &self.ddict {
            Some(dd) => zstd::bulk::Decompressor::with_prepared_dictionary(dd)?,
            None => zstd::bulk::Decompressor::new()?,
        };
        d.decompress(z, cap).context("zstd decompress")
    }

    /// Record `i` (copy).
    pub fn record(&self, i: u64) -> Result<Vec<u8>> {
        if i >= self.header.records {
            bail!("record {i} past {}", self.header.records);
        }
        let br = self.header.block_records as u64;
        let row = self.row((i / br) as usize)?;
        Ok(row_record(&row, (i % br) as usize)?.to_vec())
    }
}
