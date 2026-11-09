//! Range/step writer (spec §3.4). Pass 1 (`push`): encode records, spool them to `etl/`
//! (zstd level 1), sample them for the dictionaries, collect rk/bh keys and postings.
//! Pass 2 (`finish`): train the dictionaries, write every file into a temp dir next to the
//! segments, `range.toml` last (blake3 of every file), rename all into place, `fsync` the
//! directory. The caller then commits the manifest.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use kdx_decode::{Family, IndexedBlock};
use kdx_types::record::{encode_block_with, encode_tx};
use serde::{Deserialize, Serialize};

use crate::adj::AdjTracker;
use crate::blockfile::{dict_id, BlockFileWriter};
use crate::etl::SortedU128;
use crate::files::{
    blake3_file, file_name, fsync_dir, range_file_names, tmp_dir, write_file, FileKind,
};
use crate::keys::write_keys;
use crate::manifest::{RangeEntry, RangeKind, FORMAT_VERSION};
use crate::phf::{debug_fallback_env, write_index, IndexKey};
use crate::postings::PostingsWriter;

/// BlockOrd slots per height (`BlockOrd = height × 20 + chain`, spec §3.2).
pub const SLOT_WIDTH: u32 = 20;
/// Default posting entries kept in memory per family before a sorted run is spilled.
pub const ETL_MEM_ENTRIES: usize = 8 << 20;

#[derive(Debug, Clone, Copy)]
pub struct WriteOpts {
    /// zstd level (19 for ranges, 9 for steps).
    pub level: i32,
    /// Records per compressed block (format v1: 64).
    pub block_records: u32,
    /// Dictionary size in bytes (65,536).
    pub dict_bytes: usize,
    /// Records sampled per file kind for dictionary training (≤ 20,000).
    pub dict_samples: u32,
    /// Force the sorted-array rk/bh index (Review Focus 4 check; also env
    /// `KDX_DEBUG_RK_FALLBACK=1`).
    pub force_sorted_index: bool,
    /// Posting entries (16 B each) kept in memory per family before a sorted run is spilled
    /// (5 families; default [`ETL_MEM_ENTRIES`]). Backfill lowers it under its memory cap.
    pub postings_mem_entries: usize,
}

impl WriteOpts {
    pub fn new(level: i32, dict_samples: u32) -> WriteOpts {
        WriteOpts {
            level,
            block_records: 64,
            dict_bytes: 65536,
            dict_samples,
            force_sorted_index: false,
            postings_mem_entries: ETL_MEM_ENTRIES,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileStat {
    pub bytes: u64,
    pub blake3: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostingStat {
    pub entities: u64,
    pub entries: u64,
}

/// `range.toml`: the range header.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RangeInfo {
    pub format_version: u8,
    pub network: String,
    pub kind: RangeKind,
    pub start: u32,
    pub end: u32,
    pub first_txnum: u64,
    pub tx_count: u64,
    pub block_count: u64,
    pub slot_width: u32,
    pub block_records: u32,
    pub level: i32,
    pub txs_dict_id: u32,
    pub txs_dict_bytes: u64,
    pub blocks_dict_id: u32,
    pub blocks_dict_bytes: u64,
    /// `ptrhash` or `sorted`.
    pub rk_index: String,
    pub bh_index: String,
    pub rk_keys: u64,
    pub bh_keys: u64,
    pub cmd_reencoded: u64,
    /// By family name.
    pub postings: BTreeMap<String, PostingStat>,
    /// Every other file of the range by file name.
    pub files: BTreeMap<String, FileStat>,
}

impl RangeInfo {
    pub fn load(path: &Path) -> Result<RangeInfo> {
        let s = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let r: RangeInfo =
            toml::from_str(&s).with_context(|| format!("parse {}", path.display()))?;
        if r.format_version != FORMAT_VERSION {
            bail!("{}: format version {}", path.display(), r.format_version);
        }
        Ok(r)
    }
    pub fn entry(&self) -> RangeEntry {
        RangeEntry {
            kind: self.kind,
            start: self.start,
            end: self.end,
            first_txnum: self.first_txnum,
            tx_count: self.tx_count,
            block_count: self.block_count,
        }
    }
}

/// Deterministic reservoir sampler (xorshift64*), so a rebuild gives the same dictionary.
struct Sampler {
    max: usize,
    seen: u64,
    rng: u64,
    samples: Vec<Vec<u8>>,
}

impl Sampler {
    fn new(max: u32) -> Sampler {
        Sampler {
            max: max as usize,
            seen: 0,
            rng: 0x9E37_79B9_7F4A_7C15,
            samples: Vec::new(),
        }
    }
    fn next(&mut self) -> u64 {
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        self.rng.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn offer(&mut self, rec: &[u8]) {
        self.seen += 1;
        if self.samples.len() < self.max {
            self.samples.push(rec.to_vec());
        } else if self.max > 0 {
            let j = self.next() % self.seen;
            if (j as usize) < self.max {
                self.samples[j as usize] = rec.to_vec();
            }
        }
    }
    fn train(&self, bytes: usize) -> Vec<u8> {
        if self.samples.len() < 16 {
            return Vec::new();
        }
        zstd::dict::from_samples(&self.samples, bytes).unwrap_or_default()
    }
}

struct Spool {
    path: PathBuf,
    enc: zstd::Encoder<'static, BufWriter<File>>,
    n: u64,
}

impl Spool {
    fn create(path: PathBuf) -> Result<Spool> {
        let f = File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let enc = zstd::Encoder::new(BufWriter::with_capacity(1 << 20, f), 1)?;
        Ok(Spool { path, enc, n: 0 })
    }
    fn put(&mut self, rec: &[u8]) -> Result<()> {
        self.enc.write_all(&(rec.len() as u32).to_le_bytes())?;
        self.enc.write_all(rec)?;
        self.n += 1;
        Ok(())
    }
    /// Close and return a reader over the records.
    fn reader(self) -> Result<SpoolReader> {
        let mut w = self.enc.finish()?;
        w.flush()?;
        drop(w);
        let f = File::open(&self.path)?;
        Ok(SpoolReader {
            dec: zstd::Decoder::new(f)?,
            left: self.n,
        })
    }
}

struct SpoolReader {
    dec: zstd::Decoder<'static, BufReader<File>>,
    left: u64,
}

impl SpoolReader {
    fn next(&mut self) -> Result<Option<Vec<u8>>> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        let mut l = [0u8; 4];
        self.dec.read_exact(&mut l)?;
        let mut v = vec![0u8; u32::from_le_bytes(l) as usize];
        self.dec.read_exact(&mut v)?;
        Ok(Some(v))
    }
}

pub struct RangeWriter {
    dir: PathBuf,
    etl: PathBuf,
    network: String,
    kind: RangeKind,
    start: u32,
    end: u32,
    first_txnum: u64,
    opts: WriteOpts,
    txs: Spool,
    blocks: Spool,
    tx_samples: Sampler,
    block_samples: Sampler,
    counts: Vec<u32>,
    present: Vec<u8>,
    last_slot: Option<usize>,
    rk: Vec<IndexKey>,
    bh: Vec<IndexKey>,
    postings: Vec<SortedU128>,
    adj: AdjTracker,
    next_rel: u64,
    blocks_n: u64,
    cmd_reencoded: u64,
    buf: Vec<u8>,
}

impl RangeWriter {
    /// Start a range `[start, end)` whose first tx gets TxNum `first_txnum`. `dir` is the
    /// segments dir; `etl_dir` gets a per-range spool dir (deleted by `finish`).
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        dir: &Path,
        etl_dir: &Path,
        network: &str,
        kind: RangeKind,
        start: u32,
        end: u32,
        first_txnum: u64,
        opts: WriteOpts,
    ) -> Result<RangeWriter> {
        if end <= start {
            bail!("empty range {start}..{end}");
        }
        fs::create_dir_all(dir)?;
        let etl = etl_dir.join(format!("{network}-{start:09}-{end:09}"));
        if etl.exists() {
            fs::remove_dir_all(&etl)?;
        }
        fs::create_dir_all(&etl)?;
        let slots = (end - start) as usize * SLOT_WIDTH as usize;
        let mut opts = opts;
        opts.force_sorted_index |= debug_fallback_env();
        Ok(RangeWriter {
            dir: dir.to_path_buf(),
            txs: Spool::create(etl.join("txs.spool"))?,
            blocks: Spool::create(etl.join("blocks.spool"))?,
            postings: Family::ALL
                .iter()
                .map(|f| SortedU128::new(&etl, f.name(), opts.postings_mem_entries))
                .collect(),
            etl,
            network: network.to_string(),
            kind,
            start,
            end,
            first_txnum,
            tx_samples: Sampler::new(opts.dict_samples),
            block_samples: Sampler::new(opts.dict_samples),
            opts,
            counts: vec![0; slots],
            present: vec![0; slots.div_ceil(8)],
            last_slot: None,
            rk: Vec::new(),
            bh: Vec::new(),
            adj: AdjTracker::default(),
            next_rel: 0,
            blocks_n: 0,
            cmd_reencoded: 0,
            buf: Vec::new(),
        })
    }

    /// TxNum the next pushed tx gets.
    pub fn next_txnum(&self) -> u64 {
        self.first_txnum + self.next_rel
    }

    /// Append one block; blocks must come in (height, chain) order.
    pub fn push(&mut self, b: &IndexedBlock) -> Result<()> {
        let h = b.block.height;
        let c = b.block.chain;
        if h < self.start as u64 || h >= self.end as u64 || c as u32 >= SLOT_WIDTH {
            bail!(
                "block ({c}, {h}) outside range {}..{}",
                self.start,
                self.end
            );
        }
        let slot = (h as usize - self.start as usize) * SLOT_WIDTH as usize + c as usize;
        if self.last_slot.is_some_and(|l| l >= slot) {
            bail!("block ({c}, {h}) out of (height, chain) order");
        }
        if b.idx.len() != b.txs.len() {
            bail!(
                "block ({c}, {h}): {} index entries for {} txs",
                b.idx.len(),
                b.txs.len()
            );
        }
        self.last_slot = Some(slot);
        self.counts[slot] = u32::try_from(b.txs.len())?;
        self.present[slot / 8] |= 1 << (slot % 8);

        self.adj.begin(h);
        self.buf.clear();
        let adj = &self.adj;
        encode_block_with(&b.block, &mut self.buf, &|ac, ah| adj.is_ref(ac, ah));
        self.adj.done(c, b.block.hash);
        self.blocks.put(&self.buf)?;
        self.block_samples.offer(&self.buf);
        self.bh.push(IndexKey::new(&b.block.hash, slot as u32));
        self.blocks_n += 1;

        for (tx, ix) in b.txs.iter().zip(&b.idx) {
            if tx.key.height() as u64 != h || tx.key.chain() != c || ix.key != tx.key {
                bail!("tx key {:?} does not belong to block ({c}, {h})", tx.key);
            }
            let rel = self.next_rel;
            self.next_rel += 1;
            self.buf.clear();
            encode_tx(tx, &mut self.buf);
            self.txs.put(&self.buf)?;
            self.tx_samples.offer(&self.buf);
            if let Some(rk) = &tx.request_key {
                self.rk.push(IndexKey::new(rk, u32::try_from(rel)?));
            }
            self.cmd_reencoded += tx.cmd_reencoded as u64;
            for (fam, ent) in &ix.postings {
                self.postings[*fam as usize].push((*ent as u128) << 64 | rel as u128)?;
            }
        }
        Ok(())
    }

    /// Write all files, rename them into the segments dir, return the manifest entry.
    pub fn finish(self) -> Result<RangeEntry> {
        let RangeWriter {
            dir,
            etl,
            network,
            kind,
            start,
            end,
            first_txnum,
            opts,
            txs,
            blocks,
            tx_samples,
            block_samples,
            counts,
            present,
            rk,
            bh,
            postings,
            cmd_reencoded,
            next_rel,
            blocks_n,
            ..
        } = self;
        let tx_count = next_rel;
        let tmp = tmp_dir(&dir, &network, start, end);
        if tmp.exists() {
            fs::remove_dir_all(&tmp)?;
        }
        fs::create_dir_all(&tmp)?;
        let fname = |k: FileKind| file_name(&network, k, start, end);

        let (txs_dict, blocks_dict) = rayon::join(
            || tx_samples.train(opts.dict_bytes),
            || block_samples.train(opts.dict_bytes),
        );
        drop((tx_samples, block_samples));
        let (txs_dict_id, blocks_dict_id) = (dict_id(&txs_dict), dict_id(&blocks_dict));
        let (txs_dict_bytes, blocks_dict_bytes) = (txs_dict.len() as u64, blocks_dict.len() as u64);

        // txs.seg
        let mut w = BlockFileWriter::create(
            &tmp.join(fname(FileKind::Txs)),
            "txs",
            txs_dict,
            opts.level,
            opts.block_records,
        )?;
        let mut r = txs.reader()?;
        while let Some(rec) = r.next()? {
            w.push(rec)?;
        }
        w.finish(Vec::new(), 0)?;
        drop(r);

        // blocks.seg
        let slots = counts.len() as u64;
        let mut w = BlockFileWriter::create(
            &tmp.join(fname(FileKind::Blocks)),
            "blocks",
            blocks_dict,
            opts.level,
            opts.block_records,
        )?;
        let mut r = blocks.reader()?;
        while let Some(rec) = r.next()? {
            w.push(rec)?;
        }
        w.finish(present, slots)?;
        drop(r);

        write_keys(&tmp.join(fname(FileKind::Keys)), SLOT_WIDTH, &counts)?;
        let (rk_keys, bh_keys) = (rk.len() as u64, bh.len() as u64);
        let rk_kind = write_index(&tmp.join(fname(FileKind::Rk)), rk, opts.force_sorted_index)?;
        let bh_kind = write_index(&tmp.join(fname(FileKind::Bh)), bh, opts.force_sorted_index)?;

        let mut pstats = BTreeMap::new();
        for (fam, col) in Family::ALL.iter().zip(postings) {
            let mut pw = PostingsWriter::create(
                &tmp.join(fname(FileKind::of_family(*fam))),
                *fam,
                tx_count,
            )?;
            let mut cur: Option<u64> = None;
            let mut vals: Vec<u32> = Vec::new();
            for v in col.into_sorted()? {
                let v = v?;
                let ent = (v >> 64) as u64;
                let rel = v as u64 as u32;
                if cur != Some(ent) {
                    if let Some(e) = cur {
                        pw.push(e, &vals)?;
                    }
                    cur = Some(ent);
                    vals.clear();
                }
                vals.push(rel);
            }
            if let Some(e) = cur {
                pw.push(e, &vals)?;
            }
            let (entities, entries) = pw.finish()?;
            pstats.insert(fam.name().to_string(), PostingStat { entities, entries });
        }

        let mut files = BTreeMap::new();
        for n in range_file_names(&network, start, end) {
            if n == fname(FileKind::Range) {
                continue;
            }
            let p = tmp.join(&n);
            let bytes = fs::metadata(&p)
                .with_context(|| format!("missing {n}"))?
                .len();
            files.insert(
                n,
                FileStat {
                    bytes,
                    blake3: blake3_file(&p)?,
                },
            );
        }
        let info = RangeInfo {
            format_version: FORMAT_VERSION,
            network: network.clone(),
            kind,
            start,
            end,
            first_txnum,
            tx_count,
            block_count: blocks_n,
            slot_width: SLOT_WIDTH,
            block_records: opts.block_records,
            level: opts.level,
            txs_dict_id,
            txs_dict_bytes,
            blocks_dict_id,
            blocks_dict_bytes,
            rk_index: rk_kind.name().to_string(),
            bh_index: bh_kind.name().to_string(),
            rk_keys,
            bh_keys,
            cmd_reencoded,
            postings: pstats,
            files,
        };
        write_file(
            &tmp.join(fname(FileKind::Range)),
            toml::to_string(&info)?.as_bytes(),
        )?;
        fsync_dir(&tmp)?;

        // range.toml goes last so a range with a readable range.toml is complete.
        let mut names = range_file_names(&network, start, end);
        names.retain(|n| *n != fname(FileKind::Range));
        names.push(fname(FileKind::Range));
        for n in &names {
            fs::rename(tmp.join(n), dir.join(n)).with_context(|| format!("rename {n}"))?;
        }
        fsync_dir(&dir)?;
        fs::remove_dir_all(&tmp)?;
        let _ = fs::remove_dir_all(&etl);
        Ok(info.entry())
    }
}
