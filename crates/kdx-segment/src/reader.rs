//! Read side over every range listed in the manifest (spec §3.5–§3.7).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use kdx_decode::Family;
use kdx_types::record::{decode_block_with, decode_tx, BlockRecord, TxRecord};
use kdx_types::{Dict, Hash32, TxKey};

use crate::adj::record_block_hash;
use crate::blockfile::BlockFile;
use crate::files::{file_name, FileKind};
use crate::keys::Keys;
use crate::manifest::{Manifest, RangeEntry};
use crate::phf::{Index, KeyIndex};
use crate::postings::{PostingIter, Postings};
use crate::writer::{RangeInfo, SLOT_WIDTH};

/// One opened range or step.
pub struct RangeReader {
    pub info: RangeInfo,
    pub txs: BlockFile,
    pub blocks: BlockFile,
    pub keys: Keys,
    pub rk: Index,
    pub bh: Index,
    /// By `Family as usize`.
    pub postings: Vec<Postings>,
    /// Set bits of `blocks.present` before each 64-bit word.
    rank: Vec<u32>,
}

impl RangeReader {
    pub fn open(dir: &Path, e: &RangeEntry, network: &str) -> Result<RangeReader> {
        let f = |k: FileKind| dir.join(file_name(network, k, e.start, e.end));
        let info = RangeInfo::load(&f(FileKind::Range))?;
        if info.entry() != *e {
            bail!(
                "range.toml of {}..{} disagrees with the manifest",
                e.start,
                e.end
            );
        }
        if info.slot_width != SLOT_WIDTH {
            bail!(
                "range {}..{}: slot width {}",
                e.start,
                e.end,
                info.slot_width
            );
        }
        let txs = BlockFile::open(&f(FileKind::Txs))?;
        let blocks = BlockFile::open(&f(FileKind::Blocks))?;
        let keys = Keys::open(&f(FileKind::Keys))?;
        let rk = Index::open(&f(FileKind::Rk))?;
        let bh = Index::open(&f(FileKind::Bh))?;
        let mut postings = Vec::new();
        for fam in Family::ALL {
            postings.push(Postings::open(&f(FileKind::of_family(fam)), e.first_txnum)?);
        }
        let p = &blocks.header.present;
        let mut rank = Vec::with_capacity(p.len().div_ceil(8));
        let mut acc = 0u32;
        for w in p.chunks(8) {
            rank.push(acc);
            acc += w.iter().map(|b| b.count_ones()).sum::<u32>();
        }
        if txs.records() != e.tx_count
            || blocks.records() != e.block_count
            || acc as u64 != e.block_count
        {
            bail!(
                "range {}..{}: record counts differ from the manifest",
                e.start,
                e.end
            );
        }
        if keys.tx_count() != e.tx_count || keys.slots() as u64 != blocks.header.slots {
            bail!(
                "range {}..{}: keys.ef disagrees with the record files",
                e.start,
                e.end
            );
        }
        Ok(RangeReader {
            info,
            txs,
            blocks,
            keys,
            rk,
            bh,
            postings,
            rank,
        })
    }

    pub fn slots(&self) -> usize {
        self.keys.slots()
    }

    pub fn slot(&self, chain: u8, height: u32) -> Option<usize> {
        if height < self.info.start || height >= self.info.end || chain as u32 >= SLOT_WIDTH {
            return None;
        }
        Some((height - self.info.start) as usize * SLOT_WIDTH as usize + chain as usize)
    }

    /// (chain, height) of a slot.
    pub fn slot_pos(&self, slot: usize) -> (u8, u32) {
        (
            (slot % SLOT_WIDTH as usize) as u8,
            self.info.start + (slot / SLOT_WIDTH as usize) as u32,
        )
    }

    pub fn present(&self, slot: usize) -> bool {
        let p = &self.blocks.header.present;
        slot / 8 < p.len() && p[slot / 8] & (1 << (slot % 8)) != 0
    }

    /// Record index in `blocks.seg` of a present slot.
    fn block_index(&self, slot: usize) -> Option<u64> {
        if !self.present(slot) {
            return None;
        }
        let w = slot / 64;
        let p = &self.blocks.header.present;
        let mut r = self.rank[w];
        for b in &p[w * 8..slot / 8] {
            r += b.count_ones();
        }
        r += (p[slot / 8] & ((1u8 << (slot % 8)) - 1)).count_ones();
        Some(r as u64)
    }

    /// Stored hash of the block in `slot`.
    pub fn block_hash(&self, slot: usize) -> Result<Option<Hash32>> {
        match self.block_index(slot) {
            Some(i) => Ok(Some(record_block_hash(&self.blocks.record(i)?)?)),
            None => Ok(None),
        }
    }

    /// Raw (encoded) block record of a slot.
    pub fn block_raw(&self, slot: usize) -> Result<Option<Vec<u8>>> {
        match self.block_index(slot) {
            Some(i) => Ok(Some(self.blocks.record(i)?)),
            None => Ok(None),
        }
    }

    pub fn block_at(&self, slot: usize) -> Result<Option<BlockRecord>> {
        let Some(raw) = self.block_raw(slot)? else {
            return Ok(None);
        };
        let resolve = |c: u8, h: u64| -> Option<Hash32> {
            let s = self.slot(c, u32::try_from(h).ok()?)?;
            self.block_hash(s).ok().flatten()
        };
        Ok(Some(decode_block_with(&raw, &resolve)?))
    }

    /// Tx record at a TxNum relative to the range start.
    pub fn tx_rel(&self, rel: u64) -> Result<TxRecord> {
        Ok(decode_tx(&self.txs.record(rel)?)?)
    }

    /// TxKey of a relative TxNum (from `keys.ef`).
    pub fn txkey_rel(&self, rel: u64) -> Option<TxKey> {
        let s = self.keys.slot_of(rel)?;
        let pos = rel - self.keys.first(s)?;
        let (c, h) = self.slot_pos(s);
        Some(TxKey::new(h, c, u16::try_from(pos).ok()?))
    }

    /// Relative TxNum of a TxKey.
    pub fn rel_of(&self, k: TxKey) -> Option<u64> {
        let s = self.slot(k.chain(), k.height())?;
        let pos = k.pos() as u64;
        (pos < self.keys.count(s)).then(|| self.keys.first(s).map(|f| f + pos))?
    }

    /// Relative TxNum of a request key (confirmed against the record).
    pub fn rk_rel(&self, rk: &Hash32) -> Result<Option<u64>> {
        let mut c = Vec::new();
        self.rk.candidates(rk, &mut c);
        c.sort_unstable_by(|a, b| b.cmp(a));
        for o in c {
            if o as u64 >= self.info.tx_count {
                continue;
            }
            if self.tx_rel(o as u64)?.request_key.as_ref() == Some(rk) {
                return Ok(Some(o as u64));
            }
        }
        Ok(None)
    }

    /// Slot of a block hash (confirmed against the record).
    pub fn bh_slot(&self, h: &Hash32) -> Result<Option<usize>> {
        let mut c = Vec::new();
        self.bh.candidates(h, &mut c);
        for s in c {
            if self.block_hash(s as usize)?.as_ref() == Some(h) {
                return Ok(Some(s as usize));
            }
        }
        Ok(None)
    }

    pub fn postings(&self, f: Family, entity: u64) -> Option<PostingIter> {
        self.postings[f as usize].list(entity)
    }
}

/// All frozen ranges of a segments dir.
pub struct Segments {
    pub dir: PathBuf,
    pub manifest: Manifest,
    pub ranges: Vec<RangeReader>,
    /// Dictionary the records' ids refer to (rendering is the caller's job).
    pub dict: Arc<dyn Dict + Send + Sync>,
}

impl Segments {
    /// Open every range listed in `dir/manifest.json` (an empty store when there is none).
    pub fn open(dir: &Path, dict: Arc<dyn Dict + Send + Sync>) -> Result<Segments> {
        let manifest = Manifest::load(dir)?.unwrap_or_else(|| Manifest::new(""));
        let mut ranges = Vec::new();
        for e in &manifest.entries {
            ranges.push(
                RangeReader::open(dir, e, &manifest.network)
                    .with_context(|| format!("open range {}..{}", e.start, e.end))?,
            );
        }
        Ok(Segments {
            dir: dir.to_path_buf(),
            manifest,
            ranges,
            dict,
        })
    }

    fn range_of_txnum(&self, t: u64) -> Option<&RangeReader> {
        self.manifest.entry_of_txnum(t).map(|i| &self.ranges[i])
    }

    fn range_of_height(&self, h: u32) -> Option<&RangeReader> {
        self.manifest.entry_of_height(h).map(|i| &self.ranges[i])
    }

    pub fn tx(&self, txnum: u64) -> Result<Option<TxRecord>> {
        match self.range_of_txnum(txnum) {
            Some(r) => Ok(Some(r.tx_rel(txnum - r.info.first_txnum)?)),
            None => Ok(None),
        }
    }

    pub fn txkey(&self, txnum: u64) -> Option<TxKey> {
        let r = self.range_of_txnum(txnum)?;
        r.txkey_rel(txnum - r.info.first_txnum)
    }

    pub fn txnum_of(&self, k: TxKey) -> Option<u64> {
        let r = self.range_of_height(k.height())?;
        r.rel_of(k).map(|x| x + r.info.first_txnum)
    }

    pub fn block(&self, chain: u8, height: u32) -> Result<Option<BlockRecord>> {
        let Some(r) = self.range_of_height(height) else {
            return Ok(None);
        };
        match r.slot(chain, height) {
            Some(s) => r.block_at(s),
            None => Ok(None),
        }
    }

    /// TxNum of a request key, ranges newest first.
    pub fn by_rk(&self, rk: &Hash32) -> Result<Option<u64>> {
        for r in self.ranges.iter().rev() {
            if let Some(x) = r.rk_rel(rk)? {
                return Ok(Some(x + r.info.first_txnum));
            }
        }
        Ok(None)
    }

    /// (chain, height) of a block hash, ranges newest first.
    pub fn by_block_hash(&self, h: &Hash32) -> Result<Option<(u8, u32)>> {
        for r in self.ranges.iter().rev() {
            if let Some(s) = r.bh_slot(h)? {
                return Ok(Some(r.slot_pos(s)));
            }
        }
        Ok(None)
    }

    /// TxNums of `(family, entity)`, newest first across ranges.
    pub fn postings(&self, f: Family, entity: u64) -> impl Iterator<Item = u64> + '_ {
        self.ranges
            .iter()
            .rev()
            .flat_map(move |r| r.postings(f, entity).into_iter().flatten())
    }
}
