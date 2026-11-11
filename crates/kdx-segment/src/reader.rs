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
use crate::manifest::{Manifest, RangeEntry, RangeKind};
use crate::phf::{Index, KeyIndex};
use crate::postings::{ListMemo, PostingIter, Postings};
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

    /// Postings of `(f, entity)` with relative TxNum `<= max_rel` (None = all), newest first.
    pub fn postings_le(
        &self,
        f: Family,
        entity: u64,
        max_rel: Option<u32>,
    ) -> Result<Option<PostingIter>> {
        self.postings[f as usize].list_le(entity, max_rel)
    }

    /// `postings_le` through a per-request memo; `range` is this reader's index in its
    /// `Segments`.
    pub fn postings_le_memo(
        &self,
        f: Family,
        entity: u64,
        max_rel: Option<u32>,
        memo: &ListMemo,
        range: u32,
    ) -> Result<Option<PostingIter>> {
        self.postings[f as usize].list_le_memo(entity, max_rel, Some((memo, range)))
    }

    fn key(&self) -> (RangeKind, u32, u32, u64) {
        (
            self.info.kind,
            self.info.start,
            self.info.end,
            self.info.first_txnum,
        )
    }
}

/// All frozen ranges of a segments dir.
pub struct Segments {
    pub dir: PathBuf,
    pub manifest: Manifest,
    /// Shared so a refreshed `Segments` reuses unchanged readers (and an old one keeps its
    /// mmaps alive while a reader still holds it, even after a merge deleted the files).
    pub ranges: Vec<Arc<RangeReader>>,
    /// Dictionary the records' ids refer to (rendering is the caller's job).
    pub dict: Arc<dyn Dict + Send + Sync>,
}

impl Segments {
    /// Open every range listed in `dir/manifest.json` (an empty store when there is none).
    pub fn open(dir: &Path, dict: Arc<dyn Dict + Send + Sync>) -> Result<Segments> {
        let manifest = Manifest::load(dir)?.unwrap_or_else(|| Manifest::new(""));
        let mut ranges = Vec::new();
        for e in &manifest.entries {
            ranges.push(Arc::new(
                RangeReader::open(dir, e, &manifest.network)
                    .with_context(|| format!("open range {}..{}", e.start, e.end))?,
            ));
        }
        Ok(Segments {
            dir: dir.to_path_buf(),
            manifest,
            ranges,
            dict,
        })
    }

    /// Reload `manifest.json`; `None` when it is unchanged. Readers whose
    /// `(kind, start, end, first_txnum)` is unchanged are reused, new ones are opened.
    pub fn refresh(&self) -> Result<Option<Segments>> {
        let manifest = Manifest::load(&self.dir)?.unwrap_or_else(|| Manifest::new(""));
        if manifest == self.manifest {
            return Ok(None);
        }
        let mut ranges = Vec::with_capacity(manifest.entries.len());
        for e in &manifest.entries {
            let k = (e.kind, e.start, e.end, e.first_txnum);
            match self.ranges.iter().find(|r| r.key() == k) {
                Some(r) => ranges.push(r.clone()),
                None => ranges.push(Arc::new(
                    RangeReader::open(&self.dir, e, &manifest.network)
                        .with_context(|| format!("open range {}..{}", e.start, e.end))?,
                )),
            }
        }
        Ok(Some(Segments {
            dir: self.dir.clone(),
            manifest,
            ranges,
            dict: self.dict.clone(),
        }))
    }

    /// Height below which every block is frozen.
    pub fn frontier(&self) -> u32 {
        self.manifest.frontier
    }

    /// Largest TxNum at a height below `h` (None when there is none in the segments).
    pub fn last_txnum_below_height(&self, h: u32) -> Option<u64> {
        let first = self.manifest.entries.first()?;
        if h <= first.start {
            return None;
        }
        let end = if h >= self.manifest.frontier {
            self.manifest.next_txnum
        } else {
            let r = self.range_of_height(h)?;
            r.info.first_txnum + r.keys.first(r.slot(0, h)?)?
        };
        end.checked_sub(1).filter(|&t| t >= first.first_txnum)
    }

    /// TxNums of `(f, entity)` that are `<= max_txnum` (None = all), newest first across
    /// ranges. Stops with an error item when a list does not decode.
    pub fn postings_from(
        &self,
        f: Family,
        entity: u64,
        max_txnum: Option<u64>,
    ) -> impl Iterator<Item = Result<u64>> + '_ {
        self.postings_from_memo(f, entity, max_txnum, None)
    }

    /// `postings_from` with a per-request memo of parsed Roaring directories.
    pub fn postings_from_memo<'a>(
        &'a self,
        f: Family,
        entity: u64,
        max_txnum: Option<u64>,
        memo: Option<&'a ListMemo>,
    ) -> impl Iterator<Item = Result<u64>> + 'a {
        self.ranges
            .iter()
            .enumerate()
            .rev()
            .filter(move |(_, r)| max_txnum.is_none_or(|m| r.info.first_txnum <= m))
            .flat_map(move |(ri, r)| {
                let max_rel = max_txnum.and_then(|m| {
                    let rel = m - r.info.first_txnum;
                    (rel < r.info.tx_count).then(|| u32::try_from(rel).unwrap_or(u32::MAX))
                });
                let res: Box<dyn Iterator<Item = Result<u64>>> = match match memo {
                    Some(m) => r.postings_le_memo(f, entity, max_rel, m, ri as u32),
                    None => r.postings_le(f, entity, max_rel),
                } {
                    Ok(Some(it)) => Box::new(it.map(Ok)),
                    Ok(None) => Box::new(std::iter::empty()),
                    Err(e) => Box::new(std::iter::once(Err(e.context(format!(
                        "postings of range {}..{}",
                        r.info.start, r.info.end
                    ))))),
                };
                res
            })
    }

    fn range_of_txnum(&self, t: u64) -> Option<&RangeReader> {
        self.manifest.entry_of_txnum(t).map(|i| &*self.ranges[i])
    }

    fn range_of_height(&self, h: u32) -> Option<&RangeReader> {
        self.manifest.entry_of_height(h).map(|i| &*self.ranges[i])
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

    /// TxNums of the txs of block (chain, height), in position order.
    pub fn block_txnums(&self, chain: u8, height: u32) -> Option<std::ops::Range<u64>> {
        let r = self.range_of_height(height)?;
        let s = r.slot(chain, height)?;
        let a = r.keys.first(s)?;
        Some(r.info.first_txnum + a..r.info.first_txnum + a + r.keys.count(s))
    }

    /// Stored hash of block (chain, height).
    pub fn block_hash(&self, chain: u8, height: u32) -> Result<Option<Hash32>> {
        let Some(r) = self.range_of_height(height) else {
            return Ok(None);
        };
        match r.slot(chain, height) {
            Some(s) => r.block_hash(s),
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
