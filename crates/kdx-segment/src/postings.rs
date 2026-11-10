//! Posting files `acct.pst`, `xfer.pst`, `pk.pst`, `evt.pst`, `code.pst` (spec §3.6).
//!
//! File (all integers little-endian unless noted):
//! ```text
//! [4]  "KDXP"   u8 version = 1   u8 family   u64 range_txs
//! lists area: one list per entity, in entity order
//! entities:   n × u64 (ascending)
//! offsets:    sucds EliasFano of n + 1 list start offsets (relative to the lists area)
//! trailer:    u64 n, u64 lists_len, u64 offsets_len
//! ```
//! A list holds TxNums relative to the range's first TxNum, ascending; the first byte picks
//! the encoding by length (`len` ≤ 16: delta varints; ≤ max(range_txs / 32, 16): Elias-Fano;
//! denser: Roaring):
//! - `0`: varint len, varint first, varint deltas
//! - `1`: varint len, sucds `EliasFano` serialization (universe = range_txs)
//! - `2`: Roaring bitmap serialization (`RoaringBitmap::serialize_into`)

use std::io::Write;
use std::path::Path;

use anyhow::{bail, ensure, Context, Result};
use kdx_decode::Family;
use kdx_types::varint::{get_u64, put_u64};
use memmap2::Mmap;
use roaring::RoaringBitmap;
use sucds::mii_sequences::{EliasFano, EliasFanoBuilder};
use sucds::Serializable;

use crate::files::DurableFile;

pub const MAGIC: &[u8; 4] = b"KDXP";
const HEAD: usize = 14;

pub const ENC_DELTA: u8 = 0;
pub const ENC_EF: u8 = 1;
pub const ENC_ROARING: u8 = 2;

/// Encoding picked for a list of `len` entries in a range of `range_txs` txs.
pub fn encoding_for(len: usize, range_txs: u64) -> u8 {
    if len <= 16 {
        ENC_DELTA
    } else if len as u64 <= (range_txs / 32).max(16) {
        ENC_EF
    } else {
        ENC_ROARING
    }
}

pub fn encode_list(vals: &[u32], range_txs: u64, out: &mut Vec<u8>) -> Result<()> {
    let enc = encoding_for(vals.len(), range_txs);
    out.push(enc);
    match enc {
        ENC_DELTA => {
            put_u64(out, vals.len() as u64);
            let mut prev = 0u32;
            for (i, &v) in vals.iter().enumerate() {
                put_u64(out, if i == 0 { v } else { v - prev } as u64);
                prev = v;
            }
        }
        ENC_EF => {
            put_u64(out, vals.len() as u64);
            let mut b = EliasFanoBuilder::new(range_txs.max(1) as usize, vals.len())?;
            for &v in vals {
                b.push(v as usize)?;
            }
            b.build().serialize_into(&mut *out)?;
        }
        _ => {
            let r = RoaringBitmap::from_sorted_iter(vals.iter().copied())
                .map_err(|e| anyhow::anyhow!("roaring: {e}"))?;
            r.serialize_into(&mut *out)?;
        }
    }
    Ok(())
}

pub fn decode_list(mut b: &[u8]) -> Result<Vec<u32>> {
    let enc = *b.first().context("empty list")?;
    b = &b[1..];
    match enc {
        ENC_DELTA => {
            let buf = &mut b;
            let n = usize::try_from(get_u64(buf)?)?;
            // Every value takes at least one byte: never trust `n` beyond the buffer.
            ensure!(n <= buf.len(), "delta list length {n} exceeds its bytes");
            let mut v = Vec::with_capacity(n);
            let mut acc = 0u64;
            for i in 0..n {
                let d = get_u64(buf)?;
                acc = if i == 0 {
                    d
                } else {
                    acc.checked_add(d).context("delta list overflows")?
                };
                v.push(u32::try_from(acc)?);
            }
            Ok(v)
        }
        ENC_EF => {
            let buf = &mut b;
            let n = get_u64(buf)? as usize;
            let ef = EliasFano::deserialize_from(*buf)?;
            if ef.len() != n {
                bail!("EF list length {} != {n}", ef.len());
            }
            Ok(ef.iter(0).map(|x| x as u32).collect())
        }
        ENC_ROARING => {
            let r = RoaringBitmap::deserialize_from(b)?;
            Ok(r.iter().collect())
        }
        e => bail!("unknown posting encoding {e}"),
    }
}

pub struct PostingsWriter {
    f: DurableFile,
    range_txs: u64,
    entities: Vec<u64>,
    offsets: Vec<u64>,
    pos: u64,
    pub entries: u64,
    buf: Vec<u8>,
}

impl PostingsWriter {
    pub fn create(path: &Path, family: Family, range_txs: u64) -> Result<PostingsWriter> {
        let mut f = DurableFile::create(path)?;
        f.write_all(MAGIC)?;
        f.write_all(&[1, family as u8])?;
        f.write_all(&range_txs.to_le_bytes())?;
        Ok(PostingsWriter {
            f,
            range_txs,
            entities: Vec::new(),
            offsets: vec![0],
            pos: 0,
            entries: 0,
            buf: Vec::new(),
        })
    }

    /// Append the list of `entity` (entities strictly ascending, values ascending).
    pub fn push(&mut self, entity: u64, vals: &[u32]) -> Result<()> {
        if self.entities.last().is_some_and(|&l| l >= entity) {
            bail!("posting entities out of order");
        }
        self.buf.clear();
        encode_list(vals, self.range_txs, &mut self.buf)?;
        self.f.write_all(&self.buf)?;
        self.pos += self.buf.len() as u64;
        self.entities.push(entity);
        self.offsets.push(self.pos);
        self.entries += vals.len() as u64;
        Ok(())
    }

    /// Returns (entities, entries).
    pub fn finish(mut self) -> Result<(u64, u64)> {
        for e in &self.entities {
            self.f.write_all(&e.to_le_bytes())?;
        }
        let mut b = EliasFanoBuilder::new(self.pos as usize + 1, self.offsets.len())?;
        for &o in &self.offsets {
            b.push(o as usize)?;
        }
        let mut efb = Vec::new();
        b.build().serialize_into(&mut efb)?;
        self.f.write_all(&efb)?;
        let n = self.entities.len() as u64;
        self.f.write_all(&n.to_le_bytes())?;
        self.f.write_all(&self.pos.to_le_bytes())?;
        self.f.write_all(&(efb.len() as u64).to_le_bytes())?;
        self.f.finish()?;
        Ok((n, self.entries))
    }
}

/// An opened posting file of one range.
pub struct Postings {
    map: Mmap,
    pub family: u8,
    pub range_txs: u64,
    first_txnum: u64,
    n: usize,
    ent_at: usize,
    offsets: EliasFano,
}

/// TxNums of one list, newest (largest) first. Delta and Elias-Fano lists are decoded up to
/// the bound; a Roaring list is iterated lazily from the back.
pub struct PostingIter {
    inner: Inner,
    base: u64,
    len: usize,
}

enum Inner {
    Vec(Vec<u32>),
    Roaring(roaring::bitmap::IntoIter),
}

impl Iterator for PostingIter {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        let v = match &mut self.inner {
            Inner::Vec(v) => v.pop(),
            Inner::Roaring(it) => it.next_back(),
        }?;
        self.len = self.len.saturating_sub(1);
        Some(self.base + v as u64)
    }
}

impl PostingIter {
    /// Entries left.
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Decode the list in `b`, keeping values `<= max` (None = all).
fn list_le(b: &[u8], max: Option<u32>, base: u64) -> Result<PostingIter> {
    let enc = *b.first().context("empty list")?;
    if enc == ENC_ROARING {
        let r = RoaringBitmap::deserialize_from(&b[1..])?;
        let len = match max {
            Some(m) => r.rank(m) as usize,
            None => r.len() as usize,
        };
        let mut it = r.into_iter();
        if let Some(m) = max {
            it.advance_back_to(m);
        }
        return Ok(PostingIter {
            inner: Inner::Roaring(it),
            base,
            len,
        });
    }
    let mut v = decode_list(b)?;
    if let Some(m) = max {
        let n = v.partition_point(|&x| x <= m);
        v.truncate(n);
    }
    Ok(PostingIter {
        len: v.len(),
        inner: Inner::Vec(v),
        base,
    })
}

impl Postings {
    pub fn open(path: &Path, first_txnum: u64) -> Result<Postings> {
        let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        // SAFETY: segment files are immutable after rename.
        let map = unsafe { Mmap::map(&file)? };
        let len = map.len();
        if len < HEAD + 24 || &map[..4] != MAGIC || map[4] != 1 {
            bail!("{}: not a postings v1 file", path.display());
        }
        let family = map[5];
        let range_txs = u64::from_le_bytes(map[6..14].try_into()?);
        let t = |i: usize| {
            u64::from_le_bytes(
                map[len - 24 + 8 * i..len - 16 + 8 * i]
                    .try_into()
                    .expect("8"),
            ) as usize
        };
        let (n, lists_len, ef_len) = (t(0), t(1), t(2));
        let ent_at = HEAD + lists_len;
        let ef_at = ent_at + 8 * n;
        if ef_at + ef_len + 24 != len {
            bail!("{}: inconsistent trailer", path.display());
        }
        let offsets = EliasFano::deserialize_from(&map[ef_at..ef_at + ef_len])?;
        if offsets.len() != n + 1 {
            bail!(
                "{}: {} offsets for {n} entities",
                path.display(),
                offsets.len()
            );
        }
        Ok(Postings {
            map,
            family,
            range_txs,
            first_txnum,
            n,
            ent_at,
            offsets,
        })
    }

    pub fn entities(&self) -> usize {
        self.n
    }

    /// Entity at directory position `i`.
    pub fn entity_at(&self, i: usize) -> u64 {
        let a = self.ent_at + 8 * i;
        u64::from_le_bytes(self.map[a..a + 8].try_into().expect("8"))
    }

    fn find(&self, entity: u64) -> Option<usize> {
        let (mut lo, mut hi) = (0usize, self.n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.entity_at(mid).cmp(&entity) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Relative values of the list at directory position `i`.
    pub fn list_at(&self, i: usize) -> Result<Vec<u32>> {
        decode_list(self.list_bytes(i)?)
    }

    /// TxNums of `entity`, descending; None when absent (or undecodable).
    pub fn list(&self, entity: u64) -> Option<PostingIter> {
        self.list_le(entity, None).ok().flatten()
    }

    fn list_bytes(&self, i: usize) -> Result<&[u8]> {
        let a = self.offsets.select(i).context("offset")?;
        let b = self.offsets.select(i + 1).context("offset")?;
        ensure!(HEAD + b <= self.ent_at && a <= b, "posting list offsets out of bounds");
        Ok(&self.map[HEAD + a..HEAD + b])
    }

    /// TxNums of `entity` whose relative value is `<= max_rel` (None = all), descending.
    /// `Ok(None)` when the entity has no list; `Err` when the list does not decode.
    pub fn list_le(&self, entity: u64, max_rel: Option<u32>) -> Result<Option<PostingIter>> {
        let Some(i) = self.find(entity) else {
            return Ok(None);
        };
        Ok(Some(list_le(self.list_bytes(i)?, max_rel, self.first_txnum)?))
    }
}

#[cfg(test)]
mod list_tests {
    use super::*;

    /// Round trip at the delta/EF boundary (16/17) and the EF/Roaring threshold.
    #[test]
    fn list_encodings_round_trip() {
        let range_txs = 32 * 40;
        for (len, enc) in [
            (1, ENC_DELTA),
            (16, ENC_DELTA),
            (17, ENC_EF),
            (40, ENC_EF),
            (41, ENC_ROARING),
        ] {
            let vals: Vec<u32> = (0..len as u32)
                .map(|i| i * 31 % range_txs as u32 + i)
                .collect();
            let mut vals = vals;
            vals.sort_unstable();
            vals.dedup();
            let mut out = Vec::new();
            encode_list(&vals, range_txs, &mut out).unwrap();
            assert_eq!(out[0], encoding_for(vals.len(), range_txs), "len {len}");
            assert_eq!(encoding_for(len, range_txs), enc);
            assert_eq!(decode_list(&out).unwrap(), vals);
        }
        // A delta list that claims more values than it has bytes is rejected.
        assert!(decode_list(&[ENC_DELTA, 0x7f, 1]).is_err());
        // Bounded reverse iteration agrees with the decoded list for every encoding.
        for len in [5usize, 30, 200] {
            let vals: Vec<u32> = (0..len as u32).map(|i| i * 3).collect();
            let mut out = Vec::new();
            encode_list(&vals, range_txs, &mut out).unwrap();
            for max in [None, Some(0), Some(1), Some(44), Some(45), Some(10_000)] {
                let want: Vec<u64> = vals
                    .iter()
                    .rev()
                    .filter(|&&v| max.is_none_or(|m| v <= m))
                    .map(|&v| 7 + v as u64)
                    .collect();
                let it = list_le(&out, max, 7).unwrap();
                assert_eq!(it.len(), want.len(), "len {len} max {max:?}");
                assert_eq!(it.collect::<Vec<_>>(), want, "len {len} max {max:?}");
            }
        }
    }
}
