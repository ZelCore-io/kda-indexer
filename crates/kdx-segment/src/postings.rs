//! Posting files `acct.pst`, `xfer.pst`, `pk.pst`, `evt.pst`, `code.pst`, `pact.pst` (spec §3.6;
//! `pact` since store format v2).
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

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{bail, ensure, Context, Result};
use kdx_decode::Family;
use kdx_types::varint::{get_u64, put_u64};
use memmap2::Mmap;
use roaring::RoaringBitmap;
use sucds::mii_sequences::{EliasFano, EliasFanoBuilder};

use crate::ef;
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

/// An Elias-Fano list body (after the length): decoded by `ef::values`, which checks every
/// stored length, so damaged bytes are an `Err` (M-6). The values fit `u32` (relative TxNums).
fn ef_list(b: &[u8], n: usize) -> Result<Vec<u32>> {
    let d = ef::values(b)?;
    ensure!(
        d.used == b.len(),
        "EF list: {} bytes after its data",
        b.len() - d.used
    );
    ensure!(
        d.values.len() == n,
        "EF list length {} != {n}",
        d.values.len()
    );
    d.values
        .into_iter()
        .map(|v| u32::try_from(v).context("EF list value past u32"))
        .collect()
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
            ef_list(buf, n)
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
        if self.entities.len().is_multiple_of(4096) {
            kdx_types::heartbeat::beat(kdx_types::heartbeat::Component::Ingest);
        }
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
        kdx_types::heartbeat::beat(kdx_types::heartbeat::Component::Ingest);
        Ok((n, self.entries))
    }
}

/// An opened posting file of one range.
pub struct Postings {
    map: Arc<Mmap>,
    pub family: u8,
    pub range_txs: u64,
    first_txnum: u64,
    n: usize,
    ent_at: usize,
    offsets: EliasFano,
}

/// TxNums of one list, newest (largest) first. Delta and Elias-Fano lists are decoded whole
/// and cut at the bound: both are short by construction (an Elias-Fano list holds at most
/// `range_txs / 32` entries), and the Elias-Fano decode checks every stored length (M-6).
/// Roaring lists, the long ones, are walked lazily from the back, so a page of 10 from a list
/// of a million entries touches about 10 of them (R4).
pub struct PostingIter {
    inner: Inner,
    base: u64,
    len: usize,
}

enum Inner {
    Vec(Vec<u32>),
    Roaring(RoaringIter),
}

impl Iterator for PostingIter {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        let v = match &mut self.inner {
            Inner::Vec(v) => v.pop(),
            Inner::Roaring(it) => it.next(),
        }?;
        self.len = self.len.saturating_sub(1);
        Some(self.base + v as u64)
    }
}

impl PostingIter {
    /// The same entries, oldest (smallest) first. A decoded (Delta or Elias-Fano) list is
    /// walked forwards; a Roaring list is decoded whole (one range's list) and reversed.
    pub fn oldest_first(self) -> Box<dyn Iterator<Item = u64>> {
        let base = self.base;
        if matches!(self.inner, Inner::Roaring(_)) {
            let mut v: Vec<u64> = self.collect();
            v.reverse();
            return Box::new(v.into_iter());
        }
        match self.inner {
            Inner::Vec(v) => Box::new(v.into_iter().map(move |x| base + u64::from(x))),
            Inner::Roaring(_) => unreachable!("handled above"),
        }
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
        let backing: Backing = Arc::new(b.to_vec());
        let view = Arc::new(RoaringView::parse(backing, 1, b.len())?);
        return roaring_list(view, max, base);
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
        let map = Arc::new(unsafe { Mmap::map(&file)? });
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
        // Damaged trailer values must not overflow the size arithmetic.
        let ent_at = HEAD.saturating_add(lists_len);
        let ef_at = n.saturating_mul(8).saturating_add(ent_at);
        if ef_at.saturating_add(ef_len).saturating_add(24) != len {
            bail!("{}: inconsistent trailer", path.display());
        }
        // Checked read: a damaged file is an `Err`, not a panic or an abort inside sucds (M-6).
        let (offsets, used) =
            ef::load(&map[ef_at..ef_at + ef_len]).with_context(|| format!("{}", path.display()))?;
        if used != ef_len {
            bail!("{}: offsets take {used} of {ef_len} bytes", path.display());
        }
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
        ensure!(
            HEAD + b <= self.ent_at && a <= b,
            "posting list offsets out of bounds"
        );
        Ok(&self.map[HEAD + a..HEAD + b])
    }

    /// `segments check --deep`: for a Roaring list, the lazy walk (`list_le`, the read path)
    /// from three bounds (none, the edge below the last container, a value derived from
    /// `seed`) equals `full`, the list's full decode (relative values, ascending). Other
    /// encodings: `Ok(true)`.
    pub fn lazy_walk_matches(&self, i: usize, full: &[u32], seed: u64) -> Result<bool> {
        if self.list_bytes(i)?.first() != Some(&ENC_ROARING) {
            return Ok(true);
        }
        let Some(&last) = full.last() else {
            return Ok(true);
        };
        let entity = self.entity_at(i);
        let edge = ((last >> 16) << 16).saturating_sub(1);
        let pick = (seed % (u64::from(last) + 2)) as u32;
        for max in [None, Some(edge), Some(pick)] {
            let want: Vec<u64> = full
                .iter()
                .rev()
                .filter(|&&v| max.is_none_or(|m| v <= m))
                .map(|&v| self.first_txnum + u64::from(v))
                .collect();
            let Some(it) = self.list_le(entity, max)? else {
                return Ok(false);
            };
            if it.len() != want.len() || !it.eq(want.iter().copied()) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// TxNums of `entity` whose relative value is `<= max_rel` (None = all), descending.
    /// `Ok(None)` when the entity has no list; `Err` when the list does not decode.
    pub fn list_le(&self, entity: u64, max_rel: Option<u32>) -> Result<Option<PostingIter>> {
        self.list_le_memo(entity, max_rel, None)
    }

    /// `list_le` with a per-request memo: `(memo, range index)` keys the parsed header and
    /// the decoded containers of a Roaring list, so a walk that re-seeks the list within one
    /// request does not parse or decode them again.
    pub fn list_le_memo(
        &self,
        entity: u64,
        max_rel: Option<u32>,
        memo: Option<(&ListMemo, u32)>,
    ) -> Result<Option<PostingIter>> {
        let Some(i) = self.find(entity) else {
            return Ok(None);
        };
        let b = self.list_bytes(i)?;
        if b.first() != Some(&ENC_ROARING) {
            return Ok(Some(list_le(b, max_rel, self.first_txnum)?));
        }
        let a = self.offsets.select(i).context("offset")?;
        let end = self.offsets.select(i + 1).context("offset")?;
        let key = MemoKey {
            range: memo.map_or(0, |m| m.1),
            family: self.family,
            entity,
        };
        let parse = || -> Result<Arc<RoaringView>> {
            let backing: Backing = self.map.clone();
            Ok(Arc::new(RoaringView::parse(
                backing,
                HEAD + a + 1,
                HEAD + end,
            )?))
        };
        let view = match memo {
            Some((m, _)) => m.view(key, parse)?,
            None => parse()?,
        };
        Ok(Some(roaring_list(view, max_rel, self.first_txnum)?))
    }
}

type Backing = Arc<dyn AsRef<[u8]> + Send + Sync>;

/// Roaring portable format (cookies 12346 / 12347), read in place over the stored bytes: the
/// header is parsed once (keys, cardinalities, container offsets, prefix counts); a container
/// is decoded only when a walk reaches it. Structure is validated at parse (so decoding a
/// container cannot fail); `segments check --deep` still validates every value through
/// `RoaringBitmap::deserialize_from`.
pub struct RoaringView {
    backing: Backing,
    start: usize,
    end: usize,
    keys: Vec<u16>,
    cards: Vec<u32>,
    /// Container data offsets, relative to `start`.
    offs: Vec<usize>,
    run: Vec<bool>,
    /// Entries in containers `[0, i)`; the last item is the total.
    before: Vec<u64>,
}

const COOKIE_NO_RUN: u32 = 12346;
const COOKIE_RUN: u16 = 12347;
const NO_OFFSET_THRESHOLD: usize = 4;
const ARRAY_LIMIT: u32 = 4096;

impl RoaringView {
    /// View of the Roaring bytes `backing[start..end]`.
    pub fn parse(backing: Backing, start: usize, end: usize) -> Result<RoaringView> {
        let all: &[u8] = (*backing).as_ref();
        ensure!(
            start <= end && end <= all.len(),
            "roaring list out of bounds"
        );
        let b = &all[start..end];
        let u16_at = |p: usize| -> Result<u16> {
            Ok(u16::from_le_bytes(
                b.get(p..p + 2).context("short roaring list")?.try_into()?,
            ))
        };
        let u32_at = |p: usize| -> Result<u32> {
            Ok(u32::from_le_bytes(
                b.get(p..p + 4).context("short roaring list")?.try_into()?,
            ))
        };
        let cookie = u32_at(0)?;
        let (n, mut pos, run_bits, has_offsets) = if cookie == COOKIE_NO_RUN {
            (u32_at(4)? as usize, 8, None, true)
        } else if cookie as u16 == COOKIE_RUN {
            let n = (cookie >> 16) as usize + 1;
            let rb = b.get(4..4 + n.div_ceil(8)).context("short roaring list")?;
            (n, 4 + n.div_ceil(8), Some(rb), n >= NO_OFFSET_THRESHOLD)
        } else {
            bail!("unknown roaring cookie {cookie}");
        };
        ensure!(n <= 1 << 16, "roaring container count {n}");
        let desc = pos;
        pos += 4 * n;
        let offs_at = pos;
        if has_offsets {
            pos += 4 * n;
        }
        ensure!(pos <= b.len(), "short roaring header");
        let mut v = RoaringView {
            backing: Arc::clone(&backing),
            start,
            end,
            keys: Vec::with_capacity(n),
            cards: Vec::with_capacity(n),
            offs: Vec::with_capacity(n),
            run: Vec::with_capacity(n),
            before: Vec::with_capacity(n + 1),
        };
        let mut total = 0u64;
        let mut seq = pos;
        for i in 0..n {
            let key = u16_at(desc + 4 * i)?;
            let card = u32::from(u16_at(desc + 4 * i + 2)?) + 1;
            ensure!(
                v.keys.last().is_none_or(|&k| key > k),
                "roaring keys not ascending"
            );
            let is_run = run_bits.is_some_and(|rb| rb[i / 8] & (1 << (i % 8)) != 0);
            let off = if has_offsets {
                u32_at(offs_at + 4 * i)? as usize
            } else {
                seq
            };
            let size = if is_run {
                2 + 4 * usize::from(u16_at(off)?)
            } else if card <= ARRAY_LIMIT {
                2 * card as usize
            } else {
                8192
            };
            ensure!(
                off >= pos && off + size <= b.len(),
                "roaring container out of bounds"
            );
            seq = off + size;
            v.keys.push(key);
            v.cards.push(card);
            v.offs.push(off);
            v.run.push(is_run);
            v.before.push(total);
            total += u64::from(card);
        }
        v.before.push(total);
        Ok(v)
    }

    /// Entries in the list.
    pub fn len(&self) -> u64 {
        self.before.last().copied().unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn bytes(&self) -> &[u8] {
        &(*self.backing).as_ref()[self.start..self.end]
    }

    /// Bytes this view holds in a memo.
    fn memo_size(&self) -> usize {
        64 + self.keys.len() * 23
    }

    fn u16_at(&self, p: usize) -> u16 {
        let b = self.bytes();
        u16::from_le_bytes([b[p], b[p + 1]])
    }

    fn word_at(&self, off: usize, w: usize) -> u64 {
        let b = self.bytes();
        let p = off + 8 * w;
        u64::from_le_bytes(b[p..p + 8].try_into().expect("8"))
    }

    /// End (inclusive, clamped) of run `k` of the run container at `off`.
    fn run_end(&self, off: usize, k: usize) -> u32 {
        (u32::from(self.u16_at(off + 2 + 4 * k)) + u32::from(self.u16_at(off + 4 + 4 * k)))
            .min(0xFFFF)
    }

    /// A cursor over container `i` positioned at its largest value `<= low` (`None` = its
    /// largest), and the number of values `<= low`. Nothing is decoded: arrays are
    /// binary-searched in place, bitmaps are counted by popcount, runs are scanned.
    /// Never fails: `parse` checked the bounds.
    fn cursor(&self, i: usize, low: Option<u16>) -> (Cursor, u64) {
        let off = self.offs[i];
        let card = self.cards[i] as usize;
        if self.run[i] {
            let runs = usize::from(self.u16_at(off));
            let low = low.map_or(0xFFFFu32, u32::from);
            let (mut count, mut last) = (0u64, None);
            for k in 0..runs {
                let s = u32::from(self.u16_at(off + 2 + 4 * k));
                if s > low {
                    break;
                }
                let e = self.run_end(off, k).min(low);
                count += u64::from(e - s + 1);
                last = Some((k + 1, e));
            }
            return match last {
                Some((r, v)) => (Cursor::Run { off, r, v }, count),
                None => (Cursor::Empty, 0),
            };
        }
        if card as u32 <= ARRAY_LIMIT {
            let n = match low {
                None => card,
                Some(l) => {
                    let (mut lo, mut hi) = (0usize, card);
                    while lo < hi {
                        let mid = lo + (hi - lo) / 2;
                        if self.u16_at(off + 2 * mid) <= l {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    lo
                }
            };
            return (Cursor::Array { off, n }, n as u64);
        }
        let (w, word, count) = match low {
            None => (1023, self.word_at(off, 1023), u64::from(self.cards[i])),
            Some(l) => {
                let (w, bit) = (usize::from(l >> 6), u32::from(l & 63));
                let mask = if bit == 63 {
                    !0
                } else {
                    (1u64 << (bit + 1)) - 1
                };
                let word = self.word_at(off, w) & mask;
                let below: u32 = (0..w).map(|x| self.word_at(off, x).count_ones()).sum();
                (w, word, u64::from(below) + u64::from(word.count_ones()))
            }
        };
        (Cursor::Bitmap { off, w, word }, count)
    }
}

/// Position inside one container of a descending walk.
enum Cursor {
    Empty,
    /// Values `[0, n)` of the array at `off` remain.
    Array {
        off: usize,
        n: usize,
    },
    /// `word` holds the bits of word `w` not yet returned; words below `w` are untouched.
    Bitmap {
        off: usize,
        w: usize,
        word: u64,
    },
    /// Runs `[0, r)` remain; `v` is the next value, inside run `r - 1`.
    Run {
        off: usize,
        r: usize,
        v: u32,
    },
}

/// Descending walk over a `RoaringView`, one container at a time.
pub struct RoaringIter {
    view: Arc<RoaringView>,
    /// Containers `[0, next_ci)` are not entered yet.
    next_ci: usize,
    cur: Cursor,
    hi: u32,
}

impl RoaringIter {
    fn next(&mut self) -> Option<u32> {
        loop {
            let v = &*self.view;
            match &mut self.cur {
                Cursor::Empty => {}
                Cursor::Array { off, n } => {
                    if *n > 0 {
                        *n -= 1;
                        return Some(self.hi | u32::from(v.u16_at(*off + 2 * *n)));
                    }
                }
                Cursor::Bitmap { off, w, word } => loop {
                    if *word != 0 {
                        let bit = 63 - word.leading_zeros();
                        *word &= !(1u64 << bit);
                        return Some(self.hi | (*w as u32 * 64 + bit));
                    }
                    if *w == 0 {
                        break;
                    }
                    *w -= 1;
                    *word = v.word_at(*off, *w);
                },
                Cursor::Run { off, r, v: val } => {
                    if *r > 0 {
                        let out = *val;
                        if out == u32::from(v.u16_at(*off + 2 + 4 * (*r - 1))) {
                            *r -= 1;
                            if *r > 0 {
                                *val = v.run_end(*off, *r - 1);
                            }
                        } else {
                            *val -= 1;
                        }
                        return Some(self.hi | out);
                    }
                }
            }
            self.next_ci = self.next_ci.checked_sub(1)?;
            self.hi = u32::from(v.keys[self.next_ci]) << 16;
            self.cur = v.cursor(self.next_ci, None).0;
        }
    }
}

/// Iterator over the values `<= max` of `view`, newest first, and their count.
fn roaring_list(view: Arc<RoaringView>, max: Option<u32>, base: u64) -> Result<PostingIter> {
    let mut it = RoaringIter {
        next_ci: view.keys.len(),
        view: Arc::clone(&view),
        cur: Cursor::Empty,
        hi: 0,
    };
    let mut len = view.len();
    if let Some(m) = max {
        let (bk, low) = ((m >> 16) as u16, (m & 0xFFFF) as u16);
        // Containers [0, idx) have key <= bk.
        let idx = view.keys.partition_point(|&k| k <= bk);
        len = view.before[idx];
        it.next_ci = idx;
        if idx > 0 && view.keys[idx - 1] == bk {
            let (cur, n) = view.cursor(idx - 1, Some(low));
            len = view.before[idx - 1] + n;
            it.next_ci = idx - 1;
            it.cur = cur;
            it.hi = u32::from(bk) << 16;
        }
    }
    Ok(PostingIter {
        inner: Inner::Roaring(it),
        base,
        len: len as usize,
    })
}

/// Memo key of a posting list: the range (index in its `Segments`), family and entity.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct MemoKey {
    pub range: u32,
    pub family: u8,
    pub entity: u64,
}

/// Bytes one request may hold in a `ListMemo`.
pub const MEMO_CAP: usize = 64 << 20;

/// Per-request memo of parsed Roaring directories (R4). Containers are walked in place, so
/// there is nothing to decode and cache per container; what a re-seek repeats is the header
/// parse (keys, cardinalities, offsets, prefix counts), and that is what is kept. Cheap to
/// clone (shared). Entries past the cap are not kept; the walk then parses as without a memo.
#[derive(Clone, Default)]
pub struct ListMemo(Arc<Mutex<MemoInner>>);

#[derive(Default)]
struct MemoInner {
    views: HashMap<MemoKey, Arc<RoaringView>>,
    bytes: usize,
    hits: u64,
    misses: u64,
}

impl ListMemo {
    pub fn new() -> ListMemo {
        ListMemo::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MemoInner> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn view(
        &self,
        key: MemoKey,
        parse: impl FnOnce() -> Result<Arc<RoaringView>>,
    ) -> Result<Arc<RoaringView>> {
        {
            let mut g = self.lock();
            if let Some(v) = g.views.get(&key).map(Arc::clone) {
                g.hits += 1;
                return Ok(v);
            }
        }
        let v = parse()?;
        let mut g = self.lock();
        g.misses += 1;
        if g.bytes + v.memo_size() <= MEMO_CAP {
            g.bytes += v.memo_size();
            g.views.insert(key, Arc::clone(&v));
        }
        Ok(v)
    }

    /// (hits, misses, bytes held).
    pub fn stats(&self) -> (u64, u64, usize) {
        let g = self.lock();
        (g.hits, g.misses, g.bytes)
    }
}

#[cfg(test)]
mod list_tests {
    use super::*;

    /// R4: the lazy walk of `list_le` matches a full decode, for every encoding and bound.
    #[test]
    fn list_le_matches_full_decode() {
        let range_txs = 1u64 << 20;
        for len in [1usize, 16, 17, 500, 40_000] {
            let vals: Vec<u32> = (0..len as u32).map(|i| i * 13 + (i % 7)).collect();
            let mut b = Vec::new();
            encode_list(&vals, range_txs, &mut b).unwrap();
            let last = *vals.last().unwrap();
            for max in [
                None,
                Some(0),
                Some(5),
                Some(last / 2),
                Some(last),
                Some(last + 9),
            ] {
                let mut want: Vec<u64> = vals
                    .iter()
                    .filter(|&&v| max.is_none_or(|m| v <= m))
                    .map(|&v| 1000 + v as u64)
                    .collect();
                want.reverse();
                let it = list_le(&b, max, 1000).unwrap();
                assert_eq!(it.len(), want.len(), "len {len} max {max:?}");
                assert_eq!(it.collect::<Vec<_>>(), want, "len {len} max {max:?}");
            }
        }
    }

    /// A Roaring list with array, bitmap, run and full containers, as serialized bytes
    /// (with the tag byte), and its values.
    fn roaring_fixture(runs: bool, big: bool) -> (Vec<u8>, Vec<u32>) {
        let mut r = RoaringBitmap::new();
        // key 0: sparse array; key 1: dense bitmap (every other value); key 2: long runs.
        r.extend((0..300u32).map(|i| i * 211));
        r.extend((0..65536u32).step_by(2).map(|i| (1 << 16) | i));
        r.extend((0..40_000u32).map(|i| (2 << 16) | (i + 1000)));
        r.extend((0..8u32).map(|i| (2 << 16) | (50_000 + 3 * i)));
        if big {
            // keys 3..=32 full (1.97M values), key 33: one value at its top edge.
            r.insert_range(3 << 16..33 << 16);
            r.insert((33 << 16) | 65535);
        }
        r.insert(40 << 16);
        if runs {
            r.optimize();
        } else {
            r.remove_run_compression();
        }
        let vals: Vec<u32> = r.iter().collect();
        let mut b = vec![ENC_ROARING];
        r.serialize_into(&mut b).unwrap();
        (b, vals)
    }

    /// Task 4: the lazy Roaring walk equals the full decode filtered to `<= bound`, for
    /// array, bitmap, run and full containers, at container edges and random bounds, with
    /// and without a memo.
    #[test]
    fn roaring_lazy_matches_full_decode() {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let mut rnd = move |hi: u32| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x % (u64::from(hi) + 1)) as u32
        };
        for (runs, big) in [(false, false), (true, false), (false, true), (true, true)] {
            let (b, vals) = roaring_fixture(runs, big);
            let last = *vals.last().unwrap();
            let mut bounds: Vec<Option<u32>> = vec![None, Some(0), Some(last), Some(last + 1)];
            for k in [0u32, 1, 2, 3, 32, 33, 34, 39, 40, 41] {
                bounds.extend([
                    Some((k << 16).saturating_sub(1)),
                    Some(k << 16),
                    Some((k << 16) | 65535),
                ]);
            }
            bounds.extend((0..20).map(|_| Some(rnd(last))));
            let memo = ListMemo::new();
            let backing: Backing = Arc::new(b.clone());
            let view = Arc::new(RoaringView::parse(backing, 1, b.len()).unwrap());
            assert_eq!(view.len(), vals.len() as u64, "runs {runs} big {big}");
            for (n, max) in bounds.into_iter().enumerate() {
                let mut want: Vec<u64> = vals
                    .iter()
                    .filter(|&&v| max.is_none_or(|m| v <= m))
                    .map(|&v| 9 + v as u64)
                    .collect();
                want.reverse();
                let it = roaring_list(Arc::clone(&view), max, 9).unwrap();
                assert_eq!(it.len(), want.len(), "runs {runs} big {big} max {max:?}");
                assert_eq!(
                    it.collect::<Vec<_>>(),
                    want,
                    "runs {runs} big {big} #{n} max {max:?}"
                );
                // The same through the memo (twice: miss, then hit).
                let key = MemoKey {
                    range: 0,
                    family: 0,
                    entity: 1,
                };
                for _ in 0..2 {
                    let v = memo.view(key, || Ok(Arc::clone(&view))).unwrap();
                    let it = roaring_list(v, max, 9).unwrap();
                    assert_eq!(it.take(5).count(), want.len().min(5));
                }
            }
            let (hits, misses, bytes) = memo.stats();
            assert!(hits > 0 && misses == 1 && bytes > 0 && bytes <= MEMO_CAP);
        }
    }

    /// Run cookie with fewer than 4 containers has no offset header: containers are read in
    /// sequence.
    #[test]
    fn roaring_run_cookie_without_offsets() {
        let mut r = RoaringBitmap::new();
        r.insert_range(100..30_000);
        r.insert_range((1 << 16) + 5..(1 << 16) + 9_000);
        r.optimize();
        let vals: Vec<u32> = r.iter().collect();
        let mut b = vec![ENC_ROARING];
        r.serialize_into(&mut b).unwrap();
        assert_eq!(b[1] as u16 | (b[2] as u16) << 8, 12347);
        for max in [
            None,
            Some(99),
            Some(100),
            Some(29_999),
            Some(70_000),
            Some(u32::MAX),
        ] {
            let mut want: Vec<u64> = vals
                .iter()
                .filter(|&&v| max.is_none_or(|m| v <= m))
                .map(|&v| v as u64)
                .collect();
            want.reverse();
            let it = list_le(&b, max, 0).unwrap();
            assert_eq!(it.len(), want.len());
            assert_eq!(it.collect::<Vec<_>>(), want, "max {max:?}");
        }
        // Truncated bytes are an error, never a panic.
        for cut in [3usize, 9, 20, b.len() - 1] {
            let backing: Backing = Arc::new(b[..cut].to_vec());
            assert!(RoaringView::parse(backing, 1, cut).is_err(), "cut {cut}");
        }
    }

    /// Every Roaring list of the `*.pst` files in `$KDX_DEV_SEGMENTS`: the lazy walk from 20
    /// pseudo-random bounds (and none) equals the full decode filtered to `<= bound`. Run with
    /// `KDX_DEV_SEGMENTS=<segments dir> cargo test -p kdx-segment -- --ignored --nocapture
    /// roaring_dev_store_equality`.
    #[test]
    #[ignore]
    fn roaring_dev_store_equality() {
        let dir = std::env::var("KDX_DEV_SEGMENTS").expect("KDX_DEV_SEGMENTS");
        let mut x = 0x2545_F491_4F6C_DD1Du64;
        let (mut lists, mut walks) = (0u64, 0u64);
        for f in std::fs::read_dir(dir).unwrap() {
            let f = f.unwrap().path();
            if f.extension().is_none_or(|e| e != "pst") {
                continue;
            }
            let pst = Postings::open(&f, 0).unwrap();
            for i in 0..pst.entities() {
                let ent = pst.entity_at(i);
                let full = pst.list_at(i).unwrap();
                if pst.list_bytes(i).unwrap()[0] != ENC_ROARING {
                    continue;
                }
                lists += 1;
                let last = *full.last().unwrap();
                for k in 0..21 {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let max = (k > 0).then(|| (x % (u64::from(last) + 2)) as u32);
                    let mut want: Vec<u64> = full
                        .iter()
                        .filter(|&&v| max.is_none_or(|m| v <= m))
                        .map(|&v| v as u64)
                        .collect();
                    want.reverse();
                    let it = pst.list_le(ent, max).unwrap().unwrap();
                    assert_eq!(it.len(), want.len(), "{f:?} entity {ent} max {max:?}");
                    assert_eq!(
                        it.collect::<Vec<_>>(),
                        want,
                        "{f:?} entity {ent} max {max:?}"
                    );
                    walks += 1;
                }
            }
        }
        println!("{lists} Roaring lists, {walks} bounded walks equal to the full decode");
        assert!(lists > 0, "no Roaring list in the store");
    }

    /// Timing, as d2d998c did for EF: a page of 10 from a 2M-entry Roaring list, full
    /// decode (`RoaringBitmap::deserialize_from`) vs the lazy walk. Run with
    /// `cargo test -p kdx-segment --release -- --ignored --nocapture roaring_page_timing`.
    #[test]
    #[ignore]
    fn roaring_page_timing() {
        let (b, vals) = roaring_fixture(false, true);
        let mid = vals[vals.len() / 2];
        let t = std::time::Instant::now();
        for _ in 0..50 {
            let r = RoaringBitmap::deserialize_from(&b[1..]).unwrap();
            let mut it = r.into_iter();
            it.advance_back_to(mid);
            let p: Vec<u32> = std::iter::from_fn(|| it.next_back()).take(10).collect();
            assert_eq!(p.len(), 10);
        }
        let full = t.elapsed() / 50;
        let backing: Backing = Arc::new(b.clone());
        let t = std::time::Instant::now();
        for _ in 0..50 {
            let view = Arc::new(RoaringView::parse(Arc::clone(&backing), 1, b.len()).unwrap());
            let p: Vec<u64> = roaring_list(view, Some(mid), 0).unwrap().take(10).collect();
            assert_eq!(p.len(), 10);
        }
        let lazy = t.elapsed() / 50;
        println!(
            "{} entries: full decode {full:?}, lazy page of 10 {lazy:?}",
            vals.len()
        );
        assert!(lazy < full);
    }

    /// Timing: a page of 10 from a large EF list must not cost a full decode. Run with
    /// `cargo test -p kdx-segment --release -- --ignored --nocapture ef_page_timing`.
    #[test]
    #[ignore]
    fn ef_page_timing() {
        let range_txs = 1u64 << 26;
        let vals: Vec<u32> = (0..(range_txs / 32) as u32).map(|i| i * 31).collect();
        let mut b = Vec::new();
        encode_list(&vals, range_txs, &mut b).unwrap();
        assert_eq!(b[0], ENC_EF);
        let t = std::time::Instant::now();
        for _ in 0..100 {
            let _ = decode_list(&b).unwrap();
        }
        let full = t.elapsed() / 100;
        let t = std::time::Instant::now();
        for _ in 0..100 {
            let p: Vec<u64> = list_le(&b, Some(vals[vals.len() / 2]), 0)
                .unwrap()
                .take(10)
                .collect();
            assert_eq!(p.len(), 10);
        }
        let page = t.elapsed() / 100;
        println!(
            "{} entries: full decode {full:?}, page of 10 {page:?}",
            vals.len()
        );
        assert!(page < full);
    }

    /// `segments check --deep`: the lazy Roaring walk of a written file equals the full
    /// decode; a full decode that disagrees with the file is reported.
    #[test]
    fn lazy_walk_matches_written_lists() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("x.pst");
        let range_txs = 300_000u64;
        let mut w = PostingsWriter::create(&path, Family::Acct, range_txs).unwrap();
        let big: Vec<u32> = (0..200_000u32).filter(|v| v % 7 < 3).collect();
        let small: Vec<u32> = vec![1, 5, 9];
        w.push(3, &small).unwrap();
        w.push(8, &big).unwrap();
        w.finish().unwrap();
        let p = Postings::open(&path, 100).unwrap();
        assert_eq!(p.list_bytes(1).unwrap()[0], ENC_ROARING);
        for i in 0..p.entities() {
            let full = p.list_at(i).unwrap();
            for seed in [0u64, 1, 65_536, 123_456_789] {
                assert!(p.lazy_walk_matches(i, &full, seed).unwrap());
            }
        }
        let mut wrong = p.list_at(1).unwrap();
        wrong.pop();
        assert!(!p.lazy_walk_matches(1, &wrong, 0).unwrap());
    }

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
