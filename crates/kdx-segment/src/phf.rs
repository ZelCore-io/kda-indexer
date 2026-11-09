//! `rk.idx` (request key → tx offset in range) and `bh.idx` (block hash → BlockOrd slot in
//! range), spec §3.7. Keys are reduced to their first 8 bytes (big-endian u64 "prefix"); the
//! next 4 bytes are a fingerprint. Every hit is confirmed by the caller against the full
//! 32-byte key in the record.
//!
//! File: `b"KDXH"`, `u8 version = 1`, `u8 kind` (0 = PtrHash, 1 = sorted), `u64 LE n`, then
//! - PtrHash: `f64 LE alpha`, `f64 LE lambda`, `u64 LE seed`, `u64 LE pilots_len`, pilots,
//!   `u64 LE remap_len`, remap `u32 LE` each, zero padding to a multiple of 4, then `n` slots
//!   of `(u32 LE offset, u32 LE fingerprint)` indexed by the PtrHash value.
//! - sorted: `n` × (`[8]` prefix big-endian, `[5]` offset big-endian), ascending by prefix then
//!   offset; binary search, duplicates allowed.
//!
//! The PtrHash form is used unless two keys of the range share a prefix, the build fails, or
//! the fallback is forced (`WriteOpts::force_sorted_index` / env `KDX_DEBUG_RK_FALLBACK=1`).

use std::io::Write;
use std::path::Path;

use anyhow::{bail, Context, Result};
use kdx_types::Hash32;
use memmap2::Mmap;
use ptr_hash::bucket_fn::Linear;
use ptr_hash::{PtrHash, PtrHashParams};

use crate::files::DurableFile;

pub const MAGIC: &[u8; 4] = b"KDXH";

type Phf = PtrHash<u64, Linear, Vec<u32>, ptr_hash::hash::FxHash, Vec<u8>, true, true>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    PtrHash,
    Sorted,
}

impl IndexKind {
    pub fn name(self) -> &'static str {
        match self {
            IndexKind::PtrHash => "ptrhash",
            IndexKind::Sorted => "sorted",
        }
    }
}

pub fn prefix(k: &Hash32) -> u64 {
    u64::from_be_bytes(k.0[..8].try_into().expect("8 bytes"))
}

pub fn fingerprint(k: &Hash32) -> u32 {
    u32::from_be_bytes(k.0[8..12].try_into().expect("4 bytes"))
}

/// One key of an index under construction.
#[derive(Debug, Clone, Copy)]
pub struct IndexKey {
    pub prefix: u64,
    pub fp: u32,
    pub offset: u32,
}

impl IndexKey {
    pub fn new(k: &Hash32, offset: u32) -> IndexKey {
        IndexKey {
            prefix: prefix(k),
            fp: fingerprint(k),
            offset,
        }
    }
}

pub trait KeyIndex {
    /// First candidate offset for `key` (confirm against the record).
    fn get(&self, key: &Hash32) -> Option<u32> {
        let mut v = Vec::new();
        self.candidates(key, &mut v);
        v.first().copied()
    }
    /// All candidate offsets for `key` (several only in a sorted index with a shared prefix).
    fn candidates(&self, key: &Hash32, out: &mut Vec<u32>);
}

/// Env switch forcing the sorted fallback (Review Focus 4 check).
pub fn debug_fallback_env() -> bool {
    std::env::var("KDX_DEBUG_RK_FALLBACK").is_ok_and(|v| !v.is_empty() && v != "0")
}

/// Build and write an index; returns the kind written.
pub fn write_index(path: &Path, mut keys: Vec<IndexKey>, force_sorted: bool) -> Result<IndexKind> {
    keys.sort_unstable_by_key(|k| (k.prefix, k.offset));
    let dup = keys.windows(2).any(|w| w[0].prefix == w[1].prefix);
    let phf = if force_sorted || dup || keys.is_empty() {
        None
    } else {
        let prefixes: Vec<u64> = keys.iter().map(|k| k.prefix).collect();
        let params = PtrHashParams::default_fast();
        std::panic::catch_unwind(|| Phf::try_new(&prefixes, params))
            .ok()
            .flatten()
    };
    let mut f = DurableFile::create(path)?;
    f.write_all(MAGIC)?;
    f.write_all(&[1])?;
    let n = keys.len() as u64;
    let kind = match phf {
        Some(p) => {
            f.write_all(&[0])?;
            f.write_all(&n.to_le_bytes())?;
            let params = PtrHashParams::default_fast();
            f.write_all(&params.alpha.to_le_bytes())?;
            f.write_all(&params.lambda.to_le_bytes())?;
            let (pn, seed, pilots, remap) = p.saved_parts();
            debug_assert_eq!(pn as u64, n);
            f.write_all(&seed.to_le_bytes())?;
            f.write_all(&(pilots.len() as u64).to_le_bytes())?;
            f.write_all(pilots)?;
            f.write_all(&(remap.len() as u64).to_le_bytes())?;
            for r in remap {
                f.write_all(&r.to_le_bytes())?;
            }
            let written = 4 + 1 + 1 + 8 + 16 + 8 + 8 + pilots.len() + 8 + 4 * remap.len();
            let pad = (4 - written % 4) % 4;
            f.write_all(&[0u8; 4][..pad])?;
            let mut slots = vec![(0u32, 0u32); keys.len()];
            for k in &keys {
                let i = p.index(&k.prefix);
                slots[i] = (k.offset, k.fp);
            }
            for (o, fp) in slots {
                f.write_all(&o.to_le_bytes())?;
                f.write_all(&fp.to_le_bytes())?;
            }
            IndexKind::PtrHash
        }
        None => {
            f.write_all(&[1])?;
            f.write_all(&n.to_le_bytes())?;
            for k in &keys {
                f.write_all(&k.prefix.to_be_bytes())?;
                f.write_all(&(k.offset as u64).to_be_bytes()[3..])?;
            }
            IndexKind::Sorted
        }
    };
    f.finish()?;
    Ok(kind)
}

/// An opened index (either form).
pub enum Index {
    PtrHash(Box<PtrHashIndex>),
    Sorted(SortedIndex),
}

pub struct PtrHashIndex {
    phf: Phf,
    map: Mmap,
    slots_at: usize,
    n: usize,
}

pub struct SortedIndex {
    map: Mmap,
    n: usize,
}

fn rd_u64(b: &[u8], at: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        b.get(at..at + 8).context("short index")?.try_into()?,
    ))
}

impl Index {
    pub fn open(path: &Path) -> Result<Index> {
        let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        // SAFETY: segment files are immutable after rename.
        let map = unsafe { Mmap::map(&file)? };
        if map.len() < 14 || &map[..4] != MAGIC || map[4] != 1 {
            bail!("{}: not an index v1 file", path.display());
        }
        let n = rd_u64(&map, 6)? as usize;
        match map[5] {
            0 => {
                let alpha = f64::from_le_bytes(map[14..22].try_into()?);
                let lambda = f64::from_le_bytes(map[22..30].try_into()?);
                let seed = rd_u64(&map, 30)?;
                let pl = rd_u64(&map, 38)? as usize;
                let pilots = map.get(46..46 + pl).context("short pilots")?.to_vec();
                let rl = rd_u64(&map, 46 + pl)? as usize;
                let ra = 54 + pl;
                let rb = map.get(ra..ra + 4 * rl).context("short remap")?;
                let remap: Vec<u32> = rb
                    .chunks_exact(4)
                    .map(|c| u32::from_le_bytes(c.try_into().expect("4")))
                    .collect();
                let end = ra + 4 * rl;
                let slots_at = end + (4 - end % 4) % 4;
                if map.len() != slots_at + 8 * n {
                    bail!(
                        "{}: size {} != {}",
                        path.display(),
                        map.len(),
                        slots_at + 8 * n
                    );
                }
                let mut params = PtrHashParams::default_fast();
                params.alpha = alpha;
                params.lambda = lambda;
                let phf = Phf::from_saved_parts(n, params, seed, pilots, remap);
                Ok(Index::PtrHash(Box::new(PtrHashIndex {
                    phf,
                    map,
                    slots_at,
                    n,
                })))
            }
            1 => {
                if map.len() != 14 + 13 * n {
                    bail!("{}: size {} != {}", path.display(), map.len(), 14 + 13 * n);
                }
                Ok(Index::Sorted(SortedIndex { map, n }))
            }
            k => bail!("{}: unknown index kind {k}", path.display()),
        }
    }

    pub fn kind(&self) -> IndexKind {
        match self {
            Index::PtrHash(_) => IndexKind::PtrHash,
            Index::Sorted(_) => IndexKind::Sorted,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Index::PtrHash(p) => p.n,
            Index::Sorted(s) => s.n,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl KeyIndex for Index {
    fn candidates(&self, key: &Hash32, out: &mut Vec<u32>) {
        match self {
            Index::PtrHash(p) => p.candidates(key, out),
            Index::Sorted(s) => s.candidates(key, out),
        }
    }
}

impl KeyIndex for PtrHashIndex {
    fn candidates(&self, key: &Hash32, out: &mut Vec<u32>) {
        out.clear();
        if self.n == 0 {
            return;
        }
        let i = self.phf.index(&prefix(key));
        if i >= self.n {
            return;
        }
        let at = self.slots_at + 8 * i;
        let o = u32::from_le_bytes(self.map[at..at + 4].try_into().expect("4"));
        let fp = u32::from_le_bytes(self.map[at + 4..at + 8].try_into().expect("4"));
        if fp == fingerprint(key) {
            out.push(o);
        }
    }
}

impl SortedIndex {
    fn entry(&self, i: usize) -> (u64, u32) {
        let at = 14 + 13 * i;
        let p = u64::from_be_bytes(self.map[at..at + 8].try_into().expect("8"));
        let mut o = [0u8; 8];
        o[3..].copy_from_slice(&self.map[at + 8..at + 13]);
        (p, u64::from_be_bytes(o) as u32)
    }
}

impl KeyIndex for SortedIndex {
    fn candidates(&self, key: &Hash32, out: &mut Vec<u32>) {
        out.clear();
        let p = prefix(key);
        let (mut lo, mut hi) = (0usize, self.n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.entry(mid).0 < p {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        while lo < self.n {
            let (q, o) = self.entry(lo);
            if q != p {
                break;
            }
            out.push(o);
            lo += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two builds of the same keys give byte-identical files, and every key resolves.
    #[test]
    fn ptrhash_build_is_reproducible() {
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let keys: Vec<Hash32> = (0..200_000)
            .map(|_| {
                let mut h = [0u8; 32];
                for c in h.chunks_mut(8) {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    c.copy_from_slice(&x.to_le_bytes());
                }
                Hash32(h)
            })
            .collect();
        let dir = std::env::temp_dir().join(format!("kdx-phf-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mk = |name: &str| {
            let p = dir.join(name);
            let ik = keys
                .iter()
                .enumerate()
                .map(|(i, k)| IndexKey::new(k, i as u32))
                .collect();
            assert_eq!(write_index(&p, ik, false).unwrap(), IndexKind::PtrHash);
            p
        };
        let (a, b) = (mk("a.idx"), mk("b.idx"));
        assert_eq!(std::fs::read(&a).unwrap(), std::fs::read(&b).unwrap());
        let idx = Index::open(&a).unwrap();
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(idx.get(k), Some(i as u32));
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
