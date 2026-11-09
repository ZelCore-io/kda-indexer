//! `keys.ef`: Elias-Fano list of the first TxNum (relative to the range's first TxNum) of every
//! BlockOrd slot, plus one end sentinel (= the range's tx count). A slot without a block
//! (chain not in the graph yet, spec Review Focus 1) repeats the next slot's start, so its tx
//! count is 0.
//!
//! File: `b"KDXK"`, `u8 version = 1`, `u32 LE slot_width`, `u64 LE slots`, then the sucds
//! `EliasFano` serialization of `slots + 1` values (rank enabled on load).

use std::io::Write;
use std::path::Path;

use anyhow::{bail, Context, Result};
use sucds::mii_sequences::{EliasFano, EliasFanoBuilder};
use sucds::Serializable;

use crate::files::DurableFile;

pub const MAGIC: &[u8; 4] = b"KDXK";

/// Write `keys.ef` from per-slot tx counts.
pub fn write_keys(path: &Path, slot_width: u32, counts: &[u32]) -> Result<()> {
    let total: u64 = counts.iter().map(|&c| c as u64).sum();
    let mut b = EliasFanoBuilder::new(total as usize + 1, counts.len() + 1)?;
    let mut acc = 0u64;
    for &c in counts {
        b.push(acc as usize)?;
        acc += c as u64;
    }
    b.push(acc as usize)?;
    let ef = b.build();
    let mut f = DurableFile::create(path)?;
    f.write_all(MAGIC)?;
    f.write_all(&[1])?;
    f.write_all(&slot_width.to_le_bytes())?;
    f.write_all(&(counts.len() as u64).to_le_bytes())?;
    ef.serialize_into(&mut f)?;
    f.finish()
}

pub struct Keys {
    ef: EliasFano,
    pub slot_width: u32,
    slots: usize,
}

impl Keys {
    pub fn open(path: &Path) -> Result<Keys> {
        let b = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
        if b.len() < 17 || &b[..4] != MAGIC || b[4] != 1 {
            bail!("{}: not a keys.ef v1 file", path.display());
        }
        let slot_width = u32::from_le_bytes(b[5..9].try_into()?);
        let slots = u64::from_le_bytes(b[9..17].try_into()?) as usize;
        let ef = EliasFano::deserialize_from(&b[17..])?.enable_rank();
        if ef.len() != slots + 1 {
            bail!("{}: {} values for {slots} slots", path.display(), ef.len());
        }
        Ok(Keys {
            ef,
            slot_width,
            slots,
        })
    }

    pub fn slots(&self) -> usize {
        self.slots
    }

    /// Txs in the range (end sentinel).
    pub fn tx_count(&self) -> u64 {
        self.ef.select(self.slots).unwrap_or(0) as u64
    }

    /// First relative TxNum of slot `s` (`s == slots` gives the tx count).
    pub fn first(&self, s: usize) -> Option<u64> {
        self.ef.select(s).map(|v| v as u64)
    }

    /// Txs in slot `s` (0 = no block).
    pub fn count(&self, s: usize) -> u64 {
        match (self.first(s), self.first(s + 1)) {
            (Some(a), Some(b)) => b - a,
            _ => 0,
        }
    }

    /// Slot holding relative TxNum `t`: the last slot whose start is <= t.
    pub fn slot_of(&self, t: u64) -> Option<usize> {
        if t >= self.tx_count() {
            return None;
        }
        // rank(t + 1) = values <= t; the sentinel is > t, so the result is a real slot.
        let r = self.ef.rank(t as usize + 1)?;
        r.checked_sub(1)
    }
}
