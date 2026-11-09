//! Adjacent-parent refs in `blocks.seg` (record flags.b2, form 1): an adjacent whose hash
//! equals the stored hash of block (adjacent chain, height − 1) **in the same range** is
//! written as a 1-byte ref. The first height of a range is always written raw, so a range
//! decodes without its neighbours.

use std::collections::HashMap;

use anyhow::{bail, Result};
use kdx_types::Hash32;

/// Writer-side tracker of the block hashes of the previous and current height.
#[derive(Default)]
pub struct AdjTracker {
    height: Option<u64>,
    prev: HashMap<u8, Hash32>,
    cur: HashMap<u8, Hash32>,
}

impl AdjTracker {
    /// Call before encoding a block at `height` (blocks arrive in height order).
    pub fn begin(&mut self, height: u64) {
        match self.height {
            Some(h) if h == height => {}
            Some(h) if h + 1 == height => {
                self.prev = std::mem::take(&mut self.cur);
                self.height = Some(height);
            }
            _ => {
                self.prev.clear();
                self.cur.clear();
                self.height = Some(height);
            }
        }
    }

    /// Whether adjacent `(chain, hash)` can be stored as a ref.
    pub fn is_ref(&self, chain: u8, hash: &Hash32) -> bool {
        self.prev.get(&chain) == Some(hash)
    }

    /// Record the block just encoded.
    pub fn done(&mut self, chain: u8, hash: Hash32) {
        self.cur.insert(chain, hash);
    }
}

/// Block hash of an encoded block record (bytes 2..34, after version and flags).
pub fn record_block_hash(rec: &[u8]) -> Result<Hash32> {
    if rec.len() < 34 {
        bail!("short block record");
    }
    let mut h = [0u8; 32];
    h.copy_from_slice(&rec[2..34]);
    Ok(Hash32(h))
}
