//! Stage checkpoints: `(stage u8, chain u8) -> height u32 + hash [32]`.

use anyhow::{Context, Result};
use kdx_types::Hash32;

use crate::env::{be_u32, RwTxn, Txn};
use crate::tables::{parse_hash, Table};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Headers = 1,
    Bodies = 2,
    Tail = 5,
    Freeze = 6,
}

fn key(s: Stage, chain: u8) -> [u8; 2] {
    [s as u8, chain]
}

pub fn checkpoint(txn: &impl Txn, s: Stage, chain: u8) -> Result<Option<(u32, Hash32)>> {
    txn.get(Table::StageCheckpoint, &key(s, chain))?
        .map(|v| {
            let h = be_u32(v.get(..4).context("short checkpoint")?).context("checkpoint height")?;
            let hash = parse_hash(&v[4..]).context("checkpoint hash")?;
            Ok((h, hash))
        })
        .transpose()
}

pub fn set_checkpoint(
    txn: &mut RwTxn<'_>,
    s: Stage,
    chain: u8,
    height: u32,
    hash: Hash32,
) -> Result<()> {
    let mut v = [0u8; 36];
    v[..4].copy_from_slice(&height.to_be_bytes());
    v[4..].copy_from_slice(&hash.0);
    txn.put(Table::StageCheckpoint, &key(s, chain), &v)
}

pub fn clear_checkpoint(txn: &mut RwTxn<'_>, s: Stage, chain: u8) -> Result<bool> {
    txn.del(Table::StageCheckpoint, &key(s, chain))
}
