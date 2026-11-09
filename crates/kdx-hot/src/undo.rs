//! Undo records (spec §4.5): one row per tail block, the ops that reverse its writes.
//! Interned names are never undone (§3.2).
//!
//! Encoding: varint op count, then per op a tag byte and fields (integers big-endian):
//! 0 Header chain u8, height u32, hash [32] | 1 Tx TxKey u64 | 2 Rk [32] |
//! 3 Posting family u8, entity u64, TxKey u64 | 4 DefpactStep pact [32], TxKey u64 |
//! 5 Counter name bytes (varint len), delta i64.

use anyhow::{bail, Context, Result};
use kdx_types::varint::{get_bytes, get_u64, get_u8, put_bytes, put_u64, take_array};
use kdx_types::{Hash32, TxKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UndoOp {
    Header {
        chain: u8,
        height: u32,
        hash: Hash32,
    },
    Tx(TxKey),
    Rk(Hash32),
    Posting {
        family: u8,
        entity: u64,
        key: TxKey,
    },
    DefpactStep {
        pact: Hash32,
        key: TxKey,
    },
    /// Builder call: an owned name (decoded undo rows cannot hand out `&'static str`).
    Counter {
        name: String,
        delta: i64,
    },
}

pub fn encode(ops: &[UndoOp]) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, ops.len() as u64);
    for op in ops {
        match op {
            UndoOp::Header {
                chain,
                height,
                hash,
            } => {
                out.push(0);
                out.push(*chain);
                out.extend_from_slice(&height.to_be_bytes());
                out.extend_from_slice(&hash.0);
            }
            UndoOp::Tx(k) => {
                out.push(1);
                out.extend_from_slice(&k.to_be_bytes());
            }
            UndoOp::Rk(h) => {
                out.push(2);
                out.extend_from_slice(&h.0);
            }
            UndoOp::Posting {
                family,
                entity,
                key,
            } => {
                out.push(3);
                out.push(*family);
                out.extend_from_slice(&entity.to_be_bytes());
                out.extend_from_slice(&key.to_be_bytes());
            }
            UndoOp::DefpactStep { pact, key } => {
                out.push(4);
                out.extend_from_slice(&pact.0);
                out.extend_from_slice(&key.to_be_bytes());
            }
            UndoOp::Counter { name, delta } => {
                out.push(5);
                put_bytes(&mut out, name.as_bytes());
                out.extend_from_slice(&delta.to_be_bytes());
            }
        }
    }
    out
}

pub fn decode(mut b: &[u8]) -> Result<Vec<UndoOp>> {
    let buf = &mut b;
    let n = get_u64(buf)? as usize;
    let mut ops = Vec::with_capacity(n.min(buf.len()));
    for _ in 0..n {
        ops.push(match get_u8(buf)? {
            0 => UndoOp::Header {
                chain: get_u8(buf)?,
                height: u32::from_be_bytes(take_array(buf)?),
                hash: Hash32(take_array(buf)?),
            },
            1 => UndoOp::Tx(TxKey::from_be_bytes(take_array(buf)?)),
            2 => UndoOp::Rk(Hash32(take_array(buf)?)),
            3 => UndoOp::Posting {
                family: get_u8(buf)?,
                entity: u64::from_be_bytes(take_array(buf)?),
                key: TxKey::from_be_bytes(take_array(buf)?),
            },
            4 => UndoOp::DefpactStep {
                pact: Hash32(take_array(buf)?),
                key: TxKey::from_be_bytes(take_array(buf)?),
            },
            5 => UndoOp::Counter {
                name: String::from_utf8(get_bytes(buf)?.to_vec()).context("counter name")?,
                delta: i64::from_be_bytes(take_array(buf)?),
            },
            t => bail!("unknown undo op tag {t}"),
        });
    }
    if !buf.is_empty() {
        bail!("{} trailing bytes after undo record", buf.len());
    }
    Ok(ops)
}
