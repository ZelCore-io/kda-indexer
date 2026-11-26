//! Stored per-key row counts (`RowCounts`, PR #48 part 2): the exact `totalCount` of the
//! posting-keyed GraphQL lists without a walk.
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | `RowCounts` | kind u8 (`kdx_decode::rowcount::CountKind`), entity u64, chain u8 | user rows u64, coinbase rows u64 |
//!
//! - The table is global, frozen and hot alike: `tail::apply` adds each tx's terms
//!   ([`kdx_decode::rowcount::tx_counts`]) in the block's write txn, and `tail::unwind_to`
//!   subtracts the same terms, recomputed from the record before it is deleted (the undo
//!   format does not change). The freezer moves records and leaves the counts alone.
//! - `Meta row_counts_ready` holds the TxNum frontier the counts were built at by
//!   `kda-indexer reconcile-counts` ([`set_ready`]). Readers use the table only while it is
//!   set ([`ready`]); every path that writes frozen ranges outside the freezer clears it
//!   ([`clear_ready`]), and so does a tx whose output does not render. Without the marker the
//!   table still follows the tail, it is just not trusted.

use std::collections::HashMap;

use anyhow::{Context, Result};
use kdx_decode::rowcount::{tx_counts, CountKind};
use kdx_types::record::TxRecord;
use kdx_types::Dict;

use crate::env::{RwTxn, Txn};
use crate::tables::Table;

/// Meta key of the readiness marker.
pub const READY: &str = "row_counts_ready";

pub fn key(kind: CountKind, entity: u64, chain: u8) -> [u8; 10] {
    let mut k = [0u8; 10];
    k[0] = kind as u8;
    k[1..9].copy_from_slice(&entity.to_be_bytes());
    k[9] = chain;
    k
}

pub fn parse_key(k: &[u8]) -> Option<(CountKind, u64, u8)> {
    if k.len() != 10 {
        return None;
    }
    Some((
        CountKind::from_u8(k[0])?,
        u64::from_be_bytes(k[1..9].try_into().ok()?),
        k[9],
    ))
}

/// `(user, coinbase)` rows of one value.
pub fn decode_val(v: &[u8]) -> (u64, u64) {
    let g = |r: std::ops::Range<usize>| {
        v.get(r)
            .and_then(|b| b.try_into().ok())
            .map_or(0, u64::from_be_bytes)
    };
    (g(0..8), g(8..16))
}

pub fn encode_val((u, c): (u64, u64)) -> [u8; 16] {
    let mut v = [0u8; 16];
    v[..8].copy_from_slice(&u.to_be_bytes());
    v[8..].copy_from_slice(&c.to_be_bytes());
    v
}

/// Whether the stored counts may answer (the marker is set and the table exists).
pub fn ready(txn: &impl Txn) -> Result<bool> {
    Ok(txn.has_table(Table::RowCounts) && txn.meta(READY)?.is_some())
}

/// Set the marker at TxNum frontier `next_txnum` (the end of what `reconcile-counts` read).
pub fn set_ready(txn: &mut RwTxn<'_>, next_txnum: u64) -> Result<()> {
    txn.set_meta_u64(READY, next_txnum)
}

/// Clear the marker: the counts no longer describe the store; readers walk until the next
/// `reconcile-counts`. Returns whether it was set.
pub fn clear_ready(txn: &mut RwTxn<'_>) -> Result<bool> {
    let was = txn.meta(READY)?.is_some();
    if was {
        txn.del(Table::Meta, READY.as_bytes())?;
        tracing::warn!("row counts: readiness marker cleared; totalCount walks until `kda-indexer reconcile-counts`");
    }
    Ok(was)
}

/// `(user, coinbase)` rows stored for `(kind, entity)`, on `chain` or summed over all chains.
pub fn sum(txn: &impl Txn, kind: CountKind, entity: u64, chain: Option<u8>) -> Result<(u64, u64)> {
    if let Some(c) = chain {
        return Ok(txn
            .get(Table::RowCounts, &key(kind, entity, c))?
            .map_or((0, 0), |v| decode_val(&v)));
    }
    let from = key(kind, entity, 0);
    let rows = txn.scan(Table::RowCounts, &from, |k| k.get(..9) == Some(&from[..9]))?;
    Ok(rows.iter().fold((0, 0), |(u, c), (_, v)| {
        let (a, b) = decode_val(v);
        (u + a, c + b)
    }))
}

/// Add `sign` × the terms of `r` (1 at apply, -1 at unwind). A record whose output does not
/// render clears the marker instead (the walk fails on it too, so no count is trusted).
pub fn add_tx(txn: &mut RwTxn<'_>, r: &TxRecord, dict: &dyn Dict, sign: i64) -> Result<()> {
    let terms = match tx_counts(r, dict) {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(chain = r.key.chain(), height = r.key.height(), pos = r.key.pos(),
                error = %e, "row counts: output does not render");
            clear_ready(txn)?;
            return Ok(());
        }
    };
    let cb = r.key.is_coinbase();
    for t in terms {
        let k = key(t.kind, t.entity, r.key.chain());
        let (mut u, mut c) = txn
            .get(Table::RowCounts, &k)?
            .map_or((0, 0), |v| decode_val(&v));
        let slot = if cb { &mut c } else { &mut u };
        *slot = if sign >= 0 {
            slot.saturating_add(t.rows)
        } else {
            match slot.checked_sub(t.rows) {
                Some(v) => v,
                None => {
                    // Only possible when the table was not built for this record (the marker
                    // is not set then, or the store is damaged): never trusted again as is.
                    clear_ready(txn)?;
                    0
                }
            }
        };
        if (u, c) == (0, 0) {
            txn.del(Table::RowCounts, &k)?;
        } else {
            txn.put(Table::RowCounts, &k, &encode_val((u, c)))?;
        }
    }
    Ok(())
}

/// Counts being built in memory (`reconcile-counts`), keyed like the table.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Tally(pub HashMap<(CountKind, u64, u8), (u64, u64)>);

impl Tally {
    /// Add the terms of `r`; `false` when its output does not render.
    pub fn add(&mut self, r: &TxRecord, dict: &dyn Dict) -> bool {
        let Ok(terms) = tx_counts(r, dict) else {
            return false;
        };
        let cb = r.key.is_coinbase();
        for t in terms {
            let e = self
                .0
                .entry((t.kind, t.entity, r.key.chain()))
                .or_insert((0, 0));
            if cb {
                e.1 += t.rows;
            } else {
                e.0 += t.rows;
            }
        }
        true
    }

    pub fn merge(&mut self, mut o: Tally) {
        if self.0.len() < o.0.len() {
            std::mem::swap(&mut self.0, &mut o.0);
        }
        for (k, (u, c)) in o.0 {
            let e = self.0.entry(k).or_insert((0, 0));
            e.0 += u;
            e.1 += c;
        }
    }
}

/// Every `RowCounts` row as a [`Tally`] (tests and `reconcile-counts --check`).
pub fn read_all(txn: &impl Txn) -> Result<Tally> {
    let mut t = Tally::default();
    if !txn.has_table(Table::RowCounts) {
        return Ok(t);
    }
    for (k, v) in txn.scan(Table::RowCounts, &[], |_| true)? {
        let k = parse_key(&k).context("bad RowCounts key")?;
        t.0.insert(k, decode_val(&v));
    }
    Ok(t)
}

/// Delete every `RowCounts` row.
pub fn clear_table(txn: &mut RwTxn<'_>) -> Result<()> {
    loop {
        let rows = txn.scan_limit(Table::RowCounts, &[], 100_000, |_| true)?;
        if rows.is_empty() {
            return Ok(());
        }
        for (k, _) in rows {
            txn.del(Table::RowCounts, &k)?;
        }
    }
}
