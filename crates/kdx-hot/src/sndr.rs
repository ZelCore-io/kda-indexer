//! The `sndr` posting family in the hot store (FIX-2): backfill over an existing tail and the
//! rollback that removes it.
//!
//! New tail blocks get their `TailPostings` family-6 rows and `UndoOp::Posting{family: 6}`
//! ops from `tail::apply` like every other family. A tail written by an older binary has
//! none; [`backfill`] adds them, row for row and op for op as `apply` would have written
//! them, and sets `Meta sndr_hot = 1`. [`drop_family`] removes them again (rows, undo ops,
//! the `Meta` key) so an older binary, whose freezer rejects family 6, can run on the store.
//! Both run in the caller's write txn, offline (the tail must not run concurrently).

use anyhow::{bail, ensure, Context, Result};
use kdx_decode::index::indexed_sender;
use kdx_decode::Family;
use kdx_types::record::decode_tx;
use kdx_types::{Dict, DictKind, TxKey};

use crate::env::{meta, RwTxn, Txn};
use crate::tables::{block_key, parse_txkey, posting_key, Table};
use crate::undo::{self, UndoOp};

/// Rows per chunk when walking `TailTxs`, `TailPostings` and `Undo`.
const CHUNK: usize = 4096;

/// What [`backfill`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackfillStats {
    /// `TailTxs` records read.
    pub txs: u64,
    /// Records with an indexable sender (each has a family-6 row after the pass).
    pub senders: u64,
    /// Family-6 rows (and undo ops) this pass added; 0 on a re-run.
    pub inserted: u64,
    /// `Undo` rows rewritten.
    pub undo_rows: u64,
}

/// What [`drop_family`] removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DropStats {
    pub postings: u64,
    pub undo_ops: u64,
    pub undo_rows: u64,
}

/// Put the `sndr` posting of every `TailTxs` record (the AccountId of its indexed sender,
/// [`indexed_sender`]) and append its `UndoOp::Posting` to the block's `Undo` row, at the
/// place `tail::apply` puts it (after the tx's other posting ops: family 6 sorts last), so a
/// later unwind removes it. Then `Meta sndr_hot = 1`. Insert-if-absent: a row already present
/// gets no second undo op, so a re-run changes nothing. A sender that passes `account_ok`
/// but is not in `dict` is an error (`apply` interns it before writing the record).
pub fn backfill(txn: &mut RwTxn<'_>, dict: &impl Dict) -> Result<BackfillStats> {
    let mut st = BackfillStats::default();
    // Inserted (key, entity) of the current block, flushed into its Undo row on block change.
    let mut block: Option<(u8, u32)> = None;
    let mut added: Vec<(TxKey, u64)> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let rows = txn.scan_chunk(Table::TailTxs, after.as_deref(), CHUNK, |_| true)?;
        for (k, v) in &rows {
            let key = parse_txkey(k).context("TailTxs key")?;
            st.txs += 1;
            let bk = (key.chain(), key.height());
            if block != Some(bk) {
                flush(txn, block, &mut added, &mut st)?;
                block = Some(bk);
            }
            let r = decode_tx(v).with_context(|| format!("TailTxs record {key:?}"))?;
            ensure!(r.key == key, "TailTxs record {key:?} holds key {:?}", r.key);
            let Some(sender) =
                indexed_sender(&r).map_err(|e| anyhow::anyhow!("cmd of {key:?}: {e:?}"))?
            else {
                continue;
            };
            st.senders += 1;
            let id = dict
                .find(DictKind::Account, &sender)
                .with_context(|| format!("sender of {key:?} is not in the dictionary"))?;
            let ent = id as u64;
            if txn.insert(
                Table::TailPostings,
                &posting_key(Family::Sndr as u8, ent, key),
                &[],
            )? {
                added.push((key, ent));
            }
        }
        if rows.len() < CHUNK {
            break;
        }
        after = rows.last().map(|r| r.0.clone());
    }
    flush(txn, block, &mut added, &mut st)?;
    txn.set_meta_u64(meta::SNDR_HOT, 1)?;
    Ok(st)
}

/// Insert the undo ops of `added` (one block's new family-6 rows, in TxKey order) into that
/// block's `Undo` row.
fn flush(
    txn: &mut RwTxn<'_>,
    block: Option<(u8, u32)>,
    added: &mut Vec<(TxKey, u64)>,
    st: &mut BackfillStats,
) -> Result<()> {
    let Some((chain, height)) = block else {
        return Ok(());
    };
    if added.is_empty() {
        return Ok(());
    }
    let bk = block_key(chain, height);
    let v = txn
        .get(Table::Undo, &bk)?
        .with_context(|| format!("no Undo row for hot block ({chain}, {height})"))?;
    let mut ops = undo::decode(&v).with_context(|| format!("Undo ({chain}, {height})"))?;
    for (key, entity) in added.drain(..) {
        let at = op_slot(&ops, key)
            .with_context(|| format!("Undo ({chain}, {height}) has no Tx op for {key:?}"))?;
        ops.insert(
            at,
            UndoOp::Posting {
                family: Family::Sndr as u8,
                entity,
                key,
            },
        );
        st.inserted += 1;
    }
    txn.put(Table::Undo, &bk, &undo::encode(&ops))?;
    st.undo_rows += 1;
    Ok(())
}

/// Where `apply` puts the last posting op of tx `key`: after its `Tx` op, its `Rk` op and its
/// other `Posting` ops.
fn op_slot(ops: &[UndoOp], key: TxKey) -> Option<usize> {
    let mut i = ops.iter().position(|o| *o == UndoOp::Tx(key))? + 1;
    while let Some(o) = ops.get(i) {
        match o {
            UndoOp::Rk(_) => {}
            UndoOp::Posting { key: k, .. } if *k == key => {}
            _ => break,
        }
        i += 1;
    }
    Some(i)
}

/// Remove every trace of an optional posting family from the hot store: its `TailPostings`
/// rows, its `UndoOp::Posting` ops in `Undo` rows, and its `Meta` readiness key. Only `sndr`
/// is optional; the required families are refused.
pub fn drop_family(txn: &mut RwTxn<'_>, family: Family) -> Result<DropStats> {
    if family != Family::Sndr {
        bail!(
            "posting family {} is required and cannot be dropped",
            family.name()
        );
    }
    let f = family as u8;
    let mut st = DropStats::default();
    loop {
        let rows = txn.scan_limit(Table::TailPostings, &[f], CHUNK, |k| k.first() == Some(&f))?;
        for (k, _) in &rows {
            txn.del(Table::TailPostings, k)?;
            st.postings += 1;
        }
        if rows.len() < CHUNK {
            break;
        }
    }
    let mut after: Option<Vec<u8>> = None;
    loop {
        let rows = txn.scan_chunk(Table::Undo, after.as_deref(), CHUNK, |_| true)?;
        for (k, v) in &rows {
            let mut ops = undo::decode(v).context("Undo row")?;
            let n = ops.len();
            ops.retain(|o| !matches!(o, UndoOp::Posting { family, .. } if *family == f));
            if ops.len() != n {
                st.undo_ops += (n - ops.len()) as u64;
                st.undo_rows += 1;
                txn.put(Table::Undo, k, &undo::encode(&ops))?;
            }
        }
        if rows.len() < CHUNK {
            break;
        }
        after = rows.last().map(|r| r.0.clone());
    }
    txn.del(Table::Meta, meta::SNDR_HOT.as_bytes())?;
    Ok(st)
}

/// `Meta sndr_hot = 1`: every hot tx carries its `sndr` posting (set by [`backfill`]).
pub fn hot_ready(txn: &impl Txn) -> Result<bool> {
    Ok(txn.meta_u64(meta::SNDR_HOT)? == Some(1))
}

#[cfg(test)]
mod tests {
    use kdx_types::{Hash32, TxKey};

    use super::op_slot;
    use crate::undo::UndoOp;

    #[test]
    fn op_slot_follows_the_txs_own_ops() {
        let (a, b) = (TxKey::new(9, 1, 1), TxKey::new(9, 1, 2));
        let post = |key, family| UndoOp::Posting {
            family,
            entity: 3,
            key,
        };
        let ops = vec![
            UndoOp::Header {
                chain: 1,
                height: 9,
                hash: Hash32([1; 32]),
            },
            UndoOp::Tx(a),
            UndoOp::Rk(Hash32([2; 32])),
            post(a, 1),
            post(a, 3),
            UndoOp::Tx(b),
            UndoOp::Counter {
                name: "txs".into(),
                delta: 2,
            },
        ];
        assert_eq!(op_slot(&ops, a), Some(5));
        assert_eq!(op_slot(&ops, b), Some(6));
        assert_eq!(op_slot(&ops, TxKey::new(9, 1, 3)), None);
    }
}
