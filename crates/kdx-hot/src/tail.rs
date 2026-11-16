//! Applying tail blocks to the hot store and unwinding them (spec §4.3, §4.5).

use anyhow::{bail, Context, Result};
use kdx_decode::IndexedBlock;
use kdx_types::record::{decode_block, encode_block, encode_tx};
use kdx_types::TxKey;

use crate::balances;
use crate::checkpoint::{clear_checkpoint, set_checkpoint, Stage};
use crate::dict::HotDict;
use crate::env::{RwTxn, Txn};
use crate::nft;
use crate::tables::{block_key, parse_block_key, posting_key, Table};
use crate::undo::{self, UndoOp};

/// Counter names maintained by `apply`.
pub mod counters {
    pub const BLOCKS: &str = "blocks";
    /// All tx records, coinbase included.
    pub const TXS: &str = "txs";
    pub const USER_TXS: &str = "user_txs";
    pub const CMD_REENCODED: &str = "cmd_reencoded";
    /// Account strings longer than `MAX_ACCOUNT_LEN`, left unindexed.
    pub const UNINDEXED_ACCOUNTS: &str = "unindexed_accounts";
    /// Per-chain tx records: `txs.cNN`.
    pub fn txs_chain(chain: u8) -> String {
        format!("txs.c{chain:02}")
    }
}

fn defpact_list(v: &[u8]) -> Vec<TxKey> {
    v.chunks_exact(8)
        .map(|c| TxKey::from_be_bytes(c.try_into().expect("8-byte chunk")))
        .collect()
}

fn defpact_bytes(keys: &[TxKey]) -> Vec<u8> {
    keys.iter().flat_map(|k| k.to_be_bytes()).collect()
}

/// Write one block (header, txs, request keys, postings, defpact steps, counters), its undo
/// record, the queued dictionary names, the balance dirty marks of its `xfer`/`acct` postings
/// (B-3), the NFT ledger-event effects (`nft.rs`) and the `Tail` checkpoint, all in `txn`.
/// The block must not already be stored at (chain, height).
pub fn apply(txn: &mut RwTxn<'_>, b: &IndexedBlock, dict: &mut HotDict) -> Result<()> {
    let br = &b.block;
    let chain = br.chain;
    let height = u32::try_from(br.height).context("height does not fit u32")?;
    let bk = block_key(chain, height);
    if txn.get(Table::Headers, &bk)?.is_some() {
        bail!("block chain {chain} height {height} is already in the hot store");
    }
    let mut ops = Vec::new();
    dict.persist(txn)?;
    let coin = balances::coin_module(txn)?;
    // M-11: marks only while balances are on; the watermark shows blocks applied without.
    let marking = balances::marking();
    if marking {
        balances::note_marked(txn, chain, height)?;
    }

    let mut buf = Vec::new();
    encode_block(br, &mut buf);
    txn.put(Table::Headers, &bk, &buf)?;
    txn.put(Table::HeaderByHash, &br.hash.0, &bk)?;
    ops.push(UndoOp::Header {
        chain,
        height,
        hash: br.hash,
    });

    for (r, x) in b.txs.iter().zip(&b.idx) {
        let k = r.key.to_be_bytes();
        buf.clear();
        encode_tx(r, &mut buf);
        txn.put(Table::TailTxs, &k, &buf)?;
        ops.push(UndoOp::Tx(r.key));
        if let Some(rk) = &r.request_key {
            // A request key already present (same tx on another fork, not yet unwound) is
            // left to its owner; only rows this block created are undone.
            if txn.insert(Table::TailRk, &rk.0, &k)? {
                ops.push(UndoOp::Rk(*rk));
            }
        }
        for &(f, e) in &x.postings {
            if txn.insert(Table::TailPostings, &posting_key(f as u8, e, r.key), &[])? {
                ops.push(UndoOp::Posting {
                    family: f as u8,
                    entity: e,
                    key: r.key,
                });
            }
            // B-3: balances this block may change are re-read from the node.
            if marking {
                balances::mark_posting(txn, f as u8, e, r.key, coin, true)?;
            }
        }
        if !r.events.is_empty() {
            // Plan Task 11: NFT ledger events update `NftBalances` / mark `NftDirty`.
            let evs = nft::classify(&*dict, &r.events);
            if !evs.is_empty() {
                nft::apply_events(txn, &*dict, &evs, chain, height)?;
            }
        }
        if let Some(p) = &x.pact_id {
            let mut keys = txn
                .get(Table::Defpacts, &p.0)?
                .map(|v| defpact_list(&v))
                .unwrap_or_default();
            if let Err(pos) = keys.binary_search(&r.key) {
                keys.insert(pos, r.key);
                txn.put(Table::Defpacts, &p.0, &defpact_bytes(&keys))?;
                ops.push(UndoOp::DefpactStep {
                    pact: *p,
                    key: r.key,
                });
            }
        }
    }

    let n = b.txs.len() as i64;
    for (name, delta) in [
        (counters::BLOCKS.to_owned(), 1),
        (counters::TXS.to_owned(), n),
        (counters::USER_TXS.to_owned(), n - 1),
        (counters::CMD_REENCODED.to_owned(), b.cmd_reencoded as i64),
        (
            counters::UNINDEXED_ACCOUNTS.to_owned(),
            b.unindexed_accounts as i64,
        ),
        (counters::txs_chain(chain), n),
    ] {
        if delta != 0 {
            txn.add_counter(&name, delta)?;
            ops.push(UndoOp::Counter { name, delta });
        }
    }

    txn.put(Table::Undo, &bk, &undo::encode(&ops))?;
    set_checkpoint(txn, Stage::Tail, chain, height, br.hash)?;
    Ok(())
}

/// `coin`: the `coin` ModuleId for the balance dirty marks of undone `acct` postings (B-3).
fn undo_op(txn: &mut RwTxn<'_>, op: &UndoOp, coin: Option<u32>) -> Result<()> {
    match op {
        UndoOp::Header {
            chain,
            height,
            hash,
        } => {
            txn.del(Table::Headers, &block_key(*chain, *height))?;
            txn.del(Table::HeaderByHash, &hash.0)?;
        }
        UndoOp::Tx(k) => {
            // Plan Task 11: rows this tx's NFT events touched are re-read from the node.
            nft::mark_tx_dirty(txn, *k)?;
            txn.del(Table::TailTxs, &k.to_be_bytes())?;
        }
        UndoOp::Rk(h) => {
            txn.del(Table::TailRk, &h.0)?;
        }
        UndoOp::Posting {
            family,
            entity,
            key,
        } => {
            txn.del(Table::TailPostings, &posting_key(*family, *entity, *key))?;
            // B-3: no balance undo ops; the row is marked dirty and re-read.
            if balances::marking() {
                balances::mark_posting(txn, *family, *entity, *key, coin, false)?;
            }
        }
        UndoOp::DefpactStep { pact, key } => {
            if let Some(v) = txn.get(Table::Defpacts, &pact.0)? {
                let mut keys = defpact_list(&v);
                keys.retain(|k| k != key);
                if keys.is_empty() {
                    txn.del(Table::Defpacts, &pact.0)?;
                } else {
                    txn.put(Table::Defpacts, &pact.0, &defpact_bytes(&keys))?;
                }
            }
        }
        UndoOp::Counter { name, delta } => {
            txn.add_counter(name, delta.checked_neg().unwrap_or(i64::MAX))?;
        }
    }
    Ok(())
}

/// Undo every hot block of `chain` above `height`, newest first. The `Tail` checkpoint moves
/// to (height, hash) when that header is in the hot store, otherwise it is cleared and the
/// caller sets it (for example at the frozen frontier). Returns the number of blocks undone.
pub fn unwind_to(txn: &mut RwTxn<'_>, chain: u8, height: u32) -> Result<u32> {
    let from = match height.checked_add(1) {
        Some(h) => block_key(chain, h),
        None => return Ok(0),
    };
    let rows = txn.scan(Table::Undo, &from, |k| k.first() == Some(&chain))?;
    let coin = balances::coin_module(txn)?;
    let mut n = 0u32;
    for (k, v) in rows.iter().rev() {
        let (_, h) = parse_block_key(k).context("bad Undo key")?;
        let ops = undo::decode(v).with_context(|| format!("Undo chain {chain} height {h}"))?;
        for op in ops.iter().rev() {
            undo_op(txn, op, coin)?;
        }
        txn.del(Table::Undo, k)?;
        n += 1;
    }
    match txn.get(Table::Headers, &block_key(chain, height))? {
        Some(v) => {
            let br = decode_block(&v).context("hot header record")?;
            set_checkpoint(txn, Stage::Tail, chain, height, br.hash)?;
        }
        None => {
            clear_checkpoint(txn, Stage::Tail, chain)?;
        }
    }
    Ok(n)
}

#[cfg(test)]
mod pact_family_tests {
    use kdx_decode::index::pact_entity;
    use kdx_decode::{Family, IndexedBlock, TxIndex};
    use kdx_types::record::{BlockRecord, TxRecord};
    use kdx_types::{Hash32, TxKey};

    use crate::tables::{posting_key, Table};
    use crate::{Geometry, HotDict, HotStore, Txn};

    /// Format v2: a `pact` posting (family 5) is written by `apply` and removed by undo.
    #[test]
    fn pact_posting_apply_and_undo() {
        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        let pact = Hash32([7u8; 32]);
        let key = TxKey::new(100, 3, 1);
        let block = BlockRecord {
            hash: Hash32([1u8; 32]),
            chain: 3,
            height: 100,
            ..Default::default()
        };
        let tx = TxRecord {
            key,
            request_key: Some(pact),
            ..Default::default()
        };
        let ent = pact_entity(&pact);
        let ib = IndexedBlock {
            block,
            txs: vec![tx],
            idx: vec![TxIndex {
                key,
                postings: vec![(Family::Pact, ent)],
                pact_id: Some(pact),
                code_hash: None,
            }],
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        };
        let pk = posting_key(Family::Pact as u8, ent, key);
        assert_eq!(pk[0], 5);
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        super::apply(&mut txn, &ib, &mut dict).unwrap();
        assert!(txn.get(Table::TailPostings, &pk).unwrap().is_some());
        assert_eq!(super::unwind_to(&mut txn, 3, 99).unwrap(), 1);
        assert!(txn.get(Table::TailPostings, &pk).unwrap().is_none());
        assert!(txn.get(Table::Defpacts, &pact.0).unwrap().is_none());
        txn.commit().unwrap();
    }
}

#[cfg(test)]
mod balance_mark_tests {
    use kdx_decode::index::xfer_entity;
    use kdx_decode::{Family, IndexedBlock, TxIndex};
    use kdx_types::record::{BlockRecord, TxRecord};
    use kdx_types::{DictKind, DictMut, Hash32, TxKey};

    use crate::balances::{bal_key, dirty_height, TokenRow};
    use crate::{Geometry, HotDict, HotStore, Table, Txn};

    /// B-3: apply marks `xfer` and `acct` postings dirty; undo marks them again.
    #[test]
    fn apply_and_undo_mark_dirty() {
        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let coin = dict.intern(DictKind::Module, "coin");
        let tok = dict.intern(DictKind::Module, "free.tok");
        let alice = dict.intern(DictKind::Account, "alice");
        let key = TxKey::new(200, 4, 1);
        let ib = IndexedBlock {
            block: BlockRecord {
                hash: Hash32([2u8; 32]),
                chain: 4,
                height: 200,
                ..Default::default()
            },
            txs: vec![TxRecord {
                key,
                ..Default::default()
            }],
            idx: vec![TxIndex {
                key,
                postings: vec![
                    (Family::Xfer, xfer_entity(alice, tok)),
                    (Family::Acct, alice as u64),
                ],
                pact_id: None,
                code_hash: None,
            }],
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        };
        super::apply(&mut txn, &ib, &mut dict).unwrap();
        for m in [tok, coin] {
            let v = txn.get(Table::BalanceDirty, &bal_key(alice, m, 4)).unwrap();
            assert_eq!(v.as_deref().and_then(dirty_height), Some(200));
        }
        let t = txn.get(Table::Tokens, &tok.to_be_bytes()).unwrap();
        assert_eq!(
            t.as_deref()
                .and_then(TokenRow::decode)
                .map(|t| t.first_height),
            Some(200)
        );
        // The reconciler cleared the marks; an undo marks the rows again.
        txn.del(Table::BalanceDirty, &bal_key(alice, tok, 4))
            .unwrap();
        txn.del(Table::BalanceDirty, &bal_key(alice, coin, 4))
            .unwrap();
        assert_eq!(super::unwind_to(&mut txn, 4, 199).unwrap(), 1);
        assert_eq!(txn.count(Table::BalanceDirty).unwrap(), 2);
        txn.commit().unwrap();
    }
}
