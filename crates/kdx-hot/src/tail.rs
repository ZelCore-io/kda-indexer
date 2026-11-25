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
use crate::prices;
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
    /// D3 rows dropped at the tail's per-block-per-emitter cap.
    pub const D3_TAIL_DROPPED: &str = crate::balances::counters::D3_TAIL_DROPPED;
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

/// Write one block (header, txs, request keys, postings, counters; no `Defpacts` rows since
/// T-5, the `pact` postings carry the defpact history), its undo
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

    let mut d3 = D3Block::default();
    // `TokenSwaps` rows this block replaces, for the unwind (`prices.rs`).
    let mut price_undo = Vec::new();
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
        // Ruling R-P5P9-192: credits no TRANSFER names, marked like postings (no hourly lag).
        if marking && !x.d3.is_empty() {
            mark_d3(txn, dict, x, r.key, coin, &mut d3)?;
        }
        if !r.events.is_empty() {
            // Plan Task 11: NFT ledger events update `NftBalances` / mark `NftDirty`.
            let evs = nft::classify(&*dict, &r.events);
            if !evs.is_empty() {
                nft::apply_events(txn, &*dict, &evs, chain, height)?;
            }
            // `lastTokenPriceInKda`: the newest DEX swap per token.
            if let Some(s) = prices::swap_of_tx(&*dict, r) {
                prices::upsert(txn, chain, &s, Some(&mut price_undo))?;
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
        (counters::D3_TAIL_DROPPED.to_owned(), d3.dropped as i64),
        (
            balances::counters::D3_DISCOVER_TOO_LONG.to_owned(),
            d3.too_long as i64,
        ),
        (
            balances::counters::D3_MODREF_CAPPED.to_owned(),
            d3.modref_capped as i64,
        ),
    ] {
        if delta != 0 {
            txn.add_counter(&name, delta)?;
            ops.push(UndoOp::Counter { name, delta });
        }
    }

    if !d3.marked.is_empty() {
        // Kept beside the undo record, in Meta: which D3 rows a block marks depends on the
        // dictionary and token rows, which a reorg does not roll back, so the undo record
        // itself stays replay-identical.
        txn.set_meta(&balances::d3_undo_key(chain, height), &d3.marked)?;
    }
    if !price_undo.is_empty() {
        // Beside the undo record like the D3 marks: older binaries can still read the record.
        txn.set_meta(&prices::undo_key(chain, height), &price_undo)?;
    } else {
        // A log left at this height for a replaced block (an older binary unwound it without
        // reading the log) must not apply to this one (PR #31 review note 3).
        txn.del(Table::Meta, prices::undo_key(chain, height).as_bytes())?;
    }
    txn.put(Table::Undo, &bk, &undo::encode(&ops))?;
    set_checkpoint(txn, Stage::Tail, chain, height, br.hash)?;
    Ok(())
}

/// D3 rows one emitter may have marked per tail block (review sketch 5); more are dropped and
/// counted ([`counters::D3_TAIL_DROPPED`]), the block is applied regardless.
pub const D3_ROWS_PER_EMITTER_PER_BLOCK: u32 = 1024;
/// Discovered modules one tail block may queue for their `describe-module` check.
pub const D3_DISCOVER_PER_BLOCK: usize = 16;

/// Per-block state of the tail's D3 marks.
#[derive(Default)]
struct D3Block {
    per_emitter: std::collections::HashMap<String, u32>,
    discovered: usize,
    dropped: u64,
    /// Discoverable names over `MAX_MODULE_LEN` (re-review N1; a pure function of the txs, so
    /// the counter's undo op stays replay-identical).
    too_long: u64,
    /// Rule 2b rows dropped at the per-tx cap (a pure function of the txs, like `too_long`).
    modref_capped: u64,
    /// account u32 ‖ module u32 ‖ TxKey of each row marked (for the undo).
    marked: Vec<u8>,
}

/// The tracked fungible tokens as the tail sees them now: a `Tokens` row that is not a
/// not-fungible verdict (the seed pass's `tracked_tokens` rule).
fn tail_is_token(txn: &RwTxn<'_>, dict: &HotDict, m: &str) -> bool {
    use kdx_types::Dict;
    dict.mem()
        .find(kdx_types::DictKind::Module, m)
        .and_then(|id| balances::token_row(txn, id).ok().flatten())
        .is_some_and(|t| t.kind != balances::KIND_NOT_FUNGIBLE)
}

/// Mark the D3 rows of one tx (the same rows the seed pass marks for it:
/// `kdx_decode::receivers::d3_filter` of the candidates `extract` computed), with the tx's
/// height and no verified-row skip, under the per-tx caps of the filter and
/// [`D3_ROWS_PER_EMITTER_PER_BLOCK`]; queue its discovered modules. It interns nothing.
fn mark_d3(
    txn: &mut RwTxn<'_>,
    dict: &HotDict,
    x: &kdx_decode::TxIndex,
    key: TxKey,
    coin: Option<u32>,
    st: &mut D3Block,
) -> Result<()> {
    use kdx_types::{Dict, DictKind};
    let r = {
        let t = &*txn;
        kdx_decode::receivers::d3_filter(&x.d3, &|m| tail_is_token(t, dict, m))
    };
    for ((a, m), em) in r.all_pairs() {
        let used = st.per_emitter.entry(em.to_owned()).or_insert(0);
        if *used >= D3_ROWS_PER_EMITTER_PER_BLOCK {
            st.dropped += 1;
            continue;
        }
        *used += 1;
        // Names come from decode time (postings, `intern_d3`), so backfill and tail build the
        // same dictionary. A row whose name is new (e.g. an exec string) waits for the hourly
        // pass, which interns it.
        let module = match coin.filter(|_| m == "coin") {
            Some(c) => Some(c),
            None => dict.mem().find(DictKind::Module, m),
        };
        let (Some(module), Some(account)) = (module, dict.mem().find(DictKind::Account, a)) else {
            // Not counted: whether a name exists depends on the dictionary, which a reorg does
            // not roll back, so a counter would differ between replays.
            continue;
        };
        if balances::mark_row(txn, account, module, key.chain(), key.height(), coin)? {
            st.marked.extend_from_slice(&account.to_be_bytes());
            st.marked.extend_from_slice(&module.to_be_bytes());
            st.marked.extend_from_slice(&key.to_be_bytes());
        }
    }
    st.too_long += u64::from(r.discovered_too_long);
    st.modref_capped += u64::from(r.modref_capped);
    for m in &r.discovered {
        if st.discovered >= D3_DISCOVER_PER_BLOCK {
            break;
        }
        let tracked = dict
            .mem()
            .find(DictKind::Module, m)
            .is_some_and(|id| matches!(balances::token_row(&*txn, id), Ok(Some(_))));
        if !tracked {
            st.discovered += 1;
            balances::queue_discovered(txn, m, key.chain(), key.height())?;
        }
    }
    Ok(())
}

/// Ruling R-P5P9-192: an undone block's D3 rows are marked again, like undone postings.
fn undo_d3(txn: &mut RwTxn<'_>, chain: u8, height: u32, coin: Option<u32>) -> Result<()> {
    let k = balances::d3_undo_key(chain, height);
    let Some(v) = txn.meta(&k)? else {
        return Ok(());
    };
    if balances::marking() {
        for e in v.chunks_exact(16) {
            let account = u32::from_be_bytes(e[..4].try_into().expect("4 bytes"));
            let module = u32::from_be_bytes(e[4..8].try_into().expect("4 bytes"));
            let key = TxKey::from_be_bytes(e[8..].try_into().expect("8 bytes"));
            balances::mark_row(txn, account, module, key.chain(), key.height(), coin)?;
        }
    }
    txn.del(Table::Meta, k.as_bytes())?;
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
        // Undo records written before T-5 still name `Defpacts` rows; drop them on undo.
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
        undo_d3(txn, chain, h, coin)?;
        prices::undo_block(txn, chain, h)?;
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
                d3: Default::default(),
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
        // T-5: the defpact history is the `pact` posting; no `Defpacts` row is written.
        assert!(txn.get(Table::Defpacts, &pact.0).unwrap().is_none());
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
                d3: Default::default(),
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

    /// `lastTokenPriceInKda`: a tail block's swap updates `TokenSwaps`; unwinding the block
    /// restores the row it replaced, and the freezer-side log key is gone.
    #[test]
    fn swaps_apply_and_unwind() {
        use kdx_types::record::EventRec;
        use kdx_types::value;

        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        dict.intern(DictKind::Module, "coin");
        let block = |dict: &mut HotDict, height: u32, amount: f64| {
            let key = TxKey::new(height, 2, 1);
            let mref = |ns: Option<&str>, name: &str| serde_json::json!({"refSpec": [], "refName": {"namespace": ns, "name": name}});
            let params = serde_json::json!([
                "pool",
                "k:user",
                amount,
                mref(None, "coin"),
                2.0,
                mref(Some("kaddex"), "kdx")
            ]);
            let ev = EventRec {
                module: dict.intern(DictKind::Module, "kaddex.exchange"),
                name: dict.intern(DictKind::EventName, "kaddex.exchange.SWAP"),
                module_hash: 0,
                params: value::to_vec(&params, &*dict),
            };
            let data = serde_json::json!([
                {"amount": amount, "token": "coin"},
                {"amount": 2.0, "token": "kaddex.kdx"}
            ]);
            IndexedBlock {
                block: BlockRecord {
                    hash: Hash32([height as u8; 32]),
                    chain: 2,
                    height: u64::from(height),
                    ..Default::default()
                },
                txs: vec![TxRecord {
                    key,
                    status_ok: true,
                    cmd: Some(
                        format!(
                            r#"{{"meta":{{"creationTime":{}}}}}"#,
                            1_700_000_000 + height
                        )
                        .into_bytes(),
                    ),
                    result: value::to_vec(&data, &*dict),
                    events: vec![ev],
                    ..Default::default()
                }],
                idx: vec![TxIndex {
                    key,
                    postings: vec![],
                    pact_id: None,
                    code_hash: None,
                    d3: Default::default(),
                }],
                cmd_reencoded: 0,
                unindexed_accounts: 0,
            }
        };
        let b1 = block(&mut dict, 400, 1.0);
        super::apply(&mut txn, &b1, &mut dict).unwrap();
        let b2 = block(&mut dict, 401, 3.0);
        super::apply(&mut txn, &b2, &mut dict).unwrap();
        let price = |txn: &crate::RwTxn<'_>| {
            crate::prices::lookup(txn, "kaddex.kdx")
                .unwrap()
                .and_then(|r| crate::prices::price_text(&r))
        };
        assert_eq!(price(&txn).as_deref(), Some("1.500000000000"));
        assert!(txn
            .meta(&crate::prices::undo_key(2, 401))
            .unwrap()
            .is_some());
        assert_eq!(super::unwind_to(&mut txn, 2, 400).unwrap(), 1);
        assert_eq!(price(&txn).as_deref(), Some("0.500000000000"));
        assert!(txn
            .meta(&crate::prices::undo_key(2, 401))
            .unwrap()
            .is_none());
        assert_eq!(super::unwind_to(&mut txn, 2, 399).unwrap(), 1);
        assert_eq!(price(&txn), None);
        txn.commit().unwrap();
    }

    /// FIX-2: a `sndr` posting (family 6) marks no balance row, on apply or on undo; the
    /// sender's balance is covered by its `acct` posting.
    #[test]
    fn sndr_postings_mark_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        dict.intern(DictKind::Module, "coin");
        let alice = dict.intern(DictKind::Account, "alice");
        let key = TxKey::new(300, 5, 1);
        let ib = IndexedBlock {
            block: BlockRecord {
                hash: Hash32([3u8; 32]),
                chain: 5,
                height: 300,
                ..Default::default()
            },
            txs: vec![TxRecord {
                key,
                ..Default::default()
            }],
            idx: vec![TxIndex {
                key,
                postings: vec![(Family::Sndr, alice as u64)],
                pact_id: None,
                code_hash: None,
                d3: Default::default(),
            }],
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        };
        assert!(crate::balances::marking());
        super::apply(&mut txn, &ib, &mut dict).unwrap();
        assert_eq!(txn.count(Table::TailPostings).unwrap(), 1);
        assert_eq!(txn.count(Table::BalanceDirty).unwrap(), 0);
        assert_eq!(txn.count(Table::Tokens).unwrap(), 0);
        // Direct calls, both directions, with a coin module and an entity that would be a
        // valid AccountId or xfer entity.
        let coin = crate::balances::coin_module(&txn).unwrap();
        assert!(coin.is_some());
        for e in [alice as u64, xfer_entity(alice, coin.unwrap())] {
            for note in [true, false] {
                crate::balances::mark_posting(&mut txn, Family::Sndr as u8, e, key, coin, note)
                    .unwrap();
            }
        }
        assert_eq!(super::unwind_to(&mut txn, 5, 299).unwrap(), 1);
        assert_eq!(txn.count(Table::TailPostings).unwrap(), 0);
        assert_eq!(txn.count(Table::BalanceDirty).unwrap(), 0);
        assert_eq!(txn.count(Table::Tokens).unwrap(), 0);
        txn.commit().unwrap();
    }
}

#[cfg(test)]
mod legacy_defpact_undo_tests {
    use kdx_types::{Hash32, TxKey};

    use super::{defpact_bytes, defpact_list, unwind_to};
    use crate::tables::block_key;
    use crate::undo::{self, UndoOp};
    use crate::{Geometry, HotStore, Table, Txn};

    /// m12 (T-5): an undo record written before T-5 names `Defpacts` rows; unwinding its
    /// block drops that key from each row, and a row left without keys is deleted.
    #[test]
    fn unwinding_a_pre_t5_defpact_step_drops_the_rows_it_names() {
        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        let (two, one) = (Hash32([7u8; 32]), Hash32([9u8; 32]));
        let (k1, k2, k3) = (
            TxKey::new(5, 0, 1),
            TxKey::new(3, 0, 2),
            TxKey::new(5, 0, 2),
        );
        let mut txn = hot.rw().unwrap();
        txn.put(Table::Defpacts, &two.0, &defpact_bytes(&[k2, k1]))
            .unwrap();
        txn.put(Table::Defpacts, &one.0, &defpact_bytes(&[k3]))
            .unwrap();
        let ops = [
            UndoOp::DefpactStep { pact: two, key: k1 },
            UndoOp::DefpactStep { pact: one, key: k3 },
        ];
        txn.put(Table::Undo, &block_key(0, 5), &undo::encode(&ops))
            .unwrap();
        assert_eq!(unwind_to(&mut txn, 0, 4).unwrap(), 1);
        let left = txn.get(Table::Defpacts, &two.0).unwrap().expect("row kept");
        assert_eq!(defpact_list(&left), vec![k2]);
        assert!(txn.get(Table::Defpacts, &one.0).unwrap().is_none());
        assert!(txn.get(Table::Undo, &block_key(0, 5)).unwrap().is_none());
        txn.commit().unwrap();
    }
}
