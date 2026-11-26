//! A module's holders (`balance(module:)` without `accountName`): every `(account, chain)`
//! stock's `Balances` table would hold for the module, at any balance, ordered by the first
//! event that named it.
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | `Holders` | ModuleId u32, chain u8, first TxKey u64, AccountId u32 | empty |
//! | `HolderFirst` | AccountId u32, ModuleId u32, chain u8 (the `Balances` key) | first TxKey u64 |
//! | `HolderByTx` | first TxKey u64, ModuleId u32, AccountId u32 | empty |
//!
//! - The set is stock's `Balances` set for a module (`payload.ts`): the from and to sides of
//!   every event named exactly `TRANSFER` with three params (fungible-v2), the marmalade
//!   ledgers' `MINT` account and `TRANSFER` receiver, and both sides of a marmalade `RECONCILE`
//!   (`kdx_decode::build::holder_sides`, carried as `TxIndex::holders`). Sides not empty and
//!   at most 1024 bytes. One difference, on purpose: coinbase miners are holders here (stock
//!   adds no `Balances` row on its coinbase path; a miner holds coin).
//! - Order: stock serves its own insert id, newest first. That id is the order stock's ingest
//!   happened to insert, which cannot be reproduced; the closest stable order is the first
//!   event that named the row (`first`, the TxKey: height, chain, position), newest first,
//!   then AccountId. `HolderFirst` holds the same `first` per row, and `HolderByTx` lists the
//!   rows a tx is the first of, so an entry is found and removed exactly.
//! - Kept from block 0 by every writer, in the write txn of the change: `tail::apply` notes each
//!   side ([`touch`]), the undo of a tx removes the rows it is the first of ([`untouch_tx`]);
//!   backfill notes each range's sides in the range's MDBX commit ([`add_tally`]). [`touch`]
//!   keeps the smallest `first`, so a range built twice, or in any order, gives the same table.
//! - Storage per tx: at most one row in each table per new side, and at most three sides per
//!   event (`holder_sides`), so a holder flood costs a bounded amount per event, paid in gas.
//! - Readiness (the PR #48 and #51 patterns, own keys): `Meta holders_ready`; per-chain stamps
//!   `holders_at@<chain>` = the chain's `Tail` checkpoint, written by apply and unwind; and
//!   `Meta holders_txnum` ([`TXNUM_AT`]) = `next_txnum`, written in the txn of every move of
//!   `next_txnum` (backfill range, freeze) and by `reconcile-holders` and a new store, so a
//!   range an older build backfilled is caught although no tip moved. A new store sets the
//!   marker (every writer keeps the table from block 0); an existing store gets it from
//!   `kda-indexer reconcile-holders`. Readers use the table only while [`ready`].

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use kdx_types::TxKey;

use crate::balances::{bal_key, parse_bal_key};
use crate::checkpoint::Stage;
use crate::env::{RwTxn, Txn};
use crate::tables::Table;

/// Meta key of the readiness marker.
pub const READY: &str = "holders_ready";
/// Meta key prefix of the per-chain tip the table follows (`holders_at@<chain>`).
pub const AT_PREFIX: &str = "holders_at@";
/// Meta key of the TxNum frontier the table follows (u64; review I-1 of PR #54).
pub const TXNUM_AT: &str = "holders_txnum";

/// Record that the table follows the store's current `next_txnum`, in the write txn that
/// moved it (a backfill range with its first transfers, a freeze) or set the marker.
pub fn stamp_txnum(txn: &mut RwTxn<'_>) -> Result<()> {
    crate::rowcount::stamp_txnum_at(txn, TXNUM_AT)
}

pub fn at_key(chain: u8) -> String {
    format!("{AT_PREFIX}{chain}")
}

/// `Holders` key.
pub fn hold_key(module: u32, chain: u8, first: TxKey, account: u32) -> [u8; 17] {
    let mut k = [0u8; 17];
    k[..4].copy_from_slice(&module.to_be_bytes());
    k[4] = chain;
    k[5..13].copy_from_slice(&first.to_be_bytes());
    k[13..].copy_from_slice(&account.to_be_bytes());
    k
}

/// `HolderByTx` key.
pub fn by_tx_key(first: TxKey, module: u32, account: u32) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&first.to_be_bytes());
    k[8..12].copy_from_slice(&module.to_be_bytes());
    k[12..].copy_from_slice(&account.to_be_bytes());
    k
}

/// One holder entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Holder {
    pub module: u32,
    pub chain: u8,
    pub first: TxKey,
    pub account: u32,
}

impl Holder {
    /// The serving order key: newest first transfer first, then the larger AccountId.
    pub fn order(&self) -> (u64, u32) {
        (self.first.0, self.account)
    }
}

pub fn parse_hold_key(k: &[u8]) -> Option<Holder> {
    if k.len() != 17 {
        return None;
    }
    Some(Holder {
        module: u32::from_be_bytes(k[..4].try_into().ok()?),
        chain: k[4],
        first: TxKey::from_be_bytes(k[5..13].try_into().ok()?),
        account: u32::from_be_bytes(k[13..].try_into().ok()?),
    })
}

/// The first transfer stored for `(account, module, chain)`.
pub fn first_of(txn: &impl Txn, account: u32, module: u32, chain: u8) -> Result<Option<TxKey>> {
    Ok(txn
        .get(Table::HolderFirst, &bal_key(account, module, chain))?
        .and_then(|v| Some(TxKey::from_be_bytes(v.as_slice().try_into().ok()?))))
}

/// Note a holder side `(account, module)` of the tx at `key`: the row is listed from its
/// smallest `first`. Returns whether the table changed.
pub fn touch(txn: &mut RwTxn<'_>, account: u32, module: u32, key: TxKey) -> Result<bool> {
    let chain = key.chain();
    if let Some(old) = first_of(txn, account, module, chain)? {
        if old <= key {
            return Ok(false);
        }
        // Only out-of-order writers (backfill ranges, the rebuild walk) get here.
        txn.del(Table::Holders, &hold_key(module, chain, old, account))?;
        txn.del(Table::HolderByTx, &by_tx_key(old, module, account))?;
    }
    txn.put(
        Table::HolderFirst,
        &bal_key(account, module, chain),
        &key.to_be_bytes(),
    )?;
    txn.put(Table::Holders, &hold_key(module, chain, key, account), &[])?;
    txn.put(Table::HolderByTx, &by_tx_key(key, module, account), &[])?;
    Ok(true)
}

/// Undo of the tx at `key`: every row it is the first of goes (an unwind removes every later
/// tx of the chain too, so no later one is left to keep it; an earlier first, from backfill
/// or an earlier block, is not listed under `key` and stays). Reads only the rows `key`
/// created. Returns how many it removed.
pub fn untouch_tx(txn: &mut RwTxn<'_>, key: TxKey) -> Result<usize> {
    let p = key.to_be_bytes();
    let rows = txn.scan(Table::HolderByTx, &p, |k| k.starts_with(&p))?;
    for (k, _) in &rows {
        let (Some(m), Some(a)) = (
            k.get(8..12)
                .and_then(|b| b.try_into().ok())
                .map(u32::from_be_bytes),
            k.get(12..16)
                .and_then(|b| b.try_into().ok())
                .map(u32::from_be_bytes),
        ) else {
            anyhow::bail!("bad HolderByTx key");
        };
        txn.del(Table::HolderByTx, k)?;
        if first_of(txn, a, m, key.chain())? == Some(key) {
            txn.del(Table::HolderFirst, &bal_key(a, m, key.chain()))?;
            txn.del(Table::Holders, &hold_key(m, key.chain(), key, a))?;
        }
    }
    Ok(rows.len())
}

/// First transfers of a batch of records (a backfill range or a rebuild walk): the smallest
/// TxKey per `(account, module, chain)`, keyed like `Balances`.
#[derive(Debug, Default, Clone)]
pub struct Tally {
    first: HashMap<[u8; 9], TxKey>,
}

impl Tally {
    pub fn add(&mut self, account: u32, module: u32, key: TxKey) {
        let e = self
            .first
            .entry(bal_key(account, module, key.chain()))
            .or_insert(key);
        if key < *e {
            *e = key;
        }
    }
    /// The sides of one tx (`TxIndex::holders`).
    pub fn add_sides(&mut self, sides: &[(u32, u32)], key: TxKey) {
        for &(a, m) in sides {
            self.add(a, m, key);
        }
    }
    pub fn merge(&mut self, o: Tally) {
        for (k, v) in o.first {
            let e = self.first.entry(k).or_insert(v);
            if v < *e {
                *e = v;
            }
        }
    }
    pub fn len(&self) -> usize {
        self.first.len()
    }
    pub fn is_empty(&self) -> bool {
        self.first.is_empty()
    }
    /// The entries as `Holders` rows, sorted by key.
    pub fn into_sorted(self) -> Vec<Holder> {
        let mut v: Vec<Holder> = self
            .first
            .into_iter()
            .filter_map(|(k, first)| {
                let (account, module, chain) = parse_bal_key(&k)?;
                Some(Holder {
                    module,
                    chain,
                    first,
                    account,
                })
            })
            .collect();
        v.sort_unstable_by_key(|h| hold_key(h.module, h.chain, h.first, h.account));
        v
    }
}

/// [`touch`] every entry of `t` (backfill: in the range's MDBX commit).
pub fn add_tally(txn: &mut RwTxn<'_>, t: &Tally) -> Result<()> {
    for (k, &first) in &t.first {
        if let Some((a, m, _)) = parse_bal_key(k) {
            touch(txn, a, m, first)?;
        }
    }
    Ok(())
}

/// Every stored entry (tests and `reconcile-holders --check`).
pub fn read_all(txn: &impl Txn) -> Result<Vec<Holder>> {
    let mut out = Vec::new();
    txn.walk_keys(Table::Holders, &[], |k| {
        if let Some(h) = parse_hold_key(k) {
            out.push(h);
        }
        true
    })?;
    Ok(out)
}

/// Remove every entry (the start of a rebuild).
pub fn clear_table(txn: &mut RwTxn<'_>) -> Result<()> {
    txn.clear(Table::Holders)?;
    txn.clear(Table::HolderFirst)?;
    txn.clear(Table::HolderByTx)
}

/// Which way a page reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    /// Newest first transfer first (stock's `first`/`after`).
    NewestFirst,
    /// Oldest first (stock's `last`/`before` before it reverses the page).
    OldestFirst,
}

/// Up to `limit` holders of `module` on `chains`, in `order`, strictly past `cursor` (the
/// [`Holder::order`] of the previous page's edge). One bounded seek per chain: at most
/// `limit` holders read per chain, so a page costs `chains × limit` reads whatever the module's
/// size. A damaged key is skipped and not counted (secpass B M-16), at most `limit` of them per
/// chain, so a damaged table cannot end a page early nor make it read more than `2 × limit`
/// keys per chain. `check` runs before each chain and every 64 keys (the request deadline).
pub fn page(
    txn: &impl Txn,
    module: u32,
    chains: &[u8],
    order: Order,
    cursor: Option<(u64, u32)>,
    limit: usize,
    check: &mut dyn FnMut() -> Result<()>,
) -> Result<Vec<Holder>> {
    let mut all = Vec::new();
    for &c in chains {
        check()?;
        let mut prefix = [0u8; 5];
        prefix[..4].copy_from_slice(&module.to_be_bytes());
        prefix[4] = c;
        let at = cursor.map(|(f, a)| hold_key(module, c, TxKey(f), a));
        let mut n = 0usize;
        let mut skipped = 0usize;
        match order {
            Order::NewestFirst => {
                // Keys below the cursor's (exclusive), newest first.
                let upper = at.map(|k| k.to_vec()).unwrap_or_default();
                for r in txn.scan_rev_prefix(Table::Holders, &prefix, &upper)? {
                    if n >= limit || skipped > limit {
                        break;
                    }
                    let (k, _) = r?;
                    match parse_hold_key(&k) {
                        Some(h) => {
                            all.push(h);
                            n += 1;
                        }
                        None => skipped += 1,
                    }
                    if (n + skipped).is_multiple_of(64) {
                        check()?;
                    }
                }
            }
            Order::OldestFirst => {
                // Keys strictly above the cursor's: the smallest is the cursor key ‖ 0x00.
                let from = match at {
                    Some(k) => {
                        let mut v = k.to_vec();
                        v.push(0);
                        v
                    }
                    None => prefix.to_vec(),
                };
                let mut err = None;
                let rows = txn.scan(Table::Holders, &from, |k| {
                    if n >= limit || skipped > limit || !k.starts_with(&prefix) {
                        return false;
                    }
                    if parse_hold_key(k).is_some() {
                        n += 1;
                    } else {
                        skipped += 1;
                    }
                    if (n + skipped).is_multiple_of(64) {
                        if let Err(e) = check() {
                            err = Some(e);
                            return false;
                        }
                    }
                    true
                })?;
                if let Some(e) = err {
                    return Err(e);
                }
                all.extend(rows.iter().filter_map(|(k, _)| parse_hold_key(k)));
            }
        }
    }
    match order {
        Order::NewestFirst => all.sort_unstable_by_key(|h| std::cmp::Reverse(h.order())),
        Order::OldestFirst => all.sort_unstable_by_key(Holder::order),
    }
    all.truncate(limit);
    Ok(all)
}

// --- readiness (PR #48 pattern) ---

/// Record that the table follows `chain` up to its current `Tail` checkpoint.
pub fn stamp(txn: &mut RwTxn<'_>, chain: u8) -> Result<()> {
    match txn.get(Table::StageCheckpoint, &[Stage::Tail as u8, chain])? {
        Some(v) => txn.set_meta(&at_key(chain), &v),
        None => txn.del(Table::Meta, at_key(chain).as_bytes()).map(drop),
    }
}

/// [`stamp`] every chain: the stamps become exactly the current tips (`reconcile-holders`).
pub fn stamp_all(txn: &mut RwTxn<'_>) -> Result<()> {
    for c in stamps(txn)?.into_keys() {
        txn.del(Table::Meta, at_key(c).as_bytes())?;
    }
    for (c, v) in crate::rowcount::tips(txn)? {
        txn.set_meta(&at_key(c), &v)?;
    }
    Ok(())
}

fn stamps(txn: &impl Txn) -> Result<BTreeMap<u8, Vec<u8>>> {
    let p = AT_PREFIX.as_bytes();
    let mut out = BTreeMap::new();
    for (k, v) in txn.scan(Table::Meta, p, |k| k.starts_with(p))? {
        let c = std::str::from_utf8(&k[p.len()..])
            .ok()
            .and_then(|s| s.parse::<u8>().ok())
            .with_context(|| format!("bad Meta key {}", String::from_utf8_lossy(&k)))?;
        out.insert(c, v);
    }
    Ok(out)
}

/// Why the table does not follow the hot tips (None: every stamp equals its chain's tip).
pub fn tip_mismatch(txn: &impl Txn) -> Result<Option<String>> {
    let (tips, stamps) = (crate::rowcount::tips(txn)?, stamps(txn)?);
    if tips == stamps {
        return Ok(None);
    }
    let c = tips
        .keys()
        .chain(stamps.keys())
        .find(|c| tips.get(c) != stamps.get(c))
        .copied()
        .unwrap_or_default();
    Ok(Some(format!(
        "chain {c}: holders follow another tip than the tail's (blocks applied or unwound by a build without the holders table?)"
    )))
}

pub fn marker_set(txn: &impl Txn) -> Result<bool> {
    Ok(txn.meta(READY)?.is_some())
}

/// Why the table cannot be trusted (None: it can, as far as the stamps show): a chain's
/// `holders_at@` stamp differs from its `Tail` checkpoint ([`tip_mismatch`]), or
/// `holders_txnum` differs from `next_txnum` (a range backfilled or frozen by a build
/// without the stamp).
pub fn untrusted(txn: &impl Txn) -> Result<Option<String>> {
    if let Some(why) = tip_mismatch(txn)? {
        return Ok(Some(why));
    }
    crate::rowcount::txnum_mismatch(txn, TXNUM_AT)
}

/// Whether the table may answer: the tables exist, the marker is set, every chain's stamp
/// equals its hot tip and `holders_txnum` equals `next_txnum`, read in `txn`'s snapshot.
pub fn ready(txn: &impl Txn) -> Result<bool> {
    Ok(txn.has_table(Table::Holders)
        && txn.has_table(Table::HolderFirst)
        && txn.has_table(Table::HolderByTx)
        && marker_set(txn)?
        && untrusted(txn)?.is_none())
}

/// Clear a set marker whose table does not follow the store (startup, before every apply
/// and unwind, before every move of `next_txnum`). Returns the reason when it cleared it.
pub fn guard(txn: &mut RwTxn<'_>) -> Result<Option<String>> {
    if !marker_set(txn)? {
        return Ok(None);
    }
    let Some(why) = untrusted(txn)? else {
        return Ok(None);
    };
    tracing::warn!(reason = %why, "holders: the table does not follow the store");
    clear_ready(txn)?;
    Ok(Some(why))
}

/// Set the marker (`next_txnum`: the TxNum frontier the table was built at; 0 on a new store).
pub fn set_ready(txn: &mut RwTxn<'_>, next_txnum: u64) -> Result<()> {
    txn.set_meta_u64(READY, next_txnum)
}

/// Clear the marker: `balance(module:)` answers `NOT_AVAILABLE` until the next
/// `reconcile-holders`. Returns whether it was set.
pub fn clear_ready(txn: &mut RwTxn<'_>) -> Result<bool> {
    let was = txn.meta(READY)?.is_some();
    if was {
        txn.del(Table::Meta, READY.as_bytes())?;
        tracing::warn!(
            "holders: readiness marker cleared; module holder lists are not served until `kda-indexer reconcile-holders`"
        );
    }
    Ok(was)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use kdx_decode::index::xfer_entity;
    use kdx_decode::{Family, IndexedBlock, TxIndex};
    use kdx_types::record::{BlockRecord, TxRecord};
    use kdx_types::{Hash32, TxKey};

    use super::{page, read_all, ready, Holder, Order};
    use crate::{Geometry, HotDict, HotStore, Table, Txn};

    /// A small deterministic generator (no extra dev-dependency).
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// One block: per tx, its `(account, module)` transfer sides.
    type Blk = Vec<Vec<(u32, u32)>>;

    fn block(chain: u8, height: u32, txs: &Blk, fork: u8) -> IndexedBlock {
        let mut hash = [fork; 32];
        hash[0] = chain;
        hash[1..5].copy_from_slice(&height.to_be_bytes());
        let mut recs = Vec::new();
        let mut idx = Vec::new();
        for (i, sides) in txs.iter().enumerate() {
            let key = TxKey::new(height, chain, i as u16 + 1);
            recs.push(TxRecord {
                key,
                ..Default::default()
            });
            idx.push(TxIndex {
                key,
                postings: sides
                    .iter()
                    .map(|&(a, m)| (Family::Xfer, xfer_entity(a, m)))
                    .collect(),
                pact_id: None,
                code_hash: None,
                d3: Default::default(),
                holders: {
                    let mut h = sides.clone();
                    h.sort_unstable();
                    h.dedup();
                    h
                },
            });
        }
        IndexedBlock {
            block: BlockRecord {
                hash: Hash32(hash),
                chain,
                height: u64::from(height),
                ..Default::default()
            },
            txs: recs,
            idx,
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        }
    }

    /// The replay model: the smallest TxKey per `(account, module, chain)` over the applied
    /// blocks.
    fn model(applied: &BTreeMap<u8, Vec<Blk>>, backfilled: &[(u32, u32, u8)]) -> Vec<Holder> {
        let mut first: BTreeMap<(u32, u32, u8), TxKey> = BTreeMap::new();
        for &(a, m, c) in backfilled {
            first.insert((a, m, c), TxKey::new(0, c, 0));
        }
        for (&c, blocks) in applied {
            for (h, txs) in blocks.iter().enumerate() {
                for (i, sides) in txs.iter().enumerate() {
                    let key = TxKey::new(h as u32, c, i as u16 + 1);
                    for &(a, m) in sides {
                        let e = first.entry((a, m, c)).or_insert(key);
                        *e = (*e).min(key);
                    }
                }
            }
        }
        let mut v: Vec<Holder> = first
            .into_iter()
            .map(|((account, module, chain), first)| Holder {
                module,
                chain,
                first,
                account,
            })
            .collect();
        v.sort_by_key(|h| super::hold_key(h.module, h.chain, h.first, h.account));
        v
    }

    /// Apply and unwind keep the table equal to the replay model of the blocks still applied,
    /// over random block sequences, reorgs (unwind then a different fork) and repeated
    /// transfers; `HolderFirst` agrees with `Holders` row for row, and the readiness stamps
    /// follow the tips.
    #[test]
    fn apply_and_unwind_match_a_replay_model() {
        for seed in 1..=40u64 {
            let dir = tempfile::tempdir().unwrap();
            let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let mut applied: BTreeMap<u8, Vec<Blk>> = BTreeMap::new();
            let mut fork = 1u8;
            // Rows a backfill range noted (its first at a frozen position below every tail
            // tx: height 0, pos 0), mixed with the tail's applies and unwinds on the same rows.
            let mut backfilled = Vec::new();
            for _ in 0..60 {
                let mut txn = hot.rw().unwrap();
                let mut dict = HotDict::load(&txn).unwrap();
                let c = rng.below(3) as u8;
                let chain = applied.entry(c).or_default();
                if rng.below(8) == 0 {
                    let (a, m) = (1 + rng.below(5) as u32, 1 + rng.below(2) as u32);
                    let mut t = super::Tally::default();
                    t.add(a, m, TxKey::new(0, c, 0));
                    super::add_tally(&mut txn, &t).unwrap();
                    backfilled.push((a, m, c));
                } else if !chain.is_empty() && rng.below(4) == 0 {
                    let to = rng.below(chain.len() as u64) as u32;
                    // Unwind to `to - 1` (keep heights below `to`); height 0 stays.
                    let keep = to.max(1);
                    crate::tail::unwind_to(&mut txn, c, keep - 1).unwrap();
                    chain.truncate(keep as usize);
                    fork = fork.wrapping_add(1);
                } else {
                    let txs: Blk = (0..1 + rng.below(3))
                        .map(|_| {
                            (0..rng.below(4))
                                .map(|_| (1 + rng.below(5) as u32, 1 + rng.below(2) as u32))
                                .collect()
                        })
                        .collect();
                    let h = chain.len() as u32;
                    crate::tail::apply(&mut txn, &block(c, h, &txs, fork), &mut dict).unwrap();
                    chain.push(txs);
                }
                txn.commit().unwrap();
                let ro = hot.ro().unwrap();
                let got = read_all(&ro).unwrap();
                assert_eq!(got, model(&applied, &backfilled), "seed {seed}");
                assert_eq!(ro.count(Table::HolderFirst).unwrap(), got.len());
                assert_eq!(ro.count(Table::HolderByTx).unwrap(), got.len());
                for h in &got {
                    assert_eq!(
                        super::first_of(&ro, h.account, h.module, h.chain).unwrap(),
                        Some(h.first)
                    );
                }
                assert!(ready(&ro).unwrap(), "seed {seed}: stamps follow the tips");
            }
        }
    }

    /// Backfill's tally gives the table apply gives, whatever order the ranges come in.
    #[test]
    fn a_tally_in_any_order_equals_apply() {
        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        let mut rng = Rng(77);
        let mut blocks = Vec::new();
        for h in 0..30u32 {
            for c in 0..2u8 {
                let txs: Blk = (0..2)
                    .map(|_| {
                        (0..3)
                            .map(|_| (1 + rng.below(6) as u32, 1 + rng.below(2) as u32))
                            .collect()
                    })
                    .collect();
                blocks.push(block(c, h, &txs, 1));
            }
        }
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        for b in &blocks {
            crate::tail::apply(&mut txn, b, &mut dict).unwrap();
        }
        let want = read_all(&txn).unwrap();
        txn.commit().unwrap();

        let dir2 = tempfile::tempdir().unwrap();
        let hot2 = HotStore::open(dir2.path(), Geometry::default()).unwrap();
        // Newest range first, and the first range twice.
        for part in [&blocks[30..], &blocks[..30], &blocks[..30]] {
            let mut t = super::Tally::default();
            for b in part {
                for x in &b.idx {
                    t.add_sides(&x.holders, x.key);
                }
            }
            let mut txn = hot2.rw().unwrap();
            super::add_tally(&mut txn, &t).unwrap();
            txn.commit().unwrap();
        }
        let ro = hot2.ro().unwrap();
        assert_eq!(read_all(&ro).unwrap(), want);
        assert_eq!(ro.count(Table::HolderFirst).unwrap(), want.len());
    }

    /// Review M-6 of PR #54: a row whose first transfer came from backfill keeps it through a
    /// tail apply and unwind of a later transfer on the same row; a row the tail created and
    /// backfill later lowered survives the unwind of the tail's tx; a row only the unwound tx
    /// named goes. `HolderByTx` lists exactly the current firsts.
    #[test]
    fn backfill_first_then_tail_apply_and_unwind_on_the_same_row() {
        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        let (a, b, c, m) = (1u32, 2u32, 3u32, 7u32);
        // Backfill: `a` first at height 2 (pos 1).
        let early = TxKey::new(2, 0, 1);
        let mut t = super::Tally::default();
        t.add(a, m, early);
        let mut txn = hot.rw().unwrap();
        super::add_tally(&mut txn, &t).unwrap();
        txn.commit().unwrap();
        // The tail applies heights 0..=6 on chain 0 (empty), then height 7 naming a, b and c.
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        for h in 0..7 {
            crate::tail::apply(&mut txn, &block(0, h, &vec![vec![]], 1), &mut dict).unwrap();
        }
        let txs: Blk = vec![vec![(a, m), (b, m), (c, m)]];
        crate::tail::apply(&mut txn, &block(0, 7, &txs, 1), &mut dict).unwrap();
        txn.commit().unwrap();
        let late = TxKey::new(7, 0, 1);
        // Backfill lowers `b` below the tail's first afterwards.
        let mut txn = hot.rw().unwrap();
        let mut t = super::Tally::default();
        t.add(b, m, TxKey::new(1, 0, 1));
        super::add_tally(&mut txn, &t).unwrap();
        txn.commit().unwrap();
        let ro = hot.ro().unwrap();
        assert_eq!(super::first_of(&ro, a, m, 0).unwrap(), Some(early));
        assert_eq!(super::first_of(&ro, c, m, 0).unwrap(), Some(late));
        assert_eq!(ro.count(Table::HolderByTx).unwrap(), 3);
        drop(ro);
        // Unwind height 7.
        let mut txn = hot.rw().unwrap();
        crate::tail::unwind_to(&mut txn, 0, 6).unwrap();
        txn.commit().unwrap();
        let ro = hot.ro().unwrap();
        let got: Vec<(u32, TxKey)> = read_all(&ro)
            .unwrap()
            .into_iter()
            .map(|h| (h.account, h.first))
            .collect();
        assert_eq!(got, vec![(b, TxKey::new(1, 0, 1)), (a, early)]);
        assert_eq!(super::first_of(&ro, c, m, 0).unwrap(), None);
        assert_eq!(ro.count(Table::HolderFirst).unwrap(), 2);
        assert_eq!(ro.count(Table::HolderByTx).unwrap(), 2);
        assert!(ready(&ro).unwrap());
    }

    /// Secpass B M-16: a damaged key in the page's range is skipped, not counted, so the page
    /// still holds `limit` holders and the paging does not end early.
    #[test]
    fn a_damaged_key_does_not_end_the_page_early() {
        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        let m = 7u32;
        let mut txn = hot.rw().unwrap();
        for (a, h) in [(1u32, 1u32), (2, 2), (3, 3), (4, 4)] {
            let k = super::hold_key(m, 0, TxKey::new(h, 0, 1), a);
            txn.put(Table::Holders, &k, &[]).unwrap();
        }
        // One byte short of the height-3 key: sorts between heights 2 and 3.
        let bad = super::hold_key(m, 0, TxKey::new(3, 0, 1), 3);
        txn.put(Table::Holders, &bad[..16], &[]).unwrap();
        txn.commit().unwrap();
        let ro = hot.ro().unwrap();
        for order in [Order::NewestFirst, Order::OldestFirst] {
            let p = page(&ro, m, &[0], order, None, 3, &mut || Ok(())).unwrap();
            assert_eq!(p.len(), 3, "{order:?}");
        }
    }

    /// Review I-1 of PR #54: a range backfilled (or frozen) by a build without the
    /// `holders_txnum` stamp moves `next_txnum` but no tip: `ready` is false and `guard`
    /// clears the marker. This build's writers stamp in the txn that moves it.
    #[test]
    fn a_range_backfilled_without_the_txnum_stamp_is_caught() {
        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        let ro = hot.ro().unwrap();
        assert!(ready(&ro).unwrap(), "a new store is ready at TxNum 0");
        drop(ro);
        // This build: a range commit moves `next_txnum` with the stamp.
        let mut txn = hot.rw().unwrap();
        assert_eq!(super::guard(&mut txn).unwrap(), None);
        txn.set_next_txnum(100).unwrap();
        super::stamp_txnum(&mut txn).unwrap();
        txn.commit().unwrap();
        assert!(ready(&hot.ro().unwrap()).unwrap());
        // An older build: the next range moves `next_txnum`, no stamp, no tip.
        let mut txn = hot.rw().unwrap();
        txn.set_next_txnum(250).unwrap();
        txn.commit().unwrap();
        let ro = hot.ro().unwrap();
        assert!(!ready(&ro).unwrap());
        let why = super::untrusted(&ro).unwrap().unwrap();
        assert!(why.contains("holders_txnum"), "{why}");
        drop(ro);
        // Back on this build: the guard (startup, apply, unwind, the next range or freeze)
        // clears the marker instead of stamping over it.
        let mut txn = hot.rw().unwrap();
        assert!(super::guard(&mut txn).unwrap().is_some());
        super::stamp_txnum(&mut txn).unwrap();
        txn.commit().unwrap();
        let ro = hot.ro().unwrap();
        assert!(!super::marker_set(&ro).unwrap());
        assert!(!ready(&ro).unwrap());
        drop(ro);
        // A missing stamp (a store from before this stamp) is a mismatch too.
        let mut txn = hot.rw().unwrap();
        super::set_ready(&mut txn, 250).unwrap();
        txn.del(Table::Meta, super::TXNUM_AT.as_bytes()).unwrap();
        txn.commit().unwrap();
        assert!(!ready(&hot.ro().unwrap()).unwrap());
    }

    /// Pages walk a module's holders over several chains newest first (and oldest first),
    /// joined pages equal the whole list with none repeated or skipped, other modules never
    /// show, and each page reads at most `limit` keys per chain.
    #[test]
    fn pages_join_to_the_whole_list_in_both_orders() {
        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        for h in 0..20u32 {
            for c in 0..3u8 {
                let txs: Blk = vec![vec![(h * 3 + u32::from(c) + 1, 7), (h + 100, 8)]];
                crate::tail::apply(&mut txn, &block(c, h, &txs, 1), &mut dict).unwrap();
            }
        }
        txn.commit().unwrap();
        let ro = hot.ro().unwrap();
        let mut all: Vec<Holder> = read_all(&ro)
            .unwrap()
            .into_iter()
            .filter(|h| h.module == 7 && h.chain != 1)
            .collect();
        all.sort_by_key(|h| std::cmp::Reverse(h.order()));
        assert_eq!(all.len(), 40);
        for (order, want) in [
            (Order::NewestFirst, all.clone()),
            (Order::OldestFirst, all.iter().rev().copied().collect()),
        ] {
            for size in [1usize, 3, 7, 40, 41] {
                let mut got = Vec::new();
                let mut cur = None;
                loop {
                    let p = page(&ro, 7, &[0, 2], order, cur, size, &mut || Ok(())).unwrap();
                    assert!(p.len() <= size);
                    got.extend(p.iter().copied());
                    if p.len() < size {
                        break;
                    }
                    cur = p.last().map(Holder::order);
                }
                assert_eq!(got, want, "{order:?} size {size}");
            }
        }
        // The deadline check stops a page.
        let e = page(&ro, 7, &[0, 2], Order::NewestFirst, None, 10, &mut || {
            anyhow::bail!("deadline")
        });
        assert!(e.is_err());
        // An unknown module is empty.
        let p = page(
            &ro,
            99,
            &[0, 1, 2],
            Order::NewestFirst,
            None,
            10,
            &mut || Ok(()),
        )
        .unwrap();
        assert!(p.is_empty());
    }
}
