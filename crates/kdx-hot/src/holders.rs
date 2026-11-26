//! A module's holders (`balance(module:)` without `accountName`): every `(account, chain)` a
//! fungible TRANSFER event named, at any balance, ordered by its first transfer.
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | `Holders` | ModuleId u32, chain u8, first TxKey u64, AccountId u32 | empty |
//! | `HolderFirst` | AccountId u32, ModuleId u32, chain u8 (the `Balances` key) | first TxKey u64 |
//!
//! - The set is stock's `Balances` set for a module: the from and to sides of every TRANSFER
//!   event (`from`/`to` not empty), so it includes holders at 0. It is read from the `xfer`
//!   postings, which carry exactly those sides (`kdx_decode::index::output_postings`).
//! - Order: stock serves its own insert id, newest first. That id is the order stock's ingest
//!   happened to insert, which cannot be reproduced; the closest stable order is the first
//!   transfer that named the row (`first`, the TxKey: height, chain, position), newest first,
//!   then AccountId. `HolderFirst` holds the same `first` per row, so an entry is found and
//!   removed exactly.
//! - Kept from block 0 by every writer, in the write txn of the change: `tail::apply` notes each
//!   `xfer` posting ([`touch`]), undo of a posting removes the entry when that posting was its
//!   first ([`untouch`]); backfill notes each range's postings in the range's MDBX commit
//!   ([`add_tally`]). [`touch`] keeps the smallest `first`, so a range built twice, or in any
//!   order, gives the same table.
//! - Storage per tx: at most one `Holders` and one `HolderFirst` row per new `xfer` posting,
//!   each smaller than the posting row itself (`TailPostings`), so a holder flood costs no
//!   more than the postings it already writes.
//! - Readiness (the PR #48 pattern, own keys): `Meta holders_ready` and per-chain stamps
//!   `holders_at@<chain>` = the chain's `Tail` checkpoint, written by apply and unwind. A new
//!   store sets the marker (every writer keeps the table from block 0); an existing store gets
//!   it from `kda-indexer reconcile-holders`. Readers use the table only while [`ready`].

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use kdx_decode::Family;
use kdx_types::TxKey;

use crate::balances::{bal_key, parse_bal_key};
use crate::checkpoint::Stage;
use crate::env::{RwTxn, Txn};
use crate::tables::Table;

/// Meta key of the readiness marker.
pub const READY: &str = "holders_ready";
/// Meta key prefix of the per-chain tip the table follows (`holders_at@<chain>`).
pub const AT_PREFIX: &str = "holders_at@";

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

/// Note a TRANSFER side `(account, module)` of the tx at `key`: the row is listed from its
/// smallest `first`. Returns whether the table changed.
pub fn touch(txn: &mut RwTxn<'_>, account: u32, module: u32, key: TxKey) -> Result<bool> {
    let chain = key.chain();
    if let Some(old) = first_of(txn, account, module, chain)? {
        if old <= key {
            return Ok(false);
        }
        // Only out-of-order writers (backfill ranges, the rebuild walk) get here.
        txn.del(Table::Holders, &hold_key(module, chain, old, account))?;
    }
    txn.put(
        Table::HolderFirst,
        &bal_key(account, module, chain),
        &key.to_be_bytes(),
    )?;
    txn.put(Table::Holders, &hold_key(module, chain, key, account), &[])?;
    Ok(true)
}

/// Undo of a TRANSFER side noted at `key`: the entry goes when `key` was its first transfer
/// (an unwind removes every later tx of the chain too, so no later one is left to keep it).
/// Returns whether it removed one.
pub fn untouch(txn: &mut RwTxn<'_>, account: u32, module: u32, key: TxKey) -> Result<bool> {
    let chain = key.chain();
    if first_of(txn, account, module, chain)? != Some(key) {
        return Ok(false);
    }
    txn.del(Table::HolderFirst, &bal_key(account, module, chain))?;
    txn.del(Table::Holders, &hold_key(module, chain, key, account))?;
    Ok(true)
}

/// The `(account, module)` of an `xfer` posting.
pub fn xfer_sides(family: u8, entity: u64) -> Option<(u32, u32)> {
    (family == Family::Xfer as u8).then_some(((entity >> 32) as u32, entity as u32))
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
    pub fn add_postings(&mut self, postings: &[(Family, u64)], key: TxKey) {
        for &(f, e) in postings {
            if let Some((a, m)) = xfer_sides(f as u8, e) {
                self.add(a, m, key);
            }
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
    txn.clear(Table::HolderFirst)
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
/// `limit` keys read per chain, so a page costs `chains × limit` reads whatever the module's
/// size. `check` runs before each chain and every 64 keys (the request deadline).
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
        match order {
            Order::NewestFirst => {
                // Keys below the cursor's (exclusive), newest first.
                let upper = at.map(|k| k.to_vec()).unwrap_or_default();
                for r in txn.scan_rev_prefix(Table::Holders, &prefix, &upper)? {
                    if n >= limit {
                        break;
                    }
                    let (k, _) = r?;
                    if let Some(h) = parse_hold_key(&k) {
                        all.push(h);
                    }
                    n += 1;
                    if n.is_multiple_of(64) {
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
                    if n >= limit || !k.starts_with(&prefix) {
                        return false;
                    }
                    n += 1;
                    if n.is_multiple_of(64) {
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

/// Whether the table may answer: both tables exist, the marker is set and every chain's stamp
/// equals its hot tip, read in `txn`'s snapshot.
pub fn ready(txn: &impl Txn) -> Result<bool> {
    Ok(txn.has_table(Table::Holders)
        && txn.has_table(Table::HolderFirst)
        && marker_set(txn)?
        && tip_mismatch(txn)?.is_none())
}

/// Clear a set marker whose table does not follow the tips (startup, and before every apply
/// and unwind). Returns the reason when it cleared it.
pub fn guard(txn: &mut RwTxn<'_>) -> Result<Option<String>> {
    if !marker_set(txn)? {
        return Ok(None);
    }
    let Some(why) = tip_mismatch(txn)? else {
        return Ok(None);
    };
    tracing::warn!(reason = %why, "holders: the table does not follow the hot tips");
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
    fn model(applied: &BTreeMap<u8, Vec<Blk>>) -> Vec<Holder> {
        let mut first: BTreeMap<(u32, u32, u8), TxKey> = BTreeMap::new();
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
            for _ in 0..60 {
                let mut txn = hot.rw().unwrap();
                let mut dict = HotDict::load(&txn).unwrap();
                let c = rng.below(3) as u8;
                let chain = applied.entry(c).or_default();
                if !chain.is_empty() && rng.below(4) == 0 {
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
                assert_eq!(got, model(&applied), "seed {seed}");
                assert_eq!(ro.count(Table::HolderFirst).unwrap(), got.len());
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
                    t.add_postings(&x.postings, x.key);
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
