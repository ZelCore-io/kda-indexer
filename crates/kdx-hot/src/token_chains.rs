//! The chains where a module has balance rows, and the first of them per chain: the set and
//! order of stock's `tokens` query (`balance-db-repository.ts`: distinct `(module, chainId)` of
//! its `Balances` table, `coin` left out, ordered by `MIN(Balances.id)`).
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | `TokenChains` | ModuleId u32 | chain mask u32, then per set chain (ascending) the first TxKey u64 of its `Holders` rows |
//!
//! - Source: the `Holders` table (`holders.rs`), which holds exactly stock's `Balances` set,
//!   one row per `(module, chain, account)` from the first event that named it. A chain's bit
//!   is set while the module has at least one `Holders` row on it, and its first TxKey is the
//!   smallest `first` among them (the first key of the `(module, chain)` prefix).
//! - Why its own table and not the `Tokens` row (`balances.rs`): a `Tokens` row means "a
//!   tracked fungible token" to the reconciler, the D3 pass and the key listing; stock's set
//!   also holds modules with no fungible transfer (the marmalade ledgers), which must not
//!   become tracked tokens.
//! - Kept by the three `Holders` writers, in the same write txn: [`note`] in `holders::touch`
//!   (apply, backfill ranges, the rebuild), [`recheck`] in `holders::untouch_tx` (unwind: one
//!   prefix seek per removed row) and [`clear`] in `holders::clear_table`. The table is
//!   therefore a function of `Holders` after every commit, whatever the order of the writes,
//!   so it is exact under apply and unwind and a backfill gives the table the tail gives.
//! - Readiness: `Meta token_chains_ready` says the table was built from `Holders` (a new store
//!   sets it: both start empty; an existing store gets it from [`fill`], run once at startup
//!   by `kda-indexer run`). Readers also need `holders::ready` ([`ready`]).
//! - Size: one row per module that has a holder, at most 4 + 8 × 32 bytes. Stock's set grows
//!   the same way: a module needs an on-chain deploy and a transfer event, both paid in gas.

use std::collections::BTreeMap;

use anyhow::Result;
use kdx_types::TxKey;

use crate::env::{RwTxn, Txn};
use crate::holders::Holder;
use crate::tables::Table;

/// Meta key of the readiness marker (u64: the TxNum frontier when it was set).
pub const READY: &str = "token_chains_ready";
/// Chains a row can hold (the mask's width). Rows on a chain past it are not listed.
pub const MAX_CHAINS: u8 = 32;

/// One `TokenChains` value: first TxKey per chain with holders.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainsRow {
    pub first: BTreeMap<u8, TxKey>,
}

impl ChainsRow {
    pub fn mask(&self) -> u32 {
        self.first.keys().fold(0, |m, &c| m | (1 << c))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(4 + 8 * self.first.len());
        v.extend_from_slice(&self.mask().to_be_bytes());
        for k in self.first.values() {
            v.extend_from_slice(&k.to_be_bytes());
        }
        v
    }

    pub fn decode(v: &[u8]) -> Option<ChainsRow> {
        let mask = u32::from_be_bytes(v.get(..4)?.try_into().ok()?);
        if v.len() != 4 + 8 * mask.count_ones() as usize {
            return None;
        }
        let mut first = BTreeMap::new();
        let mut at = 4;
        for c in 0..MAX_CHAINS {
            if mask & (1 << c) != 0 {
                first.insert(c, TxKey::from_be_bytes(v[at..at + 8].try_into().ok()?));
                at += 8;
            }
        }
        Some(ChainsRow { first })
    }
}

/// The row of `module` (None: no holders anywhere).
pub fn row(txn: &impl Txn, module: u32) -> Result<Option<ChainsRow>> {
    match txn.get(Table::TokenChains, &module.to_be_bytes())? {
        Some(v) => Ok(Some(
            ChainsRow::decode(&v).ok_or_else(|| anyhow::anyhow!("bad TokenChains row"))?,
        )),
        None => Ok(None),
    }
}

fn put(txn: &mut RwTxn<'_>, module: u32, r: &ChainsRow) -> Result<()> {
    if r.first.is_empty() {
        txn.del(Table::TokenChains, &module.to_be_bytes())?;
        Ok(())
    } else {
        txn.put(Table::TokenChains, &module.to_be_bytes(), &r.encode())
    }
}

/// A `Holders` row `(module, chain, key)` was written: the chain's first is at most `key`.
pub fn note(txn: &mut RwTxn<'_>, module: u32, chain: u8, key: TxKey) -> Result<()> {
    if chain >= MAX_CHAINS {
        return Ok(());
    }
    let mut r = row(txn, module)?.unwrap_or_default();
    match r.first.get(&chain) {
        Some(&k) if k <= key => Ok(()),
        _ => {
            r.first.insert(chain, key);
            put(txn, module, &r)
        }
    }
}

/// A `Holders` row of `(module, chain)` was removed: the chain's first is the first key left
/// under the prefix (one seek), or the bit goes.
pub fn recheck(txn: &mut RwTxn<'_>, module: u32, chain: u8) -> Result<()> {
    if chain >= MAX_CHAINS {
        return Ok(());
    }
    let left = first_holder(txn, module, chain)?;
    let mut r = row(txn, module)?.unwrap_or_default();
    let before = r.clone();
    match left {
        Some(k) => r.first.insert(chain, k),
        None => r.first.remove(&chain),
    };
    if r != before {
        put(txn, module, &r)?;
    }
    Ok(())
}

/// The smallest `first` of the `Holders` rows of `(module, chain)`.
fn first_holder(txn: &impl Txn, module: u32, chain: u8) -> Result<Option<TxKey>> {
    let mut p = [0u8; 5];
    p[..4].copy_from_slice(&module.to_be_bytes());
    p[4] = chain;
    Ok(txn
        .scan_limit(Table::Holders, &p, 1, |k| k.starts_with(&p))?
        .first()
        .and_then(|(k, _)| crate::holders::parse_hold_key(k))
        .map(|h| h.first))
}

/// Empty the table (with `holders::clear_table`; the rebuild's touches refill it).
pub fn clear(txn: &mut RwTxn<'_>) -> Result<()> {
    txn.clear(Table::TokenChains)
}

/// Build the table from `Holders` in `txn` and set the marker: one seek per `(module, chain)`
/// that has holders (the walk skips past each prefix). Returns the `(module, chain)` count.
/// `stop` is checked every 1,024 seeks; a stop is an error and the caller drops the txn.
pub fn fill(txn: &mut RwTxn<'_>, stop: &dyn Fn() -> bool) -> Result<u64> {
    clear(txn)?;
    let mut rows: BTreeMap<u32, ChainsRow> = BTreeMap::new();
    let mut from: Vec<u8> = Vec::new();
    let mut n = 0u64;
    let mut seeks = 0u64;
    loop {
        seeks += 1;
        if seeks.is_multiple_of(1_024) && stop() {
            anyhow::bail!(
                "token chain list fill stopped (the txn is dropped; the next run redoes it)"
            );
        }
        let Some(h) = txn
            .scan_limit(Table::Holders, &from, 1, |_| true)?
            .first()
            .and_then(|(k, _)| crate::holders::parse_hold_key(k))
        else {
            break;
        };
        if h.chain < MAX_CHAINS {
            rows.entry(h.module)
                .or_default()
                .first
                .insert(h.chain, h.first);
            n += 1;
        }
        // The next (module, chain) prefix.
        let next = ((u64::from(h.module) << 8) | u64::from(h.chain)) + 1;
        if next > 0xFF_FFFF_FFFF {
            break;
        }
        from = next.to_be_bytes()[3..].to_vec();
    }
    for (m, r) in &rows {
        put(txn, *m, r)?;
    }
    let at = txn.next_txnum()?;
    txn.set_meta_u64(READY, at)?;
    Ok(n)
}

pub fn marker_set(txn: &impl Txn) -> Result<bool> {
    Ok(txn.meta(READY)?.is_some())
}

/// Whether the table may answer: built from `Holders` ([`READY`]) and `Holders` ready.
pub fn ready(txn: &impl Txn) -> Result<bool> {
    Ok(txn.has_table(Table::TokenChains) && marker_set(txn)? && crate::holders::ready(txn)?)
}

/// Every row, in module order, while `tick()` holds (a reader's row budget).
pub fn read_all(txn: &impl Txn, mut tick: impl FnMut() -> bool) -> Result<Vec<(u32, ChainsRow)>> {
    let mut out = Vec::new();
    for (k, v) in txn.scan(Table::TokenChains, &[], |_| tick())? {
        let (Ok(m), Some(r)) = (<[u8; 4]>::try_from(k.as_slice()), ChainsRow::decode(&v)) else {
            anyhow::bail!("bad TokenChains row");
        };
        out.push((u32::from_be_bytes(m), r));
    }
    Ok(out)
}

/// The table `Holders` rows give (tests and checks).
pub fn derive(holders: &[Holder]) -> Vec<(u32, ChainsRow)> {
    let mut rows: BTreeMap<u32, ChainsRow> = BTreeMap::new();
    for h in holders.iter().filter(|h| h.chain < MAX_CHAINS) {
        let e = rows
            .entry(h.module)
            .or_default()
            .first
            .entry(h.chain)
            .or_insert(h.first);
        if h.first < *e {
            *e = h.first;
        }
    }
    rows.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::{holders, Geometry, HotStore};

    fn check(txn: &impl Txn) {
        let want = derive(&holders::read_all(txn).unwrap());
        assert_eq!(read_all(txn, || true).unwrap(), want);
    }

    #[test]
    fn row_round_trip() {
        let mut r = ChainsRow::default();
        assert_eq!(ChainsRow::decode(&r.encode()), Some(r.clone()));
        r.first.insert(0, TxKey::new(5, 0, 1));
        r.first.insert(19, TxKey::new(3, 19, 0));
        r.first.insert(31, TxKey::new(9, 31, 2));
        assert_eq!(ChainsRow::decode(&r.encode()), Some(r.clone()));
        assert_eq!(r.mask(), 1 | (1 << 19) | (1 << 31));
        assert!(ChainsRow::decode(&r.encode()[..11]).is_none());
        assert!(ChainsRow::decode(&[0, 0]).is_none());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        /// Any sequence of the `Holders` writers (touch in any order, undo of any tx) leaves
        /// `TokenChains` equal to what the `Holders` rows give, after every step; a fill from
        /// the final `Holders` gives the same table. Chains at and past [`MAX_CHAINS`] have
        /// `Holders` rows but no bit (skipped by every writer, `derive` and `fill`).
        #[test]
        fn follows_holders_under_apply_and_unwind(
            ops in prop::collection::vec(
                (
                    0u8..4,
                    1u32..5,
                    1u32..3,
                    prop::sample::select(vec![0u8, 1, 2, MAX_CHAINS - 1, MAX_CHAINS, 40]),
                    0u32..6,
                    0u16..3,
                ),
                1..80,
            )
        ) {
            let dir = tempfile::tempdir().unwrap();
            let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
            for (op, a, m, c, h, p) in ops {
                let key = TxKey::new(h, c, p);
                let mut txn = hot.rw().unwrap();
                if op == 0 {
                    holders::untouch_tx(&mut txn, key).unwrap();
                } else {
                    holders::touch(&mut txn, a, m, key).unwrap();
                }
                txn.commit().unwrap();
                check(&hot.ro().unwrap());
            }
            let mut txn = hot.rw().unwrap();
            let before = read_all(&txn, || true).unwrap();
            fill(&mut txn, &|| false).unwrap();
            prop_assert_eq!(read_all(&txn, || true).unwrap(), before);
        }
    }

    /// A new store starts ready (both tables empty); a clear keeps the two tables equal.
    #[test]
    fn new_store_is_ready_and_clear_follows_holders() {
        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        assert!(ready(&hot.ro().unwrap()).unwrap());
        let mut txn = hot.rw().unwrap();
        holders::touch(&mut txn, 1, 7, TxKey::new(4, 2, 0)).unwrap();
        holders::touch(&mut txn, 2, 7, TxKey::new(3, 5, 0)).unwrap();
        check(&txn);
        holders::clear_table(&mut txn).unwrap();
        check(&txn);
        assert_eq!(txn.count(Table::TokenChains).unwrap(), 0);
        txn.commit().unwrap();
    }

    /// An existing store without the marker: `fill` builds the table from `Holders`.
    #[test]
    fn fill_builds_from_holders() {
        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        for (a, m, h, c) in [(1, 3, 9, 0), (2, 3, 4, 0), (1, 3, 2, 7), (5, 1, 8, 19)] {
            holders::touch(&mut txn, a, m, TxKey::new(h, c, 0)).unwrap();
        }
        // An older build's store: holders rows, no TokenChains rows, no marker.
        txn.clear(Table::TokenChains).unwrap();
        txn.del(Table::Meta, READY.as_bytes()).unwrap();
        assert!(!ready(&txn).unwrap());
        assert_eq!(fill(&mut txn, &|| false).unwrap(), 3);
        check(&txn);
        assert!(ready(&txn).unwrap());
        let r = row(&txn, 3).unwrap().unwrap();
        assert_eq!(r.first[&0], TxKey::new(4, 0, 0));
        assert_eq!(r.first[&7], TxKey::new(2, 7, 0));
        txn.commit().unwrap();
    }
}
