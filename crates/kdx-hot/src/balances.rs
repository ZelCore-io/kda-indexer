//! Balance state in additive hot tables (rule A-1, plan Task 9, spec §3.9). All integers are
//! big-endian.
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | `Balances` | AccountId u32, ModuleId u32, chain u8 | flags u8 (b0: absent at the node, b1: unverified), balance text as the node printed it, guard hash [16], as_of u32, reconciled_at u32 |
//! | `BalanceDirty` | same | [`Mark`]: marked_height u32, seq u64 |
//! | `Guards` | guard hash [16] | guard JSON bytes as the node returned them |
//! | `KeyAccounts` | blake2b-128(public key) [16], AccountId u32 | empty |
//! | `Tokens` | ModuleId u32 | first_height u32, holders u64, kind u8 |
//!
//! - The guard hash is blake2b-128 of the guard JSON. A keyset reference resolved with
//!   `describe-keyset` is cached under [`ref_hash`] of its chain and name.
//! - `as_of` is the node tip the `/local` answer read (`metaData.blockHeight − 1`: the node
//!   reports the pending block's height); `reconciled_at` is unix seconds.
//! - Dirty marks (B-3): tail apply and tail undo mark every `xfer` posting's `(account, module,
//!   chain)` and every `acct` posting's `(account, coin, chain)` in the same txn as the block.
//!   A mark keeps the highest height and takes the writing txn's id as its `seq`; the
//!   reconciler deletes it only when it is still the exact mark it read and not above the
//!   answer's `as_of` (SR-1, I-5, M-3).
//! - `unverified`: the row's last re-read failed or was parked; the route serves it `stale`.
//! - `holders` counts rows with a balance above zero; it is approximate (reorgs, rows not yet
//!   reconciled).

use anyhow::{Context, Result};
use kdx_decode::Family;
use kdx_types::{blake2b_128, DictKind, TxKey};
use serde_json::Value;

use crate::dict::fwd_key;
use crate::env::{be_u32, RwTxn, Txn};
use crate::tables::Table;

/// `Meta` keys of the balance seed (B-2).
pub mod meta {
    /// u32: the max tail height when the seed finished.
    pub const BALANCES_SEEDED: &str = "balances_seeded";
    /// Resume cursor of a running seed (see `kdx_ingest::seed`).
    pub const BALANCES_SEED_CURSOR: &str = "balances_seed_cursor";
}

/// `Tokens.kind`: a module seen as the module of a `TRANSFER` event (an `xfer` posting).
pub const KIND_FUNGIBLE: u8 = 0;

/// `(AccountId, ModuleId, chain)` key of `Balances` and `BalanceDirty`.
pub fn bal_key(account: u32, module: u32, chain: u8) -> [u8; 9] {
    let mut k = [0u8; 9];
    k[..4].copy_from_slice(&account.to_be_bytes());
    k[4..8].copy_from_slice(&module.to_be_bytes());
    k[8] = chain;
    k
}

pub fn parse_bal_key(k: &[u8]) -> Option<(u32, u32, u8)> {
    if k.len() != 9 {
        return None;
    }
    Some((be_u32(&k[..4])?, be_u32(&k[4..8])?, k[8]))
}

/// One `Balances` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceRow {
    /// The node has no row for the account in this module on this chain.
    pub absent: bool,
    /// The balance as the node printed it (empty when absent).
    pub balance: String,
    /// blake2b-128 of the guard JSON (zero when absent).
    pub guard: [u8; 16],
    pub as_of: u32,
    pub reconciled_at: u32,
    /// The last re-read after a change failed (I-5): the balance may be outdated.
    pub unverified: bool,
}

impl BalanceRow {
    pub fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(1 + self.balance.len() + 24);
        v.push(u8::from(self.absent) | (u8::from(self.unverified) << 1));
        v.extend_from_slice(self.balance.as_bytes());
        v.extend_from_slice(&self.guard);
        v.extend_from_slice(&self.as_of.to_be_bytes());
        v.extend_from_slice(&self.reconciled_at.to_be_bytes());
        v
    }

    pub fn decode(v: &[u8]) -> Option<BalanceRow> {
        let n = v.len().checked_sub(24)?;
        if n < 1 {
            return None;
        }
        let balance = std::str::from_utf8(&v[1..n]).ok()?.to_owned();
        Some(BalanceRow {
            absent: v[0] & 1 == 1,
            balance,
            guard: v[n..n + 16].try_into().ok()?,
            as_of: be_u32(&v[n + 16..n + 20])?,
            reconciled_at: be_u32(&v[n + 20..n + 24])?,
            unverified: v[0] & 2 == 2,
        })
    }

    /// The balance is above zero (counts as a holder).
    pub fn positive(&self) -> bool {
        !self.absent && decimal_positive(&self.balance)
    }
}

/// A decimal text (`12.5`, `0.0`, `1e-8`) is above zero: no leading minus and a non-zero
/// digit in the mantissa.
pub fn decimal_positive(s: &str) -> bool {
    let s = s.trim();
    if s.starts_with('-') {
        return false;
    }
    let mant = s.split(['e', 'E']).next().unwrap_or("");
    mant.bytes().any(|b| (b'1'..=b'9').contains(&b))
}

/// A dirty mark: the highest height that marked the row, and the id of the write txn that last
/// wrote it. MDBX write txn ids grow, so any later write of the mark (a higher block, a reorg's
/// undo or re-apply at any height, a requeue) changes it, and a reconciler that read a mark
/// deletes only that exact mark (SR-1, I-5, M-3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mark {
    pub height: u32,
    pub seq: u64,
}

impl Mark {
    pub fn encode(&self) -> [u8; 12] {
        let mut v = [0u8; 12];
        v[..4].copy_from_slice(&self.height.to_be_bytes());
        v[4..].copy_from_slice(&self.seq.to_be_bytes());
        v
    }

    /// 12 bytes; a bare 4-byte height decodes with `seq` 0.
    pub fn decode(v: &[u8]) -> Option<Mark> {
        let height = u32::from_be_bytes(v.get(..4)?.try_into().ok()?);
        let seq = match v.len() {
            4 => 0,
            12 => u64::from_be_bytes(v[4..].try_into().ok()?),
            _ => return None,
        };
        Some(Mark { height, seq })
    }
}

/// Height of a `BalanceDirty` value.
pub fn dirty_height(v: &[u8]) -> Option<u32> {
    Mark::decode(v).map(|m| m.height)
}

/// Mark a row dirty at `height` (B-3): the mark keeps the higher height and always takes this
/// txn's id, so a mark written after a reconciler read it never equals what it read. Returns
/// the mark written.
pub fn mark_dirty(
    txn: &mut RwTxn<'_>,
    account: u32,
    module: u32,
    chain: u8,
    height: u32,
) -> Result<Mark> {
    let k = bal_key(account, module, chain);
    let old = txn
        .get(Table::BalanceDirty, &k)?
        .as_deref()
        .and_then(dirty_height)
        .unwrap_or(0);
    let m = Mark {
        height: old.max(height),
        seq: txn.txn_id()?,
    };
    txn.put(Table::BalanceDirty, &k, &m.encode())?;
    Ok(m)
}

/// Delete the mark of `key` in `table` only when it is still exactly `read` (SR-1): a mark
/// raised or rewritten since the read survives. Returns whether it was deleted.
pub fn clear_mark(txn: &mut RwTxn<'_>, table: Table, key: &[u8], read: Mark) -> Result<bool> {
    match txn.get(table, key)?.as_deref().and_then(Mark::decode) {
        Some(m) if m == read => txn.del(table, key),
        _ => Ok(false),
    }
}

/// Flag an existing `Balances` row as unverified (I-5): its re-read after a change failed, so
/// the route serves it `stale` until a later read succeeds.
pub fn set_unverified(txn: &mut RwTxn<'_>, key: &[u8; 9]) -> Result<()> {
    if let Some(mut r) = txn
        .get(Table::Balances, key)?
        .as_deref()
        .and_then(BalanceRow::decode)
    {
        if !r.unverified {
            r.unverified = true;
            txn.put(Table::Balances, key, &r.encode())?;
        }
    }
    Ok(())
}

/// The ModuleId of `coin`, when the dictionary holds it.
pub fn coin_module(txn: &impl Txn) -> Result<Option<u32>> {
    Ok(txn
        .get(Table::DictFwd(DictKind::Module), &fwd_key(b"coin"))?
        .and_then(|v| be_u32(&v)))
}

/// Dirty mark for one posting of a tail block being applied or undone (B-3): an `xfer` posting
/// marks `(account, module, chain)` and notes the token; an `acct` posting marks
/// `(account, coin, chain)`. Other families are ignored. `note_tokens` is false on undo (a
/// token's first height is never raised).
pub fn mark_posting(
    txn: &mut RwTxn<'_>,
    family: u8,
    entity: u64,
    key: TxKey,
    coin: Option<u32>,
    note_tokens: bool,
) -> Result<()> {
    let (chain, height) = (key.chain(), key.height());
    if family == Family::Xfer as u8 {
        let (account, module) = ((entity >> 32) as u32, entity as u32);
        mark_dirty(txn, account, module, chain, height)?;
        if note_tokens {
            note_token(txn, module, height)?;
        }
    } else if family == Family::Acct as u8 {
        if let (Some(coin), Ok(account)) = (coin, u32::try_from(entity)) {
            mark_dirty(txn, account, coin, chain, height)?;
        }
    }
    Ok(())
}

/// One `Tokens` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRow {
    pub first_height: u32,
    pub holders: u64,
    pub kind: u8,
}

impl TokenRow {
    pub fn encode(&self) -> [u8; 13] {
        let mut v = [0u8; 13];
        v[..4].copy_from_slice(&self.first_height.to_be_bytes());
        v[4..12].copy_from_slice(&self.holders.to_be_bytes());
        v[12] = self.kind;
        v
    }
    pub fn decode(v: &[u8]) -> Option<TokenRow> {
        if v.len() != 13 {
            return None;
        }
        Some(TokenRow {
            first_height: be_u32(&v[..4])?,
            holders: u64::from_be_bytes(v[4..12].try_into().ok()?),
            kind: v[12],
        })
    }
}

/// Create the token row or lower its `first_height` to `height`.
pub fn note_token(txn: &mut RwTxn<'_>, module: u32, height: u32) -> Result<()> {
    let k = module.to_be_bytes();
    let row = match txn.get(Table::Tokens, &k)? {
        Some(v) => {
            let mut r = TokenRow::decode(&v).context("bad Tokens row")?;
            if r.first_height <= height {
                return Ok(());
            }
            r.first_height = height;
            r
        }
        None => TokenRow {
            first_height: height,
            holders: 0,
            kind: KIND_FUNGIBLE,
        },
    };
    txn.put(Table::Tokens, &k, &row.encode())
}

/// Add `delta` to a token's holder count (saturating at 0). A module with no row gets one
/// with `first_height` u32::MAX until a posting lowers it.
pub fn adjust_holders(txn: &mut RwTxn<'_>, module: u32, delta: i64) -> Result<()> {
    let k = module.to_be_bytes();
    let mut r = match txn.get(Table::Tokens, &k)? {
        Some(v) => TokenRow::decode(&v).context("bad Tokens row")?,
        None => TokenRow {
            first_height: u32::MAX,
            holders: 0,
            kind: KIND_FUNGIBLE,
        },
    };
    r.holders = if delta >= 0 {
        r.holders.saturating_add(delta as u64)
    } else {
        r.holders.saturating_sub(delta.unsigned_abs())
    };
    txn.put(Table::Tokens, &k, &r.encode())
}

/// Guard hash of the guard JSON bytes.
pub fn guard_hash(json: &[u8]) -> [u8; 16] {
    blake2b_128(json)
}

/// `Guards` key of a keyset reference resolved on `chain` (B-4; keysets are per chain):
/// blake2b-128 of `keysetref:{chain}:{name}`.
pub fn ref_hash(chain: u8, name: &str) -> [u8; 16] {
    blake2b_128(format!("keysetref:{chain}:{name}").as_bytes())
}

/// `KeyAccounts` key prefix of a public key.
pub fn key_hash(pubkey: &str) -> [u8; 16] {
    blake2b_128(pubkey.as_bytes())
}

pub fn key_account_key(pubkey: &[u8; 16], account: u32) -> [u8; 20] {
    let mut k = [0u8; 20];
    k[..16].copy_from_slice(pubkey);
    k[16..].copy_from_slice(&account.to_be_bytes());
    k
}

/// What a guard contributes to `KeyAccounts` (B-4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardKeys {
    /// An inline keyset: its keys.
    Keys(Vec<String>),
    /// A keyset reference (`ns.name` or `name`), resolved with `describe-keyset`.
    Ref(String),
    /// User, capability, module and pact guards: no rows.
    Other,
}

/// Classify a guard as the node returns it (Pact 4 and Pact 5 JSON forms).
pub fn guard_keys(g: &Value) -> GuardKeys {
    if let (Some(Value::Array(keys)), true) = (g.get("keys"), g.get("pred").is_some()) {
        return GuardKeys::Keys(
            keys.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        );
    }
    match g.get("keysetref") {
        Some(Value::String(s)) => GuardKeys::Ref(s.clone()),
        Some(Value::Object(o)) => {
            let ksn = o.get("ksn").and_then(Value::as_str).unwrap_or("");
            match o.get("ns").and_then(Value::as_str) {
                _ if ksn.is_empty() => GuardKeys::Other,
                Some(ns) if !ns.is_empty() => GuardKeys::Ref(format!("{ns}.{ksn}")),
                _ => GuardKeys::Ref(ksn.to_owned()),
            }
        }
        _ => GuardKeys::Other,
    }
}

/// The public keys of a stored guard of a row on `chain`: inline keys, or the cached
/// resolution of a reference.
pub fn stored_guard_keys(txn: &impl Txn, guard: &[u8; 16], chain: u8) -> Result<Vec<String>> {
    let Some(v) = txn.get(Table::Guards, guard)? else {
        return Ok(vec![]);
    };
    let g: Value = match serde_json::from_slice(&v) {
        Ok(g) => g,
        Err(_) => return Ok(vec![]),
    };
    Ok(match guard_keys(&g) {
        GuardKeys::Keys(k) => k,
        GuardKeys::Ref(name) => {
            match txn
                .get(Table::Guards, &ref_hash(chain, &name))?
                .and_then(|v| serde_json::from_slice::<Value>(&v).ok())
                .map(|g| guard_keys(&g))
            {
                Some(GuardKeys::Keys(k)) => k,
                _ => vec![],
            }
        }
        GuardKeys::Other => vec![],
    })
}

/// Rows of one account in `Balances`, in key order.
pub fn account_rows(txn: &impl Txn, account: u32) -> Result<Vec<(u32, u8, BalanceRow)>> {
    let p = account.to_be_bytes();
    let mut out = Vec::new();
    for (k, v) in txn.scan(Table::Balances, &p, |k| k.starts_with(&p))? {
        let (_, m, c) = parse_bal_key(&k).context("bad Balances key")?;
        out.push((m, c, BalanceRow::decode(&v).context("bad Balances row")?));
    }
    Ok(out)
}

/// Whether `(account, module, chain)` waits for a reconcile.
pub fn is_dirty(txn: &impl Txn, account: u32, module: u32, chain: u8) -> Result<bool> {
    Ok(txn
        .get(Table::BalanceDirty, &bal_key(account, module, chain))?
        .is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Geometry, HotStore};

    #[test]
    fn row_codec_and_marks() {
        let r = BalanceRow {
            absent: false,
            balance: "12.000000000001".into(),
            guard: [9; 16],
            as_of: 7,
            reconciled_at: 1_700_000_000,
            unverified: true,
        };
        assert_eq!(BalanceRow::decode(&r.encode()), Some(r.clone()));
        assert!(r.positive());
        assert!(!decimal_positive("0.0"));
        assert!(decimal_positive("1e-8"));
        assert!(!decimal_positive("-3"));
        let t = TokenRow {
            first_height: 5,
            holders: 3,
            kind: KIND_FUNGIBLE,
        };
        assert_eq!(TokenRow::decode(&t.encode()), Some(t));

        let d = tempfile::tempdir().unwrap();
        let hs = HotStore::open(d.path(), Geometry::default()).unwrap();
        let mut txn = hs.rw().unwrap();
        mark_dirty(&mut txn, 1, 2, 3, 100).unwrap();
        mark_dirty(&mut txn, 1, 2, 3, 90).unwrap();
        let v = txn.get(Table::BalanceDirty, &bal_key(1, 2, 3)).unwrap();
        assert_eq!(v.as_deref().and_then(dirty_height), Some(100));
        note_token(&mut txn, 2, 50).unwrap();
        note_token(&mut txn, 2, 60).unwrap();
        adjust_holders(&mut txn, 2, 1).unwrap();
        adjust_holders(&mut txn, 2, -5).unwrap();
        let t = TokenRow::decode(
            &txn.get(Table::Tokens, &2u32.to_be_bytes())
                .unwrap()
                .unwrap(),
        );
        assert_eq!(t.map(|t| (t.first_height, t.holders)), Some((50, 0)));
        txn.commit().unwrap();
    }

    #[test]
    fn guard_forms() {
        let ks: Value = serde_json::json!({"keys": ["ab", "cd"], "pred": "keys-all"});
        assert_eq!(
            guard_keys(&ks),
            GuardKeys::Keys(vec!["ab".into(), "cd".into()])
        );
        let r4: Value = serde_json::json!({"keysetref": "ns.admin"});
        assert_eq!(guard_keys(&r4), GuardKeys::Ref("ns.admin".into()));
        let r5: Value = serde_json::json!({"keysetref": {"ns": "free", "ksn": "k"}});
        assert_eq!(guard_keys(&r5), GuardKeys::Ref("free.k".into()));
        let r5b: Value = serde_json::json!({"keysetref": {"ns": null, "ksn": "k"}});
        assert_eq!(guard_keys(&r5b), GuardKeys::Ref("k".into()));
        let cap: Value = serde_json::json!({"cgName": "x.Y", "cgArgs": [], "cgPactId": null});
        assert_eq!(guard_keys(&cap), GuardKeys::Other);
    }

    /// A-1: a store without the additive tables opens read-only and reads them as empty.
    #[test]
    fn read_only_without_additive_tables() {
        let d = tempfile::tempdir().unwrap();
        {
            // A v2 store as P5 wrote it: base tables only.
            let env = reth_libmdbx::Environment::builder()
                .set_max_dbs(crate::env::MAX_DBS)
                .open(d.path())
                .unwrap();
            let txn = env.begin_rw_txn().unwrap();
            for t in Table::all().into_iter().filter(|t| !t.is_additive()) {
                let db = txn
                    .create_db(Some(&t.name()), reth_libmdbx::DatabaseFlags::CREATE)
                    .unwrap();
                if t == Table::Meta {
                    txn.put(
                        db.dbi(),
                        crate::env::meta::FORMAT_VERSION,
                        crate::FORMAT_VERSION.to_be_bytes(),
                        reth_libmdbx::WriteFlags::empty(),
                    )
                    .unwrap();
                }
            }
            txn.commit().unwrap();
        }
        let ro = HotStore::open_ro(d.path()).unwrap();
        let txn = ro.ro().unwrap();
        assert!(!txn.has_table(Table::Balances));
        assert_eq!(txn.count(Table::Balances).unwrap(), 0);
        assert!(txn.get(Table::Tokens, &[0, 0, 0, 0]).unwrap().is_none());
        assert!(account_rows(&txn, 1).unwrap().is_empty());
        assert_eq!(
            txn.scan_rev_prefix(Table::KeyAccounts, &[], &[])
                .unwrap()
                .count(),
            0
        );
        drop(txn);
        drop(ro);
        // `open` (rw) creates them; the read-only open then sees them.
        drop(HotStore::open(d.path(), Geometry::default()).unwrap());
        let ro = HotStore::open_ro(d.path()).unwrap();
        assert!(ro.ro().unwrap().has_table(Table::Balances));
    }
}
