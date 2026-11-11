//! NFT balance state in additive hot tables (rule A-1, plan Task 11, spec §3.9). All integers
//! are big-endian.
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | `NftBalances` | AccountId u32, ledger ModuleId u32, blake2b-128(token id) [16], chain u8 | source u8 (1 = ledger event, 2 = `/local`), ledger ModuleId u32, as_of u32, token-id length u16, token id, balance text |
//! | `NftDirty` | same | marked_height u32, ledger ModuleId u32, token id |
//!
//! - A *ledger* is a module on the allowlist ([`DEFAULT_LEDGERS`], or `[nft] ledgers`): anyone can
//!   deploy a module named `ledger` and emit marmalade-shaped events for a real token id, so a
//!   name suffix proves nothing. The ledger's ModuleId is part of the row key, so one ledger can
//!   never overwrite another's row for the same token id. Only events whose first parameter is
//!   a token id (`t:…`) count.
//! - A v2 `RECONCILE [token-id, amount, sender {account, previous, current}, receiver {…}]`
//!   carries the balances after the change: both parties' `current` are upserted (source 1; the
//!   empty account of a mint or burn is skipped). Any other ledger event of a module that
//!   emitted no `RECONCILE` in the same tx (marmalade v1: `MINT`, `BURN`, `TRANSFER`, …) marks
//!   the string params after the token id `NftDirty`; the reconciler reads those with
//!   `get-balance`.
//! - An event at height `h` replaces a row unless the row is newer (`as_of > h`), or equal and
//!   read from the node: a fill or a replay never moves a row back.
//! - Zero balances are kept (they carry `as_of`, so an older event cannot resurrect a balance);
//!   the route lists positive balances only.
//! - An account that is not in the account dictionary (a non-principal name never seen as a tx
//!   sender or a `TRANSFER` party) has no AccountId and gets no row.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::Result;
use kdx_types::record::{decode_tx, EventRec};
use kdx_types::{blake2b_128, value, Dict, DictKind, TxKey};
use serde_json::Value;

use crate::env::{be_u32, RwTxn, Txn};
use crate::tables::Table;

/// `Meta` keys of the history fill.
pub mod meta {
    /// u32: the highest tail height when the fill finished.
    pub const NFT_FILLED: &str = "nft_filled";
    /// The trusted ledgers the fill has walked, sorted, one per line (M-9).
    pub const NFT_FILLED_LEDGERS: &str = "nft_filled_ledgers";
    /// u32: end height of the last segment range the fill finished.
    pub const NFT_FILL_CURSOR: &str = "nft_fill_cursor";
}

pub const SOURCE_EVENT: u8 = 1;
pub const SOURCE_LOCAL: u8 = 2;

/// Longest token id and account name the tables keep and `/local` is asked about.
pub const MAX_TOKEN_LEN: usize = 256;

/// The marmalade ledgers on mainnet.
pub const DEFAULT_LEDGERS: &[&str] = &["marmalade.ledger", "marmalade-v2.ledger"];

static LEDGERS: OnceLock<Vec<String>> = OnceLock::new();

/// Set the trusted ledger modules once at startup (`[nft] ledgers`); a second call is ignored.
pub fn set_ledgers(l: Vec<String>) {
    let _ = LEDGERS.set(l);
}

/// The trusted ledgers (`[nft] ledgers`, else [`DEFAULT_LEDGERS`]), sorted.
pub fn ledgers() -> Vec<String> {
    let mut v: Vec<String> = match LEDGERS.get() {
        Some(l) => l.clone(),
        None => DEFAULT_LEDGERS.iter().map(|s| (*s).to_owned()).collect(),
    };
    v.sort();
    v.dedup();
    v
}

/// Whether `module` (qualified name) is a trusted ledger.
pub fn is_ledger(module: &str) -> bool {
    match LEDGERS.get() {
        Some(l) => l.iter().any(|m| m == module),
        None => DEFAULT_LEDGERS.contains(&module),
    }
}

/// `(AccountId, ledger ModuleId, token hash, chain)` key of `NftBalances` and `NftDirty`.
pub fn nft_key(account: u32, ledger: u32, token: &[u8; 16], chain: u8) -> [u8; 25] {
    let mut k = [0u8; 25];
    k[..4].copy_from_slice(&account.to_be_bytes());
    k[4..8].copy_from_slice(&ledger.to_be_bytes());
    k[8..24].copy_from_slice(token);
    k[24] = chain;
    k
}

pub fn parse_nft_key(k: &[u8]) -> Option<(u32, u32, [u8; 16], u8)> {
    if k.len() != 25 {
        return None;
    }
    Some((
        be_u32(&k[..4])?,
        be_u32(&k[4..8])?,
        k[8..24].try_into().ok()?,
        k[24],
    ))
}

pub fn token_hash(token_id: &str) -> [u8; 16] {
    blake2b_128(token_id.as_bytes())
}

/// One `NftBalances` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NftRow {
    pub source: u8,
    pub ledger: u32,
    pub as_of: u32,
    pub token_id: String,
    pub balance: String,
}

impl NftRow {
    pub fn encode(&self) -> Vec<u8> {
        let t = self.token_id.as_bytes();
        let mut v = Vec::with_capacity(11 + t.len() + self.balance.len());
        v.push(self.source);
        v.extend_from_slice(&self.ledger.to_be_bytes());
        v.extend_from_slice(&self.as_of.to_be_bytes());
        v.extend_from_slice(&(t.len() as u16).to_be_bytes());
        v.extend_from_slice(t);
        v.extend_from_slice(self.balance.as_bytes());
        v
    }

    pub fn decode(v: &[u8]) -> Option<NftRow> {
        if v.len() < 11 {
            return None;
        }
        let n = usize::from(u16::from_be_bytes(v[9..11].try_into().ok()?));
        let t = v.get(11..11 + n)?;
        Some(NftRow {
            source: v[0],
            ledger: be_u32(&v[1..5])?,
            as_of: be_u32(&v[5..9])?,
            token_id: std::str::from_utf8(t).ok()?.to_owned(),
            balance: std::str::from_utf8(&v[11 + n..]).ok()?.to_owned(),
        })
    }

    pub fn positive(&self) -> bool {
        crate::balances::decimal_positive(&self.balance)
    }
}

/// One `NftDirty` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NftMark {
    pub height: u32,
    /// Id of the write txn that last wrote the mark (see `balances::Mark`): a reconcile
    /// deletes only the exact mark it read (SR-1, I-5, M-3).
    pub seq: u64,
    pub ledger: u32,
    pub token_id: String,
}

impl NftMark {
    pub fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(16 + self.token_id.len());
        v.extend_from_slice(&self.height.to_be_bytes());
        v.extend_from_slice(&self.seq.to_be_bytes());
        v.extend_from_slice(&self.ledger.to_be_bytes());
        v.extend_from_slice(self.token_id.as_bytes());
        v
    }

    pub fn decode(v: &[u8]) -> Option<NftMark> {
        if v.len() < 16 {
            return None;
        }
        Some(NftMark {
            height: be_u32(&v[..4])?,
            seq: u64::from_be_bytes(v[4..12].try_into().ok()?),
            ledger: be_u32(&v[12..16])?,
            token_id: std::str::from_utf8(&v[16..]).ok()?.to_owned(),
        })
    }
}

/// Mark a row dirty at `height`; an existing mark keeps the higher height, and every write takes
/// this txn's id (a mark rewritten after a reconcile read it never equals what it read).
pub fn mark_dirty(
    txn: &mut RwTxn<'_>,
    account: u32,
    token_id: &str,
    chain: u8,
    ledger: u32,
    height: u32,
) -> Result<()> {
    let k = nft_key(account, ledger, &token_hash(token_id), chain);
    let old = txn
        .get(Table::NftDirty, &k)?
        .and_then(|v| NftMark::decode(&v))
        .map_or(0, |m| m.height);
    let mark = NftMark {
        height: old.max(height),
        seq: txn.txn_id()?,
        ledger,
        token_id: token_id.to_owned(),
    };
    txn.put(Table::NftDirty, &k, &mark.encode())
}

/// Apply a ledger-event balance (source 1) unless the stored row is newer. Returns whether the
/// row was written.
pub fn upsert_event(
    txn: &mut RwTxn<'_>,
    account: u32,
    token_id: &str,
    chain: u8,
    ledger: u32,
    height: u32,
    balance: &str,
) -> Result<bool> {
    let k = nft_key(account, ledger, &token_hash(token_id), chain);
    if let Some(old) = txn
        .get(Table::NftBalances, &k)?
        .and_then(|v| NftRow::decode(&v))
    {
        // The key holds the ledger; a row stored under it by another ledger (a corrupt
        // store) is never replaced.
        if old.ledger != ledger {
            return Ok(false);
        }
        if old.as_of > height || (old.as_of == height && old.source != SOURCE_EVENT) {
            return Ok(false);
        }
    }
    let row = NftRow {
        source: SOURCE_EVENT,
        ledger,
        as_of: height,
        token_id: token_id.to_owned(),
        balance: balance.to_owned(),
    };
    txn.put(Table::NftBalances, &k, &row.encode())?;
    Ok(true)
}

/// All `NftBalances` rows of an account: `(token hash, chain, row)`, key order.
pub fn account_rows(txn: &impl Txn, account: u32) -> Result<Vec<([u8; 16], u8, NftRow)>> {
    let p = account.to_be_bytes();
    let mut out = Vec::new();
    for (k, v) in txn.scan(Table::NftBalances, &p, |k| k.starts_with(&p))? {
        if let (Some((_, _, t, c)), Some(r)) = (parse_nft_key(&k), NftRow::decode(&v)) {
            out.push((t, c, r));
        }
    }
    Ok(out)
}

pub fn is_dirty(
    txn: &impl Txn,
    account: u32,
    ledger: u32,
    token_id: &str,
    chain: u8,
) -> Result<bool> {
    let k = nft_key(account, ledger, &token_hash(token_id), chain);
    Ok(txn.get(Table::NftDirty, &k)?.is_some())
}

// ---------------------------------------------------------------------------------------
// Event classification

/// One ledger party of a `RECONCILE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Party {
    pub account: String,
    pub current: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NftEvent {
    /// v2 `RECONCILE`: the balances after the change (non-empty accounts only).
    Reconcile {
        ledger: u32,
        token_id: String,
        parties: Vec<Party>,
    },
    /// Another ledger event: the accounts it names, to be read from the node.
    Touch {
        ledger: u32,
        token_id: String,
        accounts: Vec<String>,
    },
}

/// A number as the codec kept it: a JSON number's text, `{"decimal": …}` or `{"int": …}`.
fn num_text(v: &Value) -> Option<String> {
    match v {
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Object(o) => o.get("decimal").or_else(|| o.get("int")).and_then(num_text),
        _ => None,
    }
}

fn account_name_ok(s: &str) -> bool {
    !s.is_empty() && s.len() <= kdx_decode::build::MAX_ACCOUNT_LEN
}

fn token_ok(v: Option<&Value>) -> Option<&str> {
    v.and_then(Value::as_str)
        .filter(|t| t.starts_with("t:") && t.len() <= MAX_TOKEN_LEN)
}

fn reconcile(ledger: u32, p: &[Value]) -> Option<NftEvent> {
    if p.len() != 4 || !matches!(p[1], Value::Number(_) | Value::Object(_)) {
        return None;
    }
    let token_id = token_ok(p.first())?.to_owned();
    let mut parties = Vec::new();
    for side in &p[2..] {
        let o = side.as_object()?;
        let account = o.get("account")?.as_str()?;
        let current = num_text(o.get("current")?)?;
        o.get("previous")?;
        if account_name_ok(account) {
            parties.push(Party {
                account: account.to_owned(),
                current,
            });
        }
    }
    Some(NftEvent::Reconcile {
        ledger,
        token_id,
        parties,
    })
}

/// The NFT events of one tx's events. `dict` must resolve every module, event-name and account
/// id the events reference. A module that emitted a v2 `RECONCILE` in the tx gets no `Touch`
/// events from it.
pub fn classify(dict: &dyn Dict, events: &[EventRec]) -> Vec<NftEvent> {
    let mut out = Vec::new();
    let mut touches = Vec::new();
    for e in events {
        let Some(module) = dict.name(DictKind::Module, e.module) else {
            continue;
        };
        if !is_ledger(module) {
            continue;
        }
        let Some(full) = dict.name(DictKind::EventName, e.name) else {
            continue;
        };
        let name = full.rsplit('.').next().unwrap_or(full);
        let Ok(Value::Array(p)) = value::from_slice(&e.params, dict) else {
            continue;
        };
        if name == "RECONCILE" {
            if let Some(ev) = reconcile(e.module, &p) {
                out.push(ev);
                continue;
            }
        }
        let Some(token_id) = token_ok(p.first()) else {
            continue;
        };
        let accounts: Vec<String> = p[1..]
            .iter()
            .filter_map(Value::as_str)
            .filter(|s| account_name_ok(s))
            .map(str::to_owned)
            .collect();
        if !accounts.is_empty() {
            touches.push(NftEvent::Touch {
                ledger: e.module,
                token_id: token_id.to_owned(),
                accounts,
            });
        }
    }
    for t in touches {
        let covered = matches!(&t, NftEvent::Touch { ledger, .. }
            if out.iter().any(|o| matches!(o, NftEvent::Reconcile { ledger: l, .. } if l == ledger)));
        if !covered {
            out.push(t);
        }
    }
    out
}

/// Write the effects of a tx's NFT events at `(chain, height)`: `RECONCILE` balances are
/// upserted, `Touch` accounts are marked dirty. Accounts outside `dict` are skipped.
/// Returns the number of rows written or marked.
pub fn apply_events(
    txn: &mut RwTxn<'_>,
    dict: &dyn Dict,
    events: &[NftEvent],
    chain: u8,
    height: u32,
) -> Result<u32> {
    let mut n = 0;
    for ev in events {
        match ev {
            NftEvent::Reconcile {
                ledger,
                token_id,
                parties,
            } => {
                for p in parties {
                    if let Some(a) = dict.find(DictKind::Account, &p.account) {
                        n += u32::from(upsert_event(
                            txn, a, token_id, chain, *ledger, height, &p.current,
                        )?);
                    }
                }
            }
            NftEvent::Touch {
                ledger,
                token_id,
                accounts,
            } => {
                for acct in accounts {
                    if let Some(a) = dict.find(DictKind::Account, acct) {
                        mark_dirty(txn, a, token_id, chain, *ledger, height)?;
                        n += 1;
                    }
                }
            }
        }
    }
    Ok(n)
}

/// Mark every row the events touch dirty (a reorg unwound them): `RECONCILE` parties and
/// `Touch` accounts alike.
pub fn mark_all(
    txn: &mut RwTxn<'_>,
    dict: &dyn Dict,
    events: &[NftEvent],
    chain: u8,
    height: u32,
) -> Result<u32> {
    let mut n = 0;
    for ev in events {
        let (ledger, token_id, names): (u32, &str, Vec<&str>) = match ev {
            NftEvent::Reconcile {
                ledger,
                token_id,
                parties,
            } => (
                *ledger,
                token_id,
                parties.iter().map(|p| p.account.as_str()).collect(),
            ),
            NftEvent::Touch {
                ledger,
                token_id,
                accounts,
            } => (
                *ledger,
                token_id,
                accounts.iter().map(String::as_str).collect(),
            ),
        };
        for acct in names {
            if let Some(a) = dict.find(DictKind::Account, acct) {
                mark_dirty(txn, a, token_id, chain, ledger, height)?;
                n += 1;
            }
        }
    }
    Ok(n)
}

// ---------------------------------------------------------------------------------------
// Names read straight from a txn (undo has no dictionary)

/// A dictionary of names resolved up front.
struct Resolved(HashMap<(DictKind, u32), String>);

impl Dict for Resolved {
    fn find(&self, _: DictKind, _: &str) -> Option<u32> {
        None
    }
    fn name(&self, k: DictKind, id: u32) -> Option<&str> {
        self.0.get(&(k, id)).map(String::as_str)
    }
}

/// Pass-one dictionary: records the ids a decode asks for.
struct Recorder(RefCell<Vec<(DictKind, u32)>>);

impl Dict for Recorder {
    fn find(&self, _: DictKind, _: &str) -> Option<u32> {
        None
    }
    fn name(&self, k: DictKind, id: u32) -> Option<&str> {
        self.0.borrow_mut().push((k, id));
        Some("")
    }
}

fn txn_name(txn: &impl Txn, k: DictKind, id: u32) -> Result<Option<String>> {
    Ok(txn
        .get(Table::DictRev(k), &id.to_be_bytes())?
        .and_then(|v| String::from_utf8(v).ok()))
}

/// [`classify`] for a record read from the store without a loaded dictionary: the names the
/// events need are read from `DictRev` (two passes: the ledger events' account ids are found
/// first, then resolved).
pub fn classify_in_txn(txn: &impl Txn, events: &[EventRec]) -> Result<Vec<NftEvent>> {
    let mut names = HashMap::new();
    let mut ledger_events = Vec::new();
    for e in events {
        let Some(m) = txn_name(txn, DictKind::Module, e.module)? else {
            continue;
        };
        if is_ledger(&m) {
            names.insert((DictKind::Module, e.module), m);
            if let Some(n) = txn_name(txn, DictKind::EventName, e.name)? {
                names.insert((DictKind::EventName, e.name), n);
            }
            ledger_events.push(e);
        }
    }
    if ledger_events.is_empty() {
        return Ok(vec![]);
    }
    let rec = Recorder(RefCell::new(Vec::new()));
    for e in &ledger_events {
        let _ = value::from_slice(&e.params, &rec);
    }
    for (k, id) in rec.0.into_inner() {
        if let std::collections::hash_map::Entry::Vacant(v) = names.entry((k, id)) {
            if let Some(n) = txn_name(txn, k, id)? {
                v.insert(n);
            }
        }
    }
    let resolved = Resolved(names);
    let owned: Vec<EventRec> = ledger_events.into_iter().cloned().collect();
    Ok(classify(&resolved, &owned))
}

/// Undo support: mark dirty every NFT row the tx at `key` touched (record read from
/// `TailTxs`; the account ids are looked up by name in the txn's dictionary).
pub fn mark_tx_dirty(txn: &mut RwTxn<'_>, key: TxKey) -> Result<u32> {
    let Some(v) = txn.get(Table::TailTxs, &key.to_be_bytes())? else {
        return Ok(0);
    };
    let Ok(rec) = decode_tx(&v) else {
        return Ok(0);
    };
    if rec.events.is_empty() {
        return Ok(0);
    }
    let evs = classify_in_txn(&*txn, &rec.events)?;
    if evs.is_empty() {
        return Ok(0);
    }
    let finder = TxnFinder { txn: &*txn };
    let mut marks = Vec::new();
    for ev in &evs {
        let (ledger, token_id, names): (u32, &str, Vec<&str>) = match ev {
            NftEvent::Reconcile {
                ledger,
                token_id,
                parties,
            } => (
                *ledger,
                token_id,
                parties.iter().map(|p| p.account.as_str()).collect(),
            ),
            NftEvent::Touch {
                ledger,
                token_id,
                accounts,
            } => (
                *ledger,
                token_id,
                accounts.iter().map(String::as_str).collect(),
            ),
        };
        for acct in names {
            if let Some(a) = finder.account(acct)? {
                marks.push((a, token_id.to_owned(), ledger));
            }
        }
    }
    let n = marks.len() as u32;
    for (a, t, l) in marks {
        mark_dirty(txn, a, &t, key.chain(), l, key.height())?;
    }
    Ok(n)
}

struct TxnFinder<'a, T: Txn> {
    txn: &'a T,
}

impl<T: Txn> TxnFinder<'_, T> {
    fn account(&self, name: &str) -> Result<Option<u32>> {
        Ok(self
            .txn
            .get(
                Table::DictFwd(DictKind::Account),
                &crate::dict::fwd_key(name.as_bytes()),
            )?
            .and_then(|v| be_u32(&v)))
    }
}

#[cfg(test)]
mod tests {
    use kdx_types::{DictMut, MemDict};
    use serde_json::json;

    use super::*;

    fn ev(dict: &mut MemDict, module: &str, name: &str, params: Value) -> EventRec {
        EventRec {
            module: dict.intern(DictKind::Module, module),
            name: dict.intern(DictKind::EventName, &format!("{module}.{name}")),
            module_hash: 0,
            params: value::to_vec(&params, &*dict),
        }
    }

    #[test]
    fn row_and_mark_codecs() {
        let r = NftRow {
            source: 2,
            ledger: 7,
            as_of: 99,
            token_id: "t:abc".into(),
            balance: "1.0".into(),
        };
        assert_eq!(NftRow::decode(&r.encode()), Some(r.clone()));
        assert!(r.positive());
        let m = NftMark {
            height: 5,
            seq: 77,
            ledger: 1,
            token_id: "t:x".into(),
        };
        assert_eq!(NftMark::decode(&m.encode()), Some(m));
        let k = nft_key(3, 4, &token_hash("t:abc"), 9);
        assert_eq!(parse_nft_key(&k), Some((3, 4, token_hash("t:abc"), 9)));
    }

    #[test]
    fn classifies_v2_reconcile_and_v1_touches() {
        let mut d = MemDict::new();
        d.intern(DictKind::Account, "k:alice");
        d.intern(DictKind::Account, "k:bob");
        let rec = ev(
            &mut d,
            "marmalade-v2.ledger",
            "RECONCILE",
            json!(["t:tok", 1, {"account": "", "current": 0, "previous": 0},
                   {"account": "k:alice", "current": 1, "previous": 0}]),
        );
        let transfer_same_ledger = ev(
            &mut d,
            "marmalade-v2.ledger",
            "TRANSFER",
            json!(["t:tok", "k:alice", "k:bob", 1]),
        );
        let v1 = ev(
            &mut d,
            "marmalade.ledger",
            "TRANSFER",
            json!(["t:old", "k:alice", "k:bob", 1]),
        );
        let coin = ev(&mut d, "coin", "TRANSFER", json!(["k:alice", "k:bob", 1.5]));
        let notnft = ev(
            &mut d,
            "free.ledger",
            "TRANSFER",
            json!(["k:alice", "k:bob", 1.5]),
        );
        let got = classify(&d, &[rec, transfer_same_ledger, v1, coin, notnft]);
        assert_eq!(got.len(), 2);
        match &got[0] {
            NftEvent::Reconcile {
                token_id, parties, ..
            } => {
                assert_eq!(token_id, "t:tok");
                assert_eq!(
                    parties,
                    &[Party {
                        account: "k:alice".into(),
                        current: "1".into()
                    }]
                );
            }
            e => panic!("{e:?}"),
        }
        match &got[1] {
            NftEvent::Touch {
                token_id, accounts, ..
            } => {
                assert_eq!(token_id, "t:old");
                assert_eq!(accounts, &["k:alice".to_owned(), "k:bob".to_owned()]);
            }
            e => panic!("{e:?}"),
        }
    }

    #[test]
    fn upsert_never_moves_a_row_back() {
        let dir = tempfile::tempdir().unwrap();
        let hot = crate::HotStore::open(dir.path(), crate::Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        assert!(upsert_event(&mut txn, 1, "t:a", 0, 5, 100, "1.0").unwrap());
        assert!(!upsert_event(&mut txn, 1, "t:a", 0, 5, 99, "9.0").unwrap());
        // Same height, event source: a later event of the block replaces.
        assert!(upsert_event(&mut txn, 1, "t:a", 0, 5, 100, "0.0").unwrap());
        // A /local row at the same height wins.
        let k = nft_key(1, 5, &token_hash("t:a"), 0);
        let local = NftRow {
            source: SOURCE_LOCAL,
            ledger: 5,
            as_of: 100,
            token_id: "t:a".into(),
            balance: "2.0".into(),
        };
        txn.put(Table::NftBalances, &k, &local.encode()).unwrap();
        assert!(!upsert_event(&mut txn, 1, "t:a", 0, 5, 100, "3.0").unwrap());
        assert!(upsert_event(&mut txn, 1, "t:a", 0, 5, 101, "3.0").unwrap());
        let rows = account_rows(&txn, 1).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].2.balance, "3.0");
        txn.commit().unwrap();
    }

    #[test]
    fn dirty_mark_keeps_the_highest_height() {
        let dir = tempfile::tempdir().unwrap();
        let hot = crate::HotStore::open(dir.path(), crate::Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        mark_dirty(&mut txn, 1, "t:a", 0, 5, 50).unwrap();
        mark_dirty(&mut txn, 1, "t:a", 0, 5, 40).unwrap();
        let k = nft_key(1, 5, &token_hash("t:a"), 0);
        let m = NftMark::decode(&txn.get(Table::NftDirty, &k).unwrap().unwrap()).unwrap();
        assert_eq!(m.height, 50);
        assert!(is_dirty(&txn, 1, 5, "t:a", 0).unwrap());
        txn.commit().unwrap();
    }

    /// Tail apply writes the `RECONCILE` balance; an undo marks the row dirty (plan Task 11).
    #[test]
    fn tail_apply_and_undo() {
        use kdx_decode::{IndexedBlock, TxIndex};
        use kdx_types::record::{BlockRecord, TxRecord};
        use kdx_types::Hash32;

        let dir = tempfile::tempdir().unwrap();
        let hot = crate::HotStore::open(dir.path(), crate::Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        let mut dict = crate::HotDict::load(&txn).unwrap();
        let alice = dict.intern(DictKind::Account, "k:alice");
        let ledger = dict.intern(DictKind::Module, "marmalade-v2.ledger");
        let e = EventRec {
            module: ledger,
            name: dict.intern(DictKind::EventName, "marmalade-v2.ledger.RECONCILE"),
            module_hash: 0,
            params: value::to_vec(
                &json!(["t:tok", 1, {"account": "", "current": 0, "previous": 0},
                        {"account": "k:alice", "current": 1, "previous": 0}]),
                &dict,
            ),
        };
        let key = TxKey::new(300, 2, 1);
        let ib = IndexedBlock {
            block: BlockRecord {
                hash: Hash32([3u8; 32]),
                chain: 2,
                height: 300,
                ..Default::default()
            },
            txs: vec![TxRecord {
                key,
                events: vec![e],
                ..Default::default()
            }],
            idx: vec![TxIndex {
                key,
                postings: vec![],
                pact_id: None,
                code_hash: None,
            }],
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        };
        crate::tail::apply(&mut txn, &ib, &mut dict).unwrap();
        let rows = account_rows(&txn, alice).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            (rows[0].1, rows[0].2.as_of, rows[0].2.balance.as_str()),
            (2, 300, "1")
        );
        assert_eq!(rows[0].2.source, SOURCE_EVENT);
        assert!(!is_dirty(&txn, alice, ledger, "t:tok", 2).unwrap());
        assert_eq!(crate::tail::unwind_to(&mut txn, 2, 299).unwrap(), 1);
        assert!(is_dirty(&txn, alice, ledger, "t:tok", 2).unwrap());
        let m = NftMark::decode(
            &txn.get(
                Table::NftDirty,
                &nft_key(alice, ledger, &token_hash("t:tok"), 2),
            )
            .unwrap()
            .unwrap(),
        )
        .unwrap();
        assert_eq!((m.height, m.ledger), (300, ledger));
        txn.commit().unwrap();
    }

    /// An event from a `*.ledger` module that is not on the allowlist changes nothing.
    #[test]
    fn spoofed_ledger_is_ignored() {
        let mut d = MemDict::new();
        d.intern(DictKind::Account, "k:alice");
        let spoof = ev(
            &mut d,
            "free.x.ledger",
            "RECONCILE",
            json!(["t:tok", 1, {"account": "", "current": 0, "previous": 0},
                   {"account": "k:alice", "current": 1, "previous": 0}]),
        );
        let spoof_t = ev(
            &mut d,
            "free.x.ledger",
            "TRANSFER",
            json!(["t:tok", "k:alice", "k:alice", 1]),
        );
        assert!(classify(&d, &[spoof, spoof_t]).is_empty());
    }

    /// Two trusted ledgers with the same token id keep separate rows.
    #[test]
    fn ledgers_keep_separate_rows() {
        let dir = tempfile::tempdir().unwrap();
        let hot = crate::HotStore::open(dir.path(), crate::Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        assert!(upsert_event(&mut txn, 1, "t:a", 0, 5, 100, "1.0").unwrap());
        assert!(upsert_event(&mut txn, 1, "t:a", 0, 6, 90, "7.0").unwrap());
        mark_dirty(&mut txn, 1, "t:a", 0, 6, 91).unwrap();
        let rows = account_rows(&txn, 1).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(is_dirty(&txn, 1, 6, "t:a", 0).unwrap());
        assert!(!is_dirty(&txn, 1, 5, "t:a", 0).unwrap());
        // A row under ledger 5's key that claims another ledger is left alone.
        let k = nft_key(1, 5, &token_hash("t:a"), 0);
        let bad = NftRow {
            source: 1,
            ledger: 6,
            as_of: 1,
            token_id: "t:a".into(),
            balance: "9".into(),
        };
        txn.put(Table::NftBalances, &k, &bad.encode()).unwrap();
        assert!(!upsert_event(&mut txn, 1, "t:a", 0, 5, 200, "2.0").unwrap());
        txn.commit().unwrap();
    }
}
