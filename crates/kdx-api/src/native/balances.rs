//! Current balances from the additive hot tables (spec §3.9, plan Task 9):
//! `/accounts/{a}/balances` and `/keys/{pk}/accounts`. A store without the tables (A-1)
//! answers empty lists.
//!
//! - `as_of_height` is the node tip of the row's last `/local` read (for
//!   an absent row, the chain's tail height at the call).
//! - `stale: true` when the row is also in `BalanceDirty` (B-3): a block changed it after that
//!   read, or a reorg unwound one; or when the row is `unverified` (its re-read after a change
//!   failed or was parked, I-5).
//! - Every row that exists on chain is listed at any balance, 0 included, as stock lists it
//!   (owner ruling 2026-11-26; one rule on every balance list, native and GraphQL). Rows the
//!   node reported absent are left out unless dirty. A dirty key with no row yet
//!   (the seed backlog, a new account; M-4) is listed with `balance`, `guard` and
//!   `as_of_height` null and `stale: true`.
//! - Rows of a module marked not fungible (R-P5P9-8 #2) are left out.
//! - `/accounts/{a}/balances` is paged by (module id, chain): `limit` 1..=100 (default 100),
//!   `cursor` = `next_cursor`; rows left out are skipped before `limit`, so a page with a
//!   `next_cursor` is full (unless the walk hit [`MAX_BALANCE_WALK`] keys).
//! - `/keys/{pk}/accounts` lists only rows of `coin` and of modules confirmed `fungible-v2`
//!   on the row's chain (I-6): another module's guard is the module's word, not the chain's.

use std::collections::{BTreeMap, HashMap};

use axum::extract::{Path, Query};
use axum::response::{IntoResponse, Response};
use kdx_hot::balances::{self, bal_key, key_account_key, key_hash, BalanceRow, KIND_NOT_FUNGIBLE};
use kdx_hot::{Table, Txn};
use kdx_types::{Dict, DictKind};
use serde_json::{json, Value};

use super::{account_name, list_response, parse_path, parse_query, run, St, Q};
use crate::store::View;
use crate::{ApiError, ApiResult};

type PathS = Result<Path<String>, axum::extract::rejection::PathRejection>;
type QueryQ = Result<Query<Q>, axum::extract::rejection::QueryRejection>;

/// Longest public key accepted (bytes).
const MAX_KEY_LEN: usize = 256;

fn guard_json(view: &View, g: &[u8; 16]) -> ApiResult<Value> {
    Ok(view
        .txn
        .get(Table::Guards, g)?
        .and_then(|v| serde_json::from_slice(&v).ok())
        .unwrap_or(Value::Null))
}

fn row_json(
    view: &View,
    module: &str,
    account: u32,
    m: u32,
    c: u8,
    r: &BalanceRow,
) -> ApiResult<Value> {
    Ok(json!({
        "module": module,
        "chain": c,
        "balance": r.balance,
        "guard": guard_json(view, &r.guard)?,
        "as_of_height": r.as_of,
        "stale": r.unverified || balances::is_dirty(&view.txn, account, m, c)?,
    }))
}

/// A dirty key with no usable row (M-4).
/// Keys one `/accounts/{a}/balances` page reads at most (listed or not).
const MAX_BALANCE_WALK: usize = 10_000;

fn pending_json(module: &str, c: u8, r: Option<&BalanceRow>) -> Value {
    json!({
        "module": module,
        "chain": c,
        "balance": Value::Null,
        "guard": Value::Null,
        "as_of_height": r.map(|r| r.as_of),
        "stale": true,
    })
}

/// `(module, chain)` of an `/accounts/{a}/balances` cursor: 10 hex digits.
fn parse_bal_cursor(c: &str) -> ApiResult<(u32, u8)> {
    let bad = || ApiError::BadRequest("invalid cursor".into());
    // ASCII hex only, so the byte slices below always fall on char boundaries.
    if c.len() != 10 || !c.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(bad());
    }
    let m = u32::from_str_radix(&c[..8], 16).map_err(|_| bad())?;
    let ch = u8::from_str_radix(&c[8..], 16).map_err(|_| bad())?;
    Ok((m, ch))
}

/// Current balances of an account per token and chain.
#[utoipa::path(
    get,
    path = "/api/v1/accounts/{account}/balances",
    params(
        ("account" = String, Path, description = "Account name"),
        ("token" = Option<String>, Query, description = "Token module (default: every token)"),
        ("chain" = Option<u8>, Query, description = "Chain id"),
        ("limit" = Option<u32>, Query, description = "1..=100, default 100"),
        ("cursor" = Option<String>, Query, description = "Opaque page cursor"),
    ),
    responses((status = 200, description = "`items` of {module, chain, balance, guard, as_of_height, stale}, `next_cursor`, `as_of`"))
)]
pub async fn account_balances(st: St, path: PathS, q: QueryQ) -> Response {
    let (account, q) = match (parse_path(path), parse_query(q)) {
        (Ok(p), Ok(q)) => (p, q),
        (Err(e), _) | (_, Err(e)) => return e.into_response(),
    };
    run(st.0, move |view| {
        let account = account_name(&account)?;
        if q.chain
            .is_some_and(|c| u32::from(c) >= kdx_segment::SLOT_WIDTH)
        {
            return Err(ApiError::BadRequest("chain out of range".into()));
        }
        let limit = q.limit.unwrap_or(super::MAX_LIMIT);
        if limit == 0 || limit > super::MAX_LIMIT {
            return Err(ApiError::BadRequest(format!(
                "limit must be 1..={}",
                super::MAX_LIMIT
            )));
        }
        let after = q.cursor.as_deref().map(parse_bal_cursor).transpose()?;
        let d = view.dict();
        let Some(a) = d.account_id(account) else {
            return list_response(view, vec![], None);
        };
        let mut prefix = a.to_be_bytes().to_vec();
        if let Some(t) = q.token.as_deref() {
            match d.module_id(t) {
                Some(m) => prefix.extend_from_slice(&m.to_be_bytes()),
                None => return list_response(view, vec![], None),
            }
        }
        let mut from = match after {
            // The smallest key above the cursor's (all keys are 9 bytes).
            Some((m, c)) => [&bal_key(a, m, c)[..], &[0]].concat(),
            None => prefix.clone(),
        };
        // Keys are walked in batches until `limit` items are listed and one more listed item
        // is seen (it sets `next_cursor`), so rows left out (absent, other chain,
        // hidden module) never make a page short while it carries a cursor (review minor 1).
        // The walk stops after `MAX_BALANCE_WALK` keys; that page carries a cursor at the
        // last key read.
        let n = limit as usize + 1;
        let mut hidden: HashMap<u32, bool> = HashMap::new();
        let mut items = Vec::new();
        let mut last = None;
        let mut more = false;
        let mut walked = 0usize;
        'walk: loop {
            // The first `n` keys of each table: their union holds the first `n` keys of the
            // account in rows and marks together.
            let mut keys: BTreeMap<Vec<u8>, (Option<BalanceRow>, bool)> = BTreeMap::new();
            for (k, v) in view
                .txn
                .scan_limit(Table::Balances, &from, n, |k| k.starts_with(&prefix))?
            {
                keys.entry(k).or_default().0 = BalanceRow::decode(&v);
            }
            for t in [Table::BalanceDirty, Table::BalanceSeed] {
                for (k, _) in view
                    .txn
                    .scan_limit(t, &from, n, |k| k.starts_with(&prefix))?
                {
                    keys.entry(k).or_default().1 = true;
                }
            }
            let end = keys.len() < n;
            for (k, (row, dirty)) in keys.iter().take(n) {
                view.check_deadline()?;
                if walked >= MAX_BALANCE_WALK {
                    more = true;
                    break 'walk;
                }
                walked += 1;
                from = [&k[..], &[0]].concat();
                let Some((_, m, c)) = balances::parse_bal_key(k) else {
                    continue;
                };
                if q.chain.is_some_and(|x| x != c) {
                    continue;
                }
                let hide = match hidden.get(&m) {
                    Some(&h) => h,
                    None => {
                        let h = balances::token_row(&view.txn, m)?
                            .is_some_and(|t| t.kind == KIND_NOT_FUNGIBLE);
                        hidden.insert(m, h);
                        h
                    }
                };
                // A row on chain is listed at any balance; an absent one only when a later
                // block touched it (then it waits for its read).
                let listed = !hide && (row.as_ref().is_some_and(|r| !r.absent) || *dirty);
                if !listed {
                    continue;
                }
                if items.len() == limit as usize {
                    more = true;
                    break 'walk;
                }
                let module = d.name(DictKind::Module, m).unwrap_or_default().to_owned();
                items.push(match row {
                    Some(r) if !r.absent => row_json(view, &module, a, m, c, r)?,
                    r => pending_json(&module, c, r.as_ref()),
                });
                last = Some((m, c));
            }
            if end {
                break;
            }
        }
        // At the walk cap with fewer than `limit` items, resume after the last key read.
        if more && items.len() < limit as usize {
            last = balances::parse_bal_key(&from[..from.len() - 1]).map(|(_, m, c)| (m, c));
        }
        let next = more
            .then_some(last)
            .flatten()
            .map(|(m, c)| format!("{m:08x}{c:02x}"));
        list_response(view, items, next)
    })
    .await
}

/// Accounts whose guard holds the key (inline keysets and resolved keyset references, B-4),
/// with the balances whose guard holds it. Paged by account: `cursor` is the `next_cursor`.
#[utoipa::path(
    get,
    path = "/api/v1/keys/{pubkey}/accounts",
    params(
        ("pubkey" = String, Path, description = "Public key"),
        ("limit" = Option<u32>, Query, description = "1..=100, default 20"),
        ("cursor" = Option<String>, Query, description = "Opaque page cursor"),
        ("token" = Option<String>, Query, description = "Only this module's rows, if it is a confirmed fungible-v2 on the row's chain. Without it, only the `[balances] key_modules` (default `coin`)"),
    ),
    responses((status = 200, description = "`items` of {account, modules: [{module, chain, balance, as_of_height, stale, trusted}]}, `next_cursor`, `as_of`. `trusted`: the module is one of `key_modules`. A row proves only that the account's guard holds the key, not ownership: anyone can create an account guarded by someone else's key"))
)]
pub async fn key_accounts(st: St, path: PathS, q: QueryQ) -> Response {
    let (pk, q) = match (parse_path(path), parse_query(q)) {
        (Ok(p), Ok(q)) => (p, q),
        (Err(e), _) | (_, Err(e)) => return e.into_response(),
    };
    // M-1 (SEC-2): a module that merely implements fungible-v2 can list any balance under
    // any key, so by default only the governance-trusted `key_modules` are listed; another
    // module only when asked for by `token`, and marked `trusted: false`.
    let key_modules = st.0.key_modules();
    run(st.0, move |view| {
        if pk.is_empty() || pk.len() > MAX_KEY_LEN {
            return Err(ApiError::BadRequest(format!(
                "public key must be 1..={MAX_KEY_LEN} bytes"
            )));
        }
        let limit = q.limit.unwrap_or(super::DEFAULT_LIMIT);
        if limit == 0 || limit > super::MAX_LIMIT {
            return Err(ApiError::BadRequest(format!(
                "limit must be 1..={}",
                super::MAX_LIMIT
            )));
        }
        let after = match q.cursor.as_deref() {
            None => None,
            Some(c) => Some(
                u32::from_str_radix(c, 16)
                    .ok()
                    .filter(|_| c.len() == 8)
                    .ok_or_else(|| ApiError::BadRequest("invalid cursor".into()))?,
            ),
        };
        let kh = key_hash(&pk);
        let from = match after {
            Some(a) => key_account_key(&kh, a.saturating_add(1)).to_vec(),
            None => kh.to_vec(),
        };
        if after == Some(u32::MAX) {
            return list_response(view, vec![], None);
        }
        let rows = view
            .txn
            .scan_limit(Table::KeyAccounts, &from, limit as usize + 1, |k| {
                k.starts_with(&kh)
            })?;
        let d = view.dict();
        let coin = balances::coin_module(&view.txn)?;
        let module_id = |name: &str| -> ApiResult<Option<u32>> {
            Ok(view
                .txn
                .get(
                    Table::DictFwd(DictKind::Module),
                    &kdx_hot::dict::fwd_key(name.as_bytes()),
                )?
                .and_then(|v| v.get(..4).and_then(|b| b.try_into().ok()))
                .map(u32::from_be_bytes))
        };
        // The modules listed, and whether each is one of `key_modules`.
        let wanted: Vec<(u32, bool)> = match q.token.as_deref() {
            Some(t) => module_id(t)?
                .map(|m| (m, key_modules.iter().any(|k| k == t)))
                .into_iter()
                .collect(),
            None => {
                let mut v = Vec::new();
                for k in &key_modules {
                    if let Some(m) = module_id(k)? {
                        v.push((m, true));
                    }
                }
                v
            }
        };
        let mut items = Vec::new();
        let mut last = None;
        for (k, _) in rows.iter().take(limit as usize) {
            view.check_deadline()?;
            let a = key_account(k)?;
            last = Some(a);
            let mut modules = Vec::new();
            for (m, c, r) in balances::account_rows(&view.txn, a)? {
                let Some(&(_, key_module)) = wanted.iter().find(|w| w.0 == m) else {
                    continue;
                };
                // One rule with `/accounts/{a}/balances`: a row on chain is listed at any
                // balance; an absent one only when a later block touched it, waiting for its
                // read (balance null, `stale: true`).
                let dirty = balances::is_dirty(&view.txn, a, m, c)?;
                if (r.absent && !dirty)
                    || !balances::trusted(&view.txn, m, c, coin)?
                    || !balances::stored_guard_keys(&view.txn, &r.guard, c)?.contains(&pk)
                {
                    continue;
                }
                modules.push(json!({
                    "module": d.name(DictKind::Module, m).unwrap_or_default(),
                    "chain": c,
                    "balance": if r.absent { Value::Null } else { Value::from(r.balance.as_str()) },
                    "as_of_height": r.as_of,
                    "stale": r.unverified || dirty,
                    "trusted": key_module,
                }));
            }
            // The cursor still walks `KeyAccounts`; an account with no listed row is left out.
            if modules.is_empty() {
                continue;
            }
            items.push(json!({
                "account": d.name(DictKind::Account, a).unwrap_or_default(),
                "modules": modules,
            }));
        }
        let next = (rows.len() > limit as usize)
            .then_some(last)
            .flatten()
            .map(|a| format!("{a:08x}"));
        list_response(view, items, next)
    })
    .await
}

/// The account id of a `KeyAccounts` key (bytes 16..20).
fn key_account(k: &[u8]) -> ApiResult<u32> {
    k.get(16..20)
        .and_then(|b| b.try_into().ok())
        .map(u32::from_be_bytes)
        .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("bad KeyAccounts key")))
}

#[cfg(test)]
mod cursor_parse_tests {
    use super::{key_account, parse_bal_cursor};

    /// F-49: a `KeyAccounts` key shorter than 20 bytes is a 500, not a panic.
    #[test]
    fn short_key_accounts_key_is_an_error() {
        let mut k = vec![0u8; 20];
        k[16..20].copy_from_slice(&7u32.to_be_bytes());
        assert_eq!(key_account(&k).unwrap(), 7);
        for n in [0, 16, 19] {
            assert!(matches!(
                key_account(&k[..n]),
                Err(crate::ApiError::Internal(_))
            ));
        }
    }

    #[test]
    fn rejects_non_hex_and_multibyte_without_panicking() {
        assert_eq!(parse_bal_cursor("0000000a13").ok(), Some((10, 0x13)));
        // 10 bytes long, but byte 8 is inside a multi-byte char.
        for bad in [
            "0000000\u{e9}0",
            "00000000\u{e9}",
            "zzzzzzzzzz",
            "",
            "0000000000ff",
        ] {
            assert!(parse_bal_cursor(bad).is_err(), "{bad:?}");
        }
    }
}

#[cfg(test)]
mod key_accounts_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::extract::{Path, Query, State};
    use kdx_hot::balances::{bal_key, guard_hash, key_account_key, key_hash, BalanceRow, TokenRow};
    use kdx_hot::{HotDict, Table};
    use kdx_types::{DictKind, DictMut};
    use serde_json::{json, Value};

    use crate::{Api, ApiSettings, Store, TipWatch};

    /// `coin` and a confirmed fungible-v2 `free.evil` both hold an account guarded by `k1`.
    fn fixture() -> (tempfile::TempDir, Arc<crate::ApiState>) {
        let d = tempfile::tempdir().unwrap();
        let g = kdx_hot::Geometry {
            max_gb: 1,
            growth_mb: 16,
        };
        let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap());
        let mut w = hot.rw().unwrap();
        let mut dict = HotDict::load(&w).unwrap();
        let coin = dict.intern(DictKind::Module, "coin");
        let evil = dict.intern(DictKind::Module, "free.evil");
        let a = dict.intern(DictKind::Account, "alice");
        let b = dict.intern(DictKind::Account, "bob");
        let carol = dict.intern(DictKind::Account, "carol");
        let dave = dict.intern(DictKind::Account, "dave");
        dict.persist(&mut w).unwrap();
        let guard = br#"{"keys":["k1"],"pred":"keys-all"}"#;
        let gh = guard_hash(guard);
        w.put(Table::Guards, &gh, guard).unwrap();
        let row = |bal: &str| BalanceRow {
            absent: false,
            balance: bal.into(),
            guard: gh,
            as_of: 1,
            reconciled_at: 1,
            unverified: false,
        };
        let token = TokenRow {
            fungible_chains: 1,
            checked_chains: 1,
            ..TokenRow::default()
        };
        w.put(Table::Tokens, &evil.to_be_bytes(), &token.encode())
            .unwrap();
        // alice: coin and free.evil; bob: free.evil only.
        for (acct, m, bal) in [(a, coin, "1.5"), (a, evil, "1000000"), (b, evil, "42")] {
            w.put(Table::Balances, &bal_key(acct, m, 0), &row(bal).encode())
                .unwrap();
            w.put(
                Table::KeyAccounts,
                &key_account_key(&key_hash("k1"), acct),
                &[],
            )
            .unwrap();
        }
        // alice's coin on chain 1 is held at 0 by the node: it exists on chain, so it is
        // listed at 0 (as stock). Her coin on chain 2 was reported absent: never listed.
        w.put(Table::Balances, &bal_key(a, coin, 1), &row("0.0").encode())
            .unwrap();
        let absent = BalanceRow {
            absent: true,
            ..row("")
        };
        w.put(Table::Balances, &bal_key(a, coin, 2), &absent.encode())
            .unwrap();
        // carol: coin at 0 on chains 0..=5, 3 on chain 6 (every row is listed and paged).
        for c in 0..=6u8 {
            let bal = if c == 6 { "3" } else { "0" };
            w.put(
                Table::Balances,
                &bal_key(carol, coin, c),
                &row(bal).encode(),
            )
            .unwrap();
        }
        // dave (guarded by `k2`): coin at 0 on chain 0 that a later block touched (dirty), so a
        // re-read is queued.
        let guard2 = br#"{"keys":["k2"],"pred":"keys-all"}"#;
        let gh2 = guard_hash(guard2);
        w.put(Table::Guards, &gh2, guard2).unwrap();
        let dave_row = BalanceRow {
            guard: gh2,
            ..row("0.0")
        };
        w.put(Table::Balances, &bal_key(dave, coin, 0), &dave_row.encode())
            .unwrap();
        w.put(Table::BalanceDirty, &bal_key(dave, coin, 0), &[1])
            .unwrap();
        w.put(
            Table::KeyAccounts,
            &key_account_key(&key_hash("k2"), dave),
            &[],
        )
        .unwrap();
        w.commit().unwrap();
        let store = Arc::new(Store::open(hot, d.path()).unwrap());
        let settings = ApiSettings {
            network: "mainnet01".into(),
            deadline: Duration::from_secs(5),
            max_in_flight: 16,
            max_body_bytes: 8 << 20,
            v1txs_max_body_bytes: 8 << 20,
            v1txs_deadline: std::time::Duration::from_secs(30),
            rate_per_ip: 20,
            rate_burst: 40,
            trusted_proxies: Vec::new(),
            read_threads: 2,
            ready_max_lag: 10,
            max_event_lists: 64,
        };
        (d, Api::new(store, settings, TipWatch::default()).state())
    }

    async fn get(st: &Arc<crate::ApiState>, token: Option<&str>) -> Value {
        let q = super::Q {
            token: token.map(str::to_owned),
            ..super::Q::default()
        };
        let r = super::key_accounts(State(st.clone()), Ok(Path("k1".into())), Ok(Query(q))).await;
        let body = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn rows(v: &Value) -> Vec<(String, String, bool)> {
        let mut out = Vec::new();
        for it in v["items"].as_array().unwrap() {
            for m in it["modules"].as_array().unwrap() {
                out.push((
                    it["account"].as_str().unwrap().to_owned(),
                    m["module"].as_str().unwrap().to_owned(),
                    m["trusted"].as_bool().unwrap(),
                ));
            }
        }
        out
    }

    #[tokio::test]
    async fn by_key_lists_only_key_modules_unless_asked() {
        let (_d, st) = fixture();
        // Default: coin only (alice's rows on chains 0 and 1, the latter at 0); bob
        // (free.evil only) is left out of the page.
        let v = get(&st, None).await;
        assert_eq!(
            rows(&v),
            [
                ("alice".into(), "coin".into(), true),
                ("alice".into(), "coin".into(), true)
            ],
            "{v}"
        );
        assert_eq!(v["items"].as_array().unwrap().len(), 1, "{v}");
        // Asked for by name: listed, and marked untrusted.
        let v = get(&st, Some("free.evil")).await;
        assert_eq!(
            rows(&v),
            [
                ("alice".into(), "free.evil".into(), false),
                ("bob".into(), "free.evil".into(), false)
            ],
            "{v}"
        );
        // An unknown or unconfirmed module lists nothing.
        let v = get(&st, Some("free.none")).await;
        assert!(rows(&v).is_empty(), "{v}");
    }

    /// Owner ruling 2026-11-26 (stock compatibility): a row that exists on chain is listed at
    /// any balance, 0 included, on `/accounts/{a}/balances` (with or without `chain`) and on
    /// `/keys/{pk}/accounts`; a row the node reported absent is not.
    #[tokio::test]
    async fn rows_on_chain_are_listed_at_any_balance() {
        let (_d, st) = fixture();
        let bal = |chain: Option<u8>| {
            let st = st.clone();
            async move {
                let q = super::Q {
                    chain,
                    ..super::Q::default()
                };
                let r = super::account_balances(State(st), Ok(Path("alice".into())), Ok(Query(q)))
                    .await;
                let body = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
                let v: Value = serde_json::from_slice(&body).unwrap();
                v["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|i| {
                        (
                            i["module"].as_str().unwrap().to_owned(),
                            i["chain"].as_u64().unwrap(),
                            i["balance"].clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        };
        assert_eq!(
            bal(None).await,
            [
                ("coin".to_owned(), 0, json!("1.5")),
                ("coin".to_owned(), 1, json!("0.0")),
                ("free.evil".to_owned(), 0, json!("1000000"))
            ]
        );
        assert_eq!(bal(Some(1)).await, [("coin".to_owned(), 1, json!("0.0"))]);
        assert!(bal(Some(2)).await.is_empty());
        // By key: alice's coin rows on chains 0 and 1 (at 0), not the absent one.
        let v = get(&st, None).await;
        let chains: Vec<(u64, Value)> = v["items"][0]["modules"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| (m["chain"].as_u64().unwrap(), m["balance"].clone()))
            .collect();
        assert_eq!(chains, [(0, json!("1.5")), (1, json!("0.0"))], "{v}");
    }

    async fn balances(st: &Arc<crate::ApiState>, account: &str, q: super::Q) -> Value {
        let r = super::account_balances(State(st.clone()), Ok(Path(account.into())), Ok(Query(q)))
            .await;
        let body = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    /// Rows at 0 page like any other: every page before the last is full and the walk
    /// lists every row of the account in (module, chain) order.
    #[tokio::test]
    async fn balances_pages_list_zero_rows_in_order() {
        let (_d, st) = fixture();
        let mut cursor = None;
        let mut chains = Vec::new();
        loop {
            let q = super::Q {
                limit: Some(2),
                cursor: cursor.take(),
                ..super::Q::default()
            };
            let v = balances(&st, "carol", q).await;
            let items = v["items"].as_array().unwrap();
            chains.extend(items.iter().map(|i| i["chain"].as_u64().unwrap()));
            match v["next_cursor"].as_str() {
                Some(c) => {
                    assert_eq!(items.len(), 2, "{v}");
                    cursor = Some(c.to_owned());
                }
                None => break,
            }
        }
        assert_eq!(chains, [0, 1, 2, 3, 4, 5, 6]);
    }

    /// A row at 0 that a later block touched is listed with its stored balance and
    /// `stale: true` on both routes, like any dirty row.
    #[tokio::test]
    async fn a_dirty_zero_row_is_stale_on_both_routes() {
        let (_d, st) = fixture();
        let v = balances(&st, "dave", super::Q::default()).await;
        let items = v["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{v}");
        assert_eq!(items[0]["balance"], "0.0", "{v}");
        assert_eq!(items[0]["stale"], true, "{v}");
        let r = super::key_accounts(
            State(st.clone()),
            Ok(Path("k2".into())),
            Ok(Query(super::Q::default())),
        )
        .await;
        let body = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let items = v["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{v}");
        assert_eq!(items[0]["account"], "dave", "{v}");
        let m = &items[0]["modules"][0];
        assert_eq!(m["chain"], 0, "{v}");
        assert_eq!(m["balance"], "0.0", "{v}");
        assert_eq!(m["stale"], true, "{v}");
    }

    #[tokio::test]
    async fn key_modules_setting_widens_the_default() {
        let (_d, st) = fixture();
        st.set_key_modules(vec!["coin".into(), "free.evil".into()]);
        let v = get(&st, None).await;
        assert_eq!(rows(&v).len(), 4, "{v}");
        assert!(rows(&v).iter().all(|r| r.2), "{v}");
    }
}
