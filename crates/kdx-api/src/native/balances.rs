//! Current balances from the additive hot tables (spec §3.9, plan Task 9):
//! `/accounts/{a}/balances` and `/keys/{pk}/accounts`. A store without the tables (A-1)
//! answers empty lists.
//!
//! - `as_of_height` is the node tip of the row's last `/local` read (for
//!   an absent row, the chain's tail height at the call).
//! - `stale: true` when the row is also in `BalanceDirty` (B-3): a block changed it after that
//!   read, or a reorg unwound one; or when the row is `unverified` (its re-read after a change
//!   failed or was parked, I-5).
//! - Rows the node reported absent are left out unless dirty. A dirty key with no row yet
//!   (the seed backlog, a new account; M-4) is listed with `balance`, `guard` and
//!   `as_of_height` null and `stale: true`.
//! - Rows of a module marked not fungible (R-P5P9-8 #2) are left out.
//! - `/accounts/{a}/balances` is paged by (module id, chain): `limit` 1..=100 (default 100),
//!   `cursor` = `next_cursor`; a page may hold fewer items than `limit` (filtered keys).
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
        let from = match after {
            // The smallest key above the cursor's (all keys are 9 bytes).
            Some((m, c)) => [&bal_key(a, m, c)[..], &[0]].concat(),
            None => prefix.clone(),
        };
        // The first `limit + 1` keys of each table: their union holds the first `limit + 1`
        // keys of the account in rows and marks together.
        let n = limit as usize + 1;
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
        let mut hidden: HashMap<u32, bool> = HashMap::new();
        let mut items = Vec::new();
        let mut last = None;
        for (k, (row, dirty)) in keys.iter().take(limit as usize) {
            view.check_deadline()?;
            let Some((_, m, c)) = balances::parse_bal_key(k) else {
                continue;
            };
            last = Some((m, c));
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
            if hide {
                continue;
            }
            let module = d.name(DictKind::Module, m).unwrap_or_default().to_owned();
            match row {
                Some(r) if !r.absent => items.push(row_json(view, &module, a, m, c, r)?),
                r if *dirty => items.push(pending_json(&module, c, r.as_ref())),
                _ => {}
            }
        }
        let next = (keys.len() > limit as usize)
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
    ),
    responses((status = 200, description = "`items` of {account, modules: [{module, chain, balance, as_of_height, stale}]}, `next_cursor`, `as_of`"))
)]
pub async fn key_accounts(st: St, path: PathS, q: QueryQ) -> Response {
    let (pk, q) = match (parse_path(path), parse_query(q)) {
        (Ok(p), Ok(q)) => (p, q),
        (Err(e), _) | (_, Err(e)) => return e.into_response(),
    };
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
        let mut items = Vec::new();
        let mut last = None;
        for (k, _) in rows.iter().take(limit as usize) {
            view.check_deadline()?;
            let a = u32::from_be_bytes(
                k[16..20]
                    .try_into()
                    .map_err(|_| ApiError::Internal(anyhow::anyhow!("bad KeyAccounts key")))?,
            );
            last = Some(a);
            let mut modules = Vec::new();
            for (m, c, r) in balances::account_rows(&view.txn, a)? {
                if r.absent
                    || !balances::trusted(&view.txn, m, c, coin)?
                    || !balances::stored_guard_keys(&view.txn, &r.guard, c)?.contains(&pk)
                {
                    continue;
                }
                modules.push(json!({
                    "module": d.name(DictKind::Module, m).unwrap_or_default(),
                    "chain": c,
                    "balance": r.balance,
                    "as_of_height": r.as_of,
                    "stale": r.unverified || balances::is_dirty(&view.txn, a, m, c)?,
                }));
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

#[cfg(test)]
mod cursor_parse_tests {
    use super::parse_bal_cursor;

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
