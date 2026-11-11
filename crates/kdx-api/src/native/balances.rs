//! Current balances from the additive hot tables (spec §3.9, plan Task 9):
//! `/accounts/{a}/balances` and `/keys/{pk}/accounts`. A store without the tables (A-1)
//! answers empty lists.
//!
//! - `as_of_height` is the node tip of the row's last `/local` read (for
//!   an absent row, the chain's tail height at the call).
//! - `stale: true` when the row is also in `BalanceDirty` (B-3): a block changed it after that
//!   read, or a reorg unwound one.
//! - Rows the node reported absent are left out.

use axum::extract::{Path, Query};
use axum::response::{IntoResponse, Response};
use kdx_hot::balances::{self, key_account_key, key_hash, BalanceRow};
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
        "stale": balances::is_dirty(&view.txn, account, m, c)?,
    }))
}

/// Current balances of an account per token and chain.
#[utoipa::path(
    get,
    path = "/api/v1/accounts/{account}/balances",
    params(
        ("account" = String, Path, description = "Account name"),
        ("token" = Option<String>, Query, description = "Token module (default: every token)"),
        ("chain" = Option<u8>, Query, description = "Chain id"),
    ),
    responses((status = 200, description = "`items` of {module, chain, balance, guard, as_of_height, stale}, `as_of`"))
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
        let d = view.dict();
        let Some(a) = d.account_id(account) else {
            return list_response(view, vec![], None);
        };
        let token = match q.token.as_deref() {
            None => None,
            Some(t) => match d.module_id(t) {
                Some(m) => Some(m),
                None => return list_response(view, vec![], None),
            },
        };
        let mut items = Vec::new();
        for (m, c, r) in balances::account_rows(&view.txn, a)? {
            if r.absent || token.is_some_and(|t| t != m) || q.chain.is_some_and(|x| x != c) {
                continue;
            }
            let module = d.name(DictKind::Module, m).unwrap_or_default().to_owned();
            items.push(row_json(view, &module, a, m, c, &r)?);
        }
        list_response(view, items, None)
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
                if r.absent || !balances::stored_guard_keys(&view.txn, &r.guard, c)?.contains(&pk) {
                    continue;
                }
                modules.push(json!({
                    "module": d.name(DictKind::Module, m).unwrap_or_default(),
                    "chain": c,
                    "balance": r.balance,
                    "as_of_height": r.as_of,
                    "stale": balances::is_dirty(&view.txn, a, m, c)?,
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
