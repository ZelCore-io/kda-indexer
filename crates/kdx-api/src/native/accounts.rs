//! Account and key histories: `/accounts/{a}/transfers`, `/accounts/{a}/txs`, `/keys/{pk}/txs`.

use axum::extract::{Path, Query};
use axum::response::{IntoResponse, Response};
use kdx_decode::index::xfer_entity;
use kdx_decode::Family;
use kdx_types::{Dict, DictKind};
use serde_json::Value;

use super::txs::{summary, transfer_json};
use super::{
    account_name, candidates, list_params, list_response, page_rows, parse_path, parse_query, run,
    tx_context, ListParams, Q, St,
};
use crate::compat::shapes;
use crate::{ApiError, ApiResult};

type PathS = Result<Path<String>, axum::extract::rejection::PathRejection>;
type QueryQ = Result<Query<Q>, axum::extract::rejection::QueryRejection>;

/// Transfers of `token` (default `coin`) with the account on either side, newest first;
/// a cross-chain pair is resolved from the same tx's events.
#[utoipa::path(
    get,
    path = "/api/v1/accounts/{account}/transfers",
    params(
        ("account" = String, Path, description = "Account name"),
        ListParams,
        ("token" = Option<String>, Query, description = "Token module, default `coin`"),
    ),
    responses((status = 200, description = "`items` of transfers, `next_cursor`, `as_of`"))
)]
pub async fn transfers(st: St, path: PathS, q: QueryQ) -> Response {
    let (account, q) = match (parse_path(path), parse_query(q)) {
        (Ok(p), Ok(q)) => (p, q),
        (Err(e), _) | (_, Err(e)) => return e.into_response(),
    };
    run(st.0, move |view| {
        let lp = list_params(&q)?;
        let account = account_name(&account)?;
        let token = q.token.as_deref().unwrap_or("coin");
        let d = view.dict();
        let (Some(a), Some(m)) = (d.account_id(account), d.module_id(token)) else {
            return list_response(view, vec![], None);
        };
        let keys = candidates(view, Family::Xfer, xfer_entity(a, m), &lp)?;
        let (items, next) = page_rows(view, keys, &lp, |view, r, blocks| {
            let out = shapes::output(view, r)?;
            let evs = shapes::events(&out);
            let trs = shapes::transfers(&evs);
            let Some(b) = blocks.get(view, r.key.chain(), r.key.height())? else {
                return Ok(vec![]);
            };
            let ctx = tx_context(r, b);
            let mut rows = Vec::new();
            for t in trs.iter().filter(|t| t.token == token && (t.from == account || t.to == account)) {
                let Value::Object(mut row) = transfer_json(r, b, &evs, t) else { continue };
                for (k, v) in &ctx {
                    row.entry(k.clone()).or_insert_with(|| v.clone());
                }
                rows.push((crate::compat::row_sub(t.pos, 0, 0)?, Value::Object(row)));
            }
            Ok(rows)
        })?;
        list_response(view, items, next)
    })
    .await
}

/// Txs touching the account (`role=any`, default) or sent by it (`role=sender`).
#[utoipa::path(
    get,
    path = "/api/v1/accounts/{account}/txs",
    params(
        ("account" = String, Path, description = "Account name"),
        ListParams,
        ("role" = Option<String>, Query, description = "`any` (default) or `sender` (meta sender)"),
    ),
    responses((status = 200, description = "`items` of tx summaries, `next_cursor`, `as_of`"))
)]
pub async fn txs(st: St, path: PathS, q: QueryQ) -> Response {
    let (account, q) = match (parse_path(path), parse_query(q)) {
        (Ok(p), Ok(q)) => (p, q),
        (Err(e), _) | (_, Err(e)) => return e.into_response(),
    };
    let sender_only = match q.role.as_deref() {
        None | Some("any") => false,
        Some("sender") => true,
        Some(_) => return ApiError::BadRequest("role must be any or sender".into()).into_response(),
    };
    run(st.0, move |view| {
        let lp = list_params(&q)?;
        let account = account_name(&account)?;
        let Some(a) = view.dict().account_id(account) else {
            return list_response(view, vec![], None);
        };
        let keys = candidates(view, Family::Acct, u64::from(a), &lp)?;
        let (items, next) = tx_rows(view, keys, &lp, |r| {
            if !sender_only {
                return true;
            }
            shapes::cmd_value(r)
                .and_then(|c| c.pointer("/meta/sender").and_then(Value::as_str).map(|s| s == account))
                .unwrap_or(false)
        })?;
        list_response(view, items, next)
    })
    .await
}

/// Txs signed by a public key.
#[utoipa::path(
    get,
    path = "/api/v1/keys/{pubkey}/txs",
    params(("pubkey" = String, Path, description = "Public key as signed"), ListParams),
    responses((status = 200, description = "`items` of tx summaries, `next_cursor`, `as_of`"))
)]
pub async fn key_txs(st: St, path: PathS, q: QueryQ) -> Response {
    let (pk, q) = match (parse_path(path), parse_query(q)) {
        (Ok(p), Ok(q)) => (p, q),
        (Err(e), _) | (_, Err(e)) => return e.into_response(),
    };
    run(st.0, move |view| {
        let lp = list_params(&q)?;
        if pk.is_empty() || pk.len() > 512 {
            return Err(ApiError::BadRequest("public key must be 1..=512 bytes".into()));
        }
        let Some(id) = view.dict().find(DictKind::PubKey, &pk) else {
            return list_response(view, vec![], None);
        };
        let keys = candidates(view, Family::Pk, u64::from(id), &lp)?;
        let (items, next) = tx_rows(view, keys, &lp, |_| true)?;
        list_response(view, items, next)
    })
    .await
}

/// One summary row per tx that passes `keep`.
fn tx_rows(
    view: &crate::store::View,
    keys: Box<dyn Iterator<Item = ApiResult<kdx_types::TxKey>> + '_>,
    lp: &super::Lp,
    keep: impl Fn(&kdx_types::record::TxRecord) -> bool,
) -> ApiResult<(Vec<Value>, Option<String>)> {
    page_rows(view, keys, lp, |view, r, blocks| {
        if !keep(r) {
            return Ok(vec![]);
        }
        let Some(b) = blocks.get(view, r.key.chain(), r.key.height())? else {
            return Ok(vec![]);
        };
        Ok(vec![(0, summary(view, r, b)?)])
    })
}
