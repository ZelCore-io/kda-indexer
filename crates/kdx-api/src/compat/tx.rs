//! `/txs/tx` and `/txs/txs` (chainweb-data `txHandler` / `txsHandler`).

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use kdx_types::Hash32;
use serde_json::Value;

use super::{aeson, hidden, shapes, text, Params};
use crate::server::ApiState;
use crate::store::View;
use crate::ApiResult;

const NO_KEY: &str = "You must specify a search string";
const NOT_FOUND: &str = "Tx not found";

/// TxDetails of a request key (canonical only, so at most one).
fn lookup(view: &View, rk: &str) -> ApiResult<Vec<Value>> {
    let Ok(h) = Hash32::from_b64url(rk) else {
        return Ok(Vec::new());
    };
    let Some((k, r)) = view.tx_by_rk(&h)? else {
        return Ok(Vec::new());
    };
    if hidden(k) || k.is_coinbase() {
        return Ok(Vec::new());
    }
    let Some(b) = view.block(k.chain(), k.height())? else {
        return Ok(Vec::new());
    };
    Ok(shapes::tx_detail(view, &r, &b)?.into_iter().collect())
}

async fn run(st: Arc<ApiState>, q: Vec<(String, String)>, many: bool) -> Response {
    let p = Params::new(q);
    let Some(rk) = p.get("requestkey").map(str::to_owned) else {
        return text(StatusCode::NOT_FOUND, NO_KEY);
    };
    match st.read(move |v| lookup(v, &rk)).await {
        Ok(v) if v.is_empty() => text(StatusCode::NOT_FOUND, NOT_FOUND),
        Ok(mut v) if !many => aeson(&v.swap_remove(0)),
        Ok(v) => aeson(&Value::Array(v)),
        Err(e) => e.into_response(),
    }
}

pub async fn tx_handler(
    State(st): State<Arc<ApiState>>,
    Query(q): Query<Vec<(String, String)>>,
) -> Response {
    run(st, q, false).await
}

pub async fn txs_handler(
    State(st): State<Arc<ApiState>>,
    Query(q): Query<Vec<(String, String)>>,
) -> Response {
    run(st, q, true).await
}
