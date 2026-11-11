//! `/txs/tx` and `/txs/txs` (chainweb-data `txHandler` / `txsHandler`).

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use kdx_types::Hash32;
use serde_json::Value;

use super::{aeson, shapes, text, Params};
use crate::server::ApiState;
use crate::store::View;
use crate::ApiResult;

const NO_KEY: &str = "You must specify a search string";
const NOT_FOUND: &str = "Tx not found";

/// TxDetails of a request key, one per stored location (canonical only). Only genesis txs
/// have several: their request key repeats across chains (F4).
fn lookup(view: &View, rk: &str) -> ApiResult<Vec<Value>> {
    let Ok(h) = Hash32::from_b64url(rk) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for (k, r) in view.txs_by_rk(&h)? {
        // Genesis txs are served here, as chainweb-data does (G1).
        if k.is_coinbase() {
            continue;
        }
        let Some(b) = view.block(k.chain(), k.height())? else {
            continue;
        };
        out.extend(shapes::tx_detail(view, &r, &b)?);
    }
    Ok(out)
}

/// chainweb-data's `txHandler` answers only a request key with exactly one row; `txsHandler`
/// lists every row. Both answer 404 "Tx not found" when there is none.
fn answer(mut rows: Vec<Value>, many: bool) -> Option<Value> {
    match (rows.len(), many) {
        (0, _) => None,
        (_, true) => Some(Value::Array(rows)),
        (1, false) => rows.pop(),
        (_, false) => None,
    }
}

async fn run(st: Arc<ApiState>, q: Vec<(String, String)>, many: bool) -> Response {
    let p = Params::new(q);
    let Some(rk) = p.get("requestkey").map(str::to_owned) else {
        return text(StatusCode::NOT_FOUND, NO_KEY);
    };
    match st.read(move |v| lookup(v, &rk)).await {
        Ok(v) => match answer(v, many) {
            Some(x) => aeson(&x),
            None => text(StatusCode::NOT_FOUND, NOT_FOUND),
        },
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::answer;

    /// F4: a request key with rows on several chains is listed by `/txs/txs` and is a 404 on
    /// `/txs/tx`, as in chainweb-data.
    #[test]
    fn several_rows() {
        let rows = vec![json!({"chain": 0}), json!({"chain": 1})];
        assert_eq!(answer(rows.clone(), true), Some(json!(rows)));
        assert_eq!(answer(rows, false), None);
        assert_eq!(
            answer(vec![json!({"chain": 3})], false),
            Some(json!({"chain": 3}))
        );
        assert_eq!(
            answer(vec![json!({"chain": 3})], true),
            Some(json!([{"chain": 3}]))
        );
        assert_eq!(answer(Vec::new(), true), None);
        assert_eq!(answer(Vec::new(), false), None);
    }
}
