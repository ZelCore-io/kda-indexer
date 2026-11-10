//! `/txs/recent` (chainweb-data `recentTxsHandler`): the ten newest user txs as
//! `TxSummary` records, height descending. chainweb-data leaves the order inside a height
//! to Postgres; here it is request key descending, as everywhere else.

use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use serde_json::{Map, Value};

use super::{aeson, hidden, shapes, store_floor};
use crate::server::ApiState;
use crate::store::View;
use crate::ApiResult;

/// Number of txs answered.
const RECENT: usize = 10;
/// Heights walked at most when the newest ones hold few user txs.
const MAX_HEIGHTS: u32 = 20_000;

/// `TxSummary` fields of a `TxDetail`.
fn summary(d: &Value) -> Value {
    let mut m = Map::new();
    for k in [
        "blockHash",
        "chain",
        "code",
        "continuation",
        "creationTime",
        "height",
        "initialCode",
        "previousSteps",
        "requestKey",
        "sender",
    ] {
        m.insert(k.into(), d.get(k).cloned().unwrap_or(Value::Null));
    }
    let ok = d.get("success").and_then(Value::as_bool) == Some(true);
    m.insert(
        "result".into(),
        if ok { "TxSucceeded" } else { "TxFailed" }.into(),
    );
    Value::Object(m)
}

fn recent(view: &View) -> ApiResult<Vec<Value>> {
    let mut out = Vec::new();
    let Some(mut h) = view.max_indexed_height()? else {
        return Ok(out);
    };
    let floor = store_floor(view);
    for _ in 0..MAX_HEIGHTS {
        view.check_deadline()?;
        let mut txs = Vec::new();
        for c in 0..kdx_segment::SLOT_WIDTH as u8 {
            for r in view.block_txs(c, h)? {
                if !r.key.is_coinbase() && !hidden(view, r.key) {
                    txs.push(r);
                }
            }
        }
        txs.sort_by_key(|r| std::cmp::Reverse(shapes::rk_order(r)));
        for r in txs {
            let Some(b) = view.block(r.key.chain(), r.key.height())? else {
                continue;
            };
            if let Some(d) = shapes::tx_detail(view, &r, &b)? {
                out.push(summary(&d));
                if out.len() == RECENT {
                    return Ok(out);
                }
            }
        }
        if h <= floor {
            break;
        }
        h -= 1;
    }
    Ok(out)
}

pub async fn handler(State(st): State<Arc<ApiState>>) -> Response {
    match st.read(recent).await {
        Ok(v) => aeson(&Value::Array(v)),
        Err(e) => e.into_response(),
    }
}
