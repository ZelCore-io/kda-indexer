//! Native API v1 (spec §5.1), internal and unstable. JSON in snake_case; lists take `limit`
//! (20, max 100), an opaque `cursor`, `order` (`desc` newest first, default; `asc`), `minheight`,
//! `maxheight` and `chain`. Every response carries `as_of`.
//!
//! Paging: a cursor is a [`Cursor`] (TxKey + position inside the tx + block fingerprint), so it
//! survives a freeze and answers `400 cursor invalidated by reorg` after a reorg. A filtered
//! list scans at most [`SCAN_CAP`] txs per request; a page can therefore come back shorter
//! than `limit` with a non-null `next_cursor`: follow it until it is null.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use kdx_decode::Family;
use kdx_types::record::{BlockRecord, TxRecord};
use kdx_types::{Hash32, TxKey};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use utoipa::IntoParams;

use crate::cursor::Cursor;
use crate::server::{json, ApiState};
use crate::store::{PostingQuery, View};
use crate::{ApiError, ApiResult};

pub mod accounts;
pub mod blocks;
pub mod events;
pub mod stats;
pub mod txs;

pub const DEFAULT_LIMIT: u32 = 20;
pub const MAX_LIMIT: u32 = 100;
/// Txs one filtered list request may examine before it answers with a continuation cursor.
pub const SCAN_CAP: usize = 5_000;
/// Most postings `order=asc` materialises.
pub const ASC_CAP: usize = 200_000;
/// Longest account name the API looks up.
pub const MAX_ACCOUNT_LEN: usize = 256;

/// Superset of every list route's query parameters (each route reads what it documents).
#[derive(Debug, Default, Deserialize)]
pub struct Q {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
    pub order: Option<String>,
    pub minheight: Option<u32>,
    pub maxheight: Option<u32>,
    pub chain: Option<u8>,
    pub chains: Option<String>,
    pub token: Option<String>,
    pub role: Option<String>,
    pub module: Option<String>,
    pub name: Option<String>,
    pub include: Option<String>,
    pub param0: Option<String>,
    pub param1: Option<String>,
    pub param2: Option<String>,
    pub param3: Option<String>,
    pub param4: Option<String>,
    pub param5: Option<String>,
    pub param6: Option<String>,
    pub param7: Option<String>,
    pub param8: Option<String>,
    pub param9: Option<String>,
}

/// Parameters every list route takes (documentation of [`Q`]).
#[derive(IntoParams)]
#[into_params(parameter_in = Query)]
#[allow(dead_code)]
pub struct ListParams {
    /// Page size, 1..=100 (default 20).
    limit: Option<u32>,
    /// Opaque token of the previous page's `next_cursor`.
    cursor: Option<String>,
    /// `desc` (default, newest first) or `asc`.
    order: Option<String>,
    /// Lowest height (inclusive).
    minheight: Option<u32>,
    /// Highest height (inclusive).
    maxheight: Option<u32>,
    /// Only this chain (0..19).
    chain: Option<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Desc,
    Asc,
}

/// Validated list parameters.
pub struct Lp {
    pub limit: usize,
    pub order: Order,
    pub cursor: Option<Cursor>,
    pub min_height: Option<u32>,
    pub max_height: Option<u32>,
    pub chain: Option<u8>,
}

pub fn list_params(q: &Q) -> ApiResult<Lp> {
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT);
    if limit == 0 || limit > MAX_LIMIT {
        return Err(ApiError::BadRequest(format!("limit must be 1..={MAX_LIMIT}")));
    }
    let order = match q.order.as_deref() {
        None | Some("desc") => Order::Desc,
        Some("asc") => Order::Asc,
        Some(_) => return Err(ApiError::BadRequest("order must be asc or desc".into())),
    };
    if let Some(c) = q.chain {
        if u32::from(c) >= kdx_segment::SLOT_WIDTH {
            return Err(ApiError::BadRequest("chain out of range".into()));
        }
    }
    if let (Some(a), Some(b)) = (q.minheight, q.maxheight) {
        if a > b {
            return Err(ApiError::BadRequest("minheight is above maxheight".into()));
        }
    }
    Ok(Lp {
        limit: limit as usize,
        order,
        cursor: q.cursor.as_deref().map(Cursor::decode).transpose()?,
        min_height: q.minheight,
        max_height: q.maxheight,
        chain: q.chain,
    })
}

pub fn account_name(s: &str) -> ApiResult<&str> {
    if s.is_empty() || s.len() > MAX_ACCOUNT_LEN {
        return Err(ApiError::BadRequest(format!(
            "account name must be 1..={MAX_ACCOUNT_LEN} bytes"
        )));
    }
    Ok(s)
}

pub fn parse_hash(s: &str, what: &str) -> ApiResult<Hash32> {
    Hash32::from_b64url(s).map_err(|_| ApiError::BadRequest(format!("invalid {what}")))
}

/// `as_of` of a view: the lowest indexed tip over the chains and the frozen frontier.
pub fn as_of(view: &View) -> ApiResult<Value> {
    let tips = view.chain_tips()?;
    Ok(json!({
        "indexed_tip_min": tips.iter().map(|t| t.1).min(),
        "frozen_frontier": view.seg_frontier,
    }))
}

/// Wrap one resource: its fields plus `as_of`.
pub fn resource(view: &View, mut v: Value) -> ApiResult<Response> {
    if let Value::Object(m) = &mut v {
        m.insert("as_of".into(), as_of(view)?);
    }
    Ok(json(&v))
}

pub fn list_response(view: &View, items: Vec<Value>, next: Option<String>) -> ApiResult<Response> {
    Ok(json(&json!({
        "items": items,
        "next_cursor": next,
        "as_of": as_of(view)?,
    })))
}

pub fn not_implemented() -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        axum::Json(json!({"error": "not implemented until P6"})),
    )
        .into_response()
}

async fn nope() -> Response {
    not_implemented()
}

pub fn router() -> Router<Arc<ApiState>> {
    let v1 = Router::new()
        .route("/status", get(crate::status::handler))
        .route("/stats", get(stats::stats))
        .route("/blocks", get(blocks::list))
        .route("/blocks/{hash}", get(blocks::by_hash))
        .route("/blocks/{hash}/txs", get(blocks::txs))
        .route("/chains/{chain}/blocks/{height}", get(blocks::by_height))
        .route("/txs/{request_key}", get(txs::get_tx))
        .route("/accounts/{account}/transfers", get(accounts::transfers))
        .route("/accounts/{account}/txs", get(accounts::txs))
        .route("/keys/{pubkey}/txs", get(accounts::key_txs))
        .route("/events", get(events::list))
        // P6.
        .route("/txs/{request_key}/defpact", get(nope))
        .route("/accounts/{account}/balances", get(nope))
        .route("/keys/{pubkey}/accounts", get(nope))
        .route("/tokens", get(nope))
        .route("/tokens/{module}", get(nope))
        .route("/search/code", get(nope))
        .route("/search/events", get(nope))
        .route("/nft/accounts/{account}", get(nope));
    Router::new().nest("/api/v1", v1)
}

/// Run a read handler: parse errors and read errors both become responses.
pub async fn run<F>(st: Arc<ApiState>, f: F) -> Response
where
    F: FnOnce(&View) -> ApiResult<Response> + Send + 'static,
{
    match st.read(f).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

pub fn parse_query<T>(r: Result<Query<T>, axum::extract::rejection::QueryRejection>) -> ApiResult<T> {
    r.map(|q| q.0)
        .map_err(|e| ApiError::BadRequest(format!("invalid query: {e}")))
}

pub fn parse_path<T>(r: Result<Path<T>, axum::extract::rejection::PathRejection>) -> ApiResult<T> {
    r.map(|p| p.0)
        .map_err(|e| ApiError::BadRequest(format!("invalid path: {e}")))
}

/// Shared handler state wrapper type.
pub type St = State<Arc<ApiState>>;

// ---------------------------------------------------------------------------------------
// Paging over tx candidates

/// Rows of one tx: `(sub, row)`, ascending by `sub`.
pub type TxRows = Vec<(u16, Value)>;

/// Block cache of one request (rows need the block time and hash).
#[derive(Default)]
pub struct Blocks(HashMap<(u8, u32), Option<BlockRecord>>);

impl Blocks {
    pub fn get(&mut self, view: &View, c: u8, h: u32) -> ApiResult<Option<&BlockRecord>> {
        if self.0.len() > 512 {
            self.0.clear();
        }
        use std::collections::hash_map::Entry;
        Ok(match self.0.entry((c, h)) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(v) => v.insert(view.block(c, h)?),
        }
        .as_ref())
    }
}

/// Candidate txs of a posting entity in `order`, honouring the cursor.
pub fn candidates<'v>(
    view: &'v View,
    f: Family,
    entity: u64,
    lp: &Lp,
) -> ApiResult<Box<dyn Iterator<Item = ApiResult<TxKey>> + 'v>> {
    if let Some(c) = &lp.cursor {
        c.check(view)?;
    }
    let mut q = PostingQuery {
        min_height: lp.min_height,
        max_height: lp.max_height,
        chain: lp.chain,
        before: None,
    };
    match lp.order {
        Order::Desc => {
            // Keys up to and including the cursor's tx (it may have rows left).
            q.before = lp.cursor.as_ref().map(|c| TxKey(c.key.0.saturating_add(0x100)));
            view.postings(f, entity, q)
        }
        Order::Asc => {
            let mut all = Vec::new();
            for k in view.postings(f, entity, q)? {
                all.push(k?);
                if all.len() > ASC_CAP {
                    return Err(ApiError::BadRequest(format!(
                        "order=asc matches more than {ASC_CAP} txs: narrow minheight/maxheight"
                    )));
                }
            }
            all.reverse();
            let from = lp.cursor.as_ref().map_or(TxKey(0), |c| c.key);
            let start = all.partition_point(|k| *k < from);
            Ok(Box::new(all.into_iter().skip(start).map(Ok)))
        }
    }
}

/// Fill a page from `keys`: `rows` yields the rows of each tx. Stops after `lp.limit` rows (a
/// further row proves there is more) or after [`SCAN_CAP`] examined txs.
pub fn page_rows(
    view: &View,
    keys: impl Iterator<Item = ApiResult<TxKey>>,
    lp: &Lp,
    mut rows: impl FnMut(&View, &TxRecord, &mut Blocks) -> ApiResult<TxRows>,
) -> ApiResult<(Vec<Value>, Option<String>)> {
    let mut blocks = Blocks::default();
    let mut items = Vec::new();
    let mut last_emitted: Option<(TxKey, u16)> = None;
    let mut last_scanned: Option<TxKey> = None;
    let mut capped = false;
    let mut more = false;
    'keys: for (scanned, k) in keys.enumerate() {
        let k = k?;
        if scanned == SCAN_CAP {
            capped = true;
            break;
        }
        last_scanned = Some(k);
        let Some(r) = view.tx(k)? else { continue };
        for (sub, row) in rows(view, &r, &mut blocks)? {
            if lp.cursor.as_ref().is_some_and(|c| c.key == k && sub < c.sub) {
                continue;
            }
            if items.len() == lp.limit {
                more = true;
                break 'keys;
            }
            items.push(row);
            last_emitted = Some((k, sub.saturating_add(1)));
        }
    }
    let next = if more {
        last_emitted
    } else if capped {
        last_scanned.map(|k| (k, u16::MAX))
    } else {
        None
    };
    let next = next
        .map(|(k, sub)| Cursor::new(view, k, sub).map(|c| c.encode()))
        .transpose()?;
    Ok((items, next))
}

/// `base` merged with the block context every row carries.
pub fn tx_context(r: &TxRecord, b: &BlockRecord) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert(
        "request_key".into(),
        match &r.request_key {
            Some(h) if !r.key.is_coinbase() => h.to_b64url().into(),
            _ => Value::Null,
        },
    );
    m.insert("chain".into(), u64::from(r.key.chain()).into());
    m.insert("height".into(), u64::from(r.key.height()).into());
    m.insert("block_hash".into(), b.hash.to_b64url().into());
    m.insert(
        "block_time".into(),
        crate::compat::shapes::utc_micros(b.creation_time).into(),
    );
    m.insert("tx_index".into(), u64::from(r.key.pos()).into());
    m
}
