//! Fungible tokens from the additive `Tokens` table (spec §3.9, plan Task 9): `/tokens` and
//! `/tokens/{module}`. A module is listed once it is the module of a `TRANSFER` event (an
//! `xfer` posting), unless it is marked not fungible (R-P5P9-8 #2). `holders` counts reconciled rows with a balance above zero; it is
//! approximate (`holders_approx: true`). A store without the table (A-1) lists nothing.

use std::collections::BinaryHeap;

use axum::extract::{Path, Query};
use axum::response::{IntoResponse, Response};
use kdx_hot::balances::{TokenRow, KIND_NOT_FUNGIBLE};
use kdx_hot::{Table, Txn};
use kdx_types::{Dict, DictKind};
use serde_json::{json, Value};

use super::{list_response, parse_path, parse_query, resource, run, St, Q};
use crate::{ApiError, ApiResult};

type PathS = Result<Path<String>, axum::extract::rejection::PathRejection>;
type QueryQ = Result<Query<Q>, axum::extract::rejection::QueryRejection>;

/// `Tokens` rows per read during a listing.
const CHUNK: usize = 10_000;

fn token_json(module: &str, t: &TokenRow) -> Value {
    json!({
        "module": module,
        "first_height": (t.first_height != u32::MAX).then_some(t.first_height),
        "holders": t.holders,
        "holders_approx": true,
    })
}

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<String> {
    if !s.len().is_multiple_of(2) || s.len() > 2048 {
        return None;
    }
    let b: Option<Vec<u8>> = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect();
    String::from_utf8(b?).ok()
}

/// Fungible tokens by module name, paged (M-5): `limit` 1..=100 (default 20), `cursor` =
/// `next_cursor`. Modules marked not fungible (R-P5P9-8 #2) are left out.
#[utoipa::path(
    get,
    path = "/api/v1/tokens",
    params(
        ("limit" = Option<u32>, Query, description = "1..=100, default 20"),
        ("cursor" = Option<String>, Query, description = "Opaque page cursor"),
    ),
    responses((status = 200, description = "`items` of {module, first_height, holders, holders_approx}, `next_cursor`, `as_of`"))
)]
pub async fn list(st: St, q: QueryQ) -> Response {
    let q = match parse_query(q) {
        Ok(q) => q,
        Err(e) => return e.into_response(),
    };
    run(st.0, move |view| {
        let limit = q.limit.unwrap_or(super::DEFAULT_LIMIT) as usize;
        if limit == 0 || limit > super::MAX_LIMIT as usize {
            return Err(ApiError::BadRequest(format!(
                "limit must be 1..={}",
                super::MAX_LIMIT
            )));
        }
        let after = match q.cursor.as_deref() {
            None => None,
            Some(c) => Some(unhex(c).ok_or_else(|| ApiError::BadRequest("invalid cursor".into()))?),
        };
        let d = view.dict();
        // The `limit + 1` smallest names above the cursor.
        let mut heap: BinaryHeap<(String, Vec<u8>)> = BinaryHeap::new();
        let mut from: Option<Vec<u8>> = None;
        loop {
            view.check_deadline()?;
            let rows = view
                .txn
                .scan_chunk(Table::Tokens, from.as_deref(), CHUNK, |_| true)?;
            for (k, v) in &rows {
                let (Ok(id), Some(t)) = (<[u8; 4]>::try_from(k.as_slice()), TokenRow::decode(v))
                else {
                    continue;
                };
                if t.kind == KIND_NOT_FUNGIBLE {
                    continue;
                }
                let Some(name) = d.name(DictKind::Module, u32::from_be_bytes(id)) else {
                    continue;
                };
                if after.as_deref().is_some_and(|a| name <= a) {
                    continue;
                }
                heap.push((name.to_owned(), v.clone()));
                if heap.len() > limit + 1 {
                    heap.pop();
                }
            }
            match rows.last() {
                Some((k, _)) if rows.len() == CHUNK => from = Some(k.clone()),
                _ => break,
            }
        }
        let mut page = heap.into_sorted_vec();
        let more = page.len() > limit;
        page.truncate(limit);
        let next = more.then(|| page.last().map(|(n, _)| hex(n))).flatten();
        let items = page
            .iter()
            .filter_map(|(n, v)| Some(token_json(n, &TokenRow::decode(v)?)))
            .collect();
        list_response(view, items, next)
    })
    .await
}

/// One fungible token.
#[utoipa::path(
    get,
    path = "/api/v1/tokens/{module}",
    params(("module" = String, Path, description = "Token module, e.g. `coin`")),
    responses(
        (status = 200, description = "{module, first_height, holders, holders_approx, as_of}"),
        (status = 404, description = "Not a known token")
    )
)]
pub async fn get(st: St, path: PathS) -> Response {
    let module = match parse_path(path) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    run(st.0, move |view| -> ApiResult<Response> {
        let not_found = || ApiError::NotFound(format!("token {module} not found"));
        let id = view.dict().module_id(&module).ok_or_else(not_found)?;
        let t = view
            .txn
            .get(Table::Tokens, &id.to_be_bytes())?
            .as_deref()
            .and_then(TokenRow::decode)
            .filter(|t| t.kind != KIND_NOT_FUNGIBLE)
            .ok_or_else(not_found)?;
        resource(view, token_json(&module, &t))
    })
    .await
}
