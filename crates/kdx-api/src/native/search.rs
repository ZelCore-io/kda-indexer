//! `GET /api/v1/search/code` and `GET /api/v1/search/events` (plan Task 10, spec §3.8).
//!
//! Both take `q` (a LIKE pattern: `%` and `_` wildcards, `\` escapes; matched anywhere in the
//! text), `limit` (1..=`min(100, search.max_results)`), `cursor`, `minheight`, `maxheight`,
//! newest first only. `q` needs `search.min_len` characters other than wildcards. Both run
//! under the search gate (S-5): `503` when `search.concurrency` searches run, `429` when this
//! client already runs `search.per_client`. Answers are `{items, next_cursor, truncated,
//! as_of}`: `truncated` means the deadline or a cap cut the answer; follow `next_cursor`
//! until it is null.
//!
//! `/search/code` reads the code index only: `q` needs a run of 3 or more characters without
//! wildcards (`400` otherwise), and a store with no index answers `503`. Items are tx
//! summaries as `/accounts/{a}/txs` answers them. In fallback mode (spec §3.8, S-1) short
//! bodies are not indexed: `partial: true` says the pattern could match some of them. It
//! also says the index trails the data tip (a lagging tail stage): newer bodies may be
//! missing.
//!
//! `/search/events` is the compat bounded event scan (`/txs/events?search=`) in the native
//! shape: events whose qualified name or parameter text contains `q`.

use std::cell::Cell;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::Query;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use kdx_types::like::Like;
use kdx_types::TxKey;
use serde_json::json;

use super::{
    as_of, list_params, page_rows, parse_query, txs::summary, ListParams, Lp, Order, St, Q,
};
use crate::compat::search::{half_left, index_path, IndexPath};
use crate::compat::Paging;
use crate::cursor::Cursor;
use crate::search::{check_pattern, index_covers, literal_chars, rejected, ClientIp, SearchCtx};
use crate::server::json;
use crate::store::{code_hit, PostingQuery};
use crate::{ApiError, ApiResult};

type QueryQ = Result<Query<Q>, axum::extract::rejection::QueryRejection>;

/// Validate `q` and the list parameters (`Err`: the 400).
fn inputs(q: &Q, ctx: &SearchCtx) -> ApiResult<(String, Lp)> {
    let pat =
        q.q.clone()
            .ok_or_else(|| ApiError::BadRequest("q is required".into()))?;
    check_pattern("q", &pat).map_err(ApiError::BadRequest)?;
    let min = ctx.settings.min_len;
    if literal_chars(&pat) < min {
        rejected("too_short");
        return Err(ApiError::BadRequest(format!(
            "q must have at least {min} characters other than wildcards"
        )));
    }
    let lp = list_params(q)?;
    let max = super::MAX_LIMIT.min(u32::try_from(ctx.settings.max_results).unwrap_or(u32::MAX));
    if lp.limit > max as usize {
        return Err(ApiError::BadRequest(format!("limit must be 1..={max}")));
    }
    if lp.order == Order::Asc {
        return Err(ApiError::BadRequest(
            "search lists are newest first only".into(),
        ));
    }
    Ok((pat, lp))
}

/// Code search: txs whose exec code contains `q`.
#[utoipa::path(
    get,
    path = "/api/v1/search/code",
    params(
        ("q" = String, Query, description = "LIKE pattern matched anywhere in the exec code; needs search.min_len characters other than wildcards and a run of 3 without wildcards"),
        ListParams,
    ),
    responses(
        (status = 200, description = "`items` of tx summaries, `next_cursor`, `truncated`, `partial`, `as_of`"),
        (status = 400, description = "q missing, too short, too long or without a 3-character run"),
        (status = 429, description = "This client already runs a search"),
        (status = 503, description = "Too many searches in flight, or no code index built"),
    )
)]
pub async fn code(
    st: St,
    Extension(ctx): Extension<Arc<SearchCtx>>,
    ClientIp(ip): ClientIp,
    q: QueryQ,
) -> Response {
    let q = match parse_query(q) {
        Ok(q) => q,
        Err(e) => return e.into_response(),
    };
    let (pat, lp) = match inputs(&q, &ctx) {
        Ok(x) => x,
        Err(e) => return e.into_response(),
    };
    if kdx_search::grams(&Like::new(&pat)).is_none() {
        rejected("too_short");
        return ApiError::BadRequest(
            "q needs a run of 3 or more characters without wildcards".into(),
        )
        .into_response();
    }
    let permit = match ctx.gate.try_acquire(ip) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let res =
        st.0.read_heavy(move |view| {
            let _permit = permit;
            let until = (Instant::now() + Duration::from_millis(ctx.settings.timeout_ms))
                .min(view.deadline);
            // Keys up to and including the cursor's tx (it may have rows left).
            let before = lp
                .cursor
                .as_ref()
                .map(|c| TxKey(c.key.0.saturating_add(0x100)));
            let max_h = match (lp.max_height, before) {
                (Some(m), Some(b)) => Some(m.min(b.height())),
                (m, b) => m.or(b.map(|b| b.height())),
            };
            let window = (lp.min_height, max_h);
            let (hits, rebuilt_through, capped, lagging) =
                match index_path(view, &ctx, &pat, window, until, false) {
                    IndexPath::Hits {
                        hits,
                        rebuilt_through,
                        capped,
                        lagging,
                    } => (hits, rebuilt_through, capped, lagging),
                    IndexPath::Timeout => {
                        return Ok(json(&json!({
                            "items": [], "next_cursor": null, "truncated": true,
                            "partial": false, "as_of": as_of(view)?,
                        })))
                    }
                    IndexPath::None => {
                        return Err(ApiError::Unavailable("no code index is built".into()))
                    }
                };
            let partial = lagging
                || ctx
                    .index
                    .get()
                    .is_some_and(|s| !index_covers(&s.manifest, &pat));
            if let Some(c) = &lp.cursor {
                c.check(view)?;
            }
            let pq = PostingQuery {
                min_height: lp.min_height,
                max_height: lp.max_height,
                chain: lp.chain,
                before,
            };
            // Starting the walks and reading rows each get half of what is left of the
            // request deadline (at least the search budget).
            let (keys, cut_walks) = view.code_txs(&hits, rebuilt_through, pq, half_left(view))?;
            let until = until.max(half_left(view));
            let hashes: HashSet<[u8; 16]> = hits.iter().map(|h| h.hash).collect();
            // Stop pulling keys at the search deadline; resume after the last one read.
            let last: Cell<Option<TxKey>> = Cell::new(None);
            let cut: Cell<Option<TxKey>> = Cell::new(None);
            let timed = keys.map_while(|r| {
                if last.get().is_some() && Instant::now() >= until {
                    cut.set(last.get());
                    return None;
                }
                if let Ok(k) = &r {
                    last.set(Some(*k));
                }
                Some(r)
            });
            let (items, mut next) = page_rows(view, timed, &lp, |view, r, blocks| {
                if !code_hit(r, &hashes) {
                    return Ok(vec![]);
                }
                let Some(b) = blocks.get(view, r.key.chain(), r.key.height())? else {
                    return Ok(vec![]);
                };
                Ok(vec![(0, summary(view, r, b)?)])
            })?;
            if cut_walks {
                rejected("timeout");
            }
            let mut truncated = capped || cut_walks;
            if next.is_none() {
                if let Some(k) = cut.get() {
                    rejected("timeout");
                    truncated = true;
                    next = Some(Cursor::new(view, k, u16::MAX)?.encode());
                }
            }
            Ok(json(&json!({
                "items": items,
                "next_cursor": next,
                "truncated": truncated,
                "partial": partial,
                "as_of": as_of(view)?,
            })))
        })
        .await;
    match res {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

/// Event search: events whose qualified name or parameter text contains `q` (bounded scan).
#[utoipa::path(
    get,
    path = "/api/v1/search/events",
    params(
        ("q" = String, Query, description = "LIKE pattern matched anywhere in the qualified event name or the parameter text; needs search.min_len characters other than wildcards"),
        ListParams,
    ),
    responses(
        (status = 200, description = "`items` of events with their tx context, `next_cursor`, `truncated`, `as_of`"),
        (status = 400, description = "q missing, too short or too long"),
        (status = 429, description = "This client already runs a search"),
        (status = 503, description = "Too many searches in flight"),
    )
)]
pub async fn events(
    st: St,
    Extension(ctx): Extension<Arc<SearchCtx>>,
    ClientIp(ip): ClientIp,
    q: QueryQ,
) -> Response {
    let q = match parse_query(q) {
        Ok(q) => q,
        Err(e) => return e.into_response(),
    };
    let (pat, lp) = match inputs(&q, &ctx) {
        Ok(x) => x,
        Err(e) => return e.into_response(),
    };
    if lp.chain.is_some() {
        return ApiError::BadRequest("chain is not supported on /search/events".into())
            .into_response();
    }
    let permit = match ctx.gate.try_acquire(ip) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let res =
        st.0.read_heavy(move |view| {
            let _permit = permit;
            let pg = Paging {
                limit: lp.limit,
                offset: 0,
                next: lp.cursor,
                min_height: lp.min_height,
                max_height: lp.max_height,
                empty: false,
            };
            let page =
                crate::compat::events::search_scan(view, Like::new(&pat), &pg, ctx.settings, true)?;
            Ok(json(&json!({
                "items": page.rows,
                "next_cursor": page.next.map(|c| c.encode()),
                "truncated": page.truncated,
                "as_of": as_of(view)?,
            })))
        })
        .await;
    match res {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}
