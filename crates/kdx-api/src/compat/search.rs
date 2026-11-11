//! `/txs/search` (chainweb-data `searchTxs`): `search` finds txs whose own exec code contains
//! the pattern (`LIKE '%search%'`, `%` and `_` wildcards, ruling S-4); `pactid` lists the
//! `cont` txs of a defpact.
//!
//! `search`: `[TxSummary]`, height desc then requestKey desc, limit 10 (max 250), offset up
//! to 10,000 or `next`, `Chainweb-Next`. Candidates come from the code index (3-gram
//! prefilter, verified by the LIKE matcher, expanded through the `code` postings) when the
//! pattern has a literal run of 3 or more characters and the index holds every body it can
//! match; otherwise from the bounded newest-first scan (`search.max_scan_heights`,
//! `search.timeout_ms`). Either path answers what it found with `X-Search-Truncated: true`
//! when the deadline or a cap cut it, plus `Chainweb-Next` when there is more to read.
//! chainweb-data also matches `cont` txs through their initial tx's code; this route does
//! not (S-3, allowlist class `search-cont-history`).
//!
//! `pactid`: the `cont` txs whose `payload.cont.pactId` is the given one (the starter is
//! excluded), successful first, then height desc (ties: request key ascending), limit 50
//! (max 50, the newest 1,000 txs of the pact considered, R-P5P9-3); height and paging
//! parameters are ignored and no `Chainweb-Next` is sent, as chainweb-data does.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use kdx_types::record::{BlockRecord, TxRecord};
use kdx_types::{Hash32, TxKey};
use serde_json::Value;

use super::{
    aeson, check_strategy, collect_page, hidden, int_param, page_response, paging, recent,
    resume_exhausted, resume_max, shapes, store_floor, text, Candidates, Like, Page, Paging,
    Params, RowSource, ScanLimits,
};
use crate::search::{check_pattern, index_covers, rejected, ClientIp, SearchCtx};
use crate::server::ApiState;
use crate::store::{CodeHit, PostingQuery, View};
use crate::ApiResult;

/// `pactid` rows answered at most (chainweb-data's limit).
pub const PACT_LIMIT: i128 = 50;
/// Txs of one defpact read at most (R-P5P9-3): the newest ones.
pub const PACT_CAP: usize = 1_000;

pub async fn handler(
    State(st): State<Arc<ApiState>>,
    Extension(ctx): Extension<Arc<SearchCtx>>,
    ClientIp(ip): ClientIp,
    headers: HeaderMap,
    Query(q): Query<Vec<(String, String)>>,
) -> Response {
    let p = Params::new(q);
    // servant parses every declared parameter before the handler runs.
    for k in ["limit", "offset", "minheight", "maxheight"] {
        if let Err(r) = int_param(&p, k) {
            return r;
        }
    }
    match (p.get("search"), p.get("pactid")) {
        (None, None) => text(StatusCode::NOT_FOUND, "You must specify a search string"),
        (Some(_), Some(_)) => text(
            StatusCode::BAD_REQUEST,
            "You should only specify a pactid or a search string, not both!",
        ),
        (None, Some(pid)) => by_pact(st, &p, pid.to_owned()).await,
        (Some(s), None) => {
            let pg = match paging(&p) {
                Ok(pg) => pg,
                Err(r) => return r,
            };
            if let Err(r) = check_strategy(&headers) {
                return r;
            }
            if let Err(t) = check_pattern("search", s) {
                return text(StatusCode::BAD_REQUEST, t);
            }
            let permit = match ctx.gate.try_acquire(ip) {
                Ok(p) => p,
                Err(r) => return r,
            };
            let raw = s.to_owned();
            let res = st
                .read_heavy(move |view| {
                    let _permit = permit;
                    search_page(view, &ctx, &raw, &pg)
                })
                .await;
            match res {
                Ok(page) => page_response(page),
                Err(e) => e.into_response(),
            }
        }
    }
}

/// One `TxSummary` row per user tx; with `like`, only txs whose own exec code matches it
/// (the scan path; index candidates are verified already).
struct Summaries {
    like: Option<Like>,
}

impl RowSource for Summaries {
    fn rows(&mut self, _view: &View, r: &TxRecord) -> ApiResult<Vec<(u16, i64)>> {
        if r.key.is_coinbase() || r.request_key.is_none() {
            return Ok(Vec::new());
        }
        let Some(code) = kdx_search::exec_code(r) else {
            return Ok(Vec::new());
        };
        if self.like.as_ref().is_some_and(|l| !l.matches(&code)) {
            return Ok(Vec::new());
        }
        Ok(vec![(0, 0)])
    }

    fn idx_of_sub(&self, _k: TxKey, _sub: u16) -> i64 {
        0
    }

    fn render(
        &mut self,
        view: &View,
        r: &TxRecord,
        b: &BlockRecord,
        _sub: u16,
    ) -> ApiResult<Value> {
        match shapes::tx_detail(view, r, b)? {
            Some(d) => Ok(recent::summary(&d)),
            None => Err(crate::ApiError::Internal(anyhow::anyhow!(
                "search row {:?} without a parsed cmd",
                r.key
            ))),
        }
    }
}

/// What the code index offers for a pattern.
pub enum IndexPath {
    /// Verified candidate bodies, and whether the candidate cap cut them.
    Hits {
        hits: Vec<CodeHit>,
        rebuilt_through: Option<u32>,
        capped: bool,
    },
    /// The candidate query ran out of time.
    Timeout,
    /// No index, or it cannot answer this pattern completely: scan.
    None,
}

/// Ask the code index for `raw` (`complete`: refuse a fallback-mode index that may miss
/// short bodies).
pub fn index_path(ctx: &SearchCtx, raw: &str, until: Instant, complete: bool) -> IndexPath {
    let Some(s) = ctx.index.get() else {
        return IndexPath::None;
    };
    if complete && !index_covers(&s.manifest, raw) {
        return IndexPath::None;
    }
    match s.candidates(&Like::new(raw), until) {
        Ok(c) => {
            if c.truncated {
                rejected("cap");
            }
            let clamp = |h: u64| u32::try_from(h).unwrap_or(u32::MAX);
            IndexPath::Hits {
                hits: c
                    .hashes
                    .iter()
                    .map(|(hash, f, l)| CodeHit {
                        hash: *hash,
                        first_h: clamp(*f),
                        last_h: clamp(*l),
                    })
                    .collect(),
                rebuilt_through: s.manifest.rebuilt_through_height,
                capped: c.truncated,
            }
        }
        Err(kdx_search::SearchError::Timeout) => {
            rejected("timeout");
            IndexPath::Timeout
        }
        Err(kdx_search::SearchError::NoIndexPath) => IndexPath::None,
        Err(kdx_search::SearchError::Other(e)) => {
            tracing::warn!(
                error = format!("{e:#}"),
                "search: index query failed, scanning"
            );
            IndexPath::None
        }
    }
}

fn search_page(view: &View, ctx: &SearchCtx, raw: &str, pg: &Paging) -> ApiResult<Page> {
    let empty = |truncated| Page {
        rows: Vec::new(),
        next: None,
        truncated,
    };
    if pg.empty || resume_exhausted(pg) {
        return Ok(empty(false));
    }
    if let Some(c) = pg.next {
        c.check(view)?;
    }
    let max_h = resume_max(pg);
    let set = ctx.settings;
    let until = (Instant::now() + Duration::from_millis(set.timeout_ms)).min(view.deadline);
    let limits = ScanLimits {
        max_heights: set.max_scan_heights.max(1),
        until,
    };
    match index_path(ctx, raw, until, true) {
        IndexPath::Hits {
            hits,
            rebuilt_through,
            capped,
        } => {
            let q = PostingQuery {
                min_height: pg.min_height,
                max_height: max_h,
                chain: None,
                before: None,
            };
            let keys = view.code_txs(&hits, rebuilt_through, q)?;
            let mut src = Summaries { like: None };
            let mut page = collect_page(
                view,
                Candidates::Keys(keys.peekable()),
                &mut src,
                pg,
                Some(limits),
            )?;
            if page.truncated {
                rejected("timeout");
            }
            page.truncated |= capped;
            Ok(page)
        }
        // Nothing found in time: no token (it would repeat the same query).
        IndexPath::Timeout => Ok(empty(true)),
        IndexPath::None => {
            let floor = store_floor(view).max(pg.min_height.unwrap_or(0));
            let top = match (view.max_data_height()?, max_h) {
                (None, _) => return Ok(empty(false)),
                (Some(t), Some(m)) => t.min(m),
                (Some(t), None) => t,
            };
            if top < floor {
                return Ok(empty(false));
            }
            let mut src = Summaries {
                like: Some(Like::new(raw)),
            };
            let page = collect_page(
                view,
                Candidates::Scan {
                    next: Some(top),
                    floor,
                },
                &mut src,
                pg,
                Some(limits),
            )?;
            if page.truncated {
                rejected(if Instant::now() >= until {
                    "timeout"
                } else {
                    "cap"
                });
            }
            Ok(page)
        }
    }
}

/// `pactid`: chainweb-data's `queryTxsByPactId`.
async fn by_pact(st: Arc<ApiState>, p: &Params, pid: String) -> Response {
    let limit = match int_param(p, "limit") {
        Ok(l) => l.map_or(PACT_LIMIT, |l| l.min(PACT_LIMIT)),
        Err(r) => return r,
    };
    if limit < 0 {
        // A negative LIMIT reaches Postgres in chainweb-data.
        return text(StatusCode::INTERNAL_SERVER_ERROR, "Something went wrong");
    }
    // chainweb-data compares the text with its pactid column: a value that is not a hash
    // matches nothing.
    let pact = match Hash32::from_b64url(&pid) {
        Ok(h) if pid.len() <= 64 => h,
        _ => return aeson(&Value::Array(Vec::new())),
    };
    let limit = limit as usize;
    let res = st
        .read(move |view| {
            let (keys, _capped) = view.pact_txs(&pact, PACT_CAP)?;
            let mut rows: Vec<(bool, TxKey, Vec<u8>, Value)> = Vec::new();
            for (i, (k, cont)) in keys.into_iter().enumerate() {
                if i % 64 == 63 {
                    view.check_deadline()?;
                }
                if !cont || hidden(view, k) {
                    continue;
                }
                let (Some(r), Some(b)) = (view.tx(k)?, view.block(k.chain(), k.height())?) else {
                    continue;
                };
                if let Some(d) = shapes::tx_detail(view, &r, &b)? {
                    let ok = d.get("success").and_then(Value::as_bool) == Some(true);
                    rows.push((ok, k, shapes::rk_order(&r), recent::summary(&d)));
                }
            }
            // Successful first, then height desc. chainweb-data leaves ties to Postgres; live
            // it answered them request key ascending (2 of 2 tied pairs, 2026-11-11).
            rows.sort_by(|a, b| {
                b.0.cmp(&a.0)
                    .then(b.1.height().cmp(&a.1.height()))
                    .then(a.2.cmp(&b.2))
                    .then(a.1.chain().cmp(&b.1.chain()))
            });
            rows.truncate(limit);
            Ok(rows.into_iter().map(|r| r.3).collect::<Vec<_>>())
        })
        .await;
    match res {
        Ok(v) => aeson(&Value::Array(v)),
        Err(e) => e.into_response(),
    }
}
