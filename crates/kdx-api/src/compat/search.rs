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
//! Genesis-block txs are listed, as chainweb-data does (6E-F); the other compat list routes
//! hide them. chainweb-data also matches `cont` txs through their initial tx's code; this route does
//! not (S-3, allowlist class `search-cont-history`).
//!
//! `pactid`: the `cont` txs whose `payload.cont.pactId` is the given one (the starter is
//! excluded), successful first, then height desc (ties: request key ascending), limit 50
//! (max 50, the newest 1,000 txs of the pact considered, R-P5P9-3); height and paging
//! parameters are ignored and no `Chainweb-Next` is sent, as chainweb-data does. When that
//! cap cut the walk the answer carries `X-Search-Truncated: true` (R-P5P9-48).

use std::collections::HashSet;
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
    aeson, check_strategy, collect_page, int_param, page_response, paging, recent,
    resume_exhausted, resume_max, shapes, store_floor, text, Candidates, Like, Page, Paging,
    Params, RowSource, ScanLimits, SEARCH_TRUNCATED,
};
use crate::search::{check_pattern, index_covers, lagging, rejected, ClientIp, SearchCtx};
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
/// (the scan path); with `hashes`, only txs whose exec code hashes to one of them (the index
/// path: posting entities are 8-byte prefixes).
struct Summaries {
    like: Option<Like>,
    hashes: Option<HashSet<[u8; 16]>>,
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
        if self
            .hashes
            .as_ref()
            .is_some_and(|h| !h.contains(&kdx_types::blake2b_128(code.as_bytes())))
        {
            return Ok(Vec::new());
        }
        Ok(vec![(0, 0)])
    }

    fn idx_of_sub(&self, _k: TxKey, _sub: u16) -> i64 {
        0
    }

    fn lists_genesis(&self) -> bool {
        true
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
        /// The index trails the tip (only when not `complete`; see `search::lagging`).
        lagging: bool,
    },
    /// The candidate query ran out of time.
    Timeout,
    /// No index, or it cannot answer this pattern completely: scan.
    None,
}

/// Ask the code index for `raw` with heights `min..=max` (`complete`: refuse a fallback-mode
/// index that may miss short bodies).
pub fn index_path(
    view: &View,
    ctx: &SearchCtx,
    raw: &str,
    (min, max): (Option<u32>, Option<u32>),
    until: Instant,
    complete: bool,
) -> IndexPath {
    let Some(s) = ctx.index.get() else {
        return IndexPath::None;
    };
    if complete && !index_covers(&s.manifest, raw) {
        return IndexPath::None;
    }
    let tip = view.max_data_height().ok().flatten();
    let built_through = ctx
        .index
        .built_through()
        .or(Some(s.manifest.built_through_height));
    let lagging = lagging(tip, built_through);
    if complete && lagging {
        rejected("lag");
        return IndexPath::None;
    }
    let w = kdx_search::Window {
        lo: u64::from(min.unwrap_or(0)),
        hi: max.map_or(u64::MAX, u64::from),
        upper_from: u64::from(view.code_upper_from(s.manifest.rebuilt_through_height)),
    };
    match s.candidates(&Like::new(raw), w, until) {
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
                lagging,
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

/// Now plus half of the time left to the request deadline.
pub fn half_left(view: &View) -> Instant {
    let now = Instant::now();
    now + view.deadline.saturating_duration_since(now) / 2
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
    match index_path(view, ctx, raw, (pg.min_height, max_h), until, true) {
        IndexPath::Hits {
            hits,
            rebuilt_through,
            capped,
            ..
        } => {
            let q = PostingQuery {
                min_height: pg.min_height,
                max_height: max_h,
                chain: None,
                before: None,
            };
            // Starting the walks and reading rows each get half of what is left of the
            // request deadline (at least the search budget).
            let (keys, cut) = view.code_txs(&hits, rebuilt_through, q, half_left(view))?;
            let mut src = Summaries {
                like: None,
                hashes: Some(hits.iter().map(|h| h.hash).collect()),
            };
            let rows = ScanLimits {
                until: until.max(half_left(view)),
                ..limits
            };
            let mut page = collect_page(
                view,
                Candidates::Keys(keys.peekable()),
                &mut src,
                pg,
                Some(rows),
            )?;
            if page.truncated || cut {
                rejected("timeout");
            }
            page.truncated |= capped || cut;
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
                hashes: None,
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
        .read(move |view| pact_rows(view, &pact, limit, PACT_CAP))
        .await;
    match res {
        Ok((rows, capped)) => pact_response(rows, capped),
        Err(e) => e.into_response(),
    }
}

/// The `cont` rows of `pact` among its newest `cap` txs, successful first, then height desc,
/// at most `limit`; the flag is true when the cap cut the walk (older txs were not read).
fn pact_rows(
    view: &View,
    pact: &Hash32,
    limit: usize,
    cap: usize,
) -> ApiResult<(Vec<Value>, bool)> {
    let (keys, capped) = view.pact_txs(pact, cap)?;
    let mut rows: Vec<(bool, TxKey, Vec<u8>, Value)> = Vec::new();
    for (i, (k, cont)) in keys.into_iter().enumerate() {
        if i % 64 == 63 {
            view.check_deadline()?;
        }
        if !cont {
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
    // Successful first, then height desc. chainweb-data leaves ties to Postgres; live it
    // answered them request key ascending (2 of 2 tied pairs, 2026-11-11).
    rows.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then(b.1.height().cmp(&a.1.height()))
            .then(a.2.cmp(&b.2))
            .then(a.1.chain().cmp(&b.1.chain()))
    });
    rows.truncate(limit);
    Ok((rows.into_iter().map(|r| r.3).collect(), capped))
}

/// The `pactid` answer: `X-Search-Truncated: true` when the cap cut the walk (R-P5P9-48; an
/// additive header that chainweb-data clients ignore), no such header otherwise.
fn pact_response(rows: Vec<Value>, capped: bool) -> Response {
    let mut resp = aeson(&Value::Array(rows));
    if capped {
        resp.headers_mut().insert(
            SEARCH_TRUNCATED,
            axum::http::HeaderValue::from_static("true"),
        );
    }
    resp
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use kdx_decode::{Family, IndexedBlock, TxIndex};
    use kdx_hot::{Geometry, HotDict, HotStore};
    use kdx_types::record::{BlockRecord, TxRecord};
    use kdx_types::{blake2b_128, Hash32, TxKey};

    use super::super::hidden;
    use super::*;
    use crate::search::SearchCtx;
    use crate::store::Store;

    fn block(chain: u8, height: u32, code: &str) -> IndexedBlock {
        let h = blake2b_128(code.as_bytes());
        let key = TxKey::new(height, chain, 1);
        let tx = TxRecord {
            key,
            request_key: Some(Hash32([chain.wrapping_add(1); 32])),
            cmd: Some(
                serde_json::to_vec(&serde_json::json!({"payload": {"exec": {"code": code}}}))
                    .unwrap(),
            ),
            raw_output: Some(
                br#"{"gas":1,"result":{"status":"success","data":"ok"},"reqKey":"x","logs":null,"events":[],"metaData":null,"continuation":null,"txId":1}"#.to_vec(),
            ),
            status_ok: true,
            ..TxRecord::default()
        };
        IndexedBlock {
            block: BlockRecord {
                hash: Hash32([chain.wrapping_add(100); 32]),
                chain,
                height: u64::from(height),
                ..BlockRecord::default()
            },
            txs: vec![tx],
            idx: vec![TxIndex {
                key,
                postings: vec![(Family::Code, kdx_decode::index::code_entity(&h))],
                pact_id: None,
                code_hash: Some(h),
            }],
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        }
    }

    /// A `cont` tx of `pact` at (`chain`, `height`).
    fn cont_block(chain: u8, height: u32, pact: &Hash32) -> IndexedBlock {
        let key = TxKey::new(height, chain, 1);
        let cmd = serde_json::json!({"payload": {"cont": {"pactId": pact.to_b64url(),
            "step": 1, "rollback": false, "data": {}, "proof": null}}});
        let tx = TxRecord {
            key,
            request_key: Some(Hash32([height as u8; 32])),
            cmd: Some(serde_json::to_vec(&cmd).unwrap()),
            raw_output: Some(
                br#"{"gas":1,"result":{"status":"success","data":"ok"},"reqKey":"x","logs":null,"events":[],"metaData":null,"continuation":null,"txId":1}"#.to_vec(),
            ),
            status_ok: true,
            ..TxRecord::default()
        };
        IndexedBlock {
            block: BlockRecord {
                hash: Hash32([height as u8; 32]),
                chain,
                height: u64::from(height),
                ..BlockRecord::default()
            },
            txs: vec![tx],
            idx: vec![TxIndex {
                key,
                postings: vec![(Family::Pact, kdx_decode::index::pact_entity(pact))],
                pact_id: Some(*pact),
                code_hash: None,
            }],
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        }
    }

    /// R-P5P9-48 item 4: a `pactid` answer carries `X-Search-Truncated: true` exactly when
    /// the cap cut the walk (here a cap of 2 over 3 txs; the route uses PACT_CAP, 1,000).
    #[test]
    fn pactid_cap_sends_the_truncated_header() {
        let d = tempfile::tempdir().unwrap();
        let g = Geometry {
            max_gb: 1,
            growth_mb: 16,
        };
        let hot = Arc::new(HotStore::open(&d.path().join("hot"), g).unwrap());
        let pact = Hash32([7; 32]);
        {
            let mut w = hot.rw().unwrap();
            w.set_meta(kdx_hot::env::meta::NETWORK, b"mainnet01")
                .unwrap();
            w.set_meta(
                kdx_hot::env::meta::CHAIN_GRAPH,
                b"[[0,[0,1,2,3,4,5,6,7,8,9]]]",
            )
            .unwrap();
            let mut dict = HotDict::load(&w).unwrap();
            for h in [20, 21, 22] {
                kdx_hot::tail::apply(&mut w, &cont_block(3, h, &pact), &mut dict).unwrap();
            }
            w.commit().unwrap();
        }
        let store = Arc::new(Store::open(hot, d.path()).unwrap());
        let view = store
            .view(Instant::now() + Duration::from_secs(10))
            .unwrap();
        let heights = |rows: &[Value]| -> Vec<u64> {
            rows.iter().map(|r| r["height"].as_u64().unwrap()).collect()
        };
        // Capped: the newest 2 txs, and the header.
        let (rows, capped) = pact_rows(&view, &pact, 50, 2).unwrap();
        assert_eq!((heights(&rows), capped), (vec![22, 21], true));
        let resp = pact_response(rows, capped);
        assert_eq!(
            resp.headers().get(SEARCH_TRUNCATED).map(|v| v.as_bytes()),
            Some(&b"true"[..])
        );
        // Not capped (the cap holds every tx, or exactly all of them): no header.
        for cap in [3, PACT_CAP] {
            let (rows, capped) = pact_rows(&view, &pact, 50, cap).unwrap();
            assert_eq!((heights(&rows), capped), (vec![22, 21, 20], false));
            assert!(pact_response(rows, capped)
                .headers()
                .get(SEARCH_TRUNCATED)
                .is_none());
        }
    }

    /// 6E-F: `/txs/search` lists genesis txs as chainweb-data does (mainnet: h0 on chains
    /// 0-9, h852054 on chains 10-19), while the other compat list routes still hide them.
    /// The fixture's graph puts the genesis blocks at heights 5 and 8, so the bounded scan
    /// stays small.
    #[test]
    fn search_lists_genesis_txs() {
        let d = tempfile::tempdir().unwrap();
        let g = Geometry {
            max_gb: 1,
            growth_mb: 16,
        };
        let hot = Arc::new(HotStore::open(&d.path().join("hot"), g).unwrap());
        let code = "(coin.create-account \"kad-ops-20\" (read-keyset \"kad-ops-20\"))";
        {
            let mut w = hot.rw().unwrap();
            w.set_meta(kdx_hot::env::meta::NETWORK, b"mainnet01")
                .unwrap();
            w.set_meta(
                kdx_hot::env::meta::CHAIN_GRAPH,
                b"[[5,[0,1,2,3,4,5,6,7,8,9]],[8,[10,11,12,13,14,15,16,17,18,19]]]",
            )
            .unwrap();
            let mut dict = HotDict::load(&w).unwrap();
            for b in [block(3, 5, code), block(12, 8, code), block(12, 9, code)] {
                kdx_hot::tail::apply(&mut w, &b, &mut dict).unwrap();
            }
            w.commit().unwrap();
        }
        let store = Arc::new(Store::open(hot, d.path()).unwrap());
        let view = store
            .view(Instant::now() + Duration::from_secs(10))
            .unwrap();
        let ctx = SearchCtx::new(super::super::SearchSettings::default(), None);
        let pg = Paging {
            limit: 10,
            offset: 0,
            next: None,
            min_height: None,
            max_height: None,
            empty: false,
        };
        let page = search_page(&view, &ctx, "kad-ops-20", &pg).unwrap();
        let heights: Vec<u64> = page
            .rows
            .iter()
            .map(|r| r["height"].as_u64().unwrap())
            .collect();
        assert_eq!(heights, vec![9, 8, 5], "{:?}", page.rows);
        // The other list routes keep hiding them.
        assert!(hidden(&view, TxKey::new(8, 12, 1)));
        assert!(hidden(&view, TxKey::new(5, 3, 1)));
    }
}
