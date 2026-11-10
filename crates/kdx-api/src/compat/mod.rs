//! chainweb-data compatible routes (spec §5.2): `/txs/tx`, `/txs/txs`, `/txs/account`,
//! `/txs/events`. Shapes, error texts and row order follow chainweb-data's `Server.hs` and
//! `Queries.hs` and chainweb-api's records; `Chainweb-Next` tokens are our own `Cursor`s.
//!
//! Row order of the paged routes is chainweb-data's: `height desc, requestkey desc, idx
//! asc`, request keys compared bytewise (Postgres "C" collation, checked live), coinbase
//! rows keyed `cb`. Postings come in TxKey order, so every height is collected whole and
//! re-sorted; ties chainweb-data leaves to Postgres (coinbase rows of several chains at one
//! height) are broken by chain, then event position.

pub mod account;
pub mod events;
pub mod recent;
pub mod shapes;
pub mod stats;
pub mod tx;

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::{header, HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Router};
use kdx_types::record::{BlockRecord, TxRecord};
use kdx_types::TxKey;
use serde_json::Value;

use crate::cursor::Cursor;
use crate::server::{ApiState, CHAINWEB_NEXT};
use crate::store::View;
use crate::{ApiError, ApiResult};

/// Genesis-block txs are not served by the compat list routes (`/txs/account`,
/// `/txs/events`, `/txs/recent`, `/v1/txs`). `/txs/tx` and `/txs/txs` serve them, as
/// chainweb-data does (ruling R3E-1, G1). The native API serves them everywhere.
pub const COMPAT_HIDE_GENESIS: bool = true;

/// True for a tx the compat list routes must not show. Genesis heights come from the
/// store's chain graph, not mainnet constants (R9).
pub fn hidden(view: &View, k: TxKey) -> bool {
    COMPAT_HIDE_GENESIS && view.genesis.is_genesis(k)
}

/// Page caps (spec §5.2).
pub const DEFAULT_LIMIT: u64 = 10;
pub const MAX_LIMIT: u64 = 250;
pub const MAX_OFFSET: u64 = 10_000;

/// Header marking a bounded scan that stopped before filling the page.
pub const SEARCH_TRUNCATED: HeaderName = HeaderName::from_static("x-search-truncated");

/// `[search]` keys the event scan uses.
#[derive(Debug, Clone, Copy)]
pub struct SearchSettings {
    pub min_len: usize,
    pub max_scan_heights: u32,
    pub timeout_ms: u64,
}

impl Default for SearchSettings {
    fn default() -> Self {
        SearchSettings {
            min_len: 4,
            max_scan_heights: 20_000,
            timeout_ms: 2000,
        }
    }
}

/// The compat router (register with `Api::compat_routes`).
pub fn router(search: SearchSettings) -> Router<Arc<ApiState>> {
    Router::new()
        .route("/txs/tx", get(tx::tx_handler))
        .route("/txs/txs", get(tx::txs_handler))
        .route("/txs/account/{account}", get(account::handler))
        .route("/txs/events", get(events::handler))
        .route("/txs/recent", get(recent::handler))
        .route("/stats", get(stats::handler))
        .merge(crate::v1txs::router())
        .layer(Extension(search))
}

// ---------------------------------------------------------------------------------------
// Responses

/// A plain-text answer (chainweb-data's error bodies).
pub fn text(code: StatusCode, msg: impl Into<String>) -> Response {
    (
        code,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        msg.into(),
    )
        .into_response()
}

/// 200 with the aeson text of `v`.
pub fn aeson(v: &Value) -> Response {
    let mut b = Vec::new();
    shapes::write_aeson(v, &mut b);
    json_bytes(b)
}

pub fn json_bytes(b: Vec<u8>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json;charset=utf-8")],
        Body::from(b),
    )
        .into_response()
}

/// A page of rows with its `Chainweb-Next` token and truncation flag.
pub fn page_response(p: Page) -> Response {
    let mut b = Vec::with_capacity(p.rows.iter().map(|_| 512).sum());
    b.push(b'[');
    for (i, r) in p.rows.iter().enumerate() {
        if i > 0 {
            b.push(b',');
        }
        shapes::write_aeson(r, &mut b);
    }
    b.push(b']');
    let mut resp = json_bytes(b);
    if let Some(n) = p.next {
        if let Ok(v) = n.encode().parse() {
            resp.headers_mut().insert(CHAINWEB_NEXT, v);
        }
    }
    if p.truncated {
        resp.headers_mut()
            .insert(SEARCH_TRUNCATED, header::HeaderValue::from_static("true"));
    }
    resp
}

// ---------------------------------------------------------------------------------------
// Query parameters

/// Query pairs in order (the first occurrence of a name wins, as in servant).
pub struct Params(Vec<(String, String)>);

impl Params {
    pub fn new(q: Vec<(String, String)>) -> Params {
        Params(q)
    }
    pub fn get(&self, k: &str) -> Option<&str> {
        self.0.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str())
    }
}

/// servant's `Integer` parameter parse: optional sign then digits, nothing after.
/// `Err` is chainweb-data's 400 text.
#[allow(clippy::result_large_err)] // the Err is the finished answer
pub fn int_param(p: &Params, name: &str) -> Result<Option<i128>, Response> {
    let Some(v) = p.get(name) else {
        return Ok(None);
    };
    let fail = |extra: &str| {
        text(
            StatusCode::BAD_REQUEST,
            format!("Error parsing query parameter {name} failed: could not parse: `{v}'{extra}"),
        )
    };
    let body = v.strip_prefix(['-', '+']).unwrap_or(v);
    if !body.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(fail(" (input does not start with a digit)"));
    }
    if !body.bytes().all(|b| b.is_ascii_digit()) {
        return Err(fail(""));
    }
    let digits = body.trim_start_matches('0');
    // Anything past i128 saturates (the caps below apply anyway).
    let n: i128 = if digits.len() > 30 {
        i128::MAX / 2
    } else {
        digits.parse().unwrap_or(0)
    };
    Ok(Some(if v.starts_with('-') { -n } else { n }))
}

/// `Chainweb-Execution-Strategy`: `Bounded` / `Unbounded` accepted (and ignored), other
/// values refused as chainweb-data does.
#[allow(clippy::result_large_err)] // the Err is the finished answer
pub fn check_strategy(h: &HeaderMap) -> Result<(), Response> {
    match h.get("chainweb-execution-strategy") {
        None => Ok(()),
        Some(v) if v == "Bounded" || v == "Unbounded" => Ok(()),
        Some(v) => Err(text(
            StatusCode::BAD_REQUEST,
            format!(
                "Unknown Chainweb-Execution-Strategy: {}",
                String::from_utf8_lossy(v.as_bytes())
            ),
        )),
    }
}

/// Paging inputs of `/txs/account` and `/txs/events`.
#[derive(Debug, Clone, Copy)]
pub struct Paging {
    pub limit: usize,
    pub offset: u64,
    pub next: Option<Cursor>,
    pub min_height: Option<u32>,
    /// Inclusive; `Some(None)`-like "nothing" is `empty`.
    pub max_height: Option<u32>,
    /// The height filters admit no row.
    pub empty: bool,
}

/// Parse `limit`, `offset`, `next`, `minheight`, `maxheight` with chainweb-data's answers.
#[allow(clippy::result_large_err)] // the Err is the finished answer
pub fn paging(p: &Params) -> Result<Paging, Response> {
    let limit = int_param(p, "limit")?;
    let offset = int_param(p, "offset")?;
    let minh = int_param(p, "minheight")?;
    let maxh = int_param(p, "maxheight")?;
    let next = match (p.get("next"), offset) {
        (Some(_), Some(_)) => {
            return Err(text(
                StatusCode::BAD_REQUEST,
                "next token query parameter not allowed with offset",
            ))
        }
        (Some(t), None) => Some(Cursor::decode(t).map_err(|_| {
            let shown: String = t.chars().take(100).collect();
            text(
                StatusCode::BAD_REQUEST,
                format!("Invalid next token: {shown}"),
            )
        })?),
        (None, _) => None,
    };
    let offset = offset.unwrap_or(0).max(0) as u64;
    if offset > MAX_OFFSET {
        return Err(text(
            StatusCode::BAD_REQUEST,
            format!("offset is capped at {MAX_OFFSET}: page further with the Chainweb-Next token as the next parameter"),
        ));
    }
    let limit = limit.unwrap_or(DEFAULT_LIMIT as i128);
    if limit < 0 {
        // chainweb-data passes a negative LIMIT to Postgres and answers this.
        return Err(text(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Something went wrong",
        ));
    }
    let limit = limit.min(MAX_LIMIT as i128) as usize;
    let clamp = |v: i128| v.clamp(0, u32::MAX as i128) as u32;
    let empty = maxh.is_some_and(|m| m < 0) || matches!((minh, maxh), (Some(a), Some(b)) if a > b);
    Ok(Paging {
        limit,
        offset,
        next,
        min_height: minh.map(clamp),
        max_height: maxh.map(clamp),
        empty,
    })
}

// ---------------------------------------------------------------------------------------
// LIKE

/// Postgres `LIKE '%' || needle || '%'`: `%` any run, `_` one character, `\` escapes.
pub struct Like {
    plain: Option<String>,
    pat: Vec<Tok>,
}

#[derive(Clone, Copy, PartialEq)]
enum Tok {
    Any,
    One,
    Ch(char),
}

impl Like {
    pub fn new(needle: &str) -> Like {
        if !needle.contains(['%', '_', '\\']) {
            return Like {
                plain: Some(needle.to_owned()),
                pat: Vec::new(),
            };
        }
        let mut pat = vec![Tok::Any];
        let mut it = needle.chars();
        while let Some(c) = it.next() {
            pat.push(match c {
                '%' => Tok::Any,
                '_' => Tok::One,
                '\\' => Tok::Ch(it.next().unwrap_or('\\')),
                c => Tok::Ch(c),
            });
        }
        pat.push(Tok::Any);
        pat.dedup_by(|a, b| *a == Tok::Any && *b == Tok::Any);
        Like { plain: None, pat }
    }

    pub fn matches(&self, s: &str) -> bool {
        if let Some(p) = &self.plain {
            return s.contains(p.as_str());
        }
        // Greedy match with backtracking to the last `%` (linear in practice).
        let t: Vec<char> = s.chars().collect();
        let (mut i, mut j) = (0usize, 0usize);
        let mut star: Option<(usize, usize)> = None;
        while i < t.len() {
            match self.pat.get(j) {
                Some(Tok::Any) => {
                    star = Some((j, i));
                    j += 1;
                }
                Some(Tok::One) => {
                    i += 1;
                    j += 1;
                }
                Some(Tok::Ch(c)) if *c == t[i] => {
                    i += 1;
                    j += 1;
                }
                _ => match star {
                    Some((sj, si)) => {
                        star = Some((sj, si + 1));
                        j = sj + 1;
                        i = si + 1;
                    }
                    None => return false,
                },
            }
        }
        self.pat[j..].iter().all(|t| *t == Tok::Any)
    }
}

// ---------------------------------------------------------------------------------------
// Row engine

/// One output row before rendering: its place in chainweb-data's order and where it comes
/// from (the tx and a route-defined sub-position).
#[derive(Debug, Clone)]
pub struct RowRef {
    pub key: TxKey,
    pub sub: u16,
    pub rk: Vec<u8>,
    pub idx: i64,
}

impl RowRef {
    /// Order inside one height: requestkey desc, idx asc, then chain and sub (our ties).
    fn cmp_in_height(&self, o: &RowRef) -> Ordering {
        o.rk.cmp(&self.rk)
            .then(self.idx.cmp(&o.idx))
            .then(self.key.chain().cmp(&o.key.chain()))
            .then(self.sub.cmp(&o.sub))
    }
}

/// Cursor `sub` marking "every row of this height was consumed" (a bounded scan cut).
pub const SUB_HEIGHT_DONE: u16 = u16::MAX;

/// Row sub-position `pos << shift | side` as a cursor `sub`. Positions that do not fit,
/// or that would encode `SUB_HEIGHT_DONE`, are an error instead of a wrapped value that
/// collides with another row or reads as "height done" (R8).
pub fn row_sub(pos: usize, shift: u32, side: u16) -> ApiResult<u16> {
    let v = u16::try_from(pos)
        .ok()
        .and_then(|p| p.checked_shl(shift).filter(|s| (s >> shift) == p))
        .map(|p| p | side)
        .filter(|&v| v != SUB_HEIGHT_DONE);
    v.ok_or_else(|| ApiError::Internal(anyhow::anyhow!("row position {pos} exceeds the cursor range")))
}

/// A route's view of one tx: its rows (sub, idx), and how to render one.
pub trait RowSource {
    /// Rows of `r` (already loaded), as `(sub, idx)`.
    fn rows(&mut self, view: &View, r: &TxRecord) -> ApiResult<Vec<(u16, i64)>>;
    /// The chainweb-data `idx` of row `sub` of the tx at `k`.
    fn idx_of_sub(&self, k: TxKey, sub: u16) -> i64;
    /// Render row `sub` of `r`.
    fn render(&mut self, view: &View, r: &TxRecord, b: &BlockRecord, sub: u16) -> ApiResult<Value>;
    /// False when `render` ignores the block (keys-only sources): the walk then skips
    /// loading it (R4).
    fn needs_block(&self) -> bool {
        true
    }
}

/// Where the candidate txs of a height come from.
pub enum Candidates<'v> {
    /// TxKeys newest first (posting lists), grouped by height here.
    Keys(std::iter::Peekable<Box<dyn Iterator<Item = ApiResult<TxKey>> + 'v>>),
    /// Every tx of every height from `next` down to `floor` (bounded scan).
    Scan { next: Option<u32>, floor: u32 },
}

/// Limits of a bounded scan.
#[derive(Debug, Clone, Copy)]
pub struct ScanLimits {
    pub max_heights: u32,
    pub until: Instant,
}

pub struct Page {
    pub rows: Vec<Value>,
    pub next: Option<Cursor>,
    pub truncated: bool,
}

/// Lowest height the store holds (scan floor).
pub fn store_floor(view: &View) -> u32 {
    view.seg
        .ranges
        .first()
        .map(|r| r.info.start)
        .unwrap_or_else(|| view.hot_floor())
}

/// Collect one page: skip `offset` rows (or the rows up to the cursor), take `limit`.
pub fn collect_page(
    view: &View,
    mut cands: Candidates<'_>,
    src: &mut dyn RowSource,
    pg: &Paging,
    scan: Option<ScanLimits>,
) -> ApiResult<Page> {
    let mut out = Vec::new();
    let mut skip = pg.offset;
    let mut after: Option<RowRef> = None;
    if let Some(c) = pg.next {
        if c.sub == SUB_HEIGHT_DONE {
            // Rows of `offset` a cut scan still owed (R7).
            skip = u64::from(c.key.pos());
        } else {
            let r = view
                .tx(c.key)?
                .ok_or_else(|| ApiError::BadRequest(crate::cursor::REORGED.into()))?;
            after = Some(RowRef {
                key: c.key,
                sub: c.sub,
                rk: shapes::rk_order(&r),
                idx: src.idx_of_sub(c.key, c.sub),
            });
        }
    }
    if pg.limit == 0 {
        return Ok(Page {
            rows: out,
            next: None,
            truncated: false,
        });
    }
    let mut scanned = 0u32;
    let no_block = BlockRecord::default();
    let mut blocks: BTreeMap<(u8, u32), BlockRecord> = BTreeMap::new();
    loop {
        view.check_deadline()?;
        // One height's txs.
        let (h, txs): (u32, Vec<TxRecord>) = match &mut cands {
            Candidates::Keys(it) => {
                let Some(first) = it.next() else { break };
                let first = first?;
                let h = first.height();
                let mut keys = vec![first];
                while let Some(Ok(k)) = it.peek() {
                    if k.height() != h {
                        break;
                    }
                    keys.push(*k);
                    it.next();
                }
                if let Some(Err(_)) = it.peek() {
                    if let Some(Err(e)) = it.next() {
                        return Err(e);
                    }
                }
                keys.dedup();
                let mut txs = Vec::with_capacity(keys.len());
                for k in keys {
                    if hidden(view, k) {
                        continue;
                    }
                    if let Some(r) = view.tx(k)? {
                        txs.push(r);
                    }
                }
                (h, txs)
            }
            Candidates::Scan { next, floor } => {
                let Some(h) = *next else { break };
                if let Some(l) = scan {
                    if scanned >= l.max_heights || Instant::now() >= l.until {
                        // Cut: resume below the last whole height.
                        let done = h + 1;
                        let owed = u16::try_from(skip).unwrap_or(u16::MAX);
                        let next = Cursor::height_done(view, done, SUB_HEIGHT_DONE, owed)?;
                        return Ok(Page {
                            rows: out,
                            next: Some(next),
                            truncated: true,
                        });
                    }
                }
                scanned += 1;
                *next = if h > *floor { Some(h - 1) } else { None };
                let mut txs = Vec::new();
                for c in 0..kdx_segment::SLOT_WIDTH as u8 {
                    for r in view.block_txs(c, h)? {
                        if !hidden(view, r.key) {
                            txs.push(r);
                        }
                    }
                }
                (h, txs)
            }
        };
        let mut rows: Vec<(RowRef, usize)> = Vec::new();
        for (i, r) in txs.iter().enumerate() {
            // One height can hold many txs: keep the deadline inside it too.
            if i % 64 == 63 {
                view.check_deadline()?;
            }
            for (sub, idx) in src.rows(view, r)? {
                rows.push((
                    RowRef {
                        key: r.key,
                        sub,
                        rk: shapes::rk_order(r),
                        idx,
                    },
                    i,
                ));
            }
        }
        rows.sort_by(|a, b| a.0.cmp_in_height(&b.0));
        for (row, i) in rows {
            if let Some(a) = &after {
                if a.key.height() == h && row.cmp_in_height(a) != Ordering::Greater {
                    continue;
                }
            }
            if skip > 0 {
                skip -= 1;
                continue;
            }
            let r = &txs[i];
            if !src.needs_block() {
                out.push(src.render(view, r, &no_block, row.sub)?);
            } else {
                let bk = (r.key.chain(), r.key.height());
                if let std::collections::btree_map::Entry::Vacant(e) = blocks.entry(bk) {
                    let b = view.block(bk.0, bk.1)?.ok_or_else(|| {
                        ApiError::Internal(anyhow::anyhow!("block of {:?} missing", r.key))
                    })?;
                    e.insert(b);
                }
                out.push(src.render(view, r, &blocks[&bk], row.sub)?);
            }
            if out.len() == pg.limit {
                return Ok(Page {
                    rows: out,
                    next: Some(Cursor::new(view, row.key, row.sub)?),
                    truncated: false,
                });
            }
        }
    }
    Ok(Page {
        rows: out,
        next: None,
        truncated: false,
    })
}

/// Upper height bound for a walk resuming at `next` within `max_height`.
pub fn resume_max(pg: &Paging) -> Option<u32> {
    match pg.next {
        Some(c) if c.sub == SUB_HEIGHT_DONE => {
            let h = c.key.height().checked_sub(1);
            Some(h.map_or(0, |h| pg.max_height.map_or(h, |m| m.min(h))))
        }
        Some(c) => Some(
            pg.max_height
                .map_or(c.key.height(), |m| m.min(c.key.height())),
        ),
        None => pg.max_height,
    }
}

/// True when a resumed walk has nothing left (a cut at the lowest height).
pub fn resume_exhausted(pg: &Paging) -> bool {
    matches!(pg.next, Some(c) if c.sub == SUB_HEIGHT_DONE && c.key.height() == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like() {
        assert!(Like::new("TRA").matches("coin.TRANSFER"));
        assert!(Like::new("coin.T_ANS%ER").matches("coin.TRANSFER"));
        assert!(!Like::new("coin.T_NS").matches("coin.TRANSFER"));
        assert!(Like::new("%").matches(""));
        assert!(Like::new("a\\%b").matches("xa%by"));
        assert!(!Like::new("a\\%b").matches("xaZby"));
        assert!(Like::new("_").matches("é"));
        assert!(!Like::new("_").matches(""));
    }

    #[test]
    fn row_sub_range() {
        assert_eq!(row_sub(5, 0, 0).unwrap(), 5);
        assert_eq!(row_sub(5, 1, 1).unwrap(), 11);
        assert_eq!(row_sub(32_766, 1, 1).unwrap(), 65_533);
        // 32,767 to-side would encode SUB_HEIGHT_DONE; 32,768 wraps.
        assert!(row_sub(32_767, 1, 1).is_err());
        assert!(row_sub(32_768, 1, 0).is_err());
        assert!(row_sub(65_535, 0, 0).is_err());
        assert!(row_sub(70_000, 0, 0).is_err());
    }

    #[test]
    fn genesis() {
        let g = crate::store::Genesis::from_meta(
            Some(b"mainnet01"),
            Some(b"[[852054,[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]],[0,[0,1,2,3,4,5,6,7,8,9]]]"),
        );
        assert!(g.is_genesis(TxKey::new(0, 3, 1)));
        assert!(g.is_genesis(TxKey::new(852_054, 12, 1)));
        assert!(!g.is_genesis(TxKey::new(852_054, 2, 1)));
        assert!(!g.is_genesis(TxKey::new(1, 0, 1)));
        // Another network's graph change height (R9).
        let t = crate::store::Genesis::from_meta(
            Some(b"testnet04"),
            Some(b"[[0,[0,1,2,3,4,5,6,7,8,9]],[332604,[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]]]"),
        );
        assert!(t.is_genesis(TxKey::new(332_604, 12, 1)));
        assert!(!t.is_genesis(TxKey::new(852_054, 12, 1)));
        assert!(!crate::store::Genesis::default().is_genesis(TxKey::new(0, 0, 1)));
        assert_eq!(t.chains().count(), 20);
    }
}
