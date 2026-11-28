//! `/txs/account/{account}` (chainweb-data `accountHandler`): one `TransferDetail` per
//! transfer row of token `token` (default `coin`) with the account on either side, from the
//! `xfer` postings. chainweb-data unions a from-side and a to-side query, so a transfer to
//! oneself is two rows; so it is here.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use kdx_decode::index::xfer_entity;
use kdx_decode::Family;
use kdx_types::record::{BlockRecord, TxRecord};
use kdx_types::TxKey;
use serde_json::Value;

use super::schemas::{ExecutionStrategy, LimitErrors, PagingParams, TransferDetail};
use super::shapes::{self, Ev, Transfer};
use super::{
    chain_param, check_strategy, collect_page, page_response, paging, resume_exhausted, resume_max,
    Candidates, Page, Paging, Params, RowSource,
};
use crate::server::ApiState;
use crate::store::{PostingQuery, View};
use crate::ApiResult;

pub struct Src {
    account: String,
    token: String,
    cache: HashMap<TxKey, (Vec<Ev>, Vec<Transfer>)>,
    /// Keys-only mode (`/v1/txs`): rows are not rendered, the tx of each is recorded.
    keys: Option<Vec<KeyRow>>,
    /// Keys-only mode: rendered outputs kept for the caller, so it does not render them
    /// a second time (R4).
    outs: HashMap<TxKey, Value>,
    /// Keys-only mode: what the kept records may hold (F-21).
    budget: KeepBudget,
}

/// A row of a keys-only page: the tx key and, for its first row, the record and its
/// rendered output.
///
/// Byte budget of the records a keys-only page keeps (F-21): kept rows hold at most `cap`
/// estimated bytes (the response body cap); past it a row keeps only its key and the
/// writer reads it again. Once the estimate of every row passes twice the cap the body
/// cannot fit, and the request fails early with the body-cap answer.
#[derive(Debug, Clone, Copy)]
pub struct KeepBudget {
    cap: usize,
    kept: usize,
    est: usize,
}

impl KeepBudget {
    pub fn new(cap: usize) -> KeepBudget {
        KeepBudget {
            cap,
            kept: 0,
            est: 0,
        }
    }

    /// Account a row of `n` estimated bytes: true when it may be kept.
    pub fn admit(&mut self, n: usize) -> ApiResult<bool> {
        self.est = self.est.saturating_add(n);
        if self.est > self.cap.saturating_mul(2) {
            return Err(crate::ApiError::Unavailable(format!(
                "response larger than {} MB: use a smaller limit",
                self.cap >> 20
            )));
        }
        if self.kept.saturating_add(n) > self.cap {
            return Ok(false);
        }
        self.kept += n;
        Ok(true)
    }

    pub fn kept(&self) -> usize {
        self.kept
    }
}

/// Size estimate of a JSON value as written: object keys, string contents, 8 per other
/// scalar (no quotes or punctuation, so it stays below the written size).
pub fn json_size(v: &Value) -> usize {
    match v {
        Value::String(s) => s.len(),
        Value::Array(a) => a.iter().map(json_size).sum(),
        Value::Object(m) => m.iter().map(|(k, v)| k.len() + json_size(v)).sum(),
        _ => 8,
    }
}

pub type KeyRow = (TxKey, Option<(TxRecord, Value)>);

impl Src {
    fn new(account: &str, token: &str) -> Src {
        Src {
            account: account.to_owned(),
            token: token.to_owned(),
            cache: HashMap::new(),
            keys: None,
            outs: HashMap::new(),
            budget: KeepBudget::new(usize::MAX),
        }
    }

    /// A source whose rows are not rendered; `take_keys` returns the tx key of each row
    /// in order, with the record and output kept while `budget` allows.
    pub fn keys_only(account: &str, token: &str, budget: KeepBudget) -> Src {
        Src {
            keys: Some(Vec::new()),
            budget,
            ..Src::new(account, token)
        }
    }

    /// The rows, and the budget left for the next source of the same page.
    pub fn take_keys(&mut self) -> (Vec<KeyRow>, KeepBudget) {
        (self.keys.take().unwrap_or_default(), self.budget)
    }

    fn load(&mut self, view: &View, r: &TxRecord) -> ApiResult<&(Vec<Ev>, Vec<Transfer>)> {
        if !self.cache.contains_key(&r.key) {
            if self.cache.len() > 4096 {
                self.cache.clear();
            }
            let out = shapes::output(view, r)?;
            let evs = shapes::events(&out);
            let trs = shapes::transfers(&evs);
            if self.keys.is_some() {
                if self.outs.len() > 4096 {
                    self.outs.clear();
                }
                self.outs.insert(r.key, out);
            }
            self.cache.insert(r.key, (evs, trs));
        }
        Ok(&self.cache[&r.key])
    }
}

impl RowSource for Src {
    fn rows(&mut self, view: &View, r: &TxRecord) -> ApiResult<Vec<(u16, i64)>> {
        let (acct, token) = (self.account.clone(), self.token.clone());
        let (_, trs) = self.load(view, r)?;
        let mut out = Vec::new();
        for t in trs.iter().filter(|t| t.token == token) {
            let idx = shapes::ev_idx(r.key, t.pos);
            if t.from == acct {
                out.push((super::row_sub(t.pos, 1, 0)?, idx));
            }
            if t.to == acct {
                out.push((super::row_sub(t.pos, 1, 1)?, idx));
            }
        }
        Ok(out)
    }

    fn idx_of_sub(&self, k: TxKey, sub: u16) -> i64 {
        shapes::ev_idx(k, (sub >> 1) as usize)
    }

    fn needs_block(&self) -> bool {
        self.keys.is_none()
    }

    fn render(&mut self, view: &View, r: &TxRecord, b: &BlockRecord, sub: u16) -> ApiResult<Value> {
        if let Some(keys) = &mut self.keys {
            let full = match self.outs.remove(&r.key) {
                Some(o) => {
                    // The output and the command are both in the written entry.
                    let n = json_size(&o) + r.cmd.as_ref().map_or(0, Vec::len);
                    self.budget.admit(n)?.then(|| (r.clone(), o))
                }
                None => None,
            };
            keys.push((r.key, full));
            return Ok(Value::Null);
        }
        let pos = (sub >> 1) as usize;
        let (evs, trs) = self.load(view, r)?;
        let t = trs
            .iter()
            .find(|t| t.pos == pos)
            .ok_or_else(|| crate::ApiError::Internal(anyhow::anyhow!("transfer row vanished")))?;
        Ok(shapes::transfer_detail(r, b, evs, t))
    }
}

pub(super) fn page(
    view: &View,
    account: &str,
    token: &str,
    chain: Option<u8>,
    pg: &Paging,
) -> ApiResult<Page> {
    let empty = Page {
        rows: Vec::new(),
        next: None,
        truncated: false,
    };
    if pg.empty || resume_exhausted(pg) {
        return Ok(empty);
    }
    if let Some(c) = pg.next {
        c.check(view)?;
    }
    let d = view.dict();
    let (Some(a), Some(m)) = (d.account_id(account), d.module_id(token)) else {
        return Ok(empty);
    };
    let q = PostingQuery {
        min_height: pg.min_height,
        max_height: resume_max(pg),
        chain,
        before: None,
    };
    let it = view
        .postings(Family::Xfer, xfer_entity(a, m), q)?
        .peekable();
    let mut src = Src::new(account, token);
    collect_page(view, Candidates::Keys(it), &mut src, pg, None)
}

/// Transfers of an account
///
/// chainweb-data `accountHandler`: a JSON array of `TransferDetail`, newest first (height
/// descending, request key descending, event index ascending), the rows of `token` (default
/// `coin`) with the account on either side. A transfer to oneself is one row. Pages carry
/// `Chainweb-Next` when there is more. Genesis transactions are hidden; miner rewards are
/// listed. A chain id outside 0..=19, an unknown account or token answers an empty array; a `chain` past a 64-bit integer is a 400.
/// `amount` is the shortest exact decimal text, so it differs from chainweb-data's raw
/// text (`"138000"` for `"138000.000000000000"`) while being the same decimal.
#[utoipa::path(
    get,
    path = "/txs/account/{account}",
    operation_id = "compat_account",
    tag = "transfers",
    params(
        ("account" = String, Path, description = "Account name, 1 to 256 bytes."),
        ("token" = Option<String>, Query, description = "Fungible module of the transfers (default `coin`).", example = "coin"),
        ("chain" = Option<i64>, Query, description = "Only this chain (0 to 19); another whole number answers an empty page, a number beyond a 64-bit integer is a 400."),
        PagingParams,
        ("Chainweb-Execution-Strategy" = Option<ExecutionStrategy>, Header, description = "Accepted and ignored. Any other value is `400 Unknown Chainweb-Execution-Strategy: ...`."),
    ),
    responses(
        (status = 200, description = "A page of transfers", body = Vec<TransferDetail>, content_type = "application/json",
            headers(("Chainweb-Next" = String, description = "Token of the next page (pass as `next`); absent on the last page."))),
        (status = 400, description = "A parameter does not parse (`Error parsing query parameter ...`), `offset` above 10,000, `next` with `offset`, or an invalid `next`", body = String, content_type = "text/plain"),
        LimitErrors,
    )
)]
pub async fn handler(
    State(st): State<Arc<ApiState>>,
    Path(account): Path<String>,
    headers: HeaderMap,
    Query(q): Query<Vec<(String, String)>>,
) -> Response {
    let p = Params::new(q);
    let chain = match chain_param(&p) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let pg = match paging(&p, true) {
        Ok(pg) => pg,
        Err(r) => return r,
    };
    if let Err(r) = check_strategy(&headers) {
        return r;
    }
    let token = p.get("token").unwrap_or("coin").to_owned();
    // A chain id that does not exist matches nothing.
    let chain = match chain {
        None => None,
        Some(c) if (0..kdx_segment::SLOT_WIDTH as i128).contains(&c) => Some(c as u8),
        Some(_) => {
            return page_response(Page {
                rows: Vec::new(),
                next: None,
                truncated: false,
            })
        }
    };
    match st
        .read(move |v| page(v, &account, &token, chain, &pg))
        .await
    {
        Ok(p) => page_response(p),
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// F-21: retained rows fit a byte budget of the body cap; past it only keys are kept,
    /// and once the estimate passes twice the cap the request fails early.
    #[test]
    fn keep_budget() {
        let mut k = KeepBudget::new(100);
        assert!(k.admit(60).unwrap());
        assert!(!k.admit(50).unwrap(), "over the budget: key only");
        assert!(k.admit(40).unwrap(), "still fits");
        assert_eq!(k.kept(), 100);
        assert!(matches!(k.admit(51), Err(crate::ApiError::Unavailable(_))));
    }

    /// F-21: the size estimate counts string content, keys and scalars.
    #[test]
    fn json_size_estimate() {
        let v = serde_json::json!({"ab": ["xyz", 1, null], "c": {"d": "e"}});
        // keys 2+1+1, strings 3+1, three scalars at 8.
        assert_eq!(json_size(&v), 2 + 1 + 1 + 3 + 1 + 8 + 8);
    }
}
