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

use super::shapes::{self, Ev, Transfer};
use super::{
    check_strategy, collect_page, int_param, page_response, paging, resume_exhausted, resume_max,
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
    keys: Option<Vec<TxKey>>,
}

impl Src {
    fn new(account: &str, token: &str) -> Src {
        Src {
            account: account.to_owned(),
            token: token.to_owned(),
            cache: HashMap::new(),
            keys: None,
        }
    }

    /// A source whose rows are not rendered; `take_keys` returns the tx key of each row
    /// in order.
    pub fn keys_only(account: &str, token: &str) -> Src {
        Src {
            keys: Some(Vec::new()),
            ..Src::new(account, token)
        }
    }

    pub fn take_keys(&mut self) -> Vec<TxKey> {
        self.keys.take().unwrap_or_default()
    }

    fn load(&mut self, view: &View, r: &TxRecord) -> ApiResult<&(Vec<Ev>, Vec<Transfer>)> {
        if !self.cache.contains_key(&r.key) {
            if self.cache.len() > 4096 {
                self.cache.clear();
            }
            let evs = shapes::events(&shapes::output(view, r)?);
            let trs = shapes::transfers(&evs);
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
            let pos = (t.pos as u16) << 1;
            if t.from == acct {
                out.push((pos, idx));
            }
            if t.to == acct {
                out.push((pos | 1, idx));
            }
        }
        Ok(out)
    }

    fn idx_of_sub(&self, k: TxKey, sub: u16) -> i64 {
        shapes::ev_idx(k, (sub >> 1) as usize)
    }

    fn render(&mut self, view: &View, r: &TxRecord, b: &BlockRecord, sub: u16) -> ApiResult<Value> {
        if let Some(keys) = &mut self.keys {
            keys.push(r.key);
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

fn page(
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

pub async fn handler(
    State(st): State<Arc<ApiState>>,
    Path(account): Path<String>,
    headers: HeaderMap,
    Query(q): Query<Vec<(String, String)>>,
) -> Response {
    let p = Params::new(q);
    let chain = match int_param(&p, "chain") {
        Ok(c) => c,
        Err(r) => return r,
    };
    let pg = match paging(&p) {
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
