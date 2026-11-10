//! `/txs/events` (chainweb-data `evHandler` + `eventSearchCond`): `modulename` exact,
//! `name` a `LIKE '%name%'` on the qualified name, `param` a `LIKE` on the params text
//! (aeson's encoding, chainweb-data's `paramtext` column), `search` a `LIKE` on either.
//!
//! Candidates: the `evt` lists of the event names matching `name` (k-way merge) when there
//! are at most `max_event_lists` of them, else the module wildcard list of `modulename`,
//! else a bounded newest-first scan of every block (`search.max_scan_heights`,
//! `search.timeout_ms`; a cut answers what it found, `X-Search-Truncated: true` and a token).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use kdx_decode::index::evt_module_entity;
use kdx_decode::Family;
use kdx_types::record::{BlockRecord, TxRecord};
use kdx_types::{Dict, DictKind, TxKey};
use serde_json::Value;

use super::shapes::{self, Ev};
use super::{
    check_strategy, collect_page, page_response, paging, resume_exhausted, resume_max, store_floor,
    text, Candidates, Like, Page, Paging, Params, RowSource, ScanLimits, SearchSettings,
};
use crate::server::ApiState;
use crate::store::{PostingQuery, View};
use crate::{ApiError, ApiResult};

#[derive(Default)]
struct Filter {
    search: Option<Like>,
    param: Option<Like>,
    name: Option<Like>,
    name_raw: Option<String>,
    module: Option<String>,
}

impl Filter {
    fn matches(&self, e: &Ev) -> bool {
        if let Some(m) = &self.module {
            if &e.module != m {
                return false;
            }
        }
        if let Some(n) = &self.name {
            if !n.matches(&e.qual_name) {
                return false;
            }
        }
        if self.param.is_none() && self.search.is_none() {
            return true;
        }
        let pt = shapes::param_text(&e.params);
        if let Some(p) = &self.param {
            if !p.matches(&pt) {
                return false;
            }
        }
        if let Some(s) = &self.search {
            if !s.matches(&e.qual_name) && !s.matches(&pt) {
                return false;
            }
        }
        true
    }
}

struct Src {
    f: Filter,
    cache: HashMap<TxKey, Vec<Ev>>,
}

impl Src {
    fn load(&mut self, view: &View, r: &TxRecord) -> ApiResult<&Vec<Ev>> {
        if !self.cache.contains_key(&r.key) {
            if self.cache.len() > 4096 {
                self.cache.clear();
            }
            let evs = shapes::events(&shapes::output(view, r)?);
            self.cache.insert(r.key, evs);
        }
        Ok(&self.cache[&r.key])
    }
}

impl RowSource for Src {
    fn rows(&mut self, view: &View, r: &TxRecord) -> ApiResult<Vec<(u16, i64)>> {
        self.load(view, r)?;
        let evs = &self.cache[&r.key];
        Ok(evs
            .iter()
            .filter(|e| self.f.matches(e))
            .map(|e| (e.pos as u16, shapes::ev_idx(r.key, e.pos)))
            .collect())
    }

    fn idx_of_sub(&self, k: TxKey, sub: u16) -> i64 {
        shapes::ev_idx(k, sub as usize)
    }

    fn render(&mut self, view: &View, r: &TxRecord, b: &BlockRecord, sub: u16) -> ApiResult<Value> {
        let evs = self.load(view, r)?;
        let e = evs
            .get(sub as usize)
            .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("event row vanished")))?;
        Ok(shapes::event_detail(r, b, e))
    }
}

/// Newest-first merge of several TxKey streams (duplicates kept; grouped later).
struct Merge<'v> {
    its: Vec<std::iter::Peekable<Box<dyn Iterator<Item = ApiResult<TxKey>> + 'v>>>,
}

impl Iterator for Merge<'_> {
    type Item = ApiResult<TxKey>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut best: Option<(usize, TxKey)> = None;
        for (i, it) in self.its.iter_mut().enumerate() {
            match it.peek() {
                None => {}
                Some(Err(_)) => return it.next(),
                Some(Ok(k)) if best.is_none_or(|(_, b)| *k > b) => best = Some((i, *k)),
                Some(Ok(_)) => {}
            }
        }
        let (i, _) = best?;
        self.its[i].next()
    }
}

/// Event-name ids for `name` (plain substring of the qualified name, within `module`),
/// or None when they are too many (or `name` uses LIKE wildcards).
fn name_ids(view: &View, name: &str, module: Option<&str>, cap: usize) -> Option<Vec<u32>> {
    if name.contains(['%', '_', '\\']) {
        return None;
    }
    let d = view.dict();
    let (ids, more) = d.event_names(name, usize::MAX);
    let prefix = module.map(|m| format!("{m}."));
    let ids: Vec<u32> = ids
        .into_iter()
        .filter(|id| match &prefix {
            Some(p) => d
                .name(DictKind::EventName, *id)
                .is_some_and(|n| n.starts_with(p.as_str())),
            None => true,
        })
        .collect();
    (!more && ids.len() <= cap).then_some(ids)
}

fn page(
    view: &View,
    f: Filter,
    pg: &Paging,
    search: SearchSettings,
    cap: usize,
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
    let max_h = resume_max(pg);
    let q = PostingQuery {
        min_height: pg.min_height,
        max_height: max_h,
        chain: None,
        before: None,
    };
    let module_id = match &f.module {
        Some(m) => match view.dict().module_id(m) {
            Some(id) => Some(id),
            None => return Ok(empty),
        },
        None => None,
    };
    let ids = f
        .name_raw
        .as_deref()
        .and_then(|n| name_ids(view, n, f.module.as_deref(), cap));
    let mut src = Src {
        f,
        cache: HashMap::new(),
    };
    if let Some(ids) = ids {
        if ids.is_empty() {
            return Ok(empty);
        }
        let mut its = Vec::with_capacity(ids.len());
        for id in ids {
            its.push(view.postings(Family::Evt, id as u64, q)?.peekable());
        }
        let m: Box<dyn Iterator<Item = ApiResult<TxKey>> + '_> = Box::new(Merge { its });
        return collect_page(view, Candidates::Keys(m.peekable()), &mut src, pg, None);
    }
    if let Some(mid) = module_id {
        let it = view
            .postings(Family::Evt, evt_module_entity(mid), q)?
            .peekable();
        return collect_page(view, Candidates::Keys(it), &mut src, pg, None);
    }
    // Bounded scan of every block, newest first.
    let floor = store_floor(view).max(pg.min_height.unwrap_or(0));
    let top = match (view.max_indexed_height()?, max_h) {
        (None, _) => return Ok(empty),
        (Some(t), Some(m)) => t.min(m),
        (Some(t), None) => t,
    };
    if top < floor {
        return Ok(empty);
    }
    let limits = ScanLimits {
        max_heights: search.max_scan_heights.max(1),
        until: Instant::now() + Duration::from_millis(search.timeout_ms),
    };
    collect_page(
        view,
        Candidates::Scan {
            next: Some(top),
            floor,
        },
        &mut src,
        pg,
        Some(limits),
    )
}

/// Longest `name`/`param`/`search` accepted (characters).
pub const MAX_PATTERN_CHARS: usize = 256;
/// Most unescaped `%` wildcards in one pattern (bounds the matcher's backtracking).
pub const MAX_PATTERN_WILDCARDS: usize = 8;

/// Unescaped `%` in a LIKE pattern.
fn wildcards(s: &str) -> usize {
    let mut n = 0;
    let mut it = s.chars();
    while let Some(c) = it.next() {
        match c {
            '\\' => {
                it.next();
            }
            '%' => n += 1,
            _ => {}
        }
    }
    n
}

/// Refuse patterns that would make a request expensive: too long, too many wildcards.
/// `Err` is the 400 text.
fn check_patterns(p: &Params, s: &SearchSettings) -> Result<(), String> {
    for k in ["name", "param", "search"] {
        let Some(v) = p.get(k) else { continue };
        if v.chars().count() > MAX_PATTERN_CHARS {
            return Err(format!("{k} must be at most {MAX_PATTERN_CHARS} characters"));
        }
        if wildcards(v) > MAX_PATTERN_WILDCARDS {
            return Err(format!("{k} may contain at most {MAX_PATTERN_WILDCARDS} '%' wildcards"));
        }
    }
    if let Some(v) = p.get("search") {
        if v.chars().count() < s.min_len {
            return Err(format!("search must be at least {} characters", s.min_len));
        }
    }
    Ok(())
}

pub async fn handler(
    State(st): State<Arc<ApiState>>,
    Extension(search): Extension<SearchSettings>,
    headers: HeaderMap,
    Query(q): Query<Vec<(String, String)>>,
) -> Response {
    let p = Params::new(q);
    let pg = match paging(&p) {
        Ok(pg) => pg,
        Err(r) => return r,
    };
    if let Err(r) = check_strategy(&headers) {
        return r;
    }
    if let Err(t) = check_patterns(&p, &search) {
        return text(StatusCode::BAD_REQUEST, t);
    }
    let f = Filter {
        search: p.get("search").map(Like::new),
        param: p.get("param").map(Like::new),
        name: p.get("name").map(Like::new),
        name_raw: p.get("name").map(str::to_owned),
        module: p.get("modulename").map(str::to_owned),
    };
    let cap = st.settings.max_event_lists;
    match st.read(move |v| page(v, f, &pg, search, cap)).await {
        Ok(p) => page_response(p),
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(q: &[(&str, &str)]) -> Params {
        Params::new(q.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect())
    }

    #[test]
    fn pattern_length_and_wildcards_capped() {
        let s = SearchSettings::default();
        let long = "a".repeat(MAX_PATTERN_CHARS + 1);
        let many = "%a".repeat(MAX_PATTERN_WILDCARDS + 1);
        for k in ["name", "param", "search"] {
            assert!(check_patterns(&params(&[("modulename", "coin"), (k, &long)]), &s).is_err(), "{k} long");
            assert!(check_patterns(&params(&[("modulename", "coin"), (k, &many)]), &s).is_err(), "{k} %");
        }
        let ok = "a".repeat(MAX_PATTERN_CHARS);
        assert!(check_patterns(&params(&[("modulename", "coin"), ("param", &ok)]), &s).is_ok());
        assert!(check_patterns(&params(&[("modulename", "coin"), ("name", "TRANSFER")]), &s).is_ok());
        // An escaped `%` is a literal, not a wildcard.
        let lit = "\\%a".repeat(MAX_PATTERN_WILDCARDS + 1);
        assert!(check_patterns(&params(&[("modulename", "coin"), ("param", &lit)]), &s).is_ok());
    }
}
