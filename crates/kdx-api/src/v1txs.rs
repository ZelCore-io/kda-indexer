//! `GET /v1/txs/{account}/{namespace}/{limit}`: the transactions behind an account's newest
//! token transfers, as `TxDetail`s. Written from the behaviour note (docs/specs, not
//! public): path segments are optional and fall back to the query parameters of the same
//! name, `namespace` is the fungible module (default `coin`), `limit` counts transfer rows
//! (a JavaScript number, default 50, cap 10,000), token rows come first and, for a token
//! other than `coin`, the account's `coin` rows follow; request keys are de-duplicated
//! keeping the first, miner-reward rows count toward `limit` and produce no entry.
//!
//! Rows are read from the same `xfer` postings as `/txs/account`; entries are built with
//! the `/txs/txs` builder. Genesis txs follow the compat list-route rule (hidden).

use std::collections::HashSet;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use kdx_decode::index::xfer_entity;
use kdx_decode::Family;
use kdx_types::TxKey;

use crate::compat::account::Src;
use crate::compat::shapes::{self, write_aeson};
use crate::compat::{collect_page, json_bytes, text, Candidates, Paging};
use crate::server::ApiState;
use crate::store::{PostingQuery, View};
use crate::{ApiError, ApiResult};

const DEFAULT_LIMIT: u64 = 50;
const MAX_LIMIT: u64 = 10_000;

pub fn router() -> Router<Arc<ApiState>> {
    Router::new()
        .route("/v1/txs", get(handler))
        .route("/v1/txs/", get(handler))
        .route("/v1/txs/{*rest}", get(handler))
}

fn is_js_ws(c: char) -> bool {
    c.is_whitespace() || c == '\u{feff}'
}

/// JavaScript `Number(s)` for the string forms a path or query value can take.
fn js_number(s: &str) -> f64 {
    let t = s.trim_matches(is_js_ws);
    if t.is_empty() {
        return 0.0;
    }
    let b = t.as_bytes();
    if b.len() > 2 && b[0] == b'0' {
        let radix = match b[1] {
            b'x' | b'X' => 16,
            b'o' | b'O' => 8,
            b'b' | b'B' => 2,
            _ => 0,
        };
        if radix != 0 {
            return match u128::from_str_radix(&t[2..], radix) {
                Ok(n) => n as f64,
                Err(e) if matches!(e.kind(), std::num::IntErrorKind::PosOverflow) => f64::INFINITY,
                Err(_) => f64::NAN,
            };
        }
    }
    match t {
        "Infinity" | "+Infinity" => return f64::INFINITY,
        "-Infinity" => return f64::NEG_INFINITY,
        _ => {}
    }
    // [+-] (digits [. digits?] | . digits) ([eE] [+-] digits)?
    let digits = |from: usize| b[from..].iter().take_while(|c| c.is_ascii_digit()).count();
    let mut i = usize::from(matches!(b[0], b'+' | b'-'));
    let int = digits(i);
    i += int;
    let mut frac = 0;
    if b.get(i) == Some(&b'.') {
        frac = digits(i + 1);
        i += 1 + frac;
    }
    if int + frac == 0 {
        return f64::NAN;
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        let mut j = i + 1;
        if matches!(b.get(j), Some(b'+' | b'-')) {
            j += 1;
        }
        let e = digits(j);
        if e == 0 {
            return f64::NAN;
        }
        i = j + e;
    }
    if i != b.len() {
        return f64::NAN;
    }
    t.parse::<f64>().unwrap_or(f64::NAN)
}

/// The number of transfer rows to collect; `Err` is a negative or fractional value.
fn resolve_limit(n: f64) -> Result<u64, ()> {
    if n.is_nan() || n == 0.0 {
        return Ok(DEFAULT_LIMIT);
    }
    let n = n.min(MAX_LIMIT as f64);
    if n < 0.0 || n.fract() != 0.0 {
        return Err(());
    }
    Ok(n as u64)
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let hex = |c: u8| (c as char).to_digit(16);
            if let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2])) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Request parameters: a non-empty path segment wins over the query value.
#[derive(Debug, PartialEq)]
struct Params {
    account: String,
    namespace: String,
    limit: Result<u64, ()>,
}

/// `None`: more than three path segments (no such route).
fn params(path: &str, query: &[(String, String)]) -> Option<Params> {
    let rest = path.strip_prefix("/v1/txs").unwrap_or("");
    let rest = rest.strip_prefix('/').unwrap_or(rest);
    let segs: Vec<String> = if rest.is_empty() {
        Vec::new()
    } else {
        rest.split('/').map(pct_decode).collect()
    };
    if segs.len() > 3 && segs[3..].iter().any(|s| !s.is_empty()) {
        return None;
    }
    let pick = |i: usize, name: &str| -> String {
        match segs.get(i).filter(|s| !s.is_empty()) {
            Some(s) => s.clone(),
            None => query
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_default(),
        }
    };
    let namespace = pick(1, "namespace");
    Some(Params {
        account: pick(0, "account"),
        namespace: if namespace.is_empty() {
            "coin".to_owned()
        } else {
            namespace
        },
        limit: resolve_limit(js_number(&pick(2, "limit"))),
    })
}

/// Tx keys of the newest `limit` transfer rows of `token` for `account`.
fn token_rows(view: &View, account: &str, token: &str, limit: usize) -> ApiResult<Vec<TxKey>> {
    let d = view.dict();
    let (Some(a), Some(m)) = (d.account_id(account), d.module_id(token)) else {
        return Ok(Vec::new());
    };
    let it = view.postings(Family::Xfer, xfer_entity(a, m), PostingQuery::default())?;
    let mut src = Src::keys_only(account, token);
    let pg = Paging {
        limit,
        offset: 0,
        next: None,
        min_height: None,
        max_height: None,
        empty: false,
    };
    collect_page(view, Candidates::Keys(it.peekable()), &mut src, &pg, None)?;
    Ok(src.take_keys())
}

/// The JSON array of entries, as bytes; `cap` bounds the body.
fn entries(
    view: &View,
    account: &str,
    namespace: &str,
    limit: usize,
    cap: usize,
) -> ApiResult<Vec<u8>> {
    let mut keys = token_rows(view, account, namespace, limit)?;
    if namespace != "coin" {
        keys.extend(token_rows(view, account, "coin", limit)?);
    }
    let mut seen = HashSet::new();
    let mut body = vec![b'['];
    let mut first = true;
    for k in keys {
        // A tx with several matching rows counts once; miner rewards have no entry.
        if k.is_coinbase() || !seen.insert(k) {
            continue;
        }
        view.check_deadline()?;
        let Some(r) = view.tx(k)? else { continue };
        let Some(b) = view.block(k.chain(), k.height())? else {
            continue;
        };
        let Some(d) = shapes::tx_detail(view, &r, &b)? else {
            continue;
        };
        if !first {
            body.push(b',');
        }
        first = false;
        write_aeson(&d, &mut body);
        if body.len() > cap {
            return Err(ApiError::Unavailable(format!(
                "response larger than {} MB: use a smaller limit",
                cap >> 20
            )));
        }
    }
    body.push(b']');
    Ok(body)
}

async fn handler(
    State(st): State<Arc<ApiState>>,
    uri: Uri,
    Query(q): Query<Vec<(String, String)>>,
) -> Response {
    let not_found = || text(StatusCode::NOT_FOUND, "Not Found");
    let Some(p) = params(uri.path(), &q) else {
        return not_found();
    };
    if p.account.is_empty() {
        return text(StatusCode::BAD_REQUEST, "Bad Request");
    }
    let Ok(limit) = p.limit else {
        return not_found();
    };
    let cap = st.settings.v1txs_max_body_bytes;
    match st
        .read_heavy(move |v| entries(v, &p.account, &p.namespace, limit as usize, cap))
        .await
    {
        Ok(body) => json_bytes(body),
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect()
    }

    #[test]
    fn numbers() {
        assert_eq!(js_number(" 50 "), 50.0);
        assert_eq!(js_number("0x10"), 16.0);
        assert_eq!(js_number("1e2"), 100.0);
        assert_eq!(js_number(".5"), 0.5);
        assert_eq!(js_number(""), 0.0);
        assert!(js_number("abc").is_nan());
        assert!(js_number("inf").is_nan());
        assert!(js_number("1e").is_nan());
        assert!(js_number("-0x10").is_nan());
        assert_eq!(js_number("Infinity"), f64::INFINITY);
    }

    #[test]
    fn limits() {
        assert_eq!(resolve_limit(js_number("")), Ok(50));
        assert_eq!(resolve_limit(js_number("abc")), Ok(50));
        assert_eq!(resolve_limit(js_number("0")), Ok(50));
        assert_eq!(resolve_limit(js_number("-0")), Ok(50));
        assert_eq!(resolve_limit(js_number("250")), Ok(250));
        assert_eq!(resolve_limit(js_number("20000")), Ok(10_000));
        assert_eq!(resolve_limit(js_number("0x10")), Ok(16));
        assert_eq!(resolve_limit(js_number("-5")), Err(()));
        assert_eq!(resolve_limit(js_number("2.5")), Err(()));
        assert_eq!(resolve_limit(js_number("2.0")), Ok(2));
    }

    #[test]
    fn path_and_query() {
        let p = params("/v1/txs/k:abc/kaddex.kdx/10", &[]).unwrap();
        assert_eq!(
            (p.account.as_str(), p.namespace.as_str(), p.limit),
            ("k:abc", "kaddex.kdx", Ok(10))
        );
        let p = params(
            "/v1/txs/k:abc",
            &q(&[("namespace", "n_x.y"), ("limit", "0x10")]),
        )
        .unwrap();
        assert_eq!((p.namespace.as_str(), p.limit), ("n_x.y", Ok(16)));
        // the path wins; an empty segment falls back to the query
        let p = params("/v1/txs/k:abc/coin/5", &q(&[("limit", "9")])).unwrap();
        assert_eq!(p.limit, Ok(5));
        let p = params("/v1/txs//coin/5", &q(&[("account", "q:r")])).unwrap();
        assert_eq!(p.account, "q:r");
        let p = params("/v1/txs", &[]).unwrap();
        assert!(p.account.is_empty());
        assert_eq!((p.namespace.as_str(), p.limit), ("coin", Ok(50)));
        let p = params("/v1/txs/a%20b%2Fc/coin/-1", &[]).unwrap();
        assert_eq!((p.account.as_str(), p.limit), ("a b/c", Err(())));
        assert!(params("/v1/txs/a/b/c/d", &[]).is_none());
        assert_eq!(pct_decode("%zz%4"), "%zz%4");
    }
}
