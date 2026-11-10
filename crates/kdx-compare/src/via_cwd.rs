//! The `kda_txs_via_cwd` target: answers `GET /v1/txs/{account}/{namespace}/{limit}` by
//! composing chainweb-data calls, per the `/v1/txs` behaviour note (section 3). Used when no
//! live kda-txs is reachable. No cache: every request recomposes from scratch.
//!
//! 1. newest `limit` transfer rows of `namespace` for the account (`/txs/account`, paged by
//!    `offset`, 250 rows per page), then, when `namespace` is not `coin`, the same for `coin`;
//! 2. request keys of those rows in row order, duplicates dropped (first kept);
//! 3. one `/txs/txs?requestkey=` per key, the first `TxDetail` of each; a key that cannot be
//!    resolved is dropped.
//!
//! Harness choice: a transport error or a 5xx from chainweb-data is an error of this target
//! (class `b-error`), not a silent drop, so a flaky upstream does not show up as a diff.

use std::time::Instant;

use anyhow::Context;
use bytes::Bytes;
use serde_json::Value;

use crate::gen::{enc_query, enc_segment, is_request_key};
use crate::target::{HttpCapture, Target};

pub const DEFAULT_LIMIT: u64 = 50;
pub const MAX_LIMIT: u64 = 10_000;
/// chainweb-data's page cap for `/txs/account`.
const PAGE: u64 = 250;

/// A negative or fractional `limit`: chainweb-data rejects it, so the live service answers 404.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadLimit;

/// A parsed `/v1/txs` request.
#[derive(Debug, Clone, PartialEq)]
pub struct Params {
    pub account: String,
    pub namespace: String,
    /// `Err(BadLimit)`: the live service answers `404` (negative or fractional `limit`).
    pub limit: Result<u64, BadLimit>,
}

pub(crate) fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let h = |c: u8| (c as char).to_digit(16);
            if let (Some(x), Some(y)) = (h(b[i + 1]), h(b[i + 2])) {
                out.push((x * 16 + y) as u8);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn is_js_ws(c: char) -> bool {
    c.is_whitespace() || c == '\u{feff}'
}

/// JavaScript `Number(s)` for the string forms a query or path value can take.
pub fn js_number(s: &str) -> f64 {
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
    // StrDecimalLiteral: [+-] (digits [. digits?] | . digits) ([eE] [+-] digits)?
    let mut i = 0;
    if matches!(b[0], b'+' | b'-') {
        i = 1;
    }
    let digits = |from: usize| b[from..].iter().take_while(|c| c.is_ascii_digit()).count();
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

/// The live service's `limit` rule: `NaN`/`0`/empty -> 50, cap 10,000, negative or fractional
/// -> rejected (404).
pub fn resolve_limit(n: f64) -> Result<u64, BadLimit> {
    if n.is_nan() || n == 0.0 {
        return Ok(DEFAULT_LIMIT);
    }
    let n = n.min(MAX_LIMIT as f64);
    if n < 0.0 || n.fract() != 0.0 {
        return Err(BadLimit);
    }
    Ok(n as u64)
}

/// Splits `/v1/txs/{account}/{namespace}/{limit}?account=&namespace=&limit=`: a non-empty path
/// value wins over the query value.
pub fn parse(path_and_query: &str) -> Params {
    let (path, query) = path_and_query
        .split_once('?')
        .unwrap_or((path_and_query, ""));
    let rest = path.strip_prefix("/v1/txs").unwrap_or("");
    let mut seg = rest.strip_prefix('/').unwrap_or(rest).split('/');
    let mut take = |name: &str| -> String {
        let p = seg.next().map(pct_decode).unwrap_or_default();
        if !p.is_empty() {
            return p;
        }
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default()
    };
    let account = take("account");
    let mut namespace = take("namespace");
    let limit = take("limit");
    if namespace.is_empty() {
        namespace = "coin".into();
    }
    Params {
        account,
        namespace,
        limit: resolve_limit(js_number(&limit)),
    }
}

fn text(status: u16, body: &str, t0: Instant) -> HttpCapture {
    HttpCapture {
        status,
        headers: vec![("content-type".into(), "text/plain".into())],
        body: Bytes::from(body.to_string()),
        ms: t0.elapsed().as_millis() as u64,
    }
}

/// The newest `limit` request keys (rows, with repeats) of `module` for `account`; `None` when
/// chainweb-data answered a client error or a body that is not an array (the live service
/// answers 404 then).
async fn rows(
    cwd: &Target,
    account: &str,
    module: &str,
    limit: u64,
) -> anyhow::Result<Option<Vec<String>>> {
    let mut keys = Vec::new();
    while (keys.len() as u64) < limit {
        let want = (limit - keys.len() as u64).min(PAGE);
        let path = format!(
            "/txs/account/{}?token={}&limit={want}&offset={}",
            enc_segment(account),
            enc_query(module),
            keys.len()
        );
        let cap = cwd.get_direct(&path).await?;
        if cap.status >= 500 {
            anyhow::bail!("chainweb-data {path}: status {}", cap.status);
        }
        let Some(Value::Array(page)) = (cap.status == 200).then(|| cap.json()).flatten() else {
            return Ok(None);
        };
        let n = page.len() as u64;
        keys.extend(page.iter().map(|r| {
            r.get("requestKey")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        }));
        if n < want {
            break;
        }
    }
    Ok(Some(keys))
}

/// `GET {cwd}/v1/txs/...` composed from chainweb-data calls.
pub async fn fetch(cwd: &Target, path_and_query: &str) -> anyhow::Result<HttpCapture> {
    let t0 = Instant::now();
    let p = parse(path_and_query);
    if p.account.is_empty() {
        return Ok(text(400, "Bad Request", t0));
    }
    let Ok(limit) = p.limit else {
        return Ok(text(404, "Not Found", t0));
    };
    let mut modules = vec![p.namespace.as_str()];
    if p.namespace != "coin" {
        modules.push("coin");
    }
    let mut keys: Vec<String> = Vec::new();
    for m in modules {
        match rows(cwd, &p.account, m, limit).await? {
            Some(k) => keys.extend(k),
            None => return Ok(text(404, "Not Found", t0)),
        }
    }
    let mut seen = std::collections::HashSet::new();
    keys.retain(|k| seen.insert(k.clone()));
    let mut out = Vec::new();
    for rk in keys {
        // Coinbase rows carry `cb`; never a real request key, never resolvable.
        if !is_request_key(&rk) {
            continue;
        }
        let cap = cwd
            .get_direct(&format!("/txs/txs?requestkey={}", enc_query(&rk)))
            .await?;
        if cap.status >= 500 {
            anyhow::bail!("chainweb-data /txs/txs {rk}: status {}", cap.status);
        }
        if cap.status != 200 {
            continue;
        }
        if let Some(Value::Array(v)) = cap.json() {
            if let Some(first) = v.into_iter().find(|d| {
                d.get("requestKey")
                    .and_then(Value::as_str)
                    .is_some_and(|s| !s.is_empty())
            }) {
                out.push(first);
            }
        }
    }
    let body = serde_json::to_vec(&Value::Array(out)).context("encoding /v1/txs answer")?;
    Ok(HttpCapture {
        status: 200,
        headers: vec![("content-type".into(), "application/json".into())],
        body: Bytes::from(body),
        ms: t0.elapsed().as_millis() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn js_numbers() {
        assert_eq!(js_number(" 50 "), 50.0);
        assert_eq!(js_number("0x10"), 16.0);
        assert_eq!(js_number("1e2"), 100.0);
        assert_eq!(js_number(".5"), 0.5);
        assert_eq!(js_number("5."), 5.0);
        assert_eq!(js_number(""), 0.0);
        assert!(js_number("abc").is_nan());
        assert!(js_number("inf").is_nan());
        assert!(js_number("nan").is_nan());
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
        assert_eq!(resolve_limit(js_number("99999")), Ok(10_000));
        assert_eq!(resolve_limit(js_number("Infinity")), Ok(10_000));
        assert_eq!(resolve_limit(js_number("-5")), Err(BadLimit));
        assert_eq!(resolve_limit(js_number("1.5")), Err(BadLimit));
        assert_eq!(resolve_limit(js_number("2.0")), Ok(2));
    }

    #[test]
    fn params() {
        let p = parse("/v1/txs/k:abc/kaddex.kdx/10");
        assert_eq!(p.account, "k:abc");
        assert_eq!(p.namespace, "kaddex.kdx");
        assert_eq!(p.limit, Ok(10));
        let p = parse("/v1/txs/k:abc?namespace=n_x.y&limit=0x10");
        assert_eq!(p.namespace, "n_x.y");
        assert_eq!(p.limit, Ok(16));
        // path wins; empty path segment falls back to the query
        let p = parse("/v1/txs/k:abc/coin/5?limit=9");
        assert_eq!(p.limit, Ok(5));
        let p = parse("/v1/txs//coin/5?account=q%3Ar");
        assert_eq!(p.account, "q:r");
        let p = parse("/v1/txs/");
        assert!(p.account.is_empty());
        assert_eq!(p.namespace, "coin");
        let p = parse("/v1/txs/a%20b/coin/-1");
        assert_eq!(p.account, "a b");
        assert_eq!(p.limit, Err(BadLimit));
    }
}
