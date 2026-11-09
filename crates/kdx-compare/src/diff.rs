//! Structural JSON diff (spec section 7.3) and paginated fetch.

use serde_json::Value;

use crate::normalize::{iso_eq_coarse, number_f64, parse_iso, RouteKind, FLOAT_KEYS};
pub use crate::report::{DiffEntry, DiffKind};
use crate::target::{HttpCapture, Target};

/// At most this many entries per diff.
pub const MAX_ENTRIES: usize = 50;
const FLOAT_REL_EPS: f64 = 1e-12;
/// `/stats.transactionCount` is a Postgres estimate in chainweb-data.
const STATS_COUNT_TOLERANCE: f64 = 0.01;
/// Path used to report a `/stats` count difference that is inside the tolerance (allowlisted
/// as `stats-estimate`, so the hit is visible in the summary).
pub const STATS_ESTIMATE_PATH: &str = "/transactionCount~estimate";

/// Diff of two (normalised) values. Objects compare as maps, arrays positionally.
pub fn diff(a: &Value, b: &Value) -> Vec<DiffEntry> {
    let mut out = Vec::new();
    walk(a, b, &mut String::new(), None, &mut out);
    out
}

/// `diff` plus route rules that need both sides (the `/stats` count tolerance).
pub fn diff_route(route: &RouteKind, a: &Value, b: &Value) -> Vec<DiffEntry> {
    if *route == RouteKind::Stats {
        let (mut a2, mut b2) = (a.clone(), b.clone());
        let ca = a.get("transactionCount").and_then(number_f64);
        let cb = b.get("transactionCount").and_then(number_f64);
        let mut extra = None;
        if let (Some(x), Some(y)) = (ca, cb) {
            if x != y && (x - y).abs() <= STATS_COUNT_TOLERANCE * x.abs().max(y.abs()) {
                for v in [&mut a2, &mut b2] {
                    if let Some(m) = v.as_object_mut() {
                        m.remove("transactionCount");
                    }
                }
                extra = Some(DiffEntry {
                    path: STATS_ESTIMATE_PATH.into(),
                    kind: DiffKind::Changed,
                    a: a.get("transactionCount").cloned(),
                    b: b.get("transactionCount").cloned(),
                });
            }
        }
        let mut d = diff(&a2, &b2);
        if let Some(e) = extra {
            d.insert(0, e);
            d.truncate(MAX_ENTRIES);
        }
        return d;
    }
    diff(a, b)
}

fn escape(k: &str) -> String {
    k.replace('~', "~0").replace('/', "~1")
}

fn kind_of(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

fn push(
    out: &mut Vec<DiffEntry>,
    path: &str,
    kind: DiffKind,
    a: Option<&Value>,
    b: Option<&Value>,
) {
    if out.len() < MAX_ENTRIES {
        out.push(DiffEntry {
            path: if path.is_empty() {
                "/".into()
            } else {
                path.to_string()
            },
            kind,
            a: a.cloned(),
            b: b.cloned(),
        });
    }
}

fn walk(a: &Value, b: &Value, path: &mut String, key: Option<&str>, out: &mut Vec<DiffEntry>) {
    if out.len() >= MAX_ENTRIES {
        return;
    }
    match (a, b) {
        (Value::Object(ma), Value::Object(mb)) => {
            for (k, va) in ma {
                let len = path.len();
                path.push('/');
                path.push_str(&escape(k));
                match mb.get(k) {
                    Some(vb) => walk(va, vb, path, Some(k), out),
                    None => push(out, path, DiffKind::Extra, Some(va), None),
                }
                path.truncate(len);
            }
            for (k, vb) in mb {
                if !ma.contains_key(k) {
                    let p = format!("{path}/{}", escape(k));
                    push(out, &p, DiffKind::Missing, None, Some(vb));
                }
            }
        }
        (Value::Array(xa), Value::Array(xb)) => {
            for i in 0..xa.len().max(xb.len()) {
                let len = path.len();
                path.push('/');
                path.push_str(&i.to_string());
                match (xa.get(i), xb.get(i)) {
                    (Some(va), Some(vb)) => walk(va, vb, path, None, out),
                    (Some(va), None) => push(out, path, DiffKind::Extra, Some(va), None),
                    (None, Some(vb)) => push(out, path, DiffKind::Missing, None, Some(vb)),
                    (None, None) => {}
                }
                path.truncate(len);
            }
        }
        (Value::Number(na), Value::Number(nb)) => {
            if na.to_string() == nb.to_string() {
                return;
            }
            if key.is_some_and(|k| FLOAT_KEYS.contains(&k)) {
                if let (Some(x), Some(y)) = (number_f64(a), number_f64(b)) {
                    if x == y || (x - y).abs() <= FLOAT_REL_EPS * x.abs().max(y.abs()) {
                        return;
                    }
                }
            }
            push(out, path, DiffKind::Changed, Some(a), Some(b));
        }
        (Value::String(sa), Value::String(sb)) => {
            if sa == sb {
                return;
            }
            if let (Some(ta), Some(tb)) = (parse_iso(sa), parse_iso(sb)) {
                if iso_eq_coarse(&ta, &tb) {
                    return;
                }
            }
            push(out, path, DiffKind::Changed, Some(a), Some(b));
        }
        _ if a == b => {}
        _ if kind_of(a) == kind_of(b) => push(out, path, DiffKind::Changed, Some(a), Some(b)),
        _ => push(out, path, DiffKind::Type, Some(a), Some(b)),
    }
}

/// Result of following `Chainweb-Next` pages.
pub struct Paged {
    /// Captures in page order (the first one carries the status compared).
    pub pages: Vec<HttpCapture>,
    /// Concatenated items (the first body as is if it is not an array).
    pub items: Value,
}

/// `path` with `offset` and `next` removed and `next=<token>` appended.
pub fn with_next(path_and_query: &str, token: &str) -> String {
    let (p, q) = path_and_query
        .split_once('?')
        .unwrap_or((path_and_query, ""));
    let mut parts: Vec<String> = q
        .split('&')
        .filter(|s| !s.is_empty() && !s.starts_with("offset=") && !s.starts_with("next="))
        .map(str::to_string)
        .collect();
    let enc: String = url::form_urlencoded::byte_serialize(token.as_bytes()).collect();
    parts.push(format!("next={enc}"));
    format!("{p}?{}", parts.join("&"))
}

/// GETs `path` and follows `Chainweb-Next` up to `depth` further pages, concatenating the
/// item arrays. Tokens are never compared.
pub async fn follow_pages(target: &Target, path: &str, depth: u32) -> anyhow::Result<Paged> {
    let first = target.get(path).await?;
    let mut items = match first.json() {
        Some(v) => v,
        None => Value::Null,
    };
    let mut next = if first.status == 200 && items.is_array() {
        first.next_token().map(str::to_string)
    } else {
        None
    };
    let mut pages = vec![first];
    let mut followed = 0;
    while let Some(tok) = next.take() {
        if followed >= depth {
            break;
        }
        followed += 1;
        let cap = target.get(&with_next(path, &tok)).await?;
        if cap.status != 200 {
            pages.push(cap);
            break;
        }
        if let (Some(Value::Array(more)), Value::Array(all)) = (cap.json(), &mut items) {
            all.extend(more);
        }
        next = cap.next_token().map(str::to_string);
        pages.push(cap);
    }
    Ok(Paged { pages, items })
}
