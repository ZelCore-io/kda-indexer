//! Structural JSON diff (spec section 7.3) and paginated fetch.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::normalize::{iso_eq_coarse, number_f64, parse_iso, RouteKind, FLOAT_KEYS};
pub use crate::report::{DiffEntry, DiffKind};
use crate::target::{HttpCapture, Target};

/// At most this many entries per diff.
pub const MAX_ENTRIES: usize = 50;
const FLOAT_REL_EPS: f64 = 1e-12;
/// `/stats.transactionCount` is a Postgres estimate in chainweb-data.
const STATS_COUNT_TOLERANCE: f64 = 0.01;
/// `/stats.coinsInCirculation`: both sides compute the supply at their own height and time.
const STATS_SUPPLY_TOLERANCE: f64 = 1e-6;
/// Path used to report a `/stats` supply difference that is inside the tolerance.
pub const STATS_SUPPLY_PATH: &str = "/coinsInCirculation~tolerance";
/// `/coins`: a text decimal that both sides compute at their own tip (same tolerance).
const COINS_SUPPLY_TOLERANCE: f64 = 1e-6;
/// Path used to report a `/coins` difference inside the tolerance (allowlisted as
/// `coins-supply-tolerance`, so the hit is visible); a bigger one stays a root diff.
pub const COINS_SUPPLY_PATH: &str = "/~coins-supply-tolerance";
/// Path used to report a `/stats` count difference that is inside the tolerance (allowlisted
/// as `stats-estimate`, so the hit is visible in the summary).
pub const STATS_ESTIMATE_PATH: &str = "/transactionCount~estimate";

/// Diff of two (normalised) values. Objects compare as maps, arrays positionally.
pub fn diff(a: &Value, b: &Value) -> Vec<DiffEntry> {
    let mut out = Vec::new();
    walk(a, b, &mut String::new(), None, &mut out, &mut None);
    out
}

/// A normaliser consulted at each differing leaf (path, A value, B value): the id of the rule
/// that explains the difference, or `None` when it stays a diff.
pub type Explain<'a> = &'a dyn Fn(&str, &Value, &Value) -> Option<&'static str>;

struct Hook<'a> {
    explain: Explain<'a>,
    hits: BTreeSet<&'static str>,
}

/// [`diff`] with a normaliser: a leaf difference that `explain` accepts is left out (it does
/// not use up [`MAX_ENTRIES`]); returns the remaining entries and the ids of the rules that
/// fired.
pub fn diff_explained(
    a: &Value,
    b: &Value,
    explain: Explain<'_>,
) -> (Vec<DiffEntry>, BTreeSet<&'static str>) {
    let mut out = Vec::new();
    let mut hook = Some(Hook {
        explain,
        hits: BTreeSet::new(),
    });
    walk(a, b, &mut String::new(), None, &mut out, &mut hook);
    (out, hook.map(|h| h.hits).unwrap_or_default())
}

/// `diff` plus route rules that need both sides (the `/stats` tolerances).
pub fn diff_route(route: &RouteKind, a: &Value, b: &Value) -> Vec<DiffEntry> {
    if *route == RouteKind::Coins {
        if let (Some(x), Some(y)) = (num_or_text_f64(a), num_or_text_f64(b)) {
            if x != y && (x - y).abs() <= COINS_SUPPLY_TOLERANCE * x.abs().max(y.abs()) {
                return vec![DiffEntry {
                    path: COINS_SUPPLY_PATH.into(),
                    kind: DiffKind::Changed,
                    a: Some(a.clone()),
                    b: Some(b.clone()),
                }];
            }
        }
    }
    if *route == RouteKind::Stats {
        let (mut a2, mut b2) = (a.clone(), b.clone());
        let mut extra = Vec::new();
        for (key, tol, path) in [
            (
                "transactionCount",
                STATS_COUNT_TOLERANCE,
                STATS_ESTIMATE_PATH,
            ),
            (
                "coinsInCirculation",
                STATS_SUPPLY_TOLERANCE,
                STATS_SUPPLY_PATH,
            ),
        ] {
            let x = a.get(key).and_then(num_or_text_f64);
            let y = b.get(key).and_then(num_or_text_f64);
            if let (Some(x), Some(y)) = (x, y) {
                if x != y && (x - y).abs() <= tol * x.abs().max(y.abs()) {
                    for v in [&mut a2, &mut b2] {
                        if let Some(m) = v.as_object_mut() {
                            m.remove(key);
                        }
                    }
                    extra.push(DiffEntry {
                        path: path.into(),
                        kind: DiffKind::Changed,
                        a: a.get(key).cloned(),
                        b: b.get(key).cloned(),
                    });
                }
            }
        }
        let mut d = diff(&a2, &b2);
        if !extra.is_empty() {
            d.splice(0..0, extra);
            d.truncate(MAX_ENTRIES);
        }
        return d;
    }
    diff(a, b)
}

/// A number, or a decimal written as text (`/coins`), as f64.
pub(crate) fn num_or_text_f64(v: &Value) -> Option<f64> {
    match v {
        Value::String(s) => s.parse().ok(),
        _ => number_f64(v),
    }
}

/// Items `/txs/recent` returns on each side.
pub const RECENT_ITEMS: usize = 10;

/// `/txs/recent` (both sides normalised): each side has exactly 10 items and is height
/// descending; items present on both sides (by `requestKey`) are diffed. Items on one side
/// only are skew (the two sides read slightly different tips), reported as the second value
/// rather than as diffs.
pub fn recent_diff(a: &Value, b: &Value) -> (Vec<DiffEntry>, bool) {
    let (Some(xa), Some(xb)) = (a.as_array(), b.as_array()) else {
        return (diff(a, b), false);
    };
    let mut out = Vec::new();
    for (side, xs) in [("a", xa), ("b", xb)] {
        if xs.len() != RECENT_ITEMS {
            push(
                &mut out,
                &format!("/@count-{side}"),
                DiffKind::Changed,
                Some(&Value::from(xs.len())),
                Some(&Value::from(RECENT_ITEMS)),
            );
        }
        let heights: Vec<Option<u64>> = xs
            .iter()
            .map(|x| x.get("height").and_then(Value::as_u64))
            .collect();
        let ordered =
            heights.iter().all(Option::is_some) && heights.windows(2).all(|w| w[0] >= w[1]);
        if !ordered {
            push(
                &mut out,
                &format!("/@order-{side}"),
                DiffKind::Changed,
                xs.first(),
                None,
            );
        }
    }
    let key = |v: &Value| {
        v.get("requestKey")
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    let by_b: std::collections::HashMap<String, &Value> =
        xb.iter().filter_map(|v| Some((key(v)?, v))).collect();
    let mut overlap = 0;
    let mut skew = false;
    for va in xa {
        match key(va).and_then(|k| by_b.get(&k).map(|vb| (k, *vb))) {
            Some((k, vb)) => {
                overlap += 1;
                for mut e in diff(va, vb) {
                    e.path = format!("/{k}{}", if e.path == "/" { "" } else { &e.path });
                    if out.len() < MAX_ENTRIES {
                        out.push(e);
                    }
                }
            }
            None => skew = true,
        }
    }
    if overlap < xb.len() {
        skew = true;
    }
    (out, skew)
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

/// A changed leaf: left out when the hook explains it.
fn leaf(
    out: &mut Vec<DiffEntry>,
    path: &str,
    kind: DiffKind,
    a: &Value,
    b: &Value,
    hook: &mut Option<Hook<'_>>,
) {
    if let Some(h) = hook {
        if let Some(id) = (h.explain)(path, a, b) {
            h.hits.insert(id);
            return;
        }
    }
    push(out, path, kind, Some(a), Some(b));
}

fn walk(
    a: &Value,
    b: &Value,
    path: &mut String,
    key: Option<&str>,
    out: &mut Vec<DiffEntry>,
    hook: &mut Option<Hook<'_>>,
) {
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
                    Some(vb) => walk(va, vb, path, Some(k), out, hook),
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
                    (Some(va), Some(vb)) => walk(va, vb, path, None, out, hook),
                    (Some(va), None) => push(out, path, DiffKind::Extra, Some(va), None),
                    (None, Some(vb)) => push(out, path, DiffKind::Missing, None, Some(vb)),
                    (None, None) => {}
                }
                path.truncate(len);
            }
        }
        (Value::Number(na), Value::Number(nb)) => {
            // Same decimal value in another text form (a signed cmd can carry
            // `9.990000000000` where chainweb-data and compat print `9.99`).
            if na.to_string() == nb.to_string()
                || decimal_canon(&na.to_string())
                    .is_some_and(|c| Some(c) == decimal_canon(&nb.to_string()))
            {
                return;
            }
            if key.is_some_and(|k| FLOAT_KEYS.contains(&k)) {
                if let (Some(x), Some(y)) = (number_f64(a), number_f64(b)) {
                    if x == y || (x - y).abs() <= FLOAT_REL_EPS * x.abs().max(y.abs()) {
                        return;
                    }
                }
            }
            leaf(out, path, DiffKind::Changed, a, b, hook);
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
            leaf(out, path, DiffKind::Changed, a, b, hook);
        }
        _ if a == b => {}
        _ if kind_of(a) == kind_of(b) => leaf(out, path, DiffKind::Changed, a, b, hook),
        _ => leaf(out, path, DiffKind::Type, a, b, hook),
    }
}

/// Result of following `Chainweb-Next` pages.
pub struct Paged {
    /// Captures in page order (the first one carries the status compared).
    pub pages: Vec<HttpCapture>,
    /// Concatenated items (the first body as is if it is not an array).
    pub items: Value,
    /// Stopped at the follow depth while a `Chainweb-Next` token was still pending.
    pub truncated: bool,
    /// Rows dropped because a page started with the previous page's last row.
    pub boundary_dups: u32,
}

/// JSON path of the visible allowlist hit for rows repeated across a page boundary.
pub const PAGE_DUP_PATH: &str = "/~page-dup";

/// chainweb-data can start a `Chainweb-Next` page with the last row of the previous page.
/// Such a row is dropped (counted in `boundary_dups`) unless it is a transfer to oneself,
/// which chainweb-data legitimately lists twice.
fn repeats_boundary(all: &[Value], first: Option<&Value>) -> bool {
    let (Some(last), Some(first)) = (all.last(), first) else {
        return false;
    };
    let self_transfer =
        first.get("fromAccount").is_some() && first.get("fromAccount") == first.get("toAccount");
    last == first && !self_transfer
}

/// A JSON number's exact decimal value as `(negative, significant digits, exponent)`, so
/// `9.99`, `9.990` and `999e-2` compare equal. `None` for anything that is not a number.
pub fn decimal_canon(t: &str) -> Option<(bool, String, i64)> {
    let (neg, t) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t),
    };
    let (mant, exp) = match t.find(['e', 'E']) {
        Some(i) => (&t[..i], t[i + 1..].parse::<i64>().ok()?),
        None => (t, 0),
    };
    let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
    if int.is_empty() || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
        return None;
    }
    let all = format!("{int}{frac}");
    let mut exp = exp - frac.len() as i64;
    let digits = all.trim_start_matches('0');
    let trimmed = digits.trim_end_matches('0');
    exp += (digits.len() - trimmed.len()) as i64;
    if trimmed.is_empty() {
        return Some((false, String::new(), 0));
    }
    Some((neg, trimmed.to_string(), exp))
}

/// JSON path of the visible allowlist hit for [`cut_to_common_depth`].
pub const PAGE_CUT_PATH: &str = "/~page-cut";

/// chainweb-data serves pages shorter than `limit` that still carry a next token (its page
/// size is `limit` times a throttling factor). After following the same number of pages one
/// side then holds fewer rows only because it was cut, not because rows are missing. A
/// `truncated` side reached its last height only partly (a height can continue on the next
/// page, and the rows of one height come in no fixed order on chainweb-data), so both item
/// arrays are cut to the rows strictly above the highest such last height. Returns true when
/// it cut anything.
pub fn cut_to_common_depth(a: &mut Value, a_trunc: bool, b: &mut Value, b_trunc: bool) -> bool {
    let (Value::Array(xa), Value::Array(xb)) = (&mut *a, &mut *b) else {
        return false;
    };
    let height = |v: &Value| v.get("height").and_then(Value::as_u64);
    let last = |x: &[Value], t: bool| if t { x.last().and_then(height) } else { None };
    let Some(cut) = last(xa, a_trunc).max(last(xb, b_trunc)) else {
        return false;
    };
    // Rows are height-descending: keep those above the cut height.
    let keep = |v: &Value| height(v).is_some_and(|h| h > cut);
    let before = xa.len() + xb.len();
    xa.retain(keep);
    xb.retain(keep);
    xa.len() + xb.len() != before
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
    let mut truncated = false;
    let mut boundary_dups = 0;
    while let Some(tok) = next.take() {
        if followed >= depth {
            truncated = true;
            break;
        }
        followed += 1;
        let cap = target.get(&with_next(path, &tok)).await?;
        if cap.status != 200 {
            pages.push(cap);
            break;
        }
        if let (Some(Value::Array(more)), Value::Array(all)) = (cap.json(), &mut items) {
            let skip = usize::from(repeats_boundary(all, more.first()));
            boundary_dups += skip as u32;
            all.extend(more.into_iter().skip(skip));
        }
        next = cap.next_token().map(str::to_string);
        pages.push(cap);
    }
    Ok(Paged {
        pages,
        items,
        truncated,
        boundary_dups,
    })
}

#[cfg(test)]
mod harness_rule_tests {
    use super::*;
    use crate::normalize::normalize;
    use serde_json::json;

    /// Items `k{n}` for `n` in `ids`, height `200 - n` (so a lower id is newer... higher).
    fn recent(ids: std::ops::Range<i64>) -> Value {
        Value::Array(
            ids.rev()
                .map(|n| json!({"height": 200 + n, "requestKey": format!("k{n}"), "sender": "a"}))
                .collect(),
        )
    }

    #[test]
    fn recent_overlap_and_skew() {
        let a = recent(0..10);
        // B is one tip ahead: it has one new item, A's oldest item fell off.
        let b = recent(1..11);
        let (d, skew) = recent_diff(&a, &b);
        assert!(d.is_empty(), "{d:?}");
        assert!(skew);
        // identical -> no skew
        let (d, skew) = recent_diff(&a, &a.clone());
        assert!(d.is_empty() && !skew);
        // a changed field on an overlapping item is a diff
        let mut c = b.clone();
        c[2]["sender"] = json!("zzz");
        let (d, _) = recent_diff(&a, &c);
        assert_eq!(d.len(), 1);
        assert!(d[0].path.ends_with("/sender"), "{}", d[0].path);
        // wrong count and order are diffs
        let short = Value::Array(a.as_array().unwrap()[..9].to_vec());
        assert_eq!(recent_diff(&a, &short).0[0].path, "/@count-b");
        let mut unordered = a.clone();
        unordered.as_array_mut().unwrap().swap(0, 9);
        assert_eq!(recent_diff(&a, &unordered).0[0].path, "/@order-b");
    }

    #[test]
    fn stats_tolerances() {
        let a = json!({"transactionCount": 1000, "coinsInCirculation": 379605351.59648});
        let b =
            json!({"transactionCount": 1005, "coinsInCirculation": 379605351.59648 * (1.0 + 5e-7)});
        let d = diff_route(&RouteKind::Stats, &a, &b);
        let paths: Vec<&str> = d.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec![STATS_ESTIMATE_PATH, STATS_SUPPLY_PATH]);
        // outside the tolerance the real field is reported
        let c =
            json!({"transactionCount": 1000, "coinsInCirculation": 379605351.59648 * (1.0 + 5e-6)});
        let d = diff_route(&RouteKind::Stats, &a, &c);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].path, "/coinsInCirculation");
    }

    #[test]
    fn v1txs_set_semantics() {
        let mut a =
            json!([{"requestKey": "b", "_id": "x", "gasPrice": 1.0e-8}, {"requestKey": "a"}]);
        let mut b = json!([{"requestKey": "a"}, {"requestKey": "b", "gasPrice": 1.0e-8}]);
        normalize(&RouteKind::V1Txs, &mut a);
        normalize(&RouteKind::V1Txs, &mut b);
        assert!(diff_route(&RouteKind::V1Txs, &a, &b).is_empty());
    }

    #[test]
    fn cut_drops_partial_last_height_and_ties_compare_as_multiset() {
        let row =
            |h: u64, c: u64| json!({"height": h, "chain": c, "requestKey": "<coinbase>", "idx": 0});
        // Same length, both truncated inside height 5 with different chains of it.
        let mut a = json!([row(7, 1), row(6, 0), row(6, 3), row(5, 0)]);
        let mut b = json!([row(7, 1), row(6, 3), row(6, 0), row(5, 9)]);
        assert!(cut_to_common_depth(&mut a, true, &mut b, true));
        normalize(&RouteKind::Events, &mut a);
        normalize(&RouteKind::Events, &mut b);
        assert_eq!(a.as_array().unwrap().len(), 3);
        assert!(diff_route(&RouteKind::Events, &a, &b).is_empty());
        // Neither side truncated: nothing is cut, a missing row still shows.
        let mut a = json!([row(7, 1), row(6, 0)]);
        let mut b = json!([row(7, 1)]);
        assert!(!cut_to_common_depth(&mut a, false, &mut b, false));
        assert!(!diff_route(&RouteKind::Events, &a, &b).is_empty());
    }

    #[test]
    fn numbers_compare_by_decimal_value() {
        assert_eq!(decimal_canon("9.99"), decimal_canon("9.990000000000"));
        assert_eq!(decimal_canon("999e-2"), decimal_canon("9.99"));
        assert_eq!(decimal_canon("0"), decimal_canon("-0.0"));
        assert_ne!(decimal_canon("9.99"), decimal_canon("9.991"));
        assert_ne!(decimal_canon("100"), decimal_canon("10"));
        let a: Value = serde_json::from_str("[9.99]").unwrap();
        let b: Value = serde_json::from_str("[9.990000000000]").unwrap();
        assert!(diff_route(&RouteKind::Tx, &a, &b).is_empty());
    }
}

#[cfg(test)]
mod explain_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn explained_leaves_are_left_out_and_reported() {
        let ex = |p: &str, _a: &Value, _b: &Value| p.ends_with("/f").then_some("rule-f");
        let a = json!({"f": 1, "g": "x", "o": {"f": "a"}, "t": 1});
        let b = json!({"f": 2, "g": "y", "o": {"f": "b"}, "t": "1"});
        let (d, hits) = diff_explained(&a, &b, &ex);
        let paths: Vec<&str> = d.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, ["/g", "/t"]);
        assert_eq!(hits.into_iter().collect::<Vec<_>>(), ["rule-f"]);
        // Without a hook, the same as `diff`.
        assert_eq!(diff(&a, &b).len(), 4);
    }
}
