//! Normaliser (spec section 7.3): applied to each side before `diff`.
//!
//! - Object key order is ignored: keys are sorted (the diff compares objects as maps anyway).
//! - `gasPrice` is a float in chainweb-data, so it is rewritten to the shortest f64 text on
//!   routes that carry it; `diff` then compares it with relative epsilon 1e-12.
//! - ISO-8601 timestamps are compared by `diff` at the coarser precision of the two sides.
//! - Arrays with a route sort key: inside each run of items with equal sort keys the items are
//!   ordered by their canonical text, so ties compare as multisets. Order across different
//!   sort keys is kept as returned (it is part of the API).
//! - Module references `{"refName": .., "refSpec": [..]}` (in module-deploy and other event
//!   params): the `refSpec` interface list is compared as a set. The node lists the interfaces
//!   in its own internal order and the API keeps that order, so two correct sides can differ.

use serde::Serialize;
use serde_json::{Map, Number, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub enum RouteKind {
    Tx,
    Txs,
    Account,
    Events,
    Recent,
    Stats,
    Search,
    Coins,
    /// Native `/api/v1/accounts/{a}/balances` against `/local` (H-1).
    Balances,
    V1Txs,
    Other,
}

impl RouteKind {
    pub fn from_path(path_and_query: &str) -> RouteKind {
        let path = path_and_query.split('?').next().unwrap_or("");
        let path = path.trim_end_matches('/');
        match path {
            "/txs/tx" => RouteKind::Tx,
            "/txs/txs" => RouteKind::Txs,
            "/txs/events" => RouteKind::Events,
            "/txs/recent" => RouteKind::Recent,
            "/txs/search" => RouteKind::Search,
            "/stats" => RouteKind::Stats,
            "/coins" => RouteKind::Coins,
            p if p.starts_with("/txs/account/") => RouteKind::Account,
            p if p.starts_with("/api/v1/accounts/") && p.contains("/balances") => {
                RouteKind::Balances
            }
            p if p.starts_with("/v1/txs/") => RouteKind::V1Txs,
            _ => RouteKind::Other,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RouteKind::Tx => "tx",
            RouteKind::Txs => "txs",
            RouteKind::Account => "account",
            RouteKind::Events => "events",
            RouteKind::Recent => "recent",
            RouteKind::Stats => "stats",
            RouteKind::Search => "search",
            RouteKind::Coins => "coins",
            RouteKind::Balances => "balances",
            RouteKind::V1Txs => "v1txs",
            RouteKind::Other => "other",
        }
    }

    /// Routes paged with `Chainweb-Next`.
    pub fn paginated(self) -> bool {
        matches!(
            self,
            RouteKind::Account | RouteKind::Events | RouteKind::Search
        )
    }

    /// Routes whose chainweb-data payload has a float `gasPrice`.
    fn float_gas_price(self) -> bool {
        matches!(
            self,
            RouteKind::Tx | RouteKind::Txs | RouteKind::V1Txs | RouteKind::Other
        )
    }
}

/// The JSON keys whose numbers are floats on the chainweb-data side.
pub const FLOAT_KEYS: &[&str] = &["gasPrice"];

pub fn normalize(route: &RouteKind, v: &mut Value) {
    sort_keys(v);
    refspec_as_set(v);
    if route.float_gas_price() {
        canon_floats(v);
    }
    if let Value::Array(items) = v {
        match route {
            RouteKind::Account => tie_groups(items, account_key),
            RouteKind::Events => tie_groups(items, events_key),
            RouteKind::V1Txs => v1txs_as_set(items),
            RouteKind::Txs => txs_as_set(items),
            _ => {}
        }
    }
}

/// `/v1/txs` answers are sets keyed by `requestKey` (the order is not meaningful and the live
/// service adds a cache `_id` to some entries).
fn v1txs_as_set(items: &mut [Value]) {
    for it in items.iter_mut() {
        if let Some(m) = it.as_object_mut() {
            m.remove("_id");
        }
    }
    items.sort_by_cached_key(|v| {
        (
            v.get("requestKey")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            v.to_string(),
        )
    });
}

/// `/txs/txs` answers are sets: chainweb-data's query has no order, and a genesis request key
/// has one row per chain (F4). Sorted by chain, height and block.
fn txs_as_set(items: &mut [Value]) {
    items.sort_by_cached_key(|v| {
        (
            v.get("chain").and_then(Value::as_u64),
            v.get("height").and_then(Value::as_u64),
            v.get("blockHash")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            v.to_string(),
        )
    });
}

fn sort_keys(v: &mut Value) {
    match v {
        Value::Object(m) => {
            let mut entries: Vec<(String, Value)> = std::mem::take(m).into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut out = Map::with_capacity(entries.len());
            for (k, mut val) in entries {
                sort_keys(&mut val);
                out.insert(k, val);
            }
            *m = out;
        }
        Value::Array(a) => a.iter_mut().for_each(sort_keys),
        _ => {}
    }
}

/// Sort the `refSpec` list of every module reference `{"refName", "refSpec"}` by canonical text
/// (run after `sort_keys`), so the interface list compares as a set.
fn refspec_as_set(v: &mut Value) {
    match v {
        Value::Object(m) => {
            let is_ref = m.len() == 2 && m.contains_key("refName");
            for (k, val) in m.iter_mut() {
                if is_ref && k == "refSpec" {
                    if let Value::Array(a) = val {
                        a.sort_by_cached_key(|x| x.to_string());
                    }
                }
                refspec_as_set(val);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(refspec_as_set),
        _ => {}
    }
}

fn canon_floats(v: &mut Value) {
    match v {
        Value::Object(m) => {
            for (k, val) in m.iter_mut() {
                if FLOAT_KEYS.contains(&k.as_str()) {
                    if let Some(f) = number_f64(val) {
                        if let Some(n) = Number::from_f64(f) {
                            *val = Value::Number(n);
                        }
                    }
                } else {
                    canon_floats(val);
                }
            }
        }
        Value::Array(a) => a.iter_mut().for_each(canon_floats),
        _ => {}
    }
}

/// A number's value as f64 (numbers only; decimal objects are left alone).
pub fn number_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.to_string().parse::<f64>().ok(),
        _ => None,
    }
}

type SortKey = Vec<String>;

fn field(v: &Value, k: &str) -> String {
    v.get(k).map(|x| x.to_string()).unwrap_or_default()
}

/// Rows of one height (`height desc`): chainweb-data returns the rows of one height, across
/// chains and within a block, in Postgres physical order (seen live: coinbase rows of chains
/// 8,6,1,7,0,2,... at one height), so a height compares as a multiset (allowlist
/// `same-height-tie-order`, which extends the accepted `coinbase-tie-order`). Missing or extra
/// rows still show; only their order inside one height is ignored.
fn account_key(v: &Value) -> SortKey {
    vec![field(v, "height")]
}

/// Same as [`account_key`].
fn events_key(v: &Value) -> SortKey {
    vec![field(v, "height")]
}

/// `/txs/search` rows within one height as a set (fv-checklist T-item, R3E-8): rows of one
/// height (adjacent, `height desc`) are ordered by their canonical text; rows across heights
/// keep their order. Returns whether the order changed.
pub fn height_ties_as_set(items: &mut [Value]) -> bool {
    let before = items.to_vec();
    tie_groups(items, account_key);
    items != before.as_slice()
}

fn tie_groups(items: &mut [Value], key: fn(&Value) -> SortKey) {
    let mut start = 0;
    while start < items.len() {
        let k = key(&items[start]);
        let mut end = start + 1;
        while end < items.len() && key(&items[end]) == k {
            end += 1;
        }
        if end - start > 1 {
            items[start..end].sort_by_cached_key(|v| v.to_string());
        }
        start = end;
    }
}

/// A parsed ISO-8601 UTC timestamp: whole seconds since the epoch and the fraction digits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsoTime {
    pub secs: i64,
    pub frac: String,
}

/// Parses `YYYY-MM-DDTHH:MM:SS[.f+]Z` (also accepts `+00:00`). Anything else is `None`.
pub fn parse_iso(s: &str) -> Option<IsoTime> {
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' {
        return None;
    }
    if b[16] != b':' {
        return None;
    }
    let num = |r: std::ops::Range<usize>| -> Option<i64> {
        let t = s.get(r)?;
        if !t.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        t.parse().ok()
    };
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (h, mi, se) = (num(11..13)?, num(14..16)?, num(17..19)?);
    let rest = &s[19..];
    let (frac, tz) = match rest.strip_prefix('.') {
        Some(r) => {
            let n = r.bytes().take_while(u8::is_ascii_digit).count();
            if n == 0 {
                return None;
            }
            (r[..n].to_string(), &r[n..])
        }
        None => (String::new(), rest),
    };
    if tz != "Z" && tz != "+00:00" {
        return None;
    }
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || se > 60 {
        return None;
    }
    let days = crate::report::days_from_civil(y, mo as u32, d as u32);
    Some(IsoTime {
        secs: days * 86_400 + h * 3600 + mi * 60 + se,
        frac,
    })
}

/// Equal at the coarser precision of the two (fraction truncated to the shorter length,
/// missing digits read as zeros).
pub fn iso_eq_coarse(a: &IsoTime, b: &IsoTime) -> bool {
    if a.secs != b.secs {
        return false;
    }
    let n = a.frac.len().min(b.frac.len());
    let ta = a.frac[..n].trim_end_matches('0');
    let tb = b.frac[..n].trim_end_matches('0');
    ta == tb
}

/// `YYYY-MM-DDTHH:MM:SS.ffffffZ` from microseconds since the epoch.
pub fn iso_micros(us: u64) -> String {
    let secs = us / 1_000_000;
    let f = us % 1_000_000;
    let s = crate::report::iso_secs(secs);
    format!("{}.{f:06}Z", s.trim_end_matches('Z'))
}

#[cfg(test)]
mod refspec_tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn refspec_compares_as_set() {
        let mut a: Value = serde_json::from_str(
            r#"[{"refName":{"name":"coin","namespace":null},"refSpec":[{"name":"fungible-xchain-v1","namespace":null},{"name":"fungible-v2","namespace":null}]}]"#,
        )
        .unwrap();
        let mut b: Value = serde_json::from_str(
            r#"[{"refSpec":[{"namespace":null,"name":"fungible-v2"},{"namespace":null,"name":"fungible-xchain-v1"}],"refName":{"namespace":null,"name":"coin"}}]"#,
        )
        .unwrap();
        normalize(&RouteKind::Tx, &mut a);
        normalize(&RouteKind::Tx, &mut b);
        assert_eq!(a.to_string(), b.to_string());
    }

    /// F4: /txs/txs rows compare as a set (chainweb-data's query has no order).
    #[test]
    fn txs_rows_as_set() {
        let mut a = json!([
            {"chain": 10, "height": 852054, "blockHash": "y"},
            {"chain": 0, "height": 0, "blockHash": "x"}
        ]);
        let mut b = json!([
            {"chain": 0, "height": 0, "blockHash": "x"},
            {"chain": 10, "height": 852054, "blockHash": "y"}
        ]);
        normalize(&RouteKind::Txs, &mut a);
        normalize(&RouteKind::Txs, &mut b);
        assert_eq!(a, b);
    }
}
