//! R-P5P9-57 normalisers of the GraphQL pairs. Each one is narrow: it explains one diff class
//! that the ruling assigns to the harness, and shows as a visible allowlist hit (a
//! `/~graphql-r57-*` path) so the summary counts it. None of them hides a value difference
//! the node confirms:
//!
//! - F2: `Float`/`Decimal` fields and numbers above 2^53 compare at f64 precision (the oracle
//!   computes them as JavaScript floats; ours are exact).
//! - F7: JSON texts that hold objects compare as objects (key order). Numbers keep their text,
//!   so a decimal format difference inside (F9) is not hidden.
//! - M2b/M2c: list rows within one height compare as a set (`sort_ties` in `graphql.rs`), and
//!   a set cut by the page end on both sides is left out ([`cut_trailing_ties`]).
//! - Per-field rules (`[graphql] field_rules`): rule (a) and (b) fields the controller sets
//!   after the node referee (F5, F6, F8, F9, F10, `Transfer.amount`).
//! - NodeBalance: a one-block coinbase skew against `/local` ([`one_block_skew`]).
//! - R-P5P9-58 kinds: `oracle_empty`, `count_lower_bound` (leaf rules) and the row rules
//!   `oracle_orphan_dupes` and `by_key_extra_row` (node-checked, applied before the diff; the
//!   pure parts are here, the node reads in `graphql.rs`). Each has its own visible hit.
//! - R-P5P9-60 kinds (rule (a), node = ours): `oracle_strips_quotes` (`cmd.nonce` with inner
//!   quotes) and `count_self_consistent` (a root count the oracle's own complete list
//!   contradicts). Decimal text in `result.continuation` and `result.goodResult` uses the
//!   existing `json_numbers_by_value`. `payload.rollback` has no rule.

use serde_json::Value;

use crate::config::{FieldRule, FieldRuleKind};
use crate::diff::decimal_canon;

/// Visible hit of the F2 rule (allowlist id `graphql-r57-float`).
pub const F2_PATH: &str = "/~graphql-r57-float";
/// Visible hit of the F7 rule (allowlist id `graphql-r57-json-object`).
pub const F7_PATH: &str = "/~graphql-r57-json-object";
/// Visible hit of a per-field rule (allowlist id `graphql-r57-field`).
pub const FIELD_PATH: &str = "/~graphql-r57-field";
/// Visible hit of a height tie cut by the page end (allowlist id `graphql-r57-height-ties`).
pub const TIE_CUT_PATH: &str = "/~graphql-r57-height-tie-cut";
/// Visible hit of `oracle_empty` (allowlist id `graphql-r58-oracle-empty`).
pub const ORACLE_EMPTY_PATH: &str = "/~graphql-r58-oracle-empty";
/// Visible hit of `count_lower_bound` (allowlist id `graphql-r58-count-lower-bound`).
pub const COUNT_LOWER_BOUND_PATH: &str = "/~graphql-r58-count-lower-bound";
/// Visible hit of `oracle_orphan_dupes` (allowlist id `graphql-r58-oracle-orphan-dupes`).
pub const ORPHAN_DUPES_PATH: &str = "/~graphql-r58-oracle-orphan-dupes";
/// Visible hit of `by_key_extra_row` (allowlist id `graphql-r58-by-key-extra-row`).
pub const BY_KEY_EXTRA_PATH: &str = "/~graphql-r58-by-key-extra-row";
/// Visible hit of `oracle_strips_quotes` (allowlist id `graphql-r60-oracle-strips-quotes`).
pub const STRIPS_QUOTES_PATH: &str = "/~graphql-r60-oracle-strips-quotes";
/// Visible hit of `count_self_consistent` (allowlist id `graphql-r60-count-self-consistent`).
pub const COUNT_SELF_CONSISTENT_PATH: &str = "/~graphql-r60-count-self-consistent";

/// Fields typed `Float` or float-valued `Decimal` in the vendored schema
/// (`crates/kdx-graphql/schema/kadena-graph.graphql`): `FungibleChainAccount.balance`,
/// `FungibleAccount.totalBalance`, `NetworkInfo.coinsInCirculation`, `networkHashRate`,
/// `totalDifficulty` and `TransactionMeta.gasPrice`. `Transfer.amount` is left out: its
/// trailing zeros are a rule (b) field (P7F), and `Block.nonce`/`flags` are whole numbers.
pub const FLOAT_FIELDS: &[&str] = &[
    "balance",
    "totalBalance",
    "coinsInCirculation",
    "networkHashRate",
    "totalDifficulty",
    "gasPrice",
];

/// Whole numbers at or above this compare at f64 precision whatever their field.
const BIG: f64 = 9_007_199_254_740_992.0;
/// Relative tolerance of "f64 precision": a few dozen ulps, enough for the oracle's float sum
/// over 20 chains, far below any balance or difficulty change.
const F64_REL: f64 = 1e-14;

/// A numeric leaf as f64: a JSON number, a canonical number (`#<digits>e<exp>`, see
/// `normalize_gql`) or a decimal text.
pub fn num_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.to_string().parse().ok(),
        Value::String(s) => match s.strip_prefix('#') {
            Some(c) => c.parse().ok(),
            None => decimal_canon(s).and_then(|_| s.parse().ok()),
        },
        _ => None,
    }
}

pub fn f64_eq(x: f64, y: f64) -> bool {
    x == y || (x - y).abs() <= F64_REL * x.abs().max(y.abs())
}

/// The exact decimal value of a numeric leaf (sign, digits, exponent), in any of the forms
/// [`num_f64`] reads.
fn decimal_value(v: &Value) -> Option<(bool, String, i64)> {
    match v {
        Value::Number(n) => decimal_canon(&n.to_string()),
        Value::String(s) => match s.strip_prefix('#') {
            Some(c) => {
                let (m, e) = c.split_once('e')?;
                let (neg, d) = m.strip_prefix('-').map_or((false, m), |d| (true, d));
                Some((neg, d.to_string(), e.parse().ok()?))
            }
            None => decimal_canon(s),
        },
        _ => None,
    }
}

/// The path's field names: list indexes and connection wrappers (`edges/<n>/node`) left out,
/// JSON pointer escapes undone.
fn field_names(path: &str) -> Vec<String> {
    let raw: Vec<String> = path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.replace("~1", "/").replace("~0", "~"))
        .collect();
    let index = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    let mut out = Vec::new();
    for (i, s) in raw.iter().enumerate() {
        let after_edge = i >= 2 && raw[i - 2] == "edges" && index(&raw[i - 1]);
        if index(s) || s == "edges" || (s == "node" && after_edge) {
            continue;
        }
        out.push(s.clone());
    }
    out
}

/// Whether the dotted `field` names the end of `path`.
pub fn field_matches(path: &str, field: &str) -> bool {
    let names = field_names(path);
    let want: Vec<&str> = field.split('.').collect();
    names.len() >= want.len()
        && names[names.len() - want.len()..]
            .iter()
            .zip(&want)
            .all(|(a, b)| a == b)
}

/// JSON texts holding at least one object, equal as values (objects compare as maps; numbers
/// by their text, since the workspace's serde_json keeps number text).
fn same_json_objects(a: &str, b: &str) -> bool {
    fn has_object(v: &Value) -> bool {
        match v {
            Value::Object(_) => true,
            Value::Array(xs) => xs.iter().any(has_object),
            _ => false,
        }
    }
    let starts = |s: &str| matches!(s.trim_start().as_bytes().first(), Some(b'{' | b'['));
    if !starts(a) || !starts(b) {
        return false;
    }
    match (
        serde_json::from_str::<Value>(a),
        serde_json::from_str::<Value>(b),
    ) {
        (Ok(x), Ok(y)) => has_object(&x) && x == y,
        _ => false,
    }
}

/// Structural equality with numbers compared by decimal value.
fn same_by_value(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(_), Value::Number(_)) => decimal_value(a) == decimal_value(b),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same_by_value(p, q))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, p)| y.get(k).is_some_and(|q| same_by_value(p, q)))
        }
        _ => a == b,
    }
}

/// Whether a side's value is exactly `want`: a string's text, or a number's decimal value
/// (a JSON number, or a canonical number `#<digits>e<exp>` from `normalize_gql`).
fn value_is(v: &Value, want: &str) -> bool {
    match v {
        Value::String(s) if !s.starts_with('#') => s == want,
        Value::String(_) | Value::Number(_) => {
            decimal_value(v).is_some_and(|x| decimal_canon(want) == Some(x))
        }
        Value::Bool(b) => want == if *b { "true" } else { "false" },
        _ => false,
    }
}

/// Whether `r` explains the difference `a` (ours) against `b` (the oracle); the visible hit
/// path when it does. `lower_bound`: our response carries `totalCountLowerBound: true`.
pub fn rule_explains(
    r: &FieldRule,
    a: &Value,
    b: &Value,
    lower_bound: bool,
) -> Option<&'static str> {
    let matched = r.ours.as_deref().is_none_or(|w| value_is(a, w))
        && r.oracle.as_deref().is_none_or(|w| value_is(b, w));
    if !matched {
        return None;
    }
    let ok = match r.rule {
        // F5: ours is the oracle's string JSON-encoded once more, never the other way.
        FieldRuleKind::JsonUnquote => {
            matches!((a, b), (Value::String(s), Value::String(t))
                if serde_json::from_str::<String>(s).is_ok_and(|u| &u == t))
        }
        FieldRuleKind::NullOrEmpty => {
            let empty = |v: &Value| v.is_null() || v.as_str() == Some("");
            empty(a) && empty(b)
        }
        FieldRuleKind::DecimalValue => {
            matches!((decimal_value(a), decimal_value(b)), (Some(x), Some(y)) if x == y)
        }
        FieldRuleKind::JsonNumbersByValue => match (a, b) {
            (Value::String(s), Value::String(t)) => match (
                serde_json::from_str::<Value>(s),
                serde_json::from_str::<Value>(t),
            ) {
                (Ok(x), Ok(y)) => same_by_value(&x, &y),
                _ => false,
            },
            _ => false,
        },
        FieldRuleKind::OracleNull => b.is_null() && !a.is_null(),
        FieldRuleKind::OursNull => a.is_null() && !b.is_null(),
        FieldRuleKind::OracleEmpty => b.as_str() == Some("") && !a.is_null(),
        FieldRuleKind::CountLowerBound => {
            lower_bound && matches!((a.as_u64(), b.as_u64()), (Some(x), Some(y)) if x < y)
        }
        // R-P5P9-60: ours decodes to a string with a `"` in it, and the oracle's is that
        // string with every `"` removed (it strips them). Nothing else.
        FieldRuleKind::OracleStripsQuotes => {
            matches!((a, b), (Value::String(s), Value::String(t))
                if serde_json::from_str::<String>(s)
                    .is_ok_and(|u| u.contains('"') && u.replace('"', "") == *t))
        }
        // R-P5P9-60 amendment: an f64-rounded oracle decimal against ours, the exact one.
        FieldRuleKind::OracleF64Rounded => match (a, b) {
            (Value::String(s), Value::String(t)) if s != t => {
                match (
                    decimal_canon(s),
                    decimal_canon(t),
                    s.parse::<f64>(),
                    t.parse::<f64>(),
                ) {
                    (Some(x), Some(y), Ok(p), Ok(q)) => {
                        p.is_finite() && p == q && x.1.len() >= y.1.len()
                    }
                    _ => false,
                }
            }
            _ => false,
        },
        FieldRuleKind::OracleOrphanDupes
        | FieldRuleKind::ByKeyExtraRow
        | FieldRuleKind::CountSelfConsistent => false,
    };
    ok.then_some(match r.rule {
        FieldRuleKind::OracleEmpty => ORACLE_EMPTY_PATH,
        FieldRuleKind::OracleStripsQuotes => STRIPS_QUOTES_PATH,
        FieldRuleKind::CountLowerBound => COUNT_LOWER_BOUND_PATH,
        _ => FIELD_PATH,
    })
}

/// Whether our raw response's count flag may explain a count (N1): `totalCountLowerBound` is
/// true and our `data` holds exactly one `totalCount` outside any `edges` subtree. With two
/// such counts the flag cannot say which one it is about, so the diff stays for triage.
pub fn count_flag_applies(raw: &Value) -> bool {
    fn count(v: &Value, n: &mut usize) {
        match v {
            Value::Object(m) => {
                for (k, x) in m {
                    if k == "edges" {
                        continue;
                    }
                    if k == "totalCount" {
                        *n += 1;
                    }
                    count(x, n);
                }
            }
            Value::Array(xs) => xs.iter().for_each(|x| count(x, n)),
            _ => {}
        }
    }
    let mut n = 0;
    if let Some(d) = raw.get("data") {
        count(d, &mut n);
    }
    raw.pointer("/extensions/totalCountLowerBound") == Some(&Value::Bool(true)) && n == 1
}

/// R-P5P9-60 count self-consistency, on the normalised answers `a` (ours) and `b` (the
/// oracle's): for each `count_self_consistent` rule `<root>.totalCount`, the oracle's count
/// takes ours only when neither page is cut (`pageInfo.hasNextPage` false and
/// `hasPreviousPage` not true on both), the two edge sets match, ours' count equals its own
/// edge count and the oracle's count differs from its own. Returns whether any count was
/// explained.
pub fn count_self_consistent(rules: &[FieldRule], a: &Value, b: &mut Value) -> bool {
    let whole = |c: &Value| {
        c.pointer("/pageInfo/hasNextPage") == Some(&Value::Bool(false))
            && c.pointer("/pageInfo/hasPreviousPage") != Some(&Value::Bool(true))
    };
    let edges = |c: &Value| {
        c.get("edges").and_then(Value::as_array).map(|e| {
            let mut s: Vec<String> = e.iter().map(Value::to_string).collect();
            s.sort();
            s
        })
    };
    let count = |c: &Value| c.get("totalCount").and_then(Value::as_u64);
    let mut hit = false;
    for r in rules
        .iter()
        .filter(|r| r.rule == FieldRuleKind::CountSelfConsistent)
    {
        let Some((root, "totalCount")) = r.field.split_once('.') else {
            continue;
        };
        let ptr = format!("/data/{root}");
        let (Some(ca), Some(cb)) = (a.pointer(&ptr), b.pointer(&ptr)) else {
            continue;
        };
        let (Some(ea), Some(eb), Some(na), Some(nb)) = (edges(ca), edges(cb), count(ca), count(cb))
        else {
            continue;
        };
        if whole(ca) && whole(cb) && ea == eb && na == ea.len() as u64 && nb != eb.len() as u64 {
            if let Some(n) = b.pointer_mut(&format!("{ptr}/totalCount")) {
                *n = Value::from(na);
                hit = true;
            }
        }
    }
    hit
}

/// The explainer of one oracle pair diff: per-field rules first, then F2 and F7. Returns the
/// visible hit path of the rule that explains `a` (ours) against `b` (the oracle) at `path`.
/// `lower_bound`: our raw response flagged `totalCountLowerBound` (read before
/// `normalize_gql` strips `extensions`).
pub fn explain<'r>(
    rules: &'r [FieldRule],
    lower_bound: bool,
) -> impl Fn(&str, &Value, &Value) -> Option<&'static str> + 'r {
    move |path: &str, a: &Value, b: &Value| {
        // Counts under `edges` are exact by construction: the flag never explains them.
        let in_edges = path.split('/').any(|s| s == "edges");
        let lower_bound = lower_bound && !in_edges;
        let hit = rules
            .iter()
            .filter(|r| !r.rule.is_row() && field_matches(path, &r.field))
            .find_map(|r| rule_explains(r, a, b, lower_bound));
        if hit.is_some() {
            return hit;
        }
        if let (Some(x), Some(y)) = (num_f64(a), num_f64(b)) {
            let key = path.rsplit('/').next().unwrap_or("");
            // `nonce` (Block.nonce, a numeric cmd nonce) stays exact at any size.
            let float =
                FLOAT_FIELDS.contains(&key) || (key != "nonce" && x.abs().max(y.abs()) >= BIG);
            if float && f64_eq(x, y) {
                return Some(F2_PATH);
            }
        }
        if let (Value::String(s), Value::String(t)) = (a, b) {
            if same_json_objects(s, t) {
                return Some(F7_PATH);
            }
        }
        None
    }
}

/// The height of a list row (`edges[i]`) in any of the shapes the operations select.
pub fn edge_height_key(e: &Value) -> Option<String> {
    let n = e.get("node")?;
    [
        "/height",
        "/result/height",
        "/block/height",
        "/result/block/height",
    ]
    .iter()
    .find_map(|p| n.pointer(p))
    .filter(|v| !v.is_null())
    .map(Value::to_string)
}

/// M2b/M2c at the page end: when both sides still have a next page and their last rows are
/// at the same height but differ as sets, the page size cut that height's tie set at
/// different rows. Those trailing rows are dropped from both sides (the rows before them
/// still compare). A page whose rows all share that height is never cut: it compares in full
/// as a set, so a row missing from it stays a diff. Returns whether anything was dropped.
/// `a` and `b` are root connections.
pub fn cut_trailing_ties(a: &mut Value, b: &mut Value) -> bool {
    let more = |c: &Value| c.pointer("/pageInfo/hasNextPage") == Some(&Value::Bool(true));
    if !more(a) || !more(b) {
        return false;
    }
    let tail = |c: &Value| -> Option<(String, usize, Vec<String>)> {
        let edges = c.get("edges")?.as_array()?;
        let h = edge_height_key(edges.last()?)?;
        let n = edges
            .iter()
            .rev()
            .take_while(|e| edge_height_key(e).as_deref() == Some(h.as_str()))
            .count();
        if n == edges.len() {
            return None;
        }
        let mut set: Vec<String> = edges[edges.len() - n..]
            .iter()
            .map(Value::to_string)
            .collect();
        set.sort();
        Some((h, n, set))
    };
    let (Some((ha, na, sa)), Some((hb, nb, sb))) = (tail(a), tail(b)) else {
        return false;
    };
    if ha != hb || sa == sb {
        return false;
    }
    for (c, n) in [(a, na), (b, nb)] {
        if let Some(e) = c.get_mut("edges").and_then(Value::as_array_mut) {
            e.truncate(e.len() - n);
        }
    }
    true
}

/// A non-negative decimal text in fixed point of 12 places (coin's precision); `None` past it.
fn fixed12(s: &str) -> Option<i128> {
    let s = s.trim();
    let (i, f) = s.split_once('.').unwrap_or((s, ""));
    let digits = |x: &str| x.bytes().all(|b| b.is_ascii_digit());
    if i.is_empty() || !digits(i) || !digits(f) {
        return None;
    }
    let f = f.trim_end_matches('0');
    if f.len() > 12 {
        return None;
    }
    let frac: i128 = if f.is_empty() {
        0
    } else {
        format!("{f:0<12}").parse().ok()?
    };
    i.parse::<i128>()
        .ok()?
        .checked_mul(1_000_000_000_000)?
        .checked_add(frac)
}

/// NodeBalance, R-P5P9-57 with the R-P5P9-48 tolerance kept: ours is the node's balance one
/// block earlier. The node's decimal at `node_h` minus that block's coinbase (`reward`, in
/// fixed point of 12 places), computed exactly, must be our `Float` exactly once rounded to
/// f64. The caller also checks that the coinbase at `node_h` paid the account
/// ([`coinbase_pays`]).
pub fn one_block_skew(ours: f64, node: &str, reward: i128) -> bool {
    if reward <= 0 {
        return false;
    }
    let Some(before) = fixed12(node).and_then(|n| n.checked_sub(reward)) else {
        return false;
    };
    if before < 0 {
        return false;
    }
    let unit = 1_000_000_000_000i128;
    format!("{}.{:012}", before / unit, before % unit)
        .parse::<f64>()
        .is_ok_and(|x| x == ours)
}

/// Whether a node payload with outputs (`payload/<hash>/outputs`) carries a successful
/// coinbase whose `coin.TRANSFER` pays `account`: its balance changed in that block.
pub fn coinbase_pays(payload: &Value, account: &str) -> bool {
    let Some(cb) = payload
        .get("coinbase")
        .and_then(Value::as_str)
        .and_then(|t| kdx_types::hash::b64url_decode(t).ok())
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
    else {
        return false;
    };
    cb.pointer("/result/status").and_then(Value::as_str) == Some("success")
        && cb
            .get("events")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|e| {
                e.get("name").and_then(Value::as_str) == Some("TRANSFER")
                    && e.pointer("/module/name").and_then(Value::as_str) == Some("coin")
                    && e.pointer("/params/1").and_then(Value::as_str) == Some(account)
            })
}

// ---------------------------------------------------------------------------------------------
// R-P5P9-58 row rules: the pure parts.

/// The block hash and request key of a relay `Transaction` id
/// (`base64("Transaction:[\"<block>\",\"<request key>\"]")`).
pub fn tx_id_block(id: &str) -> Option<(String, String)> {
    let url: String = id
        .trim_end_matches('=')
        .chars()
        .map(|c| match c {
            '+' => '-',
            '/' => '_',
            c => c,
        })
        .collect();
    let raw = kdx_types::hash::b64url_decode(&url).ok()?;
    let text = std::str::from_utf8(&raw)
        .ok()?
        .strip_prefix("Transaction:")?;
    let parts: Vec<String> = serde_json::from_str(text).ok()?;
    match parts.as_slice() {
        [block, rk] => Some((block.clone(), rk.clone())),
        _ => None,
    }
}

/// A transaction row of a root connection: (request key `node.hash`, block from `node.id`).
fn tx_row(e: &Value) -> Option<(String, String)> {
    let n = e.get("node")?;
    let rk = n.get("hash")?.as_str()?;
    let (block, id_rk) = tx_id_block(n.get("id")?.as_str()?)?;
    (id_rk == rk).then(|| (rk.to_string(), block))
}

/// Request keys the connection lists more than once, with each copy's block.
pub fn dupe_copies(conn: &Value) -> Vec<(String, Vec<String>)> {
    let mut by: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for e in conn
        .get("edges")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some((rk, block)) = tx_row(e) {
            by.entry(rk).or_default().push(block);
        }
    }
    by.into_iter().filter(|(_, v)| v.len() > 1).collect()
}

/// The block of `rk`'s row in the connection (ours).
pub fn conn_block_of(conn: &Value, rk: &str) -> Option<String> {
    conn.get("edges")?
        .as_array()?
        .iter()
        .filter_map(tx_row)
        .find(|(r, _)| r == rk)
        .map(|(_, b)| b)
}

/// Drops the oracle's copies `(request key, block)` from its connection and lowers its
/// `totalCount` by as many (left alone at or above `cap`: the oracle's count cap). Returns
/// the rows dropped.
pub fn drop_copies(conn: &mut Value, drop: &[(String, String)], cap: u64) -> usize {
    let Some(edges) = conn.get_mut("edges").and_then(Value::as_array_mut) else {
        return 0;
    };
    let n = edges.len();
    edges.retain(|e| tx_row(e).is_none_or(|r| !drop.contains(&r)));
    let gone = n - edges.len();
    if let Some(t) = conn.get_mut("totalCount") {
        if let Some(c) = t.as_u64().filter(|c| *c < cap) {
            *t = Value::from(c.saturating_sub(gone as u64));
        }
    }
    gone
}

/// The key of a by-key account row: account name and, on chain lists, the chain.
fn account_key(r: &Value) -> Option<(String, Option<String>)> {
    let name = r.get("accountName")?.as_str()?.to_string();
    let chain = r.get("chainId").map(|c| match c {
        Value::String(s) => s.clone(),
        v => v.to_string(),
    });
    Some((name, chain))
}

/// Indexes of our by-key rows whose (account, chain) the oracle's list lacks.
pub fn extra_rows(ours: &[Value], oracle: &[Value]) -> Vec<usize> {
    let theirs: Vec<_> = oracle.iter().filter_map(account_key).collect();
    ours.iter()
        .enumerate()
        .filter(|(_, r)| account_key(r).is_some_and(|k| !theirs.contains(&k)))
        .map(|(i, _)| i)
        .collect()
}

/// Whether a keyset guard (`keys`) holds `key`.
pub fn guard_holds(guard: &Value, key: &str) -> bool {
    guard
        .get("keys")
        .and_then(Value::as_array)
        .is_some_and(|ks| ks.iter().any(|k| k.as_str() == Some(key)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rule(field: &str, rule: FieldRuleKind) -> FieldRule {
        FieldRule::new(field, rule)
    }

    #[test]
    fn f2_floats_and_big_numbers_at_f64_precision_only() {
        let ex = explain(&[], false);
        // CG-A: difficulty above 2^53 (the oracle's JS rounding), totalBalance float sum.
        let d = ("/data/block/difficulty", json!("#1308071046363245969e0"));
        assert_eq!(ex(d.0, &d.1, &json!("#1308071046363246e3")), Some(F2_PATH));
        assert_eq!(
            ex(
                "/data/fungibleAccount/totalBalance",
                &json!("#382257669e-7"),
                &json!("#38225766899999996e-15")
            ),
            Some(F2_PATH)
        );
        // A real difference on a Float field stays a diff.
        assert_eq!(
            ex(
                "/data/fungibleAccount/totalBalance",
                &json!("#382257669e-7"),
                &json!("#382257670e-7")
            ),
            None
        );
        // Below 2^53 on a non-Float field: exact, as before (heights, gas).
        assert_eq!(
            ex("/data/block/height", &json!("#5e0"), &json!("#6e0")),
            None
        );
        // Transfer.amount trailing zeros is not F2 (rule (b), P7F).
        assert_eq!(
            ex(
                "/data/transfers/edges/7/node/amount",
                &json!("26.57382000000000000000000000"),
                &json!("26.57382")
            ),
            None
        );
        // Above 2^53 but differing beyond f64 precision: a diff.
        assert_eq!(ex(d.0, &d.1, &json!("#1308071046363300000e0")), None);
        // `nonce` stays exact above 2^53 (Block.nonce, a numeric cmd nonce; R-P5P9-58).
        let big = (
            json!("#1308071046363245969e0"),
            json!("#1308071046363246e3"),
        );
        assert_eq!(ex("/data/block/nonce", &big.0, &big.1), None);
        assert_eq!(
            ex("/data/blocksFromHeight/edges/2/node/nonce", &big.0, &big.1),
            None
        );
    }

    #[test]
    fn r58_oracle_empty_and_exact_value_matchers_explain_only_the_ruled_class() {
        use FieldRuleKind::*;
        let with = |f: &str, k, ours: Option<&str>, oracle: Option<&str>| FieldRule {
            ours: ours.map(str::to_string),
            oracle: oracle.map(str::to_string),
            ..rule(f, k)
        };
        let rules = [
            rule("signers.scheme", OracleEmpty),
            with("result.transactionId", OursNull, None, Some("0")),
            with("result.goodResult", OracleNull, Some("\"\""), None),
        ];
        let ex = explain(&rules, false);
        let hit = |p: &str, a: Value, b: Value| ex(p, &a, &b);
        // F8 present scheme: the oracle's "" against ours; never our null or a value of the
        // oracle's.
        let s = "/data/transaction/cmd/signers/0/scheme";
        assert_eq!(hit(s, json!("ED25519"), json!("")), Some(ORACLE_EMPTY_PATH));
        assert_eq!(hit(s, Value::Null, json!("")), None);
        assert_eq!(hit(s, json!("ED25519"), json!("WebAuthn")), None);
        assert_eq!(hit(s, json!(""), json!("ED25519")), None);
        // transactionId: our null against the oracle's "0" only (string or number), never
        // against a real id of a successful tx.
        let t = "/data/transaction/result/transactionId";
        assert_eq!(hit(t, Value::Null, json!("0")), Some(FIELD_PATH));
        // A number 0, raw or as `normalize_gql` writes it (`#e0`).
        assert_eq!(hit(t, Value::Null, json!(0)), Some(FIELD_PATH));
        assert_eq!(hit(t, Value::Null, json!("#e0")), Some(FIELD_PATH));
        assert_eq!(hit(t, Value::Null, json!("12345")), None);
        // goodResult: the oracle's null against our "\"\"" only.
        let g = "/data/transaction/result/goodResult";
        assert_eq!(hit(g, json!("\"\""), Value::Null), Some(FIELD_PATH));
        assert_eq!(hit(g, json!("\"Write succeeded\""), Value::Null), None);
        assert_eq!(hit(g, json!(""), Value::Null), None);
    }

    #[test]
    fn r58_count_lower_bound_only_when_flagged_and_ours_is_lower() {
        let rules = [rule("totalCount", FieldRuleKind::CountLowerBound)];
        let t = "/data/transfers/totalCount";
        let flagged = explain(&rules, true);
        assert_eq!(
            flagged(t, &json!(1663), &json!(4590)),
            Some(COUNT_LOWER_BOUND_PATH)
        );
        // Ours higher than the oracle's: a diff even when flagged.
        assert_eq!(flagged(t, &json!(4591), &json!(4590)), None);
        // Not flagged: a lower count of ours is a diff.
        assert_eq!(explain(&rules, false)(t, &json!(1663), &json!(4590)), None);
        // Only on the listed field.
        assert_eq!(flagged("/data/block/height", &json!(1), &json!(2)), None);
    }

    #[test]
    fn n1_count_flag_applies_only_to_the_sole_count_outside_edges() {
        let flag = |mut v: Value| {
            v["extensions"] = json!({"totalCountLowerBound": true});
            v
        };
        let one = json!({"data": {"transfers": {"totalCount": 1, "edges": []}}});
        assert!(count_flag_applies(&flag(one.clone())));
        assert!(!count_flag_applies(&one));
        let two = json!({"data": {"fungibleAccount": {
            "transactions": {"totalCount": 5}, "transfers": {"totalCount": 1}}}});
        assert!(!count_flag_applies(&flag(two)));
        // A nested count under `edges` does not count against the root count.
        let nested = json!({"data": {"transfers": {"totalCount": 1,
            "edges": [{"node": {"result": {"transfers": {"totalCount": 2}}}}]}}});
        assert!(count_flag_applies(&flag(nested)));
        // The path under `edges` is never explained, even when the flag applies.
        let rules = [rule("totalCount", FieldRuleKind::CountLowerBound)];
        let e = "/data/transfers/edges/0/node/result/transfers/totalCount";
        assert_eq!(explain(&rules, true)(e, &json!(1), &json!(2)), None);
    }

    #[test]
    fn f7_json_objects_compare_as_objects_but_numbers_keep_their_text() {
        let ex = explain(&[], false);
        let p = "/data/transaction/cmd/payload/data";
        let a = json!(r#"{"keyset":{"pred":"keys-all","keys":["461f"]}}"#);
        let b = json!(r#"{"keyset":{"keys":["461f"],"pred":"keys-all"}}"#);
        assert_eq!(ex(p, &a, &b), Some(F7_PATH));
        // goodResult: an array of objects in another key order.
        let g = "/data/transaction/result/goodResult";
        let a = json!(r#"[{"amount":3.05449975,"token":"runonflux.flux"}]"#);
        let b = json!(r#"[{"token":"runonflux.flux","amount":3.05449975}]"#);
        assert_eq!(ex(g, &a, &b), Some(F7_PATH));
        // A value change inside the object stays a diff.
        let c = json!(r#"[{"token":"runonflux.flux","amount":3.05449976}]"#);
        assert_eq!(ex(g, &a, &c), None);
        // F9: same value in another number form inside a JSON array is not F7.
        let e = "/data/events/edges/0/node/parameters";
        let a = json!(r#"["k:a","k:b",1.1e-5]"#);
        let b = json!(r#"["k:a","k:b",0.000011]"#);
        assert_eq!(ex(e, &a, &b), None);
        // Nor inside an object.
        let a = json!(r#"{"amount":1.1e-5}"#);
        let b = json!(r#"{"amount":0.000011}"#);
        assert_eq!(ex(p, &a, &b), None);
        // F6: a JSON-quoted string is not an object.
        let code = "/data/transaction/cmd/payload/code";
        assert_eq!(ex(code, &json!("(coin.x)"), &json!("\"(coin.x)\"")), None);
    }

    #[test]
    fn field_paths_skip_indexes_and_connection_wrappers() {
        assert!(field_matches("/data/transaction/cmd/nonce", "cmd.nonce"));
        assert!(field_matches(
            "/data/transfers/edges/3/node/amount",
            "transfers.amount"
        ));
        assert!(field_matches(
            "/data/transaction/result/transfers/edges/1/node/amount",
            "transfers.amount"
        ));
        assert!(field_matches(
            "/data/transaction/cmd/signers/0/scheme",
            "signers.scheme"
        ));
        assert!(!field_matches("/data/block/nonce", "cmd.nonce"));
        // `node` as a real field (the `node(id)` root) is kept.
        assert!(field_matches("/data/node/hash", "node.hash"));
    }

    #[test]
    fn per_field_rules_are_narrow() {
        use FieldRuleKind::*;
        let rules = [
            rule("cmd.nonce", JsonUnquote),
            rule("payload.code", JsonUnquote),
            rule("signers.scheme", NullOrEmpty),
            rule("events.parameters", JsonNumbersByValue),
            rule("transfers.amount", DecimalValue),
            rule("result.transactionId", DecimalValue),
            rule("payload.pactId", OracleNull),
            rule("result.crossChainTransfer", OursNull),
        ];
        let ex = explain(&rules, false);
        let hit = |p: &str, a: Value, b: Value| ex(p, &a, &b);
        // F5: ours double-encoded.
        let n = "/data/transaction/cmd/nonce";
        assert_eq!(
            hit(
                n,
                json!("\"2025-07-15T01:39:41.630Z\""),
                json!("2025-07-15T01:39:41.630Z")
            ),
            Some(FIELD_PATH)
        );
        assert_eq!(hit(n, json!("\"2025-07-15\""), json!("2025-07-16")), None);
        // Only ours = JSON-encoded(oracle) (R-P5P9-58): the oracle's JSON-quoted is a diff.
        let c = "/data/transaction/cmd/payload/code";
        assert_eq!(hit(c, json!("(coin.x)"), json!("\"(coin.x)\"")), None);
        assert_eq!(hit(n, json!("2025-07-15"), json!("\"2025-07-15\"")), None);
        // F8.
        let s = "/data/transaction/cmd/signers/0/scheme";
        assert_eq!(hit(s, Value::Null, json!("")), Some(FIELD_PATH));
        assert_eq!(hit(s, Value::Null, json!("ED25519")), None);
        // F9: numbers by value inside the parameters text, nothing else.
        let e = "/data/events/edges/0/node/parameters";
        assert_eq!(
            hit(e, json!(r#"["k:a",1.1e-5]"#), json!(r#"["k:a",0.000011]"#)),
            Some(FIELD_PATH)
        );
        assert_eq!(
            hit(e, json!(r#"["k:a",1.1e-5]"#), json!(r#"["k:b",0.000011]"#)),
            None
        );
        // Transfer.amount trailing zeros; F10 number against string.
        let a = "/data/transfers/edges/7/node/amount";
        assert_eq!(
            hit(a, json!("26.57382000000000000000000000"), json!("26.57382")),
            Some(FIELD_PATH)
        );
        assert_eq!(hit(a, json!("26.57382"), json!("26.57383")), None);
        let t = "/data/transaction/result/transactionId";
        assert_eq!(hit(t, json!("#5e0"), json!("5")), Some(FIELD_PATH));
        assert_eq!(hit(t, Value::Null, json!("0")), None);
        // Oracle null only where listed, and never our null.
        let p = "/data/transaction/cmd/payload/pactId";
        assert_eq!(hit(p, json!("Ufcf"), Value::Null), Some(FIELD_PATH));
        assert_eq!(hit(p, Value::Null, json!("Ufcf")), None);
        let x = "/data/transfers/edges/5/node/crossChainTransfer";
        assert_eq!(
            hit(x, Value::Null, json!({"receiverAccount": "k:d6"})),
            None
        );
        let x = "/data/transaction/result/crossChainTransfer";
        assert_eq!(
            hit(x, Value::Null, json!({"receiverAccount": "k:d6"})),
            Some(FIELD_PATH)
        );
        assert_eq!(
            hit(x, json!({"receiverAccount": "k:d6"}), Value::Null),
            None
        );
        assert_eq!(hit("/data/transaction/hash", json!("x"), Value::Null), None);
        // A rule never applies off its field.
        assert_eq!(hit("/data/block/nonce", json!("\"1\""), json!("1")), None);
    }

    #[test]
    fn trailing_ties_cut_only_when_both_pages_continue_and_the_sets_differ() {
        let row = |h: u64, c: u64| json!({"node": {"height": h, "chainId": c}});
        let conn = |rows: Vec<Value>, more: bool| json!({"edges": rows, "pageInfo": {"hasNextPage": more}});
        let mut a = conn(vec![row(9, 0), row(8, 0), row(8, 1)], true);
        let mut b = conn(vec![row(9, 0), row(8, 2), row(8, 3)], true);
        assert!(cut_trailing_ties(&mut a, &mut b));
        assert_eq!(a["edges"].as_array().unwrap().len(), 1);
        assert_eq!(b["edges"].as_array().unwrap().len(), 1);
        // The same set in another order: nothing to cut (the tie sort handles it).
        let mut a = conn(vec![row(9, 0), row(8, 0), row(8, 1)], true);
        let mut b = conn(vec![row(9, 0), row(8, 1), row(8, 0)], true);
        assert!(!cut_trailing_ties(&mut a, &mut b));
        // A complete list (no next page) is compared in full.
        let mut a = conn(vec![row(9, 0), row(8, 0)], false);
        let mut b = conn(vec![row(9, 0), row(8, 2)], true);
        assert!(!cut_trailing_ties(&mut a, &mut b));
        // Different last heights: a real order or membership difference.
        let mut a = conn(vec![row(9, 0), row(8, 0)], true);
        let mut b = conn(vec![row(9, 0), row(7, 0)], true);
        assert!(!cut_trailing_ties(&mut a, &mut b));
        // A page whose rows all share one height is never cut (R-P5P9-58): on either side.
        let mut a = conn(vec![row(8, 0), row(8, 1), row(8, 2)], true);
        let mut b = conn(vec![row(8, 0), row(8, 2), row(8, 3)], true);
        assert!(!cut_trailing_ties(&mut a, &mut b));
        assert_eq!(a["edges"].as_array().unwrap().len(), 3);
        let mut a = conn(vec![row(8, 0), row(8, 1)], true);
        let mut b = conn(vec![row(9, 0), row(8, 0), row(8, 2)], true);
        assert!(!cut_trailing_ties(&mut a, &mut b));
    }

    #[test]
    fn one_block_skew_is_one_coinbase_at_f64_precision() {
        let r = 904_046_000_000; // 0.904046 KDA, CG-A's chain 2 record
        assert!(one_block_skew(173.644202255336, "174.548248255336", r));
        // A large balance whose Float is rounded in the last place.
        assert!(one_block_skew(77474.7172621827, "77475.621308182711", r));
        // Two rewards, the node lower, or no reward: not a one-block skew.
        assert!(!one_block_skew(208.902467335336, "210.710559335336", r));
        assert!(!one_block_skew(174.548248255336, "173.644202255336", r));
        assert!(!one_block_skew(173.644202255336, "174.548248255337", 0));
        assert!(!one_block_skew(173.644202255336, "174.548249255336", r));
        // Exact: one ulp of our Float off the node's decimal minus the reward is not a skew.
        assert!(!one_block_skew(
            f64::from_bits(173.644202255336f64.to_bits() + 1),
            "174.548248255336",
            r
        ));
        assert!(one_block_skew(0.904046, "1.808092", r));
        assert!(!one_block_skew(0.0, "0.5", r));
    }

    #[test]
    fn coinbase_pays_only_its_miner_on_success() {
        let cb = |status: &str, to: &str| {
            let v = json!({"result": {"status": status}, "events": [{"name": "TRANSFER",
                "module": {"name": "coin", "namespace": null}, "params": ["", to, 0.904046]}]});
            json!({"coinbase": kdx_types::hash::b64url_encode(v.to_string().as_bytes())})
        };
        assert!(coinbase_pays(&cb("success", "k:miner"), "k:miner"));
        assert!(!coinbase_pays(&cb("success", "k:other"), "k:miner"));
        assert!(!coinbase_pays(&cb("failure", "k:miner"), "k:miner"));
        assert!(!coinbase_pays(&json!({}), "k:miner"));
    }

    fn tx_id(block: &str, rk: &str) -> String {
        // Standard base64 with padding, as the relay ids are written.
        let b = format!("Transaction:[\"{block}\",\"{rk}\"]");
        let url = kdx_types::hash::b64url_encode(b.as_bytes());
        let mut out: String = url
            .chars()
            .map(|c| match c {
                '-' => '+',
                '_' => '/',
                c => c,
            })
            .collect();
        while !out.len().is_multiple_of(4) {
            out.push('=');
        }
        out
    }

    #[test]
    fn r58_orphan_dupes_drop_only_the_named_copies_and_lower_the_count() {
        assert_eq!(
            tx_id_block(&tx_id("XJfQNVV9", "aYo73A6i")),
            Some(("XJfQNVV9".into(), "aYo73A6i".into()))
        );
        assert_eq!(tx_id_block("bm90IGFuIGlk"), None);
        let e = |b: &str, rk: &str| json!({"node": {"id": tx_id(b, rk), "hash": rk}});
        let mut oracle = json!({"totalCount": 4571, "edges": [
            e("XJfQNVV9", "aYo73A6i"), e("Ye2tcl_c", "aYo73A6i"), e("B1", "other")]});
        let ours =
            json!({"totalCount": 4570, "edges": [e("XJfQNVV9", "aYo73A6i"), e("B1", "other")]});
        assert_eq!(
            dupe_copies(&oracle),
            vec![(
                "aYo73A6i".to_string(),
                vec!["XJfQNVV9".to_string(), "Ye2tcl_c".to_string()]
            )]
        );
        assert!(dupe_copies(&ours).is_empty());
        assert_eq!(
            conn_block_of(&ours, "aYo73A6i").as_deref(),
            Some("XJfQNVV9")
        );
        let drop = [("aYo73A6i".to_string(), "Ye2tcl_c".to_string())];
        assert_eq!(drop_copies(&mut oracle, &drop, 10_000), 1);
        assert_eq!(oracle, ours);
        // At the oracle's count cap the count is left alone.
        let mut capped = json!({"totalCount": 10000, "edges": [e("Ye2tcl_c", "aYo73A6i")]});
        assert_eq!(drop_copies(&mut capped, &drop, 10_000), 1);
        assert_eq!(capped["totalCount"], 10000);
    }

    #[test]
    fn r58_by_key_extra_rows_and_guards() {
        let acct = |n: &str| json!({"accountName": n, "fungibleName": "coin"});
        let ours = [acct("e7f7130f"), acct("k:251efb06"), acct("251efb06")];
        let theirs = [acct("e7f7130f"), acct("251efb06")];
        assert_eq!(extra_rows(&ours, &theirs), vec![1]);
        // Chain lists key on (account, chain).
        let ch = |n: &str, c: &str| json!({"accountName": n, "chainId": c});
        assert_eq!(
            extra_rows(&[ch("a", "1"), ch("a", "7")], &[ch("a", "1")]),
            vec![1]
        );
        let g = json!({"keys": ["e764b2fd", "e7f7130f"], "pred": "keys-any"});
        assert!(guard_holds(&g, "e7f7130f"));
        assert!(!guard_holds(&g, "251efb06"));
        assert!(!guard_holds(&json!({"fun": "x"}), "e7f7130f"));
    }

    /// R-P5P9-60 `cmd.nonce` inner quotes: ours decodes to a string with `"` in it and the
    /// oracle's is it with every `"` removed (CG-2 QTransactionByKey, node = ours 3/3).
    #[test]
    fn r60_oracle_strips_quotes_explains_only_the_stripped_nonce() {
        let r = [rule("cmd.nonce", FieldRuleKind::OracleStripsQuotes)];
        let e = explain(&r, false);
        let p = "/data/transaction/cmd/nonce";
        let s = |x: &str| Value::String(x.into());
        // The ruled class, both CG-2 shapes.
        let ours = s(r#""\"2024-07-30T00:14:10.646Z\"""#);
        assert_eq!(
            e(p, &ours, &s("2024-07-30T00:14:10.646Z")),
            Some(STRIPS_QUOTES_PATH)
        );
        let ours2 = s(r#""XEDS-\"2022-12-20T13:23:43.670Z\"""#);
        assert_eq!(
            e(p, &ours2, &s("XEDS-2022-12-20T13:23:43.670Z")),
            Some(STRIPS_QUOTES_PATH)
        );
        // Neighbours stay diffs: another value once stripped, quotes kept or partly
        // stripped, ours with no inner quote (that is F5's json_unquote), ours not JSON
        // encoded, a non-string side, and another field.
        assert_eq!(e(p, &ours, &s("2024-07-30T00:14:10.647Z")), None);
        assert_eq!(e(p, &ours, &s(r#""2024-07-30T00:14:10.646Z""#)), None);
        assert_eq!(e(p, &ours2, &s(r#"XEDS-"2022-12-20T13:23:43.670Z"#)), None);
        assert_eq!(e(p, &s(r#""abc""#), &s("abc")), None);
        assert_eq!(e(p, &s(r#"a"b"#), &s("ab")), None);
        assert_eq!(e(p, &ours, &Value::Null), None);
        assert_eq!(
            e(
                "/data/transaction/hash",
                &ours,
                &s("2024-07-30T00:14:10.646Z")
            ),
            None
        );
        // Never the other way round.
        assert_eq!(e(p, &s("2024-07-30T00:14:10.646Z"), &ours), None);
    }

    /// R-P5P9-60 decimal text in `result.continuation` and `result.goodResult` (CG-2
    /// GetRelatedTransaction and QTransactionByKey): numbers by value, objects as objects;
    /// `goodResult` keeps its exact `oracle_null` rule too.
    #[test]
    fn r60_continuation_and_good_result_numbers_by_value() {
        let mut good = FieldRule::new("result.goodResult", FieldRuleKind::OracleNull);
        good.ours = Some("\"\"".into());
        let r = [
            rule("result.continuation", FieldRuleKind::JsonNumbersByValue),
            good,
            rule("result.goodResult", FieldRuleKind::JsonNumbersByValue),
        ];
        let e = explain(&r, false);
        let s = |x: &str| Value::String(x.into());
        let cont = "/data/transaction/result/continuation";
        let ours = s(
            r#"{"executed":null,"pactId":"7wSC","stepHasRollback":false,"step":0,"yield":{"data":{"amount":5.0e-2,"receiver":"k:08df"}},"continuation":{"args":["k:08df","2",5.0e-2],"def":"coin.transfer-crosschain"},"stepCount":2}"#,
        );
        let oracle = s(
            r#"{"step":0,"yield":{"data":{"receiver":"k:08df","amount":0.05}},"pactId":"7wSC","executed":null,"stepCount":2,"continuation":{"def":"coin.transfer-crosschain","args":["k:08df","2",0.05]},"stepHasRollback":false}"#,
        );
        assert_eq!(e(cont, &ours, &oracle), Some(FIELD_PATH));
        assert_eq!(
            e(cont, &s(r#"{"a":1.0e-3}"#), &s(r#"{"a":0.001}"#)),
            Some(FIELD_PATH)
        );
        let gr = "/data/transaction/result/goodResult";
        assert_eq!(
            e(gr, &s(r#"{"amount":5.0e-2}"#), &s(r#"{"amount":0.05}"#)),
            Some(FIELD_PATH)
        );
        // Both goodResult rules apply: the exact oracle_null one still explains its class.
        assert_eq!(e(gr, &s("\"\""), &Value::Null), Some(FIELD_PATH));
        // Neighbours stay diffs: another amount, a key missing, a number against its text,
        // a step of another value, and another field.
        assert_eq!(e(cont, &s(r#"{"a":5.0e-2}"#), &s(r#"{"a":0.5}"#)), None);
        assert_eq!(e(cont, &s(r#"{"a":1,"b":2}"#), &s(r#"{"a":1}"#)), None);
        assert_eq!(e(cont, &s(r#"{"a":1}"#), &s(r#"{"a":"1"}"#)), None);
        assert_eq!(e(cont, &s(r#"{"step":0}"#), &s(r#"{"step":1}"#)), None);
        assert_eq!(e(gr, &s(r#"{"a":1}"#), &Value::Null), None);
        assert_eq!(
            e(
                "/data/transaction/cmd/payload/data",
                &s(r#"{"a":1.0}"#),
                &s(r#"{"a":1}"#)
            ),
            None
        );
    }

    /// R-P5P9-60 Transactions-9e58b95d: the oracle's count contradicts its own complete list.
    #[test]
    fn r60_count_self_consistent_only_when_the_oracle_contradicts_its_own_list() {
        let r = [rule(
            "transactions.totalCount",
            FieldRuleKind::CountSelfConsistent,
        )];
        let conn = |n: u64, ids: &[&str], next: bool| {
            let edges: Vec<Value> = ids.iter().map(|i| json!({"node": {"hash": i}})).collect();
            json!({"data": {"transactions": {"totalCount": n, "edges": edges,
                "pageInfo": {"hasNextPage": next, "hasPreviousPage": false}}}})
        };
        let run = |a: Value, mut b: Value| {
            let hit = count_self_consistent(&r, &a, &mut b);
            (hit, b["data"]["transactions"]["totalCount"].clone())
        };
        let ids = ["g", "p", "e", "P"];
        // The CG-2 classes: ours 4 of 4 against the oracle's 1 over the same 4 rows, and
        // ours 1 of 1 against its 3 over the same row.
        assert_eq!(
            run(conn(4, &ids, false), conn(1, &ids, false)),
            (true, json!(4))
        );
        assert_eq!(
            run(conn(1, &["l"], false), conn(3, &["l"], false)),
            (true, json!(1))
        );
        // Neighbours stay: a cut page on either side, other rows, ours' own count off, the
        // oracle's count matching its rows (then ours is the odd one), and another root.
        assert_eq!(
            run(conn(4, &ids, true), conn(1, &ids, false)),
            (false, json!(1))
        );
        assert_eq!(
            run(conn(4, &ids, false), conn(1, &ids, true)),
            (false, json!(1))
        );
        assert_eq!(
            run(conn(4, &ids, false), conn(1, &["g", "p", "e", "x"], false)),
            (false, json!(1))
        );
        assert_eq!(
            run(conn(5, &ids, false), conn(1, &ids, false)),
            (false, json!(1))
        );
        assert_eq!(
            run(conn(4, &ids[..3], false), conn(4, &ids[..3], false)),
            (false, json!(4))
        );
        let mut prev = conn(1, &ids, false);
        prev["data"]["transactions"]["pageInfo"]["hasPreviousPage"] = json!(true);
        assert_eq!(run(conn(4, &ids, false), prev), (false, json!(1)));
        let mut no_info = conn(1, &ids, false);
        no_info["data"]["transactions"]
            .as_object_mut()
            .unwrap()
            .remove("pageInfo");
        assert_eq!(run(conn(4, &ids, false), no_info), (false, json!(1)));
        let other = [rule(
            "transfers.totalCount",
            FieldRuleKind::CountSelfConsistent,
        )];
        let mut b = conn(1, &ids, false);
        assert!(!count_self_consistent(
            &other,
            &conn(4, &ids, false),
            &mut b
        ));
    }

    /// R-P5P9-60 amendment 1: `signers.clist.args` numbers by value (node = ours 2/2:
    /// `1e-06`, `1.0`; the oracle `0.000001`, `1`).
    #[test]
    fn r60_clist_args_numbers_by_value() {
        let r = [rule(
            "signers.clist.args",
            FieldRuleKind::JsonNumbersByValue,
        )];
        let e = explain(&r, false);
        let p = "/data/transaction/cmd/signers/0/clist/0/args";
        let s = |x: &str| Value::String(x.into());
        assert_eq!(
            e(
                p,
                &s(r#"["k:e4f0","k:b51e",1e-06]"#),
                &s(r#"["k:e4f0","k:b51e",0.000001]"#)
            ),
            Some(FIELD_PATH)
        );
        assert_eq!(
            e(
                p,
                &s(r#"["cyberfly-account-gas",{"int":1},1.0]"#),
                &s(r#"["cyberfly-account-gas",{"int":1},1]"#)
            ),
            Some(FIELD_PATH)
        );
        // Neighbours stay: another amount, a number against its text, another account, an
        // extra argument, and another field.
        assert_eq!(e(p, &s(r#"["k:a",1e-06]"#), &s(r#"["k:a",0.00001]"#)), None);
        assert_eq!(e(p, &s(r#"["k:a",1.0]"#), &s(r#"["k:a","1"]"#)), None);
        assert_eq!(e(p, &s(r#"["k:a",1.0]"#), &s(r#"["k:b",1]"#)), None);
        assert_eq!(e(p, &s(r#"["k:a",1.0]"#), &s(r#"["k:a",1,2]"#)), None);
        let other = "/data/transaction/cmd/signers/0/clist/0/name";
        assert_eq!(e(other, &s("[1.0]"), &s("[1]")), None);
    }

    /// R-P5P9-60 amendment 2: `transfers.amount`, the oracle's f64-rounded string against
    /// ours, the exact Pact decimal.
    #[test]
    fn r60_transfer_amount_f64_rounded_only() {
        let r = [rule("transfers.amount", FieldRuleKind::OracleF64Rounded)];
        let e = explain(&r, false);
        let p = "/data/transfers/edges/0/node/amount";
        let s = |x: &str| Value::String(x.into());
        // The ruled class: same f64, ours the longer exact decimal.
        assert_eq!(
            e(p, &s("123456789.123456789012"), &s("123456789.12345679")),
            Some(FIELD_PATH)
        );
        assert_eq!(
            e(p, &s("540796.439262685228"), &s("540796.4392626852")),
            Some(FIELD_PATH)
        );
        // A real 1-unit-in-the-last-place difference on a small amount stays a diff.
        assert_eq!(e(p, &s("1.000000000001"), &s("1.000000000002")), None);
        assert_eq!(e(p, &s("0.000001"), &s("0.000002")), None);
        // A difference above f64 precision, the oracle with more digits than ours, a
        // non-numeric string, a number against a string, and another field stay diffs.
        assert_eq!(
            e(p, &s("123456789.123456789012"), &s("123456789.1234566")),
            None
        );
        assert_eq!(
            e(p, &s("123456789.12345679"), &s("123456789.123456789012")),
            None
        );
        assert_eq!(e(p, &s("1.5x"), &s("1.5")), None);
        assert_eq!(e(p, &s("NaN"), &s("NaN ")), None);
        assert_eq!(
            e(p, &s("123456789.123456789012"), &json!(123456789.12345679)),
            None
        );
        let other = "/data/transfers/edges/0/node/creationTime";
        assert_eq!(
            e(
                other,
                &s("123456789.123456789012"),
                &s("123456789.12345679")
            ),
            None
        );
    }
}
