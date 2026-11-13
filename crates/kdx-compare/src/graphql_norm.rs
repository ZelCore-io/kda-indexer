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

/// Whether `rule` explains the difference `a` (ours) against `b` (the oracle).
pub fn rule_explains(rule: FieldRuleKind, a: &Value, b: &Value) -> bool {
    match rule {
        FieldRuleKind::JsonUnquote => {
            let unq = |x: &Value, y: &Value| {
                matches!((x, y), (Value::String(s), Value::String(t))
                    if serde_json::from_str::<String>(s).is_ok_and(|u| &u == t))
            };
            unq(a, b) || unq(b, a)
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
    }
}

/// The explainer of one oracle pair diff: per-field rules first, then F2 and F7. Returns the
/// visible hit path of the rule that explains `a` (ours) against `b` (the oracle) at `path`.
pub fn explain<'r>(
    rules: &'r [FieldRule],
) -> impl Fn(&str, &Value, &Value) -> Option<&'static str> + 'r {
    move |path: &str, a: &Value, b: &Value| {
        if rules
            .iter()
            .any(|r| field_matches(path, &r.field) && rule_explains(r.rule, a, b))
        {
            return Some(FIELD_PATH);
        }
        if let (Some(x), Some(y)) = (num_f64(a), num_f64(b)) {
            let key = path.rsplit('/').next().unwrap_or("");
            let float = FLOAT_FIELDS.contains(&key) || x.abs().max(y.abs()) >= BIG;
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
/// still compare). Returns whether anything was dropped. `a` and `b` are root connections.
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

/// NodeBalance, R-P5P9-57 with the R-P5P9-48 tolerance kept: ours is the node's balance one
/// block earlier, the node's read at `node_h` being higher by that block's coinbase
/// (`reward`, in fixed point of 12 places) within f64 precision of our `Float`.
pub fn one_block_skew(ours: f64, node: &str, reward: i128) -> bool {
    let (Ok(theirs), r) = (node.trim().parse::<f64>(), reward as f64 / 1e12) else {
        return false;
    };
    r > 0.0 && ((theirs - ours) - r).abs() <= F64_REL * theirs.abs().max(1.0) * 8.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rule(field: &str, rule: FieldRuleKind) -> FieldRule {
        FieldRule {
            field: field.into(),
            rule,
        }
    }

    #[test]
    fn f2_floats_and_big_numbers_at_f64_precision_only() {
        let ex = explain(&[]);
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
    }

    #[test]
    fn f7_json_objects_compare_as_objects_but_numbers_keep_their_text() {
        let ex = explain(&[]);
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
        let ex = explain(&rules);
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
        // F6: the oracle's JSON-quoted.
        let c = "/data/transaction/cmd/payload/code";
        assert_eq!(
            hit(c, json!("(coin.x)"), json!("\"(coin.x)\"")),
            Some(FIELD_PATH)
        );
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
    }
}
