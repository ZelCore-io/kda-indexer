//! Offline verdict on one stored pair of answers (the endpoint suite, `scripts/suite`).
//!
//! The suite runner sends each case to ours and to the stock service itself, then pipes the
//! two answers through `kda-compare pair-diff` so that every verdict uses the same
//! normalisers and allowlist as the live harness: REST answers go through
//! [`normalize`] + [`diff_route`], GraphQL answers through [`compare_answers`] with the
//! configured `[graphql] field_rules`, and both through [`Allowlist::classify_known`].
//!
//! Input, one JSON object per line:
//! `{"id", "kind": "rest"|"graphql", "request", "max"?, "ours": {"status", "body"},
//! "stock": {"status", "body"}}`. `request` is the REST path and query, or the GraphQL
//! operation name followed by a space and the variables (the harness's `gql_request` form).
//! `body` is the parsed JSON answer, or a string when the answer was not JSON. `max`: the
//! stock side's highest indexed height (rows above it exist only on ours).
//!
//! Output, one line per input: `{"id", "class", "ids", "known_ids", "entries"}` where
//! `class` is `equal`, `allowlisted`, `known-gap`, `diff` or `status` (the HTTP statuses
//! differ; bodies are still diffed and shown), `entries` at most [`MAX_SHOWN`].

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::allowlist::Allowlist;
use crate::config::FieldRule;
use crate::diff::diff_route;
use crate::graphql::{compare_answers, Answer};
use crate::normalize::{normalize, RouteKind};
use crate::report::DiffEntry;

/// Entries kept in one output line.
pub const MAX_SHOWN: usize = 8;

#[derive(Debug, Clone, Deserialize)]
pub struct Side {
    #[serde(default)]
    pub status: u16,
    #[serde(default)]
    pub body: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PairIn {
    pub id: String,
    pub kind: String,
    #[serde(default)]
    pub request: String,
    #[serde(default)]
    pub max: Option<u64>,
    pub ours: Side,
    pub stock: Side,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Verdict {
    pub id: String,
    pub class: String,
    pub ids: Vec<String>,
    pub known_ids: Vec<String>,
    pub entries: Vec<DiffEntry>,
}

/// The verdict on one pair. `rules` and `cap`: `[graphql] field_rules` and
/// `total_count_cap` of the config.
pub fn verdict(p: PairIn, allow: &Allowlist, rules: &[FieldRule], cap: u64) -> Verdict {
    let (route, d) = if p.kind == "graphql" {
        let mut a = Answer {
            body: p.ours.body,
            status: p.ours.status,
            raw: String::new(),
            ms: Vec::new(),
        };
        let mut b = Answer {
            body: p.stock.body,
            status: p.stock.status,
            raw: String::new(),
            ms: Vec::new(),
        };
        let d = compare_answers(&mut a, &mut b, p.max.unwrap_or(u64::MAX), cap, rules);
        (RouteKind::Other, d)
    } else {
        let route = RouteKind::from_path(&p.request);
        let (mut a, mut b) = (p.ours.body, p.stock.body);
        normalize(&route, &mut a);
        normalize(&route, &mut b);
        let d = diff_route(&route, &a, &b);
        (route, d)
    };
    let c = allow.classify_known(&route, &p.request, &[], &d);
    let (mut class, shown) = if d.is_empty() {
        ("equal", Vec::new())
    } else {
        c.class(&d)
    };
    if p.ours.status != p.stock.status {
        class = "status";
    }
    Verdict {
        id: p.id,
        class: class.to_string(),
        ids: c.ids,
        known_ids: c.known_ids,
        entries: shown.into_iter().take(MAX_SHOWN).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn empty_allow() -> Allowlist {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.toml");
        std::fs::write(&p, "").unwrap();
        Allowlist::load(&p).unwrap()
    }

    fn pair(kind: &str, req: &str, a: Value, b: Value) -> PairIn {
        PairIn {
            id: "c1".into(),
            kind: kind.into(),
            request: req.into(),
            max: None,
            ours: Side {
                status: 200,
                body: a,
            },
            stock: Side {
                status: 200,
                body: b,
            },
        }
    }

    #[test]
    fn rest_equal_and_diff() {
        let allow = empty_allow();
        let v = verdict(
            pair("rest", "/txs/recent", json!([{"a": 1}]), json!([{"a": 1}])),
            &allow,
            &[],
            10_000,
        );
        assert_eq!(v.class, "equal");
        let v = verdict(
            pair("rest", "/txs/recent", json!([{"a": 1}]), json!([{"a": 2}])),
            &allow,
            &[],
            10_000,
        );
        assert_eq!(v.class, "diff");
        assert!(!v.entries.is_empty());
    }

    #[test]
    fn graphql_same_answer_is_equal() {
        let allow = empty_allow();
        let a = json!({"data": {"lastBlockHeight": 5}});
        let v = verdict(
            pair("graphql", "QLastBlockHeight {}", a.clone(), a),
            &allow,
            &[],
            10_000,
        );
        assert_eq!(v.class, "equal");
    }

    #[test]
    fn status_mismatch_wins() {
        let allow = empty_allow();
        let mut p = pair("rest", "/txs/tx?requestkey=x", json!("e"), json!("e"));
        p.stock.status = 500;
        assert_eq!(verdict(p, &allow, &[], 10_000).class, "status");
    }

    /// H2 item 1: `transaction(minimumDepth: > 100)`. FIX-4 refuses it exactly like stock
    /// (same code and message, no data), so the pair is `equal` and no allowlist entry is
    /// needed; the pre-FIX-4 answer (ours returned the transaction) stays a diff.
    #[test]
    fn minimum_depth_over_100_refused_like_stock_is_equal() {
        let allow = empty_allow();
        let refusal = json!({"data": null, "errors": [{
            "message": "[ERROR][GRAPHQL][VALID_RANGE] Minimum depth must not be higher than 100.",
            "extensions": {"code": "INTERNAL_SERVER_ERROR"}}]});
        let v = verdict(
            pair(
                "graphql",
                "QTransactionByKey {}",
                refusal.clone(),
                refusal.clone(),
            ),
            &allow,
            &[],
            10_000,
        );
        assert_eq!(v.class, "equal", "{:?}", v.entries);
        let answered = json!({"data": {"transaction": {"hash": "h"}}});
        let v = verdict(
            pair("graphql", "QTransactionByKey {}", answered, refusal),
            &allow,
            &[],
            10_000,
        );
        assert_eq!(v.class, "diff");
        assert!(
            v.ids.is_empty(),
            "no allowlist entry explains it: {:?}",
            v.ids
        );
    }
    /// H3 item 2: `pactQuery(pactQuery: true)`, `gasLimitEstimate(input: -1)` and an absent
    /// variable are variable-coercion refusals. FIX-4 answers them as stock does (HTTP 500,
    /// `INTERNAL_SERVER_ERROR`, no `data`), with its own message text: equal. The pre-FIX-4
    /// build answered HTTP 200 with the codeless variable error (code `ERROR`) and `data`:
    /// that is a `status` row, and no NOT_SUPPORTED rule applies (the call never reached the
    /// resolver).
    #[test]
    fn variable_refusals_are_equal_with_fix4_and_a_status_row_before() {
        let allow = empty_allow();
        let ours_now = json!({"errors": [{
            "message": "Expected input type \"PactQuery\", found true.",
            "extensions": {"code": "INTERNAL_SERVER_ERROR"}}]});
        let stock = json!({"errors": [{
            "message": "Variable \"$pactQuery\" got invalid value true; Expected type \"PactQuery\" to be an object.",
            "extensions": {"code": "INTERNAL_SERVER_ERROR"}}]});
        let mut p = pair("graphql", "S_pactQuery {}", ours_now, stock.clone());
        p.ours.status = 500;
        p.stock.status = 500;
        let v = verdict(p, &allow, &[], 10_000);
        assert_eq!(v.class, "equal", "{:?}", v.entries);
        let ours_before = json!({"data": {"pactQuery": null}, "errors": [{
            "message": "Expected input type", "extensions": {"code": "ERROR"}}]});
        let mut p = pair("graphql", "S_pactQuery {}", ours_before, stock);
        p.stock.status = 500;
        let v = verdict(p, &allow, &[], 10_000);
        assert_eq!(v.class, "status");
        assert!(v.ids.is_empty(), "{:?}", v.ids);
    }
}
