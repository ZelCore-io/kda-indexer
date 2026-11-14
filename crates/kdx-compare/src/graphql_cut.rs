//! R-P5P9-61 H5: a top-level GraphQL list (descending by height) whose pages are cut at a
//! height boundary. The order within one height is a contract choice (R3E-8), so a page cut
//! inside the boundary height shows each side a different subset of that height's rows. The
//! harness explains such a pair only when all three of ruling (2)'s conditions hold:
//!
//! - (i) the rows at every height above the boundary match as multisets (by value, numbers by
//!   value, order ignored). Unlike the ruling's wording, `id` is kept: for rows that select no
//!   block hash, the relay `id` is the only block identity, so ignoring it could hide a fork
//!   copy above the boundary; the same canonical row has the same `id` on both servers;
//! - (ii) at the boundary height both sides hold the same number of rows, after the oracle's
//!   node-confirmed orphan copies are removed and node-confirmed oracle gaps (a canonical row
//!   of ours the oracle's complete boundary set lacks) are added back;
//! - (iii) every row of either side at the boundary height was checked on the node with a
//!   single-row `/poll` read: ours all canonical, the oracle's canonical or a confirmed orphan
//!   copy.
//!
//! The pure parts are here; the node reads are in `graphql.rs` (`gql_boundary_cut`). Anything
//! that cannot be checked (no height, no chain or request key, a failed or capped read, a row
//! twice on one side) leaves the pair as it was, so its difference stays a diff.

use serde_json::Value;

use crate::graphql_norm::{edge_height_key, tx_id_block};

/// Visible hit of H5 (allowlist id `graphql-h5-boundary-cut`); its entry carries the node
/// evidence.
pub const BOUNDARY_CUT_PATH: &str = "/~graphql-h5-boundary-cut";
/// Single-row node reads per compare at most (both sides together).
pub const NODE_CHECKS: usize = 60;

/// A list row's identity for a single-row node read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowRef {
    pub chain: Option<u8>,
    pub height: Option<u64>,
    pub block: Option<String>,
    pub rk: Option<String>,
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Whether a list row is a transaction (its `hash` is the request key).
fn is_tx_row(n: &Value) -> bool {
    n.get("cmd").is_some()
        || n.get("result").is_some()
        || n.get("id")
            .and_then(Value::as_str)
            .and_then(tx_id_block)
            .is_some()
}

/// The identity of a root list row (`edges[i]`) in the shapes the operations select.
pub fn row_ref(e: &Value) -> RowRef {
    let n = e.get("node").unwrap_or(&Value::Null);
    let first = |ps: &[&str]| ps.iter().find_map(|p| n.pointer(p)).and_then(text);
    let id_block = n.get("id").and_then(Value::as_str).and_then(tx_id_block);
    let rk = first(&["/requestKey"])
        .or_else(|| id_block.as_ref().map(|(_, rk)| rk.clone()))
        .or_else(|| is_tx_row(n).then(|| first(&["/hash"])).flatten());
    RowRef {
        chain: first(&[
            "/chainId",
            "/block/chainId",
            "/result/block/chainId",
            "/cmd/meta/chainId",
        ])
        .and_then(|c| c.parse().ok()),
        height: edge_height_key(e).and_then(|h| h.parse().ok()),
        block: first(&["/blockHash", "/block/hash", "/result/block/hash"])
            .or_else(|| id_block.map(|(b, _)| b)),
        rk,
    }
}

/// The node's verdict on one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Canon {
    /// The node puts the row's request key in the row's block (or, for a row that names no
    /// block, at the row's height with the row's content).
    Yes,
    /// The node puts the request key elsewhere, or its tx at that height lacks the row.
    No,
    /// No evidence: no read, a failed read, or a key the node does not know.
    Unknown,
}

impl Canon {
    pub fn as_str(self) -> &'static str {
        match self {
            Canon::Yes => "canonical",
            Canon::No => "not-canonical",
            Canon::Unknown => "unknown",
        }
    }
}

fn same_number(a: &Value, b: &Value) -> bool {
    let canon = |v: &Value| text(v).and_then(|t| crate::diff::decimal_canon(&t));
    match (canon(a), canon(b)) {
        (Some(x), Some(y)) => x == y,
        _ => false,
    }
}

/// The node's verdict on a row from its `/poll` entry (`None`: the node did not answer the
/// key). Returns the verdict and the node's height for the key.
pub fn canon_verdict(e: &Value, r: &RowRef, poll: Option<&Value>) -> (Canon, Option<u64>) {
    let Some(p) = poll else {
        return (Canon::Unknown, None);
    };
    let pb = p.pointer("/metaData/blockHash").and_then(Value::as_str);
    let ph = p.pointer("/metaData/blockHeight").and_then(Value::as_u64);
    if pb.is_none() && ph.is_none() {
        return (Canon::Unknown, None);
    }
    let height_ok = r.height.is_none_or(|h| ph == Some(h));
    if let Some(b) = &r.block {
        let yes = pb == Some(b.as_str()) && height_ok;
        return (if yes { Canon::Yes } else { Canon::No }, ph);
    }
    let Some(h) = r.height else {
        return (Canon::Unknown, ph);
    };
    if ph != Some(h) {
        return (Canon::No, ph);
    }
    // No block named: the row's content must be the node's at that height.
    let n = e.get("node").unwrap_or(&Value::Null);
    let events = p.get("events").and_then(Value::as_array);
    let s = |k: &str| n.get(k).and_then(Value::as_str);
    let verdict = if is_tx_row(n) {
        Canon::Yes
    } else if let (Some(from), Some(to), Some(amount)) =
        (s("senderAccount"), s("receiverAccount"), n.get("amount"))
    {
        let hit = events.into_iter().flatten().any(|ev| {
            ev.get("name").and_then(Value::as_str) == Some("TRANSFER")
                && ev.pointer("/params/0").and_then(Value::as_str) == Some(from)
                && ev.pointer("/params/1").and_then(Value::as_str) == Some(to)
                && ev
                    .pointer("/params/2")
                    .is_some_and(|x| same_number(x, amount))
        });
        if hit {
            Canon::Yes
        } else {
            Canon::No
        }
    } else {
        Canon::Unknown
    };
    (verdict, ph)
}

/// The rows of one side, as (height, edge).
pub type Rows = Vec<(u64, Value)>;

/// The boundary of two cut pages: its height, each side's rows there and whether each side's
/// page holds rows below it (its boundary set is then complete on the page).
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub height: u64,
    pub a: Vec<Value>,
    pub b: Vec<Value>,
    pub a_complete: bool,
    pub b_complete: bool,
}

/// An edge in the form two rows compare in: numbers by value, cursors dropped.
pub fn row_text(e: &Value) -> String {
    let mut v = e.clone();
    crate::graphql::normalize_gql(&mut v, u64::MAX);
    v.to_string()
}

fn multiset<'a>(rows: impl Iterator<Item = &'a Value>) -> Vec<String> {
    let mut s: Vec<String> = rows.map(row_text).collect();
    s.sort();
    s
}

/// Ruling (2)'s precondition and condition (i) on two cut pages (rows above the oracle's
/// ceiling already left out): both non-empty, each in descending height order; the boundary
/// is the higher of the two lowest heights; the rows above it match as multisets. Rows below
/// the boundary (on the side that reaches further) are beyond the other side's cut.
pub fn plan(a: &Rows, b: &Rows) -> Result<Plan, &'static str> {
    let desc = |r: &Rows| r.windows(2).all(|w| w[0].0 >= w[1].0);
    if a.is_empty() || b.is_empty() {
        return Err("empty page");
    }
    if !desc(a) || !desc(b) {
        return Err("not descending by height");
    }
    let low = |r: &Rows| r.last().map_or(0, |x| x.0);
    let h = low(a).max(low(b));
    let above = |r: &Rows| multiset(r.iter().filter(|x| x.0 > h).map(|x| &x.1));
    if above(a) != above(b) {
        return Err("rows above the boundary differ");
    }
    let at =
        |r: &Rows| -> Vec<Value> { r.iter().filter(|x| x.0 == h).map(|x| x.1.clone()).collect() };
    Ok(Plan {
        height: h,
        a: at(a),
        b: at(b),
        a_complete: low(a) < h,
        b_complete: low(b) < h,
    })
}

/// Conditions (ii) and (iii) on a [`plan`] with the node's verdicts on its boundary rows
/// (`va` for ours, `vb` for the oracle's). Returns (oracle orphan copies, oracle gaps).
pub fn explains(p: &Plan, va: &[Canon], vb: &[Canon]) -> Result<(usize, usize), &'static str> {
    let (ta, tb) = (multiset(p.a.iter()), multiset(p.b.iter()));
    let twice = |s: &[String]| s.windows(2).any(|w| w[0] == w[1]);
    if twice(&ta) || twice(&tb) {
        return Err("a row twice on one side");
    }
    if va.len() != p.a.len() || vb.len() != p.b.len() {
        return Err("rows not checked");
    }
    if va.iter().chain(vb).any(|v| *v == Canon::Unknown) {
        return Err("a row the node did not settle");
    }
    if va.contains(&Canon::No) {
        return Err("a row of ours the node calls not canonical");
    }
    let orphans = vb.iter().filter(|v| **v == Canon::No).count();
    let b_canon: Vec<String> =
        p.b.iter()
            .zip(vb)
            .filter(|(_, v)| **v == Canon::Yes)
            .map(|(e, _)| row_text(e))
            .collect();
    // Ours' whole boundary set is on its page: a canonical row the oracle lists there and
    // ours lacks is a real difference.
    if p.a_complete && b_canon.iter().any(|x| !ta.contains(x)) {
        return Err("ours lacks a canonical boundary row the oracle lists");
    }
    // The oracle's whole boundary set is on its page: a canonical row of ours it lacks is
    // a node-confirmed oracle gap.
    let gaps = if p.b_complete {
        ta.iter().filter(|x| !tb.contains(x)).count()
    } else {
        0
    };
    if p.a.len() + orphans != p.b.len() + gaps {
        return Err("boundary counts differ");
    }
    Ok((orphans, gaps))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A transfer row of QTransfersByAccount's shape (no height selected).
    fn xfer(rk: &str, block: &str, to: &str) -> Value {
        json!({"cursor": "c", "node": {"id": format!("T{rk}{block}"), "amount": 1.5,
            "senderAccount": "s", "receiverAccount": to, "chainId": 0, "blockHash": block,
            "requestKey": rk}})
    }

    fn at(h: u64, rk: &str) -> (u64, Value) {
        (
            h,
            json!({"node": {"hash": rk, "result": {"block": {"height": h}}}}),
        )
    }

    #[test]
    fn row_refs_and_node_verdicts() {
        let r = row_ref(&xfer("rk1", "B1", "m"));
        assert_eq!(
            r,
            RowRef {
                chain: Some(0),
                height: None,
                block: Some("B1".into()),
                rk: Some("rk1".into())
            }
        );
        let poll = |b: &str, h: u64| json!({"metaData": {"blockHash": b, "blockHeight": h}});
        let e = xfer("rk1", "B1", "m");
        assert_eq!(
            canon_verdict(&e, &r, Some(&poll("B1", 9))),
            (Canon::Yes, Some(9))
        );
        assert_eq!(
            canon_verdict(&e, &r, Some(&poll("B2", 9))),
            (Canon::No, Some(9))
        );
        assert_eq!(canon_verdict(&e, &r, None), (Canon::Unknown, None));
        // A row naming no block: the height and the node's TRANSFER must match.
        let e = json!({"node": {"amount": "1.50", "senderAccount": "s", "receiverAccount": "m",
            "block": {"chainId": 0, "height": 9}, "requestKey": "rk1"}});
        let r = row_ref(&e);
        assert_eq!((r.block.clone(), r.height), (None, Some(9)));
        let ev = |to: &str| {
            json!({"metaData": {"blockHash": "B1", "blockHeight": 9},
            "events": [{"name": "TRANSFER", "params": ["s", to, 1.5]}]})
        };
        assert_eq!(canon_verdict(&e, &r, Some(&ev("m"))).0, Canon::Yes);
        assert_eq!(canon_verdict(&e, &r, Some(&ev("other"))).0, Canon::No);
        assert_eq!(canon_verdict(&e, &r, Some(&poll("B1", 8))).0, Canon::No);
        // A tx row: the request key is its hash, the block its id's.
        let (_, t) = at(9, "rk7");
        let r = row_ref(&t);
        assert_eq!((r.rk.as_deref(), r.height), (Some("rk7"), Some(9)));
        assert_eq!(canon_verdict(&t, &r, Some(&poll("B", 9))).0, Canon::Yes);
        // No chain or key: nothing to read.
        assert_eq!(row_ref(&json!({"node": {"amount": 1}})), RowRef::default());
    }

    #[test]
    fn plan_needs_descending_pages_and_equal_rows_above_the_boundary() {
        let a = vec![at(10, "x"), at(10, "y"), at(9, "p"), at(9, "q")];
        let b = vec![at(10, "y"), at(10, "x"), at(9, "q"), at(9, "r")];
        let p = plan(&a, &b).unwrap();
        assert_eq!((p.height, p.a.len(), p.b.len()), (9, 2, 2));
        assert!(!p.a_complete && !p.b_complete);
        // A swap at an inner height.
        let b2 = vec![at(10, "y"), at(10, "z"), at(9, "q"), at(9, "r")];
        assert!(plan(&a, &b2).is_err());
        // Ascending, or empty.
        let up = vec![at(9, "p"), at(10, "x")];
        assert!(plan(&up, &up).is_err());
        assert!(plan(&a, &Vec::new()).is_err());
        // The oracle reaches further: its boundary set (9) is complete on its page.
        let b3 = vec![at(10, "x"), at(10, "y"), at(9, "q"), at(8, "s")];
        let p = plan(&a, &b3).unwrap();
        assert_eq!((p.height, p.a_complete, p.b_complete), (9, false, true));
    }

    #[test]
    fn explains_only_when_counts_hold_and_every_boundary_row_is_settled() {
        use Canon::*;
        let a = vec![at(10, "x"), at(9, "p"), at(9, "q")];
        let b = vec![at(10, "x"), at(9, "q"), at(9, "r")];
        let p = plan(&a, &b).unwrap();
        assert_eq!(explains(&p, &[Yes, Yes], &[Yes, Yes]), Ok((0, 0)));
        assert!(explains(&p, &[Yes, No], &[Yes, Yes]).is_err());
        assert!(explains(&p, &[Yes, Yes], &[Yes, Unknown]).is_err());
        // An oracle orphan copy at the boundary: its count drops and no longer matches.
        assert!(explains(&p, &[Yes, Yes], &[Yes, No]).is_err());
        // Unequal counts: the oracle shows one boundary row fewer.
        let b1 = vec![at(10, "x"), at(9, "q")];
        let p1 = plan(&a, &b1).unwrap();
        assert!(explains(&p1, &[Yes, Yes], &[Yes]).is_err());
        // The oracle's complete boundary set lacks ours' canonical p: a gap, added back.
        let b2 = vec![at(10, "x"), at(9, "q"), at(8, "s")];
        let p2 = plan(&a, &b2).unwrap();
        assert_eq!(explains(&p2, &[Yes, Yes], &[Yes]), Ok((0, 1)));
        // Ours' complete boundary set lacks the oracle's canonical r: a real difference.
        let a3 = vec![at(10, "x"), at(9, "p"), at(8, "s")];
        let b3 = vec![at(10, "x"), at(9, "r"), at(9, "o")];
        let p3 = plan(&a3, &b3).unwrap();
        assert!(explains(&p3, &[Yes], &[Yes, No]).is_err());
        // ...while an orphan copy taking the oracle's slot is explained.
        let b4 = vec![at(10, "x"), at(9, "p"), at(9, "o")];
        let p4 = plan(&a3, &b4).unwrap();
        assert_eq!(explains(&p4, &[Yes], &[Yes, No]), Ok((1, 0)));
        // A row twice on one side.
        let b5 = vec![at(10, "x"), at(9, "q"), at(9, "q")];
        let p5 = plan(&a, &b5).unwrap();
        assert!(explains(&p5, &[Yes, Yes], &[Yes, Yes]).is_err());
    }
}
