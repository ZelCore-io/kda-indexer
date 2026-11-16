//! FVH-H (R-P5P9-97, R-P5P9-98): the CG-5 re-referee's harness leftovers and the owner's
//! exact-decimal rule. Each case is the record's shape; each is RED without its fix.

use super::*;
use crate::graphql_window::{ROW_PATH, WINDOW_PATH};

fn txs(more: bool, rows: &[(&str, u64)]) -> crate::graphql::Answer {
    h7_page(
        "transactions",
        more,
        rows.iter()
            .map(|(rk, h)| h7_tx(rk, *h, &format!("B{h}"), "2024-01-01T10:00:00Z"))
            .collect(),
    )
}

/// R-P5P9-97 W (#404's records): ours' newest-first cut list lies wholly above the frozen
/// oracle's ceiling, and an oracle-only row the node calls canonical sits below ours' lowest
/// row. It is below our window, not "without a height to place". With ours complete the
/// row stays a diff.
#[tokio::test]
async fn h_w_rows_below_ours_when_ours_is_wholly_above_the_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock {
        tip: 7_000_000,
        ..NodeMock::default()
    };
    for (rk, h) in [("r200", 200u64), ("r199", 199), ("r100", 100)] {
        let (k, v) = h7_poll(rk, &format!("B{h}"), h, json!([]));
        node.polls.insert(k, v);
    }
    h7_back_cmds(&mut node, "2024-01-01T10:00:00Z", &[]);
    let ctx = gql_ctx(node, dir.path()).await;
    let ta = &ctx.targets["a"];
    let vars = serde_json::Map::new();
    let ours = || txs(true, &[("r200", 200), ("r199", 199)]);
    let (d, note) = ctx
        .gql_settle(
            ta,
            &vars,
            &mut ours(),
            &mut txs(true, &[("r100", 100)]),
            150,
            false,
        )
        .await;
    let text = format!("{d:?} {note:?}");
    assert!(!text.contains("without a height to place"), "{text}");
    assert!(d.iter().any(|e| e.path == WINDOW_PATH), "{text}");
    assert!(
        d.iter()
            .all(|e| e.path.starts_with("/~") && e.path != ROW_PATH),
        "{text}"
    );
    // Ours' list is complete (no next page): the canonical row is one ours lacks, a diff.
    let mut full = txs(false, &[("r200", 200), ("r199", 199)]);
    let (d, _) = ctx
        .gql_settle(
            ta,
            &vars,
            &mut full,
            &mut txs(true, &[("r100", 100)]),
            150,
            false,
        )
        .await;
    assert!(
        d.iter()
            .any(|e| !e.path.starts_with("/~") || e.path == ROW_PATH),
        "{d:?}"
    );
}

/// The node's `/poll` entries for R-P5P9-98's case: x1 moves `{"decimal":"14988.000000000000"}`,
/// x2 `{"decimal":"14989.000000000000"}` (not ours' value).
fn r98_node() -> NodeMock {
    let mut node = NodeMock::default();
    for (rk, amount) in [("x1", "14988.000000000000"), ("x2", "14989.000000000000")] {
        let (k, v) = h7_xfer_poll(rk, "B100", 100, "s", "r", json!({ "decimal": amount }));
        node.polls.insert(k, v);
    }
    node
}

/// An indexed transfer row (QTransfersByAccount's identity) or, with `index` None, a row
/// without an index (Transfers-f3be2db0's multiset identity).
fn r98_row(rk: &str, amount: &str, index: Option<u64>) -> Value {
    let mut e = h7_xrow(rk, 100, json!(amount), Some(("s", "r")));
    if let Some(i) = index {
        e["node"]["blockHash"] = json!("B100");
        e["node"]["orderIndex"] = json!(i);
    }
    e
}

/// R-P5P9-98, live: ours "14988", the oracle "14988.000000000000" and the node
/// `{"decimal":"14988.000000000000"}` are equal as exact decimals → explained, on the X path
/// (an indexed row, #744) and in H7's row identity (a row without an index, T #531). A node
/// value that differs (14989) leaves both unexplained.
#[tokio::test]
async fn h_r98_exact_decimal_equality_live() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = gql_ctx(r98_node(), dir.path()).await;
    let vars = serde_json::Map::new();
    let settle = |a: Vec<Value>, b: Vec<Value>| {
        let ctx = &ctx;
        let vars = &vars;
        async move {
            let mut a = h7_page("transfers", false, a);
            let mut b = h7_page("transfers", false, b);
            ctx.gql_settle(&ctx.targets["a"], vars, &mut a, &mut b, 7_000_000, false)
                .await
                .0
        }
    };
    // x1: the node moved 14988 (ours' value); x2: the node moved 14989, so ours and the
    // oracle agree with each other but not with the node.
    for (rk, explained) in [("x1", true), ("x2", false)] {
        for index in [Some(0), None] {
            let d = settle(
                vec![r98_row(rk, "14988", index)],
                vec![r98_row(rk, "14988.000000000000", index)],
            )
            .await;
            let un = c3_unexplained(&ctx, &d);
            assert_eq!(un.is_empty(), explained, "{rk} {index:?} {d:?}");
            if explained && index.is_some() {
                let hit = d
                    .iter()
                    .find(|e| e.path == crate::graphql_norm::DECIMAL_ZEROS_PATH)
                    .expect("the decimal hit");
                let node = &hit.a.as_ref().unwrap()["node"]["amount"];
                assert_eq!(node, &json!("14988.000000000000"));
            }
        }
    }
}

/// R-P5P9-98, stored: the same record from the diff log, refereed → explained; with the
/// node's other value → unexplained.
#[tokio::test]
async fn h_r98_exact_decimal_equality_stored() {
    use crate::report::{DiffEntry, DiffKind};
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(r98_node()).await;
    let ops = c3_ops(dir.path());
    let cfg = c3_cfg(
        &nu,
        "http://127.0.0.1:9/graphql",
        "http://127.0.0.1:9/graphql",
        dir.path(),
        &ops,
        "",
    );
    let rec = |rk: &str| {
        let page = |amount: &str| {
            json!({"data": {"transfers": {"totalCount": 1, "pageInfo": {"hasNextPage": false},
                "edges": [r98_row(rk, amount, Some(0))]}}})
        };
        e2_stored(
            "QH7",
            json!({"first": 20}),
            page("14988"),
            page("14988.000000000000"),
            vec![DiffEntry {
                path: "/data/transfers/edges/0/node/amount".into(),
                kind: DiffKind::Changed,
                a: Some(json!("14988")),
                b: Some(json!("14988.000000000000")),
            }],
        )
    };
    let rep = e2_referee(cfg.clone(), dir.path(), &[rec("x1")]).await;
    assert_eq!((rep.explained, rep.unexplained), (1, 0), "{rep:?}");
    let rep = e2_referee(cfg, dir.path(), &[rec("x2"), rec("x2")]).await;
    assert_eq!((rep.explained, rep.unexplained), (0, 2), "{rep:?}");
}

/// R-P5P9-97 W, M #2046 and X: our QTransfersByAccount document selects each row's chain
/// and height (the oracle's schema has both), so the count walk can bound ours' rows and
/// the X arm can read the node's TRANSFER.
#[test]
fn h_our_transfer_document_selects_chain_and_height() {
    let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../compare/graphql/QTransfersByAccount.graphql");
    let text = std::fs::read_to_string(p).unwrap();
    let op = crate::graphql::parse_op("QTransfersByAccount", &text, false).unwrap();
    let row = json!({"node": {"id": "x", "requestKey": "rk", "chainId": 3, "height": 9,
        "blockHash": "B", "orderIndex": 0, "amount": "1",
        "senderAccount": "s", "receiverAccount": "r"}});
    let id = crate::graphql_window::row_id(&row);
    assert_eq!((id.chain, id.height), (Some(3), Some(9)));
    for f in [" chainId ", " height "] {
        assert!(op.query.contains(f), "{f}: {}", op.query);
    }
}
