//! FVH-E2b (R-P5P9-86 TC, X, REST composition, v1txs; R-P5P9-87 stored forms).

use super::*;
use crate::graphql::H6;
use crate::report::{DiffEntry, DiffKind};

/// A list server for the count walk: `rows` under `root`, `first` a page from the `after`
/// cursor (a row index), with `count` as totalCount.
fn walk_reply(
    root: &'static str,
    rows: Vec<Value>,
    count: u64,
) -> impl Fn(&Value) -> Value + Clone + Send + Sync {
    move |req: &Value| {
        let first = req["variables"]["first"].as_u64().unwrap_or(100) as usize;
        let at = req["variables"]["after"]
            .as_str()
            .and_then(|c| c.parse::<usize>().ok())
            .unwrap_or(0);
        let page: Vec<Value> = rows.iter().skip(at).take(first).cloned().collect();
        let end = at + page.len();
        json!({"data": {root: {"totalCount": count,
            "pageInfo": {"hasNextPage": end < rows.len(), "endCursor": end.to_string()},
            "edges": page}}})
    }
}

/// The node of `h7_orphans_only_by_block`: t2 canonical in B100 at 100, and B100 the only
/// block there.
fn tc_node() -> NodeMock {
    let mut node = NodeMock::default();
    let (k, v) = h7_xfer_poll("t2", "B100", 100, "s", "r", json!(1.5));
    node.polls.insert(k, v);
    node.headers.insert(
        (0, 100),
        vec![json!({"hash": "B100", "height": 100, "payloadHash": "P100", "chainId": 0})],
    );
    let cb = json!({"reqKey": "cb100", "events": []}).to_string();
    node.payloads.insert(
        "P100".into(),
        json!({"coinbase": kdx_types::hash::b64url_encode(cb.as_bytes())}),
    );
    node
}

fn tc_row(rk: &str, block: Option<&str>) -> Value {
    let mut e = h7_xrow(rk, 100, json!(1.5), Some(("s", "r")));
    if let Some(b) = block {
        e["node"]["blockHash"] = json!(b);
    }
    e
}

fn tc_page(rows: Vec<Value>, count: u64) -> Value {
    json!({"data": {"transfers": {"totalCount": count,
        "pageInfo": {"hasNextPage": false}, "edges": rows}}})
}

fn answer(body: Value) -> crate::graphql::Answer {
    crate::graphql::Answer {
        body,
        status: 200,
        raw: String::new(),
        ms: vec![0],
    }
}

/// Ours (A) and the oracle (B) answering the walk with these rows and counts.
async fn tc_ctx(dir: &std::path::Path, ours: (Vec<Value>, u64), oracle: (Vec<Value>, u64)) -> Ctx {
    let nu = node_server(tc_node()).await;
    let au = ours_fn_server(walk_reply("transfers", ours.0, ours.1)).await;
    let bu = ours_fn_server(walk_reply("transfers", oracle.0, oracle.1)).await;
    let ops = c3_ops(dir);
    Ctx::new(c3_cfg(&nu, &au, &bu, dir, &ops, ""))
        .await
        .unwrap()
}

/// The live rules on two first pages that differ by their totalCount alone.
async fn tc_live(ctx: &Ctx, a: Value, b: Value) -> (Vec<DiffEntry>, Option<String>) {
    let op = ctx.gql.ops.iter().find(|o| o.stem == "QH7").unwrap();
    let (ta, tb) = (&ctx.targets["a"], &ctx.targets["b"]);
    let vars = h7_vars(&[("accountName", json!("s")), ("first", json!(1))]);
    let (mut a, mut b) = (answer(a), answer(b));
    ctx.gql_rules(ta, H6::Live(tb), op, &vars, &mut a, &mut b, 7_000_000)
        .await
}

/// R-P5P9-86 TC: #243's shape, the oracle's count above ours and ours = the node: the walk
/// finds the oracle's one extra row, an orphan by block on the node, and the counts differ
/// by that one row: explained, the count entry gone. A walk with one unexplained one-sided
/// row (the node does not place the oracle's extra row) and a walk whose one-sided rows do
/// not account for the counts stay unexplained, the count difference kept.
#[tokio::test]
async fn e2b_tc_the_count_walk_settles_every_one_sided_row() {
    let dir = tempfile::tempdir().unwrap();
    let page_a = tc_page(vec![tc_row("t2", Some("B100"))], 1);
    let page_b = tc_page(vec![tc_row("t2", Some("B100"))], 2);
    let walk_b = vec![tc_row("t2", Some("B100")), tc_row("q", Some("X100"))];
    let ctx = tc_ctx(
        dir.path(),
        (vec![tc_row("t2", Some("B100"))], 1),
        (walk_b.clone(), 2),
    )
    .await;
    let (d, note) = tc_live(&ctx, page_a.clone(), page_b.clone()).await;
    let un = c3_unexplained(&ctx, &d);
    assert!(un.is_empty(), "{un:?} {d:?} {note:?}");
    let walk = d
        .iter()
        .find(|e| e.path == crate::graphql_window::COUNT_WALK_PATH)
        .expect("the count walk hit");
    assert_eq!(walk.a.as_ref().unwrap()["oneSided"], json!(0));
    assert_eq!(walk.b.as_ref().unwrap()["oneSided"], json!(1));
    assert!(d.iter().all(|e| e.path != "/data/transfers/totalCount"));

    // The oracle's extra row without a block: the node does not settle it.
    let dir2 = tempfile::tempdir().unwrap();
    let ctx = tc_ctx(
        dir2.path(),
        (vec![tc_row("t2", Some("B100"))], 1),
        (vec![tc_row("t2", Some("B100")), tc_row("q", None)], 2),
    )
    .await;
    let (d, note) = tc_live(&ctx, page_a.clone(), page_b.clone()).await;
    let un = c3_unexplained(&ctx, &d);
    assert!(
        un.contains(&"/data/transfers/totalCount".to_string()),
        "{d:?}"
    );
    assert!(note.unwrap_or_default().contains("count walk"));

    // The one-sided rows (one) do not account for the counts (ours 1, the oracle's 3).
    let dir3 = tempfile::tempdir().unwrap();
    let ctx = tc_ctx(
        dir3.path(),
        (vec![tc_row("t2", Some("B100"))], 1),
        (walk_b, 3),
    )
    .await;
    let page_b3 = tc_page(vec![tc_row("t2", Some("B100"))], 3);
    let (d, note) = tc_live(&ctx, page_a, page_b3).await;
    assert!(c3_unexplained(&ctx, &d).contains(&"/data/transfers/totalCount".to_string()));
    assert!(note.unwrap_or_default().contains("the counts differ by -2"));
}

/// R-P5P9-86 TC: a side past 5,000 rows is `count walk capped`, its own reason, unexplained.
#[tokio::test]
async fn e2b_tc_the_walk_is_capped_at_5000_rows_a_side() {
    let dir = tempfile::tempdir().unwrap();
    let many: Vec<Value> = (0..5_001)
        .map(|i| tc_row(&format!("x{i}"), Some("X100")))
        .collect();
    let ctx = tc_ctx(
        dir.path(),
        (vec![tc_row("t2", Some("B100"))], 1),
        (many, 5_001),
    )
    .await;
    let page_a = tc_page(vec![tc_row("t2", Some("B100"))], 1);
    let page_b = tc_page(vec![tc_row("t2", Some("B100"))], 5_001);
    let (d, note) = tc_live(&ctx, page_a, page_b).await;
    let un = c3_unexplained(&ctx, &d);
    assert!(
        un.contains(&crate::graphql_window::COUNT_WALK_CAPPED_PATH.to_string()),
        "{un:?}"
    );
    assert!(note.unwrap_or_default().contains("count walk capped"));
}

/// R-P5P9-86 TC with R-P5P9-87, the stored form: the referee walks ours bounded at the
/// record's height (its highest stored row, 100) and the frozen oracle as it is now. Ours
/// now lists a newer row at 120, which the bound leaves out: #243's stored record is
/// explained. Unbounded, that row would make the counts disagree.
#[tokio::test]
async fn e2b_tc_the_referee_walks_ours_bounded_at_the_records_height() {
    let dir = tempfile::tempdir().unwrap();
    let newer = h7_xrow("n120", 120, json!(1.5), Some(("s", "r")));
    let nu = node_server(tc_node()).await;
    let au = ours_fn_server(walk_reply(
        "transfers",
        vec![newer, tc_row("t2", Some("B100"))],
        2,
    ))
    .await;
    let walk_b = vec![tc_row("t2", Some("B100")), tc_row("q", Some("X100"))];
    let bu = ours_fn_server(walk_reply("transfers", walk_b, 2)).await;
    let ops = c3_ops(dir.path());
    let cfg = c3_cfg(&nu, &au, &bu, dir.path(), &ops, "");
    let count = DiffEntry {
        path: "/data/transfers/totalCount".into(),
        kind: DiffKind::Changed,
        a: Some(json!(1)),
        b: Some(json!(2)),
    };
    let rec = e2_stored(
        "QH7",
        json!({"accountName": "s", "first": 1}),
        tc_page(vec![tc_row("t2", Some("B100"))], 1),
        tc_page(vec![tc_row("t2", Some("B100"))], 2),
        vec![count],
    );
    let rep = e2_referee(cfg, dir.path(), &[rec]).await;
    assert_eq!((rep.explained, rep.unexplained), (1, 0), "{rep:?}");
    assert_eq!(rep.by_class.get("graphql-count-walk"), Some(&1), "{rep:?}");
}

/// R-P5P9-86 X: ours' amount equals the node's TRANSFER text and the oracle's differs only
/// by trailing zeros: excused as `/~graphql-oracle-decimal-zeros`. Ours unlike the node, and
/// a pair equal as floats but not as decimals, stay unexplained.
#[tokio::test]
async fn e2b_x_trailing_zeros_with_ours_equal_to_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for (rk, amount) in [("t1", "0.92363"), ("t2", "0.92364"), ("t3", "0.1")] {
        let n: Value = serde_json::from_str(amount).unwrap();
        let (k, v) = h7_xfer_poll(rk, "B100", 100, "s", "r", n);
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let vars = serde_json::Map::new();
    let row = |rk: &str, amount: &str| {
        // QTransfersByAccount's identity: request key, orderIndex and block.
        let mut e = h7_xrow(rk, 100, json!(amount), Some(("s", "r")));
        e["node"]["blockHash"] = json!("B100");
        e["node"]["orderIndex"] = json!(0);
        e
    };
    let settle = |rk: &'static str, ours: &'static str, oracle: &'static str| {
        let ctx = &ctx;
        let vars = &vars;
        async move {
            let mut a = h7_page("transfers", false, vec![row(rk, ours)]);
            let mut b = h7_page("transfers", false, vec![row(rk, oracle)]);
            let (d, _) = ctx
                .gql_settle(&ctx.targets["a"], vars, &mut a, &mut b, 7_000_000, false)
                .await;
            d
        }
    };
    let zeros = "0.923630000000000000000000000000";
    let d = settle("t1", "0.92363", zeros).await;
    assert!(c3_unexplained(&ctx, &d).is_empty(), "{d:?}");
    let hit = d
        .iter()
        .find(|e| e.path == crate::graphql_norm::DECIMAL_ZEROS_PATH)
        .expect("the decimal-zeros hit");
    assert_eq!(hit.a.as_ref().unwrap()["node"]["amount"], json!("0.92363"));
    // Ours' text is not the node's (0.92364).
    let d = settle("t2", "0.92363", zeros).await;
    let un = c3_unexplained(&ctx, &d);
    assert!(
        un.contains(&"/data/transfers/edges/0/node/amount".to_string()),
        "{d:?}"
    );
    // Equal as f64, not as decimals.
    let d = settle("t3", "0.1", "0.1000000000000000055511151231257827").await;
    assert!(!c3_unexplained(&ctx, &d).is_empty(), "{d:?}");
    assert!(d
        .iter()
        .all(|e| e.path != crate::graphql_norm::DECIMAL_ZEROS_PATH));
    // The text rule itself.
    use crate::graphql_norm::decimal_zeros_only;
    assert!(decimal_zeros_only("0.92363", zeros));
    assert!(decimal_zeros_only("1.0", "1"));
    assert!(!decimal_zeros_only("0.92363", "0.92363"));
    assert!(!decimal_zeros_only(
        "0.1",
        "0.1000000000000000055511151231257827"
    ));
    assert!(!decimal_zeros_only("01.5", "1.5"));
    assert!(!decimal_zeros_only("-0.5", "0.50"));
    assert!(!decimal_zeros_only("1e2", "100"));
}
