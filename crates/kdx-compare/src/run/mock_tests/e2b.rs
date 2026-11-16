//! FVH-E2b (R-P5P9-86 TC, X, REST composition, v1txs; R-P5P9-87 stored forms).

use super::*;
use crate::graphql::H6;
use crate::report::{DiffEntry, DiffKind};

/// A list server for the count walk: `rows` under `root`, `first` a page from the `after`
/// cursor (a row index), with `count` as totalCount.
pub(super) fn walk_reply(
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

/// A REST side answering its n-th compare request (an account or /v1/txs path) with
/// `bodies[n]` (the last one after that), and `[]` on any other path (no tip, so no lag
/// window).
pub(super) async fn reads_server(bodies: Vec<Value>) -> String {
    let n = Arc::new(Mutex::new(0usize));
    let app = Router::new().fallback(move |uri: Uri| {
        let (n, bodies) = (n.clone(), bodies.clone());
        async move {
            let p = uri.path();
            if !(p.starts_with("/v1/txs") || p.starts_with("/txs/account")) {
                return Json(json!([]));
            }
            let mut g = n.lock().unwrap();
            let body = bodies[(*g).min(bodies.len() - 1)].clone();
            *g += 1;
            Json(body)
        }
    });
    serve(app).await.trim_end_matches('/').to_string()
}

pub(super) fn rest_cfg(
    nu: &str,
    au: &str,
    bu: &str,
    out: &std::path::Path,
    b_kind: &str,
) -> CompareConfig {
    let allow =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
    toml::from_str(&format!(
        r#"
network = "mainnet01"
rate_rps = 1000.0
follow_depth = 0
pairs = [["a", "b"]]
allowlist = {allow:?}
out_dir = {out:?}
[node]
url = "{nu}"
timeout_ms = 20000
[targets.a]
url = "{au}"
kind = "kda_indexer_compat"
[targets.b]
url = "{bu}"
kind = "{b_kind}"
"#
    ))
    .unwrap()
}

const ACCT: &str = "k:5fd26d23";

pub(super) fn acct_row(rk: &str, h: u64, amount: &str) -> Value {
    json!({"requestKey": rk, "blockHash": format!("c{h}"), "chain": 9, "height": h, "idx": 0,
        "amount": amount, "fromAccount": if rk == "<coinbase>" { "" } else { "x" },
        "toAccount": ACCT, "token": "coin"})
}

/// The node of #1854's shape on chain 9: u1 in its block with an event naming the account,
/// and the coinbase of c7000104 paying the account 0.904046.
pub(super) fn rest_node() -> NodeMock {
    let mut node = canon_node(&[9], 6_999_990..7_000_200);
    node.polls.insert(
        "u1".into(),
        (
            9,
            json!({"reqKey": "u1", "metaData": {"blockHash": "c7000105"},
                "events": [{"name": "TRANSFER", "params": ["x", ACCT, 1.5]}]}),
        ),
    );
    node.headers.insert(
        (9, 7_000_104),
        vec![
            json!({"hash": "c7000104", "height": 7_000_104, "chainId": 9,
            "payloadHash": "P104"}),
        ],
    );
    let cb = json!({"reqKey": "cb", "events": [{"name": "TRANSFER", "module": {"name": "coin"},
        "params": ["", ACCT, 0.904046]}]})
    .to_string();
    node.payloads.insert(
        "P104".into(),
        json!({"coinbase": kdx_types::hash::b64url_encode(cb.as_bytes())}),
    );
    node
}

pub(super) const ACCT_REQ: &str = "/txs/account/k:5fd26d23?token=coin&chain=9&limit=5";

/// Ours (full, limit 5) and chainweb-data: a user row and a coinbase row only ours lists,
/// and a chainweb-data-only row at `edge` (ours' lowest height is 7000100).
pub(super) fn acct_pair(edge: u64) -> (Value, Value) {
    let shared = [
        acct_row("s1", 7_000_103, "2.0"),
        acct_row("s2", 7_000_101, "3.0"),
        acct_row("s3", 7_000_100, "4.0"),
    ];
    let mut a = vec![
        acct_row("u1", 7_000_105, "1.5"),
        acct_row("<coinbase>", 7_000_104, "0.904046"),
    ];
    a.extend(shared.iter().cloned());
    let mut b: Vec<Value> = shared.to_vec();
    b.push(acct_row("e1", edge, "5.0"));
    (Value::Array(a), Value::Array(b))
}

/// R-P5P9-86 (REST records where several rules apply), #1854's shape: a user row and a
/// coinbase row only ours lists (each node-confirmed), and a chainweb-data-only row at ours'
/// bottom boundary height while ours held its limit. Each part has its rule, and the record
/// is explained, listing each rule and its rows. The chainweb-data-only row above ours'
/// lowest height leaves the record unexplained.
#[tokio::test]
async fn e2b_rest_rules_combine_row_by_row() {
    // R-P5P9-95 m-6: the oracle-only row below ours' lowest height (at it, only inside the
    // lag horizon: f_m6_…).
    for (edge, explained) in [(7_000_099, true), (7_000_102, false)] {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = acct_pair(edge);
        let nu = node_server(rest_node()).await;
        let (au, bu) = (reads_server(vec![a]).await, reads_server(vec![b]).await);
        let ctx = Ctx::new(rest_cfg(&nu, &au, &bu, dir.path(), "chainweb_data"))
            .await
            .unwrap();
        let req = Request::new(ACCT_REQ.into(), Origin::Seed, None);
        let o = ctx
            .vs_target(&req, &ctx.targets["a"], &ctx.targets["b"], None)
            .await;
        if explained {
            assert_eq!(o.class, "allowlisted", "{o:?}");
            for id in [
                "cwd-missing-node",
                "cwd-missing-node-coinbase",
                "ours-limit-edge",
                "rest-compose",
            ] {
                assert!(o.allowlist_ids.contains(&id.to_string()), "{id} {o:?}");
            }
        } else {
            assert_eq!(o.class, "diff", "{o:?}");
        }
    }
}

pub(super) fn v1_row(rk: &str, h: u64) -> Value {
    json!({"requestKey": rk, "chain": 0, "height": h, "blockHash": format!("c{h}"),
        "amount": 1.5, "code": "x", "events": [], "gasLimit": 600})
}

pub(super) const V1_REQ: &str = "/v1/txs/k:5fd26d23/coin/10";

/// #1871's first reads: ours' one row at 7006546, kda-txs' four at 7006517-520.
pub(super) fn v1_first() -> (Value, Value) {
    let b: Vec<Value> = (517..=520)
        .rev()
        .map(|h| v1_row(&format!("r{h}"), 7_006_000 + h))
        .collect();
    (json!([v1_row("r546", 7_006_546)]), Value::Array(b))
}

pub(super) fn v1_node(bad: Option<&str>) -> NodeMock {
    let mut node = canon_node(&[0], 7_006_500..7_006_600);
    for h in [517u64, 518, 519, 520, 546] {
        let rk = format!("r{h}");
        let block = if bad == Some(rk.as_str()) {
            "elsewhere".to_string()
        } else {
            format!("c{}", 7_006_000 + h)
        };
        node.poll.insert(rk, (0, block));
    }
    node
}

/// R-P5P9-86 v1txs, live, #1871's shape: the pages do not overlap and kda-txs' rows end
/// lower. The re-read 60 s later agrees on the overlap: explained as oracle lag. A re-read
/// still apart, with a row kda-txs lacks on the overlap, stays unexplained.
#[tokio::test]
async fn e2b_v1txs_edge_rereads_once() {
    let (a0, b0) = v1_first();
    let all: Vec<Value> = std::iter::once(v1_row("r546", 7_006_546))
        .chain(b0.as_array().unwrap().iter().cloned())
        .collect();
    let mut gap = all.clone();
    gap.remove(2);
    for (b1, explained) in [
        (Value::Array(all.clone()), true),
        (Value::Array(gap), false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let nu = node_server(v1_node(None)).await;
        let au = reads_server(vec![a0.clone(), Value::Array(all.clone())]).await;
        let bu = reads_server(vec![b0.clone(), b1]).await;
        let ctx = Ctx::new(rest_cfg(&nu, &au, &bu, dir.path(), "kda_txs"))
            .await
            .unwrap();
        let o = v1_compare(&ctx, V1_REQ).await;
        if explained {
            assert_eq!(o.class, "allowlisted", "{o:?}");
            assert_eq!(o.allowlist_ids, vec!["kdatxs-edge-lag".to_string()]);
        } else {
            assert_eq!(o.class, "diff", "{o:?}");
        }
    }
}

pub(super) fn rest_record(
    route: &str,
    req: &str,
    a: &Value,
    b: &Value,
) -> crate::report::DiffRecord {
    crate::report::DiffRecord {
        ts: "2026-11-15T09:00:00Z".into(),
        route: route.into(),
        request: req.into(),
        a_target: "a".into(),
        b_target: "b".into(),
        a_status: Some(200),
        b_status: Some(200),
        a_body_trimmed: a.to_string(),
        b_body_trimmed: b.to_string(),
        diff: Vec::new(),
        class: "diff".into(),
        allowlisted: None,
        known_gap: None,
        curl: String::new(),
        note: None,
        body_cap: None,
        a_pages: None,
        a_tip_height: None,
        a_lowest_height: None,
    }
}

/// R-P5P9-87, the stored forms in the referee. An account record of #1854's shape is
/// explained by the rules combined row by row, and its neighbour (a chainweb-data-only row
/// above ours' lowest height) stays unexplained. A /v1/txs record of #1871's shape is
/// explained when every stored row is node-canonical and a read now agrees on the overlap;
/// one stored row the node puts in another block leaves it unexplained.
#[tokio::test]
async fn e2b_the_referee_settles_stored_rest_records() {
    // R-P5P9-95 m-6: below ours' lowest height; at it the stored form has no tip to bound
    // the row by, so it stays unexplained.
    for (edge, explained) in [(7_000_099, 1), (7_000_100, 0), (7_000_102, 0)] {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = acct_pair(edge);
        let nu = node_server(rest_node()).await;
        let cfg = rest_cfg(
            &nu,
            "http://127.0.0.1:9",
            "http://127.0.0.1:9",
            dir.path(),
            "chainweb_data",
        );
        let rec = rest_record("account", ACCT_REQ, &a, &b);
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        assert_eq!((rep.refereed, rep.explained), (1, explained), "{rep:?}");
        assert_eq!(rep.unexplained, 1 - explained, "{rep:?}");
        if explained == 1 {
            assert_eq!(rep.by_class.get("rest-compose"), Some(&1), "{rep:?}");
        }
    }
    let (a0, b0) = v1_first();
    let all: Vec<Value> = std::iter::once(v1_row("r546", 7_006_546))
        .chain(b0.as_array().unwrap().iter().cloned())
        .collect();
    for (bad, explained) in [(None, 1), (Some("r518"), 0)] {
        let dir = tempfile::tempdir().unwrap();
        let nu = node_server(v1_node(bad)).await;
        // R-P5P9-95 I-3: ours' window floor (10 rows down to 7006537) above kda-txs' rows.
        let au = paths_server(Value::Array(all.clone()), floor_rows(7_006_546, 10)).await;
        let bu = reads_server(vec![Value::Array(all.clone())]).await;
        let cfg = rest_cfg(&nu, &au, &bu, dir.path(), "kda_txs");
        let rec = rest_record("v1txs", V1_REQ, &a0, &b0);
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        assert_eq!((rep.refereed, rep.explained), (1, explained), "{rep:?}");
        if explained == 1 {
            assert_eq!(rep.by_class.get("kdatxs-edge-lag"), Some(&1), "{rep:?}");
        }
    }
}

/// R-P5P9-88 (F): Transfers-8c8a0f56's root list selects no `pageInfo`. With `first` set it
/// is a connection of that page size, and H7 settles it: ours' full page (2 of first 2) is
/// cut, the oracle's 1 row is complete, and ours' extra row is canonical on the node.
/// The stand-in `pageInfo` is taken off the bodies again. Without `first` the note stays.
#[tokio::test]
async fn e3_f_a_root_list_without_page_info_is_a_connection_of_first() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for rk in ["u", "v"] {
        let (k, v) = h7_xfer_poll(rk, "B100", 100, "s", "r", json!(1.5));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let r = |rk: &str| {
        json!({"node": {"amount": 1.5, "requestKey": rk,
            "block": {"height": 100, "chainId": "0"}}})
    };
    let page = |edges: Vec<Value>| answer(json!({"data": {"transfers": {"edges": edges}}}));
    let (mut a, mut b) = (page(vec![r("u"), r("v")]), page(vec![r("u")]));
    let vars = h7_vars(&[("first", json!(2))]);
    let (d, note) = ctx
        .gql_settle(&ctx.targets["a"], &vars, &mut a, &mut b, 7_000_000, false)
        .await;
    assert!(only_hits(&d), "{d:?} {note:?}");
    assert!(c3_unexplained(&ctx, &d).is_empty(), "{d:?}");
    assert!(
        d.iter()
            .any(|e| e.path == crate::graphql_window::WINDOW_PATH),
        "{d:?}"
    );
    assert!(a.body.pointer("/data/transfers/pageInfo").is_none());
    assert!(b.body.pointer("/data/transfers/pageInfo").is_none());
}

/// BlocksByHeight's page (as E1's #70 test, `completedBlockHeights`) of blocks at height 58 (c0 and c19), each with a nested
/// `transactions` list without a key: `(chain, totalCount, cut, gas prices)`.
fn g_page(blocks: &[(u64, u64, bool, Vec<String>)]) -> Value {
    let edges: Vec<Value> = blocks
        .iter()
        .map(|(chain, total, more, prices)| {
            let txs: Vec<Value> = prices
                .iter()
                .map(|p| json!({"node": {"cmd": {"meta": {"gasPrice": p}}}}))
                .collect();
            json!({"node": {"transactions": {"totalCount": total,
                "pageInfo": {"hasNextPage": more}, "edges": txs},
                "hash": format!("H{chain}"), "height": 58, "chainId": chain.to_string()}})
        })
        .collect();
    json!({"data": {"completedBlockHeights": {"pageInfo": {"hasNextPage": false},
        "edges": edges}}})
}

/// The node: c0's canonical block at 58 is H0, whose payload holds `n` transactions.
fn g_node(n: usize) -> NodeMock {
    let mut node = NodeMock::default();
    node.headers.insert(
        (0, 58),
        vec![json!({"hash": "H0", "height": 58, "payloadHash": "P0", "chainId": 0})],
    );
    // R-P5P9-95 m-4: each tx's signed command, its gas price ours' row's (`{i}e-8`).
    let txs: Vec<Value> = (0..n)
        .map(|i| {
            let cmd = format!("{{\"meta\": {{\"gasPrice\": {i}e-8}}}}");
            let w = json!({"hash": format!("rk{i}"), "cmd": cmd}).to_string();
            json!([kdx_types::hash::b64url_encode(w.as_bytes()), ""])
        })
        .collect();
    node.payloads
        .insert("P0".into(), json!({"transactions": txs}));
    node
}

/// R-P5P9-88 (G): #70's shape. The oracle cut c0's nested list at 20 of 126 rows and ours
/// lists all 126. The oracle's 20 are ours' first 20 in order, and the node's block holds
/// 126 txs: settled by prefix (`/~graphql-h6-prefix`), live and from the stored record. A
/// prefix out of order, and a node block of another count, stay `/~graphql-h6-unfinished`.
#[tokio::test]
async fn e3_g_a_cut_nested_list_is_settled_by_prefix() {
    let prices: Vec<String> = (0..126).map(|i| format!("{i}e-8")).collect();
    let ours = g_page(&[
        (0, 126, false, prices.clone()),
        (19, 1, false, vec!["1e-8".into()]),
    ]);
    let oracle = g_page(&[
        (0, 126, true, prices[..20].to_vec()),
        (19, 1, false, vec!["1e-8".into()]),
    ]);
    let mut swapped = prices[..20].to_vec();
    swapped.swap(0, 1);
    let oracle_bad = g_page(&[(0, 126, true, swapped), (19, 1, false, vec!["1e-8".into()])]);
    let settle = |n: usize, a: Value, b: Value| async move {
        let dir = tempfile::tempdir().unwrap();
        let ctx = gql_ctx(g_node(n), dir.path()).await;
        let (mut a, mut b) = (answer(a), answer(b));
        let (d, _) = ctx
            .gql_settle(
                &ctx.targets["a"],
                &serde_json::Map::new(),
                &mut a,
                &mut b,
                7_000_000,
                false,
            )
            .await;
        (c3_unexplained(&ctx, &d), d)
    };
    let (un, d) = settle(126, ours.clone(), oracle.clone()).await;
    assert!(un.is_empty(), "{un:?} {d:?}");
    let hit = d
        .iter()
        .find(|e| e.path == crate::graphql_scope::PREFIX_PATH)
        .expect("the prefix hit");
    assert_eq!(
        hit.a.as_ref().unwrap()["lists"][0]["node"]["txs"],
        json!(126)
    );
    // R-P5P9-97 G: the oracle's rows are not ours' prefix in order, but every one is a
    // distinct tx of the node's block and ours is the whole block: settled as multisets.
    let (un, d) = settle(126, ours.clone(), oracle_bad).await;
    assert!(un.is_empty(), "{un:?} {d:?}");
    assert!(d
        .iter()
        .any(|e| e.path == crate::graphql_scope::PREFIX_PATH));
    // #91's shape: ours the node's order reversed, the oracle another order.
    let mut rev = prices.clone();
    rev.reverse();
    let ours_rev = g_page(&[(0, 126, false, rev), (19, 1, false, vec!["1e-8".into()])]);
    let (un, d) = settle(126, ours_rev.clone(), oracle.clone()).await;
    assert!(un.is_empty(), "{un:?} {d:?}");
    // Out of order with an oracle row the node's block does not hold (or holds once only):
    // unfinished.
    let mut stray = prices[..20].to_vec();
    stray.swap(0, 1);
    stray[5] = "999e-8".into();
    let mut twice = prices[..20].to_vec();
    twice.swap(0, 1);
    twice[5] = twice[4].clone();
    for rows in [stray, twice] {
        let o = g_page(&[(0, 126, true, rows), (19, 1, false, vec!["1e-8".into()])]);
        let (un, d) = settle(126, ours_rev.clone(), o).await;
        assert!(
            un.contains(&crate::graphql_scope::UNFINISHED_PATH.to_string()),
            "{un:?}"
        );
        assert!(format!("{d:?}").contains("in any order, oracle"), "{d:?}");
    }
    // Ours' 126 rows against a node block of 125 txs.
    let (un, d) = settle(125, ours.clone(), oracle.clone()).await;
    assert!(
        un.contains(&crate::graphql_scope::UNFINISHED_PATH.to_string()),
        "{un:?}"
    );
    assert!(
        format!("{d:?}").contains("the node's block 125 txs"),
        "{d:?}"
    );

    // The stored record (R-P5P9-87), with the positional diff CG-5 logged.
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(g_node(126)).await;
    let ops = dir.path().join("ops");
    std::fs::create_dir_all(&ops).unwrap();
    std::fs::write(
        ops.join("QG.graphql"),
        "# kdx: oracle\nquery QG { completedBlockHeights(heights: [58]) { pageInfo { hasNextPage } \
         edges { node { hash height chainId transactions { totalCount pageInfo { hasNextPage } \
         edges { node { cmd { meta { gasPrice } } } } } } } } }\n",
    )
    .unwrap();
    let cfg = c3_cfg(&nu, &nu, &nu, dir.path(), &ops, "");
    let pos = DiffEntry {
        path: "/data/completedBlockHeights/edges/0/node/chainId".into(),
        kind: DiffKind::Changed,
        a: Some(json!("0")),
        b: Some(json!("19")),
    };
    let rec = e2_stored("QG", json!({}), ours, oracle, vec![pos]);
    let rep = e2_referee(cfg, dir.path(), &[rec]).await;
    assert_eq!((rep.explained, rep.unexplained), (1, 0), "{rep:?}");
    assert_eq!(rep.by_class.get("graphql-h6-prefix"), Some(&1), "{rep:?}");
}

/// R-P5P9-105 (G, #70's stored shape): `blocksFromHeight(startHeight, endHeight)` with no
/// `first` and no `pageInfo` is uncut by construction, so the G arm runs. Each block's
/// nested `transactions` selects only `cmd.meta.gasPrice` and is cut at 20 of 126 rows: ours'
/// 20 are the node's block in reverse order, the oracle's 20 its forward order. Explained by
/// prefix and multiset against the node's block. An oracle row not in the node's block stays
/// unexplained.
#[tokio::test]
async fn i_g_a_height_range_root_without_first_runs_the_g_arm() {
    let page = |prices: Vec<String>| {
        let txs: Vec<Value> = prices
            .iter()
            .map(|p| json!({"node": {"cmd": {"meta": {"gasPrice": p}}}}))
            .collect();
        json!({"data": {"blocksFromHeight": {"edges": [{"node": {"chainId": 0,
            "creationTime": "2026-11-15T09:00:00Z", "height": 58, "hash": "H0",
            "canonical": true, "transactions": {"totalCount": 126, "edges": txs}}}]}}})
    };
    let fwd: Vec<String> = (0..126).map(|i| format!("{i}e-8")).collect();
    let rev: Vec<String> = fwd.iter().rev().cloned().collect();
    let ours = page(rev[..20].to_vec());
    let mut stray = fwd[..20].to_vec();
    stray[7] = "999e-8".into();
    for (oracle, explained) in [(fwd[..20].to_vec(), 1usize), (stray, 0)] {
        let dir = tempfile::tempdir().unwrap();
        let nu = node_server(g_node(126)).await;
        let ops = dir.path().join("ops");
        std::fs::create_dir_all(&ops).unwrap();
        std::fs::write(
            ops.join("QG70.graphql"),
            "# kdx: oracle\nquery QG70($s: Int!, $e: Int) { blocksFromHeight(startHeight: $s, \
             endHeight: $e) { edges { node { chainId creationTime height hash canonical \
             transactions { totalCount edges { node { cmd { meta { gasPrice } } } } } } } } }\n",
        )
        .unwrap();
        let cfg = c3_cfg(&nu, &nu, &nu, dir.path(), &ops, "");
        let diff = (0..20)
            .map(|i| DiffEntry {
                path: format!(
                    "/data/blocksFromHeight/edges/0/node/transactions/edges/{i}/node/cmd/meta/gasPrice"
                ),
                kind: DiffKind::Changed,
                a: Some(json!(rev[i])),
                b: Some(json!(oracle[i])),
            })
            .collect();
        let rec = e2_stored(
            "QG70",
            json!({"s": 58, "e": 58}),
            ours.clone(),
            page(oracle),
            diff,
        );
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        assert_eq!(rep.explained, explained, "{explained}: {rep:?}");
        if explained == 1 {
            assert_eq!(rep.by_class.get("graphql-h6-prefix"), Some(&1), "{rep:?}");
        }
    }
}

// R-P5P9-92 (1)'s distinct-heights form is replaced by R-P5P9-93's window form:
// `e3b::e3b_tc_243_the_oracle_counts_the_windows_heights`.

/// R-P5P9-92 (9): the referee's `--max-reads` covers ours' and the oracle's reads too. #243's
/// stored record with a cap of 2 reads (one is the chain graph at the start): the walk
/// reads ours' page, then stops before the oracle's with its own reason, unexplained. With
/// the default cap the same record is explained.
#[tokio::test]
async fn e3_tc_the_referee_walk_stops_at_the_read_cap() {
    for (max_reads, explained) in [(2usize, false), (200, true)] {
        let dir = tempfile::tempdir().unwrap();
        let nu = node_server(tc_node()).await;
        let au = ours_fn_server(walk_reply("transfers", vec![tc_row("t2", Some("B100"))], 1)).await;
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
        let log = dir.path().join("diffs-cap.jsonl");
        std::fs::write(&log, serde_json::to_string(&rec.bounded()).unwrap() + "\n").unwrap();
        let opts = crate::referee::Opts {
            logs: vec![log],
            max_reads,
            rps: 10,
        };
        let rep = crate::referee::run(cfg, &opts).await.unwrap();
        assert_eq!(rep.explained == 1, explained, "{rep:?}");
        if !explained {
            assert_eq!(rep.unexplained, 1, "{rep:?}");
            let v = serde_json::to_string(&rep.still_unexplained).unwrap();
            assert!(
                v.contains("count walk stopped: the run's read cap is reached"),
                "{v}"
            );
        }
    }
}

/// R-P5P9-95 m-4: ours' rows past the oracle's prefix are checked against the node's block
/// by their `cmd`, not by count alone. Ours' row 50 (past the oracle's 20) has a gas price
/// no tx of the node's block has: unfinished.
#[tokio::test]
async fn f_m4_rows_past_the_prefix_match_the_nodes_commands() {
    let prices: Vec<String> = (0..126).map(|i| format!("{i}e-8")).collect();
    let mut bad = prices.clone();
    bad[50] = "999e-8".into();
    let ours = g_page(&[(0, 126, false, bad), (19, 1, false, vec!["1e-8".into()])]);
    let oracle = g_page(&[
        (0, 126, true, prices[..20].to_vec()),
        (19, 1, false, vec!["1e-8".into()]),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let ctx = gql_ctx(g_node(126), dir.path()).await;
    let (mut a, mut b) = (answer(ours), answer(oracle));
    let (d, _) = ctx
        .gql_settle(
            &ctx.targets["a"],
            &serde_json::Map::new(),
            &mut a,
            &mut b,
            7_000_000,
            false,
        )
        .await;
    let un = c3_unexplained(&ctx, &d);
    assert!(
        un.contains(&crate::graphql_scope::UNFINISHED_PATH.to_string()),
        "{un:?} {d:?}"
    );
}

/// A server of ours that answers `/v1/txs` with `v1` and `/txs/account` with `acct`.
pub(super) async fn paths_server(v1: Value, acct: Value) -> String {
    let app = Router::new().fallback(move |uri: Uri| {
        let (v1, acct) = (v1.clone(), acct.clone());
        async move {
            let p = uri.path();
            Json(if p.starts_with("/v1/txs") {
                v1
            } else if p.starts_with("/txs/account") {
                acct
            } else {
                json!([])
            })
        }
    });
    serve(app).await.trim_end_matches('/').to_string()
}

/// R-P5P9-95 I-2: live, "ours full" is read from ours' own page, never from the oracle's
/// cut. Ours is complete (80 rows of limit 100, no Chainweb-Next): 79 shared rows and a
/// coinbase row the node pays (so R3E-14's node-gap rule does not take the record and the
/// rows are combined row by row). Chainweb-data lists the 79 and 19 older rows ours lacks,
/// and its page is cut at its depth: a diff, though the older rows lie below ours' lowest.
#[tokio::test]
async fn f_i2_ours_full_is_read_from_ours_own_page() {
    use axum::response::IntoResponse;
    let dir = tempfile::tempdir().unwrap();
    let shared: Vec<Value> = (0..79u64)
        .map(|i| acct_row(&format!("s{i}"), 7_000_200 - i, "1.0"))
        .collect();
    let mut ours = shared.clone();
    ours.push(acct_row("<coinbase>", 7_000_104, "0.904046"));
    let mut cwd = shared;
    cwd.extend((0..19u64).map(|i| acct_row(&format!("o{i}"), 7_000_103 - i, "1.0")));
    let nu = node_server(rest_node()).await;
    let au = reads_server(vec![Value::Array(ours)]).await;
    let body = Value::Array(cwd);
    let app = Router::new().fallback(move || {
        let body = body.clone();
        async move { ([("Chainweb-Next", "next")], Json(body)).into_response() }
    });
    let bu = serve(app).await.trim_end_matches('/').to_string();
    let ctx = Ctx::new(rest_cfg(&nu, &au, &bu, dir.path(), "chainweb_data"))
        .await
        .unwrap();
    let req = Request::new(
        "/txs/account/k:5fd26d23?token=coin&chain=9&limit=100".into(),
        Origin::Seed,
        None,
    );
    let o = ctx
        .vs_target(&req, &ctx.targets["a"], &ctx.targets["b"], None)
        .await;
    assert_eq!(o.class, "diff", "{o:?}");
}

/// R-P5P9-95 I-3, live: kda-txs' re-read has caught up and still lists 517-520, which ours'
/// re-read (546 only) lacks. The re-reads agree on their overlap (546), but that is not
/// agreement: a diff.
#[tokio::test]
async fn f_i3_the_reread_must_cover_the_first_reads_kdatxs_rows() {
    let (a0, b0) = v1_first();
    let all: Vec<Value> = std::iter::once(v1_row("r546", 7_006_546))
        .chain(b0.as_array().unwrap().iter().cloned())
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(v1_node(None)).await;
    let au = reads_server(vec![a0.clone(), a0.clone()]).await;
    let bu = reads_server(vec![b0.clone(), Value::Array(all)]).await;
    let ctx = Ctx::new(rest_cfg(&nu, &au, &bu, dir.path(), "kda_txs"))
        .await
        .unwrap();
    let o = v1_compare(&ctx, V1_REQ).await;
    assert_eq!(o.class, "diff", "{o:?}");
}

/// Ours' `/txs/account` rows (coinbases included) from `top` down, `n` of them.
pub(super) fn floor_rows(top: u64, n: u64) -> Value {
    Value::Array(
        (0..n)
            .map(|i| {
                let mut r = acct_row("<coinbase>", top - i, "0.9");
                r["chain"] = json!(0);
                r
            })
            .collect(),
    )
}

/// R-P5P9-95 I-3, the stored form: ours' newest `limit` rows bounded at the record's height
/// give ours' window floor, and every kda-txs-only row must lie below it. Ours' 10 rows
/// down to 7006537 put 517-520 below: explained. Ours' 10 rows down to 7006518 put 518-520
/// inside ours' window: unexplained, though the read now agrees on its overlap. Ours with
/// fewer than `limit` rows has no floor: unexplained.
#[tokio::test]
async fn f_i3_the_stored_form_reads_ours_window_floor() {
    let (a0, b0) = v1_first();
    let all: Vec<Value> = std::iter::once(v1_row("r546", 7_006_546))
        .chain(b0.as_array().unwrap().iter().cloned())
        .collect();
    for (floor, explained) in [
        (floor_rows(7_006_546, 10), 1),
        (floor_rows(7_006_527, 10), 0),
        (floor_rows(7_006_546, 9), 0),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let nu = node_server(v1_node(None)).await;
        let au = paths_server(Value::Array(all.clone()), floor).await;
        let bu = reads_server(vec![Value::Array(all.clone())]).await;
        let cfg = rest_cfg(&nu, &au, &bu, dir.path(), "kda_txs");
        let rec = rest_record("v1txs", V1_REQ, &a0, &b0);
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        assert_eq!((rep.refereed, rep.explained), (1, explained), "{rep:?}");
    }
}

/// R-P5P9-95 m-6: a one-sided row of ours listed twice settles nothing (each copy would pass
/// the node check on its own), and an oracle-only row at ours' lowest height is ours' limit
/// only inside the lag horizon (no tip here: never).
#[tokio::test]
async fn f_m6_rest_compose_refuses_duplicates_and_the_unbounded_edge() {
    for (twice, edge) in [(true, 7_000_099), (false, 7_000_100)] {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, b) = acct_pair(edge);
        if twice {
            let u1 = a[0].clone();
            a.as_array_mut().unwrap().insert(1, u1);
        }
        let nu = node_server(rest_node()).await;
        let (au, bu) = (reads_server(vec![a]).await, reads_server(vec![b]).await);
        let ctx = Ctx::new(rest_cfg(&nu, &au, &bu, dir.path(), "chainweb_data"))
            .await
            .unwrap();
        let req = Request::new(ACCT_REQ.into(), Origin::Seed, None);
        let o = ctx
            .vs_target(&req, &ctx.targets["a"], &ctx.targets["b"], None)
            .await;
        assert_eq!(o.class, "diff", "{twice} {edge}: {o:?}");
    }
}

/// R-P5P9-105 (self-transfers, #1385's shape): a row with from = to, the same requestKey and
/// idx, is listed twice by contract (as sender and as receiver; cwd does the same). The
/// duplicate check counts it as two rows, so the stored record is explained once the node
/// confirms the row. A true duplicate (from ≠ to) still settles nothing.
#[tokio::test]
async fn i_rest_a_self_transfer_listed_twice_is_two_rows() {
    for (selfx, explained) in [(true, 1usize), (false, 0)] {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = acct_pair(7_000_099);
        let mut u1 = a[0].clone();
        if selfx {
            u1["fromAccount"] = json!(ACCT);
            u1["amount"] = json!("0.000031");
        }
        let mut rows = vec![u1.clone(), u1];
        rows.extend(a.as_array().unwrap()[2..].iter().cloned());
        let a = Value::Array(rows);
        let nu = node_server(rest_node()).await;
        let cfg = rest_cfg(
            &nu,
            "http://127.0.0.1:9",
            "http://127.0.0.1:9",
            dir.path(),
            "chainweb_data",
        );
        let rec = rest_record("account", ACCT_REQ, &a, &b);
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        assert_eq!(rep.explained, explained, "self {selfx}: {rep:?}");
        if !selfx {
            let still = serde_json::to_string(&rep.still_unexplained).unwrap();
            assert!(still.contains("listed more than once"), "{still}");
        }
    }
}

/// R-P5P9-95 m-9: the REST stored form's node polls (`node_confirms`) are reads of the run's
/// budget. With only the chain graph's read allowed, a user row only ours lists is not
/// confirmed; with the budget it is.
#[tokio::test]
async fn f_m9_rest_node_polls_count_in_max_reads() {
    let shared = [
        acct_row("s1", 7_000_103, "2.0"),
        acct_row("s2", 7_000_101, "3.0"),
        acct_row("s3", 7_000_100, "4.0"),
    ];
    let mut a = vec![acct_row("u1", 7_000_105, "1.5")];
    a.extend(shared.iter().cloned());
    let (a, b) = (Value::Array(a), Value::Array(shared.to_vec()));
    let mut reads = Vec::new();
    for (max_reads, explained) in [(200usize, 1usize), (1, 0)] {
        let dir = tempfile::tempdir().unwrap();
        let nu = node_server(rest_node()).await;
        let cfg = rest_cfg(
            &nu,
            "http://127.0.0.1:9",
            "http://127.0.0.1:9",
            dir.path(),
            "chainweb_data",
        );
        let rec = rest_record("account", ACCT_REQ, &a, &b);
        let log = dir.path().join("diffs-m9.jsonl");
        std::fs::write(&log, serde_json::to_string(&rec.bounded()).unwrap() + "\n").unwrap();
        let opts = crate::referee::Opts {
            logs: vec![log],
            max_reads,
            rps: 100,
        };
        let rep = crate::referee::run(cfg, &opts).await.unwrap();
        assert_eq!(rep.explained, explained, "{max_reads}: {rep:?}");
        reads.push(rep.node_reads);
    }
    // The chain graph and u1's `/poll` (one chunk; before the fix only the chain graph).
    assert_eq!(reads[0], 2, "{reads:?}");
}

/// R-P5P9-97 TC: in referee mode the walk reads up to 20,000 rows a side (the oracle is
/// frozen and offline), so #832's 5,001-row side is walked, not capped; live stays at 5,000
/// (above), and the referee's cap still stops a side past 20,000.
#[tokio::test]
async fn h_tc_the_referee_walks_up_to_20000_rows_a_side() {
    let dir = tempfile::tempdir().unwrap();
    let rows = |n: usize| -> Vec<Value> {
        (0..n)
            .map(|i| tc_row(&format!("x{i}"), Some("X100")))
            .collect()
    };
    let ctx = tc_ctx(
        dir.path(),
        (vec![tc_row("t2", Some("B100"))], 1),
        (rows(5_001), 5_001),
    )
    .await;
    assert_eq!(
        ctx.gql.walk_rows.load(std::sync::atomic::Ordering::Relaxed),
        crate::graphql_window::COUNT_WALK_ROWS
    );
    crate::referee::referee_mode(&ctx, 1_000_000);
    assert_eq!(
        ctx.gql.walk_rows.load(std::sync::atomic::Ordering::Relaxed),
        20_000
    );
    let page_a = tc_page(vec![tc_row("t2", Some("B100"))], 1);
    let page_b = tc_page(vec![tc_row("t2", Some("B100"))], 5_001);
    let (_, note) = tc_live(&ctx, page_a.clone(), page_b).await;
    let note = note.unwrap_or_default();
    assert!(!note.contains("count walk capped"), "{note}");
    // Past 20,000 rows: capped.
    let dir2 = tempfile::tempdir().unwrap();
    let ctx = tc_ctx(
        dir2.path(),
        (vec![tc_row("t2", Some("B100"))], 1),
        (rows(20_001), 20_001),
    )
    .await;
    crate::referee::referee_mode(&ctx, 1_000_000);
    let page_b = tc_page(vec![tc_row("t2", Some("B100"))], 20_001);
    let (_, note) = tc_live(&ctx, page_a, page_b).await;
    let note = note.unwrap_or_default();
    assert!(note.contains("lists more than 20000 rows"), "{note}");
}
