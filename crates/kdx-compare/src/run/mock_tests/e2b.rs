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

fn acct_row(rk: &str, h: u64, amount: &str) -> Value {
    json!({"requestKey": rk, "blockHash": format!("c{h}"), "chain": 9, "height": h, "idx": 0,
        "amount": amount, "fromAccount": if rk == "<coinbase>" { "" } else { "x" },
        "toAccount": ACCT, "token": "coin"})
}

/// The node of #1854's shape on chain 9: u1 in its block with an event naming the account,
/// and the coinbase of c7000104 paying the account 0.904046.
fn rest_node() -> NodeMock {
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

const ACCT_REQ: &str = "/txs/account/k:5fd26d23?token=coin&chain=9&limit=5";

/// Ours (full, limit 5) and chainweb-data: a user row and a coinbase row only ours lists,
/// and a chainweb-data-only row at `edge` (ours' lowest height is 7000100).
fn acct_pair(edge: u64) -> (Value, Value) {
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
    for (edge, explained) in [(7_000_100, true), (7_000_102, false)] {
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
    }
}

/// R-P5P9-87, the stored forms in the referee. An account record of #1854's shape is
/// explained by the rules combined row by row, and its neighbour (a chainweb-data-only row
/// above ours' lowest height) stays unexplained. A /v1/txs record of #1871's shape is
/// explained when every stored row is node-canonical and a read now agrees on the overlap;
/// one stored row the node puts in another block leaves it unexplained.
#[tokio::test]
async fn e2b_the_referee_settles_stored_rest_records() {
    for (edge, explained) in [(7_000_100, 1), (7_000_102, 0)] {
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
        let au = reads_server(vec![Value::Array(all.clone())]).await;
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
    let txs: Vec<Value> = (0..n)
        .map(|i| {
            let w = json!({"hash": format!("rk{i}")}).to_string();
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
    // The oracle's rows are not ours' prefix in order.
    let (un, _) = settle(126, ours.clone(), oracle_bad).await;
    assert!(
        un.contains(&crate::graphql_scope::UNFINISHED_PATH.to_string()),
        "{un:?}"
    );
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
