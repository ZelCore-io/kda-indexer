//! Compares against mocked node, kda-indexer (A) and chainweb-data (B) servers: the
//! node-settled classes of R-P5P9-48.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::http::Uri;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use super::Ctx;
use crate::config::CompareConfig;
use crate::gen::{Origin, Request};

/// The node: every chain's cut at `tip`, the canonical hash per (chain, height) and the block
/// `/poll` puts each request key in. Every path asked is logged.
#[derive(Clone, Default)]
pub(super) struct NodeMock {
    pub tip: u64,
    pub canon: HashMap<(u8, u64), String>,
    pub poll: HashMap<String, (u8, String)>,
    /// `/local` answers by (chain, account): the `result.data` of a `details` read. Each
    /// call logs `local <chain> <code> <account>`.
    pub local: HashMap<(u8, String), Value>,
    /// `GET /header` answers by (chain, height): every header the node holds at that height
    /// (orphans included), and `payload/<hash>/outputs` answers by payload hash.
    pub headers: HashMap<(u8, u64), Vec<Value>>,
    pub payloads: HashMap<String, Value>,
    pub calls: Arc<Mutex<Vec<String>>>,
}

/// One HTTP side: `/txs/search` (with `X-Search-Truncated: true` when `truncated`),
/// `/txs/recent` and `/coins`.
#[derive(Clone, Default)]
pub(super) struct SideMock {
    pub search: Value,
    pub truncated: bool,
    pub recent: Value,
    pub coins: String,
}

async fn serve(app: Router) -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}/")
}

async fn node_server(n: NodeMock) -> String {
    let log = |n: &NodeMock, uri: &Uri| n.calls.lock().unwrap().push(uri.to_string());
    let app = Router::new()
        .route(
            "/info",
            get(|| async {
                let g = |r: std::ops::Range<u8>| -> Vec<Value> { r.map(|c| json!([c, []])).collect() };
                Json(json!({"nodeVersion": "mainnet01", "nodePackageVersion": "2.30",
                    "nodeGraphHistory": [[852054, g(0..20)], [0, g(0..10)]]}))
            }),
        )
        .route(
            "/chainweb/0.0/mainnet01/cut",
            get(|State(n): State<NodeMock>| async move {
                let hashes: serde_json::Map<String, Value> = (0..20u8)
                    .map(|c| (c.to_string(), json!({"height": n.tip, "hash": format!("cut-{c}")})))
                    .collect();
                Json(json!({"hashes": hashes, "height": n.tip * 20, "weight": "w"}))
            }),
        )
        .route(
            "/chainweb/0.0/mainnet01/chain/{c}/header/branch",
            post(
                move |State(n): State<NodeMock>,
                      Path(c): Path<u8>,
                      uri: Uri,
                      Query(q): Query<HashMap<String, u64>>| async move {
                    log(&n, &uri);
                    let (lo, hi) = (q["minheight"], q["maxheight"]);
                    let items: Vec<Value> = (lo..=hi)
                        .filter_map(|h| {
                            n.canon
                                .get(&(c, h))
                                .map(|x| json!({"height": h, "hash": x, "chainId": c}))
                        })
                        .collect();
                    Json(json!({"items": items, "next": null, "limit": items.len()}))
                },
            ),
        )
        .route(
            "/chainweb/0.0/mainnet01/chain/{c}/header",
            get(
                |State(n): State<NodeMock>,
                 Path(c): Path<u8>,
                 Query(q): Query<HashMap<String, u64>>| async move {
                    let items: Vec<Value> = (q["minheight"]..=q["maxheight"])
                        .flat_map(|h| n.headers.get(&(c, h)).cloned().unwrap_or_default())
                        .collect();
                    Json(json!({"items": items, "next": null, "limit": items.len()}))
                },
            ),
        )
        .route(
            "/chainweb/0.0/mainnet01/chain/{c}/payload/{hash}/outputs",
            get(
                |State(n): State<NodeMock>, Path((_c, hash)): Path<(u8, String)>| async move {
                    match n.payloads.get(&hash) {
                        Some(p) => Json(p.clone()).into_response(),
                        None => axum::http::StatusCode::NOT_FOUND.into_response(),
                    }
                },
            ),
        )
        .route(
            "/chainweb/0.0/mainnet01/chain/{c}/pact/api/v1/poll",
            post(
                move |State(n): State<NodeMock>,
                      Path(c): Path<u8>,
                      uri: Uri,
                      Json(body): Json<Value>| async move {
                    log(&n, &uri);
                    let mut out = serde_json::Map::new();
                    for rk in body["requestKeys"].as_array().into_iter().flatten() {
                        let rk = rk.as_str().unwrap_or_default();
                        if let Some((pc, bh)) = n.poll.get(rk) {
                            if *pc == c {
                                out.insert(
                                    rk.into(),
                                    json!({"reqKey": rk, "metaData": {"blockHash": bh}, "events": []}),
                                );
                            }
                        }
                    }
                    Json(Value::Object(out))
                },
            ),
        )
        .route(
            "/chainweb/0.0/mainnet01/chain/{c}/pact/api/v1/local",
            post(
                move |State(n): State<NodeMock>, Path(c): Path<u8>, Json(body): Json<Value>| async move {
                    let cmd: Value =
                        serde_json::from_str(body["cmd"].as_str().unwrap_or("{}")).unwrap_or_default();
                    let code = cmd.pointer("/payload/exec/code").and_then(Value::as_str).unwrap_or("");
                    let acct = cmd.pointer("/payload/exec/data/a").and_then(Value::as_str).unwrap_or("");
                    n.calls.lock().unwrap().push(format!("local {c} {code} {acct}"));
                    let meta = json!({"blockHeight": n.tip + 1});
                    Json(match n.local.get(&(c, acct.to_string())) {
                        Some(d) => json!({"result": {"status": "success", "data": d}, "metaData": meta}),
                        None => json!({"result": {"status": "failure",
                            "error": {"message": "with-read: row not found"}}, "metaData": meta}),
                    })
                },
            ),
        )
        .fallback(move |State(n): State<NodeMock>, uri: Uri| async move {
            log(&n, &uri);
            axum::http::StatusCode::NOT_FOUND
        })
        .with_state(n);
    serve(app).await
}

async fn side_server(s: SideMock) -> String {
    let app = Router::new()
        .route(
            "/txs/search",
            get(|State(s): State<SideMock>| async move {
                let mut r: Response = Json(s.search.clone()).into_response();
                if s.truncated {
                    r.headers_mut()
                        .insert("x-search-truncated", "true".parse().unwrap());
                }
                r
            }),
        )
        .route(
            "/txs/recent",
            get(|State(s): State<SideMock>| async move { Json(s.recent.clone()) }),
        )
        .route(
            "/coins",
            get(|State(s): State<SideMock>| async move { s.coins.clone() }),
        )
        .with_state(s);
    serve(app).await
}

/// A context comparing `a` (compat) with `b` (chainweb-data), and the node mock's call log.
pub(super) async fn setup(
    node: NodeMock,
    a: SideMock,
    b: SideMock,
    out: &std::path::Path,
) -> (Ctx, Arc<Mutex<Vec<String>>>) {
    let calls = node.calls.clone();
    let (nu, au, bu) = (
        node_server(node).await,
        side_server(a).await,
        side_server(b).await,
    );
    let allow =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
    let cfg: CompareConfig = toml::from_str(&format!(
        r#"
network = "mainnet01"
rate_rps = 1000.0
follow_depth = 0
pairs = [["a", "b"]]
allowlist = {allow:?}
out_dir = {out:?}
[node]
url = "{nu}"
timeout_ms = 5000
[targets.a]
url = "{au}"
kind = "kda_indexer_compat"
[targets.b]
url = "{bu}"
kind = "chainweb_data"
"#
    ))
    .unwrap();
    (Ctx::new(cfg).await.unwrap(), calls)
}

/// One compare of `path`: the class, the ids of the allowlist entries hit and the
/// unexplained diff paths.
pub(super) async fn compare(ctx: &Ctx, path: &str) -> (String, Vec<String>, Vec<String>) {
    let req = Request::new(path.to_string(), Origin::Seed, None);
    let mut o = ctx.process(&req).await;
    assert_eq!(o.len(), 1);
    let o = o.remove(0);
    let ids: Vec<String> = ctx
        .summary
        .lock()
        .unwrap()
        .allowlist_hits
        .keys()
        .cloned()
        .collect();
    let paths = o
        .record
        .as_ref()
        .filter(|r| r.class == "diff")
        .map(|r| r.diff.iter().map(|e| e.path.clone()).collect())
        .unwrap_or_default();
    (o.class, ids, paths)
}

/// A `/txs/search` row.
pub(super) fn row(rk: &str, chain: u8, height: u64, block: &str) -> Value {
    json!({"requestKey": rk, "chain": chain, "height": height, "blockHash": block,
        "code": "(coin.transfer)", "continuation": null, "creationTime": "t", "initialCode": null,
        "previousSteps": null, "result": "TxSucceeded", "sender": "s"})
}

/// `/txs/search` rows chainweb-data lists in a block off the node's canonical branch.
#[tokio::test]
async fn search_orphan_rows_are_oracle_orphan() {
    let out = tempfile::tempdir().unwrap();
    let mut node = NodeMock {
        tip: 7_400_500,
        ..NodeMock::default()
    };
    node.canon.insert((2, 7_399_318), "OHz-ZmnP".into());
    node.canon.insert((2, 7_399_300), "canon300".into());
    node.canon.insert((5, 7_372_639), "3vd0hYPX".into());
    // A lists the canonical rows; B lists the same plus one in an orphaned block at a height
    // where both have a canonical row, and one at a height A has no row.
    let a_rows = json!([
        row("t1", 2, 7_399_318, "OHz-ZmnP"),
        row("t0", 2, 7_399_300, "canon300")
    ]);
    let b_rows = json!([
        row("t1", 2, 7_399_318, "OHz-ZmnP"),
        row("o1", 2, 7_399_318, "oC2lnmOL"),
        row("t0", 2, 7_399_300, "canon300"),
        row("o2", 5, 7_372_639, "zbojrfLx"),
    ]);
    let a = SideMock {
        search: a_rows.clone(),
        ..SideMock::default()
    };
    let b = SideMock {
        search: b_rows,
        ..SideMock::default()
    };
    let (ctx, calls) = setup(node, a, b, out.path()).await;
    let req = "/txs/search?search=coin.transfer&minheight=7372000&maxheight=7400000&limit=10";
    let (class, ids, paths) = compare(&ctx, req).await;
    assert_eq!(class, "allowlisted", "unexplained: {paths:?}");
    assert_eq!(ids, vec!["oracle-orphan".to_string()]);
    // Decided on the cut's branch, never by the plain header range.
    let calls = calls.lock().unwrap().clone();
    assert!(calls.iter().any(|c| c.contains("/header/branch")));
    assert!(!calls.iter().any(|c| c.contains("/header?")), "{calls:?}");
    // A B-only row in the canonical block is no orphan: it stays a diff.
    let mut node = NodeMock {
        tip: 7_400_500,
        ..NodeMock::default()
    };
    node.canon.insert((2, 7_399_318), "OHz-ZmnP".into());
    node.canon.insert((2, 7_399_300), "canon300".into());
    let b = SideMock {
        search: json!([
            row("t1", 2, 7_399_318, "OHz-ZmnP"),
            row("x9", 2, 7_399_318, "OHz-ZmnP"),
            row("t0", 2, 7_399_300, "canon300"),
        ]),
        ..SideMock::default()
    };
    let a = SideMock {
        search: a_rows,
        ..SideMock::default()
    };
    let out2 = tempfile::tempdir().unwrap();
    let (ctx, _) = setup(node, a, b, out2.path()).await;
    let (class, ids, _) = compare(&ctx, req).await;
    assert_eq!(class, "diff");
    assert!(ids.is_empty(), "{ids:?}");
}

/// Full `pactid` pages (limit 4): rows at or past the last row of a full page leave both
/// sides, then chainweb-data's orphan row is classified.
#[tokio::test]
async fn pactid_orphan_on_full_pages() {
    let out = tempfile::tempdir().unwrap();
    let node = canon_node(&[2], 7_399_000..7_399_010);
    let a = SideMock {
        search: json!([
            row("r9", 2, 7_399_009, "c7399009"),
            row("r8", 2, 7_399_008, "c7399008"),
            row("r7", 2, 7_399_007, "c7399007"),
            row("r6", 2, 7_399_006, "c7399006"),
        ]),
        ..SideMock::default()
    };
    let b = SideMock {
        search: json!([
            row("r9", 2, 7_399_009, "c7399009"),
            row("o8", 2, 7_399_008, "orphan8"),
            row("r8", 2, 7_399_008, "c7399008"),
            row("r7", 2, 7_399_007, "c7399007"),
        ]),
        ..SideMock::default()
    };
    let (ctx, _) = setup(node, a, b, out.path()).await;
    let (class, ids, paths) = compare(&ctx, "/txs/search?pactid=9INv2Zb3&limit=4").await;
    assert_eq!(class, "allowlisted", "unexplained: {paths:?}");
    assert_eq!(ids, vec!["oracle-orphan".to_string()]);
}

/// A node whose canonical block at (c, h) is `c{h}` for every chain in `chains`.
fn canon_node(chains: &[u8], heights: std::ops::Range<u64>) -> NodeMock {
    let mut node = NodeMock {
        tip: 7_400_500,
        ..NodeMock::default()
    };
    for &c in chains {
        for h in heights.clone() {
            node.canon.insert((c, h), format!("c{h}"));
        }
    }
    node
}

/// chainweb-data's tip as `/txs/recent` shows it.
fn recent_at(h: u64) -> Value {
    json!([row("x", 7, h, "any")])
}

/// Rows kda-indexer lists and chainweb-data lacks: above chainweb-data's tip they are
/// oracle lag, settled without the node.
#[tokio::test]
async fn search_rows_above_oracle_tip_are_oracle_lag() {
    let out = tempfile::tempdir().unwrap();
    let node = canon_node(&[0], 7_400_000..7_400_400);
    let a = SideMock {
        search: json!([
            row("new2", 0, 7_400_313, "c7400313"),
            row("new1", 0, 7_400_312, "c7400312"),
            row("old", 0, 7_400_064, "c7400064"),
        ]),
        ..SideMock::default()
    };
    let b = SideMock {
        search: json!([row("old", 0, 7_400_064, "c7400064")]),
        recent: recent_at(7_400_200),
        ..SideMock::default()
    };
    let (ctx, calls) = setup(node, a, b, out.path()).await;
    let req = "/txs/search?search=5unn&minheight=7400000&maxheight=7400400&limit=10";
    let (class, ids, paths) = compare(&ctx, req).await;
    assert_eq!(class, "allowlisted", "unexplained: {paths:?}");
    assert_eq!(ids, vec!["oracle-tip-lag".to_string()]);
    assert!(!calls.lock().unwrap().iter().any(|c| c.contains("/poll")));
}

/// Rows kda-indexer lists and chainweb-data lacks below its tip: oracle gap only when the
/// node's /poll puts the tx in kda-indexer's block and that block is canonical.
#[tokio::test]
async fn search_rows_the_node_confirms_are_oracle_gap() {
    let req = "/txs/search?search=Au4cjML8&minheight=7392000&maxheight=7392500&limit=10";
    let a_rows = json!([
        row("h2", 1, 7_392_417, "c7392417"),
        row("h1", 1, 7_392_368, "c7392368"),
        row("t0", 1, 7_392_000, "c7392000"),
    ]);
    let b = || SideMock {
        search: json!([row("t0", 1, 7_392_000, "c7392000")]),
        recent: recent_at(7_400_400),
        ..SideMock::default()
    };
    let a = || SideMock {
        search: a_rows.clone(),
        ..SideMock::default()
    };
    // Both confirmed.
    let out = tempfile::tempdir().unwrap();
    let mut node = canon_node(&[1], 7_392_000..7_392_500);
    node.poll.insert("h2".into(), (1, "c7392417".into()));
    node.poll.insert("h1".into(), (1, "c7392368".into()));
    let (ctx, _) = setup(node, a(), b(), out.path()).await;
    let (class, ids, paths) = compare(&ctx, req).await;
    assert_eq!(class, "allowlisted", "unexplained: {paths:?}");
    assert_eq!(ids, vec!["oracle-gap".to_string()]);
    // The node puts h1 in another block: h1 stays a diff.
    let out = tempfile::tempdir().unwrap();
    let mut node = canon_node(&[1], 7_392_000..7_392_500);
    node.poll.insert("h2".into(), (1, "c7392417".into()));
    node.poll.insert("h1".into(), (1, "other".into()));
    let (ctx, _) = setup(node, a(), b(), out.path()).await;
    let (class, _, _) = compare(&ctx, req).await;
    assert_eq!(class, "diff");
    // /poll agrees but the block is not canonical on the cut's branch: still a diff.
    let out = tempfile::tempdir().unwrap();
    let mut node = canon_node(&[1], 7_392_000..7_392_500);
    node.canon.insert((1, 7_392_368), "canonical".into());
    node.poll.insert("h2".into(), (1, "c7392417".into()));
    node.poll.insert("h1".into(), (1, "c7392368".into()));
    let (ctx, _) = setup(node, a(), b(), out.path()).await;
    let (class, _, _) = compare(&ctx, req).await;
    assert_eq!(class, "diff");
}

/// A full kda-indexer `pactid` page whose newest row chainweb-data lacks: the node confirms
/// it (oracle gap), and chainweb-data's rows past kda-indexer's last row leave the compare.
#[tokio::test]
async fn pactid_gap_on_a_full_page() {
    let out = tempfile::tempdir().unwrap();
    let mut node = canon_node(&[2], 7_399_000..7_399_400);
    node.poll.insert("n1".into(), (2, "c7399313".into()));
    let a = SideMock {
        search: json!([
            row("n1", 2, 7_399_313, "c7399313"),
            row("r9", 2, 7_399_009, "c7399009"),
            row("r8", 2, 7_399_008, "c7399008"),
        ]),
        ..SideMock::default()
    };
    let b_rows = json!([
        row("r9", 2, 7_399_009, "c7399009"),
        row("r8", 2, 7_399_008, "c7399008"),
        row("r7", 2, 7_399_007, "c7399007"),
    ]);
    let b = SideMock {
        search: b_rows,
        recent: recent_at(7_400_400),
        ..SideMock::default()
    };
    let (ctx, _) = setup(node, a, b, out.path()).await;
    let (class, ids, paths) = compare(&ctx, "/txs/search?pactid=Zq9GapPact&limit=3").await;
    assert_eq!(class, "allowlisted", "unexplained: {paths:?}");
    assert_eq!(ids, vec!["oracle-gap".to_string()]);
}

/// /coins against a chainweb-data 145 heights behind: 2,621.7 KDA lower is that gap (the
/// coinbase minted over it is 2,621.73 KDA). With the tips equal it is a diff.
#[tokio::test]
async fn coins_lag_scales_with_the_tip_gap() {
    let side = |coins: &str, tip: u64| SideMock {
        coins: coins.into(),
        recent: recent_at(tip),
        ..SideMock::default()
    };
    let out = tempfile::tempdir().unwrap();
    let (ctx, _) = setup(
        canon_node(&[], 0..0),
        side("379699987.13176", 7_400_100),
        side("379697365.43176", 7_399_955),
        out.path(),
    )
    .await;
    let (class, ids, paths) = compare(&ctx, "/coins").await;
    assert_eq!(class, "allowlisted", "unexplained: {paths:?}");
    assert_eq!(ids, vec!["oracle-tip-lag".to_string()]);
    let out = tempfile::tempdir().unwrap();
    let (ctx, _) = setup(
        canon_node(&[], 0..0),
        side("379699987.13176", 7_400_100),
        side("379697365.43176", 7_400_100),
        out.path(),
    )
    .await;
    let (class, _, paths) = compare(&ctx, "/coins").await;
    assert_eq!((class.as_str(), paths), ("diff", vec!["/".to_string()]));
}

/// A capped `pactid` answer (X-Search-Truncated) that lacks chainweb-data's old successful
/// continuation is the `pactid-cap` class; the same answer without the header is a diff.
#[tokio::test]
async fn pactid_cap_needs_kda_indexers_header() {
    let failed = |mut r: Value| {
        r["result"] = json!("TxFailed");
        r
    };
    let a_rows = json!([
        failed(row("f9", 1, 7_399_009, "c7399009")),
        failed(row("f8", 1, 7_399_008, "c7399008")),
    ]);
    let b_rows = json!([
        row("ok", 1, 6_072_365, "c6072365"),
        failed(row("f9", 1, 7_399_009, "c7399009")),
        failed(row("f8", 1, 7_399_008, "c7399008")),
    ]);
    for (truncated, class, ids) in [
        (true, "allowlisted", vec!["pactid-cap".to_string()]),
        (false, "diff", vec![]),
    ] {
        let out = tempfile::tempdir().unwrap();
        let mut node = canon_node(&[1], 7_399_000..7_399_010);
        node.canon.insert((1, 6_072_365), "c6072365".into());
        let a = SideMock {
            search: a_rows.clone(),
            truncated,
            ..SideMock::default()
        };
        let b = SideMock {
            search: b_rows.clone(),
            ..SideMock::default()
        };
        let (ctx, _) = setup(node, a, b, out.path()).await;
        let got = compare(&ctx, "/txs/search?pactid=wfSVcapped").await;
        assert_eq!((got.0.as_str(), got.1), (class, ids), "{:?}", got.2);
    }
}

/// R-P5P9-57 cadence: the oracle lane holds only the oracle pairs; the node and native pairs
/// run in their own lane, so the oracle limiter does not pace them.
#[tokio::test]
async fn graphql_lanes_split_oracle_and_node_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let own = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/graphql");
    let allow =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
    let out = dir.path();
    let cfg: CompareConfig = toml::from_str(&format!(
        r#"
network = "mainnet01"
pairs = [["gql", "oracle"], ["gql", "node"], ["gql", "native"]]
allowlist = {allow:?}
out_dir = {out:?}
[node]
url = "{nu}"
timeout_ms = 5000
[graphql]
own_ops_dir = {own:?}
[targets.gql]
url = "http://127.0.0.1:9/graphql"
kind = "kda_indexer_graphql"
[targets.native]
url = "http://127.0.0.1:9"
kind = "kda_indexer_native"
[targets.oracle]
url = "https://oracle.invalid/graphql"
kind = "graphql_oracle"
max_height = 6000000
"#
    ))
    .unwrap();
    let ctx = Ctx::new(cfg).await.unwrap();
    let name = |(pi, op): &(usize, Arc<crate::graphql::GqlOp>)| {
        format!("{}|{}", op.stem, ctx.cfg.pairs[*pi].1)
    };
    let oracle: Vec<String> = ctx.lane_jobs(true).iter().map(name).collect();
    let node: Vec<String> = ctx.lane_jobs(false).iter().map(name).collect();
    assert!(oracle.iter().all(|j| j.ends_with("|oracle")), "{oracle:?}");
    assert!(oracle.iter().any(|j| j == "QBlock|oracle"), "{oracle:?}");
    let mut node_sorted = node.clone();
    node_sorted.sort();
    assert_eq!(
        node_sorted,
        [
            "NodeBalance|native",
            "NodeBalance|node",
            "NodeBlock|node",
            "NodeTx|node"
        ]
    );
    assert_eq!(oracle.len() + node.len(), ctx.gql_jobs().len());
}

/// R-P5P9-57: `coins_every_secs` puts one `/coins` request into the pipeline per period and
/// stops once the pipeline closes.
#[tokio::test]
async fn coins_cadence_feeds_the_pipeline_until_it_closes() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Request>(4);
    let task = tokio::spawn(super::coins_cadence(
        tx.downgrade(),
        std::time::Duration::from_millis(20),
    ));
    for _ in 0..3 {
        let q = rx.recv().await.unwrap();
        assert_eq!(q.path_and_query, "/coins");
        assert_eq!(q.route, crate::normalize::RouteKind::Coins);
    }
    drop(tx);
    tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .expect("the cadence task ends once the pipeline closes")
        .unwrap();
}

/// Our GraphQL (A) for the row rules' own reads: `block(hash)` and `fungibleAccount`.
async fn gql_server(blocks: Value, accounts: Value) -> String {
    let app = Router::new().route(
        "/graphql",
        post(move |Json(req): Json<Value>| {
            let (blocks, accounts) = (blocks.clone(), accounts.clone());
            async move {
                let q = req["query"].as_str().unwrap_or("");
                let v = &req["variables"];
                let pick = |m: &Value, k: &Value| m.get(k.as_str().unwrap_or("")).cloned();
                Json(if q.contains("block(hash") {
                    json!({"data": {"block": pick(&blocks, &v["h"])}})
                } else if q.contains("fungibleAccount(") {
                    json!({"data": {"fungibleAccount": pick(&accounts, &v["a"])}})
                } else {
                    json!({"data": null})
                })
            }
        }),
    );
    format!("{}graphql", serve(app).await)
}

/// R-P5P9-58 row rules against the node: an oracle copy of a request key leaves only when the
/// node's canonical header is our copy's block, and an extra by-key row of ours leaves only
/// when the node's guard holds the key. The node reads are single-row `coin.details` calls on
/// the chain ours lists for the key, and `header/branch` at one height; never a scan.
#[tokio::test]
async fn r58_row_rules_follow_the_node_with_single_row_reads() {
    use crate::graphql::{base64_std, compare_answers, Answer};
    use crate::graphql_norm::{BY_KEY_EXTRA_PATH, ORPHAN_DUPES_PATH};
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock {
        tip: 6_400_000,
        ..NodeMock::default()
    };
    node.canon.insert((0, 6_300_256), "XJfQNVV9".into());
    node.canon.insert((19, 6_293_362), "Q0p95P2u".into());
    let guard = |k: &str| json!({"keys": [k], "pred": "keys-all"});
    node.local.insert(
        (7, "k:251efb06".into()),
        json!({"balance": 0.0, "guard": guard("251efb06")}),
    );
    node.local.insert(
        (1, "k:other".into()),
        json!({"balance": 1.0, "guard": guard("zzz")}),
    );
    let calls = node.calls.clone();
    let nu = node_server(node).await;
    let chains = |cs: &[(&str, &str)]| {
        let rows: Vec<Value> = cs
            .iter()
            .map(|(c, k)| json!({"chainId": c, "guard": {"keys": [k]}}))
            .collect();
        json!({ "chainAccounts": rows })
    };
    let au = gql_server(
        json!({"XJfQNVV9": {"chainId": 0, "height": 6_300_256},
               "VpOurs": {"chainId": "19", "height": 6_293_362}}),
        json!({"k:251efb06": chains(&[("3", "x"), ("7", "251efb06")]),
               "k:other": chains(&[("1", "251efb06")])}),
    )
    .await;
    let allow =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
    let out = dir.path();
    let cfg: CompareConfig = toml::from_str(&format!(
        r#"
network = "mainnet01"
pairs = [["a", "b"]]
allowlist = {allow:?}
out_dir = {out:?}
[node]
url = "{nu}"
timeout_ms = 5000
[graphql]
own_ops_dir = "no-such-dir"
field_rules = [
  {{ field = "transactionsByPublicKey", rule = "oracle_orphan_dupes" }},
  {{ field = "fungibleAccountsByPublicKey", rule = "by_key_extra_row" }},
]
[targets.a]
url = "{au}"
kind = "kda_indexer_graphql"
[targets.b]
url = "https://oracle.invalid/graphql"
kind = "graphql_oracle"
max_height = 6400000
"#
    ))
    .unwrap();
    cfg.validate().unwrap();
    let ctx = Ctx::new(cfg).await.unwrap();
    let ta = &ctx.targets["a"];
    let ans = |body: Value| Answer {
        body,
        status: 200,
        raw: String::new(),
        ms: Vec::new(),
    };
    let e = |b: &str, rk: &str| {
        let id = base64_std(format!("Transaction:[\"{b}\",\"{rk}\"]").as_bytes());
        json!({"node": {"id": id, "hash": rk}})
    };
    let conn = |n: u64, edges: Vec<Value>| {
        ans(
            json!({"data": {"transactionsByPublicKey": {"totalCount": n, "edges": edges,
            "pageInfo": {"hasNextPage": false}}}}),
        )
    };
    // aYo73A6i: ours in XJfQNVV9, the node's canonical block at c0 h6300256; the oracle also
    // lists it in Ye2tcl_c. VpiDdEFw: the node's canonical at our copy's height is neither
    // copy, so nothing is settled and the oracle keeps both.
    let ours = || {
        conn(
            3,
            vec![
                e("XJfQNVV9", "aYo73A6i"),
                e("VpOurs", "VpiDdEFw"),
                e("B1", "r3"),
            ],
        )
    };
    let mut a = ours();
    let mut b = conn(
        5,
        vec![
            e("XJfQNVV9", "aYo73A6i"),
            e("Ye2tcl_c", "aYo73A6i"),
            e("VpOurs", "VpiDdEFw"),
            e("3JSVsmL1", "VpiDdEFw"),
            e("B1", "r3"),
        ],
    );
    let none = serde_json::Map::new();
    assert_eq!(
        ctx.gql_row_rules(ta, &none, &mut a, &mut b).await,
        [ORPHAN_DUPES_PATH]
    );
    let kept = &b.body["data"]["transactionsByPublicKey"];
    assert_eq!(kept["totalCount"], 4);
    assert_eq!(kept["edges"].as_array().unwrap().len(), 4);
    assert!(!kept["edges"].to_string().contains("Ye2tcl_c"));
    // The unsettled copy is still a difference (a row and the count).
    let d = compare_answers(
        &mut a,
        &mut b,
        u64::MAX,
        10_000,
        &ctx.cfg.graphql.field_rules,
    );
    assert!(
        d.iter()
            .any(|x| x.path == "/data/transactionsByPublicKey/totalCount"),
        "{d:?}"
    );
    // With only the settled duplicate, the compare is the visible hit alone.
    let mut a = ours();
    let mut b = conn(
        4,
        vec![
            e("XJfQNVV9", "aYo73A6i"),
            e("Ye2tcl_c", "aYo73A6i"),
            e("VpOurs", "VpiDdEFw"),
            e("B1", "r3"),
        ],
    );
    a.body["data"]["transactionsByPublicKey"]["totalCount"] = json!(3);
    let hits = ctx.gql_row_rules(ta, &none, &mut a, &mut b).await;
    assert_eq!(hits, [ORPHAN_DUPES_PATH]);
    assert!(compare_answers(&mut a, &mut b, u64::MAX, 10_000, &[]).is_empty());

    // By key: k:251efb06 is ours only and the node's c7 guard holds the key; k:other is ours
    // only but the node's c1 guard does not hold it, so it stays a diff.
    let acct = |n: &str| json!({"accountName": n, "fungibleName": "coin"});
    let list = |rows: Vec<Value>| ans(json!({"data": {"fungibleAccountsByPublicKey": rows}}));
    let mut vars = serde_json::Map::new();
    vars.insert("publicKey".into(), json!("251efb06"));
    vars.insert("fungibleName".into(), json!("coin"));
    let mut a = list(vec![acct("251efb06"), acct("k:251efb06"), acct("k:other")]);
    let mut b = list(vec![acct("251efb06")]);
    let hits = ctx.gql_row_rules(ta, &vars, &mut a, &mut b).await;
    assert_eq!(hits, [BY_KEY_EXTRA_PATH]);
    assert_eq!(
        a.body["data"]["fungibleAccountsByPublicKey"],
        json!([acct("251efb06"), acct("k:other")])
    );
    let d = compare_answers(&mut a, &mut b, u64::MAX, 10_000, &[]);
    assert_eq!(d.len(), 1, "{d:?}");
    assert_eq!(d[0].path, "/data/fungibleAccountsByPublicKey/1");
    // The node saw two single-row details reads, on the chains ours lists for the key.
    let calls = calls.lock().unwrap().clone();
    let local: Vec<&String> = calls.iter().filter(|c| c.starts_with("local ")).collect();
    assert_eq!(
        local,
        [
            "local 7 (coin.details (read-msg \"a\")) k:251efb06",
            "local 1 (coin.details (read-msg \"a\")) k:other"
        ]
    );
    assert!(calls.iter().all(|c| !c.contains("/header?")), "{calls:?}");
}

/// N2: the one-block skew accepts a coinbase only from a lone header at the height; a fork
/// there (the plain header range returns orphans too) leaves the diff.
#[tokio::test]
async fn n2_mined_at_needs_a_single_header_at_the_height() {
    let dir = tempfile::tempdir().unwrap();
    let cb = |to: &str| {
        let v = json!({"result": {"status": "success"}, "events": [{"name": "TRANSFER",
            "module": {"name": "coin", "namespace": null}, "params": ["", to, 0.9]}]});
        json!({"coinbase": kdx_types::hash::b64url_encode(v.to_string().as_bytes())})
    };
    let hd = |h: &str, p: &str| json!({"hash": h, "payloadHash": p, "height": 100, "chainId": 0});
    let mut node = NodeMock::default();
    node.headers.insert((0, 100), vec![hd("h1", "p1")]);
    node.headers
        .insert((1, 100), vec![hd("h1", "p1"), hd("h2", "p2")]);
    node.payloads.insert("p1".into(), cb("k:miner"));
    node.payloads.insert("p2".into(), cb("k:miner"));
    let (ctx, _) = setup(node, SideMock::default(), SideMock::default(), dir.path()).await;
    assert!(ctx.mined_at(0, 100, "k:miner").await);
    assert!(!ctx.mined_at(0, 100, "k:other").await);
    // Two headers at the height: a fork, even though both coinbases pay the miner.
    assert!(!ctx.mined_at(1, 100, "k:miner").await);
}

/// R3: a request key whose copy in our data is at height 0 (genesis keys repeat across
/// chains) is never settled by `oracle_orphan_dupes`: the oracle keeps both copies.
#[tokio::test]
async fn r3_genesis_request_keys_are_not_deduped() {
    use crate::graphql::{base64_std, Answer};
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock {
        tip: 100,
        ..NodeMock::default()
    };
    node.canon.insert((0, 0), "GenOurs".into());
    let nu = node_server(node).await;
    let au = gql_server(json!({"GenOurs": {"chainId": 0, "height": 0}}), json!({})).await;
    let allow =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
    let out = dir.path();
    let cfg: CompareConfig = toml::from_str(&format!(
        r#"
network = "mainnet01"
pairs = [["a", "b"]]
allowlist = {allow:?}
out_dir = {out:?}
[node]
url = "{nu}"
timeout_ms = 5000
[graphql]
own_ops_dir = "no-such-dir"
field_rules = [
  {{ field = "transactionsByPublicKey", rule = "oracle_orphan_dupes" }},
]
[targets.a]
url = "{au}"
kind = "kda_indexer_graphql"
[targets.b]
url = "https://oracle.invalid/graphql"
kind = "graphql_oracle"
max_height = 100
"#
    ))
    .unwrap();
    let ctx = Ctx::new(cfg).await.unwrap();
    let ta = &ctx.targets["a"];
    let e = |b: &str, rk: &str| {
        let id = base64_std(format!("Transaction:[\"{b}\",\"{rk}\"]").as_bytes());
        json!({"node": {"id": id, "hash": rk}})
    };
    let conn = |n: u64, edges: Vec<Value>| Answer {
        body: json!({"data": {"transactionsByPublicKey": {"totalCount": n, "edges": edges,
            "pageInfo": {"hasNextPage": false}}}}),
        status: 200,
        raw: String::new(),
        ms: Vec::new(),
    };
    let mut a = conn(1, vec![e("GenOurs", "genesis-rk")]);
    let mut b = conn(
        2,
        vec![e("GenOurs", "genesis-rk"), e("GenOther", "genesis-rk")],
    );
    let none = serde_json::Map::new();
    let hits = ctx.gql_row_rules(ta, &none, &mut a, &mut b).await;
    assert!(hits.is_empty(), "{hits:?}");
    let kept = &b.body["data"]["transactionsByPublicKey"];
    assert_eq!(kept["totalCount"], 2);
    assert_eq!(kept["edges"].as_array().unwrap().len(), 2);
}
