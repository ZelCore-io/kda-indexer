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
use crate::graphql_window::Reads;

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
    /// Whole `/poll` entries by request key (chain, entry), for reads that need the height
    /// and events; `poll_fail`: every `/poll` answers 500.
    pub polls: HashMap<String, (u8, Value)>,
    pub poll_fail: bool,
    /// `header/branch` answers nothing above this height (a height above the node's cut).
    pub branch_top: Option<u64>,
    /// `header/branch` answers this header at (chain, height) before any other rule (a
    /// canonical header the plain `/header` range does not hold).
    pub branch: HashMap<(u8, u64), Value>,
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
                    // The canonical header at each height: `canon`'s block (its stored
                    // header when `headers` holds it), else the only header stored there.
                    let items: Vec<Value> = (lo..=hi)
                        .filter(|h| n.branch_top.is_none_or(|top| *h <= top))
                        .filter_map(|h| {
                            if let Some(x) = n.branch.get(&(c, h)) {
                                return Some(x.clone());
                            }
                            let stored = n.headers.get(&(c, h)).cloned().unwrap_or_default();
                            match n.canon.get(&(c, h)) {
                                Some(x) => Some(
                                    stored
                                        .into_iter()
                                        .find(|s| s["hash"] == json!(x))
                                        .unwrap_or_else(|| json!({"height": h, "hash": x, "chainId": c})),
                                ),
                                None if stored.len() == 1 => stored.into_iter().next(),
                                None => None,
                            }
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
            "/chainweb/0.0/mainnet01/chain/{c}/header/{hash}",
            get(
                |State(n): State<NodeMock>, Path((c, hash)): Path<(u8, String)>| async move {
                    n.calls.lock().unwrap().push(format!("header-by-hash {hash}"));
                    let found = n
                        .headers
                        .iter()
                        .filter(|((hc, _), _)| *hc == c)
                        .flat_map(|(_, hs)| hs.iter())
                        .find(|h| h["hash"] == json!(hash))
                        .cloned();
                    match found {
                        Some(h) => Json(h).into_response(),
                        None => axum::http::StatusCode::NOT_FOUND.into_response(),
                    }
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
                    let keys = body["requestKeys"].as_array().map_or(0, Vec::len);
                    n.calls.lock().unwrap().push(format!("poll {c} {keys}"));
                    if n.poll_fail {
                        return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }
                    let mut out = serde_json::Map::new();
                    for rk in body["requestKeys"].as_array().into_iter().flatten() {
                        let rk = rk.as_str().unwrap_or_default();
                        if let Some((pc, v)) = n.polls.get(rk) {
                            if *pc == c {
                                out.insert(rk.into(), v.clone());
                            }
                        }
                        if let Some((pc, bh)) = n.poll.get(rk) {
                            if *pc == c {
                                out.insert(
                                    rk.into(),
                                    json!({"reqKey": rk, "metaData": {"blockHash": bh}, "events": []}),
                                );
                            }
                        }
                    }
                    Json(Value::Object(out)).into_response()
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
    let node_calls = node.calls.clone();
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
    // R-P5P9-64 M-8: the row rules' node reads count against the referee's run cap. Spent,
    // they read nothing and drop nothing (the rows stay a diff); the orphan check alike.
    ctx.gql
        .node_budget
        .store(0, std::sync::atomic::Ordering::Relaxed);
    let mut a = list(vec![acct("251efb06"), acct("k:251efb06")]);
    let mut b = list(vec![acct("251efb06")]);
    assert!(ctx
        .gql_row_rules(ta, &vars, &mut a, &mut b)
        .await
        .is_empty());
    ctx.canon.lock().unwrap().clear();
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
    assert!(ctx
        .gql_row_rules(ta, &none, &mut a, &mut b)
        .await
        .is_empty());
    let after = node_calls.lock().unwrap().len();
    assert_eq!(after, calls.len(), "no node read past the cap");
}

/// N2: the one-block skew accepts a coinbase only from the canonical block at the height. A
/// fork the cut's branch does not settle (the mock's branch answers no header where two are
/// stored and none is `canon`) leaves the diff.
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
    assert!(ctx.mined_at(0, 100, "k:miner", &mut Reads::default()).await);
    assert!(!ctx.mined_at(0, 100, "k:other", &mut Reads::default()).await);
    // Two headers at the height: a fork, even though both coinbases pay the miner.
    assert!(!ctx.mined_at(1, 100, "k:miner", &mut Reads::default()).await);
}

/// R-P5P9-68 (7): N2's `mined_at` takes the canonical block from the cut's branch (the H7
/// helper, R-P5P9-67 m-5), never the `/header` range by height, which holds orphans too.
#[tokio::test]
async fn c4_mined_at_takes_the_canonical_block_from_the_cuts_branch() {
    let dir = tempfile::tempdir().unwrap();
    let cb = |to: &str| {
        let v = json!({"result": {"status": "success"}, "events": [{"name": "TRANSFER",
            "module": {"name": "coin", "namespace": null}, "params": ["", to, 0.9]}]});
        json!({"coinbase": kdx_types::hash::b64url_encode(v.to_string().as_bytes())})
    };
    let hd = |h: &str, p: &str, at: u64, c: u8| json!({"hash": h, "payloadHash": p, "height": at, "chainId": c});
    let mut node = NodeMock {
        tip: 200,
        branch_top: Some(120),
        ..NodeMock::default()
    };
    // Chain 2: the range holds only an orphan, whose coinbase pays the miner; the cut's
    // branch holds the canonical block, whose coinbase pays another account.
    node.headers
        .insert((2, 100), vec![hd("orphan2", "po2", 100, 2)]);
    node.branch.insert((2, 100), hd("canon2", "pc2", 100, 2));
    node.payloads.insert("po2".into(), cb("k:miner"));
    node.payloads.insert("pc2".into(), cb("k:other"));
    // Chain 3: a fork in the range; the cut's branch names the block that pays the miner.
    node.headers.insert(
        (3, 100),
        vec![hd("orphan3", "po3", 100, 3), hd("canon3", "pc3", 100, 3)],
    );
    node.canon.insert((3, 100), "canon3".into());
    node.payloads.insert("po3".into(), cb("k:other"));
    node.payloads.insert("pc3".into(), cb("k:miner"));
    // Chain 4: one header at a height above the cut (the branch holds none there).
    node.headers
        .insert((4, 150), vec![hd("above4", "pa4", 150, 4)]);
    node.payloads.insert("pa4".into(), cb("k:miner"));
    let (ctx, calls) = setup(node, SideMock::default(), SideMock::default(), dir.path()).await;
    assert!(
        !ctx.mined_at(2, 100, "k:miner", &mut Reads::default()).await,
        "an orphan at the height is not taken as canonical"
    );
    assert!(
        ctx.mined_at(2, 100, "k:other", &mut Reads::default()).await,
        "the canonical block's coinbase"
    );
    assert!(
        ctx.mined_at(3, 100, "k:miner", &mut Reads::default()).await,
        "the branch settles a fork"
    );
    assert!(!ctx.mined_at(3, 100, "k:other", &mut Reads::default()).await);
    assert!(
        !ctx.mined_at(4, 150, "k:miner", &mut Reads::default()).await,
        "above the cut: not settled"
    );
    assert!(
        calls
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.contains("header/branch")),
        "the canonical block is read through the cut's branch"
    );
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

/// A GraphQL server of whole block lists for the harness's list query (R-P5P9-60 H2): it
/// pages `lists[<block hash>]` two rows at a time (whatever `first` asks), the cursor being
/// the next index, and counts its calls.
async fn list_server(lists: Value, calls: Arc<Mutex<usize>>) -> String {
    keyed_list_server(lists, calls, 2, None, false).await
}

/// [`list_server`] with `per` rows a page and, with `fail`, a 500 for that (block, page).
/// Each edge also answers the harness's key read (`kdxKey`, R-P5P9-61 H6): a list entry is
/// a node (its key is its `hash`) or a `[key, node]` pair. A read whose `first` is not the
/// harness's page size (I-5's one-page retry) answers that many rows. With `oracle_cursor`,
/// a block named `CUR…` fails every cursor page (`after` set), as the oracle's
/// `Block.events` does, and `DEAD…` also fails the one-page retry.
async fn keyed_list_server(
    lists: Value,
    calls: Arc<Mutex<usize>>,
    per: usize,
    fail: Option<(&'static str, usize)>,
    oracle_cursor: bool,
) -> String {
    let app = Router::new().route(
        "/graphql",
        post(move |Json(req): Json<Value>| {
            let (lists, calls) = (lists.clone(), calls.clone());
            async move {
                *calls.lock().unwrap() += 1;
                let v = &req["variables"];
                let p = v["p"].as_str().unwrap_or("");
                let rows = lists[p].as_array().cloned().unwrap_or_default();
                let at: usize = v["after"].as_str().map_or(0, |c| c.parse().unwrap());
                let q = req["query"].as_str().unwrap_or("");
                let first: usize = q
                    .split_once("(first: ")
                    .and_then(|(_, r)| r.split(|c: char| !c.is_ascii_digit()).next())
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(per);
                let one = first != crate::graphql_scope::PAGE_SIZE as usize;
                let per = if one { first } else { per };
                let cursor = v["after"].is_string();
                if fail == Some((p, at / per))
                    || (oracle_cursor && p.starts_with("CUR") && cursor)
                    || (oracle_cursor && p.starts_with("DEAD") && (cursor || one))
                {
                    let err = json!({"errors": [{"message": "invalid input syntax for type integer",
                        "extensions": {"code": "INTERNAL_SERVER_ERROR"}}], "data": null});
                    return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, Json(err))
                        .into_response();
                }
                let end = (at + per).min(rows.len());
                // REP: a cursor page repeats the last row of the page before it.
                let start = if p.starts_with("REP") && cursor {
                    at - 1
                } else {
                    at
                };
                let edges: Vec<Value> = rows[start..end]
                    .iter()
                    .map(|r| match r {
                        Value::Array(kn) => json!({"node": kn[1], "kdxKey": {"hash": kn[0]}}),
                        n => json!({"node": n, "kdxKey": {"hash": n["hash"]}}),
                    })
                    .collect();
                Json(json!({"data": {"block": {"c": {
                    "pageInfo": {"hasNextPage": end < rows.len(), "endCursor": end.to_string()},
                    "edges": edges}}}}))
                .into_response()
            }
        }),
    );
    format!("{}graphql", serve(app).await)
}

/// R-P5P9-60 H2: a nested block list cut on either side is read to its end on both sides at
/// each target's rate, and the full sets compare: the same rows in another order and cut at
/// other members are not a diff; a row only one side's full list has stays a diff; a list
/// past `MAX_PAGES` pages, or with no parent hash in the answer, is left as it was.
#[tokio::test]
async fn h2_cut_block_lists_are_finished_and_compared_as_sets() {
    use crate::graphql::{compare_answers_with, parse_op, Answer};
    use crate::graphql_scope::MAX_PAGES;
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let t = |i: u32| json!({"id": format!("T{i}"), "hash": format!("h{i}")});
    let all: Vec<Value> = (1..=7).map(t).collect();
    let long: Vec<Value> = (1..=(2 * MAX_PAGES as u32 + 2)).map(t).collect();
    let (ca, cb) = (Arc::new(Mutex::new(0)), Arc::new(Mutex::new(0)));
    // Ours lacks T7 in block "M"; both have the whole of "B" and the over-long "L".
    let au = list_server(json!({"B": all, "M": all[..6], "L": long}), ca.clone()).await;
    let bu = list_server(json!({"B": all, "M": all, "L": long}), cb.clone()).await;
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
[targets.a]
url = "{au}"
kind = "kda_indexer_graphql"
rate_rps = 1000.0
[targets.b]
url = "{bu}"
kind = "kda_indexer_graphql"
rate_rps = 1000.0
"#
    ))
    .unwrap();
    let ctx = Ctx::new(cfg).await.unwrap();
    let (ta, tb) = (&ctx.targets["a"], &ctx.targets["b"]);
    let op = parse_op(
        "QBlock",
        "query QBlock($hash: String!) { block(hash: $hash) { hash transactions(first: 5) { totalCount pageInfo { hasNextPage } edges { cursor node { id hash } } } } }",
        false,
    )
    .unwrap();
    let page = |hash: Option<&str>, total: usize, ids: &[u32]| {
        let edges: Vec<Value> = ids
            .iter()
            .map(|i| json!({"cursor": "x", "node": t(*i)}))
            .collect();
        let mut block = json!({"transactions": {"totalCount": total,
            "pageInfo": {"hasNextPage": total > ids.len()}, "edges": edges}});
        if let Some(h) = hash {
            block["hash"] = json!(h);
        }
        Answer {
            body: json!({"data": {"block": block}}),
            status: 200,
            raw: String::new(),
            ms: Vec::new(),
        }
    };
    let (ctx, op) = (&ctx, &op);
    let run = |mut a: Answer, mut b: Answer| async move {
        let (n, _) = ctx
            .gql_finish_lists(op, &serde_json::Map::new(), ta, tb, &mut a, &mut b)
            .await;
        let d = compare_answers_with(&mut a, &mut b, u64::MAX, 10_000, &[], false);
        let paths: Vec<String> = d.into_iter().map(|e| e.path).collect();
        (n, paths)
    };
    // Block B: both pages cut at other members and in another order: finished, no diff.
    let (n, d) = run(
        page(Some("B"), 7, &[5, 4, 3, 2, 1]),
        page(Some("B"), 7, &[3, 4, 5, 6, 7]),
    )
    .await;
    assert_eq!((n, d.clone()), (1, vec![]), "{d:?}");
    assert_eq!((*ca.lock().unwrap(), *cb.lock().unwrap()), (4, 4));
    // Block M: ours' full list lacks T7, a real difference: both pages claim 7 (so the count
    // does not differ), ours finishes to 6 rows, and the missing row stays a diff in the edges.
    let (n, d) = run(
        page(Some("M"), 7, &[1, 2, 3, 4, 5]),
        page(Some("M"), 7, &[1, 2, 3, 4, 5]),
    )
    .await;
    assert_eq!(n, 1);
    assert!(
        d.iter()
            .any(|p| p.starts_with("/data/block/transactions/edges")),
        "{d:?}"
    );
    // Block L: past MAX_PAGES pages: not finished; the cut pages stay a diff.
    let (n, d) = run(
        page(Some("L"), 42, &[1, 2, 3, 4, 5]),
        page(Some("L"), 42, &[2, 3, 4, 5, 6]),
    )
    .await;
    assert_eq!(n, 0);
    assert!(
        d.iter()
            .any(|p| p.starts_with("/data/block/transactions/edges")),
        "{d:?}"
    );
    // No parent hash in the answer: not finished, no reads.
    let before = *ca.lock().unwrap();
    let (n, d) = run(
        page(None, 7, &[5, 4, 3, 2, 1]),
        page(None, 7, &[3, 4, 5, 6, 7]),
    )
    .await;
    assert_eq!(n, 0);
    assert!(!d.is_empty());
    assert_eq!(*ca.lock().unwrap(), before);
    // Not cut on either side: compared as a set with no reads (order only).
    let (n, d) = run(
        page(Some("B"), 3, &[3, 1, 2]),
        page(Some("B"), 3, &[1, 2, 3]),
    )
    .await;
    assert_eq!((n, d), (0, vec![]));
    assert_eq!(*ca.lock().unwrap(), before);
}

/// A context for direct GraphQL oracle-pair checks against `node` (the targets are not read).
async fn gql_ctx(node: NodeMock, out: &std::path::Path) -> Ctx {
    gql_ctx_at(node, out, "http://127.0.0.1:9/graphql").await
}

/// [`gql_ctx`] with ours (A) at `a_url`.
async fn gql_ctx_at(node: NodeMock, out: &std::path::Path, a_url: &str) -> Ctx {
    let nu = node_server(node).await;
    let allow =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
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
[targets.a]
url = "{a_url}"
kind = "kda_indexer_graphql"
[targets.b]
url = "https://oracle.invalid/graphql"
kind = "graphql_oracle"
max_height = 7000000
"#
    ))
    .unwrap();
    Ctx::new(cfg).await.unwrap()
}

/// A transaction row of Transactions' shape: a relay id (block, request key), a height and
/// the sender's `creationTime` (the oracle's sort key).
fn h7_tx(rk: &str, h: u64, block: &str, ct: &str) -> Value {
    let id = crate::graphql::base64_std(format!("Transaction:[\"{block}\",\"{rk}\"]").as_bytes());
    json!({"cursor": "c", "node": {"id": id, "hash": rk,
        "cmd": {"meta": {"chainId": "0", "creationTime": ct}},
        "result": {"height": h, "block": {"hash": block, "chainId": 0}}}})
}

/// A node `/poll` entry: the request key in `block` at `h`, with these events.
fn h7_poll(rk: &str, block: &str, h: u64, events: Value) -> (String, (u8, Value)) {
    (
        rk.to_string(),
        (
            0,
            json!({"reqKey": rk, "metaData": {"blockHash": block, "blockHeight": h},
                "events": events}),
        ),
    )
}

/// A root connection page.
fn h7_page(root: &str, more: bool, edges: Vec<Value>) -> crate::graphql::Answer {
    crate::graphql::Answer {
        body: json!({"data": {root: {"totalCount": 40,
            "pageInfo": {"hasNextPage": more}, "edges": edges}}}),
        status: 200,
        raw: String::new(),
        ms: Vec::new(),
    }
}

/// R-P5P9-62 H7, the window referee (it replaces R-P5P9-61 H5, whose cases stay below as H7
/// cases). Rows match by identity (request key, index, block, chain); each side's window is
/// its own sort key (ours by height, the oracle's by `creationTime`); every one-sided row
/// gets a single-row node read. The CG-3T sort-key shape, an oracle orphan and a
/// node-confirmed oracle gap are explained; the page-join shape, an ours-only row the node
/// rejects, two rows equal in value under different request keys and a node failure stay
/// diffs. The node reads per compare are in the record.
#[tokio::test]
async fn h7_window_referee_accounts_for_every_row_with_node_evidence() {
    use crate::graphql::Answer;
    use crate::graphql_window::{ROW_PATH, WINDOW_PATH};
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock {
        tip: 7_000_000,
        ..NodeMock::default()
    };
    // H5's transfers: request key -> (block, height, receiver).
    let canon = [
        ("t1", "B1", 101, "r"),
        ("t2", "B2", 100, "r"),
        ("t3", "B2", 100, "r"),
        ("t4", "B2", 100, "r"),
        ("t6", "B1", 101, "r"),
        ("t7", "B3", 99, "r"),
        ("w1", "B2", 100, "k:miner-a"),
        ("w2", "B2", 100, "k:miner-a"),
        ("k1", "B2", 100, "r"),
        ("k2", "B2", 100, "r"),
    ];
    for (rk, b, h, to) in canon {
        let (k, v) = h7_poll(
            rk,
            b,
            h,
            json!([{"name": "TRANSFER", "params": ["s", to, 1.5]}]),
        );
        node.polls.insert(k, v);
    }
    // Transactions: r100..r105 in B<h>; z104 is in Q104 on the node (ours lists it in B104).
    for (rk, h) in [
        ("r100", 100),
        ("r103", 103),
        ("r104", 104),
        ("r105", 105),
        ("x104", 104),
    ] {
        let (k, v) = h7_poll(rk, &format!("B{h}"), h, json!([]));
        node.polls.insert(k, v);
    }
    let (k, v) = h7_poll("z104", "Q104", 104, json!([]));
    node.polls.insert(k, v);
    let calls = node.calls.clone();
    let ctx = gql_ctx(node.clone(), dir.path()).await;
    let ctx = &ctx;
    let polls = || {
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.starts_with("poll"))
            .count()
    };
    let run = |mut a: Answer, mut b: Answer| async move {
        let ta = &ctx.targets["a"];
        let vars = serde_json::Map::new();
        ctx.gql_settle(ta, &vars, &mut a, &mut b, 7_000_000, false)
            .await
    };
    let hit = |d: &[crate::report::DiffEntry]| d.iter().find(|e| e.path == WINDOW_PATH).cloned();
    let explained = |d: &[crate::report::DiffEntry]| {
        hit(d).is_some()
            && d.iter()
                .all(|e| e.path.starts_with("/~") && e.path != ROW_PATH)
    };
    let stays_diff = |d: &[crate::report::DiffEntry]| {
        d.iter()
            .any(|e| !e.path.starts_with("/~") || e.path == ROW_PATH)
    };
    let txs = |more: bool, rows: &[(&str, u64, &str)]| {
        h7_page(
            "transactions",
            more,
            rows.iter()
                .map(|(rk, h, ct)| h7_tx(rk, *h, &format!("B{h}"), ct))
                .collect(),
        )
    };
    let t = |m: &str| format!("2024-01-01T10:{m}Z");
    let (t05, t01, t04, t03, t02) = (t("05:00"), t("01:00"), t("04:00"), t("03:00"), t("02:00"));

    // 1. The CG-3T sort-key shape: ours the top 3 by height, the oracle the top 3 by
    //    creationTime. r104 (ours only) sits below the oracle's lowest creationTime; r100
    //    (oracle only) below our lowest height. Both canonical: explained, 2 single-row reads.
    let before = polls();
    let (d, _) = run(
        txs(
            true,
            &[
                ("r105", 105, &t05),
                ("r104", 104, &t01),
                ("r103", 103, &t04),
            ],
        ),
        txs(
            true,
            &[
                ("r105", 105, &t05),
                ("r103", 103, &t04),
                ("r100", 100, &t03),
            ],
        ),
    )
    .await;
    assert!(explained(&d), "{d:?}");
    let ev = hit(&d).unwrap();
    let text = format!("{} {}", ev.a.clone().unwrap(), ev.b.clone().unwrap());
    assert!(text.contains("outside-oracle-window"), "{text}");
    assert!(text.contains("below-our-window"), "{text}");
    assert_eq!(ev.b.clone().unwrap()["nodeReads"], json!(2), "{text}");
    assert_eq!(polls() - before, 2);
    //    The same rows on full pages, in the oracle's creationTime order: explained, no read.
    let before = polls();
    let (d, _) = run(
        txs(
            false,
            &[
                ("r105", 105, &t05),
                ("r104", 104, &t01),
                ("r103", 103, &t04),
            ],
        ),
        txs(
            false,
            &[
                ("r105", 105, &t05),
                ("r103", 103, &t04),
                ("r104", 104, &t01),
            ],
        ),
    )
    .await;
    assert!(explained(&d), "{d:?}");
    assert_eq!(polls(), before);
    //    Ours out of height order is our defect, never explained, though every row would
    //    otherwise be accounted for (r104 ours-only and canonical, r100 below our window).
    let (d, note) = run(
        txs(
            true,
            &[
                ("r105", 105, &t05),
                ("r103", 103, &t04),
                ("r104", 104, &t01),
            ],
        ),
        txs(
            true,
            &[
                ("r105", 105, &t05),
                ("r103", 103, &t04),
                ("r100", 100, &t03),
            ],
        ),
    )
    .await;
    assert!(stays_diff(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("descending height order"));
    let (d, _) = run(
        txs(
            false,
            &[
                ("r105", 105, &t05),
                ("r103", 103, &t04),
                ("r104", 104, &t01),
            ],
        ),
        txs(
            false,
            &[
                ("r105", 105, &t05),
                ("r104", 104, &t01),
                ("r103", 103, &t04),
            ],
        ),
    )
    .await;
    assert!(stays_diff(&d), "{d:?}");

    // 2. The page-join shape: x104, canonical, only on the oracle's page and strictly
    //    inside our window (above our lowest height 103): a diff, the row named.
    let (d, note) = run(
        txs(
            true,
            &[
                ("r105", 105, &t05),
                ("r104", 104, &t01),
                ("r103", 103, &t04),
            ],
        ),
        txs(
            true,
            &[
                ("r105", 105, &t05),
                ("x104", 104, &t02),
                ("r104", 104, &t01),
            ],
        ),
    )
    .await;
    assert!(stays_diff(&d), "{d:?}");
    let row = d.iter().find(|e| e.path == ROW_PATH).expect("the row");
    assert!(
        row.b.as_ref().unwrap().to_string().contains("x104"),
        "{row:?}"
    );
    assert!(note.unwrap_or_default().contains("inside our window"));

    // 3. An oracle orphan: the oracle's copy of r104 in block X104, the node's is B104.
    let mut orphan = h7_tx("r104", 104, "X104", &t01);
    orphan["node"]["result"]["block"]["hash"] = json!("X104");
    let mut b = txs(false, &[("r105", 105, &t05), ("r104", 104, &t01)]);
    b.body["data"]["transactions"]["edges"]
        .as_array_mut()
        .unwrap()
        .push(orphan);
    let (d, _) = run(txs(false, &[("r105", 105, &t05), ("r104", 104, &t01)]), b).await;
    assert!(explained(&d), "{d:?}");
    assert!(hit(&d).unwrap().b.unwrap().to_string().contains("orphan"));

    // 4. A node-confirmed oracle gap: r104 canonical, inside the oracle's complete list.
    let (d, _) = run(
        txs(
            false,
            &[
                ("r105", 105, &t05),
                ("r104", 104, &t01),
                ("r103", 103, &t04),
            ],
        ),
        txs(false, &[("r105", 105, &t05), ("r103", 103, &t04)]),
    )
    .await;
    assert!(explained(&d), "{d:?}");
    assert!(hit(&d)
        .unwrap()
        .a
        .unwrap()
        .to_string()
        .contains("oracle-gap"));

    // 5. An ours-only row the node puts in another block: a diff.
    let mut z = h7_tx("z104", 104, "B104", &t01);
    z["node"]["result"]["block"]["hash"] = json!("B104");
    let mut a = txs(false, &[("r105", 105, &t05)]);
    a.body["data"]["transactions"]["edges"]
        .as_array_mut()
        .unwrap()
        .push(z);
    let (d, _) = run(a, txs(false, &[("r105", 105, &t05)])).await;
    assert!(stays_diff(&d), "{d:?}");

    // 6. Two rows equal in value under different request keys (k1 ours, k2 the oracle's),
    //    both canonical, our list complete: k2 is a row ours lacks, a diff.
    let xfer = |rk: &str| {
        json!({"cursor": "c", "node": {"amount": 1.5, "senderAccount": "s",
            "receiverAccount": "r", "block": {"chainId": "0", "height": 100}, "requestKey": rk}})
    };
    let (d, _) = run(
        h7_page("transfers", false, vec![xfer("k1")]),
        h7_page("transfers", false, vec![xfer("k2")]),
    )
    .await;
    assert!(stays_diff(&d), "{d:?}");

    // H5's cases, now H7 cases. QTransfersByAccount's rows: a block hash, no height (the
    // node gives it). Transfers-825fa035's rows: a height, no block hash.
    let by_block = |rk: &str, b: &str| {
        json!({"cursor": "c", "node": {"id": format!("{b}/{rk}"), "amount": "1.5",
            "senderAccount": "s", "receiverAccount": "r", "chainId": 0, "blockHash": b,
            "requestKey": rk}})
    };
    let by_height = |rk: &str, h: u64, to: &str| {
        json!({"cursor": "c", "node": {"amount": 1.5, "senderAccount": "s",
            "receiverAccount": to, "block": {"chainId": "0", "height": h}, "requestKey": rk}})
    };
    let page = |edges: Vec<Value>| h7_page("transfers", true, edges);
    // R-P5P9-64 (I-1): with the document, our longer read (first = our rows + 50, no
    // `after`) answers the first rows of `longer`.
    let run_i1 = |a: Answer, b: Answer, longer: Vec<Value>| {
        let (node, dir) = (node.clone(), dir.path().to_path_buf());
        async move {
            let url = ours_fn_server(longer_reply("transfers", &longer)).await;
            let c = gql_ctx_at(node, &dir, &url).await;
            let (mut a, mut b) = (a, b);
            let mut vars = serde_json::Map::new();
            vars.insert("first".into(), json!(3));
            let op = h7_op("transfers");
            c.gql_settle_op(
                &c.targets["a"],
                Some(&op),
                &vars,
                &mut a,
                &mut b,
                7_000_000,
                false,
            )
            .await
        }
    };
    // The CG-3 shape: each side a different canonical subset of the boundary height; one
    // single-row /poll per (chain, request key): ours' heights (t1, t2, t3) and t4.
    // t4 sits at our boundary height: excused only because our longer read lists it after
    // our page (I-1). Without the document there is no such read: not explained.
    let cg3 = || {
        (
            page(vec![
                by_block("t1", "B1"),
                by_block("t2", "B2"),
                by_block("t3", "B2"),
            ]),
            page(vec![
                by_block("t1", "B1"),
                by_block("t2", "B2"),
                by_block("t4", "B2"),
            ]),
        )
    };
    let (a, b) = cg3();
    let (d, note) = run(a, b).await;
    assert!(stays_diff(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("no document"));
    let before = polls();
    let (a, b) = cg3();
    let longer = ["t1", "t2", "t3", "t4"]
        .iter()
        .map(|rk| by_block(rk, if *rk == "t1" { "B1" } else { "B2" }))
        .collect();
    let (d, _) = run_i1(a, b, longer).await;
    assert!(explained(&d), "{d:?}");
    assert!(hit(&d)
        .unwrap()
        .b
        .unwrap()
        .to_string()
        .contains("after-our-cut"));
    let log: Vec<String> = calls.lock().unwrap().clone();
    let polled: Vec<&String> = log.iter().filter(|c| c.starts_with("poll")).collect();
    assert_eq!(polled.len() - before, 4, "{polled:?}");
    assert!(polled.iter().all(|c| c.ends_with(" 1")), "{polled:?}");
    // R-P5P9-64 (I-1), the reviewer's shape: node order t2, t3, t4 at height 100; ours
    // [t2, t4] (t3 skipped), the oracle [t3, t2]. Our longer read lists t2, t4, then t7 at
    // 99: t3 is not after our page, a row we skipped: a diff.
    let (d, note) = run_i1(
        page(vec![by_height("t2", 100, "r"), by_height("t4", 100, "r")]),
        page(vec![by_height("t3", 100, "r"), by_height("t2", 100, "r")]),
        vec![
            by_height("t2", 100, "r"),
            by_height("t4", 100, "r"),
            by_height("t7", 99, "r"),
        ],
    )
    .await;
    assert!(stays_diff(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("a row we skipped"));
    //    Its neighbour, the whole page at one height: ours [t2, t3], the oracle [t4, t2], our
    //    longer read t2, t3, t4: t4 lies after our page: explained.
    let (d, _) = run_i1(
        page(vec![by_height("t2", 100, "r"), by_height("t3", 100, "r")]),
        page(vec![by_height("t4", 100, "r"), by_height("t2", 100, "r")]),
        vec![
            by_height("t2", 100, "r"),
            by_height("t3", 100, "r"),
            by_height("t4", 100, "r"),
        ],
    )
    .await;
    assert!(explained(&d), "{d:?}");
    //    Our page not a prefix of our own longer read (a cursor join): a diff.
    let (d, note) = run_i1(
        page(vec![by_height("t2", 100, "r"), by_height("t3", 100, "r")]),
        page(vec![by_height("t4", 100, "r"), by_height("t2", 100, "r")]),
        vec![
            by_height("t3", 100, "r"),
            by_height("t2", 100, "r"),
            by_height("t4", 100, "r"),
        ],
    )
    .await;
    assert!(stays_diff(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("not a prefix"));
    // A swap above the boundary (t1 against the canonical t6 at 101): a diff.
    let (d, _) = run(
        page(vec![
            by_block("t1", "B1"),
            by_block("t2", "B2"),
            by_block("t3", "B2"),
        ]),
        page(vec![
            by_block("t6", "B1"),
            by_block("t2", "B2"),
            by_block("t3", "B2"),
        ]),
    )
    .await;
    assert!(stays_diff(&d), "{d:?}");
    // The same row (w1) paid to another receiver on our side: a field diff.
    let (d, _) = run(
        page(vec![
            by_height("t2", 100, "r"),
            by_height("w1", 100, "k:miner-b"),
        ]),
        page(vec![
            by_height("t2", 100, "r"),
            by_height("w1", 100, "k:miner-a"),
        ]),
    )
    .await;
    assert!(stays_diff(&d), "{d:?}");
    // The oracle's w1 and w2 pay another receiver than the node's, in the node's canonical
    // block: never orphans by content (R-P5P9-63 (2)), so unknown: a diff.
    let (d, _) = run(
        page(vec![
            by_height("t1", 101, "r"),
            by_height("t2", 100, "r"),
            by_height("t3", 100, "r"),
            by_height("t7", 99, "r"),
        ]),
        page(vec![
            by_height("t1", 101, "r"),
            by_height("t2", 100, "r"),
            by_height("w1", 100, "k:miner-b"),
            by_height("w2", 100, "k:miner-b"),
        ]),
    )
    .await;
    assert!(stays_diff(&d), "{d:?}");
    // One such copy in the oracle's last slot: a diff too.
    let (d, _) = run(
        page(vec![
            by_height("t1", 101, "r"),
            by_height("t2", 100, "r"),
            by_height("t3", 100, "r"),
            by_height("t7", 99, "r"),
        ]),
        page(vec![
            by_height("t1", 101, "r"),
            by_height("t3", 100, "r"),
            by_height("t2", 100, "r"),
            by_height("w1", 100, "k:miner-b"),
        ]),
    )
    .await;
    assert!(stays_diff(&d), "{d:?}");
    // The same rows on both pages in another order, no height selected: the order rules
    // decide, no node read.
    let before = calls.lock().unwrap().len();
    let (d, _) = run(
        page(vec![
            by_block("t1", "B1"),
            by_block("t2", "B2"),
            by_block("t3", "B2"),
        ]),
        page(vec![
            by_block("t3", "B2"),
            by_block("t1", "B1"),
            by_block("t2", "B2"),
        ]),
    )
    .await;
    assert!(d.is_empty(), "{d:?}");
    assert_eq!(calls.lock().unwrap().len(), before);

    // 7. A node failure: the CG-3T shape stays a diff, with the reason in the note.
    let failing = gql_ctx(
        NodeMock {
            poll_fail: true,
            ..node
        },
        dir.path(),
    )
    .await;
    let mut a = txs(
        true,
        &[
            ("r105", 105, &t05),
            ("r104", 104, &t01),
            ("r103", 103, &t04),
        ],
    );
    let mut b = txs(
        true,
        &[
            ("r105", 105, &t05),
            ("r103", 103, &t04),
            ("r100", 100, &t03),
        ],
    );
    let vars = serde_json::Map::new();
    let (d, note) = failing
        .gql_settle(
            &failing.targets["a"],
            &vars,
            &mut a,
            &mut b,
            7_000_000,
            false,
        )
        .await;
    assert!(stays_diff(&d) && hit(&d).is_none(), "{d:?}");
    assert!(note.unwrap_or_default().contains("did not settle"));
}

/// Ours (A) as a GraphQL mock answering every request with `f(request)`.
async fn ours_fn_server<F>(f: F) -> String
where
    F: Fn(&Value) -> Value + Clone + Send + Sync + 'static,
{
    let app = Router::new().route(
        "/graphql",
        post(move |Json(req): Json<Value>| {
            let f = f.clone();
            async move { Json(f(&req)) }
        }),
    );
    format!("{}graphql", serve(app).await)
}

/// Ours answering a list read with the first `first` rows of `rows` under `root`.
fn longer_reply(root: &str, rows: &[Value]) -> impl Fn(&Value) -> Value + Clone + Send + Sync {
    let (root, rows) = (root.to_string(), rows.to_vec());
    move |req: &Value| {
        let n = req["variables"]["first"].as_u64().unwrap_or(0) as usize;
        let page: Vec<Value> = rows.iter().take(n).cloned().collect();
        json!({"data": {root.as_str(): {"pageInfo": {"hasNextPage": n < rows.len()},
            "edges": page}}})
    }
}

/// A list document over `root` with the arguments H7's filter reads.
fn h7_op(root: &str) -> crate::graphql::GqlOp {
    let q = format!(
        "query QH7($accountName: String, $first: Int, $after: String) {{ {root}(accountName: \
         $accountName, first: $first, after: $after) {{ totalCount pageInfo {{ hasNextPage \
         endCursor }} edges {{ cursor node {{ requestKey }} }} }} }}"
    );
    crate::graphql::parse_op("QH7", &q, false).unwrap()
}

/// A node `/poll` entry with one coin TRANSFER event `from -> to` of `amount`.
fn h7_xfer_poll(
    rk: &str,
    block: &str,
    h: u64,
    from: &str,
    to: &str,
    amount: Value,
) -> (String, (u8, Value)) {
    h7_poll(
        rk,
        block,
        h,
        json!([{"name": "TRANSFER", "module": {"name": "coin"}, "params": [from, to, amount]}]),
    )
}

/// A transfer row with a height and no index (Transfers-825fa035's shape), selecting the
/// amount only, or also its sender and receiver.
fn h7_xrow(rk: &str, h: u64, amount: Value, accounts: Option<(&str, &str)>) -> Value {
    let mut n = json!({"amount": amount, "block": {"chainId": "0", "height": h},
        "requestKey": rk});
    if let Some((s, r)) = accounts {
        n["senderAccount"] = json!(s);
        n["receiverAccount"] = json!(r);
    }
    json!({"cursor": "c", "node": n})
}

fn only_hits(d: &[crate::report::DiffEntry]) -> bool {
    d.iter()
        .all(|e| e.path.starts_with("/~") && e.path != crate::graphql_window::ROW_PATH)
}

fn h7_vars(pairs: &[(&str, Value)]) -> serde_json::Map<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

/// R-P5P9-63 (1): an ours-only canonical row is checked against the query's own filter
/// (here `accountName` and the height window), by its own fields or else the node's event.
/// A row the node holds that does not match the filter is a served bug: a diff.
#[tokio::test]
async fn h7_ours_only_rows_outside_the_filter_stay_diffs() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for (rk, h, to) in [
        ("e1", 100, "acct"),
        ("e2", 100, "other"),
        ("e3", 100, "acct"),
        ("e4", 99, "acct"),
    ] {
        let (k, v) = h7_xfer_poll(rk, &format!("B{h}"), h, "s", to, json!(1.5));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let row = |rk: &&str| h7_xrow(rk, if *rk == "e4" { 99 } else { 100 }, json!(1.5), None);
    let run = |a: Vec<&str>, b: Vec<&str>, vars: serde_json::Map<String, Value>| {
        let ctx = &ctx;
        let a: Vec<Value> = a.iter().map(row).collect();
        let b: Vec<Value> = b.iter().map(row).collect();
        async move {
            let mut a = h7_page("transfers", false, a);
            let mut b = h7_page("transfers", false, b);
            ctx.gql_settle(&ctx.targets["a"], &vars, &mut a, &mut b, 7_000_000, false)
                .await
        }
    };
    let acct = || h7_vars(&[("accountName", json!("acct"))]);
    // e2 pays "other": the node holds it, but not for this account.
    let (d, note) = run(vec!["e1", "e2"], vec!["e1"], acct()).await;
    assert!(!only_hits(&d), "{d:?}");
    assert!(note
        .unwrap_or_default()
        .contains("outside the query's account"));
    // e3 pays the account: explained (a node-confirmed oracle gap).
    let (d, _) = run(vec!["e1", "e3"], vec!["e1"], acct()).await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?}");
    // e4 sits below minHeight 100: a diff.
    let mut v = acct();
    v.insert("minHeight".into(), json!(100));
    let (d, note) = run(vec!["e1", "e4"], vec!["e1"], v).await;
    assert!(!only_hits(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("height window"));
}

/// R-P5P9-63 (2) with R-P5P9-64 M-2: an oracle-only row is an orphan only by block. A key
/// `/poll` does not know, at a height whose only block has another coinbase, is unknown,
/// not an orphan: a diff. A row in another block than the node's only block at its height
/// is an orphan: explained.
#[tokio::test]
async fn h7_orphans_only_by_block() {
    let dir = tempfile::tempdir().unwrap();
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
    let ctx = gql_ctx(node, dir.path()).await;
    let row = |rk: &str, block: Option<&str>| {
        let mut e = h7_xrow(rk, 100, json!(1.5), Some(("s", "r")));
        if let Some(b) = block {
            e["node"]["blockHash"] = json!(b);
        }
        e
    };
    let vars = serde_json::Map::new();
    // q: /poll answers {} for it, and the node's only block at 100 has another coinbase.
    let mut a = h7_page("transfers", false, vec![row("t2", None)]);
    let mut b = h7_page("transfers", false, vec![row("t2", None), row("q", None)]);
    let (d, note) = ctx
        .gql_settle(&ctx.targets["a"], &vars, &mut a, &mut b, 7_000_000, false)
        .await;
    assert!(!only_hits(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("did not settle"));
    // The same row in block X100 while the node's only block at 100 is B100: an orphan.
    let mut a = h7_page("transfers", false, vec![row("t2", Some("B100"))]);
    let mut b = h7_page(
        "transfers",
        false,
        vec![row("t2", Some("B100")), row("q", Some("X100"))],
    );
    let (d, _) = ctx
        .gql_settle(&ctx.targets["a"], &vars, &mut a, &mut b, 7_000_000, false)
        .await;
    assert!(only_hits(&d), "{d:?}");
    assert!(format!("{d:?}").contains("orphan"), "{d:?}");
}

/// R-P5P9-63 (3): rows without `orderIndex` or `id` within one request key compare as a
/// multiset on every selected field. The same two transfers of u in another order with an
/// ours-only canonical row are explained; a row ours lists more often than the node holds
/// it, and an oracle row the node does not hold, stay diffs.
#[tokio::test]
async fn h7_rows_without_an_index_compare_as_a_multiset() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let ev =
        |a: f64| json!({"name": "TRANSFER", "module": {"name": "coin"}, "params": ["s", "r", a]});
    let (k, v) = h7_poll("u", "B100", 100, json!([ev(1.0), ev(2.0)]));
    node.polls.insert(k, v);
    let (k, v) = h7_xfer_poll("x", "B101", 101, "s", "r", json!(1.5));
    node.polls.insert(k, v);
    let ctx = gql_ctx(node, dir.path()).await;
    let u = |a: &str| h7_xrow("u", 100, json!(a), Some(("s", "r")));
    let x = h7_xrow("x", 101, json!("1.5"), Some(("s", "r")));
    let run = |a: Vec<Value>, b: Vec<Value>| {
        let ctx = &ctx;
        async move {
            let mut a = h7_page("transfers", false, a);
            let mut b = h7_page("transfers", false, b);
            ctx.gql_settle(
                &ctx.targets["a"],
                &serde_json::Map::new(),
                &mut a,
                &mut b,
                7_000_000,
                false,
            )
            .await
        }
    };
    let (d, note) = run(
        vec![x.clone(), u("1.0"), u("2.0")],
        vec![u("2.0"), u("1.0")],
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    // Ours lists u's 1.0 twice; the node's transaction holds it once.
    let (d, note) = run(vec![u("1.0"), u("1.0")], vec![u("1.0")]).await;
    assert!(!only_hits(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("holds it 1 times"));
    // The oracle's 3.0 is not the node's: unknown, a diff.
    let (d, _) = run(vec![u("1.0"), u("2.0")], vec![u("1.0"), u("3.0")]).await;
    assert!(!only_hits(&d), "{d:?}");
}

/// R-P5P9-63 (4): at most 300 node reads per compare; a spent cap leaves the pair
/// unexplained, with the reason.
#[tokio::test]
async fn h7_per_compare_cap_is_300_node_reads() {
    use crate::graphql_window::{Reads, NODE_READS};
    assert_eq!(NODE_READS, 300);
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for (rk, h) in [("r100", 100), ("r104", 104)] {
        let (k, v) = h7_poll(rk, &format!("B{h}"), h, json!([]));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let mut st = Reads::default();
    st.node = NODE_READS - 1;
    assert!(ctx.h7_take(&mut st).is_ok());
    let err = ctx.h7_take(&mut st).unwrap_err();
    assert!(err.contains("300 node reads per compare"), "{err}");
    let t = |m: &str| format!("2024-01-01T10:{m}Z");
    let txs = |rows: &[(&str, u64, String)]| {
        h7_page(
            "transactions",
            true,
            rows.iter()
                .map(|(rk, h, ct)| h7_tx(rk, *h, &format!("B{h}"), ct))
                .collect(),
        )
    };
    let (mut a, mut b) = (
        txs(&[
            ("r105", 105, t("05:00")),
            ("r104", 104, t("01:00")),
            ("r103", 103, t("04:00")),
        ]),
        txs(&[
            ("r105", 105, t("05:00")),
            ("r103", 103, t("04:00")),
            ("r100", 100, t("03:00")),
        ]),
    );
    let vars = serde_json::Map::new();
    let (d, note) = ctx
        .gql_window_referee(
            &ctx.targets["a"],
            None,
            &vars,
            &mut a,
            &mut b,
            7_000_000,
            false,
            &mut st,
        )
        .await;
    assert!(
        d.iter()
            .all(|e| e.path != crate::graphql_window::WINDOW_PATH),
        "{d:?}"
    );
    assert!(note
        .unwrap_or_default()
        .contains("300 node reads per compare"));
}

/// R-P5P9-63 (5): a coinbase row with a block hash and no height is read by the node's
/// header-by-hash route; its block's height then gives the node's block and coinbase. An
/// ours-only coinbase row is explained; an oracle coinbase row in another block than the
/// node's at that height is an orphan; a block the node does not know stays a diff.
#[tokio::test]
async fn h7_coinbase_rows_by_block_hash_read_the_header_by_hash() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let (k, v) = h7_xfer_poll("t1", "B1", 801, "s", "r", json!(1.5));
    node.polls.insert(k, v);
    let c7 = json!({"hash": "C7", "height": 700, "payloadHash": "PC7", "chainId": 0});
    node.headers.insert((0, 700), vec![c7]);
    node.headers.insert(
        (0, 1),
        vec![json!({"hash": "O7", "height": 700, "chainId": 0})],
    );
    let cb = json!({"reqKey": "cbk", "events": [{"name": "TRANSFER", "module": {"name": "coin"},
        "params": ["", "k:miner", 2.3]}]})
    .to_string();
    node.payloads.insert(
        "PC7".into(),
        json!({"coinbase": kdx_types::hash::b64url_encode(cb.as_bytes())}),
    );
    let calls = node.calls.clone();
    let ctx = gql_ctx(node, dir.path()).await;
    let row = |rk: &str, b: &str, to: &str, amount: &str| {
        json!({"cursor": "c", "node": {"amount": amount, "senderAccount": if rk == "t1" { "s" } else { "" },
            "receiverAccount": to, "chainId": 0, "blockHash": b, "requestKey": rk}})
    };
    let t1 = row("t1", "B1", "r", "1.5");
    let run = |a: Vec<Value>, b: Vec<Value>| {
        let ctx = &ctx;
        async move {
            let mut a = h7_page("transfers", false, a);
            let mut b = h7_page("transfers", false, b);
            ctx.gql_settle(
                &ctx.targets["a"],
                &serde_json::Map::new(),
                &mut a,
                &mut b,
                7_000_000,
                false,
            )
            .await
        }
    };
    let (d, note) = run(
        vec![t1.clone(), row("cbk", "C7", "k:miner", "2.3")],
        vec![t1.clone()],
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    assert!(calls
        .lock()
        .unwrap()
        .iter()
        .any(|c| c == "header-by-hash C7"));
    // The oracle's copy in O7, which the node holds at 700 beside its only canonical C7.
    let (d, note) = run(
        vec![t1.clone()],
        vec![t1.clone(), row("cbk", "O7", "k:miner", "2.3")],
    )
    .await;
    assert!(only_hits(&d), "{d:?} {note:?}");
    assert!(format!("{d:?}").contains("orphan"), "{d:?}");
    // A block the node does not know: unknown, a diff.
    let (d, _) = run(
        vec![t1.clone(), row("cbk", "Z7", "k:miner", "2.3")],
        vec![t1],
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
}

/// R-P5P9-63 (6): H7 covers ascending lists (`blocksFromHeight`) symmetrically: our boundary
/// is our highest height, an oracle-only row above it is outside our window. A row skipped
/// inside our window and ours in descending order stay diffs.
#[tokio::test]
async fn h7_ascending_lists_are_refereed_symmetrically() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for h in 10..=13u64 {
        node.headers.insert(
            (0, h),
            vec![json!({"hash": format!("B{h}"), "height": h, "chainId": 0})],
        );
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let blk = |h: u64| json!({"cursor": "c", "node": {"hash": format!("B{h}"), "height": h, "chainId": 0}});
    let run = |a: &[u64], b: &[u64]| {
        let ctx = &ctx;
        let (a, b): (Vec<Value>, Vec<Value>) = (
            a.iter().map(|h| blk(*h)).collect(),
            b.iter().map(|h| blk(*h)).collect(),
        );
        async move {
            let mut a = h7_page("blocksFromHeight", true, a);
            let mut b = h7_page("blocksFromHeight", true, b);
            ctx.gql_settle(
                &ctx.targets["a"],
                &serde_json::Map::new(),
                &mut a,
                &mut b,
                7_000_000,
                false,
            )
            .await
        }
    };
    let (d, note) = run(&[10, 11, 12], &[10, 11, 13]).await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    assert!(format!("{d:?}").contains("above-our-window"), "{d:?}");
    let (d, note) = run(&[10, 12], &[10, 11]).await;
    assert!(!only_hits(&d), "{d:?}");
    assert!(note
        .unwrap_or_default()
        .contains("strictly inside our window"));
    let (d, note) = run(&[12, 11, 10], &[10, 11, 13]).await;
    assert!(!only_hits(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("ascending height order"));
}

/// R-P5P9-64 I-2: the content of a one-sided row is checked on every selected field the
/// node's entry holds: an event's parameters (by value), a tx's gas. An ours-only row with
/// one changed parameter, or another gas, stays a diff; the node's values are explained.
#[tokio::test]
async fn h7_content_of_one_sided_rows_is_the_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let xfer = json!([{"name": "TRANSFER", "module": {"name": "coin"}, "params": ["s", "r", 1.5]}]);
    for rk in ["e1", "e5"] {
        let (k, v) = h7_poll(rk, "B100", 100, xfer.clone());
        node.polls.insert(k, v);
    }
    for (rk, h) in [("r105", 105), ("r104", 104)] {
        let (k, mut v) = h7_poll(rk, &format!("B{h}"), h, json!([]));
        v.1["gas"] = json!(50);
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let ev = |rk: &str, params: &str| {
        json!({"cursor": "c", "node": {"name": "TRANSFER", "qualifiedName": "coin.TRANSFER",
            "orderIndex": 0, "parameters": params, "chainId": "0",
            "block": {"hash": "B100", "height": 100}, "requestKey": rk}})
    };
    let settle = |a: Vec<Value>, b: Vec<Value>, root: &'static str| {
        let ctx = &ctx;
        async move {
            let mut a = h7_page(root, false, a);
            let mut b = h7_page(root, false, b);
            ctx.gql_settle(
                &ctx.targets["a"],
                &serde_json::Map::new(),
                &mut a,
                &mut b,
                7_000_000,
                false,
            )
            .await
        }
    };
    let e1 = ev("e1", r#"["s","r",1.5]"#);
    let (d, note) = settle(
        vec![e1.clone(), ev("e5", r#"["s","r",2.5]"#)],
        vec![e1.clone()],
        "events",
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("not canonical"));
    let (d, _) = settle(
        vec![e1.clone(), ev("e5", r#"["s","r",1.50]"#)],
        vec![e1],
        "events",
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?}");
    let tx = |rk: &str, h: u64, gas: u64| {
        let mut e = h7_tx(rk, h, &format!("B{h}"), "2024-01-01T10:00:00Z");
        e["node"]["result"]["gas"] = json!(gas);
        e
    };
    let (d, _) = settle(
        vec![tx("r105", 105, 50), tx("r104", 104, 999)],
        vec![tx("r105", 105, 50)],
        "transactions",
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
    let (d, _) = settle(
        vec![tx("r105", 105, 50), tx("r104", 104, 50)],
        vec![tx("r105", 105, 50)],
        "transactions",
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?}");
}

/// R-P5P9-64 I-3: a follow page that errors fails the fetch (a-error), never a cut page; a
/// cut page of ours shorter than first × the pages fetched is not a window.
#[tokio::test]
async fn h7_failed_or_short_follow_pages_are_not_windows() {
    let dir = tempfile::tempdir().unwrap();
    let url = ours_fn_server(|req: &Value| {
        if req["variables"]["after"].is_null() {
            json!({"data": {"transfers": {"pageInfo": {"hasNextPage": true, "endCursor": "c1"},
                "edges": [{"cursor": "c1", "node": {"requestKey": "k1"}}]}}})
        } else {
            json!({"errors": [{"message": "limit", "extensions": {"code": "LIMIT"}}], "data": null})
        }
    })
    .await;
    let ctx = gql_ctx_at(NodeMock::default(), dir.path(), &url).await;
    let op = h7_op("transfers");
    let vars = h7_vars(&[("first", json!(1))]);
    let Err(err) = crate::graphql::fetch_pages(&ctx.targets["a"], &op, &vars, 3).await else {
        panic!("a follow page with errors was read as a cut page");
    };
    assert!(
        format!("{err:#}").contains("follow page 2 failed"),
        "{err:#}"
    );
    // A short cut page: the CG-3T sort-key shape, but ours fetched 2 pages of first 5.
    let mut node = NodeMock::default();
    for (rk, h) in [("r100", 100), ("r104", 104)] {
        let (k, v) = h7_poll(rk, &format!("B{h}"), h, json!([]));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let t = |m: &str| format!("2024-01-01T10:{m}Z");
    let txs = |rows: &[(&str, u64, String)]| {
        h7_page(
            "transactions",
            true,
            rows.iter()
                .map(|(rk, h, ct)| h7_tx(rk, *h, &format!("B{h}"), ct))
                .collect(),
        )
    };
    let mut a = txs(&[
        ("r105", 105, t("05:00")),
        ("r104", 104, t("01:00")),
        ("r103", 103, t("04:00")),
    ]);
    a.ms = vec![1, 1];
    let mut b = txs(&[
        ("r105", 105, t("05:00")),
        ("r103", 103, t("04:00")),
        ("r100", 100, t("03:00")),
    ]);
    let vars = h7_vars(&[("first", json!(5))]);
    let (d, note) = ctx
        .gql_settle(&ctx.targets["a"], &vars, &mut a, &mut b, 7_000_000, false)
        .await;
    assert!(!only_hits(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("short page"));
}

/// R-P5P9-64 M-3: an amount of ours must equal the node's exactly; only the oracle's may be
/// the f64-rounded form.
#[tokio::test]
async fn h7_amounts_of_ours_are_exact() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for (rk, h) in [("t2", 100), ("m1", 100), ("m2", 99)] {
        let (k, v) = h7_xfer_poll(rk, &format!("B{h}"), h, "s", "r", json!(1.5));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let f64ish = json!("1.50000000000000001");
    let run = |a: Vec<Value>, b: Vec<Value>, cut: bool| {
        let ctx = &ctx;
        async move {
            let mut a = h7_page("transfers", cut, a);
            let mut b = h7_page("transfers", cut, b);
            ctx.gql_settle(
                &ctx.targets["a"],
                &serde_json::Map::new(),
                &mut a,
                &mut b,
                7_000_000,
                false,
            )
            .await
        }
    };
    let t2 = h7_xrow("t2", 100, json!("1.5"), Some(("s", "r")));
    let (d, _) = run(
        vec![
            t2.clone(),
            h7_xrow("m1", 100, f64ish.clone(), Some(("s", "r"))),
        ],
        vec![t2.clone()],
        false,
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
    let (d, note) = run(
        vec![t2.clone()],
        vec![t2, h7_xrow("m2", 99, f64ish, Some(("s", "r")))],
        true,
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
}

/// R-P5P9-62 H8 (`Transfer.orderIndex`) on the node (R-P5P9-64 M-4): ours numbers a transfer
/// by its TRANSFER event's index in the transaction, the oracle by its rank among the
/// transaction's TRANSFER events. One node `/poll` per request key gives both: the oracle's
/// value r must be the rank of the node's TRANSFER event with the row's content, and the
/// row takes that event's index, which then checks ours. Ours 0, 1, 2, 5 against the
/// oracle's 0, 1, 2, 3 is explained; the oracle's 0, 1, 3, 2, ours numbering 4 where the
/// node says 5, a transfer ours lacks and a document with no content fields stay diffs.
#[tokio::test]
async fn h8_transfer_order_index_is_the_rank_of_ours() {
    use crate::graphql_window::ORDER_INDEX_PATH;
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let t =
        |a: f64| json!({"name": "TRANSFER", "module": {"name": "coin"}, "params": ["s", "r", a]});
    let x = json!({"name": "X", "module": {"name": "coin"}, "params": []});
    let (k, v) = h7_poll(
        "u1",
        "B9",
        9,
        json!([t(1.0), t(2.0), t(3.0), x.clone(), x, t(4.0)]),
    );
    node.polls.insert(k, v);
    let xfer = |idx: i64, amount: &str| {
        let id = crate::graphql::base64_std(
            format!("Transfer:[\"B9\",\"0\",\"{idx}\",\"mh\",\"u1\"]").as_bytes(),
        );
        json!({"id": id, "amount": amount, "creationTime": "2024-01-01T10:00:00Z",
            "orderIndex": idx, "senderAccount": "s", "receiverAccount": "r",
            "moduleName": "coin", "chainId": "0", "blockHash": "B9", "requestKey": "u1"})
    };
    let page = |rows: &[(i64, &str)]| {
        h7_page(
            "transfers",
            false,
            rows.iter()
                .map(|(i, a)| json!({"cursor": "c", "node": xfer(*i, a)}))
                .collect(),
        )
    };
    let ctx = gql_ctx(node, dir.path()).await;
    let vars = h7_vars(&[("requestKey", json!("u1"))]);
    let settle = |mut a: crate::graphql::Answer, mut b: crate::graphql::Answer| {
        let (ctx, vars) = (&ctx, &vars);
        async move {
            ctx.gql_settle(&ctx.targets["a"], vars, &mut a, &mut b, 7_000_000, true)
                .await
                .0
        }
    };
    let only = |d: &[crate::report::DiffEntry]| d.iter().all(|e| e.path.starts_with("/~"));
    let ours = [(0, "1.0"), (1, "2.0"), (2, "3.0"), (5, "4.0")];
    let d = settle(
        page(&ours),
        page(&[(0, "1.0"), (1, "2.0"), (2, "3.0"), (3, "4.0")]),
    )
    .await;
    assert!(only(&d), "{d:?}");
    let hit = d
        .iter()
        .find(|e| e.path == ORDER_INDEX_PATH)
        .expect("H8 hit");
    assert!(
        hit.b.as_ref().unwrap().to_string().contains("u1"),
        "{hit:?}"
    );
    // The oracle's 0, 1, 3, 2: its 3 sits on the transfer ranked 2: a diff.
    let d = settle(
        page(&ours),
        page(&[(0, "1.0"), (1, "2.0"), (3, "3.0"), (2, "4.0")]),
    )
    .await;
    assert!(!only(&d), "{d:?}");
    assert!(d.iter().all(|e| e.path != ORDER_INDEX_PATH), "{d:?}");
    // Ours numbers the fourth transfer 4; the node's event index is 5: a diff.
    let d = settle(
        page(&[(0, "1.0"), (1, "2.0"), (2, "3.0"), (4, "4.0")]),
        page(&[(0, "1.0"), (1, "2.0"), (2, "3.0"), (3, "4.0")]),
    )
    .await;
    assert!(!only(&d), "{d:?}");
    // A transfer ours lacks (ours 0, 1, 5; the oracle's 0, 1, 2, 3): a diff.
    let d = settle(
        page(&[(0, "1.0"), (1, "2.0"), (5, "4.0")]),
        page(&[(0, "1.0"), (1, "2.0"), (2, "3.0"), (3, "4.0")]),
    )
    .await;
    assert!(!only(&d), "{d:?}");
    // No content field selected (only the id, orderIndex and the key): no remap, a diff.
    let bare = |rows: &[i64]| {
        h7_page(
            "transfers",
            false,
            rows.iter()
                .map(|i| {
                    let mut n = xfer(*i, "0");
                    n.as_object_mut()
                        .unwrap()
                        .retain(|k, _| ["id", "orderIndex", "requestKey"].contains(&k.as_str()));
                    json!({"cursor": "c", "node": n})
                })
                .collect(),
        )
    };
    let d = settle(bare(&[0, 1, 2, 5]), bare(&[0, 1, 2, 3])).await;
    assert!(!only(&d), "{d:?}");
    assert!(d.iter().all(|e| e.path != ORDER_INDEX_PATH), "{d:?}");
}

/// R-P5P9-64 M-9: H7 paths that had no test. An oracle duplicate of a canonical row of ours
/// is explained, and stays a diff when the node rejects ours; ours listing a row twice, and
/// a row whose identity cannot be decided, stay diffs; rows above the oracle's ceiling are
/// set aside with the stall hit.
#[tokio::test]
async fn h7_duplicates_twice_ceiling_and_undecidable_rows() {
    use crate::graphql::ORACLE_STALL_PATH;
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for (rk, b, h) in [
        ("r100", "B100", 100),
        ("r103", "B103", 103),
        ("r104", "B104", 104),
        ("r105", "B105", 105),
        ("z104", "Q104", 104),
    ] {
        let (k, v) = h7_poll(rk, b, h, json!([]));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let ct = "2024-01-01T10:00:00Z";
    let tx = |rk: &str, h: u64| h7_tx(rk, h, &format!("B{h}"), ct);
    let run = |a: Vec<Value>, b: Vec<Value>, cut: bool, max: u64| {
        let ctx = &ctx;
        async move {
            let mut a = h7_page("transactions", cut, a);
            let mut b = h7_page("transactions", cut, b);
            ctx.gql_settle(
                &ctx.targets["a"],
                &serde_json::Map::new(),
                &mut a,
                &mut b,
                max,
                false,
            )
            .await
        }
    };
    let (d, _) = run(
        vec![tx("r105", 105), tx("r104", 104)],
        vec![tx("r105", 105), tx("r104", 104), tx("r104", 104)],
        false,
        7_000_000,
    )
    .await;
    assert!(
        only_hits(&d) && format!("{d:?}").contains("duplicate"),
        "{d:?}"
    );
    let (d, _) = run(
        vec![tx("r105", 105), tx("z104", 104)],
        vec![tx("r105", 105), tx("z104", 104), tx("z104", 104)],
        false,
        7_000_000,
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
    let (d, note) = run(
        vec![tx("r105", 105), tx("r104", 104), tx("r104", 104)],
        vec![tx("r105", 105), tx("r104", 104)],
        false,
        7_000_000,
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("twice"));
    // The ceiling at 104: r105 is set aside on both sides; the rest is accounted for.
    let (d, _) = run(
        vec![tx("r105", 105), tx("r104", 104), tx("r103", 103)],
        vec![tx("r105", 105), tx("r103", 103), tx("r100", 100)],
        true,
        104,
    )
    .await;
    assert!(only_hits(&d), "{d:?}");
    assert!(d.iter().any(|e| e.path == ORACLE_STALL_PATH), "{d:?}");
    // A transfer row without a request key: H7 is not applied; the pages stay a diff.
    let bad = || {
        h7_page(
            "transfers",
            false,
            vec![json!({"cursor": "c", "node": {"amount": 1}})],
        )
    };
    let ok = h7_page(
        "transfers",
        false,
        vec![h7_xrow("t2", 100, json!(1.5), None)],
    );
    let (mut a, mut b) = (bad(), ok);
    let (d, note) = ctx
        .gql_settle(
            &ctx.targets["a"],
            &serde_json::Map::new(),
            &mut a,
            &mut b,
            7_000_000,
            false,
        )
        .await;
    assert!(!d.is_empty() && !only_hits(&d), "{d:?}");
    assert!(note
        .unwrap_or_default()
        .contains("identity cannot be decided"));
}

/// R-P5P9-62 `payload.data`: ours sends the node's null `cont.data` as the text `"null"`,
/// the oracle `"{}"`. Explained only in that direction, only on a continuation, and only
/// where the node's command for the request key has `cont.data` null (single-row reads:
/// `/poll`, the header at its height, that block's payload). An exec payload, the reverse
/// direction and a node `cont.data` that is not null stay diffs.
#[tokio::test]
async fn r62_payload_data_null_only_where_the_node_cont_data_is_null() {
    use crate::graphql_window::PAYLOAD_DATA_PATH;
    use kdx_types::hash::b64url_encode;
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let cmd = |rk: &str, payload: Value| {
        let c = json!({"payload": payload, "meta": {"chainId": "0"}}).to_string();
        let w = json!({"hash": rk, "sigs": [], "cmd": c}).to_string();
        json!([b64url_encode(w.as_bytes()), b64url_encode(b"{}")])
    };
    let cont = |data: Value| json!({"cont": {"pactId": "p", "step": 1, "rollback": false, "data": data, "proof": null}});
    node.payloads.insert(
        "PP".into(),
        json!({"transactions": [
            cmd("c1", cont(Value::Null)),
            cmd("e1", json!({"exec": {"code": "(+ 1 2)", "data": null}})),
            cmd("c2", cont(json!({"x": 1}))),
        ]}),
    );
    node.headers.insert(
        (0, 50),
        vec![json!({"hash": "BP", "height": 50, "payloadHash": "PP"})],
    );
    for rk in ["c1", "e1", "c2"] {
        let (k, v) = h7_poll(rk, "BP", 50, json!([]));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let tx = |rk: &str, data: &str| crate::graphql::Answer {
        body: json!({"data": {"transaction": {"hash": rk,
            "cmd": {"meta": {"chainId": "0"}, "payload": {"data": data}}}}}),
        status: 200,
        raw: String::new(),
        ms: Vec::new(),
    };
    let vars = serde_json::Map::new();
    let settle = |mut a: crate::graphql::Answer, mut b: crate::graphql::Answer| {
        let (ctx, vars) = (&ctx, &vars);
        async move {
            ctx.gql_settle(&ctx.targets["a"], vars, &mut a, &mut b, 7_000_000, false)
                .await
                .0
        }
    };
    let only_hits = |d: &[crate::report::DiffEntry]| d.iter().all(|e| e.path.starts_with("/~"));
    // A continuation whose node cont.data is null: ours "null", the oracle's "{}".
    let d = settle(tx("c1", "null"), tx("c1", "{}")).await;
    assert!(only_hits(&d), "{d:?}");
    let hit = d.iter().find(|e| e.path == PAYLOAD_DATA_PATH).expect("hit");
    assert!(
        hit.b.as_ref().unwrap().to_string().contains("c1"),
        "{hit:?}"
    );
    // An exec payload: a diff.
    let d = settle(tx("e1", "null"), tx("e1", "{}")).await;
    assert!(!only_hits(&d), "{d:?}");
    // Ours "{}" against the oracle's "null": a diff.
    let d = settle(tx("c1", "{}"), tx("c1", "null")).await;
    assert!(!only_hits(&d), "{d:?}");
    // The node's cont.data is not null: a diff.
    let d = settle(tx("c2", "null"), tx("c2", "{}")).await;
    assert!(!only_hits(&d), "{d:?}");
}

/// R-P5P9-62 `kda-compare referee` on a stored record log and a mock node: the GraphQL
/// oracle-pair records still unexplained are re-refereed with H7, H8, H9 and `payload.data`
/// (single-row node reads under the run's cap and rate). The output counts the classes and
/// lists every record still unexplained with its evidence; a spent cap leaves a record
/// unexplained, and records of other pairs are left alone.
#[tokio::test]
async fn referee_reruns_the_node_verdicts_on_a_stored_log() {
    use crate::referee::{run, Opts};
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock {
        tip: 7_000_000,
        ..NodeMock::default()
    };
    for (rk, h) in [
        ("r100", 100),
        ("r103", 103),
        ("r104", 104),
        ("r105", 105),
        ("x104", 104),
    ] {
        // The sender k:x pays the gas: the account filter of R-P5P9-63 (1).
        let gas = json!([{"name": "TRANSFER", "params": ["k:x", "k:miner", 0.01],
            "module": {"name": "coin"}}]);
        let (k, v) = h7_poll(rk, &format!("B{h}"), h, gas);
        node.polls.insert(k, v);
    }
    let nu = node_server(node).await;
    let ops = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/graphql");
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
own_ops_dir = {ops:?}
field_rules = [{{ field = "transfers.creationTime", rule = "creation_time_half_even" }}]
[targets.a]
url = "http://127.0.0.1:9/graphql"
kind = "kda_indexer_graphql"
[targets.b]
url = "https://oracle.invalid/graphql"
kind = "graphql_oracle"
max_height = 7000000
"#
    ))
    .unwrap();
    let t = |m: &str| format!("2024-01-01T10:{m}Z");
    let (t05, t01, t04, t03, t02) = (t("05:00"), t("01:00"), t("04:00"), t("03:00"), t("02:00"));
    let txs = |rows: &[(&str, u64, &str)]| {
        h7_page(
            "transactions",
            true,
            rows.iter()
                .map(|(rk, h, ct)| h7_tx(rk, *h, &format!("B{h}"), ct))
                .collect(),
        )
        .body
        .to_string()
    };
    let xfer = |ct: &str| {
        json!({"data": {"transfers": {"pageInfo": {"hasNextPage": false}, "edges": [
            {"cursor": "c", "node": {"amount": "1.5", "creationTime": ct, "orderIndex": 0,
            "senderAccount": "s", "receiverAccount": "r", "chainId": "0", "blockHash": "B9",
            "requestKey": "u1"}}]}}})
        .to_string()
    };
    let rec = |stem: &str, vars: Value, b_target: &str, a: String, b: String, cap: bool| {
        let mut r = json!({"ts": "2026-11-14T20:00:00Z", "route": format!("graphql:{stem}"),
            "request": format!("{stem} {vars}"), "a_target": "a", "b_target": b_target,
            "a_status": 200, "b_status": 200, "a_body_trimmed": a, "b_body_trimmed": b,
            "diff": [{"path": "/data", "kind": "Changed"}], "class": "diff",
            "curl": ""});
        if cap {
            r["body_cap"] = json!("bodies cut at the 8 MiB body limit");
        }
        r["a_pages"] = json!(1);
        r.to_string()
    };
    // The same with its stored diff holding H6's entry `path` (R-P5P9-64 M-1), or with no
    // page count.
    let with = |line: String, f: &dyn Fn(&mut Value)| {
        let mut v: Value = serde_json::from_str(&line).unwrap();
        f(&mut v);
        v.to_string()
    };
    let h6 = |path: &'static str| {
        move |v: &mut Value| {
            v["diff"] = json!([{"path": path, "kind": "Changed",
                "b": {"lists": ["/data/block/events"]}}]);
        }
    };
    let acct = json!({"accountName": "k:x", "first": 3});
    let lines = [
        // The CG-3T sort-key shape: explained by H7 (2 node reads).
        rec(
            "QTransactionsByAccount",
            acct.clone(),
            "b",
            txs(&[
                ("r105", 105, &t05),
                ("r104", 104, &t01),
                ("r103", 103, &t04),
            ]),
            txs(&[
                ("r105", 105, &t05),
                ("r103", 103, &t04),
                ("r100", 100, &t03),
            ]),
            false,
        ),
        // The page-join shape: still unexplained, with the row.
        rec(
            "QTransactionsByAccount",
            acct.clone(),
            "b",
            txs(&[
                ("r105", 105, &t05),
                ("r104", 104, &t01),
                ("r103", 103, &t04),
            ]),
            txs(&[
                ("r105", 105, &t05),
                ("x104", 104, &t02),
                ("r104", 104, &t01),
            ]),
            false,
        ),
        // A creationTime fraction: explained by H9, no node read.
        rec(
            "QTransfersByRequestKey",
            json!({"requestKey": "u1"}),
            "b",
            xfer("2024-01-01T10:00:15.000Z"),
            xfer("2024-01-01T10:00:15.290Z"),
            false,
        ),
        // A body cut by the log cap: not refereed.
        rec(
            "QTransactionsByAccount",
            acct.clone(),
            "b",
            "{".into(),
            "{".into(),
            true,
        ),
        // R-P5P9-64 I-4: another pair's record (B the node, not an oracle here) and a B
        // target this config does not know: listed, not refereed.
        rec("QBlock", json!({}), "node", "{}".into(), "{}".into(), false),
        rec("QBlock", json!({}), "c", "{}".into(), "{}".into(), false),
        //    A line cut part way (a crash during an append, a full disk): listed with its line.
        r#"{"ts": "2026-11-14T20:00:00Z", "route": "graphql:QTransac"#.to_string(),
        // R-P5P9-64 M-1: H9's record with a list the live run did not finish (the oracle's
        // cursor) or left for its budget: H6's entries keep it unexplained, by class.
        with(
            rec(
                "QTransfersByRequestKey",
                json!({"requestKey": "u1"}),
                "b",
                xfer("2024-01-01T10:00:15.000Z"),
                xfer("2024-01-01T10:00:15.290Z"),
                false,
            ),
            &h6(crate::graphql_scope::UNFINISHED_PATH),
        ),
        with(
            rec(
                "QTransfersByRequestKey",
                json!({"requestKey": "u1"}),
                "b",
                xfer("2024-01-01T10:00:15.000Z"),
                xfer("2024-01-01T10:00:15.290Z"),
                false,
            ),
            &h6(crate::graphql_scope::HARNESS_LIMIT_PATH),
        ),
        //    A record with no page count of ours (H7's short-page check needs it).
        with(
            rec(
                "QTransactionsByAccount",
                acct.clone(),
                "b",
                txs(&[("r105", 105, &t05)]),
                txs(&[("r103", 103, &t04)]),
                false,
            ),
            &|v| {
                v.as_object_mut().unwrap().remove("a_pages");
            },
        ),
    ];
    let log = out.join("diffs-2026-11-14.jsonl");
    std::fs::write(&log, lines.join("\n") + "\n").unwrap();
    let opts = Opts {
        logs: vec![log.clone()],
        max_reads: 6_000,
        rps: 10,
    };
    let rep = run(cfg.clone(), &opts).await.unwrap();
    assert_eq!(
        (
            rep.records,
            rep.refereed,
            rep.explained,
            rep.unexplained,
            rep.not_refereed
        ),
        (10, 9, 2, 3, 5),
        "{rep:?}"
    );
    assert!(!rep.passed());
    let class = |rep: &crate::referee::Report, c: &str| rep.by_class.get(c).copied();
    assert_eq!(
        class(&rep, "not refereed: B is not an oracle in this config"),
        Some(2)
    );
    assert_eq!(class(&rep, "not refereed: not a diff record"), Some(1));
    assert_eq!(
        class(&rep, "unexplained: h6-unfinished"),
        Some(1),
        "{rep:?}"
    );
    assert_eq!(
        class(&rep, "unexplained: harness-limit"),
        Some(1),
        "{rep:?}"
    );
    let listed = |rep: &crate::referee::Report, at: &str| {
        rep.still_unexplained
            .iter()
            .any(|v| v["at"] == json!(format!("{}:{at}", log.display())))
    };
    for at in ["5", "6", "7", "10"] {
        assert!(listed(&rep, at), "line {at}: {:?}", rep.still_unexplained);
    }
    assert_eq!(
        rep.by_class.get("graphql-h7-window-referee"),
        Some(&1),
        "{rep:?}"
    );
    assert_eq!(
        rep.by_class.get("graphql-h9-creation-time-fraction"),
        Some(&1),
        "{rep:?}"
    );
    assert_eq!(rep.by_class.get("unexplained: h7-row"), Some(&1), "{rep:?}");
    // The node's chain graph, then two reads per H7 record.
    assert_eq!(rep.node_reads, 5, "{rep:?}");
    let still = serde_json::to_string(&rep.still_unexplained).unwrap();
    assert!(
        still.contains("x104") && still.contains("inside our window"),
        "{still}"
    );
    // R-P5P9-64 M-8: a cap of two node reads (the chain graph and one): the sort-key record
    // cannot be settled and says so; every record after it is unread, never passed, the H9
    // record (no node read) among them.
    let rep = run(
        cfg,
        &Opts {
            max_reads: 2,
            ..opts
        },
    )
    .await
    .unwrap();
    assert_eq!(
        (rep.explained, rep.unexplained, rep.not_refereed),
        (0, 1, 9),
        "{rep:?}"
    );
    assert_eq!(class(&rep, crate::referee::UNREAD), Some(6), "{rep:?}");
    assert_eq!(rep.node_reads, 2, "{rep:?}");
    assert!(!rep.passed());
    let still = serde_json::to_string(&rep.still_unexplained).unwrap();
    assert!(still.contains("node read cap"), "{still}");
}

/// R-P5P9-61 H6 (ruling (3)): the nested `transactions` of large blocks (BlocksByHeight: no
/// key selected, only `gasPrice`) are paged to the end on both sides within the page budget
/// and compared as one multiset per block with its totalCount. The 147-tx shape (equal sets,
/// different page subsets, five cut blocks in one answer) is explained; one changed field, a
/// list one row short (unequal lengths), a page that fails part way, a key twice on one side
/// and a spent budget each stay a diff, the last marked as a harness limit.
#[tokio::test]
async fn h6_large_block_lists_are_paged_to_the_end_within_the_budget() {
    use crate::graphql::{compare_answers_with, parse_op};
    use crate::graphql_scope::{HARNESS_LIMIT_PATH, MAX_PAGES_PATH, UNFINISHED_PATH};
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let gp = |i: usize| json!({"cmd": {"meta": {"gasPrice": if i.is_multiple_of(3) { 1e-6 } else { 1e-7 }}}});
    let block =
        |n: usize| -> Vec<Value> { (0..n).map(|i| json!([format!("k{i}"), gp(i)])).collect() };
    let big = block(147);
    let mut changed = big.clone();
    changed[50] = json!(["k50", {"cmd": {"meta": {"gasPrice": 2e-7}}}]);
    // The oracle's paging repeats k3 where k6 belongs: the same value, so only the key shows.
    let mut twice = big.clone();
    twice[6] = big[3].clone();
    let mut short = big.clone();
    short.pop();
    let huge = block(1001);
    let ours = json!({"CVQ6": big, "B1": block(30), "B2": block(30), "B3": block(30),
        "B4": block(30), "CH": big, "FAIL": big, "TW": big, "SH": short, "CUR": big,
        "DEAD": big, "HUGE": huge, "REP": big});
    let oracle = json!({"CVQ6": big, "B1": block(30), "B2": block(30), "B3": block(30),
        "B4": block(30), "CH": changed, "FAIL": big, "TW": twice, "SH": big, "CUR": big,
        "DEAD": big, "HUGE": huge, "REP": big});
    let calls = Arc::new(Mutex::new(0));
    let au = keyed_list_server(ours.clone(), calls.clone(), 50, Some(("FAIL", 1)), false).await;
    // The oracle as CG-3T found it: its `Block.events` cursor fails past the first page.
    let bu = keyed_list_server(oracle.clone(), calls.clone(), 50, None, true).await;
    let allow =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
    let out = dir.path();
    let cfg = |budget: usize| -> CompareConfig {
        toml::from_str(&format!(
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
list_page_budget = {budget}
[targets.a]
url = "{au}"
kind = "kda_indexer_graphql"
rate_rps = 1000.0
[targets.b]
url = "{bu}"
kind = "kda_indexer_graphql"
rate_rps = 1000.0
"#
        ))
        .unwrap()
    };
    let ctx = Ctx::new(cfg(40)).await.unwrap();
    let small = Ctx::new(cfg(2)).await.unwrap();
    let op = parse_op(
        "BlocksByHeight",
        "query BlocksByHeight($startHeight: Int!, $endHeight: Int) { blocksFromHeight(startHeight: $startHeight, endHeight: $endHeight) { edges { node { chainId creationTime transactions { totalCount edges { node { cmd { meta { gasPrice } } } } } height coinbase hash canonical } } } }",
        false,
    )
    .unwrap();
    // One answer: per block its hash, the totalCount it claims and the rows of its page
    // (ours the first 20, the oracle's the last 20 in reverse).
    fn answer(src: &Value, blocks: &[(&str, usize)], head: bool) -> crate::graphql::Answer {
        let edges: Vec<Value> = blocks
            .iter()
            .map(|(h, total)| {
                let rows = src[*h].as_array().unwrap();
                let idx: Vec<usize> = if head {
                    (0..20).collect()
                } else {
                    (rows.len() - 20..rows.len()).rev().collect()
                };
                let tx: Vec<Value> = idx.iter().map(|i| json!({"node": rows[*i][1]})).collect();
                json!({"node": {"chainId": 0, "hash": h, "height": 4968386,
                    "transactions": {"totalCount": total, "edges": tx}}})
            })
            .collect();
        crate::graphql::Answer {
            body: json!({"data": {"blocksFromHeight": {"edges": edges}}}),
            status: 200,
            raw: String::new(),
            ms: Vec::new(),
        }
    }
    async fn run(
        ctx: &Ctx,
        op: &crate::graphql::GqlOp,
        src: (&Value, &Value),
        blocks: &[(&str, usize)],
    ) -> (usize, Vec<String>, Vec<String>) {
        run_with(ctx, op, src, blocks, false).await
    }
    // `same`: the oracle's page is ours' page (equal cut pages).
    async fn run_with(
        ctx: &Ctx,
        op: &crate::graphql::GqlOp,
        src: (&Value, &Value),
        blocks: &[(&str, usize)],
        same: bool,
    ) -> (usize, Vec<String>, Vec<String>) {
        let (ta, tb) = (&ctx.targets["a"], &ctx.targets["b"]);
        let (mut a, mut b) = (answer(src.0, blocks, true), answer(src.1, blocks, same));
        let (n, entries) = ctx
            .gql_finish_lists(op, &serde_json::Map::new(), ta, tb, &mut a, &mut b)
            .await;
        let d = compare_answers_with(&mut a, &mut b, u64::MAX, 10_000, &[], false);
        (
            n,
            entries.into_iter().map(|e| e.path).collect(),
            d.into_iter().map(|e| e.path).collect(),
        )
    }
    let src = (&ours, &oracle);
    // The 147-tx shape in five cut blocks: every list finished, no diff.
    let five = [
        ("CVQ6", 147),
        ("B1", 30),
        ("B2", 30),
        ("B3", 30),
        ("B4", 30),
    ];
    let (n, limit, d) = run(&ctx, &op, src, &five).await;
    assert_eq!((n, limit, d.clone()), (5, vec![], vec![]), "{d:?}");
    // One changed field in one nested row: finished, and the row stays a diff.
    let (n, _, d) = run(&ctx, &op, src, &[("CH", 147)]).await;
    assert_eq!(n, 1);
    assert!(
        d.iter()
            .any(|p| p.starts_with("/data/blocksFromHeight/edges/0/node/transactions/edges")),
        "{d:?}"
    );
    // Ours' full list one row short of its totalCount: finished, the missing row a diff.
    let (n, _, d) = run(&ctx, &op, src, &[("SH", 147)]).await;
    assert_eq!(n, 1);
    assert!(
        d.iter()
            .any(|p| p.starts_with("/data/blocksFromHeight/edges/0/node/transactions/edges")),
        "{d:?}"
    );
    // R-P5P9-64 I-5: the oracle (CUR) fails every cursor page, as its `Block.events` does:
    // it is read again in one page of its totalCount, with the key checks, and finished.
    let (n, limit, d) = run(&ctx, &op, src, &[("CUR", 147)]).await;
    assert_eq!((n, limit, d.clone()), (1, vec![], vec![]), "CUR: {d:?}");
    // R-P5P9-67 I-D: the retry is the oracle's only. Ours' page 2 answering 500 (FAIL), or
    // repeating a key (REP), leaves the list unfinished: a cursor-page defect of ours.
    for h in ["FAIL", "REP"] {
        let (n, limit, d) = run(&ctx, &op, src, &[(h, 147)]).await;
        assert_eq!((n, limit), (0, vec![UNFINISHED_PATH.to_string()]), "{h}");
        assert!(
            d.iter()
                .any(|p| p.starts_with("/data/blocksFromHeight/edges/0/node/transactions")),
            "{h}: {d:?}"
        );
    }
    // The oracle cannot be paged to its end (DEAD: the retry fails too), or its paging gives
    // k3 twice (in place of k6, the same value): not finished, the cut pages stay a diff, and
    // the record says why (no allowlist entry matches it).
    for h in ["DEAD", "TW"] {
        let (n, limit, d) = run(&ctx, &op, src, &[(h, 147)]).await;
        assert_eq!((n, limit), (0, vec![UNFINISHED_PATH.to_string()]), "{h}");
        assert!(
            d.iter()
                .any(|p| p.starts_with("/data/blocksFromHeight/edges/0/node/transactions")),
            "{h}: {d:?}"
        );
    }
    // Equal cut pages are no match without the full lists: the entry stays.
    let (n, limit, d) = run_with(&ctx, &op, src, &[("DEAD", 147)], true).await;
    assert_eq!(
        (n, limit, d),
        (0, vec![UNFINISHED_PATH.to_string()], vec![])
    );
    // R-P5P9-64 M-5: past MAX_PAGES (1,001 rows in pages of 50): a harness limit, marked.
    let (n, limit, _) = run(&ctx, &op, src, &[("HUGE", 1001)]).await;
    assert_eq!((n, limit), (0, vec![MAX_PAGES_PATH.to_string()]));
    // A budget of 2 pages a side: the 3-page list is not read to its end; a diff marked as a
    // harness limit.
    let (n, limit, d) = run(&small, &op, src, &[("CVQ6", 147)]).await;
    assert_eq!((n, limit), (0, vec![HARNESS_LIMIT_PATH.to_string()]));
    assert!(!d.is_empty());
}

/// fv-checklist T-item (R3E-8, R-P5P9-48/49): `/txs/search?search=` rows within one height
/// compare as a set against chainweb-data (its query leaves the order inside a height to
/// Postgres), with the visible hit `/~search-tie-order`; across heights the order still
/// counts, and a row listed twice stays a diff.
#[tokio::test]
async fn search_ties_within_a_height_compare_as_a_set() {
    let dir = tempfile::tempdir().unwrap();
    let node = NodeMock {
        tip: 7_000_000,
        ..NodeMock::default()
    };
    let rows = |order: &[(&str, u64)]| {
        Value::Array(
            order
                .iter()
                .map(|(rk, h)| row(rk, 0, *h, &format!("B{h}")))
                .collect(),
        )
    };
    let ours = SideMock {
        search: rows(&[("x", 10), ("y", 10), ("w", 10), ("z", 9)]),
        ..SideMock::default()
    };
    let path = "/txs/search?search=coin.transfer&limit=10";
    let case = |b: &[(&str, u64)]| {
        let b = SideMock {
            search: rows(b),
            ..SideMock::default()
        };
        let (node, ours, out) = (node.clone(), ours.clone(), dir.path().to_path_buf());
        async move {
            let (ctx, _) = setup(node, ours, b, &out).await;
            compare(&ctx, path).await
        }
    };
    // The same rows of height 10 in another order: explained, with the visible hit.
    let (class, ids, paths) = case(&[("w", 10), ("x", 10), ("y", 10), ("z", 9)]).await;
    assert_eq!(class, "allowlisted", "{paths:?}");
    assert!(ids.contains(&"search-tie-order".to_string()), "{ids:?}");
    // The same rows with heights in another order: a diff.
    let (class, _, paths) = case(&[("z", 9), ("x", 10), ("y", 10), ("w", 10)]).await;
    assert_eq!(class, "diff");
    assert!(paths.iter().any(|p| !p.starts_with("/~")), "{paths:?}");
    // A row listed twice inside the height (in place of w): a diff.
    let (class, _, paths) = case(&[("y", 10), ("x", 10), ("x", 10), ("z", 9)]).await;
    assert_eq!(class, "diff");
    assert!(paths.iter().any(|p| !p.starts_with("/~")), "{paths:?}");
}

/// R-P5P9-65 (1): the referee runs the live compare's rules with the document, so I-1's
/// longer read of ours runs there too. An oracle-only row at our boundary height that our
/// longer read lists after our page is explained, and a row we skipped stays a diff, in the
/// live run and in the referee alike.
#[tokio::test]
async fn boundary_rows_settle_alike_in_the_live_run_and_the_referee() {
    use crate::graphql::{Answer, H6};
    use crate::normalize::RouteKind;
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock {
        tip: 7_000_000,
        ..NodeMock::default()
    };
    for (rk, b, h) in [("t2", "B2", 100), ("t3", "B2", 100), ("t4", "B2", 100)] {
        let (k, v) = h7_xfer_poll(rk, b, h, "s", "r", json!(1.5));
        node.polls.insert(k, v);
    }
    let nu = node_server(node).await;
    let row = |rk: &str, h: u64| {
        json!({"cursor": "c", "node": {"amount": 1.5, "senderAccount": "s",
            "receiverAccount": "r", "block": {"chainId": "0", "height": h}, "requestKey": rk}})
    };
    // Ours: the longer read (first = 2 + 50) by account: "s" lists t2, t3, t4 (t4 after our
    // page), "r" lists t2, t4, t7 (t3, the oracle's, absent: a row we skipped).
    let (excused, skipped) = (
        vec![row("t2", 100), row("t3", 100), row("t4", 100)],
        vec![row("t2", 100), row("t4", 100), row("t7", 99)],
    );
    let au = ours_fn_server(move |req: &Value| {
        let rows = if req["variables"]["accountName"] == json!("s") {
            &excused
        } else {
            &skipped
        };
        longer_reply("transfers", rows)(req)
    })
    .await;
    let op = h7_op("transfers");
    let ops = dir.path().join("ops");
    std::fs::create_dir_all(&ops).unwrap();
    std::fs::write(
        ops.join("QH7.graphql"),
        format!("# kdx: oracle\n{}\n", op.query),
    )
    .unwrap();
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
own_ops_dir = {ops:?}
[targets.a]
url = "{au}"
kind = "kda_indexer_graphql"
[targets.b]
url = "https://oracle.invalid/graphql"
kind = "graphql_oracle"
max_height = 7000000
"#
    ))
    .unwrap();
    let ctx = Ctx::new(cfg.clone()).await.unwrap();
    let page = |edges: Vec<Value>| Answer {
        ms: vec![0],
        ..h7_page("transfers", true, edges)
    };
    let cases = [
        // Ours [t2, t3], the oracle [t4, t2]: t4 lies after our page.
        (
            "s",
            vec![row("t2", 100), row("t3", 100)],
            vec![row("t4", 100), row("t2", 100)],
            true,
        ),
        // Ours [t2, t4] (t3 skipped), the oracle [t3, t2].
        (
            "r",
            vec![row("t2", 100), row("t4", 100)],
            vec![row("t3", 100), row("t2", 100)],
            false,
        ),
    ];
    for (acct, ours, oracle, ok) in cases {
        let vars = h7_vars(&[("accountName", json!(acct)), ("first", json!(2))]);
        let (mut a, mut b) = (page(ours), page(oracle));
        let (a_body, b_body) = (a.body.to_string(), b.body.to_string());
        // The live run.
        let (d, note) = ctx
            .gql_rules(
                &ctx.targets["a"],
                H6::Live(&ctx.targets["b"]),
                &op,
                &vars,
                &mut a,
                &mut b,
                7_000_000,
            )
            .await;
        let (un, _) = ctx
            .allow
            .classify_request(&RouteKind::Other, "graphql:QH7", &[], &d);
        assert_eq!(un.is_empty(), ok, "live {acct}: {un:?} {note:?}");
        // The referee, on the record the run would log.
        let rec = json!({"ts": "2026-11-14T20:00:00Z", "route": "graphql:QH7",
            "request": format!("QH7 {}", Value::Object(vars.clone())), "a_target": "a",
            "b_target": "b", "a_status": 200, "b_status": 200, "a_body_trimmed": a_body,
            "b_body_trimmed": b_body, "diff": [], "class": "diff", "curl": "", "a_pages": 1});
        let log = out.join(format!("diffs-{acct}.jsonl"));
        std::fs::write(&log, format!("{rec}\n")).unwrap();
        let opts = crate::referee::Opts {
            logs: vec![log],
            max_reads: 6_000,
            rps: 10,
        };
        let rep = crate::referee::run(cfg.clone(), &opts).await.unwrap();
        assert_eq!(
            (rep.explained, rep.unexplained, rep.not_refereed),
            (usize::from(ok), usize::from(!ok), 0),
            "referee {acct}: {rep:?}"
        );
        if !ok {
            let still = serde_json::to_string(&rep.still_unexplained).unwrap();
            assert!(still.contains("a row we skipped"), "{still}");
        }
    }
}

/// R-P5P9-65 (2): H7's header-by-hash reads (their own client, 1 r/s, one in flight) and the
/// node client (at `node_max_rps` − 1) together stay within `node_max_rps` on the node.
#[tokio::test]
async fn header_reads_and_the_node_client_stay_within_node_max_rps() {
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let out = dir.path();
    let allow =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
    let cfg: CompareConfig = toml::from_str(&format!(
        r#"
network = "mainnet01"
pairs = [["a", "b"]]
out_dir = {out:?}
node_max_rps = 4
allowlist = {allow:?}
[node]
url = "{nu}"
timeout_ms = 5000
[graphql]
own_ops_dir = "no-such-dir"
[targets.a]
url = "http://127.0.0.1:9/graphql"
kind = "kda_indexer_graphql"
[targets.b]
url = "https://oracle.invalid/graphql"
kind = "graphql_oracle"
"#
    ))
    .unwrap();
    assert_eq!(cfg.node_client_rps(), 3);
    let ctx = Ctx::new(cfg).await.unwrap();
    let ctx = &ctx;
    let t0 = tokio::time::Instant::now();
    let until = t0 + Duration::from_secs(2);
    // Reads completed inside the two seconds, each side as fast as its limiter allows.
    let node = async move {
        let mut n = 0;
        while tokio::time::Instant::now() < until {
            let ok = ctx.node.cut().await.is_ok();
            n += usize::from(ok && tokio::time::Instant::now() < until);
        }
        n
    };
    let header = async move {
        let mut n = 0;
        for i in 0.. {
            if tokio::time::Instant::now() >= until {
                break;
            }
            let mut st = crate::graphql_window::Reads::default();
            let ok = ctx
                .h7_block_height(0, &format!("H{i}"), &mut st)
                .await
                .is_ok();
            n += usize::from(ok && tokio::time::Instant::now() < until);
        }
        n
    };
    let (n, h) = tokio::join!(node, header);
    assert!(n >= 1 && h >= 1, "{n} {h}");
    // 4 r/s for 2 s, plus one: the limiters' first reads are not delayed.
    assert!(n + h <= 4 * 2 + 1, "node {n} + header {h} reads in 2 s");
}

// ---------------------------------------------------------------------------------------------
// FVH-C3 (R-P5P9-67): the re-review's findings I-A to I-D and m-1 to m-6.

/// A config with ours at `au`, the oracle at `bu`, the documents of `ops`, and `extra` lines
/// at the top level.
fn c3_cfg(
    nu: &str,
    au: &str,
    bu: &str,
    out: &std::path::Path,
    ops: &std::path::Path,
    extra: &str,
) -> CompareConfig {
    let allow =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
    toml::from_str(&format!(
        r#"
network = "mainnet01"
pairs = [["a", "b"]]
allowlist = {allow:?}
out_dir = {out:?}
{extra}
[node]
url = "{nu}"
timeout_ms = 5000
[graphql]
own_ops_dir = {ops:?}
[targets.a]
url = "{au}"
kind = "kda_indexer_graphql"
rate_rps = 1000.0
[targets.b]
url = "{bu}"
kind = "graphql_oracle"
max_height = 7000000
rate_rps = 1000.0
"#
    ))
    .unwrap()
}

/// The paths of `d` no allowlist entry explains.
fn c3_unexplained(ctx: &Ctx, d: &[crate::report::DiffEntry]) -> Vec<String> {
    let (un, _) =
        ctx.allow
            .classify_request(&crate::normalize::RouteKind::Other, "graphql:QH7", &[], d);
    un.into_iter().map(|e| e.path).collect()
}

/// An ops dir holding QH7 over `transfers`.
fn c3_ops(dir: &std::path::Path) -> std::path::PathBuf {
    let ops = dir.join("ops");
    std::fs::create_dir_all(&ops).unwrap();
    std::fs::write(
        ops.join("QH7.graphql"),
        format!("# kdx: oracle\n{}\n", h7_op("transfers").query),
    )
    .unwrap();
    ops
}

/// R-P5P9-67 I-A (a): a cut page of ours shorter than `first` × the pages fetched is a diff
/// (`/~graphql-h7-short-page`), never the allowlisted common prefix. The one exception is
/// ours' Q10 cap, a page of exactly min(first, 100) × pages, which H7 takes as a window.
#[tokio::test]
async fn c3_a_short_page_of_ours_is_not_explained() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let (k, v) = h7_poll("x", "B10", 10, json!([]));
    node.polls.insert(k, v);
    let ctx = gql_ctx(node, dir.path()).await;
    let t = |i: u64| {
        h7_tx(
            &format!("t{i}"),
            1000 - i,
            &format!("B{}", 1000 - i),
            "2024-01-01T10:00:00Z",
        )
    };
    let page = |rows: Vec<Value>, pages: usize| crate::graphql::Answer {
        ms: vec![1; pages],
        ..h7_page("transactions", true, rows)
    };
    // The review's scenario: first 5, two pages. Ours ends with 8 rows (a cursor join lost
    // t9 and t10); the oracle lists t1 to t10. The prefix is equal.
    let mut a = page((1..=8).map(t).collect(), 2);
    let mut b = page((1..=10).map(t).collect(), 2);
    let vars = h7_vars(&[("first", json!(5))]);
    let (d, note) = ctx
        .gql_settle(&ctx.targets["a"], &vars, &mut a, &mut b, 7_000_000, false)
        .await;
    let un = c3_unexplained(&ctx, &d);
    assert!(
        un.iter().any(|p| p == "/~graphql-h7-short-page"),
        "{un:?} {note:?}"
    );
    // Ours' cap: first 150, one page of 100 rows. The oracle's 101st row lies below our
    // window: explained by H7.
    let mut a = page((1..=100).map(t).collect(), 1);
    let mut rows: Vec<Value> = (1..=100).map(t).collect();
    rows.push(h7_tx("x", 10, "B10", "2024-01-01T09:00:00Z"));
    let mut b = page(rows, 1);
    let vars = h7_vars(&[("first", json!(150))]);
    let (d, note) = ctx
        .gql_settle(&ctx.targets["a"], &vars, &mut a, &mut b, 7_000_000, false)
        .await;
    assert!(c3_unexplained(&ctx, &d).is_empty(), "{d:?} {note:?}");
    assert!(
        d.iter()
            .any(|e| e.path == crate::graphql_window::WINDOW_PATH),
        "{d:?}"
    );
}

/// R-P5P9-67 I-A (b): a follow page of ours that answers HTTP 200 with GraphQL `errors`,
/// no `data` or no root connection is class `diff` with `/~graphql-follow-page-fault`; a
/// 503 stays `a-error`.
#[tokio::test]
async fn c3_a_follow_page_fault_of_ours_is_a_diff_and_a_5xx_an_error() {
    use axum::http::StatusCode;
    let dir = tempfile::tempdir().unwrap();
    let app = Router::new().route(
        "/graphql",
        post(|Json(req): Json<Value>| async move {
            let v = &req["variables"];
            if v["after"].is_null() {
                return Json(json!({"data": {"transfers": {
                    "pageInfo": {"hasNextPage": true, "endCursor": "c1"},
                    "edges": [{"cursor": "c1", "node": {"requestKey": "k1"}}]}}}))
                .into_response();
            }
            match v["accountName"].as_str().unwrap_or("") {
                "errors" => {
                    Json(json!({"errors": [{"message": "cursor"}], "data": null})).into_response()
                }
                "no-data" => Json(json!({})).into_response(),
                "no-root" => Json(json!({"data": {"other": 1}})).into_response(),
                _ => (StatusCode::SERVICE_UNAVAILABLE, "busy").into_response(),
            }
        }),
    );
    let au = format!("{}graphql", serve(app).await);
    let ctx = gql_ctx_at(NodeMock::default(), dir.path(), &au).await;
    let op = h7_op("transfers");
    for (mode, class) in [
        ("errors", "diff"),
        ("no-data", "diff"),
        ("no-root", "diff"),
        ("busy", "a-error"),
    ] {
        let vars = h7_vars(&[("accountName", json!(mode)), ("first", json!(1))]);
        let o = ctx
            .gql_vs_oracle(&ctx.targets["a"], &ctx.targets["b"], &op, &vars, 7_000_000)
            .await;
        assert_eq!(o.class, class, "{mode}: {o:?}");
        let rec = o.record.unwrap();
        let fault = rec
            .diff
            .iter()
            .any(|e| e.path == "/~graphql-follow-page-fault");
        assert_eq!(fault, class == "diff", "{mode}: {rec:?}");
        if fault {
            let (un, _) = ctx.allow.classify_request(
                &crate::normalize::RouteKind::Other,
                "graphql:QH7",
                &[],
                &rec.diff,
            );
            assert_eq!(un.len(), 1, "{mode}");
        }
    }
}

/// R-P5P9-67 I-B: an ours-only block row is checked on every selected field against the
/// node's canonical header, in ours' encodings. One changed field stays a diff; a field the
/// header cannot give (`powHash` of an undecodable header) leaves the row unsettled.
#[tokio::test]
async fn c3_ours_only_block_rows_are_checked_on_every_selected_field() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let ones = kdx_types::hash::b64url_encode(&[0xffu8; 32]);
    let mut other = [0xffu8; 32];
    other[0] = 0xfe;
    let other = kdx_types::hash::b64url_encode(&other);
    // 2024-01-01T10:00:00.123456Z in microseconds.
    let ct: u64 = 1_704_103_200_123_456;
    for h in 10..=12u64 {
        node.headers.insert(
            (0, h),
            vec![
                json!({"hash": format!("B{h}"), "height": h, "chainId": 0, "nonce": "777",
                "creationTime": ct, "epochStart": ct - 1_000_000, "target": ones,
                "weight": ones, "payloadHash": format!("P{h}"),
                "parent": format!("B{}", h - 1)}),
            ],
        );
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let blk = |h: u64| {
        json!({"cursor": "c", "node": {"hash": format!("B{h}"), "height": h, "chainId": 0,
            "nonce": "777", "creationTime": "2024-01-01T10:00:00.123Z",
            "epoch": "2024-01-01T09:59:59.123Z", "target": ones, "weight": ones,
            "difficulty": 1, "payloadHash": format!("P{h}"),
            "parent": {"hash": format!("B{}", h - 1)}}})
    };
    let run = |last: Value| {
        let ctx = &ctx;
        let (a, b) = (vec![blk(10), blk(11), last], vec![blk(10), blk(11)]);
        async move {
            let mut a = h7_page("blocksFromHeight", false, a);
            let mut b = h7_page("blocksFromHeight", false, b);
            let (d, note) = ctx
                .gql_settle(
                    &ctx.targets["a"],
                    &serde_json::Map::new(),
                    &mut a,
                    &mut b,
                    7_000_000,
                    false,
                )
                .await;
            (c3_unexplained(ctx, &d), note.unwrap_or_default())
        }
    };
    // Every field the node's: a node-confirmed oracle gap.
    let (un, note) = run(blk(12)).await;
    assert!(un.is_empty(), "{un:?} {note}");
    for (field, value) in [
        ("nonce", json!("778")),
        ("creationTime", json!("2024-01-01T10:00:00.124Z")),
        ("epoch", json!("2024-01-01T09:59:58.123Z")),
        ("target", json!(other)),
        ("weight", json!(other)),
        ("difficulty", json!(2)),
        ("payloadHash", json!("PX")),
        ("parent", json!({"hash": "BX"})),
    ] {
        let mut row = blk(12);
        row["node"][field] = value;
        let (un, note) = run(row).await;
        assert!(!un.is_empty(), "{field}: {note}");
        assert!(note.contains(field), "{field}: {note}");
    }
    let mut row = blk(12);
    row["node"]["powHash"] = json!("00ab");
    let (un, note) = run(row).await;
    assert!(!un.is_empty(), "powHash: {note}");
    assert!(note.contains("powHash"), "{note}");
}

/// R-P5P9-67 I-C: an ours-only row is excused only when the harness checked every root
/// argument with a non-null value or a literal (`first`, `after`, `last`, `before` aside).
#[tokio::test]
async fn c3_root_filters_the_harness_does_not_check_refuse_the_excuse() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for (rk, h) in [("t1", 101), ("t2", 100)] {
        let (k, v) = h7_poll(rk, &format!("B{h}"), h, json!([]));
        node.polls.insert(k, v);
    }
    for h in [11u64, 12] {
        node.headers.insert(
            (0, h),
            vec![json!({"hash": format!("B{h}"), "height": h, "chainId": 0})],
        );
    }
    let au = ours_fn_server(|_: &Value| json!({"data": {"block": {"chainId": "0"}}})).await;
    let ctx = gql_ctx_at(node, dir.path(), &au).await;
    let parse = |stem: &str, q: &str| crate::graphql::parse_op(stem, q, false).unwrap();
    let by_key = parse(
        "QTransactionsByPublicKey",
        "query QTransactionsByPublicKey($publicKey: String, $first: Int, $after: String) { \
         transactionsByPublicKey(publicKey: $publicKey, first: $first, after: $after) { \
         totalCount pageInfo { hasNextPage endCursor } edges { cursor node { id hash } } } }",
    );
    let plain = parse(
        "QTxs",
        "query QTxs($first: Int, $after: String) { transactions(first: $first, after: $after) \
         { totalCount pageInfo { hasNextPage endCursor } edges { cursor node { id hash } } } }",
    );
    let heights = |literal: &str| {
        parse(
            "QCompletedBlockHeights",
            &format!(
                "query QCompletedBlockHeights($chainIds: [String!], $first: Int) {{ \
                 completedBlockHeights(chainIds: $chainIds,{literal} first: $first) {{ \
                 pageInfo {{ hasNextPage }} edges {{ node {{ height chainId hash }} }} }} }}"
            ),
        )
    };
    let tx = |rk: &str, h: u64| {
        let id = crate::graphql::base64_std(format!("Transaction:[\"B{h}\",\"{rk}\"]").as_bytes());
        json!({"cursor": "c", "node": {"id": id, "hash": rk}})
    };
    let blk = |h: u64| json!({"cursor": "c", "node": {"height": h, "chainId": 0, "hash": format!("B{h}")}});
    let run = |op: crate::graphql::GqlOp,
               root: &'static str,
               a: Vec<Value>,
               b: Vec<Value>,
               vars: serde_json::Map<String, Value>| {
        let ctx = &ctx;
        async move {
            let mut a = h7_page(root, false, a);
            let mut b = h7_page(root, false, b);
            let (d, note) = ctx
                .gql_settle_op(
                    &ctx.targets["a"],
                    Some(&op),
                    &vars,
                    &mut a,
                    &mut b,
                    7_000_000,
                    false,
                )
                .await;
            (c3_unexplained(ctx, &d), note.unwrap_or_default())
        }
    };
    let txs = || (vec![tx("t1", 101), tx("t2", 100)], vec![tx("t1", 101)]);
    // QTransactionsByPublicKey: t2 is canonical, but the harness does not check the key.
    let (a, b) = txs();
    let vars = h7_vars(&[("publicKey", json!("pk")), ("first", json!(2))]);
    let (un, note) = run(by_key.clone(), "transactionsByPublicKey", a, b, vars).await;
    assert!(!un.is_empty(), "{note}");
    assert!(
        note.contains("a filter the harness does not check: publicKey"),
        "{note}"
    );
    // The same rows with the key null, and on a list with no filter: explained.
    let (a, b) = txs();
    let vars = h7_vars(&[("publicKey", Value::Null), ("first", json!(2))]);
    let (un, note) = run(by_key, "transactionsByPublicKey", a, b, vars).await;
    assert!(un.is_empty(), "{un:?} {note}");
    let (a, b) = txs();
    let (un, note) = run(plain, "transactions", a, b, h7_vars(&[("first", json!(2))])).await;
    assert!(un.is_empty(), "{un:?} {note}");
    // A literal `heightCount`: B11 is canonical, but the literal is not checked.
    let vars = || h7_vars(&[("chainIds", json!(["0"])), ("first", json!(2))]);
    let (un, note) = run(
        heights(" heightCount: 3,"),
        "completedBlockHeights",
        vec![blk(12), blk(11)],
        vec![blk(12)],
        vars(),
    )
    .await;
    assert!(!un.is_empty(), "{note}");
    assert!(
        note.contains("a filter the harness does not check: heightCount"),
        "{note}"
    );
    let (un, note) = run(
        heights(""),
        "completedBlockHeights",
        vec![blk(12), blk(11)],
        vec![blk(12)],
        vars(),
    )
    .await;
    assert!(un.is_empty(), "{un:?} {note}");
}

/// R-P5P9-67 m-1: H6's entries survive the record's 50-entry cap, so the referee keeps a
/// record with an unfinished list unexplained even behind 60 field diffs.
#[tokio::test]
async fn c3_the_referee_keeps_h6_past_the_records_entry_cap() {
    use crate::report::{DiffEntry, DiffKind, DiffRecord};
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let ops = c3_ops(dir.path());
    let cfg = c3_cfg(
        &nu,
        "http://127.0.0.1:9/graphql",
        "https://oracle.invalid/graphql",
        dir.path(),
        &ops,
        "",
    );
    let body = json!({"data": {"transfers": {"totalCount": 1, "pageInfo": {"hasNextPage": false},
        "edges": [{"cursor": "c", "node": {"requestKey": "k1"}}]}}})
    .to_string();
    let mut diff: Vec<DiffEntry> = (0..60)
        .map(|i| DiffEntry {
            path: format!("/data/transfers/edges/0/node/f{i}"),
            kind: DiffKind::Changed,
            a: Some(json!(1)),
            b: Some(json!(2)),
        })
        .collect();
    diff.push(DiffEntry {
        path: crate::graphql_scope::UNFINISHED_PATH.into(),
        kind: DiffKind::Changed,
        a: Some(json!({"lists": []})),
        b: Some(json!({"lists": [{"list": "/data/x", "why": "a page read failed"}]})),
    });
    let rec = DiffRecord {
        ts: "2026-11-14T20:00:00Z".into(),
        route: "graphql:QH7".into(),
        request: format!("QH7 {}", json!({"first": 1})),
        a_target: "a".into(),
        b_target: "b".into(),
        a_status: Some(200),
        b_status: Some(200),
        a_body_trimmed: body.clone(),
        b_body_trimmed: body,
        diff,
        class: "diff".into(),
        allowlisted: None,
        curl: String::new(),
        note: None,
        body_cap: None,
        a_pages: Some(1),
    };
    let log = dir.path().join("diffs-cap.jsonl");
    let line = serde_json::to_string(&rec.bounded()).unwrap();
    std::fs::write(&log, format!("{line}\n")).unwrap();
    let opts = crate::referee::Opts {
        logs: vec![log],
        max_reads: 100,
        rps: 10,
    };
    let rep = crate::referee::run(cfg, &opts).await.unwrap();
    assert_eq!((rep.explained, rep.unexplained), (0, 1), "{rep:?}");
    let still = serde_json::to_string(&rep.still_unexplained).unwrap();
    assert!(still.contains("h6-unfinished"), "{still}");
}

/// R-P5P9-67 m-2: `node_max_rps` below 2 is refused in `Ctx::new` while header reads are
/// on (an oracle target), so `referee --rps 1` is refused too.
#[tokio::test]
async fn c3_node_max_rps_below_2_is_refused_with_header_reads() {
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let none = dir.path().join("no-ops");
    let cfg = |extra: &str| {
        c3_cfg(
            &nu,
            "http://127.0.0.1:9/graphql",
            "https://oracle.invalid/graphql",
            dir.path(),
            &none,
            extra,
        )
    };
    for n in [0, 1] {
        let Err(e) = Ctx::new(cfg(&format!("node_max_rps = {n}"))).await else {
            panic!("node_max_rps {n} was accepted");
        };
        assert!(format!("{e:#}").contains("node_max_rps"), "{e:#}");
    }
    assert!(Ctx::new(cfg("node_max_rps = 2")).await.is_ok());
    let log = dir.path().join("empty.jsonl");
    std::fs::write(&log, "").unwrap();
    let opts = |rps| crate::referee::Opts {
        logs: vec![log.clone()],
        max_reads: 10,
        rps,
    };
    assert!(crate::referee::run(cfg(""), &opts(1)).await.is_err());
    assert!(crate::referee::run(cfg(""), &opts(2)).await.is_ok());
}

/// R-P5P9-67 m-3: merged bytes per side are capped at 32 MiB; past it the record is a diff
/// with `/~harness-limit/graphql-merged-bytes`, which no allowlist entry matches.
#[tokio::test]
async fn c3_merged_bytes_per_side_are_capped_at_32_mib() {
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    // Pages of 7 MiB with a next page each time: six pages (follow_depth 5) are 42 MiB.
    let big = ours_fn_server(|req: &Value| {
        let n: u64 = req["variables"]["after"]
            .as_str()
            .map_or(0, |c| c.parse().unwrap());
        json!({"data": {"transfers": {"pageInfo": {"hasNextPage": true,
            "endCursor": (n + 1).to_string()},
            "edges": [{"cursor": "c", "node": {"requestKey": format!("k{n}"),
                "memo": "x".repeat(7 << 20)}}]}}})
    })
    .await;
    let small = ours_fn_server(|_: &Value| {
        json!({"data": {"transfers": {"pageInfo": {"hasNextPage": false},
            "edges": [{"cursor": "c", "node": {"requestKey": "k0"}}]}}})
    })
    .await;
    let none = dir.path().join("no-ops");
    let op = h7_op("transfers");
    let vars = h7_vars(&[("first", json!(1))]);
    for (au, bu) in [(&big, &small), (&small, &big)] {
        let ctx = Ctx::new(c3_cfg(&nu, au, bu, dir.path(), &none, "follow_depth = 5"))
            .await
            .unwrap();
        let o = ctx
            .gql_vs_oracle(&ctx.targets["a"], &ctx.targets["b"], &op, &vars, 7_000_000)
            .await;
        assert_eq!(o.class, "diff", "{:?}", o.record.as_ref().map(|r| &r.note));
        let rec = o.record.unwrap();
        let un = c3_unexplained(&ctx, &rec.diff);
        assert!(
            un.iter()
                .any(|p| p == "/~harness-limit/graphql-merged-bytes"),
            "{un:?}"
        );
    }
}

/// R-P5P9-67 m-4: the referee reads lines as bytes; a line that is not UTF-8 is listed as
/// not refereed and the run goes on.
#[tokio::test]
async fn c3_the_referee_lists_a_line_that_is_not_utf8_and_goes_on() {
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let ops = c3_ops(dir.path());
    let cfg = c3_cfg(
        &nu,
        "http://127.0.0.1:9/graphql",
        "https://oracle.invalid/graphql",
        dir.path(),
        &ops,
        "",
    );
    let body = json!({"data": {"transfers": {"pageInfo": {"hasNextPage": false},
        "edges": [{"cursor": "c", "node": {"requestKey": "k1"}}]}}})
    .to_string();
    let rec = json!({"ts": "2026-11-14T20:00:00Z", "route": "graphql:QH7",
        "request": format!("QH7 {}", json!({"first": 1})), "a_target": "a", "b_target": "b",
        "a_status": 200, "b_status": 200, "a_body_trimmed": body, "b_body_trimmed": body,
        "diff": [], "class": "diff", "curl": "", "a_pages": 1});
    let mut bytes = b"{\"class\": \"diff\", \"note\": \"cut \xe2\x82".to_vec();
    bytes.extend_from_slice(b"\xff\n");
    bytes.extend_from_slice(format!("{rec}\n").as_bytes());
    let log = dir.path().join("diffs-utf8.jsonl");
    std::fs::write(&log, bytes).unwrap();
    let opts = crate::referee::Opts {
        logs: vec![log],
        max_reads: 100,
        rps: 10,
    };
    let rep = crate::referee::run(cfg, &opts)
        .await
        .expect("the run goes on past a line that is not UTF-8");
    assert_eq!(
        (rep.records, rep.explained, rep.not_refereed),
        (2, 1, 1),
        "{rep:?}"
    );
    let still = serde_json::to_string(&rep.still_unexplained).unwrap();
    assert!(
        still.contains("not UTF-8") && still.contains("diffs-utf8.jsonl:1"),
        "{still}"
    );
}

/// R-P5P9-67 m-5: H7 decides canonicity through the node's cut branch, not the `/header`
/// range by height (which holds orphans, and above the cut can lack the canonical block).
#[tokio::test]
async fn c3_h7_canonicity_comes_through_the_cuts_branch() {
    let dir = tempfile::tempdir().unwrap();
    let hd = |h: &str| json!({"hash": h, "height": 50, "chainId": 0});
    let blk =
        |h: &str, n: u64| json!({"cursor": "c", "node": {"hash": h, "height": n, "chainId": 0}});
    let b51 = || blk("B51", 51);
    let node = |range: &[&str], canon: Option<&str>, top: Option<u64>| {
        let mut n = NodeMock {
            branch_top: top,
            ..NodeMock::default()
        };
        n.headers.insert(
            (0, 51),
            vec![json!({"hash": "B51", "height": 51, "chainId": 0})],
        );
        n.headers
            .insert((0, 50), range.iter().map(|h| hd(h)).collect());
        if let Some(c) = canon {
            n.canon.insert((0, 50), c.to_string());
        }
        n
    };
    let run = |n: NodeMock, a: Vec<Value>, b: Vec<Value>| {
        let dir = dir.path().to_path_buf();
        async move {
            let ctx = gql_ctx(n, &dir).await;
            let mut a = h7_page("blocksFromDepth", false, a);
            let mut b = h7_page("blocksFromDepth", false, b);
            let (d, note) = ctx
                .gql_settle(
                    &ctx.targets["a"],
                    &serde_json::Map::new(),
                    &mut a,
                    &mut b,
                    7_000_000,
                    false,
                )
                .await;
            let row = d.iter().any(|e| e.path == crate::graphql_window::ROW_PATH);
            (
                c3_unexplained(&ctx, &d),
                row,
                format!("{d:?}"),
                note.unwrap_or_default(),
            )
        }
    };
    // An ours-only row in the orphan O50, where the range holds only O50 (the canonical X50
    // not stored there) or both: not canonical, a diff.
    for range in [&["O50"][..], &["O50", "X50"][..]] {
        let (un, row, _, note) = run(
            node(range, Some("X50"), None),
            vec![b51(), blk("O50", 50)],
            vec![b51()],
        )
        .await;
        assert!(!un.is_empty() && row, "{range:?}: {note}");
        assert!(note.contains("not canonical"), "{range:?}: {note}");
    }
    // An oracle-only row in the canonical X50, where the range holds only O50: canonical, so
    // not an orphan; our list is complete, so a diff.
    let (un, _, d, note) = run(
        node(&["O50"], Some("X50"), None),
        vec![b51()],
        vec![b51(), blk("X50", 50)],
    )
    .await;
    assert!(!un.is_empty(), "{note}");
    assert!(!d.contains("orphan"), "{d}");
    // The neighbour: an oracle-only row in O50 beside the canonical X50 is an orphan.
    let (un, _, d, note) = run(
        node(&["X50", "O50"], Some("X50"), None),
        vec![b51()],
        vec![b51(), blk("O50", 50)],
    )
    .await;
    assert!(un.is_empty(), "{un:?} {note}");
    assert!(d.contains("orphan"), "{d}");
    // Above the cut (the branch ends at 49): no canonical block at 50 yet, so unsettled.
    let (un, row, _, note) = run(
        node(&["X50"], None, Some(49)),
        vec![b51(), blk("X50", 50)],
        vec![b51()],
    )
    .await;
    assert!(!un.is_empty() && !row, "{note}");
}

/// R-P5P9-67 m-6: our longer read keeps its `hasNextPage`. A row absent from a read that ours
/// capped (100 rows, Q10) with a next page is "undecided: our longer read was capped", at
/// n = 100 and at n = 60; a row within the capped read is still explained.
#[tokio::test]
async fn c3_a_row_absent_from_our_capped_longer_read_is_undecided() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for rk in ["x", "r59", "r99"] {
        let (k, v) = h7_poll(rk, "B941", 941, json!([]));
        node.polls.insert(k, v);
    }
    // Ours' whole list: 59 rows above height 941, then rows at 941 with x at `xAt`.
    fn full(x_at: usize) -> Vec<Value> {
        let h = |i: usize| if i < 59 { 1000 - i as u64 } else { 941 };
        let t = |rk: &str, h: u64| h7_tx(rk, h, &format!("B{h}"), "2024-01-01T10:00:00Z");
        let mut v: Vec<Value> = (0..x_at).map(|i| t(&format!("r{i}"), h(i))).collect();
        v.push(t("x", 941));
        v.extend((x_at..x_at + 10).map(|i| t(&format!("r{i}"), 941)));
        v
    }
    let au = ours_fn_server(|req: &Value| {
        let x_at: usize = req["variables"]["xAt"].as_str().unwrap().parse().unwrap();
        let rows = full(x_at);
        let n = (req["variables"]["first"].as_u64().unwrap_or(0) as usize).min(100);
        let page: Vec<Value> = rows.iter().take(n).cloned().collect();
        json!({"data": {"transactions": {"pageInfo": {"hasNextPage": n < rows.len()},
            "edges": page}}})
    })
    .await;
    let ctx = gql_ctx_at(node, dir.path(), &au).await;
    let op = h7_op("transactions");
    for (n, x_at, explained) in [(100, 105, false), (60, 105, false), (60, 80, true)] {
        let rows = full(x_at);
        let ours: Vec<Value> = rows.iter().take(n).cloned().collect();
        // The oracle lists x in place of our last row (canonical: an oracle gap).
        let mut oracle = ours[..n - 1].to_vec();
        oracle.push(rows[x_at].clone());
        let mut a = crate::graphql::Answer {
            ms: vec![1],
            ..h7_page("transactions", true, ours)
        };
        let mut b = h7_page("transactions", true, oracle);
        let vars = h7_vars(&[("xAt", json!(x_at.to_string())), ("first", json!(n))]);
        let (d, note) = ctx
            .gql_settle_op(
                &ctx.targets["a"],
                Some(&op),
                &vars,
                &mut a,
                &mut b,
                7_000_000,
                false,
            )
            .await;
        let note = note.unwrap_or_default();
        let un = c3_unexplained(&ctx, &d);
        assert_eq!(
            un.is_empty(),
            explained,
            "n {n}, x at {x_at}: {un:?} {note}"
        );
        if !explained {
            assert!(
                note.contains("undecided: our longer read was capped"),
                "n {n}: {note}"
            );
        }
    }
}

// ---------------------------------------------------------------------------------------------
// FVH-C4: `/v1/txs` answers over the body cap (R-P5P9-52 concern 5, R-P5P9-66 T-items)

/// `/v1/txs` rows of `pad` bytes of code each.
fn v1_rows(n: usize, pad: usize) -> Vec<Value> {
    (0..n)
        .map(|i| {
            json!({"requestKey": format!("rk{i:04}"), "chain": i % 20,
                "height": 7_000_000 - i as u64, "blockHash": format!("b{i}"), "amount": 1.5,
                "code": "x".repeat(pad), "events": [], "gasLimit": 600})
        })
        .collect()
}

/// A server answering `body` on every path.
async fn bytes_server(body: Vec<u8>) -> String {
    let body = bytes::Bytes::from(body);
    let app = Router::new().fallback(move || {
        let b = body.clone();
        async move { ([("content-type", "application/json")], b) }
    });
    serve(app).await
}

/// A context comparing compat (`a`) with kda-txs (`b`) on `/v1/txs`, each answering its bytes.
async fn v1_setup(a: Vec<u8>, b: Vec<u8>, out: &std::path::Path) -> Ctx {
    let (nu, au, bu) = (
        node_server(NodeMock::default()).await,
        bytes_server(a).await,
        bytes_server(b).await,
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
timeout_ms = 20000
[targets.a]
url = "{au}"
kind = "kda_indexer_compat"
[targets.b]
url = "{bu}"
kind = "kda_txs"
"#
    ))
    .unwrap();
    Ctx::new(cfg).await.unwrap()
}

/// One `/v1/txs` compare of the pair, without `process`'s 30 s kda-txs retry.
async fn v1_compare(ctx: &Ctx, path: &str) -> super::Outcome {
    let req = Request::new(path.to_string(), Origin::Seed, None);
    ctx.vs_target(&req, &ctx.targets["a"], &ctx.targets["b"], None)
        .await
}

/// Item 2: two answers over the 8 MiB cap compare by a streamed hash of the normalized
/// rows: equal sets pass (order and kda-txs's `_id` aside), one differing row is a diff that
/// names the row, with both copies from a bounded second read. Never an a-error, never cut.
#[tokio::test]
async fn c4_large_v1txs_answers_are_compared_by_streamed_hash() {
    let dir = tempfile::tempdir().unwrap();
    let rows = v1_rows(250, 36_000);
    let a = serde_json::to_vec(&rows).unwrap();
    assert!(a.len() > crate::report::BODY_CAP, "{} bytes", a.len());
    let mut same = rows.clone();
    same.reverse();
    same[3]["_id"] = json!("cache-3");
    let ctx = v1_setup(a.clone(), serde_json::to_vec(&same).unwrap(), dir.path()).await;
    let o = v1_compare(&ctx, "/v1/txs/k:big/coin/250").await;
    assert_eq!(o.class, "match", "{:?}", o.record.map(|r| (r.note, r.diff)));
    let mut one = rows.clone();
    one[137]["amount"] = json!(2.5);
    let ctx = v1_setup(a, serde_json::to_vec(&one).unwrap(), dir.path()).await;
    let o = v1_compare(&ctx, "/v1/txs/k:big/coin/250").await;
    let rec = o.record.expect("a record");
    assert_eq!(o.class, "diff", "{:?} {:?}", rec.note, rec.diff);
    let hash = rec
        .diff
        .iter()
        .find(|e| e.path == crate::streamed::HASH_PATH)
        .expect("the hash entry");
    assert_ne!(hash.a, hash.b);
    let row = rec
        .diff
        .iter()
        .find(|e| e.path == format!("{}/rk0137", crate::streamed::ROW_PATH))
        .expect("the first differing row is named");
    assert_eq!(row.a.as_ref().unwrap()["amount"], json!(1.5));
    assert_eq!(row.b.as_ref().unwrap()["amount"], json!(2.5));
    // No allowlist entry explains either entry, on any /v1/txs request.
    let (left, ids) = ctx.allow.classify_request(
        &crate::normalize::RouteKind::V1Txs,
        "/v1/txs/k:d7bf62aa/coin/250",
        &[],
        &rec.diff,
    );
    assert_eq!(left.len(), rec.diff.len(), "{ids:?}");
}

/// Item 3 (the I-6 residual of run 6E): ours' 250-row page over 8 MiB (`/v1/txs/669bd564…/
/// coin/250`, an a-error at the body cap) goes through the same streamed path while kda-txs's
/// answer is under the cap and read as usual: the same rows pass; kda-txs lacking a row is a
/// diff naming it (no limit-edge or lag rule applies to a streamed compare).
#[tokio::test]
async fn c4_a_250_row_page_over_8_mib_goes_through_the_streamed_path() {
    let dir = tempfile::tempdir().unwrap();
    let rows = v1_rows(250, 33_000);
    let b = serde_json::to_vec(&rows).unwrap();
    // Ours: the same rows, with whitespace that takes the answer over the cap.
    let mut a = b.clone();
    let end = a.pop().unwrap();
    a.extend(std::iter::repeat_n(b' ', 300_000));
    a.push(end);
    assert!(a.len() > crate::report::BODY_CAP && b.len() < crate::report::BODY_CAP);
    let path = "/v1/txs/k:669bd564/coin/250";
    let ctx = v1_setup(a.clone(), b, dir.path()).await;
    let o = v1_compare(&ctx, path).await;
    assert_eq!(o.class, "match", "{:?}", o.record.map(|r| (r.note, r.diff)));
    let short = serde_json::to_vec(&rows[..249]).unwrap();
    let ctx = v1_setup(a, short, dir.path()).await;
    let o = v1_compare(&ctx, path).await;
    let rec = o.record.expect("a record");
    assert_eq!(o.class, "diff", "{:?} {:?}", rec.note, rec.diff);
    let row = rec
        .diff
        .iter()
        .find(|e| e.path == format!("{}/rk0249", crate::streamed::ROW_PATH))
        .expect("the row only ours lists is named");
    assert!(row.a.is_some() && row.b.is_none(), "{row:?}");
}
