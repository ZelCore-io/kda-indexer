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

mod e2b;
mod e3b;
mod g;
mod h;
mod h2;

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
    /// `/local` answers by (chain, module) for a read whose code names the module, ahead of
    /// `local`; `null` answers "No value found in table <module>_ledger".
    pub local_by_module: HashMap<(u8, String), Value>,
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
    /// A tip that moves: the k-th cut read answers hashes `cut-<c>-<k>` at `tip + k - 1`, and
    /// `header/branch` from that cut answers nothing above that height.
    pub moving_cut: Option<Arc<Mutex<u64>>>,
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
                let k = n.moving_cut.as_ref().map(|m| {
                    let mut k = m.lock().unwrap();
                    *k += 1;
                    *k
                });
                let (tip, sfx) = match k {
                    Some(k) => (n.tip + k - 1, format!("-{k}")),
                    None => (n.tip, String::new()),
                };
                let hashes: serde_json::Map<String, Value> = (0..20u8)
                    .map(|c| (c.to_string(), json!({"height": tip, "hash": format!("cut-{c}{sfx}")})))
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
                      Query(q): Query<HashMap<String, u64>>,
                      body: axum::body::Bytes| async move {
                    log(&n, &uri);
                    let (lo, hi) = (q["minheight"], q["maxheight"]);
                    // A moving cut: the branch from the k-th cut tops at `tip + k - 1`.
                    let body: Value = serde_json::from_slice(&body).unwrap_or_default();
                    let from_cut = n.moving_cut.as_ref().and_then(|_| {
                        let u = body.pointer("/upper/0")?.as_str()?;
                        let k: u64 = u.rsplit('-').next()?.parse().ok()?;
                        Some(n.tip + k - 1)
                    });
                    // The canonical header at each height: `canon`'s block (its stored
                    // header when `headers` holds it), else the only header stored there.
                    let items: Vec<Value> = (lo..=hi)
                        .filter(|h| n.branch_top.is_none_or(|top| *h <= top))
                        .filter(|h| from_cut.is_none_or(|top| *h <= top))
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
                    let by_module = n
                        .local_by_module
                        .iter()
                        .find(|((mc, m), _)| *mc == c && code.contains(&format!("({m}.")));
                    if let Some(((_, m), d)) = by_module {
                        return Json(if d.is_null() {
                            json!({"result": {"status": "failure", "error": {"message":
                                format!("No value found in table {m}_ledger")}}, "metaData": meta})
                        } else {
                            json!({"result": {"status": "success", "data": d}, "metaData": meta})
                        });
                    }
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
    gql_ctx_ab(node, out, a_url, "https://oracle.invalid/graphql").await
}

/// [`gql_ctx_at`] with the oracle (`b`) at `b_url`.
async fn gql_ctx_ab(node: NodeMock, out: &std::path::Path, a_url: &str, b_url: &str) -> Ctx {
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
url = "{b_url}"
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
    // Ours sends `.mmmZ` (R-P5P9-68 (2) checks the form), so the fixture does too.
    let ct = match ct.strip_suffix('Z') {
        Some(t) if !t.contains('.') => format!("{t}.000Z"),
        _ => ct.to_string(),
    };
    json!({"cursor": "c", "node": {"id": id, "hash": rk,
        "cmd": {"meta": {"chainId": "0", "creationTime": ct}},
        "result": {"height": h, "block": {"hash": block, "chainId": 0}}}})
}

/// R-P5P9-68 (2): backs every `/poll` entry of `node` with the node's own copy of the signed
/// command, so a one-sided `h7_tx` row that selects `cmd { meta { chainId creationTime } }` is
/// settled against the node: a header at the poll's height (the block the poll names, and the
/// only one stored there) whose payload holds the key's command. Ours' fixture rows sit in
/// `B<height>`, so a poll that names another block (the node places the key elsewhere) is left
/// unbacked. `cts` gives a key's creationTime (ours' form); other keys take `default_ct`.
fn h7_back_cmds(node: &mut NodeMock, default_ct: &str, cts: &[(&str, &str)]) {
    let polls: Vec<(String, u8, String, u64)> = node
        .polls
        .iter()
        .filter_map(|(rk, (c, v))| {
            Some((
                rk.clone(),
                *c,
                v.pointer("/metaData/blockHash")?.as_str()?.to_string(),
                v.pointer("/metaData/blockHeight")?.as_u64()?,
            ))
        })
        .collect();
    for (rk, c, block, h) in polls {
        if block != format!("B{h}") {
            continue;
        }
        let ct = cts
            .iter()
            .find(|(k, _)| *k == rk)
            .map_or(default_ct, |(_, t)| *t);
        let secs = crate::graphql_window::parse_utc(ct).unwrap().0;
        let cmd = json!({"meta": {"chainId": c.to_string(), "creationTime": secs}});
        let w = json!({"hash": rk, "sigs": [], "cmd": cmd.to_string()}).to_string();
        let ph = format!("P{block}");
        let heads = node.headers.entry((c, h)).or_default();
        if !heads.iter().any(|x| x["hash"] == json!(block)) {
            heads.push(json!({"hash": block, "height": h, "chainId": c, "payloadHash": ph}));
        }
        let pl = node
            .payloads
            .entry(ph)
            .or_insert_with(|| json!({"transactions": []}));
        pl["transactions"]
            .as_array_mut()
            .unwrap()
            .push(json!([kdx_types::hash::b64url_encode(w.as_bytes()), ""]));
    }
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
    h7_back_cmds(
        &mut node,
        "2024-01-01T10:01:00Z",
        &[
            ("r105", "2024-01-01T10:05:00Z"),
            ("r103", "2024-01-01T10:04:00Z"),
        ],
    );
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
    // Two polls, plus the cut, the branch header and the payload that check ours-only r104's
    // `cmd` (R-P5P9-68 (2)).
    assert_eq!(ev.b.clone().unwrap()["nodeReads"], json!(5), "{text}");
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
        // No relay id: R-P5P9-70 (I-1) cross-checks a selected `id` with the row's fields.
        json!({"cursor": "c", "node": {"amount": "1.5", "senderAccount": "s",
            "receiverAccount": "r", "chainId": 0, "blockHash": b, "requestKey": rk}})
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
    // block: never orphans by content (R-P5P9-63 (2)), but off the node's chain by its
    // transaction (R-P5P9-75 E, CG-4 #23): explained as oracle-row-not-in-node-tx.
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
    assert!(!stays_diff(&d), "{d:?}");
    assert!(
        format!("{d:?}").contains(crate::graphql_window::NOT_IN_NODE_TX),
        "{d:?}"
    );
    // One such copy in the oracle's last slot: off the node's transaction too (R-P5P9-75 E).
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
    assert!(!stays_diff(&d), "{d:?}");
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
    // The oracle's 3.0 is not the node's: off the node's transaction (R-P5P9-75 E), while
    // ours' 2.0 is a node-confirmed oracle gap.
    let (d, _) = run(vec![u("1.0"), u("2.0")], vec![u("1.0"), u("3.0")]).await;
    assert!(only_hits(&d), "{d:?}");
    assert!(format!("{d:?}").contains("oracle-row-not-in-node-tx"));
    // Selecting the amount alone, the oracle's 3.0 cannot be placed off the node's chain
    // (E needs the sender, receiver and amount): unknown, a diff.
    let v = |a: &str| h7_xrow("u", 100, json!(a), None);
    let (d, _) = run(vec![v("1.0"), v("2.0")], vec![v("1.0"), v("3.0")]).await;
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
    h7_back_cmds(&mut node, "2024-01-01T10:00:00Z", &[]);
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
    // R-P5P9-86 NC: unsettled (the node's block for the key is the row's), not "not canonical".
    let note = note.unwrap_or_default();
    assert!(
        note.contains("the node's transaction lacks the row") && !note.contains("not canonical"),
        "{note}"
    );
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
    let Err(err) = crate::graphql::fetch_pages(&ctx.targets["a"], &op, &vars, 3, true).await else {
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
    h7_back_cmds(&mut node, "2024-01-01T10:00:00Z", &[]);
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
/// direction and a node `cont.data` that is not null stay diffs. (R-P5P9-75 K: an exec
/// payload is explained when ours equals the node's `exec.data`.)
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
    // An exec payload: R-P5P9-75 K extends the matcher to it (ours "null" = the node's
    // exec.data null), so it is explained now; d2_j_k_l_tx_fields_against_the_nodes_command
    // holds its negatives.
    let d = settle(tx("e1", "null"), tx("e1", "{}")).await;
    assert!(only_hits(&d), "{d:?}");
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
    h7_back_cmds(
        &mut node,
        "2024-01-01T10:01:00Z",
        &[
            ("r105", "2024-01-01T10:05:00Z"),
            ("r103", "2024-01-01T10:04:00Z"),
            ("r100", "2024-01-01T10:03:00Z"),
            ("x104", "2024-01-01T10:02:00Z"),
        ],
    );
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
    // The node's chain graph, then two reads per H7 record, and three more (the cut, the
    // branch header, the payload) for each of the two records whose ours-only row has its
    // `cmd` checked (R-P5P9-68 (2)).
    assert_eq!(rep.node_reads, 11, "{rep:?}");
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
        // R-P5P9-105 (G): both cut pages (no `pageInfo`) are now G's `cutBoth` candidate, an
        // unfinished entry until the node settles it; H6's own unfinished entry stays.
        assert!(
            d.iter().any(|p| {
                p.starts_with("/data/blocksFromHeight/edges/0/node/transactions")
                    || p == UNFINISHED_PATH
            }),
            "{h}: {d:?}"
        );
    }
    // The oracle cannot be paged to its end (DEAD: the retry fails too), or its paging gives
    // k3 twice (in place of k6, the same value): not finished, the cut pages stay a diff, and
    // the record says why (no allowlist entry matches it).
    for h in ["DEAD", "TW"] {
        let (n, limit, d) = run(&ctx, &op, src, &[(h, 147)]).await;
        assert_eq!((n, limit), (0, vec![UNFINISHED_PATH.to_string()]), "{h}");
        // R-P5P9-105 (G): or G's `cutBoth` unfinished entry, as above.
        assert!(
            d.iter().any(|p| {
                p.starts_with("/data/blocksFromHeight/edges/0/node/transactions")
                    || p == UNFINISHED_PATH
            }),
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
         totalCount pageInfo { hasNextPage endCursor } edges { cursor node { id hash result { \
         ... on TransactionResult { height block { hash height chainId } } } } } } }",
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
        // R-P5P9-86 W: the rows our QTransactionsByPublicKey document selects (the block's
        // height, chain and hash), so H7 places them.
        json!({"cursor": "c", "node": {"id": id, "hash": rk, "result": {"height": h,
            "block": {"hash": format!("B{h}"), "height": h, "chainId": 0}}}})
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
    // QTransactionsByPublicKey: t2 is canonical; R-P5P9-86 U checks the key against the
    // signers of the node's command, which this node cannot serve: unsettled, a diff.
    let (a, b) = txs();
    let vars = h7_vars(&[("publicKey", json!("pk")), ("first", json!(2))]);
    let (un, note) = run(by_key.clone(), "transactionsByPublicKey", a, b, vars).await;
    assert!(!un.is_empty(), "{note}");
    assert!(
        note.contains("under a publicKey filter the node did not settle"),
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
        known_gap: None,
        curl: String::new(),
        note: None,
        body_cap: None,
        a_pages: Some(1),
        a_tip_height: None,
        a_lowest_height: None,
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
    h7_back_cmds(&mut node, "2024-01-01T10:00:00Z", &[]);
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
    v1_setup_node(a, b, out, NodeMock::default()).await
}

/// [`v1_setup`] with its own node mock.
async fn v1_setup_node(a: Vec<u8>, b: Vec<u8>, out: &std::path::Path, node: NodeMock) -> Ctx {
    bytes_pair_setup(a, b, out, node, "kda_txs").await
}

/// A context comparing compat (`a`) with an oracle of `b_kind` (`b`), each answering its
/// bytes on every path.
async fn bytes_pair_setup(
    a: Vec<u8>,
    b: Vec<u8>,
    out: &std::path::Path,
    node: NodeMock,
    b_kind: &str,
) -> Ctx {
    let (nu, au, bu) = (
        node_server(node).await,
        bytes_server(a).await,
        bytes_server(b).await,
    );
    urls_pair_setup(&nu, &au, &bu, out, b_kind, 0).await
}

/// A context comparing the servers at `au` (compat) and `bu` (`b_kind`) with `depth` follow-up
/// pages per paged answer.
async fn urls_pair_setup(
    nu: &str,
    au: &str,
    bu: &str,
    out: &std::path::Path,
    b_kind: &str,
    depth: u32,
) -> Ctx {
    let allow =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
    let cfg: CompareConfig = toml::from_str(&format!(
        r#"
network = "mainnet01"
rate_rps = 1000.0
follow_depth = {depth}
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
    .unwrap();
    Ctx::new(cfg).await.unwrap()
}

/// One `/v1/txs` compare of the pair, without `process`'s 30 s kda-txs retry.
async fn v1_compare(ctx: &Ctx, path: &str) -> super::Outcome {
    let req = Request::new(path.to_string(), Origin::Seed, None);
    ctx.vs_target(&req, &ctx.targets["a"], &ctx.targets["b"], None)
        .await
}

/// Item 2, R-P5P9-69: two answers over the 32 MiB cap (here about 40 MiB each) compare by a
/// streamed hash of the normalized rows: equal sets pass (order and kda-txs's `_id` aside),
/// one differing row is a diff that names the row, with both copies from a bounded second
/// read. Never an a-error, never cut.
#[tokio::test]
async fn c4_large_v1txs_answers_are_compared_by_streamed_hash() {
    let dir = tempfile::tempdir().unwrap();
    let rows = v1_rows(250, 168_000);
    let a = serde_json::to_vec(&rows).unwrap();
    assert!(a.len() > 40 << 20, "{} bytes", a.len());
    assert!(
        a.len() > crate::graphql::MERGED_BYTES_CAP,
        "{} bytes",
        a.len()
    );
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

/// R-P5P9-69 concern 2: a `/v1/txs` answer of about 12 MiB a side (over the old 8 MiB cap,
/// under 32 MiB) keeps the normal in-memory compare with every rule. Here kda-txs stops one
/// row short of the limit, which `oracle-limit-edge` explains: allowlisted, not streamed (the
/// streamed compare has no rules, so this was a diff at 17a6058).
#[tokio::test]
async fn c4_a_12_mib_v1txs_pair_keeps_its_rules() {
    let dir = tempfile::tempdir().unwrap();
    let rows = v1_rows(250, 51_000);
    let a = serde_json::to_vec(&rows).unwrap();
    let b = serde_json::to_vec(&rows[..249]).unwrap();
    assert!(
        a.len() > 12 << 20 && b.len() > 12 << 20,
        "{} {}",
        a.len(),
        b.len()
    );
    assert!(a.len() < crate::graphql::MERGED_BYTES_CAP);
    let ctx = v1_setup(a, b, dir.path()).await;
    let o = v1_compare(&ctx, "/v1/txs/k:big/coin/250").await;
    // An explained answer keeps no diff record (R-P5P9-75: its allowlisted record is logged);
    // a streamed compare would have kept a diff.
    assert_eq!(
        o.class,
        "allowlisted",
        "{:?}",
        o.record.as_ref().map(|r| (&r.note, &r.diff))
    );
    assert!(o.record.is_some_and(|r| r.class == "allowlisted"));
}

/// Item 3 (the I-6 residual of run 6E): ours' 250-row page over the 32 MiB cap (`/v1/txs/
/// 669bd564…/coin/250`, an a-error at the body cap) goes through the same streamed path while
/// kda-txs's answer is under the cap and read as usual: the same rows pass; kda-txs lacking a
/// row is a diff naming it (no limit-edge or lag rule applies to a streamed compare).
#[tokio::test]
async fn c4_a_250_row_page_over_the_cap_goes_through_the_streamed_path() {
    let dir = tempfile::tempdir().unwrap();
    let rows = v1_rows(250, 130_000);
    let b = serde_json::to_vec(&rows).unwrap();
    // Ours: the same rows, with whitespace that takes the answer over the cap.
    let mut a = b.clone();
    let end = a.pop().unwrap();
    a.extend(std::iter::repeat_n(b' ', 2_000_000));
    a.push(end);
    let cap = crate::graphql::MERGED_BYTES_CAP;
    assert!(a.len() > cap && b.len() < cap, "{} {}", a.len(), b.len());
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

/// R-P5P9-69 concern 4: a composed `kda_txs_via_cwd` answer over the 32 MiB cap is a
/// `BodyOverCap` that carries its capture, so the streamed compare hashes the capture and
/// takes the differing row's copy from it: chainweb-data is composed once per compare, never
/// twice. Equal rows → `match`; one row ours lacks → `diff` naming the row, with the
/// composed side's copy only.
#[tokio::test]
async fn c4_an_over_cap_composed_kda_txs_via_cwd_answer_is_composed_once_and_streamed() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    // 250 transactions of about 140 KB, so that the composed answer passes 32 MiB.
    let mut rows = v1_rows(250, 140_000);
    for (i, r) in rows.iter_mut().enumerate() {
        r["requestKey"] = json!(format!("{i:043}"));
    }
    let keys: Vec<Value> = rows
        .iter()
        .map(|r| json!({"requestKey": r["requestKey"]}))
        .collect();
    let account_calls = Arc::new(AtomicUsize::new(0));
    let detail_calls = Arc::new(AtomicUsize::new(0));
    let (ac, dc) = (account_calls.clone(), detail_calls.clone());
    let details = rows.clone();
    let app = Router::new()
        .route(
            "/txs/account/{a}",
            get(move || {
                ac.fetch_add(1, Ordering::SeqCst);
                let k = keys.clone();
                async move { Json(k) }
            }),
        )
        .route(
            "/txs/txs",
            get(move |Query(q): Query<HashMap<String, String>>| {
                dc.fetch_add(1, Ordering::SeqCst);
                let found: Vec<Value> = details
                    .iter()
                    .filter(|d| d["requestKey"].as_str() == q.get("requestkey").map(String::as_str))
                    .cloned()
                    .collect();
                async move { Json(found) }
            }),
        );
    let (nu, cu) = (node_server(NodeMock::default()).await, serve(app).await);
    let allow =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
    let out = dir.path();
    let path = "/v1/txs/k:big/coin/250";
    let ctx_for = |a: Vec<u8>| {
        let (nu, cu, allow) = (nu.clone(), cu.clone(), allow.clone());
        async move {
            let au = bytes_server(a).await;
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
timeout_ms = 60000
[targets.a]
url = "{au}"
kind = "kda_indexer_compat"
[targets.b]
url = "{cu}"
kind = "kda_txs_via_cwd"
"#
            ))
            .unwrap();
            Ctx::new(cfg).await.unwrap()
        }
    };
    let a = serde_json::to_vec(&rows).unwrap();
    assert!(
        a.len() > crate::graphql::MERGED_BYTES_CAP,
        "{} bytes",
        a.len()
    );
    let ctx = ctx_for(a).await;
    let o = v1_compare(&ctx, path).await;
    assert_eq!(o.class, "match", "{:?}", o.record.map(|r| (r.note, r.diff)));
    assert_eq!(account_calls.load(Ordering::SeqCst), 1);
    assert_eq!(detail_calls.load(Ordering::SeqCst), 250);
    // Ours lacks one row: a diff naming it, whose second read is the capture, not a recompose.
    account_calls.store(0, Ordering::SeqCst);
    detail_calls.store(0, Ordering::SeqCst);
    let mut short = rows.clone();
    let gone = short.remove(100);
    let ctx = ctx_for(serde_json::to_vec(&short).unwrap()).await;
    let o = v1_compare(&ctx, path).await;
    let rec = o.record.expect("a record");
    assert_eq!(o.class, "diff", "{:?} {:?}", rec.note, rec.diff);
    let key = gone["requestKey"].as_str().unwrap();
    let row = rec
        .diff
        .iter()
        .find(|e| e.path == format!("{}/{key}", crate::streamed::ROW_PATH))
        .expect("the row ours lacks is named");
    assert!(row.a.is_none() && row.b.is_some(), "{row:?}");
    assert_eq!(account_calls.load(Ordering::SeqCst), 1, "composed twice");
    assert_eq!(detail_calls.load(Ordering::SeqCst), 250, "composed twice");
}

/// R-P5P9-68 (2): an ours-only transaction row is excused only when every `cmd` field it
/// selects was checked against the node's signed command (one cached `payload_with_outputs`
/// read per block); a selected field the harness does not check refuses the excuse.
#[tokio::test]
async fn c4_ours_only_tx_rows_have_their_cmd_fields_checked_against_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for (rk, h) in [("t1", 101), ("t2", 100)] {
        let (k, v) = h7_poll(rk, &format!("B{h}"), h, json!([]));
        node.polls.insert(k, v);
    }
    node.headers.insert(
        (0, 100),
        vec![json!({"hash": "B100", "height": 100, "chainId": 0, "payloadHash": "P100"})],
    );
    let cmd = json!({"networkId": "mainnet01", "nonce": "n2",
        "meta": {"chainId": "0", "sender": "alice", "gasLimit": 600, "gasPrice": 1.0e-8,
            "ttl": 28800, "creationTime": 1_700_000_000},
        "payload": {"exec": {"code": "(coin.transfer \"alice\" \"bob\" 1.0)", "data": {"k": 1}}},
        "signers": [{"pubKey": "pk1", "clist": [{"name": "coin.GAS", "args": []}]}]});
    let w = json!({"hash": "t2", "sigs": [], "cmd": cmd.to_string()}).to_string();
    node.payloads.insert(
        "P100".into(),
        json!({"transactions": [[kdx_types::hash::b64url_encode(w.as_bytes()), ""]]}),
    );
    let au = ours_fn_server(|_: &Value| json!({"data": {"block": {"chainId": "0"}}})).await;
    let ctx = gql_ctx_at(node, dir.path(), &au).await;
    let plain = crate::graphql::parse_op(
        "QTxs",
        "query QTxs($first: Int, $after: String) { transactions(first: $first, after: $after) \
         { totalCount pageInfo { hasNextPage endCursor } edges { cursor node { id hash cmd { \
         networkId nonce meta { chainId sender gasLimit gasPrice ttl creationTime } payload { \
         ... on ExecutionPayload { code data } } signers { pubkey clist { name args } } } } } } }",
        false,
    )
    .unwrap();
    let ours_cmd = json!({"networkId": "mainnet01", "nonce": "n2",
        "meta": {"chainId": 0, "sender": "alice", "gasLimit": "600", "gasPrice": "0.00000001",
            "ttl": "28800", "creationTime": "2023-11-14T22:13:20.000Z"},
        "payload": {"code": "\"(coin.transfer \\\"alice\\\" \\\"bob\\\" 1.0)\"", "data": "{\"k\":1}"},
        "signers": [{"pubkey": "pk1", "clist": [{"name": "coin.GAS", "args": "[]"}]}]});
    let tx = |rk: &str, h: u64, c: &Value| {
        let id = crate::graphql::base64_std(format!("Transaction:[\"B{h}\",\"{rk}\"]").as_bytes());
        json!({"cursor": "c", "node": {"id": id, "hash": rk, "cmd": c}})
    };
    let run = |c: Value| {
        let ctx = &ctx;
        let op = plain.clone();
        let t1 = tx("t1", 101, &json!({"nonce": "n1"}));
        async move {
            let mut a = h7_page("transactions", false, vec![t1.clone(), tx("t2", 100, &c)]);
            let mut b = h7_page("transactions", false, vec![t1]);
            let (d, note) = ctx
                .gql_settle_op(
                    &ctx.targets["a"],
                    Some(&op),
                    &h7_vars(&[("first", json!(2))]),
                    &mut a,
                    &mut b,
                    7_000_000,
                    false,
                )
                .await;
            (c3_unexplained(ctx, &d), note.unwrap_or_default())
        }
    };
    // Every selected cmd field is the node's: a node-confirmed oracle gap.
    let (un, note) = run(ours_cmd.clone()).await;
    assert!(un.is_empty(), "{un:?} {note}");
    // A wrong sender (and each other checked field) on the ours-only row: a diff.
    for (ptr, value, name) in [
        ("/meta/sender", json!("mallory"), "cmd.meta.sender"),
        ("/meta/gasLimit", json!("601"), "cmd.meta.gasLimit"),
        (
            "/meta/creationTime",
            json!("2023-11-14T22:13:21.000Z"),
            "cmd.meta.creationTime",
        ),
        ("/nonce", json!("nX"), "cmd.nonce"),
        (
            "/payload/code",
            json!("\"(coin.details \\\"alice\\\")\""),
            "cmd.payload.code",
        ),
        ("/payload/data", json!("{\"k\":2}"), "cmd.payload.data"),
        ("/signers/0/pubkey", json!("pk2"), "cmd.signers.pubkey"),
        (
            "/signers/0/clist/0/args",
            json!("[1]"),
            "cmd.signers.clist.args",
        ),
    ] {
        let mut c = ours_cmd.clone();
        *c.pointer_mut(ptr).unwrap() = value;
        let (un, note) = run(c).await;
        assert!(!un.is_empty(), "{name}: {note}");
        assert!(note.contains(name), "{name}: {note}");
    }
    // A selected cmd field the harness does not check: the excuse is refused.
    for (ptr, value, name) in [
        ("/signers/0/scheme", json!("ED25519"), "cmd.signers.scheme"),
        // R-P5P9-71 (3): a `__typename` that is not the schema type at its level.
        (
            "/meta/__typename",
            json!("TransactionCommand"),
            "cmd.meta.__typename",
        ),
    ] {
        let mut c = ours_cmd.clone();
        let (parent, key) = ptr.rsplit_once('/').unwrap();
        c.pointer_mut(parent).unwrap()[key] = value;
        let (un, note) = run(c).await;
        assert!(!un.is_empty(), "{name}: {note}");
        assert!(
            note.contains(&format!("unchecked field: {name}")),
            "{name}: {note}"
        );
    }
    // R-P5P9-71 (3): the schema type there is explained.
    let mut c = ours_cmd.clone();
    c["meta"]["__typename"] = json!("TransactionMeta");
    let (un, note) = run(c).await;
    assert!(
        un.is_empty(),
        "cmd.meta.__typename TransactionMeta: {un:?} {note}"
    );
}

/// R-P5P9-70 (I-1): an ours-only row is excused only when every field it selects was checked
/// against the node. A per-kind whitelist walk over the whole row refuses any other field
/// ("unchecked field: <path>", an empty `cmd.payload` included); identity fields selected more
/// than once must agree; a transfer's `creationTime` is the node's command's.
#[tokio::test]
async fn c5_ours_only_rows_have_every_selected_field_checked() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for (rk, h) in [("t1", 101), ("t2", 100)] {
        let (k, v) = h7_xfer_poll(rk, &format!("B{h}"), h, "alice", "bob", json!(1.0));
        node.polls.insert(k, v);
    }
    h7_back_cmds(&mut node, "2023-11-14T22:13:20Z", &[]);
    let au = ours_fn_server(|_: &Value| json!({"data": {"block": {"chainId": "0"}}})).await;
    let ctx = gql_ctx_at(node, dir.path(), &au).await;
    let run = |op: crate::graphql::GqlOp, vars: Vec<(&'static str, Value)>, ours: Vec<Value>| {
        let ctx = &ctx;
        async move {
            let root = if op.stem == "QTxs" {
                "transactions"
            } else {
                "transfers"
            };
            let mut a = h7_page(root, false, ours.clone());
            let mut b = h7_page(root, false, vec![ours[0].clone()]);
            let (d, note) = ctx
                .gql_settle_op(
                    &ctx.targets["a"],
                    Some(&op),
                    &h7_vars(&vars),
                    &mut a,
                    &mut b,
                    7_000_000,
                    false,
                )
                .await;
            (c3_unexplained(ctx, &d), note.unwrap_or_default())
        }
    };
    // QTransfersByAccount: a transfer's creationTime against the node's command.
    let xop = crate::graphql::parse_op(
        "QTransfersByAccount",
        "query QTransfersByAccount($accountName: String!, $first: Int, $after: String) { \
         transfers(accountName: $accountName, first: $first, after: $after) { totalCount \
         pageInfo { hasNextPage endCursor } edges { cursor node { amount block { chainId \
         height } requestKey senderAccount receiverAccount creationTime } } } }",
        false,
    )
    .unwrap();
    let x = |rk: &str, h: u64, ct: &str| {
        let mut e = h7_xrow(rk, h, json!("1.0"), Some(("alice", "bob")));
        e["node"]["creationTime"] = json!(ct);
        e
    };
    let xvars = || vec![("accountName", json!("alice")), ("first", json!(2))];
    let ok = "2023-11-14T22:13:20.000Z";
    let (un, note) = run(
        xop.clone(),
        xvars(),
        vec![x("t1", 101, ok), x("t2", 100, ok)],
    )
    .await;
    assert!(un.is_empty(), "a correct transfer: {un:?} {note}");
    let late = "2023-11-14T22:13:21.000Z";
    let (un, note) = run(
        xop.clone(),
        xvars(),
        vec![x("t1", 101, ok), x("t2", 100, late)],
    )
    .await;
    assert!(!un.is_empty(), "creationTime + 1 s: {note}");
    assert!(note.contains("creationTime"), "{note}");
    // A transaction row of QTransactionsByAccount's shape (no account filter here).
    let top = crate::graphql::parse_op(
        "QTxs",
        "query QTxs($first: Int, $after: String) { transactions(first: $first, after: $after) \
         { totalCount pageInfo { hasNextPage endCursor } edges { cursor node { id hash sigs { \
         sig } cmd { payload { ... on ExecutionPayload { code } } } result { ... on \
         TransactionResult { height logs block { hash height chainId } } } } } } }",
        false,
    )
    .unwrap();
    let tx = |rk: &str, h: u64| {
        let id = crate::graphql::base64_std(format!("Transaction:[\"B{h}\",\"{rk}\"]").as_bytes());
        json!({"cursor": "c", "node": {"id": id, "hash": rk,
            "result": {"height": h, "block": {"hash": format!("B{h}"), "height": h, "chainId": 0}}}})
    };
    let tvars = || vec![("first", json!(2))];
    let (un, note) = run(top.clone(), tvars(), vec![tx("t1", 101), tx("t2", 100)]).await;
    assert!(un.is_empty(), "a correct tx: {un:?} {note}");
    let other_id = crate::graphql::base64_std(b"Transaction:[\"B99\",\"t2\"]");
    for (ptr, value, name) in [
        ("/node/hash", json!("t9"), "hash"),
        ("/node/result/block/height", json!(99), "height"),
        ("/node/id", json!(other_id), "block"),
        ("/node/sigs", json!([{"sig": "s"}]), "unchecked field: sigs"),
        (
            "/node/result/logs",
            json!("L"),
            "unchecked field: result.logs",
        ),
        // R-P5P9-86 T: a payload with no field of its member is checked against the node's
        // (here a node without the command): still a diff.
        ("/node/cmd", json!({"payload": {}}), "cmd.payload"),
    ] {
        let mut t2 = tx("t2", 100);
        let (parent, key) = ptr.rsplit_once('/').unwrap();
        t2.pointer_mut(parent).unwrap()[key] = value;
        let (un, note) = run(top.clone(), tvars(), vec![tx("t1", 101), t2]).await;
        assert!(!un.is_empty(), "{name}: {note}");
        assert!(note.contains(name), "{name}: {note}");
    }
    // R-P5P9-71 (3): a nested `__typename` is a constant, checked against the schema type at
    // its level: the right one is explained, a wrong one is a diff.
    let typed = |result: &str, block: &str| {
        let mut t2 = tx("t2", 100);
        t2["node"]["__typename"] = json!("Transaction");
        t2["node"]["result"]["__typename"] = json!(result);
        t2["node"]["result"]["block"]["__typename"] = json!(block);
        t2
    };
    let (un, note) = run(
        top.clone(),
        tvars(),
        vec![tx("t1", 101), typed("TransactionResult", "Block")],
    )
    .await;
    assert!(un.is_empty(), "the right nested __typename: {un:?} {note}");
    for (result, block, name) in [
        (
            "TransactionResult",
            "Transaction",
            "result.block.__typename",
        ),
        ("TransactionMempoolInfo", "Block", "result.__typename"),
    ] {
        let (un, note) = run(
            top.clone(),
            tvars(),
            vec![tx("t1", 101), typed(result, block)],
        )
        .await;
        assert!(!un.is_empty(), "{name}: {note}");
        assert!(
            note.contains(&format!("unchecked field: {name}")),
            "{name}: {note}"
        );
    }
}

/// R-P5P9-70 (I-2): the page size also comes from the root's literal `first`; when no page
/// size is known (a null `$first`, no `first`) and ours' cut page holds fewer rows than the
/// oracle's, it is a short page ("page size unknown"), never a window.
#[tokio::test]
async fn c5_a_literal_null_or_absent_first_keeps_the_short_page_check() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for i in 1..=5u64 {
        let h = 1000 - i;
        let (k, v) = h7_poll(&format!("t{i}"), &format!("B{h}"), h, json!([]));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let t = |i: u64| {
        h7_tx(
            &format!("t{i}"),
            1000 - i,
            &format!("B{}", 1000 - i),
            "2024-01-01T10:00:00Z",
        )
    };
    let body = "{ totalCount pageInfo { hasNextPage endCursor } edges { cursor node { id hash \
                cmd { meta { chainId creationTime } } result { height block { hash chainId } } \
                } } }";
    for (args, vars, what) in [
        ("first: 5, after: $after", vec![], "a literal first"),
        (
            "first: $first, after: $after",
            vec![("first", Value::Null)],
            "a null $first",
        ),
        ("after: $after", vec![], "no first"),
    ] {
        let q = format!("query QL($first: Int, $after: String) {{ transactions({args}) {body} }}");
        let op = crate::graphql::parse_op("QL", &q, false).unwrap();
        let mut a = crate::graphql::Answer {
            ms: vec![1],
            ..h7_page("transactions", true, (1..=3).map(t).collect())
        };
        let mut b = h7_page("transactions", true, (1..=5).map(t).collect());
        let (d, note) = ctx
            .gql_settle_op(
                &ctx.targets["a"],
                Some(&op),
                &h7_vars(&vars),
                &mut a,
                &mut b,
                7_000_000,
                false,
            )
            .await;
        let un = c3_unexplained(&ctx, &d);
        assert!(
            un.iter()
                .any(|p| p == crate::graphql_window::SHORT_PAGE_PATH),
            "{what}: {un:?} {note:?}"
        );
    }
}

/// R-P5P9-70 (m-1): N2 reads the cut per attempt, and a height above the cached cut (the tip
/// moved since it was read) drops the cut and that height's entry once and reads again. Here
/// the second read is one block past the first cut: the coinbase there is found.
#[tokio::test]
async fn c5_mined_at_rereads_the_cut_once_when_the_tip_moved() {
    let dir = tempfile::tempdir().unwrap();
    let cb = |to: &str| {
        let v = json!({"result": {"status": "success"}, "events": [{"name": "TRANSFER",
            "module": {"name": "coin", "namespace": null}, "params": ["", to, 0.9]}]});
        json!({"coinbase": kdx_types::hash::b64url_encode(v.to_string().as_bytes())})
    };
    let cut = Arc::new(Mutex::new(0u64));
    let mut node = NodeMock {
        tip: 100,
        moving_cut: Some(cut.clone()),
        ..NodeMock::default()
    };
    for h in [100u64, 101] {
        let ph = format!("p{h}");
        node.headers.insert(
            (0, h),
            vec![json!({"hash": format!("h{h}"), "payloadHash": ph, "height": h, "chainId": 0})],
        );
        node.payloads.insert(ph, cb("k:miner"));
    }
    let (ctx, _) = setup(node, SideMock::default(), SideMock::default(), dir.path()).await;
    // The run's own cut reads at setup do not count: the compare's first cut is at 100.
    *cut.lock().unwrap() = 0;
    let mut st = Reads::default();
    assert!(ctx.mined_at(0, 100, "k:miner", &mut st).await);
    // The tip moved to 101 after the cut was read: one re-read finds the block there.
    assert!(
        ctx.mined_at(0, 101, "k:miner", &mut st).await,
        "a height above the cached cut re-reads the cut once"
    );
    assert_eq!(*cut.lock().unwrap(), 2, "one cut read more");
    // Above the newest cut too: the re-read happens once, then the diff stays.
    assert!(!ctx.mined_at(0, 105, "k:miner", &mut st).await);
    assert_eq!(*cut.lock().unwrap(), 3, "one re-read per height, no loop");
}

/// R-P5P9-70 (m-5): N2's one-block skew is excused only when ours' tip on the chain is
/// `node_h - 1` (one read of ours) and the node stores exactly one header at `node_h`, the cut
/// branch's, whose coinbase paid the account.
#[tokio::test]
async fn c5_n2_skew_needs_ours_one_below_and_a_single_header() {
    let dir = tempfile::tempdir().unwrap();
    let cb = |to: &str| {
        let v = json!({"result": {"status": "success"}, "events": [{"name": "TRANSFER",
            "module": {"name": "coin", "namespace": null}, "params": ["", to, 0.9]}]});
        // S reads the whole payload: a block's transaction list too (empty here).
        json!({"coinbase": kdx_types::hash::b64url_encode(v.to_string().as_bytes()),
            "transactions": []})
    };
    let hd =
        |h: &str, p: &str, c: u8| json!({"hash": h, "payloadHash": p, "height": 100, "chainId": c});
    let mut node = NodeMock::default();
    node.headers.insert((0, 100), vec![hd("x0", "p0", 0)]);
    node.headers.insert((1, 100), vec![hd("x1", "p1", 1)]);
    // Chain 2: a fork at node_h; the cut's branch names the block that pays the miner.
    node.headers
        .insert((2, 100), vec![hd("x2", "p2", 2), hd("y2", "q2", 2)]);
    node.canon.insert((2, 100), "x2".into());
    for p in ["p0", "p1", "p2", "q2"] {
        node.payloads.insert(p.into(), cb("k:miner"));
    }
    // R-P5P9-78 (H2 m-6): ours' block at its tip (99) is the cut branch's header there.
    for c in [0u8, 2] {
        let h =
            json!({"hash": format!("t{c}-99"), "payloadHash": "pt", "height": 99, "chainId": c});
        node.headers.insert((c, 99), vec![h]);
    }
    // Ours: one block below node_h on chains 0 and 2; already at node_h on chain 1.
    let au = ours_fn_server(|req: &Value| {
        let c = req["variables"]["c"][0].as_str().unwrap_or("0").to_string();
        let start = req["variables"]["h"].as_u64().unwrap_or(0);
        let tip = if c == "1" { 100 } else { 99 };
        let edges: Vec<Value> = (start..=tip)
            .take(2)
            .map(|h| json!({"node": {"height": h, "chainId": c, "hash": format!("t{c}-{h}")}}))
            .collect();
        json!({"data": {"blocksFromHeight": {"pageInfo": {"hasNextPage": tip >= start + 2},
            "edges": edges}}})
    })
    .await;
    let ctx = gql_ctx_at(node, dir.path(), &au).await;
    let ta = &ctx.targets["a"];
    let skew = |c: u8| {
        let ctx = &ctx;
        async move {
            // R-P5P9-75 S replaced N2's coinbase-only check: the same cases hold under it.
            ctx.s_skew(ta, c, 100, "k:miner", 10.0, "10.9", &mut Reads::default())
                .await
                .is_ok()
        }
    };
    assert!(skew(0).await, "the true skew");
    assert!(!skew(1).await, "ours at node_h: no skew");
    assert!(!skew(2).await, "a fork at node_h");
}

/// R-P5P9-70 (m-3, R-P5P9-67 I-A (b) on the first page): ours' first page with HTTP 200 and a
/// body that is not JSON, or a 4xx other than 408/429, is a diff with its own entry, which no
/// allowlist entry matches; a 503 or a 429 stays an error.
#[tokio::test]
async fn c5_a_first_page_fault_of_ours_is_a_diff_and_a_5xx_an_error() {
    use axum::http::StatusCode;
    let dir = tempfile::tempdir().unwrap();
    let app = Router::new().route(
        "/graphql",
        post(|Json(req): Json<Value>| async move {
            match req["variables"]["accountName"].as_str().unwrap_or("") {
                "text" => (StatusCode::OK, "<html>oops</html>").into_response(),
                "bad-request" => (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"errors": [{"message": "invalid"}]})),
                )
                    .into_response(),
                "not-found" => (StatusCode::NOT_FOUND, "no").into_response(),
                "rate" => (StatusCode::TOO_MANY_REQUESTS, "slow down").into_response(),
                _ => (StatusCode::SERVICE_UNAVAILABLE, "busy").into_response(),
            }
        }),
    );
    let au = format!("{}graphql", serve(app).await);
    let ctx = gql_ctx_at(NodeMock::default(), dir.path(), &au).await;
    let op = h7_op("transfers");
    for (mode, class) in [
        ("text", "diff"),
        ("bad-request", "diff"),
        ("not-found", "diff"),
        ("rate", "a-error"),
        ("busy", "a-error"),
    ] {
        let vars = h7_vars(&[("accountName", json!(mode)), ("first", json!(1))]);
        let o = ctx
            .gql_vs_oracle(&ctx.targets["a"], &ctx.targets["b"], &op, &vars, 7_000_000)
            .await;
        assert_eq!(o.class, class, "{mode}: {o:?}");
        if class == "diff" {
            let rec = o.record.unwrap();
            assert!(
                rec.diff
                    .iter()
                    .any(|e| e.path == crate::graphql::FIRST_PAGE_FAULT_PATH),
                "{mode}: {rec:?}"
            );
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

/// R-P5P9-70 (m-4): `oracle-limit-edge` cuts only when kda-txs is short by exactly one at the
/// limit (`nb + 1 == na == limit`). kda-txs short by two, or ours short by one, stays a diff.
/// R-P5P9-71 (1): so does ours lacking kda-txs's lowest row while it holds one more below.
#[tokio::test]
async fn c5_the_limit_edge_needs_the_oracle_one_short_at_the_limit() {
    let rows = v1_rows(11, 10);
    let gap = [&rows[..8], &rows[9..11]].concat();
    let cases: [(&[Value], &[Value], &str); 4] = [
        (&rows[..10], &rows[..9], "allowlisted"),
        (&rows[..10], &rows[..8], "diff"),
        (&rows[..9], &rows[..10], "diff"),
        (&gap, &rows[..9], "diff"),
    ];
    for (a, b, class) in cases {
        let (na, nb) = (a.len(), b.len());
        let dir = tempfile::tempdir().unwrap();
        let a = serde_json::to_vec(a).unwrap();
        let b = serde_json::to_vec(b).unwrap();
        let ctx = v1_setup(a, b, dir.path()).await;
        // Every row below the oracle-lag window (R3E-11), so the limit edge alone decides.
        ctx.tips.set_a_tip(7_000_000 + 2 * super::ORACLE_HORIZON);
        let o = v1_compare(&ctx, "/v1/txs/k:big/coin/10").await;
        assert_eq!(
            o.class,
            class,
            "ours {na}, kda-txs {nb}, class {class}: {:?}",
            o.record.map(|r| (r.note, r.diff))
        );
    }
}

// ---------------------------------------------------------------------------------------------
// FVH-C5b: the oracle-lag rule (R3E-11) explained rows ours lacks (R-P5P9-71 item 1)

/// R-P5P9-71 (1): in the oracle-lag window a row only kda-txs lists is no lag. Ours short by
/// one there is a diff; kda-txs lacking recent rows (the rule's case) and a kda-txs row the
/// node shows is an orphan stay explained, as does a row near A's tip.
#[tokio::test]
async fn c5b_the_lag_window_explains_only_rows_the_oracle_lacks() {
    // Heights 7_000_000 down to 6_999_990; A's tip (its /txs/recent) is the highest, so every
    // row is in the window and rows down to 6_999_995 are near the tip.
    let rows = v1_rows(11, 10);
    let orphan = {
        let mut r = rows[9].clone();
        r["blockHash"] = json!("orphan9");
        r
    };
    let node = || NodeMock {
        canon: HashMap::from([((9, 6_999_991), "b9".to_string())]),
        ..NodeMock::default()
    };
    let cases: [(&str, Vec<Value>, Vec<Value>, &str); 4] = [
        (
            "ours short by one",
            rows[..9].to_vec(),
            rows[..10].to_vec(),
            "diff",
        ),
        (
            "kda-txs lacks recent rows",
            rows[..10].to_vec(),
            rows[3..10].to_vec(),
            "allowlisted",
        ),
        (
            "kda-txs keeps an orphan",
            rows[..10].to_vec(),
            [&rows[..10], &[orphan]].concat(),
            "allowlisted",
        ),
        (
            "near A's tip",
            rows[1..10].to_vec(),
            rows[..10].to_vec(),
            "allowlisted",
        ),
    ];
    for (name, a, b, class) in cases {
        let dir = tempfile::tempdir().unwrap();
        let tip = a.iter().filter_map(|r| r["height"].as_u64()).max().unwrap();
        let ctx = v1_setup_node(
            serde_json::to_vec(&a).unwrap(),
            serde_json::to_vec(&b).unwrap(),
            dir.path(),
            node(),
        )
        .await;
        ctx.tips.set_a_tip(tip.max(7_000_000));
        let o = v1_compare(&ctx, "/v1/txs/k:big/coin/10").await;
        assert_eq!(
            o.class,
            class,
            "{name}: {:?}",
            o.record.map(|r| (r.note, r.diff))
        );
    }
}

/// R-P5P9-71 (1): when the window drops rows, the cut at a full side's lowest height drops
/// only the other side's surplus there. A row the limiting side (kda-txs, full) has at its
/// lowest height and ours (not full) lacks stays a diff; kda-txs lagging and reaching lower
/// (the cut's case) stays explained.
#[tokio::test]
async fn c5b_the_lag_cut_keeps_a_row_only_the_limiting_side_has() {
    let rows = v1_rows(11, 10);
    let cases: [(&str, Vec<Value>, Vec<Value>, &str); 2] = [
        (
            "ours short by one",
            rows[..9].to_vec(),
            rows[..10].to_vec(),
            "diff",
        ),
        (
            "kda-txs lags and reaches lower",
            rows[..10].to_vec(),
            rows[1..11].to_vec(),
            "allowlisted",
        ),
    ];
    for (name, a, b, class) in cases {
        let dir = tempfile::tempdir().unwrap();
        let ctx = v1_setup(
            serde_json::to_vec(&a).unwrap(),
            serde_json::to_vec(&b).unwrap(),
            dir.path(),
        )
        .await;
        // Only height 7_000_000 is in the window, far from A's tip.
        ctx.tips.set_a_tip(7_000_000 + super::ORACLE_HORIZON - 1);
        let o = v1_compare(&ctx, "/v1/txs/k:big/coin/10").await;
        assert_eq!(
            o.class,
            class,
            "{name}: {:?}",
            o.record.map(|r| (r.note, r.diff))
        );
    }
}

/// R-P5P9-71 (1): the node-confirmed rule (R3E-14) cuts two full sides at the higher lowest
/// height. kda-txs's row there that ours lacks (ours holding one more row below) stays a diff.
#[tokio::test]
async fn c5b_the_node_gap_cut_keeps_a_row_only_the_limiting_side_has() {
    let rows = v1_rows(11, 10);
    let a = [&rows[..9], &rows[10..11]].concat();
    let dir = tempfile::tempdir().unwrap();
    let ctx = v1_setup(
        serde_json::to_vec(&a).unwrap(),
        serde_json::to_vec(&rows[..10]).unwrap(),
        dir.path(),
    )
    .await;
    ctx.tips.set_a_tip(7_000_000 + 2 * super::ORACLE_HORIZON);
    let o = v1_compare(&ctx, "/v1/txs/k:big/coin/10").await;
    assert_eq!(o.class, "diff", "{:?}", o.record.map(|r| (r.note, r.diff)));
}

// ---------------------------------------------------------------------------------------------
// FVH-C5b: memory bounds (R-P5P9-70 m-2)

/// R-P5P9-70 (m-2): H6's nested pages count per side against `MERGED_BYTES_CAP`. A cut list
/// whose pages pass it (300 rows of about 120 KiB, 6 MiB pages) is left unread, with the
/// `/~harness-limit/` merged-bytes entry, and its cut page stays a diff.
#[tokio::test]
async fn c5b_h6_nested_pages_count_against_the_merged_bytes_cap() {
    use crate::graphql::{compare_answers_with, parse_op, MERGED_BYTES_PATH};
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let rows: Vec<Value> = (0..300)
        .map(|i| json!([format!("k{i}"), {"cmd": {"meta": {"gasPrice": i as f64 * 1e-9, "pad": "x".repeat(120 << 10)}}}]))
        .collect();
    let lists = json!({ "FAT": rows });
    let calls = Arc::new(Mutex::new(0));
    let au = keyed_list_server(lists.clone(), calls.clone(), 50, None, false).await;
    let bu = keyed_list_server(lists.clone(), calls.clone(), 50, None, false).await;
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
list_page_budget = 40
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
    let op = parse_op(
        "BlocksByHeight",
        "query BlocksByHeight($startHeight: Int!, $endHeight: Int) { blocksFromHeight(startHeight: $startHeight, endHeight: $endHeight) { edges { node { chainId creationTime transactions { totalCount edges { node { cmd { meta { gasPrice } } } } } height coinbase hash canonical } } } }",
        false,
    )
    .unwrap();
    // Ours' cut page is the first 20 rows, the oracle's the last 20.
    let answer = |head: bool| {
        let r = lists["FAT"].as_array().unwrap();
        let idx: Vec<usize> = if head {
            (0..20).collect()
        } else {
            (280..300).collect()
        };
        let tx: Vec<Value> = idx.iter().map(|i| json!({"node": r[*i][1]})).collect();
        crate::graphql::Answer {
            body: json!({"data": {"blocksFromHeight": {"edges": [{"node": {"chainId": 0,
                "hash": "FAT", "height": 4968386,
                "transactions": {"totalCount": 300, "edges": tx}}}]}}}),
            status: 200,
            raw: String::new(),
            ms: Vec::new(),
        }
    };
    let (mut a, mut b) = (answer(true), answer(false));
    let (ta, tb) = (&ctx.targets["a"], &ctx.targets["b"]);
    let (n, entries) = ctx
        .gql_finish_lists(&op, &serde_json::Map::new(), ta, tb, &mut a, &mut b)
        .await;
    let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    assert_eq!((n, paths), (0, vec![MERGED_BYTES_PATH]), "{entries:?}");
    let d = compare_answers_with(&mut a, &mut b, u64::MAX, 10_000, &[], false);
    assert!(!d.is_empty());
}

/// R-P5P9-70 (m-2): an in-memory `/v1/txs` compare with a side over 8 MiB waits for the
/// process-wide permit: while another large compare holds it, this one does not finish; a
/// small one does. Released, the large one finishes. R-P5P9-72 (5): the two positive checks
/// are bounded at 60 s (another test's large compare may take the permit first); the
/// negative check keeps `wait`, since the permit is held through it.
#[tokio::test]
async fn c5b_the_semaphore_serialises_large_v1txs_answers() {
    use std::time::{Duration, Instant};
    let dir = tempfile::tempdir().unwrap();
    // About 9 MiB a side: over the 8 MiB mark, under the 32 MiB cap (in memory).
    let large = serde_json::to_vec(&v1_rows(60, 160_000)).unwrap();
    assert!(large.len() > super::V1TXS_LARGE_BYTES && large.len() < 16 << 20);
    let ctx = v1_setup(large.clone(), large, dir.path()).await;
    let small_dir = tempfile::tempdir().unwrap();
    let small = serde_json::to_vec(&v1_rows(10, 10)).unwrap();
    let small_ctx = v1_setup(small.clone(), small, small_dir.path()).await;
    // The time a large compare takes unhindered (under the permit, released at once).
    let t0 = Instant::now();
    let o = v1_compare(&ctx, "/v1/txs/k:big/coin/60").await;
    assert_eq!(o.class, "allowlisted", "{:?}", o.record.map(|r| r.diff));
    let wait = (t0.elapsed() * 3).max(Duration::from_secs(2));
    // Another large compare holds the permit.
    let held = super::V1TXS_LARGE.acquire().await.unwrap();
    let blocked = tokio::time::timeout(wait, v1_compare(&ctx, "/v1/txs/k:big/coin/60")).await;
    assert!(blocked.is_err(), "a large compare ran beside another");
    let positive = Duration::from_secs(60);
    let o = tokio::time::timeout(positive, v1_compare(&small_ctx, "/v1/txs/k:big/coin/10")).await;
    assert!(o.is_ok(), "a small compare waited for the permit");
    drop(held);
    let o = tokio::time::timeout(positive, v1_compare(&ctx, "/v1/txs/k:big/coin/60")).await;
    assert!(
        o.is_ok(),
        "the large compare did not finish once the permit was free"
    );
}

// ---------------------------------------------------------------------------------------------
// FVH-C6: the oracle-lag window's floor and cut (R-P5P9-72 items 1 and 2)

/// R-P5P9-72 (1), review-fvh-e I-1: when ours holds its limit, a row only kda-txs lists at
/// or below ours' lowest height is ours' limit, not evidence, and goes with the window: the
/// oracle one block behind and tie order at ours' lowest height stay explained. The cut is
/// taken from both sides' lowest heights before the window drops rows, so kda-txs's rows
/// below the window, under ours' page, go too. Ours not full stays a diff.
#[tokio::test]
async fn c6_the_lag_window_floor_is_ours_lowest_height_at_its_limit() {
    // Heights 7_000_000 down to 6_999_985.
    let rows = v1_rows(16, 10);
    let at_9 = |rk: &str| {
        let mut r = rows[9].clone();
        r["requestKey"] = json!(rk);
        r
    };
    // Every row in the window, rows down to 6_999_995 near A's tip.
    let all_in = 7_000_000;
    // The window above 6_999_988: ours' page (down to 6_999_991) lies wholly inside it.
    let below = 6_999_988 + super::ORACLE_HORIZON;
    // (name, ours, kda-txs, A's tip, class).
    type Case = (&'static str, Vec<Value>, Vec<Value>, u64, &'static str);
    let cases: [Case; 5] = [
        (
            "kda-txs one block behind",
            rows[..10].to_vec(),
            rows[1..11].to_vec(),
            all_in,
            "allowlisted",
        ),
        (
            "tie order at ours' lowest height",
            [&rows[..9], &[at_9("rkX")]].concat(),
            [&rows[..9], &[at_9("rkY")]].concat(),
            all_in,
            "allowlisted",
        ),
        (
            "ours not full, short by one",
            rows[..9].to_vec(),
            rows[1..11].to_vec(),
            all_in,
            "diff",
        ),
        (
            "kda-txs reaches below the window",
            rows[..10].to_vec(),
            rows[5..15].to_vec(),
            below,
            "allowlisted",
        ),
        (
            "kda-txs reaches below the window, ours not full",
            rows[..9].to_vec(),
            rows[5..15].to_vec(),
            below,
            "diff",
        ),
    ];
    for (name, a, b, tip, class) in cases {
        let dir = tempfile::tempdir().unwrap();
        let ctx = v1_setup_node(
            serde_json::to_vec(&a).unwrap(),
            serde_json::to_vec(&b).unwrap(),
            dir.path(),
            NodeMock::default(),
        )
        .await;
        ctx.tips.set_a_tip(tip);
        let o = v1_compare(&ctx, "/v1/txs/k:big/coin/10").await;
        assert_eq!(
            o.class,
            class,
            "{name}: {:?}",
            o.record.map(|r| (r.note, r.diff))
        );
    }
}

/// R-P5P9-72 (2), review-fvh-e m-1: `/txs/events` carries no limit, so neither side is cut
/// at its lowest height: ours lacking the lowest row of an otherwise equal answer is a diff,
/// with rows in the oracle-lag window or not.
#[tokio::test]
async fn c6_an_unknown_limit_is_not_full_in_the_lag_cut() {
    let rows = v1_rows(11, 10);
    let path = "/txs/events?modulename=coin&name=TRANSFER&minheight=6999900&maxheight=7000100";
    // 5 of 11 rows in the window (above 6_999_995), or none.
    for tip in [
        6_999_995 + super::ORACLE_HORIZON,
        7_000_000 + 2 * super::ORACLE_HORIZON,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let ctx = bytes_pair_setup(
            serde_json::to_vec(&rows[..10]).unwrap(),
            serde_json::to_vec(&rows).unwrap(),
            dir.path(),
            NodeMock::default(),
            "chainweb_data",
        )
        .await;
        ctx.tips.set_a_tip(tip);
        let o = v1_compare(&ctx, path).await;
        assert_eq!(
            o.class,
            "diff",
            "tip {tip}: {:?}",
            o.record.map(|r| (r.note, r.diff))
        );
    }
}

// ---------------------------------------------------------------------------------------------
// FVH-C6: the first-page fault is ours only (R-P5P9-72 item 3)

/// A GraphQL server answering every request with HTTP 400 and a JSON `errors` body.
async fn gql_400_server() -> String {
    use axum::http::StatusCode;
    let app = Router::new().route(
        "/graphql",
        post(|| async {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"errors": [{"message": "invalid"}]})),
            )
                .into_response()
        }),
    );
    format!("{}graphql", serve(app).await)
}

/// R-P5P9-72 (3), review-fvh-e m-2: the oracle's first page keeps the plain parse, so its
/// HTTP 400 JSON answer against ours' 200 is compared (a diff), not a `b-error`.
#[tokio::test]
async fn c6_an_oracle_first_page_client_error_is_compared() {
    let dir = tempfile::tempdir().unwrap();
    let au = ours_fn_server(|_: &Value| {
        json!({"data": {"transfers": {"totalCount": 0,
            "pageInfo": {"hasNextPage": false, "endCursor": null}, "edges": []}}})
    })
    .await;
    let bu = gql_400_server().await;
    let ctx = gql_ctx_ab(NodeMock::default(), dir.path(), &au, &bu).await;
    let op = h7_op("transfers");
    let vars = h7_vars(&[("accountName", json!("k:x")), ("first", json!(1))]);
    let o = ctx
        .gql_vs_oracle(&ctx.targets["a"], &ctx.targets["b"], &op, &vars, 7_000_000)
        .await;
    assert_eq!(o.class, "diff", "{o:?}");
}

/// R-P5P9-72 (3), review-fvh-e m-2: ours' first-page fault (here HTTP 400 JSON) is the
/// `/~graphql-first-page-fault` diff in the NOT_SUPPORTED lane and in a node lane, not an
/// `a-error`.
#[tokio::test]
async fn c6_ours_first_page_fault_is_a_diff_in_the_node_lanes() {
    let dir = tempfile::tempdir().unwrap();
    let au = gql_400_server().await;
    let ctx = gql_ctx_at(NodeMock::default(), dir.path(), &au).await;
    let ta = &ctx.targets["a"];
    let fault = |o: &super::Outcome, lane: &str| {
        assert_eq!(o.class, "diff", "{lane}: {o:?}");
        let rec = o.record.as_ref().unwrap();
        assert!(
            rec.diff
                .iter()
                .any(|e| e.path == crate::graphql::FIRST_PAGE_FAULT_PATH),
            "{lane}: {rec:?}"
        );
    };
    let ns = crate::graphql::parse_op(
        "p",
        "query($q: [String!]!) { pactQuery(pactQuery: $q) { chainId } }",
        true,
    )
    .unwrap();
    let o = ctx
        .gql_not_supported(ta, &ns, &h7_vars(&[("q", json!(["(+ 1 2)"]))]))
        .await;
    fault(&o, "not-supported");
    let nb = crate::graphql::parse_op(
        "NodeBalance",
        "# kdx: node-balance\nquery NodeBalance($accountName: String!) { fungibleAccount(accountName: $accountName) { chainAccounts { chainId balance } } }",
        false,
    )
    .unwrap();
    let sample = crate::graphql::Sample {
        chain: 0,
        height: 100,
        block_hash: "b".into(),
        request_key: "rk".into(),
        accounts: vec!["k:x".into()],
        header: json!({}),
        meta: json!({}),
        signers: json!([]),
    };
    let o = ctx
        .gql_node_balance(ta, &nb, &h7_vars(&[("accountName", json!("k:x"))]), &sample)
        .await;
    fault(&o, "node-balance");
}

// ---------------------------------------------------------------------------------------------
// FVH-C6: a null leaf is not an unselected field (R-P5P9-72 item 4)

/// R-P5P9-72 (4), review-fvh-e m-3: an ours-only event row whose `parameters` is null is not
/// excused (the checks read a null as "not selected"): it stays a diff. The same row with
/// the node's parameters is explained.
#[tokio::test]
async fn c6_an_ours_only_event_with_null_parameters_stays_a_diff() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let xfer = json!([{"name": "TRANSFER", "module": {"name": "coin"}, "params": ["s", "r", 1.5]}]);
    for rk in ["e1", "e5"] {
        let (k, v) = h7_poll(rk, "B100", 100, xfer.clone());
        node.polls.insert(k, v);
    }
    h7_back_cmds(&mut node, "2024-01-01T10:00:00Z", &[]);
    let ctx = gql_ctx(node, dir.path()).await;
    let ev = |rk: &str, params: Value| {
        json!({"cursor": "c", "node": {"name": "TRANSFER", "qualifiedName": "coin.TRANSFER",
            "orderIndex": 0, "parameters": params, "chainId": "0",
            "block": {"hash": "B100", "height": 100}, "requestKey": rk}})
    };
    let e1 = ev("e1", json!(r#"["s","r",1.5]"#));
    for (e5, explained) in [
        (ev("e5", json!(r#"["s","r",1.5]"#)), true),
        (ev("e5", Value::Null), false),
    ] {
        let mut a = h7_page("events", false, vec![e1.clone(), e5.clone()]);
        let mut b = h7_page("events", false, vec![e1.clone()]);
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
        assert_eq!(only_hits(&d) && !d.is_empty(), explained, "{e5}: {d:?}");
    }
    // Under a tx's `result` and `cmd` a null is compared as a value, so it is no refusal.
    use crate::graphql_window::{unchecked_field, Kind};
    let tx = json!({"result": {"continuation": null}, "cmd": {"payload": {"data": null,
        "code": "(+ 1 2)"}}});
    assert_eq!(unchecked_field(Some(Kind::Tx), &tx), None);
    let ev = json!({"name": "TRANSFER", "parameters": null});
    assert_eq!(
        unchecked_field(Some(Kind::Event), &ev).as_deref(),
        Some("parameters")
    );
}

// ---------------------------------------------------------------------------------------------
// FVH-C7: the lag-window floor is for the single-page route only (R-P5P9-73 I-1)

/// A server answering `rows` in pages of `size`: no `next` is page 0, `next=n` is page n. Each
/// page with a successor sets `Chainweb-Next`; with `pending` the last page sets one too, as
/// chainweb-data does when more rows exist than the pages followed.
async fn paged_server(rows: Vec<Value>, size: usize, pending: bool) -> String {
    let pages: Vec<Vec<Value>> = rows.chunks(size).map(<[Value]>::to_vec).collect();
    let app = Router::new().fallback(move |uri: Uri| {
        let pages = pages.clone();
        async move {
            let n: usize = uri
                .query()
                .and_then(|q| {
                    url::form_urlencoded::parse(q.as_bytes())
                        .find(|(k, _)| k == "next")
                        .and_then(|(_, v)| v.parse().ok())
                })
                .unwrap_or(0);
            let body = serde_json::to_vec(pages.get(n).unwrap_or(&Vec::new())).unwrap();
            let mut r = ([("content-type", "application/json")], body).into_response();
            if n + 1 < pages.len() || pending {
                r.headers_mut()
                    .insert("chainweb-next", (n + 1).to_string().parse().unwrap());
            }
            r
        }
    });
    serve(app).await
}

/// One `/txs/events` compare of paged answers (`chainweb_data` oracle, three pages followed),
/// A's tip 7_000_000 so every row lies in the lag window.
async fn paged_compare(
    (a, a_size, a_pending): (Vec<Value>, usize, bool),
    (b, b_size, b_pending): (Vec<Value>, usize, bool),
    limit: Option<usize>,
) -> (String, String) {
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let au = paged_server(a, a_size, a_pending).await;
    let bu = paged_server(b, b_size, b_pending).await;
    let ctx = urls_pair_setup(&nu, &au, &bu, dir.path(), "chainweb_data", 3).await;
    ctx.tips.set_a_tip(7_000_000);
    let lim = limit.map_or(String::new(), |l| format!("&limit={l}"));
    let path = format!(
        "/txs/events?modulename=coin&name=TRANSFER&minheight=6999900&maxheight=7000100{lim}"
    );
    let o = v1_compare(&ctx, &path).await;
    let note = format!("{:?}", o.record.map(|r| (r.note, r.diff)));
    (o.class.to_string(), note)
}

/// P1: ours answers 8 rows on one page and says it is complete; the oracle is cut after four
/// pages of three. Ours lacks the 3 rows at 6_999_992 to 6_999_990: a diff, not lag.
#[tokio::test]
async fn c7_a_page_cut_does_not_explain_rows_ours_lacks() {
    let rows = v1_rows(16, 10);
    let (class, note) = paged_compare(
        (rows[..8].to_vec(), 8, false),
        (rows[..12].to_vec(), 3, true),
        None,
    )
    .await;
    assert_eq!(class, "diff", "{note}");
}

/// P2: `&limit=3`, both complete over merged pages (ours 8 rows in 3 pages, the oracle 11 in
/// 4). Ours lacks the lowest 3 rows: a diff, not ours' limit edge.
#[tokio::test]
async fn c7_a_known_limit_over_merged_pages_does_not_floor_the_window() {
    let rows = v1_rows(16, 10);
    let (class, note) = paged_compare(
        (rows[..8].to_vec(), 3, false),
        (rows[..11].to_vec(), 3, false),
        Some(3),
    )
    .await;
    assert_eq!(class, "diff", "{note}");
}

/// P3: the oracle one block behind (it lacks the newest row), both cut after four pages.
#[tokio::test]
async fn c7_oracle_one_block_behind_both_truncated_is_allowlisted() {
    let rows = v1_rows(16, 10);
    let (class, note) = paged_compare(
        (rows[..12].to_vec(), 3, true),
        (rows[1..13].to_vec(), 3, true),
        None,
    )
    .await;
    assert_eq!(class, "allowlisted", "{note}");
}

/// P4: the oracle one block behind, both complete at `limit=3`.
#[tokio::test]
async fn c7_oracle_one_block_behind_both_complete_is_allowlisted() {
    let rows = v1_rows(16, 10);
    let (class, note) = paged_compare(
        (rows[..11].to_vec(), 3, false),
        (rows[1..11].to_vec(), 3, false),
        Some(3),
    )
    .await;
    assert_eq!(class, "allowlisted", "{note}");
}

/// P5: ours cut after four pages, the oracle complete.
#[tokio::test]
async fn c7_ours_truncated_oracle_complete_is_allowlisted() {
    let rows = v1_rows(16, 10);
    let (class, note) = paged_compare(
        (rows[..12].to_vec(), 3, true),
        (rows[..11].to_vec(), 3, false),
        None,
    )
    .await;
    assert_eq!(class, "allowlisted", "{note}");
}

/// P0: equal answers, both cut after four pages (the page cut alone is allowlisted).
#[tokio::test]
async fn c7_equal_page_cut_answers_are_allowlisted() {
    let rows = v1_rows(16, 10);
    let (class, note) = paged_compare(
        (rows[..12].to_vec(), 3, true),
        (rows[..12].to_vec(), 3, true),
        None,
    )
    .await;
    assert_eq!(class, "allowlisted", "{note}");
}

// ---------------------------------------------------------------------------------------------
// 5E regression through the merged lag block (R-P5P9-74): `clamp_lists` and its unit test are
// gone; these two cases keep what it covered.

/// One `/v1/txs`-shaped row at height `h`; the same height gives the same row on both sides.
fn row_at(h: u64) -> Value {
    json!({"requestKey": format!("rk{h}"), "chain": h % 20, "height": h,
        "blockHash": format!("b{h}"), "amount": 1.5, "code": "x", "events": [], "gasLimit": 600})
}

/// A kda-indexer replica: `/txs/recent` shows one row at its `tip`, every other path `rows`.
async fn replica_server(tip: u64, rows: Vec<Value>) -> String {
    let app = Router::new().fallback(move |uri: Uri| {
        let body = if uri.path() == "/txs/recent" {
            json!([row_at(tip)])
        } else {
            Value::Array(rows.clone())
        };
        async move { Json(body) }
    });
    serve(app).await
}

/// 5E (ac2013f's case), `/v1/txs/.../5` on a replica pair, tips 7_000_030 and 7_000_000 (clamp
/// 6_999_995). One side's full page, 7_000_030 down to 6_999_998, lies wholly above the clamp,
/// the other side's reaches below it. Both held their limit, so nothing below the first page's
/// lowest row is comparable: no diff (before ac2013f the lower rows were reported as missing
/// on the first side). Both orientations: ours as the page above the clamp, and as the one below.
#[tokio::test]
async fn r74_v1_txs_a_full_page_wholly_above_the_clamp_bounds_the_other_side() {
    const B: u64 = 6_999_900;
    let above: Vec<Value> = [130, 128, 126, 100, 98].map(|x| row_at(B + x)).to_vec();
    let below: Vec<Value> = [100, 98, 90, 88, 86].map(|x| row_at(B + x)).to_vec();
    for (ours_above, ours, oracle) in [
        (false, below.clone(), above.clone()),
        (true, above.clone(), below.clone()),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let nu = node_server(NodeMock::default()).await;
        let (ta, tb) = if ours_above {
            (B + 130, B + 100)
        } else {
            (B + 100, B + 130)
        };
        let (au, bu) = (
            replica_server(ta, ours).await,
            replica_server(tb, oracle).await,
        );
        let ctx = urls_pair_setup(&nu, &au, &bu, dir.path(), "kda_indexer_compat", 0).await;
        let o = v1_compare(&ctx, "/v1/txs/k:big/coin/5").await;
        assert!(
            o.class != "diff" && o.class != "a-error",
            "ours above: {ours_above}: {} {:?}",
            o.class,
            o.record.map(|r| (r.note, r.diff))
        );
    }
}

/// 5E on a paginated route: `/txs/account`, the oracle cut after four pages of three rows
/// (7_000_000 down to 6_999_989) with every page above the clamp (6_999_985), and ours one
/// complete page that reaches below the oracle's last page. Ours' rows below it are not
/// diffs: the page cut covers them, and the rows above the clamp are dropped on both sides.
#[tokio::test]
async fn r74_account_oracle_cut_with_every_page_above_the_clamp_is_no_diff() {
    let rows = v1_rows(20, 10);
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let au = paged_server(rows.clone(), 20, false).await;
    let bu = paged_server(rows[..12].to_vec(), 3, true).await;
    let ctx = urls_pair_setup(&nu, &au, &bu, dir.path(), "chainweb_data", 3).await;
    ctx.tips.set_a_tip(6_999_985 + super::ORACLE_HORIZON);
    let o = v1_compare(&ctx, "/txs/account/k:big?token=coin").await;
    assert!(
        o.class != "diff" && o.class != "a-error",
        "{} {:?}",
        o.class,
        o.record.map(|r| (r.note, r.diff))
    );
}

// ---------------------------------------------------------------------------------------------
// R-P5P9-75, harness round D1 (CG-4's identity and structure classes)

/// A 43-character coinbase key (a parent block hash) from a short tag.
fn d1_key(tag: &str) -> String {
    format!("{tag:_<43}")
}

/// A node holding block `B<h>` at height `h` on chain 0 whose parent is `parent` and whose
/// coinbase (key `cb_key`) pays `miner` 1.5.
fn d1_coinbase_node(h: u64, parent: &str, cb_key: &str) -> NodeMock {
    let mut node = NodeMock::default();
    node.headers.insert(
        (0, h),
        vec![json!({"hash": format!("B{h}"), "height": h, "chainId": 0,
            "parent": parent, "payloadHash": format!("P{h}")})],
    );
    let cb = json!({"reqKey": cb_key, "events": [{"name": "TRANSFER",
        "module": {"name": "coin"}, "params": ["", "miner", 1.5]}]})
    .to_string();
    node.payloads.insert(
        format!("P{h}"),
        json!({"coinbase": kdx_types::hash::b64url_encode(cb.as_bytes())}),
    );
    node
}

/// R-P5P9-86 C: the node's coinbase key in its two eras: 60 characters of base64url over the
/// JSON-quoted parent hash, or 58 characters of base64 over the bare parent hash.
fn c86_node_key(parent: &str, era60: bool) -> String {
    if era60 {
        kdx_types::hash::b64url_encode(format!("\"{parent}\"").as_bytes())
    } else {
        crate::graphql::base64_std(parent.as_bytes())
            .trim_end_matches('=')
            .to_string()
    }
}

/// R-P5P9-86 C: a row whose key is not a 43-character hash is a coinbase row, paired by its
/// place and settled on the coinbase path only (the canonical header at its height and the
/// payload's coinbase output reqKey), never `/poll`. Ours' key must be the node's byte for
/// byte, in either era; the oracle's is excused only as that header's bare parent hash or as
/// base64 of its own hash (`/~graphql-coinbase-oracle-key-form`). An oracle key that decodes
/// to another hash, or ours differing from the node, stays a diff.
#[tokio::test]
async fn e1_c_coinbase_keys_in_both_node_eras_and_both_oracle_forms() {
    let dir = tempfile::tempdir().unwrap();
    let parent = d1_key("Y_oq3-w1");
    let row = |rk: &str| h7_xrow(rk, 100, json!(1.5), Some(("", "miner")));
    let run = |node: NodeMock, a: Vec<Value>, b: Vec<Value>| {
        let dir = dir.path().to_path_buf();
        async move {
            let calls = node.calls.clone();
            let ctx = gql_ctx(node, &dir).await;
            let mut a = h7_page("transfers", false, a);
            let mut b = h7_page("transfers", false, b);
            let out = ctx
                .gql_settle(
                    &ctx.targets["a"],
                    &serde_json::Map::new(),
                    &mut a,
                    &mut b,
                    7_000_000,
                    false,
                )
                .await;
            let polls = calls
                .lock()
                .unwrap()
                .iter()
                .filter(|c| c.contains("poll"))
                .count();
            (out, polls)
        }
    };
    let form = |d: &[crate::report::DiffEntry]| {
        d.iter()
            .any(|e| e.path == crate::graphql_window::COINBASE_FORM_PATH)
    };
    // The oracle's base64 of the block's own hash.
    let hash = d1_key("B100_own");
    let own = crate::graphql::base64_std(hash.as_bytes())
        .trim_end_matches('=')
        .to_string();
    let node = |k: &str| {
        let mut n = d1_coinbase_node(100, &parent, k);
        n.headers.get_mut(&(0, 100)).unwrap()[0]["hash"] = json!(hash);
        n
    };
    for era60 in [true, false] {
        let k = c86_node_key(&parent, era60);
        assert_eq!(k.len(), if era60 { 60 } else { 58 });
        for oracle in [parent.clone(), own.clone()] {
            let ((d, note), polls) = run(node(&k), vec![row(&k)], vec![row(&oracle)]).await;
            assert!(
                only_hits(&d) && form(&d),
                "era60 {era60} oracle {oracle}: {d:?} {note:?}"
            );
            assert_eq!(polls, 0, "a coinbase row is never polled");
        }
    }
    let k = c86_node_key(&parent, true);
    // An oracle key that decodes to another hash than the header's own: a diff.
    let other = crate::graphql::base64_std(d1_key("Z_other").as_bytes())
        .trim_end_matches('=')
        .to_string();
    let ((d, _), polls) = run(node(&k), vec![row(&k)], vec![row(&other)]).await;
    assert!(!only_hits(&d) && !form(&d), "{d:?}");
    assert_eq!(polls, 0);
    // A bare hash that is not the header's parent: a diff.
    let ((d, _), _) = run(node(&k), vec![row(&k)], vec![row(&d1_key("Z_other"))]).await;
    assert!(!only_hits(&d) && !form(&d), "{d:?}");
    // Ours' key in the other era's encoding than the node's: a diff, though the oracle's
    // bare parent hash is that header's.
    let ((d, _), _) = run(
        node(&k),
        vec![row(&c86_node_key(&parent, false))],
        vec![row(&parent)],
    )
    .await;
    assert!(!only_hits(&d) && !form(&d), "{d:?}");
}

/// Runs H7 and the diff on one pair of `transfers` pages (no variables).
async fn d1_settle(
    ctx: &Ctx,
    a: Vec<Value>,
    b: Vec<Value>,
    cut: (bool, bool),
) -> (Vec<crate::report::DiffEntry>, Option<String>) {
    let mut a = h7_page("transfers", cut.0, a);
    let mut b = h7_page("transfers", cut.1, b);
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

/// R-P5P9-75 A: an ours-only transfer row's `crossChainTransfer` is checked against the node:
/// null iff the node's transaction has no TRANSFER_XCHAIN for that transfer, else the target
/// chain and receiver of that event. A TRANSFER_XCHAIN row with a null `crossChainTransfer`,
/// or one that names another chain, stays a diff.
#[tokio::test]
async fn d1_a_cross_chain_transfer_is_checked_against_the_nodes_xchain_event() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for rk in ["x0", "x1"] {
        let (k, v) = h7_xfer_poll(rk, "B100", 100, "s", "r", json!(1.5));
        node.polls.insert(k, v);
    }
    let ev = |name: &str, p: Value| json!({"name": name, "module": {"name": "coin"}, "params": p});
    let (k, v) = h7_poll(
        "x2",
        "B100",
        100,
        json!([
            ev("TRANSFER", json!(["s", "", 1.5])),
            ev("TRANSFER_XCHAIN", json!(["s", "r", 1.5, "1"]))
        ]),
    );
    node.polls.insert(k, v);
    let ctx = gql_ctx(node, dir.path()).await;
    let row = |rk: &str, to: &str, x: Value| {
        let mut e = h7_xrow(rk, 100, json!(1.5), Some(("s", to)));
        e["node"]["crossChainTransfer"] = x;
        e
    };
    let x0 = row("x0", "r", Value::Null);
    let settle = |ours: Value| {
        d1_settle(
            &ctx,
            vec![x0.clone(), ours],
            vec![x0.clone()],
            (false, false),
        )
    };
    // No TRANSFER_XCHAIN in the node's transaction: ours' null is the node's.
    let (d, note) = settle(row("x1", "r", Value::Null)).await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    // The transfer's TRANSFER_XCHAIN to chain 1 and r: ours names both.
    let xc = json!({"__typename": "Transfer", "chainId": "1", "receiverAccount": "r"});
    let (d, note) = settle(row("x2", "", xc)).await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    // A TRANSFER_XCHAIN row with crossChainTransfer null: a diff.
    let (d, note) = settle(row("x2", "", Value::Null)).await;
    assert!(!only_hits(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("crossChainTransfer"));
    // Another target chain: a diff.
    let xc = json!({"chainId": "2", "receiverAccount": "r"});
    let (d, _) = settle(row("x2", "", xc)).await;
    assert!(!only_hits(&d), "{d:?}");
    // A link where the node's transaction has none: a diff.
    let xc = json!({"chainId": "1", "receiverAccount": "r"});
    let (d, _) = settle(row("x1", "r", xc)).await;
    assert!(!only_hits(&d), "{d:?}");
}

/// R-P5P9-75 B: an ours-only tx row's `result.block.canonical` is accepted when it equals
/// H7's own verdict: the node places the row in the block that is the canonical one at its
/// height on the cut's branch. `false` against that verdict, or `true` for a block the cut's
/// branch does not hold at the height, stays a diff.
#[tokio::test]
async fn d1_b_result_block_canonical_is_h7s_own_verdict() {
    let dir = tempfile::tempdir().unwrap();
    let tx = |rk: &str, canonical: Value| {
        let id = crate::graphql::base64_std(format!("Transaction:[\"B100\",\"{rk}\"]").as_bytes());
        json!({"cursor": "c", "node": {"id": id, "hash": rk, "result": {"height": 100,
            "block": {"hash": "B100", "chainId": 0, "height": 100, "canonical": canonical}}}})
    };
    let node = |branch: Option<&str>| {
        let mut n = NodeMock::default();
        for rk in ["t0", "t1"] {
            let (k, v) = h7_poll(rk, "B100", 100, json!([]));
            n.polls.insert(k, v);
        }
        n.headers.insert(
            (0, 100),
            vec![json!({"hash": "B100", "height": 100, "chainId": 0, "payloadHash": "P100"})],
        );
        if let Some(b) = branch {
            n.branch.insert(
                (0, 100),
                json!({"hash": b, "height": 100, "chainId": 0, "payloadHash": "PX"}),
            );
        }
        n
    };
    let run = |n: NodeMock, ours: Value| {
        let dir = dir.path().to_path_buf();
        async move {
            let ctx = gql_ctx(n, &dir).await;
            let t0 = tx("t0", json!(true));
            let mut a = h7_page("transactions", false, vec![t0.clone(), ours]);
            let mut b = h7_page("transactions", false, vec![t0]);
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
    let (d, note) = run(node(None), tx("t1", json!(true))).await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    // Ours says false where the node places the row on the cut's branch: a diff.
    let (d, note) = run(node(None), tx("t1", json!(false))).await;
    assert!(!only_hits(&d), "{d:?}");
    assert!(note.unwrap_or_default().contains("canonical"));
    // The cut's branch holds another block at 100: ours' true is not H7's verdict.
    let (d, _) = run(node(Some("X100")), tx("t1", json!(true))).await;
    assert!(!only_hits(&d), "{d:?}");
}

/// [`gql_ctx`] with the oracle-pair field rule `transfers.amount = oracle_f64_rounded`.
async fn d1_ctx_f64(node: NodeMock, out: &std::path::Path) -> Ctx {
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
field_rules = [{{ field = "transfers.amount", rule = "oracle_f64_rounded" }}]
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
    Ctx::new(cfg).await.unwrap()
}

/// R-P5P9-75 D: rows without an index match as a multiset after the ruled field matchers
/// (here `oracle_f64_rounded`): ours' exact amount pairs with the oracle's f64-rounded one
/// (CG-4 #136, #348). The node-confirmed duplicate excuse applies to rows without an index:
/// the oracle lists a transfer twice that the node's transaction and ours hold once (#84).
/// An amount that is not the f64 rounding, and an oracle copy the node also holds, stay diffs.
#[tokio::test]
async fn d1_d_multiset_identity_runs_the_ruled_matchers_and_the_duplicate_excuse() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let exact = "65913.172664045997";
    let (k, v) = h7_xfer_poll("f", "B100", 100, "s", "r", json!({ "decimal": exact }));
    node.polls.insert(k, v);
    let (k, v) = h7_xfer_poll("u", "B100", 100, "s", "r", json!(1.0));
    node.polls.insert(k, v);
    let ev = json!({"name": "TRANSFER", "module": {"name": "coin"}, "params": ["s", "r", 2.0]});
    let (k, v) = h7_poll("w", "B100", 100, json!([ev.clone(), ev]));
    node.polls.insert(k, v);
    let ctx = d1_ctx_f64(node, dir.path()).await;
    let x = |rk: &str, a: &str| h7_xrow(rk, 100, json!(a), Some(("s", "r")));
    // The oracle's amount is ours' as an f64.
    let (d, note) = d1_settle(
        &ctx,
        vec![x("f", exact)],
        vec![x("f", "65913.172664046")],
        (false, false),
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    // The oracle lists u twice; the node's transaction and ours hold it once.
    let (d, note) = d1_settle(
        &ctx,
        vec![x("u", "1.0")],
        vec![x("u", "1.0"), x("u", "1.0")],
        (false, false),
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    assert!(format!("{d:?}").contains("duplicate"), "{d:?}");
    // Not the f64 rounding of ours (rows selecting the amount alone, so E does not apply).
    let y = |a: &str| h7_xrow("f", 100, json!(a), None);
    let (d, _) = d1_settle(&ctx, vec![y(exact)], vec![y("65913.17")], (false, false)).await;
    assert!(!only_hits(&d), "{d:?}");
    // The node holds w twice, ours once: the oracle's second copy is a row ours lacks.
    let (d, _) = d1_settle(
        &ctx,
        vec![x("w", "2.0")],
        vec![x("w", "2.0"), x("w", "2.0")],
        (false, false),
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
}

/// R-P5P9-75 F: transfer rows that select a `requestKey` but no `orderIndex`, block or
/// transfer field (Transfers-8c8a0f56) get their identity by request-key multiset under the
/// `transfers` root, so H7 runs; a row ours lacks stays a diff. When `prepare` fails, the
/// record always says "H7 not applied" with why.
#[tokio::test]
async fn d1_f_key_only_transfer_rows_get_a_request_key_identity_and_a_note() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for (rk, h) in [("u", 101), ("v", 100), ("w", 100)] {
        let (k, v) = h7_xfer_poll(rk, &format!("B{h}"), h, "s", "r", json!(1.5));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let r = |rk: &str| json!({"cursor": "c", "node": {"chainId": "0", "requestKey": rk}});
    // Paired by request key; ours' v is a node-confirmed oracle gap.
    let (d, note) = d1_settle(&ctx, vec![r("u"), r("v")], vec![r("u")], (false, false)).await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    assert!(
        d.iter()
            .any(|e| e.path == crate::graphql_window::WINDOW_PATH),
        "{d:?}"
    );
    // An oracle row the node holds inside our complete list: a diff.
    let (d, _) = d1_settle(
        &ctx,
        vec![r("u"), r("v")],
        vec![r("v"), r("w"), r("u")],
        (false, false),
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
    // A row whose identity cannot be decided: H7 says so.
    let bare = json!({"cursor": "c", "node": {"amountText": "1"}});
    let (_, note) = d1_settle(&ctx, vec![bare, r("u")], vec![r("u")], (false, false)).await;
    assert!(note.unwrap_or_default().starts_with("H7 not applied"));
}

/// R-P5P9-75 G: a nested list whose rows select no key (BlocksByHeight's
/// `transactions { cmd { meta { gasPrice } } }`, here with each row's `creationTime` so the
/// tie sort leaves the order alone) compares as a multiset of the selected values
/// per parent, only when both sides' lists are complete with equal `totalCount`; otherwise it
/// is `/~graphql-h6-unfinished` with why. A value one side lacks stays a diff.
#[test]
fn d1_g_unkeyed_nested_lists_compare_as_multisets_only_when_complete() {
    use crate::graphql::{compare_answers_with, Answer};
    let page = |total: u64, prices: &[&str]| {
        let edges: Vec<Value> = prices
            .iter()
            .map(|p| {
                // Each row its own creationTime, so the tie sort does not order them.
                let ct = format!("2026-01-01T00:00:0{}Z", &p[..1]);
                json!({"node": {"cmd": {"meta": {"creationTime": ct, "gasPrice": p}}}})
            })
            .collect();
        Answer {
            body: json!({"data": {"completedBlockHeights": {"pageInfo": {"hasNextPage": false},
                "edges": [{"node": {"hash": "B1", "height": 1, "chainId": 0,
                    "transactions": {"totalCount": total, "edges": edges}}}]}}}),
            status: 200,
            raw: String::new(),
            ms: Vec::new(),
        }
    };
    let run = |a: Answer, b: Answer| {
        let (mut a, mut b) = (a, b);
        compare_answers_with(&mut a, &mut b, u64::MAX, 10_000, &[], false)
    };
    let unfinished = |d: &[crate::report::DiffEntry]| {
        d.iter()
            .any(|e| e.path == crate::graphql_scope::UNFINISHED_PATH)
    };
    // Complete on both sides, equal totalCount, another order: explained.
    let d = run(page(2, &["1e-8", "2e-8"]), page(2, &["2e-8", "1e-8"]));
    assert!(only_hits(&d) && !d.is_empty(), "{d:?}");
    assert!(
        d.iter()
            .any(|e| e.path == crate::graphql_scope::NESTED_MULTISET_PATH),
        "{d:?}"
    );
    // A value the oracle's block lacks: a diff.
    let d = run(page(2, &["1e-8", "2e-8"]), page(2, &["2e-8", "3e-8"]));
    assert!(!only_hits(&d), "{d:?}");
    // Cut on both sides (totalCount above the rows): unfinished, with why.
    let d = run(page(3, &["1e-8", "2e-8"]), page(3, &["2e-8", "1e-8"]));
    assert!(unfinished(&d), "{d:?}");
    // Unequal totalCount: unfinished.
    let d = run(
        page(2, &["1e-8", "2e-8"]),
        page(3, &["2e-8", "1e-8", "3e-8"]),
    );
    assert!(unfinished(&d), "{d:?}");
}

/// R-P5P9-75 E and M. E: an oracle-only transfer row whose request key's canonical
/// transaction on the node holds no TRANSFER with its (sender, receiver, amount) is off the
/// node's chain (CG-4 #23: another miner's gas row): explained as `oracle-row-not-in-node-tx`.
/// The node holding the transfer, or no canonical transaction for the key, stays a diff.
/// M: an oracle row whose block is not the node's for the key, at another height, is an
/// orphan by block; ours must place the row at the node's block.
#[tokio::test]
async fn d1_e_m_oracle_rows_off_the_nodes_tx_or_block() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for (rk, to) in [("x0", "r"), ("e", "r"), ("m", "r")] {
        let (k, v) = h7_xfer_poll(rk, "B100", 100, "s", to, json!(1.0));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let x = |rk: &str, to: &str, amount: f64| h7_xrow(rk, 100, json!(amount), Some(("s", to)));
    let x0 = x("x0", "r", 1.0);
    // E: the oracle's gas row to another miner, which the node's transaction of e lacks.
    let (d, note) = d1_settle(
        &ctx,
        vec![x0.clone()],
        vec![x0.clone(), x("e", "miner2", 0.0000323)],
        (false, false),
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    assert!(
        format!("{d:?}").contains("oracle-row-not-in-node-tx"),
        "{d:?}"
    );
    // The node holds e's transfer: a row ours lacks, a diff.
    let (d, _) = d1_settle(
        &ctx,
        vec![x0.clone()],
        vec![x0.clone(), x("e", "r", 1.0)],
        (false, false),
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
    // No canonical transaction for the key on the node: a diff.
    let (d, _) = d1_settle(
        &ctx,
        vec![x0.clone()],
        vec![x0.clone(), x("q", "miner2", 0.5)],
        (false, false),
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
    // M: the oracle places m in X90 at height 90; the node's block for m is B100.
    let at = |b: &str, h: u64| {
        let mut e = h7_xrow("m", h, json!(1.0), Some(("s", "r")));
        e["node"]["blockHash"] = json!(b);
        e
    };
    let (d, note) = d1_settle(
        &ctx,
        vec![at("B100", 100)],
        vec![at("X90", 90)],
        (false, false),
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    assert!(format!("{d:?}").contains("orphan"), "{d:?}");
    // Ours places m in X90 too: not the node's block, a diff.
    let (d, _) = d1_settle(
        &ctx,
        vec![x0.clone(), at("X90", 90)],
        vec![x0.clone()],
        (false, false),
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
}

/// R-P5P9-75 H: our own QTransactionsByPublicKey selects each row's block height, chain and
/// hash, so H7 can place its rows (CG-4: 6 records had only `id hash`).
#[test]
fn d1_h_transactions_by_public_key_selects_the_block_to_place_rows() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/graphql");
    let text = std::fs::read_to_string(dir.join("QTransactionsByPublicKey.graphql")).unwrap();
    let op = crate::graphql::parse_op("QTransactionsByPublicKey", &text, false).unwrap();
    assert!(
        op.query.contains("block { hash height chainId }"),
        "{}",
        op.query
    );
    // A row of that shape is placed: height, chain and block.
    let e = json!({"node": {"id": "x", "hash": "rk", "result": {"height": 7,
        "block": {"hash": "B7", "height": 7, "chainId": 3}}}});
    let id = crate::graphql_window::row_id(&e);
    assert_eq!(
        (id.height, id.chain, id.block.as_deref()),
        (Some(7), Some(3), Some("B7"))
    );
}

// ---------------------------------------------------------------------------------------
// FVH-D2 (R-P5P9-75, the "Harness, FVH-D2" part)
// ---------------------------------------------------------------------------------------

/// R-P5P9-75 item 0 (E's leak, the ~06:25Z ruling): E's node match runs the ruled amount
/// matchers first. Ours lacks a real node transfer and the oracle lists it with its
/// f64-rounded amount: the oracle row is the node's transfer, so it stays a diff.
#[tokio::test]
async fn d2_0_e_matches_an_f64_rounded_oracle_amount_before_calling_the_row_off_chain() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let exact = "65913.172664045997";
    for (rk, amount) in [
        ("x0", json!(1.0)),
        ("e", json!({ "decimal": exact })),
        ("en", serde_json::from_str::<Value>(exact).unwrap()),
    ] {
        let (k, v) = h7_xfer_poll(rk, "B100", 100, "s", "r", amount);
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let x0 = h7_xrow("x0", 100, json!(1.0), Some(("s", "r")));
    for rk in ["e", "en"] {
        // The oracle's f64-rounded amount of the node's real transfer; ours lacks the row.
        let rounded = h7_xrow(rk, 100, json!("65913.172664046"), Some(("s", "r")));
        let (d, note) = d1_settle(
            &ctx,
            vec![x0.clone()],
            vec![x0.clone(), rounded],
            (false, false),
        )
        .await;
        assert!(!only_hits(&d), "{rk}: {d:?} {note:?}");
        assert!(
            !format!("{d:?}").contains(crate::graphql_window::NOT_IN_NODE_TX),
            "{rk}: {d:?}"
        );
    }
    // Negative: an oracle amount that is no rounding of the node's (another value) is still
    // off the node's transaction.
    let wrong = h7_xrow("e", 100, json!("65913.1726"), Some(("s", "r")));
    let (d, note) = d1_settle(
        &ctx,
        vec![x0.clone()],
        vec![x0.clone(), wrong],
        (false, false),
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
}

fn d2_answer(body: Value) -> crate::graphql::Answer {
    crate::graphql::Answer {
        body,
        status: 200,
        raw: String::new(),
        ms: Vec::new(),
    }
}

fn d2_shipped() -> crate::allowlist::Allowlist {
    crate::allowlist::Allowlist::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml"),
    )
    .unwrap()
}

/// R-P5P9-75 I, as R-P5P9-86 TC leaves it: the operation's entry stays (no removals), but
/// `oracle_counts_heights` no longer takes ours' count without the walk, below ours or above;
/// a nested count is never the root's.
#[test]
fn d2_i_oracle_counts_heights_only_below_ours_on_transactions_9e58b95d() {
    use crate::config::{FieldRule, FieldRuleKind};
    use crate::graphql_norm::ORACLE_COUNTS_HEIGHTS_PATH;
    let rules = [FieldRule::new(
        "transactions.totalCount",
        FieldRuleKind::OracleCountsHeights,
    )];
    let conn = |n: u64| {
        json!({"data": {"transactions": {"totalCount": n, "edges": [],
            "pageInfo": {"hasNextPage": true}}}})
    };
    let cmp = |a: u64, b: u64| {
        crate::graphql::compare_answers_with(
            &mut d2_answer(conn(a)),
            &mut d2_answer(conn(b)),
            50,
            10_000,
            &rules,
            false,
        )
    };
    let al = d2_shipped();
    // The operation's name comes from its entry (no capture stem in the sources, M1).
    let op = al
        .entries
        .iter()
        .find(|e| e.id == "graphql-r75-oracle-counts-heights")
        .and_then(|e| e.request_contains.clone())
        .unwrap();
    assert!(op.len() > "graphql:".len(), "{op}");
    // R-P5P9-86 TC removed I's "oracle below ours" condition: the count difference stays in
    // the diff, above or below ours, for the count walk to settle on the node.
    for (x, y) in [(4, 1), (4, 5)] {
        let d = cmp(x, y);
        assert!(
            d.iter().any(|e| e.path == "/data/transactions/totalCount"),
            "{d:?}"
        );
        assert!(d.iter().all(|e| e.path != ORACLE_COUNTS_HEIGHTS_PATH));
    }
    assert!(cmp(4, 4).is_empty());
    // A nested count is not the root's.
    let nested =
        |n: u64| json!({"data": {"block": {"transactions": {"totalCount": n, "edges": []}}}});
    let d = crate::graphql::compare_answers_with(
        &mut d2_answer(nested(4)),
        &mut d2_answer(nested(1)),
        50,
        10_000,
        &rules,
        false,
    );
    assert!(
        d.iter().all(|e| e.path != ORACLE_COUNTS_HEIGHTS_PATH),
        "{d:?}"
    );
    assert!(!d.is_empty());
}

/// A node with one block (BJ at height 60, payload PJ) holding the commands `cmds`.
fn d2_cmd_node(cmds: &[(&str, Value)]) -> NodeMock {
    use kdx_types::hash::b64url_encode;
    let mut node = NodeMock::default();
    let txs: Vec<Value> = cmds
        .iter()
        .map(|(rk, cmd)| {
            let w = json!({"hash": rk, "sigs": [], "cmd": cmd.to_string()}).to_string();
            json!([b64url_encode(w.as_bytes()), b64url_encode(b"{}")])
        })
        .collect();
    node.payloads
        .insert("PJ".into(), json!({ "transactions": txs }));
    node.headers.insert(
        (0, 60),
        vec![json!({"hash": "BJ", "height": 60, "payloadHash": "PJ"})],
    );
    for (rk, _) in cmds {
        let (k, v) = h7_poll(rk, "BJ", 60, json!([]));
        node.polls.insert(k, v);
    }
    node
}

/// R-P5P9-75 J, K, L: `payload.rollback`, exec `payload.data` and `signers.address` are
/// explained only when ours equals the node's command (`cont.rollback`, `exec.data`, the
/// signer's `addr`). Each negative neighbour (ours not the node's) stays a diff.
#[tokio::test]
async fn d2_j_k_l_tx_fields_against_the_nodes_command() {
    use crate::graphql_window::{EXEC_PAYLOAD_DATA_PATH, ROLLBACK_PATH, SIGNER_ADDRESS_PATH};
    let dir = tempfile::tempdir().unwrap();
    let cont = |rb: bool| {
        json!({"payload": {"cont": {"pactId": "p", "step": 1, "rollback": rb, "data": {},
            "proof": null}}, "meta": {"chainId": "0"}, "signers": []})
    };
    let exec = |data: Value| {
        json!({"payload": {"exec": {"code": "(+ 1 2)", "data": data}},
            "meta": {"chainId": "0"},
            "signers": [{"pubKey": "pk1", "addr": "6a3a7c76"}, {"pubKey": "pk2"}]})
    };
    let node = d2_cmd_node(&[
        ("j1", cont(false)),
        ("k1", exec(Value::Null)),
        ("k2", exec(json!({"ks": ["a"]}))),
    ]);
    let ctx = gql_ctx(node, dir.path()).await;
    let tx = |rk: &str, payload: Value, signers: Value| {
        d2_answer(json!({"data": {"transaction": {"hash": rk,
            "cmd": {"meta": {"chainId": "0"}, "payload": payload, "signers": signers}}}}))
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
    let has = |d: &[crate::report::DiffEntry], p: &str| d.iter().any(|e| e.path == p);
    // J: ours false = the node's cont.rollback, the oracle's true.
    let rb = |x: bool| json!({"rollback": x});
    let d = settle(
        tx("j1", rb(false), json!([])),
        tx("j1", rb(true), json!([])),
    )
    .await;
    assert!(only_hits(&d) && has(&d, ROLLBACK_PATH), "{d:?}");
    // Ours true against the node's false: a diff.
    let d = settle(
        tx("j1", rb(true), json!([])),
        tx("j1", rb(false), json!([])),
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
    // K: an exec payload, ours "null" = the node's exec.data null, the oracle's "{}".
    let data = |x: &str| json!({ "data": x });
    let d = settle(
        tx("k1", data("null"), json!([])),
        tx("k1", data("{}"), json!([])),
    )
    .await;
    assert!(only_hits(&d) && has(&d, EXEC_PAYLOAD_DATA_PATH), "{d:?}");
    // By value: ours' JSON text of the node's object.
    let d = settle(
        tx("k2", data(r#"{"ks":["a"]}"#), json!([])),
        tx("k2", data("{}"), json!([])),
    )
    .await;
    assert!(only_hits(&d), "{d:?}");
    // Ours "{}" where the node's exec.data is null: a diff.
    let d = settle(
        tx("k1", data("{}"), json!([])),
        tx("k1", data("null"), json!([])),
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
    // L: ours' address = the node's signer addr, the oracle's null.
    let sg = |pk: &str, addr: Value| json!([{ "pubkey": pk, "address": addr }]);
    let d = settle(
        tx("k1", data("null"), sg("pk1", json!("6a3a7c76"))),
        tx("k1", data("null"), sg("pk1", Value::Null)),
    )
    .await;
    assert!(only_hits(&d) && has(&d, SIGNER_ADDRESS_PATH), "{d:?}");
    // Ours' address is not the node's addr: a diff.
    let d = settle(
        tx("k1", data("null"), sg("pk1", json!("ffff0000"))),
        tx("k1", data("null"), sg("pk1", Value::Null)),
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
    // The node's signer at that index has another public key: a diff.
    let d = settle(
        tx("k1", data("null"), sg("pkX", json!("6a3a7c76"))),
        tx("k1", data("null"), sg("pkX", Value::Null)),
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
}

/// R-P5P9-75 O: H8 on a transfer list nested in a transaction (QTransactionByKey): the
/// oracle's rank among the node's TRANSFER events takes the node's event index, ours'
/// numbering. Ours with another index stays a diff.
#[tokio::test]
async fn d2_o_h8_applies_to_nested_transfer_lists() {
    use crate::graphql_window::NESTED_ORDER_INDEX_PATH as ORDER_INDEX_PATH;
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let ev = json!([
        {"name": "TRANSFER", "module": {"name": "coin"}, "params": ["s", "k:miner", 0.01]},
        {"name": "OTHER", "module": {"name": "coin"}, "params": []},
        {"name": "TRANSFER", "module": {"name": "coin"}, "params": ["s", "r", 1.0]}
    ]);
    let (k, v) = h7_poll("o1", "BO", 70, ev);
    node.polls.insert(k, v);
    let ctx = gql_ctx(node, dir.path()).await;
    let tx = |idx: Value| {
        d2_answer(json!({"data": {"transaction": {"hash": "o1",
            "cmd": {"meta": {"chainId": "0"}},
            "result": {"transfers": {"edges": [{"node": {"orderIndex": idx,
                "amount": 1.0, "senderAccount": "s", "receiverAccount": "r"}}]}}}}}))
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
    let d = settle(tx(json!(2)), tx(json!(1))).await;
    assert!(
        only_hits(&d) && d.iter().any(|e| e.path == ORDER_INDEX_PATH),
        "{d:?}"
    );
    // Ours 3: not the node's event index, a diff.
    let d = settle(tx(json!(3)), tx(json!(1))).await;
    assert!(!only_hits(&d), "{d:?}");
    // The oracle's 0 is the gas transfer's rank, not this content: a diff.
    let d = settle(tx(json!(2)), tx(json!(0))).await;
    assert!(!only_hits(&d), "{d:?}");
}

/// R-P5P9-75 Q: ours lacks a chain row the oracle lists. The oracle's extra row leaves and
/// one marker stays, carrying the missing key; the rows both hold still compare. A row only
/// ours holds stays a plain diff.
#[test]
fn d2_q_missing_chain_rows_become_one_marker() {
    use crate::graphql_norm::MISSING_CHAIN_ROWS_PATH;
    let row = |c: &str, bal: &str| json!({"chainId": c, "balance": bal, "accountName": "k:a"});
    let acct = |rows: Vec<Value>| {
        d2_answer(
            json!({"data": {"fungibleAccount": {"accountName": "k:a", "chainAccounts": rows}}}),
        )
    };
    let cmp = |a: Vec<Value>, b: Vec<Value>| {
        crate::graphql::compare_answers_with(&mut acct(a), &mut acct(b), 50, 10_000, &[], false)
    };
    let d = cmp(
        vec![row("0", "1.5"), row("2", "3")],
        vec![row("0", "1.5"), row("1", "0"), row("2", "3")],
    );
    assert_eq!(d.len(), 1, "{d:?}");
    assert_eq!(d[0].path, MISSING_CHAIN_ROWS_PATH);
    assert!(
        d[0].b.as_ref().unwrap().to_string().contains("\"1\""),
        "{d:?}"
    );
    // A row both hold with another balance still shows.
    let d = cmp(
        vec![row("0", "1.5"), row("2", "4")],
        vec![row("0", "1.5"), row("1", "0"), row("2", "3")],
    );
    assert!(d.iter().any(|e| e.path.ends_with("/balance")), "{d:?}");
    // A row only ours holds: no marker.
    let d = cmp(vec![row("0", "1.5"), row("5", "1")], vec![row("0", "1.5")]);
    assert!(d.iter().all(|e| e.path != MISSING_CHAIN_ROWS_PATH), "{d:?}");
    assert!(!d.is_empty());
}

/// R-P5P9-75 (CG-4 concern 4): an allowlisted REST record is logged with its entry id (its
/// hit counted once, from the record), bodies cut to `ALLOWLISTED_BODY`.
#[tokio::test]
async fn d2_rest_allowlisted_records_are_logged_with_the_entry_id() {
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
    let b = SideMock {
        search: rows(&[("w", 10), ("x", 10), ("y", 10), ("z", 9)]),
        ..SideMock::default()
    };
    let (ctx, _) = setup(node, ours, b, dir.path()).await;
    let req = Request::new(
        "/txs/search?search=coin.transfer&limit=10".to_string(),
        Origin::Seed,
        None,
    );
    let o = ctx.process(&req).await.remove(0);
    assert_eq!(o.class, "allowlisted");
    let rec = o.record.as_ref().expect("an allowlisted record");
    assert_eq!(rec.allowlisted.as_deref(), Some("search-tie-order"));
    assert!(rec.a_body_trimmed.len() <= crate::run::ALLOWLISTED_BODY + 64);
    let s = ctx.summary.lock().unwrap();
    assert_eq!(s.allowlist_hits.get("search-tie-order"), Some(&1));
    drop(s);
    let log: String = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().starts_with("diffs-"))
        .map(|e| std::fs::read_to_string(e.path()).unwrap())
        .collect();
    assert!(
        log.contains("\"class\":\"allowlisted\"") && log.contains("search-tie-order"),
        "{log}"
    );
}

// ---------------------------------------------------------------------------------------------
// FVH-D3: R-P5P9-75 N (a page cut inside one block) and S (the NodeBalance skew)

/// A block of the node at (chain 0, `h`): its header (the only one at the height, so the
/// cut's branch holds it) and a payload whose transactions are `rks` in that order.
fn d3_block(node: &mut NodeMock, blk: &str, h: u64, rks: &[String]) {
    let ph = format!("P{blk}");
    node.headers.insert(
        (0, h),
        vec![json!({"hash": blk, "height": h, "chainId": 0, "payloadHash": ph})],
    );
    let txs: Vec<Value> = rks
        .iter()
        .map(|rk| {
            let w = json!({"hash": rk, "sigs": [], "cmd": "{}"}).to_string();
            json!([kdx_types::hash::b64url_encode(w.as_bytes()), ""])
        })
        .collect();
    node.payloads.insert(ph, json!({"transactions": txs}));
}

/// A QTransactionsByBlock row: the relay id (block, request key) and the request key.
fn d3_row(blk: &str, rk: &str) -> Value {
    let id = crate::graphql::base64_std(format!("Transaction:[\"{blk}\",\"{rk}\"]").as_bytes());
    json!({"cursor": "c", "node": {"id": id, "hash": rk}})
}

async fn d3_n_run(
    ctx: &Ctx,
    vars: &serde_json::Map<String, Value>,
    ours: &[String],
    oracle: &[String],
) -> (
    Vec<crate::report::DiffEntry>,
    Option<String>,
    crate::graphql::Answer,
    crate::graphql::Answer,
) {
    let op = crate::graphql::parse_op(
        "QTransactionsByBlock",
        "query QTransactionsByBlock($blockHash: String, $first: Int, $after: String) { \
         transactions(blockHash: $blockHash, first: $first, after: $after) { totalCount \
         pageInfo { hasNextPage endCursor } edges { cursor node { id hash } } } }",
        false,
    )
    .unwrap();
    let page = |keys: &[String]| {
        h7_page(
            "transactions",
            true,
            keys.iter().map(|k| d3_row("BLK612", k)).collect(),
        )
    };
    let (mut a, mut b) = (page(ours), page(oracle));
    let (d, note) = ctx
        .gql_window_referee(
            &ctx.targets["a"],
            Some(&op),
            vars,
            &mut a,
            &mut b,
            7_000_000,
            false,
            &mut Reads::default(),
        )
        .await;
    (d, note, a, b)
}

/// R-P5P9-75 N: a page cut inside one block is explained iff ours' rows are the node's block
/// transactions in the node's order from ours' cursor position, and every oracle row is in
/// that node block (one cached payload read per block). CG-4 #612's shape: 233 node txs, two
/// 200-row pages in different orders, ours in node order. Ours in another order, a row of
/// ours or of the oracle's outside the block, or a request that starts after a cursor (ours'
/// position unknown) stay diffs.
#[tokio::test]
async fn d3_n_a_single_block_cut_is_the_nodes_order_from_ours_cursor() {
    use crate::graphql_window::{ROW_PATH, SINGLE_BLOCK_PATH, UNDECIDED_PATH};
    let dir = tempfile::tempdir().unwrap();
    let rks: Vec<String> = (0..233).map(|i| format!("t{i:03}")).collect();
    let mut node = NodeMock::default();
    d3_block(&mut node, "BLK612", 5_607_111, &rks);
    let calls = node.calls.clone();
    let au = ours_fn_server(|req: &Value| {
        if req["query"].as_str().unwrap_or("").contains("block(hash") {
            json!({"data": {"block": {"chainId": "0"}}})
        } else {
            json!({"data": null})
        }
    })
    .await;
    let ctx = gql_ctx_at(node, dir.path(), &au).await;
    let vars = h7_vars(&[("blockHash", json!("BLK612")), ("first", json!(200))]);
    // Ours: the node's first 200 in its order. The oracle: 200 others of the block, reversed.
    let ours: Vec<String> = rks[..200].to_vec();
    let oracle: Vec<String> = rks[33..].iter().rev().cloned().collect();
    let (d, note, a, b) = d3_n_run(&ctx, &vars, &ours, &oracle).await;
    assert!(
        d.iter().any(|e| e.path == SINGLE_BLOCK_PATH),
        "{d:?} {note:?}"
    );
    assert!(only_hits(&d) && note.is_none(), "{d:?} {note:?}");
    let edges = |x: &crate::graphql::Answer| x.body["data"]["transactions"]["edges"].clone();
    let (ea, eb) = (edges(&a), edges(&b));
    assert_eq!(
        ea.as_array().unwrap().len(),
        167,
        "the rows both sides hold, paired"
    );
    assert_eq!(ea[0]["node"]["hash"], json!("t033"), "in ours' order");
    assert_eq!(eb[0]["node"]["hash"], json!("t033"));
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.contains("header-by-hash BLK612"))
            .count(),
        1,
        "the block is located once"
    );
    let diff = |d: &[crate::report::DiffEntry], note: &Option<String>, what: &str| {
        assert!(
            d.iter().all(|e| e.path != SINGLE_BLOCK_PATH),
            "{what}: {d:?}"
        );
        assert!(
            d.iter()
                .any(|e| e.path == UNDECIDED_PATH || e.path == ROW_PATH),
            "{what}: {d:?}"
        );
        let n = note.clone().unwrap_or_default();
        assert!(n.contains("N not explained"), "{what}: {n}");
    };
    // Ours in another order than the node's.
    let mut swapped = ours.clone();
    swapped.swap(10, 11);
    let (d, note, ..) = d3_n_run(&ctx, &vars, &swapped, &oracle).await;
    diff(&d, &note, "ours out of the node's order");
    // A row of ours the node block does not hold.
    let mut foreign = ours.clone();
    foreign[199] = "zzz".into();
    let (d, note, ..) = d3_n_run(&ctx, &vars, &foreign, &oracle).await;
    diff(&d, &note, "ours holds a row outside the block");
    // An oracle row the node block does not hold.
    let mut stray = oracle.clone();
    stray[0] = "yyy".into();
    let (d, note, ..) = d3_n_run(&ctx, &vars, &ours, &stray).await;
    diff(&d, &note, "an oracle row outside the block");
    // Ours' cursor position is not known when the request starts after a cursor.
    let mut after = vars.clone();
    after.insert("after".into(), json!("someCursor"));
    let (d, note, ..) = d3_n_run(&ctx, &after, &ours, &oracle).await;
    diff(&d, &note, "a request after a cursor");
}

/// A node payload with outputs: a coinbase whose events are `cb`, and transactions whose
/// outputs hold these events.
fn d3_payload(cb: Value, txs: &[Value]) -> Value {
    let enc = |v: Value| kdx_types::hash::b64url_encode(v.to_string().as_bytes());
    let out = |ev: &Value| json!({"result": {"status": "success"}, "events": ev});
    let t: Vec<Value> = txs
        .iter()
        .enumerate()
        .map(|(i, ev)| {
            let w = json!({"hash": format!("rk{i}"), "sigs": [], "cmd": "{}"});
            json!([enc(w), enc(out(ev))])
        })
        .collect();
    json!({"transactions": t, "coinbase": enc(out(&cb))})
}

/// A `coin.TRANSFER` event; `amount` is the node's JSON number text.
fn d3_xfer(from: &str, to: &str, amount: &str) -> Value {
    json!({"name": "TRANSFER", "module": {"name": "coin", "namespace": null},
        "params": [from, to, serde_json::from_str::<Value>(amount).unwrap()]})
}

/// Ours answering N2's tip read: chain `c`'s blocks from `h`, at most `first`, to its tip.
async fn d3_ours_tips(tips: &'static [(u8, u64)]) -> String {
    ours_fn_server(move |req: &Value| {
        let v = &req["variables"];
        let c = v["c"][0].as_str().unwrap_or("0").to_string();
        let start = v["h"].as_u64().unwrap_or(0);
        let first = v["first"].as_u64().unwrap_or(2);
        let tip = tips
            .iter()
            .find(|(x, _)| x.to_string() == c)
            .map_or(0, |(_, t)| *t);
        let edges: Vec<Value> = (start..=tip)
            .take(first as usize)
            .map(|h| {
                let x = (b'a' + c.parse::<u8>().unwrap_or(0)) as char;
                json!({"node": {"height": h, "chainId": c, "hash": format!("{x}{h}")}})
            })
            .collect();
        json!({"data": {"blocksFromHeight": {"pageInfo": {"hasNextPage": tip >= start + first},
            "edges": edges}}})
    })
    .await
}

/// R-P5P9-75 S: the NodeBalance skew is allowed when ours' tip is 1 or 2 blocks below
/// `node_h`, with exactly one header per height on the cut branch, and the node's balance less
/// ours is exactly the net of the coin TRANSFER events touching the account in the node's
/// blocks above ours' tip (gas fees and coinbase included). CG-4 #77's shape (a miner one
/// block behind) and two blocks behind with a fee-paying tx are explained; a net one unit off,
/// three blocks behind, a fork at a skipped height and ours at `node_h` are diffs.
#[tokio::test]
async fn d3_s_node_balance_skew_is_the_event_net_of_one_or_two_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let hd = |c: u8, h: u64, x: &str| json!({"hash": x, "payloadHash": format!("p-{x}"), "height": h, "chainId": c});
    let mut node = NodeMock::default();
    let block = |node: &mut NodeMock, c: u8, h: u64, x: &str, p: Value| {
        node.headers.entry((c, h)).or_default().push(hd(c, h, x));
        node.payloads.insert(format!("p-{x}"), p);
    };
    let cb_miner = || d3_payload(json!([d3_xfer("", "k:miner", "0.904046")]), &[]);
    let cb_other = |txs: &[Value]| d3_payload(json!([d3_xfer("", "k:m2", "0.904046")]), txs);
    // Chain 0 (CG-4 #77): ours one block behind; the node's block 100 paid the miner.
    block(&mut node, 0, 100, "a100", cb_miner());
    // Chain 1: two behind; at 99 the account pays 1.5 and its gas (0.0000123 to the miner of
    // that block), and a non-coin TRANSFER that does not move its coin balance; at 100 a
    // transfer between others.
    let paid = json!([
        d3_xfer("k:miner", "k:bob", "1.5"),
        {"name": "TRANSFER", "module": {"name": "token", "namespace": "free"},
            "params": ["k:miner", "k:x", 7.0]},
        d3_xfer("k:miner", "k:m2", "0.0000123")
    ]);
    block(&mut node, 1, 99, "b99", cb_other(&[paid]));
    block(
        &mut node,
        1,
        100,
        "b100",
        cb_other(&[json!([d3_xfer("k:carol", "k:dave", "3.0")])]),
    );
    // Chain 2: as chain 0; the balances are one unit (1e-12) off the net.
    block(&mut node, 2, 100, "c100", cb_miner());
    // Chain 3: three behind, every block paying the miner.
    for h in 98..=100 {
        block(&mut node, 3, h, &format!("d{h}"), cb_miner());
    }
    // Chain 4: two behind with a fork at 99 (the branch names x99); the net would hold.
    block(&mut node, 4, 99, "x99", cb_other(&[]));
    block(&mut node, 4, 99, "y99", cb_miner());
    node.canon.insert((4, 99), "x99".into());
    block(&mut node, 4, 100, "e100", cb_miner());
    // Chain 5: ours already at node_h.
    block(&mut node, 5, 100, "f100", cb_miner());
    // R-P5P9-78 (H2 m-6): ours' block at its tip is the cut branch's header there.
    for (c, h) in [(0, 99), (1, 98), (2, 99), (3, 97), (4, 98)] {
        let x = (b'a' + c) as char;
        block(&mut node, c, h, &format!("{x}{h}"), cb_other(&[]));
    }
    // Chain 6: ours one behind on a fork at its tip (ours g99, the branch z99); the block
    // above pays the miner, so the net would hold.
    block(&mut node, 6, 99, "z99", cb_other(&[]));
    block(&mut node, 6, 100, "g100", cb_miner());
    static TIPS: [(u8, u64); 7] = [
        (0, 99),
        (1, 98),
        (2, 99),
        (3, 97),
        (4, 98),
        (5, 100),
        (6, 99),
    ];
    let au = d3_ours_tips(&TIPS).await;
    let ctx = gql_ctx_at(node, dir.path(), &au).await;
    let ta = &ctx.targets["a"];
    let skew = |c: u8, ours: f64, node: &'static str| {
        let ctx = &ctx;
        async move {
            ctx.s_skew(ta, c, 100, "k:miner", ours, node, &mut Reads::default())
                .await
        }
    };
    let ok = skew(0, 10.0, "10.904046").await;
    assert!(ok.is_ok(), "CG-4 #77's shape: {ok:?}");
    let ok = skew(1, 6.5000123, "5.0").await;
    assert!(ok.is_ok(), "two behind with a fee-paying tx: {ok:?}");
    assert!(
        skew(1, 6.5, "5.0").await.is_err(),
        "the gas fee is part of the net"
    );
    assert!(
        skew(2, 10.0, "10.904046000001").await.is_err(),
        "a net one unit off"
    );
    assert!(
        skew(2, 10.000000000001, "10.904046").await.is_err(),
        "a net one unit off, the other way"
    );
    assert!(
        skew(3, 10.0, "12.712138").await.is_err(),
        "three blocks behind"
    );
    let fork = skew(4, 10.0, "10.904046").await;
    assert!(
        fork.as_ref().is_err_and(|w| w.contains("header")),
        "a fork at a skipped height: {fork:?}"
    );
    assert!(skew(5, 10.0, "10.904046").await.is_err(), "ours at node_h");
    let fork = skew(6, 10.0, "10.904046").await;
    assert!(
        fork.as_ref().is_err_and(|w| w.contains("tip")),
        "ours on a fork at its tip: {fork:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// R-P5P9-78 D4a: the harness findings of review-fvh-h1 and review-fvh-h2 (FVH-D4a).

/// A transfer row at height 100 with sender, receiver, amount and the index `i`.
fn d4a_irow(rk: &str, to: &str, amount: f64, i: i64) -> Value {
    let mut e = h7_xrow(rk, 100, json!(amount), Some(("s", to)));
    e["node"]["orderIndex"] = json!(i);
    e
}

/// R-P5P9-78 (H1 I-1): E's verdict is per row, never shared by key and index. CASE1: the
/// oracle lists tx e twice at index 0, another miner's gas row first and the node's own event
/// 0 second; ours lacks e. The node's own row is not explained by the gas row's class. Two
/// copies of the gas row both take E's class.
#[tokio::test]
async fn d4a_h1_i1_e_verdict_is_per_row_not_per_key_and_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for rk in ["x0", "e"] {
        let (k, v) = h7_xfer_poll(rk, "B100", 100, "s", "r", json!(1.0));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let x0 = d4a_irow("x0", "r", 1.0, 0);
    let gas = d4a_irow("e", "miner2", 0.0000323, 0);
    let (d, note) = d1_settle(
        &ctx,
        vec![x0.clone()],
        vec![x0.clone(), gas.clone(), d4a_irow("e", "r", 1.0, 0)],
        (false, false),
    )
    .await;
    assert!(!only_hits(&d), "CASE1: {d:?} {note:?}");
    let (d, note) = d1_settle(
        &ctx,
        vec![x0.clone()],
        vec![x0.clone(), gas.clone(), gas],
        (false, false),
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    assert!(
        format!("{d:?}").contains(crate::graphql_window::NOT_IN_NODE_TX),
        "{d:?}"
    );
}

/// R-P5P9-78 (H1 I-2, H2 m-3): E matches only (sender, receiver, amount). CASE2: the oracle
/// lists the node's transfer with another module name (or module hash); ours lacks it. The
/// row is not off the node's chain, so it stays a diff. A triple the node lacks, with the
/// module selected, stays explained.
#[tokio::test]
async fn d4a_h1_i2_e_matches_sender_receiver_and_amount_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for rk in ["x0", "e"] {
        let (k, v) = h7_xfer_poll(rk, "B100", 100, "s", "r", json!(1.0));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let x0 = h7_xrow("x0", 100, json!(1.0), Some(("s", "r")));
    let with = |to: &str, amount: f64, k: &str, v: &str| {
        let mut e = h7_xrow("e", 100, json!(amount), Some(("s", to)));
        e["node"][k] = json!(v);
        e
    };
    for (k, v) in [("moduleName", "n_abc.coin"), ("moduleHash", "zzz")] {
        let (d, note) = d1_settle(
            &ctx,
            vec![x0.clone()],
            vec![x0.clone(), with("r", 1.0, k, v)],
            (false, false),
        )
        .await;
        assert!(!only_hits(&d), "CASE2 {k}: {d:?} {note:?}");
    }
    let (d, note) = d1_settle(
        &ctx,
        vec![x0.clone()],
        vec![x0.clone(), with("miner2", 0.0000323, "moduleName", "coin")],
        (false, false),
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    assert!(
        format!("{d:?}").contains(crate::graphql_window::NOT_IN_NODE_TX),
        "{d:?}"
    );
}

/// R-P5P9-78 (H1 m-2): E refuses when the node's `/poll` entry has no `events`. CASE5: the
/// oracle row stays a diff. An entry whose `events` is an empty list is the node's word that
/// the transaction holds no TRANSFER: E explains.
#[tokio::test]
async fn d4a_h1_m2_e_refuses_a_poll_entry_without_events() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let (k, v) = h7_xfer_poll("x0", "B100", 100, "s", "r", json!(1.0));
    node.polls.insert(k, v);
    let meta = json!({"blockHash": "B100", "blockHeight": 100});
    node.polls
        .insert("e".into(), (0, json!({"reqKey": "e", "metaData": meta})));
    node.polls.insert(
        "f".into(),
        (0, json!({"reqKey": "f", "metaData": meta, "events": []})),
    );
    let ctx = gql_ctx(node, dir.path()).await;
    let x = |rk: &str| h7_xrow(rk, 100, json!(1.0), Some(("s", "r")));
    let (d, note) = d1_settle(&ctx, vec![x("x0")], vec![x("x0"), x("e")], (false, false)).await;
    assert!(!only_hits(&d), "CASE5: {d:?} {note:?}");
    let (d, note) = d1_settle(&ctx, vec![x("x0")], vec![x("x0"), x("f")], (false, false)).await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
}

/// R-P5P9-78 (H1 I-3): F's key-only rows are checked against the node transfers the request's
/// filter selects. CASE3: under `accountName: r`, tx k holds one TRANSFER naming r and a gas
/// TRANSFER; ours lists k twice, the oracle once: a diff. Under `accountName: s` both
/// TRANSFERs are inside the filter, and the same pages are explained. An account no node
/// TRANSFER names is a diff.
#[tokio::test]
async fn d4a_h1_i3_key_only_rows_count_the_node_transfers_inside_the_filter() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let ev = |from: &str, to: &str, a: f64| json!({"name": "TRANSFER", "module": {"name": "coin"}, "params": [from, to, a]});
    let (k, v) = h7_poll(
        "k",
        "B100",
        100,
        json!([ev("s", "r", 1.0), ev("s", "miner", 0.01)]),
    );
    node.polls.insert(k, v);
    let ctx = gql_ctx(node, dir.path()).await;
    let op = h7_op("transfers");
    let r = || json!({"cursor": "c", "node": {"chainId": "0", "requestKey": "k"}});
    let settle = |account: &'static str, a: Vec<Value>, b: Vec<Value>| {
        let (ctx, op) = (&ctx, &op);
        async move {
            let vars = h7_vars(&[("accountName", json!(account))]);
            let (mut a, mut b) = (
                h7_page("transfers", false, a),
                h7_page("transfers", false, b),
            );
            ctx.gql_settle_op(
                &ctx.targets["a"],
                Some(op),
                &vars,
                &mut a,
                &mut b,
                7_000_000,
                false,
            )
            .await
        }
    };
    let (d, note) = settle("r", vec![r(), r()], vec![r()]).await;
    assert!(!only_hits(&d), "CASE3: {d:?} {note:?}");
    let (d, note) = settle("s", vec![r(), r()], vec![r()]).await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    let (d, note) = settle("zed", vec![r()], vec![]).await;
    assert!(!only_hits(&d), "{d:?} {note:?}");
}

/// R-P5P9-78 (H1 m-1): A judges an ambiguous row against each node event it matches. CASE4:
/// a row selecting the amount and `crossChainTransfer: null` but no sender matches a plain
/// TRANSFER and the TRANSFER of a cross-chain send; the events disagree, so the row is a
/// diff. Two plain TRANSFERs of that amount agree on null: explained.
#[tokio::test]
async fn d4a_h1_m1_cross_chain_link_is_judged_against_every_matching_event() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    let ev = |name: &str, p: Value| json!({"name": name, "module": {"name": "coin"}, "params": p});
    let (k, v) = h7_xfer_poll("x0", "B100", 100, "s", "r", json!(5.0));
    node.polls.insert(k, v);
    let (k, v) = h7_poll(
        "x3",
        "B100",
        100,
        json!([
            ev("TRANSFER", json!(["s2", "r2", 5.0])),
            ev("TRANSFER", json!(["s1", "", 5.0])),
            ev("TRANSFER_XCHAIN", json!(["s1", "r", 5.0, "1"]))
        ]),
    );
    node.polls.insert(k, v);
    let (k, v) = h7_poll(
        "x4",
        "B100",
        100,
        json!([
            ev("TRANSFER", json!(["s2", "r2", 5.0])),
            ev("TRANSFER", json!(["s3", "r3", 5.0]))
        ]),
    );
    node.polls.insert(k, v);
    let ctx = gql_ctx(node, dir.path()).await;
    let x0 = h7_xrow("x0", 100, json!(5.0), Some(("s", "r")));
    let row = |rk: &str| {
        let mut e = h7_xrow(rk, 100, json!(5.0), None);
        e["node"]["crossChainTransfer"] = Value::Null;
        e
    };
    let (d, note) = d1_settle(
        &ctx,
        vec![x0.clone(), row("x3"), row("x3")],
        vec![x0.clone()],
        (false, false),
    )
    .await;
    assert!(!only_hits(&d), "CASE4: {d:?} {note:?}");
    assert!(note.unwrap_or_default().contains("crossChainTransfer"));
    let (d, note) = d1_settle(
        &ctx,
        vec![x0.clone(), row("x4"), row("x4")],
        vec![x0.clone()],
        (false, false),
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
}

/// The settle of `transfers` pages with the document `query` and `vars`.
async fn d4a_settle(
    ctx: &Ctx,
    query: &str,
    vars: &serde_json::Map<String, Value>,
    a: Vec<Value>,
    b: Vec<Value>,
) -> (Vec<crate::report::DiffEntry>, Option<String>) {
    let op = crate::graphql::parse_op("QD4a", query, false).unwrap();
    let mut a = h7_page("transfers", false, a);
    let mut b = h7_page("transfers", false, b);
    ctx.gql_settle_op(
        &ctx.targets["a"],
        Some(&op),
        vars,
        &mut a,
        &mut b,
        7_000_000,
        false,
    )
    .await
}

/// R-P5P9-78 (H1 m-3): E and F read the selection's field names, never the aliases. A row
/// selection with an alias (`senderAccount: receiverAccount`) makes H7 refuse, so E cannot
/// read a swapped pair as off the node's chain; a root aliased `transfers: events(...)` is
/// not F's `transfers`. The harness's own `kdxKey` alias and plain names stay explained.
#[tokio::test]
async fn d4a_h1_m3_e_and_f_read_field_names_not_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for (rk, h) in [("x0", 100), ("e", 100), ("u", 101), ("v", 100)] {
        let (k, v) = h7_xfer_poll(rk, &format!("B{h}"), h, "s", "r", json!(1.0));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let vars = serde_json::Map::new();
    // E: the oracle's row of e is the node's s -> r, under swapped aliases.
    let swapped = "query Q($first: Int) { transfers(first: $first) { totalCount pageInfo { \
                   hasNextPage endCursor } edges { cursor node { requestKey block { chainId \
                   height } senderAccount: receiverAccount receiverAccount: senderAccount \
                   amount } } } }";
    let x0 = h7_xrow("x0", 100, json!(1.0), Some(("r", "s")));
    let e = h7_xrow("e", 100, json!(1.0), Some(("r", "s")));
    let (d, note) = d4a_settle(&ctx, swapped, &vars, vec![x0.clone()], vec![x0.clone(), e]).await;
    assert!(!only_hits(&d), "{d:?} {note:?}");
    assert!(note.unwrap_or_default().contains("alias"));
    // F: key-only rows under `transfers: events(...)`.
    let r = |rk: &str| json!({"cursor": "c", "node": {"chainId": "0", "requestKey": rk}});
    let q = |root: &str, key: &str| {
        format!(
            "query Q($first: Int) {{ {root}(first: $first) {{ totalCount pageInfo {{ \
             hasNextPage endCursor }} edges {{ cursor node {{ chainId requestKey }} {key} }} }} }}"
        )
    };
    let (d, note) = d4a_settle(
        &ctx,
        &q("transfers: events", ""),
        &vars,
        vec![r("u"), r("v")],
        vec![r("u")],
    )
    .await;
    assert!(!only_hits(&d), "{d:?} {note:?}");
    for key in ["", "kdxKey: node { requestKey orderIndex }"] {
        let (d, note) = d4a_settle(
            &ctx,
            &q("transfers", key),
            &vars,
            vec![r("u"), r("v")],
            vec![r("u")],
        )
        .await;
        assert!(only_hits(&d) && !d.is_empty(), "{key}: {d:?} {note:?}");
    }
}

/// The N case of `d3_n_run` with the document `query`.
async fn d4a_n_run(
    ctx: &Ctx,
    query: &str,
    vars: &serde_json::Map<String, Value>,
    ours: &[String],
    oracle: &[String],
) -> (Vec<crate::report::DiffEntry>, Option<String>) {
    let op = crate::graphql::parse_op("QTransactionsByBlock", query, false).unwrap();
    let page = |keys: &[String]| {
        h7_page(
            "transactions",
            true,
            keys.iter().map(|k| d3_row("BLK612", k)).collect(),
        )
    };
    let (mut a, mut b) = (page(ours), page(oracle));
    ctx.gql_window_referee(
        &ctx.targets["a"],
        Some(&op),
        vars,
        &mut a,
        &mut b,
        7_000_000,
        false,
        &mut Reads::default(),
    )
    .await
}

/// R-P5P9-78 (H2 m-5): N applies only when the request's filter names nothing beyond block,
/// chain and height. With `accountName` set, ours' rows being the node block's first 200 does
/// not show they are the account's: a diff. `chainId` with the block still takes N.
#[tokio::test]
async fn d4a_h2_m5_n_only_under_a_block_chain_or_height_filter() {
    use crate::graphql_window::{ROW_PATH, SINGLE_BLOCK_PATH, UNDECIDED_PATH};
    let dir = tempfile::tempdir().unwrap();
    let rks: Vec<String> = (0..233).map(|i| format!("t{i:03}")).collect();
    let mut node = NodeMock::default();
    d3_block(&mut node, "BLK612", 5_607_111, &rks);
    let au = ours_fn_server(|req: &Value| {
        if req["query"].as_str().unwrap_or("").contains("block(hash") {
            json!({"data": {"block": {"chainId": "0"}}})
        } else {
            json!({"data": null})
        }
    })
    .await;
    let ctx = gql_ctx_at(node, dir.path(), &au).await;
    let ours: Vec<String> = rks[..200].to_vec();
    let oracle: Vec<String> = rks[33..].iter().rev().cloned().collect();
    let q = |arg: &str, def: &str| {
        format!(
            "query QTransactionsByBlock($blockHash: String, {def} $first: Int, $after: String) \
             {{ transactions(blockHash: $blockHash, {arg} first: $first, after: $after) {{ \
             totalCount pageInfo {{ hasNextPage endCursor }} edges {{ cursor node {{ id hash }} \
             }} }} }}"
        )
    };
    let base = h7_vars(&[("blockHash", json!("BLK612")), ("first", json!(200))]);
    let mut vars = base.clone();
    vars.insert("accountName".into(), json!("k:someone"));
    let (d, note) = d4a_n_run(
        &ctx,
        &q("accountName: $accountName,", "$accountName: String,"),
        &vars,
        &ours,
        &oracle,
    )
    .await;
    assert!(
        d.iter().all(|e| e.path != SINGLE_BLOCK_PATH)
            && d.iter()
                .any(|e| e.path == UNDECIDED_PATH || e.path == ROW_PATH),
        "{d:?} {note:?}"
    );
    let mut vars = base.clone();
    vars.insert("chainId".into(), json!("0"));
    let (d, note) = d4a_n_run(
        &ctx,
        &q("chainId: $chainId,", "$chainId: String,"),
        &vars,
        &ours,
        &oracle,
    )
    .await;
    assert!(
        d.iter().any(|e| e.path == SINGLE_BLOCK_PATH) && only_hits(&d),
        "{d:?} {note:?}"
    );
}

/// R-P5P9-78 (H2 I-2; closes H1 m-5): E and M, O and K each emit a visible hit of their own,
/// which only its own PROPOSED entry explains. A window settled without E or M emits no
/// off-chain hit.
#[tokio::test]
async fn d4a_h2_i2_e_m_o_k_have_their_own_visible_hits_and_proposed_entries() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for (rk, h) in [("x0", 100), ("e", 100), ("v", 100)] {
        let (k, v) = h7_xfer_poll(rk, &format!("B{h}"), h, "s", "r", json!(1.0));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let off = "/~graphql-r75-h7-off-chain";
    let x = |rk: &str, to: &str, amount: f64| h7_xrow(rk, 100, json!(amount), Some(("s", to)));
    let x0 = x("x0", "r", 1.0);
    // E.
    let (d, note) = d1_settle(
        &ctx,
        vec![x0.clone()],
        vec![x0.clone(), x("e", "miner2", 0.0000323)],
        (false, false),
    )
    .await;
    assert!(
        only_hits(&d) && d.iter().any(|e| e.path == off),
        "{d:?} {note:?}"
    );
    // M: the oracle places e in X90 at height 90.
    let mut m = h7_xrow("e", 90, json!(1.0), Some(("s", "r")));
    m["node"]["blockHash"] = json!("X90");
    let mut ours = h7_xrow("e", 100, json!(1.0), Some(("s", "r")));
    ours["node"]["blockHash"] = json!("B100");
    let (d, note) = d1_settle(&ctx, vec![ours], vec![m], (false, false)).await;
    assert!(
        only_hits(&d) && d.iter().any(|e| e.path == off),
        "{d:?} {note:?}"
    );
    // A node-confirmed oracle gap only: no off-chain hit.
    let (d, note) = d1_settle(
        &ctx,
        vec![x0.clone(), x("v", "r", 1.0)],
        vec![x0.clone()],
        (false, false),
    )
    .await;
    assert!(
        only_hits(&d) && d.iter().all(|e| e.path != off),
        "{d:?} {note:?}"
    );
    // Each new hit is explained by its own PROPOSED entry, and only by it.
    let allow = d2_shipped();
    for (path, id) in [
        (off, "graphql-r75-h7-off-chain"),
        (
            "/~graphql-r75-nested-order-index",
            "graphql-r75-nested-order-index",
        ),
        (
            "/~graphql-r75-exec-payload-data",
            "graphql-r75-exec-payload-data",
        ),
    ] {
        let hit = crate::report::DiffEntry {
            path: path.into(),
            kind: crate::report::DiffKind::Changed,
            a: None,
            b: None,
        };
        let (un, ids) = allow.classify_request(
            &crate::normalize::RouteKind::Other,
            "graphql:QD4a",
            &[],
            &[hit],
        );
        assert!(un.is_empty() && ids == [id], "{path}: {un:?} {ids:?}");
        let e = allow.entries.iter().find(|e| e.id == id).unwrap();
        // R-P5P9-86 flipped K and O to ACCEPTED on CG-5's node checks.
        let flipped = e.status == crate::allowlist::Status::Accepted
            && e.ruling.contains("R-P5P9-86, CG-5")
            && [
                "graphql-r75-exec-payload-data",
                "graphql-r75-nested-order-index",
            ]
            .contains(&id);
        assert!(
            (flipped
                || e.status == crate::allowlist::Status::Proposed
                    && e.owner_note.starts_with("PROPOSED"))
                && e.reason.contains("R-P5P9-78"),
            "{id}: {e:?}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// R-P5P9-81 D5: the findings of review-fvh-i (FVH-D5).

/// The settle counts in the window hit's evidence, on ours' side or the oracle's.
fn d5_settled(d: &[crate::report::DiffEntry], ours: bool) -> Value {
    let w = d
        .iter()
        .find(|e| e.path == crate::graphql_window::WINDOW_PATH)
        .expect("the window hit");
    let side = if ours { &w.a } else { &w.b };
    side.as_ref()
        .and_then(|s| s.get("settled"))
        .cloned()
        .unwrap_or(Value::Null)
}

/// The four settle classes R-P5P9-81 names, with these counts.
fn d5_counts(gap: u64, outside: u64, duplicate: u64, key_only: u64) -> Value {
    json!({"oracle-gap": gap, "outside-oracle-window": outside, "duplicate": duplicate,
        "key-only": key_only})
}

/// The rows the unindexed hit lists on ours' side and the oracle's.
fn d5_unindexed(d: &[crate::report::DiffEntry], path: &str) -> Option<(usize, usize)> {
    let e = d.iter().find(|e| e.path == path)?;
    let n = |s: &Option<Value>| {
        s.as_ref()
            .and_then(|s| s.get("rows"))
            .and_then(Value::as_array)
            .map_or(0, Vec::len)
    };
    Some((n(&e.a), n(&e.b)))
}

/// R-P5P9-81 (m-1): rows H7 settles without an index, D's node-confirmed duplicates and F's
/// key-only rows, emit their own visible hit next to the window hit, which only its own
/// PROPOSED entry explains; an indexed canonical row does not. The window hit's evidence
/// counts each side's one-sided rows per settle class.
#[tokio::test]
async fn d5_m1_rows_settled_without_an_index_have_their_own_visible_hit() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for rk in ["u", "v"] {
        let (k, v) = h7_xfer_poll(rk, "B100", 100, "s", "r", json!(1.0));
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let un = "/~graphql-r75-h7-unindexed";
    // D: the oracle lists u twice; the node's transaction and ours hold it once.
    let x = |rk: &str| h7_xrow(rk, 100, json!(1.0), Some(("s", "r")));
    let (d, note) = d1_settle(&ctx, vec![x("u")], vec![x("u"), x("u")], (false, false)).await;
    assert!(only_hits(&d), "D: {d:?} {note:?}");
    assert_eq!(d5_unindexed(&d, un), Some((0, 1)), "D: {d:?}");
    assert_eq!(d5_settled(&d, false), d5_counts(0, 0, 1, 1), "D: {d:?}");
    assert_eq!(d5_settled(&d, true), d5_counts(0, 0, 0, 0), "D: {d:?}");
    // F: key-only rows; ours' v is a node-confirmed oracle gap.
    let r = |rk: &str| json!({"cursor": "c", "node": {"chainId": "0", "requestKey": rk}});
    let (d, note) = d1_settle(&ctx, vec![r("u"), r("v")], vec![r("u")], (false, false)).await;
    assert!(only_hits(&d), "F: {d:?} {note:?}");
    assert_eq!(d5_unindexed(&d, un), Some((1, 0)), "F: {d:?}");
    assert_eq!(d5_settled(&d, true), d5_counts(1, 0, 0, 1), "F: {d:?}");
    assert_eq!(d5_settled(&d, false), d5_counts(0, 0, 0, 0), "F: {d:?}");
    // The neighbour: an indexed canonical row settled the same way emits no unindexed hit.
    let i = |rk: &str| d4a_irow(rk, "r", 1.0, 0);
    let (d, note) = d1_settle(&ctx, vec![i("u"), i("v")], vec![i("u")], (false, false)).await;
    assert!(only_hits(&d), "indexed: {d:?} {note:?}");
    assert_eq!(d5_unindexed(&d, un), None, "indexed: {d:?}");
    assert_eq!(
        d5_settled(&d, true),
        d5_counts(1, 0, 0, 0),
        "indexed: {d:?}"
    );
    // The hit is explained by its own PROPOSED entry, and only by it.
    let allow = d2_shipped();
    let hit = crate::report::DiffEntry {
        path: un.into(),
        kind: crate::report::DiffKind::Changed,
        a: None,
        b: None,
    };
    let (left, ids) = allow.classify_request(
        &crate::normalize::RouteKind::Other,
        "graphql:QD5",
        &[],
        &[hit],
    );
    let id = "graphql-r75-h7-unindexed";
    assert!(left.is_empty() && ids == [id], "{left:?} {ids:?}");
    let e = allow.entries.iter().find(|e| e.id == id).unwrap();
    assert!(
        e.status == crate::allowlist::Status::Proposed
            && e.owner_note.starts_with("PROPOSED")
            && e.reason.contains("R-P5P9-81")
            && e.ruling.contains("R-P5P9-81"),
        "{e:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// FVH-E1 (R-P5P9-86, the harness round after CG-5)

/// R-P5P9-86 M (widened): `/poll` puts the key in B100 and the node does not hold the oracle
/// row's block X9 (a header-by-hash 404, read twice): the oracle row is an orphan by block
/// (CG-5 #226). A row with only a height, at another height than the node's for the key, is
/// an orphan by height only when ours has the row at the node's block (#1997); otherwise it
/// stays a diff.
#[tokio::test]
async fn e1_m_orphans_by_an_unknown_block_and_by_height() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for rk in ["x0", "m"] {
        let (k, v) = h7_xfer_poll(rk, "B100", 100, "s", "r", json!(1.0));
        node.polls.insert(k, v);
    }
    let calls = node.calls.clone();
    let ctx = gql_ctx(node, dir.path()).await;
    let x0 = h7_xrow("x0", 100, json!(1.0), Some(("s", "r")));
    let at = |b: Option<&str>, h: Option<u64>| {
        let mut e = h7_xrow("m", 0, json!(1.0), Some(("s", "r")));
        let blk = &mut e["node"]["block"];
        match h {
            Some(h) => blk["height"] = json!(h),
            None => {
                blk.as_object_mut().unwrap().remove("height");
            }
        }
        if let Some(b) = b {
            blk["hash"] = json!(b);
        }
        e
    };
    // The unknown-block orphan: X9 is not a block the node holds; no height on the row.
    let (d, note) = d1_settle(
        &ctx,
        vec![x0.clone(), at(Some("B100"), Some(100))],
        vec![x0.clone(), at(Some("X9"), None)],
        (false, false),
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    assert!(format!("{d:?}").contains("orphan"), "{d:?}");
    let reads = calls
        .lock()
        .unwrap()
        .iter()
        .filter(|c| c.as_str() == "header-by-hash X9")
        .count();
    assert_eq!(reads, 2, "the 404 is read twice");
    // The height-only orphan: the oracle's m at 99, the node's at 100 (B100), ours at B100.
    let (d, note) = d1_settle(
        &ctx,
        vec![x0.clone(), at(None, Some(100))],
        vec![x0.clone(), at(None, Some(99))],
        (false, false),
    )
    .await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    assert!(format!("{d:?}").contains("orphan by height"), "{d:?}");
    // Height-only, but ours lacks the row at the node's block: a diff.
    let (d, _) = d1_settle(
        &ctx,
        vec![x0.clone()],
        vec![x0.clone(), at(None, Some(99))],
        (false, false),
    )
    .await;
    assert!(!only_hits(&d), "{d:?}");
}

/// R-P5P9-86 N: a single-block cut where ours lists the node's block in exactly its reverse
/// order (CG-5 #178: 219 node txs, ours' 200 are the node's 218..19) is explained; a shuffled
/// order of the same rows is not.
#[tokio::test]
async fn e1_n_ours_in_the_nodes_block_order_reversed() {
    use crate::graphql_window::SINGLE_BLOCK_PATH;
    let dir = tempfile::tempdir().unwrap();
    let rks: Vec<String> = (0..219).map(|i| format!("t{i:03}")).collect();
    let mut node = NodeMock::default();
    d3_block(&mut node, "BLK612", 4_476_191, &rks);
    let au = ours_fn_server(|req: &Value| {
        if req["query"].as_str().unwrap_or("").contains("block(hash") {
            json!({"data": {"block": {"chainId": "0"}}})
        } else {
            json!({"data": null})
        }
    })
    .await;
    let ctx = gql_ctx_at(node, dir.path(), &au).await;
    let vars = h7_vars(&[("blockHash", json!("BLK612")), ("first", json!(200))]);
    let ours: Vec<String> = rks.iter().rev().take(200).cloned().collect();
    assert_eq!((ours[0].as_str(), ours[199].as_str()), ("t218", "t019"));
    let oracle: Vec<String> = rks[..200].to_vec();
    let (d, note, ..) = d3_n_run(&ctx, &vars, &ours, &oracle).await;
    assert!(
        d.iter().any(|e| e.path == SINGLE_BLOCK_PATH),
        "{d:?} {note:?}"
    );
    assert!(only_hits(&d) && note.is_none(), "{d:?} {note:?}");
    // The same rows shuffled: neither the node's order nor its reverse.
    let mut shuffled = ours.clone();
    shuffled.swap(0, 100);
    let (d, note, ..) = d3_n_run(&ctx, &vars, &shuffled, &oracle).await;
    assert!(d.iter().all(|e| e.path != SINGLE_BLOCK_PATH), "{d:?}");
    assert!(!only_hits(&d) || note.is_some(), "{d:?} {note:?}");
    assert!(note.unwrap_or_default().contains("N not explained"));
}

/// R-P5P9-86 NC: an ours-only failed tx whose `badResult` differs from the node's error only
/// in `info` (ours "", the node "<toplevel>:0": the ACCEPTED `failed-tx-error-info` class) is
/// judged after that equivalence (CG-5 #1010): canonical, an oracle gap. A `badResult` that
/// differs in another field stays a diff, and is never called "not canonical" when the
/// node's block for the key is the row's.
#[tokio::test]
async fn e1_nc_failed_tx_error_info_before_judging_the_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for rk in ["t0", "t1"] {
        let (k, mut v) = h7_poll(rk, "B100", 100, json!([]));
        v.1["result"] = json!({"status": "failure", "error": {"info": "<toplevel>:0",
            "message": "boom", "type": "TxFailure"}});
        node.polls.insert(k, v);
    }
    let ctx = gql_ctx(node, dir.path()).await;
    let tx = |rk: &str, info: &str, message: &str| {
        let bad = json!({"info": info, "message": message, "type": "TxFailure"}).to_string();
        json!({"cursor": "c", "node": {"hash": rk, "result": {"badResult": bad,
            "block": {"hash": "B100", "height": 100, "chainId": "0"}}}})
    };
    let settle = |a: Vec<Value>, b: Vec<Value>| {
        let ctx = &ctx;
        async move {
            let mut a = h7_page("transactions", false, a);
            let mut b = h7_page("transactions", false, b);
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
    let t0 = tx("t0", "<toplevel>:0", "boom");
    // Ours' t1 with info "": the accepted equivalence, so the node holds it: an oracle gap.
    let (d, note) = settle(vec![t0.clone(), tx("t1", "", "boom")], vec![t0.clone()]).await;
    assert!(only_hits(&d) && !d.is_empty(), "{d:?} {note:?}");
    // Another message: a diff, but not "not canonical" (the node's block is the row's).
    let (d, note) = settle(vec![t0.clone(), tx("t1", "", "other")], vec![t0.clone()]).await;
    assert!(!only_hits(&d), "{d:?}");
    let all = format!("{d:?} {note:?}");
    assert!(!all.contains("not canonical"), "{all}");
}

/// R-P5P9-86 SK: ours' longer read pages on by `endCursor` until it passes our boundary
/// height. CG-5 #25: ours lists 312 rows at one height; the oracle-only row sits at ours'
/// position 200, past `first` + 50: found after our cut, explained. A tie set too long for
/// the read's page budget is "our longer read was capped", never "a row we skipped".
#[tokio::test]
async fn e1_sk_the_longer_read_pages_past_the_boundary_height() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = NodeMock::default();
    for rk in ["r19", "r200", "r1500"] {
        let (k, v) = h7_poll(rk, "B941", 941, json!([]));
        node.polls.insert(k, v);
    }
    // Ours' whole list: 5 rows above 941, then `ties` rows at 941, then 3 rows at 940.
    fn full(ties: usize) -> Vec<Value> {
        let h = |i: usize| match i {
            i if i < 5 => 1000 - i as u64,
            i if i < 5 + ties => 941,
            _ => 940,
        };
        (0..5 + ties + 3)
            .map(|i| {
                let hi = h(i);
                let mut e = h7_tx(
                    &format!("r{i}"),
                    hi,
                    &format!("B{hi}"),
                    "2024-01-01T10:00:00Z",
                );
                e["cursor"] = json!(i.to_string());
                e
            })
            .collect()
    }
    let au = ours_fn_server(|req: &Value| {
        let ties: usize = req["variables"]["ties"].as_str().unwrap().parse().unwrap();
        let rows = full(ties);
        let from = req["variables"]["after"]
            .as_str()
            .map_or(0, |c| c.parse::<usize>().unwrap() + 1);
        let n = (req["variables"]["first"].as_u64().unwrap_or(0) as usize).min(100);
        let page: Vec<Value> = rows.iter().skip(from).take(n).cloned().collect();
        let end = page
            .last()
            .map(|e| e["cursor"].clone())
            .unwrap_or(Value::Null);
        json!({"data": {"transactions": {"pageInfo": {"hasNextPage": from + n < rows.len(),
            "endCursor": end}, "edges": page}}})
    })
    .await;
    h7_back_cmds(&mut node, "2024-01-01T10:00:00Z", &[]);
    let ctx = gql_ctx_at(node, dir.path(), &au).await;
    let op = h7_op("transactions");
    for (ties, x, explained) in [(312, "r200", true), (2000, "r1500", false)] {
        let rows = full(ties);
        let ours: Vec<Value> = rows.iter().take(20).cloned().collect();
        // The oracle lists x in place of our last row (canonical: an oracle gap).
        let mut oracle = ours[..19].to_vec();
        oracle.push(
            rows.iter()
                .find(|e| e["node"]["hash"] == json!(x))
                .unwrap()
                .clone(),
        );
        let mut a = crate::graphql::Answer {
            ms: vec![1],
            ..h7_page("transactions", true, ours)
        };
        let mut b = h7_page("transactions", true, oracle);
        let vars = h7_vars(&[("ties", json!(ties.to_string())), ("first", json!(20))]);
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
        assert_eq!(un.is_empty(), explained, "ties {ties}: {un:?} {note}");
        assert!(!note.contains("a row we skipped"), "ties {ties}: {note}");
        if !explained {
            assert!(note.contains("our longer read was capped"), "{note}");
        }
    }
}

/// R-P5P9-86 F: Transfers-8c8a0f56 selects no `pageInfo` (CG-5 #28, #58, #78), so its root
/// list is not a connection H7 can window: the record says "H7 not applied: the root list
/// selects no pageInfo" and stays a diff. Its rows (`requestKey`, `block { height chainId }`,
/// no hash, no `orderIndex`) get an identity by request-key multiset.
#[tokio::test]
async fn e1_f_a_root_list_without_page_info_gets_the_note() {
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
    let page = |edges: Vec<Value>| crate::graphql::Answer {
        body: json!({"data": {"transfers": {"edges": edges}}}),
        status: 200,
        raw: String::new(),
        ms: Vec::new(),
    };
    let (mut a, mut b) = (page(vec![r("u"), r("v")]), page(vec![r("u")]));
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
    assert!(!only_hits(&d), "{d:?}");
    assert_eq!(
        note.as_deref(),
        Some("H7 not applied: the root list selects no pageInfo"),
        "{d:?}"
    );
    // The rows' identity: transfers by request-key multiset (no index, no block hash).
    let conn = |rows: Vec<Value>| json!({"pageInfo": {"hasNextPage": false}, "edges": rows});
    let p = crate::graphql_window::prepare_in(
        &conn(vec![r("u"), r("u")]),
        &conn(vec![r("u")]),
        "transfers",
    )
    .unwrap();
    assert!(p
        .a
        .iter()
        .all(|x| x.id.kind == Some(crate::graphql_window::Kind::Transfer)
            && x.id.index.is_none()
            && x.id.block.is_none()));
    assert_eq!(p.a[0].key, p.b[0].key);
}

/// R-P5P9-86 G: BlocksByHeight's root rows at one height pair by block (chainId, height,
/// hash), not by position after the tie sort (CG-5 #70: the oracle's c0 list is cut, so the
/// text sort put c0 and c19 in another order on each side). c19's lists, complete on both
/// sides, compare as a multiset; c0's cut list is `/~graphql-h6-unfinished` with why.
#[test]
fn e1_g_nested_lists_pair_by_their_parent_block() {
    use crate::graphql::{compare_answers_with, Answer};
    let blk = |chain: u64, total: u64, more: bool, prices: &[&str]| {
        let edges: Vec<Value> = prices
            .iter()
            .map(|p| {
                let ct = format!("2026-01-01T00:00:0{}Z", &p[..1]);
                json!({"node": {"cmd": {"meta": {"creationTime": ct, "gasPrice": p}}}})
            })
            .collect();
        json!({"node": {"transactions": {"totalCount": total,
            "pageInfo": {"hasNextPage": more}, "edges": edges},
            "hash": format!("H{chain}"), "height": 58, "chainId": chain}})
    };
    let page = |edges: Vec<Value>| Answer {
        body: json!({"data": {"completedBlockHeights": {"pageInfo": {"hasNextPage": false},
            "edges": edges}}}),
        status: 200,
        raw: String::new(),
        ms: Vec::new(),
    };
    let mut a = page(vec![
        blk(0, 3, false, &["1e-8", "2e-8", "3e-8"]),
        blk(19, 2, false, &["4e-8", "5e-8"]),
    ]);
    let mut b = page(vec![
        blk(0, 126, true, &["1e-8"]),
        blk(19, 2, false, &["5e-8", "4e-8"]),
    ]);
    let d = compare_answers_with(&mut a, &mut b, u64::MAX, 10_000, &[], false);
    let root = |e: &crate::report::DiffEntry| {
        e.path.ends_with("/node/chainId") || e.path.ends_with("/node/hash")
    };
    assert!(!d.iter().any(root), "parents paired by position: {d:?}");
    assert!(
        d.iter()
            .any(|e| e.path == crate::graphql_scope::NESTED_MULTISET_PATH),
        "{d:?}"
    );
    assert!(
        d.iter()
            .any(|e| e.path == crate::graphql_scope::UNFINISHED_PATH),
        "{d:?}"
    );
}

/// A stored GraphQL diff record of ours against the oracle (`a` against `b`), for the
/// referee (R-P5P9-87: every rule settles a stored record).
fn e2_stored(
    stem: &str,
    vars: Value,
    a: Value,
    b: Value,
    diff: Vec<crate::report::DiffEntry>,
) -> crate::report::DiffRecord {
    crate::report::DiffRecord {
        ts: "2026-11-15T09:00:00Z".into(),
        route: format!("graphql:{stem}"),
        request: format!("{stem} {vars}"),
        a_target: "a".into(),
        b_target: "b".into(),
        a_status: Some(200),
        b_status: Some(200),
        a_body_trimmed: a.to_string(),
        b_body_trimmed: b.to_string(),
        diff,
        class: "diff".into(),
        allowlisted: None,
        known_gap: None,
        curl: String::new(),
        note: None,
        body_cap: None,
        a_pages: Some(1),
        a_tip_height: None,
        a_lowest_height: None,
    }
}

/// Writes `recs` as a diff log and runs the referee over it.
async fn e2_referee(
    cfg: CompareConfig,
    dir: &std::path::Path,
    recs: &[crate::report::DiffRecord],
) -> crate::referee::Report {
    e2_referee_capped(cfg, dir, recs, 200).await
}

/// [`e2_referee`] with the run's `--max-reads`.
async fn e2_referee_capped(
    cfg: CompareConfig,
    dir: &std::path::Path,
    recs: &[crate::report::DiffRecord],
    max_reads: usize,
) -> crate::referee::Report {
    let log = dir.join(format!("diffs-e2-{}.jsonl", recs.len()));
    let lines: Vec<String> = recs
        .iter()
        .map(|r| serde_json::to_string(&r.bounded()).unwrap())
        .collect();
    std::fs::write(&log, lines.join("\n") + "\n").unwrap();
    let opts = crate::referee::Opts {
        logs: vec![log],
        max_reads,
        rps: 10,
    };
    crate::referee::run(cfg, &opts).await.unwrap()
}

/// R-P5P9-86 (P, Q) with R-P5P9-87: the referee reads a stored record's KNOWN-GAP hits with
/// the live run's request text. #520's shape (a follow-page fault on a P key) and #37's
/// (QNonFungibleAccount, ours without the oracle's one chain row) are known gaps, never
/// explained; the same fault on another account stays unexplained.
#[tokio::test]
async fn e2_pq_the_referee_reads_known_gaps_on_stored_records() {
    use crate::report::{DiffEntry, DiffKind};
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let ops = c3_ops(dir.path());
    std::fs::write(
        ops.join("QNonFungibleAccount.graphql"),
        "# kdx: oracle\nquery QNonFungibleAccount($accountName: String!) { nonFungibleAccount(\
         accountName: $accountName) { id accountName chainAccounts { id chainId accountName } } }\n",
    )
    .unwrap();
    let cfg = || {
        c3_cfg(
            &nu,
            "http://127.0.0.1:9/graphql",
            "https://oracle.invalid/graphql",
            dir.path(),
            &ops,
            "",
        )
    };
    let fault = vec![DiffEntry {
        path: crate::graphql::FOLLOW_PAGE_FAULT_PATH.into(),
        kind: DiffKind::Changed,
        a: Some(json!({"page": 2, "status": 200,
            "why": r#"errors: [{"message":"request deadline exceeded"}]"#})),
        b: None,
    }];
    let page = json!({"data": {"transfers": {"pageInfo": {"hasNextPage": false}, "edges": []}}});
    let k520 = "af7943004d0b08fe5402c23ec317f7a8d1bdd6ce4cc52414efcf8dbfa68ca256";
    let p = e2_stored(
        "QH7",
        json!({"accountName": k520, "first": 1}),
        page.clone(),
        page.clone(),
        fault.clone(),
    );
    let other = e2_stored(
        "QH7",
        json!({"accountName": "k:0000", "first": 1}),
        page.clone(),
        page,
        fault,
    );
    let nft = |rows: Value| {
        json!({"data": {"nonFungibleAccount": {"id": "n", "accountName": "k:a",
            "chainAccounts": rows}}})
    };
    let row = json!({"id": "c8", "chainId": "8", "accountName": "k:a"});
    let q = e2_stored(
        "QNonFungibleAccount",
        json!({"accountName": "k:a"}),
        nft(json!([])),
        nft(json!([row.clone()])),
        vec![DiffEntry {
            path: "/data/nonFungibleAccount/chainAccounts/0".into(),
            kind: DiffKind::Missing,
            a: None,
            b: Some(row),
        }],
    );
    let rep = e2_referee(cfg(), dir.path(), &[p, q]).await;
    assert_eq!(
        (rep.explained, rep.unexplained, rep.known_gap),
        (0, 0, 2),
        "{rep:?}"
    );
    assert_eq!(
        rep.by_class
            .get("known-gap: known-gap-p-heavy-account-deadline"),
        Some(&1)
    );
    assert_eq!(
        rep.by_class
            .get("known-gap: known-gap-q-missing-chain-rows"),
        Some(&1)
    );
    assert!(rep.passed(), "a known gap is no unexplained record");
    let rep = e2_referee(cfg(), dir.path(), &[other]).await;
    assert_eq!((rep.unexplained, rep.known_gap), (1, 0), "{rep:?}");
}

/// A ctx whose node holds tx `t2` at chain 0 height 100 (block B100, payload P100) with
/// the signed command `cmd`, and tx `t1` at 101.
async fn e2_cmd_ctx(dir: &std::path::Path, cmd: &Value) -> Ctx {
    let mut node = NodeMock::default();
    for (rk, h) in [("t1", 101), ("t2", 100)] {
        let (k, v) = h7_poll(rk, &format!("B{h}"), h, json!([]));
        node.polls.insert(k, v);
    }
    node.headers.insert(
        (0, 100),
        vec![json!({"hash": "B100", "height": 100, "chainId": 0, "payloadHash": "P100"})],
    );
    let w = json!({"hash": "t2", "sigs": [], "cmd": cmd.to_string()}).to_string();
    node.payloads.insert(
        "P100".into(),
        json!({"transactions": [[kdx_types::hash::b64url_encode(w.as_bytes()), ""]]}),
    );
    let au = ours_fn_server(|_: &Value| json!({"data": {"block": {"chainId": "0"}}})).await;
    gql_ctx_at(node, dir, &au).await
}

/// Settles ours [t1, t2 (node `n2`)] against the oracle's [t1] at root `root` with `doc`.
async fn e2_settle_tx(
    ctx: &Ctx,
    doc: &str,
    root: &str,
    vars: &[(&str, Value)],
    n2: Value,
) -> (Vec<String>, String) {
    let op = crate::graphql::parse_op("QE2", doc, false).unwrap();
    let tx = |rk: &str, h: u64, mut n: Value| {
        let id = crate::graphql::base64_std(format!("Transaction:[\"B{h}\",\"{rk}\"]").as_bytes());
        n["id"] = json!(id);
        n["hash"] = json!(rk);
        json!({"cursor": "c", "node": n})
    };
    let t1 = tx("t1", 101, json!({}));
    let mut a = h7_page(root, false, vec![t1.clone(), tx("t2", 100, n2)]);
    let mut b = h7_page(root, false, vec![t1]);
    let mut vs = h7_vars(&[("first", json!(2))]);
    vs.extend(h7_vars(vars));
    let (d, note) = ctx
        .gql_settle_op(
            &ctx.targets["a"],
            Some(&op),
            &vs,
            &mut a,
            &mut b,
            7_000_000,
            false,
        )
        .await;
    (c3_unexplained(ctx, &d), note.unwrap_or_default())
}

/// R-P5P9-86 T (`cmd.payload`): a payload that selects no field of its member is the node's
/// only when the document's fragments select the other member alone. A continuation tx
/// under `... on ExecutionPayload { code }` is `{}`: explained; an exec tx served as `{}`
/// there, or `{}` under both fragments: a diff.
#[tokio::test]
async fn e2_t_a_payload_with_no_field_of_its_member() {
    let dir = tempfile::tempdir().unwrap();
    let doc = |frags: &str| {
        format!(
            "query QE2($first: Int, $after: String) {{ transactions(first: $first, after: \
             $after) {{ totalCount pageInfo {{ hasNextPage endCursor }} edges {{ cursor node {{ \
             id hash cmd {{ nonce payload {{ {frags} }} }} }} }} }} }}"
        )
    };
    let exec_only = doc("... on ExecutionPayload { code }");
    let both = doc("... on ExecutionPayload { code } ... on ContinuationPayload { pactId }");
    let cmd = |payload: Value| {
        json!({"networkId": "mainnet01", "nonce": "n2", "meta": {}, "payload": payload,
            "signers": []})
    };
    let cont = cmd(json!({"cont": {"pactId": "p", "step": 1, "rollback": false,
        "data": {}, "proof": null}}));
    let exec = cmd(json!({"exec": {"code": "(+ 1 2)", "data": {}}}));
    let row = |p: Value| json!({"cmd": {"nonce": "n2", "payload": p}});
    let ctx = e2_cmd_ctx(dir.path(), &cont).await;
    let (un, note) = e2_settle_tx(&ctx, &exec_only, "transactions", &[], row(json!({}))).await;
    assert!(un.is_empty(), "{un:?} {note}");
    let (un, note) = e2_settle_tx(&ctx, &both, "transactions", &[], row(json!({}))).await;
    assert!(!un.is_empty() && note.contains("cmd.payload"), "{note}");
    let ctx = e2_cmd_ctx(dir.path(), &exec).await;
    let (un, note) = e2_settle_tx(&ctx, &exec_only, "transactions", &[], row(json!({}))).await;
    assert!(!un.is_empty() && note.contains("cmd.payload"), "{note}");
}

/// R-P5P9-86 U: `transactionsByPublicKey(publicKey)` is checked against the signers of the
/// node's command: a signer's key explains the ours-only row; a wrong key is a diff.
#[tokio::test]
async fn e2_u_the_public_key_filter_against_the_nodes_signers() {
    let dir = tempfile::tempdir().unwrap();
    let cmd = json!({"networkId": "mainnet01", "nonce": "n2", "meta": {},
        "payload": {"exec": {"code": "(+ 1 2)", "data": {}}},
        "signers": [{"pubKey": "pk0"}, {"pubKey": "pk1", "clist": []}]});
    let ctx = e2_cmd_ctx(dir.path(), &cmd).await;
    let doc = "query QE2($publicKey: String!, $first: Int, $after: String) { \
               transactionsByPublicKey(publicKey: $publicKey, first: $first, after: $after) { \
               totalCount pageInfo { hasNextPage endCursor } edges { cursor node { id hash } } } }";
    let root = "transactionsByPublicKey";
    let (un, note) = e2_settle_tx(&ctx, doc, root, &[("publicKey", json!("pk1"))], json!({})).await;
    assert!(un.is_empty(), "{un:?} {note}");
    let (un, note) = e2_settle_tx(&ctx, doc, root, &[("publicKey", json!("pk9"))], json!({})).await;
    assert!(!un.is_empty(), "{note}");
    assert!(note.contains("publicKey: no signer"), "{note}");
}

/// R-P5P9-86 S′, live: the native pair is labelled with its B target (`gql|native`), and a
/// gap is re-read once in the other order. A re-read that agrees is miner skew; one that
/// still differs stays a diff.
#[tokio::test]
async fn e2_s_the_native_pair_rereads_a_gap_in_the_other_order() {
    use std::sync::atomic::{AtomicUsize, Ordering as O};
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let native = serve(Router::new().route(
        "/api/v1/accounts/{a}/balances",
        get(|| async {
            Json(json!({"items": [{"module": "coin", "chain": 0, "balance": "10.904046"}]}))
        }),
    ))
    .await;
    let run = |late: bool| {
        let (nu, native, dir) = (nu.clone(), native.clone(), dir.path().to_path_buf());
        async move {
            let n = Arc::new(AtomicUsize::new(0));
            let gql = ours_fn_server(move |_: &Value| {
                // The first read is a block behind; the re-read (when `late`) has caught up.
                let k = n.fetch_add(1, O::SeqCst);
                let bal = if late && k > 0 { "10.904046" } else { "10.0" };
                json!({"data": {"fungibleAccount": {"chainAccounts": [
                    {"chainId": "0", "balance": bal, "guard": null}]}}})
            })
            .await;
            let own =
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/graphql");
            let allow = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../compare/allowlist.toml");
            let cfg: CompareConfig = toml::from_str(&format!(
                "network = \"mainnet01\"\npairs = [[\"gql\", \"native\"]]\nallowlist = {allow:?}\n\
                 out_dir = {dir:?}\n[node]\nurl = \"{nu}\"\ntimeout_ms = 5000\n[graphql]\n\
                 own_ops_dir = {own:?}\n[targets.gql]\nurl = \"{gql}\"\n\
                 kind = \"kda_indexer_graphql\"\n[targets.native]\nurl = \"{native}\"\n\
                 kind = \"kda_indexer_native\"\n"
            ))
            .unwrap();
            let ctx = Ctx::new(cfg).await.unwrap();
            let op = ctx
                .gql
                .ops
                .iter()
                .find(|o| o.stem == "NodeBalance")
                .unwrap()
                .clone();
            let vars = h7_vars(&[("accountName", json!("k:m"))]);
            ctx.gql_native_balance(&ctx.targets["gql"], &ctx.targets["native"], &op, &vars)
                .await
        }
    };
    let o = run(true).await;
    assert_eq!(o.class, "allowlisted", "{:?}", o.record);
    assert!(o
        .allowlist_ids
        .contains(&"graphql-native-miner-skew".to_string()));
    let rec = o.record.unwrap();
    assert_eq!(rec.b_target, "native");
    let o = run(false).await;
    assert_eq!(o.class, "diff", "{:?}", o.record);
    let rec = o.record.unwrap();
    assert_eq!(rec.b_target, "native");
    assert!(
        rec.diff.iter().any(|e| e.path == "/chain0/balance"),
        "{:?}",
        rec.diff
    );
    // R-P5P9-95 I-4: the re-read that still differs is kept as evidence.
    let ev = rec
        .diff
        .iter()
        .find(|e| e.path == "/~graphql-native-reread-differs")
        .unwrap_or_else(|| panic!("the re-read's evidence: {:?}", rec.diff));
    assert!(ev.a.is_some(), "{ev:?}");
}

/// R-P5P9-105 S′'s node: chain 0 at heights 85..=110, block time 30 s apart (height 100 at
/// the record's time). Each height's coinbase pays `k:m` 0.904046 when listed in `cb`, else
/// `k:o`; `txs` gives a height's transactions' events.
fn s_node(cb: &[u64], txs: &[(u64, Vec<Value>)]) -> NodeMock {
    let base: u64 = 1_794_733_200;
    let mut node = NodeMock {
        tip: 110,
        ..NodeMock::default()
    };
    let enc = |v: Value| kdx_types::hash::b64url_encode(v.to_string().as_bytes());
    for h in 85..=110u64 {
        let t = (base as i64 + (h as i64 - 100) * 30) as u64 * 1_000_000;
        node.headers.insert(
            (0, h),
            vec![json!({"hash": format!("H{h}"), "height": h, "chainId": 0,
                "creationTime": t, "payloadHash": format!("P{h}")})],
        );
        let miner = if cb.contains(&h) { "k:m" } else { "k:o" };
        let coinbase = json!({"events": [s_transfer("", miner, json!(0.904046))]});
        let outs: Vec<Value> = txs
            .iter()
            .filter(|(x, _)| *x == h)
            .map(|(_, evs)| json!(["", enc(json!({"events": evs}))]))
            .collect();
        node.payloads.insert(
            format!("P{h}"),
            json!({"transactions": outs, "coinbase": enc(coinbase)}),
        );
    }
    node
}

fn s_transfer(from: &str, to: &str, amount: Value) -> Value {
    json!({"name": "TRANSFER", "module": {"name": "coin"}, "params": [from, to, amount]})
}

/// The stored referee over one NodeBalance record of k:m on chain 0: ours' GraphQL `ours`
/// (a JSON number, as stored), native `native`.
async fn s_referee(node: NodeMock, ours: &str, native: &str) -> crate::referee::Report {
    use crate::report::{DiffEntry, DiffKind};
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(node).await;
    let ops = c3_ops(dir.path());
    std::fs::copy(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../compare/graphql/NodeBalance.graphql"),
        ops.join("NodeBalance.graphql"),
    )
    .unwrap();
    let cfg = c3_cfg(
        &nu,
        "http://127.0.0.1:9/graphql",
        "https://oracle.invalid/graphql",
        dir.path(),
        &ops,
        "",
    );
    let mut r = e2_stored(
        "NodeBalance",
        json!({"accountName": "k:m"}),
        Value::Null,
        Value::Null,
        vec![DiffEntry {
            path: "/chain0/balance".into(),
            kind: DiffKind::Changed,
            a: Some(serde_json::from_str(ours).unwrap()),
            b: Some(json!(native)),
        }],
    );
    r.b_target = "node".into();
    e2_referee(cfg, dir.path(), &[r]).await
}

/// R-P5P9-105 S′: the gap native − GraphQL is explained when it equals the account's net
/// change on the node: the sum of its coin TRANSFER events in 1 to 3 consecutive canonical
/// blocks of the record's height window, exact-decimal, with half an f64 ulp on ours' number.
#[tokio::test]
async fn i_s_the_gap_is_the_accounts_net_change_on_the_node() {
    let ok = |rep: &crate::referee::Report, case: &str| {
        assert_eq!((rep.explained, rep.unexplained), (1, 0), "{case}: {rep:?}");
    };
    let no = |rep: &crate::referee::Report, case: &str| {
        assert_eq!((rep.explained, rep.unexplained), (0, 1), "{case}: {rep:?}");
    };
    // (a) #75: one coinbase + 1e-12, the f64 rounding of ours' 60263.91224935622.
    let rep = s_referee(
        s_node(&[100], &[]),
        "60263.91224935622",
        "60264.816295356221",
    )
    .await;
    ok(&rep, "a");
    // (b) a miner block: its coinbase and a fee of 1.08e-5 paid to k:m.
    let fee = vec![(100, vec![s_transfer("k:p", "k:m", json!(0.0000108))])];
    let rep = s_referee(s_node(&[100], &fee), "10.0", "10.9040568").await;
    ok(&rep, "b");
    // A residual of 1e-6 more: unexplained.
    let rep = s_referee(s_node(&[100], &fee), "10.0", "10.9040578").await;
    no(&rep, "b residual");
    // (c) #1711: 25 rows in one block (the coinbase and 24 receive legs), gap
    // 1,667.96549197 + the f64 rounding of ours' number.
    let mut legs: Vec<Value> = (0..23)
        .map(|_| s_transfer("", "k:m", json!(69.5)))
        .collect();
    legs.push(s_transfer("", "k:m", json!({"decimal": "68.56144597"})));
    let rep = s_referee(
        s_node(&[100], &[(100, legs)]),
        "60263.91224935622",
        "61931.877741326221",
    )
    .await;
    ok(&rep, "c");
    // (d) a negative gap equal to a debit of k:m.
    let debit = vec![(100, vec![s_transfer("k:m", "k:x", json!(2.5))])];
    let rep = s_referee(s_node(&[], &debit), "10.0", "7.5").await;
    ok(&rep, "d");
    // Credits of 1.0 in four consecutive blocks and a gap of 4.0: more than 3 blocks.
    let four: Vec<(u64, Vec<Value>)> = (98..=101)
        .map(|h| (h, vec![s_transfer("k:p", "k:m", json!(1.0))]))
        .collect();
    let rep = s_referee(s_node(&[], &four), "10.0", "14.0").await;
    no(&rep, "four blocks");
    let rep = s_referee(s_node(&[], &four), "10.0", "13.0").await;
    ok(&rep, "three of them");
}

/// R-P5P9-86 S′ with R-P5P9-87, stored: a CG-5 NodeBalance record (labelled `node`) is miner
/// skew only when its gap is k (1 to 3) coinbases of one amount that the node's canonical
/// blocks in the record's height window pay the account. Here the node pays k:m at heights
/// 100 and 101 (the record's time is height 100's): a gap of one coinbase is explained; three
/// (the node shows two), or a gap that is no multiple, stay unexplained.
#[tokio::test]
async fn e2_s_the_referee_settles_a_stored_native_gap_by_the_nodes_coinbases() {
    use crate::report::{DiffEntry, DiffKind};
    let dir = tempfile::tempdir().unwrap();
    let base: u64 = 1_794_733_200;
    let mut node = NodeMock {
        tip: 110,
        ..NodeMock::default()
    };
    for h in 85..=110u64 {
        let t = (base as i64 + (h as i64 - 100) * 30) as u64 * 1_000_000;
        node.headers.insert(
            (0, h),
            vec![json!({"hash": format!("H{h}"), "height": h, "chainId": 0,
                "creationTime": t, "payloadHash": format!("P{h}")})],
        );
        let miner = if h == 100 || h == 101 { "k:m" } else { "k:o" };
        let cb = json!({"events": [{"name": "TRANSFER", "module": {"name": "coin"},
            "params": ["", miner, 0.904046]}]})
        .to_string();
        node.payloads.insert(
            format!("P{h}"),
            json!({"transactions": [], "coinbase": kdx_types::hash::b64url_encode(cb.as_bytes())}),
        );
    }
    let nu = node_server(node).await;
    let ops = c3_ops(dir.path());
    std::fs::copy(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../compare/graphql/NodeBalance.graphql"),
        ops.join("NodeBalance.graphql"),
    )
    .unwrap();
    let cfg = || {
        c3_cfg(
            &nu,
            "http://127.0.0.1:9/graphql",
            "https://oracle.invalid/graphql",
            dir.path(),
            &ops,
            "",
        )
    };
    let rec = |native: &str| {
        let mut r = e2_stored(
            "NodeBalance",
            json!({"accountName": "k:m"}),
            Value::Null,
            Value::Null,
            vec![DiffEntry {
                path: "/chain0/balance".into(),
                kind: DiffKind::Changed,
                a: Some(json!("10.0")),
                b: Some(json!(native)),
            }],
        );
        r.b_target = "node".into();
        r
    };
    let rep = e2_referee(cfg(), dir.path(), &[rec("10.904046")]).await;
    assert_eq!((rep.explained, rep.unexplained), (1, 0), "{rep:?}");
    assert_eq!(rep.by_class.get("graphql-native-miner-skew"), Some(&1));
    // R-P5P9-97 S′ (#117's shape): the stored GraphQL balance is a JSON number, read as an
    // exact decimal; R-P5P9-98: native's trailing zeros past 12 places are ignored. A number
    // whose gap is no coinbase multiple stays unexplained.
    for (ours, native, explained) in [
        ("10.0", "10.904046000000000", 1),
        ("10", "10.904046", 1),
        ("10.5", "10.904046", 0),
    ] {
        let mut r = rec(native);
        r.diff[0].a = Some(serde_json::from_str(ours).unwrap());
        let rep = e2_referee(cfg(), dir.path(), &[r]).await;
        assert_eq!(
            (rep.explained, rep.unexplained),
            (explained, 1 - explained),
            "{ours} {native}: {rep:?}"
        );
    }
    // R-P5P9-95 I-4: 9.095954 is GraphQL one coinbase above native (a skew leaves GraphQL
    // below native only).
    for native in ["12.712138", "10.5", "9.095954"] {
        let rep = e2_referee(cfg(), dir.path(), &[rec(native)]).await;
        assert_eq!(
            (rep.explained, rep.unexplained),
            (0, 1),
            "{native}: {rep:?}"
        );
    }
    // R-P5P9-95 I-4: a record whose live re-read still differed is refused.
    let mut r = rec("10.904046");
    r.diff.push(DiffEntry {
        path: "/~graphql-native-reread-differs".into(),
        kind: DiffKind::Changed,
        a: Some(json!({"reread": "differs"})),
        b: None,
    });
    let rep = e2_referee(cfg(), dir.path(), &[r]).await;
    assert_eq!((rep.explained, rep.unexplained), (0, 1), "{rep:?}");
}
