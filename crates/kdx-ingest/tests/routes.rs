//! Finding fixtures served through the real routes (plan Task 13, T-3): a store populated by
//! the tail writer from the captured mainnet blocks (`tests/fixtures/chain0_3125000.json.zst`),
//! then asked over the compat and native routers.
//!
//! - G1: a genesis-block tx is served by `/txs/tx` and `/txs/txs` but not listed by
//!   `/txs/recent` (ruling R3E-1).
//! - R3E-8: within a height our order is the node's payload order (the native block tx list),
//!   whatever order chainweb-data happens to return.

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::*;
use kdx_api::compat::SearchSettings;
use kdx_api::search::SearchCtx;
use kdx_api::{Api, ApiSettings, Store, TipWatch};
use kdx_types::hash::b64url_decode;
use serde_json::Value;
use tower::ServiceExt;

struct Served {
    _dir: tempfile::TempDir,
    app: axum::Router,
}

/// `places` applied to a fresh single-chain store, behind both routers.
fn served(places: &[Place]) -> Served {
    let pool = pool();
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), 10_000, 10_000, 10);
    apply_linear(&ctx, &pool, places, 4);
    // The meta the ingest context writes at start: the network and the chain graph give the
    // store its genesis heights.
    let mut w = ctx.hot.rw().unwrap();
    w.set_meta(kdx_hot::env::meta::NETWORK, b"mainnet01")
        .unwrap();
    w.set_meta(kdx_hot::env::meta::CHAIN_GRAPH, b"[[0,[0]]]")
        .unwrap();
    w.commit().unwrap();
    let store = Arc::new(Store::open(ctx.hot.clone(), dir.path()).unwrap());
    let settings = ApiSettings {
        network: "mainnet01".into(),
        deadline: Duration::from_secs(10),
        max_in_flight: 16,
        max_body_bytes: 8 << 20,
        v1txs_max_body_bytes: 8 << 20,
        rate_per_ip: 1000,
        rate_burst: 1000,
        trusted_proxies: Vec::new(),
        read_threads: 2,
        ready_max_lag: 10,
        max_event_lists: 64,
    };
    let api = Api::new(store, settings, TipWatch::default());
    let search = SearchCtx::new(SearchSettings::default(), None);
    let app = axum::Router::new()
        .merge(kdx_api::compat::router(search.clone()))
        .merge(kdx_api::native::router(search))
        .with_state(api.state());
    Served { _dir: dir, app }
}

async fn get(s: &Served, uri: &str) -> (StatusCode, String) {
    let mut req = Request::get(uri).body(Body::empty()).unwrap();
    req.extensions_mut()
        .insert(axum::extract::ConnectInfo(std::net::SocketAddr::from((
            [127, 0, 0, 1],
            1,
        ))));
    let resp = s.app.clone().oneshot(req).await.unwrap();
    let st = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 64 << 20)
        .await
        .unwrap();
    (st, String::from_utf8_lossy(&bytes).into_owned())
}

/// The request keys of pool block `i`, in the node's payload order (decoded here from the
/// captured payload, independent of the indexer).
fn payload_keys(pool: &[Value], i: usize) -> Vec<String> {
    pool[i]["payload"]["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| {
            let raw = b64url_decode(t[0].as_str().unwrap()).unwrap();
            let tx: Value = serde_json::from_slice(&raw).unwrap();
            tx["hash"].as_str().unwrap().to_owned()
        })
        .collect()
}

/// G1: the genesis block of chain 0 holds user txs. `/txs/tx` and `/txs/txs` serve them (as
/// chainweb-data does); the list routes do not show them.
#[tokio::test]
async fn g1_genesis_txs_are_served_by_txs_txs_and_hidden_from_lists() {
    let pool = pool();
    // Pool block 3 (21 txs) as the genesis block of chain 0, then a normal block after it.
    let s = served(&[
        Place {
            pool: 3,
            height: 0,
            br: 0,
            parent_br: 0,
        },
        Place {
            pool: 4,
            height: 1,
            br: 0,
            parent_br: 0,
        },
    ]);
    let genesis_keys = payload_keys(&pool, 3);
    let later_keys = payload_keys(&pool, 4);
    assert!(genesis_keys.len() >= 2 && !later_keys.is_empty());
    let rk = &genesis_keys[0];
    for route in ["/txs/txs", "/txs/tx"] {
        let (st, body) = get(&s, &format!("{route}?requestkey={rk}")).await;
        assert_eq!(st, StatusCode::OK, "{route}: {body}");
        let v: Value = serde_json::from_str(&body).unwrap();
        let rows = v.as_array().map_or(vec![v.clone()], |a| a.clone());
        assert_eq!(rows.len(), 1, "{route}: {body}");
        assert_eq!(rows[0]["height"], 0, "{route}: {body}");
        assert_eq!(rows[0]["requestKey"], rk.as_str(), "{route}: {body}");
    }
    // The recent list shows the later block's txs and none of the genesis block's.
    let (st, body) = get(&s, "/txs/recent").await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap();
    let listed: Vec<&str> = v
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["requestKey"].as_str().unwrap())
        .collect();
    assert!(!listed.is_empty(), "{body}");
    for k in &genesis_keys {
        assert!(!listed.contains(&k.as_str()), "genesis tx {k} listed");
    }
    assert!(listed.iter().all(|k| later_keys.iter().any(|l| l == k)));
    // The native API serves them everywhere.
    let (st, body) = get(&s, &format!("/api/v1/txs/{rk}")).await;
    assert_eq!(st, StatusCode::OK, "{body}");
}

/// R3E-8: a block's txs come back in the node's payload order from the native routes, in the
/// ascending default and reversed with `order=desc`.
#[tokio::test]
async fn r3e8_same_height_order_is_the_node_order() {
    let pool = pool();
    let place = Place {
        pool: 3,
        height: 3_125_003,
        br: 0,
        parent_br: 0,
    };
    let s = served(&[place]);
    let want = payload_keys(&pool, 3);
    assert!(want.len() > 10, "the fixture block has {} txs", want.len());
    // The check means something only if the node order differs from the key order.
    let mut sorted = want.clone();
    sorted.sort();
    assert_ne!(want, sorted, "payload order equals sorted order");
    sorted.reverse();
    assert_ne!(want, sorted, "payload order equals reverse-sorted order");
    let hash = hh(place.height, place.br);
    let keys = |body: &str| -> Vec<String> {
        let v: Value = serde_json::from_str(body).unwrap_or(Value::Null);
        v["items"]
            .as_array()
            .unwrap_or_else(|| panic!("no items: {body}"))
            .iter()
            .map(|r| {
                ["request_key", "requestKey"]
                    .iter()
                    .find_map(|k| r[*k].as_str())
                    .unwrap_or_else(|| panic!("no request key: {r}"))
                    .to_owned()
            })
            .collect()
    };
    let (st, body) = get(&s, &format!("/api/v1/blocks/{hash}/txs?limit=100")).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(keys(&body), want);
    let (st, body) = get(
        &s,
        &format!("/api/v1/blocks/{hash}/txs?limit=100&order=desc"),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let mut rev = want.clone();
    rev.reverse();
    assert_eq!(keys(&body), rev);
}
