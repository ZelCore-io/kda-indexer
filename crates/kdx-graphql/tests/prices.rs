//! `lastTokenPriceInKda` against real data: the 49 distinct `lastTokenPriceInKda` requests of
//! the traffic corpus (`tests/corpus/traffic-2026-11`), over a store holding the newest swap
//! of each requested token as of the G4 replay (2026-11-24, 06:42 UTC), read from mainnet.
//! The stock indexer answered 14 of these requests in that replay (the rest timed out at the
//! load balancer); `stock` in `tests/data/token-swaps-2026-11-24.jsonl` is its answer.
//! Each swap carries its tx's `requestKey` and trusted DEX `SWAP` events (name and params, read
//! from mainnet with the result), which authenticate the result's legs.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use kdx_api::{Api, ApiSettings, Store, TipWatch};
use kdx_graphql::{build_schema, router, GraphqlSettings};
use kdx_hot::prices;
use kdx_types::TxKey;
use serde_json::{json, Value};
use tower::ServiceExt;

/// The corpus document, byte for byte.
const DOC: &str =
    "query Query($moduleName: String!) {\n  lastTokenPriceInKda(moduleName: $moduleName)\n}";

fn fixture() -> Vec<Value> {
    include_str!("data/token-swaps-2026-11-24.jsonl")
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

async fn ask(app: &axum::Router, module: &str) -> Value {
    let req = Request::post("/graphql")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"query": DOC, "variables": {"moduleName": module}, "operationName": "Query"})
                .to_string(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn corpus_requests_match_the_recorded_stock_prices() {
    let rows = fixture();
    assert_eq!(rows.len(), 49);
    let d = tempfile::tempdir().unwrap();
    let g = kdx_hot::Geometry {
        max_gb: 1,
        growth_mb: 16,
    };
    let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap());
    let mut unauthenticated: Vec<&str> = Vec::new();
    {
        let mut txn = hot.rw().unwrap();
        for r in &rows {
            let Some(s) = r["swap"].as_object() else {
                continue;
            };
            let chain = s["chain"].as_u64().unwrap() as u8;
            let key = TxKey::new(s["height"].as_u64().unwrap() as u32, chain, 1);
            // The tx's trusted DEX SWAP events (read from mainnet with the result).
            let events: Vec<Value> = s["events"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|e| prices::is_swap_event_name(e["name"].as_str().unwrap()))
                .map(|e| e["params"].clone())
                .collect();
            assert!(!events.is_empty(), "{}", r["module"]);
            match prices::swap_of(
                key,
                s["creationTime"].as_u64().unwrap(),
                &s["data"],
                &events,
            ) {
                Some(swap) => {
                    prices::upsert(&mut txn, chain, &swap, None).unwrap();
                }
                None => unauthenticated.push(r["module"].as_str().unwrap()),
            }
        }
        txn.commit().unwrap();
    }
    let store = Arc::new(Store::open(hot, d.path()).unwrap());
    let settings = ApiSettings {
        network: "mainnet01".into(),
        deadline: Duration::from_secs(5),
        max_in_flight: 16,
        max_body_bytes: 8 << 20,
        v1txs_max_body_bytes: 8 << 20,
        v1txs_deadline: Duration::from_secs(30),
        rate_per_ip: 20,
        rate_burst: 40,
        trusted_proxies: Vec::new(),
        read_threads: 2,
        ready_max_lag: 10,
        max_event_lists: 64,
    };
    let st = Api::new(store, settings, TipWatch::default()).state();
    let schema = build_schema(st.clone(), GraphqlSettings::default(), None);
    let app = router(schema.clone()).with_state(st);

    // N-06: the request deadline governs the scan. Spent before the read: an error, not an
    // answer computed past it.
    let spent = async_graphql::Request::new(DOC)
        .variables(async_graphql::Variables::from_json(
            json!({"moduleName": "kaddex_kdx"}),
        ))
        .data(kdx_graphql::ReqCtx {
            deadline: std::time::Instant::now(),
        });
    let r = schema.execute(spent).await;
    assert!(
        !r.errors.is_empty(),
        "answered past the deadline: {:?}",
        r.data
    );

    // free.wiza: the tx is an arbitrage (kaddex.exchange swap-exact-in, then
    // n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange swap-exact-in); its result is
    // the sushi hop, which the trusted sushi SWAP event backs. Every row authenticates.
    assert!(unauthenticated.is_empty(), "{unauthenticated:?}");
    let (mut compared, mut matched, mut priced) = (0, 0, 0);
    let mut differ: Vec<&str> = Vec::new();
    for r in &rows {
        let module = r["module"].as_str().unwrap();
        let v = ask(&app, module).await;
        assert!(v.get("errors").is_none(), "{module}: {v}");
        let got = &v["data"]["lastTokenPriceInKda"];
        // A string (stock's `toFixed(12)` text) when a swap is stored, else null.
        let stored = r["swap"].is_object() && !unauthenticated.contains(&module);
        assert_eq!(got.is_string(), stored, "{module}: {v}");
        priced += usize::from(got.is_string());
        if let Some(stock) = r["stock"].as_str() {
            compared += 1;
            if got.as_str() == Some(stock) {
                matched += 1;
            } else {
                eprintln!("{module}: ours {got}, stock {stock}");
                differ.push(module);
            }
        }
    }
    eprintln!(
        "lastTokenPriceInKda: {priced} of 49 priced; {matched} of {compared} stock answers matched"
    );
    assert_eq!((compared, matched), (14, 14));
    assert!(differ.is_empty(), "{differ:?}");

    // Stock's LIKE: `_` is a wildcard and a part of a name matches; no match is null.
    let v = ask(&app, "kaddex_kdx").await;
    assert_eq!(v["data"]["lastTokenPriceInKda"], "0.000917373138", "{v}");
    let v = ask(&app, "free.no-such-token").await;
    assert_eq!(v["data"]["lastTokenPriceInKda"], Value::Null, "{v}");
}
