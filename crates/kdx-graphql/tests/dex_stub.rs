//! Stock's DEX root fields (stock-gap 4a): a document that names them validates; each answers
//! `NOT_SUPPORTED` with a null in its place, and the document's other fields still answer.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use kdx_api::{Api, ApiSettings, ApiState, Store, TipWatch};
use kdx_graphql::{build_schema, router, GraphqlSettings};
use serde_json::{json, Value};
use tower::ServiceExt;

fn state(d: &tempfile::TempDir) -> Arc<ApiState> {
    let g = kdx_hot::Geometry {
        max_gb: 1,
        growth_mb: 16,
    };
    let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap());
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
    Api::new(store, settings, TipWatch::default()).state()
}

async fn query(st: &Arc<ApiState>, q: &str) -> (StatusCode, Value) {
    let app =
        router(build_schema(st.clone(), GraphqlSettings::default(), None)).with_state(st.clone());
    let req = Request::post("/graphql")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "query": q }).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 16 << 20)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn dex_fields_validate_and_answer_not_supported() {
    let d = tempfile::tempdir().unwrap();
    let st = state(&d);
    let fields = [
        ("tokens", "tokens(first: 2) { edges { cursor node { id name chainId address } } pageInfo { hasNextPage } }"),
        ("pools", "pools(first: 2, orderBy: VOLUME_7D_DESC) { totalCount edges { cursor node { id key reserve0 totalSupply tvlUsd apr24h volume7dUsd token0 { name } charts(timeFrame: WEEK) { tvl { timestamp value } } } } }"),
        ("pool", "pool(id: \"x\", timeFrame: DAY, type: SWAP, first: 1) { id transactions(type: ADD_LIQUIDITY) { totalCount } }"),
        ("poolTransactions", "poolTransactions(pairId: \"1\", type: REMOVE_LIQUIDITY) { totalCount edges { node { maker amount0In requestkey transactionType } } }"),
        ("liquidityPositions", "liquidityPositions(walletAddress: \"k:a\", orderBy: APR_DESC) { totalCount edges { node { valueUsd apr24h pair { id } } } }"),
        ("dexMetrics", "dexMetrics(startDate: \"2026-11-01T00:00:00Z\", protocolAddress: \"n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange\") { totalPools currentTvlUsd tvlHistory { timestamp value } volumeHistory { value } totalVolumeUsd }"),
        ("tokenPrice", "tokenPrice(tokenAddress: \"coin\") { id priceInKda priceInUsd protocolAddress updatedAt token { id } }"),
        ("tokenPrices", "tokenPrices { id priceInKda }"),
    ];
    for (name, f) in fields {
        let (s, v) = query(
            &st,
            &format!("{{ graphConfiguration {{ minimumBlockHeight }} {f} }}"),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{name}: {v}");
        let errs = v["errors"]
            .as_array()
            .unwrap_or_else(|| panic!("{name}: {v}"));
        assert_eq!(errs.len(), 1, "{name}: {v}");
        assert_eq!(
            errs[0]["extensions"]["code"], "NOT_SUPPORTED",
            "{name}: {v}"
        );
        assert_eq!(errs[0]["path"][0], name, "{name}: {v}");
        assert_eq!(
            v["data"]["graphConfiguration"]["minimumBlockHeight"], 0,
            "{name}: {v}"
        );
    }
    // A wrong argument still fails validation (the schema is stock's, not a catch-all).
    let (_, v) = query(&st, "{ pools(orderBy: NOPE) { totalCount } }").await;
    assert_eq!(
        v["errors"][0]["extensions"]["code"], "GRAPHQL_VALIDATION_FAILED",
        "{v}"
    );
}

/// `dexMetrics` dates parse as stock's RFC 3339 `DateTime`; malformed ones fail validation.
#[tokio::test]
async fn dex_metrics_dates() {
    let d = tempfile::tempdir().unwrap();
    let st = state(&d);
    for ok in [
        "2026-11-01T00:00:00Z",
        "2026-11-01T00:00:00.123Z",
        "2024-02-29T23:59:59+02:00",
        "2026-11-01t00:00:00.123456789z",
    ] {
        let (_, v) = query(
            &st,
            &format!("{{ dexMetrics(startDate: \"{ok}\") {{ totalPools }} }}"),
        )
        .await;
        assert_eq!(
            v["errors"][0]["extensions"]["code"], "NOT_SUPPORTED",
            "{ok}: {v}"
        );
    }
    for bad in [
        "2026-11-01",
        "2025-02-29T00:00:00Z",
        "2026-13-01T00:00:00Z",
        "2026-11-01T00:00:00",
        "2026-11-01T00:00:00.Z",
        "x",
        "2026-11-01T00:00:00.1234567890Z",
    ] {
        let (_, v) = query(
            &st,
            &format!("{{ dexMetrics(startDate: \"{bad}\") {{ totalPools }} }}"),
        )
        .await;
        assert_ne!(
            v["errors"][0]["extensions"]["code"], "NOT_SUPPORTED",
            "{bad}: {v}"
        );
    }
}
