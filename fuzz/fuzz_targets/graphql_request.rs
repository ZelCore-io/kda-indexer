#![no_main]
//! A POST body (any bytes) to `/graphql` over an empty store: JSON parse, batch and size
//! caps, document parse, validation and the GQ-9 limits, then execution against no data.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use axum::Router;
use kdx_api::{Api, ApiSettings, ApiState, Store, TipWatch};
use kdx_graphql::{build_schema, router, GraphqlSettings};
use libfuzzer_sys::fuzz_target;
use tower::ServiceExt;

struct Fx {
    _dir: tempfile::TempDir,
    app: Router,
    rt: tokio::runtime::Runtime,
}

fn fx() -> &'static Fx {
    static FX: OnceLock<Fx> = OnceLock::new();
    FX.get_or_init(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let g = kdx_hot::Geometry {
            max_gb: 1,
            growth_mb: 16,
        };
        let hot = Arc::new(kdx_hot::HotStore::open(&dir.path().join("hot"), g).expect("hot"));
        let store = Arc::new(Store::open(hot, dir.path()).expect("store"));
        let settings = ApiSettings {
            network: "mainnet01".into(),
            deadline: Duration::from_secs(5),
            max_in_flight: 16,
            max_body_bytes: 8 << 20,
            v1txs_max_body_bytes: 8 << 20,
            rate_per_ip: 20,
            rate_burst: 40,
            trusted_proxies: Vec::new(),
            read_threads: 2,
            ready_max_lag: 10,
            max_event_lists: 64,
        };
        let st: Arc<ApiState> = Api::new(store, settings, TipWatch::default()).state();
        let app = router(build_schema(st.clone(), GraphqlSettings::default(), None)).with_state(st);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        Fx { _dir: dir, app, rt }
    })
}

fuzz_target!(|data: &[u8]| {
    let f = fx();
    let req = Request::post("/graphql")
        .header("content-type", "application/json")
        .body(Body::from(data.to_vec()))
        .expect("request");
    let app = f.app.clone();
    f.rt.block_on(async move {
        let resp = app.oneshot(req).await.expect("infallible");
        let _ = axum::body::to_bytes(resp.into_body(), 64 << 20).await;
    });
});
