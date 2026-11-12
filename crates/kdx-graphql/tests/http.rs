//! `/graphql` over HTTP: the GQ-9 limits answer `LIMIT` quickly, introspection answers in
//! full, unserved fields answer `NOT_SUPPORTED`, page sizes are clamped (Task 2 Step 2).

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use kdx_api::{Api, ApiSettings, ApiState, Store, TipWatch};
use kdx_graphql::{build_schema, router, GraphqlSettings};
use serde_json::{json, Value};
use tower::ServiceExt;

struct Fixture {
    _dir: tempfile::TempDir,
    st: Arc<ApiState>,
}

fn fixture() -> Fixture {
    let d = tempfile::tempdir().unwrap();
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
        rate_per_ip: 20,
        rate_burst: 40,
        trusted_proxies: Vec::new(),
        read_threads: 2,
        ready_max_lag: 10,
        max_event_lists: 64,
    };
    let st = Api::new(store, settings, TipWatch::default()).state();
    Fixture { _dir: d, st }
}

async fn post(f: &Fixture, body: String) -> (StatusCode, Value, Duration) {
    let app = router(build_schema(f.st.clone(), GraphqlSettings::default(), None))
        .with_state(f.st.clone());
    let req = Request::post("/graphql")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let t = Instant::now();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 64 << 20)
        .await
        .unwrap();
    let el = t.elapsed();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        el,
    )
}

async fn query(f: &Fixture, q: &str) -> (StatusCode, Value, Duration) {
    post(f, json!({ "query": q }).to_string()).await
}

fn limit_of(v: &Value) -> Option<&str> {
    let e = &v["errors"][0]["extensions"];
    (e["code"] == "LIMIT").then(|| e["limit"].as_str().unwrap_or(""))
}

fn fields(n: usize) -> String {
    (0..n).map(|i| format!("h{i}: hash ")).collect()
}

#[tokio::test]
async fn limits_answer_limit_fast() {
    let f = fixture();
    let deep = format!(
        "{{ blocksFromHeight(startHeight: 0, first: 1) {{ edges {{ node {{ {} hash {} }} }} }} }}",
        "parent { ".repeat(7),
        "}".repeat(7)
    );
    let complex = format!(
        "{{ blocksFromHeight(startHeight: 0, first: 100) {{ edges {{ node {{ {} }} }} }} }}",
        fields(18)
    );
    let aliases: String = (0..100)
        .map(|i| format!("t{i}: transactions(blockHash: \"x\", first: 100) {{ edges {{ node {{ hash }} }} }} "))
        .collect();
    let big = format!("{{ lastBlockHeight }} #{}", "x".repeat(33 * 1024));
    for (q, want) in [
        (deep, "depth"),
        (complex, "complexity"),
        (format!("{{ {aliases} }}"), "complexity"),
        (big, "query_size"),
    ] {
        let (s, v, el) = query(&f, &q).await;
        assert_eq!((s, limit_of(&v)), (StatusCode::OK, Some(want)), "{v}");
        assert!(el < Duration::from_millis(100), "{want}: {el:?}");
    }
    let batch: Vec<Value> = (0..11)
        .map(|_| json!({"query": "{ graphConfiguration { version } }"}))
        .collect();
    let (s, v, el) = post(&f, Value::Array(batch).to_string()).await;
    assert_eq!((s, limit_of(&v)), (StatusCode::OK, Some("batch")), "{v}");
    assert!(el < Duration::from_millis(100));
    // Variables over 64 KB, body over 128 KB.
    let (_, v, _) = post(
        &f,
        json!({"query": "{ lastBlockHeight }", "variables": {"a": "x".repeat(65 << 10)}})
            .to_string(),
    )
    .await;
    assert_eq!(limit_of(&v), Some("variables_size"), "{v}");
    let (s, v, _) = post(
        &f,
        json!({"query": "{ lastBlockHeight }", "variables": {"a": "x".repeat(130 << 10)}})
            .to_string(),
    )
    .await;
    assert_eq!(
        (s, limit_of(&v)),
        (StatusCode::PAYLOAD_TOO_LARGE, Some("body_size")),
        "{v}"
    );
}

#[tokio::test]
async fn introspection_and_unserved_fields() {
    let f = fixture();
    let (s, v, _) = query(&f, INTROSPECTION).await;
    assert_eq!(s, StatusCode::OK);
    assert!(v.get("errors").is_none(), "{v}");
    let types = v["data"]["__schema"]["types"].as_array().unwrap();
    assert!(types.iter().any(|t| t["name"] == "TransactionResult"));
    // pactQuery is never served; the rest of the operation still answers.
    let (_, v, _) = query(
        &f,
        "{ graphConfiguration { version minimumBlockHeight } pactQuery(pactQuery: [{chainId: \"0\", code: \"(+ 1 2)\"}]) { result } }",
    )
    .await;
    assert_eq!(v["errors"][0]["extensions"]["code"], "NOT_SUPPORTED", "{v}");
    assert_eq!(
        v["data"]["graphConfiguration"]["minimumBlockHeight"], 0,
        "{v}"
    );
    // Variables keep exact numbers (the workspace serde_json has arbitrary_precision).
    let (_, v, _) = post(
        &f,
        json!({"query": "query($n: Int) { blocksFromHeight(startHeight: 0, first: $n) { pageInfo { hasNextPage } } }", "variables": {"n": 1000}})
            .to_string(),
    )
    .await;
    assert_eq!(v["errors"][0]["extensions"]["code"], "NOT_SUPPORTED", "{v}");
    // A batch answers an array.
    let (_, v, _) = post(&f, json!([{"query": "{ graphConfiguration { version } }"}, {"query": "{ graphConfiguration { version } }"}]).to_string()).await;
    assert_eq!(v.as_array().map(Vec::len), Some(2), "{v}");
    // Bad requests.
    for body in [
        "not json",
        "[]",
        "{\"query\": 5}",
        "{\"query\": \"{ x }\", \"variables\": [1]}",
    ] {
        let (s, v, _) = post(&f, body.to_string()).await;
        assert_eq!(
            (s, v["errors"][0]["extensions"]["code"].as_str()),
            (StatusCode::BAD_REQUEST, Some("BAD_USER_INPUT")),
            "{body}"
        );
    }
}

#[tokio::test]
async fn get_and_upgrade() {
    let f = fixture();
    let app = router(build_schema(f.st.clone(), GraphqlSettings::default(), None))
        .with_state(f.st.clone());
    let req = Request::get("/graphql?query=%7B%20graphConfiguration%20%7B%20version%20%7D%20%7D")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: Value = serde_json::from_slice(
        &axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        v["data"]["graphConfiguration"]["version"],
        env!("CARGO_PKG_VERSION")
    );
    let req = Request::get("/graphql")
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let v: Value = serde_json::from_slice(
        &axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(v["errors"][0]["extensions"]["code"], "NOT_SUPPORTED");
}

/// graphql-js `getIntrospectionQuery()` (v16, default options).
const INTROSPECTION: &str = r#"
query IntrospectionQuery {
  __schema {
    queryType { name }
    mutationType { name }
    subscriptionType { name }
    types { ...FullType }
    directives { name description locations args { ...InputValue } }
  }
}
fragment FullType on __Type {
  kind name description
  fields(includeDeprecated: true) {
    name description
    args { ...InputValue }
    type { ...TypeRef }
    isDeprecated deprecationReason
  }
  inputFields { ...InputValue }
  interfaces { ...TypeRef }
  enumValues(includeDeprecated: true) { name description isDeprecated deprecationReason }
  possibleTypes { ...TypeRef }
}
fragment InputValue on __InputValue { name description type { ...TypeRef } defaultValue }
fragment TypeRef on __Type {
  kind name
  ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } } } }
}
"#;
