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
        fields(20)
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
    // The oracle sends the code top-level: a copy rides along with `extensions.code`.
    assert_eq!(v["errors"][0]["code"], "NOT_SUPPORTED", "{v}");
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
    assert!(v["errors"].is_null(), "{v}");
    assert_eq!(
        v["data"]["blocksFromHeight"]["pageInfo"]["hasNextPage"], false,
        "{v}"
    );
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
const INTROSPECTION: &str = include_str!("introspection.graphql");

/// A fragment bomb (C-1): `F{i}` spreads `F{i-1}` twice, so `F{n}` expands to 2^n fields in
/// about 50 bytes per level.
fn bomb(n: usize) -> String {
    let mut s = String::from("fragment F0 on Transaction { hash } ");
    for i in 1..=n {
        s += &format!(
            "fragment F{i} on Transaction {{ ...F{p} ...F{p} }} ",
            p = i - 1
        );
    }
    s + &format!("{{ transactions(first: 1) {{ edges {{ node {{ ...F{n} }} }} }} }}")
}

/// CPU time of the calling thread.
fn thread_cpu() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, writable timespec for the duration of the call.
    let r = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    assert_eq!(r, 0);
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

#[tokio::test]
async fn fragment_bomb_is_refused_before_any_unbounded_walk() {
    let f = fixture();
    // A current-thread runtime: the whole request runs on this thread, so its CPU time is
    // the request's.
    for n in [20, 26, 28, 40, 60] {
        let q = bomb(n);
        assert!(q.len() < 4096, "{}", q.len());
        let cpu = thread_cpu();
        let (s, v, el) = query(&f, &q).await;
        let cpu = thread_cpu() - cpu;
        assert_eq!(
            (s, limit_of(&v)),
            (StatusCode::OK, Some("complexity")),
            "{v}"
        );
        let msg = v["errors"][0]["message"].as_str().unwrap_or("");
        assert!(msg.contains("fragment expansion"), "{msg}");
        assert!(
            cpu < Duration::from_millis(50),
            "n={n}: {cpu:?} CPU ({el:?})"
        );
    }
    // The directive cap now lives in the bounded walk: 8 pass the limits, 9 are refused.
    let on_field = |k: usize| format!("{{ lastBlockHeight {} }}", "@include(if: true) ".repeat(k));
    let (_, v, _) = query(&f, &on_field(9)).await;
    assert_eq!(limit_of(&v), Some("directives"), "{v}");
    let (_, v, _) = query(&f, &on_field(8)).await;
    assert_ne!(limit_of(&v), Some("directives"), "{v}");
    // On a spread and on an inline fragment, and inside a fragment.
    let spread = format!(
        "fragment A on Query {{ lastBlockHeight }} {{ ...A {} }}",
        "@include(if: true) ".repeat(9)
    );
    let (_, v, _) = query(&f, &spread).await;
    assert_eq!(limit_of(&v), Some("directives"), "{v}");
    let inline = format!(
        "{{ ... on Query {} {{ lastBlockHeight }} }}",
        "@include(if: true) ".repeat(9)
    );
    let (_, v, _) = query(&f, &inline).await;
    assert_eq!(limit_of(&v), Some("directives"), "{v}");
    let inside = format!(
        "fragment A on Query {{ lastBlockHeight {} }} {{ ...A }}",
        "@include(if: true) ".repeat(9)
    );
    let (_, v, _) = query(&f, &inside).await;
    assert_eq!(limit_of(&v), Some("directives"), "{v}");
}

/// Serves the GraphQL router and a `/healthz` stand-in (another listener) on their own
/// 4-worker runtime, so a pinned worker there cannot stop the test's own timers.
fn serve_apart(f: &Fixture) -> (std::net::SocketAddr, std::net::SocketAddr) {
    let st = f.st.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let app =
                router(build_schema(st.clone(), GraphqlSettings::default(), None)).with_state(st);
            let health =
                axum::Router::new().route("/healthz", axum::routing::get(|| async { "ok" }));
            let a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send((a.local_addr().unwrap(), b.local_addr().unwrap()))
                .unwrap();
            let a = axum::serve(
                a,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            );
            let b = axum::serve(b, health);
            let _ = tokio::join!(a, b);
        });
    });
    rx.recv().unwrap()
}

/// One HTTP/1.1 exchange on a fresh connection: the status line and the body.
async fn raw(addr: std::net::SocketAddr, method: &str, path: &str, body: &str) -> (String, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    c.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    c.read_to_end(&mut out).await.unwrap();
    let out = String::from_utf8_lossy(&out).to_string();
    let (head, body) = out.split_once("\r\n\r\n").unwrap_or((&out, ""));
    (
        head.lines().next().unwrap_or("").to_string(),
        body.to_string(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_fragment_bombs_leave_other_listeners_answering() {
    let f = fixture();
    let (gql, health) = serve_apart(&f);
    let body = json!({ "query": bomb(28) }).to_string();
    let t = Instant::now();
    let bombs: Vec<_> = (0..14)
        .map(|_| {
            let body = body.clone();
            tokio::spawn(async move { raw(gql, "POST", "/graphql", &body).await })
        })
        .collect();
    // While the bombs are in flight, the other listener keeps answering.
    for _ in 0..5 {
        let h = Instant::now();
        let r = tokio::time::timeout(Duration::from_secs(2), raw(health, "GET", "/healthz", ""))
            .await
            .expect("/healthz did not answer within 2 s");
        assert!(r.0.contains("200"), "{r:?}");
        assert!(
            h.elapsed() < Duration::from_millis(500),
            "{:?}",
            h.elapsed()
        );
    }
    for b in bombs {
        let (status, body) = tokio::time::timeout(Duration::from_secs(5), b)
            .await
            .expect("a bomb was not answered within 5 s")
            .unwrap();
        assert!(status.contains("200"), "{status}");
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(limit_of(&v), Some("complexity"), "{v}");
    }
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
}
