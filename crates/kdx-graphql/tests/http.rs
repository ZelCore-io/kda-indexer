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
        v1txs_deadline: std::time::Duration::from_secs(30),
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
    // Stock's limits (FIX-3 part C): `parent` nested 7 times is over graphile's limits (a
    // coordinate nests at most twice), 41 aliased pages of 100 cost 4,100 by stock's formula.
    let aliases: String = (0..41)
        .map(|i| format!("t{i}: transactions(blockHash: \"x\", first: 100) {{ edges {{ node {{ hash }} }} }} "))
        .collect();
    let big = format!("{{ lastBlockHeight }} #{}", "x".repeat(33 * 1024));
    for (q, want, status) in [
        (deep, "depth", StatusCode::BAD_REQUEST),
        (
            format!("{{ {aliases} }}"),
            "complexity",
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (big, "query_size", StatusCode::OK),
    ] {
        let (s, v, el) = query(&f, &q).await;
        let got = v["errors"][0]["extensions"]["limit"].as_str();
        assert_eq!((s, got), (status, Some(want)), "{v}");
        assert!(el < Duration::from_millis(100), "{want}: {el:?}");
    }
    // A page of 100 with 20 fields costs 100 by stock's formula (leaves are free): served.
    let wide = format!(
        "{{ blocksFromHeight(startHeight: 0, first: 100) {{ edges {{ node {{ {} }} }} }} }}",
        fields(20)
    );
    let (s, v, _) = query(&f, &wide).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert!(v.get("errors").is_none(), "{v}");
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
    // The full document is over stock's introspection depth (see `stock_limits`); without
    // the arguments of fields it answers in full.
    let shallow = INTROSPECTION.replace("    args { ...InputValue }\n", "");
    let (s, v, _) = query(&f, &shallow).await;
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

/// FIX-3 part C: the stock indexer's refusals, with stock's text, code and HTTP status.
mod stock_limits {
    use super::*;

    const COMPLEXITY: &str = "[ERROR][GRAPHQL][VALID_RANGE] Sorry, too complicated query! Exceeded the maximum allowed complexity.";

    fn page(n: u32) -> String {
        format!("{{ transactions(first: {n}) {{ edges {{ node {{ hash }} }} }} }}")
    }

    #[tokio::test]
    async fn complexity_over_4000_is_refused_as_stock_refuses_it() {
        let f = fixture();
        // 4000 by stock's formula passes, 4001 is refused: HTTP 500, stock's message, and
        // stock's code at the top level (ours keeps `extensions`, with the limit named).
        // 4000 as four pages of 1,000 (one page of 4,000 is over the part D safety cap).
        let four: String = (0..4)
            .map(|i| format!("t{i}: transactions(first: 1000) {{ edges {{ node {{ hash }} }} }} "))
            .collect();
        let (s, v, _) = query(&f, &format!("{{ {four} }}")).await;
        assert_eq!(s, StatusCode::OK, "{v}");
        assert!(v.get("errors").is_none(), "{v}");
        let (s, v, _) = query(&f, &page(4001)).await;
        assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR, "{v}");
        let e = &v["errors"][0];
        assert_eq!(e["message"], COMPLEXITY, "{v}");
        assert_eq!(e["code"], "INTERNAL_SERVER_ERROR", "{v}");
        assert_eq!(e["extensions"]["limit"], "complexity", "{v}");
        assert!(
            e.get("locations").is_none() && e.get("path").is_none(),
            "{v}"
        );
        // FIX-4 part B (B2): a request refused before it ran has no `data` key, as stock's.
        assert!(v.get("data").is_none(), "{v}");
        // The explorer's transactions-by-account document at first: 100 costs 300.
        let doc = include_str!("../../../compare/graphql/QTransactionsByAccount.graphql");
        let (s, v, _) = post(
            &f,
            json!({"query": doc, "variables": {"accountName": "k:a", "first": 100}}).to_string(),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{v}");
        assert!(v.get("errors").is_none(), "{v}");
        // FIX-3 part D (S1): stock skips the check for any operation named
        // IntrospectionQuery; ours only for one of introspection fields alone, so a spoofed
        // name carrying real fields is priced (a documented safety difference).
        let named = page(5000).replacen('{', "query IntrospectionQuery {", 1);
        let (s, v, _) = post(
            &f,
            json!({"query": named, "operationName": "IntrospectionQuery"}).to_string(),
        )
        .await;
        assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR, "{v}");
        assert_eq!(v["errors"][0]["message"], COMPLEXITY, "{v}");
        let named = "query IntrospectionQuery { __schema { queryType { name } } }";
        let (s, v, _) = post(
            &f,
            json!({"query": named, "operationName": "IntrospectionQuery"}).to_string(),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{v}");
        assert!(v.get("errors").is_none(), "{v}");
        // The operation that runs is the one priced: a cheap selected operation next to an
        // expensive one is served, the expensive one selected is refused.
        let two = format!(
            "query Cheap {{ lastBlockHeight }} query Dear {}",
            page(4001)
        );
        for (op, want) in [
            ("Cheap", StatusCode::OK),
            ("Dear", StatusCode::INTERNAL_SERVER_ERROR),
        ] {
            let (s, v, _) = post(&f, json!({"query": two, "operationName": op}).to_string()).await;
            assert_eq!(s, want, "{op}: {v}");
        }
        // A negative page next to an expensive one cannot lower the cost (S2).
        let neg = "{ a: transactions(first: -4000) { edges { node { hash } } } b: transactions(first: 4001) { edges { node { hash } } } }";
        let (s, v, _) = query(&f, neg).await;
        assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR, "{v}");
        assert_eq!(v["errors"][0]["message"], COMPLEXITY, "{v}");
        // A validation error comes before the complexity check, as in stock.
        let bad = page(5000).replace("hash", "nope");
        let (_, v, _) = query(&f, &bad).await;
        assert_ne!(v["errors"][0]["message"], COMPLEXITY, "{v}");
    }

    /// FIX-3 part D (S3): a page over the 1,000-row safety cap that stock's gate passes is
    /// refused by ours with a `LIMIT` named `page` (stock answers it slowly or times out).
    #[tokio::test]
    async fn pages_over_the_safety_cap_are_refused() {
        let f = fixture();
        let (s, v, _) = query(&f, &page(1000)).await;
        assert_eq!(s, StatusCode::OK, "{v}");
        assert!(v.get("errors").is_none(), "{v}");
        let (s, v, _) = query(&f, &page(1001)).await;
        assert_eq!(s, StatusCode::OK, "{v}");
        let e = &v["errors"][0];
        assert_eq!(e["extensions"]["code"], "LIMIT", "{v}");
        assert_eq!(e["extensions"]["limit"], "page", "{v}");
        assert_eq!(
            e["message"],
            "a page holds at most 1000 rows (first, last and quantity above it are refused)",
            "{v}"
        );
    }

    #[tokio::test]
    async fn depth_over_9_is_refused_as_stock_refuses_it() {
        let f = fixture();
        let nine = "query D { transaction(requestKey: \"x\") { result { ... on TransactionResult { block { parent { minerAccount { transactions { edges { node { cmd { nonce } } } } } } } } } } }";
        let (s, v, _) = query(&f, nine).await;
        assert_eq!(s, StatusCode::OK, "{v}");
        assert!(
            v["errors"][0]["extensions"]["code"] != "GRAPHQL_VALIDATION_FAILED",
            "{v}"
        );
        let ten = nine.replace("cmd { nonce }", "cmd { meta { chainId } }");
        let (s, v, _) = query(&f, &ten).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
        let e = &v["errors"][0];
        assert_eq!(e["message"], "'D' exceeds operation depth limits.", "{v}");
        assert_eq!(e["code"], "GRAPHQL_VALIDATION_FAILED", "{v}");
        assert_eq!(e["extensions"]["limit"], "depth", "{v}");
        // The full introspection document is over graphile's introspection depth (12) in
        // stock too; a shallower one answers.
        let (s, v, _) = query(&f, INTROSPECTION).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
        assert_eq!(
            v["errors"][0]["message"], "'IntrospectionQuery' exceeds operation depth limits.",
            "{v}"
        );
    }
}

/// FIX-3 part E (B1): the read budget's safety ceiling. Stock's gate prices `first: 0` at 0
/// while 20 rows are served; ours prices it as served and refuses a request (a batch: the
/// sum of its operations) above `BUDGET_CEILING` with `LIMIT` named `budget`.
mod budget_ceiling {
    use super::*;
    use kdx_graphql::limits::BUDGET_CEILING;

    const T: &str = "fragment T on QueryTransactionsConnection { edges { node { result { ... on TransactionResult { events(first: 0) { edges { node { name parameters } } } } } } } }";

    /// `n` aliases of nested zero pages (20 txs x 20 events each as served).
    fn zero_pages(n: usize) -> String {
        let a: String = (0..n)
            .map(|i| format!("a{i}: transactions(first: 0) {{ ...T }} "))
            .collect();
        format!("{{ {a} }} {T}")
    }

    fn batch(ops: usize, aliases: usize) -> String {
        let q = zero_pages(aliases);
        Value::Array((0..ops).map(|_| json!({ "query": q })).collect()).to_string()
    }

    /// One alias's budget (the unit the batches below are sized in).
    fn per_alias() -> u64 {
        let doc = async_graphql::parser::parse_query(zero_pages(1)).unwrap();
        let c = kdx_graphql::limits::budget(
            &doc,
            &async_graphql::Variables::default(),
            &GraphqlSettings::default(),
        )
        .unwrap()
        .complexity;
        assert!(c >= 20 * 20, "{c}");
        c
    }

    #[tokio::test]
    async fn a_batch_is_priced_as_the_sum_of_its_operations() {
        let f = fixture();
        let per_op = (BUDGET_CEILING / per_alias() / 10) as usize;
        // Ten operations, each well under the ceiling alone, together over it: refused
        // whole, before any of them runs.
        let (s, v, el) = post(&f, batch(10, per_op + 1)).await;
        assert_eq!((s, limit_of(&v)), (StatusCode::OK, Some("budget")), "{v}");
        assert!(el < Duration::from_secs(2), "{el:?}");
        // The same ten a little smaller: answered (stock's gate prices each at 0).
        let (s, v, _) = post(&f, batch(10, per_op - 1)).await;
        assert_eq!(s, StatusCode::OK);
        let rs = v.as_array().expect("a batch answer");
        assert_eq!(rs.len(), 10);
        assert!(rs.iter().all(|r| r.get("errors").is_none()), "{}", rs[0]);
    }

    /// NFT holdings are priced per row (up to 10,000 each); stock prices them at 0.
    #[tokio::test]
    async fn aliased_holdings_over_the_ceiling_are_refused() {
        let f = fixture();
        let n = (BUDGET_CEILING / kdx_graphql::MAX_NFT_HOLDINGS as u64) as usize + 1;
        let a: String = (0..n)
            .map(|i| {
                format!("a{i}: nonFungibleAccount(accountName: \"k:x\") {{ nonFungibleTokenBalances {{ tokenId }} }} ")
            })
            .collect();
        let (s, v, _) = query(&f, &format!("{{ {a} }}")).await;
        assert_eq!((s, limit_of(&v)), (StatusCode::OK, Some("budget")), "{v}");
        // The holdings are read when the account resolves, so the account itself is priced
        // per row, whatever is selected of it (security pass on part E).
        let a: String = (0..n)
            .map(|i| format!("a{i}: nonFungibleAccount(accountName: \"k:x\") {{ accountName }} "))
            .collect();
        let (s, v, _) = query(&f, &format!("{{ {a} }}")).await;
        assert_eq!((s, limit_of(&v)), (StatusCode::OK, Some("budget")), "{v}");
        let a: String = (0..n)
            .map(|i| {
                format!("a{i}: nonFungibleChainAccount(accountName: \"k:x\", chainId: \"0\") {{ chainId }} ")
            })
            .collect();
        let (s, v, _) = query(&f, &format!("{{ {a} }}")).await;
        assert_eq!((s, limit_of(&v)), (StatusCode::OK, Some("budget")), "{v}");
        // `node`/`nodes` with NFT ids: each id one holdings read.
        use kdx_graphql::ids::{encode, NodeKey};
        let nft = |i: usize| {
            encode(&NodeKey::NonFungibleAccount {
                account: format!("k:{i}"),
            })
        };
        let ids = |r: std::ops::Range<usize>| {
            r.map(|i| format!("\"{}\"", nft(i)))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let half = n / 2 + 1;
        let q = format!(
            "{{ a: nodes(ids: [{}]) {{ id }} b: nodes(ids: [{}]) {{ id }} }}",
            ids(0..half),
            ids(half..2 * half)
        );
        let (s, v, _) = query(&f, &q).await;
        assert_eq!((s, limit_of(&v)), (StatusCode::OK, Some("budget")), "{v}");
        let a: String = (0..n)
            .map(|i| format!("a{i}: node(id: \"{}\") {{ id }} ", nft(i)))
            .collect();
        let (s, v, _) = query(&f, &format!("{{ {a} }}")).await;
        assert_eq!((s, limit_of(&v)), (StatusCode::OK, Some("budget")), "{v}");
        // Ids of other types stay cheap.
        let blocks: Vec<String> = (0..100)
            .map(|i| {
                format!(
                    "\"{}\"",
                    encode(&NodeKey::Block {
                        hash: format!("h{i}")
                    })
                )
            })
            .collect();
        let q = format!("{{ nodes(ids: [{}]) {{ id }} }}", blocks.join(", "));
        let (_, v, _) = query(&f, &q).await;
        assert_eq!(limit_of(&v), None, "{v}");
        // The explorer's own document (one account) is served.
        let one = "query NonFungibleAccount($accountName: String!) { nonFungibleAccount(accountName: $accountName) { nonFungibleTokenBalances { balance chainId tokenId info { precision supply uri } guard { ... on KeysetGuard { keys predicate } } } } }";
        let (s, v, _) = post(
            &f,
            json!({"query": one, "variables": {"accountName": "k:x"}}).to_string(),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{v}");
        assert!(v.get("errors").is_none(), "{v}");
    }
}

/// FIX-4 item 3: an error async-graphql raises for the request itself (a variable that is
/// missing or of the wrong type) carries a code like every other error, and the status is
/// stock's: HTTP 500 `INTERNAL_SERVER_ERROR`. Resolver errors keep their code and 200; a
/// batch keeps 200.
#[tokio::test]
async fn request_level_errors_carry_a_code_and_stock_s_status() {
    let f = fixture();
    let node = "query($id: ID!) { node(id: $id) { id } }";
    for vars in [json!({}), json!({"id": true}), json!({"id": {"a": 1}})] {
        let (s, v, _) = post(&f, json!({"query": node, "variables": vars}).to_string()).await;
        assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR, "{vars}: {v}");
        assert_eq!(v["data"], Value::Null, "{vars}: {v}");
        let e = &v["errors"][0];
        assert_eq!(
            e["extensions"]["code"], "INTERNAL_SERVER_ERROR",
            "{vars}: {v}"
        );
        assert_eq!(e["code"], "INTERNAL_SERVER_ERROR", "{vars}: {v}");
    }
    let (s, v, _) = post(
        &f,
        json!({"query": "query($ids: [ID!]!) { nodes(ids: $ids) { id } }", "variables": {"ids": {}}})
            .to_string(),
    )
    .await;
    assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR, "{v}");
    assert_eq!(v["errors"][0]["code"], "INTERNAL_SERVER_ERROR", "{v}");
    // A resolver's own refusal: 200 with its code.
    let (s, v, _) = query(&f, "{ block(hash: \"x\") { height } }").await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(
        v["errors"][0]["extensions"]["code"], "BAD_USER_INPUT",
        "{v}"
    );
    // A batch keeps 200; its request-level error still gets the code.
    let (s, v, _) = post(
        &f,
        json!([{"query": node, "variables": {}}, {"query": "{ graphConfiguration { version } }"}])
            .to_string(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v[0]["errors"][0]["code"], "INTERNAL_SERVER_ERROR", "{v}");
    assert!(v[1].get("errors").is_none(), "{v}");
}

/// FIX-4 part B (B2): a request refused before anything ran answers without a `data` key, as
/// stock (Apollo) does and the GraphQL spec says. A resolver's error keeps `data`.
#[tokio::test]
async fn request_level_refusals_carry_no_data_key() {
    let f = fixture();
    // Stock's complexity refusal: five pages of 1,000.
    let five: String = (0..5)
        .map(|i| format!("t{i}: transactions(first: 1000) {{ edges {{ node {{ hash }} }} }} "))
        .collect();
    let (s, v, _) = query(&f, &format!("{{ {five} }}")).await;
    assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR, "{v}");
    assert!(v.get("data").is_none(), "{v}");
    // A missing variable (async-graphql reports it with a path, but nothing ran).
    let node = "query($id: ID!) { node(id: $id) { id } }";
    let (s, v, _) = post(&f, json!({"query": node, "variables": {}}).to_string()).await;
    assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR, "{v}");
    assert!(v.get("data").is_none(), "{v}");
    // Resolver refusals keep `data`: `first` with `last`, an unreadable hash.
    for q in [
        "{ transactions(first: 1, last: 1) { totalCount } }",
        "{ block(hash: \"x\") { height } }",
    ] {
        let (s, v, _) = query(&f, q).await;
        assert_eq!(s, StatusCode::OK, "{q}: {v}");
        assert!(v.get("data").is_some(), "{q}: {v}");
        assert_eq!(v["errors"][0]["code"], "BAD_USER_INPUT", "{q}: {v}");
    }
    // A batch: only the refused item loses its `data`.
    let (s, v, _) = post(
        &f,
        json!([{"query": node, "variables": {}}, {"query": "{ lastBlockHeight }"}]).to_string(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert!(v[0].get("data").is_none(), "{v}");
    assert!(v[1].get("data").is_some(), "{v}");
}

/// FIX-4 part B item 5: stock runs Apollo Server 4.11.0, which answers a document that does
/// not parse with HTTP 400 `GRAPHQL_PARSE_FAILED` and one that fails validation with HTTP 400
/// `GRAPHQL_VALIDATION_FAILED`, both without `data`. A variable whose value does not coerce
/// stays as FIX-4 item 3 decided (stock observed 500 `INTERNAL_SERVER_ERROR`).
#[tokio::test]
async fn parse_and_validation_errors_answer_as_apollo_server_4() {
    let f = fixture();
    let check = |v: &Value, code: &str| {
        let e = &v["errors"][0];
        assert_eq!(e["code"], code, "{v}");
        assert_eq!(e["extensions"]["code"], code, "{v}");
        assert!(v.get("data").is_none(), "{v}");
    };
    let (s, v, _) = query(&f, "{ lastBlockHeight ").await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    check(&v, "GRAPHQL_PARSE_FAILED");
    for q in [
        // An unknown field.
        "{ nope }",
        // A literal of the wrong type.
        "{ block(hash: 1) { height } }",
        // A declared variable that is not used.
        "query($x: Int) { lastBlockHeight }",
    ] {
        let (s, v, _) = query(&f, q).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{q}: {v}");
        check(&v, "GRAPHQL_VALIDATION_FAILED");
    }
    // Variable values that do not coerce: 500, as item 3.
    let node = "query($id: ID!) { node(id: $id) { id } }";
    for vars in [json!({}), json!({"id": true}), json!({"id": [1]})] {
        let (s, v, _) = post(&f, json!({"query": node, "variables": vars}).to_string()).await;
        assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR, "{vars}: {v}");
        check(&v, "INTERNAL_SERVER_ERROR");
    }
    let page = "query($f: Int) { transactions(first: $f) { totalCount } }";
    let (s, v, _) = post(
        &f,
        json!({"query": page, "variables": {"f": "x"}}).to_string(),
    )
    .await;
    assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR, "{v}");
    check(&v, "INTERNAL_SERVER_ERROR");
    // Apollo validates before it coerces variables: a bad document with a bad variable value
    // answers the validation error alone.
    let both = "query($id: ID!) { node(id: $id) { id nope } }";
    let (s, v, _) = post(
        &f,
        json!({"query": both, "variables": {"id": true}}).to_string(),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    check(&v, "GRAPHQL_VALIDATION_FAILED");
    assert_eq!(v["errors"].as_array().map(Vec::len), Some(1), "{v}");
    // FIX-4 part C (M-1): the same for a document over the depth limit: the depth refusal
    // alone, not mixed with the coercion error of a variable.
    let deep = format!(
        "query($id: ID!) {{ node(id: $id) {{ id }} blocksFromHeight(startHeight: 0, first: 1) {{ edges {{ node {{ {} hash {} }} }} }} }}",
        "parent { ".repeat(7),
        "}".repeat(7)
    );
    let (s, v, _) = post(
        &f,
        json!({"query": deep, "variables": {"id": true}}).to_string(),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    check(&v, "GRAPHQL_VALIDATION_FAILED");
    assert_eq!(v["errors"][0]["extensions"]["limit"], "depth", "{v}");
    assert_eq!(v["errors"].as_array().map(Vec::len), Some(1), "{v}");
    // FIX-4 part C (M-2): a coercion error is told from a document error by async-graphql's
    // message prefix (`limits.rs`, `apollo_validation`). Pinned here against the locked
    // async-graphql: an upgrade that rewords it fails this test instead of silently turning
    // every coercion into a 400.
    let (_, v, _) = post(
        &f,
        json!({"query": node, "variables": {"id": true}}).to_string(),
    )
    .await;
    let m = v["errors"][0]["message"].as_str().unwrap_or_default();
    assert!(m.starts_with("Invalid value for argument"), "{v}");
    // A batch keeps 200; its items carry the codes.
    let (s, v, _) = post(
        &f,
        json!([{"query": "{ nope }"}, {"query": "{ lastBlockHeight "}]).to_string(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(
        v[0]["errors"][0]["code"], "GRAPHQL_VALIDATION_FAILED",
        "{v}"
    );
    assert_eq!(v[1]["errors"][0]["code"], "GRAPHQL_PARSE_FAILED", "{v}");
}

/// FIX-4 part B item 4: `transaction(minimumDepth:)` outside stock's 1..=100 is refused with
/// stock's resolver error: its message, `INTERNAL_SERVER_ERROR`, `data.transaction` null and
/// HTTP 200 (stock indexer-kadena `transaction-query-resolver.ts`).
#[tokio::test]
async fn transaction_minimum_depth_outside_1_to_100_is_refused_as_stock() {
    let f = fixture();
    let rk = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let low = "[ERROR][GRAPHQL][VALID_RANGE] Minimum depth must not be lower than 1.";
    let high = "[ERROR][GRAPHQL][VALID_RANGE] Minimum depth must not be higher than 100.";
    for (d, want) in [
        (0, Some(low)),
        (-1, Some(low)),
        (101, Some(high)),
        (1, None),
        (100, None),
    ] {
        let q = format!("{{ transaction(requestKey: \"{rk}\", minimumDepth: {d}) {{ hash }} }}");
        let (s, v, _) = query(&f, &q).await;
        assert_eq!(s, StatusCode::OK, "{d}: {v}");
        assert_eq!(v["data"], json!({"transaction": null}), "{d}: {v}");
        match want {
            Some(m) => {
                let e = &v["errors"][0];
                assert_eq!(e["message"], m, "{d}: {v}");
                assert_eq!(e["code"], "INTERNAL_SERVER_ERROR", "{d}: {v}");
                assert_eq!(e["extensions"]["code"], "INTERNAL_SERVER_ERROR", "{d}: {v}");
            }
            None => assert!(v.get("errors").is_none(), "{d}: {v}"),
        }
    }
}
