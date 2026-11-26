//! B1: `pactQuery` and `gasLimitEstimate` through a mock node's `/local`, checked against
//! answers captured from stock (`tests/data/pact/`), plus every cap, node failures and
//! hostile input.

use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{StatusCode, Uri};
use axum::response::IntoResponse;
use kdx_graphql::pact::{
    blake_b64, PactBudget, PactPass, PactSettings, GAS_NODE, GAS_PARSE, GAS_UNKNOWN, TIMEOUT_MSG,
};
use kdx_graphql::{build_schema_full, ClientIp, GraphqlSettings, KdxSchema};
use serde_json::{json, Value};

type Reply = Box<dyn Fn(&str, &Value) -> (u16, String, Duration) + Send + Sync>;

struct Mock {
    hits: AtomicUsize,
    live: AtomicUsize,
    peak: AtomicUsize,
    /// (path and query, body) of each request.
    seen: Mutex<Vec<(String, String)>>,
    reply: Reply,
}

async fn handle(State(m): State<Arc<Mock>>, uri: Uri, body: String) -> axum::response::Response {
    m.hits.fetch_add(1, Ordering::SeqCst);
    let live = m.live.fetch_add(1, Ordering::SeqCst) + 1;
    m.peak.fetch_max(live, Ordering::SeqCst);
    let pq = uri
        .path_and_query()
        .map(|p| p.to_string())
        .unwrap_or_default();
    m.seen.lock().unwrap().push((pq.clone(), body.clone()));
    let v: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let (st, b, delay) = (m.reply)(&pq, &v);
    tokio::time::sleep(delay).await;
    m.live.fetch_sub(1, Ordering::SeqCst);
    (
        StatusCode::from_u16(st).unwrap(),
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        b,
    )
        .into_response()
}

/// The `code` of a `/local` body's command.
fn code_of(body: &Value) -> String {
    let cmd: Value = serde_json::from_str(body["cmd"].as_str().unwrap_or("{}")).unwrap_or_default();
    cmd.pointer("/payload/exec/code")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

async fn mock(
    reply: impl Fn(&str, &Value) -> (u16, String, Duration) + Send + Sync + 'static,
) -> (url::Url, Arc<Mock>) {
    let m = Arc::new(Mock {
        hits: AtomicUsize::new(0),
        live: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        seen: Mutex::new(Vec::new()),
        reply: Box::new(reply),
    });
    let app = axum::Router::new().fallback(handle).with_state(m.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await });
    (format!("http://{addr}/").parse().unwrap(), m)
}

fn ok(data: &str) -> (u16, String, Duration) {
    (
        200,
        format!(r#"{{"gas":7,"result":{{"status":"success","data":{data}}},"reqKey":"x"}}"#),
        Duration::ZERO,
    )
}

fn schema(base: url::Url, s: PactSettings) -> KdxSchema {
    let node = kdx_node::NodeClient::new(&kdx_node::NodeClientConfig {
        base,
        network: "mainnet01".into(),
        timeout: Duration::from_secs(5),
        max_rps: 0,
        concurrency: s.max_in_flight,
        p2p_url: None,
        p2p_cert_sha256: None,
        max_body_bytes: s.max_response_bytes,
        max_sse_event_bytes: 1 << 16,
    })
    .unwrap();
    let pass = Arc::new(PactPass::new(node, s.clone()));
    let settings = GraphqlSettings {
        pact: s,
        ..GraphqlSettings::default()
    };
    build_schema_full(None, settings, None, None, Some(pass))
}

fn open() -> PactSettings {
    PactSettings {
        per_ip_rps: 0,
        ..PactSettings::default()
    }
}

async fn run(s: &KdxSchema, q: &str, vars: Value, ip: [u8; 4]) -> Value {
    let req = async_graphql::Request::new(q)
        .variables(async_graphql::Variables::from_json(vars))
        .data(ClientIp(IpAddr::from(ip)))
        .data(PactBudget::default());
    serde_json::to_value(s.execute(req).await).unwrap()
}

const PQ: &str =
    "query($q:[PactQuery!]!){ pactQuery(pactQuery:$q){ chainId code status result error } }";
const GAS: &str = "query($i:[String!]!){ gasLimitEstimate(input:$i){ amount inputType transaction usedPreflight usedSignatureVerification } }";

fn fixture(name: &str) -> Value {
    let p = format!("{}/tests/data/pact/{name}", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(p).unwrap()).unwrap()
}

fn err_msg(r: &Value) -> &str {
    r["errors"][0]["message"].as_str().unwrap_or_default()
}

/// Every captured `pactQuery` item, through a mock node answering what Pact 5 answers:
/// stock's exact `status`, `result` text and `error` text; the node gets stock's command.
#[tokio::test]
async fn pact_query_matches_stock_fixtures() {
    let f = fixture("pact_query.json");
    let cases = f["cases"].as_array().unwrap().clone();
    let by_code: Vec<(String, Option<String>)> = cases
        .iter()
        .map(|c| {
            (
                c["input"]["code"].as_str().unwrap().to_owned(),
                c["node"].as_str().map(str::to_owned),
            )
        })
        .collect();
    let (base, m) = mock(move |_, body| {
        let code = code_of(body);
        match by_code.iter().find(|(c, _)| *c == code) {
            Some((_, Some(n))) => (200, n.clone(), Duration::ZERO),
            _ => (404, "not found".into(), Duration::ZERO),
        }
    })
    .await;
    let s = schema(
        base,
        PactSettings {
            max_items: 30,
            ..open()
        },
    );
    let items: Vec<Value> = cases.iter().map(|c| c["input"].clone()).collect();
    let r = run(&s, PQ, json!({ "q": items }), [1, 1, 1, 1]).await;
    assert!(r.get("errors").is_none(), "{r}");
    let got = r["data"]["pactQuery"].as_array().unwrap();
    for (c, g) in cases.iter().zip(got) {
        assert_eq!(g, &c["stock"], "item {}", c["input"]);
    }
    // Two items name no chain the network has: no node call for them.
    assert_eq!(m.hits.load(Ordering::SeqCst), cases.len() - 2);
    for (pq, body) in m.seen.lock().unwrap().iter() {
        assert!(
            pq.starts_with("/chainweb/0.0/mainnet01/chain/")
                && pq.ends_with("/pact/api/v1/local?preflight=false&signatureVerification=false"),
            "{pq}"
        );
        let b: Value = serde_json::from_str(body).unwrap();
        let cmd_text = b["cmd"].as_str().unwrap();
        assert_eq!(b["hash"], blake_b64(cmd_text));
        assert_eq!(b["sigs"], json!([]));
        let cmd: Value = serde_json::from_str(cmd_text).unwrap();
        assert_eq!(cmd["networkId"], "mainnet01");
        assert_eq!(cmd["signers"], json!([]));
        assert_eq!(cmd["meta"]["gasLimit"], 2500);
        assert_eq!(cmd["meta"]["sender"], "");
        assert_eq!(cmd["meta"]["ttl"], 28800);
        if code_of(&b) == "(read-msg \"k\")" {
            assert_eq!(cmd["payload"]["exec"]["data"], json!({"k": "v", "n": "12"}));
        }
    }
    let r = run(&s, PQ, json!({ "q": [] }), [1, 1, 1, 1]).await;
    assert_eq!(r["data"]["pactQuery"], json!([]));
}

/// Each captured `gasLimitEstimate` input shape: the same error text, or the same answer
/// (a `code` transaction compared by structure: its time and nonce differ per call).
#[tokio::test]
async fn gas_limit_estimate_matches_stock_fixtures() {
    let (base, m) = mock(|pq, body| {
        let cmd: Value = serde_json::from_str(body["cmd"].as_str().unwrap_or("{}")).unwrap_or_default();
        let code = cmd.pointer("/payload/exec/code").and_then(Value::as_str).unwrap_or("");
        let pre = pq.contains("preflight=true");
        let signers = cmd["signers"].as_array().map_or(0, Vec::len);
        let sigs = body["sigs"].as_array().map_or(0, Vec::len);
        // As the node: estimates fail for `(+ 1 "a")`, for preflight without a signer for
        // the gas payer, and for a full transaction without signatures.
        let fails = code.contains("\"a\"")
            || (pre && signers == 0)
            || (pq.contains("signatureVerification=true") && sigs == 0);
        let gas = if sigs > 0 { 95 } else if pre { 94 } else { 1 };
        let r = if fails {
            json!({"gas": 0, "result": {"status": "failure", "error": {"type": "EvalError", "message": "x"}}})
        } else {
            json!({"gas": gas, "result": {"status": "success", "data": 3.5}})
        };
        let r = if pre { json!({"preflightResult": r, "preflightWarnings": []}) } else { r };
        (200, r.to_string(), Duration::ZERO)
    })
    .await;
    let s = schema(base, open());
    let f = fixture("gas_limit_estimate.json");
    for c in f["cases"].as_array().unwrap() {
        let name = c["name"].as_str().unwrap();
        let r = run(&s, GAS, json!({ "i": c["input"] }), [1, 1, 1, 1]).await;
        let stock = &c["stock"];
        if name == "g_stringified_biggas" {
            // Our gas cap (150,000) answers first; stock's node refused it too.
            assert_eq!(
                err_msg(&r),
                "[ERROR][GAS][LIMIT] gasLimit above the server cap of 150000"
            );
            assert_eq!(err_msg(stock), GAS_NODE);
            continue;
        }
        if stock.get("errors").is_some() {
            assert_eq!(err_msg(&r), err_msg(stock), "{name}: {r}");
            assert_eq!(
                r["errors"][0]["extensions"]["code"], "INTERNAL_SERVER_ERROR",
                "{name}"
            );
            assert_eq!(r["data"], Value::Null, "{name}");
            continue;
        }
        let (got, want) = (
            r["data"]["gasLimitEstimate"].as_array().unwrap(),
            stock["data"]["gasLimitEstimate"].as_array().unwrap(),
        );
        assert_eq!(got.len(), want.len(), "{name}");
        for (g, w) in got.iter().zip(want) {
            for k in [
                "amount",
                "inputType",
                "usedPreflight",
                "usedSignatureVerification",
            ] {
                assert_eq!(g[k], w[k], "{name}.{k}");
            }
            let (gt, wt) = (
                g["transaction"].as_str().unwrap(),
                w["transaction"].as_str().unwrap(),
            );
            if w["inputType"] == "code" {
                assert_eq!(shape(gt), shape(wt), "{name}");
                let tx: Value = serde_json::from_str(gt).unwrap();
                assert_eq!(tx["hash"], blake_b64(tx["cmd"].as_str().unwrap()));
            } else {
                assert_eq!(gt, wt, "{name}: byte-for-byte");
            }
        }
    }
    // Shapes stock can never build (string payload/meta) and unknown chains never reach the
    // node; nor do parse errors.
    let seen = m.seen.lock().unwrap();
    assert!(
        seen.iter()
            .all(|(pq, _)| pq.starts_with("/chainweb/0.0/mainnet01/chain/")),
        "{seen:?}"
    );
    // code (3 calls: g_code, g_code_net, g_code_fail) + g_two (2) + stringified/full-tx
    // fixtures under the cap (8).
    assert_eq!(seen.len(), 13, "{seen:?}");
    for (pq, body) in seen.iter() {
        let b: Value = serde_json::from_str(body).unwrap();
        let cmd: Value = serde_json::from_str(b["cmd"].as_str().unwrap()).unwrap();
        if cmd.get("networkId").is_none() {
            // `code` inputs: stock's flags and no networkId (a client `networkId` is dropped).
            assert!(
                pq.ends_with("preflight=false&signatureVerification=false"),
                "{pq}"
            );
        }
    }
}

/// A `code` transaction's text with its time, nonce and hash blanked.
fn shape(tx: &str) -> String {
    let mut t: Value = serde_json::from_str(tx).unwrap();
    let mut cmd: Value = serde_json::from_str(t["cmd"].as_str().unwrap()).unwrap();
    cmd["meta"]["creationTime"] = json!(0);
    cmd["nonce"] = json!("");
    t["cmd"] = json!(kdx_graphql::pact::js_json(&cmd));
    t["hash"] = json!("");
    kdx_graphql::pact::js_json(&t)
}

#[tokio::test]
async fn full_transaction_uses_preflight_and_signature_verification() {
    let (base, m) = mock(|_, _| {
        (
            200,
            r#"{"preflightResult":{"gas":120,"result":{"status":"success","data":1}},"preflightWarnings":[]}"#.into(),
            Duration::ZERO,
        )
    })
    .await;
    let s = schema(base, open());
    let cmd = r#"{"payload":{"exec":{"code":"1","data":{}}},"nonce":"n","signers":[],"meta":{"chainId":"3","sender":"s","gasLimit":1000,"gasPrice":1e-8,"ttl":600,"creationTime":1},"networkId":"mainnet01"}"#;
    let input = json!({"cmd": cmd, "hash": "abc", "sigs": ["s1"]}).to_string();
    let r = run(&s, GAS, json!({ "i": [input] }), [1, 1, 1, 1]).await;
    let g = &r["data"]["gasLimitEstimate"][0];
    assert_eq!(g["amount"], 120, "{r}");
    assert_eq!(g["inputType"], "full-transaction");
    assert_eq!(g["usedPreflight"], true);
    assert_eq!(g["usedSignatureVerification"], true);
    assert_eq!(
        g["transaction"],
        json!({"cmd": cmd, "hash": "abc", "sigs": [{"sig": "s1"}]}).to_string()
    );
    let seen = m.seen.lock().unwrap();
    assert_eq!(
        seen[0].0,
        "/chainweb/0.0/mainnet01/chain/3/pact/api/v1/local?preflight=true&signatureVerification=true"
    );
    assert_eq!(
        seen[0].1,
        g["transaction"].as_str().unwrap(),
        "the body is the transaction"
    );
}

/// The gas cap: a client command asking for more never reaches the node.
#[tokio::test]
async fn gas_limit_cap_refuses_without_a_node_call() {
    let (base, m) = mock(|_, _| ok("1")).await;
    let s = schema(base, open());
    let cmd = r#"{"payload":{"exec":{"code":"1","data":{}}},"nonce":"n","signers":[],"meta":{"chainId":"0","sender":"s","gasLimit":150001,"gasPrice":1e-8,"ttl":600,"creationTime":1},"networkId":"mainnet01"}"#;
    let r = run(
        &s,
        GAS,
        json!({ "i": [json!({"cmd": cmd}).to_string()] }),
        [1, 1, 1, 1],
    )
    .await;
    assert_eq!(
        err_msg(&r),
        "[ERROR][GAS][LIMIT] gasLimit above the server cap of 150000"
    );
    // No gas limit, or a smuggled chain (`../`), is the node's refusal, without a call.
    for meta in [
        r#"{"chainId":"0"}"#,
        r#"{"chainId":"0/../../1","gasLimit":1}"#,
        r#"{"chainId":"20","gasLimit":1}"#,
    ] {
        let cmd = format!(r#"{{"meta":{meta}}}"#);
        let r = run(
            &s,
            GAS,
            json!({ "i": [json!({"cmd": cmd}).to_string()] }),
            [1, 1, 1, 1],
        )
        .await;
        assert_eq!(err_msg(&r), GAS_NODE, "{meta}");
    }
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
}

/// Items per request: one field, and many aliases sharing the request's budget.
#[tokio::test]
async fn item_cap_counts_every_alias() {
    let (base, m) = mock(|_, _| ok("1")).await;
    let s = schema(base, open());
    let item = json!({"chainId": "0", "code": "1"});
    let r = run(&s, PQ, json!({ "q": vec![item.clone(); 21] }), [1, 1, 1, 1]).await;
    assert_eq!(
        err_msg(&r),
        "[ERROR][RAW_QUERY][LIMIT] At most 20 Pact queries and gas estimates per request"
    );
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
    // 50 aliases of 1 item each: 20 run, the rest are refused; the node sees at most 20.
    let aliases: String = (0..50)
        .map(|i| format!(" a{i}: pactQuery(pactQuery: $q) {{ result }}"))
        .collect();
    let q = format!("query($q:[PactQuery!]!){{{aliases} }}");
    let r = run(&s, &q, json!({ "q": [item.clone()] }), [1, 1, 1, 1]).await;
    let errs = r["errors"].as_array().unwrap();
    assert_eq!(errs.len(), 30, "{r}");
    assert!(
        errs.iter().all(|e| e["message"]
            .as_str()
            .unwrap()
            .contains("[LIMIT] At most 20")),
        "{r}"
    );
    assert!(
        m.hits.load(Ordering::SeqCst) <= 20,
        "{}",
        m.hits.load(Ordering::SeqCst)
    );
    // Gas inputs count against the same budget.
    let gin = json!({"code": "1", "chainId": "0"}).to_string();
    let q = "query($q:[PactQuery!]!, $i:[String!]!){ pactQuery(pactQuery:$q){ result } gasLimitEstimate(input:$i){ amount } }";
    let r = run(
        &s,
        q,
        json!({ "q": vec![item; 15], "i": vec![gin; 6] }),
        [1, 1, 1, 1],
    )
    .await;
    assert_eq!(
        err_msg(&r),
        "[ERROR][GAS][LIMIT] At most 20 Pact queries and gas estimates per request"
    );
}

#[tokio::test]
async fn per_ip_rate_limit() {
    let (base, _m) = mock(|_, _| ok("1")).await;
    let s = schema(
        base,
        PactSettings {
            max_items: 2,
            per_ip_rps: 1,
            per_ip_burst: 2,
            ..PactSettings::default()
        },
    );
    let q = json!({ "q": [{"chainId": "0", "code": "1"}] });
    for _ in 0..2 {
        let r = run(&s, PQ, q.clone(), [9, 9, 9, 9]).await;
        assert_eq!(r["data"]["pactQuery"][0]["status"], "success", "{r}");
    }
    let r = run(&s, PQ, q.clone(), [9, 9, 9, 9]).await;
    assert_eq!(
        err_msg(&r),
        "[ERROR][RAW_QUERY][RATE_LIMIT] Too many Pact queries from this client, retry later"
    );
    let r = run(&s, PQ, q, [8, 8, 8, 8]).await;
    assert_eq!(
        r["data"]["pactQuery"][0]["status"], "success",
        "another client: {r}"
    );
}

/// At most `max_in_flight` node calls at once, and at most twice that many requests inside
/// the passthrough (the rest are refused at once, holding nothing).
#[tokio::test]
async fn in_flight_and_busy_caps() {
    let (base, m) = mock(|_, _| (200, ok("1").1, Duration::from_millis(400))).await;
    let s = Arc::new(schema(
        base,
        PactSettings {
            max_in_flight: 2,
            ..open()
        },
    ));
    let r = run(
        &s,
        PQ,
        json!({ "q": vec![json!({"chainId": "0", "code": "1"}); 8] }),
        [1, 1, 1, 1],
    )
    .await;
    assert!(r["data"]["pactQuery"]
        .as_array()
        .unwrap()
        .iter()
        .all(|x| x["status"] == "success"));
    assert_eq!(m.peak.load(Ordering::SeqCst), 2);
    // 4 requests fill the passthrough (2 x 2); the fifth is refused while they run.
    let mut held = Vec::new();
    for _ in 0..4 {
        let s = s.clone();
        held.push(tokio::spawn(async move {
            run(
                &s,
                PQ,
                json!({ "q": [{"chainId": "0", "code": "1"}] }),
                [1, 1, 1, 1],
            )
            .await
        }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let t = Instant::now();
    let r = run(
        &s,
        PQ,
        json!({ "q": [{"chainId": "0", "code": "1"}] }),
        [1, 1, 1, 1],
    )
    .await;
    assert_eq!(
        err_msg(&r),
        "[ERROR][RAW_QUERY][BUSY] The Pact query service is busy, retry later"
    );
    assert!(t.elapsed() < Duration::from_millis(100));
    for h in held {
        assert_eq!(
            h.await.unwrap()["data"]["pactQuery"][0]["status"],
            "success"
        );
    }
}

/// A node that does not answer: stock's `timeout` item; a gas estimate fails as stock's.
#[tokio::test]
async fn node_timeout() {
    let (base, _m) = mock(|_, _| (200, ok("1").1, Duration::from_secs(5))).await;
    let s = schema(
        base,
        PactSettings {
            timeout: Duration::from_millis(300),
            ..open()
        },
    );
    let t = Instant::now();
    let r = run(
        &s,
        PQ,
        json!({ "q": [{"chainId": "0", "code": "1"}] }),
        [1, 1, 1, 1],
    )
    .await;
    assert_eq!(
        r["data"]["pactQuery"][0],
        json!({"chainId": "0", "code": "1", "status": "timeout", "result": null, "error": TIMEOUT_MSG})
    );
    let r = run(
        &s,
        GAS,
        json!({ "i": [json!({"code": "1", "chainId": "0"}).to_string()] }),
        [1, 1, 1, 1],
    )
    .await;
    assert_eq!(err_msg(&r), GAS_NODE);
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
}

/// Node failures: HTTP errors, non-JSON, a Pact error without `type`, an answer over the
/// body cap.
#[tokio::test]
async fn node_error_mapping() {
    let (base, _m) = mock(|_, body| match code_of(body).as_str() {
        "500" => (500, "boom".into(), Duration::ZERO),
        "400" => (400, "Validation failed".into(), Duration::ZERO),
        "text" => (200, "not json".into(), Duration::ZERO),
        "untyped" => (
            200,
            r#"{"result":{"status":"failure","error":{"message":"m"}}}"#.into(),
            Duration::ZERO,
        ),
        "nostatus" => (200, r#"{"gas":1}"#.into(), Duration::ZERO),
        "big" => (
            200,
            format!(
                r#"{{"result":{{"status":"success","data":"{}"}}}}"#,
                "x".repeat(4096)
            ),
            Duration::ZERO,
        ),
        _ => ok("1"),
    })
    .await;
    let s = schema(
        base,
        PactSettings {
            max_response_bytes: 2048,
            ..open()
        },
    );
    let items: Vec<Value> = ["500", "400", "text", "untyped", "nostatus", "big"]
        .iter()
        .map(|c| json!({"chainId": "0", "code": c}))
        .collect();
    let r = run(&s, PQ, json!({ "q": items }), [1, 1, 1, 1]).await;
    let errs: Vec<&str> = r["data"]["pactQuery"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["error"].as_str().unwrap())
        .collect();
    assert_eq!(
        errs,
        [
            "{}",
            "{}",
            "{}",
            "{}",
            "{}",
            "[ERROR][RAW_QUERY][LIMIT] The node's answer exceeds 2048 bytes"
        ]
    );
}

/// Hostile and malformed input: chains that are not canonical, a huge code string, bad gas
/// input. None reaches the node.
#[tokio::test]
async fn malformed_and_huge_input() {
    let (base, m) = mock(|_, _| ok("1")).await;
    let s = schema(
        base,
        PactSettings {
            max_input_bytes: 1024,
            ..open()
        },
    );
    // Past the request's variables cap, nothing runs at all.
    let r = run(
        &s,
        PQ,
        json!({ "q": [{"chainId": "0", "code": "(".repeat(70_000)}] }),
        [1, 1, 1, 1],
    )
    .await;
    assert_eq!(
        r["errors"][0]["extensions"]["limit"], "variables_size",
        "{r}"
    );
    let huge = "(".repeat(2_000);
    let items = json!([
        {"chainId": "00", "code": "1"},
        {"chainId": " 1", "code": "1"},
        {"chainId": "-1", "code": "1"},
        {"chainId": "256", "code": "1"},
        {"chainId": "0/../../x", "code": "1"},
        {"chainId": "0", "code": huge},
        {"chainId": "0", "code": "1", "data": [{"key": "k", "value": "v".repeat(2_000)}]},
    ]);
    let r = run(&s, PQ, json!({ "q": items }), [1, 1, 1, 1]).await;
    let got = r["data"]["pactQuery"]
        .as_array()
        .unwrap_or_else(|| panic!("{r}"));
    for g in &got[..5] {
        assert_eq!(
            (g["status"].as_str(), g["error"].as_str()),
            (Some("error"), Some("{}"))
        );
    }
    for g in &got[5..] {
        assert_eq!(
            g["error"],
            "[ERROR][RAW_QUERY][LIMIT] Pact code and data exceed 1024 bytes"
        );
    }
    for (input, msg) in [
        ("nope".to_owned(), GAS_PARSE),
        ("[1]".to_owned(), GAS_PARSE),
        (r#"{"code":1,"chainId":"0"}"#.to_owned(), GAS_PARSE),
        (r#"{"sigs":"x","cmd":"y"}"#.to_owned(), GAS_PARSE),
        (r#"{"chainId":"0"}"#.to_owned(), GAS_UNKNOWN),
        (r#"{"cmd":"not json"}"#.to_owned(), GAS_NODE),
        (
            json!({"code": huge, "chainId": "0"}).to_string(),
            "[ERROR][GAS][LIMIT] Input exceeds 1024 bytes",
        ),
    ] {
        let r = run(&s, GAS, json!({ "i": [input] }), [1, 1, 1, 1]).await;
        assert_eq!(err_msg(&r), msg);
    }
    // The first input that does not parse wins over an earlier node-side failure, as
    // stock's synchronous parse throws before its calls settle.
    let r = run(
        &s,
        GAS,
        json!({ "i": [r#"{"payload":"x","chainId":"0"}"#, "nope"] }),
        [1, 1, 1, 1],
    )
    .await;
    assert_eq!(err_msg(&r), GAS_PARSE);
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
}
