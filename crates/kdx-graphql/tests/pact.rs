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

// ---- Fix round 1 (PR #52 review): HTTP-level deadline, guard, budget, limiter, M1/M4/M7.

fn schema_with(
    base: url::Url,
    s: PactSettings,
    st: Option<Arc<kdx_api::ApiState>>,
    rate_cap: Option<usize>,
) -> (KdxSchema, Arc<PactPass>) {
    let node = kdx_node::NodeClient::new(&kdx_node::NodeClientConfig {
        base,
        network: "mainnet01".into(),
        timeout: Duration::from_secs(30),
        max_rps: 0,
        concurrency: s.max_in_flight,
        p2p_url: None,
        p2p_cert_sha256: None,
        max_body_bytes: s.max_response_bytes,
        max_sse_event_bytes: 1 << 16,
    })
    .unwrap();
    let pass = Arc::new(PactPass::with_rate_cap(node, s.clone(), rate_cap));
    let settings = GraphqlSettings {
        pact: s,
        ..GraphqlSettings::default()
    };
    (
        build_schema_full(st, settings, None, None, Some(pass.clone())),
        pass,
    )
}

struct Http {
    _dir: tempfile::TempDir,
    st: Arc<kdx_api::ApiState>,
}

fn http_state(deadline: Duration) -> Http {
    let d = tempfile::tempdir().unwrap();
    let g = kdx_hot::Geometry {
        max_gb: 1,
        growth_mb: 16,
    };
    let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap());
    let store = Arc::new(kdx_api::Store::open(hot, d.path()).unwrap());
    let settings = kdx_api::ApiSettings {
        network: "mainnet01".into(),
        deadline,
        max_in_flight: 16,
        max_body_bytes: 8 << 20,
        v1txs_max_body_bytes: 8 << 20,
        v1txs_deadline: Duration::from_secs(30),
        rate_per_ip: 1000,
        rate_burst: 1000,
        trusted_proxies: Vec::new(),
        read_threads: 2,
        ready_max_lag: 10,
        max_event_lists: 64,
        heavy_waiting_per_client: 32,
    };
    let st = kdx_api::Api::new(store, settings, kdx_api::TipWatch::default()).state();
    Http { _dir: d, st }
}

/// The `/graphql` app, with the listener's guard when `guarded`.
fn app(schema: KdxSchema, st: &Arc<kdx_api::ApiState>, guarded: bool) -> axum::Router {
    let r = kdx_graphql::router(schema);
    let r = if guarded {
        r.layer(axum::middleware::from_fn_with_state(
            st.clone(),
            kdx_api::server::guard,
        ))
    } else {
        r
    };
    r.with_state(st.clone())
}

async fn send(
    app: axum::Router,
    body: axum::body::Body,
    run: Option<kdx_api::RunDeadline>,
) -> (StatusCode, Value, Duration) {
    use tower::ServiceExt;
    let mut req = axum::http::Request::post("/graphql")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();
    if let Some(r) = run {
        req.extensions_mut().insert(r);
    }
    let t = Instant::now();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let b = axum::body::to_bytes(resp.into_body(), 64 << 20)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&b).unwrap_or(Value::Null),
        t.elapsed(),
    )
}

/// I1: naming the fields without running them (a comment, a string, an unused fragment, an
/// operation not selected, another batch member) leaves the request at its normal
/// deadline; only a Pact field that runs extends it.
#[tokio::test]
async fn only_a_running_pact_field_extends_the_deadline() {
    let (base, m) = mock(|_, _| ok("1")).await;
    let h = http_state(Duration::from_secs(5));
    let (schema, _) = schema_with(base, open(), Some(h.st.clone()), None);
    let a = app(schema, &h.st, false);
    let smuggled = [
        (
            "comment",
            json!({"query": "# pactQuery gasLimitEstimate\n{ __typename }"}),
        ),
        (
            "string",
            json!({"query": "{ __type(name: \"pactQuery gasLimitEstimate\") { name } }"}),
        ),
        (
            "unused fragment",
            json!({"query": "{ __typename } fragment F on Query { pactQuery(pactQuery: [{chainId: \"0\", code: \"1\"}]) { status } }"}),
        ),
        (
            "operation not selected",
            json!({"query": "query A { __typename } query B { pactQuery(pactQuery: [{chainId: \"0\", code: \"1\"}]) { status } }", "operationName": "A"}),
        ),
        (
            "batch member",
            json!([{"query": "{ __typename }"}, {"query": "# pactQuery\n{ gasLimitEstimate: __typename }"}]),
        ),
    ];
    for (case, body) in smuggled {
        let at = Instant::now() + Duration::from_secs(5);
        let run = kdx_api::RunDeadline::new(at, Duration::from_secs(11));
        let before = run.get();
        let (st, v, _) = send(a.clone(), body.to_string().into(), Some(run.clone())).await;
        assert_eq!(run.get(), before, "{case}: {st} {v}");
    }
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
    // The same document selected and run: extended, within the ceiling.
    let at = Instant::now() + Duration::from_secs(5);
    let run = kdx_api::RunDeadline::new(at, Duration::from_secs(11));
    let body = json!({"query": "query A { __typename } query B { pactQuery(pactQuery: [{chainId: \"0\", code: \"1\"}]) { status } }", "operationName": "B"});
    let (_, v, _) = send(a.clone(), body.to_string().into(), Some(run.clone())).await;
    assert_eq!(v["data"]["pactQuery"][0]["status"], "success", "{v}");
    assert!(run.get() > at + Duration::from_secs(5), "extended");
    assert!(run.get() <= at + Duration::from_secs(11), "ceiling");
}

/// I1/I2 through the guard: a Pact field that runs gets past the listener's deadline (its
/// node answer arrives after it), and its own timeout answers as stock's `timeout` item,
/// not a 504.
#[tokio::test]
async fn a_running_pact_field_outlives_the_listener_deadline() {
    let (base, _m) = mock(|_, v| {
        let delay = if code_of(v) == "slow" { 1500 } else { 5000 };
        (200, ok("1").1, Duration::from_millis(delay))
    })
    .await;
    let h = http_state(Duration::from_secs(1));
    let s = PactSettings {
        timeout: Duration::from_secs(3),
        ..open()
    };
    h.st.set_graphql_extra_deadline(s.timeout + Duration::from_secs(1));
    let (schema, _) = schema_with(base, s, Some(h.st.clone()), None);
    let a = app(schema, &h.st, true);
    let q = |code: &str| json!({"query": PQ, "variables": {"q": [{"chainId": "0", "code": code}]}});
    let (st, v, el) = send(a.clone(), q("slow").to_string().into(), None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["data"]["pactQuery"][0]["status"], "success", "{v}");
    assert!(el >= Duration::from_millis(1400), "{el:?}");
    let (st, v, el) = send(a, q("stuck").to_string().into(), None).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["data"]["pactQuery"][0]["status"], "timeout", "{v}");
    assert!(el < Duration::from_millis(4500), "{el:?}");
}

/// I2: a body trickled in is cut at the listener's normal deadline even with the
/// passthrough on (the guard's extra is only a ceiling).
#[tokio::test]
async fn a_slow_body_is_cut_at_the_normal_deadline() {
    let (base, _m) = mock(|_, _| ok("1")).await;
    let h = http_state(Duration::from_secs(1));
    let s = PactSettings {
        timeout: Duration::from_secs(5),
        ..open()
    };
    h.st.set_graphql_extra_deadline(s.timeout + Duration::from_secs(1));
    let (schema, _) = schema_with(base, s, Some(h.st.clone()), None);
    let a = app(schema, &h.st, true);
    let chunks = futures::stream::unfold(0u32, |i| async move {
        if i >= 30 {
            return None;
        }
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let c = if i == 0 {
            "{\"query\": \"{ pactQuery".to_string()
        } else {
            " ".into()
        };
        Some((Ok::<_, std::io::Error>(c), i + 1))
    });
    let (st, _, el) = send(a, axum::body::Body::from_stream(chunks), None).await;
    assert_ne!(st, StatusCode::OK);
    assert!(el < Duration::from_millis(1500), "{st} {el:?}");
}

/// I5: one item budget for every operation of a JSON batch over HTTP.
#[tokio::test]
async fn batch_operations_share_the_item_budget_over_http() {
    let (base, m) = mock(|_, _| ok("1")).await;
    let h = http_state(Duration::from_secs(5));
    let s = PactSettings {
        max_items: 3,
        ..open()
    };
    let (schema, _) = schema_with(base, s, Some(h.st.clone()), None);
    let a = app(schema, &h.st, false);
    let two = json!([{"chainId": "0", "code": "1"}, {"chainId": "0", "code": "2"}]);
    let body = json!([
        {"query": PQ, "variables": {"q": two}},
        {"query": PQ, "variables": {"q": two}},
    ]);
    let (_, v, _) = send(a, body.to_string().into(), None).await;
    let r = v.as_array().unwrap();
    let ok_n = r
        .iter()
        .filter(|x| x["data"]["pactQuery"].is_array())
        .count();
    assert_eq!(ok_n, 1, "{v}");
    assert!(
        r.iter().any(|x| err_msg(x).contains("[LIMIT] At most 3")),
        "{v}"
    );
    assert_eq!(m.hits.load(Ordering::SeqCst), 2);
}

/// M7: a request dropped mid-call gives its node slot and its request place back.
#[tokio::test]
async fn a_cancelled_request_releases_its_slot_and_place() {
    let (base, m) = mock(|_, _| (200, ok("1").1, Duration::from_secs(3))).await;
    let s = PactSettings {
        max_in_flight: 2,
        ..open()
    };
    let (schema, pass) = schema_with(base, s, None, None);
    let schema = Arc::new(schema);
    assert_eq!(pass.idle(), (2, 4));
    let sc = schema.clone();
    let h = tokio::spawn(async move {
        run(
            &sc,
            PQ,
            json!({"q": [{"chainId": "0", "code": "1"}]}),
            [1, 1, 1, 1],
        )
        .await
    });
    let t = Instant::now();
    while m.hits.load(Ordering::SeqCst) == 0 && t.elapsed() < Duration::from_secs(2) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(pass.idle(), (1, 3), "held while running");
    h.abort();
    let _ = h.await;
    assert_eq!(pass.idle(), (2, 4), "released on cancel");
}

fn signed_cmd(meta: &str) -> String {
    format!(
        r#"{{"networkId":"mainnet01","payload":{{"exec":{{"code":"(+ 1 2)","data":{{}}}}}},"signers":[],"meta":{meta},"nonce":"n"}}"#
    )
}

/// M1: a command with a key given twice, at any depth or spelled with an escape, never
/// reaches the node.
#[tokio::test]
async fn duplicate_keys_never_reach_the_node() {
    let (base, m) = mock(|_, _| ok("1")).await;
    let s = schema(base, open());
    let meta_ok =
        r#"{"chainId":"0","sender":"s","gasLimit":100,"gasPrice":1e-8,"ttl":600,"creationTime":1}"#;
    let dupes = [
        signed_cmd(r#"{"chainId":"0","gasLimit":1000000000,"gasLimit":100}"#),
        // The second key spelled with an escape (`\u0067` is `g`): compared decoded.
        signed_cmd(r#"{"chainId":"0","gasLimit":100,"\u0067asLimit":1000000000}"#),
        signed_cmd(&format!("{meta_ok},\"meta\":{meta_ok}").replace("}\",", "},")),
        signed_cmd(meta_ok).replace(
            "\"networkId\":\"mainnet01\"",
            "\"networkId\":\"x\",\"networkId\":\"mainnet01\"",
        ),
        signed_cmd(meta_ok).replace("\"data\":{}", "\"data\":{\"a\":1,\"a\":2}"),
    ];
    for cmd in dupes {
        let input = json!({ "cmd": cmd }).to_string();
        let r = run(&s, GAS, json!({ "i": [input] }), [3, 3, 3, 3]).await;
        assert_eq!(err_msg(&r), kdx_graphql::pact::GAS_DUP_KEYS, "{cmd}: {r}");
    }
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
    // The same command without the duplicate is sent.
    let input = json!({ "cmd": signed_cmd(meta_ok) }).to_string();
    let r = run(&s, GAS, json!({ "i": [input] }), [3, 3, 3, 3]).await;
    assert_ne!(err_msg(&r), kdx_graphql::pact::GAS_DUP_KEYS, "{r}");
    assert_eq!(m.hits.load(Ordering::SeqCst), 1);
}

/// M4: chain `"20"` is past mainnet's last chain; the count comes from the settings.
#[tokio::test]
async fn chain_20_is_refused_and_the_count_is_configured() {
    let (base, m) = mock(|_, _| ok("1")).await;
    let q = json!({ "q": [{"chainId": "20", "code": "1"}, {"chainId": "19", "code": "1"}] });
    let s = schema(base.clone(), open());
    let r = run(&s, PQ, q.clone(), [4, 4, 4, 4]).await;
    assert_eq!(r["data"]["pactQuery"][0]["status"], "error", "{r}");
    assert_eq!(r["data"]["pactQuery"][1]["status"], "success", "{r}");
    assert_eq!(m.hits.load(Ordering::SeqCst), 1);
    let s = schema(
        base,
        PactSettings {
            chains: 21,
            ..open()
        },
    );
    let r = run(&s, PQ, q, [4, 4, 4, 4]).await;
    assert_eq!(r["data"]["pactQuery"][0]["status"], "success", "{r}");
}

async fn run_ip(s: &KdxSchema, ip: IpAddr, n: usize) -> bool {
    let items = vec![json!({"chainId": "0", "code": "1"}); n];
    let req = async_graphql::Request::new(PQ)
        .variables(async_graphql::Variables::from_json(json!({ "q": items })))
        .data(ClientIp(ip))
        .data(PactBudget::default());
    let r = serde_json::to_value(s.execute(req).await).unwrap();
    r["data"]["pactQuery"].is_array()
}

fn rated() -> PactSettings {
    PactSettings {
        max_items: 2,
        per_ip_rps: 1,
        per_ip_burst: 2,
        ..PactSettings::default()
    }
}

/// I3: on a dual-stack listener IPv4 clients arrive v4-mapped; each keeps its own bucket
/// (the mapped and plain forms are one client).
#[tokio::test]
async fn v4_mapped_clients_are_not_merged() {
    let (base, _m) = mock(|_, _| ok("1")).await;
    let (s, _) = schema_with(base, rated(), None, None);
    let mapped = |a: [u8; 4]| IpAddr::from(std::net::Ipv4Addr::from(a).to_ipv6_mapped());
    assert!(run_ip(&s, mapped([1, 2, 3, 4]), 2).await);
    assert!(!run_ip(&s, mapped([1, 2, 3, 4]), 1).await, "drained");
    assert!(
        !run_ip(&s, IpAddr::from([1, 2, 3, 4]), 1).await,
        "same client"
    );
    for i in 0..20u8 {
        assert!(run_ip(&s, mapped([5, 6, 7, i]), 2).await, "client {i}");
    }
}

/// I3: a flood of new keys evicts drained buckets but never refills them: a returning
/// drained /64 starts empty and draws on its /48's overflow bucket, which every /64 of
/// that /48 shares; another /48 has its own.
#[tokio::test]
async fn a_key_flood_never_refills_and_the_48_tier_holds() {
    let (base, _m) = mock(|_, _| ok("1")).await;
    let (s, _) = schema_with(base, rated(), None, Some(16));
    let in48 = |site: u16, net: u16| IpAddr::from([0x2001, 0xdb8, site, net, 0, 0, 0, 1]);
    // 16 /64s of one /48 drain their buckets and fill the table.
    for n in 0..16 {
        assert!(run_ip(&s, in48(0, n), 2).await);
    }
    // A flood of new /64s from other /48s evicts them.
    for site in 100..164 {
        run_ip(&s, in48(site, 0), 1).await;
    }
    // Back, the drained /64s and new ones of their /48 share one overflow burst.
    let mut admitted = 0;
    for n in 0..40 {
        if run_ip(&s, in48(0, n), 1).await {
            admitted += 1;
        }
    }
    assert!(admitted <= 3, "{admitted} items admitted to a drained /48");
    // Another /48 is not held to that share once the flood's buckets have refilled (while
    // the overflow table is under pressure a new prefix starts empty too: the API
    // limiter's rule).
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert!(run_ip(&s, in48(7, 0), 2).await);
}

/// Secpass B M-6: a request whose items all fail the local checks (an unknown chain, an
/// oversize item) runs nothing on the node and keeps its normal deadline.
#[tokio::test]
async fn items_failing_local_checks_do_not_extend_the_deadline() {
    let (base, m) = mock(|_, _| ok("1")).await;
    let h = http_state(Duration::from_secs(5));
    let s = PactSettings {
        max_input_bytes: 8,
        ..open()
    };
    let (schema, _) = schema_with(base, s, Some(h.st.clone()), None);
    let a = app(schema, &h.st, false);
    let items = json!([{"chainId": "99", "code": "1"}, {"chainId": "0", "code": "(+ 1 2 3 4 5)"}]);
    let at = Instant::now() + Duration::from_secs(5);
    let run = kdx_api::RunDeadline::new(at, Duration::from_secs(11));
    let before = run.get();
    let body = json!({"query": PQ, "variables": {"q": items}});
    let (_, v, _) = send(a, body.to_string().into(), Some(run.clone())).await;
    assert_eq!(v["data"]["pactQuery"][0]["status"], "error", "{v}");
    assert_eq!(v["data"]["pactQuery"][1]["status"], "error", "{v}");
    assert_eq!(run.get(), before, "{v}");
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
}

/// Secpass B M-8: one time cap per request for its Pact items: work that shares a request's
/// budget (aliases, batch operations, anything nested later) ends within one `timeout` of
/// the first Pact field, not one `timeout` per field.
#[tokio::test]
async fn pact_items_share_one_time_cap_per_request() {
    let (base, m) = mock(|_, _| (200, ok("1").1, Duration::from_secs(5))).await;
    let s = schema(
        base,
        PactSettings {
            timeout: Duration::from_millis(300),
            ..open()
        },
    );
    let budget = PactBudget::default();
    let exec = |b: PactBudget| {
        let req = async_graphql::Request::new(PQ)
            .variables(async_graphql::Variables::from_json(
                json!({ "q": [{"chainId": "0", "code": "1"}] }),
            ))
            .data(ClientIp(IpAddr::from([1, 2, 3, 4])))
            .data(b);
        s.execute(req)
    };
    let r = serde_json::to_value(exec(budget.clone()).await).unwrap();
    assert_eq!(r["data"]["pactQuery"][0]["status"], "timeout", "{r}");
    let hits = m.hits.load(Ordering::SeqCst);
    let t = Instant::now();
    let r = serde_json::to_value(exec(budget).await).unwrap();
    assert_eq!(r["data"]["pactQuery"][0]["status"], "timeout", "{r}");
    assert!(
        t.elapsed() < Duration::from_millis(150),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(
        m.hits.load(Ordering::SeqCst),
        hits,
        "past the cap: no node call"
    );
}

/// Secpass B M-10: a field refused by the per-client rate spends none of the request's item
/// budget, so a later field is refused for the rate too, not for the item cap.
#[tokio::test]
async fn a_rate_refusal_spends_no_item_budget() {
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
    let one = json!({ "q": [{"chainId": "0", "code": "1"}, {"chainId": "0", "code": "2"}] });
    let r = run(&s, PQ, one, [7, 7, 7, 1]).await;
    assert_eq!(r["data"]["pactQuery"][1]["status"], "success", "{r}");
    let q = "{ a: pactQuery(pactQuery: [{chainId: \"0\", code: \"1\"}]) { status } \
             b: pactQuery(pactQuery: [{chainId: \"0\", code: \"1\"}]) { status } \
             c: pactQuery(pactQuery: [{chainId: \"0\", code: \"1\"}]) { status } }";
    let r = run(&s, q, json!({}), [7, 7, 7, 1]).await;
    let errs = r["errors"].as_array().unwrap();
    assert_eq!(errs.len(), 3, "{r}");
    for e in errs {
        assert!(
            e["message"].as_str().unwrap().contains("[RATE_LIMIT]"),
            "{r}"
        );
    }
}

/// Secpass B M-9: with the per-client limit on, a request with no peer address (embedding,
/// tests; never one the HTTP server takes) is refused rather than sharing one bucket.
#[tokio::test]
async fn no_peer_address_shares_no_bucket() {
    let (base, m) = mock(|_, _| ok("1")).await;
    let s = schema(
        base,
        PactSettings {
            per_ip_rps: 1,
            per_ip_burst: 20,
            ..PactSettings::default()
        },
    );
    let req = async_graphql::Request::new(PQ)
        .variables(async_graphql::Variables::from_json(
            json!({ "q": [{"chainId": "0", "code": "1"}] }),
        ))
        .data(PactBudget::default());
    let r = serde_json::to_value(s.execute(req).await).unwrap();
    assert!(err_msg(&r).contains("[RATE_LIMIT]"), "{r}");
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
}

/// The account-miss reader sends each node read once: a 5xx answer is one request, not the
/// client's three tries with backoff, and it fails fast (a slot held for one call only).
#[tokio::test]
async fn account_miss_details_are_sent_once_on_a_server_error() {
    let (base, m) = mock(|_, _| (503, "busy".into(), Duration::ZERO)).await;
    let node = kdx_node::NodeClient::new(&kdx_node::NodeClientConfig {
        base,
        network: "mainnet01".into(),
        timeout: Duration::from_secs(5),
        max_rps: 0,
        concurrency: 4,
        p2p_url: None,
        p2p_cert_sha256: None,
        max_body_bytes: 1 << 20,
        max_sse_event_bytes: 1 << 16,
    })
    .unwrap();
    let local = Arc::new(kdx_node::LocalClient::new(
        node,
        kdx_node::LocalOpts {
            rps: 0,
            concurrency: 4,
        },
    ));
    let read = kdx_graphql::miss::MissReader::local_details(local, Duration::from_secs(2));
    let t0 = Instant::now();
    let r = read(0, "coin".into(), "k:ab".into()).await;
    assert!(r.is_err());
    assert_eq!(m.hits.load(Ordering::SeqCst), 1, "one attempt");
    assert!(
        t0.elapsed() < Duration::from_millis(400),
        "{:?}",
        t0.elapsed()
    );
}
