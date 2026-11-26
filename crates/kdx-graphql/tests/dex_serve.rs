//! Stock's DEX root fields served from the Sushi ledger (stock-gap B3): each field over a
//! small store, its JSON shape (field names, string vs number, nesting) against stock's
//! answers (`tests/data/dex-stock-samples.json`, one read-only query per field, 2026-11-26),
//! stock's paging and cursors, the junk-pair filter, a poisoned pair, error texts and the
//! deadline on long ranges.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use kdx_api::{Api, ApiError, ApiSettings, ApiState, Store, TipWatch};
use kdx_graphql::dexserve::{self, Budget};
use kdx_graphql::{build_schema, router, GraphqlSettings};
use kdx_hot::dex::{self as hd, Amount, DexEvent, Kind};
use kdx_hot::{HotStore, Table};
use kdx_types::TxKey;
use serde_json::{json, Value};
use tower::ServiceExt;

const SUSHI: &str = hd::SUSHI_EXCHANGE;
const DAY: u64 = 86_400;

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn state(d: &tempfile::TempDir) -> (Arc<HotStore>, Arc<ApiState>) {
    let g = kdx_hot::Geometry {
        max_gb: 1,
        growth_mb: 16,
    };
    let hot = Arc::new(HotStore::open(&d.path().join("hot"), g).unwrap());
    let store = Arc::new(Store::open(hot.clone(), d.path()).unwrap());
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
    (hot, Api::new(store, settings, TipWatch::default()).state())
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

async fn data(st: &Arc<ApiState>, q: &str) -> Value {
    let (_, v) = query(st, q).await;
    assert!(v.get("errors").is_none(), "{q}: {v}");
    v["data"].clone()
}

struct W<'a> {
    txn: kdx_hot::RwTxn<'a>,
    pos: u16,
}

impl W<'_> {
    /// Store one event at block time `t` (s) with tx time `t` (each event its own tx).
    fn ev(&mut self, pair: &str, kind: Kind, t: u64, fields: &[&str]) {
        self.pos += 1;
        let key = TxKey::new(1_000_000 + (t / 30) as u32, 2, self.pos);
        let e = DexEvent {
            kind,
            pair: pair.into(),
            fields: fields.iter().map(|s| s.to_string()).collect(),
        };
        assert!(hd::store_event(&mut self.txn, key, 0, t * 1_000_000, t, &e).unwrap());
    }

    fn pair(&mut self, pair: &str, t: u64, r0: &str, r1: &str) {
        self.ev(pair, Kind::CreatePair, t, &[pair, "acct"]);
        self.ev(pair, Kind::AddLiquidity, t, &["lp", "lp", r0, r1, "100"]);
        self.ev(pair, Kind::Mint, t, &["alice", "100"]);
        self.ev(pair, Kind::Update, t, &[r0, r1]);
    }
}

/// Mark the DEX history fill finished (the DEX fields answer `NOT_AVAILABLE` before it).
fn mark_filled(txn: &mut kdx_hot::RwTxn<'_>) {
    txn.set_meta(hd::meta::DEX_FILLED, &1u32.to_be_bytes())
        .unwrap();
}

/// Two listable pools (`coin:kaddex.kdx` small, `coin:free.maga` large), a pair with no coin
/// leg, a pair with no reserves, and a poisoned pair. KDA/USD 0.005 from yesterday on.
fn fixture(hot: &HotStore) -> u64 {
    let today = now() / DAY * DAY;
    let y = today - DAY + 60;
    let mut w = W {
        txn: hot.rw().unwrap(),
        pos: 0,
    };
    w.pair("coin:kaddex.kdx", y, "10", "1000");
    // Yesterday: a swap of 2 KDA in. Today: 1 KDA in, then 9 kdx in for 0.1 KDA out.
    w.ev(
        "coin:kaddex.kdx",
        Kind::Swap,
        y + 60,
        &["bob", "bob", "2", "0", "0", "160"],
    );
    w.ev("coin:kaddex.kdx", Kind::Update, y + 60, &["12", "840"]);
    w.ev(
        "coin:kaddex.kdx",
        Kind::Swap,
        today + 1,
        &["carol", "carol", "1", "0", "0", "70"],
    );
    w.ev("coin:kaddex.kdx", Kind::Update, today + 1, &["13", "770"]);
    w.ev(
        "coin:kaddex.kdx",
        Kind::Swap,
        today + 2,
        &["dave", "dave", "0", "9", "0.1", "0"],
    );
    w.ev("coin:kaddex.kdx", Kind::Update, today + 2, &["12.9", "779"]);
    w.pair("coin:free.maga", y, "500", "250000");
    w.pair("free.a:free.b", y, "5", "5");
    w.ev(
        "coin:free.zero",
        Kind::CreatePair,
        y,
        &["coin:free.zero", "acct"],
    );
    w.pair("coin:free.bad", y, "50", "50");
    w.txn
        .put(Table::DexAgg, &hd::poison_key("coin:free.bad"), &[])
        .unwrap();
    mark_filled(&mut w.txn);
    for h in (y / 3600)..=(now() / 3600) {
        kdx_hot::dex_usd::put(&mut w.txn, h as u32, &Amount::parse("0.005").unwrap()).unwrap();
    }
    w.txn.commit().unwrap();
    today
}

/// A value's shape: object keys with their shapes, the first item of a list, a JSON type.
fn shape(v: &Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(m.iter().map(|(k, v)| (k.clone(), shape(v))).collect()),
        Value::Array(a) => json!([a.first().map(shape)]),
        Value::String(_) => json!("string"),
        Value::Number(_) => json!("number"),
        Value::Bool(_) => json!("bool"),
        Value::Null => json!("null"),
    }
}

/// `ours` has stock's shape wherever both hold a value (an empty list matches any list),
/// with stock's member order (the server writes selection order, PR #57).
fn same_shape(ours: &Value, stock: &Value, at: &str) {
    match (ours, stock) {
        (Value::Object(a), Value::Object(b)) => {
            let ka: Vec<_> = a.keys().collect();
            let kb: Vec<_> = b.keys().collect();
            assert_eq!(ka, kb, "{at}: keys (in order)");
            for (k, v) in a {
                // Stock's DB row id; ours is null (not reproducible, divergence 11).
                if k == "transactionId" && v.is_null() {
                    continue;
                }
                same_shape(v, &b[k], &format!("{at}.{k}"));
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            if let (Some(x), Some(y)) = (a.first(), b.first()) {
                same_shape(x, y, &format!("{at}[0]"));
            }
        }
        _ => assert_eq!(shape(ours), shape(stock), "{at}: {ours} vs {stock}"),
    }
}

fn samples() -> Value {
    serde_json::from_str(include_str!("data/dex-stock-samples.json")).unwrap()
}

const POOL_NODE: &str = "id address key reserve0 reserve1 totalSupply tvlUsd tvlChange24h volume24hUsd volumeChange24h volume7dUsd fees24hUsd feesChange24h transactionCount24h transactionCountChange24h apr24h createdAt updatedAt token0{id name chainId address} token1{id name chainId address}";
const TX_NODE: &str = "id maker amount0In amount1In amount0Out amount1Out amountUsd timestamp transactionId requestkey transactionType";

#[tokio::test]
async fn pools_match_stock_shape_sort_and_paging() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    fixture(&hot);
    let v = data(&st, &format!("{{ pools(first:2){{ totalCount pageInfo{{hasNextPage hasPreviousPage startCursor endCursor}} edges{{cursor node{{{POOL_NODE}}}}}}}}}")).await;
    let p = &v["pools"];
    // Junk filtered: no coin leg, no reserves, poisoned.
    assert_eq!(p["totalCount"], 2, "{p}");
    // Stock's first-probe answer (2026-11-26), one edge.
    let stock: Value = serde_json::from_str(r#"{"totalCount":19,"pageInfo":{"hasNextPage":true,"hasPreviousPage":false,"startCursor":"MTk=","endCursor":"MTg="},"edges":[{"cursor":"MTk=","node":{"id":"n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange-coin:n_2669414de420c0d40bbc3caa615e989eaba83d6f.happy","address":"n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange","key":"coin:n_2669414de420c0d40bbc3caa615e989eaba83d6f.happy","reserve0":"0.000029769003","reserve1":"0.033775598738","totalSupply":"-47455.927843314625","tvlUsd":5.8e-7,"tvlChange24h":-17.142857142857146,"volume24hUsd":4e-8,"volumeChange24h":-66.66666666666666,"volume7dUsd":0,"fees24hUsd":0,"feesChange24h":0,"transactionCount24h":1,"transactionCountChange24h":-50,"apr24h":0,"createdAt":"2025-10-17T23:02:47.034Z","updatedAt":"2025-12-04T15:33:31.789Z","token0":{"id":"1","name":"coin","chainId":"0","address":"coin"},"token1":{"id":"14","name":"happy","chainId":"0","address":"n_2669414de420c0d40bbc3caa615e989eaba83d6f.happy"}}}]}"#).unwrap();
    same_shape(p, &stock, "pools");
    // Correct TVL_USD_DESC (divergence 2), stock's cursor shape: base64 rank, first = count.
    let e = p["edges"].as_array().unwrap();
    assert_eq!(e[0]["node"]["key"], "coin:free.maga");
    assert_eq!(e[0]["cursor"], "Mg==");
    assert_eq!(e[1]["cursor"], "MQ==");
    let k = &e[1]["node"];
    assert_eq!(k["id"], format!("{SUSHI}-coin:kaddex.kdx"));
    assert_eq!(
        (k["reserve0"].as_str(), k["reserve1"].as_str()),
        (Some("12.9"), Some("779"))
    );
    assert_eq!(k["totalSupply"], "100");
    // TVL = 2 × 12.9 KDA × 0.005; 24h volume = (1 + 0.1) KDA × 0.005; fees × 3/997.
    assert_eq!(k["tvlUsd"].as_f64(), Some(0.129));
    assert_eq!(k["volume24hUsd"].as_f64(), Some(0.0055));
    assert_eq!(k["fees24hUsd"].as_f64(), Some(0.00001655));
    assert_eq!(k["transactionCount24h"], 2);
    assert_eq!(k["volume7dUsd"].as_f64(), Some(0.01), "yesterday's 2 KDA");
    // Yesterday 0.01 → today 0.0055: −45 %; TVL 0.12 → 0.129: +7.5 %.
    assert!((k["volumeChange24h"].as_f64().unwrap() + 45.0).abs() < 1e-9);
    assert!((k["tvlChange24h"].as_f64().unwrap() - 7.5).abs() < 1e-9);
    assert_eq!(k["token1"]["name"], "kdx");
    assert_eq!(k["token1"]["address"], "kaddex.kdx");
    // Paging: first:1, then after its cursor; last:1 is the smallest; ASC flips.
    let q = |a: &str| {
        format!("{{ pools({a}){{ totalCount pageInfo{{hasNextPage hasPreviousPage startCursor endCursor}} edges{{cursor node{{key}}}}}}}}")
    };
    let a = data(&st, &q("first:1")).await;
    assert_eq!(a["pools"]["pageInfo"]["hasNextPage"], true);
    let next = data(&st, &q(r#"first:1, after:"Mg==""#)).await;
    assert_eq!(next["pools"]["edges"][0]["node"]["key"], "coin:kaddex.kdx");
    assert_eq!(next["pools"]["pageInfo"]["hasNextPage"], false);
    assert_eq!(next["pools"]["pageInfo"]["hasPreviousPage"], true);
    let back = data(&st, &q(r#"last:1, before:"MQ==""#)).await;
    assert_eq!(back["pools"]["edges"][0]["node"]["key"], "coin:free.maga");
    let last = data(&st, &q("last:1")).await;
    assert_eq!(last["pools"]["edges"][0]["node"]["key"], "coin:kaddex.kdx");
    let asc = data(&st, &q("orderBy:TVL_USD_ASC")).await;
    assert_eq!(asc["pools"]["edges"][0]["node"]["key"], "coin:kaddex.kdx");
    let vol = data(&st, &q("orderBy:VOLUME_7D_DESC")).await;
    assert_eq!(vol["pools"]["edges"][0]["node"]["key"], "coin:kaddex.kdx");
    // A garbage cursor is stock's parseInt || 0: nothing below rank 0.
    let junk = data(&st, &q(r#"after:"!!""#)).await;
    assert_eq!(junk["pools"]["edges"], json!([]));
    let other = data(&st, &q(r#"protocolAddress:"free.other""#)).await;
    assert_eq!(other["pools"]["totalCount"], 0);
    let (_, neg) = query(&st, &q("first:-1")).await;
    assert!(
        neg["errors"][0]["message"]
            .as_str()
            .unwrap()
            .contains("LIMIT must not be negative"),
        "{neg}"
    );
}

#[tokio::test]
async fn pool_transactions_and_pool() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let today = fixture(&hot);
    let s = samples();
    let id = format!("{SUSHI}-coin:kaddex.kdx");
    let v = data(&st, &format!("{{ pool(id:\"{id}\", first:2){{ id key totalSupply charts(timeFrame:MONTH){{ volume{{timestamp value}} tvl{{timestamp value}} fees{{timestamp value}}}} transactions(first:2){{ totalCount pageInfo{{hasNextPage hasPreviousPage startCursor endCursor}} edges{{cursor node{{{TX_NODE}}}}}}}}}}}")).await;
    same_shape(&v["pool"], &s["pool"], "pool");
    let t = &v["pool"]["transactions"];
    assert_eq!(t["totalCount"], 4, "1 add + 3 swaps: {t}");
    let e = t["edges"].as_array().unwrap();
    assert_eq!(e.len(), 2);
    assert_eq!(e[0]["node"]["maker"], "dave");
    assert_eq!(e[0]["node"]["amount1In"], "9");
    assert_eq!(e[0]["node"]["amount0Out"], "0.1");
    assert_eq!(e[0]["node"]["amountUsd"], "0.0005000000");
    assert_eq!(e[1]["node"]["amountUsd"], "0.0050000000");
    assert_eq!(e[0]["node"]["transactionType"], "SWAP");
    // Stock's timestamp: the tx's creationTime.
    let ts = kdx_graphql::scalars::DateTime(((today + 2) * 1_000_000) as i64).text();
    assert_eq!(e[0]["node"]["timestamp"], ts);
    assert_eq!(t["pageInfo"]["hasNextPage"], true);
    assert_eq!(t["pageInfo"]["hasPreviousPage"], false);
    // Paging by id: no repeat, no skip, forward and backward.
    let q = |a: String| {
        format!("{{ poolTransactions(pairId:\"{id}\", {a}){{ totalCount pageInfo{{hasNextPage hasPreviousPage startCursor endCursor}} edges{{cursor node{{maker transactionType}}}}}}}}")
    };
    let mut seen = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let a = match &after {
            Some(c) => format!("first:1, after:\"{c}\""),
            None => "first:1".into(),
        };
        let p = data(&st, &q(a)).await;
        let c = &p["poolTransactions"];
        for e in c["edges"].as_array().unwrap() {
            seen.push(e["node"]["maker"].as_str().unwrap().to_string());
        }
        if c["pageInfo"]["hasNextPage"] != true {
            break;
        }
        after = c["pageInfo"]["endCursor"].as_str().map(String::from);
    }
    assert_eq!(seen, ["dave", "carol", "bob", "lp"]);
    let old = data(&st, &q("last:2".into())).await;
    let makers: Vec<_> = old["poolTransactions"]["edges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["node"]["maker"].clone())
        .collect();
    assert_eq!(
        makers,
        [json!("lp"), json!("bob")],
        "last: oldest first, as stock"
    );
    let adds = data(&st, &q("type:ADD_LIQUIDITY".into())).await;
    assert_eq!(adds["poolTransactions"]["totalCount"], 1);
    // Stock's error text for an unreadable cursor.
    let (_, bad) = query(&st, &q("first:2, after:\"zzz\"".into())).await;
    assert_eq!(
        bad["errors"][0]["message"],
        s["badCursor"]["errors"][0]["message"]
    );
    let none = data(&st, &q("first:2".into()).replace(&id, "nope")).await;
    assert_eq!(none["poolTransactions"]["totalCount"], 0);
    // Unknown pool: stock's text. Poisoned pool: unavailable, siblings still answer.
    let (_, nf) = query(&st, "{ pool(id:\"nope\"){ id } }").await;
    assert_eq!(
        nf["errors"][0]["message"],
        s["poolNotFound"]["errors"][0]["message"]
    );
    let (_, poisoned) = query(&st, &format!("{{ pool(id:\"{SUSHI}-coin:free.bad\"){{ id }} graphConfiguration{{ minimumBlockHeight }} }}")).await;
    assert!(
        poisoned["errors"][0]["message"]
            .as_str()
            .unwrap()
            .contains("unavailable"),
        "{poisoned}"
    );
    assert!(
        poisoned["data"]["graphConfiguration"].is_object(),
        "{poisoned}"
    );
    // A junk pair is not listed but still answers by id.
    let j = data(
        &st,
        &format!("{{ pool(id:\"{SUSHI}-free.a:free.b\"){{ key tvlUsd }} }}"),
    )
    .await;
    assert_eq!(j["pool"]["tvlUsd"].as_f64(), Some(0.0));
}

#[tokio::test]
async fn charts_metrics_prices_positions_tokens() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let today = fixture(&hot);
    let s = samples();
    let id = format!("{SUSHI}-coin:kaddex.kdx");
    let c = data(&st, &format!("{{ pool(id:\"{id}\"){{ d: charts(timeFrame:DAY){{ volume{{timestamp value}} tvl{{timestamp value}} fees{{timestamp value}}}} w: charts(timeFrame:WEEK){{ tvl{{timestamp value}}}} a: charts(timeFrame:ALL){{ volume{{timestamp value}} tvl{{timestamp value}} fees{{timestamp value}}}}}}}}")).await;
    same_shape(&c["pool"], &s["charts"], "charts");
    let p = &c["pool"];
    // WEEK has a TVL point per day since creation (divergence 6): yesterday and today.
    assert_eq!(p["w"]["tvl"].as_array().unwrap().len(), 2, "{p}");
    assert_eq!(p["w"]["tvl"][1]["value"], "0.12900000");
    assert_eq!(p["d"]["tvl"][0]["value"], "0.12900000");
    // DAY: hourly volume for yesterday and today: 2 KDA, then 1 + 0.1 KDA.
    let dv: Vec<_> = p["d"]["volume"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["value"].clone())
        .collect();
    assert_eq!(dv, [json!("0.0100000000"), json!("0.0055000000")]);
    assert_eq!(p["a"]["fees"][1]["value"], "0.0000165500");
    // dexMetrics: Sushi by default (divergence 3), stock's shape.
    let start = kdx_graphql::scalars::DateTime(((today - DAY) * 1_000_000) as i64).text();
    let m = data(&st, &format!("{{ a: dexMetrics(protocolAddress:\"{SUSHI}\", startDate:\"{start}\"){{ totalPools currentTvlUsd totalVolumeUsd tvlHistory{{timestamp value}} volumeHistory{{timestamp value}}}} b: dexMetrics{{ totalPools currentTvlUsd totalVolumeUsd}}}}")).await;
    same_shape(&m, &s["dexMetrics"], "dexMetrics");
    assert_eq!(m["a"]["totalPools"], 2);
    assert_eq!(m["b"]["totalPools"], 2);
    // 0.129 + 2 × 500 × 0.005.
    assert_eq!(m["a"]["currentTvlUsd"].as_f64(), Some(5.129));
    assert_eq!(m["a"]["totalVolumeUsd"].as_f64(), Some(0.0155));
    assert_eq!(m["a"]["tvlHistory"].as_array().unwrap().len(), 2);
    let z = data(&st, "{ dexMetrics(protocolAddress:\"kdlaunch.kdswap-exchange\"){ totalPools currentTvlUsd tvlHistory{value} } }").await;
    assert_eq!(z["dexMetrics"]["totalPools"], 0);
    let (_, long) = query(
        &st,
        "{ dexMetrics(startDate:\"2000-01-01T00:00:00Z\"){ totalPools } }",
    )
    .await;
    assert!(
        long["errors"][0]["message"]
            .as_str()
            .unwrap()
            .contains("at most 1100 days"),
        "{long}"
    );
    // Token prices: reserve ratio to coin (stock's path), USD at the rate.
    let t = data(&st, "{ tokenPrice(tokenAddress:\"kaddex.kdx\"){ id token{id name chainId address} priceInKda priceInUsd protocolAddress updatedAt} n: tokenPrice(tokenAddress:\"nope.nope\"){id} tokenPrices{ id priceInKda priceInUsd token{name address}}}").await;
    same_shape(&t, &s["tokenPrice"], "tokenPrice");
    assert_eq!(t["n"], Value::Null);
    let kda = t["tokenPrice"]["priceInKda"].as_f64().unwrap();
    assert!((kda - 12.9 / 779.0).abs() < 1e-15, "{kda}");
    let names: Vec<_> = t["tokenPrices"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["token"]["address"].clone())
        .collect();
    assert_eq!(
        names,
        [json!("coin"), json!("free.maga"), json!("kaddex.kdx")],
        "junk pairs' tokens left out"
    );
    // Liquidity positions: alice holds all 100 LP of both listable pools.
    let l = data(&st, "{ liquidityPositions(walletAddress:\"alice\"){ totalCount pageInfo{hasNextPage hasPreviousPage startCursor endCursor} edges{cursor node{id pairId liquidity walletAddress valueUsd apr24h createdAt updatedAt pair{id}}}}}").await;
    let lp = &l["liquidityPositions"];
    assert_eq!(lp["totalCount"], 2, "{lp}");
    assert_eq!(lp["edges"][0]["node"]["valueUsd"].as_f64(), Some(5.0));
    assert_eq!(lp["edges"][1]["node"]["valueUsd"].as_f64(), Some(0.129));
    let none = data(
        &st,
        "{ liquidityPositions(walletAddress:\"nobody\"){ totalCount edges{cursor} } }",
    )
    .await;
    assert_eq!(none["liquidityPositions"]["totalCount"], 0);
    // tokens: an empty Tokens table answers an empty page in stock's shape.
    let tk = data(&st, "{ tokens(first:3){ pageInfo{hasNextPage hasPreviousPage startCursor endCursor} edges{cursor node{id name chainId address}}}}").await;
    assert_eq!(tk["tokens"]["edges"], json!([]));
}

/// Aggregates stop at the request deadline (inside their loops) instead of running on.
#[tokio::test]
async fn long_ranges_stop_at_the_deadline() {
    let d = tempfile::tempdir().unwrap();
    let (hot, _st) = state(&d);
    fixture(&hot);
    let ro = hot.ro().unwrap();
    let past = Instant::now() - Duration::from_millis(1);
    let n = now();
    let r = dexserve::dex_metrics(
        &ro,
        n,
        Some(n - 1000 * DAY),
        Some(n),
        SUSHI,
        &mut Budget::new(past),
    );
    assert!(matches!(r, Err(ApiError::Deadline)), "{r:?}");
    let r = dexserve::pools(
        &ro,
        n,
        kdx_graphql::types::dex::PoolOrderBy::TvlUsdDesc,
        SUSHI,
        None,
        None,
        None,
        None,
        &mut Budget::new(past),
    );
    assert!(matches!(r, Err(ApiError::Deadline)));
    let ok = dexserve::dex_metrics(
        &ro,
        n,
        Some(n - 1000 * DAY),
        Some(n),
        SUSHI,
        &mut Budget::new(Instant::now() + Duration::from_secs(5)),
    );
    assert_eq!(ok.unwrap().total_pools, 2);
}

/// `dexMetrics` dates parse as stock's RFC 3339 `DateTime`; malformed ones fail validation.
#[tokio::test]
async fn dex_metrics_dates() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let mut w = hot.rw().unwrap();
    mark_filled(&mut w);
    w.commit().unwrap();
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
        assert_eq!(v["data"]["dexMetrics"]["totalPools"], 0, "{ok}: {v}");
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
        assert!(v["errors"].is_array(), "{bad}: {v}");
    }
}

/// I1: junk pairs (no coin leg, or a coin leg without reserves) whose names sort before
/// `coin`, and more listable spam pairs than the cap, leave every real pool served (ranked
/// by coin reserve, the listable filter runs before the cap); the cut is flagged
/// `extensions.dexPairsCapped`, not an error.
#[tokio::test]
async fn spam_pairs_never_crowd_out_real_pools() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let today = fixture(&hot);
    let mut w = W {
        txn: hot.rw().unwrap(),
        pos: 0,
    };
    for i in 0..200 {
        let (x, y) = (format!("a.x{i:03}"), format!("a.y{i:03}"));
        w.pair(&format!("{x}:{y}"), today - 10, "5", "5");
        let j = format!("a.j{i:03}:coin");
        w.ev(&j, Kind::CreatePair, today - 10, &[&j, "acct"]);
    }
    // Listable spam, newer than the real pools, past the cap.
    for i in 0..dexserve::MAX_PAIRS + 5 {
        w.pair(&format!("a.s{i:05}:coin"), today + 5, "1", "1");
    }
    w.txn.commit().unwrap();
    // Past the cap the largest pools are served and the response is flagged.
    let (_, full) = query(
        &st,
        "{ pools(first: 1000) { totalCount edges { node { key } } } }",
    )
    .await;
    assert!(full.get("errors").is_none(), "{}", full["errors"]);
    assert_eq!(full["extensions"]["dexPairsCapped"], true, "{full:.200}");
    let v = full["data"].clone();
    assert_eq!(
        v["pools"]["totalCount"],
        dexserve::MAX_PAIRS,
        "{}",
        v["pools"]["totalCount"]
    );
    let keys: Vec<&str> = v["pools"]["edges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["node"]["key"].as_str().unwrap())
        .collect();
    for real in ["coin:kaddex.kdx", "coin:free.maga"] {
        assert!(keys.contains(&real), "{real} missing");
    }
    let t = data(
        &st,
        "{ tokenPrice(tokenAddress: \"kaddex.kdx\") { priceInKda } }",
    )
    .await;
    assert_ne!(t["tokenPrice"]["priceInKda"], 0, "{t}");
    let (_, m) = query(&st, "{ dexMetrics { totalPools } }").await;
    assert_eq!(m["data"]["dexMetrics"]["totalPools"], dexserve::MAX_PAIRS);
    assert!(m.get("errors").is_none(), "{m}");
    assert_eq!(m["extensions"]["dexPairsCapped"], true);
    // Not capped: no flag.
    let (_, one) = query(&st, "{ pool(id: \"x\") { id } }").await;
    assert!(one["extensions"].get("dexPairsCapped").is_none(), "{one}");
}

/// 20,000 spam coin pairs (no reserves, or funded and drained) created before a new real
/// pool never count toward the cap: the new pool is served, with no cut.
#[tokio::test]
async fn unfunded_spam_never_uses_the_cap() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let today = fixture(&hot);
    let mut w = W {
        txn: hot.rw().unwrap(),
        pos: 0,
    };
    for i in 0..20_000 {
        let j = format!("a.j{i:05}:coin");
        w.ev(&j, Kind::CreatePair, today - 100, &[&j, "acct"]);
    }
    for i in 0..50 {
        let j = format!("a.d{i:02}:coin");
        w.pair(&j, today - 100, "1", "1");
        w.ev(&j, Kind::Update, today - 90, &["0", "0"]);
    }
    w.pair("coin:free.new", today + 30, "7", "70");
    w.txn.commit().unwrap();
    // The 20,000 unfunded pairs are never read: two rows per funded pair (4 fixture pairs,
    // 50 drained, the new one), five per kept pool.
    let ro = hot.ro().unwrap();
    let mut b = Budget::new(Instant::now() + Duration::from_secs(30));
    let got = dexserve::listable(&ro, &mut b).unwrap();
    assert_eq!(got.len(), 3);
    assert!(!b.capped);
    assert!(b.rows() <= 2 * 55 + 5 * 4, "{} rows", b.rows());
    drop(ro);
    let v = data(
        &st,
        "{ pools(first: 100) { totalCount edges { node { key } } } }",
    )
    .await;
    assert_eq!(v["pools"]["totalCount"], 3, "{v}");
    let keys: Vec<&str> = v["pools"]["edges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["node"]["key"].as_str().unwrap())
        .collect();
    assert!(keys.contains(&"coin:free.new"), "{keys:?}");
}

/// I1 residual: 1,500 dust-funded coin pairs created before three real pools (a few KDA of
/// gas each on chain) never hide them: ranked by coin reserve, the real pools come first,
/// no error, and the rows read stay bounded.
#[tokio::test]
async fn dust_pairs_rank_below_real_pools() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let mut w = W {
        txn: hot.rw().unwrap(),
        pos: 0,
    };
    let t0 = now() - 10 * DAY;
    for i in 0..1_500 {
        w.pair(&format!("a.dust{i:04}:coin"), t0, "1000000", "0.000001");
    }
    for (i, r) in ["40", "300", "2000"].iter().enumerate() {
        w.pair(&format!("coin:free.real{i}"), t0 + DAY, r, "1000");
    }
    for h in (t0 / 3600)..=(now() / 3600) {
        kdx_hot::dex_usd::put(&mut w.txn, h as u32, &Amount::parse("0.005").unwrap()).unwrap();
    }
    mark_filled(&mut w.txn);
    w.txn.commit().unwrap();
    let ro = hot.ro().unwrap();
    let mut b = Budget::new(Instant::now() + Duration::from_secs(30));
    let got = dexserve::listable(&ro, &mut b).unwrap();
    let keys: Vec<&str> = got.iter().map(|p| p.key.as_str()).collect();
    assert_eq!(
        &keys[..3],
        ["coin:free.real2", "coin:free.real1", "coin:free.real0"]
    );
    assert_eq!(got.len(), dexserve::MAX_PAIRS);
    assert!(b.capped);
    // Two rows per funded pair, five per pair read in full (the kept ones and one past).
    assert!(
        b.rows() <= 2 * 1_503 + 5 * (dexserve::MAX_PAIRS as u64 + 1),
        "{} rows",
        b.rows()
    );
    // Dust ties (equal coin reserve) break by pair key.
    assert_eq!(keys[3], "a.dust0000:coin");
    drop(ro);
    let (_, v) = query(
        &st,
        "{ pools(first: 3, orderBy: TVL_USD_DESC) { edges { node { key } } } }",
    )
    .await;
    assert!(v.get("errors").is_none(), "{v}");
    assert_eq!(v["extensions"]["dexPairsCapped"], true);
    assert_eq!(
        v["data"]["pools"]["edges"][0]["node"]["key"],
        "coin:free.real2"
    );
}

/// A price search ranks a token's pairs by the token's own reserve: 300 dust pairs created
/// before the token's one real route (more than the 256 expanded) do not hide it.
#[tokio::test]
async fn price_search_expands_the_largest_pairs() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let mut w = W {
        txn: hot.rw().unwrap(),
        pos: 0,
    };
    let t0 = now() - DAY;
    for i in 0..300 {
        w.pair(&format!("a.j{i:03}:free.x"), t0, "1", "0.000001");
    }
    w.pair("free.x:free.y", t0 + 60, "10", "10");
    w.pair("coin:free.y", t0 + 60, "5", "50");
    // Stock answers 0 without a KDA/USD rate.
    kdx_hot::dex_usd::put(
        &mut w.txn,
        (now() / 3600) as u32,
        &Amount::parse("0.005").unwrap(),
    )
    .unwrap();
    mark_filled(&mut w.txn);
    w.txn.commit().unwrap();
    let v = data(
        &st,
        "{ tokenPrice(tokenAddress: \"free.x\") { priceInKda } }",
    )
    .await;
    let p = v["tokenPrice"]["priceInKda"].to_string();
    assert!(p.trim_matches('"').starts_with("0.1"), "{v}");
}

/// Before the DEX history fill has finished every DEX field answers `NOT_AVAILABLE`.
#[tokio::test]
async fn dex_fields_wait_for_the_fill() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let mut w = W {
        txn: hot.rw().unwrap(),
        pos: 0,
    };
    w.pair("coin:free.early", now() - DAY, "5", "5");
    w.txn.commit().unwrap();
    for q in [
        "{ pools { totalCount } }",
        "{ dexMetrics { totalPools } }",
        "{ tokenPrice(tokenAddress: \"free.early\") { priceInKda } }",
    ] {
        let (_, v) = query(&st, q).await;
        assert_eq!(
            v["errors"][0]["extensions"]["code"], "NOT_AVAILABLE",
            "{q}: {v}"
        );
    }
    let mut w = hot.rw().unwrap();
    mark_filled(&mut w);
    w.commit().unwrap();
    let v = data(&st, "{ pools { totalCount } }").await;
    assert_eq!(v["pools"]["totalCount"], 1);
}

/// I2: a pair with a history larger than the row budget answers a one-day `dexMetrics`,
/// `pool`, its `poolTransactions` (count from the stored counts) and its charts.
#[tokio::test]
async fn large_history_reads_only_the_range() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    fixture(&hot);
    let n = now();
    let big = "coin:free.big";
    let rows = dexserve::MAX_ROWS + 50_000;
    {
        let mut w = W {
            txn: hot.rw().unwrap(),
            pos: 0,
        };
        let e = DexEvent {
            kind: Kind::CreatePair,
            pair: big.into(),
            fields: vec![big.into(), "acct".into()],
        };
        let t0 = n - 400 * DAY;
        hd::store_event(&mut w.txn, TxKey::new(10, 2, 1), 0, t0 * 1_000_000, t0, &e).unwrap();
        for i in 0..rows {
            let t = t0 + 60 + i * 30;
            let h = 11 + i as u32;
            for (kind, fields) in [
                (Kind::Update, vec!["100".to_string(), "5".to_string()]),
                (
                    Kind::Swap,
                    ["m", "m", "1", "0", "0", "1"].map(String::from).to_vec(),
                ),
            ] {
                let row = hd::StoredEvent {
                    kind,
                    time: t * 1_000_000,
                    tx_time: t,
                    fields,
                };
                w.txn
                    .put(
                        Table::DexEvents,
                        &hd::event_key(big, kind, TxKey::new(h, 2, 1), 0),
                        &row.encode(),
                    )
                    .unwrap();
            }
        }
        w.txn
            .put(
                Table::DexAgg,
                &hd::count_key(big, Kind::Swap),
                &(rows as i64).to_be_bytes(),
            )
            .unwrap();
        // Rows written directly: its funded mark (store_event keeps it) too.
        w.txn
            .put(
                Table::DexAgg,
                &hd::funded_key(big),
                &(rows as i64).to_be_bytes(),
            )
            .unwrap();
        w.txn.commit().unwrap();
    }
    let ro = hot.ro().unwrap();
    let b = || Budget::new(Instant::now() + Duration::from_secs(30));
    let t = Instant::now();
    let m = dexserve::dex_metrics(&ro, n, Some(n - DAY), Some(n), SUSHI, &mut b()).unwrap();
    assert_eq!(m.total_pools, 3);
    let id = format!("{SUSHI}-{big}");
    dexserve::pool(&ro, &id, n, Default::default(), &mut b()).unwrap();
    for tf in [
        kdx_graphql::types::dex::TimeFrame::Day,
        kdx_graphql::types::dex::TimeFrame::Week,
    ] {
        dexserve::charts(&ro, &id, tf, n, &mut b()).unwrap();
    }
    drop(ro);
    let v = data(
        &st,
        &format!("{{ poolTransactions(pairId: \"{id}\", first: 10) {{ totalCount edges {{ cursor }} }} }}"),
    )
    .await;
    assert_eq!(v["poolTransactions"]["totalCount"], rows, "{v}");
    assert_eq!(v["poolTransactions"]["edges"].as_array().unwrap().len(), 10);
    assert!(t.elapsed() < Duration::from_secs(10), "{:?}", t.elapsed());
}

/// `tokens` follows stock's set and order: every non-coin module with a holder row
/// (`Holders`, stock's `Balances` set) on a chain, newest first holder first; a chain whose
/// last holder row goes on unwind leaves the list. Before the list is built it answers
/// `NOT_AVAILABLE`.
#[tokio::test]
async fn tokens_follow_the_holder_rows() {
    use kdx_hot::{holders, token_chains};
    use kdx_types::{DictKind, DictMut};
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let mut w = hot.rw().unwrap();
    let mut dict = kdx_hot::dict::HotDict::load(&w).unwrap();
    let id = |dict: &mut kdx_hot::dict::HotDict, n: &str| dict.intern(DictKind::Module, n);
    let (coin, a, nft, gone) = (
        id(&mut dict, "coin"),
        id(&mut dict, "free.a"),
        id(&mut dict, "marmalade-v2.ledger"),
        id(&mut dict, "free.gone"),
    );
    dict.persist(&mut w).unwrap();
    holders::touch(&mut w, 1, coin, TxKey::new(5, 0, 0)).unwrap();
    holders::touch(&mut w, 1, a, TxKey::new(10, 3, 0)).unwrap();
    holders::touch(&mut w, 2, a, TxKey::new(14, 3, 0)).unwrap();
    holders::touch(&mut w, 2, a, TxKey::new(12, 1, 0)).unwrap();
    holders::touch(&mut w, 3, nft, TxKey::new(11, 1, 1)).unwrap();
    holders::touch(&mut w, 3, gone, TxKey::new(20, 2, 0)).unwrap();
    // The unwind of the only tx that named free.gone.
    holders::untouch_tx(&mut w, TxKey::new(20, 2, 0)).unwrap();
    mark_filled(&mut w);
    w.commit().unwrap();
    let q = "{ tokens(first: 10){ edges{cursor node{id name chainId}}}}";
    let v = data(&st, q).await;
    let got: Vec<(String, String)> = v["tokens"]["edges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["node"]["name"].as_str().unwrap().to_owned(),
                e["node"]["chainId"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    let want = [
        ("free.a", "1"),
        ("marmalade-v2.ledger", "1"),
        ("free.a", "3"),
    ];
    assert_eq!(got, want.map(|(n, c)| (n.to_owned(), c.to_owned())), "{v}");
    // Paging: after the first edge, the next two.
    let c = v["tokens"]["edges"][0]["cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    let p = data(
        &st,
        &format!("{{ tokens(first: 2, after: \"{c}\"){{ edges{{node{{name chainId}}}}}}}}"),
    )
    .await;
    assert_eq!(
        p["tokens"]["edges"][0]["node"]["name"],
        "marmalade-v2.ledger"
    );
    assert_eq!(p["tokens"]["edges"].as_array().unwrap().len(), 2);
    // Not built: NOT_AVAILABLE.
    let mut w = hot.rw().unwrap();
    w.del(Table::Meta, token_chains::READY.as_bytes()).unwrap();
    w.commit().unwrap();
    let (_, e) = query(&st, q).await;
    assert_eq!(e["errors"][0]["extensions"]["code"], "NOT_AVAILABLE", "{e}");
}

/// A failed DEX history fill answers its own message, not "being filled".
#[tokio::test]
async fn a_failed_fill_says_so() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let mut w = hot.rw().unwrap();
    kdx_hot::fills::set_failed(&mut w, kdx_hot::fills::DEX, "boom").unwrap();
    w.commit().unwrap();
    let (_, v) = query(&st, "{ pools { totalCount } }").await;
    assert_eq!(v["errors"][0]["extensions"]["code"], "NOT_AVAILABLE", "{v}");
    let msg = v["errors"][0]["message"].as_str().unwrap();
    assert!(
        msg.contains("failed") && !msg.contains("being filled"),
        "{msg}"
    );
    let mut w = hot.rw().unwrap();
    kdx_hot::fills::clear_failed(&mut w, kdx_hot::fills::DEX).unwrap();
    w.commit().unwrap();
    let (_, v) = query(&st, "{ pools { totalCount } }").await;
    assert!(
        v["errors"][0]["message"]
            .as_str()
            .unwrap()
            .contains("being filled"),
        "{v}"
    );
}

/// Item 4: a budget spent after the cap is reached serves the kept pairs, flagged capped.
#[tokio::test]
async fn listable_keeps_the_capped_list_when_the_budget_ends() {
    let d = tempfile::tempdir().unwrap();
    let (hot, _st) = state(&d);
    let mut w = W {
        txn: hot.rw().unwrap(),
        pos: 0,
    };
    let n = dexserve::MAX_PAIRS + 5;
    for i in 0..n {
        w.pair(&format!("a.s{i:05}:coin"), now() - DAY, "1", "1");
    }
    mark_filled(&mut w.txn);
    w.txn.commit().unwrap();
    let ro = hot.ro().unwrap();
    // Enough for the ranking and the kept pairs, not for one more pair's info.
    let rows = 2 * n as u64 + 5 * dexserve::MAX_PAIRS as u64 + 2;
    let mut b = Budget::with_rows(Instant::now() + Duration::from_secs(30), rows);
    let got = dexserve::listable(&ro, &mut b).unwrap();
    assert_eq!(got.len(), dexserve::MAX_PAIRS);
    assert!(b.capped);
}

/// Item 3: a price search that spends its budget answers no path (price 0, as stock with no
/// path), not a deadline error; the ranked neighbours are capped at 1,024.
#[tokio::test]
async fn a_spent_price_search_is_no_path() {
    assert_eq!(dexserve::MAX_RANKED_NEIGHBORS, 1_024);
    let d = tempfile::tempdir().unwrap();
    let (hot, _st) = state(&d);
    let mut w = W {
        txn: hot.rw().unwrap(),
        pos: 0,
    };
    let t0 = now() - DAY;
    for i in 0..300 {
        w.pair(&format!("a.j{i:03}:free.x"), t0, "1", "0.000001");
    }
    w.pair("free.x:free.y", t0 + 60, "10", "10");
    w.pair("coin:free.y", t0 + 60, "5", "50");
    kdx_hot::dex_usd::put(
        &mut w.txn,
        (now() / 3600) as u32,
        &Amount::parse("0.005").unwrap(),
    )
    .unwrap();
    mark_filled(&mut w.txn);
    w.txn.commit().unwrap();
    let ro = hot.ro().unwrap();
    let mut b = Budget::with_rows(Instant::now() + Duration::from_secs(30), 50);
    let p = dexserve::token_price(&ro, now(), "free.x", dexserve::DEFAULT_PROTOCOL, &mut b)
        .unwrap()
        .unwrap();
    assert!(
        format!("{:?}", p.price_in_kda).contains("text: \"0\""),
        "{p:?}"
    );
    // The 0 is flagged (`extensions.dexPricesPartial`), not silent.
    assert!(b.prices_partial);
    // A spent deadline is flagged the same way.
    let mut b = Budget::new(Instant::now());
    let p = dexserve::token_price(&ro, now(), "free.x", dexserve::DEFAULT_PROTOCOL, &mut b)
        .unwrap()
        .unwrap();
    assert!(
        format!("{:?}", p.price_in_kda).contains("text: \"0\""),
        "{p:?}"
    );
    assert!(b.prices_partial);
    // With room it finds the path, unflagged.
    let mut b = Budget::new(Instant::now() + Duration::from_secs(30));
    let p = dexserve::token_price(&ro, now(), "free.x", dexserve::DEFAULT_PROTOCOL, &mut b)
        .unwrap()
        .unwrap();
    assert!(
        !format!("{:?}", p.price_in_kda).contains("text: \"0\""),
        "{p:?}"
    );
    assert!(!b.prices_partial);
}
