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

/// `ours` has stock's shape wherever both hold a value (an empty list matches any list).
fn same_shape(ours: &Value, stock: &Value, at: &str) {
    match (ours, stock) {
        (Value::Object(a), Value::Object(b)) => {
            // As sets: JSON member order carries no meaning.
            let mut ka: Vec<_> = a.keys().collect();
            let mut kb: Vec<_> = b.keys().collect();
            ka.sort();
            kb.sort();
            assert_eq!(ka, kb, "{at}: keys");
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
    let (_hot, st) = state(&d);
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
