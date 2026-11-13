//! Websocket subscriptions (Task 6, GQ-9): delivery from the block feed, and every
//! websocket limit: subscriptions per connection, connections per client, the
//! `connection_init` timeout, the idle close, the message size cap and the per-message rate
//! tokens. Slots return to zero when a client goes away.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use kdx_api::compat::shapes::Ev;
use kdx_api::{Api, ApiSettings, ApiState, Store, TipWatch};
use kdx_graphql::feed::{FeedBlock, FeedItem};
use kdx_graphql::txlist::TxData;
use kdx_graphql::ws::active_subscriptions;
use kdx_graphql::{build_schema, router, BlockFeed, GraphqlSettings, KdxSchema};
use kdx_types::record::{BlockRecord, TxRecord};
use kdx_types::{Hash32, TxKey};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct Server {
    _dir: tempfile::TempDir,
    addr: SocketAddr,
    schema: KdxSchema,
    feed: BlockFeed,
}

async fn server(settings: GraphqlSettings, rate_burst: u32) -> Server {
    let d = tempfile::tempdir().unwrap();
    let g = kdx_hot::Geometry {
        max_gb: 1,
        growth_mb: 16,
    };
    let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap());
    let store = Arc::new(Store::open(hot, d.path()).unwrap());
    let api = ApiSettings {
        network: "mainnet01".into(),
        deadline: Duration::from_secs(5),
        max_in_flight: 16,
        max_body_bytes: 8 << 20,
        v1txs_max_body_bytes: 8 << 20,
        rate_per_ip: 1,
        rate_burst,
        // Loopback is the proxy: a test client names itself in `X-Forwarded-For`.
        trusted_proxies: vec![kdx_api::limits::IpNet::parse("127.0.0.1/32").unwrap()],
        read_threads: 2,
        ready_max_lag: 10,
        max_event_lists: 64,
    };
    let st: Arc<ApiState> = Api::new(store, api, TipWatch::default()).state();
    let feed = BlockFeed::detached();
    let schema = build_schema(st.clone(), settings, Some(feed.clone()));
    let app = router(schema.clone()).with_state(st);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    Server {
        _dir: d,
        addr,
        schema,
        feed,
    }
}

async fn connect(s: &Server, proto: &str) -> Result<Ws, u16> {
    connect_as(s, proto, None).await
}

/// Connect as client `xff` (sent in `X-Forwarded-For`; loopback is a trusted proxy).
async fn connect_as(s: &Server, proto: &str, xff: Option<&str>) -> Result<Ws, u16> {
    let mut req = format!("ws://{}/graphql", s.addr)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("sec-websocket-protocol", proto.parse().unwrap());
    if let Some(ip) = xff {
        req.headers_mut()
            .insert("x-forwarded-for", ip.parse().unwrap());
    }
    match tokio_tungstenite::connect_async(req).await {
        Ok((ws, _)) => Ok(ws),
        Err(tokio_tungstenite::tungstenite::Error::Http(r)) => Err(r.status().as_u16()),
        Err(e) => panic!("{e}"),
    }
}

async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string().into())).await.unwrap();
}

/// The next text message as JSON, or `Err(close code)` when the server closes.
async fn recv(ws: &mut Ws) -> Result<Value, u16> {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("no message within 5 s");
        match m {
            Some(Ok(Message::Text(t))) => return Ok(serde_json::from_str(&t).unwrap()),
            Some(Ok(Message::Close(f))) => return Err(f.map_or(1005, |f| u16::from(f.code))),
            Some(Ok(_)) => continue,
            Some(Err(_)) | None => return Err(1006),
        }
    }
}

async fn init(ws: &mut Ws) {
    send(ws, json!({"type": "connection_init", "payload": {}})).await;
    assert_eq!(recv(ws).await.unwrap()["type"], "connection_ack");
}

fn block(chain: u8, height: u64) -> FeedItem {
    let mut h = [0u8; 32];
    h[0] = chain;
    h[1..9].copy_from_slice(&height.to_be_bytes());
    let b = BlockRecord {
        hash: Hash32(h),
        parent: Hash32([0; 32]),
        adjacents: Vec::new(),
        chain,
        height,
        weight: [0; 32],
        target: [0xff; 32],
        nonce: 0,
        creation_time: 1_700_000_000_000_000,
        epoch_start: 1_700_000_000_000_000,
        feature_flags: 0,
        payload_hash: Hash32([0; 32]),
        transactions_hash: Hash32([0; 32]),
        outputs_hash: Hash32([0; 32]),
        chainweb_version: "mainnet01".into(),
        miner_account: 0,
        miner_data: Vec::new(),
        raw_miner: None,
        merkle_verified: true,
    };
    FeedItem::Block(Arc::new(FeedBlock {
        chain,
        height,
        hash: b.hash,
        block: Arc::new(b),
        txs: Arc::new(Vec::new()),
    }))
}

async fn wait_for(f: impl Fn() -> bool) -> bool {
    let until = Instant::now() + Duration::from_secs(3);
    while Instant::now() < until {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    f()
}

const NEW_BLOCKS: &str =
    "subscription { newBlocks(chainIds: [\"0\", \"1\"]) { chainId height hash } }";

#[tokio::test]
async fn delivery_and_subscription_and_connection_caps() {
    let s = server(
        GraphqlSettings {
            ws_subs_per_conn: 2,
            ws_conns_per_ip: 2,
            ..GraphqlSettings::default()
        },
        1000,
    )
    .await;
    let mut a = connect(&s, "graphql-transport-ws").await.unwrap();
    init(&mut a).await;
    for id in ["1", "2"] {
        send(
            &mut a,
            json!({"id": id, "type": "subscribe", "payload": {"query": NEW_BLOCKS}}),
        )
        .await;
    }
    // Streams start when first polled, in no fixed order: wait for both before the third.
    assert!(wait_for(|| active_subscriptions(&s.schema) == 2).await);
    send(
        &mut a,
        json!({"id": "3", "type": "subscribe", "payload": {"query": NEW_BLOCKS}}),
    )
    .await;
    // The third subscription on the connection is refused; the connection stays.
    // (A resolver error: `next` with the error, then `complete`.)
    let e = recv(&mut a).await.unwrap();
    assert_eq!(e["id"], "3", "{e}");
    assert_eq!(
        e["payload"]["errors"][0]["extensions"]["code"], "LIMIT",
        "{e}"
    );
    let e = recv(&mut a).await.unwrap();
    assert_eq!(
        (e["type"].as_str(), e["id"].as_str()),
        (Some("complete"), Some("3")),
        "{e}"
    );
    assert!(wait_for(|| active_subscriptions(&s.schema) == 2).await);
    // A block on a subscribed chain reaches both subscriptions once; another chain does not.
    s.feed.inject(block(5, 9));
    s.feed.inject(block(1, 7));
    let mut got = Vec::new();
    for _ in 0..2 {
        let m = recv(&mut a).await.unwrap();
        assert_eq!(m["type"], "next", "{m}");
        let b = &m["payload"]["data"]["newBlocks"][0];
        assert_eq!(
            (b["chainId"].as_i64(), b["height"].as_i64()),
            (Some(1), Some(7)),
            "{m}"
        );
        got.push(m["id"].as_str().unwrap().to_string());
    }
    got.sort();
    assert_eq!(got, ["1", "2"]);
    // Two connections per client: the third upgrade is refused with 429.
    let b = connect(&s, "graphql-transport-ws").await.unwrap();
    assert_eq!(connect(&s, "graphql-transport-ws").await.err(), Some(429));
    // An unknown subprotocol is refused.
    drop(b);
    // A client that goes away releases its subscriptions and its connection slot.
    drop(a);
    assert!(wait_for(|| active_subscriptions(&s.schema) == 0).await);
    let mut c = connect(&s, "graphql-transport-ws").await.unwrap();
    init(&mut c).await;
    // Completing a subscription releases its slot too.
    send(
        &mut c,
        json!({"id": "9", "type": "subscribe", "payload": {"query": NEW_BLOCKS}}),
    )
    .await;
    assert!(wait_for(|| active_subscriptions(&s.schema) == 1).await);
    send(&mut c, json!({"id": "9", "type": "complete"})).await;
    assert!(wait_for(|| active_subscriptions(&s.schema) == 0).await);
}

#[tokio::test]
async fn init_timeout_idle_close_and_protocols() {
    let s = server(
        GraphqlSettings {
            ws_init: Duration::from_millis(200),
            ws_idle: Duration::from_millis(400),
            ..GraphqlSettings::default()
        },
        1000,
    )
    .await;
    // No connection_init: closed 4408.
    let mut a = connect(&s, "graphql-transport-ws").await.unwrap();
    assert_eq!(recv(&mut a).await, Err(4408));
    // Initialised but no subscription: closed as idle (4000).
    let mut b = connect(&s, "graphql-transport-ws").await.unwrap();
    init(&mut b).await;
    assert_eq!(recv(&mut b).await, Err(4000));
    // The legacy subprotocol works; a missing or unknown one is refused.
    let mut c = connect(&s, "graphql-ws").await.unwrap();
    send(&mut c, json!({"type": "connection_init", "payload": {}})).await;
    assert_eq!(recv(&mut c).await.unwrap()["type"], "connection_ack");
    assert_eq!(connect(&s, "chat").await.err(), Some(400));
}

#[tokio::test]
async fn message_cap_and_rate_tokens() {
    let s = server(GraphqlSettings::default(), 1000).await;
    // A message over 64 KB closes the connection.
    let mut a = connect(&s, "graphql-transport-ws").await.unwrap();
    init(&mut a).await;
    let big = "x".repeat(70 * 1024);
    let _ = a.send(Message::Text(big.into())).await;
    // L-3: with a close frame (1009), not a dropped connection (1006).
    assert_eq!(recv(&mut a).await.err(), Some(1009));
    // Each message takes a rate token: out of tokens closes 4429.
    let s = server(GraphqlSettings::default(), 10).await;
    let mut b = connect(&s, "graphql-transport-ws").await.unwrap();
    init(&mut b).await;
    let mut closed = None;
    for _ in 0..40 {
        if b.send(Message::Text(json!({"type": "ping"}).to_string().into()))
            .await
            .is_err()
        {
            break;
        }
    }
    for _ in 0..50 {
        match recv(&mut b).await {
            Ok(_) => continue,
            Err(c) => {
                closed = Some(c);
                break;
            }
        }
    }
    assert_eq!(closed, Some(4429));
}

/// I-4: a gap inside a batch is reported after the blocks before it, and the blocks queued
/// behind it still arrive.
#[tokio::test]
async fn gap_inside_a_batch_loses_nothing() {
    let s = server(GraphqlSettings::default(), 1000).await;
    let mut a = connect(&s, "graphql-transport-ws").await.unwrap();
    init(&mut a).await;
    send(
        &mut a,
        json!({"id": "1", "type": "subscribe", "payload": {"query": NEW_BLOCKS}}),
    )
    .await;
    assert!(wait_for(|| active_subscriptions(&s.schema) == 1).await);
    s.feed.inject(block(0, 1));
    s.feed.inject(FeedItem::Gap(0));
    s.feed.inject(block(1, 2));
    let mut got = Vec::new();
    for _ in 0..3 {
        let m = recv(&mut a).await.unwrap();
        assert_eq!(m["type"], "next", "{m}");
        let p = &m["payload"];
        got.push(match p["data"]["newBlocks"].as_array() {
            Some(bs) => bs
                .iter()
                .map(|b| format!("{}/{}", b["chainId"], b["height"]))
                .collect::<Vec<_>>()
                .join(","),
            None => p["errors"][0]["extensions"]["code"].to_string(),
        });
    }
    assert_eq!(got, ["0/1", "\"INTERNAL\"", "1/2"]);
}

/// Block `(chain, height)` with one tx of `n` `coin.TRANSFER` events.
fn block_with_events(chain: u8, height: u64, n: usize) -> FeedItem {
    let FeedItem::Block(b) = block(chain, height) else {
        unreachable!()
    };
    let evs = (0..n)
        .map(|i| Ev {
            pos: i,
            qual_name: "coin.TRANSFER".into(),
            module: "coin".into(),
            module_hash: String::new(),
            params: json!(["a", "b", 1.0]),
        })
        .collect();
    let tx = TxData {
        rec: TxRecord {
            key: TxKey::new(height as u32, chain, 1),
            ..TxRecord::default()
        },
        block: b.block.clone(),
        out: Value::Null,
        cmd: None,
        evs,
        trs: Vec::new(),
        nfts: Vec::new(),
    };
    FeedItem::Block(Arc::new(FeedBlock {
        chain,
        height,
        hash: b.hash,
        block: b.block.clone(),
        txs: Arc::new(vec![Arc::new(tx)]),
    }))
}

/// The list sizes of the next `n` `next` messages, per subscription id.
async fn sizes(ws: &mut Ws, n: usize, field: &str) -> BTreeMap<String, Vec<Vec<i64>>> {
    let mut got: BTreeMap<String, Vec<Vec<i64>>> = BTreeMap::new();
    for _ in 0..n {
        let m = recv(ws).await.unwrap();
        assert_eq!(m["type"], "next", "{m}");
        let items = m["payload"]["data"][field].as_array().expect("a list");
        let hs = items
            .iter()
            .map(|b| b["height"].as_i64().unwrap_or(0))
            .collect();
        got.entry(m["id"].as_str().unwrap().into())
            .or_default()
            .push(hs);
    }
    got
}

/// I-1, M3: a message carries at most its priced blocks: one per subscribed chain without
/// `quantity`, and the newest `quantity` (clamped to `max_page`) with it.
#[tokio::test]
async fn messages_carry_at_most_their_priced_blocks() {
    let settings = GraphqlSettings {
        max_page: 2,
        ..GraphqlSettings::default()
    };
    let s = server(settings, 1000).await;
    let mut a = connect(&s, "graphql-transport-ws").await.unwrap();
    init(&mut a).await;
    for (id, q) in [
        (
            "1",
            "subscription { newBlocks(chainIds: [\"0\"]) { height } }",
        ),
        (
            "2",
            "subscription { newBlocks(chainIds: [\"0\"], quantity: 3) { height } }",
        ),
    ] {
        send(
            &mut a,
            json!({"id": id, "type": "subscribe", "payload": {"query": q}}),
        )
        .await;
    }
    assert!(wait_for(|| active_subscriptions(&s.schema) == 2).await);
    for h in 1..=3 {
        s.feed.inject(block(0, h));
    }
    let got = sizes(&mut a, 4, "newBlocks").await;
    assert_eq!(got["1"], [vec![1], vec![2], vec![3]]);
    assert_eq!(got["2"], [vec![2, 3]]);
}

/// I-1: an `events` message carries at most the default page of events; the rest follow.
#[tokio::test]
async fn events_messages_carry_at_most_the_priced_events() {
    let s = server(GraphqlSettings::default(), 1000).await;
    let mut a = connect(&s, "graphql-transport-ws").await.unwrap();
    init(&mut a).await;
    let q =
        "subscription { events(qualifiedEventName: \"coin.TRANSFER\", chainId: \"0\") { height } }";
    send(
        &mut a,
        json!({"id": "1", "type": "subscribe", "payload": {"query": q}}),
    )
    .await;
    assert!(wait_for(|| active_subscriptions(&s.schema) == 1).await);
    s.feed.inject(block_with_events(0, 7, 45));
    let got = sizes(&mut a, 3, "events").await;
    let lens: Vec<usize> = got["1"].iter().map(Vec::len).collect();
    assert_eq!(lens, [20, 20, 5]);
}

/// I-1: a subscriber that falls more than the feed's buffer behind gets one gap error and
/// stays open from the feed's tail.
#[tokio::test]
async fn lagging_subscriber_gets_a_gap_and_stays_open() {
    let s = server(GraphqlSettings::default(), 1000).await;
    let mut a = connect(&s, "graphql-transport-ws").await.unwrap();
    init(&mut a).await;
    send(
        &mut a,
        json!({"id": "1", "type": "subscribe", "payload": {"query": NEW_BLOCKS}}),
    )
    .await;
    assert!(wait_for(|| active_subscriptions(&s.schema) == 1).await);
    for h in 0..1100 {
        s.feed.inject(block(5, h));
    }
    let m = recv(&mut a).await.unwrap();
    assert_eq!(
        (
            m["type"].as_str(),
            m["payload"]["errors"][0]["extensions"]["code"].as_str()
        ),
        (Some("next"), Some("INTERNAL")),
        "{m}"
    );
    s.feed.inject(block(0, 1));
    let got = sizes(&mut a, 1, "newBlocks").await;
    assert_eq!(got["1"], [vec![1]]);
}

/// I-2: a query over the websocket is refused before it runs (only subscriptions there).
#[tokio::test]
async fn queries_over_the_socket_are_refused() {
    let s = server(GraphqlSettings::default(), 1000).await;
    let mut a = connect(&s, "graphql-transport-ws").await.unwrap();
    init(&mut a).await;
    let q = "{ graphConfiguration { version } }";
    send(
        &mut a,
        json!({"id": "1", "type": "subscribe", "payload": {"query": q}}),
    )
    .await;
    let m = recv(&mut a).await.unwrap();
    assert!(m["payload"]["data"].is_null(), "{m}");
    let errors = if m["type"] == "error" {
        &m["payload"]
    } else {
        &m["payload"]["errors"]
    };
    assert_eq!(errors[0]["extensions"]["code"], "NOT_SUPPORTED", "{m}");
}

#[tokio::test]
async fn fragment_bomb_over_the_socket_is_refused_fast() {
    let s = server(GraphqlSettings::default(), 1000).await;
    let mut a = connect(&s, "graphql-transport-ws").await.unwrap();
    init(&mut a).await;
    for (id, n) in [(1, 24), (2, 40), (3, 60)] {
        let mut q = String::from("fragment F0 on Block { hash } ");
        for i in 1..=n {
            q += &format!("fragment F{i} on Block {{ ...F{p} ...F{p} }} ", p = i - 1);
        }
        q += &format!("subscription {{ newBlocks {{ ...F{n} }} }}");
        let t = Instant::now();
        send(
            &mut a,
            json!({"id": id.to_string(), "type": "subscribe", "payload": {"query": q}}),
        )
        .await;
        let m = recv(&mut a).await.unwrap();
        assert!(
            t.elapsed() < Duration::from_secs(1),
            "n={n}: {:?}",
            t.elapsed()
        );
        assert!(m["payload"]["data"].is_null(), "{m}");
        let errors = if m["type"] == "error" {
            &m["payload"]
        } else {
            &m["payload"]["errors"]
        };
        assert_eq!(errors[0]["extensions"]["code"], "LIMIT", "{m}");
        assert_eq!(errors[0]["extensions"]["limit"], "complexity", "{m}");
        if m["type"] == "next" {
            assert_eq!(recv(&mut a).await.unwrap()["type"], "complete");
        }
    }
    // The connection stays usable.
    send(
        &mut a,
        json!({"id": "4", "type": "subscribe", "payload": {"query": "subscription { newBlocks { hash } }"}}),
    )
    .await;
    assert!(wait_for(|| active_subscriptions(&s.schema) == 1).await);
}

/// Subscribe `ids` on `ws` (`newBlocks`, which waits for blocks and sends nothing).
async fn subscribe_all(ws: &mut Ws, ids: std::ops::Range<u32>) {
    for id in ids {
        send(
            ws,
            json!({"id": id.to_string(), "type": "subscribe", "payload": {"query": NEW_BLOCKS}}),
        )
        .await;
    }
}

/// The next message is a `LIMIT` refusal of subscription `id`, for `reason`.
async fn refused(ws: &mut Ws, id: &str, reason: &str) {
    let e = recv(ws).await.unwrap();
    assert_eq!(e["id"], id, "{e}");
    let x = &e["payload"]["errors"][0]["extensions"];
    assert_eq!(
        (x["code"].as_str(), x["limit"].as_str()),
        (Some("LIMIT"), Some(reason)),
        "{e}"
    );
}

/// M-2 with the default caps: one client holds at most `ws_subs_per_ip` (40) subscriptions
/// over all its connections; the /64s of one IPv6 /48 share `ws_conns_per_48` (30)
/// connections and `ws_subs_per_48` (120) subscriptions; other clients are still served.
#[tokio::test]
async fn per_client_and_per_48_caps() {
    let s = server(GraphqlSettings::default(), 1000).await;
    let proto = "graphql-transport-ws";
    // IPv4 client A: 20 + 20 subscriptions on two connections, the 41st on a third is
    // refused.
    let mut a = Vec::new();
    for i in 0..3u32 {
        let mut w = connect_as(&s, proto, Some("192.0.2.1")).await.unwrap();
        init(&mut w).await;
        if i < 2 {
            subscribe_all(&mut w, i * 20..i * 20 + 20).await;
        }
        a.push(w);
    }
    assert!(wait_for(|| active_subscriptions(&s.schema) == 40).await);
    subscribe_all(&mut a[2], 40..41).await;
    refused(&mut a[2], "40", "ws_subscriptions_per_client").await;
    // A fresh client still subscribes.
    let mut b = connect_as(&s, proto, Some("192.0.2.2")).await.unwrap();
    init(&mut b).await;
    subscribe_all(&mut b, 0..1).await;
    assert!(wait_for(|| active_subscriptions(&s.schema) == 41).await);
    drop((a, b));
    assert!(wait_for(|| active_subscriptions(&s.schema) == 0).await);

    // One /48: three /64s hold 40 subscriptions each on 2 connections; a fourth /64's
    // first subscription is refused at the /48's 120.
    let mut v6 = Vec::new();
    for net in 1..=3u32 {
        for c in 0..2u32 {
            let ip = format!("2001:db8:1:{net}::1");
            let mut w = connect_as(&s, proto, Some(&ip)).await.unwrap();
            init(&mut w).await;
            subscribe_all(&mut w, c * 20..c * 20 + 20).await;
            v6.push(w);
        }
    }
    assert!(wait_for(|| active_subscriptions(&s.schema) == 120).await);
    let mut d = connect_as(&s, proto, Some("2001:db8:1:4::1"))
        .await
        .unwrap();
    init(&mut d).await;
    subscribe_all(&mut d, 0..1).await;
    refused(&mut d, "0", "ws_subscriptions_per_client").await;
    // Connections: the /48 holds 7; 23 more from fresh /64s fill its 30, the next is 429.
    for net in 5..28u32 {
        let ip = format!("2001:db8:1:{net}::1");
        v6.push(connect_as(&s, proto, Some(&ip)).await.unwrap());
    }
    assert_eq!(
        connect_as(&s, proto, Some("2001:db8:1:99::1")).await.err(),
        Some(429)
    );
    // Another /48 and an IPv4 client are not affected.
    let mut e = connect_as(&s, proto, Some("2001:db8:2::1")).await.unwrap();
    init(&mut e).await;
    subscribe_all(&mut e, 0..1).await;
    assert!(wait_for(|| active_subscriptions(&s.schema) == 121).await);
    assert!(connect_as(&s, proto, Some("192.0.2.3")).await.is_ok());
}

/// `connection_init` and `n` subscribes sent back to back: one burst, no wait for the ack.
async fn burst(ws: &mut Ws, n: u32) {
    send(ws, json!({"type": "connection_init", "payload": {}})).await;
    subscribe_all(ws, 0..n).await;
}

/// True when nothing (no message, no close) arrives on `ws` for `ms`.
async fn quiet(ws: &mut Ws, ms: u64) -> bool {
    tokio::time::timeout(Duration::from_millis(ms), ws.next())
        .await
        .is_err()
}

/// R-P5P9-57, the CG soak shape with the production burst (40): one client opens two
/// connections and fills each to `ws_subs_per_conn` (20) in one burst, 42 messages in all.
/// Both stay open with all 40 subscriptions (before the fix one was closed 4429).
#[tokio::test]
async fn two_opening_bursts_fill_the_client_cap_without_a_close() {
    let s = server(GraphqlSettings::default(), 40).await;
    let proto = "graphql-transport-ws";
    let mut a = connect_as(&s, proto, Some("192.0.2.10")).await.unwrap();
    let mut b = connect_as(&s, proto, Some("192.0.2.10")).await.unwrap();
    burst(&mut a, 20).await;
    burst(&mut b, 20).await;
    for w in [&mut a, &mut b] {
        assert_eq!(recv(w).await.unwrap()["type"], "connection_ack");
    }
    assert!(wait_for(|| active_subscriptions(&s.schema) == 40).await);
    assert!(quiet(&mut a, 300).await && quiet(&mut b, 300).await);
    assert_eq!(active_subscriptions(&s.schema), 40);
}

/// R-P5P9-57 on a bucket another connection of the client just drained: a burst of
/// `connection_init` and exactly `ws_subs_per_conn` subscribes is accepted; subscribe
/// `cap + 1` is refused by the cap (LIMIT `ws_subscriptions_per_connection`) and the
/// connection stays and delivers; a flood after the opening burst is still closed 4429.
#[tokio::test]
async fn opening_burst_on_a_drained_bucket_then_cap_then_flood() {
    let s = server(GraphqlSettings::default(), 40).await;
    let proto = "graphql-transport-ws";
    let ip = Some("192.0.2.20");
    // The client's other connection floods and is closed: its bucket is empty.
    let mut d = connect_as(&s, proto, ip).await.unwrap();
    init(&mut d).await;
    for _ in 0..60 {
        if d.send(Message::Text(json!({"type": "ping"}).to_string().into()))
            .await
            .is_err()
        {
            break;
        }
    }
    let mut closed = None;
    for _ in 0..70 {
        if let Err(c) = recv(&mut d).await {
            closed = Some(c);
            break;
        }
    }
    assert_eq!(
        closed,
        Some(4429),
        "the drain connection is closed for rate"
    );
    // A fresh connection of the same client fills the per-connection cap in one burst.
    let mut c = connect_as(&s, proto, ip).await.unwrap();
    burst(&mut c, 20).await;
    assert_eq!(recv(&mut c).await.unwrap()["type"], "connection_ack");
    assert!(wait_for(|| active_subscriptions(&s.schema) == 20).await);
    // cap + 1: refused by the cap, not by a close.
    subscribe_all(&mut c, 20..21).await;
    refused(&mut c, "20", "ws_subscriptions_per_connection").await;
    let e = recv(&mut c).await.unwrap();
    assert_eq!(
        (e["type"].as_str(), e["id"].as_str()),
        (Some("complete"), Some("20")),
        "{e}"
    );
    assert!(quiet(&mut c, 300).await);
    assert_eq!(active_subscriptions(&s.schema), 20);
    // The connection is open and delivers: a block reaches all 20 subscriptions.
    s.feed.inject(block(0, 5));
    let mut ids = Vec::new();
    for _ in 0..20 {
        let m = recv(&mut c).await.unwrap();
        assert_eq!(m["type"], "next", "{m}");
        ids.push(m["id"].as_str().unwrap().parse::<u32>().unwrap());
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..20).collect::<Vec<_>>());
    // Past the opening burst nothing is borrowed: a flood is closed.
    for _ in 0..60 {
        if c.send(Message::Text(json!({"type": "ping"}).to_string().into()))
            .await
            .is_err()
        {
            break;
        }
    }
    let mut closed = None;
    for _ in 0..70 {
        if let Err(code) = recv(&mut c).await {
            closed = Some(code);
            break;
        }
    }
    assert_eq!(closed, Some(4429));
    assert!(wait_for(|| active_subscriptions(&s.schema) == 0).await);
}

/// R-P5P9-57: a fresh client's flood on one connection is closed 4429 with the production
/// burst, as is a flood spread over the client's connections (the borrow is one floor per
/// client, not a grant per connection).
#[tokio::test]
async fn floods_are_still_closed_with_the_production_burst() {
    let s = server(GraphqlSettings::default(), 40).await;
    let proto = "graphql-transport-ws";
    let flood = |n: usize| {
        (0..n)
            .map(|_| Message::Text(json!({"type": "ping"}).to_string().into()))
            .collect::<Vec<_>>()
    };
    let mut a = connect_as(&s, proto, Some("192.0.2.30")).await.unwrap();
    init(&mut a).await;
    for m in flood(120) {
        if a.send(m).await.is_err() {
            break;
        }
    }
    let mut closed = None;
    for _ in 0..130 {
        if let Err(code) = recv(&mut a).await {
            closed = Some(code);
            break;
        }
    }
    assert_eq!(closed, Some(4429), "one-connection flood");
    // Ten connections of one client, 10 messages each (100 > 40 + 22): every message is
    // inside its connection's opening burst, and still the client is cut off for rate.
    let mut conns = Vec::new();
    for _ in 0..10 {
        conns.push(connect_as(&s, proto, Some("192.0.2.31")).await.unwrap());
    }
    for w in &mut conns {
        send(w, json!({"type": "connection_init", "payload": {}})).await;
        for m in flood(9) {
            let _ = w.send(m).await;
        }
    }
    let mut rate_closed = 0;
    for w in &mut conns {
        loop {
            match tokio::time::timeout(Duration::from_millis(500), w.next()).await {
                Ok(Some(Ok(Message::Close(Some(f))))) if u16::from(f.code) == 4429 => {
                    rate_closed += 1;
                    break;
                }
                Ok(Some(Ok(Message::Close(_)) | Err(_))) | Ok(None) | Err(_) => break,
                Ok(Some(Ok(_))) => continue,
            }
        }
    }
    assert!(
        rate_closed >= 3,
        "only {rate_closed} of 10 connections closed for rate"
    );
}

/// A flood on one connection from a full bucket is closed at exactly message `rate_burst + 1`:
/// the opening borrow only matters once the bucket is drained, and `ws_subs_per_conn + 2` is
/// bounded by `rate_burst` (config validation), so it never moves the cut-off. The server
/// answers every accepted message (`connection_init` with an ack, a ping with a pong), so the
/// replies counted before the 4429 close are the messages that took a token. Refill is 1/s and
/// the burst takes milliseconds.
#[tokio::test]
async fn flood_from_a_full_bucket_is_closed_at_message_rate_burst_plus_one() {
    const BURST: usize = 40;
    let s = server(GraphqlSettings::default(), BURST as u32).await;
    let mut w = connect_as(&s, "graphql-transport-ws", Some("192.0.2.40"))
        .await
        .unwrap();
    send(&mut w, json!({"type": "connection_init", "payload": {}})).await;
    for _ in 0..(BURST + 20) {
        let ping = Message::Text(json!({"type": "ping"}).to_string().into());
        if w.send(ping).await.is_err() {
            break;
        }
    }
    let mut accepted = 0;
    let code = loop {
        match recv(&mut w).await {
            Ok(_) => accepted += 1,
            Err(code) => break code,
        }
    };
    assert_eq!(code, 4429);
    // `rate_burst` messages (the init and 39 pings) are answered; message 41 is refused.
    assert_eq!(accepted, BURST, "messages answered before the close");
}
