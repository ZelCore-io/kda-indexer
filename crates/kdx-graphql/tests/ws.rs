//! Websocket subscriptions (Task 6, GQ-9): delivery from the block feed, and every
//! websocket limit: subscriptions per connection, connections per client, the
//! `connection_init` timeout, the idle close, the message size cap and the per-message rate
//! tokens. Slots return to zero when a client goes away.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use kdx_api::{Api, ApiSettings, ApiState, Store, TipWatch};
use kdx_graphql::feed::{FeedBlock, FeedItem};
use kdx_graphql::ws::active_subscriptions;
use kdx_graphql::{build_schema, router, BlockFeed, GraphqlSettings, KdxSchema};
use kdx_types::record::BlockRecord;
use kdx_types::Hash32;
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
        trusted_proxies: Vec::new(),
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
    let mut req = format!("ws://{}/graphql", s.addr)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("sec-websocket-protocol", proto.parse().unwrap());
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
        reorg: false,
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
    assert!(recv(&mut a).await.is_err());
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
