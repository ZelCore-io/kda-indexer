//! Websocket subscriptions on `/graphql` (GQ-9, GQ-17, Task 6). Both subprotocols:
//! `graphql-transport-ws` and the legacy `graphql-ws`.
//!
//! Limits, each a security control on a public surface (every refusal counts in
//! `kdx_graphql_rejected_total{reason}`):
//! - connections: at most `ws_conns_per_ip` per client (keyed like the rate limiter) and at
//!   most `ws_subs_max` per process; an upgrade over either answers 429;
//! - subscriptions: at most `ws_subs_per_conn` per connection and `ws_subs_max` per process;
//!   a refused `subscribe` gets a `LIMIT` error, the connection stays;
//! - `connection_init` must arrive within `ws_init`, else close 4408;
//! - a connection with no subscription for `ws_idle` is closed (4000);
//! - a message (or frame) over [`MAX_MESSAGE`] closes the connection;
//! - every client message takes one rate token of the client's IP; out of tokens closes
//!   the connection (4429);
//! - a client that does not read for [`SEND_TIMEOUT`] is closed, and a subscriber that falls
//!   behind the feed's buffer gets an error (memory stays bounded).
//!
//! All slots are held by guards and released on close, error or task end.

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_graphql::http::{WebSocket as GqlWebSocket, WebSocketProtocols, WsMessage};
use async_graphql::{Context, Data};
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, FromRequestParts, Request as HttpRequest};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::{SinkExt, StreamExt};
use kdx_api::limits::{ConnGuard, ConnLimiter};
use kdx_api::ApiState;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{limit, server, Code};
use crate::{gql, Gql, KdxSchema};

/// Largest client message and frame.
pub const MAX_MESSAGE: usize = 64 * 1024;
/// Longest wait for the client to take one server message.
pub const SEND_TIMEOUT: Duration = Duration::from_secs(30);
/// Client messages buffered between the socket reader and the protocol handler.
const INBOX: usize = 16;

/// Process-wide websocket state (in [`Gql`]).
pub struct WsState {
    ips: ConnLimiter,
    conns: Arc<Semaphore>,
    subs: Arc<Semaphore>,
    max_subs: usize,
}

impl WsState {
    pub fn new(s: &crate::GraphqlSettings) -> WsState {
        WsState {
            ips: ConnLimiter::new(s.ws_conns_per_ip),
            conns: Arc::new(Semaphore::new(s.ws_subs_max)),
            subs: Arc::new(Semaphore::new(s.ws_subs_max)),
            max_subs: s.ws_subs_max,
        }
    }

    /// Open subscriptions in this process.
    pub fn active_subscriptions(&self) -> usize {
        self.max_subs - self.subs.available_permits()
    }
}

/// Open subscriptions of a schema (tests, checks).
pub fn active_subscriptions(schema: &KdxSchema) -> usize {
    schema
        .data::<Gql>()
        .map_or(0, |g| g.ws.active_subscriptions())
}

/// Per-connection subscription slots (connection data).
#[derive(Clone)]
pub struct ConnSubs(Arc<Semaphore>);

/// The slots one subscription holds; dropping it releases them and the gauge.
pub struct SubSlot {
    _conn: Option<OwnedSemaphorePermit>,
    _proc: OwnedSemaphorePermit,
}

impl Drop for SubSlot {
    fn drop(&mut self) {
        kdx_metrics::get().graphql_subscriptions.dec();
    }
}

/// Take a subscription slot for the operation being resolved.
pub fn acquire(ctx: &Context<'_>) -> async_graphql::Result<SubSlot> {
    let to_err = |e: async_graphql::ServerError| async_graphql::Error {
        message: e.message,
        source: None,
        extensions: e.extensions,
    };
    let conn = match ctx.data_opt::<ConnSubs>() {
        Some(c) => Some(c.0.clone().try_acquire_owned().map_err(|_| {
            to_err(limit(
                "ws_subscriptions_per_connection",
                "too many subscriptions on this connection",
            ))
        })?),
        None => None,
    };
    let proc_ = gql(ctx)
        .ws
        .subs
        .clone()
        .try_acquire_owned()
        .map_err(|_| to_err(limit("ws_subscriptions", "too many subscriptions")))?;
    kdx_metrics::get().graphql_subscriptions.inc();
    Ok(SubSlot {
        _conn: conn,
        _proc: proc_,
    })
}

/// True for a websocket upgrade request.
pub fn is_upgrade(headers: &HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}

fn refuse(status: StatusCode, code: Code, msg: &str) -> Response {
    let r = async_graphql::Response::from_errors(vec![server(code, msg)]);
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_vec(&r).unwrap_or_default(),
    )
        .into_response()
}

fn protocol_of(headers: &HeaderMap) -> Option<WebSocketProtocols> {
    let v = headers.get(header::SEC_WEBSOCKET_PROTOCOL)?.to_str().ok()?;
    v.split(',')
        .map(str::trim)
        .find_map(|p| p.parse::<WebSocketProtocols>().ok())
}

/// Answer a websocket upgrade on `/graphql`.
pub async fn upgrade(schema: Arc<KdxSchema>, st: Arc<ApiState>, req: HttpRequest) -> Response {
    let m = kdx_metrics::get();
    let Some(g) = schema.data::<Gql>() else {
        return refuse(
            StatusCode::BAD_REQUEST,
            Code::NotSupported,
            "no schema data",
        );
    };
    if g.feed.is_none() {
        return refuse(
            StatusCode::BAD_REQUEST,
            Code::NotSupported,
            "subscriptions are not served (no block feed)",
        );
    }
    let Some(protocol) = protocol_of(req.headers()) else {
        m.graphql_rejected("ws_protocol");
        return refuse(
            StatusCode::BAD_REQUEST,
            Code::BadUserInput,
            "Sec-WebSocket-Protocol must be graphql-transport-ws or graphql-ws",
        );
    };
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)), |c| c.0);
    let ip = st.client_ip_of(peer, req.headers());
    let Some(ip_slot) = g.ws.ips.try_acquire(ip) else {
        m.graphql_rejected("ws_connections_per_ip");
        return refuse(
            StatusCode::TOO_MANY_REQUESTS,
            Code::Limit,
            "too many websocket connections from this client",
        );
    };
    let Ok(conn_slot) = g.ws.conns.clone().try_acquire_owned() else {
        m.graphql_rejected("ws_connections");
        return refuse(
            StatusCode::TOO_MANY_REQUESTS,
            Code::Limit,
            "too many websocket connections",
        );
    };
    let (mut parts, _) = req.into_parts();
    let ws = match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
        Ok(ws) => ws,
        Err(e) => return e.into_response(),
    };
    ws.protocols(["graphql-transport-ws", "graphql-ws"])
        .max_message_size(MAX_MESSAGE)
        .max_frame_size(MAX_MESSAGE)
        .on_upgrade(move |socket| async move {
            drive(socket, schema, st, ip, protocol, (ip_slot, conn_slot)).await;
        })
}

async fn drive(
    socket: WebSocket,
    schema: Arc<KdxSchema>,
    st: Arc<ApiState>,
    ip: IpAddr,
    protocol: WebSocketProtocols,
    _slots: (ConnGuard, OwnedSemaphorePermit),
) {
    let settings = crate::gql_settings(&schema);
    let m = kdx_metrics::get();
    let per_conn = Arc::new(Semaphore::new(settings.ws_subs_per_conn));
    let inited = Arc::new(AtomicBool::new(false));
    let over_rate = Arc::new(AtomicBool::new(false));
    let (mut sink, mut source) = socket.split();
    let (in_tx, in_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(INBOX);

    // Reader: one rate token per client message; oversize and protocol errors end it.
    let rate = over_rate.clone();
    let reader = tokio::spawn(async move {
        while let Some(msg) = source.next().await {
            let Ok(msg) = msg else {
                kdx_metrics::get().graphql_rejected("ws_message");
                break;
            };
            if !st.allow_ip(ip, 1) {
                kdx_metrics::get().graphql_rejected("ws_rate");
                rate.store(true, Ordering::Release);
                break;
            }
            let bytes = match msg {
                Message::Text(t) => t.as_bytes().to_vec(),
                Message::Binary(b) => b.to_vec(),
                Message::Close(_) => break,
                Message::Ping(_) | Message::Pong(_) => continue,
            };
            if in_tx.send(bytes).await.is_err() {
                break;
            }
        }
    });

    let incoming =
        futures::stream::unfold(
            in_rx,
            |mut rx| async move { rx.recv().await.map(|m| (m, rx)) },
        );
    let mut data = Data::default();
    data.insert(ConnSubs(per_conn.clone()));
    // Per-client gates (search, S-5) key on it; operations over this socket carry it.
    data.insert(crate::ClientIp(ip));
    let init_flag = inited.clone();
    let gql_ws = GqlWebSocket::new((*schema).clone(), incoming, protocol)
        .connection_data(data)
        .on_connection_init(move |_payload| {
            init_flag.store(true, Ordering::Release);
            async { Ok(Data::default()) }
        });
    let mut gql_ws = std::pin::pin!(gql_ws);

    let started = Instant::now();
    let mut idle_since = Some(Instant::now());
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let close: Option<(u16, String)> = loop {
        tokio::select! {
            out = gql_ws.next() => match out {
                Some(WsMessage::Text(t)) => {
                    match tokio::time::timeout(SEND_TIMEOUT, sink.send(Message::Text(t.into()))).await {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => break None,
                        Err(_) => {
                            m.graphql_rejected("ws_slow_reader");
                            break Some((4008, "client is not reading".into()));
                        }
                    }
                }
                Some(WsMessage::Close(code, reason)) => break Some((code, reason)),
                None => {
                    break if over_rate.load(Ordering::Acquire) {
                        Some((4429, "rate limit".into()))
                    } else {
                        None
                    };
                }
            },
            _ = tick.tick() => {
                if !inited.load(Ordering::Acquire) && started.elapsed() > settings.ws_init {
                    m.graphql_rejected("ws_init_timeout");
                    break Some((4408, "Connection initialisation timeout".into()));
                }
                let active = settings.ws_subs_per_conn - per_conn.available_permits();
                if active > 0 {
                    idle_since = None;
                } else if idle_since.is_none() {
                    idle_since = Some(Instant::now());
                }
                if idle_since.is_some_and(|t| t.elapsed() > settings.ws_idle) {
                    m.graphql_rejected("ws_idle");
                    break Some((4000, "idle".into()));
                }
            }
        }
    };
    if let Some((code, reason)) = close {
        let frame = CloseFrame {
            code,
            reason: reason.into(),
        };
        let _ = tokio::time::timeout(
            Duration::from_secs(1),
            sink.send(Message::Close(Some(frame))),
        )
        .await;
    }
    reader.abort();
}
