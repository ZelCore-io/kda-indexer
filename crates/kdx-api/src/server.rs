//! HTTP server: native, compat and admin listeners over one `ApiState`, with the limits of
//! spec §5.4 (in-flight cap 503, per-IP rate 429, deadline 504, response body cap 503).
//! Store reads run on blocking threads behind a `read_threads` semaphore, each in its own
//! `View` (one read txn per request). Access logs are off; client IPs are never logged.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Context;
use arc_swap::ArcSwapOption;
use axum::body::{Body, HttpBody};
use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::http::{header, HeaderName, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::Router;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};

use crate::limits::{bucket_key, client_ip, is_internal, IpNet, RateLimiter};
use crate::store::{Store, View};
use crate::{admin, status, ApiError, ApiResult};

#[derive(Debug, Clone)]
pub struct ApiSettings {
    pub network: String,
    pub deadline: Duration,
    pub max_in_flight: usize,
    pub max_body_bytes: usize,
    pub v1txs_max_body_bytes: usize,
    pub rate_per_ip: u32,
    pub rate_burst: u32,
    pub trusted_proxies: Vec<IpNet>,
    pub read_threads: usize,
    pub ready_max_lag: u32,
    pub max_event_lists: usize,
}

/// Listen addresses; `None` = not bound.
#[derive(Debug, Clone, Copy, Default)]
pub struct Binds {
    pub native: Option<SocketAddr>,
    pub compat: Option<SocketAddr>,
    /// GraphQL (P7; spec §5.3).
    pub graphql: Option<SocketAddr>,
    pub admin: Option<SocketAddr>,
}

/// Node cut tips per chain (fed by a `/cut` poll) and whether a backfill is running.
#[derive(Clone, Default)]
pub struct TipWatch(Arc<TipInner>);

#[derive(Default)]
struct TipInner {
    tips: ArcSwapOption<NodeTips>,
    backfill: AtomicBool,
}

#[derive(Debug, Clone)]
pub struct NodeTips {
    pub heights: BTreeMap<u8, u64>,
    pub at: SystemTime,
}

/// Node tips older than this count as unknown.
pub const TIPS_STALE: Duration = Duration::from_secs(60);

/// How long `/readyz` keeps judging lag against the last node tips once they go stale
/// (a node restart must not take the indexer out of the pool, R10).
pub const TIPS_READY_GRACE: Duration = Duration::from_secs(600);

impl TipWatch {
    pub fn set(&self, heights: BTreeMap<u8, u64>) {
        self.0.tips.store(Some(Arc::new(NodeTips {
            heights,
            at: SystemTime::now(),
        })));
    }
    /// Fresh tips, or None when unknown or stale.
    pub fn get(&self) -> Option<Arc<NodeTips>> {
        self.0
            .tips
            .load_full()
            .filter(|t| t.at.elapsed().is_ok_and(|e| e < TIPS_STALE))
    }
    /// The last tips seen, with their age, whether stale or not.
    pub fn last(&self) -> Option<(Arc<NodeTips>, Duration)> {
        let t = self.0.tips.load_full()?;
        let age = t.at.elapsed().unwrap_or_default();
        Some((t, age))
    }
    pub fn set_backfill(&self, on: bool) {
        self.0.backfill.store(on, Ordering::Release);
    }
    pub fn backfill(&self) -> bool {
        self.0.backfill.load(Ordering::Acquire)
    }
}

/// Shared by every handler (`State<Arc<ApiState>>`).
pub struct ApiState {
    pub store: Arc<Store>,
    pub settings: ApiSettings,
    pub tips: TipWatch,
    reads: Arc<Semaphore>,
    /// Status and readiness reads (`/readyz`, `/status`, the sampler): their own slots, so
    /// a busy pool never fails health checks.
    status_reads: Arc<Semaphore>,
    /// Expensive request classes (events scan, `/v1/txs`) take one of these before a read
    /// slot: at most half the pool, so they cannot hold every slot.
    heavy: Arc<Semaphore>,
    /// Heavy reads per client key (H-1): one runs, [`HEAVY_WAITING_PER_CLIENT`] wait.
    heavy_clients: Arc<HeavyClients>,
    in_flight: Arc<Semaphore>,
    limiter: RateLimiter,
    /// `[balances] key_modules` (M-1, SEC-2): the modules `GET /keys/{pubkey}/accounts`
    /// lists without `token`; unset means `["coin"]`.
    key_modules: std::sync::OnceLock<Vec<String>>,
}

tokio::task_local! {
    /// The client key ([`bucket_key`]) of the request being served, set by the data
    /// listeners' guard (H-1).
    static CLIENT: std::net::IpAddr;
}

/// Run `f` as a request of client `ip`: heavy reads inside it count against that client's
/// share (H-1). The data listeners' guard does this for every request.
pub async fn as_client<F: std::future::Future>(ip: std::net::IpAddr, f: F) -> F::Output {
    CLIENT.scope(bucket_key(ip), f).await
}

/// Heavy reads one client key may have waiting behind its running one (H-1); past that a
/// heavy read is refused at once (503, GraphQL `LIMIT`). A client never holds more than one
/// heavy slot, so with two or more slots one client cannot stall another's lists.
pub const HEAVY_WAITING_PER_CLIENT: usize = 3;

/// Heavy reads in flight per client key: the client's one-slot semaphore and its users
/// (running and waiting).
#[derive(Default)]
struct HeavyClients(
    std::sync::Mutex<std::collections::HashMap<std::net::IpAddr, (Arc<Semaphore>, usize)>>,
);

/// A client's place among its heavy reads: dropping it leaves the queue.
struct HeavyTicket {
    clients: Arc<HeavyClients>,
    key: std::net::IpAddr,
    slot: Arc<Semaphore>,
}

impl HeavyClients {
    fn enter(self: &Arc<Self>, key: std::net::IpAddr) -> ApiResult<HeavyTicket> {
        let mut m = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let e = m
            .entry(key)
            .or_insert_with(|| (Arc::new(Semaphore::new(1)), 0));
        if e.1 > HEAVY_WAITING_PER_CLIENT {
            tracing::debug!(client = %key, "api: heavy read refused, client queue full");
            return Err(ApiError::Unavailable(
                "this client already runs a heavy request and has others waiting: retry when they are done"
                    .into(),
            ));
        }
        e.1 += 1;
        Ok(HeavyTicket {
            clients: self.clone(),
            key,
            slot: e.0.clone(),
        })
    }

    #[cfg(test)]
    fn users(&self, key: std::net::IpAddr) -> usize {
        let m = self.0.lock().unwrap_or_else(|e| e.into_inner());
        m.get(&key).map_or(0, |e| e.1)
    }
}

impl Drop for HeavyTicket {
    fn drop(&mut self) {
        let mut m = self.clients.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = m.get_mut(&self.key) {
            e.1 = e.1.saturating_sub(1);
            if e.1 == 0 {
                m.remove(&self.key);
            }
        }
    }
}

impl ApiState {
    /// Run `f` over a fresh `View` on a blocking thread (at most `read_threads` at once).
    /// The view is dropped when `f` returns: no read txn outlives the request.
    pub async fn read<T, F>(self: &Arc<Self>, f: F) -> ApiResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&View) -> ApiResult<T> + Send + 'static,
    {
        self.read_on(None, &self.reads, f).await
    }

    /// `read` for an expensive request class: a `heavy` slot first, then a read slot.
    pub async fn read_heavy<T, F>(self: &Arc<Self>, f: F) -> ApiResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&View) -> ApiResult<T> + Send + 'static,
    {
        self.read_on(Some(&self.heavy), &self.reads, f).await
    }

    /// Take `n` more rate tokens for the client of this request (the guard took one): a
    /// GraphQL batch of N takes N. False when the client has fewer left.
    pub fn allow_extra(&self, peer: SocketAddr, headers: &axum::http::HeaderMap, n: u32) -> bool {
        let ip = client_ip(peer, headers, &self.settings.trusted_proxies);
        self.limiter.allow_n(ip, n)
    }

    /// Take `n` rate tokens for a client IP already resolved (websocket messages, GQ-9).
    pub fn allow_ip(&self, ip: std::net::IpAddr, n: u32) -> bool {
        self.limiter.allow_n(ip, n)
    }

    /// The client IP of a request (`trusted_proxies` applied, as the guard).
    pub fn client_ip_of(
        &self,
        peer: SocketAddr,
        headers: &axum::http::HeaderMap,
    ) -> std::net::IpAddr {
        client_ip(peer, headers, &self.settings.trusted_proxies)
    }

    /// Set `[balances] key_modules` (once, before serving; later calls are ignored).
    pub fn set_key_modules(&self, modules: Vec<String>) {
        let _ = self.key_modules.set(modules);
    }

    /// The governance-trusted modules of the by-key listing (default `["coin"]`).
    pub fn key_modules(&self) -> Vec<String> {
        self.key_modules
            .get()
            .cloned()
            .unwrap_or_else(|| vec!["coin".to_owned()])
    }

    /// `read` on the reserved status slots (cheap tip reads only).
    pub async fn read_status<T, F>(self: &Arc<Self>, f: F) -> ApiResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&View) -> ApiResult<T> + Send + 'static,
    {
        self.read_on(None, &self.status_reads, f).await
    }

    async fn read_on<T, F>(
        self: &Arc<Self>,
        first: Option<&Arc<Semaphore>>,
        pool: &Arc<Semaphore>,
        f: F,
    ) -> ApiResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&View) -> ApiResult<T> + Send + 'static,
    {
        let deadline = Instant::now() + self.settings.deadline;
        let acquire = |s: &Arc<Semaphore>| {
            tokio::time::timeout_at(deadline.into(), s.clone().acquire_owned())
        };
        // A heavy read of a known client first waits for that client's own slot (H-1), so
        // the shared heavy slots are held by at most one read per client.
        let own: Option<(HeavyTicket, OwnedSemaphorePermit)> = match first {
            Some(_) => match CLIENT.try_with(|c| *c) {
                Ok(key) => {
                    let t = self.heavy_clients.enter(key)?;
                    let p = acquire(&t.slot)
                        .await
                        .map_err(|_| ApiError::Deadline)?
                        .map_err(|e| ApiError::Internal(e.into()))?;
                    Some((t, p))
                }
                Err(_) => None,
            },
            None => None,
        };
        let gate = match first {
            Some(s) => Some(
                acquire(s)
                    .await
                    .map_err(|_| ApiError::Deadline)?
                    .map_err(|e| ApiError::Internal(e.into()))?,
            ),
            None => None,
        };
        let permit = acquire(pool)
            .await
            .map_err(|_| ApiError::Deadline)?
            .map_err(|e| ApiError::Internal(e.into()))?;
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let _p = (permit, gate, own);
            let view = store.view(deadline)?;
            view.check_deadline()?;
            f(&view)
        })
        .await
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("read task: {e}")))?
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, text) = match &self {
            ApiError::BadRequest(t) => (StatusCode::BAD_REQUEST, t.clone()),
            ApiError::NotFound(t) => (StatusCode::NOT_FOUND, t.clone()),
            ApiError::Deadline => (StatusCode::GATEWAY_TIMEOUT, self.to_string()),
            ApiError::Unavailable(t) => (StatusCode::SERVICE_UNAVAILABLE, t.clone()),
            ApiError::Internal(e) => {
                tracing::error!(error = format!("{e:#}"), "api: internal error");
                (StatusCode::INTERNAL_SERVER_ERROR, self.to_string())
            }
        };
        (
            code,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            text,
        )
            .into_response()
    }
}

/// Reserved read slots for status and readiness.
const STATUS_READS: usize = 2;

/// Header carrying the compat next-page token.
pub const CHAINWEB_NEXT: HeaderName = HeaderName::from_static("chainweb-next");

/// Limits of a data listener: in-flight cap, per-IP rate, deadline, body cap.
async fn guard(State(st): State<Arc<ApiState>>, req: Request, next: Next) -> Response {
    let Ok(slot) = st.in_flight.clone().try_acquire_owned() else {
        return ApiError::Unavailable("too many requests in flight".into()).into_response();
    };
    if let Some(ConnectInfo(peer)) = req.extensions().get::<ConnectInfo<SocketAddr>>() {
        let ip = client_ip(*peer, req.headers(), &st.settings.trusted_proxies);
        if !st.limiter.allow(ip) {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "1")],
                "rate limit exceeded",
            )
                .into_response();
        }
    }
    let cap = if req.uri().path().starts_with("/v1/txs") {
        st.settings.v1txs_max_body_bytes
    } else {
        st.settings.max_body_bytes
    };
    let delay = debug_delay();
    let client = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(peer)| client_ip(*peer, req.headers(), &st.settings.trusted_proxies));
    let run = async move {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        match client {
            Some(ip) => as_client(ip, next.run(req)).await,
            None => next.run(req).await,
        }
    };
    let resp = match tokio::time::timeout(st.settings.deadline, run).await {
        Ok(r) => r,
        Err(_) => return ApiError::Deadline.into_response(),
    };
    // Handlers build whole bodies (exact size hint); one over the cap is refused.
    if resp.body().size_hint().lower() > cap as u64 {
        return ApiError::Unavailable(format!(
            "response larger than {} MB: narrow the query",
            cap >> 20
        ))
        .into_response();
    }
    // The in-flight slot lives in the body: released when the body is sent or dropped.
    resp.map(|b| {
        Body::new(HoldSlot {
            inner: b,
            _slot: slot,
        })
    })
}

/// A response body holding its request's in-flight slot.
struct HoldSlot {
    inner: Body,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

impl HttpBody for HoldSlot {
    type Data = axum::body::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::pin::Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// Debug hook `KDX_DEBUG_API_DELAY_MS`: hold every data request this long inside the
/// in-flight slot (overload checks of the limits).
fn debug_delay() -> Duration {
    static D: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *D.get_or_init(|| {
        std::env::var("KDX_DEBUG_API_DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(Duration::from_millis)
            .unwrap_or_default()
    })
}

/// `kdx_http_*` per listener and route template (`/txs/account/{account}`); a request no
/// route matched is `unmatched`, so raw paths never become label values.
async fn track(State(api): State<&'static str>, req: Request, next: Next) -> Response {
    let t0 = Instant::now();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unmatched".to_string(), |m| m.as_str().to_string());
    let resp = next.run(req).await;
    kdx_metrics::get().http(
        api,
        &route,
        resp.status().as_u16(),
        t0.elapsed().as_secs_f64(),
    );
    resp
}

/// Admin listener: deadline only (health must answer under load).
async fn admin_guard(State(st): State<Arc<ApiState>>, req: Request, next: Next) -> Response {
    match tokio::time::timeout(st.settings.deadline, next.run(req)).await {
        Ok(r) => r,
        Err(_) => ApiError::Deadline.into_response(),
    }
}

pub struct Api {
    state: Arc<ApiState>,
    native: Router<Arc<ApiState>>,
    compat: Router<Arc<ApiState>>,
    graphql: Router<Arc<ApiState>>,
    on_bound: Option<Box<dyn FnOnce() + Send>>,
}

impl Api {
    pub fn new(store: Arc<Store>, settings: ApiSettings, tips: TipWatch) -> Api {
        let state = Arc::new(ApiState {
            reads: Arc::new(Semaphore::new(settings.read_threads.max(1))),
            status_reads: Arc::new(Semaphore::new(STATUS_READS)),
            heavy: Arc::new(Semaphore::new((settings.read_threads / 2).max(1))),
            heavy_clients: Arc::default(),
            key_modules: std::sync::OnceLock::new(),
            in_flight: Arc::new(Semaphore::new(settings.max_in_flight.max(1))),
            limiter: RateLimiter::new(settings.rate_per_ip, settings.rate_burst),
            store,
            settings,
            tips,
        });
        Api {
            state,
            native: Router::new().route("/status", axum::routing::get(status::handler)),
            compat: Router::new(),
            graphql: Router::new(),
            on_bound: None,
        }
    }

    /// Run `f` once every listener is bound (before serving; `run` sends `READY=1` here).
    pub fn on_bound(mut self, f: impl FnOnce() + Send + 'static) -> Api {
        self.on_bound = Some(Box::new(f));
        self
    }

    pub fn state(&self) -> Arc<ApiState> {
        self.state.clone()
    }

    /// Add native routes (Task 4).
    pub fn native_routes(mut self, r: Router<Arc<ApiState>>) -> Api {
        self.native = self.native.merge(r);
        self
    }

    /// Add compat routes (Tasks 5, 6).
    pub fn compat_routes(mut self, r: Router<Arc<ApiState>>) -> Api {
        self.compat = self.compat.merge(r);
        self
    }

    /// Add the GraphQL routes (P7): same guard and tracking as compat, CORS `Any`.
    pub fn graphql_routes(mut self, r: Router<Arc<ApiState>>) -> Api {
        self.graphql = self.graphql.merge(r);
        self
    }

    /// Bind every listener (all or nothing), then serve until `shutdown` (graceful).
    pub async fn serve(self, binds: Binds, shutdown: CancellationToken) -> anyhow::Result<()> {
        let st = self.state;
        let on_bound = self.on_bound;
        for (name, a) in [("native", binds.native), ("admin", binds.admin)] {
            if let Some(a) = a {
                anyhow::ensure!(
                    is_internal(a.ip()),
                    "api.{name} must bind a loopback or private address (got {a})"
                );
            }
        }
        let data = |r: Router<Arc<ApiState>>, api: &'static str| {
            r.layer(middleware::from_fn_with_state(st.clone(), guard))
                .layer(middleware::from_fn_with_state(api, track))
                .with_state(st.clone())
        };
        let native = data(self.native, "native");
        let compat = data(self.compat, "compat").layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any)
                .expose_headers([CHAINWEB_NEXT]),
        );
        let graphql = data(self.graphql, "graphql").layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );
        let admin = admin::router()
            .layer(middleware::from_fn_with_state(st.clone(), admin_guard))
            .layer(middleware::from_fn_with_state("admin", track))
            .with_state(st.clone());
        tokio::spawn(crate::sampler::run(st.clone(), shutdown.clone()));
        let mut tasks = Vec::new();
        for (name, addr, app) in [
            ("native", binds.native, native),
            ("compat", binds.compat, compat),
            ("graphql", binds.graphql, graphql),
            ("admin", binds.admin, admin),
        ] {
            let Some(addr) = addr else { continue };
            let l = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("api: bind {name} {addr}"))?;
            tracing::info!(listener = name, %addr, "api: listening");
            let stop = shutdown.clone();
            tasks.push(tokio::spawn(async move {
                axum::serve(l, app.into_make_service_with_connect_info::<SocketAddr>())
                    .with_graceful_shutdown(stop.cancelled_owned())
                    .await
                    .with_context(|| format!("api: {name} listener"))
            }));
        }
        if let Some(f) = on_bound {
            f();
        }
        for t in tasks {
            t.await??;
        }
        Ok(())
    }
}

/// A 200 JSON response.
pub fn json(v: &serde_json::Value) -> Response {
    let body = serde_json::to_vec(v).unwrap_or_default();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        Body::from(body),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn settings(read_threads: usize) -> ApiSettings {
        ApiSettings {
            network: "testnet04".into(),
            deadline: Duration::from_millis(500),
            max_in_flight: 16,
            max_body_bytes: 1 << 20,
            v1txs_max_body_bytes: 1 << 20,
            rate_per_ip: 20,
            rate_burst: 40,
            trusted_proxies: Vec::new(),
            read_threads,
            ready_max_lag: 10,
            max_event_lists: 64,
        }
    }

    #[tokio::test]
    async fn in_flight_slot_is_held_until_the_body_is_gone() {
        use tower::ServiceExt;
        let (_d, store) = crate::testutil::store();
        let mut cfg = settings(4);
        cfg.max_in_flight = 1;
        let st = Api::new(store, cfg, TipWatch::default()).state();
        let app = Router::new()
            .route("/x", axum::routing::get(|| async { "hello" }))
            .layer(middleware::from_fn_with_state(st.clone(), guard))
            .with_state(st.clone());
        let req = Request::builder().uri("/x").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(st.in_flight.available_permits(), 0, "body not sent yet");
        drop(resp);
        assert_eq!(st.in_flight.available_permits(), 1);
    }

    #[tokio::test]
    async fn status_answers_while_every_read_slot_is_busy() {
        let (_d, store) = crate::testutil::store();
        let st = Api::new(store, settings(4), TipWatch::default()).state();
        let _all = st.reads.clone().acquire_many_owned(4).await.unwrap();
        assert!(status::Status::load(&st).await.is_ok());
        // A plain read waits for a slot until its deadline.
        assert!(matches!(st.read(|_| Ok(())).await, Err(ApiError::Deadline)));
    }

    #[tokio::test]
    async fn heavy_reads_leave_slots_for_the_rest() {
        let (_d, store) = crate::testutil::store();
        let st = Api::new(store, settings(4), TipWatch::default()).state();
        let _heavy = st.heavy.clone().acquire_many_owned(2).await.unwrap();
        assert!(matches!(
            st.read_heavy(|_| Ok(())).await,
            Err(ApiError::Deadline)
        ));
        assert!(st.read(|_| Ok(())).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_client_holds_at_most_one_heavy_slot() {
        let (_d, store) = crate::testutil::store();
        // 4 read threads: 2 heavy slots.
        let st = Api::new(store, settings(4), TipWatch::default()).state();
        let a: std::net::IpAddr = "192.0.2.1".parse().unwrap();
        let b: std::net::IpAddr = "198.51.100.7".parse().unwrap();
        // Two deadline-long scans from client A.
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Arc::new(std::sync::Mutex::new(rx));
        let scan = |st: Arc<ApiState>, rx: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>| {
            tokio::spawn(as_client(a, async move {
                st.read_heavy(move |_| {
                    let _ = rx.lock().unwrap().recv_timeout(Duration::from_secs(4));
                    Ok(())
                })
                .await
            }))
        };
        let s1 = scan(st.clone(), rx.clone());
        let s2 = scan(st.clone(), rx.clone());
        let deadline = Instant::now() + Duration::from_secs(2);
        while st.heavy_clients.users(bucket_key(a)) < 2 && Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(st.heavy_clients.users(bucket_key(a)), 2);
        // Client B's list read is not stalled behind them.
        let t = Instant::now();
        let r = as_client(b, st.read_heavy(|_| Ok(()))).await;
        assert!(r.is_ok());
        assert!(
            t.elapsed() < Duration::from_millis(500),
            "{:?}",
            t.elapsed()
        );
        // A's queue is bounded: past one running and HEAVY_WAITING_PER_CLIENT waiting, a
        // heavy read is refused at once.
        let mut waiting = Vec::new();
        for _ in 2..=HEAVY_WAITING_PER_CLIENT {
            waiting.push(scan(st.clone(), rx.clone()));
        }
        while st.heavy_clients.users(bucket_key(a)) < HEAVY_WAITING_PER_CLIENT + 1 {
            tokio::task::yield_now().await;
        }
        let t = Instant::now();
        let r = as_client(a, st.read_heavy(|_| Ok(()))).await;
        assert!(matches!(r, Err(ApiError::Unavailable(_))), "{r:?}");
        assert!(t.elapsed() < Duration::from_millis(100));
        // Release the scans: every slot and every client entry comes back.
        for _ in 0..=HEAVY_WAITING_PER_CLIENT + 1 {
            let _ = tx.send(());
        }
        for h in [s1, s2].into_iter().chain(waiting) {
            assert!(h.await.unwrap().is_ok());
        }
        assert_eq!(st.heavy_clients.users(bucket_key(a)), 0);
        assert_eq!(st.heavy.available_permits(), 2);
    }
}
