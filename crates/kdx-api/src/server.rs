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
use axum::serve::ListenerExt;
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
    /// Deadline of a `/v1/txs` request (kda-txs answers within 30 s); every other request
    /// has `deadline`.
    pub v1txs_deadline: Duration,
    pub rate_per_ip: u32,
    pub rate_burst: u32,
    pub trusted_proxies: Vec<IpNet>,
    pub read_threads: usize,
    pub ready_max_lag: u32,
    pub max_event_lists: usize,
}

/// Listen addresses, any number per listener; an empty list = not bound. A listener serves
/// the same router on every one of its addresses (for example loopback plus a private LAN
/// address behind a load balancer).
#[derive(Debug, Clone, Default)]
pub struct Binds {
    pub native: Vec<SocketAddr>,
    pub compat: Vec<SocketAddr>,
    /// GraphQL (P7; spec §5.3).
    pub graphql: Vec<SocketAddr>,
    pub admin: Vec<SocketAddr>,
}

/// Node cut tips per chain (fed by a `/cut` poll), the node's `/info` facts (read by the same
/// poll) and whether a backfill is running.
#[derive(Clone, Default)]
pub struct TipWatch(Arc<TipInner>);

#[derive(Default)]
struct TipInner {
    tips: ArcSwapOption<NodeTips>,
    backfill: AtomicBool,
    /// The node's last good `/info` facts (`crate::nodeinfo`).
    info: crate::nodeinfo::SharedFacts,
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
    /// Keep a node `/info` answer when it has stock's fields; `false` (the last good answer
    /// kept) when it does not.
    pub fn set_info(&self, raw: &serde_json::Value) -> bool {
        crate::nodeinfo::store(&self.0.info, raw)
    }
    /// The last good `/info` facts, `None` before the first one.
    pub fn info(&self) -> Option<Arc<crate::nodeinfo::NodeFacts>> {
        self.0.info.load_full()
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
    /// Expensive request classes (events scan, account lists and counts) take one of these
    /// before a read slot: at most half the pool, so they cannot hold every slot.
    heavy: Arc<Semaphore>,
    /// `/v1/txs` reads (up to `v1txs_deadline` each) take one of these instead of a `heavy`
    /// slot: [`v1txs_slots`] of the pool, so a long one never holds a slot of the GraphQL
    /// or compat lists (FIX-4 part C, S-1).
    v1txs: Arc<Semaphore>,
    /// Heavy reads per client key (H-1): one runs, [`HEAVY_WAITING_PER_CLIENT`] wait.
    heavy_clients: Arc<HeavyClients>,
    in_flight: Arc<Semaphore>,
    limiter: RateLimiter,
    /// `[balances] key_modules` (M-1, SEC-2): the modules `GET /keys/{pubkey}/accounts`
    /// lists without `token`; unset means `["coin"]`.
    key_modules: std::sync::OnceLock<Vec<String>>,
    /// Extra time a `/graphql` request may run past `deadline` (B1: the `pactQuery` /
    /// `gasLimitEstimate` node passthrough's timeout); unset is none. It is the ceiling of
    /// the request's [`RunDeadline`]: only a Pact field that runs extends it.
    graphql_extra: std::sync::OnceLock<std::time::Duration>,
    /// The `/stats` count cache: the process-wide one (GraphQL `networkInfo` shares it); a
    /// test injects its own (FV-6).
    pub(crate) stats_cache: &'static crate::compat::stats::CountCache,
    /// `[api.request_log]` when on ([`Api::request_log`]).
    request_log: std::sync::OnceLock<crate::reqlog::RequestLog>,
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

/// The 503 text of a client whose heavy-read queue is full (no `Retry-After`).
pub(crate) const HEAVY_BUSY: &str =
    "this client already runs a heavy request and has others waiting: retry when they are done";

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
            return Err(ApiError::Unavailable(HEAVY_BUSY.into()));
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
        self.read_on(None, &self.reads, self.settings.deadline, f)
            .await
    }

    /// `read` with its own time budget instead of `settings.deadline`: the background work
    /// that is not a request (the `/stats` count refresh).
    pub(crate) async fn read_for<T, F>(self: &Arc<Self>, budget: Duration, f: F) -> ApiResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&View) -> ApiResult<T> + Send + 'static,
    {
        self.read_on(None, &self.reads, budget, f).await
    }

    /// `read` for an expensive request class: a `heavy` slot first, then a read slot.
    pub async fn read_heavy<T, F>(self: &Arc<Self>, f: F) -> ApiResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&View) -> ApiResult<T> + Send + 'static,
    {
        self.read_on(Some(&self.heavy), &self.reads, self.settings.deadline, f)
            .await
    }

    /// A `/v1/txs` read (FIX-4 part C, S-1): the client's heavy slot, then a slot of the
    /// `/v1/txs` pool (not a shared `heavy` one), then a read slot. Waiting for the slots is
    /// bounded by `settings.deadline` and answers 503 past it; the read itself has
    /// `settings.v1txs_deadline` (kda-txs' own 30 s).
    pub async fn read_v1txs<T, F>(self: &Arc<Self>, f: F) -> ApiResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&View) -> ApiResult<T> + Send + 'static,
    {
        let s = &self.settings;
        let wait = Slots {
            first: Some(&self.v1txs),
            pool: &self.reads,
            until: Instant::now() + s.deadline,
            late: || ApiError::Unavailable(V1TXS_BUSY.into()),
        };
        let t = Instant::now();
        let r = self.read_slots(wait, Some(s.v1txs_deadline), f).await;
        // To size the pool from real traffic (S-1): the reads that took longer than any
        // other request may.
        if t.elapsed() > s.deadline {
            tracing::info!(
                ms = t.elapsed().as_millis() as u64,
                ok = r.is_ok(),
                "api: slow /v1/txs read"
            );
        }
        r
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

    /// As [`ApiState::allow_ip`], but the client's bucket may go as low as `-borrow`
    /// ([`crate::limits::RateLimiter::allow_n_borrowing`]): a websocket connection's opening
    /// burst (R-P5P9-57).
    pub fn allow_ip_borrowing(&self, ip: std::net::IpAddr, n: u32, borrow: u32) -> bool {
        self.limiter.allow_n_borrowing(ip, n, borrow)
    }

    /// The request log, when `[api.request_log]` is on (the GraphQL handler hands it the
    /// POST body only then).
    pub fn request_log(&self) -> Option<&crate::reqlog::RequestLog> {
        self.request_log.get()
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
    /// See `graphql_extra`; set once, at start.
    pub fn set_graphql_extra_deadline(&self, extra: std::time::Duration) {
        let _ = self.graphql_extra.set(extra);
    }

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
        self.read_on(None, &self.status_reads, self.settings.deadline, f)
            .await
    }

    /// The slots in `pool` (after `first`, if any) within `budget`, which also bounds the
    /// read: a wait past it is [`ApiError::Deadline`].
    async fn read_on<T, F>(
        self: &Arc<Self>,
        first: Option<&Arc<Semaphore>>,
        pool: &Arc<Semaphore>,
        budget: Duration,
        f: F,
    ) -> ApiResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&View) -> ApiResult<T> + Send + 'static,
    {
        let wait = Slots {
            first,
            pool,
            until: Instant::now() + budget,
            late: || ApiError::Deadline,
        };
        self.read_slots(wait, None, f).await
    }

    /// Take the slots of `wait` (`late()` past `wait.until`), then run `f` on a blocking
    /// thread with a view whose deadline is `run` from now, or `wait.until` when `run` is
    /// `None` (one budget for the waits and the read).
    async fn read_slots<T, F>(
        self: &Arc<Self>,
        wait: Slots<'_>,
        run: Option<Duration>,
        f: F,
    ) -> ApiResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&View) -> ApiResult<T> + Send + 'static,
    {
        let Slots {
            first,
            pool,
            until,
            late,
        } = wait;
        let acquire =
            |s: &Arc<Semaphore>| tokio::time::timeout_at(until.into(), s.clone().acquire_owned());
        // A heavy read of a known client first waits for that client's own slot (H-1), so
        // the shared heavy slots are held by at most one read per client.
        let own: Option<(HeavyTicket, OwnedSemaphorePermit)> = match first {
            Some(_) => match CLIENT.try_with(|c| *c) {
                Ok(key) => {
                    let t = self.heavy_clients.enter(key)?;
                    let p = acquire(&t.slot)
                        .await
                        .map_err(|_| late())?
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
                    .map_err(|_| late())?
                    .map_err(|e| ApiError::Internal(e.into()))?,
            ),
            None => None,
        };
        let permit = acquire(pool)
            .await
            .map_err(|_| late())?
            .map_err(|e| ApiError::Internal(e.into()))?;
        let deadline = run.map_or(until, |d| Instant::now() + d);
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

/// The slots a read waits for, until when, and the error past it.
struct Slots<'a> {
    first: Option<&'a Arc<Semaphore>>,
    pool: &'a Arc<Semaphore>,
    until: Instant,
    late: fn() -> ApiError,
}

/// The 503 of a `/v1/txs` read that found no slot within the normal deadline.
pub(crate) const V1TXS_BUSY: &str = "too many /v1/txs requests running: retry later";

/// The `/v1/txs` slots for `read_threads` read threads: a quarter, at least one (S-1).
fn v1txs_slots(read_threads: usize) -> usize {
    (read_threads / 4).max(1)
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

/// A request's deadline, shared by the guard and the handler (B1 fix round 1, I1/I2). It
/// starts at the listener's deadline from arrival; only code that actually starts work
/// allowed to run longer (the GraphQL Pact passthrough) moves it later, by
/// [`RunDeadline::extend_to`], and never past the ceiling the guard set (`deadline` plus the
/// route's extra). The guard and the handler wait on it with [`RunDeadline::run`], which
/// follows every extension.
#[derive(Debug, Clone)]
pub struct RunDeadline {
    base: Instant,
    /// Nanoseconds after `base`.
    at: Arc<std::sync::atomic::AtomicU64>,
    ceiling: u64,
}

impl RunDeadline {
    /// A deadline at `at` that extensions may move up to `at + max_extra`.
    pub fn new(at: Instant, max_extra: Duration) -> RunDeadline {
        let base = Instant::now();
        let ns = |t: Instant| {
            u64::try_from(t.saturating_duration_since(base).as_nanos()).unwrap_or(u64::MAX)
        };
        RunDeadline {
            base,
            at: Arc::new(std::sync::atomic::AtomicU64::new(ns(at))),
            ceiling: ns(at + max_extra),
        }
    }

    /// The current deadline.
    pub fn get(&self) -> Instant {
        self.base + Duration::from_nanos(self.at.load(Ordering::Acquire))
    }

    /// Move the deadline to `t` (clamped to the ceiling; never earlier). Returns the deadline.
    pub fn extend_to(&self, t: Instant) -> Instant {
        let n = u64::try_from(t.saturating_duration_since(self.base).as_nanos())
            .unwrap_or(u64::MAX)
            .min(self.ceiling);
        self.at.fetch_max(n, Ordering::AcqRel);
        self.get()
    }

    /// Run `fut` until it ends or the deadline less `margin` passes (`None`), re-reading the
    /// deadline whenever the timer fires, so an extension made meanwhile is followed.
    pub async fn run<F: std::future::Future>(&self, margin: Duration, fut: F) -> Option<F::Output> {
        tokio::pin!(fut);
        loop {
            let until = self.get().checked_sub(margin).unwrap_or(self.base);
            tokio::select! {
                biased;
                r = &mut fut => return Some(r),
                () = tokio::time::sleep_until(until.into()) => {
                    let now = Instant::now();
                    if self.get().checked_sub(margin).is_none_or(|u| u <= now) {
                        return None;
                    }
                }
            }
        }
    }
}

/// Limits of a data listener: in-flight cap, per-IP rate, deadline, body cap.
pub async fn guard(State(st): State<Arc<ApiState>>, req: Request, next: Next) -> Response {
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
    let path = req.uri().path();
    let is_docs = path == "/docs" || path.starts_with("/docs/");
    let (cap, deadline) = if is_docs {
        // The embedded Swagger UI bundle is larger than a small `api.max_body_mb`; it is
        // static and fixed, so the body cap does not apply to it.
        (usize::MAX, st.settings.deadline)
    } else if path.starts_with("/v1/txs") {
        // The slot wait (`deadline`) plus the read (`v1txs_deadline`), as `read_v1txs`.
        (
            st.settings.v1txs_max_body_bytes,
            st.settings.deadline + st.settings.v1txs_deadline,
        )
    } else {
        (st.settings.max_body_bytes, st.settings.deadline)
    };
    // B1 (fix round 1, I2): `/graphql` may run past `deadline` only once the handler extends
    // the shared deadline for a Pact field that actually runs; the body read and every
    // other request keep `deadline`.
    let extra = if path == "/graphql" && !is_docs {
        st.graphql_extra.get().copied().unwrap_or_default()
    } else {
        Duration::ZERO
    };
    let run_deadline = RunDeadline::new(Instant::now() + deadline, extra);
    let mut req = req;
    req.extensions_mut().insert(run_deadline.clone());
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
    let resp = match run_deadline.run(Duration::ZERO, run).await {
        Some(r) => r,
        None => return ApiError::Deadline.into_response(),
    };
    // Handlers build whole bodies (exact size hint); one over the cap is refused.
    if resp.body().size_hint().lower() > cap as u64 {
        return ApiError::Unavailable(format!(
            "response larger than {} MB: narrow the query",
            cap >> 20
        ))
        .into_response();
    }
    // Fixed embedded files need no read slot: a client reading them slowly must not pin one
    // of `max_in_flight` (the rate limit still applies).
    if is_docs {
        return resp;
    }
    // The in-flight slot lives in the body: released when hyper has taken the body's last
    // frame (handlers build one frame) into its write buffer, or drops the body. That is
    // not when the client has read it: a client that stops reading holds no slot (checked
    // by `a_stalled_client_holds_no_in_flight_slot`), only buffered bytes, and the
    // listener's write-idle timeout cuts it (F-08).
    resp.map(|b| {
        Body::new(HoldSlot {
            inner: b,
            _slot: slot,
        })
    })
}

/// A listener whose connections are cut when a write makes no progress for `idle` (F-08):
/// a client that stops reading cannot pin its buffered response for longer. TCP keepalive
/// is on, so a peer that vanished is found too.
struct IdleCutListener {
    inner: tokio::net::TcpListener,
    idle: Duration,
}

impl axum::serve::Listener for IdleCutListener {
    type Io = WriteIdle<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (s, a) = axum::serve::Listener::accept(&mut self.inner).await;
        set_keepalive(&s);
        (
            WriteIdle {
                inner: s,
                idle: self.idle,
                stalled: None,
            },
            a,
        )
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

/// `SO_KEEPALIVE` on an accepted connection (best effort).
fn set_keepalive(s: &tokio::net::TcpStream) {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let on: libc::c_int = 1;
        // SAFETY: a valid socket fd and a c_int option value of the given size.
        unsafe {
            libc::setsockopt(
                s.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                std::ptr::addr_of!(on).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    #[cfg(not(unix))]
    let _ = s;
}

/// A stream whose writes fail with `TimedOut` once they have made no progress for `idle`.
/// The timer runs only while a write is pending and restarts on any progress, so a slow
/// client that keeps reading is never cut.
struct WriteIdle<S> {
    inner: S,
    idle: Duration,
    stalled: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

impl<S> WriteIdle<S> {
    /// After the inner stream answered `r`: progress clears the timer; `Pending` starts or
    /// polls it, and an expired timer is the error.
    fn watch<T>(
        &mut self,
        cx: &mut std::task::Context<'_>,
        r: std::task::Poll<std::io::Result<T>>,
    ) -> std::task::Poll<std::io::Result<T>> {
        use std::future::Future;
        use std::task::Poll;
        if r.is_ready() {
            self.stalled = None;
            return r;
        }
        let idle = self.idle;
        let t = self
            .stalled
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(idle)));
        match t.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "client stopped reading (write idle timeout)",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for WriteIdle<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for WriteIdle<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let r = std::pin::Pin::new(&mut self.inner).poll_write(cx, buf);
        self.watch(cx, r)
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let r = std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.watch(cx, r)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let r = std::pin::Pin::new(&mut self.inner).poll_flush(cx);
        self.watch(cx, r)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let r = std::pin::Pin::new(&mut self.inner).poll_shutdown(cx);
        self.watch(cx, r)
    }
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
    docs: bool,
    on_bound: Option<OnBound>,
    write_idle: Duration,
}

/// Default write-idle timeout of a connection (F-08): a client that reads nothing of a
/// response for this long is disconnected, so it cannot pin the buffered response.
pub const WRITE_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// What `on_bound` receives: every listener name with the address it actually bound (so a
/// test can bind port 0 and read the port back).
pub type BoundAddrs = [(&'static str, SocketAddr)];
type OnBound = Box<dyn FnOnce(&BoundAddrs) + Send>;

impl Api {
    pub fn new(store: Arc<Store>, settings: ApiSettings, tips: TipWatch) -> Api {
        let state = Arc::new(ApiState {
            reads: Arc::new(Semaphore::new(settings.read_threads.max(1))),
            status_reads: Arc::new(Semaphore::new(STATUS_READS)),
            heavy: Arc::new(Semaphore::new((settings.read_threads / 2).max(1))),
            v1txs: Arc::new(Semaphore::new(v1txs_slots(settings.read_threads))),
            heavy_clients: Arc::default(),
            key_modules: std::sync::OnceLock::new(),
            graphql_extra: std::sync::OnceLock::new(),
            stats_cache: &crate::compat::stats::CACHE,
            request_log: std::sync::OnceLock::new(),
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
            docs: true,
            on_bound: None,
            write_idle: WRITE_IDLE_TIMEOUT,
        }
    }

    /// Run `f` once every listener is bound (before serving; `run` sends `READY=1` here).
    pub fn on_bound(mut self, f: impl FnOnce(&BoundAddrs) + Send + 'static) -> Api {
        self.on_bound = Some(Box::new(f));
        self
    }

    pub fn state(&self) -> Arc<ApiState> {
        self.state.clone()
    }

    /// The `/stats` count cache `c` instead of the process-wide one (FV-6); before
    /// [`Api::state`] is taken.
    #[cfg(test)]
    pub(crate) fn with_stats_cache(mut self, c: &'static crate::compat::stats::CountCache) -> Api {
        Arc::get_mut(&mut self.state)
            .expect("with_stats_cache before state()")
            .stats_cache = c;
        self
    }

    /// Serve Swagger UI and the OpenAPI documents under `/docs` on the native and compat
    /// listeners (default on; `api.docs_enabled`).
    pub fn docs_enabled(mut self, on: bool) -> Api {
        self.docs = on;
        self
    }

    /// Cut a connection whose writes make no progress for `d` (default
    /// [`WRITE_IDLE_TIMEOUT`]).
    pub fn write_idle_timeout(mut self, d: Duration) -> Api {
        self.write_idle = d;
        self
    }

    /// Log the compat and GraphQL listeners' requests to `log` (`[api.request_log]`).
    pub fn request_log(self, log: crate::reqlog::RequestLog) -> Api {
        let _ = self.state.request_log.set(log);
        self
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
        let write_idle = self.write_idle;
        for (name, list) in [("native", &binds.native), ("admin", &binds.admin)] {
            for a in list {
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
        // Swagger UI and the specs ride on the two REST listeners only (never admin, which
        // carries /metrics, nor GraphQL), behind the same guard as their data routes.
        let (native_routes, compat_routes) = if self.docs {
            (
                self.native
                    .merge(crate::docs::router(crate::docs::Primary::Native)),
                self.compat
                    .merge(crate::docs::router(crate::docs::Primary::Compat)),
            )
        } else {
            (self.native, self.compat)
        };
        let native = data(native_routes, "native");
        let compat = data(compat_routes, "compat").layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any)
                .expose_headers([
                    CHAINWEB_NEXT,
                    crate::compat::stats::COUNT_AGE,
                    crate::compat::SEARCH_TRUNCATED,
                ]),
        );
        let graphql = data(self.graphql, "graphql").layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );
        // The request log wraps the whole listener (CORS, guard and all), so it records what
        // the client got.
        let (compat, graphql) = match st.request_log() {
            Some(log) => (
                compat.layer(middleware::from_fn_with_state(
                    ("compat", log.clone(), st.clone()),
                    crate::reqlog::layer,
                )),
                graphql.layer(middleware::from_fn_with_state(
                    ("graphql", log.clone(), st.clone()),
                    crate::reqlog::layer,
                )),
            ),
            None => (compat, graphql),
        };
        let admin = admin::router()
            .layer(middleware::from_fn_with_state(st.clone(), admin_guard))
            .layer(middleware::from_fn_with_state("admin", track))
            .with_state(st.clone());
        tokio::spawn(crate::sampler::run(st.clone(), shutdown.clone()));
        tokio::spawn(crate::compat::stats::run_refresh(
            st.clone(),
            shutdown.clone(),
        ));
        // Bind every address first: one failure drops the listeners bound so far, so nothing
        // serves until all of them are up.
        let mut bound = Vec::new();
        for (name, addrs, app) in [
            ("native", &binds.native, native),
            ("compat", &binds.compat, compat),
            ("graphql", &binds.graphql, graphql),
            ("admin", &binds.admin, admin),
        ] {
            for &addr in addrs {
                let l = tokio::net::TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("api: bind {name} {addr}"))?;
                bound.push((name, l, app.clone()));
            }
        }
        // Listeners stop on the caller's shutdown, or on this child token when one of them
        // exits by itself (axum::serve rarely returns, but a silent loss of one address must
        // not go unnoticed while the others keep serving).
        let stop = shutdown.child_token();
        let mut tasks = tokio::task::JoinSet::new();
        let mut addrs = Vec::with_capacity(bound.len());
        for (name, l, app) in bound {
            let addr = l.local_addr()?;
            tracing::info!(listener = name, %addr, "api: listening");
            addrs.push((name, addr));
            let stop = stop.clone();
            // `tap_io` only makes the connect info (`SocketAddr`) available for the wrapper.
            let l = IdleCutListener {
                inner: l,
                idle: write_idle,
            }
            .tap_io(|_| {});
            tasks.spawn(async move {
                axum::serve(l, app.into_make_service_with_connect_info::<SocketAddr>())
                    .with_graceful_shutdown(stop.cancelled_owned())
                    .await
                    .with_context(|| format!("api: {name} listener {addr}"))
            });
        }
        if let Some(f) = on_bound {
            f(&addrs);
        }
        let mut result = Ok(());
        while let Some(joined) = tasks.join_next().await {
            let r = joined.map_err(anyhow::Error::from).and_then(|r| r);
            if result.is_ok() {
                if let Err(e) = r {
                    result = Err(e);
                    stop.cancel();
                } else if !shutdown.is_cancelled() {
                    result = Err(anyhow::anyhow!("api: a listener exited before shutdown"));
                    stop.cancel();
                }
            }
        }
        result
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
pub(crate) mod tests {
    use super::*;

    /// B1 fix round 1: the wait follows an extension made while it runs, never past the
    /// ceiling, and never earlier than the deadline it started with.
    #[tokio::test]
    async fn run_deadline_follows_extensions_up_to_its_ceiling() {
        let t0 = Instant::now();
        let d = RunDeadline::new(t0 + Duration::from_millis(100), Duration::from_millis(300));
        assert_eq!(
            d.extend_to(t0),
            t0 + Duration::from_millis(100),
            "never earlier"
        );
        let ext = d.clone();
        let slow = async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            ext.extend_to(Instant::now() + Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(200)).await;
            7
        };
        assert_eq!(d.run(Duration::ZERO, slow).await, Some(7));
        assert!(d.get() <= t0 + Duration::from_millis(400), "ceiling");
        let stuck = tokio::time::sleep(Duration::from_secs(10));
        let t = Instant::now();
        assert_eq!(d.run(Duration::ZERO, stuck).await, None);
        assert!(t.elapsed() < Duration::from_millis(300));
        // Without an extension the wait is the plain deadline.
        let d = RunDeadline::new(
            Instant::now() + Duration::from_millis(80),
            Duration::from_secs(5),
        );
        let t = Instant::now();
        assert_eq!(
            d.run(Duration::ZERO, tokio::time::sleep(Duration::from_secs(1)))
                .await,
            None
        );
        assert!(t.elapsed() < Duration::from_millis(300));
    }

    pub(crate) fn settings(read_threads: usize) -> ApiSettings {
        ApiSettings {
            network: "testnet04".into(),
            deadline: Duration::from_millis(500),
            max_in_flight: 16,
            max_body_bytes: 1 << 20,
            v1txs_max_body_bytes: 1 << 20,
            v1txs_deadline: std::time::Duration::from_secs(30),
            rate_per_ip: 20,
            rate_burst: 40,
            trusted_proxies: Vec::new(),
            read_threads,
            ready_max_lag: 10,
            max_event_lists: 64,
        }
    }

    /// A body larger than the loopback socket buffers, so a client that does not read
    /// leaves most of it unsent.
    const BIG: usize = 32 << 20;

    /// A native listener serving `/big` (`BIG` bytes) with a write-idle timeout of `idle`:
    /// (data dir, state, address, stop, serve task).
    async fn serve_big(
        idle: Duration,
    ) -> (
        tempfile::TempDir,
        Arc<ApiState>,
        SocketAddr,
        CancellationToken,
        tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        let (d, store) = crate::testutil::store();
        let mut cfg = settings(4);
        cfg.max_in_flight = 4;
        cfg.max_body_bytes = 64 << 20;
        cfg.deadline = Duration::from_secs(10);
        let big = Router::new().route("/big", axum::routing::get(|| async { vec![b'x'; BIG] }));
        let (tx, rx) = tokio::sync::oneshot::channel();
        let api = Api::new(store, cfg, TipWatch::default())
            .native_routes(big)
            .write_idle_timeout(idle)
            .on_bound(move |a| {
                let _ = tx.send(a.to_vec());
            });
        let st = api.state();
        let binds = Binds {
            native: vec!["127.0.0.1:0".parse().unwrap()],
            ..Binds::default()
        };
        let stop = CancellationToken::new();
        let task = tokio::spawn(api.serve(binds, stop.clone()));
        let addr = rx.await.unwrap()[0].1;
        (d, st, addr, stop, task)
    }

    /// Send `GET /big`, wait `stall` without reading, then read everything at `step` bytes
    /// per `pause`: the bytes received before the connection ended.
    async fn read_big(addr: SocketAddr, stall: Duration, step: usize, pause: Duration) -> usize {
        tokio::task::spawn_blocking(move || {
            use std::io::{Read, Write};
            let mut c = std::net::TcpStream::connect(addr).unwrap();
            c.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
            c.write_all(b"GET /big HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                .unwrap();
            std::thread::sleep(stall);
            let mut got = 0;
            let mut buf = vec![0u8; step];
            loop {
                match c.read(&mut buf) {
                    Ok(0) | Err(_) => return got,
                    Ok(n) => got += n,
                }
                std::thread::sleep(pause);
            }
        })
        .await
        .unwrap()
    }

    /// `[api.request_log]`: the compat and GraphQL listeners log one line per request (the
    /// GraphQL body from the handler's response extension, the client hashed), the native
    /// listener none.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_log_covers_compat_and_graphql_only() {
        use crate::reqlog::{ClientIp, RequestLog, Settings};
        let (d, store) = crate::testutil::store();
        let path = d.path().join("log").join("requests.jsonl");
        let log = RequestLog::start(Settings {
            path: path.clone(),
            max_bytes: 1 << 20,
            keep: 1,
            sample_rate: 1.0,
            max_body_bytes: 4096,
            queue: 16,
            queue_max_bytes: 1 << 20,
            client_ip: ClientIp::Hash,
        })
        .unwrap();
        let ok = || async { "ok" };
        let gql_log = log.clone();
        let gql = move |body: axum::body::Bytes| async move {
            let mut r = "{}".into_response();
            r.extensions_mut().insert(gql_log.graphql_body(&body));
            r
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        let api = Api::new(store, settings(4), TipWatch::default())
            .native_routes(Router::new().route("/n", axum::routing::get(ok)))
            .compat_routes(Router::new().route("/txs/c/{a}", axum::routing::get(ok)))
            .graphql_routes(Router::new().route("/graphql", axum::routing::post(gql)))
            .request_log(log.clone())
            .on_bound(move |a| {
                let _ = tx.send(a.to_vec());
            });
        let binds = Binds {
            native: vec!["127.0.0.1:0".parse().unwrap()],
            compat: vec!["127.0.0.1:0".parse().unwrap()],
            graphql: vec!["127.0.0.1:0".parse().unwrap()],
            ..Binds::default()
        };
        let stop = CancellationToken::new();
        let task = tokio::spawn(api.serve(binds, stop.clone()));
        let addrs = rx.await.unwrap();
        let at = |n: &str| addrs.iter().find(|(k, _)| *k == n).unwrap().1;
        let (native, compat, graphql) = (at("native"), at("compat"), at("graphql"));
        let body = r#"{"query":"query { lastBlockHeight }","variables":{}}"#;
        tokio::task::spawn_blocking(move || {
            use std::io::{Read, Write};
            let send = |a: SocketAddr, req: String| {
                let mut c = std::net::TcpStream::connect(a).unwrap();
                c.write_all(req.as_bytes()).unwrap();
                let mut s = String::new();
                let _ = c.read_to_string(&mut s);
                assert!(s.starts_with("HTTP/1.1 200"), "{s}");
            };
            send(native, "GET /n HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".into());
            send(
                compat,
                "GET /txs/c/k%3Aab?limit=5 HTTP/1.1\r\nHost: x\r\nX-Forwarded-For: 203.0.113.9\r\nConnection: close\r\n\r\n".into(),
            );
            send(
                graphql,
                format!(
                    "POST /graphql HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ),
            );
        })
        .await
        .unwrap();
        let t0 = Instant::now();
        while log.counts().0 < 2 && t0.elapsed() < Duration::from_secs(10) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(log.counts(), (2, 0));
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2, "{text}");
        let c = lines.iter().find(|v| v["listener"] == "compat").unwrap();
        assert_eq!(c["method"], "GET");
        assert_eq!(c["uri"], "/txs/c/k%3Aab?limit=5");
        assert_eq!(c["route"], "/txs/c/{a}");
        assert_eq!(c["status"], 200);
        assert_eq!(c["client"].as_str().unwrap().len(), 16);
        let g = lines.iter().find(|v| v["listener"] == "graphql").unwrap();
        assert_eq!(g["gql"]["query"], "query { lastBlockHeight }");
        assert!(!text.contains("127.0.0.1") && !text.contains("203.0.113.9"));
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    /// F-08 (triage check): a client that stops reading a whole-body response does not hold
    /// an in-flight slot: hyper takes the single frame into its write buffer and drops the
    /// body (and the slot) at once; only the bytes stay buffered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stalled_client_holds_no_in_flight_slot() {
        let (_d, st, addr, stop, task) = serve_big(Duration::from_secs(60)).await;
        let reader = tokio::spawn(read_big(
            addr,
            Duration::from_millis(1500),
            1 << 20,
            Duration::ZERO,
        ));
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(
            st.in_flight.available_permits(),
            4,
            "slot released while stalled"
        );
        let got = reader.await.unwrap();
        assert!(got > BIG, "a reader that resumes gets it all ({got})");
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    /// F-08: a client that stops reading is disconnected once writes make no progress for
    /// the write-idle timeout; a slow client that keeps reading is not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stalled_client_is_cut_after_the_write_idle_timeout() {
        let (_d, _st, addr, stop, task) = serve_big(Duration::from_millis(400)).await;
        let stalled = read_big(addr, Duration::from_millis(2500), 1 << 20, Duration::ZERO).await;
        assert!(
            stalled < BIG,
            "stalled client got the whole body ({stalled})"
        );
        let slow = read_big(addr, Duration::ZERO, 1 << 20, Duration::from_millis(40)).await;
        assert!(slow > BIG, "slow reader got {slow} of {BIG}+headers");
        stop.cancel();
        task.await.unwrap().unwrap();
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

    /// S3 and M3: the body cap does not apply to `/docs`, and a docs response does not hold
    /// an in-flight slot while it streams; a data route and `/v1/txs` over the cap are 503.
    #[tokio::test]
    async fn docs_are_exempt_from_the_body_cap_and_the_slot() {
        use tower::ServiceExt;
        let (_d, store) = crate::testutil::store();
        let mut cfg = settings(4);
        cfg.max_body_bytes = 1 << 20;
        cfg.v1txs_max_body_bytes = 1 << 20;
        cfg.max_in_flight = 1;
        let st = Api::new(store, cfg, TipWatch::default()).state();
        let big = || async { vec![b'x'; 2 << 20] };
        let app = Router::new()
            .route("/docs/big", axum::routing::get(big))
            .route("/big", axum::routing::get(big))
            .route("/v1/txs/big", axum::routing::get(big))
            .layer(middleware::from_fn_with_state(st.clone(), guard))
            .with_state(st.clone());
        let get = |p: &'static str| {
            let app = app.clone();
            async move {
                app.oneshot(Request::builder().uri(p).body(Body::empty()).unwrap())
                    .await
                    .unwrap()
            }
        };
        let r = get("/docs/big").await;
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(
            st.in_flight.available_permits(),
            1,
            "a docs body must not hold a slot"
        );
        drop(r);
        for p in ["/big", "/v1/txs/big"] {
            let r = get(p).await;
            assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE, "{p}");
            drop(r);
        }
    }

    /// A free port on `ip` (bound once and released; the tests bind it again right away).
    fn free_addr(ip: &str) -> Option<SocketAddr> {
        let l = std::net::TcpListener::bind((ip, 0)).ok()?;
        l.local_addr().ok()
    }

    /// The status code of `GET path` on `addr` over plain HTTP/1.1.
    async fn get_status(addr: SocketAddr, path: &str) -> std::io::Result<u16> {
        get_status_with(addr, path, "").await
    }

    /// Like `get_status`, with `extra` header lines (each ending in `\r\n`).
    async fn get_status_with(addr: SocketAddr, path: &str, extra: &str) -> std::io::Result<u16> {
        let req = format!("GET {path} HTTP/1.1\r\nHost: x\r\n{extra}Connection: close\r\n\r\n");
        tokio::task::spawn_blocking(move || {
            use std::io::{Read, Write};
            let t = Some(Duration::from_secs(5));
            let mut c = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
            c.set_read_timeout(t)?;
            c.write_all(req.as_bytes())?;
            let mut head = String::new();
            c.read_to_string(&mut head)?;
            let code = head.get(9..12).and_then(|c| c.parse().ok());
            code.ok_or_else(|| std::io::Error::other(format!("no status line: {head:?}")))
        })
        .await
        .expect("get_status task")
    }

    /// Task 10: one listener on several addresses (two loopback ports, plus `[::1]` where
    /// the host has IPv6) answers `/healthz` and `/readyz` on every one of them.
    #[tokio::test]
    async fn admin_answers_on_every_listed_address() {
        let (_d, store) = crate::testutil::store();
        let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut admin = vec![loopback, loopback];
        if std::net::TcpListener::bind("[::1]:0").is_ok() {
            admin.push("[::1]:0".parse().unwrap());
        }
        let binds = Binds {
            admin: admin.clone(),
            compat: vec![loopback],
            ..Binds::default()
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        let api = Api::new(store, settings(4), TipWatch::default()).on_bound(move |a| {
            let _ = tx.send(a.to_vec());
        });
        let stop = CancellationToken::new();
        let task = tokio::spawn(api.serve(binds, stop.clone()));
        let bound = rx.await.unwrap();
        let of = |n: &str| -> Vec<SocketAddr> {
            bound.iter().filter(|b| b.0 == n).map(|b| b.1).collect()
        };
        let (admin, compat) = (of("admin"), of("compat"));
        assert!(admin.len() >= 2, "{admin:?}");
        for &a in &admin {
            assert_eq!(get_status(a, "/healthz").await.unwrap(), 200, "{a}");
            // Not ready (no node tips in a test store), but answered.
            assert_eq!(get_status(a, "/readyz").await.unwrap(), 503, "{a}");
        }
        // The compat router (no routes added here) answers on its own address, not admin's.
        assert_eq!(get_status(compat[0], "/healthz").await.unwrap(), 404);
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    /// Task 10: binding is all or nothing; a taken address fails `serve` and leaves none of
    /// the other addresses listening. A public admin address is refused before any bind.
    #[tokio::test]
    async fn one_failed_bind_leaves_nothing_listening() {
        let first = free_addr("127.0.0.1").unwrap();
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let binds = Binds {
            admin: vec![first, taken.local_addr().unwrap()],
            ..Binds::default()
        };
        let (_d, store) = crate::testutil::store();
        let api = Api::new(store, settings(4), TipWatch::default()).on_bound(|_| {
            panic!("on_bound must not run when a bind fails");
        });
        let e = api
            .serve(binds, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("api: bind admin"), "{e:#}");
        assert!(tokio::net::TcpStream::connect(first).await.is_err());

        let (_d, store) = crate::testutil::store();
        let binds = Binds {
            admin: vec![first, "8.8.8.8:31369".parse().unwrap()],
            ..Binds::default()
        };
        let api = Api::new(store, settings(4), TipWatch::default());
        let e = api
            .serve(binds, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("loopback or private"), "{e:#}");
        assert!(tokio::net::TcpStream::connect(first).await.is_err());
    }

    /// Review M-9: the client address is resolved per listener address. Behind a trusted
    /// proxy on the second (LAN-style) address two `X-Forwarded-For` values get two buckets,
    /// and the same value shares one.
    #[tokio::test]
    async fn rate_buckets_follow_forwarded_for_on_the_second_address() {
        let (_d, store) = crate::testutil::store();
        let mut cfg = settings(4);
        cfg.rate_per_ip = 1;
        cfg.rate_burst = 1;
        cfg.trusted_proxies = vec![IpNet::parse("127.0.0.1/32").unwrap()];
        let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let binds = Binds {
            compat: vec![loopback, loopback],
            ..Binds::default()
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        let api = Api::new(store, cfg, TipWatch::default())
            .compat_routes(Router::new().route("/ping", axum::routing::get(|| async { "ok" })))
            .on_bound(move |a| {
                let _ = tx.send(a.to_vec());
            });
        let stop = CancellationToken::new();
        let task = tokio::spawn(api.serve(binds, stop.clone()));
        let second = rx.await.unwrap()[1].1;
        let hdr = |ip: &str| format!("X-Forwarded-For: {ip}\r\n");
        let get = |ip: &'static str| async move {
            get_status_with(second, "/ping", &hdr(ip)).await.unwrap()
        };
        assert_eq!(get("198.51.100.1").await, 200);
        assert_eq!(get("198.51.100.1").await, 429, "same client, same bucket");
        assert_eq!(
            get("198.51.100.2").await,
            200,
            "another client, its own bucket"
        );
        stop.cancel();
        task.await.unwrap().unwrap();
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

    /// FIX-4 part B (B1), part C (S-1): `read_v1txs` reads within `v1txs_deadline` (the
    /// view's deadline), not `settings.deadline`.
    #[tokio::test]
    async fn read_v1txs_reads_with_its_own_budget() {
        let (_d, store) = crate::testutil::store();
        // settings(): a 500 ms deadline.
        let mut cfg = settings(4);
        cfg.v1txs_deadline = Duration::from_secs(5);
        let st = Api::new(store.clone(), cfg.clone(), TipWatch::default()).state();
        let slow = |v: &View| {
            std::thread::sleep(Duration::from_millis(700));
            v.check_deadline()
        };
        assert!(matches!(st.read_heavy(slow).await, Err(ApiError::Deadline)));
        assert!(st.read_v1txs(slow).await.is_ok());
        cfg.v1txs_deadline = Duration::from_millis(100);
        let st = Api::new(store, cfg, TipWatch::default()).state();
        assert!(matches!(st.read_v1txs(slow).await, Err(ApiError::Deadline)));
    }

    /// FIX-4 part C (S-1): `/v1/txs` reads have their own pool, `max(1, read_threads / 4)`,
    /// not the shared heavy slots: two long `/v1/txs` reads of two clients leave every heavy
    /// slot to the GraphQL and compat lists.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn long_v1txs_reads_do_not_block_other_heavy_routes() {
        let (_d, store) = crate::testutil::store();
        let mut cfg = settings(4);
        cfg.v1txs_deadline = Duration::from_secs(30);
        let st = Api::new(store.clone(), cfg.clone(), TipWatch::default()).state();
        assert_eq!(st.v1txs.available_permits(), 1);
        for (threads, pool) in [(1, 1), (7, 1), (8, 2), (16, 4)] {
            cfg.read_threads = threads;
            let s = Api::new(store.clone(), cfg.clone(), TipWatch::default()).state();
            assert_eq!(s.v1txs.available_permits(), pool, "read_threads {threads}");
        }
        let a: std::net::IpAddr = "192.0.2.1".parse().unwrap();
        let b: std::net::IpAddr = "198.51.100.7".parse().unwrap();
        let c: std::net::IpAddr = "203.0.113.9".parse().unwrap();
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Arc::new(std::sync::Mutex::new(rx));
        let long =
            |ip, st: Arc<ApiState>, rx: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>| {
                tokio::spawn(as_client(ip, async move {
                    st.read_v1txs(move |_| {
                        let _ = rx.lock().unwrap().recv_timeout(Duration::from_secs(10));
                        Ok(())
                    })
                    .await
                }))
            };
        let l1 = long(a, st.clone(), rx.clone());
        let l2 = long(b, st.clone(), rx.clone());
        let until = Instant::now() + Duration::from_secs(2);
        while st.v1txs.available_permits() > 0 && Instant::now() < until {
            tokio::task::yield_now().await;
        }
        assert_eq!(st.v1txs.available_permits(), 0);
        // Every heavy slot is free: another client's heavy list answers at once.
        assert_eq!(st.heavy.available_permits(), 2);
        for _ in 0..2 {
            let t = Instant::now();
            assert!(as_client(c, st.read_heavy(|_| Ok(()))).await.is_ok());
            assert!(
                t.elapsed() < Duration::from_millis(300),
                "{:?}",
                t.elapsed()
            );
        }
        for _ in 0..2 {
            let _ = tx.send(());
        }
        assert!(l1.await.unwrap().is_ok());
        // The second ran after the first or, past its 500 ms wait, was refused with 503;
        // either way it never held a heavy slot.
        let _ = l2.await.unwrap();
        assert_eq!(st.heavy.available_permits(), 2);
        assert_eq!(st.v1txs.available_permits(), 1);
    }

    /// FIX-4 part C (S-1): waiting for a `/v1/txs` slot is bounded by the normal deadline,
    /// past which the answer is 503 (the overload answer), not the 30 s read budget.
    #[tokio::test]
    async fn v1txs_slot_wait_is_capped_at_the_normal_deadline() {
        let (_d, store) = crate::testutil::store();
        // settings(): a 500 ms deadline (5 s in production), a 30 s /v1/txs budget.
        let st = Api::new(store, settings(4), TipWatch::default()).state();
        let _busy = st.v1txs.clone().acquire_owned().await.unwrap();
        let t = Instant::now();
        let r = st.read_v1txs(|_| Ok(())).await;
        let waited = t.elapsed();
        assert!(matches!(r, Err(ApiError::Unavailable(_))), "{r:?}");
        assert!(
            waited >= Duration::from_millis(450) && waited < Duration::from_secs(3),
            "{waited:?}"
        );
        let resp = r.unwrap_err().into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        // A /v1/txs read never waits on, or holds, a shared heavy slot.
        let _heavy = st.heavy.clone().acquire_many_owned(2).await.unwrap();
        drop(_busy);
        assert!(st.read_v1txs(|_| Ok(())).await.is_ok());
    }

    /// Block `height` on chain 0: `n` user txs of k:alice, each one `coin.TRANSFER` to k:bob.
    fn transfer_block(height: u32, n: usize) -> kdx_decode::DecodedBlock {
        use kdx_types::hash::b64url_encode;
        use serde_json::json;
        let h32 = |tag: u8| {
            let mut b = [tag; 32];
            b[0] = (height >> 8) as u8;
            b[1] = height as u8;
            b
        };
        let txs: Vec<_> = (0..n)
            .map(|i| {
                let cmd = json!({
                    "payload": {"exec": {"data": null, "code": format!("(t {height} {i})")}},
                    "meta": {"sender": "k:alice", "chainId": "0",
                             "creationTime": 1_700_000_000u64 + u64::from(height),
                             "gasLimit": 1000, "gasPrice": 1e-8, "ttl": 600},
                    "signers": [{"pubKey": "alice-key"}],
                    "nonce": format!("n-{height}-{i}"),
                    "networkId": "mainnet01",
                })
                .to_string();
                let rk = kdx_types::blake2b_256(cmd.as_bytes()).to_b64url();
                let tx =
                    serde_json::to_vec(&json!({"hash": rk, "sigs": [{"sig": "ab"}], "cmd": cmd}))
                        .unwrap();
                // The node's key order (the decoder checks it).
                let ev = json!({"params": ["k:alice", "k:bob", 1.5], "name": "TRANSFER",
                    "module": {"namespace": null, "name": "coin"},
                    "moduleHash": b64url_encode(&[11; 32])});
                let out = serde_json::to_vec(&json!({"gas": 1,
                    "result": {"status": "success", "data": 1}, "reqKey": rk, "logs": null,
                    "events": [ev], "metaData": null, "continuation": null, "txId": null}))
                .unwrap();
                json!([b64url_encode(&tx), b64url_encode(&out)])
            })
            .collect();
        let cb = serde_json::to_vec(
            &json!({"gas": 0, "result": {"status": "success", "data": "ok"},
            "reqKey": b64url_encode(&h32(0xCB)), "logs": null, "events": [],
            "metaData": null, "continuation": null, "txId": null}),
        )
        .unwrap();
        let miner = br#"{"account":"k:miner","predicate":"keys-all","public-keys":["m"]}"#;
        let payload_hash = b64url_encode(&h32(0xA4));
        let parent = {
            let mut b = [0xB0; 32];
            b[0] = (height.saturating_sub(1) >> 8) as u8;
            b[1] = height.saturating_sub(1) as u8;
            b
        };
        let header = json!({
            "nonce": "1", "creationTime": 1_700_000_000_000_000u64 + u64::from(height) * 30_000_000,
            "parent": b64url_encode(&parent), "adjacents": {"5": b64url_encode(&h32(0xAD))},
            "target": b64url_encode(&[0xff; 32]), "payloadHash": payload_hash, "chainId": 0,
            "weight": b64url_encode(&[1; 32]), "height": height, "chainwebVersion": "mainnet01",
            "epochStart": 1, "featureFlags": 0, "hash": b64url_encode(&h32(0xB0))
        });
        let payload = json!({
            "transactions": txs, "minerData": b64url_encode(miner),
            "transactionsHash": b64url_encode(&h32(0xA7)),
            "outputsHash": b64url_encode(&h32(0xA8)),
            "payloadHash": payload_hash, "coinbase": b64url_encode(&cb),
        });
        kdx_decode::decode_block(&header, &payload).expect("block decodes")
    }

    /// 100 blocks of 100 transfers each to `k:alice`, as a store. The store is opened on
    /// the written hot store (its dictionary is read at open).
    fn alice_store() -> (tempfile::TempDir, Arc<crate::store::Store>) {
        let d = tempfile::tempdir().unwrap();
        let g = kdx_hot::Geometry {
            max_gb: 1,
            growth_mb: 16,
        };
        let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap());
        {
            let mut w = hot.rw().unwrap();
            let mut dict = kdx_hot::HotDict::load(&w).unwrap();
            for h in 1..=100 {
                let ib = kdx_decode::index_block(
                    &transfer_block(h, 100),
                    &kdx_decode::VerifyReport::default(),
                    &mut dict,
                )
                .unwrap();
                kdx_hot::tail::apply(&mut w, &ib, &mut dict).unwrap();
            }
            dict.persist(&mut w).unwrap();
            w.commit().unwrap();
        }
        let store = Arc::new(crate::store::Store::open(hot, d.path()).unwrap());
        (d, store)
    }

    /// F-21 (PR-2 review note 7): past the keep budget the walk keeps only keys and the
    /// writer reads those rows again; the `/v1/txs` body is the same as with every row kept.
    #[test]
    fn v1txs_body_is_the_same_past_the_keep_budget() {
        use crate::compat::account::KeepBudget;
        let (_d, store) = alice_store();
        let view = store
            .view(Instant::now() + Duration::from_secs(60))
            .unwrap();
        let cap = 64 << 20;
        let all = |room: usize| {
            let mut b = KeepBudget::new(cap);
            b.admit(cap - room).unwrap();
            b
        };
        let full =
            crate::v1txs::entries_in(&view, "k:alice", "coin", 10_000, cap, all(cap)).unwrap();
        // Room for a few records only: the rest of the rows are keys.
        let rows =
            crate::v1txs::token_rows(&view, "k:alice", "coin", 10_000, &mut all(4096)).unwrap();
        let kept = rows.iter().filter(|(_, r)| r.is_some()).count();
        assert!(
            kept > 0 && kept < rows.len(),
            "kept {kept} of {}",
            rows.len()
        );
        let part =
            crate::v1txs::entries_in(&view, "k:alice", "coin", 10_000, cap, all(4096)).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&full)
                .unwrap()
                .as_array()
                .map(Vec::len),
            Some(10_000)
        );
        assert!(part == full, "the body changed past the keep budget");
    }

    /// FIX-4 part B (B1): `/v1/txs` reads within `v1txs_deadline` (kda-txs' own 30 s), not
    /// the per-request deadline, so `limit=10000` on a big account answers; the other routes
    /// keep the per-request deadline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn v1txs_has_its_own_deadline() {
        use tower::ServiceExt;
        let (_d, store) = alice_store();
        let mut cfg = settings(4);
        // Shorter than any read of this store; /v1/txs has its own budget.
        cfg.deadline = Duration::from_millis(1);
        cfg.v1txs_deadline = Duration::from_secs(30);
        cfg.v1txs_max_body_bytes = 64 << 20;
        let cfg_deadline = cfg.deadline;
        let st = Api::new(store, cfg, TipWatch::default()).state();
        let search = crate::search::SearchCtx::new(crate::compat::SearchSettings::default(), None);
        let app = crate::compat::router(search)
            .layer(middleware::from_fn_with_state(st.clone(), guard))
            .with_state(st.clone());
        let get = |uri: &str| {
            let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
            app.clone().oneshot(req)
        };
        let t0 = Instant::now();
        let resp = get("/v1/txs/k:alice/coin/10000").await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // The read outlived the per-request deadline, so the 200 shows its own budget.
        assert!(t0.elapsed() > cfg_deadline, "{:?}", t0.elapsed());
        let body = axum::body::to_bytes(resp.into_body(), 64 << 20)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v.as_array().map(Vec::len), Some(10_000));
        // The other routes keep the per-request deadline. A read of this store takes about
        // 1.4 ms in a release build, within the 1 ms tick of tokio's timer, so racing it
        // against the 1 ms deadline was flaky (200 or 504). Instead every read slot is held:
        // the request cannot finish within its deadline and must answer 504, every run.
        let held = st
            .reads
            .clone()
            .acquire_many_owned(st.settings.read_threads as u32)
            .await
            .unwrap();
        // Within the per-request deadline, not the 30 s `/v1/txs` one (audit-pr5-review note
        // 1): a route wrongly on `v1txs_deadline` would also answer 504, but after 30 s.
        let t = Instant::now();
        let resp = get("/txs/account/k:alice?limit=10000").await.unwrap();
        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
        assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
        drop(held);
    }

    /// Suite run 4: `/txs/account?chain=` beyond an i64 is chainweb-data's 400 (its `chain`
    /// is a 64-bit `Int`); a whole number that is no chain stays an empty page.
    #[tokio::test]
    async fn account_chain_beyond_i64_is_a_400() {
        use tower::ServiceExt;
        let (_d, store) = crate::testutil::store();
        let st = Api::new(store, settings(2), TipWatch::default()).state();
        let search = crate::search::SearchCtx::new(crate::compat::SearchSettings::default(), None);
        let app = crate::compat::router(search)
            .layer(middleware::from_fn_with_state(st.clone(), guard))
            .with_state(st.clone());
        let get = |uri: &str| {
            let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
            app.clone().oneshot(req)
        };
        let body = |r: Response| async move {
            let s = r.status();
            let b = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
            (s, String::from_utf8_lossy(&b).into_owned())
        };
        for chain in [
            "9223372036854775808",
            "-9223372036854775809",
            "99999999999999999999999999999999999999999",
        ] {
            let (s, b) = body(
                get(&format!(
                    "/txs/account/k:alice?chain={chain}&limit=2&offset=10000"
                ))
                .await
                .unwrap(),
            )
            .await;
            assert_eq!(s, StatusCode::BAD_REQUEST, "{chain}: {b}");
            assert!(
                b.starts_with("Error parsing query parameter chain failed"),
                "{chain}: {b}"
            );
        }
        for chain in ["9223372036854775807", "-9223372036854775808", "20", "-1"] {
            let (s, b) = body(
                get(&format!("/txs/account/k:alice?chain={chain}"))
                    .await
                    .unwrap(),
            )
            .await;
            assert_eq!(s, StatusCode::OK, "{chain}: {b}");
            assert_eq!(b, "[]", "{chain}");
        }
    }
}
