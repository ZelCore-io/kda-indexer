//! Search limits and the code index handle (spec §3.8, plan Task 10, rulings S-4 and S-5).
//!
//! One [`SearchGate`] covers every search route: `/txs/search?search`, `/txs/events?search`
//! and the native `/search/code` and `/search/events`. A global semaphore of
//! `search.concurrency` permits answers `503` when full; a per-client count (keyed like the
//! rate limiter: the IPv4 address or the IPv6 /64) runs `search.per_client` at once and queues
//! up to [`SEARCH_WAITING_PER_CLIENT`] more until the request deadline (`429` only when that
//! queue is full, `504` when a queued search's turn does not come in time); a queued search
//! takes its global permit only when its turn comes. The permit moves into the blocking read and is dropped when that read ends: on completion, on
//! its deadline, and after a client disconnect once the abandoned read stops (at most the
//! request deadline), so a client that reconnects cannot run more scans than its share.
//! Every refusal and every cut answer counts in `kdx_search_rejected_total{reason}`.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::request::Parts;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use kdx_search::{SearchManifest, Searcher};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::compat::SearchSettings;
use crate::limits::{bucket_key, client_ip};
use crate::server::ApiState;

/// Longest search pattern accepted (characters), on every search route.
pub const MAX_PATTERN_CHARS: usize = 256;
/// Most unescaped `%` wildcards in one pattern (bounds the matcher's backtracking).
pub const MAX_PATTERN_WILDCARDS: usize = 8;

/// Count a refusal or a cut answer (`too_short|busy|client_busy|wait_timeout|timeout|cap|lag`).
pub fn rejected(reason: &'static str) {
    kdx_metrics::get().search_rejected(reason);
}

/// Everything the search routes share: settings, the gate and the code index.
pub struct SearchCtx {
    pub settings: SearchSettings,
    pub gate: Arc<SearchGate>,
    pub index: SearchIndex,
}

impl SearchCtx {
    /// `search_dir`: the data dir's `search/` (None: no code index; compat scans, native 503).
    pub fn new(settings: SearchSettings, search_dir: Option<PathBuf>) -> Arc<SearchCtx> {
        Arc::new(SearchCtx {
            gate: Arc::new(SearchGate::new(settings.concurrency, settings.per_client)),
            index: SearchIndex::new(search_dir, settings.max_candidates),
            settings,
        })
    }
}

// ---------------------------------------------------------------------------------------
// Gate

/// Searches one client key may have waiting behind its `per_client` running ones: the same
/// depth as the per-client heavy-read queue (H-1). Past it a search is refused at once (429).
pub const SEARCH_WAITING_PER_CLIENT: usize = crate::server::HEAVY_WAITING_PER_CLIENT;

/// How long before the request deadline a queued search gives up, so its own refusal
/// (counted as `wait_timeout`) answers before the listener's deadline cuts the request.
const WAIT_MARGIN: Duration = Duration::from_millis(50);

/// The 429 text: the client's search queue is full.
pub const CLIENT_BUSY: &str =
    "this client already runs searches and has others waiting: retry when they are done";
/// The 503 text: `search.concurrency` searches run in total.
pub const GLOBAL_BUSY: &str = "too many searches in flight: retry shortly";
/// The 504 text: a queued search did not get its turn before the request deadline.
pub const WAIT_TIMEOUT: &str =
    "request deadline exceeded while waiting for this client's earlier searches";

pub struct SearchGate {
    global: Arc<Semaphore>,
    concurrency: usize,
    per_client: usize,
    /// Per client key: its `per_client`-permit semaphore and its users (running and
    /// waiting).
    clients: Mutex<HashMap<IpAddr, (Arc<Semaphore>, usize)>>,
}

/// A client's place among its searches (running or waiting): dropping it leaves the queue.
struct Ticket {
    gate: Arc<SearchGate>,
    key: IpAddr,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let mut m = self.gate.clients.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = m.get_mut(&self.key) {
            e.1 = e.1.saturating_sub(1);
            if e.1 == 0 {
                m.remove(&self.key);
            }
        }
    }
}

/// One running search. Dropping it frees the global slot, the client's slot and its place.
pub struct SearchPermit {
    _global: OwnedSemaphorePermit,
    _client: OwnedSemaphorePermit,
    _ticket: Ticket,
}

impl SearchGate {
    pub fn new(concurrency: usize, per_client: usize) -> SearchGate {
        SearchGate {
            global: Arc::new(Semaphore::new(concurrency.max(1))),
            concurrency: concurrency.max(1),
            per_client: per_client.max(1),
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// A permit for `ip`, waiting behind the client's running searches until `deadline`
    /// (less a small margin), or the finished refusal: `429` when the client already has
    /// `per_client` running and [`SEARCH_WAITING_PER_CLIENT`] waiting, `504` when its turn
    /// did not come in time, `503` when its turn came but `concurrency` searches run in
    /// total. A waiting search holds no global slot. Dropping the future while it waits
    /// leaves the queue.
    #[allow(clippy::result_large_err)] // the Err is the finished answer
    pub async fn acquire(
        self: &Arc<Self>,
        ip: IpAddr,
        deadline: Instant,
    ) -> Result<SearchPermit, Response> {
        let key = bucket_key(ip);
        let (ticket, slot) = {
            let mut m = self.clients.lock().unwrap_or_else(|e| e.into_inner());
            let e = m
                .entry(key)
                .or_insert_with(|| (Arc::new(Semaphore::new(self.per_client)), 0));
            if e.1 >= self.per_client + SEARCH_WAITING_PER_CLIENT {
                rejected("client_busy");
                return Err(refusal(
                    StatusCode::TOO_MANY_REQUESTS,
                    CLIENT_BUSY,
                    "search_client",
                ));
            }
            e.1 += 1;
            (
                Ticket {
                    gate: self.clone(),
                    key,
                },
                e.0.clone(),
            )
        };
        let until = deadline.checked_sub(WAIT_MARGIN).unwrap_or(deadline);
        let client = match tokio::time::timeout_at(until.into(), slot.acquire_owned()).await {
            Ok(Ok(p)) => p,
            // The semaphore is never closed; a closed one is a refusal like the timeout.
            Ok(Err(_)) | Err(_) => {
                rejected("wait_timeout");
                return Err(refusal(
                    StatusCode::GATEWAY_TIMEOUT,
                    WAIT_TIMEOUT,
                    "search_wait",
                ));
            }
        };
        let Ok(global) = self.global.clone().try_acquire_owned() else {
            rejected("busy");
            return Err(refusal(
                StatusCode::SERVICE_UNAVAILABLE,
                GLOBAL_BUSY,
                "search_global",
            ));
        };
        Ok(SearchPermit {
            _global: global,
            _client: client,
            _ticket: ticket,
        })
    }

    /// Searches running now (tests and status).
    pub fn in_flight(&self) -> usize {
        self.concurrency - self.global.available_permits()
    }

    /// Searches of `ip`'s key running or waiting (tests).
    pub fn queued(&self, ip: IpAddr) -> usize {
        let m = self.clients.lock().unwrap_or_else(|e| e.into_inner());
        m.get(&bucket_key(ip)).map_or(0, |e| e.1)
    }

    /// Client keys with a search running or waiting (tests: no entry outlives its users).
    pub fn clients(&self) -> usize {
        self.clients.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

fn refusal(code: StatusCode, msg: &'static str, reject: &'static str) -> Response {
    let mut r = (
        code,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::RETRY_AFTER, "1"),
        ],
        msg,
    )
        .into_response();
    r.extensions_mut().insert(crate::reqlog::Reject(reject));
    r
}

/// The client address as the rate limiter sees it (peer, or `X-Forwarded-For` behind a
/// trusted proxy). Without connection info (in-process tests) it is the unspecified address.
pub struct ClientIp(pub IpAddr);

impl FromRequestParts<Arc<ApiState>> for ClientIp {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        st: &Arc<ApiState>,
    ) -> Result<Self, Self::Rejection> {
        let ip = match parts.extensions.get::<ConnectInfo<SocketAddr>>() {
            Some(ConnectInfo(peer)) => {
                client_ip(*peer, &parts.headers, &st.settings.trusted_proxies)
            }
            None => IpAddr::from([0u8, 0, 0, 0]),
        };
        Ok(ClientIp(ip))
    }
}

/// The request's deadline, which bounds a search's wait in its client's queue: the guard's
/// [`crate::RunDeadline`], or `api.deadline_ms` from now without one (in-process tests).
pub struct SearchDeadline(pub Instant);

impl FromRequestParts<Arc<ApiState>> for SearchDeadline {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        st: &Arc<ApiState>,
    ) -> Result<Self, Self::Rejection> {
        Ok(SearchDeadline(
            match parts.extensions.get::<crate::RunDeadline>() {
                Some(d) => d.get(),
                None => Instant::now() + st.settings.deadline,
            },
        ))
    }
}

// ---------------------------------------------------------------------------------------
// Patterns

/// Unescaped `%` in a LIKE pattern.
pub fn wildcards(s: &str) -> usize {
    let mut n = 0;
    let mut it = s.chars();
    while let Some(c) = it.next() {
        match c {
            '\\' => {
                it.next();
            }
            '%' => n += 1,
            _ => {}
        }
    }
    n
}

/// Characters of a LIKE pattern other than `%` and `_` (an escaped one counts).
pub fn literal_chars(s: &str) -> usize {
    let mut n = 0;
    let mut it = s.chars();
    while let Some(c) = it.next() {
        match c {
            '\\' => {
                it.next();
                n += 1;
            }
            '%' | '_' => {}
            _ => n += 1,
        }
    }
    n
}

/// The length and wildcard caps every search pattern must pass (`Err`: the 400 text).
pub fn check_pattern(name: &str, v: &str) -> Result<(), String> {
    if v.chars().count() > MAX_PATTERN_CHARS {
        return Err(format!(
            "{name} must be at most {MAX_PATTERN_CHARS} characters"
        ));
    }
    if wildcards(v) > MAX_PATTERN_WILDCARDS {
        return Err(format!(
            "{name} may contain at most {MAX_PATTERN_WILDCARDS} '%' wildcards"
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Code index

/// How often the manifest is re-read (a rebuild swaps the directory; the tail stage moves
/// `built_through_height`).
const RECHECK: Duration = Duration::from_secs(5);

/// Heights the index's `built_through_height` may trail the data tip before the routes stop
/// trusting it alone (the stage commits every `commit_secs`; blocks come every ~30 s per
/// chain). Past it, compat `/txs/search` serves the index hits marked truncated (the bounded
/// scan only when there is none) and native `/search/code` answers `partial: true` (M-1).
pub const LAG_MARGIN: u32 = 20;

/// The index trails the data tip by more than [`LAG_MARGIN`]: bodies first seen above
/// `built_through` may have no doc yet (a failing or backed-off tail stage).
pub fn lagging(tip: Option<u32>, built_through: Option<u32>) -> bool {
    match (tip, built_through) {
        (Some(t), Some(b)) => t.saturating_sub(b) > LAG_MARGIN,
        _ => false,
    }
}

/// The code index the API serves, reopened when a rebuild swapped it in (`built_at`).
pub struct SearchIndex {
    dir: Option<PathBuf>,
    max_candidates: usize,
    cur: ArcSwapOption<Searcher>,
    checked: Mutex<Option<Instant>>,
    /// `built_through_height` of the manifest last read (`u64::MAX`: none).
    built_through: std::sync::atomic::AtomicU64,
    /// `stale_from_height` of the manifest last read (`u64::MAX`: none; M-7).
    stale_from: std::sync::atomic::AtomicU64,
}

impl SearchIndex {
    pub fn new(dir: Option<PathBuf>, max_candidates: usize) -> SearchIndex {
        SearchIndex {
            dir,
            max_candidates: max_candidates.max(1),
            cur: ArcSwapOption::empty(),
            checked: Mutex::new(None),
            built_through: std::sync::atomic::AtomicU64::new(u64::MAX),
            stale_from: std::sync::atomic::AtomicU64::new(u64::MAX),
        }
    }

    /// `built_through_height` of the manifest last read (the searcher's own copy is the one
    /// it was opened with; the tail stage moves it on every commit).
    pub fn built_through(&self) -> Option<u32> {
        u32::try_from(
            self.built_through
                .load(std::sync::atomic::Ordering::Relaxed),
        )
        .ok()
    }

    /// `stale_from_height` of the manifest last read: a `rebuild-range` swap changed content
    /// from there on after the index was built (M-7).
    pub fn stale_from(&self) -> Option<u32> {
        u32::try_from(self.stale_from.load(std::sync::atomic::Ordering::Relaxed)).ok()
    }

    /// The current searcher, None when no index is built. Blocking (file reads): call it on
    /// a read thread.
    pub fn get(&self) -> Option<Arc<Searcher>> {
        let dir = self.dir.as_ref()?;
        let due = {
            let mut c = self.checked.lock().unwrap_or_else(|e| e.into_inner());
            let due = c.is_none_or(|t| t.elapsed() >= RECHECK);
            if due {
                *c = Some(Instant::now());
            }
            due
        };
        if due {
            match SearchManifest::load(dir) {
                Ok(Some(m)) => {
                    self.built_through.store(
                        u64::from(m.built_through_height),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    self.stale_from.store(
                        m.stale_from_height.map_or(u64::MAX, u64::from),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    let metrics = kdx_metrics::get();
                    metrics.search_docs.set(m.docs as i64);
                    metrics
                        .search_built_through_height
                        .set(i64::from(m.built_through_height));
                    let stale = self
                        .cur
                        .load()
                        .as_ref()
                        .is_none_or(|s| s.manifest.built_at != m.built_at);
                    if stale {
                        match Searcher::open(dir, self.max_candidates) {
                            Ok(s) => self.cur.store(s.map(Arc::new)),
                            Err(e) => {
                                tracing::warn!(
                                    error = format!("{e:#}"),
                                    "search: index open failed"
                                );
                                self.cur.store(None);
                            }
                        }
                    }
                }
                Ok(None) => self.cur.store(None),
                Err(e) => {
                    tracing::warn!(error = format!("{e:#}"), "search: manifest unreadable");
                    self.cur.store(None);
                }
            }
        }
        self.cur.load_full()
    }
}

/// True when the index alone answers `p` completely: full mode, or fallback mode where every
/// body the pattern can match is long enough to be indexed (a body containing the pattern
/// has at least its literal characters). Otherwise the compat route scans (S-1, S-4).
pub fn index_covers(m: &SearchManifest, raw: &str) -> bool {
    match m.mode() {
        kdx_search::Mode::Full => true,
        // Literal chars count chars; `min_len` counts bytes (R6A-4): a char is >= 1 byte.
        kdx_search::Mode::Fallback { min_len } => literal_chars(raw) >= min_len as usize,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M-1: an index trailing the tip by more than the margin is not trusted alone.
    #[test]
    fn lagging_index() {
        use super::{lagging, LAG_MARGIN};
        assert!(!lagging(Some(1_000), Some(1_000 - LAG_MARGIN)));
        assert!(lagging(Some(1_000), Some(1_000 - LAG_MARGIN - 1)));
        assert!(!lagging(None, Some(5)));
        assert!(!lagging(Some(5), None));
        assert!(!lagging(Some(5), Some(9)));
    }

    fn soon(ms: u64) -> Instant {
        Instant::now() + Duration::from_millis(ms)
    }

    fn reject_of(r: &Response) -> Option<&'static str> {
        r.extensions().get::<crate::reqlog::Reject>().map(|x| x.0)
    }

    /// Wait (briefly) until `ip`'s key has `n` searches running or waiting.
    async fn queued_to(g: &SearchGate, ip: IpAddr, n: usize) {
        for _ in 0..200 {
            if g.queued(ip) == n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("queued {} != {n}", g.queued(ip));
    }

    /// Two concurrent searches of one client with `per_client = 1`: both succeed, the second
    /// once the first is done (the burst-429 case: stock serves both).
    #[tokio::test]
    async fn second_search_of_a_client_waits_its_turn() {
        let g = Arc::new(SearchGate::new(4, 1));
        let a = IpAddr::from([127, 0, 0, 1]);
        let p1 = g.acquire(a, soon(5_000)).await.ok().unwrap();
        let g2 = g.clone();
        let second = tokio::spawn(async move { g2.acquire(a, soon(5_000)).await.is_ok() });
        queued_to(&g, a, 2).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !second.is_finished(),
            "the second runs only after the first"
        );
        assert_eq!(g.in_flight(), 1, "a waiting search holds no global slot");
        drop(p1);
        assert!(second.await.unwrap());
        assert_eq!(g.in_flight(), 0);
        assert_eq!(g.clients(), 0);
    }

    /// `per_client` running plus `SEARCH_WAITING_PER_CLIENT` waiting; one more is a 429 at
    /// once, another client is unaffected.
    #[tokio::test]
    async fn a_full_client_queue_is_429() {
        let g = Arc::new(SearchGate::new(4, 1));
        let a = IpAddr::from([127, 0, 0, 1]);
        let p1 = g.acquire(a, soon(5_000)).await.ok().unwrap();
        let mut waiters = Vec::new();
        for _ in 0..SEARCH_WAITING_PER_CLIENT {
            let g2 = g.clone();
            waiters.push(tokio::spawn(async move {
                g2.acquire(a, soon(5_000)).await.is_ok()
            }));
        }
        queued_to(&g, a, 1 + SEARCH_WAITING_PER_CLIENT).await;
        let r = g.acquire(a, soon(5_000)).await.err().unwrap();
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(r.headers()[header::RETRY_AFTER], "1");
        assert_eq!(reject_of(&r), Some("search_client"));
        assert_eq!(
            g.queued(a),
            1 + SEARCH_WAITING_PER_CLIENT,
            "the refusal took no place"
        );
        let b = IpAddr::from([127, 0, 0, 2]);
        assert!(g.acquire(b, soon(5_000)).await.is_ok());
        drop(p1);
        // The waiters run one after another (each drops its permit when done).
        for w in waiters {
            assert!(w.await.unwrap());
        }
        assert_eq!(g.clients(), 0);
        assert_eq!(g.in_flight(), 0);
        // IPv6 clients share one queue per /64.
        let x: IpAddr = "2001:db8::1".parse().unwrap();
        let y: IpAddr = "2001:db8::2".parse().unwrap();
        let _px = g.acquire(x, soon(5_000)).await.ok().unwrap();
        assert_eq!(g.queued(y), 1);
    }

    /// A queued search whose turn does not come before the deadline answers 504 (a
    /// `wait_timeout`) and leaves the queue.
    #[tokio::test]
    async fn the_deadline_bounds_the_wait() {
        let g = Arc::new(SearchGate::new(4, 1));
        let a = IpAddr::from([127, 0, 0, 1]);
        let p1 = g.acquire(a, soon(5_000)).await.ok().unwrap();
        let t = Instant::now();
        let r = g.acquire(a, soon(200)).await.err().unwrap();
        assert!(
            t.elapsed() < Duration::from_millis(1_000),
            "{:?}",
            t.elapsed()
        );
        assert!(
            t.elapsed() >= Duration::from_millis(100),
            "{:?}",
            t.elapsed()
        );
        assert_eq!(r.status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(r.headers()[header::RETRY_AFTER], "1");
        assert_eq!(reject_of(&r), Some("search_wait"));
        assert_eq!(g.queued(a), 1);
        // A deadline already past refuses without waiting.
        let r = g.acquire(a, Instant::now()).await.err().unwrap();
        assert_eq!(r.status(), StatusCode::GATEWAY_TIMEOUT);
        drop(p1);
        assert_eq!(g.clients(), 0);
    }

    /// `search.concurrency` still bounds the searches running over every client; a search
    /// whose turn comes while it is full answers 503 and frees its client's place.
    #[tokio::test]
    async fn the_global_limit_still_holds() {
        let g = Arc::new(SearchGate::new(2, 1));
        let a = IpAddr::from([127, 0, 0, 1]);
        let b = IpAddr::from([127, 0, 0, 2]);
        let c = IpAddr::from([127, 0, 0, 3]);
        let pa = g.acquire(a, soon(5_000)).await.ok().unwrap();
        let g2 = g.clone();
        let a2 = tokio::spawn(async move { g2.acquire(a, soon(5_000)).await.ok() });
        queued_to(&g, a, 2).await;
        // The waiting search of `a` holds no global slot: `b` still gets the second one.
        let pb = g.acquire(b, soon(5_000)).await.ok().unwrap();
        assert_eq!(g.in_flight(), 2);
        let r = g.acquire(c, soon(5_000)).await.err().unwrap();
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(reject_of(&r), Some("search_global"));
        assert_eq!(
            g.queued(c),
            0,
            "a refused global slot leaves no client count behind"
        );
        // `a`'s turn comes when `pa` ends, and it takes the freed global slot.
        drop(pa);
        let pa2 = a2.await.unwrap().expect("a's second search runs");
        // Still two at most: `a`'s second search and `b`'s run; a third client is refused.
        assert_eq!(g.in_flight(), 2);
        assert!(g.acquire(c, soon(5_000)).await.is_err());
        drop((pa2, pb));
        assert_eq!(g.in_flight(), 0);
        assert_eq!(g.clients(), 0);
    }

    /// A waiter that is cancelled (its request dropped: client gone, listener deadline) or
    /// aborted leaves the queue; nothing leaks.
    #[tokio::test]
    async fn a_dropped_waiter_leaks_no_slot() {
        let g = Arc::new(SearchGate::new(4, 1));
        let a = IpAddr::from([127, 0, 0, 1]);
        let p1 = g.acquire(a, soon(5_000)).await.ok().unwrap();
        // Dropped by a timeout around it.
        let r = tokio::time::timeout(Duration::from_millis(50), g.acquire(a, soon(5_000))).await;
        assert!(r.is_err());
        assert_eq!(g.queued(a), 1);
        // Aborted task.
        let g2 = g.clone();
        let w = tokio::spawn(async move { g2.acquire(a, soon(5_000)).await.is_ok() });
        queued_to(&g, a, 2).await;
        w.abort();
        assert!(w.await.is_err());
        queued_to(&g, a, 1).await;
        drop(p1);
        assert_eq!(g.clients(), 0);
        assert_eq!(g.in_flight(), 0);
        // The client's queue is whole again: per_client + waiting fit.
        let p = g.acquire(a, soon(5_000)).await.ok().unwrap();
        let mut ws = Vec::new();
        for _ in 0..SEARCH_WAITING_PER_CLIENT {
            let g2 = g.clone();
            ws.push(tokio::spawn(async move {
                g2.acquire(a, soon(5_000)).await.is_ok()
            }));
        }
        queued_to(&g, a, 1 + SEARCH_WAITING_PER_CLIENT).await;
        drop(p);
        for w in ws {
            assert!(w.await.unwrap());
        }
        assert_eq!(g.clients(), 0);
        assert_eq!(g.in_flight(), 0);
    }

    #[test]
    fn pattern_caps() {
        assert!(check_pattern("search", &"a".repeat(MAX_PATTERN_CHARS)).is_ok());
        assert!(check_pattern("search", &"a".repeat(MAX_PATTERN_CHARS + 1)).is_err());
        assert!(check_pattern("search", &"%a".repeat(MAX_PATTERN_WILDCARDS + 1)).is_err());
        assert!(check_pattern("search", &"\\%a".repeat(MAX_PATTERN_WILDCARDS + 1)).is_ok());
        assert_eq!(literal_chars("ab%c_\\%"), 4);
    }
}
