//! Search limits and the code index handle (spec §3.8, plan Task 10, rulings S-4 and S-5).
//!
//! One [`SearchGate`] covers every search route: `/txs/search?search`, `/txs/events?search`
//! and the native `/search/code` and `/search/events`. A global semaphore of
//! `search.concurrency` permits answers `503` when full; a per-client count (keyed like the
//! rate limiter: the IPv4 address or the IPv6 /64) of `search.per_client` answers `429`. The
//! permit moves into the blocking read and is dropped when that read ends: on completion, on
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

/// Count a refusal or a cut answer (`too_short|busy|client_busy|timeout|cap|lag`).
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

pub struct SearchGate {
    global: Arc<Semaphore>,
    per_client: usize,
    clients: Mutex<HashMap<IpAddr, usize>>,
}

/// One running search. Dropping it frees the global slot and the client's slot.
pub struct SearchPermit {
    _global: OwnedSemaphorePermit,
    gate: Arc<SearchGate>,
    key: IpAddr,
}

impl Drop for SearchPermit {
    fn drop(&mut self) {
        let mut m = self.gate.clients.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(n) = m.get_mut(&self.key) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                m.remove(&self.key);
            }
        }
    }
}

impl SearchGate {
    pub fn new(concurrency: usize, per_client: usize) -> SearchGate {
        SearchGate {
            global: Arc::new(Semaphore::new(concurrency.max(1))),
            per_client: per_client.max(1),
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// A permit for `ip`, or the finished refusal: `429` when the client already runs
    /// `per_client` searches, `503` when `concurrency` searches run in total.
    #[allow(clippy::result_large_err)] // the Err is the finished answer
    pub fn try_acquire(self: &Arc<Self>, ip: IpAddr) -> Result<SearchPermit, Response> {
        let key = bucket_key(ip);
        let mut m = self.clients.lock().unwrap_or_else(|e| e.into_inner());
        let n = m.get(&key).copied().unwrap_or(0);
        if n >= self.per_client {
            rejected("client_busy");
            return Err(refusal(
                StatusCode::TOO_MANY_REQUESTS,
                "a search from this client is already running: retry when it is done",
            ));
        }
        let Ok(global) = self.global.clone().try_acquire_owned() else {
            rejected("busy");
            return Err(refusal(
                StatusCode::SERVICE_UNAVAILABLE,
                "too many searches in flight: retry shortly",
            ));
        };
        m.insert(key, n + 1);
        Ok(SearchPermit {
            _global: global,
            gate: self.clone(),
            key,
        })
    }

    /// Searches running now (tests and status).
    pub fn in_flight(&self) -> usize {
        self.clients
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .sum()
    }
}

fn refusal(code: StatusCode, msg: &'static str) -> Response {
    (
        code,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::RETRY_AFTER, "1"),
        ],
        msg,
    )
        .into_response()
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
/// chain). Past it, compat `/txs/search` runs the bounded scan (exact) and native
/// `/search/code` answers `partial: true` (M-1).
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

    #[test]
    fn gate_per_client_then_global() {
        let g = Arc::new(SearchGate::new(2, 1));
        let a = IpAddr::from([127, 0, 0, 1]);
        let b = IpAddr::from([127, 0, 0, 2]);
        let c = IpAddr::from([127, 0, 0, 3]);
        let pa = g.try_acquire(a).ok().unwrap();
        assert_eq!(
            g.try_acquire(a).err().unwrap().status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        let pb = g.try_acquire(b).ok().unwrap();
        assert_eq!(
            g.try_acquire(c).err().unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        // A refused global slot leaves no client count behind.
        assert_eq!(g.in_flight(), 2);
        drop(pa);
        assert!(g.try_acquire(a).is_ok());
        drop(pb);
        assert_eq!(g.in_flight(), 0);
        // IPv6 clients share one count per /64.
        let x: IpAddr = "2001:db8::1".parse().unwrap();
        let y: IpAddr = "2001:db8::2".parse().unwrap();
        let _px = g.try_acquire(x).ok().unwrap();
        assert!(g.try_acquire(y).is_err());
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
