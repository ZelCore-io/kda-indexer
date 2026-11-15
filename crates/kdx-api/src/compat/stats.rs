//! `/stats` (chainweb-data `statsHandler`): `{"transactionCount":N,"coinsInCirculation":X}`.
//!
//! chainweb-data answers a Postgres row estimate; this is the exact user-tx count of the
//! served store, genesis txs included (chainweb-data's transactions table holds them, G1).
//! `X` is the circulating supply at the highest stored height and the current time, on
//! mainnet01 only (the supply tables are mainnet's, R9); `null` on other networks. The key order is the
//! record's field order, not alphabetical (chainweb-data encodes the record directly).
//!
//! The count walks every hot tx key, so a request never makes it (R-P5P9-75): a background
//! task ([`run_refresh`], started and stopped with the server) recounts every
//! [`REFRESH_EVERY`], and a request answers the last count with its age in `x-count-age`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use kdx_hot::{Table, Txn};
use kdx_types::TxKey;
use tokio_util::sync::CancellationToken;

use super::{shapes, text};
use crate::server::ApiState;
use crate::store::View;
use crate::{ApiError, ApiResult};

/// The background count runs this often.
pub const REFRESH_EVERY: Duration = Duration::from_secs(120);
/// A refresh may take this long (cold pages: 5E saw a walk of the hot window pass 5 s, the
/// request deadline). A slower one fails and the last count stays.
const REFRESH_BUDGET: Duration = Duration::from_secs(60);
/// A failing refresh logs one WARN per this long.
const WARN_EVERY: Duration = Duration::from_secs(10 * 60);
/// `Retry-After` (seconds) of the `503` before the first count exists.
const RETRY_AFTER_SECS: &str = "5";

/// Header on every `/stats` answer: the age of the count in whole seconds.
pub const COUNT_AGE: HeaderName = HeaderName::from_static("x-count-age");

/// A user-tx count as `/stats` answers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxCount {
    pub n: u64,
    /// How long ago the count finished.
    pub age: Duration,
}

/// Exact user-tx count of what `view` serves: frozen ranges (`tx_count - block_count`,
/// one coinbase per block) plus the hot rows at or above the split.
pub fn user_tx_count(view: &View) -> ApiResult<u64> {
    let frozen: u64 = view
        .seg
        .manifest
        .entries
        .iter()
        .map(|e| e.tx_count.saturating_sub(e.block_count))
        .sum();
    let lo = TxKey::new(view.hot_floor(), 0, 0).to_be_bytes();
    let mut hot = 0u64;
    let mut seen = 0u64;
    let mut late = view.check_deadline().is_err();
    if !late {
        // Keys only: the hot window can hold hundreds of thousands of txs.
        view.txn.walk_keys(Table::TailTxs, &lo, |k| {
            seen += 1;
            if seen.is_multiple_of(4096) && view.check_deadline().is_err() {
                late = true;
                return false;
            }
            if let Ok(b) = <[u8; 8]>::try_from(k) {
                if !TxKey::from_be_bytes(b).is_coinbase() {
                    hot += 1;
                }
            }
            true
        })?;
    }
    if late {
        return Err(ApiError::Deadline);
    }
    Ok(frozen + hot)
}

/// The last counted value. Written by the refresh task only; no lock is held while counting.
pub(crate) struct CountCache {
    last: Mutex<Option<(Instant, u64)>>,
    /// Wakes requests that wait for the first count.
    first: tokio::sync::Notify,
    /// When the last refresh failure was logged.
    warned: Mutex<Option<Instant>>,
    counter: fn(&View) -> ApiResult<u64>,
}

/// The process-wide cache ([`crate::ApiState`] holds it for `/stats`; [`stats`] reads it).
pub(crate) static CACHE: CountCache = CountCache::new();

impl CountCache {
    const fn new() -> CountCache {
        CountCache::with_counter(user_tx_count)
    }

    /// A cache whose refresh counts with `counter` (tests inject a slow or failing one).
    const fn with_counter(counter: fn(&View) -> ApiResult<u64>) -> CountCache {
        CountCache {
            last: Mutex::new(None),
            first: tokio::sync::Notify::const_new(),
            warned: Mutex::new(None),
            counter,
        }
    }

    /// The last counted value with its age at `now`; None before the first count.
    fn peek(&self, now: Instant) -> Option<TxCount> {
        let (at, n) = (*self.last.lock().unwrap_or_else(|e| e.into_inner()))?;
        Some(TxCount {
            n,
            age: now.saturating_duration_since(at),
        })
    }

    fn store(&self, n: u64, at: Instant) {
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some((at, n));
        self.first.notify_waiters();
    }

    /// The count, waiting until `until` for the first one; None when there is still none.
    async fn wait_for_count(&self, until: Instant) -> Option<TxCount> {
        loop {
            // Arm the wakeup before looking, so a count stored in between is not missed.
            let woken = self.first.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            if let Some(c) = self.peek(Instant::now()) {
                return Some(c);
            }
            if tokio::time::timeout_at(until.into(), woken).await.is_err() {
                return self.peek(Instant::now());
            }
        }
    }

    /// True once per [`WARN_EVERY`].
    fn warn_due(&self, now: Instant) -> bool {
        let mut w = self.warned.lock().unwrap_or_else(|e| e.into_inner());
        if w.is_some_and(|at| now.saturating_duration_since(at) < WARN_EVERY) {
            return false;
        }
        *w = Some(now);
        true
    }

    /// One recount. A failed or slow one (past [`REFRESH_BUDGET`]) keeps the last count.
    async fn refresh(&self, st: &Arc<ApiState>) {
        match st.read_for(REFRESH_BUDGET, self.counter).await {
            Ok(n) => self.store(n, Instant::now()),
            Err(e) => {
                let now = Instant::now();
                if self.warn_due(now) {
                    let age = self.peek(now).map(|c| c.age.as_secs());
                    tracing::warn!(
                        error = %e,
                        count_age_secs = ?age,
                        "stats: count refresh failed; the last count stays"
                    );
                }
            }
        }
    }
}

/// Recount every [`REFRESH_EVERY`] until `shutdown`; `serve` starts it.
pub(crate) async fn run_refresh(st: Arc<ApiState>, shutdown: CancellationToken) {
    refresh_every(st, REFRESH_EVERY, shutdown).await;
}

async fn refresh_every(st: Arc<ApiState>, every: Duration, shutdown: CancellationToken) {
    let cache = st.stats_cache;
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = cache.refresh(&st) => {}
        }
    }
}

/// Network whose supply tables `crate::supply` holds.
const SUPPLY_NETWORK: &str = "mainnet01";

/// The circulating supply at the highest stored height: `Some(coins)` (`coins` None off
/// mainnet), or None on an empty store.
fn supply(view: &View) -> ApiResult<Option<Option<f64>>> {
    let Some(h) = view.max_indexed_height()? else {
        return Ok(None);
    };
    Ok(Some((view.genesis.network == SUPPLY_NETWORK).then(|| {
        crate::supply::circulating(u64::from(h), SystemTime::now())
    })))
}

/// `(user tx count, coins in circulation)` as `/stats` answers them (GraphQL `networkInfo`
/// reads the same): the last refreshed count, never a recount. Unavailable before the first
/// count (startup).
pub fn stats(view: &View) -> ApiResult<Option<(u64, Option<f64>)>> {
    let Some(coins) = supply(view)? else {
        return Ok(None);
    };
    let c = CACHE
        .peek(Instant::now())
        .ok_or_else(|| ApiError::Unavailable("the transaction count is not ready yet".into()))?;
    Ok(Some((c.n, coins)))
}

pub async fn handler(State(st): State<Arc<ApiState>>) -> Response {
    let until = Instant::now() + st.settings.deadline;
    let cache = st.stats_cache;
    let coins = match st.read(supply).await {
        Ok(Some(coins)) => coins,
        Ok(None) => return text(StatusCode::NOT_FOUND, "Server database is empty"),
        Err(e) => return e.into_response(),
    };
    let Some(count) = cache.wait_for_count(until).await else {
        let mut resp = text(
            StatusCode::SERVICE_UNAVAILABLE,
            "the transaction count is not ready yet",
        );
        resp.headers_mut().insert(
            header::RETRY_AFTER,
            HeaderValue::from_static(RETRY_AFTER_SECS),
        );
        return resp;
    };
    let mut b = format!("{{\"transactionCount\":{},\"coinsInCirculation\":", count.n).into_bytes();
    match coins {
        Some(c) => shapes::write_aeson(&shapes::aeson_double(c), &mut b),
        None => b.extend_from_slice(b"null"),
    }
    b.push(b'}');
    let mut resp = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json;charset=utf-8")],
        Body::from(b),
    )
        .into_response();
    resp.headers_mut()
        .insert(COUNT_AGE, HeaderValue::from(count.age.as_secs()));
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn count_checks_the_deadline() {
        let (_d, store) = crate::testutil::store();
        let view = store.view(Instant::now() - Duration::from_secs(1)).unwrap();
        assert!(matches!(user_tx_count(&view), Err(ApiError::Deadline)));
    }

    /// 2: the age grows with the clock while no refresh succeeds, and a refresh resets it.
    #[test]
    fn the_age_grows_until_a_refresh_stores_a_new_count() {
        let c = CountCache::new();
        let t0 = Instant::now();
        assert_eq!(c.peek(t0), None);
        c.store(5, t0);
        let age = |d: u64| c.peek(t0 + Duration::from_secs(d)).unwrap().age.as_secs();
        assert_eq!((age(0), age(10), age(130), age(3600)), (0, 10, 130, 3600));
        c.store(6, t0 + Duration::from_secs(120));
        let again = c.peek(t0 + Duration::from_secs(130)).unwrap();
        assert_eq!((again.n, again.age.as_secs()), (6, 10));
    }

    /// One WARN per `WARN_EVERY`.
    #[test]
    fn a_failing_refresh_warns_once_per_window() {
        let c = CountCache::new();
        let t0 = Instant::now();
        let due = |d: Duration| c.warn_due(t0 + d);
        assert!(due(Duration::ZERO));
        assert!(!due(Duration::from_secs(1)));
        assert!(!due(WARN_EVERY - Duration::from_secs(1)));
        assert!(due(WARN_EVERY));
        assert!(!due(WARN_EVERY + Duration::from_secs(1)));
    }

    static CALLS: AtomicUsize = AtomicUsize::new(0);

    fn counted(_: &View) -> ApiResult<u64> {
        Ok(CALLS.fetch_add(1, Ordering::SeqCst) as u64 + 1)
    }

    fn failing(_: &View) -> ApiResult<u64> {
        Err(ApiError::Deadline)
    }

    /// 4: the refresh task counts at its interval and stops on shutdown.
    #[tokio::test]
    async fn the_refresh_task_stops_on_shutdown() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::with_counter(counted)));
        let (_d, _app, st) = app(cache);
        let stop = CancellationToken::new();
        let task = tokio::spawn(refresh_every(st, Duration::from_millis(20), stop.clone()));
        tokio::time::timeout(Duration::from_secs(5), async {
            while CALLS.load(Ordering::SeqCst) < 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the task keeps refreshing");
        assert!(cache.peek(Instant::now()).is_some());
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the task stops on shutdown")
            .unwrap();
        let after = CALLS.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            CALLS.load(Ordering::SeqCst),
            after,
            "counted after shutdown"
        );
    }

    /// A failed refresh keeps the last count; the first one stores it and wakes a waiting
    /// request.
    #[tokio::test]
    async fn a_failed_refresh_keeps_the_last_count() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::with_counter(failing)));
        let (_d, _app, st) = app(cache);
        cache.store(7, Instant::now() - Duration::from_secs(300));
        cache.refresh(&st).await;
        let c = cache.peek(Instant::now()).unwrap();
        assert_eq!(c.n, 7);
        assert!(c.age >= Duration::from_secs(300));
    }

    /// A request waiting for the first count is answered when the refresh stores it.
    #[tokio::test]
    async fn a_waiting_request_gets_the_first_count() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::new()));
        let (_d, app, _st) = app(cache);
        let t = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cache.store(11, Instant::now());
        });
        let (r, took) = get(&app).await;
        t.await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert!(took < Duration::from_millis(250), "{took:?}");
        assert_eq!(
            body(r).await,
            "{\"transactionCount\":11,\"coinsInCirculation\":null}"
        );
    }

    // ---- STATS-FIX (R-P5P9-75): the count is refreshed in the background ----

    fn slow_count(_: &View) -> ApiResult<u64> {
        std::thread::sleep(Duration::from_millis(1500));
        Ok(99)
    }

    /// The compat router over a store with a checkpoint, a 300 ms deadline and `cache`.
    fn app(
        cache: &'static CountCache,
    ) -> (tempfile::TempDir, axum::Router, std::sync::Arc<ApiState>) {
        let (d, store) = crate::testutil::store();
        let mut w = store.hot_for_tests().rw().unwrap();
        kdx_hot::set_checkpoint(
            &mut w,
            kdx_hot::Stage::Tail,
            0,
            10,
            kdx_types::Hash32([1; 32]),
        )
        .unwrap();
        w.commit().unwrap();
        let settings = crate::ApiSettings {
            network: "testnet04".into(),
            deadline: Duration::from_millis(300),
            max_in_flight: 16,
            max_body_bytes: 1 << 20,
            v1txs_max_body_bytes: 1 << 20,
            rate_per_ip: 1000,
            rate_burst: 1000,
            trusted_proxies: Vec::new(),
            read_threads: 2,
            ready_max_lag: 10,
            max_event_lists: 64,
        };
        let st = crate::Api::new(store, settings, crate::TipWatch::default())
            .with_stats_cache(cache)
            .state();
        let search = crate::search::SearchCtx::new(super::super::SearchSettings::default(), None);
        (d, super::super::router(search).with_state(st.clone()), st)
    }

    async fn get(app: &axum::Router) -> (Response, Duration) {
        use axum::http::Request;
        use tower::ServiceExt;
        let t = Instant::now();
        let r = app
            .clone()
            .oneshot(Request::get("/stats").body(Body::empty()).unwrap())
            .await
            .unwrap();
        (r, t.elapsed())
    }

    async fn body(r: Response) -> String {
        let b = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
        String::from_utf8(b.to_vec()).unwrap()
    }

    /// 1: a count slower than the deadline never holds a request once a count exists: the
    /// request answers 200 at once with the previous count and its age.
    #[tokio::test]
    async fn a_slow_count_does_not_block_a_request_after_the_first_count() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::with_counter(slow_count)));
        *cache.last.lock().unwrap() = Some((Instant::now() - Duration::from_secs(60), 7));
        let (_d, app, _st) = app(cache);
        let (r, took) = get(&app).await;
        assert_eq!(r.status(), StatusCode::OK);
        assert!(took < Duration::from_millis(250), "{took:?}");
        let age: u64 = r.headers()[COUNT_AGE].to_str().unwrap().parse().unwrap();
        assert!((60..65).contains(&age), "{age}");
        assert_eq!(
            body(r).await,
            "{\"transactionCount\":7,\"coinsInCirculation\":null}"
        );
    }

    /// 2: the header is on every answer, a fresh count included.
    #[tokio::test]
    async fn the_age_header_is_on_a_fresh_count_too() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::new()));
        *cache.last.lock().unwrap() = Some((Instant::now(), 3));
        let (_d, app, _st) = app(cache);
        let (r, _) = get(&app).await;
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.headers()[COUNT_AGE], "0");
    }

    /// 3: at startup, with no count yet and a slow count, the request waits the deadline and
    /// answers 503 with `Retry-After`.
    #[tokio::test]
    async fn startup_without_a_count_answers_503_with_retry_after() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::with_counter(slow_count)));
        let (_d, app, _st) = app(cache);
        let (r, took) = get(&app).await;
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(r.headers().contains_key(header::RETRY_AFTER));
        assert!(took < Duration::from_millis(1200), "{took:?}");
    }
}
