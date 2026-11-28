//! `/stats` (chainweb-data `statsHandler`): `{"transactionCount":N,"coinsInCirculation":X}`.
//!
//! chainweb-data answers a Postgres row estimate; this is the exact user-tx count of the
//! served store, genesis txs included (chainweb-data's transactions table holds them, G1).
//! `X` is the circulating supply at the highest stored height and the current time, on
//! mainnet01 only (the supply tables are mainnet's, R9); `null` on other networks. The key order is the
//! record's field order, not alphabetical (chainweb-data encodes the record directly).
//!
//! The count walks every hot tx key, so a request never makes it (R-P5P9-75): a background
//! task ([`run_refresh`], started and stopped with the server) recounts [`REFRESH_EVERY`]
//! after each count ended, and a request answers the last count with its age in `x-count-age`
//! (counted from the start of that count).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use kdx_hot::{Table, Txn};
use kdx_types::TxKey;
use tokio_util::sync::CancellationToken;

use super::schemas::{LimitErrors, Stats};
use super::{shapes, text};
use crate::server::ApiState;
use crate::store::View;
use crate::{ApiError, ApiResult};

/// The background count starts this long after the previous one ended.
pub const REFRESH_EVERY: Duration = Duration::from_secs(120);
/// A refresh that takes longer than this logs one WARN with its duration (5E saw a walk of
/// the hot window pass 5 s, the request deadline). It still runs to completion.
const SLOW_REFRESH: Duration = Duration::from_secs(60);
/// The refresh's read has no practical deadline: the count always finishes.
const NO_DEADLINE: Duration = Duration::from_secs(365 * 24 * 3600);
/// A failing refresh logs one WARN per this long.
const WARN_EVERY: Duration = Duration::from_secs(10 * 60);
/// `Retry-After` (seconds) of the `503` before the first count exists.
const RETRY_AFTER_SECS: &str = "5";
/// While no count exists, a failed refresh is retried after this long (the `Retry-After`).
const FIRST_RETRY: Duration = Duration::from_secs(5);
/// The count is stale after this many refresh intervals; a request then logs a WARN.
const STALE_AFTER_INTERVALS: u32 = 3;

/// Header on every `/stats` 200: the age of the count in whole seconds.
pub const COUNT_AGE: HeaderName = HeaderName::from_static("x-count-age");

/// A user-tx count as `/stats` answers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxCount {
    pub n: u64,
    /// How long ago the count started (its snapshot is that old).
    pub age: Duration,
}

/// Exact user-tx count of what `view` serves: frozen ranges (`tx_count - block_count`,
/// one coinbase per block) plus the hot rows at or above the split. The walk stops with an
/// error at its next check (every 4096 keys) once `stop` is cancelled (R-P5P9-76).
pub fn user_tx_count(view: &View, stop: &CancellationToken) -> ApiResult<u64> {
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
    let mut stopped = stop.is_cancelled();
    if !late && !stopped {
        // Keys only: the hot window can hold hundreds of thousands of txs.
        view.txn.walk_keys(Table::TailTxs, &lo, |k| {
            seen += 1;
            if seen.is_multiple_of(4096) {
                if stop.is_cancelled() {
                    stopped = true;
                    return false;
                }
                if view.check_deadline().is_err() {
                    late = true;
                    return false;
                }
            }
            if let Ok(b) = <[u8; 8]>::try_from(k) {
                if !TxKey::from_be_bytes(b).is_coinbase() {
                    hot += 1;
                }
            }
            true
        })?;
    }
    if stopped {
        return Err(ApiError::Unavailable(
            "the count was stopped by shutdown".into(),
        ));
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
    /// When a request last logged that the count is stale.
    stale_warned: Mutex<Option<Instant>>,
    counter: fn(&View, &CancellationToken) -> ApiResult<u64>,
}

/// The process-wide cache ([`crate::ApiState`] holds it for `/stats`; [`stats`] reads it).
pub(crate) static CACHE: CountCache = CountCache::new();

impl CountCache {
    const fn new() -> CountCache {
        CountCache::with_counter(user_tx_count)
    }

    /// A cache whose refresh counts with `counter` (tests inject a slow or failing one).
    const fn with_counter(counter: fn(&View, &CancellationToken) -> ApiResult<u64>) -> CountCache {
        CountCache {
            last: Mutex::new(None),
            first: tokio::sync::Notify::const_new(),
            warned: Mutex::new(None),
            stale_warned: Mutex::new(None),
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

    /// True once per [`WARN_EVERY`] (a refresh failure).
    fn warn_due(&self, now: Instant) -> bool {
        Self::once_per_window(&self.warned, now)
    }

    /// True once per [`WARN_EVERY`] while a count of `age` is older than
    /// [`STALE_AFTER_INTERVALS`] refresh intervals: a refresh that never ends logs nothing
    /// itself (M-5).
    fn stale_warn_due(&self, age: Duration, now: Instant) -> bool {
        age > REFRESH_EVERY * STALE_AFTER_INTERVALS
            && Self::once_per_window(&self.stale_warned, now)
    }

    fn once_per_window(slot: &Mutex<Option<Instant>>, now: Instant) -> bool {
        let mut w = slot.lock().unwrap_or_else(|e| e.into_inner());
        if w.is_some_and(|at| now.saturating_duration_since(at) < WARN_EVERY) {
            return false;
        }
        *w = Some(now);
        true
    }

    /// One recount, run to completion unless `stop` is cancelled (the walk then ends at its
    /// next check); a failure keeps the last count. A count that took longer than
    /// `slow_after` logs one WARN with its duration; true then.
    async fn refresh(
        &self,
        st: &Arc<ApiState>,
        slow_after: Duration,
        stop: &CancellationToken,
    ) -> bool {
        let started = Instant::now();
        let (counter, stop) = (self.counter, stop.clone());
        let result = st.read_for(NO_DEADLINE, move |v| counter(v, &stop)).await;
        let took = started.elapsed();
        let slow = took > slow_after;
        match result {
            Ok(n) => self.store(n, started),
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
        if slow {
            tracing::warn!(
                took_secs = took.as_secs_f64(),
                "stats: count refresh was slow (it finished; requests answered the last count meanwhile)"
            );
        }
        slow
    }
}

/// Recount every [`REFRESH_EVERY`] until `shutdown`; `serve` starts it.
pub(crate) async fn run_refresh(st: Arc<ApiState>, shutdown: CancellationToken) {
    refresh_every(st, REFRESH_EVERY, FIRST_RETRY, shutdown).await;
}

/// Refresh, wait `every` after it ends (`retry` while no count exists yet), repeat: two
/// refreshes never overlap.
async fn refresh_every(
    st: Arc<ApiState>,
    every: Duration,
    retry: Duration,
    shutdown: CancellationToken,
) {
    let cache = st.stats_cache;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = cache.refresh(&st, SLOW_REFRESH, &shutdown) => {}
        }
        let wait = if cache.peek(Instant::now()).is_some() {
            every
        } else {
            retry
        };
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(wait) => {}
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

/// When a `/stats` request stops waiting for the first count: a margin (250 ms, or a
/// tenth of a short deadline) before the request guard's own timeout of the same length,
/// so the answer is this handler's 503 with `Retry-After`, not the guard's 504 (F-22).
fn wait_until(start: Instant, deadline: Duration) -> Instant {
    let margin = Duration::from_millis(250).min(deadline / 10);
    start + deadline - margin
}

/// Transaction count and circulating supply
///
/// chainweb-data `statsHandler`: `{"transactionCount": N, "coinsInCirculation": X}`, the
/// exact user-transaction count. A background task recounts, each count starting two
/// minutes after the previous one ended and running to completion; a request never
/// recounts and answers the last count with `X-Count-Age` (seconds since that count
/// started; every 200 has it). Before the first count exists the request waits up to the
/// deadline, then answers `503` with `Retry-After: 5`. A failed recount keeps the last
/// count and its age grows. `coinsInCirculation` is mainnet01's supply table; other
/// networks answer `404 Server database is empty`.
#[utoipa::path(
    get,
    path = "/stats",
    operation_id = "compat_stats",
    tag = "network",
    responses(
        (status = 200, description = "Counts", body = Stats, content_type = "application/json",
            headers(("X-Count-Age" = u64, description = "Seconds since the count started; readable by browsers (CORS exposed)."))),
        (status = 404, description = "`Server database is empty` (empty store, or a network other than mainnet01)", body = String, content_type = "text/plain"),
        LimitErrors,
    )
)]
pub async fn handler(State(st): State<Arc<ApiState>>) -> Response {
    let until = wait_until(Instant::now(), st.settings.deadline);
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
    if cache.stale_warn_due(count.age, Instant::now()) {
        tracing::warn!(
            count_age_secs = count.age.as_secs(),
            "stats: the count is stale (no refresh has finished for over {STALE_AFTER_INTERVALS} intervals); \
             a count may be stuck or failing"
        );
    }
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
        assert!(matches!(
            user_tx_count(&view, &CancellationToken::new()),
            Err(ApiError::Deadline)
        ));
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

    fn counted(_: &View, _: &CancellationToken) -> ApiResult<u64> {
        Ok(CALLS.fetch_add(1, Ordering::SeqCst) as u64 + 1)
    }

    fn failing(_: &View, _: &CancellationToken) -> ApiResult<u64> {
        Err(ApiError::Deadline)
    }

    /// 4: the refresh task counts at its interval and stops on shutdown.
    #[tokio::test]
    async fn the_refresh_task_stops_on_shutdown() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::with_counter(counted)));
        let (_d, _app, st) = app(cache);
        let stop = CancellationToken::new();
        let task = tokio::spawn(refresh_every(
            st,
            Duration::from_millis(20),
            Duration::from_millis(20),
            stop.clone(),
        ));
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
        // One count may already be submitted to the blocking pool when the cancel lands.
        let now = CALLS.load(Ordering::SeqCst);
        assert!(now <= after + 1, "counted after shutdown: {after} -> {now}");
    }

    /// A failed refresh keeps the last count; the first one stores it and wakes a waiting
    /// request.
    #[tokio::test]
    async fn a_failed_refresh_keeps_the_last_count() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::with_counter(failing)));
        let (_d, _app, st) = app(cache);
        cache.store(7, Instant::now() - Duration::from_secs(300));
        cache
            .refresh(&st, SLOW_REFRESH, &CancellationToken::new())
            .await;
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

    /// I-1: a cancelled token ends the walk with an error, not a count.
    #[test]
    fn a_cancelled_token_stops_the_walk() {
        let (_d, store) = crate::testutil::store();
        let view = store
            .view(Instant::now() + Duration::from_secs(60))
            .unwrap();
        assert!(user_tx_count(&view, &CancellationToken::new()).is_ok());
        let stop = CancellationToken::new();
        stop.cancel();
        assert!(matches!(
            user_tx_count(&view, &stop),
            Err(ApiError::Unavailable(_))
        ));
    }

    static STARTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

    /// A 3 s count that checks the token every 5 ms, like the real walk does.
    fn stoppable_count(_: &View, stop: &CancellationToken) -> ApiResult<u64> {
        STARTED.store(true, Ordering::SeqCst);
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(3) {
            if stop.is_cancelled() {
                return Err(ApiError::Unavailable("stopped".into()));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok(1)
    }

    /// I-1: shutdown while a count runs: the runtime drop (which waits for blocking work, as
    /// `#[tokio::main]` does at exit) returns within one check interval, not after the walk.
    #[test]
    fn a_count_in_progress_is_abandoned_on_shutdown() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let cache: &'static CountCache =
            Box::leak(Box::new(CountCache::with_counter(stoppable_count)));
        let stop = CancellationToken::new();
        let (_d, _app, task) = rt.block_on(async {
            let (d, app, st) = app(cache);
            let task = tokio::spawn(refresh_every(
                st,
                Duration::from_secs(60),
                Duration::from_secs(60),
                stop.clone(),
            ));
            (d, app, task)
        });
        rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !STARTED.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("the count starts");
            stop.cancel();
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .expect("the task stops on shutdown")
                .unwrap();
        });
        let t = Instant::now();
        drop(rt);
        let took = t.elapsed();
        assert!(
            took < Duration::from_millis(500),
            "runtime drop took {took:?}"
        );
        assert!(
            cache.peek(Instant::now()).is_none(),
            "an abandoned count is not stored"
        );
    }

    // ---- STATS-FIX (R-P5P9-75): the count is refreshed in the background ----

    fn slow_count(_: &View, _: &CancellationToken) -> ApiResult<u64> {
        std::thread::sleep(Duration::from_millis(1500));
        Ok(99)
    }

    /// The compat router over a store with a checkpoint, a 300 ms deadline and `cache`.
    fn app(
        cache: &'static CountCache,
    ) -> (tempfile::TempDir, axum::Router, std::sync::Arc<ApiState>) {
        let (d, store) = seeded_store();
        let st = crate::Api::new(store, test_settings(), crate::TipWatch::default())
            .with_stats_cache(cache)
            .state();
        let search = crate::search::SearchCtx::new(super::super::SearchSettings::default(), None);
        (d, super::super::router(search).with_state(st.clone()), st)
    }

    /// A store with a checkpoint (so `/stats` finds a highest height).
    fn seeded_store() -> (tempfile::TempDir, std::sync::Arc<crate::store::Store>) {
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
        (d, store)
    }

    fn test_settings() -> crate::ApiSettings {
        crate::ApiSettings {
            network: "testnet04".into(),
            deadline: Duration::from_millis(300),
            max_in_flight: 16,
            max_body_bytes: 1 << 20,
            v1txs_max_body_bytes: 1 << 20,
            v1txs_deadline: std::time::Duration::from_secs(30),
            rate_per_ip: 1000,
            rate_burst: 1000,
            trusted_proxies: Vec::new(),
            read_threads: 2,
            ready_max_lag: 10,
            max_event_lists: 64,
            heavy_waiting_per_client: 32,
        }
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

    /// F-22: behind the request guard (whose timeout is the same deadline) the not-ready
    /// answer is still the handler's 503 with `Retry-After`, not the guard's 504.
    #[tokio::test]
    async fn startup_503_is_not_overtaken_by_the_guard() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::with_counter(slow_count)));
        let (_d, store) = seeded_store();
        let st = crate::Api::new(store, test_settings(), crate::TipWatch::default())
            .with_stats_cache(cache)
            .state();
        let search = crate::search::SearchCtx::new(super::super::SearchSettings::default(), None);
        let app = super::super::router(search)
            .layer(axum::middleware::from_fn_with_state(
                st.clone(),
                crate::server::guard,
            ))
            .with_state(st.clone());
        let (r, _) = get(&app).await;
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(r.headers().contains_key(header::RETRY_AFTER));
    }

    /// F-22: the wait ends a margin before the guard's deadline: 250 ms, or a tenth of a
    /// short deadline.
    #[test]
    fn the_wait_ends_before_the_deadline() {
        let t = Instant::now();
        assert_eq!(
            wait_until(t, Duration::from_secs(10)),
            t + Duration::from_millis(9_750)
        );
        assert_eq!(
            wait_until(t, Duration::from_millis(300)),
            t + Duration::from_millis(270)
        );
    }

    /// A counter that honours the deadline like the real one and needs 300 ms.
    fn long_count(v: &View, _: &CancellationToken) -> ApiResult<u64> {
        let t = Instant::now();
        while t.elapsed() < Duration::from_millis(300) {
            v.check_deadline()?;
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok(42)
    }

    /// A count slower than the budget still lands (the budget only warns), and the next
    /// request answers 200 with it.
    #[tokio::test]
    async fn a_count_past_the_budget_still_lands() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::with_counter(long_count)));
        let (_d, app, st) = app(cache);
        assert!(
            cache
                .refresh(&st, Duration::from_millis(100), &CancellationToken::new())
                .await,
            "not slow"
        );
        let (r, _) = get(&app).await;
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(
            body(r).await,
            "{\"transactionCount\":42,\"coinsInCirculation\":null}"
        );
    }
    // ---- STATS-FIX-2 (R-P5P9-76) ----

    /// M-1: the age counts from the start of the count (the snapshot's age).
    #[tokio::test]
    async fn the_age_counts_from_the_start_of_the_count() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::with_counter(long_count)));
        let (_d, _app, st) = app(cache);
        cache
            .refresh(&st, SLOW_REFRESH, &CancellationToken::new())
            .await;
        let age = cache.peek(Instant::now()).unwrap().age;
        assert!(age >= Duration::from_millis(300), "{age:?}");
    }

    static FLAKY_CALLS: AtomicUsize = AtomicUsize::new(0);

    /// Fails on the first call, then counts.
    fn flaky(_: &View, _: &CancellationToken) -> ApiResult<u64> {
        if FLAKY_CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(ApiError::Deadline)
        } else {
            Ok(5)
        }
    }

    /// M-2: while no count exists, a failed refresh is retried after `retry`, not `every`.
    #[tokio::test]
    async fn a_failed_first_count_is_retried_soon() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::with_counter(flaky)));
        let (_d, _app, st) = app(cache);
        let stop = CancellationToken::new();
        let task = tokio::spawn(refresh_every(
            st,
            Duration::from_secs(3600),
            Duration::from_millis(20),
            stop.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while cache.peek(Instant::now()).is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the first count lands after the retry");
        assert_eq!(cache.peek(Instant::now()).unwrap().n, 5);
        stop.cancel();
        task.await.unwrap();
    }

    /// M-3a: while a refresh is running (holding no lock), a request answers 200 at once
    /// with the old count and its age.
    #[tokio::test]
    async fn a_request_during_a_running_refresh_answers_the_old_count() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::with_counter(slow_count)));
        cache.store(7, Instant::now() - Duration::from_secs(10));
        let (_d, app, st) = app(cache);
        let refresh = tokio::spawn(async move {
            cache
                .refresh(&st, SLOW_REFRESH, &CancellationToken::new())
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!refresh.is_finished(), "the refresh is running");
        let (r, took) = get(&app).await;
        assert_eq!(r.status(), StatusCode::OK);
        assert!(took < Duration::from_millis(250), "{took:?}");
        let age: u64 = r.headers()[COUNT_AGE].to_str().unwrap().parse().unwrap();
        assert!((10..15).contains(&age), "{age}");
        assert_eq!(
            body(r).await,
            "{\"transactionCount\":7,\"coinsInCirculation\":null}"
        );
        refresh.await.unwrap();
        assert_eq!(cache.peek(Instant::now()).unwrap().n, 99);
    }

    /// M-5: the stale-count WARN is due only for an old count, and once per window.
    #[test]
    fn a_stale_count_warns_once_per_window() {
        let c = CountCache::new();
        let t0 = Instant::now();
        let stale = REFRESH_EVERY * STALE_AFTER_INTERVALS + Duration::from_secs(1);
        assert!(!c.stale_warn_due(REFRESH_EVERY * STALE_AFTER_INTERVALS, t0));
        assert!(c.stale_warn_due(stale, t0));
        assert!(!c.stale_warn_due(stale, t0 + Duration::from_secs(1)));
        assert!(c.stale_warn_due(stale, t0 + WARN_EVERY));
    }

    /// The raw head and body of `GET /stats` on `addr`, with `extra` header lines.
    async fn raw_get(addr: std::net::SocketAddr, extra: &str) -> String {
        let req = format!("GET /stats HTTP/1.1\r\nHost: x\r\n{extra}Connection: close\r\n\r\n");
        tokio::task::spawn_blocking(move || {
            use std::io::{Read, Write};
            let mut c = std::net::TcpStream::connect(addr).unwrap();
            c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            c.write_all(req.as_bytes()).unwrap();
            let mut out = String::new();
            c.read_to_string(&mut out).unwrap();
            out
        })
        .await
        .unwrap()
    }

    /// M-3b, M-6: `Api::serve` starts the refresh task (so `/stats` reaches 200) and the
    /// compat CORS layer exposes `x-count-age` (and, F-48, the next and truncation headers).
    #[tokio::test]
    async fn serve_starts_the_refresh_and_exposes_the_age() {
        let cache: &'static CountCache = Box::leak(Box::new(CountCache::with_counter(counted)));
        let (_d, store) = seeded_store();
        let search = crate::search::SearchCtx::new(super::super::SearchSettings::default(), None);
        let loopback: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let api = crate::Api::new(store, test_settings(), crate::TipWatch::default())
            .with_stats_cache(cache)
            .compat_routes(super::super::router(search))
            .on_bound(move |a| {
                let _ = tx.send(a.to_vec());
            });
        let binds = crate::Binds {
            compat: vec![loopback],
            ..crate::Binds::default()
        };
        let stop = CancellationToken::new();
        let task = tokio::spawn(api.serve(binds, stop.clone()));
        let addr = rx
            .await
            .unwrap()
            .iter()
            .find(|b| b.0 == "compat")
            .unwrap()
            .1;
        let mut out = String::new();
        for _ in 0..100 {
            out = raw_get(addr, "Origin: http://example.com\r\n").await;
            if out.starts_with("HTTP/1.1 200") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let low = out.to_ascii_lowercase();
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
        assert!(low.contains("x-count-age: "), "{out}");
        let expose = low
            .lines()
            .find(|l| l.starts_with("access-control-expose-headers:"))
            .unwrap_or_default();
        assert!(expose.contains("x-count-age"), "{out}");
        // F-48: a browser also needs to see that a search was cut short.
        assert!(
            expose.contains(super::super::SEARCH_TRUNCATED.as_str()),
            "{out}"
        );
        assert!(expose.contains("chainweb-next"), "{out}");
        stop.cancel();
        task.await.unwrap().unwrap();
    }
}
