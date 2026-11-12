//! `/stats` (chainweb-data `statsHandler`): `{"transactionCount":N,"coinsInCirculation":X}`.
//!
//! chainweb-data answers a Postgres row estimate; this is the exact user-tx count of the
//! served store, genesis txs included (chainweb-data's transactions table holds them, G1).
//! `X` is the circulating supply at the highest stored height and the current time, on
//! mainnet01 only (the supply tables are mainnet's, R9); `null` on other networks. The key order is the
//! record's field order, not alphabetical (chainweb-data encodes the record directly).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use kdx_hot::{Table, Txn};
use kdx_types::TxKey;

use super::{shapes, text};
use crate::server::ApiState;
use crate::store::View;
use crate::ApiResult;

/// A counted answer is reused this long (the count walks every hot tx key).
const CACHE_FOR: Duration = Duration::from_secs(5);
/// A recount that misses its deadline answers the last counted value only while that value
/// is younger than this; an older one is not served (the request gets the deadline error,
/// `504`, as when nothing was counted yet).
pub const MAX_FALLBACK_AGE: Duration = Duration::from_secs(10 * 60);

/// Header on a `/stats` answer that is the last counted value, not a fresh count: its age in
/// whole seconds.
pub const COUNT_AGE: HeaderName = HeaderName::from_static("x-count-age");

/// A user-tx count as `/stats` answers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxCount {
    pub n: u64,
    /// How old the count is when it is the last counted value (a recount missed its
    /// deadline, or another request is recounting); None for a count within `CACHE_FOR`.
    pub age: Option<Duration>,
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
        return Err(crate::ApiError::Deadline);
    }
    Ok(frozen + hot)
}

/// The last counted answer and the single-flight guard of a recount.
struct CountCache {
    last: Mutex<Option<(Instant, u64)>>,
    /// Held while one request counts: concurrent misses do not each walk the window.
    counting: Mutex<()>,
}

static CACHE: CountCache = CountCache::new();

impl CountCache {
    const fn new() -> CountCache {
        CountCache {
            last: Mutex::new(None),
            counting: Mutex::new(()),
        }
    }

    /// The last counted value at `now`: within `CACHE_FOR` it is fresh (no age); otherwise,
    /// unless `fresh_only`, it is served with its age while that is under
    /// `MAX_FALLBACK_AGE`.
    fn get(&self, fresh_only: bool, now: Instant) -> Option<TxCount> {
        let (at, n) = (*self.last.lock().unwrap_or_else(|e| e.into_inner()))?;
        let age = now.saturating_duration_since(at);
        if age < CACHE_FOR {
            return Some(TxCount { n, age: None });
        }
        (!fresh_only && age < MAX_FALLBACK_AGE).then_some(TxCount { n, age: Some(age) })
    }

    /// `user_tx_count`, reused for `CACHE_FOR`. Single-flight: on a miss one request
    /// counts; the others answer the previous value, or wait for the count when there is
    /// none yet. A recount that misses the request deadline (cold pages: 5E saw a walk of
    /// the hot window pass 5 s) answers the last counted value with its age while that is
    /// under `MAX_FALLBACK_AGE`; with no such value it is a 504. chainweb-data's own count is
    /// an estimate.
    fn count(&self, view: &View) -> ApiResult<TxCount> {
        self.count_at(view, Instant::now)
    }

    /// [`Self::count`] with the clock `now` (tests move it forward).
    fn count_at(&self, view: &View, now: impl Fn() -> Instant) -> ApiResult<TxCount> {
        if let Some(c) = self.get(true, now()) {
            return Ok(c);
        }
        let _g = match self.counting.try_lock() {
            Ok(g) => g,
            Err(std::sync::TryLockError::Poisoned(e)) => e.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                if let Some(c) = self.get(false, now()) {
                    return Ok(c);
                }
                self.counting.lock().unwrap_or_else(|e| e.into_inner())
            }
        };
        if let Some(c) = self.get(true, now()) {
            return Ok(c);
        }
        match user_tx_count(view) {
            Ok(n) => {
                *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some((now(), n));
                Ok(TxCount { n, age: None })
            }
            Err(crate::ApiError::Deadline) => {
                self.get(false, now()).ok_or(crate::ApiError::Deadline)
            }
            Err(e) => Err(e),
        }
    }
}

/// Network whose supply tables `crate::supply` holds.
const SUPPLY_NETWORK: &str = "mainnet01";

/// `(user tx count, coins in circulation)` as `/stats` answers them (GraphQL `networkInfo`
/// reads the same).
pub fn stats(view: &View) -> ApiResult<Option<(u64, Option<f64>)>> {
    Ok(stats_aged(view)?.map(|(c, coins)| (c.n, coins)))
}

/// [`stats`] with the count's age (`/stats` sends it as [`COUNT_AGE`]).
fn stats_aged(view: &View) -> ApiResult<Option<(TxCount, Option<f64>)>> {
    let Some(h) = view.max_indexed_height()? else {
        return Ok(None);
    };
    let coins = (view.genesis.network == SUPPLY_NETWORK)
        .then(|| crate::supply::circulating(u64::from(h), SystemTime::now()));
    Ok(Some((CACHE.count(view)?, coins)))
}

pub async fn handler(State(st): State<Arc<ApiState>>) -> Response {
    match st.read(stats_aged).await {
        Ok(Some((count, coins))) => {
            let n = count.n;
            let mut b = format!("{{\"transactionCount\":{n},\"coinsInCirculation\":").into_bytes();
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
            if let Some(age) = count.age {
                resp.headers_mut()
                    .insert(COUNT_AGE, header::HeaderValue::from(age.as_secs()));
            }
            resp
        }
        Ok(None) => text(StatusCode::NOT_FOUND, "Server database is empty"),
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// 5E: a recount that misses the deadline answers the last counted value with its age,
    /// while that is under `MAX_FALLBACK_AGE`; with no value yet, or only an older one, it
    /// stays a deadline error.
    #[test]
    fn a_late_recount_answers_the_last_counted_value() {
        let (_d, store) = crate::testutil::store();
        let c = CountCache::new();
        let late = || store.view(Instant::now() - Duration::from_secs(1)).unwrap();
        let in_time = || {
            store
                .view(Instant::now() + Duration::from_secs(60))
                .unwrap()
        };
        assert!(matches!(c.count(&late()), Err(crate::ApiError::Deadline)));
        // An in-time count is stored and reused, with no age.
        let t0 = Instant::now();
        let n = c.count_at(&in_time(), || t0).unwrap().n;
        let fresh = TxCount { n, age: None };
        assert_eq!(c.get(true, t0), Some(fresh));
        assert_eq!(c.count_at(&late(), || t0).unwrap(), fresh);
        // Older than CACHE_FOR: the next call recounts, misses the deadline and answers the
        // stored value with its age.
        let at = |d: Duration| move || t0 + d;
        let old = CACHE_FOR * 4;
        assert_eq!(c.get(true, t0 + old), None);
        assert_eq!(
            c.count_at(&late(), at(old)).unwrap(),
            TxCount { n, age: Some(old) }
        );
        // Just under the bound: still served, with its age.
        let under = MAX_FALLBACK_AGE - Duration::from_secs(1);
        assert_eq!(
            c.count_at(&late(), at(under)).unwrap(),
            TxCount {
                n,
                age: Some(under)
            }
        );
        // At and past the bound: the deadline error, as with no value.
        for past in [MAX_FALLBACK_AGE, MAX_FALLBACK_AGE + Duration::from_secs(1)] {
            assert!(
                matches!(
                    c.count_at(&late(), at(past)),
                    Err(crate::ApiError::Deadline)
                ),
                "{past:?}"
            );
            assert_eq!(c.get(false, t0 + past), None);
        }
        // An in-time recount replaces it.
        let later = t0 + MAX_FALLBACK_AGE * 2;
        assert_eq!(c.count_at(&in_time(), || later).unwrap(), fresh);
        assert_eq!(c.get(true, later), Some(fresh));
    }

    /// While another request recounts, a waiting request answers the previous value with its
    /// age only under `MAX_FALLBACK_AGE`; an older one makes it wait for the count.
    #[test]
    fn a_concurrent_miss_serves_only_a_bounded_previous_value() {
        let (_d, store) = crate::testutil::store();
        let c = CountCache::new();
        let t0 = Instant::now();
        let view = store
            .view(Instant::now() + Duration::from_secs(60))
            .unwrap();
        let n = c.count_at(&view, || t0).unwrap().n;
        let busy = c.counting.lock().unwrap();
        let under = MAX_FALLBACK_AGE - Duration::from_secs(1);
        assert_eq!(
            c.count_at(&view, || t0 + under).unwrap(),
            TxCount {
                n,
                age: Some(under)
            }
        );
        // Past the bound it waits for the count in progress (here: until `busy` is dropped).
        std::thread::scope(|s| {
            let h = s.spawn(|| c.count_at(&view, || t0 + MAX_FALLBACK_AGE).unwrap());
            std::thread::sleep(Duration::from_millis(100));
            assert!(!h.is_finished(), "a value past the bound was served");
            drop(busy);
            assert_eq!(h.join().unwrap(), TxCount { n, age: None });
        });
    }

    #[test]
    fn count_checks_the_deadline() {
        let (_d, store) = crate::testutil::store();
        let view = store.view(Instant::now() - Duration::from_secs(1)).unwrap();
        assert!(matches!(
            user_tx_count(&view),
            Err(crate::ApiError::Deadline)
        ));
    }
}
