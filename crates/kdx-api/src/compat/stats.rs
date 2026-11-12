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
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use kdx_hot::{Table, Txn};
use kdx_types::TxKey;

use super::{shapes, text};
use crate::server::ApiState;
use crate::store::View;
use crate::ApiResult;

/// A counted answer is reused this long (the count walks every hot tx key).
const CACHE_FOR: Duration = Duration::from_secs(5);

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

    fn get(&self, fresh_only: bool) -> Option<u64> {
        let c = (*self.last.lock().unwrap_or_else(|e| e.into_inner()))?;
        (!fresh_only || c.0.elapsed() < CACHE_FOR).then_some(c.1)
    }

    /// `user_tx_count`, reused for `CACHE_FOR`. Single-flight: on a miss one request
    /// counts; the others answer the previous value, or wait for the count when there is
    /// none yet. A recount that misses the request deadline (cold pages: 5E saw a walk of
    /// the hot window pass 5 s) answers the last counted value, however old; only with no
    /// value yet is it a 504. chainweb-data's own count is an estimate.
    fn count(&self, view: &View) -> ApiResult<u64> {
        if let Some(n) = self.get(true) {
            return Ok(n);
        }
        let _g = match self.counting.try_lock() {
            Ok(g) => g,
            Err(std::sync::TryLockError::Poisoned(e)) => e.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                if let Some(n) = self.get(false) {
                    return Ok(n);
                }
                self.counting.lock().unwrap_or_else(|e| e.into_inner())
            }
        };
        if let Some(n) = self.get(true) {
            return Ok(n);
        }
        match user_tx_count(view) {
            Ok(n) => {
                *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some((Instant::now(), n));
                Ok(n)
            }
            Err(crate::ApiError::Deadline) => self.get(false).ok_or(crate::ApiError::Deadline),
            Err(e) => Err(e),
        }
    }
}

/// Network whose supply tables `crate::supply` holds.
const SUPPLY_NETWORK: &str = "mainnet01";

/// `(user tx count, coins in circulation)` as `/stats` answers them (GraphQL `networkInfo`
/// reads the same).
pub fn stats(view: &View) -> ApiResult<Option<(u64, Option<f64>)>> {
    let Some(h) = view.max_indexed_height()? else {
        return Ok(None);
    };
    let coins = (view.genesis.network == SUPPLY_NETWORK)
        .then(|| crate::supply::circulating(u64::from(h), SystemTime::now()));
    Ok(Some((CACHE.count(view)?, coins)))
}

pub async fn handler(State(st): State<Arc<ApiState>>) -> Response {
    match st.read(stats).await {
        Ok(Some((n, coins))) => {
            let mut b = format!("{{\"transactionCount\":{n},\"coinsInCirculation\":").into_bytes();
            match coins {
                Some(c) => shapes::write_aeson(&shapes::aeson_double(c), &mut b),
                None => b.extend_from_slice(b"null"),
            }
            b.push(b'}');
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json;charset=utf-8")],
                Body::from(b),
            )
                .into_response()
        }
        Ok(None) => text(StatusCode::NOT_FOUND, "Server database is empty"),
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// 5E: a recount that misses the deadline answers the last counted value (any age);
    /// with no value yet it stays a deadline error.
    #[test]
    fn a_late_recount_answers_the_last_counted_value() {
        let (_d, store) = crate::testutil::store();
        let c = CountCache::new();
        let late = || store.view(Instant::now() - Duration::from_secs(1)).unwrap();
        assert!(matches!(c.count(&late()), Err(crate::ApiError::Deadline)));
        // An in-time count is stored and reused.
        let n = c
            .count(
                &store
                    .view(Instant::now() + Duration::from_secs(60))
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(c.get(true), Some(n));
        assert_eq!(c.count(&late()).unwrap(), n);
        // A value older than CACHE_FOR (marked so it cannot be a fresh count): the next call
        // recounts, misses the deadline and answers it.
        *c.last.lock().unwrap() = Some((Instant::now() - CACHE_FOR * 4, n + 4242));
        assert_eq!(c.get(true), None);
        assert_eq!(c.count(&late()).unwrap(), n + 4242);
        // An in-time recount replaces it.
        let fresh = store
            .view(Instant::now() + Duration::from_secs(60))
            .unwrap();
        *c.last.lock().unwrap() = Some((Instant::now() - CACHE_FOR * 4, n + 4242));
        assert_eq!(c.count(&fresh).unwrap(), n);
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
