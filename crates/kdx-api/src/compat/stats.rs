//! `/stats` (chainweb-data `statsHandler`): `{"transactionCount":N,"coinsInCirculation":X}`.
//!
//! chainweb-data answers a Postgres row estimate; this is the exact user-tx count of the
//! served store (genesis txs excluded, as everywhere in compat). `X` is the circulating
//! supply at the highest stored height and the current time. The key order is the
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

/// Height of the genesis blocks of chains 10–19.
const GENESIS_20: u32 = 852_054;

/// User txs of the genesis blocks (hidden in compat).
fn genesis_user_txs(view: &View) -> ApiResult<u64> {
    let mut n = 0u64;
    for c in 0..kdx_segment::SLOT_WIDTH as u8 {
        let h = if c < 10 { 0 } else { GENESIS_20 };
        n += view
            .block_txs(c, h)?
            .iter()
            .filter(|r| !r.key.is_coinbase())
            .count() as u64;
    }
    Ok(n)
}

/// Exact user-tx count of what `view` serves: frozen ranges (`tx_count - block_count`,
/// one coinbase per block) plus the hot rows at or above the split, minus genesis txs.
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
    Ok((frozen + hot).saturating_sub(genesis_user_txs(view)?))
}

static CACHE: Mutex<Option<(Instant, u64)>> = Mutex::new(None);
/// Held while one request counts: concurrent misses do not each walk the window.
static COUNTING: Mutex<()> = Mutex::new(());

fn cache_get(fresh_only: bool) -> Option<u64> {
    let c = (*CACHE.lock().unwrap_or_else(|e| e.into_inner()))?;
    (!fresh_only || c.0.elapsed() < CACHE_FOR).then_some(c.1)
}

/// `user_tx_count`, reused for `CACHE_FOR`. Single-flight: on a miss one request counts;
/// the others answer the previous value, or wait for the count when there is none yet.
fn cached_count(view: &View) -> ApiResult<u64> {
    if let Some(n) = cache_get(true) {
        return Ok(n);
    }
    let _g = match COUNTING.try_lock() {
        Ok(g) => g,
        Err(std::sync::TryLockError::Poisoned(e)) => e.into_inner(),
        Err(std::sync::TryLockError::WouldBlock) => {
            if let Some(n) = cache_get(false) {
                return Ok(n);
            }
            COUNTING.lock().unwrap_or_else(|e| e.into_inner())
        }
    };
    if let Some(n) = cache_get(true) {
        return Ok(n);
    }
    let n = user_tx_count(view)?;
    *CACHE.lock().unwrap_or_else(|e| e.into_inner()) = Some((Instant::now(), n));
    Ok(n)
}

fn stats(view: &View) -> ApiResult<Option<(u64, f64)>> {
    let Some(h) = view.max_indexed_height()? else {
        return Ok(None);
    };
    Ok(Some((
        cached_count(view)?,
        crate::supply::circulating(u64::from(h), SystemTime::now()),
    )))
}

pub async fn handler(State(st): State<Arc<ApiState>>) -> Response {
    match st.read(stats).await {
        Ok(Some((n, coins))) => {
            let mut b = format!("{{\"transactionCount\":{n},\"coinsInCirculation\":").into_bytes();
            shapes::write_aeson(&shapes::aeson_double(coins), &mut b);
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

    #[test]
    fn count_checks_the_deadline() {
        let (_d, store) = crate::testutil::store();
        let view = store.view(Instant::now() - Duration::from_secs(1)).unwrap();
        assert!(matches!(user_tx_count(&view), Err(crate::ApiError::Deadline)));
    }
}
