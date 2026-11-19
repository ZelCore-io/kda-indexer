//! `/coins` (chainweb-data `coinsHandler`, ruling C-1): the circulating supply at the highest
//! stored height and the current time, as `show` of the Haskell `Decimal` in plain text.
//! The supply tables are mainnet's: other networks, like an empty store, answer
//! `404 Server database is empty`.

use std::sync::Arc;
use std::time::SystemTime;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use super::schemas::LimitErrors;
use super::text;
use crate::server::ApiState;

/// Network whose supply tables `crate::supply` holds.
const SUPPLY_NETWORK: &str = "mainnet01";

/// Circulating supply in plain text
///
/// chainweb-data `coinsHandler`: the circulating KDA at the highest stored height and the
/// current time, printed as the Haskell `show` of a `Decimal` (`text/plain`, for example
/// `1234567.89`). The supply tables are mainnet01's: any other network, like an empty
/// store, answers `404 Server database is empty`.
#[utoipa::path(
    get,
    path = "/coins",
    operation_id = "compat_coins",
    tag = "network",
    responses(
        (status = 200, description = "Circulating supply, decimal text", body = String, content_type = "text/plain"),
        (status = 404, description = "`Server database is empty` (empty store, or a network other than mainnet01)", body = String, content_type = "text/plain"),
        LimitErrors,
    )
)]
pub async fn handler(State(st): State<Arc<ApiState>>) -> Response {
    let res = st
        .read(|view| {
            if view.genesis.network != SUPPLY_NETWORK {
                return Ok(None);
            }
            view.max_indexed_height()
        })
        .await;
    match res {
        Ok(Some(h)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain;charset=utf-8")],
            crate::supply::circulating_text(u64::from(h), SystemTime::now()),
        )
            .into_response(),
        Ok(None) => text(StatusCode::NOT_FOUND, "Server database is empty"),
        Err(e) => e.into_response(),
    }
}
