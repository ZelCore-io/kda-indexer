//! Websocket subscriptions on `/graphql` (Task 6). Until they are served, an upgrade
//! request answers a GraphQL error `NOT_SUPPORTED`.

use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::error::{server, Code};

/// True for a websocket upgrade request.
pub fn is_upgrade(headers: &HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}

/// The answer to an upgrade while subscriptions are not served.
pub fn upgrade_not_served() -> Response {
    let r = async_graphql::Response::from_errors(vec![server(
        Code::NotSupported,
        "subscriptions are not served yet",
    )]);
    (
        StatusCode::BAD_REQUEST,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_vec(&r).unwrap_or_default(),
    )
        .into_response()
}
