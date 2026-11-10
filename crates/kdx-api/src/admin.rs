//! Admin listener: `/healthz`, `/readyz`, `/status`, `/metrics`.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde_json::json;

use crate::server::ApiState;
use crate::status::{self, Status};

pub fn router() -> Router<Arc<ApiState>> {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .route("/status", get(status::handler))
        .route("/metrics", get(metrics))
}

async fn metrics() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        kdx_metrics::encode(),
    )
        .into_response()
}

async fn readyz(State(st): State<Arc<ApiState>>) -> Response {
    let reason = match Status::load(&st).await {
        Ok(s) => s.not_ready(st.settings.ready_max_lag),
        Err(e) => Some(format!("store: {e}")),
    };
    match reason {
        None => (StatusCode::OK, axum::Json(json!({"ready": true}))).into_response(),
        Some(r) => (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({"ready": false, "reason": r})),
        )
            .into_response(),
    }
}
