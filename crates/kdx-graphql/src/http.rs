//! `/graphql` over HTTP (GQ-17): `POST` (one operation or a batch, JSON) and `GET`
//! (GraphQL-over-HTTP query parameters). The websocket upgrade on the same path is
//! [`crate::ws`]. The listener's guard middleware (in-flight cap, per-IP rate, deadline,
//! response cap) wraps all of it; this layer adds the body, batch and size caps, the extra
//! rate tokens of a batch, and a `TIMEOUT` GraphQL error just inside the guard's deadline.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_graphql::{
    BatchRequest, BatchResponse, Name, Request, Response, Value as ConstValue, Variables,
};
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Query, Request as HttpRequest, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response as HttpResponse};
use axum::routing::get;
use axum::Router;
use kdx_api::ApiState;
use serde::Deserialize;

use crate::error::{limit, server, Code};
use crate::{KdxSchema, ReqCtx};

/// Time kept back from the request deadline to answer `TIMEOUT` as GraphQL before the
/// guard's 504.
const DEADLINE_MARGIN: Duration = Duration::from_millis(100);

/// The `/graphql` routes (`Api::graphql_routes` adds the guard, tracking and CORS).
pub fn router(schema: KdxSchema) -> Router<Arc<ApiState>> {
    let schema = Arc::new(schema);
    let (g, p) = (schema.clone(), schema);
    Router::new().route(
        "/graphql",
        get(move |st: State<Arc<ApiState>>, req: HttpRequest| get_handler(g.clone(), st, req))
            .post(move |st: State<Arc<ApiState>>, req: HttpRequest| {
                post_handler(p.clone(), st, req)
            }),
    )
}

fn peer_of(req: &HttpRequest) -> Option<SocketAddr> {
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0)
}

#[derive(Debug, Deserialize)]
pub struct GetParams {
    query: Option<String>,
    variables: Option<String>,
    #[serde(rename = "operationName")]
    operation_name: Option<String>,
}

/// A JSON value as a GraphQL value, numbers kept exact (the workspace's `serde_json` has
/// `arbitrary_precision`, which serde-based conversion would turn into objects).
pub fn const_value(v: serde_json::Value) -> Option<ConstValue> {
    Some(match v {
        serde_json::Value::Null => ConstValue::Null,
        serde_json::Value::Bool(b) => ConstValue::Boolean(b),
        serde_json::Value::Number(n) => ConstValue::Number(n),
        serde_json::Value::String(s) => ConstValue::String(s),
        serde_json::Value::Array(a) => {
            ConstValue::List(a.into_iter().map(const_value).collect::<Option<_>>()?)
        }
        serde_json::Value::Object(o) => ConstValue::Object(
            o.into_iter()
                .map(|(k, v)| Some((Name::new(k), const_value(v)?)))
                .collect::<Option<_>>()?,
        ),
    })
}

/// Request variables from JSON: an object, or null/absent for none.
pub fn variables(v: serde_json::Value) -> Option<Variables> {
    match v {
        serde_json::Value::Null => Some(Variables::default()),
        serde_json::Value::Object(_) => Some(Variables::from_value(const_value(v)?)),
        _ => None,
    }
}

fn error_body(status: StatusCode, e: async_graphql::ServerError) -> HttpResponse {
    let r = Response::from_errors(vec![e]);
    json_response(status, &serde_json::to_vec(&r).unwrap_or_default())
}

fn json_response(status: StatusCode, body: &[u8]) -> HttpResponse {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        Body::from(body.to_vec()),
    )
        .into_response()
}

fn bad(msg: &str) -> HttpResponse {
    kdx_metrics::get().graphql_request("query", "rejected");
    error_body(StatusCode::BAD_REQUEST, server(Code::BadUserInput, msg))
}

/// One request object of a POST body.
fn request_of(v: serde_json::Value) -> Result<Request, &'static str> {
    let serde_json::Value::Object(mut o) = v else {
        return Err("each request must be a JSON object");
    };
    let query = match o.remove("query") {
        Some(serde_json::Value::String(q)) => q,
        _ => return Err("query must be a string"),
    };
    let mut r = Request::new(query);
    match o.remove("operationName") {
        None | Some(serde_json::Value::Null) => {}
        Some(serde_json::Value::String(n)) => r = r.operation_name(n),
        Some(_) => return Err("operationName must be a string"),
    }
    let vars = variables(o.remove("variables").unwrap_or_default())
        .ok_or("variables must be an object")?;
    Ok(r.variables(vars))
}

async fn post_handler(
    schema: Arc<KdxSchema>,
    State(st): State<Arc<ApiState>>,
    req: HttpRequest,
) -> HttpResponse {
    let peer = peer_of(&req);
    let (parts, body) = req.into_parts();
    let headers = parts.headers;
    let s = &crate::gql_settings(&schema);
    let bytes: Bytes = match axum::body::to_bytes(body, s.max_body_bytes).await {
        Ok(b) => b,
        Err(_) => {
            kdx_metrics::get().graphql_request("query", "rejected");
            return error_body(
                StatusCode::PAYLOAD_TOO_LARGE,
                limit(
                    "body_size",
                    format!("request body larger than {} bytes", s.max_body_bytes),
                ),
            );
        }
    };
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return bad("request body is not JSON");
    };
    let batch = match v {
        serde_json::Value::Array(items) => {
            if items.is_empty() {
                return bad("empty batch");
            }
            if items.len() > s.max_batch {
                kdx_metrics::get().graphql_request("batch", "rejected");
                return error_body(
                    StatusCode::OK,
                    limit(
                        "batch",
                        format!("more than {} operations in a batch", s.max_batch),
                    ),
                );
            }
            match items
                .into_iter()
                .map(request_of)
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(r) => BatchRequest::Batch(r),
                Err(m) => return bad(m),
            }
        }
        v => match request_of(v) {
            Ok(r) => BatchRequest::Single(r),
            Err(m) => return bad(m),
        },
    };
    execute(schema, &st, peer, &headers, batch).await
}

async fn get_handler(
    schema: Arc<KdxSchema>,
    State(st): State<Arc<ApiState>>,
    req: HttpRequest,
) -> HttpResponse {
    if crate::ws::is_upgrade(req.headers()) {
        return crate::ws::upgrade(schema, st, req).await;
    }
    let peer = peer_of(&req);
    let headers = req.headers().clone();
    let Ok(Query(q)) = Query::<GetParams>::try_from_uri(req.uri()) else {
        return bad("query parameters are not valid");
    };
    let Some(query) = q.query else {
        return bad("query parameter missing");
    };
    let vars = match q.variables {
        None => Variables::default(),
        Some(t) => match serde_json::from_str(&t).ok().and_then(variables) {
            Some(v) => v,
            None => return bad("variables must be a JSON object"),
        },
    };
    let mut r = Request::new(query).variables(vars);
    if let Some(n) = q.operation_name {
        r = r.operation_name(n);
    }
    execute(schema, &st, peer, &headers, BatchRequest::Single(r)).await
}

async fn execute(
    schema: Arc<KdxSchema>,
    st: &Arc<ApiState>,
    peer: Option<SocketAddr>,
    headers: &HeaderMap,
    batch: BatchRequest,
) -> HttpResponse {
    let kind = match &batch {
        BatchRequest::Single(_) => "query",
        BatchRequest::Batch(_) => "batch",
    };
    // The guard took one rate token; a batch of N takes N.
    if let (BatchRequest::Batch(b), Some(peer)) = (&batch, peer) {
        let extra = b.len().saturating_sub(1) as u32;
        if extra > 0 && !st.allow_extra(peer, headers, extra) {
            kdx_metrics::get().graphql_request(kind, "rejected");
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "1")],
                "rate limit exceeded",
            )
                .into_response();
        }
    }
    let budget = st.settings.deadline.saturating_sub(DEADLINE_MARGIN);
    let deadline = Instant::now() + budget;
    let mut batch = batch.data(ReqCtx { deadline });
    if let Some(p) = peer {
        batch = batch.data(crate::ClientIp(st.client_ip_of(p, headers)));
    }
    let resp = match tokio::time::timeout(budget, schema.execute_batch(batch)).await {
        Ok(r) => r,
        Err(_) => {
            kdx_metrics::get().graphql_request(kind, "timeout");
            return error_body(
                StatusCode::OK,
                server(Code::Timeout, "request deadline exceeded"),
            );
        }
    };
    let result = match &resp {
        BatchResponse::Single(r) => outcome(r),
        BatchResponse::Batch(rs) => rs.iter().map(outcome).max().unwrap_or(Outcome::Ok),
    };
    kdx_metrics::get().graphql_request(kind, result.label());
    match serde_json::to_vec(&resp) {
        Ok(b) => json_response(StatusCode::OK, &b),
        Err(e) => {
            tracing::error!(error = %e, "graphql: response serialization");
            error_body(
                StatusCode::INTERNAL_SERVER_ERROR,
                server(Code::Internal, "internal error"),
            )
        }
    }
}

/// The `result` label of a response: a `LIMIT` refusal outranks other errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Outcome {
    Ok,
    Error,
    Rejected,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Error => "error",
            Outcome::Rejected => "rejected",
        }
    }
}

fn outcome(r: &Response) -> Outcome {
    let limit = ConstValue::String(Code::Limit.as_str().to_owned());
    if r.errors.iter().any(|e| {
        e.extensions
            .as_ref()
            .and_then(|x| x.get("code"))
            .is_some_and(|c| *c == limit)
    }) {
        Outcome::Rejected
    } else if r.is_ok() {
        Outcome::Ok
    } else {
        Outcome::Error
    }
}
