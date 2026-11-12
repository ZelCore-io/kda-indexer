//! GraphQL error codes (`extensions.code`). Messages never carry paths, store detail or
//! internal addresses.

use async_graphql::{ErrorExtensions, ServerError};
use kdx_api::ApiError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    /// A schema field this indexer does not serve (`pactQuery`, `gasLimitEstimate`, ...).
    NotSupported,
    BadUserInput,
    /// A resource limit (`extensions.limit` names it).
    Limit,
    /// The request deadline passed.
    Timeout,
    /// `totalCount` could not be counted within its share of the deadline (GQ-10).
    CountTimeout,
    /// An `events.parametersFilter` form that is not served (GQ-11).
    UnsupportedFilter,
    Internal,
}

impl Code {
    pub fn as_str(self) -> &'static str {
        match self {
            Code::NotSupported => "NOT_SUPPORTED",
            Code::BadUserInput => "BAD_USER_INPUT",
            Code::Limit => "LIMIT",
            Code::Timeout => "TIMEOUT",
            Code::CountTimeout => "COUNT_TIMEOUT",
            Code::UnsupportedFilter => "UNSUPPORTED_FILTER",
            Code::Internal => "INTERNAL",
        }
    }
}

/// A resolver error with `extensions.code`.
pub fn err(code: Code, msg: impl Into<String>) -> async_graphql::Error {
    async_graphql::Error::new(msg).extend_with(|_, e| e.set("code", code.as_str()))
}

/// `NOT_SUPPORTED` for a field (`Type.field`).
pub fn not_supported(field: &str) -> async_graphql::Error {
    err(
        Code::NotSupported,
        format!("{field} is not served by this indexer"),
    )
}

pub fn bad_input(msg: impl Into<String>) -> async_graphql::Error {
    err(Code::BadUserInput, msg)
}

/// A request-level `LIMIT` refusal: `extensions.code = "LIMIT"`, `extensions.limit = name`.
/// Counts `kdx_graphql_rejected_total{reason = name}`.
pub fn limit(name: &'static str, msg: impl Into<String>) -> ServerError {
    kdx_metrics::get().graphql_rejected(name);
    let mut e = ServerError::new(msg, None);
    let mut x = async_graphql::ErrorExtensionValues::default();
    x.set("code", Code::Limit.as_str());
    x.set("limit", name);
    e.extensions = Some(x);
    e
}

/// A request-level error with a code (no position).
pub fn server(code: Code, msg: impl Into<String>) -> ServerError {
    let mut e = ServerError::new(msg, None);
    let mut x = async_graphql::ErrorExtensionValues::default();
    x.set("code", code.as_str());
    e.extensions = Some(x);
    e
}

/// A read-layer error as a resolver error. Internal causes are logged, not sent.
pub fn from_api(e: ApiError) -> async_graphql::Error {
    match e {
        ApiError::BadRequest(t) => err(Code::BadUserInput, t),
        ApiError::NotFound(t) => err(Code::BadUserInput, t),
        ApiError::Deadline => err(Code::Timeout, "request deadline exceeded"),
        ApiError::Unavailable(t) => err(Code::Limit, t),
        ApiError::Internal(e) => {
            tracing::error!(error = format!("{e:#}"), "graphql: internal error");
            err(Code::Internal, "internal error")
        }
    }
}
