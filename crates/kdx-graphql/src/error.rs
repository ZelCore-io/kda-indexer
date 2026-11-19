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
    /// Stored data a field needs is not there (yet): NFT `guard`/`info` before the reconciler
    /// read them, or when the node had no usable answer (GQ-15).
    NotAvailable,
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
            Code::NotAvailable => "NOT_AVAILABLE",
            Code::Internal => "INTERNAL",
        }
    }
}

/// A response body with each error's `extensions.code` also copied to a top-level `code`:
/// the GraphQL-spec place stays, and clients written against the oracle, which sends the
/// code top-level, read it too (R-P5P9-19). Takes a `Response` or a batch of them.
pub fn wire_json<T: serde::Serialize>(resp: &T) -> serde_json::Result<Vec<u8>> {
    let mut v = serde_json::to_value(resp)?;
    match &mut v {
        serde_json::Value::Array(rs) => rs.iter_mut().for_each(copy_codes),
        r => copy_codes(r),
    }
    serde_json::to_vec(&v)
}

/// Copies each error's `extensions.code` to a top-level `code`. A response without data
/// whose error has no code holds async-graphql's own request-level error (a missing or
/// mistyped variable or argument; resolver errors always carry a code through [`err`]):
/// it gets `INTERNAL_SERVER_ERROR`, the code stock answers it with (FIX-4 item 3).
fn copy_codes(resp: &mut serde_json::Value) {
    let no_data = resp.get("data").is_none_or(|d| d.is_null());
    let Some(errors) = resp.get_mut("errors").and_then(|e| e.as_array_mut()) else {
        return;
    };
    for e in errors {
        let Some(o) = e.as_object_mut() else {
            continue;
        };
        let code = o
            .get("extensions")
            .and_then(|x| x.get("code"))
            .cloned()
            .or_else(|| no_data.then(|| INTERNAL_SERVER_ERROR.into()));
        let Some(code) = code else {
            continue;
        };
        if let serde_json::Value::Object(x) = o
            .entry("extensions")
            .or_insert_with(|| serde_json::json!({}))
        {
            x.entry("code").or_insert(code.clone());
        }
        o.entry("code").or_insert(code);
    }
}

/// Whether `r` is refused as a whole by an error that carries no code: async-graphql's own
/// request-level error (see [`copy_codes`]).
pub fn codeless_request_error(r: &async_graphql::Response) -> bool {
    r.data == async_graphql::Value::Null
        && r.errors
            .iter()
            .any(|e| e.extensions.as_ref().and_then(|x| x.get("code")).is_none())
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

/// The message of the page safety cap's refusal ([`page_limit`]).
pub const PAGE_LIMIT_MESSAGE: &str =
    "a page holds at most 1000 rows (first, last and quantity above it are refused)";

/// The refusal of a page over [`crate::MAX_PAGE_ROWS`] (FIX-3 part D, S3): `LIMIT`, with
/// `extensions.limit = "page"`. Counts `kdx_graphql_rejected_total{reason = "page"}`.
pub fn page_limit() -> async_graphql::Error {
    kdx_metrics::get().graphql_rejected("page");
    err(Code::Limit, PAGE_LIMIT_MESSAGE).extend_with(|_, e| e.set("limit", "page"))
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

/// Stock's `extensions.code` of a refusal thrown in `didResolveOperation` (complexity).
pub const INTERNAL_SERVER_ERROR: &str = "INTERNAL_SERVER_ERROR";
/// Stock's `extensions.code` of a validation error (depth).
pub const GRAPHQL_VALIDATION_FAILED: &str = "GRAPHQL_VALIDATION_FAILED";

/// A refusal of the stock indexer's limits (FIX-3 part C): stock's message and code, and
/// `extensions.limit = name` so ours still names the limit. Counts
/// `kdx_graphql_rejected_total{reason = name}`.
pub fn stock_refusal(
    code: &'static str,
    name: &'static str,
    msg: impl Into<String>,
) -> ServerError {
    kdx_metrics::get().graphql_rejected(name);
    let mut e = ServerError::new(msg, None);
    let mut x = async_graphql::ErrorExtensionValues::default();
    x.set("code", code);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_code_is_also_top_level() {
        let one = async_graphql::Response::from_errors(vec![server(Code::Limit, "x")]);
        let v: serde_json::Value = serde_json::from_slice(&wire_json(&one).unwrap()).unwrap();
        assert_eq!(v["errors"][0]["extensions"]["code"], "LIMIT");
        assert_eq!(v["errors"][0]["code"], "LIMIT");
        // A batch: every response gets it. An error without a code on a response without
        // data is async-graphql's own request-level error: it gets stock's
        // `INTERNAL_SERVER_ERROR` (FIX-4 item 3).
        let plain = async_graphql::Response::from_errors(vec![ServerError::new("y", None)]);
        let batch = vec![one, plain];
        let v: serde_json::Value = serde_json::from_slice(&wire_json(&batch).unwrap()).unwrap();
        assert_eq!(v[0]["errors"][0]["code"], "LIMIT");
        assert_eq!(v[1]["errors"][0]["code"], "INTERNAL_SERVER_ERROR");
        assert_eq!(
            v[1]["errors"][0]["extensions"]["code"],
            "INTERNAL_SERVER_ERROR"
        );
        // A codeless error beside data is left alone.
        let mut partial = async_graphql::Response::new(async_graphql::Value::from(1));
        partial.errors.push(ServerError::new("z", None));
        let v: serde_json::Value = serde_json::from_slice(&wire_json(&partial).unwrap()).unwrap();
        assert!(v["errors"][0].get("code").is_none(), "{v}");
        // Data responses are untouched.
        let ok = async_graphql::Response::new(async_graphql::Value::Null);
        let v: serde_json::Value = serde_json::from_slice(&wire_json(&ok).unwrap()).unwrap();
        assert!(v.get("errors").is_none());
    }
}
