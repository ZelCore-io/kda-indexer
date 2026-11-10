//! kdx-api: the read layer over the hot store and the frozen segments (spec §4.4–§4.5, §5),
//! cursor tokens and (from P3 Task 2) the HTTP server, limits and admin endpoints.

pub mod admin;
pub mod compat;
pub mod cursor;
pub mod dict;
pub mod limits;
pub mod server;
pub mod status;
pub mod store;

pub use cursor::Cursor;
pub use dict::ViewDict;
pub use server::{Api, ApiSettings, ApiState, Binds, TipWatch};
pub use store::{PostingQuery, Store, View};

/// Errors a read can answer with. The HTTP layer maps them to status codes.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// 400 with this text.
    #[error("{0}")]
    BadRequest(String),
    /// 404 with this text.
    #[error("{0}")]
    NotFound(String),
    /// 504: the request deadline passed.
    #[error("request deadline exceeded")]
    Deadline,
    /// 503 with this text (store not ready, body too large, overloaded).
    #[error("{0}")]
    Unavailable(String),
    /// 500; the cause is logged, not sent.
    #[error("internal error")]
    Internal(#[from] anyhow::Error),
}

impl From<kdx_types::Error> for ApiError {
    fn from(e: kdx_types::Error) -> Self {
        ApiError::Internal(e.into())
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
