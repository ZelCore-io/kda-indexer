//! kdx-api: the read layer over the hot store and the frozen segments (spec §4.4–§4.5, §5),
//! cursor tokens and (from P3 Task 2) the HTTP server, limits and admin endpoints.

pub mod admin;
pub mod compat;
pub mod cursor;
pub mod dict;
pub mod docs;
pub mod limits;
pub mod native;
pub mod nodeinfo;
pub mod openapi;
pub mod sampler;
pub mod search;
pub mod server;
pub mod status;
pub mod store;
pub mod supply;
pub mod v1txs;

pub use cursor::Cursor;
pub use dict::ViewDict;
pub use server::{Api, ApiSettings, ApiState, Binds, RunDeadline, TipWatch};
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

/// Clears a "worker running" flag when the worker ends, a panic included.
pub(crate) struct ClearOnDrop<'a>(pub &'a std::sync::atomic::AtomicBool);

impl Drop for ClearOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
pub(crate) mod testutil {
    use std::sync::Arc;

    use crate::store::Store;

    /// An empty store in a temp dir (keep the dir alive while the store is used).
    pub fn store() -> (tempfile::TempDir, Arc<Store>) {
        let d = tempfile::tempdir().expect("tempdir");
        let g = kdx_hot::Geometry {
            max_gb: 1,
            growth_mb: 16,
        };
        let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).expect("hot store"));
        let s = Arc::new(Store::open(hot, d.path()).expect("store"));
        (d, s)
    }
}

#[cfg(test)]
mod clear_on_drop_tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::ClearOnDrop;

    /// F-49: the flag clears when the worker ends, a panic included.
    #[test]
    fn clears_on_panic_and_on_return() {
        let f = AtomicBool::new(true);
        {
            let _g = ClearOnDrop(&f);
        }
        assert!(!f.load(Ordering::Acquire));
        f.store(true, Ordering::Release);
        let r = std::panic::catch_unwind(|| {
            let _g = ClearOnDrop(&f);
            panic!("worker died");
        });
        assert!(r.is_err());
        assert!(!f.load(Ordering::Acquire));
    }
}
