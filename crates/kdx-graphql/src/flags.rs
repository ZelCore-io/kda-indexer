//! Response-level flags (R-P5P9-14): resolvers mark a request, and the response carries the
//! marks in its top-level `extensions`.
//!
//! - `totalCountLowerBound: true`: a `totalCount` ran out of its share of the deadline; it
//!   answers the items counted so far (a lower bound) and the edges are kept.
//! - `searchTruncated: true`: `transactionsByPactCode` stopped at a search cap or its time
//!   budget; `pageInfo.endCursor` resumes it.
//! - `truncated: true`: a `fungibleAccountsByPublicKey` or `fungibleChainAccountsByPublicKey`
//!   stopped at its 100 matching accounts or its scan window with `KeyAccounts` rows of the
//!   key left unread (L-2).
//!
//! One [`RespFlags`] per request (an extension instance is created per request), so the
//! requests of a batch are flagged separately. Subscription events carry no flags.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_graphql::extensions::{
    Extension, ExtensionContext, ExtensionFactory, NextExecute, NextPrepareRequest,
};
use async_graphql::{Context, Request, Response, ServerResult, Value};

/// The marks of one request.
#[derive(Debug, Default)]
pub struct RespFlags {
    count_lower_bound: AtomicBool,
    search_truncated: AtomicBool,
    key_accounts_truncated: AtomicBool,
}

/// Which mark to set.
#[derive(Debug, Clone, Copy)]
pub enum Flag {
    CountLowerBound,
    SearchTruncated,
    KeyAccountsTruncated,
}

impl RespFlags {
    fn cell(&self, f: Flag) -> &AtomicBool {
        match f {
            Flag::CountLowerBound => &self.count_lower_bound,
            Flag::SearchTruncated => &self.search_truncated,
            Flag::KeyAccountsTruncated => &self.key_accounts_truncated,
        }
    }
    pub fn is_set(&self, f: Flag) -> bool {
        self.cell(f).load(Ordering::Relaxed)
    }
}

/// Mark the request being resolved (no-op outside one, e.g. a subscription event).
pub fn mark(ctx: &Context<'_>, f: Flag) {
    if let Some(r) = ctx.data_opt::<Arc<RespFlags>>() {
        r.cell(f).store(true, Ordering::Relaxed);
    }
}

/// The extension (installed by `build_schema`).
pub struct Flags;

impl ExtensionFactory for Flags {
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(FlagsExt(Arc::new(RespFlags::default())))
    }
}

struct FlagsExt(Arc<RespFlags>);

#[async_graphql::async_trait::async_trait]
impl Extension for FlagsExt {
    async fn prepare_request(
        &self,
        ctx: &ExtensionContext<'_>,
        request: Request,
        next: NextPrepareRequest<'_>,
    ) -> ServerResult<Request> {
        // The request's `crossChainTransfer` step budget rides with its flags (one per
        // request, as the flags). This runs once per subscription stream, so `ws::WsOps`
        // gives each subscription message a fresh one that shadows it.
        let budget = Arc::new(crate::txlist::XchainBudget::default());
        next.run(ctx, request.data(self.0.clone()).data(budget))
            .await
    }

    async fn execute(
        &self,
        ctx: &ExtensionContext<'_>,
        operation_name: Option<&str>,
        next: NextExecute<'_>,
    ) -> Response {
        let mut r = next.run(ctx, operation_name).await;
        for (f, name) in [
            (Flag::CountLowerBound, "totalCountLowerBound"),
            (Flag::SearchTruncated, "searchTruncated"),
            (Flag::KeyAccountsTruncated, "truncated"),
        ] {
            if self.0.is_set(f) {
                r.extensions.insert(name.to_owned(), Value::Boolean(true));
            }
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_graphql::{EmptyMutation, EmptySubscription, Object, Schema};

    use crate::connection::Total;

    struct Q;

    #[Object]
    impl Q {
        async fn exact(&self, ctx: &async_graphql::Context<'_>) -> async_graphql::Result<i32> {
            Total::Deferred(Arc::new(|| Box::pin(async { Ok((7, true)) })))
                .resolve(ctx)
                .await
        }
        async fn bound(&self, ctx: &async_graphql::Context<'_>) -> async_graphql::Result<i32> {
            Total::Deferred(Arc::new(|| Box::pin(async { Ok((5, false)) })))
                .resolve(ctx)
                .await
        }
    }

    struct B;

    #[Object]
    impl B {
        /// Spend 5 reads of the request's `crossChainTransfer` budget; what is left.
        async fn spend(&self, ctx: &async_graphql::Context<'_>) -> i32 {
            let b = ctx
                .data_opt::<Arc<crate::txlist::XchainBudget>>()
                .expect("installed per request");
            b.take(5);
            b.left() as i32
        }
    }

    /// The security finding on e7da3d0: one `crossChainTransfer` budget per request, shared
    /// by its fields, fresh for the next request.
    #[tokio::test]
    async fn each_request_has_one_xchain_budget() {
        let s = Schema::build(B, EmptyMutation, EmptySubscription)
            .extension(super::Flags)
            .finish();
        let full = crate::txlist::XCHAIN_BUDGET as i32;
        for _ in 0..2 {
            let r = s.execute("{ a: spend b: spend }").await;
            assert!(r.errors.is_empty(), "{:?}", r.errors);
            let v = r.data.into_json().unwrap();
            let mut left = [v["a"].as_i64().unwrap(), v["b"].as_i64().unwrap()];
            left.sort_unstable();
            assert_eq!(left, [i64::from(full - 10), i64::from(full - 5)]);
        }
    }

    #[tokio::test]
    async fn a_count_cut_short_is_a_flagged_lower_bound() {
        let s = Schema::build(Q, EmptyMutation, EmptySubscription)
            .extension(super::Flags)
            .finish();
        let r = s.execute("{ exact }").await;
        assert!(r.errors.is_empty() && r.extensions.is_empty());
        let r = s.execute("{ exact bound }").await;
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert_eq!(
            r.data.into_json().unwrap(),
            serde_json::json!({"exact": 7, "bound": 5})
        );
        assert_eq!(
            r.extensions.get("totalCountLowerBound"),
            Some(&async_graphql::Value::Boolean(true))
        );
    }
}
