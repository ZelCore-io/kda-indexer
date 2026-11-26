//! Response-level flags (R-P5P9-14): resolvers mark a request, and the response carries the
//! marks in its top-level `extensions`.
//!
//! - `searchTruncated: true`: `transactionsByPactCode` stopped at a search cap or its time
//!   budget; `pageInfo.endCursor` resumes it.
//! - `truncated: true`: a `fungibleAccountsByPublicKey` or `fungibleChainAccountsByPublicKey`
//!   stopped at its 100 matching accounts or its scan window with `KeyAccounts` rows of the
//!   key left unread (L-2).
//! - `nodeReadIncomplete: true`: an account lookup's node read of a chain the store has no
//!   row for failed or was refused by a limit (gap Q, `crate::miss`); that chain is left
//!   out, as stock leaves out a chain whose node read failed.
//! - `dexPairsCapped: true`: a DEX listing (`pools`, `dexMetrics`, `liquidityPositions`,
//!   `tokenPrices`) had more listable pairs than `dexserve::MAX_PAIRS` and served the ones
//!   with the largest coin reserve.
//! - `dexPricesPartial: true`: a `tokenPrice` or `tokenPrices` price search ran out of its row
//!   budget or the request deadline; the tokens it could not finish answer 0 (stock's answer
//!   with no path), so a 0 in such a response may be a real price left unsearched.
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
    search_truncated: AtomicBool,
    key_accounts_truncated: AtomicBool,
    node_read_incomplete: AtomicBool,
    dex_pairs_capped: AtomicBool,
    dex_prices_partial: AtomicBool,
}

/// Which mark to set.
#[derive(Debug, Clone, Copy)]
pub enum Flag {
    SearchTruncated,
    KeyAccountsTruncated,
    /// Gap Q: a missed account row's node read failed or was refused by a limit.
    NodeReadIncomplete,
    /// A DEX listing served `dexserve::MAX_PAIRS` of more listable pairs.
    DexPairsCapped,
    /// A DEX price search was cut by the budget or deadline (some prices answer 0).
    DexPricesPartial,
}

impl RespFlags {
    fn cell(&self, f: Flag) -> &AtomicBool {
        match f {
            Flag::SearchTruncated => &self.search_truncated,
            Flag::KeyAccountsTruncated => &self.key_accounts_truncated,
            Flag::NodeReadIncomplete => &self.node_read_incomplete,
            Flag::DexPairsCapped => &self.dex_pairs_capped,
            Flag::DexPricesPartial => &self.dex_prices_partial,
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
            (Flag::SearchTruncated, "searchTruncated"),
            (Flag::KeyAccountsTruncated, "truncated"),
            (Flag::NodeReadIncomplete, "nodeReadIncomplete"),
            (Flag::DexPairsCapped, "dexPairsCapped"),
            (Flag::DexPricesPartial, "dexPricesPartial"),
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
            Total::Deferred(Arc::new(|| Box::pin(async { Ok(7) })))
                .resolve(ctx)
                .await
        }
        async fn cut(&self, ctx: &async_graphql::Context<'_>) -> async_graphql::Result<i32> {
            Total::Deferred(Arc::new(|| {
                Box::pin(async { Err(crate::txlist::count_error(kdx_api::ApiError::Deadline)) })
            }))
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

    /// GQ-10: `totalCount` is exact or an error. A cut count answers `COUNT_TIMEOUT` at its
    /// field (the rest of the response stays) and never sets a response flag.
    #[tokio::test]
    async fn a_cut_count_is_an_error_never_a_flagged_lower_bound() {
        let s = Schema::build(Q, EmptyMutation, EmptySubscription)
            .extension(super::Flags)
            .finish();
        let r = s.execute("{ exact }").await;
        assert!(r.errors.is_empty() && r.extensions.is_empty());
        let r = s.execute("{ exact cut }").await;
        assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
        let code = r.errors[0].extensions.as_ref().and_then(|e| e.get("code"));
        assert_eq!(
            code,
            Some(&async_graphql::Value::String("COUNT_TIMEOUT".into()))
        );
        assert!(r.extensions.is_empty(), "{:?}", r.extensions);
        let d = r.data.into_json().unwrap();
        assert_eq!(d["exact"], 7);
        // Review M-5: async-graphql leaves a failed field out of its object (no null
        // propagation): `cut` is absent, not `null`.
        assert!(d.get("cut").is_none(), "{d}");
    }
}
