//! The `Query` root. Root fields not served yet answer `NOT_SUPPORTED` (Tasks 3–5 fill them
//! in); `pactQuery` and `gasLimitEstimate` stay `NOT_SUPPORTED` (GQ-3, Q9).

use async_graphql::{Context, InputObject, Object, Result, SimpleObject, ID};

use crate::connection::PageArgs;
use crate::scalars::BigInt;
use crate::types::{
    default_chain_ids, unserved, Block, FungibleAccount, FungibleChainAccount, GraphConfiguration,
    NetworkInfo, Node, NonFungibleAccount, NonFungibleChainAccount, QueryBlocksFromDepthConnection,
    QueryBlocksFromDepthConnectionEdge, QueryBlocksFromHeightConnection,
    QueryBlocksFromHeightConnectionEdge, QueryCompletedBlockHeightsConnection,
    QueryCompletedBlockHeightsConnectionEdge, QueryEventsConnection,
    QueryTransactionsByPublicKeyConnection, QueryTransactionsConnection, QueryTransfersConnection,
    Transaction,
};

pub struct Query;

fn coin() -> Option<String> {
    Some("coin".to_owned())
}

#[derive(Debug, Clone, InputObject)]
pub struct PactQuery {
    pub chain_id: String,
    pub code: String,
    pub data: Option<Vec<PactQueryData>>,
}

#[derive(Debug, Clone, InputObject)]
pub struct PactQueryData {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PactQueryResponse {
    pub chain_id: String,
    pub code: String,
    pub error: Option<String>,
    pub result: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct GasLimitEstimation {
    pub amount: i32,
    pub input_type: String,
    pub transaction: String,
    pub used_preflight: bool,
    pub used_signature_verification: bool,
}

#[Object]
#[allow(unused_variables, clippy::too_many_arguments)]
impl Query {
    async fn block(&self, ctx: &Context<'_>, hash: String) -> Result<Option<Block>> {
        crate::types::block::query_block(ctx, &hash).await
    }

    async fn blocks_from_depth(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        #[graphql(default_with = "default_chain_ids()")] chain_ids: Option<Vec<String>>,
        first: Option<i32>,
        last: Option<i32>,
        minimum_depth: i32,
    ) -> Result<Option<QueryBlocksFromDepthConnection>> {
        use crate::types::block::{block_page, chain_set, non_negative, Span};
        let depth = non_negative("minimumDepth", minimum_depth)?;
        let (edges, page_info) = block_page(
            ctx,
            chain_set(chain_ids.as_deref())?,
            Span::Depth(depth),
            PageArgs::new(after, before, first, last),
        )
        .await?;
        Ok(Some(QueryBlocksFromDepthConnection {
            edges: edges
                .into_iter()
                .map(|(cursor, node)| QueryBlocksFromDepthConnectionEdge { cursor, node })
                .collect(),
            page_info,
        }))
    }

    async fn blocks_from_height(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        #[graphql(default_with = "default_chain_ids()")] chain_ids: Option<Vec<String>>,
        end_height: Option<i32>,
        first: Option<i32>,
        last: Option<i32>,
        start_height: i32,
    ) -> Result<QueryBlocksFromHeightConnection> {
        use crate::types::block::{block_page, chain_set, non_negative, Span};
        let start = non_negative("startHeight", start_height)?;
        let end = end_height
            .map(|e| non_negative("endHeight", e))
            .transpose()?;
        let (edges, page_info) = block_page(
            ctx,
            chain_set(chain_ids.as_deref())?,
            Span::Heights { start, end },
            PageArgs::new(after, before, first, last),
        )
        .await?;
        Ok(QueryBlocksFromHeightConnection {
            edges: edges
                .into_iter()
                .map(|(cursor, node)| QueryBlocksFromHeightConnectionEdge { cursor, node })
                .collect(),
            page_info,
        })
    }

    async fn completed_block_heights(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        #[graphql(default_with = "default_chain_ids()")] chain_ids: Option<Vec<String>>,
        #[graphql(default_with = "Some(true)")] completed_heights: Option<bool>,
        first: Option<i32>,
        #[graphql(default_with = "Some(3)")] height_count: Option<i32>,
        last: Option<i32>,
    ) -> Result<QueryCompletedBlockHeightsConnection> {
        use crate::types::block::{block_page, chain_set, Span, MAX_HEIGHT_COUNT};
        let count = height_count.unwrap_or(3);
        if !(0..=MAX_HEIGHT_COUNT).contains(&count) {
            return Err(crate::error::bad_input(format!(
                "heightCount must be between 0 and {MAX_HEIGHT_COUNT}"
            )));
        }
        let (edges, page_info) = block_page(
            ctx,
            chain_set(chain_ids.as_deref())?,
            Span::Completed {
                count: count as u64,
                complete: completed_heights.unwrap_or(true),
            },
            PageArgs::new(after, before, first, last),
        )
        .await?;
        Ok(QueryCompletedBlockHeightsConnection {
            edges: edges
                .into_iter()
                .map(|(cursor, node)| QueryCompletedBlockHeightsConnectionEdge { cursor, node })
                .collect(),
            page_info,
        })
    }

    async fn events(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        block_hash: Option<String>,
        chain_id: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
        max_height: Option<i32>,
        min_height: Option<i32>,
        minimum_depth: Option<i32>,
        order_index: Option<i32>,
        parameters_filter: Option<String>,
        qualified_event_name: String,
        request_key: Option<String>,
    ) -> Result<QueryEventsConnection> {
        crate::types::tx::query_events(
            ctx,
            after,
            before,
            block_hash,
            chain_id,
            first,
            last,
            max_height,
            min_height,
            minimum_depth,
            order_index,
            parameters_filter,
            qualified_event_name,
            request_key,
        )
        .await
    }

    async fn fungible_account(
        &self,
        ctx: &Context<'_>,
        account_name: String,
        #[graphql(default_with = "coin()")] fungible_name: Option<String>,
    ) -> Result<Option<FungibleAccount>> {
        crate::types::account::fungible_account(ctx, &account_name, fungible_name.as_deref()).await
    }

    async fn fungible_accounts_by_public_key(
        &self,
        ctx: &Context<'_>,
        fungible_name: Option<String>,
        public_key: String,
    ) -> Result<Vec<FungibleAccount>> {
        crate::types::account::accounts_by_public_key(
            ctx,
            &public_key,
            fungible_name.as_deref(),
            None,
        )
        .await
    }

    async fn fungible_chain_account(
        &self,
        ctx: &Context<'_>,
        account_name: String,
        chain_id: String,
        #[graphql(default_with = "coin()")] fungible_name: Option<String>,
    ) -> Result<Option<FungibleChainAccount>> {
        Ok(crate::types::account::fungible_chain_accounts(
            ctx,
            &account_name,
            &[chain_id],
            fungible_name.as_deref(),
        )
        .await?
        .and_then(|v| v.into_iter().next()))
    }

    async fn fungible_chain_accounts(
        &self,
        ctx: &Context<'_>,
        account_name: String,
        #[graphql(default_with = "default_chain_ids()")] chain_ids: Option<Vec<String>>,
        #[graphql(default_with = "coin()")] fungible_name: Option<String>,
    ) -> Result<Option<Vec<FungibleChainAccount>>> {
        crate::types::account::fungible_chain_accounts(
            ctx,
            &account_name,
            &chain_ids.unwrap_or_default(),
            fungible_name.as_deref(),
        )
        .await
    }

    async fn fungible_chain_accounts_by_public_key(
        &self,
        ctx: &Context<'_>,
        chain_id: String,
        fungible_name: Option<String>,
        public_key: String,
    ) -> Result<Vec<FungibleChainAccount>> {
        Ok(crate::types::account::accounts_by_public_key(
            ctx,
            &public_key,
            fungible_name.as_deref(),
            Some(crate::txlist::chain_arg(&chain_id)?),
        )
        .await?
        .into_iter()
        .flat_map(|a| {
            let (account, module) = (a.account, a.module);
            a.rows.into_iter().map(move |row| FungibleChainAccount {
                account: account.clone(),
                module: module.clone(),
                row,
            })
        })
        .collect())
    }

    /// Not served (Q9): the indexer does not run Pact.
    async fn gas_limit_estimate(&self, input: Vec<String>) -> Result<Vec<GasLimitEstimation>> {
        unserved!("Query.gasLimitEstimate")
    }

    async fn graph_configuration(&self) -> GraphConfiguration {
        GraphConfiguration::current()
    }

    async fn last_block_height(&self, ctx: &Context<'_>) -> Result<Option<BigInt>> {
        crate::types::block::last_block_height(ctx).await
    }

    async fn network_info(&self, ctx: &Context<'_>) -> Result<Option<NetworkInfo>> {
        crate::types::network::network_info(ctx).await
    }

    async fn node(&self, ctx: &Context<'_>, id: ID) -> Result<Option<Node>> {
        crate::types::node::node(ctx, &id).await
    }

    async fn nodes(&self, ctx: &Context<'_>, ids: Vec<ID>) -> Result<Vec<Option<Node>>> {
        crate::types::node::nodes(ctx, &ids).await
    }

    async fn non_fungible_account(
        &self,
        ctx: &Context<'_>,
        account_name: String,
    ) -> Result<Option<NonFungibleAccount>> {
        Ok(crate::types::nft::non_fungible(ctx, &account_name, None)
            .await?
            .map(|(account, items)| NonFungibleAccount { account, items }))
    }

    async fn non_fungible_chain_account(
        &self,
        ctx: &Context<'_>,
        account_name: String,
        chain_id: String,
    ) -> Result<Option<NonFungibleChainAccount>> {
        Ok(crate::types::nft::non_fungible(
            ctx,
            &account_name,
            Some(crate::txlist::chain_arg(&chain_id)?),
        )
        .await?
        .map(|(account, items)| NonFungibleChainAccount {
            account,
            chain: crate::txlist::chain_arg(&chain_id).unwrap_or(0),
            items,
        }))
    }

    /// Not served (Q9): arbitrary Pact code is never run for clients.
    async fn pact_query(&self, pact_query: Vec<PactQuery>) -> Result<Vec<PactQueryResponse>> {
        unserved!("Query.pactQuery")
    }

    async fn transaction(
        &self,
        ctx: &Context<'_>,
        block_hash: Option<String>,
        minimum_depth: Option<i32>,
        request_key: String,
    ) -> Result<Option<Transaction>> {
        crate::types::tx::query_transaction(ctx, block_hash, minimum_depth, request_key).await
    }

    async fn transactions(
        &self,
        ctx: &Context<'_>,
        account_name: Option<String>,
        after: Option<String>,
        before: Option<String>,
        block_hash: Option<String>,
        chain_id: Option<String>,
        first: Option<i32>,
        fungible_name: Option<String>,
        last: Option<i32>,
        max_height: Option<i32>,
        min_height: Option<i32>,
        minimum_depth: Option<i32>,
        request_key: Option<String>,
    ) -> Result<QueryTransactionsConnection> {
        crate::types::tx::query_transactions(
            ctx,
            account_name,
            after,
            before,
            block_hash,
            chain_id,
            first,
            fungible_name,
            last,
            max_height,
            min_height,
            minimum_depth,
            request_key,
        )
        .await
    }

    async fn transactions_by_public_key(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
        public_key: String,
    ) -> Result<QueryTransactionsByPublicKeyConnection> {
        crate::types::tx::query_transactions_by_public_key(
            ctx, after, before, first, last, public_key,
        )
        .await
    }

    async fn transfers(
        &self,
        ctx: &Context<'_>,
        account_name: Option<String>,
        after: Option<String>,
        before: Option<String>,
        block_hash: Option<String>,
        chain_id: Option<String>,
        first: Option<i32>,
        fungible_name: Option<String>,
        last: Option<i32>,
        request_key: Option<String>,
    ) -> Result<QueryTransfersConnection> {
        crate::types::tx::query_transfers(
            ctx,
            account_name,
            after,
            before,
            block_hash,
            chain_id,
            first,
            fungible_name,
            last,
            request_key,
        )
        .await
    }
}
