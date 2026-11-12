//! The `Query` root. Root fields not served yet answer `NOT_SUPPORTED` (Tasks 3–5 fill them
//! in); `pactQuery` and `gasLimitEstimate` stay `NOT_SUPPORTED` (GQ-3, Q9).

use async_graphql::{Context, InputObject, Object, Result, SimpleObject, ID};

use crate::scalars::BigInt;
use crate::types::{
    default_chain_ids, unserved, Block, FungibleAccount, FungibleChainAccount, GraphConfiguration,
    NetworkInfo, Node, NonFungibleAccount, NonFungibleChainAccount, QueryBlocksFromDepthConnection,
    QueryBlocksFromHeightConnection, QueryCompletedBlockHeightsConnection, QueryEventsConnection,
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
    async fn block(&self, hash: String) -> Result<Option<Block>> {
        unserved!("Query.block")
    }

    async fn blocks_from_depth(
        &self,
        after: Option<String>,
        before: Option<String>,
        #[graphql(default_with = "default_chain_ids()")] chain_ids: Option<Vec<String>>,
        first: Option<i32>,
        last: Option<i32>,
        minimum_depth: i32,
    ) -> Result<Option<QueryBlocksFromDepthConnection>> {
        unserved!("Query.blocksFromDepth")
    }

    async fn blocks_from_height(
        &self,
        after: Option<String>,
        before: Option<String>,
        #[graphql(default_with = "default_chain_ids()")] chain_ids: Option<Vec<String>>,
        end_height: Option<i32>,
        first: Option<i32>,
        last: Option<i32>,
        start_height: i32,
    ) -> Result<QueryBlocksFromHeightConnection> {
        unserved!("Query.blocksFromHeight")
    }

    async fn completed_block_heights(
        &self,
        after: Option<String>,
        before: Option<String>,
        #[graphql(default_with = "default_chain_ids()")] chain_ids: Option<Vec<String>>,
        #[graphql(default_with = "Some(true)")] completed_heights: Option<bool>,
        first: Option<i32>,
        #[graphql(default_with = "Some(3)")] height_count: Option<i32>,
        last: Option<i32>,
    ) -> Result<QueryCompletedBlockHeightsConnection> {
        unserved!("Query.completedBlockHeights")
    }

    async fn events(
        &self,
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
        unserved!("Query.events")
    }

    async fn fungible_account(
        &self,
        account_name: String,
        #[graphql(default_with = "coin()")] fungible_name: Option<String>,
    ) -> Result<Option<FungibleAccount>> {
        unserved!("Query.fungibleAccount")
    }

    async fn fungible_accounts_by_public_key(
        &self,
        fungible_name: Option<String>,
        public_key: String,
    ) -> Result<Vec<FungibleAccount>> {
        unserved!("Query.fungibleAccountsByPublicKey")
    }

    async fn fungible_chain_account(
        &self,
        account_name: String,
        chain_id: String,
        #[graphql(default_with = "coin()")] fungible_name: Option<String>,
    ) -> Result<Option<FungibleChainAccount>> {
        unserved!("Query.fungibleChainAccount")
    }

    async fn fungible_chain_accounts(
        &self,
        account_name: String,
        #[graphql(default_with = "default_chain_ids()")] chain_ids: Option<Vec<String>>,
        #[graphql(default_with = "coin()")] fungible_name: Option<String>,
    ) -> Result<Option<Vec<FungibleChainAccount>>> {
        unserved!("Query.fungibleChainAccounts")
    }

    async fn fungible_chain_accounts_by_public_key(
        &self,
        chain_id: String,
        fungible_name: Option<String>,
        public_key: String,
    ) -> Result<Vec<FungibleChainAccount>> {
        unserved!("Query.fungibleChainAccountsByPublicKey")
    }

    /// Not served (Q9): the indexer does not run Pact.
    async fn gas_limit_estimate(&self, input: Vec<String>) -> Result<Vec<GasLimitEstimation>> {
        unserved!("Query.gasLimitEstimate")
    }

    async fn graph_configuration(&self) -> GraphConfiguration {
        GraphConfiguration::current()
    }

    async fn last_block_height(&self) -> Result<Option<BigInt>> {
        unserved!("Query.lastBlockHeight")
    }

    async fn network_info(&self, ctx: &Context<'_>) -> Result<Option<NetworkInfo>> {
        unserved!("Query.networkInfo")
    }

    async fn node(&self, id: ID) -> Result<Option<Node>> {
        unserved!("Query.node")
    }

    async fn nodes(&self, ids: Vec<ID>) -> Result<Vec<Option<Node>>> {
        unserved!("Query.nodes")
    }

    async fn non_fungible_account(
        &self,
        account_name: String,
    ) -> Result<Option<NonFungibleAccount>> {
        unserved!("Query.nonFungibleAccount")
    }

    async fn non_fungible_chain_account(
        &self,
        account_name: String,
        chain_id: String,
    ) -> Result<Option<NonFungibleChainAccount>> {
        unserved!("Query.nonFungibleChainAccount")
    }

    /// Not served (Q9): arbitrary Pact code is never run for clients.
    async fn pact_query(&self, pact_query: Vec<PactQuery>) -> Result<Vec<PactQueryResponse>> {
        unserved!("Query.pactQuery")
    }

    async fn transaction(
        &self,
        block_hash: Option<String>,
        minimum_depth: Option<i32>,
        request_key: String,
    ) -> Result<Option<Transaction>> {
        unserved!("Query.transaction")
    }

    async fn transactions(
        &self,
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
        unserved!("Query.transactions")
    }

    async fn transactions_by_public_key(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
        public_key: String,
    ) -> Result<QueryTransactionsByPublicKeyConnection> {
        unserved!("Query.transactionsByPublicKey")
    }

    async fn transfers(
        &self,
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
        unserved!("Query.transfers")
    }
}
