//! Stock's DEX surface (indexer-kadena `90a4be1`, `schema.graphql`): 8 root fields, 17 object
//! types and 4 enums, beside Kadena's vendored schema (ZelCore extension, pinned in
//! `tests/extensions.txt`). Every root field answers `NOT_SUPPORTED` until the DEX serving
//! part lands (stock-gap plan B3); the store side is `kdx_hot::dex`. Until then a document that
//! names these fields validates, and its other fields still answer.
//!
//! The types are plain data so the serving part can fill them from `kdx_hot::dex` rows.

use async_graphql::{ComplexObject, Enum, Object, Result, SimpleObject, ID};

use crate::connection::PageInfo;
use crate::scalars::{DateTime, Decimal};
use crate::types::unserved;

/// Time frame for chart data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum TimeFrame {
    #[graphql(name = "DAY")]
    Day,
    #[graphql(name = "WEEK")]
    Week,
    #[graphql(name = "MONTH")]
    Month,
    #[graphql(name = "YEAR")]
    Year,
    #[graphql(name = "ALL")]
    All,
}

/// Sort options for pools.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum PoolOrderBy {
    #[graphql(name = "TVL_USD_ASC")]
    TvlUsdAsc,
    #[graphql(name = "TVL_USD_DESC")]
    TvlUsdDesc,
    #[graphql(name = "VOLUME_24H_ASC")]
    Volume24hAsc,
    #[graphql(name = "VOLUME_24H_DESC")]
    Volume24hDesc,
    #[graphql(name = "VOLUME_7D_ASC")]
    Volume7dAsc,
    #[graphql(name = "VOLUME_7D_DESC")]
    Volume7dDesc,
    #[graphql(name = "APR_24H_ASC")]
    Apr24hAsc,
    #[graphql(name = "APR_24H_DESC")]
    Apr24hDesc,
    #[graphql(name = "TRANSACTION_COUNT_24H_ASC")]
    TransactionCount24hAsc,
    #[graphql(name = "TRANSACTION_COUNT_24H_DESC")]
    TransactionCount24hDesc,
}

/// Transaction type for pool events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum PoolTransactionType {
    #[graphql(name = "SWAP")]
    Swap,
    #[graphql(name = "ADD_LIQUIDITY")]
    AddLiquidity,
    #[graphql(name = "REMOVE_LIQUIDITY")]
    RemoveLiquidity,
}

/// Sort options for liquidity positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum LiquidityPositionOrderBy {
    #[graphql(name = "VALUE_USD_ASC")]
    ValueUsdAsc,
    #[graphql(name = "VALUE_USD_DESC")]
    ValueUsdDesc,
    #[graphql(name = "LIQUIDITY_ASC")]
    LiquidityAsc,
    #[graphql(name = "LIQUIDITY_DESC")]
    LiquidityDesc,
    #[graphql(name = "APR_ASC")]
    AprAsc,
    #[graphql(name = "APR_DESC")]
    AprDesc,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct Token {
    pub id: ID,
    pub name: String,
    pub chain_id: String,
    pub address: Option<String>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct QueryTokensEdge {
    pub cursor: String,
    pub node: Token,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct QueryTokensConnection {
    pub edges: Vec<QueryTokensEdge>,
    pub page_info: PageInfo,
}

/// A single data point in a chart.
#[derive(Debug, Clone, SimpleObject)]
pub struct ChartDataPoint {
    pub timestamp: DateTime,
    pub value: Decimal,
}

/// Chart data for a pool.
#[derive(Debug, Clone, SimpleObject)]
pub struct PoolCharts {
    pub volume: Vec<ChartDataPoint>,
    pub tvl: Vec<ChartDataPoint>,
    pub fees: Vec<ChartDataPoint>,
}

/// A liquidity pool for a token pair.
#[derive(Debug, Clone, SimpleObject)]
#[graphql(complex)]
pub struct Pool {
    /// Served as `id` (the `Node` interface reads it through a resolver).
    #[graphql(skip)]
    pub pool_id: ID,
    pub address: String,
    pub token0: Token,
    pub token1: Token,
    pub reserve0: String,
    pub reserve1: String,
    pub total_supply: String,
    pub key: String,
    pub tvl_usd: f64,
    #[graphql(name = "tvlChange24h")]
    pub tvl_change24h: f64,
    #[graphql(name = "volume24hUsd")]
    pub volume24h_usd: f64,
    #[graphql(name = "volumeChange24h")]
    pub volume_change24h: f64,
    #[graphql(name = "volume7dUsd")]
    pub volume7d_usd: f64,
    #[graphql(name = "fees24hUsd")]
    pub fees24h_usd: f64,
    #[graphql(name = "feesChange24h")]
    pub fees_change24h: f64,
    #[graphql(name = "transactionCount24h")]
    pub transaction_count24h: i32,
    #[graphql(name = "transactionCountChange24h")]
    pub transaction_count_change24h: f64,
    #[graphql(name = "apr24h")]
    pub apr24h: f64,
    pub created_at: DateTime,
    pub updated_at: DateTime,
}

#[ComplexObject]
#[allow(unused_variables)]
impl Pool {
    pub async fn id(&self) -> Result<ID> {
        Ok(self.pool_id.clone())
    }

    async fn charts(&self, time_frame: TimeFrame) -> Result<PoolCharts> {
        unserved!("Pool.charts")
    }

    async fn transactions(
        &self,
        #[graphql(name = "type")] ty: Option<PoolTransactionType>,
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
    ) -> Result<Option<PoolTransactionsConnection>> {
        unserved!("Pool.transactions")
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct QueryPoolsConnectionEdge {
    pub cursor: String,
    pub node: Pool,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct QueryPoolsConnection {
    pub edges: Vec<QueryPoolsConnectionEdge>,
    pub page_info: PageInfo,
    pub total_count: i32,
}

/// A swap or liquidity transaction in a pool.
#[derive(Debug, Clone, SimpleObject)]
pub struct PoolTransaction {
    pub id: ID,
    pub maker: String,
    pub amount0_in: Decimal,
    pub amount1_in: Decimal,
    pub amount0_out: Decimal,
    pub amount1_out: Decimal,
    pub amount_usd: Decimal,
    pub timestamp: DateTime,
    pub transaction_id: Option<i32>,
    /// Stock's spelling (lower-case `k`).
    #[graphql(name = "requestkey")]
    pub requestkey: String,
    pub transaction_type: PoolTransactionType,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PoolTransactionEdge {
    pub cursor: String,
    pub node: PoolTransaction,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PoolTransactionsConnection {
    pub edges: Option<Vec<PoolTransactionEdge>>,
    pub page_info: Option<PageInfo>,
    pub total_count: Option<i32>,
}

/// Stock declares it but no field returns it; registered on the schema so introspection
/// matches stock's.
#[derive(Debug, Clone, SimpleObject)]
pub struct LiquidityBalance {
    pub id: i32,
    pub pair_id: String,
    pub liquidity: String,
    pub wallet_address: String,
    pub pair: Pool,
    pub created_at: DateTime,
    pub updated_at: DateTime,
}

/// A user's liquidity position in a pool.
#[derive(Debug, Clone, SimpleObject)]
pub struct LiquidityPosition {
    pub id: ID,
    pub pair_id: String,
    pub liquidity: String,
    pub wallet_address: String,
    pub value_usd: Decimal,
    #[graphql(name = "apr24h")]
    pub apr24h: Decimal,
    pub pair: Pool,
    pub created_at: DateTime,
    pub updated_at: DateTime,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct LiquidityPositionEdge {
    pub cursor: String,
    pub node: LiquidityPosition,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct LiquidityPositionsConnection {
    pub edges: Vec<LiquidityPositionEdge>,
    pub page_info: PageInfo,
    pub total_count: i32,
}

/// DEX metrics including TVL, volume, and pool count.
#[derive(Debug, Clone, SimpleObject)]
pub struct DexMetrics {
    pub total_pools: i32,
    pub current_tvl_usd: Decimal,
    pub tvl_history: Vec<ChartDataPoint>,
    pub volume_history: Vec<ChartDataPoint>,
    pub total_volume_usd: Decimal,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct TokenPrice {
    pub id: ID,
    pub token: Token,
    pub price_in_kda: Decimal,
    pub price_in_usd: Decimal,
    pub protocol_address: String,
    pub updated_at: DateTime,
}

fn tvl_usd_desc() -> Option<PoolOrderBy> {
    Some(PoolOrderBy::TvlUsdDesc)
}

fn value_usd_desc() -> Option<LiquidityPositionOrderBy> {
    Some(LiquidityPositionOrderBy::ValueUsdDesc)
}

/// The DEX root fields, merged into `Query` (`schema::QueryRoot`).
#[derive(Default)]
pub struct DexQuery;

#[Object(name = "DexQuery")]
#[allow(unused_variables, clippy::too_many_arguments)]
impl DexQuery {
    async fn tokens(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<QueryTokensConnection> {
        unserved!("Query.tokens")
    }

    /// Retrieve liquidity pools. Default page size is 20.
    async fn pools(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
        #[graphql(default_with = "tvl_usd_desc()")] order_by: Option<PoolOrderBy>,
        protocol_address: Option<String>,
    ) -> Result<QueryPoolsConnection> {
        unserved!("Query.pools")
    }

    /// Retrieve a specific pool by its ID.
    async fn pool(
        &self,
        id: ID,
        time_frame: Option<TimeFrame>,
        #[graphql(name = "type")] ty: Option<PoolTransactionType>,
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
    ) -> Result<Option<Pool>> {
        unserved!("Query.pool")
    }

    async fn pool_transactions(
        &self,
        pair_id: String,
        #[graphql(name = "type")] ty: Option<PoolTransactionType>,
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
    ) -> Result<Option<PoolTransactionsConnection>> {
        unserved!("Query.poolTransactions")
    }

    /// Get user's liquidity positions.
    async fn liquidity_positions(
        &self,
        wallet_address: String,
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
        #[graphql(default_with = "value_usd_desc()")] order_by: Option<LiquidityPositionOrderBy>,
    ) -> Result<LiquidityPositionsConnection> {
        unserved!("Query.liquidityPositions")
    }

    /// Get DEX metrics including TVL, volume, and pool count.
    async fn dex_metrics(
        &self,
        start_date: Option<DateTime>,
        end_date: Option<DateTime>,
        protocol_address: Option<String>,
    ) -> Result<DexMetrics> {
        unserved!("Query.dexMetrics")
    }

    /// Get price for a specific token.
    async fn token_price(
        &self,
        token_address: String,
        protocol_address: Option<String>,
    ) -> Result<Option<TokenPrice>> {
        unserved!("Query.tokenPrice")
    }

    /// Get prices for all tokens in a protocol.
    async fn token_prices(&self, protocol_address: Option<String>) -> Result<Vec<TokenPrice>> {
        unserved!("Query.tokenPrices")
    }
}

/// The served `Query` root: Kadena's fields (`schema::Query`) and the DEX fields.
#[derive(async_graphql::MergedObject)]
#[graphql(name = "Query")]
pub struct QueryRoot(pub crate::schema::Query, pub DexQuery);
