//! Stock's DEX surface (indexer-kadena `90a4be1`, `schema.graphql`): 8 root fields, 17 object
//! types and 4 enums, beside Kadena's vendored schema (ZelCore extension, pinned in
//! `tests/extensions.txt`), served from the Sushi ledger (`kdx_hot::dex`) and the hourly
//! KDA/USD rate (`kdx_hot::dex_usd`) by [`crate::dexserve`] (stock-gap plan B3), which lists
//! the formulas and the deliberate divergences from stock.

use async_graphql::{ComplexObject, Context, Enum, Object, Result, SimpleObject, ID};
use kdx_api::{ApiResult, View};

use crate::connection::PageInfo;
use crate::dexserve::{self as ds, Budget, TxArgs};
use crate::scalars::{DateTime, Decimal};

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Run `f` on a read slot (`heavy`: a heavy slot, for aggregates over many rows) with the
/// request's deadline as its budget. `NOT_AVAILABLE` until the DEX history fill has finished
/// (`kdx_hot::dex::meta::DEX_FILLED`): before it the ledger holds only part of the history,
/// so every DEX answer would be silently wrong (as holders and the row counts answer
/// `NOT_AVAILABLE` until ready). A listing cut at [`ds::MAX_PAIRS`] flags the response
/// `extensions.dexPairsCapped`.
async fn run<T, F>(ctx: &Context<'_>, heavy: bool, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&View, &mut Budget) -> ApiResult<T> + Send + 'static,
{
    run_gated(ctx, heavy, dex_gate, f).await
}

fn unavailable(e: anyhow::Error) -> kdx_api::ApiError {
    kdx_api::ApiError::Unavailable(format!("{e:#}"))
}

/// Why the DEX fields cannot answer yet (None: they can): the history fill has not finished,
/// or it failed and is retrying (`kdx_hot::fills`), each with its own message.
fn dex_gate(view: &View) -> ApiResult<Option<&'static str>> {
    use kdx_hot::Txn;
    if view
        .txn
        .meta(kdx_hot::dex::meta::DEX_FILLED)
        .map_err(unavailable)?
        .is_some()
    {
        return Ok(None);
    }
    Ok(Some(
        if kdx_hot::fills::failed(&view.txn, kdx_hot::fills::DEX)
            .map_err(unavailable)?
            .is_some()
        {
            "the DEX history fill failed and is retrying; DEX fields answer once it succeeds"
        } else {
            "the DEX history is being filled; DEX fields answer once it is done"
        },
    ))
}

/// `tokens` reads the token chain list (`kdx_hot::token_chains`), not the DEX ledger.
fn tokens_gate(view: &View) -> ApiResult<Option<&'static str>> {
    if kdx_hot::token_chains::ready(&view.txn).map_err(unavailable)? {
        return Ok(None);
    }
    Ok(Some(
        if kdx_hot::fills::failed(&view.txn, kdx_hot::fills::TOKEN_CHAINS)
            .map_err(unavailable)?
            .is_some()
        {
            "the token list build failed and is retrying; tokens answers once it succeeds"
        } else {
            "the token list is being built (it follows the module holder lists); tokens answers once it is done"
        },
    ))
}

async fn run_gated<T, F>(
    ctx: &Context<'_>,
    heavy: bool,
    gate: fn(&View) -> ApiResult<Option<&'static str>>,
    f: F,
) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&View, &mut Budget) -> ApiResult<T> + Send + 'static,
{
    let dl = crate::deadline(ctx);
    let st = crate::gql(ctx).state()?;
    let go = move |view: &View| -> ApiResult<Result<(T, bool), &'static str>> {
        view.check_deadline()?;
        if let Some(why) = gate(view)? {
            return Ok(Err(why));
        }
        let mut b = Budget::new(dl);
        let t = f(view, &mut b)?;
        Ok(Ok((t, b.capped)))
    };
    let r = if heavy {
        st.read_heavy(go).await
    } else {
        st.read(go).await
    };
    let (t, capped) = r
        .map_err(crate::error::from_api)?
        .map_err(|why| crate::error::err(crate::error::Code::NotAvailable, why))?;
    if capped {
        crate::flags::mark(ctx, crate::flags::Flag::DexPairsCapped);
    }
    Ok(t)
}

fn protocol(p: Option<String>) -> String {
    p.filter(|s| !s.is_empty())
        .unwrap_or_else(|| ds::DEFAULT_PROTOCOL.to_owned())
}

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
    /// The transaction arguments of `pool(...)` (stock reads them there).
    #[graphql(skip)]
    pub tx_args: TxArgs,
}

#[ComplexObject]
impl Pool {
    pub async fn id(&self) -> Result<ID> {
        Ok(self.pool_id.clone())
    }

    /// Volume and fees per hour (`DAY`) or day, TVL per day (the newest point for `DAY`).
    async fn charts(&self, ctx: &Context<'_>, time_frame: TimeFrame) -> Result<PoolCharts> {
        let id = self.pool_id.0.clone();
        let now = now_secs();
        run(ctx, true, move |v, b| {
            ds::charts(&v.txn, &id, time_frame, now, b)
        })
        .await
    }

    /// Stock takes these from `pool(...)`; arguments given here take precedence.
    async fn transactions(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "type")] ty: Option<PoolTransactionType>,
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
    ) -> Result<Option<PoolTransactionsConnection>> {
        let given = ty.is_some()
            || first.is_some()
            || after.is_some()
            || last.is_some()
            || before.is_some();
        let a = if given {
            TxArgs {
                ty,
                first,
                after,
                last,
                before,
            }
        } else {
            self.tx_args.clone()
        };
        let id = self.pool_id.0.clone();
        run(ctx, false, move |v, b| {
            ds::pool_transactions(v, &id, &a, b).map(Some)
        })
        .await
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
#[allow(clippy::too_many_arguments)]
impl DexQuery {
    async fn tokens(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<QueryTokensConnection> {
        run_gated(ctx, true, tokens_gate, move |v, b| {
            ds::tokens(v, after.as_deref(), before.as_deref(), first, last, b)
        })
        .await
    }

    /// Retrieve liquidity pools. Default page size is 20.
    async fn pools(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
        #[graphql(default_with = "tvl_usd_desc()")] order_by: Option<PoolOrderBy>,
        protocol_address: Option<String>,
    ) -> Result<QueryPoolsConnection> {
        let (now, proto) = (now_secs(), protocol(protocol_address));
        let order = order_by.unwrap_or(PoolOrderBy::TvlUsdDesc);
        run(ctx, true, move |v, b| {
            ds::pools(
                &v.txn,
                now,
                order,
                &proto,
                after.as_deref(),
                before.as_deref(),
                first,
                last,
                b,
            )
        })
        .await
    }

    /// Retrieve a specific pool by its ID.
    async fn pool(
        &self,
        ctx: &Context<'_>,
        id: ID,
        time_frame: Option<TimeFrame>,
        #[graphql(name = "type")] ty: Option<PoolTransactionType>,
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
    ) -> Result<Option<Pool>> {
        let _ = time_frame; // `Pool.charts` takes its own.
        let now = now_secs();
        let a = TxArgs {
            ty,
            first,
            after,
            last,
            before,
        };
        run(ctx, false, move |v, b| {
            ds::pool(&v.txn, &id.0, now, a, b).map(Some)
        })
        .await
    }

    async fn pool_transactions(
        &self,
        ctx: &Context<'_>,
        pair_id: String,
        #[graphql(name = "type")] ty: Option<PoolTransactionType>,
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
    ) -> Result<Option<PoolTransactionsConnection>> {
        // Stock's resolver defaults `first` to 10.
        let first = if first.is_none() && last.is_none() {
            Some(10)
        } else {
            first
        };
        let a = TxArgs {
            ty,
            first,
            after,
            last,
            before,
        };
        run(ctx, false, move |v, b| {
            ds::pool_transactions(v, &pair_id, &a, b).map(Some)
        })
        .await
    }

    /// Get user's liquidity positions.
    async fn liquidity_positions(
        &self,
        ctx: &Context<'_>,
        wallet_address: String,
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
        #[graphql(default_with = "value_usd_desc()")] order_by: Option<LiquidityPositionOrderBy>,
    ) -> Result<LiquidityPositionsConnection> {
        let now = now_secs();
        let order = order_by.unwrap_or(LiquidityPositionOrderBy::ValueUsdDesc);
        run(ctx, true, move |v, b| {
            ds::liquidity_positions(
                &v.txn,
                now,
                &wallet_address,
                order,
                after.as_deref(),
                before.as_deref(),
                first,
                last,
                b,
            )
        })
        .await
    }

    /// Get DEX metrics including TVL, volume, and pool count.
    async fn dex_metrics(
        &self,
        ctx: &Context<'_>,
        start_date: Option<DateTime>,
        end_date: Option<DateTime>,
        protocol_address: Option<String>,
    ) -> Result<DexMetrics> {
        let secs = |d: Option<DateTime>| d.map(|d| (d.0.max(0) / 1_000_000) as u64);
        let (now, proto) = (now_secs(), protocol(protocol_address));
        let (s, e) = (secs(start_date), secs(end_date));
        run(ctx, true, move |v, b| {
            ds::dex_metrics(&v.txn, now, s, e, &proto, b)
        })
        .await
    }

    /// Get price for a specific token.
    async fn token_price(
        &self,
        ctx: &Context<'_>,
        token_address: String,
        protocol_address: Option<String>,
    ) -> Result<Option<TokenPrice>> {
        let (now, proto) = (now_secs(), protocol(protocol_address));
        run(ctx, true, move |v, b| {
            ds::token_price(&v.txn, now, &token_address, &proto, b)
        })
        .await
    }

    /// Get prices for all tokens in a protocol.
    async fn token_prices(
        &self,
        ctx: &Context<'_>,
        protocol_address: Option<String>,
    ) -> Result<Vec<TokenPrice>> {
        let (now, proto) = (now_secs(), protocol(protocol_address));
        run(ctx, true, move |v, b| {
            ds::token_prices(&v.txn, now, &proto, b)
        })
        .await
    }
}

/// The served `Query` root: Kadena's fields (`schema::Query`) and the DEX fields.
#[derive(async_graphql::MergedObject)]
#[graphql(name = "Query")]
pub struct QueryRoot(pub crate::schema::Query, pub DexQuery);
