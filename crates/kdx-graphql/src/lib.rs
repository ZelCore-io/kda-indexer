//! kdx-graphql: the GraphQL adapter (spec §5.3). It serves Kadena's `@kadena/graph` schema
//! (vendored unchanged in `schema/kadena-graph.graphql`, BSD-3-Clause, see
//! THIRD_PARTY_NOTICES) over the read layer of `kdx-api`, on its own listener behind the same
//! guard middleware as compat.
//!
//! - [`build_schema`] builds the executable schema; [`router`] serves it at `/graphql`.
//! - Every resource limit (depth, complexity, batch, sizes, deadline) is in [`limits`] and
//!   [`http`]; a refusal answers a GraphQL error with `extensions.code = "LIMIT"`.
//! - Resolvers never hold a `View` across an `.await` (GQ-5): each store access is one
//!   `ApiState::read`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_graphql::{Context, EmptyMutation, Schema};
use kdx_api::ApiState;

pub mod connection;
pub mod error;
pub mod feed;
pub mod flags;
pub mod http;
pub mod ids;
pub mod limits;
pub mod loaders;
pub mod miss;
pub mod order;
pub mod pact;
pub mod scalars;
pub mod schema;
pub mod stock_limits;
pub mod stock_schema;
#[cfg(test)]
mod testutil;
pub mod txlist;
pub mod types;
pub mod ws;

pub use feed::BlockFeed;
pub use http::router;

/// The executable schema.
pub type KdxSchema =
    Schema<types::dex::QueryRoot, EmptyMutation, types::subscription::Subscription>;

/// `[graphql]` settings (plan GQ-9; the config file maps onto this).
#[derive(Debug, Clone)]
pub struct GraphqlSettings {
    /// Page size when a connection gets neither `first` nor `last`.
    pub default_page: usize,
    /// Page size of the legacy cost model ([`limits::analyze`]) only. Served pages, a
    /// subscription's `quantity` and NFT holdings are bounded by [`MAX_PAGE_ROWS`] (FIX-3
    /// part D).
    pub max_page: usize,
    /// Depth of the read-budget model ([`limits::analyze`]); not enforced: stock's depth
    /// limit is ([`stock_limits`]).
    pub max_depth: usize,
    /// Read budget of the cost model ([`limits::budget`]); over it a request is logged, not
    /// refused: stock's complexity limit refuses ([`stock_limits`]).
    pub max_complexity: u64,
    pub introspection: bool,
    /// Depth cap of `__schema`/`__type` subtrees (exempt from `max_depth`).
    pub introspection_depth: usize,
    /// Operations in one batched request.
    pub max_batch: usize,
    pub max_query_bytes: usize,
    pub max_variables_bytes: usize,
    /// HTTP request body cap (POST).
    pub max_body_bytes: usize,
    pub ws_subs_per_conn: usize,
    /// Subscriptions per client (an IPv4 address or an IPv6 /64, as the rate limiter keys
    /// it), over all its connections (M-2).
    pub ws_subs_per_ip: usize,
    /// Subscriptions per IPv6 /48 (M-2).
    pub ws_subs_per_48: usize,
    pub ws_subs_max: usize,
    pub ws_conns_per_ip: usize,
    /// Connections per IPv6 /48 (M-2).
    pub ws_conns_per_48: usize,
    /// Connections per process (M-2: sized apart from `ws_subs_max`).
    pub ws_conns_max: usize,
    pub ws_init: Duration,
    pub ws_idle: Duration,
    /// How often the server pings a websocket peer (audit F-25), [`ws::WS_PING`].
    pub ws_ping: Duration,
    /// A websocket peer that sends no frame (not even a pong) this long is closed (audit
    /// F-25), [`ws::WS_SILENCE`].
    pub ws_silence: Duration,
    pub ws_tx_wait: Duration,
    pub feed_poll: Duration,
    /// Byte budget of the decoded blocks the feed's broadcast ring holds (M-3).
    pub feed_ring_bytes: usize,
    /// `networkInfo.networkId`.
    pub network_id: String,
    /// `networkInfo.networkHost` (never the node URL).
    pub network_host: String,
    /// Automatic persisted queries (not built; the config refuses `true`).
    pub apq: bool,
    /// Keys a `last`/`before` page of a source with no oldest-first walk may collect between
    /// its cursor and the newest end ([`txlist::BACK_KEYS`]; tests lower it).
    pub back_keys: usize,
    /// Transfer keys `balance(...)` reads per (account, module) to decide which zero rows a
    /// TRANSFER touched ([`types::account::BALANCE_TOUCH_KEYS`]; tests lower it).
    pub balance_touch_keys: usize,
    /// `pactQuery` / `gasLimitEstimate` limits (B1).
    pub pact: pact::PactSettings,
}

/// Hard safety cap of the rows of one list page (`first`/`last`), of a subscription's
/// `quantity` and of an account's NFT holdings (FIX-3 part D, S3). It applies after stock's
/// complexity gate: a page above it is refused (`LIMIT`, `extensions.limit = "page"`), where
/// stock answers it slowly or times out; holdings are cut at it.
pub const MAX_PAGE_ROWS: usize = 1000;

/// Most NFT holdings of one account served (FIX-3 part E, S1). Above it the first
/// `MAX_NFT_HOLDINGS` are served with a partial-data `LIMIT` error
/// (`extensions.limit = "nft_holdings"`), never cut silently.
pub const MAX_NFT_HOLDINGS: usize = 10_000;

/// Request body cap (plan GQ-9: body ≤ 128 KB).
pub const MAX_BODY_BYTES: usize = 128 * 1024;

impl Default for GraphqlSettings {
    fn default() -> Self {
        GraphqlSettings {
            default_page: 20,
            max_page: 100,
            max_depth: 10,
            max_complexity: 2000,
            introspection: true,
            introspection_depth: 14,
            max_batch: 10,
            max_query_bytes: 32 * 1024,
            max_variables_bytes: 64 * 1024,
            max_body_bytes: MAX_BODY_BYTES,
            ws_subs_per_conn: 20,
            ws_subs_per_ip: 40,
            ws_subs_per_48: 120,
            ws_subs_max: 2000,
            ws_conns_per_ip: 10,
            ws_conns_per_48: 30,
            ws_conns_max: 1000,
            ws_init: Duration::from_secs(10),
            ws_idle: Duration::from_secs(600),
            ws_ping: ws::WS_PING,
            ws_silence: ws::WS_SILENCE,
            ws_tx_wait: Duration::from_secs(3600),
            feed_poll: Duration::from_millis(1000),
            feed_ring_bytes: crate::feed::RING_BYTES,
            network_id: "mainnet01".into(),
            network_host: String::new(),
            apq: false,
            back_keys: txlist::BACK_KEYS,
            balance_touch_keys: types::account::BALANCE_TOUCH_KEYS,
            pact: pact::PactSettings::default(),
        }
    }
}

/// Schema-wide data every resolver can reach (`gql(ctx)`).
pub struct Gql {
    state: Option<Arc<ApiState>>,
    pub settings: GraphqlSettings,
    pub feed: Option<BlockFeed>,
    pub ws: ws::WsState,
    /// The P6 search context (code index and gate) `transactionsByPactCode` runs on; `None`
    /// answers that field `NOT_SUPPORTED`.
    pub search: Option<Arc<kdx_api::search::SearchCtx>>,
    /// The node `/local` passthrough; `None` answers `pactQuery` / `gasLimitEstimate`
    /// `NOT_SUPPORTED`.
    pub pact: Option<Arc<pact::PactPass>>,
    /// The node read on a missed account row (gap Q); `None` answers from the store only.
    pub miss: Option<Arc<miss::MissReader>>,
}

impl Gql {
    /// The read layer. A detached schema (`print-schema`, `check-ops`) has none: the first
    /// resolver that needs the store fails, which is what `check-ops` relies on.
    pub fn state(&self) -> async_graphql::Result<&Arc<ApiState>> {
        self.state
            .as_ref()
            .ok_or_else(|| error::err(error::Code::Internal, "no store attached"))
    }
}

/// The schema data of a resolver's context.
pub fn gql<'a>(ctx: &Context<'a>) -> &'a Gql {
    ctx.data_unchecked::<Gql>()
}

/// Per-request data: the deadline every batch of one HTTP request shares.
#[derive(Debug, Clone, Copy)]
pub struct ReqCtx {
    pub deadline: Instant,
}

/// An HTTP request's shared deadline (B1 fix round 1, I1): the outer timers of the guard
/// and the handler follow it; a Pact field that runs extends it ([`crate::pact`]).
#[derive(Debug, Clone)]
pub struct RequestRun(pub kdx_api::RunDeadline);

/// The client address of an HTTP request or a websocket connection (`trusted_proxies`
/// applied). Per-client gates (the search gate, S-5) refuse an operation without one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientIp(pub std::net::IpAddr);

/// The client address of the operation being resolved, when known.
pub fn client_ip(ctx: &Context<'_>) -> Option<std::net::IpAddr> {
    ctx.data_opt::<ClientIp>().map(|c| c.0)
}

/// The request deadline (a request without one, such as a subscription event, gets the
/// default 5 s from now).
pub fn deadline(ctx: &Context<'_>) -> Instant {
    ctx.data_opt::<ReqCtx>()
        .map(|r| r.deadline)
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(5))
}

/// The schema over a store.
pub fn build_schema(
    state: Arc<ApiState>,
    settings: GraphqlSettings,
    feed: Option<BlockFeed>,
) -> KdxSchema {
    build(Some(state), settings, feed, None, None, None, false)
}

/// The schema over a store, with the P6 search context for `transactionsByPactCode`.
pub fn build_schema_with_search(
    state: Arc<ApiState>,
    settings: GraphqlSettings,
    feed: Option<BlockFeed>,
    search: Arc<kdx_api::search::SearchCtx>,
) -> KdxSchema {
    build(Some(state), settings, feed, Some(search), None, None, false)
}

/// The schema with no store (`graphql print-schema`).
/// [`build_schema_with_search`] plus the node `/local` passthrough (B1); `store` may be
/// `None` (tests).
pub fn build_schema_full(
    state: Option<Arc<ApiState>>,
    settings: GraphqlSettings,
    feed: Option<BlockFeed>,
    search: Option<Arc<kdx_api::search::SearchCtx>>,
    pact: Option<Arc<pact::PactPass>>,
) -> KdxSchema {
    build(state, settings, feed, search, pact, None, false)
}

/// [`build_schema_full`] plus the node read on a missed account row (gap Q).
pub fn build_schema_with_miss(
    state: Option<Arc<ApiState>>,
    settings: GraphqlSettings,
    feed: Option<BlockFeed>,
    search: Option<Arc<kdx_api::search::SearchCtx>>,
    pact: Option<Arc<pact::PactPass>>,
    miss: Option<Arc<miss::MissReader>>,
) -> KdxSchema {
    build(state, settings, feed, search, pact, miss, false)
}

pub fn build_schema_detached(settings: GraphqlSettings) -> KdxSchema {
    build(None, settings, None, None, None, None, false)
}

/// The schema of `graphql check-ops`: validation and limits run, execution never does.
pub fn build_schema_check(settings: GraphqlSettings) -> KdxSchema {
    build(None, settings, None, None, None, None, true)
}

fn build(
    state: Option<Arc<ApiState>>,
    settings: GraphqlSettings,
    feed: Option<BlockFeed>,
    search: Option<Arc<kdx_api::search::SearchCtx>>,
    pact: Option<Arc<pact::PactPass>>,
    miss: Option<Arc<miss::MissReader>>,
    check_only: bool,
) -> KdxSchema {
    let introspection = settings.introspection;
    let mut b = Schema::build(
        types::dex::QueryRoot(schema::Query, types::dex::DexQuery),
        EmptyMutation,
        types::subscription::Subscription,
    )
    // Stock declares `LiquidityBalance` though no field returns it (DEX extension).
    .register_output_type::<types::dex::LiquidityBalance>()
    .data(Gql {
        state,
        ws: ws::WsState::new(&settings),
        settings,
        feed,
        search,
        pact,
        miss,
    })
    // Outermost: puts the final response in selection-set order (async-graphql answers
    // in completion order), and only keeps a document the limits accepted.
    .extension(order::FieldOrder)
    .extension(limits::Limits)
    .extension(flags::Flags)
    .extension(ws::WsOps)
    // Parser recursion cap. No `limit_directives` (C-1): its walk expands fragments with no
    // visit cap; `limits` counts directives inside its own bounded walk, which runs first.
    .limit_recursive_depth(limits::PARSER_RECURSION);
    if !introspection {
        b = b.disable_introspection();
    }
    if check_only {
        b = b.extension(limits::CheckOnly);
    }
    b.finish()
}

/// The settings a schema was built with.
pub fn gql_settings(schema: &KdxSchema) -> GraphqlSettings {
    schema
        .data::<Gql>()
        .map(|g| g.settings.clone())
        .unwrap_or_default()
}
