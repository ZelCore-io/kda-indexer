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
pub mod http;
pub mod ids;
pub mod limits;
pub mod loaders;
pub mod scalars;
pub mod schema;
pub mod txlist;
pub mod types;
pub mod ws;

pub use feed::BlockFeed;
pub use http::router;

/// The executable schema.
pub type KdxSchema = Schema<schema::Query, EmptyMutation, types::subscription::Subscription>;

/// `[graphql]` settings (plan GQ-9; the config file maps onto this).
#[derive(Debug, Clone)]
pub struct GraphqlSettings {
    /// Page size when a connection gets neither `first` nor `last`.
    pub default_page: usize,
    /// Larger `first`/`last` values are clamped to this (not refused).
    pub max_page: usize,
    pub max_depth: usize,
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
    pub ws_subs_max: usize,
    pub ws_conns_per_ip: usize,
    pub ws_init: Duration,
    pub ws_idle: Duration,
    pub ws_tx_wait: Duration,
    pub feed_poll: Duration,
    /// `networkInfo.networkId`.
    pub network_id: String,
    /// `networkInfo.networkHost` (never the node URL).
    pub network_host: String,
    /// Automatic persisted queries (not built; the config refuses `true`).
    pub apq: bool,
}

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
            ws_subs_max: 2000,
            ws_conns_per_ip: 10,
            ws_init: Duration::from_secs(10),
            ws_idle: Duration::from_secs(600),
            ws_tx_wait: Duration::from_secs(3600),
            feed_poll: Duration::from_millis(1000),
            network_id: "mainnet01".into(),
            network_host: String::new(),
            apq: false,
        }
    }
}

/// Schema-wide data every resolver can reach (`gql(ctx)`).
pub struct Gql {
    state: Option<Arc<ApiState>>,
    pub settings: GraphqlSettings,
    pub feed: Option<BlockFeed>,
    pub ws: ws::WsState,
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
    build(Some(state), settings, feed, false)
}

/// The schema with no store (`graphql print-schema`).
pub fn build_schema_detached(settings: GraphqlSettings) -> KdxSchema {
    build(None, settings, None, false)
}

/// The schema of `graphql check-ops`: validation and limits run, execution never does.
pub fn build_schema_check(settings: GraphqlSettings) -> KdxSchema {
    build(None, settings, None, true)
}

fn build(
    state: Option<Arc<ApiState>>,
    settings: GraphqlSettings,
    feed: Option<BlockFeed>,
    check_only: bool,
) -> KdxSchema {
    let introspection = settings.introspection;
    let mut b = Schema::build(
        schema::Query,
        EmptyMutation,
        types::subscription::Subscription,
    )
    .data(Gql {
        state,
        ws: ws::WsState::new(&settings),
        settings,
        feed,
    })
    .extension(limits::Limits)
    // Parser recursion and directive caps (the AST walk in `limits` is bounded separately).
    .limit_recursive_depth(limits::PARSER_RECURSION)
    .limit_directives(limits::MAX_DIRECTIVES);
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
