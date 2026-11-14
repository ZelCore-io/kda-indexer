//! kdx-compare: diffs a chainweb-data compatible API (target A) against another target or
//! the chainweb-node oracle (spec section 7).

pub mod allowlist;
pub mod config;
pub mod diff;
pub mod gen;
pub mod graphql;
pub mod graphql_norm;
pub mod graphql_scope;
pub mod graphql_window;
pub mod graphql_ws;
pub mod metrics;
pub mod normalize;
pub mod oracle;
pub mod referee;
pub mod report;
pub mod rewards;
pub mod run;
pub mod summary;
pub mod target;
pub mod via_cwd;
