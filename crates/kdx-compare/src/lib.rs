//! kdx-compare: diffs a chainweb-data compatible API (target A) against another target or
//! the chainweb-node oracle (spec section 7).

pub mod allowlist;
pub mod config;
pub mod diff;
pub mod metrics;
pub mod normalize;
pub mod report;
pub mod target;
