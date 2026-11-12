//! `NetworkInfo` and `GraphConfiguration` (Task 3).

use async_graphql::SimpleObject;

use crate::scalars::BigInt;

#[derive(Debug, Clone, SimpleObject)]
pub struct NetworkInfo {
    pub api_version: String,
    pub coins_in_circulation: f64,
    pub network_hash_rate: f64,
    pub network_host: String,
    pub network_id: String,
    pub total_difficulty: f64,
    pub transaction_count: i32,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct GraphConfiguration {
    pub minimum_block_height: Option<BigInt>,
    pub version: String,
}

impl GraphConfiguration {
    /// GQ-13: the package version; full history from height 0.
    pub fn current() -> GraphConfiguration {
        GraphConfiguration {
            minimum_block_height: Some(BigInt(0)),
            version: env!("CARGO_PKG_VERSION").to_owned(),
        }
    }
}
