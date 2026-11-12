//! `NetworkInfo` and `GraphConfiguration` (Task 3).

use async_graphql::{Context, Result, SimpleObject};
use kdx_decode::header;

use crate::error::from_api;
use crate::gql;

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

/// Seconds between blocks on one chain (chainweb's block delay).
pub const BLOCK_SECONDS: f64 = 30.0;

/// `networkInfo` (GQ-13): ids from config; `coinsInCirculation` and `transactionCount` from
/// the `/stats` code path; `totalDifficulty` is the sum over chains of the tip block's
/// `weight` (cumulative difficulty) and `networkHashRate` the sum over chains of the tip
/// block's difficulty divided by the 30 s block time. `None` on an empty store.
pub async fn network_info(ctx: &Context<'_>) -> Result<Option<NetworkInfo>> {
    let g = gql(ctx);
    let found = g
        .state()?
        .read(|view| {
            let mut tips = Vec::new();
            for (c, h, _) in view.chain_tips()? {
                if let Some(b) = view.block(c, h)? {
                    tips.push(b);
                }
            }
            let stats = kdx_api::compat::stats::stats(view)?;
            Ok((tips, stats))
        })
        .await
        .map_err(from_api)?;
    let (tips, Some((txs, coins))) = found else {
        return Ok(None);
    };
    let (total_difficulty, hash_rate) = cut_numbers(&tips);
    Ok(Some(NetworkInfo {
        api_version: "0.0".into(),
        coins_in_circulation: coins.unwrap_or(0.0),
        network_hash_rate: hash_rate,
        network_host: g.settings.network_host.clone(),
        network_id: g.settings.network_id.clone(),
        total_difficulty,
        transaction_count: txs.min(i32::MAX as u64) as i32,
    }))
}

/// `(totalDifficulty, networkHashRate)` of the tip headers of a cut.
pub fn cut_numbers(tips: &[kdx_types::record::BlockRecord]) -> (f64, f64) {
    let total = tips.iter().map(|b| header::le256_f64(&b.weight)).sum();
    let rate = tips
        .iter()
        .map(|b| header::difficulty(&b.target) as f64)
        .sum::<f64>()
        / BLOCK_SECONDS;
    (total, rate)
}
