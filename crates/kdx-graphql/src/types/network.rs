//! `NetworkInfo` and `GraphConfiguration` (Task 3).

use async_graphql::{ComplexObject, Context, Object, Result, SimpleObject};
use kdx_decode::header;

use crate::error::from_api;
use crate::gql;

use crate::scalars::BigInt;

#[derive(Debug, Clone, SimpleObject)]
#[graphql(complex)]
pub struct NetworkInfo {
    pub api_version: String,
    pub coins_in_circulation: f64,
    pub network_hash_rate: f64,
    pub network_host: String,
    pub network_id: String,
    pub total_difficulty: f64,
    pub transaction_count: i32,
}

/// ZelCore extension `NetworkInfo.countersOfEachChain`. `blocksCount` is the chain's block
/// count from its genesis height to the stored tip (blocks are dense). `transactionCount`
/// and `totalGasUsedInKda` come from the stored per-chain counters (`kdx_hot::chaincount`),
/// read in the same snapshot as the tips: the chain's user txs (coinbase excluded, as
/// stock) and Σ gas × gasPrice, exact, printed as stock prints its `DECIMAL(20,10)`
/// column (half-up, exactly 10 decimals, a string). While the counters are not ready
/// (`reconcile-counts` not run on an older store, or a guard cleared them) both answer
/// `null` with `NOT_AVAILABLE`.
#[derive(Debug, Clone)]
pub struct CountersOfEachChain {
    pub chain: u8,
    pub blocks: u64,
    /// `None` while the stored counters are not ready.
    pub counts: Option<kdx_hot::chaincount::Counts>,
}

#[Object]
impl CountersOfEachChain {
    async fn chain_id(&self) -> String {
        self.chain.to_string()
    }
    async fn blocks_count(&self) -> i32 {
        self.blocks.min(i32::MAX as u64) as i32
    }
    async fn transaction_count(&self) -> Result<Option<i32>> {
        let c = self.counts.ok_or_else(|| not_ready("transactionCount"))?;
        Ok(Some(c.user_txs.min(i32::MAX as u64) as i32))
    }
    async fn total_gas_used_in_kda(&self) -> Result<Option<String>> {
        let c = self.counts.ok_or_else(|| not_ready("totalGasUsedInKda"))?;
        Ok(Some(c.gas.to_stock_text()))
    }
}

/// One chain's stored counters. A chain with no row (one the chain graph added after the
/// counters were built) or a damaged row is not ready, never a `0` served as exact (secpass
/// B M-15).
fn chain_counts(
    rows: &std::collections::BTreeMap<u8, Option<kdx_hot::chaincount::Counts>>,
    chain: u8,
) -> Option<kdx_hot::chaincount::Counts> {
    rows.get(&chain).copied().flatten()
}

fn not_ready(field: &str) -> async_graphql::Error {
    crate::error::err(
        crate::error::Code::NotAvailable,
        format!(
            "{field}: the per-chain counters are not built yet (`kda-indexer reconcile-counts`)"
        ),
    )
}

#[ComplexObject]
impl NetworkInfo {
    /// ZelCore extension: per-chain counters, ascending by chain id.
    async fn counters_of_each_chain(&self, ctx: &Context<'_>) -> Result<Vec<CountersOfEachChain>> {
        let (tips, counts) = gql(ctx)
            .state()?
            .read_status(|view| {
                let tips = super::block::tips_and_genesis(view)?;
                // O(20) reads in the tips' snapshot: never ahead of or behind them.
                let counts = if kdx_hot::chaincount::ready(&view.txn)? {
                    Some(kdx_hot::chaincount::read_all(&view.txn)?)
                } else {
                    None
                };
                Ok((tips, counts))
            })
            .await
            .map_err(from_api)?;
        let mut out: Vec<CountersOfEachChain> = tips
            .into_iter()
            .map(|(c, g, t)| CountersOfEachChain {
                chain: c,
                blocks: (t + 1).saturating_sub(g),
                counts: counts.as_ref().and_then(|m| chain_counts(m, c)),
            })
            .collect();
        out.sort_by_key(|x| x.chain);
        Ok(out)
    }
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
            minimum_block_height: Some(BigInt::new(0)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_graphql::{EmptyMutation, EmptySubscription, Schema};
    use kdx_hot::chaincount::{parse_price, Counts};

    struct Q(Vec<CountersOfEachChain>);

    #[Object]
    impl Q {
        async fn counters(&self) -> Vec<CountersOfEachChain> {
            self.0.clone()
        }
    }

    async fn run(rows: Vec<CountersOfEachChain>) -> async_graphql::Response {
        Schema::new(Q(rows), EmptyMutation, EmptySubscription)
            .execute("{ counters { chainId blocksCount transactionCount totalGasUsedInKda } }")
            .await
    }

    /// Stock's live values (2026-11-26) come out with stock's types and text: `Int`, and a
    /// string with exactly 10 decimals, trailing zeros kept.
    #[tokio::test]
    async fn ready_counters_answer_as_stock() {
        let row = |chain, blocks, txs, gas: &str| CountersOfEachChain {
            chain,
            blocks,
            counts: Some(Counts {
                user_txs: txs,
                gas: parse_price(gas).unwrap(),
            }),
        };
        let r = run(vec![
            row(0, 7_438_327, 189_149_913, "126267.904006390149"),
            row(18, 6_586_279, 859_255, "205.878216864"),
            row(19, 1, 0, "0"),
        ])
        .await;
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        let v = serde_json::to_value(&r.data).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"counters": [
                {"chainId": "0", "blocksCount": 7438327, "transactionCount": 189149913,
                 "totalGasUsedInKda": "126267.9040063901"},
                {"chainId": "18", "blocksCount": 6586279, "transactionCount": 859255,
                 "totalGasUsedInKda": "205.8782168640"},
                {"chainId": "19", "blocksCount": 1, "transactionCount": 0,
                 "totalGasUsedInKda": "0.0000000000"},
            ]})
        );
    }

    /// Secpass B M-15: a chain with no counters row, or a damaged one, is not ready.
    #[test]
    fn a_missing_or_damaged_chain_row_is_not_ready() {
        let c = Counts {
            user_txs: 5,
            gas: parse_price("1").unwrap(),
        };
        let rows = std::collections::BTreeMap::from([(0u8, Some(c)), (1, None)]);
        assert_eq!(super::chain_counts(&rows, 0), Some(c));
        assert_eq!(super::chain_counts(&rows, 1), None, "damaged");
        assert_eq!(super::chain_counts(&rows, 2), None, "missing");
    }

    /// Not ready: both fields `null` with `NOT_AVAILABLE`; `blocksCount` still answers.
    #[tokio::test]
    async fn unready_counters_answer_not_available() {
        let r = run(vec![CountersOfEachChain {
            chain: 3,
            blocks: 9,
            counts: None,
        }])
        .await;
        assert_eq!(r.errors.len(), 2, "{:?}", r.errors);
        for e in &r.errors {
            assert!(
                format!("{:?}", e.extensions).contains("NOT_AVAILABLE"),
                "{e:?}"
            );
        }
        let v = serde_json::to_value(&r.data).unwrap();
        assert_eq!(v["counters"][0]["blocksCount"], 9);
        assert!(v["counters"][0]["transactionCount"].is_null());
        assert!(v["counters"][0]["totalGasUsedInKda"].is_null());
    }
}
