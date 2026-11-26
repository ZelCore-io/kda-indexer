//! `NetworkInfo` and `GraphConfiguration` (Task 3).

use async_graphql::{ComplexObject, Context, Object, Result, SimpleObject};
use kdx_decode::header;

use crate::error::from_api;
use crate::gql;

use crate::scalars::{BigInt, DateTime};
use kdx_api::nodeinfo::NodeFacts;
use std::sync::Arc;

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
    /// The node's last good `/info` facts (`kdx_api::nodeinfo`), read once per request.
    #[graphql(skip)]
    pub node: Option<Arc<NodeFacts>>,
}

/// Stock's `GenesisHeight`: a chain's first height, from the node's `/info`.
#[derive(Debug, Clone, SimpleObject)]
pub struct GenesisHeight {
    pub chain_id: String,
    pub height: i32,
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

/// The node fields before the first good `/info` answer (stock answers them from its memory
/// cache, which is empty until its first good read).
fn no_node_info(field: &str) -> async_graphql::Error {
    crate::error::err(
        crate::error::Code::NotAvailable,
        format!("{field}: the node's /info has not been read yet"),
    )
}

/// A node number as GraphQL `Int` (graphql-js refuses one past 32 bits, as this does).
fn node_int(field: &str, n: i64) -> Result<Option<i32>> {
    i32::try_from(n).map(Some).map_err(|_| {
        crate::error::err(
            crate::error::Code::Internal,
            format!("{field}: the node's value {n} is not a 32-bit Int"),
        )
    })
}

impl NetworkInfo {
    fn facts(&self, field: &str) -> Result<&NodeFacts> {
        self.node.as_deref().ok_or_else(|| no_node_info(field))
    }
}

#[ComplexObject]
impl NetworkInfo {
    /// `nodeBlockDelay` of the node's `/info` (microseconds).
    async fn node_block_delay(&self) -> Result<Option<i32>> {
        node_int("nodeBlockDelay", self.facts("nodeBlockDelay")?.block_delay)
    }
    /// `nodeChains` of the node's `/info`, in its order.
    async fn node_chains(&self) -> Result<Option<Vec<String>>> {
        Ok(Some(self.facts("nodeChains")?.chains.clone()))
    }
    /// `nodeNumberOfChains` of the node's `/info`.
    async fn number_of_chains(&self) -> Result<Option<i32>> {
        node_int(
            "numberOfChains",
            self.facts("numberOfChains")?.number_of_chains,
        )
    }
    /// `nodeGenesisHeights` of the node's `/info`, in its order.
    async fn genesis_heights(&self) -> Result<Option<Vec<GenesisHeight>>> {
        let f = self.facts("genesisHeights")?;
        let mut out = Vec::with_capacity(f.genesis_heights.len());
        for (c, h) in &f.genesis_heights {
            out.push(GenesisHeight {
                chain_id: c.clone(),
                height: node_int("genesisHeights", *h)?.unwrap_or_default(),
            });
        }
        Ok(Some(out))
    }
    /// `nodePackageVersion` of the node's `/info`.
    async fn node_package_version(&self) -> Result<Option<String>> {
        Ok(Some(
            self.facts("nodePackageVersion")?.package_version.clone(),
        ))
    }
    /// `nodeServiceDate` of the node's `/info` as `toISOString` text; `null` when the node
    /// sends `null` (or a date that does not parse).
    async fn node_service_date(&self) -> Result<Option<DateTime>> {
        let f = self.facts("nodeServiceDate")?;
        Ok(f.service_date
            .as_deref()
            .and_then(crate::scalars::parse_rfc3339))
    }
    /// `nodeLatestBehaviorHeight` of the node's `/info`.
    async fn node_latest_behavior_height(&self) -> Result<Option<i32>> {
        node_int(
            "nodeLatestBehaviorHeight",
            self.facts("nodeLatestBehaviorHeight")?
                .latest_behavior_height,
        )
    }
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
    let node = g.state()?.tips.info();
    Ok(Some(NetworkInfo {
        api_version: "0.0".into(),
        coins_in_circulation: coins.unwrap_or(0.0),
        network_hash_rate: hash_rate,
        network_host: g.settings.network_host.clone(),
        network_id: g.settings.network_id.clone(),
        total_difficulty,
        transaction_count: txs.min(i32::MAX as u64) as i32,
        node,
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

    struct N(NetworkInfo);

    #[Object]
    impl N {
        async fn network_info(&self) -> NetworkInfo {
            self.0.clone()
        }
    }

    const NODE_SEL: &str = "{ networkInfo { networkId nodeBlockDelay nodeChains numberOfChains \
        genesisHeights { chainId height } nodePackageVersion nodeServiceDate \
        nodeLatestBehaviorHeight } }";

    fn info(node: Option<serde_json::Value>) -> NetworkInfo {
        NetworkInfo {
            api_version: "0.0".into(),
            coins_in_circulation: 0.0,
            network_hash_rate: 0.0,
            network_host: "h".into(),
            network_id: "mainnet01".into(),
            total_difficulty: 0.0,
            transaction_count: 0,
            node: node
                .map(|v| std::sync::Arc::new(kdx_api::nodeinfo::NodeFacts::from_info(&v).unwrap())),
        }
    }

    fn mainnet_info() -> serde_json::Value {
        serde_json::json!({
            "nodeApiVersion": "0.0", "nodeBlockDelay": 30000000,
            "nodeChains": ["0", "1", "10", "19"],
            "nodeGenesisHeights": [["0", 0], ["10", 852054], ["19", 852054]],
            "nodeGraphHistory": [], "nodeHistoricalChains": [],
            "nodeLatestBehaviorHeight": 5785924, "nodeNumberOfChains": 20,
            "nodePackageVersion": "2.29", "nodeServiceDate": "2025-07-23T00:00:00Z",
            "nodeVersion": "mainnet01"
        })
    }

    async fn run_node(node: Option<serde_json::Value>) -> async_graphql::Response {
        Schema::new(N(info(node)), EmptyMutation, EmptySubscription)
            .execute(NODE_SEL)
            .await
    }

    /// The node fields answer stock's values and types from the cached `/info` (stock's
    /// network-info fixture: `nodeServiceDate` as JavaScript's `toISOString`).
    #[tokio::test]
    async fn node_fields_answer_as_stock() {
        let r = run_node(Some(mainnet_info())).await;
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        let v = serde_json::to_value(&r.data).unwrap();
        assert_eq!(
            v["networkInfo"],
            serde_json::json!({
                "networkId": "mainnet01",
                "nodeBlockDelay": 30000000,
                "nodeChains": ["0", "1", "10", "19"],
                "numberOfChains": 20,
                "genesisHeights": [
                    {"chainId": "0", "height": 0},
                    {"chainId": "10", "height": 852054},
                    {"chainId": "19", "height": 852054}
                ],
                "nodePackageVersion": "2.29",
                "nodeServiceDate": "2025-07-23T00:00:00.000Z",
                "nodeLatestBehaviorHeight": 5785924
            })
        );
        // A `null` service date is `null` with no error (stock's field is nullable).
        let mut n = mainnet_info();
        n["nodeServiceDate"] = serde_json::Value::Null;
        let r = run_node(Some(n)).await;
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        let v = serde_json::to_value(&r.data).unwrap();
        assert!(v["networkInfo"]["nodeServiceDate"].is_null());
    }

    /// Before the first good `/info` answer each node field is `null` with `NOT_AVAILABLE`
    /// beside it (`nodeServiceDate` included); the other fields still answer.
    #[tokio::test]
    async fn node_fields_without_an_answer_are_not_available() {
        let r = run_node(None).await;
        assert_eq!(r.errors.len(), 7, "{:?}", r.errors);
        for e in &r.errors {
            assert!(
                format!("{:?}", e.extensions).contains("NOT_AVAILABLE"),
                "{e:?}"
            );
        }
        let v = serde_json::to_value(&r.data).unwrap();
        assert_eq!(v["networkInfo"]["networkId"], "mainnet01");
        for f in [
            "nodeBlockDelay",
            "nodeChains",
            "numberOfChains",
            "genesisHeights",
            "nodePackageVersion",
            "nodeServiceDate",
            "nodeLatestBehaviorHeight",
        ] {
            assert!(v["networkInfo"][f].is_null(), "{f}");
        }
    }
}
