//! Harness configuration (spec section 7.4). TOML, unknown keys rejected.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use url::Url;

pub use kdx_node::NodeConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompareConfig {
    pub network: String,
    /// Requests per second across all targets (spec section 7.4).
    #[serde(default = "d_rate_rps")]
    pub rate_rps: f64,
    #[serde(default = "d_timeout_ms")]
    pub timeout_ms: u64,
    /// Pages followed via `next` on paginated routes.
    #[serde(default = "d_follow_depth")]
    pub follow_depth: u32,
    /// Sample heights at `min(tip) - min_depth`.
    #[serde(default = "d_min_depth")]
    pub min_depth: u64,
    /// Separate limit for node (oracle and generator) requests; 0 = unlimited. The node can
    /// take more than chainweb-data, so it does not share the targets' bucket.
    #[serde(default = "d_node_max_rps")]
    pub node_max_rps: u32,
    pub node: NodeConfig,
    #[serde(default)]
    pub targets: BTreeMap<String, TargetConfig>,
    /// `(a, b)`: target names, or `"node"` for the node oracle as B.
    #[serde(default)]
    pub pairs: Vec<(String, String)>,
    #[serde(default = "d_allowlist")]
    pub allowlist: PathBuf,
    #[serde(default)]
    pub seeds: Option<PathBuf>,
    #[serde(default)]
    pub replay: Option<PathBuf>,
    #[serde(default = "d_out_dir")]
    pub out_dir: PathBuf,
    #[serde(default)]
    pub metrics_bind: Option<String>,
    #[serde(default)]
    pub mix: Mix,
    /// Largest accepted (decompressed) target response body; a bigger one fails that request.
    #[serde(default = "d_max_body_bytes")]
    pub max_body_bytes: usize,
    /// GraphQL harness settings (spec section 7, plan Task 7).
    #[serde(default)]
    pub graphql: GraphqlConfig,
}

/// `[graphql]`: where the operation documents come from.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct GraphqlConfig {
    /// A directory of captured client operation documents (`*.graphql`, with an optional
    /// `*.txt` next to each whose `## example variables` block seeds the variables). Loaded at
    /// start and never committed: unset by default.
    pub ops_dir: Option<PathBuf>,
    /// Our own operations, written from the vendored schema.
    pub own_ops_dir: PathBuf,
    /// Operation names (file stems before the `-<sha8>` suffix) to leave out.
    pub skip_ops: Vec<String>,
    /// Heights per sampled block window and attempts per sample (generator bounds).
    pub window: u64,
    /// `totalCount` values above this compare as this (GQ-10).
    pub total_count_cap: u64,
}

impl Default for GraphqlConfig {
    fn default() -> Self {
        Self {
            ops_dir: None,
            own_ops_dir: PathBuf::from("compare/graphql"),
            skip_ops: Vec::new(),
            window: 50,
            total_count_cap: 10_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub url: Url,
    pub kind: TargetKind,
    /// Accept a self-signed TLS certificate (only for an internal `graphql_oracle`).
    #[serde(default)]
    pub insecure_tls: bool,
    /// Own request rate of this target in requests per second (`graphql_oracle`: capped at
    /// 1.0 whatever is set); unset = the shared `rate_rps` bucket.
    #[serde(default)]
    pub rate_rps: Option<f64>,
    /// `graphql_oracle`: compare only heights up to this (the oracle's stall height minus a
    /// margin). Required for that kind.
    #[serde(default)]
    pub max_height: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    ChainwebData,
    KdaIndexerCompat,
    KdaTxs,
    /// Answers `/v1/txs` by composing chainweb-data calls (the `url` is a chainweb-data
    /// instance); used when no live kda-txs is reachable.
    KdaTxsViaCwd,
    /// kda-indexer native API: answers `/api/v1/accounts/{a}/balances` (the balances pair, A).
    KdaIndexerNative,
    /// The node's `/local` (H-1: `details` reads only): the balances pair's B. The `url` is
    /// ignored; the harness uses `[node]`.
    NodeLocal,
    /// kda-indexer GraphQL adapter (A of the GraphQL pairs): the `url` is the full endpoint.
    KdaIndexerGraphql,
    /// The GraphQL reference server (B of the GraphQL pairs): HTTP POST only, its own
    /// limiter (at most 1 r/s, one in flight), heights capped at `max_height`.
    GraphqlOracle,
}

impl TargetKind {
    /// Whether a target of this kind answers `route`: chainweb-data every compat route but
    /// `/v1/txs`, the kda-txs kinds only `/v1/txs`, kda-indexer compat all of them.
    pub fn is_graphql(self) -> bool {
        matches!(
            self,
            TargetKind::KdaIndexerGraphql | TargetKind::GraphqlOracle
        )
    }

    pub fn serves(self, route: crate::normalize::RouteKind) -> bool {
        use crate::normalize::RouteKind;
        match self {
            // GraphQL targets answer no REST route; their pairs run in `graphql.rs`.
            TargetKind::KdaIndexerGraphql | TargetKind::GraphqlOracle => false,
            TargetKind::ChainwebData => !matches!(route, RouteKind::V1Txs | RouteKind::Balances),
            TargetKind::KdaTxs | TargetKind::KdaTxsViaCwd => route == RouteKind::V1Txs,
            TargetKind::KdaIndexerCompat => route != RouteKind::Balances,
            TargetKind::KdaIndexerNative | TargetKind::NodeLocal => route == RouteKind::Balances,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Mix {
    pub replay: f64,
    pub random: f64,
    pub seeds: f64,
}

impl Default for Mix {
    fn default() -> Self {
        Self {
            replay: 0.5,
            random: 0.4,
            seeds: 0.1,
        }
    }
}

fn d_rate_rps() -> f64 {
    5.0
}
fn d_timeout_ms() -> u64 {
    30_000
}
fn d_follow_depth() -> u32 {
    3
}
fn d_min_depth() -> u64 {
    10
}
fn d_node_max_rps() -> u32 {
    50
}
fn d_allowlist() -> PathBuf {
    PathBuf::from("compare/allowlist.toml")
}
fn d_max_body_bytes() -> usize {
    8 << 20
}

fn d_out_dir() -> PathBuf {
    PathBuf::from("test-runs/compare")
}

impl CompareConfig {
    pub fn load(path: &Path) -> anyhow::Result<CompareConfig> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: CompareConfig =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.rate_rps.is_finite() || self.rate_rps <= 0.0 {
            bail!("rate_rps must be > 0");
        }
        for (a, b) in &self.pairs {
            if !self.targets.contains_key(a) {
                bail!("pair side A {a:?} is not a configured target");
            }
            if b != "node" && !self.targets.contains_key(b) {
                bail!("pair side B {b:?} is neither \"node\" nor a configured target");
            }
        }
        for (n, t) in &self.targets {
            if t.kind == TargetKind::GraphqlOracle && t.max_height.is_none() {
                bail!("target {n}: graphql_oracle needs max_height");
            }
            if t.rate_rps.is_some_and(|r| !r.is_finite() || r <= 0.0) {
                bail!("target {n}: rate_rps must be > 0");
            }
            if t.insecure_tls && t.kind != TargetKind::GraphqlOracle {
                bail!("target {n}: insecure_tls is only for graphql_oracle");
            }
        }
        for (w, n) in [
            (self.mix.replay, "replay"),
            (self.mix.random, "random"),
            (self.mix.seeds, "seeds"),
        ] {
            if w < 0.0 {
                bail!("mix.{n} must be >= 0");
            }
        }
        Ok(())
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// The first configured target that is side A of some pair (or the first target).
    pub fn primary_target(&self) -> Option<&str> {
        let rest = |n: &&str| self.targets.get(*n).is_some_and(|t| !t.kind.is_graphql());
        self.pairs
            .iter()
            .map(|(a, _)| a.as_str())
            .find(rest)
            .or_else(|| self.targets.keys().map(String::as_str).find(rest))
    }

    /// Whether any pair has a GraphQL target as side A.
    pub fn has_graphql_pairs(&self) -> bool {
        self.pairs.iter().any(|(a, _)| {
            self.targets
                .get(a)
                .is_some_and(|t| t.kind == TargetKind::KdaIndexerGraphql)
        })
    }

    pub fn node_client(&self) -> anyhow::Result<kdx_node::NodeClient> {
        let p2p_cert_sha256 = self
            .node
            .p2p_cert_sha256
            .as_deref()
            .map(kdx_node::parse_sha256_hex)
            .transpose()
            .context("node.p2p_cert_sha256")?;
        let ncfg = kdx_node::NodeClientConfig {
            base: self.node.url.clone(),
            network: self.network.clone(),
            timeout: Duration::from_millis(self.node.timeout_ms),
            max_rps: self.node_max_rps,
            concurrency: 8,
            p2p_url: self.node.p2p_url.clone(),
            p2p_cert_sha256,
            max_body_bytes: self.node.max_body_bytes,
            max_sse_event_bytes: self.node.max_sse_event_bytes,
        };
        Ok(kdx_node::NodeClient::new(&ncfg)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
network = "mainnet01"
pairs = [["gql", "oracle"], ["gql", "node"]]
[node]
url = "http://127.0.0.1:31351"
[targets.gql]
url = "http://127.0.0.1:31363/graphql"
kind = "kda_indexer_graphql"
[targets.oracle]
url = "https://oracle.invalid/graphql"
kind = "graphql_oracle"
insecure_tls = true
rate_rps = 5.0
max_height = 6000000
"#;

    #[test]
    fn graphql_targets_parse_and_the_oracle_needs_a_ceiling() {
        let c: CompareConfig = toml::from_str(BASE).unwrap();
        c.validate().unwrap();
        assert!(c.has_graphql_pairs());
        // GraphQL targets are never the REST primary.
        assert_eq!(c.primary_target(), None);
        let no_ceiling = BASE.replace("max_height = 6000000\n", "");
        let c: CompareConfig = toml::from_str(&no_ceiling).unwrap();
        assert!(c.validate().unwrap_err().to_string().contains("max_height"));
        let wrong_kind = BASE.replace(
            "kind = \"kda_indexer_graphql\"",
            "kind = \"chainweb_data\"\ninsecure_tls = true",
        );
        let c: CompareConfig = toml::from_str(&wrong_kind).unwrap();
        assert!(c
            .validate()
            .unwrap_err()
            .to_string()
            .contains("insecure_tls"));
    }
}
