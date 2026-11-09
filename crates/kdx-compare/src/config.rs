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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub url: Url,
    pub kind: TargetKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    ChainwebData,
    KdaIndexerCompat,
    KdaTxs,
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
        self.pairs
            .first()
            .map(|(a, _)| a.as_str())
            .or_else(|| self.targets.keys().next().map(String::as_str))
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
