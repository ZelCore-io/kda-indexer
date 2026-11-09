//! Configuration file (spec section 6.5).

use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub network: String,
    pub data_dir: PathBuf,
    pub node: NodeConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
    #[serde(default)]
    pub backfill: BackfillConfig,
    #[serde(default)]
    pub tail: TailConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub search: SearchConfig,
    #[serde(default)]
    pub balances: BalancesConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub log: LogConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub url: Url,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p2p_url: Option<Url>,
    /// Hex SHA-256 of the node's self-signed leaf certificate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p2p_cert_sha256: Option<String>,
    #[serde(default = "d_timeout_ms")]
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct IngestConfig {
    pub finality_depth: u64,
    pub batch_size: u32,
    pub node_concurrency: usize,
    pub max_rps: u32,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            finality_depth: 1000,
            batch_size: 50,
            node_concurrency: 8,
            max_rps: 200,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BackfillConfig {
    pub ranges_in_flight: u32,
    pub range_heights: u64,
    pub step_heights: u64,
    pub min_free_gb: u64,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            ranges_in_flight: 2,
            range_heights: 100_000,
            step_heights: 10_000,
            min_free_gb: 60,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TailConfig {
    pub commit_ms: u64,
    pub commit_blocks: u32,
    pub cut_poll_secs: u64,
}

impl Default for TailConfig {
    fn default() -> Self {
        Self {
            commit_ms: 1000,
            commit_blocks: 40,
            cut_poll_secs: 10,
        }
    }
}

/// Listen addresses are plain strings so example configs can carry placeholders.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ApiConfig {
    pub native: String,
    pub compat: String,
    pub graphql: String,
    pub admin: String,
    pub trusted_proxies: Vec<String>,
    pub rate_per_ip: u32,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            native: "127.0.0.1:31361".into(),
            compat: "127.0.0.1:31362".into(),
            graphql: "127.0.0.1:31363".into(),
            admin: "127.0.0.1:31369".into(),
            trusted_proxies: Vec::new(),
            rate_per_ip: 20,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SearchConfig {
    pub min_len: u32,
    pub max_results: u32,
    pub max_candidates: u32,
    pub max_scan_heights: u32,
    pub timeout_ms: u64,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            min_len: 4,
            max_results: 100,
            max_candidates: 10_000,
            max_scan_heights: 20_000,
            timeout_ms: 2000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BalancesConfig {
    pub local_rps: u32,
}

impl Default for BalancesConfig {
    fn default() -> Self {
        Self { local_rps: 50 }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetricsConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub textfile: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LogConfig {
    /// "text" or "json".
    pub format: String,
    pub level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            format: "text".into(),
            level: "info".into(),
        }
    }
}

fn d_timeout_ms() -> u64 {
    30_000
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Config> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))
    }
}
