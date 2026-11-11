//! Configuration file (spec section 6.5).

use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

/// `[node]` section; shared with kdx-compare.
pub use kdx_node::NodeConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub network: String,
    pub data_dir: PathBuf,
    pub node: NodeConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub backfill: BackfillConfig,
    #[serde(default)]
    pub freezer: FreezerConfig,
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
    /// Node request budget during backfill; falls back to `ingest.max_rps` (spec section 4.2).
    pub max_rps: Option<u32>,
    /// zstd level for range files.
    pub zstd_level: i32,
    /// Records sampled per file kind to train a range dictionary.
    pub dict_samples: u32,
    /// Raw spool (ETL) bytes buffered per range before a sorted run is spilled to `etl/`.
    pub etl_mem_mb: u64,
    /// Byte budget (MiB) of backfill's anonymous memory: `(ranges_in_flight + 1) × etl_mem_mb`
    /// plus the range writer, whose posting buffers get what the spools leave. Backfill logs
    /// a WARN and fetches one range at a time while `RssAnon` is above it.
    pub mem_cap_mb: u64,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            ranges_in_flight: 2,
            range_heights: 100_000,
            step_heights: 10_000,
            min_free_gb: 60,
            max_rps: None,
            zstd_level: 19,
            dict_samples: 20_000,
            etl_mem_mb: 1024,
            mem_cap_mb: 6144,
        }
    }
}

/// `[storage]`: MDBX hot store geometry (spec section 3.3, B9).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    /// Upper bound of the MDBX map.
    pub mdbx_max_gb: u64,
    /// MDBX growth step.
    pub mdbx_growth_mb: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            mdbx_max_gb: 64,
            mdbx_growth_mb: 256,
        }
    }
}

/// `[freezer]`: step files written from the hot tail.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FreezerConfig {
    /// zstd level for step files (re-compressed at the range level on merge).
    pub zstd_level: i32,
}

impl Default for FreezerConfig {
    fn default() -> Self {
        Self { zstd_level: 9 }
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

/// Listen addresses are plain strings so example configs can carry placeholders (a
/// `<placeholder>` left in is refused by `validate`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ApiConfig {
    pub native: String,
    pub compat: String,
    /// Parsed, not bound before P7.
    pub graphql: String,
    pub admin: String,
    /// Peers (IP or CIDR) whose `X-Forwarded-For` is trusted for the client IP.
    pub trusted_proxies: Vec<String>,
    pub rate_per_ip: u32,
    pub rate_burst: u32,
    /// Per-request deadline (504 past it).
    pub deadline_ms: u64,
    /// Requests in flight over all listeners (503 past it).
    pub max_in_flight: usize,
    /// Response body cap (503 "narrow the query" past it).
    pub max_body_mb: u64,
    /// Response body cap of `/v1/txs`.
    pub v1txs_max_body_mb: u64,
    /// Concurrent store reads (blocking threads).
    pub read_threads: usize,
    /// `/readyz` fails when a non-halted chain lags the node by more blocks than this.
    pub ready_max_lag: u32,
    /// Most event-name lists one event query expands to.
    pub max_event_lists: usize,
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
            rate_burst: 40,
            deadline_ms: 5000,
            max_in_flight: 512,
            max_body_mb: 8,
            v1txs_max_body_mb: 32,
            read_threads: 4,
            ready_max_lag: 20,
            max_event_lists: 256,
        }
    }
}

impl ApiConfig {
    /// Parsed listen addresses. Native and admin must be loopback or private (decision Q4);
    /// compat may be any address (it binds loopback until P8 by configuration).
    pub fn binds(&self) -> anyhow::Result<kdx_api::server::Binds> {
        let parse = |key: &str, v: &str| -> anyhow::Result<std::net::SocketAddr> {
            anyhow::ensure!(
                !v.contains('<') && !v.contains('>'),
                "api.{key} still holds a placeholder ({v:?})"
            );
            v.parse()
                .with_context(|| format!("api.{key} is not an address:port ({v:?})"))
        };
        let native = parse("native", &self.native)?;
        let compat = parse("compat", &self.compat)?;
        let _graphql = parse("graphql", &self.graphql)?;
        let admin = parse("admin", &self.admin)?;
        for (key, a) in [("native", native), ("admin", admin)] {
            anyhow::ensure!(
                kdx_api::limits::is_internal(a.ip()),
                "api.{key} must bind a loopback or private address (got {a})"
            );
        }
        Ok(kdx_api::server::Binds {
            native: Some(native),
            compat: Some(compat),
            admin: Some(admin),
        })
    }

    pub fn settings(&self, network: &str) -> anyhow::Result<kdx_api::server::ApiSettings> {
        let mut trusted = Vec::new();
        for p in &self.trusted_proxies {
            trusted.push(
                kdx_api::limits::IpNet::parse(p)
                    .with_context(|| format!("api.trusted_proxies: {p:?} is not an IP or CIDR"))?,
            );
        }
        Ok(kdx_api::server::ApiSettings {
            network: network.to_owned(),
            deadline: std::time::Duration::from_millis(self.deadline_ms),
            max_in_flight: self.max_in_flight,
            max_body_bytes: (self.max_body_mb as usize) << 20,
            v1txs_max_body_bytes: (self.v1txs_max_body_mb as usize) << 20,
            rate_per_ip: self.rate_per_ip,
            rate_burst: self.rate_burst,
            trusted_proxies: trusted,
            read_threads: self.read_threads,
            ready_max_lag: self.ready_max_lag,
            max_event_lists: self.max_event_lists,
        })
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

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Config> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        cfg.validate()
            .with_context(|| format!("checking config {}", path.display()))?;
        Ok(cfg)
    }

    /// Value ranges serde cannot express.
    pub fn validate(&self) -> anyhow::Result<()> {
        // The node silently answers at most 1000 entries per payload batch.
        anyhow::ensure!(
            (1..=MAX_BATCH_SIZE).contains(&self.ingest.batch_size),
            "ingest.batch_size must be in 1..={MAX_BATCH_SIZE} (got {})",
            self.ingest.batch_size
        );
        let b = &self.backfill;
        anyhow::ensure!(
            b.step_heights >= 10,
            "backfill.step_heights must be at least 10 (got {})",
            b.step_heights
        );
        anyhow::ensure!(
            b.range_heights.is_multiple_of(b.step_heights),
            "backfill.range_heights ({}) must be a multiple of backfill.step_heights ({})",
            b.range_heights,
            b.step_heights
        );
        anyhow::ensure!(
            (1..=22).contains(&b.zstd_level) && (1..=22).contains(&self.freezer.zstd_level),
            "zstd levels must be in 1..=22"
        );
        anyhow::ensure!(
            b.etl_mem_mb >= 16 && b.mem_cap_mb >= 512,
            "backfill.etl_mem_mb must be >= 16 and backfill.mem_cap_mb >= 512"
        );
        anyhow::ensure!(b.dict_samples > 0, "backfill.dict_samples must be > 0");
        anyhow::ensure!(
            self.storage.mdbx_max_gb > 0 && self.storage.mdbx_growth_mb > 0,
            "storage.mdbx_max_gb and storage.mdbx_growth_mb must be > 0"
        );
        anyhow::ensure!(
            b.max_rps != Some(0),
            "backfill.max_rps must be > 0 when set"
        );
        let a = &self.api;
        a.binds()?;
        a.settings(&self.network)?;
        anyhow::ensure!(
            a.deadline_ms >= 100
                && a.max_in_flight > 0
                && a.read_threads > 0
                && a.rate_per_ip > 0
                && a.rate_burst > 0
                && a.max_body_mb > 0
                && a.v1txs_max_body_mb > 0
                && a.max_event_lists > 0,
            "api: deadline_ms must be >= 100 and the other limits > 0"
        );
        Ok(())
    }

    /// Node request budget for backfill: `backfill.max_rps`, else `ingest.max_rps`.
    pub fn backfill_max_rps(&self) -> u32 {
        self.backfill.max_rps.unwrap_or(self.ingest.max_rps)
    }
}

/// The node's own cap on `payload/outputs/batch` entries.
pub const MAX_BATCH_SIZE: u32 = 1000;
