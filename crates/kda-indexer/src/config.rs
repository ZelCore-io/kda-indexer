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
    pub graphql: GraphqlConfig,
    #[serde(default)]
    pub search: SearchConfig,
    #[serde(default)]
    pub balances: BalancesConfig,
    #[serde(default)]
    pub nft: NftConfig,
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
    /// GraphQL listener (spec §5.3; `[graphql]` holds its limits).
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
        let graphql = parse("graphql", &self.graphql)?;
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
            graphql: Some(graphql),
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

/// `[graphql]`: limits of the GraphQL listener (plan GQ-9, GQ-13). The address is
/// `api.graphql`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct GraphqlConfig {
    /// Page size when a connection gets neither `first` nor `last`.
    pub default_page: u32,
    /// Larger `first`/`last` values are clamped to this.
    pub max_page: u32,
    pub max_depth: u32,
    pub max_complexity: u64,
    pub introspection: bool,
    /// Depth cap of introspection (exempt from `max_depth`).
    pub introspection_depth: u32,
    /// Operations in one batched request (a batch of N takes N rate tokens).
    pub max_batch: u32,
    pub max_query_bytes: u32,
    pub max_variables_bytes: u32,
    pub ws_subs_per_conn: u32,
    /// Subscriptions per client (IPv4 address or IPv6 /64) over all its connections.
    pub ws_subs_per_ip: u32,
    /// Subscriptions per IPv6 /48.
    pub ws_subs_per_48: u32,
    pub ws_subs_max: u32,
    pub ws_conns_per_ip: u32,
    /// Connections per IPv6 /48.
    pub ws_conns_per_48: u32,
    /// Connections per process.
    pub ws_conns_max: u32,
    pub ws_init_secs: u64,
    pub ws_idle_secs: u64,
    pub ws_tx_wait_secs: u64,
    pub feed_poll_ms: u64,
    /// Byte budget (MB) of the decoded blocks the subscription feed's ring holds.
    pub feed_ring_mb: u32,
    /// `networkInfo.networkHost`: a public name for clients, never the node URL.
    pub network_host: String,
    /// Automatic persisted queries. Not built: `true` is refused.
    pub apq: bool,
}

impl Default for GraphqlConfig {
    fn default() -> Self {
        Self {
            default_page: 20,
            max_page: 100,
            max_depth: 10,
            max_complexity: 2000,
            introspection: true,
            introspection_depth: 14,
            max_batch: 10,
            max_query_bytes: 32768,
            max_variables_bytes: 65536,
            ws_subs_per_conn: 20,
            ws_subs_per_ip: 40,
            ws_subs_per_48: 120,
            ws_subs_max: 2000,
            ws_conns_per_ip: 10,
            ws_conns_per_48: 30,
            ws_conns_max: 1000,
            ws_init_secs: 10,
            ws_idle_secs: 600,
            ws_tx_wait_secs: 3600,
            feed_poll_ms: 1000,
            feed_ring_mb: 64,
            network_host: String::new(),
            apq: false,
        }
    }
}

impl GraphqlConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=1000).contains(&self.max_page) && (1..=self.max_page).contains(&self.default_page),
            "graphql.max_page must be in 1..=1000 and graphql.default_page in 1..=max_page"
        );
        anyhow::ensure!(
            self.max_depth > 0
                && self.max_complexity > 0
                && self.introspection_depth > 0
                && self.max_batch > 0
                && self.ws_subs_per_conn > 0
                && self.ws_subs_per_ip > 0
                && self.ws_subs_per_48 > 0
                && self.ws_subs_max > 0
                && self.ws_conns_per_ip > 0
                && self.ws_conns_per_48 > 0
                && self.ws_conns_max > 0
                && self.ws_init_secs > 0
                && self.ws_idle_secs > 0
                && self.ws_tx_wait_secs > 0,
            "graphql limits must be > 0"
        );
        anyhow::ensure!(
            (1024..=kdx_graphql::MAX_BODY_BYTES as u32).contains(&self.max_query_bytes)
                && (1..=kdx_graphql::MAX_BODY_BYTES as u32).contains(&self.max_variables_bytes),
            "graphql.max_query_bytes must be in 1024..={0} and max_variables_bytes in 1..={0}",
            kdx_graphql::MAX_BODY_BYTES
        );
        anyhow::ensure!(
            self.feed_poll_ms >= 100,
            "graphql.feed_poll_ms must be at least 100"
        );
        anyhow::ensure!(
            (8..=4096).contains(&self.feed_ring_mb),
            "graphql.feed_ring_mb must be in 8..=4096"
        );
        anyhow::ensure!(
            !self.apq,
            "graphql.apq: automatic persisted queries are not built in this version"
        );
        anyhow::ensure!(
            !self.network_host.contains('<') && !self.network_host.contains('>'),
            "graphql.network_host still holds a placeholder"
        );
        Ok(())
    }

    pub fn settings(&self, network: &str) -> kdx_graphql::GraphqlSettings {
        use std::time::Duration;
        kdx_graphql::GraphqlSettings {
            default_page: self.default_page as usize,
            max_page: self.max_page as usize,
            max_depth: self.max_depth as usize,
            max_complexity: self.max_complexity,
            introspection: self.introspection,
            introspection_depth: self.introspection_depth as usize,
            max_batch: self.max_batch as usize,
            max_query_bytes: self.max_query_bytes as usize,
            max_variables_bytes: self.max_variables_bytes as usize,
            max_body_bytes: kdx_graphql::MAX_BODY_BYTES,
            ws_subs_per_conn: self.ws_subs_per_conn as usize,
            ws_subs_per_ip: self.ws_subs_per_ip as usize,
            ws_subs_per_48: self.ws_subs_per_48 as usize,
            ws_subs_max: self.ws_subs_max as usize,
            ws_conns_per_ip: self.ws_conns_per_ip as usize,
            ws_conns_per_48: self.ws_conns_per_48 as usize,
            ws_conns_max: self.ws_conns_max as usize,
            ws_init: Duration::from_secs(self.ws_init_secs),
            ws_idle: Duration::from_secs(self.ws_idle_secs),
            ws_tx_wait: Duration::from_secs(self.ws_tx_wait_secs),
            feed_poll: Duration::from_millis(self.feed_poll_ms),
            feed_ring_bytes: self.feed_ring_mb as usize * (1 << 20),
            network_id: network.to_owned(),
            network_host: self.network_host.clone(),
            apq: self.apq,
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
    /// Code index mode (S-1): `auto` (decided by the projection at rebuild), `full`, `fallback`.
    pub mode: String,
    /// Fallback mode: bodies shorter than this many bytes (and not a module or interface) are
    /// not indexed. 0 = derive it from the projection.
    pub fallback_min_len: u32,
    /// Tail stage commit period.
    pub commit_secs: u64,
    /// Concurrent searches (all search routes together); more answer 503.
    pub concurrency: u32,
    /// Concurrent searches per client; more answer 429.
    pub per_client: u32,
    /// `search rebuild` threads (decode and tantivy writer).
    pub build_threads: u32,
    /// `search rebuild` tantivy writer heap, MiB (also the periodic rebuild's).
    pub heap_mb: u32,
    /// Periodic in-process rebuild (keeps readers' upper expansion window short); false turns
    /// it off.
    pub periodic_rebuild: bool,
    /// Rebuild when the index is this many days old (0: never by age).
    pub rebuild_days: u32,
    /// Rebuild when the upper expansion window (tip − `rebuilt_through_height`) reaches this
    /// many heights (0: never by window).
    pub rebuild_window_heights: u32,
}

impl SearchConfig {
    pub fn mode_sel(&self) -> anyhow::Result<kdx_search::ModeSel> {
        kdx_search::ModeSel::parse(&self.mode, self.fallback_min_len)
    }
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            min_len: 4,
            max_results: 100,
            max_candidates: 10_000,
            max_scan_heights: 20_000,
            timeout_ms: 2000,
            mode: "auto".into(),
            fallback_min_len: 0,
            commit_secs: 60,
            concurrency: 4,
            per_client: 1,
            build_threads: 2,
            heap_mb: 512,
            periodic_rebuild: true,
            rebuild_days: 7,
            rebuild_window_heights: 30_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BalancesConfig {
    /// Run the balance seed and the `/local` reconciler inside `run` (B-5).
    pub enabled: bool,
    /// `/local` calls started per second (B-1; also the reconciler node client's rate).
    pub local_rps: u32,
    /// `/local` calls in flight.
    pub local_concurrency: u32,
    /// Most dirty keys of one non-coin module per reconcile batch (I-7).
    pub module_share: u32,
    /// Most `Balances` rows of one non-coin module (I-7).
    pub module_rows: u32,
    /// Most `Balances` rows of one account (I-7; `coin` rows are always kept).
    pub account_rows: u32,
    /// Hours after which a module's fungible check and a not-fungible verdict are re-done.
    pub recheck_hours: u32,
    /// Modules `GET /keys/{pubkey}/accounts` lists without `token` (M-1, SEC-2). Any module
    /// can implement fungible-v2 and report a balance under any key, so only the ones listed
    /// here are governance-trusted; others appear only when asked for by `token`.
    pub key_modules: Vec<String>,
}

impl Default for BalancesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            local_rps: 50,
            local_concurrency: 8,
            module_share: 32,
            module_rows: 1_000_000,
            account_rows: 1_000,
            recheck_hours: 24,
            key_modules: vec!["coin".to_owned()],
        }
    }
}

/// Most `/local` calls per second (B-1, H-1).
pub const MAX_LOCAL_RPS: u32 = 50;

impl BalancesConfig {
    /// Value ranges (M-6): `local_rps` 0 would turn both `/local` rate limiters off.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=MAX_LOCAL_RPS).contains(&self.local_rps),
            "balances.local_rps must be in 1..={MAX_LOCAL_RPS} (got {})",
            self.local_rps
        );
        anyhow::ensure!(
            (1..=64).contains(&self.local_concurrency),
            "balances.local_concurrency must be in 1..=64 (got {})",
            self.local_concurrency
        );
        anyhow::ensure!(
            self.module_share >= 1
                && self.module_rows >= 1
                && self.account_rows >= 1
                && self.recheck_hours >= 1,
            "balances.module_share, module_rows, account_rows and recheck_hours must be >= 1"
        );
        for m in &self.key_modules {
            anyhow::ensure!(
                kdx_node::local::valid_module(m),
                "balances.key_modules: `{m}` is not a module name (name or namespace.name)"
            );
        }
        Ok(())
    }
}

/// `[nft]`: which ledger modules the NFT tables trust.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct NftConfig {
    /// Qualified names of the ledger modules whose events update `NftBalances`. Anyone can
    /// deploy a module named `ledger`, so none is trusted by its name alone.
    pub ledgers: Vec<String>,
}

impl Default for NftConfig {
    fn default() -> Self {
        Self {
            ledgers: kdx_hot::nft::DEFAULT_LEDGERS
                .iter()
                .map(|s| (*s).to_owned())
                .collect(),
        }
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
        self.balances.validate()?;
        self.graphql.validate()?;
        let s = &self.search;
        s.mode_sel()?;
        anyhow::ensure!(
            s.commit_secs > 0 && s.concurrency > 0 && s.per_client > 0 && s.build_threads > 0,
            "search.commit_secs, concurrency, per_client and build_threads must be > 0"
        );
        anyhow::ensure!(s.heap_mb >= 16, "search.heap_mb must be at least 16");
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

#[cfg(test)]
mod balances_tests {
    use super::*;

    /// `[graphql]` defaults are the plan's GQ-9 values; bad values are refused.
    #[test]
    fn graphql_values_are_validated() {
        let ok = GraphqlConfig::default();
        assert!(ok.validate().is_ok());
        let s = ok.settings("mainnet01");
        assert_eq!(
            (
                s.default_page,
                s.max_page,
                s.max_depth,
                s.max_complexity,
                s.max_batch
            ),
            (20, 100, 10, 2000, 10)
        );
        for bad in [
            GraphqlConfig {
                apq: true,
                ..ok.clone()
            },
            GraphqlConfig {
                default_page: 101,
                ..ok.clone()
            },
            GraphqlConfig {
                max_depth: 0,
                ..ok.clone()
            },
            GraphqlConfig {
                max_query_bytes: 1 << 20,
                ..ok.clone()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }

    /// M-6: `local_rps` 0 (no limit) or above the B-1 budget, and zero concurrency, are refused.
    #[test]
    fn balances_values_are_validated() {
        let ok = BalancesConfig::default();
        assert!(ok.validate().is_ok());
        assert_eq!(ok.key_modules, ["coin"]);
        let wide = BalancesConfig {
            key_modules: vec!["coin".into(), "kaddex.kdx".into()],
            ..ok.clone()
        };
        assert!(wide.validate().is_ok());
        for bad in [
            BalancesConfig {
                local_rps: 0,
                ..ok.clone()
            },
            BalancesConfig {
                local_rps: MAX_LOCAL_RPS + 1,
                ..ok.clone()
            },
            BalancesConfig {
                local_concurrency: 0,
                ..ok.clone()
            },
            BalancesConfig {
                account_rows: 0,
                ..ok.clone()
            },
            BalancesConfig {
                key_modules: vec!["coin".into(), "free.evil; drop".into()],
                ..ok.clone()
            },
            BalancesConfig {
                key_modules: vec!["".into()],
                ..ok.clone()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }
}
