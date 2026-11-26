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
    /// `[dex_usd]`: the DIA KDA/USD feed for the DEX USD fields (B3). Off by default.
    #[serde(default)]
    pub dex_usd: DexUsdConfig,
}

/// `[dex_usd]`: poll DIA's KDA/USD quotation (stock's source) and store an hourly rate for
/// the DEX USD fields (`kdx_ingest::dia`). Owner-approved outside dependency (B3). Off (the
/// default): the DEX USD fields answer 0, as stock does without a KDA/USD price.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DexUsdConfig {
    pub enabled: bool,
    /// https URL of a DIA `assetQuotation` answer. Redirects are never followed.
    pub url: String,
    /// Allow an `http://` URL (tests and local mirrors only; off by default).
    pub allow_http: bool,
    pub interval_s: u64,
    pub timeout_ms: u64,
    /// Sent as the User-Agent: keep it generic.
    pub user_agent: String,
    pub max_response_bytes: u32,
    /// Accepted price range (USD per KDA).
    pub min_price: f64,
    pub max_price: f64,
    /// A move larger than this (percent of the last accepted price) is held back until
    /// `confirm_polls` polls in a row agree.
    pub max_jump_pct: f64,
    pub confirm_polls: u32,
    /// Oldest accepted answer `Time`, seconds.
    pub max_age_s: u64,
}

impl Default for DexUsdConfig {
    fn default() -> Self {
        let d = kdx_ingest::dia::DiaSettings::default();
        Self {
            enabled: false,
            url: d.url,
            allow_http: false,
            interval_s: d.interval.as_secs(),
            timeout_ms: d.timeout.as_millis() as u64,
            user_agent: d.user_agent,
            max_response_bytes: d.max_response_bytes as u32,
            min_price: d.min_price,
            max_price: d.max_price,
            max_jump_pct: d.max_jump_pct,
            confirm_polls: d.confirm_polls,
            max_age_s: d.max_age.as_secs(),
        }
    }
}

impl DexUsdConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        let u = url::Url::parse(&self.url).map_err(|e| anyhow::anyhow!("dex_usd.url: {e}"))?;
        anyhow::ensure!(
            (u.scheme() == "https" || (self.allow_http && u.scheme() == "http"))
                && u.host().is_some(),
            "dex_usd.url must be an https URL with a host (http only with dex_usd.allow_http)"
        );
        anyhow::ensure!(
            (300..=86_400).contains(&self.max_age_s),
            "dex_usd.max_age_s must be in 300..=86400"
        );
        anyhow::ensure!(
            (60..=86_400).contains(&self.interval_s),
            "dex_usd.interval_s must be in 60..=86400"
        );
        anyhow::ensure!(
            (1_000..=60_000).contains(&self.timeout_ms),
            "dex_usd.timeout_ms must be in 1000..=60000"
        );
        anyhow::ensure!(
            (256..=1 << 20).contains(&self.max_response_bytes),
            "dex_usd.max_response_bytes must be in 256..=1048576"
        );
        anyhow::ensure!(
            self.min_price > 0.0 && self.max_price.is_finite() && self.min_price < self.max_price,
            "dex_usd: need 0 < min_price < max_price"
        );
        anyhow::ensure!(
            self.max_jump_pct > 0.0
                && self.max_jump_pct.is_finite()
                && (1..=100).contains(&self.confirm_polls),
            "dex_usd: max_jump_pct > 0 and confirm_polls in 1..=100"
        );
        anyhow::ensure!(
            !self.user_agent.is_empty()
                && self.user_agent.is_ascii()
                && self.user_agent.len() <= 64,
            "dex_usd.user_agent: 1..=64 ASCII characters"
        );
        Ok(())
    }

    pub fn settings(&self) -> kdx_ingest::dia::DiaSettings {
        kdx_ingest::dia::DiaSettings {
            url: self.url.clone(),
            interval: std::time::Duration::from_secs(self.interval_s),
            timeout: std::time::Duration::from_millis(self.timeout_ms),
            user_agent: self.user_agent.clone(),
            max_response_bytes: self.max_response_bytes as usize,
            min_price: self.min_price,
            max_price: self.max_price,
            max_jump_pct: self.max_jump_pct,
            confirm_polls: self.confirm_polls,
            max_age: std::time::Duration::from_secs(self.max_age_s),
        }
    }
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

/// The addresses of one listener: a string (`"127.0.0.1:31369"`) or an array of strings
/// (`["127.0.0.1:31369", "10.1.2.3:31369"]`). Every address is bound. They are plain strings
/// so example configs can carry placeholders (a `<placeholder>` left in is refused by
/// `validate`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum ListenAddrs {
    One(String),
    Many(Vec<String>),
}

impl ListenAddrs {
    pub fn as_slice(&self) -> &[String] {
        match self {
            ListenAddrs::One(s) => std::slice::from_ref(s),
            ListenAddrs::Many(v) => v,
        }
    }
}

impl From<&str> for ListenAddrs {
    fn from(s: &str) -> Self {
        ListenAddrs::One(s.to_owned())
    }
}

impl<'de> Deserialize<'de> for ListenAddrs {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = ListenAddrs;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an \"address:port\" string or an array of them")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<ListenAddrs, E> {
                Ok(ListenAddrs::One(v.to_owned()))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<ListenAddrs, A::Error> {
                let mut v = Vec::new();
                while let Some(s) = seq.next_element::<String>()? {
                    v.push(s);
                }
                Ok(ListenAddrs::Many(v))
            }
        }
        d.deserialize_any(V)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ApiConfig {
    pub native: ListenAddrs,
    pub compat: ListenAddrs,
    /// GraphQL listener (spec §5.3; `[graphql]` holds its limits).
    pub graphql: ListenAddrs,
    pub admin: ListenAddrs,
    /// Peers (IP or CIDR) whose `X-Forwarded-For` is trusted for the client IP.
    pub trusted_proxies: Vec<String>,
    pub rate_per_ip: u32,
    pub rate_burst: u32,
    /// Per-request deadline (504 past it).
    pub deadline_ms: u64,
    /// Requests in flight over all listeners (503 past it).
    pub max_in_flight: usize,
    /// Serve Swagger UI and the OpenAPI documents under `/docs` on the native and compat
    /// listeners (404 when false).
    pub docs_enabled: bool,
    /// Response body cap (503 "narrow the query" past it).
    pub max_body_mb: u64,
    /// Response body cap of `/v1/txs`.
    pub v1txs_max_body_mb: u64,
    /// Deadline of a `/v1/txs` request (default 30,000: kda-txs answers 404 at 30 s).
    pub v1txs_deadline_ms: u64,
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
            docs_enabled: true,
            max_body_mb: 8,
            v1txs_max_body_mb: 32,
            v1txs_deadline_ms: 30_000,
            read_threads: 4,
            ready_max_lag: 20,
            max_event_lists: 256,
        }
    }
}

impl ApiConfig {
    /// Parsed listen addresses, one or more per listener. Native and admin must be loopback
    /// or private (decision Q4); compat and graphql may be any address (they bind loopback
    /// until the load-balancer host config adds a private LAN address).
    ///
    /// Refused: an empty list, a placeholder, an address listed twice (within a listener or
    /// across listeners), and a wildcard (`0.0.0.0`, `[::]`) next to another address on the
    /// same port, in one listener or across two (the wildcard already covers it, so the second
    /// bind would fail).
    pub fn binds(&self) -> anyhow::Result<kdx_api::server::Binds> {
        use std::net::SocketAddr;
        let parse = |key: &str, l: &ListenAddrs| -> anyhow::Result<Vec<SocketAddr>> {
            let list = l.as_slice();
            anyhow::ensure!(!list.is_empty(), "api.{key} lists no address");
            let mut out: Vec<SocketAddr> = Vec::with_capacity(list.len());
            for v in list {
                anyhow::ensure!(
                    !v.contains('<') && !v.contains('>'),
                    "api.{key} still holds a placeholder ({v:?})"
                );
                let a: SocketAddr = v
                    .parse()
                    .with_context(|| format!("api.{key} is not an address:port ({v:?})"))?;
                anyhow::ensure!(!out.contains(&a), "api.{key} lists {a} twice");
                out.push(a);
            }
            for a in out.iter().filter(|a| a.ip().is_unspecified()) {
                anyhow::ensure!(
                    out.iter().filter(|b| b.port() == a.port()).count() == 1,
                    "api.{key}: {a} already covers every address on port {}; list it alone",
                    a.port()
                );
            }
            Ok(out)
        };
        let native = parse("native", &self.native)?;
        let compat = parse("compat", &self.compat)?;
        let graphql = parse("graphql", &self.graphql)?;
        let admin = parse("admin", &self.admin)?;
        for (key, list) in [("native", &native), ("admin", &admin)] {
            for a in list {
                anyhow::ensure!(
                    kdx_api::limits::is_internal(a.ip()),
                    "api.{key} must bind a loopback or private address (got {a})"
                );
            }
        }
        let all = [
            ("native", &native),
            ("compat", &compat),
            ("graphql", &graphql),
            ("admin", &admin),
        ];
        for (i, (k1, l1)) in all.iter().enumerate() {
            for (k2, l2) in &all[i + 1..] {
                if let Some(a) = l1.iter().find(|a| l2.contains(a)) {
                    anyhow::bail!("api.{k1} and api.{k2} both list {a}");
                }
                // A wildcard in one listener already covers every address of the other on
                // that port (the second bind would fail at start on Linux).
                let clash = l1.iter().find_map(|a| {
                    l2.iter()
                        .find(|b| {
                            a.port() == b.port()
                                && (a.ip().is_unspecified() || b.ip().is_unspecified())
                        })
                        .map(|b| (a, b))
                });
                if let Some((a, b)) = clash {
                    anyhow::bail!(
                        "api.{k1} {a} and api.{k2} {b} share a port and one is a wildcard"
                    );
                }
            }
        }
        Ok(kdx_api::server::Binds {
            native,
            compat,
            graphql,
            admin,
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
            v1txs_deadline: std::time::Duration::from_millis(self.v1txs_deadline_ms),
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
    /// Page size of the legacy cost model (`check-ops`) only; served pages, subscription
    /// `quantity` and NFT holdings have the fixed 1,000-row safety cap (FIX-3 part D).
    pub max_page: u32,
    /// Read-budget model only; stock's depth limit (9) is what refuses.
    pub max_depth: u32,
    /// Read budget (logged above it, never refused); stock's complexity limit (4000) refuses.
    pub max_complexity: u64,
    pub introspection: bool,
    /// Read-budget model only; stock's introspection depth limit (12) is what refuses.
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
    /// Retired: `totalCount` is exact and runs to `api.deadline_ms` (a cut count is a
    /// `COUNT_TIMEOUT` error). Still accepted, and ignored, so older config files load.
    pub count_budget_ms: Option<u64>,
    /// Serve sender lists (`transactions(accountName)`, `FungibleChainAccount.transactions`,
    /// REST `role=sender`) from the `sndr` postings once the sender index is complete
    /// (FIX-2). `false` forces the old path (the account's postings filtered on
    /// `meta.sender`, bounded by the 1 s scan window); kept for the on-host equivalence check.
    pub sender_index: bool,
    /// `networkInfo.networkHost`: a public name for clients, never the node URL.
    pub network_host: String,
    /// Automatic persisted queries. Not built: `true` is refused.
    pub apq: bool,
    /// `[graphql.pact]`: the `pactQuery` / `gasLimitEstimate` node passthrough (B1).
    pub pact: PactConfig,
    /// `[graphql.account_miss]`: the node read on a missed account row (gap Q).
    pub account_miss: AccountMissConfig,
}

/// `[graphql.account_miss]`: `fungibleAccount`, `fungibleChainAccount(s)` and
/// `FungibleAccount.chainAccounts` read `(<module>.details)` from the node (`node.url`) for an
/// asked chain the store has no row for, as stock reads every chain (gap Q, owner decision
/// 2026-11-26). Found rows are fed back to the reconciler in `run`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AccountMissConfig {
    /// On by default; off answers from the store only.
    pub enabled: bool,
    /// Node reads per request, all fields and aliases together (an account's 20 chains x 2).
    pub max_reads_per_request: u32,
    /// Per client (the API limiter's keys): reads per second (0 = no per-client limit) and
    /// burst (at least 20: one account's chains).
    pub per_ip_rps: u32,
    pub per_ip_burst: u32,
    /// Reads in flight across the process; the longest wait for one of these slots, past
    /// which the read is refused (the chain answers store-only, flagged).
    pub max_in_flight: u32,
    pub slot_wait_ms: u64,
    /// Node calls per second across the process and their burst. A budget of its own, on top
    /// of the reconciler's `balances.local_rps`, so at most 25/s (default 20): a read with no
    /// token is refused at once, never queued. `node_burst` is 20..=64: at least one
    /// account's chains, and at most about three accounts' worth of reads in one instant on
    /// top of the reconciler.
    pub node_rps: u32,
    pub node_burst: u32,
    /// Per read, slot wait included.
    pub timeout_ms: u64,
    /// How long a found row, an absent answer and a node error (0: not cached) are reused.
    /// Absent answers are one chain mask per (account, module), kept 6 h by default (at most
    /// 24 h): a row created without an event on a chain cached absent stays hidden up to that
    /// long; rows with an event are indexed and served from the store regardless.
    pub ttl_found_secs: u64,
    pub ttl_absent_secs: u64,
    pub ttl_error_secs: u64,
    /// Cached answers at most (two generations, the older dropped whole).
    pub cache_entries: u32,
}

impl Default for AccountMissConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_reads_per_request: 40,
            per_ip_rps: 20,
            per_ip_burst: 60,
            max_in_flight: 16,
            slot_wait_ms: 150,
            node_rps: 20,
            node_burst: 40,
            timeout_ms: 3_000,
            ttl_found_secs: 30,
            ttl_absent_secs: 21_600,
            ttl_error_secs: 30,
            cache_entries: 100_000,
        }
    }
}

impl AccountMissConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=400).contains(&self.max_reads_per_request)
                && (1..=256).contains(&self.max_in_flight)
                && self.slot_wait_ms <= 1_000
                && (1..=25).contains(&self.node_rps)
                && (20..=64).contains(&self.node_burst)
                && (500..=30_000).contains(&self.timeout_ms)
                && self.ttl_found_secs <= 3_600
                && (1..=86_400).contains(&self.ttl_absent_secs)
                && self.ttl_error_secs <= 600
                && (1..=1_000_000).contains(&self.cache_entries),
            "graphql.account_miss: max_reads_per_request 1..=400, max_in_flight 1..=256, \
             slot_wait_ms <= 1000, node_rps 1..=25, node_burst 20..=64, \
             timeout_ms 500..=30000, ttl_found_secs <= 3600, ttl_absent_secs 1..=86400, \
             ttl_error_secs <= 600, cache_entries 1..=1000000"
        );
        anyhow::ensure!(
            self.per_ip_rps == 0 || self.per_ip_burst >= 20,
            "graphql.account_miss.per_ip_burst must be at least 20 (or per_ip_rps 0)"
        );
        Ok(())
    }

    /// The reader's limits on a network of `chains` chains.
    pub fn settings(&self, chains: u8) -> kdx_graphql::miss::MissSettings {
        use std::time::Duration;
        kdx_graphql::miss::MissSettings {
            max_reads_per_request: self.max_reads_per_request as usize,
            per_ip_rps: self.per_ip_rps,
            per_ip_burst: self.per_ip_burst,
            node_rps: self.node_rps,
            node_burst: self.node_burst,
            timeout: Duration::from_millis(self.timeout_ms),
            slot_wait: Duration::from_millis(self.slot_wait_ms),
            ttl_found: Duration::from_secs(self.ttl_found_secs),
            ttl_absent: Duration::from_secs(self.ttl_absent_secs),
            ttl_error: Duration::from_secs(self.ttl_error_secs),
            cache_entries: self.cache_entries as usize,
            max_in_flight: self.max_in_flight as usize,
            chains,
        }
    }
}

/// `[graphql.pact]`: client Pact run through the node's `/local` (`node.url`, `network`),
/// as stock does (B1, owner-approved 2026-11-26). Never `/send`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct PactConfig {
    /// Off (the default): both fields answer `NOT_SUPPORTED`. An operator turns it on to
    /// let clients run read-only Pact on the configured node.
    pub enabled: bool,
    /// Items (`pactQuery` entries plus `gasLimitEstimate` inputs) per request, all aliases
    /// and batch operations together.
    pub max_items: u32,
    /// `/local` calls in flight across the process (2x that many requests may wait).
    pub max_in_flight: u32,
    /// Per client (IPv4 address, IPv6 /64, with a /24 or /48 overflow tier): calls per
    /// second (0 = no per-IP limit) and burst (at least `max_items`).
    pub per_ip_rps: u32,
    pub per_ip_burst: u32,
    /// Per item, slot wait included (stock: 10 s).
    pub timeout_ms: u64,
    /// Largest `code` + `data` of one item, or one gas input.
    pub max_input_bytes: u32,
    /// Largest `meta.gasLimit` a client command may ask for.
    pub max_gas_limit: u64,
    /// Largest node answer read.
    pub max_response_bytes: u32,
    /// Node calls per second across the process (0 = no limit beyond `max_in_flight`;
    /// default twice the default `max_in_flight`).
    pub node_rps: u32,
}

/// Chains of a Chainweb network (M4): 20 on every network chainweb runs today. The store's
/// chain graph, when it has one, overrides this at `serve` (`cmd::serve::pact_pass`).
pub fn network_chains(network: &str) -> u8 {
    match network {
        "mainnet01" | "testnet04" | "testnet05" | "development" => 20,
        _ => 20,
    }
}

impl Default for PactConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_items: 20,
            max_in_flight: 8,
            per_ip_rps: 5,
            per_ip_burst: 20,
            timeout_ms: 10_000,
            max_input_bytes: 65_536,
            max_gas_limit: 150_000,
            max_response_bytes: 1 << 20,
            node_rps: 16,
        }
    }
}

impl PactConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=100).contains(&self.max_items)
                && (1..=256).contains(&self.max_in_flight)
                && (1_000..=60_000).contains(&self.timeout_ms)
                && (1..=kdx_graphql::MAX_BODY_BYTES as u32).contains(&self.max_input_bytes)
                && (1..=10_000_000).contains(&self.max_gas_limit)
                && (1024..=(64 << 20)).contains(&self.max_response_bytes),
            "graphql.pact: max_items 1..=100, max_in_flight 1..=256, timeout_ms 1000..=60000, \
             max_input_bytes 1..={}, max_gas_limit 1..=10000000, max_response_bytes 1024..=64 MiB",
            kdx_graphql::MAX_BODY_BYTES
        );
        anyhow::ensure!(
            self.per_ip_rps == 0 || self.per_ip_burst >= self.max_items,
            "graphql.pact.per_ip_burst must be at least max_items (or per_ip_rps 0)"
        );
        Ok(())
    }

    /// The passthrough's limits on a network of `chains` chains.
    pub fn settings(&self, chains: u8) -> kdx_graphql::pact::PactSettings {
        kdx_graphql::pact::PactSettings {
            max_items: self.max_items as usize,
            max_in_flight: self.max_in_flight as usize,
            per_ip_rps: self.per_ip_rps,
            per_ip_burst: self.per_ip_burst,
            timeout: std::time::Duration::from_millis(self.timeout_ms),
            max_input_bytes: self.max_input_bytes as usize,
            max_gas_limit: self.max_gas_limit,
            max_response_bytes: self.max_response_bytes as usize,
            chains,
        }
    }
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
            count_budget_ms: None,
            sender_index: true,
            network_host: String::new(),
            apq: false,
            pact: PactConfig::default(),
            account_miss: AccountMissConfig::default(),
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
        self.pact.validate()?;
        self.account_miss.validate()?;
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
            ws_ping: kdx_graphql::ws::WS_PING,
            ws_silence: kdx_graphql::ws::WS_SILENCE,
            ws_tx_wait: Duration::from_secs(self.ws_tx_wait_secs),
            feed_poll: Duration::from_millis(self.feed_poll_ms),
            feed_ring_bytes: self.feed_ring_mb as usize * (1 << 20),
            network_id: network.to_owned(),
            network_host: self.network_host.clone(),
            apq: self.apq,
            back_keys: kdx_graphql::txlist::BACK_KEYS,
            balance_touch_keys: kdx_graphql::types::account::BALANCE_TOUCH_KEYS,
            pact: self.pact.settings(network_chains(network)),
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
    /// Dirty keys of one non-coin module that take a batch slot before the others get theirs
    /// (I-7): the fair share. Room left in a seed batch is topped up beyond it.
    pub module_share: u32,
    /// Most `Balances` rows of one non-coin module (I-7).
    pub module_rows: u32,
    /// Most `Balances` rows of one confirmed `fungible-v2` module other than `coin` (F-14:
    /// anyone can deploy one). `reconcile-balances --status` shows it, the largest module's
    /// row count and the capped modules.
    pub fungible_module_rows: u32,
    /// Most `Balances` rows of one account (I-7; `coin` rows are always kept).
    pub account_rows: u32,
    /// Hours after which a module's fungible check and a not-fungible verdict are re-done.
    pub recheck_hours: u32,
    /// Modules `GET /keys/{pubkey}/accounts` lists without `token` (M-1, SEC-2). Any module
    /// can implement fungible-v2 and report a balance under any key, so only the ones listed
    /// here are governance-trusted; others appear only when asked for by `token`.
    pub key_modules: Vec<String>,
    /// Modules the key listing never sends a `keys` (or `describe-module`) read, on any chain.
    /// The node blocks its cut for about 14 minutes while it answers a `keys` of a huge table
    /// (`free.crankk01.crankk01-token-table`, Nov 2026), so even a first read must not happen.
    pub keylist_skip: Vec<String>,
    /// Worker threads that read and decode the D3 history walk's frozen slices (marks are
    /// still applied in TxNum order, one write txn per slice). 0: the cores less 2, at least 1.
    pub history_walk_threads: u32,
}

impl Default for BalancesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            local_rps: 50,
            local_concurrency: 8,
            module_share: 32,
            module_rows: 1_000_000,
            fungible_module_rows: kdx_ingest::reconcile::DEFAULT_FUNGIBLE_MODULE_ROWS,
            account_rows: 1_000,
            recheck_hours: 24,
            key_modules: vec!["coin".to_owned()],
            keylist_skip: vec!["free.crankk01".to_owned()],
            history_walk_threads: 0,
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
                && self.fungible_module_rows >= 1
                && self.account_rows >= 1
                && self.recheck_hours >= 1,
            "balances.module_share, module_rows, fungible_module_rows, account_rows and \
             recheck_hours must be >= 1"
        );
        anyhow::ensure!(
            self.history_walk_threads <= 256,
            "balances.history_walk_threads must be in 0..=256 (got {})",
            self.history_walk_threads
        );
        for m in &self.key_modules {
            anyhow::ensure!(
                kdx_node::local::valid_module(m),
                "balances.key_modules: `{m}` is not a module name (name or namespace.name)"
            );
        }
        for m in &self.keylist_skip {
            anyhow::ensure!(
                kdx_node::local::valid_module(m),
                "balances.keylist_skip: `{m}` is not a module name (name or namespace.name)"
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
        // F-45: 0 is a multiple of every step (plan_units looped forever on it), and the
        // backfill plans ranges in u32 heights.
        anyhow::ensure!(
            b.range_heights >= b.step_heights && u32::try_from(b.range_heights).is_ok(),
            "backfill.range_heights must be in step_heights..=4294967295 (got {})",
            b.range_heights
        );
        anyhow::ensure!(
            b.ranges_in_flight > 0,
            "backfill.ranges_in_flight must be > 0"
        );
        anyhow::ensure!(
            self.ingest.node_concurrency > 0,
            "ingest.node_concurrency must be > 0"
        );
        // Owner ruling (audit F-45): 0 is refused, not taken as "no limit".
        anyhow::ensure!(self.ingest.max_rps > 0, "ingest.max_rps must be > 0");
        kdx_segment::manifest::check_network(&self.network).context("network")?;
        if let Some(pin) = &self.node.p2p_cert_sha256 {
            kdx_node::parse_sha256_hex(pin).context("node.p2p_cert_sha256")?;
        }
        anyhow::ensure!(
            self.node.p2p_url.is_none() || self.node.p2p_cert_sha256.is_some(),
            "node.p2p_url is set but node.p2p_cert_sha256 is not"
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
        self.dex_usd.validate()?;
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
                && a.v1txs_deadline_ms >= 100
                && a.max_in_flight > 0
                && a.read_threads > 0
                && a.rate_per_ip > 0
                && a.rate_burst > 0
                && a.max_body_mb > 0
                && a.v1txs_max_body_mb > 0
                && a.max_event_lists > 0,
            "api: deadline_ms and v1txs_deadline_ms must be >= 100 and the other limits > 0"
        );
        let g = &self.graphql;
        anyhow::ensure!(
            g.ws_subs_per_conn <= g.ws_subs_per_ip.min(100)
                && g.ws_subs_per_conn.saturating_add(2) <= a.rate_burst,
            "graphql.ws_subs_per_conn must be <= ws_subs_per_ip (max 100), and \
             ws_subs_per_conn + 2 (the ws opening borrow, R-P5P9-57) <= api.rate_burst"
        );
        anyhow::ensure!(
            g.ws_subs_per_ip <= g.ws_subs_per_48 && g.ws_subs_per_48 <= g.ws_subs_max,
            "graphql: ws_subs_per_ip <= ws_subs_per_48 <= ws_subs_max must hold"
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

    /// B1 fix round 1 (M2, I3, M4): the passthrough is off unless an operator turns it on,
    /// has a global node rate by default, and takes its chains from the network.
    #[test]
    fn pact_defaults_off_with_a_node_rate() {
        let p = PactConfig::default();
        assert!(!p.enabled);
        assert!(p.node_rps > 0);
        assert_eq!(p.settings(network_chains("mainnet01")).chains, 20);
        assert_eq!(p.settings(7).chains, 7);
    }

    /// The miss read's node budget comes on top of the reconciler's: at most 25/s, default
    /// 20/s; the cache is capped at a million entries.
    #[test]
    fn account_miss_node_rate_is_bounded() {
        let m = AccountMissConfig::default();
        assert!(m.validate().is_ok());
        assert_eq!(m.node_rps, 20);
        for bad in [
            AccountMissConfig {
                node_rps: 26,
                ..AccountMissConfig::default()
            },
            AccountMissConfig {
                cache_entries: 1_000_001,
                ..AccountMissConfig::default()
            },
            AccountMissConfig {
                slot_wait_ms: 1_001,
                ..AccountMissConfig::default()
            },
            AccountMissConfig {
                node_burst: 65,
                ..AccountMissConfig::default()
            },
            AccountMissConfig {
                node_burst: 19,
                ..AccountMissConfig::default()
            },
            AccountMissConfig {
                ttl_absent_secs: 86_401,
                ..AccountMissConfig::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
        let edge = AccountMissConfig {
            node_burst: 64,
            ..AccountMissConfig::default()
        };
        assert!(edge.validate().is_ok());
        let s = m.settings(20);
        assert_eq!((s.node_rps, s.node_burst), (20, 40));
        assert_eq!(s.ttl_absent, std::time::Duration::from_secs(6 * 3600));
        assert_eq!(s.slot_wait, std::time::Duration::from_millis(150));
    }

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
        assert_eq!(ok.keylist_skip, ["free.crankk01"]);
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
            BalancesConfig {
                keylist_skip: vec!["free.x; drop".into()],
                ..ok.clone()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }
}

#[cfg(test)]
mod api_tests {
    use super::*;

    fn api(text: &str) -> anyhow::Result<ApiConfig> {
        Ok(toml::from_str::<ApiConfig>(text)?)
    }

    /// Task 10: a listener takes one address or an array; every address is in `Binds`.
    #[test]
    fn listeners_take_a_string_or_an_array() {
        let a = api(r#"
            native = "127.0.0.1:31361"
            compat = ["127.0.0.1:31362", "10.1.2.3:31362"]
            graphql = ["127.0.0.1:31363"]
            admin = ["127.0.0.1:31369", "10.1.2.3:31369", "[::1]:31369", "[fd00::5]:31369"]
        "#)
        .unwrap();
        assert_eq!(a.native, ListenAddrs::One("127.0.0.1:31361".into()));
        let b = a.binds().unwrap();
        let s = |v: &[std::net::SocketAddr]| v.iter().map(|a| a.to_string()).collect::<Vec<_>>();
        assert_eq!(s(&b.native), ["127.0.0.1:31361"]);
        assert_eq!(s(&b.compat), ["127.0.0.1:31362", "10.1.2.3:31362"]);
        assert_eq!(s(&b.graphql), ["127.0.0.1:31363"]);
        assert_eq!(
            s(&b.admin),
            [
                "127.0.0.1:31369",
                "10.1.2.3:31369",
                "[::1]:31369",
                "[fd00::5]:31369"
            ]
        );
        // The defaults are one loopback address each.
        let d = ApiConfig::default().binds().unwrap();
        assert_eq!(
            (
                d.native.len(),
                d.compat.len(),
                d.graphql.len(),
                d.admin.len()
            ),
            (1, 1, 1, 1)
        );
        // A string and a one-element array serialise back as they came.
        let text = toml::to_string(&a).unwrap();
        assert!(text.contains(r#"native = "127.0.0.1:31361""#), "{text}");
        assert!(text.contains(r#"graphql = ["127.0.0.1:31363"]"#), "{text}");
    }

    /// Task 10: a public address on native or admin, in any position, is refused, as are empty
    /// lists, placeholders, repeats and a wildcard next to another address on its port.
    #[test]
    fn bad_listener_lists_are_refused() {
        for (text, want) in [
            (
                r#"admin = ["127.0.0.1:31369", "8.8.8.8:31369"]"#,
                "loopback or private",
            ),
            (r#"admin = "0.0.0.0:31369""#, "loopback or private"),
            (
                r#"native = ["[::1]:31361", "[2001:db8::1]:31361"]"#,
                "loopback or private",
            ),
            (r#"compat = []"#, "lists no address"),
            (
                r#"compat = ["127.0.0.1:31362", "<private-lan-ip>:31362"]"#,
                "placeholder",
            ),
            (
                r#"compat = ["127.0.0.1:31362", "127.0.0.1"]"#,
                "not an address:port",
            ),
            (
                r#"compat = ["127.0.0.1:31362", "127.0.0.1:31362"]"#,
                "twice",
            ),
            (
                r#"compat = ["0.0.0.0:31362", "10.1.2.3:31362"]"#,
                "list it alone",
            ),
            (
                r#"graphql = ["[::]:31363", "127.0.0.1:31363"]"#,
                "list it alone",
            ),
            (
                r#"graphql = "127.0.0.1:31362""#,
                "both list 127.0.0.1:31362",
            ),
            (
                r#"compat = "0.0.0.0:31362"
graphql = "127.0.0.1:31362""#,
                "share a port and one is a wildcard",
            ),
        ] {
            let e = api(text).unwrap().binds().unwrap_err();
            assert!(format!("{e:#}").contains(want), "{text}: {e:#}");
        }
        // A wildcard alone, or next to an address on another port, is fine for compat.
        let ok = api(r#"compat = ["0.0.0.0:31362", "127.0.0.1:31372"]"#).unwrap();
        assert!(ok.binds().is_ok());
        // A number or a table is a parse error that names the expected form.
        for text in ["admin = 31369", "admin = [31369]", "admin = { a = 1 }"] {
            let e = api(text).unwrap_err();
            assert!(format!("{e:#}").contains("string"), "{text}: {e:#}");
        }
    }
}

#[cfg(test)]
mod ws_bound_tests {
    use super::*;

    fn cfg(api: &str, graphql: &str) -> anyhow::Result<()> {
        let text = format!(
            "network = \"mainnet01\"\ndata_dir = \"data\"\n[node]\nurl = \"http://127.0.0.1:1848\"\n\
             [api]\n{api}\n[graphql]\n{graphql}\n"
        );
        toml::from_str::<Config>(&text)?.validate()
    }

    /// FIX-4 part B (B1): `api.v1txs_deadline_ms` defaults to 30,000 (kda-txs' own ceiling),
    /// reaches the settings, and below 100 is refused as `deadline_ms` is.
    #[test]
    fn v1txs_deadline_ms_defaults_reaches_settings_and_is_bounded() {
        let parse = |a: &str| {
            let text = format!(
                "network = \"mainnet01\"\ndata_dir = \"data\"\n[node]\n\
                 url = \"http://127.0.0.1:1848\"\n[api]\n{a}\n"
            );
            toml::from_str::<Config>(&text).unwrap()
        };
        let c = parse("");
        assert_eq!(c.api.v1txs_deadline_ms, 30_000);
        c.validate().unwrap();
        let s = parse("v1txs_deadline_ms = 12000")
            .api
            .settings("mainnet01")
            .unwrap();
        assert_eq!(s.v1txs_deadline, std::time::Duration::from_millis(12_000));
        assert!(cfg("v1txs_deadline_ms = 100", "").is_ok());
        let e = cfg("v1txs_deadline_ms = 99", "").unwrap_err();
        assert!(format!("{e:#}").contains("v1txs_deadline_ms"), "{e:#}");
    }

    /// `graphql.count_budget_ms` is retired (GQ-10: `totalCount` is exact): a file that still
    /// sets it loads and validates, and nothing reads it.
    #[test]
    fn count_budget_ms_is_accepted_and_ignored() {
        let parse = |g: &str| {
            let text = format!(
                "network = \"mainnet01\"\ndata_dir = \"data\"\n[node]\n\
                 url = \"http://127.0.0.1:1848\"\n[graphql]\n{g}\n"
            );
            toml::from_str::<Config>(&text).unwrap()
        };
        assert_eq!(parse("").graphql.count_budget_ms, None);
        for old in ["count_budget_ms = 50", "count_budget_ms = 0"] {
            let c = parse(old);
            c.validate().unwrap();
            assert!(c.graphql.count_budget_ms.is_some());
        }
    }

    /// `api.docs_enabled` defaults to true (Swagger UI under `/docs`), parses `false`, and is
    /// documented where operators look: both example configs and the API and operations docs.
    #[test]
    fn docs_enabled_parses_defaults_and_is_documented() {
        let parse = |a: &str| {
            let text = format!(
                "network = \"mainnet01\"\ndata_dir = \"data\"\n[node]\n\
                 url = \"http://127.0.0.1:1848\"\n[api]\n{a}\n"
            );
            toml::from_str::<Config>(&text).unwrap()
        };
        assert!(parse("").api.docs_enabled);
        assert!(ApiConfig::default().docs_enabled);
        let c = parse("docs_enabled = false");
        c.validate().unwrap();
        assert!(!c.api.docs_enabled);
        for (name, text) in [
            (
                "mainnet01.toml.example",
                include_str!("../../../packaging/config/mainnet01.toml.example"),
            ),
            (
                "testnet.toml.example",
                include_str!("../../../packaging/config/testnet.toml.example"),
            ),
        ] {
            assert!(
                text.lines().any(|l| l.starts_with("docs_enabled = true")),
                "{name} lacks docs_enabled"
            );
        }
        for (name, text) in [
            ("api.md", include_str!("../../../docs/api.md")),
            ("operations.md", include_str!("../../../docs/operations.md")),
        ] {
            assert!(
                text.contains("docs_enabled"),
                "{name} does not document docs_enabled"
            );
            assert!(text.contains("/docs"), "{name} does not name /docs");
        }
    }

    /// `graphql.sender_index` (FIX-2) defaults to true and parses `false`.
    #[test]
    fn sender_index_parses_and_defaults() {
        let parse = |g: &str| {
            let text = format!(
                "network = \"mainnet01\"\ndata_dir = \"data\"\n[node]\n\
                 url = \"http://127.0.0.1:1848\"\n[graphql]\n{g}\n"
            );
            toml::from_str::<Config>(&text).unwrap()
        };
        assert!(parse("").graphql.sender_index);
        let c = parse("sender_index = false");
        c.validate().unwrap();
        assert!(!c.graphql.sender_index);
    }

    /// The ws opening borrow is `ws_subs_per_conn + 2` (R-P5P9-57): the bound keeps it inside
    /// `api.rate_burst`, and a connection never holds more than its client may.
    #[test]
    fn ws_subs_per_conn_is_bounded_by_the_burst_and_the_client_cap() {
        cfg("", "").expect("defaults pass");
        let burst = "rate_burst = 40";
        cfg(burst, "ws_subs_per_conn = 38").expect("38 + 2 = rate_burst passes");
        let e = cfg(burst, "ws_subs_per_conn = 39").expect_err("39 + 2 > rate_burst");
        assert!(format!("{e:#}").contains("R-P5P9-57"), "{e:#}");
        let e = cfg(burst, "ws_subs_per_conn = 41\nws_subs_per_ip = 40")
            .expect_err("more than the client's cap");
        assert!(format!("{e:#}").contains("ws_subs_per_conn"), "{e:#}");
        // The hard ceiling of 100 holds even when the burst and the client cap allow more.
        cfg(
            "rate_burst = 500",
            "ws_subs_per_conn = 101\nws_subs_per_ip = 200\nws_subs_per_48 = 400",
        )
        .expect_err("over 100");
        cfg(
            "rate_burst = 500",
            "ws_subs_per_conn = 100\nws_subs_per_ip = 200\nws_subs_per_48 = 400",
        )
        .expect("100 passes");
    }

    /// Client caps nest: per IP <= per /48 <= per process.
    #[test]
    fn ws_client_caps_must_nest() {
        cfg(
            "",
            "ws_subs_per_ip = 40\nws_subs_per_48 = 40\nws_subs_max = 40",
        )
        .expect("equal caps pass");
        for bad in [
            "ws_subs_per_48 = 39",
            "ws_subs_per_48 = 3000",
            "ws_subs_max = 100",
        ] {
            let e = cfg("", bad).expect_err(bad);
            assert!(
                format!("{e:#}").contains("ws_subs_per_ip <="),
                "{bad}: {e:#}"
            );
        }
    }
}

#[cfg(test)]
mod validate_tests {
    use super::*;

    /// A config with `head` (top-level keys), `node` (extra `[node]` keys) and `rest` (more
    /// sections), validated.
    fn check(head: &str, node: &str, rest: &str) -> anyhow::Result<()> {
        let head = if head.contains("network") {
            head.to_owned()
        } else {
            format!("network = \"mainnet01\"\n{head}")
        };
        let text = format!(
            "{head}\ndata_dir = \"data\"\n[node]\nurl = \"http://127.0.0.1:1848\"\n{node}\n{rest}\n"
        );
        toml::from_str::<Config>(&text)?.validate()
    }

    fn refused(head: &str, node: &str, rest: &str, says: &str) {
        let e = check(head, node, rest).expect_err(rest);
        assert!(
            format!("{e:#}").contains(says),
            "{head} {node} {rest}: {e:#}"
        );
    }

    /// F-45: values that `config check` passed but that hang or fail at run time.
    #[test]
    fn zero_and_out_of_range_values_are_refused() {
        check("", "", "").expect("defaults pass");
        // 0 is a multiple of every step: `plan_units` looped forever on it.
        refused("", "", "[backfill]\nrange_heights = 0", "range_heights");
        refused(
            "",
            "",
            "[backfill]\nrange_heights = 5000000000\nstep_heights = 10",
            "range_heights",
        );
        refused(
            "",
            "",
            "[backfill]\nranges_in_flight = 0",
            "ranges_in_flight",
        );
        refused("", "", "[ingest]\nnode_concurrency = 0", "node_concurrency");
        // Owner ruling: 0 is refused rather than meaning "no limit".
        refused("", "", "[ingest]\nmax_rps = 0", "ingest.max_rps");
        check("", "", "[ingest]\nmax_rps = 1").expect("1 passes");
    }

    #[test]
    fn network_must_be_a_plain_name() {
        refused("network = \"../x\"", "", "", "network");
        refused("network = \"\"", "", "", "network");
        check("network = \"testnet04\"", "", "").expect("plain name passes");
    }

    #[test]
    fn p2p_cert_pin_is_checked_with_the_url() {
        let pin = "ab".repeat(32);
        check(
            "",
            &format!("p2p_url = \"https://127.0.0.1:1789\"\np2p_cert_sha256 = \"{pin}\""),
            "",
        )
        .expect("url plus a 64-hex pin passes");
        refused(
            "",
            "p2p_url = \"https://127.0.0.1:1789\"",
            "",
            "p2p_cert_sha256",
        );
        refused(
            "",
            "p2p_url = \"https://127.0.0.1:1789\"\np2p_cert_sha256 = \"abcd\"",
            "",
            "p2p_cert_sha256",
        );
        refused(
            "",
            &format!(
                "p2p_url = \"https://127.0.0.1:1789\"\np2p_cert_sha256 = \"{}\"",
                "zz".repeat(32)
            ),
            "",
            "p2p_cert_sha256",
        );
    }
}

#[cfg(test)]
mod dex_usd_tests {
    use super::DexUsdConfig;

    /// The DIA URL must be https unless `allow_http` (tests, local mirrors) says otherwise.
    #[test]
    fn dia_url_needs_https() {
        let mut c = DexUsdConfig::default();
        assert!(c.validate().is_ok(), "stock's https endpoint");
        c.url = "http://127.0.0.1:8080/q".into();
        assert!(c.validate().is_err());
        c.allow_http = true;
        assert!(c.validate().is_ok());
        c.url = "file:///etc/passwd".into();
        assert!(c.validate().is_err());
        c = DexUsdConfig {
            max_age_s: 10,
            ..DexUsdConfig::default()
        };
        assert!(c.validate().is_err());
    }
}
