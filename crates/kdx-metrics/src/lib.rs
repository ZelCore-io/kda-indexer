//! One Prometheus registry per process (spec §6.7), shared by `kdx-node`, `kdx-ingest` and
//! `kdx-api`. `get()` is always usable; `init` stamps `kdx_build_info`.
//!
//! Label values are bounded by construction: `chain` is 0..19, `endpoint` and `stage` are
//! `&'static str` call-site names, `route` is the router's path template (never a raw path),
//! `status` is a status code. Nothing request-derived (accounts, request keys, raw URLs) is
//! ever a label value.

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::OnceLock;
use std::time::Duration;

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;
use tokio_util::sync::CancellationToken;

type Labels = Vec<(&'static str, String)>;
type Hist = Family<Labels, Histogram, fn() -> Histogram>;
type Count = Family<Labels, Counter>;
type Gau = Family<Labels, Gauge>;
/// A float gauge.
pub type FGauge = Gauge<f64, AtomicU64>;

fn seconds() -> Histogram {
    // 1 ms .. ~65 s.
    Histogram::new(exponential_buckets(0.001, 2.0, 17))
}
fn depths() -> Histogram {
    Histogram::new(exponential_buckets(1.0, 2.0, 10))
}

fn hist(f: fn() -> Histogram) -> Hist {
    Family::new_with_constructor(f)
}

pub struct Metrics {
    registry: Registry,
    pub build_info: Gau,
    pub node_tip_height: Gau,
    pub indexed_tip_height: Gau,
    pub chain_lag_blocks: Gau,
    pub chain_halted: Gau,
    pub frozen_frontier_height: Gauge,
    pub ingest_blocks: Count,
    pub ingest_txs: Count,
    pub stage_duration: Hist,
    pub backfill_blocks_per_second: FGauge,
    pub backfill_next_height: Gauge,
    pub backfill_end_height: Gauge,
    pub reorgs: Count,
    pub reorg_depth: Hist,
    pub node_requests: Count,
    pub node_request_seconds: Hist,
    pub hot_store_bytes: Gauge,
    pub segment_bytes: Gau,
    pub disk_free_bytes: Gauge,
    pub mdbx_commit_seconds: Histogram,
    pub http_requests: Count,
    pub http_request_seconds: Hist,
    pub search_rejected: Count,
    /// Payload Merkle check per ingested block: `result` = verified | mismatch_legacy |
    /// mismatch (B8, ruling R-5).
    pub merkle_blocks: Count,
    /// Freezer work: `op` = freeze | merge.
    pub freezer_runs: Count,
    pub freezer_seconds: Hist,
    /// Read views: `phase` = open (taking the txn and segment snapshot) | held (open to drop).
    pub read_view_seconds: Hist,
    pub dict_snapshot_names: Gauge,
    /// Rows in `BalanceDirty` (spec §3.9, plan Task 9).
    pub balance_dirty_rows: Gauge,
    /// `/local` reconcile outcomes: `result` = ok | absent | error | skipped | parked.
    pub balance_reconcile: Count,
    /// Duration of one `/local` reconcile call (`result` as above).
    pub balance_reconcile_seconds: Hist,
}

impl Metrics {
    fn new() -> Metrics {
        let mut r = Registry::default();
        // prometheus-client appends `_total` to counters and `_bucket`/`_sum`/`_count` to
        // histograms; names below are registered without those suffixes.
        macro_rules! reg {
            ($name:literal, $help:literal, $m:expr) => {{
                let m = $m;
                r.register($name, $help, m.clone());
                m
            }};
        }
        let build_info = reg!(
            "kdx_build_info",
            "Build: version, git sha, network (value 1)",
            Gau::default()
        );
        let node_tip_height = reg!(
            "kdx_node_tip_height",
            "Node cut height per chain",
            Gau::default()
        );
        let indexed_tip_height = reg!(
            "kdx_indexed_tip_height",
            "Indexed tail height per chain",
            Gau::default()
        );
        let chain_lag_blocks = reg!(
            "kdx_chain_lag_blocks",
            "Node tip minus indexed tip per chain",
            Gau::default()
        );
        let chain_halted = reg!(
            "kdx_chain_halted",
            "1 when ingest halted the chain",
            Gau::default()
        );
        let frozen_frontier_height = reg!(
            "kdx_frozen_frontier_height",
            "First height not yet frozen into segments",
            Gauge::default()
        );
        let ingest_blocks = reg!(
            "kdx_ingest_blocks",
            "Blocks indexed (mode backfill|tail)",
            Count::default()
        );
        let ingest_txs = reg!(
            "kdx_ingest_txs",
            "Txs indexed (mode backfill|tail)",
            Count::default()
        );
        let stage_duration = reg!(
            "kdx_stage_duration_seconds",
            "Ingest stage durations",
            hist(seconds)
        );
        let backfill_blocks_per_second = reg!(
            "kdx_backfill_blocks_per_second",
            "Blocks per second of the last committed backfill range",
            FGauge::default()
        );
        let backfill_next_height = reg!(
            "kdx_backfill_next_height",
            "Backfill: first height not yet committed",
            Gauge::default()
        );
        let backfill_end_height = reg!(
            "kdx_backfill_end_height",
            "Backfill: target end height (exclusive); 0 when no backfill ran",
            Gauge::default()
        );
        let reorgs = reg!("kdx_reorgs", "Reorgs applied per chain", Count::default());
        let reorg_depth = reg!(
            "kdx_reorg_depth",
            "Reorg depth in blocks per chain",
            hist(depths)
        );
        let node_requests = reg!(
            "kdx_node_requests",
            "Node requests per endpoint and status (0 = transport error)",
            Count::default()
        );
        let node_request_seconds = reg!(
            "kdx_node_request_seconds",
            "Node request duration per endpoint and status",
            hist(seconds)
        );
        let hot_store_bytes = reg!(
            "kdx_hot_store_bytes",
            "Size of the hot store data file",
            Gauge::default()
        );
        let segment_bytes = reg!(
            "kdx_segment_bytes",
            "Segment bytes per file kind",
            Gau::default()
        );
        let disk_free_bytes = reg!(
            "kdx_disk_free_bytes",
            "Free bytes on the data volume",
            Gauge::default()
        );
        let mdbx_commit_seconds = reg!(
            "kdx_mdbx_commit_seconds",
            "Duration of a hot-store write commit",
            Histogram::new(exponential_buckets(0.001, 2.0, 17))
        );
        let http_requests = reg!(
            "kdx_http_requests",
            "HTTP requests per api, route template and status",
            Count::default()
        );
        let http_request_seconds = reg!(
            "kdx_http_request_seconds",
            "HTTP request duration per api, route template and status",
            hist(seconds)
        );
        let search_rejected = reg!(
            "kdx_search_rejected",
            "Event text searches rejected (too_short|busy|timeout|cap)",
            Count::default()
        );
        let merkle_blocks = reg!(
            "kdx_merkle_blocks",
            "Payload Merkle check per ingested block (result verified|mismatch_legacy|mismatch)",
            Count::default()
        );
        let freezer_runs = reg!(
            "kdx_freezer_runs",
            "Freezer operations (op freeze|merge)",
            Count::default()
        );
        let freezer_seconds = reg!(
            "kdx_freezer_seconds",
            "Freezer operation duration (op freeze|merge)",
            hist(seconds)
        );
        let read_view_seconds = reg!(
            "kdx_read_view_seconds",
            "Read view timings (phase open|held)",
            hist(seconds)
        );
        let dict_snapshot_names = reg!(
            "kdx_dict_snapshot_names",
            "Names in the API dictionary snapshot",
            Gauge::default()
        );
        let balance_dirty_rows = reg!(
            "kdx_balance_dirty_rows",
            "Balance rows waiting for a /local reconcile",
            Gauge::default()
        );
        let balance_reconcile = reg!(
            "kdx_balance_reconcile",
            "Balance reconcile outcomes (result ok|absent|error|skipped|parked)",
            Count::default()
        );
        let balance_reconcile_seconds = reg!(
            "kdx_balance_reconcile_seconds",
            "Balance reconcile call duration (result ok|absent|error|skipped)",
            hist(seconds)
        );
        Metrics {
            registry: r,
            build_info,
            node_tip_height,
            indexed_tip_height,
            chain_lag_blocks,
            chain_halted,
            frozen_frontier_height,
            ingest_blocks,
            ingest_txs,
            stage_duration,
            backfill_blocks_per_second,
            backfill_next_height,
            backfill_end_height,
            reorgs,
            reorg_depth,
            node_requests,
            node_request_seconds,
            hot_store_bytes,
            segment_bytes,
            disk_free_bytes,
            mdbx_commit_seconds,
            http_requests,
            http_request_seconds,
            search_rejected,
            merkle_blocks,
            freezer_runs,
            freezer_seconds,
            read_view_seconds,
            dict_snapshot_names,
            balance_dirty_rows,
            balance_reconcile,
            balance_reconcile_seconds,
        }
    }

    // Recording helpers: one place that fixes label names.

    pub fn http(&self, api: &'static str, route: &str, status: u16, secs: f64) {
        let l: Labels = vec![
            ("api", api.into()),
            ("route", route.into()),
            ("status", status.to_string()),
        ];
        self.http_requests.get_or_create(&l).inc();
        self.http_request_seconds.get_or_create(&l).observe(secs);
    }

    pub fn node_request(&self, endpoint: &str, status: u16, secs: f64) {
        let l: Labels = vec![
            ("endpoint", endpoint.into()),
            ("status", status.to_string()),
        ];
        self.node_requests.get_or_create(&l).inc();
        self.node_request_seconds.get_or_create(&l).observe(secs);
    }

    pub fn stage(&self, stage: &'static str, secs: f64) {
        self.stage_duration
            .get_or_create(&vec![("stage", stage.into())])
            .observe(secs);
    }

    pub fn ingested(&self, mode: &'static str, blocks: u64, txs: u64) {
        let l: Labels = vec![("mode", mode.into())];
        self.ingest_blocks.get_or_create(&l).inc_by(blocks);
        self.ingest_txs.get_or_create(&l).inc_by(txs);
    }

    pub fn reorg(&self, chain: u8, depth: u32) {
        let l: Labels = vec![("chain", chain.to_string())];
        self.reorgs.get_or_create(&l).inc();
        self.reorg_depth.get_or_create(&l).observe(f64::from(depth));
    }

    pub fn freezer(&self, op: &'static str, secs: f64) {
        let l: Labels = vec![("op", op.into())];
        self.freezer_runs.get_or_create(&l).inc();
        self.freezer_seconds.get_or_create(&l).observe(secs);
    }

    pub fn read_view(&self, phase: &'static str, secs: f64) {
        self.read_view_seconds
            .get_or_create(&vec![("phase", phase.into())])
            .observe(secs);
    }

    pub fn merkle(&self, result: &'static str) {
        self.merkle_blocks
            .get_or_create(&vec![("result", result.into())])
            .inc();
    }

    /// One balance reconcile outcome (`ok|absent|error|skipped|parked`) and its duration.
    pub fn balance_reconcile(&self, result: &'static str, secs: f64) {
        let l: Labels = vec![("result", result.into())];
        self.balance_reconcile.get_or_create(&l).inc();
        self.balance_reconcile_seconds
            .get_or_create(&l)
            .observe(secs);
    }

    pub fn search_rejected(&self, reason: &'static str) {
        self.search_rejected
            .get_or_create(&vec![("reason", reason.into())])
            .inc();
    }

    /// Per-chain tip gauges. `node` is None while the node tips are unknown (the node and lag
    /// series are then left as they were).
    pub fn chain(&self, chain: u8, node: Option<u64>, indexed: u64, halted: bool) {
        let l: Labels = vec![("chain", chain.to_string())];
        self.indexed_tip_height
            .get_or_create(&l)
            .set(i64::try_from(indexed).unwrap_or(i64::MAX));
        self.chain_halted.get_or_create(&l).set(i64::from(halted));
        if let Some(n) = node {
            self.node_tip_height
                .get_or_create(&l)
                .set(i64::try_from(n).unwrap_or(i64::MAX));
            self.chain_lag_blocks
                .get_or_create(&l)
                .set(i64::try_from(n.saturating_sub(indexed)).unwrap_or(i64::MAX));
        }
    }

    /// Largest chain lag last sampled (None before the first sample with known node tips).
    pub fn max_chain_lag(&self) -> Option<i64> {
        (0..20u8)
            .filter_map(|c| {
                let l: Labels = vec![("chain", c.to_string())];
                self.chain_lag_blocks.get(&l).map(|g| g.get())
            })
            .max()
    }

    pub fn segment_kind_bytes(&self, kind: &str, bytes: u64) {
        self.segment_bytes
            .get_or_create(&vec![("kind", kind.to_string())])
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
    }

    pub fn encode(&self) -> String {
        let mut out = String::new();
        let _ = prometheus_client::encoding::text::encode(&mut out, &self.registry);
        out
    }
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

/// The process-wide metrics (created on first use).
pub fn get() -> &'static Metrics {
    METRICS.get_or_init(Metrics::new)
}

/// Stamp `kdx_build_info`. Call once at startup.
pub fn init(network: &str, version: &str, git_sha: &str) {
    let l: Labels = vec![
        ("version", version.to_string()),
        ("git_sha", git_sha.to_string()),
        ("network", network.to_string()),
    ];
    get().build_info.get_or_create(&l).set(1);
}

/// Text exposition of the whole registry.
pub fn encode() -> String {
    get().encode()
}

/// Write the exposition to `path` every `every` until `shutdown`: temp file in the same
/// directory, then rename. A failure is logged once and retried silently.
pub async fn textfile_loop(path: PathBuf, every: Duration, shutdown: CancellationToken) {
    let mut failed = false;
    loop {
        // The collector reads the classic text format: drop the OpenMetrics terminator.
        match write_atomic(&path, &encode().replace("# EOF\n", "")) {
            Ok(()) => {
                if failed {
                    tracing::info!(path = %path.display(), "metrics: textfile writable again");
                }
                failed = false;
            }
            Err(e) => {
                if !failed {
                    tracing::warn!(path = %path.display(), error = %e, "metrics: textfile write failed (retrying every interval; /metrics is unaffected)");
                }
                failed = true;
            }
        }
        tokio::select! {
            () = shutdown.cancelled() => break,
            () = tokio::time::sleep(every) => {}
        }
    }
}

/// Write `text` to `path` through a sibling temp file and a rename.
pub fn write_atomic(path: &Path, text: &str) -> std::io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn families_encode_with_template_labels() {
        init("mainnet01", "0.0.0", "abc");
        let m = get();
        m.http("compat", "/txs/account/{account}", 200, 0.01);
        m.node_request("header/branch", 200, 0.02);
        m.ingested("tail", 3, 5);
        m.reorg(2, 1);
        m.chain(0, Some(10), 8, false);
        let t = encode();
        for want in [
            "kdx_build_info{version=\"0.0.0\",git_sha=\"abc\",network=\"mainnet01\"} 1",
            "kdx_http_requests_total{api=\"compat\",route=\"/txs/account/{account}\",status=\"200\"} 1",
            "kdx_http_request_seconds_bucket",
            "kdx_node_requests_total{endpoint=\"header/branch\",status=\"200\"} 1",
            "kdx_ingest_blocks_total{mode=\"tail\"} 3",
            "kdx_chain_lag_blocks{chain=\"0\"} 2",
            "kdx_reorg_depth_bucket",
        ] {
            assert!(t.contains(want), "missing {want}\n{t}");
        }
    }

    #[test]
    fn textfile_write_replaces_atomically() {
        let d = std::env::temp_dir().join(format!("kdx-metrics-test-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        let p = d.join("x.prom");
        write_atomic(&p, "a 1\n").unwrap();
        write_atomic(&p, "a 2\n").unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "a 2\n");
        assert!(!d.join("x.prom.tmp").exists());
        std::fs::remove_dir_all(&d).unwrap();
    }
}
