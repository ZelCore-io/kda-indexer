//! `serve`: the read-only API process (native, compat, admin) over a data dir another
//! process writes (P3: beside run E; later read replicas). Read-only MDBX open, no `LOCK`,
//! no ingest. Node tips come from a light `/cut` poll; the node is optional.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use kdx_api::{Api, Store, TipWatch};
use kdx_hot::HotStore;
use tokio_util::sync::CancellationToken;

use crate::config::Config;

/// `/cut` poll interval for node tips.
pub const TIP_POLL: Duration = Duration::from_secs(10);

pub fn signals(token: CancellationToken) -> anyhow::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    tokio::spawn(async move {
        tokio::select! {
            _ = term.recv() => tracing::warn!("SIGTERM: stopping"),
            _ = int.recv() => tracing::warn!("SIGINT: stopping"),
        }
        super::notify::stopping();
        token.cancel();
    });
    Ok(())
}

/// Poll the node's cut into `tips` until `stop` (errors leave the tips to go stale).
pub fn spawn_tip_poll(cfg: &Config, tips: TipWatch, stop: CancellationToken) {
    let node = match super::node_client(cfg) {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(error = %e, "api: no node client, node tips stay unknown");
            return;
        }
    };
    tokio::spawn(async move {
        loop {
            match node.cut().await {
                Ok(c) => tips.set(c.hashes.iter().map(|(k, e)| (*k, e.height)).collect()),
                Err(e) => tracing::debug!(error = %e, "api: /cut poll failed"),
            }
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(TIP_POLL) => {}
            }
        }
    });
}

/// Write the metrics to `metrics.textfile` every 15 s when it is configured.
pub fn spawn_textfile(cfg: &Config, stop: CancellationToken) {
    if let Some(path) = cfg.metrics.textfile.clone() {
        tokio::spawn(kdx_metrics::textfile_loop(
            path,
            Duration::from_secs(15),
            stop,
        ));
    }
}

/// Build the API over a store (shared by `serve` and `run`).
pub fn api(cfg: &Config, store: Arc<Store>, tips: TipWatch) -> anyhow::Result<Api> {
    let search = kdx_api::compat::SearchSettings {
        min_len: cfg.search.min_len as usize,
        max_scan_heights: cfg.search.max_scan_heights,
        timeout_ms: cfg.search.timeout_ms,
    };
    Ok(Api::new(store, cfg.api.settings(&cfg.network)?, tips)
        .native_routes(kdx_api::native::router())
        .compat_routes(kdx_api::compat::router(search)))
}

pub async fn run(cfg: &Config, data_dir: &Path) -> anyhow::Result<()> {
    let data_dir = std::fs::canonicalize(data_dir)
        .with_context(|| format!("data dir {}", data_dir.display()))?;
    let hot = Arc::new(HotStore::open_ro(&data_dir.join("hot"))?);
    let store = Arc::new(Store::open(hot, &data_dir)?);
    let token = CancellationToken::new();
    signals(token.clone())?;
    let tips = TipWatch::default();
    spawn_tip_poll(cfg, tips.clone(), token.clone());
    spawn_textfile(cfg, token.clone());
    // `serve` has no ingest: the watchdog watches the API runtime only.
    super::notify::spawn_watchdog(&[kdx_types::heartbeat::Component::Api]);
    super::notify::spawn_api_heartbeat(token.clone());
    super::notify::spawn_status(token.clone(), || "serving (read-only)".into());
    tracing::info!(data_dir = %data_dir.display(), "serve: read-only API");
    api(cfg, store, tips)?
        .on_bound(|| super::notify::ready("serving (read-only)"))
        .serve(cfg.api.binds()?, token)
        .await
}
