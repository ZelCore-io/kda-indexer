//! `serve`: the read-only API process (native, compat, GraphQL, admin) over a data dir another
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

/// How often `serve` looks for ranges swapped under it.
pub const SWAP_WATCH: Duration = Duration::from_secs(60);

/// F-16: `serve` keeps the segment files it opened. After a `rebuild-range` swap or a
/// `segments add-index` it can pair an old index with a new file, so the operator must
/// restart it (docs/operations.md). This makes a missed restart visible: once a minute it
/// stats each range's `range.toml` and logs an error, once per range, for every range in the
/// on-disk manifest whose file is no longer the one it opened.
pub fn spawn_swap_watch(store: Arc<Store>, stop: CancellationToken) {
    tokio::spawn(async move {
        let mut warned = std::collections::HashSet::new();
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(SWAP_WATCH) => {}
            }
            let seg = store.segments();
            let swapped = seg.swapped_ranges();
            if swapped.is_empty() {
                continue;
            }
            // A merge deletes its steps' files: those leave the manifest, not a swap.
            let Ok(Some(m)) = kdx_segment::Manifest::load(&seg.dir) else {
                continue;
            };
            for r in swapped {
                let listed = m.entries.iter().any(|e| (e.start, e.end) == r);
                if listed && warned.insert(r) {
                    tracing::error!(
                        range = %format!("{}..{}", r.0, r.1),
                        "serve: range swapped on disk after serve opened it (rebuild-range or segments add-index); restart serve"
                    );
                }
            }
        }
    });
}

/// Build the API over a store (shared by `serve` and `run`). The search routes read the code
/// index in `data_dir/search/` and share one search gate (S-5).
pub fn api(
    cfg: &Config,
    store: Arc<Store>,
    tips: TipWatch,
    data_dir: &Path,
) -> anyhow::Result<Api> {
    store.set_sender_index(cfg.graphql.sender_index);
    let settings = kdx_api::compat::SearchSettings {
        min_len: cfg.search.min_len as usize,
        max_scan_heights: cfg.search.max_scan_heights,
        timeout_ms: cfg.search.timeout_ms,
        concurrency: cfg.search.concurrency as usize,
        per_client: cfg.search.per_client as usize,
        max_candidates: cfg.search.max_candidates as usize,
        max_results: cfg.search.max_results as usize,
    };
    let search = kdx_api::search::SearchCtx::new(settings, Some(kdx_search::search_dir(data_dir)));
    let api = Api::new(store, cfg.api.settings(&cfg.network)?, tips)
        .docs_enabled(cfg.api.docs_enabled)
        .native_routes(kdx_api::native::router(search.clone()))
        .compat_routes(kdx_api::compat::router(search.clone()));
    api.state()
        .set_key_modules(cfg.balances.key_modules.clone());
    // GraphQL (spec §5.3) on `api.graphql`; subscriptions come from the committed-block feed
    // (GQ-6), which polls the store, so `serve` and replicas have them as `run` does.
    let gs = cfg.graphql.settings(&cfg.network);
    let feed = kdx_graphql::BlockFeed::spawn(api.state(), gs.feed_poll, gs.feed_ring_bytes);
    let pact = pact_pass(cfg, &gs, &api.state())?;
    if let Some(p) = &pact {
        // B1: a request naming the passthrough's fields may run its timeout past `deadline`.
        api.state()
            .set_graphql_extra_deadline(p.settings().timeout + Duration::from_secs(1));
    }
    let schema =
        kdx_graphql::build_schema_full(Some(api.state()), gs, Some(feed), Some(search), pact);
    Ok(api.graphql_routes(kdx_graphql::router(schema)))
}

/// The `pactQuery` / `gasLimitEstimate` passthrough (B1): its own node client on `node.url`
/// and `network` (never a client-given URL), with the passthrough's concurrency, rate and
/// body cap, so it neither shares nor drains ingest's node budget.
fn pact_pass(
    cfg: &Config,
    gs: &kdx_graphql::GraphqlSettings,
    st: &kdx_api::ApiState,
) -> anyhow::Result<Option<Arc<kdx_graphql::pact::PactPass>>> {
    let p = &cfg.graphql.pact;
    if !p.enabled {
        return Ok(None);
    }
    // M4: the network's chains from the store's chain graph when it has one (the node's own
    // graph), else the configured network's count.
    let mut settings = gs.pact.clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    if let Ok(view) = st.store.view(deadline) {
        if let Some(n) = view.genesis.chains().map(|(c, _)| c).max() {
            settings.chains = n.saturating_add(1);
        }
    }
    let mut c = cfg.clone();
    c.ingest.max_rps = p.node_rps;
    c.ingest.node_concurrency = p.max_in_flight.max(1) as usize;
    c.node.max_body_bytes = p.max_response_bytes as usize;
    c.node.timeout_ms = p.timeout_ms;
    let node = super::node_client(&c).context("graphql.pact node client")?;
    Ok(Some(Arc::new(kdx_graphql::pact::PactPass::new(
        node, settings,
    ))))
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
    spawn_swap_watch(store.clone(), token.clone());
    crate::alloc::spawn_stats(token.clone());
    // `serve` has no ingest: the watchdog watches the API runtime only.
    super::notify::spawn_watchdog(&[kdx_types::heartbeat::Component::Api]);
    super::notify::spawn_api_heartbeat(token.clone());
    super::notify::spawn_status(token.clone(), || "serving (read-only)".into());
    tracing::info!(data_dir = %data_dir.display(), "serve: read-only API");
    api(cfg, store, tips, &data_dir)?
        .on_bound(|_| super::notify::ready("serving (read-only)"))
        .serve(cfg.api.binds()?, token)
        .await
}
