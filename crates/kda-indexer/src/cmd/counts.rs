//! `kda-indexer reconcile-counts`: build the stored per-key row counts behind GraphQL
//! `totalCount` (PR #48 part 2; `kdx_ingest::row_counts`). Offline: it takes the data-dir
//! lock, so the service must be stopped. Idempotent: every run rebuilds the table from the
//! records and sets the readiness marker last. `--check` only counts and compares with the
//! stored table (read-only, no lock).

use std::path::Path;

use kdx_hot::HotStore;

use crate::config::Config;

pub fn run(
    cfg: &Config,
    data_dir: &Path,
    threads: Option<usize>,
    check: bool,
) -> anyhow::Result<()> {
    let _lock = if check {
        None
    } else {
        Some(super::lock_data_dir(data_dir)?)
    };
    let hot_dir = super::hot::hot_dir(data_dir);
    let hs = if check {
        HotStore::open_ro(&hot_dir)?
    } else {
        HotStore::open(&hot_dir, super::hot::geometry(cfg))?
    };
    let threads = threads
        .filter(|&t| t > 0)
        .or(Some(cfg.balances.history_walk_threads as usize).filter(|&t| t > 0))
        .unwrap_or_else(kdx_ingest::seed_receivers::default_history_threads);
    let segdir = super::segments::segments_dir(data_dir);
    tracing::info!(threads, check, "reconcile-counts: counting every record");
    let rep = kdx_ingest::row_counts::reconcile(&segdir, &hs, threads, check, &|| false)?;
    tracing::info!(?rep, "reconcile-counts: done");
    println!("{}", serde_json::to_string_pretty(&rep)?);
    if !check && !rep.chain_counters_ready {
        tracing::warn!(
            "reconcile-counts: the chain counters are written but not marked ready (a per-chain gas sum past u128 KDA, or a partial store); countersOfEachChain answers NOT_AVAILABLE"
        );
    }
    if !check && !rep.ready {
        anyhow::bail!(
            "{} records do not render: the counts are written but not marked ready (totalCount keeps walking)",
            rep.unrendered
        );
    }
    Ok(())
}

/// `kda-indexer reconcile-holders`: build the module holders table (`kdx_ingest::holders_fill`)
/// of a store built before it existed. Offline like `reconcile-counts`; `--check` compares.
pub fn run_holders(
    cfg: &Config,
    data_dir: &Path,
    threads: Option<usize>,
    check: bool,
) -> anyhow::Result<()> {
    let _lock = if check {
        None
    } else {
        Some(super::lock_data_dir(data_dir)?)
    };
    let hot_dir = super::hot::hot_dir(data_dir);
    let hs = if check {
        HotStore::open_ro(&hot_dir)?
    } else {
        HotStore::open(&hot_dir, super::hot::geometry(cfg))?
    };
    let threads = threads
        .filter(|&t| t > 0)
        .or(Some(cfg.balances.history_walk_threads as usize).filter(|&t| t > 0))
        .unwrap_or_else(kdx_ingest::seed_receivers::default_history_threads);
    let segdir = super::segments::segments_dir(data_dir);
    tracing::info!(threads, check, "reconcile-holders: reading every record");
    let rep = kdx_ingest::holders_fill::reconcile(&segdir, &hs, threads, check, &|| false)?;
    tracing::info!(?rep, "reconcile-holders: done");
    println!("{}", serde_json::to_string_pretty(&rep)?);
    if !check && !rep.ready {
        anyhow::bail!(
            "{} records do not render: the holders table is written but not marked ready",
            rep.unrendered
        );
    }
    Ok(())
}
