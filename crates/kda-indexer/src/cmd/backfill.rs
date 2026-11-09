//! `backfill --from H --to H` (spec §6.8): canonical blocks into frozen segments, resumable.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use kdx_ingest::{Ctx, HeaderMode, IngestOpts};

use crate::config::Config;

pub fn opts(cfg: &Config, data_dir: &Path) -> IngestOpts {
    let b = &cfg.backfill;
    IngestOpts {
        network: cfg.network.clone(),
        data_dir: data_dir.to_path_buf(),
        geometry: super::hot::geometry(cfg),
        finality_depth: cfg.ingest.finality_depth,
        batch_size: cfg.ingest.batch_size,
        ranges_in_flight: b.ranges_in_flight,
        range_heights: b.range_heights,
        step_heights: b.step_heights,
        min_free_gb: b.min_free_gb,
        range_level: b.zstd_level,
        step_level: cfg.freezer.zstd_level,
        dict_samples: b.dict_samples,
        etl_mem_mb: b.etl_mem_mb,
        mem_cap_mb: b.mem_cap_mb,
        commit_ms: cfg.tail.commit_ms,
        commit_blocks: cfg.tail.commit_blocks,
        cut_poll_secs: cfg.tail.cut_poll_secs,
        until: None,
    }
}

pub async fn run(
    cfg: &Config,
    data_dir: &Path,
    from: &str,
    to: &str,
    mode: HeaderMode,
) -> anyhow::Result<()> {
    // Backfill's own request budget (`backfill.max_rps`, else `ingest.max_rps`).
    let mut ncfg = cfg.clone();
    ncfg.ingest.max_rps = cfg.backfill_max_rps();
    let node = super::node_client(&ncfg)?;
    node.set_batch_limit(cfg.ingest.batch_size);
    let cut = node.cut().await?;
    let tip = cut
        .hashes
        .values()
        .map(|e| e.height)
        .min()
        .map(|h| h.saturating_sub(cfg.ingest.finality_depth));
    let from = u32::try_from(super::dump::parse_height(from, tip)?).context("--from")?;
    let to = u32::try_from(super::dump::parse_height(to, tip)?).context("--to")?;
    anyhow::ensure!(from <= to, "--from {from} > --to {to}");
    tracing::info!(data_dir = %data_dir.display(), from, to, ?tip, "backfill");
    let ctx = Arc::new(Ctx::open(opts(cfg, data_dir), node).await?);
    let s = kdx_ingest::backfill::run(ctx, from, to, mode).await?;
    println!("{}", serde_json::to_string_pretty(&s)?);
    Ok(())
}
