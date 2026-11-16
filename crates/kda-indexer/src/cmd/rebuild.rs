//! `rebuild-range --range S | --step S [--dry-run]` (T-4): refetch one frozen range or step
//! from the node, compare it with the stored one (record counts, content digest, blake3 of
//! every file) and swap it in crash-safely. See `kdx_ingest::rebuild`.

use std::path::Path;
use std::sync::Arc;

use kdx_ingest::{Ctx, HeaderMode};
use kdx_segment::RangeKind;

use crate::config::Config;

pub async fn run(
    cfg: &Config,
    data_dir: &Path,
    kind: RangeKind,
    start: u32,
    mode: HeaderMode,
    dry_run: bool,
) -> anyhow::Result<()> {
    let _lock = super::lock_data_dir(data_dir)?;
    let mut ncfg = cfg.clone();
    ncfg.ingest.max_rps = cfg.backfill_max_rps();
    let node = super::node_client(&ncfg)?;
    node.set_batch_limit(cfg.ingest.batch_size);
    let ctx = Arc::new(Ctx::open(super::backfill::opts(cfg, data_dir), node).await?);
    tracing::info!(data_dir = %data_dir.display(), kind = kind.name(), start, dry_run, "rebuild-range");
    let rep = kdx_ingest::rebuild::run(ctx, kind, start, mode, dry_run).await?;
    println!("{}", serde_json::to_string_pretty(&rep)?);
    Ok(())
}
