//! `run` (spec §6.8): backfill what is final and not frozen yet, then the live tail plus the
//! freezer until SIGTERM/SIGINT (the current commit finishes, exit 0). The hidden `tail`
//! command is the same without the freezer (dev).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use kdx_hot::checkpoint::set_checkpoint;
use kdx_hot::{dict, Stage, Table, Txn};
use kdx_ingest::freezer::{self, META_PARTIAL_FROM};
use kdx_ingest::{Ctx, HeaderMode};
use tokio_util::sync::CancellationToken;

use crate::config::Config;

pub struct RunArgs {
    pub data_dir: PathBuf,
    /// Dev: start an empty store at this height (aligned down to a step), no backfill.
    pub start_height: Option<String>,
    /// Dev: followers stop at this height; `run` exits when all have.
    pub until: Option<u32>,
    /// `tail`: no freezer.
    pub freeze: bool,
    pub header_mode: HeaderMode,
}

fn signals(token: CancellationToken) -> anyhow::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    tokio::spawn(async move {
        tokio::select! {
            _ = term.recv() => tracing::warn!("SIGTERM: finishing the current commit, then exiting"),
            _ = int.recv() => tracing::warn!("SIGINT: finishing the current commit, then exiting"),
        }
        token.cancel();
    });
    Ok(())
}

fn resolve_height(s: &str, cut_min: u64) -> anyhow::Result<u64> {
    super::dump::parse_height(s, Some(cut_min))
}

fn opts(cfg: &Config, data_dir: &Path, until: Option<u32>) -> kdx_ingest::IngestOpts {
    let mut o = super::backfill::opts(cfg, data_dir);
    o.until = until;
    o
}

pub async fn run(cfg: &Config, a: RunArgs) -> anyhow::Result<()> {
    std::fs::create_dir_all(&a.data_dir)
        .with_context(|| format!("create {}", a.data_dir.display()))?;
    let data_dir = std::fs::canonicalize(&a.data_dir)?;
    let _lock = super::lock_data_dir(&data_dir)?;
    let free_gb = kdx_ingest::disk::free_bytes(&data_dir)? >> 30;
    tracing::info!(data_dir = %data_dir.display(), free_gb, min_free_gb = cfg.backfill.min_free_gb, "run: data dir");
    kdx_ingest::disk::ensure_free(&data_dir, cfg.backfill.min_free_gb)?;
    let token = CancellationToken::new();
    signals(token.clone())?;

    let node = super::node_client(cfg)?;
    node.set_batch_limit(cfg.ingest.batch_size);
    // One MDBX environment for the whole process (backfill, tail, freezer, API).
    let o = opts(cfg, &data_dir, a.until);
    let hot = Arc::new(kdx_hot::HotStore::open(
        &kdx_ingest::ctx::hot_dir(&data_dir),
        o.geometry,
    )?);
    let ctx = Arc::new(Ctx::open_with(o, node, hot.clone()).await?);
    // The API binds before backfill: admin and status answer while it runs, data routes
    // serve what is committed. It stops with the process (token).
    let tips = kdx_api::TipWatch::default();
    let api = {
        let store = Arc::new(kdx_api::Store::open(hot.clone(), &data_dir)?);
        let api = super::serve::api(cfg, store, tips.clone())?;
        let binds = cfg.api.binds()?;
        let stop = token.clone();
        super::serve::spawn_tip_poll(cfg, tips.clone(), token.clone());
        super::serve::spawn_textfile(cfg, token.clone());
        tokio::spawn(async move {
            let r = api.serve(binds, stop.clone()).await;
            if let Err(e) = &r {
                // An API that cannot bind stops the process (fail fast under systemd).
                tracing::error!(error = format!("{e:#}"), "run: API failed, stopping");
                stop.cancel();
            }
            r
        })
    };
    let cut = ctx.node.cut().await?;
    let cut_min = cut
        .hashes
        .values()
        .map(|e| e.height)
        .min()
        .context("empty cut")?;
    let f = cfg.ingest.finality_depth;
    let step = cfg.backfill.step_heights;

    // Read-only look at the store.
    let (frontier, hot_blocks, partial, has_cp, manifest_empty) = {
        let txn = ctx.hot.ro()?;
        let mut has_cp = false;
        for c in 0..kdx_segment::SLOT_WIDTH as u8 {
            has_cp |= kdx_hot::checkpoint(&txn, Stage::Headers, c)?.is_some()
                || kdx_hot::checkpoint(&txn, Stage::Tail, c)?.is_some();
        }
        let m = kdx_segment::Manifest::load(&ctx.segdir)?;
        (
            txn.frozen_frontier()?,
            txn.count(Table::Headers)?,
            txn.meta(META_PARTIAL_FROM)?
                .and_then(|v| v.try_into().ok().map(u32::from_be_bytes)),
            has_cp,
            m.is_none_or(|m| m.entries.is_empty()),
        )
    };
    let empty = manifest_empty && frontier == 0 && hot_blocks == 0 && !has_cp;

    let start = match &a.start_height {
        Some(s) if empty => {
            let h = resolve_height(s, cut_min)?;
            let h = h - h % step;
            anyhow::ensure!(
                h >= step,
                "--start-height must be at least one step ({step})"
            );
            Some(u32::try_from(h)?)
        }
        Some(_) if partial.is_some() => {
            tracing::info!(
                partial_from = partial,
                "run: --start-height ignored, the store already started there"
            );
            None
        }
        Some(_) => bail!(
            "--start-height needs an empty store ({} is not)",
            data_dir.display()
        ),
        None => None,
    };
    // Backfill only when no tail data exists (the tail otherwise catches up itself).
    let final_tip = cut_min.saturating_sub(f);
    let do_backfill = start.is_none()
        && hot_blocks == 0
        && !(partial.is_some() && manifest_empty)
        && u64::from(frontier) + step <= final_tip + 1;
    let bf_from = u64::from(frontier);
    let bf_end = if do_backfill {
        (final_tip + 1) - (final_tip + 1) % step
    } else {
        bf_from
    };
    let backfill_blocks: u64 = (bf_from..bf_end)
        .map(|h| ctx.graph.chains_at(h).len() as u64)
        .sum();
    let tail_from = start
        .map(u64::from)
        .unwrap_or(bf_end.max(u64::from(frontier)));
    let tail_blocks: u64 = cut
        .hashes
        .values()
        .map(|e| (e.height + 1).saturating_sub(tail_from))
        .sum();
    tracing::info!(
        data_dir = %data_dir.display(),
        network = %cfg.network,
        frontier,
        hot_blocks,
        cut_min,
        start_height = start,
        backfill = do_backfill,
        backfill_from = bf_from,
        backfill_end = bf_end,
        backfill_blocks,
        tail_catch_up_blocks = tail_blocks,
        planned_blocks = backfill_blocks + tail_blocks,
        freezer = a.freeze,
        "run: plan"
    );

    if let Some(h) = start {
        init_partial(&ctx, &cut, h).await?;
    }

    if do_backfill {
        // Backfill with its own request budget, then reopen for the tail.
        drop(ctx);
        let mut bcfg = cfg.clone();
        bcfg.ingest.max_rps = cfg.backfill_max_rps();
        let bnode = super::node_client(&bcfg)?;
        bnode.set_batch_limit(cfg.ingest.batch_size);
        let bctx = Arc::new(Ctx::open_with(opts(cfg, &data_dir, a.until), bnode, hot.clone()).await?);
        let to = u32::try_from(final_tip)?;
        let bf = kdx_ingest::backfill::run(bctx, u32::try_from(bf_from)?, to, a.header_mode);
        tips.set_backfill(true);
        tokio::select! {
            r = bf => {
                let s = r?;
                tracing::info!(summary = %serde_json::to_string(&s)?, "run: backfill done");
            }
            _ = token.cancelled() => {
                tracing::warn!("run: stopped during backfill (the next start resumes at the manifest frontier)");
                return Ok(());
            }
        }
        tips.set_backfill(false);
        let node = super::node_client(cfg)?;
        node.set_batch_limit(cfg.ingest.batch_size);
        let ctx = Arc::new(Ctx::open_with(opts(cfg, &data_dir, a.until), node, hot).await?);
        let r = follow(ctx, token.clone(), a.freeze).await;
        return stop_api(api, token, r).await;
    }
    let r = follow(ctx, token.clone(), a.freeze).await;
    stop_api(api, token, r).await
}

/// Stop the API with the process; an API failure (bind) is reported after ingest's result.
async fn stop_api(
    api: tokio::task::JoinHandle<anyhow::Result<()>>,
    token: CancellationToken,
    r: anyhow::Result<()>,
) -> anyhow::Result<()> {
    token.cancel();
    let a = api.await.map_err(anyhow::Error::from).and_then(|x| x);
    r.and(a.context("api"))
}

/// Dev `--start-height`: the empty store starts at `h` (frontier, `Headers` checkpoints at
/// `h − 1` from the node's canonical chain, `Meta partial_from`).
async fn init_partial(ctx: &Ctx, cut: &kdx_node::Cut, h: u32) -> anyhow::Result<()> {
    let mut cps = Vec::new();
    for &c in ctx.graph.chains_at(u64::from(h)) {
        let e = cut
            .hashes
            .get(&c)
            .with_context(|| format!("cut has no chain {c}"))?;
        let hs = ctx
            .node
            .header_branch_all(c, &e.hash, u64::from(h - 1), u64::from(h - 1))
            .await?;
        let first = hs
            .first()
            .with_context(|| format!("chain {c}: the node returned no header at {}", h - 1))?;
        let hash = kdx_ingest::reorg::header_hash(first, "hash")?;
        cps.push((c, hash));
    }
    let d = ctx.dict.lock().unwrap_or_else(|e| e.into_inner());
    let mut txn = ctx.hot.rw()?;
    for (c, hash) in cps {
        set_checkpoint(&mut txn, Stage::Headers, c, h - 1, hash)?;
    }
    txn.set_frozen_frontier(h)?;
    txn.set_meta(META_PARTIAL_FROM, &h.to_be_bytes())?;
    txn.set_meta(&dict::lens_key(h), &dict::encode_lens(&d))?;
    txn.commit()?;
    tracing::info!(
        start = h,
        "run: empty store starts at --start-height (no history below)"
    );
    Ok(())
}

async fn follow(ctx: Arc<Ctx>, token: CancellationToken, freeze: bool) -> anyhow::Result<()> {
    {
        let c = ctx.clone();
        tokio::task::spawn_blocking(move || freezer::reconcile(&c)).await??;
    }
    let tail_stop = token.child_token();
    let mut tail = tokio::spawn(kdx_ingest::tail::run(ctx.clone(), tail_stop.clone()));
    let fz_stop = token.child_token();
    let fz = freeze.then(|| {
        let ctx = ctx.clone();
        let stop = fz_stop.clone();
        tokio::spawn(async move {
            while !stop.is_cancelled() {
                let c = ctx.clone();
                // One step or merge at a time; a running one always finishes.
                match tokio::task::spawn_blocking(move || freezer::tick(&c)).await? {
                    Ok(Some(_)) => continue,
                    Ok(None) => {}
                    Err(e) => return Err(e),
                }
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                }
            }
            Ok::<_, anyhow::Error>(())
        })
    });
    let res = match fz {
        Some(mut fz) => {
            tokio::select! {
                r = &mut fz => {
                    // The freezer failed: stop the tail, report the error.
                    tail_stop.cancel();
                    let _ = (&mut tail).await;
                    match r {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e.context("freezer")),
                        Err(e) => Err(e.into()),
                    }
                }
                r = &mut tail => {
                    let r = r.map_err(anyhow::Error::from).and_then(|x| x);
                    fz_stop.cancel();
                    let fr = fz.await;
                    r.and(fr.map_err(anyhow::Error::from).and_then(|x| x))
                }
            }
        }
        None => tail.await?,
    };
    let txn = ctx.hot.ro()?;
    tracing::info!(
        frontier = txn.frozen_frontier()?,
        hot_blocks = txn.count(Table::Headers)?,
        reorgs = txn.counter(kdx_ingest::apply::counters::REORGS)?,
        reorg_blocks = txn.counter(kdx_ingest::apply::counters::REORG_BLOCKS)?,
        reorg_depth_max = txn.counter(kdx_ingest::apply::counters::REORG_DEPTH_MAX)?,
        "run: stopped"
    );
    res
}
