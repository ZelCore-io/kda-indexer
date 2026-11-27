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
use kdx_types::heartbeat::{self, Component, Source};
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
        super::notify::stopping();
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
    // A background writer whose dictionary reload failed (F-52) stops the process.
    kdx_ingest::ctx::set_process_stop(token.clone());
    super::notify::spawn_watchdog(&[Component::Ingest, Component::Api]);
    super::notify::spawn_api_heartbeat(token.clone());
    heartbeat::beat(Component::Ingest);

    let node = super::node_client(cfg)?;
    node.set_batch_limit(cfg.ingest.batch_size);
    // One MDBX environment for the whole process (backfill, tail, freezer, API).
    let o = opts(cfg, &data_dir, a.until);
    let hot = Arc::new(kdx_hot::HotStore::open(
        &kdx_ingest::ctx::hot_dir(&data_dir),
        o.geometry,
    )?);
    // PR #48 review I-1: clear the stored-counts marker before serving when a build without
    // stored counts moved the tips (logged with the chain and heights).
    kdx_ingest::row_counts::startup_guard(&hot)?;
    kdx_ingest::holders_fill::startup_guard(&hot)?;
    let ctx = Arc::new(Ctx::open_with(o, node, hot.clone()).await?);
    // The API binds before backfill: admin and status answer while it runs, data routes
    // serve what is committed. It stops with the process (token).
    let tips = kdx_api::TipWatch::default();
    let api = {
        let store = Arc::new(kdx_api::Store::open(hot.clone(), &data_dir)?);
        // Gap Q: rows the GraphQL miss read finds on the node are marked for the reconciler.
        let (miss_tx, miss_rx) = kdx_ingest::miss_feed::channel();
        kdx_ingest::miss_feed::spawn(ctx.clone(), miss_rx, token.clone());
        let sink: kdx_graphql::miss::FoundSink = Arc::new(move |r: kdx_graphql::miss::FoundRow| {
            let _ = miss_tx.try_send((r.account, r.module, r.chain, r.height));
        });
        let api = super::serve::api(cfg, store, tips.clone(), &data_dir, Some(sink))?
            .on_bound(|_| super::notify::ready("api bound, starting"));
        let binds = cfg.api.binds()?;
        let stop = token.clone();
        super::serve::spawn_tip_poll(cfg, tips.clone(), token.clone());
        super::serve::spawn_textfile(cfg, token.clone());
        crate::alloc::spawn_stats(token.clone());
        let (t, range) = (tips.clone(), cfg.backfill.range_heights.max(1));
        super::notify::spawn_status(token.clone(), move || status_text(&t, range));
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
        let bctx =
            Arc::new(Ctx::open_with(opts(cfg, &data_dir, a.until), bnode, hot.clone()).await?);
        let to = u32::try_from(final_tip)?;
        let bf = kdx_ingest::backfill::run(bctx, u32::try_from(bf_from)?, to, a.header_mode);
        tips.set_backfill(true);
        let Some((s, api)) = backfill_with_api(bf, &token, api).await? else {
            return Ok(());
        };
        tracing::info!(summary = %serde_json::to_string(&s)?, "run: backfill done");
        tips.set_backfill(false);
        let node = super::node_client(cfg)?;
        node.set_batch_limit(cfg.ingest.batch_size);
        let ctx = Arc::new(Ctx::open_with(opts(cfg, &data_dir, a.until), node, hot).await?);
        let r = follow(
            ctx,
            token.clone(),
            a.freeze,
            search_opts(cfg, &data_dir)?,
            balances_opts(cfg)?,
            cfg.dex_usd.enabled.then(|| cfg.dex_usd.settings()),
        )
        .await;
        return stop_api(api, token, r).await;
    }
    let r = follow(
        ctx,
        token.clone(),
        a.freeze,
        search_opts(cfg, &data_dir)?,
        balances_opts(cfg)?,
        cfg.dex_usd.enabled.then(|| cfg.dex_usd.settings()),
    )
    .await;
    stop_api(api, token, r).await
}

/// Stage 8 (balances, B-5) from `[balances]`: its own node client and options, or None when
/// disabled.
fn balances_opts(
    cfg: &Config,
) -> anyhow::Result<Option<(kdx_node::NodeClient, kdx_ingest::reconcile::ReconcileOpts)>> {
    let b = &cfg.balances;
    if !b.enabled {
        return Ok(None);
    }
    let o = kdx_ingest::reconcile::ReconcileOpts {
        local: kdx_node::LocalOpts {
            rps: b.local_rps,
            concurrency: b.local_concurrency.max(1) as usize,
        },
        // Room for every read node's calls in flight once keys go `local_batch_keys` a call,
        // capped at 1,024 rows: one write txn per batch, committed after its slowest call,
        // and the tail, the receivers pass and the tip share wait behind it (PR #67 I5).
        batch: (b.local_batch_keys as usize * 80).clamp(256, 1024),
        seed: true,
        caps: kdx_ingest::reconcile::Caps {
            module_share: b.module_share as usize,
            module_rows: b.module_rows,
            fungible_module_rows: b.fungible_module_rows,
            account_rows: b.account_rows,
            recheck_secs: b.recheck_hours.saturating_mul(3600),
            history_slice_txs: kdx_ingest::seed_receivers::HISTORY_SLICE_TXS,
            history_walk_threads: b.history_walk_threads as usize,
            refill_window_batches: b.refill_window_batches as usize,
        },
        keylist_skip: b.keylist_skip.clone(),
        batch_keys: b.local_batch_keys as usize,
        read_nodes: read_nodes(cfg)?,
    };
    Ok(Some((super::local_node_client(cfg)?, o)))
}

/// `balances.local_urls` as read nodes `n0`, `n1`, …: `node.url` shares the reconciler's
/// client (`None`); every other URL gets a client of its own with the same `/local` caps.
fn read_nodes(cfg: &Config) -> anyhow::Result<kdx_ingest::reconcile::ReadNodes> {
    let mut v = Vec::new();
    for (i, u) in cfg.balances.local_urls.iter().enumerate() {
        let node = if *u == cfg.node.url {
            None
        } else {
            let mut c = cfg.clone();
            c.node.url = u.clone();
            // The p2p settings name `node.url`'s peer endpoint; a read node needs none.
            c.node.p2p_url = None;
            c.node.p2p_cert_sha256 = None;
            Some(super::local_node_client(&c)?)
        };
        v.push((format!("n{i}"), node));
    }
    Ok(kdx_ingest::reconcile::ReadNodes(v))
}

/// Tail stage 7 settings from `[search]`.
fn search_opts(
    cfg: &Config,
    data_dir: &Path,
) -> anyhow::Result<kdx_ingest::search_stage::SearchStageOpts> {
    let s = &cfg.search;
    Ok(kdx_ingest::search_stage::SearchStageOpts {
        data_dir: data_dir.to_path_buf(),
        mode: s.mode_sel()?,
        commit_secs: s.commit_secs,
        build_threads: s.build_threads as usize,
        heap_mb: s.heap_mb as usize,
        rebuild_every_secs: if s.periodic_rebuild {
            u64::from(s.rebuild_days) * 86_400
        } else {
            0
        },
        rebuild_window_heights: if s.periodic_rebuild {
            s.rebuild_window_heights
        } else {
            0
        },
    })
}

/// `STATUS=` text: backfill progress as range r of n, else the tail's largest lag; a search
/// rebuild running in this process adds its ranges done.
fn status_text(tips: &kdx_api::TipWatch, range_heights: u64) -> String {
    let base = ingest_status(tips, range_heights);
    match kdx_search::progress() {
        Some((d, t)) => format!("{base}; search rebuild {d}/{t}"),
        None => base,
    }
}

fn ingest_status(tips: &kdx_api::TipWatch, range_heights: u64) -> String {
    let m = kdx_metrics::get();
    if tips.backfill() {
        let next = m.backfill_next_height.get().max(0) as u64;
        let end = m.backfill_end_height.get().max(0) as u64;
        format!(
            "backfill r{}/{}",
            (next / range_heights + 1).min(end.div_ceil(range_heights).max(1)),
            end.div_ceil(range_heights).max(1)
        )
    } else {
        match m.max_chain_lag() {
            Some(l) => format!("tail lag {l}"),
            None => "tail".into(),
        }
    }
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

/// Runs the backfill `bf` while the API task `api` serves. `Some`: the backfill finished and
/// the API still serves. `None`: stopped by a signal during the backfill. F-18: when the run
/// ends here the API task is joined first, so an API that failed (it cancels the token: a
/// bind or serve error) makes the run fail and systemd (`Restart=on-failure`) restarts it.
async fn backfill_with_api<T>(
    bf: impl std::future::Future<Output = anyhow::Result<T>>,
    token: &CancellationToken,
    api: tokio::task::JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<Option<(T, tokio::task::JoinHandle<anyhow::Result<()>>)>> {
    tokio::select! {
        r = bf => match r {
            Ok(s) => Ok(Some((s, api))),
            Err(e) => stop_api(api, token.clone(), Err(e)).await.map(|()| None),
        },
        _ = token.cancelled() => {
            tracing::warn!("run: stopped during backfill (the next start resumes at the manifest frontier)");
            stop_api(api, token.clone(), Ok(())).await.map(|()| None)
        }
    }
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
    // A store from height h has no all-time per-chain counters: `countersOfEachChain`
    // answers NOT_AVAILABLE rather than counts that miss heights below h.
    kdx_hot::chaincount::clear_ready(&mut txn)?;
    // No history below `h`: the holders table (exact only from block 0) is not served.
    kdx_hot::holders::clear_ready(&mut txn)?;
    txn.set_meta(&dict::lens_key(h), &dict::encode_lens(&d))?;
    txn.commit()?;
    tracing::info!(
        start = h,
        "run: empty store starts at --start-height (no history below)"
    );
    Ok(())
}

async fn follow(
    ctx: Arc<Ctx>,
    token: CancellationToken,
    freeze: bool,
    search: kdx_ingest::search_stage::SearchStageOpts,
    balances: Option<(kdx_node::NodeClient, kdx_ingest::reconcile::ReconcileOpts)>,
    dia: Option<kdx_ingest::dia::DiaSettings>,
) -> anyhow::Result<()> {
    {
        let c = ctx.clone();
        tokio::task::spawn_blocking(move || freezer::reconcile(&c)).await??;
    }
    // From here the tail (and the freezer) are the ingest sources the watchdog watches.
    heartbeat::beat_source(Source::Tail);
    heartbeat::retire(Source::Run);
    let tail_stop = token.child_token();
    let mut tail = tokio::spawn(kdx_ingest::tail::run(ctx.clone(), tail_stop.clone()));
    // Tail stage 7: code search docs (rebuilds the index first when there is none). Its
    // failures are logged and retried inside; it stops with the process.
    let search_stop = token.child_token();
    let search = tokio::spawn(kdx_ingest::search_stage::run(
        ctx.clone(),
        search,
        search_stop.clone(),
    ));
    // One-off fills (each resumable; new blocks keep their tables themselves): the swap price
    // history (`lastTokenPriceInKda`), the Sushi DEX ledger (stock-gap B2) and the token chain
    // list (`tokens`). A failure is recorded and retried with backoff ([`supervise_fill`]);
    // ingest goes on without them.
    let price_stop = token.child_token();
    tokio::spawn(supervise_fill(
        ctx.clone(),
        kdx_hot::fills::PRICES,
        price_stop.clone(),
        price_fill,
    ));
    tokio::spawn(supervise_fill(
        ctx.clone(),
        kdx_hot::fills::DEX,
        price_stop.clone(),
        dex_fill,
    ));
    tokio::spawn(supervise_fill(
        ctx.clone(),
        kdx_hot::fills::TOKEN_CHAINS,
        price_stop.clone(),
        token_chains_fill,
    ));
    // DEX USD feed (B3): DIA's KDA/USD rate, polled into `DexUsd` when `[dex_usd]` is on.
    // Failures are logged and keep the last good rate; ingest never waits on it.
    if let Some(d) = dia {
        tokio::spawn(kdx_ingest::dia::run(ctx.hot.clone(), d, price_stop.clone()));
    }
    // Stage 8: balance seed (when absent) and `/local` reconciler (B-5). Its errors are
    // logged; ingest goes on without it.
    let bal_stop = token.child_token();
    let bal = balances.map(|(node, o)| {
        tokio::spawn(kdx_ingest::reconcile::supervise(
            ctx.clone(),
            node,
            o,
            bal_stop.clone(),
        ))
    });
    let fz_stop = token.child_token();
    let fz = freeze.then(|| {
        let ctx = ctx.clone();
        let stop = fz_stop.clone();
        tokio::spawn(async move {
            while !stop.is_cancelled() {
                heartbeat::beat_source(Source::Freezer);
                let c = ctx.clone();
                // One step or merge at a time; a running one always finishes.
                match tokio::task::spawn_blocking(move || {
                    heartbeat::as_source(Source::Freezer, || freezer::tick(&c))
                })
                .await?
                {
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
    price_stop.cancel();
    search_stop.cancel();
    if let Err(e) = search.await.map_err(anyhow::Error::from).and_then(|x| x) {
        tracing::error!(error = format!("{e:#}"), "search stage");
    }
    bal_stop.cancel();
    if let Some(b) = bal {
        if let Err(e) = b.await.map_err(anyhow::Error::from).and_then(|x| x) {
            tracing::error!(error = format!("{e:#}"), "balances reconciler");
        }
    }
    let txn = ctx.hot.ro()?;
    tracing::info!(
        frontier = txn.frozen_frontier()?,
        hot_blocks = txn.count(Table::Headers)?,
        reorgs = txn.counter(kdx_ingest::apply::counters::REORGS)?,
        reorg_blocks = txn.counter(kdx_ingest::apply::counters::REORG_BLOCKS)?,
        reorg_depth_max = txn.counter(kdx_ingest::apply::counters::REORG_DEPTH_MAX)?,
        "run: stopped"
    );
    // F-52: a writer that stopped the process after a failed dictionary reload cancelled the
    // token like a signal; the run still fails, so systemd (`Restart=on-failure`) restarts it.
    kdx_ingest::ctx::fail_on_reload_failure(res, kdx_ingest::ctx::reload_failure())
}

/// First wait after a one-off fill fails; it doubles per failure up to [`FILL_RETRY_MAX`].
const FILL_RETRY_FIRST: Duration = Duration::from_secs(30);
const FILL_RETRY_MAX: Duration = Duration::from_secs(3_600);
/// Longest one attempt of a one-off fill may run before it is told to stop and counted as a
/// failure (`kdx_fill_timeouts`). Generous: a full mainnet history fill takes hours, and
/// each fill resumes where the stopped attempt left it.
const FILL_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(24 * 3_600);

/// Run a one-off fill until it succeeds or `stop`. A failure (an error or a panic) is logged,
/// counted (`kdx_fill_failures`, `kdx_fill_failed` = 1), recorded in `Meta fill_failed/<name>`
/// (readers then answer "fill failed" rather than "being filled") and retried after
/// [`FILL_RETRY_FIRST`], doubling to [`FILL_RETRY_MAX`]: a transient error heals without a
/// restart, and a deterministic one stays visible instead of leaving the gate shut silently.
/// An attempt that runs past [`FILL_ATTEMPT_TIMEOUT`] is logged and counted at once, told to
/// stop (its own stop flag), and, once it has returned, recorded as failed and retried. A fill
/// stuck where it never checks its stop flag is reported but cannot be replaced (a blocking
/// thread cannot be killed, and two attempts must not run at once).
async fn supervise_fill(
    ctx: Arc<Ctx>,
    name: &'static str,
    stop: CancellationToken,
    f: fn(&Ctx, &dyn Fn() -> bool) -> anyhow::Result<()>,
) {
    let mut wait = FILL_RETRY_FIRST;
    loop {
        let attempt = stop.child_token();
        let (c, s) = (ctx.clone(), attempt.clone());
        let mut h = tokio::task::spawn_blocking(move || f(&c, &|| s.is_cancelled()));
        let joined = match tokio::time::timeout(FILL_ATTEMPT_TIMEOUT, &mut h).await {
            Ok(r) => Some(r),
            Err(_) => {
                kdx_metrics::get().fill_timeout(name);
                tracing::error!(
                    fill = name,
                    timeout_secs = FILL_ATTEMPT_TIMEOUT.as_secs(),
                    "fill attempt timed out; stopping it"
                );
                attempt.cancel();
                // Whatever it returns after the stop (an early Ok included) is a failure.
                let _ = h.await;
                None
            }
        };
        let err = match joined {
            None => Some(format!(
                "attempt timed out after {} s",
                FILL_ATTEMPT_TIMEOUT.as_secs()
            )),
            Some(Ok(Ok(()))) => None,
            Some(Ok(Err(e))) => Some(format!("{e:#}")),
            Some(Err(e)) => Some(format!("fill task: {e}")),
        };
        let ok = err.is_none();
        if !ok && stop.is_cancelled() {
            return;
        }
        kdx_metrics::get().fill(name, ok);
        let rec = {
            let (c, e) = (ctx.clone(), err.clone());
            tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                let mut w = c.hot.rw()?;
                match &e {
                    Some(e) => kdx_hot::fills::set_failed(&mut w, name, e)?,
                    None => kdx_hot::fills::clear_failed(&mut w, name)?,
                }
                w.commit()
            })
            .await
        };
        if let Ok(Err(e)) | Err(e) = rec.map_err(anyhow::Error::from) {
            tracing::warn!(
                fill = name,
                error = format!("{e:#}"),
                "fill status not stored"
            );
        }
        let Some(e) = err else { return };
        tracing::error!(
            fill = name,
            error = e,
            retry_secs = wait.as_secs(),
            "fill failed; retrying"
        );
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(wait) => {}
        }
        wait = (wait * 2).min(FILL_RETRY_MAX);
    }
}

fn price_fill(c: &Ctx, stop: &dyn Fn() -> bool) -> anyhow::Result<()> {
    if kdx_ingest::price_fill::filled(&c.hot.ro()?)? {
        return Ok(());
    }
    tracing::info!("prices: swap history fill");
    let rep = kdx_ingest::price_fill::run(&c.hot, &c.segdir, &stop)?;
    tracing::info!(report = %serde_json::to_string(&rep)?, "prices: fill pass ended");
    Ok(())
}

fn dex_fill(c: &Ctx, stop: &dyn Fn() -> bool) -> anyhow::Result<()> {
    // Derived rows of another layout are cleared here and rebuilt by the fill.
    let mut w = c.hot.rw()?;
    if kdx_hot::dex::ensure_layout(&mut w)? {
        tracing::info!(
            layout = kdx_hot::dex::LAYOUT,
            "dex: derived rows cleared for a new layout; the fill rebuilds them"
        );
    }
    w.commit()?;
    if kdx_ingest::dex_fill::filled(&c.hot.ro()?)? {
        return Ok(());
    }
    tracing::info!("dex: Sushi history fill");
    let rep = kdx_ingest::dex_fill::run(&c.hot, &c.segdir, &stop)?;
    tracing::info!(report = %serde_json::to_string(&rep)?, "dex: fill pass ended");
    Ok(())
}

/// The token chain list of a store an older build wrote (`kdx_hot::token_chains`): built
/// from `Holders` in one write txn (one seek per module and chain), no manual step.
fn token_chains_fill(c: &Ctx, stop: &dyn Fn() -> bool) -> anyhow::Result<()> {
    if stop() || kdx_hot::token_chains::marker_set(&c.hot.ro()?)? {
        return Ok(());
    }
    let mut w = c.hot.rw()?;
    let n = kdx_hot::token_chains::fill(&mut w, stop)?;
    w.commit()?;
    tracing::info!(
        module_chains = n,
        "tokens: chain list built from the holder rows"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use tokio_util::sync::CancellationToken;

    use super::backfill_with_api;

    /// F-18: the API fails (say, its address is taken) while the backfill runs. The API task
    /// cancels the token; `run` must end with that error, so systemd restarts it.
    #[tokio::test]
    async fn an_api_failure_during_backfill_is_an_error() {
        let token = CancellationToken::new();
        let stop = token.clone();
        let api = tokio::spawn(async move {
            stop.cancel();
            Err(anyhow::anyhow!("bind 127.0.0.1:1: address in use"))
        });
        let r = backfill_with_api(pending::<anyhow::Result<()>>(), &token, api).await;
        let e = r.expect_err("an API failure during backfill must fail the run");
        assert!(format!("{e:#}").contains("address in use"), "{e:#}");
    }

    /// A signal during the backfill stops the run cleanly once the API has stopped.
    #[tokio::test]
    async fn a_signal_during_backfill_is_a_clean_stop() {
        let token = CancellationToken::new();
        let stop = token.clone();
        let api = tokio::spawn(async move {
            stop.cancelled().await;
            Ok(())
        });
        token.cancel();
        let r = backfill_with_api(pending::<anyhow::Result<()>>(), &token, api).await;
        assert!(r.unwrap().is_none());
    }

    /// F-52: a writer whose dictionary reload failed cancels the process token, which stops
    /// the run as a signal does; the recorded failure still makes `run` end with an error, so
    /// systemd restarts it.
    #[tokio::test]
    async fn a_reload_failure_stop_is_an_error() {
        let token = CancellationToken::new();
        let stop = token.clone();
        let api = tokio::spawn(async move {
            stop.cancelled().await;
            Ok(())
        });
        // The writer: its reload failed, so it stops the process.
        let e = anyhow::Error::new(kdx_ingest::ctx::DictReloadFailed::new(anyhow::anyhow!(
            "no readers"
        )));
        assert!(kdx_ingest::ctx::stop_on_reload_failure(&e, &token));
        let r = backfill_with_api(pending::<anyhow::Result<()>>(), &token, api).await;
        assert!(r.unwrap().is_none(), "the stop itself is clean");
        let failed = kdx_ingest::ctx::reload_failure();
        assert!(failed.is_some(), "the failure is recorded");
        let e = kdx_ingest::ctx::fail_on_reload_failure(Ok(()), failed)
            .expect_err("a stop after a failed reload must fail the run");
        assert!(format!("{e:#}").contains("no readers"), "{e:#}");
    }

    /// A backfill error is returned after the API task has been joined.
    #[tokio::test]
    async fn a_backfill_error_joins_the_api() {
        let token = CancellationToken::new();
        let stop = token.clone();
        let joined = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let j = joined.clone();
        let api = tokio::spawn(async move {
            stop.cancelled().await;
            j.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        });
        let bf = async { Err::<(), _>(anyhow::anyhow!("node gone")) };
        let e = backfill_with_api(bf, &token, api).await.unwrap_err();
        assert!(format!("{e:#}").contains("node gone"));
        assert!(
            joined.load(std::sync::atomic::Ordering::SeqCst),
            "API not joined"
        );
    }
}
