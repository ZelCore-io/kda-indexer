//! Live tail (spec §4.3): one follower per chain, triggered by `header/updates` (SSE,
//! reconnect with backoff) and by `/cut` every `cut_poll_secs` (gaps, SSE loss). A follower
//! fetches the branch from its stored tip to the newest known header (at most
//! `CATCHUP_HEIGHTS` per round), decodes it and sends it to the single writer, which commits
//! one MDBX txn per `commit_ms` / `commit_blocks`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures::StreamExt;
use kdx_hot::Txn;
use kdx_types::Hash32;
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::apply::{self, Batch};
use crate::ctx::Ctx;
use crate::fetch;
use crate::pipeline;
use crate::reorg::{self, ChainView, ReorgOutcome};

/// Heights fetched per follower round while catching up.
pub const CATCHUP_HEIGHTS: u32 = 500;
/// Writer queue depth (batches).
const QUEUE: usize = 64;

/// Newest known header of a chain: (height, hash b64url).
type Target = Option<(u64, String)>;

fn header_of(ev: &Value) -> Option<(u8, u64, String)> {
    let h = ev.get("header").unwrap_or(ev);
    Some((
        u8::try_from(h.get("chainId")?.as_u64()?).ok()?,
        h.get("height")?.as_u64()?,
        h.get("hash")?.as_str()?.to_owned(),
    ))
}

fn offer(tx: &watch::Sender<Target>, height: u64, hash: String) {
    tx.send_if_modified(|t| match t {
        Some((h, x)) if *h > height || (*h == height && *x == hash) => false,
        _ => {
            *t = Some((height, hash));
            true
        }
    });
}

async fn sse_task(
    ctx: Arc<Ctx>,
    targets: BTreeMap<u8, watch::Sender<Target>>,
    stop: CancellationToken,
) {
    let mut backoff = 1u64;
    while !stop.is_cancelled() {
        match ctx.node.header_updates().await {
            Ok(mut s) => {
                tracing::info!("tail: header/updates connected");
                backoff = 1;
                loop {
                    let ev = tokio::select! {
                        _ = stop.cancelled() => return,
                        ev = s.next() => ev,
                    };
                    match ev {
                        Some(Ok(v)) => {
                            if let Some((c, h, x)) = header_of(&v) {
                                if let Some(t) = targets.get(&c) {
                                    offer(t, h, x);
                                }
                            }
                        }
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "tail: header/updates error");
                            break;
                        }
                        None => {
                            tracing::warn!("tail: header/updates ended");
                            break;
                        }
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, backoff, "tail: header/updates connect failed"),
        }
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(backoff)) => {}
        }
        backoff = (backoff * 2).min(60);
    }
}

async fn cut_task(
    ctx: Arc<Ctx>,
    targets: BTreeMap<u8, watch::Sender<Target>>,
    stop: CancellationToken,
) {
    let every = Duration::from_secs(ctx.opts.cut_poll_secs.max(1));
    loop {
        match ctx.node.cut().await {
            Ok(cut) => {
                for (c, e) in cut.hashes {
                    if let Some(t) = targets.get(&c) {
                        offer(t, e.height, e.hash);
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "tail: /cut failed"),
        }
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(every) => {}
        }
    }
}

/// One chain's follower; returns when stopped, halted or past `until`.
async fn follower(
    ctx: Arc<Ctx>,
    mut view: ChainView,
    mut target: watch::Receiver<Target>,
    out: mpsc::Sender<Batch>,
    stop: CancellationToken,
    feed: Target,
) -> Result<()> {
    let chain = view.chain;
    let keep = u32::try_from(ctx.opts.finality_depth)? + 1;
    let until = ctx.opts.until;
    let mut backoff = 1u64;
    let mut feed = feed;
    loop {
        if until.is_some_and(|u| view.tip.0 >= u) {
            tracing::info!(chain, height = view.tip.0, "tail: follower reached --until");
            return Ok(());
        }
        let t = match feed.take() {
            Some(f) => Some(f),
            None => target.borrow_and_update().clone(),
        };
        let Some((th, thash)) = t else {
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                r = target.changed() => { if r.is_err() && stop.is_cancelled() { return Ok(()); } r?; continue; }
            }
        };
        // Only a header at or above the stored tip height moves the chain (a lower one is an
        // ancestor or a lighter fork; the node's next header settles it).
        if th < u64::from(view.tip.0) || thash == view.tip.1.to_b64url() {
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                r = target.changed() => { if r.is_err() && stop.is_cancelled() { return Ok(()); } r?; continue; }
            }
        }
        let mut max = u64::from(view.tip.0) + u64::from(CATCHUP_HEIGHTS);
        if let Some(u) = until {
            max = max.min(u64::from(u));
        }
        // `header/branch` returns nothing when `maxheight` is above the upper header's height
        // (seen live), so the bound is only sent when it cuts the branch.
        let max = (max < th).then_some(max);
        let step = async {
            let br = reorg::handle(&ctx, &view, &thash, max).await?;
            if br.headers.is_empty() {
                return Ok::<_, anyhow::Error>(None);
            }
            if let ReorgOutcome::ChainHalted { .. } = br.outcome {
                return Ok(Some((br.outcome, Vec::new(), Vec::new())));
            }
            let hs: Vec<(u32, Hash32)> = br
                .headers
                .iter()
                .map(|h| Ok((reorg::header_height(h)?, reorg::header_hash(h, "hash")?)))
                .collect::<Result<_>>()?;
            let bodies = fetch::bodies(&ctx.node, chain, br.headers).await?;
            let prepared = tokio::task::spawn_blocking(move || {
                use rayon::prelude::*;
                bodies
                    .par_iter()
                    .map(|(h, p)| {
                        let height = reorg::header_height(h)?;
                        pipeline::prepare_value(chain, height, h, p)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .await??;
            Ok(Some((br.outcome, hs, prepared)))
        };
        let res = tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            r = step => r,
        };
        match res {
            Ok(None) => {
                // The target is an ancestor of (or equal to) the stored tip.
                tokio::select! {
                    _ = stop.cancelled() => return Ok(()),
                    r = target.changed() => { if r.is_err() && stop.is_cancelled() { return Ok(()); } r?; }
                }
            }
            Ok(Some((ReorgOutcome::ChainHalted { fork_height, depth }, _, _))) => {
                tracing::error!(
                    chain,
                    fork_height,
                    depth,
                    tip = view.tip.0,
                    "tail: reorg deeper than finality_depth or below the frozen frontier; chain halted"
                );
                out.send(Batch {
                    chain,
                    fork: None,
                    blocks: Vec::new(),
                    halted: true,
                })
                .await
                .ok();
                return Ok(());
            }
            Ok(Some((outcome, hs, prepared))) => {
                backoff = 1;
                let fork = match outcome {
                    ReorgOutcome::Reorg(f) => {
                        tracing::warn!(
                            chain,
                            fork = f.height,
                            depth = f.depth,
                            old_tip = view.tip.0,
                            new_tip = hs.last().map(|x| x.0),
                            "tail: reorg"
                        );
                        Some(f)
                    }
                    _ => None,
                };
                view.advance(fork.map(|f| f.height), &hs, keep);
                if out
                    .send(Batch {
                        chain,
                        fork,
                        blocks: prepared,
                        halted: false,
                    })
                    .await
                    .is_err()
                {
                    return Ok(()); // writer gone (shutdown or error)
                }
            }
            Err(e) => {
                tracing::warn!(chain, error = %format!("{e:#}"), backoff, "tail: follower round failed");
                tokio::select! {
                    _ = stop.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_secs(backoff)) => {}
                }
                backoff = (backoff * 2).min(60);
            }
        }
    }
}

/// The single writer: buffers batches, commits per `commit_ms` / `commit_blocks`, drains the
/// queue on shutdown.
///
/// A block that cannot be indexed (`apply::PoisonBlock`) halts only its chain: the follower is
/// stopped, `halted.<chain>` is counted and the other chains' batches are committed again.
/// Any other error cancels `shutdown` (followers, feeders and the caller's freezer stop) and
/// is returned.
async fn writer(
    ctx: Arc<Ctx>,
    mut rx: mpsc::Receiver<Batch>,
    shutdown: CancellationToken,
    stops: BTreeMap<u8, CancellationToken>,
) -> Result<()> {
    let mut poisoned: BTreeSet<u8> = BTreeSet::new();
    let max_wait = Duration::from_millis(ctx.opts.commit_ms.max(1));
    let max_blocks = ctx.opts.commit_blocks.max(1) as usize;
    let mut buf: Vec<Batch> = Vec::new();
    let mut nblocks = 0usize;
    let mut first_at: Option<Instant> = None;
    let mut open = true;
    let mut last_log = Instant::now();
    let (mut tot_blocks, mut tot_reorgs) = (0u64, 0u32);
    while open || !buf.is_empty() {
        let deadline = first_at.map(|t| t + max_wait);
        let got = if !open {
            None
        } else if let Some(d) = deadline {
            tokio::time::timeout_at(d.into(), rx.recv()).await.ok()
        } else {
            // Idle tick: a quiet chain is not a hang. A writer stuck in a commit does not
            // reach this line, so the beats stop and the watchdog sees it.
            crate::heartbeat::beat_source(crate::heartbeat::Source::Tail);
            tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .ok()
        };
        match got {
            Some(Some(b)) if poisoned.contains(&b.chain) => continue,
            Some(Some(b)) => {
                nblocks += b.blocks.len();
                first_at.get_or_insert_with(Instant::now);
                buf.push(b);
                if nblocks < max_blocks {
                    continue;
                }
            }
            Some(None) => open = false,
            None => {}
        }
        if buf.is_empty() {
            continue;
        }
        let mut batches = std::mem::take(&mut buf);
        let c = ctx.clone();
        let st = loop {
            let err = match tokio::task::block_in_place(|| apply::commit(&c, &batches)) {
                Ok(st) => break st,
                Err(e) => e,
            };
            let Some(p) = err
                .downcast_ref::<apply::PoisonBlock>()
                .filter(|p| !poisoned.contains(&p.chain))
            else {
                shutdown.cancel();
                return Err(err.context("tail writer"));
            };
            let chain = p.chain;
            tracing::error!(chain, height = p.height, error = %err, "tail: block cannot be indexed; halting this chain (Counters halted.{chain})");
            poisoned.insert(chain);
            if let Some(t) = stops.get(&chain) {
                t.cancel();
            }
            batches.retain(|b| b.chain != chain);
            batches.push(Batch {
                chain,
                fork: None,
                blocks: Vec::new(),
                halted: true,
            });
        };
        nblocks = 0;
        first_at = None;
        tot_blocks += st.blocks;
        tot_reorgs += st.reorgs;
        tracing::debug!(
            blocks = st.blocks,
            txs = st.txs,
            reorgs = st.reorgs,
            secs = st.secs,
            "tail: commit"
        );
        if last_log.elapsed() > Duration::from_secs(60) {
            last_log = Instant::now();
            log_progress(&ctx, tot_blocks, tot_reorgs);
        }
    }
    log_progress(&ctx, tot_blocks, tot_reorgs);
    Ok(())
}

fn log_progress(ctx: &Ctx, blocks: u64, reorgs: u32) {
    let Ok(txn) = ctx.hot.ro() else { return };
    let tips: Vec<u32> = (0..kdx_segment::SLOT_WIDTH as u8)
        .filter_map(|c| {
            kdx_hot::checkpoint(&txn, kdx_hot::Stage::Tail, c)
                .ok()
                .flatten()
        })
        .map(|t| t.0)
        .collect();
    tracing::info!(
        blocks_applied = blocks,
        reorgs,
        min_tip = tips.iter().min(),
        max_tip = tips.iter().max(),
        frontier = txn.frozen_frontier().ok(),
        "tail: progress"
    );
}

/// Run the tail until `shutdown` (or until every follower has stopped: `--until`, halts).
pub async fn run(ctx: Arc<Ctx>, shutdown: CancellationToken) -> Result<()> {
    let keep = u32::try_from(ctx.opts.finality_depth)? + 1;
    let cut = ctx.node.cut().await.context("tail: /cut")?;
    let tip_h = cut.hashes.values().map(|e| e.height).min().unwrap_or(0);
    let chains: Vec<u8> = ctx.graph.chains_at(tip_h).to_vec();
    let mut views = Vec::new();
    {
        let txn = ctx.hot.ro()?;
        let halted: Vec<u8> = chains
            .iter()
            .copied()
            .filter(|&c| txn.counter(&apply::counters::halted(c)).unwrap_or(0) > 0)
            .collect();
        for &c in &chains {
            if halted.contains(&c) {
                tracing::error!(
                    chain = c,
                    "tail: chain is halted (Counters halted.{c}); not following it"
                );
                continue;
            }
            match ChainView::load(&txn, c, keep)? {
                Some(v) => views.push(v),
                None => anyhow::bail!("tail: chain {c} has no Tail or Headers checkpoint (empty store? use backfill or --start-height)"),
            }
        }
    }
    let behind: u64 = views
        .iter()
        .map(|v| {
            cut.hashes
                .get(&v.chain)
                .map(|e| e.height.saturating_sub(u64::from(v.tip.0)))
                .unwrap_or(0)
        })
        .sum();
    tracing::info!(
        chains = views.len(),
        catch_up_blocks = behind,
        min_stored = views.iter().map(|v| v.tip.0).min(),
        cut_min = tip_h,
        "tail: start"
    );

    let mut senders = BTreeMap::new();
    let mut receivers = Vec::new();
    for v in &views {
        let (tx, rx) = watch::channel(cut.hashes.get(&v.chain).map(|e| (e.height, e.hash.clone())));
        senders.insert(v.chain, tx);
        receivers.push(rx);
    }
    let feeders = shutdown.child_token();
    let sse = tokio::spawn(sse_task(ctx.clone(), senders.clone(), feeders.clone()));
    let poll = tokio::spawn(cut_task(ctx.clone(), senders, feeders.clone()));

    let stops: BTreeMap<u8, CancellationToken> = views
        .iter()
        .map(|v| (v.chain, shutdown.child_token()))
        .collect();
    let (btx, brx) = mpsc::channel(QUEUE);
    let w = tokio::spawn(writer(ctx.clone(), brx, shutdown.clone(), stops.clone()));
    // Dev forced-reorg check: `KDX_DEBUG_FEED=chain:height:hash` makes that chain's first
    // round follow the given (orphan) header; the next round meets the canonical branch and
    // goes through the normal reorg path.
    let feed: Option<(u8, u64, String)> = std::env::var("KDX_DEBUG_FEED").ok().and_then(|v| {
        let mut it = v.splitn(3, ':');
        Some((
            it.next()?.parse().ok()?,
            it.next()?.parse().ok()?,
            it.next()?.to_owned(),
        ))
    });
    if let Some(f) = &feed {
        tracing::warn!(chain = f.0, height = f.1, hash = %f.2, "tail: KDX_DEBUG_FEED set");
    }
    let mut fs = Vec::new();
    for (v, rx) in views.into_iter().zip(receivers) {
        let fd = feed
            .as_ref()
            .filter(|f| f.0 == v.chain)
            .map(|f| (f.1, f.2.clone()));
        let stop = stops[&v.chain].clone();
        fs.push(tokio::spawn(follower(
            ctx.clone(),
            v,
            rx,
            btx.clone(),
            stop,
            fd,
        )));
    }
    drop(btx);
    let mut err = None;
    for f in fs {
        match f.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => err = err.or(Some(e)),
            Err(e) => err = err.or(Some(e.into())),
        }
    }
    feeders.cancel();
    let _ = sse.await;
    let _ = poll.await;
    // All senders are gone: the writer drains and commits what it holds. Its error comes
    // first: a follower's send error is only a consequence of it.
    w.await??;
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}
