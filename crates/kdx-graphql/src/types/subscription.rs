//! The `Subscription` root (GQ-6, Task 6) over the committed-block feed ([`crate::feed`]).
//!
//! - Every subscription holds a per-connection and a per-process slot ([`crate::ws`]) and
//!   counts in `kdx_graphql_subscriptions`; all are released when its stream drops.
//! - A message never carries more items than the limits walker priced (I-1): `newBlocks`
//!   and `newBlocksFromDepth` at most one block per subscribed chain (a catch-up is split
//!   over several messages), `quantity` at most `quantity` (clamped to `max_page`), and
//!   `events` at most the default page of events (the rest follow in the next messages).
//! - A feed gap on a subscribed chain is one `INTERNAL` error, after the blocks before it;
//!   the stream stays open. A subscriber that falls more than the feed's capacity behind is
//!   moved to the feed's tail and gets the same error (memory stays bounded).
//! - `…FromDepth` and `events(minimumDepth)` release a block once its chain's tip is at least
//!   `minimumDepth` above it, from the feed's recent canonical blocks once the store confirms
//!   each is still canonical (one store read per release).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use async_graphql::{Context, Result, Subscription as SubscriptionAttr};
use futures::stream::{self, BoxStream, StreamExt};
use kdx_api::ApiState;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::sync::broadcast::Receiver;

use super::block::chain_set;
use super::{Block, Event, Transaction};
use crate::error::{bad_input, err, from_api, not_supported, Code};
use crate::feed::{load_blocks, BlockFeed, FeedBlock, FeedItem, MAX_PER_TICK};
use crate::gql;
use crate::txlist::{chain_arg, hash_arg, load, name_arg, Blocks, ParamFilter};
use crate::ws::SubSlot;

/// Most feed items one `newBlocks` message carries.
const MAX_BATCH: usize = 100;

pub struct Subscription;

fn default_chain_ids() -> Option<Vec<String>> {
    super::default_chain_ids()
}

fn feed_of(ctx: &Context<'_>) -> Result<BlockFeed> {
    gql(ctx)
        .feed
        .clone()
        .ok_or_else(|| not_supported("subscriptions (no block feed)"))
}

/// Feed receiver plus the slot it holds.
struct Sub {
    rx: Receiver<FeedItem>,
    _slot: SubSlot,
    /// A gap or lag met inside a batch: reported on the next call, after the blocks before
    /// it.
    pending: Option<async_graphql::Error>,
}

fn gap_error(chain: u8) -> async_graphql::Error {
    err(
        Code::Internal,
        format!("blocks of chain {chain} were skipped by the feed"),
    )
}

fn lag_error() -> async_graphql::Error {
    err(
        Code::Internal,
        "blocks were skipped: the subscriber fell behind the block feed",
    )
}

/// The next feed items for `chains` (a batch of at most `cap` blocks), or an error item;
/// `None` ends. A lagging receiver moves to the feed's tail (the skip is the error).
async fn next_batch(
    sub: &mut Sub,
    chains: &[u8],
    cap: usize,
) -> Option<Result<Vec<Arc<FeedBlock>>>> {
    if let Some(e) = sub.pending.take() {
        return Some(Err(e));
    }
    let cap = cap.max(1);
    loop {
        match sub.rx.recv().await {
            Ok(FeedItem::Block(b)) if chains.contains(&b.chain) => {
                let mut out = vec![b];
                while out.len() < cap {
                    match sub.rx.try_recv() {
                        Ok(FeedItem::Block(b)) if chains.contains(&b.chain) => out.push(b),
                        Ok(FeedItem::Gap(c)) if chains.contains(&c) => {
                            // Deliver what we have; the gap is reported on the next call,
                            // and the items queued behind it follow (I-4).
                            sub.pending = Some(gap_error(c));
                            return Some(Ok(out));
                        }
                        Ok(_) => {}
                        Err(TryRecvError::Lagged(_)) => {
                            sub.rx = sub.rx.resubscribe();
                            sub.pending = Some(lag_error());
                            return Some(Ok(out));
                        }
                        Err(_) => break,
                    }
                }
                return Some(Ok(out));
            }
            Ok(FeedItem::Gap(c)) if chains.contains(&c) => return Some(Err(gap_error(c))),
            Ok(_) => {}
            Err(RecvError::Lagged(_)) => {
                sub.rx = sub.rx.resubscribe();
                return Some(Err(lag_error()));
            }
            Err(RecvError::Closed) => return None,
        }
    }
}

/// Releases blocks once they are `depth` below their chain's tip.
struct DepthGate {
    depth: u64,
    /// The subscriber reads the blocks' txs: load them from the store (the feed caches
    /// block records only).
    txs: bool,
    chains: Vec<u8>,
    next: HashMap<u8, u64>,
}

impl DepthGate {
    /// The next blocks at `depth`, at most `cap` of them (lowest heights first; the rest
    /// wait for the next call), and a chain more than [`MAX_PER_TICK`] behind (skipped to
    /// its target: a gap).
    async fn release(
        &mut self,
        feed: &BlockFeed,
        state: &Arc<ApiState>,
        cap: usize,
    ) -> Result<(Vec<Arc<FeedBlock>>, Option<u8>)> {
        let tips = feed.tips();
        let mut want = Vec::new();
        let mut gap = None;
        for &c in &self.chains {
            let Some(&tip) = tips.get(&c) else { continue };
            let Some(target) = tip.checked_sub(self.depth) else {
                continue;
            };
            let next = *self.next.entry(c).or_insert(target + 1);
            if next > target {
                continue;
            }
            if target - next >= MAX_PER_TICK {
                gap = Some(c);
                self.next.insert(c, target + 1);
                continue;
            }
            want.extend((next..=target).map(|h| (h, c)));
        }
        want.sort_unstable();
        want.truncate(cap.max(1));
        for &(h, c) in &want {
            self.next.insert(c, h + 1);
        }
        let picked: Vec<_> = want
            .into_iter()
            .map(|(h, c)| ((c, h), feed.recent(c, h)))
            .collect();
        if picked.is_empty() {
            return Ok((Vec::new(), gap));
        }
        // A cached block is released only while the store still holds it at its height: a
        // reorg replaces it with the stored block (I-5).
        let txs = self.txs;
        let mut out = state
            .read(move |view| {
                let mut out = Vec::with_capacity(picked.len());
                let mut missing = Vec::new();
                for ((c, h), b) in picked {
                    let Ok(h32) = u32::try_from(h) else { continue };
                    match b {
                        Some(b) if !txs && view.block_hash(c, h32)? == Some(b.hash) => {
                            out.push(Arc::new(FeedBlock {
                                chain: c,
                                height: h,
                                hash: b.hash,
                                block: b,
                                txs: Arc::new(Vec::new()),
                            }));
                        }
                        _ => missing.push((c, h)),
                    }
                }
                out.extend(load_blocks(view, &missing)?);
                Ok(out)
            })
            .await
            .map_err(from_api)?;
        out.sort_by_key(|b| (b.height, b.chain));
        Ok((out, gap))
    }
}

/// State of a depth-released stream.
struct DepthSub {
    sub: Sub,
    chains: Vec<u8>,
    gate: DepthGate,
    feed: BlockFeed,
    state: Arc<ApiState>,
    /// Try a release before waiting for the next feed item (false after an empty or failed
    /// release, so errors never repeat without a new item).
    ready: bool,
}

/// Feed items for `chains` in batches of at most `cap` blocks, released at `depth` when
/// given.
fn feed_stream(
    ctx: &Context<'_>,
    chains: Vec<u8>,
    depth: Option<u64>,
    cap: usize,
    txs: bool,
) -> Result<BoxStream<'static, Result<Vec<Arc<FeedBlock>>>>> {
    let feed = feed_of(ctx)?;
    let state = gql(ctx).state()?.clone();
    let slot = crate::ws::acquire(ctx)?;
    let sub = Sub {
        rx: feed.subscribe(),
        _slot: slot,
        pending: None,
    };
    match depth.filter(|d| *d > 0) {
        None => Ok(
            stream::unfold((sub, chains), move |(mut sub, chains)| async move {
                let item = next_batch(&mut sub, &chains, cap).await?;
                Some((item, (sub, chains)))
            })
            .boxed(),
        ),
        Some(depth) => {
            let gate = DepthGate {
                depth,
                txs,
                chains: chains.clone(),
                next: HashMap::new(),
            };
            let st = DepthSub {
                sub,
                chains,
                gate,
                feed,
                state,
                ready: false,
            };
            Ok(stream::unfold(st, move |mut st| async move {
                loop {
                    if let Some(e) = st.sub.pending.take() {
                        return Some((Err(e), st));
                    }
                    if st.ready {
                        st.ready = false;
                        match st.gate.release(&st.feed, &st.state, cap).await {
                            Err(e) => return Some((Err(e), st)),
                            Ok((blocks, gap)) if !blocks.is_empty() => {
                                // More may be due: release again before waiting.
                                st.ready = true;
                                st.sub.pending = gap.map(gap_error);
                                return Some((Ok(blocks), st));
                            }
                            Ok((_, Some(c))) => return Some((Err(gap_error(c)), st)),
                            Ok(_) => {}
                        }
                    }
                    // Any feed item for these chains is a wake-up.
                    if let Err(e) = next_batch(&mut st.sub, &st.chains, MAX_BATCH).await? {
                        return Some((Err(e), st));
                    }
                    st.ready = true;
                }
            })
            .boxed())
        }
    }
}

/// A `quantity` argument (ZelCore extension): 1..=[`MAX_BATCH`]; absent is 20. Clamped to
/// `max_page`, as the limits walker prices it (M3).
fn quantity_arg(v: Option<i32>, max_page: usize) -> Result<usize> {
    match v.unwrap_or(20) {
        n @ 1..=100 => Ok((n as usize).min(max_page.max(1))),
        _ => Err(bad_input(format!(
            "quantity must be between 1 and {MAX_BATCH}"
        ))),
    }
}

/// The user txs of `blocks`, newest first (height, chain, position descending), at most `q`.
fn newest_txs(blocks: &[Arc<FeedBlock>], q: usize) -> Vec<Transaction> {
    let mut txs: Vec<_> = blocks
        .iter()
        .flat_map(|b| b.txs.iter())
        .filter(|d| !d.is_coinbase())
        .collect();
    txs.sort_by_key(|d| std::cmp::Reverse(d.key()));
    txs.into_iter()
        .take(q)
        .filter_map(|d| Transaction::new(d.clone()))
        .collect()
}

fn depth_arg(name: &str, v: Option<i32>) -> Result<Option<u64>> {
    v.map(|d| u64::try_from(d).map_err(|_| bad_input(format!("{name} must not be negative"))))
        .transpose()
}

#[SubscriptionAttr]
impl Subscription {
    async fn events(
        &self,
        ctx: &Context<'_>,
        chain_id: Option<String>,
        minimum_depth: Option<i32>,
        parameters_filter: Option<String>,
        qualified_event_name: String,
    ) -> Result<BoxStream<'static, Result<Option<Vec<Event>>>>> {
        let qual = name_arg("qualifiedEventName", &qualified_event_name)?.to_owned();
        let chains = match chain_id {
            Some(c) => vec![chain_arg(&c)?],
            None => chain_set(None)?,
        };
        let params = parameters_filter
            .as_deref()
            .map(ParamFilter::parse)
            .transpose()?;
        let depth = depth_arg("minimumDepth", minimum_depth)?;
        // At most the walker's price per message: the default page (I-1).
        let per_msg = {
            let s = &gql(ctx).settings;
            s.default_page.min(s.max_page).max(1)
        };
        let cap = chains.len();
        let s = feed_stream(ctx, chains, depth, cap, true)?;
        Ok(stream::unfold(
            (s, VecDeque::new(), qual, params),
            move |(mut s, mut q, qual, params)| async move {
                loop {
                    if !q.is_empty() {
                        let n = q.len().min(per_msg);
                        let msg: Vec<Event> = q.drain(..n).collect();
                        return Some((Ok(Some(msg)), (s, q, qual, params)));
                    }
                    match s.next().await? {
                        Err(e) => return Some((Err(e), (s, q, qual, params))),
                        Ok(blocks) => {
                            for d in blocks.iter().flat_map(|b| b.txs.iter()) {
                                for (i, e) in d.evs.iter().enumerate() {
                                    if e.qual_name == qual
                                        && params.as_ref().is_none_or(|p| p.matches(&e.params))
                                    {
                                        q.push_back(Event::new(d.clone(), i));
                                    }
                                }
                            }
                        }
                    }
                }
            },
        )
        .boxed())
    }

    async fn new_blocks(
        &self,
        ctx: &Context<'_>,
        #[graphql(default_with = "default_chain_ids()")] chain_ids: Option<Vec<String>>,
        // ZelCore extension: at most this many blocks per message (the newest). Absent keeps
        // Kadena's behaviour (every block), at most one block per subscribed chain per
        // message, as priced.
        quantity: Option<i32>,
    ) -> Result<BoxStream<'static, Result<Option<Vec<Block>>>>> {
        let chains = chain_set(chain_ids.as_deref())?;
        let (cap, q) = match quantity {
            Some(_) => (
                MAX_BATCH,
                quantity_arg(quantity, gql(ctx).settings.max_page)?,
            ),
            None => (chains.len(), usize::MAX),
        };
        Ok(feed_stream(ctx, chains, None, cap, false)?
            .map(move |item| {
                item.map(|bs| {
                    let skip = bs.len().saturating_sub(q);
                    Some(
                        bs.iter()
                            .skip(skip)
                            .map(|b| Block::new(b.block.clone()))
                            .collect(),
                    )
                })
            })
            .boxed())
    }

    /// ZelCore extension: the user txs of newly committed blocks on every chain, newest
    /// first, at most `quantity` per message (a message per feed batch that has any).
    async fn transactions(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 20)] quantity: Option<i32>,
    ) -> Result<BoxStream<'static, Result<Option<Vec<Transaction>>>>> {
        let q = quantity_arg(quantity, gql(ctx).settings.max_page)?;
        let s = feed_stream(ctx, chain_set(None)?, None, MAX_BATCH, true)?;
        Ok(s.filter_map(move |item| {
            let out = item.map(|blocks| newest_txs(&blocks, q));
            async move {
                match out {
                    Ok(v) if v.is_empty() => None,
                    Ok(v) => Some(Ok(Some(v))),
                    Err(e) => Some(Err(e)),
                }
            }
        })
        .boxed())
    }

    async fn new_blocks_from_depth(
        &self,
        ctx: &Context<'_>,
        #[graphql(default_with = "default_chain_ids()")] chain_ids: Option<Vec<String>>,
        minimum_depth: i32,
    ) -> Result<BoxStream<'static, Result<Option<Vec<Block>>>>> {
        let chains = chain_set(chain_ids.as_deref())?;
        let depth = depth_arg("minimumDepth", Some(minimum_depth))?.unwrap_or(0);
        // At most one block per subscribed chain per message, as priced (I-1).
        let cap = chains.len();
        let s = feed_stream(ctx, chains, Some(depth).filter(|d| *d > 0), cap, false)?;
        Ok(s.map(|item| {
            item.map(|bs| Some(bs.iter().map(|b| Block::new(b.block.clone())).collect()))
        })
        .boxed())
    }

    /// One item when the tx is (or becomes) indexed, then the stream ends; a `TIMEOUT`
    /// error after `ws_tx_wait`.
    async fn transaction(
        &self,
        ctx: &Context<'_>,
        chain_id: Option<String>,
        request_key: String,
    ) -> Result<BoxStream<'static, Result<Option<Transaction>>>> {
        let rk = hash_arg("requestKey", &request_key)?;
        let chain = chain_id.as_deref().map(chain_arg).transpose()?;
        let feed = feed_of(ctx)?;
        let state = gql(ctx).state()?.clone();
        let until = Instant::now() + gql(ctx).settings.ws_tx_wait;
        let slot = crate::ws::acquire(ctx)?;
        let rx = feed.subscribe();
        let lookup = move |state: Arc<ApiState>| async move {
            state
                .read(move |view| {
                    let mut blocks = Blocks::default();
                    for (k, rec) in view.txs_by_rk(&rk)? {
                        if k.is_coinbase() || chain.is_some_and(|c| c != k.chain()) {
                            continue;
                        }
                        if let Some(d) = load(view, k, Some(rec), &mut blocks)? {
                            return Ok(Some(d));
                        }
                    }
                    Ok(None)
                })
                .await
                .map_err(from_api)
        };
        let s = stream::once(async move {
            let _slot = slot;
            let mut rx = rx;
            match lookup(state.clone()).await {
                Ok(Some(d)) => return Ok(Transaction::new(d)),
                Ok(None) => {}
                Err(e) => return Err(e),
            }
            loop {
                let left = until.saturating_duration_since(Instant::now());
                match tokio::time::timeout(left, rx.recv()).await {
                    Err(_) => {
                        return Err(err(
                            Code::Timeout,
                            "the transaction was not indexed in time",
                        ))
                    }
                    Ok(Ok(FeedItem::Block(b))) => {
                        if chain.is_some_and(|c| c != b.chain) {
                            continue;
                        }
                        if let Some(d) = b
                            .txs
                            .iter()
                            .find(|d| !d.is_coinbase() && d.rec.request_key == Some(rk))
                        {
                            return Ok(Transaction::new(d.clone()));
                        }
                    }
                    Ok(Ok(FeedItem::Gap(_))) | Ok(Err(RecvError::Lagged(_))) => {
                        // Items were skipped: look in the store again.
                        rx = rx.resubscribe();
                        if let Some(d) = lookup(state.clone()).await? {
                            return Ok(Transaction::new(d));
                        }
                    }
                    Ok(Err(RecvError::Closed)) => return Ok(None),
                }
            }
        });
        Ok(s.boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{self, feed_block, hash};

    /// I-5: the depth gate releases the stored block when the cached one was reorged away.
    #[tokio::test]
    async fn depth_gate_rechecks_the_store() {
        let f = testutil::store();
        f.apply(5, 0, 0);
        f.apply(5, 1, 1);
        f.apply(5, 2, 1);
        // The feed still caches the orphaned block of fork 0 at height 1, with tip 2.
        let feed = BlockFeed::detached();
        feed.inject(FeedItem::Block(feed_block(5, 1, 0)));
        feed.inject(FeedItem::Block(feed_block(5, 2, 1)));
        let mut gate = DepthGate {
            depth: 1,
            txs: false,
            chains: vec![5],
            next: HashMap::from([(5, 1)]),
        };
        let (out, gap) = gate.release(&feed, &f.st, 20).await.unwrap();
        let got: Vec<_> = out.iter().map(|b| (b.height, b.hash)).collect();
        assert_eq!((got, gap), (vec![(1, hash(5, 1, 1))], None));
    }

    /// I-1: a depth release carries at most `cap` blocks; the rest follow on the next calls.
    #[tokio::test]
    async fn depth_gate_releases_at_most_cap_per_call() {
        let f = testutil::store();
        let feed = BlockFeed::detached();
        for h in 0..=4 {
            f.apply(5, h, 0);
            feed.inject(FeedItem::Block(feed_block(5, h, 0)));
        }
        let mut gate = DepthGate {
            depth: 1,
            txs: false,
            chains: vec![5],
            next: HashMap::from([(5, 1)]),
        };
        let mut got = Vec::new();
        for _ in 0..4 {
            let (out, _) = gate.release(&feed, &f.st, 1).await.unwrap();
            got.push(out.iter().map(|b| b.height).collect::<Vec<_>>());
        }
        assert_eq!(got, [vec![1], vec![2], vec![3], vec![]]);
    }
}
