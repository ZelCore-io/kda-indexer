//! `BlockFeed` (GQ-6, Task 6): new canonical blocks per chain for subscriptions, polled from
//! the committed tail checkpoints, so `serve` and replicas (no writer in their process) have
//! subscriptions too.
//!
//! - Every `poll` it reads the per-chain stored tips (one status-slot read). For each chain
//!   whose tip moved it reads the new blocks with their txs (one read) and sends them on a
//!   bounded broadcast channel (capacity [`CAPACITY`]).
//! - A changed hash at a height already sent is a reorg: the feed walks back over the hashes
//!   it sent (the last [`RING`] per chain) to the first one still stored, and sends the
//!   replacements with `reorg: true`.
//! - More than [`MAX_PER_TICK`] new blocks on a chain in one tick (a catch-up after a pause),
//!   or a fork deeper than the ring, is a gap: one [`FeedItem::Gap`] and the feed resumes
//!   from the new tip.
//! - The last [`RECENT`] heights per chain stay in memory for `…FromDepth` releases.
//! - The polling task ends when the last handle drops.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kdx_api::store::View;
use kdx_api::{ApiResult, ApiState};
use kdx_types::record::BlockRecord;
use kdx_types::Hash32;
use tokio::sync::broadcast;

use crate::txlist::{load, Blocks, TxData};

/// Items the broadcast channel holds before a slow subscriber lags.
pub const CAPACITY: usize = 1024;
/// Most new blocks per chain per tick.
pub const MAX_PER_TICK: u64 = 200;
/// Sent hashes kept per chain for reorg detection.
pub const RING: usize = 64;
/// Recent canonical blocks kept per chain for depth releases.
pub const RECENT: u64 = 256;

/// One new canonical block with its txs (coinbase first).
#[derive(Debug)]
pub struct FeedBlock {
    pub chain: u8,
    pub height: u64,
    pub hash: Hash32,
    pub block: Arc<BlockRecord>,
    pub txs: Arc<Vec<Arc<TxData>>>,
    /// The block replaces one sent before at its height.
    pub reorg: bool,
}

#[derive(Debug, Clone)]
pub enum FeedItem {
    Block(Arc<FeedBlock>),
    /// Blocks of `chain` were skipped (catch-up or deep fork).
    Gap(u8),
}

struct Inner {
    tx: broadcast::Sender<FeedItem>,
    tips: Mutex<BTreeMap<u8, u64>>,
    recent: Mutex<HashMap<(u8, u64), Arc<FeedBlock>>>,
}

/// Handle of the block feed (cheap to clone).
#[derive(Clone)]
pub struct BlockFeed {
    inner: Arc<Inner>,
}

impl Default for BlockFeed {
    fn default() -> Self {
        BlockFeed::detached()
    }
}

impl BlockFeed {
    /// A feed with no polling task (tests, `check-ops`): items come only from [`inject`].
    ///
    /// [`inject`]: BlockFeed::inject
    pub fn detached() -> BlockFeed {
        let (tx, _) = broadcast::channel(CAPACITY);
        BlockFeed {
            inner: Arc::new(Inner {
                tx,
                tips: Mutex::new(BTreeMap::new()),
                recent: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Start polling `state` every `poll`.
    pub fn spawn(state: Arc<ApiState>, poll: Duration) -> BlockFeed {
        let feed = BlockFeed::detached();
        let weak = Arc::downgrade(&feed.inner);
        tokio::spawn(run(state, poll.max(Duration::from_millis(50)), weak));
        feed
    }

    pub fn subscribe(&self) -> broadcast::Receiver<FeedItem> {
        self.inner.tx.subscribe()
    }

    /// The stored tip height per chain, as of the last tick.
    pub fn tips(&self) -> BTreeMap<u8, u64> {
        self.inner
            .tips
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The recent canonical block at `(chain, height)`, if the feed still holds it.
    pub fn recent(&self, chain: u8, height: u64) -> Option<Arc<FeedBlock>> {
        self.inner
            .recent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(chain, height))
            .cloned()
    }

    /// Send an item as the polling task would (tests and the harness's feed checks).
    pub fn inject(&self, item: FeedItem) {
        if let FeedItem::Block(b) = &item {
            self.remember(b.clone());
            let mut t = self.inner.tips.lock().unwrap_or_else(|e| e.into_inner());
            let e = t.entry(b.chain).or_insert(b.height);
            *e = (*e).max(b.height);
        }
        let _ = self.inner.tx.send(item);
    }

    fn remember(&self, b: Arc<FeedBlock>) {
        let mut r = self.inner.recent.lock().unwrap_or_else(|e| e.into_inner());
        let (c, h) = (b.chain, b.height);
        r.insert((c, h), b);
        r.retain(|k, _| k.0 != c || k.1 + RECENT > h);
    }
}

/// The blocks `(chain, height)` with their txs, as feed items (one read).
pub fn load_blocks(view: &View, keys: &[(u8, u64)]) -> ApiResult<Vec<Arc<FeedBlock>>> {
    let mut out = Vec::with_capacity(keys.len());
    let mut blocks = Blocks::default();
    for &(c, h) in keys {
        view.check_deadline()?;
        let Ok(h32) = u32::try_from(h) else { continue };
        let Some(b) = view.block(c, h32)? else {
            continue;
        };
        let mut txs = Vec::new();
        for rec in view.block_txs(c, h32)? {
            if let Some(d) = load(view, rec.key, Some(rec), &mut blocks)? {
                txs.push(d);
            }
        }
        out.push(Arc::new(FeedBlock {
            chain: c,
            height: h,
            hash: b.hash,
            block: Arc::new(b),
            txs: Arc::new(txs),
            reorg: false,
        }));
    }
    Ok(out)
}

enum Step {
    /// New blocks from height `from + 1`.
    Blocks {
        from: u64,
        blocks: Vec<Arc<FeedBlock>>,
    },
    /// The tip moved back to (or stayed at) a height already sent, at `from`.
    Back {
        from: u64,
    },
    Gap,
}

/// Sent `(height, hash)` per chain, oldest first.
type Sent = HashMap<u8, Vec<(u64, Hash32)>>;

async fn run(state: Arc<ApiState>, poll: Duration, weak: Weak<Inner>) {
    let mut sent: Sent = HashMap::new();
    let mut newest_ct: u64 = 0;
    let mut tick = tokio::time::interval(poll);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let Some(inner) = weak.upgrade() else {
            return;
        };
        let feed = BlockFeed { inner };
        if let Err(e) = step(&state, &feed, &mut sent, &mut newest_ct).await {
            tracing::debug!(error = %e, "graphql feed tick failed");
        }
        if newest_ct > 0 {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_micros() as u64);
            kdx_metrics::get()
                .graphql_feed_lag_seconds
                .set(now.saturating_sub(newest_ct) as f64 / 1e6);
        }
    }
}

async fn step(
    state: &Arc<ApiState>,
    feed: &BlockFeed,
    sent: &mut Sent,
    newest_ct: &mut u64,
) -> ApiResult<()> {
    let tips = state.read_status(|v| v.chain_tips()).await?;
    {
        let mut t = feed.inner.tips.lock().unwrap_or_else(|e| e.into_inner());
        for (c, h, _) in &tips {
            t.insert(*c, u64::from(*h));
        }
    }
    for (c, h, hash) in tips {
        let h = u64::from(h);
        let Some(ring) = sent.get(&c).cloned() else {
            // First sight: start from the current tip, send nothing.
            sent.insert(c, vec![(h, hash)]);
            continue;
        };
        if ring.last() == Some(&(h, hash)) {
            continue;
        }
        let r = ring.clone();
        let step = state
            .read(move |view| {
                let mut from = None;
                for (hh, hhash) in r.iter().rev() {
                    let Ok(h32) = u32::try_from(*hh) else {
                        continue;
                    };
                    if view.block_hash(c, h32)? == Some(*hhash) {
                        from = Some(*hh);
                        break;
                    }
                }
                let Some(from) = from else {
                    return Ok(Step::Gap);
                };
                if h <= from {
                    return Ok(Step::Back { from: h });
                }
                if h - from > MAX_PER_TICK {
                    return Ok(Step::Gap);
                }
                let keys: Vec<(u8, u64)> = (from + 1..=h).map(|x| (c, x)).collect();
                Ok(Step::Blocks {
                    from,
                    blocks: load_blocks(view, &keys)?,
                })
            })
            .await?;
        let last_sent = ring.last().map_or(0, |x| x.0);
        let entry = sent.entry(c).or_default();
        match step {
            Step::Gap => {
                let _ = feed.inner.tx.send(FeedItem::Gap(c));
                *entry = vec![(h, hash)];
            }
            Step::Back { from } => {
                entry.retain(|x| x.0 <= from);
                if entry.last().map(|x| x.0) != Some(h) {
                    entry.push((h, hash));
                }
            }
            Step::Blocks { from, blocks } => {
                entry.retain(|x| x.0 <= from);
                for b in blocks {
                    let b = Arc::new(FeedBlock {
                        reorg: b.height <= last_sent,
                        chain: b.chain,
                        height: b.height,
                        hash: b.hash,
                        block: b.block.clone(),
                        txs: b.txs.clone(),
                    });
                    *newest_ct = (*newest_ct).max(b.block.creation_time);
                    entry.push((b.height, b.hash));
                    feed.remember(b.clone());
                    let _ = feed.inner.tx.send(FeedItem::Block(b));
                }
                if entry.len() > RING {
                    let cut = entry.len() - RING;
                    entry.drain(..cut);
                }
            }
        }
    }
    Ok(())
}
