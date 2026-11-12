//! `BlockFeed` (GQ-6, Task 6): new canonical blocks per chain for subscriptions, polled from
//! the committed tail checkpoints, so `serve` and replicas (no writer in their process) have
//! subscriptions too.
//!
//! - Every `poll` it reads the per-chain stored tips (one status-slot read). For each chain
//!   whose tip moved it reads the new blocks with their txs (one read) and sends them on a
//!   bounded broadcast channel (capacity [`CAPACITY`]).
//! - A changed hash at a height already sent is a reorg: the feed walks back over the hashes
//!   it sent (the last [`RING`] per chain) to the first one still stored, and sends the
//!   replacements.
//! - More than [`MAX_PER_TICK`] new blocks on a chain in one tick (a catch-up after a pause),
//!   or a fork deeper than the ring, is a gap: one [`FeedItem::Gap`] and the feed resumes
//!   from the new tip.
//! - The block records (no txs) of the last [`RECENT`] heights per chain stay in memory for
//!   `…FromDepth` releases, pruned per chain (M4). A tick
//!   reads every moved chain before it caches, publishes tips or sends anything, and drops
//!   the cached heights a rollback, reorg or gap made stale.
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
    /// Per chain, by height: block records only (txs are loaded again on release), so the
    /// cache stays a few MB in the dense era.
    recent: Mutex<HashMap<u8, BTreeMap<u64, Arc<BlockRecord>>>>,
    /// The order of the feed's state changes (tests assert it, L-1).
    #[cfg(test)]
    log: Mutex<Vec<&'static str>>,
}

impl Inner {
    /// Record a state change: `cache` (the recent cache changed), `tips` (the tips were
    /// published) or `send` (an item went to the subscribers).
    #[cfg(test)]
    fn note(&self, what: &'static str) {
        self.log
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(what);
    }

    #[cfg(not(test))]
    #[inline(always)]
    fn note(&self, _what: &'static str) {}
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
                #[cfg(test)]
                log: Mutex::new(Vec::new()),
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

    /// The recent canonical block record at `(chain, height)`, if the feed still holds it.
    pub fn recent(&self, chain: u8, height: u64) -> Option<Arc<BlockRecord>> {
        self.inner
            .recent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&chain)
            .and_then(|m| m.get(&height))
            .cloned()
    }

    /// Send an item as the polling task would (tests and the harness's feed checks).
    pub fn inject(&self, item: FeedItem) {
        if let FeedItem::Block(b) = &item {
            self.remember(b);
            let mut t = self.inner.tips.lock().unwrap_or_else(|e| e.into_inner());
            let e = t.entry(b.chain).or_insert(b.height);
            *e = (*e).max(b.height);
        }
        let _ = self.inner.tx.send(item);
    }

    /// Drop the cached blocks of `chain` above `height` (all of them with `None`): they are
    /// no longer canonical after a rollback, a reorg or a gap.
    fn forget_above(&self, chain: u8, height: Option<u64>) {
        self.inner.note("cache");
        let mut r = self.inner.recent.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(m) = r.get_mut(&chain) {
            match height {
                Some(h) => drop(m.split_off(&h.saturating_add(1))),
                None => m.clear(),
            }
        }
    }

    /// Cache the record of `b`, dropping its chain's heights more than [`RECENT`] below it
    /// (per chain, not a scan of the whole cache).
    fn remember(&self, b: &FeedBlock) {
        self.inner.note("cache");
        let mut r = self.inner.recent.lock().unwrap_or_else(|e| e.into_inner());
        let m = r.entry(b.chain).or_default();
        m.insert(b.height, b.block.clone());
        let floor = (b.height + 1).saturating_sub(RECENT);
        while m.first_key_value().is_some_and(|(k, _)| *k < floor) {
            m.pop_first();
        }
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
/// Consecutive failed reads per chain (M-4).
type Fails = HashMap<u8, u32>;

/// Consecutive failed reads after which a chain resumes from its tip with a `Gap` (M-4).
pub const MAX_CHAIN_FAILS: u32 = 30;

#[cfg(test)]
static FAIL_CHAIN: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(u32::MAX);

/// Test hook: the reads of `chain` fail.
#[cfg(test)]
fn injected_failure(chain: u8) -> bool {
    FAIL_CHAIN.load(std::sync::atomic::Ordering::Relaxed) == u32::from(chain)
}
#[cfg(not(test))]
fn injected_failure(_: u8) -> bool {
    false
}

async fn run(state: Arc<ApiState>, poll: Duration, weak: Weak<Inner>) {
    let mut sent: Sent = HashMap::new();
    let mut fails: Fails = HashMap::new();
    let mut newest_ct: u64 = 0;
    let mut tick = tokio::time::interval(poll);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let Some(inner) = weak.upgrade() else {
            return;
        };
        let feed = BlockFeed { inner };
        if let Err(e) = step(&state, &feed, &mut sent, &mut fails, &mut newest_ct).await {
            tracing::warn!(error = %e, "graphql feed: reading the chain tips failed");
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
    fails: &mut Fails,
    newest_ct: &mut u64,
) -> ApiResult<()> {
    let tips = state.read_status(|v| v.chain_tips()).await?;
    // Read every moved chain first. Nothing is cached, published or sent until all reads
    // are done, so a subscriber woken by one chain's block never sees another chain's new
    // tip while its reorged heights still hold the orphaned blocks (I-5).
    // M-4: one chain's failed read skips that chain only (its tip is not published either),
    // and after MAX_CHAIN_FAILS in a row the chain resumes from its tip with a Gap.
    let mut moved = Vec::new();
    let mut skipped = Vec::new();
    for &(c, h, hash) in &tips {
        let h = u64::from(h);
        let Some(ring) = sent.get(&c) else {
            // First sight: start from the current tip, send nothing.
            sent.insert(c, vec![(h, hash)]);
            continue;
        };
        if ring.last() == Some(&(h, hash)) {
            continue;
        }
        let r = ring.clone();
        let res = state
            .read(move |view| {
                if injected_failure(c) {
                    return Err(kdx_api::ApiError::Unavailable("injected".into()));
                }
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
            .await;
        let step = match res {
            Ok(s) => {
                fails.remove(&c);
                s
            }
            Err(e) => {
                let n = fails.entry(c).or_insert(0);
                *n += 1;
                tracing::warn!(
                    chain = c,
                    failures = *n,
                    error = %e,
                    "graphql feed: reading a chain's new blocks failed; the chain is skipped this tick"
                );
                if *n < MAX_CHAIN_FAILS {
                    skipped.push(c);
                    continue;
                }
                fails.remove(&c);
                Step::Gap
            }
        };
        moved.push((c, h, hash, step));
    }
    // No await from here on: cache the replacements, then publish the tips, then send.
    let mut items = Vec::new();
    for (c, h, hash, step) in moved {
        let entry = sent.entry(c).or_default();
        match step {
            Step::Gap => {
                feed.forget_above(c, None);
                items.push(FeedItem::Gap(c));
                *entry = vec![(h, hash)];
            }
            Step::Back { from } => {
                feed.forget_above(c, Some(from));
                entry.retain(|x| x.0 <= from);
                if entry.last().map(|x| x.0) != Some(h) {
                    entry.push((h, hash));
                }
            }
            Step::Blocks { from, blocks } => {
                feed.forget_above(c, Some(from));
                entry.retain(|x| x.0 <= from);
                for b in blocks {
                    *newest_ct = (*newest_ct).max(b.block.creation_time);
                    entry.push((b.height, b.hash));
                    feed.remember(&b);
                    items.push(FeedItem::Block(b));
                }
                if entry.len() > RING {
                    let cut = entry.len() - RING;
                    entry.drain(..cut);
                }
            }
        }
    }
    {
        let mut t = feed.inner.tips.lock().unwrap_or_else(|e| e.into_inner());
        feed.inner.note("tips");
        for (c, h, _) in tips.iter().filter(|t| !skipped.contains(&t.0)) {
            t.insert(*c, u64::from(*h));
        }
    }
    for item in items {
        feed.inner.note("send");
        let _ = feed.inner.tx.send(item);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{self, hash};

    /// I-5: a subscriber woken by one chain's new block never sees another chain's new tip
    /// while that chain's reorged height still holds the orphaned block.
    #[tokio::test]
    async fn tips_move_only_after_the_replacements_are_cached() {
        let f = testutil::store();
        for c in [2u8, 5] {
            f.apply(c, 0, 0);
        }
        let feed = BlockFeed::detached();
        let (mut sent, mut ct) = (Sent::new(), 0);
        step(&f.st, &feed, &mut sent, &mut Fails::new(), &mut ct)
            .await
            .unwrap();
        f.apply(5, 1, 0);
        step(&f.st, &feed, &mut sent, &mut Fails::new(), &mut ct)
            .await
            .unwrap();
        assert_eq!(feed.recent(5, 1).map(|b| b.hash), Some(hash(5, 1, 0)));
        // One tick: chain 5 replaces height 1 and grows to 2; chain 2 gains a block.
        f.unwind(5, 0);
        f.apply(5, 1, 1);
        f.apply(5, 2, 1);
        f.apply(2, 1, 0);
        feed.inner.log.lock().unwrap().clear();
        let mut rx = feed.subscribe();
        let obs = feed.clone();
        let seen = tokio::spawn(async move {
            loop {
                if let Ok(FeedItem::Block(b)) = rx.recv().await {
                    if b.chain == 2 {
                        let tip = obs.tips()[&5];
                        return (tip, obs.recent(5, 1).map(|b| b.hash));
                    }
                }
            }
        });
        step(&f.st, &feed, &mut sent, &mut Fails::new(), &mut ct)
            .await
            .unwrap();
        let (tip, at1) = seen.await.unwrap();
        assert_eq!((tip, at1), (2, Some(hash(5, 1, 1))));
        // L-1: assert the order itself, not only what an observer happens to see. Within the
        // tick every cache change (the stale height dropped, the replacements stored) comes
        // before the tips are published, and the tips before the first item is sent.
        let log = feed.inner.log.lock().unwrap().clone();
        let tips = log
            .iter()
            .position(|x| *x == "tips")
            .expect("tips published");
        let first_send = log.iter().position(|x| *x == "send").expect("items sent");
        // Chain 5: the stale height dropped and the replacement stored; chain 2: its block.
        assert!(
            log[..tips].iter().filter(|x| **x == "cache").count() >= 3,
            "{log:?}"
        );
        assert!(
            log[tips..].iter().all(|x| *x != "cache"),
            "cache after tips: {log:?}"
        );
        assert!(tips < first_send, "{log:?}");
        assert_eq!(log.iter().filter(|x| **x == "tips").count(), 1, "{log:?}");
    }

    /// M-4: a chain whose reads fail is skipped alone: the other chains keep delivering,
    /// and after MAX_CHAIN_FAILS failures in a row the failing chain gets a Gap.
    #[tokio::test]
    async fn one_failing_chain_does_not_stop_the_others() {
        let f = testutil::store();
        for c in [0u8, 1] {
            f.apply(c, 0, 0);
        }
        let feed = BlockFeed::detached();
        let (mut sent, mut fails, mut ct) = (Sent::new(), Fails::new(), 0);
        step(&f.st, &feed, &mut sent, &mut fails, &mut ct)
            .await
            .unwrap();
        let mut rx = feed.subscribe();
        FAIL_CHAIN.store(1, std::sync::atomic::Ordering::Relaxed);
        for h in 1..MAX_CHAIN_FAILS as u64 {
            f.apply(0, h, 0);
            f.apply(1, h, 0);
            step(&f.st, &feed, &mut sent, &mut fails, &mut ct)
                .await
                .unwrap();
            match rx.try_recv() {
                Ok(FeedItem::Block(b)) => assert_eq!((b.chain, b.height), (0, h)),
                other => panic!("tick {h}: {other:?}"),
            }
            assert!(rx.try_recv().is_err(), "chain 1 sent nothing");
            assert_eq!(feed.tips().get(&1), Some(&0), "chain 1's tip is held");
            assert_eq!(fails.get(&1), Some(&(h as u32)));
        }
        // The 30th failure in a row: chain 1 resumes from its tip with a Gap.
        step(&f.st, &feed, &mut sent, &mut fails, &mut ct)
            .await
            .unwrap();
        FAIL_CHAIN.store(u32::MAX, std::sync::atomic::Ordering::Relaxed);
        assert!(matches!(rx.try_recv(), Ok(FeedItem::Gap(1))));
        let tip = MAX_CHAIN_FAILS as u64 - 1;
        assert_eq!(feed.tips().get(&1), Some(&tip));
        assert!(fails.is_empty());
        f.apply(1, tip + 1, 0);
        step(&f.st, &feed, &mut sent, &mut fails, &mut ct)
            .await
            .unwrap();
        match rx.try_recv() {
            Ok(FeedItem::Block(b)) => assert_eq!((b.chain, b.height), (1, tip + 1)),
            other => panic!("{other:?}"),
        }
    }

    /// M4: the recent cache keeps block records only (never a block's txs) and prunes each
    /// chain on its own.
    #[test]
    fn recent_keeps_records_only_and_prunes_per_chain() {
        let feed = BlockFeed::detached();
        let tx = Arc::new(TxData {
            rec: kdx_types::record::TxRecord::default(),
            block: Arc::new(BlockRecord::default()),
            out: serde_json::Value::Null,
            cmd: None,
            evs: Vec::new(),
            trs: Vec::new(),
            nfts: Vec::new(),
        });
        let b = testutil::feed_block(0, 1, 0);
        feed.inject(FeedItem::Block(Arc::new(FeedBlock {
            txs: Arc::new(vec![tx.clone()]),
            chain: b.chain,
            height: b.height,
            hash: b.hash,
            block: b.block.clone(),
        })));
        // Push the item out of the broadcast buffer: only the cache could still hold it.
        for h in 0..(CAPACITY as u64 + 10) {
            feed.inject(FeedItem::Block(testutil::feed_block(1, h, 0)));
        }
        assert_eq!(Arc::strong_count(&tx), 1, "the cache holds a block's txs");
        assert_eq!(feed.recent(0, 1).map(|b| b.hash), Some(hash(0, 1, 0)));
        let top = CAPACITY as u64 + 9;
        assert!(feed.recent(1, top - RECENT).is_none());
        assert!(feed.recent(1, top - RECENT + 1).is_some());
    }
}
