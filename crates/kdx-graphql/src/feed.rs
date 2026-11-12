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
//! - The ring is byte-bounded (M-3): the feed keeps an estimate of the decoded blocks among
//!   the last [`CAPACITY`] items that are still alive. Before a block would take it past
//!   the budget ([`RING_BYTES`] by default), the feed sends [`FeedItem::Pad`] items until the
//!   oldest held blocks leave the ring. Only a receiver that has not read those blocks lags
//!   (the existing lag error); a receiver that keeps up only skips the pads.
//! - The polling task ends when the last handle drops.

use std::collections::{BTreeMap, HashMap, VecDeque};
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
/// Default byte budget of the decoded blocks the broadcast ring holds (M-3).
pub const RING_BYTES: usize = 64 << 20;

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
    /// No content: sent to push old blocks out of the ring past its byte budget (M-3).
    /// Receivers skip it.
    Pad,
}

struct Inner {
    tx: broadcast::Sender<FeedItem>,
    tips: Mutex<BTreeMap<u8, u64>>,
    /// Per chain, by height: block records only (txs are loaded again on release), so the
    /// cache stays a few MB in the dense era.
    recent: Mutex<HashMap<u8, BTreeMap<u64, Arc<BlockRecord>>>>,
    /// Every send goes through it (M-3).
    ring: Mutex<Ring>,
}

/// The byte account of the broadcast ring (M-3).
struct Ring {
    /// Items sent into the channel: its tail position (a send with no receiver stores
    /// nothing and is not counted).
    sent: u64,
    /// Blocks among the last [`CAPACITY`] sent that may still be alive, oldest first:
    /// (position, block, approximate bytes).
    held: VecDeque<(u64, Weak<FeedBlock>, usize)>,
    /// Their bytes.
    bytes: usize,
    budget: usize,
}

impl Ring {
    /// Forget the blocks every receiver has read (the channel dropped them) and those the
    /// channel has overwritten.
    fn prune(&mut self) {
        let floor = self.sent.saturating_sub(CAPACITY as u64);
        let mut bytes = 0;
        self.held.retain(|(pos, w, n)| {
            let keep = *pos >= floor && w.strong_count() > 0;
            if keep {
                bytes += n;
            }
            keep
        });
        self.bytes = bytes;
    }

    /// Room for `need` more bytes: pads until the oldest held blocks leave the ring (the
    /// channel overwrites position `p` when it sends `p + CAPACITY`). At most [`CAPACITY`]
    /// pads in all, so a receiver that has read every block sent so far never lags; a block
    /// over the whole budget leaves the ring holding only itself.
    fn make_room(&mut self, tx: &broadcast::Sender<FeedItem>, need: usize) {
        loop {
            self.prune();
            if self.bytes + need <= self.budget {
                return;
            }
            let Some(&(oldest, ..)) = self.held.front() else {
                return;
            };
            let pads = (oldest + CAPACITY as u64 + 1).saturating_sub(self.sent);
            for _ in 0..pads {
                if tx.send(FeedItem::Pad).is_err() {
                    // No receiver: the channel holds nothing.
                    self.held.clear();
                    self.bytes = 0;
                    return;
                }
                self.sent += 1;
            }
        }
    }
}

/// Approximate heap bytes of a JSON value (each node counted at its inline size).
fn value_bytes(v: &serde_json::Value) -> usize {
    use serde_json::Value;
    const NODE: usize = std::mem::size_of::<Value>();
    NODE + match v {
        Value::String(s) => s.len(),
        Value::Array(a) => a.iter().map(value_bytes).sum(),
        Value::Object(m) => m.iter().map(|(k, v)| NODE + k.len() + value_bytes(v)).sum(),
        _ => 0,
    }
}

/// Approximate bytes of a decoded tx: its stored record's buffers and the decoded output,
/// command, events and transfers.
fn tx_bytes(d: &TxData) -> usize {
    use std::mem::size_of;
    let r = &d.rec;
    let opt = |b: &Option<Vec<u8>>| b.as_ref().map_or(0, Vec::len);
    let rec = r.result.len()
        + opt(&r.cmd)
        + opt(&r.metadata)
        + opt(&r.continuation)
        + opt(&r.extra)
        + opt(&r.raw_tx)
        + opt(&r.raw_output)
        + std::mem::size_of_val(r.sigs.as_slice())
        + std::mem::size_of_val(r.events.as_slice())
        + std::mem::size_of_val(r.transfers.as_slice());
    let evs: usize = d
        .evs
        .iter()
        .map(|e| {
            size_of::<kdx_api::compat::shapes::Ev>()
                + e.qual_name.len()
                + e.module.len()
                + e.module_hash.len()
                + value_bytes(&e.params)
        })
        .sum();
    let tr = |t: &kdx_api::compat::shapes::Transfer| {
        size_of::<kdx_api::compat::shapes::Transfer>()
            + t.token.len()
            + t.from.len()
            + t.to.len()
            + t.amount.len()
    };
    let trs: usize = d.trs.iter().map(tr).sum();
    let nfts: usize = d.nfts.iter().map(|n| tr(&n.tr) + n.token_id.len()).sum();
    size_of::<TxData>()
        + rec
        + value_bytes(&d.out)
        + d.cmd.as_ref().map_or(0, value_bytes)
        + evs
        + trs
        + nfts
}

/// Approximate bytes a feed block holds (its txs; the record is small and shared).
pub fn block_bytes(b: &FeedBlock) -> usize {
    std::mem::size_of::<FeedBlock>()
        + std::mem::size_of::<BlockRecord>()
        + b.txs.iter().map(|d| tx_bytes(d)).sum::<usize>()
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
        BlockFeed::detached_with(RING_BYTES)
    }

    /// A detached feed whose ring holds at most `ring_bytes` of decoded blocks (M-3).
    pub fn detached_with(ring_bytes: usize) -> BlockFeed {
        let (tx, _) = broadcast::channel(CAPACITY);
        BlockFeed {
            inner: Arc::new(Inner {
                tx,
                tips: Mutex::new(BTreeMap::new()),
                recent: Mutex::new(HashMap::new()),
                ring: Mutex::new(Ring {
                    sent: 0,
                    held: VecDeque::new(),
                    bytes: 0,
                    budget: ring_bytes,
                }),
            }),
        }
    }

    /// Start polling `state` every `poll`; the ring holds at most `ring_bytes` of decoded
    /// blocks (M-3).
    pub fn spawn(state: Arc<ApiState>, poll: Duration, ring_bytes: usize) -> BlockFeed {
        let feed = BlockFeed::detached_with(ring_bytes);
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
        self.send(item);
    }

    /// Send `item`, within the ring's byte budget (M-3).
    fn send(&self, item: FeedItem) {
        let mut ring = self.inner.ring.lock().unwrap_or_else(|e| e.into_inner());
        let block = match &item {
            FeedItem::Block(b) => Some((Arc::downgrade(b), block_bytes(b))),
            _ => None,
        };
        if let Some((_, need)) = &block {
            ring.make_room(&self.inner.tx, *need);
        }
        if self.inner.tx.send(item).is_ok() {
            let pos = ring.sent;
            ring.sent += 1;
            if let Some((w, need)) = block {
                ring.held.push_back((pos, w, need));
            }
        }
        ring.prune();
        kdx_metrics::get()
            .graphql_feed_ring_bytes
            .set(i64::try_from(ring.bytes).unwrap_or(i64::MAX));
    }

    /// Approximate bytes of decoded blocks the ring holds (M-3).
    pub fn ring_bytes(&self) -> usize {
        let mut ring = self.inner.ring.lock().unwrap_or_else(|e| e.into_inner());
        ring.prune();
        ring.bytes
    }

    /// Drop the cached blocks of `chain` above `height` (all of them with `None`): they are
    /// no longer canonical after a rollback, a reorg or a gap.
    fn forget_above(&self, chain: u8, height: Option<u64>) {
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
        for (c, h, _) in tips.iter().filter(|t| !skipped.contains(&t.0)) {
            t.insert(*c, u64::from(*h));
        }
    }
    for item in items {
        feed.send(item);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{self, hash};

    /// A block of `chain` at `height` with one tx whose output holds `bytes` of text.
    fn big_block(chain: u8, height: u64, bytes: usize) -> Arc<FeedBlock> {
        let b = testutil::feed_block(chain, height, 0);
        let tx = TxData {
            rec: kdx_types::record::TxRecord {
                key: kdx_types::TxKey::new(height as u32, chain, 1),
                ..Default::default()
            },
            block: b.block.clone(),
            out: serde_json::Value::String("x".repeat(bytes)),
            cmd: None,
            evs: Vec::new(),
            trs: Vec::new(),
            nfts: Vec::new(),
        };
        Arc::new(FeedBlock {
            chain,
            height,
            hash: b.hash,
            block: b.block.clone(),
            txs: Arc::new(vec![Arc::new(tx)]),
        })
    }

    /// M-3: a receiver that never reads holds at most the ring's byte budget of decoded
    /// blocks (counted by the blocks still alive, not only the estimate); it gets the lag
    /// error, and a receiver that keeps up gets every block and never lags.
    #[test]
    fn a_stalled_receiver_holds_at_most_the_ring_budget() {
        use tokio::sync::broadcast::error::TryRecvError;
        const MB: usize = 1 << 20;
        let feed = BlockFeed::detached();
        let mut stalled = feed.subscribe();
        let mut live = feed.subscribe();
        let mut weak = Vec::new();
        let mut got = Vec::new();
        for h in 0..100u64 {
            let b = big_block(3, h, MB);
            weak.push(Arc::downgrade(&b));
            feed.inject(FeedItem::Block(b));
            loop {
                match live.try_recv() {
                    Ok(FeedItem::Block(b)) => got.push(b.height),
                    Ok(_) => {}
                    Err(TryRecvError::Empty) => break,
                    Err(e) => panic!("the live receiver lagged at {h}: {e:?}"),
                }
            }
            let alive = weak.iter().filter(|w| w.strong_count() > 0).count();
            assert!(
                alive * MB <= RING_BYTES,
                "{alive} blocks of 1 MB alive at {h}"
            );
            assert!(feed.ring_bytes() <= RING_BYTES, "{}", feed.ring_bytes());
        }
        assert_eq!(got, (0..100).collect::<Vec<_>>());
        assert!(matches!(stalled.try_recv(), Err(TryRecvError::Lagged(_))));
        // A block over the whole budget leaves the ring holding only itself.
        let huge = big_block(3, 100, RING_BYTES + MB);
        let w = Arc::downgrade(&huge);
        feed.inject(FeedItem::Block(huge));
        assert_eq!(weak.iter().filter(|w| w.strong_count() > 0).count(), 0);
        assert!(w.strong_count() > 0);
        drop((stalled, live));
        feed.inject(FeedItem::Block(big_block(3, 101, MB)));
        assert_eq!(feed.ring_bytes(), 0);
    }

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
