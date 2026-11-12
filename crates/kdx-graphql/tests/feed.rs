//! The block feed end to end (plan Task 14, Review Focus 5): a real store, the real polling
//! feed and the real subscriptions, driven through a scripted reorg, a freezer step and a
//! deeper reorg. `newBlocks` emits every canonical block once; `newBlocksFromDepth` never
//! emits an orphan and emits each released canonical block once.
//!
//! The store is built the way the tail writer builds it (blocks without txs: the feed and
//! the gate care about heights and hashes). The freezer step is the store-side half of a
//! freeze: the hot frontier moves up, so heights below it are no longer served from hot
//! rows. The byte-level freeze (segments written, hot rows dropped) is covered by the
//! kdx-ingest freeze tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_graphql::Request;
use futures::StreamExt;
use kdx_api::{Api, ApiSettings, ApiState, Store, TipWatch};
use kdx_decode::IndexedBlock;
use kdx_graphql::ws::active_subscriptions;
use kdx_graphql::{build_schema, BlockFeed, GraphqlSettings};
use kdx_hot::{HotDict, HotStore};
use kdx_types::record::BlockRecord;
use kdx_types::Hash32;

struct Fixture {
    _dir: tempfile::TempDir,
    hot: Arc<HotStore>,
    st: Arc<ApiState>,
}

fn fixture() -> Fixture {
    let d = tempfile::tempdir().unwrap();
    let g = kdx_hot::Geometry {
        max_gb: 1,
        growth_mb: 16,
    };
    let hot = Arc::new(HotStore::open(&d.path().join("hot"), g).unwrap());
    let store = Arc::new(Store::open(hot.clone(), d.path()).unwrap());
    let settings = ApiSettings {
        network: "mainnet01".into(),
        deadline: Duration::from_secs(5),
        max_in_flight: 16,
        max_body_bytes: 8 << 20,
        v1txs_max_body_bytes: 8 << 20,
        rate_per_ip: 20,
        rate_burst: 40,
        trusted_proxies: Vec::new(),
        read_threads: 2,
        ready_max_lag: 10,
        max_event_lists: 64,
    };
    let st = Api::new(store, settings, TipWatch::default()).state();
    Fixture { _dir: d, hot, st }
}

/// The hash of block `(chain, height)` on fork `fork`.
fn hash(chain: u8, height: u64, fork: u8) -> Hash32 {
    let mut h = [0u8; 32];
    h[0] = chain;
    h[1..9].copy_from_slice(&height.to_be_bytes());
    h[9] = fork;
    Hash32(h)
}

impl Fixture {
    fn apply(&self, chain: u8, height: u64, fork: u8) {
        let mut w = self.hot.rw().unwrap();
        let mut dict = HotDict::load(&w).unwrap();
        let b = IndexedBlock {
            block: BlockRecord {
                hash: hash(chain, height, fork),
                chain,
                height,
                ..BlockRecord::default()
            },
            txs: Vec::new(),
            idx: Vec::new(),
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        };
        kdx_hot::tail::apply(&mut w, &b, &mut dict).unwrap();
        w.commit().unwrap();
    }

    fn apply_run(&self, chain: u8, heights: std::ops::RangeInclusive<u64>, fork: u8) {
        for h in heights {
            self.apply(chain, h, fork);
        }
    }

    /// A reorg's rollback: undo every block of `chain` above `height`.
    fn unwind(&self, chain: u8, height: u32) {
        let mut w = self.hot.rw().unwrap();
        kdx_hot::tail::unwind_to(&mut w, chain, height).unwrap();
        w.commit().unwrap();
    }

    /// The freezer step: heights below `height` leave the hot rows.
    fn freeze_below(&self, height: u32) {
        let mut w = self.hot.rw().unwrap();
        w.set_frozen_frontier(height).unwrap();
        w.commit().unwrap();
    }
}

async fn until(what: &str, f: impl Fn() -> bool) {
    let end = Instant::now() + Duration::from_secs(15);
    while Instant::now() < end {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

/// What a subscription delivered: `(chain, height, hash)` in order, and any error message.
#[derive(Default)]
struct Seen {
    blocks: Vec<(u8, u64, String)>,
    errors: Vec<String>,
}

struct Run {
    fx: Fixture,
    feed: BlockFeed,
    seen: Arc<Mutex<Seen>>,
}

impl Run {
    /// A store with blocks 0..=3 of fork 0 on chains 0 and 1, the polling feed on it, and
    /// `subscription` running.
    async fn start(subscription: &str) -> Run {
        let fx = fixture();
        for c in [0u8, 1] {
            fx.apply_run(c, 0..=3, 0);
        }
        let feed = BlockFeed::spawn(fx.st.clone(), Duration::from_millis(50));
        let schema = build_schema(
            fx.st.clone(),
            GraphqlSettings::default(),
            Some(feed.clone()),
        );
        until("the feed to see the first tips", || {
            feed.tips() == BTreeMap::from([(0, 3), (1, 3)])
        })
        .await;
        let seen = Arc::new(Mutex::new(Seen::default()));
        let sink = seen.clone();
        let mut stream = schema.execute_stream(Request::new(subscription));
        tokio::spawn(async move {
            while let Some(r) = stream.next().await {
                let mut s = sink.lock().unwrap();
                for e in &r.errors {
                    s.errors.push(e.message.clone());
                }
                let v = r.data.into_json().unwrap();
                for (_, list) in v.as_object().into_iter().flatten() {
                    for b in list.as_array().into_iter().flatten() {
                        s.blocks.push((
                            b["chainId"].as_u64().unwrap() as u8,
                            b["height"].as_u64().unwrap(),
                            b["hash"].as_str().unwrap().to_owned(),
                        ));
                    }
                }
            }
        });
        until("the subscription to open", || {
            active_subscriptions(&schema) == 1
        })
        .await;
        Run { fx, feed, seen }
    }

    /// Wait until the feed holds block `(chain, height)` of `fork` as its tip.
    async fn absorbed(&self, chain: u8, height: u64, fork: u8) {
        until("the feed to absorb a block", || {
            self.feed.tips().get(&chain) == Some(&height)
                && self.feed.recent(chain, height).map(|b| b.hash)
                    == Some(hash(chain, height, fork))
        })
        .await;
    }

    fn blocks(&self) -> Vec<(u8, u64, String)> {
        self.seen.lock().unwrap().blocks.clone()
    }
}

fn name(chain: u8, height: u64, fork: u8) -> (u8, u64, String) {
    (chain, height, hash(chain, height, fork).to_b64url())
}

/// The scripted history. Chains 0 and 1 start at height 3 (fork 0, not emitted: the feed
/// starts at the tips). Returns the blocks that were canonical at some point and later
/// orphaned. `freeze` is the freezer step's height; `settle` runs after each stage.
///
/// 1. chain 0 grows to 5 and chain 1 to 4 (fork 0).
/// 2. reorg on chain 0, two deep: heights 4..=6 of fork 1.
/// 3. the freezer step, then chain 1 grows to 6 (fork 0) and chain 0 to 7.
/// 4. reorg on chain 1, three deep: heights 4..=7 of fork 2.
/// 5. one more block on each chain (the barrier: whatever was owed has arrived by then).
async fn script(r: &Run, freeze: u32) -> BTreeSet<(u8, u64, String)> {
    r.fx.apply_run(0, 4..=5, 0);
    r.fx.apply_run(1, 4..=4, 0);
    r.absorbed(0, 5, 0).await;
    r.absorbed(1, 4, 0).await;

    r.fx.unwind(0, 3);
    r.fx.apply_run(0, 4..=6, 1);
    r.absorbed(0, 6, 1).await;

    r.fx.freeze_below(freeze);
    r.fx.apply_run(1, 5..=6, 0);
    r.fx.apply_run(0, 7..=7, 1);
    r.absorbed(1, 6, 0).await;
    r.absorbed(0, 7, 1).await;

    r.fx.unwind(1, 3);
    r.fx.apply_run(1, 4..=7, 2);
    r.absorbed(1, 7, 2).await;

    r.fx.apply_run(0, 8..=8, 1);
    r.fx.apply_run(1, 8..=8, 2);
    r.absorbed(0, 8, 1).await;
    r.absorbed(1, 8, 2).await;
    [(0, 4), (0, 5), (1, 4), (1, 5), (1, 6)]
        .into_iter()
        .map(|(c, h)| name(c, h, 0))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_blocks_emits_every_canonical_block_once_across_a_reorg_and_a_freeze() {
    let r =
        Run::start(r#"subscription { newBlocks(chainIds: ["0", "1"]) { chainId height hash } }"#)
            .await;
    let orphans = script(&r, 3).await;
    // Chain 0 ends on fork 1 and chain 1 on fork 2: heights 4..=8 are the canonical blocks
    // that arrived after the first tips.
    let canonical: BTreeSet<_> = (4..=8)
        .map(|h| name(0, h, 1))
        .chain((4..=8).map(|h| name(1, h, 2)))
        .collect();
    until("every canonical block", || {
        let got = r.blocks();
        canonical.iter().all(|b| got.contains(b))
    })
    .await;
    let got = r.blocks();
    for b in &canonical {
        assert_eq!(got.iter().filter(|x| *x == b).count(), 1, "{b:?} not once");
    }
    // Nothing else came: no block twice, and the rest are blocks that were canonical when
    // they were sent and orphaned since.
    let distinct: BTreeSet<_> = got.iter().cloned().collect();
    assert_eq!(distinct.len(), got.len(), "a block was sent twice: {got:?}");
    for b in &got {
        assert!(
            canonical.contains(b) || orphans.contains(b),
            "unexpected block {b:?}"
        );
    }
    // Per chain, the blocks come in the order the chain grew: a replacement never precedes
    // the blocks it replaces.
    for c in [0u8, 1] {
        let forks: Vec<u8> = got
            .iter()
            .filter(|b| b.0 == c)
            .map(|b| if canonical.contains(b) { 1 } else { 0 })
            .collect();
        assert!(
            forks.windows(2).all(|w| w[0] <= w[1]),
            "chain {c}: {forks:?}"
        );
    }
    assert!(
        r.seen.lock().unwrap().errors.is_empty(),
        "{:?}",
        r.seen.lock().unwrap().errors
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_blocks_from_depth_never_emits_an_orphan() {
    let r = Run::start(
        r#"subscription { newBlocksFromDepth(chainIds: ["0", "1"], minimumDepth: 3) { chainId height hash } }"#,
    )
    .await;
    // Both reorgs of the script are at most `minimumDepth` deep. The freezer step only
    // moves heights the gate has already released.
    let orphans = script(&r, 1).await;
    // Tips end at 8, so everything up to height 5 is due: chain 0 is fork 0 up to height 3
    // and fork 1 above; chain 1 is fork 0 up to 3 and fork 2 above.
    let canon = |c: u8, h: u64| {
        name(
            c,
            h,
            if h <= 3 {
                0
            } else if c == 0 {
                1
            } else {
                2
            },
        )
    };
    until("every due block", || {
        let got = r.blocks();
        [0u8, 1].iter().all(|c| got.contains(&canon(*c, 5)))
    })
    .await;
    let got = r.blocks();
    for b in &got {
        assert!(!orphans.contains(b), "an orphan was released: {b:?}");
    }
    for c in [0u8, 1] {
        let heights: Vec<u64> = got.iter().filter(|b| b.0 == c).map(|b| b.1).collect();
        // Released heights are consecutive, each once, up to the due height. (Where the
        // run starts depends on which tick the subscription first woke on.)
        let first = heights[0];
        assert!((1..=3).contains(&first), "chain {c} started at {first}");
        assert_eq!(heights, (first..=5).collect::<Vec<_>>(), "chain {c}");
        for h in first..=5 {
            assert!(
                got.contains(&canon(c, h)),
                "chain {c} height {h} is not canonical"
            );
        }
    }
    assert!(
        r.seen.lock().unwrap().errors.is_empty(),
        "{:?}",
        r.seen.lock().unwrap().errors
    );
}
