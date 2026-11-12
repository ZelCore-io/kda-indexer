//! Store fixtures for unit tests: a temporary hot store with blocks applied and unwound the
//! way the tail writer does, and feed blocks that match them.

use std::sync::Arc;
use std::time::Duration;

use kdx_api::{Api, ApiSettings, ApiState, Store, TipWatch};
use kdx_decode::IndexedBlock;
use kdx_hot::{HotDict, HotStore};
use kdx_types::record::BlockRecord;
use kdx_types::Hash32;

use crate::feed::FeedBlock;

pub struct Fixture {
    _dir: tempfile::TempDir,
    hot: Arc<HotStore>,
    pub st: Arc<ApiState>,
}

/// An empty store behind an `ApiState`.
pub fn store() -> Fixture {
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
pub fn hash(chain: u8, height: u64, fork: u8) -> Hash32 {
    let mut h = [0u8; 32];
    h[0] = chain;
    h[1..9].copy_from_slice(&height.to_be_bytes());
    h[9] = fork;
    Hash32(h)
}

fn record(chain: u8, height: u64, fork: u8) -> BlockRecord {
    BlockRecord {
        hash: hash(chain, height, fork),
        chain,
        height,
        ..BlockRecord::default()
    }
}

/// Block `(chain, height)` of fork `fork` as the feed would carry it (no txs).
pub fn feed_block(chain: u8, height: u64, fork: u8) -> Arc<FeedBlock> {
    let b = record(chain, height, fork);
    Arc::new(FeedBlock {
        chain,
        height,
        hash: b.hash,
        block: Arc::new(b),
        txs: Arc::new(Vec::new()),
    })
}

impl Fixture {
    /// Store block `(chain, height)` of fork `fork` (no txs) as the new tip of `chain`.
    pub fn apply(&self, chain: u8, height: u64, fork: u8) {
        let mut w = self.hot.rw().unwrap();
        let mut dict = HotDict::load(&w).unwrap();
        let b = IndexedBlock {
            block: record(chain, height, fork),
            txs: Vec::new(),
            idx: Vec::new(),
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        };
        kdx_hot::tail::apply(&mut w, &b, &mut dict).unwrap();
        w.commit().unwrap();
    }

    /// Undo every block of `chain` above `height` (a reorg's rollback).
    pub fn unwind(&self, chain: u8, height: u32) {
        let mut w = self.hot.rw().unwrap();
        kdx_hot::tail::unwind_to(&mut w, chain, height).unwrap();
        w.commit().unwrap();
    }
}
