//! Sushi DEX history fill (`kdx_hot::dex`, stock-gap B2).
//!
//! Once per store (`Meta dex_filled`), `run` walks the `evt` postings of the Sushi event names
//! (`kdx_hot::dex::EVENT_NAMES`), hot tail first and then the segments (oldest first), and
//! stores each tx's trusted Sushi events with `kdx_hot::dex::apply_tx`. On a store that
//! predates the DEX tables this builds them from all of chain 2's history (no height floor, as
//! stock: the first real `CREATE_PAIR` is at 6,121,854, and the postings already limit the walk
//! to txs with Sushi event names); on a fresh reindex it runs when the tail starts, like the price fill, and new tail blocks
//! store their own events (`kdx_hot::tail::apply`). No node calls: only stored records.
//!
//! - **Exact beside the live tail.** The stored DEX state is a function of the set of events
//!   stored (`kdx_hot::dex` module doc), and storing an event twice is a no-op (`DexByTx`), so
//!   the fill's order and overlaps with the tail do not matter. A hot tx is read in the write
//!   txn that stores its events: a tail unwind runs wholly before (the tx is gone and is
//!   skipped) or after (it finds the rows through `DexByTx` and removes them, whoever wrote
//!   them). The segments are opened after the hot pass, so a block frozen meanwhile is read
//!   there; frozen blocks never unwind.
//! - **Bounded.** Batches of [`BATCH`] txs per write txn; at most
//!   `kdx_hot::dex::MAX_EVENTS_PER_TX` events per tx.
//! - **Resumable.** After each segment batch the last TxNum done is stored in the same write
//!   txn (`Meta dex_fill_txnum`); a restarted fill redoes the hot pass and resumes after it.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use kdx_decode::Family;
use kdx_hot::dex::{self, meta};
use kdx_hot::tables::{block_key, parse_txkey};
use kdx_hot::{checkpoint, HotDict, HotStore, Stage, Table, Txn};
use kdx_segment::Segments;
use kdx_types::record::{decode_block, decode_tx, TxRecord};
use kdx_types::{Dict, DictKind, MemDict, TxKey};
use serde::Serialize;

/// Txs per write txn.
pub const BATCH: usize = 500;

#[derive(Debug, Default, Serialize)]
pub struct DexFillReport {
    /// Sushi event names interned in the store.
    pub event_names: u64,
    pub hot_txs: u64,
    pub frozen_txs: u64,
    /// Segment txs skipped because an earlier, stopped fill already did them.
    pub resumed_after: Option<u64>,
    /// Events stored, already present, and trusted but not stored (malformed or capped).
    pub stored: u64,
    pub present: u64,
    pub rejected: u64,
    /// Txs whose block header was not found (not stored).
    pub no_header: u64,
    pub filled_height: Option<u32>,
    pub stopped: bool,
    pub secs: f64,
}

impl DexFillReport {
    fn add(&mut self, st: dex::ApplyStats) {
        self.stored += st.stored;
        self.present += st.present;
        self.rejected += st.rejected;
    }
}

/// Whether the fill has finished on this store.
pub fn filled(txn: &impl Txn) -> Result<bool> {
    Ok(txn.meta(meta::DEX_FILLED)?.is_some())
}

/// EventName ids of the Sushi events.
fn dex_names(dict: &MemDict) -> Vec<u32> {
    dex::EVENT_NAMES
        .iter()
        .filter_map(|n| dict.find(DictKind::EventName, n))
        .collect()
}

fn in_range(k: TxKey) -> bool {
    k.chain() == dex::SUSHI_CHAIN
}

/// The hot pass's batch: each tail tx and its header are read inside the write txn that stores
/// its events.
fn apply_hot(
    hot: &HotStore,
    dict: &dyn Dict,
    keys: &[TxKey],
    rep: &mut DexFillReport,
) -> Result<()> {
    let mut txn = hot.rw()?;
    for k in keys {
        let Some(b) = txn.get(Table::TailTxs, &k.to_be_bytes())? else {
            continue;
        };
        let r = decode_tx(&b).context("hot tx record")?;
        rep.hot_txs += 1;
        let Some(h) = txn.get(Table::Headers, &block_key(k.chain(), k.height()))? else {
            rep.no_header += 1;
            continue;
        };
        let time = decode_block(&h).context("hot header")?.creation_time;
        rep.add(dex::apply_tx(&mut txn, dict, &r, time)?);
    }
    txn.commit()
}

fn apply_batch(
    hot: &HotStore,
    dict: &dyn Dict,
    recs: &[(u64, TxRecord)],
    cursor: Option<u64>,
    rep: &mut DexFillReport,
) -> Result<()> {
    let mut txn = hot.rw()?;
    for (time, r) in recs {
        rep.add(dex::apply_tx(&mut txn, dict, r, *time)?);
    }
    if let Some(c) = cursor {
        txn.set_meta_u64(meta::DEX_FILL_CURSOR, c)?;
    }
    txn.commit()
}

/// Run (or resume) the fill. `stop` is polled between batches.
pub fn run(hot: &HotStore, segdir: &Path, stop: &dyn Fn() -> bool) -> Result<DexFillReport> {
    let t0 = Instant::now();
    let mut rep = DexFillReport::default();
    let dict: Arc<MemDict> = Arc::new(HotDict::load(&hot.ro()?)?.mem().clone());
    let names = dex_names(&dict);
    rep.event_names = names.len() as u64;
    let stopped = |mut rep: DexFillReport| {
        rep.stopped = true;
        rep.secs = t0.elapsed().as_secs_f64();
        Ok(rep)
    };

    // Hot pass: tail txs, ascending.
    let mut keys: BTreeSet<TxKey> = BTreeSet::new();
    for &id in &names {
        let mut prefix = vec![Family::Evt as u8];
        prefix.extend_from_slice(&u64::from(id).to_be_bytes());
        for (k, _) in hot
            .ro()?
            .scan(Table::TailPostings, &prefix, |k| k.starts_with(&prefix))?
        {
            let key = parse_txkey(&k[9..]).context("posting key")?;
            if in_range(key) {
                keys.insert(key);
            }
        }
    }
    let keys: Vec<TxKey> = keys.into_iter().collect();
    for chunk in keys.chunks(BATCH) {
        apply_hot(hot, &*dict, chunk, &mut rep)?;
        if stop() {
            return stopped(rep);
        }
    }

    // Segment pass, oldest first, after the stored resume point, with a fresh dictionary
    // snapshot (names the tail interned meanwhile resolve too).
    let dict: Arc<MemDict> = Arc::new(HotDict::load(&hot.ro()?)?.mem().clone());
    let names = dex_names(&dict);
    rep.event_names = names.len() as u64;
    let resume = hot
        .ro()?
        .meta(meta::DEX_FILL_CURSOR)?
        .and_then(|v| v.as_slice().try_into().ok().map(u64::from_be_bytes));
    rep.resumed_after = resume;
    let segs = Segments::open_without_filters(segdir, dict.clone())?;
    let mut nums: Vec<u64> = names
        .iter()
        .flat_map(|&id| segs.postings(Family::Evt, u64::from(id)))
        .filter(|&t| resume.is_none_or(|r| t > r))
        .collect();
    nums.sort_unstable();
    nums.dedup();
    // One header read per block (a block holds many txs).
    let mut last_block: Option<((u8, u32), u64)> = None;
    for chunk in nums.chunks(BATCH) {
        let mut recs = Vec::with_capacity(chunk.len());
        for &t in chunk {
            let Some(k) = segs.txkey(t).filter(|k| in_range(*k)) else {
                continue;
            };
            let Some(r) = segs.tx(t)? else {
                continue;
            };
            let at = (k.chain(), k.height());
            let time = match last_block {
                Some((b, time)) if b == at => time,
                _ => match segs.block(at.0, at.1)? {
                    Some(b) => {
                        last_block = Some((at, b.creation_time));
                        b.creation_time
                    }
                    None => {
                        rep.no_header += 1;
                        continue;
                    }
                },
            };
            recs.push((time, r));
        }
        rep.frozen_txs += recs.len() as u64;
        apply_batch(hot, &*dict, &recs, chunk.last().copied(), &mut rep)?;
        if stop() {
            return stopped(rep);
        }
    }

    // Done: the highest tail height marks the fill point.
    let mut txn = hot.rw()?;
    let mut top = txn.frozen_frontier()?;
    for c in 0..kdx_segment::SLOT_WIDTH as u8 {
        if let Some((h, _)) = checkpoint(&txn, Stage::Tail, c)? {
            top = top.max(h);
        }
    }
    txn.set_meta(meta::DEX_FILLED, &top.to_be_bytes())?;
    txn.commit()?;
    rep.filled_height = Some(top);
    rep.secs = t0.elapsed().as_secs_f64();
    Ok(rep)
}

#[cfg(test)]
mod tests {
    use kdx_decode::{IndexedBlock, TxIndex};
    use kdx_hot::dex::{Kind, TABLES};
    use kdx_hot::Geometry;
    use kdx_types::record::{BlockRecord, EventRec};
    use kdx_types::{value, DictMut, Hash32};
    use serde_json::json;

    use super::*;

    type Snapshot = Vec<Vec<(Vec<u8>, Vec<u8>)>>;

    fn snapshot(hot: &HotStore) -> Snapshot {
        let txn = hot.ro().unwrap();
        TABLES
            .iter()
            .map(|&t| txn.scan(t, &[], |_| true).unwrap())
            .collect()
    }

    fn mref(ns: Option<&str>, name: &str) -> serde_json::Value {
        json!({"refSpec": [], "refName": {"namespace": ns, "name": name}})
    }

    /// A tail block at `height` with one tx holding Sushi events (CREATE_PAIR, a mint, a swap)
    /// and an `evt` posting per event name, as the decoder writes them.
    fn block(dict: &mut HotDict, height: u32) -> IndexedBlock {
        let key = TxKey::new(height, 2, 1);
        let (coin, wiza) = (mref(None, "coin"), mref(Some("free"), "wiza"));
        let mut events = Vec::new();
        let mut postings = Vec::new();
        for (kind, p) in [
            (Kind::CreatePair, json!([coin, wiza, "coin:free.wiza", "p"])),
            (Kind::Mint, json!(["coin:free.wiza", "k:a", height % 7 + 1])),
            (
                Kind::Swap,
                json!(["k:a", "k:a", 0.5, coin, height % 5 + 1, wiza]),
            ),
        ] {
            let (module, name) = kind.module_and_name();
            let id = dict.intern(DictKind::EventName, &format!("{module}.{name}"));
            events.push(EventRec {
                module: dict.intern(DictKind::Module, module),
                name: id,
                module_hash: 0,
                params: value::to_vec(&p, &*dict),
            });
            postings.push((Family::Evt, u64::from(id)));
        }
        IndexedBlock {
            block: BlockRecord {
                hash: Hash32([height as u8; 32]),
                chain: 2,
                height: u64::from(height),
                creation_time: 1_790_000_000_000_000 + u64::from(height) * 30_000_000,
                ..Default::default()
            },
            txs: vec![TxRecord {
                key,
                status_ok: true,
                events,
                ..Default::default()
            }],
            idx: vec![TxIndex {
                key,
                postings,
                pact_id: None,
                code_hash: None,
                d3: Default::default(),
                holders: Vec::new(),
            }],
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        }
    }

    /// Backfill equals ingest: blocks applied by the tail store their events; with the DEX
    /// tables emptied (a store from before them), the fill rebuilds the same bytes, is
    /// idempotent, and marks the store filled. A block unwound after the fill loses its rows.
    #[test]
    fn fill_equals_ingest() {
        let d = tempfile::tempdir().unwrap();
        let segdir = d.path().join("segments");
        std::fs::create_dir_all(&segdir).unwrap();
        let hot = HotStore::open(&d.path().join("hot"), Geometry::default()).unwrap();
        // The first real CREATE_PAIR's height (below the old, wrong floor of 6,277,684).
        let h0 = 6_121_854;
        {
            let mut txn = hot.rw().unwrap();
            let mut dict = HotDict::load(&txn).unwrap();
            for h in h0..h0 + 4 {
                let b = block(&mut dict, h);
                kdx_hot::tail::apply(&mut txn, &b, &mut dict).unwrap();
            }
            txn.commit().unwrap();
        }
        let ingest = snapshot(&hot);
        assert!(!ingest[1].is_empty());
        {
            let mut txn = hot.rw().unwrap();
            for t in TABLES {
                for (k, _) in txn.scan(t, &[], |_| true).unwrap() {
                    txn.del(t, &k).unwrap();
                }
            }
            txn.commit().unwrap();
        }
        assert!(!filled(&hot.ro().unwrap()).unwrap());
        let rep = run(&hot, &segdir, &|| false).unwrap();
        assert_eq!((rep.hot_txs, rep.stored, rep.rejected), (4, 12, 0));
        assert!(filled(&hot.ro().unwrap()).unwrap());
        assert_eq!(snapshot(&hot), ingest);
        // A second run stores nothing new.
        let rep = run(&hot, &segdir, &|| false).unwrap();
        assert_eq!((rep.stored, rep.present), (0, 12));
        assert_eq!(snapshot(&hot), ingest);
        // Rows the fill wrote unwind with their block.
        let mut txn = hot.rw().unwrap();
        assert_eq!(kdx_hot::tail::unwind_to(&mut txn, 2, h0 - 1).unwrap(), 4);
        txn.commit().unwrap();
        assert!(snapshot(&hot).iter().all(Vec::is_empty));
    }
}
