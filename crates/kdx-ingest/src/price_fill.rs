//! Token swap history fill (`lastTokenPriceInKda`, `kdx_hot::prices`).
//!
//! Once per store (`Meta price_filled`), `run` walks the `evt` postings of the trusted DEX swap
//! events (`kdx_hot::prices::DEX_SWAP_EVENTS`), hot tail first and then the segments (oldest
//! first), and stores each swap with
//! `kdx_hot::prices::upsert` (newest per (chain, token) wins, so replaying a tx or seeing a
//! block in both the tail and the segments is harmless). No node calls and no resync: only
//! stored records are read.
//!
//! - **Bounded:** writes go in batches of [`BATCH`] txs, each with its own write txn.
//! - **Resumable:** after each segment batch the last TxNum done is stored in the same write txn
//!   (`Meta price_fill_txnum`); a restarted fill redoes the (small) hot pass and resumes the
//!   segment pass after it.
//! - New tail blocks update the table themselves (`kdx_hot::tail::apply`), with an undo log.
//!   Rows the fill writes from hot blocks carry none: a reorg of a block the fill read from the
//!   tail (blocks applied by a binary without this table) keeps its swap until a newer one.
//! - **Beside the live tail (N-14):** a hot tx is read in the write txn that stores its swap,
//!   so an unwind cannot fall between the two; and where a tail block applied during the fill
//!   holds a newer row, the fill's row goes into that block's undo log
//!   (`kdx_hot::prices::upsert_fill`), so unwinding the block leaves it, not no price.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use kdx_decode::Family;
use kdx_hot::prices::{self, meta};
use kdx_hot::tables::parse_txkey;
use kdx_hot::{checkpoint, HotDict, HotStore, Stage, Table, Txn};
use kdx_segment::Segments;
use kdx_types::record::{decode_tx, TxRecord};
use kdx_types::{Dict, DictKind, MemDict, TxKey};
use serde::Serialize;

/// Txs per write txn.
pub const BATCH: usize = 500;

#[derive(Debug, Default, Serialize)]
pub struct FillReport {
    /// Trusted DEX swap event names walked.
    pub event_names: u64,
    pub hot_txs: u64,
    pub frozen_txs: u64,
    /// Segment txs skipped because an earlier, stopped fill already did them.
    pub resumed_after: Option<u64>,
    /// Txs that were swaps, and the rows they wrote.
    pub swaps: u64,
    pub rows: u64,
    pub filled_height: Option<u32>,
    pub stopped: bool,
    pub secs: f64,
}

/// Whether the fill has finished on this store.
pub fn filled(txn: &impl Txn) -> Result<bool> {
    Ok(txn.meta(meta::PRICE_FILLED)?.is_some())
}

/// EventName ids of the trusted DEX swap events.
fn swap_names(dict: &MemDict) -> Vec<u32> {
    (0..dict.len(DictKind::EventName) as u32)
        .filter(|&id| {
            dict.name(DictKind::EventName, id)
                .is_some_and(prices::is_swap_event_name)
        })
        .collect()
}

fn apply_batch(
    hot: &HotStore,
    dict: &dyn Dict,
    recs: &[(TxKey, TxRecord)],
    cursor: Option<u64>,
    rep: &mut FillReport,
) -> Result<()> {
    let mut txn = hot.rw()?;
    for (k, r) in recs {
        if let Some(s) = prices::swap_of_tx(dict, r) {
            rep.swaps += 1;
            rep.rows += u64::from(prices::upsert_fill(&mut txn, k.chain(), &s)?);
        }
    }
    if let Some(c) = cursor {
        txn.set_meta_u64(meta::PRICE_FILL_CURSOR, c)?;
    }
    txn.commit()
}

/// The hot pass's batch: each tail tx is read inside the write txn that stores its swap
/// (N-14). Write txns are serialized, so a tail unwind runs wholly before or after: a tx it
/// removed is not read, and one still there cannot be removed before its row is written.
fn apply_hot(hot: &HotStore, dict: &dyn Dict, keys: &[TxKey], rep: &mut FillReport) -> Result<()> {
    let mut txn = hot.rw()?;
    for k in keys {
        let Some(b) = txn.get(Table::TailTxs, &k.to_be_bytes())? else {
            continue;
        };
        let r = decode_tx(&b).context("hot tx record")?;
        rep.hot_txs += 1;
        if let Some(s) = prices::swap_of_tx(dict, &r) {
            rep.swaps += 1;
            rep.rows += u64::from(prices::upsert_fill(&mut txn, k.chain(), &s)?);
        }
    }
    txn.commit()
}

/// Run (or resume) the fill. `stop` is polled between batches.
pub fn run(hot: &HotStore, segdir: &Path, stop: &dyn Fn() -> bool) -> Result<FillReport> {
    let t0 = Instant::now();
    let mut rep = FillReport::default();
    let dict: Arc<MemDict> = Arc::new(HotDict::load(&hot.ro()?)?.mem().clone());
    let names = swap_names(&dict);
    rep.event_names = names.len() as u64;
    let stopped = |mut rep: FillReport| {
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
            keys.insert(parse_txkey(&k[9..]).context("posting key")?);
        }
    }
    let keys: Vec<TxKey> = keys.into_iter().collect();
    for chunk in keys.chunks(BATCH) {
        apply_hot(hot, &*dict, chunk, &mut rep)?;
        if stop() {
            return stopped(rep);
        }
    }

    // Segment pass, oldest first, after the stored resume point. A fresh dictionary snapshot,
    // so names the tail interned meanwhile resolve too.
    let dict: Arc<MemDict> = Arc::new(HotDict::load(&hot.ro()?)?.mem().clone());
    let names = swap_names(&dict);
    rep.event_names = names.len() as u64;
    let resume = hot
        .ro()?
        .meta(meta::PRICE_FILL_CURSOR)?
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
    for chunk in nums.chunks(BATCH) {
        let mut recs = Vec::with_capacity(chunk.len());
        for &t in chunk {
            if let (Some(k), Some(r)) = (segs.txkey(t), segs.tx(t)?) {
                recs.push((k, r));
            }
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
    txn.set_meta(meta::PRICE_FILLED, &top.to_be_bytes())?;
    txn.commit()?;
    rep.filled_height = Some(top);
    rep.secs = t0.elapsed().as_secs_f64();
    Ok(rep)
}

#[cfg(test)]
mod tests {
    use kdx_types::record::EventRec;
    use kdx_types::{value, DictMut};

    use super::*;

    /// Hot pass only (no segments): the fill stores the newest swap per token, is idempotent,
    /// and marks the store filled; a non-swap event does not count.
    #[test]
    fn fills_from_the_hot_tail() {
        let d = tempfile::tempdir().unwrap();
        let segdir = d.path().join("segments");
        std::fs::create_dir_all(&segdir).unwrap();
        let hot = HotStore::open(&d.path().join("hot"), kdx_hot::Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let swap_ev = dict.intern(DictKind::EventName, "kaddex.exchange.SWAP");
        let other_ev = dict.intern(DictKind::EventName, "coin.TRANSFER");
        let module = dict.intern(DictKind::Module, "kaddex.exchange");
        for (h, ev, amt) in [
            (10u32, swap_ev, 1.0),
            (11, swap_ev, 4.0),
            (12, other_ev, 9.0),
        ] {
            let key = TxKey::new(h, 1, 1);
            let data = serde_json::json!([
                {"amount": amt, "token": "coin"}, {"amount": 2.0, "token": "free.t"}
            ]);
            let r = TxRecord {
                key,
                status_ok: true,
                cmd: Some(format!(r#"{{"meta":{{"creationTime":{h}}}}}"#).into_bytes()),
                result: value::to_vec(&data, &dict),
                events: vec![EventRec {
                    module,
                    name: ev,
                    module_hash: 0,
                    params: value::to_vec(
                        &serde_json::json!([
                            "pool", "k:user", amt,
                            {"refName": {"namespace": null, "name": "coin"}},
                            2.0,
                            {"refName": {"namespace": "free", "name": "t"}}
                        ]),
                        &dict,
                    ),
                }],
                ..Default::default()
            };
            let mut b = Vec::new();
            kdx_types::record::encode_tx(&r, &mut b);
            txn.put(Table::TailTxs, &key.to_be_bytes(), &b).unwrap();
            txn.put(
                Table::TailPostings,
                &kdx_hot::tables::posting_key(Family::Evt as u8, u64::from(ev), key),
                &[],
            )
            .unwrap();
        }
        dict.persist(&mut txn).unwrap();
        txn.commit().unwrap();

        let rep = run(&hot, &segdir, &|| false).unwrap();
        // One row per swap: the non-coin token (`coin` is never a key).
        assert_eq!((rep.hot_txs, rep.swaps, rep.rows), (2, 2, 2));
        assert!(filled(&hot.ro().unwrap()).unwrap());
        let row = prices::lookup(&hot.ro().unwrap(), "free.t")
            .unwrap()
            .unwrap();
        assert_eq!(prices::price_text(&row).as_deref(), Some("2.000000000000"));
        // Again: nothing new is written.
        let rep = run(&hot, &segdir, &|| false).unwrap();
        assert_eq!((rep.swaps, rep.rows), (2, 0));
    }

    /// A `kaddex.exchange.SWAP` tx at `h` on chain 1: `amt` KDA for 2 `free.t`.
    fn swap_tx(dict: &mut HotDict, h: u32, amt: f64) -> TxRecord {
        let ev = dict.intern(DictKind::EventName, "kaddex.exchange.SWAP");
        let module = dict.intern(DictKind::Module, "kaddex.exchange");
        let data = serde_json::json!([
            {"amount": amt, "token": "coin"}, {"amount": 2.0, "token": "free.t"}
        ]);
        TxRecord {
            key: TxKey::new(h, 1, 1),
            status_ok: true,
            cmd: Some(format!(r#"{{"meta":{{"creationTime":{h}}}}}"#).into_bytes()),
            result: value::to_vec(&data, &*dict),
            events: vec![EventRec {
                module,
                name: ev,
                module_hash: 0,
                params: value::to_vec(
                    &serde_json::json!([
                        "pool", "k:user", amt,
                        {"refName": {"namespace": null, "name": "coin"}},
                        2.0,
                        {"refName": {"namespace": "free", "name": "t"}}
                    ]),
                    &*dict,
                ),
            }],
            ..Default::default()
        }
    }

    /// N-14 (segment pass): a tail block applied during the fill stored the first row of a
    /// token, logging "no row before". The fill's older swap of the token is not stored (the
    /// tail's is newer), but unwinding that block must leave the fill's swap, not delete the
    /// token's price.
    #[test]
    fn an_unwind_during_the_fill_keeps_the_filled_swap() {
        let d = tempfile::tempdir().unwrap();
        let hot = HotStore::open(&d.path().join("hot"), kdx_hot::Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let (old, tip) = (swap_tx(&mut dict, 10, 1.0), swap_tx(&mut dict, 50, 4.0));
        dict.persist(&mut txn).unwrap();
        // The live tail applies block 50 (as `tail::apply` does).
        let mut undo = Vec::new();
        let s = prices::swap_of_tx(&dict, &tip).unwrap();
        prices::upsert(&mut txn, 1, &s, Some(&mut undo)).unwrap();
        txn.set_meta(&prices::undo_key(1, 50), &undo).unwrap();
        txn.commit().unwrap();
        // The fill's segment batch holds the swap at height 10.
        let mut rep = FillReport::default();
        apply_batch(&hot, &dict, &[(old.key, old)], Some(1), &mut rep).unwrap();
        assert_eq!(rep.rows, 0, "the tail's row is newer");
        let mut txn = hot.rw().unwrap();
        prices::undo_block(&mut txn, 1, 50).unwrap();
        txn.commit().unwrap();
        let row = prices::lookup(&hot.ro().unwrap(), "free.t").unwrap();
        assert_eq!(
            row.map(|r| r.key),
            Some(TxKey::new(10, 1, 1)),
            "the filled swap is what the unwind restores"
        );
    }

    /// N-14 (hot pass): a tail tx unwound between the fill's read and its write is not stored.
    #[test]
    fn a_hot_tx_unwound_before_the_write_is_not_stored() {
        let d = tempfile::tempdir().unwrap();
        let hot = HotStore::open(&d.path().join("hot"), kdx_hot::Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let tx = swap_tx(&mut dict, 10, 1.0);
        dict.persist(&mut txn).unwrap();
        let mut b = Vec::new();
        kdx_types::record::encode_tx(&tx, &mut b);
        txn.put(Table::TailTxs, &tx.key.to_be_bytes(), &b).unwrap();
        txn.commit().unwrap();
        let keys = [tx.key];
        // The unwind removes the block's txs before the fill writes.
        let mut txn = hot.rw().unwrap();
        txn.del(Table::TailTxs, &tx.key.to_be_bytes()).unwrap();
        txn.commit().unwrap();
        let mut rep = FillReport::default();
        apply_hot(&hot, &dict, &keys, &mut rep).unwrap();
        assert_eq!((rep.hot_txs, rep.rows), (0, 0));
        assert_eq!(prices::lookup(&hot.ro().unwrap(), "free.t").unwrap(), None);
    }
}
