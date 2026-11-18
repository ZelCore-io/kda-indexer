//! FIX-2 T1: the `sndr` posting family (6) in the hot store, on real mainnet blocks
//! (`common/mod.rs`): the tail writes it with undo ops and unwinds it like every other
//! family; `kdx_hot::sndr::drop_family` turns a tail into exactly what a tail without the
//! family holds; `kdx_hot::sndr::backfill` turns that back into exactly what the tail writes.

mod common;

use std::collections::BTreeSet;

use common::*;
use kdx_decode::index::indexed_sender;
use kdx_decode::Family;
use kdx_hot::tables::{parse_txkey, posting_key};
use kdx_hot::undo::{self, UndoOp};
use kdx_hot::{HotStore, Table, Txn};
use kdx_types::record::decode_tx;
use kdx_types::{Dict, DictKind, TxKey};
use serde_json::Value;

/// Tables the family touches; their digests must match exactly.
const TABLES: [&str; 3] = ["TailPostings", "Undo", "TailTxs"];

fn tables(hot: &HotStore) -> Vec<(String, (usize, String))> {
    let d = digest(hot);
    TABLES
        .iter()
        .map(|t| (t.to_string(), d[*t].clone()))
        .collect()
}

/// The family-6 `TailPostings` rows as (entity, key).
fn sndr_rows(hot: &HotStore) -> BTreeSet<(u64, TxKey)> {
    let ro = hot.ro().unwrap();
    ro.scan(Table::TailPostings, &[6], |k| k[0] == 6)
        .unwrap()
        .into_iter()
        .map(|(k, _)| {
            (
                u64::from_be_bytes(k[1..9].try_into().unwrap()),
                parse_txkey(&k[9..]).unwrap(),
            )
        })
        .collect()
}

/// Every family-6 undo op as (entity, key).
fn sndr_ops(hot: &HotStore) -> BTreeSet<(u64, TxKey)> {
    let ro = hot.ro().unwrap();
    let mut out = BTreeSet::new();
    for (_, v) in ro.scan(Table::Undo, &[], |_| true).unwrap() {
        for op in undo::decode(&v).unwrap() {
            if let UndoOp::Posting {
                family: 6,
                entity,
                key,
            } = op
            {
                assert!(out.insert((entity, key)), "duplicate undo op {key:?}");
            }
        }
    }
    out
}

/// (entity, key) of every hot record with an indexed sender, from the records themselves.
fn expected(hot: &HotStore) -> BTreeSet<(u64, TxKey)> {
    let ro = hot.ro().unwrap();
    let dict = kdx_hot::HotDict::load(&ro).unwrap();
    let mut out = BTreeSet::new();
    for (_, v) in ro.scan(Table::TailTxs, &[], |_| true).unwrap() {
        let r = decode_tx(&v).unwrap();
        if let Some(s) = indexed_sender(&r).unwrap() {
            let id = dict.find(DictKind::Account, &s).unwrap();
            out.insert((id as u64, r.key));
        }
    }
    out
}

/// Pool blocks 0..n on chain 0 from height 0.
fn linear(n: usize) -> Vec<Place> {
    branch(&(0..n).collect::<Vec<_>>(), 0, 0, 0)
}

/// The tail as an older binary writes it: the same blocks through `tail::apply`, with the
/// `sndr` postings removed from the index entries.
fn apply_without_sndr(c: &kdx_ingest::Ctx, pool: &[Value], places: &[Place]) {
    // A build before FIX-2 never set the readiness key a fresh store now gets.
    let mut txn = c.hot.rw().unwrap();
    txn.del(Table::Meta, kdx_hot::env::meta::SNDR_HOT.as_bytes())
        .unwrap();
    txn.commit().unwrap();
    for p in places {
        let mut dict = c.dict.lock().unwrap();
        let mut txn = c.hot.rw().unwrap();
        let mut ib = kdx_ingest::pipeline::index(&prepare(pool, *p), &mut *dict).unwrap();
        for x in &mut ib.idx {
            x.postings.retain(|(f, _)| *f != Family::Sndr);
        }
        kdx_hot::tail::apply(&mut txn, &ib, &mut dict).unwrap();
        dict.persist(&mut txn).unwrap();
        txn.commit().unwrap();
    }
}

#[test]
fn the_tail_writes_sndr_rows_with_undo_ops_and_unwinds_them() {
    let pool = pool();
    let d = tempfile::tempdir().unwrap();
    let c = ctx(d.path(), 100, 1000, 100);
    apply_linear(&c, &pool, &linear(POOL_LEN), 5);
    let want = expected(&c.hot);
    assert!(want.len() > 20, "the pool has user txs: {}", want.len());
    assert_eq!(sndr_rows(&c.hot), want);
    assert_eq!(sndr_ops(&c.hot), want);
    // sndr ⊆ acct, row by row.
    let ro = c.hot.ro().unwrap();
    for (e, k) in &want {
        let acct = posting_key(Family::Acct as u8, *e, *k);
        assert!(ro.get(Table::TailPostings, &acct).unwrap().is_some());
    }
    drop(ro);

    // Unwind the upper half: only the lower half's rows remain; then everything.
    let mut txn = c.hot.rw().unwrap();
    kdx_hot::tail::unwind_to(&mut txn, 0, 11).unwrap();
    txn.commit().unwrap();
    let low: BTreeSet<_> = want
        .iter()
        .filter(|(_, k)| k.height() <= 11)
        .copied()
        .collect();
    assert!(!low.is_empty() && low.len() < want.len());
    assert_eq!(sndr_rows(&c.hot), low);
    assert_eq!(sndr_ops(&c.hot), low);
    // `unwind_to` keeps its target height: down to height 0 leaves only its rows.
    let mut txn = c.hot.rw().unwrap();
    kdx_hot::tail::unwind_to(&mut txn, 0, 0).unwrap();
    txn.commit().unwrap();
    let zero: BTreeSet<_> = want
        .iter()
        .filter(|(_, k)| k.height() == 0)
        .copied()
        .collect();
    assert_eq!(sndr_rows(&c.hot), zero);
}

#[test]
fn drop_family_leaves_exactly_the_tail_without_sndr() {
    let pool = pool();
    let places = linear(POOL_LEN);
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = ctx(da.path(), 100, 1000, 100);
    apply_linear(&a, &pool, &places, 1);
    let b = ctx(db.path(), 100, 1000, 100);
    apply_without_sndr(&b, &pool, &places);
    assert!(sndr_rows(&b.hot).is_empty());
    assert_ne!(tables(&a.hot), tables(&b.hot));

    let mut txn = a.hot.rw().unwrap();
    txn.set_meta_u64(kdx_hot::env::meta::SNDR_HOT, 1).unwrap();
    let st = kdx_hot::sndr::drop_family(&mut txn, Family::Sndr).unwrap();
    assert!(!kdx_hot::sndr::hot_ready(&txn).unwrap());
    txn.commit().unwrap();
    assert!(st.postings > 20 && st.undo_ops == st.postings, "{st:?}");
    assert_eq!(tables(&a.hot), tables(&b.hot));
    // The 0b5cb03 freezer loop (`Family::from_u8` of that binary knows 0..=5) accepts every
    // row, and no undo op names family 6.
    let ro = a.hot.ro().unwrap();
    for (k, _) in ro.scan(Table::TailPostings, &[], |_| true).unwrap() {
        assert!(Family::REQUIRED.get(k[0] as usize).is_some(), "{k:?}");
    }
    drop(ro);
    assert!(sndr_ops(&a.hot).is_empty());

    // A required family is refused.
    let mut txn = a.hot.rw().unwrap();
    assert!(kdx_hot::sndr::drop_family(&mut txn, Family::Acct).is_err());
}

#[test]
fn backfill_reproduces_the_tail_and_a_reorg_after_it_leaves_no_sndr_row() {
    let pool = pool();
    let places = linear(POOL_LEN);
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = ctx(da.path(), 100, 1000, 100);
    apply_linear(&a, &pool, &places, 3);
    let b = ctx(db.path(), 100, 1000, 100);
    apply_without_sndr(&b, &pool, &places);
    assert_ne!(tables(&a.hot), tables(&b.hot));

    let dict = b.dict.lock().unwrap().mem().clone();
    let mut txn = b.hot.rw().unwrap();
    assert!(!kdx_hot::sndr::hot_ready(&txn).unwrap());
    let st = kdx_hot::sndr::backfill(&mut txn, &dict).unwrap();
    assert!(kdx_hot::sndr::hot_ready(&txn).unwrap());
    txn.commit().unwrap();
    let want = expected(&a.hot);
    assert_eq!(st.inserted as usize, want.len(), "{st:?}");
    assert_eq!(st.senders as usize, want.len(), "{st:?}");
    assert_eq!(tables(&a.hot), tables(&b.hot), "backfill = the new tail");

    // Idempotent: a second pass inserts nothing and changes no byte.
    let mut txn = b.hot.rw().unwrap();
    let again = kdx_hot::sndr::backfill(&mut txn, &dict).unwrap();
    txn.commit().unwrap();
    assert_eq!((again.inserted, again.undo_rows), (0, 0), "{again:?}");
    assert_eq!(tables(&a.hot), tables(&b.hot));

    // A reorg after the pass: the losing blocks' family-6 rows go with them, and the
    // winner's are written by the tail.
    let fork = 9u32;
    let winner = branch(&[20, 21, 22, 23, 0, 1], fork + 1, 1, 0);
    for c in [&a, &b] {
        apply_fork(c, &pool, fork, POOL_LEN as u32 - 1 - fork, &winner);
    }
    let rows = sndr_rows(&b.hot);
    assert_eq!(rows, expected(&b.hot));
    assert!(rows
        .iter()
        .all(|(_, k)| k.height() <= fork + winner.len() as u32));
    assert_eq!(tables(&a.hot), tables(&b.hot));
    // Unwinding everything above height 0 leaves only height 0's rows.
    let mut txn = b.hot.rw().unwrap();
    kdx_hot::tail::unwind_to(&mut txn, 0, 0).unwrap();
    txn.commit().unwrap();
    assert!(sndr_rows(&b.hot).iter().all(|(_, k)| k.height() == 0));
    assert_eq!(sndr_rows(&b.hot), expected(&b.hot));
}

#[test]
fn backfill_refuses_a_sender_missing_from_the_dictionary() {
    let pool = pool();
    let d = tempfile::tempdir().unwrap();
    let c = ctx(d.path(), 100, 1000, 100);
    apply_without_sndr(&c, &pool, &linear(4));
    let mut txn = c.hot.rw().unwrap();
    let empty = kdx_types::MemDict::new();
    let e = kdx_hot::sndr::backfill(&mut txn, &empty).unwrap_err();
    assert!(format!("{e:#}").contains("not in the dictionary"), "{e:#}");
}

/// Frozen `sndr` postings of every entry as (entity, TxKey).
fn frozen_sndr(c: &kdx_ingest::Ctx) -> BTreeSet<(u64, TxKey)> {
    let dict = c.dict.lock().unwrap().mem().clone();
    let seg = kdx_segment::Segments::open(&c.segdir, std::sync::Arc::new(dict)).unwrap();
    assert!(seg.sender_ready());
    let mut got = BTreeSet::new();
    for r in &seg.ranges {
        let p = r.sndr.as_ref().expect("every step has sndr.pst");
        for i in 0..p.entities() {
            let e = p.entity_at(i);
            for rel in p.list_at(i).unwrap() {
                let key = r.txkey_rel(u64::from(rel)).unwrap();
                assert!(got.insert((e, key)), "duplicate {key:?}");
            }
        }
    }
    // The same through the reader's family accessor.
    let n: usize = got
        .iter()
        .map(|(e, _)| *e)
        .collect::<BTreeSet<_>>()
        .iter()
        .flat_map(|e| seg.postings(Family::Sndr, *e))
        .count();
    assert_eq!(n, got.len());
    got
}

/// A freeze moves the hot family-6 rows into the steps' `sndr` postings: the frozen
/// (entity, TxKey) set equals the twin's hot rows below the frontier. Both freezer paths:
/// a fresh store has `Meta sndr_hot = 1` from its init, so the freezer takes the hot rows;
/// with the key cleared (a tail that never ran the sender backfill) it derives them from
/// the records. Both give the same postings and byte-identical step files.
#[test]
fn a_freeze_moves_sndr_rows_into_the_step() {
    let pool = pool();
    let places = places_for(&[0], 12);
    let db = tempfile::tempdir().unwrap();
    // Twin store, never frozen: its hot rows are the expectation.
    let t = ctx(db.path(), 100, 1000, 100);
    apply_linear(&t, &pool, &places[0].1, 1);
    let hot_rows = sndr_rows(&t.hot);
    let mut steps = Vec::new();
    for hot_path in [true, false] {
        let da = tempfile::tempdir().unwrap();
        let c = ctx(da.path(), 2, 1000, 2);
        let ready = kdx_hot::sndr::hot_ready(&c.hot.ro().unwrap()).unwrap();
        assert!(ready, "a fresh store is ready");
        if !hot_path {
            let mut txn = c.hot.rw().unwrap();
            txn.del(Table::Meta, kdx_hot::env::meta::SNDR_HOT.as_bytes())
                .unwrap();
            txn.commit().unwrap();
        }
        apply_linear(&c, &pool, &places[0].1, 1);
        while kdx_ingest::freezer::tick(&c).unwrap().is_some() {}
        let frontier = c.hot.ro().unwrap().frozen_frontier().unwrap();
        assert!(frontier >= 4, "{frontier}");
        let want: BTreeSet<_> = hot_rows
            .iter()
            .filter(|(_, k)| k.height() < frontier)
            .copied()
            .collect();
        assert!(!want.is_empty());
        // The frozen rows left the hot store.
        assert!(sndr_rows(&c.hot)
            .iter()
            .all(|(_, k)| k.height() >= frontier));
        assert_eq!(frozen_sndr(&c), want, "hot path {hot_path}");
        steps.push(seg_files(&c.segdir));
    }
    assert_eq!(
        steps[0], steps[1],
        "hot path and derive path write the same bytes"
    );
}

/// FIX-2 fix 2: the freezer derives every step's `sndr` postings from the records, never
/// from the hot family-6 rows. A store whose rows are gone while `Meta sndr_hot = 1` says
/// ready (a rollback that skipped `hot drop-family sndr`) still freezes complete `sndr`
/// files, byte for byte those of a store whose rows are intact.
#[test]
fn the_freezer_derives_sndr_even_when_the_hot_rows_are_gone() {
    let mut stores = Vec::new();
    for drop_rows in [false, true] {
        let pool = pool();
        let places = places_for(&[0], 12);
        let d = tempfile::tempdir().unwrap();
        let c = ctx(d.path(), 2, 1000, 2);
        apply_linear(&c, &pool, &places[0].1, 1);
        if drop_rows {
            let mut txn = c.hot.rw().unwrap();
            kdx_hot::sndr::drop_family(&mut txn, Family::Sndr).unwrap();
            txn.set_meta_u64(kdx_hot::env::meta::SNDR_HOT, 1).unwrap();
            txn.commit().unwrap();
        }
        while kdx_ingest::freezer::tick(&c).unwrap().is_some() {}
        let seg =
            kdx_segment::Segments::open(&c.segdir, std::sync::Arc::new(kdx_types::MemDict::new()))
                .unwrap();
        assert!(seg.ranges.len() >= 2);
        for r in &seg.ranges {
            assert!(
                r.sndr.as_ref().unwrap().entities() > 0,
                "rows dropped {drop_rows}"
            );
        }
        stores.push(seg_files(&c.segdir));
    }
    assert_eq!(
        stores[0], stores[1],
        "the hot rows do not change the frozen bytes"
    );
}

/// blake3 of every file in a segments dir by name.
fn seg_files(dir: &std::path::Path) -> std::collections::BTreeMap<String, String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_file())
        .map(|p| {
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                kdx_segment::files::blake3_file(&p).unwrap(),
            )
        })
        .collect()
}
