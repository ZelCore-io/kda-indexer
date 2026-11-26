//! `ChainCounters` (`chaincount.rs`): apply and unwind keep the per-chain user-tx count and
//! gas sum exact, a reorg removes the orphaned gas, and the tip guard distrusts counters a
//! build without them moved.

use kdx_decode::{IndexedBlock, TxIndex};
use kdx_hot::chaincount::{self, parse_price, Counts, Kda, Tally};
use kdx_hot::{set_checkpoint, Geometry, HotDict, HotStore, Stage};
use kdx_types::record::{BlockRecord, TxRecord};
use kdx_types::{Hash32, TxKey};

fn tx(chain: u8, height: u32, pos: u16, gas: u64, price: &str) -> TxRecord {
    TxRecord {
        key: TxKey::new(height, chain, pos),
        cmd: (pos > 0)
            .then(|| format!(r#"{{"meta":{{"gasPrice":{price},"sender":"a"}}}}"#).into_bytes()),
        gas,
        ..Default::default()
    }
}

/// A block on `chain` at `height`: a coinbase plus user txs `(gas, gasPrice JSON)`.
fn block(chain: u8, height: u32, fork: u8, users: &[(u64, &str)]) -> IndexedBlock {
    let mut txs = vec![tx(chain, height, 0, 0, "0")];
    for (i, (g, p)) in users.iter().enumerate() {
        txs.push(tx(chain, height, i as u16 + 1, *g, p));
    }
    let idx = txs
        .iter()
        .map(|t| TxIndex {
            key: t.key,
            postings: vec![],
            pact_id: None,
            code_hash: None,
            d3: Default::default(),
        })
        .collect();
    IndexedBlock {
        block: BlockRecord {
            hash: Hash32([fork ^ height as u8 ^ (chain << 4); 32]),
            chain,
            height: u64::from(height),
            ..Default::default()
        },
        txs,
        idx,
        cmd_reencoded: 0,
        unindexed_accounts: 0,
    }
}

fn kda(s: &str) -> Kda {
    parse_price(s).unwrap()
}

fn apply(hot: &HotStore, b: &IndexedBlock) {
    let mut txn = hot.rw().unwrap();
    let mut dict = HotDict::load(&txn).unwrap();
    kdx_hot::tail::apply(&mut txn, b, &mut dict).unwrap();
    txn.commit().unwrap();
}

fn counts(hot: &HotStore, chain: u8) -> Counts {
    chaincount::get(&hot.ro().unwrap(), chain).unwrap()
}

#[test]
fn new_store_is_ready_and_apply_unwind_stay_exact() {
    let dir = tempfile::tempdir().unwrap();
    let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
    assert!(chaincount::ready(&hot.ro().unwrap()).unwrap());

    let b10 = block(0, 10, 0, &[(600, "1.0e-8"), (1000, "\"0.00000001\"")]);
    let b11 = block(0, 11, 0, &[(150_000, "0.000001")]);
    let c1 = block(
        1,
        10,
        0,
        &[(700, "{\"decimal\":\"1e-7\"}"), (5, "\"garbage\"")],
    );
    for b in [&b10, &b11, &c1] {
        apply(&hot, b);
    }
    let want0 = Counts {
        user_txs: 3,
        // 600e-8 + 1000e-8 + 150000e-6 = 0.150016
        gas: kda("0.150016"),
    };
    assert_eq!(counts(&hot, 0), want0);
    assert_eq!(
        counts(&hot, 1),
        Counts {
            user_txs: 2,
            gas: kda("0.00007")
        }
    );
    assert!(chaincount::ready(&hot.ro().unwrap()).unwrap());
    assert_eq!(counts(&hot, 0).gas.to_stock_text(), "0.1500160000");

    // Reorg: unwind chain 0 to 10 and apply a fork block with other gas.
    let mut txn = hot.rw().unwrap();
    assert_eq!(kdx_hot::tail::unwind_to(&mut txn, 0, 10).unwrap(), 1);
    txn.commit().unwrap();
    assert_eq!(
        counts(&hot, 0),
        Counts {
            user_txs: 2,
            gas: kda("0.000016")
        }
    );
    apply(&hot, &block(0, 11, 9, &[(1, "0.1"), (2, "0.1")]));
    assert_eq!(
        counts(&hot, 0),
        Counts {
            user_txs: 4,
            gas: kda("0.300016")
        }
    );
    assert!(chaincount::ready(&hot.ro().unwrap()).unwrap());

    // Unwinding everything leaves no row, and an in-memory tally of what is left agrees.
    let mut txn = hot.rw().unwrap();
    kdx_hot::tail::unwind_to(&mut txn, 0, 9).unwrap();
    txn.commit().unwrap();
    assert_eq!(counts(&hot, 0), Counts::default());
    let mut t = Tally::default();
    for tx in &c1.txs {
        t.add(tx);
    }
    assert_eq!(t.unpriced, 1);
    let ro = hot.ro().unwrap();
    assert_eq!(chaincount::differing(&ro, &t).unwrap(), 0);
    assert!(chaincount::ready(&ro).unwrap());
}

#[test]
fn tip_moved_without_stamp_is_not_trusted() {
    let dir = tempfile::tempdir().unwrap();
    let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
    apply(&hot, &block(2, 5, 0, &[(10, "0.01")]));
    // A build with PR #48 but without chain counters applies height 6: the tip and the row
    // counts' stamp move, the chain counters' stamp does not.
    let mut txn = hot.rw().unwrap();
    set_checkpoint(&mut txn, Stage::Tail, 2, 6, Hash32([6; 32])).unwrap();
    kdx_hot::rowcount::stamp(&mut txn, 2).unwrap();
    txn.commit().unwrap();
    assert!(kdx_hot::rowcount::tip_mismatch(&hot.ro().unwrap())
        .unwrap()
        .is_none());
    let ro = hot.ro().unwrap();
    assert!(chaincount::marker_set(&ro).unwrap());
    assert!(
        !chaincount::ready(&ro).unwrap(),
        "readers distrust it at once"
    );
    drop(ro);
    // The next apply runs the guard first and clears the marker; it is not stamped over.
    let mut txn = hot.rw().unwrap();
    let mut dict = HotDict::load(&txn).unwrap();
    kdx_hot::tail::apply(&mut txn, &block(2, 7, 0, &[(1, "1")]), &mut dict).unwrap();
    txn.commit().unwrap();
    let ro = hot.ro().unwrap();
    assert!(!chaincount::marker_set(&ro).unwrap());
    assert!(!chaincount::ready(&ro).unwrap());
}

#[test]
fn hostile_prices_never_panic_and_overflow_clears_the_marker() {
    let dir = tempfile::tempdir().unwrap();
    let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
    apply(
        &hot,
        &block(
            3,
            1,
            0,
            &[
                (u64::MAX, "-5"),
                (u64::MAX, "\"1e99999999999999999999\""),
                (u64::MAX, "null"),
                (u64::MAX, "[1]"),
            ],
        ),
    );
    // Malformed or negative prices add 0 KDA; the txs still count.
    assert_eq!(
        counts(&hot, 3),
        Counts {
            user_txs: 4,
            gas: Kda::ZERO
        }
    );
    assert!(chaincount::ready(&hot.ro().unwrap()).unwrap());
    // A product past u128 whole KDA: counted, not served.
    apply(
        &hot,
        &block(
            3,
            2,
            0,
            &[(u64::MAX, "340282366920938463463374607431768211455")],
        ),
    );
    let ro = hot.ro().unwrap();
    assert_eq!(counts(&hot, 3).user_txs, 5);
    assert!(!chaincount::marker_set(&ro).unwrap());
    drop(ro);
    // Its unwind does not panic either.
    let mut txn = hot.rw().unwrap();
    kdx_hot::tail::unwind_to(&mut txn, 3, 1).unwrap();
    txn.commit().unwrap();
    assert_eq!(counts(&hot, 3).user_txs, 4);
}

/// Review I-2: a build without the frontier stamps adds a backfill range (or freezes one):
/// `next_txnum` moves but no tail tip does. Neither the chain counters nor the row counts are
/// trusted, and the next write's guard clears both markers. A healthy store (marker set by a
/// reconcile, then a tail block) stays ready.
#[test]
fn txnum_moved_without_stamp_is_not_trusted() {
    let dir = tempfile::tempdir().unwrap();
    let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
    apply(&hot, &block(2, 5, 0, &[(10, "0.01")]));
    {
        // What `reconcile-counts` writes for the row counts.
        let mut txn = hot.rw().unwrap();
        kdx_hot::rowcount::set_ready(&mut txn, 0).unwrap();
        kdx_hot::rowcount::stamp_all(&mut txn).unwrap();
        txn.commit().unwrap();
    }
    let ro = hot.ro().unwrap();
    assert!(chaincount::ready(&ro).unwrap());
    assert!(kdx_hot::rowcount::ready(&ro).unwrap());
    drop(ro);
    // An older build commits a backfill range: next_txnum and the frozen frontier move.
    let mut txn = hot.rw().unwrap();
    txn.set_next_txnum(40).unwrap();
    txn.set_frozen_frontier(5).unwrap();
    txn.commit().unwrap();
    let ro = hot.ro().unwrap();
    assert!(chaincount::marker_set(&ro).unwrap());
    assert!(!chaincount::ready(&ro).unwrap(), "NOT_AVAILABLE");
    assert!(kdx_hot::rowcount::marker_set(&ro).unwrap());
    assert!(!kdx_hot::rowcount::ready(&ro).unwrap(), "lists walk");
    drop(ro);
    // The guards (startup, every apply and unwind) clear both markers rather than
    // stamping over the gap.
    let mut txn = hot.rw().unwrap();
    assert!(kdx_hot::rowcount::guard(&mut txn).unwrap().is_some());
    txn.commit().unwrap();
    apply(&hot, &block(2, 6, 0, &[(1, "1")]));
    let ro = hot.ro().unwrap();
    assert!(!chaincount::marker_set(&ro).unwrap());
    assert!(!kdx_hot::rowcount::marker_set(&ro).unwrap());
}

/// Review M-2: a damaged row is never served as zero: the counters are not ready, and the
/// guard clears the marker.
#[test]
fn damaged_row_is_not_served() {
    let dir = tempfile::tempdir().unwrap();
    let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
    apply(&hot, &block(2, 5, 0, &[(10, "0.01")]));
    apply(&hot, &block(3, 5, 0, &[(10, "0.01")]));
    let mut txn = hot.rw().unwrap();
    txn.put(kdx_hot::Table::ChainCounters, &[3], &[1, 2, 3])
        .unwrap();
    txn.commit().unwrap();
    assert!(!chaincount::ready(&hot.ro().unwrap()).unwrap());
    let mut txn = hot.rw().unwrap();
    assert!(chaincount::guard(&mut txn).unwrap().is_some());
    txn.commit().unwrap();
    assert!(!chaincount::marker_set(&hot.ro().unwrap()).unwrap());
}
