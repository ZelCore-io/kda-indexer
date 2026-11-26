//! `reconcile-balances --rewalk` (PR #26 review N-6, N-7): the command path under the data-dir
//! lock. Refused while the lock is held, for an unknown name and for `coin` (nothing written);
//! otherwise it marks the listed modules' posting holders and queues their history walk, and
//! a second run marks nothing new and queues the same set.

use kda_indexer::cmd::reconcile::rewalk;
use kda_indexer::config::Config;
use kdx_decode::Family;
use kdx_hot::balances::{self, bal_key};
use kdx_hot::{HotDict, HotStore, Table, Txn};
use kdx_types::{Dict, DictKind, DictMut, TxKey};

fn cfg() -> Config {
    Config::load(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../packaging/config/mainnet01.toml"
    )))
    .unwrap()
}

fn posting(account: u32, module: u32, chain: u8) -> Vec<u8> {
    let mut k = vec![Family::Xfer as u8];
    k.extend_from_slice(&((u64::from(account) << 32) | u64::from(module)).to_be_bytes());
    k.extend_from_slice(&TxKey::new(100, chain, 1).to_be_bytes());
    k
}

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn rewalk_marks_queues_and_refuses_unknown_and_a_held_lock() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(kdx_ingest::ctx::segments_dir(dir.path())).unwrap();
    let hot_dir = kda_indexer::cmd::hot::hot_dir(dir.path());
    let (wiza, other, alice, bob) = {
        let hs = HotStore::open(&hot_dir, kda_indexer::cmd::hot::geometry(&cfg())).unwrap();
        let mut txn = hs.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let coin = dict.intern(DictKind::Module, "coin");
        let wiza = dict.intern(DictKind::Module, "free.wiza");
        let other = dict.intern(DictKind::Module, "free.other");
        let alice = dict.intern(DictKind::Account, "alice");
        let bob = dict.intern(DictKind::Account, "bob");
        dict.persist(&mut txn).unwrap();
        for k in [
            posting(bob, coin, 3),
            posting(alice, wiza, 1),
            posting(bob, wiza, 2),
            posting(alice, other, 0),
        ] {
            txn.put(Table::TailPostings, &k, &[]).unwrap();
        }
        txn.commit().unwrap();
        (wiza, other, alice, bob)
    };
    let untouched = |why: &str| {
        let hs = HotStore::open_ro(&hot_dir).unwrap();
        let txn = hs.ro().unwrap();
        assert_eq!(balances::dirty_count(&txn).unwrap(), 0, "{why}");
        assert!(balances::rewalk_pending(&txn).unwrap().is_empty(), "{why}");
    };

    {
        let _held = kda_indexer::cmd::lock_data_dir(dir.path()).unwrap();
        assert!(rewalk(&cfg(), dir.path(), &names(&["free.wiza"])).is_err());
    }
    untouched("lock held");
    let err = rewalk(&cfg(), dir.path(), &names(&["free.wiza", "free.nope"])).unwrap_err();
    assert!(format!("{err:#}").contains("unknown module"), "{err:#}");
    untouched("unknown name");

    let out = rewalk(&cfg(), dir.path(), &names(&["free.wiza"])).unwrap();
    assert_eq!(out["posting_rows_marked"], 2, "{out}");
    assert_eq!(out["rewalk_queued"], 1, "{out}");
    assert_eq!(out["rewalk_pending"], 1, "{out}");
    {
        let hs = HotStore::open_ro(&hot_dir).unwrap();
        let txn = hs.ro().unwrap();
        assert!(txn
            .get(Table::BalanceSeed, &bal_key(alice, wiza, 1))
            .unwrap()
            .is_some());
        assert!(txn
            .get(Table::BalanceSeed, &bal_key(bob, wiza, 2))
            .unwrap()
            .is_some());
        assert!(txn
            .get(Table::BalanceSeed, &bal_key(alice, other, 0))
            .unwrap()
            .is_none());
        assert_eq!(balances::rewalk_pending(&txn).unwrap(), vec![wiza]);
    }

    // Idempotent: nothing new to mark, the same pending set.
    let again = rewalk(&cfg(), dir.path(), &names(&["free.wiza", "free.wiza"])).unwrap();
    assert_eq!(again["posting_rows_marked"], 0, "{again}");
    assert_eq!(again["rewalk_pending"], 1, "{again}");
    {
        let hs = HotStore::open_ro(&hot_dir).unwrap();
        assert_eq!(balances::dirty_count(&hs.ro().unwrap()).unwrap(), 2);
    }

    // `coin` (zero rows gap Q): only its history walk is queued; its posting holders are not
    // marked (N-6).
    let out = rewalk(&cfg(), dir.path(), &names(&["coin"])).unwrap();
    assert_eq!(out["posting_rows_marked"], 0, "{out}");
    assert_eq!(out["rewalk_queued"], 1, "{out}");
    assert_eq!(out["rewalk_pending"], 2, "{out}");
    let hs = HotStore::open_ro(&hot_dir).unwrap();
    let txn = hs.ro().unwrap();
    assert_eq!(balances::dirty_count(&txn).unwrap(), 2);
    let coin = HotDict::load(&txn)
        .unwrap()
        .find(DictKind::Module, "coin")
        .unwrap();
    assert!(balances::rewalk_pending(&txn).unwrap().contains(&coin));
}
