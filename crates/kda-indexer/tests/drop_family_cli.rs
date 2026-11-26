//! `hot drop-family sndr` (FIX-2): offline, under the data-dir lock; afterwards no hot row or
//! undo op names family 6, `Meta sndr_hot` is gone, and the freezer of a binary that knows
//! only families 0..=5 would accept every `TailPostings` row (every family byte is <= 5).

#[path = "../../kdx-ingest/tests/common/mod.rs"]
mod common;

use common::*;
use kda_indexer::config::Config;
use kdx_hot::undo::{self, UndoOp};
use kdx_hot::{Table, Txn};

fn cfg() -> Config {
    Config::load(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../packaging/config/mainnet01.toml"
    )))
    .unwrap()
}

#[test]
fn drop_family_sndr_removes_rows_undo_ops_and_the_ready_key() {
    let pool = pool();
    let dir = tempfile::tempdir().unwrap();
    {
        let c = ctx(dir.path(), 2, 1000, 2);
        apply_linear(&c, &pool, &branch(&(0..12).collect::<Vec<_>>(), 0, 0, 0), 4);
        let mut txn = c.hot.rw().unwrap();
        txn.set_meta_u64(kdx_hot::env::meta::SNDR_HOT, 1).unwrap();
        kdx_hot::rowcount::set_ready(&mut txn, 0).unwrap();
        txn.commit().unwrap();
        let ro = c.hot.ro().unwrap();
        assert!(!ro
            .scan(Table::TailPostings, &[6], |k| k[0] == 6)
            .unwrap()
            .is_empty());
    }

    // Refused while another process holds the lock, and for a required family.
    {
        let _held = kda_indexer::cmd::lock_data_dir(dir.path()).unwrap();
        let err = kda_indexer::cmd::hot::drop_family(&cfg(), dir.path(), "sndr").unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("add-index") && msg.contains("drop-family"),
            "N4: the lock message names the offline commands: {msg}"
        );
    }
    assert!(kda_indexer::cmd::hot::drop_family(&cfg(), dir.path(), "acct").is_err());

    kda_indexer::cmd::hot::drop_family(&cfg(), dir.path(), "sndr").unwrap();
    let c = ctx(dir.path(), 2, 1000, 2);
    {
        let ro = c.hot.ro().unwrap();
        assert!(!kdx_hot::sndr::hot_ready(&ro).unwrap());
        assert!(
            !kdx_hot::rowcount::marker_set(&ro).unwrap(),
            "PR #48: counts marker cleared"
        );
        for (k, _) in ro.scan(Table::TailPostings, &[], |_| true).unwrap() {
            assert!(k[0] <= 5, "{k:?}");
        }
        for (_, v) in ro.scan(Table::Undo, &[], |_| true).unwrap() {
            for op in undo::decode(&v).unwrap() {
                assert!(!matches!(op, UndoOp::Posting { family: 6, .. }), "{op:?}");
            }
        }
    }
}

/// A data dir without a hot store is refused (non-zero exit) and no store is created.
#[test]
fn drop_family_refuses_a_data_dir_without_a_hot_store() {
    let dir = tempfile::tempdir().unwrap();
    let e = kda_indexer::cmd::hot::drop_family(&cfg(), dir.path(), "sndr").unwrap_err();
    assert!(e.to_string().contains("no hot store"), "{e:#}");
    assert!(!dir.path().join("hot").exists());
    let cfgp = dir.path().join("config.toml");
    std::fs::write(
        &cfgp,
        std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/config/mainnet01.toml"
        ))
        .unwrap(),
    )
    .unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_kda-indexer"))
        .arg("--config")
        .arg(&cfgp)
        .args(["hot", "drop-family", "sndr", "--data-dir"])
        .arg(dir.path().join("data"))
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(!dir.path().join("data").join("hot").exists());
}
