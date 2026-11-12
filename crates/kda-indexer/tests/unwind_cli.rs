//! `unwind --chain C --to H` (Task 14): the CLI's unwind followed by a re-apply of the same
//! blocks through the tail writer gives the original `hot stats --digest`; `--to` below the
//! frozen frontier is refused and changes nothing. Real mainnet blocks from the shared
//! stateful harness of kdx-ingest.

#[path = "../../kdx-ingest/tests/common/mod.rs"]
mod common;

use common::*;
use kda_indexer::config::Config;
use kdx_hot::{Stage, Txn};

/// The packaged example config (the data dir is passed separately).
fn cfg() -> Config {
    Config::load(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../packaging/config/mainnet01.toml"
    )))
    .unwrap()
}

#[test]
fn unwind_then_reapply_restores_the_digest() {
    let pool = pool();
    let ids: Vec<usize> = (0..12).collect();
    let chain = branch(&ids, 0, 0, 0);
    let dir = tempfile::tempdir().unwrap();
    let (d0, dict0) = {
        let c = ctx(dir.path(), 100, 1000, 100);
        apply_linear(&c, &pool, &chain, 4);
        (digest(&c.hot), dict_rows(&c.hot))
    };

    kda_indexer::cmd::unwind::run(&cfg(), dir.path(), 0, Some(5), false).unwrap();

    let c = ctx(dir.path(), 100, 1000, 100);
    {
        let ro = c.hot.ro().unwrap();
        let tip = kdx_hot::checkpoint(&ro, Stage::Tail, 0).unwrap();
        assert_eq!(tip.map(|t| t.0), Some(5));
        assert_eq!(ro.count(kdx_hot::Table::Headers).unwrap(), 6);
    }
    assert_ne!(digest(&c.hot), d0);
    apply_linear(&c, &pool, &chain[6..], 3);
    assert_eq!(digest(&c.hot), d0);
    assert_eq!(dict_rows(&c.hot), dict0);
}

#[test]
fn unwind_below_the_frozen_frontier_is_refused() {
    let pool = pool();
    let ids: Vec<usize> = (0..12).collect();
    let dir = tempfile::tempdir().unwrap();
    let (d0, frontier) = {
        let c = ctx(dir.path(), 2, 1000, 2);
        apply_linear(&c, &pool, &branch(&ids, 0, 0, 0), 12);
        while kdx_ingest::freezer::tick(&c).unwrap().is_some() {}
        let f = c.hot.ro().unwrap().frozen_frontier().unwrap();
        (digest(&c.hot), f)
    };
    assert!(frontier >= 4, "frontier {frontier}");
    let e = kda_indexer::cmd::unwind::run(&cfg(), dir.path(), 0, Some(frontier - 2), false)
        .unwrap_err();
    assert!(format!("{e:#}").contains("frozen frontier"), "{e:#}");
    {
        let c = ctx(dir.path(), 2, 1000, 2);
        assert_eq!(digest(&c.hot), d0, "a refused unwind changes nothing");
    }
    // `H = frontier − 1` undoes every hot block of the chain.
    kda_indexer::cmd::unwind::run(&cfg(), dir.path(), 0, Some(frontier - 1), false).unwrap();
    let c = ctx(dir.path(), 2, 1000, 2);
    assert_ne!(digest(&c.hot), d0);
    assert_eq!(
        c.hot.ro().unwrap().count(kdx_hot::Table::Headers).unwrap(),
        0
    );
}
