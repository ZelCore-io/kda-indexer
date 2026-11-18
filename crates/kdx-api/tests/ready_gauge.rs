//! FIX-2 fix 4: `kdx_sender_index_ready` is set when the store opens (and on every segment
//! refresh), not only once a sender list runs: install step 4 reads it right after start.
//! Its own test binary, since the gauge is process-global.

use std::sync::Arc;

use kdx_api::Store;
use kdx_hot::{Geometry, HotStore};

fn store(dir: &std::path::Path, sndr_hot: bool) -> Store {
    let hot = Arc::new(HotStore::open(&dir.join("hot"), Geometry::default()).unwrap());
    // A fresh hot store is ready; a tail an older binary wrote has no `Meta sndr_hot`.
    let mut t = hot.rw().unwrap();
    if sndr_hot {
        t.set_meta_u64(kdx_hot::env::meta::SNDR_HOT, 1).unwrap();
    } else {
        t.del(
            kdx_hot::Table::Meta,
            kdx_hot::env::meta::SNDR_HOT.as_bytes(),
        )
        .unwrap();
    }
    t.commit().unwrap();
    Store::open(hot, dir).unwrap()
}

#[test]
fn the_ready_gauge_is_set_when_the_store_opens() {
    let gauge = || kdx_metrics::get().sender_index_ready.get();
    let ready = tempfile::tempdir().unwrap();
    let not_ready = tempfile::tempdir().unwrap();

    let _s = store(not_ready.path(), false);
    assert_eq!(gauge(), 0, "a hot tail without its sndr rows");
    // An empty segments dir is ready; the hot tail says so: 1 before any request.
    let _s = store(ready.path(), true);
    assert_eq!(gauge(), 1);
    let again = tempfile::tempdir().unwrap();
    let _s = store(again.path(), false);
    assert_eq!(gauge(), 0);
}
