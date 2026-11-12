//! Freeze = backfill (Task 14, replay determinism): the steps the freezer writes from the
//! hot store, and the range it merges from ten of them, are byte-equal to what `backfill`
//! writes for the same heights fetched from a node. The node is a local stand-in serving the
//! pool blocks (`/cut`, `header/branch`, `payload/outputs/batch`), so the backfill runs its
//! own fetch, decode, build and range-writer path end to end.

mod common;

use std::collections::BTreeMap;

use std::path::Path;
use std::sync::Arc;

use common::*;

fn files(dir: &Path) -> BTreeMap<String, String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_file())
        .map(|p| {
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                blake3::hash(&std::fs::read(&p).unwrap())
                    .to_hex()
                    .to_string(),
            )
        })
        .collect()
}

/// Freeze with `range` (merge on when 10 × step fits), then backfill the frozen heights
/// from the stand-in node into a fresh data dir; the segment dirs must be equal.
async fn freeze_equals_backfill(range: u64) {
    let pool = pool();
    let all: Vec<usize> = (0..POOL_LEN).collect();
    let chain = branch(&all, 0, 0, 0);

    let df = tempfile::tempdir().unwrap();
    let frozen = {
        let c = ctx(df.path(), 2, range, 2);
        apply_linear(&c, &pool, &chain, 5);
        while kdx_ingest::freezer::tick(&c).unwrap().is_some() {}
        kdx_segment::manifest::Manifest::load(&c.segdir)
            .unwrap()
            .unwrap()
    };
    assert_eq!(frozen.frontier, 20);

    let base = fake_node(&pool, &[(0, chain)]);
    let db = tempfile::tempdir().unwrap();
    let c = Arc::new(ctx_with_node(db.path(), 2, range, 2, &base));
    kdx_ingest::backfill::run(c.clone(), 0, 19, kdx_ingest::HeaderMode::Cut)
        .await
        .unwrap();
    let backfilled = kdx_segment::manifest::Manifest::load(&c.segdir)
        .unwrap()
        .unwrap();
    assert_eq!(backfilled, frozen);
    let (ff, fb) = (
        files(&df.path().join("segments")),
        files(&db.path().join("segments")),
    );
    assert!(ff.len() >= 15, "{ff:?}");
    assert_eq!(fb, ff);
}

/// Ten steps of 2 heights, no merge: the step files are byte-equal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frozen_steps_equal_backfilled_steps() {
    freeze_equals_backfill(1000).await;
}

/// Ten steps merged into the range 0..20: byte-equal to a backfilled range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_merged_range_equals_a_backfilled_range() {
    freeze_equals_backfill(20).await;
}

/// Content digest per manifest entry plus the split-independent total.
fn content(c: &kdx_ingest::Ctx) -> (Vec<(u32, u32, String)>, String) {
    use kdx_segment::content::{self, ContentDigest};
    let m = kdx_segment::Manifest::load(&c.segdir).unwrap().unwrap();
    let dict = kdx_hot::HotDict::load(&c.hot.ro().unwrap()).unwrap();
    let mut total = ContentDigest::new();
    let mut per = Vec::new();
    for e in &m.entries {
        let r = kdx_segment::RangeReader::open(&c.segdir, e, &m.network).unwrap();
        per.push((e.start, e.end, content::range(&r, &dict).unwrap().blake3));
        content::feed(&r, &dict, &mut total).unwrap();
    }
    (per, total.finish().blake3)
}

fn deep_check(c: &kdx_ingest::Ctx) {
    let rep = kdx_segment::check(&c.segdir, true).unwrap();
    assert!(rep.ok(), "{rep:?}");
}

/// 9B-2, as ruled (R-P5P9-29): two chains, the same blocks, committed per height chain 0
/// first in one store and chain 1 first in the other. Names get ids in commit order and the
/// freezer copies the hot bytes, so the step bytes may differ (accepted); the content of
/// every step is equal, and equal to a backfill of the same heights from the node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frozen_step_content_does_not_depend_on_the_cross_chain_commit_order() {
    let pool = pool();
    let places = places_for(&[0, 1], 12);
    let base = fake_node(&pool, &places);
    let (da, db, dc) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    let a = content(&freeze_store(da.path(), &pool, &base, &places, &[0, 1]));
    let b = content(&freeze_store(db.path(), &pool, &base, &places, &[1, 0]));
    assert!(a.0.len() >= 4, "{a:?}");
    assert_eq!(a, b);
    let last = a.0.last().unwrap().1 - 1;
    let c = Arc::new(ctx_full(dc.path(), 2, 1000, 2, &base, &[0, 1]));
    kdx_ingest::backfill::run(c.clone(), 0, last, kdx_ingest::HeaderMode::Cut)
        .await
        .unwrap();
    assert_eq!(content(&c), a, "backfill content");
}

/// T-4 determinism: `rebuild-range --step` of a frozen single-chain step gives blake3-equal
/// files; the swap leaves no rebuild dir and a store that passes `segments check --deep`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_of_a_frozen_step_is_byte_equal_on_one_chain() {
    let pool = pool();
    let places = places_for(&[0], 24);
    let base = fake_node(&pool, &places);
    let d = tempfile::tempdir().unwrap();
    let c = Arc::new(freeze_store(d.path(), &pool, &base, &places, &[0]));
    let before = content(&c);
    let e = kdx_segment::Manifest::load(&c.segdir)
        .unwrap()
        .unwrap()
        .entries[3]
        .clone();
    let rep = kdx_ingest::rebuild::run(
        c.clone(),
        e.kind,
        e.start,
        kdx_ingest::HeaderMode::Cut,
        false,
    )
    .await
    .unwrap();
    assert!(rep.swapped && rep.bytes_equal, "{rep:?}");
    assert_eq!(rep.content_equal, Some(true));
    assert!(!kdx_segment::swap::rebuild_dir(&c.segdir, e.start).exists());
    deep_check(&c);
    assert_eq!(content(&c), before);
}

/// Multi-chain step committed in non-backfill order: the rebuild is content-equal (bytes
/// may differ, R-P5P9-29), both blake3 sets are reported, `--dry-run` swaps nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_on_two_chains_is_content_equal() {
    let pool = pool();
    let places = places_for(&[0, 1], 12);
    let base = fake_node(&pool, &places);
    let d = tempfile::tempdir().unwrap();
    let c = Arc::new(freeze_store(d.path(), &pool, &base, &places, &[1, 0]));
    let before = content(&c);
    let e = kdx_segment::Manifest::load(&c.segdir)
        .unwrap()
        .unwrap()
        .entries[2]
        .clone();
    let dry = kdx_ingest::rebuild::run(
        c.clone(),
        e.kind,
        e.start,
        kdx_ingest::HeaderMode::Cut,
        true,
    )
    .await
    .unwrap();
    assert!(!dry.swapped && dry.content_equal == Some(true), "{dry:?}");
    assert!(!kdx_segment::swap::rebuild_dir(&c.segdir, e.start).exists());
    let rep = kdx_ingest::rebuild::run(
        c.clone(),
        e.kind,
        e.start,
        kdx_ingest::HeaderMode::Cut,
        false,
    )
    .await
    .unwrap();
    assert!(rep.swapped, "{rep:?}");
    assert_eq!(rep.content_equal, Some(true));
    assert_eq!(rep.old_blake3.len(), rep.new_blake3.len());
    deep_check(&c);
    assert_eq!(content(&c), before);
}

/// Repair: one step's txs file damaged, `segments check` fails, `rebuild-range` restores it
/// and `segments check --deep` passes with the original content.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_repairs_a_corrupted_step() {
    let pool = pool();
    let places = places_for(&[0, 1], 12);
    let base = fake_node(&pool, &places);
    let d = tempfile::tempdir().unwrap();
    let c = Arc::new(freeze_store(d.path(), &pool, &base, &places, &[0, 1]));
    let before = content(&c);
    let e = kdx_segment::Manifest::load(&c.segdir)
        .unwrap()
        .unwrap()
        .entries[1]
        .clone();
    let txs = c.segdir.join(kdx_segment::files::file_name(
        "mainnet01",
        kdx_segment::files::FileKind::Txs,
        e.start,
        e.end,
    ));
    let mut b = std::fs::read(&txs).unwrap();
    let mid = b.len() / 2;
    for x in &mut b[mid..mid + 16] {
        *x ^= 0xa5;
    }
    std::fs::write(&txs, &b).unwrap();
    assert!(!kdx_segment::check(&c.segdir, false).unwrap().ok());
    let rep = kdx_ingest::rebuild::run(
        c.clone(),
        e.kind,
        e.start,
        kdx_ingest::HeaderMode::Cut,
        false,
    )
    .await
    .unwrap();
    assert!(rep.swapped && !rep.bytes_equal, "{rep:?}");
    assert_ne!(rep.content_equal, Some(true), "{rep:?}");
    deep_check(&c);
    assert_eq!(content(&c), before);
}
