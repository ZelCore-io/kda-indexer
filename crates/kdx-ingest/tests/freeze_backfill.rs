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
        // Row counts set by `reconcile-counts`, then frozen: a freeze moves `next_txnum` and
        // the TxNum stamps with it in its txn (review I-2), so neither marker is cleared.
        let rep =
            kdx_ingest::row_counts::reconcile(&c.segdir, &c.hot, 2, false, &|| false).unwrap();
        assert!(rep.ready && rep.chain_counters_ready, "{rep:?}");
        while kdx_ingest::freezer::tick(&c).unwrap().is_some() {}
        assert!(kdx_hot::rowcount::ready(&c.hot.ro().unwrap()).unwrap());
        // Ingest (tail apply, then the freezer) kept the chain counters from block 0.
        chain_counters_match_the_walk(&c);
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
    // Backfill keeps the chain counters, the row counts and the holders (and their markers)
    // from block 0 (secpass A I-1: the row counts need no `reconcile-counts`).
    every_derived_table_is_exact(&c);
    // A resume that resets the hot frontier (a range whose MDBX commit landed but whose
    // manifest commit did not) would count that range twice: the marker is cleared.
    {
        let mut t = c.hot.rw().unwrap();
        t.set_frozen_frontier(22).unwrap();
        t.commit().unwrap();
    }
    let (_, _, info) = kdx_ingest::backfill::resume(&c).unwrap();
    assert!(info.hot_reset);
    assert!(!kdx_hot::chaincount::marker_set(&c.hot.ro().unwrap()).unwrap());
    assert!(!count_marker(&c));
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
    set_count_marker(&c);
    assert!(kdx_hot::chaincount::marker_set(&c.hot.ro().unwrap()).unwrap());
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
    assert!(
        !count_marker(&c),
        "PR #48: rebuild-range clears the stored-counts marker"
    );
    assert!(
        !kdx_hot::chaincount::marker_set(&c.hot.ro().unwrap()).unwrap(),
        "rebuild-range clears the chain counters' marker"
    );
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
    // The damaged step is unreadable, so no content digest is compared (I-1 covers that).
    assert_eq!(rep.content_equal, None, "{rep:?}");
    deep_check(&c);
    assert_eq!(content(&c), before);
}

type Blocks = Vec<(
    kdx_types::record::BlockRecord,
    Vec<kdx_types::record::TxRecord>,
)>;
/// One content change applied to block `i` (tx `j`).
type Change = fn(&mut Blocks, usize, usize);

/// Every block of `c`'s frozen entries with its txs, as the reader returns them.
fn frozen_blocks(c: &kdx_ingest::Ctx) -> Blocks {
    let m = kdx_segment::Manifest::load(&c.segdir).unwrap().unwrap();
    let mut out = Vec::new();
    for e in &m.entries {
        let r = kdx_segment::RangeReader::open(&c.segdir, e, &m.network).unwrap();
        for s in 0..r.slots() {
            let Some(b) = r.block_at(s).unwrap() else {
                continue;
            };
            let (a, n) = (r.keys.first(s).unwrap(), r.keys.count(s));
            let txs = (a..a + n).map(|rel| r.tx_rel(rel).unwrap()).collect();
            out.push((b, txs));
        }
    }
    out
}

/// I-1: the content digest sees a content change. The same blocks digest equal; a header
/// field, a tx's output, a transfer index, the presence bits, a dropped tx and the block
/// order each change it. Two stores whose node served another payload at one height differ
/// in that step and in the total, and agree on every other step.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn content_digest_differs_on_every_content_change() {
    use kdx_segment::content::ContentDigest;
    let pool = pool();
    let places = places_for(&[0], 24);
    let base = fake_node(&pool, &places);
    let da = tempfile::tempdir().unwrap();
    let a = freeze_store(da.path(), &pool, &base, &places, &[0]);
    let dict = kdx_hot::HotDict::load(&a.hot.ro().unwrap()).unwrap();
    let blocks = frozen_blocks(&a);
    let digest = |bs: &Blocks| {
        let mut d = ContentDigest::new();
        for (b, txs) in bs {
            d.block(b, txs, &dict).unwrap();
        }
        d.finish()
    };
    let same = digest(&blocks);
    assert_eq!(digest(&blocks.clone()), same);
    assert!(same.txs > 0 && blocks.len() >= 10, "{same:?}");
    let (bi, ti) = blocks
        .iter()
        .enumerate()
        .find_map(|(i, (_, txs))| {
            txs.iter()
                .position(|t| !t.transfers.is_empty() && t.present != 0)
                .map(|j| (i, j))
        })
        .expect("fixture: a tx with a transfer");
    let changes: [(&str, Change); 6] = [
        ("header nonce", |bs, i, _| bs[i].0.nonce ^= 1),
        ("output gas", |bs, i, j| bs[i].1[j].gas += 1),
        ("transfer index", |bs, i, j| bs[i].1[j].transfers[0] ^= 1),
        ("presence bits", |bs, i, j| {
            let p = bs[i].1[j].present;
            bs[i].1[j].present = p & (p - 1);
        }),
        ("tx dropped", |bs, i, _| {
            bs[i].1.pop();
        }),
        ("block order", |bs, _, _| bs.swap(0, 1)),
    ];
    for (what, change) in changes {
        let mut bs = blocks.clone();
        change(&mut bs, bi, ti);
        assert_ne!(digest(&bs).blake3, same.blake3, "{what}");
    }

    // One height served with another payload: that step and the total differ.
    let mut other = places.clone();
    other[0].1[5].pool = (other[0].1[5].pool + 1) % POOL_LEN;
    let base = fake_node(&pool, &other);
    let db = tempfile::tempdir().unwrap();
    let b = freeze_store(db.path(), &pool, &base, &other, &[0]);
    let (pa, ta) = content(&a);
    let (pb, tb) = content(&b);
    assert_eq!(pa.len(), pb.len());
    assert_ne!(ta, tb, "total");
    for (x, y) in pa.iter().zip(&pb) {
        assert_eq!((x.0, x.1), (y.0, y.1));
        if (x.0..x.1).contains(&5) {
            assert_ne!(x.2, y.2, "step {}..{}", x.0, x.1);
        } else {
            assert_eq!(x.2, y.2, "step {}..{}", x.0, x.1);
        }
    }
}

/// The two-chain store of `rebuild_on_two_chains_is_content_equal`, reopened against a node
/// that serves `node` instead: (ctx, step entry 2, data dir guard).
fn store_with_node(
    node: impl Fn(&mut Vec<(u8, Vec<Place>)>),
) -> (
    Arc<kdx_ingest::Ctx>,
    kdx_segment::RangeEntry,
    tempfile::TempDir,
) {
    let pool = pool();
    let places = places_for(&[0, 1], 12);
    let d = tempfile::tempdir().unwrap();
    {
        let base = fake_node(&pool, &places);
        let _ = freeze_store(d.path(), &pool, &base, &places, &[1, 0]);
    }
    let mut served = places.clone();
    node(&mut served);
    let base = fake_node(&pool, &served);
    let c = Arc::new(ctx_full(d.path(), 2, 1000, 2, &base, &[0, 1]));
    let e = kdx_segment::Manifest::load(&c.segdir)
        .unwrap()
        .unwrap()
        .entries[2]
        .clone();
    (c, e, d)
}

fn dict_fwd_rows(c: &kdx_ingest::Ctx) -> usize {
    use kdx_hot::Txn;
    let txn = c.hot.ro().unwrap();
    kdx_types::dict::DictKind::ALL
        .into_iter()
        .map(|k| txn.count(kdx_hot::Table::DictFwd(k)).unwrap())
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dry_run_rebuild_persists_no_dictionary_names() {
    // m6: `write` persisted every name interned while indexing the refetched blocks, dry run
    // or not. A name only the in-memory dictionary holds stands in for one first seen there.
    use kdx_types::dict::DictMut;
    let (c, e, _d) = store_with_node(|_| {});
    let before = dict_fwd_rows(&c);
    c.dict
        .lock()
        .unwrap()
        .intern(kdx_types::dict::DictKind::Module, "m6.dry-run-only");
    let dry = kdx_ingest::rebuild::run(
        c.clone(),
        e.kind,
        e.start,
        kdx_ingest::HeaderMode::Cut,
        true,
    )
    .await
    .unwrap();
    assert!(dry.dry_run && !dry.swapped, "{dry:?}");
    assert_eq!(
        dict_fwd_rows(&c),
        before,
        "a dry run wrote dictionary names"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_refuses_a_range_that_does_not_link_to_its_neighbours() {
    // m7: the node's chain forked inside the range and stayed forked: the rebuilt step links
    // to the stored one before it, but not to the stored first block after it.
    let (c, e, _d) = store_with_node(|s| {
        for (ch, ps) in s.iter_mut() {
            for p in ps.iter_mut().filter(|p| p.height >= 4) {
                p.br = *ch + 7;
                p.parent_br = if p.height == 4 { *ch } else { *ch + 7 };
            }
        }
    });
    let err = kdx_ingest::rebuild::run(
        c.clone(),
        e.kind,
        e.start,
        kdx_ingest::HeaderMode::Cut,
        false,
    )
    .await
    .unwrap_err();
    assert!(format!("{err:#}").contains("linkage"), "{err:#}");
    assert!(!kdx_segment::swap::rebuild_dir(&c.segdir, e.start).exists());
    deep_check(&c);

    // Forked from the height before the range: its first block has no stored parent.
    let (c, e, _d) = store_with_node(|s| {
        for (ch, ps) in s.iter_mut() {
            for p in ps.iter_mut().filter(|p| p.height >= 3) {
                p.br = *ch + 7;
                p.parent_br = if p.height == 3 { *ch } else { *ch + 7 };
            }
        }
    });
    let err = kdx_ingest::rebuild::run(
        c.clone(),
        e.kind,
        e.start,
        kdx_ingest::HeaderMode::Cut,
        false,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{err:#}").contains("does not follow stored"),
        "{err:#}"
    );
}

/// (account, chain) of every acct posting of `e` (balance rows of the coin module).
fn coin_rows(
    c: &kdx_ingest::Ctx,
    e: &kdx_segment::RangeEntry,
) -> std::collections::BTreeSet<(u32, u8)> {
    let r = kdx_segment::RangeReader::open(&c.segdir, e, "mainnet01").unwrap();
    let p = &r.postings[kdx_decode::Family::Acct as usize];
    let mut out = std::collections::BTreeSet::new();
    for i in 0..p.entities() {
        for rel in p.list_at(i).unwrap() {
            let (chain, _) = r.slot_pos(r.keys.slot_of(u64::from(rel)).unwrap());
            out.insert((p.entity_at(i) as u32, chain));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_with_changed_content_marks_both_versions_dirty_and_search_stale() {
    // M-7: the node moved step 2's blocks between the chains (same hashes, so the linkage
    // holds; other txs per chain). Balances of the old and the new content are marked dirty
    // and the search index is marked stale from the step.
    use kdx_hot::Txn;
    let (c, e, d) = store_with_node(|s| {
        for h in [4usize, 5] {
            let (a, b) = (s[0].1[h].pool, s[1].1[h].pool);
            s[0].1[h].pool = b;
            s[1].1[h].pool = a;
        }
    });
    let old = coin_rows(&c, &e);
    {
        let mut txn = c.hot.rw().unwrap();
        for t in [kdx_hot::Table::BalanceDirty, kdx_hot::Table::BalanceSeed] {
            for (k, _) in txn.scan(t, &[], |_| true).unwrap() {
                txn.del(t, &k).unwrap();
            }
        }
        txn.commit().unwrap();
    }
    let sdir = kdx_search::search_dir(d.path());
    std::fs::create_dir_all(&sdir).unwrap();
    kdx_search::SearchManifest {
        format: kdx_search::manifest::FORMAT,
        tantivy: "test".into(),
        mode: "full".into(),
        fallback_min_len: 0,
        built_through_height: 11,
        rebuilt_through_height: Some(11),
        stale_from_height: None,
        docs: 0,
        bytes: 0,
        built_at: 1,
    }
    .commit(&sdir)
    .unwrap();

    let rep = kdx_ingest::rebuild::run(
        c.clone(),
        e.kind,
        e.start,
        kdx_ingest::HeaderMode::Cut,
        false,
    )
    .await
    .unwrap();
    assert!(rep.swapped && rep.content_equal == Some(false), "{rep:?}");
    assert!(rep.dirty_marks > 0 && rep.search_marked_stale, "{rep:?}");
    let new = coin_rows(&c, &e);
    assert!(
        old.difference(&new).next().is_some(),
        "fixture: no old-only row"
    );
    assert!(
        new.difference(&old).next().is_some(),
        "fixture: no new-only row"
    );
    let txn = c.hot.ro().unwrap();
    let coin = kdx_hot::balances::coin_module(&txn).unwrap().unwrap();
    for &(a, ch) in old.union(&new) {
        assert!(
            kdx_hot::balances::is_dirty(&txn, a, coin, ch).unwrap(),
            "account {a} chain {ch} not dirty"
        );
    }
    let m = kdx_search::SearchManifest::load(&sdir).unwrap().unwrap();
    assert_eq!(m.stale_from_height, Some(e.start));
}

/// Turns a range into one written before FIX-2: `sndr.pst` and `kf.fil` leave the disk and
/// its `range.toml` (`files` and `postings.sndr`).
fn strip_optional(segdir: &Path, net: &str, e: &kdx_segment::RangeEntry) {
    use kdx_segment::files::{file_name, optional_file_names, FileKind};
    let toml = segdir.join(file_name(net, FileKind::Range, e.start, e.end));
    let mut info = kdx_segment::RangeInfo::load(&toml).unwrap();
    for n in optional_file_names(net, e.start, e.end) {
        assert!(info.files.remove(&n).is_some(), "{n} listed");
        std::fs::remove_file(segdir.join(&n)).unwrap();
    }
    assert!(info.postings.remove("sndr").is_some());
    std::fs::write(&toml, toml::to_string(&info).unwrap()).unwrap();
}

/// Ten frozen steps (no merge yet) in a fresh store; returns its dir.
fn ten_steps() -> tempfile::TempDir {
    let pool = pool();
    let all: Vec<usize> = (0..POOL_LEN).collect();
    let d = tempfile::tempdir().unwrap();
    let c = ctx(d.path(), 2, 1000, 2);
    apply_linear(&c, &pool, &branch(&all, 0, 0, 0), 5);
    while kdx_ingest::freezer::tick(&c).unwrap().is_some() {}
    d
}

/// FIX-2 T3: a merge of steps without `sndr.pst` (some or all) derives the sender postings
/// from the records and writes byte-for-byte the range a merge of complete steps writes;
/// stripped steps open and pass `segments check --deep` before the merge.
#[test]
fn a_merge_of_steps_with_and_without_sndr_gives_the_same_bytes() {
    let mut merged = Vec::new();
    for strip in [0usize, 2, 1] {
        // strip 0: none, 2: every other step, 1: every step.
        let d = ten_steps();
        {
            let c = ctx(d.path(), 2, 20, 2);
            let m = kdx_segment::Manifest::load(&c.segdir).unwrap().unwrap();
            assert_eq!(m.entries.len(), 10);
            if strip > 0 {
                for e in m.entries.iter().step_by(strip) {
                    strip_optional(&c.segdir, &m.network, e);
                }
                let seg =
                    kdx_segment::Segments::open(&c.segdir, Arc::new(kdx_types::MemDict::new()))
                        .unwrap();
                assert!(!seg.sender_ready());
                deep_check(&c);
            }
            assert!(kdx_ingest::freezer::merge(&c).unwrap().is_some());
            deep_check(&c);
            let seg = kdx_segment::Segments::open(&c.segdir, Arc::new(kdx_types::MemDict::new()))
                .unwrap();
            assert_eq!(seg.ranges.len(), 1);
            assert!(seg.sender_ready() && seg.ranges[0].filter.is_some());
        }
        let f = files(&d.path().join("segments"));
        assert!(f.keys().any(|n| n.contains("-sndr-")) && f.keys().any(|n| n.contains("-kf-")));
        merged.push(f);
    }
    assert_eq!(merged[1], merged[0], "every other step stripped");
    assert_eq!(merged[2], merged[0], "every step stripped");
}

/// FIX-2 T3: `rebuild-range` of a step written before FIX-2 adds `sndr.pst` and `kf.fil`
/// (listed, swapped in), keeps the content, and leaves a store that passes the deep check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_of_a_step_without_optional_files_adds_them() {
    let pool = pool();
    let places = places_for(&[0], 24);
    let base = fake_node(&pool, &places);
    let d = tempfile::tempdir().unwrap();
    let c = Arc::new(freeze_store(d.path(), &pool, &base, &places, &[0]));
    let before = content(&c);
    let m = kdx_segment::Manifest::load(&c.segdir).unwrap().unwrap();
    let e = m.entries[3].clone();
    let want = files(&c.segdir);
    strip_optional(&c.segdir, &m.network, &e);
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
    assert_eq!(rep.content_equal, Some(true));
    let opt = kdx_segment::files::optional_file_names(&m.network, e.start, e.end);
    for n in &opt {
        assert_eq!(rep.old_blake3[n], "-");
        assert_eq!(rep.new_blake3[n], want[n]);
    }
    assert!(!kdx_segment::swap::rebuild_dir(&c.segdir, e.start).exists());
    // The rebuilt step is the step the freezer wrote, optional files included.
    assert_eq!(files(&c.segdir), want);
    deep_check(&c);
    assert_eq!(content(&c), before);
}

/// FIX-2 T3: the startup cleanup (`remove_unlisted`, run by `freezer::reconcile`) keeps the
/// optional files a range lists and removes optional files of a range that is not listed.
#[test]
fn startup_cleanup_keeps_listed_optional_files() {
    let d = ten_steps();
    let c = ctx(d.path(), 2, 1000, 2);
    let net = kdx_segment::Manifest::load(&c.segdir)
        .unwrap()
        .unwrap()
        .network;
    let stray = kdx_segment::files::optional_file_names(&net, 500, 502);
    for n in &stray {
        std::fs::write(c.segdir.join(n), b"x").unwrap();
    }
    let before = files(&c.segdir);
    kdx_ingest::freezer::reconcile(&c).unwrap();
    let mut want = before.clone();
    for n in &stray {
        want.remove(n);
    }
    assert_eq!(files(&c.segdir), want);
    assert_eq!(want.keys().filter(|n| n.contains("-kf-")).count(), 10);
    assert_eq!(want.keys().filter(|n| n.contains("-sndr-")).count(), 10);
}

/// The stored chain counters are ready, non-trivial and equal to what `reconcile-counts`
/// builds from every record of the store (frozen and hot).
fn chain_counters_match_the_walk(c: &kdx_ingest::Ctx) {
    let (walked, _) = kdx_ingest::row_counts::tally_both(&c.segdir, &c.hot, 2, &|| false).unwrap();
    let ro = c.hot.ro().unwrap();
    assert!(kdx_hot::chaincount::ready(&ro).unwrap());
    assert!(
        walked.chains.chains.values().any(|v| v.user_txs > 0),
        "{walked:?}"
    );
    assert_eq!(
        kdx_hot::chaincount::differing(&ro, &walked.chains).unwrap(),
        0
    );
}

/// Chain counters, row counts and holders are all ready and equal to what
/// `reconcile-counts --check` and `reconcile-holders --check` build from every record.
fn every_derived_table_is_exact(c: &kdx_ingest::Ctx) {
    chain_counters_match_the_walk(c);
    let ro = c.hot.ro().unwrap();
    assert!(kdx_hot::rowcount::ready(&ro).unwrap(), "row counts ready");
    assert!(kdx_hot::holders::ready(&ro).unwrap(), "holders ready");
    assert!(!kdx_hot::rowcount::read_all(&ro).unwrap().is_empty());
    // PR #59 review m-a: every range is in the manifest, so no resume blob is left in Meta.
    assert!(kdx_hot::Txn::meta(&ro, kdx_hot::rowcount::RANGE)
        .unwrap()
        .is_none());
    assert!(kdx_hot::Txn::meta(&ro, kdx_hot::chaincount::RANGE)
        .unwrap()
        .is_none());
    drop(ro);
    let rep = kdx_ingest::row_counts::reconcile(&c.segdir, &c.hot, 2, true, &|| false).unwrap();
    assert_eq!(rep.differing, Some(0), "row counts: {rep:?}");
    let rep = kdx_ingest::holders_fill::reconcile(&c.segdir, &c.hot, 2, true, &|| false).unwrap();
    assert_eq!(rep.differing, Some(0), "holders: {rep:?}");
    assert!(rep.rows > 0, "{rep:?}");
}

/// Secpass A I-1, m-1, m-2: a from-scratch backfill killed between a range's MDBX commit and
/// its manifest commit resumes with every derived table still ready: the chain counters and
/// row counts subtract the range's stored tally, the holders drop the rows the range added
/// (their names are truncated with it), and the rebuilt range adds them again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_kill_between_the_hot_and_manifest_commits_keeps_every_table_exact() {
    let pool = pool();
    let all: Vec<usize> = (0..POOL_LEN).collect();
    let base = fake_node(&pool, &[(0, branch(&all, 0, 0, 0))]);
    let db = tempfile::tempdir().unwrap();
    let c = Arc::new(ctx_with_node(db.path(), 2, 2, 2, &base));
    kdx_ingest::backfill::run(c.clone(), 0, 19, kdx_ingest::HeaderMode::Cut)
        .await
        .unwrap();
    every_derived_table_is_exact(&c);
    let mut m = kdx_segment::manifest::Manifest::load(&c.segdir)
        .unwrap()
        .unwrap();
    assert!(m.entries.len() >= 2, "{m:?}");
    // Find a last range that added holders, so m-2 is exercised.
    let last = m.entries.pop().unwrap();
    // PR #59 review m-a: the manifest commit drops the range's resume blobs; a kill between
    // the two commits leaves them. Put back what the last range's commit wrote: the full
    // store's counts minus those of a store backfilled to just before that range.
    {
        let pdb = tempfile::tempdir().unwrap();
        let p = Arc::new(ctx_with_node(pdb.path(), 2, 2, 2, &base));
        kdx_ingest::backfill::run(p.clone(), 0, last.start - 1, kdx_ingest::HeaderMode::Cut)
            .await
            .unwrap();
        let pm = kdx_segment::manifest::Manifest::load(&p.segdir)
            .unwrap()
            .unwrap();
        let ranges = |m: &kdx_segment::manifest::Manifest| -> Vec<(u32, u32)> {
            m.entries.iter().map(|e| (e.start, e.end)).collect()
        };
        assert_eq!(ranges(&pm), ranges(&m));
        let (full, pre) = (c.hot.ro().unwrap(), p.hot.ro().unwrap());
        let pre_rows: BTreeMap<_, _> = kdx_hot::rowcount::read_all(&pre).unwrap().iter().collect();
        let mut rows = kdx_hot::rowcount::Tally::default();
        for (k, (u, cb)) in kdx_hot::rowcount::read_all(&full).unwrap().iter() {
            let (pu, pc) = pre_rows.get(&k).copied().unwrap_or((0, 0));
            if (u, cb) != (pu, pc) {
                rows.insert(k, (u - pu, cb - pc));
            }
        }
        let pre_chains = kdx_hot::chaincount::read_all(&pre).unwrap();
        let mut chains = kdx_hot::chaincount::Tally::default();
        for (ch, v) in kdx_hot::chaincount::read_all(&full).unwrap() {
            let v = v.unwrap();
            let pv = pre_chains.get(&ch).copied().flatten().unwrap_or_default();
            chains.chains.insert(
                ch,
                kdx_hot::chaincount::Counts {
                    user_txs: v.user_txs - pv.user_txs,
                    gas: v.gas.checked_sub(pv.gas).unwrap(),
                },
            );
        }
        drop((full, pre));
        let mut t = c.hot.rw().unwrap();
        kdx_hot::chaincount::set_range(&mut t, last.start, last.end, &chains).unwrap();
        kdx_hot::rowcount::set_range(&mut t, last.start, last.end, &rows).unwrap();
        t.commit().unwrap();
    }
    m.frontier = last.start;
    m.next_txnum = last.first_txnum;
    m.commit(&c.segdir).unwrap();
    let before = kdx_hot::holders::read_all(&c.hot.ro().unwrap()).unwrap();
    let (_, _, info) = kdx_ingest::backfill::resume(&c).unwrap();
    assert!(info.hot_reset);
    {
        let ro = c.hot.ro().unwrap();
        assert!(
            kdx_hot::chaincount::ready(&ro).unwrap(),
            "m-1: counters kept"
        );
        assert!(
            kdx_hot::rowcount::ready(&ro).unwrap(),
            "m-1: row counts kept"
        );
        assert!(kdx_hot::holders::ready(&ro).unwrap());
        let after = kdx_hot::holders::read_all(&ro).unwrap();
        assert!(after.iter().all(|h| h.first.height() < last.start), "m-2");
        assert_eq!(
            after.len(),
            before
                .iter()
                .filter(|h| h.first.height() < last.start)
                .count()
        );
    }
    // The tables equal the walk of what the manifest now lists.
    chain_counters_match_the_walk(&c);
    let rep = kdx_ingest::row_counts::reconcile(&c.segdir, &c.hot, 2, true, &|| false).unwrap();
    assert_eq!(rep.differing, Some(0), "row counts after resume: {rep:?}");
    let rep = kdx_ingest::holders_fill::reconcile(&c.segdir, &c.hot, 2, true, &|| false).unwrap();
    assert_eq!(rep.differing, Some(0), "holders after resume: {rep:?}");
    // The range is built again: every table exact and ready.
    kdx_ingest::backfill::run(c.clone(), 0, 19, kdx_ingest::HeaderMode::Cut)
        .await
        .unwrap();
    every_derived_table_is_exact(&c);
    assert_eq!(
        kdx_hot::holders::read_all(&c.hot.ro().unwrap()).unwrap(),
        before
    );
}

fn set_count_marker(c: &kdx_ingest::Ctx) {
    let mut t = c.hot.rw().unwrap();
    kdx_hot::rowcount::set_ready(&mut t, 0).unwrap();
    t.commit().unwrap();
}

fn count_marker(c: &kdx_ingest::Ctx) -> bool {
    kdx_hot::rowcount::marker_set(&c.hot.ro().unwrap()).unwrap()
}

/// Review I-1: `backfill --from H` (H > 0) into a new data dir stores counters that miss
/// every height below H. They are not served as exact, and `reconcile-counts` does not mark
/// them ready (a partial store, like `run --start-height`). The row counts describe what is
/// stored, which is also what the list walk reads, so `reconcile-counts` still sets theirs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backfill_from_mid_chain_is_not_served_as_exact() {
    let pool = pool();
    let all: Vec<usize> = (0..POOL_LEN).collect();
    let base = fake_node(&pool, &[(0, branch(&all, 0, 0, 0))]);
    let db = tempfile::tempdir().unwrap();
    let c = Arc::new(ctx_with_node(db.path(), 2, 1000, 2, &base));
    assert!(kdx_hot::chaincount::ready(&c.hot.ro().unwrap()).unwrap());
    kdx_ingest::backfill::run(c.clone(), 10, 19, kdx_ingest::HeaderMode::Cut)
        .await
        .unwrap();
    let m = kdx_segment::manifest::Manifest::load(&c.segdir)
        .unwrap()
        .unwrap();
    assert_eq!(m.entries.first().unwrap().start, 10);
    assert!(!kdx_hot::chaincount::marker_set(&c.hot.ro().unwrap()).unwrap());
    let rep = kdx_ingest::row_counts::reconcile(&c.segdir, &c.hot, 2, false, &|| false).unwrap();
    assert!(!rep.chain_counters_ready, "{rep:?}");
    let ro = c.hot.ro().unwrap();
    assert!(!kdx_hot::chaincount::marker_set(&ro).unwrap());
    assert!(!kdx_hot::chaincount::ready(&ro).unwrap());
    assert!(
        rep.ready && kdx_hot::rowcount::ready(&ro).unwrap(),
        "{rep:?}"
    );
    drop(ro);
    let rep = kdx_ingest::row_counts::reconcile(&c.segdir, &c.hot, 2, true, &|| false).unwrap();
    assert_eq!(
        rep.differing,
        Some(0),
        "row counts equal the stored records"
    );
}

/// The `TokenChains` table is ready and equal to what `Holders` gives.
fn token_chains_are_exact(c: &kdx_ingest::Ctx) {
    let ro = c.hot.ro().unwrap();
    assert!(
        kdx_hot::token_chains::ready(&ro).unwrap(),
        "token chains ready"
    );
    let want = kdx_hot::token_chains::derive(&kdx_hot::holders::read_all(&ro).unwrap());
    assert!(!want.is_empty());
    assert_eq!(kdx_hot::token_chains::read_all(&ro, || true).unwrap(), want);
}

/// The DEX and price fill markers.
fn fills_done(c: &kdx_ingest::Ctx) -> (bool, bool) {
    let ro = c.hot.ro().unwrap();
    (
        kdx_ingest::dex_fill::filled(&ro).unwrap(),
        kdx_hot::prices::ready(&ro).unwrap(),
    )
}

/// Final review I-4 (#54 park, "real-path ready-survives test"): a from-scratch backfill,
/// then the tail on top of it (linear blocks, a reorg that unwinds one, the freezer), keeps
/// every creation-time marker set (row counts, chain counters, holders, token chains) and
/// every table exact against a fresh tally; the DEX and price fills done on the backfilled
/// store stay done. A stamp one side misses would clear a marker at the first tail block.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backfilled_store_stays_ready_under_the_tail() {
    let pool = pool();
    let all: Vec<usize> = (0..POOL_LEN).collect();
    let chain = branch(&all, 0, 0, 0);
    let base = fake_node(&pool, &[(0, chain.clone())]);
    let db = tempfile::tempdir().unwrap();
    let c = Arc::new(ctx_with_node(db.path(), 2, 2, 2, &base));
    kdx_ingest::backfill::run(c.clone(), 0, 15, kdx_ingest::HeaderMode::Cut)
        .await
        .unwrap();
    every_derived_table_is_exact(&c);
    token_chains_are_exact(&c);
    // The fills `run` starts once the backfill is done.
    let rep = kdx_ingest::dex_fill::run(&c.hot, &c.segdir, &|| false).unwrap();
    assert!(!rep.stopped, "{rep:?}");
    let rep = kdx_ingest::price_fill::run(&c.hot, &c.segdir, &|| false).unwrap();
    assert!(!rep.stopped, "{rep:?}");
    assert_eq!(fills_done(&c), (true, true));
    let rows_before = kdx_hot::rowcount::read_all(&c.hot.ro().unwrap()).unwrap();

    // Tail: heights 16..=21 on the backfilled chain, one block per commit.
    apply_linear(&c, &pool, &chain[16..22], 1);
    every_derived_table_is_exact(&c);
    token_chains_are_exact(&c);
    assert_ne!(
        kdx_hot::rowcount::read_all(&c.hot.ro().unwrap()).unwrap(),
        rows_before,
        "the tail blocks are counted"
    );
    // A reorg: unwind 21, apply 21..=23 of another branch.
    let fork = branch(&[21, 22, 23], 21, 1, 0);
    apply_fork(&c, &pool, 20, 1, &fork);
    assert!(kdx_hot::holders::ready(&c.hot.ro().unwrap()).unwrap());
    every_derived_table_is_exact(&c);
    token_chains_are_exact(&c);
    // The freezer takes the finalized tail blocks (16..=19) into segments.
    let mut ticks = 0;
    while kdx_ingest::freezer::tick(&c).unwrap().is_some() {
        ticks += 1;
    }
    assert!(ticks >= 2, "the freezer froze the tail steps: {ticks}");
    assert_eq!(
        kdx_hot::Txn::frozen_frontier(&c.hot.ro().unwrap()).unwrap(),
        20
    );
    every_derived_table_is_exact(&c);
    token_chains_are_exact(&c);
    assert_eq!(fills_done(&c), (true, true));
}
