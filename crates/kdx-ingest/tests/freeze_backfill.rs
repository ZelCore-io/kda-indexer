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
    // The damaged step is unreadable, so no content digest is compared (I-1 covers that).
    assert_eq!(rep.content_equal, None, "{rep:?}");
    deep_check(&c);
    assert_eq!(content(&c), before);
}

type Blocks = Vec<(
    kdx_types::record::BlockRecord,
    Vec<kdx_types::record::TxRecord>,
)>;

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
    let changes: [(&str, fn(&mut Blocks, usize, usize)); 6] = [
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
