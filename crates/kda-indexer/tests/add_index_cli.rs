//! `segments add-index` (FIX-2 T4). A fixture store is written twice from the same blocks:
//! once by today's writer, once the way a build before FIX-2 wrote it (writer flag
//! `legacy_segments`: no `sndr.pst`, no `kf.fil`; hot tail without family-6 rows or
//! `Meta sndr_hot`). add-index must turn the second into the first byte for byte, survive a
//! kill at each commit point (the next writer start recovers, a rerun completes), do nothing
//! on a finished store, write nothing with `--dry-run`, repair ranges a rollback left with
//! listed-but-missing or untrusted files, and refuse a sender the dictionary lacks.

#[path = "../../kdx-ingest/tests/common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use common::*;
use kdx_ingest::add_index::{self, Only, Opts};

/// Ranges of 8 heights, steps of 2, finality 2: two merged ranges, steps and a hot tail.
fn fixture(dir: &Path, legacy: bool) {
    let pool = pool();
    let mut c = ctx(dir, 2, 8, 2);
    c.opts.legacy_segments = legacy;
    apply_linear(
        &c,
        &pool,
        &branch(&(0..POOL_LEN).collect::<Vec<_>>(), 0, 0, 0),
        5,
    );
    while kdx_ingest::freezer::tick(&c).unwrap().is_some() {}
    if legacy {
        // The tail of a build before FIX-2: no family-6 rows, no `Meta sndr_hot`.
        let mut txn = c.hot.rw().unwrap();
        kdx_hot::sndr::drop_family(&mut txn, kdx_decode::Family::Sndr).unwrap();
        txn.commit().unwrap();
    }
    let m = kdx_segment::Manifest::load(&c.segdir).unwrap().unwrap();
    let kinds: Vec<_> = m.entries.iter().map(|e| e.kind).collect();
    assert!(
        kinds.contains(&kdx_segment::RangeKind::Range)
            && kinds.contains(&kdx_segment::RangeKind::Step),
        "{kinds:?}"
    );
}

/// blake3 of every file in the segments dir, and the names of any dirs in it.
fn files(data: &Path) -> BTreeMap<String, String> {
    std::fs::read_dir(data.join("segments"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .map(|p| {
            let n = p.file_name().unwrap().to_string_lossy().into_owned();
            if p.is_dir() {
                (n, "dir".into())
            } else {
                let h = kdx_segment::files::blake3_file(&p).unwrap();
                (n, h)
            }
        })
        .collect()
}

fn hot(data: &Path) -> kdx_hot::HotStore {
    kdx_hot::HotStore::open(&data.join("hot"), kdx_hot::Geometry::default()).unwrap()
}

fn hot_digest(data: &Path) -> BTreeMap<String, (usize, String)> {
    digest(&kdx_hot::HotStore::open_ro(&data.join("hot")).unwrap())
}

/// `segments check --deep` with the hot dictionary: every `sndr` list is exactly the txs its
/// entity sent.
fn check_deep(data: &Path, what: &str) {
    let dict = {
        let hs = kdx_hot::HotStore::open_ro(&data.join("hot")).unwrap();
        let txn = hs.ro().unwrap();
        kdx_hot::HotDict::load(&txn).unwrap()
    };
    let rep = kdx_segment::check_with(&data.join("segments"), true, Some(&dict)).unwrap();
    assert!(rep.ok(), "{what}: {rep:?}");
}

fn ready(data: &Path) -> bool {
    kdx_segment::Segments::open(
        &data.join("segments"),
        std::sync::Arc::new(kdx_types::MemDict::new()),
    )
    .unwrap()
    .sender_ready()
}

fn opts(jobs: usize, dry_run: bool) -> Opts {
    Opts {
        jobs,
        only: Only::Both,
        dry_run,
    }
}

/// (reference store written with the new files, legacy store) in `<tmp>/a`, `<tmp>/b`.
fn pair(t: &Path) -> (PathBuf, PathBuf) {
    let (a, b) = (t.join("a"), t.join("b"));
    fixture(&a, false);
    fixture(&b, true);
    (a, b)
}

#[test]
fn add_index_gives_the_writers_bytes_and_backfills_the_hot_store() {
    let t = tempfile::tempdir().unwrap();
    let (a, b) = pair(t.path());
    let want = files(&a);
    let before = files(&b);
    assert!(want.keys().any(|n| n.contains("-sndr-")) && want.keys().any(|n| n.contains("-kf-")));
    assert!(!before
        .keys()
        .any(|n| n.contains("-sndr-") || n.contains("-kf-")));
    assert_ne!(hot_digest(&a), hot_digest(&b));
    check_deep(&b, "legacy store");
    assert!(!ready(&b));

    let rep = add_index::run(&b.join("segments"), &hot(&b), opts(2, false)).unwrap();
    assert!(rep.ok(), "{rep:?}");
    let m = kdx_segment::Manifest::load(&b.join("segments"))
        .unwrap()
        .unwrap();
    assert_eq!(rep.entries.len(), m.entries.len());
    assert_eq!(rep.totals.built, m.entries.len());
    assert!(rep
        .entries
        .iter()
        .all(|e| e.swapped && e.built == ["sndr", "kf"]));
    assert!(rep.totals.senders > 20 && rep.totals.sndr_bytes > 0 && rep.totals.filter_bytes > 0);
    let h = rep.hot.as_ref().unwrap();
    assert!(
        !h.already_ready && h.inserted > 0 && h.inserted == h.senders,
        "{h:?}"
    );

    // sndr.pst, kf.fil and range.toml exactly as the writer writes them; nothing else moved.
    assert_eq!(files(&b), want);
    assert_eq!(hot_digest(&b), hot_digest(&a), "hot store = the new tail");
    check_deep(&b, "after add-index");
    assert!(ready(&b));
}

/// FIX-2 fix 2, the rollback that skipped `hot drop-family sndr`: an older binary extended
/// the tail without family-6 rows while `Meta sndr_hot` stayed 1. After the re-upgrade the
/// freezer derives every step's `sndr` postings from the records (not from the gappy hot
/// rows) and add-index runs the hot backfill although `sndr_hot` already says 1: the store
/// ends byte for byte the writer's, with complete sender lists.
#[test]
fn a_rollback_without_drop_family_is_repaired_by_freeze_and_add_index() {
    let t = tempfile::tempdir().unwrap();
    let (a, b) = (t.path().join("a"), t.path().join("b"));
    fixture(&a, false);
    {
        let pool = pool();
        let c = ctx(&b, 2, 8, 2);
        let all = branch(&(0..POOL_LEN).collect::<Vec<_>>(), 0, 0, 0);
        // Blocks the older binary applied have no family-6 rows (dropped right after their
        // apply); `sndr_hot` says 1 throughout.
        let gap = |c: &kdx_ingest::Ctx| {
            let mut txn = c.hot.rw().unwrap();
            kdx_hot::sndr::drop_family(&mut txn, kdx_decode::Family::Sndr).unwrap();
            txn.set_meta_u64(kdx_hot::env::meta::SNDR_HOT, 1).unwrap();
            txn.commit().unwrap();
        };
        apply_linear(&c, &pool, &all[..10], 5);
        gap(&c);
        apply_linear(&c, &pool, &all[10..], 5);
        assert!(kdx_hot::sndr::hot_ready(&c.hot.ro().unwrap()).unwrap());
        // The re-upgraded writer freezes over the gap ...
        while kdx_ingest::freezer::tick(&c).unwrap().is_some() {}
        // ... and the tail left in the hot store lacks its rows too.
        gap(&c);
    }
    check_deep(&b, "frozen over a gappy tail");
    assert_ne!(
        hot_digest(&b),
        hot_digest(&a),
        "the hot tail still has the gap"
    );

    let rep = add_index::run(&b.join("segments"), &hot(&b), opts(2, false)).unwrap();
    assert!(rep.ok(), "{rep:?}");
    let h = rep.hot.as_ref().unwrap();
    assert!(h.already_ready && h.inserted > 0, "{h:?}");
    assert_eq!(files(&b), files(&a));
    assert_eq!(hot_digest(&b), hot_digest(&a), "hot store = the new tail");
    check_deep(&b, "after add-index");
    assert!(ready(&b));
}

#[test]
fn a_rerun_on_a_finished_store_does_nothing() {
    let t = tempfile::tempdir().unwrap();
    let (a, _) = pair(t.path());
    let before = (files(&a), hot_digest(&a));
    let rep = add_index::run(&a.join("segments"), &hot(&a), opts(4, false)).unwrap();
    assert!(rep.ok(), "{rep:?}");
    assert!(rep.entries.is_empty(), "{rep:?}");
    assert_eq!(rep.totals.skipped, rep.totals.entries);
    let h = rep.hot.as_ref().unwrap();
    assert!(h.already_ready && h.inserted == 0, "{h:?}");
    assert_eq!((files(&a), hot_digest(&a)), before);
}

#[test]
fn dry_run_writes_nothing() {
    let t = tempfile::tempdir().unwrap();
    let (a, b) = pair(t.path());
    let before = (files(&b), hot_digest(&b));
    let ro = kdx_hot::HotStore::open_ro(&b.join("hot")).unwrap();
    let rep = add_index::run(&b.join("segments"), &ro, opts(2, true)).unwrap();
    drop(ro);
    assert!(rep.ok(), "{rep:?}");
    assert!(rep.dry_run && !rep.entries.is_empty());
    assert!(rep
        .entries
        .iter()
        .all(|e| !e.swapped && e.sndr_bytes > 0 && e.filter_bytes > 0));
    // The sizes it reports are the ones a real run writes.
    let sndr: u64 = files(&a)
        .keys()
        .filter(|n| n.contains("-sndr-"))
        .map(|n| std::fs::metadata(a.join("segments").join(n)).unwrap().len())
        .sum();
    assert_eq!(rep.totals.sndr_bytes, sndr);
    assert!(rep.hot.as_ref().unwrap().txs > 0);
    assert_eq!((files(&b), hot_digest(&b)), before);
    let etl: Vec<_> = std::fs::read_dir(b.join("etl"))
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .filter(|n| n.to_string_lossy().starts_with("add-index"))
        .collect();
    assert!(etl.is_empty(), "{etl:?}");
}

/// A rollback's old binary deletes `sndr.pst`/`kf.fil` at startup while `range.toml` keeps
/// listing them; a damaged `kf.fil` is not trusted. add-index rebuilds exactly those files.
#[test]
fn listed_but_missing_or_untrusted_files_are_repaired() {
    let t = tempfile::tempdir().unwrap();
    let (a, _) = pair(t.path());
    let want = files(&a);
    let seg = a.join("segments");
    let m = kdx_segment::Manifest::load(&seg).unwrap().unwrap();
    let name = |k, e: &kdx_segment::RangeEntry| {
        kdx_segment::files::file_name(&m.network, k, e.start, e.end)
    };
    use kdx_segment::files::FileKind;
    let (e0, e1, e2) = (&m.entries[0], &m.entries[1], &m.entries[2]);
    std::fs::remove_file(seg.join(name(FileKind::Sndr, e0))).unwrap();
    std::fs::remove_file(seg.join(name(FileKind::KeyFilter, e0))).unwrap();
    std::fs::remove_file(seg.join(name(FileKind::KeyFilter, e1))).unwrap();
    let p = seg.join(name(FileKind::KeyFilter, e2));
    let mut bytes = std::fs::read(&p).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x5a;
    std::fs::write(&p, bytes).unwrap();
    assert!(!ready(&a));

    let rep = add_index::run(&seg, &hot(&a), opts(2, false)).unwrap();
    assert!(rep.ok(), "{rep:?}");
    let by = add_index::by_start(&rep);
    assert_eq!(by.len(), 3, "{rep:?}");
    assert_eq!(by[&e0.start].built, ["sndr", "kf"]);
    assert_eq!(by[&e1.start].built, ["kf"]);
    assert_eq!(by[&e2.start].built, ["kf"]);
    assert_eq!(files(&a), want);
    check_deep(&a, "repaired");
    assert!(ready(&a));
}

#[test]
fn a_sender_missing_from_the_dictionary_fails_the_entry_and_swaps_nothing() {
    let t = tempfile::tempdir().unwrap();
    let b = t.path().join("b");
    fixture(&b, true);
    let before = files(&b);
    let seg = b.join("segments");
    let m = kdx_segment::Manifest::load(&seg).unwrap().unwrap();
    let empty = kdx_types::MemDict::new();
    let err = add_index::add_entry(&seg, &m.network, &m.entries[0], &empty, Only::Both, false)
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("not in the dictionary"),
        "{err:#}"
    );
    assert_eq!(files(&b), before);
    // The key filter alone needs no dictionary.
    let rep =
        add_index::add_entry(&seg, &m.network, &m.entries[0], &empty, Only::Filter, false).unwrap();
    assert_eq!(rep.built, ["kf"]);
    check_deep(&b, "filter only");
}

fn config(dir: &Path) -> PathBuf {
    let src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../packaging/config/mainnet01.toml"
    ))
    .unwrap();
    let p = dir.join("config.toml");
    std::fs::write(&p, src).unwrap();
    p
}

fn add_index_cmd(cfg: &Path, data: &Path, crash_at: Option<&str>) -> std::process::Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_kda-indexer"));
    c.arg("--config")
        .arg(cfg)
        .args(["segments", "add-index", "--jobs", "1", "--data-dir"])
        .arg(data);
    if let Some(p) = crash_at {
        c.env("KDX_CRASH_AT", p);
    }
    c.output().unwrap()
}

/// Killed at each commit point; every time the store passes the deep check, the next writer
/// start (`freezer::reconcile`, which runs `swap::recover`) recovers, and a final run
/// completes the store byte for byte.
#[test]
fn killed_at_each_commit_point_then_recovered_and_completed() {
    let t = tempfile::tempdir().unwrap();
    let (a, b) = pair(t.path());
    let cfg = config(t.path());
    let want = files(&a);
    for point in [
        "add-index-before-journal",
        "add-index-mid-swap",
        "add-index-before-cleanup",
    ] {
        let out = add_index_cmd(&cfg, &b, Some(point));
        assert!(!out.status.success(), "{point}: not killed");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            err.contains(&format!("KDX_CRASH_AT={point}")),
            "{point}: {err}"
        );
        check_deep(&b, point);
        // The next writer start.
        let c = ctx(&b, 2, 8, 2);
        kdx_ingest::freezer::reconcile(&c).unwrap();
        drop(c);
        let left: Vec<_> = files(&b)
            .into_keys()
            .filter(|n| n.starts_with("rebuild.") || n.starts_with(".swap-"))
            .collect();
        assert!(left.is_empty(), "{point}: {left:?}");
        check_deep(&b, point);
    }
    let out = add_index_cmd(&cfg, &b, None);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rep: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    // Two entries were completed by the crashed runs and their recovery.
    assert_eq!(rep["totals"]["skipped"], 2, "{rep}");
    assert_eq!(files(&b), want);
    assert_eq!(hot_digest(&b), hot_digest(&a));
    check_deep(&b, "completed");
    assert!(ready(&b));
}

#[test]
fn add_index_refuses_a_data_dir_without_a_store() {
    let t = tempfile::tempdir().unwrap();
    let cfg = config(t.path());
    let out = add_index_cmd(&cfg, &t.path().join("nothing"), None);
    assert!(!out.status.success());
    assert!(!t.path().join("nothing").join("hot").exists());
}
