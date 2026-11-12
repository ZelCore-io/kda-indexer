//! Finding fixtures B (Task 14): P5/P6 review items that changed code, as regression tests
//! where a test can express them. The others are listed in the run report with the existing
//! tests that already cover them.

mod common;

use common::*;

/// Review M4 (P5): on `resume`, a Merkle spool of a range the manifest lists (a crash came
/// between the manifest commit and the append) is appended to `merkle-unverified.jsonl`
/// after the earlier lines; a spool of an unlisted range is dropped with `etl/`.
#[test]
fn resume_recovers_the_spools_of_listed_ranges_only() {
    let pool = pool();
    let ids: Vec<usize> = (0..8).collect();
    let dir = tempfile::tempdir().unwrap();
    let c = ctx(dir.path(), 2, 1000, 2);
    apply_linear(&c, &pool, &branch(&ids, 0, 0, 0), 8);
    while kdx_ingest::freezer::tick(&c).unwrap().is_some() {}
    let m = kdx_segment::manifest::Manifest::load(&c.segdir)
        .unwrap()
        .unwrap();
    assert!(m.entries.iter().any(|e| e.start == 2), "{m:?}");
    let path = dir.path().join(kdx_ingest::backfill::MERKLE_UNVERIFIED);
    std::fs::write(&path, "earlier\n").unwrap();
    std::fs::write(c.etl.join("merkle-000000002.jsonl"), "listed\n").unwrap();
    std::fs::write(c.etl.join("merkle-000000900.jsonl"), "unlisted\n").unwrap();

    // Only the recovery matters here; what resume says about the hot tail does not.
    let _ = kdx_ingest::backfill::resume(&c);

    assert_eq!(std::fs::read_to_string(&path).unwrap(), "earlier\nlisted\n");
    assert!(!c.etl.join("merkle-000000002.jsonl").exists());
    assert!(!c.etl.join("merkle-000000900.jsonl").exists());
}
