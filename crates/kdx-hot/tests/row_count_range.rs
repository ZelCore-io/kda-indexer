//! The row counts' per-range resume blob (`rowcount::RANGE`, PR #59 review m-b and m-c).

use kdx_decode::rowcount::CountKind;
use kdx_hot::rowcount::{self, Tally, MAX_RANGE_ROWS, RANGE};
use kdx_hot::{Geometry, HotStore, Txn};

fn tally(rows: &[(u64, (u64, u64))]) -> Tally {
    let mut t = Tally::default();
    for &(e, v) in rows {
        t.insert((CountKind::AcctXfer, e, 0), v);
    }
    t
}

/// m-c: a subtraction below zero on any row changes no row (computed first, then written),
/// keeps the blob and reports false so the caller clears the marker; an exact one subtracts
/// every row and drops the blob.
#[test]
fn undo_range_writes_nothing_unless_every_row_subtracts() {
    let dir = tempfile::tempdir().unwrap();
    let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
    let mut t = hot.rw().unwrap();
    rowcount::add_tally(&mut t, &tally(&[(1, (5, 1)), (2, (1, 0))]), 1).unwrap();
    // Row 1 can lose (3, 1); row 2 cannot lose (4, 0).
    rowcount::set_range(&mut t, 10, 20, &tally(&[(1, (3, 1)), (2, (4, 0))])).unwrap();
    assert!(!rowcount::undo_range(&mut t, 10, 20).unwrap());
    assert_eq!(
        rowcount::read_all(&t).unwrap(),
        tally(&[(1, (5, 1)), (2, (1, 0))])
    );
    // Exact: row 1 goes to (2, 0), row 2 to zero (deleted).
    rowcount::set_range(&mut t, 10, 20, &tally(&[(1, (3, 1)), (2, (1, 0))])).unwrap();
    assert!(rowcount::undo_range(&mut t, 10, 20).unwrap());
    assert_eq!(rowcount::read_all(&t).unwrap(), tally(&[(1, (2, 0))]));
    assert!(t.meta(RANGE).unwrap().is_none());
    t.commit().unwrap();
}

/// m-b: a range over the cap stores no blob (and drops an older one), so its resume clears
/// the marker instead of reading a 100s-of-MB Meta value.
#[test]
fn a_range_over_the_cap_stores_no_blob() {
    let dir = tempfile::tempdir().unwrap();
    let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
    let mut t = hot.rw().unwrap();
    rowcount::set_range(&mut t, 0, 10, &tally(&[(1, (1, 0))])).unwrap();
    assert!(t.meta(RANGE).unwrap().is_some());
    let big: Vec<(u64, (u64, u64))> = (0..=MAX_RANGE_ROWS as u64).map(|e| (e, (1, 0))).collect();
    rowcount::set_range(&mut t, 10, 20, &tally(&big)).unwrap();
    assert!(t.meta(RANGE).unwrap().is_none());
    assert!(!rowcount::undo_range(&mut t, 10, 20).unwrap());
    t.commit().unwrap();
}
