//! F-16: a long-running reader (`serve`) keeps the files it opened. A range swapped on disk
//! after that (`rebuild-range`, `segments add-index`) is detectable, so `serve` can log that
//! it needs a restart.

use std::sync::Arc;

use kdx_segment::files::{file_name, FileKind};
use kdx_segment::{Manifest, RangeKind, RangeWriter, Segments, WriteOpts};
use kdx_types::MemDict;

#[test]
fn a_range_toml_replaced_after_open_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let seg = dir.path().join("segments");
    let mut m = Manifest::new("mainnet01");
    let w = RangeWriter::create(
        &seg,
        &dir.path().join("etl"),
        "mainnet01",
        RangeKind::Step,
        0,
        4,
        0,
        WriteOpts::new(1, 0),
    )
    .unwrap();
    let e = w.finish().unwrap();
    m.push(e.clone()).unwrap();
    m.commit(&seg).unwrap();
    let s = Segments::open(&seg, Arc::new(MemDict::new())).unwrap();
    assert!(s.swapped_ranges().is_empty());
    // A swap renames a new `range.toml` into place (same content, new file).
    let p = seg.join(file_name("mainnet01", FileKind::Range, e.start, e.end));
    let tmp = seg.join("range.toml.new");
    std::fs::copy(&p, &tmp).unwrap();
    std::fs::rename(&tmp, &p).unwrap();
    assert_eq!(s.swapped_ranges(), vec![(e.start, e.end)]);
    // A fresh open sees the current files.
    let s2 = Segments::open(&seg, Arc::new(MemDict::new())).unwrap();
    assert!(s2.swapped_ranges().is_empty());
}
