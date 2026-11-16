//! Block-file reads: the per-thread zstd context (files with and without a dictionary, an
//! error frame in between, several threads) and the read-ahead spans of `prefetch`.

use std::path::Path;

use kdx_segment::blockfile::{BlockFile, BlockFileWriter};

fn rec(i: u64) -> Vec<u8> {
    format!("record {i} of the test file; ")
        .repeat(1 + (i % 7) as usize)
        .into_bytes()
}

fn write(dir: &Path, name: &str, dict: Vec<u8>, n: u64) -> BlockFile {
    let p = dir.join(name);
    let mut w = BlockFileWriter::create(&p, "txs", dict, 3, 4).unwrap();
    for i in 0..n {
        w.push(rec(i)).unwrap();
    }
    w.finish(Vec::new(), 0).unwrap();
    BlockFile::open(&p).unwrap()
}

#[test]
fn one_context_per_thread_reads_files_with_and_without_a_dictionary() {
    let dir = tempfile::tempdir().unwrap();
    // A raw-content dictionary (any bytes that do not start with the dictionary magic).
    let with = write(
        dir.path(),
        "a.seg",
        b"record of the test file; ".repeat(40),
        30,
    );
    let without = write(dir.path(), "b.seg", Vec::new(), 30);
    // A third file whose first frame is damaged (its zstd magic number zeroed).
    let bad_path = dir.path().join("c.seg");
    drop(write(dir.path(), "c.seg", Vec::new(), 8));
    let mut b = std::fs::read(&bad_path).unwrap();
    b[..4].copy_from_slice(&[0; 4]);
    std::fs::write(&bad_path, &b).unwrap();
    let bad = BlockFile::open(&bad_path).unwrap();

    // Interleaved on one thread: the context carries nothing from one file to the next, and
    // a failed frame leaves it usable.
    for i in 0..30 {
        assert_eq!(with.record(i).unwrap(), rec(i), "dict file, record {i}");
        assert!(bad.record(0).is_err());
        assert_eq!(without.record(29 - i).unwrap(), rec(29 - i), "plain file");
    }
    // Several threads, each with its own context, over the same files.
    std::thread::scope(|s| {
        for t in 0..4u64 {
            let (with, without) = (&with, &without);
            s.spawn(move || {
                for j in 0..30 {
                    let i = (j + t * 7) % 30;
                    assert_eq!(with.record(i).unwrap(), rec(i));
                    assert_eq!(without.record(i).unwrap(), rec(i));
                }
            });
        }
    });
}

#[test]
fn prefetch_spans_cover_the_rows_of_the_records_merged_when_adjacent() {
    let dir = tempfile::tempdir().unwrap();
    // 10 records, 4 per row: rows 0 (0-3), 1 (4-7), 2 (8-9).
    let f = write(dir.path(), "p.seg", Vec::new(), 10);
    let all = f.record_spans(0..10).unwrap();
    assert_eq!(all.len(), 1, "{all:?}");
    assert_eq!(all[0].0, 0);
    let total = all[0].1;
    let row = |r: u64| f.record_spans([r * 4]).unwrap()[0];
    let (r0, r1, r2) = (row(0), row(1), row(2));
    assert_eq!(r0.1 + r1.1 + r2.1, total);
    assert_eq!((r1.0, r2.0), (r0.1, r0.1 + r1.1));
    // Rows 0 and 2: two spans; rows 0 and 1 (records 3 and 4): one merged span.
    assert_eq!(f.record_spans([9, 1, 0]).unwrap(), vec![r0, r2]);
    assert_eq!(f.record_spans([4, 3]).unwrap(), vec![(0, r0.1 + r1.1)]);
    // Records past the end are skipped; no records, no spans.
    assert!(f.record_spans([10, 99]).unwrap().is_empty());
    assert!(f.record_spans([]).unwrap().is_empty());
    // The advice covers exactly those bytes, and the records read back unchanged.
    assert_eq!(f.prefetch(0..10).unwrap(), total);
    assert_eq!(f.prefetch([9]).unwrap(), r2.1);
    for i in 0..10 {
        assert_eq!(f.record(i).unwrap(), rec(i));
    }
}
