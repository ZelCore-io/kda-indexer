//! Regression tests for two robustness fixes found by the property tests (`props.rs`): a
//! hostile manifest and a damaged block-file offsets file are refusals, not panics.

use kdx_segment::blockfile::{BlockFile, BlockFileWriter};
use kdx_segment::manifest::Manifest;

#[test]
fn manifest_tx_count_overflow_is_refused_not_a_panic() {
    // first_txnum + tx_count overflows u64: a refusal, never an overflow panic.
    let json = r#"{"format_version":2,"network":"n","entries":[{"kind":"step","start":0,"end":1,"first_txnum":18446744073709551615,"tx_count":2,"block_count":0}],"frontier":1,"next_txnum":1}"#;
    let e = Manifest::parse(json.as_bytes()).unwrap_err().to_string();
    assert!(e.contains("overflows"), "{e}");
}

#[test]
fn damaged_block_file_offsets_are_an_error_not_a_slice_panic() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("t.seg");
    let mut w = BlockFileWriter::create(&p, "txs", Vec::new(), 3, 2).unwrap();
    for i in 0..6u8 {
        w.push(vec![i; 40]).unwrap();
    }
    w.finish(Vec::new(), 0).unwrap();
    // Every offset after the size byte becomes 0xff..: past the end of the data file.
    let off = p.with_extension("off");
    let mut b = std::fs::read(&off).unwrap();
    for x in &mut b[1..] {
        *x = 0xff;
    }
    std::fs::write(&off, &b).unwrap();
    let f = BlockFile::open(&p).unwrap();
    for i in 0..6 {
        assert!(f.record(i).is_err());
    }
}

#[test]
fn sorted_index_with_a_huge_key_count_is_refused_not_an_overflow_panic() {
    // Found by the `phf_open` fuzz target: `14 + 13 * n` overflowed for a damaged key count.
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("t.idx");
    let mut b = b"KDXH".to_vec();
    b.extend_from_slice(&[1, 1]); // version 1, sorted
    b.extend_from_slice(&(u64::MAX / 8).to_le_bytes());
    b.extend_from_slice(&[0; 13]);
    std::fs::write(&p, &b).unwrap();
    let e = kdx_segment::Index::open(&p).err().expect("refused");
    assert!(e.to_string().contains("key count"), "{e}");
}
