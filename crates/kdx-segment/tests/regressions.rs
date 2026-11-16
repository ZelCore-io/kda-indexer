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

/// `network` starts every segment file name the manifest lists: a value that is not a plain
/// name would name files outside `segments/` (`../x` gives `segments/../x-txs-...`). Parse,
/// commit and the other `validate` callers refuse it.
#[test]
fn manifest_network_must_be_a_plain_name() {
    let parse = |net: &str| {
        let mut v: serde_json::Value = serde_json::from_str(
            r#"{"format_version":2,"network":"n","entries":[{"kind":"step","start":0,"end":1,"first_txnum":0,"tx_count":1,"block_count":1}],"frontier":1,"next_txnum":1}"#,
        )
        .unwrap();
        v["network"] = net.into();
        Manifest::parse(&serde_json::to_vec(&v).unwrap())
    };
    let long = "a".repeat(kdx_segment::manifest::MAX_NETWORK_LEN + 1);
    for bad in [
        "../x",
        "..",
        ".",
        "a/..",
        "a/b",
        "/abs",
        "a\\b",
        "..\\x",
        "a..b",
        ".hidden",
        "",
        "a\0b",
        "net work",
        "nét",
        long.as_str(),
    ] {
        let e = parse(bad).unwrap_err().to_string();
        assert!(e.contains("not a plain name"), "{bad:?}: {e}");
    }
    let max = "a".repeat(kdx_segment::manifest::MAX_NETWORK_LEN);
    for good in [
        "mainnet01",
        "testnet04",
        "development",
        "fast-development",
        "net_1.a",
        max.as_str(),
    ] {
        assert_eq!(parse(good).unwrap().network, good);
    }
    // Nothing writes one either.
    let dir = tempfile::tempdir().unwrap();
    let e = Manifest::new("../x").commit(dir.path()).unwrap_err();
    assert!(e.to_string().contains("not a plain name"), "{e}");
    assert!(!dir.path().join("manifest.json").exists());
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

#[test]
fn keys_with_decreasing_starts_count_zero_instead_of_underflowing() {
    // Found by the `ef_keys` fuzz target (128 bytes, as the fuzzer wrote it): the file opens,
    // but a slot start is below the one before it, so `count` underflowed.
    const HEX: &str = "4b44584b010200000028000000000000000200000000000000552a5555555555555555010000000000540000000000008001000000000000000000000000000000020000000000000000004000000000000000000029000000000000000100000000000000000000000000000000000100000000000000000000000000000000";
    let bytes: Vec<u8> = (0..HEX.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&HEX[i..i + 2], 16).unwrap())
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("keys.ef");
    std::fs::write(&p, &bytes).unwrap();
    // Since M-6 the Elias-Fano data is rebuilt and compared at open, so a damaged file no
    // longer opens. If it ever does, the lookups must still not underflow.
    let r = kdx_segment::keys::Keys::open(&p);
    assert!(r.is_err(), "damaged keys.ef opened");
    if let Ok(k) = r {
        for s in 0..k.slots() + 2 {
            let _ = (k.first(s), k.count(s));
        }
    }
}
#[test]
fn postings_trailer_with_huge_sizes_is_refused_not_an_overflow_panic() {
    // Found by the `postings_list` fuzz target: `ef_at + ef_len + 24` overflowed.
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("t.post");
    let mut b = b"KDXP".to_vec();
    b.extend_from_slice(&[1, 0]);
    b.extend_from_slice(&1000u64.to_le_bytes());
    for v in [0u64, 0, u64::MAX] {
        b.extend_from_slice(&v.to_le_bytes()); // entities, lists_len, ef_len
    }
    std::fs::write(&p, &b).unwrap();
    let e = kdx_segment::postings::Postings::open(&p, 0)
        .err()
        .expect("refused");
    assert!(e.to_string().contains("trailer"), "{e}");
}

#[test]
fn keys_with_slot_count_u64_max_is_refused_not_an_overflow_panic() {
    // Found by the `ef_keys` fuzz target once M-6 let damaged Elias-Fano data reach the slot
    // check: `slots + 1` overflowed for a stored slot count of u64::MAX.
    const HEX: &str = "4b44584b0102000000ffffffffffffffff020000000000000055555555555955555555010000000000540000000000000001000000000000000000000000000000020000000000000000004000000000000000000029000000000000000100000000000000000000000000000000000000000000000000002900000000000000";
    let bytes: Vec<u8> = (0..HEX.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&HEX[i..i + 2], 16).unwrap())
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("keys.ef");
    std::fs::write(&p, &bytes).unwrap();
    assert!(kdx_segment::keys::Keys::open(&p).is_err());
}
