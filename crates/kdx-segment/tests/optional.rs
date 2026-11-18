//! FIX-2 T3: the optional range files `sndr.pst` and `kf.fil`. A store read with its key
//! filters answers every request-key and block-hash lookup exactly as the same store read
//! without them (removed, damaged, or with a bad header), so a filter can only skip work and
//! never turn a stored key into a miss.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use kdx_decode::{Family, IndexedBlock, TxIndex};
use kdx_segment::files::{file_name, optional_file_names, FileKind};
use kdx_segment::{Manifest, RangeEntry, RangeInfo, RangeKind, RangeWriter, Segments, WriteOpts};
use kdx_types::record::{BlockRecord, TxRecord};
use kdx_types::{Hash32, MemDict, TxKey};
use proptest::prelude::*;

const NET: &str = "mainnet01";

/// One block: its hash and the request keys of its txs (each tx's sender `entity` gets an
/// `acct` and a `sndr` posting).
#[derive(Debug, Clone)]
struct Blk {
    chain: u8,
    height: u32,
    hash: Hash32,
    rks: Vec<(Hash32, u64)>,
}

fn indexed(b: &Blk) -> IndexedBlock {
    let txs: Vec<TxRecord> = b
        .rks
        .iter()
        .enumerate()
        .map(|(i, (rk, _))| TxRecord {
            key: TxKey::new(b.height, b.chain, i as u16),
            request_key: Some(*rk),
            ..TxRecord::default()
        })
        .collect();
    let idx = txs
        .iter()
        .zip(&b.rks)
        .map(|(r, (_, ent))| TxIndex {
            key: r.key,
            postings: vec![(Family::Acct, *ent), (Family::Sndr, *ent)],
            pact_id: None,
            code_hash: None,
        })
        .collect();
    IndexedBlock {
        block: BlockRecord {
            hash: b.hash,
            chain: b.chain,
            height: u64::from(b.height),
            ..BlockRecord::default()
        },
        txs,
        idx,
        cmd_reencoded: 0,
        unindexed_accounts: 0,
    }
}

/// Writes `ranges` (each `(start, end, sorted index form, blocks)`) and the manifest.
fn write_store(dir: &Path, ranges: &[(u32, u32, bool, Vec<Blk>)]) {
    let seg = dir.join("segments");
    let mut m = Manifest::new(NET);
    let mut first = 0;
    for (start, end, sorted, blocks) in ranges {
        let mut o = WriteOpts::new(1, 0);
        o.force_sorted_index = *sorted;
        let mut w = RangeWriter::create(
            &seg,
            &dir.join("etl"),
            NET,
            RangeKind::Step,
            *start,
            *end,
            first,
            o,
        )
        .unwrap();
        for b in blocks {
            w.push(&indexed(b)).unwrap();
        }
        let e = w.finish().unwrap();
        first = e.end_txnum();
        m.push(e).unwrap();
    }
    m.commit(&seg).unwrap();
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        if p.is_file() {
            std::fs::copy(&p, to.join(p.file_name().unwrap())).unwrap();
        }
    }
}

fn toml_path(seg: &Path, e: &RangeEntry) -> std::path::PathBuf {
    seg.join(file_name(NET, FileKind::Range, e.start, e.end))
}

fn kf_path(seg: &Path, e: &RangeEntry) -> std::path::PathBuf {
    seg.join(file_name(NET, FileKind::KeyFilter, e.start, e.end))
}

/// The range as written before FIX-2: both optional files gone from disk and `range.toml`.
fn strip(seg: &Path, e: &RangeEntry) {
    let mut info = RangeInfo::load(&toml_path(seg, e)).unwrap();
    for n in optional_file_names(NET, e.start, e.end) {
        info.files.remove(&n).unwrap();
        std::fs::remove_file(seg.join(&n)).unwrap();
    }
    info.postings.remove("sndr").unwrap();
    std::fs::write(toml_path(seg, e), toml::to_string(&info).unwrap()).unwrap();
}

/// How a store's key filters are damaged.
#[derive(Debug, Clone, Copy)]
enum Damage {
    /// Removed from disk and `range.toml`.
    Stripped,
    /// One fingerprint byte flipped: the blake3 in `range.toml` no longer matches.
    FlippedByte(usize),
    /// The header's key count changed and `range.toml` re-hashed to match, so only the
    /// header/key-count checks can catch it.
    BadHeaderRehashed,
    /// Listed in `range.toml`, missing on disk.
    Missing,
}

fn damage(seg: &Path, e: &RangeEntry, d: Damage) {
    let p = kf_path(seg, e);
    match d {
        Damage::Stripped => strip(seg, e),
        Damage::Missing => std::fs::remove_file(&p).unwrap(),
        Damage::FlippedByte(at) => {
            let mut b = std::fs::read(&p).unwrap();
            // Past the magic, version and the bh section header: a fingerprint or rk byte.
            let i = 5 + 32 + at % (b.len() - 37);
            b[i] ^= 0x5a;
            std::fs::write(&p, &b).unwrap();
        }
        Damage::BadHeaderRehashed => {
            let mut b = std::fs::read(&p).unwrap();
            // bh n_keys (offset 5 + 16): claim one key more than the range has.
            let n = u64::from_le_bytes(b[21..29].try_into().unwrap());
            b[21..29].copy_from_slice(&(n + 1_000_000).to_le_bytes());
            std::fs::write(&p, &b).unwrap();
            let mut info = RangeInfo::load(&toml_path(seg, e)).unwrap();
            let st = info
                .files
                .get_mut(p.file_name().unwrap().to_str().unwrap())
                .unwrap();
            st.blake3 = blake3::hash(&b).to_hex().to_string();
            std::fs::write(toml_path(seg, e), toml::to_string(&info).unwrap()).unwrap();
        }
    }
}

fn open(seg: &Path) -> Segments {
    Segments::open(seg, Arc::new(MemDict::new())).unwrap()
}

fn h(prefix: u64, tail: u8) -> Hash32 {
    let mut x = [tail; 32];
    x[..8].copy_from_slice(&prefix.to_be_bytes());
    Hash32(x)
}

/// Up to three ranges of 4 heights; keys from a small prefix pool so that prefixes repeat
/// inside a range (the sorted index form) and request keys repeat across ranges.
fn store() -> impl Strategy<Value = Vec<(u32, u32, bool, Vec<Blk>)>> {
    let blk = (
        0u8..3,
        0u32..4,
        0u64..40,
        proptest::collection::vec((0u64..60, any::<u8>(), 1u64..5), 1..4),
    );
    (
        1usize..4,
        proptest::collection::vec(any::<bool>(), 3),
        proptest::collection::vec(proptest::collection::vec(blk, 1..8), 3),
    )
        .prop_map(|(n, sorted, per)| {
            (0..n)
                .map(|i| {
                    let start = i as u32 * 4;
                    let mut by_slot = BTreeMap::new();
                    for (c, dh, hp, rks) in &per[i] {
                        let height = start + dh;
                        by_slot.entry((height, *c)).or_insert_with(|| Blk {
                            chain: *c,
                            height,
                            hash: h(*hp, 0x11 ^ *c ^ height as u8),
                            rks: rks.iter().map(|(p, t, e)| (h(*p, *t), *e)).collect(),
                        });
                    }
                    (start, start + 4, sorted[i], by_slot.into_values().collect())
                })
                .collect()
        })
}

/// `by_rk` answers, then `by_block_hash` answers.
type Answers = (Vec<Option<u64>>, Vec<Option<(u8, u32)>>);

fn answers(s: &Segments, rks: &[Hash32], bhs: &[Hash32]) -> Answers {
    (
        rks.iter().map(|k| s.by_rk(k).unwrap()).collect(),
        bhs.iter().map(|k| s.by_block_hash(k).unwrap()).collect(),
    )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    #[test]
    fn lookups_with_and_without_key_filters_agree(
        ranges in store(),
        absent in proptest::collection::vec((0u64..80, any::<u8>()), 40),
        which in 0usize..4,
        at in any::<usize>(),
    ) {
        let d = tempfile::tempdir().unwrap();
        write_store(d.path(), &ranges);
        let seg = d.path().join("segments");
        let full = open(&seg);
        prop_assert!(full.sender_ready());
        prop_assert!(full.ranges.iter().all(|r| r.filter.is_some() && r.sndr.is_some()));
        prop_assert!(full.key_filter_bytes() > 0);
        let rep0 = kdx_segment::check(&seg, true).unwrap();
        prop_assert!(rep0.ok(), "{:?}", rep0);

        // Every stored key, shared-prefix neighbours and keys that are not stored.
        let mut rks: Vec<Hash32> = Vec::new();
        let mut bhs: Vec<Hash32> = Vec::new();
        for (_, _, _, blocks) in &ranges {
            for b in blocks {
                bhs.push(b.hash);
                rks.extend(b.rks.iter().map(|(k, _)| *k));
            }
        }
        let stored = (rks.len(), bhs.len());
        for (p, t) in &absent {
            rks.push(h(*p, *t));
            bhs.push(h(*p, *t));
        }
        let want = answers(&full, &rks, &bhs);
        prop_assert!(want.0[..stored.0].iter().all(Option::is_some));
        prop_assert!(want.1[..stored.1].iter().all(Option::is_some));

        let m = full.manifest.clone();
        for dmg in [Damage::Stripped, Damage::FlippedByte(at), Damage::BadHeaderRehashed, Damage::Missing] {
            let d2 = tempfile::tempdir().unwrap();
            let seg2 = d2.path().join("segments");
            copy_dir(&seg, &seg2);
            // One range, or every range.
            let victims: Vec<&RangeEntry> = if which < m.entries.len() {
                vec![&m.entries[which]]
            } else {
                m.entries.iter().collect()
            };
            for e in &victims {
                damage(&seg2, e, dmg);
            }
            let s = open(&seg2);
            for (r, e) in s.ranges.iter().zip(&m.entries) {
                let hit = victims.iter().any(|v| v.start == e.start);
                prop_assert_eq!(r.filter.is_some(), !hit, "{:?}", dmg);
            }
            prop_assert_eq!(answers(&s, &rks, &bhs), want.clone(), "{:?}", dmg);
            let rep = kdx_segment::check(&seg2, true).unwrap();
            match dmg {
                // A range from before FIX-2 is a healthy range, it just lacks the sender view.
                Damage::Stripped => {
                    prop_assert!(rep.ok(), "{:?}", rep);
                    prop_assert!(!s.sender_ready());
                }
                _ => prop_assert!(!rep.ok()),
            }
        }
    }
}

/// The filter is used: it admits the stored keys and rules almost every other key out, and
/// the filtered lookups still find the stored ones.
#[test]
fn a_range_whose_filter_says_no_is_skipped() {
    let blocks = vec![Blk {
        chain: 0,
        height: 0,
        hash: h(7, 1),
        rks: vec![(h(9, 2), 1)],
    }];
    let d = tempfile::tempdir().unwrap();
    write_store(d.path(), &[(0, 4, false, blocks)]);
    let s = open(&d.path().join("segments"));
    let r = &s.ranges[0];
    assert!(r.may_have_rk(&h(9, 2)) && r.may_have_bh(&h(7, 1)));
    // Among many absent keys most are ruled out (false positive rate ~2^-8).
    let out = (100..1100u64).filter(|&p| !r.may_have_rk(&h(p, 3))).count();
    assert!(out > 950, "{out}");
    assert_eq!(s.by_rk(&h(9, 2)).unwrap(), Some(0));
    assert_eq!(s.by_block_hash(&h(7, 1)).unwrap(), Some((0, 0)));
}

/// A listed `sndr.pst` with a bad header fails the open (fail-closed), unlike a bad filter.
#[test]
fn a_listed_sndr_file_with_a_bad_header_fails_the_open() {
    let blocks = vec![Blk {
        chain: 0,
        height: 1,
        hash: h(7, 1),
        rks: vec![(h(9, 2), 1)],
    }];
    let d = tempfile::tempdir().unwrap();
    write_store(d.path(), &[(0, 4, false, blocks)]);
    let seg = d.path().join("segments");
    let m = Manifest::load(&seg).unwrap().unwrap();
    let e = &m.entries[0];
    let p = seg.join(file_name(NET, FileKind::Sndr, e.start, e.end));
    let mut b = std::fs::read(&p).unwrap();
    b[0] ^= 0xff;
    std::fs::write(&p, &b).unwrap();
    assert!(Segments::open(&seg, Arc::new(MemDict::new())).is_err());
    // Missing instead: the range opens without it.
    std::fs::remove_file(&p).unwrap();
    let s = open(&seg);
    assert!(s.ranges[0].sndr.is_none() && !s.sender_ready());
    assert!(s.ranges[0].filter.is_some());
}
