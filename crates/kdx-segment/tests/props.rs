//! Property tests for the segment formats (plan Task 13, T-1): keys.ef, posting lists, adjacent
//! refs, the rk/bh index (PtrHash and sorted), block files, the manifest and file checksums.
//! Each property checks against a plain reference (`BTreeSet`, `BTreeMap`, a prefix-sum, the
//! `blake3` crate), never against the code under test.

#[macro_use]
#[path = "../../kdx-types/tests/common/prop.rs"]
mod prop;
use prop::*;

use std::collections::{BTreeMap, BTreeSet};

use kdx_decode::Family;
use kdx_segment::adj::{record_block_hash, AdjTracker};
use kdx_segment::blockfile::{row_record, BlockFile, BlockFileWriter};
use kdx_segment::files::{blake3_file, file_name, range_file_names, FileKind};
use kdx_segment::keys::{write_keys, Keys};
use kdx_segment::manifest::{Manifest, RangeEntry, RangeKind};
use kdx_segment::phf::{fingerprint, prefix, write_index, Index, IndexKey, IndexKind, KeyIndex};
use kdx_segment::postings::{
    decode_list, encode_list, encoding_for, ListMemo, Postings, PostingsWriter, MEMO_CAP,
};
use kdx_segment::writer::{FileStat, RangeInfo};
use kdx_types::record::{decode_block_with, encode_block_with, BlockRecord};
use kdx_types::Hash32;

fn flip(bytes: &mut [u8], at: &proptest::sample::Index, x: u8) -> usize {
    let i = at.index(bytes.len());
    bytes[i] ^= x;
    i
}

// ---------------------------------------------------------------- keys.ef

fn counts() -> impl Strategy<Value = Vec<u32>> {
    proptest::collection::vec(
        prop_oneof![3 => 0u32..4, 1 => 0u32..200, 1 => Just(0u32)],
        0..60,
    )
}

props! {
    fn keys_equal_a_prefix_sum_reference(counts in counts(), width in 1u32..64) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("keys.ef");
        write_keys(&p, width, &counts).unwrap();
        let k = Keys::open(&p).unwrap();
        // Reference: prefix sums, and `slot_of(t)` = the last slot starting at or before t.
        let mut start = vec![0u64];
        for c in &counts {
            start.push(start.last().unwrap() + *c as u64);
        }
        let total = *start.last().unwrap();
        prop_assert_eq!(k.slot_width, width);
        prop_assert_eq!(k.slots(), counts.len());
        prop_assert_eq!(k.tx_count(), total);
        for (s, v) in start.iter().enumerate() {
            prop_assert_eq!(k.first(s), Some(*v));
        }
        prop_assert_eq!(k.first(counts.len() + 1), None);
        for (s, c) in counts.iter().enumerate() {
            prop_assert_eq!(k.count(s), *c as u64);
        }
        let probe = |t: u64| (0..counts.len()).rev().find(|&s| start[s] <= t);
        for t in 0..total.min(400) {
            prop_assert_eq!(k.slot_of(t), probe(t));
        }
        prop_assert_eq!(k.slot_of(total), None);
        prop_assert_eq!(k.slot_of(u64::MAX), None);
    }

    fn keys_corrupt_headers_are_rejected(counts in counts(), at in any::<proptest::sample::Index>(), x in 1u8..=255, cut in any::<proptest::sample::Index>()) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("keys.ef");
        write_keys(&p, 8, &counts).unwrap();
        let good = std::fs::read(&p).unwrap();
        // Magic, version and slot count flips are refused (slot_width is free data).
        // Flips inside the Elias-Fano payload are not tried: sucds allocates `Vec::with_capacity`
        // from a stored length, so a flipped length aborts the process (finding, see below).
        let mut b = good.clone();
        let i = at.index(17);
        b[i] ^= x;
        std::fs::write(&p, &b).unwrap();
        let r = Keys::open(&p);
        if i < 5 || (9..17).contains(&i) {
            prop_assert!(r.is_err(), "flip at {} accepted", i);
        }
        // Whatever opens answers every query without panicking.
        if let Ok(k) = r {
            for s in 0..k.slots().min(100) + 2 {
                let _ = (k.first(s), k.count(s));
            }
            let _ = (k.tx_count(), k.slot_of(0), k.slot_of(5), k.slot_of(u64::MAX));
        }
        // A strict prefix of the file never opens as the same keys.
        let n = cut.index(good.len());
        std::fs::write(&p, &good[..n]).unwrap();
        if let Ok(k) = Keys::open(&p) {
            prop_assert!(k.slots() != counts.len() || n == good.len(), "truncated file accepted");
        }
    }
}

// ---------------------------------------------------------------- posting lists

/// A range size and a few entities with sets of relative TxNums, shaped to hit the delta,
/// Elias-Fano and Roaring (array, bitmap) encodings, in one or several 65536-containers.
/// One case in five takes a mainnet-sized universe (`wide_lists`, m3).
fn lists() -> impl Strategy<Value = (u32, BTreeMap<u64, BTreeSet<u32>>)> {
    prop_oneof![4 => small_lists(), 1 => wide_lists()]
}

/// Mainnet ranges hold millions of txs (m3). 1M..2.2M: Roaring lists (more than `r / 32`
/// entries) whose containers reach key 33, as strided sets (array containers) and dense runs
/// near the top (bitmap/run containers); 4M..`u32::MAX / 2`: delta and Elias-Fano lists over
/// a large universe, spread or packed near the top.
fn wide_lists() -> impl Strategy<Value = (u32, BTreeMap<u64, BTreeSet<u32>>)> {
    prop_oneof![1_000_000u32..2_200_000, 4_000_000u32..u32::MAX / 2].prop_flat_map(|r| {
        let roaring = r < 4_000_000;
        let shape = prop_oneof![
            proptest::collection::btree_set(0..r, 0..17),
            proptest::collection::btree_set(0..r, 17..2500),
            proptest::collection::btree_set(r - r / 64..r, 17..2500),
            (1u32..4000, 0u32..70_000).prop_map(move |(extra, gap)| {
                let len = if roaring {
                    r / 32 + extra
                } else {
                    3000 + extra
                };
                let end = r - gap.min(r - len);
                (end - len..end).collect()
            }),
            (24u32..32, 0u32..1000).prop_map(move |(step, off)| {
                if roaring {
                    (off..r).step_by(step as usize).collect()
                } else {
                    (r - 2000 * step..r).step_by(step as usize).collect()
                }
            }),
        ];
        (
            Just(r),
            proptest::collection::btree_map(any::<u64>(), shape, 1..4),
        )
    })
}

/// m3: the wide generator does reach what it is for (fixed seed, 64 samples): Roaring lists
/// with a container key above 2, and Elias-Fano lists in a universe over 4M.
#[test]
fn wide_lists_reach_high_roaring_containers_and_large_ef_universes() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::{Config, RngSeed, TestRunner};
    let mut runner = TestRunner::new(Config {
        rng_seed: RngSeed::Fixed(prop::seed_of("wide_lists")),
        ..Config::default()
    });
    let (mut roaring_high, mut ef_large) = (0, 0);
    for _ in 0..64 {
        let (r, map) = wide_lists().new_tree(&mut runner).unwrap().current();
        for set in map.values() {
            match kdx_segment::postings::encoding_for(set.len(), r as u64) {
                2 if set.last().is_some_and(|&v| v >> 16 > 2) => roaring_high += 1,
                1 if r > 4_000_000 => ef_large += 1,
                _ => {}
            }
        }
    }
    assert!(
        roaring_high > 0 && ef_large > 0,
        "{roaring_high} {ef_large}"
    );
}

fn small_lists() -> impl Strategy<Value = (u32, BTreeMap<u64, BTreeSet<u32>>)> {
    prop_oneof![64u32..3000, 66_000u32..140_000].prop_flat_map(|r| {
        let cap = |n: usize| (r as usize).min(n) + 1;
        let shape = prop_oneof![
            proptest::collection::btree_set(0..r, 0..cap(17)),
            proptest::collection::btree_set(0..r, 0..cap(300)),
            proptest::collection::btree_set(0..r, 0..cap(2500)),
            (0..r, 3000u32..9000).prop_map(move |(s, l)| (s..s.saturating_add(l).min(r)).collect()),
        ];
        (
            Just(r),
            proptest::collection::btree_map(any::<u64>(), shape, 1..6),
        )
    })
}

/// Like `lists`, but only delta and Roaring lists (no Elias-Fano list).
fn lists_without_ef() -> impl Strategy<Value = (u32, BTreeMap<u64, BTreeSet<u32>>)> {
    prop_oneof![64u32..3000, 66_000u32..140_000].prop_flat_map(|r| {
        let shape = prop_oneof![
            proptest::collection::btree_set(0..r, 0..17),
            (0..r, 3000u32..9000).prop_map(move |(s, l)| (s..s.saturating_add(l).min(r)).collect()),
        ];
        (
            Just(r),
            proptest::collection::btree_map(any::<u64>(), shape, 1..6),
        )
    })
}

props! {
    fn posting_lists_roundtrip_in_every_encoding((r, map) in lists()) {
        for set in map.values() {
            let v: Vec<u32> = set.iter().copied().collect();
            let mut b = Vec::new();
            encode_list(&v, r as u64, &mut b).unwrap();
            prop_assert_eq!(b[0], encoding_for(v.len(), r as u64));
            prop_assert_eq!(decode_list(&b).unwrap(), v);
        }
    }

    fn posting_file_reads_equal_btreeset_reference((r, map) in lists(), base in 0u64..(1 << 40), seed in any::<u64>(), bounds in proptest::collection::vec(any::<u32>(), 4)) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("acct.pst");
        let mut w = PostingsWriter::create(&p, Family::Acct, r as u64).unwrap();
        for (e, set) in &map {
            let v: Vec<u32> = set.iter().copied().collect();
            w.push(*e, &v).unwrap();
        }
        w.finish().unwrap();
        let pf = Postings::open(&p, base).unwrap();
        prop_assert_eq!(pf.entities(), map.len());
        let memo = ListMemo::new();
        for (i, (e, set)) in map.iter().enumerate() {
            prop_assert_eq!(pf.entity_at(i), *e);
            let full: Vec<u32> = set.iter().copied().collect();
            prop_assert_eq!(pf.list_at(i).unwrap(), full.clone());
            prop_assert!(pf.lazy_walk_matches(i, &full, seed).unwrap());
            // Newest first, absolute TxNums.
            let want: Vec<u64> = set.iter().rev().map(|&v| base + v as u64).collect();
            let it = pf.list(*e).unwrap();
            prop_assert_eq!(it.len(), want.len());
            prop_assert_eq!(it.collect::<Vec<_>>(), want);
            // Oldest first (m5, `inspect postings --oldest`): ascending.
            let asc: Vec<u64> = set.iter().map(|&v| base + v as u64).collect();
            prop_assert_eq!(pf.list(*e).unwrap().oldest_first().collect::<Vec<_>>(), asc);
            // Any bound: the lazy walk equals the filtered full decode (R4), with and without
            // a memo, and from the container edges (the first two and the top one).
            let mut bs: Vec<u32> = bounds.iter().map(|b| b % (r + 2)).collect();
            let top = (r >> 16) << 16;
            bs.extend([0, 65_535, 65_536, r.saturating_sub(1), top, top.saturating_sub(1)]);
            for b in bs {
                let want: Vec<u64> = set.range(..=b).rev().map(|&v| base + v as u64).collect();
                let it = pf.list_le(*e, Some(b)).unwrap().unwrap();
                prop_assert_eq!(it.len(), want.len());
                prop_assert_eq!(it.collect::<Vec<_>>(), want.clone());
                let asc: Vec<u64> = set.range(..=b).map(|&v| base + v as u64).collect();
                let old = pf.list_le(*e, Some(b)).unwrap().unwrap().oldest_first();
                prop_assert_eq!(old.collect::<Vec<_>>(), asc);
                let m = pf.list_le_memo(*e, Some(b), Some((&memo, 7))).unwrap().unwrap();
                prop_assert_eq!(m.collect::<Vec<_>>(), want);
            }
        }
        let (_, _, bytes) = memo.stats();
        prop_assert!(bytes <= MEMO_CAP);
        // An entity that is not there has no list.
        let absent = (0u64..).find(|e| !map.contains_key(e)).unwrap();
        prop_assert!(pf.list(absent).is_none());
        prop_assert!(pf.list_le(absent, Some(3)).unwrap().is_none());
    }

    fn posting_garbage_never_panics(b in proptest::collection::vec(any::<u8>(), 0..80), enc in prop_oneof![Just(0u8), Just(2u8), 3u8..=255]) {
        // Encoding 1 (Elias-Fano) is left out: see the sucds finding below.
        let mut v = vec![enc];
        v.extend_from_slice(&b);
        let _ = decode_list(&v);
    }

    fn posting_file_corruption_never_panics((r, map) in lists_without_ef(), at in any::<proptest::sample::Index>(), x in 1u8..=255) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("acct.pst");
        let mut w = PostingsWriter::create(&p, Family::Acct, r as u64).unwrap();
        for (e, set) in &map {
            w.push(*e, &set.iter().copied().collect::<Vec<_>>()).unwrap();
        }
        w.finish().unwrap();
        let mut b = std::fs::read(&p).unwrap();
        // Flip inside the entity column only. The lists, the Elias-Fano offsets and the trailer
        // are left alone: a flipped list encoding byte or length reaches sucds, which panics or
        // aborts on garbage (finding, see below).
        let n = u64::from_le_bytes(b[b.len() - 24..b.len() - 16].try_into().unwrap()) as usize;
        let lists_len = u64::from_le_bytes(b[b.len() - 16..b.len() - 8].try_into().unwrap()) as usize;
        let i = 14 + lists_len + at.index(8 * n);
        b[i] ^= x;
        std::fs::write(&p, &b).unwrap();
        if let Ok(pf) = Postings::open(&p, 0) {
            for i in 0..pf.entities().min(8) {
                let e = pf.entity_at(i);
                let _ = pf.list_at(i);
                for bound in [None, Some(0), Some(70_000), Some(u32::MAX)] {
                    if let Ok(Some(it)) = pf.list_le(e, bound) {
                        let _ = it.take(200_000).count();
                    }
                }
                let _ = pf.lazy_walk_matches(i, &[1, 2, 3], 9);
            }
        }
    }
}

#[test]
fn list_memo_stays_under_its_cap() {
    // Roaring lists with one value in each of 65,536 containers: each parsed header weighs
    // about 1.5 MB, so 50 of them would be 75 MB. The memo keeps at most MEMO_CAP.
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("pk.pst");
    // range_txs = 1 makes every list with more than 16 entries a Roaring list.
    let mut w = PostingsWriter::create(&p, Family::Pk, 1).unwrap();
    let vals: Vec<u32> = (0..65_536u32).map(|k| k << 16 | (k % 7)).collect();
    for e in 0..50u64 {
        w.push(e, &vals).unwrap();
    }
    w.finish().unwrap();
    let pf = Postings::open(&p, 0).unwrap();
    let memo = ListMemo::new();
    for e in 0..50u64 {
        let it = pf
            .list_le_memo(e, Some(u32::MAX), Some((&memo, 0)))
            .unwrap()
            .unwrap();
        assert_eq!(it.len(), vals.len());
        // Reference: the last values of the list, newest first.
        let want: Vec<u64> = vals.iter().rev().take(3).map(|&v| u64::from(v)).collect();
        assert_eq!(it.take(3).collect::<Vec<u64>>(), want);
    }
    let (hits, misses, bytes) = memo.stats();
    assert!(
        bytes <= MEMO_CAP,
        "memo holds {bytes} bytes, cap {MEMO_CAP}"
    );
    assert!(bytes > 0);
    assert_eq!(hits, 0);
    assert_eq!(misses, 50);
    // A kept entry is a hit the second time; an entry past the cap is parsed again, correctly.
    let first = pf
        .list_le_memo(0, None, Some((&memo, 0)))
        .unwrap()
        .unwrap()
        .count();
    let last = pf
        .list_le_memo(49, None, Some((&memo, 0)))
        .unwrap()
        .unwrap()
        .count();
    assert_eq!((first, last), (65_536, 65_536));
    assert!(memo.stats().0 >= 1);
}

// ---------------------------------------------------------------- adjacent refs

props! {
    fn adjacent_refs_roundtrip_through_the_tracker(
        start in 1u64..1_000_000,
        steps in proptest::collection::vec(proptest::collection::vec((any::<bool>(), arr::<32>()), 4), 1..6),
        own in proptest::collection::vec(arr::<32>(), 24),
    ) {
        // Four chains, `steps.len()` consecutive heights. `(true, _)` makes the adjacent equal to
        // the previous height's block hash of that chain; `(false, h)` is a random hash.
        let mut truth: BTreeMap<(u8, u64), Hash32> = BTreeMap::new();
        let mut enc: Vec<(BlockRecord, Vec<u8>)> = Vec::new();
        let mut tracker = AdjTracker::default();
        for (hi, step) in steps.iter().enumerate() {
            let h = start + hi as u64;
            tracker.begin(h);
            for chain in 0..4u8 {
                let (same, rnd) = &step[chain as usize];
                let hash = Hash32(own[hi * 4 + chain as usize]);
                let adj_chain = (chain + 1) % 4;
                let adj_hash = match truth.get(&(adj_chain, h - 1)) {
                    Some(prev) if *same => *prev,
                    _ => Hash32(*rnd),
                };
                let r = BlockRecord {
                    hash,
                    chain,
                    height: h,
                    adjacents: vec![(adj_chain, adj_hash)],
                    ..Default::default()
                };
                // Reference: a ref is possible only when the previous height of this range holds
                // exactly that hash for that chain (the first height of a range is always raw).
                let is_ref = hi > 0 && truth.get(&(adj_chain, h - 1)) == Some(&adj_hash);
                let mut b = Vec::new();
                encode_block_with(&r, &mut b, &|c, hh| tracker.is_ref(c, hh));
                let mut raw = Vec::new();
                kdx_types::record::encode_block(&r, &mut raw);
                // Segment form: a flag byte per adjacent (+1), a ref drops the 32-byte hash.
                prop_assert_eq!(b.len() as i64, raw.len() as i64 + 1 - if is_ref { 32 } else { 0 });
                prop_assert_eq!(record_block_hash(&b).unwrap(), hash);
                tracker.done(chain, hash);
                truth.insert((chain, h), hash);
                enc.push((r, b));
            }
        }
        for (r, b) in &enc {
            let back = decode_block_with(b, &|c, h| truth.get(&(c, h)).copied()).unwrap();
            prop_assert_eq!(&back, r);
        }
    }
}

// ---------------------------------------------------------------- rk / bh index (PtrHash)

fn tail() -> impl Strategy<Value = [u8; 24]> {
    arr::<24>()
}

fn key_of(prefix: u64, tail: &[u8; 24]) -> Hash32 {
    let mut k = [0u8; 32];
    k[..8].copy_from_slice(&prefix.to_be_bytes());
    k[8..].copy_from_slice(tail);
    Hash32(k)
}

props! {
    fn index_finds_every_key_and_rejects_non_keys(
        keys in proptest::collection::btree_map(any::<u64>(), tail(), 0..300),
        outsiders in proptest::collection::vec(arr::<32>(), 40),
        force in any::<bool>(),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("rk.idx");
        // Offsets are the position in prefix order, as in a range; unique prefixes.
        let all: Vec<Hash32> = keys.iter().map(|(p, t)| key_of(*p, t)).collect();
        let ik: Vec<IndexKey> = all.iter().enumerate().map(|(i, k)| IndexKey::new(k, i as u32)).collect();
        let kind = write_index(&p, ik, force).unwrap();
        if force || all.is_empty() {
            prop_assert_eq!(kind, IndexKind::Sorted);
        }
        let idx = Index::open(&p).unwrap();
        prop_assert_eq!(idx.kind(), kind);
        prop_assert_eq!(idx.len(), all.len());
        let mut c = Vec::new();
        for (i, k) in all.iter().enumerate() {
            idx.candidates(k, &mut c);
            prop_assert_eq!(&c[..], &[i as u32][..], "key {} not found", i);
            prop_assert_eq!(idx.get(k), Some(i as u32));
        }
        for o in outsiders {
            let o = Hash32(o);
            if keys.contains_key(&prefix(&o)) {
                continue;
            }
            idx.candidates(&o, &mut c);
            for &cand in &c {
                // A candidate for a non-key is only ever a fingerprint collision, which the
                // caller's full-key comparison then rejects.
                prop_assert_eq!(idx.kind(), IndexKind::PtrHash);
                let stored = &all[cand as usize];
                prop_assert_eq!(fingerprint(stored), fingerprint(&o));
                prop_assert!(*stored != o);
            }
        }
    }

    fn sorted_index_keeps_shared_prefixes(
        entries in proptest::collection::vec((0u64..5, tail(), any::<u32>()), 1..80),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("bh.idx");
        let ik: Vec<IndexKey> = entries.iter().map(|(pre, t, o)| IndexKey::new(&key_of(*pre, t), *o)).collect();
        let kind = write_index(&p, ik, false).unwrap();
        let idx = Index::open(&p).unwrap();
        // Reference: offsets grouped by prefix, ascending (prefix, offset) order.
        let mut by_prefix: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        for (pre, _, o) in &entries {
            by_prefix.entry(*pre).or_default().push(*o);
        }
        for v in by_prefix.values_mut() {
            v.sort_unstable();
        }
        // A shared prefix cannot be a PtrHash key: the build falls back to the sorted form.
        if by_prefix.values().any(|v| v.len() > 1) {
            prop_assert_eq!(kind, IndexKind::Sorted);
        }
        let mut c = Vec::new();
        for (pre, t, _) in &entries {
            idx.candidates(&key_of(*pre, t), &mut c);
            prop_assert_eq!(&c, &by_prefix[pre]);
        }
        idx.candidates(&key_of(9, &[0; 24]), &mut c);
        // Absent prefix: nothing, or (PtrHash only) a fingerprint collision for the caller to drop.
        prop_assert!(c.is_empty() || kind == IndexKind::PtrHash);
    }

    fn index_corruption_is_refused_or_harmless(
        keys in proptest::collection::btree_map(any::<u64>(), tail(), 40..120),
        at in any::<proptest::sample::Index>(),
        x in 1u8..=255,
        cut in any::<proptest::sample::Index>(),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("rk.idx");
        let all: Vec<Hash32> = keys.iter().map(|(p, t)| key_of(*p, t)).collect();
        let ik: Vec<IndexKey> = all.iter().enumerate().map(|(i, k)| IndexKey::new(k, i as u32)).collect();
        write_index(&p, ik, false).unwrap();
        let good = std::fs::read(&p).unwrap();
        // Truncation is always refused (M5).
        std::fs::write(&p, &good[..cut.index(good.len())]).unwrap();
        prop_assert!(Index::open(&p).is_err());
        // A flip in magic, version, kind or key count is refused; any other flip opens at most,
        // and lookups never panic or read out of bounds.
        let mut b = good.clone();
        let i = flip(&mut b, &at, x);
        std::fs::write(&p, &b).unwrap();
        let r = Index::open(&p);
        if i < 14 {
            prop_assert!(r.is_err(), "header flip at {} accepted", i);
        }
        if let Ok(idx) = r {
            let mut c = Vec::new();
            for k in &all {
                idx.candidates(k, &mut c);
            }
            idx.candidates(&Hash32([7; 32]), &mut c);
        }
    }
}

// ---------------------------------------------------------------- block files

props! {
    fn blockfile_roundtrip(
        recs in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..200), 0..70),
        per_row in 1u32..9,
        present in proptest::collection::vec(any::<u8>(), 0..4),
        slots in any::<u64>(),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("blocks.seg");
        let mut w = BlockFileWriter::create(&p, "blocks", Vec::new(), 3, per_row).unwrap();
        for r in &recs {
            w.push(r.clone()).unwrap();
        }
        let (n, _) = w.finish(present.clone(), slots).unwrap();
        prop_assert_eq!(n, recs.len() as u64);
        let f = BlockFile::open(&p).unwrap();
        prop_assert_eq!(f.records(), recs.len() as u64);
        prop_assert_eq!(f.rows(), recs.len().div_ceil(per_row as usize));
        prop_assert_eq!(&f.header.present, &present);
        prop_assert_eq!(f.header.slots, slots);
        for (i, r) in recs.iter().enumerate() {
            prop_assert_eq!(&f.record(i as u64).unwrap(), r);
        }
        prop_assert!(f.record(recs.len() as u64).is_err());
        prop_assert!(f.row(f.rows()).is_err());
    }

    fn row_garbage_never_panics(row in proptest::collection::vec(any::<u8>(), 0..64), i in 0usize..20) {
        let _ = row_record(&row, i);
        let _ = kdx_segment::blockfile::row_len(&row);
    }

    fn blockfile_data_corruption_never_panics(
        recs in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 1..100), 1..30),
        file in 0usize..2,
        at in any::<proptest::sample::Index>(),
        x in 1u8..=255,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.seg");
        let mut w = BlockFileWriter::create(&p, "txs", Vec::new(), 3, 4).unwrap();
        for r in &recs {
            w.push(r.clone()).unwrap();
        }
        w.finish(Vec::new(), 0).unwrap();
        // The data file and the offsets file (the .conf header is bincode and read by reth).
        let target = if file == 0 { p.clone() } else { p.with_extension("off") };
        let mut b = std::fs::read(&target).unwrap();
        flip(&mut b, &at, x);
        std::fs::write(&target, &b).unwrap();
        if let Ok(f) = BlockFile::open(&p) {
            for i in 0..recs.len() as u64 {
                let _ = f.record(i);
            }
        }
    }
}

// ---------------------------------------------------------------- manifest

fn entries() -> impl Strategy<Value = Vec<RangeEntry>> {
    (
        0u32..1_000,
        0u64..1_000_000,
        proptest::collection::vec(
            (1u32..100_000, 0u64..5_000_000, 0u64..100_000, any::<bool>()),
            1..8,
        ),
    )
        .prop_map(|(h0, t0, steps)| {
            let (mut h, mut t) = (h0, t0);
            let mut v = Vec::new();
            for (len, txs, blocks, step) in steps {
                v.push(RangeEntry {
                    kind: if step {
                        RangeKind::Step
                    } else {
                        RangeKind::Range
                    },
                    start: h,
                    end: h + len,
                    first_txnum: t,
                    tx_count: txs,
                    block_count: blocks,
                });
                h += len;
                t += txs;
            }
            v
        })
}

fn manifest_of(entries: &[RangeEntry]) -> Manifest {
    let mut m = Manifest::new("mainnet01");
    for e in entries {
        m.push(e.clone()).unwrap();
    }
    m
}

props! {
    fn manifest_roundtrip_and_lookups(es in entries(), q in proptest::collection::vec(any::<u64>(), 6)) {
        let m = manifest_of(&es);
        m.validate().unwrap();
        let bytes = serde_json::to_vec(&m).unwrap();
        prop_assert_eq!(Manifest::parse(&bytes).unwrap(), m.clone());
        let dir = tempfile::tempdir().unwrap();
        m.commit(dir.path()).unwrap();
        prop_assert_eq!(Manifest::load(dir.path()).unwrap().unwrap(), m.clone());
        // Lookups equal a linear scan.
        let last = es.last().unwrap();
        let tmax = last.first_txnum + last.tx_count;
        for raw in q {
            let t = raw % (tmax + 3);
            let want = es.iter().position(|e| e.first_txnum <= t && t < e.first_txnum + e.tx_count);
            prop_assert_eq!(m.entry_of_txnum(t), want);
            let h = (raw % (last.end as u64 + 3)) as u32;
            let want = es.iter().position(|e| e.start <= h && h < e.end);
            prop_assert_eq!(m.entry_of_height(h), want);
        }
    }

    fn manifest_inconsistencies_are_refused(es in entries(), which in 0usize..6) {
        let m = manifest_of(&es);
        let mut bad = m.clone();
        match which {
            0 => bad.frontier += 1,
            1 => bad.next_txnum += 1,
            2 => bad.format_version += 1,
            3 => bad.entries[0].end = bad.entries[0].start,
            4 => bad.entries.last_mut().unwrap().first_txnum += 1,
            _ => {
                // A gap in heights.
                bad.entries.last_mut().unwrap().start += 1;
                if bad.entries.len() == 1 {
                    bad.entries[0].start = bad.entries[0].end;
                }
            }
        }
        prop_assert!(Manifest::parse(&serde_json::to_vec(&bad).unwrap()).is_err());
    }

    fn manifest_replace_span_merges_exactly(es in entries(), i in any::<proptest::sample::Index>(), j in any::<proptest::sample::Index>()) {
        let mut m = manifest_of(&es);
        let (a, b) = (i.index(es.len()), j.index(es.len()));
        let (a, b) = (a.min(b), a.max(b));
        let merged = RangeEntry {
            kind: RangeKind::Range,
            start: es[a].start,
            end: es[b].end,
            first_txnum: es[a].first_txnum,
            tx_count: es[a..=b].iter().map(|e| e.tx_count).sum(),
            block_count: es[a..=b].iter().map(|e| e.block_count).sum(),
        };
        let old = m.replace_span(merged.clone()).unwrap();
        prop_assert_eq!(&old[..], &es[a..=b]);
        m.validate().unwrap();
        prop_assert_eq!(m.entries.len(), es.len() - (b - a));
        // A merge with the wrong tx count is refused and leaves the manifest as it was.
        let mut m2 = manifest_of(&es);
        let mut wrong = merged;
        wrong.tx_count += 1;
        prop_assert!(m2.replace_span(wrong).is_err());
        prop_assert_eq!(m2, manifest_of(&es));
    }

    fn manifest_hostile_numbers_never_panic(
        first in prop_oneof![any::<u64>(), Just(u64::MAX), Just(u64::MAX - 1)],
        count in prop_oneof![any::<u64>(), Just(u64::MAX), Just(1u64)],
        h in any::<u32>(),
    ) {
        let json = format!(
            r#"{{"format_version":2,"network":"n","entries":[{{"kind":"step","start":{h},"end":{h2},"first_txnum":{first},"tx_count":{count},"block_count":0}}],"frontier":{h2},"next_txnum":{nt}}}"#,
            h2 = h.wrapping_add(1),
            nt = first.wrapping_add(count),
        );
        let _ = Manifest::parse(json.as_bytes());
    }

    fn manifest_garbage_never_panics(b in proptest::collection::vec(any::<u8>(), 0..200)) {
        let _ = Manifest::parse(&b);
    }
}

// ---------------------------------------------------------------- checksums (blake3)

props! {
    fn check_refuses_a_wrong_blake3(
        contents in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 1..300), 22),
        victim in any::<proptest::sample::Index>(),
        at in any::<proptest::sample::Index>(),
        x in 1u8..=255,
        mode in 0usize..2,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (net, start, end) = ("mainnet01", 0u32, 10_000u32);
        let names: Vec<String> = range_file_names(net, start, end);
        let range_name = file_name(net, FileKind::Range, start, end);
        let others: Vec<&String> = names.iter().filter(|n| **n != range_name).collect();
        let mut files = BTreeMap::new();
        for (n, c) in others.iter().zip(&contents) {
            std::fs::write(dir.path().join(n), c).unwrap();
            let hash = blake3_file(&dir.path().join(n)).unwrap();
            // Reference: the blake3 crate's own hash of the bytes.
            prop_assert_eq!(&hash, &blake3::hash(c).to_hex().to_string());
            files.insert((*n).clone(), FileStat { bytes: c.len() as u64, blake3: hash });
        }
        let info = RangeInfo {
            format_version: 2,
            network: net.into(),
            kind: RangeKind::Range,
            start,
            end,
            first_txnum: 0,
            tx_count: 1,
            block_count: 1,
            slot_width: 20,
            block_records: 64,
            level: 3,
            txs_dict_id: 0,
            txs_dict_bytes: 0,
            blocks_dict_id: 0,
            blocks_dict_bytes: 0,
            rk_index: "sorted".into(),
            bh_index: "sorted".into(),
            rk_keys: 0,
            bh_keys: 0,
            cmd_reencoded: 0,
            postings: BTreeMap::new(),
            files,
        };
        std::fs::write(dir.path().join(&range_name), toml::to_string(&info).unwrap()).unwrap();
        let mut m = Manifest::new(net);
        m.push(info.entry()).unwrap();
        m.commit(dir.path()).unwrap();
        let rep = kdx_segment::check(dir.path(), false).unwrap();
        prop_assert!(rep.ok(), "{:?}", rep.errors);
        let name = others[victim.index(others.len())].clone();
        let path = dir.path().join(&name);
        let mut b = std::fs::read(&path).unwrap();
        if mode == 0 {
            // Same size, one byte changed: only the checksum can tell.
            flip(&mut b, &at, x);
        } else {
            b.pop();
        }
        std::fs::write(&path, &b).unwrap();
        let rep = kdx_segment::check(dir.path(), false).unwrap();
        prop_assert!(!rep.ok());
        prop_assert!(rep.blake3_bad.iter().any(|s| s.starts_with(&name)), "{:?}", rep.blake3_bad);
    }
}

/// The fuzz paths of `ef_keys` and `postings_list` over one input; a crash is the failure.
fn sucds_fuzz_paths(name: &str, data: &[u8], dir: &std::path::Path) {
    if name.contains("ef_keys") {
        let p = dir.join("keys.ef");
        std::fs::write(&p, data).unwrap();
        if let Ok(k) = Keys::open(&p) {
            for s in 0..k.slots().min(64) + 2 {
                let _ = (k.first(s), k.count(s));
            }
            for t in [0, 1, k.tx_count() / 2, k.tx_count(), u64::MAX] {
                let _ = k.slot_of(t);
            }
        }
    } else if data.len() >= 12 {
        let bound = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let body = &data[12..];
        let _ = decode_list(body);
        let p = dir.join("postings");
        std::fs::write(&p, body).unwrap();
        if let Ok(pf) = Postings::open(&p, 1_000) {
            for i in 0..pf.entities().min(64) {
                if let Ok(full) = pf.list_at(i) {
                    let _ = pf.lazy_walk_matches(i, &full, u64::from(bound));
                }
                for max in [None, Some(bound)] {
                    if let Ok(Some(it)) = pf.list_le(pf.entity_at(i), max) {
                        for _ in it.take(256) {}
                    }
                }
            }
        }
    }
}

#[test]
fn damaged_elias_fano_data_is_refused_not_a_crash() {
    // M-6 (R-P5P9-25): sucds' `deserialize_from` trusts stored lengths, so a damaged keys.ef
    // or Elias-Fano posting list panicked inside sucds or aborted the process on a huge
    // `Vec::with_capacity`. `kdx_segment::ef` now checks the bytes first. The fixtures are the
    // fuzz findings (each crashed before the fix). Run in a child process, so an abort fails
    // this test instead of taking the harness with it.
    const CHILD: &str = "KDX_SUCDS_CHILD";
    if std::env::var(CHILD).is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("keys.ef");
        write_keys(&p, 8, &[3, 0, 5, 2]).unwrap();
        let good = std::fs::read(&p).unwrap();
        for at in 17..good.len().saturating_sub(8) {
            let mut b = good.clone();
            b[at..at + 8].copy_from_slice(&(1u64 << 60).to_le_bytes());
            std::fs::write(&p, &b).unwrap();
            assert!(Keys::open(&p).is_err(), "length 2^60 at byte {at} opened");
        }
        // The same class in a posting list: encoding byte 1 (Elias-Fano) over garbage.
        for b in 0..=255u8 {
            let r = decode_list(&[1, b, b, b, b, b, b, b, b, b, 1, 2, 3, 4, 5, 6, 7, 8]);
            assert!(r.is_err());
        }
        let fixtures =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sucds");
        let mut seen = 0;
        for e in std::fs::read_dir(&fixtures).unwrap() {
            let e = e.unwrap();
            let name = e.file_name().to_string_lossy().into_owned();
            sucds_fuzz_paths(&name, &std::fs::read(e.path()).unwrap(), dir.path());
            if name.contains("ef_keys") {
                std::fs::write(&p, std::fs::read(e.path()).unwrap()).unwrap();
                assert!(Keys::open(&p).is_err(), "{name} opened");
            }
            seen += 1;
        }
        assert_eq!(seen, 8, "fixtures in {}", fixtures.display());
        return;
    }
    let exe = std::env::current_exe().unwrap();
    let st = std::process::Command::new(exe)
        .args(["--exact", "damaged_elias_fano_data_is_refused_not_a_crash"])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        st.status.success(),
        "child died: {:?}\n{}",
        st.status,
        String::from_utf8_lossy(&st.stderr)
    );
}
