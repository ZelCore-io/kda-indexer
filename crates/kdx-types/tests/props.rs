//! Property tests for the kdx-types codecs (plan Task 13, T-1). Every property compares with an
//! independent reference (a formula, a `BTreeMap`, a regex, serde_json's own text).

#[macro_use]
#[path = "common/prop.rs"]
mod prop;
use prop::*;

use std::collections::{BTreeMap, BTreeSet};

use kdx_types::dict::{Dict, DictKind, DictMut, MemDict};
use kdx_types::like::Like;
use kdx_types::record::{
    decode_block, decode_block_with, decode_tx, encode_block, encode_block_with, encode_tx,
    BlockRecord, CbReqKey, EventRec, Sig, TxRecord,
};
use kdx_types::varint::{get_bytes, get_u32, get_u64, put_bytes, put_u64};
use kdx_types::{value, Hash32, TxKey};
use serde_json::Value;

// ---------------------------------------------------------------- varint

/// u64 values with the small and boundary ones uniform `any::<u64>()` never reaches (m2):
/// below and at `u32::MAX`, one past it, every power of two and every all-ones value (each
/// varint length from 1 to 10 bytes, and both sides of every length step).
fn u64_edges() -> impl Strategy<Value = u64> {
    prop_oneof![
        2 => any::<u64>(),
        2 => 0..=u32::MAX as u64,
        1 => 0u64..300,
        1 => Just(u32::MAX as u64),
        1 => Just(u32::MAX as u64 + 1),
        1 => Just(u64::MAX),
        2 => (0u32..64).prop_map(|s| 1u64 << s),
        2 => (1u32..=64).prop_map(|s| u64::MAX >> (64 - s)),
    ]
}

/// M8 of the P9 review: a `get_u32` that always errors survived the uniform property.
#[test]
fn get_u32_accepts_every_u32_and_refuses_one_more() {
    for v in [0u64, 1, 127, 128, 16_383, 16_384, u32::MAX as u64] {
        let mut out = Vec::new();
        put_u64(&mut out, v);
        let mut s = out.as_slice();
        assert_eq!(get_u32(&mut s).unwrap() as u64, v);
        assert!(s.is_empty());
    }
    for v in [u32::MAX as u64 + 1, u64::MAX] {
        let mut out = Vec::new();
        put_u64(&mut out, v);
        assert!(get_u32(&mut out.as_slice()).is_err(), "{v}");
    }
}

props! {
    fn varint_roundtrip_and_length(v in u64_edges()) {
        let mut out = Vec::new();
        put_u64(&mut out, v);
        // Reference length: ceil(bits / 7), at least one byte.
        let bits = 64 - v.leading_zeros() as usize;
        prop_assert_eq!(out.len(), bits.div_ceil(7).max(1));
        let mut s = out.as_slice();
        prop_assert_eq!(get_u64(&mut s).unwrap(), v);
        prop_assert!(s.is_empty());
    }

    fn varint_padded_encoding_is_refused(v in u64_edges(), pad in 1usize..4) {
        // Canonical form only (L5): append zero-continuation padding after the shortest form.
        let mut out = Vec::new();
        put_u64(&mut out, v);
        let last = out.len() - 1;
        out[last] |= 0x80;
        out.extend(std::iter::repeat_n(0x80u8, pad - 1));
        out.push(0);
        let mut s = out.as_slice();
        prop_assert!(get_u64(&mut s).is_err());
    }

    fn varint_garbage_is_canonical_or_error(b in proptest::collection::vec(any::<u8>(), 0..14)) {
        let mut s = b.as_slice();
        if let Ok(v) = get_u64(&mut s) {
            // Whatever decodes re-encodes to exactly the bytes it consumed.
            let used = b.len() - s.len();
            let mut again = Vec::new();
            put_u64(&mut again, v);
            prop_assert_eq!(&again[..], &b[..used]);
        }
    }

    fn varint_u32_bound(v in u64_edges()) {
        let mut out = Vec::new();
        put_u64(&mut out, v);
        let mut s = out.as_slice();
        prop_assert_eq!(get_u32(&mut s).is_ok(), v <= u32::MAX as u64);
    }

    fn bytes_roundtrip(b in proptest::collection::vec(any::<u8>(), 0..300), tail in proptest::collection::vec(any::<u8>(), 0..8)) {
        let mut out = Vec::new();
        put_bytes(&mut out, &b);
        let cut = out.len();
        out.extend_from_slice(&tail);
        let mut s = out.as_slice();
        prop_assert_eq!(get_bytes(&mut s).unwrap(), &b[..]);
        prop_assert_eq!(s, &tail[..]);
        // A strict prefix of the length-prefixed form never decodes.
        for k in 0..cut {
            let mut p = &out[..k];
            prop_assert!(get_bytes(&mut p).is_err());
        }
    }
}

// ---------------------------------------------------------------- TxKey

props! {
    fn txkey_fields_and_order(
        a in (any::<u32>(), any::<u8>(), any::<u16>()),
        b in (any::<u32>(), any::<u8>(), any::<u16>()),
    ) {
        let (ka, kb) = (TxKey::new(a.0, a.1, a.2), TxKey::new(b.0, b.1, b.2));
        prop_assert_eq!((ka.height(), ka.chain(), ka.pos()), a);
        prop_assert_eq!(ka.0 & 0xff, 0);
        prop_assert_eq!(ka.is_coinbase(), a.2 == 0);
        // Reference ordering: the (height, chain, pos) tuple, numerically and as bytes.
        prop_assert_eq!(ka.cmp(&kb), a.cmp(&b));
        prop_assert_eq!(ka.to_be_bytes().cmp(&kb.to_be_bytes()), a.cmp(&b));
        prop_assert_eq!(TxKey::from_be_bytes(ka.to_be_bytes()), ka);
    }
}

// ---------------------------------------------------------------- value codec

const ACCOUNTS: [&str; 4] = ["k:0123abcd", "alice", "coin", "bob"];

fn dict() -> MemDict {
    let mut d = MemDict::new();
    for a in ACCOUNTS {
        d.intern(DictKind::Account, a);
    }
    d
}

fn number() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(|n| n.to_string()),
        any::<u64>().prop_map(|n| n.to_string()),
        "-?(0|[1-9][0-9]{0,30})",
        "-?(0|[1-9][0-9]{0,2})\\.[0-9]{1,6}",
        "-?[0-9]\\.[0-9]{1,5}[eE]-?[0-9]{1,3}",
        Just("1.0".to_string()),
        Just("2.36e-3".to_string()),
    ]
    .prop_map(|t| serde_json::from_str::<Value>(&t).expect("number text"))
}

fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        proptest::sample::select(ACCOUNTS.to_vec()).prop_map(String::from),
        "\\PC{0,12}",
        "[a-z:0-9]{0,8}",
    ]
}

fn json() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        number(),
        text().prop_map(Value::String),
    ];
    leaf.prop_recursive(5, 60, 6, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
            proptest::collection::vec((text(), inner), 0..6)
                .prop_map(|kv| Value::Object(kv.into_iter().collect())),
        ]
    })
}

props! {
    fn value_roundtrip_is_text_exact(v in json()) {
        let d = dict();
        let enc = value::to_vec(&v, &d);
        let back = value::from_slice(&enc, &d).unwrap();
        // Reference: serde_json's own rendering of the original.
        prop_assert_eq!(serde_json::to_string(&back).unwrap(), serde_json::to_string(&v).unwrap());
        // A later state of the dictionary (more accounts) decodes the same bytes identically.
        let mut d2 = d.clone();
        d2.intern(DictKind::Account, "late");
        prop_assert_eq!(value::from_slice(&enc, &d2).unwrap(), back);
    }

    fn value_truncation_never_decodes(v in json(), cut in any::<proptest::sample::Index>()) {
        let d = dict();
        let enc = value::to_vec(&v, &d);
        let k = cut.index(enc.len());
        prop_assert!(value::from_slice(&enc[..k], &d).is_err());
        let mut more = enc.clone();
        more.push(0xc0);
        prop_assert!(value::from_slice(&more, &d).is_err());
    }

    fn value_garbage_never_panics(b in proptest::collection::vec(any::<u8>(), 0..64)) {
        let d = dict();
        let _ = value::from_slice(&b, &d);
        let mut s = b.as_slice();
        let _ = value::decode(&mut s, &d);
    }

    fn value_byte_flip_never_panics(v in json(), at in any::<proptest::sample::Index>(), x in 1u8..=255) {
        let d = dict();
        let mut enc = value::to_vec(&v, &d);
        let i = at.index(enc.len());
        enc[i] ^= x;
        let _ = value::from_slice(&enc, &d);
    }

    fn value_depth_is_bounded(n in 100usize..400) {
        // L3: nesting beyond MAX_DEPTH is an error, never a stack overflow.
        let mut enc = vec![0x91u8; n];
        enc.push(0xc0);
        let r = value::from_slice(&enc, &dict());
        prop_assert_eq!(r.is_ok(), n <= value::MAX_DEPTH);
    }
}

// ---------------------------------------------------------------- dictionary

props! {
    fn dict_ids_are_dense_first_seen(
        ops in proptest::collection::vec((0usize..5, "[a-c]{0,3}"), 0..80)
    ) {
        let mut d = MemDict::new();
        // Reference: one Vec per kind, id = position of first sight.
        let mut reference: Vec<Vec<String>> = vec![Vec::new(); 5];
        for (k, s) in &ops {
            let kind = DictKind::ALL[*k];
            let id = d.intern(kind, s);
            let want = match reference[*k].iter().position(|x| x == s) {
                Some(p) => p,
                None => {
                    reference[*k].push(s.clone());
                    reference[*k].len() - 1
                }
            };
            prop_assert_eq!(id as usize, want);
        }
        for (i, kind) in DictKind::ALL.iter().enumerate() {
            prop_assert_eq!(d.names(*kind), &reference[i][..]);
            for (id, s) in reference[i].iter().enumerate() {
                prop_assert_eq!(d.find(*kind, s), Some(id as u32));
                prop_assert_eq!(d.name(*kind, id as u32), Some(s.as_str()));
            }
            prop_assert_eq!(d.name(*kind, reference[i].len() as u32), None);
        }
        // The serialized form rebuilds the same reverse maps.
        let back: MemDict = serde_json::from_str(&serde_json::to_string(&d).unwrap()).unwrap();
        for (i, kind) in DictKind::ALL.iter().enumerate() {
            prop_assert_eq!(back.names(*kind), &reference[i][..]);
            for (id, s) in reference[i].iter().enumerate() {
                prop_assert_eq!(back.find(*kind, s), Some(id as u32));
            }
        }
    }
}

// ---------------------------------------------------------------- records

fn hash() -> impl Strategy<Value = Hash32> {
    arr::<32>().prop_map(Hash32)
}

fn blob() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..40)
}

fn sig() -> impl Strategy<Value = Sig> {
    prop_oneof![
        arr::<64>().prop_map(Sig::Ed25519),
        "\\PC{0,30}".prop_map(Sig::Other),
        // A canonical hex string: from_wire turns it into Ed25519, a stored Other keeps its tag.
        "[0-9a-f]{128}".prop_map(Sig::Other),
    ]
}

fn cb_req_key() -> impl Strategy<Value = CbReqKey> {
    prop_oneof![
        hash().prop_map(CbReqKey::Plain),
        hash().prop_map(CbReqKey::B64Text),
        hash().prop_map(CbReqKey::B64Quoted),
        "\\PC{0,40}".prop_map(CbReqKey::Other),
    ]
}

fn event() -> impl Strategy<Value = EventRec> {
    (any::<u32>(), any::<u32>(), any::<u32>(), blob()).prop_map(
        |(module, name, module_hash, params)| EventRec {
            module,
            name,
            module_hash,
            params,
        },
    )
}

fn tx_record() -> impl Strategy<Value = TxRecord> {
    let a = (
        any::<u64>(),
        proptest::option::of(hash()),
        proptest::option::of(cb_req_key()),
        proptest::option::of(blob()),
        proptest::collection::vec(sig(), 0..4),
        any::<bool>(),
        blob(),
        any::<u64>(),
    );
    let b = (
        proptest::option::of(hash()),
        proptest::option::of(any::<u64>()),
        proptest::option::of(blob()),
        proptest::option::of(blob()),
        proptest::collection::vec(event(), 0..4),
        proptest::collection::vec(any::<u16>(), 0..6),
        any::<u8>(),
    );
    let c = (
        proptest::option::of(blob()),
        proptest::option::of(blob()),
        proptest::option::of(blob()),
        any::<bool>(),
        any::<bool>(),
    );
    (a, b, c).prop_map(|(a, b, c)| TxRecord {
        key: TxKey(a.0),
        request_key: a.1,
        cb_req_key: a.2,
        cmd: a.3,
        sigs: a.4,
        status_ok: a.5,
        result: a.6,
        gas: a.7,
        logs: b.0,
        pact_tx_id: b.1,
        metadata: b.2,
        continuation: b.3,
        events: b.4,
        transfers: b.5,
        present: b.6,
        extra: c.0,
        raw_tx: c.1,
        raw_output: c.2,
        cmd_unparsed: c.3,
        cmd_reencoded: c.4,
    })
}

fn block_record() -> impl Strategy<Value = BlockRecord> {
    let a = (
        hash(),
        hash(),
        any::<u8>(),
        any::<u64>(),
        arr::<32>(),
        arr::<32>(),
        any::<u64>(),
    );
    let b = (
        any::<u64>(),
        any::<u64>(),
        any::<u64>(),
        hash(),
        hash(),
        hash(),
    );
    let c = (
        proptest::collection::btree_map(any::<u8>(), hash(), 0..6),
        "[a-z0-9_]{0,24}",
        any::<u32>(),
        blob(),
        proptest::option::of(blob()),
        any::<bool>(),
    );
    (a, b, c).prop_map(|(a, b, c)| BlockRecord {
        hash: a.0,
        parent: a.1,
        chain: a.2,
        height: a.3,
        weight: a.4,
        target: a.5,
        nonce: a.6,
        creation_time: b.0,
        epoch_start: b.1,
        feature_flags: b.2,
        payload_hash: b.3,
        transactions_hash: b.4,
        outputs_hash: b.5,
        adjacents: c.0.into_iter().collect(),
        chainweb_version: c.1,
        miner_account: c.2,
        miner_data: c.3,
        raw_miner: c.4,
        merkle_verified: c.5,
    })
}

props! {
    fn tx_record_roundtrip(r in tx_record()) {
        let mut b = Vec::new();
        encode_tx(&r, &mut b);
        let back = decode_tx(&b).unwrap();
        prop_assert_eq!(&back, &r);
        // Canonical: re-encoding gives the same bytes.
        let mut again = Vec::new();
        encode_tx(&back, &mut again);
        prop_assert_eq!(again, b);
    }

    fn tx_record_truncation_and_trailing_are_errors(r in tx_record(), cut in any::<proptest::sample::Index>()) {
        let mut b = Vec::new();
        encode_tx(&r, &mut b);
        // The parse is deterministic, so every strict prefix hits EOF.
        prop_assert!(decode_tx(&b[..cut.index(b.len())]).is_err());
        b.push(0);
        prop_assert!(decode_tx(&b).is_err());
    }

    fn tx_record_corruption_never_panics(r in tx_record(), at in any::<proptest::sample::Index>(), x in 1u8..=255) {
        let mut b = Vec::new();
        encode_tx(&r, &mut b);
        let i = at.index(b.len());
        b[i] ^= x;
        if let Ok(back) = decode_tx(&b) {
            // Whatever decodes is canonical: it re-encodes to the same bytes.
            let mut again = Vec::new();
            encode_tx(&back, &mut again);
            prop_assert_eq!(again, b);
        }
    }

    fn tx_record_garbage_never_panics(b in proptest::collection::vec(any::<u8>(), 0..120)) {
        let _ = decode_tx(&b);
        let _ = decode_block(&b);
        let mut with_version = vec![1u8];
        with_version.extend_from_slice(&b);
        let _ = decode_tx(&with_version);
        let _ = decode_block(&with_version);
    }

    fn block_record_roundtrip(r in block_record()) {
        let mut b = Vec::new();
        encode_block(&r, &mut b);
        let back = decode_block(&b).unwrap();
        prop_assert_eq!(&back, &r);
        prop_assert!(decode_block(&b[..b.len() - 1]).is_err() || b.is_empty());
        b.push(0);
        prop_assert!(decode_block(&b).is_err());
    }

    fn block_record_truncation_is_an_error(r in block_record(), cut in any::<proptest::sample::Index>()) {
        let mut b = Vec::new();
        encode_block(&r, &mut b);
        prop_assert!(decode_block(&b[..cut.index(b.len())]).is_err());
    }

    fn block_record_adjacent_refs_roundtrip(r in block_record()) {
        // Reference: a hash is "stored in the previous height" when its first byte is even.
        let is_ref = |_c: u8, h: &Hash32| h.0[0].is_multiple_of(2);
        let by_chain: BTreeMap<u8, Hash32> = r.adjacents.iter().cloned().collect();
        let mut b = Vec::new();
        encode_block_with(&r, &mut b, &is_ref);
        let back = decode_block_with(&b, &|c, h| {
            assert_eq!(h, r.height - 1, "refs resolve at height - 1");
            by_chain.get(&c).copied()
        });
        if r.height == 0 && r.adjacents.iter().any(|(c, h)| is_ref(*c, h)) {
            // Height 0 never writes a ref, so this still decodes with an empty resolver.
        }
        prop_assert_eq!(back.unwrap(), r.clone());
        // Plain decode without a resolver: only a record with no refs decodes.
        let any_ref = r.height > 0 && r.adjacents.iter().any(|(c, h)| is_ref(*c, h));
        prop_assert_eq!(decode_block(&b).is_ok(), !any_ref);
    }

    fn cb_req_key_wire_roundtrip(k in cb_req_key(), s in "\\PC{0,60}") {
        // A typed key survives to_wire/from_wire for the three hash forms.
        match &k {
            CbReqKey::Other(_) => {}
            _ => prop_assert_eq!(CbReqKey::from_wire(&k.to_wire()), k.clone()),
        }
        // Any string keeps its exact wire form.
        prop_assert_eq!(CbReqKey::from_wire(&s).to_wire(), s.clone());
    }

    fn sig_wire_roundtrip(s in "\\PC{0,140}", raw in arr::<64>()) {
        prop_assert_eq!(Sig::from_wire(&s).to_wire(), s);
        let hex_s = hex::encode(raw);
        prop_assert_eq!(Sig::from_wire(&hex_s), Sig::Ed25519(raw));
        // Uppercase hex is not canonical: kept verbatim.
        let upper = hex_s.to_uppercase();
        if upper != hex_s {
            prop_assert_eq!(Sig::from_wire(&upper), Sig::Other(upper.clone()));
        }
    }
}

// ---------------------------------------------------------------- LIKE

/// Reference translation: Postgres `LIKE '%' || needle || '%'` as a Rust regex.
fn like_regex(needle: &str) -> regex::Regex {
    let mut re = String::from("(?s)^.*");
    let mut it = needle.chars();
    while let Some(c) = it.next() {
        match c {
            '%' => re.push_str(".*"),
            '_' => re.push('.'),
            '\\' => {
                let lit = it.next().unwrap_or('\\');
                re.push_str(&regex::escape(&lit.to_string()));
            }
            c => re.push_str(&regex::escape(&c.to_string())),
        }
    }
    re.push_str(".*$");
    regex::Regex::new(&re).unwrap()
}

fn like_alphabet() -> impl Strategy<Value = String> {
    proptest::collection::vec(
        prop_oneof![
            4 => Just('a'),
            4 => Just('b'),
            2 => Just('%'),
            2 => Just('_'),
            1 => Just('\\'),
            1 => Just('\n'),
            1 => Just('é'),
            1 => Just('.'),
            1 => Just('*'),
        ],
        0..10,
    )
    .prop_map(|v| v.into_iter().collect())
}

fn trigrams(s: &str) -> BTreeSet<Vec<char>> {
    let c: Vec<char> = s.chars().collect();
    c.windows(3).map(|w| w.to_vec()).collect()
}

props! {
    fn like_equals_regex_reference(needle in like_alphabet(), hay in "[ab%_\\\\\né.*]{0,14}") {
        let like = Like::new(&needle);
        prop_assert_eq!(like.matches(&hay), like_regex(&needle).is_match(&hay), "needle {:?} hay {:?}", needle, hay);
    }

    fn like_equals_regex_reference_on_near_misses(
        needle in like_alphabet(),
        fill in proptest::collection::vec("[ab_é]{0,2}", 12),
        ch in proptest::collection::vec(prop_oneof![Just('a'), Just('b'), Just('é')], 12),
        edit in (any::<proptest::sample::Index>(), 0u8..3, prop_oneof![Just('a'), Just('b'), Just('_'), Just('é')]),
    ) {
        // m1: a haystack instantiated from the needle (a match), then one char inserted,
        // deleted or replaced, so both sides are often near the match boundary (a `_` that
        // matches zero or two chars, an escape that matches the wrong char).
        let mut hay: Vec<char> = Vec::new();
        let (mut f, mut c) = (fill.iter(), ch.iter());
        let mut it = needle.chars();
        while let Some(x) = it.next() {
            match x {
                '%' => hay.extend(f.next().map(String::as_str).unwrap_or("").chars()),
                '_' => hay.push(*c.next().unwrap_or(&'a')),
                '\\' => hay.push(it.next().unwrap_or('\\')),
                x => hay.push(x),
            }
        }
        let (at, op, x) = edit;
        let i = at.index(hay.len() + 1);
        match op {
            0 => hay.insert(i, x),
            1 if i < hay.len() => { hay.remove(i); }
            _ if i < hay.len() => hay[i] = x,
            _ => hay.push(x),
        }
        let hay: String = hay.into_iter().collect();
        let like = Like::new(&needle);
        prop_assert_eq!(like.matches(&hay), like_regex(&needle).is_match(&hay), "needle {:?} hay {:?}", needle, hay);
    }

    fn like_prefilter_is_a_superset(needle in like_alphabet(), fill in proptest::collection::vec("[ab%_é]{0,3}", 12), ch in proptest::collection::vec(prop_oneof![Just('a'), Just('é'), Just('Z')], 12)) {
        // Build a haystack that matches by instantiating the pattern (so matches are common).
        let mut hay = String::new();
        let (mut f, mut c) = (fill.iter(), ch.iter());
        let mut it = needle.chars();
        hay.push_str(&fill[0]);
        while let Some(x) = it.next() {
            match x {
                '%' => hay.push_str(f.next().map(String::as_str).unwrap_or("")),
                '_' => hay.push(*c.next().unwrap_or(&'a')),
                '\\' => hay.push(it.next().unwrap_or('\\')),
                x => hay.push(x),
            }
        }
        let like = Like::new(&needle);
        prop_assert!(like.matches(&hay), "instantiated {:?} from {:?}", hay, needle);
        prop_assert!(like_regex(&needle).is_match(&hay));
        let hay_grams = trigrams(&hay);
        for run in like.literal_runs() {
            prop_assert!(hay.contains(&run), "run {:?} missing from {:?}", run, hay);
            prop_assert!(trigrams(&run).is_subset(&hay_grams));
        }
    }

    fn like_prefilter_runs_are_literal_substrings(needle in like_alphabet(), hay in "[ab%_é]{0,16}") {
        // Whenever a haystack matches (random ones too), every run is inside it.
        let like = Like::new(&needle);
        if like.matches(&hay) {
            for run in like.literal_runs() {
                prop_assert!(hay.contains(&run));
            }
        }
    }
}

/// m1: fixed near misses of `_` and of an escaped `_`.
#[test]
fn like_underscore_near_misses() {
    for (needle, hay, want) in [
        ("_", "", false),
        ("_", "x", true),
        ("a_b", "ab", false),
        ("a_b", "axb", true),
        ("a_b", "axxb", false),
        ("a__b", "ab", false),
        ("a__b", "axb", false),
        ("a__b", "axyb", true),
        ("\\_", "_", true),
        ("\\_", "x", false),
        ("a\\_b", "axb", false),
        ("a\\_b", "a_b", true),
    ] {
        assert_eq!(
            Like::new(needle).matches(hay),
            want,
            "{needle:?} vs {hay:?}"
        );
        assert_eq!(
            like_regex(needle).is_match(hay),
            want,
            "reference {needle:?} vs {hay:?}"
        );
    }
}

#[test]
fn like_cost_is_bounded_by_pattern_times_text() {
    // R5: the worst case for backtracking matchers, `%a%a%a..%b` over `aaaa..`, is O(n*m).
    let t = std::time::Instant::now();
    let pattern: String = "%a".repeat(300) + "%b";
    let hay = "a".repeat(3000);
    assert!(!Like::new(&pattern).matches(&hay));
    let pattern2: String = "_%".repeat(300) + "b";
    assert!(!Like::new(&pattern2).matches(&hay));
    assert!(
        t.elapsed() < std::time::Duration::from_secs(10),
        "LIKE took {:?}",
        t.elapsed()
    );
}
