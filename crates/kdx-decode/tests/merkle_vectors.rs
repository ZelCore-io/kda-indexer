//! Payload Merkle vectors (Task 14): 20 mainnet blocks across eras, captured from a node as
//! served (`header` in object encoding, `payload` with outputs). The expected roots are the
//! node's own claims (the header's `payloadHash`, the payload's `transactionsHash` and
//! `outputsHash`), so the oracle is the chain, not this crate.
//!
//! For every block: `merkle_check` over the served leaves is `Verified`; the records built
//! from the block, encoded and decoded again, re-render to the same roots (`check_records`);
//! and a flipped leaf (miner data, an output, a record field) is a mismatch at the right root.

use std::io::Read;

use kdx_decode::merkle::{check_records, records_roots};
use kdx_decode::{
    build_records, decode_block, merkle_check, merkle_check_roots, verify_block, MerkleResult,
    MerkleWhich,
};
use kdx_types::record::{decode_block as dec_block, decode_tx, encode_block, encode_tx};
use kdx_types::{Hash32, MemDict};
use serde_json::Value;

/// (chain, height, user txs, payloadHash) in fixture order. Eras: genesis of chain 0 and of
/// the 20-chain graph (852,054), the legacy era (375,0xx, re-encoded genesis-style cmds),
/// 2.8M-3.1M, the busy 4.375M window (279 txs is the largest block captured) and 6.2M.
const VECTORS: [(u8, u64, usize, &str); 20] = [
    (0, 0, 6, "k1H3DsInAPvJ0W_zPxnrpkeSNdPUT0S9U8bqDLG739o"),
    (
        10,
        852_054,
        6,
        "i-MN4AoxsaPds4M_MzwNSUygAkGnPZoCDvahfckowt4",
    ),
    (0, 375_000, 0, "qtZm_YbQeJjEYivtkBh9BGdYEfVdz96Z_Djlby6zO5E"),
    (0, 375_006, 1, "XKKMbma6oeYd25a1umZs9KPvgsYvHj3Gc8aWQqb6JrU"),
    (0, 375_011, 2, "1bOmx7zKfKbDcq4CvacbvKPFI6ULVOCAAAowk_r96o4"),
    (
        0,
        2_875_000,
        0,
        "RF-MZTLLCbAFxZSXyLHOzYB9E1wgtqMBPYfXpWzqEzo",
    ),
    (
        0,
        2_875_004,
        1,
        "bvX03dOGF6Z7inSKeKON_3tZr7nT4sYfRrR9gewEGCY",
    ),
    (
        0,
        2_875_003,
        2,
        "YGwfNldz6PqytMfdqfvTLLTWt9zY61q0fbxgC-ZqN1M",
    ),
    (
        0,
        3_125_001,
        3,
        "_KO1LhalPk6nEduK6Ki_p_xnFANWLw_t6hkArssg_iU",
    ),
    (
        0,
        3_125_000,
        5,
        "rUYT_GQqkdN4AodYq9Q0UJaWqVm24_GV8MBhQgkZD2c",
    ),
    (
        0,
        2_875_001,
        9,
        "aB2THts4KncCb12o2y-dVLQ1mJNXwMmFobe6qj2BPoE",
    ),
    (
        0,
        3_125_003,
        21,
        "yYyFs72rnq9JijhWBMt1MpMKBshtiJPPZV4Jr1eKaNg",
    ),
    (
        0,
        4_375_018,
        4,
        "Kb-tNV6m8sqhJ0cGhAQIJotvx0f3Gp1bMuRDFSKiZV8",
    ),
    (
        0,
        4_375_014,
        12,
        "IzjfIKlpEASRZER9ydGxfXUbndTDmu3rjLwcYk7C850",
    ),
    (
        0,
        4_375_000,
        279,
        "4Q2JyF0X2nrf83sGA7GWC3Qmmad23wkKfM07aLV7krg",
    ),
    (
        0,
        6_200_008,
        1,
        "YJFKBJzDB3M49vhq5NyW93OSxl0E6hT-JlzqftIsDtQ",
    ),
    (
        0,
        6_200_005,
        3,
        "sO7j_0Wr51FvOiUd7l4IrmpICDpS8Wd0eA60PFJ3oh8",
    ),
    (
        0,
        6_200_000,
        14,
        "MjTgZATMuVB45hcB358uo0nODyapPpuCmZyeIyTjYsI",
    ),
    (
        0,
        6_200_004,
        27,
        "IblY5FrBeMVggZ7q916n9zirrCsKMY5q8IofincjQYU",
    ),
    (
        0,
        3_125_007,
        0,
        "Cqvfq9-DQtvX8Xsg30muxly7IdA5DNWUEJPfTaQp-7Y",
    ),
];

fn fixture() -> Vec<Value> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/merkle_vectors.json.zst"
    );
    let f = std::fs::File::open(path).expect("fixture");
    let mut s = Vec::new();
    zstd::Decoder::new(f).unwrap().read_to_end(&mut s).unwrap();
    serde_json::from_slice(&s).unwrap()
}

fn hash(s: &str) -> Hash32 {
    Hash32::from_b64url(s).unwrap()
}

fn mismatch_at(r: MerkleResult) -> Option<MerkleWhich> {
    match r {
        MerkleResult::Verified => None,
        MerkleResult::Mismatch { which, .. } => Some(which),
    }
}

#[test]
fn vectors_verify_from_served_leaves_and_from_records() {
    let blocks = fixture();
    assert_eq!(blocks.len(), VECTORS.len());
    for (v, (chain, height, ntx, ph)) in blocks.iter().zip(VECTORS) {
        let at = format!("chain {chain} height {height}");
        let b = decode_block(&v["header"], &v["payload"]).expect(&at);
        assert_eq!(
            (b.header.chain_id, b.header.height),
            (chain, height),
            "{at}"
        );
        assert_eq!(b.txs.len(), ntx, "{at}");
        let expected = hash(ph);
        assert_eq!(b.header.payload_hash, expected, "{at}");
        // The node's child roots, read from the JSON, not from the decoder.
        let tx_claim = hash(v["payload"]["transactionsHash"].as_str().unwrap());
        let out_claim = hash(v["payload"]["outputsHash"].as_str().unwrap());

        let roots = merkle_check_roots(&b);
        assert_eq!(
            (roots.tx, roots.out, roots.payload),
            (tx_claim, out_claim, expected),
            "{at}"
        );
        assert_eq!(merkle_check(&b, &expected), MerkleResult::Verified, "{at}");

        // Records: build, encode, decode, re-render.
        let mut report = verify_block(&b, &expected).expect(&at);
        report.merkle = Some(merkle_check(&b, &expected));
        let mut dict = MemDict::default();
        let (br, trs) = build_records(&b, &report, &mut dict).expect(&at);
        assert!(br.merkle_verified, "{at}");
        let mut buf = Vec::new();
        encode_block(&br, &mut buf);
        let br2 = dec_block(&buf).unwrap();
        let trs2: Vec<_> = trs
            .iter()
            .map(|t| {
                let mut buf = Vec::new();
                encode_tx(t, &mut buf);
                decode_tx(&buf).unwrap()
            })
            .collect();
        assert_eq!(trs2.len(), ntx + 1, "{at}: coinbase + user txs");
        assert_eq!(
            check_records(&br2, &trs2, &dict).unwrap(),
            MerkleResult::Verified,
            "{at}"
        );
        let rr = records_roots(&br2, &trs2, &dict).unwrap();
        assert_eq!((rr.tx, rr.out, rr.payload), (tx_claim, out_claim, expected));
    }
}

#[test]
fn a_flipped_leaf_fails_at_its_root() {
    for (v, (chain, height, ntx, ph)) in fixture().iter().zip(VECTORS) {
        let at = format!("chain {chain} height {height}");
        let expected = hash(ph);
        let b = decode_block(&v["header"], &v["payload"]).unwrap();

        // minerData is the first leaf of transactionsHash.
        let mut m = b.clone();
        let last = m.miner_bytes.len() - 1;
        m.miner_bytes[last] ^= 1;
        assert_eq!(
            mismatch_at(merkle_check(&m, &expected)),
            Some(MerkleWhich::TxRoot),
            "{at}"
        );
        // The coinbase output is the first leaf of outputsHash.
        let mut c = b.clone();
        c.coinbase_bytes[0] ^= 0x20;
        assert_eq!(
            mismatch_at(merkle_check(&c, &expected)),
            Some(MerkleWhich::OutRoot),
            "{at}"
        );
        if ntx > 0 {
            // The last user tx (odd positions exercise the unbalanced right edge).
            let mut t = b.clone();
            let lt = t.txs.last_mut().unwrap();
            let i = lt.tx_bytes.len() / 2;
            lt.tx_bytes[i] ^= 1;
            assert_eq!(
                mismatch_at(merkle_check(&t, &expected)),
                Some(MerkleWhich::TxRoot),
                "{at}"
            );
            let mut o = b.clone();
            let lo = o.txs.last_mut().unwrap();
            let i = lo.output_bytes.len() / 2;
            lo.output_bytes[i] ^= 1;
            assert_eq!(
                mismatch_at(merkle_check(&o, &expected)),
                Some(MerkleWhich::OutRoot),
                "{at}"
            );
        }
        // Another header claim: the recomputed payload root no longer matches.
        let mut other = expected;
        other.0[0] ^= 1;
        assert_eq!(
            mismatch_at(merkle_check(&b, &other)),
            Some(MerkleWhich::PayloadRoot),
            "{at}"
        );

        // Records: a changed field re-renders a different leaf.
        let mut report = verify_block(&b, &expected).unwrap();
        report.merkle = Some(merkle_check(&b, &expected));
        let mut dict = MemDict::default();
        let (br, mut trs) = build_records(&b, &report, &mut dict).unwrap();
        let last = trs.len() - 1;
        trs[last].gas += 1;
        assert_eq!(
            mismatch_at(check_records(&br, &trs, &dict).unwrap()),
            Some(MerkleWhich::OutRoot),
            "{at}"
        );
    }
}
