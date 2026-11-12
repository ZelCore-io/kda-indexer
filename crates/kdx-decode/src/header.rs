//! Derived block header fields (GQ-12): the header's binary encoding, its proof-of-work hash,
//! and the 256-bit numbers (`target`, `weight`, difficulty) as integers.
//!
//! The rules are chainweb-node's, written down in `docs/format.md` "Derived header fields":
//! the binary header is the fixed sequence of fields below, all integers little-endian; the
//! proof-of-work hash is BLAKE2s-256 over the header bytes without the trailing block hash;
//! a block is valid work when that hash, read as a little-endian 256-bit number, is at most
//! the target.

use blake2::{Blake2s256, Digest};
use kdx_types::record::BlockRecord;
use kdx_types::Hash32;

/// The chainweb version code that the binary header carries.
pub fn version_code(name: &str) -> Option<u32> {
    match name {
        "mainnet01" => Some(0x0000_0005),
        "testnet04" => Some(0x0000_0007),
        _ => None,
    }
}

/// The binary header of `b` (what the node's `/header` serves base64url-encoded):
/// feature flags u64, creation time u64 (µs), parent hash, the adjacent hashes (u16 count,
/// then chain u32 and hash per adjacent, ascending by chain), target, payload hash, chain
/// u32, weight, height u64, version code u32, epoch start u64 (µs), nonce u64, block hash.
pub fn encode_header_bytes(b: &BlockRecord, version_code: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(318);
    out.extend_from_slice(&b.feature_flags.to_le_bytes());
    out.extend_from_slice(&b.creation_time.to_le_bytes());
    out.extend_from_slice(&b.parent.0);
    let mut adj = b.adjacents.clone();
    adj.sort_by_key(|a| a.0);
    out.extend_from_slice(&(adj.len() as u16).to_le_bytes());
    for (c, h) in &adj {
        out.extend_from_slice(&u32::from(*c).to_le_bytes());
        out.extend_from_slice(&h.0);
    }
    out.extend_from_slice(&b.target);
    out.extend_from_slice(&b.payload_hash.0);
    out.extend_from_slice(&u32::from(b.chain).to_le_bytes());
    out.extend_from_slice(&b.weight);
    out.extend_from_slice(&b.height.to_le_bytes());
    out.extend_from_slice(&version_code.to_le_bytes());
    out.extend_from_slice(&b.epoch_start.to_le_bytes());
    out.extend_from_slice(&b.nonce.to_le_bytes());
    out.extend_from_slice(&b.hash.0);
    out
}

/// The proof-of-work hash: BLAKE2s-256 of the header bytes without the block hash. `None`
/// for a chainweb version this build does not know.
pub fn pow_hash(b: &BlockRecord) -> Option<Hash32> {
    let code = version_code(&b.chainweb_version)?;
    let bytes = encode_header_bytes(b, code);
    let d = Blake2s256::digest(&bytes[..bytes.len() - 32]);
    let mut h = [0u8; 32];
    h.copy_from_slice(&d);
    Some(Hash32(h))
}

/// `powHash` as chainweb-data renders it: lowercase hex of the bytes reversed (the 256-bit
/// number big-endian).
pub fn pow_hash_hex(h: &Hash32) -> String {
    let mut s = String::with_capacity(64);
    for b in h.0.iter().rev() {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Whether `pow`, read as a little-endian 256-bit number, is at most `target`.
pub fn pow_ok(pow: &[u8; 32], target: &[u8; 32]) -> bool {
    for i in (0..32).rev() {
        match pow[i].cmp(&target[i]) {
            std::cmp::Ordering::Less => return true,
            std::cmp::Ordering::Greater => return false,
            std::cmp::Ordering::Equal => {}
        }
    }
    true
}

type U256 = [u64; 4];

fn limbs(le: &[u8; 32]) -> U256 {
    let mut l = [0u64; 4];
    for (i, x) in l.iter_mut().enumerate() {
        let mut w = [0u8; 8];
        w.copy_from_slice(&le[i * 8..i * 8 + 8]);
        *x = u64::from_le_bytes(w);
    }
    l
}

fn is_zero(a: &U256) -> bool {
    a.iter().all(|x| *x == 0)
}

/// Divide in place by a small divisor; returns the remainder.
fn div_small(a: &mut U256, d: u64) -> u64 {
    let mut rem: u128 = 0;
    for x in a.iter_mut().rev() {
        let cur = (rem << 64) | u128::from(*x);
        *x = (cur / u128::from(d)) as u64;
        rem = cur % u128::from(d);
    }
    rem as u64
}

/// A little-endian 256-bit number as decimal text (chainweb-data's `numeric(80,0)` columns
/// for `target` and `weight`).
pub fn le256_decimal(le: &[u8; 32]) -> String {
    let mut a = limbs(le);
    if is_zero(&a) {
        return "0".into();
    }
    const CHUNK: u64 = 10_000_000_000_000_000_000; // 10^19
    let mut parts = Vec::new();
    while !is_zero(&a) {
        parts.push(div_small(&mut a, CHUNK));
    }
    let mut s = parts.pop().map(|p| p.to_string()).unwrap_or_default();
    for p in parts.iter().rev() {
        s.push_str(&format!("{p:019}"));
    }
    s
}

/// A little-endian 256-bit number as `f64` (for sums such as `totalDifficulty`).
pub fn le256_f64(le: &[u8; 32]) -> f64 {
    limbs(le)
        .iter()
        .rev()
        .fold(0.0, |acc, x| acc * 18_446_744_073_709_551_616.0 + *x as f64)
}

fn ge(a: &U256, b: &U256) -> bool {
    for i in (0..4).rev() {
        if a[i] != b[i] {
            return a[i] > b[i];
        }
    }
    true
}

fn sub(a: &mut U256, b: &U256) {
    let mut borrow = false;
    for i in 0..4 {
        let (x, o1) = a[i].overflowing_sub(b[i]);
        let (y, o2) = x.overflowing_sub(u64::from(borrow));
        a[i] = y;
        borrow = o1 || o2;
    }
}

/// Block difficulty: (2^256 − 1) / target, integer division (chainweb-node's
/// `targetToDifficulty`). Saturates at `u128::MAX`; a zero target gives `u128::MAX`.
pub fn difficulty(target: &[u8; 32]) -> u128 {
    let t = limbs(target);
    if is_zero(&t) {
        return u128::MAX;
    }
    // Binary long division of the all-ones numerator.
    let mut rem: U256 = [0; 4];
    let mut q: U256 = [0; 4];
    for bit in (0..256).rev() {
        let carry = rem[3] >> 63;
        for i in (1..4).rev() {
            rem[i] = (rem[i] << 1) | (rem[i - 1] >> 63);
        }
        rem[0] = (rem[0] << 1) | 1;
        if carry == 1 || ge(&rem, &t) {
            sub(&mut rem, &t);
            q[bit / 64] |= 1u64 << (bit % 64);
        }
    }
    if q[2] != 0 || q[3] != 0 {
        return u128::MAX;
    }
    (u128::from(q[1]) << 64) | u128::from(q[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::decode_header;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    /// Chain 0 at height 4,000,000 on mainnet01: the node's object form and its binary form
    /// (public chain data).
    const OBJ: &str = r#"{"nonce":"11126140711860512686","creationTime":1692435833297252,"parent":"Zrksg2k2hV6LVUr_9sz9rNwQ1jAa-noFOeRLRv7Dr8w","adjacents":{"10":"njCvmXuTPQ8MOwdQgUpEyCfcSTrU9Dks6Rs9Man9Foo","5":"O-KOYBi7iyehhKSsdQmKT6Ayo8vusIjSquM6vtGUInQ","15":"xNNSwhRfPRdmLZNZnepYmUCZZ3QKAuqGeUpVf268TRE"},"target":"NbiVhElEaG_kkDseRpo1dutRiExrYqDgGgAAAAAAAAA","payloadHash":"CPlUtVb8MWhLK2gPpcoTipkz-pTuCJzM2TcwhFi7jsA","chainId":0,"weight":"xS0h2c7zXObhiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","height":4000000,"chainwebVersion":"mainnet01","epochStart":1692434617560219,"featureFlags":0,"hash":"1BAHgwrK9bT5DpvIGs32lI7kQV8NeQ1bltgypstNgaQ"}"#;
    const BIN: &str = "AAAAAAAAAABkGabsQgMGAGa5LINpNoVei1VK__bM_azcENYwGvp6BTnkS0b-w6_MAwAFAAAAO-KOYBi7iyehhKSsdQmKT6Ayo8vusIjSquM6vtGUInQKAAAAnjCvmXuTPQ8MOwdQgUpEyCfcSTrU9Dks6Rs9Man9FooPAAAAxNNSwhRfPRdmLZNZnepYmUCZZ3QKAuqGeUpVf268TRE1uJWESURob-SQOx5GmjV261GITGtioOAaAAAAAAAAAAj5VLVW_DFoSytoD6XKE4qZM_qU7giczNk3MIRYu47AAAAAAMUtIdnO81zm4YgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAk9AAAAAAAFAAAAm2wvpEIDBgCuJ3VTB_5nmtQQB4MKyvW0-Q6byBrN9pSO5EFfDXkNW5bYMqbLTYGk";

    pub(crate) fn record(obj: &str) -> BlockRecord {
        let h = decode_header(&serde_json::from_str(obj).unwrap()).unwrap();
        BlockRecord {
            hash: h.hash,
            parent: h.parent,
            adjacents: h
                .adjacents_order
                .iter()
                .map(|c| (*c, h.adjacents[c]))
                .collect(),
            chain: h.chain_id,
            height: h.height,
            weight: h.weight,
            target: h.target,
            nonce: h.nonce,
            creation_time: h.creation_time,
            epoch_start: h.epoch_start,
            feature_flags: h.feature_flags,
            payload_hash: h.payload_hash,
            transactions_hash: Hash32([0; 32]),
            outputs_hash: Hash32([0; 32]),
            chainweb_version: h.chainweb_version.clone(),
            miner_account: 0,
            miner_data: Vec::new(),
            raw_miner: None,
            merkle_verified: false,
        }
    }

    #[test]
    fn binary_header_matches_the_node() {
        let b = record(OBJ);
        let bin = URL_SAFE_NO_PAD.decode(BIN).unwrap();
        assert_eq!(bin.len(), 318);
        assert_eq!(
            encode_header_bytes(&b, version_code("mainnet01").unwrap()),
            bin
        );
    }

    #[test]
    fn pow_hash_is_within_target() {
        let b = record(OBJ);
        let p = pow_hash(&b).unwrap();
        assert!(pow_ok(&p.0, &b.target));
        // The hash of other bytes is not (the check is not vacuous).
        let mut c = b.clone();
        c.nonce ^= 1;
        assert!(!pow_ok(&pow_hash(&c).unwrap().0, &b.target));
        assert_eq!(pow_hash_hex(&p).len(), 64);
        assert!(pow_hash(&BlockRecord {
            chainweb_version: "nope".into(),
            ..b
        })
        .is_none());
    }

    #[test]
    fn numbers() {
        let mut t = [0u8; 32];
        assert_eq!(le256_decimal(&t), "0");
        t[0] = 39;
        assert_eq!(le256_decimal(&t), "39");
        let max = [0xffu8; 32];
        assert_eq!(
            le256_decimal(&max),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
        assert_eq!(difficulty(&max), 1);
        let mut half = [0xffu8; 32];
        half[31] = 0x7f; // 2^255 - 1
        assert_eq!(difficulty(&half), 2);
        // target 2^200: (2^256-1) / 2^200 = 2^56 - 1
        let mut p = [0u8; 32];
        p[25] = 1;
        assert_eq!(difficulty(&p), (1u128 << 56) - 1);
        assert_eq!(difficulty(&t), u128::MAX); // target 39 is below 2^128 → saturates
        assert!((le256_f64(&max) - 1.157920892373162e77).abs() < 1e62);
        let b = record(OBJ);
        // The node's own number for this block's target, read as an integer.
        let d = difficulty(&b.target);
        assert!(
            d > 1_000_000_000_000 && d < 1_000_000_000_000_000_000,
            "{d}"
        );
    }

    /// GQ-12 cross-era check: `KDX_HEADER_SAMPLE` names a JSON-lines file of
    /// `{"obj": <node header object>, "bin": <node binary header, base64url>}` rows (fetched
    /// from the node by the operator). Every row must encode to the node's bytes and its
    /// proof-of-work hash must be within its target.
    #[test]
    #[ignore]
    fn header_sample_from_env() {
        let Ok(path) = std::env::var("KDX_HEADER_SAMPLE") else {
            return;
        };
        let text = std::fs::read_to_string(path).unwrap();
        let (mut n, mut mined) = (0, 0);
        for line in text.lines() {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            let b = record(&v["obj"].to_string());
            let bin = URL_SAFE_NO_PAD.decode(v["bin"].as_str().unwrap()).unwrap();
            let code = version_code(&b.chainweb_version).unwrap();
            assert_eq!(
                encode_header_bytes(&b, code),
                bin,
                "{} {}",
                b.chain,
                b.height
            );
            // Genesis blocks are not mined (chains 10–19 start at 852,054 on mainnet01).
            let genesis = b.height == 0 || (b.chain >= 10 && b.height == 852_054);
            if !genesis {
                assert!(
                    pow_ok(&pow_hash(&b).unwrap().0, &b.target),
                    "{} {}",
                    b.chain,
                    b.height
                );
                mined += 1;
            }
            n += 1;
        }
        eprintln!("header sample: {n} blocks encode to the node's bytes; {mined} mined blocks are within target");
    }
}
