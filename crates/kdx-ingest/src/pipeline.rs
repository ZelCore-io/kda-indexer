//! Decode/verify (parallel) and index (sequential, in (height, chain) order).

use anyhow::{bail, Context, Result};
use kdx_decode::{
    decode_block, index_block, merkle_check, merkle_check_roots, verify_block, DecodedBlock,
    IndexedBlock, MerkleResult, VerifyReport,
};
use kdx_types::DictMut;
use rayon::prelude::*;
use serde_json::Value;

/// Spool key of a block: height (BE u32) then chain, so key order is (height, chain) order.
pub fn spool_key(height: u32, chain: u8) -> [u8; 5] {
    let mut k = [0u8; 5];
    k[..4].copy_from_slice(&height.to_be_bytes());
    k[4] = chain;
    k
}

pub fn parse_spool_key(k: &[u8]) -> Result<(u32, u8)> {
    if k.len() != 5 {
        bail!("bad spool key length {}", k.len());
    }
    Ok((u32::from_be_bytes(k[..4].try_into().unwrap()), k[4]))
}

/// Decode and verify one spooled block (`[header, payload]` JSON). Hard errors carry the
/// chain and height. `verify_block`, not the strict one: legacy re-encoded `cmd`s are
/// recorded in the report, not refused. The payload Merkle check (B8) runs here, in the
/// decode workers; its outcome is `report.merkle` (a mismatch is not an error: the callers
/// apply ruling R-5, soft in backfill, strict in the tail).
pub fn prepare(chain: u8, height: u32, raw: &[u8]) -> Result<(DecodedBlock, VerifyReport)> {
    let (hdr, payload): (Value, Value) = serde_json::from_slice(raw)
        .with_context(|| format!("chain {chain} height {height}: spooled JSON"))?;
    prepare_value(chain, height, &hdr, &payload)
}

/// [`prepare`] from already parsed `(header, payload with outputs)` JSON (tail).
pub fn prepare_value(
    chain: u8,
    height: u32,
    hdr: &Value,
    payload: &Value,
) -> Result<(DecodedBlock, VerifyReport)> {
    let b = decode_block(hdr, payload)
        .map_err(|e| anyhow::anyhow!("chain {chain} height {height}: decode: {e}"))?;
    if b.header.chain_id != chain || b.header.height != u64::from(height) {
        bail!(
            "chain {chain} height {height}: header says chain {} height {}",
            b.header.chain_id,
            b.header.height
        );
    }
    let mut report = verify_block(&b, &b.header.payload_hash)
        .map_err(|e| anyhow::anyhow!("chain {chain} height {height}: verify: {e}"))?;
    report.merkle = Some(merkle_check(&b, &b.header.payload_hash));
    Ok((b, report))
}

/// The Merkle outcome `prepare` stored (a block that skipped it counts as a mismatch).
pub fn merkle_of(p: &(DecodedBlock, VerifyReport)) -> MerkleResult {
    p.1.merkle.unwrap_or(MerkleResult::Mismatch {
        which: kdx_decode::MerkleWhich::PayloadRoot,
        legacy: false,
    })
}

/// One line of `<data_dir>/merkle-unverified.jsonl` (backfill, ruling R-5).
pub fn merkle_line(b: &DecodedBlock, r: &MerkleResult) -> Option<String> {
    let MerkleResult::Mismatch { which, legacy } = r else {
        return None;
    };
    Some(
        serde_json::json!({
            "chain": b.header.chain_id,
            "height": b.header.height,
            "hash": b.header.hash.to_b64url(),
            "which": which.as_str(),
            "legacy": legacy,
        })
        .to_string(),
    )
}

/// Log text of the three roots, computed vs claimed (tail halts).
pub fn merkle_roots_text(b: &DecodedBlock) -> String {
    let r = merkle_check_roots(b);
    format!(
        "computed tx {} out {} payload {}; claimed tx {} out {} payload {}",
        r.tx, r.out, r.payload, b.transactions_hash, b.outputs_hash, b.header.payload_hash
    )
}

/// Dev check of the Merkle policy (R-5): `KDX_DEBUG_CORRUPT=merkle:<chain>` flips one byte
/// of the minerData leaf of every block of that chain after decoding and re-runs the check
/// (tail: halt after one refetch; backfill: bit false and a `merkle-unverified.jsonl` line).
pub fn debug_corrupt_merkle(p: &mut (DecodedBlock, VerifyReport)) {
    static CHAIN: std::sync::OnceLock<Option<u8>> = std::sync::OnceLock::new();
    let target = CHAIN.get_or_init(|| {
        std::env::var("KDX_DEBUG_CORRUPT")
            .ok()
            .and_then(|v| v.strip_prefix("merkle:")?.parse().ok())
    });
    if *target != Some(p.0.header.chain_id) {
        return;
    }
    if let Some(x) = p.0.miner_bytes.first_mut() {
        *x ^= 0x01;
    }
    p.1.merkle = Some(merkle_check(&p.0, &p.0.header.payload_hash));
}

/// [`prepare`] over a batch on rayon; output in input order.
pub fn prepare_batch(items: &[(u32, u8, Vec<u8>)]) -> Result<Vec<(DecodedBlock, VerifyReport)>> {
    items
        .par_iter()
        .map(|(h, c, raw)| prepare(*c, *h, raw))
        .collect()
}

/// `build_records` + `extract` with the shared dictionary. Callers keep (height, chain) order
/// so interned ids are deterministic (range-commit order).
pub fn index(
    prepared: &(DecodedBlock, VerifyReport),
    dict: &mut impl DictMut,
) -> Result<IndexedBlock> {
    let (b, report) = prepared;
    index_block(b, report, dict).map_err(|e| {
        anyhow::anyhow!(
            "chain {} height {}: build: {e}",
            b.header.chain_id,
            b.header.height
        )
    })
}
