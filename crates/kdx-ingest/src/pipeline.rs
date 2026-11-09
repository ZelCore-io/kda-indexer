//! Decode/verify (parallel) and index (sequential, in (height, chain) order).

use anyhow::{bail, Context, Result};
use kdx_decode::{
    decode_block, index_block, verify_block, DecodedBlock, IndexedBlock, VerifyReport,
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
/// recorded in the report, not refused.
pub fn prepare(chain: u8, height: u32, raw: &[u8]) -> Result<(DecodedBlock, VerifyReport)> {
    let (hdr, payload): (Value, Value) = serde_json::from_slice(raw)
        .with_context(|| format!("chain {chain} height {height}: spooled JSON"))?;
    let b = decode_block(&hdr, &payload)
        .map_err(|e| anyhow::anyhow!("chain {chain} height {height}: decode: {e}"))?;
    if b.header.chain_id != chain || b.header.height != u64::from(height) {
        bail!(
            "chain {chain} height {height}: header says chain {} height {}",
            b.header.chain_id,
            b.header.height
        );
    }
    let report = verify_block(&b, &b.header.payload_hash)
        .map_err(|e| anyhow::anyhow!("chain {chain} height {height}: verify: {e}"))?;
    Ok((b, report))
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
