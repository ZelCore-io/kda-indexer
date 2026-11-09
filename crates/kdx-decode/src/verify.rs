//! Consistency checks on a decoded block (spec §2.2 checks 1-2) plus parent linkage.
//!
//! None of this verifies payload *contents*. The payload-hash check only compares the
//! payload's own `payloadHash` claim with the header, and the tx checks compare fields of the
//! same response with each other. Recomputing the Merkle roots from the leaves
//! (leaves -> `transactionsHash`/`outputsHash` -> `payloadHash`) is P5 (B8); records carry
//! `merkle_verified = false` until then.

use kdx_types::{blake2b_256, Hash32};

use crate::payload::{DecodedBlock, Header};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerifyError {
    /// The payload's self-declared `payloadHash` differs from the header's (consistency
    /// only: the payload contents are not hashed).
    #[error("payload claims payloadHash {got}, header has {expected} (consistency check)")]
    PayloadHashClaim { expected: Hash32, got: Hash32 },
    #[error("tx {pos}: blake2b-256(cmd) {computed} != hash {hash}")]
    TxHash {
        pos: usize,
        hash: Hash32,
        computed: Hash32,
    },
    #[error("tx {pos}: output reqKey {req_key:?} != hash {hash}")]
    ReqKey {
        pos: usize,
        hash: Hash32,
        req_key: Option<String>,
    },
    #[error("linkage: {0}")]
    Linkage(String),
}

/// Soft findings of `verify_block` (the block is still accepted).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyReport {
    /// User-tx positions (1..n) whose served `cmd` does not hash to `hash`.
    ///
    /// Measured on mainnet01 (node 3.2): many txs below about height 2.3M
    /// (chain 0: 18/18 at 1.0M, 73/74 at 2.0M, 47/349 at 2.2M, 0 from 2.4M)
    /// are served with a re-encoded `cmd` (Aeson-style numbers such as
    /// `1.0e-5`), so `blake2b-256(cmd) != hash` although `hash == reqKey`.
    /// The original bytes are not available from the node for those txs.
    pub cmd_hash_mismatch: Vec<usize>,
    /// User-tx positions whose output could not be parsed far enough to read `reqKey`
    /// (see `TxFlags::output_unrepresentable`); `hash == reqKey` is unchecked for them.
    pub req_key_unchecked: Vec<usize>,
}

impl VerifyReport {
    pub fn is_clean(&self) -> bool {
        self.cmd_hash_mismatch.is_empty() && self.req_key_unchecked.is_empty()
    }
}

/// Consistency checks (spec §2.2 checks 1-2; NOT a content verification, see the module docs).
/// Hard errors: the payload's claimed `payloadHash` differs from the header's, or a user tx's
/// `hash != output.reqKey`. `blake2b-256(cmd bytes) != hash` is reported in
/// `VerifyReport` instead of failing (see `cmd_hash_mismatch`); `VerifyError::TxHash`
/// is kept for callers that want the strict check (`verify_block_strict`).
pub fn verify_block(
    b: &DecodedBlock,
    expected_payload_hash: &Hash32,
) -> Result<VerifyReport, VerifyError> {
    if b.payload_hash != *expected_payload_hash {
        return Err(VerifyError::PayloadHashClaim {
            expected: *expected_payload_hash,
            got: b.payload_hash,
        });
    }
    let mut report = VerifyReport::default();
    for (i, t) in b.txs.iter().enumerate() {
        let pos = i + 1;
        if blake2b_256(t.cmd.as_bytes()) != t.hash {
            report.cmd_hash_mismatch.push(pos);
        }
        let rk = t.out_req_key.as_deref();
        if rk.is_none() && t.flags.output_unrepresentable {
            report.req_key_unchecked.push(pos);
            continue;
        }
        if rk != Some(t.hash.to_b64url().as_str()) {
            return Err(VerifyError::ReqKey {
                pos,
                hash: t.hash,
                req_key: rk.map(str::to_owned),
            });
        }
    }
    Ok(report)
}

/// `verify_block` with the cmd-hash check as a hard error.
pub fn verify_block_strict(
    b: &DecodedBlock,
    expected_payload_hash: &Hash32,
) -> Result<(), VerifyError> {
    let r = verify_block(b, expected_payload_hash)?;
    match r.cmd_hash_mismatch.first() {
        None => Ok(()),
        Some(&pos) => {
            let t = &b.txs[pos - 1];
            Err(VerifyError::TxHash {
                pos,
                hash: t.hash,
                computed: blake2b_256(t.cmd.as_bytes()),
            })
        }
    }
}

/// `next` is the direct child of `prev` on the same chain.
pub fn verify_linkage(prev: &Header, next: &Header) -> Result<(), VerifyError> {
    if prev.chain_id != next.chain_id {
        return Err(VerifyError::Linkage(format!(
            "chain {} != {}",
            prev.chain_id, next.chain_id
        )));
    }
    if prev.height + 1 != next.height {
        return Err(VerifyError::Linkage(format!(
            "chain {}: height {} does not follow {}",
            next.chain_id, next.height, prev.height
        )));
    }
    if next.parent != prev.hash {
        return Err(VerifyError::Linkage(format!(
            "chain {} height {}: parent {} != previous hash {}",
            next.chain_id, next.height, next.parent, prev.hash
        )));
    }
    Ok(())
}
