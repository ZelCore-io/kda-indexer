//! Ingest checks 1-2 of spec §2.2 plus parent linkage. Merkle roots are P5.

use kdx_types::{blake2b_256, Hash32};

use crate::payload::{DecodedBlock, Header};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerifyError {
    #[error("payload hash {got} != header payload hash {expected}")]
    PayloadHash { expected: Hash32, got: Hash32 },
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
}

impl VerifyReport {
    pub fn is_clean(&self) -> bool {
        self.cmd_hash_mismatch.is_empty()
    }
}

/// Spec §2.2 checks 1-2. Hard errors: `payloadHash` differs, or a user tx's
/// `hash != output.reqKey`. `blake2b-256(cmd bytes) != hash` is reported in
/// `VerifyReport` instead of failing (see `cmd_hash_mismatch`); `VerifyError::TxHash`
/// is kept for callers that want the strict check (`verify_block_strict`).
pub fn verify_block(
    b: &DecodedBlock,
    expected_payload_hash: &Hash32,
) -> Result<VerifyReport, VerifyError> {
    if b.payload_hash != *expected_payload_hash {
        return Err(VerifyError::PayloadHash {
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
        let rk = t.output.get("reqKey").and_then(|v| v.as_str());
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
