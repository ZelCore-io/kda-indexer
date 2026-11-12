//! Chainweb payload Merkle roots (B8, spec §2.2 check 3), recomputed from the exact leaf
//! bytes the node served. The rules are described in `docs/format.md` ("Payload Merkle
//! verification"); in short:
//!
//! - hash: SHA-512/256 (FIPS 180-4, its own initial values; not a truncated SHA-512);
//! - an input entry is `0x00 ‖ tag (u16 BE) ‖ bytes`, hashed once (a leaf);
//! - an inner node is `0x01 ‖ left ‖ right`, hashed;
//! - an entry that is itself a root (the payload's two child roots) enters the tree as is;
//! - the tree over n entries is built left to right with a stack: push each entry at height
//!   0, merge the top two while their heights are equal; when the entries run out, merge the
//!   rest from the top down. Zero entries hash the empty string; one entry is the root.
//!
//! `transactionsHash` = tree(minerData, tx₁ … txₙ), `outputsHash` = tree(coinbase,
//! out₁ … outₙ), `payloadHash` = tree(transactionsHash, outputsHash). The block header hash
//! is not recomputed (ruling R-6).

use kdx_types::record::{BlockRecord, TxRecord};
use kdx_types::{Dict, Hash32};
use sha2::{Digest, Sha512_256};

use crate::render::{render_miner_bytes, render_output_bytes, render_tx_bytes, RenderError};

/// Chainweb Merkle universe tags of the payload inputs.
pub const TAG_TRANSACTION: u16 = 0x0013;
pub const TAG_TRANSACTION_OUTPUT: u16 = 0x0014;
pub const TAG_MINER_DATA: u16 = 0x0017;
pub const TAG_COINBASE_OUTPUT: u16 = 0x0018;

const LEAF_PREFIX: u8 = 0x00;
const NODE_PREFIX: u8 = 0x01;

fn finish(h: Sha512_256) -> Hash32 {
    let mut a = [0u8; 32];
    a.copy_from_slice(&h.finalize());
    Hash32(a)
}

/// Leaf hash of a tagged input entry.
pub fn leaf(tag: u16, bytes: &[u8]) -> Hash32 {
    let mut h = Sha512_256::new();
    h.update([LEAF_PREFIX]);
    h.update(tag.to_be_bytes());
    h.update(bytes);
    finish(h)
}

/// Inner node hash.
pub fn node(left: &Hash32, right: &Hash32) -> Hash32 {
    let mut h = Sha512_256::new();
    h.update([NODE_PREFIX]);
    h.update(left.0);
    h.update(right.0);
    finish(h)
}

/// Root of the tree over already hashed entries (leaf hashes or child roots), in log order.
pub fn root<I: IntoIterator<Item = Hash32>>(entries: I) -> Hash32 {
    let mut stack: Vec<(u32, Hash32)> = Vec::new();
    for e in entries {
        stack.push((0, e));
        while let [.., (hb, _), (ha, _)] = stack[..] {
            if ha != hb {
                break;
            }
            merge_top(&mut stack);
        }
    }
    if stack.is_empty() {
        return finish(Sha512_256::new());
    }
    while stack.len() > 1 {
        merge_top(&mut stack);
    }
    stack[0].1
}

/// Replace the top two stack entries (b below a) by `node(b, a)` one level above a.
fn merge_top(stack: &mut Vec<(u32, Hash32)>) {
    let (ha, a) = stack.pop().expect("two entries");
    let (_, b) = stack.pop().expect("two entries");
    stack.push((ha + 1, node(&b, &a)));
}

/// `transactionsHash`: minerData, then the tx objects in payload order.
pub fn transactions_root<I, B>(miner_data: &[u8], txs: I) -> Hash32
where
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    root(
        std::iter::once(leaf(TAG_MINER_DATA, miner_data))
            .chain(txs.into_iter().map(|t| leaf(TAG_TRANSACTION, t.as_ref()))),
    )
}

/// `outputsHash`: the coinbase output, then the tx outputs in payload order.
pub fn outputs_root<I, B>(coinbase: &[u8], outputs: I) -> Hash32
where
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    root(
        std::iter::once(leaf(TAG_COINBASE_OUTPUT, coinbase)).chain(
            outputs
                .into_iter()
                .map(|o| leaf(TAG_TRANSACTION_OUTPUT, o.as_ref())),
        ),
    )
}

/// `payloadHash` from the two child roots.
pub fn payload_root(tx_root: &Hash32, out_root: &Hash32) -> Hash32 {
    root([*tx_root, *out_root])
}

/// Which recomputed root disagreed first (checked in this order).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MerkleWhich {
    /// Computed transactions root != the payload's `transactionsHash`.
    TxRoot,
    /// Computed outputs root != the payload's `outputsHash`.
    OutRoot,
    /// `payload_root(computed …)` != the header's `payloadHash`.
    PayloadRoot,
}

impl MerkleWhich {
    pub fn as_str(self) -> &'static str {
        match self {
            MerkleWhich::TxRoot => "tx_root",
            MerkleWhich::OutRoot => "out_root",
            MerkleWhich::PayloadRoot => "payload_root",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MerkleResult {
    Verified,
    /// `legacy`: the block holds a user tx whose served `cmd` does not hash to its request
    /// key (re-encoded by the node, `cmd_reencoded`), so the original leaf bytes are gone.
    Mismatch {
        which: MerkleWhich,
        legacy: bool,
    },
}

impl MerkleResult {
    pub fn is_verified(&self) -> bool {
        matches!(self, MerkleResult::Verified)
    }

    /// Metric/report label: `verified`, `mismatch_legacy` or `mismatch`.
    pub fn label(&self) -> &'static str {
        match self {
            MerkleResult::Verified => "verified",
            MerkleResult::Mismatch { legacy: true, .. } => "mismatch_legacy",
            MerkleResult::Mismatch { legacy: false, .. } => "mismatch",
        }
    }
}

/// The three recomputed roots of one block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MerkleRoots {
    pub tx: Hash32,
    pub out: Hash32,
    pub payload: Hash32,
}

impl MerkleRoots {
    pub fn from_children(tx: Hash32, out: Hash32) -> Self {
        MerkleRoots {
            tx,
            out,
            payload: payload_root(&tx, &out),
        }
    }

    /// Compare with the claimed roots (payload `transactionsHash`, `outputsHash`, header
    /// `payloadHash`); the first difference wins.
    pub fn compare(
        &self,
        tx: &Hash32,
        out: &Hash32,
        payload: &Hash32,
        legacy: impl FnOnce() -> bool,
    ) -> MerkleResult {
        let which = if self.tx != *tx {
            MerkleWhich::TxRoot
        } else if self.out != *out {
            MerkleWhich::OutRoot
        } else if self.payload != *payload {
            MerkleWhich::PayloadRoot
        } else {
            return MerkleResult::Verified;
        };
        MerkleResult::Mismatch {
            which,
            legacy: legacy(),
        }
    }
}

/// Recompute the roots from stored records, leaves re-rendered through `render.rs` (exact
/// bytes): this checks the storage round trip as well as the content. `txs[0]` is the
/// coinbase record, then the user txs in payload order. Compared with the record's
/// `transactions_hash`, `outputs_hash` and `payload_hash` (the header's, checked equal to the
/// payload's claim at ingest).
pub fn check_records(
    block: &BlockRecord,
    txs: &[TxRecord],
    dict: &dyn Dict,
) -> Result<MerkleResult, RenderError> {
    Ok(records_roots(block, txs, dict)?.compare(
        &block.transactions_hash,
        &block.outputs_hash,
        &block.payload_hash,
        || txs.iter().any(|t| t.cmd_reencoded),
    ))
}

/// The roots [`check_records`] compares.
pub fn records_roots(
    block: &BlockRecord,
    txs: &[TxRecord],
    dict: &dyn Dict,
) -> Result<MerkleRoots, RenderError> {
    let (cb, user) = txs
        .split_first()
        .ok_or_else(|| RenderError::Shape("block without a coinbase record".into()))?;
    let miner = render_miner_bytes(block, dict)?;
    let mut tx_leaves = Vec::with_capacity(user.len());
    let mut out_leaves = Vec::with_capacity(user.len());
    for t in user {
        let b = render_tx_bytes(t, dict)?
            .ok_or_else(|| RenderError::Shape("user tx record without a tx object".into()))?;
        tx_leaves.push(b);
        out_leaves.push(render_output_bytes(t, dict)?);
    }
    let coinbase = render_output_bytes(cb, dict)?;
    Ok(MerkleRoots::from_children(
        transactions_root(&miner, &tx_leaves),
        outputs_root(&coinbase, &out_leaves),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash32 {
        Hash32([b; 32])
    }

    #[test]
    fn sha512_256_known_answer() {
        // FIPS 180-4 example: SHA-512/256("abc").
        let mut s = Sha512_256::new();
        s.update(b"abc");
        let d = finish(s);
        assert_eq!(
            hex(&d.0),
            "53048e2681941ef99b2e29b76b4c7dabe4c2d0c634fc6d46e0e2f13107e7af23"
        );
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn tree_shapes() {
        let empty = root([]);
        assert_eq!(empty, finish(Sha512_256::new()));
        assert_eq!(root([h(1)]), h(1));
        assert_eq!(root([h(1), h(2)]), node(&h(1), &h(2)));
        // 3: ((1 2) 3)
        assert_eq!(root([h(1), h(2), h(3)]), node(&node(&h(1), &h(2)), &h(3)));
        // 4: ((1 2) (3 4))
        let n12 = node(&h(1), &h(2));
        let n34 = node(&h(3), &h(4));
        assert_eq!(root([h(1), h(2), h(3), h(4)]), node(&n12, &n34));
        // 5: (((1 2) (3 4)) 5)
        assert_eq!(
            root([h(1), h(2), h(3), h(4), h(5)]),
            node(&node(&n12, &n34), &h(5))
        );
        // 6: (((1 2) (3 4)) (5 6))
        assert_eq!(
            root((1..=6).map(h)),
            node(&node(&n12, &n34), &node(&h(5), &h(6)))
        );
        // 7: (((1 2) (3 4)) ((5 6) 7))
        assert_eq!(
            root((1..=7).map(h)),
            node(&node(&n12, &n34), &node(&node(&h(5), &h(6)), &h(7)))
        );
    }

    #[test]
    fn leaf_and_payload() {
        let mut s = Sha512_256::new();
        s.update([0u8, 0x00, 0x17]);
        s.update(b"m");
        assert_eq!(leaf(TAG_MINER_DATA, b"m"), finish(s));
        assert_eq!(
            transactions_root(b"m", [] as [&[u8]; 0]),
            leaf(TAG_MINER_DATA, b"m")
        );
        let t = transactions_root(b"m", [b"t1".as_slice()]);
        assert_eq!(
            t,
            node(&leaf(TAG_MINER_DATA, b"m"), &leaf(TAG_TRANSACTION, b"t1"))
        );
        let o = outputs_root(b"c", [b"o1".as_slice()]);
        assert_eq!(payload_root(&t, &o), node(&t, &o));
    }
}
