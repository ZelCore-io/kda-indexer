//! Content digest (ruling on 9B-1/9B-2): blake3 over the decoded records in (height, chain,
//! tx index) order with every name resolved through the dictionary, so two stores whose
//! dictionaries gave the same names other ids, or whose files were compressed with another
//! zstd dictionary, digest equal when they hold the same blocks.
//!
//! Per block: height, chain, the rendered header (`render_header`), the `merkle_verified`
//! bit and the rendered miner data. Per tx: its position, request key, the rendered tx
//! object and output bytes (the node's bytes, names resolved), the transfer event indexes
//! and the presence bits. Every field is length-prefixed.

use anyhow::{Context, Result};
use kdx_decode::render::{render_header, render_miner_bytes, render_output_bytes, render_tx_bytes};
use kdx_types::dict::Dict;
use kdx_types::record::{BlockRecord, TxRecord};

use crate::reader::RangeReader;

/// Block and tx counts plus the blake3 hex of the content.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ContentSummary {
    pub blocks: u64,
    pub txs: u64,
    pub blake3: String,
}

/// Running content digest; feed blocks in (height, chain) order.
pub struct ContentDigest {
    h: blake3::Hasher,
    blocks: u64,
    txs: u64,
}

impl Default for ContentDigest {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentDigest {
    pub fn new() -> ContentDigest {
        ContentDigest {
            h: blake3::Hasher::new(),
            blocks: 0,
            txs: 0,
        }
    }

    fn field(&mut self, b: &[u8]) {
        self.h.update(&(b.len() as u64).to_be_bytes());
        self.h.update(b);
    }

    /// One block and its txs (in tx index order).
    pub fn block(&mut self, b: &BlockRecord, txs: &[TxRecord], dict: &dyn Dict) -> Result<()> {
        let tag = || format!("chain {} height {}", b.chain, b.height);
        self.field(&b.height.to_be_bytes());
        self.field(&[b.chain]);
        self.field(&serde_json::to_vec(&render_header(b))?);
        self.field(&[u8::from(b.merkle_verified)]);
        self.field(&render_miner_bytes(b, dict).with_context(|| format!("{}: miner", tag()))?);
        self.field(&(txs.len() as u64).to_be_bytes());
        for t in txs {
            self.field(&t.key.0.to_be_bytes());
            self.field(t.request_key.as_ref().map(|h| &h.0[..]).unwrap_or(&[]));
            let tx = render_tx_bytes(t, dict).with_context(|| format!("{}: tx", tag()))?;
            self.field(tx.as_deref().unwrap_or(&[]));
            self.field(
                &render_output_bytes(t, dict).with_context(|| format!("{}: output", tag()))?,
            );
            let xs: Vec<u8> = t.transfers.iter().flat_map(|x| x.to_be_bytes()).collect();
            self.field(&xs);
            self.field(&[t.present]);
        }
        self.blocks += 1;
        self.txs += txs.len() as u64;
        Ok(())
    }

    pub fn finish(self) -> ContentSummary {
        ContentSummary {
            blocks: self.blocks,
            txs: self.txs,
            blake3: self.h.finalize().to_hex().to_string(),
        }
    }
}

/// Content digest of one range or step file set.
pub fn range(r: &RangeReader, dict: &dyn Dict) -> Result<ContentSummary> {
    let mut d = ContentDigest::new();
    feed(r, dict, &mut d)?;
    Ok(d.finish())
}

/// Feed one range into `d` (ranges in height order give a digest that does not depend on
/// how the heights are split into ranges and steps).
pub fn feed(r: &RangeReader, dict: &dyn Dict, d: &mut ContentDigest) -> Result<()> {
    for s in 0..r.slots() {
        let Some(b) = r.block_at(s)? else { continue };
        let (a, n) = (r.keys.first(s).context("keys: no first")?, r.keys.count(s));
        let txs = (a..a + n)
            .map(|rel| r.tx_rel(rel))
            .collect::<Result<Vec<_>>>()?;
        d.block(&b, &txs, dict)?;
    }
    Ok(())
}
