//! Fork detection for the tail (spec §4.5). A follower keeps the hashes of its chain's last
//! `F + 1` stored heights. New headers come from `header/branch` with `lower` = the stored tip
//! (then sampled older hashes if the node does not know the tip), `upper` = the new hash: the
//! answer is exactly the new branch above the fork point. Its first header's parent must be a
//! stored hash; the fork depth is `tip − fork`. Deeper than `F`, or below the frozen frontier,
//! halts that chain only.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use kdx_hot::tables::{block_key, parse_block_key};
use kdx_hot::{checkpoint, Stage, Table, Txn};
use kdx_types::Hash32;
use serde_json::Value;

use crate::apply::Fork;
use crate::ctx::Ctx;

/// A follower's view of its chain: stored tip and recent hashes.
#[derive(Debug, Clone)]
pub struct ChainView {
    pub chain: u8,
    pub tip: (u32, Hash32),
    recent: BTreeMap<u32, Hash32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReorgOutcome {
    /// The new headers extend the stored tip.
    Extend,
    /// The stored chain forks below the tip; unwind to `fork.height` then apply.
    Reorg(Fork),
    /// Fork deeper than `F` or below the frozen frontier: the chain stops.
    ChainHalted { fork_height: u32, depth: u32 },
}

/// The new branch above the fork point (ascending) and what it means for the stored chain.
#[derive(Debug)]
pub struct Branch {
    pub outcome: ReorgOutcome,
    pub headers: Vec<Value>,
}

pub fn header_height(h: &Value) -> Result<u32> {
    let v = h
        .get("height")
        .and_then(Value::as_u64)
        .context("header height")?;
    Ok(u32::try_from(v)?)
}

pub fn header_hash(h: &Value, field: &str) -> Result<Hash32> {
    Hash32::from_b64url(
        h.get(field)
            .and_then(Value::as_str)
            .context("header hash")?,
    )
    .map_err(|e| anyhow::anyhow!("header {field}: {e}"))
}

impl ChainView {
    /// From the `Tail` checkpoint (else the `Headers` checkpoint at `frozen_frontier − 1`) and
    /// the chain's hot `Headers` rows. `None` when the chain has no checkpoint at all.
    pub fn load(txn: &impl Txn, chain: u8, keep: u32) -> Result<Option<ChainView>> {
        let base = checkpoint(txn, Stage::Headers, chain)?;
        let tip = match checkpoint(txn, Stage::Tail, chain)? {
            Some(t) => t,
            None => match base {
                Some(b) => b,
                None => return Ok(None),
            },
        };
        let mut recent = BTreeMap::new();
        if let Some((h, hash)) = base {
            recent.insert(h, hash);
        }
        let lo = tip.0.saturating_sub(keep);
        for (k, v) in txn.scan(Table::Headers, &block_key(chain, lo), |k| {
            k.first() == Some(&chain)
        })? {
            let (_, h) = parse_block_key(&k).context("Headers key")?;
            if h > tip.0 {
                break;
            }
            let b = kdx_types::record::decode_block(&v).context("hot header")?;
            recent.insert(h, b.hash);
        }
        if recent.get(&tip.0) != Some(&tip.1) {
            bail!("chain {chain}: tip {} not among the stored headers", tip.0);
        }
        let mut v = ChainView { chain, tip, recent };
        v.trim(keep);
        Ok(Some(v))
    }

    pub fn hash_at(&self, h: u32) -> Option<Hash32> {
        self.recent.get(&h).copied()
    }

    /// Tip, then hashes at tip−1, −2, −4, … (the `lower` set when the tip is unknown).
    pub fn lower_samples(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut d = 0u32;
        while let Some(h) = self.tip.0.checked_sub(d) {
            match self.recent.get(&h) {
                Some(x) => out.push(x.to_b64url()),
                None => break,
            }
            d = if d == 0 { 1 } else { d * 2 };
        }
        out
    }

    /// Record applied blocks `(height, hash)` (ascending) after an optional unwind.
    pub fn advance(&mut self, fork: Option<u32>, blocks: &[(u32, Hash32)], keep: u32) {
        if let Some(f) = fork {
            self.recent.retain(|&h, _| h <= f);
            if let Some(x) = self.recent.get(&f) {
                self.tip = (f, *x);
            }
        }
        for &(h, x) in blocks {
            self.recent.insert(h, x);
            self.tip = (h, x);
        }
        self.trim(keep);
    }

    fn trim(&mut self, keep: u32) {
        let lo = self.tip.0.saturating_sub(keep);
        self.recent = self.recent.split_off(&lo);
    }

    /// Classify a branch whose first header is at `h0` with parent `p0`.
    pub fn classify(&self, h0: u32, p0: &Hash32, frontier: u32, f: u32) -> Result<ReorgOutcome> {
        let fork = h0.checked_sub(1).context("branch starts at genesis")?;
        if fork == self.tip.0 && *p0 == self.tip.1 {
            return Ok(ReorgOutcome::Extend);
        }
        if fork > self.tip.0 {
            bail!(
                "chain {}: branch starts at {h0}, above the stored tip {}",
                self.chain,
                self.tip.0
            );
        }
        let depth = self.tip.0 - fork;
        if depth > f || fork + 1 < frontier {
            return Ok(ReorgOutcome::ChainHalted {
                fork_height: fork,
                depth,
            });
        }
        match self.hash_at(fork) {
            Some(x) if x == *p0 => Ok(ReorgOutcome::Reorg(Fork {
                height: fork,
                depth,
            })),
            Some(_) => bail!(
                "chain {}: branch parent at {fork} is not the stored block there",
                self.chain
            ),
            None => Ok(ReorgOutcome::ChainHalted {
                fork_height: fork,
                depth,
            }),
        }
    }
}

/// Fetch the branch ending in `upper` above the stored chain (at most `max` height) and
/// classify it against `view`. An empty branch means `upper` is already stored.
pub async fn handle(ctx: &Ctx, view: &ChainView, upper: &str, max: Option<u64>) -> Result<Branch> {
    let chain = view.chain;
    let lower = [view.tip.1.to_b64url()];
    let headers = match ctx
        .node
        .header_branch_since(chain, &lower, upper, max)
        .await
    {
        Ok(h) => h,
        Err(e) => {
            tracing::debug!(chain, error = %e, "branch with the tip as lower failed; sampled lower");
            ctx.node
                .header_branch_since(chain, &view.lower_samples(), upper, max)
                .await?
        }
    };
    let Some(first) = headers.first() else {
        return Ok(Branch {
            outcome: ReorgOutcome::Extend,
            headers,
        });
    };
    let h0 = header_height(first)?;
    let p0 = header_hash(first, "parent")?;
    let frontier = ctx.hot.ro()?.frozen_frontier()?;
    let f = u32::try_from(ctx.opts.finality_depth)?;
    // Linkage inside the branch.
    for w in headers.windows(2) {
        if header_hash(&w[1], "parent")? != header_hash(&w[0], "hash")?
            || header_height(&w[1])? != header_height(&w[0])? + 1
        {
            bail!("chain {chain}: header/branch answer is not a linked chain");
        }
    }
    let outcome = view.classify(h0, &p0, frontier, f)?;
    Ok(Branch { outcome, headers })
}
