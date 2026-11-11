//! The tail's single writer (spec §4.3): batches from the chain followers are committed in
//! one MDBX txn per `commit_ms` / `commit_blocks`: intern (block-apply order) → `tail::apply`
//! (rows, undo, `Tail` checkpoint) → dictionary `persist`, all in the same txn. A reorg batch
//! carries its fork point: the unwind and the new branch always land in one txn.

use std::time::Instant;

use anyhow::{Context, Result};
use kdx_decode::{DecodedBlock, VerifyReport};
use kdx_hot::{HotDict, RwTxn, Txn};

use crate::ctx::Ctx;
use crate::pipeline;

/// Counter names written by the writer (besides the per-block ones of `kdx_hot::tail`).
pub mod counters {
    pub const REORGS: &str = "reorgs";
    /// Blocks undone by reorgs.
    pub const REORG_BLOCKS: &str = "reorg_blocks";
    pub const REORG_DEPTH_MAX: &str = "reorg_depth_max";
    pub fn reorgs_chain(chain: u8) -> String {
        format!("reorgs.c{chain:02}")
    }
    pub fn halted(chain: u8) -> String {
        format!("halted.{chain}")
    }
    /// Why a chain halted (set next to `halted.<chain>`; `unwind --clear-halt` clears both).
    pub const HALT_REASONS: [&str; 3] = ["reorg", "poison", "merkle"];
    pub fn halted_reason(chain: u8, reason: &str) -> String {
        format!("halted.{chain}.{reason}")
    }
}

/// Where a reorg batch forks: unwind `chain` to `height` (kept), then apply the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fork {
    pub height: u32,
    /// Old tip height − fork height.
    pub depth: u32,
}

/// One follower message.
pub struct Batch {
    pub chain: u8,
    pub fork: Option<Fork>,
    /// Consecutive blocks of `chain`, ascending, the first one's parent is the fork point (or
    /// the stored tip).
    pub blocks: Vec<(DecodedBlock, VerifyReport)>,
    /// The follower stopped this chain; counted, nothing applied.
    pub halted: bool,
    /// One of `counters::HALT_REASONS` when `halted`: `reorg` (deeper than allowed), `poison`
    /// (block cannot be indexed) or `merkle` (payload Merkle mismatch after one refetch, R-5).
    pub halt_reason: &'static str,
}

/// A block that cannot be indexed (decode/build failure). The writer halts only its chain
/// (review M6); every other commit error stops the tail.
#[derive(Debug)]
pub struct PoisonBlock {
    pub chain: u8,
    pub height: u64,
    pub msg: String,
}

impl std::fmt::Display for PoisonBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "chain {} height {}: {}",
            self.chain, self.height, self.msg
        )
    }
}

impl std::error::Error for PoisonBlock {}

#[derive(Debug, Default, Clone, Copy)]
pub struct CommitStats {
    pub blocks: u64,
    pub txs: u64,
    pub reorgs: u32,
    pub undone: u32,
    pub secs: f64,
}

fn bump_max(txn: &mut RwTxn<'_>, name: &str, v: i64) -> Result<()> {
    let cur = i64::try_from(txn.counter(name)?).unwrap_or(i64::MAX);
    if v > cur {
        txn.add_counter(name, v - cur)?;
    }
    Ok(())
}

/// Dev check of the poison-block path: `KDX_DEBUG_POISON=chain:height` fails that block.
fn debug_poison(chain: u8, height: u64) -> bool {
    std::env::var("KDX_DEBUG_POISON").is_ok_and(|v| v == format!("{chain}:{height}"))
}

/// Apply `batches` in order in one txn and commit it. On any error the in-memory dictionary
/// is reloaded from MDBX, so names whose txn did not commit are forgotten.
pub fn commit(ctx: &Ctx, batches: &[Batch]) -> Result<CommitStats> {
    // Lock order everywhere: dictionary, then the MDBX writer.
    let mut dict = ctx.dict.lock().unwrap_or_else(|e| e.into_inner());
    let r = commit_locked(ctx, batches, &mut dict);
    if r.is_err() {
        ctx.reload_dict(&mut dict);
    }
    r
}

fn commit_locked(ctx: &Ctx, batches: &[Batch], dict: &mut HotDict) -> Result<CommitStats> {
    let t0 = Instant::now();
    let mut st = CommitStats::default();
    let mut forks: Vec<(u8, u32)> = Vec::new();
    let mut txn = ctx.hot.rw()?;
    for b in batches {
        if b.halted {
            txn.add_counter(&counters::halted(b.chain), 1)?;
            if !b.halt_reason.is_empty() {
                txn.add_counter(&counters::halted_reason(b.chain, b.halt_reason), 1)?;
            }
            continue;
        }
        if let Some(f) = b.fork {
            let frontier = txn.frozen_frontier()?;
            anyhow::ensure!(
                f.height + 1 >= frontier,
                "chain {}: fork at {} is below the frozen frontier {frontier}",
                b.chain,
                f.height
            );
            let n = kdx_hot::tail::unwind_to(&mut txn, b.chain, f.height)?;
            txn.add_counter(counters::REORGS, 1)?;
            txn.add_counter(&counters::reorgs_chain(b.chain), 1)?;
            txn.add_counter(counters::REORG_BLOCKS, i64::from(n))?;
            bump_max(&mut txn, counters::REORG_DEPTH_MAX, i64::from(f.depth))?;
            st.reorgs += 1;
            st.undone += n;
            forks.push((b.chain, f.depth));
        }
        for p in &b.blocks {
            if debug_poison(b.chain, p.0.header.height) {
                return Err(PoisonBlock {
                    chain: b.chain,
                    height: p.0.header.height,
                    msg: "KDX_DEBUG_POISON".into(),
                }
                .into());
            }
            let ib = pipeline::index(p, &mut *dict).map_err(|e| PoisonBlock {
                chain: b.chain,
                height: p.0.header.height,
                msg: format!("{e:#}"),
            })?;
            kdx_hot::tail::apply(&mut txn, &ib, dict).with_context(|| {
                format!("apply chain {} height {}", ib.block.chain, ib.block.height)
            })?;
            st.blocks += 1;
            st.txs += ib.txs.len() as u64;
        }
    }
    dict.persist(&mut txn)?;
    let tc = Instant::now();
    txn.commit()?;
    crate::heartbeat::beat_source(crate::heartbeat::Source::Tail);
    st.secs = t0.elapsed().as_secs_f64();
    let m = kdx_metrics::get();
    m.mdbx_commit_seconds.observe(tc.elapsed().as_secs_f64());
    m.stage("tail_commit", st.secs);
    m.ingested("tail", st.blocks, st.txs);
    for (chain, depth) in forks {
        m.reorg(chain, depth);
    }
    Ok(st)
}
