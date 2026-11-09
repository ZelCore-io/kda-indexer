//! `unwind --chain C --to H` (spec §6.8): undo the chain's hot blocks above `H` in one txn
//! (run it while the indexer is stopped; the tail re-applies from `H` on the next start).
//! Refused below the frozen frontier: frozen heights are never unwound (`H = frontier − 1`
//! undoes every hot block of the chain).

use std::path::Path;

use kdx_hot::{HotStore, Txn};

use crate::config::Config;

pub fn run(cfg: &Config, data_dir: &Path, chain: u8, to: u32) -> anyhow::Result<()> {
    let hs = HotStore::open(&super::hot::hot_dir(data_dir), super::hot::geometry(cfg))?;
    let mut txn = hs.rw()?;
    let frontier = txn.frozen_frontier()?;
    anyhow::ensure!(
        to + 1 >= frontier,
        "--to {to} is below the frozen frontier {frontier}: frozen heights cannot be unwound"
    );
    let tip = kdx_hot::checkpoint(&txn, kdx_hot::Stage::Tail, chain)?;
    let n = kdx_hot::tail::unwind_to(&mut txn, chain, to)?;
    txn.commit()?;
    tracing::info!(chain, to, frontier, old_tip = ?tip.map(|t| t.0), undone = n, "unwind");
    println!(
        "chain {chain}: {n} blocks unwound to height {to} (tip was {:?})",
        tip.map(|t| t.0)
    );
    Ok(())
}
