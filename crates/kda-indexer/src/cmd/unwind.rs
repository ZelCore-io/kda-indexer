//! `unwind --chain C [--to H] [--clear-halt]` (spec §6.8).
//!
//! `--to H` undoes the chain's hot blocks above `H` in one txn; the tail re-applies from `H`
//! on the next start. Refused below the frozen frontier: frozen heights are never unwound
//! (`H = frontier − 1` undoes every hot block of the chain).
//!
//! `--clear-halt` deletes the chain's `halted.<C>` counter so the tail follows it again. With
//! `--to` both happen in the same txn. It refuses when the chain's hot state is inconsistent
//! (the `Tail` checkpoint does not match the stored header, or rows sit above the tip).
//!
//! Takes the data-dir lock, so it only runs while the indexer is stopped.

use std::path::Path;

use anyhow::{ensure, Context};
use kdx_hot::tables::block_key;
use kdx_hot::{HotStore, Stage, Table, Txn};

use crate::config::Config;

/// The chain's hot rows agree with its `Tail` checkpoint: the checkpointed header is stored
/// with that hash, no header is stored above it, and with no checkpoint no hot header exists.
fn check_consistent(txn: &impl Txn, chain: u8, frontier: u32) -> anyhow::Result<()> {
    let tip = kdx_hot::checkpoint(txn, Stage::Tail, chain)?;
    let from = tip.map_or(frontier, |(h, _)| h + 1);
    let above = txn.scan(Table::Headers, &block_key(chain, from), |k| {
        k.first() == Some(&chain)
    })?;
    ensure!(
        above.is_empty(),
        "chain {chain}: {} hot headers above the Tail checkpoint {:?}; hot state is inconsistent",
        above.len(),
        tip.map(|t| t.0)
    );
    if let Some((h, hash)) = tip {
        if h >= frontier {
            let row = txn
                .get(Table::Headers, &block_key(chain, h))?
                .with_context(|| format!("chain {chain}: Tail checkpoint {h} has no stored header; hot state is inconsistent"))?;
            let br = kdx_types::record::decode_block(&row)?;
            ensure!(
                br.hash == hash,
                "chain {chain}: Tail checkpoint hash at {h} differs from the stored header; hot state is inconsistent"
            );
            ensure!(
                txn.get(Table::Undo, &block_key(chain, h))?.is_some(),
                "chain {chain}: no undo record at the tip {h}; hot state is inconsistent"
            );
        }
    }
    Ok(())
}

pub fn run(
    cfg: &Config,
    data_dir: &Path,
    chain: u8,
    to: Option<u32>,
    clear_halt: bool,
) -> anyhow::Result<()> {
    ensure!(
        to.is_some() || clear_halt,
        "give --to H, --clear-halt, or both"
    );
    let _lock = super::lock_data_dir(data_dir)?;
    let hs = HotStore::open(&super::hot::hot_dir(data_dir), super::hot::geometry(cfg))?;
    let mut txn = hs.rw()?;
    let frontier = txn.frozen_frontier()?;
    let tip = kdx_hot::checkpoint(&txn, Stage::Tail, chain)?;
    if let Some(to) = to {
        ensure!(
            to + 1 >= frontier,
            "--to {to} is below the frozen frontier {frontier}: frozen heights cannot be unwound"
        );
        let n = kdx_hot::tail::unwind_to(&mut txn, chain, to)?;
        tracing::info!(chain, to, frontier, old_tip = ?tip.map(|t| t.0), undone = n, "unwind");
        println!(
            "chain {chain}: {n} blocks unwound to height {to} (tip was {:?})",
            tip.map(|t| t.0)
        );
    }
    if clear_halt {
        check_consistent(&txn, chain, frontier)?;
        let name = kdx_ingest::apply::counters::halted(chain);
        let old = txn.counter(&name)?;
        txn.del(Table::Counters, name.as_bytes())?;
        tracing::warn!(chain, old_counter = old, "unwind: halted state cleared");
        println!("chain {chain}: halted counter {name} cleared (was {old})");
    }
    txn.commit()?;
    Ok(())
}
