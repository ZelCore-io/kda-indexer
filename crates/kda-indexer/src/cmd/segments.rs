//! `segments list` and `segments check [--deep]` over `<data_dir>/segments`.

use std::path::{Path, PathBuf};

use anyhow::bail;
use kdx_segment::files::{file_name, FileKind};
use kdx_segment::{Manifest, RangeInfo};

/// Frozen segments live in `<data_dir>/segments` (spec §3.1).
pub fn segments_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("segments")
}

pub fn list(data_dir: &Path, json: bool) -> anyhow::Result<()> {
    let dir = segments_dir(data_dir);
    let Some(m) = Manifest::load(&dir)? else {
        bail!("no manifest.json in {}", dir.display());
    };
    let mut rows = Vec::new();
    for e in &m.entries {
        let info =
            RangeInfo::load(&dir.join(file_name(&m.network, FileKind::Range, e.start, e.end)))?;
        let bytes: u64 = info.files.values().map(|f| f.bytes).sum();
        rows.push((e.clone(), info, bytes));
    }
    if json {
        let v: Vec<_> = rows
            .iter()
            .map(|(e, i, b)| {
                serde_json::json!({
                    "kind": e.kind.name(), "start": e.start, "end": e.end,
                    "first_txnum": e.first_txnum, "tx_count": e.tx_count,
                    "block_count": e.block_count, "bytes": b, "level": i.level,
                    "rk_index": i.rk_index, "bh_index": i.bh_index,
                    "cmd_reencoded": i.cmd_reencoded, "postings": i.postings,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "network": m.network, "frontier": m.frontier,
                "next_txnum": m.next_txnum, "entries": v,
            }))?
        );
        return Ok(());
    }
    println!("segments    {}", dir.display());
    println!("network     {}", m.network);
    println!("frontier    {}", m.frontier);
    println!("next_txnum  {}", m.next_txnum);
    println!(
        "{:<6} {:>10} {:>10} {:>12} {:>10} {:>9} {:>12} {:>5} {:>8} {:>8}",
        "kind", "start", "end", "first_txnum", "txs", "blocks", "bytes", "lvl", "rk", "bh"
    );
    let mut total = 0u64;
    for (e, i, b) in &rows {
        total += b;
        println!(
            "{:<6} {:>10} {:>10} {:>12} {:>10} {:>9} {:>12} {:>5} {:>8} {:>8}",
            e.kind.name(),
            e.start,
            e.end,
            e.first_txnum,
            e.tx_count,
            e.block_count,
            b,
            i.level,
            i.rk_index,
            i.bh_index
        );
    }
    println!("{} entries, {} bytes", rows.len(), total);
    Ok(())
}

pub fn check(data_dir: &Path, deep: bool) -> anyhow::Result<()> {
    let dir = segments_dir(data_dir);
    let t0 = std::time::Instant::now();
    let rep = kdx_segment::check(&dir, deep)?;
    println!("{}", serde_json::to_string_pretty(&rep)?);
    eprintln!(
        "segments check{}: {} ({} ranges, {:.1}s)",
        if deep { " --deep" } else { "" },
        if rep.ok() { "OK" } else { "FAILED" },
        rep.ranges,
        t0.elapsed().as_secs_f64()
    );
    if !rep.ok() {
        std::process::exit(1);
    }
    Ok(())
}
