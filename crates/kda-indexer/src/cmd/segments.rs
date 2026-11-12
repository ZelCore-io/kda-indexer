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
        let info = RangeInfo::load(&kdx_segment::swap::resolve(
            &dir,
            &file_name(&m.network, FileKind::Range, e.start, e.end),
        ))?;
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
                    "format_version": i.format_version, "files": i.files,
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

/// `segments digest [--content] [--start S]`: one line per entry and a total. `--content`
/// digests the decoded records with names resolved through the hot dictionary (ruling on
/// 9B-1/9B-2: equal for the same blocks whatever the ids or compression); the total is fed
/// across entries in height order, so it does not depend on the range/step split. Without
/// `--content`, blake3 of the files (the `range.toml` hashes plus its own).
pub fn digest(
    _cfg: &crate::config::Config,
    data_dir: &Path,
    content: bool,
    start: Option<u32>,
) -> anyhow::Result<()> {
    use anyhow::Context;
    let dir = segments_dir(data_dir);
    let Some(m) = Manifest::load(&dir)? else {
        bail!("no manifest.json in {}", dir.display());
    };
    let dict = if content {
        let hs = kdx_hot::HotStore::open_ro(&data_dir.join("hot")).context("hot store")?;
        let txn = hs.ro()?;
        Some(kdx_hot::HotDict::load(&txn)?)
    } else {
        None
    };
    let mut total = kdx_segment::content::ContentDigest::new();
    let mut files = blake3::Hasher::new();
    for e in m
        .entries
        .iter()
        .filter(|e| start.is_none_or(|s| s == e.start))
    {
        let line = match &dict {
            Some(d) => {
                let r = kdx_segment::RangeReader::open(&dir, e, &m.network)?;
                let c = kdx_segment::content::range(&r, d)?;
                kdx_segment::content::feed(&r, d, &mut total)?;
                format!("{} blocks {} txs content {}", c.blocks, c.txs, c.blake3)
            }
            None => {
                let mut h = blake3::Hasher::new();
                for n in kdx_segment::files::range_file_names(&m.network, e.start, e.end) {
                    let b = kdx_segment::files::blake3_file(&kdx_segment::swap::resolve(&dir, &n))?;
                    h.update(n.as_bytes());
                    h.update(b.as_bytes());
                }
                let d = h.finalize().to_hex().to_string();
                files.update(d.as_bytes());
                format!("files {d}")
            }
        };
        println!("{:<5} {:>9}..{:<9} {line}", e.kind.name(), e.start, e.end);
    }
    if dict.is_some() {
        let c = total.finish();
        println!(
            "total {} blocks {} txs content {}",
            c.blocks, c.txs, c.blake3
        );
    } else {
        println!("total files {}", files.finalize().to_hex());
    }
    Ok(())
}
