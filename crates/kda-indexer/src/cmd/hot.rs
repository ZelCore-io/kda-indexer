//! Hidden `hot stats`: open the hot store read-only and print `Meta`, table row counts,
//! dictionary sizes, counters and the MDBX geometry.

use std::path::Path;

use anyhow::Context;
use kdx_hot::env::meta;
use kdx_hot::{Geometry, HotDict, HotStore, Table, Txn};
use kdx_types::DictKind;

use crate::config::Config;

/// The hot store lives in `<data_dir>/hot` (spec §6.6).
pub fn hot_dir(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("hot")
}

pub fn geometry(cfg: &Config) -> Geometry {
    Geometry {
        max_gb: cfg.storage.mdbx_max_gb,
        growth_mb: cfg.storage.mdbx_growth_mb,
    }
}

pub fn stats(cfg: &Config, data_dir: &Path, digest: bool) -> anyhow::Result<()> {
    let dir = hot_dir(data_dir);
    println!("hot store          {}", dir.display());
    // Read-only: no write txn, so it can run next to the service (docs/operations.md).
    let hs = HotStore::open_ro(&dir)?;
    let st = hs.env().stat().context("env stat")?;
    let info = hs.env().info().context("env info")?;
    println!(
        "geometry           page {} B, current map {} B, readers {}/{} (configured: growth {} MB, upper {} GB; recorded in Meta geometry)",
        st.page_size(),
        info.map_size(),
        info.num_readers(),
        info.max_readers(),
        cfg.storage.mdbx_growth_mb,
        cfg.storage.mdbx_max_gb,
    );
    let txn = hs.ro()?;
    println!("meta");
    for (k, v) in txn.scan(Table::Meta, &[], |_| true)? {
        let name = String::from_utf8_lossy(&k);
        let shown = match (name.as_ref(), v.len()) {
            (meta::FORMAT_VERSION | meta::NEXT_TXNUM, 8) => {
                u64::from_be_bytes(v.as_slice().try_into()?).to_string()
            }
            (meta::FROZEN_FRONTIER, 4) => u32::from_be_bytes(v.as_slice().try_into()?).to_string(),
            _ => String::from_utf8_lossy(&v).into_owned(),
        };
        println!("  {name:<16} {shown}");
    }
    println!("tables");
    for t in Table::all() {
        if digest && t != Table::Meta {
            // blake3 over every (key, value) row: replay checks compare two states.
            let mut h = blake3::Hasher::new();
            for (k, v) in txn.scan(t, &[], |_| true)? {
                h.update(&(k.len() as u32).to_be_bytes());
                h.update(&k);
                h.update(&(v.len() as u32).to_be_bytes());
                h.update(&v);
            }
            println!(
                "  {:<20} {:<10} {}",
                t.name(),
                txn.count(t)?,
                &h.finalize().to_hex()[..16]
            );
        } else {
            println!("  {:<20} {}", t.name(), txn.count(t)?);
        }
    }
    let dict = HotDict::load(&txn)?;
    let sizes: Vec<String> = DictKind::ALL
        .iter()
        .map(|k| format!("{k:?} {}", dict.len(*k)))
        .collect();
    println!("dict sizes         {}", sizes.join(", "));
    let counters = txn.scan(Table::Counters, &[], |_| true)?;
    if counters.is_empty() {
        println!("counters           (none)");
    }
    for (k, v) in counters {
        let n = v.as_slice().try_into().map(u64::from_be_bytes).unwrap_or(0);
        println!("  counter {:<10} {n}", String::from_utf8_lossy(&k));
    }
    Ok(())
}
