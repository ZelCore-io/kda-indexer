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
        println!("{}", meta_line(&k, &v));
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
    for line in counter_lines(&counters) {
        println!("{line}");
    }
    Ok(())
}

/// One `Meta` row. Known keys print as numbers; `dict_lens@<frontier>` as its five sizes;
/// anything else as text when printable, else hex. Never raw bytes: a NUL in the output makes
/// `grep` and friends treat the whole listing as binary and hide the counters below it.
fn meta_line(k: &[u8], v: &[u8]) -> String {
    let name = String::from_utf8_lossy(k);
    let shown = match (name.as_ref(), v.len()) {
        (meta::FORMAT_VERSION | meta::NEXT_TXNUM, 8) => {
            u64::from_be_bytes(v.try_into().unwrap_or_default()).to_string()
        }
        (meta::FROZEN_FRONTIER, 4) => {
            u32::from_be_bytes(v.try_into().unwrap_or_default()).to_string()
        }
        (n, _) if n.starts_with("dict_lens@") => match kdx_hot::dict::decode_lens(v) {
            Ok(l) => l.iter().map(u32::to_string).collect::<Vec<_>>().join(","),
            Err(_) => hex(v),
        },
        _ => match std::str::from_utf8(v) {
            Ok(t) if t.chars().all(|c| !c.is_control()) => t.to_owned(),
            _ => hex(v),
        },
    };
    format!("  {name:<16} {shown}")
}

fn hex(v: &[u8]) -> String {
    format!(
        "0x{}",
        v.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}

/// The `Counters` table, one `counter <name> <value>` line per row (values are u64 BE).
fn counter_lines(rows: &[(Vec<u8>, Vec<u8>)]) -> Vec<String> {
    if rows.is_empty() {
        return vec!["counters           (none)".to_owned()];
    }
    rows.iter()
        .map(|(k, v)| {
            let n = <[u8; 8]>::try_from(v.as_slice())
                .map(u64::from_be_bytes)
                .unwrap_or(0);
            format!("  counter {:<24} {n}", String::from_utf8_lossy(k))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{counter_lines, meta_line};

    #[test]
    fn counters_print_values() {
        let rows = vec![
            (b"halted.3".to_vec(), 1u64.to_be_bytes().to_vec()),
            (b"halted.3.merkle".to_vec(), 1u64.to_be_bytes().to_vec()),
            (b"user_txs".to_vec(), 986u64.to_be_bytes().to_vec()),
        ];
        let out = counter_lines(&rows);
        assert_eq!(out.len(), 3);
        assert!(
            out[0].contains("halted.3") && out[0].ends_with(" 1"),
            "{out:?}"
        );
        assert!(
            out[1].contains("halted.3.merkle") && out[1].ends_with(" 1"),
            "{out:?}"
        );
        assert!(out[2].ends_with(" 986"), "{out:?}");
        assert_eq!(counter_lines(&[]), ["counters           (none)"]);
    }

    #[test]
    fn meta_lines_are_text() {
        let mut lens = Vec::new();
        for n in [0u32, 7, 1, 2, 3] {
            lens.extend(n.to_be_bytes());
        }
        let line = meta_line(b"dict_lens@100", &lens);
        assert!(line.ends_with("0,7,1,2,3"), "{line}");
        let line = meta_line(b"odd", &[0, 1, 255]);
        assert!(line.ends_with("0x0001ff"), "{line}");
        assert!(meta_line(b"format_version", &2u64.to_be_bytes()).ends_with(" 2"));
    }
}
