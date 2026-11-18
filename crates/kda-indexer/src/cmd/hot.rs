//! Hidden `hot stats`: open the hot store read-only and print `Meta`, table row counts,
//! dictionary sizes, counters and the MDBX geometry.
//!
//! Hidden `hot drop-family sndr` (FIX-2): offline (takes the data-dir lock), one write txn;
//! removes the `sndr` family's `TailPostings` rows, its undo ops and `Meta sndr_hot`, so an
//! older binary (whose freezer rejects family 6) can run on the store.

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
        println!("  {:<20} {}", t.name(), txn.count(t)?);
    }
    if digest {
        // Name-resolved (ruling on 9B-1): records rendered through the dictionary, postings
        // by entity name, dictionaries as name sets; additive tables (balances, NFT, marks)
        // come from `/local` reads at wall-clock times and have no digest.
        println!("digest (name-resolved)");
        for (name, (rows, h)) in kdx_ingest::content::hot_digest(&txn)? {
            println!("  {name:<20} {rows:<10} {}", &h[..16]);
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

/// Parse a posting family name; only optional families can be dropped.
fn droppable(name: &str) -> anyhow::Result<kdx_decode::Family> {
    use kdx_decode::Family;
    match Family::ALL.into_iter().find(|f| f.name() == name) {
        Some(Family::Sndr) => Ok(Family::Sndr),
        Some(f) => anyhow::bail!(
            "posting family {} is required and cannot be dropped",
            f.name()
        ),
        None => anyhow::bail!("unknown posting family {name:?} (droppable: sndr)"),
    }
}

/// Open the existing hot store of `data_dir` for writing. A missing store is refused, never
/// created: a maintenance command on a mistyped data dir must fail, not start an empty one.
pub fn open_existing(cfg: &Config, data_dir: &Path) -> anyhow::Result<HotStore> {
    let dir = hot_dir(data_dir);
    if !dir.join("mdbx.dat").is_file() {
        anyhow::bail!("no hot store in {} (wrong --data-dir?)", dir.display());
    }
    HotStore::open(&dir, geometry(cfg))
}

pub fn drop_family(cfg: &Config, data_dir: &Path, family: &str) -> anyhow::Result<()> {
    let fam = droppable(family)?;
    let _lock = super::lock_data_dir(data_dir)?;
    let hs = open_existing(cfg, data_dir)?;
    let mut txn = hs.rw()?;
    let st = kdx_hot::sndr::drop_family(&mut txn, fam)?;
    txn.commit()?;
    tracing::info!(
        family,
        postings = st.postings,
        undo_ops = st.undo_ops,
        undo_rows = st.undo_rows,
        "hot drop-family"
    );
    println!(
        "dropped family {family}: {} TailPostings rows, {} undo ops in {} Undo rows, Meta {} cleared",
        st.postings,
        st.undo_ops,
        st.undo_rows,
        meta::SNDR_HOT
    );
    Ok(())
}

/// One `Meta` row. Known keys print as numbers; `dict_lens@<frontier>` as its five sizes;
/// anything else as text when printable, else hex. Never raw bytes: a NUL in the output makes
/// `grep` and friends treat the whole listing as binary and hide the counters below it.
fn meta_line(k: &[u8], v: &[u8]) -> String {
    let name = String::from_utf8_lossy(k);
    let shown = match (name.as_ref(), v.len()) {
        (meta::FORMAT_VERSION | meta::NEXT_TXNUM | meta::SNDR_HOT, 8) => {
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
    use super::{counter_lines, droppable, meta_line};

    #[test]
    fn only_sndr_can_be_dropped() {
        assert_eq!(droppable("sndr").unwrap(), kdx_decode::Family::Sndr);
        for f in ["acct", "xfer", "pk", "evt", "code", "pact"] {
            let e = droppable(f).unwrap_err().to_string();
            assert!(e.contains("required"), "{f}: {e}");
        }
        assert!(droppable("nope")
            .unwrap_err()
            .to_string()
            .contains("unknown"));
    }

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
