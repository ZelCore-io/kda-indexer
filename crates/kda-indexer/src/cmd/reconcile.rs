//! `kda-indexer reconcile-balances`: manual reruns of the balance seed and full re-reads, and
//! the reconcile status (plan Task 9). `run` seeds and reconciles by itself (B-5); `--seed`
//! and `--all` take the data dir lock, so they run while the service is stopped. `--status`
//! only reads.

use std::path::Path;

use kdx_hot::balances::{meta, BalanceRow};
use kdx_hot::{HotStore, Table, Txn};
use serde_json::{json, Value};

use crate::config::Config;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Run (or resume) the seed walk (B-2). Marks rows dirty; the next `run` reads them.
    Seed,
    /// Mark every `Balances` row dirty (height 0; a tail mark is kept).
    All,
    /// Run the NFT history fill again (plan Task 11).
    NftFill,
    /// Dirty rows, rate, ETA, seed state.
    Status,
}

/// Rows per write txn of `--all`.
const ALL_CHUNK: usize = 100_000;

pub async fn run(cfg: &Config, data_dir: &Path, mode: Mode) -> anyhow::Result<()> {
    let hot_dir = super::hot::hot_dir(data_dir);
    match mode {
        Mode::Status => status(&HotStore::open_ro(&hot_dir)?),
        Mode::Seed => {
            let _lock = super::lock_data_dir(data_dir)?;
            let hs = HotStore::open(&hot_dir, super::hot::geometry(cfg))?;
            let segdir = kdx_ingest::ctx::segments_dir(data_dir);
            let rep = kdx_ingest::seed::run(&hs, &segdir, &|| false)?;
            println!("{}", serde_json::to_string_pretty(&rep)?);
            Ok(())
        }
        Mode::NftFill => {
            let _lock = super::lock_data_dir(data_dir)?;
            let hs = HotStore::open(&hot_dir, super::hot::geometry(cfg))?;
            let segdir = kdx_ingest::ctx::segments_dir(data_dir);
            let rep = kdx_ingest::nft_fill::run(&hs, &segdir, &|| false)?;
            println!("{}", serde_json::to_string_pretty(&rep)?);
            // Then drain the marks it made with `get-balance` (the service does this itself).
            let local = kdx_node::LocalClient::new(
                super::local_node_client(cfg)?,
                kdx_node::LocalOpts {
                    rps: cfg.balances.local_rps,
                    concurrency: cfg.balances.local_concurrency.max(1) as usize,
                },
            );
            let (mut cursor, mut total) = (None, kdx_ingest::nft_fill::StepStats::default());
            for _ in 0..10_000 {
                let st = kdx_ingest::nft_fill::step(&hs, &local, &mut cursor).await?;
                if st.rows() == 0 {
                    break;
                }
                total.ok += st.ok;
                total.absent += st.absent;
                total.dropped += st.dropped;
                total.node_errors += st.node_errors;
            }
            println!("{}", json!({ "dirty_reconciled": format!("{total:?}") }));
            Ok(())
        }
        Mode::All => {
            let _lock = super::lock_data_dir(data_dir)?;
            let hs = HotStore::open(&hot_dir, super::hot::geometry(cfg))?;
            let mut after: Option<Vec<u8>> = None;
            let mut marked = 0u64;
            loop {
                let rows =
                    hs.ro()?
                        .scan_chunk(Table::Balances, after.as_deref(), ALL_CHUNK, |_| true)?;
                let mut txn = hs.rw()?;
                let mark = kdx_hot::balances::Mark {
                    height: 0,
                    seq: txn.txn_id()?,
                }
                .encode();
                for (k, _) in &rows {
                    let Ok(k) = <[u8; 9]>::try_from(k.as_slice()) else {
                        continue;
                    };
                    if kdx_hot::balances::seed_mark(&mut txn, &k, &mark)? {
                        marked += 1;
                    }
                }
                txn.commit()?;
                match rows.last() {
                    Some((k, _)) if rows.len() == ALL_CHUNK => after = Some(k.clone()),
                    _ => break,
                }
            }
            println!("{}", json!({ "marked_dirty": marked }));
            Ok(())
        }
    }
}

fn status(hs: &HotStore) -> anyhow::Result<()> {
    let txn = hs.ro()?;
    let dirty = kdx_hot::balances::dirty_count(&txn)?;
    let stats: Value = txn
        .meta(kdx_ingest::reconcile::META_STATS)?
        .and_then(|v| serde_json::from_slice(&v).ok())
        .unwrap_or(Value::Null);
    let rate = stats.get("rate").and_then(Value::as_f64).unwrap_or(0.0);
    let rows = txn.count(Table::Balances)?;
    let mut absent = 0u64;
    let mut after: Option<Vec<u8>> = None;
    loop {
        // A fresh read txn per chunk (no long-held reader).
        let chunk = hs
            .ro()?
            .scan_chunk(Table::Balances, after.as_deref(), ALL_CHUNK, |_| true)?;
        absent += chunk
            .iter()
            .filter(|(_, v)| BalanceRow::decode(v).is_some_and(|r| r.absent))
            .count() as u64;
        match chunk.last() {
            Some((k, _)) if chunk.len() == ALL_CHUNK => after = Some(k.clone()),
            _ => break,
        }
    }
    let seeded = txn
        .meta(meta::BALANCES_SEEDED)?
        .and_then(|v| v.try_into().ok().map(u32::from_be_bytes));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let out = json!({
        "tables_present": txn.has_table(Table::Balances),
        "dirty_rows": dirty,
        "balance_rows": rows,
        "absent_rows": absent,
        "guards": txn.count(Table::Guards)?,
        "key_accounts": txn.count(Table::KeyAccounts)?,
        "tokens": txn.count(Table::Tokens)?,
        "nft_rows": txn.count(Table::NftBalances)?,
        "nft_dirty_rows": txn.count(Table::NftDirty)?,
        "nft_filled_height": txn
            .meta(kdx_hot::nft::meta::NFT_FILLED)?
            .and_then(|v| v.try_into().ok().map(u32::from_be_bytes)),
        "seeded_height": seeded,
        "seed_in_progress": txn.meta(meta::BALANCES_SEED_CURSOR)?.is_some(),
        "rate_per_s": rate,
        "eta_secs": (rate > 0.0).then(|| (dirty as f64 / rate).round() as u64),
        "heartbeat_age_secs": stats.get("t").and_then(Value::as_u64).map(|t| now.saturating_sub(t)),
        "done_since_start": stats.get("done"),
    });
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}
