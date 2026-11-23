//! `kda-indexer reconcile-balances`: manual reruns of the balance seed and full re-reads, and
//! the reconcile status (plan Task 9). `run` seeds and reconciles by itself (B-5); `--seed`
//! and `--all` take the data dir lock, so they run while the service is stopped. `--seed`
//! also runs the receivers passes when pending (D1, then the token receivers pass D3);
//! `--seed --dry-run` only counts what they would mark (read-only, no lock). `--status` only reads.

use std::path::Path;

use kdx_hot::balances::{meta, BalanceRow};
use kdx_hot::{HotStore, Table, Txn};
use kdx_ingest::seed_receivers::Rules;
use serde_json::{json, Value};

use crate::config::Config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    /// Re-mark the posting holders of these modules and queue their D3 history walk now
    /// (D3 gaps: tokens whose rows a fixed rule missed).
    Rewalk(Vec<String>),
    /// Run (or resume) the seed walk (B-2). Marks rows dirty; the next `run` reads them.
    Seed,
    /// The receivers pass (D1) as a count only: candidates and would-be-marked rows; writes
    /// nothing (read-only store, no data dir lock).
    SeedDryRun,
    /// Mark every `Balances` row dirty (height 0; a tail mark is kept), and put the keys
    /// parked for unusable answers (`balances_parked_invalid/`, H-1) back in the seed backlog.
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
        Mode::Status => status(cfg, &HotStore::open_ro(&hot_dir)?),
        Mode::Rewalk(names) => {
            println!("{}", rewalk(cfg, data_dir, &names)?);
            Ok(())
        }
        Mode::Seed => {
            let _lock = super::lock_data_dir(data_dir)?;
            let hs = HotStore::open(&hot_dir, super::hot::geometry(cfg))?;
            let segdir = kdx_ingest::ctx::segments_dir(data_dir);
            let rep = kdx_ingest::seed::run(&hs, &segdir, &|| false)?;
            println!("{}", serde_json::to_string_pretty(&rep)?);
            // D1: the receivers pass, once per store (`Meta balances_seed_receivers`); D3: the
            // token receivers pass (`Meta balances_seed_token_receivers`), up to the tail.
            receivers(cfg, &hs, &segdir, false, Rules::D1)?;
            receivers(cfg, &hs, &segdir, false, Rules::D3)
        }
        Mode::SeedDryRun => {
            let hs = HotStore::open_ro(&hot_dir)?;
            let segdir = kdx_ingest::ctx::segments_dir(data_dir);
            receivers(cfg, &hs, &segdir, true, Rules::D1)?;
            receivers(cfg, &hs, &segdir, true, Rules::D3)
        }
        Mode::NftFill => {
            let _lock = super::lock_data_dir(data_dir)?;
            let hs = HotStore::open(&hot_dir, super::hot::geometry(cfg))?;
            let segdir = kdx_ingest::ctx::segments_dir(data_dir);
            let rep = kdx_ingest::nft_fill::run(&hs, &segdir, None, &|| false)?;
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
            let mut txn = hs.rw()?;
            let parked_invalid = kdx_hot::balances::requeue_parked_invalid(&mut txn)?;
            txn.commit()?;
            println!(
                "{}",
                json!({ "marked_dirty": marked, "parked_invalid_requeued": parked_invalid })
            );
            Ok(())
        }
    }
}

/// `--rewalk`: under the data-dir lock, mark every row the listed modules' `xfer` postings
/// name and queue their D3 history walk now. `coin` and unknown names are refused before
/// anything is written. Returns the JSON report the command prints.
pub fn rewalk(cfg: &Config, data_dir: &Path, names: &[String]) -> anyhow::Result<Value> {
    // N-6: `coin` would mark every coin holder (millions) into the seed backlog; its rows are
    // kept by the tail and `--seed`/`--all`, not by the token walk.
    if names.iter().any(|n| n.trim() == "coin") {
        anyhow::bail!("--rewalk does not take coin (use --seed or --all)");
    }
    let _lock = super::lock_data_dir(data_dir)?;
    let hs = HotStore::open(&super::hot::hot_dir(data_dir), super::hot::geometry(cfg))?;
    let segdir = kdx_ingest::ctx::segments_dir(data_dir);
    let ids = module_ids(&hs, names)?;
    let set: std::collections::HashSet<u32> = ids.iter().copied().collect();
    let rep = kdx_ingest::seed::mark_modules(&hs, &segdir, &set, &|| false)?;
    let mut txn = hs.rw()?;
    let restarted = kdx_hot::balances::queue_rewalk(&mut txn, &ids)?;
    txn.commit()?;
    Ok(json!({
        "modules": names,
        "posting_rows_marked": rep.dirty_inserted,
        "rewalk_queued": ids.len(),
        "walk_in_progress_restarted": restarted,
        "rewalk_pending": kdx_hot::balances::rewalk_pending(&hs.ro()?)?.len(),
    }))
}

/// The module ids of `names`; an unknown name is an error (nothing is written).
fn module_ids(hs: &HotStore, names: &[String]) -> anyhow::Result<Vec<u32>> {
    let txn = hs.ro()?;
    let mut out = Vec::new();
    for n in names {
        let n = n.trim();
        let id = txn
            .get(
                Table::DictFwd(kdx_types::DictKind::Module),
                &kdx_hot::dict::fwd_key(n.as_bytes()),
            )?
            .and_then(|v| <[u8; 4]>::try_from(v.as_slice()).ok())
            .map(u32::from_be_bytes)
            .ok_or_else(|| anyhow::anyhow!("unknown module: {n}"))?;
        if !out.contains(&id) {
            out.push(id);
        }
    }
    Ok(out)
}

/// A receivers pass (D1 or D3), or its dry run; fails when it could not read some txs (L1).
fn receivers(
    cfg: &Config,
    hs: &HotStore,
    segdir: &Path,
    dry_run: bool,
    rules: Rules,
) -> anyhow::Result<()> {
    let dict = std::sync::Mutex::new(kdx_hot::HotDict::load(&hs.ro()?)?);
    let opts = kdx_ingest::seed_receivers::Opts {
        forks: kdx_ingest::seed_receivers::forks_for(&cfg.network),
        dry_run,
        rules,
        // Per-token budget of distinct rows; drops show in `seed_token_receivers_dropped`.
        max_event_candidates: std::env::var("KDX_D3_MAX_EVENT_CANDIDATES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(kdx_ingest::seed_receivers::MAX_EVENT_CANDIDATES),
        ..Default::default()
    };
    let rep = kdx_ingest::seed_receivers::run(hs, &dict, segdir, &opts, &|| false)?;
    let key = match rules {
        Rules::D1 => "receivers",
        Rules::D3 => "token_receivers",
    };
    println!("{}", json!({ key: rep }));
    if rep.errors > 0 {
        anyhow::bail!(
            "receivers pass: {} txs could not be read (logged with block, chain and request \
             key); their receivers may be missing",
            rep.errors
        );
    }
    Ok(())
}

/// F-14 status: the non-coin module with the most `Balances` rows (`coin` has no cap) and the modules flagged at
/// their row cap (`Tokens` rows, read in chunks of short txns).
#[derive(Default)]
struct ModuleCaps {
    largest: Value,
    capped: Vec<String>,
}

fn module_caps(hs: &HotStore) -> anyhow::Result<ModuleCaps> {
    let name = |m: u32| -> anyhow::Result<String> {
        Ok(hs
            .ro()?
            .get(
                Table::DictRev(kdx_types::DictKind::Module),
                &m.to_be_bytes(),
            )?
            .map(|v| String::from_utf8_lossy(&v).into_owned())
            .unwrap_or_else(|| format!("#{m}")))
    };
    let mut rows: Vec<(u32, u32)> = Vec::new();
    let mut capped = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let chunk = hs
            .ro()?
            .scan_chunk(Table::Tokens, after.as_deref(), ALL_CHUNK, |_| true)?;
        for (k, v) in &chunk {
            let (Some(m), Some(t)) = (
                k.as_slice().try_into().ok().map(u32::from_be_bytes),
                kdx_hot::balances::TokenRow::decode(v),
            ) else {
                continue;
            };
            if t.capped() {
                capped.push(m);
            }
            rows.push((t.rows, m));
        }
        match chunk.last() {
            Some((k, _)) if chunk.len() == ALL_CHUNK => after = Some(k.clone()),
            _ => break,
        }
    }
    rows.sort_unstable_by(|a, b| b.cmp(a));
    let mut largest = Value::Null;
    for (n, m) in rows {
        let module = name(m)?;
        if module != "coin" {
            largest = json!({ "module": module, "rows": n });
            break;
        }
    }
    Ok(ModuleCaps {
        largest,
        capped: capped
            .into_iter()
            .map(name)
            .collect::<anyhow::Result<_>>()?,
    })
}

fn status(cfg: &Config, hs: &HotStore) -> anyhow::Result<()> {
    // F-46: no read txn spans the Balances scan (it pins MDBX pages while `run` writes).
    // The first short txn reads the progress counters, the scan pages in its own short txns,
    // and a second short txn reads the rest.
    let (dirty, stats, rows) = {
        let txn = hs.ro()?;
        let dirty = kdx_hot::balances::dirty_count(&txn)?;
        let stats: Value = txn
            .meta(kdx_ingest::reconcile::META_STATS)?
            .and_then(|v| serde_json::from_slice(&v).ok())
            .unwrap_or(Value::Null);
        (dirty, stats, txn.count(Table::Balances)?)
    };
    let rate = stats.get("rate").and_then(Value::as_f64).unwrap_or(0.0);
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
    let caps = module_caps(hs)?;
    let txn = hs.ro()?;
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
        "nft_details_rows": txn.count(Table::NftDetails)?,
        "nft_filled_height": txn
            .meta(kdx_hot::nft::meta::NFT_FILLED)?
            .and_then(|v| v.try_into().ok().map(u32::from_be_bytes)),
        "seeded_height": seeded,
        "seed_in_progress": txn.meta(meta::BALANCES_SEED_CURSOR)?.is_some(),
        "seed_receivers_height": txn
            .meta(meta::BALANCES_SEED_RECEIVERS)?
            .and_then(|v| v.try_into().ok().map(u32::from_be_bytes)),
        "seed_receivers_in_progress": txn
            .meta(meta::BALANCES_SEED_RECEIVERS_CURSOR)?
            .is_some(),
        // D1 L1: txs the receivers pass could not read; must be 0 (see its logs otherwise).
        "seed_receivers_errors": txn
            .meta(meta::BALANCES_SEED_RECEIVERS_ERRORS)?
            .and_then(|v| v.try_into().ok().map(u64::from_be_bytes)),
        // D3: the token receivers pass (done-through height, running, unreadable txs).
        "seed_token_receivers_height": txn
            .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)?
            .and_then(|v| v.try_into().ok().map(u32::from_be_bytes)),
        "seed_token_receivers_in_progress": txn
            .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS_CURSOR)?
            .is_some(),
        "d3_discover_queued": kdx_hot::balances::discovered_queue(&txn, kdx_hot::balances::D3_DISCOVER_QUEUE_MAX)?.len(),
        "d3_rewalk_pending": kdx_hot::balances::rewalk_pending(&txn)?.len(),
        // H-1: keys parked after repeated unusable `details` answers; `--all` re-queues them.
        "parked_invalid": kdx_hot::balances::parked_invalid(&txn)?.len(),
        "d3_discover_pending": kdx_hot::balances::discover_pending(&txn, kdx_hot::balances::D3_DISCOVER_PENDING_MAX)?.len(),
        "seed_token_receivers_dropped": txn
            .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS_DROPPED)?
            .and_then(|v| serde_json::from_slice::<Value>(&v).ok()),
        "seed_token_receivers_errors": txn
            .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS_ERRORS)?
            .and_then(|v| v.try_into().ok().map(u64::from_be_bytes)),
        // F-14: row caps per module and over-size guards (stored as null).
        "fungible_module_rows_cap": cfg.balances.fungible_module_rows,
        "untrusted_module_rows_cap": cfg.balances.module_rows,
        "largest_module_rows": caps.largest,
        "capped_modules": caps.capped,
        "guards_capped": txn
            .meta(meta::GUARDS_CAPPED)?
            .and_then(|v| v.try_into().ok().map(u64::from_be_bytes)),
        "rate_per_s": rate,
        "eta_secs": (rate > 0.0).then(|| (dirty as f64 / rate).round() as u64),
        "heartbeat_age_secs": stats.get("t").and_then(Value::as_u64).map(|t| now.saturating_sub(t)),
        "done_since_start": stats.get("done"),
    });
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}
