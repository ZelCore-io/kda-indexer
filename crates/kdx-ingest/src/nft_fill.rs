//! NFT history fill and `NftDirty` reconcile (ruling N-1, plan Task 11).
//!
//! - **Fill** ([`run`], once per store, in `run` before the balance seed; `Meta nft_filled`):
//!   an ascending walk of the `evt` postings of every ledger event name (a module on the trusted
//!   ledger allowlist, `kdx_hot::nft::is_ledger`: `marmalade-v2.ledger`, `marmalade.ledger`). The hot `TailPostings` come first, then the
//!   segments (a block frozen meanwhile is seen in one of the two, or in both: replaying an
//!   event is idempotent, see `kdx_hot::nft`). Each tx's events go through
//!   `kdx_hot::nft::classify`: v2 `RECONCILE` parties are upserted (source 1), other ledger
//!   events mark `NftDirty`. Writes happen in batches of [`BATCH`] txs, each with a fresh read
//!   txn for the reads. A stopped fill resumes from the start (idempotent, small).
//! - **Dirty reconcile** ([`step`], called by the reconciler loop): `NftDirty` rows are read
//!   with `{ledger}.get-balance` through the shared [`LocalClient`] and stored as source 2.
//!   A cursor walks the table in key order and wraps, so a row that keeps failing at the node
//!   never blocks the others.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use futures::future::join_all;
use kdx_decode::Family;
use kdx_hot::nft::{self, meta, NftMark, NftRow};
use kdx_hot::tables::parse_txkey;
use kdx_hot::{checkpoint, HotDict, HotStore, RwTxn, Stage, Table, Txn};
use kdx_node::local::{Local, LocalClient, LocalError};
use kdx_segment::Segments;
use kdx_types::record::decode_tx;
use kdx_types::{Dict, DictKind, MemDict, TxKey};
use serde::Serialize;

/// Txs per write txn.
pub const BATCH: usize = 500;
/// `NftDirty` rows per reconcile step.
pub const DIRTY_BATCH: usize = 32;

#[derive(Debug, Default, Serialize)]
pub struct FillReport {
    /// Ledger event names walked.
    pub event_names: u64,
    pub hot_txs: u64,
    pub frozen_txs: u64,
    /// Rows written or marked.
    pub effects: u64,
    pub filled_height: Option<u32>,
    pub stopped: bool,
    pub secs: f64,
}

/// Whether the fill has finished on this store.
pub fn filled(txn: &impl Txn) -> Result<bool> {
    Ok(txn.meta(meta::NFT_FILLED)?.is_some())
}

/// EventName ids of ledger modules.
fn ledger_names(dict: &MemDict) -> Vec<u32> {
    (0..dict.len(DictKind::EventName) as u32)
        .filter(|&id| {
            dict.name(DictKind::EventName, id)
                .and_then(|n| n.rsplit_once('.'))
                .is_some_and(|(m, _)| nft::is_ledger(m))
        })
        .collect()
}

fn apply_batch(
    hot: &HotStore,
    dict: &MemDict,
    recs: Vec<(TxKey, kdx_types::record::TxRecord)>,
    rep: &mut FillReport,
) -> Result<()> {
    let mut txn = hot.rw()?;
    for (k, r) in recs {
        let evs = nft::classify(dict, &r.events);
        if !evs.is_empty() {
            rep.effects += u64::from(nft::apply_events(
                &mut txn,
                dict,
                &evs,
                k.chain(),
                k.height(),
            )?);
        }
    }
    txn.commit()
}

/// Run the fill. `stop` is polled between batches.
pub fn run(hot: &HotStore, segdir: &Path, stop: &dyn Fn() -> bool) -> Result<FillReport> {
    let t0 = Instant::now();
    let mut rep = FillReport::default();
    let dict: Arc<MemDict> = Arc::new(HotDict::load(&hot.ro()?)?.mem().clone());
    let names = ledger_names(&dict);
    rep.event_names = names.len() as u64;

    // Hot pass: tail txs, ascending.
    let mut keys: BTreeSet<TxKey> = BTreeSet::new();
    for &id in &names {
        let mut prefix = vec![Family::Evt as u8];
        prefix.extend_from_slice(&u64::from(id).to_be_bytes());
        for (k, _) in hot
            .ro()?
            .scan(Table::TailPostings, &prefix, |k| k.starts_with(&prefix))?
        {
            keys.insert(parse_txkey(&k[9..]).context("posting key")?);
        }
    }
    let keys: Vec<TxKey> = keys.into_iter().collect();
    for chunk in keys.chunks(BATCH) {
        let recs = {
            let txn = hot.ro()?;
            let mut v = Vec::with_capacity(chunk.len());
            for k in chunk {
                if let Some(b) = txn.get(Table::TailTxs, &k.to_be_bytes())? {
                    v.push((*k, decode_tx(&b).context("hot tx record")?));
                }
            }
            v
        };
        rep.hot_txs += recs.len() as u64;
        apply_batch(hot, &dict, recs, &mut rep)?;
        if stop() {
            rep.stopped = true;
            rep.secs = t0.elapsed().as_secs_f64();
            return Ok(rep);
        }
    }

    // Segment pass, oldest first (TxNum order).
    let segs = Segments::open(segdir, dict.clone())?;
    let mut nums: Vec<u64> = names
        .iter()
        .flat_map(|&id| segs.postings(Family::Evt, u64::from(id)))
        .collect();
    nums.sort_unstable();
    nums.dedup();
    for chunk in nums.chunks(BATCH) {
        let mut recs = Vec::with_capacity(chunk.len());
        for &t in chunk {
            if let (Some(k), Some(r)) = (segs.txkey(t), segs.tx(t)?) {
                recs.push((k, r));
            }
        }
        rep.frozen_txs += recs.len() as u64;
        apply_batch(hot, &dict, recs, &mut rep)?;
        if stop() {
            rep.stopped = true;
            rep.secs = t0.elapsed().as_secs_f64();
            return Ok(rep);
        }
    }

    // Done: the highest tail height marks the fill point.
    let mut txn = hot.rw()?;
    let mut top = txn.frozen_frontier()?;
    for c in 0..kdx_segment::SLOT_WIDTH as u8 {
        if let Some((h, _)) = checkpoint(&txn, Stage::Tail, c)? {
            top = top.max(h);
        }
    }
    txn.set_meta(meta::NFT_FILLED, &top.to_be_bytes())?;
    txn.commit()?;
    rep.filled_height = Some(top);
    rep.secs = t0.elapsed().as_secs_f64();
    Ok(rep)
}

// ---------------------------------------------------------------------------------------
// NftDirty reconcile

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StepStats {
    pub ok: u64,
    pub absent: u64,
    /// A delivered but unusable answer, or a B-1 refusal: the mark is dropped.
    pub dropped: u64,
    pub node_errors: u64,
}

impl StepStats {
    pub fn rows(&self) -> u64 {
        self.ok + self.absent + self.dropped + self.node_errors
    }
}

struct Item {
    key: [u8; 25],
    chain: u8,
    mark: NftMark,
    account: Option<String>,
    ledger: Option<String>,
    /// Tail height of the chain before the call (`as_of` of an absent answer).
    tail: u32,
}

enum Outcome {
    Found(String, u32),
    Absent,
    Dropped,
    NodeError,
}

fn dict_name(txn: &impl Txn, k: DictKind, id: u32) -> Result<Option<String>> {
    Ok(txn
        .get(Table::DictRev(k), &id.to_be_bytes())?
        .and_then(|v| String::from_utf8(v).ok()))
}

async fn call(local: &LocalClient, it: &Item) -> Outcome {
    let (Some(acct), Some(ledger)) = (&it.account, &it.ledger) else {
        return Outcome::Dropped;
    };
    match local
        .get_balance(it.chain, ledger, &it.mark.token_id, acct)
        .await
    {
        Ok(Local::Found { value, height }) => {
            Outcome::Found(value, u32::try_from(height).unwrap_or(u32::MAX))
        }
        Ok(Local::Absent) => Outcome::Absent,
        Err(LocalError::Skipped(_)) => Outcome::Dropped,
        Err(LocalError::Failure(m)) => {
            tracing::debug!(ledger = %ledger, message = %m, "nft get-balance failed");
            Outcome::Dropped
        }
        Err(LocalError::Node(e)) => {
            tracing::debug!(error = %e, "nft get-balance: node error, kept dirty");
            Outcome::NodeError
        }
    }
}

/// Store a `/local` answer unless the row is already newer (an event applied after the read).
fn write_local(txn: &mut RwTxn<'_>, it: &Item, balance: String, as_of: u32) -> Result<()> {
    let old = txn
        .get(Table::NftBalances, &it.key)?
        .and_then(|v| NftRow::decode(&v));
    if old.is_some_and(|o| o.as_of > as_of) {
        return Ok(());
    }
    let row = NftRow {
        source: nft::SOURCE_LOCAL,
        ledger: it.mark.ledger,
        as_of,
        token_id: it.mark.token_id.clone(),
        balance,
    };
    txn.put(Table::NftBalances, &it.key, &row.encode())
}

fn clear_if_covered(txn: &mut RwTxn<'_>, it: &Item, as_of: u32) -> Result<()> {
    if let Some(m) = txn
        .get(Table::NftDirty, &it.key)?
        .and_then(|v| NftMark::decode(&v))
    {
        if m.height <= as_of {
            txn.del(Table::NftDirty, &it.key)?;
        }
    }
    Ok(())
}

/// Reconcile the next [`DIRTY_BATCH`] `NftDirty` rows after `cursor` (key order; the cursor
/// wraps to the start at the end of the table). Returns what was done; all-zero when the table
/// is empty.
pub async fn step(
    hot: &HotStore,
    local: &LocalClient,
    cursor: &mut Option<Vec<u8>>,
) -> Result<StepStats> {
    let mut items = Vec::new();
    {
        let txn = hot.ro()?;
        let rows = txn.scan_chunk(Table::NftDirty, cursor.as_deref(), DIRTY_BATCH, |_| true)?;
        match rows.last() {
            Some((k, _)) => *cursor = Some(k.clone()),
            None => {
                *cursor = None;
                return Ok(StepStats::default());
            }
        }
        for (k, v) in rows {
            let (Ok(key), Some((account, chain)), Some(mark)) = (
                <[u8; 25]>::try_from(k.as_slice()),
                nft::parse_nft_key(&k).map(|(a, _, _, c)| (a, c)),
                NftMark::decode(&v),
            ) else {
                continue;
            };
            let tail = checkpoint(&txn, Stage::Tail, chain)?
                .map(|(h, _)| h)
                .unwrap_or(txn.frozen_frontier()?.saturating_sub(1));
            items.push(Item {
                key,
                chain,
                account: dict_name(&txn, DictKind::Account, account)?,
                ledger: dict_name(&txn, DictKind::Module, mark.ledger)?,
                mark,
                tail,
            });
        }
    }
    let outcomes: Vec<Outcome> = join_all(items.iter().map(|it| call(local, it))).await;
    let mut st = StepStats::default();
    let mut txn = hot.rw()?;
    for (it, out) in items.iter().zip(outcomes) {
        match out {
            Outcome::Found(balance, as_of) => {
                write_local(&mut txn, it, balance, as_of)?;
                clear_if_covered(&mut txn, it, as_of)?;
                st.ok += 1;
            }
            Outcome::Absent => {
                write_local(&mut txn, it, "0.0".into(), it.tail)?;
                clear_if_covered(&mut txn, it, it.tail)?;
                st.absent += 1;
            }
            Outcome::Dropped => {
                txn.del(Table::NftDirty, &it.key)?;
                st.dropped += 1;
            }
            Outcome::NodeError => st.node_errors += 1,
        }
    }
    txn.commit()?;
    Ok(st)
}
