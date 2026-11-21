//! The balance seed's receivers pass (D1): mark `(receiver, coin, chain)` dirty for the
//! accounts that received KDA with no event naming them, before coin v3
//! (`kdx_decode::receivers`). The posting-based seed ([`crate::seed`]) and the tail marks
//! never see them, so without this pass a funded row can be missing for good.
//!
//! - Scope: the stored user txs with heights in `[from, to)`, where `from` is the height the
//!   last finished pass reached (`Meta balances_seed_receivers`, 0 when absent) and `to` is
//!   [`target`]: `COIN_V3_HEIGHT`, or lower while some chain's stored blocks end below it.
//!   On mainnet the whole history below the cutoff is frozen before the tail starts, so the
//!   pass runs once per store; a store whose tail is still below the cutoff runs it again
//!   later for the new heights only.
//! - Order: hot `TailTxs` first (no cursor: small, redone on resume), then the segments in
//!   TxNum order (opened after the hot pass, so a block the freezer moves meanwhile is seen
//!   in one of the two).
//! - Writes: names are interned in the shared dictionary (lock order: dictionary, then the
//!   MDBX writer; an account no posting named gets its first id here) and marked with
//!   `seed_mark` (height 0, never overwrites a tail mark), every [`TX_CHUNK`] txs with the
//!   resume cursor `Meta balances_seed_receivers_cursor` (start height, next TxNum). The end
//!   sets `balances_seed_receivers` to `to` and removes the cursor. Idempotent: a rerun marks
//!   the same rows.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use kdx_decode::receivers::{record_receivers, COIN_V3_HEIGHT};
use kdx_hot::balances::{self, bal_key, meta};
use kdx_hot::{checkpoint, HotDict, HotStore, Stage, Table, Txn};
use kdx_segment::Segments;
use kdx_types::record::decode_tx;
use kdx_types::{DictKind, DictMut, MemDict, TxKey};
use serde::Serialize;

/// Txs per write txn (and between `stop` polls) in the segment walk.
pub const TX_CHUNK: usize = 50_000;
/// Hot `TailTxs` rows per read txn.
const HOT_CHUNK: usize = 10_000;

#[derive(Debug, Default, Serialize)]
pub struct ReceiversReport {
    pub from: u32,
    pub to: u32,
    pub hot_txs: u64,
    pub frozen_txs: u64,
    /// Distinct `(account, chain)` candidates written (per write txn).
    pub candidates: u64,
    /// Accounts the dictionary did not know (no posting ever named them).
    pub new_accounts: u64,
    /// Dirty rows inserted (rows already dirty are not counted).
    pub dirty_inserted: u64,
    /// Records whose cmd or continuation could not be read (skipped, logged).
    pub errors: u64,
    /// No `coin` module in the dictionary: nothing can be marked, the pass stays pending.
    pub no_coin: bool,
    pub stopped: bool,
    pub secs: f64,
}

/// The height below which every chain's blocks are stored (frozen, or hot up to the chain's
/// tail checkpoint), capped at [`COIN_V3_HEIGHT`]. Chains without a checkpoint are ignored.
pub fn target(txn: &impl Txn) -> Result<u32> {
    let f = txn.frozen_frontier()?;
    let mut low: Option<u32> = None;
    for c in 0..kdx_segment::SLOT_WIDTH as u8 {
        if let Some((h, _)) = checkpoint(txn, Stage::Tail, c)? {
            let top = f.max(h.saturating_add(1));
            low = Some(low.map_or(top, |l| l.min(top)));
        }
    }
    Ok(low.unwrap_or(f).min(COIN_V3_HEIGHT))
}

/// `Some((from, to))` when the pass has work: it never finished on this store, or the stored
/// history below the cutoff grew since it did.
pub fn pending(txn: &impl Txn) -> Result<Option<(u32, u32)>> {
    let done = txn
        .meta(meta::BALANCES_SEED_RECEIVERS)?
        .as_deref()
        .and_then(be_u32);
    let to = target(txn)?;
    let from = done.unwrap_or(0);
    Ok((done.is_none() || to > from).then_some((from, to.max(from))))
}

fn be_u32(v: &[u8]) -> Option<u32> {
    Some(u32::from_be_bytes(v.try_into().ok()?))
}

/// Pending marks of one write txn.
#[derive(Default)]
struct Pending {
    rows: BTreeSet<(String, u8)>,
}

impl Pending {
    fn flush(
        &mut self,
        hot: &HotStore,
        dict: &Mutex<HotDict>,
        coin: u32,
        cursor: Option<[u8; 12]>,
        rep: &mut ReceiversReport,
    ) -> Result<()> {
        let mut d = dict.lock().unwrap_or_else(|e| e.into_inner());
        let r = self.write(hot, &mut d, coin, cursor, rep);
        if r.is_err() {
            // Names drained from `pending` never reached MDBX: forget them (review M3).
            *d = HotDict::load(&hot.ro()?)?;
        }
        r
    }

    fn write(
        &mut self,
        hot: &HotStore,
        d: &mut HotDict,
        coin: u32,
        cursor: Option<[u8; 12]>,
        rep: &mut ReceiversReport,
    ) -> Result<()> {
        let mut txn = hot.rw()?;
        let mark = balances::Mark {
            height: 0,
            seq: txn.txn_id()?,
        }
        .encode();
        let before = d.len(DictKind::Account);
        let rows: Vec<[u8; 9]> = self
            .rows
            .iter()
            .map(|(a, c)| bal_key(d.intern(DictKind::Account, a), coin, *c))
            .collect();
        rep.new_accounts += (d.len(DictKind::Account) - before) as u64;
        d.persist(&mut txn)?;
        for k in &rows {
            if balances::seed_mark(&mut txn, k, &mark)? {
                rep.dirty_inserted += 1;
            }
        }
        if let Some(c) = cursor {
            txn.set_meta(meta::BALANCES_SEED_RECEIVERS_CURSOR, &c)?;
        }
        txn.commit()?;
        rep.candidates += self.rows.len() as u64;
        self.rows.clear();
        Ok(())
    }
}

fn add(
    p: &mut Pending,
    k: TxKey,
    r: &kdx_types::record::TxRecord,
    dict: &MemDict,
    rep: &mut ReceiversReport,
) {
    match record_receivers(r, dict) {
        Ok(v) => p.rows.extend(v.into_iter().map(|a| (a, k.chain()))),
        Err(e) => {
            rep.errors += 1;
            tracing::warn!(key = ?k, error = %e, "balances: receivers of a tx not readable");
        }
    }
}

fn cursor(from: u32, next: u64) -> [u8; 12] {
    let mut v = [0u8; 12];
    v[..4].copy_from_slice(&from.to_be_bytes());
    v[4..].copy_from_slice(&next.to_be_bytes());
    v
}

/// Run (or resume) the pass when [`pending`]; otherwise return an empty report.
pub fn run(
    hot: &HotStore,
    dict: &Mutex<HotDict>,
    segdir: &Path,
    stop: &dyn Fn() -> bool,
) -> Result<ReceiversReport> {
    run_with(hot, dict, segdir, TX_CHUNK, stop)
}

/// [`run`] with `chunk` txs per write txn. `stop` is polled after each segment chunk; a
/// stopped pass commits its chunk and cursor and returns with `stopped`.
pub fn run_with(
    hot: &HotStore,
    dict: &Mutex<HotDict>,
    segdir: &Path,
    chunk: usize,
    stop: &dyn Fn() -> bool,
) -> Result<ReceiversReport> {
    let t0 = Instant::now();
    let mut rep = ReceiversReport::default();
    let (coin, span, resume) = {
        let txn = hot.ro()?;
        let resume = txn
            .meta(meta::BALANCES_SEED_RECEIVERS_CURSOR)?
            .and_then(|v| {
                let from = be_u32(v.get(..4)?)?;
                Some((from, u64::from_be_bytes(v.get(4..12)?.try_into().ok()?)))
            });
        (balances::coin_module(&txn)?, pending(&txn)?, resume)
    };
    let Some((from, to)) = span else {
        return Ok(rep);
    };
    (rep.from, rep.to) = (from, to);
    let Some(coin) = coin else {
        rep.no_coin = true;
        tracing::warn!("balances: no coin module yet; receivers pass postponed (D1)");
        return Ok(rep);
    };
    tracing::info!(from, to, "balances: receivers pass (D1) started");
    let snap: Arc<MemDict> = Arc::new(dict.lock().unwrap_or_else(|e| e.into_inner()).mem().clone());
    let mut p = Pending::default();

    // Hot pass: TailTxs at heights [from, to), key order.
    let mut after: Option<Vec<u8>> = None;
    let first = TxKey::new(from, 0, 0).to_be_bytes();
    loop {
        let rows = {
            let txn = hot.ro()?;
            let keep = |k: &[u8]| {
                k.get(..8)
                    .and_then(|b| b.try_into().ok())
                    .is_some_and(|b| TxKey::from_be_bytes(b).height() < to)
            };
            match &after {
                None => txn.scan_limit(Table::TailTxs, &first, HOT_CHUNK, keep)?,
                Some(a) => txn.scan_chunk(Table::TailTxs, Some(a), HOT_CHUNK, keep)?,
            }
        };
        for (k, v) in &rows {
            let key = TxKey::from_be_bytes(k[..8].try_into().context("TailTxs key")?);
            if key.is_coinbase() {
                continue;
            }
            match decode_tx(v) {
                Ok(r) => add(&mut p, key, &r, &snap, &mut rep),
                Err(e) => {
                    rep.errors += 1;
                    tracing::warn!(key = ?key, error = %e, "balances: hot tx record not readable");
                }
            }
            rep.hot_txs += 1;
        }
        match rows.last() {
            Some((k, _)) if rows.len() == HOT_CHUNK => after = Some(k.clone()),
            _ => break,
        }
    }
    p.flush(hot, dict, coin, None, &mut rep)?;

    // Segment pass, TxNum order, resuming at the cursor of a pass with the same start.
    let segs = Segments::open_without_filters(segdir, snap.clone())?;
    let start = match segs.last_txnum_below_height(from) {
        Some(t) => t + 1,
        None => segs.manifest.entries.first().map_or(0, |e| e.first_txnum),
    };
    let start = match resume {
        Some((f, next)) if f == from => start.max(next),
        _ => start,
    };
    let end = segs.last_txnum_below_height(to).map_or(start, |t| t + 1);
    let mut n = 0usize;
    for t in start..end {
        if let Some(key) = segs.txkey(t) {
            if !key.is_coinbase() && (from..to).contains(&key.height()) {
                if let Some(r) = segs.tx(t)? {
                    add(&mut p, key, &r, &snap, &mut rep);
                    rep.frozen_txs += 1;
                }
            }
        }
        n += 1;
        if n.is_multiple_of(chunk.max(1)) {
            p.flush(hot, dict, coin, Some(cursor(from, t + 1)), &mut rep)?;
            if n.is_multiple_of(chunk.max(1) * 20) {
                tracing::info!(
                    txnum = t,
                    end,
                    frozen_txs = rep.frozen_txs,
                    candidates = rep.candidates,
                    dirty = rep.dirty_inserted,
                    "balances: receivers pass progress"
                );
            }
            if stop() {
                rep.stopped = true;
                rep.secs = t0.elapsed().as_secs_f64();
                return Ok(rep);
            }
        }
    }
    p.flush(hot, dict, coin, None, &mut rep)?;

    let mut txn = hot.rw()?;
    txn.set_meta(meta::BALANCES_SEED_RECEIVERS, &to.to_be_bytes())?;
    txn.del(Table::Meta, meta::BALANCES_SEED_RECEIVERS_CURSOR.as_bytes())?;
    txn.commit()?;
    rep.secs = t0.elapsed().as_secs_f64();
    Ok(rep)
}
