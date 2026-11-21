//! The balance seed's receivers pass (D1): mark `(receiver, coin, chain)` dirty for the
//! accounts that received KDA with no event naming them, before coin v3
//! (`kdx_decode::receivers`). The posting-based seed ([`crate::seed`]) and the tail marks
//! never see them, so without this pass a funded row can be missing for good.
//!
//! - Scope: the stored user txs with heights in `[from, to)`, where `from` is the height the
//!   last finished pass reached (`Meta balances_seed_receivers`, 0 when absent) and `to` is
//!   [`target`]: the network's coin v3 height, or lower while some chain's stored blocks end
//!   below it. Fork heights are per network ([`forks_for`]).
//!   On mainnet the whole history below the cutoff is frozen before the tail starts, so the
//!   pass runs once per store; a store whose tail is still below the cutoff runs it again
//!   later for the new heights only.
//! - Order: hot `TailTxs` first (no cursor: small, redone on resume), then the segments in
//!   TxNum order (opened after the hot pass, so a block the freezer moves meanwhile is seen
//!   in one of the two).
//! - Writes: names are interned in the shared dictionary (lock order: dictionary, then the
//!   MDBX writer; an account no posting named gets its first id here) and marked with
//!   `seed_mark` (height 0, never overwrites a tail mark), every [`TX_CHUNK`] txs with the
//!   resume cursor `Meta balances_seed_receivers_cursor` (start height, next TxNum, errors so
//!   far). A key that already has a `Balances` row is not marked: the tail keeps it fresh, so
//!   it is correct (review M2). The end sets `balances_seed_receivers` to `to`, stores the
//!   error count (`balances_seed_receivers_errors`, shown by `--status`) and removes the
//!   cursor. Idempotent: a rerun marks the same rows.
//! - Refused (review L3) while the store has no tail yet or its backfill is incomplete (MDBX
//!   frontier ≠ segment manifest frontier): a backfill resume truncates names interned after
//!   its frontier, and the pass's `BalanceSeed` keys would then point at reused ids.
//! - `dry_run` ([`Opts`]): reads only, and reports `candidates`, `new_accounts`,
//!   `skipped_existing` and the would-be-marked `dirty_inserted`.

use std::collections::{BTreeSet, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use kdx_decode::receivers::{network_forks, record_receivers, Forks, MAINNET01};
use kdx_hot::balances::{self, bal_key, meta};
use kdx_hot::{checkpoint, HotDict, HotStore, Stage, Table, Txn};
use kdx_segment::{Manifest, Segments};
use kdx_types::record::decode_tx;
use kdx_types::{Dict, DictKind, DictMut, MemDict, TxKey};
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
    /// Dirty rows inserted (rows already dirty are not counted); a dry run: would insert.
    pub dirty_inserted: u64,
    /// Candidates not marked because their key already has a `Balances` row (per write txn,
    /// like `candidates`).
    pub skipped_existing: u64,
    /// Records whose cmd or continuation could not be read (logged with block, chain and
    /// request key), over the pass and its resumes. Must be 0; also in Meta for `--status`.
    pub errors: u64,
    /// No `coin` module in the dictionary: nothing can be marked, the pass stays pending.
    pub no_coin: bool,
    pub stopped: bool,
    /// Nothing was written (`--dry-run`).
    pub dry_run: bool,
    pub secs: f64,
}

/// The height below which every chain's blocks are stored (frozen, or hot up to the chain's
/// tail checkpoint), capped at [`COIN_V3_HEIGHT`]. Chains without a checkpoint are ignored.
pub fn target(txn: &impl Txn, forks: Forks) -> Result<u32> {
    let f = txn.frozen_frontier()?;
    let mut low: Option<u32> = None;
    for c in 0..kdx_segment::SLOT_WIDTH as u8 {
        if let Some((h, _)) = checkpoint(txn, Stage::Tail, c)? {
            let top = f.max(h.saturating_add(1));
            low = Some(low.map_or(top, |l| l.min(top)));
        }
    }
    Ok(low.unwrap_or(f).min(forks.coin_v3))
}

/// `Some((from, to))` when the pass has work: it never finished on this store, or the stored
/// history below the cutoff grew since it did.
pub fn pending(txn: &impl Txn, forks: Forks) -> Result<Option<(u32, u32)>> {
    let done = txn
        .meta(meta::BALANCES_SEED_RECEIVERS)?
        .as_deref()
        .and_then(be_u32);
    let to = target(txn, forks)?;
    let from = done.unwrap_or(0);
    Ok((done.is_none() || to > from).then_some((from, to.max(from))))
}

fn be_u32(v: &[u8]) -> Option<u32> {
    Some(u32::from_be_bytes(v.try_into().ok()?))
}

/// The fork heights of `network`; mainnet01's with a warning when this build has none
/// (review L2). The fallback scans more txs on a network whose forks came earlier, nothing
/// is missed.
pub fn forks_for(network: &str) -> Forks {
    network_forks(network).unwrap_or_else(|| {
        tracing::warn!(
            network,
            "balances: no events/coin v3 fork heights for this network; the receivers pass \
             (D1) uses mainnet01's"
        );
        MAINNET01
    })
}

/// Refuse the pass (review L3) while the store has no tail yet, or its backfill stopped
/// between a range's MDBX commit and its manifest: the next backfill resume truncates the
/// names interned after the manifest frontier, the pass's included.
pub fn check_ready(txn: &impl Txn, segdir: &Path) -> Result<()> {
    let mf = Manifest::load(segdir)?.map_or(0, |m| m.frontier);
    let f = txn.frozen_frontier()?;
    if f != mf {
        bail!(
            "balances receivers pass refused: the backfill is incomplete (hot frozen \
             frontier {f}, segment manifest frontier {mf}); let `backfill`/`run` finish first"
        );
    }
    let mut tail = false;
    for c in 0..kdx_segment::SLOT_WIDTH as u8 {
        tail |= checkpoint(txn, Stage::Tail, c)?.is_some();
    }
    if !tail {
        bail!(
            "balances receivers pass refused: the store has no tail yet (backfill not \
             finished into follow); let `run` reach the tail first"
        );
    }
    Ok(())
}

/// Pending marks of one write txn, plus a dry run's pass-wide sets.
#[derive(Default)]
struct Pending {
    rows: BTreeSet<(String, u8)>,
    /// Dry run: pairs that would be marked and names that would be new, over the pass.
    dry_marked: HashSet<(String, u8)>,
    dry_new: HashSet<String>,
}

impl Pending {
    fn flush(
        &mut self,
        hot: &HotStore,
        dict: &Mutex<HotDict>,
        coin: u32,
        cursor: Option<[u8; 20]>,
        rep: &mut ReceiversReport,
    ) -> Result<()> {
        let mut d = dict.lock().unwrap_or_else(|e| e.into_inner());
        if rep.dry_run {
            return self.count(hot, &d, coin, rep);
        }
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
        cursor: Option<[u8; 20]>,
        rep: &mut ReceiversReport,
    ) -> Result<()> {
        let mut txn = hot.rw()?;
        let mark = balances::Mark {
            height: 0,
            seq: txn.txn_id()?,
        }
        .encode();
        let before = d.len(DictKind::Account);
        let mut rows: Vec<[u8; 9]> = Vec::with_capacity(self.rows.len());
        let mut skipped = 0u64;
        for (a, c) in &self.rows {
            let k = bal_key(d.intern(DictKind::Account, a), coin, *c);
            // A key with a verified row is kept fresh by the tail: correct, no re-read (review
            // M2). An unverified row (its last re-read failed) is marked like an absent key.
            if verified_row(txn.get(Table::Balances, &k)?.as_deref()) {
                skipped += 1;
            } else {
                rows.push(k);
            }
        }
        let new = (d.len(DictKind::Account) - before) as u64;
        d.persist(&mut txn)?;
        let mut inserted = 0u64;
        for k in &rows {
            if balances::seed_mark(&mut txn, k, &mark)? {
                inserted += 1;
            }
        }
        if let Some(c) = cursor {
            txn.set_meta(meta::BALANCES_SEED_RECEIVERS_CURSOR, &c)?;
        }
        txn.commit()?;
        rep.new_accounts += new;
        rep.skipped_existing += skipped;
        rep.dirty_inserted += inserted;
        rep.candidates += self.rows.len() as u64;
        self.rows.clear();
        Ok(())
    }

    /// Dry run: what [`Pending::write`] would do, written nowhere.
    fn count(
        &mut self,
        hot: &HotStore,
        d: &HotDict,
        coin: u32,
        rep: &mut ReceiversReport,
    ) -> Result<()> {
        let txn = hot.ro()?;
        for (a, c) in std::mem::take(&mut self.rows) {
            let would = match d.find(DictKind::Account, &a) {
                None => {
                    if self.dry_new.insert(a.clone()) {
                        rep.new_accounts += 1;
                    }
                    true
                }
                Some(id)
                    if verified_row(
                        txn.get(Table::Balances, &bal_key(id, coin, c))?.as_deref(),
                    ) =>
                {
                    rep.skipped_existing += 1;
                    false
                }
                Some(id) => !balances::is_dirty(&txn, id, coin, c)?,
            };
            rep.candidates += 1;
            if would && self.dry_marked.insert((a, c)) {
                rep.dirty_inserted += 1;
            }
        }
        Ok(())
    }
}

/// A `Balances` row the pass may leave to the tail: it decodes and is not `unverified`.
fn verified_row(v: Option<&[u8]>) -> bool {
    v.and_then(balances::BalanceRow::decode)
        .is_some_and(|r| !r.unverified)
}

fn add(
    p: &mut Pending,
    k: TxKey,
    r: &kdx_types::record::TxRecord,
    dict: &MemDict,
    forks: Forks,
    rep: &mut ReceiversReport,
) {
    match record_receivers(r, dict, forks) {
        Ok(v) => p.rows.extend(v.into_iter().map(|a| (a, k.chain()))),
        Err(e) => {
            rep.errors += 1;
            tracing::error!(
                height = k.height(),
                chain = k.chain(),
                request_key = %r.request_key.map(|h| h.to_b64url()).unwrap_or_default(),
                error = %e,
                "balances: receivers of a tx not readable (D1 pass error)"
            );
        }
    }
}

/// Resume cursor: start height ‖ next frozen TxNum ‖ frozen-walk errors so far.
fn cursor(from: u32, next: u64, errors: u64) -> [u8; 20] {
    let mut v = [0u8; 20];
    v[..4].copy_from_slice(&from.to_be_bytes());
    v[4..12].copy_from_slice(&next.to_be_bytes());
    v[12..].copy_from_slice(&errors.to_be_bytes());
    v
}

fn parse_cursor(v: &[u8]) -> Option<(u32, u64, u64)> {
    let from = be_u32(v.get(..4)?)?;
    let next = u64::from_be_bytes(v.get(4..12)?.try_into().ok()?);
    // A 12-byte cursor (before the error count was kept) resumes with 0 errors.
    let errors = match v.get(12..20) {
        Some(b) => u64::from_be_bytes(b.try_into().ok()?),
        None => 0,
    };
    Some((from, next, errors))
}

/// Pass options: the network's forks, txs per write txn, and whether to write at all.
#[derive(Debug, Clone)]
pub struct Opts {
    pub forks: Forks,
    /// Txs per write txn (and between `stop` polls) in the segment walk.
    pub chunk: usize,
    /// Count only: no marks, names, cursor or flag are written.
    pub dry_run: bool,
}

impl Default for Opts {
    fn default() -> Opts {
        Opts {
            forks: MAINNET01,
            chunk: TX_CHUNK,
            dry_run: false,
        }
    }
}

/// Run (or resume) the pass when [`pending`]; otherwise return an empty report. Errors when
/// the store is not ready ([`check_ready`]). `stop` is polled after each segment chunk; a
/// stopped pass commits its chunk and cursor and returns with `stopped`.
pub fn run(
    hot: &HotStore,
    dict: &Mutex<HotDict>,
    segdir: &Path,
    o: &Opts,
    stop: &dyn Fn() -> bool,
) -> Result<ReceiversReport> {
    let t0 = Instant::now();
    let mut rep = ReceiversReport {
        dry_run: o.dry_run,
        ..ReceiversReport::default()
    };
    let (coin, span, resume, prior_errors) = {
        let txn = hot.ro()?;
        let resume = txn
            .meta(meta::BALANCES_SEED_RECEIVERS_CURSOR)?
            .as_deref()
            .and_then(parse_cursor);
        let prior = txn
            .meta(meta::BALANCES_SEED_RECEIVERS_ERRORS)?
            .as_deref()
            .and_then(|v| Some(u64::from_be_bytes(v.try_into().ok()?)))
            .unwrap_or(0);
        let span = pending(&txn, o.forks)?;
        if span.is_some() {
            check_ready(&txn, segdir)?;
        }
        (balances::coin_module(&txn)?, span, resume, prior)
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
    tracing::info!(
        from,
        to,
        dry_run = o.dry_run,
        "balances: receivers pass (D1) started"
    );
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
                Ok(r) => add(&mut p, key, &r, &snap, o.forks, &mut rep),
                Err(e) => {
                    rep.errors += 1;
                    tracing::error!(
                        height = key.height(),
                        chain = key.chain(),
                        error = %e,
                        "balances: hot tx record not readable (D1 pass error)"
                    );
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
    // The hot part is redone on resume; the frozen walk's errors resume with the cursor.
    let hot_errors = rep.errors;

    // Segment pass, TxNum order, resuming at the cursor of a pass with the same start.
    let segs = Segments::open_without_filters(segdir, snap.clone())?;
    let start = match segs.last_txnum_below_height(from) {
        Some(t) => t + 1,
        None => segs.manifest.entries.first().map_or(0, |e| e.first_txnum),
    };
    let start = match resume {
        Some((f, next, errors)) if f == from && !o.dry_run => {
            rep.errors += errors;
            start.max(next)
        }
        _ => start,
    };
    let end = segs.last_txnum_below_height(to).map_or(start, |t| t + 1);
    let chunk = o.chunk.max(1);
    let mut n = 0usize;
    for t in start..end {
        if let Some(key) = segs.txkey(t) {
            if !key.is_coinbase() && (from..to).contains(&key.height()) {
                match segs.tx(t)? {
                    Some(r) => {
                        add(&mut p, key, &r, &snap, o.forks, &mut rep);
                        rep.frozen_txs += 1;
                    }
                    None => {
                        rep.errors += 1;
                        tracing::error!(
                            height = key.height(),
                            chain = key.chain(),
                            position = key.pos(),
                            txnum = t,
                            "balances: frozen tx has a key but no record (D1 pass error)"
                        );
                    }
                }
            }
        }
        n += 1;
        if n.is_multiple_of(chunk) {
            let c = cursor(from, t + 1, rep.errors - hot_errors);
            p.flush(hot, dict, coin, Some(c), &mut rep)?;
            if n.is_multiple_of(chunk * 20) {
                tracing::info!(
                    txnum = t,
                    end,
                    frozen_txs = rep.frozen_txs,
                    candidates = rep.candidates,
                    dirty = rep.dirty_inserted,
                    errors = rep.errors,
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
    rep.secs = t0.elapsed().as_secs_f64();
    if rep.errors > 0 {
        tracing::error!(
            errors = rep.errors,
            "balances: receivers pass (D1) could not read some txs; their receivers may be \
             missing (see the errors logged above)"
        );
    }
    if o.dry_run {
        return Ok(rep);
    }

    // Errors of earlier finished passes on this store add up; a pass from 0 starts over.
    let total = if from == 0 { 0 } else { prior_errors } + rep.errors;
    let mut txn = hot.rw()?;
    txn.set_meta(meta::BALANCES_SEED_RECEIVERS, &to.to_be_bytes())?;
    txn.set_meta(meta::BALANCES_SEED_RECEIVERS_ERRORS, &total.to_be_bytes())?;
    txn.del(Table::Meta, meta::BALANCES_SEED_RECEIVERS_CURSOR.as_bytes())?;
    txn.commit()?;
    Ok(rep)
}
