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
//!   MDBX writer; an account no posting named gets its first id here). D1 rows are marked like
//!   D3 rows: through `balances::mark_row` (the postings' mark path) at the height of the
//!   latest tx naming them, every [`TX_CHUNK`] txs with the resume cursor
//!   `Meta balances_seed_receivers_cursor` (start height, next TxNum, errors so far). A key
//!   whose verified `Balances` row was read at or above that height is not marked (review H1:
//!   an older row predates the credit). The end sets `balances_seed_receivers` to `to`, stores
//!   the error count (`balances_seed_receivers_errors`, shown by `--status`) and removes the
//!   cursor. Idempotent: a rerun marks the same rows.
//! - Refused (review L3) while the store has no tail yet or its backfill is incomplete (MDBX
//!   frontier ≠ segment manifest frontier): a backfill resume truncates names interned after
//!   its frontier, and the pass's `BalanceSeed` keys would then point at reused ids.
//! - `dry_run` ([`Opts`]): reads only, and reports `candidates`, `new_accounts`,
//!   `skipped_existing` and the would-be-marked `dirty_inserted`.
//!
//! D3 ([`Rules::D3`], its own flag `balances_seed_token_receivers`, cursor and error count):
//! the same walk over **all** stored heights (no coin v3 cap), with the rules of
//! `kdx_decode::receivers::d3_filter` (the tail marks the same rows live, ruling R-P5P9-192):
//! cross-chain defpacts of any module, pre-event execs for coin and the tokens their code calls
//! (incl. receivers only in `result.data`), token execs that emitted nothing of the token,
//! non-TRANSFER events of tracked tokens and `fungible-v2` module references, capped per tx and
//! per pass (a budget of distinct rows per token; a hit drops and counts that token's excess,
//! the pass completes, review H2). A coinbase below the injection fix whose block's miner
//! account holds a `"` gives the accounts it really credited, on its chain. Discovered modules
//! are queued for the reconciler's `describe-module` check ([`check_discovered`], review M1),
//! and tokens tracked since the last pass get their history walked (review M2). As the tail
//! grows the pass is pending again and walks only the new heights (hourly in the service).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use kdx_decode::receivers::{
    injected_miner_receivers, network_forks, record_receivers, record_token_receivers, Forks,
    MAINNET01,
};
use kdx_hot::balances::{self, bal_key, meta, TokenRow, KIND_NOT_FUNGIBLE};
use kdx_hot::{checkpoint, HotDict, HotStore, Stage, Table, Txn};
use kdx_segment::{Manifest, Segments};
use kdx_types::record::decode_tx;
use kdx_types::{Dict, DictKind, DictMut, MemDict, TxKey};
use serde::Serialize;

/// Txs per write txn (and between `stop` polls) in the segment walk.
pub const TX_CHUNK: usize = 50_000;
/// Hot `TailTxs` rows per read txn.
const HOT_CHUNK: usize = 10_000;

/// Which receivers the pass marks.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Rules {
    /// Event-less coin receivers below coin v3 (D1).
    #[default]
    D1,
    /// Receivers no TRANSFER names, of coin and every token, at every height (D3).
    D3,
}

impl Rules {
    /// Meta keys: done-through height, resume cursor, error count.
    fn keys(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Rules::D1 => (
                meta::BALANCES_SEED_RECEIVERS,
                meta::BALANCES_SEED_RECEIVERS_CURSOR,
                meta::BALANCES_SEED_RECEIVERS_ERRORS,
            ),
            Rules::D3 => (
                meta::BALANCES_SEED_TOKEN_RECEIVERS,
                meta::BALANCES_SEED_TOKEN_RECEIVERS_CURSOR,
                meta::BALANCES_SEED_TOKEN_RECEIVERS_ERRORS,
            ),
        }
    }
    fn cap(self, forks: Forks) -> u32 {
        match self {
            Rules::D1 => forks.coin_v3,
            Rules::D3 => u32::MAX,
        }
    }
}

#[derive(Debug, Default, Serialize)]
pub struct ReceiversReport {
    pub rules: Rules,
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
    /// D3: candidates of a not-fungible module, or new keys of a module at its row cap.
    pub skipped_module: u64,
    /// D3: discovered modules the pass queued for their `describe-module` check (review M1;
    /// e.g. found by a `fungible-v2` modref); tracked once the node confirms them.
    pub tokens_noted: u64,
    /// D3: coinbases below the injection fix whose miner account holds a `"`.
    pub injected_miners: u64,
    /// D3: distinct capped candidates taken (events, token rows of execs).
    pub event_candidates: u64,
    /// D3: capped candidates dropped per tracked token at its per-pass budget of distinct rows
    /// (`Opts::max_event_candidates`, review H2); counted per take (a repeat of a dropped row
    /// counts again: a distinct count would need memory the chain data controls). The pass completes regardless; Meta
    /// `balances_seed_token_receivers_dropped` keeps them for `--status`.
    pub dropped_tokens: BTreeMap<String, u64>,
    /// D3: discovered modules not queued because the pass reached
    /// [`MAX_DISCOVERED_PER_PASS`] (review M1).
    pub discovered_dropped: u64,
    /// D3: discovered modules not queued because their namespace reached
    /// [`MAX_DISCOVERED_PER_NAMESPACE`] in this pass (security review).
    pub discovered_namespace_dropped: u64,
    /// D3: pending discoveries (dropped earlier at a quota) re-queued at the start.
    pub discovered_requeued: u64,
    /// D3: exec-derived rows dropped at a sender's budget, and at the exec ceiling.
    pub dropped_exec_sender: u64,
    pub dropped_exec_ceiling: u64,
    /// D3: newly tracked tokens whose history this run walked (review M2); a dry run: would
    /// walk.
    pub rewalk_tokens: u64,
    /// D3: the bound on tracked-token candidates of this pass: tracked tokens × the
    /// per-token budget (review round 5: tracked candidates have no global ceiling; their
    /// total is bounded by this product, and a token at its budget keeps the pass pending).
    pub tracked_bound: u64,
    /// D3: capped candidates of untracked emitters dropped at the shared bucket's limit.
    pub dropped_untracked: u64,
    /// D3: capped candidates dropped at the global ceiling.
    pub dropped_ceiling: u64,
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
    target_capped(txn, forks.coin_v3)
}

fn target_capped(txn: &impl Txn, cap: u32) -> Result<u32> {
    let f = txn.frozen_frontier()?;
    let mut low: Option<u32> = None;
    for c in 0..kdx_segment::SLOT_WIDTH as u8 {
        if let Some((h, _)) = checkpoint(txn, Stage::Tail, c)? {
            let top = f.max(h.saturating_add(1));
            low = Some(low.map_or(top, |l| l.min(top)));
        }
    }
    Ok(low.unwrap_or(f).min(cap))
}

/// `Some((from, to))` when the pass has work: it never finished on this store, or the stored
/// history below the cutoff grew since it did.
pub fn pending(txn: &impl Txn, forks: Forks) -> Result<Option<(u32, u32)>> {
    pending_for(
        txn,
        &Opts {
            forks,
            ..Opts::default()
        },
    )
}

/// [`pending`] for the pass `o` describes (its rules' flag and height cap).
pub fn pending_for(txn: &impl Txn, o: &Opts) -> Result<Option<(u32, u32)>> {
    let done = txn.meta(o.rules.keys().0)?.as_deref().and_then(be_u32);
    let to = target_capped(txn, o.rules.cap(o.forks))?;
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
    /// `(account, module, chain)` with the highest height of a tx that names it (review H1).
    rows: BTreeMap<(String, String, u8), u32>,
    /// D3: discovered modules to queue for their `describe-module` check, per chain, with the
    /// lowest height seen.
    tokens: HashMap<(String, u8), u32>,
    /// Dry run: rows that would be marked, names and tokens that would be new, over the pass.
    dry_marked: HashSet<(String, String, u8)>,
    dry_new: HashSet<String>,
    dry_tokens: HashSet<String>,
    /// D3: distinct capped rows taken per tracked token over the pass, and by all untracked
    /// emitters (review H2: distinct rows, not raw takes).
    emitters: HashMap<String, u64>,
    untracked: u64,
    /// D3: hashes of the capped rows taken so far in this pass (a repeat is free).
    seen: HashSet<u64>,
    /// D3: modules queued by this pass so far (the per-pass discovery cap), and per namespace
    /// (security review: junk namespaces must not fill the cap).
    queued: usize,
    queued_ns: HashMap<String, usize>,
    /// D3: discoveries dropped at the per-pass quotas, kept pending for the next pass.
    dropped_disc: HashMap<(String, u8), u32>,
    /// D3: exec-derived rows taken per sender, and in all (security review: charged to the
    /// sender, never to the called token).
    exec_senders: HashMap<String, u64>,
    exec_total: u64,
    /// JSON for `BALANCES_SEED_TOKEN_RECEIVERS_BUDGET`, written with the next cursor.
    budget: Option<Vec<u8>>,
}

/// Modules the pass may queue for their `describe-module` check per pass (review M1).
pub const MAX_DISCOVERED_PER_PASS: usize = 8192;
/// Of those, per namespace (`ns` of `ns.module`; root modules share one), so a flood of junk
/// namespaces cannot crowd out a real token (security review).
pub const MAX_DISCOVERED_PER_NAMESPACE: usize = 4;

/// Why a module's candidate is not marked (D3): a not-fungible verdict, or the row cap (I-7)
/// for a key without a row. `None`: mark it.
fn module_skip(txn: &impl Txn, module: u32, coin: u32, key: &[u8; 9]) -> Result<bool> {
    if module == coin {
        return Ok(false);
    }
    Ok(match balances::token_row(txn, module)? {
        Some(t) if t.kind == KIND_NOT_FUNGIBLE => true,
        Some(t) => t.capped() && txn.get(Table::Balances, key)?.is_none(),
        None => false,
    })
}

/// A verified `Balances` row read at or above `height`: it already reflects the credit, so it
/// needs no re-read (review H1). Older or unverified rows are marked like absent keys.
fn fresh_row(v: Option<&[u8]>, height: u32) -> bool {
    v.and_then(balances::BalanceRow::decode)
        .is_some_and(|r| !r.unverified && r.as_of >= height)
}

impl Pending {
    fn flush(
        &mut self,
        hot: &HotStore,
        dict: &Mutex<HotDict>,
        (coin, keys): (u32, &str),
        cursor: Option<[u8; CURSOR_LEN]>,
        rep: &mut ReceiversReport,
    ) -> Result<()> {
        let mut d = dict.lock().unwrap_or_else(|e| e.into_inner());
        if rep.dry_run {
            return self.count(hot, &d, coin, rep);
        }
        let r = self.write(hot, &mut d, (coin, keys), cursor, rep);
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
        (coin, cursor_key): (u32, &str),
        cursor: Option<[u8; CURSOR_LEN]>,
        rep: &mut ReceiversReport,
    ) -> Result<()> {
        let mut txn = hot.rw()?;
        let before = d.len(DictKind::Account);
        let mut rows: Vec<([u8; 9], u32)> = Vec::with_capacity(self.rows.len());
        let (mut skipped, mut skipped_module) = (0u64, 0u64);
        for ((a, m, c), h) in &self.rows {
            let mid = if m == "coin" {
                coin
            } else {
                d.intern(DictKind::Module, m)
            };
            let k = bal_key(d.intern(DictKind::Account, a), mid, *c);
            if module_skip(&txn, mid, coin, &k)? {
                skipped_module += 1;
            } else if fresh_row(txn.get(Table::Balances, &k)?.as_deref(), *h) {
                skipped += 1;
            } else {
                rows.push((k, *h));
            }
        }
        let new = (d.len(DictKind::Account) - before) as u64;
        d.persist(&mut txn)?;
        let mut inserted = 0u64;
        for (k, h) in &rows {
            // The postings' mark path, at the credit's height (review H1).
            let (a, m, c) = balances::parse_bal_key(k).context("bal key")?;
            let new = txn.get(Table::BalanceDirty, k)?.is_none();
            if balances::mark_row(&mut txn, a, m, c, *h, Some(coin))? && new {
                inserted += 1;
            }
        }
        for ((m, c), h) in self.tokens.drain() {
            let tracked = d.find(DictKind::Module, &m).is_some_and(|id| {
                id == coin || matches!(balances::token_row(&txn, id), Ok(Some(_)))
            });
            if !tracked && balances::queue_discovered(&mut txn, &m, c, h)? {
                rep.tokens_noted += 1;
            }
        }
        for ((m, c), h) in self.dropped_disc.drain() {
            balances::pend_discovered(&mut txn, &m, c, h)?;
        }
        if let Some(c) = cursor {
            txn.set_meta(cursor_key, &c)?;
            if let Some(b) = self.budget.take() {
                txn.set_meta(meta::BALANCES_SEED_TOKEN_RECEIVERS_BUDGET, &b)?;
            }
        }
        txn.commit()?;
        rep.new_accounts += new;
        rep.skipped_existing += skipped;
        rep.skipped_module += skipped_module;
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
        for ((a, m, c), h) in std::mem::take(&mut self.rows) {
            let mid = if m == "coin" {
                Some(coin)
            } else {
                d.find(DictKind::Module, &m)
            };
            let aid = d.find(DictKind::Account, &a);
            if aid.is_none() && self.dry_new.insert(a.clone()) {
                rep.new_accounts += 1;
            }
            let would = match (aid, mid) {
                (Some(id), Some(mid)) => {
                    let k = bal_key(id, mid, c);
                    if module_skip(&txn, mid, coin, &k)? {
                        rep.skipped_module += 1;
                        false
                    } else if fresh_row(txn.get(Table::Balances, &k)?.as_deref(), h) {
                        rep.skipped_existing += 1;
                        false
                    } else {
                        txn.get(Table::BalanceDirty, &k)?.is_none()
                    }
                }
                _ => true,
            };
            rep.candidates += 1;
            if would && self.dry_marked.insert((a, m, c)) {
                rep.dirty_inserted += 1;
            }
        }
        self.dropped_disc.clear();
        for ((m, _), _) in std::mem::take(&mut self.tokens) {
            let tracked = match d.find(DictKind::Module, &m) {
                Some(id) => id == coin || balances::token_row(&txn, id)?.is_some(),
                None => false,
            };
            if !tracked && self.dry_tokens.insert(m) {
                rep.tokens_noted += 1;
            }
        }
        Ok(())
    }
}

/// What the walk needs per tx: the rules, the forks and (D3) the tracked tokens.
struct Walk {
    rules: Rules,
    forks: Forks,
    tokens: HashSet<String>,
    /// A history walk for newly tracked tokens (review M2): only rows of these modules, no
    /// injected miners, no discovery.
    only: Option<HashSet<String>>,
    max_event_candidates: u64,
    max_untracked_candidates: u64,
    max_total_candidates: u64,
    max_exec_per_sender: u64,
    max_exec_candidates: u64,
}

/// A module whose rows the pass may mark: coin, or a name `/local` can call (B-1).
fn readable(m: &str) -> bool {
    kdx_decode::receivers::readable_module(m)
}

fn row_hash(a: &str, m: &str, chain: u8) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    (a, m, chain).hash(&mut h);
    h.finish()
}

fn add(
    p: &mut Pending,
    k: TxKey,
    r: &kdx_types::record::TxRecord,
    dict: &MemDict,
    w: &Walk,
    rep: &mut ReceiversReport,
) {
    let res = match w.rules {
        Rules::D1 => record_receivers(r, dict, w.forks)
            .map(|v| v.into_iter().map(|a| (a, "coin".to_owned())).collect()),
        Rules::D3 => {
            let is_token = |m: &str| w.tokens.contains(m);
            record_token_receivers(r, dict, w.forks, &is_token).map(|t| {
                if w.only.is_none() {
                    for m in t.discovered.iter().filter(|m| !w.tokens.contains(*m)) {
                        let key = (m.clone(), k.chain());
                        let ns = balances::discover_namespace(m).to_owned();
                        if let Some(e) = p.tokens.get_mut(&key) {
                            *e = (*e).min(k.height());
                        } else if p.queued_ns.get(&ns).copied().unwrap_or(0)
                            >= MAX_DISCOVERED_PER_NAMESPACE
                        {
                            rep.discovered_namespace_dropped += 1;
                            p.dropped_disc.entry(key).or_insert(k.height());
                        } else if p.queued < MAX_DISCOVERED_PER_PASS {
                            p.queued += 1;
                            *p.queued_ns.entry(ns).or_insert(0) += 1;
                            p.tokens.insert(key, k.height());
                        } else {
                            rep.discovered_dropped += 1;
                            p.dropped_disc.entry(key).or_insert(k.height());
                        }
                    }
                }
                let keep = |m: &str| w.only.as_ref().is_none_or(|o| o.contains(m));
                // Security review: budgets per tracked token, one shared bucket for untracked
                // emitters, a global ceiling; events first, then exec rows.
                let mut v: Vec<(String, String)> =
                    t.pairs.into_iter().filter(|(_, m)| keep(m)).collect();
                let capped = t.event_pairs.into_iter().zip(t.event_emitters);
                for (x, em) in capped.chain(t.exec_pairs.into_iter().zip(t.exec_emitters)) {
                    if keep(&x.1) && take(p, w, rep, &em, row_hash(&x.0, &x.1, k.chain())) {
                        v.push(x);
                    }
                }
                v
            })
        }
    };
    match res {
        Ok(v) => {
            for (a, m) in v.into_iter().filter(|(_, m)| readable(m)) {
                let e = p.rows.entry((a, m, k.chain())).or_insert(0);
                *e = (*e).max(k.height());
            }
        }
        Err(e) => {
            rep.errors += 1;
            tracing::error!(
                height = k.height(),
                chain = k.chain(),
                request_key = %r.request_key.map(|h| h.to_b64url()).unwrap_or_default(),
                rules = ?w.rules,
                error = %e,
                "balances: receivers of a tx not readable (receivers pass error)"
            );
        }
    }
}

/// Budget one capped candidate (row hash `row`) charged to emitter `em` (review H2):
/// - a row this pass already took is free (repeating strings costs nothing);
/// - an exec-derived row (`exec:<sender>`) is charged to its tx's sender
///   (`max_exec_per_sender`) under one exec ceiling (`max_exec_candidates`), never to the
///   called token; drops are counted (`dropped_exec_sender`, `dropped_exec_ceiling`);
/// - a tracked token has its own budget of distinct rows (`max_event_candidates`); over it the
///   row is dropped and counted for that token only (`dropped_tokens`), the pass completes;
/// - every untracked emitter shares one bucket (`max_untracked_candidates`) under a global
///   ceiling for untracked candidates (`max_total_candidates`); a hit there drops the excess,
///   counts it (`dropped_untracked`, `dropped_ceiling`) and the pass still completes.
fn take(p: &mut Pending, w: &Walk, rep: &mut ReceiversReport, em: &str, row: u64) -> bool {
    if p.seen.contains(&row) {
        return true;
    }
    if let Some(sender) = em.strip_prefix(kdx_decode::receivers::EXEC_BUCKET) {
        // Exec-derived rows: per sender, under one exec ceiling (security review).
        if p.exec_total >= w.max_exec_candidates {
            rep.dropped_exec_ceiling += 1;
            return false;
        }
        let n = p.exec_senders.entry(sender.to_owned()).or_insert(0);
        if *n >= w.max_exec_per_sender {
            rep.dropped_exec_sender += 1;
            return false;
        }
        *n += 1;
        p.exec_total += 1;
    } else if w.tokens.contains(em) {
        let n = p.emitters.entry(em.to_owned()).or_insert(0);
        if *n >= w.max_event_candidates {
            *rep.dropped_tokens.entry(em.to_owned()).or_insert(0) += 1;
            return false;
        }
        *n += 1;
    } else {
        // One check: the shared bucket and the global ceiling (review round 4: the ceiling
        // bounds untracked candidates only) count the same thing; the lower one binds and
        // names the drop.
        let limit = w.max_untracked_candidates.min(w.max_total_candidates);
        if p.untracked >= limit {
            if w.max_total_candidates < w.max_untracked_candidates {
                rep.dropped_ceiling += 1;
            } else {
                rep.dropped_untracked += 1;
            }
            return false;
        }
        p.untracked += 1;
    }
    p.seen.insert(row);
    rep.event_candidates += 1;
    true
}

/// D3: the tracked fungible tokens (`Tokens`, not-fungible verdicts excluded) by name.
fn tracked_tokens(txn: &impl Txn, dict: &MemDict) -> Result<HashSet<String>> {
    let mut out = HashSet::new();
    for (k, v) in txn.scan(Table::Tokens, &[], |_| true)? {
        let (Ok(id), Some(t)) = (<[u8; 4]>::try_from(k.as_slice()), TokenRow::decode(&v)) else {
            continue;
        };
        if t.kind == KIND_NOT_FUNGIBLE {
            continue;
        }
        if let Some(n) = dict.name(DictKind::Module, u32::from_be_bytes(id)) {
            out.insert(n.to_owned());
        }
    }
    Ok(out)
}

/// D3 (a): `(account, coin, chain)` for the accounts an injected miner account of a block
/// below the coinbase injection fix really credited, on that block's chain only.
fn miner_rows(
    p: &mut Pending,
    br: &kdx_types::record::BlockRecord,
    key: TxKey,
    dict: &MemDict,
    rep: &mut ReceiversReport,
) {
    let Some(name) = dict.name(DictKind::Account, br.miner_account) else {
        return;
    };
    let v = injected_miner_receivers(name);
    if !v.is_empty() {
        rep.injected_miners += 1;
        for a in v {
            let e = p
                .rows
                .entry((a, "coin".to_owned(), key.chain()))
                .or_insert(0);
            *e = (*e).max(key.height());
        }
    }
}

/// Counters of the frozen walk that a resume adds back (the hot part is redone).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Carry {
    errors: u64,
    dropped_untracked: u64,
    dropped_ceiling: u64,
    /// Capped candidates of untracked emitters taken (shared bucket and ceiling).
    untracked: u64,
    event_candidates: u64,
    /// Distinct rows taken per tracked token, and dropped per token (review L1, H2).
    taken: BTreeMap<String, u64>,
    dropped: BTreeMap<String, u64>,
    /// Exec-derived rows taken, and dropped at a sender's budget / the exec ceiling (the
    /// per-sender counts are not carried: a resume may grant a sender its budget again, the
    /// ceiling still binds).
    exec: [u64; 3],
}

fn map_minus(a: &BTreeMap<String, u64>, b: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
    a.iter()
        .map(|(k, v)| (k.clone(), v - b.get(k).copied().unwrap_or(0)))
        .filter(|(_, v)| *v > 0)
        .collect()
}

impl Carry {
    fn now(rep: &ReceiversReport, p: &Pending) -> Carry {
        Carry {
            errors: rep.errors,
            dropped_untracked: rep.dropped_untracked,
            dropped_ceiling: rep.dropped_ceiling,
            untracked: p.untracked,
            event_candidates: rep.event_candidates,
            taken: p.emitters.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            dropped: rep.dropped_tokens.clone(),
            exec: [
                p.exec_total,
                rep.dropped_exec_sender,
                rep.dropped_exec_ceiling,
            ],
        }
    }
    fn minus(&self, o: &Carry) -> Carry {
        Carry {
            errors: self.errors - o.errors,
            dropped_untracked: self.dropped_untracked - o.dropped_untracked,
            dropped_ceiling: self.dropped_ceiling - o.dropped_ceiling,
            untracked: self.untracked - o.untracked,
            event_candidates: self.event_candidates - o.event_candidates,
            taken: map_minus(&self.taken, &o.taken),
            dropped: map_minus(&self.dropped, &o.dropped),
            exec: [0, 1, 2].map(|i| self.exec[i] - o.exec[i]),
        }
    }
    /// The per-token maps, for `BALANCES_SEED_TOKEN_RECEIVERS_BUDGET` (keyed by the cursor's
    /// start height and next TxNum, so a stale one is never added).
    fn budget_json(&self, from: u32, next: u64) -> Vec<u8> {
        serde_json::json!({"from": from, "next": next, "taken": self.taken,
            "dropped": self.dropped, "exec": self.exec})
        .to_string()
        .into_bytes()
    }
}

const CURSOR_LEN: usize = 52;

/// Resume cursor: start height ‖ next frozen TxNum ‖ the frozen walk's [`Carry`] counters
/// (errors, dropped untracked, dropped at the ceiling, untracked taken, capped taken). The
/// per-token maps go to `BALANCES_SEED_TOKEN_RECEIVERS_BUDGET` in the same txn.
fn cursor(from: u32, next: u64, c: &Carry) -> [u8; CURSOR_LEN] {
    let mut v = [0u8; CURSOR_LEN];
    v[..4].copy_from_slice(&from.to_be_bytes());
    v[4..12].copy_from_slice(&next.to_be_bytes());
    let n = [
        c.errors,
        c.dropped_untracked,
        c.dropped_ceiling,
        c.untracked,
        c.event_candidates,
    ];
    for (i, x) in n.iter().enumerate() {
        v[12 + 8 * i..20 + 8 * i].copy_from_slice(&x.to_be_bytes());
    }
    v
}

fn parse_cursor(v: &[u8]) -> Option<(u32, u64, Carry)> {
    let from = be_u32(v.get(..4)?)?;
    let next = u64::from_be_bytes(v.get(4..12)?.try_into().ok()?);
    // Older, shorter cursors (12 bytes: no counters; 20: errors only) resume with zeros.
    let at = |i: usize| {
        v.get(12 + 8 * i..20 + 8 * i)
            .and_then(|b| b.try_into().ok())
            .map_or(0, u64::from_be_bytes)
    };
    Some((
        from,
        next,
        Carry {
            errors: at(0),
            dropped_untracked: at(1),
            dropped_ceiling: at(2),
            untracked: at(3),
            event_candidates: at(4),
            ..Carry::default()
        },
    ))
}

/// The per-token maps stored with the cursor `(from, next)`; empty when absent or stale.
type Budget = (BTreeMap<String, u64>, BTreeMap<String, u64>, [u64; 3]);

fn parse_budget(v: Option<&[u8]>, from: u32, next: u64) -> Budget {
    let Some(v) = v.and_then(|v| serde_json::from_slice::<serde_json::Value>(v).ok()) else {
        return Default::default();
    };
    if v["from"].as_u64() != Some(u64::from(from)) || v["next"].as_u64() != Some(next) {
        return Default::default();
    }
    let map = |x: &serde_json::Value| -> BTreeMap<String, u64> {
        x.as_object()
            .into_iter()
            .flatten()
            .filter_map(|(k, v)| Some((k.clone(), v.as_u64()?)))
            .collect()
    };
    let e = |i: usize| v["exec"][i].as_u64().unwrap_or(0);
    (map(&v["taken"]), map(&v["dropped"]), [e(0), e(1), e(2)])
}

/// Pass options: the network's forks, txs per write txn, and whether to write at all.
#[derive(Debug, Clone)]
pub struct Opts {
    pub forks: Forks,
    /// Txs per write txn (and between `stop` polls) in the segment walk.
    pub chunk: usize,
    /// Count only: no marks, names, cursor or flag are written.
    pub dry_run: bool,
    /// D1 (default) or D3.
    pub rules: Rules,
    /// D3: distinct capped rows (events, token rows of execs) per tracked token per pass
    /// (security review; review H2: over it that token's rows are dropped and counted).
    pub max_event_candidates: u64,
    /// D3: capped candidates shared by all untracked emitters per pass.
    pub max_untracked_candidates: u64,
    /// D3: global ceiling on capped candidates per pass.
    pub max_total_candidates: u64,
    /// D3: the history walk of newly tracked tokens (review M2) runs at most this often
    /// (seconds; 0 = whenever some are pending). The service passes [`REWALK_EVERY_SECS`].
    pub rewalk_every_secs: u32,
    /// D3: distinct exec-derived token rows per tx sender per pass (security review).
    pub max_exec_per_sender: u64,
    /// D3: distinct exec-derived token rows per pass, all senders.
    pub max_exec_candidates: u64,
}

/// Default per-token, per-pass budget of distinct D3 capped rows.
pub const MAX_EVENT_CANDIDATES: u64 = 2_000_000;
/// Default per-sender budget of exec-derived D3 rows.
pub const MAX_EXEC_PER_SENDER: u64 = 100_000;
/// Default ceiling on exec-derived D3 rows per pass.
pub const MAX_EXEC_CANDIDATES: u64 = 2_000_000;
/// Default shared bucket of untracked emitters.
pub const MAX_UNTRACKED_CANDIDATES: u64 = 2_000_000;
/// Default global ceiling on D3 capped candidates.
pub const MAX_TOTAL_CANDIDATES: u64 = 20_000_000;
/// The service's interval between history walks for newly tracked tokens (a walk reads all
/// stored history, about as long as the first D3 pass).
pub const REWALK_EVERY_SECS: u32 = 24 * 3600;

impl Default for Opts {
    fn default() -> Opts {
        Opts {
            forks: MAINNET01,
            chunk: TX_CHUNK,
            dry_run: false,
            rules: Rules::D1,
            max_event_candidates: MAX_EVENT_CANDIDATES,
            max_untracked_candidates: MAX_UNTRACKED_CANDIDATES,
            max_total_candidates: MAX_TOTAL_CANDIDATES,
            rewalk_every_secs: 0,
            max_exec_per_sender: MAX_EXEC_PER_SENDER,
            max_exec_candidates: MAX_EXEC_CANDIDATES,
        }
    }
}

fn now_unix() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as u32)
}

/// What one walk over `[from, to)` shares.
struct Env<'a> {
    hot: &'a HotStore,
    dict: &'a Mutex<HotDict>,
    segdir: &'a Path,
    snap: Arc<MemDict>,
    o: &'a Opts,
    ck: (u32, &'a str),
}

/// Walk the stored txs at heights `[from, to)`: hot `TailTxs`, then the segments in TxNum
/// order. `resume` (the main walk only): the cursor of this pass, `Some(None)` for a fresh
/// main walk that writes cursors, `None` for a walk that writes none (history walk). Returns
/// whether it was stopped.
fn walk(
    e: &Env,
    w: &Walk,
    p: &mut Pending,
    rep: &mut ReceiversReport,
    (from, to): (u32, u32),
    resume: Option<Option<(u64, Carry)>>,
    stop: &dyn Fn() -> bool,
) -> Result<bool> {
    let (hot, snap, o) = (e.hot, &e.snap, e.o);
    let miners = |key: TxKey| {
        o.rules == Rules::D3 && w.only.is_none() && key.height() < o.forks.coinbase_fix
    };

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
                if miners(key) {
                    let bk = kdx_hot::tables::block_key(key.chain(), key.height());
                    if let Some(v) = hot.ro()?.get(Table::Headers, &bk)? {
                        let br = kdx_types::record::decode_block(&v)?;
                        miner_rows(p, &br, key, snap, rep);
                    }
                }
                continue;
            }
            match decode_tx(v) {
                Ok(r) => add(p, key, &r, snap, w, rep),
                Err(e) => {
                    rep.errors += 1;
                    tracing::error!(
                        height = key.height(),
                        chain = key.chain(),
                        error = %e,
                        "balances: hot tx record not readable (receivers pass error)"
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
    p.flush(hot, e.dict, e.ck, None, rep)?;
    // The hot part is redone on resume; the frozen walk's counters resume with the cursor.
    let hot_part = Carry::now(rep, p);

    // Segment pass, TxNum order, resuming at the cursor of a pass with the same start.
    let segs = Segments::open_without_filters(e.segdir, snap.clone())?;
    let start = match segs.last_txnum_below_height(from) {
        Some(t) => t + 1,
        None => segs.manifest.entries.first().map_or(0, |e| e.first_txnum),
    };
    let start = match &resume {
        Some(Some((next, c))) => {
            rep.errors += c.errors;
            rep.dropped_untracked += c.dropped_untracked;
            rep.dropped_ceiling += c.dropped_ceiling;
            rep.event_candidates += c.event_candidates;
            p.untracked += c.untracked;
            for (k, v) in &c.taken {
                *p.emitters.entry(k.clone()).or_insert(0) += v;
            }
            for (k, v) in &c.dropped {
                *rep.dropped_tokens.entry(k.clone()).or_insert(0) += v;
            }
            p.exec_total += c.exec[0];
            rep.dropped_exec_sender += c.exec[1];
            rep.dropped_exec_ceiling += c.exec[2];
            start.max(*next)
        }
        _ => start,
    };
    let end = segs.last_txnum_below_height(to).map_or(start, |t| t + 1);
    let chunk = o.chunk.max(1);
    let mut n = 0usize;
    for t in start..end {
        if let Some(key) = segs.txkey(t) {
            if key.is_coinbase() && miners(key) && (from..to).contains(&key.height()) {
                if let Some(br) = segs.block(key.chain(), key.height())? {
                    miner_rows(p, &br, key, snap, rep);
                }
            } else if !key.is_coinbase() && (from..to).contains(&key.height()) {
                match segs.tx(t)? {
                    Some(r) => {
                        add(p, key, &r, snap, w, rep);
                        rep.frozen_txs += 1;
                    }
                    None => {
                        rep.errors += 1;
                        tracing::error!(
                            height = key.height(),
                            chain = key.chain(),
                            position = key.pos(),
                            txnum = t,
                            "balances: frozen tx has a key but no record (receivers pass error)"
                        );
                    }
                }
            }
        }
        n += 1;
        if n.is_multiple_of(chunk) {
            let c = resume.as_ref().map(|_| {
                let carry = Carry::now(rep, p).minus(&hot_part);
                p.budget = Some(carry.budget_json(from, t + 1));
                cursor(from, t + 1, &carry)
            });
            p.flush(hot, e.dict, e.ck, c, rep)?;
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
                return Ok(true);
            }
        }
    }
    p.flush(hot, e.dict, e.ck, None, rep)?;
    Ok(false)
}

/// Run (or resume) the pass when [`pending`]; otherwise return an empty report. Errors when
/// the store is not ready ([`check_ready`]). `stop` is polled after each segment chunk; a
/// stopped pass commits its chunk and cursor and returns with `stopped`.
///
/// D3, after the walk of `[from, to)`: the tokens tracked since the last pass (review M2,
/// [`meta::D3_REWALK`]) get their history walked over `[0, from)` (rules for those tokens
/// only), at most every [`Opts::rewalk_every_secs`]; a pass from 0 covers them already.
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
        rules: o.rules,
        ..ReceiversReport::default()
    };
    let (flag_key, cursor_key, errors_key) = o.rules.keys();
    let (coin, span, resume, budget, prior_errors) = {
        let txn = hot.ro()?;
        let resume = txn.meta(cursor_key)?.as_deref().and_then(parse_cursor);
        let budget = txn.meta(meta::BALANCES_SEED_TOKEN_RECEIVERS_BUDGET)?;
        let prior = txn
            .meta(errors_key)?
            .as_deref()
            .and_then(|v| Some(u64::from_be_bytes(v.try_into().ok()?)))
            .unwrap_or(0);
        let span = pending_for(&txn, o)?;
        if span.is_some() {
            check_ready(&txn, segdir)?;
        }
        (balances::coin_module(&txn)?, span, resume, budget, prior)
    };
    let Some((from, to)) = span else {
        return Ok(rep);
    };
    (rep.from, rep.to) = (from, to);
    let Some(coin) = coin else {
        rep.no_coin = true;
        tracing::warn!(rules = ?o.rules, "balances: no coin module yet; receivers pass postponed");
        return Ok(rep);
    };
    tracing::info!(
        from,
        to,
        dry_run = o.dry_run,
        rules = ?o.rules,
        "balances: receivers pass started"
    );
    let snap: Arc<MemDict> = Arc::new(dict.lock().unwrap_or_else(|e| e.into_inner()).mem().clone());
    let (tokens, rewalk) = match o.rules {
        Rules::D1 => (HashSet::new(), Vec::new()),
        Rules::D3 => {
            let txn = hot.ro()?;
            (
                tracked_tokens(&txn, &snap)?,
                balances::rewalk_pending(&txn)?,
            )
        }
    };
    let w = Walk {
        rules: o.rules,
        forks: o.forks,
        tokens,
        only: None,
        max_event_candidates: o.max_event_candidates,
        max_untracked_candidates: o.max_untracked_candidates,
        max_total_candidates: o.max_total_candidates,
        max_exec_per_sender: o.max_exec_per_sender,
        max_exec_candidates: o.max_exec_candidates,
    };
    rep.tracked_bound = (w.tokens.len() as u64).saturating_mul(o.max_event_candidates);
    let env = Env {
        hot,
        dict,
        segdir,
        snap: snap.clone(),
        o,
        ck: (coin, cursor_key),
    };
    // A resume continues the cursor of a pass with the same start, with its budget counters
    // (review L1). No cap-hit list is reloaded: a budget hit no longer withholds the pass
    // (review H2, M3).
    let resume = match resume {
        Some((f, next, mut c)) if f == from && !o.dry_run => {
            (c.taken, c.dropped, c.exec) = parse_budget(budget.as_deref(), from, next);
            Some((next, c))
        }
        _ => None,
    };
    if o.rules == Rules::D3 && !o.dry_run {
        // Discoveries an earlier pass or the tail dropped at a quota go first (security
        // review: nothing is lost for good).
        let mut txn = hot.rw()?;
        rep.discovered_requeued =
            balances::requeue_pending(&mut txn, MAX_DISCOVERED_PER_PASS)? as u64;
        txn.commit()?;
    }
    let mut p = Pending::default();
    if walk(&env, &w, &mut p, &mut rep, (from, to), Some(resume), stop)? {
        rep.stopped = true;
        rep.secs = t0.elapsed().as_secs_f64();
        return Ok(rep);
    }
    if rep.errors > 0 {
        tracing::error!(
            errors = rep.errors,
            "balances: receivers pass could not read some txs; their receivers may be \
             missing (see the errors logged above)"
        );
    }
    if !rep.dropped_tokens.is_empty() {
        tracing::warn!(
            budget = o.max_event_candidates,
            tokens = ?rep.dropped_tokens,
            "balances: D3 dropped rows of tokens over their per-pass budget of distinct rows \
             (the pass completes; rerun `reconcile-balances --seed` after deleting the D3 flag \
             with KDX_D3_MAX_EVENT_CANDIDATES raised if a token is genuine)"
        );
    }
    if o.dry_run {
        if o.rules == Rules::D3 && from > 0 {
            rep.rewalk_tokens = rewalk.len() as u64;
        }
        rep.secs = t0.elapsed().as_secs_f64();
        return Ok(rep);
    }

    // Errors of earlier finished passes on this store add up; a pass from 0 starts over.
    let total = if from == 0 { 0 } else { prior_errors } + rep.errors;
    let mut txn = hot.rw()?;
    if rep.dropped_untracked + rep.dropped_ceiling > 0 {
        tracing::warn!(
            dropped_untracked = rep.dropped_untracked,
            dropped_ceiling = rep.dropped_ceiling,
            "balances: D3 dropped candidates of untracked emitters / over the global ceiling"
        );
    }
    if o.rules == Rules::D3 {
        let dropped = serde_json::json!({
            "untracked": rep.dropped_untracked, "ceiling": rep.dropped_ceiling,
            "tokens": rep.dropped_tokens, "discovered": rep.discovered_dropped,
            "discovered_namespace": rep.discovered_namespace_dropped,
            "exec_sender": rep.dropped_exec_sender, "exec_ceiling": rep.dropped_exec_ceiling,
            "tracked_bound": rep.tracked_bound});
        txn.set_meta(
            meta::BALANCES_SEED_TOKEN_RECEIVERS_DROPPED,
            dropped.to_string().as_bytes(),
        )?;
        // Written by earlier builds, which withheld the pass on a budget hit (review M3).
        txn.del(
            Table::Meta,
            meta::BALANCES_SEED_TOKEN_RECEIVERS_CAP_HIT.as_bytes(),
        )?;
        txn.del(
            Table::Meta,
            meta::BALANCES_SEED_TOKEN_RECEIVERS_BUDGET.as_bytes(),
        )?;
        if from == 0 {
            // The walk from 0 covered every token tracked when it started.
            for m in &rewalk {
                balances::clear_rewalk(&mut txn, *m)?;
            }
        }
    }
    txn.set_meta(flag_key, &to.to_be_bytes())?;
    txn.set_meta(errors_key, &total.to_be_bytes())?;
    txn.del(Table::Meta, cursor_key.as_bytes())?;
    txn.commit()?;

    if o.rules == Rules::D3 && from > 0 && !rewalk.is_empty() {
        let last = hot
            .ro()?
            .meta(meta::D3_REWALK_AT)?
            .as_deref()
            .and_then(be_u32)
            .unwrap_or(0);
        let now = now_unix();
        if now >= last.saturating_add(o.rewalk_every_secs)
            && history_walk(&env, &w, &rewalk, from, &mut rep, stop)?
        {
            rep.stopped = true;
        }
    }
    rep.secs = t0.elapsed().as_secs_f64();
    Ok(rep)
}

/// Review M2: walk `[0, upto)` with the D3 rules for the newly tracked tokens `ids` only, then
/// clear them from [`meta::D3_REWALK`]. Returns whether it was stopped (they stay pending).
fn history_walk(
    env: &Env,
    main: &Walk,
    ids: &[u32],
    upto: u32,
    rep: &mut ReceiversReport,
    stop: &dyn Fn() -> bool,
) -> Result<bool> {
    let names: HashSet<String> = ids
        .iter()
        .filter_map(|id| env.snap.name(DictKind::Module, *id).map(str::to_owned))
        .filter(|m| main.tokens.contains(m))
        .collect();
    tracing::info!(tokens = ?names, upto, "balances: D3 history walk of newly tracked tokens");
    let w = Walk {
        rules: Rules::D3,
        forks: main.forks,
        tokens: main.tokens.clone(),
        only: Some(names),
        max_event_candidates: main.max_event_candidates,
        max_untracked_candidates: main.max_untracked_candidates,
        max_total_candidates: main.max_total_candidates,
        max_exec_per_sender: main.max_exec_per_sender,
        max_exec_candidates: main.max_exec_candidates,
    };
    let mut p = Pending::default();
    if walk(env, &w, &mut p, rep, (0, upto), None, stop)? {
        return Ok(true);
    }
    rep.rewalk_tokens = ids.len() as u64;
    let mut txn = env.hot.rw()?;
    for m in ids {
        balances::clear_rewalk(&mut txn, *m)?;
    }
    txn.set_meta(meta::D3_REWALK_AT, &now_unix().to_be_bytes())?;
    txn.commit()?;
    Ok(false)
}

/// Review M1: check up to `n` queued discovered modules with `check(chain, module)` (the
/// node's `describe-module` lists `fungible-v2`: `Some(true)`; not a fungible module or no
/// such module: `Some(false)`; node unreachable: `None`, kept for later). A confirmed module
/// becomes a tracked token (`note_token` at the lowest height that named it, which also queues
/// its history walk, review M2); a rejected one is dropped. Returns `(tracked, rejected)`.
pub fn check_discovered(
    hot: &HotStore,
    dict: &Mutex<HotDict>,
    queued: &[(String, u8, u32)],
    verdicts: &[Option<bool>],
) -> Result<(u64, u64)> {
    let mut d = dict.lock().unwrap_or_else(|e| e.into_inner());
    let mut txn = hot.rw()?;
    let (mut tracked, mut rejected) = (0u64, 0u64);
    for ((m, c, h), v) in queued.iter().zip(verdicts) {
        match v {
            Some(true) => {
                let id = d.intern(DictKind::Module, m);
                d.persist(&mut txn)?;
                balances::note_token(&mut txn, id, *h)?;
                balances::unqueue_discovered(&mut txn, m, *c)?;
                tracked += 1;
            }
            Some(false) => {
                balances::unqueue_discovered(&mut txn, m, *c)?;
                rejected += 1;
            }
            None => {}
        }
    }
    let r = txn.commit();
    if r.is_err() {
        *d = HotDict::load(&hot.ro()?)?;
    }
    r?;
    Ok((tracked, rejected))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_token_budget_counters_resume_with_their_cursor_only() {
        let mut c = Carry {
            errors: 1,
            ..Carry::default()
        };
        c.taken.insert("free.spam".into(), 7);
        c.dropped.insert("free.spam".into(), 3);
        let hot = Carry {
            taken: [("free.spam".to_owned(), 2)].into(),
            ..Carry::default()
        };
        let frozen = c.minus(&hot);
        assert_eq!(frozen.taken.get("free.spam"), Some(&5));
        let j = frozen.budget_json(10, 99);
        let (taken, dropped, exec) = parse_budget(Some(&j), 10, 99);
        assert_eq!(
            (taken, dropped),
            (frozen.taken.clone(), frozen.dropped.clone())
        );
        assert_eq!(exec, frozen.exec);
        // Stored with another cursor (an older pass, or a stop before the next cursor): ignored.
        assert_eq!(parse_budget(Some(&j), 10, 100), Default::default());
        assert_eq!(parse_budget(Some(&j), 0, 99), Default::default());
        assert_eq!(parse_budget(None, 10, 99), Default::default());
    }
}
