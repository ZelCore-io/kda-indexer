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
//!
//! D3 ([`Rules::D3`], its own flag `balances_seed_token_receivers`, cursor and error count):
//! the same walk over **all** stored heights (no coin v3 cap), with the rules of
//! `kdx_decode::receivers::token_receivers`: cross-chain defpacts of any module (token
//! finishes and starts emit no TRANSFER), pre-event execs for coin and the tokens their code
//! calls (incl. receivers only in `result.data`), non-TRANSFER events of tracked tokens and
//! `fungible-v2` module references (which also discover the referenced token, e.g.
//! `kaddex.skdx`), capped per tx and per pass. A coinbase below the injection fix whose
//! block's miner account holds a `"` gives the accounts it really credited, on its chain. Rows of a module whose name fails the `/local` module check (B-1; e.g. a non-ASCII
//! name Pact 5 cannot parse) are never marked or noted. As the tail grows the pass is
//! pending again and walks only the new heights (hourly in the service).

use std::collections::{BTreeSet, HashMap, HashSet};
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
    /// D3: modules the pass starts tracking (`Tokens`), e.g. found by a `fungible-v2` modref.
    pub tokens_noted: u64,
    /// D3: coinbases below the injection fix whose miner account holds a `"`.
    pub injected_miners: u64,
    /// D3: capped candidates taken (events, token rows of execs).
    pub event_candidates: u64,
    /// D3: emitters that reached `Opts::max_event_candidates`. Non-empty: the pass is not
    /// marked done (Meta `balances_seed_token_receivers_cap_hit` names them); rerun it with a
    /// raised cap.
    pub cap_hit: Vec<String>,
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
    /// `(account, module, chain)`.
    rows: BTreeSet<(String, String, u8)>,
    /// D3: modules to note as tokens, with the lowest height seen.
    tokens: HashMap<String, u32>,
    /// Dry run: rows that would be marked, names and tokens that would be new, over the pass.
    dry_marked: HashSet<(String, String, u8)>,
    dry_new: HashSet<String>,
    dry_tokens: HashSet<String>,
    /// D3: capped candidates taken per tracked token, and by all untracked emitters.
    emitters: HashMap<String, u64>,
    untracked: u64,
}

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
        let mark = balances::Mark {
            height: 0,
            seq: txn.txn_id()?,
        }
        .encode();
        let before = d.len(DictKind::Account);
        let mut rows: Vec<[u8; 9]> = Vec::with_capacity(self.rows.len());
        let (mut skipped, mut skipped_module) = (0u64, 0u64);
        for (a, m, c) in &self.rows {
            let mid = if m == "coin" {
                coin
            } else {
                d.intern(DictKind::Module, m)
            };
            let k = bal_key(d.intern(DictKind::Account, a), mid, *c);
            // A key with a verified row is kept fresh by the tail: correct, no re-read (review
            // M2). An unverified row (its last re-read failed) is marked like an absent key.
            if module_skip(&txn, mid, coin, &k)? {
                skipped_module += 1;
            } else if verified_row(txn.get(Table::Balances, &k)?.as_deref()) {
                skipped += 1;
            } else {
                rows.push(k);
            }
        }
        let tokens: Vec<(u32, u32)> = self
            .tokens
            .drain()
            .map(|(m, h)| (d.intern(DictKind::Module, &m), h))
            .collect();
        let new = (d.len(DictKind::Account) - before) as u64;
        d.persist(&mut txn)?;
        let mut inserted = 0u64;
        for k in &rows {
            if balances::seed_mark(&mut txn, k, &mark)? {
                inserted += 1;
            }
        }
        for (m, h) in tokens {
            if m != coin && balances::token_row(&txn, m)?.is_none() {
                rep.tokens_noted += 1;
            }
            if m != coin {
                balances::note_token(&mut txn, m, h)?;
            }
        }
        if let Some(c) = cursor {
            txn.set_meta(cursor_key, &c)?;
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
        for (a, m, c) in std::mem::take(&mut self.rows) {
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
                    } else if verified_row(txn.get(Table::Balances, &k)?.as_deref()) {
                        rep.skipped_existing += 1;
                        false
                    } else {
                        !balances::is_dirty(&txn, id, mid, c)?
                    }
                }
                _ => true,
            };
            rep.candidates += 1;
            if would && self.dry_marked.insert((a, m, c)) {
                rep.dirty_inserted += 1;
            }
        }
        for (m, _) in std::mem::take(&mut self.tokens) {
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

/// A `Balances` row the pass may leave to the tail: it decodes and is not `unverified`.
fn verified_row(v: Option<&[u8]>) -> bool {
    v.and_then(balances::BalanceRow::decode)
        .is_some_and(|r| !r.unverified)
}

/// What the walk needs per tx: the rules, the forks and (D3) the tracked tokens.
struct Walk {
    rules: Rules,
    forks: Forks,
    tokens: HashSet<String>,
    max_event_candidates: u64,
    max_untracked_candidates: u64,
    max_total_candidates: u64,
}

/// A module whose rows the pass may mark: coin, or a name `/local` can call (B-1).
fn readable(m: &str) -> bool {
    m == "coin" || kdx_node::local::valid_module(m)
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
                for m in t.discovered.into_iter().filter(|m| readable(m)) {
                    let e = p.tokens.entry(m).or_insert(k.height());
                    *e = (*e).min(k.height());
                }
                // Security review: budgets per tracked token, one shared bucket for untracked
                // emitters, a global ceiling; events first, then exec rows.
                let mut v = t.pairs;
                let capped = t.event_pairs.into_iter().zip(t.event_emitters);
                for (x, em) in capped.chain(t.exec_pairs.into_iter().zip(t.exec_emitters)) {
                    if take(p, w, rep, &em) {
                        v.push(x);
                    }
                }
                v
            })
        }
    };
    match res {
        Ok(v) => p.rows.extend(
            v.into_iter()
                .filter(|(_, m)| readable(m))
                .map(|(a, m)| (a, m, k.chain())),
        ),
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

/// Budget one capped candidate charged to emitter `em` (final cap design):
/// - a tracked token has its own budget (`max_event_candidates`); a hit is listed in
///   `cap_hit` and keeps the pass pending (visible, rerun with a raised cap);
/// - every untracked emitter shares one bucket (`max_untracked_candidates`) under a global
///   ceiling for untracked candidates (`max_total_candidates`); a hit there drops the excess,
///   counts it (`dropped_untracked`, `dropped_ceiling`) and the pass still completes, so no
///   emitter can stall it.
fn take(p: &mut Pending, w: &Walk, rep: &mut ReceiversReport, em: &str) -> bool {
    if w.tokens.contains(em) {
        let n = p.emitters.entry(em.to_owned()).or_insert(0);
        if *n >= w.max_event_candidates {
            if !rep.cap_hit.iter().any(|x| x == em) {
                rep.cap_hit.push(em.to_owned());
            }
            return false;
        }
        *n += 1;
    } else {
        // The global ceiling bounds untracked candidates only (review round 4): a tracked
        // token is bounded by its own budget and never dropped.
        if p.untracked >= w.max_total_candidates {
            rep.dropped_ceiling += 1;
            return false;
        }
        if p.untracked >= w.max_untracked_candidates {
            rep.dropped_untracked += 1;
            return false;
        }
        p.untracked += 1;
    }
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
    chain: u8,
    dict: &MemDict,
    rep: &mut ReceiversReport,
) {
    let Some(name) = dict.name(DictKind::Account, br.miner_account) else {
        return;
    };
    let v = injected_miner_receivers(name);
    if !v.is_empty() {
        rep.injected_miners += 1;
        p.rows
            .extend(v.into_iter().map(|a| (a, "coin".to_owned(), chain)));
    }
}

/// Counters of the frozen walk that a resume adds back (the hot part is redone).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Carry {
    errors: u64,
    dropped_untracked: u64,
    dropped_ceiling: u64,
    /// Capped candidates of untracked emitters taken (shared bucket and ceiling).
    untracked: u64,
    event_candidates: u64,
}

impl Carry {
    fn now(rep: &ReceiversReport, p: &Pending) -> Carry {
        Carry {
            errors: rep.errors,
            dropped_untracked: rep.dropped_untracked,
            dropped_ceiling: rep.dropped_ceiling,
            untracked: p.untracked,
            event_candidates: rep.event_candidates,
        }
    }
    fn minus(self, o: Carry) -> Carry {
        Carry {
            errors: self.errors - o.errors,
            dropped_untracked: self.dropped_untracked - o.dropped_untracked,
            dropped_ceiling: self.dropped_ceiling - o.dropped_ceiling,
            untracked: self.untracked - o.untracked,
            event_candidates: self.event_candidates - o.event_candidates,
        }
    }
}

const CURSOR_LEN: usize = 52;

/// Resume cursor: start height ‖ next frozen TxNum ‖ the frozen walk's [`Carry`] counters
/// (errors, dropped untracked, dropped at the ceiling, untracked taken, capped taken).
fn cursor(from: u32, next: u64, c: Carry) -> [u8; CURSOR_LEN] {
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
        },
    ))
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
    /// D3: capped candidates (events, token rows of execs) per emitter module per pass
    /// (security review).
    pub max_event_candidates: u64,
    /// D3: capped candidates shared by all untracked emitters per pass.
    pub max_untracked_candidates: u64,
    /// D3: global ceiling on capped candidates per pass.
    pub max_total_candidates: u64,
}

/// Default per-emitter, per-pass cap on D3 capped candidates.
pub const MAX_EVENT_CANDIDATES: u64 = 2_000_000;
/// Default shared bucket of untracked emitters.
pub const MAX_UNTRACKED_CANDIDATES: u64 = 2_000_000;
/// Default global ceiling on D3 capped candidates.
pub const MAX_TOTAL_CANDIDATES: u64 = 20_000_000;

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
        rules: o.rules,
        ..ReceiversReport::default()
    };
    let (flag_key, cursor_key, errors_key) = o.rules.keys();
    let (coin, span, resume, prior_errors) = {
        let txn = hot.ro()?;
        let resume = txn.meta(cursor_key)?.as_deref().and_then(parse_cursor);
        let prior = txn
            .meta(errors_key)?
            .as_deref()
            .and_then(|v| Some(u64::from_be_bytes(v.try_into().ok()?)))
            .unwrap_or(0);
        let span = pending_for(&txn, o)?;
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
    let mut p = Pending::default();
    let w = Walk {
        rules: o.rules,
        forks: o.forks,
        tokens: match o.rules {
            Rules::D1 => HashSet::new(),
            Rules::D3 => tracked_tokens(&hot.ro()?, &snap)?,
        },
        max_event_candidates: o.max_event_candidates,
        max_untracked_candidates: o.max_untracked_candidates,
        max_total_candidates: o.max_total_candidates,
    };
    let miners = |key: TxKey| o.rules == Rules::D3 && key.height() < o.forks.coinbase_fix;
    let ck = (coin, cursor_key);

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
                        miner_rows(&mut p, &br, key.chain(), &snap, &mut rep);
                    }
                }
                continue;
            }
            match decode_tx(v) {
                Ok(r) => add(&mut p, key, &r, &snap, &w, &mut rep),
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
    p.flush(hot, dict, ck, None, &mut rep)?;
    // The hot part is redone on resume; the frozen walk's counters resume with the cursor.
    let hot_part = Carry::now(&rep, &p);

    // Segment pass, TxNum order, resuming at the cursor of a pass with the same start.
    let segs = Segments::open_without_filters(segdir, snap.clone())?;
    let start = match segs.last_txnum_below_height(from) {
        Some(t) => t + 1,
        None => segs.manifest.entries.first().map_or(0, |e| e.first_txnum),
    };
    let start = match resume {
        Some((f, next, c)) if f == from && !o.dry_run => {
            rep.errors += c.errors;
            rep.dropped_untracked += c.dropped_untracked;
            rep.dropped_ceiling += c.dropped_ceiling;
            rep.event_candidates += c.event_candidates;
            p.untracked += c.untracked;
            // A tracked token's cap hit before the stop keeps the pass pending.
            if let Some(why) = hot
                .ro()?
                .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS_CAP_HIT)?
            {
                let v: serde_json::Value = serde_json::from_slice(&why).unwrap_or_default();
                for e in v["emitters"].as_array().into_iter().flatten() {
                    if let Some(e) = e.as_str().filter(|e| !rep.cap_hit.iter().any(|x| x == e)) {
                        rep.cap_hit.push(e.to_owned());
                    }
                }
            }
            start.max(next)
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
                    miner_rows(&mut p, &br, key.chain(), &snap, &mut rep);
                }
            } else if !key.is_coinbase() && (from..to).contains(&key.height()) {
                match segs.tx(t)? {
                    Some(r) => {
                        add(&mut p, key, &r, &snap, &w, &mut rep);
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
            let c = cursor(from, t + 1, Carry::now(&rep, &p).minus(hot_part));
            p.flush(hot, dict, ck, Some(c), &mut rep)?;
            if !o.dry_run && !rep.cap_hit.is_empty() {
                // Kept across a stop, so a resumed pass is not marked done by mistake.
                let why =
                    serde_json::json!({"cap": o.max_event_candidates, "emitters": rep.cap_hit});
                let mut txn = hot.rw()?;
                txn.set_meta(
                    meta::BALANCES_SEED_TOKEN_RECEIVERS_CAP_HIT,
                    why.to_string().as_bytes(),
                )?;
                txn.commit()?;
            }
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
    p.flush(hot, dict, ck, None, &mut rep)?;
    rep.secs = t0.elapsed().as_secs_f64();
    if !rep.cap_hit.is_empty() {
        tracing::error!(
            cap = o.max_event_candidates,
            emitters = ?rep.cap_hit,
            "balances: D3 candidates of some emitters reached the per-pass cap; the pass stays \
             pending (rerun `reconcile-balances --seed` with KDX_D3_MAX_EVENT_CANDIDATES raised)"
        );
    }
    if rep.errors > 0 {
        tracing::error!(
            errors = rep.errors,
            "balances: receivers pass could not read some txs; their receivers may be \
             missing (see the errors logged above)"
        );
    }
    if o.dry_run {
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
            "untracked": rep.dropped_untracked, "ceiling": rep.dropped_ceiling});
        txn.set_meta(
            meta::BALANCES_SEED_TOKEN_RECEIVERS_DROPPED,
            dropped.to_string().as_bytes(),
        )?;
        if rep.cap_hit.is_empty() {
            txn.del(
                Table::Meta,
                meta::BALANCES_SEED_TOKEN_RECEIVERS_CAP_HIT.as_bytes(),
            )?;
        } else {
            let why = serde_json::json!({"cap": o.max_event_candidates, "emitters": rep.cap_hit});
            txn.set_meta(
                meta::BALANCES_SEED_TOKEN_RECEIVERS_CAP_HIT,
                why.to_string().as_bytes(),
            )?;
        }
    }
    if rep.cap_hit.is_empty() {
        txn.set_meta(flag_key, &to.to_be_bytes())?;
    }
    txn.set_meta(errors_key, &total.to_be_bytes())?;
    txn.del(Table::Meta, cursor_key.as_bytes())?;
    txn.commit()?;
    Ok(rep)
}
