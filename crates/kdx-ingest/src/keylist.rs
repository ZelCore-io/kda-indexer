//! D3 gap groups 3 and 4: the key listing of tracked tokens ("system accounts").
//!
//! Some funded rows of a token are named in no tx: the token's own `init` writes an account
//! that is a string literal of the module code (`free.shatter` `ROOT` on 18 chains, `free.KAYC`
//! `ROOT`, `free.ba-t3` `CORE`), or the module writes a name built at run time with no event
//! (the heron fee account `u:…heron-fees.require-WALLET:…`, the `kaddex.kdx` pool account).
//! No tx-local rule can find them. The node can: `/local` allows `(keys <module>.<table>)` on
//! any module's table (measured on mainnet01, 2026-11-23, flat 40,000 gas).
//!
//! - Which table: the module's `deftable`s whose `defschema` has a `balance` field
//!   ([`ledger_tables`], from `describe-module`'s code on that chain).
//! - Which tokens: every tracked token (`Tokens` row, not a not-fungible verdict, not at its
//!   row cap, not `coin`). A token never listed comes first, so a newly tracked one (D3
//!   discovery, first posting) is listed within a refresh; then each again after
//!   [`SWEEP_SECS`].
//! - Its own task ([`run`], PR #27 H-1): the listing never runs inside the reconcile loop, so
//!   a slow node read cannot hold up dirty rows, NFTs or history slices. One step, one
//!   `(token, chain)`: one `describe-module` read, then one `keys` read per ledger table, each
//!   sent once with [`READ_TIMEOUT`] (no retry inside the client). At most one step per
//!   [`STEP_EVERY`], inside the shared `/local` rate limit. A panic in the task is logged at
//!   ERROR and the task restarted after [`PANIC_PAUSE`] (R-4). With `balances.local_concurrency`
//!   = 1 the listing is off ([`enabled`], R-3): a read would hold the only slot. Progress (`Meta
//!   balances_keylist/<module>`) is kept per step, so a restart resumes.
//! - `balances.keylist_skip`: modules never read on any chain (the first `keys` of a huge table
//!   already blocks the node's cut, so parking after a timeout is too late); `due` leaves them out.
//! - A module with no ledger table on a chain lists nothing there, like a module not deployed
//!   (no failure; looked at again at the next sweep).
//! - Failures are per `(token, chain)`: an unusable or over-cap answer or a pact failure marks
//!   nothing for that chain, is logged and counted, and only that chain is listed again, after
//!   [`RETRY_BASE_SECS`] doubling with each failure in a row up to [`RETRY_CAP_SECS`]. A `keys`
//!   timeout parks that `(token, chain)` for good ([`PARKED`]): the node does not stop the
//!   read when the client gives up, and on mainnet01 one such read held the chain's pact
//!   service (and so the node's cut, the tail and the reconcile's reads on that chain) for
//!   about 14 minutes after every 90 s timeout. A connection or HTTP error, or a `describe-module` timeout (small code: the node is
//!   stalling, not the token), pauses the whole listing ([`NODE_PAUSE`],
//!   doubling to [`NODE_PAUSE_MAX`]) and counts against the chain after [`NODE_TRIES`] in a
//!   row. A failed token goes to the back of the queue, so one token cannot hold it.
//! - Budget: a token that runs up [`FAIL_BUDGET`] chain failures (a chain already at
//!   [`RETRY_CAP_SECS`] adds none, R-2) with no clean round between
//!   is marked unlistable (logged, counted, `reconcile-balances --status`) and left alone for
//!   [`UNLISTABLE_SECS`], then tried once more with a fresh budget.
//! - Marks: only keys we hold nothing for (no `Balances` row, no dirty or seed mark) are marked,
//!   through `balances::mark_row` (the postings' mark path, so the row caps apply) at the height
//!   the node read. The normal reconcile then reads them like any dirty row.
//! - Limits: a listing over [`MAX_KEYS_PER_LISTING`] keys is refused whole. The read timeout
//!   binds first on a slow node: at the measured ~2,000 keys/s, 150,000 keys take ~75 s of the
//!   90 s; a table the node cannot list within [`READ_TIMEOUT`] fails like any other read.
//! - Refused (like the receivers pass, review L3) while the store has no tail yet or its
//!   backfill is incomplete: the listing interns account names.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use kdx_hot::balances::{self, bal_key, meta, TokenRow, KIND_NOT_FUNGIBLE};
use kdx_hot::{HotDict, HotStore, Table, Txn};
use kdx_node::local::{
    valid_ident, valid_module, Local, LocalClient, LocalError, MAX_LISTED_KEYS, MAX_NAME_LEN,
};
use kdx_node::NodeError;
use kdx_types::{Dict, DictKind, DictMut};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::Ctx;

/// Seconds after which a listed token is listed again (the periodic sweep).
pub const SWEEP_SECS: u32 = 7 * 86_400;
/// Seconds before a `(token, chain)` whose listing failed once is listed again; each further
/// failure in a row doubles it, up to [`RETRY_CAP_SECS`].
pub const RETRY_BASE_SECS: u32 = 3_600;
/// Longest pause before a failed `(token, chain)` is listed again.
pub const RETRY_CAP_SECS: u32 = SWEEP_SECS;
/// Chain failures a token may run up with no clean round between before it is unlistable.
pub const FAIL_BUDGET: u16 = 32;
/// Seconds an unlistable token is left alone before one more try with a fresh budget.
pub const UNLISTABLE_SECS: u32 = 30 * 86_400;
/// Least time between two steps (one `(token, chain)` each: 1 + tables `/local` reads).
pub const STEP_EVERY: Duration = Duration::from_secs(1);
/// How often the queue of due tokens is rebuilt from the store (newly tracked tokens join).
pub const REFRESH: Duration = Duration::from_secs(600);
/// Timeout of one listing read, sent once. Sized to the key cap: the node listed 9,480 keys
/// in 4.4 s (about 2,000 keys/s, mainnet01, 2026-11-23), so 150,000 keys take about 75 s.
pub const READ_TIMEOUT: Duration = Duration::from_secs(90);
/// Connection or HTTP errors in a row on one `(token, chain)` before it counts as failed.
pub const NODE_TRIES: u32 = 3;
/// Pause of the whole listing after a connection or HTTP error; doubles with each in a row.
pub const NODE_PAUSE: Duration = Duration::from_secs(30);
/// Longest such pause.
pub const NODE_PAUSE_MAX: Duration = Duration::from_secs(600);
/// Pause after an error of the store.
const STORE_PAUSE: Duration = Duration::from_secs(60);
/// Pause before the listing task is started again after a panic.
const PANIC_PAUSE: Duration = Duration::from_secs(60);
/// Most ledger tables listed per module and chain.
pub const MAX_TABLES: usize = 4;
/// Most keys one `(token, table, chain)` listing may answer; a bigger one is refused whole
/// (fail-closed). Lower than the node client's [`MAX_LISTED_KEYS`]: the largest non-coin
/// token on mainnet (`free.finux`, 126,246 rows in all, 2026-11-22) fits.
pub const MAX_KEYS_PER_LISTING: usize = 150_000;

/// Replace string literals by `""` and drop `;` comments, so the scan below sees only code.
/// A Pact string may span lines (`\` continuations); an unterminated one runs to the end.
fn code_only(code: &str) -> String {
    let mut out = String::with_capacity(code.len());
    let mut it = code.chars();
    while let Some(c) = it.next() {
        match c {
            '"' => {
                out.push_str("\"\"");
                while let Some(c) = it.next() {
                    match c {
                        '\\' => {
                            it.next();
                        }
                        '"' => break,
                        _ => {}
                    }
                }
            }
            ';' => {
                for c in it.by_ref() {
                    if c == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            _ => out.push(c),
        }
    }
    out
}

/// The tokens of a code text: parentheses, brackets and braces alone, other runs split at
/// whitespace.
fn tokens(code: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, c) in code.char_indices() {
        let delim = matches!(c, '(' | ')' | '[' | ']' | '{' | '}');
        if c.is_whitespace() || delim {
            if let Some(s) = start.take() {
                out.push(&code[s..i]);
            }
            if delim {
                out.push(&code[i..i + 1]);
            }
        } else if start.is_none() {
            start = Some(i);
        }
    }
    if let Some(s) = start {
        out.push(&code[s..]);
    }
    out
}

/// The last segment of a possibly qualified name (`ns.mod.schema` → `schema`).
fn last_segment(s: &str) -> &str {
    s.rsplit('.').next().unwrap_or(s)
}

/// The ledger tables of a module's code: each `(deftable T:{S})` whose `(defschema S …)` has a
/// `balance` field at its top level (a `balance` inside `@model` does not count). In code
/// order, at most [`MAX_TABLES`], each a valid identifier.
pub fn ledger_tables(code: &str) -> Vec<String> {
    let clean = code_only(code);
    let t = tokens(&clean);
    let mut schemas: Vec<&str> = Vec::new();
    let mut tables: Vec<(&str, &str)> = Vec::new();
    let mut i = 0;
    while i + 1 < t.len() {
        if t[i] != "(" {
            i += 1;
            continue;
        }
        match t[i + 1] {
            "defschema" if i + 2 < t.len() => {
                let name = t[i + 2];
                let (mut depth, mut j, mut has) = (1usize, i + 3, false);
                while j < t.len() && depth > 0 {
                    match t[j] {
                        "(" | "[" | "{" => depth += 1,
                        ")" | "]" | "}" => depth -= 1,
                        f if depth == 1 => {
                            has |= f.starts_with("balance:")
                                || (f == "balance"
                                    && t.get(j + 1).is_some_and(|n| n.starts_with(':')));
                        }
                        _ => {}
                    }
                    j += 1;
                }
                if has {
                    schemas.push(name);
                }
                i = j;
            }
            "deftable" if i + 2 < t.len() => {
                // `T:{S}`, `T: {S}` or `T :{S}`: the name, then `{`, the schema, `}`.
                let name = t[i + 2].trim_end_matches(':');
                let mut j = i + 3;
                while j < t.len() && t[j] == ":" {
                    j += 1;
                }
                if t.get(j) == Some(&"{") {
                    if let Some(s) = t.get(j + 1) {
                        tables.push((name, last_segment(s)));
                    }
                }
                i = j;
            }
            _ => i += 1,
        }
    }
    let mut out: Vec<String> = Vec::new();
    for (name, schema) in tables {
        if schemas.contains(&schema) && valid_ident(name) && !out.iter().any(|o| o == name) {
            out.push(name.to_owned());
        }
    }
    out.truncate(MAX_TABLES);
    out
}

/// A chain whose listing failed: failures in a row, and when it is listed again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainFail {
    pub chain: u8,
    pub fails: u8,
    /// Unix seconds; [`PARKED`]: never.
    pub retry_at: u32,
}

/// `retry_at` of a parked `(token, chain)`: its `keys` read timed out, and the node keeps
/// listing after the client gives up (on mainnet01 about 14 minutes of a chain's pact
/// service, during which the node's cut stood still), so it is never sent again.
pub const PARKED: u32 = u32::MAX;

impl ChainFail {
    /// Never listed again (a `keys` read timed out).
    pub fn parked(&self) -> bool {
        self.retry_at == PARKED
    }
}

/// The pause before a `(token, chain)` that failed `fails` times in a row is listed again.
pub fn retry_after(fails: u8) -> u32 {
    let shift = u32::from(fails.max(1) - 1).min(16);
    (u64::from(RETRY_BASE_SECS) << shift).min(u64::from(RETRY_CAP_SECS)) as u32
}

/// A full round lists every chain not waiting out a failure; a retry round only the failed
/// chains that are due.
const FULL: u8 = 1;
/// Over its failure budget: left alone until [`UNLISTABLE_SECS`] after `listed_at`.
const UNLISTABLE: u8 = 2;
/// Layout version of [`ListState::encode`].
const STATE_V: u8 = 2;
/// Bytes before the failed chains in that layout.
const HEAD: usize = 14;

/// One token's listing progress ([`meta::KEYLIST`]).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListState {
    /// Unix seconds of the last complete full round (unlistable: when it was given up); 0:
    /// never.
    pub listed_at: u32,
    /// Index (into the network's chains) of the next chain of the round in progress.
    pub next_chain: u8,
    /// [`FULL`], [`UNLISTABLE`].
    pub flags: u8,
    /// Chain failures since the token's last round that ended with no failed chain.
    pub fail_total: u16,
    /// Rows marked since the last full round began: the room under the row cap counts them,
    /// as the module's `rows` grow only when the reconcile reads the marks (review note 1).
    pub marked_round: u32,
    /// The chains whose last listing failed.
    pub chains: Vec<ChainFail>,
}

impl ListState {
    /// Version, listed_at u32, next_chain, flags, fail_total u16, marked_round u32, count,
    /// then per failed chain: chain, fails, retry_at u32.
    pub fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(HEAD + 6 * self.chains.len());
        v.push(STATE_V);
        v.extend_from_slice(&self.listed_at.to_be_bytes());
        v.push(self.next_chain);
        v.push(self.flags);
        v.extend_from_slice(&self.fail_total.to_be_bytes());
        v.extend_from_slice(&self.marked_round.to_be_bytes());
        v.push(self.chains.len().min(255) as u8);
        for c in self.chains.iter().take(255) {
            v.push(c.chain);
            v.push(c.fails);
            v.extend_from_slice(&c.retry_at.to_be_bytes());
        }
        v
    }
    /// None for another layout (the token is then listed as never listed).
    pub fn decode(v: &[u8]) -> Option<ListState> {
        if v.len() < HEAD || v[0] != STATE_V {
            return None;
        }
        let n = usize::from(v[HEAD - 1]);
        if v.len() != HEAD + 6 * n {
            return None;
        }
        let u32_at =
            |i: usize| -> Option<u32> { Some(u32::from_be_bytes(v[i..i + 4].try_into().ok()?)) };
        let chains = (0..n)
            .map(|i| {
                let o = HEAD + 6 * i;
                Some(ChainFail {
                    chain: v[o],
                    fails: v[o + 1],
                    retry_at: u32_at(o + 2)?,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(ListState {
            listed_at: u32_at(1)?,
            next_chain: v[5],
            flags: v[6],
            fail_total: u16::from_be_bytes([v[7], v[8]]),
            marked_round: u32_at(9)?,
            chains,
        })
    }
    pub fn unlistable(&self) -> bool {
        self.flags & UNLISTABLE != 0
    }
    /// Due for a listing at `now`: never listed, a round in progress, older than
    /// [`SWEEP_SECS`], or a failed chain whose pause is over; an unlistable token only after
    /// [`UNLISTABLE_SECS`].
    pub fn due(&self, now: u32) -> bool {
        let age = now.saturating_sub(self.listed_at);
        if self.unlistable() {
            return age >= UNLISTABLE_SECS;
        }
        self.listed_at == 0
            || self.next_chain > 0
            || age >= SWEEP_SECS
            || self.chains.iter().any(|c| c.retry_at <= now)
    }
    /// Start a round at `now` (full, or a retry of the failed chains that are due).
    fn begin(&mut self, now: u32) {
        if self.unlistable() {
            // Given up long enough: one more try, with a fresh budget. Parked chains stay
            // parked: a fresh budget does not make a table the node cannot list quick.
            let parked = self.chains.iter().filter(|c| c.parked()).copied().collect();
            *self = ListState {
                chains: parked,
                ..ListState::default()
            };
        }
        let full = self.listed_at == 0 || now.saturating_sub(self.listed_at) >= SWEEP_SECS;
        self.flags = if full { FULL } else { 0 };
        if full {
            self.marked_round = 0;
        }
    }
    /// Whether this round lists `chain` at `now`.
    fn selected(&self, chain: u8, now: u32) -> bool {
        match self.chains.iter().find(|c| c.chain == chain) {
            Some(c) => c.retry_at <= now,
            None => self.flags & FULL != 0,
        }
    }
    /// `chain` failed at `now`: its pause grows, and the token's budget shrinks.
    fn failed(&mut self, chain: u8, now: u32) {
        let i = match self.chains.iter().position(|c| c.chain == chain) {
            Some(i) => i,
            None => {
                self.chains.push(ChainFail {
                    chain,
                    fails: 0,
                    retry_at: 0,
                });
                self.chains.len() - 1
            }
        };
        let c = &mut self.chains[i];
        // A chain already at the longest pause costs the token's budget no more (PR #27 R-2): one
        // table that never lists (over the key cap) must not, a few times a month, make the
        // healthy chains of its token unlistable after months.
        let at_cap = c.fails > 0 && retry_after(c.fails) >= RETRY_CAP_SECS;
        c.fails = c.fails.saturating_add(1);
        c.retry_at = now.saturating_add(retry_after(c.fails));
        if !at_cap {
            self.fail_total = self.fail_total.saturating_add(1);
        }
    }
    /// `chain`'s `keys` read timed out: parked for good (charged to the budget once).
    fn park(&mut self, chain: u8) {
        self.chains.retain(|c| c.chain != chain);
        self.chains.push(ChainFail {
            chain,
            fails: 1,
            retry_at: PARKED,
        });
        self.fail_total = self.fail_total.saturating_add(1);
    }
    /// No chain waits for a retry (parked chains never do).
    fn clean(&self) -> bool {
        self.chains.iter().all(ChainFail::parked)
    }
    /// `chain` listed: no longer failed.
    fn cleared(&mut self, chain: u8) {
        self.chains.retain(|c| c.chain != chain);
    }
}

fn state_key(module: u32) -> String {
    format!("{}{module:010}", meta::KEYLIST)
}

/// The stored listing state of `module` (default: never listed).
pub fn state(txn: &impl Txn, module: u32) -> Result<ListState> {
    Ok(txn
        .meta(&state_key(module))?
        .as_deref()
        .and_then(ListState::decode)
        .unwrap_or_default())
}

/// The tokens due for a listing at `now`, by name, oldest listing first (never listed
/// first): tracked tokens other than `coin`, not-fungible verdicts and capped tokens, whose
/// names pass the B-1 check, and that are not in `skip` (`balances.keylist_skip`: never read).
pub fn due(
    txn: &impl Txn,
    dict: &impl Dict,
    now: u32,
    skip: &[String],
) -> Result<Vec<(u32, String, ListState)>> {
    let coin = balances::coin_module(txn)?;
    let mut out = Vec::new();
    for (k, v) in txn.scan(Table::Tokens, &[], |_| true)? {
        let (Ok(id), Some(t)) = (<[u8; 4]>::try_from(k.as_slice()), TokenRow::decode(&v)) else {
            continue;
        };
        let id = u32::from_be_bytes(id);
        if Some(id) == coin || t.kind == KIND_NOT_FUNGIBLE || t.capped() {
            continue;
        }
        let Some(name) = dict.name(DictKind::Module, id) else {
            continue;
        };
        if !valid_module(name) || skip.iter().any(|s| s == name) {
            continue;
        }
        let st = state(txn, id)?;
        if st.due(now) {
            out.push((id, name.to_owned(), st));
        }
    }
    out.sort_by_key(|(id, _, st)| (st.listed_at, *id));
    Ok(out)
}

/// Token counts of the key listing (`--status`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Summary {
    /// Last round ended with no failed chain.
    pub listed: u64,
    /// A round in progress.
    pub in_progress: u64,
    /// Some chain failed and waits for its retry.
    pub failed: u64,
    /// Over the failure budget, left alone.
    pub unlistable: u64,
    /// Some chain is parked: its `keys` read timed out, never sent again.
    pub parked: u64,
}

/// The [`Summary`] of the stored listing states.
pub fn summary(txn: &impl Txn) -> Result<Summary> {
    let p = meta::KEYLIST.as_bytes();
    let mut s = Summary::default();
    for (_, v) in txn.scan(Table::Meta, p, |k| k.starts_with(p))? {
        let Some(st) = ListState::decode(&v) else {
            continue;
        };
        if st.unlistable() {
            s.unlistable += 1;
            continue;
        }
        s.listed += u64::from(st.listed_at > 0 && st.clean());
        s.in_progress += u64::from(st.next_chain > 0);
        s.failed += u64::from(!st.clean());
        s.parked += u64::from(st.chains.iter().any(ChainFail::parked));
    }
    Ok(s)
}

/// What one listing marked.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Marked {
    /// Keys the node listed.
    pub listed: u64,
    /// Keys we already hold a row or a mark for.
    pub known: u64,
    /// Keys marked dirty for the balance read.
    pub marked: u64,
    /// Keys not marked: an empty or over-long name, or the module's row cap.
    pub skipped: u64,
}

/// Mark the listed `keys` of `module` on `chain` that we hold nothing for, at `height` (the
/// block the node read), through the postings' mark path. Names are interned (lock order:
/// dictionary, then the MDBX writer), but only for keys that will be marked: a module with a
/// not-fungible verdict or at its row cap interns and marks nothing, and new keys stop at the
/// room left under `row_cap` (I-7: its `Balances` rows, the `already` marks of this round's
/// earlier listings not read yet, and this listing's marks), so a hostile token's long
/// listings cannot grow the dictionary past what the cap allows.
#[allow(clippy::too_many_arguments)]
pub fn mark_keys(
    hot: &HotStore,
    dict: &Mutex<HotDict>,
    module: u32,
    chain: u8,
    keys: &[String],
    height: u32,
    row_cap: u32,
    already: u64,
) -> Result<Marked> {
    // The dictionary lock is held across the write txn (up to 150,000 x 3 lookups); ingest
    // interns under the same lock, so a big listing can pause it for a fraction of a second
    // (review note 5). Fine at the measured sizes; a two-phase lookup is parked.
    let mut d = dict.lock().unwrap_or_else(|e| e.into_inner());
    let r = (|| -> Result<Marked> {
        let mut txn = hot.rw()?;
        let coin = balances::coin_module(&txn)?;
        let mut m = Marked {
            listed: keys.len() as u64,
            ..Marked::default()
        };
        let t = balances::token_row(&txn, module)?.unwrap_or_default();
        if t.kind == KIND_NOT_FUNGIBLE || t.capped() {
            m.skipped = m.listed;
            return Ok(m);
        }
        let mut room = u64::from(row_cap.saturating_sub(t.rows)).saturating_sub(already);
        for a in keys {
            if a.is_empty() || a.len() > MAX_NAME_LEN {
                m.skipped += 1;
                continue;
            }
            if let Some(id) = d.find(DictKind::Account, a) {
                let k = bal_key(id, module, chain);
                if txn.get(Table::Balances, &k)?.is_some()
                    || txn.get(Table::BalanceDirty, &k)?.is_some()
                    || txn.get(Table::BalanceSeed, &k)?.is_some()
                {
                    m.known += 1;
                    continue;
                }
            }
            if room == 0 {
                m.skipped += 1;
                continue;
            }
            let id = d.intern(DictKind::Account, a);
            if balances::mark_row(&mut txn, id, module, chain, height, coin)? {
                m.marked += 1;
                room -= 1;
            } else {
                m.skipped += 1;
            }
        }
        d.persist(&mut txn)?;
        txn.commit()?;
        Ok(m)
    })();
    if r.is_err() {
        // Names interned here never reached MDBX: forget them (review M3).
        *d = HotDict::load(&hot.ro()?)?;
    }
    r
}

/// Running totals ([`meta::KEYLIST_STATS`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stats {
    /// Complete token listings (every chain tried).
    pub tokens_listed: u64,
    /// `(token, chain)` listings that failed (logged).
    pub chain_failures: u64,
    /// Connection or HTTP errors of the node (each paused the listing).
    #[serde(default)]
    pub node_errors: u64,
    /// Tokens given up over their failure budget.
    #[serde(default)]
    pub unlistable: u64,
    #[serde(flatten)]
    pub marked: Marked,
    /// The last failure: module, chain, reason.
    pub last_failure: Option<String>,
}

fn add_stats(txn: &mut kdx_hot::RwTxn<'_>, f: impl FnOnce(&mut Stats)) -> Result<()> {
    let mut s: Stats = txn
        .meta(meta::KEYLIST_STATS)?
        .and_then(|v| serde_json::from_slice(&v).ok())
        .unwrap_or_default();
    f(&mut s);
    txn.set_meta(meta::KEYLIST_STATS, &serde_json::to_vec(&s)?)
}

/// The listing task's queue of due tokens and its pacing.
pub struct KeyList {
    queue: VecDeque<(u32, String, ListState)>,
    refreshed: Option<Instant>,
    next_at: Option<Instant>,
    /// Connection or HTTP errors in a row on the step at the front of the queue.
    node_errors: u32,
    /// The current pause of the whole listing after such errors.
    pause: Duration,
    /// Timeout of one read ([`READ_TIMEOUT`]; tests shorten it).
    read_timeout: Duration,
    /// `balances.keylist_skip`: modules never sent a read on any chain.
    skip: Vec<String>,
}

impl Default for KeyList {
    fn default() -> KeyList {
        KeyList {
            queue: VecDeque::new(),
            refreshed: None,
            next_at: None,
            node_errors: 0,
            pause: Duration::ZERO,
            read_timeout: READ_TIMEOUT,
            skip: Vec::new(),
        }
    }
}

/// How one `(token, chain)` went.
enum ChainResult {
    /// Listed (or the module is not on the chain).
    Done(Marked),
    /// Marked nothing; logged; the chain counts as failed.
    Failed(String),
    /// A `keys` read timed out: marked nothing; logged; the chain is parked.
    TooSlow(String),
    /// A connection or HTTP error: the node, not the token, may be the trouble.
    Node(String),
}

fn now_unix() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as u32)
}

/// A node error of a listing read. `token_slow`: a timeout of a `keys` read is the token's (its
/// table may be too big to list in time, H-1). A `describe-module` timeout (the module code is
/// small and bounded) and any other error are the node's, so a node that hangs pauses the
/// listing instead of burning the budgets of the tokens (PR #27 R-1).
fn node_result(what: &str, e: NodeError, token_slow: bool) -> ChainResult {
    match e {
        NodeError::Timeout if token_slow => ChainResult::TooSlow(format!("{what}: timeout")),
        e => ChainResult::Node(format!("{what}: {e}")),
    }
}

#[allow(clippy::too_many_arguments)]
async fn list_chain(
    ctx: &Ctx,
    local: &LocalClient,
    caps: &crate::reconcile::Caps,
    timeout: Duration,
    module: u32,
    name: &str,
    chain: u8,
    already: u64,
) -> Result<ChainResult> {
    let code = match local.module_code(chain, name, timeout).await {
        Ok(Local::Absent) => return Ok(ChainResult::Done(Marked::default())),
        Ok(Local::Found { value, .. }) => value,
        Err(LocalError::Node(e)) => return Ok(node_result("describe-module", e, false)),
        Err(e) => return Ok(ChainResult::Failed(format!("describe-module: {e}"))),
    };
    let tables = ledger_tables(&code);
    if tables.is_empty() {
        // Nothing this rule can list (review note 6): done until the next sweep, not a failure
        // retried and logged forever. A ledger whose field is not `balance` is a known gap.
        tracing::debug!(module = %name, chain, "balances: key listing: no table with a balance field");
        return Ok(ChainResult::Done(Marked::default()));
    }
    let mut lists = Vec::with_capacity(tables.len());
    for t in &tables {
        let max = MAX_KEYS_PER_LISTING.min(MAX_LISTED_KEYS);
        match local.table_keys(chain, name, t, max, timeout).await {
            Ok(Local::Found { value, height, .. }) => {
                lists.push((value, u32::try_from(height).unwrap_or(u32::MAX)))
            }
            Ok(Local::Absent) => {}
            Err(LocalError::Node(e)) => return Ok(node_result(&format!("keys {t}"), e, true)),
            Err(e) => return Ok(ChainResult::Failed(format!("keys {t}: {e}"))),
        }
    }
    // The I-7 cap the reconcile applies to this module: confirmed `fungible-v2` modules get
    // the larger one (F-14).
    let trusted =
        balances::token_row(&ctx.hot.ro()?, module)?.is_some_and(|t| t.fungible_chains != 0);
    let row_cap = if trusted {
        caps.fungible_module_rows
    } else {
        caps.module_rows
    };
    let (hot, dict) = (ctx.hot.clone(), ctx.dict.clone());
    let total = tokio::task::spawn_blocking(move || -> Result<Marked> {
        let mut total = Marked::default();
        for (keys, height) in lists {
            let before = already + total.marked;
            let m = mark_keys(&hot, &dict, module, chain, &keys, height, row_cap, before)?;
            total.listed += m.listed;
            total.known += m.known;
            total.marked += m.marked;
            total.skipped += m.skipped;
        }
        Ok(total)
    })
    .await??;
    Ok(ChainResult::Done(total))
}

/// One paced step of the key listing (see the module doc): returns whether it read the node.
/// Errors of the store are returned; node trouble never is.
pub async fn step(
    ctx: &Ctx,
    local: &LocalClient,
    caps: &crate::reconcile::Caps,
    kl: &mut KeyList,
) -> Result<bool> {
    let now = Instant::now();
    if kl.next_at.is_some_and(|t| now < t) {
        return Ok(false);
    }
    kl.next_at = Some(now + STEP_EVERY);
    if kl
        .refreshed
        .is_none_or(|t| now.duration_since(t) >= REFRESH)
    {
        kl.refreshed = Some(now);
        let ro = ctx.hot.ro()?;
        if let Err(e) = crate::seed_receivers::check_ready(&ro, &ctx.segdir) {
            tracing::debug!(error = %e, "balances: key listing postponed");
            kl.queue.clear();
            return Ok(false);
        }
        let d = ctx.dict.lock().unwrap_or_else(|e| e.into_inner());
        kl.queue = due(&ro, &*d, now_unix(), &kl.skip)?.into();
        if !kl.queue.is_empty() {
            tracing::info!(
                tokens = kl.queue.len(),
                "balances: key listing of tracked tokens due"
            );
        }
    }
    let Some((module, name, mut st)) = kl.queue.front().cloned() else {
        return Ok(false);
    };
    let chains = ctx.graph.chains_at(u64::MAX).to_vec();
    let t = now_unix();
    if st.next_chain == 0 {
        st.begin(t);
    }
    // Chains this round does not list (waiting out a failure, or not failed in a retry
    // round) cost nothing.
    let mut idx = usize::from(st.next_chain);
    while chains.get(idx).is_some_and(|&c| !st.selected(c, t)) {
        idx += 1;
    }
    let Some(&chain) = chains.get(idx) else {
        let mut txn = ctx.hot.rw()?;
        end_round(&mut txn, kl, module, &mut st, t)?;
        txn.commit()?;
        return Ok(false);
    };
    let already = u64::from(st.marked_round);
    let res = list_chain(
        ctx,
        local,
        caps,
        kl.read_timeout,
        module,
        &name,
        chain,
        already,
    )
    .await?;
    let mut txn = ctx.hot.rw()?;
    let mut failed = false;
    match res {
        ChainResult::Node(e) => {
            kl.node_errors += 1;
            add_stats(&mut txn, |s| s.node_errors += 1)?;
            if kl.node_errors < NODE_TRIES {
                kl.pause = (kl.pause * 2).clamp(NODE_PAUSE, NODE_PAUSE_MAX);
                tracing::debug!(module = %name, chain, error = %e, pause_s = kl.pause.as_secs(),
                    "balances: key listing: node error, listing paused");
                kl.next_at = Some(now + kl.pause);
                txn.commit()?;
                return Ok(true);
            }
            kl.next_at = Some(now + kl.pause);
            fail(
                &mut txn,
                &mut st,
                &name,
                chain,
                t,
                &format!("node: {e}"),
                false,
            )?;
            failed = true;
        }
        ChainResult::Failed(e) => {
            fail(&mut txn, &mut st, &name, chain, t, &e, false)?;
            failed = true;
        }
        ChainResult::TooSlow(e) => {
            fail(&mut txn, &mut st, &name, chain, t, &e, true)?;
            failed = true;
        }
        ChainResult::Done(m) => {
            kl.pause = Duration::ZERO;
            st.cleared(chain);
            st.marked_round = st
                .marked_round
                .saturating_add(u32::try_from(m.marked).unwrap_or(u32::MAX));
            if m.marked > 0 {
                tracing::info!(module = %name, chain, listed = m.listed, marked = m.marked,
                    "balances: key listing marked rows no tx names");
            }
            add_stats(&mut txn, |s| {
                s.marked.listed += m.listed;
                s.marked.known += m.known;
                s.marked.marked += m.marked;
                s.marked.skipped += m.skipped;
            })?;
        }
    }
    kl.node_errors = 0;
    st.next_chain = u8::try_from(idx + 1).unwrap_or(u8::MAX);
    if st.fail_total >= FAIL_BUDGET {
        // Given up (H-1: a token of junk tables must not take the listing's time forever).
        tracing::warn!(module = %name, failures = st.fail_total,
            "balances: key listing: token over its failure budget, unlistable for {} days",
            UNLISTABLE_SECS / 86_400);
        st = ListState {
            listed_at: t,
            flags: UNLISTABLE,
            fail_total: st.fail_total,
            chains: st.chains,
            ..ListState::default()
        };
        add_stats(&mut txn, |s| s.unlistable += 1)?;
        kl.queue.pop_front();
    } else if usize::from(st.next_chain) >= chains.len() {
        end_round(&mut txn, kl, module, &mut st, t)?;
    } else if failed {
        // To the back: one failing token cannot hold the queue.
        if let Some(mut f) = kl.queue.pop_front() {
            f.2 = st.clone();
            kl.queue.push_back(f);
        }
    } else if let Some(f) = kl.queue.front_mut() {
        f.2 = st.clone();
    }
    txn.set_meta(&state_key(module), &st.encode())?;
    txn.commit()?;
    Ok(true)
}

/// Close the round at the front of the queue at `now` (stored by the caller's `txn`).
fn end_round(
    txn: &mut kdx_hot::RwTxn<'_>,
    kl: &mut KeyList,
    module: u32,
    st: &mut ListState,
    now: u32,
) -> Result<()> {
    if st.flags & FULL != 0 {
        st.listed_at = now;
        add_stats(txn, |s| s.tokens_listed += 1)?;
    }
    st.next_chain = 0;
    st.flags = 0;
    if st.clean() {
        st.fail_total = 0;
    }
    kl.queue.pop_front();
    txn.set_meta(&state_key(module), &st.encode())
}

fn fail(
    txn: &mut kdx_hot::RwTxn<'_>,
    st: &mut ListState,
    name: &str,
    chain: u8,
    now: u32,
    why: &str,
    park: bool,
) -> Result<()> {
    if park {
        st.park(chain);
        tracing::warn!(module = %name, chain, reason = %why,
            "balances: key listing failed; nothing marked for this chain, parked for good \
             (the node may still be listing the table, holding that chain)");
    } else {
        st.failed(chain, now);
        let fails = st
            .chains
            .iter()
            .find(|c| c.chain == chain)
            .map_or(0, |c| c.fails);
        tracing::warn!(module = %name, chain, reason = %why, fails,
            retry_in_s = retry_after(fails),
            "balances: key listing failed; nothing marked for this chain, retried later");
    }
    let cut: String = why.chars().take(200).collect();
    add_stats(txn, |s| {
        s.chain_failures += 1;
        s.last_failure = Some(format!("{name} chain {chain}: {cut}"));
    })
    .context("key listing stats")
}

/// Whether the listing may run with `local_concurrency` `/local` slots. With one slot a listing
/// read (up to [`READ_TIMEOUT`]) would hold the reconcile's only slot (PR #27 R-3), so the
/// listing is off then; the shipped configs use 8.
pub fn enabled(local_concurrency: usize) -> bool {
    local_concurrency > 1
}

/// The key listing as its own task (PR #27 H-1), kept alive: a panic in it is logged at ERROR
/// and the task restarted after [`PANIC_PAUSE`] (R-4; its progress is in the store, so it
/// resumes), until `stop`.
pub async fn run(
    ctx: Arc<Ctx>,
    local: Arc<LocalClient>,
    caps: crate::reconcile::Caps,
    skip: Vec<String>,
    stop: CancellationToken,
) {
    supervise(&stop, PANIC_PAUSE, || {
        tokio::spawn(run_task(
            ctx.clone(),
            local.clone(),
            caps,
            skip.clone(),
            stop.clone(),
        ))
    })
    .await;
}

/// Run the task `start` makes until it ends by itself (`stop`); when it panics, log at ERROR,
/// wait `pause` and start it again.
async fn supervise(
    stop: &CancellationToken,
    pause: Duration,
    mut start: impl FnMut() -> tokio::task::JoinHandle<()>,
) {
    loop {
        match start().await {
            Ok(()) => return,
            Err(e) if e.is_panic() => {
                tracing::error!(error = %e, pause_s = pause.as_secs(),
                    "balances: key listing task panicked; restarting after a pause");
            }
            // Cancelled by a runtime shutdown.
            Err(_) => return,
        }
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(pause) => {}
        }
    }
}

/// The listing loop: steps paced by [`STEP_EVERY`] (and the node pauses), apart from the
/// reconcile loop, until `stop`. A store error is logged and the loop pauses; it never ends the
/// reconcile.
async fn run_task(
    ctx: Arc<Ctx>,
    local: Arc<LocalClient>,
    caps: crate::reconcile::Caps,
    skip: Vec<String>,
    stop: CancellationToken,
) {
    let mut kl = KeyList {
        skip,
        ..KeyList::default()
    };
    loop {
        let r = tokio::select! {
            _ = stop.cancelled() => return,
            r = step(&ctx, &local, &caps, &mut kl) => r,
        };
        if let Err(e) = r {
            tracing::warn!(error = %e, "balances: key listing step failed; retried later");
            kl.next_at = Some(Instant::now() + STORE_PAUSE);
        }
        let wait = kl
            .next_at
            .map_or(STEP_EVERY, |t| t.saturating_duration_since(Instant::now()));
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(wait.max(Duration::from_millis(10))) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use kdx_hot::{set_checkpoint, Geometry, Stage};
    use kdx_node::local::LocalOpts;
    use kdx_node::{ChainGraph, NodeClient, NodeClientConfig, NodeInfo};
    use kdx_types::Hash32;
    use serde_json::{json, Value};

    use super::*;
    use crate::reconcile::Caps;
    use crate::IngestOpts;

    type Answer = dyn Fn(u8, &str, &Value) -> Option<Value> + Send + Sync;

    /// A fake node: `answer(chain, code, data)` gives `result` (with tip height 10), or None
    /// for an HTTP 500; `{"sleep_ms": n}` answers nothing for `n` ms, then closes. Counts the
    /// `/local` calls. One thread per connection, so a slow answer holds up no other.
    fn fake_node(answer: Arc<Answer>, calls: Arc<AtomicUsize>) -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        std::thread::spawn(move || {
            for s in l.incoming() {
                let Ok(mut s) = s else { continue };
                let (answer, calls) = (answer.clone(), calls.clone());
                std::thread::spawn(move || {
                    let (mut buf, mut tmp) = (Vec::new(), [0u8; 8192]);
                    let (head_end, len) = loop {
                        let n = s.read(&mut tmp).unwrap_or(0);
                        if n == 0 {
                            break (0, 0);
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&buf[..i]).to_lowercase();
                            let len = head
                                .lines()
                                .find_map(|l| l.strip_prefix("content-length:"))
                                .and_then(|v| v.trim().parse::<usize>().ok())
                                .unwrap_or(0);
                            break (i + 4, len);
                        }
                    };
                    while head_end > 0 && buf.len() < head_end + len {
                        let n = s.read(&mut tmp).unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    calls.fetch_add(1, Ordering::SeqCst);
                    let req: Value =
                        serde_json::from_slice(&buf[head_end..]).unwrap_or(Value::Null);
                    let cmd: Value = req
                        .get("cmd")
                        .and_then(Value::as_str)
                        .and_then(|c| serde_json::from_str(c).ok())
                        .unwrap_or(Value::Null);
                    let chain: u8 = cmd
                        .pointer("/meta/chainId")
                        .and_then(Value::as_str)
                        .and_then(|c| c.parse().ok())
                        .unwrap_or(0);
                    let code = cmd
                        .pointer("/payload/exec/code")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let data = cmd
                        .pointer("/payload/exec/data")
                        .cloned()
                        .unwrap_or(Value::Null);
                    let r = answer(chain, code, &data);
                    if let Some(ms) = r
                        .as_ref()
                        .and_then(|r| r.get("sleep_ms"))
                        .and_then(Value::as_u64)
                    {
                        std::thread::sleep(Duration::from_millis(ms));
                        return;
                    }
                    let (status, body) = match r {
                        Some(r) => (
                            "200 OK",
                            json!({"result": r,
                        "metaData": {"blockHeight": 11, "prevBlockHash": "H"}})
                            .to_string(),
                        ),
                        None => ("500 Internal Server Error", "down".to_owned()),
                    };
                    let _ = write!(
                    s,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                );
                });
            }
        });
        format!("http://{addr}")
    }

    /// A store ready for the listing (a tail on chain 0), `coin` and `tokens` tracked, and a
    /// client of a fake node on chains 0 and 1.
    fn setup(
        dir: &std::path::Path,
        tokens: &[&str],
        answer: Arc<Answer>,
    ) -> (Ctx, LocalClient, Vec<u32>, Arc<AtomicUsize>) {
        setup_chains(dir, tokens, answer, 2)
    }

    /// [`setup`] with a graph of chains `0..chains`.
    fn setup_chains(
        dir: &std::path::Path,
        tokens: &[&str],
        answer: Arc<Answer>,
        chains: u8,
    ) -> (Ctx, LocalClient, Vec<u32>, Arc<AtomicUsize>) {
        let hot = Arc::new(HotStore::open(&dir.join("hot"), Geometry::default()).unwrap());
        let mut txn = hot.rw().unwrap();
        let mut d = HotDict::load(&txn).unwrap();
        let coin = d.intern(DictKind::Module, "coin");
        let ids: Vec<u32> = tokens
            .iter()
            .map(|t| d.intern(DictKind::Module, t))
            .collect();
        d.persist(&mut txn).unwrap();
        for &m in ids.iter().chain([&coin]) {
            balances::note_token(&mut txn, m, 5).unwrap();
        }
        set_checkpoint(&mut txn, Stage::Tail, 0, 10, Hash32([0x11; 32])).unwrap();
        txn.commit().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let node = NodeClient::new(&NodeClientConfig {
            base: fake_node(answer, calls.clone()).parse().unwrap(),
            network: "mainnet01".into(),
            timeout: Duration::from_secs(5),
            max_rps: 0,
            concurrency: 4,
            p2p_url: None,
            p2p_cert_sha256: None,
            max_body_bytes: 1 << 20,
            max_sse_event_bytes: 1 << 20,
        })
        .unwrap();
        let info = NodeInfo {
            node_version: String::new(),
            chainweb_version: "mainnet01".into(),
            graph_history: vec![(0, (0..chains).collect())],
            raw: Value::Null,
        };
        let ctx = Ctx {
            opts: IngestOpts {
                network: "mainnet01".into(),
                data_dir: dir.to_path_buf(),
                geometry: Geometry::default(),
                finality_depth: 100,
                batch_size: 50,
                ranges_in_flight: 1,
                range_heights: 300,
                step_heights: 100,
                min_free_gb: 0,
                range_level: 3,
                step_level: 3,
                dict_samples: 0,
                etl_mem_mb: 16,
                mem_cap_mb: 256,
                commit_ms: 1000,
                commit_blocks: 10,
                cut_poll_secs: 10,
                until: None,
                legacy_segments: false,
            },
            node: node.clone(),
            hot: hot.clone(),
            dict: Arc::new(Mutex::new(HotDict::load(&hot.ro().unwrap()).unwrap())),
            segdir: dir.join("segments"),
            etl: dir.join("etl"),
            graph: ChainGraph::from_info(&info),
        };
        let local = LocalClient::new(
            node,
            LocalOpts {
                rps: 0,
                concurrency: 4,
            },
        );
        (ctx, local, ids, calls)
    }

    fn ok(data: Value) -> Option<Value> {
        Some(json!({"status": "success", "data": data}))
    }

    fn not_deployed(m: &str) -> Option<Value> {
        Some(json!({"status": "failure", "error": {"type": "EvalError",
            "message": format!("Cannot find module: {m}")}}))
    }

    fn account(ctx: &Ctx, a: &str) -> Option<u32> {
        ctx.dict.lock().unwrap().find(DictKind::Account, a)
    }

    fn dirty(ctx: &Ctx, a: &str, m: u32, c: u8) -> Option<u32> {
        let id = account(ctx, a)?;
        ctx.hot
            .ro()
            .unwrap()
            .get(Table::BalanceDirty, &bal_key(id, m, c))
            .unwrap()
            .as_deref()
            .and_then(balances::dirty_height)
    }

    /// Steps until the queue is empty (pacing skipped), at most `n`.
    async fn run_steps(ctx: &Ctx, local: &LocalClient, kl: &mut KeyList, n: usize) {
        for _ in 0..n {
            kl.next_at = None;
            step(ctx, local, &Caps::default(), kl).await.unwrap();
            if kl.queue.is_empty() && kl.refreshed.is_some() {
                return;
            }
        }
        panic!("queue not drained after {n} steps");
    }

    #[test]
    fn list_state_round_trip_and_due() {
        let now = 2_000_000;
        let st = ListState {
            listed_at: 1_000_000,
            next_chain: 3,
            flags: FULL,
            fail_total: 7,
            marked_round: 9,
            chains: vec![ChainFail {
                chain: 19,
                fails: 2,
                retry_at: now + 5,
            }],
        };
        assert_eq!(ListState::decode(&st.encode()), Some(st.clone()));
        assert_eq!(ListState::decode(&[0; 9]), None, "the first layout");
        assert_eq!(ListState::decode(&st.encode()[..15]), None);
        assert!(ListState::default().due(now), "never listed");
        let done = |at| ListState {
            listed_at: at,
            ..ListState::default()
        };
        assert!(!done(now - 10).due(now));
        assert!(done(now - SWEEP_SECS).due(now), "the periodic sweep");
        let mut f = done(now - 10);
        f.failed(4, now);
        assert!(!f.due(now + RETRY_BASE_SECS - 1));
        assert!(
            f.due(now + RETRY_BASE_SECS),
            "a failed chain is retried sooner"
        );
        assert!(
            ListState {
                next_chain: 1,
                ..done(now - 10)
            }
            .due(now),
            "round in progress"
        );
        let gone = ListState {
            listed_at: now,
            flags: UNLISTABLE,
            ..ListState::default()
        };
        assert!(!gone.due(now + SWEEP_SECS) && gone.due(now + UNLISTABLE_SECS));
    }

    /// H-1: the pause of a failing `(token, chain)` doubles per failure in a row, capped.
    /// R-2: a chain that keeps failing at the longest pause stops costing the token's budget.
    #[test]
    fn a_chain_at_the_longest_pause_costs_no_more_budget() {
        let mut st = ListState::default();
        for _ in 0..40 {
            st.failed(7, 1_000);
        }
        // Charged until its pause reached the cap (9 failures), then free.
        assert_eq!(st.fail_total, 9);
        assert_eq!(st.chains[0].fails, 40);
        assert!(
            st.fail_total < FAIL_BUDGET,
            "a lone big chain is never unlistable"
        );
        st.failed(8, 1_000);
        assert_eq!(st.fail_total, 10, "another chain is charged as before");
    }

    /// R-3: the listing needs a second `/local` slot, so the reconcile is never blocked.
    #[test]
    fn the_listing_needs_a_second_local_slot() {
        assert!(!enabled(1));
        assert!(enabled(2) && enabled(8));
    }

    #[test]
    fn retry_backoff_grows_and_is_capped() {
        assert_eq!(retry_after(1), RETRY_BASE_SECS);
        assert_eq!(retry_after(2), 2 * RETRY_BASE_SECS);
        assert_eq!(retry_after(4), 8 * RETRY_BASE_SECS);
        assert_eq!(retry_after(9), RETRY_CAP_SECS);
        assert_eq!(retry_after(255), RETRY_CAP_SECS);
        // A retry round lists only the failed chains that are due; a full round skips a
        // chain still waiting out its pause.
        let mut st = ListState {
            listed_at: 100,
            ..ListState::default()
        };
        st.failed(2, 1_000);
        st.failed(2, 1_000 + RETRY_BASE_SECS);
        assert_eq!(st.chains[0].retry_at, 1_000 + 3 * RETRY_BASE_SECS);
        st.begin(1_000 + 3 * RETRY_BASE_SECS);
        assert!(st.selected(2, 1_000 + 3 * RETRY_BASE_SECS) && !st.selected(0, 1_000));
        st.begin(100 + SWEEP_SECS);
        assert!(st.selected(0, 100 + SWEEP_SECS));
        assert!(!st.selected(2, 1_000), "still waiting");
    }

    /// Never-listed tokens first; `coin`, not-fungible verdicts and capped tokens never.
    #[test]
    fn due_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let toks = [
            "free.KAYC",
            "free.shatter",
            "free.junk",
            "free.big",
            "bad name",
        ];
        let (ctx, _, ids, _) = setup(dir.path(), &toks, Arc::new(|_, _, _| None));
        let mut txn = ctx.hot.rw().unwrap();
        balances::update_token(&mut txn, ids[2], |t| t.kind = KIND_NOT_FUNGIBLE).unwrap();
        balances::update_token(&mut txn, ids[3], |t| t.flags |= balances::TOKEN_CAPPED).unwrap();
        let old = ListState {
            listed_at: 100,
            ..ListState::default()
        };
        txn.set_meta(&state_key(ids[0]), &old.encode()).unwrap();
        txn.commit().unwrap();
        let d = ctx.dict.lock().unwrap();
        let names: Vec<String> = due(&ctx.hot.ro().unwrap(), &*d, 100 + SWEEP_SECS, &[])
            .unwrap()
            .into_iter()
            .map(|(_, n, _)| n)
            .collect();
        assert_eq!(names, ["free.shatter", "free.KAYC"]);
    }

    /// `free.shatter` on chain 0 (KDA-03 node, 2026-11-23): four keys. The holder we already
    /// have (a row) and one already queued (a mark) are left alone; `ROOT`, written by the
    /// token's own `init`, and the others are marked at the height the node read.
    #[test]
    fn only_keys_we_hold_nothing_for_are_marked() {
        let dir = tempfile::tempdir().unwrap();
        let (ctx, _, ids, _) = setup(dir.path(), &["free.shatter"], Arc::new(|_, _, _| None));
        let shatter = ids[0];
        let have = "k:0d39e69d5a71e4abf926c84cc12fcef7eb7e993ce062a77dc692c9715b3b7ad4";
        let queued = "k:acca57aef1ef1f99e9948fa811c3c8c187af18f9424071a6cec956ea5d3737c6";
        {
            let mut d = ctx.dict.lock().unwrap();
            let mut txn = ctx.hot.rw().unwrap();
            let (h, q) = (
                d.intern(DictKind::Account, have),
                d.intern(DictKind::Account, queued),
            );
            d.persist(&mut txn).unwrap();
            txn.put(Table::Balances, &bal_key(h, shatter, 0), b"row")
                .unwrap();
            balances::mark_dirty(&mut txn, q, shatter, 0, 7).unwrap();
            txn.commit().unwrap();
        }
        let keys: Vec<String> = ["GeorgePublish", "ROOT", have, queued, ""]
            .into_iter()
            .map(String::from)
            .chain([format!("k:{}", "a".repeat(MAX_NAME_LEN))])
            .collect();
        let m = mark_keys(&ctx.hot, &ctx.dict, shatter, 0, &keys, 7_429_654, 100, 0).unwrap();
        assert_eq!(
            m,
            Marked {
                listed: 6,
                known: 2,
                marked: 2,
                skipped: 2
            }
        );
        assert_eq!(dirty(&ctx, "ROOT", shatter, 0), Some(7_429_654));
        assert_eq!(dirty(&ctx, "GeorgePublish", shatter, 0), Some(7_429_654));
        assert_eq!(
            dirty(&ctx, queued, shatter, 0),
            Some(7),
            "an existing mark is kept"
        );
        assert_eq!(dirty(&ctx, have, shatter, 0), None);
        // A second listing marks nothing new.
        let again = mark_keys(
            &ctx.hot,
            &ctx.dict,
            shatter,
            0,
            &keys[..4],
            7_429_700,
            100,
            0,
        )
        .unwrap();
        assert_eq!(again.marked, 0);
    }

    /// Security review: a long listing of a module near its I-7 row cap interns and marks only
    /// the room left; a module at its cap or with a not-fungible verdict interns nothing.
    #[test]
    fn a_long_listing_stops_at_the_row_cap() {
        let dir = tempfile::tempdir().unwrap();
        let toks = ["free.spam", "free.full", "free.notfung"];
        let (ctx, _, ids, _) = setup(dir.path(), &toks, Arc::new(|_, _, _| None));
        let mut txn = ctx.hot.rw().unwrap();
        balances::update_token(&mut txn, ids[0], |t| t.rows = 98).unwrap();
        balances::update_token(&mut txn, ids[1], |t| t.flags |= balances::TOKEN_CAPPED).unwrap();
        balances::update_token(&mut txn, ids[2], |t| t.kind = KIND_NOT_FUNGIBLE).unwrap();
        txn.commit().unwrap();
        let keys: Vec<String> = (0..10_000).map(|i| format!("junk-{i}")).collect();
        let accounts = || ctx.dict.lock().unwrap().len(DictKind::Account);
        let before = accounts();
        let m = mark_keys(&ctx.hot, &ctx.dict, ids[0], 0, &keys, 10, 100, 0).unwrap();
        assert_eq!((m.marked, m.skipped), (2, 9_998));
        assert_eq!(accounts(), before + 2);
        for &full in &ids[1..] {
            let m = mark_keys(&ctx.hot, &ctx.dict, full, 0, &keys, 10, 100, 0).unwrap();
            assert_eq!((m.marked, m.skipped), (0, 10_000));
        }
        assert_eq!(accounts(), before + 2);
        let hot = ctx.hot.ro().unwrap();
        assert_eq!(hot.count(Table::BalanceDirty).unwrap(), 2);
    }

    /// PR #27 review note 13: a listing answer over the node client's body cap (1 MB here) is
    /// refused through the real client after one request, fails only that chain and marks
    /// nothing.
    #[tokio::test]
    async fn an_answer_over_the_body_cap_fails_the_chain_once() {
        let dir = tempfile::tempdir().unwrap();
        let answer: Arc<Answer> = Arc::new(move |chain, code, _| {
            if code.contains("describe-module") {
                return ok(json!(KAYC));
            }
            if chain == 1 {
                return ok(json!(["ROOT"]));
            }
            let keys: Vec<String> = (0..20_000).map(|i| format!("k:{i:062}")).collect();
            ok(json!(keys))
        });
        let (ctx, local, ids, calls) = setup(dir.path(), &["free.big"], answer);
        let mut kl = KeyList::default();
        run_steps(&ctx, &local, &mut kl, 5).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "2 describe + 2 keys, no retry"
        );
        let ro = ctx.hot.ro().unwrap();
        assert_eq!(
            ro.count(Table::BalanceDirty).unwrap(),
            1,
            "chain 1 ROOT only"
        );
        let st = state(&ro, ids[0]).unwrap();
        assert_eq!(st.chains.iter().map(|c| c.chain).collect::<Vec<_>>(), [0]);
        let stats: Stats =
            serde_json::from_slice(&ro.meta(meta::KEYLIST_STATS).unwrap().unwrap()).unwrap();
        let why = stats.last_failure.unwrap();
        assert!(
            why.contains("invalid answer") && why.contains("exceeds"),
            "{why}"
        );
    }

    /// PR #27 review note 1: the room under the row cap counts the marks this round already
    /// made (the module's `rows` grow only when the reconcile reads them), so listings on
    /// several chains together stay under the cap.
    #[tokio::test]
    async fn the_row_cap_holds_across_the_listings_of_a_round() {
        let dir = tempfile::tempdir().unwrap();
        let answer: Arc<Answer> = Arc::new(move |_, code, _| {
            if code.contains("describe-module") {
                return ok(json!(KAYC));
            }
            let keys: Vec<String> = (0..5).map(|i| format!("junk-{i}")).collect();
            ok(json!(keys))
        });
        let (ctx, local, ids, _) = setup(dir.path(), &["free.spam"], answer);
        let caps = Caps {
            module_rows: 7,
            ..Caps::default()
        };
        let mut kl = KeyList::default();
        for _ in 0..3 {
            kl.next_at = None;
            step(&ctx, &local, &caps, &mut kl).await.unwrap();
        }
        let ro = ctx.hot.ro().unwrap();
        assert_eq!(
            ro.count(Table::BalanceDirty).unwrap(),
            7,
            "5 on chain 0, 2 on chain 1"
        );
        assert_eq!(state(&ro, ids[0]).unwrap().marked_round, 7);
    }

    /// D3 gap groups 3 and 4 end to end: `free.KAYC` `ROOT` (only in its code, written by
    /// `init`) on chain 0, the module absent on chain 1; the `kaddex.kdx` pool account (a
    /// run-time name) on chain 1, with its other tables not listed.
    #[tokio::test]
    async fn a_listing_marks_rows_no_tx_names() {
        let dir = tempfile::tempdir().unwrap();
        let pool = "ywxuQYJz8EF4B5RM4c6ADjl0HvFT5EB3wrmjb4TcEoE";
        let answer: Arc<Answer> = Arc::new(move |chain, code, data| {
            let m = data.get("m").and_then(Value::as_str).unwrap_or("");
            match (chain, code, m) {
                (0, c, "free.KAYC") if c.contains("describe-module") => ok(json!(KAYC)),
                (1, c, "kaddex.kdx") if c.contains("describe-module") => ok(json!(KDX)),
                (_, c, m) if c.contains("describe-module") => not_deployed(m),
                (0, "(keys free.KAYC.token-table)", _) => ok(json!(["ROOT"])),
                (1, "(keys kaddex.kdx.token-table)", _) => ok(json!([pool, "k:a"])),
                _ => panic!("unexpected call {chain} {code}"),
            }
        });
        let (ctx, local, ids, calls) = setup(dir.path(), &["free.KAYC", "kaddex.kdx"], answer);
        let mut kl = KeyList::default();
        run_steps(&ctx, &local, &mut kl, 10).await;
        assert_eq!(dirty(&ctx, "ROOT", ids[0], 0), Some(10));
        assert_eq!(dirty(&ctx, pool, ids[1], 1), Some(10));
        assert_eq!(dirty(&ctx, pool, ids[1], 0), None);
        // 2 tokens x 2 chains describe-module, one keys read per deployed chain.
        assert_eq!(calls.load(Ordering::SeqCst), 6);
        let ro = ctx.hot.ro().unwrap();
        for &m in &ids {
            let st = state(&ro, m).unwrap();
            assert!(
                st.listed_at > 0 && st.next_chain == 0 && st.chains.is_empty(),
                "{st:?}"
            );
        }
        let stats: Stats =
            serde_json::from_slice(&ro.meta(meta::KEYLIST_STATS).unwrap().unwrap()).unwrap();
        assert_eq!(
            (
                stats.tokens_listed,
                stats.marked.marked,
                stats.chain_failures
            ),
            (2, 3, 0)
        );
        assert_eq!(
            summary(&ro).unwrap(),
            Summary {
                listed: 2,
                ..Summary::default()
            }
        );
        // Listed: not due again before the sweep.
        drop(ro);
        kl.refreshed = None;
        kl.next_at = None;
        assert!(!step(&ctx, &local, &Caps::default(), &mut kl).await.unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 6);
    }

    /// Fail-closed: a refused `keys` read and a node that stays down mark nothing, are logged
    /// and counted, and leave the chain failed (retried after `RETRY_BASE_SECS`); the other
    /// chain of the same token still lists. A module with no ledger table (review note 6) is
    /// no failure: nothing to list, done until the next sweep.
    #[tokio::test]
    async fn failures_mark_nothing_and_are_retried() {
        let dir = tempfile::tempdir().unwrap();
        let answer: Arc<Answer> = Arc::new(move |chain, code, data| {
            let m = data.get("m").and_then(Value::as_str).unwrap_or("");
            match (chain, code, m) {
                (_, _, "free.down") => None,
                (0, c, "free.refused") if c.contains("describe-module") => ok(json!(KAYC)),
                (1, c, "free.refused") if c.contains("describe-module") => ok(json!(KAYC)),
                (0, "(keys free.refused.token-table)", _) => Some(json!({"status": "failure",
                    "error": {"message": "Module admin necessary for operation"}})),
                (1, "(keys free.refused.token-table)", _) => ok(json!(["ROOT"])),
                (_, c, "free.notable") if c.contains("describe-module") => ok(json!(
                    "(module notable G (defschema s x:integer) (deftable t:{s}))"
                )),
                _ => panic!("unexpected call {chain} {code}"),
            }
        });
        let toks = ["free.refused", "free.notable", "free.down"];
        let (ctx, local, ids, _) = setup(dir.path(), &toks, answer);
        let mut kl = KeyList::default();
        run_steps(&ctx, &local, &mut kl, 30).await;
        assert_eq!(dirty(&ctx, "ROOT", ids[0], 0), None);
        assert_eq!(dirty(&ctx, "ROOT", ids[0], 1), Some(10));
        let ro = ctx.hot.ro().unwrap();
        let st: Vec<ListState> = ids.iter().map(|&m| state(&ro, m).unwrap()).collect();
        let failed = |s: &ListState| s.chains.iter().map(|c| c.chain).collect::<Vec<_>>();
        assert_eq!(
            st.iter().map(failed).collect::<Vec<_>>(),
            [vec![0], vec![], vec![0, 1]]
        );
        assert!(st.iter().all(|s| s.listed_at > 0));
        let retry = st[0].chains[0].retry_at;
        assert!(!st[0].due(retry - 1) && st[0].due(retry));
        let stats: Stats =
            serde_json::from_slice(&ro.meta(meta::KEYLIST_STATS).unwrap().unwrap()).unwrap();
        assert_eq!(stats.chain_failures, 3);
        // free.down: one request per try, NODE_TRIES tries per chain (no client retries).
        assert_eq!(stats.node_errors, 2 * u64::from(NODE_TRIES));
        assert_eq!(
            summary(&ro).unwrap(),
            Summary {
                listed: 1,
                failed: 2,
                ..Summary::default()
            }
        );
        assert!(stats
            .last_failure
            .unwrap()
            .starts_with("free.down chain 1: node"));
    }

    /// The node does not stop a `keys` read when the client times out: on mainnet01 one
    /// `(keys free.crankk01.crankk01-token-table)` on chain 0 held that chain's pact service for
    /// about 14 minutes after each 90 s timeout (the node logged `Timeout while processing cut`
    /// throughout; its cut, the tail and every chain-0 `/local` of the reconcile stood still).
    /// So a `keys` timeout parks that `(token, chain)` for good: no retry, no sweep, and not
    /// after the token's unlistable period either; the token's other chains list as before.
    #[tokio::test]
    async fn a_keys_timeout_parks_the_chain_for_good() {
        let dir = tempfile::tempdir().unwrap();
        let answer: Arc<Answer> = Arc::new(move |chain, code, _| match (chain, code) {
            (_, c) if c.contains("describe-module") => ok(json!(KAYC)),
            (0, _) => Some(json!({"sleep_ms": 3_000})),
            (1, _) => ok(json!(["ROOT"])),
            _ => panic!("unexpected call {chain} {code}"),
        });
        let (ctx, local, ids, calls) = setup(dir.path(), &["free.huge"], answer);
        let mut kl = KeyList {
            read_timeout: Duration::from_millis(300),
            ..KeyList::default()
        };
        let t0 = Instant::now();
        assert!(step(&ctx, &local, &Caps::default(), &mut kl).await.unwrap());
        assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
        assert_eq!(calls.load(Ordering::SeqCst), 2, "describe + one keys read");
        run_steps(&ctx, &local, &mut kl, 5).await;
        assert_eq!(calls.load(Ordering::SeqCst), 4, "chain 0 timed out once");
        assert_eq!(dirty(&ctx, "ROOT", ids[0], 1), Some(10));
        let st = state(&ctx.hot.ro().unwrap(), ids[0]).unwrap();
        assert_eq!((st.chains.len(), st.chains[0].chain), (1, 0));
        assert!(st.chains[0].parked(), "{st:?}");
        for later in [RETRY_CAP_SECS, SWEEP_SECS - 1, 10 * UNLISTABLE_SECS] {
            let now = now_unix() + later;
            assert!(!st.selected(0, now), "chain 0 is never listed again");
        }
        assert!(
            !st.due(now_unix() + SWEEP_SECS - 10),
            "no retry round before the sweep"
        );
        assert_eq!(
            summary(&ctx.hot.ro().unwrap()).unwrap(),
            Summary {
                listed: 1,
                parked: 1,
                ..Summary::default()
            }
        );
        // The weekly sweep lists chain 1 again and leaves chain 0 alone.
        let mut txn = ctx.hot.rw().unwrap();
        let old = ListState {
            listed_at: 1,
            ..st.clone()
        };
        txn.set_meta(&state_key(ids[0]), &old.encode()).unwrap();
        txn.commit().unwrap();
        kl.refreshed = None;
        run_steps(&ctx, &local, &mut kl, 5).await;
        assert_eq!(calls.load(Ordering::SeqCst), 6, "chain 1 only");
        let st = state(&ctx.hot.ro().unwrap(), ids[0]).unwrap();
        assert!(st.listed_at > 1 && st.chains[0].parked(), "{st:?}");
        // Given up and tried again after the unlistable period: the parked chain stays parked.
        let mut gone = ListState {
            listed_at: 1,
            flags: UNLISTABLE,
            fail_total: FAIL_BUDGET,
            ..st.clone()
        };
        gone.begin(1 + UNLISTABLE_SECS);
        assert!(gone.chains[0].parked() && !gone.selected(0, u32::MAX - 1));
        assert!(gone.selected(1, 1 + UNLISTABLE_SECS) && gone.fail_total == 0);
    }

    /// `balances.keylist_skip`: a skipped module is never sent a read (`describe-module` or
    /// `keys`) on any chain, costs no budget and leaves no stored state; other tokens list as
    /// before. The node blocks its cut while it answers a `keys` of a huge table, so even the
    /// first read must not happen.
    #[tokio::test]
    async fn a_skipped_module_is_never_read() {
        let dir = tempfile::tempdir().unwrap();
        let skipped_reads = Arc::new(AtomicUsize::new(0));
        let seen = skipped_reads.clone();
        let answer: Arc<Answer> = Arc::new(move |_, code, _| {
            if code.contains("crankk01") {
                seen.fetch_add(1, Ordering::SeqCst);
            }
            if code.contains("describe-module") {
                ok(json!(KAYC))
            } else {
                ok(json!(["ROOT"]))
            }
        });
        let (ctx, local, ids, calls) = setup(dir.path(), &["free.crankk01", "free.fine"], answer);
        let mut kl = KeyList {
            skip: vec!["free.crankk01".into()],
            ..KeyList::default()
        };
        run_steps(&ctx, &local, &mut kl, 10).await;
        assert_eq!(
            skipped_reads.load(Ordering::SeqCst),
            0,
            "no read of a skipped module"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "free.fine: 2 chains x (describe + keys)"
        );
        let ro = ctx.hot.ro().unwrap();
        assert_eq!(
            state(&ro, ids[0]).unwrap(),
            ListState::default(),
            "no state, no budget"
        );
        assert!(state(&ro, ids[1]).unwrap().listed_at > 0);
        assert_eq!(dirty(&ctx, "ROOT", ids[0], 0), None);
        assert_eq!(
            summary(&ro).unwrap(),
            Summary {
                listed: 1,
                ..Summary::default()
            }
        );
        // Even a due token of the list is refused by `due`, the sweep included.
        let d = ctx.dict.lock().unwrap();
        let all = due(&ro, &*d, now_unix() + 10 * SWEEP_SECS, &kl.skip).unwrap();
        assert_eq!(
            all.iter().map(|t| t.1.as_str()).collect::<Vec<_>>(),
            ["free.fine"]
        );
    }

    /// H-1: a junk token (every `keys` read times out) goes to the back of the queue after each
    /// failure, so a good token behind it is listed first; once over its budget it is
    /// unlistable, shown by `--status`, and not due again for `UNLISTABLE_SECS`.
    #[tokio::test]
    async fn junk_tokens_cannot_hold_the_queue() {
        let dir = tempfile::tempdir().unwrap();
        let answer: Arc<Answer> = Arc::new(move |_, code, data| {
            let m = data.get("m").and_then(Value::as_str).unwrap_or("");
            match (code, m) {
                (c, _) if c.contains("describe-module") => ok(json!(KAYC)),
                ("(keys free.good.token-table)", _) => ok(json!(["ROOT"])),
                ("(keys free.junk.token-table)", _) => Some(json!({"sleep_ms": 2_000})),
                _ => panic!("unexpected call {code}"),
            }
        });
        let (ctx, local, ids, _) = setup(dir.path(), &["free.junk", "free.good"], answer);
        // The junk token is one failure from its budget.
        let mut txn = ctx.hot.rw().unwrap();
        let near = ListState {
            listed_at: 1,
            fail_total: FAIL_BUDGET - 2,
            ..ListState::default()
        };
        txn.set_meta(&state_key(ids[0]), &near.encode()).unwrap();
        txn.commit().unwrap();
        let mut kl = KeyList {
            read_timeout: Duration::from_millis(200),
            ..KeyList::default()
        };
        // Step 1: junk chain 0 times out, junk to the back; steps 2-3: the good token.
        for _ in 0..3 {
            kl.next_at = None;
            step(&ctx, &local, &Caps::default(), &mut kl).await.unwrap();
        }
        assert_eq!(dirty(&ctx, "ROOT", ids[1], 0), Some(10));
        assert_eq!(dirty(&ctx, "ROOT", ids[1], 1), Some(10));
        run_steps(&ctx, &local, &mut kl, 3).await;
        let ro = ctx.hot.ro().unwrap();
        let st = state(&ro, ids[0]).unwrap();
        assert!(st.unlistable(), "{st:?}");
        assert!(!st.due(now_unix() + SWEEP_SECS));
        assert!(st.due(st.listed_at + UNLISTABLE_SECS));
        assert_eq!(
            summary(&ro).unwrap(),
            Summary {
                listed: 1,
                unlistable: 1,
                ..Summary::default()
            }
        );
        let stats: Stats =
            serde_json::from_slice(&ro.meta(meta::KEYLIST_STATS).unwrap().unwrap()).unwrap();
        assert_eq!((stats.unlistable, stats.chain_failures), (1, 2));
        drop(ro);
        // Given up: no longer due, so no more reads.
        kl.refreshed = None;
        kl.next_at = None;
        assert!(!step(&ctx, &local, &Caps::default(), &mut kl).await.unwrap());
    }

    /// R-1: a node that hangs instead of refusing connections must not burn the failure
    /// budget of a token on 20 chains. A `describe-module` timeout is a node error (the pause
    /// and the slow charge after `NODE_TRIES`), not the token's; two hours of such a stall
    /// (90 s per read plus the pauses, simulated) leave the token far from `FAIL_BUDGET`.
    #[tokio::test]
    async fn a_hung_node_for_two_hours_does_not_make_a_token_unlistable() {
        let dir = tempfile::tempdir().unwrap();
        let answer: Arc<Answer> = Arc::new(move |_, _, _| Some(json!({"sleep_ms": 2_000})));
        let (ctx, local, ids, _) = setup_chains(dir.path(), &["free.wide"], answer, 20);
        let mut kl = KeyList {
            read_timeout: Duration::from_millis(50),
            ..KeyList::default()
        };
        let (mut sim, two_hours) = (Duration::ZERO, Duration::from_secs(2 * 3600));
        let mut steps = 0;
        while sim < two_hours {
            kl.next_at = None;
            step(&ctx, &local, &Caps::default(), &mut kl).await.unwrap();
            // The simulated clock: the read ran into READ_TIMEOUT, then the listing waited.
            sim += READ_TIMEOUT;
            sim += kl.next_at.map_or(Duration::ZERO, |t| {
                t.saturating_duration_since(Instant::now())
            });
            steps += 1;
            assert!(steps < 500, "the simulated clock did not advance");
        }
        let ro = ctx.hot.ro().unwrap();
        let st = state(&ro, ids[0]).unwrap();
        assert!(!st.unlistable(), "{st:?}");
        assert!(st.fail_total < FAIL_BUDGET / 2, "{st:?}");
    }

    /// R-4: a panic in the listing task is not lost: the supervisor restarts the task after
    /// the pause (and logs it at ERROR), and a clean end by `stop` ends the supervisor.
    #[tokio::test]
    async fn a_panic_in_the_listing_task_restarts_it() {
        let stop = CancellationToken::new();
        let starts = Arc::new(AtomicUsize::new(0));
        let (s2, st2) = (starts.clone(), stop.clone());
        let sup = tokio::spawn({
            let stop = stop.clone();
            async move {
                supervise(&stop, Duration::from_millis(20), move || {
                    let n = s2.fetch_add(1, Ordering::SeqCst);
                    let stop = st2.clone();
                    tokio::spawn(async move {
                        if n < 2 {
                            panic!("injected panic {n}");
                        }
                        stop.cancelled().await;
                    })
                })
                .await
            }
        });
        let t0 = Instant::now();
        while starts.load(Ordering::SeqCst) < 3 {
            assert!(t0.elapsed() < Duration::from_secs(5), "not restarted");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!sup.is_finished(), "the third run is alive");
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), sup)
            .await
            .expect("supervisor ends on stop")
            .unwrap();
        assert_eq!(starts.load(Ordering::SeqCst), 3);
    }

    /// `free.KAYC` (chain 0, D3 gap group 3), as `describe-module` answered it.
    const KAYC: &str = r#"(module KAYC GOVERNANCE
  @doc " KoolApes is the koolest token. "
  (defconst ROOT_ACCOUNT_ID:string 'ROOT)
  (defschema token-schema
    @doc " An account, holding a token balance. \
         \ \
         \ ROW KEY: accountId. "
    balance:decimal
    guard:guard
  )
  (defschema crosschain-schema
    @doc " Schema for yielded value in cross-chain transfers "
    receiver:string
    receiver-guard:guard
    amount:decimal
  )
  (deftable token-table:{token-schema})
)"#;

    /// The heron token (D3 gap group 4): its table is `coin-table`, with an `@model` that
    /// names `balance` before the field.
    const HERON: &str = r#"(module heron GOV
  (defschema coin-schema
    @doc "The coin contract token schema"
    @model [ (invariant (>= balance 0.0)) ]

    balance:decimal
    guard:guard)
  (defschema crosschain-schema
  @doc "Schema for yielded value in cross-chain transfers"
  receiver:string
  receiver-guard:guard
  amount:decimal
  source-chain:string)
  (deftable coin-table:{coin-schema})
)"#;

    /// `kaddex.kdx` (D3 gap group 4): six tables, one ledger.
    const KDX: &str = r#"(module kdx GOVERNANCE
  @model
    [ (defproperty conserves-mass
        (= (column-delta token-table 'balance) 0.0))
    ]
  (defschema token-schema
    @doc "The KDX contract token schema"
    @model [ (invariant (>= balance 0.0)) ]

    balance:decimal
    guard:guard)
  (defschema token-supply
      total-burned:decimal
      total-minted:decimal)
  (defschema privilege
    guard:guard)
  (defschema special-account
    account:string)
  (defschema mint-cap
    maximum:decimal)
  (defschema contract-lock-status
      lock:bool)
  (deftable token-table:{token-schema})
  (deftable supply-table:{token-supply})
  (deftable privileges:{privilege})
  (deftable special-accounts:{special-account})
  (deftable mint-cap-table:{mint-cap})
  (deftable contract-lock:{contract-lock-status})
)"#;

    #[test]
    fn the_ledger_table_of_real_tokens() {
        assert_eq!(ledger_tables(KAYC), ["token-table"]);
        assert_eq!(ledger_tables(HERON), ["coin-table"]);
        assert_eq!(ledger_tables(KDX), ["token-table"]);
    }

    /// `balance` only in a comment, a string or `@model` is no field; a table name that is
    /// not an identifier is never returned (it is written into code).
    #[test]
    fn only_real_balance_fields_count() {
        let code = r#"(module m G
  ; (defschema fake balance:decimal) (deftable fake-table:{fake})
  (defschema s1 @doc "balance:decimal (deftable t:{s1})" @model [ (invariant (>= balance 0.0)) ] guard:guard)
  (defschema s2 balance : decimal)
  (defschema s3 balance:decimal)
  (deftable t1:{s1})
  (deftable t2 : {s2})
  (deftable t3:{free.m.s3})
  (deftable t3:{s3})
)"#;
        assert_eq!(ledger_tables(code), ["t2", "t3"]);
        assert!(
            ledger_tables("(module m G (defschema s balance:decimal) (deftable \"x\":{s}))")
                .is_empty()
        );
        assert!(ledger_tables("").is_empty());
        assert!(ledger_tables("(defschema").is_empty());
    }
}
