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
//!   [`SWEEP_SECS`], and a token whose last listing failed on some chain after [`RETRY_SECS`].
//! - Per step, one `(token, chain)`: one `describe-module` read, then one `keys` read per
//!   ledger table. At most one step per [`STEP_EVERY`], inside the shared `/local` rate limit.
//!   Progress (`Meta balances_keylist/<module>`: when last listed, next chain, chains that
//!   failed) is kept per step, so a restart resumes.
//! - Marks: only keys we hold nothing for (no `Balances` row, no dirty or seed mark) are marked,
//!   through `balances::mark_row` (the postings' mark path, so the row caps apply) at the height
//!   the node read. The normal reconcile then reads them like any dirty row.
//! - Fail-closed: a listing over [`kdx_node::local::MAX_LISTED_KEYS`], an unusable answer, a
//!   module with no ledger table or a node error that repeats [`NODE_TRIES`] times marks nothing
//!   for that chain, is logged with the module and chain, and counts as failed (retried after
//!   [`RETRY_SECS`]). Totals are in `Meta balances_keylist_stats` (`reconcile-balances
//!   --status`).
//! - Refused (like the receivers pass, review L3) while the store has no tail yet or its
//!   backfill is incomplete: the listing interns account names.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use kdx_hot::balances::{self, bal_key, meta, TokenRow, KIND_NOT_FUNGIBLE};
use kdx_hot::{HotDict, HotStore, Table, Txn};
use kdx_node::local::{
    valid_ident, valid_module, Local, LocalClient, LocalError, MAX_LISTED_KEYS, MAX_NAME_LEN,
};
use kdx_types::{Dict, DictKind, DictMut};
use serde::{Deserialize, Serialize};

use crate::Ctx;

/// Seconds after which a listed token is listed again (the periodic sweep).
pub const SWEEP_SECS: u32 = 7 * 86_400;
/// Seconds after which a token whose last listing failed on some chain is listed again.
pub const RETRY_SECS: u32 = 86_400;
/// Least time between two steps (one `(token, chain)` each: 1 + tables `/local` reads).
pub const STEP_EVERY: Duration = Duration::from_secs(1);
/// How often the queue of due tokens is rebuilt from the store (newly tracked tokens join).
pub const REFRESH: Duration = Duration::from_secs(600);
/// Node errors in a row on one `(token, chain)` before it is given up as failed.
pub const NODE_TRIES: u32 = 3;
/// Pause after a node error before the same step is tried again.
pub const NODE_RETRY: Duration = Duration::from_secs(30);
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

/// One token's listing progress ([`meta::KEYLIST`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ListState {
    /// Unix seconds of the last complete listing; 0: never.
    pub listed_at: u32,
    /// Index (into the network's chains) of the next chain to list.
    pub next_chain: u8,
    /// Chains whose listing failed in this round (or the last, when between rounds).
    pub failed: u32,
}

impl ListState {
    pub fn encode(&self) -> [u8; 9] {
        let mut v = [0u8; 9];
        v[..4].copy_from_slice(&self.listed_at.to_be_bytes());
        v[4] = self.next_chain;
        v[5..].copy_from_slice(&self.failed.to_be_bytes());
        v
    }
    pub fn decode(v: &[u8]) -> Option<ListState> {
        if v.len() != 9 {
            return None;
        }
        Some(ListState {
            listed_at: u32::from_be_bytes(v[..4].try_into().ok()?),
            next_chain: v[4],
            failed: u32::from_be_bytes(v[5..].try_into().ok()?),
        })
    }
    /// Due for a listing at `now`: never listed, a round in progress, or older than
    /// [`SWEEP_SECS`] ([`RETRY_SECS`] when a chain failed).
    pub fn due(&self, now: u32) -> bool {
        let age = now.saturating_sub(self.listed_at);
        self.listed_at == 0
            || self.next_chain > 0
            || age >= SWEEP_SECS
            || (self.failed != 0 && age >= RETRY_SECS)
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
/// names pass the B-1 check.
pub fn due(txn: &impl Txn, dict: &impl Dict, now: u32) -> Result<Vec<(u32, String, ListState)>> {
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
        if !valid_module(name) {
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

/// `(listed, in progress, with a failed chain)` token counts of the key listing (`--status`).
pub fn summary(txn: &impl Txn) -> Result<(u64, u64, u64)> {
    let p = meta::KEYLIST.as_bytes();
    let (mut listed, mut running, mut failed) = (0, 0, 0);
    for (_, v) in txn.scan(Table::Meta, p, |k| k.starts_with(p))? {
        let Some(st) = ListState::decode(&v) else {
            continue;
        };
        listed += u64::from(st.listed_at > 0);
        running += u64::from(st.next_chain > 0);
        failed += u64::from(st.failed != 0);
    }
    Ok((listed, running, failed))
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
/// room left under `row_cap` (I-7: its `Balances` rows plus this listing's marks), so a hostile
/// token's long listing cannot grow the dictionary past what the cap allows.
pub fn mark_keys(
    hot: &HotStore,
    dict: &Mutex<HotDict>,
    module: u32,
    chain: u8,
    keys: &[String],
    height: u32,
    row_cap: u32,
) -> Result<Marked> {
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
        let mut room = u64::from(row_cap.saturating_sub(t.rows));
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

/// The loop's key listing: the queue of due tokens and the pacing.
#[derive(Default)]
pub struct KeyList {
    queue: VecDeque<(u32, String, ListState)>,
    refreshed: Option<Instant>,
    next_at: Option<Instant>,
    node_errors: u32,
}

/// How one `(token, chain)` went.
enum ChainResult {
    /// Listed (or the module is not on the chain).
    Done(Marked),
    /// Marked nothing; logged; the chain counts as failed.
    Failed(String),
    /// Node unreachable: try the same step again later.
    Retry(String),
}

fn now_unix() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as u32)
}

async fn list_chain(
    ctx: &Ctx,
    local: &LocalClient,
    caps: &crate::reconcile::Caps,
    module: u32,
    name: &str,
    chain: u8,
) -> Result<ChainResult> {
    let code = match local.module_code(chain, name).await {
        Ok(Local::Absent) => return Ok(ChainResult::Done(Marked::default())),
        Ok(Local::Found { value, .. }) => value,
        Err(LocalError::Node(e)) => return Ok(ChainResult::Retry(e.to_string())),
        Err(e) => return Ok(ChainResult::Failed(format!("describe-module: {e}"))),
    };
    let tables = ledger_tables(&code);
    if tables.is_empty() {
        return Ok(ChainResult::Failed("no table with a balance field".into()));
    }
    let mut lists = Vec::with_capacity(tables.len());
    for t in &tables {
        match local
            .table_keys(chain, name, t, MAX_KEYS_PER_LISTING.min(MAX_LISTED_KEYS))
            .await
        {
            Ok(Local::Found { value, height, .. }) => {
                lists.push((value, u32::try_from(height).unwrap_or(u32::MAX)))
            }
            Ok(Local::Absent) => {}
            Err(LocalError::Node(e)) => return Ok(ChainResult::Retry(e.to_string())),
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
            let m = mark_keys(&hot, &dict, module, chain, &keys, height, row_cap)?;
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
        kl.queue = due(&ro, &*d, now_unix())?.into();
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
    if st.next_chain == 0 {
        st.failed = 0;
    }
    let idx = usize::from(st.next_chain);
    let Some(&chain) = chains.get(idx) else {
        st.next_chain = 0;
        return finish(ctx, kl, module, st);
    };
    let res = list_chain(ctx, local, caps, module, &name, chain).await?;
    let mut txn = ctx.hot.rw()?;
    match res {
        ChainResult::Retry(e) => {
            kl.node_errors += 1;
            if kl.node_errors < NODE_TRIES {
                tracing::debug!(module = %name, chain, error = %e, "balances: key listing: node error, retried");
                kl.next_at = Some(now + NODE_RETRY);
                return Ok(true);
            }
            fail(&mut txn, &mut st, &name, chain, &format!("node: {e}"))?;
        }
        ChainResult::Failed(e) => fail(&mut txn, &mut st, &name, chain, &e)?,
        ChainResult::Done(m) => {
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
    st.next_chain = st.next_chain.saturating_add(1);
    if usize::from(st.next_chain) >= chains.len() {
        st.next_chain = 0;
        st.listed_at = now_unix();
        add_stats(&mut txn, |s| s.tokens_listed += 1)?;
        kl.queue.pop_front();
    } else if let Some(f) = kl.queue.front_mut() {
        f.2 = st;
    }
    txn.set_meta(&state_key(module), &st.encode())?;
    txn.commit()?;
    Ok(true)
}

/// A round with no chain left (the network's chains shrank): close it.
fn finish(ctx: &Ctx, kl: &mut KeyList, module: u32, mut st: ListState) -> Result<bool> {
    st.listed_at = now_unix();
    let mut txn = ctx.hot.rw()?;
    txn.set_meta(&state_key(module), &st.encode())?;
    txn.commit()?;
    kl.queue.pop_front();
    Ok(false)
}

fn fail(
    txn: &mut kdx_hot::RwTxn<'_>,
    st: &mut ListState,
    name: &str,
    chain: u8,
    why: &str,
) -> Result<()> {
    tracing::warn!(module = %name, chain, reason = %why,
        "balances: key listing failed; nothing marked for this chain, retried later");
    if chain < 32 {
        st.failed |= 1 << chain;
    }
    let cut: String = why.chars().take(200).collect();
    add_stats(txn, |s| {
        s.chain_failures += 1;
        s.last_failure = Some(format!("{name} chain {chain}: {cut}"));
    })
    .context("key listing stats")
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
    /// for an HTTP 500. Counts the `/local` calls.
    fn fake_node(answer: Arc<Answer>, calls: Arc<AtomicUsize>) -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        std::thread::spawn(move || {
            for s in l.incoming() {
                let Ok(mut s) = s else { continue };
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
                let req: Value = serde_json::from_slice(&buf[head_end..]).unwrap_or(Value::Null);
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
                let (status, body) = match answer(chain, code, &data) {
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
            graph_history: vec![(0, vec![0, 1])],
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
        let st = ListState {
            listed_at: 1_000_000,
            next_chain: 3,
            failed: 1 << 19,
        };
        assert_eq!(ListState::decode(&st.encode()), Some(st));
        assert_eq!(ListState::decode(&[0; 8]), None);
        let now = 2_000_000;
        assert!(ListState::default().due(now), "never listed");
        let done = |at, failed| ListState {
            listed_at: at,
            next_chain: 0,
            failed,
        };
        assert!(!done(now - 10, 0).due(now));
        assert!(done(now - SWEEP_SECS, 0).due(now), "the periodic sweep");
        assert!(!done(now - RETRY_SECS + 1, 1).due(now));
        assert!(
            done(now - RETRY_SECS, 1).due(now),
            "a failed chain is retried sooner"
        );
        assert!(
            ListState {
                next_chain: 1,
                ..done(now - 10, 0)
            }
            .due(now),
            "round in progress"
        );
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
            next_chain: 0,
            failed: 0,
        };
        txn.set_meta(&state_key(ids[0]), &old.encode()).unwrap();
        txn.commit().unwrap();
        let d = ctx.dict.lock().unwrap();
        let names: Vec<String> = due(&ctx.hot.ro().unwrap(), &*d, 100 + SWEEP_SECS)
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
        let m = mark_keys(&ctx.hot, &ctx.dict, shatter, 0, &keys, 7_429_654, 100).unwrap();
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
        let again = mark_keys(&ctx.hot, &ctx.dict, shatter, 0, &keys[..4], 7_429_700, 100).unwrap();
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
        let m = mark_keys(&ctx.hot, &ctx.dict, ids[0], 0, &keys, 10, 100).unwrap();
        assert_eq!((m.marked, m.skipped), (2, 9_998));
        assert_eq!(accounts(), before + 2);
        for &full in &ids[1..] {
            let m = mark_keys(&ctx.hot, &ctx.dict, full, 0, &keys, 10, 100).unwrap();
            assert_eq!((m.marked, m.skipped), (0, 10_000));
        }
        assert_eq!(accounts(), before + 2);
        let hot = ctx.hot.ro().unwrap();
        assert_eq!(hot.count(Table::BalanceDirty).unwrap(), 2);
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
                st.listed_at > 0 && st.next_chain == 0 && st.failed == 0,
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
        assert_eq!(summary(&ro).unwrap(), (2, 0, 0));
        // Listed: not due again before the sweep.
        drop(ro);
        kl.refreshed = None;
        kl.next_at = None;
        assert!(!step(&ctx, &local, &Caps::default(), &mut kl).await.unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 6);
    }

    /// Fail-closed: a refused `keys` read, a module with no ledger table and a node that stays
    /// down mark nothing, are logged and counted, and leave the chain failed (retried after
    /// `RETRY_SECS`); the other chain of the same token still lists.
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
        assert_eq!(
            st.iter().map(|s| s.failed).collect::<Vec<_>>(),
            [0b01, 0b11, 0b11]
        );
        assert!(st.iter().all(|s| s.listed_at > 0));
        let now = st[0].listed_at;
        assert!(!st[0].due(now + RETRY_SECS - 1) && st[0].due(now + RETRY_SECS));
        let stats: Stats =
            serde_json::from_slice(&ro.meta(meta::KEYLIST_STATS).unwrap().unwrap()).unwrap();
        assert_eq!(stats.chain_failures, 5);
        assert_eq!(summary(&ro).unwrap(), (3, 0, 3));
        assert!(stats
            .last_failure
            .unwrap()
            .starts_with("free.down chain 1: node"));
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
