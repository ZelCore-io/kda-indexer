//! Balance reconciler (spec §3.9, plan Task 9, rulings B-1…B-5). Runs only inside `run` (the
//! writer process).
//!
//! - First, when `Meta balances_seeded` is absent, the seed ([`crate::seed`]) runs (B-5).
//! - Then, forever: take a batch of dirty keys ([`refill`], [`ReconcileOpts::batch`] keys):
//!   tail marks first (`coin` first, then a fair share per module, I-7), the seed backlog
//!   (`BalanceSeed`) only behind them (R-P5P9-8 #1); call `{module}.details` for each through
//!   [`LocalClient`] (its own token bucket and concurrency cap); write `Balances`, `Guards`,
//!   `KeyAccounts` and `Tokens` in one write txn per batch.
//! - Trust (I-6): a guard feeds `KeyAccounts` only for `coin` and for a module whose
//!   `describe-module` lists `fungible-v2` on the row's chain (checked once per module and
//!   chain, re-checked after [`Caps::recheck_secs`]); keyset references are resolved with
//!   `describe-keyset` only for those. Other modules' balances are stored and served, their
//!   guards never index a key.
//! - Not fungible (R-P5P9-8 #2): a module whose `details` calls all fail in
//!   [`NOT_FUNGIBLE_AFTER`] batches in a row is marked; the tail stops marking it, its keys
//!   are dropped without a call (`result="not_fungible"`), `/tokens` hides it; after
//!   [`Caps::recheck_secs`] it is active again.
//! - Caps (I-7): a non-coin module stops getting new rows at [`Caps::module_rows`] (the tail
//!   then marks no new key of it) and an account at [`Caps::account_rows`]
//!   (`result="capped"`).
//! - A dirty key is deleted only when its mark, read again in the write txn, is still the exact
//!   [`Mark`] (height and write sequence) read at refill, and its height is not above the
//!   answer's `as_of` (SR-1, M-3): a mark raised or rewritten meanwhile (a new block, a reorg's
//!   undo or re-apply) survives. Every arm that drops a mark (skipped, error, parked) follows
//!   the same rule (I-5). A success's `as_of` is the tip it read (`metaData.blockHeight − 1`); an
//!   absent answer carries
//!   no height, so its `as_of` is the chain's tail height read before the call (the node is at
//!   or past it).
//! - Outcomes (`kdx_balance_reconcile_total{result}`):
//!   - `ok`, `absent`: row written.
//!   - `skipped`: B-1 refused the call (module name, name length); the key is dropped.
//!   - `error`, Pact failure or unusable answer (the node evaluated the code and it failed,
//!     e.g. a module with no `details`; or the answer is not JSON, over the body cap, or has no
//!     decimal balance or `metaData`): the key is dropped, and an existing row keeps its
//!     balance flagged `unverified` (served `stale: true`, I-5). The next tail mark retries
//!     it. No back-off.
//!   - `error`, node error (transport or HTTP): the key stays dirty and its mark moves up to
//!     the chain's tail height (back of the queue). The loop backs off exponentially up to
//!     [`BACKOFF_MAX`] only when node errors are at least half of a batch's calls
//!     ([`backs_off`]). A key with [`MAX_KEY_RETRIES`] node errors in a row in batches that
//!     did not back off is parked (`parked`: mark dropped until the next tail mark, row
//!     flagged `unverified`). A backed-off batch counts nothing and resets the counts (SR-2):
//!     a node outage parks no key. Never read as a zero balance.
//! - Heartbeat: `Meta balances_reconcile_stats` (JSON: unix time, rows done, rate, dirty) per
//!   batch; not part of the systemd watchdog.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures::future::join_all;
use kdx_hot::balances::{
    self, clear_mark, guard_hash, guard_keys, key_account_key, key_hash, ref_hash, set_unverified,
    stored_guard_keys, BalanceRow, GuardKeys, Mark, TokenRow, KIND_FUNGIBLE, KIND_NOT_FUNGIBLE,
};
use kdx_hot::{checkpoint, HotStore, RwTxn, Stage, Table, Txn};
use kdx_node::local::{Details, Local, LocalClient, LocalError, LocalOpts};
use kdx_node::NodeClient;
use kdx_types::DictKind;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::Ctx;

/// Consecutive node errors after which a key is parked: its dirty mark is dropped (counted as
/// `result="parked"`) until the next tail mark of the row.
pub const MAX_KEY_RETRIES: u32 = 5;
/// Keys (and modules) whose errors are counted at once (the map is cleared beyond this).
const MAX_TRACKED: usize = 100_000;
/// Batches in a row in which every `details` call of a module failed (and none answered) after
/// which the module is marked not fungible (R-P5P9-8 #2).
pub const NOT_FUNGIBLE_AFTER: u32 = 3;
/// Seed rows read per refill, per batch slot.
const SEED_WINDOW: usize = 16;
/// `BalanceDirty` rows per read txn during a refill.
const SCAN_CHUNK: usize = 100_000;
const IDLE: Duration = Duration::from_secs(2);
pub const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// How often the loop looks for not-fungible modules due for a re-check.
const RECHECK_SCAN: Duration = Duration::from_secs(3600);
/// `Meta` key of the reconciler heartbeat.
pub const META_STATS: &str = "balances_reconcile_stats";

/// Queue fairness and growth limits (I-7, R-P5P9-8 #2).
#[derive(Debug, Clone, Copy)]
pub struct Caps {
    /// Most dirty keys of one non-coin module per batch.
    pub module_share: usize,
    /// Most `Balances` rows of one non-coin module. Past it, a key with no row is dropped
    /// (`result="capped"`) and the tail marks no new key of the module.
    pub module_rows: u32,
    /// Most `Balances` rows of one account; past it, a new non-coin row is dropped (`capped`).
    pub account_rows: u32,
    /// Seconds after which a module's fungible checks and a not-fungible verdict are re-done.
    pub recheck_secs: u32,
}

impl Default for Caps {
    fn default() -> Self {
        Caps {
            module_share: 32,
            module_rows: 1_000_000,
            account_rows: 1_000,
            recheck_secs: 86_400,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReconcileOpts {
    pub local: LocalOpts,
    /// Dirty keys per batch (one write txn).
    pub batch: usize,
    /// Run the seed first when `Meta balances_seeded` is absent (B-5).
    pub seed: bool,
    pub caps: Caps,
}

/// One dirty key taken for a batch: a tail mark (`BalanceDirty`) or a seed mark
/// (`BalanceSeed`), with the mark as read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dirty {
    pub key: [u8; 9],
    pub mark: Mark,
    pub seed: bool,
}

impl Dirty {
    fn table(&self) -> Table {
        if self.seed {
            Table::BalanceSeed
        } else {
            Table::BalanceDirty
        }
    }
}

type Heap = BinaryHeap<(Mark, [u8; 9])>;

/// Keep the `cap` oldest entries (height, then write sequence, then key).
fn keep(h: &mut Heap, e: (Mark, [u8; 9]), cap: usize) {
    h.push(e);
    if h.len() > cap {
        h.pop();
    }
}

fn module_of(key: &[u8; 9]) -> u32 {
    u32::from_be_bytes([key[4], key[5], key[6], key[7]])
}

/// Round-robin over modules, the module with the oldest key first, until `out` holds `n`.
fn fair(out: &mut Vec<Dirty>, by_module: HashMap<u32, Heap>, n: usize, seed: bool) {
    let mut lists: Vec<Vec<(Mark, [u8; 9])>> = by_module
        .into_values()
        .map(BinaryHeap::into_sorted_vec)
        .filter(|l| !l.is_empty())
        .collect();
    lists.sort_by_key(|l| l[0]);
    let mut i = 0;
    while out.len() < n {
        let mut any = false;
        for l in &lists {
            if out.len() >= n {
                break;
            }
            if let Some(&(mark, key)) = l.get(i) {
                out.push(Dirty { key, mark, seed });
                any = true;
            }
        }
        if !any {
            break;
        }
        i += 1;
    }
}

/// The next batch of up to `n` dirty keys (R-P5P9-8 #1, I-7), in this order:
/// 1. tail marks of `coin`, oldest first;
/// 2. tail marks of the other modules, round-robin over modules, at most `share` each;
/// 3. only when 1 and 2 leave room, the seed backlog (the first `SEED_WINDOW × n` rows of
///    `BalanceSeed`): `coin` first, then the other modules as in 2.
///
/// Tail marks are all of `BalanceDirty` (what changed since its last reconcile); every batch
/// takes a fresh refill, so a new `coin` mark waits at most one batch, whatever other modules
/// mark.
pub fn refill(hot: &HotStore, n: usize, share: usize) -> Result<Vec<Dirty>> {
    let share = share.max(1);
    let coin = balances::coin_module(&hot.ro()?)?;
    let mut coin_rows = Heap::new();
    let mut by_module: HashMap<u32, Heap> = HashMap::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let rows =
            hot.ro()?
                .scan_chunk(Table::BalanceDirty, after.as_deref(), SCAN_CHUNK, |_| true)?;
        for (k, v) in &rows {
            let (Ok(key), Some(m)) = (<[u8; 9]>::try_from(k.as_slice()), Mark::decode(v)) else {
                continue;
            };
            let module = module_of(&key);
            if Some(module) == coin {
                keep(&mut coin_rows, (m, key), n);
            } else {
                keep(by_module.entry(module).or_default(), (m, key), share);
            }
        }
        match rows.last() {
            Some((k, _)) if rows.len() == SCAN_CHUNK => after = Some(k.clone()),
            _ => break,
        }
    }
    let mut out: Vec<Dirty> = coin_rows
        .into_sorted_vec()
        .into_iter()
        .map(|(mark, key)| Dirty {
            key,
            mark,
            seed: false,
        })
        .collect();
    fair(&mut out, by_module, n, false);
    if out.len() < n {
        let rows = hot.ro()?.scan_limit(
            Table::BalanceSeed,
            &[],
            n.saturating_mul(SEED_WINDOW),
            |_| true,
        )?;
        let mut by_module: HashMap<u32, Heap> = HashMap::new();
        for (k, v) in &rows {
            let (Ok(key), Some(mark)) = (<[u8; 9]>::try_from(k.as_slice()), Mark::decode(v)) else {
                continue;
            };
            if Some(module_of(&key)) == coin {
                if out.len() < n {
                    out.push(Dirty {
                        key,
                        mark,
                        seed: true,
                    });
                }
            } else {
                keep(
                    by_module.entry(module_of(&key)).or_default(),
                    (mark, key),
                    share,
                );
            }
        }
        fair(&mut out, by_module, n, true);
    }
    Ok(out)
}

/// One dirty key with what the call needs.
struct Item {
    d: Dirty,
    account: u32,
    module: u32,
    chain: u8,
    account_name: Option<String>,
    module_name: Option<String>,
    /// Tail height of the chain before the call (`as_of` of an absent answer).
    tail: u32,
    /// The guard feeds `KeyAccounts` (I-6): `coin`, or `fungible-v2` on this chain.
    trusted: bool,
    /// The module's fungible check on this chain is missing or due.
    check: bool,
    /// No call: `not_fungible` (R-P5P9-8 #2) or `capped` (I-7, module at its cap, no row).
    skip: Option<&'static str>,
}

enum Outcome {
    Found(Details, u32, Option<(String, Option<Value>)>),
    Absent(u32),
    Skipped,
    Failure,
    NodeError,
    Dropped(&'static str),
}

impl Outcome {
    fn label(&self) -> &'static str {
        match self {
            Outcome::Found(..) => "ok",
            Outcome::Absent(_) => "absent",
            Outcome::Skipped => "skipped",
            Outcome::Failure | Outcome::NodeError => "error",
            Outcome::Dropped(l) => l,
        }
    }
}

fn now_unix() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .unwrap_or(0)
}

fn tail_height(txn: &impl Txn, chain: u8) -> Result<u32> {
    Ok(checkpoint(txn, Stage::Tail, chain)?
        .map(|(h, _)| h)
        .unwrap_or(txn.frozen_frontier()?.saturating_sub(1)))
}

fn dict_name(txn: &impl Txn, k: DictKind, id: u32) -> Result<Option<String>> {
    Ok(txn
        .get(Table::DictRev(k), &id.to_be_bytes())?
        .and_then(|v| String::from_utf8(v).ok()))
}

/// Call the node for one item; resolve a keyset reference on the same chain, only for a
/// trusted module (I-6: an unconfirmed module's guard is never indexed).
async fn call(local: &LocalClient, it: &Item) -> Outcome {
    if let Some(l) = it.skip {
        let out = Outcome::Dropped(l);
        kdx_metrics::get().balance_reconcile(out.label(), 0.0);
        return out;
    }
    let (Some(acct), Some(module)) = (&it.account_name, &it.module_name) else {
        return Outcome::Skipped;
    };
    let (r, secs) = local.details_timed(it.chain, module, acct).await;
    let out = match r {
        Ok(Local::Found { value, height }) => {
            let as_of = u32::try_from(height).unwrap_or(u32::MAX);
            let resolved = match guard_keys(&value.guard) {
                GuardKeys::Ref(name) if it.trusted => {
                    match local.describe_keyset(it.chain, &name).await {
                        Ok(Local::Found { value: ks, .. }) => Some((name, Some(ks))),
                        Ok(Local::Absent) => Some((name, None)),
                        Err(e) => {
                            tracing::debug!(error = %e, "describe-keyset failed; keys left as cached");
                            None
                        }
                    }
                }
                _ => None,
            };
            Outcome::Found(value, as_of, resolved)
        }
        Ok(Local::Absent) => Outcome::Absent(it.tail),
        Err(LocalError::Skipped(_)) => Outcome::Skipped,
        Err(LocalError::Failure(m)) => {
            tracing::debug!(module = %module, chain = it.chain, message = %m, "details failed");
            Outcome::Failure
        }
        Err(LocalError::Node(e)) => {
            tracing::debug!(error = %e, "details: node error, kept dirty");
            Outcome::NodeError
        }
    };
    kdx_metrics::get().balance_reconcile(out.label(), secs);
    out
}

/// Whether another trusted row of `account` (not `except`) has `pk` in its guard. Bounded by
/// the account's row cap (I-7).
fn account_has_key(
    txn: &impl Txn,
    account: u32,
    pk: &str,
    except: (u32, u8),
    coin: Option<u32>,
) -> Result<bool> {
    for (m, c, row) in balances::account_rows(txn, account)? {
        if (m, c) == except || row.absent || !balances::trusted(txn, m, c, coin)? {
            continue;
        }
        if stored_guard_keys(txn, &row.guard, c)?
            .iter()
            .any(|k| k == pk)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether a new row may be written (I-7). Always for a trusted row (`coin`, or a module
/// confirmed `fungible-v2` on this chain) and for a key that has a row. An untrusted module
/// stops at [`Caps::module_rows`] (and is flagged, so the tail stops marking new keys of it),
/// and an account takes at most [`Caps::account_rows`] untrusted rows: absent rows, rows of
/// not-fungible modules and trusted rows do not count, so dust from spam modules cannot fill
/// an account's cap and keep a real token's row out (security review ruling).
fn admit(txn: &mut RwTxn<'_>, it: &Item, coin: Option<u32>, caps: &Caps) -> Result<bool> {
    if it.trusted || Some(it.module) == coin || txn.get(Table::Balances, &it.d.key)?.is_some() {
        return Ok(true);
    }
    let t = balances::token_row(txn, it.module)?.unwrap_or_default();
    if t.rows >= caps.module_rows {
        if !t.capped() {
            balances::update_token(txn, it.module, |t| t.flags |= balances::TOKEN_CAPPED)?;
            tracing::warn!(
                module = it.module_name.as_deref().unwrap_or(""),
                rows = t.rows,
                "balances: module reached its row cap; new keys are dropped"
            );
        }
        return Ok(false);
    }
    let p = it.account.to_be_bytes();
    let cap = caps.account_rows as usize;
    let mut counted = 0usize;
    let mut tokens: HashMap<u32, Option<TokenRow>> = HashMap::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        // The first read starts at the account's prefix, the next ones after the last key.
        let rows = match &after {
            None => txn.scan_limit(Table::Balances, &p, 1024, |k| k.starts_with(&p))?,
            Some(a) => txn.scan_chunk(Table::Balances, Some(a), 1024, |k| k.starts_with(&p))?,
        };
        for (k, v) in &rows {
            let (Some((_, m, c)), Some(r)) = (balances::parse_bal_key(k), BalanceRow::decode(v))
            else {
                continue;
            };
            if r.absent || Some(m) == coin {
                continue;
            }
            let t = match tokens.get(&m) {
                Some(t) => *t,
                None => {
                    let t = balances::token_row(txn, m)?;
                    tokens.insert(m, t);
                    t
                }
            };
            if t.is_some_and(|t| t.kind == KIND_NOT_FUNGIBLE || t.fungible_on(c)) {
                continue;
            }
            counted += 1;
            if counted >= cap {
                return Ok(false);
            }
        }
        match rows.last() {
            Some((k, _)) if rows.len() == 1024 => after = Some(k.clone()),
            _ => return Ok(true),
        }
    }
}

/// Write one answer: row, guard keys (trusted modules only, I-6), holders, row count.
fn write_row(txn: &mut RwTxn<'_>, it: &Item, new: BalanceRow, coin: Option<u32>) -> Result<()> {
    let old = txn
        .get(Table::Balances, &it.d.key)?
        .and_then(|v| BalanceRow::decode(&v));
    let old_keys = match &old {
        Some(o) if !o.absent => stored_guard_keys(txn, &o.guard, it.chain)?,
        _ => vec![],
    };
    let new_keys = if new.absent || !it.trusted {
        vec![]
    } else {
        stored_guard_keys(txn, &new.guard, it.chain)?
    };
    match (
        old.as_ref().is_some_and(BalanceRow::positive),
        new.positive(),
    ) {
        (false, true) => balances::adjust_holders(txn, it.module, 1)?,
        (true, false) => balances::adjust_holders(txn, it.module, -1)?,
        _ => {}
    }
    if old.is_none() {
        balances::update_token(txn, it.module, |t| t.rows = t.rows.saturating_add(1))?;
    }
    txn.put(Table::Balances, &it.d.key, &new.encode())?;
    for pk in &new_keys {
        txn.put(
            Table::KeyAccounts,
            &key_account_key(&key_hash(pk), it.account),
            &[],
        )?;
    }
    let keep: HashSet<&String> = new_keys.iter().collect();
    for pk in old_keys.iter().filter(|k| !keep.contains(k)) {
        if !account_has_key(txn, it.account, pk, (it.module, it.chain), coin)? {
            txn.del(
                Table::KeyAccounts,
                &key_account_key(&key_hash(pk), it.account),
            )?;
        }
    }
    Ok(())
}

/// Delete the dirty key when it still holds the mark read at refill and that mark is not above
/// `as_of` (SR-1, M-3).
fn clear_if_covered(txn: &mut RwTxn<'_>, it: &Item, as_of: u32) -> Result<()> {
    if it.d.mark.height <= as_of {
        clear_mark(txn, it.d.table(), &it.d.key, it.d.mark)?;
    }
    Ok(())
}

/// Drop the mark of a call that gave no usable answer, only when it is still the mark read at
/// refill (I-5); an existing row is flagged unverified so the route serves it stale.
fn drop_mark(txn: &mut RwTxn<'_>, it: &Item) -> Result<()> {
    if clear_mark(txn, it.d.table(), &it.d.key, it.d.mark)? {
        set_unverified(txn, &it.d.key)?;
    }
    Ok(())
}

#[derive(Debug, Default, Clone)]
pub struct BatchStats {
    pub ok: u64,
    pub absent: u64,
    pub skipped: u64,
    pub failures: u64,
    pub node_errors: u64,
    /// Keys dropped without a call or a row (`not_fungible`, `capped`).
    pub dropped: u64,
    /// Keys whose call hit a node (transport or HTTP) error, with the mark the batch wrote
    /// for them (moved up to the tail height); still dirty.
    pub node_error_keys: Vec<([u8; 9], Mark)>,
    /// Modules with an answer (`ok` or `absent`) in this batch.
    pub answered: HashSet<u32>,
    /// Non-coin modules with a failed `details` call in this batch.
    pub failed_modules: HashSet<u32>,
}

impl BatchStats {
    /// Calls the node answered or failed (B-1 refusals and drops make none).
    pub fn calls(&self) -> u64 {
        self.ok + self.absent + self.failures + self.node_errors
    }
}

/// Back off only when node errors are at least half of the batch's calls: the node itself is
/// failing. A minority of failing keys is a property of those keys (they go to the back of the
/// queue, and [`MAX_KEY_RETRIES`] parks them), and must not slow every other key.
pub fn backs_off(st: &BatchStats) -> bool {
    st.node_errors > 0 && st.node_errors * 2 >= st.calls()
}

/// Reconcile one batch of dirty keys.
pub async fn reconcile_batch(
    ctx: &Ctx,
    local: &LocalClient,
    keys: &[Dirty],
    caps: &Caps,
) -> Result<BatchStats> {
    let now = now_unix();
    let (coin, mut items) = {
        let txn = ctx.hot.ro()?;
        let coin = balances::coin_module(&txn)?;
        let mut tails = HashMap::new();
        let mut tokens: HashMap<u32, Option<TokenRow>> = HashMap::new();
        let mut v = Vec::with_capacity(keys.len());
        for d in keys {
            let (account, module, chain) =
                balances::parse_bal_key(&d.key).context("bad BalanceDirty key")?;
            let tail = match tails.get(&chain) {
                Some(&t) => t,
                None => {
                    let t = tail_height(&txn, chain)?;
                    tails.insert(chain, t);
                    t
                }
            };
            let t = match tokens.get(&module) {
                Some(t) => *t,
                None => {
                    let t = balances::token_row(&txn, module)?;
                    tokens.insert(module, t);
                    t
                }
            };
            let is_coin = Some(module) == coin;
            let skip = match t {
                Some(t) if !is_coin && t.kind == KIND_NOT_FUNGIBLE => Some("not_fungible"),
                Some(t)
                    if !is_coin && t.capped() && txn.get(Table::Balances, &d.key)?.is_none() =>
                {
                    Some("capped")
                }
                _ => None,
            };
            let bit = 1u32.checked_shl(u32::from(chain)).unwrap_or(0);
            v.push(Item {
                d: *d,
                account,
                module,
                chain,
                account_name: dict_name(&txn, DictKind::Account, account)?,
                module_name: dict_name(&txn, DictKind::Module, module)?,
                tail,
                trusted: is_coin || t.is_some_and(|t| t.fungible_on(chain)),
                check: !is_coin
                    && skip.is_none()
                    && t.is_none_or(|t| {
                        t.checked_chains & bit == 0
                            || now >= t.checked_at.saturating_add(caps.recheck_secs)
                    }),
                skip,
            });
        }
        (coin, v)
    };

    // I-6: the fungible check of each (module, chain) that needs one, before the calls.
    let mut pairs: Vec<(u32, u8, String)> = items
        .iter()
        .filter(|it| it.check)
        .filter_map(|it| Some((it.module, it.chain, it.module_name.clone()?)))
        .collect();
    pairs.sort_unstable();
    pairs.dedup();
    let answers = join_all(
        pairs
            .iter()
            .map(|(_, c, m)| local.implements_fungible(*c, m)),
    )
    .await;
    let mut verdict: HashMap<(u32, u8), bool> = HashMap::new();
    for ((m, c, _), r) in pairs.iter().zip(answers) {
        match r {
            Ok(Local::Found { value, .. }) => {
                verdict.insert((*m, *c), value);
            }
            Ok(Local::Absent) | Err(LocalError::Failure(_)) => {
                verdict.insert((*m, *c), false);
            }
            Err(LocalError::Node(_) | LocalError::Skipped(_)) => {}
        }
    }
    for it in &mut items {
        if let Some(&v) = verdict.get(&(it.module, it.chain)) {
            it.trusted = v;
        }
    }

    // In-flight calls are capped by the client's semaphore (`local_concurrency`).
    let outcomes: Vec<Outcome> = join_all(items.iter().map(|it| call(local, it))).await;

    let mut st = BatchStats::default();
    let mut txn = ctx.hot.rw()?;
    for (&(m, c), &ok) in &verdict {
        balances::update_token(&mut txn, m, |t| {
            if now >= t.checked_at.saturating_add(caps.recheck_secs) {
                t.checked_chains = 0;
                t.checked_at = now;
            }
            t.checked_chains |= 1 << c;
            if ok {
                t.fungible_chains |= 1 << c;
            } else {
                t.fungible_chains &= !(1 << c);
            }
        })?;
    }
    for (it, out) in items.iter().zip(outcomes) {
        match out {
            Outcome::Found(..) | Outcome::Absent(_) if !admit(&mut txn, it, coin, caps)? => {
                clear_mark(&mut txn, it.d.table(), &it.d.key, it.d.mark)?;
                kdx_metrics::get().balance_reconcile("capped", 0.0);
                st.dropped += 1;
            }
            Outcome::Found(d, as_of, resolved) => {
                let gj = serde_json::to_vec(&d.guard)?;
                let gh = guard_hash(&gj);
                txn.insert(Table::Guards, &gh, &gj)?;
                if let Some((name, ks)) = resolved {
                    let rk = ref_hash(it.chain, &name);
                    match ks {
                        Some(ks) => txn.put(Table::Guards, &rk, &serde_json::to_vec(&ks)?)?,
                        None => {
                            txn.del(Table::Guards, &rk)?;
                        }
                    }
                }
                let row = BalanceRow {
                    absent: false,
                    balance: d.balance,
                    guard: gh,
                    as_of,
                    reconciled_at: now,
                    unverified: false,
                };
                write_row(&mut txn, it, row, coin)?;
                clear_if_covered(&mut txn, it, as_of)?;
                st.ok += 1;
                st.answered.insert(it.module);
            }
            Outcome::Absent(as_of) => {
                let row = BalanceRow {
                    absent: true,
                    balance: String::new(),
                    guard: [0; 16],
                    as_of,
                    reconciled_at: now,
                    unverified: false,
                };
                write_row(&mut txn, it, row, coin)?;
                clear_if_covered(&mut txn, it, as_of)?;
                st.absent += 1;
                st.answered.insert(it.module);
            }
            Outcome::Skipped => {
                drop_mark(&mut txn, it)?;
                st.skipped += 1;
            }
            Outcome::Failure => {
                drop_mark(&mut txn, it)?;
                st.failures += 1;
                if Some(it.module) != coin {
                    st.failed_modules.insert(it.module);
                }
            }
            Outcome::NodeError => {
                // To the back of the queue: the mark moves up to the tail height (never down)
                // and out of the seed backlog.
                let m = balances::mark_dirty(&mut txn, it.account, it.module, it.chain, it.tail)?;
                st.node_errors += 1;
                st.node_error_keys.push((it.d.key, m));
            }
            Outcome::Dropped(_) => {
                clear_mark(&mut txn, it.d.table(), &it.d.key, it.d.mark)?;
                st.dropped += 1;
            }
        }
    }
    txn.commit()?;
    Ok(st)
}

/// Count batches in a row in which every `details` call of a module failed (R-P5P9-8 #2); an
/// answer resets the count. Returns (and forgets) the modules that reached
/// [`NOT_FUNGIBLE_AFTER`].
pub fn note_failures(fails: &mut HashMap<u32, u32>, st: &BatchStats) -> Vec<u32> {
    for m in &st.answered {
        fails.remove(m);
    }
    if fails.len() > MAX_TRACKED {
        fails.clear();
    }
    let mut out = Vec::new();
    for m in st.failed_modules.difference(&st.answered) {
        let n = fails.entry(*m).or_insert(0);
        *n += 1;
        if *n >= NOT_FUNGIBLE_AFTER {
            fails.remove(m);
            out.push(*m);
        }
    }
    out.sort_unstable();
    out
}

/// Mark modules not fungible (R-P5P9-8 #2): the tail stops marking them, their dirty keys are
/// dropped without a call and `/tokens` hides them until [`recheck_not_fungible`]. A module
/// that passed the fungible check on some chain is never marked.
pub fn mark_not_fungible(hot: &HotStore, modules: &[u32]) -> Result<()> {
    let now = now_unix();
    let mut txn = hot.rw()?;
    for &m in modules {
        balances::update_token(&mut txn, m, |t| {
            if t.fungible_chains == 0 {
                t.kind = KIND_NOT_FUNGIBLE;
                t.checked_at = now;
            }
        })?;
    }
    txn.commit()
}

/// Make the not-fungible modules whose verdict is at least `recheck_secs` old active again:
/// their next marks are read, and [`NOT_FUNGIBLE_AFTER`] failing batches mark them again.
/// Returns how many.
pub fn recheck_not_fungible(hot: &HotStore, now: u32, recheck_secs: u32) -> Result<usize> {
    let due: Vec<u32> = hot
        .ro()?
        .scan(Table::Tokens, &[], |_| true)?
        .into_iter()
        .filter_map(|(k, v)| {
            let t = TokenRow::decode(&v)?;
            (t.kind == KIND_NOT_FUNGIBLE && now >= t.checked_at.saturating_add(recheck_secs))
                .then_some(u32::from_be_bytes(<[u8; 4]>::try_from(k.as_slice()).ok()?))
        })
        .collect();
    if due.is_empty() {
        return Ok(0);
    }
    let mut txn = hot.rw()?;
    for &m in &due {
        balances::update_token(&mut txn, m, |t| {
            t.kind = KIND_FUNGIBLE;
            t.checked_at = now;
            t.checked_chains = 0;
        })?;
    }
    txn.commit()?;
    Ok(due.len())
}

/// Run (or resume) the balance seed (B-2). Returns false when stopped.
async fn seed_pass(ctx: &Arc<Ctx>, stop: &CancellationToken) -> Result<bool> {
    tracing::info!("balances: no seed yet, seeding (B-2)");
    let (c, s) = (ctx.clone(), stop.clone());
    let rep = tokio::task::spawn_blocking(move || {
        crate::seed::run(&c.hot, &c.segdir, &|| s.is_cancelled())
    })
    .await??;
    tracing::info!(report = %serde_json::to_string(&rep)?, "balances: seed pass ended");
    Ok(!rep.stopped)
}

/// M-11: the tail applied blocks without dirty marks (`[balances] enabled = false`, or an
/// older binary on this store). Clears the gap flag and the seed's done flag and cursor, so the
/// seed marks every known row again (behind the tail marks). Returns whether there was a gap.
pub fn take_mark_gap(hot: &HotStore) -> Result<bool> {
    use kdx_hot::balances::meta;
    if hot.ro()?.meta(meta::MARK_GAP)?.is_none() {
        return Ok(false);
    }
    let mut txn = hot.rw()?;
    for k in [
        meta::MARK_GAP,
        meta::BALANCES_SEEDED,
        meta::BALANCES_SEED_CURSOR,
    ] {
        txn.del(Table::Meta, k.as_bytes())?;
    }
    txn.commit()?;
    Ok(true)
}

fn gap_warn() {
    tracing::warn!(
        "balances: blocks were applied without dirty marks (balances off, or an older \
         binary); re-running the seed (M-11)"
    );
}

/// Pause before a failed NFT history fill is tried again (M-8).
const NFT_FILL_RETRY: Duration = Duration::from_secs(3600);

/// The NFT history fill for the trusted ledgers not walked yet on this store (all of them on a
/// new store; a ledger added to `[nft] ledgers` since, M-9). Returns false when stopped.
async fn nft_fill_pass(ctx: &Arc<Ctx>, stop: &CancellationToken) -> Result<bool> {
    let pending = crate::nft_fill::pending_ledgers(&ctx.hot.ro()?)?;
    if pending.is_empty() {
        return Ok(true);
    }
    tracing::info!(ledgers = ?pending, "nft: history fill (N-1, M-9)");
    let (c, s) = (ctx.clone(), stop.clone());
    let rep = tokio::task::spawn_blocking(move || {
        crate::nft_fill::run(&c.hot, &c.segdir, Some(&pending), &|| s.is_cancelled())
    })
    .await??;
    tracing::info!(report = %serde_json::to_string(&rep)?, "nft: fill pass ended");
    Ok(!rep.stopped)
}

/// M-8: NFT rows are a "should"; a failed fill is logged and retried after `retry`
/// ([`NFT_FILL_RETRY`]), while the balance seed and reconcile ("must") go on.
fn nft_failed(e: &anyhow::Error, retry: Duration) {
    tracing::error!(
        error = format!("{e:#}"),
        retry_secs = retry.as_secs(),
        "nft: history fill failed; balances go on, the fill is retried later"
    );
}

/// Write the heartbeat and the dirty gauge.
fn heartbeat(ctx: &Ctx, done: u64, rate: f64) -> Result<usize> {
    let dirty = balances::dirty_count(&ctx.hot.ro()?)?;
    kdx_metrics::get().balance_dirty_rows.set(dirty as i64);
    let s = json!({"t": now_unix(), "done": done, "rate": rate, "dirty": dirty});
    let mut txn = ctx.hot.rw()?;
    txn.set_meta(META_STATS, s.to_string().as_bytes())?;
    txn.commit()?;
    Ok(dirty)
}

/// The reconciler task (B-5: `run` only). `node` is a client of its own (its rate and
/// concurrency are the `/local` budget, apart from the tail's). Failures of single calls are
/// counted, never fatal; a store error ends the task with an error.
pub async fn run(
    ctx: Arc<Ctx>,
    node: NodeClient,
    o: ReconcileOpts,
    stop: CancellationToken,
) -> Result<()> {
    run_with(ctx, node, o, stop, NFT_FILL_RETRY).await
}

/// [`run`] with `fill_retry`, the pause before a failed NFT fill is tried again (P6 M-8: the
/// test shortens it).
async fn run_with(
    ctx: Arc<Ctx>,
    node: NodeClient,
    o: ReconcileOpts,
    stop: CancellationToken,
    fill_retry: Duration,
) -> Result<()> {
    let mut nft_retry: Option<Instant> = None;
    if o.seed {
        match nft_fill_pass(&ctx, &stop).await {
            Ok(true) => {}
            Ok(false) => return Ok(()),
            Err(e) => {
                nft_failed(&e, fill_retry);
                nft_retry = Some(Instant::now());
            }
        }
    }
    if o.seed && take_mark_gap(&ctx.hot)? {
        gap_warn();
    }
    if o.seed && !crate::seed::seeded(&ctx.hot.ro()?)? && !seed_pass(&ctx, &stop).await? {
        return Ok(());
    }
    let local = LocalClient::new(node, o.local);
    let mut nft_cursor: Option<Vec<u8>> = None;
    let mut details_cursor: Option<Vec<u8>> = None;
    let mut backoff: Option<Duration> = None;
    let mut retries: HashMap<[u8; 9], u32> = HashMap::new();
    let mut fails: HashMap<u32, u32> = HashMap::new();
    let mut last_recheck: Option<Instant> = None;
    let mut done = 0u64;
    let mut rate = 0.0f64;
    let mut last_log = Instant::now();
    loop {
        if stop.is_cancelled() {
            return Ok(());
        }
        if o.seed && take_mark_gap(&ctx.hot)? {
            gap_warn();
            if !seed_pass(&ctx, &stop).await? {
                return Ok(());
            }
        }
        if nft_retry.is_some_and(|t| t.elapsed() >= fill_retry) {
            match nft_fill_pass(&ctx, &stop).await {
                Ok(true) => nft_retry = None,
                Ok(false) => return Ok(()),
                Err(e) => {
                    nft_failed(&e, fill_retry);
                    nft_retry = Some(Instant::now());
                }
            }
        }
        if last_recheck.is_none_or(|t| t.elapsed() >= RECHECK_SCAN) {
            let n = recheck_not_fungible(&ctx.hot, now_unix(), o.caps.recheck_secs)?;
            if n > 0 {
                tracing::info!(modules = n, "balances: not-fungible modules re-checked");
            }
            last_recheck = Some(Instant::now());
        }
        // NFT rows (plan Task 11) share the `/local` budget; the table is small, so one step
        // per loop keeps them current even behind a long balance backlog.
        let nft = crate::nft_fill::step(&ctx.hot, &local, &mut nft_cursor).await?;
        if nft.rows() > 0 {
            tracing::debug!(?nft, "nft: dirty rows reconciled");
        }
        // NFT guards and token info for GraphQL (GQ-15), in the same budget: at most
        // `nft_details::MAX_CALLS` calls per loop.
        let det = crate::nft_details::step(&ctx.hot, &local, &mut details_cursor).await?;
        if det.calls() > 0 {
            tracing::debug!(?det, "nft: details read");
        }
        let (h, n, share) = (ctx.hot.clone(), o.batch, o.caps.module_share);
        let batch = tokio::task::spawn_blocking(move || refill(&h, n, share)).await??;
        if batch.is_empty() {
            kdx_metrics::get()
                .balance_dirty_rows
                .set(balances::dirty_count(&ctx.hot.ro()?)? as i64);
            if nft.rows() > nft.node_errors || det.calls() > det.node_errors {
                continue;
            }
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(IDLE) => continue,
            }
        }
        let t0 = Instant::now();
        let st = reconcile_batch(&ctx, &local, &batch, &o.caps).await?;
        let k = st.ok + st.absent + st.skipped + st.failures + st.dropped;
        done += k;
        let r = k as f64 / t0.elapsed().as_secs_f64().max(1e-3);
        rate = if rate == 0.0 { r } else { 0.8 * rate + 0.2 * r };
        let dirty = heartbeat(&ctx, done, rate)?;
        if last_log.elapsed() > Duration::from_secs(60) {
            tracing::info!(
                done,
                dirty,
                rate = format!("{rate:.1}"),
                ?st,
                "balances: reconcile"
            );
            last_log = Instant::now();
        }
        let gone = note_failures(&mut fails, &st);
        if !gone.is_empty() {
            mark_not_fungible(&ctx.hot, &gone)?;
            tracing::warn!(
                modules = ?gone,
                "balances: modules whose details calls keep failing are marked not fungible"
            );
        }
        let refill_keys: Vec<([u8; 9], Mark)> = batch.iter().map(|d| (d.key, d.mark)).collect();
        let parked = parked_after(&mut retries, &refill_keys, &st);
        if !parked.is_empty() {
            park(&ctx.hot, &parked)?;
        }
        if backs_off(&st) {
            let d = backoff.map_or(Duration::from_secs(1), |d| (d * 2).min(BACKOFF_MAX));
            backoff = Some(d);
            tracing::warn!(
                errors = st.node_errors,
                backoff_secs = d.as_secs(),
                "balances: node errors, backing off"
            );
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(d) => {}
            }
        } else {
            backoff = None;
        }
    }
}

/// The keys to park after a batch (SR-2): none when the batch backs off (the node itself is
/// failing; the counts start again), else [`count_retries`].
pub fn parked_after(
    retries: &mut HashMap<[u8; 9], u32>,
    batch: &[([u8; 9], Mark)],
    st: &BatchStats,
) -> Vec<([u8; 9], Mark)> {
    if backs_off(st) {
        retries.clear();
        return Vec::new();
    }
    count_retries(retries, batch, &st.node_error_keys)
}

/// Count consecutive node errors per key of a batch: keys that answered are forgotten; the
/// keys that reached [`MAX_KEY_RETRIES`] are returned (and forgotten) for parking, with the
/// mark the batch wrote for them.
pub fn count_retries(
    retries: &mut HashMap<[u8; 9], u32>,
    batch: &[([u8; 9], Mark)],
    errored: &[([u8; 9], Mark)],
) -> Vec<([u8; 9], Mark)> {
    let failed: HashSet<&[u8; 9]> = errored.iter().map(|(k, _)| k).collect();
    for (k, _) in batch {
        if !failed.contains(k) {
            retries.remove(k);
        }
    }
    if retries.len() > MAX_TRACKED {
        retries.clear();
    }
    let mut out = Vec::new();
    for (k, m) in errored {
        let n = retries.entry(*k).or_insert(0);
        *n += 1;
        if *n >= MAX_KEY_RETRIES {
            retries.remove(k);
            out.push((*k, *m));
        }
    }
    out
}

/// Drop the dirty marks of keys that kept failing at the node (`result="parked"`), each only
/// when it is still the mark the failing batch wrote (SR-1): a tail mark written since
/// survives. A parked key's row is flagged unverified (I-5).
pub fn park(hot: &HotStore, keys: &[([u8; 9], Mark)]) -> Result<()> {
    let mut txn = hot.rw()?;
    for (k, m) in keys {
        if clear_mark(&mut txn, Table::BalanceDirty, k, *m)? {
            set_unverified(&mut txn, k)?;
            kdx_metrics::get().balance_reconcile("parked", 0.0);
        }
    }
    txn.commit()?;
    tracing::warn!(
        keys = keys.len(),
        retries = MAX_KEY_RETRIES,
        "balances: parked keys that kept failing at the node until their next tail mark"
    );
    Ok(())
}

/// [`run`] with restarts: a failed reconciler (a store error) is logged and started again
/// after [`BACKOFF_MAX`]; balances never stop ingest.
pub async fn supervise(
    ctx: Arc<Ctx>,
    node: NodeClient,
    o: ReconcileOpts,
    stop: CancellationToken,
) -> Result<()> {
    loop {
        match run(ctx.clone(), node.clone(), o, stop.clone()).await {
            Ok(()) => return Ok(()),
            Err(e) => tracing::error!(
                error = format!("{e:#}"),
                retry_secs = BACKOFF_MAX.as_secs(),
                "balances reconciler failed"
            ),
        }
        tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            _ = tokio::time::sleep(BACKOFF_MAX) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::sync::Mutex;

    use kdx_hot::{Geometry, HotDict, HotStore};
    use kdx_node::{ChainGraph, NodeClientConfig, NodeInfo};
    use kdx_types::DictMut;

    use super::*;
    use crate::IngestOpts;

    /// A fake node: `/local` answers a success for every account except `bad`, whose answer
    /// arrives but has no balance. One connection at a time (`Connection: close`).
    fn fake_node() -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        std::thread::spawn(move || {
            for s in l.incoming() {
                let Ok(mut s) = s else { return };
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
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
                let req: Value = serde_json::from_slice(&buf[head_end..]).unwrap_or(Value::Null);
                let cmd: Value = req
                    .get("cmd")
                    .and_then(Value::as_str)
                    .and_then(|c| serde_json::from_str(c).ok())
                    .unwrap_or(Value::Null);
                let acct = cmd
                    .pointer("/payload/exec/data/a")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let m = cmd.pointer("/payload/exec/data/m").and_then(Value::as_str);
                let body = if let Some(m) = m {
                    // The fungible check: only `free.tok` lists fungible-v2.
                    json!({"result": {"status": "success", "data": m == "free.tok"},
                           "metaData": {"blockHeight": 11}})
                } else if acct == "bad" {
                    json!({"result": {"status": "success", "data": {"guard": null}},
                           "metaData": {"blockHeight": 11}})
                } else {
                    json!({"result": {"status": "success", "data": {"account": acct,
                           "balance": 1.5, "guard": {"keys": ["k1"], "pred": "keys-all"}}},
                           "metaData": {"blockHeight": 11}})
                }
                .to_string();
                let _ = write!(
                    s,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        format!("http://{addr}")
    }

    fn opts(dir: &std::path::Path) -> IngestOpts {
        IngestOpts {
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
        }
    }

    /// A store with `coin` and `accounts`, each marked dirty for coin on chain 0 at height 10.
    fn setup(dir: &std::path::Path, accounts: &[&str]) -> (Arc<HotStore>, u32, Vec<u32>) {
        let hot = Arc::new(HotStore::open(&dir.join("hot"), Geometry::default()).unwrap());
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let coin = dict.intern(DictKind::Module, "coin");
        let ids: Vec<u32> = accounts
            .iter()
            .map(|a| dict.intern(DictKind::Account, a))
            .collect();
        dict.persist(&mut txn).unwrap();
        for &a in &ids {
            balances::mark_dirty(&mut txn, a, coin, 0, 10).unwrap();
        }
        txn.commit().unwrap();
        (hot, coin, ids)
    }

    fn client(dir: &std::path::Path, hot: &Arc<HotStore>) -> (Ctx, LocalClient) {
        let node = NodeClient::new(&NodeClientConfig {
            base: fake_node().parse().unwrap(),
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
            graph_history: vec![(0, vec![0])],
            raw: Value::Null,
        };
        let ctx = Ctx {
            opts: opts(dir),
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
        (ctx, local)
    }

    fn mark_of(hot: &HotStore, k: &[u8; 9]) -> Option<Mark> {
        hot.ro()
            .unwrap()
            .get(Table::BalanceDirty, k)
            .unwrap()
            .as_deref()
            .and_then(Mark::decode)
    }

    /// One key whose answer is unusable is dropped as a failure; the other keys of the batch
    /// reconcile, and the batch does not back off.
    #[tokio::test]
    async fn one_invalid_answer_does_not_stall_the_batch() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _, _) = setup(dir.path(), &["alice", "bad", "carol"]);
        let (ctx, local) = client(dir.path(), &hot);
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        assert_eq!(keys.len(), 3);
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!((st.ok, st.failures, st.node_errors), (2, 1, 0));
        assert!(!backs_off(&st));
        let txn = hot.ro().unwrap();
        // as_of = blockHeight 11 − 1 covers the marks at 10; the bad key's mark is dropped.
        assert_eq!(txn.count(Table::BalanceDirty).unwrap(), 0);
        assert_eq!(txn.count(Table::Balances).unwrap(), 2);
        assert_eq!(txn.count(Table::KeyAccounts).unwrap(), 2);
        let rows = balances::account_rows(&txn, 0).unwrap();
        assert_eq!(rows[0].2.balance, "1.5");
        assert_eq!(rows[0].2.as_of, 10);
    }

    /// SR-1, I-5: a tail mark written between the batch's read and the drop (error, skipped)
    /// or the park survives; the exact mark read is dropped and flags an existing row.
    #[tokio::test]
    async fn a_mark_raised_after_the_read_survives_drop_skip_and_park() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["bad", "parked"]);
        let (bad, parked) = (ids[0], ids[1]);
        let skip = balances::bal_key(bad, 999, 0); // module id with no name: skipped
        let old_row = BalanceRow {
            absent: false,
            balance: "3.0".into(),
            guard: [0; 16],
            as_of: 5,
            reconciled_at: 1,
            unverified: false,
        };
        {
            let mut txn = hot.rw().unwrap();
            balances::mark_dirty(&mut txn, bad, 999, 0, 10).unwrap();
            txn.put(
                Table::Balances,
                &balances::bal_key(bad, coin, 0),
                &old_row.encode(),
            )
            .unwrap();
            txn.commit().unwrap();
        }
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        assert_eq!(keys.len(), 3);
        // The tail marks all three keys again after the read (a real transfer at 12).
        {
            let mut txn = hot.rw().unwrap();
            balances::mark_dirty(&mut txn, bad, coin, 0, 12).unwrap();
            balances::mark_dirty(&mut txn, parked, coin, 0, 12).unwrap();
            balances::mark_dirty(&mut txn, bad, 999, 0, 12).unwrap();
            txn.commit().unwrap();
        }
        let (ctx, local) = client(dir.path(), &hot);
        let pk = balances::bal_key(parked, coin, 0);
        let rest: Vec<_> = keys.iter().filter(|d| d.key != pk).copied().collect();
        let st = reconcile_batch(&ctx, &local, &rest, &Caps::default())
            .await
            .unwrap();
        assert_eq!((st.failures, st.skipped), (1, 1));
        let stale = keys
            .iter()
            .find(|d| d.key == pk)
            .map(|d| (d.key, d.mark))
            .unwrap();
        park(&hot, &[stale]).unwrap();
        for k in [balances::bal_key(bad, coin, 0), pk, skip] {
            assert_eq!(mark_of(&hot, &k).map(|m| m.height), Some(12), "{k:?}");
        }
        let row = |h: &HotStore| {
            BalanceRow::decode(
                &h.ro()
                    .unwrap()
                    .get(Table::Balances, &balances::bal_key(bad, coin, 0))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        };
        assert!(!row(&hot).unverified);
        // Read again: the current marks are dropped, and the existing row is flagged.
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        let rest: Vec<_> = keys.iter().filter(|d| d.key != pk).copied().collect();
        reconcile_batch(&ctx, &local, &rest, &Caps::default())
            .await
            .unwrap();
        park(
            &hot,
            &[keys
                .iter()
                .find(|d| d.key == pk)
                .map(|d| (d.key, d.mark))
                .unwrap()],
        )
        .unwrap();
        assert_eq!(hot.ro().unwrap().count(Table::BalanceDirty).unwrap(), 0);
        assert!(row(&hot).unverified);
    }

    /// M-3: a reorg between the read and the clear re-marks the key at the same height; the
    /// answer still covers that height, but the newer mark is kept for the next read.
    #[tokio::test]
    async fn a_reorg_between_read_and_clear_keeps_the_newer_mark() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["alice"]);
        let k = balances::bal_key(ids[0], coin, 0);
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        {
            // Undo (mark at 10) and re-apply of a lower block (9): the height stays 10.
            let mut txn = hot.rw().unwrap();
            balances::mark_dirty(&mut txn, ids[0], coin, 0, 10).unwrap();
            balances::mark_dirty(&mut txn, ids[0], coin, 0, 9).unwrap();
            txn.commit().unwrap();
        }
        assert_eq!(mark_of(&hot, &k).map(|m| m.height), Some(10));
        assert_ne!(mark_of(&hot, &k), Some(keys[0].mark));
        let (ctx, local) = client(dir.path(), &hot);
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!(st.ok, 1);
        assert!(mark_of(&hot, &k).is_some(), "the reorg's mark must survive");
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert!(mark_of(&hot, &k).is_none());
    }

    /// SR-2: a node outage (every call a node error) parks nothing, however long it lasts; a
    /// key failing alone among answering keys is parked after MAX_KEY_RETRIES batches.
    #[test]
    fn a_node_outage_parks_nothing() {
        let mut m = HashMap::new();
        let mk = Mark { height: 1, seq: 1 };
        let batch: Vec<_> = (0..4u8).map(|i| ([i; 9], mk)).collect();
        let outage = BatchStats {
            node_errors: 4,
            node_error_keys: batch.clone(),
            ..Default::default()
        };
        let one = BatchStats {
            ok: 3,
            node_errors: 1,
            node_error_keys: vec![batch[0]],
            ..Default::default()
        };
        for _ in 0..MAX_KEY_RETRIES * 3 {
            assert!(parked_after(&mut m, &batch, &outage).is_empty());
        }
        assert!(m.is_empty());
        // Counts started before an outage start again after it.
        for _ in 1..MAX_KEY_RETRIES {
            assert!(parked_after(&mut m, &batch, &one).is_empty());
        }
        assert!(parked_after(&mut m, &batch, &outage).is_empty());
        for _ in 1..MAX_KEY_RETRIES {
            assert!(parked_after(&mut m, &batch, &one).is_empty());
        }
        assert_eq!(parked_after(&mut m, &batch, &one), vec![batch[0]]);
    }

    fn intern(hot: &HotStore, kind: DictKind, names: &[&str]) -> Vec<u32> {
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let ids = names.iter().map(|n| dict.intern(kind, n)).collect();
        dict.persist(&mut txn).unwrap();
        txn.commit().unwrap();
        ids
    }

    fn mark(hot: &HotStore, a: u32, m: u32, c: u8, h: u32) {
        let mut txn = hot.rw().unwrap();
        balances::mark_dirty(&mut txn, a, m, c, h).unwrap();
        txn.commit().unwrap();
    }

    /// I-7, R-P5P9-8 #1: older marks of a spam module do not delay a coin mark past the first
    /// batch; the spam module gets its share; tail marks go before the seed backlog, coin first.
    #[test]
    fn a_spam_module_cannot_delay_coin_rows_and_tail_marks_go_first() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["alice"]);
        let mods = intern(&hot, DictKind::Module, &["free.spam", "free.tok"]);
        let (spam, tok) = (mods[0], mods[1]);
        let names: Vec<String> = (0..300).map(|i| format!("s{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let accts = intern(&hot, DictKind::Account, &refs);
        {
            let mut txn = hot.rw().unwrap();
            for (i, &a) in accts.iter().enumerate() {
                balances::mark_dirty(&mut txn, a, spam, 0, 1 + i as u32 % 5).unwrap();
            }
            balances::mark_dirty(&mut txn, accts[0], tok, 0, 9).unwrap();
            let sm = Mark {
                height: 0,
                seq: txn.txn_id().unwrap(),
            }
            .encode();
            for &a in &accts {
                balances::seed_mark(&mut txn, &balances::bal_key(a, coin, 1), &sm).unwrap();
            }
            txn.commit().unwrap();
        }
        let b = refill(&hot, 16, 4).unwrap();
        assert_eq!(b.len(), 16);
        assert_eq!(b[0].key, balances::bal_key(ids[0], coin, 0));
        assert!(!b[0].seed);
        assert_eq!(b.iter().filter(|d| module_of(&d.key) == tok).count(), 1);
        assert_eq!(b.iter().filter(|d| module_of(&d.key) == spam).count(), 4);
        let seeds: Vec<_> = b.iter().filter(|d| d.seed).collect();
        assert_eq!(seeds.len(), 10);
        assert!(seeds.iter().all(|d| module_of(&d.key) == coin));
        assert!(b[..6].iter().all(|d| !d.seed));
        // A tail mark takes a key out of the seed backlog; the dirty count holds both tables.
        mark(&hot, accts[0], coin, 1, 20);
        let txn = hot.ro().unwrap();
        assert_eq!(txn.count(Table::BalanceSeed).unwrap(), 299);
        assert_eq!(balances::dirty_count(&txn).unwrap(), 1 + 300 + 1 + 299 + 1);
        assert!(balances::is_dirty(&txn, accts[1], coin, 1).unwrap());
    }

    /// I-6: rows of a module that did not pass the fungible check keep their balance, but their
    /// guard never reaches `KeyAccounts`; coin and a confirmed `fungible-v2` module do.
    #[tokio::test]
    async fn a_guard_from_an_unconfirmed_module_never_reaches_key_accounts() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["alice"]);
        let mods = intern(&hot, DictKind::Module, &["free.evil", "free.tok"]);
        let accts = intern(&hot, DictKind::Account, &["victim-savings", "bob"]);
        mark(&hot, accts[0], mods[0], 0, 10);
        mark(&hot, accts[1], mods[1], 0, 10);
        let (ctx, local) = client(dir.path(), &hot);
        let keys = refill(&hot, 64, 32).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!(st.ok, 3);
        let txn = hot.ro().unwrap();
        assert_eq!(txn.count(Table::Balances).unwrap(), 3);
        let ka = |a: u32| {
            txn.get(Table::KeyAccounts, &key_account_key(&key_hash("k1"), a))
                .unwrap()
                .is_some()
        };
        assert!(ka(ids[0]), "coin");
        assert!(ka(accts[1]), "confirmed fungible-v2");
        assert!(!ka(accts[0]), "unconfirmed module");
        assert_eq!(txn.count(Table::KeyAccounts).unwrap(), 2);
        let evil = balances::token_row(&txn, mods[0]).unwrap().unwrap();
        assert!(!evil.fungible_on(0) && evil.checked_chains == 1);
        assert!(balances::token_row(&txn, mods[1])
            .unwrap()
            .unwrap()
            .fungible_on(0));
        assert!(balances::trusted(&txn, coin, 7, Some(coin)).unwrap());
    }

    /// R-P5P9-8 #2: a module whose calls fail in NOT_FUNGIBLE_AFTER batches in a row is marked
    /// not fungible; its keys are then dropped without a call; the re-check revives it.
    #[tokio::test]
    async fn failing_modules_are_marked_not_fungible_and_rechecked() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _, _) = setup(dir.path(), &[]);
        let nf = intern(&hot, DictKind::Module, &["free.nf"])[0];
        let carol = intern(&hot, DictKind::Account, &["carol"])[0];
        let fail = BatchStats {
            failed_modules: [nf].into(),
            ..Default::default()
        };
        let ok = BatchStats {
            answered: [nf].into(),
            ..Default::default()
        };
        let mut fails = HashMap::new();
        for _ in 1..NOT_FUNGIBLE_AFTER {
            assert!(note_failures(&mut fails, &fail).is_empty());
        }
        assert!(note_failures(&mut fails, &ok).is_empty());
        for _ in 1..NOT_FUNGIBLE_AFTER {
            assert!(note_failures(&mut fails, &fail).is_empty());
        }
        assert_eq!(note_failures(&mut fails, &fail), vec![nf]);
        mark_not_fungible(&hot, &[nf]).unwrap();
        let kind = |h: &HotStore| {
            balances::token_row(&h.ro().unwrap(), nf)
                .unwrap()
                .unwrap()
                .kind
        };
        assert_eq!(kind(&hot), KIND_NOT_FUNGIBLE);
        mark(&hot, carol, nf, 0, 10);
        let (ctx, local) = client(dir.path(), &hot);
        let keys = refill(&hot, 8, 4).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!((st.dropped, st.calls()), (1, 0));
        assert_eq!(balances::dirty_count(&hot.ro().unwrap()).unwrap(), 0);
        assert_eq!(recheck_not_fungible(&hot, now_unix(), 3600).unwrap(), 0);
        assert_eq!(
            recheck_not_fungible(&hot, now_unix() + 3600, 3600).unwrap(),
            1
        );
        assert_eq!(kind(&hot), KIND_FUNGIBLE);
    }

    /// I-7: past an untrusted module's row cap new keys are dropped and the module is flagged.
    /// The account cap counts only untrusted rows (security review ruling): an account with
    /// cap-many spam rows still gets its coin and confirmed fungible-v2 rows.
    #[tokio::test]
    async fn row_caps_drop_new_keys() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _, ids) = setup(dir.path(), &["alice"]);
        let mods = intern(
            &hot,
            DictKind::Module,
            &["free.spam", "free.two", "free.tok", "free.three"],
        );
        let accts = intern(&hot, DictKind::Account, &["a1", "a2"]);
        mark(&hot, accts[0], mods[0], 0, 10);
        mark(&hot, accts[1], mods[0], 0, 10);
        let (ctx, local) = client(dir.path(), &hot);
        let caps = Caps {
            module_rows: 1,
            ..Caps::default()
        };
        let keys = refill(&hot, 64, 32).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &caps).await.unwrap();
        assert_eq!((st.ok, st.dropped), (2, 1));
        let t = balances::token_row(&hot.ro().unwrap(), mods[0])
            .unwrap()
            .unwrap();
        assert!(t.capped() && t.rows == 1);
        // Account cap 1: alice's coin row does not count; one spam row fills the cap.
        let caps = Caps {
            account_rows: 1,
            ..Caps::default()
        };
        mark(&hot, ids[0], mods[1], 0, 10);
        let keys = refill(&hot, 64, 32).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &caps).await.unwrap();
        assert_eq!((st.ok, st.dropped), (1, 0));
        // At the cap: a confirmed fungible-v2 token is still admitted, more spam is not.
        mark(&hot, ids[0], mods[2], 0, 10);
        mark(&hot, ids[0], mods[3], 0, 10);
        let keys = refill(&hot, 64, 32).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &caps).await.unwrap();
        assert_eq!((st.ok, st.dropped), (1, 1));
        let txn = hot.ro().unwrap();
        assert!(txn
            .get(Table::Balances, &balances::bal_key(ids[0], mods[2], 0))
            .unwrap()
            .is_some());
        assert!(txn
            .get(Table::Balances, &balances::bal_key(ids[0], mods[3], 0))
            .unwrap()
            .is_none());
        assert_eq!(txn.count(Table::Balances).unwrap(), 4);
        assert_eq!(balances::dirty_count(&txn).unwrap(), 0);
    }

    /// P6 M-8 fixture: a trusted ledger's event posting whose hot tx record does not decode,
    /// so the NFT history fill fails on it. Returns the tx key.
    fn undecodable_ledger_event(hot: &HotStore) -> kdx_types::TxKey {
        let ev = intern(hot, DictKind::EventName, &["marmalade-v2.ledger.RECONCILE"])[0];
        let k = kdx_types::TxKey::new(5, 0, 1);
        let mut pk = vec![kdx_decode::Family::Evt as u8];
        pk.extend_from_slice(&u64::from(ev).to_be_bytes());
        pk.extend_from_slice(&k.to_be_bytes());
        let mut txn = hot.rw().unwrap();
        txn.put(Table::TailPostings, &pk, &[]).unwrap();
        txn.put(Table::TailTxs, &k.to_be_bytes(), b"\xffnot a tx record")
            .unwrap();
        txn.commit().unwrap();
        k
    }

    /// Polls `f` every 50 ms for up to 20 s.
    async fn until(what: &str, f: impl Fn() -> bool) {
        let end = Instant::now() + Duration::from_secs(20);
        while !f() {
            assert!(Instant::now() < end, "timed out waiting for: {what}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// P6 M-8: an NFT history fill that keeps failing never blocks the balances. `run` logs
    /// the failure, seeds and reconciles anyway, and retries the fill; once the bad record is
    /// gone a retry completes it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failing_nft_fill_is_retried_while_balances_go_on() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _, _) = setup(dir.path(), &[]);
        let segdir = dir.path().join("segments");
        std::fs::create_dir_all(&segdir).unwrap();
        let k = undecodable_ledger_event(&hot);
        let e = crate::nft_fill::run(&hot, &segdir, None, &|| false).unwrap_err();
        assert!(format!("{e:#}").contains("hot tx record"), "{e:#}");
        let (ctx, _) = client(dir.path(), &hot);
        let node = ctx.node.clone();
        let o = ReconcileOpts {
            local: LocalOpts {
                rps: 0,
                concurrency: 4,
            },
            batch: 10,
            seed: true,
            caps: Caps::default(),
        };
        let stop = CancellationToken::new();
        let task = tokio::spawn(run_with(
            Arc::new(ctx),
            node,
            o,
            stop.clone(),
            Duration::ZERO,
        ));
        let pending = || {
            crate::nft_fill::pending_ledgers(&hot.ro().unwrap())
                .unwrap()
                .len()
        };
        until("the balance seed", || {
            assert!(!task.is_finished(), "run ended on the fill error");
            crate::seed::seeded(&hot.ro().unwrap()).unwrap()
        })
        .await;
        assert!(pending() > 0, "the fill cannot have completed");
        let mut txn = hot.rw().unwrap();
        txn.del(Table::TailTxs, &k.to_be_bytes()).unwrap();
        txn.commit().unwrap();
        until("the fill retry", || {
            assert!(!task.is_finished(), "run ended");
            pending() == 0
        })
        .await;
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    /// M-11: a block applied above a chain's mark watermark + 1 (blocks applied without marks)
    /// makes the reconciler re-run the seed; contiguous blocks and a chain's first block do not.
    #[test]
    fn a_mark_gap_reruns_the_seed() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _, _) = setup(dir.path(), &[]);
        let note = |chain: u8, h: u32| {
            let mut txn = hot.rw().unwrap();
            balances::note_marked(&mut txn, chain, h).unwrap();
            txn.set_meta(balances::meta::BALANCES_SEEDED, &7u32.to_be_bytes())
                .unwrap();
            txn.commit().unwrap();
        };
        note(0, 10);
        note(0, 11);
        note(0, 11); // a reorg re-applies a height
        note(1, 3);
        assert!(!take_mark_gap(&hot).unwrap());
        note(0, 15); // 12..=14 applied without marks
        assert!(take_mark_gap(&hot).unwrap());
        assert!(!crate::seed::seeded(&hot.ro().unwrap()).unwrap());
        assert!(!take_mark_gap(&hot).unwrap());
    }

    #[test]
    fn back_off_needs_a_node_error_majority_and_failing_keys_park() {
        let mut st = BatchStats {
            ok: 9,
            node_errors: 1,
            ..Default::default()
        };
        assert!(!backs_off(&st));
        st.node_errors = 9;
        assert!(backs_off(&st));
        let mut m = HashMap::new();
        let k = [1u8; 9];
        let mk = Mark { height: 0, seq: 1 };
        let batch = vec![(k, mk), ([2u8; 9], mk)];
        for _ in 1..MAX_KEY_RETRIES {
            assert!(count_retries(&mut m, &batch, &[(k, mk)]).is_empty());
        }
        assert_eq!(count_retries(&mut m, &batch, &[(k, mk)]), vec![(k, mk)]);
        assert!(m.is_empty());
    }
}
