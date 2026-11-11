//! Balance reconciler (spec §3.9, plan Task 9, rulings B-1…B-5). Runs only inside `run` (the
//! writer process).
//!
//! - First, when `Meta balances_seeded` is absent, the seed ([`crate::seed`]) runs (B-5).
//! - Then, forever: take dirty keys oldest-first (lowest `marked_height`; a refill keeps the
//!   [`REFILL`] oldest of a chunked scan), [`ReconcileOpts::batch`] at a time; call
//!   `{module}.details` for each through [`LocalClient`] (its own token bucket and concurrency
//!   cap); resolve keyset references with `describe-keyset`; write `Balances`, `Guards`,
//!   `KeyAccounts` and `Tokens.holders` in one write txn per batch.
//! - A dirty key is deleted only when its mark (read in the write txn) is not above the
//!   answer's `as_of`. A success's `as_of` is the tip it read (`metaData.blockHeight − 1`); an
//!   absent answer carries
//!   no height, so its `as_of` is the chain's tail height read before the call (the node is at
//!   or past it).
//! - Outcomes (`kdx_balance_reconcile_total{result}`):
//!   - `ok`, `absent`: row written.
//!   - `skipped`: B-1 refused the call (module name, name length); the key is dropped.
//!   - `error`, Pact failure (the node evaluated the code and it failed, e.g. a module with no
//!     `details`): the key is dropped and an existing row is left as it was. The next tail mark
//!     retries it.
//!   - `error`, node error (transport, HTTP, decode, no `metaData`): the key stays dirty, its
//!     mark moves up to the chain's tail height (so it goes to the back of the queue), and the
//!     loop backs off exponentially up to [`BACKOFF_MAX`]. Never read as a zero balance.
//! - Heartbeat: `Meta balances_reconcile_stats` (JSON: unix time, rows done, rate, dirty) per
//!   batch; not part of the systemd watchdog.

use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures::future::join_all;
use kdx_hot::balances::{
    self, dirty_height, guard_hash, guard_keys, key_account_key, key_hash, ref_hash,
    stored_guard_keys, BalanceRow, GuardKeys,
};
use kdx_hot::{checkpoint, RwTxn, Stage, Table, Txn};
use kdx_node::local::{Details, Local, LocalClient, LocalError, LocalOpts};
use kdx_node::NodeClient;
use kdx_types::DictKind;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::Ctx;

/// Dirty keys kept per refill (oldest marks first).
pub const REFILL: usize = 4096;
/// `BalanceDirty` rows per read txn during a refill.
const SCAN_CHUNK: usize = 100_000;
const IDLE: Duration = Duration::from_secs(2);
pub const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// `Meta` key of the reconciler heartbeat.
pub const META_STATS: &str = "balances_reconcile_stats";

#[derive(Debug, Clone, Copy)]
pub struct ReconcileOpts {
    pub local: LocalOpts,
    /// Dirty keys per batch (one write txn).
    pub batch: usize,
    /// Run the seed first when `Meta balances_seeded` is absent (B-5).
    pub seed: bool,
}

/// The oldest `REFILL` dirty keys, oldest first. Fresh read txn per chunk.
pub fn refill(hot: &kdx_hot::HotStore, n: usize) -> Result<VecDeque<([u8; 9], u32)>> {
    let mut heap: BinaryHeap<(u32, [u8; 9])> = BinaryHeap::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let rows =
            hot.ro()?
                .scan_chunk(Table::BalanceDirty, after.as_deref(), SCAN_CHUNK, |_| true)?;
        for (k, v) in &rows {
            let (Ok(key), Some(h)) = (<[u8; 9]>::try_from(k.as_slice()), dirty_height(v)) else {
                continue;
            };
            if heap.len() < n {
                heap.push((h, key));
            } else if heap.peek().is_some_and(|top| (h, key) < *top) {
                heap.pop();
                heap.push((h, key));
            }
        }
        match rows.last() {
            Some((k, _)) if rows.len() == SCAN_CHUNK => after = Some(k.clone()),
            _ => break,
        }
    }
    Ok(heap
        .into_sorted_vec()
        .into_iter()
        .map(|(h, k)| (k, h))
        .collect())
}

/// One dirty key with what the call needs.
struct Item {
    key: [u8; 9],
    account: u32,
    module: u32,
    chain: u8,
    account_name: Option<String>,
    module_name: Option<String>,
    /// Tail height of the chain before the call (`as_of` of an absent answer).
    tail: u32,
}

enum Outcome {
    Found(Details, u32, Option<(String, Option<Value>)>),
    Absent(u32),
    Skipped,
    Failure,
    NodeError,
}

impl Outcome {
    fn label(&self) -> &'static str {
        match self {
            Outcome::Found(..) => "ok",
            Outcome::Absent(_) => "absent",
            Outcome::Skipped => "skipped",
            Outcome::Failure | Outcome::NodeError => "error",
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

/// Call the node for one item; resolve a keyset reference on the same chain.
async fn call(local: &LocalClient, it: &Item) -> Outcome {
    let (Some(acct), Some(module)) = (&it.account_name, &it.module_name) else {
        return Outcome::Skipped;
    };
    let (r, secs) = local.details_timed(it.chain, module, acct).await;
    let out = match r {
        Ok(Local::Found { value, height }) => {
            let as_of = u32::try_from(height).unwrap_or(u32::MAX);
            let resolved = match guard_keys(&value.guard) {
                GuardKeys::Ref(name) => match local.describe_keyset(it.chain, &name).await {
                    Ok(Local::Found { value: ks, .. }) => Some((name, Some(ks))),
                    Ok(Local::Absent) => Some((name, None)),
                    Err(e) => {
                        tracing::debug!(error = %e, "describe-keyset failed; keys left as cached");
                        None
                    }
                },
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

/// Whether another row of `account` (not `except`) has `pk` in its guard.
fn account_has_key(txn: &impl Txn, account: u32, pk: &str, except: (u32, u8)) -> Result<bool> {
    for (m, c, row) in balances::account_rows(txn, account)? {
        if (m, c) == except || row.absent {
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

/// Write one answer: row, guard, keys, holders.
fn write_row(txn: &mut RwTxn<'_>, it: &Item, new: BalanceRow) -> Result<()> {
    let old = txn
        .get(Table::Balances, &it.key)?
        .and_then(|v| BalanceRow::decode(&v));
    let old_keys = match &old {
        Some(o) if !o.absent => stored_guard_keys(txn, &o.guard, it.chain)?,
        _ => vec![],
    };
    let new_keys = if new.absent {
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
    txn.put(Table::Balances, &it.key, &new.encode())?;
    for pk in &new_keys {
        txn.put(
            Table::KeyAccounts,
            &key_account_key(&key_hash(pk), it.account),
            &[],
        )?;
    }
    let keep: HashSet<&String> = new_keys.iter().collect();
    for pk in old_keys.iter().filter(|k| !keep.contains(k)) {
        if !account_has_key(txn, it.account, pk, (it.module, it.chain))? {
            txn.del(
                Table::KeyAccounts,
                &key_account_key(&key_hash(pk), it.account),
            )?;
        }
    }
    Ok(())
}

/// Delete the dirty key when its mark is not above `as_of`.
fn clear_if_covered(txn: &mut RwTxn<'_>, key: &[u8; 9], as_of: u32) -> Result<()> {
    if let Some(h) = txn
        .get(Table::BalanceDirty, key)?
        .as_deref()
        .and_then(dirty_height)
    {
        if h <= as_of {
            txn.del(Table::BalanceDirty, key)?;
        }
    }
    Ok(())
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BatchStats {
    pub ok: u64,
    pub absent: u64,
    pub skipped: u64,
    pub failures: u64,
    pub node_errors: u64,
}

/// Reconcile one batch of dirty keys.
pub async fn reconcile_batch(
    ctx: &Ctx,
    local: &LocalClient,
    keys: &[([u8; 9], u32)],
) -> Result<BatchStats> {
    let items: Vec<Item> = {
        let txn = ctx.hot.ro()?;
        let mut tails = HashMap::new();
        let mut v = Vec::with_capacity(keys.len());
        for (key, _) in keys {
            let (account, module, chain) =
                balances::parse_bal_key(key).context("bad BalanceDirty key")?;
            let tail = match tails.get(&chain) {
                Some(&t) => t,
                None => {
                    let t = tail_height(&txn, chain)?;
                    tails.insert(chain, t);
                    t
                }
            };
            v.push(Item {
                key: *key,
                account,
                module,
                chain,
                account_name: dict_name(&txn, DictKind::Account, account)?,
                module_name: dict_name(&txn, DictKind::Module, module)?,
                tail,
            });
        }
        v
    };
    // In-flight calls are capped by the client's semaphore (`local_concurrency`).
    let outcomes: Vec<Outcome> = join_all(items.iter().map(|it| call(local, it))).await;

    let mut st = BatchStats::default();
    let now = now_unix();
    let mut txn = ctx.hot.rw()?;
    for (it, out) in items.iter().zip(outcomes) {
        match out {
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
                };
                write_row(&mut txn, it, row)?;
                clear_if_covered(&mut txn, &it.key, as_of)?;
                st.ok += 1;
            }
            Outcome::Absent(as_of) => {
                let row = BalanceRow {
                    absent: true,
                    balance: String::new(),
                    guard: [0; 16],
                    as_of,
                    reconciled_at: now,
                };
                write_row(&mut txn, it, row)?;
                clear_if_covered(&mut txn, &it.key, as_of)?;
                st.absent += 1;
            }
            Outcome::Skipped => {
                txn.del(Table::BalanceDirty, &it.key)?;
                st.skipped += 1;
            }
            Outcome::Failure => {
                txn.del(Table::BalanceDirty, &it.key)?;
                st.failures += 1;
            }
            Outcome::NodeError => {
                // To the back of the queue: the mark moves up to the tail height (never down).
                balances::mark_dirty(&mut txn, it.account, it.module, it.chain, it.tail)?;
                st.node_errors += 1;
            }
        }
    }
    txn.commit()?;
    Ok(st)
}

/// Write the heartbeat and the dirty gauge.
fn heartbeat(ctx: &Ctx, done: u64, rate: f64) -> Result<usize> {
    let dirty = ctx.hot.ro()?.count(Table::BalanceDirty)?;
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
    let seeded = crate::seed::seeded(&ctx.hot.ro()?)?;
    if o.seed && !seeded {
        tracing::info!("balances: no seed yet, seeding (B-2)");
        let (c, s) = (ctx.clone(), stop.clone());
        let rep = tokio::task::spawn_blocking(move || {
            crate::seed::run(&c.hot, &c.segdir, &|| s.is_cancelled())
        })
        .await??;
        tracing::info!(report = %serde_json::to_string(&rep)?, "balances: seed pass ended");
        if rep.stopped {
            return Ok(());
        }
    }
    let local = LocalClient::new(node, o.local);
    let mut queue: VecDeque<([u8; 9], u32)> = VecDeque::new();
    let mut backoff: Option<Duration> = None;
    let mut done = 0u64;
    let mut rate = 0.0f64;
    let mut last_log = Instant::now();
    loop {
        if stop.is_cancelled() {
            return Ok(());
        }
        if queue.is_empty() {
            let h = ctx.hot.clone();
            queue = tokio::task::spawn_blocking(move || refill(&h, REFILL)).await??;
            kdx_metrics::get()
                .balance_dirty_rows
                .set(ctx.hot.ro()?.count(Table::BalanceDirty)? as i64);
        }
        if queue.is_empty() {
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(IDLE) => continue,
            }
        }
        let n = o.batch.min(queue.len());
        let batch: Vec<_> = queue.drain(..n).collect();
        let t0 = Instant::now();
        let st = reconcile_batch(&ctx, &local, &batch).await?;
        let k = st.ok + st.absent + st.skipped + st.failures;
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
        if st.node_errors > 0 {
            let d = backoff.map_or(Duration::from_secs(1), |d| (d * 2).min(BACKOFF_MAX));
            backoff = Some(d);
            tracing::warn!(
                errors = st.node_errors,
                backoff_secs = d.as_secs(),
                "balances: node errors, backing off"
            );
            // The failed keys sit at the back now; take a fresh queue after the pause.
            queue.clear();
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(d) => {}
            }
        } else {
            backoff = None;
        }
    }
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
