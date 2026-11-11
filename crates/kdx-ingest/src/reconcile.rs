//! Balance reconciler (spec §3.9, plan Task 9, rulings B-1…B-5). Runs only inside `run` (the
//! writer process).
//!
//! - First, when `Meta balances_seeded` is absent, the seed ([`crate::seed`]) runs (B-5).
//! - Then, forever: take dirty keys oldest-first (lowest `marked_height`; a refill keeps the
//!   [`REFILL`] oldest of a chunked scan), [`ReconcileOpts::batch`] at a time; call
//!   `{module}.details` for each through [`LocalClient`] (its own token bucket and concurrency
//!   cap); resolve keyset references with `describe-keyset`; write `Balances`, `Guards`,
//!   `KeyAccounts` and `Tokens.holders` in one write txn per batch.
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

use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures::future::join_all;
use kdx_hot::balances::{
    self, clear_mark, guard_hash, guard_keys, key_account_key, key_hash, ref_hash, set_unverified,
    stored_guard_keys, BalanceRow, GuardKeys, Mark,
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
/// Keys whose node errors are counted at once (the map is cleared beyond this).
const MAX_TRACKED: usize = 100_000;
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
pub fn refill(hot: &HotStore, n: usize) -> Result<VecDeque<([u8; 9], Mark)>> {
    let mut heap: BinaryHeap<(u32, [u8; 9], u64)> = BinaryHeap::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let rows =
            hot.ro()?
                .scan_chunk(Table::BalanceDirty, after.as_deref(), SCAN_CHUNK, |_| true)?;
        for (k, v) in &rows {
            let (Ok(key), Some(m)) = (<[u8; 9]>::try_from(k.as_slice()), Mark::decode(v)) else {
                continue;
            };
            let e = (m.height, key, m.seq);
            if heap.len() < n {
                heap.push(e);
            } else if heap.peek().is_some_and(|top| e < *top) {
                heap.pop();
                heap.push(e);
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
        .map(|(height, k, seq)| (k, Mark { height, seq }))
        .collect())
}

/// One dirty key with what the call needs.
struct Item {
    key: [u8; 9],
    /// The mark as read at refill: only this exact mark is ever deleted.
    mark: Mark,
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

/// Delete the dirty key when it still holds the mark read at refill and that mark is not above
/// `as_of` (SR-1, M-3).
fn clear_if_covered(txn: &mut RwTxn<'_>, it: &Item, as_of: u32) -> Result<()> {
    if it.mark.height <= as_of {
        clear_mark(txn, Table::BalanceDirty, &it.key, it.mark)?;
    }
    Ok(())
}

/// Drop the mark of a call that gave no usable answer, only when it is still the mark read at
/// refill (I-5); an existing row is flagged unverified so the route serves it stale.
fn drop_mark(txn: &mut RwTxn<'_>, it: &Item) -> Result<()> {
    if clear_mark(txn, Table::BalanceDirty, &it.key, it.mark)? {
        set_unverified(txn, &it.key)?;
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
    /// Keys whose call hit a node (transport or HTTP) error, with the mark the batch wrote
    /// for them (moved up to the tail height); still dirty.
    pub node_error_keys: Vec<([u8; 9], Mark)>,
}

impl BatchStats {
    /// Calls the node answered or failed (B-1 refusals make none).
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
    keys: &[([u8; 9], Mark)],
) -> Result<BatchStats> {
    let items: Vec<Item> = {
        let txn = ctx.hot.ro()?;
        let mut tails = HashMap::new();
        let mut v = Vec::with_capacity(keys.len());
        for (key, mark) in keys {
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
                mark: *mark,
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
                    unverified: false,
                };
                write_row(&mut txn, it, row)?;
                clear_if_covered(&mut txn, it, as_of)?;
                st.ok += 1;
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
                write_row(&mut txn, it, row)?;
                clear_if_covered(&mut txn, it, as_of)?;
                st.absent += 1;
            }
            Outcome::Skipped => {
                drop_mark(&mut txn, it)?;
                st.skipped += 1;
            }
            Outcome::Failure => {
                drop_mark(&mut txn, it)?;
                st.failures += 1;
            }
            Outcome::NodeError => {
                // To the back of the queue: the mark moves up to the tail height (never down).
                let m = balances::mark_dirty(&mut txn, it.account, it.module, it.chain, it.tail)?;
                st.node_errors += 1;
                st.node_error_keys.push((it.key, m));
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
    if o.seed && !crate::nft_fill::filled(&ctx.hot.ro()?)? {
        tracing::info!("nft: no history fill yet, filling (N-1)");
        let (c, s) = (ctx.clone(), stop.clone());
        let rep = tokio::task::spawn_blocking(move || {
            crate::nft_fill::run(&c.hot, &c.segdir, &|| s.is_cancelled())
        })
        .await??;
        tracing::info!(report = %serde_json::to_string(&rep)?, "nft: fill pass ended");
        if rep.stopped {
            return Ok(());
        }
    }
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
    let mut nft_cursor: Option<Vec<u8>> = None;
    let mut queue: VecDeque<([u8; 9], Mark)> = VecDeque::new();
    let mut backoff: Option<Duration> = None;
    let mut retries: HashMap<[u8; 9], u32> = HashMap::new();
    let mut done = 0u64;
    let mut rate = 0.0f64;
    let mut last_log = Instant::now();
    loop {
        if stop.is_cancelled() {
            return Ok(());
        }
        // NFT rows (plan Task 11) share the `/local` budget; the table is small, so one step
        // per loop keeps them current even behind a long balance backlog.
        let nft = crate::nft_fill::step(&ctx.hot, &local, &mut nft_cursor).await?;
        if nft.rows() > 0 {
            tracing::debug!(?nft, "nft: dirty rows reconciled");
        }
        if queue.is_empty() {
            let h = ctx.hot.clone();
            queue = tokio::task::spawn_blocking(move || refill(&h, REFILL)).await??;
            kdx_metrics::get()
                .balance_dirty_rows
                .set(ctx.hot.ro()?.count(Table::BalanceDirty)? as i64);
        }
        if queue.is_empty() {
            if nft.rows() > nft.node_errors {
                continue;
            }
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
        let parked = parked_after(&mut retries, &batch, &st);
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
                let body = if acct == "bad" {
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
        let keys: Vec<_> = refill(&hot, REFILL).unwrap().into_iter().collect();
        assert_eq!(keys.len(), 3);
        let st = reconcile_batch(&ctx, &local, &keys).await.unwrap();
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
        let keys: Vec<_> = refill(&hot, REFILL).unwrap().into_iter().collect();
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
        let rest: Vec<_> = keys.iter().filter(|(k, _)| *k != pk).copied().collect();
        let st = reconcile_batch(&ctx, &local, &rest).await.unwrap();
        assert_eq!((st.failures, st.skipped), (1, 1));
        let stale = *keys.iter().find(|(k, _)| *k == pk).unwrap();
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
        let keys: Vec<_> = refill(&hot, REFILL).unwrap().into_iter().collect();
        let rest: Vec<_> = keys.iter().filter(|(k, _)| *k != pk).copied().collect();
        reconcile_batch(&ctx, &local, &rest).await.unwrap();
        park(&hot, &[*keys.iter().find(|(k, _)| *k == pk).unwrap()]).unwrap();
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
        let keys: Vec<_> = refill(&hot, REFILL).unwrap().into_iter().collect();
        {
            // Undo (mark at 10) and re-apply of a lower block (9): the height stays 10.
            let mut txn = hot.rw().unwrap();
            balances::mark_dirty(&mut txn, ids[0], coin, 0, 10).unwrap();
            balances::mark_dirty(&mut txn, ids[0], coin, 0, 9).unwrap();
            txn.commit().unwrap();
        }
        assert_eq!(mark_of(&hot, &k).map(|m| m.height), Some(10));
        assert_ne!(mark_of(&hot, &k), Some(keys[0].1));
        let (ctx, local) = client(dir.path(), &hot);
        let st = reconcile_batch(&ctx, &local, &keys).await.unwrap();
        assert_eq!(st.ok, 1);
        assert!(mark_of(&hot, &k).is_some(), "the reorg's mark must survive");
        let keys: Vec<_> = refill(&hot, REFILL).unwrap().into_iter().collect();
        reconcile_batch(&ctx, &local, &keys).await.unwrap();
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
