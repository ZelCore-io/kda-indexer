//! Node read on a missed account row (gap Q, owner decision 2026-11-26: "index + node read
//! on miss").
//!
//! Stock answers `fungibleAccount`, `fungibleChainAccount(s)` and `FungibleAccount.chainAccounts`
//! from a live `(<module>.details "<account>")` per chain (`getAccountInfo_NODE`,
//! `getChainsAccountInfo_NODE`), so it lists rows no index names: an account created inside
//! another module's code (`free.crankkc.create-card-for-address`) at balance 0. We answer from
//! the store and, for each asked chain with no row (or a row the node once reported absent),
//! read the node once, as stock does:
//!
//! - found: the row is served as the reconciler would store it (the node's balance text, its
//!   guard), and fed back ([`FoundSink`]) so the store indexes it;
//! - absent: no row (stock: the failed `details` call is dropped);
//! - failure (timeout, transport, refusal by our limits): no row, as stock drops a failed
//!   chain, and the response carries `extensions.nodeReadIncomplete: true`. Never a guessed
//!   row.
//!
//! Limits (stock has none): only an [`account_like`] name of a module the store knows and
//! serves; the API's per-client token bucket ([`kdx_api::limits::RateLimiter`]), checked first;
//! a per-request read budget across all fields and aliases ([`MissBudget`]); a node rate of its
//! own (default 20/s, at most 25/s, on top of the reconciler's `balances.local_rps`); a global
//! in-flight cap; a per-read timeout within the request deadline; a bounded TTL cache of
//! found and absent answers (negatives included) and, briefly, of node errors, keyed by the
//! exact `(account, module, chain)`. The code sent is fixed (`(<module>.details
//! (read-msg "a"))`, account in `data`), the node URL is the configured one: no client text
//! reaches a URL or the code.
//!
//! Fail fast (PR #56 I-1): the global limits never queue a read for long. A read with no node
//! token is refused at once, and a read waits at most [`MissSettings::slot_wait`] for an
//! in-flight slot; a refused read leaves its chain unanswered (`nodeReadIncomplete`), so a
//! saturated reader costs a lookup at most that wait, never the read timeout. Concurrent
//! reads of one `(account, module, chain)` share one node call (single flight): the call
//! runs as a shared future that any waiter drives, so a cancelled first caller does not
//! strand the others, and it is dropped (slot released, entry removed) when no caller waits.
//!
//! A request with no peer address (tests, embedding; never one the HTTP server takes) gets no
//! node read while the per-client limit is on: it answers store-only with
//! `nodeReadIncomplete`, rather than share one bucket that anyone could drain.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use async_graphql::Context;
use futures::future::{BoxFuture, FutureExt, Shared, WeakShared};
use kdx_decode::receivers::account_like;
use serde_json::Value;
use tokio::sync::Semaphore;

/// One node answer for `(account, module, chain)`.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeRow {
    /// The row: balance text as the node printed it, the guard, and the height the node read.
    Found {
        balance: String,
        guard: Value,
        height: u64,
    },
    /// The node has no such row.
    Absent,
}

/// Why a read gave no answer. Only [`ReadErr::Node`] is cached (for
/// [`MissSettings::ttl_error`]); a timeout or a refusal by our own limits is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadErr {
    Timeout,
    Node,
    /// Refused by a global limit (no node token, no slot within the slot wait).
    Busy,
}

/// The node call: `(chain, module, account)` to its answer. Production wraps
/// [`kdx_node::LocalClient::details`]; tests pass a mock.
pub type DetailsFn =
    Arc<dyn Fn(u8, String, String) -> BoxFuture<'static, Result<NodeRow, ReadErr>> + Send + Sync>;

/// A row the node has and the store has not: `(account, module, chain, node height)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundRow {
    pub account: String,
    pub module: String,
    pub chain: u8,
    pub height: u32,
}

/// Where found rows go to be indexed (`run`: the ingest writer marks them dirty; `serve`
/// has none). Must not block: a full queue drops.
pub type FoundSink = Arc<dyn Fn(FoundRow) + Send + Sync>;

/// `[graphql.account_miss]` limits.
#[derive(Debug, Clone)]
pub struct MissSettings {
    /// Node reads one request may cause, all fields and aliases together (one account's 20
    /// chains, twice).
    pub max_reads_per_request: usize,
    /// Per client (the API limiter's keys): reads per second (0 = no per-client limit), burst.
    pub per_ip_rps: u32,
    pub per_ip_burst: u32,
    /// Node calls per second across the process (0 = none) and their burst. A read with no
    /// token is refused at once, never queued.
    pub node_rps: u32,
    pub node_burst: u32,
    /// Per read, slot wait included (and never past the request deadline).
    pub timeout: Duration,
    /// Longest wait for an in-flight slot; past it the read is refused.
    pub slot_wait: Duration,
    /// How long a found row, an absent answer and a node error are reused (zero: a node
    /// error is not cached).
    pub ttl_found: Duration,
    pub ttl_absent: Duration,
    pub ttl_error: Duration,
    /// Cache entries at most.
    pub cache_entries: usize,
    /// Reads in flight across the process.
    pub max_in_flight: usize,
    /// Chains of the network (`fungibleAccount` asks `0..chains`).
    pub chains: u8,
}

impl Default for MissSettings {
    fn default() -> Self {
        MissSettings {
            max_reads_per_request: 40,
            per_ip_rps: 20,
            per_ip_burst: 60,
            node_rps: 20,
            node_burst: 40,
            timeout: Duration::from_secs(3),
            slot_wait: Duration::from_millis(150),
            ttl_found: Duration::from_secs(30),
            ttl_absent: Duration::from_secs(600),
            ttl_error: Duration::from_secs(30),
            cache_entries: 100_000,
            max_in_flight: 16,
            chains: 20,
        }
    }
}

/// One request's read budget, shared by its fields, aliases and batch operations.
#[derive(Debug, Clone, Default)]
pub struct MissBudget {
    used: Arc<AtomicUsize>,
}

impl MissBudget {
    /// Reads charged so far (tests).
    #[doc(hidden)]
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

type Key = (String, String, u8);

/// A cached answer: the node's row, or a node error (served as unanswered).
#[derive(Debug, Clone)]
enum Cached {
    Row(NodeRow),
    Failed,
}

/// The answer cache: two generations of at most half the cap each. An insert goes to the
/// young one; when it is full it becomes the old one and the previous old one is retired
/// whole (handed back to be dropped outside the lock). O(1) per insert, no scan; an expired
/// entry is never served and leaves with its generation.
#[derive(Debug, Default)]
struct Cache {
    young: HashMap<Key, (Instant, Cached)>,
    old: HashMap<Key, (Instant, Cached)>,
}

type Generation = HashMap<Key, (Instant, Cached)>;

impl Cache {
    fn get(&self, k: &Key, now: Instant) -> Option<Cached> {
        self.young
            .get(k)
            .or_else(|| self.old.get(k))
            .filter(|(exp, _)| *exp > now)
            .map(|(_, v)| v.clone())
    }

    #[must_use]
    fn put(&mut self, k: Key, exp: Instant, v: Cached, cap: usize) -> Option<Generation> {
        let half = (cap / 2).max(1);
        let mut retired = None;
        if self.young.len() >= half && !self.young.contains_key(&k) {
            let young = std::mem::take(&mut self.young);
            retired = Some(std::mem::replace(&mut self.old, young));
        }
        self.young.insert(k, (exp, v));
        retired
    }

    fn len(&self) -> usize {
        self.young.len() + self.old.len()
    }
}

/// A token bucket of the whole reader (the node rate), never waited on.
#[derive(Debug)]
struct Bucket {
    rate: f64,
    burst: f64,
    state: Mutex<(f64, Instant)>,
}

impl Bucket {
    fn new(rate: u32, burst: u32) -> Option<Bucket> {
        (rate > 0).then(|| {
            let burst = f64::from(burst.max(1));
            Bucket {
                rate: f64::from(rate),
                burst,
                state: Mutex::new((burst, Instant::now())),
            }
        })
    }

    fn try_take(&self, now: Instant) -> bool {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let (tokens, last) = *s;
        let t = (tokens + now.saturating_duration_since(last).as_secs_f64() * self.rate)
            .min(self.burst);
        let ok = t >= 1.0;
        *s = (if ok { t - 1.0 } else { t }, now);
        ok
    }
}

type Call = Shared<BoxFuture<'static, Result<NodeRow, ReadErr>>>;

/// A node call in flight, shared by every reader of its key. The map holds it weakly: the
/// call lives only while a caller waits on it.
struct Flight {
    id: u64,
    until: Instant,
    call: WeakShared<BoxFuture<'static, Result<NodeRow, ReadErr>>>,
}

/// What the shared calls need (they outlive the caller that started them).
struct Core {
    details: DetailsFn,
    settings: MissSettings,
    slots: Semaphore,
    cache: Mutex<Cache>,
    flights: Mutex<HashMap<Key, Flight>>,
    next_id: AtomicU64,
    calls: AtomicUsize,
}

impl Core {
    fn cache(&self) -> MutexGuard<'_, Cache> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn flights(&self) -> MutexGuard<'_, HashMap<Key, Flight>> {
        self.flights.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn remember(&self, k: Key, v: Cached) {
        let ttl = match &v {
            Cached::Row(NodeRow::Found { .. }) => self.settings.ttl_found,
            Cached::Row(NodeRow::Absent) => self.settings.ttl_absent,
            Cached::Failed => self.settings.ttl_error,
        };
        if ttl.is_zero() {
            return;
        }
        let cap = self.settings.cache_entries.max(1);
        let retired = self.cache().put(k, Instant::now() + ttl, v, cap);
        // Dropped here, after the lock is released.
        drop(retired);
    }

    /// The node call of one key: a slot (waiting at most the slot wait), the node, the cache;
    /// all before `until`.
    async fn call(&self, k: &Key, until: Instant) -> Result<NodeRow, ReadErr> {
        let left = || until.saturating_duration_since(Instant::now());
        let wait = self.settings.slot_wait.min(left());
        let _slot = match tokio::time::timeout(wait, self.slots.acquire()).await {
            Ok(Ok(p)) => p,
            Ok(Err(_)) => return Err(ReadErr::Node),
            Err(_) => {
                kdx_metrics::get().graphql_rejected("account_miss_busy");
                return Err(ReadErr::Busy);
            }
        };
        let t = left();
        if t.is_zero() {
            return Err(ReadErr::Timeout);
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        let call = (self.details)(k.2, k.1.clone(), k.0.clone());
        let r = tokio::time::timeout(t, call)
            .await
            .map_err(|_| ReadErr::Timeout)
            .and_then(|r| r);
        match &r {
            Ok(row) => self.remember(k.clone(), Cached::Row(row.clone())),
            Err(ReadErr::Node) => self.remember(k.clone(), Cached::Failed),
            Err(_) => {}
        }
        r
    }
}

/// Removes its flight from the map when the shared call ends or is dropped unfinished.
struct FlightGuard {
    core: Arc<Core>,
    key: Key,
    id: u64,
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        let mut f = self.core.flights();
        if f.get(&self.key).is_some_and(|x| x.id == self.id) {
            f.remove(&self.key);
        }
    }
}

/// The miss reader: node call, limits, cache, sink.
pub struct MissReader {
    core: Arc<Core>,
    sink: Option<FoundSink>,
    rate: Option<kdx_api::limits::RateLimiter>,
    node: Option<Bucket>,
}

impl std::fmt::Debug for MissReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MissReader")
            .field("settings", &self.core.settings)
            .finish_non_exhaustive()
    }
}

/// What [`MissReader::read`] gives: the found rows by chain, and whether any asked chain went
/// unanswered (failure, or refused by a limit).
#[derive(Debug, Default)]
pub struct MissOutcome {
    pub found: Vec<(u8, String, Value)>,
    pub incomplete: bool,
}

impl MissReader {
    pub fn new(details: DetailsFn, sink: Option<FoundSink>, settings: MissSettings) -> MissReader {
        let rate = (settings.per_ip_rps > 0)
            .then(|| kdx_api::limits::RateLimiter::new(settings.per_ip_rps, settings.per_ip_burst));
        let node = Bucket::new(settings.node_rps, settings.node_burst);
        MissReader {
            core: Arc::new(Core {
                details,
                slots: Semaphore::new(settings.max_in_flight.max(1)),
                cache: Mutex::new(Cache::default()),
                flights: Mutex::new(HashMap::new()),
                next_id: AtomicU64::new(0),
                calls: AtomicUsize::new(0),
                settings,
            }),
            sink,
            rate,
            node,
        }
    }

    /// The production node call over a [`kdx_node::LocalClient`] of its own. Its rate and
    /// concurrency must not queue: the reader's own node bucket and slots are the caps.
    pub fn local_details(local: Arc<kdx_node::LocalClient>) -> DetailsFn {
        Arc::new(move |chain, module, account| {
            let local = local.clone();
            Box::pin(async move {
                match local.details(chain, &module, &account).await {
                    Ok(kdx_node::Local::Found { value, height, .. }) => Ok(NodeRow::Found {
                        balance: value.balance,
                        guard: value.guard,
                        height,
                    }),
                    Ok(kdx_node::Local::Absent) => Ok(NodeRow::Absent),
                    Err(kdx_node::LocalError::Node(kdx_node::NodeError::Timeout)) => {
                        Err(ReadErr::Timeout)
                    }
                    Err(_) => Err(ReadErr::Node),
                }
            })
        })
    }

    pub fn settings(&self) -> &MissSettings {
        &self.core.settings
    }

    /// Node calls made so far (tests).
    #[doc(hidden)]
    pub fn calls(&self) -> usize {
        self.core.calls.load(Ordering::Relaxed)
    }

    /// Cached entries (tests).
    #[doc(hidden)]
    pub fn cached(&self) -> usize {
        self.core.cache().len()
    }

    /// Calls in flight (tests).
    #[doc(hidden)]
    pub fn in_flight(&self) -> usize {
        self.core.flights().len()
    }

    /// Reads left in the request budget.
    fn budget_left(&self, ctx: &Context<'_>) -> usize {
        let max = self.core.settings.max_reads_per_request;
        ctx.data_opt::<MissBudget>()
            .map_or(max, |b| max.saturating_sub(b.used.load(Ordering::Acquire)))
    }

    /// Take `n` reads from the request budget; how many were granted.
    fn budget(&self, ctx: &Context<'_>, n: usize) -> usize {
        let max = self.core.settings.max_reads_per_request;
        let Some(b) = ctx.data_opt::<MissBudget>() else {
            return n.min(max);
        };
        let mut granted = 0;
        let _ = b
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                granted = n.min(max.saturating_sub(used));
                Some(used + granted)
            });
        granted
    }

    /// Read `chains` of `(account, module)` that the store has no row for. The caller has
    /// checked the module is one the store serves.
    pub async fn read(
        &self,
        ctx: &Context<'_>,
        account: &str,
        module: &str,
        chains: &[u8],
    ) -> MissOutcome {
        let mut out = MissOutcome::default();
        if chains.is_empty() || !account_like(account) {
            return out;
        }
        let now = Instant::now();
        let mut todo = Vec::new();
        {
            let cache = self.core.cache();
            for &c in chains {
                if c >= self.core.settings.chains || todo.contains(&c) {
                    continue;
                }
                let k = (account.to_owned(), module.to_owned(), c);
                match cache.get(&k, now) {
                    Some(Cached::Row(NodeRow::Found { balance, guard, .. })) => {
                        out.found.push((c, balance, guard));
                    }
                    Some(Cached::Row(NodeRow::Absent)) => {}
                    Some(Cached::Failed) => out.incomplete = true,
                    None => todo.push(c),
                }
            }
        }
        if todo.is_empty() {
            out.found.sort_by_key(|(c, _, _)| *c);
            return out;
        }
        // The client's own limit first, so a refused client spends none of the request
        // budget; it is charged only what the budget can still grant.
        let want = todo.len().min(self.budget_left(ctx));
        if want > 0 {
            if let Some(rate) = &self.rate {
                let allowed = match crate::client_ip(ctx) {
                    // No peer address: no shared fallback bucket.
                    None => false,
                    Some(ip) => rate.allow_n(ip, u32::try_from(want).unwrap_or(u32::MAX)),
                };
                if !allowed {
                    out.incomplete = true;
                    kdx_metrics::get().graphql_rejected("account_miss_rate");
                    out.found.sort_by_key(|(c, _, _)| *c);
                    return out;
                }
            }
        }
        let granted = self.budget(ctx, todo.len());
        if granted < todo.len() {
            out.incomplete = true;
            kdx_metrics::get().graphql_rejected("account_miss_budget");
        }
        todo.truncate(granted);
        let deadline = (Instant::now() + self.core.settings.timeout).min(crate::deadline(ctx));
        let reads = todo.into_iter().map(|c| async move {
            let r = self.one(c, account, module, deadline).await;
            (c, r)
        });
        for (c, r) in futures::future::join_all(reads).await {
            match r {
                Ok(NodeRow::Found {
                    balance,
                    guard,
                    height,
                }) => {
                    if let Some(sink) = &self.sink {
                        sink(FoundRow {
                            account: account.to_owned(),
                            module: module.to_owned(),
                            chain: c,
                            height: u32::try_from(height).unwrap_or(u32::MAX),
                        });
                    }
                    out.found.push((c, balance, guard));
                }
                Ok(NodeRow::Absent) => {}
                Err(_) => out.incomplete = true,
            }
        }
        out.found.sort_by_key(|(c, _, _)| *c);
        out
    }

    /// One read before `deadline`: join the key's call in flight, or start one.
    async fn one(
        &self,
        chain: u8,
        account: &str,
        module: &str,
        deadline: Instant,
    ) -> Result<NodeRow, ReadErr> {
        let call = self.flight((account.to_owned(), module.to_owned(), chain), deadline)?;
        let left = deadline.saturating_duration_since(Instant::now());
        tokio::time::timeout(left, call)
            .await
            .unwrap_or(Err(ReadErr::Timeout))
    }

    /// The key's call in flight, or a new one (a node token taken without waiting).
    fn flight(&self, key: Key, deadline: Instant) -> Result<Call, ReadErr> {
        // Declared before the lock so that a stale call, if this is its last handle, is
        // dropped after the lock is released (its guard takes the same lock).
        let mut _stale: Option<Call> = None;
        let mut flights = self.core.flights();
        let now = Instant::now();
        if let Some(f) = flights.get(&key) {
            if let Some(call) = f.call.upgrade() {
                if f.until > now {
                    return Ok(call);
                }
                _stale = Some(call);
            }
        }
        // Answered while this read waited: no new call.
        let cached = self.core.cache().get(&key, now);
        match cached {
            Some(Cached::Row(row)) => return Ok(futures::future::ready(Ok(row)).boxed().shared()),
            Some(Cached::Failed) => return Err(ReadErr::Node),
            None => {}
        }
        if self.node.as_ref().is_some_and(|b| !b.try_take(now)) {
            kdx_metrics::get().graphql_rejected("account_miss_node_rate");
            return Err(ReadErr::Busy);
        }
        let id = self.core.next_id.fetch_add(1, Ordering::Relaxed);
        let guard = FlightGuard {
            core: self.core.clone(),
            key: key.clone(),
            id,
        };
        let core = self.core.clone();
        let k = key.clone();
        let call: Call = async move {
            let _guard = guard;
            core.call(&k, deadline).await
        }
        .boxed()
        .shared();
        if let Some(weak) = call.downgrade() {
            flights.insert(
                key,
                Flight {
                    id,
                    until: deadline,
                    call: weak,
                },
            );
        }
        Ok(call)
    }
}
