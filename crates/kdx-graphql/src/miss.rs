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
//! in-flight cap; a per-read timeout within the request deadline; a bounded TTL cache (see
//! [Cache]) of found rows and, briefly, node errors per `(account, module, chain)`, and of
//! the node's absent answers as one chain mask per `(account, module)`. The code sent is fixed (`(<module>.details
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
    /// Defaults 2/s and 20 (one account's chains), a tenth of the node rate (secpass B I-1):
    /// one client at its maximum rate leaves most of the node budget to the others. Each
    /// cache insert is one of the client's reads, so the same limit bounds the cache churn
    /// one client causes (50,000 inserts, half the default cache, take it about 7 h, longer
    /// than the absent TTL).
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
    /// How long a found row, an absent answer and a node error are reused (zero: not
    /// cached). Absent answers live long (default 6 h): a typical lookup misses on 19 of 20
    /// chains, and only a long negative TTL keeps the node budget for new accounts. The cost:
    /// a row created without an event on a chain cached absent stays hidden up to this long
    /// (a row with an event is indexed and served from the store regardless).
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
            per_ip_rps: 2,
            per_ip_burst: 20,
            node_rps: 20,
            node_burst: 40,
            timeout: Duration::from_secs(3),
            slot_wait: Duration::from_millis(150),
            ttl_found: Duration::from_secs(30),
            ttl_absent: Duration::from_secs(6 * 3600),
            ttl_error: Duration::from_secs(30),
            cache_entries: 100_000,
            max_in_flight: 16,
            chains: 20,
        }
    }
}

/// One request's read budget, shared by its fields, aliases and batch operations, and its
/// memo of the reads it started (PR #65 I-1). A key that one field of the request has already
/// asked, granted or refused, is never charged again: every later field joins that answer, so
/// a page of blocks by one miner makes one charge and gives every block the same row. The memo
/// holds at most one entry per key the request asked, bounded by the request's own fields.
#[derive(Clone, Default)]
pub struct MissBudget {
    used: Arc<AtomicUsize>,
    memo: Arc<Mutex<HashMap<Key, Call>>>,
}

impl std::fmt::Debug for MissBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MissBudget")
            .field("used", &self.used())
            .field("memo", &self.memo().len())
            .finish()
    }
}

impl MissBudget {
    /// Reads charged so far (tests).
    #[doc(hidden)]
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    fn memo(&self) -> MutexGuard<'_, HashMap<Key, Call>> {
        self.memo.lock().unwrap_or_else(|e| e.into_inner())
    }
}

type Key = (String, String, u8);

/// A cache key: `(account, module, Some(chain))` for one chain's found row or node error,
/// `(account, module, None)` for the account's absent mask.
type CacheKey = (String, String, Option<u8>);

/// A cached answer: the node's found row, a node error (served as unanswered), or the
/// chains the node answered absent (bit `c` for chain `c`).
#[derive(Debug, Clone)]
enum Cached {
    Row(NodeRow),
    Failed,
    Absent(u32),
}

/// The mask bit of `chain`; 0 (never cached absent) for a chain past the mask's 32 bits.
fn bit(chain: u8) -> u32 {
    1u32.checked_shl(u32::from(chain)).unwrap_or(0)
}

/// The answer cache: two generations of at most half the cap each. An insert goes to the
/// young one; when it is full it becomes the old one and the previous old one is retired
/// whole (handed back to be dropped outside the lock). O(1) per insert, no scan; an expired
/// entry is never served and leaves with its generation.
///
/// Absent answers of one `(account, module)` share one entry, a chain mask, so an account
/// costs one entry however many chains it misses on (found rows and node errors stay per
/// chain, short-lived). Merging: a bit is set only by the node's absent answer for that
/// chain (never by a timeout, a refusal, a node error or anything a client sends). A bit
/// set into a live entry keeps the entry's expiry, the earliest of its bits', so no bit is
/// served past its own TTL (a newer bit may expire early, costing one read); into an
/// expired or missing entry it starts a new one. A found answer clears its chain's bit.
#[derive(Debug, Default)]
struct Cache {
    young: Generation,
    old: Generation,
}

type Generation = HashMap<CacheKey, (Instant, Cached)>;

impl Cache {
    fn entry(&self, k: &CacheKey, now: Instant) -> Option<&(Instant, Cached)> {
        self.young
            .get(k)
            .or_else(|| self.old.get(k))
            .filter(|(exp, _)| *exp > now)
    }

    /// The answer for one chain: absent if its mask bit is set, else its own entry.
    fn get(&self, k: &Key, now: Instant) -> Option<Cached> {
        let mask = (k.0.clone(), k.1.clone(), None);
        if let Some((_, Cached::Absent(m))) = self.entry(&mask, now) {
            if m & bit(k.2) != 0 {
                return Some(Cached::Row(NodeRow::Absent));
            }
        }
        self.entry(&(k.0.clone(), k.1.clone(), Some(k.2)), now)
            .map(|(_, v)| v.clone())
    }

    /// The account's live absent mask.
    fn absent_mask(&self, account: &str, module: &str, now: Instant) -> u32 {
        match self.entry(&(account.to_owned(), module.to_owned(), None), now) {
            Some((_, Cached::Absent(m))) => *m,
            _ => 0,
        }
    }

    fn remove(&mut self, k: &CacheKey) {
        self.young.remove(k);
        self.old.remove(k);
    }

    /// The node answered `k`'s chain absent: set its bit (see the merge rule above) and
    /// drop the chain's own entry. Not while a live found row is cached for the chain: coin
    /// rows are never deleted, so such an answer is stale (a call that outlived a newer one,
    /// or the dropped-feed race) and would hide the row for the whole absent TTL.
    #[must_use]
    fn mark_absent(
        &mut self,
        k: &Key,
        exp: Instant,
        now: Instant,
        cap: usize,
    ) -> Option<Generation> {
        let b = bit(k.2);
        if b == 0 {
            return None;
        }
        let ck = (k.0.clone(), k.1.clone(), Some(k.2));
        if let Some((_, Cached::Row(NodeRow::Found { .. }))) = self.entry(&ck, now) {
            return None;
        }
        self.remove(&ck);
        let mk = (k.0.clone(), k.1.clone(), None);
        let (exp, m) = match self.entry(&mk, now) {
            Some((e, Cached::Absent(m))) => ((*e).min(exp), m | b),
            _ => (exp, b),
        };
        self.put(mk, exp, Cached::Absent(m), cap)
    }

    /// The node found `k`'s chain: clear its absent bit in each generation, the entry's
    /// expiry unchanged; a mask left empty is dropped from its own generation only.
    fn clear_absent(&mut self, k: &Key) {
        let mk = (k.0.clone(), k.1.clone(), None);
        let b = bit(k.2);
        for g in [&mut self.young, &mut self.old] {
            if let Some((_, Cached::Absent(m))) = g.get_mut(&mk) {
                *m &= !b;
                if *m == 0 {
                    g.remove(&mk);
                }
            }
        }
    }

    #[must_use]
    fn put(&mut self, k: CacheKey, exp: Instant, v: Cached, cap: usize) -> Option<Generation> {
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
            Cached::Row(NodeRow::Absent) | Cached::Absent(_) => self.settings.ttl_absent,
            Cached::Failed => self.settings.ttl_error,
        };
        let cap = self.settings.cache_entries.max(1);
        let now = Instant::now();
        let exp = now + ttl;
        let retired = {
            let mut cache = self.cache();
            match v {
                Cached::Row(NodeRow::Absent) | Cached::Absent(_) => {
                    if ttl.is_zero() {
                        None
                    } else {
                        cache.mark_absent(&k, exp, now, cap)
                    }
                }
                v => {
                    // A found row supersedes an absent bit even when found rows are not
                    // cached.
                    if matches!(v, Cached::Row(NodeRow::Found { .. })) {
                        cache.clear_absent(&k);
                    }
                    if ttl.is_zero() {
                        None
                    } else {
                        cache.put((k.0, k.1, Some(k.2)), exp, v, cap)
                    }
                }
            }
        };
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
            let absent = cache.absent_mask(account, module, now);
            for &c in chains {
                if c >= self.core.settings.chains || todo.contains(&c) || absent & bit(c) != 0 {
                    continue;
                }
                let k = (account.to_owned(), module.to_owned(), c);
                match cache.get(&k, now) {
                    Some(Cached::Row(NodeRow::Found { balance, guard, .. })) => {
                        out.found.push((c, balance, guard));
                    }
                    Some(Cached::Row(NodeRow::Absent) | Cached::Absent(_)) => {}
                    Some(Cached::Failed) => out.incomplete = true,
                    None => todo.push(c),
                }
            }
        }
        let key = |c: u8| (account.to_owned(), module.to_owned(), c);
        let memo = ctx.data_opt::<MissBudget>();
        // Keys another field of this request already asked: join that answer, uncharged.
        let mut calls: Vec<(u8, Call, bool)> = Vec::new();
        if let Some(b) = memo {
            let m = b.memo();
            todo.retain(|&c| match m.get(&key(c)) {
                Some(call) => {
                    calls.push((c, call.clone(), false));
                    false
                }
                None => true,
            });
        }
        if !todo.is_empty() {
            let refused = self.charge(ctx, &mut todo);
            if !refused.is_empty() {
                out.incomplete = true;
            }
            let deadline = (Instant::now() + self.core.settings.timeout).min(crate::deadline(ctx));
            let mut m = memo.map(MissBudget::memo);
            for c in todo {
                let call = self.request_call(key(c), deadline);
                if let Some(m) = m.as_mut() {
                    m.insert(key(c), call.clone());
                }
                calls.push((c, call, true));
            }
            // A refused key is remembered as refused, so every field of the request that asks
            // it answers alike (unanswered), and none is charged again.
            if let Some(m) = m.as_mut() {
                for c in refused {
                    let busy: Call = futures::future::ready(Err(ReadErr::Busy)).boxed().shared();
                    m.insert(key(c), busy);
                }
            }
        }
        let reads = calls
            .into_iter()
            .map(|(c, call, own)| async move { (c, call.await, own) });
        for (c, r, own) in futures::future::join_all(reads).await {
            match r {
                Ok(NodeRow::Found {
                    balance,
                    guard,
                    height,
                }) => {
                    // Fed back once, by the field that started the read.
                    if own {
                        if let Some(sink) = &self.sink {
                            sink(FoundRow {
                                account: account.to_owned(),
                                module: module.to_owned(),
                                chain: c,
                                height: u32::try_from(height).unwrap_or(u32::MAX),
                            });
                        }
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

    /// Charge the client's limit and the request budget for `todo`, keeping in `todo` only
    /// the chains granted; the chains refused are returned.
    fn charge(&self, ctx: &Context<'_>, todo: &mut Vec<u8>) -> Vec<u8> {
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
                    kdx_metrics::get().graphql_rejected("account_miss_rate");
                    return std::mem::take(todo);
                }
            }
        }
        let granted = self.budget(ctx, todo.len());
        if granted < todo.len() {
            kdx_metrics::get().graphql_rejected("account_miss_budget");
        }
        todo.split_off(granted)
    }

    /// One read before `deadline`, shareable by every field of the request: join the key's
    /// call in flight, or start one. A refusal is a ready error.
    fn request_call(&self, key: Key, deadline: Instant) -> Call {
        match self.flight(key, deadline) {
            Err(e) => futures::future::ready(Err(e)).boxed().shared(),
            Ok(call) => {
                let left = deadline.saturating_duration_since(Instant::now());
                async move {
                    tokio::time::timeout(left, call)
                        .await
                        .unwrap_or(Err(ReadErr::Timeout))
                }
                .boxed()
                .shared()
            }
        }
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
            Some(Cached::Absent(_)) => {
                return Ok(futures::future::ready(Ok(NodeRow::Absent)).boxed().shared())
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn key(c: u8) -> Key {
        ("k:a".into(), "coin".into(), c)
    }

    fn absent(cache: &Cache, c: u8, now: Instant) -> bool {
        matches!(cache.get(&key(c), now), Some(Cached::Row(NodeRow::Absent)))
    }

    /// The absent mask: one entry per account, bits merged, no bit served past its own
    /// TTL, a newer bit never extends an older one, an expired entry starts over.
    #[test]
    fn absent_bits_merge_and_expire_by_the_earliest() {
        let t0 = Instant::now();
        let ttl = Duration::from_secs(100);
        let mut c = Cache::default();
        let _ = c.mark_absent(&key(1), t0 + ttl, t0, 1000);
        let t1 = t0 + Duration::from_secs(60);
        let _ = c.mark_absent(&key(2), t1 + ttl, t1, 1000);
        assert_eq!(c.len(), 1, "one entry per (account, module)");
        assert!(absent(&c, 1, t1) && absent(&c, 2, t1) && !absent(&c, 3, t1));
        // Chain 1's TTL ends at t0 + 100: neither bit is served past it.
        let t2 = t0 + Duration::from_secs(101);
        assert!(!absent(&c, 1, t2) && !absent(&c, 2, t2));
        // Set into an expired entry: a fresh entry, old bits gone.
        let _ = c.mark_absent(&key(3), t2 + ttl, t2, 1000);
        assert!(absent(&c, 3, t2) && !absent(&c, 1, t2) && !absent(&c, 2, t2));
        assert!(absent(&c, 3, t2 + Duration::from_secs(99)));
        assert!(!absent(&c, 3, t2 + ttl));
    }

    /// Secpass B M-13: an absent answer that arrives while a live found row is cached for
    /// that chain is stale (coin rows are never deleted) and changes nothing.
    #[test]
    fn absent_after_a_live_found_row_is_ignored() {
        let t0 = Instant::now();
        let exp = t0 + Duration::from_secs(100);
        let mut c = Cache::default();
        let found = NodeRow::Found {
            balance: "1.0".into(),
            guard: Value::Null,
            height: 1,
        };
        let _ = c.put(
            ("k:a".into(), "coin".into(), Some(3)),
            exp,
            Cached::Row(found.clone()),
            1000,
        );
        let _ = c.mark_absent(&key(3), exp, t0, 1000);
        assert!(!absent(&c, 3, t0));
        assert!(matches!(c.get(&key(3), t0), Some(Cached::Row(r)) if r == found));
        // Once the found row expires, an absent answer is taken.
        let t1 = exp + Duration::from_secs(1);
        let _ = c.mark_absent(&key(3), t1 + Duration::from_secs(100), t1, 1000);
        assert!(absent(&c, 3, t1));
    }

    /// Secpass B M-13: clearing a bit empties a mask per generation; a younger mask with
    /// other bits keeps them.
    #[test]
    fn clear_absent_works_per_generation() {
        let t0 = Instant::now();
        let exp = t0 + Duration::from_secs(100);
        let mk: CacheKey = ("k:a".into(), "coin".into(), None);
        let mut c = Cache::default();
        c.old.insert(mk.clone(), (exp, Cached::Absent(bit(3))));
        c.young
            .insert(mk.clone(), (exp, Cached::Absent(bit(3) | bit(5))));
        c.clear_absent(&key(3));
        assert!(!absent(&c, 3, t0));
        assert!(absent(&c, 5, t0), "bit 5 survives in the young mask");
        assert!(!c.old.contains_key(&mk), "the emptied old mask is dropped");
    }

    fn core(s: MissSettings) -> Core {
        Core {
            details: Arc::new(|_, _, _| futures::future::ready(Err(ReadErr::Node)).boxed()),
            slots: Semaphore::new(1),
            cache: Mutex::new(Cache::default()),
            flights: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            calls: AtomicUsize::new(0),
            settings: s,
        }
    }

    /// Secpass B M-13: `remember(Found)` clears the chain's absent bit, also when found rows
    /// are not cached (`ttl_found` 0).
    #[test]
    fn remember_found_clears_the_bit_even_uncached() {
        let found = NodeRow::Found {
            balance: "1.0".into(),
            guard: Value::Null,
            height: 1,
        };
        for ttl_found in [Duration::from_secs(30), Duration::ZERO] {
            let core = core(MissSettings {
                ttl_found,
                ..MissSettings::default()
            });
            core.remember(key(3), Cached::Row(NodeRow::Absent));
            core.remember(key(4), Cached::Row(NodeRow::Absent));
            let now = Instant::now();
            assert!(absent(&core.cache(), 3, now));
            core.remember(key(3), Cached::Row(found.clone()));
            let cache = core.cache();
            assert!(!absent(&cache, 3, now) && absent(&cache, 4, now));
            let served = cache.get(&key(3), now);
            if ttl_found.is_zero() {
                assert!(served.is_none(), "not cached");
            } else {
                assert!(matches!(served, Some(Cached::Row(r)) if r == found));
            }
        }
    }

    #[test]
    fn a_found_answer_clears_its_bit_and_chains_past_the_mask_are_not_cached() {
        let t0 = Instant::now();
        let exp = t0 + Duration::from_secs(100);
        let mut c = Cache::default();
        let _ = c.mark_absent(&key(4), exp, t0, 1000);
        let _ = c.mark_absent(&key(5), exp, t0, 1000);
        c.clear_absent(&key(4));
        assert!(!absent(&c, 4, t0) && absent(&c, 5, t0));
        c.clear_absent(&key(5));
        assert_eq!(c.len(), 0, "an empty mask is dropped");
        assert!(c.mark_absent(&key(40), exp, t0, 1000).is_none());
        assert_eq!(c.len(), 0);
        assert_eq!(bit(31), 1 << 31);
        assert_eq!(bit(32), 0);
        assert_eq!(bit(255), 0);
    }
}
