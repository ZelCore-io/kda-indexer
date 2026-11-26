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
//! serves; a per-request read budget across all fields and aliases ([`MissBudget`]); the
//! API's per-client token bucket ([`kdx_api::limits::RateLimiter`]); a global in-flight cap and
//! node rate (the [`kdx_node::LocalClient`] of its own); a per-read timeout within the request
//! deadline; a bounded TTL cache of found and absent answers (negatives included), keyed by
//! the exact `(account, module, chain)`. The code sent is fixed (`(<module>.details
//! (read-msg "a"))`, account in `data`), the node URL is the configured one: no client text
//! reaches a URL or the code.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_graphql::Context;
use futures::future::BoxFuture;
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

/// Why a read gave no answer (never cached).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadErr {
    Timeout,
    Node,
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
    /// Per read, slot wait included (and never past the request deadline).
    pub timeout: Duration,
    /// How long a found row and an absent answer are reused.
    pub ttl_found: Duration,
    pub ttl_absent: Duration,
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
            timeout: Duration::from_secs(3),
            ttl_found: Duration::from_secs(30),
            ttl_absent: Duration::from_secs(600),
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

type Key = (String, String, u8);

/// The miss reader: node call, limits, cache, sink.
pub struct MissReader {
    details: DetailsFn,
    sink: Option<FoundSink>,
    settings: MissSettings,
    slots: Semaphore,
    rate: Option<kdx_api::limits::RateLimiter>,
    cache: Mutex<HashMap<Key, (Instant, NodeRow)>>,
    calls: AtomicUsize,
}

impl std::fmt::Debug for MissReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MissReader")
            .field("settings", &self.settings)
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
        MissReader {
            details,
            sink,
            slots: Semaphore::new(settings.max_in_flight.max(1)),
            rate,
            cache: Mutex::new(HashMap::new()),
            calls: AtomicUsize::new(0),
            settings,
        }
    }

    /// The production node call over a [`kdx_node::LocalClient`] of its own (its rate and
    /// concurrency are this reader's global caps).
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
        &self.settings
    }

    /// Node calls made so far (tests).
    #[doc(hidden)]
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    /// Cached entries (tests).
    #[doc(hidden)]
    pub fn cached(&self) -> usize {
        self.lock().len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Key, (Instant, NodeRow)>> {
        self.cache.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn cached_row(&self, k: &Key, now: Instant) -> Option<NodeRow> {
        let c = self.lock();
        c.get(k)
            .filter(|(exp, _)| *exp > now)
            .map(|(_, r)| r.clone())
    }

    fn remember(&self, k: Key, row: NodeRow) {
        let ttl = match row {
            NodeRow::Found { .. } => self.settings.ttl_found,
            NodeRow::Absent => self.settings.ttl_absent,
        };
        let now = Instant::now();
        let mut c = self.lock();
        let cap = self.settings.cache_entries.max(1);
        if c.len() >= cap && !c.contains_key(&k) {
            c.retain(|_, (exp, _)| *exp > now);
            if c.len() >= cap {
                // Still full of live entries: drop an eighth (arbitrary ones) rather than grow.
                let drop: Vec<Key> = c.keys().take(cap / 8 + 1).cloned().collect();
                for d in drop {
                    c.remove(&d);
                }
            }
        }
        c.insert(k, (now + ttl, row));
    }

    /// Take `n` reads from the request budget; how many were granted.
    fn budget(&self, ctx: &Context<'_>, n: usize) -> usize {
        let max = self.settings.max_reads_per_request;
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
        for &c in chains {
            if c >= self.settings.chains || todo.contains(&c) {
                continue;
            }
            let k = (account.to_owned(), module.to_owned(), c);
            match self.cached_row(&k, now) {
                Some(NodeRow::Found { balance, guard, .. }) => out.found.push((c, balance, guard)),
                Some(NodeRow::Absent) => {}
                None => todo.push(c),
            }
        }
        if todo.is_empty() {
            return out;
        }
        let granted = self.budget(ctx, todo.len());
        if granted < todo.len() {
            out.incomplete = true;
            kdx_metrics::get().graphql_rejected("account_miss_budget");
        }
        todo.truncate(granted);
        if todo.is_empty() {
            return out;
        }
        let ip = crate::client_ip(ctx).unwrap_or(IpAddr::from([0, 0, 0, 0]));
        let n = u32::try_from(todo.len()).unwrap_or(u32::MAX);
        if self.rate.as_ref().is_some_and(|r| !r.allow_n(ip, n)) {
            out.incomplete = true;
            kdx_metrics::get().graphql_rejected("account_miss_rate");
            return out;
        }
        let deadline = (Instant::now() + self.settings.timeout).min(crate::deadline(ctx));
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

    /// One read: a slot, the node, the cache; all before `deadline`.
    async fn one(
        &self,
        chain: u8,
        account: &str,
        module: &str,
        deadline: Instant,
    ) -> Result<NodeRow, ReadErr> {
        let left = || deadline.saturating_duration_since(Instant::now());
        let _slot = tokio::time::timeout(left(), self.slots.acquire())
            .await
            .map_err(|_| ReadErr::Timeout)?
            .map_err(|_| ReadErr::Node)?;
        let t = left();
        if t.is_zero() {
            return Err(ReadErr::Timeout);
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        let call = (self.details)(chain, module.to_owned(), account.to_owned());
        let r = tokio::time::timeout(t, call)
            .await
            .map_err(|_| ReadErr::Timeout)
            .and_then(|r| r);
        if let Ok(row) = &r {
            self.remember((account.to_owned(), module.to_owned(), chain), row.clone());
        }
        r
    }
}
