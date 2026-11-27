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
//! - Not fungible (R-P5P9-8 #2): a module whose `details` calls all fail or answer unusably
//!   in [`NOT_FUNGIBLE_AFTER`] batches in a row is marked; the tail stops marking it, its keys
//!   are dropped without a call (`result="not_fungible"`), `/tokens` hides it; after
//!   [`Caps::recheck_secs`] it is active again.
//! - Caps (I-7): a non-coin module stops getting new rows at [`Caps::module_rows`], a confirmed
//!   `fungible-v2` one at [`Caps::fungible_module_rows`] (F-14; the tail then marks no new key
//!   of it), and an account at [`Caps::account_rows`]
//!   (`result="capped"`).
//! - A dirty key is deleted only when its mark, read again in the write txn, is still the exact
//!   [`Mark`] (height and write sequence) read at refill, and its height is not above the
//!   answer's `as_of` (SR-1, M-3): a mark raised or rewritten meanwhile (a new block, a reorg's
//!   undo or re-apply) survives. Every arm that drops a mark (skipped, error, parked) follows
//!   the same rule (I-5). A success's `as_of` is the tip it read (`metaData.blockHeight − 1`); an
//!   absent answer carries
//!   no height, so its `as_of` is the chain's tail height read before the call (the node is at
//!   or past it).
//! - Branch check (D2): a height alone does not say which branch the node read. Right after a
//!   quick reorg flip-and-back, `/local` can still answer from the orphan's state at the same
//!   height. A success therefore clears its mark only when its `metaData.prevBlockHash` (the
//!   hash of the tip it read) equals our stored header at `as_of` on that chain. Otherwise
//!   (another branch, no hash, or no header of ours at that height because the node is ahead
//!   of our tail) the row is written flagged `unverified` (served `stale: true`) and the mark
//!   moves up to the tail height, back of the queue, with no retry count
//!   (`BatchStats::other_branch`, `result="other_branch"`). When we hold another header at
//!   that height (a definite other branch) and already have a row, the row is kept (balance,
//!   guard, key index) and only flagged unverified (L1). A batch whose answers were all
//!   off-branch pauses the loop [`OFF_BRANCH_PAUSE`] ([`off_branch_pause`]); [`BranchWatch`]
//!   WARNs on answers without `prevBlockHash` and on an off-branch majority over a window.
//! - Residual race (D2): an `Absent` answer carries no `metaData`, so its branch cannot be
//!   checked. If the node answers absent from an orphan's state (a row that exists only on our
//!   branch), the row is stored absent and its mark cleared as before; only the account's next
//!   posting on that chain, or `reconcile-balances --all`, re-reads it. Reorgs are logged, not
//!   kept in the store, so absent answers right after a reorg are not re-queued.
//! - Outcomes (`kdx_balance_reconcile_total{result}`):
//!   - `ok`, `absent`: row written (`ok` from another branch: written unverified, still dirty).
//!   - `other_branch`, counted in addition to `ok`: the answer was not read on our branch.
//!   - `skipped`: B-1 refused the call (module name, name length); the key is dropped.
//!   - `error`, Pact failure (the node evaluated the code and it failed, e.g. a module with no
//!     `details`): the key is dropped, and an existing row keeps its balance flagged
//!     `unverified` (served `stale: true`, I-5). The next tail mark retries it. No back-off.
//!   - `invalid`, unusable answer ([`LocalError::Invalid`]: not JSON, over the body cap, no
//!     decimal balance or `metaData`): the key stays dirty, its mark moved up to the tail
//!     height (back of the queue), no back-off; a batch of only invalid answers pauses
//!     [`INVALID_PAUSE`]. After [`MAX_KEY_RETRIES`] in a row it is parked like a node error
//!     and listed in `balances_parked_invalid/` (bounded) for `reconcile-balances --status`
//!     and the retry pass `--all`. Invalid answers count as module failures for the
//!     not-fungible rule (H-1), so a module that never answers usably stops taking batches.
//!   - `error`, node error (transport or HTTP): the key stays dirty and its mark moves up to
//!     the chain's tail height (back of the queue). The loop backs off exponentially up to
//!     [`BACKOFF_MAX`] only when node errors are at least half of a batch's calls
//!     ([`backs_off`]). A key with [`MAX_KEY_RETRIES`] node errors (or invalid answers) in a
//!     row in batches that did not back off is parked (`parked`: mark dropped until the next
//!     tail mark, row flagged `unverified`). A backed-off batch counts nothing and resets the counts (SR-2):
//!     a node outage parks no key. Never read as a zero balance.
//! - Heartbeat: `Meta balances_reconcile_stats` (JSON: unix time, rows done, in-batch `rate`,
//!   wall-clock `wall_rate`, dirty) per batch; not part of the systemd watchdog.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures::future::join_all;
use kdx_hot::balances::{
    self, clear_mark, guard_hash, guard_keys, key_account_key, key_hash, ref_hash, set_unverified,
    stored_guard_keys, BalanceRow, GuardKeys, Mark, TokenRow, KIND_FUNGIBLE, KIND_NOT_FUNGIBLE,
};
use kdx_hot::tables::block_key;
use kdx_hot::{checkpoint, HotStore, RwTxn, Stage, Table, Txn};
use kdx_node::local::{
    BatchEntry, Details, Local, LocalClient, LocalError, LocalOpts, LocalResult,
};
use kdx_node::{NodeClient, NodeError};
use kdx_types::record::decode_block;
use kdx_types::{DictKind, Hash32};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::read_pool::{ReadNode, ReadPool, LAG_BLOCKS};
use crate::Ctx;

/// Consecutive node errors or invalid answers after which a key is parked: its dirty mark is dropped (counted as
/// `result="parked"`) until the next tail mark of the row.
pub const MAX_KEY_RETRIES: u32 = 5;
/// Keys (and modules) whose errors are counted at once (the map is cleared beyond this).
const MAX_TRACKED: usize = 100_000;
/// Batches in a row in which every `details` call of a module failed (and none answered) after
/// which the module is marked not fungible (R-P5P9-8 #2).
pub const NOT_FUNGIBLE_AFTER: u32 = 3;
/// Seed rows read per refill, per batch slot.
const SEED_WINDOW: usize = 16;
/// Percent of each batch [`refill`] reserves for the newest tail marks per chain (PR #66
/// review I1).
const TIP_SHARE_PCT: usize = 25;
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
    /// Txs per slice of the D3 history walk, one write txn each (re-review N2).
    pub history_slice_txs: u64,
    /// Worker threads that read and decode the history walk's frozen slices
    /// (`balances.history_walk_threads`; 0: [`crate::seed_receivers::default_history_threads`]).
    pub history_walk_threads: usize,
    /// Most `Balances` rows of one confirmed `fungible-v2` module other than `coin` (F-14:
    /// anyone can deploy one). Past it, a key with no row is dropped and the module flagged
    /// like an untrusted one at [`Caps::module_rows`].
    pub fungible_module_rows: u32,
    /// Batches one refill window holds (`balances.refill_window_batches`, 1..=32; 1: a refill
    /// every batch). See [`refill_window`].
    pub refill_window_batches: usize,
}

/// Default of [`Caps::fungible_module_rows`] (F-14): 10x the largest non-coin module on
/// mainnet, `free.finux` with 126,246 rows (KDA-03 hot store scan, 2026-11-22).
pub const DEFAULT_FUNGIBLE_MODULE_ROWS: u32 = 1_262_460;

impl Default for Caps {
    fn default() -> Self {
        Caps {
            module_share: 32,
            module_rows: 1_000_000,
            account_rows: 1_000,
            recheck_secs: 86_400,
            history_slice_txs: crate::seed_receivers::HISTORY_SLICE_TXS,
            history_walk_threads: 0,
            fungible_module_rows: DEFAULT_FUNGIBLE_MODULE_ROWS,
            refill_window_batches: DEFAULT_REFILL_WINDOW,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReconcileOpts {
    pub local: LocalOpts,
    /// Dirty keys per batch (one write txn).
    pub batch: usize,
    /// Run the seed first when `Meta balances_seeded` is absent (B-5).
    pub seed: bool,
    pub caps: Caps,
    /// `balances.keylist_skip`: modules the key listing never reads (the node blocks its cut
    /// while it answers a `keys` of a huge table).
    pub keylist_skip: Vec<String>,
    /// `balances.local_batch_keys`: accounts per batched `details` call (1: one call a key).
    pub batch_keys: usize,
    /// `balances.local_urls`: the nodes the `details` reads are spread over; empty means the
    /// primary (`node.url`) alone.
    pub read_nodes: ReadNodes,
}

/// One read node of `balances.local_urls`: its label (`n0`, `n1`, …) and its own node client
/// (its rate and concurrency caps are that node's), or `None` for the primary (`node.url`),
/// which shares the reconciler's client and budget.
#[derive(Clone, Default)]
pub struct ReadNodes(pub Vec<(String, Option<NodeClient>)>);

impl std::fmt::Debug for ReadNodes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.0
                    .iter()
                    .map(|(l, n)| format!("{l}{}", if n.is_none() { " (primary)" } else { "" })),
            )
            .finish()
    }
}

impl ReadNodes {
    /// The read pool over `primary` and the other nodes, each with the `/local` caps `o`.
    pub fn pool(&self, primary: &Arc<LocalClient>, o: LocalOpts, batch_keys: usize) -> ReadPool {
        let nodes = if self.0.is_empty() {
            vec![ReadNode::new("n0".into(), primary.clone(), true)]
        } else {
            self.0
                .iter()
                .map(|(label, node)| match node {
                    None => ReadNode::new(label.clone(), primary.clone(), true),
                    Some(n) => ReadNode::new(
                        label.clone(),
                        Arc::new(LocalClient::new(n.clone(), o)),
                        false,
                    ),
                })
                .collect()
        };
        ReadPool::new(nodes, batch_keys)
    }
}

/// One dirty key taken for a batch: a tail mark (`BalanceDirty`) or a seed mark
/// (`BalanceSeed`), with the mark as read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dirty {
    pub key: [u8; 9],
    pub mark: Mark,
    pub seed: bool,
    /// Taken for the tip share ([`refill_with`] step 0): its batched read goes through
    /// [`ReadPool::pick`], which can choose the primary, not [`ReadPool::pick_batch`], so a
    /// secondary a few blocks behind does not hold a fresh tip mark back (PR #70 review I1).
    pub tip: bool,
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
/// Newest-kept heap ([`keep_newest`]) per chain, for the tip share.
type TipHeaps = BTreeMap<u8, BinaryHeap<Reverse<(Mark, [u8; 9])>>>;

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
/// Two passes over the same lists: the fair pass deals at most `share` rows per module; the
/// top-up pass then fills the room that is left with the modules' remaining rows, one per
/// module per round, uncapped. So `share` orders the slots and never shrinks the batch.
fn fair(out: &mut Vec<Dirty>, by_module: HashMap<u32, Heap>, n: usize, share: usize, seed: bool) {
    let mut lists: Vec<Vec<(Mark, [u8; 9])>> = by_module
        .into_values()
        .map(BinaryHeap::into_sorted_vec)
        .filter(|l| !l.is_empty())
        .collect();
    lists.sort_by_key(|l| l[0]);
    for (from, to) in [(0, share), (share, usize::MAX)] {
        let mut i = from;
        while i < to && out.len() < n {
            let mut any = false;
            for l in &lists {
                if out.len() >= n {
                    break;
                }
                if let Some(&(mark, key)) = l.get(i) {
                    out.push(Dirty {
                        key,
                        mark,
                        seed,
                        tip: false,
                    });
                    any = true;
                }
            }
            if !any {
                break;
            }
            i += 1;
        }
    }
}

/// Keep the `cap` newest entries.
fn keep_newest(h: &mut BinaryHeap<Reverse<(Mark, [u8; 9])>>, e: (Mark, [u8; 9]), cap: usize) {
    h.push(Reverse(e));
    if h.len() > cap {
        h.pop();
    }
}

/// Newest first, round-robin over chains, until `out` holds `n` (the tip share). A module
/// other than coin takes at most its `share` (`taken` counts its rows).
fn tip_deal(
    out: &mut Vec<Dirty>,
    by_chain: TipHeaps,
    n: usize,
    cap: Option<(usize, &mut HashMap<u32, usize>)>,
) {
    let lists: Vec<Vec<(Mark, [u8; 9])>> = by_chain
        .into_values()
        .map(|h| {
            h.into_sorted_vec()
                .into_iter()
                .map(|Reverse(e)| e)
                .collect()
        })
        .collect();
    let (share, mut taken) = match cap {
        Some((s, t)) => (s, Some(t)),
        None => (usize::MAX, None),
    };
    let mut i = 0;
    while out.len() < n {
        let mut any = false;
        for l in &lists {
            if out.len() >= n {
                break;
            }
            let Some(&(mark, key)) = l.get(i) else {
                continue;
            };
            any = true;
            if let Some(t) = taken.as_deref_mut() {
                let c = t.entry(module_of(&key)).or_insert(0);
                if *c >= share {
                    continue;
                }
                *c += 1;
            }
            out.push(Dirty {
                key,
                mark,
                seed: false,
                tip: true,
            });
        }
        if !any {
            break;
        }
        i += 1;
    }
}

/// The next batch of up to `n` dirty keys (R-P5P9-8 #1, I-7), in this order:
/// 0. the tip share, [`TIP_SHARE_PCT`] of `n`: the newest tail marks of each chain,
///    round-robin over chains, `coin` marks only while they fill it; other modules take the
///    room `coin` leaves (at most `share` each, counted with their rows in 2). So a miner's
///    or a recent receiver's mark is not starved behind an old backlog the receivers passes
///    (D1, D3) marked at historic heights (PR #66 review I1);
/// 1. tail marks of `coin`, oldest first;
/// 2. tail marks of the other modules, round-robin over modules, at most `share` each;
/// 3. only when 0 to 2 leave room, the seed backlog (the first `SEED_WINDOW × n` rows of
///    `BalanceSeed`): `coin` first, then the other modules round-robin, at most `share` each
///    in the fair pass; the room that is left is topped up from the window's remaining rows
///    without the cap (a window of one module is a full batch, not `share` rows).
///
/// Tail marks are all of `BalanceDirty` (what changed since its last reconcile); every batch
/// takes a fresh refill, so a new `coin` mark waits at most one batch behind newer `coin`
/// marks, whatever other modules mark.
pub fn refill(hot: &HotStore, n: usize, share: usize) -> Result<Vec<Dirty>> {
    refill_with(hot, n, share, &HashMap::new())
}

/// [`refill`], with `back`: the keys recent batches marked again at the tail to send them to
/// the back of the queue ([`note_remarked`]), with the mark written. A row whose mark is
/// still exactly that one takes the old-backlog path (1, 2) only, never the tip share (PR #66
/// review I3): its new tail mark is the newest of its chain, and in the tip share a chain
/// whose node fails for some keys would re-read them every batch and park them within
/// seconds. A newer mark (the key moved again) is a fresh tip mark and gets the share.
pub fn refill_with(
    hot: &HotStore,
    n: usize,
    share: usize,
    back: &HashMap<[u8; 9], Mark>,
) -> Result<Vec<Dirty>> {
    let share = share.max(1);
    let coin = balances::coin_module(&hot.ro()?)?;
    let tip_n = n * TIP_SHARE_PCT / 100;
    let mut coin_rows = Heap::new();
    let mut by_module: HashMap<u32, Heap> = HashMap::new();
    let mut tip_coin: TipHeaps = BTreeMap::new();
    let mut tip_other: TipHeaps = BTreeMap::new();
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
            if tip_n > 0 && back.get(&key) != Some(&m) {
                let tip = if Some(module) == coin {
                    &mut tip_coin
                } else {
                    &mut tip_other
                };
                keep_newest(tip.entry(key[8]).or_default(), (m, key), tip_n);
            }
            if Some(module) == coin {
                keep(&mut coin_rows, (m, key), n);
            } else {
                // `share` past the tip's: the tip may take rows of the module's oldest.
                keep(
                    by_module.entry(module).or_default(),
                    (m, key),
                    share.saturating_add(tip_n),
                );
            }
        }
        match rows.last() {
            Some((k, _)) if rows.len() == SCAN_CHUNK => after = Some(k.clone()),
            _ => break,
        }
    }
    let mut out = Vec::with_capacity(n);
    let mut taken: HashMap<u32, usize> = HashMap::new();
    tip_deal(&mut out, tip_coin, tip_n, None);
    tip_deal(&mut out, tip_other, tip_n, Some((share, &mut taken)));
    let tipped: HashSet<[u8; 9]> = out.iter().map(|d| d.key).collect();
    out.extend(
        coin_rows
            .into_sorted_vec()
            .into_iter()
            .filter(|(_, key)| !tipped.contains(key))
            .take(n.saturating_sub(tipped.len()))
            .map(|(mark, key)| Dirty {
                key,
                mark,
                seed: false,
                tip: false,
            }),
    );
    // A module's rows in the tip count against its `share`.
    let by_module: HashMap<u32, Heap> = by_module
        .into_iter()
        .map(|(m, h)| {
            let left = share.saturating_sub(taken.get(&m).copied().unwrap_or(0));
            let h: Heap = h
                .into_sorted_vec()
                .into_iter()
                .filter(|(_, key)| !tipped.contains(key))
                .take(left)
                .collect();
            (m, h)
        })
        .collect();
    fair(&mut out, by_module, n, share, false);
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
                        tip: false,
                    });
                }
            } else {
                // Up to `n` per module: the fair pass uses `share` of them, the top-up pass
                // the rest, so a window of one module still fills the batch.
                keep(
                    by_module.entry(module_of(&key)).or_default(),
                    (mark, key),
                    n,
                );
            }
        }
        fair(&mut out, by_module, n, share, true);
    }
    Ok(out)
}

/// Default `balances.refill_window_batches`: batches one refill window holds.
pub const DEFAULT_REFILL_WINDOW: usize = 8;
/// Most `balances.refill_window_batches`.
pub const MAX_REFILL_WINDOW: usize = 32;
/// A refill window older than this (counted from the end of its walk) is taken again even
/// with keys left. A tip mark made just after a walk passed its key waits at most about this
/// long plus that walk, the next walk or the batch it overlaps (the longer) and the batch that
/// reads it.
pub const WINDOW_MAX_AGE: Duration = Duration::from_secs(5);

/// The refill of `k` batches at once: [`refill_with`] for `k × n` keys and `k × share` per
/// module. One refill walks all of `BalanceDirty` whatever `n` is, so a window spreads that
/// walk over `k` batches. Its order is [`refill_with`]'s: the tip share first, then the old
/// backlog, the seed backlog last.
pub fn refill_window(
    hot: &HotStore,
    n: usize,
    share: usize,
    k: usize,
    back: &HashMap<[u8; 9], Mark>,
) -> Result<Vec<Dirty>> {
    let k = k.clamp(1, MAX_REFILL_WINDOW);
    refill_with(
        hot,
        n.saturating_mul(k),
        share.max(1).saturating_mul(k),
        back,
    )
}

/// The keys of one refill window ([`refill_window`]), handed out a batch at a time. Every
/// key keeps the exact mark read at refill, so compare-and-clear still deletes only that
/// mark: a key the tail marks again while the window is in use keeps its new mark and is read
/// again later. The window is due for a refill once it is used up or [`WINDOW_MAX_AGE`] old.
#[derive(Debug, Default)]
pub struct RefillWindow {
    keys: Vec<Dirty>,
    pos: usize,
    taken: Option<Instant>,
}

impl RefillWindow {
    /// Whether to refill before the next batch: no keys left, or the marks were read
    /// `max_age` or longer before `now`.
    pub fn due(&self, now: Instant, max_age: Duration) -> bool {
        self.pos >= self.keys.len()
            || self
                .taken
                .is_none_or(|t| now.saturating_duration_since(t) >= max_age)
    }

    /// A new window: `keys` as refilled, its age counted from `at` (the end of the refill).
    pub fn set(&mut self, keys: Vec<Dirty>, at: Instant) {
        self.keys = keys;
        self.pos = 0;
        self.taken = Some(at);
    }

    /// [`Self::set`] without the keys of `read`: a window refilled while the batch `read` was
    /// in flight would hand its keys out again (their marks are cleared only at its commit).
    pub fn set_without(&mut self, mut keys: Vec<Dirty>, at: Instant, read: &[Dirty]) {
        let read: HashSet<[u8; 9]> = read.iter().map(|d| d.key).collect();
        keys.retain(|d| !read.contains(&d.key));
        self.set(keys, at);
    }

    /// The next batch: up to `n` keys, in the window's order.
    pub fn next_batch(&mut self, n: usize) -> Vec<Dirty> {
        let end = self.pos.saturating_add(n.max(1)).min(self.keys.len());
        let out = self.keys[self.pos..end].to_vec();
        self.pos = end;
        out
    }

    /// Keys not handed out yet.
    pub fn left(&self) -> usize {
        self.keys.len() - self.pos
    }

    /// Drop the window (a seed pass rewrote the marks): the next batch refills.
    pub fn clear(&mut self) {
        *self = Self::default();
    }
}

/// One walk of [`spawn_refill`]: the keys, the seconds the walk took and when it ended (the
/// window's age counts from there, PR #70 review M1).
pub type Refilled = (Vec<Dirty>, f64, Instant);

/// [`refill_window`] on a blocking thread: the keys, the seconds the walk took (also
/// observed as the `refill` phase) and when the walk ended.
pub fn spawn_refill(
    hot: Arc<HotStore>,
    n: usize,
    share: usize,
    k: usize,
    back: HashMap<[u8; 9], Mark>,
) -> tokio::task::JoinHandle<Result<Refilled>> {
    tokio::task::spawn_blocking(move || {
        let t = Instant::now();
        let keys = refill_window(&hot, n, share, k, &back)?;
        let ended = Instant::now();
        let secs = ended.saturating_duration_since(t).as_secs_f64();
        kdx_metrics::get().balance_batch_phase("refill", secs);
        Ok((keys, secs, ended))
    })
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

/// A `details` success.
#[derive(Clone)]
struct Answer {
    details: Details,
    /// The tip height the node read.
    as_of: u32,
    /// The hash of the block at `as_of` the node read (`metaData.prevBlockHash`), D2.
    prev_hash: Option<String>,
    /// A keyset reference resolved with `describe-keyset`: (name, keyset or none).
    resolved: Option<(String, Option<Value>)>,
}

#[derive(Clone)]
enum Outcome {
    Found(Box<Answer>),
    Absent(u32),
    Skipped,
    Failure,
    /// The answer arrived but is unusable here (`LocalError::Invalid`): the key stays queued.
    Invalid,
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
            Outcome::Invalid => "invalid",
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
    let out = outcome_of(local, it, r).await;
    kdx_metrics::get().balance_reconcile(out.label(), secs);
    out
}

/// The keyset a trusted row's guard refers to, read on the same chain (I-6); `None` when the
/// guard names none or the read failed (keys left as cached).
async fn resolve_ref(
    local: &LocalClient,
    it: &Item,
    guard: &Value,
) -> Option<(String, Option<Value>)> {
    match guard_keys(guard) {
        GuardKeys::Ref(name) if it.trusted => match local.describe_keyset(it.chain, &name).await {
            Ok(Local::Found { value: ks, .. }) => Some((name, Some(ks))),
            Ok(Local::Absent) => Some((name, None)),
            Err(e) => {
                tracing::debug!(error = %e, "describe-keyset failed; keys left as cached");
                None
            }
        },
        _ => None,
    }
}

/// What one `details` answer means for `it` (the single-key reading every path shares).
async fn outcome_of(local: &LocalClient, it: &Item, r: LocalResult<Details>) -> Outcome {
    let module = it.module_name.as_deref().unwrap_or("");
    match r {
        Ok(Local::Found {
            value,
            height,
            prev_hash,
        }) => found(local, it, value, height, prev_hash).await,
        Ok(Local::Absent) => Outcome::Absent(it.tail),
        Err(LocalError::Skipped(_)) => Outcome::Skipped,
        Err(LocalError::Failure(m)) => {
            tracing::debug!(module = %module, chain = it.chain, message = %m, "details failed");
            Outcome::Failure
        }
        Err(LocalError::Invalid(m)) => {
            // Never a silent drop (D3 gap group 2): the key stays dirty and is reported.
            tracing::warn!(
                module = %module,
                chain = it.chain,
                message = %m,
                "balances: details answer unusable; key kept dirty"
            );
            Outcome::Invalid
        }
        Err(LocalError::Node(e)) => {
            tracing::debug!(error = %e, "details: node error, kept dirty");
            Outcome::NodeError
        }
    }
}

/// A `details` success read at `height` on the block `prev_hash`.
async fn found(
    local: &LocalClient,
    it: &Item,
    details: Details,
    height: u64,
    prev_hash: Option<String>,
) -> Outcome {
    let as_of = u32::try_from(height).unwrap_or(u32::MAX);
    let resolved = resolve_ref(local, it, &details.guard).await;
    Outcome::Found(Box::new(Answer {
        details,
        as_of,
        prev_hash,
        resolved,
    }))
}

/// The answer was read more than [`LAG_BLOCKS`] below our tail on its chain.
fn lagging(height: u64, tail: u32) -> bool {
    height.saturating_add(u64::from(LAG_BLOCKS)) < u64::from(tail)
}

/// The answer was read more than [`LAG_BLOCKS`] above our tail on its chain: we hold no header
/// there yet, so it cannot be branch-checked (`Branch::Unknown`).
fn ahead(height: u64, tail: u32) -> bool {
    height > u64::from(tail).saturating_add(u64::from(LAG_BLOCKS))
}

/// The result label of one read-node call, for its metrics.
fn node_label<T>(r: &LocalResult<T>) -> &'static str {
    match r {
        Ok(Local::Found { .. }) => "ok",
        Ok(Local::Absent) => "absent",
        Err(LocalError::Node(_)) => "node_error",
        Err(LocalError::Invalid(_)) => "invalid",
        Err(LocalError::Failure(_) | LocalError::Skipped(_)) => "error",
    }
}

/// Whether an answer from node `primary` (true: `node.url`) decides the key here. `Absent`
/// carries no block identity, and a Pact failure or an unusable answer from another node may
/// be that node's own lag (a module it does not know yet): those are asked of the primary,
/// and never drop a mark or count toward the not-fungible verdict (PR #67 review I2).
fn decided_by<T>(r: &LocalResult<T>, primary: bool) -> bool {
    primary
        || !matches!(
            r,
            Ok(Local::Absent) | Err(LocalError::Failure(_) | LocalError::Invalid(_))
        )
}

/// Send one read through the pool with failover: a node error, or an answer more than
/// [`LAG_BLOCKS`] below our tail, marks the node down and the read goes to the next node;
/// once every node was tried, the last lagging answer (still exact, only old) or the last node
/// error stands. An answer more than [`LAG_BLOCKS`] above our tail, or a Pact failure from a
/// node other than the primary, marks that node down too and is returned at once (the
/// failure is then asked of the primary, [`decided_by`]).
/// `batch` picks with [`ReadPool::pick_batch`], else [`ReadPool::pick`]; the nodes of `avoid`
/// are not asked. Returns the result, the seconds of the call that stands and the index of
/// the node that answered.
async fn pooled<T, F, Fut>(
    pool: &ReadPool,
    tail: u32,
    keys: u64,
    batch: bool,
    avoid: &[usize],
    mut send: F,
) -> (LocalResult<T>, f64, usize)
where
    F: FnMut(Arc<LocalClient>) -> Fut,
    Fut: std::future::Future<Output = (LocalResult<T>, f64)>,
{
    let mut tried: Vec<usize> = avoid.to_vec();
    let mut last: Option<(LocalResult<T>, f64, usize)> = None;
    let pick = |t: &[usize]| {
        if batch {
            pool.pick_batch(t)
        } else {
            pool.pick(t)
        }
    };
    while let Some(i) = pick(&tried) {
        tried.push(i);
        let node = pool.node(i);
        let (r, secs) = send(node.client.clone()).await;
        let m = kdx_metrics::get();
        match &r {
            Err(LocalError::Node(_)) => {
                m.balance_local_node(&node.label, "node_error", 0);
                pool.mark_down(i, "node error");
                last = match last {
                    // A lagging answer beats a later node error.
                    Some(l @ (Ok(_), ..)) => Some(l),
                    _ => Some((r, secs, i)),
                };
            }
            Ok(Local::Found { height, .. }) if lagging(*height, tail) => {
                m.balance_local_node(&node.label, "lagging", 0);
                pool.mark_down(i, "lagging");
                last = Some((r, secs, i));
            }
            Ok(Local::Found { height, .. }) if ahead(*height, tail) => {
                // Not tried elsewhere: while our tail catches up every node is ahead, and the
                // answer (unchecked, stored unverified and read again) costs one call, not N.
                m.balance_local_node(&node.label, "ahead", 0);
                pool.mark_down(i, "ahead");
                return (r, secs, i);
            }
            Err(LocalError::Failure(_)) if !node.primary => {
                // A secondary that cannot resolve a module the primary knows (it lags) is
                // skipped for a while; the key is read again on the primary.
                m.balance_local_node(&node.label, "error", 0);
                pool.mark_down(i, "pact failure");
                return (r, secs, i);
            }
            _ => {
                let answered = match &r {
                    Ok(Local::Found { .. }) => keys,
                    Ok(Local::Absent) => 1,
                    _ => 0,
                };
                m.balance_local_node(&node.label, node_label(&r), answered);
                pool.mark_up(i);
                return (r, secs, i);
            }
        }
    }
    last.unwrap_or_else(|| {
        (
            Err(NodeError::Transport("no read node".into()).into()),
            0.0,
            0,
        )
    })
}

/// One key read on its own on the primary (`local`, `node.url`).
async fn read_primary(local: &LocalClient, it: &Item) -> Outcome {
    let (Some(acct), Some(module)) = (&it.account_name, &it.module_name) else {
        return Outcome::Skipped;
    };
    let (r, secs) = local.details_timed(it.chain, module, acct).await;
    let out = outcome_of(local, it, r).await;
    kdx_metrics::get().balance_reconcile(out.label(), secs);
    out
}

/// The highest mark height of the items of one key (a tail and a seed mark of it): an answer
/// below it cannot clear every mark of the key.
fn want_of(items: &[Item], k: &[usize]) -> u32 {
    k.iter().map(|&i| items[i].d.mark.height).max().unwrap_or(0)
}

/// Whether `r` is an answer read below height `want` (it cannot clear a mark at `want`).
fn below<T>(r: &LocalResult<T>, want: u32) -> bool {
    matches!(r, Ok(Local::Found { height, .. }) if *height < u64::from(want))
}

/// One key read on its own through the pool ([`ReadPool::pick`]), not on the nodes of
/// `avoid`. What a node other than the primary cannot decide ([`decided_by`]: `Absent`, a
/// Pact failure, an unusable answer) is asked again of the primary (`local`, `node.url`):
/// absent rows, dropped marks and the not-fungible verdict are exactly the single-node
/// reconciler's. An answer from another node read below `want` (it lags, within
/// [`LAG_BLOCKS`]) is asked once more of the other nodes, so a fresh mark is not held back by
/// that node's lag (PR #70 review I1); a node error there keeps the first answer.
///
/// Also returns when the first read answered (the rest went to re-reads, PR #70 review M2).
async fn read_single_at(
    local: &LocalClient,
    pool: &ReadPool,
    it: &Item,
    want: u32,
    avoid: &[usize],
) -> (Outcome, Instant) {
    let (Some(acct), Some(module)) = (&it.account_name, &it.module_name) else {
        return (Outcome::Skipped, Instant::now());
    };
    let send = |c: Arc<LocalClient>| async move { c.details_timed(it.chain, module, acct).await };
    let (mut r, mut secs, mut i) = pooled(pool, it.tail, 1, false, avoid, send).await;
    let answered = Instant::now();
    if below(&r, want) && !pool.node(i).primary {
        let mut avoid = avoid.to_vec();
        avoid.push(i);
        let again = pooled(pool, it.tail, 1, false, &avoid, send).await;
        if !matches!(again.0, Err(LocalError::Node(_))) {
            (r, secs, i) = again;
        }
    }
    if !decided_by(&r, pool.node(i).primary) {
        return (read_primary(local, it).await, answered);
    }
    let out = outcome_of(local, it, r).await;
    kdx_metrics::get().balance_reconcile(out.label(), secs);
    (out, answered)
}

/// [`read_single_at`] of the key whose items are `k`.
async fn read_single(local: &LocalClient, pool: &ReadPool, items: &[Item], k: &[usize]) -> Outcome {
    read_single_at(local, pool, &items[k[0]], want_of(items, k), &[])
        .await
        .0
}

/// One outcome for every item of `idx` (the same key, listed more than once in a batch).
fn fan(idx: &[usize], o: Outcome) -> Vec<(usize, Outcome)> {
    idx.iter().map(|&k| (k, o.clone())).collect()
}

/// Read the keys of `keys` (one chain, one module; each entry the item indexes of one key,
/// so a key is asked once) in one batched `details` call ([`LocalClient::details_many`], the
/// `try` form). An answer gives every key the same block (height and hash), checked per key
/// like a single answer. A present entry counts only when it names the key asked and parses;
/// any other entry is read alone through the pool. A `try` marker is never taken as absent,
/// for any module: that key is read alone on the primary, and that read decides, as before.
/// A failed call, an unusable answer or a refused call reads the keys one by one.
///
/// `batch` sends the call through [`ReadPool::pick_batch`] (the nodes other than the
/// primary), else through [`ReadPool::pick`] (the tip share, [`Dirty::tip`]). A key whose
/// mark the answer cannot clear (the answering node, not the primary, read below it) is read
/// once more alone through [`ReadPool::pick`], not on that node, so it can reach the primary
/// (PR #70 review I1); a node error there keeps the batched answer.
///
/// Also returns when the chunk's first read answered: the rest of its time went to the reads
/// it sent after (keys read alone, a `try` marker or `Absent` asked of the primary).
async fn read_chunk(
    local: &LocalClient,
    pool: &ReadPool,
    items: &[Item],
    keys: Vec<Vec<usize>>,
    batch: bool,
) -> (Vec<(usize, Outcome)>, Instant) {
    if keys.len() == 1 {
        let (o, at) = read_single_at(
            local,
            pool,
            &items[keys[0][0]],
            want_of(items, &keys[0]),
            &[],
        )
        .await;
        return (fan(&keys[0], o), at);
    }
    let first = &items[keys[0][0]];
    let module = first.module_name.as_deref().unwrap_or("");
    let accts: Vec<&str> = keys
        .iter()
        .map(|k| items[k[0]].account_name.as_deref().unwrap_or(""))
        .collect();
    let n = keys.len() as u64;
    let (r, secs, node) = pooled(pool, first.tail, n, batch, &[], |c| {
        let accts = accts.clone();
        async move { c.details_many(first.chain, module, &accts).await }
    })
    .await;
    let answered = Instant::now();
    let singles = |ks: Vec<Vec<usize>>| {
        join_all(ks.into_iter().map(|k| async move {
            let o = read_single(local, pool, items, &k).await;
            fan(&k, o)
        }))
    };
    match r {
        Ok(Local::Found {
            value,
            height,
            prev_hash,
        }) => {
            let (mut ok, mut again, mut on_primary) = (Vec::new(), Vec::new(), Vec::new());
            let mut resend = Vec::new();
            let lags = !pool.node(node).primary;
            for (k, e) in keys.into_iter().zip(value) {
                match e {
                    BatchEntry::Found(d) if lags && height < u64::from(want_of(items, &k)) => {
                        resend.push((k, d))
                    }
                    BatchEntry::Found(d) => ok.push((k, d)),
                    BatchEntry::Marker => on_primary.push(k),
                    BatchEntry::Unusable(why) => {
                        tracing::debug!(
                            chain = first.chain,
                            module,
                            why,
                            "details batch entry unusable; read alone"
                        );
                        again.push(k);
                    }
                }
            }
            let prev = prev_hash.as_ref();
            let take = |k: Vec<usize>, d: Details| async move {
                let o = found(local, &items[k[0]], d, height, prev.cloned()).await;
                kdx_metrics::get().balance_reconcile(o.label(), secs);
                fan(&k, o)
            };
            let (a, b, c, e) = futures::join!(
                join_all(ok.into_iter().map(|(k, d)| take(k, d))),
                singles(again),
                join_all(on_primary.into_iter().map(|k| async move {
                    let o = read_primary(local, &items[k[0]]).await;
                    fan(&k, o)
                })),
                join_all(resend.into_iter().map(|(k, d)| async move {
                    let it = &items[k[0]];
                    match read_single_at(local, pool, it, want_of(items, &k), &[node])
                        .await
                        .0
                    {
                        Outcome::NodeError => take(k, d).await,
                        o => fan(&k, o),
                    }
                })),
            );
            let out = a.into_iter().chain(b).chain(c).chain(e).flatten().collect();
            (out, answered)
        }
        Err(LocalError::Node(e)) => {
            tracing::debug!(error = %e, "details batch: node error on every read node, kept dirty");
            let out = keys
                .into_iter()
                .flatten()
                .map(|k| {
                    kdx_metrics::get().balance_reconcile("error", secs);
                    (k, Outcome::NodeError)
                })
                .collect();
            (out, answered)
        }
        other => {
            if let Err(e) = &other {
                tracing::debug!(chain = first.chain, module, error = %e, "details batch failed; keys read one by one");
            }
            (
                singles(keys).await.into_iter().flatten().collect(),
                answered,
            )
        }
    }
}

/// [`call`] for every item through the read pool: keys of one chain and module go in batched
/// calls of `pool.batch_keys` distinct keys ([`read_chunk`]), spread over the pool's nodes. A
/// key listed twice (a tail and a seed mark) is asked once and both items get its outcome.
/// Also returns when the last chunk's first read answered (`None`: no chunk was read).
async fn call_pooled(
    local: &LocalClient,
    pool: &ReadPool,
    items: &[Item],
) -> (Vec<Outcome>, Option<Instant>) {
    let mut out: Vec<Option<Outcome>> = (0..items.len()).map(|_| None).collect();
    type Group = (HashMap<u32, usize>, Vec<Vec<usize>>);
    let mut groups: BTreeMap<(u8, u32), Group> = BTreeMap::new();
    for (k, it) in items.iter().enumerate() {
        if let Some(l) = it.skip {
            kdx_metrics::get().balance_reconcile(l, 0.0);
            out[k] = Some(Outcome::Dropped(l));
        } else if it.account_name.is_none() || it.module_name.is_none() {
            out[k] = Some(Outcome::Skipped);
        } else {
            let (seen, keys) = groups.entry((it.chain, it.module)).or_default();
            match seen.get(&it.account) {
                Some(&p) => keys[p].push(k),
                None => {
                    seen.insert(it.account, keys.len());
                    keys.push(vec![k]);
                }
            }
        }
    }
    // The tip share's keys (a key with a tip item) go in their own chunks, through
    // `ReadPool::pick` (PR #70 review I1); the rest through `ReadPool::pick_batch`.
    let chunks = groups.into_values().flat_map(|(_, g)| {
        let (tip, rest): (Vec<_>, Vec<_>) = g
            .into_iter()
            .partition(|k| k.iter().any(|&i| items[i].d.tip));
        let cut = |ks: Vec<Vec<usize>>, batch: bool| {
            ks.chunks(pool.batch_keys)
                .map(|c| (c.to_vec(), batch))
                .collect::<Vec<_>>()
        };
        let mut out = cut(tip, false);
        out.extend(cut(rest, true));
        out
    });
    let mut first: Option<Instant> = None;
    for (outs, at) in
        join_all(chunks.map(|(c, batch)| read_chunk(local, pool, items, c, batch))).await
    {
        first = first.max(Some(at));
        for (k, o) in outs {
            out[k] = Some(o);
        }
    }
    // Every key has an outcome; were one missing, it stays dirty (a node error), never dropped.
    let out = out
        .into_iter()
        .map(|o| o.unwrap_or(Outcome::NodeError))
        .collect();
    (out, first)
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

/// Whether a new row may be written (I-7). Always for `coin` and for a key that has a row. A
/// module confirmed `fungible-v2` on this chain stops at [`Caps::fungible_module_rows`]
/// (F-14), an untrusted module at [`Caps::module_rows`] (and is flagged, so the tail stops marking new keys of it),
/// and an account takes at most [`Caps::account_rows`] untrusted rows: absent rows, rows of
/// not-fungible modules and trusted rows do not count, so dust from spam modules cannot fill
/// an account's cap and keep a real token's row out (security review ruling).
fn admit(txn: &mut RwTxn<'_>, it: &Item, coin: Option<u32>, caps: &Caps) -> Result<bool> {
    if Some(it.module) == coin || txn.get(Table::Balances, &it.d.key)?.is_some() {
        return Ok(true);
    }
    let t = balances::token_row(txn, it.module)?.unwrap_or_default();
    // F-14: a confirmed fungible-v2 module has its own (higher) row cap.
    let module_cap = if it.trusted {
        caps.fungible_module_rows
    } else {
        caps.module_rows
    };
    if t.rows >= module_cap {
        if !t.capped() {
            balances::update_token(txn, it.module, |t| t.flags |= balances::TOKEN_CAPPED)?;
            tracing::warn!(
                module = it.module_name.as_deref().unwrap_or(""),
                rows = t.rows,
                trusted = it.trusted,
                "balances: module reached its row cap; new keys are dropped"
            );
        }
        return Ok(false);
    }
    if it.trusted {
        return Ok(true);
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

/// I3 (PR #67 review): whether the row held was read at a later block than `as_of`, on our
/// branch. A lagging read node (up to [`LAG_BLOCKS`] behind, more as a last resort) can answer
/// from an older block than an earlier answer; that row is kept and the key is marked again,
/// so a later read decides. The mark is not cleared by the older answer: after a reorg the
/// newer row may be from an orphaned block, and only a read at or above its height replaces
/// it. An unverified row never holds: it may come from another branch or from a node ahead of
/// our headers, and a checked answer replaces it.
fn holds_newer(txn: &impl Txn, it: &Item, as_of: u32) -> Result<bool> {
    Ok(txn
        .get(Table::Balances, &it.d.key)?
        .and_then(|v| BalanceRow::decode(&v))
        .is_some_and(|o| o.as_of > as_of && !o.unverified))
}

/// Write one answer: row, guard keys (trusted modules only, I-6), holders, row count. Never
/// over a verified row read at a later block ([`holds_newer`]): that row is kept.
fn write_row(txn: &mut RwTxn<'_>, it: &Item, new: BalanceRow, coin: Option<u32>) -> Result<()> {
    let old = txn
        .get(Table::Balances, &it.d.key)?
        .and_then(|v| BalanceRow::decode(&v));
    if old
        .as_ref()
        .is_some_and(|o| o.as_of > new.as_of && !o.unverified)
    {
        return Ok(());
    }
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
/// `as_of` (SR-1, M-3). Only for an answer read on our branch ([`branch_of`]); an `Absent`
/// answer has no block identity to check (see the module docs).
fn clear_if_covered(txn: &mut RwTxn<'_>, it: &Item, as_of: u32) -> Result<()> {
    if it.d.mark.height <= as_of {
        clear_mark(txn, it.d.table(), &it.d.key, it.d.mark)?;
    }
    Ok(())
}

/// D2: where an answer was read, relative to the block we hold at its height.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Branch {
    /// `prevBlockHash` equals our stored header at `as_of`: the node read our branch.
    Ours,
    /// We hold a header at `as_of` and it has another hash: the node read another branch.
    Other,
    /// No hash, an unreadable one, or no header of ours at `as_of` (the node is ahead of our
    /// tail): the answer cannot be checked.
    Unknown,
}

/// D2: whether the answer read the block we hold at `as_of` on its chain: its `prevBlockHash`
/// (the hash of the node's tip, whose state `/local` read) against our stored header there. A
/// height alone does not say which branch it is on: right after a quick reorg flip-and-back the
/// node can still answer from the orphan's state at the same height. Shared by the fungible
/// reconcile and the NFT fill (`nft_fill`).
pub(crate) fn branch_of(
    txn: &impl Txn,
    chain: u8,
    as_of: u32,
    prev_hash: Option<&str>,
) -> Result<Branch> {
    let Some(prev) = prev_hash.and_then(|h| Hash32::from_b64url(h).ok()) else {
        return Ok(Branch::Unknown);
    };
    Ok(match txn.get(Table::Headers, &block_key(chain, as_of))? {
        Some(v) if decode_block(&v).context("hot header record")?.hash == prev => Branch::Ours,
        Some(_) => Branch::Other,
        None => Branch::Unknown,
    })
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
    /// Of `ok`: answers read on a block other than the header we hold at their height (another
    /// branch, or above our tail). Stored unverified; the key stays dirty (D2).
    pub other_branch: u64,
    /// Of `other_branch`: answers with no `metaData.prevBlockHash` at all.
    pub no_hash: u64,
    pub absent: u64,
    pub skipped: u64,
    pub failures: u64,
    /// Answers this build could not use (`LocalError::Invalid`): the key goes to the back of
    /// the queue like a node error, is parked after [`MAX_KEY_RETRIES`] in a row, and its
    /// module counts as failing in this batch unless another key of it answered (H-1).
    pub invalid: u64,
    pub node_errors: u64,
    /// Keys dropped without a call or a row (`not_fungible`, `capped`).
    pub dropped: u64,
    /// Keys whose call hit a node (transport or HTTP) error, with the mark the batch wrote
    /// for them (moved up to the tail height); still dirty.
    pub node_error_keys: Vec<([u8; 9], Mark)>,
    /// Keys with an invalid answer, with the mark the batch wrote for them; still dirty.
    pub invalid_keys: Vec<([u8; 9], Mark)>,
    /// Keys the batch marked again at the tail to send them to the back of the queue (a node
    /// error, an off-branch answer, an invalid seed answer), with the mark written. The loop
    /// keeps them out of the tip share ([`note_remarked`], [`refill_with`]).
    pub remarked: Vec<([u8; 9], Mark)>,
    /// Modules with an answer (`ok` or `absent`) in this batch.
    pub answered: HashSet<u32>,
    /// Non-coin modules with a failed or invalid `details` call in this batch.
    pub failed_modules: HashSet<u32>,
    /// Where the batch's time went (`kdx_balance_batch_phase_seconds`).
    pub phases: Phases,
}

/// Seconds one reconcile batch spent per phase, each also observed in
/// `kdx_balance_batch_phase_seconds{phase}`.
#[derive(Debug, Default, Clone, Copy)]
pub struct Phases {
    /// The read txn before the calls: tail heights, token rows, account and module names.
    pub prep: f64,
    /// The fungible checks (I-6) on the primary.
    pub fungible: f64,
    /// From the first `details` call to the last chunk's first (batched) answer.
    pub reads_batched: f64,
    /// After that, until every read ended: keys read alone, `try` markers and `Absent`
    /// answers asked again of the primary, keyset reads.
    pub reads_rereads: f64,
    /// Waiting for the write txn (the tail or another writer holds it).
    pub txn_wait: f64,
    /// Header (`branch_of`) and held-row (`holds_newer`) reads in the write txn.
    pub branch: f64,
    /// Rows, guards, key index and counts written in the write txn.
    pub write: f64,
    /// Marks cleared (compare-and-clear) or written again in the write txn.
    pub clear: f64,
    /// The write txn's commit.
    pub commit: f64,
}

impl Phases {
    fn observe(&self) {
        let m = kdx_metrics::get();
        for (phase, secs) in [
            ("prep", self.prep),
            ("fungible", self.fungible),
            ("reads_batched", self.reads_batched),
            ("reads_rereads", self.reads_rereads),
            ("txn_wait", self.txn_wait),
            ("branch", self.branch),
            ("write", self.write),
            ("clear", self.clear),
            ("commit", self.commit),
        ] {
            m.balance_batch_phase(phase, secs);
        }
    }
}

impl BatchStats {
    /// Calls the node answered or failed (B-1 refusals and drops make none).
    pub fn calls(&self) -> u64 {
        self.ok + self.absent + self.failures + self.invalid + self.node_errors
    }
}

/// Back off only when node errors are at least half of the batch's calls: the node itself is
/// failing. A minority of failing keys is a property of those keys (they go to the back of the
/// queue, and [`MAX_KEY_RETRIES`] parks them), and must not slow every other key.
pub fn backs_off(st: &BatchStats) -> bool {
    st.node_errors > 0 && st.node_errors * 2 >= st.calls()
}

/// The pause before the next batch when every answer of this one was read off our branch.
pub const OFF_BRANCH_PAUSE: Duration = Duration::from_secs(2);

/// M1 (D2 review): when every answer of a batch was read off our branch, wait
/// [`OFF_BRANCH_PAUSE`] before the next batch instead of re-reading the same keys at once.
pub fn off_branch_pause(st: &BatchStats) -> Option<Duration> {
    (st.other_branch > 0 && st.other_branch == st.ok + st.absent).then_some(OFF_BRANCH_PAUSE)
}

/// The pause before the next batch when every call of this one gave an invalid answer.
pub const INVALID_PAUSE: Duration = Duration::from_secs(30);

/// When every call of a batch gave an invalid answer, wait [`INVALID_PAUSE`]: invalid keys stay
/// queued (never dropped), so a queue holding only them would otherwise re-read them at once,
/// spinning the `/local` budget. Never stops re-reading, only spaces it out.
pub fn invalid_pause(st: &BatchStats) -> Option<Duration> {
    (st.invalid > 0 && st.invalid == st.calls()).then_some(INVALID_PAUSE)
}

/// The window [`BranchWatch`] judges off-branch answers over.
pub const BRANCH_WINDOW: Duration = Duration::from_secs(60);
/// The fewest `ok` answers in a window for [`BranchWatch`] to call off-branch answers dominant.
pub const BRANCH_WARN_MIN: u64 = 50;

/// What [`BranchWatch::note`] asks the loop to warn about.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BranchAlert {
    /// Answers in this batch with no `prevBlockHash` (at most one warning per window).
    pub no_hash: Option<u64>,
    /// (off-branch, ok) in the window just ended, when off-branch answers were the majority.
    pub dominant: Option<(u64, u64)>,
}

/// M1 (D2 review): watches off-branch answers over [`BRANCH_WINDOW`] windows. The caller
/// passes the time, so tests drive it with made-up instants.
#[derive(Debug, Default)]
pub struct BranchWatch {
    start: Option<Instant>,
    ok: u64,
    other: u64,
    no_hash_warned: Option<Instant>,
}

impl BranchWatch {
    /// Add one batch at `now`. A batch with answers lacking `prevBlockHash` alerts at once,
    /// then at most once per window. When a window ends, it alerts if off-branch answers were
    /// more than half of its `ok` answers and there were at least [`BRANCH_WARN_MIN`] of them;
    /// then a new window starts.
    pub fn note(&mut self, now: Instant, st: &BatchStats) -> BranchAlert {
        let mut alert = BranchAlert::default();
        if st.no_hash > 0
            && self
                .no_hash_warned
                .is_none_or(|t| now.duration_since(t) >= BRANCH_WINDOW)
        {
            alert.no_hash = Some(st.no_hash);
            self.no_hash_warned = Some(now);
        }
        let start = *self.start.get_or_insert(now);
        self.ok += st.ok;
        self.other += st.other_branch;
        if now.duration_since(start) >= BRANCH_WINDOW {
            if self.ok >= BRANCH_WARN_MIN && self.other * 2 > self.ok {
                alert.dominant = Some((self.other, self.ok));
            }
            *self = BranchWatch {
                start: Some(now),
                no_hash_warned: self.no_hash_warned,
                ..Default::default()
            };
        }
        alert
    }
}

/// Reconcile one batch of dirty keys, one `details` call per key on `local`.
pub async fn reconcile_batch(
    ctx: &Ctx,
    local: &LocalClient,
    keys: &[Dirty],
    caps: &Caps,
) -> Result<BatchStats> {
    reconcile_batch_with(ctx, local, None, keys, caps).await
}

/// [`reconcile_batch`] with the `details` reads sent through `pool` (batched per chain and
/// module, spread over its nodes); `local` (the primary, `node.url`) still makes the fungible
/// checks, the keyset reads and the re-read of an `Absent` answer from another node. Every
/// answer is checked against our store's branch the same way, whichever node gave it.
pub async fn reconcile_batch_with(
    ctx: &Ctx,
    local: &LocalClient,
    pool: Option<&ReadPool>,
    keys: &[Dirty],
    caps: &Caps,
) -> Result<BatchStats> {
    let now = now_unix();
    let mut ph = Phases::default();
    let t = Instant::now();
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
    ph.prep = secs_since(t);

    // I-6: the fungible check of each (module, chain) that needs one, before the calls.
    let mut pairs: Vec<(u32, u8, String)> = items
        .iter()
        .filter(|it| it.check)
        .filter_map(|it| Some((it.module, it.chain, it.module_name.clone()?)))
        .collect();
    pairs.sort_unstable();
    pairs.dedup();
    let t = Instant::now();
    let answers = join_all(
        pairs
            .iter()
            .map(|(_, c, m)| local.implements_fungible(*c, m)),
    )
    .await;
    ph.fungible = secs_since(t);
    let mut verdict: HashMap<(u32, u8), bool> = HashMap::new();
    for ((m, c, _), r) in pairs.iter().zip(answers) {
        match r {
            Ok(Local::Found { value, .. }) => {
                verdict.insert((*m, *c), value);
            }
            Ok(Local::Absent) | Err(LocalError::Failure(_) | LocalError::Invalid(_)) => {
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

    // In-flight calls are capped by each client's semaphore (`local_concurrency`).
    let t = Instant::now();
    let (outcomes, first): (Vec<Outcome>, Option<Instant>) = match pool {
        Some(p) => call_pooled(local, p, &items).await,
        None => (join_all(items.iter().map(|it| call(local, it))).await, None),
    };
    let end = Instant::now();
    let first = first.unwrap_or(end).clamp(t, end);
    ph.reads_batched = (first - t).as_secs_f64();
    ph.reads_rereads = (end - first).as_secs_f64();

    let mut st = BatchStats::default();
    let t = Instant::now();
    let mut txn = ctx.hot.rw()?;
    ph.txn_wait = secs_since(t);
    let t_write = Instant::now();
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
            Outcome::Found(a) => {
                let Answer {
                    details: d,
                    as_of,
                    prev_hash,
                    resolved,
                } = *a;
                let tb = Instant::now();
                let branch = branch_of(&txn, it.chain, as_of, prev_hash.as_deref())?;
                let ours = branch == Branch::Ours;
                // I3 (PR #67 review): a lagging read node's answer never moves a row back.
                let newer = holds_newer(&txn, it, as_of)?;
                // L1 (D2 review): an answer known to come from another branch does not replace
                // a row we hold: the row (balance, guard, key index) stays and is only flagged
                // unverified. With no row yet, or when the answer cannot be checked (no hash,
                // node ahead of our tail), the answer is stored unverified.
                let keep_old =
                    branch == Branch::Other && txn.get(Table::Balances, &it.d.key)?.is_some();
                ph.branch += secs_since(tb);
                if newer {
                    tracing::debug!(
                        chain = it.chain,
                        as_of,
                        "details answer older than the row held; row kept"
                    );
                } else if keep_old {
                    set_unverified(&mut txn, &it.d.key)?;
                } else {
                    if d.guard_capped {
                        // F-14: stored as null (no key rows); counted for `--status`.
                        let n = txn
                            .meta(balances::meta::GUARDS_CAPPED)?
                            .and_then(|v| v.try_into().ok().map(u64::from_be_bytes))
                            .unwrap_or(0);
                        txn.set_meta(
                            balances::meta::GUARDS_CAPPED,
                            &n.saturating_add(1).to_be_bytes(),
                        )?;
                        tracing::debug!(
                            module = it.module_name.as_deref().unwrap_or(""),
                            chain = it.chain,
                            "details: guard over the size cap, stored as null"
                        );
                    }
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
                        unverified: !ours,
                    };
                    write_row(&mut txn, it, row, coin)?;
                }
                let tc = Instant::now();
                if ours && newer {
                    // I3: the row held is newer than this answer; a later read decides the
                    // mark (to the back of the queue, like a node error, no retry count).
                    let m =
                        balances::mark_dirty(&mut txn, it.account, it.module, it.chain, it.tail)?;
                    st.remarked.push((it.d.key, m));
                } else if ours {
                    clear_if_covered(&mut txn, it, as_of)?;
                } else {
                    // D2: read on another branch (or above our tail). Served `stale`; the key
                    // stays dirty and goes to the back of the queue, like a node error but
                    // without a retry count (it is not the key's fault).
                    tracing::debug!(
                        chain = it.chain,
                        as_of,
                        prev_hash = prev_hash.as_deref().unwrap_or(""),
                        ?branch,
                        "details answer is not on our branch; kept dirty"
                    );
                    let m =
                        balances::mark_dirty(&mut txn, it.account, it.module, it.chain, it.tail)?;
                    st.remarked.push((it.d.key, m));
                    kdx_metrics::get().balance_reconcile("other_branch", 0.0);
                    st.other_branch += 1;
                    if prev_hash.is_none() {
                        st.no_hash += 1;
                    }
                }
                ph.clear += secs_since(tc);
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
                let tb = Instant::now();
                let newer = holds_newer(&txn, it, as_of)?;
                ph.branch += secs_since(tb);
                if newer {
                    // I3: never over a newer row; a later read decides the mark.
                    let tc = Instant::now();
                    let m =
                        balances::mark_dirty(&mut txn, it.account, it.module, it.chain, it.tail)?;
                    st.remarked.push((it.d.key, m));
                    ph.clear += secs_since(tc);
                } else {
                    write_row(&mut txn, it, row, coin)?;
                    let tc = Instant::now();
                    clear_if_covered(&mut txn, it, as_of)?;
                    ph.clear += secs_since(tc);
                }
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
            Outcome::Invalid => {
                // The answer came, only this build cannot use it. The mark is not dropped
                // here; [`MAX_KEY_RETRIES`] invalid answers in a row park the key (H-1), and a
                // module whose every answer is unusable counts as failing (R-P5P9-8 #2).
                // N-03: a tail mark keeps its place (it is not moved to the back), so the next
                // batches read the same keys and park them; moved to the back, a module with
                // more unusable keys than the retry map tracks would rotate them forever,
                // none reaching a second strike. A seed mark becomes a tail mark once.
                let m = if it.d.seed {
                    let m =
                        balances::mark_dirty(&mut txn, it.account, it.module, it.chain, it.tail)?;
                    st.remarked.push((it.d.key, m));
                    m
                } else {
                    it.d.mark
                };
                st.invalid += 1;
                st.invalid_keys.push((it.d.key, m));
                if Some(it.module) != coin {
                    st.failed_modules.insert(it.module);
                }
            }
            Outcome::NodeError => {
                // To the back of the queue: the mark moves up to the tail height (never down)
                // and out of the seed backlog, and stays out of the tip share (`remarked`).
                let m = balances::mark_dirty(&mut txn, it.account, it.module, it.chain, it.tail)?;
                st.remarked.push((it.d.key, m));
                st.node_errors += 1;
                st.node_error_keys.push((it.d.key, m));
            }
            Outcome::Dropped(_) => {
                clear_mark(&mut txn, it.d.table(), &it.d.key, it.d.mark)?;
                st.dropped += 1;
            }
        }
    }
    ph.write = (secs_since(t_write) - ph.branch - ph.clear).max(0.0);
    let t = Instant::now();
    txn.commit()?;
    ph.commit = secs_since(t);
    ph.observe();
    st.phases = ph;
    Ok(st)
}

fn secs_since(t: Instant) -> f64 {
    t.elapsed().as_secs_f64()
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

use crate::seed_receivers::Rules;

/// Clears `kdx_balance_receivers_pass{rules}` when the pass ends, however it ends.
struct PassGauge(&'static str);

/// The `rules` label of the receivers pass metrics.
fn rules_label(rules: Rules) -> &'static str {
    match rules {
        Rules::D1 => "d1",
        Rules::D3 => "d3",
    }
}

impl PassGauge {
    fn start(rules: Rules) -> PassGauge {
        let label = rules_label(rules);
        kdx_metrics::get()
            .balance_receivers_pass
            .get_or_create(&vec![("rules", label.into())])
            .set(1);
        PassGauge(label)
    }
}

impl Drop for PassGauge {
    fn drop(&mut self) {
        kdx_metrics::get()
            .balance_receivers_pass
            .get_or_create(&vec![("rules", self.0.into())])
            .set(0);
    }
}

/// D1: the receivers pass of the seed (`seed_receivers`), when the store has not had it, or
/// its stored history below coin v3 grew since. Postponed (to the next recheck) while the
/// store is not ready for it (review L3). It runs in [`receivers_task`], beside the reconcile
/// loop: tail, seed and miss-feed dirty rows are reconciled while it walks (review L4 kept it
/// inline when it was D1 alone; D3's first walk over every stored height held all of them
/// for hours on a fresh store). Returns false when stopped.
///
/// D3 (`Rules::D3`): the token receivers pass, same mechanics over every stored height; after
/// its first walk it is pending again whenever the tail grew, and walks only the new heights.
async fn receivers_pass(
    ctx: &Arc<Ctx>,
    stop: &CancellationToken,
    rules: crate::seed_receivers::Rules,
) -> Result<bool> {
    let opts = crate::seed_receivers::Opts {
        forks: crate::seed_receivers::forks_for(&ctx.opts.network),
        rules,
        rewalk_every_secs: crate::seed_receivers::REWALK_EVERY_SECS,
        // Walked in slices by the loop (`history_slice`), never inline (re-review N2).
        history_inline: false,
        ..Default::default()
    };
    let Some((from, to)) = crate::seed_receivers::pending_for(&ctx.hot.ro()?, &opts)? else {
        return Ok(true);
    };
    if let Err(e) = crate::seed_receivers::check_ready(&ctx.hot.ro()?, &ctx.segdir) {
        tracing::warn!(error = %e, ?rules, "balances: receivers pass postponed");
        return Ok(true);
    }
    tracing::info!(
        from,
        to,
        ?rules,
        "balances: marking receivers no event names (D1: coin below coin v3; D3: all modules)"
    );
    let _gauge = PassGauge::start(rules);
    let (c, s) = (ctx.clone(), stop.clone());
    let rep = tokio::task::spawn_blocking(move || {
        crate::seed_receivers::run(&c.hot, &c.dict, &c.segdir, &opts, &|| s.is_cancelled())
    })
    .await??;
    if rep.errors > 0 {
        tracing::error!(report = %serde_json::to_string(&rep)?, "balances: receivers pass ended with errors");
    } else {
        tracing::info!(report = %serde_json::to_string(&rep)?, "balances: receivers pass ended");
    }
    Ok(!rep.stopped)
}

/// Pause before a failed receivers pass is tried again (from its persisted cursor).
const RECEIVERS_RETRY: Duration = Duration::from_secs(60);

/// `kdx_balance_receivers_pass_failed{rules}`: 1 while the last attempt failed, else 0.
fn set_pass_failed(rules: Rules, failed: bool) {
    kdx_metrics::get()
        .balance_receivers_pass_failed
        .get_or_create(&vec![("rules", rules_label(rules).into())])
        .set(i64::from(failed));
}

/// One round of the receivers passes: D1, then D3 even when D1 failed (separate cursors,
/// PR #66 review I2). `failed` gets each attempt's outcome (the service sets
/// `kdx_balance_receivers_pass_failed{rules}`, [`set_pass_failed`]). Returns the pause before
/// the next round ([`RECEIVERS_RETRY`] after a failure, else [`RECHECK_SCAN`]), or `None`
/// when a pass was stopped.
async fn receivers_round<F, Fut, G>(mut pass: F, mut failed: G) -> Option<Duration>
where
    F: FnMut(Rules) -> Fut,
    G: FnMut(Rules, bool),
    Fut: std::future::Future<Output = Result<bool>>,
{
    let mut pause = RECHECK_SCAN;
    for rules in [Rules::D1, Rules::D3] {
        let r = pass(rules).await;
        failed(rules, r.is_err());
        match r {
            Ok(true) => {}
            Ok(false) => return None,
            Err(e) => {
                tracing::warn!(
                    error = format!("{e:#}"),
                    ?rules,
                    "balances: receivers pass failed; retried later from its cursor"
                );
                pause = RECEIVERS_RETRY;
            }
        }
    }
    Some(pause)
}

/// The D1 and D3 receivers passes in their own task, beside the reconcile loop (like
/// [`history_task`]): D1 then D3 at once, then again every [`RECHECK_SCAN`] after the last
/// round ended (D1: a tail still below coin v3 stores new pre-v3 txs; D3: the new heights of
/// the tail). A "not ready" or "no coin" postpone also waits the full [`RECHECK_SCAN`]. This
/// task is the only caller in the service, so a recheck never starts a pass while one is
/// running. Each pass resumes from its persisted cursor and commits per chunk; `stop` ends it
/// within one chunk. Errors are logged, raise `kdx_balance_receivers_pass_failed`, and are
/// retried after [`RECEIVERS_RETRY`], never fatal, as for the history walk.
async fn receivers_task(ctx: Arc<Ctx>, stop: CancellationToken) {
    tracing::info!("balances: receivers pass task started (D1, D3)");
    loop {
        let (c, s) = (&ctx, &stop);
        let pass = |rules| async move {
            let r = receivers_pass(c, s, rules).await;
            // F-52: a failed dictionary reload stops the process (and so this task).
            if let Err(e) = &r {
                crate::ctx::stop_on_reload_failure(e, s);
            }
            r
        };
        let Some(pause) = receivers_round(pass, set_pass_failed).await else {
            return;
        };
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(pause) => {}
        }
    }
}

/// Pause before a failed history walk is tried again (from its persisted cursor).
const HISTORY_RETRY: Duration = Duration::from_secs(60);
/// Pause between looks for a history walk to begin when none is pending.
const HISTORY_IDLE: Duration = Duration::from_secs(30);

/// Re-review N2: the D3 history walk for newly tracked tokens, in its own task so it never
/// waits on `/local` batches or the loop's idle sleep (and they never wait on it). It begins
/// or resumes the walk from its persisted cursor ([`crate::seed_receivers::history_begin`])
/// and walks it to the end in a blocking thread: frozen slices decoded by `threads` workers
/// and committed in TxNum order, one write txn per slice of `slice` txs. Errors are logged
/// and retried after [`HISTORY_RETRY`], never fatal; `stop` ends it within one slice.
async fn history_task(ctx: Arc<Ctx>, slice: u64, threads: usize, stop: CancellationToken) {
    let threads = if threads == 0 {
        crate::seed_receivers::default_history_threads()
    } else {
        threads
    };
    tracing::info!(threads, slice, "balances: D3 history walk task started");
    while !stop.is_cancelled() {
        let (c, s) = (ctx.clone(), stop.clone());
        let r = tokio::task::spawn_blocking(move || {
            history_walk(&c, slice, threads, &|| s.is_cancelled())
        })
        .await;
        let pause = match r {
            Ok(Ok(true)) => continue,
            Ok(Ok(false)) => HISTORY_IDLE,
            Ok(Err(e)) if crate::ctx::stop_on_reload_failure(&e, &stop) => return,
            Ok(Err(e)) => {
                // The next try resumes from the persisted cursor.
                tracing::warn!(error = %e, "balances: D3 history walk failed; retried later");
                HISTORY_RETRY
            }
            Err(e) => {
                tracing::error!(error = %e, "balances: D3 history walk task panicked; retried later");
                HISTORY_RETRY
            }
        };
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(pause) => {}
        }
    }
}

/// Walk the pending D3 history walk to its end or until `stop`. Returns whether there was one.
pub fn history_walk(
    ctx: &Ctx,
    slice: u64,
    threads: usize,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<bool> {
    let opts = crate::seed_receivers::Opts {
        forks: crate::seed_receivers::forks_for(&ctx.opts.network),
        rules: Rules::D3,
        rewalk_every_secs: crate::seed_receivers::REWALK_EVERY_SECS,
        ..Default::default()
    };
    let Some(mut w) =
        crate::seed_receivers::history_begin(&ctx.hot, &ctx.dict, &ctx.segdir, &opts)?
    else {
        return Ok(false);
    };
    let (t0, mut txs, mut last_log) = (Instant::now(), 0u64, Instant::now());
    while !stop() {
        let s = w.step_par(
            &ctx.hot,
            &ctx.dict,
            &ctx.segdir,
            &opts,
            slice,
            threads,
            stop,
        )?;
        txs += s.txs;
        if s.done {
            tracing::info!(tokens = s.tokens, txs, secs = t0.elapsed().as_secs(),
                report = %serde_json::to_string(&s.report)?, "balances: D3 history walk ended");
            break;
        }
        if last_log.elapsed() > Duration::from_secs(60) {
            let (next, hot) = w.position();
            tracing::info!(
                next,
                hot,
                txs,
                threads,
                rate = format!("{:.0}", txs as f64 / t0.elapsed().as_secs_f64().max(1e-3)),
                "balances: D3 history walk progress"
            );
            last_log = Instant::now();
        }
    }
    Ok(true)
}

/// `describe-module` reads per reconcile round for the D3 discovery queue (review M1).
pub const DISCOVER_READS_PER_ROUND: usize = 4;

/// Review M1: check a few queued discovered modules with the node; a module whose
/// `describe-module` lists `fungible-v2` on that chain becomes a tracked token.
async fn discovery_step(ctx: &Arc<Ctx>, local: &LocalClient) -> Result<()> {
    let queued = kdx_hot::balances::discovered_queue(&ctx.hot.ro()?, DISCOVER_READS_PER_ROUND)?;
    if queued.is_empty() {
        return Ok(());
    }
    let answers = join_all(
        queued
            .iter()
            .map(|(m, c, _)| local.implements_fungible(*c, m)),
    )
    .await;
    let verdicts: Vec<Option<bool>> = answers
        .into_iter()
        .map(|r| match r {
            Ok(Local::Found { value, .. }) => Some(value),
            Ok(Local::Absent) | Err(LocalError::Failure(_) | LocalError::Invalid(_)) => Some(false),
            Err(LocalError::Node(_) | LocalError::Skipped(_)) => None,
        })
        .collect();
    // The dictionary mutex may be held by a receivers pass for a whole chunk write: wait for
    // it on a blocking thread, not a runtime worker (PR #66 review M4).
    let c = ctx.clone();
    let (tracked, rejected) = tokio::task::spawn_blocking(move || {
        crate::seed_receivers::check_discovered(&c.hot, &c.dict, &queued, &verdicts)
    })
    .await??;
    if tracked + rejected > 0 {
        tracing::info!(tracked, rejected, "balances: D3 discovered modules checked");
    }
    Ok(())
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

/// Rows cleared per second of wall-clock time since the loop started: refill, discovery, the
/// history slice, the NFT steps and idle waits all count (unlike the in-batch `rate`, which
/// times `reconcile_batch` alone). Zero until a second has passed, so a first fast batch does
/// not report a huge rate.
fn wall_rate(done: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs < 1.0 {
        0.0
    } else {
        done as f64 / secs
    }
}

/// Write the heartbeat and the dirty gauge. `rate` is the in-batch EWMA, `wall_rate` the
/// wall-clock throughput of the loop ([`wall_rate`]); `--status` bases its ETA on the latter.
fn heartbeat(ctx: &Ctx, done: u64, rate: f64, wall_rate: f64) -> Result<usize> {
    let dirty = balances::dirty_count(&ctx.hot.ro()?)?;
    kdx_metrics::get().balance_dirty_rows.set(dirty as i64);
    let s = json!({
        "t": now_unix(), "done": done, "rate": rate, "wall_rate": wall_rate, "dirty": dirty
    });
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
    // The seed stays inline: posting-only and fast, and its marks must exist before refill
    // tells seed rows from tail rows. The receivers passes run beside the loop (below).
    if o.seed && !crate::seed::seeded(&ctx.hot.ro()?)? && !seed_pass(&ctx, &stop).await? {
        return Ok(());
    }
    let local = Arc::new(LocalClient::new(node, o.local));
    // D3 gap groups 3 and 4: rows no tx names, from the node's key listing, in its own task
    // (PR #27 H-1: a slow listing read never holds up this loop). It shares the `/local`
    // limits and ends with this loop (the guard cancels it on every return).
    let keylist_stop = stop.child_token();
    let _keylist_guard = keylist_stop.clone().drop_guard();
    if o.seed && !crate::keylist::enabled(o.local.concurrency) {
        tracing::warn!(
            "balances: key listing off: balances.local_concurrency = 1 would let one listing read hold the only /local slot"
        );
    }
    if o.seed && crate::keylist::enabled(o.local.concurrency) {
        tokio::spawn(crate::keylist::run(
            ctx.clone(),
            local.clone(),
            o.caps,
            o.keylist_skip.clone(),
            keylist_stop,
        ));
    }
    // N-11: expired not-fungible verdicts are lifted before the history walk starts, which
    // reads only modules not marked not fungible (the loop's first pass came after the spawn).
    let n = recheck_not_fungible(&ctx.hot, now_unix(), o.caps.recheck_secs)?;
    if n > 0 {
        tracing::info!(modules = n, "balances: not-fungible modules re-checked");
    }
    // Re-review N2: the D3 history walk in its own task; awaited on every return so no walk
    // write outlives the run.
    let history_stop = stop.child_token();
    let history = o.seed.then(|| {
        tokio::spawn(history_task(
            ctx.clone(),
            o.caps.history_slice_txs,
            o.caps.history_walk_threads,
            history_stop.clone(),
        ))
    });
    // The D1 and D3 receivers passes, beside the loop the same way and with the same stop.
    let receivers = o
        .seed
        .then(|| tokio::spawn(receivers_task(ctx.clone(), history_stop.clone())));
    let pool = o.read_nodes.pool(&local, o.local, o.batch_keys);
    tracing::info!(
        nodes = ?o.read_nodes,
        batch_keys = pool.batch_keys,
        "balances: details reads spread over the read nodes"
    );
    let r = reconcile_loop(&ctx, &local, &pool, &o, &stop, fill_retry, nft_retry).await;
    history_stop.cancel();
    for h in [history, receivers].into_iter().flatten() {
        let _ = h.await;
    }
    r
}

/// The reconcile loop of [`run_with`] after its first passes.
#[allow(clippy::too_many_arguments)]
async fn reconcile_loop(
    ctx: &Arc<Ctx>,
    local: &Arc<LocalClient>,
    pool: &ReadPool,
    o: &ReconcileOpts,
    stop: &CancellationToken,
    fill_retry: Duration,
    mut nft_retry: Option<Instant>,
) -> Result<()> {
    let (ctx, local, stop) = (ctx.clone(), local.clone(), stop.clone());
    let mut nft_cursor: Option<Vec<u8>> = None;
    let mut details_cursor: Option<Vec<u8>> = None;
    let mut backoff: Option<Duration> = None;
    let mut retries: HashMap<[u8; 9], u32> = HashMap::new();
    let mut back: HashMap<[u8; 9], Mark> = HashMap::new();
    let mut fails: HashMap<u32, u32> = HashMap::new();
    let mut last_recheck: Option<Instant> = None;
    let mut done = 0u64;
    let mut rate = 0.0f64;
    let started = Instant::now();
    let mut last_log = Instant::now();
    let mut watch = BranchWatch::default();
    let mut window = RefillWindow::default();
    let mut refill_secs = 0.0f64;
    let mut batch_secs = 0.0f64;
    loop {
        if stop.is_cancelled() {
            return Ok(());
        }
        if o.seed && take_mark_gap(&ctx.hot)? {
            gap_warn();
            window.clear();
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
            // The receivers passes' hourly recheck is `receivers_task`'s, never this loop's.
            let n = recheck_not_fungible(&ctx.hot, now_unix(), o.caps.recheck_secs)?;
            if n > 0 {
                tracing::info!(modules = n, "balances: not-fungible modules re-checked");
            }
            last_recheck = Some(Instant::now());
        }
        if o.seed {
            if let Err(e) = discovery_step(&ctx, &local).await {
                if !crate::ctx::stop_on_reload_failure(&e, &stop) {
                    tracing::warn!(error = %e, "balances: D3 discovery check failed; retried next round");
                }
            }
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
        // One walk of `BalanceDirty` per window of `refill_window_batches` batches; every
        // batch of it still gets its own write txn, bookkeeping and backoff below.
        let refill = |back: &HashMap<[u8; 9], Mark>| {
            spawn_refill(
                ctx.hot.clone(),
                o.batch,
                o.caps.module_share,
                o.caps.refill_window_batches,
                back.clone(),
            )
        };
        // PR #70 review M5: no walk and no slice once stopping (a failed dictionary reload
        // in `discovery_step` cancels `stop` and carries on to here).
        if stop.is_cancelled() {
            return Ok(());
        }
        if window.due(Instant::now(), WINDOW_MAX_AGE) {
            let (keys, secs, ended) = refill(&back).await??;
            refill_secs = secs;
            // The age counts from the end of the walk: counted from its start, a walk and
            // one slow batch would use up the age and every batch would refill again.
            window.set(keys, ended);
            if stop.is_cancelled() {
                return Ok(());
            }
        }
        let batch = window.next_batch(o.batch);
        if batch.is_empty() {
            kdx_metrics::get()
                .balance_dirty_rows
                .set(balances::dirty_count(&ctx.hot.ro()?)? as i64);
            // Off-branch NFT answers (D2) are not progress either: no immediate re-loop.
            if nft.rows() > nft.node_errors + nft.other_branch || det.calls() > det.node_errors {
                continue;
            }
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(IDLE) => continue,
            }
        }
        // The window is used up (or will be too old) after this batch: walk the table for the
        // next one while this batch reads. The new window leaves out this batch's keys (with a
        // window of one batch it would be that batch again: refilled after it, as before).
        let next = (o.caps.refill_window_batches > 1
            && !stop.is_cancelled()
            && (window.left() == 0
                || window.due(
                    Instant::now() + Duration::from_secs_f64(batch_secs),
                    WINDOW_MAX_AGE,
                )))
        .then(|| refill(&back));
        // The batch and its bookkeeping; the walk spawned above is awaited whatever they
        // return, so it never outlives the loop (PR #70 review M5: a `?` here used to drop its
        // handle and leave the walk running).
        let res: Result<BatchStats> = async {
            let t0 = Instant::now();
            let st = reconcile_batch_with(&ctx, &local, Some(pool), &batch, &o.caps).await?;
            batch_secs = secs_since(t0);
            let k = st.ok + st.absent + st.skipped + st.failures + st.dropped;
            done += k;
            let r = k as f64 / t0.elapsed().as_secs_f64().max(1e-3);
            rate = if rate == 0.0 { r } else { 0.8 * rate + 0.2 * r };
            let wall = wall_rate(done, started.elapsed());
            let dirty = heartbeat(&ctx, done, rate, wall)?;
            if last_log.elapsed() > Duration::from_secs(60) {
                tracing::info!(
                    done,
                    dirty,
                    rate = format!("{rate:.1}"),
                    wall_rate = format!("{wall:.1}"),
                    refill_secs = format!("{refill_secs:.2}"),
                    window_left = window.left(),
                    ?st,
                    "balances: reconcile"
                );
                last_log = Instant::now();
            }
            let t = Instant::now();
            after_batch(&ctx.hot, &mut fails, &mut retries, &batch, &st)?;
            kdx_metrics::get().balance_batch_phase("after", secs_since(t));
            note_remarked(&mut back, &batch, &st);
            Ok(st)
        }
        .await;
        let fresh = match next {
            Some(next) => Some(next.await),
            None => None,
        };
        let st = res?;
        if let Some(fresh) = fresh {
            let (keys, secs, ended) = fresh??;
            refill_secs = secs;
            window.set_without(keys, ended, &batch);
        }
        let alert = watch.note(Instant::now(), &st);
        if let Some(n) = alert.no_hash {
            tracing::warn!(
                answers = n,
                "balances: /local answers carry no metaData.prevBlockHash; rows cannot be \
                 verified and stay queued (D2)"
            );
        }
        if let Some((other, ok)) = alert.dominant {
            tracing::warn!(
                other_branch = other,
                ok,
                window_secs = BRANCH_WINDOW.as_secs(),
                "balances: most /local answers are not on our branch; rows stay unverified \
                 (node off our cut, behind a balancer, or our tail lagging)"
            );
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
            // M1: every answer was off our branch; re-reading the same keys at once would
            // only spin the `/local` budget. Never stops re-reading, only spaces it out.
            if let Some(d) = off_branch_pause(&st) {
                tracing::debug!(
                    answers = st.other_branch,
                    "balances: batch all off-branch, pausing"
                );
                tokio::select! {
                    _ = stop.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(d) => {}
                }
            } else if let Some(d) = invalid_pause(&st) {
                tracing::warn!(
                    invalid = st.invalid,
                    "balances: every answer of the batch was unusable; keys kept, pausing"
                );
                tokio::select! {
                    _ = stop.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(d) => {}
                }
            }
        }
    }
}

/// The bookkeeping after a batch: modules failing [`NOT_FUNGIBLE_AFTER`] batches in a row are
/// marked not fungible ([`note_failures`]), keys failing [`MAX_KEY_RETRIES`] batches in a row
/// are parked ([`parked_after`]).
pub fn after_batch(
    hot: &HotStore,
    fails: &mut HashMap<u32, u32>,
    retries: &mut HashMap<[u8; 9], u32>,
    batch: &[Dirty],
    st: &BatchStats,
) -> Result<()> {
    let gone = note_failures(fails, st);
    if !gone.is_empty() {
        mark_not_fungible(hot, &gone)?;
        tracing::warn!(
            modules = ?gone,
            "balances: modules whose details calls keep failing are marked not fungible"
        );
    }
    let refill_keys: Vec<([u8; 9], Mark)> = batch.iter().map(|d| (d.key, d.mark)).collect();
    let parked = parked_after(retries, &refill_keys, st);
    if !parked.is_empty() {
        let invalid: HashSet<[u8; 9]> = st.invalid_keys.iter().map(|(k, _)| *k).collect();
        park_keys(hot, &parked, &invalid)?;
    }
    Ok(())
}

/// Track the keys a batch sent to the back of the queue ([`BatchStats::remarked`]) for
/// [`refill_with`]: the batch's other keys are forgotten (answered, dropped, or kept in
/// place), its re-marked keys recorded with the mark written. Bounded like the retry map.
pub fn note_remarked(back: &mut HashMap<[u8; 9], Mark>, batch: &[Dirty], st: &BatchStats) {
    for d in batch {
        back.remove(&d.key);
    }
    if back.len() > MAX_TRACKED {
        back.clear();
    }
    back.extend(st.remarked.iter().copied());
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
    let errored: Vec<([u8; 9], Mark)> = st
        .node_error_keys
        .iter()
        .chain(&st.invalid_keys)
        .copied()
        .collect();
    count_retries(retries, batch, &errored)
}

/// Count consecutive node errors or invalid answers per key of a batch: keys that answered are forgotten; the
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
    park_keys(hot, keys, &HashSet::new())
}

/// [`park`], and the keys in `invalid` (parked for unusable answers, H-1) also go to the
/// parked-invalid list (`balances::parked_invalid`) for inspection and the explicit retry
/// pass (`reconcile-balances --all`), so they are not lost when no new tail mark comes.
pub fn park_keys(
    hot: &HotStore,
    keys: &[([u8; 9], Mark)],
    invalid: &HashSet<[u8; 9]>,
) -> Result<()> {
    let now = now_unix();
    let (mut parked, mut listed) = (0usize, 0usize);
    let mut txn = hot.rw()?;
    for (k, m) in keys {
        if clear_mark(&mut txn, Table::BalanceDirty, k, *m)? {
            set_unverified(&mut txn, k)?;
            kdx_metrics::get().balance_reconcile("parked", 0.0);
            parked += 1;
            if invalid.contains(k) && balances::record_parked_invalid(&mut txn, k, now)? {
                listed += 1;
            }
        }
    }
    txn.commit()?;
    tracing::warn!(
        keys = parked,
        invalid_listed = listed,
        retries = MAX_KEY_RETRIES,
        "balances: parked keys that kept failing at the node or answering unusably until \
         their next tail mark (unusable ones listed for reconcile-balances --all)"
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
        match run(ctx.clone(), node.clone(), o.clone(), stop.clone()).await {
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
pub(crate) mod tests {
    use std::io::{Read, Write};
    use std::sync::Mutex;

    use kdx_hot::{Geometry, HotDict, HotStore};
    use kdx_node::{ChainGraph, NodeClientConfig, NodeInfo};
    use kdx_types::{DictMut, Hash32};

    use super::*;
    use crate::IngestOpts;

    /// The hash of the header [`setup`] stores at chain 0 height 10: the fake node's tip.
    pub(crate) fn tip_hash() -> Hash32 {
        Hash32([0x11; 32])
    }

    /// Store a header at (`chain`, `height`) with `hash`, as the tail would.
    pub(crate) fn put_header(hot: &HotStore, chain: u8, height: u32, hash: Hash32) {
        let br = kdx_types::record::BlockRecord {
            hash,
            chain,
            height: u64::from(height),
            ..Default::default()
        };
        let mut buf = Vec::new();
        kdx_types::record::encode_block(&br, &mut buf);
        let mut txn = hot.rw().unwrap();
        txn.put(
            Table::Headers,
            &kdx_hot::tables::block_key(chain, height),
            &buf,
        )
        .unwrap();
        txn.commit().unwrap();
    }

    /// A fake node: `/local` answers a success for every account except `bad`, whose answer
    /// arrives but has no balance (invalid), and `fails`, a pact failure; an NFT `get-balance` (data `t`) answers 2.0. Every success
    /// reads tip height 10 (`blockHeight` 11) and names the tip's hash as `prevBlockHash`: the
    /// text in `prev`, read per answer (empty: no `prevBlockHash` at all). One
    /// connection at a time (`Connection: close`).
    /// `describe-module` reads the fake node received (all tests of this module).
    static DESCRIBES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    pub(crate) fn fake_node(prev: Arc<Mutex<String>>) -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        std::thread::spawn(move || {
            for s in l.incoming() {
                let Ok(mut s) = s else { return };
                let prev = prev.clone();
                // One thread per connection: a hung answer holds up no other.
                std::thread::spawn(move || {
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
                    let req: Value =
                        serde_json::from_slice(&buf[head_end..]).unwrap_or(Value::Null);
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
                    let code = cmd
                        .pointer("/payload/exec/code")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if code.starts_with("(at 'code (describe-module") {
                        // PR #27 H-1: the key listing's read hangs (a table too big to answer).
                        DESCRIBES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        std::thread::sleep(Duration::from_secs(30));
                        return;
                    }
                    let prev = prev.lock().unwrap().clone();
                    let meta = if prev.is_empty() {
                        json!({"blockHeight": 11})
                    } else {
                        json!({"blockHeight": 11, "prevBlockHash": prev})
                    };
                    let body = if let Some(m) = m {
                    // The fungible check: only `free.tok` lists fungible-v2.
                    json!({"result": {"status": "success", "data": m == "free.tok"},
                           "metaData": meta.clone()})
                } else if cmd.pointer("/payload/exec/data/t").is_some() {
                    // An NFT `get-balance`.
                    json!({"result": {"status": "success", "data": 2.0}, "metaData": meta.clone()})
                } else if acct == "huge" {
                    // F-14: a guard over the 4096-byte cap.
                    json!({"result": {"status": "success", "data": {"account": acct,
                           "balance": 1.5, "guard": {"keys": ["k".repeat(5000)],
                           "pred": "keys-all"}}}, "metaData": meta.clone()})
                } else if acct == "wide" {
                    // F-14: a keyset of 65 keys.
                    let keys: Vec<String> = (0..65).map(|i| format!("w{i}")).collect();
                    json!({"result": {"status": "success", "data": {"account": acct,
                           "balance": 1.5, "guard": {"keys": keys, "pred": "keys-all"}}},
                           "metaData": meta.clone()})
                } else if acct == "wiza" {
                    // D3 gap group 2: a full-precision Pact decimal (255 places, 257 bytes).
                    json!({"result": {"status": "success", "data": {"account": acct,
                           "balance": {"decimal": wiza_balance()}, "guard": null}},
                           "metaData": meta.clone()})
                } else if acct == "fails" {
                    json!({"result": {"status": "failure", "error": {"message": "boom"}},
                           "metaData": meta.clone()})
                } else if acct.starts_with("bad") {
                    json!({"result": {"status": "success", "data": {"guard": null}},
                           "metaData": meta.clone()})
                } else {
                    json!({"result": {"status": "success", "data": {"account": acct,
                           "balance": 1.5, "guard": {"keys": ["k1"], "pred": "keys-all"}}},
                           "metaData": meta.clone()})
                }
                .to_string();
                    let _ = write!(
                    s,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                );
                });
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
            legacy_segments: false,
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
        put_header(&hot, 0, 10, tip_hash());
        (hot, coin, ids)
    }

    /// A client of a fake node whose answers name the stored tip (`tip_hash`).
    fn client(dir: &std::path::Path, hot: &Arc<HotStore>) -> (Ctx, LocalClient) {
        client_on(dir, hot, Arc::new(Mutex::new(tip_hash().to_b64url())))
    }

    /// A client of a fake node whose answers name `prev` as `prevBlockHash`.
    pub(crate) fn client_on(
        dir: &std::path::Path,
        hot: &Arc<HotStore>,
        prev: Arc<Mutex<String>>,
    ) -> (Ctx, LocalClient) {
        let node = NodeClient::new(&NodeClientConfig {
            base: fake_node(prev).parse().unwrap(),
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

    fn row_of(hot: &HotStore, k: &[u8; 9]) -> BalanceRow {
        BalanceRow::decode(&hot.ro().unwrap().get(Table::Balances, k).unwrap().unwrap()).unwrap()
    }

    /// The `free.wiza` balance text of D3 gap group 2 (`0.0161938904166666…`, 255 places).
    pub(crate) fn wiza_balance() -> String {
        format!("0.0161938904166666{}8", "6".repeat(255 - 17))
    }

    /// D3 gap group 2: a 257-byte Pact decimal is a balance like any other.
    #[tokio::test]
    async fn a_full_precision_decimal_balance_is_stored() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _, _) = setup(dir.path(), &["wiza"]);
        let (ctx, local) = client(dir.path(), &hot);
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!((st.ok, st.failures, st.invalid), (1, 0, 0), "{st:?}");
        let txn = hot.ro().unwrap();
        assert_eq!(txn.count(Table::BalanceDirty).unwrap(), 0);
        let rows = balances::account_rows(&txn, 0).unwrap();
        assert_eq!(rows[0].2.balance, wiza_balance());
        assert!(rows[0].2.positive());
    }

    /// One key whose answer is unusable is counted as `invalid` and stays queued (its mark
    /// kept, never dropped: D3 gap group 2 lost 364 rows that way); the other keys of the batch
    /// reconcile, the batch does not back off and the module is not counted as failing.
    #[tokio::test]
    async fn one_invalid_answer_does_not_stall_the_batch() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["alice", "bad", "carol"]);
        let (ctx, local) = client(dir.path(), &hot);
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        assert_eq!(keys.len(), 3);
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!(
            (st.ok, st.failures, st.invalid, st.node_errors),
            (2, 0, 1, 0)
        );
        assert!(st.failed_modules.is_empty());
        assert!(!backs_off(&st));
        let bad = balances::bal_key(ids[1], coin, 0);
        assert_eq!(st.invalid_keys.len(), 1);
        assert_eq!(st.invalid_keys[0].0, bad);
        let txn = hot.ro().unwrap();
        // as_of = blockHeight 11 − 1 covers the marks at 10; the bad key's mark is kept.
        assert_eq!(txn.count(Table::BalanceDirty).unwrap(), 1);
        assert!(txn.get(Table::BalanceDirty, &bad).unwrap().is_some());
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
        // `fails`: the node answers a pact failure (a dropped mark); see `fake_node`.
        let (hot, coin, ids) = setup(dir.path(), &["fails", "parked"]);
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

    /// H-1 (PR #26 review): modules whose `details` answer is never usable (here every key
    /// answers without a balance) are marked not fungible after [`NOT_FUNGIBLE_AFTER`] batches,
    /// like failing ones. Their keys then stop taking batch slots, so the seed backlog (where
    /// `--rewalk` puts its marks) is read again and coin keeps progressing.
    #[tokio::test]
    async fn modules_with_unusable_answers_stop_taking_batches() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["carol"]);
        let junk = intern(&hot, DictKind::Module, &["free.junk1", "free.junk2"]);
        let names: Vec<String> = (0..12).map(|i| format!("bad{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let accts = intern(&hot, DictKind::Account, &refs);
        for (i, a) in accts.iter().enumerate() {
            mark(&hot, *a, junk[i % 2], 0, 10);
        }
        let alice = intern(&hot, DictKind::Account, &["alice"])[0];
        let seed_key = balances::bal_key(alice, coin, 0);
        let mut txn = hot.rw().unwrap();
        balances::seed_mark(&mut txn, &seed_key, &Mark { height: 0, seq: 1 }.encode()).unwrap();
        txn.commit().unwrap();
        let (ctx, local) = client(dir.path(), &hot);
        let (mut fails, mut retries) = (HashMap::new(), HashMap::new());
        let mut invalid = 0;
        for _ in 0..NOT_FUNGIBLE_AFTER + 3 {
            let keys = refill(&hot, 8, 4).unwrap();
            if keys.is_empty() {
                break;
            }
            let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
                .await
                .unwrap();
            invalid += st.invalid;
            after_batch(&hot, &mut fails, &mut retries, &keys, &st).unwrap();
        }
        assert!(invalid > 0);
        let txn = hot.ro().unwrap();
        for m in &junk {
            let t = balances::token_row(&txn, *m).unwrap().unwrap();
            assert_eq!(t.kind, KIND_NOT_FUNGIBLE);
        }
        assert_eq!(balances::dirty_count(&txn).unwrap(), 0);
        assert!(txn.get(Table::Balances, &seed_key).unwrap().is_some());
        assert!(txn
            .get(Table::Balances, &balances::bal_key(ids[0], coin, 0))
            .unwrap()
            .is_some());
    }

    /// N-03: unusable keys of a module that passed the fungible check (so it is never marked
    /// not fungible) keep their place in the queue, so the same keys come back and are parked
    /// after [`MAX_KEY_RETRIES`] batches. Moved to the back, a module with more such keys than
    /// the retry map tracks would rotate them forever, none ever reaching a second strike.
    #[tokio::test]
    async fn unusable_keys_of_a_fungible_module_are_parked() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _, _) = setup(dir.path(), &[]);
        let tok = intern(&hot, DictKind::Module, &["free.tok"])[0];
        let mut txn = hot.rw().unwrap();
        balances::update_token(&mut txn, tok, |t| t.fungible_chains = 1).unwrap();
        txn.commit().unwrap();
        let names: Vec<String> = (0..40).map(|i| format!("bad{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        for a in intern(&hot, DictKind::Account, &refs) {
            mark(&hot, a, tok, 0, 10);
        }
        let (ctx, local) = client(dir.path(), &hot);
        let (mut fails, mut retries) = (HashMap::new(), HashMap::new());
        for i in 0..MAX_KEY_RETRIES {
            let keys = refill(&hot, 8, 4).unwrap();
            assert_eq!(keys.len(), 4, "batch {i}");
            let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
                .await
                .unwrap();
            assert_eq!(st.invalid, 4, "batch {i}: {st:?}");
            after_batch(&hot, &mut fails, &mut retries, &keys, &st).unwrap();
        }
        let txn = hot.ro().unwrap();
        assert_eq!(balances::dirty_count(&txn).unwrap(), 36);
        assert_eq!(balances::parked_invalid(&txn).unwrap().len(), 4);
        assert!(retries.is_empty(), "{retries:?}");
        let t = balances::token_row(&txn, tok).unwrap().unwrap();
        assert_ne!(t.kind, KIND_NOT_FUNGIBLE);
        drop(txn);

        // PR #49 m-3: a park is not final. A new tx of a parked key marks it again (the
        // tail's one mark path), so the next batch reads it.
        let parked = balances::parked_invalid(&hot.ro().unwrap()).unwrap();
        let k = parked[0];
        let (a, m, c) = balances::parse_bal_key(&k).unwrap();
        let mut txn = hot.rw().unwrap();
        assert!(txn.get(Table::BalanceDirty, &k).unwrap().is_none());
        assert!(balances::mark_row(&mut txn, a, m, c, 20, None).unwrap());
        txn.commit().unwrap();
        assert_eq!(balances::dirty_count(&hot.ro().unwrap()).unwrap(), 37);
        let keys = refill(&hot, 64, 64).unwrap();
        assert!(
            keys.iter().any(|d| d.key == k && !d.seed),
            "re-marked key is queued"
        );
    }

    /// H-1: a key whose answer stays unusable while its module answers for others is parked
    /// after [`MAX_KEY_RETRIES`] batches (no more batch slots), counted as invalid each time,
    /// and kept in the parked-invalid list; the explicit pass (`reconcile-balances --all`)
    /// puts it back in the seed backlog.
    #[tokio::test]
    async fn an_unusable_key_is_parked_and_kept_for_a_retry_pass() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["alice", "bad"]);
        let bad = balances::bal_key(ids[1], coin, 0);
        let (ctx, local) = client(dir.path(), &hot);
        let (mut fails, mut retries) = (HashMap::new(), HashMap::new());
        for i in 0..MAX_KEY_RETRIES {
            assert!(mark_of(&hot, &bad).is_some(), "batch {i}");
            let keys = refill(&hot, 8, 4).unwrap();
            let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
                .await
                .unwrap();
            assert_eq!(st.invalid, 1, "batch {i}");
            after_batch(&hot, &mut fails, &mut retries, &keys, &st).unwrap();
        }
        assert!(mark_of(&hot, &bad).is_none());
        assert!(refill(&hot, 8, 4).unwrap().is_empty());
        assert_eq!(
            balances::parked_invalid(&hot.ro().unwrap()).unwrap(),
            vec![bad]
        );
        let mut txn = hot.rw().unwrap();
        assert_eq!(balances::requeue_parked_invalid(&mut txn).unwrap(), 1);
        txn.commit().unwrap();
        assert!(balances::parked_invalid(&hot.ro().unwrap())
            .unwrap()
            .is_empty());
        let keys = refill(&hot, 8, 4).unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys[0].key == bad && keys[0].seed);
    }

    /// M-3 and D2: a reorg between the read and the clear re-marks the key at the same height;
    /// the newer mark is kept for the next read. That next read can still come from the orphan's
    /// state at the same height (a quick flip-and-back): its `prevBlockHash` names the orphan,
    /// so the mark survives and the row is stored unverified. Only an answer read on the block
    /// we hold at that height clears it.
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
        // The re-apply put the other branch's block at 10; the node still reads the orphan.
        let canon = Hash32([0x22; 32]);
        put_header(&hot, 0, 10, canon);
        let prev = Arc::new(Mutex::new(tip_hash().to_b64url()));
        let (ctx, local) = client_on(dir.path(), &hot, prev.clone());
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!(st.ok, 1);
        assert!(mark_of(&hot, &k).is_some(), "the reorg's mark must survive");
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert!(
            mark_of(&hot, &k).is_some(),
            "an answer read on the orphan must not clear the mark"
        );
        assert!(row_of(&hot, &k).unverified);
        // The node moves to our branch: the next read clears the mark.
        *prev.lock().unwrap() = canon.to_b64url();
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert!(mark_of(&hot, &k).is_none());
        assert!(!row_of(&hot, &k).unverified);
    }

    /// D2: an answer whose `prevBlockHash` is not the header we hold at its height (another
    /// branch) is stored unverified (served `stale`) and its mark stays queued; the same read
    /// on our branch clears the mark and verifies the row.
    #[tokio::test]
    async fn an_answer_from_another_branch_keeps_the_mark_and_serves_stale() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["alice"]);
        let k = balances::bal_key(ids[0], coin, 0);
        let prev = Arc::new(Mutex::new(Hash32([0x33; 32]).to_b64url()));
        let (ctx, local) = client_on(dir.path(), &hot, prev.clone());
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!((st.ok, st.other_branch), (1, 1));
        assert!(mark_of(&hot, &k).is_some(), "mark must survive");
        let row = row_of(&hot, &k);
        assert!(row.unverified);
        assert_eq!((row.balance.as_str(), row.as_of), ("1.5", 10));
        assert!(balances::is_dirty(&hot.ro().unwrap(), ids[0], coin, 0).unwrap());

        *prev.lock().unwrap() = tip_hash().to_b64url();
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        assert_eq!(keys.len(), 1, "the key is still queued");
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!((st.ok, st.other_branch), (1, 0));
        assert!(mark_of(&hot, &k).is_none());
        assert!(!row_of(&hot, &k).unverified);
    }

    fn other_branch_count() -> u64 {
        kdx_metrics::get()
            .balance_reconcile
            .get_or_create(&vec![("result", "other_branch".into())])
            .get()
    }

    /// M1 (D2 review): an off-branch answer is counted under its own metric label, so a
    /// dashboard can tell it from a verified `ok`.
    #[tokio::test]
    async fn an_off_branch_answer_has_its_own_metric_label() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _, _) = setup(dir.path(), &["alice"]);
        let prev = Arc::new(Mutex::new(Hash32([0x33; 32]).to_b64url()));
        let (ctx, local) = client_on(dir.path(), &hot, prev);
        let before = other_branch_count();
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        // The registry is global and tests run in parallel: at least this test's answer.
        assert!(other_branch_count() > before);
    }

    /// M1: an answer with no `prevBlockHash` is off-branch (unverified, kept dirty) and counted
    /// in `no_hash` for the warning.
    #[tokio::test]
    async fn an_answer_without_prev_block_hash_is_counted_and_kept_dirty() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["alice"]);
        let k = balances::bal_key(ids[0], coin, 0);
        let (ctx, local) = client_on(dir.path(), &hot, Arc::new(Mutex::new(String::new())));
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!((st.ok, st.other_branch, st.no_hash), (1, 1, 1));
        assert!(mark_of(&hot, &k).is_some());
        assert!(row_of(&hot, &k).unverified);
    }

    /// M1: a batch whose answers were all off-branch pauses the loop briefly; a batch with one
    /// verified answer, or none at all, does not.
    #[test]
    fn an_all_off_branch_batch_pauses_the_next() {
        let all = BatchStats {
            ok: 3,
            other_branch: 3,
            ..Default::default()
        };
        assert_eq!(off_branch_pause(&all), Some(OFF_BRANCH_PAUSE));
        assert!(OFF_BRANCH_PAUSE <= Duration::from_secs(5), "bounded");
        let one_good = BatchStats {
            ok: 3,
            other_branch: 2,
            ..Default::default()
        };
        assert_eq!(off_branch_pause(&one_good), None);
        let with_absent = BatchStats {
            ok: 1,
            other_branch: 1,
            absent: 1,
            ..Default::default()
        };
        assert_eq!(off_branch_pause(&with_absent), None);
        assert_eq!(off_branch_pause(&BatchStats::default()), None);
        let errors = BatchStats {
            node_errors: 4,
            ..Default::default()
        };
        assert_eq!(off_branch_pause(&errors), None);
    }

    /// Invalid answers keep their keys queued: a batch of only those pauses the next one (no
    /// spin), a batch with any usable answer does not.
    #[test]
    fn an_all_invalid_batch_pauses_the_next() {
        let all = BatchStats {
            invalid: 3,
            ..Default::default()
        };
        assert_eq!(invalid_pause(&all), Some(INVALID_PAUSE));
        assert!(!backs_off(&all));
        let mixed = BatchStats {
            ok: 1,
            invalid: 3,
            ..Default::default()
        };
        assert_eq!(invalid_pause(&mixed), None);
        assert_eq!(invalid_pause(&BatchStats::default()), None);
    }

    /// M1: off-branch answers that are the majority of `ok` over a window, with enough answers,
    /// raise one alert at the window's end; a missing hash alerts at once, then at most once
    /// per window.
    #[test]
    fn the_branch_watch_alerts_on_a_majority_and_on_missing_hashes() {
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        let batch = |ok, other, no_hash| BatchStats {
            ok,
            other_branch: other,
            no_hash,
            ..Default::default()
        };
        let mut w = BranchWatch::default();
        assert_eq!(w.note(at(0), &batch(40, 30, 0)), BranchAlert::default());
        assert_eq!(w.note(at(30), &batch(40, 30, 0)), BranchAlert::default());
        assert_eq!(
            w.note(at(60), &batch(20, 10, 0)).dominant,
            Some((70, 100)),
            "70 of 100 in the window"
        );
        // A new window: a minority, then too few answers, never alert.
        assert_eq!(w.note(at(90), &batch(100, 40, 0)).dominant, None);
        assert_eq!(w.note(at(120), &batch(0, 0, 0)).dominant, None);
        assert_eq!(w.note(at(150), &batch(10, 10, 0)).dominant, None);
        assert_eq!(w.note(at(180), &batch(10, 10, 0)).dominant, None);
        // Missing hashes: at once, then quiet within the window, then again.
        assert_eq!(w.note(at(200), &batch(5, 5, 5)).no_hash, Some(5));
        assert_eq!(w.note(at(230), &batch(5, 5, 5)).no_hash, None);
        assert_eq!(w.note(at(261), &batch(5, 5, 2)).no_hash, Some(2));
    }

    /// L1 (D2 review): an answer from a definite other branch (we hold another header at its
    /// height) keeps the previous row's balance, guard and public-key index; the row is only
    /// flagged unverified and the mark stays.
    #[tokio::test]
    async fn a_definite_other_branch_answer_keeps_the_previous_row() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["alice"]);
        let k = balances::bal_key(ids[0], coin, 0);
        let old = BalanceRow {
            absent: false,
            balance: "7.0".into(),
            guard: [0; 16],
            as_of: 8,
            reconciled_at: 1,
            unverified: false,
        };
        {
            let mut txn = hot.rw().unwrap();
            txn.put(Table::Balances, &k, &old.encode()).unwrap();
            txn.commit().unwrap();
        }
        let prev = Arc::new(Mutex::new(Hash32([0x33; 32]).to_b64url()));
        let (ctx, local) = client_on(dir.path(), &hot, prev);
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!(st.other_branch, 1);
        let row = row_of(&hot, &k);
        assert_eq!(
            (row.balance.as_str(), row.as_of, row.guard),
            ("7.0", 8, [0; 16])
        );
        assert!(row.unverified);
        assert!(mark_of(&hot, &k).is_some());
        let idx = hot
            .ro()
            .unwrap()
            .get(
                Table::KeyAccounts,
                &balances::key_account_key(&balances::key_hash("k1"), ids[0]),
            )
            .unwrap();
        assert!(idx.is_none(), "the other branch's key is not indexed");
    }

    /// D2: an answer read at a height whose header we do not hold yet (the node is ahead of
    /// our tail) cannot be checked: it is treated like another branch's.
    #[tokio::test]
    async fn an_answer_above_our_headers_keeps_the_mark() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["alice"]);
        {
            let mut txn = hot.rw().unwrap();
            txn.del(Table::Headers, &kdx_hot::tables::block_key(0, 10))
                .unwrap();
            txn.commit().unwrap();
        }
        let k = balances::bal_key(ids[0], coin, 0);
        let (ctx, local) = client(dir.path(), &hot);
        let keys: Vec<_> = refill(&hot, 64, 32).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!(st.other_branch, 1);
        assert!(mark_of(&hot, &k).is_some());
        assert!(row_of(&hot, &k).unverified);
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

    /// PR #66 review I3: keys of a chain whose node fails for them are marked again at the tail
    /// (the newest marks of their chain) and must still go to the back of the queue: kept out
    /// of the tip share, they are not re-read every batch and parked while a backlog waits.
    /// A fresh tip mark of the same chain still gets the share.
    #[test]
    fn keys_marked_again_after_a_node_error_stay_out_of_the_tip_share() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, _) = setup(dir.path(), &[]);
        let names: Vec<String> = (0..43).map(|i| format!("acc{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let accts = intern(&hot, DictKind::Account, &refs);
        // An old backlog on chain 0; two keys at the tip of chain 1, whose node fails for them.
        for (i, a) in accts[..40].iter().enumerate() {
            mark(&hot, *a, coin, 0, 1 + i as u32);
        }
        for a in &accts[40..42] {
            mark(&hot, *a, coin, 1, 100);
        }
        let failing: HashSet<[u8; 9]> = accts[40..42]
            .iter()
            .map(|a| balances::bal_key(*a, coin, 1))
            .collect();
        let fresh = balances::bal_key(accts[42], coin, 1);
        let (n, tip_n) = (8, 8 * TIP_SHARE_PCT / 100);
        let (mut fails, mut retries, mut back) = (HashMap::new(), HashMap::new(), HashMap::new());
        for i in 0..MAX_KEY_RETRIES {
            if i == 3 {
                mark(&hot, accts[42], coin, 1, 101);
            }
            let keys = refill_with(&hot, n, 4, &back).unwrap();
            let tip: Vec<[u8; 9]> = keys[..tip_n].iter().map(|d| d.key).collect();
            if i >= 2 {
                assert!(
                    tip.iter().all(|k| !failing.contains(k)),
                    "batch {i}: {keys:?}"
                );
            }
            if i == 3 {
                assert!(tip.contains(&fresh), "batch {i}: {keys:?}");
            }
            let mut st = BatchStats::default();
            let mut txn = hot.rw().unwrap();
            for d in &keys {
                if failing.contains(&d.key) {
                    let a = u32::from_be_bytes(d.key[..4].try_into().unwrap());
                    let m = balances::mark_dirty(&mut txn, a, coin, 1, 100).unwrap();
                    st.node_errors += 1;
                    st.node_error_keys.push((d.key, m));
                    st.remarked.push((d.key, m));
                } else {
                    clear_mark(&mut txn, d.table(), &d.key, d.mark).unwrap();
                    st.ok += 1;
                }
            }
            txn.commit().unwrap();
            after_batch(&hot, &mut fails, &mut retries, &keys, &st).unwrap();
            note_remarked(&mut back, &keys, &st);
        }
        let txn = hot.ro().unwrap();
        assert!(
            balances::dirty_count(&txn).unwrap() > 2,
            "the backlog still waits"
        );
        for k in &failing {
            assert!(txn.get(Table::BalanceDirty, k).unwrap().is_some(), "parked");
            assert!(retries.get(k).copied().unwrap_or(0) < 2, "{retries:?}");
        }
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

    /// PR #66 review I2: a D1 failure does not keep D3 from running in the same round, and
    /// `kdx_balance_receivers_pass_failed{rules}` is 1 while the last attempt failed, 0 after
    /// one succeeds.
    #[tokio::test]
    async fn a_failed_d1_pass_still_runs_d3_and_raises_its_failed_gauge() {
        let ran = std::sync::Mutex::new(Vec::new());
        let mut gauge = HashMap::new();
        let mut set = |r: Rules, f: bool| {
            gauge.insert(rules_label(r), f);
        };
        let pause = receivers_round(
            |r| {
                ran.lock().unwrap().push(r);
                async move {
                    match r {
                        Rules::D1 => Err(anyhow::anyhow!("d1 broke")),
                        Rules::D3 => Ok(true),
                    }
                }
            },
            &mut set,
        )
        .await;
        assert_eq!(*ran.lock().unwrap(), vec![Rules::D1, Rules::D3]);
        assert_eq!(pause, Some(RECEIVERS_RETRY));
        assert_eq!((gauge["d1"], gauge["d3"]), (true, false));
        let pause = receivers_round(|_| async { Ok(true) }, &mut |r, f| {
            gauge.insert(rules_label(r), f);
        })
        .await;
        assert_eq!(pause, Some(RECHECK_SCAN));
        assert_eq!((gauge["d1"], gauge["d3"]), (false, false));
        let stopped = receivers_round(|_| async { Ok(false) }, &mut |_, _| {}).await;
        assert_eq!(stopped, None);
    }

    /// PR #66 review I1: a large backlog of old coin marks (D1/D3 at historic heights) does
    /// not starve the tip. A miner's mark at the newest height of its chain is in the first
    /// batch (the tip share is coin first, so the token's tip mark waits for the fair pass).
    #[test]
    fn the_tip_gets_its_share_of_a_batch_behind_an_old_backlog() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, _) = setup(dir.path(), &[]);
        let tok = intern(&hot, DictKind::Module, &["free.tok"])[0];
        let names: Vec<String> = (0..500).map(|i| format!("old{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let old = intern(&hot, DictKind::Account, &refs);
        let tip = intern(&hot, DictKind::Account, &["miner", "recv"]);
        assert!(refill(&hot, 16, 4).unwrap().is_empty());
        {
            let mut txn = hot.rw().unwrap();
            for (i, &a) in old.iter().enumerate() {
                balances::mark_dirty(&mut txn, a, coin, (i % 3) as u8, 10 + i as u32).unwrap();
                balances::mark_dirty(&mut txn, a, tok, 1, 10 + i as u32).unwrap();
            }
            txn.commit().unwrap();
        }
        // Marked last, at the chain's newest height.
        mark(&hot, tip[0], coin, 1, 5000);
        mark(&hot, tip[1], tok, 1, 5000);
        let b = refill(&hot, 16, 4).unwrap();
        assert_eq!(b.len(), 16);
        let has = |k: [u8; 9]| b.iter().any(|d| d.key == k && !d.seed);
        assert!(
            has(balances::bal_key(tip[0], coin, 1)),
            "miner in the first batch"
        );
        // The rest of the batch is still the oldest coin backlog, coin first.
        assert!(has(balances::bal_key(old[0], coin, 0)));
        let coin_rows = b.iter().filter(|d| module_of(&d.key) == coin).count();
        assert!(coin_rows >= 16 - TIP_SHARE_PCT * 16 / 100, "{coin_rows}");
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

    /// The wall-clock rate counts the whole elapsed time, waits included, and stays zero
    /// within the first second.
    #[test]
    fn wall_rate_counts_all_elapsed_time() {
        assert_eq!(wall_rate(600, Duration::from_secs(60)), 10.0);
        assert_eq!(wall_rate(600, Duration::from_secs(120)), 5.0);
        assert_eq!(wall_rate(600, Duration::from_millis(500)), 0.0);
    }

    /// Seed `count` rows of `module` (chain 0) for accounts named `{prefix}{i}`.
    fn seed_rows(hot: &HotStore, module: u32, prefix: &str, count: usize) {
        let names: Vec<String> = (0..count).map(|i| format!("{prefix}{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let accts = intern(hot, DictKind::Account, &refs);
        let mut txn = hot.rw().unwrap();
        let sm = Mark {
            height: 0,
            seq: txn.txn_id().unwrap(),
        }
        .encode();
        for &a in &accts {
            balances::seed_mark(&mut txn, &balances::bal_key(a, module, 0), &sm).unwrap();
        }
        txn.commit().unwrap();
    }

    /// A seed window that holds one non-coin module only still fills the whole batch: the
    /// per-module share orders the slots, it does not shrink the batch (the 2026-11 drain
    /// slowdown: batches of ~70 rows instead of 256).
    #[test]
    fn a_single_module_seed_window_fills_the_batch() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _coin, _ids) = setup(dir.path(), &[]);
        let spam = intern(&hot, DictKind::Module, &["free.spam"])[0];
        seed_rows(&hot, spam, "a", 400);
        let b = refill(&hot, 8, 4).unwrap();
        assert_eq!(b.len(), 8);
        assert!(b.iter().all(|d| d.seed && module_of(&d.key) == spam));
        let distinct: HashSet<_> = b.iter().map(|d| d.key).collect();
        assert_eq!(distinct.len(), 8);
    }

    /// With `coin` rows in the window too, the room `coin` leaves is topped up from the one
    /// module that has rows left, and `coin` stays uncapped.
    #[test]
    fn a_coin_plus_single_module_seed_window_fills_the_batch() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, _ids) = setup(dir.path(), &[]);
        let spam = intern(&hot, DictKind::Module, &["free.spam"])[0];
        seed_rows(&hot, coin, "c", 5);
        seed_rows(&hot, spam, "s", 400);
        let b = refill(&hot, 16, 4).unwrap();
        assert_eq!(b.len(), 16);
        assert_eq!(b.iter().filter(|d| module_of(&d.key) == coin).count(), 5);
        assert_eq!(b.iter().filter(|d| module_of(&d.key) == spam).count(), 11);
    }

    /// A mixed window still shares the slots fairly: two big modules split a small batch
    /// evenly, and a small module is served in full before a big one takes the rest.
    #[test]
    fn a_mixed_seed_window_still_shares_slots_fairly() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _coin, _ids) = setup(dir.path(), &[]);
        let mods = intern(
            &hot,
            DictKind::Module,
            &["free.big", "free.big2", "free.small"],
        );
        let (big, big2, small) = (mods[0], mods[1], mods[2]);
        seed_rows(&hot, big, "a", 60);
        seed_rows(&hot, big2, "b", 60);
        let b = refill(&hot, 8, 4).unwrap();
        assert_eq!(b.len(), 8);
        assert_eq!(b.iter().filter(|d| module_of(&d.key) == big).count(), 4);
        assert_eq!(b.iter().filter(|d| module_of(&d.key) == big2).count(), 4);
        // A small module joins: it gets its two rows, the big ones split what is left.
        seed_rows(&hot, small, "z", 2);
        let b = refill(&hot, 16, 4).unwrap();
        assert_eq!(b.len(), 16);
        let count = |m| b.iter().filter(|d| module_of(&d.key) == m).count();
        assert_eq!((count(small), count(big), count(big2)), (2, 7, 7));
        // Slots are dealt round-robin: the first round holds one row of each module.
        let first: HashSet<_> = b[..3].iter().map(|d| module_of(&d.key)).collect();
        assert_eq!(first.len(), 3);
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

    /// F-14: an over-size guard is stored as null (the balance row is still written and a
    /// counter records it) and gives no `KeyAccounts` rows; a 65-key keyset indexes nothing; a
    /// confirmed fungible-v2 module stops at its own row cap and is flagged.
    #[tokio::test]
    async fn oversize_guards_and_the_fungible_row_cap() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _, _) = setup(dir.path(), &["alice"]);
        let tok = intern(&hot, DictKind::Module, &["free.tok"])[0];
        let accts = intern(&hot, DictKind::Account, &["huge", "wide", "carol"]);
        mark(&hot, accts[0], tok, 0, 10);
        mark(&hot, accts[1], tok, 0, 10);
        let (ctx, local) = client(dir.path(), &hot);
        let keys = refill(&hot, 64, 32).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!((st.ok, st.dropped), (3, 0));
        let txn = hot.ro().unwrap();
        let huge = row_of(&hot, &balances::bal_key(accts[0], tok, 0));
        assert_eq!((huge.absent, huge.balance.as_str()), (false, "1.5"));
        assert_eq!(huge.guard, guard_hash(b"null"));
        assert!(row_of(&hot, &balances::bal_key(accts[1], tok, 0)).positive());
        // Only alice's coin row ("k1") is in the key index.
        assert_eq!(txn.count(Table::KeyAccounts).unwrap(), 1);
        assert_eq!(
            txn.meta(balances::meta::GUARDS_CAPPED)
                .unwrap()
                .map(|v| u64::from_be_bytes(v[..].try_into().unwrap())),
            Some(1)
        );
        drop(txn);
        // Row cap 2 for confirmed fungible-v2 modules: free.tok has 2 rows, a third is dropped.
        let caps = Caps {
            fungible_module_rows: 2,
            ..Caps::default()
        };
        mark(&hot, accts[2], tok, 0, 10);
        let keys = refill(&hot, 64, 32).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &caps).await.unwrap();
        assert_eq!((st.ok, st.dropped), (0, 1));
        let txn = hot.ro().unwrap();
        let t = balances::token_row(&txn, tok).unwrap().unwrap();
        assert!(t.capped() && t.rows == 2);
        assert!(txn
            .get(Table::Balances, &balances::bal_key(accts[2], tok, 0))
            .unwrap()
            .is_none());
        // A key that already has a row is still reconciled at the cap.
        drop(txn);
        mark(&hot, accts[0], tok, 0, 20);
        let keys = refill(&hot, 64, 32).unwrap();
        let st = reconcile_batch(&ctx, &local, &keys, &caps).await.unwrap();
        assert_eq!((st.ok, st.dropped), (1, 0));
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
        // D1 L3: the receivers pass waits for a tail; this store's tail reached height 1.
        let mut txn = hot.rw().unwrap();
        kdx_hot::set_checkpoint(
            &mut txn,
            kdx_hot::Stage::Tail,
            0,
            1,
            kdx_types::Hash32([1; 32]),
        )
        .unwrap();
        txn.commit().unwrap();
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
            keylist_skip: Vec::new(),
            batch_keys: 1,
            read_nodes: ReadNodes::default(),
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
        // D1: `run` also runs the receivers pass once (here: nothing stored below the cutoff).
        until("the receivers pass", || {
            let txn = hot.ro().unwrap();
            txn.meta(balances::meta::BALANCES_SEED_RECEIVERS)
                .unwrap()
                .is_some()
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

    /// A receivers pass's write for one row, as `seed_receivers` makes it in its own write
    /// txn: skip a verified row read at or above `height` (review H1), else mark it through
    /// the postings' mark path. Returns whether it marked.
    fn pass_marks(hot: &HotStore, a: u32, coin: u32, height: u32) -> bool {
        let mut txn = hot.rw().unwrap();
        let k = balances::bal_key(a, coin, 0);
        let row = txn.get(Table::Balances, &k).unwrap();
        if crate::seed_receivers::fresh_row(row.as_deref(), height) {
            return false;
        }
        assert!(balances::mark_row(&mut txn, a, coin, 0, height, Some(coin)).unwrap());
        txn.commit().unwrap();
        true
    }

    /// The row, its holder and row counts and its `KeyAccounts` entry after a background
    /// receivers pass and the loop wrote in some order: exactly one verified row with the
    /// node's value, counted once, no mark left.
    fn assert_exact(hot: &HotStore, a: u32, coin: u32) {
        let txn = hot.ro().unwrap();
        let row = txn
            .get(Table::Balances, &balances::bal_key(a, coin, 0))
            .unwrap()
            .as_deref()
            .and_then(BalanceRow::decode)
            .expect("row");
        assert_eq!(
            (row.balance.as_str(), row.as_of, row.unverified),
            ("1.5", 10, false)
        );
        let t = balances::token_row(&txn, coin).unwrap().unwrap();
        assert_eq!((t.holders, t.rows), (1, 1), "counted once");
        assert_eq!(txn.count(Table::Balances).unwrap(), 1);
        assert_eq!(txn.count(Table::KeyAccounts).unwrap(), 1);
        assert_eq!(balances::dirty_count(&txn).unwrap(), 0, "no mark left");
    }

    /// Refill and reconcile until no mark is left (at most `rounds` batches).
    async fn drain(ctx: &Ctx, local: &LocalClient, hot: &HotStore, rounds: usize) -> usize {
        for i in 0..rounds {
            let keys = refill(hot, 64, 32).unwrap();
            if keys.is_empty() {
                return i;
            }
            reconcile_batch(ctx, local, &keys, &Caps::default())
                .await
                .unwrap();
        }
        rounds
    }

    /// A receivers pass in the background and the loop write the same `(account, coin,
    /// chain)` in every order; MDBX serialises their write txns, so each order is one of
    /// these. The final row is exact in all of them.
    #[tokio::test]
    async fn a_background_receivers_pass_and_the_loop_interleave_exactly() {
        // 1. The pass marks between the loop's refill and its write: the loop's
        //    compare-and-clear (SR-1) keeps the newer mark, and a second batch re-reads.
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["k:a"]);
        let (ctx, local) = client(dir.path(), &hot);
        let keys = refill(&hot, 64, 32).unwrap();
        assert_eq!(keys.len(), 1);
        assert!(
            pass_marks(&hot, ids[0], coin, 10),
            "no row yet: the pass marks"
        );
        reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert_eq!(
            balances::dirty_count(&hot.ro().unwrap()).unwrap(),
            1,
            "the pass's mark survives the loop's write"
        );
        assert_eq!(drain(&ctx, &local, &hot, 4).await, 1);
        assert_exact(&hot, ids[0], coin);

        // 2. The loop first, then the pass: the row was read at the tip (as_of 10), at or
        //    above the credit, so the pass skips it and nothing is re-read.
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["k:a"]);
        let (ctx, local) = client(dir.path(), &hot);
        assert_eq!(drain(&ctx, &local, &hot, 4).await, 1);
        assert!(
            !pass_marks(&hot, ids[0], coin, 10),
            "a fresh row is skipped"
        );
        assert_exact(&hot, ids[0], coin);
        //    A row the loop flagged unverified (a failed re-read) is marked again and fixed.
        let mut txn = hot.rw().unwrap();
        balances::set_unverified(&mut txn, &balances::bal_key(ids[0], coin, 0)).unwrap();
        txn.commit().unwrap();
        assert!(pass_marks(&hot, ids[0], coin, 10));
        assert_eq!(drain(&ctx, &local, &hot, 4).await, 1);
        assert_exact(&hot, ids[0], coin);

        // 3. The pass first, over the tail's mark: one mark (the higher height), one read.
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["k:a"]);
        let (ctx, local) = client(dir.path(), &hot);
        assert!(pass_marks(&hot, ids[0], coin, 7));
        assert_eq!(balances::dirty_count(&hot.ro().unwrap()).unwrap(), 1);
        assert_eq!(drain(&ctx, &local, &hot, 4).await, 1);
        assert_exact(&hot, ids[0], coin);
    }

    /// Re-review N2: a pending D3 history walk runs in slices between the loop's other steps;
    /// dirty rows are reconciled while it is still pending, and the walk then completes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dirty_rows_are_reconciled_while_a_history_walk_is_pending() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _, _) = setup(dir.path(), &["k:a"]);
        let segdir = dir.path().join("segments");
        std::fs::create_dir_all(&segdir).unwrap();
        let mut txn = hot.rw().unwrap();
        kdx_hot::set_checkpoint(
            &mut txn,
            kdx_hot::Stage::Tail,
            0,
            1,
            kdx_types::Hash32([1; 32]),
        )
        .unwrap();
        // D3 done through height 60,000; a token first seen at 5 waits for its history walk.
        txn.set_meta(
            balances::meta::BALANCES_SEED_TOKEN_RECEIVERS,
            &60_000u32.to_be_bytes(),
        )
        .unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let tok = dict.intern(DictKind::Module, "free.t");
        dict.persist(&mut txn).unwrap();
        balances::note_token(&mut txn, tok, 5).unwrap();
        // 50,000 stored txs below the done height (unreadable records count as pass errors):
        // at one tx per slice the walk needs 50,000 slices, far longer than one batch.
        for h in 2..50_002u32 {
            let k = kdx_types::TxKey::new(h, 0, 1);
            txn.put(Table::TailTxs, &k.to_be_bytes(), b"\xffnot a tx record")
                .unwrap();
        }
        txn.commit().unwrap();
        assert_eq!(
            balances::rewalk_pending(&hot.ro().unwrap()).unwrap(),
            vec![tok]
        );
        let (ctx, _) = client(dir.path(), &hot);
        let node = ctx.node.clone();
        let o = ReconcileOpts {
            local: LocalOpts {
                rps: 0,
                concurrency: 4,
            },
            batch: 10,
            seed: true,
            caps: Caps {
                history_slice_txs: 1,
                ..Caps::default()
            },
            keylist_skip: Vec::new(),
            batch_keys: 1,
            read_nodes: ReadNodes::default(),
        };
        let stop = CancellationToken::new();
        let task = tokio::spawn(run_with(
            Arc::new(ctx),
            node,
            o,
            stop.clone(),
            Duration::ZERO,
        ));
        let walk_pending = || {
            !balances::rewalk_pending(&hot.ro().unwrap())
                .unwrap()
                .is_empty()
        };
        until("the dirty row reconciled", || {
            assert!(!task.is_finished(), "run ended");
            balances::dirty_count(&hot.ro().unwrap()).unwrap() == 0
        })
        .await;
        assert!(walk_pending(), "the walk ran inline before the dirty rows");
        let end = Instant::now() + Duration::from_secs(300);
        while walk_pending() {
            assert!(!task.is_finished(), "run ended");
            assert!(Instant::now() < end, "the history walk never ended");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    /// The history walk runs in its own task: it starts without waiting for a batch, and a
    /// stop ends the run (and the walk) within a slice, leaving the walk resumable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stop_ends_a_running_history_walk_promptly() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _, _) = setup(dir.path(), &[]);
        std::fs::create_dir_all(dir.path().join("segments")).unwrap();
        let mut txn = hot.rw().unwrap();
        kdx_hot::set_checkpoint(
            &mut txn,
            kdx_hot::Stage::Tail,
            0,
            1,
            kdx_types::Hash32([1; 32]),
        )
        .unwrap();
        txn.set_meta(
            balances::meta::BALANCES_SEED_TOKEN_RECEIVERS,
            &200_000u32.to_be_bytes(),
        )
        .unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let tok = dict.intern(DictKind::Module, "free.t");
        dict.persist(&mut txn).unwrap();
        balances::note_token(&mut txn, tok, 5).unwrap();
        for h in 2..150_002u32 {
            let k = kdx_types::TxKey::new(h, 0, 1);
            txn.put(Table::TailTxs, &k.to_be_bytes(), b"\xffnot a tx record")
                .unwrap();
        }
        txn.commit().unwrap();
        let (ctx, _) = client(dir.path(), &hot);
        let node = ctx.node.clone();
        let o = ReconcileOpts {
            local: LocalOpts {
                rps: 0,
                concurrency: 4,
            },
            batch: 10,
            seed: true,
            caps: Caps {
                history_slice_txs: 10,
                history_walk_threads: 4,
                ..Caps::default()
            },
            keylist_skip: Vec::new(),
            batch_keys: 1,
            read_nodes: ReadNodes::default(),
        };
        let stop = CancellationToken::new();
        let task = tokio::spawn(run_with(
            Arc::new(ctx),
            node,
            o,
            stop.clone(),
            Duration::ZERO,
        ));
        let walking = || {
            hot.ro()
                .unwrap()
                .meta(balances::meta::D3_REWALK_WALK)
                .unwrap()
                .is_some()
        };
        until("the walk started", || {
            assert!(!task.is_finished(), "run ended");
            walking()
        })
        .await;
        stop.cancel();
        let t0 = Instant::now();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the run and its walk end within 5 s of a stop")
            .unwrap()
            .unwrap();
        assert!(t0.elapsed() < Duration::from_secs(5));
        assert!(
            walking(),
            "the walk is still pending, resumable from its cursor"
        );
        assert_eq!(
            balances::rewalk_pending(&hot.ro().unwrap()).unwrap(),
            vec![tok]
        );
    }

    /// PR #27 H-1: the key listing runs in its own task. While its `describe-module` read of
    /// a tracked token hangs, the loop still reconciles a dirty row, and a stop ends the run
    /// at once (the hung read is not awaited).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_hung_key_listing_read_does_not_hold_up_the_reconcile() {
        use std::sync::atomic::Ordering::SeqCst;
        let dir = tempfile::tempdir().unwrap();
        let (hot, coin, ids) = setup(dir.path(), &["k:a", "k:b"]);
        let segdir = dir.path().join("segments");
        std::fs::create_dir_all(&segdir).unwrap();
        let mut txn = hot.rw().unwrap();
        kdx_hot::set_checkpoint(
            &mut txn,
            kdx_hot::Stage::Tail,
            0,
            1,
            kdx_types::Hash32([1; 32]),
        )
        .unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let tok = dict.intern(DictKind::Module, "free.slow");
        dict.persist(&mut txn).unwrap();
        balances::note_token(&mut txn, tok, 5).unwrap();
        txn.commit().unwrap();
        let before = DESCRIBES.load(SeqCst);
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
            keylist_skip: Vec::new(),
            batch_keys: 1,
            read_nodes: ReadNodes::default(),
        };
        let stop = CancellationToken::new();
        let task = tokio::spawn(run_with(
            Arc::new(ctx),
            node,
            o,
            stop.clone(),
            Duration::ZERO,
        ));
        until("the key listing's read", || DESCRIBES.load(SeqCst) > before).await;
        // A new dirty row while that read hangs (30 s, past `until`'s 20 s).
        let mut txn = hot.rw().unwrap();
        balances::mark_dirty(&mut txn, ids[1], coin, 0, 10).unwrap();
        txn.commit().unwrap();
        let t0 = Instant::now();
        until("the dirty rows reconciled", || {
            assert!(!task.is_finished(), "run ended");
            balances::dirty_count(&hot.ro().unwrap()).unwrap() == 0
        })
        .await;
        // Inline, the hung read (3 client tries of the 5 s timeout) would hold the loop 15 s.
        assert!(t0.elapsed() < Duration::from_secs(5), "{:?}", t0.elapsed());
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the run ends at once on stop")
            .unwrap()
            .unwrap();
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

    /// Batched and multi-node `details` reads (`reconcile_batch_with`, `read_pool`).
    mod pooled {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use super::*;

        /// A mock node. `/local` answers by account: `ghost…` has no row (Pact's absent
        /// failure; in a `try` batch that entry is the marker), `fails` is a Pact failure (a
        /// marker in a batch), `bad` is an answer with no balance, `liar` (non-coin) returns the
        /// marker shape itself, `gc` has the guard key in `gc_key`, any other account `balance`
        /// with key `k1`. Module `free.nod` has no `details` and `fail_module` is unknown to
        /// this node (every call fails, batches whole), `no_echo` answers without `account`,
        /// `free.tok` is the one module listing fungible-v2. `down`: HTTP 503 for everything.
        /// Every success reads height `height` (default 10) on block `prev`.
        pub(crate) struct Mock {
            pub prev: Mutex<String>,
            pub balance: &'static str,
            pub gc_key: Mutex<&'static str>,
            pub down: std::sync::atomic::AtomicBool,
            pub height: std::sync::atomic::AtomicU64,
            pub fail_module: Mutex<Option<&'static str>>,
            pub no_echo: Mutex<Option<&'static str>>,
            pub batch_calls: AtomicUsize,
            pub batch_accounts: AtomicUsize,
            pub single_calls: AtomicUsize,
            /// Milliseconds every answer waits.
            pub delay_ms: std::sync::atomic::AtomicU64,
        }

        impl Mock {
            fn new(prev: Hash32, balance: &'static str) -> Arc<Mock> {
                Arc::new(Mock {
                    prev: Mutex::new(prev.to_b64url()),
                    balance,
                    gc_key: Mutex::new("k1"),
                    down: Default::default(),
                    height: 10.into(),
                    fail_module: Mutex::new(None),
                    no_echo: Mutex::new(None),
                    batch_calls: AtomicUsize::new(0),
                    batch_accounts: AtomicUsize::new(0),
                    single_calls: AtomicUsize::new(0),
                    delay_ms: 0.into(),
                })
            }

            fn calls(&self) -> usize {
                self.batch_calls.load(Ordering::SeqCst) + self.single_calls.load(Ordering::SeqCst)
            }

            fn entry(&self, module: &str, acct: &str) -> Result<Value, &'static str> {
                if module == "free.nod" || *self.fail_module.lock().unwrap() == Some(module) {
                    return Err("Cannot resolve module");
                }
                if acct.starts_with("ghost") {
                    return Err("No value found in table coin_coin-table for key: ghost");
                }
                if acct == "fails" {
                    return Err("boom");
                }
                if acct == "bad" {
                    return Ok(json!({"account": acct, "guard": null}));
                }
                if acct == "liar" && module != "coin" {
                    return Ok(json!({"account": acct, "absent": true}));
                }
                let key = if acct == "gc" {
                    *self.gc_key.lock().unwrap()
                } else {
                    "k1"
                };
                let bal = if module == "coin" {
                    self.balance
                } else {
                    "7.25"
                };
                let mut v = json!({"account": acct, "balance": bal.parse::<f64>().unwrap(),
                          "guard": {"keys": [key], "pred": "keys-all"}});
                if *self.no_echo.lock().unwrap() == Some(module) {
                    v.as_object_mut().unwrap().remove("account");
                }
                Ok(v)
            }

            fn answer(&self, cmd: &Value) -> (u16, Value) {
                std::thread::sleep(Duration::from_millis(self.delay_ms.load(Ordering::SeqCst)));
                if self.down.load(Ordering::SeqCst) {
                    return (503, json!({}));
                }
                let meta = json!({"blockHeight": self.height.load(Ordering::SeqCst) + 1,
                                  "prevBlockHash": *self.prev.lock().unwrap()});
                let ok = |data: Value| {
                    (
                        200,
                        json!({"result": {"status": "success", "data": data}, "metaData": meta}),
                    )
                };
                let fail = |m: &str| {
                    (
                        200,
                        json!({"result": {"status": "failure", "error": {"message": m}}}),
                    )
                };
                let data = cmd
                    .pointer("/payload/exec/data")
                    .cloned()
                    .unwrap_or(Value::Null);
                let code = cmd
                    .pointer("/payload/exec/code")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if let Some(m) = data.get("m").and_then(Value::as_str) {
                    return ok(json!(m == "free.tok"));
                }
                if let Some(list) = data.get("as").and_then(Value::as_array) {
                    self.batch_calls.fetch_add(1, Ordering::SeqCst);
                    self.batch_accounts.fetch_add(list.len(), Ordering::SeqCst);
                    // `(map (lambda (a) (try {…} (M.details a))) …)`
                    let module = code
                        .split("} (")
                        .nth(1)
                        .and_then(|c| c.split(".details").next())
                        .unwrap_or("");
                    let mut out = Vec::new();
                    for a in list {
                        let a = a.as_str().unwrap_or("");
                        match self.entry(module, a) {
                            Ok(v) => out.push(v),
                            // An unknown module fails the whole call; `try` catches the rest.
                            Err(m) if m.starts_with("Cannot resolve") => return fail(m),
                            Err(_) => out.push(json!({"account": a, "absent": true})),
                        }
                    }
                    return ok(Value::Array(out));
                }
                self.single_calls.fetch_add(1, Ordering::SeqCst);
                let module = code
                    .strip_prefix('(')
                    .and_then(|c| c.split(".details").next())
                    .unwrap_or("");
                match self.entry(module, data.get("a").and_then(Value::as_str).unwrap_or("")) {
                    Ok(v) => ok(v),
                    Err(m) => fail(m),
                }
            }
        }

        /// Serve `mock` on a local port; its base URL.
        fn serve(mock: Arc<Mock>) -> String {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            std::thread::spawn(move || {
                for s in l.incoming() {
                    let Ok(mut s) = s else { return };
                    let mock = mock.clone();
                    std::thread::spawn(move || {
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
                        let req: Value =
                            serde_json::from_slice(&buf[head_end..]).unwrap_or(Value::Null);
                        let cmd: Value = req
                            .get("cmd")
                            .and_then(Value::as_str)
                            .and_then(|c| serde_json::from_str(c).ok())
                            .unwrap_or(Value::Null);
                        let (status, body) = mock.answer(&cmd);
                        let body = body.to_string();
                        let _ = write!(
                            s,
                            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                    });
                }
            });
            format!("http://{addr}")
        }

        fn local_at(url: &str) -> Arc<LocalClient> {
            let node = NodeClient::new(&NodeClientConfig {
                base: url.parse().unwrap(),
                network: "mainnet01".into(),
                timeout: Duration::from_secs(5),
                max_rps: 0,
                concurrency: 8,
                p2p_url: None,
                p2p_cert_sha256: None,
                max_body_bytes: 1 << 20,
                max_sse_event_bytes: 1 << 20,
            })
            .unwrap();
            Arc::new(LocalClient::new(
                node,
                LocalOpts {
                    rps: 0,
                    concurrency: 8,
                },
            ))
        }

        /// A URL nothing listens on (connection refused).
        fn dead_url() -> String {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let a = l.local_addr().unwrap();
            drop(l);
            format!("http://{a}")
        }

        fn pool_of(nodes: &[(&Arc<LocalClient>, bool)], batch_keys: usize) -> ReadPool {
            ReadPool::new(
                nodes
                    .iter()
                    .enumerate()
                    .map(|(i, (c, p))| ReadNode::new(format!("test{i}"), (*c).clone(), *p))
                    .collect(),
                batch_keys,
            )
        }

        /// A store with `coin` rows of `coin_accts`, `free.tok` and `free.nod` rows of
        /// `tok_accts`, all dirty on chain 0 at height 10 (our header there is `tip_hash`).
        fn store(
            dir: &std::path::Path,
            coin_accts: &[&str],
            tok_accts: &[&str],
        ) -> (Ctx, Arc<HotStore>) {
            let hot = Arc::new(HotStore::open(&dir.join("hot"), Geometry::default()).unwrap());
            let mut txn = hot.rw().unwrap();
            let mut dict = HotDict::load(&txn).unwrap();
            let coin = dict.intern(DictKind::Module, "coin");
            let tok = dict.intern(DictKind::Module, "free.tok");
            let nod = dict.intern(DictKind::Module, "free.nod");
            let mut marks = Vec::new();
            for a in coin_accts {
                marks.push((dict.intern(DictKind::Account, a), coin));
            }
            for a in tok_accts {
                let id = dict.intern(DictKind::Account, a);
                marks.push((id, tok));
                marks.push((id, nod));
            }
            dict.persist(&mut txn).unwrap();
            for (a, m) in marks {
                balances::mark_dirty(&mut txn, a, m, 0, 10).unwrap();
            }
            txn.commit().unwrap();
            put_header(&hot, 0, 10, tip_hash());
            let (ctx, _) = client_on(dir, &hot, Arc::new(Mutex::new(tip_hash().to_b64url())));
            (ctx, hot)
        }

        fn mark_again(hot: &HotStore, account: &str, module: &str) {
            let mut txn = hot.rw().unwrap();
            let mut d = HotDict::load(&txn).unwrap();
            // Known names: interning finds their ids.
            let (a, m) = (
                d.intern(DictKind::Account, account),
                d.intern(DictKind::Module, module),
            );
            balances::mark_dirty(&mut txn, a, m, 0, 10).unwrap();
            txn.commit().unwrap();
        }

        /// Every row (with `reconciled_at` zeroed), the dirty keys and the key index.
        type Dump = (Vec<(Vec<u8>, Vec<u8>)>, Vec<Vec<u8>>, Vec<Vec<u8>>);

        fn dump(hot: &HotStore) -> Dump {
            let txn = hot.ro().unwrap();
            let all = |t| txn.scan_chunk(t, None, 100_000, |_| true).unwrap();
            let rows = all(Table::Balances)
                .into_iter()
                .map(|(k, v)| {
                    let mut r = BalanceRow::decode(&v).unwrap();
                    r.reconciled_at = 0;
                    (k, r.encode())
                })
                .collect();
            let keys = |t| all(t).into_iter().map(|(k, _)| k).collect();
            (rows, keys(Table::BalanceDirty), keys(Table::KeyAccounts))
        }

        async fn drain(
            ctx: &Ctx,
            hot: &HotStore,
            local: &LocalClient,
            pool: Option<&ReadPool>,
        ) -> BatchStats {
            let keys = refill(hot, 4096, 64).unwrap();
            reconcile_batch_with(ctx, local, pool, &keys, &Caps::default())
                .await
                .unwrap()
        }

        const COIN: &[&str] = &[
            "alice", "bob", "carol", "ghost1", "gc", "fails", "bad", "dave", "ghost2",
        ];
        const TOK: &[&str] = &["alice", "bob", "erin"];

        /// Batched reads write exactly the rows, marks and key index single reads write, for
        /// present, absent, guard-changed, non-coin, failing and unusable keys, in fewer calls.
        #[tokio::test]
        async fn batched_reads_match_single_reads() {
            let mut out = Vec::new();
            for batch_keys in [None, Some(50)] {
                let dir = tempfile::tempdir().unwrap();
                let (ctx, hot) = store(dir.path(), COIN, TOK);
                let mock = Mock::new(tip_hash(), "1.5");
                let local = local_at(&serve(mock.clone()));
                let pool = batch_keys.map(|b| pool_of(&[(&local, true)], b));
                let st1 = drain(&ctx, &hot, &local, pool.as_ref()).await;
                // The guard of `gc` changes; its row is read again.
                *mock.gc_key.lock().unwrap() = "k2";
                mark_again(&hot, "gc", "coin");
                let st2 = drain(&ctx, &hot, &local, pool.as_ref()).await;
                let counts = |s: &BatchStats| {
                    (
                        s.ok,
                        s.absent,
                        s.failures,
                        s.invalid,
                        s.node_errors,
                        s.other_branch,
                    )
                };
                out.push((
                    dump(&hot),
                    counts(&st1),
                    counts(&st2),
                    mock.calls(),
                    mock.batch_calls.load(Ordering::SeqCst),
                ));
            }
            let (single, batched) = (&out[0], &out[1]);
            assert_eq!(single.0, batched.0, "same rows, marks and key index");
            assert_eq!(
                (single.1, single.2),
                (batched.1, batched.2),
                "same outcomes"
            );
            // 6 present coin rows + 2 ghosts absent, `fails` failed, `bad` invalid; free.tok 3
            // present; free.nod 3 failures.
            assert_eq!(single.1, (8, 2, 4, 1, 0, 0), "{:?}", single.1);
            let rows = &single.0 .0;
            assert!(rows
                .iter()
                .any(|(_, v)| BalanceRow::decode(v).is_some_and(|r| r.balance == "7.25")));
            assert_eq!(single.4, 0, "the single path never batches");
            // This mix is the worst case (a failing key in every group reads it one by one);
            // the savings are in `reads_spread_over_nodes_and_fail_over` (40 keys, 10 calls).
            assert!(batched.4 > 0, "the batched path batches");
        }

        /// A failing key or an absent key is a `try` marker: the rest of the batch is taken and
        /// only that key is read again, alone, on the primary, which decides it as before.
        #[tokio::test]
        async fn a_failed_batch_falls_back_to_single_reads() {
            let names: Vec<String> = (0..10).map(|i| format!("acct{i}")).collect();
            let mut accts: Vec<&str> = names.iter().map(String::as_str).collect();
            accts.push("fails");
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &accts, &[]);
            let mock = Mock::new(tip_hash(), "1.5");
            let local = local_at(&serve(mock.clone()));
            let pool = pool_of(&[(&local, true)], 50);
            let st = drain(&ctx, &hot, &local, Some(&pool)).await;
            assert_eq!((st.ok, st.failures), (10, 1), "{st:?}");
            assert_eq!(mock.batch_calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                mock.single_calls.load(Ordering::SeqCst),
                1,
                "only the failing key read alone"
            );
            assert_eq!(hot.ro().unwrap().count(Table::BalanceDirty).unwrap(), 0);

            // 32 keys with one absent: one batch call and one single read, no split.
            let names: Vec<String> = (0..31).map(|i| format!("acct{i}")).collect();
            let mut accts: Vec<&str> = names.iter().map(String::as_str).collect();
            accts.push("ghost9");
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &accts, &[]);
            let mock = Mock::new(tip_hash(), "1.5");
            let local = local_at(&serve(mock.clone()));
            let pool = pool_of(&[(&local, true)], 50);
            let st = drain(&ctx, &hot, &local, Some(&pool)).await;
            assert_eq!((st.ok, st.absent, st.failures), (31, 1, 0), "{st:?}");
            assert_eq!(
                (
                    mock.batch_calls.load(Ordering::SeqCst),
                    mock.single_calls.load(Ordering::SeqCst)
                ),
                (1, 1),
                "the absent key alone"
            );
        }

        fn set_tail(hot: &HotStore, height: u32) {
            let mut txn = hot.rw().unwrap();
            kdx_hot::set_checkpoint(&mut txn, kdx_hot::Stage::Tail, 0, height, tip_hash()).unwrap();
            txn.commit().unwrap();
        }

        fn rows_of(hot: &HotStore) -> Vec<BalanceRow> {
            let txn = hot.ro().unwrap();
            txn.scan_chunk(Table::Balances, None, 1000, |_| true)
                .unwrap()
                .into_iter()
                .map(|(_, v)| BalanceRow::decode(&v).unwrap())
                .collect()
        }

        fn dirty(hot: &HotStore) -> usize {
            hot.ro().unwrap().count(Table::BalanceDirty).unwrap()
        }

        /// Absent markers in a batch from a secondary: the present keys are taken from the
        /// batch, each marked key is read alone on the primary, and a key absent there is
        /// written absent exactly as the single path writes it. A non-coin module that returns
        /// the marker shape itself is read alone and never written absent.
        #[tokio::test]
        async fn absent_markers_are_read_again_on_the_primary() {
            let accts = ["acct1", "ghost1", "acct2", "ghost2"];
            let mut out = Vec::new();
            for batched in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let (ctx, hot) = store(dir.path(), &accts, &[]);
                // Absent rows are read at our tail (Absent has no height): it covers the marks.
                set_tail(&hot, 10);
                let (p, sec) = (Mock::new(tip_hash(), "1.5"), Mock::new(tip_hash(), "1.5"));
                let (pc, sc) = (local_at(&serve(p.clone())), local_at(&serve(sec.clone())));
                let pool = batched.then(|| pool_of(&[(&sc, false)], 25));
                let st = drain(&ctx, &hot, &pc, pool.as_ref()).await;
                assert_eq!((st.ok, st.absent), (2, 2), "{st:?}");
                if batched {
                    assert_eq!(sec.batch_calls.load(Ordering::SeqCst), 1);
                    assert_eq!(sec.single_calls.load(Ordering::SeqCst), 0);
                    assert_eq!(
                        p.single_calls.load(Ordering::SeqCst),
                        2,
                        "each marker read alone on the primary"
                    );
                }
                assert_eq!(dirty(&hot), 0);
                out.push(dump(&hot));
            }
            assert_eq!(out[0], out[1], "absent rows as the single path writes them");

            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &[], &["alice", "liar"]);
            let (p, sec) = (Mock::new(tip_hash(), "1.5"), Mock::new(tip_hash(), "1.5"));
            let (pc, sc) = (local_at(&serve(p.clone())), local_at(&serve(sec.clone())));
            let st = drain(&ctx, &hot, &pc, Some(&pool_of(&[(&sc, false)], 25))).await;
            // free.tok: alice found, liar invalid (no balance) on the primary; free.nod fails.
            assert_eq!((st.ok, st.absent, st.invalid), (1, 0, 1), "{st:?}");
            assert!(rows_of(&hot).iter().all(|r| !r.absent), "never absent");
        }

        /// I2: a Pact failure only on a secondary (it does not know the module yet) neither
        /// drops a mark nor counts toward the not-fungible verdict: the key is read on the
        /// primary and written.
        #[tokio::test]
        async fn a_failure_only_on_a_secondary_is_read_on_the_primary() {
            for batch_keys in [1, 25] {
                let dir = tempfile::tempdir().unwrap();
                let (ctx, hot) = store(dir.path(), &[], &["alice", "bob"]);
                let (p, sec) = (Mock::new(tip_hash(), "1.5"), Mock::new(tip_hash(), "1.5"));
                *sec.fail_module.lock().unwrap() = Some("free.tok");
                let (pc, sc) = (local_at(&serve(p.clone())), local_at(&serve(sec.clone())));
                let pool = pool_of(&[(&sc, false)], batch_keys);
                let st = drain(&ctx, &hot, &pc, Some(&pool)).await;
                // free.tok: both found via the primary; free.nod (no `details`) fails on both.
                assert_eq!((st.ok, st.failures), (2, 2), "{batch_keys}: {st:?}");
                assert_eq!(
                    st.failed_modules.len(),
                    1,
                    "only free.nod counts as failing"
                );
                assert!(!st.failed_modules.contains(&module_id(&hot, "free.tok")));
                assert_eq!(
                    rows_of(&hot).iter().filter(|r| r.balance == "7.25").count(),
                    2,
                    "free.tok rows written"
                );
                assert_eq!(dirty(&hot), 0, "marks cleared by the primary's answers");
            }
        }

        fn module_id(hot: &HotStore, name: &str) -> u32 {
            let mut txn = hot.rw().unwrap();
            let mut d = HotDict::load(&txn).unwrap();
            let id = d.intern(DictKind::Module, name);
            d.persist(&mut txn).unwrap();
            txn.commit().unwrap();
            id
        }

        /// I3: an answer from a lagging node never moves a row back: the row read at 20 stays,
        /// and the key is marked again so a later read decides; an answer at or above it
        /// replaces it.
        #[tokio::test]
        async fn a_lagging_answer_never_moves_a_row_back() {
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &["alice"], &[]);
            set_tail(&hot, 20);
            for h in [15, 20, 21] {
                put_header(&hot, 0, h, tip_hash());
            }
            let p = Mock::new(tip_hash(), "1.5");
            p.height.store(20, Ordering::SeqCst);
            let lag = Mock::new(tip_hash(), "0.5");
            lag.height.store(15, Ordering::SeqCst);
            let (pc, lc) = (local_at(&serve(p.clone())), local_at(&serve(lag.clone())));
            let st = drain(&ctx, &hot, &pc, Some(&pool_of(&[(&pc, true)], 25))).await;
            assert_eq!(st.ok, 1);
            let r = &rows_of(&hot)[0];
            assert_eq!((r.balance.as_str(), r.as_of), ("1.5", 20));

            // The lagging node (5 blocks behind: within `LAG_BLOCKS`, so it is used).
            mark_again(&hot, "alice", "coin");
            let st = drain(&ctx, &hot, &pc, Some(&pool_of(&[(&lc, false)], 25))).await;
            assert_eq!((st.ok, st.other_branch), (1, 0), "{st:?}");
            let r = &rows_of(&hot)[0];
            assert_eq!(
                (r.balance.as_str(), r.as_of, r.unverified),
                ("1.5", 20, false),
                "the newer row stays"
            );
            assert_eq!(dirty(&hot), 1, "a later read decides");
            assert_eq!(lag.calls(), 1);

            // The primary moves on to 21: its answer replaces the row and clears the mark.
            p.height.store(21, Ordering::SeqCst);
            *p.prev.lock().unwrap() = tip_hash().to_b64url();
            let st = drain(&ctx, &hot, &pc, Some(&pool_of(&[(&pc, true)], 25))).await;
            assert_eq!(st.ok, 1);
            let r = &rows_of(&hot)[0];
            assert_eq!((r.balance.as_str(), r.as_of), ("1.5", 21));
            assert_eq!(dirty(&hot), 0);
        }

        /// A node more than `LAG_BLOCKS` above our tail is skipped for a while: its answer
        /// (unchecked, no header of ours there) is kept unverified and the keys read again,
        /// and the next reads go to the other node.
        #[tokio::test]
        async fn a_node_far_ahead_is_skipped() {
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &["alice", "bob"], &[]);
            let (p, fast) = (Mock::new(tip_hash(), "1.5"), Mock::new(tip_hash(), "9.0"));
            fast.height.store(30, Ordering::SeqCst);
            let (pc, fc) = (local_at(&serve(p.clone())), local_at(&serve(fast.clone())));
            let pool = pool_of(&[(&fc, false), (&pc, true)], 25);
            let st = drain(&ctx, &hot, &pc, Some(&pool)).await;
            assert_eq!((st.ok, st.other_branch), (2, 2), "{st:?}");
            assert_eq!(dirty(&hot), 2, "read again");
            for _ in 0..3 {
                let st = drain(&ctx, &hot, &pc, Some(&pool)).await;
                assert_eq!((st.ok, st.other_branch), (2, 0), "{st:?}");
                mark_again(&hot, "alice", "coin");
                mark_again(&hot, "bob", "coin");
            }
            assert_eq!(fast.calls(), 1, "skipped while down");
            assert!(rows_of(&hot)
                .iter()
                .all(|r| r.balance == "1.5" && !r.unverified));
        }

        /// A module whose `details` does not echo `account` cannot be matched by name: every
        /// key of it is read alone.
        #[tokio::test]
        async fn a_module_that_does_not_echo_the_account_is_read_key_by_key() {
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &[], &["alice", "bob", "erin"]);
            let p = Mock::new(tip_hash(), "1.5");
            *p.no_echo.lock().unwrap() = Some("free.tok");
            let pc = local_at(&serve(p.clone()));
            let st = drain(&ctx, &hot, &pc, Some(&pool_of(&[(&pc, true)], 25))).await;
            assert_eq!(st.ok, 3, "{st:?}");
            assert_eq!(
                rows_of(&hot).iter().filter(|r| r.balance == "7.25").count(),
                3
            );
            // One batch for free.tok, one for free.nod (fails whole), then 3 + 3 single reads.
            assert_eq!(p.batch_calls.load(Ordering::SeqCst), 2);
            assert_eq!(p.single_calls.load(Ordering::SeqCst), 6);
        }

        /// A key listed twice in one batch (a tail and a seed mark) is asked once; both items
        /// get its outcome.
        #[tokio::test]
        async fn duplicate_keys_are_asked_once() {
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &["alice", "bob", "carol"], &[]);
            let p = Mock::new(tip_hash(), "1.5");
            let pc = local_at(&serve(p.clone()));
            let pool = pool_of(&[(&pc, true)], 25);
            let keys = refill(&hot, 4096, 64).unwrap();
            let twice: Vec<Dirty> = keys.iter().chain(&keys).copied().collect();
            let st = reconcile_batch_with(&ctx, &pc, Some(&pool), &twice, &Caps::default())
                .await
                .unwrap();
            assert_eq!(st.ok, 6, "{st:?}");
            assert_eq!(p.batch_calls.load(Ordering::SeqCst), 1);
            assert_eq!(p.batch_accounts.load(Ordering::SeqCst), 3, "each key once");
            assert_eq!(dirty(&hot), 0);
        }

        /// Batched calls go to the read nodes other than the primary (its budget is for the
        /// reads only it decides); a node that fails is skipped and its reads go to another
        /// node, with no node error left for the batch.
        #[tokio::test]
        async fn reads_spread_over_nodes_and_fail_over() {
            let names: Vec<String> = (0..40).map(|i| format!("acct{i}")).collect();
            let accts: Vec<&str> = names.iter().map(String::as_str).collect();
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &accts, &[]);
            let (ma, mb) = (Mock::new(tip_hash(), "1.5"), Mock::new(tip_hash(), "1.5"));
            let (a, b) = (local_at(&serve(ma.clone())), local_at(&serve(mb.clone())));
            let pool = pool_of(&[(&a, true), (&b, false)], 4);
            // Keys outside the tip share (the tip share goes through `pick`, PR #70 I1).
            let keys: Vec<Dirty> = refill(&hot, 4096, 64)
                .unwrap()
                .into_iter()
                .map(|d| Dirty { tip: false, ..d })
                .collect();
            let st = reconcile_batch_with(&ctx, &a, Some(&pool), &keys, &Caps::default())
                .await
                .unwrap();
            assert_eq!(st.ok, 40, "{st:?}");
            let (ca, cb) = (
                ma.batch_calls.load(Ordering::SeqCst),
                mb.batch_calls.load(Ordering::SeqCst),
            );
            assert_eq!((ca, cb), (0, 10), "batched reads spare the primary");
            // The tip share is spread over every node, the primary too.
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &accts, &[]);
            let (ma, mb) = (Mock::new(tip_hash(), "1.5"), Mock::new(tip_hash(), "1.5"));
            let (a, b) = (local_at(&serve(ma.clone())), local_at(&serve(mb.clone())));
            let pool = pool_of(&[(&a, true), (&b, false)], 4);
            let keys = refill(&hot, 4096, 64).unwrap();
            assert!(keys.iter().all(|d| d.tip), "40 keys fit the tip share");
            let st = reconcile_batch_with(&ctx, &a, Some(&pool), &keys, &Caps::default())
                .await
                .unwrap();
            assert_eq!(st.ok, 40, "{st:?}");
            assert!(
                ma.batch_calls.load(Ordering::SeqCst) > 0
                    && mb.batch_calls.load(Ordering::SeqCst) > 0,
                "tip share over both nodes"
            );

            // The first node is down: everything is read on the second.
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &accts, &[]);
            let mb = Mock::new(tip_hash(), "1.5");
            let b = local_at(&serve(mb.clone()));
            let dead = local_at(&dead_url());
            let pool = pool_of(&[(&dead, false), (&b, false)], 4);
            let st = drain(&ctx, &hot, &b, Some(&pool)).await;
            assert_eq!((st.ok, st.node_errors), (40, 0), "{st:?}");
            assert_eq!(mb.batch_calls.load(Ordering::SeqCst), 10);
            assert_eq!(hot.ro().unwrap().count(Table::BalanceDirty).unwrap(), 0);

            // A down node among others: the key reconciles, the stats show no node error.
            mb.down.store(true, Ordering::SeqCst);
            mark_again(&hot, "acct1", "coin");
            let mc = Mock::new(tip_hash(), "1.5");
            let c = local_at(&serve(mc.clone()));
            let pool = pool_of(&[(&b, false), (&c, true)], 4);
            let st = drain(&ctx, &hot, &c, Some(&pool)).await;
            assert_eq!((st.ok, st.node_errors), (1, 0), "{st:?}");
            assert_eq!(mc.calls(), 1);

            // Every node down: node errors, the key stays dirty.
            mc.down.store(true, Ordering::SeqCst);
            mark_again(&hot, "acct2", "coin");
            let st = drain(&ctx, &hot, &c, Some(&pool)).await;
            assert_eq!((st.ok, st.node_errors), (0, 1), "{st:?}");
            assert_eq!(hot.ro().unwrap().count(Table::BalanceDirty).unwrap(), 1);
        }

        /// An answer from a node on another branch is not stored over our row: the row is only
        /// flagged unverified and the key stays dirty; a later answer on our branch clears it.
        /// An `Absent` answer from a non-primary node is asked again of the primary.
        #[tokio::test]
        async fn an_off_branch_node_answer_is_retried_not_stored() {
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &["alice"], &[]);
            let good = Mock::new(tip_hash(), "1.5");
            let fork = Mock::new(Hash32([0x22; 32]), "99.0");
            let (g, f) = (
                local_at(&serve(good.clone())),
                local_at(&serve(fork.clone())),
            );
            let st = drain(&ctx, &hot, &g, Some(&pool_of(&[(&g, true)], 25))).await;
            assert_eq!(st.ok, 1);
            let key = {
                let txn = hot.ro().unwrap();
                txn.scan_chunk(Table::Balances, None, 10, |_| true).unwrap()[0]
                    .0
                    .clone()
            };
            let row = |hot: &HotStore| {
                BalanceRow::decode(
                    &hot.ro()
                        .unwrap()
                        .get(Table::Balances, &key)
                        .unwrap()
                        .unwrap(),
                )
                .unwrap()
            };
            assert_eq!(
                (row(&hot).balance.as_str(), row(&hot).unverified),
                ("1.5", false)
            );

            mark_again(&hot, "alice", "coin");
            let st = drain(&ctx, &hot, &g, Some(&pool_of(&[(&f, false)], 25))).await;
            assert_eq!((st.ok, st.other_branch), (1, 1), "{st:?}");
            assert_eq!(row(&hot).balance, "1.5", "the fork's value is not stored");
            assert!(row(&hot).unverified);
            assert_eq!(
                hot.ro().unwrap().count(Table::BalanceDirty).unwrap(),
                1,
                "retried"
            );

            let st = drain(&ctx, &hot, &g, Some(&pool_of(&[(&g, true)], 25))).await;
            assert_eq!((st.ok, st.other_branch), (1, 0), "{st:?}");
            assert!(!row(&hot).unverified);
            assert_eq!(hot.ro().unwrap().count(Table::BalanceDirty).unwrap(), 0);

            // Absent on a secondary node: the primary is asked too.
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &["ghost1"], &[]);
            let (p, s) = (Mock::new(tip_hash(), "1.5"), Mock::new(tip_hash(), "1.5"));
            let (pc, sc) = (local_at(&serve(p.clone())), local_at(&serve(s.clone())));
            let st = drain(&ctx, &hot, &pc, Some(&pool_of(&[(&sc, false)], 25))).await;
            assert_eq!(st.absent, 1, "{st:?}");
            assert_eq!(
                (
                    s.single_calls.load(Ordering::SeqCst),
                    p.single_calls.load(Ordering::SeqCst)
                ),
                (1, 1)
            );
        }
        /// `count` coin accounts `acct0…`; with `ghosts`, every fourth one absent (`ghost…`:
        /// here the tail checkpoint is unset, so an absent answer's `as_of` is 0 and its mark
        /// stays).
        fn accounts(count: usize, ghosts: bool) -> Vec<String> {
            (0..count)
                .map(|i| {
                    if ghosts && i % 4 == 3 {
                        format!("ghost{i}")
                    } else {
                        format!("acct{i}")
                    }
                })
                .collect()
        }

        /// The loop's options for these tests: `batch` keys a batch, windows of `k` batches.
        fn loop_opts(batch: usize, k: usize) -> ReconcileOpts {
            ReconcileOpts {
                local: LocalOpts {
                    rps: 0,
                    concurrency: 8,
                },
                batch,
                seed: false,
                caps: Caps {
                    refill_window_batches: k,
                    ..Caps::default()
                },
                keylist_skip: Vec::new(),
                batch_keys: 4,
                read_nodes: ReadNodes::default(),
            }
        }

        /// PR #70 review I1: a secondary 3 blocks behind (within `LAG_BLOCKS`, so still up)
        /// does not hold fresh marks back. Every key of one window is cleared within that
        /// window: the tip share is read through `pick`, and keys a batched answer from the
        /// secondary could not clear are read again off that node.
        #[tokio::test]
        async fn a_secondary_a_few_blocks_behind_does_not_hold_fresh_marks() {
            let names = accounts(24, false);
            let names: Vec<&str> = names.iter().map(String::as_str).collect();
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &names, &[]);
            put_header(&hot, 0, 7, tip_hash());
            let (p, sec) = (Mock::new(tip_hash(), "1.5"), Mock::new(tip_hash(), "1.5"));
            sec.height.store(7, Ordering::SeqCst);
            let (pc, sc) = (local_at(&serve(p.clone())), local_at(&serve(sec.clone())));
            let pool = pool_of(&[(&pc, true), (&sc, false)], 4);
            let keys = refill_window(&hot, 8, 64, 2, &HashMap::new()).unwrap();
            assert_eq!(keys.len(), 16);
            assert!(keys.iter().any(|d| d.tip) && keys.iter().any(|d| !d.tip));
            let mut w = RefillWindow::default();
            w.set(keys.clone(), Instant::now());
            loop {
                let batch = w.next_batch(8);
                if batch.is_empty() {
                    break;
                }
                reconcile_batch_with(&ctx, &pc, Some(&pool), &batch, &Caps::default())
                    .await
                    .unwrap();
            }
            assert!(
                sec.batch_calls.load(Ordering::SeqCst) > 0,
                "the secondary took batched reads"
            );
            for d in &keys {
                assert_eq!(super::mark_of(&hot, &d.key), None, "{d:?} still dirty");
            }
            assert_eq!(
                dirty(&hot),
                24 - 16,
                "only keys outside the window are left"
            );
        }

        /// PR #70 review M6: the real `reconcile_loop`, with windows of 4 batches and a
        /// secondary, drains a table to exactly the rows single reads on one node write.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn the_loop_drains_a_table_with_a_refill_window() {
            let names = accounts(40, true);
            let names: Vec<&str> = names.iter().map(String::as_str).collect();
            // Reference: single reads on one node.
            let rd = tempfile::tempdir().unwrap();
            let (rctx, rhot) = store(rd.path(), &names, &[]);
            let rl = local_at(&serve(Mock::new(tip_hash(), "1.5")));
            for _ in 0..10 {
                drain(&rctx, &rhot, &rl, None).await;
            }
            assert_eq!(dirty(&rhot), 10, "only the absent keys' marks stay");
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &names, &[]);
            let (p, sec) = (Mock::new(tip_hash(), "1.5"), Mock::new(tip_hash(), "1.5"));
            let (pc, sc) = (local_at(&serve(p.clone())), local_at(&serve(sec.clone())));
            let pool = pool_of(&[(&pc, true), (&sc, false)], 4);
            let (ctx, stop) = (Arc::new(ctx), CancellationToken::new());
            let o = loop_opts(8, 4);
            let run = reconcile_loop(&ctx, &pc, &pool, &o, &stop, Duration::from_secs(60), None);
            let want = dump(&rhot);
            let watch = async {
                until("the loop writes the rows single reads write", || {
                    dump(&hot) == want
                })
                .await;
                stop.cancel();
            };
            let (r, ()) = tokio::join!(run, watch);
            r.unwrap();
            assert!(
                sec.batch_calls.load(Ordering::SeqCst) > 0,
                "the secondary read"
            );
            assert!(
                p.single_calls.load(Ordering::SeqCst) > 0,
                "absent asked of the primary"
            );
            assert_eq!(dump(&hot), want);
        }

        /// PR #70 review M5: a stop while a batch reads and the next window's walk is spawned
        /// ends the loop with no walk left behind (nothing else holds the store).
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_stop_during_a_prefetch_leaves_no_walk_behind() {
            let names = accounts(40, true);
            let names: Vec<&str> = names.iter().map(String::as_str).collect();
            let dir = tempfile::tempdir().unwrap();
            let (ctx, hot) = store(dir.path(), &names, &[]);
            let (p, sec) = (Mock::new(tip_hash(), "1.5"), Mock::new(tip_hash(), "1.5"));
            sec.delay_ms.store(300, Ordering::SeqCst);
            p.delay_ms.store(300, Ordering::SeqCst);
            let (pc, sc) = (local_at(&serve(p.clone())), local_at(&serve(sec.clone())));
            let pool = pool_of(&[(&pc, true), (&sc, false)], 4);
            let (ctx, stop) = (Arc::new(ctx), CancellationToken::new());
            let held = Arc::strong_count(&hot);
            // Windows of 2 batches: the second batch of each window spawns the next walk.
            let o = loop_opts(8, 2);
            let run = reconcile_loop(&ctx, &pc, &pool, &o, &stop, Duration::from_secs(60), None);
            let watch = async {
                // The first batch is in; the second (with the walk spawned) is reading.
                until("the first batch", || dirty(&hot) <= 32).await;
                stop.cancel();
            };
            let t = Instant::now();
            let (r, ()) = tokio::join!(run, watch);
            r.unwrap();
            assert!(t.elapsed() < Duration::from_secs(15), "ended promptly");
            assert_eq!(
                Arc::strong_count(&hot),
                held,
                "no walk holds the store after the loop"
            );
            assert!(dirty(&hot) > 0, "stopped before the table drained");
        }
    }
}

#[cfg(test)]
mod miner_rows {
    use super::tests::*;
    use super::*;
    use kdx_hot::{Geometry, HotDict, HotStore};
    use kdx_types::{Dict, DictKind, DictMut};
    use std::sync::Mutex;

    fn fixture() -> (kdx_decode::DecodedBlock, kdx_decode::VerifyReport) {
        let raw =
            zstd::decode_all(&include_bytes!("../tests/fixtures/chain0_3125000.json.zst")[..])
                .unwrap();
        let v: Vec<Value> = serde_json::from_slice(&raw).unwrap();
        crate::pipeline::prepare_value(0, 3_125_000, &v[0]["header"], &v[0]["payload"]).unwrap()
    }

    /// Tail-apply the fixture block (with its coinbase) into a fresh store whose header at
    /// height 10 is the fake node's tip. Returns the store and the miner name.
    fn store_with_coinbase(dir: &std::path::Path) -> (Arc<HotStore>, String) {
        let hot = Arc::new(HotStore::open(&dir.join("hot"), Geometry::default()).unwrap());
        let (b, rep) = fixture();
        let miner = b.miner["account"].as_str().unwrap().to_owned();
        let evs = b.coinbase["events"].as_array().unwrap();
        assert!(
            evs.iter()
                .any(|e| e["name"] == "TRANSFER" && e["params"][1] == miner.as_str()),
            "the fixture coinbase credits the miner: {evs:?}"
        );
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        dict.intern(DictKind::Module, "coin");
        let ib = kdx_decode::index_block(&b, &rep, &mut dict).unwrap();
        kdx_hot::tail::apply(&mut txn, &ib, &mut dict).unwrap();
        txn.commit().unwrap();
        put_header(&hot, 0, 10, tip_hash());
        (hot, miner)
    }

    /// The `minerAccount` resolver's lookup (`types/block.rs` + `account::chain_rows`):
    /// header's miner id -> name -> find -> `Balances` scan of `(account, coin)` on its chain.
    fn miner_row(hot: &HotStore) -> Option<BalanceRow> {
        let txn = hot.ro().unwrap();
        let dict = HotDict::load(&txn).unwrap();
        let hv = txn
            .get(Table::Headers, &block_key(0, 3_125_000))
            .unwrap()
            .unwrap();
        let br = decode_block(&hv).unwrap();
        let name = dict.name(DictKind::Account, br.miner_account)?.to_owned();
        let a = dict.find(DictKind::Account, &name)?;
        let m = dict.find(DictKind::Module, "coin")?;
        txn.get(Table::Balances, &balances::bal_key(a, m, 0))
            .unwrap()
            .as_deref()
            .and_then(BalanceRow::decode)
            .filter(|r| !r.absent)
    }

    /// A coinbase-only miner: the tail marks its coin row, one reconcile batch stores it, and
    /// the resolver's lookup finds it; its guard key reaches `KeyAccounts`.
    #[tokio::test]
    async fn a_coinbase_only_miner_gets_a_coin_row_that_miner_account_finds() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, miner) = store_with_coinbase(dir.path());
        let (a, coin) = {
            let txn = hot.ro().unwrap();
            let d = HotDict::load(&txn).unwrap();
            (
                d.find(DictKind::Account, &miner).unwrap(),
                d.find(DictKind::Module, "coin").unwrap(),
            )
        };
        let k = balances::bal_key(a, coin, 0);
        assert!(
            mark_of_pub(&hot, &k),
            "the coinbase marks the miner's coin row"
        );
        assert!(miner_row(&hot).is_none(), "no row before the reconciler");
        let (ctx, local) = client_on(
            dir.path(),
            &hot,
            Arc::new(Mutex::new(tip_hash().to_b64url())),
        );
        let keys = refill(&hot, 64, 32).unwrap();
        assert!(
            keys.iter().any(|d| d.key == k),
            "the miner key is refilled first"
        );
        let st = reconcile_batch(&ctx, &local, &keys, &Caps::default())
            .await
            .unwrap();
        assert!(st.ok >= 1, "{st:?}");
        let row = miner_row(&hot).expect("minerAccount finds the reconciled row");
        assert_eq!(row.balance, "1.5");
        let txn = hot.ro().unwrap();
        assert!(txn
            .get(Table::KeyAccounts, &key_account_key(&key_hash("k1"), a))
            .unwrap()
            .is_some());
    }

    /// A fresh store (seed done, D1 done, the first D3 pass pending): `run` reconciles the
    /// miner's tail mark while the first D3 receivers pass is pending. The test holds the
    /// shared dictionary lock, so the pass blocks at its start (`run` snapshots the dictionary
    /// before it walks anything; the loop never takes the lock here), and the order is
    /// deterministic: the miner row must appear with `balances_seed_token_receivers` still
    /// absent, and the pass must then finish once the lock is released. On the old inline
    /// order the loop never starts and the test times out. It proves the loop is not gated on
    /// the pass; no real pass mark interleaves with a loop batch here (the interleave test
    /// covers the three orders against the pass's write).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tail_marks_are_reconciled_while_the_first_d3_pass_runs() {
        let dir = tempfile::tempdir().unwrap();
        let (hot, _miner) = store_with_coinbase(dir.path());
        let segdir = dir.path().join("segments");
        std::fs::create_dir_all(&segdir).unwrap();
        let mut txn = hot.rw().unwrap();
        txn.set_meta(balances::meta::BALANCES_SEEDED, &3_125_000u32.to_be_bytes())
            .unwrap();
        txn.set_meta(
            balances::meta::BALANCES_SEED_RECEIVERS,
            &u32::MAX.to_be_bytes(),
        )
        .unwrap();
        // More unreadable hot txs than one chunk: the pass writes (and takes the dictionary
        // lock) before it ends.
        for h in 20..(20 + 2 * crate::seed_receivers::TX_CHUNK as u32) {
            let k = kdx_types::TxKey::new(h, 1, 1);
            txn.put(Table::TailTxs, &k.to_be_bytes(), b"\xffnot a tx record")
                .unwrap();
        }
        txn.commit().unwrap();
        let (ctx, _) = client_on(
            dir.path(),
            &hot,
            Arc::new(Mutex::new(tip_hash().to_b64url())),
        );
        let node = ctx.node.clone();
        let dict = ctx.dict.clone();
        let o = ReconcileOpts {
            local: LocalOpts {
                // One slot: the key listing (which takes the dictionary lock) stays off.
                rps: 0,
                concurrency: 1,
            },
            batch: 10,
            seed: true,
            caps: Caps::default(),
            keylist_skip: Vec::new(),
            batch_keys: 1,
            read_nodes: ReadNodes::default(),
        };
        let d3_done = || {
            hot.ro()
                .unwrap()
                .meta(balances::meta::BALANCES_SEED_TOKEN_RECEIVERS)
                .unwrap()
                .is_some()
        };
        // Hold the dictionary lock on a plain thread until the miner row is seen.
        let (locked_tx, locked_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let _held = dict.lock().unwrap();
            locked_tx.send(()).unwrap();
            let _ = release_rx.recv();
        });
        locked_rx.recv().unwrap();
        let stop = CancellationToken::new();
        let task = tokio::spawn(super::run(Arc::new(ctx), node, o, stop.clone()));
        let end = Instant::now() + Duration::from_secs(120);
        while miner_row(&hot).is_none() {
            assert!(!task.is_finished(), "run ended");
            assert!(Instant::now() < end, "timed out waiting for the miner row");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!d3_done(), "the D3 pass ended before the miner row");
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        while !d3_done() {
            assert!(!task.is_finished(), "run ended");
            assert!(Instant::now() < end, "timed out waiting for the D3 pass");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    fn mark_of_pub(hot: &HotStore, k: &[u8; 9]) -> bool {
        hot.ro()
            .unwrap()
            .get(Table::BalanceDirty, k)
            .unwrap()
            .is_some()
    }
}

#[cfg(test)]
#[path = "reconcile_drain_tests.rs"]
mod drain_tests;
