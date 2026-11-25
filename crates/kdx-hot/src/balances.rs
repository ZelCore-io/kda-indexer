//! Balance state in additive hot tables (rule A-1, plan Task 9, spec §3.9). All integers are
//! big-endian.
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | `Balances` | AccountId u32, ModuleId u32, chain u8 | flags u8 (b0: absent at the node, b1: unverified), balance text as the node printed it, guard hash [16], as_of u32, reconciled_at u32 |
//! | `BalanceDirty` | same | [`Mark`]: marked_height u32, seq u64 (tail marks) |
//! | `BalanceSeed` | same | [`Mark`] with height 0 (the seed backlog, B-2) |
//! | `Guards` | guard hash [16] | guard JSON bytes as the node returned them |
//! | `KeyAccounts` | blake2b-128(public key) [16], AccountId u32 | empty |
//! | `Tokens` | ModuleId u32 | [`TokenRow`]: first_height u32, holders u64, kind u8, flags u8, rows u32, fungible_chains u32, checked_chains u32, checked_at u32 |
//!
//! - The guard hash is blake2b-128 of the guard JSON. A keyset reference resolved with
//!   `describe-keyset` is cached under [`ref_hash`] of its chain and name.
//! - `as_of` is the node tip the `/local` answer read (`metaData.blockHeight − 1`: the node
//!   reports the pending block's height); `reconciled_at` is unix seconds.
//! - Dirty marks (B-3): tail apply and tail undo mark every `xfer` posting's `(account, module,
//!   chain)` and every `acct` posting's `(account, coin, chain)` in the same txn as the block.
//!   They also mark the D3 rows of each tx (receivers no TRANSFER names, ruling R-P5P9-192)
//!   through [`mark_row`], the same path the seed pass uses.
//!   A mark keeps the highest height and takes the writing txn's id as its `seq`; the
//!   reconciler deletes it only when it is still the exact mark it read and not above the
//!   answer's `as_of` (SR-1, I-5, M-3).
//! - `unverified`: the row's last re-read failed or was parked, or its answer was read on a block
//!   other than our header at its `as_of` (another branch, D2); the route serves it `stale`.
//! - `holders` counts rows with a balance above zero; it is approximate (reorgs, rows not yet
//!   reconciled).

use anyhow::{Context, Result};
use kdx_decode::Family;
use kdx_types::{blake2b_128, DictKind, TxKey};
use serde_json::Value;

use crate::dict::fwd_key;
use crate::env::{be_u32, RwTxn, Txn};
use crate::tables::Table;

/// `Meta` keys of the balance seed (B-2).
pub mod meta {
    /// u64: `details` answers whose guard was over the size cap and stored as null (F-14).
    pub const GUARDS_CAPPED: &str = "balances_guards_capped";
    /// u32: the max tail height when the seed finished.
    pub const BALANCES_SEEDED: &str = "balances_seeded";
    /// Resume cursor of a running seed (see `kdx_ingest::seed`).
    pub const BALANCES_SEED_CURSOR: &str = "balances_seed_cursor";
    /// Prefix of the per-chain mark watermark `balances_marked_through/{chain}`: u32, the
    /// height of the last block the tail applied with dirty marks (M-11).
    pub const MARKED_THROUGH: &str = "balances_marked_through";
    /// u32: the tail applied a block above a chain's watermark + 1, so blocks in between got
    /// no marks (`[balances] enabled = false`, or an older binary). The reconciler re-runs the
    /// seed and clears it.
    pub const MARK_GAP: &str = "balances_mark_gap";
    /// u32: the receivers pass (D1, `kdx_ingest::seed_receivers`) has marked the event-less
    /// receivers of every stored tx below this height. Absent: the pass never ran.
    pub const BALANCES_SEED_RECEIVERS: &str = "balances_seed_receivers";
    /// Resume cursor of a running receivers pass: start height u32 ‖ next frozen TxNum u64.
    pub const BALANCES_SEED_RECEIVERS_CURSOR: &str = "balances_seed_receivers_cursor";
    /// Txs the receivers pass could not read (u64), over the pass and its resumes (D1 L1).
    /// Kept after the pass ends so `--status` shows it; non-zero needs a look at the logs.
    pub const BALANCES_SEED_RECEIVERS_ERRORS: &str = "balances_seed_receivers_errors";
    /// u32: the token receivers pass (D3, `kdx_ingest::seed_receivers` with `Rules::D3`) has
    /// marked the receivers no TRANSFER names, of coin and every token, for every stored tx
    /// below this height. Absent: the pass never ran.
    pub const BALANCES_SEED_TOKEN_RECEIVERS: &str = "balances_seed_token_receivers";
    /// Resume cursor of a running D3 pass (same layout as the D1 cursor).
    pub const BALANCES_SEED_TOKEN_RECEIVERS_CURSOR: &str = "balances_seed_token_receivers_cursor";
    /// Txs the D3 pass could not read (u64), like [`BALANCES_SEED_RECEIVERS_ERRORS`].
    pub const BALANCES_SEED_TOKEN_RECEIVERS_ERRORS: &str = "balances_seed_token_receivers_errors";
    /// Legacy (builds before fix round 1 withheld the D3 pass on a budget hit and listed the
    /// tokens here). A D3 pass deletes it; nothing reads it (review H2, M3).
    pub const BALANCES_SEED_TOKEN_RECEIVERS_CAP_HIT: &str = "balances_seed_token_receivers_cap_hit";
    /// JSON `{"untracked", "ceiling", "tokens", "discovered", "tracked_bound"}`: capped
    /// candidates the last D3 pass dropped (shared bucket of untracked emitters, global
    /// ceiling, per tracked token at its budget of distinct rows; the pass completes
    /// regardless), discovered modules over the per-pass cap, and the bound on its
    /// tracked-token candidates (tokens × budget).
    pub const BALANCES_SEED_TOKEN_RECEIVERS_DROPPED: &str = "balances_seed_token_receivers_dropped";
    /// JSON: the per-token budget counters of a running D3 pass's frozen walk, written with
    /// its cursor so a resume does not grant a token its budget twice (review L1).
    pub const BALANCES_SEED_TOKEN_RECEIVERS_BUDGET: &str = "balances_seed_token_receivers_budget";
    /// Prefix of the D3 discovery queue `balances_d3_discover/{ns}/{chain:02}/{module}` (u32: the
    /// lowest height that named it): modules a tx put forward as tokens (cross-chain defpact,
    /// own or same-namespace `fungible-v2` modref). The reconciler checks each with
    /// `describe-module` on that chain, a few reads per round, and tracks it only when it
    /// lists `fungible-v2` (review M1). At most [`super::D3_DISCOVER_QUEUE_MAX`] entries, and
    /// [`super::D3_DISCOVER_PER_NAMESPACE`] per namespace.
    pub const D3_DISCOVER: &str = "balances_d3_discover/";
    /// u32: entries in the discovery queue.
    pub const D3_DISCOVER_LEN: &str = "balances_d3_discover_len";
    /// Prefix of `balances_d3_discover_pending/{chain:02}/{module}` (u32 height): discoveries
    /// dropped at a quota or a full queue; the next pass re-queues them first.
    pub const D3_DISCOVER_PENDING: &str = "balances_d3_discover_pending/";
    /// u32: entries in the pending list.
    pub const D3_DISCOVER_PENDING_LEN: &str = "balances_d3_discover_pending_len";
    /// The last pending key `requeue_pending` scanned: the next call starts after it and wraps
    /// around (review (a): entries the queue keeps refusing cannot freeze the rest).
    pub const D3_DISCOVER_PENDING_FROM: &str = "balances_d3_discover_pending_from";
    /// Prefix of `balances_d3_rewalk/{ModuleId:010}` (empty): tokens tracked since the last
    /// D3 pass walked history. The next pass walks the D3 rules for them from height 0 up to
    /// its done height (review M2).
    pub const D3_REWALK: &str = "balances_d3_rewalk/";
    /// Prefix of [`super::d3_undo_key`]: D3 rows a hot block marked (removed by its undo, or
    /// when the freezer moves the block out of the hot store).
    pub const D3_UNDO: &str = "balances_d3_undo/";
    /// Prefix of `balances_parked_invalid/{key hex}` (u32 unix seconds): keys the reconciler
    /// parked after [`super::PARKED_INVALID_MAX`]-bounded retries of unusable `details`
    /// answers (`LocalError::Invalid`, PR #26 review H-1). Kept for inspection
    /// (`reconcile-balances --status`); `reconcile-balances --all` puts them back in the seed
    /// backlog and clears the list.
    pub const PARKED_INVALID: &str = "balances_parked_invalid/";
    /// u32: entries in the parked-invalid list.
    pub const PARKED_INVALID_LEN: &str = "balances_parked_invalid_len";
    /// u32 unix seconds: when the last history walk for newly tracked tokens ended.
    pub const D3_REWALK_AT: &str = "balances_d3_rewalk_at";
    /// JSON: the running history walk (re-review N2): the done height it walks below, its
    /// tokens, the next slice's start and its budget counters; written with each slice's
    /// marks, deleted when it ends.
    pub const D3_REWALK_WALK: &str = "balances_d3_rewalk_walk";
    /// Prefix + 10-digit ModuleId: the token's key listing (D3 gap groups 3 and 4): layout
    /// version 2, u32 unix seconds of the last complete listing (0: never), the index of the
    /// next chain, flags, u16 failures, then per failed chain its id, failures in a row and
    /// u32 retry time (`kdx_ingest::keylist::ListState`).
    pub const KEYLIST: &str = "balances_keylist/";
    /// JSON: running totals of the key listing (shown by `reconcile-balances --status`).
    pub const KEYLIST_STATS: &str = "balances_keylist_stats";
}

/// Entries the D3 discovery queue holds at most; more are dropped and counted
/// ([`counters::D3_DISCOVER_DROPPED`]).
pub const D3_DISCOVER_QUEUE_MAX: usize = 16_384;
/// Dropped discoveries kept for a later pass at most.
pub const D3_DISCOVER_PENDING_MAX: usize = 1_000_000;
/// Entries one namespace may hold in the discovery queue (security review).
pub const D3_DISCOVER_PER_NAMESPACE: usize = 4;

/// Counters of the D3 marks outside the seed pass.
pub mod counters {
    /// D3 rows the tail dropped at its per-tx or per-block-per-emitter caps.
    pub const D3_TAIL_DROPPED: &str = "d3_tail_dropped";
    /// Discovered modules dropped at a full discovery queue.
    pub const D3_DISCOVER_DROPPED: &str = "d3_discover_dropped";
    /// Dropped discoveries not kept because the pending list was full.
    pub const D3_DISCOVER_LOST: &str = "d3_discover_lost";
    /// Discovered module names over `kdx_decode::receivers::MAX_MODULE_LEN` bytes, dropped
    /// (re-review N1: such a name can never be a Meta key).
    pub const D3_DISCOVER_TOO_LONG: &str = "d3_discover_too_long";
    /// D3 rule 2b references and rows (a tracked token named in code) the tail dropped at the
    /// per-tx caps (`kdx_decode::receivers::MAX_MODREF_PAIRS_PER_TX` rows,
    /// `kdx_decode::receivers::MAX_MODREFS_PER_TX` tracked references), each counted once.
    pub const D3_MODREF_CAPPED: &str = "d3_modref_capped";
}

/// The key a discovered module's quota counts against (security review): its namespace
/// (`ns` of `ns.module`; `-` for root modules), or in an open namespace (`free`, `user`: anyone
/// deploys there) the module itself, so junk modules there cannot starve a real one.
pub fn discover_namespace(module: &str) -> &str {
    match module.split_once('.') {
        Some((n, _)) if kdx_decode::receivers::OPEN_NAMESPACES.contains(&n) => module,
        Some((n, _)) => n,
        None => "-",
    }
}

fn discover_key(module: &str, chain: u8) -> String {
    format!(
        "{}{}/{chain:02}/{module}",
        meta::D3_DISCOVER,
        discover_namespace(module)
    )
}

/// Queue `module` (a discovered name, not tracked yet) for its `describe-module` check on
/// `chain` (review M1). Returns false when it is dropped and counted: its namespace already has
/// [`D3_DISCOVER_PER_NAMESPACE`] entries queued (security review: junk namespaces cannot crowd
/// out a real token), or the queue is full.
pub fn queue_discovered(txn: &mut RwTxn<'_>, module: &str, chain: u8, height: u32) -> Result<bool> {
    if too_long(txn, module)? {
        return Ok(false);
    }
    if try_queue(txn, module, chain, height)? {
        return Ok(true);
    }
    txn.add_counter(counters::D3_DISCOVER_DROPPED, 1)?;
    pend_discovered(txn, module, chain, height)?;
    Ok(false)
}

/// Re-review N1: a discovered name longer than `MAX_MODULE_LEN` is dropped and counted here
/// (a backstop of the cap in `receivers::discover`), never made a key.
fn too_long(txn: &mut RwTxn<'_>, module: &str) -> Result<bool> {
    if module.len() <= kdx_decode::receivers::MAX_MODULE_LEN {
        return Ok(false);
    }
    txn.add_counter(counters::D3_DISCOVER_TOO_LONG, 1)?;
    Ok(true)
}

fn pending_key(module: &str, chain: u8) -> String {
    format!("{}{chain:02}/{module}", meta::D3_DISCOVER_PENDING)
}

/// Keep a dropped discovery for the next pass, which re-queues it first (nothing is lost for
/// good). At most [`D3_DISCOVER_PENDING_MAX`] entries; beyond that it is only counted.
pub fn pend_discovered(txn: &mut RwTxn<'_>, module: &str, chain: u8, height: u32) -> Result<()> {
    if too_long(txn, module)? {
        return Ok(());
    }
    let k = pending_key(module, chain);
    if let Some(h) = txn.meta(&k)?.as_deref().and_then(be_u32) {
        if height < h {
            txn.set_meta(&k, &height.to_be_bytes())?;
        }
        return Ok(());
    }
    let len = txn
        .meta(meta::D3_DISCOVER_PENDING_LEN)?
        .as_deref()
        .and_then(be_u32)
        .unwrap_or(0);
    if len as usize >= D3_DISCOVER_PENDING_MAX {
        txn.add_counter(counters::D3_DISCOVER_LOST, 1)?;
        return Ok(());
    }
    txn.set_meta(&k, &height.to_be_bytes())?;
    txn.set_meta(meta::D3_DISCOVER_PENDING_LEN, &(len + 1).to_be_bytes())
}

/// Move up to `n` pending discoveries into the queue (those it refuses stay pending). Returns
/// how many moved. Each call scans from after the last key the previous call scanned
/// ([`meta::D3_DISCOVER_PENDING_FROM`]) and wraps around, so refused entries sorting first
/// cannot hold back the rest (review (a)).
pub fn requeue_pending(txn: &mut RwTxn<'_>, n: usize) -> Result<usize> {
    let p = meta::D3_DISCOVER_PENDING.as_bytes();
    let from = txn
        .meta(meta::D3_DISCOVER_PENDING_FROM)?
        .filter(|a| a.starts_with(p));
    let mut rows = txn.scan_chunk(Table::Meta, from.as_deref().or(Some(p)), n, |k| {
        k.starts_with(p)
    })?;
    if let Some(a) = from.as_deref().filter(|_| rows.len() < n) {
        // Wrap around: the keys up to and including the last one scanned.
        rows.extend(txn.scan_limit(Table::Meta, p, n - rows.len(), |k| {
            k.starts_with(p) && k <= a
        })?);
    }
    match rows.last() {
        Some((k, _)) => txn.set_meta(meta::D3_DISCOVER_PENDING_FROM, k)?,
        None => {
            txn.del(Table::Meta, meta::D3_DISCOVER_PENDING_FROM.as_bytes())?;
        }
    }
    let mut moved = 0;
    for (k, v) in rows {
        let Some((c, m)) = std::str::from_utf8(&k[p.len()..])
            .ok()
            .and_then(|r| r.split_once('/'))
        else {
            continue;
        };
        let (Ok(c), Some(h)) = (c.parse::<u8>(), be_u32(&v)) else {
            continue;
        };
        let m = m.to_owned();
        if try_queue(txn, &m, c, h)? {
            txn.del(Table::Meta, &k)?;
            let len = txn
                .meta(meta::D3_DISCOVER_PENDING_LEN)?
                .as_deref()
                .and_then(be_u32)
                .unwrap_or(0);
            txn.set_meta(
                meta::D3_DISCOVER_PENDING_LEN,
                &len.saturating_sub(1).to_be_bytes(),
            )?;
            moved += 1;
        }
    }
    Ok(moved)
}

/// Pending discoveries: `(module, chain)`.
pub fn discover_pending(txn: &impl Txn, n: usize) -> Result<Vec<(String, u8)>> {
    let p = meta::D3_DISCOVER_PENDING.as_bytes();
    Ok(txn
        .scan_limit(Table::Meta, p, n, |k| k.starts_with(p))?
        .into_iter()
        .filter_map(|(k, _)| {
            let (c, m) = std::str::from_utf8(k.get(p.len()..)?)
                .ok()?
                .split_once('/')?;
            Some((m.to_owned(), c.parse().ok()?))
        })
        .collect())
}

/// Queue `module` unless its quota or the queue is full.
fn try_queue(txn: &mut RwTxn<'_>, module: &str, chain: u8, height: u32) -> Result<bool> {
    if module.len() > kdx_decode::receivers::MAX_MODULE_LEN {
        return Ok(false);
    }
    let k = discover_key(module, chain);
    if let Some(h) = txn.meta(&k)?.as_deref().and_then(be_u32) {
        if height < h {
            txn.set_meta(&k, &height.to_be_bytes())?;
        }
        return Ok(true);
    }
    let ns = format!("{}{}/", meta::D3_DISCOVER, discover_namespace(module));
    let in_ns = txn
        .scan_limit(Table::Meta, ns.as_bytes(), D3_DISCOVER_PER_NAMESPACE, |k| {
            k.starts_with(ns.as_bytes())
        })?
        .len();
    let len = txn
        .meta(meta::D3_DISCOVER_LEN)?
        .as_deref()
        .and_then(be_u32)
        .unwrap_or(0);
    if in_ns >= D3_DISCOVER_PER_NAMESPACE || len as usize >= D3_DISCOVER_QUEUE_MAX {
        return Ok(false);
    }
    txn.set_meta(&k, &height.to_be_bytes())?;
    txn.set_meta(meta::D3_DISCOVER_LEN, &(len + 1).to_be_bytes())?;
    Ok(true)
}

/// Up to `n` entries of the discovery queue: `(module, chain, height)`.
pub fn discovered_queue(txn: &impl Txn, n: usize) -> Result<Vec<(String, u8, u32)>> {
    let p = meta::D3_DISCOVER.as_bytes();
    Ok(txn
        .scan_limit(Table::Meta, p, n, |k| k.starts_with(p))?
        .into_iter()
        .filter_map(|(k, v)| {
            let rest = std::str::from_utf8(k.get(p.len()..)?).ok()?;
            let mut it = rest.splitn(3, '/');
            let (_, c, m) = (it.next()?, it.next()?, it.next()?);
            Some((m.to_owned(), c.parse().ok()?, be_u32(&v)?))
        })
        .collect())
}

/// Remove one discovery queue entry.
pub fn unqueue_discovered(txn: &mut RwTxn<'_>, module: &str, chain: u8) -> Result<()> {
    if txn.del(Table::Meta, discover_key(module, chain).as_bytes())? {
        let len = txn
            .meta(meta::D3_DISCOVER_LEN)?
            .as_deref()
            .and_then(be_u32)
            .unwrap_or(0);
        txn.set_meta(meta::D3_DISCOVER_LEN, &len.saturating_sub(1).to_be_bytes())?;
    }
    Ok(())
}

/// `Meta` key of the D3 rows the tail marked for block (chain, height): account u32 ‖ module
/// u32 ‖ TxKey per row; a reorg's undo marks them again (ruling R-P5P9-192).
pub fn d3_undo_key(chain: u8, height: u32) -> String {
    format!("{}{chain:02}/{height:010}", meta::D3_UNDO)
}

/// Tokens waiting for their D3 history walk (review M2).
pub fn rewalk_pending(txn: &impl Txn) -> Result<Vec<u32>> {
    let p = meta::D3_REWALK.as_bytes();
    Ok(txn
        .scan(Table::Meta, p, |k| k.starts_with(p))?
        .into_iter()
        .filter_map(|(k, _)| std::str::from_utf8(k.get(p.len()..)?).ok()?.parse().ok())
        .collect())
}

/// Done: `module`'s history was walked.
pub fn clear_rewalk(txn: &mut RwTxn<'_>, module: u32) -> Result<()> {
    txn.del(
        Table::Meta,
        format!("{}{module:010}", meta::D3_REWALK).as_bytes(),
    )?;
    Ok(())
}

/// Queue a D3 history walk of `modules` now (`reconcile-balances --rewalk`): each joins the
/// pending set, and the daily spacing is reset (`balances_d3_rewalk_at` deleted), so the
/// service starts the walk at its next history slice instead of a day after the last walk.
/// A walk in progress is abandoned (its state deleted): its own tokens are still pending
/// until a walk ends, so the new walk covers them and `modules` together, from height 0.
/// Returns whether a walk in progress was restarted.
pub fn queue_rewalk(txn: &mut RwTxn<'_>, modules: &[u32]) -> Result<bool> {
    for m in modules {
        txn.put(
            Table::Meta,
            format!("{}{m:010}", meta::D3_REWALK).as_bytes(),
            &[],
        )?;
    }
    txn.del(Table::Meta, meta::D3_REWALK_AT.as_bytes())?;
    txn.del(Table::Meta, meta::D3_REWALK_WALK.as_bytes())
}

/// Most entries in the parked-invalid list ([`meta::PARKED_INVALID`]); keys parked beyond it
/// are only counted (`result="parked"`), like a node-error park.
pub const PARKED_INVALID_MAX: u32 = 100_000;

fn parked_invalid_key(k: &[u8; 9]) -> Vec<u8> {
    let mut out = meta::PARKED_INVALID.as_bytes().to_vec();
    for b in k {
        out.extend_from_slice(format!("{b:02x}").as_bytes());
    }
    out
}

fn parked_invalid_len(txn: &impl Txn) -> Result<u32> {
    Ok(txn
        .meta(meta::PARKED_INVALID_LEN)?
        .as_deref()
        .and_then(|v| <[u8; 4]>::try_from(v).ok())
        .map_or(0, u32::from_be_bytes))
}

/// Record a key parked for unusable answers (H-1). Returns whether it is in the list now
/// (false only when the list is full).
pub fn record_parked_invalid(txn: &mut RwTxn<'_>, k: &[u8; 9], now: u32) -> Result<bool> {
    let key = parked_invalid_key(k);
    if txn.get(Table::Meta, &key)?.is_some() {
        txn.put(Table::Meta, &key, &now.to_be_bytes())?;
        return Ok(true);
    }
    let len = parked_invalid_len(txn)?;
    if len >= PARKED_INVALID_MAX {
        return Ok(false);
    }
    txn.put(Table::Meta, &key, &now.to_be_bytes())?;
    txn.set_meta(meta::PARKED_INVALID_LEN, &(len + 1).to_be_bytes())?;
    Ok(true)
}

/// The keys in the parked-invalid list.
pub fn parked_invalid(txn: &impl Txn) -> Result<Vec<[u8; 9]>> {
    let p = meta::PARKED_INVALID.as_bytes();
    Ok(txn
        .scan(Table::Meta, p, |k| k.starts_with(p))?
        .into_iter()
        .filter_map(|(k, _)| {
            let hex = k.get(p.len()..)?;
            let mut out = [0u8; 9];
            if hex.len() != 18 {
                return None;
            }
            for (i, b) in out.iter_mut().enumerate() {
                *b = u8::from_str_radix(std::str::from_utf8(&hex[2 * i..2 * i + 2]).ok()?, 16)
                    .ok()?;
            }
            Some(out)
        })
        .collect())
}

/// Put every parked-invalid key back in the seed backlog (height 0; a key with a tail mark
/// keeps it) and clear the list. Returns how many keys the list held.
pub fn requeue_parked_invalid(txn: &mut RwTxn<'_>) -> Result<usize> {
    let keys = parked_invalid(txn)?;
    let mark = Mark {
        height: 0,
        seq: txn.txn_id()?,
    }
    .encode();
    for k in &keys {
        seed_mark(txn, k, &mark)?;
        txn.del(Table::Meta, &parked_invalid_key(k))?;
    }
    txn.del(Table::Meta, meta::PARKED_INVALID_LEN.as_bytes())?;
    Ok(keys.len())
}

static MARKING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// Whether the tail writes balance dirty marks: `[balances] enabled` (M-11), set at startup.
pub fn set_marking(on: bool) {
    MARKING.store(on, std::sync::atomic::Ordering::Relaxed);
}

pub fn marking() -> bool {
    MARKING.load(std::sync::atomic::Ordering::Relaxed)
}

/// Move a chain's mark watermark to `height` (M-11); a jump past watermark + 1 flags a gap.
pub fn note_marked(txn: &mut RwTxn<'_>, chain: u8, height: u32) -> Result<()> {
    let k = format!("{}/{chain}", meta::MARKED_THROUGH);
    if let Some(w) = txn.meta(&k)?.as_deref().and_then(be_u32) {
        if height > w.saturating_add(1) {
            txn.set_meta(meta::MARK_GAP, &height.to_be_bytes())?;
        }
    }
    txn.set_meta(&k, &height.to_be_bytes())
}

/// `Tokens.kind`: a module seen as the module of a `TRANSFER` event (an `xfer` posting).
pub const KIND_FUNGIBLE: u8 = 0;
/// `Tokens.kind`: its `details` calls kept failing (R-P5P9-8 #2). The tail does not mark it,
/// the reconciler drops its dirty keys without a call, `/tokens` hides it; the reconciler
/// makes it [`KIND_FUNGIBLE`] again after its re-check interval.
pub const KIND_NOT_FUNGIBLE: u8 = 1;
/// `Tokens.flags`: the module reached its row cap (I-7); the tail marks no new key of it.
pub const TOKEN_CAPPED: u8 = 1;

/// `(AccountId, ModuleId, chain)` key of `Balances` and `BalanceDirty`.
pub fn bal_key(account: u32, module: u32, chain: u8) -> [u8; 9] {
    let mut k = [0u8; 9];
    k[..4].copy_from_slice(&account.to_be_bytes());
    k[4..8].copy_from_slice(&module.to_be_bytes());
    k[8] = chain;
    k
}

pub fn parse_bal_key(k: &[u8]) -> Option<(u32, u32, u8)> {
    if k.len() != 9 {
        return None;
    }
    Some((be_u32(&k[..4])?, be_u32(&k[4..8])?, k[8]))
}

/// One `Balances` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceRow {
    /// The node has no row for the account in this module on this chain.
    pub absent: bool,
    /// The balance as the node printed it (empty when absent).
    pub balance: String,
    /// blake2b-128 of the guard JSON (zero when absent).
    pub guard: [u8; 16],
    pub as_of: u32,
    pub reconciled_at: u32,
    /// The last re-read after a change failed (I-5): the balance may be outdated.
    pub unverified: bool,
}

impl BalanceRow {
    pub fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(1 + self.balance.len() + 24);
        v.push(u8::from(self.absent) | (u8::from(self.unverified) << 1));
        v.extend_from_slice(self.balance.as_bytes());
        v.extend_from_slice(&self.guard);
        v.extend_from_slice(&self.as_of.to_be_bytes());
        v.extend_from_slice(&self.reconciled_at.to_be_bytes());
        v
    }

    pub fn decode(v: &[u8]) -> Option<BalanceRow> {
        let n = v.len().checked_sub(24)?;
        if n < 1 {
            return None;
        }
        let balance = std::str::from_utf8(&v[1..n]).ok()?.to_owned();
        Some(BalanceRow {
            absent: v[0] & 1 == 1,
            balance,
            guard: v[n..n + 16].try_into().ok()?,
            as_of: be_u32(&v[n + 16..n + 20])?,
            reconciled_at: be_u32(&v[n + 20..n + 24])?,
            unverified: v[0] & 2 == 2,
        })
    }

    /// The balance is above zero (counts as a holder).
    pub fn positive(&self) -> bool {
        !self.absent && decimal_positive(&self.balance)
    }
}

/// A decimal text (`12.5`, `0.0`, `1e-8`) is above zero: no leading minus and a non-zero
/// digit in the mantissa.
pub fn decimal_positive(s: &str) -> bool {
    let s = s.trim();
    if s.starts_with('-') {
        return false;
    }
    let mant = s.split(['e', 'E']).next().unwrap_or("");
    mant.bytes().any(|b| (b'1'..=b'9').contains(&b))
}

/// A dirty mark: the highest height that marked the row, and the id of the write txn that last
/// wrote it. MDBX write txn ids grow, so any later write of the mark (a higher block, a reorg's
/// undo or re-apply at any height, a requeue) changes it, and a reconciler that read a mark
/// deletes only that exact mark (SR-1, I-5, M-3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mark {
    pub height: u32,
    pub seq: u64,
}

impl Mark {
    pub fn encode(&self) -> [u8; 12] {
        let mut v = [0u8; 12];
        v[..4].copy_from_slice(&self.height.to_be_bytes());
        v[4..].copy_from_slice(&self.seq.to_be_bytes());
        v
    }

    /// 12 bytes; a bare 4-byte height decodes with `seq` 0.
    pub fn decode(v: &[u8]) -> Option<Mark> {
        let height = u32::from_be_bytes(v.get(..4)?.try_into().ok()?);
        let seq = match v.len() {
            4 => 0,
            12 => u64::from_be_bytes(v[4..].try_into().ok()?),
            _ => return None,
        };
        Some(Mark { height, seq })
    }
}

/// Height of a `BalanceDirty` value.
pub fn dirty_height(v: &[u8]) -> Option<u32> {
    Mark::decode(v).map(|m| m.height)
}

/// Mark a row dirty at `height` (B-3): the mark keeps the higher height and always takes this
/// txn's id, so a mark written after a reconciler read it never equals what it read. Returns
/// the mark written.
pub fn mark_dirty(
    txn: &mut RwTxn<'_>,
    account: u32,
    module: u32,
    chain: u8,
    height: u32,
) -> Result<Mark> {
    let k = bal_key(account, module, chain);
    let old = txn
        .get(Table::BalanceDirty, &k)?
        .as_deref()
        .and_then(dirty_height)
        .unwrap_or(0);
    let m = Mark {
        height: old.max(height),
        seq: txn.txn_id()?,
    };
    txn.put(Table::BalanceDirty, &k, &m.encode())?;
    // A tail mark takes the key out of the seed backlog (R-P5P9-8 #1).
    txn.del(Table::BalanceSeed, &k)?;
    Ok(m)
}

/// Add a seed mark (B-2) unless the key already has a tail mark or a seed mark; returns
/// whether it was added.
pub fn seed_mark(txn: &mut RwTxn<'_>, k: &[u8; 9], mark: &[u8; 12]) -> Result<bool> {
    if txn.get(Table::BalanceDirty, k)?.is_some() {
        return Ok(false);
    }
    txn.insert(Table::BalanceSeed, k, mark)
}

/// Dirty keys: tail marks plus the seed backlog.
pub fn dirty_count(txn: &impl Txn) -> Result<usize> {
    Ok(txn.count(Table::BalanceDirty)? + txn.count(Table::BalanceSeed)?)
}

/// Delete the mark of `key` in `table` only when it is still exactly `read` (SR-1): a mark
/// raised or rewritten since the read survives. Returns whether it was deleted.
pub fn clear_mark(txn: &mut RwTxn<'_>, table: Table, key: &[u8], read: Mark) -> Result<bool> {
    match txn.get(table, key)?.as_deref().and_then(Mark::decode) {
        Some(m) if m == read => txn.del(table, key),
        _ => Ok(false),
    }
}

/// Flag an existing `Balances` row as unverified (I-5): its re-read after a change failed, so
/// the route serves it `stale` until a later read succeeds.
pub fn set_unverified(txn: &mut RwTxn<'_>, key: &[u8; 9]) -> Result<()> {
    if let Some(mut r) = txn
        .get(Table::Balances, key)?
        .as_deref()
        .and_then(BalanceRow::decode)
    {
        if !r.unverified {
            r.unverified = true;
            txn.put(Table::Balances, key, &r.encode())?;
        }
    }
    Ok(())
}

/// The ModuleId of `coin`, when the dictionary holds it.
pub fn coin_module(txn: &impl Txn) -> Result<Option<u32>> {
    Ok(txn
        .get(Table::DictFwd(DictKind::Module), &fwd_key(b"coin"))?
        .and_then(|v| be_u32(&v)))
}

/// Dirty mark for one posting of a tail block being applied or undone (B-3): an `xfer` posting
/// marks `(account, module, chain)` and notes the token; an `acct` posting marks
/// `(account, coin, chain)`. Other families (`pk`, `evt`, `code`, `pact`, `sndr`) are ignored. `note_tokens` is false on undo (a
/// token's first height is never raised).
pub fn mark_posting(
    txn: &mut RwTxn<'_>,
    family: u8,
    entity: u64,
    key: TxKey,
    coin: Option<u32>,
    note_tokens: bool,
) -> Result<()> {
    let (chain, height) = (key.chain(), key.height());
    if family == Family::Xfer as u8 {
        let (account, module) = ((entity >> 32) as u32, entity as u32);
        if !mark_row(txn, account, module, chain, height, coin)? {
            return Ok(());
        }
        if note_tokens {
            note_token(txn, module, height)?;
        }
    } else if family == Family::Acct as u8 {
        if let (Some(coin), Ok(account)) = (coin, u32::try_from(entity)) {
            mark_dirty(txn, account, coin, chain, height)?;
        }
    }
    Ok(())
}

/// Mark `(account, module, chain)` dirty at `height` (B-3), the one mark path of postings and
/// D3 rows (tail and seed pass): no mark for a not-fungible module (R-P5P9-8 #2), nor for a
/// new key of a module at its row cap (I-7). Returns whether it marked.
pub fn mark_row(
    txn: &mut RwTxn<'_>,
    account: u32,
    module: u32,
    chain: u8,
    height: u32,
    coin: Option<u32>,
) -> Result<bool> {
    if Some(module) != coin {
        if let Some(t) = token_row(txn, module)? {
            if t.kind == KIND_NOT_FUNGIBLE
                || (t.capped()
                    && txn
                        .get(Table::Balances, &bal_key(account, module, chain))?
                        .is_none())
            {
                return Ok(false);
            }
        }
    }
    mark_dirty(txn, account, module, chain, height)?;
    Ok(true)
}

/// One `Tokens` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TokenRow {
    pub first_height: u32,
    pub holders: u64,
    pub kind: u8,
    /// b0 [`TOKEN_CAPPED`].
    pub flags: u8,
    /// `Balances` rows of the module (I-7 cap).
    pub rows: u32,
    /// Chains where `describe-module` lists `fungible-v2` (I-6): rows of the module on these
    /// chains feed `KeyAccounts`.
    pub fungible_chains: u32,
    /// Chains whose fungible check is done since `checked_at`.
    pub checked_chains: u32,
    /// Unix seconds: start of the current check round, or of the not-fungible verdict.
    pub checked_at: u32,
}

impl TokenRow {
    pub fn encode(&self) -> [u8; 30] {
        let mut v = [0u8; 30];
        v[..4].copy_from_slice(&self.first_height.to_be_bytes());
        v[4..12].copy_from_slice(&self.holders.to_be_bytes());
        v[12] = self.kind;
        v[13] = self.flags;
        v[14..18].copy_from_slice(&self.rows.to_be_bytes());
        v[18..22].copy_from_slice(&self.fungible_chains.to_be_bytes());
        v[22..26].copy_from_slice(&self.checked_chains.to_be_bytes());
        v[26..30].copy_from_slice(&self.checked_at.to_be_bytes());
        v
    }
    /// 30 bytes; the 13-byte form written before the checks decodes with them zero.
    pub fn decode(v: &[u8]) -> Option<TokenRow> {
        if v.len() != 13 && v.len() != 30 {
            return None;
        }
        let mut t = TokenRow {
            first_height: be_u32(&v[..4])?,
            holders: u64::from_be_bytes(v[4..12].try_into().ok()?),
            kind: v[12],
            ..TokenRow::default()
        };
        if v.len() == 30 {
            t.flags = v[13];
            t.rows = be_u32(&v[14..18])?;
            t.fungible_chains = be_u32(&v[18..22])?;
            t.checked_chains = be_u32(&v[22..26])?;
            t.checked_at = be_u32(&v[26..30])?;
        }
        Some(t)
    }
    pub fn capped(&self) -> bool {
        self.flags & TOKEN_CAPPED != 0
    }
    /// The module's guards on `chain` are trusted (I-6).
    pub fn fungible_on(&self, chain: u8) -> bool {
        chain < 32 && self.fungible_chains & (1 << chain) != 0
    }
}

/// The `Tokens` row of a module.
pub fn token_row(txn: &impl Txn, module: u32) -> Result<Option<TokenRow>> {
    match txn.get(Table::Tokens, &module.to_be_bytes())? {
        Some(v) => Ok(Some(TokenRow::decode(&v).context("bad Tokens row")?)),
        None => Ok(None),
    }
}

/// Change a token row in place; a module with no row gets one with `first_height` u32::MAX
/// until a posting lowers it. It is queued for its D3 history walk ([`meta::D3_REWALK`])
/// when its `first_height` becomes lower than the D3 done height.
pub fn update_token(txn: &mut RwTxn<'_>, module: u32, f: impl FnOnce(&mut TokenRow)) -> Result<()> {
    let mut r = token_row(txn, module)?.unwrap_or(TokenRow {
        first_height: u32::MAX,
        ..TokenRow::default()
    });
    let before = r;
    f(&mut r);
    let new = txn.get(Table::Tokens, &module.to_be_bytes())?.is_none();
    if r != before || new {
        txn.put(Table::Tokens, &module.to_be_bytes(), &r.encode())?;
    }
    // Newly tracked, or first seen lower: a credit below the D3 done height may predate the
    // token's tracking, so its history is walked (review M2). A token first seen at or above
    // that height is covered by the tail and the next pass (re-review N2). No done height
    // yet: the first pass (from 0) covers it, and clears the tokens it started with.
    let done = txn
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)?
        .as_deref()
        .and_then(be_u32)
        .unwrap_or(u32::MAX);
    if r.first_height < done
        && (new || before.first_height >= done)
        && coin_module(txn)? != Some(module)
    {
        txn.set_meta(&format!("{}{module:010}", meta::D3_REWALK), &[])?;
    }
    Ok(())
}

/// Whether the guards of `module`'s rows on `chain` feed `KeyAccounts` (I-6): `coin`, or a
/// module whose `describe-module` lists `fungible-v2` on that chain.
pub fn trusted(txn: &impl Txn, module: u32, chain: u8, coin: Option<u32>) -> Result<bool> {
    Ok(Some(module) == coin || token_row(txn, module)?.is_some_and(|t| t.fungible_on(chain)))
}

/// Create the token row or lower its `first_height` to `height`.
pub fn note_token(txn: &mut RwTxn<'_>, module: u32, height: u32) -> Result<()> {
    update_token(txn, module, |r| r.first_height = r.first_height.min(height))
}

/// Add `delta` to a token's holder count (saturating at 0).
pub fn adjust_holders(txn: &mut RwTxn<'_>, module: u32, delta: i64) -> Result<()> {
    update_token(txn, module, |r| {
        r.holders = if delta >= 0 {
            r.holders.saturating_add(delta as u64)
        } else {
            r.holders.saturating_sub(delta.unsigned_abs())
        }
    })
}

/// Guard hash of the guard JSON bytes.
pub fn guard_hash(json: &[u8]) -> [u8; 16] {
    blake2b_128(json)
}

/// `Guards` key of a keyset reference resolved on `chain` (B-4; keysets are per chain):
/// blake2b-128 of `keysetref:{chain}:{name}`.
pub fn ref_hash(chain: u8, name: &str) -> [u8; 16] {
    blake2b_128(format!("keysetref:{chain}:{name}").as_bytes())
}

/// `KeyAccounts` key prefix of a public key.
pub fn key_hash(pubkey: &str) -> [u8; 16] {
    blake2b_128(pubkey.as_bytes())
}

pub fn key_account_key(pubkey: &[u8; 16], account: u32) -> [u8; 20] {
    let mut k = [0u8; 20];
    k[..16].copy_from_slice(pubkey);
    k[16..].copy_from_slice(&account.to_be_bytes());
    k
}

/// What a guard contributes to `KeyAccounts` (B-4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardKeys {
    /// An inline keyset: its keys.
    Keys(Vec<String>),
    /// A keyset reference (`ns.name` or `name`), resolved with `describe-keyset`.
    Ref(String),
    /// User, capability, module and pact guards: no rows.
    Other,
}

/// Most keys of an inline keyset that `KeyAccounts` indexes (F-14); a larger one is `Other`.
pub const MAX_KEYSET_KEYS: usize = 64;
/// Longest key, in bytes, of an indexed keyset (F-14); a keyset with a longer one is `Other`.
/// Not 128: mainnet holds 9,372 guards with `WEBAUTHN-` keys of 160 to 191 bytes (KDA-03 hot
/// store scan, 2026-11-22), and those must stay in the key index.
pub const MAX_KEY_LEN: usize = 256;

/// Classify a guard as the node returns it (Pact 4 and Pact 5 JSON forms).
pub fn guard_keys(g: &Value) -> GuardKeys {
    if let (Some(Value::Array(keys)), true) = (g.get("keys"), g.get("pred").is_some()) {
        // F-14: a keyset too large to be a real one gives no rows (it would write one
        // `KeyAccounts` row per key).
        if keys.len() > MAX_KEYSET_KEYS
            || keys
                .iter()
                .any(|k| k.as_str().is_some_and(|k| k.len() > MAX_KEY_LEN))
        {
            return GuardKeys::Other;
        }
        return GuardKeys::Keys(
            keys.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        );
    }
    match g.get("keysetref") {
        Some(Value::String(s)) => GuardKeys::Ref(s.clone()),
        Some(Value::Object(o)) => {
            let ksn = o.get("ksn").and_then(Value::as_str).unwrap_or("");
            match o.get("ns").and_then(Value::as_str) {
                _ if ksn.is_empty() => GuardKeys::Other,
                Some(ns) if !ns.is_empty() => GuardKeys::Ref(format!("{ns}.{ksn}")),
                _ => GuardKeys::Ref(ksn.to_owned()),
            }
        }
        _ => GuardKeys::Other,
    }
}

/// The public keys of a stored guard of a row on `chain`: inline keys, or the cached
/// resolution of a reference.
pub fn stored_guard_keys(txn: &impl Txn, guard: &[u8; 16], chain: u8) -> Result<Vec<String>> {
    let Some(v) = txn.get(Table::Guards, guard)? else {
        return Ok(vec![]);
    };
    let g: Value = match serde_json::from_slice(&v) {
        Ok(g) => g,
        Err(_) => return Ok(vec![]),
    };
    Ok(match guard_keys(&g) {
        GuardKeys::Keys(k) => k,
        GuardKeys::Ref(name) => {
            match txn
                .get(Table::Guards, &ref_hash(chain, &name))?
                .and_then(|v| serde_json::from_slice::<Value>(&v).ok())
                .map(|g| guard_keys(&g))
            {
                Some(GuardKeys::Keys(k)) => k,
                _ => vec![],
            }
        }
        GuardKeys::Other => vec![],
    })
}

/// Rows of one account in `Balances`, in key order.
pub fn account_rows(txn: &impl Txn, account: u32) -> Result<Vec<(u32, u8, BalanceRow)>> {
    let p = account.to_be_bytes();
    let mut out = Vec::new();
    for (k, v) in txn.scan(Table::Balances, &p, |k| k.starts_with(&p))? {
        let (_, m, c) = parse_bal_key(&k).context("bad Balances key")?;
        out.push((m, c, BalanceRow::decode(&v).context("bad Balances row")?));
    }
    Ok(out)
}

/// Whether `(account, module, chain)` waits for a reconcile (a tail or a seed mark).
pub fn is_dirty(txn: &impl Txn, account: u32, module: u32, chain: u8) -> Result<bool> {
    let k = bal_key(account, module, chain);
    Ok(txn.get(Table::BalanceDirty, &k)?.is_some() || txn.get(Table::BalanceSeed, &k)?.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Geometry, HotStore};

    /// F-14: a keyset of more than [`MAX_KEYSET_KEYS`] keys, or with a key longer than
    /// [`MAX_KEY_LEN`] bytes, indexes nothing.
    #[test]
    fn oversize_keysets_index_nothing() {
        let ks = |keys: Vec<String>| serde_json::json!({"keys": keys, "pred": "keys-all"});
        let n = |n: usize| (0..n).map(|i| format!("{i:064x}")).collect::<Vec<_>>();
        assert!(matches!(guard_keys(&ks(n(MAX_KEYSET_KEYS))), GuardKeys::Keys(k) if k.len() == 64));
        assert_eq!(guard_keys(&ks(n(MAX_KEYSET_KEYS + 1))), GuardKeys::Other);
        assert!(matches!(
            guard_keys(&ks(vec!["a".repeat(MAX_KEY_LEN)])),
            GuardKeys::Keys(_)
        ));
        assert_eq!(
            guard_keys(&ks(vec!["a".repeat(MAX_KEY_LEN + 1)])),
            GuardKeys::Other
        );
        // A WebAuthn key as long as the longest one on mainnet (191 bytes) stays indexed.
        let webauthn = format!("WEBAUTHN-{}", "a5".repeat(91));
        assert!(matches!(
            guard_keys(&ks(vec![webauthn])),
            GuardKeys::Keys(_)
        ));
    }

    /// `--rewalk`: listed modules join the pending set, a running walk restarts with its own
    /// tokens still pending, and the daily spacing is reset.
    #[test]
    fn queue_rewalk_restarts_with_every_pending_token() {
        let d = tempfile::tempdir().unwrap();
        let hs = HotStore::open(d.path(), Geometry::default()).unwrap();
        let mut txn = hs.rw().unwrap();
        txn.put(
            Table::Meta,
            format!("{}{:010}", meta::D3_REWALK, 479).as_bytes(),
            &[],
        )
        .unwrap();
        txn.set_meta(meta::D3_REWALK_WALK, b"{}").unwrap();
        txn.set_meta(meta::D3_REWALK_AT, &5u32.to_be_bytes())
            .unwrap();
        assert!(queue_rewalk(&mut txn, &[12, 479]).unwrap());
        let mut p = rewalk_pending(&txn).unwrap();
        p.sort_unstable();
        assert_eq!(p, vec![12, 479]);
        assert!(txn.meta(meta::D3_REWALK_WALK).unwrap().is_none());
        assert!(txn.meta(meta::D3_REWALK_AT).unwrap().is_none());
        assert!(!queue_rewalk(&mut txn, &[12]).unwrap());
    }

    /// H-1: parked-invalid keys are listed once, bounded, and the retry pass seeds them back.
    #[test]
    fn parked_invalid_list_records_and_requeues() {
        let d = tempfile::tempdir().unwrap();
        let hs = HotStore::open(d.path(), Geometry::default()).unwrap();
        let (a, b) = (bal_key(1, 2, 3), bal_key(4, 5, 6));
        let mut txn = hs.rw().unwrap();
        assert!(record_parked_invalid(&mut txn, &a, 10).unwrap());
        assert!(record_parked_invalid(&mut txn, &a, 11).unwrap());
        assert!(record_parked_invalid(&mut txn, &b, 12).unwrap());
        assert_eq!(parked_invalid_len(&txn).unwrap(), 2);
        let mut l = parked_invalid(&txn).unwrap();
        l.sort_unstable();
        assert_eq!(l, vec![a, b]);
        txn.set_meta(meta::PARKED_INVALID_LEN, &PARKED_INVALID_MAX.to_be_bytes())
            .unwrap();
        assert!(!record_parked_invalid(&mut txn, &bal_key(7, 8, 9), 13).unwrap());
        // `b` has a tail mark: it keeps it and gets no seed mark.
        mark_dirty(&mut txn, 4, 5, 6, 20).unwrap();
        assert_eq!(requeue_parked_invalid(&mut txn).unwrap(), 2);
        assert!(parked_invalid(&txn).unwrap().is_empty());
        assert_eq!(parked_invalid_len(&txn).unwrap(), 0);
        assert!(txn.get(Table::BalanceSeed, &a).unwrap().is_some());
        assert!(txn.get(Table::BalanceSeed, &b).unwrap().is_none());
    }

    #[test]
    fn row_codec_and_marks() {
        let r = BalanceRow {
            absent: false,
            balance: "12.000000000001".into(),
            guard: [9; 16],
            as_of: 7,
            reconciled_at: 1_700_000_000,
            unverified: true,
        };
        assert_eq!(BalanceRow::decode(&r.encode()), Some(r.clone()));
        assert!(r.positive());
        assert!(!decimal_positive("0.0"));
        assert!(decimal_positive("1e-8"));
        assert!(!decimal_positive("-3"));
        let t = TokenRow {
            first_height: 5,
            holders: 3,
            kind: KIND_NOT_FUNGIBLE,
            flags: TOKEN_CAPPED,
            rows: 9,
            fungible_chains: 1 << 19,
            checked_chains: 3,
            checked_at: 77,
        };
        assert_eq!(TokenRow::decode(&t.encode()), Some(t));
        assert!(t.capped() && t.fungible_on(19) && !t.fungible_on(0));
        let mut old = [0u8; 13];
        old[..4].copy_from_slice(&5u32.to_be_bytes());
        assert_eq!(TokenRow::decode(&old).map(|t| t.first_height), Some(5));

        let d = tempfile::tempdir().unwrap();
        let hs = HotStore::open(d.path(), Geometry::default()).unwrap();
        let mut txn = hs.rw().unwrap();
        mark_dirty(&mut txn, 1, 2, 3, 100).unwrap();
        mark_dirty(&mut txn, 1, 2, 3, 90).unwrap();
        let v = txn.get(Table::BalanceDirty, &bal_key(1, 2, 3)).unwrap();
        assert_eq!(v.as_deref().and_then(dirty_height), Some(100));
        note_token(&mut txn, 2, 50).unwrap();
        note_token(&mut txn, 2, 60).unwrap();
        adjust_holders(&mut txn, 2, 1).unwrap();
        adjust_holders(&mut txn, 2, -5).unwrap();
        let t = TokenRow::decode(
            &txn.get(Table::Tokens, &2u32.to_be_bytes())
                .unwrap()
                .unwrap(),
        );
        assert_eq!(t.map(|t| (t.first_height, t.holders)), Some((50, 0)));
        txn.commit().unwrap();
    }

    #[test]
    fn guard_forms() {
        let ks: Value = serde_json::json!({"keys": ["ab", "cd"], "pred": "keys-all"});
        assert_eq!(
            guard_keys(&ks),
            GuardKeys::Keys(vec!["ab".into(), "cd".into()])
        );
        let r4: Value = serde_json::json!({"keysetref": "ns.admin"});
        assert_eq!(guard_keys(&r4), GuardKeys::Ref("ns.admin".into()));
        let r5: Value = serde_json::json!({"keysetref": {"ns": "free", "ksn": "k"}});
        assert_eq!(guard_keys(&r5), GuardKeys::Ref("free.k".into()));
        let r5b: Value = serde_json::json!({"keysetref": {"ns": null, "ksn": "k"}});
        assert_eq!(guard_keys(&r5b), GuardKeys::Ref("k".into()));
        let cap: Value = serde_json::json!({"cgName": "x.Y", "cgArgs": [], "cgPactId": null});
        assert_eq!(guard_keys(&cap), GuardKeys::Other);
    }

    /// A-1: a store without the additive tables opens read-only and reads them as empty.
    #[test]
    fn read_only_without_additive_tables() {
        let d = tempfile::tempdir().unwrap();
        {
            // A v2 store as P5 wrote it: base tables only.
            let env = reth_libmdbx::Environment::builder()
                .set_max_dbs(crate::env::MAX_DBS)
                .open(d.path())
                .unwrap();
            let txn = env.begin_rw_txn().unwrap();
            for t in Table::all().into_iter().filter(|t| !t.is_additive()) {
                let db = txn
                    .create_db(Some(&t.name()), reth_libmdbx::DatabaseFlags::CREATE)
                    .unwrap();
                if t == Table::Meta {
                    txn.put(
                        db.dbi(),
                        crate::env::meta::FORMAT_VERSION,
                        crate::FORMAT_VERSION.to_be_bytes(),
                        reth_libmdbx::WriteFlags::empty(),
                    )
                    .unwrap();
                }
            }
            txn.commit().unwrap();
        }
        let ro = HotStore::open_ro(d.path()).unwrap();
        let txn = ro.ro().unwrap();
        assert!(!txn.has_table(Table::Balances));
        assert_eq!(txn.count(Table::Balances).unwrap(), 0);
        assert!(txn.get(Table::Tokens, &[0, 0, 0, 0]).unwrap().is_none());
        assert!(account_rows(&txn, 1).unwrap().is_empty());
        assert_eq!(
            txn.scan_rev_prefix(Table::KeyAccounts, &[], &[])
                .unwrap()
                .count(),
            0
        );
        drop(txn);
        drop(ro);
        // `open` (rw) creates them; the read-only open then sees them.
        drop(HotStore::open(d.path(), Geometry::default()).unwrap());
        let ro = HotStore::open_ro(d.path()).unwrap();
        assert!(ro.ro().unwrap().has_table(Table::Balances));
    }
}
