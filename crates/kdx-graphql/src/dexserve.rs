//! Serving stock's DEX surface (stock-gap plan B3, part 2) from the Sushi ledger that
//! `kdx_hot::dex` stores, with USD values from the hourly DIA KDA/USD rate
//! (`kdx_hot::dex_usd`). Stock source: indexer-kadena `pool-db-repository.ts`,
//! `dex-metrics-db-repository.ts`, `liquidity-position-db-repository.ts`,
//! `token-price-db-repository.ts`, `pair-service.ts` and `pagination.ts`.
//!
//! Formulas (stock's, exact here):
//! - A token's KDA price is the reserve ratio along the first path to `coin` (stock's BFS,
//!   `findOptimalKdaPricePath`); USD = KDA × the KDA/USD rate.
//! - A pool's TVL = reserve0 × price0 + reserve1 × price1, which for a `coin` pair is
//!   2 × the coin reserve (in KDA), × the rate of the hour.
//! - A swap's value is its coin leg (coin in, else coin out); its fee is value × 0.003/0.997
//!   (stock: `in × FEE / (1 − FEE)`). An add/remove is worth 2 × its coin amount.
//! - Windows (stock's `updatePoolStats`): "24h" = the current UTC day so far, its previous
//!   value = the previous UTC day; "7d" = the 7 UTC days before today. Changes are
//!   `(cur − prev) / prev × 100`, 0 when prev is 0. `apr24h = fees24h × 365 / tvl`.
//! - `Float` fields are rounded to 8 places (stock's `formatTo8Decimals`); `amountUsd` and
//!   chart volumes/fees are text with 10 places, chart TVL text with 8 (stock's columns).
//!
//! Deliberate divergences from stock (each is a stock mistake; `docs/graphql.md` and the
//! compare allowlist carry them):
//! 1. `totalSupply` = Σ MINT − Σ BURN of the LP token (stock's float running sum went
//!    negative).
//! 2. Sorting is correct for every `PoolOrderBy` (stock served every order reversed:
//!    `TVL_USD_DESC` started with the smallest pool, `VOLUME_7D_DESC` with a zero volume).
//!    The cursor format is kept: base64 of a rank, the first item holding the largest rank.
//! 3. `dexMetrics` defaults `protocolAddress` to Sushi, like `pools` (stock's default names a
//!    DEX it does not index and answers zeros).
//! 4. USD is priced with the rate of the event's hour, not the rate at ingest time.
//! 5. Exact decimals instead of floats; amounts are canonical text (`1.25672e-7` is
//!    `0.000000125672`, trailing zeros dropped).
//! 6. `charts` has a TVL point for every day of the range (stock only for days with an
//!    event, so a quiet week answered `charts(WEEK)` empty).
//! 7. Stats are computed at query time (stock froze them at the pair's last event, so a quiet
//!    pool kept an old day's "24h" volume forever).
//! 8. Volume counts swaps only (stock also added liquidity deposits and withdrawals).
//! 9. A REMOVE_LIQUIDITY's amounts are oriented to the pair (stock crossed them).
//! 10. `pageInfo` is computed from the rows (stock's `poolTransactions` answered
//!     `hasPreviousPage: true` on every page); `before` and `last` page backwards.
//! 11. Not reproducible: stock DB row ids (`Token.id` is the module name,
//!     `PoolTransaction.id` is the event's chain position, `transactionId` is null), DB
//!     timestamps (`createdAt`/`updatedAt` are block times), `Token.chainId` is `"2"` (stock
//!     hardcoded `"0"`), and `tokens` ranks modules by first height (stock by a balance row
//!     id).
//!
//! Times match stock: windows, chart buckets and the `amountUsd` rate hour use the tx's
//! `meta.creationTime` (block time when absent), as stored per event (`kdx_hot::dex`).
//!
//! Cost: nothing walks a pair's whole history. Listings read only coin pairs that were ever
//! funded, rank them by coin reserve, filter them as listable and only then cap them
//! ([`MAX_PAIRS`], flagged `extensions.dexPairsCapped` when hit);
//! `poolTransactions.totalCount` reads the stored per-kind counts; range reads seek by time
//! ([`seek_time`]); a [`Budget`] of rows is checked as rows are read and against the request
//! deadline in every loop; aggregates and price searches run on a heavy slot (resolvers).

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

use async_graphql::ID;
use base64::Engine;
use kdx_api::{ApiError, ApiResult, View};
use kdx_hot::dex::{self as hd, Amount, Bucket, Kind, PairStatus, StoredEvent};
use kdx_hot::dex_usd::{self, Rates};
use kdx_hot::{Table, Txn};
use kdx_types::{Dict, DictKind, TxKey};

use crate::connection::PageInfo;
use crate::dexnum::Dec;
use crate::scalars::{DateTime, Decimal};
use crate::types::dex::*;

pub const DEFAULT_PROTOCOL: &str = hd::SUSHI_EXCHANGE;
/// Listable pairs one field serves, largest coin reserve first; past it the field answers
/// those and flags the response `extensions.dexPairsCapped` ([`Budget::capped`]).
pub const MAX_PAIRS: usize = 1_000;
/// Rows one field reads (checked as rows are read, so it also bounds what a field holds).
pub const MAX_ROWS: u64 = 250_000;
/// The price path: at most 3 hops (stock's `findOptimalKdaPricePath` stops at path length 4).
pub const MAX_HOPS: usize = 3;
/// Pairs of one token a price search expands (largest own reserve first), and tokens it
/// visits.
pub const MAX_NEIGHBORS: usize = 256;
/// Pairs of one token a price search reads to rank its neighbours.
pub const MAX_RANKED_NEIGHBORS: usize = 1_024;
pub const MAX_PRICE_TOKENS: usize = 2_048;
/// Longest `dexMetrics` range.
pub const MAX_METRIC_DAYS: u64 = 1_100;
/// Largest page.
pub const MAX_PAGE: i32 = crate::MAX_PAGE_ROWS as i32;
const DAY: u64 = 86_400;
const HOUR: u64 = 3_600;

/// Row and deadline budget of one field.
pub struct Budget {
    dl: Instant,
    rows: u64,
    max: u64,
    out: bool,
    /// A listing had more than [`MAX_PAIRS`] listable pairs and served the largest: the
    /// resolver flags the response (never a silent partial list).
    pub capped: bool,
}

impl Budget {
    pub fn new(dl: Instant) -> Budget {
        Budget::with_rows(dl, MAX_ROWS)
    }

    /// A budget of `max` rows instead of [`MAX_ROWS`] (tests of the spent-budget paths).
    pub fn with_rows(dl: Instant, max: u64) -> Budget {
        Budget {
            dl,
            rows: 0,
            max,
            out: false,
            capped: false,
        }
    }

    /// Count a row; false once the budget or the deadline is spent.
    pub fn tick(&mut self) -> bool {
        self.rows += 1;
        if self.rows > self.max || (self.rows.is_multiple_of(64) && Instant::now() >= self.dl) {
            self.out = true;
        }
        !self.out
    }

    /// Rows counted so far.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn check(&self) -> ApiResult<()> {
        if self.out || Instant::now() >= self.dl {
            Err(ApiError::Deadline)
        } else {
            Ok(())
        }
    }
}

fn api(e: anyhow::Error) -> ApiError {
    ApiError::Unavailable(format!("{e:#}"))
}

fn bad(msg: impl Into<String>) -> ApiError {
    ApiError::BadRequest(msg.into())
}

fn unavailable(id: &str) -> ApiError {
    ApiError::Unavailable(format!(
        "pool {id} is unavailable: its stored DEX data could not be read"
    ))
}

// Pairs.

/// What a field knows of one pair.
#[derive(Debug, Clone)]
pub struct PairInfo {
    pub key: String,
    pub t0: String,
    pub t1: String,
    /// The `coin` side (0 or 1).
    pub coin: Option<usize>,
    pub status: PairStatus,
    pub created_us: u64,
    pub updated_us: u64,
}

impl PairInfo {
    pub fn id(&self) -> String {
        format!("{DEFAULT_PROTOCOL}-{}", self.key)
    }

    fn reserve(&self, side: usize) -> Dec {
        self.status
            .reserves
            .as_ref()
            .map(|r| Dec::from_amount(if side == 0 { &r.0 } else { &r.1 }))
            .unwrap_or_default()
    }

    /// TVL in KDA: 2 × the coin reserve.
    fn tvl_kda(&self, reserves: Option<&(Amount, Amount)>) -> Dec {
        match (self.coin, reserves) {
            (Some(c), Some(r)) => Dec::from_amount(if c == 0 { &r.0 } else { &r.1 }).mul_int(2),
            _ => Dec::zero(),
        }
    }
}

/// The pair key of a pool id (`<exchange>-<key>`).
pub fn pair_key_of_id(id: &str) -> Option<&str> {
    id.strip_prefix(DEFAULT_PROTOCOL)?.strip_prefix('-')
}

fn first_time(txn: &impl Txn, prefix: &[u8], rev: bool) -> ApiResult<Option<StoredEvent>> {
    let row = if rev {
        txn.scan_rev_prefix(Table::DexEvents, prefix, &[])
            .map_err(api)?
            .next()
            .transpose()
            .map_err(api)?
    } else {
        txn.scan_limit(Table::DexEvents, prefix, 1, |k| k.starts_with(prefix))
            .map_err(api)?
            .into_iter()
            .next()
    };
    Ok(row.and_then(|(_, v)| StoredEvent::decode(&v).ok()))
}

/// A pair's info; `None` when no `CREATE_PAIR` is stored.
pub fn pair_info(txn: &impl Txn, key: &str) -> ApiResult<Option<PairInfo>> {
    let status = hd::pair_status(txn, key).map_err(api)?;
    if !status.created {
        return Ok(None);
    }
    let (t0, t1) = key.split_once(':').unwrap_or((key, ""));
    let coin = if t0 == "coin" {
        Some(0)
    } else if t1 == "coin" {
        Some(1)
    } else {
        None
    };
    let created_us = first_time(txn, &hd::event_prefix(key, Kind::CreatePair), false)?
        .map(|e| e.time)
        .unwrap_or(0);
    let updated_us = first_time(txn, &hd::event_prefix(key, Kind::Update), true)?
        .map(|e| e.time)
        .unwrap_or(created_us);
    Ok(Some(PairInfo {
        key: key.to_owned(),
        t0: t0.to_owned(),
        t1: t1.to_owned(),
        coin,
        status,
        created_us,
        updated_us,
    }))
}

/// Listable pairs (`PairStatus::listable`: created, not poisoned, a coin leg, non-zero
/// reserves), largest coin reserve first, then by pair key. Candidates are the coin pairs
/// that were ever funded (`kdx_hot::dex::funded_pairs`): each costs its funded key and its
/// latest `UPDATE` ([`hd::latest_reserves`]), so pairs created and never funded are never
/// read, and about 80k funded pairs fit the row budget. Ranked by the coin reserve, the walk
/// then reads full pair info until [`MAX_PAIRS`] listable pairs are kept, so dust-funded spam
/// (however many, however old) ranks below real pools and only costs its two rows.
/// [`Budget::capped`] is set only when another listable pair exists past the kept ones.
/// Spent budget or deadline is an error, never a partial list.
///
/// Ranks are recomputed per request (as stock ranks its pools per request), so a cursor of a
/// listing names a position in that request's order: a reserve change between two pages can
/// move a pool across the page boundary.
pub fn listable(txn: &impl Txn, b: &mut Budget) -> ApiResult<Vec<PairInfo>> {
    let mut ranked: Vec<(Dec, String)> = Vec::new();
    let mut err = None;
    hd::funded_pairs(txn, |pair| {
        // The funded key and the latest UPDATE.
        if !(b.tick() && b.tick()) {
            return false;
        }
        match hd::latest_reserves(txn, pair) {
            Ok(Some((r0, r1))) if !r0.is_zero() && !r1.is_zero() => {
                let c = if pair.split(':').next() == Some("coin") {
                    r0
                } else {
                    r1
                };
                ranked.push((Dec::from_amount(&c), pair.to_owned()));
            }
            Ok(_) => {}
            Err(e) => {
                err = Some(e);
                return false;
            }
        }
        true
    })
    .map_err(api)?;
    if let Some(e) = err {
        return Err(api(e));
    }
    b.check()?;
    ranked.sort_by(|x, y| y.0.cmp(&x.0).then_with(|| x.1.cmp(&y.1)));
    let mut out = Vec::new();
    for (_, k) in ranked {
        // A pair's info: its status (three reads) and two time reads.
        for _ in 0..5 {
            b.tick();
        }
        if out.len() == MAX_PAIRS && b.check().is_err() {
            // The cap is reached and the budget ends before the next pair is known: serve
            // the kept pairs, flagged (another listable pair may exist past them).
            b.capped = true;
            break;
        }
        b.check()?;
        let Some(p) = pair_info(txn, &k)? else {
            continue;
        };
        if !p.status.listable() {
            continue;
        }
        if out.len() == MAX_PAIRS {
            tracing::warn!(
                cap = MAX_PAIRS,
                "dex: more listable pairs than the cap; the largest are served"
            );
            b.capped = true;
            break;
        }
        out.push(p);
    }
    Ok(out)
}

/// A stored amount (`None` when absent); an unreadable one makes the pool unavailable.
fn stored_amount(v: Option<Vec<u8>>, key: &str) -> ApiResult<Option<Amount>> {
    match v {
        None => Ok(None),
        Some(v) => std::str::from_utf8(&v)
            .ok()
            .and_then(Amount::parse_stored)
            .map(Some)
            .ok_or_else(|| unavailable(&format!("{DEFAULT_PROTOCOL}-{key}"))),
    }
}

fn supply(txn: &impl Txn, key: &str) -> ApiResult<Amount> {
    let v = txn.get(Table::DexAgg, &hd::supply_key(key)).map_err(api)?;
    Ok(stored_amount(v, key)?.unwrap_or_default())
}

/// Hour buckets of a pair in `from..=to`.
fn buckets(
    txn: &impl Txn,
    key: &str,
    from: u32,
    to: u32,
    b: &mut Budget,
) -> ApiResult<Vec<(u32, Bucket)>> {
    let mut prefix = hd::bucket_key(key, 0);
    prefix.truncate(prefix.len() - 4);
    let rows = txn
        .scan(Table::DexAgg, &hd::bucket_key(key, from), |k| {
            b.tick()
                && k.starts_with(&prefix)
                && k.len() == prefix.len() + 4
                && u32::from_be_bytes([
                    k[k.len() - 4],
                    k[k.len() - 3],
                    k[k.len() - 2],
                    k[k.len() - 1],
                ]) <= to
        })
        .map_err(api)?;
    b.check()?;
    Ok(rows
        .into_iter()
        .filter_map(|(k, v)| {
            let h = u32::from_be_bytes(k[k.len() - 4..].try_into().ok()?);
            Some((h, Bucket::decode(&v).ok()?))
        })
        .collect())
}

/// The first `DexEvents` key under `prefix` (one pair and kind) of an event in a block at or
/// after `t_us`: a binary search over heights (block times rise with height on the Sushi
/// chain), about 32 point seeks whatever the pair's history.
fn seek_time(txn: &impl Txn, prefix: &[u8], t_us: u64, b: &mut Budget) -> ApiResult<Vec<u8>> {
    let (mut lo, mut hi) = (0u64, 1u64 << 32);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        b.tick();
        let mut k = prefix.to_vec();
        k.extend_from_slice(&(mid as u32).to_be_bytes());
        let row = txn
            .scan_limit(Table::DexEvents, &k, 1, |kk| kk.starts_with(prefix))
            .map_err(api)?
            .into_iter()
            .next();
        let at_or_after = match row {
            None => true,
            Some((_, v)) => StoredEvent::decode(&v).map_or(true, |e| e.time >= t_us),
        };
        if at_or_after {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    b.check()?;
    let mut k = prefix.to_vec();
    match u32::try_from(lo) {
        Ok(h) => k.extend_from_slice(&h.to_be_bytes()),
        Err(_) => k.extend_from_slice(&[0xff; 11]),
    }
    Ok(k)
}

type Reserves = (Amount, Amount);
/// Reserves before a range, then each UPDATE in it as (block time µs, reserves).
type Updates = (Option<Reserves>, Vec<(u64, Reserves)>);

/// The UPDATEs of a pair in blocks of `from_us..to_us`, as (block time µs, reserves), with the
/// reserves before `from_us` (carry-in). Reads only the range (two seeks, then the rows).
fn updates(
    txn: &impl Txn,
    key: &str,
    from_us: u64,
    to_us: u64,
    b: &mut Budget,
) -> ApiResult<Updates> {
    let prefix = hd::event_prefix(key, Kind::Update);
    let start = seek_time(txn, &prefix, from_us, b)?;
    let end = seek_time(txn, &prefix, to_us, b)?;
    let carry = newest_before(txn, &prefix, &start, b)?;
    let rows = txn
        .scan(Table::DexEvents, &start, |k| {
            b.tick() && k.starts_with(&prefix) && k < end.as_slice()
        })
        .map_err(api)?;
    b.check()?;
    let ups = rows
        .into_iter()
        .filter_map(|(_, v)| {
            let e = StoredEvent::decode(&v).ok()?;
            Some((e.time, (e.amount(0)?, e.amount(1)?)))
        })
        .collect();
    Ok((carry, ups))
}

/// The reserves of the newest readable UPDATE under `prefix` below key `upper`.
fn newest_before(
    txn: &impl Txn,
    prefix: &[u8],
    upper: &[u8],
    b: &mut Budget,
) -> ApiResult<Option<Reserves>> {
    for row in txn
        .scan_rev_prefix(Table::DexEvents, prefix, upper)
        .map_err(api)?
    {
        if !b.tick() {
            break;
        }
        let (_, v) = row.map_err(api)?;
        if let Some(r) = StoredEvent::decode(&v)
            .ok()
            .and_then(|e| e.amount(0).zip(e.amount(1)))
        {
            return Ok(Some(r));
        }
    }
    b.check()?;
    Ok(None)
}

/// The reserves of the newest UPDATE in a block before `t_us` (a seek, not a walk).
fn reserves_before(
    txn: &impl Txn,
    key: &str,
    t_us: u64,
    b: &mut Budget,
) -> ApiResult<Option<Reserves>> {
    let prefix = hd::event_prefix(key, Kind::Update);
    let upper = seek_time(txn, &prefix, t_us, b)?;
    newest_before(txn, &prefix, &upper, b)
}

fn hour_of_secs(s: u64) -> u32 {
    u32::try_from(s / HOUR).unwrap_or(u32::MAX)
}

fn rates(txn: &impl Txn, from_h: u32, to_h: u32, b: &mut Budget) -> ApiResult<Rates> {
    let r = dex_usd::load(txn, from_h, to_h, &mut || b.tick()).map_err(api)?;
    b.check()?;
    Ok(r)
}

fn usd(kda: &Dec, rates: &Rates, hour: u32) -> Dec {
    rates
        .at(hour)
        .map(|r| kda.mul(&Dec::from_amount(r)))
        .unwrap_or_default()
}

/// A bucket's swap value in KDA (coin side in + out).
fn bucket_kda(bk: &Bucket, coin: Option<usize>) -> Dec {
    match coin {
        Some(c) => Dec::from_amount(&bk.sums[c]).add(&Dec::from_amount(&bk.sums[2 + c])),
        None => Dec::zero(),
    }
}

/// Stock's swap fee is `in × 0.003/0.997` on the input side; ours is taken on the coin side
/// (the swap's value), so a swap with KDA out differs by the price impact (allowlisted).
fn fee_of(v: &Dec) -> Dec {
    v.mul_int(3).div_int(997).unwrap_or_default()
}

fn change(cur: &Dec, prev: &Dec) -> f64 {
    match cur.sub(prev).mul_int(100).div(prev) {
        Some(c) => c.to_f64(),
        None => 0.0,
    }
}

// Pools.

/// A pool's stats at `now` (rounded as stock stores them).
#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub tvl: Dec,
    pub tvl_prev: Dec,
    pub vol24: Dec,
    pub vol24_prev: Dec,
    pub vol7d: Dec,
    pub fees24: Dec,
    pub fees24_prev: Dec,
    pub count24: i64,
    pub count24_prev: i64,
    pub apr: Dec,
}

/// The rates [`stats`] needs at `now` (load once per field).
pub fn stats_rates(txn: &impl Txn, now: u64, b: &mut Budget) -> ApiResult<Rates> {
    let th = hour_of_secs(now / DAY * DAY);
    rates(
        txn,
        th.saturating_sub(7 * 24 + 1),
        hour_of_secs(now).saturating_add(48),
        b,
    )
}

/// A pair's stats at `now`, from its hour buckets (by tx time, stock's) of the last 8 days and
/// two reserve reads; `rt` from [`stats_rates`].
pub fn stats(
    txn: &impl Txn,
    p: &PairInfo,
    now: u64,
    rt: &Rates,
    b: &mut Budget,
) -> ApiResult<Stats> {
    let today = now / DAY * DAY;
    let th = hour_of_secs(today);
    let from = th.saturating_sub(7 * 24);
    let mut s = Stats::default();
    for (h, bk) in buckets(txn, &p.key, from, u32::MAX, b)? {
        let v = usd(&bucket_kda(&bk, p.coin), rt, h);
        let f = fee_of(&v);
        let n = bk.swaps.wrapping_add(bk.adds).wrapping_add(bk.removes);
        if h >= th {
            s.vol24 = s.vol24.add(&v);
            s.fees24 = s.fees24.add(&f);
            s.count24 = s.count24.wrapping_add(n);
        } else {
            s.vol7d = s.vol7d.add(&v);
            if h >= th.saturating_sub(24) {
                s.vol24_prev = s.vol24_prev.add(&v);
                s.fees24_prev = s.fees24_prev.add(&f);
                s.count24_prev = s.count24_prev.wrapping_add(n);
            }
        }
    }
    s.tvl = usd(
        &p.tvl_kda(p.status.reserves.as_ref()),
        rt,
        hour_of_secs(now),
    )
    .round(8);
    let prev = reserves_before(txn, &p.key, today.saturating_mul(1_000_000), b)?;
    s.tvl_prev = usd(&p.tvl_kda(prev.as_ref()), rt, th.saturating_sub(1)).round(8);
    for x in [
        &mut s.vol24,
        &mut s.vol24_prev,
        &mut s.vol7d,
        &mut s.fees24,
        &mut s.fees24_prev,
    ] {
        *x = x.round(8);
    }
    s.apr = s
        .fees24
        .mul_int(365)
        .div(&s.tvl)
        .unwrap_or_default()
        .round(8);
    Ok(s)
}

fn token(name: &str) -> Token {
    let short = match name.split_once('.') {
        Some((_, rest)) => rest.split('.').next().unwrap_or(rest),
        None => name,
    };
    Token {
        id: ID(name.to_owned()),
        name: short.to_owned(),
        chain_id: hd::SUSHI_CHAIN.to_string(),
        address: Some(name.to_owned()),
    }
}

pub fn pool_node(txn: &impl Txn, p: &PairInfo, s: &Stats, args: TxArgs) -> ApiResult<Pool> {
    let i32of = |n: i64| i32::try_from(n).unwrap_or(i32::MAX);
    Ok(Pool {
        pool_id: ID(p.id()),
        address: DEFAULT_PROTOCOL.to_owned(),
        token0: token(&p.t0),
        token1: token(&p.t1),
        reserve0: p.reserve(0).text(),
        reserve1: p.reserve(1).text(),
        total_supply: supply(txn, &p.key)?.to_text(),
        key: p.key.clone(),
        tvl_usd: s.tvl.to_f64(),
        tvl_change24h: change(&s.tvl, &s.tvl_prev),
        volume24h_usd: s.vol24.to_f64(),
        volume_change24h: change(&s.vol24, &s.vol24_prev),
        volume7d_usd: s.vol7d.to_f64(),
        fees24h_usd: s.fees24.to_f64(),
        fees_change24h: change(&s.fees24, &s.fees24_prev),
        transaction_count24h: i32of(s.count24),
        transaction_count_change24h: change(
            &Dec::int(s.count24.max(0) as u64),
            &Dec::int(s.count24_prev.max(0) as u64),
        ),
        apr24h: s.apr.to_f64(),
        created_at: DateTime(p.created_us as i64),
        updated_at: DateTime(p.updated_us as i64),
        tx_args: args,
    })
}

/// `pool(id)`: stock's not-found text for an unknown id; a poisoned pair is unavailable.
pub fn pool(txn: &impl Txn, id: &str, now: u64, args: TxArgs, b: &mut Budget) -> ApiResult<Pool> {
    let not_found = || {
        bad(format!(
            "[ERROR][GRAPHQL][DB][DATA_MISSING] Token pair not found for pool {id}"
        ))
    };
    let key = pair_key_of_id(id).ok_or_else(not_found)?;
    let p = pair_info(txn, key)?.ok_or_else(not_found)?;
    if p.status.poisoned {
        return Err(unavailable(id));
    }
    let rt = stats_rates(txn, now, b)?;
    let s = stats(txn, &p, now, &rt, b)?;
    pool_node(txn, &p, &s, args)
}

// Stock paging (`pagination.ts`): cursors are base64 of an integer rank.

/// JS `parseInt(x) || 0` of a decoded cursor.
fn parse_int(c: &str) -> Option<u128> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(c.trim())
        .ok()
        .and_then(|b| String::from_utf8(b).ok())?;
    let t = raw.trim_start();
    let digits: String = t.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

fn enc(n: u128) -> String {
    base64::engine::general_purpose::STANDARD.encode(n.to_string())
}

fn check_n(n: Option<i32>) -> ApiResult<()> {
    match n {
        Some(x) if x < 0 => Err(bad("LIMIT must not be negative")),
        Some(x) if x > MAX_PAGE => Err(bad(format!("first and last may be at most {MAX_PAGE}"))),
        _ => Ok(()),
    }
}

/// Stock's `getPaginationParams` + `getPageInfo` over `items` sorted by rank descending
/// (default page `default`).
pub fn stock_page<T: Clone>(
    items: &[(u128, T)],
    after: Option<&str>,
    before: Option<&str>,
    first: Option<i32>,
    last: Option<i32>,
    default: usize,
) -> ApiResult<(Vec<(String, T)>, PageInfo)> {
    check_n(first)?;
    check_n(last)?;
    let n = |x: Option<i32>| x.filter(|v| *v > 0).map_or(default, |v| v as usize);
    let (limit, desc, rows): (usize, bool, Vec<&(u128, T)>) = if let Some(a) = after {
        let a = parse_int(a).unwrap_or(0);
        let l = n(first) + 1;
        (l, true, items.iter().filter(|r| r.0 < a).take(l).collect())
    } else if let Some(bf) = before {
        let bf = parse_int(bf).unwrap_or(0);
        let l = n(last) + 1;
        (
            l,
            false,
            items.iter().rev().filter(|r| r.0 > bf).take(l).collect(),
        )
    } else if first.is_some_and(|f| f > 0) || last.is_none_or(|l| l <= 0) {
        let l = n(first) + 1;
        (l, true, items.iter().take(l).collect())
    } else {
        let l = n(last) + 1;
        (l, false, items.iter().rev().take(l).collect())
    };
    let shown = limit - 1;
    let idx = rows.len().min(shown);
    let mut page: Vec<(String, T)> = rows[..idx]
        .iter()
        .map(|r| (enc(r.0), r.1.clone()))
        .collect();
    let (has_next, has_prev) = if desc {
        (rows.len() > shown, after.is_some())
    } else {
        page.reverse();
        (before.is_some(), rows.len() > shown)
    };
    let info = PageInfo {
        start_cursor: page.first().map(|e| e.0.clone()),
        end_cursor: page.last().map(|e| e.0.clone()),
        has_next_page: has_next && !page.is_empty(),
        has_previous_page: has_prev && !page.is_empty(),
    };
    Ok((page, info))
}

fn order_key(o: PoolOrderBy, s: &Stats) -> (Dec, bool) {
    use PoolOrderBy::*;
    match o {
        TvlUsdAsc => (s.tvl.clone(), false),
        TvlUsdDesc => (s.tvl.clone(), true),
        Volume24hAsc => (s.vol24.clone(), false),
        Volume24hDesc => (s.vol24.clone(), true),
        Volume7dAsc => (s.vol7d.clone(), false),
        Volume7dDesc => (s.vol7d.clone(), true),
        Apr24hAsc => (s.apr.clone(), false),
        Apr24hDesc => (s.apr.clone(), true),
        TransactionCount24hAsc => (Dec::int(s.count24.max(0) as u64), false),
        TransactionCount24hDesc => (Dec::int(s.count24.max(0) as u64), true),
    }
}

/// `pools`: listable Sushi pairs, sorted correctly (divergence 2), stock's cursors.
#[allow(clippy::too_many_arguments)]
pub fn pools(
    txn: &impl Txn,
    now: u64,
    order: PoolOrderBy,
    protocol: &str,
    after: Option<&str>,
    before: Option<&str>,
    first: Option<i32>,
    last: Option<i32>,
    b: &mut Budget,
) -> ApiResult<QueryPoolsConnection> {
    let mut rows: Vec<(Dec, String, Pool)> = Vec::new();
    if protocol == DEFAULT_PROTOCOL {
        let pairs = listable(txn, b)?;
        let rt = stats_rates(txn, now, b)?;
        for p in pairs {
            b.check()?;
            let s = stats(txn, &p, now, &rt, b)?;
            let (k, _) = order_key(order, &s);
            rows.push((k, p.id(), pool_node(txn, &p, &s, TxArgs::default())?));
        }
    }
    let desc = order_key(order, &Stats::default()).1;
    rows.sort_by(|a, b| {
        let o = a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1));
        if desc {
            o.reverse()
        } else {
            o
        }
    });
    let n = rows.len() as u128;
    let items: Vec<(u128, Pool)> = rows
        .into_iter()
        .enumerate()
        .map(|(i, r)| (n - i as u128, r.2))
        .collect();
    let (page, info) = stock_page(&items, after, before, first, last, 20)?;
    Ok(QueryPoolsConnection {
        total_count: items.len() as i32,
        edges: page
            .into_iter()
            .map(|(cursor, node)| QueryPoolsConnectionEdge { cursor, node })
            .collect(),
        page_info: info,
    })
}

// Pool transactions.

/// The pool-transaction arguments `pool(...)` passes down to `Pool.transactions`.
#[derive(Debug, Clone, Default)]
pub struct TxArgs {
    pub ty: Option<PoolTransactionType>,
    pub first: Option<i32>,
    pub after: Option<String>,
    pub last: Option<i32>,
    pub before: Option<String>,
}

fn kinds(ty: Option<PoolTransactionType>) -> Vec<Kind> {
    match ty {
        Some(PoolTransactionType::Swap) => vec![Kind::Swap],
        Some(PoolTransactionType::AddLiquidity) => vec![Kind::AddLiquidity],
        Some(PoolTransactionType::RemoveLiquidity) => vec![Kind::RemoveLiquidity],
        None => vec![Kind::Swap, Kind::AddLiquidity, Kind::RemoveLiquidity],
    }
}

/// An event id: `TxKey ‖ event index` as one integer (chain order).
fn ev_id(ev: &[u8]) -> Option<u128> {
    let a: [u8; 10] = ev.try_into().ok()?;
    let mut b = [0u8; 16];
    b[6..].copy_from_slice(&a);
    Some(u128::from_be_bytes(b))
}

fn ev_bytes(id: u128) -> [u8; 10] {
    let b = id.min((1u128 << 80) - 1).to_be_bytes();
    b[6..].try_into().unwrap_or([0; 10])
}

/// An event's time in seconds: its tx's `meta.creationTime` (stock's), else its block's.
fn event_secs(e: &StoredEvent) -> u64 {
    if e.tx_time > 0 {
        e.tx_time
    } else {
        e.time / 1_000_000
    }
}

fn tx_node(
    view: &View,
    pool_id: &str,
    id: u128,
    e: &StoredEvent,
    coin: Option<usize>,
    rt: &Rates,
) -> ApiResult<PoolTransaction> {
    // Stored amounts passed `Amount::parse_stored` at ingest; one that no longer reads is a
    // damaged store, reported as such (never a silent 0).
    let amt = |i: usize| -> ApiResult<Amount> { e.amount(i).ok_or_else(|| unavailable(pool_id)) };
    let zero = Amount::zero;
    let (a0i, a1i, a0o, a1o, ty) = match e.kind {
        Kind::Swap => (
            amt(2)?,
            amt(3)?,
            amt(4)?,
            amt(5)?,
            PoolTransactionType::Swap,
        ),
        Kind::AddLiquidity => (
            amt(2)?,
            amt(3)?,
            zero(),
            zero(),
            PoolTransactionType::AddLiquidity,
        ),
        _ => (
            zero(),
            zero(),
            amt(2)?,
            amt(3)?,
            PoolTransactionType::RemoveLiquidity,
        ),
    };
    let kda = coin.map(|c| match e.kind {
        Kind::Swap => {
            let i = if c == 0 { &a0i } else { &a1i };
            let o = if c == 0 { &a0o } else { &a1o };
            Dec::from_amount(if i.is_zero() { o } else { i })
        }
        Kind::AddLiquidity => Dec::from_amount(if c == 0 { &a0i } else { &a1i }).mul_int(2),
        _ => Dec::from_amount(if c == 0 { &a0o } else { &a1o }).mul_int(2),
    });
    let secs = event_secs(e);
    let amount_usd = usd(&kda.unwrap_or_default(), rt, hour_of_secs(secs))
        .round(8)
        .fixed(10);
    let key = TxKey((id >> 16) as u64);
    let requestkey = view
        .tx(key)
        .ok()
        .flatten()
        .and_then(|r| r.request_key)
        .map(|h| h.to_b64url())
        .unwrap_or_default();
    let dec = |a: &Amount| Decimal::verbatim(a.to_text());
    Ok(PoolTransaction {
        id: ID(id.to_string()),
        maker: e.fields.first().cloned().unwrap_or_default(),
        amount0_in: dec(&a0i),
        amount1_in: dec(&a1i),
        amount0_out: dec(&a0o),
        amount1_out: dec(&a1o),
        amount_usd: Decimal::verbatim(amount_usd),
        timestamp: DateTime(i64::try_from(secs.saturating_mul(1_000_000)).unwrap_or(i64::MAX)),
        transaction_id: None,
        requestkey,
        transaction_type: ty,
    })
}

/// `poolTransactions` / `Pool.transactions`: stock's id paging (`first` newest first,
/// `last` oldest first; `after`/`before` are ids), default page 10.
pub fn pool_transactions(
    view: &View,
    pair_id: &str,
    a: &TxArgs,
    b: &mut Budget,
) -> ApiResult<PoolTransactionsConnection> {
    let txn = &view.txn;
    check_n(a.first)?;
    check_n(a.last)?;
    let nan = || bad("invalid input syntax for type integer: \"NaN\"");
    let after = a
        .after
        .as_deref()
        .map(|c| parse_int(c).ok_or_else(nan))
        .transpose()?;
    let before = a
        .before
        .as_deref()
        .map(|c| parse_int(c).ok_or_else(nan))
        .transpose()?;
    let empty = PoolTransactionsConnection {
        edges: Some(Vec::new()),
        page_info: Some(PageInfo {
            end_cursor: None,
            has_next_page: false,
            has_previous_page: false,
            start_cursor: None,
        }),
        total_count: Some(0),
    };
    let Some(key) = pair_key_of_id(pair_id) else {
        return Ok(empty);
    };
    let Some(p) = pair_info(txn, key)? else {
        return Ok(empty);
    };
    if p.status.poisoned {
        return Err(unavailable(pair_id));
    }
    let limit = a
        .first
        .filter(|n| *n > 0)
        .or(a.last.filter(|n| *n > 0))
        .unwrap_or(10) as usize;
    let desc = a.last.is_none_or(|n| n <= 0) || a.first.is_some_and(|n| n > 0);
    let mut got: Vec<(u128, StoredEvent)> = Vec::new();
    let mut total = 0i64;
    for k in kinds(a.ty) {
        // The stored per-kind count (exact under apply and unwind), not a walk.
        let n = hd::event_count(txn, key, k)
            .map_err(api)?
            .ok_or_else(|| unavailable(pair_id))?;
        total = total.saturating_add(n.max(0));
        let prefix = hd::event_prefix(key, k);
        let ok = |id: u128| {
            let lo = if desc { before } else { after };
            let hi = if desc { after } else { before };
            lo.is_none_or(|x| id > x) && hi.is_none_or(|x| id < x)
        };
        let mut taken = 0usize;
        if desc {
            let mut upper = Vec::new();
            if let Some(x) = after {
                upper = prefix.clone();
                upper.extend_from_slice(&ev_bytes(x));
            }
            for row in txn
                .scan_rev_prefix(Table::DexEvents, &prefix, &upper)
                .map_err(api)?
            {
                if !b.tick() || taken > limit {
                    break;
                }
                let (kk, v) = row.map_err(api)?;
                let Some(id) = ev_id(&kk[prefix.len()..]) else {
                    continue;
                };
                if !ok(id) {
                    if before.is_some_and(|x| id <= x) {
                        break;
                    }
                    continue;
                }
                if let Ok(e) = StoredEvent::decode(&v) {
                    got.push((id, e));
                    taken += 1;
                }
            }
        } else {
            let mut from = prefix.clone();
            if let Some(x) = after {
                from.extend_from_slice(&ev_bytes(x));
            }
            let rows = txn
                .scan(Table::DexEvents, &from, |kk| {
                    if !b.tick() || !kk.starts_with(&prefix) || taken > limit {
                        return false;
                    }
                    match ev_id(&kk[prefix.len()..]) {
                        Some(id) if before.is_some_and(|x| id >= x) => false,
                        Some(id) if ok(id) => {
                            taken += 1;
                            true
                        }
                        _ => true,
                    }
                })
                .map_err(api)?;
            for (kk, v) in rows {
                let Some(id) = ev_id(&kk[prefix.len()..]) else {
                    continue;
                };
                if ok(id) {
                    if let Ok(e) = StoredEvent::decode(&v) {
                        got.push((id, e));
                    }
                }
            }
        }
        b.check()?;
    }
    got.sort_by(|x, y| if desc { y.0.cmp(&x.0) } else { x.0.cmp(&y.0) });
    let more = got.len() > limit;
    got.truncate(limit);
    let (lo, hi) = got
        .iter()
        .map(|g| hour_of_secs(event_secs(&g.1)))
        .fold((u32::MAX, 0u32), |(l, h), x| (l.min(x), h.max(x)));
    let rt = if got.is_empty() {
        Rates::default()
    } else {
        rates(txn, lo, hi, b)?
    };
    let edges = got
        .iter()
        .map(|(id, e)| {
            Ok(PoolTransactionEdge {
                cursor: enc(*id),
                node: tx_node(view, pair_id, *id, e, p.coin, &rt)?,
            })
        })
        .collect::<ApiResult<Vec<PoolTransactionEdge>>>()?;
    let paged = a.after.is_some() || a.before.is_some();
    Ok(PoolTransactionsConnection {
        page_info: Some(PageInfo {
            start_cursor: edges.first().map(|e| e.cursor.clone()),
            end_cursor: edges.last().map(|e| e.cursor.clone()),
            has_next_page: more,
            has_previous_page: paged && !edges.is_empty(),
        }),
        edges: Some(edges),
        total_count: Some(i32::try_from(total).unwrap_or(i32::MAX)),
    })
}

// Charts and metrics.

fn day_of_us(t: u64) -> u64 {
    t / 1_000_000 / DAY * DAY
}

/// Per-day TVL (USD, rounded 8) at the end of each day in `from_day..=to_day` (unix s), from
/// the pair's UPDATEs of that range only (a seek, then the range's rows) and the reserves
/// before it; days before the pair's first UPDATE have none.
fn tvl_days(
    txn: &impl Txn,
    p: &PairInfo,
    from_day: u64,
    to_day: u64,
    now: u64,
    rt: &Rates,
    b: &mut Budget,
) -> ApiResult<Vec<(u64, Dec)>> {
    let us = |s: u64| s.saturating_mul(1_000_000);
    let (mut cur, ups) = updates(txn, &p.key, us(from_day), us(to_day.saturating_add(DAY)), b)?;
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut d = from_day;
    while d <= to_day {
        let end_us = us(d + DAY);
        while i < ups.len() && ups[i].0 < end_us {
            cur = Some(ups[i].1.clone());
            i += 1;
        }
        if cur.is_some() {
            let at = (d + DAY - 1).min(now);
            out.push((
                d,
                usd(&p.tvl_kda(cur.as_ref()), rt, hour_of_secs(at)).round(8),
            ));
        }
        d += DAY;
    }
    b.check()?;
    Ok(out)
}

fn point(t_secs: u64, v: Decimal) -> ChartDataPoint {
    ChartDataPoint {
        timestamp: DateTime((t_secs as i64).saturating_mul(1_000_000)),
        value: v,
    }
}

/// `Pool.charts(timeFrame)`.
pub fn charts(
    txn: &impl Txn,
    pool_id: &str,
    tf: TimeFrame,
    now: u64,
    b: &mut Budget,
) -> ApiResult<PoolCharts> {
    let key = pair_key_of_id(pool_id).ok_or_else(|| unavailable(pool_id))?;
    let p = pair_info(txn, key)?.ok_or_else(|| unavailable(pool_id))?;
    if p.status.poisoned {
        return Err(unavailable(pool_id));
    }
    let today = now / DAY * DAY;
    let start = match tf {
        TimeFrame::Day => today.saturating_sub(DAY),
        TimeFrame::Week => today.saturating_sub(7 * DAY),
        TimeFrame::Month => today.saturating_sub(30 * DAY),
        TimeFrame::Year => today.saturating_sub(365 * DAY),
        TimeFrame::All => day_of_us(p.created_us),
    };
    let rt = rates(
        txn,
        hour_of_secs(start).saturating_sub(1),
        hour_of_secs(now),
        b,
    )?;
    let hourly = tf == TimeFrame::Day;
    let mut vol: Vec<(u64, Dec, Dec)> = Vec::new();
    for (h, bk) in buckets(txn, key, hour_of_secs(start), hour_of_secs(now), b)? {
        if bk.swaps == 0 {
            continue;
        }
        let v = usd(&bucket_kda(&bk, p.coin), &rt, h);
        let t = if hourly {
            u64::from(h) * HOUR
        } else {
            u64::from(h) * HOUR / DAY * DAY
        };
        match vol.last_mut() {
            Some(l) if l.0 == t => {
                l.2 = l.2.add(&fee_of(&v));
                l.1 = l.1.add(&v);
            }
            _ => vol.push((t, v.clone(), fee_of(&v))),
        }
    }
    let tvl = if hourly {
        // Stock: the newest TVL point (its own time), whatever its day.
        let e = first_time(txn, &hd::event_prefix(key, Kind::Update), true)?;
        e.map(|e| {
            let r = e.amount(0).zip(e.amount(1));
            let t = event_secs(&e);
            let v = usd(&p.tvl_kda(r.as_ref()), &rt, hour_of_secs(t)).round(8);
            vec![point(t, Decimal::verbatim(v.fixed(8)))]
        })
        .unwrap_or_default()
    } else {
        let from = start.max(day_of_us(p.created_us));
        tvl_days(txn, &p, from, today, now, &rt, b)?
            .into_iter()
            .map(|(d, v)| point(d, Decimal::verbatim(v.fixed(8))))
            .collect()
    };
    Ok(PoolCharts {
        volume: vol
            .iter()
            .map(|(t, v, _)| point(*t, Decimal::verbatim(v.round(8).fixed(10))))
            .collect(),
        fees: vol
            .iter()
            .map(|(t, _, f)| point(*t, Decimal::verbatim(f.round(8).fixed(10))))
            .collect(),
        tvl,
    })
}

fn num(d: &Dec) -> Decimal {
    Decimal::new(&d.text()).unwrap_or_else(|| Decimal::verbatim("0".into()))
}

/// `dexMetrics(startDate, endDate, protocolAddress)` (dates in unix seconds).
pub fn dex_metrics(
    txn: &impl Txn,
    now: u64,
    start: Option<u64>,
    end: Option<u64>,
    protocol: &str,
    b: &mut Budget,
) -> ApiResult<DexMetrics> {
    let start = start.unwrap_or(now.saturating_sub(30 * DAY));
    let end = end.unwrap_or(now);
    if end > start && (end - start) / DAY > MAX_METRIC_DAYS {
        return Err(bad(format!(
            "dexMetrics: the date range may span at most {MAX_METRIC_DAYS} days"
        )));
    }
    let mut m = DexMetrics {
        total_pools: 0,
        current_tvl_usd: num(&Dec::zero()),
        tvl_history: Vec::new(),
        volume_history: Vec::new(),
        total_volume_usd: num(&Dec::zero()),
    };
    if protocol != DEFAULT_PROTOCOL {
        return Ok(m);
    }
    let pairs = listable(txn, b)?;
    m.total_pools = pairs.len() as i32;
    let lo_h = hour_of_secs(start.min(end) / DAY * DAY).saturating_sub(1);
    let rt = rates(txn, lo_h, hour_of_secs(now.max(end)), b)?;
    let mut cur = Dec::zero();
    let mut tvl: std::collections::BTreeMap<u64, Dec> = Default::default();
    let mut vol: std::collections::BTreeMap<u64, Dec> = Default::default();
    // Stock's day series: start, start + 1 day, … ≤ end, each truncated to its day.
    let days: Vec<u64> = (0..=MAX_METRIC_DAYS)
        .map(|k| start + k * DAY)
        .take_while(|t| *t <= end)
        .map(|t| t / DAY * DAY)
        .collect();
    for p in &pairs {
        b.check()?;
        cur = cur.add(
            &usd(
                &p.tvl_kda(p.status.reserves.as_ref()),
                &rt,
                hour_of_secs(now),
            )
            .round(8),
        );
        if let (Some(f), Some(l)) = (days.first(), days.last()) {
            for (d, v) in tvl_days(txn, p, *f, *l, now, &rt, b)? {
                if days.binary_search(&d).is_ok() {
                    let e = tvl.entry(d).or_default();
                    *e = e.add(&v);
                }
            }
        }
        for (h, bk) in buckets(txn, &p.key, hour_of_secs(start), hour_of_secs(end), b)? {
            if bk.swaps == 0 || u64::from(h) * HOUR < start {
                continue;
            }
            let v = usd(&bucket_kda(&bk, p.coin), &rt, h);
            let e = vol.entry(u64::from(h) * HOUR / DAY * DAY).or_default();
            *e = e.add(&v);
        }
    }
    b.check()?;
    let mut total = Dec::zero();
    m.volume_history = vol
        .iter()
        .map(|(d, v)| {
            let v = v.round(8);
            total = total.add(&v);
            point(*d, num(&v))
        })
        .collect();
    m.tvl_history = tvl.iter().map(|(d, v)| point(*d, num(v))).collect();
    m.current_tvl_usd = num(&cur);
    m.total_volume_usd = num(&total);
    Ok(m)
}

// Liquidity positions.

fn lp_order(o: LiquidityPositionOrderBy, v: &Dec, liq: &Dec, apr: &Dec) -> (Dec, bool) {
    use LiquidityPositionOrderBy::*;
    match o {
        ValueUsdAsc => (v.clone(), false),
        ValueUsdDesc => (v.clone(), true),
        LiquidityAsc => (liq.clone(), false),
        LiquidityDesc => (liq.clone(), true),
        AprAsc => (apr.clone(), false),
        AprDesc => (apr.clone(), true),
    }
}

/// `liquidityPositions(walletAddress)`: the wallet's non-zero LP balances in listable pairs.
#[allow(clippy::too_many_arguments)]
pub fn liquidity_positions(
    txn: &impl Txn,
    now: u64,
    wallet: &str,
    order: LiquidityPositionOrderBy,
    after: Option<&str>,
    before: Option<&str>,
    first: Option<i32>,
    last: Option<i32>,
    b: &mut Budget,
) -> ApiResult<LiquidityPositionsConnection> {
    // One point read per listable pair (no walk of every LP balance).
    let mut rows: Vec<(Dec, String, LiquidityPosition)> = Vec::new();
    if !wallet.is_empty() && wallet.len() <= hd::MAX_ACCOUNT_LEN {
        let pairs = listable(txn, b)?;
        let mut rt = None;
        for p in pairs {
            b.check()?;
            let v = txn
                .get(Table::DexLp, &hd::lp_key(&p.key, wallet))
                .map_err(api)?;
            let Some(liq) = stored_amount(v, &p.key)? else {
                continue;
            };
            let liq_d = Dec::from_amount(&liq);
            if liq_d.is_zero() {
                continue;
            }
            if rt.is_none() {
                rt = Some(stats_rates(txn, now, b)?);
            }
            let s = stats(txn, &p, now, rt.as_ref().unwrap_or(&Rates::default()), b)?;
            let sup = Dec::from_amount(&supply(txn, &p.key)?);
            let value = liq_d
                .div(&sup)
                .map(|r| r.mul(&s.tvl))
                .unwrap_or_default()
                .round(20);
            let (k, _) = lp_order(order, &value, &liq_d, &s.apr);
            let pool = pool_node(txn, &p, &s, TxArgs::default())?;
            rows.push((
                k,
                p.id(),
                LiquidityPosition {
                    id: ID(format!("{}:{wallet}", p.id())),
                    pair_id: p.id(),
                    liquidity: liq_d.text(),
                    wallet_address: wallet.to_owned(),
                    value_usd: num(&value),
                    apr24h: num(&s.apr),
                    pair: pool,
                    created_at: DateTime(p.created_us as i64),
                    updated_at: DateTime(p.updated_us as i64),
                },
            ));
        }
    }
    let desc = lp_order(order, &Dec::zero(), &Dec::zero(), &Dec::zero()).1;
    rows.sort_by(|a, b| {
        let o = a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1));
        if desc {
            o.reverse()
        } else {
            o
        }
    });
    let n = rows.len() as u128;
    let items: Vec<(u128, LiquidityPosition)> = rows
        .into_iter()
        .enumerate()
        .map(|(i, r)| (n - i as u128, r.2))
        .collect();
    let first = if first.is_none() && last.is_none() {
        Some(10)
    } else {
        first
    };
    let (page, info) = stock_page(&items, after, before, first, last, 10)?;
    Ok(LiquidityPositionsConnection {
        total_count: items.len() as i32,
        edges: page
            .into_iter()
            .map(|(cursor, node)| LiquidityPositionEdge { cursor, node })
            .collect(),
        page_info: info,
    })
}

// Token prices.

/// A price search: pair statuses read once per field.
struct PriceSearch<'a, T: Txn> {
    txn: &'a T,
    pairs: HashMap<String, PairStatus>,
}

impl<T: Txn> PriceSearch<'_, T> {
    fn status(&mut self, pair: &str, b: &mut Budget) -> ApiResult<&PairStatus> {
        if !self.pairs.contains_key(pair) {
            b.tick();
            let st = hd::pair_status(self.txn, pair).map_err(api)?;
            self.pairs.insert(pair.to_owned(), st);
        }
        b.check()?;
        Ok(&self.pairs[pair])
    }

    /// Whether `token` is in a stored pair (stock: a token row exists).
    fn known(&self, token: &str) -> ApiResult<bool> {
        let mut any = false;
        hd::token_pairs(self.txn, token, |_| {
            any = true;
            false
        })
        .map_err(api)?;
        Ok(any)
    }

    /// Reserves of `pair` as (this token's, the other's), when created and not poisoned.
    fn sides(
        &mut self,
        pair: &str,
        token: &str,
        b: &mut Budget,
    ) -> ApiResult<Option<Option<(Dec, Dec)>>> {
        let st = self.status(pair, b)?;
        if !st.created || st.poisoned {
            return Ok(None);
        }
        let first = pair.split(':').next() == Some(token);
        Ok(Some(st.reserves.as_ref().map(|(r0, r1)| {
            let (a, c) = (Dec::from_amount(r0), Dec::from_amount(r1));
            if first {
                (a, c)
            } else {
                (c, a)
            }
        })))
    }

    /// Stock's `findOptimalKdaPricePath`: BFS from `token`; at each token a direct `coin`
    /// pair gives the answer (0 when its reserves are zero or unknown, as stock), else the
    /// amount moves to each neighbour at the reserve ratio, at most [`MAX_HOPS`] hops out.
    /// Neighbours: up to [`MAX_RANKED_NEIGHBORS`] of the token's pairs from the token index,
    /// ranked by the token's own reserve (largest first, then pair key); the top
    /// [`MAX_NEIGHBORS`] are expanded, and at most [`MAX_PRICE_TOKENS`] tokens are visited.
    /// (Stock walks its pairs in id order; ranking keeps dust spam from hiding real routes.)
    ///
    /// Limit: the token index lists a token's pairs oldest `CREATE_PAIR` first, and only the
    /// first [`MAX_RANKED_NEIGHBORS`] are ranked. A token with more pairs than that has its
    /// newer pairs left out of the search, so spam pairs created before a real route can still
    /// hide it (then the token prices at 0, as stock prices a token with no path).
    ///
    /// A search that spends the field's row budget or deadline answers no path (`None`: price
    /// 0, stock's answer with no path) rather than an error.
    fn price_in_kda(&mut self, token: &str, b: &mut Budget) -> ApiResult<Option<Dec>> {
        match self.search(token, b) {
            Err(ApiError::Deadline) => Ok(None),
            r => r,
        }
    }

    fn search(&mut self, token: &str, b: &mut Budget) -> ApiResult<Option<Dec>> {
        if token == "coin" {
            return Ok(Some(Dec::int(1)));
        }
        let mut q = VecDeque::from([(token.to_owned(), Dec::int(1), 0usize)]);
        let mut seen = HashSet::from([token.to_owned()]);
        while let Some((t, amt, depth)) = q.pop_front() {
            if let Some((direct, _)) = hd::pair_of(&t, "coin") {
                if let Some(r) = self.sides(&direct, &t, b)? {
                    let kda = r
                        .and_then(|(rt, rc)| amt.mul(&rc).div(&rt))
                        .unwrap_or_default();
                    return Ok(Some(kda));
                }
            }
            if depth >= MAX_HOPS {
                continue;
            }
            let mut names = HashSet::new();
            hd::token_pairs(self.txn, &t, |pair| {
                if b.tick() && names.len() < MAX_RANKED_NEIGHBORS {
                    names.insert(pair.to_owned());
                    true
                } else {
                    false
                }
            })
            .map_err(api)?;
            b.check()?;
            // Rank by this token's own reserve (one read each), so dust pairs spammed onto a
            // token cannot crowd its real pairs out of the expansion; ties by pair key.
            let mut ranked: Vec<(Dec, String)> = Vec::with_capacity(names.len());
            for pair in names {
                if !b.tick() {
                    break;
                }
                // A coin pair never funded has no reserves to rank: skip its UPDATE read (the
                // funded count is kept for coin pairs only; other pairs are read).
                if pair.split(':').any(|x| x == "coin")
                    && self
                        .txn
                        .get(Table::DexAgg, &hd::funded_key(&pair))
                        .map_err(api)?
                        .is_none()
                {
                    continue;
                }
                let Some((r0, r1)) = hd::latest_reserves(self.txn, &pair).map_err(api)? else {
                    continue;
                };
                let own = if pair.split(':').next() == Some(t.as_str()) {
                    r0
                } else {
                    r1
                };
                if !own.is_zero() {
                    ranked.push((Dec::from_amount(&own), pair));
                }
            }
            b.check()?;
            ranked.sort_by(|x, y| y.0.cmp(&x.0).then_with(|| x.1.cmp(&y.1)));
            ranked.truncate(MAX_NEIGHBORS);
            for (_, pair) in ranked {
                let Some(other) = pair.split(':').find(|x| *x != t).map(str::to_owned) else {
                    continue;
                };
                if seen.contains(&other) {
                    continue;
                }
                let Some(Some((rs, ro))) = self.sides(&pair, &t, b)? else {
                    continue;
                };
                if let Some(x) = amt.mul(&ro).div(&rs) {
                    if seen.len() >= MAX_PRICE_TOKENS {
                        return Ok(None);
                    }
                    seen.insert(other.clone());
                    q.push_back((other, x, depth + 1));
                }
            }
        }
        Ok(None)
    }
}

fn price_node(
    kda: Option<Dec>,
    name: &str,
    protocol: &str,
    rate: Option<&Amount>,
    now: u64,
) -> TokenPrice {
    // Stock answers 0/0 when it has no KDA/USD price or no path.
    let (k, u) = match (kda, rate) {
        (Some(k), Some(r)) => {
            let u = k.mul(&Dec::from_amount(r));
            (k, u)
        }
        _ => (Dec::zero(), Dec::zero()),
    };
    TokenPrice {
        id: ID(format!("price-{name}")),
        token: token(name),
        price_in_kda: num(&k.round(24)),
        price_in_usd: num(&u.round(24)),
        protocol_address: protocol.to_owned(),
        updated_at: DateTime((now as i64).saturating_mul(1_000_000)),
    }
}

/// `tokenPrice`: null for a token in no stored pair. A bounded search from the token (a
/// direct `coin` pair is one read).
pub fn token_price(
    txn: &impl Txn,
    now: u64,
    addr: &str,
    protocol: &str,
    b: &mut Budget,
) -> ApiResult<Option<TokenPrice>> {
    let mut ps = PriceSearch {
        txn,
        pairs: HashMap::new(),
    };
    if addr.is_empty() || addr.len() > hd::MAX_ACCOUNT_LEN || !ps.known(addr)? {
        return Ok(None);
    }
    let kda = if protocol == DEFAULT_PROTOCOL {
        ps.price_in_kda(addr, b)?
    } else {
        None
    };
    let rate = dex_usd::latest(txn).map_err(api)?.map(|r| r.1);
    Ok(Some(price_node(kda, addr, protocol, rate.as_ref(), now)))
}

/// `tokenPrices`: every token of a listable pair (sorted by address; stock's order is the
/// database's).
pub fn token_prices(
    txn: &impl Txn,
    now: u64,
    protocol: &str,
    b: &mut Budget,
) -> ApiResult<Vec<TokenPrice>> {
    if protocol != DEFAULT_PROTOCOL {
        return Ok(Vec::new());
    }
    let pairs = listable(txn, b)?;
    let rate = dex_usd::latest(txn).map_err(api)?.map(|r| r.1);
    let mut names: Vec<&str> = pairs
        .iter()
        .flat_map(|p| [p.t0.as_str(), p.t1.as_str()])
        .collect();
    names.sort_unstable();
    names.dedup();
    let mut ps = PriceSearch {
        txn,
        pairs: HashMap::new(),
    };
    let mut out = Vec::with_capacity(names.len());
    for n in names {
        let kda = ps.price_in_kda(n, b)?;
        out.push(price_node(kda, n, protocol, rate.as_ref(), now));
    }
    Ok(out)
}

// Tokens.

/// `tokens`: stock's set and order (`balance-db-repository.ts`): one node per (non-coin
/// module, chain) with a holder row (`kdx_hot::token_chains`, built from `Holders`, stock's
/// `Balances` set), newest first holder row first (stock: `MIN(Balances.id)`, its insert
/// order); cursor = that row's TxKey ‖ module id ‖ chain (stock: the row id itself, an
/// internal number we cannot reproduce). The caller checks `token_chains::ready`.
pub fn tokens(
    view: &View,
    after: Option<&str>,
    before: Option<&str>,
    first: Option<i32>,
    last: Option<i32>,
    b: &mut Budget,
) -> ApiResult<QueryTokensConnection> {
    let txn = &view.txn;
    let mut items: Vec<(u128, Token)> = Vec::new();
    {
        let dict = view.dict();
        let rows = kdx_hot::token_chains::read_all(txn, || b.tick()).map_err(api)?;
        b.check()?;
        for (id, row) in rows {
            let Some(name) = dict.name(DictKind::Module, id) else {
                continue;
            };
            if name == "coin" {
                continue;
            }
            for (&chain, first) in &row.first {
                let rank = (u128::from(first.0) << 40) | (u128::from(id) << 8) | u128::from(chain);
                let tid = base64::engine::general_purpose::STANDARD
                    .encode(format!("Token:[{name},{chain}]"));
                items.push((
                    rank,
                    Token {
                        id: ID(tid),
                        name: name.to_owned(),
                        chain_id: chain.to_string(),
                        address: None,
                    },
                ));
            }
        }
    }
    items.sort_by_key(|i| std::cmp::Reverse(i.0));
    let (page, info) = stock_page(&items, after, before, first, last, 20)?;
    Ok(QueryTokensConnection {
        edges: page
            .into_iter()
            .map(|(cursor, node)| QueryTokensEdge { cursor, node })
            .collect(),
        page_info: info,
    })
}
