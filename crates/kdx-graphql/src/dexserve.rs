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
//!     hardcoded `"0"`), hour buckets use the block's time (stock the sender's), and
//!     `tokens` ranks modules by first height (stock by a balance row id).
//!
//! Cost: every list is bounded by [`MAX_PAIRS`] pairs and a [`Budget`] of rows checked
//! against the request deadline inside every loop; aggregates over long ranges run on a
//! heavy slot (resolvers).

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
/// Distinct pairs one field reads (CREATE_PAIR spam beyond it is not listed).
pub const MAX_PAIRS: usize = 10_000;
/// Rows one field reads.
pub const MAX_ROWS: u64 = 2_000_000;
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
    out: bool,
}

impl Budget {
    pub fn new(dl: Instant) -> Budget {
        Budget {
            dl,
            rows: 0,
            out: false,
        }
    }

    /// Count a row; false once the budget or the deadline is spent.
    pub fn tick(&mut self) -> bool {
        self.rows += 1;
        if self.rows > MAX_ROWS || (self.rows.is_multiple_of(64) && Instant::now() >= self.dl) {
            self.out = true;
        }
        !self.out
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

/// Distinct pair keys with a stored `CREATE_PAIR` (key order), at most [`MAX_PAIRS`].
pub fn pair_keys(txn: &impl Txn, b: &mut Budget) -> ApiResult<Vec<String>> {
    let mut keys: Vec<String> = Vec::new();
    if !txn.has_table(Table::DexPairs) {
        return Ok(keys);
    }
    txn.walk_keys(Table::DexPairs, &[], |k| {
        if !b.tick() || keys.len() >= MAX_PAIRS {
            return false;
        }
        if let Some(p) = k.iter().position(|c| *c == 0) {
            let name = String::from_utf8_lossy(&k[..p]);
            if keys.last().map(String::as_str) != Some(&*name) {
                keys.push(name.into_owned());
            }
        }
        true
    })
    .map_err(api)?;
    b.check()?;
    Ok(keys)
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
/// reserves) in key order.
pub fn listable(txn: &impl Txn, b: &mut Budget) -> ApiResult<Vec<PairInfo>> {
    let mut out = Vec::new();
    for k in pair_keys(txn, b)? {
        if !b.tick() {
            break;
        }
        if let Some(p) = pair_info(txn, &k)? {
            if p.status.listable() {
                out.push(p);
            }
        }
    }
    b.check()?;
    Ok(out)
}

fn supply(txn: &impl Txn, key: &str) -> ApiResult<String> {
    Ok(txn
        .get(Table::DexAgg, &hd::supply_key(key))
        .map_err(api)?
        .and_then(|v| String::from_utf8(v).ok())
        .filter(|t| Amount::parse_stored(t).is_some())
        .unwrap_or_else(|| "0".into()))
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

/// Every UPDATE of a pair from `from_us` on, as (block time µs, tx time s, reserves).
fn updates(
    txn: &impl Txn,
    key: &str,
    b: &mut Budget,
) -> ApiResult<Vec<(u64, u64, Amount, Amount)>> {
    let prefix = hd::event_prefix(key, Kind::Update);
    let rows = txn
        .scan(Table::DexEvents, &prefix, |k| {
            b.tick() && k.starts_with(&prefix)
        })
        .map_err(api)?;
    b.check()?;
    Ok(rows
        .into_iter()
        .filter_map(|(_, v)| {
            let e = StoredEvent::decode(&v).ok()?;
            Some((e.time, e.tx_time, e.amount(0)?, e.amount(1)?))
        })
        .collect())
}

/// The reserves of the newest UPDATE before `t_us`.
fn reserves_before(
    txn: &impl Txn,
    key: &str,
    t_us: u64,
    b: &mut Budget,
) -> ApiResult<Option<(Amount, Amount)>> {
    let prefix = hd::event_prefix(key, Kind::Update);
    for row in txn
        .scan_rev_prefix(Table::DexEvents, &prefix, &[])
        .map_err(api)?
    {
        if !b.tick() {
            break;
        }
        let (_, v) = row.map_err(api)?;
        let Ok(e) = StoredEvent::decode(&v) else {
            continue;
        };
        if e.time < t_us {
            return Ok(e.amount(0).zip(e.amount(1)));
        }
    }
    b.check()?;
    Ok(None)
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

pub fn stats(txn: &impl Txn, p: &PairInfo, now: u64, b: &mut Budget) -> ApiResult<Stats> {
    let today = now / DAY * DAY;
    let th = hour_of_secs(today);
    let from = th.saturating_sub(7 * 24);
    let rt = rates(txn, from.saturating_sub(1), hour_of_secs(now), b)?;
    let mut s = Stats::default();
    for (h, bk) in buckets(txn, &p.key, from, u32::MAX, b)? {
        let v = usd(&bucket_kda(&bk, p.coin), &rt, h);
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
        &rt,
        hour_of_secs(now),
    )
    .round(8);
    let prev = reserves_before(txn, &p.key, today.saturating_mul(1_000_000), b)?;
    s.tvl_prev = usd(&p.tvl_kda(prev.as_ref()), &rt, th.saturating_sub(1)).round(8);
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
        total_supply: supply(txn, &p.key)?,
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
    let s = stats(txn, &p, now, b)?;
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
        for p in listable(txn, b)? {
            b.check()?;
            let s = stats(txn, &p, now, b)?;
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

fn tx_node(
    view: &View,
    id: u128,
    e: &StoredEvent,
    coin: Option<usize>,
    rt: &Rates,
) -> PoolTransaction {
    let f = |i: usize| e.fields.get(i).cloned().unwrap_or_else(|| "0".into());
    let z = || "0".to_string();
    let (a0i, a1i, a0o, a1o, kda, ty) = match e.kind {
        Kind::Swap => {
            let kda = coin.map(|c| {
                let i = Dec::parse(&f(2 + c)).unwrap_or_default();
                if i.is_zero() {
                    Dec::parse(&f(4 + c)).unwrap_or_default()
                } else {
                    i
                }
            });
            (f(2), f(3), f(4), f(5), kda, PoolTransactionType::Swap)
        }
        Kind::AddLiquidity => {
            let kda = coin.map(|c| Dec::parse(&f(2 + c)).unwrap_or_default().mul_int(2));
            (f(2), f(3), z(), z(), kda, PoolTransactionType::AddLiquidity)
        }
        _ => {
            let kda = coin.map(|c| Dec::parse(&f(2 + c)).unwrap_or_default().mul_int(2));
            (
                z(),
                z(),
                f(2),
                f(3),
                kda,
                PoolTransactionType::RemoveLiquidity,
            )
        }
    };
    let hour = hour_of_secs(e.time / 1_000_000);
    let amount_usd = usd(&kda.unwrap_or_default(), rt, hour).round(8).fixed(10);
    let key = TxKey((id >> 16) as u64);
    let requestkey = view
        .tx(key)
        .ok()
        .flatten()
        .and_then(|r| r.request_key)
        .map(|h| h.to_b64url())
        .unwrap_or_default();
    let ts = if e.tx_time > 0 {
        e.tx_time.saturating_mul(1_000_000)
    } else {
        e.time
    };
    let dec = |t: String| Decimal::verbatim(t);
    PoolTransaction {
        id: ID(id.to_string()),
        maker: f(0),
        amount0_in: dec(a0i),
        amount1_in: dec(a1i),
        amount0_out: dec(a0o),
        amount1_out: dec(a1o),
        amount_usd: dec(amount_usd),
        timestamp: DateTime(ts as i64),
        transaction_id: None,
        requestkey,
        transaction_type: ty,
    }
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
        let prefix = hd::event_prefix(key, k);
        txn.walk_keys(Table::DexEvents, &prefix, |kk| {
            if !b.tick() || !kk.starts_with(&prefix) {
                return false;
            }
            total += 1;
            true
        })
        .map_err(api)?;
        b.check()?;
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
        .map(|g| hour_of_secs(g.1.time / 1_000_000))
        .fold((u32::MAX, 0u32), |(l, h), x| (l.min(x), h.max(x)));
    let rt = if got.is_empty() {
        Rates::default()
    } else {
        rates(txn, lo, hi, b)?
    };
    let edges: Vec<PoolTransactionEdge> = got
        .iter()
        .map(|(id, e)| PoolTransactionEdge {
            cursor: enc(*id),
            node: tx_node(view, *id, e, p.coin, &rt),
        })
        .collect();
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

/// Per-day TVL (USD, rounded 8) at the end of each day in `from_day..=to_day` (unix s),
/// from the pair's UPDATE history; days before the pair's first UPDATE have none.
fn tvl_days(
    txn: &impl Txn,
    p: &PairInfo,
    from_day: u64,
    to_day: u64,
    now: u64,
    rt: &Rates,
    b: &mut Budget,
) -> ApiResult<Vec<(u64, Dec)>> {
    let ups = updates(txn, &p.key, b)?;
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut cur: Option<(Amount, Amount)> = None;
    let mut d = from_day;
    while d <= to_day {
        if !b.tick() {
            break;
        }
        let end_us = (d + DAY).saturating_mul(1_000_000);
        while i < ups.len() && ups[i].0 < end_us {
            cur = Some((ups[i].2.clone(), ups[i].3.clone()));
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
            let t = if e.tx_time > 0 {
                e.tx_time
            } else {
                e.time / 1_000_000
            };
            let v = usd(
                &p.tvl_kda(r.as_ref()),
                &rt,
                hour_of_secs(e.time / 1_000_000),
            )
            .round(8);
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
    let mut mine: Vec<(String, String)> = Vec::new();
    if !wallet.is_empty() && wallet.len() <= hd::MAX_ACCOUNT_LEN {
        for p in listable(txn, b)? {
            b.check()?;
            if let Some(v) = txn
                .get(Table::DexLp, &hd::lp_key(&p.key, wallet))
                .map_err(api)?
            {
                mine.push((p.key.clone(), String::from_utf8_lossy(&v).into_owned()));
            }
        }
    }
    let mut rows: Vec<(Dec, String, LiquidityPosition)> = Vec::new();
    for (key, liq) in mine {
        b.check()?;
        let Some(p) = pair_info(txn, &key)? else {
            continue;
        };
        let liq_d = Dec::parse(&liq).unwrap_or_default();
        if !p.status.listable() || liq_d.is_zero() {
            continue;
        }
        let s = stats(txn, &p, now, b)?;
        let sup = Dec::parse(&supply(txn, &key)?).unwrap_or_default();
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

/// Latest reserves of every created, unpoisoned pair with non-zero reserves.
fn price_graph(txn: &impl Txn, b: &mut Budget) -> ApiResult<Vec<PairInfo>> {
    let mut out = Vec::new();
    for k in pair_keys(txn, b)? {
        if !b.tick() {
            break;
        }
        if let Some(p) = pair_info(txn, &k)? {
            let ok = !p.status.poisoned
                && p.status
                    .reserves
                    .as_ref()
                    .is_some_and(|(a, c)| !a.is_zero() && !c.is_zero());
            if ok {
                out.push(p);
            }
        }
    }
    b.check()?;
    Ok(out)
}

/// Stock's `findOptimalKdaPricePath`: BFS from `token`; at each token a direct `coin` pair
/// gives the answer, else the amount moves to each neighbour at the reserve ratio.
pub fn price_in_kda(graph: &[PairInfo], token: &str) -> Option<Dec> {
    if token == "coin" {
        return Some(Dec::int(1));
    }
    let mut adj: HashMap<&str, Vec<(&str, Dec, Dec)>> = HashMap::new();
    for p in graph {
        let (r0, r1) = (p.reserve(0), p.reserve(1));
        adj.entry(&p.t0)
            .or_default()
            .push((&p.t1, r0.clone(), r1.clone()));
        adj.entry(&p.t1).or_default().push((&p.t0, r1, r0));
    }
    let mut q = VecDeque::from([(token, Dec::int(1))]);
    let mut seen = HashSet::from([token]);
    while let Some((t, amt)) = q.pop_front() {
        let edges = adj.get(t)?;
        if let Some((_, rs, ro)) = edges.iter().find(|e| e.0 == "coin") {
            return amt.mul(ro).div(rs);
        }
        for (o, rs, ro) in edges {
            if seen.insert(o) {
                if let Some(x) = amt.mul(ro).div(rs) {
                    q.push_back((o, x));
                }
            }
        }
    }
    None
}

fn price_node(
    graph: &[PairInfo],
    name: &str,
    protocol: &str,
    rate: Option<&Amount>,
    now: u64,
) -> TokenPrice {
    let kda = if protocol == DEFAULT_PROTOCOL {
        price_in_kda(graph, name)
    } else {
        None
    };
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

/// `tokenPrice`: null for a token in no stored pair.
pub fn token_price(
    txn: &impl Txn,
    now: u64,
    addr: &str,
    protocol: &str,
    b: &mut Budget,
) -> ApiResult<Option<TokenPrice>> {
    let graph = price_graph(txn, b)?;
    let known = graph.iter().any(|p| p.t0 == addr || p.t1 == addr);
    if !known {
        return Ok(None);
    }
    let rate = dex_usd::latest(txn).map_err(api)?.map(|r| r.1);
    Ok(Some(price_node(&graph, addr, protocol, rate.as_ref(), now)))
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
    let graph = price_graph(txn, b)?;
    let rate = dex_usd::latest(txn).map_err(api)?.map(|r| r.1);
    let mut names: Vec<&str> = graph
        .iter()
        .filter(|p| p.status.listable())
        .flat_map(|p| [p.t0.as_str(), p.t1.as_str()])
        .collect();
    names.sort_unstable();
    names.dedup();
    Ok(names
        .into_iter()
        .map(|n| price_node(&graph, n, protocol, rate.as_ref(), now))
        .collect())
}

// Tokens.

/// `tokens`: one node per (non-coin module, fungible chain) of the `Tokens` table, newest
/// first height first; cursor = first height ‖ module id ‖ chain.
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
    if txn.has_table(Table::Tokens) {
        let dict = view.dict();
        let rows = txn.scan(Table::Tokens, &[], |_| b.tick()).map_err(api)?;
        b.check()?;
        for (k, v) in rows {
            let (Ok(id), Some(row)) = (
                <[u8; 4]>::try_from(k.as_slice()).map(u32::from_be_bytes),
                kdx_hot::balances::TokenRow::decode(&v),
            ) else {
                continue;
            };
            let Some(name) = dict.name(DictKind::Module, id) else {
                continue;
            };
            if name == "coin" {
                continue;
            }
            for chain in 0..32u32 {
                if row.fungible_chains & (1 << chain) == 0 {
                    continue;
                }
                let rank = (u128::from(row.first_height) << 40)
                    | (u128::from(id) << 8)
                    | u128::from(chain);
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
