//! Stored per-chain counters (`ChainCounters`): the user-tx count and the gas paid in KDA
//! that `networkInfo.countersOfEachChain` serves as `transactionCount` and
//! `totalGasUsedInKda`.
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | `ChainCounters` | chain u8 | user txs u64, gas KDA whole part u128, gas KDA fraction u128 (units of 1e-36 KDA, below 10^36) |
//!
//! - A user tx (coinbase excluded, as stock counts) adds 1 and `gas × meta.gasPrice`, kept
//!   as an exact fixed-point decimal ([`Kda`]): no float, nothing rounded until the
//!   resolver prints it (stock keeps a float sum rounded to 10 decimals and never removes
//!   an orphaned block's gas; see `docs/comparison.md`).
//! - Maintained from block 0 by every writer: `tail::apply` adds each tx in the block's write
//!   txn, `tail::unwind_to_with` subtracts it (recomputed from the record before it is
//!   deleted), and backfill adds each range's totals in the range's MDBX commit
//!   ([`add_tally`]). The freezer moves records and leaves the counters alone.
//!   `kda-indexer reconcile-counts` rebuilds the table from every record ([`replace`]).
//! - `Meta chain_counters_ready` ([`READY`]) says the table may answer. A new hot store sets
//!   it (empty table, empty store: exact). Tip guard, the PR #48 pattern with its own
//!   stamps: `tail::apply` and `tail::unwind_to_with` write `Meta chain_counters_at@<chain>`
//!   = the chain's new `Tail` checkpoint in the txn that changes the counters, and readers
//!   need every stamp to equal its tip, so blocks any build without this table applied or
//!   unwound (one with PR #48 included: it writes `row_counts_at@` but not these) make the
//!   counters untrusted ([`ready`], [`guard`]). A rebuilt range, a backfill resume that
//!   resets the hot frontier, a subtraction below zero, a sum past `u128` KDA or an undone
//!   tx without its record clear the marker; `reconcile-counts` sets it again.
//! - Untrusted numbers: `gasPrice` is parsed by [`parse_price`] (no float, bounded work, no
//!   panic). A price that is missing, negative or malformed adds 0 KDA (the tx still
//!   counts); a product or sum past `u128` whole KDA clears the marker.

use std::collections::BTreeMap;

use anyhow::Result;
use kdx_types::record::TxRecord;
use serde_json::Value;

use crate::checkpoint::Stage;
use crate::env::{RwTxn, Txn};
use crate::tables::Table;

/// Meta key of the readiness marker (u64: the `next_txnum` it was set at).
pub const READY: &str = "chain_counters_ready";
/// Meta key prefix of the per-chain tip the counters follow (`chain_counters_at@<chain>`).
pub const AT_PREFIX: &str = "chain_counters_at@";

pub fn at_key(chain: u8) -> String {
    format!("{AT_PREFIX}{chain}")
}

/// Record that the counters include `chain` up to its current `Tail` checkpoint (absent when
/// the chain has none), in the write txn that changed them.
pub fn stamp(txn: &mut RwTxn<'_>, chain: u8) -> Result<()> {
    match txn.get(Table::StageCheckpoint, &[Stage::Tail as u8, chain])? {
        Some(v) => txn.set_meta(&at_key(chain), &v),
        None => txn.del(Table::Meta, at_key(chain).as_bytes()).map(drop),
    }
}

/// [`stamp`] every chain: the stamps become exactly the current tips (`reconcile-counts`).
pub fn stamp_all(txn: &mut RwTxn<'_>) -> Result<()> {
    for c in stamps(txn)?.into_keys() {
        txn.del(Table::Meta, at_key(c).as_bytes())?;
    }
    for (c, v) in tips(txn)? {
        txn.set_meta(&at_key(c), &v)?;
    }
    Ok(())
}

fn tips(txn: &impl Txn) -> Result<BTreeMap<u8, Vec<u8>>> {
    let p = Stage::Tail as u8;
    Ok(txn
        .scan(Table::StageCheckpoint, &[p], |k| k.first() == Some(&p))?
        .into_iter()
        .filter_map(|(k, v)| Some((*k.get(1)?, v)))
        .collect())
}

fn stamps(txn: &impl Txn) -> Result<BTreeMap<u8, Vec<u8>>> {
    let p = AT_PREFIX.as_bytes();
    let mut out = BTreeMap::new();
    for (k, v) in txn.scan(Table::Meta, p, |k| k.starts_with(p))? {
        let c = std::str::from_utf8(&k[p.len()..])
            .ok()
            .and_then(|s| s.parse::<u8>().ok())
            .ok_or_else(|| anyhow::anyhow!("bad Meta key {}", String::from_utf8_lossy(&k)))?;
        out.insert(c, v);
    }
    Ok(out)
}

/// Why the counters do not follow the hot tips (None: every chain's stamp equals its `Tail`
/// checkpoint, and no chain without a tip has a stamp). At most 20 + 20 rows read.
pub fn tip_mismatch(txn: &impl Txn) -> Result<Option<String>> {
    let (tips, stamps) = (tips(txn)?, stamps(txn)?);
    if tips == stamps {
        return Ok(None);
    }
    let c = tips
        .keys()
        .chain(stamps.keys())
        .find(|c| tips.get(c) != stamps.get(c))
        .copied()
        .unwrap_or_default();
    let h = |v: Option<&Vec<u8>>| {
        v.and_then(|v| v.get(..4))
            .and_then(|b| b.try_into().ok())
            .map_or("none".to_string(), |b| {
                format!("height {}", u32::from_be_bytes(b))
            })
    };
    Ok(Some(format!(
        "chain {c}: counters follow {} but the tail tip is {} (blocks applied or unwound by a build without chain counters?)",
        h(stamps.get(&c)),
        h(tips.get(&c))
    )))
}

/// 10^18.
const E18: u128 = 1_000_000_000_000_000_000;
/// Fraction units per KDA: 10^36.
pub const FRAC_ONE: u128 = E18 * E18;
/// Fraction digits kept.
pub const FRAC_DIGITS: usize = 36;

/// A non-negative KDA amount: `whole + frac / 10^36`, `frac < 10^36`. Exact for every price
/// with at most 36 fraction digits (chainweb accepts at most 12).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Kda {
    pub whole: u128,
    pub frac: u128,
}

impl Kda {
    pub const ZERO: Kda = Kda { whole: 0, frac: 0 };

    pub fn checked_add(self, o: Kda) -> Option<Kda> {
        let mut frac = self.frac + o.frac; // both < 10^36: no overflow
        let mut whole = self.whole.checked_add(o.whole)?;
        if frac >= FRAC_ONE {
            frac -= FRAC_ONE;
            whole = whole.checked_add(1)?;
        }
        Some(Kda { whole, frac })
    }

    pub fn checked_sub(self, o: Kda) -> Option<Kda> {
        let mut whole = self.whole.checked_sub(o.whole)?;
        let frac = if self.frac >= o.frac {
            self.frac - o.frac
        } else {
            whole = whole.checked_sub(1)?;
            self.frac + FRAC_ONE - o.frac
        };
        Some(Kda { whole, frac })
    }

    /// `self × n`, exact; `None` past `u128` whole KDA.
    pub fn checked_mul_u64(self, n: u64) -> Option<Kda> {
        let n = u128::from(n);
        // frac = hi·10^18 + lo; each partial product < 2^64 · 10^18 < 2^124.
        let (hi, lo) = (self.frac / E18, self.frac % E18);
        let p0 = n * lo;
        let p1 = n * hi + p0 / E18;
        let frac = (p1 % E18) * E18 + p0 % E18;
        let whole = self.whole.checked_mul(n)?.checked_add(p1 / E18)?;
        Some(Kda { whole, frac })
    }

    /// Stock's text: rounded half-up to 10 decimals, exactly 10 digits after the point
    /// (Postgres `DECIMAL(20,10)` output, for example `"126267.9040063901"`, `"205.8782168640"`).
    pub fn to_stock_text(self) -> String {
        const UNIT: u128 = FRAC_ONE / 10_000_000_000; // 10^26
        let (mut q, r) = (self.frac / UNIT, self.frac % UNIT);
        let mut whole = self.whole;
        if r >= UNIT / 2 {
            q += 1;
            if q == 10_000_000_000 {
                match whole.checked_add(1) {
                    Some(w) => {
                        whole = w;
                        q = 0;
                    }
                    // Only at u128::MAX whole KDA (never reached: sums past it clear the
                    // marker): truncate instead of carrying.
                    None => q -= 1,
                }
            }
        }
        format!("{whole}.{q:010}")
    }

    fn to_bytes(self) -> [u8; 32] {
        let mut b = [0u8; 32];
        b[..16].copy_from_slice(&self.whole.to_be_bytes());
        b[16..].copy_from_slice(&self.frac.to_be_bytes());
        b
    }
}

/// Parse a decimal number text (`"0.00000001"`, `"1.0e-8"`, `"1E-6"`, `"10"`) into [`Kda`],
/// rounded half-up at the 36th fraction digit. `None` for a negative non-zero value, a value
/// of `u128::MAX` whole KDA or more, or text that is not a plain decimal number. The work is
/// one pass over the text plus at most 40 + 37 digit steps, whatever the exponent.
pub fn parse_price(s: &str) -> Option<Kda> {
    let b = s.as_bytes();
    let mut i = 0;
    let neg = match b.first() {
        Some(b'-') => {
            i = 1;
            true
        }
        _ => false,
    };
    let int_start = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    let int = &b[int_start..i];
    let mut frac: &[u8] = &[];
    if b.get(i) == Some(&b'.') {
        let f = i + 1;
        i = f;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        frac = &b[f..i];
    }
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    let mut exp: i64 = 0;
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        let eneg = match b.get(i) {
            Some(b'-') => {
                i += 1;
                true
            }
            Some(b'+') => {
                i += 1;
                false
            }
            _ => false,
        };
        let e0 = i;
        while i < b.len() && b[i].is_ascii_digit() {
            // Clamped: past ±10^12 the value is 0 or out of range either way.
            exp = (exp * 10 + i64::from(b[i] - b'0')).min(1_000_000_000_000);
            i += 1;
        }
        if i == e0 {
            return None;
        }
        if eneg {
            exp = -exp;
        }
    }
    if i != b.len() {
        return None;
    }
    // Digits without the point, leading zeros dropped; `pos` = digits before the point.
    let all = int.iter().chain(frac).copied();
    let lead = all.clone().take_while(|&d| d == b'0').count();
    let digits: Vec<u8> = all.skip(lead).map(|d| d - b'0').collect();
    if digits.is_empty() {
        return Some(Kda::ZERO);
    }
    if neg {
        return None;
    }
    let pos = (int.len() as i64 - lead as i64).saturating_add(exp);
    let digit = |k: i64| -> u128 {
        if k < 0 {
            0
        } else {
            usize::try_from(k)
                .ok()
                .and_then(|k| digits.get(k))
                .map_or(0, |&d| u128::from(d))
        }
    };
    let mut whole: u128 = 0;
    // The first digit is non-zero, so past 39 whole digits this overflows: at most 40 steps.
    for k in 0..pos.max(0) {
        whole = whole.checked_mul(10)?.checked_add(digit(k))?;
    }
    let mut f: u128 = 0;
    for j in 0..FRAC_DIGITS as i64 {
        f = f * 10 + digit(pos.saturating_add(j));
    }
    let mut out = Kda { whole, frac: f };
    if digit(pos.saturating_add(FRAC_DIGITS as i64)) >= 5 {
        out = out.checked_add(Kda { whole: 0, frac: 1 })?;
    }
    Some(out)
}

/// `meta.gasPrice` of a signed `cmd` as Pact reads a `ParsedDecimal`: a JSON number, a string
/// holding one, or `{"decimal": ...}`. `None` when absent or malformed.
pub fn gas_price(cmd: &[u8]) -> Option<Kda> {
    #[derive(serde::Deserialize)]
    struct Cmd {
        meta: Option<Meta>,
    }
    #[derive(serde::Deserialize)]
    struct Meta {
        #[serde(rename = "gasPrice")]
        gas_price: Option<Value>,
    }
    fn of(v: &Value, depth: u8) -> Option<Kda> {
        match v {
            Value::Number(n) => parse_price(&n.to_string()),
            Value::String(s) => parse_price(s.trim()),
            Value::Object(o) if depth == 0 => of(o.get("decimal")?, 1),
            _ => None,
        }
    }
    let c: Cmd = serde_json::from_slice(cmd).ok()?;
    of(c.meta?.gas_price.as_ref()?, 0)
}

/// What one user tx adds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Term {
    /// `gas × gasPrice` (0 when the price is missing or malformed); `None` past `u128` KDA.
    pub fee: Option<Kda>,
    /// Whether `meta.gasPrice` parsed.
    pub priced: bool,
}

/// `None` for a coinbase (not counted, as stock), else the tx's [`Term`].
pub fn tx_term(r: &TxRecord) -> Option<Term> {
    if r.key.is_coinbase() {
        return None;
    }
    let price = r.cmd.as_deref().and_then(gas_price);
    Some(Term {
        fee: price.unwrap_or(Kda::ZERO).checked_mul_u64(r.gas),
        priced: price.is_some(),
    })
}

/// One chain's counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub user_txs: u64,
    pub gas: Kda,
}

pub fn encode_val(c: Counts) -> [u8; 40] {
    let mut v = [0u8; 40];
    v[..8].copy_from_slice(&c.user_txs.to_be_bytes());
    v[8..].copy_from_slice(&c.gas.to_bytes());
    v
}

/// `None` for a value of the wrong size or a fraction of 10^36 or more (a damaged row).
pub fn decode_val(v: &[u8]) -> Option<Counts> {
    if v.len() != 40 {
        return None;
    }
    let user_txs = u64::from_be_bytes(v[..8].try_into().ok()?);
    let whole = u128::from_be_bytes(v[8..24].try_into().ok()?);
    let frac = u128::from_be_bytes(v[24..40].try_into().ok()?);
    (frac < FRAC_ONE).then_some(Counts {
        user_txs,
        gas: Kda { whole, frac },
    })
}

/// Counters being built in memory (backfill ranges, `reconcile-counts`): chain -> counts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tally {
    pub chains: BTreeMap<u8, Counts>,
    /// User txs whose `gasPrice` is missing or malformed (counted, 0 KDA added).
    pub unpriced: u64,
    /// Sums past `u128` whole KDA: the counters cannot be trusted.
    pub overflow: bool,
}

impl Tally {
    pub fn add(&mut self, r: &TxRecord) {
        let Some(term) = tx_term(r) else {
            return;
        };
        if !term.priced {
            self.unpriced += 1;
        }
        let e = self.chains.entry(r.key.chain()).or_default();
        e.user_txs = e.user_txs.saturating_add(1);
        match term.fee.and_then(|f| e.gas.checked_add(f)) {
            Some(g) => e.gas = g,
            None => self.overflow = true,
        }
    }

    pub fn merge(&mut self, o: Tally) {
        self.unpriced = self.unpriced.saturating_add(o.unpriced);
        self.overflow |= o.overflow;
        for (c, v) in o.chains {
            let e = self.chains.entry(c).or_default();
            e.user_txs = e.user_txs.saturating_add(v.user_txs);
            match e.gas.checked_add(v.gas) {
                Some(g) => e.gas = g,
                None => self.overflow = true,
            }
        }
    }
}

/// Whether the readiness marker is set (whatever the tips; see [`ready`]).
pub fn marker_set(txn: &impl Txn) -> Result<bool> {
    Ok(txn.meta(READY)?.is_some())
}

/// Whether the stored counters may answer: the table exists, the marker is set and every
/// chain's `row_counts_at` stamp equals its hot tip, in `txn`'s snapshot.
pub fn ready(txn: &impl Txn) -> Result<bool> {
    Ok(txn.has_table(Table::ChainCounters) && marker_set(txn)? && tip_mismatch(txn)?.is_none())
}

/// Set the marker at TxNum frontier `next_txnum`.
pub fn set_ready(txn: &mut RwTxn<'_>, next_txnum: u64) -> Result<()> {
    txn.set_meta_u64(READY, next_txnum)
}

/// Clear the marker. Returns whether it was set.
pub fn clear_ready(txn: &mut RwTxn<'_>) -> Result<bool> {
    let was = txn.meta(READY)?.is_some();
    if was {
        txn.del(Table::Meta, READY.as_bytes())?;
        tracing::warn!(
            "chain counters: readiness marker cleared; countersOfEachChain answers NOT_AVAILABLE until `kda-indexer reconcile-counts`"
        );
    }
    Ok(was)
}

/// Startup and per-write check: clear a set marker whose counters do not follow the hot
/// tips. Returns the reason when it cleared it.
pub fn guard(txn: &mut RwTxn<'_>) -> Result<Option<String>> {
    if !marker_set(txn)? {
        return Ok(None);
    }
    let Some(why) = tip_mismatch(txn)? else {
        return Ok(None);
    };
    tracing::warn!(reason = %why, "chain counters: the counters do not follow the hot tips");
    clear_ready(txn)?;
    Ok(Some(why))
}

/// The stored counters of `chain` (zero when it has no row). A damaged row reads as zero
/// and is reported by [`read_all`].
pub fn get(txn: &impl Txn, chain: u8) -> Result<Counts> {
    Ok(txn
        .get(Table::ChainCounters, &[chain])?
        .and_then(|v| decode_val(&v))
        .unwrap_or_default())
}

/// Every stored row: chain -> counts (`None` for a damaged row). Twenty rows at most.
pub fn read_all(txn: &impl Txn) -> Result<BTreeMap<u8, Option<Counts>>> {
    let mut out = BTreeMap::new();
    if !txn.has_table(Table::ChainCounters) {
        return Ok(out);
    }
    for (k, v) in txn.scan(Table::ChainCounters, &[], |_| true)? {
        if let [c] = k[..] {
            out.insert(c, decode_val(&v));
        }
    }
    Ok(out)
}

fn put(txn: &mut RwTxn<'_>, chain: u8, c: Counts) -> Result<()> {
    if c == Counts::default() {
        txn.del(Table::ChainCounters, &[chain]).map(drop)
    } else {
        txn.put(Table::ChainCounters, &[chain], &encode_val(c))
    }
}

/// Add (`sign` ≥ 0) or subtract `r`'s term, in the block's write txn. A subtraction below
/// zero, a sum past `u128` KDA or a damaged row clears the marker (the value is then left
/// as close as it can be; it is not served until `reconcile-counts`).
pub fn add_tx(txn: &mut RwTxn<'_>, r: &TxRecord, sign: i64) -> Result<()> {
    let Some(term) = tx_term(r) else {
        return Ok(());
    };
    let chain = r.key.chain();
    let raw = txn.get(Table::ChainCounters, &[chain])?;
    let mut c = match raw.as_deref().map(decode_val) {
        None => Counts::default(),
        Some(Some(c)) => c,
        Some(None) => {
            clear_ready(txn)?;
            Counts::default()
        }
    };
    let next = if sign >= 0 {
        c.user_txs
            .checked_add(1)
            .zip(term.fee.and_then(|f| c.gas.checked_add(f)))
    } else {
        c.user_txs
            .checked_sub(1)
            .zip(term.fee.and_then(|f| c.gas.checked_sub(f)))
    };
    match next {
        Some((n, g)) => {
            c.user_txs = n;
            c.gas = g;
        }
        None => {
            tracing::warn!(
                chain,
                height = r.key.height(),
                pos = r.key.pos(),
                "chain counters: out of range"
            );
            clear_ready(txn)?;
            c.user_txs = if sign >= 0 {
                c.user_txs.saturating_add(1)
            } else {
                c.user_txs.saturating_sub(1)
            };
        }
    }
    put(txn, chain, c)
}

/// Add a backfill range's totals in its commit txn.
pub fn add_tally(txn: &mut RwTxn<'_>, t: &Tally) -> Result<()> {
    if t.overflow {
        clear_ready(txn)?;
    }
    for (&chain, v) in &t.chains {
        let raw = txn.get(Table::ChainCounters, &[chain])?;
        let cur = match raw.as_deref().map(decode_val) {
            None => Some(Counts::default()),
            Some(c) => c,
        };
        let next = cur.and_then(|c| {
            Some(Counts {
                user_txs: c.user_txs.checked_add(v.user_txs)?,
                gas: c.gas.checked_add(v.gas)?,
            })
        });
        match next {
            Some(n) => put(txn, chain, n)?,
            None => {
                clear_ready(txn)?;
            }
        }
    }
    Ok(())
}

/// Replace the whole table with `t` (`reconcile-counts`; the caller sets the marker).
pub fn replace(txn: &mut RwTxn<'_>, t: &Tally) -> Result<()> {
    txn.clear(Table::ChainCounters)?;
    for (&chain, &v) in &t.chains {
        put(txn, chain, v)?;
    }
    Ok(())
}

/// Stored rows that differ from `t` (missing, extra, damaged or different each count 1).
pub fn differing(txn: &impl Txn, t: &Tally) -> Result<u64> {
    let stored = read_all(txn)?;
    let mut n = 0u64;
    for (c, v) in &stored {
        if v.as_ref() != t.chains.get(c).filter(|x| **x != Counts::default()) {
            n += 1;
        }
    }
    n += t
        .chains
        .iter()
        .filter(|(c, v)| **v != Counts::default() && !stored.contains_key(c))
        .count() as u64;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> Kda {
        parse_price(s).unwrap()
    }

    #[test]
    fn parses_node_forms() {
        let e8 = Kda {
            whole: 0,
            frac: FRAC_ONE / 100_000_000,
        };
        for s in [
            "0.00000001",
            "1.0e-8",
            "1e-8",
            "1E-8",
            "0.1e-7",
            "10e-9",
            "1.0E-08",
        ] {
            assert_eq!(k(s), e8, "{s}");
        }
        assert_eq!(k("10"), Kda { whole: 10, frac: 0 });
        assert_eq!(
            k("2.5"),
            Kda {
                whole: 2,
                frac: FRAC_ONE / 2
            }
        );
        assert_eq!(
            k("1e2"),
            Kda {
                whole: 100,
                frac: 0
            }
        );
        assert_eq!(k("-0"), Kda::ZERO);
        assert_eq!(k("0.000"), Kda::ZERO);
    }

    #[test]
    fn rejects_hostile_prices_without_panic() {
        for s in [
            "",
            "-",
            ".",
            "e5",
            "1e",
            "1e+",
            "abc",
            "1.2.3",
            "-1",
            "-0.00000001",
            "NaN",
            "Infinity",
            "1 ",
            "0x10",
            "1e999999999999999999999999",
            "999999999999999999999999999999999999999999",
        ] {
            assert!(parse_price(s).is_none(), "{s}");
        }
        // Tiny or long values are bounded work and round at the 36th digit.
        assert_eq!(k("1e-999999999999999999999"), Kda::ZERO);
        assert_eq!(
            k(&format!("0.{}", "9".repeat(5000))),
            Kda { whole: 1, frac: 0 }
        );
        assert_eq!(
            k("0.0000000000000000000000000000000000015"),
            Kda { whole: 0, frac: 2 }
        );
        assert_eq!(
            k("0.0000000000000000000000000000000000014"),
            Kda { whole: 0, frac: 1 }
        );
    }

    #[test]
    fn gas_price_forms() {
        let e8 = k("0.00000001");
        assert_eq!(gas_price(br#"{"meta":{"gasPrice":1.0e-8}}"#), Some(e8));
        assert_eq!(
            gas_price(br#"{"meta":{"gasPrice":"0.00000001"}}"#),
            Some(e8)
        );
        assert_eq!(
            gas_price(br#"{"meta":{"gasPrice":{"decimal":"1e-8"}}}"#),
            Some(e8)
        );
        assert_eq!(
            gas_price(br#"{"meta":{"gasPrice":{"decimal":{"decimal":1}}}}"#),
            None
        );
        assert_eq!(gas_price(br#"{"meta":{"gasPrice":-1}}"#), None);
        assert_eq!(gas_price(br#"{"meta":{}}"#), None);
        assert_eq!(gas_price(b"not json"), None);
    }

    #[test]
    fn mul_and_sum_are_exact_and_overflow_safe() {
        // 1e-8 × 1000 gas, a million times: exactly 10 KDA.
        let fee = k("0.00000001").checked_mul_u64(1000).unwrap();
        let mut s = Kda::ZERO;
        for _ in 0..1_000_000 {
            s = s.checked_add(fee).unwrap();
        }
        assert_eq!(s, Kda { whole: 10, frac: 0 });
        // 0.1 summed ten times is exactly 1 (a float sum is not).
        let mut s = Kda::ZERO;
        for _ in 0..10 {
            s = s.checked_add(k("0.1")).unwrap();
        }
        assert_eq!(s, Kda { whole: 1, frac: 0 });
        // Max gas × the finest fraction: carries, no overflow.
        let p = Kda {
            whole: 0,
            frac: FRAC_ONE - 1,
        };
        let m = p.checked_mul_u64(u64::MAX).unwrap();
        assert_eq!(m.whole, u128::from(u64::MAX) - 1);
        assert_eq!(m.frac, FRAC_ONE - u128::from(u64::MAX));
        // Past u128 whole KDA: None, never a panic or a wrap.
        let big = Kda {
            whole: u128::MAX,
            frac: 0,
        };
        assert!(big.checked_mul_u64(2).is_none());
        assert!(big
            .checked_add(Kda {
                whole: 0,
                frac: FRAC_ONE - 1
            })
            .is_some());
        assert!(Kda {
            whole: u128::MAX,
            frac: FRAC_ONE - 1
        }
        .checked_add(Kda { whole: 0, frac: 1 })
        .is_none());
        assert!(Kda::ZERO.checked_sub(Kda { whole: 0, frac: 1 }).is_none());
        assert_eq!(
            Kda { whole: 1, frac: 0 }.checked_sub(Kda { whole: 0, frac: 1 }),
            Some(Kda {
                whole: 0,
                frac: FRAC_ONE - 1
            })
        );
    }

    #[test]
    fn stock_text_rounds_half_up_to_ten_decimals() {
        // Shapes from stock's live response (2026-11-26).
        assert_eq!(k("126267.9040063901").to_stock_text(), "126267.9040063901");
        assert_eq!(k("205.878216864").to_stock_text(), "205.8782168640");
        assert_eq!(Kda::ZERO.to_stock_text(), "0.0000000000");
        assert_eq!(k("0.00000000005").to_stock_text(), "0.0000000001");
        assert_eq!(k("0.000000000049999").to_stock_text(), "0.0000000000");
        assert_eq!(k("9.99999999995").to_stock_text(), "10.0000000000");
        assert_eq!(
            Kda {
                whole: u128::MAX,
                frac: FRAC_ONE - 1
            }
            .to_stock_text(),
            format!("{}.9999999999", u128::MAX)
        );
    }

    #[test]
    fn value_roundtrip_and_damage() {
        let c = Counts {
            user_txs: 189_149_913,
            gas: k("126267.904006390123456789"),
        };
        assert_eq!(decode_val(&encode_val(c)), Some(c));
        assert_eq!(decode_val(&[0u8; 16]), None);
        let mut bad = encode_val(c);
        bad[24..].copy_from_slice(&FRAC_ONE.to_be_bytes());
        assert_eq!(decode_val(&bad), None);
    }
}
