//! Sushi DEX ledger (stock-gap B2): the store side of stock's DEX fields (`pools`, `pool`,
//! `poolTransactions`, `liquidityPositions`, `dexMetrics`, `tokenPrice(s)`, `tokens`).
//!
//! Stock (indexer-kadena `90a4be1`, `services/pair.ts`, `pair-service.ts`) indexes one DEX:
//! Sushi on Kadena, module [`SUSHI_EXCHANGE`] and its LP token module [`SUSHI_TOKENS`], on
//! chain [`SUSHI_CHAIN`], at any height (stock applies no height filter: `services/pair.ts`
//! filters by module and name only, and its backfill starts at block 0; the first real
//! `CREATE_PAIR` is at 6,121,854). It reads these events (params as stock destructures them):
//!
//! | Event | Params |
//! |---|---|
//! | `CREATE_PAIR` | `[token0 ref, token1 ref, key, account]` |
//! | `UPDATE` | `[key, reserve0, reserve1]` |
//! | `SWAP` | `[sender, receiver, in, token-in ref, out, token-out ref]` |
//! | `ADD_LIQUIDITY`, `REMOVE_LIQUIDITY` | `[sender, to, token0 ref, token1 ref, amount0, amount1, liquidity]` |
//! | `MINT_EVENT`, `BURN_EVENT` (tokens module) | `[key, account, amount]` |
//! | `TRANSFER_EVENT` (tokens module) | `[key, sender, receiver, amount]` |
//!
//! **Trust.** Only an event whose `EventName` is exactly one of [`EVENT_NAMES`] *and* whose
//! `Module` is the matching Sushi module is read (Pact lets only a module emit events under its
//! own name; the `n_` principal namespace cannot be squatted). Amounts come from these events
//! only, never from the tx's `result` or its args (both are the sender's). A failed or coinbase
//! tx or another chain is skipped before any decoding.
//!
//! **Pairs.** A pair is named by its two token names sorted bytewise and joined by `:` (stock's
//! `createOrFindPair` key; Pact names hold no `:`). `token0` is the smaller name. Every event is
//! oriented to that order on its own (an `UPDATE`/LP key `x:y` is split and re-sorted; a swap or
//! liquidity event names its tokens), so no event depends on another being stored first: the
//! stored state is a pure function of the *set* of events stored. That makes ingest, unwind and
//! the history fill (in any order, beside the live tail) give the same bytes.
//!
//! **Amounts** are exact decimals ([`Amount`]): at most [`SCALE`] fractional digits, at most
//! [`MAX_AMOUNT_TEXT`] bytes of text, exponent within ±[`MAX_EXP`], never negative. An event with
//! an amount outside that is not stored (counted in [`ApplyStats::rejected`]). Sums are
//! arbitrary precision, so they never overflow, and stored text is read back with the wider
//! bound [`MAX_STORED_TEXT`] (an input is below `10^(96+64)`, so a sum of fewer than `2^64`
//! of them has at most 180 integer digits: about 206 bytes of text). No division happens at ingest: a price in KDA is
//! `coin leg / token leg` of a swap, taken by the reader (which must treat a zero leg as no
//! price).
//!
//! **Tables** (additive, rule A-1; integers big-endian; `pair` = `token0:token1` utf-8;
//! `ev` = `TxKey u64 ‖ event index u16`, the event's place in its tx):
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | `DexPairs` | pair, 0x00, ev | empty (one row per `CREATE_PAIR`) |
//! | `DexEvents` | pair, 0x00, kind u8, ev | kind u8, block time u64 (µs), tx `meta.creationTime` u64 (s, as sent; 0 when absent), then varint-length fields ([`StoredEvent`]) |
//! | `DexByTx` | ev | kind u8, pair (where the event's row is; unwind and idempotence index) |
//! | `DexLp` | pair, 0x00, account | LP balance, decimal text (row absent when zero) |
//! | `DexAgg` | 0x01, pair | LP total supply, decimal text (Σ mint − Σ burn; absent when zero) |
//! | `DexAgg` | 0x02, pair, 0x00, hour u32 | [`Bucket`]: swap/add/remove counts (wrapping i64) and swap in/out sums per side |
//! | `DexAgg` | 0x03, pair | empty: the pair is poisoned ([`poisoned`]); never removed |
//! | `DexAgg` | 0x04, pair, 0x00, kind u8 | i64 (wrapping): stored SWAP / ADD_LIQUIDITY / REMOVE_LIQUIDITY events of the pair ([`event_count`]); absent when zero |
//! | `DexAgg` | 0x05, token, 0x00, ev | pair: one row per token of each `CREATE_PAIR` (a token's pairs, oldest first; [`token_pairs`]) |
//! | `DexAgg` | 0x06, pair | i64 (wrapping): stored `UPDATE`s with both reserves non-zero, for pairs with a `coin` leg only; absent when zero. The listing candidates ([`funded_pairs`]): a coin pair that was never funded is never read |
//! | `DexUsd` | hour u32 | USD per KDA, decimal text: reserved for the serving part's DIA feed; ingest never writes it |
//!
//! `DexEvents` fields per kind (amounts oriented to the pair, as decimal text):
//! `CREATE_PAIR` `[sushi key, account]`; `UPDATE` `[reserve0, reserve1]`; `SWAP` `[maker,
//! receiver, amount0In, amount1In, amount0Out, amount1Out]`; `ADD_LIQUIDITY` /
//! `REMOVE_LIQUIDITY` `[sender, to, amount0, amount1, liquidity]`; `MINT_EVENT` / `BURN_EVENT`
//! `[account, amount]`; `TRANSFER_EVENT` `[sender, receiver, amount]`.
//! The hour of a bucket is the tx's `meta.creationTime / 3600 s` ([`event_hour`]), stock's
//! time for its 24h/7d windows and `PoolTransaction.timestamp` (sender-chosen,
//! `pair-service.ts:691`), or the block's `creationTime` when the tx carries none. It is read
//! from the stored row ([`StoredEvent::tx_time`]), so unwind removes an event from the bucket
//! it was added to. The chain bounds `creationTime` by the tx's TTL, so a block can add to an
//! earlier hour than its own.
//!
//! **Layout.** [`LAYOUT`] is the version of the derived rows (buckets, counts, the token
//! index). A store written under another version is cleared by [`ensure_layout`] and rebuilt
//! by the history fill (the stored state is a function of the stored events, so the fill
//! beside the live tail gives the same bytes).
//! Fees are not stored: Sushi's fee is 0.3 % of the input, so a bucket's fee per side is
//! `in × 3 / 997` (stock's `in × FEE / (1 − FEE)`), exact from the stored sums.
//!
//! **Containment.** No stored DEX value can make [`apply_tx`] or [`undo_block`] fail: a row
//! that cannot be read back (it cannot happen with the bounds above; it would take a bug or a
//! damaged store) marks its pair poisoned (`DexAgg` 0x03): the pair's aggregates are no longer
//! updated and the serving part must report the pair as unavailable. Its events are still
//! stored and unwound, so the tail and every other pair carry on. Only storage errors (the
//! database itself) propagate.
//!
//! **Deliberate divergences from stock** (stock's definitions are wrong or unsafe there):
//! - LP total supply is Σ `MINT_EVENT` − Σ `BURN_EVENT` (the module's real supply). Stock's
//!   `totalSupply` is Σ ADD `liquidity` − Σ REMOVE `liquidity`, which leaves out Sushi's locked
//!   minimum, so a correct stock row differs from ours by that minimum.
//! - A tx with more than [`MAX_EVENTS_PER_TX`] trusted events keeps its first ones and drops
//!   the rest, so such a tx stores a partial ledger (e.g. a `MINT_EVENT` kept, its matching
//!   `BURN_EVENT` dropped). Only the sender's own pair can be affected; no real tx comes close.
//!
//! **Junk pairs.** Anyone can create a pair for two junk tokens on the real module (stock has
//! the same exposure), so `DexPairs` counts are not a listing. Ingest stores every pair; the
//! serving part filters listings with [`pair_status`] / [`PairStatus::listable`] (created, not
//! poisoned, a `coin` leg, non-zero latest reserves).
//!
//! **Reorgs.** A block's rows are found through `DexByTx` (its keys are the block's TxKeys, one
//! contiguous range): [`undo_block`] reverses each row's effect and deletes it. No undo log is
//! needed, and rows the history fill wrote are unwound the same way.
//!
//! **Bounds.** At most [`MAX_EVENTS_PER_TX`] Sushi events are read per tx; each writes at most
//! 4 rows of at most a few hundred bytes (names ≤ 256 bytes, amounts ≤ [`MAX_AMOUNT_TEXT`]).

use anyhow::{bail, Context, Result};
use kdx_types::record::TxRecord;
use kdx_types::varint::{get_bytes, put_bytes};
use kdx_types::{value, Dict, DictKind, TxKey};
use serde_json::Value;

use crate::env::{RwTxn, Txn};
use crate::prices::valid_token;
use crate::tables::Table;

/// Sushi's exchange module.
pub const SUSHI_EXCHANGE: &str = "n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange";
/// Sushi's LP token module.
pub const SUSHI_TOKENS: &str = "n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange-tokens";
/// The only chain Sushi is deployed on.
pub const SUSHI_CHAIN: u8 = 2;
/// Fractional digits an amount may have.
pub const SCALE: usize = 24;
/// Longest amount text accepted from an event (bytes).
pub const MAX_AMOUNT_TEXT: usize = 96;
/// Longest stored amount text read back (bytes). Every sum the writer can produce is far
/// shorter (about 206 bytes, see the module doc); a longer one poisons its pair.
pub const MAX_STORED_TEXT: usize = 512;
/// Largest exponent magnitude accepted in an amount (`1e40`).
pub const MAX_EXP: i64 = 64;
/// Sushi events read per tx; the rest are not stored.
pub const MAX_EVENTS_PER_TX: usize = 64;
/// Longest account name stored (Pact accounts are at most 256 characters).
pub const MAX_ACCOUNT_LEN: usize = 256;

/// Sushi event kinds (stored as the `kind` byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Kind {
    CreatePair = 0,
    Update = 1,
    Swap = 2,
    AddLiquidity = 3,
    RemoveLiquidity = 4,
    Mint = 5,
    Burn = 6,
    Transfer = 7,
}

impl Kind {
    pub const ALL: [Kind; 8] = [
        Kind::CreatePair,
        Kind::Update,
        Kind::Swap,
        Kind::AddLiquidity,
        Kind::RemoveLiquidity,
        Kind::Mint,
        Kind::Burn,
        Kind::Transfer,
    ];

    pub fn from_u8(b: u8) -> Option<Kind> {
        Kind::ALL.get(usize::from(b)).copied()
    }

    /// The emitting module and the event's name.
    pub fn module_and_name(self) -> (&'static str, &'static str) {
        match self {
            Kind::CreatePair => (SUSHI_EXCHANGE, "CREATE_PAIR"),
            Kind::Update => (SUSHI_EXCHANGE, "UPDATE"),
            Kind::Swap => (SUSHI_EXCHANGE, "SWAP"),
            Kind::AddLiquidity => (SUSHI_EXCHANGE, "ADD_LIQUIDITY"),
            Kind::RemoveLiquidity => (SUSHI_EXCHANGE, "REMOVE_LIQUIDITY"),
            Kind::Mint => (SUSHI_TOKENS, "MINT_EVENT"),
            Kind::Burn => (SUSHI_TOKENS, "BURN_EVENT"),
            Kind::Transfer => (SUSHI_TOKENS, "TRANSFER_EVENT"),
        }
    }

    /// Fields a stored event of this kind carries.
    fn fields(self) -> usize {
        match self {
            Kind::CreatePair | Kind::Update | Kind::Mint | Kind::Burn => 2,
            Kind::Transfer => 3,
            Kind::AddLiquidity | Kind::RemoveLiquidity => 5,
            Kind::Swap => 6,
        }
    }
}

/// The qualified event names read (`<module>.<NAME>`), in [`Kind`] order.
pub const EVENT_NAMES: [&str; 8] = [
    "n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange.CREATE_PAIR",
    "n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange.UPDATE",
    "n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange.SWAP",
    "n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange.ADD_LIQUIDITY",
    "n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange.REMOVE_LIQUIDITY",
    "n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange-tokens.MINT_EVENT",
    "n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange-tokens.BURN_EVENT",
    "n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange-tokens.TRANSFER_EVENT",
];

/// The kind of a qualified event name, when it is one of [`EVENT_NAMES`].
pub fn kind_of_name(name: &str) -> Option<Kind> {
    EVENT_NAMES
        .iter()
        .position(|n| *n == name)
        .and_then(|i| Kind::from_u8(i as u8))
}

// ---------------------------------------------------------------------------------------------
// Exact decimals.

/// Limb base of [`Amount`] magnitudes.
const BASE: u64 = 1_000_000_000;

/// An exact signed decimal: `mag × 10^-SCALE`, `mag` in base-10⁹ limbs, least significant
/// first, no high zero limbs (zero is empty and never negative).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Amount {
    neg: bool,
    mag: Vec<u32>,
}

impl Amount {
    pub fn zero() -> Amount {
        Amount::default()
    }

    pub fn is_zero(&self) -> bool {
        self.mag.is_empty()
    }

    pub fn is_negative(&self) -> bool {
        self.neg
    }

    /// Parse `-?digits(.digits)?([eE][+-]?digits)?`; `None` when longer than
    /// [`MAX_AMOUNT_TEXT`], with an exponent beyond ±[`MAX_EXP`], or with a non-zero digit past
    /// [`SCALE`] fractional places (it would not be exact).
    pub fn parse(s: &str) -> Option<Amount> {
        Amount::parse_within(s, MAX_AMOUNT_TEXT)
    }

    /// Parse stored text (a sum or a stored event field): as [`Amount::parse`] with the bound
    /// [`MAX_STORED_TEXT`].
    pub fn parse_stored(s: &str) -> Option<Amount> {
        Amount::parse_within(s, MAX_STORED_TEXT)
    }

    fn parse_within(s: &str, max: usize) -> Option<Amount> {
        if s.is_empty() || s.len() > max {
            return None;
        }
        let (neg, s) = match s.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, s),
        };
        let (num, exp) = match s.find(['e', 'E']) {
            Some(i) => {
                let e = &s[i + 1..];
                let digits = e.strip_prefix(['+', '-']).unwrap_or(e);
                if digits.is_empty()
                    || digits.len() > 4
                    || !digits.bytes().all(|b| b.is_ascii_digit())
                {
                    return None;
                }
                let v: i64 = e.parse().ok()?;
                if v.abs() > MAX_EXP {
                    return None;
                }
                (&s[..i], v)
            }
            None => (s, 0),
        };
        let (int, frac) = match num.split_once('.') {
            Some((a, b)) => (a, b),
            None => (num, ""),
        };
        if int.is_empty() && frac.is_empty() {
            return None;
        }
        if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
            return None;
        }
        // value = digits × 10^(exp − frac.len()); stored as digits × 10^(SCALE + exp − frac.len()).
        let mut digits: Vec<u8> = int.bytes().chain(frac.bytes()).map(|b| b - b'0').collect();
        let shift = SCALE as i64 + exp - frac.len() as i64;
        if shift >= 0 {
            digits.extend(std::iter::repeat_n(0, shift as usize));
        } else {
            let cut = (-shift) as usize;
            if cut > digits.len() {
                if digits.iter().any(|d| *d != 0) {
                    return None;
                }
                digits.clear();
            } else {
                let tail = digits.split_off(digits.len() - cut);
                if tail.iter().any(|d| *d != 0) {
                    return None;
                }
            }
        }
        let mut a = Amount {
            neg,
            mag: limbs_of_digits(&digits),
        };
        a.norm();
        Some(a)
    }

    /// Canonical text: `-`, integer digits, and `.` plus fractional digits without trailing
    /// zeros (`0` for zero).
    pub fn to_text(&self) -> String {
        if self.is_zero() {
            return "0".to_owned();
        }
        let mut d = String::new();
        for (i, l) in self.mag.iter().rev().enumerate() {
            if i == 0 {
                d.push_str(&l.to_string());
            } else {
                d.push_str(&format!("{l:09}"));
            }
        }
        if d.len() <= SCALE {
            d = format!("{}{d}", "0".repeat(SCALE + 1 - d.len()));
        }
        let (int, frac) = d.split_at(d.len() - SCALE);
        let frac = frac.trim_end_matches('0');
        let sign = if self.neg { "-" } else { "" };
        if frac.is_empty() {
            format!("{sign}{int}")
        } else {
            format!("{sign}{int}.{frac}")
        }
    }

    /// `self + other` (`sub`: `self − other`), exact.
    pub fn add(&self, other: &Amount, sub: bool) -> Amount {
        let oneg = other.neg != sub;
        let mut r = if self.neg == oneg {
            Amount {
                neg: self.neg,
                mag: add_mag(&self.mag, &other.mag),
            }
        } else if cmp_mag(&self.mag, &other.mag) != std::cmp::Ordering::Less {
            Amount {
                neg: self.neg,
                mag: sub_mag(&self.mag, &other.mag),
            }
        } else {
            Amount {
                neg: oneg,
                mag: sub_mag(&other.mag, &self.mag),
            }
        };
        r.norm();
        r
    }

    fn norm(&mut self) {
        while self.mag.last() == Some(&0) {
            self.mag.pop();
        }
        if self.mag.is_empty() {
            self.neg = false;
        }
    }
}

fn limbs_of_digits(d: &[u8]) -> Vec<u32> {
    let mut out = Vec::with_capacity(d.len() / 9 + 1);
    let mut end = d.len();
    while end > 0 {
        let start = end.saturating_sub(9);
        let mut v = 0u32;
        for x in &d[start..end] {
            v = v * 10 + u32::from(*x);
        }
        out.push(v);
        end = start;
    }
    out
}

fn cmp_mag(a: &[u32], b: &[u32]) -> std::cmp::Ordering {
    a.len()
        .cmp(&b.len())
        .then_with(|| a.iter().rev().cmp(b.iter().rev()))
}

fn add_mag(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len().max(b.len()) + 1);
    let mut carry = 0u64;
    for i in 0..a.len().max(b.len()) {
        let s = u64::from(*a.get(i).unwrap_or(&0)) + u64::from(*b.get(i).unwrap_or(&0)) + carry;
        out.push((s % BASE) as u32);
        carry = s / BASE;
    }
    if carry > 0 {
        out.push(carry as u32);
    }
    out
}

/// `a − b` with `a ≥ b`.
fn sub_mag(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len());
    let mut borrow = 0i64;
    for (i, x) in a.iter().enumerate() {
        let mut d = i64::from(*x) - i64::from(*b.get(i).unwrap_or(&0)) - borrow;
        if d < 0 {
            d += BASE as i64;
            borrow = 1;
        } else {
            borrow = 0;
        }
        out.push(d as u32);
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Decoding.

/// A Pact amount as stock reads it (`typeof a === 'number' ? a : a.decimal`; we also take
/// `{"int": n}`), non-negative and exact.
pub fn amount_of(v: &Value) -> Option<Amount> {
    let text = match v {
        Value::Number(n) => n.to_string(),
        Value::Object(o) => match o.get("decimal").or_else(|| o.get("int"))? {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            _ => return None,
        },
        _ => return None,
    };
    Amount::parse(&text).filter(|a| !a.is_negative())
}

/// The qualified name of a Pact module reference (`{"refName":{"namespace","name"}}`).
pub fn module_ref(v: &Value) -> Option<String> {
    let r = v.get("refName")?;
    let name = r.get("name")?.as_str()?;
    let q = match r.get("namespace") {
        Some(Value::String(ns)) => format!("{ns}.{name}"),
        None | Some(Value::Null) => name.to_owned(),
        _ => return None,
    };
    valid_token(&q).then_some(q)
}

fn account_of(v: &Value) -> Option<String> {
    v.as_str()
        .filter(|s| !s.is_empty() && s.len() <= MAX_ACCOUNT_LEN)
        .map(str::to_owned)
}

/// The pair of two distinct token names: `(pair, flipped)`, `flipped` when `a` is not token0.
pub fn pair_of(a: &str, b: &str) -> Option<(String, bool)> {
    if a == b || !valid_token(a) || !valid_token(b) {
        return None;
    }
    Some(if a < b {
        (format!("{a}:{b}"), false)
    } else {
        (format!("{b}:{a}"), true)
    })
}

/// The pair of a Sushi key `x:y` (`UPDATE`, LP events).
pub fn pair_of_key(key: &str) -> Option<(String, bool)> {
    let (a, b) = key.split_once(':')?;
    pair_of(a, b)
}

/// One decoded Sushi event, oriented to its pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DexEvent {
    pub kind: Kind,
    pub pair: String,
    /// The fields stored for the kind (module doc), amounts as canonical text.
    pub fields: Vec<String>,
}

/// Decode a Sushi event's params. `None` when they do not have the layout stock reads, or an
/// amount, name or account is out of bounds.
pub fn decode_event(kind: Kind, params: &Value) -> Option<DexEvent> {
    let p = params.as_array()?;
    let amt = |i: usize| amount_of(p.get(i)?);
    let (pair, fields) = match kind {
        Kind::CreatePair => {
            if p.len() < 3 {
                return None;
            }
            let (pair, _) = pair_of(&module_ref(&p[0])?, &module_ref(&p[1])?)?;
            let key = p[2].as_str().filter(|k| k.len() <= 2 * 256 + 1)?;
            // The key must name the same two tokens.
            if pair_of_key(key)?.0 != pair {
                return None;
            }
            let account = match p.get(3) {
                Some(v) => account_of(v)?,
                None => String::new(),
            };
            (pair, vec![key.to_owned(), account])
        }
        Kind::Update => {
            let (pair, flip) = pair_of_key(p.first()?.as_str()?)?;
            let (r0, r1) = (amt(1)?, amt(2)?);
            let (r0, r1) = if flip { (r1, r0) } else { (r0, r1) };
            (pair, vec![r0.to_text(), r1.to_text()])
        }
        Kind::Swap => {
            if p.len() != 6 {
                return None;
            }
            let (maker, receiver) = (account_of(&p[0])?, account_of(&p[1])?);
            let (ain, aout) = (amt(2)?, amt(4)?);
            let (tin, tout) = (module_ref(&p[3])?, module_ref(&p[5])?);
            let (pair, flip) = pair_of(&tin, &tout)?;
            let z = Amount::zero();
            // token-in is token0 unless flipped.
            let (a0i, a1i, a0o, a1o) = if flip {
                (z.clone(), ain, aout, z)
            } else {
                (ain, z.clone(), z, aout)
            };
            (
                pair,
                vec![
                    maker,
                    receiver,
                    a0i.to_text(),
                    a1i.to_text(),
                    a0o.to_text(),
                    a1o.to_text(),
                ],
            )
        }
        Kind::AddLiquidity | Kind::RemoveLiquidity => {
            if p.len() != 7 {
                return None;
            }
            let (sender, to) = (account_of(&p[0])?, account_of(&p[1])?);
            let (pair, flip) = pair_of(&module_ref(&p[2])?, &module_ref(&p[3])?)?;
            let (a0, a1, liq) = (amt(4)?, amt(5)?, amt(6)?);
            let (a0, a1) = if flip { (a1, a0) } else { (a0, a1) };
            (
                pair,
                vec![sender, to, a0.to_text(), a1.to_text(), liq.to_text()],
            )
        }
        Kind::Mint | Kind::Burn => {
            if p.len() != 3 {
                return None;
            }
            let (pair, _) = pair_of_key(p[0].as_str()?)?;
            (pair, vec![account_of(&p[1])?, amt(2)?.to_text()])
        }
        Kind::Transfer => {
            if p.len() != 4 {
                return None;
            }
            let (pair, _) = pair_of_key(p[0].as_str()?)?;
            (
                pair,
                vec![account_of(&p[1])?, account_of(&p[2])?, amt(3)?.to_text()],
            )
        }
    };
    Some(DexEvent { kind, pair, fields })
}

/// The kind of a tx event when it is a trusted Sushi event: its name is one of
/// [`EVENT_NAMES`] and its module is the matching Sushi module.
pub fn trusted_kind(dict: &dyn Dict, e: &kdx_types::record::EventRec) -> Option<Kind> {
    let kind = kind_of_name(dict.name(DictKind::EventName, e.name)?)?;
    let module = dict.name(DictKind::Module, e.module)?;
    (module == kind.module_and_name().0).then_some(kind)
}

/// Whether a tx can hold Sushi events at all (checked before any decoding).
pub fn in_scope(r: &TxRecord) -> bool {
    r.status_ok && !r.key.is_coinbase() && r.key.chain() == SUSHI_CHAIN && !r.events.is_empty()
}

/// The trusted, well-formed Sushi events of a tx: `(event index, event)`, at most
/// [`MAX_EVENTS_PER_TX`] trusted events read. Also returns how many trusted events were not
/// usable (malformed, out of bounds, or past the cap).
pub fn events_of_tx(dict: &dyn Dict, r: &TxRecord) -> (Vec<(u16, DexEvent)>, u64) {
    let mut out = Vec::new();
    let mut rejected = 0u64;
    if !in_scope(r) {
        return (out, 0);
    }
    let mut seen = 0usize;
    for (i, e) in r.events.iter().enumerate() {
        let Some(kind) = trusted_kind(dict, e) else {
            continue;
        };
        seen += 1;
        let Ok(idx) = u16::try_from(i) else {
            rejected += 1;
            continue;
        };
        if seen > MAX_EVENTS_PER_TX {
            rejected += 1;
            continue;
        }
        match value::from_slice(&e.params, dict)
            .ok()
            .and_then(|p| decode_event(kind, &p))
        {
            Some(ev) => out.push((idx, ev)),
            None => rejected += 1,
        }
    }
    (out, rejected)
}

// ---------------------------------------------------------------------------------------------
// Rows.

/// `ev` = TxKey ‖ event index.
pub fn ev_key(key: TxKey, idx: u16) -> [u8; 10] {
    let mut k = [0u8; 10];
    k[..8].copy_from_slice(&key.to_be_bytes());
    k[8..].copy_from_slice(&idx.to_be_bytes());
    k
}

/// `DexPairs` key.
pub fn pair_row_key(pair: &str, key: TxKey, idx: u16) -> Vec<u8> {
    let mut k = Vec::with_capacity(pair.len() + 11);
    k.extend_from_slice(pair.as_bytes());
    k.push(0);
    k.extend_from_slice(&ev_key(key, idx));
    k
}

/// `DexEvents` key prefix of one pair and kind.
pub fn event_prefix(pair: &str, kind: Kind) -> Vec<u8> {
    let mut k = Vec::with_capacity(pair.len() + 12);
    k.extend_from_slice(pair.as_bytes());
    k.push(0);
    k.push(kind as u8);
    k
}

/// `DexEvents` key.
pub fn event_key(pair: &str, kind: Kind, key: TxKey, idx: u16) -> Vec<u8> {
    let mut k = event_prefix(pair, kind);
    k.extend_from_slice(&ev_key(key, idx));
    k
}

/// `DexLp` key.
pub fn lp_key(pair: &str, account: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(pair.len() + account.len() + 1);
    k.extend_from_slice(pair.as_bytes());
    k.push(0);
    k.extend_from_slice(account.as_bytes());
    k
}

/// `DexAgg` key of a pair's LP supply.
pub fn supply_key(pair: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(pair.len() + 1);
    k.push(1);
    k.extend_from_slice(pair.as_bytes());
    k
}

/// `DexAgg` key of a pair's hour bucket.
pub fn bucket_key(pair: &str, hour: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(pair.len() + 6);
    k.push(2);
    k.extend_from_slice(pair.as_bytes());
    k.push(0);
    k.extend_from_slice(&hour.to_be_bytes());
    k
}

/// The hour bucket of a block creation time (µs).
pub fn hour_of(block_time_us: u64) -> u32 {
    u32::try_from(block_time_us / 3_600_000_000).unwrap_or(u32::MAX)
}

/// The hour of a stored event: its tx's `meta.creationTime` (stock's time), else its block's.
pub fn event_hour(e: &StoredEvent) -> u32 {
    if e.tx_time > 0 {
        u32::try_from(e.tx_time / 3_600).unwrap_or(u32::MAX)
    } else {
        hour_of(e.time)
    }
}

/// `DexAgg` key of a pair's event count of one kind.
pub fn count_key(pair: &str, kind: Kind) -> Vec<u8> {
    let mut k = Vec::with_capacity(pair.len() + 3);
    k.push(4);
    k.extend_from_slice(pair.as_bytes());
    k.push(0);
    k.push(kind as u8);
    k
}

/// `DexAgg` prefix of a token's pairs (oldest `CREATE_PAIR` first).
pub fn token_prefix(token: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(token.len() + 12);
    k.push(5);
    k.extend_from_slice(token.as_bytes());
    k.push(0);
    k
}

fn token_key(token: &str, key: TxKey, idx: u16) -> Vec<u8> {
    let mut k = token_prefix(token);
    k.extend_from_slice(&ev_key(key, idx));
    k
}

/// Kinds with a stored count ([`count_key`]).
pub fn counted(kind: Kind) -> bool {
    matches!(
        kind,
        Kind::Swap | Kind::AddLiquidity | Kind::RemoveLiquidity
    )
}

/// The number of stored events of `kind` of a pair (`None` when the row is unreadable).
pub fn event_count(txn: &impl Txn, pair: &str, kind: Kind) -> Result<Option<i64>> {
    Ok(match txn.get(Table::DexAgg, &count_key(pair, kind))? {
        None => Some(0),
        Some(v) => <[u8; 8]>::try_from(v.as_slice())
            .ok()
            .map(i64::from_be_bytes),
    })
}

fn bump_count(txn: &mut RwTxn<'_>, pair: &str, kind: Kind, d: i64) -> Fx {
    if !counted(kind) {
        return Ok(true);
    }
    let Some(n) = event_count(txn, pair, kind)? else {
        return Ok(false);
    };
    let k = count_key(pair, kind);
    match n.wrapping_add(d) {
        0 => {
            txn.del(Table::DexAgg, &k)?;
        }
        n => txn.put(Table::DexAgg, &k, &n.to_be_bytes())?,
    }
    Ok(true)
}

/// Write (`del` false) or delete the token-index rows of a `CREATE_PAIR`.
fn index_pair(txn: &mut RwTxn<'_>, pair: &str, key: TxKey, idx: u16, del: bool) -> Result<()> {
    for t in pair.split(':') {
        let k = token_key(t, key, idx);
        if del {
            txn.del(Table::DexAgg, &k)?;
        } else {
            txn.put(Table::DexAgg, &k, pair.as_bytes())?;
        }
    }
    Ok(())
}

/// A token's pairs from the token index, oldest `CREATE_PAIR` first, while `f(pair)` holds
/// (a pair created twice is passed twice).
pub fn token_pairs(txn: &impl Txn, token: &str, mut f: impl FnMut(&str) -> bool) -> Result<()> {
    let prefix = token_prefix(token);
    let mut go = true;
    let mut from = prefix.clone();
    // Chunks keep the walk lazy without holding a cursor across `f`.
    while go {
        let rows = txn.scan_limit(Table::DexAgg, &from, 256, |k| k.starts_with(&prefix))?;
        let Some(last) = rows.last().map(|r| r.0.clone()) else {
            break;
        };
        for (_, v) in &rows {
            if !f(&String::from_utf8_lossy(v)) {
                go = false;
                break;
            }
        }
        if rows.len() < 256 {
            break;
        }
        from = last;
        from.push(0);
    }
    Ok(())
}

/// `DexAgg` key of a coin pair's count of funded `UPDATE`s.
pub fn funded_key(pair: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(pair.len() + 1);
    k.push(6);
    k.extend_from_slice(pair.as_bytes());
    k
}

/// Add (`d` 1) or remove (`d` -1) a stored `UPDATE` from its coin pair's funded count. The
/// count depends only on the set of stored `UPDATE`s, so it is exact under unwind and in
/// any fill order. `Ok(false)`: an unreadable row (the caller poisons the pair).
fn bump_funded(txn: &mut RwTxn<'_>, pair: &str, e: &StoredEvent, d: i64) -> Fx {
    if e.kind != Kind::Update || !pair.split(':').any(|t| t == "coin") {
        return Ok(true);
    }
    let (Some(r0), Some(r1)) = (e.amount(0), e.amount(1)) else {
        return Ok(false);
    };
    if r0.is_zero() || r1.is_zero() {
        return Ok(true);
    }
    let k = funded_key(pair);
    let n = match txn.get(Table::DexAgg, &k)? {
        None => 0,
        Some(v) => match <[u8; 8]>::try_from(v.as_slice()) {
            Ok(b) => i64::from_be_bytes(b),
            Err(_) => return Ok(false),
        },
    };
    match n.wrapping_add(d) {
        0 => {
            txn.del(Table::DexAgg, &k)?;
        }
        n => txn.put(Table::DexAgg, &k, &n.to_be_bytes())?,
    }
    Ok(true)
}

/// Coin pairs with at least one funded `UPDATE` (key order) while `f(pair)` holds: the only
/// pairs that can be listable. A pair created and never funded is not here, however many.
pub fn funded_pairs(txn: &impl Txn, mut f: impl FnMut(&str) -> bool) -> Result<()> {
    txn.walk_keys(Table::DexAgg, &[6], |k| {
        k.first() == Some(&6) && f(&String::from_utf8_lossy(&k[1..]))
    })
}

/// Version of the derived DEX rows (2: buckets by tx time, event counts, token index,
/// funded coin pairs).
pub const LAYOUT: u32 = 2;

/// Clear the DEX tables of a store written under another [`LAYOUT`] and reset the history
/// fill, so it rebuilds them (`DexUsd` is kept). Returns whether it cleared.
pub fn ensure_layout(txn: &mut RwTxn<'_>) -> Result<bool> {
    if txn.meta(meta::DEX_LAYOUT)?.as_deref() == Some(&LAYOUT.to_be_bytes()[..]) {
        return Ok(false);
    }
    for t in TABLES {
        txn.clear(t)?;
    }
    txn.del(Table::Meta, meta::DEX_FILLED.as_bytes())?;
    txn.del(Table::Meta, meta::DEX_FILL_CURSOR.as_bytes())?;
    txn.set_meta(meta::DEX_LAYOUT, &LAYOUT.to_be_bytes())?;
    Ok(true)
}

/// A `DexEvents` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    pub kind: Kind,
    /// Block creation time, µs.
    pub time: u64,
    /// The tx's `meta.creationTime` (seconds, as the sender set it; 0 when absent): stock's
    /// `PoolTransaction.timestamp`.
    pub tx_time: u64,
    pub fields: Vec<String>,
}

impl StoredEvent {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![self.kind as u8];
        out.extend_from_slice(&self.time.to_be_bytes());
        out.extend_from_slice(&self.tx_time.to_be_bytes());
        for f in &self.fields {
            put_bytes(&mut out, f.as_bytes());
        }
        out
    }

    pub fn decode(v: &[u8]) -> Result<StoredEvent> {
        let kind = Kind::from_u8(*v.first().context("empty dex event")?).context("dex kind")?;
        let time = u64::from_be_bytes(v.get(1..9).context("dex time")?.try_into()?);
        let tx_time = u64::from_be_bytes(v.get(9..17).context("dex tx time")?.try_into()?);
        let mut b = &v[17..];
        let mut fields = Vec::with_capacity(kind.fields());
        while !b.is_empty() {
            fields.push(String::from_utf8(get_bytes(&mut b)?.to_vec()).context("dex field")?);
        }
        if fields.len() != kind.fields() {
            bail!("dex event {kind:?} has {} fields", fields.len());
        }
        Ok(StoredEvent {
            kind,
            time,
            tx_time,
            fields,
        })
    }

    /// Field `i` as an amount (`None` when absent or unreadable).
    pub fn amount(&self, i: usize) -> Option<Amount> {
        Amount::parse_stored(self.fields.get(i)?)
    }
}

/// A pair's hour bucket (`DexAgg` 0x02).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Bucket {
    /// Counts add and subtract with wrapping, so they stay exact under unwind in any order.
    pub swaps: i64,
    pub adds: i64,
    pub removes: i64,
    /// Swap inputs and outputs per side: `[in0, in1, out0, out1]`.
    pub sums: [Amount; 4],
}

impl Bucket {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        for n in [self.swaps, self.adds, self.removes] {
            out.extend_from_slice(&n.to_be_bytes());
        }
        for a in &self.sums {
            put_bytes(&mut out, a.to_text().as_bytes());
        }
        out
    }

    pub fn decode(v: &[u8]) -> Result<Bucket> {
        let n = |i: usize| -> Result<i64> {
            Ok(i64::from_be_bytes(
                v.get(i * 8..i * 8 + 8)
                    .context("bucket count")?
                    .try_into()?,
            ))
        };
        let mut b = v.get(24..).context("bucket")?;
        let mut sums: [Amount; 4] = Default::default();
        for s in &mut sums {
            let t = std::str::from_utf8(get_bytes(&mut b)?).context("bucket sum")?;
            *s = Amount::parse_stored(t).context("bucket sum")?;
        }
        Ok(Bucket {
            swaps: n(0)?,
            adds: n(1)?,
            removes: n(2)?,
            sums,
        })
    }
}

/// What [`apply_tx`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplyStats {
    /// Events stored.
    pub stored: u64,
    /// Trusted events left out (malformed, out of bounds or past the per-tx cap).
    pub rejected: u64,
    /// Events already stored (a replay).
    pub present: u64,
}

/// `DexAgg` key of a pair's poison mark.
pub fn poison_key(pair: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(pair.len() + 1);
    k.push(3);
    k.extend_from_slice(pair.as_bytes());
    k
}

/// Whether a pair is poisoned: a stored value of it could not be read back, so its aggregates
/// stopped and the serving part must report it as unavailable.
pub fn poisoned(txn: &impl Txn, pair: &str) -> Result<bool> {
    Ok(txn.get(Table::DexAgg, &poison_key(pair))?.is_some())
}

fn poison(txn: &mut RwTxn<'_>, pair: &str, at: &str) -> Result<()> {
    tracing::warn!(pair, at, "dex: unreadable stored value, pair poisoned");
    txn.put(Table::DexAgg, &poison_key(pair), &[])
}

/// The result of changing aggregates from stored data: `Ok(false)` when a stored value could
/// not be read or would not fit [`MAX_STORED_TEXT`] (the caller poisons the pair). `Err` is a
/// storage error only.
type Fx = Result<bool>;

fn add_amount_row(txn: &mut RwTxn<'_>, t: Table, k: &[u8], d: &Amount, sub: bool) -> Fx {
    let cur = match txn.get(t, k)? {
        Some(v) => match std::str::from_utf8(&v).ok().and_then(Amount::parse_stored) {
            Some(a) => a,
            None => return Ok(false),
        },
        None => Amount::zero(),
    };
    let new = cur.add(d, sub);
    if new.is_zero() {
        txn.del(t, k)?;
    } else {
        let text = new.to_text();
        if text.len() > MAX_STORED_TEXT {
            return Ok(false);
        }
        txn.put(t, k, text.as_bytes())?;
    }
    Ok(true)
}

/// Add (`sub` false) or remove (`sub` true) one stored event's effect on the aggregates.
fn effect(txn: &mut RwTxn<'_>, pair: &str, e: &StoredEvent, sub: bool) -> Fx {
    match e.kind {
        Kind::CreatePair | Kind::Update => {}
        Kind::Swap | Kind::AddLiquidity | Kind::RemoveLiquidity => {
            let k = bucket_key(pair, event_hour(e));
            let mut b = match txn.get(Table::DexAgg, &k)? {
                Some(v) => match Bucket::decode(&v) {
                    Ok(b) => b,
                    Err(_) => return Ok(false),
                },
                None => Bucket::default(),
            };
            let d: i64 = if sub { -1 } else { 1 };
            match e.kind {
                Kind::Swap => {
                    b.swaps = b.swaps.wrapping_add(d);
                    for i in 0..4 {
                        let Some(x) = e.amount(2 + i) else {
                            return Ok(false);
                        };
                        b.sums[i] = b.sums[i].add(&x, sub);
                    }
                }
                Kind::AddLiquidity => b.adds = b.adds.wrapping_add(d),
                _ => b.removes = b.removes.wrapping_add(d),
            }
            if b.swaps == 0 && b.adds == 0 && b.removes == 0 {
                txn.del(Table::DexAgg, &k)?;
            } else {
                if b.sums.iter().any(|s| s.to_text().len() > MAX_STORED_TEXT) {
                    return Ok(false);
                }
                txn.put(Table::DexAgg, &k, &b.encode())?;
            }
        }
        Kind::Mint | Kind::Burn => {
            let (Some(amt), Some(account)) = (e.amount(1), e.fields.first()) else {
                return Ok(false);
            };
            let up = sub != (e.kind == Kind::Burn);
            return Ok(
                add_amount_row(txn, Table::DexAgg, &supply_key(pair), &amt, up)?
                    && add_amount_row(txn, Table::DexLp, &lp_key(pair, account), &amt, up)?,
            );
        }
        Kind::Transfer => {
            let (Some(amt), Some(from), Some(to)) =
                (e.amount(2), e.fields.first(), e.fields.get(1))
            else {
                return Ok(false);
            };
            return Ok(
                add_amount_row(txn, Table::DexLp, &lp_key(pair, from), &amt, !sub)?
                    && add_amount_row(txn, Table::DexLp, &lp_key(pair, to), &amt, sub)?,
            );
        }
    }
    Ok(true)
}

/// Store one decoded event of the tx at `key` (block time `time`, µs; tx `meta.creationTime`
/// `tx_time`), once: a replay of a stored event changes nothing. Returns whether it was
/// stored. A pair whose stored values cannot be read is poisoned, never an error.
pub fn store_event(
    txn: &mut RwTxn<'_>,
    key: TxKey,
    idx: u16,
    time: u64,
    tx_time: u64,
    ev: &DexEvent,
) -> Result<bool> {
    let mut by = vec![ev.kind as u8];
    by.extend_from_slice(ev.pair.as_bytes());
    if !txn.insert(Table::DexByTx, &ev_key(key, idx), &by)? {
        return Ok(false);
    }
    let row = StoredEvent {
        kind: ev.kind,
        time,
        tx_time,
        fields: ev.fields.clone(),
    };
    txn.put(
        Table::DexEvents,
        &event_key(&ev.pair, ev.kind, key, idx),
        &row.encode(),
    )?;
    if ev.kind == Kind::CreatePair {
        txn.put(Table::DexPairs, &pair_row_key(&ev.pair, key, idx), &[])?;
        index_pair(txn, &ev.pair, key, idx, false)?;
    }
    // Counts follow the stored rows (poisoned or not), so they stay exact under unwind.
    if !bump_count(txn, &ev.pair, ev.kind, 1)? || !bump_funded(txn, &ev.pair, &row, 1)? {
        poison(txn, &ev.pair, "count")?;
    }
    if !poisoned(txn, &ev.pair)? && !effect(txn, &ev.pair, &row, false)? {
        poison(txn, &ev.pair, "apply")?;
    }
    Ok(true)
}

/// Store the trusted Sushi events of a tx in block time `time` (µs). Idempotent. Never fails
/// on DEX data (only on a storage error).
pub fn apply_tx(
    txn: &mut RwTxn<'_>,
    dict: &dyn Dict,
    r: &TxRecord,
    time: u64,
) -> Result<ApplyStats> {
    let mut st = ApplyStats::default();
    let (evs, rejected) = events_of_tx(dict, r);
    st.rejected = rejected;
    if evs.is_empty() {
        return Ok(st);
    }
    let tx_time = crate::prices::creation_time(r.cmd.as_deref());
    for (idx, ev) in &evs {
        if store_event(txn, r.key, *idx, time, tx_time, ev)? {
            st.stored += 1;
        } else {
            st.present += 1;
        }
    }
    Ok(st)
}

/// A `DexByTx` row: `(tx, event index, kind, pair)`.
fn by_tx_row(k: &[u8], v: &[u8]) -> Option<(TxKey, u16, Kind, String)> {
    let key = TxKey::from_be_bytes(k.get(..8)?.try_into().ok()?);
    let idx = u16::from_be_bytes(k.get(8..10)?.try_into().ok()?);
    let kind = Kind::from_u8(*v.first()?)?;
    let pair = std::str::from_utf8(v.get(1..)?).ok()?.to_owned();
    Some((key, idx, kind, pair))
}

/// Remove every Sushi event stored for the block at `(chain, height)` and reverse its effect,
/// newest first. Returns the events removed. Never fails on DEX data (only on a storage
/// error): a row that cannot be read is removed and its pair poisoned.
pub fn undo_block(txn: &mut RwTxn<'_>, chain: u8, height: u32) -> Result<u32> {
    if !txn.has_table(Table::DexByTx) {
        return Ok(0);
    }
    let mut prefix = [0u8; 5];
    prefix[..4].copy_from_slice(&height.to_be_bytes());
    prefix[4] = chain;
    let rows = txn.scan(Table::DexByTx, &prefix, |k| k.starts_with(&prefix))?;
    let mut n = 0;
    for (k, v) in rows.iter().rev() {
        txn.del(Table::DexByTx, k)?;
        n += 1;
        let Some((key, idx, kind, pair)) = by_tx_row(k, v) else {
            orphan_events(txn, k)?;
            continue;
        };
        let ek = event_key(&pair, kind, key, idx);
        let row = txn.get(Table::DexEvents, &ek)?;
        txn.del(Table::DexEvents, &ek)?;
        if kind == Kind::CreatePair {
            txn.del(Table::DexPairs, &pair_row_key(&pair, key, idx))?;
            index_pair(txn, &pair, key, idx, true)?;
        }
        if !bump_count(txn, &pair, kind, -1)? {
            poison(txn, &pair, "count")?;
        }
        let ok = match row.as_deref().map(StoredEvent::decode) {
            Some(Ok(row)) if row.kind == kind => {
                bump_funded(txn, &pair, &row, -1)?
                    && (poisoned(txn, &pair)? || effect(txn, &pair, &row, true)?)
            }
            _ => false,
        };
        if !ok {
            poison(txn, &pair, "unwind")?;
        }
    }
    Ok(n)
}

/// Unwind of an unreadable `DexByTx` row (key `ev` = `k`): find its `DexEvents` rows by a
/// full key walk (only a damaged store gets here), read each, remove it, take it out of its
/// pair's event count and funded count (so a drained spam pair cannot stay a listing
/// candidate), and poison its pair. A `DexEvents` row that cannot be read cannot be
/// uncounted (its funded state is unknown); its pair is poisoned, so it is never listed or
/// priced whatever its counts say.
fn orphan_events(txn: &mut RwTxn<'_>, k: &[u8]) -> Result<()> {
    tracing::warn!(?k, "dex: unreadable DexByTx row removed");
    let Some(ev) = k.get(..10) else {
        return Ok(());
    };
    let mut found = Vec::new();
    txn.walk_keys(Table::DexEvents, &[], |key| {
        if key.len() > 12 && key.ends_with(ev) {
            found.push(key.to_vec());
        }
        true
    })?;
    for ek in found {
        let row = txn.get(Table::DexEvents, &ek)?;
        txn.del(Table::DexEvents, &ek)?;
        let Some(z) = ek.iter().position(|b| *b == 0) else {
            continue;
        };
        let Ok(pair) = std::str::from_utf8(&ek[..z]) else {
            continue;
        };
        let kind = ek.get(z + 1).copied().and_then(Kind::from_u8);
        if let Some(kind) = kind {
            // Best effort: the pair is poisoned below whatever these return.
            bump_count(txn, pair, kind, -1)?;
        }
        if let Some(Ok(e)) = row.as_deref().map(StoredEvent::decode) {
            if Some(e.kind) == kind {
                bump_funded(txn, pair, &e, -1)?;
            }
        }
        if ek.get(z + 1) == Some(&(Kind::CreatePair as u8)) {
            let mut pk = ek[..=z].to_vec();
            pk.extend_from_slice(ev);
            txn.del(Table::DexPairs, &pk)?;
            if let (Ok(a), Ok(b)) = (<[u8; 8]>::try_from(&ev[..8]), <[u8; 2]>::try_from(&ev[8..])) {
                index_pair(
                    txn,
                    pair,
                    TxKey::from_be_bytes(a),
                    u16::from_be_bytes(b),
                    true,
                )?;
            }
        }
        poison(txn, pair, "unwind")?;
    }
    Ok(())
}

/// What the serving part needs to leave junk pairs out of listings (ingest stores them all).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PairStatus {
    /// A `CREATE_PAIR` is stored.
    pub created: bool,
    /// See [`poisoned`].
    pub poisoned: bool,
    /// One token is `coin` (so the pair has a KDA price leg).
    pub coin_leg: bool,
    /// The latest `UPDATE`'s reserves, oriented to the pair.
    pub reserves: Option<(Amount, Amount)>,
}

impl PairStatus {
    /// The suggested listing rule: created, not poisoned, a `coin` leg and non-zero reserves.
    pub fn listable(&self) -> bool {
        self.created
            && !self.poisoned
            && self.coin_leg
            && self
                .reserves
                .as_ref()
                .is_some_and(|(a, b)| !a.is_zero() && !b.is_zero())
    }
}

/// A pair's [`PairStatus`] (three short reads).
pub fn pair_status(txn: &impl Txn, pair: &str) -> Result<PairStatus> {
    let mut p = pair.as_bytes().to_vec();
    p.push(0);
    let created = !txn
        .scan_limit(Table::DexPairs, &p, 1, |k| k.starts_with(&p))?
        .is_empty();
    Ok(PairStatus {
        created,
        poisoned: poisoned(txn, pair)?,
        coin_leg: pair.split(':').any(|t| t == "coin"),
        reserves: latest_reserves(txn, pair)?,
    })
}

/// The reserves of a pair's latest stored `UPDATE`, oriented to the pair (one read; `None`
/// when there is none or it is unreadable). Listings rank funded pairs by it without reading
/// the rest of [`pair_status`].
pub fn latest_reserves(txn: &impl Txn, pair: &str) -> Result<Option<(Amount, Amount)>> {
    Ok(
        match txn
            .scan_rev_prefix(Table::DexEvents, &event_prefix(pair, Kind::Update), &[])?
            .next()
        {
            Some(row) => StoredEvent::decode(&row?.1)
                .ok()
                .and_then(|e| Some((e.amount(0)?, e.amount(1)?))),
            None => None,
        },
    )
}

/// Tables this module writes (the serving part also reads `DexUsd`).
pub const TABLES: [Table; 5] = [
    Table::DexPairs,
    Table::DexEvents,
    Table::DexByTx,
    Table::DexLp,
    Table::DexAgg,
];

pub mod meta {
    /// Set when the history fill finished (u32: the highest tail height then).
    pub const DEX_FILLED: &str = "dex_filled";
    /// The fill's resume point: the last segment TxNum done (u64).
    pub const DEX_FILL_CURSOR: &str = "dex_fill_txnum";
    /// [`super::LAYOUT`] of the stored derived rows (u32).
    pub const DEX_LAYOUT: &str = "dex_layout";
}

#[cfg(test)]
mod tests;
