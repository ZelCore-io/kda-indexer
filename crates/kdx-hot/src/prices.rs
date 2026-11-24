//! Last DEX swap per token, for GraphQL `lastTokenPriceInKda` (additive table `TokenSwaps`).
//!
//! Stock semantics (indexer-kadena `90a4be1`, `last-token-price-in-kda-resolver.ts`): the newest
//! transaction (`ORDER BY creationtime DESC, id DESC`) whose `result` text contains
//! `moduleName` (SQL `LIKE '%' || $1 || '%'`); when its `result.data[0].token` and
//! `result.data[1].token` are set, the answer is `kda / token` with `kda` the amount of the
//! element whose token is `coin` when that is element 0, else element 1, computed with
//! decimal.js (`div` at 20 significant digits, then `toFixed(12)`, both ROUND_HALF_UP), and sent
//! as a string. Otherwise the answer is null.
//!
//! What we keep instead of scanning every result (rulings in `docs/graphql.md`):
//! - a **swap** is a successful tx with a [`DEX_SWAP_EVENTS`] event (Kaddex/eckoDEX
//!   `kaddex.exchange.SWAP`, KDSwap `kdlaunch.kdswap-exchange.SWAP`) whose
//!   `result.data` is a list with a non-empty string `token` in elements 0 and 1;
//! - one row per (chain, token) for every string `token` in the list (a routed swap names 3+
//!   tokens; stock matches all of them by text): the swap's order key and elements 0 and 1;
//! - order key `(meta.creationTime, height, chain, position)`, newest wins;
//! - a lookup matches `moduleName` against the stored token names as stock's `LIKE` pattern
//!   (`%` and `_` wildcards, `\` escape) and takes the newest matching row; it reads at most
//!   [`LIKE_SCAN_ROWS`] rows, and on a larger table matches the exact name (a prefix range).
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | `TokenSwaps` | token (utf-8), chain u8 | creationTime u64, TxKey u64, then 4 × varint-length bytes: token 0, amount 0 JSON, token 1, amount 1 JSON |
//!
//! Reorgs: every row a tail block writes is logged with its previous value in
//! `Meta price_undo/{chain}/{height}` (like the D3 marks); an unwind restores them newest first,
//! the freezer drops the log with the block.

use anyhow::{Context, Result};
use kdx_types::record::{EventRec, TxRecord};
use kdx_types::varint::{get_bytes, get_u8, put_bytes};
use kdx_types::{value, Dict, DictKind, TxKey};
use serde_json::Value;

use crate::env::{RwTxn, Txn};
use crate::tables::Table;

pub mod meta {
    /// Prefix of a tail block's undo log (`price_undo/{chain:02}/{height:010}`).
    pub const PRICE_UNDO: &str = "price_undo/";
    /// Set when the history fill finished (u32: the highest tail height then).
    pub const PRICE_FILLED: &str = "price_filled";
    /// The fill's resume point: the last segment TxNum done (u64).
    pub const PRICE_FILL_CURSOR: &str = "price_fill_txnum";
}

/// `TokenSwaps` key.
pub fn row_key(chain: u8, token: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(token.len() + 1);
    k.extend_from_slice(token.as_bytes());
    k.push(chain);
    k
}

/// The DEX swap events we trust (security review: swap events are written by anyone's
/// module, so only these exchanges feed `TokenSwaps`). Kaddex/eckoDEX and KDSwap.
pub const DEX_SWAP_EVENTS: &[&str] = &["kaddex.exchange.SWAP", "kdlaunch.kdswap-exchange.SWAP"];
/// Longest token name stored (longer names are skipped; Pact module names are far shorter).
pub const MAX_TOKEN_LEN: usize = 256;
/// Most tokens one swap may update (a routed swap names one per hop).
pub const MAX_TOKENS_PER_SWAP: usize = 8;
/// Rows a `LIKE` lookup may read; above it the lookup matches the exact name only.
pub const LIKE_SCAN_ROWS: usize = 20_000;

pub fn undo_key(chain: u8, height: u32) -> String {
    format!("{}{chain:02}/{height:010}", meta::PRICE_UNDO)
}

/// One stored swap: its order key and the first two `{token, amount}` elements of its result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwapRow {
    /// `meta.creationTime` of the tx (seconds).
    pub ctime: u64,
    pub key: TxKey,
    /// `(token, amount as JSON text)` of `result.data[0]` and `[1]`.
    pub legs: [(String, String); 2],
}

impl SwapRow {
    /// Newest-first order: creation time, then height, chain, position.
    pub fn order(&self) -> (u64, u32, u8, u16) {
        (
            self.ctime,
            self.key.height(),
            self.key.chain(),
            self.key.pos(),
        )
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 + 64);
        out.extend_from_slice(&self.ctime.to_be_bytes());
        out.extend_from_slice(&self.key.to_be_bytes());
        for (t, a) in &self.legs {
            put_bytes(&mut out, t.as_bytes());
            put_bytes(&mut out, a.as_bytes());
        }
        out
    }

    pub fn decode(v: &[u8]) -> Option<SwapRow> {
        let ctime = u64::from_be_bytes(v.get(..8)?.try_into().ok()?);
        let key = TxKey::from_be_bytes(v.get(8..16)?.try_into().ok()?);
        let mut b = v.get(16..)?;
        let mut s =
            || -> Option<String> { String::from_utf8(get_bytes(&mut b).ok()?.to_vec()).ok() };
        let legs = [(s()?, s()?), (s()?, s()?)];
        b.is_empty().then_some(SwapRow { ctime, key, legs })
    }
}

/// A swap found in a tx: the row to store and every token its result names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Swap {
    pub row: SwapRow,
    pub tokens: Vec<String>,
}

/// An event name (`<module>.<NAME>`) of a trusted DEX swap ([`DEX_SWAP_EVENTS`]).
pub fn is_swap_event_name(name: &str) -> bool {
    DEX_SWAP_EVENTS.contains(&name)
}

pub fn has_swap_event(dict: &dyn Dict, events: &[EventRec]) -> bool {
    events.iter().any(|e| {
        dict.name(DictKind::EventName, e.name)
            .is_some_and(is_swap_event_name)
    })
}

/// `meta.creationTime` of a signed `cmd` (0 when missing or not a number).
pub fn creation_time(cmd: Option<&[u8]>) -> u64 {
    let Some(v) = cmd.and_then(|c| serde_json::from_slice::<Value>(c).ok()) else {
        return 0;
    };
    let Some(n) = v.pointer("/meta/creationTime") else {
        return 0;
    };
    n.as_u64()
        .or_else(|| n.as_f64().filter(|f| *f >= 0.0).map(|f| f as u64))
        .unwrap_or(0)
}

fn token_of(e: &Value) -> Option<&str> {
    e.get("token")?
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= MAX_TOKEN_LEN)
}

/// The swap in a successful tx's `result.data`, when elements 0 and 1 carry a token.
pub fn swap_of(key: TxKey, ctime: u64, data: &Value) -> Option<Swap> {
    let list = data.as_array()?;
    let (e0, e1) = (list.first()?, list.get(1)?);
    let (t0, t1) = (token_of(e0)?, token_of(e1)?);
    let amount = |e: &Value| e.get("amount").map(|a| a.to_string()).unwrap_or_default();
    let mut tokens: Vec<String> = Vec::new();
    for t in list.iter().filter_map(token_of) {
        if tokens.len() == MAX_TOKENS_PER_SWAP {
            break;
        }
        if !tokens.iter().any(|x| x == t) {
            tokens.push(t.to_owned());
        }
    }
    Some(Swap {
        row: SwapRow {
            ctime,
            key,
            legs: [(t0.to_owned(), amount(e0)), (t1.to_owned(), amount(e1))],
        },
        tokens,
    })
}

/// The swap in a stored tx: successful, with a `*.SWAP` event, and a swap-shaped result.
pub fn swap_of_tx(dict: &dyn Dict, r: &TxRecord) -> Option<Swap> {
    if !r.status_ok || r.key.is_coinbase() || !has_swap_event(dict, &r.events) {
        return None;
    }
    let data = value::from_slice(&r.result, dict).ok()?;
    swap_of(r.key, creation_time(r.cmd.as_deref()), &data)
}

/// Store `swap` for each of its tokens on `chain` where it is newer than the stored row.
/// With `undo`, every replaced row is logged (key, previous value) for [`undo_block`].
/// Returns the rows written. Replaying a swap writes nothing.
pub fn upsert(
    txn: &mut RwTxn<'_>,
    chain: u8,
    swap: &Swap,
    mut undo: Option<&mut Vec<u8>>,
) -> Result<u32> {
    let val = swap.row.encode();
    let mut n = 0;
    for t in &swap.tokens {
        let k = row_key(chain, t);
        let prev = txn.get(Table::TokenSwaps, &k)?;
        if let Some(p) = prev.as_deref().and_then(SwapRow::decode) {
            if p.order() >= swap.row.order() {
                continue;
            }
        }
        if let Some(u) = undo.as_deref_mut() {
            put_bytes(u, &k);
            match &prev {
                Some(p) => {
                    u.push(1);
                    put_bytes(u, p);
                }
                None => u.push(0),
            }
        }
        txn.put(Table::TokenSwaps, &k, &val)?;
        n += 1;
    }
    Ok(n)
}

/// Reverse the rows a tail block wrote (its `price_undo` log), newest first, and drop the log.
pub fn undo_block(txn: &mut RwTxn<'_>, chain: u8, height: u32) -> Result<u32> {
    let k = undo_key(chain, height);
    let Some(v) = txn.meta(&k)? else {
        return Ok(0);
    };
    let mut entries = Vec::new();
    let mut b = v.as_slice();
    while !b.is_empty() {
        let key = get_bytes(&mut b).context("price undo key")?.to_vec();
        let prev = match get_u8(&mut b).context("price undo flag")? {
            0 => None,
            _ => Some(get_bytes(&mut b).context("price undo value")?.to_vec()),
        };
        entries.push((key, prev));
    }
    let n = entries.len() as u32;
    for (key, prev) in entries.into_iter().rev() {
        match prev {
            Some(p) => txn.put(Table::TokenSwaps, &key, &p)?,
            None => {
                txn.del(Table::TokenSwaps, &key)?;
            }
        }
    }
    txn.del(Table::Meta, k.as_bytes())?;
    Ok(n)
}

/// The newest stored swap whose token matches `module` as stock's `LIKE '%module%'`.
/// Bounded: reads at most [`LIKE_SCAN_ROWS`] + 1 rows; when the table is larger, only rows
/// whose token equals `module` (one key range) are considered.
pub fn lookup(txn: &impl Txn, module: &str) -> Result<Option<SwapRow>> {
    let all = txn.scan_limit(Table::TokenSwaps, &[], LIKE_SCAN_ROWS + 1, |_| true)?;
    let (rows, exact) = if all.len() <= LIKE_SCAN_ROWS {
        (all, false)
    } else {
        let p = module.as_bytes().to_vec();
        let rows = txn.scan_limit(Table::TokenSwaps, &p, 256, |k| k.starts_with(&p))?;
        (rows, true)
    };
    let mut best: Option<SwapRow> = None;
    for (k, v) in rows {
        let Some(token) = k
            .split_last()
            .and_then(|(_, t)| std::str::from_utf8(t).ok())
        else {
            continue;
        };
        let hit = if exact {
            token == module
        } else {
            like_contains(token, module)
        };
        if !hit {
            continue;
        }
        let Some(row) = SwapRow::decode(&v) else {
            continue;
        };
        if best.as_ref().is_none_or(|b| row.order() > b.order()) {
            best = Some(row);
        }
    }
    Ok(best)
}

/// SQL `text LIKE '%' || pattern || '%'` (PostgreSQL: `%` any run, `_` one character,
/// `\` escapes the next character; case-sensitive).
pub fn like_contains(text: &str, pattern: &str) -> bool {
    // Tokens: Some(c) a literal, None `_`; `%` splits; leading and trailing `%` are implied.
    #[derive(Clone, Copy, PartialEq)]
    enum P {
        Lit(char),
        One,
        Any,
    }
    let mut p = vec![P::Any];
    let mut it = pattern.chars();
    while let Some(c) = it.next() {
        p.push(match c {
            '%' => P::Any,
            '_' => P::One,
            '\\' => P::Lit(it.next().unwrap_or('\\')),
            c => P::Lit(c),
        });
    }
    p.push(P::Any);
    let t: Vec<char> = text.chars().collect();
    // dp[j]: pattern prefix of length j matches the text prefix seen so far.
    let mut dp = vec![false; p.len() + 1];
    dp[0] = true;
    for j in 1..=p.len() {
        dp[j] = dp[j - 1] && p[j - 1] == P::Any;
    }
    for &c in &t {
        let mut next = vec![false; p.len() + 1];
        for j in 1..=p.len() {
            next[j] = match p[j - 1] {
                P::Any => next[j - 1] || dp[j],
                P::One => dp[j - 1],
                P::Lit(l) => dp[j - 1] && l == c,
            };
        }
        dp = next;
    }
    dp[p.len()]
}

// ---------------------------------------------------------------------------------------
// Price arithmetic: decimal.js as the stock resolver uses it.

/// Longest amount text read (a JS double prints at most ~330 characters).
const MAX_AMOUNT_TEXT: usize = 1024;
/// Most significant digits an amount may have (decimal.js divides at 20).
const MAX_SIG_DIGITS: usize = 80;
/// Largest decimal exponent of an amount, either way.
const MAX_EXP: i64 = 1000;
/// Most integer digits a price may print; a larger one is answered `null`.
const MAX_INT_DIGITS: i64 = 400;

/// A decimal `digits × 10^exp` (digits most significant first, no leading zeros; empty = 0).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Dec {
    neg: bool,
    digits: Vec<u8>,
    exp: i64,
}

impl Dec {
    /// `None` beyond [`MAX_AMOUNT_TEXT`], [`MAX_SIG_DIGITS`] or [`MAX_EXP`] (security
    /// review: an unbounded exponent or digit string made division and rendering unbounded).
    fn parse(s: &str) -> Option<Dec> {
        if s.len() > MAX_AMOUNT_TEXT {
            return None;
        }
        let s = s.trim();
        let (neg, s) = match s.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, s.strip_prefix('+').unwrap_or(s)),
        };
        let (mant, e) = match s.find(['e', 'E']) {
            Some(i) => (
                &s[..i],
                s[i + 1..]
                    .parse::<i64>()
                    .ok()
                    .filter(|e| e.abs() <= 4 * MAX_EXP)?,
            ),
            None => (s, 0),
        };
        let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
        if int.is_empty() && frac.is_empty() {
            return None;
        }
        let mut digits = Vec::with_capacity(int.len() + frac.len());
        for c in int.chars().chain(frac.chars()) {
            digits.push(c.to_digit(10)? as u8);
        }
        let mut d = Dec {
            neg,
            digits,
            exp: e.checked_sub(frac.len() as i64)?,
        };
        d.normalize();
        (d.digits.len() <= MAX_SIG_DIGITS && d.exp.abs() <= MAX_EXP).then_some(d)
    }

    fn normalize(&mut self) {
        let lead = self.digits.iter().take_while(|&&d| d == 0).count();
        self.digits.drain(..lead);
        while self.digits.last() == Some(&0) {
            self.digits.pop();
            self.exp += 1;
        }
    }

    fn is_zero(&self) -> bool {
        self.digits.is_empty()
    }

    /// Keep at most `keep` leading digits, rounding half up on the first dropped one.
    fn round_digits(&mut self, keep: usize) {
        if self.digits.len() <= keep {
            return;
        }
        let up = self.digits[keep] >= 5;
        let dropped = self.digits.len() - keep;
        self.digits.truncate(keep);
        self.exp += dropped as i64;
        if up {
            let mut i = self.digits.len();
            loop {
                if i == 0 {
                    self.digits.insert(0, 1);
                    break;
                }
                i -= 1;
                if self.digits[i] == 9 {
                    self.digits[i] = 0;
                } else {
                    self.digits[i] += 1;
                    break;
                }
            }
        }
        self.normalize();
    }

    /// `self / d` rounded to `sig` significant digits, half up (decimal.js `div`).
    fn div(&self, d: &Dec, sig: usize) -> Dec {
        let neg = self.neg != d.neg;
        if self.is_zero() {
            return Dec {
                neg,
                digits: vec![],
                exp: 0,
            };
        }
        // Long division of the digit strings: remainder × 10 + next digit, one quotient digit
        // per step, until `sig + 1` significant digits (the extra one decides the rounding).
        let mut rem: Vec<u8> = Vec::new();
        let mut q: Vec<u8> = Vec::new();
        let mut i = 0usize;
        let mut exp = self.exp - d.exp;
        loop {
            if i < self.digits.len() {
                rem.push(self.digits[i]);
            } else {
                if is_zero_digits(&rem) {
                    break;
                }
                rem.push(0);
                exp -= 1;
            }
            i += 1;
            let mut qd = 0u8;
            while cmp_digits(&rem, &d.digits) != std::cmp::Ordering::Less {
                sub_digits(&mut rem, &d.digits);
                qd += 1;
            }
            q.push(qd);
            let sig_now = q.len() - q.iter().take_while(|&&x| x == 0).count();
            if sig_now > sig && i >= self.digits.len() {
                break;
            }
        }
        // Digits past the dividend's length shifted the exponent; those within it did not.
        let mut out = Dec {
            neg,
            digits: q,
            exp,
        };
        out.normalize();
        out.round_digits(sig);
        out
    }

    /// decimal.js `toFixed(dp)` (half up); `None` past [`MAX_INT_DIGITS`] integer digits.
    fn to_fixed(&self, dp: u32) -> Option<String> {
        if self.digits.len() as i64 + self.exp > MAX_INT_DIGITS {
            return None;
        }
        let mut r = self.clone();
        let min_exp = -(dp as i64);
        if r.exp < min_exp {
            let keep = r.digits.len() as i64 - (min_exp - r.exp);
            if keep < 0 {
                r.digits.clear();
                r.exp = min_exp;
            } else {
                // keep = 0 rounds to 0 or to one unit in the last place.
                r.round_digits(keep as usize);
            }
        }
        // Render digits × 10^exp with exactly `dp` fraction digits.
        let mut s: String = r.digits.iter().map(|d| (b'0' + d) as char).collect();
        if s.is_empty() {
            s.push('0');
            r.exp = 0;
        }
        let frac_len = dp as i64;
        if r.exp >= 0 {
            s.extend(std::iter::repeat_n('0', r.exp as usize));
            s.push('.');
            s.extend(std::iter::repeat_n('0', frac_len as usize));
        } else {
            let point = s.len() as i64 + r.exp; // digits before the point
            if point <= 0 {
                let mut t = String::from("0.");
                t.extend(std::iter::repeat_n('0', (-point) as usize));
                t.push_str(&s);
                s = t;
            } else {
                s.insert(point as usize, '.');
            }
            let have = s.len() - s.find('.').expect("point") - 1;
            s.extend(std::iter::repeat_n(
                '0',
                (frac_len as usize).saturating_sub(have),
            ));
        }
        Some(if self.neg && !self.is_zero() {
            format!("-{s}")
        } else {
            s
        })
    }
}

fn is_zero_digits(a: &[u8]) -> bool {
    a.iter().all(|&d| d == 0)
}

fn cmp_digits(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let a = &a[a.iter().take_while(|&&d| d == 0).count()..];
    let b = &b[b.iter().take_while(|&&d| d == 0).count()..];
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

/// `a -= b` (a >= b), in place, keeping `a`'s length.
fn sub_digits(a: &mut [u8], b: &[u8]) {
    let mut borrow = 0i8;
    let (la, lb) = (a.len(), b.len());
    for i in 0..la {
        let bd = if i < lb { b[lb - 1 - i] as i8 } else { 0 };
        let mut x = a[la - 1 - i] as i8 - bd - borrow;
        borrow = 0;
        if x < 0 {
            x += 10;
            borrow = 1;
        }
        a[la - 1 - i] = x as u8;
    }
}

/// An amount as the stock resolver reads it: `amount.decimal ?? amount.integer ?? amount`,
/// where a JSON number went through JavaScript (`JSON.parse`, so an IEEE double, then
/// `Number#toString`) and a string is exact. `None` for anything decimal.js would refuse.
fn amount_dec(json: &str) -> Option<Dec> {
    let a: Value = serde_json::from_str(json).ok()?;
    fn pick<'a>(o: &'a serde_json::Map<String, Value>, f: &str) -> Option<&'a Value> {
        o.get(f).filter(|v| !v.is_null())
    }
    let v = match &a {
        Value::Object(o) => pick(o, "decimal")
            .or_else(|| pick(o, "integer"))
            .unwrap_or(&a)
            .clone(),
        _ => a.clone(),
    };
    match v {
        Value::Number(n) => {
            let f: f64 = n.to_string().parse().ok()?;
            f.is_finite().then(|| Dec::parse(&format!("{f}")))?
        }
        Value::String(s) => Dec::parse(&s),
        _ => None,
    }
}

/// The stock answer for a stored swap: `kda / token` as `toFixed(12)` text, where `kda` is
/// leg 0 when leg 0's token is `coin`, else leg 1. `None` when an amount is unreadable.
/// A zero token amount gives decimal.js's `Infinity` / `NaN` text, as stock sends.
pub fn price_text(row: &SwapRow) -> Option<String> {
    let first_is_kda = row.legs[0].0 == "coin";
    let (kda, tok) = if first_is_kda {
        (&row.legs[0].1, &row.legs[1].1)
    } else {
        (&row.legs[1].1, &row.legs[0].1)
    };
    let (k, t) = (amount_dec(kda)?, amount_dec(tok)?);
    if t.is_zero() {
        return Some(match (k.is_zero(), k.neg != t.neg) {
            (true, _) => "NaN".into(),
            (false, false) => "Infinity".into(),
            (false, true) => "-Infinity".into(),
        });
    }
    k.div(&t, 20).to_fixed(12)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{Geometry, HotStore};

    fn row(ctime: u64, key: TxKey, l0: (&str, &str), l1: (&str, &str)) -> SwapRow {
        SwapRow {
            ctime,
            key,
            legs: [(l0.0.into(), l0.1.into()), (l1.0.into(), l1.1.into())],
        }
    }

    fn price(l0: (&str, &str), l1: (&str, &str)) -> Option<String> {
        price_text(&row(1, TxKey::new(1, 0, 1), l0, l1))
    }

    #[test]
    fn price_math_matches_decimal_js() {
        // kb-USDC -> KDA swap from chain 2 (KDA second): 164.195661313882 / 0.82543.
        assert_eq!(
            price(("n_e5.kb-USDC", "0.82543"), ("coin", "164.195661313882")).as_deref(),
            Some("198.921363790851")
        );
        // KDA first: 689.006542078688 / 67 (an integer amount).
        assert_eq!(
            price(("coin", "689.006542078688"), ("runonflux.flux", "67")).as_deref(),
            Some("10.283679732518")
        );
        // Tiny prices keep 12 places, trailing zeros included.
        assert_eq!(
            price(("coin", "0.5"), ("free.t", "1000000000000")).as_deref(),
            Some("0.000000000001")
        );
        assert_eq!(
            price(("coin", "0.4"), ("free.t", "1000000000000")).as_deref(),
            Some("0.000000000000")
        );
        assert_eq!(
            price(("coin", "1"), ("free.t", "4")).as_deref(),
            Some("0.250000000000")
        );
        // Neither leg is coin: leg 1 / leg 0, as stock.
        assert_eq!(
            price(("a.x", "2"), ("b.y", "3")).as_deref(),
            Some("1.500000000000")
        );
    }

    #[test]
    fn division_rounds_to_20_significant_digits_first() {
        // 2/3 = 0.66666666666666666667 (20 digits, half up), then toFixed(12).
        assert_eq!(
            price(("coin", "2"), ("t", "3")).as_deref(),
            Some("0.666666666667")
        );
        // A JSON number goes through a double first (1234567890123.4568) ...
        assert_eq!(
            price(("coin", "1234567890123.456789"), ("t", "0.1")).as_deref(),
            Some("12345678901234.568000000000")
        );
        // ... a `{decimal}` string does not: 20 significant digits, padded with zeros.
        assert_eq!(
            price(
                ("coin", r#"{"decimal":"1234567890123.456789"}"#),
                ("t", "0.1")
            )
            .as_deref(),
            Some("12345678901234.567890000000")
        );
        assert_eq!(
            price(("coin", r#"{"decimal":"1"}"#), ("t", r#"{"decimal":"3"}"#)).as_deref(),
            Some("0.333333333333")
        );
        // 1/7 * 10^6 = 142857.14285714285714 (20 sig) -> 142857.142857142857.
        assert_eq!(
            price(("coin", "1000000"), ("t", "7")).as_deref(),
            Some("142857.142857142857")
        );
    }

    #[test]
    fn amounts_go_through_a_javascript_double() {
        // 0.1234567890123456789 parses to the double 0.12345678901234568.
        let d = amount_dec("0.1234567890123456789").unwrap();
        assert_eq!(d, Dec::parse("0.12345678901234568").unwrap());
        // `{decimal: "..."}` is exact; `{int: 5}` is unreadable (decimal.js throws).
        let d = amount_dec(r#"{"decimal":"0.1234567890123456789"}"#).unwrap();
        assert_eq!(d, Dec::parse("0.1234567890123456789").unwrap());
        assert_eq!(amount_dec(r#"{"int":5}"#), None);
        assert_eq!(price(("coin", r#"{"int":5}"#), ("t", "1")), None);
        // Zero token amount: decimal.js text.
        assert_eq!(
            price(("coin", "1"), ("t", "0")).as_deref(),
            Some("Infinity")
        );
        assert_eq!(price(("coin", "0"), ("t", "0")).as_deref(), Some("NaN"));
        assert_eq!(
            price(("coin", "0"), ("t", "2")).as_deref(),
            Some("0.000000000000")
        );
    }

    #[test]
    fn parses_swap_results() {
        let k = TxKey::new(7432482, 2, 3);
        let data = json!([
            {"amount": 67, "token": "runonflux.flux"},
            {"amount": 689.006542078688, "token": "coin"},
            {"amount": 3.444274, "token": "n_e5.kb-USDC"}
        ]);
        let s = swap_of(k, 1764000000, &data).unwrap();
        assert_eq!(s.tokens, ["runonflux.flux", "coin", "n_e5.kb-USDC"]);
        assert_eq!(
            s.row,
            row(
                1764000000,
                k,
                ("runonflux.flux", "67"),
                ("coin", "689.006542078688")
            )
        );
        // Not swaps: a string, one element, a missing or empty token.
        assert_eq!(swap_of(k, 0, &json!("Write succeeded")), None);
        assert_eq!(
            swap_of(k, 0, &json!([{"token": "coin", "amount": 1}])),
            None
        );
        assert_eq!(
            swap_of(
                k,
                0,
                &json!([{"token": "coin", "amount": 1}, {"amount": 2}])
            ),
            None
        );
        assert_eq!(
            swap_of(
                k,
                0,
                &json!([{"token": "", "amount": 1}, {"token": "a", "amount": 2}])
            ),
            None
        );
        assert!(is_swap_event_name("kaddex.exchange.SWAP"));
        assert!(is_swap_event_name("kdlaunch.kdswap-exchange.SWAP"));
        // Only the trusted DEX modules count; anyone can deploy a module with a SWAP event.
        assert!(!is_swap_event_name("free.fake-dex.SWAP"));
        assert!(!is_swap_event_name("kaddex.exchange.SWAPPED"));
        assert!(!is_swap_event_name("coin.TRANSFER"));
        assert_eq!(
            creation_time(Some(br#"{"meta":{"creationTime":1764000000}}"#)),
            1764000000
        );
        assert_eq!(creation_time(Some(b"{}")), 0);
    }

    /// Security review: amounts, exponents and token lists are bounded.
    #[test]
    fn hostile_inputs_are_bounded() {
        // Exponents and digit counts past the limits are unreadable (null), and fast.
        assert_eq!(amount_dec(r#"{"decimal":"1e999999999"}"#), None);
        assert_eq!(amount_dec(r#"{"decimal":"1e-1001"}"#), None);
        assert_eq!(
            amount_dec(&format!(r#"{{"decimal":"{}"}}"#, "7".repeat(81))),
            None
        );
        assert_eq!(
            amount_dec(&format!(r#"{{"decimal":"0.{}1"}}"#, "0".repeat(2000))),
            None
        );
        assert!(amount_dec(&format!(r#"{{"decimal":"{}"}}"#, "7".repeat(80))).is_some());
        assert_eq!(
            price(("coin", r#"{"decimal":"1e999999999"}"#), ("t", "1")),
            None
        );
        // A price with more than 400 integer digits is null, not a huge string.
        assert_eq!(
            price(
                ("coin", r#"{"decimal":"1e500"}"#),
                ("t", r#"{"decimal":"1e-500"}"#)
            ),
            None
        );
        // A tiny one is zero; the largest allowed one renders.
        assert_eq!(
            price(
                ("coin", r#"{"decimal":"1e-1000"}"#),
                ("t", r#"{"decimal":"1e1000"}"#)
            )
            .as_deref(),
            Some("0.000000000000")
        );
        assert!(price(("coin", r#"{"decimal":"1e300"}"#), ("t", "1")).is_some());
        // 80-digit operands divide quickly.
        let big = format!(r#"{{"decimal":"{}"}}"#, "9".repeat(80));
        assert_eq!(
            price(("coin", &big), ("t", &big)).as_deref(),
            Some("1.000000000000")
        );
        // Token caps: names over 256 bytes are skipped (elements 0/1: no swap); at most 8.
        let long = "x".repeat(MAX_TOKEN_LEN + 1);
        let k = TxKey::new(1, 0, 1);
        assert_eq!(
            swap_of(
                k,
                0,
                &json!([{"token": long, "amount": 1}, {"token": "coin", "amount": 1}])
            ),
            None
        );
        let many: Vec<Value> = (0..50)
            .map(|i| json!({"token": format!("free.t{i}"), "amount": 1}))
            .collect();
        let s = swap_of(k, 0, &Value::Array(many)).unwrap();
        assert_eq!(s.tokens.len(), MAX_TOKENS_PER_SWAP);
    }

    /// Security review: a table larger than the scan budget answers exact names from one key
    /// range instead of reading every row.
    #[test]
    fn lookup_is_bounded_on_a_large_table() {
        let d = tempfile::tempdir().unwrap();
        let hot = HotStore::open(d.path(), Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        for i in 0..=LIKE_SCAN_ROWS {
            let s = Swap {
                row: row(1, TxKey::new(1, 0, 1), ("coin", "1"), ("free.t", "2")),
                tokens: vec![format!("free.tok{i:06}")],
            };
            upsert(&mut txn, 0, &s, None).unwrap();
        }
        let s = Swap {
            row: row(9, TxKey::new(2, 1, 1), ("coin", "3"), ("free.t", "1")),
            tokens: vec!["free.tok000007".into()],
        };
        upsert(&mut txn, 1, &s, None).unwrap();
        // Exact: the newest of its rows (chain 1).
        let got = lookup(&txn, "free.tok000007").unwrap().unwrap();
        assert_eq!(got.legs[0].1, "3");
        // A part of a name no longer matches on a table this large.
        assert_eq!(lookup(&txn, "tok000007").unwrap(), None);
    }

    #[test]
    fn row_codec_round_trips() {
        let r = row(
            5,
            TxKey::new(9, 1, 2),
            ("coin", "1.5"),
            ("free.t", "{\"decimal\":\"2\"}"),
        );
        assert_eq!(SwapRow::decode(&r.encode()), Some(r.clone()));
        let mut bad = r.encode();
        bad.push(0);
        assert_eq!(SwapRow::decode(&bad), None);
    }

    #[test]
    fn like_semantics() {
        assert!(like_contains("kaddex.kdx", "kaddex.kdx"));
        assert!(like_contains("kaddex.kdx2", "kaddex.kdx"));
        assert!(like_contains("n_582f.bro", "n_582f"));
        assert!(like_contains("nX582f.bro", "n_582f"));
        assert!(!like_contains("n582f.bro", "n_582f"));
        assert!(!like_contains("n_582f.bro", "n\\_582g"));
        assert!(like_contains("free.maga", "f%maga"));
        assert!(like_contains("coin", ""));
        assert!(!like_contains("Coin", "coin"));
    }

    #[test]
    fn newest_swap_wins_and_unwind_restores() {
        let d = tempfile::tempdir().unwrap();
        let hot = HotStore::open(d.path(), Geometry::default()).unwrap();
        let swap = |ctime, k: TxKey, amt: &str| Swap {
            row: row(ctime, k, ("coin", amt), ("free.t", "1")),
            tokens: vec!["coin".into(), "free.t".into()],
        };
        let mut txn = hot.rw().unwrap();
        // History (fill): no undo log.
        assert_eq!(
            upsert(&mut txn, 0, &swap(100, TxKey::new(10, 0, 1), "1"), None).unwrap(),
            2
        );
        // Replaying it, or an older swap, writes nothing.
        assert_eq!(
            upsert(&mut txn, 0, &swap(100, TxKey::new(10, 0, 1), "1"), None).unwrap(),
            0
        );
        assert_eq!(
            upsert(&mut txn, 0, &swap(99, TxKey::new(11, 0, 1), "9"), None).unwrap(),
            0
        );
        // Another chain keeps its own row; a newer creationTime there wins the lookup.
        assert_eq!(
            upsert(&mut txn, 3, &swap(150, TxKey::new(9, 3, 1), "3"), None).unwrap(),
            2
        );
        // Tail block 20 on chain 0: logged.
        let mut log = Vec::new();
        upsert(
            &mut txn,
            0,
            &swap(200, TxKey::new(20, 0, 1), "2"),
            Some(&mut log),
        )
        .unwrap();
        txn.set_meta(&undo_key(0, 20), &log).unwrap();
        // Tail block 21 on chain 0, a second swap of the same token.
        let mut log = Vec::new();
        upsert(
            &mut txn,
            0,
            &swap(210, TxKey::new(21, 0, 4), "4"),
            Some(&mut log),
        )
        .unwrap();
        txn.set_meta(&undo_key(0, 21), &log).unwrap();
        let got = lookup(&txn, "free.t").unwrap().unwrap();
        assert_eq!(got.legs[0].1, "4");
        assert_eq!(price_text(&got).as_deref(), Some("4.000000000000"));
        // Unwind 21 then 20 (newest first): chain 3's row (ctime 150) is newest again.
        assert_eq!(undo_block(&mut txn, 0, 21).unwrap(), 2);
        assert_eq!(lookup(&txn, "free.t").unwrap().unwrap().legs[0].1, "2");
        assert_eq!(undo_block(&mut txn, 0, 20).unwrap(), 2);
        assert_eq!(lookup(&txn, "free.t").unwrap().unwrap().legs[0].1, "3");
        assert!(txn.meta(&undo_key(0, 20)).unwrap().is_none());
        let r0 = SwapRow::decode(
            &txn.get(Table::TokenSwaps, &row_key(0, "free.t"))
                .unwrap()
                .unwrap(),
        );
        assert_eq!(r0.unwrap().legs[0].1, "1");
        // An unwind of a block that wrote nothing is a no-op; unknown tokens find nothing.
        assert_eq!(undo_block(&mut txn, 0, 22).unwrap(), 0);
        assert_eq!(lookup(&txn, "free.none").unwrap(), None);
        // A first row created by a tail block is deleted on unwind.
        let mut log = Vec::new();
        let s = Swap {
            row: row(300, TxKey::new(30, 5, 1), ("coin", "1"), ("free.new", "2")),
            tokens: vec!["free.new".into()],
        };
        upsert(&mut txn, 5, &s, Some(&mut log)).unwrap();
        txn.set_meta(&undo_key(5, 30), &log).unwrap();
        assert!(lookup(&txn, "free.new").unwrap().is_some());
        undo_block(&mut txn, 5, 30).unwrap();
        assert_eq!(lookup(&txn, "free.new").unwrap(), None);
    }
}
