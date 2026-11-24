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
//!   `kaddex.exchange.SWAP`, KDSwap `kdlaunch.kdswap-exchange.SWAP`, Sushi
//!   `n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange.SWAP`) whose `result.data` is a
//!   list where elements 0 and 1 equal one such event's `(token-in, in)` and `(token-out, out)`
//!   (tokens by the module reference's qualified name, amounts as numbers). The result is
//!   written by the sender, the event only by the DEX module, so the event authenticates the
//!   legs (fix round 1 of PR #31). Exactly one leg must be `coin`: a token-to-token swap has
//!   no price in KDA and stores nothing;
//! - one row per (chain, token) for the non-`coin` token and for every further string `token`
//!   in the list that a trusted SWAP event of the tx names on a hop with a `coin` side (a
//!   routed swap names 3+ tokens; stock matches all of them by text): the swap's order key and
//!   elements 0 and 1. `coin` itself is never a key;
//! - order key `(meta.creationTime, height, chain, position)`, newest wins;
//! - a lookup matches `moduleName` against the stored token names as stock's `LIKE` pattern
//!   (`%` and `_` wildcards, `\` escape) and takes the newest matching row. It is bounded:
//!   a pattern over [`MAX_LIKE_PATTERN`] bytes or with more than [`MAX_LIKE_WILDCARDS`]
//!   wildcards matches nothing; it reads at most [`LIKE_SCAN_ROWS`] rows and does at most
//!   [`LIKE_WORK_CELLS`] of matching work over the whole lookup, and past either reads the exact
//!   name on each chain; the caller's deadline is checked inside the scan.
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
/// module, so only these exchanges feed `TokenSwaps`). Kaddex/eckoDEX, KDSwap and the
/// namespaced Sushi exchange (`n_` principal namespace; Pact lets only that module emit events
/// under its name, so trust is scoped to that one module).
pub const DEX_SWAP_EVENTS: &[&str] = &[
    "kaddex.exchange.SWAP",
    "kdlaunch.kdswap-exchange.SWAP",
    "n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange.SWAP",
];
/// The KDA token.
const COIN: &str = "coin";
/// Longest token name stored (longer names are skipped; Pact module names are far shorter).
pub const MAX_TOKEN_LEN: usize = 256;
/// Most tokens one swap may update (a routed swap names one per hop).
pub const MAX_TOKENS_PER_SWAP: usize = 8;
/// Chains an exact-name lookup reads (one point read per chain).
pub const MAX_CHAINS: u8 = 20;
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

/// A swap found in a tx: the row stored under `tokens` (the result's legs) and the rows of the
/// routed tokens, each with the legs of its own trusted hop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Swap {
    pub row: SwapRow,
    pub tokens: Vec<String>,
    /// A further token the result names and a trusted `SWAP` event of the tx trades against
    /// `coin`, with that event's own legs (never the result's).
    pub routed: Vec<(String, SwapRow)>,
}

/// An event name (`<module>.<NAME>`) of a trusted DEX swap ([`DEX_SWAP_EVENTS`]).
pub fn is_swap_event_name(name: &str) -> bool {
    DEX_SWAP_EVENTS.contains(&name)
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

/// Characters of a Pact qualified name besides ASCII letters and digits (Pact identifiers
/// allow these symbols; `.` separates namespace, module and member).
const NAME_SYMBOLS: &str = "%#+-_&$@<>=^?*!|/~.";

/// A token name we store: non-empty, at most [`MAX_TOKEN_LEN`] bytes, and only characters
/// valid in a Pact qualified name (no whitespace or control characters).
pub fn valid_token(t: &str) -> bool {
    !t.is_empty()
        && t.len() <= MAX_TOKEN_LEN
        && t.chars()
            .all(|c| c.is_ascii_alphanumeric() || NAME_SYMBOLS.contains(c))
}

fn token_of(e: &Value) -> Option<&str> {
    e.get("token")?.as_str().filter(|s| valid_token(s))
}

/// The qualified name of a Pact module reference
/// (`{"refName":{"namespace":ns|null,"name":n},"refSpec":[..]}`).
fn module_ref_name(v: &Value) -> Option<String> {
    let r = v.get("refName")?;
    let name = r.get("name")?.as_str()?;
    let q = match r.get("namespace") {
        Some(Value::String(ns)) => format!("{ns}.{name}"),
        None | Some(Value::Null) => name.to_owned(),
        _ => return None,
    };
    valid_token(&q).then_some(q)
}

/// One hop of a trusted DEX swap, from its `SWAP` event params
/// `[sender, receiver, in, token-in, out, token-out]` (Kaddex and KDSwap share the layout).
struct Hop {
    token_in: String,
    amount_in: Dec,
    /// The amounts as the event wrote them (JSON text), as a stored leg keeps them.
    raw_in: String,
    token_out: String,
    amount_out: Dec,
    raw_out: String,
}

fn hop_of(params: &Value) -> Option<Hop> {
    let p = params.as_array().filter(|p| p.len() == 6)?;
    Some(Hop {
        amount_in: amount_of(&p[2])?,
        raw_in: p[2].to_string(),
        token_in: module_ref_name(&p[3])?,
        amount_out: amount_of(&p[4])?,
        raw_out: p[4].to_string(),
        token_out: module_ref_name(&p[5])?,
    })
}

/// The params of each trusted DEX `SWAP` event of a tx ([`DEX_SWAP_EVENTS`]); unreadable
/// params are left out.
pub fn swap_event_params(dict: &dyn Dict, events: &[EventRec]) -> Vec<Value> {
    events
        .iter()
        .filter(|e| {
            dict.name(DictKind::EventName, e.name)
                .is_some_and(is_swap_event_name)
        })
        .filter_map(|e| value::from_slice(&e.params, dict).ok())
        .collect()
}

/// The swap in a successful tx's `result.data`, given the params of the tx's trusted DEX
/// `SWAP` events: elements 0 and 1 must carry a token, and equal `(token-in, in)` and
/// `(token-out, out)` of one of those events. Further tokens of the list are kept only with the
/// legs of a trusted event of the tx that trades them against `coin` (never the result's legs:
/// the result is the sender's, so a dust swap in one pair must not price another token).
pub fn swap_of(key: TxKey, ctime: u64, data: &Value, swap_events: &[Value]) -> Option<Swap> {
    let hops: Vec<Hop> = swap_events.iter().filter_map(hop_of).collect();
    if hops.is_empty() {
        return None;
    }
    let list = data.as_array()?;
    let (e0, e1) = (list.first()?, list.get(1)?);
    let (t0, t1) = (token_of(e0)?, token_of(e1)?);
    let (a0, a1) = (e0.get("amount")?, e1.get("amount")?);
    let (d0, d1) = (amount_of(a0)?, amount_of(a1)?);
    // Only a KDA pair has a price in KDA: exactly one leg is `coin`; the row is keyed under the
    // other token. A token-to-token swap would otherwise store a nonsense price.
    if (t0 == COIN) == (t1 == COIN) {
        return None;
    }
    let authentic = hops.iter().any(|h| {
        h.token_in == t0
            && h.token_out == t1
            && same_amount(&h.amount_in, &d0)
            && same_amount(&h.amount_out, &d1)
    });
    if !authentic {
        return None;
    }
    let row = SwapRow {
        ctime,
        key,
        legs: [
            (t0.to_owned(), a0.to_string()),
            (t1.to_owned(), a1.to_string()),
        ],
    };
    let main = if t0 == COIN { t1 } else { t0 };
    let tokens = vec![main.to_owned()];
    let mut routed: Vec<(String, SwapRow)> = Vec::new();
    // A routed token is priced by its own hop: the first trusted event that trades it against
    // `coin`, with that event's token and amount legs.
    for t in list
        .iter()
        .skip(2)
        .filter_map(token_of)
        .filter(|t| *t != COIN && *t != main)
    {
        if tokens.len() + routed.len() == MAX_TOKENS_PER_SWAP {
            break;
        }
        if routed.iter().any(|(x, _)| x == t) {
            continue;
        }
        let own = hops.iter().find(|h| {
            (h.token_in == t && h.token_out == COIN) || (h.token_in == COIN && h.token_out == t)
        });
        if let Some(h) = own {
            routed.push((
                t.to_owned(),
                SwapRow {
                    ctime,
                    key,
                    legs: [
                        (h.token_in.clone(), h.raw_in.clone()),
                        (h.token_out.clone(), h.raw_out.clone()),
                    ],
                },
            ));
        }
    }
    Some(Swap {
        row,
        tokens,
        routed,
    })
}

/// The swap in a stored tx: successful, not coinbase, with a trusted DEX `SWAP` event whose
/// legs its result repeats (see [`swap_of`]).
pub fn swap_of_tx(dict: &dyn Dict, r: &TxRecord) -> Option<Swap> {
    if !r.status_ok || r.key.is_coinbase() {
        return None;
    }
    let events = swap_event_params(dict, &r.events);
    if events.is_empty() {
        return None;
    }
    let data = value::from_slice(&r.result, dict).ok()?;
    swap_of(r.key, creation_time(r.cmd.as_deref()), &data, &events)
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
    let rows =
        (swap.tokens.iter().map(|t| (t, &swap.row))).chain(swap.routed.iter().map(|(t, r)| (t, r)));
    let mut n = 0;
    for (t, row) in rows {
        let val = row.encode();
        let k = row_key(chain, t);
        let prev = txn.get(Table::TokenSwaps, &k)?;
        if let Some(p) = prev.as_deref().and_then(SwapRow::decode) {
            if p.order() >= row.order() {
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

/// Longest `LIKE` pattern a lookup accepts (bytes); a longer one answers no match.
pub const MAX_LIKE_PATTERN: usize = 256;
/// Most `%` / `_` wildcards (unescaped) a pattern may hold; more answers no match.
pub const MAX_LIKE_WILDCARDS: usize = 8;
/// Matching work one lookup may do, in pattern-by-text cells summed over every row it tests;
/// past it the lookup stops matching and reads the exact name on each chain instead.
pub const LIKE_WORK_CELLS: u64 = 20_000_000;
/// Rows between two deadline checks in the table scan.
const DEADLINE_EVERY: usize = 256;

/// The newest stored swap whose token matches `module` as stock's `LIKE '%module%'`.
/// Bounded: reads at most [`LIKE_SCAN_ROWS`] + 1 rows; when the table is larger, only the
/// exact name is looked up, with one point read per chain ([`MAX_CHAINS`]). The pattern is
/// limited ([`MAX_LIKE_PATTERN`], [`MAX_LIKE_WILDCARDS`]) and the matching work of the whole
/// lookup is capped ([`LIKE_WORK_CELLS`]).
pub fn lookup(txn: &impl Txn, module: &str) -> Result<Option<SwapRow>> {
    lookup_checked(txn, module, &mut || Ok(()))
}

/// [`lookup`] that calls `check` every [`DEADLINE_EVERY`] rows (the request deadline); an error
/// from it ends the lookup with that error.
pub fn lookup_checked(
    txn: &impl Txn,
    module: &str,
    check: &mut dyn FnMut() -> Result<()>,
) -> Result<Option<SwapRow>> {
    let Some(mut pattern) = LikePattern::new(module) else {
        return Ok(None);
    };
    let all = txn.scan_limit(Table::TokenSwaps, &[], LIKE_SCAN_ROWS + 1, |_| true)?;
    let mut best: Option<SwapRow> = None;
    fn keep(best: &mut Option<SwapRow>, v: &[u8]) {
        if let Some(row) = SwapRow::decode(v) {
            if best.as_ref().is_none_or(|b| row.order() > b.order()) {
                *best = Some(row);
            }
        }
    }
    let mut scanned = all.len() <= LIKE_SCAN_ROWS;
    if scanned {
        // Keys are token ‖ chain, so one token's rows are adjacent: match each name once.
        let mut last: Option<(&[u8], bool)> = None;
        for (i, (k, v)) in all.iter().enumerate() {
            if i % DEADLINE_EVERY == 0 {
                check()?;
            }
            let Some((_, token)) = k.split_last() else {
                continue;
            };
            let hit = match last {
                Some((t, hit)) if t == token => hit,
                _ => {
                    if pattern.work > LIKE_WORK_CELLS {
                        scanned = false;
                        break;
                    }
                    let hit = std::str::from_utf8(token).is_ok_and(|t| pattern.matches(t));
                    last = Some((token, hit));
                    hit
                }
            };
            if hit {
                keep(&mut best, v);
            }
        }
    }
    if !scanned {
        // A larger table, or the work cap: the exact name only (drop any partial scan result).
        best = None;
        if valid_token(module) {
            for c in 0..MAX_CHAINS {
                if let Some(v) = txn.get(Table::TokenSwaps, &row_key(c, module))? {
                    keep(&mut best, &v);
                }
            }
        }
    }
    Ok(best)
}

#[derive(Clone, Copy, PartialEq)]
enum P {
    Lit(char),
    One,
    Any,
}

/// A compiled SQL `text LIKE '%' || pattern || '%'` (PostgreSQL: `%` any run, `_` one
/// character, `\` escapes the next character; case-sensitive). The two DP rows are reused for
/// every text, and `work` counts the cells filled so far.
struct LikePattern {
    p: Vec<P>,
    cur: Vec<bool>,
    next: Vec<bool>,
    work: u64,
}

impl LikePattern {
    /// `None` when the pattern is longer than [`MAX_LIKE_PATTERN`] bytes or holds more than
    /// [`MAX_LIKE_WILDCARDS`] unescaped wildcards.
    fn new(pattern: &str) -> Option<LikePattern> {
        if pattern.len() > MAX_LIKE_PATTERN {
            return None;
        }
        // Leading and trailing `%` are implied.
        let mut p = vec![P::Any];
        let mut wild = 0;
        let mut it = pattern.chars();
        while let Some(c) = it.next() {
            p.push(match c {
                '%' => P::Any,
                '_' => P::One,
                '\\' => P::Lit(it.next().unwrap_or('\\')),
                c => P::Lit(c),
            });
            wild += usize::from(matches!(p.last(), Some(P::Any | P::One)));
        }
        if wild > MAX_LIKE_WILDCARDS {
            return None;
        }
        p.push(P::Any);
        let n = p.len() + 1;
        Some(LikePattern {
            p,
            cur: vec![false; n],
            next: vec![false; n],
            work: 0,
        })
    }

    fn matches(&mut self, text: &str) -> bool {
        let n = self.p.len();
        // cur[j]: pattern prefix of length j matches the text prefix seen so far.
        self.cur[0] = true;
        for j in 1..=n {
            self.cur[j] = self.cur[j - 1] && self.p[j - 1] == P::Any;
        }
        for c in text.chars() {
            self.work += n as u64;
            self.next[0] = false;
            for j in 1..=n {
                self.next[j] = match self.p[j - 1] {
                    P::Any => self.next[j - 1] || self.cur[j],
                    P::One => self.cur[j - 1],
                    P::Lit(l) => self.cur[j - 1] && l == c,
                };
            }
            std::mem::swap(&mut self.cur, &mut self.next);
        }
        self.cur[n]
    }
}

/// SQL `text LIKE '%' || pattern || '%'`; false for a pattern [`LikePattern::new`] refuses.
pub fn like_contains(text: &str, pattern: &str) -> bool {
    LikePattern::new(pattern).is_some_and(|mut p| p.matches(text))
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
    amount_of(&serde_json::from_str(json).ok()?)
}

/// Two amounts are the same number (zero of either sign included).
fn same_amount(a: &Dec, b: &Dec) -> bool {
    (a.is_zero() && b.is_zero()) || a == b
}

/// [`amount_dec`] of a parsed JSON value.
fn amount_of(a: &Value) -> Option<Dec> {
    fn pick<'a>(o: &'a serde_json::Map<String, Value>, f: &str) -> Option<&'a Value> {
        o.get(f).filter(|v| !v.is_null())
    }
    let v = match a {
        Value::Object(o) => pick(o, "decimal")
            .or_else(|| pick(o, "integer"))
            .unwrap_or(a),
        _ => a,
    };
    match v {
        Value::Number(n) => {
            let f: f64 = n.to_string().parse().ok()?;
            f.is_finite().then(|| Dec::parse(&format!("{f}")))?
        }
        Value::String(s) => Dec::parse(s),
        _ => None,
    }
}

/// The stock answer for a stored swap: `kda / token` as `toFixed(12)` text, where `kda` is
/// the amount of the leg whose token is `coin`. `None` when neither leg is `coin` or an amount
/// is unreadable.
/// A zero token amount gives decimal.js's `Infinity` / `NaN` text, as stock sends.
pub fn price_text(row: &SwapRow) -> Option<String> {
    let (kda, tok) = match (row.legs[0].0 == COIN, row.legs[1].0 == COIN) {
        (true, false) => (&row.legs[0].1, &row.legs[1].1),
        (false, true) => (&row.legs[1].1, &row.legs[0].1),
        // Not a KDA pair (the ingest never stores one): no price.
        _ => return None,
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

    /// A Pact module reference as event params carry it.
    fn mref(q: &str) -> Value {
        let (ns, name) = match q.rsplit_once('.') {
            Some((ns, n)) => (json!(ns), n),
            None => (Value::Null, q),
        };
        json!({"refSpec": [{"namespace": null, "name": "fungible-v2"}],
               "refName": {"namespace": ns, "name": name}})
    }

    /// `SWAP` event params `[sender, receiver, in, token-in, out, token-out]`.
    fn ev(tin: &str, ain: Value, tout: &str, aout: Value) -> Value {
        json!(["pool", "k:user", ain, mref(tin), aout, mref(tout)])
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
        // Neither leg is coin (or both are): no price in KDA, whatever is stored.
        assert_eq!(price(("a.x", "2"), ("b.y", "3")), None);
        assert_eq!(price(("coin", "2"), ("coin", "3")), None);
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
        // A routed swap: one event per hop.
        let evs = [
            ev(
                "runonflux.flux",
                json!(67.0),
                "coin",
                json!(689.006542078688),
            ),
            ev(
                "coin",
                json!(689.006542078688),
                "n_e5.kb-USDC",
                json!(3.444274),
            ),
        ];
        let s = swap_of(k, 1764000000, &data, &evs).unwrap();
        // `coin` itself is never a key; the routed token has a trusted hop with a coin side and
        // is stored with that hop's legs, not the result's.
        assert_eq!(s.tokens, ["runonflux.flux"]);
        assert_eq!(s.routed.len(), 1);
        assert_eq!(s.routed[0].0, "n_e5.kb-USDC");
        assert_eq!(
            s.routed[0].1.legs,
            [
                ("coin".to_owned(), "689.006542078688".to_owned()),
                ("n_e5.kb-USDC".to_owned(), "3.444274".to_owned())
            ]
        );
        assert_eq!(
            s.row,
            row(
                1764000000,
                k,
                ("runonflux.flux", "67"),
                ("coin", "689.006542078688")
            )
        );
        // A token the events do not name is not stored (the first hop alone: no USDC row).
        let s = swap_of(k, 1764000000, &data, &evs[..1]).unwrap();
        assert_eq!(s.tokens, ["runonflux.flux"]);
        // A routed token named only by a hop without a coin side is not stored.
        let mut via = evs.clone();
        via[1] = ev("n_e5.kb-USDC", json!(3.444274), "free.other", json!(9));
        let s = swap_of(k, 1764000000, &data, &via).unwrap();
        assert_eq!(s.tokens, ["runonflux.flux"]);
        // Token-to-token swap: not a KDA pair, nothing is stored.
        let tt = [ev("free.a", json!(2), "free.b", json!(3))];
        let ab = json!([{"token": "free.a", "amount": 2}, {"token": "free.b", "amount": 3}]);
        assert_eq!(swap_of(k, 0, &ab, &tt), None);
        // Neither is it when both legs are coin.
        let cc = [ev("coin", json!(2), "coin", json!(3))];
        let both = json!([{"token": "coin", "amount": 2}, {"token": "coin", "amount": 3}]);
        assert_eq!(swap_of(k, 0, &both, &cc), None);
        // Amounts compare as numbers: a `{"decimal"}` leg matches the same number.
        let s = swap_of(
            k,
            0,
            &json!([{"amount": {"decimal": "67.0"}, "token": "runonflux.flux"},
                    {"amount": 689.006542078688, "token": "coin"}]),
            &evs,
        );
        assert!(s.is_some());
        // Not swaps: a string, one element, a missing or empty token, no event.
        let one = [ev("coin", json!(1), "a", json!(2))];
        assert_eq!(swap_of(k, 0, &json!("Write succeeded"), &one), None);
        assert_eq!(
            swap_of(k, 0, &json!([{"token": "coin", "amount": 1}]), &one),
            None
        );
        assert_eq!(
            swap_of(
                k,
                0,
                &json!([{"token": "coin", "amount": 1}, {"amount": 2}]),
                &one
            ),
            None
        );
        assert_eq!(
            swap_of(
                k,
                0,
                &json!([{"token": "", "amount": 1}, {"token": "a", "amount": 2}]),
                &one
            ),
            None
        );
        let ok = json!([{"token": "coin", "amount": 1}, {"token": "a", "amount": 2}]);
        assert!(swap_of(k, 0, &ok, &one).is_some());
        assert_eq!(swap_of(k, 0, &ok, &[]), None);
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
        // Token caps: names over 256 bytes, or with characters a Pact name cannot hold, are
        // skipped (elements 0/1: no swap); at most 8.
        let k = TxKey::new(1, 0, 1);
        for bad in [
            "x".repeat(MAX_TOKEN_LEN + 1),
            "free.t\u{5}x".into(),
            "free.t x".into(),
            "free.t\n".into(),
            "free.tök".into(),
        ] {
            assert!(!valid_token(&bad), "{bad:?}");
            assert_eq!(
                swap_of(
                    k,
                    0,
                    &json!([{"token": bad, "amount": 1}, {"token": "coin", "amount": 1}]),
                    &[ev(&bad, json!(1), "coin", json!(1))]
                ),
                None
            );
        }
        assert!(valid_token(
            "n_e595727b657fbbb3b8e362a05a7bb8d12865c1ff.kb-USDC"
        ));
        let mut many = vec![json!({"token": "coin", "amount": 1})];
        many.extend((0..49).map(|i| json!({"token": format!("free.t{i}"), "amount": 1})));
        let evs: Vec<Value> = (0..49)
            .map(|i| ev("coin", json!(1), &format!("free.t{i}"), json!(1)))
            .collect();
        let s = swap_of(k, 0, &Value::Array(many), &evs).unwrap();
        assert_eq!(s.tokens.len() + s.routed.len(), MAX_TOKENS_PER_SWAP);
    }

    /// PR #31 review H1: the result is written by the sender. A genuine trusted `SWAP` event
    /// with a crafted result stores no row, and a crafted extra token is not stored.
    #[test]
    fn a_crafted_result_beside_a_genuine_event_stores_nothing() {
        let k = TxKey::new(1, 2, 1);
        // Dust swap of 0.001 KDA for 3 KDX; the result claims 1,000,000 KDA for 1 KDX.
        let genuine = [ev("coin", json!(0.001), "kaddex.kdx", json!(3.0))];
        for crafted in [
            json!([{"token": "coin", "amount": 1000000}, {"token": "kaddex.kdx", "amount": 1}]),
            json!([{"token": "coin", "amount": 0.001}, {"token": "kaddex.kdx", "amount": 1}]),
            json!([{"token": "coin", "amount": 1000000}, {"token": "kaddex.kdx", "amount": 3}]),
            // Legs swapped, or a token the event does not name.
            json!([{"token": "kaddex.kdx", "amount": 3}, {"token": "coin", "amount": 0.001}]),
            json!([{"token": "coin", "amount": 0.001}, {"token": "free.fake", "amount": 3}]),
        ] {
            assert_eq!(swap_of(k, 0, &crafted, &genuine), None, "{crafted}");
        }
        // The honest result passes; an appended token the events do not name is dropped.
        let honest = json!([
            {"token": "coin", "amount": 0.001},
            {"token": "kaddex.kdx", "amount": 3},
            {"token": "free.victim", "amount": 1}
        ]);
        let s = swap_of(k, 0, &honest, &genuine).unwrap();
        assert_eq!(s.tokens, ["kaddex.kdx"]);
        // Through a stored tx: only trusted DEX events count.
        use kdx_types::DictMut;
        let mut dict = kdx_types::MemDict::default();
        let rec = |dict: &mut kdx_types::MemDict, name: &str, data: &Value| TxRecord {
            key: k,
            status_ok: true,
            result: value::to_vec(data, &*dict),
            events: vec![EventRec {
                module: 0,
                name: dict.intern(DictKind::EventName, name),
                module_hash: 0,
                params: value::to_vec(&genuine[0], &*dict),
            }],
            ..Default::default()
        };
        let r = rec(&mut dict, "kaddex.exchange.SWAP", &honest);
        assert_eq!(swap_of_tx(&dict, &r).unwrap().tokens, ["kaddex.kdx"]);
        let r = rec(&mut dict, "free.fake-dex.SWAP", &honest);
        assert_eq!(swap_of_tx(&dict, &r), None);
        let crafted =
            json!([{"token": "coin", "amount": 1000000}, {"token": "kaddex.kdx", "amount": 1}]);
        let r = rec(&mut dict, "kaddex.exchange.SWAP", &crafted);
        assert_eq!(swap_of_tx(&dict, &r), None);
    }

    /// PR #31 review H2: a routed token is priced with the legs of its own coin-side hop, not
    /// with the result's legs 0/1. A dust `free.junk -> coin` swap paired with a dust
    /// `coin -> kaddex.kdx` swap in one tx cannot set kaddex.kdx's price from the junk pair.
    #[test]
    fn a_routed_token_gets_its_own_hops_price() {
        let k = TxKey::new(1, 3, 1);
        let junk_hop = ev("free.junk", json!(1000), "coin", json!(0.001));
        let kdx_hop = ev("coin", json!(2.0), "kaddex.kdx", json!(4.0));
        // Attack: the result is the sender's, `[junk, coin, kdx]`; legs 0/1 are the junk hop.
        let crafted = json!([
            {"token": "free.junk", "amount": 1000},
            {"token": "coin", "amount": 0.001},
            {"token": "kaddex.kdx", "amount": 1}
        ]);
        let s = swap_of(k, 7, &crafted, &[junk_hop.clone(), kdx_hop.clone()]).unwrap();
        assert_eq!(s.tokens, ["free.junk"]);
        assert_eq!(price_text(&s.row).as_deref(), Some("0.000001000000"));
        // kaddex.kdx is stored with its own hop (0.5 KDA per KDX), never the junk ratio.
        assert_eq!(s.routed.len(), 1);
        let (t, r) = &s.routed[0];
        assert_eq!(t, "kaddex.kdx");
        assert_eq!(price_text(r).as_deref(), Some("0.500000000000"));
        assert_eq!(r.legs[0], ("coin".to_owned(), "2.0".to_owned()));
        assert_eq!(r.legs[1], ("kaddex.kdx".to_owned(), "4.0".to_owned()));
        assert_eq!((r.ctime, r.key), (7, k));
        // Without a trusted hop of its own the routed token gets no row at all.
        let s = swap_of(k, 7, &crafted, std::slice::from_ref(&junk_hop)).unwrap();
        assert!(s.routed.is_empty());
        // A hop of another token, or a token-to-token hop (no coin side), does not price it.
        let other = ev("coin", json!(1), "free.other", json!(1));
        let tok_tok = ev("free.junk", json!(1), "kaddex.kdx", json!(1));
        let s = swap_of(k, 7, &crafted, &[junk_hop.clone(), other, tok_tok]).unwrap();
        assert!(s.routed.is_empty());
        // Honest routed swap, `coin -> A -> B` shaped as `[A, coin, B]`: B has its own hop.
        let s = swap_of(k, 7, &crafted, &[junk_hop, kdx_hop]).unwrap();
        // Stored through the table: each token reads back its own price.
        let d = tempfile::tempdir().unwrap();
        let hot = HotStore::open(d.path(), Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        assert_eq!(upsert(&mut txn, 3, &s, None).unwrap(), 2);
        let kdx = lookup(&txn, "kaddex.kdx").unwrap().unwrap();
        assert_eq!(price_text(&kdx).as_deref(), Some("0.500000000000"));
        let junk = lookup(&txn, "free.junk").unwrap().unwrap();
        assert_eq!(price_text(&junk).as_deref(), Some("0.000001000000"));
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
                routed: vec![],
            };
            upsert(&mut txn, 0, &s, None).unwrap();
        }
        let s = Swap {
            row: row(9, TxKey::new(2, 1, 1), ("coin", "3"), ("free.t", "1")),
            tokens: vec!["free.tok000007".into()],
            routed: vec![],
        };
        upsert(&mut txn, 1, &s, None).unwrap();
        // Exact: the newest of its rows (chain 1).
        let got = lookup(&txn, "free.tok000007").unwrap().unwrap();
        assert_eq!(got.legs[0].1, "3");
        // A part of a name no longer matches on a table this large.
        assert_eq!(lookup(&txn, "tok000007").unwrap(), None);
        // Exact reads per chain: rows of names that extend the module's key (review M1:
        // `module` + a low byte sorted between the module's chain rows) cannot hide chain 19.
        for i in 0..300u32 {
            let s = Swap {
                row: row(1, TxKey::new(1, 0, 1), ("coin", "1"), ("free.t", "2")),
                tokens: vec![
                    format!("free.tok000007{}", char::from((i % 8 + 1) as u8)) + &i.to_string(),
                ],
                routed: vec![],
            };
            upsert(&mut txn, 0, &s, None).unwrap();
        }
        let s = Swap {
            row: row(20, TxKey::new(3, 19, 1), ("coin", "5"), ("free.t", "1")),
            tokens: vec!["free.tok000007".into()],
            routed: vec![],
        };
        upsert(&mut txn, 19, &s, None).unwrap();
        assert_eq!(
            lookup(&txn, "free.tok000007").unwrap().unwrap().legs[0].1,
            "5"
        );
        // A name a Pact module cannot have is not looked up.
        assert_eq!(lookup(&txn, "free.tok000007\n").unwrap(), None);
    }

    /// PR #31 review: the `LIKE` pattern is user input. A long pattern, many wildcards or a
    /// lookup past its work cap cannot burn the scan; the deadline check runs inside it.
    #[test]
    fn like_lookup_is_bounded() {
        // Pattern limits: answered as no match.
        assert!(like_contains("free.tok", "free.tok"));
        assert!(!like_contains("a", &"a".repeat(MAX_LIKE_PATTERN + 1)));
        assert!(like_contains("a", &"%".repeat(MAX_LIKE_WILDCARDS)));
        assert!(!like_contains("a", &"%".repeat(MAX_LIKE_WILDCARDS + 1)));
        assert!(!like_contains("a", &"_".repeat(MAX_LIKE_WILDCARDS + 1)));
        // An escaped wildcard is a literal and does not count.
        assert!(like_contains("a_b", "\\_"));
        assert!(LikePattern::new(&"\\%".repeat(MAX_LIKE_WILDCARDS + 1)).is_some());
        // The DP rows are reused: many texts, one allocation, work counted across all of them.
        let mut p = LikePattern::new("a_c").unwrap();
        assert!(p.matches("xabcx") && !p.matches("xac") && p.matches("abc"));
        assert_eq!(p.work, (5 + 3 + 3) * 5);

        let d = tempfile::tempdir().unwrap();
        let hot = HotStore::open(d.path(), Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        for i in 0..3000u32 {
            let name = format!("free.{}{i:06}", "t".repeat(200));
            let s = Swap {
                row: row(1, TxKey::new(i + 1, 0, 1), ("coin", "1"), (&name, "2")),
                tokens: vec![name],
                routed: vec![],
            };
            upsert(&mut txn, 0, &s, None).unwrap();
        }
        // An over-long pattern or too many wildcards: null, without reading the table.
        let mut calls = 0;
        let long = "t".repeat(MAX_LIKE_PATTERN + 1);
        assert_eq!(
            lookup_checked(&txn, &long, &mut || {
                calls += 1;
                Ok(())
            })
            .unwrap(),
            None
        );
        assert_eq!(calls, 0);
        assert_eq!(lookup(&txn, "%%%%%%%%%").unwrap(), None);
        // The deadline is checked inside the scan (every DEADLINE_EVERY rows) and its error
        // ends the lookup.
        let mut calls = 0;
        let r = lookup_checked(&txn, "free.t", &mut || {
            calls += 1;
            anyhow::ensure!(calls < 3, "deadline");
            Ok(())
        });
        assert!(r.is_err());
        assert_eq!(calls, 3);
        // Work cap: 3000 names of 207 chars against a 200-char pattern with a wildcard cost
        // past LIKE_WORK_CELLS, so the lookup falls back to the exact name (here a miss) rather
        // than finishing the scan.
        let pat = format!("{}%x", "t".repeat(198));
        assert_eq!(lookup(&txn, &pat).unwrap(), None);
        // A cheap pattern still scans everything: the newest of the matches wins.
        let got = lookup(&txn, "free.ttt").unwrap().unwrap();
        assert_eq!(got.key.height(), 3000);
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
            routed: vec![],
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
            routed: vec![],
        };
        upsert(&mut txn, 5, &s, Some(&mut log)).unwrap();
        txn.set_meta(&undo_key(5, 30), &log).unwrap();
        assert!(lookup(&txn, "free.new").unwrap().is_some());
        undo_block(&mut txn, 5, 30).unwrap();
        assert_eq!(lookup(&txn, "free.new").unwrap(), None);
    }
}
