//! Balance receivers no event names (D1). Used only by the balance seed's receivers pass
//! (`kdx_ingest::seed_receivers`); postings, transfers and tx lists never see them.
//!
//! Before coin v3 ([`Forks::coin_v3`]) KDA could reach an account with no
//! event naming it: every credit below the Pact events fork (1,138,000) and every cross-chain
//! credit (`coin.transfer-crosschain` step 1) up to 1,722,499. Such an account has no posting
//! on that chain, so the posting-based seed never marks its `(account, coin, chain)` row.
//! This module lists the candidate accounts of one tx:
//!
//! - **Cross-chain finish** (any height): a `cont` tx whose output continuation is step 1 of
//!   `coin.transfer-crosschain` gives `continuation.args[1]`, the receiver (structured field).
//!   From coin v3 on the same tx carries a TRANSFER naming it, so marking it again is harmless.
//! - **`exec` below the events fork** ([`Forks::events`]): every [`account_like`] string
//!   value in `exec.data` (any depth; object keys are not values and are skipped) and every
//!   string literal (`"..."`, or a `'symbol`) anywhere in `exec.code` (comments skipped).
//!   From the events fork on, every `exec` credit has an event naming the account, so
//!   `exec` txs there give nothing (review M2).
//!
//! Over-inclusion only costs one `/local` read per extra `(account, chain)`: the answer is
//! "no row", stored absent and never served. Under-inclusion leaves money missing, so the rule
//! is deliberately wide.

use kdx_types::record::TxRecord;
use kdx_types::{value, Dict};
use serde_json::Value;

use crate::build::account_ok;
use crate::render::{render_output, RenderError};

/// The two fork heights the rule depends on, per network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Forks {
    /// Pact events fork: from here on every `exec` coin credit emits an event naming the
    /// account (TRANSFER, RELEASE_ALLOCATION), so only cross-chain finishes lack one.
    pub events: u32,
    /// Coin v3 fork: from here on every coin credit emits a TRANSFER naming the receiver,
    /// cross-chain finishes included.
    pub coin_v3: u32,
}

/// Mainnet01 fork heights.
pub const MAINNET01: Forks = Forks {
    events: 1_138_000,
    coin_v3: 1_722_500,
};

/// The fork heights of `network`, or `None` when this build does not know them.
// TODO(D1): testnet04's events and coin v3 heights (chainweb's testnet04 version tables);
// the repo has none, so callers fall back to [`MAINNET01`] and warn. Both are below mainnet's,
// so the fallback only scans more txs; it never misses a receiver.
pub fn network_forks(network: &str) -> Option<Forks> {
    match network {
        "mainnet01" => Some(MAINNET01),
        _ => None,
    }
}

/// `coin.transfer-crosschain`, the defpact whose step 1 credits the receiver.
const XCHAIN_DEF: &str = "coin.transfer-crosschain";

/// Whether `s` can be a coin account name: 3 to 256 characters, the length rule of the coin
/// contract's `validate-account` (v1 and later), and indexable (`account_ok`: at most 256
/// bytes). No charset rule: coin v2's Latin-1 check did not exist at genesis, and a wider
/// rule only costs node reads. This covers `k:`/`w:`/`r:` principals, bare public keys and
/// legacy names alike.
pub fn account_like(s: &str) -> bool {
    account_ok(s) && (3..=256).contains(&s.chars().count())
}

/// Candidate receivers of a tx at `height` with parsed `cmd` and output `continuation`
/// (the CommandResult's `continuation` field, when present). Deduplicated, first-seen order.
pub fn balance_receivers(
    cmd: &Value,
    continuation: Option<&Value>,
    height: u32,
    forks: Forks,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |s: &str| {
        if account_like(s) && !out.iter().any(|x| x == s) {
            out.push(s.to_owned());
        }
    };
    if cmd.pointer("/payload/cont").is_some() {
        if let Some(c) = continuation {
            let step1 = c.get("step").and_then(Value::as_u64) == Some(1);
            let def = c.pointer("/continuation/def").and_then(Value::as_str);
            if step1 && def == Some(XCHAIN_DEF) {
                if let Some(r) = c.pointer("/continuation/args/1").and_then(Value::as_str) {
                    push(r);
                }
            }
        }
    } else if height < forks.events {
        if let Some(data) = cmd.pointer("/payload/exec/data") {
            data_strings(data, &mut push);
        }
        if let Some(code) = cmd.pointer("/payload/exec/code").and_then(Value::as_str) {
            for s in code_literals(code) {
                push(&s);
            }
        }
    }
    out
}

/// [`balance_receivers`] of a stored record (none for the coinbase; an error for a cmd that
/// is not JSON).
pub fn record_receivers(
    r: &TxRecord,
    dict: &dyn Dict,
    forks: Forks,
) -> Result<Vec<String>, RenderError> {
    let Some(cmd) = r.cmd.as_deref().filter(|_| r.key.pos() != 0) else {
        return Ok(Vec::new());
    };
    // Lenient parse: only string values are read, a faithful copy is not needed. A cmd that
    // does not parse is an error (counted and logged by the pass), never a silent skip (L1).
    let cmd = serde_json::from_slice::<Value>(cmd)
        .map_err(|e| RenderError::Shape(format!("cmd is not JSON: {e}")))?;
    let cont = if cmd.pointer("/payload/cont").is_some() {
        if r.raw_output.is_some() {
            render_output(r, dict)?.get("continuation").cloned()
        } else {
            match &r.continuation {
                Some(b) => Some(
                    value::from_slice(b, dict)
                        .map_err(|e| RenderError::Shape(format!("continuation: {e}")))?,
                ),
                None => None,
            }
        }
    } else {
        None
    };
    Ok(balance_receivers(
        &cmd,
        cont.as_ref(),
        r.key.height(),
        forks,
    ))
}

fn data_strings(v: &Value, f: &mut impl FnMut(&str)) {
    match v {
        Value::String(s) => f(s),
        Value::Array(a) => a.iter().for_each(|x| data_strings(x, f)),
        Value::Object(m) => m.values().for_each(|x| data_strings(x, f)),
        _ => {}
    }
}

/// Every string literal (`"..."` with `\"`/`\\` unescaped) and `'symbol` in `code`, wherever
/// it sits: a `(use coin)` then bare `transfer`, a `let` binding or another module's call can
/// carry the receiver (review M1). `;` comments are skipped.
fn code_literals(code: &str) -> Vec<String> {
    let b = code.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b';' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'"' => {
                let mut s = Vec::new();
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    if b[i] == b'\\' && i + 1 < b.len() {
                        i += 1;
                    }
                    s.push(b[i]);
                    i += 1;
                }
                out.push(String::from_utf8_lossy(&s).into_owned());
            }
            b'\'' => {
                let start = i + 1;
                let mut j = start;
                while j < b.len() && !is_delim(b[j]) {
                    j += 1;
                }
                out.push(String::from_utf8_lossy(&b[start..j]).into_owned());
                i = j;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    out
}

fn is_delim(c: u8) -> bool {
    c.is_ascii_whitespace() || matches!(c, b'(' | b')' | b'[' | b']' | b'{' | b'}' | b'"' | b',')
}
