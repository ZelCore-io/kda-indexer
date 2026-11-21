//! Balance receivers no event names (D1). Used only by the balance seed's receivers pass
//! (`kdx_ingest::seed_receivers`); postings, transfers and tx lists never see them.
//!
//! Before coin v3 (mainnet01 height [`COIN_V3_HEIGHT`]) KDA could reach an account with no
//! event naming it: every credit below the Pact events fork (1,138,000) and every cross-chain
//! credit (`coin.transfer-crosschain` step 1) up to 1,722,499. Such an account has no posting
//! on that chain, so the posting-based seed never marks its `(account, coin, chain)` row.
//! This module lists the candidate accounts of one tx:
//!
//! - **Cross-chain finish** (any height): a `cont` tx whose output continuation is step 1 of
//!   `coin.transfer-crosschain` gives `continuation.args[1]`, the receiver (structured field).
//!   From coin v3 on the same tx carries a TRANSFER naming it, so marking it again is harmless.
//! - **`exec` below [`COIN_V3_HEIGHT`]**: every [`account_like`] string value in `exec.data`
//!   (any depth; object keys are not values and are skipped) and every string literal
//!   (`"..."`, or a `'symbol`) inside the s-expression of a call whose head starts with
//!   `coin.` in `exec.code` (nested forms included; comments skipped).
//!
//! Over-inclusion only costs one `/local` read per extra `(account, chain)`: the answer is
//! "no row", stored absent and never served. Under-inclusion leaves money missing, so the rule
//! is deliberately wide.

use kdx_types::record::TxRecord;
use kdx_types::{value, Dict};
use serde_json::Value;

use crate::build::account_ok;
use crate::render::{render_output, RenderError};

/// Mainnet01 coin v3 fork height: from here on every coin credit emits a TRANSFER event
/// naming the receiver, cross-chain finishes included.
pub const COIN_V3_HEIGHT: u32 = 1_722_500;

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
pub fn balance_receivers(cmd: &Value, continuation: Option<&Value>, height: u32) -> Vec<String> {
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
    } else if height < COIN_V3_HEIGHT {
        if let Some(data) = cmd.pointer("/payload/exec/data") {
            data_strings(data, &mut push);
        }
        if let Some(code) = cmd.pointer("/payload/exec/code").and_then(Value::as_str) {
            for s in coin_call_literals(code) {
                push(&s);
            }
        }
    }
    out
}

/// [`balance_receivers`] of a stored record (none for the coinbase or a cmd that is not JSON).
pub fn record_receivers(r: &TxRecord, dict: &dyn Dict) -> Result<Vec<String>, RenderError> {
    let Some(cmd) = r.cmd.as_deref().filter(|_| r.key.pos() != 0) else {
        return Ok(Vec::new());
    };
    // Lenient parse: only string values are read, a faithful copy is not needed.
    let Ok(cmd) = serde_json::from_slice::<Value>(cmd) else {
        return Ok(Vec::new());
    };
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
    Ok(balance_receivers(&cmd, cont.as_ref(), r.key.height()))
}

fn data_strings(v: &Value, f: &mut impl FnMut(&str)) {
    match v {
        Value::String(s) => f(s),
        Value::Array(a) => a.iter().for_each(|x| data_strings(x, f)),
        Value::Object(m) => m.values().for_each(|x| data_strings(x, f)),
        _ => {}
    }
}

/// String literals (`"..."` with `\"`/`\\` unescaped, and `'symbols`) inside every call
/// whose head starts with `coin.`, nested forms included. `;` comments are skipped.
fn coin_call_literals(code: &str) -> Vec<String> {
    let b = code.as_bytes();
    let mut out = Vec::new();
    // One entry per open paren: whether it opened a `coin.` call.
    let mut stack: Vec<bool> = Vec::new();
    let mut inside = 0usize;
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
                if inside > 0 {
                    out.push(String::from_utf8_lossy(&s).into_owned());
                }
            }
            b'\'' => {
                let start = i + 1;
                let mut j = start;
                while j < b.len() && !is_delim(b[j]) {
                    j += 1;
                }
                if inside > 0 {
                    out.push(String::from_utf8_lossy(&b[start..j]).into_owned());
                }
                i = j;
                continue;
            }
            b'(' => {
                let mut j = i + 1;
                while j < b.len() && b[j].is_ascii_whitespace() {
                    j += 1;
                }
                let coin = b[j..].starts_with(b"coin.");
                stack.push(coin);
                inside += usize::from(coin);
            }
            b')' if stack.pop() == Some(true) => inside -= 1,
            _ => {}
        }
        i += 1;
    }
    out
}

fn is_delim(c: u8) -> bool {
    c.is_ascii_whitespace() || matches!(c, b'(' | b')' | b'[' | b']' | b'{' | b'}' | b'"' | b',')
}
