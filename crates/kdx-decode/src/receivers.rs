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
    /// Coinbase injection fix (chainweb `vuln797Fix`): below it the coinbase code was built by
    /// interpolating the miner account, so an account holding a `"` ran as code (D3 a).
    pub coinbase_fix: u32,
}

/// Mainnet01 fork heights.
pub const MAINNET01: Forks = Forks {
    events: 1_138_000,
    coin_v3: 1_722_500,
    coinbase_fix: 121_452,
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

// ---------------------------------------------------------------------------------------------
// D3: receivers of every fungible module (coin included) that no TRANSFER event names.
// ---------------------------------------------------------------------------------------------

/// The coinbase code chainweb built from the miner account before the injection fix, by
/// string interpolation (`(coin.coinbase "<account>" (read-keyset "miner-keyset") <reward>)`).
const COINBASE_HEAD: &str = "(coin.coinbase \"";
const COINBASE_TAIL: &str = "\" (read-keyset \"miner-keyset\") 0.0)";

/// D3 (a): the accounts an early coinbase really credited when the block's miner account
/// carries a `"`. Early mainnet built the coinbase code by interpolating the miner account, so
/// an account such as `<key>" (read-keyset "miner-keyset") 23045.23)(coin.coinbase "<key>`
/// ran as code and credited `<key>`, a name no posting ever states on its own. Returns the
/// [`account_like`] string literals of the interpolated code; empty for a plain account (its
/// own `acct` posting already marks it).
pub fn injected_miner_receivers(miner: &str) -> Vec<String> {
    if !miner.contains('"') {
        return Vec::new();
    }
    let code = format!("{COINBASE_HEAD}{miner}{COINBASE_TAIL}");
    let mut out: Vec<String> = Vec::new();
    for s in code_literals(&code) {
        if account_like(&s) && s != "miner-keyset" && !out.contains(&s) {
            out.push(s);
        }
    }
    out
}

/// D3 candidates of one tx: `(account, module)` pairs to re-read, and the modules the tx shows
/// to be fungible (`discovered`, for token discovery).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TokenReceivers {
    /// From the cross-chain and `exec` rules.
    pub pairs: Vec<(String, String)>,
    /// From events (rule 3), at most [`MAX_EVENT_PAIRS_PER_TX`]; never coin.
    pub event_pairs: Vec<(String, String)>,
    /// The module each `event_pairs` entry is charged to (same index): the event's emitter, or
    /// the token an `exec` calls. The pass budgets its per-pass cap per emitter.
    pub event_emitters: Vec<String>,
    /// Token rows of an `exec` (rule 2 for tokens), at most [`MAX_EXEC_PAIRS_PER_TX`], with
    /// the called token as emitter (same index in `exec_emitters`). Separate from the event
    /// budget, and filled after the events, so exec strings never starve a tx's events.
    pub exec_pairs: Vec<(String, String)>,
    pub exec_emitters: Vec<String>,
    pub discovered: Vec<String>,
}

/// Event-derived candidates kept per tx (security review: chain data must not amplify the
/// pass). Real credits name a handful of accounts per tx.
pub const MAX_EVENT_PAIRS_PER_TX: usize = 64;
/// Exec-derived token candidates kept per tx (own budget, see [`TokenReceivers::exec_pairs`]).
pub const MAX_EXEC_PAIRS_PER_TX: usize = 64;

/// Whether an event of `emitter` may name rows of `referenced` through a module reference: the
/// module itself, a tracked token, or a module of the same governed namespace (only the
/// namespace owner deploys there; `free` and `user` are open to anyone and do not count).
fn may_reference(emitter: &str, referenced: &str, is_token: &dyn Fn(&str) -> bool) -> bool {
    let ns = |m: &str| m.split_once('.').map(|(n, _)| n.to_owned());
    emitter == referenced
        || is_token(emitter)
        || matches!((ns(emitter), ns(referenced)),
            (Some(a), Some(b)) if a == b && a != "free" && a != "user")
}

impl TokenReceivers {
    fn push(&mut self, account: &str, module: &str) {
        if account_like(account) && !self.pairs.iter().any(|(a, m)| a == account && m == module) {
            self.pairs.push((account.to_owned(), module.to_owned()));
        }
    }
    /// A capped candidate (events, and token rows of an `exec`), charged to `emitter`.
    fn push_event(&mut self, account: &str, module: &str, emitter: &str) {
        if module != "coin"
            && account_like(account)
            && self.event_pairs.len() < MAX_EVENT_PAIRS_PER_TX
            && !self
                .event_pairs
                .iter()
                .any(|(a, m)| a == account && m == module)
        {
            self.event_pairs
                .push((account.to_owned(), module.to_owned()));
            self.event_emitters.push(emitter.to_owned());
        }
    }
    fn push_exec(&mut self, account: &str, module: &str) {
        if module != "coin"
            && account_like(account)
            && self.exec_pairs.len() < MAX_EXEC_PAIRS_PER_TX
            && !self
                .exec_pairs
                .iter()
                .any(|(a, m)| a == account && m == module)
        {
            self.exec_pairs
                .push((account.to_owned(), module.to_owned()));
            self.exec_emitters.push(module.to_owned());
        }
    }
    fn discover(&mut self, module: &str) {
        if module != "coin" && !self.discovered.iter().any(|m| m == module) {
            self.discovered.push(module.to_owned());
        }
    }
}

const XCHAIN_SUFFIX: &str = ".transfer-crosschain";

/// D3 candidates of a tx with parsed `cmd` (None when it is not JSON) and rendered `output`
/// (the CommandResult) at `height`. `is_token` says whether a module is a tracked fungible
/// token (coin is always one). Rules:
///
/// 1. **Cross-chain defpact of any module** (`<m>.transfer-crosschain`, any height): step 1
///    credits `args[1]` and step 0 debits `args[0]`. Tokens emit no TRANSFER for either
///    (only `pact.X_YIELD` / `X_RESUME`); coin did not before coin v3. `<m>` is discovered.
/// 2. **`exec`**: the D1 strings (`exec.data` values, `exec.code` literals) plus every string
///    of `result.data` (a receiver built at run time and returned, D3 b), for every tracked
///    token `<m>` the code calls (`<m>.fn`; some token credits emit no event of the token at
///    all), and for coin below the events fork.
/// 3. **Events** (any height, into `event_pairs`, capped at [`MAX_EVENT_PAIRS_PER_TX`], never
///    coin): an event of a token that is not a well-formed TRANSFER (`WRAP`, `UNWRAP`, `MINT`,
///    …) gives its [`account_like`] params for that token; an event whose params hold a
///    `fungible-v2` module reference gives them for the referenced module, which is
///    discovered (D3 d: `kaddex.skdx` is only ever minted that way, by `kaddex.alchemist`),
///    when the emitter may name it ([`may_reference`]). Strings inside a module reference
///    are not accounts.
///
/// Over-inclusion costs one `/local` read per extra row (answered absent, stored, never
/// served); a missing candidate leaves a funded row missing.
pub fn token_receivers(
    cmd: Option<&Value>,
    output: &Value,
    height: u32,
    forks: Forks,
    is_token: &dyn Fn(&str) -> bool,
) -> TokenReceivers {
    let mut r = TokenReceivers::default();
    // A failed tx credits nothing (security review).
    if output.pointer("/result/status").and_then(Value::as_str) != Some("success") {
        return r;
    }
    let is_cont = cmd.is_some_and(|c| c.pointer("/payload/cont").is_some());
    if let Some(c) = output.get("continuation").filter(|c| !c.is_null()) {
        let def = c.pointer("/continuation/def").and_then(Value::as_str);
        if let Some(m) = def.and_then(|d| d.strip_suffix(XCHAIN_SUFFIX)) {
            let side = match c.get("step").and_then(Value::as_u64) {
                Some(1) if is_cont => Some(1),
                Some(0) if !is_cont => Some(0),
                _ => None,
            };
            if let Some(a) = side.and_then(|i| c.pointer(&format!("/continuation/args/{i}"))) {
                if let Some(a) = a.as_str() {
                    r.push(a, m);
                }
                r.discover(m);
            }
        }
    }
    for e in output
        .get("events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(ev) = crate::build::parse_event(e) else {
            continue;
        };
        let mut targets: Vec<String> = Vec::new();
        modrefs(ev.params, &mut |m| {
            if m != "coin"
                && may_reference(&ev.module, m, is_token)
                && !targets.iter().any(|t| t == m)
            {
                targets.push(m.to_owned());
            }
        });
        targets.iter().for_each(|m| r.discover(m));
        if ev.module != "coin"
            && is_token(&ev.module)
            && crate::build::transfer_accounts(&ev).is_none()
            && !targets.contains(&ev.module)
        {
            targets.push(ev.module.clone());
        }
        if targets.is_empty() {
            continue;
        }
        let mut strings = Vec::new();
        param_strings(ev.params, &mut strings);
        for m in &targets {
            for s in &strings {
                r.push_event(s, m, &ev.module);
            }
        }
    }
    if let (Some(c), false) = (cmd, is_cont) {
        let code = c
            .pointer("/payload/exec/code")
            .and_then(Value::as_str)
            .unwrap_or("");
        // Coin only below the events fork (from there on every exec coin credit has an event).
        let mut modules = Vec::new();
        if height < forks.events {
            modules.push("coin".to_owned());
        }
        // From the events fork on, a token whose own events appear in the tx already has its
        // accounts named there (TRANSFER marks them; rule 3 the rest): only a token call that
        // emitted nothing of the token at all needs the exec strings (dry run on KDA-03: without
        // this, millions of extra reads).
        let emitted: Vec<String> = output
            .get("events")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(crate::build::parse_event)
            .map(|e| e.module)
            .collect();
        for m in called_modules(code) {
            if height >= forks.events && emitted.contains(&m) {
                continue;
            }
            if is_token(&m) && !modules.contains(&m) {
                modules.push(m);
            }
        }
        let mut strings = Vec::new();
        if let Some(d) = c.pointer("/payload/exec/data") {
            data_strings(d, &mut |s| strings.push(s.to_owned()));
        }
        strings.extend(code_literals(code));
        if let Some(d) = output.pointer("/result/data") {
            data_strings(d, &mut |s| strings.push(s.to_owned()));
        }
        // Coin rows (below the events fork) as in D1; token rows are capped like events.
        for m in &modules {
            for s in &strings {
                if m == "coin" {
                    r.push(s, m);
                } else {
                    r.push_exec(s, m);
                }
            }
        }
    }
    r
}

/// The modules `code` calls as `<module>.<fn>` (`ns.mod.fn` → `ns.mod`, `mod.fn` → `mod`),
/// outside string literals, `'symbols` and `;` comments (security review: a literal is data,
/// not a call).
fn called_modules(code: &str) -> Vec<String> {
    let b = code.as_bytes();
    let mut bare = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b';' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'"' => {
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    i += if b[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
            }
            b'\'' => {
                i += 1;
                while i < b.len() && !is_delim(b[i]) {
                    i += 1;
                }
            }
            c => {
                bare.push(c);
                i += 1;
                continue;
            }
        }
        bare.push(b' ');
    }
    let bare = String::from_utf8_lossy(&bare);
    let mut out = Vec::new();
    for w in bare.split(|c: char| is_delim(c as u8) || !c.is_ascii()) {
        if let Some((m, f)) = w.rsplit_once('.') {
            if !m.is_empty() && !f.is_empty() && !out.iter().any(|x: &String| x == m) {
                out.push(m.to_owned());
            }
        }
    }
    out
}

/// A `fungible-v2` module reference (`{"refSpec": [.. {"name": "fungible-v2"} ..],
/// "refName": {"namespace", "name"}}`) gives its qualified module name.
fn fungible_modref(v: &serde_json::Map<String, Value>) -> Option<String> {
    let spec = v.get("refSpec")?.as_array()?;
    let fungible = spec.iter().any(|s| {
        s.get("name").and_then(Value::as_str) == Some("fungible-v2")
            && s.get("namespace").is_none_or(Value::is_null)
    });
    let n = v.get("refName")?;
    let name = n.get("name")?.as_str()?;
    if !fungible || name.is_empty() {
        return None;
    }
    Some(match n.get("namespace").and_then(Value::as_str) {
        Some(ns) if !ns.is_empty() => format!("{ns}.{name}"),
        _ => name.to_owned(),
    })
}

fn modrefs(v: &Value, f: &mut impl FnMut(&str)) {
    match v {
        Value::Array(a) => a.iter().for_each(|x| modrefs(x, f)),
        Value::Object(m) if m.contains_key("refSpec") => {
            if let Some(n) = fungible_modref(m) {
                f(&n);
            }
        }
        Value::Object(m) => m.values().for_each(|x| modrefs(x, f)),
        _ => {}
    }
}

/// String params at any depth, module references skipped.
fn param_strings(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::String(s) => out.push(s.clone()),
        Value::Array(a) => a.iter().for_each(|x| param_strings(x, out)),
        Value::Object(m) if m.contains_key("refSpec") => {}
        Value::Object(m) => m.values().for_each(|x| param_strings(x, out)),
        _ => {}
    }
}

/// [`token_receivers`] of a stored record (none for the coinbase; an error for a cmd that is
/// not JSON or an output that does not render).
pub fn record_token_receivers(
    r: &TxRecord,
    dict: &dyn Dict,
    forks: Forks,
    is_token: &dyn Fn(&str) -> bool,
) -> Result<TokenReceivers, RenderError> {
    let Some(cmd) = r.cmd.as_deref().filter(|_| r.key.pos() != 0) else {
        return Ok(TokenReceivers::default());
    };
    let cmd = serde_json::from_slice::<Value>(cmd)
        .map_err(|e| RenderError::Shape(format!("cmd is not JSON: {e}")))?;
    let out = render_output(r, dict)?;
    Ok(token_receivers(
        Some(&cmd),
        &out,
        r.key.height(),
        forks,
        is_token,
    ))
}
