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
    /// Token rows of an `exec` (rule 2 for tokens), at most [`MAX_EXEC_PAIRS_PER_TX`], charged
    /// to the tx's sender (`exec_emitters`: [`EXEC_BUCKET`] + sender, same index), never to the
    /// called token: anyone can call a token with junk strings, and that must not spend the
    /// token's own budget (security review). Filled after the events, so exec strings never
    /// starve a tx's events.
    pub exec_pairs: Vec<(String, String)>,
    pub exec_emitters: Vec<String>,
    /// Modules that may be fungible tokens, at most [`MAX_DISCOVERED_PER_TX`]: the module of
    /// a cross-chain defpact, and a `fungible-v2` modref named by its own module or a module of
    /// the same governed namespace (review M1). A name here is only a claim (a plain object
    /// renders like a modref): it becomes a tracked token only after the node's
    /// `describe-module` lists `fungible-v2` on that chain (`seed_receivers` discovery queue).
    pub discovered: Vec<String>,
}

/// Event-derived candidates kept per tx (security review: chain data must not amplify the
/// pass). Real credits name a handful of accounts per tx.
pub const MAX_EVENT_PAIRS_PER_TX: usize = 64;
/// Exec-derived token candidates kept per tx (own budget, see [`TokenReceivers::exec_pairs`]).
pub const MAX_EXEC_PAIRS_PER_TX: usize = 64;
/// Prefix of the budget key of exec-derived rows: `exec:<sender>` (a module name holds no `:`).
pub const EXEC_BUCKET: &str = "exec:";
/// Modules one tx may put forward as tokens (review M1).
pub const MAX_DISCOVERED_PER_TX: usize = 4;
/// Module references kept per event (each one with a string uses a pair slot of the tx, so
/// more can never add a pair).
const MAX_REFS_PER_EVENT: usize = MAX_EVENT_PAIRS_PER_TX;

/// Namespaces where anyone may deploy: a module there proves nothing about its neighbours
/// (review L3: add any other namespace whose user guard is open).
pub const OPEN_NAMESPACES: &[&str] = &["free", "user"];

/// `emitter` and `referenced` sit in the same governed namespace (only its owner deploys there).
fn same_governed_namespace(emitter: &str, referenced: &str) -> bool {
    let ns = |m: &str| m.split_once('.').map(|(n, _)| n.to_owned());
    matches!((ns(emitter), ns(referenced)),
        (Some(a), Some(b)) if a == b && !OPEN_NAMESPACES.contains(&a.as_str()))
}

/// A module whose rows `/local` can read (B-1): `coin`, or `ident` / `ns.ident` with ASCII
/// identifiers (the rule of `kdx_node::local::valid_module`; e.g. a non-ASCII name Pact 5
/// cannot parse is never read, marked or tracked).
pub fn readable_module(m: &str) -> bool {
    fn ident(s: &str) -> bool {
        let mut b = s.bytes();
        matches!(b.next(), Some(c) if c.is_ascii_alphabetic() || c == b'_')
            && b.all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
    }
    let mut parts = m.splitn(2, '.');
    let first = parts.next().unwrap_or("");
    m == "coin" || (ident(first) && parts.next().is_none_or(ident))
}

impl TokenReceivers {
    fn push(&mut self, account: &str, module: &str) {
        if account_like(account)
            && readable_module(module)
            && !self.pairs.iter().any(|(a, m)| a == account && m == module)
        {
            self.pairs.push((account.to_owned(), module.to_owned()));
        }
    }
    /// A capped candidate (events, and token rows of an `exec`), charged to `emitter`.
    fn push_event(&mut self, account: &str, module: &str, emitter: &str) {
        if module != "coin"
            && account_like(account)
            && readable_module(module)
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
    fn push_exec(&mut self, account: &str, module: &str, sender: &str) {
        if module != "coin"
            && account_like(account)
            && readable_module(module)
            && self.exec_pairs.len() < MAX_EXEC_PAIRS_PER_TX
            && !self
                .exec_pairs
                .iter()
                .any(|(a, m)| a == account && m == module)
        {
            self.exec_pairs
                .push((account.to_owned(), module.to_owned()));
            self.exec_emitters.push(format!("{EXEC_BUCKET}{sender}"));
        }
    }
    fn discover(&mut self, module: &str) {
        if module != "coin"
            && readable_module(module)
            && self.discovered.len() < MAX_DISCOVERED_PER_TX
            && !self.discovered.iter().any(|m| m == module)
        {
            self.discovered.push(module.to_owned());
        }
    }
    /// Every `(account, module)` of the tx: pairs, then capped event and exec rows.
    pub fn all_pairs(&self) -> impl Iterator<Item = (&(String, String), &str)> {
        self.pairs
            .iter()
            .map(|p| (p, p.1.as_str()))
            .chain(
                self.event_pairs
                    .iter()
                    .zip(self.event_emitters.iter().map(String::as_str)),
            )
            .chain(
                self.exec_pairs
                    .iter()
                    .zip(self.exec_emitters.iter().map(String::as_str)),
            )
    }
}

const XCHAIN_SUFFIX: &str = ".transfer-crosschain";

/// One event of a tx as D3 needs it (no tracked-token knowledge yet).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct D3Event {
    /// The emitting module.
    pub module: String,
    /// A well-formed TRANSFER (its accounts are postings already).
    pub transfer: bool,
    /// `fungible-v2` modref targets in the params (coin excluded), at most 64.
    pub refs: Vec<String>,
    /// Distinct [`account_like`] string params outside modrefs, at most
    /// [`MAX_EVENT_PAIRS_PER_TX`] (the tx's event rows are capped at that anyway).
    pub strings: Vec<String>,
}

/// The `exec` part of a tx as D3 needs it.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct D3Exec {
    /// Below the events fork: the strings count for coin too (uncapped).
    pub coin: bool,
    /// Modules the code calls (`<m>.fn`), minus those that emitted an event in the tx from the
    /// events fork on; the filter keeps the tracked tokens.
    pub called: Vec<String>,
    /// Distinct [`account_like`] strings of `exec.data`, `exec.code` literals and
    /// `result.data`, first-seen order; capped at [`MAX_EXEC_PAIRS_PER_TX`] unless `coin`.
    pub strings: Vec<String>,
    /// `meta.sender` of the cmd (empty when absent): the budget key of the token rows.
    pub sender: String,
}

/// D3 raw candidates of one tx: everything [`d3_filter`] needs, without knowing which modules
/// are tracked. Computed once per tx at decode time for the tail (`TxIndex::d3`), and by the
/// seed pass from stored records, so both mark the same rows (ruling R-P5P9-192).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct D3Candidates {
    /// Rule 1: the module of a cross-chain defpact step of this tx, and the account it credits
    /// (step 1) or debits (step 0), when that argument is a string.
    pub xchain: Option<(String, Option<String>)>,
    /// Rule 3.
    pub events: Vec<D3Event>,
    /// Rule 2 (an `exec` tx with a parsed cmd).
    pub exec: Option<D3Exec>,
}

impl D3Candidates {
    pub fn is_empty(&self) -> bool {
        self.xchain.is_none() && self.events.is_empty() && self.exec.is_none()
    }
}

fn push_distinct(v: &mut Vec<String>, s: &str, cap: usize) {
    if v.len() < cap && account_like(s) && !v.iter().any(|x| x == s) {
        v.push(s.to_owned());
    }
}

/// The raw D3 candidates of a tx with parsed `cmd` (None when it is not JSON) and `output`
/// (the CommandResult) at `height` (pure: no tracked-token knowledge). A failed tx gives none.
pub fn d3_candidates(
    cmd: Option<&Value>,
    output: &Value,
    height: u32,
    forks: Forks,
) -> D3Candidates {
    let mut c = D3Candidates::default();
    // A failed tx credits nothing (security review).
    if output.pointer("/result/status").and_then(Value::as_str) != Some("success") {
        return c;
    }
    let is_cont = cmd.is_some_and(|c| c.pointer("/payload/cont").is_some());
    if let Some(k) = output.get("continuation").filter(|k| !k.is_null()) {
        let def = k.pointer("/continuation/def").and_then(Value::as_str);
        if let Some(m) = def.and_then(|d| d.strip_suffix(XCHAIN_SUFFIX)) {
            let side = match k.get("step").and_then(Value::as_u64) {
                Some(1) if is_cont => Some(1),
                Some(0) if !is_cont => Some(0),
                _ => None,
            };
            if let Some(a) = side.and_then(|i| k.pointer(&format!("/continuation/args/{i}"))) {
                c.xchain = Some((m.to_owned(), a.as_str().map(str::to_owned)));
            }
        }
    }
    let mut emitted: Vec<String> = Vec::new();
    for e in output
        .get("events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(ev) = crate::build::parse_event(e) else {
            continue;
        };
        if !emitted.contains(&ev.module) {
            emitted.push(ev.module.clone());
        }
        let mut d = D3Event {
            module: ev.module.clone(),
            transfer: crate::build::transfer_accounts(&ev).is_some(),
            ..D3Event::default()
        };
        modrefs(ev.params, &mut |m| {
            if m != "coin" && d.refs.len() < MAX_REFS_PER_EVENT && !d.refs.iter().any(|t| t == m) {
                d.refs.push(m.to_owned());
            }
        });
        let mut strings = Vec::new();
        param_strings(ev.params, &mut strings);
        for s in &strings {
            push_distinct(&mut d.strings, s, MAX_EVENT_PAIRS_PER_TX);
        }
        // An event with no string can give no row; one with no ref and a TRANSFER neither.
        if !d.strings.is_empty() || !d.refs.is_empty() {
            c.events.push(d);
        }
    }
    if let (Some(cmd), false) = (cmd, is_cont) {
        let code = cmd
            .pointer("/payload/exec/code")
            .and_then(Value::as_str)
            .unwrap_or("");
        let coin = height < forks.events;
        // From the events fork on, a token whose own events appear in the tx already has its
        // accounts named there (TRANSFER marks them; rule 3 the rest): only a token call that
        // emitted nothing of the token at all needs the exec strings (dry run on KDA-03: without
        // this, millions of extra reads).
        let called: Vec<String> = called_modules(code)
            .into_iter()
            .filter(|m| m != "coin" && (coin || !emitted.contains(m)))
            .collect();
        if coin || !called.is_empty() {
            let cap = if coin {
                usize::MAX
            } else {
                MAX_EXEC_PAIRS_PER_TX
            };
            let mut strings = Vec::new();
            let mut add = |s: &str| push_distinct(&mut strings, s, cap);
            if let Some(d) = cmd.pointer("/payload/exec/data") {
                data_strings(d, &mut add);
            }
            for s in code_literals(code) {
                add(&s);
            }
            if let Some(d) = output.pointer("/result/data") {
                data_strings(d, &mut add);
            }
            let sender = cmd
                .pointer("/meta/sender")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            c.exec = Some(D3Exec {
                coin,
                called,
                strings,
                sender,
            });
        }
    }
    c
}

/// The rows of `c` given which modules are tracked fungible tokens (`is_token`; coin is always
/// one). Rules:
///
/// 1. **Cross-chain defpact of any module** (`<m>.transfer-crosschain`, any height): step 1
///    credits `args[1]` and step 0 debits `args[0]`. Tokens emit no TRANSFER for either
///    (only `pact.X_YIELD` / `X_RESUME`); coin did not before coin v3. `<m>` is discovered.
/// 2. **`exec`**: the D1 strings (`exec.data` values, `exec.code` literals) plus every string
///    of `result.data` (a receiver built at run time and returned, D3 b), for every tracked
///    token `<m>` the code calls (`<m>.fn`; from the events fork on only one that emitted
///    nothing in the tx), and for coin below the events fork.
/// 3. **Events** (any height, into `event_pairs`, capped at [`MAX_EVENT_PAIRS_PER_TX`], never
///    coin): an event of a token that is not a well-formed TRANSFER (`WRAP`, `UNWRAP`, `MINT`,
///    …) gives its [`account_like`] params for that token; an event whose params hold a
///    `fungible-v2` module reference gives them for the referenced module when the emitter is
///    that module, a module of the same governed namespace (D3 d: `kaddex.skdx` is only ever
///    minted that way, by `kaddex.alchemist`), or a tracked token naming another tracked
///    token. Only the first two discover the referenced module (review M1). Strings inside a
///    module reference are not accounts.
///
/// Rows and discoveries of a module `/local` cannot read ([`readable_module`]) are dropped.
pub fn d3_filter(c: &D3Candidates, is_token: &dyn Fn(&str) -> bool) -> TokenReceivers {
    let mut r = TokenReceivers::default();
    if let Some((m, a)) = &c.xchain {
        if let Some(a) = a {
            r.push(a, m);
        }
        r.discover(m);
    }
    for ev in &c.events {
        let mut targets: Vec<&str> = Vec::new();
        for m in &ev.refs {
            let own = ev.module == *m || same_governed_namespace(&ev.module, m);
            if own {
                r.discover(m);
            }
            if (own || (is_token(&ev.module) && is_token(m))) && !targets.contains(&m.as_str()) {
                targets.push(m);
            }
        }
        if ev.module != "coin"
            && is_token(&ev.module)
            && !ev.transfer
            && !targets.contains(&ev.module.as_str())
        {
            targets.push(&ev.module);
        }
        for m in targets {
            for s in &ev.strings {
                r.push_event(s, m, &ev.module);
            }
        }
    }
    if let Some(x) = &c.exec {
        let mut modules: Vec<&str> = Vec::new();
        if x.coin {
            modules.push("coin");
        }
        for m in &x.called {
            if is_token(m) && !modules.contains(&m.as_str()) {
                modules.push(m);
            }
        }
        // Coin rows (below the events fork) as in D1; token rows are capped like events.
        for m in modules {
            for s in &x.strings {
                if m == "coin" {
                    r.push(s, m);
                } else {
                    r.push_exec(s, m, &x.sender);
                }
            }
        }
    }
    r
}

/// [`d3_filter`] of [`d3_candidates`]: the D3 rows of one tx.
pub fn token_receivers(
    cmd: Option<&Value>,
    output: &Value,
    height: u32,
    forks: Forks,
    is_token: &dyn Fn(&str) -> bool,
) -> TokenReceivers {
    d3_filter(&d3_candidates(cmd, output, height, forks), is_token)
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

/// [`d3_candidates`] of a stored record (none for the coinbase; an error for a cmd that is
/// not JSON or an output that does not render).
pub fn record_d3_candidates(
    r: &TxRecord,
    dict: &dyn Dict,
    forks: Forks,
) -> Result<D3Candidates, RenderError> {
    let Some(cmd) = r.cmd.as_deref().filter(|_| r.key.pos() != 0) else {
        return Ok(D3Candidates::default());
    };
    let cmd = serde_json::from_slice::<Value>(cmd)
        .map_err(|e| RenderError::Shape(format!("cmd is not JSON: {e}")))?;
    let out = render_output(r, dict)?;
    Ok(d3_candidates(Some(&cmd), &out, r.key.height(), forks))
}

/// [`token_receivers`] of a stored record (none for the coinbase; an error for a cmd that is
/// not JSON or an output that does not render).
pub fn record_token_receivers(
    r: &TxRecord,
    dict: &dyn Dict,
    forks: Forks,
    is_token: &dyn Fn(&str) -> bool,
) -> Result<TokenReceivers, RenderError> {
    Ok(d3_filter(&record_d3_candidates(r, dict, forks)?, is_token))
}
