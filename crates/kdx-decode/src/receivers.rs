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

use std::collections::HashMap;

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
    /// From events (rule 3), at most [`MAX_EVENT_PAIRS_PER_EVENT`] new rows per event and
    /// [`MAX_EVENT_PAIRS_PER_TX`] per tx; never coin.
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
    /// Discoverable names over [`MAX_MODULE_LEN`] bytes, dropped (re-review N1).
    pub discovered_too_long: u32,
    /// Rule 2b drops, each counted once (by the tail and the pass, never an error): tracked
    /// references over [`MAX_MODREFS_PER_TX`] and rows over [`MAX_MODREF_PAIRS_PER_TX`].
    pub modref_capped: u32,
}

/// Event-derived candidates one event may add for itself (security review: chain data must not
/// amplify the pass). Also the most strings and module references kept from one event. The
/// allowance is per event and spent only by that event's own rows: an event that yields no row
/// (junk) funds nothing, so it cannot raise another event's budget.
pub const MAX_EVENT_PAIRS_PER_EVENT: usize = 64;
/// Ceiling of event-derived candidates per tx, whatever its event count: half of the tail's
/// per-emitter block budget (`kdx_hot::tail::D3_ROWS_PER_EMITTER_PER_BLOCK`, 1,024). A batch tx
/// (D3 gap group 1: a kaddex migration WRAP tx credits 45 accounts in 90 events, about two
/// rows each) needs about 94; the old fixed 64 per tx kept only its first 21 accounts.
pub const MAX_EVENT_PAIRS_PER_TX: usize = 512;
/// Exec-derived token candidates kept per tx (own budget, see [`TokenReceivers::exec_pairs`]).
pub const MAX_EXEC_PAIRS_PER_TX: usize = 64;

/// Accounts rule 2c reads exactly from one tx's `coin.create-account` calls, at most (gap
/// Q), counting only data-backed calls (see [`coin_create_accounts`]). Not the
/// [`MAX_EXEC_PAIRS_PER_TX`] cap: the bulk creators send 200 calls per tx (mainnet chain 12,
/// heights 1.55M-2.16M), each guarded by its own `(read-keyset "ks<i>")` of `exec.data`.
/// Gas does not bound the count: a call in an untaken branch or an uncalled `defun` is
/// collected for parse gas alone (secpass A I-2). What bounds a data-backed call is the
/// `exec.data` entry it needs, which the sender pays bytes for.
pub const MAX_COIN_CREATE_PER_TX: usize = 1_000;
/// Literal-only `coin.create-account` calls (account and guard both stated in the code, no
/// `exec.data` entry behind them) kept per tx: 64, the exec-literal cap before #56
/// ([`MAX_EXEC_PAIRS_PER_TX`]). Such calls cost only parse gas when they never run.
pub const MAX_COIN_CREATE_LITERALS_PER_TX: usize = 64;
/// Exec-derived token candidates kept from a module-deploy tx (D3 gap group 5): its code holds
/// the module's own constants (a `ROOT` or genesis account its init credits) among many other
/// literals. Literals with whitespace (doc strings, messages) are skipped there; the rest are
/// charged to the sender like any exec row.
pub const MAX_DEPLOY_EXEC_PAIRS: usize = 512;
/// Rule 2b rows (a tracked token named in code) kept per tx: the sender of every kept token
/// first, then the account-like string arguments of the calls passing it. The excess is
/// dropped and counted ([`TokenReceivers::modref_capped`]).
pub const MAX_MODREF_PAIRS_PER_TX: usize = 16;
/// Tracked tokens kept per tx for rule 2b, counted after the tracked-token filter (review I1:
/// untracked names never spend it); the rest are dropped and counted.
pub const MAX_MODREFS_PER_TX: usize = 8;
/// Account-like strings read from one call for rule 2b (a memory bound only: rows over
/// [`MAX_MODREF_PAIRS_PER_TX`] are counted in [`TokenReceivers::modref_capped`]).
const MAX_MODREF_STRINGS_PER_CALL: usize = 64;
/// Modules one deploy tx may count as called.
const MAX_DEPLOYED_PER_TX: usize = 4;
/// Prefix of the budget key of exec-derived rows: `exec:<sender>` (a module name holds no `:`).
pub const EXEC_BUCKET: &str = "exec:";
/// Modules one tx may put forward as tokens (review M1).
pub const MAX_DISCOVERED_PER_TX: usize = 4;
/// Longest discovered module name kept (bytes; re-review N1): a discovered name becomes an
/// MDBX key (the discovery queue and pending list), whose limit is about 2 KB. Real module
/// names are far shorter; a longer one is dropped and counted
/// ([`TokenReceivers::discovered_too_long`]), never an error.
pub const MAX_MODULE_LEN: usize = 256;
/// Module references kept per event (each one with a string uses a pair slot of the tx, so
/// more can never add a pair).
const MAX_REFS_PER_EVENT: usize = MAX_EVENT_PAIRS_PER_EVENT;

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
    fn push_event(&mut self, account: &str, module: &str, emitter: &str, cap: usize) {
        if module != "coin"
            && account_like(account)
            && readable_module(module)
            && self.event_pairs.len() < cap
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
    /// An exec row, counted in `used` (its own budget: normal calls or a deployed module).
    fn push_exec(
        &mut self,
        account: &str,
        module: &str,
        sender: &str,
        used: &mut usize,
        cap: usize,
    ) {
        if module != "coin"
            && account_like(account)
            && readable_module(module)
            && *used < cap
            && !self
                .exec_pairs
                .iter()
                .any(|(a, m)| a == account && m == module)
        {
            self.exec_pairs
                .push((account.to_owned(), module.to_owned()));
            self.exec_emitters.push(format!("{EXEC_BUCKET}{sender}"));
            *used += 1;
        }
    }
    fn discover(&mut self, module: &str) {
        if module.len() > MAX_MODULE_LEN {
            if readable_module(module) {
                self.discovered_too_long += 1;
            }
            return;
        }
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

/// Cross-chain defpacts by their own name (the part after the module's last `.`, matched
/// exactly): `(name, argument step 0 debits, argument step 1 credits)`.
/// - `fungible-xchain-v1`'s `(transfer-crosschain sender receiver guard target-chain amount)`;
/// - the Hyperlane bridge tokens' (`kb-*`, Kadena-ISS `syn-template.pact` /
///   `col-template.pact`) `(transfer-create-to-crosschain receiver guard amount target-chain)`:
///   its step 0 runs inside the mailbox's `handle` and debits nobody, its step 1 credits
///   `args[0]` (D3 gap group 6, G4 rows #4–#7). Their other credits are covered elsewhere:
///   `transfer`/`transfer-create` emit TRANSFER, `handle` emits `RECEIVED_TRANSFER_REMOTE`
///   (rule 3), and `transfer-remote` burns the sender of a `mailbox.dispatch` (rule 2b).
const XCHAIN_DEFPACTS: &[(&str, Option<usize>, Option<usize>)] = &[
    ("transfer-crosschain", Some(0), Some(1)),
    ("transfer-create-to-crosschain", None, Some(0)),
];

/// One event of a tx as D3 needs it (no tracked-token knowledge yet).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct D3Event {
    /// The emitting module.
    pub module: String,
    /// A well-formed TRANSFER (its accounts are postings already).
    pub transfer: bool,
    /// `fungible-v2` modref targets in the params (coin excluded), at most 64.
    pub refs: Vec<String>,
    /// Distinct [`account_like`] string params outside modrefs and amounts, at most
    /// [`MAX_EVENT_PAIRS_PER_EVENT`].
    pub strings: Vec<String>,
}

/// The `exec` part of a tx as D3 needs it.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct D3Exec {
    /// The strings count for coin too: below the events fork (uncapped).
    pub coin: bool,
    /// Rule 2c: when the code calls `coin.create-account` from the events fork on, the
    /// [`account_like`] code literals, then the `exec.data` strings, distinct, at most
    /// [`MAX_EXEC_PAIRS_PER_TX`]. Never `result.data` (`Write succeeded`) or a keyset
    /// predicate (`keys-all`): they are never accounts. Empty otherwise.
    pub coin_create: Vec<String>,
    /// Rule 2c, exact (gap Q): the first argument of every `coin.create-account` call of the
    /// code, read from the parsed code ([`coin_create_accounts`]): a string literal, or
    /// `(read-msg k)` / `(read-string k)` of a string in `exec.data`. At most
    /// [`MAX_COIN_CREATE_PER_TX`] data-backed and [`MAX_COIN_CREATE_LITERALS_PER_TX`]
    /// literal-only, [`account_like`] only, first-seen order. Interned at
    /// decode time, so the tail marks them live. Empty below the events fork (rule 1 then
    /// counts every exec string) and when the code makes no such call.
    pub coin_created: Vec<String>,
    /// Modules the code calls (`<m>.fn`) and does not deploy, minus those that emitted an
    /// event in the tx from the events fork on; the filter keeps the tracked tokens.
    pub called: Vec<String>,
    /// Modules the code deploys (`(module <m> ..)`, which then runs its own functions
    /// unqualified), same event rule. Only they get [`Self::deploy_strings`].
    pub deployed: Vec<String>,
    /// Distinct [`account_like`] strings of `exec.data`, `exec.code` literals and
    /// `result.data`, first-seen order; capped at [`MAX_EXEC_PAIRS_PER_TX`] unless `coin`.
    pub strings: Vec<String>,
    /// For the deployed modules only: distinct code literals without whitespace, at most
    /// [`MAX_DEPLOY_EXEC_PAIRS`] (data and result strings never get the deploy allowance).
    pub deploy_strings: Vec<String>,
    /// `meta.sender` of the cmd (empty when absent): the budget key of the token rows.
    pub sender: String,
    /// Rule 2b: every module named in the code (a dotted reference anywhere, or the module of
    /// a qualified function; see `modref_args`), same event rule as `called`, each with the
    /// direct [`account_like`] string arguments of the qualified calls of another module that
    /// pass it (`(ns.mailbox.dispatch ns.kb-USDC 1 "<to>" 1.0)`), given as indices into
    /// [`Self::ref_calls`], first-seen order. Not capped: [`d3_filter`] caps the tracked ones
    /// and attaches their strings (at most [`MAX_MODREF_STRINGS_PER_CALL`] each) only then.
    pub refs: Vec<ModRef>,
    /// Rule 2b: the direct [`account_like`] string arguments of each qualified call of another
    /// module that passes a name, at most [`MAX_MODREF_STRINGS_PER_CALL`] per call, stored once
    /// per call however many names it passes (review I2), so the size stays linear in the code.
    pub ref_calls: Vec<Vec<String>>,
}

/// A module named in code with the indices of the [`D3Exec::ref_calls`] that pass it.
pub type ModRef = (String, Vec<u32>);

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

/// Keyset predicates built into Pact: never account names (rule 2c skips them).
const KEYSET_PREDS: [&str; 3] = ["keys-all", "keys-any", "keys-2"];

/// [`data_strings`] for rule 2c: skips a keyset's `pred` value (an object with `keys` and
/// `pred`) and the built-in predicate names wherever they sit.
fn create_data_strings(v: &Value, f: &mut impl FnMut(&str)) {
    match v {
        Value::String(s) if !KEYSET_PREDS.contains(&s.as_str()) => f(s),
        Value::Array(a) => a.iter().for_each(|x| create_data_strings(x, f)),
        Value::Object(m) => {
            let keyset = m.contains_key("keys") && m.contains_key("pred");
            for (k, x) in m {
                if !(keyset && k == "pred") {
                    create_data_strings(x, f);
                }
            }
        }
        _ => {}
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
        let xchain = def.and_then(|d| d.rsplit_once('.')).and_then(|(m, name)| {
            XCHAIN_DEFPACTS
                .iter()
                .find(|x| x.0 == name)
                .map(|&(_, debit, credit)| (m, debit, credit))
        });
        if let Some((m, debit, credit)) = xchain.filter(|x| !x.0.is_empty()) {
            let side = match k.get("step").and_then(Value::as_u64) {
                Some(1) if is_cont => Some(credit),
                Some(0) if !is_cont => Some(debit),
                _ => None,
            };
            if let Some(arg) = side {
                let a = arg
                    .and_then(|i| k.pointer(&format!("/continuation/args/{i}")))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                c.xchain = Some((m.to_owned(), a));
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
        for s in strings.iter().filter(|s| !amount_text(s)) {
            push_distinct(&mut d.strings, s, MAX_EVENT_PAIRS_PER_EVENT);
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
        // Rule 2c (zero rows, gap Q): `coin.create-account` writes a row at 0 and no event
        // names its account at any height, so its strings count for coin (capped).
        let coin_create = !coin && calls_coin_create(code);
        // From the events fork on, a token whose own events appear in the tx already has its
        // accounts named there (TRANSFER marks them; rule 3 the rest): only a token call that
        // emitted nothing of the token at all needs the exec strings (dry run on KDA-03: without
        // this, millions of extra reads).
        let data = cmd.pointer("/payload/exec/data");
        let keep = |m: &String| m != "coin" && (coin || !emitted.contains(m));
        let deployed: Vec<String> = deployed_modules(code, data)
            .into_iter()
            .filter(keep)
            .collect();
        let mut called: Vec<String> = Vec::new();
        for m in called_modules(code).into_iter().filter(keep) {
            if !called.contains(&m) && !deployed.contains(&m) {
                called.push(m);
            }
        }
        let (refs, ref_calls) = modref_args(code);
        let refs: Vec<ModRef> = refs.into_iter().filter(|(m, _)| keep(m)).collect();
        // Rule 2c: code literals first, so a batch create-account reaches the account names
        // (literals) before keyset data can fill the cap (review minor 2).
        let mut create_strings = Vec::new();
        let coin_created = if coin_create {
            coin_create_accounts(code, data)
        } else {
            Vec::new()
        };
        if coin_create {
            let literals = code_literals(code);
            let mut add = |s: &str| {
                if !KEYSET_PREDS.contains(&s) {
                    push_distinct(&mut create_strings, s, MAX_EXEC_PAIRS_PER_TX);
                }
            };
            for s in &literals {
                add(s);
            }
            if let Some(d) = data {
                create_data_strings(d, &mut add);
            }
        }
        if coin || coin_create || !called.is_empty() || !deployed.is_empty() || !refs.is_empty() {
            let cap = if coin {
                usize::MAX
            } else {
                MAX_EXEC_PAIRS_PER_TX
            };
            let mut strings = Vec::new();
            let mut deploy_strings = Vec::new();
            let mut add = |s: &str| push_distinct(&mut strings, s, cap);
            if let Some(d) = data {
                data_strings(d, &mut add);
            }
            let literals = code_literals(code);
            for s in &literals {
                add(s);
            }
            if let Some(d) = output.pointer("/result/data") {
                data_strings(d, &mut add);
            }
            if !deployed.is_empty() {
                for s in literals.iter().filter(|s| !s.contains(char::is_whitespace)) {
                    push_distinct(&mut deploy_strings, s, MAX_DEPLOY_EXEC_PAIRS);
                }
            }
            let sender = cmd
                .pointer("/meta/sender")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            c.exec = Some(D3Exec {
                coin,
                coin_create: create_strings,
                coin_created,
                called,
                deployed,
                strings,
                deploy_strings,
                sender,
                refs,
                ref_calls,
            });
        }
    }
    c
}

/// The rows of `c` given which modules are tracked fungible tokens (`is_token`; coin is always
/// one). Rules:
///
/// 1. **Cross-chain defpact of any module** (`XCHAIN_DEFPACTS`, any height):
///    `<m>.transfer-crosschain` step 1 credits `args[1]` and step 0 debits `args[0]`;
///    `<m>.transfer-create-to-crosschain` step 1 credits `args[0]`. Tokens emit no TRANSFER for
///    either (only `pact.X_YIELD` / `X_RESUME`); coin did not before coin v3. `<m>` is
///    discovered.
/// 2. **`exec`**: the D1 strings (`exec.data` values, `exec.code` literals) plus every string
///    of `result.data` (a receiver built at run time and returned, D3 b), for every tracked
///    token `<m>` the code calls (`<m>.fn`; from the events fork on only one that emitted
///    nothing in the tx), and for coin below the events fork.
///
///    2c. **`coin.create-account`** from the events fork on: the account it creates holds a
///    row at 0 that no event names (zero rows, gap Q), so the code literals, then the
///    `exec.data` strings count for coin ([`D3Exec::coin_create`]: at most
///    [`MAX_EXEC_PAIRS_PER_TX`], never `result.data` or a keyset predicate).
///
///    2b. **A tracked token named in code** (review I1: a dotted reference anywhere, bound
///    through `let`, in a dead branch or a list, or the module of a qualified call) that
///    emitted nothing in the tx: `meta.sender` (a bridge `transfer-remote` burn), then the
///    direct string arguments of the qualified calls of another module passing it. At most
///    [`MAX_MODREFS_PER_TX`] tracked tokens and [`MAX_MODREF_PAIRS_PER_TX`] rows per tx, each
///    drop counted once in [`TokenReceivers::modref_capped`].
/// 3. **Events** (any height, into `event_pairs`, at most [`MAX_EVENT_PAIRS_PER_EVENT`] new
///    rows earned by each event for itself and [`MAX_EVENT_PAIRS_PER_TX`] per tx, never coin;
///    amounts are never candidates): an event of a token that is not a well-formed TRANSFER (`WRAP`, `UNWRAP`, `MINT`,
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
        // This event's own allowance: it can only spend what it earns, never another's.
        let cap = (r.event_pairs.len() + MAX_EVENT_PAIRS_PER_EVENT).min(MAX_EVENT_PAIRS_PER_TX);
        for m in targets {
            for s in &ev.strings {
                r.push_event(s, m, &ev.module, cap);
            }
        }
    }
    if let Some(x) = &c.exec {
        let mut modules: Vec<&str> = Vec::new();
        if x.coin {
            modules.push("coin");
        }
        // Rule 2c: the exact created accounts, then its own strings, for coin only.
        for s in x.coin_created.iter().chain(&x.coin_create) {
            r.push(s, "coin");
        }
        for m in &x.called {
            if is_token(m) && !modules.contains(&m.as_str()) {
                modules.push(m);
            }
        }
        // Coin rows (below the events fork) as in D1; token rows are capped like events.
        let mut used = 0;
        for m in modules {
            for s in &x.strings {
                if m == "coin" {
                    r.push(s, m);
                } else {
                    r.push_exec(s, m, &x.sender, &mut used, MAX_EXEC_PAIRS_PER_TX);
                }
            }
        }
        // A deployed tracked token: its own literal budget, never shared with other calls.
        let mut used = 0;
        for m in x.deployed.iter().filter(|m| is_token(m)) {
            for s in &x.deploy_strings {
                r.push_exec(s, m, &x.sender, &mut used, MAX_DEPLOY_EXEC_PAIRS);
            }
        }
        // Rule 2b (review I1): a tracked token named anywhere in the code with no event of its
        // own in the tx may have debited `meta.sender` (a bridge `transfer-remote` burn). Only
        // tracked names count toward the reference cap; each dropped reference and row is
        // counted once. Every kept token's sender row comes before any string row. Strings are
        // attached here, for the kept tokens only (review I2).
        let tracked: Vec<&ModRef> = x.refs.iter().filter(|(m, _)| is_token(m)).collect();
        let kept = tracked.len().min(MAX_MODREFS_PER_TX);
        r.modref_capped = r
            .modref_capped
            .saturating_add(u32::try_from(tracked.len() - kept).unwrap_or(u32::MAX));
        let tracked: Vec<(&String, Vec<&String>)> = tracked[..kept]
            .iter()
            .map(|(m, calls)| {
                let mut strings: Vec<&String> = Vec::new();
                let all = calls
                    .iter()
                    .filter_map(|&k| x.ref_calls.get(k as usize))
                    .flatten();
                for s in all {
                    if strings.len() >= MAX_MODREF_STRINGS_PER_CALL {
                        break;
                    }
                    if !strings.contains(&s) {
                        strings.push(s);
                    }
                }
                (m, strings)
            })
            .collect();
        let mut used = 0;
        let rows = tracked
            .iter()
            .map(|(m, _)| (&x.sender, *m))
            .chain(tracked.iter().flat_map(|(m, strings)| {
                strings
                    .iter()
                    .filter(|s| ***s != x.sender)
                    .map(move |s| (*s, *m))
            }));
        for (s, m) in rows {
            let new = account_like(s) && !r.exec_pairs.iter().any(|(a, b)| a == s && b == m);
            if new && used >= MAX_MODREF_PAIRS_PER_TX {
                r.modref_capped = r.modref_capped.saturating_add(1);
            } else {
                r.push_exec(s, m, &x.sender, &mut used, MAX_MODREF_PAIRS_PER_TX);
            }
        }
    }
    r
}

/// Rule 2b: every module named in code (review I1), first-seen order, each with the
/// [`account_like`] string arguments of the calls that pass it directly.
///
/// A name is any word outside strings, `'symbols` and `;` comments: a dotted identifier
/// (`ns.mod`) wherever it sits (an argument, a `let` binding, a dead `if false` branch, a list),
/// and the module of a qualified function (`ns.mod.fn` gives `ns.mod`). Root `coin.*` words are
/// skipped (a call of coin, never a namespaced module). Names are not capped here: the cap
/// applies to tracked tokens only ([`d3_filter`]), so untracked names cannot use it up. The
/// list is bounded by the code length.
///
/// Strings belong only to a name passed as a direct argument of a call whose head is a
/// qualified function of another module (`(ns.mailbox.dispatch ns.kb-USDC 1 "<to>" 1.0)`).
/// Such a call is stored once in the second list, with at most
/// [`MAX_MODREF_STRINGS_PER_CALL`] strings, and each name it passes records the call's index
/// (review I2: never a copy of the strings per name).
fn modref_args(code: &str) -> (Vec<ModRef>, Vec<Vec<String>>) {
    struct Frame {
        head: Option<String>,
        items: usize,
        /// Indices into `out` of the names passed directly to this call.
        idents: Vec<usize>,
        strings: Vec<String>,
    }
    let b = code.as_bytes();
    let mut stack: Vec<Frame> = Vec::new();
    let mut out: Vec<ModRef> = Vec::new();
    let mut calls: Vec<Vec<String>> = Vec::new();
    let mut seen: HashMap<String, usize> = HashMap::new();
    let mut name = |m: &str, out: &mut Vec<ModRef>| -> Option<usize> {
        if m.starts_with("coin.") || !readable_module(m) {
            return None;
        }
        Some(*seen.entry(m.to_owned()).or_insert_with(|| {
            out.push((m.to_owned(), Vec::new()));
            out.len() - 1
        }))
    };
    let close = |f: Frame, out: &mut Vec<ModRef>, calls: &mut Vec<Vec<String>>| {
        let Some(head) = f.head.filter(|h| h.contains('.')) else {
            return;
        };
        if f.strings.is_empty() {
            return;
        }
        let head_module = head.rsplit_once('.').map_or("", |(m, _)| m);
        let mut ids = f.idents;
        ids.sort_unstable();
        ids.dedup();
        ids.retain(|&id| out[id].0 != head_module);
        if ids.is_empty() {
            return;
        }
        let Ok(k) = u32::try_from(calls.len()) else {
            return;
        };
        calls.push(f.strings);
        for id in ids {
            out[id].1.push(k);
        }
    };
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b';' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'(' | b'[' | b'{' => {
                if let Some(f) = stack.last_mut() {
                    f.items += 1;
                }
                // Only `(` opens a call; `[`/`{` hold data, never a head.
                stack.push(Frame {
                    head: if b[i] == b'(' {
                        None
                    } else {
                        Some(String::new())
                    },
                    items: 0,
                    idents: Vec::new(),
                    strings: Vec::new(),
                });
                i += 1;
            }
            b')' | b']' | b'}' => {
                if let Some(f) = stack.pop() {
                    close(f, &mut out, &mut calls);
                }
                i += 1;
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
                i += 1;
                if let Some(f) = stack.last_mut() {
                    f.items += 1;
                    let s = String::from_utf8_lossy(&s);
                    if f.strings.len() < MAX_MODREF_STRINGS_PER_CALL
                        && account_like(&s)
                        && !f.strings.iter().any(|x| *x == s)
                    {
                        f.strings.push(s.into_owned());
                    }
                }
            }
            c if is_delim(c) => i += 1,
            _ => {
                let start = i;
                while i < b.len() && !is_delim(b[i]) && !matches!(b[i], b'"' | b';') {
                    i += 1;
                }
                let w = String::from_utf8_lossy(&b[start..i]);
                if w.starts_with('\'') {
                    if let Some(f) = stack.last_mut() {
                        f.items += 1;
                    }
                    continue;
                }
                let dots = w.matches('.').count();
                // `ns.mod` itself, or the module of `ns.mod.fn`.
                let direct = if dots == 1 { name(&w, &mut out) } else { None };
                if dots >= 2 {
                    if let Some((m, _)) = w.rsplit_once('.') {
                        name(m, &mut out);
                    }
                }
                if let Some(f) = stack.last_mut() {
                    if f.items == 0 && f.head.is_none() {
                        f.head = Some(w.into_owned());
                    } else if let Some(id) = direct {
                        f.idents.push(id);
                    }
                    f.items += 1;
                }
            }
        }
    }
    (out, calls)
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
    let mut out = Vec::new();
    for w in code_words(code) {
        if let Some((m, f)) = w.rsplit_once('.') {
            if !m.is_empty() && !f.is_empty() && !out.iter().any(|x: &String| x == m) {
                out.push(m.to_owned());
            }
        }
    }
    out
}

/// The coin function that writes a row with no event naming its account.
const COIN_CREATE: &str = "coin.create-account";

/// Whether `code` calls `coin.create-account` (outside strings, `'symbols` and comments).
fn calls_coin_create(code: &str) -> bool {
    code.contains(COIN_CREATE) && code_words(code).iter().any(|w| w == COIN_CREATE)
}

/// One token of Pact code for [`coin_create_accounts`].
#[derive(Debug, PartialEq)]
enum PactTok {
    /// `(`, `[` or `{`.
    Open,
    /// `)`, `]` or `}`.
    Close,
    /// A `"string"` literal: its text when it holds no escape but `\"` and `\\` (`None`
    /// otherwise: never guessed).
    Str(Option<String>),
    /// A `'symbol`.
    Sym(String),
    /// Any other atom (a name, a number, `coin.create-account`).
    Word(String),
}

/// The tokens of `code` (`;` comments skipped).
fn pact_tokens(code: &str) -> Vec<PactTok> {
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
            b'(' | b'[' | b'{' => {
                out.push(PactTok::Open);
                i += 1;
            }
            b')' | b']' | b'}' => {
                out.push(PactTok::Close);
                i += 1;
            }
            b'"' => {
                let mut s = Vec::new();
                let mut exact = true;
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    if b[i] == b'\\' {
                        match b.get(i + 1) {
                            Some(&c @ (b'"' | b'\\')) => s.push(c),
                            _ => exact = false,
                        }
                        i += 2;
                        continue;
                    }
                    s.push(b[i]);
                    i += 1;
                }
                i += 1;
                let text = String::from_utf8(s).ok().filter(|_| exact);
                out.push(PactTok::Str(text));
            }
            b'\'' => {
                let start = i + 1;
                let mut j = start;
                while j < b.len() && !is_delim(b[j]) && b[j] != b';' {
                    j += 1;
                }
                out.push(PactTok::Sym(
                    String::from_utf8_lossy(&b[start..j]).into_owned(),
                ));
                i = j;
            }
            c if c.is_ascii_whitespace() || c == b',' => i += 1,
            _ => {
                let start = i;
                while i < b.len() && !is_delim(b[i]) && b[i] != b';' {
                    i += 1;
                }
                out.push(PactTok::Word(
                    String::from_utf8_lossy(&b[start..i]).into_owned(),
                ));
            }
        }
    }
    out
}

/// Rule 2c, exact (gap Q): the account of every `(coin.create-account <account> ..)` call in
/// `code`, read from its tokens: the first argument when it is a string literal, or
/// `(read-msg k)` / `(read-string k)` (`k` a literal or `'symbol`) naming a string of `data`.
/// Any other first argument (a variable, another call, a literal with an escape other than
/// `\"` / `\\`) gives nothing: the account is never guessed. A call inside a string or a
/// comment is no call. A call in code that may never run (a `defun` body in a deploy, an
/// untaken `if` branch) is still collected: the account named is exact, but the row may not
/// exist. A collected account only marks the row for the reconciler, which reads it from the
/// node and hides an absent answer, but each one is interned and costs a node read, so the
/// count is bounded by what the sender pays for (secpass A I-2):
/// - data-backed calls, at most [`MAX_COIN_CREATE_PER_TX`] in all: the account is a
///   `read-msg` / `read-string` of `exec.data`, or a literal whose guard is
///   `(read-keyset k)` naming an `exec.data` keyset under a key no earlier call used, whose
///   `keys` name the account: the bare key, or `k:` + a key (the bulk creators' form), the
///   key a 64-char lowercase hex ed25519 key. Any
///   other entry (`{}`, a keyset of other keys, one value copied under many keys) costs the
///   sender a few bytes, not a name's worth, and backs nothing (PR #59 review P-1);
/// - literal-only calls, at most [`MAX_COIN_CREATE_LITERALS_PER_TX`] of them.
///
/// Distinct, [`account_like`], first-seen order.
pub fn coin_create_accounts(code: &str, data: Option<&Value>) -> Vec<String> {
    let mut out = Vec::new();
    if !code.contains(COIN_CREATE) {
        return out;
    }
    let t = pact_tokens(code);
    let mut literals = 0usize;
    let mut used_keys: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for i in 0..t.len() {
        if out.len() >= MAX_COIN_CREATE_PER_TX {
            break;
        }
        if !(t[i] == PactTok::Open
            && matches!(t.get(i + 1), Some(PactTok::Word(w)) if w == COIN_CREATE))
        {
            continue;
        }
        // (account, whether an `exec.data` entry backs the call)
        let account = match t.get(i + 2) {
            Some(PactTok::Str(Some(s))) => {
                // The guard `(read-keyset k)`, `k` a data keyset naming the account that no
                // earlier call used.
                let backed = match (
                    t.get(i + 3),
                    t.get(i + 4),
                    data_key(t.get(i + 5)),
                    t.get(i + 6),
                ) {
                    (
                        Some(PactTok::Open),
                        Some(PactTok::Word(f)),
                        Some(k),
                        Some(PactTok::Close),
                    ) if f == "read-keyset"
                        && data
                            .and_then(|d| d.get(k))
                            .is_some_and(|ks| keyset_names(ks, s))
                        && !used_keys.contains(k) =>
                    {
                        Some(k)
                    }
                    _ => None,
                };
                Some((s.as_str(), backed))
            }
            Some(PactTok::Open) => match (t.get(i + 3), data_key(t.get(i + 4)), t.get(i + 5)) {
                (Some(PactTok::Word(f)), Some(k), Some(PactTok::Close))
                    if f == "read-msg" || f == "read-string" =>
                {
                    data.and_then(|d| d.get(k))
                        .and_then(Value::as_str)
                        .map(|a| (a, Some(k)))
                }
                _ => None,
            },
            _ => None,
        };
        let Some((a, backed)) = account else {
            continue;
        };
        if backed.is_none() && literals >= MAX_COIN_CREATE_LITERALS_PER_TX {
            continue;
        }
        let before = out.len();
        push_distinct(&mut out, a, MAX_COIN_CREATE_PER_TX);
        if out.len() > before {
            match backed {
                Some(k) => {
                    used_keys.insert(k);
                }
                None => literals += 1,
            }
        }
    }
    out
}

/// Whether `ks` is a keyset object whose `keys` name `account` through a 64-char lowercase
/// hex ed25519 key: the bare key is the account, or `k:` + the key is. A short key costs the
/// sender a few bytes, not a name's worth, so it backs nothing (PR #59 fix round 2).
fn keyset_names(ks: &Value, account: &str) -> bool {
    let bare = account.strip_prefix("k:").unwrap_or(account);
    is_ed25519_hex(bare)
        && ks
            .get("keys")
            .and_then(Value::as_array)
            .is_some_and(|keys| keys.iter().any(|k| k.as_str() == Some(bare)))
}

fn is_ed25519_hex(k: &str) -> bool {
    k.len() == 64 && k.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// The data key a `read-msg` argument names (a literal or a `'symbol`).
fn data_key(k: Option<&PactTok>) -> Option<&str> {
    match k {
        Some(PactTok::Str(Some(k)) | PactTok::Sym(k)) => Some(k.as_str()),
        _ => None,
    }
}

/// The words of `code` outside string literals, `'symbols` and `;` comments.
fn code_words(code: &str) -> Vec<String> {
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
    bare.split(|c: char| is_delim(c as u8) || !c.is_ascii())
        .filter(|w| !w.is_empty())
        .map(str::to_owned)
        .collect()
}

/// The modules `code` deploys (`(module <name> ..)`, qualified by the last `(namespace ..)`
/// before it: a `"literal"`, a `'symbol` or `(read-msg 'k)` / `(read-string 'k)` of `data`),
/// at most [`MAX_DEPLOYED_PER_TX`]. Strings and `;` comments are never code. A deploy runs its
/// own functions unqualified (`(init)`), so [`called_modules`] cannot see them (D3 gap group 5).
fn deployed_modules(code: &str, data: Option<&Value>) -> Vec<String> {
    enum Tok {
        Open,
        Close,
        Str(String),
        Word(String),
    }
    let b = code.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b';' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'(' => {
                toks.push(Tok::Open);
                i += 1;
            }
            b')' => {
                toks.push(Tok::Close);
                i += 1;
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
                i += 1;
                toks.push(Tok::Str(String::from_utf8_lossy(&s).into_owned()));
            }
            b'\'' => {
                let start = i + 1;
                i = start;
                while i < b.len() && !is_delim(b[i]) {
                    i += 1;
                }
                toks.push(Tok::Str(String::from_utf8_lossy(&b[start..i]).into_owned()));
            }
            c if is_delim(c) => i += 1,
            _ => {
                let start = i;
                while i < b.len() && !is_delim(b[i]) && b[i] != b';' {
                    i += 1;
                }
                toks.push(Tok::Word(
                    String::from_utf8_lossy(&b[start..i]).into_owned(),
                ));
            }
        }
    }
    let word = |j: usize| match toks.get(j) {
        Some(Tok::Word(w)) => Some(w.as_str()),
        _ => None,
    };
    let mut ns: Option<String> = None;
    let mut out: Vec<String> = Vec::new();
    for j in 0..toks.len() {
        if !matches!(toks[j], Tok::Open) {
            continue;
        }
        match word(j + 1) {
            Some("namespace") => {
                ns = match (toks.get(j + 2), toks.get(j + 3), word(j + 3)) {
                    (Some(Tok::Str(n)), _, _) => Some(n.clone()),
                    (Some(Tok::Open), _, Some("read-msg" | "read-string")) => {
                        match toks.get(j + 4) {
                            Some(Tok::Str(k)) => data
                                .and_then(|d| d.get(k))
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            _ => None,
                        }
                    }
                    _ => None,
                };
            }
            Some("module") => {
                if let Some(name) = word(j + 2) {
                    let m = match ns.as_deref() {
                        Some(n) if !n.is_empty() => format!("{n}.{name}"),
                        _ => name.to_owned(),
                    };
                    if out.len() < MAX_DEPLOYED_PER_TX && !out.contains(&m) {
                        out.push(m);
                    }
                }
            }
            _ => {}
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

/// String params at any depth, module references and Pact numbers (`{"decimal": ..}`,
/// `{"int": ..}`) skipped.
fn param_strings(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::String(s) => out.push(s.clone()),
        Value::Array(a) => a.iter().for_each(|x| param_strings(x, out)),
        Value::Object(m) if m.contains_key("refSpec") => {}
        Value::Object(m)
            if m.len() == 1 && (m.contains_key("decimal") || m.contains_key("int")) => {}
        Value::Object(m) => m.values().for_each(|x| param_strings(x, out)),
        _ => {}
    }
}

/// A plain number text (`-`? digits, `.` digits?), as an amount param prints: never an account
/// name in an event (D3 gap group 1: amounts used up the per-tx cap). A legacy account named
/// only by digits is still marked by its TRANSFER postings and the exec rule.
fn amount_text(s: &str) -> bool {
    let s = s.strip_prefix('-').unwrap_or(s);
    let (i, f) = s.split_once('.').unwrap_or((s, "0"));
    let digits = |x: &str| !x.is_empty() && x.bytes().all(|b| b.is_ascii_digit());
    digits(i) && digits(f)
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
