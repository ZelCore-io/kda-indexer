//! Read-only Pact `/local` calls for balances, guards and keysets (spec §2.1, §3.9; plan
//! Task 9, rulings B-1 and H-1).
//!
//! Safety (B-1):
//! - Only six code forms are ever sent, each a fixed text with names passed in `data`:
//!   `({m}.details (read-msg "a"))`, `(describe-keyset (read-msg "k"))`,
//!   `({m}.get-balance (read-msg "t") (read-msg "a"))`, the fungible check
//!   `(contains "fungible-v2" (at 'interfaces (describe-module (read-msg "m"))))`, and the two
//!   GQ-15 forms for a trusted NFT ledger, `({m}.details (read-msg "t") (read-msg "a"))` and
//!   `({m}.get-token-info (read-msg "t"))`. Account, keyset and token names come from chain
//!   data and may be hostile; they never enter code.
//! - `m` must match `^[A-Za-z_][A-Za-z0-9_\-]*(\.[A-Za-z_][A-Za-z0-9_\-]*)?$`, otherwise the
//!   call is not made ([`LocalError::Skipped`]).
//! - Names are capped at [`MAX_NAME_LEN`] bytes; `meta.gasLimit` is [`GAS_LIMIT`].
//! - Calls pass a token bucket (`rps`) and a concurrency cap of their own, then the node
//!   client's limits. Nothing is ever signed or sent to `/send`.
//!
//! Answers: a success gives the value, the tip height it was read at
//! (`metaData.blockHeight − 1`) and that tip's block hash (`metaData.prevBlockHash`: the
//! pending block's parent), which says which branch the state was read on (D2: after a quick
//! reorg flip-and-back the node can still answer from the orphan's state at the same height).
//! `/local` sends no hash of the pending block itself. A failure whose message says
//! the row or keyset does not exist is [`Local::Absent`] (the node sends no `metaData` then).
//! Any other Pact failure is [`LocalError::Failure`]; a delivered answer that cannot be used
//! (not JSON, over the body cap, no `result` or `metaData.blockHeight`, no decimal balance)
//! is [`LocalError::Invalid`]; only transport and HTTP errors are [`LocalError::Node`]. None
//! is ever read as a zero balance.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use blake2::{Blake2b, Digest};
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use crate::limit::RateLimiter;
use crate::types::NodeError;
use crate::NodeClient;

/// `meta.gasLimit` of every `/local` call (B-1).
pub const GAS_LIMIT: u64 = 150_000;
/// Longest account, keyset or token name sent (bytes).
pub const MAX_NAME_LEN: usize = 1024;

#[derive(Debug, Clone, Copy)]
pub struct LocalOpts {
    /// Calls started per second (`balances.local_rps`, 1..=50 in the config); 0 = no limit of
    /// its own (tests only).
    pub rps: u32,
    /// Calls in flight (`balances.local_concurrency`).
    pub concurrency: usize,
}

/// A `/local` answer.
#[derive(Debug, Clone, PartialEq)]
pub enum Local<T> {
    /// The value, and the block whose state the node read: its tip, at height
    /// `metaData.blockHeight − 1` (`blockHeight` is the pending block's height), with hash
    /// `metaData.prevBlockHash` (base64url as the node sent it; `None` when the answer has
    /// none). The hash tells which branch the height belongs to (D2).
    Found {
        value: T,
        height: u64,
        prev_hash: Option<String>,
    },
    /// The node has no such row or keyset.
    Absent,
}

/// `{module}.details`: the balance as the node printed it and the guard.
#[derive(Debug, Clone, PartialEq)]
pub struct Details {
    pub balance: String,
    /// The guard, or null when its JSON is over [`MAX_GUARD_JSON`] bytes (F-14).
    pub guard: Value,
    /// The node's guard was over [`MAX_GUARD_JSON`] bytes and is replaced by null.
    pub guard_capped: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum LocalError {
    /// B-1 refused the call (bad module name, over-long name): nothing was sent.
    #[error("skipped: {0}")]
    Skipped(String),
    /// The node evaluated the code and it failed for another reason than a missing row. A
    /// property of this call.
    #[error("pact failure: {0}")]
    Failure(String),
    /// The answer arrived but this build cannot use it (not JSON, over the body cap, no
    /// `result`, no `metaData.blockHeight`, no decimal balance). Says nothing about the row:
    /// callers keep their work queued for a few retries and report it (D3 gap group 2: valid
    /// long decimals were read as failures and their marks dropped at once).
    #[error("invalid answer: {0}")]
    Invalid(String),
    /// Transport or HTTP error (timeout, connection, non-2xx status): retry later.
    #[error("node: {0}")]
    Node(#[from] NodeError),
}

pub type LocalResult<T> = std::result::Result<Local<T>, LocalError>;

/// The B-1 module name check: `name` or `namespace.name`, identifier characters only.
pub fn valid_module(m: &str) -> bool {
    let mut parts = m.splitn(2, '.');
    let first = parts.next().unwrap_or("");
    valid_ident(first) && parts.next().is_none_or(valid_ident)
}

/// One Pact identifier (a namespace, module or table name): identifier characters only, so
/// it can be written into code as text.
pub fn valid_ident(s: &str) -> bool {
    let mut b = s.bytes();
    matches!(b.next(), Some(c) if c.is_ascii_alphabetic() || c == b'_')
        && b.all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
}

/// Most keys one `keys` listing may answer ([`LocalClient::table_keys`]); a bigger table is
/// refused whole (about 70 bytes a key: 250,000 keys is about 17 MB of answer). The largest
/// token table measured on mainnet01 (`kaddex.kdx` chain 2, 2026-11-23) has 9,480 keys.
pub const MAX_LISTED_KEYS: usize = 250_000;

/// The failure message means the module is not deployed on this chain (Pact 5, measured on
/// mainnet01: `Cannot find module: free.ba-t1`).
pub fn is_no_module_message(msg: &str) -> bool {
    msg.starts_with("Cannot find module: ")
}

/// The failure message means the module is on this chain but its table was never created
/// there (Pact 5, measured on mainnet01: `(keys free.KAYC.token-table)` on chain 4 answers
/// `Error during database operation: Table access failed because table
/// free.KAYC_token-table was not found.`): the table has no keys.
pub fn is_no_table_message(msg: &str) -> bool {
    msg.starts_with("Error during database operation: Table access failed because table ")
        && msg.ends_with(" was not found.")
}

/// The failure message means "no such row" (Pact 5 `No value found in table …`, Pact 4
/// `with-read: row not found …`) or "no such keyset".
pub fn is_absent_message(msg: &str) -> bool {
    msg.starts_with("No value found in table ")
        || msg.starts_with("with-read: row not found")
        || msg.starts_with("read: row not found")
        || msg.starts_with("Cannot find keyset")
        || msg.starts_with("No such keyset")
        || msg.starts_with("Keyset not found")
}

/// The unsigned `/local` command for `code` with `data` (B-1), as the request body
/// `{hash, sigs, cmd}`: `hash` is blake2b-256 of the `cmd` text, base64url without padding.
pub fn command(network: &str, chain: u8, code: &str, data: &Value, now_secs: u64) -> Value {
    let s = |x: &str| Value::String(x.to_owned()).to_string();
    let cmd = format!(
        "{{\"networkId\":{net},\"payload\":{{\"exec\":{{\"code\":{code},\"data\":{data}}}}},\
         \"signers\":[],\"meta\":{{\"chainId\":{chain},\"sender\":\"\",\"gasLimit\":{GAS_LIMIT},\
         \"gasPrice\":0.00000001,\"ttl\":600,\"creationTime\":{ct}}},\"nonce\":{nonce}}}",
        net = s(network),
        code = s(code),
        data = data,
        chain = s(&chain.to_string()),
        ct = now_secs.saturating_sub(30),
        nonce = s(&format!("kdx-{now_secs}")),
    );
    let digest = Blake2b::<blake2::digest::consts::U32>::digest(cmd.as_bytes());
    let hash = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest);
    json!({"hash": hash, "sigs": [], "cmd": cmd})
}

/// The failure message means `module` has no member `member` (Pact 5, measured on mainnet01:
/// `Module marmalade.ledger has no such member: get-token-info`). Exact match: the ledger is a
/// trusted name and the code is fixed, so no hostile text can produce it.
pub fn is_no_member_message(msg: &str, module: &str, member: &str) -> bool {
    msg == format!("Module {module} has no such member: {member}")
        || msg == format!("Cannot resolve {module}.{member}")
}

/// Longest guard JSON and token uri accepted from an NFT ledger answer (bytes, GQ-15).
pub const MAX_NFT_TEXT: usize = 4096;
/// Largest serialized guard or keyset stored from a `details` / `describe-keyset` answer
/// (F-14, the limit NFT guards use); a larger one is stored as null.
pub const MAX_GUARD_JSON: usize = MAX_NFT_TEXT;

/// `{ledger}.get-token-info` of one token (GQ-15): `precision`, `supply` (decimal text) and `uri`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenInfo {
    pub precision: i32,
    pub supply: String,
    pub uri: String,
}

/// Longest decimal text accepted from a node answer (bytes). Pact decimals have at most 255
/// decimal places (`Data.Decimal`, `Word8` places) and an unbounded integer mantissa; real
/// balances print at most about 260 bytes (`free.wiza`). 1,024 bytes covers every place plus
/// a 760-digit integer part, and still bounds garbage from a hostile node (M-7).
pub const MAX_DECIMAL_LEN: usize = 1024;

/// A decimal text (M-7): optional `-`, digits, optional `.` and digits, optional exponent; at
/// most [`MAX_DECIMAL_LEN`] bytes.
pub fn is_decimal(s: &str) -> bool {
    if s.is_empty() || s.len() > MAX_DECIMAL_LEN {
        return false;
    }
    let digits = |x: &str| !x.is_empty() && x.bytes().all(|b| b.is_ascii_digit());
    let s = s.strip_prefix('-').unwrap_or(s);
    let (mant, exp) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], Some(&s[i + 1..])),
        None => (s, None),
    };
    let (int, frac) = match mant.split_once('.') {
        Some((a, b)) => (a, Some(b)),
        None => (mant, None),
    };
    digits(int)
        && frac.is_none_or(digits)
        && exp.is_none_or(|e| digits(e.strip_prefix(['+', '-']).unwrap_or(e)))
}

/// A decimal as the node printed it: a JSON number's text, `{"decimal": …}` or `{"int": …}`.
/// A string must be a decimal text ([`is_decimal`]); anything else is no decimal.
pub fn decimal_text(v: &Value) -> Option<String> {
    match v {
        // F-13: a bare number's text gets the same check as a string's.
        Value::Number(n) => Some(n.to_string()).filter(|t| is_decimal(t)),
        Value::String(s) => is_decimal(s).then(|| s.clone()),
        Value::Object(o) => o
            .get("decimal")
            .or_else(|| o.get("int"))
            .and_then(decimal_text),
        _ => None,
    }
}

/// `/local` client with its own rate and concurrency limits over a [`NodeClient`].
pub struct LocalClient {
    node: NodeClient,
    limiter: RateLimiter,
    sem: Semaphore,
}

/// A delivered answer that cannot be used: a per-call [`LocalError::Invalid`], never a node
/// error (one odd answer must not make the caller back off for every key).
fn invalid(what: &str) -> LocalError {
    LocalError::Invalid(what.chars().take(200).collect())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn check_name(what: &str, s: &str) -> Result<(), LocalError> {
    if s.is_empty() || s.len() > MAX_NAME_LEN {
        return Err(LocalError::Skipped(format!(
            "{what} name must be 1..={MAX_NAME_LEN} bytes"
        )));
    }
    Ok(())
}

fn check_module(m: &str) -> Result<(), LocalError> {
    if !valid_module(m) {
        return Err(LocalError::Skipped(
            "module name fails the B-1 check".into(),
        ));
    }
    Ok(())
}

impl LocalClient {
    pub fn new(node: NodeClient, o: LocalOpts) -> LocalClient {
        LocalClient {
            node,
            limiter: RateLimiter::new(o.rps),
            sem: Semaphore::new(o.concurrency.max(1)),
        }
    }

    /// One call: limits, send, classify. `Found` carries the `result.data` value.
    async fn call(&self, chain: u8, code: &str, data: Value) -> LocalResult<Value> {
        self.call_timed(chain, code, data).await.0
    }

    /// [`Self::call`] sent once, with its own `timeout` (the key listing, PR #27 H-1: no
    /// retry inside the client; a timeout is [`NodeError::Timeout`]).
    async fn call_once(
        &self,
        chain: u8,
        code: &str,
        data: Value,
        timeout: Duration,
    ) -> LocalResult<Value> {
        let _p = self
            .sem
            .acquire()
            .await
            .map_err(|_| NodeError::Transport("local semaphore closed".into()))?;
        self.limiter.acquire().await;
        self.exchange(chain, code, data, Some(timeout)).await
    }

    /// [`Self::call`] plus the seconds from the send (after the rate and concurrency waits)
    /// to the answer.
    async fn call_timed(&self, chain: u8, code: &str, data: Value) -> (LocalResult<Value>, f64) {
        let _p = match self.sem.acquire().await {
            Ok(p) => p,
            Err(_) => {
                return (
                    Err(NodeError::Transport("local semaphore closed".into()).into()),
                    0.0,
                )
            }
        };
        self.limiter.acquire().await;
        let t0 = std::time::Instant::now();
        let r = self.exchange(chain, code, data, None).await;
        (r, t0.elapsed().as_secs_f64())
    }

    /// One exchange; `once`: a single attempt with that timeout.
    async fn exchange(
        &self,
        chain: u8,
        code: &str,
        data: Value,
        once: Option<Duration>,
    ) -> LocalResult<Value> {
        let body = command(self.node.network(), chain, code, &data, now_secs());
        let sent = match once {
            Some(t) => self.node.local_once(chain, &body, t).await,
            None => self.node.local(chain, &body).await,
        };
        let r = match sent {
            Ok(r) => r,
            // An answer arrived but is not usable (not JSON, over the body cap): a property of
            // this call, not of the node's health.
            Err(e @ (NodeError::Decode(_) | NodeError::TooLarge { .. })) => {
                return Err(invalid(&e.to_string()))
            }
            Err(e) => return Err(e.into()),
        };
        classify(&r)
    }
}

/// Classify one `/local` answer body.
fn classify(r: &Value) -> LocalResult<Value> {
    let result = r.get("result").ok_or_else(|| invalid("no result"))?;
    match result.get("status").and_then(Value::as_str) {
        Some("success") => {
            // `metaData.blockHeight` is the height of the block the node would build
            // next (its tip + 1, measured on mainnet01); the state read is the tip's,
            // and `prevBlockHash` is the tip's hash.
            let height = r
                .pointer("/metaData/blockHeight")
                .and_then(Value::as_u64)
                .and_then(|h| h.checked_sub(1))
                .ok_or_else(|| invalid("no metaData.blockHeight"))?;
            let prev_hash = r
                .pointer("/metaData/prevBlockHash")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let value = result.get("data").cloned().unwrap_or(Value::Null);
            Ok(Local::Found {
                value,
                height,
                prev_hash,
            })
        }
        Some("failure") => {
            let msg = result
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("");
            if is_absent_message(msg) {
                Ok(Local::Absent)
            } else {
                let cut: String = msg.chars().take(200).collect();
                Err(LocalError::Failure(cut))
            }
        }
        _ => Err(invalid("result has no status")),
    }
}

impl LocalClient {
    /// `({module}.details (read-msg "a"))` with `data {"a": account}`.
    pub async fn details(&self, chain: u8, module: &str, account: &str) -> LocalResult<Details> {
        self.details_timed(chain, module, account).await.0
    }

    /// [`Self::details`] plus the call's seconds (0 when B-1 refused it).
    pub async fn details_timed(
        &self,
        chain: u8,
        module: &str,
        account: &str,
    ) -> (LocalResult<Details>, f64) {
        if let Err(e) = check_module(module).and_then(|_| check_name("account", account)) {
            return (Err(e), 0.0);
        }
        let code = format!("({module}.details (read-msg \"a\"))");
        let (r, secs) = self.call_timed(chain, &code, json!({ "a": account })).await;
        (r.and_then(details_of), secs)
    }
}

fn details_of(r: Local<Value>) -> LocalResult<Details> {
    {
        match r {
            Local::Absent => Ok(Local::Absent),
            Local::Found {
                value,
                height,
                prev_hash,
            } => {
                let balance = value
                    .get("balance")
                    .and_then(decimal_text)
                    .ok_or_else(|| invalid("details: no decimal balance"))?;
                let (guard, guard_capped) =
                    capped_guard(value.get("guard").cloned().unwrap_or(Value::Null));
                Ok(Local::Found {
                    value: Details {
                        balance,
                        guard,
                        guard_capped,
                    },
                    height,
                    prev_hash,
                })
            }
        }
    }
}

impl LocalClient {
    /// Whether `module` lists `fungible-v2` among its interfaces on `chain` (I-6):
    /// `(contains "fungible-v2" (at 'interfaces (describe-module (read-msg "m"))))` with
    /// `data {"m": module}`. A non-boolean answer is a failure.
    pub async fn implements_fungible(&self, chain: u8, module: &str) -> LocalResult<bool> {
        check_module(module)?;
        match self
            .call(
                chain,
                "(contains \"fungible-v2\" (at 'interfaces (describe-module (read-msg \"m\"))))",
                json!({ "m": module }),
            )
            .await?
        {
            Local::Found {
                value,
                height,
                prev_hash,
            } => match value.as_bool() {
                Some(b) => Ok(Local::Found {
                    value: b,
                    height,
                    prev_hash,
                }),
                None => Err(invalid("describe-module: not a boolean")),
            },
            Local::Absent => Ok(Local::Absent),
        }
    }

    /// `(at 'code (describe-module (read-msg "m")))` with `data {"m": module}`: the module's
    /// code on `chain`, absent when the module is not deployed there. Sent once, with
    /// `timeout` (no retry inside the client: the caller paces its own retries).
    pub async fn module_code(
        &self,
        chain: u8,
        module: &str,
        timeout: Duration,
    ) -> LocalResult<String> {
        check_module(module)?;
        code_of(
            self.call_once(
                chain,
                "(at 'code (describe-module (read-msg \"m\")))",
                json!({ "m": module }),
                timeout,
            )
            .await,
        )
    }

    /// `(keys {module}.{table})`: every row key of a module's table on `chain`, at most
    /// `max` (D3 gap groups 3 and 4: rows no tx names). The node allows it in `/local` for
    /// any module's table and charges a flat 40,000 gas whatever the table's size (measured
    /// on mainnet01, 2026-11-23: 4 keys and 9,480 keys alike), but the time grows with the
    /// table (about 2,000 keys/s measured), so the read is sent once, with `timeout`: a table
    /// the node cannot list in time is a [`NodeError::Timeout`], never retried here.
    pub async fn table_keys(
        &self,
        chain: u8,
        module: &str,
        table: &str,
        max: usize,
        timeout: Duration,
    ) -> LocalResult<Vec<String>> {
        check_module(module)?;
        if !valid_ident(table) {
            return Err(LocalError::Skipped("table name fails the B-1 check".into()));
        }
        match self
            .call_once(
                chain,
                &format!("(keys {module}.{table})"),
                json!({}),
                timeout,
            )
            .await
        {
            Ok(r) => keys_of(r, max),
            Err(LocalError::Failure(m)) if is_no_table_message(&m) => Ok(Local::Absent),
            Err(e) => Err(e),
        }
    }

    /// `(describe-keyset (read-msg "k"))` with `data {"k": name}`: the keyset JSON, or null
    /// when it is over [`MAX_GUARD_JSON`] bytes (F-14).
    pub async fn describe_keyset(&self, chain: u8, name: &str) -> LocalResult<Value> {
        check_name("keyset", name)?;
        let r = self
            .call(
                chain,
                "(describe-keyset (read-msg \"k\"))",
                json!({ "k": name }),
            )
            .await?;
        keyset_of(r)
    }

    /// `({module}.get-balance (read-msg "t") (read-msg "a"))` with `data {"t": token, "a":
    /// account}` (marmalade, plan Task 11): the balance as the node printed it.
    pub async fn get_balance(
        &self,
        chain: u8,
        module: &str,
        token_id: &str,
        account: &str,
    ) -> LocalResult<String> {
        check_module(module)?;
        check_name("account", account)?;
        check_name("token", token_id)?;
        let code = format!("({module}.get-balance (read-msg \"t\") (read-msg \"a\"))");
        match self
            .call(chain, &code, json!({ "t": token_id, "a": account }))
            .await?
        {
            Local::Absent => Ok(Local::Absent),
            Local::Found {
                value,
                height,
                prev_hash,
            } => Ok(Local::Found {
                value: decimal_text(&value).ok_or_else(|| invalid("get-balance: not a decimal"))?,
                height,
                prev_hash,
            }),
        }
    }
}

impl LocalClient {
    /// `({ledger}.details (read-msg "t") (read-msg "a"))` with `data {"t": token, "a": account}`
    /// (GQ-15): the holding's guard, compact JSON. The answer must name the same token and
    /// account when it names them, and the guard must be an object of at most [`MAX_NFT_TEXT`]
    /// bytes; anything else is unusable.
    pub async fn nft_guard(
        &self,
        chain: u8,
        ledger: &str,
        token_id: &str,
        account: &str,
    ) -> LocalResult<String> {
        check_module(ledger)?;
        check_name("account", account)?;
        check_name("token", token_id)?;
        let code = format!("({ledger}.details (read-msg \"t\") (read-msg \"a\"))");
        let r = self
            .call(chain, &code, json!({ "t": token_id, "a": account }))
            .await?;
        nft_guard_of(r, token_id, account)
    }

    /// `({ledger}.get-token-info (read-msg "t"))` with `data {"t": token}` (GQ-15). A ledger
    /// without the function answers a [`LocalError::Failure`] that [`is_no_member_message`]
    /// recognizes.
    pub async fn nft_token_info(
        &self,
        chain: u8,
        ledger: &str,
        token_id: &str,
    ) -> LocalResult<TokenInfo> {
        check_module(ledger)?;
        check_name("token", token_id)?;
        let code = format!("({ledger}.get-token-info (read-msg \"t\"))");
        let r = self.call(chain, &code, json!({ "t": token_id })).await?;
        token_info_of(r, token_id)
    }
}

/// F-14: a guard whose JSON is over [`MAX_GUARD_JSON`] bytes becomes null; true when it did.
/// A `describe-module` code answer: the code text; a module not on the chain is absent.
fn code_of(r: LocalResult<Value>) -> LocalResult<String> {
    match r {
        Ok(Local::Found {
            value: Value::String(code),
            height,
            prev_hash,
        }) => Ok(Local::Found {
            value: code,
            height,
            prev_hash,
        }),
        Ok(Local::Found { .. }) => Err(invalid("describe-module: code is not a string")),
        Ok(Local::Absent) => Ok(Local::Absent),
        Err(LocalError::Failure(m)) if is_no_module_message(&m) => Ok(Local::Absent),
        Err(e) => Err(e),
    }
}

/// A `keys` answer: every key a string, at most `max` of them (else the whole answer is
/// unusable: a partial listing would look complete).
fn keys_of(r: Local<Value>, max: usize) -> LocalResult<Vec<String>> {
    let (value, height, prev_hash) = match r {
        Local::Absent => return Ok(Local::Absent),
        Local::Found {
            value,
            height,
            prev_hash,
        } => (value, height, prev_hash),
    };
    let Value::Array(items) = value else {
        return Err(invalid("keys: not a list"));
    };
    if items.len() > max {
        return Err(invalid(&format!("keys: {} keys, over {max}", items.len())));
    }
    let keys = items
        .into_iter()
        .map(|v| match v {
            Value::String(s) => Some(s),
            _ => None,
        })
        .collect::<Option<Vec<String>>>()
        .ok_or_else(|| invalid("keys: a key is not a string"))?;
    Ok(Local::Found {
        value: keys,
        height,
        prev_hash,
    })
}

fn capped_guard(g: Value) -> (Value, bool) {
    if g.to_string().len() > MAX_GUARD_JSON {
        (Value::Null, true)
    } else {
        (g, false)
    }
}

/// A `describe-keyset` answer, null when over [`MAX_GUARD_JSON`] bytes (F-14).
fn keyset_of(r: Local<Value>) -> LocalResult<Value> {
    Ok(match r {
        Local::Found {
            value,
            height,
            prev_hash,
        } => Local::Found {
            value: capped_guard(value).0,
            height,
            prev_hash,
        },
        Local::Absent => Local::Absent,
    })
}

fn echoes(v: &Value, field: &str, want: &str) -> bool {
    v.get(field).is_none_or(|x| x.as_str() == Some(want))
}

fn nft_guard_of(r: Local<Value>, token_id: &str, account: &str) -> LocalResult<String> {
    let (value, height, prev_hash) = match r {
        Local::Absent => return Ok(Local::Absent),
        Local::Found {
            value,
            height,
            prev_hash,
        } => (value, height, prev_hash),
    };
    if !echoes(&value, "id", token_id) || !echoes(&value, "account", account) {
        return Err(invalid("nft details: another token or account"));
    }
    let guard = match value.get("guard") {
        Some(g @ Value::Object(_)) => g.to_string(),
        _ => return Err(invalid("nft details: no guard object")),
    };
    if guard.len() > MAX_NFT_TEXT {
        return Err(invalid("nft details: guard over the size cap"));
    }
    Ok(Local::Found {
        value: guard,
        height,
        prev_hash,
    })
}

fn token_info_of(r: Local<Value>, token_id: &str) -> LocalResult<TokenInfo> {
    let (value, height, prev_hash) = match r {
        Local::Absent => return Ok(Local::Absent),
        Local::Found {
            value,
            height,
            prev_hash,
        } => (value, height, prev_hash),
    };
    if !echoes(&value, "id", token_id) {
        return Err(invalid("token info: another token"));
    }
    let precision = value
        .get("precision")
        .and_then(|p| p.as_i64().or_else(|| p.get("int").and_then(Value::as_i64)))
        .and_then(|p| i32::try_from(p).ok())
        .filter(|p| *p >= 0)
        .ok_or_else(|| invalid("token info: no integer precision"))?;
    let supply = value
        .get("supply")
        .and_then(decimal_text)
        .ok_or_else(|| invalid("token info: no decimal supply"))?;
    let uri = value
        .get("uri")
        .and_then(Value::as_str)
        .filter(|u| u.len() <= MAX_NFT_TEXT)
        .ok_or_else(|| invalid("token info: no uri text within the size cap"))?
        .to_owned();
    Ok(Local::Found {
        value: TokenInfo {
            precision,
            supply,
            uri,
        },
        height,
        prev_hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// GQ-15: the two NFT forms keep the token and account in `data`; answers are checked.
    #[test]
    fn nft_forms_and_answers() {
        let (t, a) = ("t:x\") (coin.transfer", "k:y\") (z");
        let code = "(marmalade-v2.ledger.details (read-msg \"t\") (read-msg \"a\"))";
        let b = command("mainnet01", 8, code, &json!({ "t": t, "a": a }), 1_000);
        let cmd: Value = serde_json::from_str(b["cmd"].as_str().unwrap()).unwrap();
        assert_eq!(
            cmd.pointer("/payload/exec/code").and_then(Value::as_str),
            Some(code)
        );
        assert_eq!(cmd.pointer("/payload/exec/data/t"), Some(&json!(t)));
        assert_eq!(cmd.pointer("/payload/exec/data/a"), Some(&json!(a)));
        let found = |v: Value| Local::Found {
            value: v,
            height: 9,
            prev_hash: Some("P".into()),
        };
        let g = json!({"keys": ["ab"], "pred": "keys-all"});
        assert_eq!(
            nft_guard_of(
                found(json!({"id": "t:1", "account": "k:a", "balance": 1, "guard": g})),
                "t:1",
                "k:a"
            )
            .unwrap(),
            Local::Found {
                value: r#"{"keys":["ab"],"pred":"keys-all"}"#.into(),
                height: 9,
                prev_hash: Some("P".into()),
            }
        );
        for bad in [
            json!({"id": "t:2", "account": "k:a", "guard": g}),
            json!({"id": "t:1", "account": "k:b", "guard": g}),
            json!({"id": "t:1", "account": "k:a", "guard": null}),
            json!({"id": "t:1", "account": "k:a", "guard": "x"}),
            json!({"guard": {"k": "x".repeat(MAX_NFT_TEXT)}}),
        ] {
            assert!(
                nft_guard_of(found(bad.clone()), "t:1", "k:a").is_err(),
                "{bad}"
            );
        }
        assert_eq!(
            nft_guard_of(Local::Absent, "t:1", "k:a").unwrap(),
            Local::Absent
        );
        let info = token_info_of(
            found(json!({"id": "t:1", "supply": 1.0, "precision": 0, "uri": "ipfs://x", "policies": []})),
            "t:1",
        )
        .unwrap();
        assert_eq!(
            info,
            Local::Found {
                value: TokenInfo {
                    precision: 0,
                    supply: "1.0".into(),
                    uri: "ipfs://x".into()
                },
                height: 9,
                prev_hash: Some("P".into()),
            }
        );
        assert!(token_info_of(
            found(json!({"supply": {"decimal": "2.5"}, "precision": {"int": 1}, "uri": ""})),
            "t:1"
        )
        .is_ok());
        for bad in [
            json!({"id": "t:2", "supply": 1, "precision": 0, "uri": ""}),
            json!({"supply": "lots", "precision": 0, "uri": ""}),
            json!({"supply": 1, "precision": -1, "uri": ""}),
            json!({"supply": 1, "precision": 1.5, "uri": ""}),
            json!({"supply": 1, "precision": 0}),
            json!({"supply": 1, "precision": 0, "uri": "x".repeat(MAX_NFT_TEXT + 1)}),
        ] {
            assert!(token_info_of(found(bad.clone()), "t:1").is_err(), "{bad}");
        }
        assert!(is_no_member_message(
            "Module marmalade.ledger has no such member: get-token-info",
            "marmalade.ledger",
            "get-token-info"
        ));
        assert!(!is_no_member_message(
            "Module marmalade.ledger has no such member: get-token-info x",
            "marmalade.ledger",
            "get-token-info"
        ));
    }

    #[test]
    fn module_check() {
        for ok in ["coin", "free.my-token", "n_abc.x_y", "_a.b-1"] {
            assert!(valid_module(ok), "{ok}");
        }
        for bad in [
            "",
            "1coin",
            "coin.",
            "a.b.c",
            "coin (x)",
            "coin\") (coin.transfer",
            ".coin",
            "co in",
        ] {
            assert!(!valid_module(bad), "{bad}");
        }
    }

    /// B-1: a hostile account is data; the code is the fixed form.
    #[test]
    fn hostile_account_stays_in_data() {
        let acct = "x\") (coin.transfer";
        let b = command(
            "mainnet01",
            3,
            "(coin.details (read-msg \"a\"))",
            &json!({ "a": acct }),
            1_000,
        );
        let cmd: Value = serde_json::from_str(b["cmd"].as_str().unwrap()).unwrap();
        assert_eq!(
            cmd.pointer("/payload/exec/code").and_then(Value::as_str),
            Some("(coin.details (read-msg \"a\"))")
        );
        assert_eq!(
            cmd.pointer("/payload/exec/data/a").and_then(Value::as_str),
            Some(acct)
        );
        assert_eq!(cmd.pointer("/meta/chainId"), Some(&json!("3")));
        assert_eq!(cmd.pointer("/meta/gasLimit"), Some(&json!(150000)));
        assert_eq!(cmd.pointer("/meta/creationTime"), Some(&json!(970)));
        let h = Blake2b::<blake2::digest::consts::U32>::digest(b["cmd"].as_str().unwrap());
        assert_eq!(
            b["hash"].as_str().unwrap(),
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(h)
        );
    }

    /// M-7: a hostile `details` answer cannot store arbitrary text as a balance.
    #[test]
    fn balance_text_must_be_a_decimal() {
        for ok in ["0", "12.5", "-3.0", "1e-8", "1.5E+3", "000.000"] {
            assert_eq!(decimal_text(&json!(ok)).as_deref(), Some(ok), "{ok}");
            assert!(decimal_text(&json!({ "decimal": ok })).is_some());
        }
        let long = "9".repeat(MAX_DECIMAL_LEN + 1);
        for bad in [
            "", "-", ".5", "1.", "1e", "abc", "1.0 ", "<script>", "1..2", &long,
        ] {
            assert_eq!(decimal_text(&json!(bad)), None, "{bad}");
        }
        assert!(details_of(Local::Found {
            value: json!({"balance": "lots", "guard": null}),
            height: 1,
            prev_hash: None,
        })
        .is_err());
    }

    /// D3 gap group 2: `free.wiza` balances carry Pact's full 255 decimal places (about 260
    /// bytes, e.g. `0.0161938904166666…`). They are valid decimals and must read; only absurd
    /// lengths are refused.
    #[test]
    fn a_full_precision_pact_decimal_reads() {
        let wiza = format!("0.0161938904166666{}8", "6".repeat(255 - 17));
        assert_eq!(wiza.len(), 257);
        assert_eq!(
            decimal_text(&json!({ "decimal": wiza })).as_deref(),
            Some(wiza.as_str())
        );
        let big = format!("242.{}", "3".repeat(255));
        assert_eq!(decimal_text(&json!(big)).as_deref(), Some(big.as_str()));
        let r = details_of(Local::Found {
            value: json!({"account": "k:a", "balance": {"decimal": wiza}, "guard": null}),
            height: 1,
            prev_hash: None,
        });
        assert!(matches!(&r, Ok(Local::Found { value, .. }) if value.balance == wiza));
        // An unusable answer is `Invalid`, not a pact `Failure`: the reconciler retries the key.
        let r = details_of(Local::Found {
            value: json!({"balance": "lots", "guard": null}),
            height: 1,
            prev_hash: None,
        });
        assert!(matches!(r, Err(LocalError::Invalid(_))), "{r:?}");
    }

    /// F-13: a bare JSON number is checked like a decimal string (a hostile node could send
    /// one of any length).
    #[test]
    fn a_long_bare_number_is_no_decimal() {
        let long = "9".repeat(MAX_DECIMAL_LEN + 1);
        let v: Value = serde_json::from_str(&long).unwrap();
        assert_eq!(decimal_text(&v), None);
        assert_eq!(decimal_text(&json!({ "decimal": v })), None);
        assert_eq!(decimal_text(&json!(12)), Some("12".into()));
    }

    /// F-14: a guard over [`MAX_GUARD_JSON`] bytes is stored as null and flagged; the balance
    /// still reads. One at the cap is kept.
    #[test]
    fn an_over_size_guard_reads_as_null() {
        let at = |n: usize| {
            // `{"keys":["…"],"pred":"keys-all"}` serializes to n bytes.
            let pad = n - r#"{"keys":[""],"pred":"keys-all"}"#.len();
            json!({"keys": ["a".repeat(pad)], "pred": "keys-all"})
        };
        assert_eq!(at(MAX_GUARD_JSON).to_string().len(), MAX_GUARD_JSON);
        let read = |g: Value| match details_of(Local::Found {
            value: json!({"balance": 1.5, "guard": g}),
            height: 1,
            prev_hash: None,
        }) {
            Ok(Local::Found { value, .. }) => value,
            other => panic!("{other:?}"),
        };
        let d = read(at(MAX_GUARD_JSON + 1));
        assert_eq!(
            (d.balance.as_str(), &d.guard, d.guard_capped),
            ("1.5", &Value::Null, true)
        );
        let d = read(at(MAX_GUARD_JSON));
        assert!(!d.guard_capped && d.guard == at(MAX_GUARD_JSON));
        // A `describe-keyset` answer over the cap reads as null too.
        let ks = |g: Value| {
            keyset_of(Local::Found {
                value: g,
                height: 1,
                prev_hash: None,
            })
            .unwrap()
        };
        assert!(matches!(
            ks(at(MAX_GUARD_JSON + 1)),
            Local::Found {
                value: Value::Null,
                ..
            }
        ));
        assert!(matches!(
            ks(at(MAX_GUARD_JSON)),
            Local::Found {
                value: Value::Object(_),
                ..
            }
        ));
    }

    /// D2: a success carries the tip's height and hash (`prevBlockHash`); an answer without
    /// the hash still reads (`None`), and an absent answer carries neither.
    #[test]
    fn a_success_names_the_block_it_read() {
        let ok =
            |meta: Value| json!({"result": {"status": "success", "data": 1}, "metaData": meta});
        assert_eq!(
            classify(&ok(json!({"blockHeight": 11, "prevBlockHash": "H10"}))).unwrap(),
            Local::Found {
                value: json!(1),
                height: 10,
                prev_hash: Some("H10".into()),
            }
        );
        assert!(matches!(
            classify(&ok(json!({"blockHeight": 11}))).unwrap(),
            Local::Found {
                height: 10,
                prev_hash: None,
                ..
            }
        ));
        assert!(classify(&ok(json!({"prevBlockHash": "H10"}))).is_err());
        let absent = json!({"result": {"status": "failure",
            "error": {"message": "No value found in table coin_coin-table for key: x"}}});
        assert_eq!(classify(&absent).unwrap(), Local::Absent);
    }

    /// D3 gap groups 3 and 4 (KDA-03 node, 2026-11-23): `describe-module` answers the code;
    /// a module not deployed on the chain is absent, not a failure.
    #[test]
    fn module_code_answers() {
        let meta = json!({"blockHeight": 11, "prevBlockHash": "H10"});
        let ok = json!({"result": {"status": "success",
            "data": "(module KAYC GOVERNANCE (deftable token-table:{token-schema}))"},
            "metaData": meta});
        let Local::Found { value, height, .. } = code_of(classify(&ok)).unwrap() else {
            panic!("found")
        };
        assert!(value.starts_with("(module KAYC"));
        assert_eq!(height, 10);
        let gone = json!({"result": {"status": "failure", "error": {"callStack": [],
            "type": "EvalError", "message": "Cannot find module: free.ba-t1",
            "info": "<toplevel>:0"}}});
        assert_eq!(code_of(classify(&gone)).unwrap(), Local::Absent);
        let num =
            json!({"result": {"status": "success", "data": 1}, "metaData": {"blockHeight": 2}});
        assert!(matches!(
            code_of(classify(&num)),
            Err(LocalError::Invalid(_))
        ));
    }

    /// `(keys free.shatter.token-table)` on chain 0 answered these four keys (gas 40003); a
    /// non-string key or a listing over the cap is unusable, never cut short.
    #[test]
    fn table_keys_answers() {
        let meta = json!({"blockHeight": 11, "prevBlockHash": "H10"});
        let four = [
            "GeorgePublish",
            "ROOT",
            "k:0d39e69d5a71e4abf926c84cc12fcef7eb7e993ce062a77dc692c9715b3b7ad4",
            "k:acca57aef1ef1f99e9948fa811c3c8c187af18f9424071a6cec956ea5d3737c6",
        ];
        let ok = json!({"result": {"status": "success", "data": four}, "metaData": meta});
        let Local::Found { value, .. } = keys_of(classify(&ok).unwrap(), 4).unwrap() else {
            panic!("found")
        };
        assert_eq!(value, four);
        assert!(matches!(
            keys_of(classify(&ok).unwrap(), 3),
            Err(LocalError::Invalid(m)) if m.contains("over 3")
        ));
        let mixed = json!({"result": {"status": "success", "data": ["a", 1]}, "metaData": meta});
        assert!(matches!(
            keys_of(classify(&mixed).unwrap(), 4),
            Err(LocalError::Invalid(_))
        ));
    }

    /// `free.KAYC` is deployed on chain 4 but its table was never created there.
    #[test]
    fn a_table_never_created_has_no_keys() {
        assert!(is_no_table_message(
            "Error during database operation: Table access failed because table \
             free.KAYC_token-table was not found."
        ));
        assert!(!is_no_table_message(
            "Error during database operation: Table access failed because table x was locked"
        ));
        assert!(!is_no_table_message("Module admin necessary for operation"));
    }

    /// The table name goes into the code as text: identifier characters only.
    #[test]
    fn table_names_are_checked() {
        assert!(valid_ident("token-table"));
        assert!(valid_ident("coin_table2"));
        assert!(!valid_ident("t) (coin.transfer"));
        assert!(!valid_ident("ns.t"));
        assert!(!valid_ident(""));
        assert!(!valid_ident("1t"));
    }

    #[test]
    fn absent_and_decimals() {
        assert!(is_absent_message(
            "No value found in table coin_coin-table for key: x\") (coin...."
        ));
        assert!(is_absent_message(
            "Cannot find keyset in the database: free.x"
        ));
        assert!(!is_absent_message("Cannot resolve free.x.details"));
        assert_eq!(
            decimal_text(&serde_json::from_str::<Value>("12.000000000001").unwrap()),
            Some("12.000000000001".into())
        );
        assert_eq!(decimal_text(&json!({"decimal": "0.1"})), Some("0.1".into()));
        assert_eq!(decimal_text(&json!({"int": 5})), Some("5".into()));
    }

    /// Live dev check (B-1): `KDX_LOCAL_NODE=<service url> cargo test -p kdx-node -- --ignored
    /// local_live`. Six `/local` reads on mainnet01 chains 0 and 8 (two more per optional
    /// `KDX_LOCAL_NFT` / `KDX_LOCAL_ACCOUNT` holding), nothing else.
    #[tokio::test]
    #[ignore]
    async fn local_live() {
        let Ok(base) = std::env::var("KDX_LOCAL_NODE") else {
            return;
        };
        let node = NodeClient::new(&crate::NodeClientConfig {
            base: base.parse().unwrap(),
            network: "mainnet01".into(),
            timeout: std::time::Duration::from_secs(20),
            max_rps: 5,
            concurrency: 1,
            p2p_url: None,
            p2p_cert_sha256: None,
            max_body_bytes: 1 << 20,
            max_sse_event_bytes: 1 << 20,
        })
        .unwrap();
        let l = LocalClient::new(
            node,
            LocalOpts {
                rps: 5,
                concurrency: 1,
            },
        );
        let hostile = l.details(0, "coin", "x\") (coin.transfer").await.unwrap();
        assert_eq!(hostile, Local::Absent);
        assert!(matches!(
            l.details(0, "coin (x)", "a").await,
            Err(LocalError::Skipped(_))
        ));
        let ks = l.describe_keyset(0, "ns-admin-keyset").await.unwrap();
        assert!(matches!(ks, Local::Found { height, .. } if height > 0));
        let miss = l
            .describe_keyset(0, "free.kdx-no-such-keyset")
            .await
            .unwrap();
        assert_eq!(miss, Local::Absent);
        // GQ-15: hostile token and account ids are data and answer `Absent` on both forms.
        let (t, a) = ("t:x\") (coin.transfer", "k:y\") (z");
        let v2 = "marmalade-v2.ledger";
        assert_eq!(l.nft_guard(8, v2, t, a).await.unwrap(), Local::Absent);
        assert_eq!(l.nft_token_info(8, v2, t).await.unwrap(), Local::Absent);
        // `KDX_LOCAL_NFT=chain,ledger,token,account`: one real holding.
        if let Ok(spec) = std::env::var("KDX_LOCAL_NFT") {
            let p: Vec<&str> = spec.splitn(4, ',').collect();
            let chain: u8 = p[0].parse().unwrap();
            let g = l.nft_guard(chain, p[1], p[2], p[3]).await.unwrap();
            println!("guard: {g:?}");
            assert!(matches!(g, Local::Found { .. }));
            match l.nft_token_info(chain, p[1], p[2]).await {
                Err(LocalError::Failure(m)) => {
                    println!("info: {m}");
                    assert!(is_no_member_message(&m, p[1], "get-token-info"));
                }
                r => {
                    println!("info: {r:?}");
                    assert!(matches!(r, Ok(Local::Found { .. })));
                }
            }
        }
        if let Ok(acct) = std::env::var("KDX_LOCAL_ACCOUNT") {
            let d = l.details(0, "coin", &acct).await.unwrap();
            println!("{acct}: {d:?}");
            assert!(matches!(d, Local::Found { .. }));
        }
    }
}
