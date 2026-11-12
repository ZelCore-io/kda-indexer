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
//! Answers: a success gives the value and the tip height it was read at
//! (`metaData.blockHeight − 1`). A failure whose message says
//! the row or keyset does not exist is [`Local::Absent`] (the node sends no `metaData` then).
//! Any other Pact failure, and any delivered answer that cannot be used, is
//! [`LocalError::Failure`]; only transport and HTTP errors are [`LocalError::Node`]. Neither is
//! ever read as a zero balance.

use std::time::{SystemTime, UNIX_EPOCH};

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
    /// The value, and the height of the block whose state the node read: its tip,
    /// `metaData.blockHeight − 1` (`blockHeight` is the pending block's height).
    Found { value: T, height: u64 },
    /// The node has no such row or keyset.
    Absent,
}

/// `{module}.details`: the balance as the node printed it and the guard.
#[derive(Debug, Clone, PartialEq)]
pub struct Details {
    pub balance: String,
    pub guard: Value,
}

#[derive(Debug, thiserror::Error)]
pub enum LocalError {
    /// B-1 refused the call (bad module name, over-long name): nothing was sent.
    #[error("skipped: {0}")]
    Skipped(String),
    /// The node evaluated the code and it failed for another reason than a missing row, or
    /// its answer arrived but is unusable (not JSON, over the body cap, no `result`, no
    /// `metaData.blockHeight`, no decimal balance). A property of this call.
    #[error("pact failure: {0}")]
    Failure(String),
    /// Transport or HTTP error (timeout, connection, non-2xx status): retry later.
    #[error("node: {0}")]
    Node(#[from] NodeError),
}

pub type LocalResult<T> = std::result::Result<Local<T>, LocalError>;

/// The B-1 module name check: `name` or `namespace.name`, identifier characters only.
pub fn valid_module(m: &str) -> bool {
    fn ident(s: &str) -> bool {
        let mut b = s.bytes();
        matches!(b.next(), Some(c) if c.is_ascii_alphabetic() || c == b'_')
            && b.all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
    }
    let mut parts = m.splitn(2, '.');
    let first = parts.next().unwrap_or("");
    ident(first) && parts.next().is_none_or(ident)
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

/// `{ledger}.get-token-info` of one token (GQ-15): `precision`, `supply` (decimal text) and `uri`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenInfo {
    pub precision: i32,
    pub supply: String,
    pub uri: String,
}

/// Longest decimal text accepted from a node answer (bytes).
pub const MAX_DECIMAL_LEN: usize = 128;

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
        Value::Number(n) => Some(n.to_string()),
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

/// A delivered answer that cannot be used: a per-call [`LocalError::Failure`], never a node
/// error (one odd answer must not make the caller back off for every key).
fn invalid(what: &str) -> LocalError {
    let cut: String = what.chars().take(200).collect();
    LocalError::Failure(format!("invalid answer: {cut}"))
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
        let r = self.exchange(chain, code, data).await;
        (r, t0.elapsed().as_secs_f64())
    }

    async fn exchange(&self, chain: u8, code: &str, data: Value) -> LocalResult<Value> {
        let body = command(self.node.network(), chain, code, &data, now_secs());
        let r = match self.node.local(chain, &body).await {
            Ok(r) => r,
            // An answer arrived but is not usable (not JSON, over the body cap): a property of
            // this call, not of the node's health.
            Err(e @ (NodeError::Decode(_) | NodeError::TooLarge { .. })) => {
                return Err(invalid(&e.to_string()))
            }
            Err(e) => return Err(e.into()),
        };
        let result = r.get("result").ok_or_else(|| invalid("no result"))?;
        match result.get("status").and_then(Value::as_str) {
            Some("success") => {
                // `metaData.blockHeight` is the height of the block the node would build
                // next (its tip + 1, measured on mainnet01); the state read is the tip's.
                let height = r
                    .pointer("/metaData/blockHeight")
                    .and_then(Value::as_u64)
                    .and_then(|h| h.checked_sub(1))
                    .ok_or_else(|| invalid("no metaData.blockHeight"))?;
                let value = result.get("data").cloned().unwrap_or(Value::Null);
                Ok(Local::Found { value, height })
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
            Local::Found { value, height } => {
                let balance = value
                    .get("balance")
                    .and_then(decimal_text)
                    .ok_or_else(|| invalid("details: no decimal balance"))?;
                let guard = value.get("guard").cloned().unwrap_or(Value::Null);
                Ok(Local::Found {
                    value: Details { balance, guard },
                    height,
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
            Local::Found { value, height } => match value.as_bool() {
                Some(b) => Ok(Local::Found { value: b, height }),
                None => Err(invalid("describe-module: not a boolean")),
            },
            Local::Absent => Ok(Local::Absent),
        }
    }

    /// `(describe-keyset (read-msg "k"))` with `data {"k": name}`: the keyset JSON.
    pub async fn describe_keyset(&self, chain: u8, name: &str) -> LocalResult<Value> {
        check_name("keyset", name)?;
        self.call(
            chain,
            "(describe-keyset (read-msg \"k\"))",
            json!({ "k": name }),
        )
        .await
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
            Local::Found { value, height } => Ok(Local::Found {
                value: decimal_text(&value).ok_or_else(|| invalid("get-balance: not a decimal"))?,
                height,
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

fn echoes(v: &Value, field: &str, want: &str) -> bool {
    v.get(field).is_none_or(|x| x.as_str() == Some(want))
}

fn nft_guard_of(r: Local<Value>, token_id: &str, account: &str) -> LocalResult<String> {
    let (value, height) = match r {
        Local::Absent => return Ok(Local::Absent),
        Local::Found { value, height } => (value, height),
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
    })
}

fn token_info_of(r: Local<Value>, token_id: &str) -> LocalResult<TokenInfo> {
    let (value, height) = match r {
        Local::Absent => return Ok(Local::Absent),
        Local::Found { value, height } => (value, height),
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
                height: 9
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
                height: 9
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
        })
        .is_err());
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
