//! Read-only Pact `/local` calls for balances, guards and keysets (spec §2.1, §3.9; plan
//! Task 9, rulings B-1 and H-1).
//!
//! Safety (B-1):
//! - Only three code forms are ever sent, each a fixed text with names passed in `data`:
//!   `({m}.details (read-msg "a"))`, `(describe-keyset (read-msg "k"))` and
//!   `({m}.get-balance (read-msg "t") (read-msg "a"))`. Account, keyset and token names come
//!   from chain data and may be hostile; they never enter code.
//! - `m` must match `^[A-Za-z_][A-Za-z0-9_\-]*(\.[A-Za-z_][A-Za-z0-9_\-]*)?$`, otherwise the
//!   call is not made ([`LocalError::Skipped`]).
//! - Names are capped at [`MAX_NAME_LEN`] bytes; `meta.gasLimit` is [`GAS_LIMIT`].
//! - Calls pass a token bucket (`rps`) and a concurrency cap of their own, then the node
//!   client's limits. Nothing is ever signed or sent to `/send`.
//!
//! Answers: a success gives the value and the tip height it was read at
//! (`metaData.blockHeight − 1`). A failure whose message says
//! the row or keyset does not exist is [`Local::Absent`] (the node sends no `metaData` then).
//! Any other Pact failure is [`LocalError::Failure`]; transport, HTTP and decode errors are
//! [`LocalError::Node`]. Neither is ever read as a zero balance.

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
    /// Calls started per second (`balances.local_rps`); 0 = no limit of its own.
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
    /// The node evaluated the code and it failed for another reason than a missing row.
    #[error("pact failure: {0}")]
    Failure(String),
    /// Transport, HTTP, decode error or an answer without `metaData.blockHeight`: retry later.
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

/// A decimal as the node printed it: a JSON number's text, `{"decimal": …}` or `{"int": …}`.
pub fn decimal_text(v: &Value) -> Option<String> {
    match v {
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
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
        let r = self.node.local(chain, &body).await?;
        let result = r
            .get("result")
            .ok_or_else(|| NodeError::Decode("local: no result".into()))?;
        match result.get("status").and_then(Value::as_str) {
            Some("success") => {
                // `metaData.blockHeight` is the height of the block the node would build
                // next (its tip + 1, measured on mainnet01); the state read is the tip's.
                let height = r
                    .pointer("/metaData/blockHeight")
                    .and_then(Value::as_u64)
                    .and_then(|h| h.checked_sub(1))
                    .ok_or_else(|| NodeError::Decode("local: no metaData.blockHeight".into()))?;
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
            _ => Err(NodeError::Decode("local: result has no status".into()).into()),
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
                    .ok_or_else(|| NodeError::Decode("details: no balance".into()))?;
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
                value: decimal_text(&value)
                    .ok_or_else(|| NodeError::Decode("get-balance: not a decimal".into()))?,
                height,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    /// local_live`. Four `/local` reads on mainnet01 chain 0, nothing else.
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
        if let Ok(acct) = std::env::var("KDX_LOCAL_ACCOUNT") {
            let d = l.details(0, "coin", &acct).await.unwrap();
            println!("{acct}: {d:?}");
            assert!(matches!(d, Local::Found { .. }));
        }
    }
}
