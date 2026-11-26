//! `pactQuery` and `gasLimitEstimate`: client Pact run through the node's `/local` (B1,
//! owner-approved 2026-11-26), answered the way stock (hack-a-chain `indexer-kadena`)
//! answers them.
//!
//! Stock behaviour copied (fixtures in `tests/data/pact/`, captured from stock):
//! - `pactQuery`: each item is an unsigned `/local` call (preflight and signature
//!   verification off, gas limit 2500, `@kadena/client` default meta). `result` is
//!   `JSON.stringify` of the node's `result.data` as JavaScript decodes it ([`js_json`]);
//!   `error` is the Pact error's `message`, or `{}` when the node answered no Pact error (an
//!   HTTP error, an unknown chain); `timeout` after 10 s with stock's fixed text.
//! - `gasLimitEstimate`: stock's parser (a zod schema: `payload`, `meta` strings, `sigs`,
//!   `signers` string arrays; unknown keys, `networkId` among them, dropped), its six-shape
//!   classifier and its command builder. The first input that does not parse or classify
//!   fails the field with that input's message; then any node failure fails it with
//!   [`GAS_NODE`].
//!
//! Stock mistakes kept, because the answer stays the same and costs us less: `payload`,
//! `meta` and `signers` must be strings in stock's schema, so `full-command`,
//! `partial-command` and `payload` inputs can never build a command the node accepts; they
//! answer [`GAS_NODE`] without a node call. `networkId` never overrides the network.
//!
//! Our limits (stock has none): items per request (all aliases together), a per-IP token
//! bucket, a global in-flight cap, the input size, the gas limit a command may ask for, the
//! node answer's size. Every refusal is a stock-style message.

use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_graphql::{Context, ErrorExtensions};
use base64::Engine as _;
use blake2::Digest as _;
use serde_json::{Map, Value};
use tokio::sync::Semaphore;

use crate::error::INTERNAL_SERVER_ERROR;
use crate::schema::{GasLimitEstimation, PactQuery, PactQueryResponse};

/// Stock's `pactQuery` timeout text (`utils/raw-query.ts`).
pub const TIMEOUT_MSG: &str =
    "[ERROR][RAW_QUERY][CREATE_TIMEOUT] The query took too long to execute and was aborted";
/// Stock's `pactQuery` error when the node answered no Pact error: `JSON.stringify` of its own
/// error object.
pub const NODE_FAILURE: &str = "{}";
/// Stock's `gasLimitEstimate` error for any node-side failure.
pub const GAS_NODE: &str =
    "[ERROR][GAS_API_GATEWAY][ESTIMATE_GAS] Chainweb Node was unable to estimate the gas limit";
/// Stock's `gasLimitEstimate` error for an input that is not JSON or fails its schema.
pub const GAS_PARSE: &str =
    "[ERROR][GAS][PARSER] Unable to parse input as JSON. Please see the README for the accepted input format.";
/// Our refusal of a command holding a duplicate JSON key (fix round 1, M1).
pub const GAS_DUP_KEYS: &str = "[ERROR][GAS][INPUT] The command holds a duplicate JSON key";
/// Stock's `gasLimitEstimate` error for an input of no known shape.
pub const GAS_UNKNOWN: &str =
    "[ERROR][GRAPHQL][GAS][INPUT_CHECKER] Unknown input type. Please see the README for the accepted input format.";

/// `@kadena/client` defaults stock's `pactQuery` sends (`dirtyReadClient`).
const QUERY_GAS_LIMIT: u64 = 2500;
/// Stock's gas-estimate defaults (`domain/gas/transaction.gas.ts`).
const ESTIMATE_GAS_LIMIT: u64 = 10_000;
const TTL: u64 = 28_800;

/// `[graphql.pact]`: the passthrough's limits.
#[derive(Debug, Clone)]
pub struct PactSettings {
    /// Items (`pactQuery` entries plus `gasLimitEstimate` inputs) one request may hold,
    /// across all its fields and aliases.
    pub max_items: usize,
    /// `/local` calls in flight across the process.
    pub max_in_flight: usize,
    /// Per client (the API limiter's keys: an IPv4 address, v4-mapped IPv6 unmapped, or an
    /// IPv6 /64, with a /24 or /48 overflow tier): `/local` calls per second (0 = no per-IP
    /// limit) and burst.
    pub per_ip_rps: u32,
    pub per_ip_burst: u32,
    /// Per item, waiting for a slot included (stock: 10 s).
    pub timeout: Duration,
    /// Largest `code` plus `data` of one `pactQuery` item, or one `gasLimitEstimate` input.
    pub max_input_bytes: usize,
    /// Largest `meta.gasLimit` a client command may ask for.
    pub max_gas_limit: u64,
    /// Largest node answer read (the node client's body cap).
    pub max_response_bytes: usize,
    /// Chains of the network (`0..chains`).
    pub chains: u8,
}

impl Default for PactSettings {
    fn default() -> Self {
        PactSettings {
            max_items: 20,
            max_in_flight: 8,
            per_ip_rps: 5,
            per_ip_burst: 20,
            timeout: Duration::from_secs(10),
            max_input_bytes: 64 * 1024,
            max_gas_limit: 150_000,
            max_response_bytes: 1 << 20,
            chains: 20,
        }
    }
}

/// `code` + `data` bytes of one `pactQuery` item.
fn query_size(q: &PactQuery) -> usize {
    q.code.len()
        + q.data
            .iter()
            .flatten()
            .map(|d| d.key.len() + d.value.len())
            .sum::<usize>()
}

/// One request's share of the passthrough, for all its fields and aliases (and the
/// operations of a batch): the items asked for so far, and its one place among the
/// requests inside ([`PactPass::enter`]), released when the request ends; and its one time
/// cap, set when its first Pact field starts (secpass B M-8).
#[derive(Debug, Clone, Default)]
pub struct PactBudget {
    used: Arc<AtomicUsize>,
    place: Arc<Mutex<Option<tokio::sync::OwnedSemaphorePermit>>>,
    until: Arc<OnceLock<Instant>>,
}

/// The passthrough: the node client (fixed URL and network), the slots, the per-IP buckets.
pub struct PactPass {
    node: kdx_node::NodeClient,
    settings: PactSettings,
    slots: Semaphore,
    /// Requests inside the passthrough (2 × `max_in_flight`): past it a request is refused at
    /// once, so waiting requests cannot hold the API's in-flight slots.
    requests: Arc<Semaphore>,
    /// Per-client buckets (`None` when `per_ip_rps` is 0): the API's limiter (I3).
    rate: Option<kdx_api::limits::RateLimiter>,
}

impl std::fmt::Debug for PactPass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PactPass")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy)]
enum Field {
    Query,
    Gas,
}

impl Field {
    fn label(self) -> &'static str {
        match self {
            Field::Query => "pactQuery",
            Field::Gas => "gasLimitEstimate",
        }
    }
    fn tag(self) -> &'static str {
        match self {
            Field::Query => "RAW_QUERY",
            Field::Gas => "GAS",
        }
    }
}

fn metric(field: Field, outcome: &'static str) {
    kdx_metrics::get().graphql_pact(field.label(), outcome);
}

/// A field error as stock answers a thrown resolver error: `extensions.code` (and, on the
/// wire, `code`) `INTERNAL_SERVER_ERROR`.
fn field_err(msg: impl Into<String>) -> async_graphql::Error {
    async_graphql::Error::new(msg).extend_with(|_, e| e.set("code", INTERNAL_SERVER_ERROR))
}

/// A refusal of one of this indexer's Pact caps: stock's code, plus `extensions.limit = name`
/// so it is nulled in place rather than bubbled ([`crate::error::nulls_in_place`]).
fn cap_err(name: &'static str, msg: impl Into<String>) -> async_graphql::Error {
    field_err(msg).extend_with(|_, e| e.set("limit", name))
}

/// Why a `/local` call gave no Pact answer.
enum CallErr {
    Timeout,
    TooLarge,
    /// Transport, non-2xx status, or a body that is not JSON.
    Node,
}

impl PactPass {
    /// `node` must be a client of its own: its concurrency and body cap are this
    /// passthrough's (`max_in_flight`, `max_response_bytes`).
    pub fn new(node: kdx_node::NodeClient, settings: PactSettings) -> PactPass {
        PactPass::with_rate_cap(node, settings, None)
    }

    /// As [`PactPass::new`], with at most `cap` per-client buckets (tests; `None` is the
    /// API limiter's default).
    #[doc(hidden)]
    pub fn with_rate_cap(
        node: kdx_node::NodeClient,
        settings: PactSettings,
        cap: Option<usize>,
    ) -> PactPass {
        let (rps, burst) = (settings.per_ip_rps, settings.per_ip_burst);
        let rate = (rps > 0).then(|| match cap {
            Some(c) => kdx_api::limits::RateLimiter::with_cap(rps, burst, c),
            None => kdx_api::limits::RateLimiter::new(rps, burst),
        });
        PactPass {
            node,
            slots: Semaphore::new(settings.max_in_flight.max(1)),
            requests: Arc::new(Semaphore::new(
                settings.max_in_flight.max(1).saturating_mul(2),
            )),
            settings,
            rate,
        }
    }

    /// Free node slots and free request places (tests: every permit comes back).
    #[doc(hidden)]
    pub fn idle(&self) -> (usize, usize) {
        (
            self.slots.available_permits(),
            self.requests.available_permits(),
        )
    }

    /// I1: a Pact field is about to call the node, so the request may now run to the
    /// passthrough's timeout (plus the answer margin). Only here is the request's shared
    /// deadline extended, never past the ceiling the guard set.
    fn extend_deadline(&self, ctx: &Context<'_>) {
        if let Some(r) = ctx.data_opt::<crate::RequestRun>() {
            r.0.extend_to(
                Instant::now()
                    + self.settings.timeout
                    + crate::http::PACT_MARGIN
                    + crate::http::DEADLINE_MARGIN,
            );
        }
    }

    pub fn settings(&self) -> &PactSettings {
        &self.settings
    }

    /// The chain a client string names: canonical decimal (`"7"`, not `"07"`) below `chains`.
    /// Anything else never reaches the node (stock's node answers 404 there).
    fn chain(&self, s: &str) -> Option<u8> {
        let n: u8 = s.parse().ok()?;
        (n.to_string() == s && n < self.settings.chains).then_some(n)
    }

    /// The per-request item budget (checked, not yet charged), then the per-IP bucket, then
    /// the budget charged: a field the bucket refuses spends none of the request's items
    /// (secpass B M-10). With the per-IP limit on, a request with no peer address (never one
    /// the HTTP server takes) is refused rather than sharing one bucket (M-9).
    fn admit(&self, ctx: &Context<'_>, n: usize, field: Field) -> async_graphql::Result<()> {
        let max = self.settings.max_items;
        let budget = ctx.data_opt::<PactBudget>();
        let limit = || {
            metric(field, "limit_items");
            kdx_metrics::get().graphql_rejected("pact_items");
            cap_err(
                "pact_items",
                format!(
                    "[ERROR][{}][LIMIT] At most {max} Pact queries and gas estimates per request",
                    field.tag()
                ),
            )
        };
        let used = budget.map_or(0, |b| b.used.load(Ordering::Acquire));
        if n > max || used.saturating_add(n) > max {
            return Err(limit());
        }
        let allowed = match (&self.rate, crate::client_ip(ctx)) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(_), Some(ip)) => self.take(ip, n),
        };
        if !allowed {
            metric(field, "limit_rate");
            kdx_metrics::get().graphql_rejected("pact_rate");
            return Err(cap_err(
                "pact_rate",
                format!(
                    "[ERROR][{}][RATE_LIMIT] Too many Pact queries from this client, retry later",
                    field.tag()
                ),
            ));
        }
        // Charged now; a concurrent field of the same request may have taken the room.
        if let Some(b) = budget {
            if b.used.fetch_add(n, Ordering::AcqRel).saturating_add(n) > max {
                return Err(limit());
            }
        }
        Ok(())
    }

    /// The request's Pact time cap: `timeout` after its first Pact field started; every item
    /// of every field of the request (aliases, batch operations) ends by it.
    fn until(&self, ctx: &Context<'_>) -> Instant {
        let fresh = || Instant::now() + self.settings.timeout;
        ctx.data_opt::<PactBudget>()
            .map_or_else(fresh, |b| *b.until.get_or_init(fresh))
    }

    /// Whether an item passes the local checks (size, chain), so it would call the node.
    fn query_runs(&self, q: &PactQuery) -> bool {
        query_size(q) <= self.settings.max_input_bytes && self.chain(&q.chain_id).is_some()
    }

    /// A request's place in the passthrough (one per request, however many fields and
    /// aliases name it; held in its [`PactBudget`] until it ends), or a refusal when the
    /// passthrough is full. Without a budget (no HTTP request) the field holds its own.
    fn enter(
        &self,
        ctx: &Context<'_>,
        field: Field,
    ) -> async_graphql::Result<Option<tokio::sync::OwnedSemaphorePermit>> {
        let budget = ctx.data_opt::<PactBudget>();
        let mut held = budget.map(|b| b.place.lock().unwrap_or_else(|e| e.into_inner()));
        if held.as_ref().is_some_and(|h| h.is_some()) {
            return Ok(None);
        }
        let permit = self.requests.clone().try_acquire_owned().map_err(|_| {
            metric(field, "limit_busy");
            kdx_metrics::get().graphql_rejected("pact_busy");
            cap_err(
                "pact_busy",
                format!(
                    "[ERROR][{}][BUSY] The Pact query service is busy, retry later",
                    field.tag()
                ),
            )
        })?;
        match held.as_deref_mut() {
            Some(slot) => {
                *slot = Some(permit);
                Ok(None)
            }
            None => Ok(Some(permit)),
        }
    }

    /// Take `n` tokens from the client's bucket ([`kdx_api::limits::RateLimiter`]: /64 keys
    /// with a /48 overflow tier, v4-mapped addresses unmapped, eviction never refills a
    /// drained key).
    fn take(&self, ip: IpAddr, n: usize) -> bool {
        self.rate
            .as_ref()
            .is_none_or(|r| r.allow_n(ip, u32::try_from(n).unwrap_or(u32::MAX)))
    }

    /// One `/local` call: a slot, then the node, all within `deadline`.
    async fn call(
        &self,
        chain: u8,
        body: Vec<u8>,
        preflight: bool,
        sigs: bool,
        deadline: Instant,
    ) -> Result<Value, CallErr> {
        let left = || deadline.saturating_duration_since(Instant::now());
        let _slot = tokio::time::timeout(left(), self.slots.acquire())
            .await
            .map_err(|_| CallErr::Timeout)?
            .map_err(|_| CallErr::Node)?;
        let t = left();
        if t.is_zero() {
            return Err(CallErr::Timeout);
        }
        let res = tokio::time::timeout(t, self.node.local_raw(chain, body, preflight, sigs, t))
            .await
            .map_err(|_| CallErr::Timeout)?;
        let (status, bytes) = match res {
            Ok(x) => x,
            Err(kdx_node::NodeError::Timeout) => return Err(CallErr::Timeout),
            Err(kdx_node::NodeError::TooLarge { .. }) => return Err(CallErr::TooLarge),
            Err(_) => return Err(CallErr::Node),
        };
        if !(200..300).contains(&status) {
            return Err(CallErr::Node);
        }
        serde_json::from_slice(&bytes).map_err(|_| CallErr::Node)
    }

    /// `pactQuery`: every item, in order, at once (bounded by the slots).
    pub async fn pact_query(
        &self,
        ctx: &Context<'_>,
        items: Vec<PactQuery>,
    ) -> async_graphql::Result<Vec<PactQueryResponse>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        self.admit(ctx, items.len(), Field::Query)?;
        let _in = self.enter(ctx, Field::Query)?;
        // Only when an item will call the node (secpass B M-6), as for gas estimates.
        if items.iter().any(|q| self.query_runs(q)) {
            self.extend_deadline(ctx);
        }
        let until = self.until(ctx);
        Ok(futures::future::join_all(items.into_iter().map(|q| self.one_query(q, until))).await)
    }

    async fn one_query(&self, q: PactQuery, until: Instant) -> PactQueryResponse {
        let (status, result, error) = self.query_answer(&q, until).await;
        metric(Field::Query, status_label(status, error.as_deref()));
        PactQueryResponse {
            chain_id: q.chain_id,
            code: q.code,
            status: status.to_owned(),
            result,
            error,
        }
    }

    async fn query_answer(
        &self,
        q: &PactQuery,
        until: Instant,
    ) -> (&'static str, Option<String>, Option<String>) {
        let err = |e: String| ("error", None, Some(e));
        if query_size(q) > self.settings.max_input_bytes {
            return err(format!(
                "[ERROR][RAW_QUERY][LIMIT] Pact code and data exceed {} bytes",
                self.settings.max_input_bytes
            ));
        }
        let Some(chain) = self.chain(&q.chain_id) else {
            return err(NODE_FAILURE.into());
        };
        let mut data = Map::new();
        for d in q.data.iter().flatten() {
            data.insert(d.key.clone(), Value::String(d.value.clone()));
        }
        let (secs, ms) = now();
        let cmd = query_cmd(
            self.node.network(),
            &q.chain_id,
            &q.code,
            &Value::Object(data),
            secs,
            ms,
        );
        let deadline = (Instant::now() + self.settings.timeout).min(until);
        match self
            .call(
                chain,
                transaction(&cmd, &[]).into_bytes(),
                false,
                false,
                deadline,
            )
            .await
        {
            Err(CallErr::Timeout) => ("timeout", None, Some(TIMEOUT_MSG.into())),
            Err(CallErr::TooLarge) => err(format!(
                "[ERROR][RAW_QUERY][LIMIT] The node's answer exceeds {} bytes",
                self.settings.max_response_bytes
            )),
            Err(CallErr::Node) => err(NODE_FAILURE.into()),
            Ok(v) => query_result(&v),
        }
    }

    /// `gasLimitEstimate`: parse and classify every input in order (the first bad one fails
    /// the field, as stock's synchronous `map` throws), then the node calls at once; the
    /// first failure fails the field and drops the rest.
    pub async fn gas_limit_estimate(
        &self,
        ctx: &Context<'_>,
        input: Vec<String>,
    ) -> async_graphql::Result<Vec<GasLimitEstimation>> {
        if input.is_empty() {
            return Ok(Vec::new());
        }
        self.admit(ctx, input.len(), Field::Gas)?;
        let _in = self.enter(ctx, Field::Gas)?;
        let plans = input
            .iter()
            .map(|s| self.plan_gas(s))
            .collect::<Result<Vec<_>, _>>()
            .inspect_err(|_| metric(Field::Gas, "bad_input"))?;
        if plans.iter().any(|p| matches!(p, GasPlan::Call { .. })) {
            self.extend_deadline(ctx);
        }
        let until = self.until(ctx);
        futures::future::try_join_all(plans.into_iter().map(|p| self.run_gas(p, until))).await
    }

    fn plan_gas(&self, s: &str) -> async_graphql::Result<GasPlan> {
        if s.len() > self.settings.max_input_bytes {
            return Err(field_err(format!(
                "[ERROR][GAS][LIMIT] Input exceeds {} bytes",
                self.settings.max_input_bytes
            )));
        }
        let input = parse_gas_input(s).ok_or_else(|| field_err(GAS_PARSE))?;
        let has = |k: &str| input.contains_key(k);
        let st = |k: &str| input.get(k).and_then(Value::as_str).unwrap_or_default();
        let sigs = || -> Vec<String> {
            input
                .get("sigs")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default()
        };
        // The shape is stock's `inputType`; these never reach the node (module docs).
        let fail = |_shape: &'static str| Ok(GasPlan::Fail(GAS_NODE.into()));
        if has("cmd") && has("hash") && has("sigs") {
            self.signed_plan(
                "full-transaction",
                st("cmd"),
                st("hash").to_owned(),
                sigs(),
                true,
            )
        } else if has("cmd") {
            let hash = blake_b64(st("cmd"));
            self.signed_plan("stringified-command", st("cmd"), hash, sigs(), false)
        } else if has("payload") && has("meta") && has("signers") {
            fail("full-command")
        } else if has("payload") && (has("meta") || (has("signers") && has("chainId"))) {
            fail("partial-command")
        } else if has("payload") && has("chainId") {
            fail("payload")
        } else if has("code") && has("chainId") {
            let chain_id = st("chainId");
            let Some(chain) = self.chain(chain_id) else {
                return fail("code");
            };
            let (secs, ms) = now();
            let cmd = code_cmd(chain_id, st("code"), secs, ms);
            Ok(GasPlan::Call {
                input_type: "code",
                chain,
                transaction: transaction(&cmd, &[]),
                preflight: false,
                sigs: false,
            })
        } else {
            Err(field_err(GAS_UNKNOWN))
        }
    }

    /// `full-transaction` / `stringified-command`: the client's command text as is (signed
    /// text cannot be changed), checked for a chain and a gas limit within the cap; preflight
    /// on, signature verification on for a full transaction (stock's flags).
    fn signed_plan(
        &self,
        input_type: &'static str,
        cmd: &str,
        hash: String,
        sigs: Vec<String>,
        verify: bool,
    ) -> async_graphql::Result<GasPlan> {
        // M1: a key given twice anywhere could read one way here (serde_json keeps the last)
        // and another way at the node; such a command never reaches it.
        let parsed: Option<Value> = serde_json::from_str(cmd).ok();
        if parsed.is_some() && serde_json::from_str::<NoDupKeys>(cmd).is_err() {
            return Ok(GasPlan::Fail(GAS_DUP_KEYS.into()));
        }
        let meta = parsed.as_ref().and_then(|v| v.get("meta"));
        let chain = meta
            .and_then(|m| m.get("chainId"))
            .and_then(Value::as_str)
            .and_then(|c| self.chain(c));
        let Some(chain) = chain else {
            return Ok(GasPlan::Fail(GAS_NODE.into()));
        };
        let gas = meta.and_then(|m| m.get("gasLimit")).and_then(Value::as_f64);
        match gas {
            Some(g) if g <= self.settings.max_gas_limit as f64 => {}
            Some(_) => {
                return Ok(GasPlan::Fail(format!(
                    "[ERROR][GAS][LIMIT] gasLimit above the server cap of {}",
                    self.settings.max_gas_limit
                )))
            }
            None => return Ok(GasPlan::Fail(GAS_NODE.into())),
        }
        let mut tx = Map::new();
        tx.insert("cmd".into(), Value::String(cmd.to_owned()));
        tx.insert("hash".into(), Value::String(hash));
        tx.insert(
            "sigs".into(),
            Value::Array(
                sigs.into_iter()
                    .map(|s| {
                        let mut o = Map::new();
                        o.insert("sig".into(), Value::String(s));
                        Value::Object(o)
                    })
                    .collect(),
            ),
        );
        Ok(GasPlan::Call {
            input_type,
            chain,
            transaction: js_json(&Value::Object(tx)),
            preflight: true,
            sigs: verify,
        })
    }

    async fn run_gas(
        &self,
        plan: GasPlan,
        until: Instant,
    ) -> async_graphql::Result<GasLimitEstimation> {
        let (input_type, chain, transaction, preflight, sigs) = match plan {
            GasPlan::Fail(msg) => {
                metric(Field::Gas, "error");
                return Err(field_err(msg));
            }
            GasPlan::Call {
                input_type,
                chain,
                transaction,
                preflight,
                sigs,
            } => (input_type, chain, transaction, preflight, sigs),
        };
        let deadline = (Instant::now() + self.settings.timeout).min(until);
        let v = match self
            .call(
                chain,
                transaction.clone().into_bytes(),
                preflight,
                sigs,
                deadline,
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                metric(
                    Field::Gas,
                    match e {
                        CallErr::Timeout => "timeout",
                        CallErr::TooLarge => "too_large",
                        CallErr::Node => "node_error",
                    },
                );
                return Err(field_err(GAS_NODE));
            }
        };
        // `@kadena/client`'s `local` unwraps `preflightResult` when preflight is on.
        let r = if preflight {
            v.get("preflightResult").cloned().unwrap_or(Value::Null)
        } else {
            v
        };
        let ok = r.pointer("/result/status").and_then(Value::as_str) == Some("success");
        let amount = r
            .get("gas")
            .and_then(Value::as_i64)
            .and_then(|g| i32::try_from(g).ok());
        match (ok, amount) {
            (true, Some(amount)) => {
                metric(Field::Gas, "success");
                Ok(GasLimitEstimation {
                    amount,
                    input_type: input_type.to_owned(),
                    transaction,
                    used_preflight: preflight,
                    used_signature_verification: sigs,
                })
            }
            _ => {
                metric(Field::Gas, "error");
                Err(field_err(GAS_NODE))
            }
        }
    }
}

enum GasPlan {
    /// Fails in the node-call phase with this message (stock's node would refuse it, or a cap).
    Fail(String),
    Call {
        input_type: &'static str,
        chain: u8,
        transaction: String,
        preflight: bool,
        sigs: bool,
    },
}

fn status_label(status: &str, error: Option<&str>) -> &'static str {
    match (status, error) {
        ("success", _) => "success",
        ("timeout", _) => "timeout",
        (_, Some(NODE_FAILURE)) => "node_error",
        (_, Some(e)) if e.starts_with("[ERROR][RAW_QUERY][LIMIT]") => "limit",
        _ => "error",
    }
}

/// Stock's zod schema over `JSON.parse(input)`: an object whose known keys, when present,
/// have the right types (`null` is not "absent"); unknown keys are dropped.
fn parse_gas_input(s: &str) -> Option<Map<String, Value>> {
    let Value::Object(o) = serde_json::from_str::<Value>(s).ok()? else {
        return None;
    };
    let mut out = Map::new();
    for (k, v) in o {
        let ok = match k.as_str() {
            "cmd" | "hash" | "payload" | "meta" | "chainId" | "code" => v.is_string(),
            "sigs" | "signers" => v.as_array().is_some_and(|a| a.iter().all(Value::is_string)),
            _ => continue,
        };
        if !ok {
            return None;
        }
        out.insert(k, v);
    }
    Some(out)
}

/// Seconds and milliseconds since the epoch.
fn now() -> (u64, u128) {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs(), d.as_millis())
}

/// blake2b-256 of `s`, base64url without padding (a Pact command hash).
pub fn blake_b64(s: &str) -> String {
    let d = blake2::Blake2b::<blake2::digest::consts::U32>::digest(s.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(d)
}

fn js_str(s: &str) -> String {
    Value::String(s.to_owned()).to_string()
}

/// The command text of a `pactQuery` item (`@kadena/client` `dirtyReadClient` defaults).
pub fn query_cmd(
    network: &str,
    chain_id: &str,
    code: &str,
    data: &Value,
    secs: u64,
    ms: u128,
) -> String {
    format!(
        "{{\"payload\":{{\"exec\":{{\"code\":{code},\"data\":{data}}}}},\"nonce\":\"kjs:nonce:{ms}\",\
         \"signers\":[],\"meta\":{{\"gasLimit\":{QUERY_GAS_LIMIT},\"gasPrice\":1e-8,\"sender\":\"\",\
         \"ttl\":{TTL},\"creationTime\":{secs},\"chainId\":{chain}}},\"networkId\":{net}}}",
        code = js_str(code),
        data = js_json(data),
        chain = js_str(chain_id),
        net = js_str(network),
    )
}

/// The command text stock builds for a `code` gas input: its key order, no `networkId`
/// (captured from stock).
pub fn code_cmd(chain_id: &str, code: &str, secs: u64, ms: u128) -> String {
    format!(
        "{{\"meta\":{{\"gasLimit\":{ESTIMATE_GAS_LIMIT},\"gasPrice\":1e-8,\"sender\":\"\",\"ttl\":{TTL},\
         \"creationTime\":{secs},\"chainId\":{chain}}},\"payload\":{{\"exec\":{{\"code\":{code},\
         \"data\":{{}}}}}},\"nonce\":\"kjs:nonce:{ms}\",\"signers\":[]}}",
        chain = js_str(chain_id),
        code = js_str(code),
    )
}

/// `JSON.stringify({cmd, hash, sigs})` with `hash` computed.
fn transaction(cmd: &str, sigs: &[String]) -> String {
    let sigs: Vec<String> = sigs
        .iter()
        .map(|s| format!("{{\"sig\":{}}}", js_str(s)))
        .collect();
    format!(
        "{{\"cmd\":{},\"hash\":{},\"sigs\":[{}]}}",
        js_str(cmd),
        js_str(&blake_b64(cmd)),
        sigs.join(",")
    )
}

/// A `pactQuery` item's answer from the node's `/local` body.
fn query_result(v: &Value) -> (&'static str, Option<String>, Option<String>) {
    let r = v.get("result");
    match r.and_then(|r| r.get("status")).and_then(Value::as_str) {
        Some("success") => ("success", r.and_then(|r| r.get("data")).map(js_json), None),
        Some("failure") => {
            let e = r.and_then(|r| r.get("error"));
            let msg = e.and_then(|e| e.get("message")).and_then(Value::as_str);
            let typed = e.and_then(|e| e.get("type")).is_some_and(Value::is_string);
            match msg {
                Some(m) if typed => ("error", None, Some(m.to_owned())),
                _ => ("error", None, Some(NODE_FAILURE.into())),
            }
        }
        _ => ("error", None, Some(NODE_FAILURE.into())),
    }
}

/// `JSON.stringify` of `v` as JavaScript's `JSON.parse` would have decoded it: numbers as
/// doubles in `Number#toString` form, array-index keys first in ascending order, the other
/// keys in insertion order (`serde_json`'s `preserve_order` keeps it, and keeps a repeated
/// key at its first place with its last value, as JavaScript does).
pub fn js_json(v: &Value) -> String {
    let mut out = String::new();
    write_js(v, &mut out);
    out
}

fn write_js(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&n.as_f64().map_or_else(|| "null".into(), js_number)),
        Value::String(s) => out.push_str(&js_str(s)),
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_js(x, out);
            }
            out.push(']');
        }
        Value::Object(o) => {
            let mut idx: Vec<(u32, &String, &Value)> = Vec::new();
            let mut rest: Vec<(&String, &Value)> = Vec::new();
            for (k, x) in o {
                match array_index(k) {
                    Some(i) => idx.push((i, k, x)),
                    None => rest.push((k, x)),
                }
            }
            idx.sort_by_key(|(i, _, _)| *i);
            out.push('{');
            let all = idx.into_iter().map(|(_, k, x)| (k, x)).chain(rest);
            for (i, (k, x)) in all.enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&js_str(k));
                out.push(':');
                write_js(x, out);
            }
            out.push('}');
        }
    }
}

/// A JavaScript array index: canonical decimal below 2^32 − 1.
fn array_index(k: &str) -> Option<u32> {
    let n: u32 = k.parse().ok()?;
    (n != u32::MAX && n.to_string() == k).then_some(n)
}

/// JavaScript's `Number#toString` (ECMA-262 Number::toString, radix 10). Rust's shortest
/// round-trip digits (`{:e}`) are the same digits JavaScript picks.
pub fn js_number(f: f64) -> String {
    if !f.is_finite() {
        return "null".into();
    }
    if f == 0.0 {
        return "0".into();
    }
    let e = format!("{:e}", f.abs());
    let Some((mant, exp)) = e.split_once('e') else {
        return f.to_string();
    };
    let Ok(exp) = exp.parse::<i32>() else {
        return f.to_string();
    };
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    let k = digits.len() as i32;
    let n = exp + 1;
    let body = if k <= n && n <= 21 {
        format!("{digits}{}", "0".repeat((n - k) as usize))
    } else if 0 < n && n <= 21 {
        format!("{}.{}", &digits[..n as usize], &digits[n as usize..])
    } else if -6 < n && n <= 0 {
        format!("0.{}{digits}", "0".repeat((-n) as usize))
    } else {
        let e = n - 1;
        let sign = if e >= 0 { '+' } else { '-' };
        if k == 1 {
            format!("{digits}e{sign}{}", e.abs())
        } else {
            format!("{}.{}e{sign}{}", &digits[..1], &digits[1..], e.abs())
        }
    };
    if f < 0.0 {
        format!("-{body}")
    } else {
        body
    }
}

/// Any JSON value, refused when an object anywhere in it repeats a key (decoded keys
/// compared, so an escaped spelling is the same key). Depth is serde_json's limit.
struct NoDupKeys;

impl<'de> serde::Deserialize<'de> for NoDupKeys {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(NoDupVisitor)
    }
}

struct NoDupVisitor;

impl<'de> serde::de::Visitor<'de> for NoDupVisitor {
    type Value = NoDupKeys;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JSON without duplicate keys")
    }
    fn visit_bool<E>(self, _: bool) -> Result<NoDupKeys, E> {
        Ok(NoDupKeys)
    }
    fn visit_i64<E>(self, _: i64) -> Result<NoDupKeys, E> {
        Ok(NoDupKeys)
    }
    fn visit_u64<E>(self, _: u64) -> Result<NoDupKeys, E> {
        Ok(NoDupKeys)
    }
    fn visit_f64<E>(self, _: f64) -> Result<NoDupKeys, E> {
        Ok(NoDupKeys)
    }
    fn visit_str<E>(self, _: &str) -> Result<NoDupKeys, E> {
        Ok(NoDupKeys)
    }
    fn visit_unit<E>(self) -> Result<NoDupKeys, E> {
        Ok(NoDupKeys)
    }
    fn visit_none<E>(self) -> Result<NoDupKeys, E> {
        Ok(NoDupKeys)
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut a: A) -> Result<NoDupKeys, A::Error> {
        while a.next_element::<NoDupKeys>()?.is_some() {}
        Ok(NoDupKeys)
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut a: A) -> Result<NoDupKeys, A::Error> {
        let mut seen = std::collections::HashSet::new();
        while let Some(k) = a.next_key::<String>()? {
            if !seen.insert(k) {
                return Err(serde::de::Error::custom("duplicate key"));
            }
            a.next_value::<NoDupKeys>()?;
        }
        Ok(NoDupKeys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_render_as_javascript_does() {
        for (f, s) in [
            (3.0, "3"),
            (3.75, "3.75"),
            (0.1, "0.1"),
            (1e-8, "1e-8"),
            (1e-7, "1e-7"),
            (0.000001, "0.000001"),
            (123.456, "123.456"),
            (1e21, "1e+21"),
            (1e20, "100000000000000000000"),
            (899999999999999999991.0, "900000000000000000000"),
            (-2.5, "-2.5"),
            (-0.0, "0"),
            (2245.8633723, "2245.8633723"),
            (1.5e300, "1.5e+300"),
            (5e-324, "5e-324"),
        ] {
            assert_eq!(js_number(f), s, "{f:e}");
        }
    }

    #[test]
    fn objects_order_index_keys_first_and_repeat_keys_in_place() {
        let v: Value =
            serde_json::from_str(r#"{"b":1.0,"10":2,"a":[1.50,true,null],"2":"x","b":7,"01":0}"#)
                .unwrap();
        assert_eq!(
            js_json(&v),
            r#"{"2":"x","10":2,"b":7,"a":[1.5,true,null],"01":0}"#
        );
        let v: Value = serde_json::from_str(r#"{"s":"é\u0001\n\"\\/"}"#).unwrap();
        assert_eq!(js_json(&v), "{\"s\":\"\u{e9}\\u0001\\n\\\"\\\\/\"}");
        let v: Value = serde_json::from_str("1e400").unwrap();
        assert_eq!(js_json(&v), "null");
    }

    #[test]
    fn hash_is_blake2b_base64url() {
        // Pact's hash of the empty string.
        assert_eq!(blake_b64(""), "DldRwCblQ7Loqy6wYJnaodHl30d3j3eH-qtFzfEv46g");
    }

    #[test]
    fn gas_schema_is_stocks() {
        assert!(parse_gas_input("nope").is_none());
        assert!(parse_gas_input("[]").is_none());
        assert!(parse_gas_input("null").is_none());
        assert!(parse_gas_input(r#"{"payload":{"exec":{}}}"#).is_none());
        assert!(parse_gas_input(r#"{"code":null}"#).is_none());
        assert!(parse_gas_input(r#"{"sigs":[1]}"#).is_none());
        let o =
            parse_gas_input(r#"{"code":"(+ 1 2)","chainId":"0","networkId":"x","z":1}"#).unwrap();
        assert_eq!(o.keys().collect::<Vec<_>>(), ["code", "chainId"]);
    }
}
