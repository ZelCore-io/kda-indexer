//! Documentation types of the compat API: the JSON records, the parameter sets shared by
//! several routes and the failure answers every route can give. Nothing here is used at
//! runtime; the handlers build their JSON by hand (aeson-compatible text, see `shapes`)
//! and these types only feed the OpenAPI document (`crate::openapi::CompatApiDoc`).

#![allow(dead_code)]

use serde_json::Value;
use utoipa::{IntoParams, IntoResponses, ToSchema};

/// `Chainweb-Execution-Strategy`: accepted and ignored.
#[derive(ToSchema)]
pub enum ExecutionStrategy {
    Bounded,
    Unbounded,
}

/// Paging parameters of `/txs/account`, `/txs/events` and `/txs/search?search=`
/// (chainweb-data's `limit`, `offset`, `next`, `minheight`, `maxheight`).
#[derive(IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PagingParams {
    /// Page size, an integer (servant's `Integer`: a sign then digits). Default 10; a value
    /// above 250 is capped to 250. A negative value answers `500 Something went wrong`
    /// (chainweb-data passes it to SQL `LIMIT`). Anything else that is not an integer is
    /// `400 Error parsing query parameter limit failed: could not parse: ...`.
    #[param(default = 10, example = 10)]
    limit: Option<i64>,
    /// Rows to skip, default 0, at most 10,000 (more answers `400`; page further with
    /// `next`). A negative value counts as 0. Not allowed together with `next`.
    #[param(default = 0, maximum = 10000, example = 0)]
    offset: Option<i64>,
    /// The `Chainweb-Next` token of the previous page. Tokens are kda-indexer cursors;
    /// treat them as opaque. A token chainweb-data issued is accepted too and resumes on
    /// the page chainweb-data serves after it (chainweb-data does not accept ours). A token
    /// that does not decode is `400 Invalid next token: ...`; with `offset` it is
    /// `400 next token query parameter not allowed with offset`.
    next: Option<String>,
    /// Lowest block height (inclusive).
    minheight: Option<i64>,
    /// Highest block height (inclusive). A negative value, or `minheight` above
    /// `maxheight`, answers an empty page.
    maxheight: Option<i64>,
}

/// One transaction of `/txs/tx`, `/txs/txs` and `/v1/txs` (chainweb-api `TxDetail`).
#[derive(ToSchema)]
#[schema(rename_all = "camelCase")]
pub struct TxDetail {
    /// Transaction time-to-live in seconds.
    ttl: Option<i64>,
    gas_limit: Option<i64>,
    /// The gas price as a JSON number (aeson `Double`).
    gas_price: Option<f64>,
    nonce: Option<String>,
    /// Defpact id of a continuation.
    pact_id: Option<String>,
    rollback: Option<bool>,
    step: Option<i64>,
    /// The payload data (exec or cont), any JSON.
    data: Value,
    /// SPV proof of a continuation.
    proof: Option<String>,
    gas: Option<i64>,
    /// The result data on success, the error on failure.
    result: Value,
    logs: String,
    metadata: Value,
    continuation: Value,
    /// Pact transaction id (0 when the node gave none).
    txid: i64,
    chain: u8,
    height: u64,
    /// Block creation time, `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` (fraction without trailing zeros).
    block_time: String,
    block_hash: String,
    /// Transaction creation time, whole seconds, `YYYY-MM-DDTHH:MM:SSZ`.
    creation_time: Option<String>,
    request_key: String,
    sender: Option<String>,
    /// Exec code (null for a continuation).
    code: Option<String>,
    success: bool,
    events: Vec<TxEvent>,
    /// Code of the transaction that started the defpact, for a continuation.
    initial_code: Option<String>,
    /// Earlier steps of the defpact, for a continuation.
    previous_steps: Option<Vec<String>>,
    signers: Vec<Signer>,
    sigs: Vec<Sig>,
}

/// An event inside a `TxDetail`.
#[derive(ToSchema)]
pub struct TxEvent {
    /// Qualified name, `module.NAME`.
    name: String,
    params: Vec<Value>,
}

#[derive(ToSchema)]
#[schema(rename_all = "camelCase")]
pub struct Signer {
    addr: Option<String>,
    scheme: Option<String>,
    pub_key: String,
    clist: Vec<Capability>,
}

#[derive(ToSchema)]
pub struct Capability {
    name: String,
    args: Vec<Value>,
}

#[derive(ToSchema)]
pub struct Sig {
    sig: String,
}

/// A transaction row of `/txs/recent` and `/txs/search` (chainweb-api `TxSummary`).
#[derive(ToSchema)]
#[schema(rename_all = "camelCase")]
pub struct TxSummary {
    block_hash: String,
    chain: u8,
    code: Option<String>,
    continuation: Value,
    creation_time: Option<String>,
    height: u64,
    initial_code: Option<String>,
    previous_steps: Option<Vec<String>>,
    request_key: String,
    sender: Option<String>,
    result: TxResult,
}

#[derive(ToSchema)]
pub enum TxResult {
    TxSucceeded,
    TxFailed,
}

/// A transfer row of `/txs/account/{account}` (chainweb-api `TransferDetail`).
#[derive(ToSchema)]
#[schema(rename_all = "camelCase")]
pub struct TransferDetail {
    block_hash: String,
    request_key: String,
    chain: u8,
    height: u64,
    /// Event index, ordered with the other rows of the same transaction.
    idx: i64,
    /// Fungible module, for example `coin`.
    token: String,
    from_account: String,
    to_account: String,
    /// The other side of a cross-chain transfer, from the `TRANSFER_XCHAIN` / `X_RESUME`
    /// events of the same transaction.
    cross_chain_account: Option<String>,
    cross_chain_id: Option<i64>,
    /// Decimal text, shortest exact form (`"138000"`, not `"138000.000000000000"`): equal
    /// as a decimal to chainweb-data's, different as a string.
    amount: String,
    block_time: String,
}

/// An event row of `/txs/events` (chainweb-api `EventDetail`).
#[derive(ToSchema)]
#[schema(rename_all = "camelCase")]
pub struct EventDetail {
    /// Qualified name, `module.NAME`.
    name: String,
    params: Vec<Value>,
    module_hash: String,
    chain: u8,
    height: u64,
    block_time: String,
    block_hash: String,
    request_key: String,
    idx: i64,
}

/// `/stats` (chainweb-data `statsHandler`).
#[derive(ToSchema)]
#[schema(rename_all = "camelCase")]
pub struct Stats {
    /// Exact user-transaction count, as of `X-Count-Age` seconds ago.
    transaction_count: u64,
    /// Circulating KDA; null when the supply table has no value for the height.
    coins_in_circulation: Option<f64>,
}

/// Answers any compat data route can give instead of its own.
#[derive(IntoResponses)]
pub enum LimitErrors {
    /// The per-client request rate (`api.rate_per_ip`, `api.rate_burst`) is used up, or, on a
    /// search route, the client already runs `search.per_client` searches and has 3 more
    /// waiting (a search past `search.per_client` waits its turn, it is not refused). Retry
    /// after the `Retry-After` seconds.
    #[response(
        status = 429,
        content_type = "text/plain",
        headers(("Retry-After" = String, description = "Seconds to wait (1)."))
    )]
    TooManyRequests(String),
    /// `api.max_in_flight` requests are running, the answer would exceed `api.max_body_mb`
    /// (`response larger than N MB: narrow the query`), `search.concurrency` searches are
    /// running, or (on `/stats`) the first count is not ready. On `/txs/events`, `/txs/search`
    /// and `/v1/txs`, which take a per-client heavy-read slot, a client that already runs one
    /// heavy request and has others waiting is refused too (`this client already runs a heavy
    /// request and has others waiting: retry when they are done`; the queue holds
    /// `api.heavy_waiting_per_client` waiting requests, 32 by default). Search refusals, the
    /// in-flight and heavy-read refusals and `/stats` carry `Retry-After`; the body-size
    /// refusal does not.
    #[response(
        status = 503,
        content_type = "text/plain",
        headers(("Retry-After" = String, description = "Seconds to wait; absent on the in-flight and body-size refusals."))
    )]
    Unavailable(String),
    /// A read failed; the cause is logged, the body is `internal error`.
    #[response(status = 500, content_type = "text/plain")]
    Internal(String),
    /// The request ran past `api.deadline_ms` (5 s by default): `request deadline exceeded`.
    /// On a search route, a search still waiting behind the client's earlier ones at the
    /// deadline answers `request deadline exceeded while waiting for this client's earlier
    /// searches`, with `Retry-After: 1`.
    #[response(status = 504, content_type = "text/plain")]
    Deadline(String),
}

/// Answers of `/v1/txs`: its own deadline, slot pool and body cap, so the shared
/// `LimitErrors` text for 503 and 504 does not apply.
#[derive(IntoResponses)]
pub enum V1TxsLimitErrors {
    /// The per-client request rate (`api.rate_per_ip`, `api.rate_burst`) is used up. Retry
    /// after the `Retry-After` seconds.
    #[response(
        status = 429,
        content_type = "text/plain",
        headers(("Retry-After" = String, description = "Seconds to wait (1)."))
    )]
    TooManyRequests(String),
    /// Refused: `api.max_in_flight` requests are running (`too many requests in flight`);
    /// no `/v1/txs` slot freed within `api.deadline_ms` (5 s by default),
    /// `too many /v1/txs requests running: retry later`; the client already runs a heavy
    /// request and has others waiting (`this client already runs a heavy request and has
    /// others waiting: retry when they are done`; up to `api.heavy_waiting_per_client`, 32
    /// by default, may wait); or the answer is larger than `api.v1txs_max_body_mb`
    /// (`response larger than N MB: use a smaller limit`). The in-flight, slot and
    /// heavy-queue refusals carry `Retry-After: 1`; the body-size refusal does not.
    #[response(
        status = 503,
        content_type = "text/plain",
        headers(("Retry-After" = String, description = "Seconds to wait (1); absent on the body-size refusal."))
    )]
    Unavailable(String),
    /// A read failed; the cause is logged, the body is `internal error`.
    #[response(status = 500, content_type = "text/plain")]
    Internal(String),
    /// The read ran past `api.v1txs_deadline_ms` (30 s by default):
    /// `request deadline exceeded`. kda-txs answers 404 at that point.
    #[response(status = 504, content_type = "text/plain")]
    Deadline(String),
}
