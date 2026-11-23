use std::collections::{BTreeMap, HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream::Stream;
use serde_json::{json, Map, Value};
use tokio::sync::Semaphore;
use url::Url;

use crate::graph::ChainGraph;
use crate::limit::RateLimiter;
use crate::sse::header_events;
use crate::tls::pinned_client_config;
use crate::types::{Cut, CutEntry, NodeError, NodeInfo, Page, Result, StatRow};

pub const USER_AGENT: &str = "kda-indexer/0.1";
const HEADER_ACCEPT: &str = "application/json;blockheader-encoding=object";
const DEFAULT_BATCH_LIMIT: u32 = 50;
/// Most pages one paginated header walk follows (F-36).
const MAX_PAGES: usize = 10_000;
/// Most headers `header_branch_since` accepts; it has no lower height to derive a bound from.
const MAX_SINCE_HEADERS: u64 = 1_000_000;
/// Most bytes of a `header/updates` error body read (F-12).
const MAX_ERROR_BODY: usize = 4096;

#[derive(Debug, Clone)]
pub struct NodeClientConfig {
    /// Service API base, e.g. `http://127.0.0.1:31351`.
    pub base: Url,
    /// Network id, e.g. `mainnet01`.
    pub network: String,
    pub timeout: Duration,
    /// 0 disables rate limiting.
    pub max_rps: u32,
    pub concurrency: usize,
    pub p2p_url: Option<Url>,
    pub p2p_cert_sha256: Option<[u8; 32]>,
    /// Largest accepted (decompressed) response body.
    pub max_body_bytes: usize,
    /// Largest accepted server-sent event.
    pub max_sse_event_bytes: usize,
}

struct Route {
    base: Url,
    /// The request timeout; `stream_http` applies it to connecting and to the response
    /// headers only.
    timeout: Duration,
    http: reqwest::Client,
    /// Same TLS settings but no overall timeout, for SSE.
    stream_http: reqwest::Client,
}

struct Shared {
    network: String,
    limiter: RateLimiter,
    sem: Semaphore,
    stats: Mutex<HashMap<(String, u16), (u64, f64)>>,
    batch_limit: AtomicU32,
    max_body_bytes: usize,
    max_sse_event_bytes: usize,
}

#[derive(Clone)]
pub struct NodeClient {
    route: Arc<Route>,
    shared: Arc<Shared>,
    p2p: Option<Arc<Route>>,
}

fn build_http(
    timeout: Option<Duration>,
    connect_timeout: Duration,
    tls: Option<rustls::ClientConfig>,
) -> Result<reqwest::Client> {
    let mut b = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .gzip(true)
        .connect_timeout(connect_timeout);
    if let Some(t) = timeout {
        b = b.timeout(t);
    }
    if let Some(cfg) = tls {
        b = b.use_preconfigured_tls(cfg);
    }
    b.build().map_err(|e| NodeError::Config(e.to_string()))
}

fn build_route(base: Url, timeout: Duration, tls: Option<rustls::ClientConfig>) -> Result<Route> {
    let ct = timeout.min(Duration::from_secs(10));
    Ok(Route {
        base,
        timeout,
        http: build_http(Some(timeout), ct, tls.clone())?,
        stream_http: build_http(None, ct, tls)?,
    })
}

fn body_prefix(body: &[u8]) -> String {
    String::from_utf8_lossy(&body[..body.len().min(300)]).into_owned()
}

/// Read a (decompressed) body chunk by chunk, failing once it exceeds `limit` bytes.
async fn read_capped(
    mut resp: reqwest::Response,
    limit: usize,
) -> std::result::Result<Bytes, ReadError> {
    if resp.content_length().is_some_and(|n| n > limit as u64) {
        return Err(ReadError::TooLarge);
    }
    let mut buf = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(ReadError::Http)? {
        if buf.len() + chunk.len() > limit {
            return Err(ReadError::TooLarge);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(buf))
}

enum ReadError {
    TooLarge,
    Http(reqwest::Error),
}

fn decode<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T> {
    serde_json::from_slice(body)
        .map_err(|e| NodeError::Decode(format!("{e}; body starts: {}", body_prefix(body))))
}

/// F-10: a payload hash goes into a URL path, so it may only hold unpadded base64url
/// characters, 1 to 43 of them (a real hash is 43; nothing here can leave the path segment).
/// The ingest checks the full `Hash32` form before it calls the client.
fn check_payload_hash(h: &str) -> Result<()> {
    if (1..=43).contains(&h.len())
        && h.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        Ok(())
    } else {
        Err(NodeError::Decode(format!(
            "invalid payload hash {:?}",
            body_prefix(h.as_bytes())
        )))
    }
}

fn parse_page(v: Value) -> Result<Page<Value>> {
    let mut m = match v {
        Value::Object(m) => m,
        other => {
            return Err(NodeError::Decode(format!(
                "page is not an object: {}",
                body_prefix(other.to_string().as_bytes())
            )))
        }
    };
    let items = match m.remove("items") {
        Some(Value::Array(a)) => a,
        _ => return Err(NodeError::Decode("page has no items array".into())),
    };
    let limit = m.get("limit").and_then(Value::as_u64).unwrap_or(0) as u32;
    let next = m.get("next").and_then(Value::as_str).map(str::to_owned);
    Ok(Page { items, limit, next })
}

impl NodeClient {
    pub fn new(cfg: &NodeClientConfig) -> Result<NodeClient> {
        let route = build_route(cfg.base.clone(), cfg.timeout, None)?;
        let p2p = match (&cfg.p2p_url, &cfg.p2p_cert_sha256) {
            (Some(url), Some(pin)) => Some(Arc::new(build_route(
                url.clone(),
                cfg.timeout,
                Some(pinned_client_config(*pin)?),
            )?)),
            (Some(_), None) => {
                return Err(NodeError::Config(
                    "p2p_url is set but p2p_cert_sha256 is not".into(),
                ))
            }
            _ => None,
        };
        Ok(NodeClient {
            route: Arc::new(route),
            shared: Arc::new(Shared {
                network: cfg.network.clone(),
                limiter: RateLimiter::new(cfg.max_rps),
                sem: Semaphore::new(cfg.concurrency.max(1)),
                stats: Mutex::new(HashMap::new()),
                batch_limit: AtomicU32::new(DEFAULT_BATCH_LIMIT),
                max_body_bytes: cfg.max_body_bytes.max(1),
                max_sse_event_bytes: cfg.max_sse_event_bytes.max(1),
            }),
            p2p,
        })
    }

    /// A sibling client that talks to the pinned P2P endpoint. Shares the rate limit,
    /// concurrency, stats and batch limit with `self`. `None` when no P2P endpoint is configured.
    pub fn p2p_client(&self) -> Option<NodeClient> {
        self.p2p.as_ref().map(|r| NodeClient {
            route: r.clone(),
            shared: self.shared.clone(),
            p2p: None,
        })
    }

    pub fn base(&self) -> &Url {
        &self.route.base
    }

    pub fn network(&self) -> &str {
        &self.shared.network
    }

    pub fn batch_limit(&self) -> u32 {
        self.shared.batch_limit.load(Ordering::Relaxed)
    }

    pub fn set_batch_limit(&self, n: u32) {
        self.shared.batch_limit.store(n.max(1), Ordering::Relaxed);
    }

    /// Request counters per endpoint and status.
    pub fn stats(&self) -> Vec<StatRow> {
        let g = self.shared.stats.lock().unwrap();
        let mut v: Vec<StatRow> = g
            .iter()
            .map(|((e, s), (c, t))| StatRow {
                endpoint: e.clone(),
                status: *s,
                count: *c,
                total_secs: *t,
            })
            .collect();
        v.sort_by(|a, b| (&a.endpoint, a.status).cmp(&(&b.endpoint, b.status)));
        v
    }

    fn record(&self, endpoint: &str, status: u16, secs: f64) {
        kdx_metrics::get().node_request(endpoint, status, secs);
        let mut g = self.shared.stats.lock().unwrap();
        let e = g.entry((endpoint.to_string(), status)).or_insert((0, 0.0));
        e.0 += 1;
        e.1 += secs;
    }

    /// F-10: an error, not a panic, when `path` does not join.
    fn root_url(&self, path: &str) -> Result<Url> {
        self.route
            .base
            .join(path)
            .map_err(|e| NodeError::Config(format!("url path {path:?}: {e}")))
    }

    fn net_url(&self, path: &str) -> Result<Url> {
        self.root_url(&format!("chainweb/0.0/{}/{}", self.shared.network, path))
    }

    fn chain_url(&self, chain: u8, path: &str) -> Result<Url> {
        self.net_url(&format!("chain/{chain}/{path}"))
    }

    /// Send with rate limit, concurrency cap and retries (3 attempts, 0.5/1/2 s backoff on
    /// timeout, connect error and 5xx; never on 4xx). Returns status and body for any HTTP status.
    async fn send(
        &self,
        endpoint: &'static str,
        build: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Result<(u16, Bytes)> {
        self.send_tries(endpoint, 3, build).await
    }

    /// [`Self::send`] with at most `tries` attempts (1: no retry).
    async fn send_tries(
        &self,
        endpoint: &'static str,
        tries: u32,
        build: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Result<(u16, Bytes)> {
        let mut last: Option<NodeError> = None;
        for attempt in 0..tries.max(1) {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(500 << (attempt - 1))).await;
            }
            let _permit = self
                .shared
                .sem
                .acquire()
                .await
                .map_err(|_| NodeError::Transport("semaphore closed".into()))?;
            self.shared.limiter.acquire().await;
            let t0 = Instant::now();
            let res = build(&self.route.http).send().await;
            let resp = match res {
                Ok(r) => r,
                Err(e) => {
                    self.record(endpoint, 0, t0.elapsed().as_secs_f64());
                    let err = if e.is_timeout() {
                        NodeError::Timeout
                    } else {
                        NodeError::Transport(e.to_string())
                    };
                    if e.is_timeout() || e.is_connect() || e.is_request() {
                        tracing::debug!(endpoint, attempt, error = %err, "node request failed, retrying");
                        last = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            };
            let status = resp.status().as_u16();
            let body = match read_capped(resp, self.shared.max_body_bytes).await {
                Ok(b) => b,
                Err(ReadError::TooLarge) => {
                    self.record(endpoint, status, t0.elapsed().as_secs_f64());
                    return Err(NodeError::TooLarge {
                        limit: self.shared.max_body_bytes,
                    });
                }
                Err(ReadError::Http(e)) => {
                    self.record(endpoint, 0, t0.elapsed().as_secs_f64());
                    last = Some(if e.is_timeout() {
                        NodeError::Timeout
                    } else {
                        NodeError::Transport(e.to_string())
                    });
                    continue;
                }
            };
            self.record(endpoint, status, t0.elapsed().as_secs_f64());
            if status >= 500 && attempt + 1 < tries {
                last = Some(NodeError::Http {
                    status,
                    body_prefix: body_prefix(&body),
                });
                continue;
            }
            return Ok((status, body));
        }
        Err(last.unwrap_or(NodeError::Timeout))
    }

    /// `send` + 2xx check + JSON decode.
    async fn json(
        &self,
        endpoint: &'static str,
        build: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Result<Value> {
        self.json_tries(endpoint, 3, build).await
    }

    /// [`Self::json`] with at most `tries` attempts.
    async fn json_tries(
        &self,
        endpoint: &'static str,
        tries: u32,
        build: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Result<Value> {
        let (status, body) = self.send_tries(endpoint, tries, build).await?;
        if !(200..300).contains(&status) {
            return Err(NodeError::Http {
                status,
                body_prefix: body_prefix(&body),
            });
        }
        decode(&body)
    }

    /// `GET /info` (root path, not under the network prefix).
    pub async fn info(&self) -> Result<NodeInfo> {
        let url = self.root_url("info")?;
        let raw = self.json("info", |h| h.get(url.clone())).await?;
        let s = |k: &str| raw.get(k).and_then(Value::as_str).map(str::to_owned);
        let chainweb_version =
            s("nodeVersion").ok_or_else(|| NodeError::Decode("info: no nodeVersion".into()))?;
        let node_version = s("nodePackageVersion").unwrap_or_default();
        // [[startHeight, [[chain, [adjacents]], ...]], ...] (the node lists newest first)
        let hist = raw
            .get("nodeGraphHistory")
            .or_else(|| raw.get("nodeHistoricalChains"))
            .and_then(Value::as_array)
            .ok_or_else(|| NodeError::Decode("info: no nodeGraphHistory".into()))?;
        let mut graph_history = Vec::new();
        for entry in hist {
            let (h, chains) = entry
                .as_array()
                .and_then(|a| Some((a.first()?.as_u64()?, a.get(1)?.as_array()?)))
                .ok_or_else(|| NodeError::Decode("info: bad graph history entry".into()))?;
            let mut ids: Vec<u8> = chains
                .iter()
                .filter_map(|c| c.as_array()?.first()?.as_u64().map(|n| n as u8))
                .collect();
            ids.sort_unstable();
            graph_history.push((h, ids));
        }
        graph_history.sort_by_key(|(h, _)| *h);
        Ok(NodeInfo {
            node_version,
            chainweb_version,
            graph_history,
            raw,
        })
    }

    pub async fn chain_graph(&self) -> Result<ChainGraph> {
        Ok(ChainGraph::from_info(&self.info().await?))
    }

    /// `GET /cut`.
    pub async fn cut(&self) -> Result<Cut> {
        let url = self.net_url("cut")?;
        let v = self.json("cut", |h| h.get(url.clone())).await?;
        let hashes_v = v
            .get("hashes")
            .and_then(Value::as_object)
            .ok_or_else(|| NodeError::Decode("cut: no hashes".into()))?;
        let mut hashes = BTreeMap::new();
        for (k, e) in hashes_v {
            let chain: u8 = k
                .parse()
                .map_err(|_| NodeError::Decode(format!("cut: bad chain id {k}")))?;
            let height = e
                .get("height")
                .and_then(Value::as_u64)
                .ok_or_else(|| NodeError::Decode("cut: bad height".into()))?;
            let hash = e
                .get("hash")
                .and_then(Value::as_str)
                .ok_or_else(|| NodeError::Decode("cut: bad hash".into()))?
                .to_string();
            hashes.insert(chain, CutEntry { height, hash });
        }
        let height = v.get("height").and_then(Value::as_u64).unwrap_or(0);
        let weight = v
            .get("weight")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        Ok(Cut {
            hashes,
            height,
            weight,
        })
    }

    /// `GET /chain/{c}/header?minheight&maxheight&limit&next` (object encoding).
    pub async fn headers(
        &self,
        chain: u8,
        min: u64,
        max: u64,
        limit: Option<u32>,
        next: Option<&str>,
    ) -> Result<Page<Value>> {
        let url = self.chain_url(chain, "header")?;
        let mut q: Vec<(&str, String)> = vec![
            ("minheight", min.to_string()),
            ("maxheight", max.to_string()),
        ];
        if let Some(l) = limit {
            q.push(("limit", l.to_string()));
        }
        if let Some(n) = next {
            q.push(("next", n.to_string()));
        }
        let v = self
            .json("header", |h| {
                h.get(url.clone()).query(&q).header("Accept", HEADER_ACCEPT)
            })
            .await?;
        parse_page(v)
    }

    /// All headers in `min..=max`, following `next` tokens.
    ///
    /// **Includes orphans**: the plain header range returns every block the node knows at
    /// those heights, canonical or not. Probe use only; height walks of the chain must use
    /// [`NodeClient::header_branch_all`].
    pub async fn headers_all(&self, chain: u8, min: u64, max: u64) -> Result<Vec<Value>> {
        // Orphans included: up to two headers per height.
        let cap = span(min, max).saturating_mul(2);
        paginate("header", cap, |next| async move {
            self.headers(chain, min, max, None, next.as_deref()).await
        })
        .await
    }

    /// `POST /chain/{c}/header/branch` (one page).
    pub async fn header_branch(
        &self,
        chain: u8,
        lower: &[String],
        upper: &[String],
        min: Option<u64>,
        max: Option<u64>,
    ) -> Result<Page<Value>> {
        self.header_branch_page(chain, lower, upper, min, max, None)
            .await
    }

    /// The canonical headers of `chain` at `min..=max`: the branch ending in `upper` (a block
    /// hash, e.g. the cut's), following `next` tokens. Errors unless there is exactly one
    /// header per height, sorted ascending.
    pub async fn header_branch_all(
        &self,
        chain: u8,
        upper: &str,
        min: u64,
        max: u64,
    ) -> Result<Vec<Value>> {
        let upper = [upper.to_owned()];
        let upper = &upper;
        let mut out = paginate("header/branch", span(min, max), |next| async move {
            self.header_branch_page(chain, &[], upper, Some(min), Some(max), next.as_deref())
                .await
        })
        .await?;
        out.sort_by_key(|h| h.get("height").and_then(Value::as_u64).unwrap_or(u64::MAX));
        let heights: Vec<Option<u64>> = out
            .iter()
            .map(|h| h.get("height").and_then(Value::as_u64))
            .collect();
        let want: Vec<Option<u64>> = (min..=max).map(Some).collect();
        if heights != want {
            return Err(NodeError::Decode(format!(
                "header/branch chain {chain} {min}..={max}: got {} headers, expected one per height",
                out.len()
            )));
        }
        Ok(out)
    }

    /// Headers on the branch ending in `upper` that are not ancestors of any `lower` hash, up
    /// to `max` height, following `next` tokens, sorted ascending by height. With `lower` =
    /// a stored tip this is the new branch above the fork point (tail follow, spec §4.3).
    pub async fn header_branch_since(
        &self,
        chain: u8,
        lower: &[String],
        upper: &str,
        max: Option<u64>,
    ) -> Result<Vec<Value>> {
        let upper = [upper.to_owned()];
        let upper = &upper;
        let mut out = paginate("header/branch", MAX_SINCE_HEADERS, |next| async move {
            self.header_branch_page(chain, lower, upper, None, max, next.as_deref())
                .await
        })
        .await?;
        out.sort_by_key(|h| h.get("height").and_then(Value::as_u64).unwrap_or(u64::MAX));
        Ok(out)
    }

    async fn header_branch_page(
        &self,
        chain: u8,
        lower: &[String],
        upper: &[String],
        min: Option<u64>,
        max: Option<u64>,
        next: Option<&str>,
    ) -> Result<Page<Value>> {
        let url = self.chain_url(chain, "header/branch")?;
        let mut q: Vec<(&str, String)> = Vec::new();
        if let Some(m) = min {
            q.push(("minheight", m.to_string()));
        }
        if let Some(m) = max {
            q.push(("maxheight", m.to_string()));
        }
        if let Some(n) = next {
            q.push(("next", n.to_string()));
        }
        let body = json!({ "lower": lower, "upper": upper });
        let v = self
            .json("header_branch", |h| {
                h.post(url.clone())
                    .query(&q)
                    .header("Accept", HEADER_ACCEPT)
                    .json(&body)
            })
            .await?;
        parse_page(v)
    }

    /// One raw `payload/outputs/batch` call (no dedupe, no chunking). Entries can be
    /// fewer than requested; callers map by `payloadHash`.
    pub async fn payload_batch_raw(&self, chain: u8, hashes: &[String]) -> Result<Vec<Value>> {
        let url = self.chain_url(chain, "payload/outputs/batch")?;
        let v = self
            .json("payload_batch", |h| h.post(url.clone()).json(&hashes))
            .await?;
        match v {
            Value::Array(a) => Ok(a),
            _ => Err(NodeError::Decode("payload batch: not an array".into())),
        }
    }

    /// `GET /chain/{c}/payload/{hash}/outputs`.
    pub async fn payload_with_outputs(&self, chain: u8, hash: &str) -> Result<Value> {
        check_payload_hash(hash)?;
        let url = self.chain_url(chain, &format!("payload/{hash}/outputs"))?;
        self.json("payload", |h| h.get(url.clone())).await
    }

    /// Payloads with outputs for `hashes`, keyed by `payloadHash`. Input is deduplicated and
    /// chunked by `batch_limit`; hashes absent from a batch response are refetched singly.
    pub async fn payloads_with_outputs(
        &self,
        chain: u8,
        hashes: &[String],
    ) -> Result<HashMap<String, Value>> {
        for h in hashes {
            check_payload_hash(h)?;
        }
        let mut seen = HashSet::new();
        let uniq: Vec<String> = hashes
            .iter()
            .filter(|h| seen.insert(h.as_str()))
            .cloned()
            .collect();
        let mut out: HashMap<String, Value> = HashMap::with_capacity(uniq.len());
        for chunk in uniq.chunks(self.batch_limit() as usize) {
            for entry in self.payload_batch_raw(chain, chunk).await? {
                if let Some(h) = entry.get("payloadHash").and_then(Value::as_str) {
                    out.entry(h.to_string()).or_insert(entry);
                }
            }
        }
        for h in &uniq {
            if !out.contains_key(h) {
                let v = self.payload_with_outputs(chain, h).await?;
                match v.get("payloadHash").and_then(Value::as_str) {
                    Some(got) if got == h => {
                        out.insert(h.clone(), v);
                    }
                    _ => return Err(NodeError::Missing { hash: h.clone() }),
                }
            }
        }
        Ok(out)
    }

    /// `GET /chain/{c}/block?minheight&maxheight` (header and payload in one call). `None` on 404.
    pub async fn block(&self, chain: u8, min: u64, max: u64) -> Result<Option<Page<Value>>> {
        let url = self.chain_url(chain, "block")?;
        let q = [
            ("minheight", min.to_string()),
            ("maxheight", max.to_string()),
        ];
        // The block route answers 406 to the blockheader-encoding Accept parameter; it already
        // returns object-encoded headers, so ask for plain JSON.
        let (status, body) = self
            .send("block", |h| {
                h.get(url.clone())
                    .query(&q)
                    .header("Accept", "application/json")
            })
            .await?;
        match status {
            404 => Ok(None),
            200..=299 => Ok(Some(parse_page(decode(&body)?)?)),
            _ => Err(NodeError::Http {
                status,
                body_prefix: body_prefix(&body),
            }),
        }
    }

    /// `POST /chain/{c}/pact/api/v1/poll`.
    pub async fn poll(&self, chain: u8, request_keys: &[String]) -> Result<Map<String, Value>> {
        let url = self.chain_url(chain, "pact/api/v1/poll")?;
        let body = json!({ "requestKeys": request_keys });
        match self
            .json("poll", |h| h.post(url.clone()).json(&body))
            .await?
        {
            Value::Object(m) => Ok(m),
            _ => Err(NodeError::Decode("poll: not an object".into())),
        }
    }

    /// `POST /chain/{c}/pact/api/v1/local` with `cmd` passed through unchanged.
    pub async fn local(&self, chain: u8, cmd: &Value) -> Result<Value> {
        let url = self.chain_url(chain, "pact/api/v1/local")?;
        self.json("local", |h| h.post(url.clone()).json(cmd)).await
    }

    /// [`Self::local`] sent once (no retry), with `timeout` for this request in place of the
    /// client's (PR #27 H-1: a heavy read the node cannot answer in time must cost one request,
    /// not three, and the caller decides when to try again).
    pub async fn local_once(&self, chain: u8, cmd: &Value, timeout: Duration) -> Result<Value> {
        let url = self.chain_url(chain, "pact/api/v1/local")?;
        self.json_tries("local", 1, |h| {
            h.post(url.clone()).json(cmd).timeout(timeout)
        })
        .await
    }

    /// `GET /header/updates` (SSE). Yields the JSON payload of each `BlockHeader` event and
    /// ends when the connection drops; the caller reconnects.
    pub async fn header_updates(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Value>> + Send>>> {
        let url = self.net_url("header/updates")?;
        let _p = self
            .shared
            .sem
            .acquire()
            .await
            .map_err(|_| NodeError::Transport("semaphore closed".into()))?;
        self.shared.limiter.acquire().await;
        let t0 = Instant::now();
        // F-12: the stream client has no overall timeout; connecting and the response
        // headers get the request timeout.
        let send = self
            .route
            .stream_http
            .get(url)
            .header("Accept", "text/event-stream")
            .send();
        let resp = match tokio::time::timeout(self.route.timeout, send).await {
            Ok(r) => r.map_err(|e| {
                if e.is_timeout() {
                    NodeError::Timeout
                } else {
                    NodeError::Transport(e.to_string())
                }
            })?,
            Err(_) => {
                self.record("header_updates", 0, t0.elapsed().as_secs_f64());
                return Err(NodeError::Timeout);
            }
        };
        let status = resp.status().as_u16();
        self.record("header_updates", status, t0.elapsed().as_secs_f64());
        if !resp.status().is_success() {
            // Capped and timed: the semaphore permit is still held.
            let b =
                match tokio::time::timeout(self.route.timeout, read_capped(resp, MAX_ERROR_BODY))
                    .await
                {
                    Ok(Ok(b)) => b,
                    _ => Bytes::new(),
                };
            return Err(NodeError::Http {
                status,
                body_prefix: body_prefix(&b),
            });
        }
        Ok(header_events(
            Box::pin(resp.bytes_stream()),
            self.shared.max_sse_event_bytes,
        ))
    }
}

/// Heights in `min..=max`.
fn span(min: u64, max: u64) -> u64 {
    max.saturating_sub(min).saturating_add(1)
}

/// Follow `next` tokens (F-36): at most [`MAX_PAGES`] pages and `max_items` items; a repeated
/// token or an empty page that still has a `next` token is an error.
async fn paginate<F, Fut>(what: &str, max_items: u64, mut page: F) -> Result<Vec<Value>>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<Page<Value>>>,
{
    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut next: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let p = page(next.take()).await?;
        let empty = p.items.is_empty();
        out.extend(p.items);
        if out.len() as u64 > max_items {
            return Err(NodeError::Decode(format!(
                "{what}: more than {max_items} headers"
            )));
        }
        let Some(n) = p.next else {
            return Ok(out);
        };
        if empty {
            return Err(NodeError::Decode(format!(
                "{what}: empty page with a next token"
            )));
        }
        if !seen.insert(n.clone()) {
            return Err(NodeError::Decode(format!("{what}: next token repeats")));
        }
        next = Some(n);
    }
    Err(NodeError::Decode(format!(
        "{what}: more than {MAX_PAGES} pages"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::atomic::AtomicUsize;

    /// What the mock node does with one request.
    enum Reply {
        Json(String),
        /// Raw bytes, then the connection stays open and silent.
        RawThenHang(Vec<u8>),
        /// Read the request, never answer.
        Hang,
        /// Answer after a pause.
        Slow(Duration, String),
    }

    /// One-request-per-connection HTTP mock; `f` gets the request line's target
    /// (path and query) and the request count so far.
    fn mock(f: impl Fn(&str, usize) -> Reply + Send + Sync + 'static) -> (Url, Arc<AtomicUsize>) {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let n = Arc::new(AtomicUsize::new(0));
        let (f, n2) = (Arc::new(f), n.clone());
        std::thread::spawn(move || {
            for s in l.incoming() {
                let Ok(mut s) = s else { return };
                let (f, n) = (f.clone(), n2.clone());
                std::thread::spawn(move || {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    let head = loop {
                        let k = s.read(&mut tmp).unwrap_or(0);
                        if k == 0 {
                            return;
                        }
                        buf.extend_from_slice(&tmp[..k]);
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break String::from_utf8_lossy(&buf[..i]).into_owned();
                        }
                    };
                    let target = head.split(' ').nth(1).unwrap_or("").to_owned();
                    let i = n.fetch_add(1, Ordering::SeqCst);
                    match f(&target, i) {
                        Reply::Json(b) => {
                            let _ = write!(
                                s,
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
                                b.len()
                            );
                        }
                        Reply::RawThenHang(b) => {
                            let _ = s.write_all(&b);
                            std::thread::sleep(Duration::from_secs(30));
                        }
                        Reply::Hang => std::thread::sleep(Duration::from_secs(30)),
                        Reply::Slow(d, b) => {
                            std::thread::sleep(d);
                            let _ = write!(
                                s,
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
                                b.len()
                            );
                        }
                    }
                });
            }
        });
        (format!("http://{addr}/").parse().unwrap(), n)
    }

    fn client(base: Url) -> NodeClient {
        NodeClient::new(&NodeClientConfig {
            base,
            network: "mainnet01".into(),
            timeout: Duration::from_millis(300),
            max_rps: 0,
            concurrency: 4,
            p2p_url: None,
            p2p_cert_sha256: None,
            max_body_bytes: 1 << 20,
            max_sse_event_bytes: 1 << 16,
        })
        .unwrap()
    }

    fn hdr(height: u64) -> Value {
        json!({ "height": height, "hash": format!("h{height}") })
    }

    fn page(items: Vec<Value>, next: Option<&str>) -> Reply {
        Reply::Json(json!({ "items": items, "limit": items.len(), "next": next }).to_string())
    }

    async fn within<T>(f: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(10), f)
            .await
            .expect("call did not finish in 10 s")
    }

    /// F-36: an A→B→A token cycle ends with a decode error in all three walkers.
    #[tokio::test]
    async fn pagination_token_cycle_is_an_error() {
        let (base, _) = mock(|_, i| {
            let tok = if i % 2 == 0 { "A" } else { "B" };
            page(vec![hdr(i as u64 % 3)], Some(tok))
        });
        let c = client(base);
        assert!(matches!(
            within(c.headers_all(0, 0, 1_000)).await,
            Err(NodeError::Decode(_))
        ));
        assert!(matches!(
            within(c.header_branch_all(0, "u", 0, 1_000)).await,
            Err(NodeError::Decode(_))
        ));
        assert!(matches!(
            within(c.header_branch_since(0, &[], "u", None)).await,
            Err(NodeError::Decode(_))
        ));
    }

    /// F-36: an endless stream of distinct tokens stops at the item bound of the range, and
    /// an empty page that still has a `next` token is an error.
    #[tokio::test]
    async fn pagination_is_bounded() {
        let (base, n) = mock(|_, i| page(vec![hdr(i as u64)], Some(&format!("t{i}"))));
        let c = client(base);
        assert!(matches!(
            within(c.headers_all(0, 0, 9)).await,
            Err(NodeError::Decode(_))
        ));
        // The plain header range includes orphans: the bound is 2 × the heights asked for.
        assert!(n.load(Ordering::SeqCst) <= 22, "{n:?} requests");
        let (base, _) = mock(|_, i| page(Vec::new(), Some(&format!("t{i}"))));
        let c = client(base);
        assert!(matches!(
            within(c.header_branch_since(0, &[], "u", None)).await,
            Err(NodeError::Decode(_))
        ));
    }

    /// F-10: a payload hash that is not 32 bytes of unpadded base64url never reaches a URL.
    #[tokio::test]
    async fn payload_hash_is_checked_before_the_url() {
        let (base, n) = mock(|_, _| Reply::Json("{}".into()));
        let c = client(base);
        for bad in ["../../info", "a?b", "", "a%2Fb", &"A".repeat(44)] {
            assert!(
                matches!(
                    within(c.payload_with_outputs(0, bad)).await,
                    Err(NodeError::Decode(_))
                ),
                "{bad}"
            );
            assert!(within(c.payloads_with_outputs(0, &[bad.to_string()]))
                .await
                .is_err());
        }
        assert_eq!(n.load(Ordering::SeqCst), 0, "no request may be sent");
    }

    /// F-12: a node that accepts the SSE connection but sends no headers times out.
    #[tokio::test]
    async fn header_updates_connect_times_out() {
        let (base, _) = mock(|_, _| Reply::Hang);
        let c = client(base);
        assert!(matches!(
            within(c.header_updates()).await,
            Err(NodeError::Timeout)
        ));
    }

    /// F-12: an error answer's body is read capped and under the timeout.
    #[tokio::test]
    async fn header_updates_error_body_is_capped() {
        let (base, _) = mock(|_, _| {
            let mut b =
                b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 100000000\r\n\r\n".to_vec();
            b.extend_from_slice(&[b'x'; 8192]);
            Reply::RawThenHang(b)
        });
        let c = client(base);
        assert!(matches!(
            within(c.header_updates()).await,
            Err(NodeError::Http { status: 500, .. })
        ));
    }

    /// PR #27 H-1: the key listing's `/local` read is sent once, with its own timeout: a node
    /// that never answers costs one request and that timeout (the generic `local` retries a
    /// timeout three times), and a slow answer inside it is taken even past the client's
    /// default timeout (300 ms here).
    #[tokio::test]
    async fn local_once_sends_once_with_its_own_timeout() {
        let (base, n) = mock(|_, _| Reply::Hang);
        let c = client(base);
        let t0 = Instant::now();
        let r = within(c.local_once(0, &json!({}), Duration::from_millis(200))).await;
        assert!(matches!(r, Err(NodeError::Timeout)), "{r:?}");
        assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
        assert_eq!(n.load(Ordering::SeqCst), 1);
        let (base, n) = mock(|_, _| Reply::Slow(Duration::from_millis(600), "{\"a\":1}".into()));
        let c = client(base);
        let v = within(c.local_once(0, &json!({}), Duration::from_secs(5)))
            .await
            .unwrap();
        assert_eq!(v, json!({"a": 1}));
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }
}
