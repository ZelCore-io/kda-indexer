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
}

struct Route {
    base: Url,
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
        http: build_http(Some(timeout), ct, tls.clone())?,
        stream_http: build_http(None, ct, tls)?,
    })
}

fn body_prefix(body: &[u8]) -> String {
    String::from_utf8_lossy(&body[..body.len().min(300)]).into_owned()
}

fn decode<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T> {
    serde_json::from_slice(body)
        .map_err(|e| NodeError::Decode(format!("{e}; body starts: {}", body_prefix(body))))
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
        let mut g = self.shared.stats.lock().unwrap();
        let e = g.entry((endpoint.to_string(), status)).or_insert((0, 0.0));
        e.0 += 1;
        e.1 += secs;
    }

    fn root_url(&self, path: &str) -> Url {
        self.route.base.join(path).expect("valid path")
    }

    fn net_url(&self, path: &str) -> Url {
        self.root_url(&format!("chainweb/0.0/{}/{}", self.shared.network, path))
    }

    fn chain_url(&self, chain: u8, path: &str) -> Url {
        self.net_url(&format!("chain/{chain}/{path}"))
    }

    /// Send with rate limit, concurrency cap and retries (3 attempts, 0.5/1/2 s backoff on
    /// timeout, connect error and 5xx; never on 4xx). Returns status and body for any HTTP status.
    async fn send(
        &self,
        endpoint: &'static str,
        build: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Result<(u16, Bytes)> {
        let mut last: Option<NodeError> = None;
        for attempt in 0..3u32 {
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
            let body = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
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
            if status >= 500 && attempt < 2 {
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
        let (status, body) = self.send(endpoint, build).await?;
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
        let url = self.root_url("info");
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
        let url = self.net_url("cut");
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
        let url = self.chain_url(chain, "header");
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
    pub async fn headers_all(&self, chain: u8, min: u64, max: u64) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        let mut next: Option<String> = None;
        loop {
            let page = self.headers(chain, min, max, None, next.as_deref()).await?;
            out.extend(page.items);
            match page.next {
                Some(n) => next = Some(n),
                None => return Ok(out),
            }
        }
    }

    /// `POST /chain/{c}/header/branch`.
    pub async fn header_branch(
        &self,
        chain: u8,
        lower: &[String],
        upper: &[String],
        min: Option<u64>,
        max: Option<u64>,
    ) -> Result<Page<Value>> {
        let url = self.chain_url(chain, "header/branch");
        let mut q: Vec<(&str, String)> = Vec::new();
        if let Some(m) = min {
            q.push(("minheight", m.to_string()));
        }
        if let Some(m) = max {
            q.push(("maxheight", m.to_string()));
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
        let url = self.chain_url(chain, "payload/outputs/batch");
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
        let url = self.chain_url(chain, &format!("payload/{hash}/outputs"));
        self.json("payload", |h| h.get(url.clone())).await
    }

    /// Payloads with outputs for `hashes`, keyed by `payloadHash`. Input is deduplicated and
    /// chunked by `batch_limit`; hashes absent from a batch response are refetched singly.
    pub async fn payloads_with_outputs(
        &self,
        chain: u8,
        hashes: &[String],
    ) -> Result<HashMap<String, Value>> {
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
        let url = self.chain_url(chain, "block");
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
        let url = self.chain_url(chain, "pact/api/v1/poll");
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
        let url = self.chain_url(chain, "pact/api/v1/local");
        self.json("local", |h| h.post(url.clone()).json(cmd)).await
    }

    /// `GET /header/updates` (SSE). Yields the JSON payload of each `BlockHeader` event and
    /// ends when the connection drops; the caller reconnects.
    pub async fn header_updates(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Value>> + Send>>> {
        let url = self.net_url("header/updates");
        let _p = self
            .shared
            .sem
            .acquire()
            .await
            .map_err(|_| NodeError::Transport("semaphore closed".into()))?;
        self.shared.limiter.acquire().await;
        let t0 = Instant::now();
        let resp = self
            .route
            .stream_http
            .get(url)
            .header("Accept", "text/event-stream")
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    NodeError::Timeout
                } else {
                    NodeError::Transport(e.to_string())
                }
            })?;
        let status = resp.status().as_u16();
        self.record("header_updates", status, t0.elapsed().as_secs_f64());
        if !resp.status().is_success() {
            let b = resp.bytes().await.unwrap_or_default();
            return Err(NodeError::Http {
                status,
                body_prefix: body_prefix(&b),
            });
        }
        Ok(header_events(Box::pin(resp.bytes_stream())))
    }
}
