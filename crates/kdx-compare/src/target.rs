//! HTTP targets (chainweb-data, kda-indexer compat, kda-txs) behind one shared rate limit.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use bytes::Bytes;
use tokio::sync::Mutex;
use url::Url;

use crate::config::{CompareConfig, TargetKind};

/// Token bucket with burst 1 and a fractional rate, shared by every target, plus a cap on
/// requests in flight (slow chainweb-data queries hold a database connection each; one at a
/// time keeps the harness from stacking them).
pub struct Limiter {
    interval: Duration,
    next: Mutex<tokio::time::Instant>,
    in_flight: tokio::sync::Semaphore,
}

/// Requests in flight across all targets.
pub const MAX_IN_FLIGHT: usize = 1;

impl Limiter {
    pub fn new(rps: f64) -> Arc<Limiter> {
        Arc::new(Limiter {
            interval: Duration::from_secs_f64(1.0 / rps),
            next: Mutex::new(tokio::time::Instant::now()),
            in_flight: tokio::sync::Semaphore::new(MAX_IN_FLIGHT),
        })
    }

    /// One of the limiter's in-flight slots (held for the length of a read).
    pub(crate) async fn slot(&self) -> Option<tokio::sync::SemaphorePermit<'_>> {
        self.in_flight.acquire().await.ok()
    }

    pub async fn acquire(&self) {
        let mut next = self.next.lock().await;
        let now = tokio::time::Instant::now();
        if *next > now {
            tokio::time::sleep_until(*next).await;
            *next += self.interval;
        } else {
            *next = now + self.interval;
        }
    }
}

/// One captured HTTP exchange.
#[derive(Debug, Clone)]
pub struct HttpCapture {
    pub status: u16,
    /// Only `content-type`, `Chainweb-Next` and `x-search-truncated`.
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    pub ms: u64,
}

impl HttpCapture {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn next_token(&self) -> Option<&str> {
        self.header("Chainweb-Next")
    }

    pub fn json(&self) -> Option<serde_json::Value> {
        serde_json::from_slice(&self.body).ok()
    }
}

/// An answer over the target's body cap (`max_body_bytes`). A `/v1/txs` answer over the cap
/// goes through the streamed compare ([`crate::streamed`], R-P5P9-52 concern 5); a composed
/// `kda_txs_via_cwd` answer carries its capture, so it is not composed twice.
#[derive(Debug)]
pub struct BodyOverCap {
    pub msg: String,
    pub capture: Option<HttpCapture>,
}

impl std::fmt::Display for BodyOverCap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for BodyOverCap {}

/// A streamed read's status, kept headers, bytes read and time.
#[derive(Debug, Clone)]
pub struct StreamMeta {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub bytes: usize,
    pub ms: u64,
}

/// The response headers the harness keeps: `content-type`, `Chainweb-Next` and
/// `x-search-truncated`.
fn kept_headers(resp: &reqwest::Response) -> Vec<(String, String)> {
    let mut headers = Vec::new();
    for k in ["content-type", "chainweb-next", "x-search-truncated"] {
        if let Some(v) = resp.headers().get(k).and_then(|v| v.to_str().ok()) {
            let name = if k == "chainweb-next" {
                "Chainweb-Next"
            } else {
                k
            };
            headers.push((name.to_string(), v.to_string()));
        }
    }
    headers
}

/// The HTTP client options of one target.
#[derive(Debug, Clone)]
pub struct ClientOpts {
    pub timeout: Duration,
    /// Accept a self-signed certificate (an internal `graphql_oracle` only).
    pub insecure_tls: bool,
    /// Local address the client binds (`[targets.<n>] source_addr`, R-P5P9-60 H4).
    pub source_addr: Option<std::net::IpAddr>,
}

impl ClientOpts {
    /// The client builder of these options.
    pub fn builder(&self) -> reqwest::ClientBuilder {
        reqwest::Client::builder()
            .user_agent(kdx_node::USER_AGENT)
            .timeout(self.timeout)
            // chainweb-data (Warp) closes idle keep-alive connections; do not reuse old ones.
            .pool_idle_timeout(Duration::from_secs(5))
            .danger_accept_invalid_certs(self.insecure_tls)
            .local_address(self.source_addr)
    }
}

#[derive(Clone)]
pub struct Target {
    pub name: String,
    pub url: Url,
    pub kind: TargetKind,
    http: reqwest::Client,
    limiter: Arc<Limiter>,
    max_body_bytes: usize,
}

impl Target {
    pub fn new(
        name: &str,
        url: Url,
        kind: TargetKind,
        timeout: Duration,
        limiter: Arc<Limiter>,
        max_body_bytes: usize,
    ) -> anyhow::Result<Target> {
        Target::with_tls(name, url, kind, timeout, limiter, max_body_bytes, false)
    }

    /// [`Target::new`] with the option to accept a self-signed certificate (an internal
    /// `graphql_oracle` only; never for public endpoints).
    pub fn with_tls(
        name: &str,
        url: Url,
        kind: TargetKind,
        timeout: Duration,
        limiter: Arc<Limiter>,
        max_body_bytes: usize,
        insecure_tls: bool,
    ) -> anyhow::Result<Target> {
        let opts = ClientOpts {
            timeout,
            insecure_tls,
            source_addr: None,
        };
        Target::with_opts(name, url, kind, limiter, max_body_bytes, &opts)
    }

    /// [`Target::new`] with every client option ([`ClientOpts`]).
    pub fn with_opts(
        name: &str,
        url: Url,
        kind: TargetKind,
        limiter: Arc<Limiter>,
        max_body_bytes: usize,
        opts: &ClientOpts,
    ) -> anyhow::Result<Target> {
        let http = opts.builder().build().context("building http client")?;
        Ok(Target {
            name: name.to_string(),
            url,
            kind,
            http,
            limiter,
            max_body_bytes,
        })
    }

    /// All configured targets, sharing one limiter at `rate_rps`.
    pub fn all(cfg: &CompareConfig) -> anyhow::Result<Vec<Target>> {
        let limiter = Limiter::new(cfg.rate_rps);
        cfg.targets
            .iter()
            .map(|(n, t)| {
                // The GraphQL oracle never shares the bucket: its own limiter, at most 1 r/s.
                // kda-indexer's GraphQL never shares it either (R-P5P9-57 cadence): the shared
                // bucket is chainweb-data's budget, and its one request in flight made every
                // GraphQL step wait behind slow chainweb-data reads.
                let lim = match t.kind {
                    TargetKind::GraphqlOracle => Limiter::new(t.rate_rps.unwrap_or(1.0).min(1.0)),
                    TargetKind::KdaIndexerGraphql => {
                        Limiter::new(t.rate_rps.unwrap_or(cfg.rate_rps))
                    }
                    _ => match t.rate_rps {
                        Some(r) => Limiter::new(r),
                        None => limiter.clone(),
                    },
                };
                let opts = ClientOpts {
                    timeout: cfg.timeout(),
                    insecure_tls: t.insecure_tls,
                    source_addr: t.source_addr,
                };
                Target::with_opts(n, t.url.clone(), t.kind, lim, cfg.max_body_bytes, &opts)
            })
            .collect()
    }

    /// One `POST {url}` with a JSON body, under this target's limiter (HTTP status and body
    /// returned as captured; transport failures and timeouts are errors).
    pub async fn post_json(&self, body: &serde_json::Value) -> anyhow::Result<HttpCapture> {
        let _permit = self
            .limiter
            .in_flight
            .acquire()
            .await
            .context("limiter closed")?;
        self.limiter.acquire().await;
        let t0 = Instant::now();
        let mut resp = self
            .http
            .post(self.url.clone())
            .header("accept", "application/json")
            .json(body)
            .send()
            .await
            .with_context(|| format!("{} POST", self.name))?;
        let status = resp.status().as_u16();
        let limit = self.max_body_bytes;
        let mut buf = Vec::new();
        while let Some(chunk) = resp
            .chunk()
            .await
            .with_context(|| format!("{} POST body", self.name))?
        {
            if buf.len() + chunk.len() > limit {
                anyhow::bail!("{} POST: body exceeds {limit} bytes", self.name);
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(HttpCapture {
            status,
            headers: Vec::new(),
            body: Bytes::from(buf),
            ms: t0.elapsed().as_millis() as u64,
        })
    }

    /// The body cap of this target's reads.
    pub fn max_body_bytes(&self) -> usize {
        self.max_body_bytes
    }

    fn full_url(&self, path_and_query: &str) -> String {
        let base = self.url.as_str().trim_end_matches('/');
        format!("{base}{path_and_query}")
    }

    /// `GET {url}{path_and_query}`. A `kda_txs_via_cwd` target answers `/v1/txs/...` by
    /// composing chainweb-data calls (each under the shared limit); everything else is one
    /// request. Non-2xx statuses are returned, not errors; transport failures and timeouts
    /// are errors.
    pub async fn get(&self, path_and_query: &str) -> anyhow::Result<HttpCapture> {
        if self.kind == TargetKind::KdaTxsViaCwd {
            let cap = if path_and_query.starts_with("/v1/txs") {
                crate::via_cwd::fetch(self, path_and_query).await?
            } else {
                HttpCapture {
                    status: 404,
                    headers: Vec::new(),
                    body: Bytes::from_static(b"Not Found"),
                    ms: 0,
                }
            };
            if cap.body.len() > self.max_body_bytes {
                return Err(BodyOverCap {
                    msg: format!(
                        "{} GET {path_and_query}: composed body exceeds {} bytes",
                        self.name, self.max_body_bytes
                    ),
                    capture: Some(cap),
                }
                .into());
            }
            return Ok(cap);
        }
        self.get_direct(path_and_query).await
    }

    /// One `GET {url}{path_and_query}` under the shared limit.
    pub async fn get_direct(&self, path_and_query: &str) -> anyhow::Result<HttpCapture> {
        let _permit = self
            .limiter
            .in_flight
            .acquire()
            .await
            .context("limiter closed")?;
        self.limiter.acquire().await;
        let t0 = Instant::now();
        let url = self.full_url(path_and_query);
        // chainweb-data scans without a bound unless asked (plan Task 12): `search=` goes out
        // with `Chainweb-Execution-Strategy: Bounded`, inside the request's height window.
        let bounded = self.kind == TargetKind::ChainwebData
            && path_and_query.starts_with("/txs/search?search=");
        let build = |h: &reqwest::Client| {
            let r = h.get(&url);
            if bounded {
                r.header("Chainweb-Execution-Strategy", "Bounded")
            } else {
                r
            }
        };
        let resp = match build(&self.http).send().await {
            Ok(r) => r,
            // One retry for a failed send on a reused connection (GET is idempotent).
            Err(e) if !e.is_timeout() => {
                tracing::debug!(error = %e, "retrying once");
                self.limiter.acquire().await;
                build(&self.http)
                    .send()
                    .await
                    .with_context(|| format!("{} GET {path_and_query}", self.name))?
            }
            Err(e) => {
                return Err(
                    anyhow::Error::new(e).context(format!("{} GET {path_and_query}", self.name))
                )
            }
        };
        let status = resp.status().as_u16();
        let headers = kept_headers(&resp);
        // Bodies are untrusted (replayed heavy routes, a misbehaving target): read with a cap.
        let limit = self.max_body_bytes;
        let over = || -> anyhow::Error {
            BodyOverCap {
                msg: format!(
                    "{} GET {path_and_query}: body exceeds {limit} bytes",
                    self.name
                ),
                capture: None,
            }
            .into()
        };
        if resp.content_length().is_some_and(|n| n > limit as u64) {
            return Err(over());
        }
        let mut resp = resp;
        let mut buf = Vec::new();
        while let Some(chunk) = resp
            .chunk()
            .await
            .with_context(|| format!("{} GET {path_and_query} body", self.name))?
        {
            if buf.len() + chunk.len() > limit {
                return Err(over());
            }
            buf.extend_from_slice(&chunk);
        }
        let body = Bytes::from(buf);
        Ok(HttpCapture {
            status,
            headers,
            body,
            ms: t0.elapsed().as_millis() as u64,
        })
    }
}

impl Target {
    /// `GET {url}{path_and_query}` read as a stream, for an answer over the body cap
    /// ([`crate::streamed`]): each chunk goes to `sink`, which returns `true` to stop. Reads at
    /// most `limit` bytes. One request under the shared limit (a `kda_txs_via_cwd` target
    /// composes its answer as [`Target::get`] does). Transport failures and timeouts are
    /// errors; any status is returned.
    pub async fn get_chunks(
        &self,
        path_and_query: &str,
        limit: usize,
        sink: &mut (dyn FnMut(&[u8]) -> bool + Send),
    ) -> anyhow::Result<StreamMeta> {
        let t0 = Instant::now();
        if self.kind == TargetKind::KdaTxsViaCwd {
            let cap = if path_and_query.starts_with("/v1/txs") {
                crate::via_cwd::fetch(self, path_and_query).await?
            } else {
                HttpCapture {
                    status: 404,
                    headers: Vec::new(),
                    body: Bytes::from_static(b"Not Found"),
                    ms: 0,
                }
            };
            let n = cap.body.len().min(limit);
            sink(&cap.body[..n]);
            return Ok(StreamMeta {
                status: cap.status,
                headers: cap.headers,
                bytes: n,
                ms: t0.elapsed().as_millis() as u64,
            });
        }
        let _permit = self
            .limiter
            .in_flight
            .acquire()
            .await
            .context("limiter closed")?;
        self.limiter.acquire().await;
        let url = self.full_url(path_and_query);
        let mut resp = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("{} GET {path_and_query}", self.name))?;
        let status = resp.status().as_u16();
        let headers = kept_headers(&resp);
        let mut bytes = 0usize;
        while let Some(chunk) = resp
            .chunk()
            .await
            .with_context(|| format!("{} GET {path_and_query} body", self.name))?
        {
            let n = chunk.len().min(limit - bytes);
            bytes += n;
            if sink(&chunk[..n]) || bytes >= limit {
                break;
            }
        }
        Ok(StreamMeta {
            status,
            headers,
            bytes,
            ms: t0.elapsed().as_millis() as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn graphql_targets_never_share_the_rest_bucket() {
        let cfg: CompareConfig = toml::from_str(
            r#"
network = "mainnet01"
pairs = [["compat", "cwd"], ["gql", "oracle"]]
[node]
url = "http://127.0.0.1:31351"
[targets.cwd]
url = "http://127.0.0.1:31352"
kind = "chainweb_data"
[targets.compat]
url = "http://127.0.0.1:31362"
kind = "kda_indexer_compat"
[targets.gql]
url = "http://127.0.0.1:31363/graphql"
kind = "kda_indexer_graphql"
[targets.oracle]
url = "https://oracle.invalid/graphql"
kind = "graphql_oracle"
max_height = 6000000
"#,
        )
        .unwrap();
        let t: BTreeMap<String, Target> = Target::all(&cfg)
            .unwrap()
            .into_iter()
            .map(|t| (t.name.clone(), t))
            .collect();
        // chainweb-data and compat share the bucket (and its one request in flight) ...
        assert!(Arc::ptr_eq(&t["cwd"].limiter, &t["compat"].limiter));
        // ... kda-indexer's GraphQL and the oracle have their own (R-P5P9-57 cadence).
        assert!(!Arc::ptr_eq(&t["gql"].limiter, &t["cwd"].limiter));
        assert!(!Arc::ptr_eq(&t["oracle"].limiter, &t["cwd"].limiter));
        assert!(!Arc::ptr_eq(&t["gql"].limiter, &t["oracle"].limiter));
        // The GraphQL limiter runs at the shared rate when the target sets none.
        assert_eq!(
            t["gql"].limiter.interval,
            Duration::from_secs_f64(1.0 / 5.0)
        );
    }

    /// A one-shot HTTP server on 127.0.0.1 that answers `{}` and reports each peer address.
    async fn peer_server() -> (
        String,
        tokio::sync::mpsc::UnboundedReceiver<std::net::IpAddr>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/graphql", l.local_addr().unwrap());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut s, peer)) = l.accept().await {
                let _ = tx.send(peer.ip());
                let mut buf = vec![0u8; 8192];
                let _ = s.read(&mut buf).await;
                let _ = s
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}")
                    .await;
            }
        });
        (url, rx)
    }

    fn one_target(url: &str, source: Option<&str>) -> Target {
        let src = source.map_or(String::new(), |a| format!("source_addr = \"{a}\"\n"));
        let cfg: CompareConfig = toml::from_str(&format!(
            "network = \"mainnet01\"\npairs = []\n[node]\nurl = \"http://127.0.0.1:1\"\n\
             [targets.gql]\nurl = \"{url}\"\nkind = \"kda_indexer_graphql\"\n{src}"
        ))
        .unwrap();
        cfg.validate().unwrap();
        Target::all(&cfg).unwrap().remove(0)
    }

    /// R-P5P9-60 H4: `[targets.<n>] source_addr` reaches the target's client builder: the
    /// server sees that source, and an address the host does not own fails the bind.
    #[tokio::test]
    async fn h4_source_addr_reaches_the_client_builder() {
        let (url, mut peers) = peer_server().await;
        let body = serde_json::json!({"query": "{ x }"});
        // Bound to loopback: the request goes out from it.
        let t = one_target(&url, Some("127.0.0.1"));
        assert_eq!(t.post_json(&body).await.unwrap().status, 200);
        assert_eq!(peers.recv().await.unwrap().to_string(), "127.0.0.1");
        // Bound to TEST-NET-1 (never a local address): the connect fails at the bind, so the
        // option is in the client; without it the same URL answers.
        let bad = one_target(&url, Some("192.0.2.1"));
        assert!(bad.post_json(&body).await.is_err());
        let none = one_target(&url, None);
        assert_eq!(none.post_json(&body).await.unwrap().status, 200);
        // A bad address is a config error, not a silent default.
        let e = toml::from_str::<CompareConfig>(&format!(
            "network = \"m\"\npairs = []\n[node]\nurl = \"http://127.0.0.1:1\"\n\
             [targets.gql]\nurl = \"{url}\"\nkind = \"kda_indexer_graphql\"\nsource_addr = \"localhost\"\n"
        ));
        assert!(e.is_err());
    }

    /// On Linux every 127/8 address is local: a second loopback source is seen as itself.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn h4_a_second_loopback_source_is_what_the_server_sees() {
        let (url, mut peers) = peer_server().await;
        let t = one_target(&url, Some("127.0.0.2"));
        let body = serde_json::json!({"query": "{ x }"});
        assert_eq!(t.post_json(&body).await.unwrap().status, 200);
        assert_eq!(peers.recv().await.unwrap().to_string(), "127.0.0.2");
    }
}
