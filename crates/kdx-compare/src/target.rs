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
        let http = reqwest::Client::builder()
            .user_agent(kdx_node::USER_AGENT)
            .timeout(timeout)
            // chainweb-data (Warp) closes idle keep-alive connections; do not reuse old ones.
            .pool_idle_timeout(Duration::from_secs(5))
            .danger_accept_invalid_certs(insecure_tls)
            .build()
            .context("building http client")?;
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
                let lim = match t.kind {
                    TargetKind::GraphqlOracle => Limiter::new(t.rate_rps.unwrap_or(1.0).min(1.0)),
                    _ => match t.rate_rps {
                        Some(r) => Limiter::new(r),
                        None => limiter.clone(),
                    },
                };
                Target::with_tls(
                    n,
                    t.url.clone(),
                    t.kind,
                    cfg.timeout(),
                    lim,
                    cfg.max_body_bytes,
                    t.insecure_tls,
                )
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
                anyhow::bail!(
                    "{} GET {path_and_query}: composed body exceeds {} bytes",
                    self.name,
                    self.max_body_bytes
                );
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
        // Bodies are untrusted (replayed heavy routes, a misbehaving target): read with a cap.
        let limit = self.max_body_bytes;
        if resp.content_length().is_some_and(|n| n > limit as u64) {
            anyhow::bail!(
                "{} GET {path_and_query}: body exceeds {limit} bytes",
                self.name
            );
        }
        let mut resp = resp;
        let mut buf = Vec::new();
        while let Some(chunk) = resp
            .chunk()
            .await
            .with_context(|| format!("{} GET {path_and_query} body", self.name))?
        {
            if buf.len() + chunk.len() > limit {
                anyhow::bail!(
                    "{} GET {path_and_query}: body exceeds {limit} bytes",
                    self.name
                );
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
