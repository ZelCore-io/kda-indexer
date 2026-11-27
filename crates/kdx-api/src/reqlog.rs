//! Opt-in request log (`[api.request_log]`, off by default): one JSON line per served request
//! on the compat and GraphQL listeners, for the hourly answer check after a cutover
//! (`scripts/hourly_check.py`, operations.md "Request log").
//!
//! The request path only samples, builds a small [`Entry`] and `try_send`s it on a bounded
//! queue; a writer thread formats, writes and rotates. A full queue drops the line and counts
//! it (`kdx_request_log_lines_total{result="dropped"}`); serving never waits for the log.
//!
//! What a line holds: time, listener, method, path and query, route template, status,
//! latency, the indexed tip range (lowest and highest chain, as last sampled for `/metrics`,
//! at most 10 s old) and the largest chain lag, and for a GraphQL POST its body's
//! `operationName`, `query` and `variables` (only when the body fits `max_body_bytes`). The
//! client address is a keyed hash (the key is random per process, so hashes do not link
//! across restarts) or left out; never the address itself. No header is written.

use std::collections::hash_map::RandomState;
use std::fs::{File, OpenOptions};
use std::hash::BuildHasher;
use std::io::{BufWriter, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use serde_json::{json, Map, Value};

use crate::server::ApiState;

/// Longest path-and-query written; a longer one is cut and flagged `uri_cut`.
pub const MAX_URI_CHARS: usize = 8192;
/// Lines the writer takes from the queue before it flushes.
const WRITE_BATCH: usize = 1024;
/// What an entry costs in the queue besides its URI and body (fields, strings, the slot).
const ENTRY_OVERHEAD: usize = 256;

/// How the client address appears in a line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientIp {
    /// A keyed 64-bit hash, hex (key random per process).
    Hash,
    /// Not written.
    Omit,
}

#[derive(Debug, Clone)]
pub struct Settings {
    /// The current file; rotated files are `<path>.1` (newest) to `<path>.<keep>`.
    pub path: PathBuf,
    /// Rotate once the current file reaches this size.
    pub max_bytes: u64,
    /// Rotated files kept (0: the current file is truncated instead).
    pub keep: u32,
    /// Share of requests logged, 0.0 to 1.0, spread evenly (every n-th, not random).
    pub sample_rate: f64,
    /// A GraphQL body larger than this is not written (`gql_cut` and its length instead).
    pub max_body_bytes: usize,
    /// Lines waiting for the writer at most; more are dropped and counted.
    pub queue: usize,
    /// Bytes waiting for the writer at most (URIs, GraphQL bodies and a fixed overhead per
    /// line); a line that would go over is dropped and counted (`dropped_bytes`).
    pub queue_max_bytes: usize,
    pub client_ip: ClientIp,
}

/// The GraphQL POST body for the log, handed over in the response's extensions (the handler
/// adds it only while the log is on). Built by [`RequestLog::graphql_body`], which keeps the
/// body only when it fits `max_body_bytes`: an oversized body is never held past its
/// request, only its length.
#[derive(Debug, Clone)]
pub struct GraphqlBody {
    pub body: Option<Bytes>,
    pub len: usize,
}

/// One request, as the request path hands it to the writer.
#[derive(Debug)]
pub struct Entry {
    pub unix_ms: u64,
    pub listener: &'static str,
    pub method: String,
    pub uri: String,
    pub route: Option<String>,
    pub status: u16,
    pub micros: u64,
    pub client: Option<u64>,
    pub tips: Option<(i64, i64)>,
    pub lag: Option<i64>,
    pub body: Option<GraphqlBody>,
}

impl Entry {
    /// What the entry holds in memory while it waits in the queue, roughly.
    pub fn queued_bytes(&self) -> usize {
        ENTRY_OVERHEAD
            + self.uri.len()
            + self.method.len()
            + self.route.as_ref().map_or(0, String::len)
            + self
                .body
                .as_ref()
                .and_then(|b| b.body.as_ref())
                .map_or(0, Bytes::len)
    }

    /// The JSON line (without the newline).
    pub fn to_line(&self, max_body: usize) -> String {
        let mut uri = self.uri.as_str();
        let mut cut = false;
        if uri.len() > MAX_URI_CHARS {
            let mut end = MAX_URI_CHARS;
            while !uri.is_char_boundary(end) {
                end -= 1;
            }
            uri = &uri[..end];
            cut = true;
        }
        let mut v = json!({
            "t": self.unix_ms,
            "listener": self.listener,
            "method": self.method,
            "uri": uri,
            "route": self.route,
            "status": self.status,
            "ms": (self.micros as f64 / 1000.0 * 10.0).round() / 10.0,
        });
        if cut {
            v["uri_cut"] = Value::Bool(true);
        }
        if let Some(c) = self.client {
            v["client"] = Value::String(format!("{c:016x}"));
        }
        if let Some((lo, hi)) = self.tips {
            v["tip_min"] = lo.into();
            v["tip_max"] = hi.into();
        }
        if let Some(l) = self.lag {
            v["lag_max"] = l.into();
        }
        if let Some(b) = &self.body {
            match b.body.as_ref().and_then(|x| graphql_fields(x, max_body)) {
                Some(g) => v["gql"] = g,
                None => {
                    v["gql_cut"] = Value::Bool(true);
                    v["gql_len"] = b.len.into();
                }
            }
        }
        v.to_string()
    }
}

/// `operationName`, `query` and `variables` of each request in a GraphQL body (an object, or
/// an array for a batch); None when the body is over `max` bytes or not a JSON object/array.
pub fn graphql_fields(body: &[u8], max: usize) -> Option<Value> {
    if body.len() > max {
        return None;
    }
    let one = |o: &Map<String, Value>| {
        let mut m = Map::new();
        for k in ["operationName", "query", "variables"] {
            if let Some(x) = o.get(k).filter(|x| !x.is_null()) {
                m.insert(k.to_string(), x.clone());
            }
        }
        Value::Object(m)
    };
    match serde_json::from_slice::<Value>(body).ok()? {
        Value::Object(o) => Some(one(&o)),
        Value::Array(a) => Some(Value::Array(
            a.iter()
                .map(|x| x.as_object().map_or(Value::Null, one))
                .collect(),
        )),
        _ => None,
    }
}

/// Spreads `rate` of the requests evenly: request `n` (from 0) is logged when
/// `floor((n + 1) * rate) > floor(n * rate)`.
pub fn sampled(n: u64, rate: f64) -> bool {
    if rate >= 1.0 {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }
    ((n + 1) as f64 * rate).floor() > (n as f64 * rate).floor()
}

/// The current file plus its rotations.
struct Rotating {
    path: PathBuf,
    max_bytes: u64,
    keep: u32,
    out: BufWriter<File>,
    size: u64,
}

fn open_append(path: &Path) -> std::io::Result<File> {
    if let Some(d) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(d)?;
    }
    OpenOptions::new().create(true).append(true).open(path)
}

fn rotated(path: &Path, i: u32) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(format!(".{i}"));
    PathBuf::from(s)
}

impl Rotating {
    fn open(s: &Settings) -> std::io::Result<Rotating> {
        let f = open_append(&s.path)?;
        let size = f.metadata()?.len();
        Ok(Rotating {
            path: s.path.clone(),
            max_bytes: s.max_bytes.max(1),
            keep: s.keep,
            out: BufWriter::new(f),
            size,
        })
    }

    fn write_line(&mut self, line: &str) -> std::io::Result<()> {
        if self.size > 0 && self.size + line.len() as u64 + 1 > self.max_bytes {
            self.rotate()?;
        }
        self.out.write_all(line.as_bytes())?;
        self.out.write_all(b"\n")?;
        self.size += line.len() as u64 + 1;
        Ok(())
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        self.out.flush()?;
        if self.keep == 0 {
            std::fs::remove_file(&self.path)?;
        } else {
            for i in (1..self.keep).rev() {
                match std::fs::rename(rotated(&self.path, i), rotated(&self.path, i + 1)) {
                    Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
                    _ => {}
                }
            }
            std::fs::rename(&self.path, rotated(&self.path, 1))?;
        }
        self.out = BufWriter::new(open_append(&self.path)?);
        self.size = 0;
        Ok(())
    }
}

struct Inner {
    tx: SyncSender<Entry>,
    seen: AtomicU64,
    written: Arc<AtomicU64>,
    dropped: AtomicU64,
    /// Bytes of the entries in the queue ([`Entry::queued_bytes`]); the writer subtracts.
    queued: Arc<AtomicU64>,
    max_queued: u64,
    max_body: usize,
    rate: f64,
    client_ip: ClientIp,
    key: RandomState,
}

/// Handle of a running request log (cheap to clone).
#[derive(Clone)]
pub struct RequestLog(Arc<Inner>);

impl std::fmt::Debug for RequestLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestLog")
            .field("rate", &self.0.rate)
            .finish_non_exhaustive()
    }
}

impl RequestLog {
    /// Opens (creating its directory) the file and starts the writer thread. Fails when the
    /// file cannot be opened, so a bad path stops the start-up instead of logging nothing.
    pub fn start(s: Settings) -> anyhow::Result<RequestLog> {
        let mut w = Rotating::open(&s)
            .map_err(|e| anyhow::anyhow!("api.request_log: open {}: {e}", s.path.display()))?;
        let (tx, rx) = sync_channel(s.queue.max(1));
        let log = RequestLog::with_sender(tx, &s);
        let written = log.0.written.clone();
        let queued = log.0.queued.clone();
        let max_body = s.max_body_bytes;
        std::thread::Builder::new()
            .name("kdx-request-log".into())
            .spawn(move || writer(&rx, &mut w, max_body, &written, &queued))?;
        Ok(log)
    }

    fn with_sender(tx: SyncSender<Entry>, s: &Settings) -> RequestLog {
        RequestLog(Arc::new(Inner {
            tx,
            seen: AtomicU64::new(0),
            written: Arc::new(AtomicU64::new(0)),
            dropped: AtomicU64::new(0),
            queued: Arc::new(AtomicU64::new(0)),
            max_queued: s.queue_max_bytes as u64,
            max_body: s.max_body_bytes,
            rate: s.sample_rate,
            client_ip: s.client_ip,
            key: RandomState::new(),
        }))
    }

    fn take_sample(&self) -> bool {
        sampled(self.0.seen.fetch_add(1, Ordering::Relaxed), self.0.rate)
    }

    fn client(&self, ip: IpAddr) -> Option<u64> {
        match self.0.client_ip {
            ClientIp::Omit => None,
            ClientIp::Hash => Some(self.0.key.hash_one(ip)),
        }
    }

    /// What the log may keep of a GraphQL body read on the request path: the body (a
    /// reference-counted handle, no copy) only when it fits `max_body_bytes`, else its length.
    pub fn graphql_body(&self, body: &Bytes) -> GraphqlBody {
        GraphqlBody {
            body: (body.len() <= self.0.max_body).then(|| body.clone()),
            len: body.len(),
        }
    }

    /// Queues `e`, or drops and counts it when the queue is full (`dropped`) or its bytes
    /// would pass `queue_max_bytes` (`dropped_bytes`).
    pub fn submit(&self, e: Entry) {
        let cost = e.queued_bytes() as u64;
        let q = &self.0.queued;
        let mut cur = q.load(Ordering::Relaxed);
        loop {
            if cur + cost > self.0.max_queued {
                self.0.dropped.fetch_add(1, Ordering::Relaxed);
                kdx_metrics::get().request_log("dropped_bytes");
                return;
            }
            match q.compare_exchange_weak(cur, cur + cost, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => break,
                Err(now) => cur = now,
            }
        }
        if self.0.tx.try_send(e).is_err() {
            q.fetch_sub(cost, Ordering::AcqRel);
            self.0.dropped.fetch_add(1, Ordering::Relaxed);
            kdx_metrics::get().request_log("dropped");
        }
    }

    /// Bytes waiting in the queue now.
    pub fn queued_bytes(&self) -> u64 {
        self.0.queued.load(Ordering::Relaxed)
    }

    /// Lines written and dropped so far by this handle's writer.
    pub fn counts(&self) -> (u64, u64) {
        (
            self.0.written.load(Ordering::Relaxed),
            self.0.dropped.load(Ordering::Relaxed),
        )
    }
}

fn writer(
    rx: &Receiver<Entry>,
    w: &mut Rotating,
    max_body: usize,
    written: &AtomicU64,
    queued: &AtomicU64,
) {
    let m = kdx_metrics::get();
    let mut warned = false;
    while let Ok(first) = rx.recv() {
        let mut next = Some(first);
        let mut n = 0;
        while let Some(e) = next.take() {
            let line = e.to_line(max_body);
            queued.fetch_sub(e.queued_bytes() as u64, Ordering::AcqRel);
            drop(e);
            match w.write_line(&line) {
                Ok(()) => {
                    written.fetch_add(1, Ordering::Relaxed);
                    m.request_log("written");
                    warned = false;
                }
                Err(err) => {
                    m.request_log("error");
                    if !warned {
                        tracing::warn!(error = %err, "request log: write failed");
                        warned = true;
                    }
                }
            }
            n += 1;
            if n < WRITE_BATCH {
                next = rx.try_recv().ok();
            }
        }
        if let Err(err) = w.out.flush() {
            m.request_log("error");
            if !warned {
                tracing::warn!(error = %err, "request log: flush failed");
                warned = true;
            }
        }
    }
    let _ = w.out.flush();
}

/// Middleware of the compat and GraphQL listeners (outermost, so the guard's 429/503 and the
/// deadline's 504 are logged as the client saw them).
pub async fn layer(
    State((listener, log, st)): State<(&'static str, RequestLog, Arc<ApiState>)>,
    req: Request,
    next: Next,
) -> Response {
    if !log.take_sample() {
        return next.run(req).await;
    }
    let t0 = Instant::now();
    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    let method = req.method().as_str().to_string();
    let uri = req
        .uri()
        .path_and_query()
        .map_or_else(|| req.uri().path().to_string(), |p| p.as_str().to_string());
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string());
    let client = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .and_then(|ConnectInfo(peer)| log.client(st.client_ip_of(*peer, req.headers())));
    let mut resp = next.run(req).await;
    let body = resp.extensions_mut().remove::<GraphqlBody>();
    let m = kdx_metrics::get();
    log.submit(Entry {
        unix_ms,
        listener,
        method,
        uri,
        route,
        status: resp.status().as_u16(),
        micros: u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX),
        client,
        tips: m.indexed_tip_range(),
        lag: m.max_chain_lag(),
        body,
    });
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(dir: &Path) -> Settings {
        Settings {
            path: dir.join("sub").join("requests.jsonl"),
            max_bytes: 1 << 20,
            keep: 2,
            sample_rate: 1.0,
            max_body_bytes: 4096,
            queue: 64,
            queue_max_bytes: 8 << 20,
            client_ip: ClientIp::Hash,
        }
    }

    fn entry(body: Option<&str>) -> Entry {
        Entry {
            unix_ms: 1_764_244_800_123,
            listener: "graphql",
            method: "POST".into(),
            uri: "/graphql".into(),
            route: Some("/graphql".into()),
            status: 200,
            micros: 12_345,
            client: Some(0xab),
            tips: Some((6_000_000, 6_000_002)),
            lag: Some(1),
            body: body.map(|b| GraphqlBody {
                body: Some(Bytes::from(b.to_string())),
                len: b.len(),
            }),
        }
    }

    #[test]
    fn sampling_is_even_and_exact() {
        let n = |r: f64| (0..1000u64).filter(|&i| sampled(i, r)).count();
        assert_eq!(n(1.0), 1000);
        assert_eq!(n(0.0), 0);
        assert_eq!(n(0.25), 250);
        assert_eq!(n(0.1), 100);
        // Every fourth, not bunched at the start.
        assert!(!sampled(0, 0.25) && !sampled(1, 0.25) && !sampled(2, 0.25) && sampled(3, 0.25));
    }

    #[test]
    fn line_has_the_fields_and_only_the_graphql_request_parts() {
        let body = r#"{"operationName":"Q","query":"query Q { lastBlockHeight }","variables":{"a":1},"extensions":{"x":"secret"}}"#;
        let v: Value = serde_json::from_str(&entry(Some(body)).to_line(4096)).unwrap();
        assert_eq!(v["t"], 1_764_244_800_123u64);
        assert_eq!(v["listener"], "graphql");
        assert_eq!(v["status"], 200);
        assert_eq!(v["ms"], 12.3);
        assert_eq!(v["client"], "00000000000000ab");
        assert_eq!(v["tip_min"], 6_000_000);
        assert_eq!(v["tip_max"], 6_000_002);
        assert_eq!(v["lag_max"], 1);
        assert_eq!(v["gql"]["operationName"], "Q");
        assert_eq!(v["gql"]["variables"]["a"], 1);
        assert!(v["gql"].get("extensions").is_none());
        assert!(!v.to_string().contains("secret"));
    }

    #[test]
    fn oversized_or_invalid_body_is_not_written() {
        let big = format!(r#"{{"query":"{}"}}"#, "x".repeat(100));
        let v: Value = serde_json::from_str(&entry(Some(&big)).to_line(50)).unwrap();
        assert!(v.get("gql").is_none());
        assert_eq!(v["gql_cut"], true);
        assert_eq!(v["gql_len"], big.len());
        assert!(!v.to_string().contains("xxxx"));
        let v: Value = serde_json::from_str(&entry(Some("not json")).to_line(4096)).unwrap();
        assert_eq!(v["gql_cut"], true);
        // A batch keeps each request's three parts.
        let b = r#"[{"query":"{a}"},{"query":"{b}","variables":{"x":2}}]"#;
        let v: Value = serde_json::from_str(&entry(Some(b)).to_line(4096)).unwrap();
        assert_eq!(v["gql"][1]["variables"]["x"], 2);
    }

    /// Security review: an oversized body is never kept for the queue (only its length), and
    /// a burst of large-bodied lines never holds more than `queue_max_bytes` in the queue.
    #[test]
    fn oversized_body_is_never_queued_and_queue_bytes_are_capped() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = settings(dir.path());
        s.max_body_bytes = 4096;
        s.queue = 10_000;
        s.queue_max_bytes = 64 * 1024;
        let (tx, rx) = sync_channel(s.queue);
        let log = RequestLog::with_sender(tx, &s);
        let big = Bytes::from(vec![b'x'; 1 << 20]);
        let g = log.graphql_body(&big);
        assert!(g.body.is_none(), "an oversized body is not kept");
        assert_eq!(g.len, 1 << 20);
        let mut e = entry(None);
        e.body = Some(g);
        assert!(e.queued_bytes() < 1024, "only its length is queued");
        let v: Value = serde_json::from_str(&e.to_line(4096)).unwrap();
        assert_eq!(
            (v["gql_cut"].clone(), v["gql_len"].clone()),
            (Value::Bool(true), (1u64 << 20).into())
        );
        let fits = Bytes::from(vec![b'{'; 4000]);
        assert!(log.graphql_body(&fits).body.is_some());
        // A burst of 4,000-byte bodies with no writer draining: the queued bytes stay under
        // the cap and the rest is dropped and counted.
        let before = metric("dropped_bytes");
        for _ in 0..1000 {
            let mut e = entry(None);
            e.body = Some(log.graphql_body(&fits));
            log.submit(e);
            assert!(log.queued_bytes() <= s.queue_max_bytes as u64);
        }
        let queued = rx.try_iter().count();
        assert!(queued > 0 && queued < 20, "{queued}");
        assert_eq!(log.counts().1, 1000 - queued as u64);
        assert!(metric("dropped_bytes") >= before + 1000 - queued as u64);
    }

    #[test]
    fn long_uri_is_cut_on_a_char_boundary() {
        let mut e = entry(None);
        e.uri = format!("/txs/search?search={}", "é".repeat(MAX_URI_CHARS));
        let v: Value = serde_json::from_str(&e.to_line(0)).unwrap();
        assert_eq!(v["uri_cut"], true);
        assert!(v["uri"].as_str().unwrap().len() <= MAX_URI_CHARS);
        assert!(v.get("gql").is_none() && v.get("gql_cut").is_none());
    }

    #[test]
    fn client_is_hashed_or_omitted_never_raw() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, _rx) = sync_channel(4);
        let log = RequestLog::with_sender(tx.clone(), &settings(dir.path()));
        let ip: IpAddr = "198.51.100.7".parse().unwrap();
        let h = log.client(ip).unwrap();
        assert_eq!(log.client(ip), Some(h), "stable within a process");
        assert_ne!(log.client("198.51.100.8".parse().unwrap()), Some(h));
        let mut e = entry(None);
        e.client = Some(h);
        assert!(!e.to_line(0).contains("198.51.100"));
        let mut s = settings(dir.path());
        s.client_ip = ClientIp::Omit;
        assert_eq!(RequestLog::with_sender(tx, &s).client(ip), None);
    }

    #[test]
    fn full_queue_drops_and_counts_without_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, rx) = sync_channel(2);
        let log = RequestLog::with_sender(tx, &settings(dir.path()));
        let before = metric("dropped");
        for _ in 0..5 {
            log.submit(entry(None));
        }
        assert_eq!(log.counts().1, 3);
        assert!(metric("dropped") >= before + 3);
        assert_eq!(log.queued_bytes(), 2 * entry(None).queued_bytes() as u64);
        assert_eq!(rx.try_iter().count(), 2);
    }

    fn metric(result: &str) -> u64 {
        kdx_metrics::get()
            .request_log_lines
            .get_or_create(&vec![("result", result.to_string())])
            .get()
    }

    #[test]
    fn writer_writes_lines_and_rotates() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = settings(dir.path());
        let one = entry(None).to_line(0).len() as u64 + 1;
        s.max_bytes = one * 3;
        let log = RequestLog::start(s.clone()).unwrap();
        for _ in 0..10 {
            log.submit(entry(None));
        }
        let t0 = Instant::now();
        while log.counts().0 < 10 && t0.elapsed().as_secs() < 10 {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(log.counts(), (10, 0));
        assert_eq!(log.queued_bytes(), 0, "the writer gives the bytes back");
        let lines = |p: &Path| {
            std::fs::read_to_string(p)
                .unwrap_or_default()
                .lines()
                .count()
        };
        // 10 lines, 3 per file: current 1, .1 and .2 three each, the oldest three gone (keep 2).
        assert_eq!(lines(&s.path), 1);
        assert_eq!(lines(&rotated(&s.path, 1)), 3);
        assert_eq!(lines(&rotated(&s.path, 2)), 3);
        assert!(!rotated(&s.path, 3).exists());
        for l in std::fs::read_to_string(&s.path).unwrap().lines() {
            let v: Value = serde_json::from_str(l).unwrap();
            assert_eq!(v["route"], "/graphql");
        }
    }

    #[test]
    fn bad_path_fails_at_start() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, "x").unwrap();
        let mut s = settings(dir.path());
        s.path = file.join("requests.jsonl");
        assert!(RequestLog::start(s).is_err());
    }
}
