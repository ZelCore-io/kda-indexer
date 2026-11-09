//! The compare loop (spec section 7.4): draw requests, send them to both sides of each pair,
//! normalise, diff, classify, log and count.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::allowlist::Allowlist;
use crate::config::CompareConfig;
use crate::diff::{diff_route, follow_pages, DiffEntry, DiffKind, Paged};
use crate::gen::random::{canonical_header, RandomGen};
use crate::gen::replay::ReplayGen;
use crate::gen::seeds::SeedGen;
use crate::gen::{Generator, Mixer, OracleRef, Request, Tips, CHAIN_FROM_A};
use crate::metrics::Metrics;
use crate::normalize::{normalize, RouteKind};
use crate::oracle;
use crate::report::{iso_secs, now_secs, trim_body, DailySummary, DiffLog, DiffRecord};
use crate::target::Target;

/// Workers sending requests concurrently (the shared limiter caps the rate). Kept low:
/// slow chainweb-data queries (account/events pages) can take 30 s each.
const WORKERS: usize = 3;
const TIP_REFRESH: Duration = Duration::from_secs(60);
const SUMMARY_FLUSH: Duration = Duration::from_secs(60);
/// Consecutive A errors (timeouts, 5xx) that pause all A traffic: a slow chainweb-data keeps
/// running abandoned queries, so the harness backs off instead of adding more.
const BACKOFF_AFTER: u32 = 3;
const BACKOFF_FIRST: Duration = Duration::from_secs(120);
const BACKOFF_MAX: Duration = Duration::from_secs(900);

pub struct Ctx {
    pub cfg: CompareConfig,
    pub targets: HashMap<String, Target>,
    pub node: kdx_node::NodeClient,
    pub tips: Arc<Tips>,
    pub graph: kdx_node::ChainGraph,
    pub allow: Allowlist,
    pub log: DiffLog,
    pub summary: Mutex<DailySummary>,
    pub metrics: Metrics,
    /// Set on shutdown: workers finish their current request and take no new ones.
    pub stopping: std::sync::atomic::AtomicBool,
    /// Circuit breaker on A: consecutive errors and the current pause.
    a_errors_in_row: std::sync::atomic::AtomicU32,
    backoff: Mutex<(Option<tokio::time::Instant>, Duration)>,
}

/// What one compare produced.
#[derive(Debug, Clone)]
pub struct Outcome {
    pub class: String,
    pub record: Option<DiffRecord>,
}

impl Ctx {
    pub async fn new(cfg: CompareConfig) -> anyhow::Result<Ctx> {
        let node = cfg.node_client()?;
        let targets = Target::all(&cfg)?
            .into_iter()
            .map(|t| (t.name.clone(), t))
            .collect();
        let allow = Allowlist::load(&cfg.allowlist)?;
        let log = DiffLog::new(&cfg.out_dir)?;
        let graph = node.chain_graph().await?;
        let ctx = Ctx {
            graph,
            node,
            targets,
            tips: Arc::new(Tips::default()),
            allow,
            log,
            summary: Mutex::new(DailySummary::new()),
            metrics: Metrics::new(),
            stopping: std::sync::atomic::AtomicBool::new(false),
            a_errors_in_row: std::sync::atomic::AtomicU32::new(0),
            backoff: Mutex::new((None, BACKOFF_FIRST)),
            cfg,
        };
        ctx.refresh_tips().await?;
        Ok(ctx)
    }

    fn primary(&self) -> Option<&Target> {
        self.cfg.primary_target().and_then(|n| self.targets.get(n))
    }

    pub async fn refresh_tips(&self) -> anyhow::Result<()> {
        self.tips.refresh(&self.node, self.primary()).await
    }

    fn count_request(&self, route: &str, target: &str, n: usize) {
        let mut s = self.summary.lock().expect("summary");
        for _ in 0..n {
            self.metrics.request(route, target);
            s.request(route, target);
        }
    }

    fn count_class(&self, route: &str, class: &str) {
        self.metrics.diff(route, class);
        self.summary.lock().expect("summary").class(route, class);
    }

    pub fn flush_summary(&self) {
        let mut s = self.summary.lock().expect("summary");
        match s.flush(&self.cfg.out_dir) {
            Ok(Some(fresh)) => *s = fresh,
            Ok(None) => {}
            Err(e) => tracing::warn!(error = %e, "summary flush"),
        }
    }

    /// Waits while the A circuit breaker is open.
    async fn wait_backoff(&self) {
        let until = self.backoff.lock().expect("backoff").0;
        if let Some(t) = until {
            if t > tokio::time::Instant::now() {
                tokio::time::sleep_until(t).await;
            }
        }
    }

    fn note_a_result(&self, class: &str) {
        use std::sync::atomic::Ordering;
        let mut b = self.backoff.lock().expect("backoff");
        if class == "a-error" {
            let n = self.a_errors_in_row.fetch_add(1, Ordering::Relaxed) + 1;
            if n >= BACKOFF_AFTER {
                let pause = b.1;
                tracing::warn!(
                    secs = pause.as_secs(),
                    "A keeps failing: pausing A requests"
                );
                b.0 = Some(tokio::time::Instant::now() + pause);
                b.1 = (pause * 2).min(BACKOFF_MAX);
                self.a_errors_in_row.store(0, Ordering::Relaxed);
                drop(b);
                self.count_class("breaker", "a-paused");
            }
        } else {
            self.a_errors_in_row.store(0, Ordering::Relaxed);
            b.1 = BACKOFF_FIRST;
        }
    }

    /// Sends `req` through every pair; returns one outcome per pair.
    pub async fn process(&self, req: &Request) -> Vec<Outcome> {
        self.wait_backoff().await;
        let mut out = Vec::new();
        for (a, b) in &self.cfg.pairs {
            let Some(ta) = self.targets.get(a) else {
                continue;
            };
            let o = if b == "node" {
                self.vs_node(req, ta).await
            } else if let Some(tb) = self.targets.get(b) {
                self.vs_target(req, ta, tb).await
            } else {
                continue;
            };
            let route = req.route.as_str();
            self.note_a_result(&o.class);
            self.count_class(route, &o.class);
            if let Some(rec) = &o.record {
                {
                    let mut s = self.summary.lock().expect("summary");
                    if let Some(ids) = &rec.allowlisted {
                        for id in ids.split(',') {
                            s.allowlist_hit(id);
                        }
                    }
                    if rec.class == "diff" {
                        s.unexplained(route, &rec.diff);
                    }
                }
                if let Err(e) = self.log.append(rec) {
                    tracing::warn!(error = %e, "diff log append");
                }
            }
            out.push(o);
        }
        out
    }

    async fn fetch_a(&self, req: &Request, t: &Target) -> anyhow::Result<Paged> {
        let depth = if req.route.paginated() {
            self.cfg.follow_depth
        } else {
            0
        };
        let p = follow_pages(t, &req.path_and_query, depth).await;
        let n = p.as_ref().map(|p| p.pages.len()).unwrap_or(1);
        self.count_request(req.route.as_str(), &t.name, n);
        p
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        &self,
        req: &Request,
        a: &Target,
        b_name: &str,
        a_cap: Option<&Paged>,
        b_status: Option<u16>,
        b_body: String,
        diff: Vec<DiffEntry>,
        class: &str,
        allowlisted: &[String],
        b_curl: String,
        note: Option<String>,
    ) -> DiffRecord {
        let first = a_cap.and_then(|p| p.pages.first());
        DiffRecord {
            ts: iso_secs(now_secs()),
            route: req.route.as_str().to_string(),
            request: req.path_and_query.clone(),
            a_target: a.name.clone(),
            b_target: b_name.to_string(),
            a_status: first.map(|c| c.status),
            b_status,
            a_body_trimmed: first.map(|c| trim_body(&c.body)).unwrap_or_default(),
            b_body_trimmed: trim_body(b_body.as_bytes()),
            diff,
            class: class.to_string(),
            allowlisted: (!allowlisted.is_empty()).then(|| allowlisted.join(",")),
            curl: format!("curl -s \"$A{}\" ; {}", req.path_and_query, b_curl),
            note,
        }
    }

    /// A against another target: same path on both sides.
    async fn vs_target(&self, req: &Request, ta: &Target, tb: &Target) -> Outcome {
        let pa = self.fetch_a(req, ta).await;
        let pb = self.fetch_a(req, tb).await;
        let (pa, pb) = match (pa, pb) {
            (Ok(a), Ok(b)) => (a, b),
            (Err(e), _) => return self.a_error(req, ta, &tb.name, None, &e.to_string()),
            (Ok(a), Err(e)) => {
                return self.b_error(req, ta, &tb.name, Some(&a), &e.to_string(), String::new())
            }
        };
        let sa = pa.pages[0].status;
        let sb = pb.pages[0].status;
        if sa >= 500 {
            return self.a_error(req, ta, &tb.name, Some(&pa), "5xx");
        }
        let (mut va, mut vb) = (pa.items.clone(), pb.items.clone());
        normalize(&req.route, &mut va);
        normalize(&req.route, &mut vb);
        let mut d = diff_route(&req.route, &va, &vb);
        if sa != sb {
            d.insert(
                0,
                DiffEntry {
                    path: "/@status".into(),
                    kind: DiffKind::Changed,
                    a: Some(json!(sa)),
                    b: Some(json!(sb)),
                },
            );
        }
        let curl_b = format!("curl -s \"$B{}\"", req.path_and_query);
        let b_body = trim_body(&pb.pages[0].body);
        self.finish(
            req,
            ta,
            &tb.name,
            &pa,
            Some(sb),
            b_body,
            d,
            false,
            curl_b,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish(
        &self,
        req: &Request,
        ta: &Target,
        b_name: &str,
        pa: &Paged,
        b_status: Option<u16>,
        b_body: String,
        d: Vec<DiffEntry>,
        legacy: bool,
        curl_b: String,
        note: Option<String>,
    ) -> Outcome {
        let (unexplained, ids) = self
            .allow
            .classify_request(&req.route, &req.path_and_query, &d);
        let (legacy_entries, other): (Vec<_>, Vec<_>) = if legacy {
            unexplained
                .into_iter()
                .partition(|e| oracle::is_cmd_path(&e.path))
        } else {
            (Vec::new(), unexplained)
        };
        let class = if !other.is_empty() {
            "diff"
        } else if legacy {
            // Known category: the node serves a re-encoded cmd for this tx (logged with the
            // rehash note even when every projected field agrees).
            "legacy-cmd"
        } else if !ids.is_empty() {
            "allowlisted"
        } else {
            "match"
        };
        if !ids.is_empty() {
            let mut s = self.summary.lock().expect("summary");
            if class == "allowlisted" || class == "match" {
                for id in &ids {
                    s.allowlist_hit(id);
                }
            }
        }
        let record = match class {
            "diff" | "legacy-cmd" => {
                let mut entries = other;
                entries.extend(legacy_entries);
                Some(self.record(
                    req,
                    ta,
                    b_name,
                    Some(pa),
                    b_status,
                    b_body,
                    entries,
                    class,
                    &ids,
                    curl_b,
                    note,
                ))
            }
            _ => None,
        };
        Outcome {
            class: class.to_string(),
            record,
        }
    }

    fn a_error(
        &self,
        req: &Request,
        ta: &Target,
        b_name: &str,
        pa: Option<&Paged>,
        msg: &str,
    ) -> Outcome {
        let rec = self.record(
            req,
            ta,
            b_name,
            pa,
            None,
            String::new(),
            Vec::new(),
            "a-error",
            &[],
            String::new(),
            Some(msg.to_string()),
        );
        Outcome {
            class: "a-error".into(),
            record: Some(rec),
        }
    }

    fn b_error(
        &self,
        req: &Request,
        ta: &Target,
        b_name: &str,
        pa: Option<&Paged>,
        msg: &str,
        curl_b: String,
    ) -> Outcome {
        let rec = self.record(
            req,
            ta,
            b_name,
            pa,
            None,
            String::new(),
            Vec::new(),
            "b-error",
            &[],
            curl_b,
            Some(msg.to_string()),
        );
        Outcome {
            class: "b-error".into(),
            record: Some(rec),
        }
    }

    fn simple(class: &str) -> Outcome {
        Outcome {
            class: class.into(),
            record: None,
        }
    }

    /// A against the node oracle.
    async fn vs_node(&self, req: &Request, ta: &Target) -> Outcome {
        let pa = match self.fetch_a(req, ta).await {
            Ok(p) => p,
            Err(e) => return self.a_error(req, ta, "node", None, &format!("{e:#}")),
        };
        let sa = pa.pages[0].status;
        if sa >= 500 {
            return self.a_error(req, ta, "node", Some(&pa), &format!("status {sa}"));
        }
        let mut va = pa.items.clone();
        normalize(&req.route, &mut va);
        match &req.oracle {
            None => Self::simple("no-oracle"),
            Some(OracleRef::Poll { chain, request_key }) => {
                self.poll_compare(req, ta, &pa, *chain, request_key).await
            }
            Some(OracleRef::Header {
                chain,
                height,
                hash,
            }) => {
                self.header_compare(req, ta, &pa, *chain, *height, hash)
                    .await
            }
        }
    }

    fn node_curl_poll(&self, chain: u8, rk: &str) -> String {
        format!(
            "curl -s -X POST -H 'content-type: application/json' -d '{{\"requestKeys\":[\"{rk}\"]}}' \"$B/chainweb/0.0/{}/chain/{chain}/pact/api/v1/poll\"",
            self.cfg.network
        )
    }

    async fn poll_compare(
        &self,
        req: &Request,
        ta: &Target,
        pa: &Paged,
        chain: u8,
        rk: &str,
    ) -> Outcome {
        let a_body = &pa.items;
        let a_first = match a_body {
            Value::Array(xs) => xs.first().cloned(),
            Value::Object(_) => Some(a_body.clone()),
            _ => None,
        };
        let chain = if chain == CHAIN_FROM_A {
            match a_first
                .as_ref()
                .and_then(|v| v.get("chain"))
                .and_then(Value::as_u64)
            {
                Some(c) => c as u8,
                None => return Self::simple("no-oracle"),
            }
        } else {
            chain
        };
        let curl_b = self.node_curl_poll(chain, rk);
        let route = req.route.as_str();
        self.count_request(route, "node", 1);
        let poll = match self.node.poll(chain, &[rk.to_string()]).await {
            Ok(m) => m,
            Err(e) => return self.b_error(req, ta, "node", Some(pa), &e.to_string(), curl_b),
        };
        let a_found = pa.pages[0].status == 200 && a_first.is_some();
        let Some(entry) = poll.get(rk).cloned() else {
            if !a_found {
                return Self::simple("match");
            }
            let d = vec![DiffEntry {
                path: "/".into(),
                kind: DiffKind::Extra,
                a: a_first,
                b: None,
            }];
            return self.finish(
                req,
                ta,
                "node",
                pa,
                Some(200),
                "{}".into(),
                d,
                false,
                curl_b,
                Some("node /poll does not know this request key".into()),
            );
        };
        let height = entry
            .pointer("/metaData/blockHeight")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if height > self.tips.min_tip() {
            return Self::simple("out-of-range");
        }
        self.count_request(route, "node", 2);
        let header = match canonical_header(&self.node, &self.tips, chain, height).await {
            Ok(Some(h)) => h,
            Ok(None) => {
                return self.b_error(req, ta, "node", Some(pa), "no canonical header", curl_b)
            }
            Err(e) => return self.b_error(req, ta, "node", Some(pa), &format!("{e:#}"), curl_b),
        };
        let tx = match self.payload_tx(chain, &header, rk).await {
            Ok(t) => t,
            Err(e) => return self.b_error(req, ta, "node", Some(pa), &format!("{e:#}"), curl_b),
        };
        let mut vb = oracle::from_poll(&entry, &header, tx.as_ref());
        let block_hash = vb.get("blockHash").cloned().unwrap_or(Value::Null);
        // A side: the canonical row (for /txs/txs, other rows go under /orphans).
        let mut va = match (&req.route, a_body) {
            (RouteKind::Txs, Value::Array(rows)) => {
                let (canon, orphans): (Vec<&Value>, Vec<&Value>) = rows
                    .iter()
                    .partition(|r| r.get("blockHash") == Some(&block_hash));
                let mut v = canon
                    .first()
                    .map(|r| oracle::project_tx_detail(r))
                    .unwrap_or(Value::Null);
                if !orphans.is_empty() {
                    if let Value::Object(m) = &mut v {
                        m.insert(
                            "orphans".into(),
                            Value::Array(
                                orphans.iter().map(|r| oracle::project_block(r)).collect(),
                            ),
                        );
                    }
                }
                v
            }
            (_, Value::Object(_)) => oracle::project_tx_detail(a_body),
            _ => Value::Null,
        };
        normalize(&req.route, &mut va);
        normalize(&req.route, &mut vb);
        let d = if va.is_null() {
            // A does not have the tx (or only orphan rows). Genesis-block txs get their own
            // path so the allowlist can name them.
            let genesis = height == 0 || !self.graph.chains_at(height - 1).contains(&chain);
            vec![DiffEntry {
                path: if genesis {
                    "/@genesis".into()
                } else {
                    "/".into()
                },
                kind: DiffKind::Missing,
                a: None,
                b: Some(vb.clone()),
            }]
        } else {
            diff_route(&req.route, &va, &vb)
        };
        let legacy = !va.is_null() && tx.as_ref().is_some_and(|t| !oracle::cmd_hashes(t));
        let note = match (&tx, legacy) {
            (Some(t), true) => {
                let a_row = match (&req.route, a_body) {
                    (RouteKind::Txs, Value::Array(rows)) => rows
                        .iter()
                        .find(|r| r.get("blockHash") == Some(&block_hash))
                        .cloned(),
                    (_, Value::Object(_)) => Some(a_body.clone()),
                    _ => None,
                };
                a_row.map(|row| {
                    let (rebuilt, template) = oracle::rehash_from_a(t, &row);
                    format!(
                        "legacy cmd: node-served cmd does not hash to the request key; cmd rebuilt from A's fields hashes={rebuilt}; node cmd reserialises to its own text={template}"
                    )
                })
            }
            _ => None,
        };
        let b_body = serde_json::to_string(&entry).unwrap_or_default();
        self.finish(
            req,
            ta,
            "node",
            pa,
            Some(200),
            b_body,
            d,
            legacy,
            curl_b,
            note,
        )
    }

    async fn payload_tx(
        &self,
        chain: u8,
        header: &Value,
        rk: &str,
    ) -> anyhow::Result<Option<kdx_decode::DecodedTx>> {
        let ph = header
            .get("payloadHash")
            .and_then(Value::as_str)
            .context("header without payloadHash")?;
        let payload = self.node.payload_with_outputs(chain, ph).await?;
        let b = kdx_decode::decode_block(header, &payload)
            .map_err(|e| anyhow::anyhow!("decode: {e}"))?;
        Ok(b.txs.into_iter().find(|t| t.hash.to_b64url() == rk))
    }

    async fn header_compare(
        &self,
        req: &Request,
        ta: &Target,
        pa: &Paged,
        chain: u8,
        height: u64,
        hash: &str,
    ) -> Outcome {
        if height > self.tips.min_tip() {
            return Self::simple("out-of-range");
        }
        let curl_b = format!(
            "curl -s -X POST -H 'accept: application/json;blockheader-encoding=object' -H 'content-type: application/json' -d '{{\"lower\":[],\"upper\":[\"<cut hash of chain {chain}>\"]}}' \"$B/chainweb/0.0/{}/chain/{chain}/header/branch?minheight={height}&maxheight={height}\"",
            self.cfg.network
        );
        self.count_request(req.route.as_str(), "node", 1);
        let header = match canonical_header(&self.node, &self.tips, chain, height).await {
            Ok(Some(h)) => h,
            Ok(None) => {
                return self.b_error(req, ta, "node", Some(pa), "no canonical header", curl_b)
            }
            Err(e) => return self.b_error(req, ta, "node", Some(pa), &format!("{e:#}"), curl_b),
        };
        if header.get("hash").and_then(Value::as_str) != Some(hash) {
            let rec = self.record(
                req,
                ta,
                "node",
                Some(pa),
                Some(200),
                header.to_string(),
                Vec::new(),
                "reorg",
                &[],
                curl_b,
                Some(format!("sampled block {hash} is no longer canonical")),
            );
            return Outcome {
                class: "reorg".into(),
                record: Some(rec),
            };
        }
        let mut vb = Value::Object(oracle::header_fields(&header));
        let bh = vb.get("blockHash").cloned();
        let mut va = match &pa.items {
            Value::Array(rows) => {
                let (canon, orphans): (Vec<&Value>, Vec<&Value>) =
                    rows.iter().partition(|r| r.get("blockHash") == bh.as_ref());
                let mut v = canon
                    .first()
                    .map(|r| oracle::project_block(r))
                    .unwrap_or(Value::Null);
                if !orphans.is_empty() {
                    if let Value::Object(m) = &mut v {
                        m.insert(
                            "orphans".into(),
                            Value::Array(
                                orphans.iter().map(|r| oracle::project_block(r)).collect(),
                            ),
                        );
                    }
                }
                v
            }
            _ => Value::Null,
        };
        normalize(&req.route, &mut va);
        normalize(&req.route, &mut vb);
        let d = diff_route(&req.route, &va, &vb);
        self.finish(
            req,
            ta,
            "node",
            pa,
            Some(200),
            header.to_string(),
            d,
            false,
            curl_b,
            None,
        )
    }
}

/// `run`: continuous loop until SIGINT/SIGTERM (or `max_secs`, when set).
pub async fn run(cfg: CompareConfig, max_secs: Option<u64>) -> anyhow::Result<()> {
    let ctx = Arc::new(Ctx::new(cfg).await?);
    if let Some(bind) = ctx.cfg.metrics_bind.clone() {
        let m = ctx.metrics.clone();
        tokio::spawn(async move {
            if let Err(e) = m.serve(bind).await {
                tracing::error!(error = %e, "metrics server");
            }
        });
    }
    let graph = ctx.node.chain_graph().await?;
    let random = Some(RandomGen::new(
        ctx.node.clone(),
        graph,
        ctx.tips.clone(),
        ctx.cfg.min_depth,
    ));
    let seeds = match &ctx.cfg.seeds {
        Some(p) => Some(SeedGen::load(p)?.with_node(ctx.node.clone(), ctx.tips.clone())),
        None => None,
    };
    let replay = match &ctx.cfg.replay {
        Some(p) => Some(ReplayGen::load(p)?),
        None => None,
    };
    let mut mixer = Mixer::new(random, seeds, replay, ctx.cfg.mix.clone());

    let (tx, rx) = mpsc::channel::<Request>(32);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));
    let mut workers = Vec::new();
    for _ in 0..WORKERS {
        let ctx = ctx.clone();
        let rx = rx.clone();
        workers.push(tokio::spawn(async move {
            loop {
                let req = { rx.lock().await.recv().await };
                let Some(req) = req else { break };
                if ctx.stopping.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                for o in ctx.process(&req).await {
                    tracing::debug!(route = req.route.as_str(), class = %o.class, path = %req.path_and_query);
                }
            }
        }));
    }
    // Periodic tip refresh and summary flush.
    {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let mut t_tip = tokio::time::interval(TIP_REFRESH);
            let mut t_sum = tokio::time::interval(SUMMARY_FLUSH);
            t_tip.tick().await;
            loop {
                tokio::select! {
                    _ = t_tip.tick() => {
                        if let Err(e) = ctx.refresh_tips().await {
                            tracing::warn!(error = %e, "tip refresh");
                        }
                    }
                    _ = t_sum.tick() => ctx.flush_summary(),
                }
            }
        });
    }
    let stop = shutdown_signal(max_secs);
    tokio::pin!(stop);
    let mut empty_seen = 0;
    loop {
        tokio::select! {
            _ = &mut stop => break,
            r = mixer.next() => {
                match r {
                    Ok(reqs) => {
                        let e = mixer.empty_blocks();
                        for _ in empty_seen..e {
                            ctx.count_class("block", "empty-block");
                        }
                        empty_seen = e;
                        for q in reqs {
                            tokio::select! {
                                _ = &mut stop => { return finish(ctx, tx, workers).await; }
                                s = tx.send(q) => { if s.is_err() { break; } }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %format!("{e:#}"), "generator");
                        ctx.count_class("generator", "gen-error");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
    finish(ctx, tx, workers).await
}

async fn finish(
    ctx: Arc<Ctx>,
    tx: mpsc::Sender<Request>,
    workers: Vec<tokio::task::JoinHandle<()>>,
) -> anyhow::Result<()> {
    tracing::info!("shutting down: finishing in-flight requests");
    ctx.stopping
        .store(true, std::sync::atomic::Ordering::Relaxed);
    drop(tx);
    for w in workers {
        let _ = tokio::time::timeout(Duration::from_secs(60), w).await;
    }
    ctx.flush_summary();
    tracing::info!("summary flushed");
    Ok(())
}

async fn shutdown_signal(max_secs: Option<u64>) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    let timer = async {
        match max_secs {
            Some(s) => tokio::time::sleep(Duration::from_secs(s)).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
        _ = timer => {}
    }
}

/// `once`: one request through the full pipeline; returns the outcomes.
pub async fn once(cfg: CompareConfig, path: &str) -> anyhow::Result<Vec<Outcome>> {
    let ctx = Ctx::new(cfg).await?;
    let route = RouteKind::from_path(path);
    // Oracle from the request itself: tx/txs with a request key use /poll (chain from A).
    let oracle = match route {
        RouteKind::Tx | RouteKind::Txs => path
            .split_once('?')
            .and_then(|(_, q)| q.split('&').find_map(|kv| kv.strip_prefix("requestkey=")))
            .map(|rk| OracleRef::Poll {
                chain: CHAIN_FROM_A,
                request_key: rk.to_string(),
            }),
        _ => None,
    };
    let req = Request::new(path.to_string(), crate::gen::Origin::Seed, oracle);
    let out = ctx.process(&req).await;
    ctx.flush_summary();
    Ok(out)
}
