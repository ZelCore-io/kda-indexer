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
use crate::diff::{
    cut_to_common_depth, diff_route, follow_pages, recent_diff, DiffEntry, DiffKind, Paged,
    PAGE_CUT_PATH, PAGE_DUP_PATH, STATS_ESTIMATE_PATH, STATS_SUPPLY_PATH,
};
use crate::gen::random::{canonical_header, RandomGen};
use crate::gen::replay::ReplayGen;
use crate::gen::seeds::SeedGen;
use crate::gen::{Generator, Mixer, OracleRef, Request, Tips, CHAIN_FROM_A};
use crate::metrics::Metrics;
use crate::normalize::{normalize, RouteKind};
use crate::oracle;
use crate::report::{
    iso_secs, now_secs, sh_quote, sh_url, trim_body, DailySummary, DiffLog, DiffRecord,
};
use crate::target::Target;

/// Workers sending requests concurrently (the shared limiter caps the rate). Kept low:
/// slow chainweb-data queries (account/events pages) can take 30 s each.
const WORKERS: usize = 3;
/// kda-txs's own query limit; a 404 that took this long is its timeout.
const KDA_TXS_LIMIT_MS: u64 = 29_000;
/// At most this many A-only account rows are checked against chainweb-data's /txs/tx.
const GAP_CHECKS: usize = 5;
/// At most this many canonical-header lookups on the node per compare for the orphan rules
/// (rows at a (chain, height) already looked up are free). Seen live: 13 chainweb-data orphan
/// rows in one 3-page account answer, and more than 50 kda-txs orphan rows in one
/// `/v1/txs/k:7fb0d75f.../coin/250` answer; a 250-row page needs at most 250 lookups.
const ORPHAN_CHECKS: usize = 300;
/// JSON path of the visible allowlist hit for a genesis request key that A answers 404 on
/// `/txs/tx` because it is stored on several chains (F4, chainweb-data does the same).
const GENESIS_MULTI_PATH: &str = "/~genesis-multi-chain";
/// JSON path of the visible allowlist hit for kda-txs event lists the node contradicts.
const KDATXS_EVENTS_NODE_PATH: &str = "/~kdatxs-events-node";
/// JSON path of the visible allowlist hit for event rows chainweb-data lacks at whole heights.
const CWD_EVENTS_GAP_PATH: &str = "/~cwd-events-missing-node";
/// At most this many rows per compare go to the node for the event tie-break.
const EVENT_NODE_CHECKS: usize = 25;
/// JSON path of the visible allowlist hit for rows missing from chainweb-data's transfers.
const CWD_TRANSFERS_GAP_PATH: &str = "/~cwd-transfers-gap";
/// Visible hit of `/txs/search?search=` rows chainweb-data matched through a `cont` tx's
/// initial code (S-3).
const SEARCH_CONT_HISTORY_PATH: &str = "/~search-cont-history";
/// Visible hit of `/txs/search?pactid=` rows compared with ties sorted by request key.
const PACTID_TIE_ORDER_PATH: &str = "/~pactid-tie-order";

/// JSON path of the visible allowlist hit for [`events_as_multiset`].
const EVENT_ORDER_PATH: &str = "/~event-order";
/// JSON path of the visible allowlist hit for rows above a lagging oracle's tip (ruling R3E-11).
const ORACLE_LAG_PATH: &str = "/~oracle-lag";
/// JSON path of the visible allowlist hit for a kda-txs answer that changed on a retry.
const ORACLE_UNSTABLE_PATH: &str = "/~oracle-unstable";
/// JSON path of the visible allowlist hit for the oldest height cut at a `/v1/txs` limit.
const LIMIT_EDGE_PATH: &str = "/~limit-edge";
/// Heights below A's tip that the oracles (chainweb-data, kda-txs) may not have filled yet:
/// seen live, chainweb-data at its tip still missed rows 1-4 h old (filled later), so rows of
/// the last 12 h (1440 heights) are not compared against them.
const ORACLE_HORIZON: u64 = 1440;
/// JSON path of the visible allowlist hit for a chainweb-data page that ended on a coinbase row.
const CWD_CURSOR_SKIP_PATH: &str = "/~cwd-cursor-skip";
/// JSON path of the visible allowlist hit for /v1/txs rows kda-txs leaves out.
const KDATXS_GAP_PATH: &str = "/~kdatxs-gap";
/// JSON paths of the visible allowlist hits for A-only rows the node confirms (ruling R3E-14).
const CWD_MISSING_NODE_PATH: &str = "/~cwd-missing-node";
const KDATXS_MISSING_NODE_PATH: &str = "/~kdatxs-missing-node";
/// At most this many A-only rows per compare are polled on the node for that rule.
const NODE_GAP_CHECKS: usize = 300;
/// Request keys per node `/poll` call of that rule.
const NODE_POLL_CHUNK: usize = 25;
/// Replica pair: list rows are compared only up to the lower of the two tips minus this
/// many heights. A tip is the highest row height on `/txs/recent`; a chain can sit below
/// it by up to the braid's diameter (3 on the 20-chain graph) plus whatever the tail of
/// that side lags on that one chain, so the top heights are not final on both sides yet.
const REPLICA_TIP_MARGIN: u64 = 5;
/// How long a chainweb-data tip reading is reused.
const ORACLE_TIP_TTL: Duration = Duration::from_secs(60);
/// Upper bound of the supply minted per height on all chains together (coinbase rewards; live
/// about 31 KDA per height in 2026-11), used when chainweb-data's tip lags A's.
const SUPPLY_PER_HEIGHT: f64 = 60.0;
/// Pause before the one retry of a `/v1/txs` diff against kda-txs.
const KDA_TXS_RETRY: Duration = Duration::from_secs(30);
/// A canonical hash read on the node is reused by later compares only this many heights or
/// more below the chain's cut (a fork near the tip can still change it).
const CANON_CACHE_DEPTH: u64 = 20;
/// Entries of that cache at most; it is emptied when full.
const CANON_CACHE_MAX: usize = 100_000;
/// JSON path of the visible allowlist hit for `/txs/search` rows chainweb-data lists in a
/// block that is not on the node's canonical branch (R-P5P9-48).
const ORACLE_ORPHAN_PATH: &str = "/~oracle-orphan";
/// JSON path of the visible allowlist hit for `/txs/search` rows chainweb-data lacks below its
/// tip while the node puts them in kda-indexer's canonical block (R-P5P9-48).
const ORACLE_GAP_PATH: &str = "/~oracle-gap";

/// For each pair of tx objects at the same place (the object itself, or the same index of
/// two arrays) whose `events` hold the same items in another order, sorts both `events`
/// arrays by canonical text. Returns how many txs that was.
fn events_as_multiset(a: &mut Value, b: &mut Value) -> u32 {
    fn one(x: &mut Value, y: &mut Value) -> u32 {
        let (Some(Value::Array(ex)), Some(Value::Array(ey))) =
            (x.get_mut("events"), y.get_mut("events"))
        else {
            return 0;
        };
        if ex == ey || ex.len() != ey.len() {
            return 0;
        }
        // Sort by text with numbers as f64 (the sides can write the same decimal in other
        // forms, e.g. 0.2463053640037912 with more digits); equal means no diff entries.
        let mut sx = ex.clone();
        let mut sy = ey.clone();
        sx.sort_by_cached_key(num_canon_text);
        sy.sort_by_cached_key(num_canon_text);
        if !crate::diff::diff(&Value::Array(sx.clone()), &Value::Array(sy.clone())).is_empty() {
            return 0;
        }
        *ex = sx;
        *ey = sy;
        1
    }
    match (a, b) {
        (Value::Array(xa), Value::Array(xb)) => xa
            .iter_mut()
            .zip(xb.iter_mut())
            .map(|(x, y)| one(x, y))
            .sum(),
        (x @ Value::Object(_), y @ Value::Object(_)) => one(x, y),
        _ => 0,
    }
}

/// Items of array `a` that `b` does not hold (as a multiset; non-arrays hold nothing).
fn only_in_first(a: &Value, b: &Value) -> Vec<Value> {
    let (Value::Array(xa), Value::Array(xb)) = (a, b) else {
        return Vec::new();
    };
    let mut left: HashMap<String, usize> = HashMap::new();
    for x in xb {
        *left.entry(x.to_string()).or_default() += 1;
    }
    xa.iter()
        .filter(|x| match left.get_mut(&x.to_string()) {
            Some(n) if *n > 0 => {
                *n -= 1;
                false
            }
            _ => true,
        })
        .cloned()
        .collect()
}
const TIP_REFRESH: Duration = Duration::from_secs(60);
const SUMMARY_FLUSH: Duration = Duration::from_secs(60);
/// Consecutive A errors (timeouts, 5xx) that pause all A traffic: a slow chainweb-data keeps
/// running abandoned queries, so the harness backs off instead of adding more.
const BACKOFF_AFTER: u32 = 3;
const BACKOFF_FIRST: Duration = Duration::from_secs(120);
const BACKOFF_MAX: Duration = Duration::from_secs(900);

/// Canonical text of a value with every number written as its f64.
fn num_canon_text(v: &Value) -> String {
    fn walk(v: &Value) -> Value {
        match v {
            Value::Number(n) => json!(n.as_f64().map(|f| f.to_string())),
            Value::Array(x) => Value::Array(x.iter().map(walk).collect()),
            Value::Object(m) => {
                Value::Object(m.iter().map(|(k, x)| (k.clone(), walk(x))).collect())
            }
            other => other.clone(),
        }
    }
    walk(v).to_string()
}

/// Rewrites every JSON number as its nearest f64.
fn numbers_as_f64(v: &mut Value) {
    match v {
        Value::Number(n) => {
            if let Some(x) = n.as_f64().and_then(serde_json::Number::from_f64) {
                *n = x;
            }
        }
        Value::Array(x) => x.iter_mut().for_each(numbers_as_f64),
        Value::Object(m) => m.values_mut().for_each(numbers_as_f64),
        _ => {}
    }
}

fn len_of(v: &Value) -> usize {
    v.as_array().map_or(0, Vec::len)
}

fn row_height(r: &Value) -> Option<u64> {
    r.get("height").and_then(Value::as_u64)
}

/// Drops rows above `h`; returns how many.
fn drop_above(v: &mut Value, h: u64) -> usize {
    let Value::Array(x) = v else { return 0 };
    let n = x.len();
    x.retain(|r| row_height(r).is_none_or(|rh| rh <= h));
    n - x.len()
}

fn drop_at_or_below(v: &mut Value, h: u64) {
    if let Value::Array(x) = v {
        x.retain(|r| row_height(r).is_none_or(|rh| rh > h));
    }
}

fn min_height(v: &Value) -> Option<u64> {
    v.as_array()?.iter().filter_map(row_height).min()
}

/// Height up to which a replica pair compares list rows: the lower of the two tips minus
/// `REPLICA_TIP_MARGIN`.
fn replica_clamp(tip_a: u64, tip_b: u64) -> u64 {
    tip_a.min(tip_b).saturating_sub(REPLICA_TIP_MARGIN)
}

/// Replica pair: splits off the route-intrinsic tolerance entries (`/stats` count and
/// supply inside their tolerance, which move on every block, so two independently tailing
/// stores differ there whenever they read different tips). Returns the other entries and
/// whether any tolerance entry was dropped (the outcome is then `skew`, not `match`).
/// Every oracle-defect allowlist entry stays out of a replica pair.
fn split_replica_skew(d: Vec<DiffEntry>) -> (Vec<DiffEntry>, bool) {
    let n = d.len();
    let rest: Vec<DiffEntry> = d
        .into_iter()
        .filter(|e| e.path != STATS_ESTIMATE_PATH && e.path != STATS_SUPPLY_PATH)
        .collect();
    let skew = rest.len() != n;
    (rest, skew)
}

fn num_value(v: &Value) -> Option<f64> {
    match v {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

/// The `limit` query value, if any.
/// Rows of `a` whose (requestKey, blockHash, idx) is not in `b` (counted as a multiset).
/// (requestKey, blockHash, idx) of a row: the identity both list rules match rows by.
type RowKey = (String, String, Option<String>);

fn row_key(r: &Value) -> RowKey {
    let s = |k: &str| r.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    let idx = r.get("idx").and_then(Value::as_f64).map(|f| f.to_string());
    (s("requestKey"), s("blockHash"), idx)
}

/// A row's chain id, written as an integer or (after [`numbers_as_f64`], kda-txs pair) as a
/// float.
fn row_chain(r: &Value) -> Option<u8> {
    let c = r.get("chain")?;
    if let Some(n) = c.as_u64() {
        return u8::try_from(n).ok();
    }
    let f = c.as_f64()?;
    (f.fract() == 0.0 && (0.0..=255.0).contains(&f)).then_some(f as u8)
}

/// Two event lists with the same items, in any order (numbers compared as written).
fn same_events(a: &Value, b: &Value) -> bool {
    let (Value::Array(x), Value::Array(y)) = (a, b) else {
        return a == b;
    };
    if x.len() != y.len() {
        return false;
    }
    let mut sx = x.clone();
    let mut sy = y.clone();
    sx.sort_by_cached_key(num_canon_text);
    sy.sort_by_cached_key(num_canon_text);
    crate::diff::diff(&Value::Array(sx), &Value::Array(sy)).is_empty()
}

fn only_keys_in_first(a: &Value, b: &Value) -> Vec<Value> {
    let key = row_key;
    let mut have: HashMap<RowKey, usize> = HashMap::new();
    if let Value::Array(xb) = b {
        for r in xb {
            *have.entry(key(r)).or_default() += 1;
        }
    }
    let mut out = Vec::new();
    if let Value::Array(xa) = a {
        for r in xa {
            match have.get_mut(&key(r)) {
                Some(n) if *n > 0 => *n -= 1,
                _ => out.push(r.clone()),
            }
        }
    }
    out
}

/// The account a `/txs/account/{account}` or `/v1/txs/{account}/...` request names.
fn query_account(req: &Request) -> String {
    if req.route == RouteKind::V1Txs {
        return crate::via_cwd::parse(&req.path_and_query).account;
    }
    let path = req.path_and_query.split('?').next().unwrap_or("");
    path.strip_prefix("/txs/account/")
        .map(crate::via_cwd::pct_decode)
        .unwrap_or_default()
}

/// `/coins` against chainweb-data (R-P5P9-48): a difference past the relative 1e-6 of
/// `coins-supply-tolerance` is the tip gap when the side with the higher tip has the higher
/// supply and the difference is at most the coinbase minted between the two tips, plus that
/// relative tolerance (seen live in run 6E-3: 2,621.7 KDA with chainweb-data about 145
/// heights behind). Tips are A's and chainweb-data's `/txs/recent`; no gap, no allowance.
/// Returns the visible hit.
fn coins_tip_lag(a: &Value, b: &Value, a_tip: u64, b_tip: u64) -> Option<DiffEntry> {
    let x = crate::diff::num_or_text_f64(a)?;
    let y = crate::diff::num_or_text_f64(b)?;
    let tol = 1e-6 * x.abs().max(y.abs());
    let d = (x - y).abs();
    if a_tip == 0 || a_tip == b_tip || d <= tol || (x > y) != (a_tip > b_tip) {
        return None;
    }
    let minted = crate::rewards::minted_between(a_tip.min(b_tip), a_tip.max(b_tip));
    (d <= minted + tol).then(|| DiffEntry {
        path: format!("{ORACLE_LAG_PATH}/coins"),
        kind: DiffKind::Changed,
        a: Some(json!({"value": a, "tip": a_tip})),
        b: Some(json!({"value": b, "tip": b_tip, "minted_between": minted})),
    })
}

/// What [`Ctx::search_oracle_rows`] moved out of a `/txs/search` compare.
#[derive(Debug, Default)]
struct SearchRows {
    /// chainweb-data rows in a block off the node's canonical branch.
    orphans: Vec<Value>,
    /// A rows above chainweb-data's tip.
    lag: Vec<Value>,
    /// A rows below it that the node puts in A's canonical block.
    gap: Vec<Value>,
    /// chainweb-data's tip, when it was read.
    b_tip: Option<u64>,
    /// Rows of A and of B dropped at or past the last row of a full page.
    a_cut: usize,
    b_cut: usize,
}

/// A `/txs/search` row's place in the answer order: `pactid` successful first, then height
/// descending; `search=` height descending.
fn search_place(pactid: bool, r: &Value) -> (bool, std::cmp::Reverse<u64>) {
    let failed = pactid && r.get("result").and_then(Value::as_str) != Some("TxSucceeded");
    let h = r.get("height").and_then(Value::as_u64).unwrap_or(0);
    (failed, std::cmp::Reverse(h))
}

impl SearchRows {
    /// The visible allowlist hits.
    fn hits(&self) -> Vec<DiffEntry> {
        let first = |rows: &[Value]| -> Vec<Value> {
            rows.iter()
                .take(10)
                .map(|r| {
                    json!([
                        r.get("chain"),
                        r.get("height"),
                        r.get("requestKey"),
                        r.get("blockHash")
                    ])
                })
                .collect()
        };
        let mut d = Vec::new();
        if !self.orphans.is_empty() {
            d.push(DiffEntry {
                path: ORACLE_ORPHAN_PATH.into(),
                kind: DiffKind::Missing,
                a: None,
                b: Some(json!({"rows": self.orphans.len(), "a_cut": self.a_cut,
                    "b_cut": self.b_cut, "first": first(&self.orphans)})),
            });
        }
        if !self.lag.is_empty() {
            d.push(DiffEntry {
                path: ORACLE_LAG_PATH.into(),
                kind: DiffKind::Extra,
                a: Some(json!({"rows": self.lag.len(), "b_tip": self.b_tip,
                    "a_cut": self.a_cut, "b_cut": self.b_cut, "first": first(&self.lag)})),
                b: None,
            });
        }
        if !self.gap.is_empty() {
            d.push(DiffEntry {
                path: ORACLE_GAP_PATH.into(),
                kind: DiffKind::Extra,
                a: Some(json!({"rows": self.gap.len(), "b_tip": self.b_tip,
                    "a_cut": self.a_cut, "b_cut": self.b_cut, "first": first(&self.gap)})),
                b: None,
            });
        }
        d
    }
}

/// Whether a `/txs/search` side may hold more rows than it answered: a `pactid` answer has
/// no `Chainweb-Next`, so one with `limit` rows (default and maximum 50) may be cut; a
/// `search=` answer is complete unless a pending `Chainweb-Next` was left (then the two
/// sides were already cut to their common depth).
fn search_page_full(path_and_query: &str, rows: usize, page_cut: bool) -> bool {
    if page_cut || !path_and_query.contains("pactid=") {
        return false;
    }
    let limit = query_limit(path_and_query).map_or(50, |l| l.min(50));
    rows >= limit
}

fn query_limit(path_and_query: &str) -> Option<usize> {
    let q = path_and_query.split_once('?')?.1;
    url::form_urlencoded::parse(q.as_bytes())
        .find(|(k, _)| k == "limit")
        .and_then(|(_, v)| v.parse().ok())
}

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
    /// chainweb-data tip per target name (time read, max height on `/txs/recent`).
    oracle_tips: Mutex<HashMap<String, (std::time::Instant, u64)>>,
    /// The node's canonical block hash per (chain, height), read on the cut's branch and kept
    /// across compares once at least CANON_CACHE_DEPTH heights below the chain's cut (the
    /// answer for every (chain, height, hash) question at that place).
    canon: Mutex<HashMap<(u8, u64), String>>,
    /// GraphQL operations, sampler and oracle guard (empty without GraphQL pairs).
    pub gql: crate::graphql::GqlState,
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
        let gql = crate::graphql::GqlState::new(&cfg)?;
        let ctx = Ctx {
            gql,
            graph,
            node,
            targets,
            tips: Arc::new(Tips::default()),
            allow,
            log,
            summary: Mutex::new(DailySummary::load_or_new(&cfg.out_dir)),
            metrics: Metrics::new(),
            stopping: std::sync::atomic::AtomicBool::new(false),
            a_errors_in_row: std::sync::atomic::AtomicU32::new(0),
            backoff: Mutex::new((None, BACKOFF_FIRST)),
            oracle_tips: Mutex::new(HashMap::new()),
            canon: Mutex::new(HashMap::new()),
            cfg,
        };
        ctx.refresh_tips().await?;
        Ok(ctx)
    }

    pub fn primary(&self) -> Option<&Target> {
        self.cfg.primary_target().and_then(|n| self.targets.get(n))
    }

    pub async fn refresh_tips(&self) -> anyhow::Result<()> {
        self.tips.refresh(&self.node, self.primary()).await
    }

    pub(crate) fn count_request(&self, route: &str, target: &str, n: usize) {
        let mut s = self.summary.lock().expect("summary");
        for _ in 0..n {
            self.metrics.request(route, target);
            s.request(route, target);
        }
    }

    pub(crate) fn count_class(&self, route: &str, class: &str) {
        self.metrics.diff(route, class);
        self.summary.lock().expect("summary").class(route, class);
    }

    pub fn flush_summary(&self) {
        let mut s = self.summary.lock().expect("summary");
        match s.flush(&self.cfg.out_dir) {
            Ok(Some(mut fresh)) => {
                fresh.graphql_unpaired = std::mem::take(&mut s.graphql_unpaired);
                *s = fresh;
            }
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
            // A pair only compares a request both sides serve (the node serves no /v1/txs).
            if !ta.kind.serves(req.route) {
                continue;
            }
            let o = if b == "node" || b == "node_local" {
                if req.route == RouteKind::V1Txs {
                    continue;
                }
                if req.route == RouteKind::Balances {
                    self.balances_vs_local(req, ta).await
                } else if b == "node_local" {
                    continue;
                } else {
                    self.vs_node(req, ta).await
                }
            } else if let Some(tb) = self.targets.get(b) {
                if !tb.kind.serves(req.route) {
                    continue;
                }
                let mut o = self.vs_target(req, ta, tb, None).await;
                // kda-txs is fed by its own indexer and can answer differently a moment later
                // (seen live: set shifts that were gone when re-fetched). One retry; when it
                // agrees, the visible hit /~oracle-unstable replaces the diff.
                if o.class == "diff"
                    && req.route == RouteKind::V1Txs
                    && tb.kind == crate::config::TargetKind::KdaTxs
                {
                    tokio::time::sleep(KDA_TXS_RETRY).await;
                    let unstable = DiffEntry {
                        path: ORACLE_UNSTABLE_PATH.into(),
                        kind: DiffKind::Changed,
                        a: None,
                        b: None,
                    };
                    let o2 = self.vs_target(req, ta, tb, Some(unstable)).await;
                    if o2.class != "diff" {
                        o = o2;
                    }
                }
                o
            } else {
                continue;
            };
            let route = req.route.as_str();
            self.summary
                .lock()
                .expect("summary")
                .pair(route, a, b, &o.class);
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
        if let Ok(p) = &p {
            let mut s = self.summary.lock().expect("summary");
            for c in &p.pages {
                s.latency(req.route.as_str(), &t.name, c.ms);
            }
        }
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
            curl: format!("curl -s {} ; {}", sh_url("A", &req.path_and_query), b_curl),
            note,
        }
    }

    /// A against another target: same path on both sides.
    async fn vs_target(
        &self,
        req: &Request,
        ta: &Target,
        tb: &Target,
        extra: Option<DiffEntry>,
    ) -> Outcome {
        let pa = self.fetch_a(req, ta).await;
        let pb = self.fetch_a(req, tb).await;
        let (pa, pb) = match (pa, pb) {
            (Ok(a), Ok(b)) => (a, b),
            (Err(e), _) => return self.a_error(req, ta, &tb.name, None, &format!("{e:#}")),
            (Ok(a), Err(e)) => {
                return self.b_error(
                    req,
                    ta,
                    &tb.name,
                    Some(&a),
                    &format!("{e:#}"),
                    String::new(),
                )
            }
        };
        let sa = pa.pages[0].status;
        let sb = pb.pages[0].status;
        if sa >= 500 {
            return self.a_error(req, ta, &tb.name, Some(&pa), "5xx");
        }
        // kda-txs answers 404 "Not Found" when its own 30 s query limit runs out (seen live on
        // busy accounts): an oracle failure, not an answer.
        if tb.kind == crate::config::TargetKind::KdaTxs
            && sb == 404
            && pb.pages[0].ms >= KDA_TXS_LIMIT_MS
        {
            return self.b_error(
                req,
                ta,
                &tb.name,
                Some(&pa),
                "kda-txs answered 404 at its 30 s query limit",
                String::new(),
            );
        }
        let (mut va, mut vb) = (pa.items.clone(), pb.items.clone());
        let page_cut = req.route.paginated()
            && cut_to_common_depth(&mut va, pa.truncated, &mut vb, pb.truncated);
        // chainweb-data's Chainweb-Next cursor is (height, request key, idx) without the chain:
        // after a page that ends on a coinbase row it skips the coinbase rows of the other
        // chains at that height (seen live: /txs/events coin.TRANSFER at 3293927, 10 of 20
        // coinbase rows). That height is dropped on both sides.
        let mut cursor_skip = 0usize;
        if req.route.paginated()
            && tb.kind == crate::config::TargetKind::ChainwebData
            && sa == 200
            && sb == 200
        {
            let n = pb.pages.len();
            let hs: Vec<u64> = pb.pages[..n.saturating_sub(1)]
                .iter()
                .filter_map(|c| {
                    let v = c.json()?;
                    let last = v.as_array()?.last()?.clone();
                    (last.get("requestKey").and_then(Value::as_str) == Some("<coinbase>"))
                        .then(|| row_height(&last))
                        .flatten()
                })
                .collect();
            if !hs.is_empty() {
                let before = len_of(&va) + len_of(&vb);
                for v in [&mut va, &mut vb] {
                    if let Value::Array(x) = v {
                        x.retain(|r| row_height(r).is_none_or(|h| !hs.contains(&h)));
                    }
                }
                cursor_skip = before - (len_of(&va) + len_of(&vb));
            }
        }
        // Ruling R3E-11: the oracles lag A and fill recent heights late. Account, event and
        // /v1/txs rows are compared only below A's tip minus ORACLE_HORIZON; when that drops
        // rows, a side that held its full limit (or was cut at a page) is cut at its lowest
        // height on both sides.
        // Whether each side held its full limit (or was cut at a page), read before any rule
        // drops rows; used by the node-confirmed rule below.
        let row_limit = if req.route == RouteKind::V1Txs {
            crate::via_cwd::parse(&req.path_and_query)
                .limit
                .ok()
                .map(|l| l as usize)
        } else {
            query_limit(&req.path_and_query)
        };
        let full_a0 = pa.truncated || page_cut || row_limit.is_none_or(|l| len_of(&va) >= l);
        let full_b0 = pb.truncated || page_cut || row_limit.is_none_or(|l| len_of(&vb) >= l);
        let mut lag_rows = 0usize;
        // Replica pair (two kda-indexer targets): rows above the lower of the two tips minus
        // a margin are dropped (the sides read different tips and a chain can trail its
        // side's tip), with no allowlist hit.
        let replica = ta.kind == crate::config::TargetKind::KdaIndexerCompat
            && tb.kind == crate::config::TargetKind::KdaIndexerCompat;
        let clamp_to = if replica {
            match (self.oracle_tip(ta).await, self.oracle_tip(tb).await) {
                (Some(x), Some(y)) => Some(replica_clamp(x, y)),
                _ => None,
            }
        } else {
            let t = self.tips.a_tip();
            (t > ORACLE_HORIZON).then(|| t - ORACLE_HORIZON)
        };
        if matches!(
            req.route,
            RouteKind::Account | RouteKind::Events | RouteKind::V1Txs
        ) && sa == 200
            && sb == 200
            && clamp_to.is_some()
        {
            let clamp = clamp_to.unwrap_or(0);
            let limit = if req.route == RouteKind::V1Txs {
                crate::via_cwd::parse(&req.path_and_query)
                    .limit
                    .ok()
                    .map(|l| l as usize)
            } else {
                query_limit(&req.path_and_query)
            };
            let full = |n: usize, t: bool| t || limit.is_none_or(|l| n >= l);
            let (na, nb) = (len_of(&va), len_of(&vb));
            let fa = full(na, pa.truncated || page_cut);
            let fb = full(nb, pb.truncated || page_cut);
            let dropped = drop_above(&mut va, clamp) + drop_above(&mut vb, clamp);
            if dropped > 0 {
                let cut = [(fa, min_height(&va)), (fb, min_height(&vb))]
                    .into_iter()
                    .filter_map(|(f, h)| if f { h } else { None })
                    .max();
                if let Some(cut) = cut {
                    drop_at_or_below(&mut va, cut);
                    drop_at_or_below(&mut vb, cut);
                }
                lag_rows = (na + nb) - (len_of(&va) + len_of(&vb));
            }
        }
        // kda-txs can stop one row short of the limit (seen live: limit 250, 250 rows on A,
        // 249 on kda-txs, A's extra row the oldest). When one side holds `limit` rows and the
        // counts differ, both sides keep the rows above the higher of the two lowest heights.
        let mut limit_edge = 0usize;
        if req.route == RouteKind::V1Txs && sa == 200 && sb == 200 {
            if let Ok(l) = crate::via_cwd::parse(&req.path_and_query).limit {
                let (na, nb) = (len_of(&va), len_of(&vb));
                if na != nb && na.max(nb) as u64 == l {
                    let cut = min_height(&va).max(min_height(&vb));
                    if let Some(cut) = cut {
                        drop_at_or_below(&mut va, cut);
                        drop_at_or_below(&mut vb, cut);
                        limit_edge = (na + nb) - (len_of(&va) + len_of(&vb));
                    }
                }
            }
        }
        // /stats against a lagging chainweb-data: the supply differs by what was minted in
        // the heights between the two tips (R3E-11).
        let mut supply_lag: Option<(Value, Value)> = None;
        if req.route == RouteKind::Stats
            && tb.kind == crate::config::TargetKind::ChainwebData
            && sa == 200
            && sb == 200
        {
            let a_tip = self.tips.a_tip();
            // chainweb-data's /txs/recent can time out (seen live 01:32Z, supply 2730 KDA
            // lower): the tip is then taken as A's and the 240-height floor applies.
            {
                let b_tip = self.oracle_tip(tb).await.unwrap_or(a_tip);
                let x = va.get("coinsInCirculation").and_then(num_value);
                let y = vb.get("coinsInCirculation").and_then(num_value);
                if let (Some(x), Some(y)) = (x, y) {
                    // chainweb-data also fills recent heights late (seen live: 5153 KDA lower
                    // with its /txs/recent near the tip), so at least 240 heights (2 h) count.
                    let d = a_tip.saturating_sub(b_tip).max(240) as f64;
                    let tol = 1e-6 * x.abs().max(y.abs());
                    if x > y && d > 0.0 && x - y > tol && x - y <= d * SUPPLY_PER_HEIGHT + tol {
                        let pair = (
                            va.get("coinsInCirculation").cloned().unwrap_or(Value::Null),
                            vb.get("coinsInCirculation").cloned().unwrap_or(Value::Null),
                        );
                        for v in [&mut va, &mut vb] {
                            if let Some(m) = v.as_object_mut() {
                                m.remove("coinsInCirculation");
                            }
                        }
                        supply_lag = Some(pair);
                    }
                }
            }
        }
        // /coins against chainweb-data: the supply grows with every block, so a difference
        // within the coinbase minted between the two tips is that gap (R-P5P9-48).
        let mut coins_lag: Option<DiffEntry> = None;
        if req.route == RouteKind::Coins
            && tb.kind == crate::config::TargetKind::ChainwebData
            && sa == 200
            && sb == 200
            && va != vb
        {
            if let Some(b_tip) = self.oracle_tip(tb).await {
                coins_lag = coins_tip_lag(&va, &vb, self.tips.a_tip(), b_tip);
                if coins_lag.is_some() {
                    vb = va.clone();
                }
            }
        }
        normalize(&req.route, &mut va);
        normalize(&req.route, &mut vb);
        // chainweb-data and kda-txs list a tx's events in their own order (seen live on
        // cross-chain txs: node and compat [TRANSFER, TRANSFER_XCHAIN, TRANSFER, X_YIELD],
        // both oracles [TRANSFER, TRANSFER, TRANSFER_XCHAIN, X_YIELD]). The node pair checks
        // the order; here a tx's events compare as a multiset, as the visible hit /~event-order.
        let event_order = if matches!(req.route, RouteKind::Tx | RouteKind::Txs | RouteKind::V1Txs)
        {
            events_as_multiset(&mut va, &mut vb)
        } else {
            0
        };
        // Oracle rows of orphaned blocks: chainweb-data (account, events) and kda-txs
        // (/v1/txs) can list a tx in a block the node no longer has as canonical, beside or
        // instead of the canonical one. Rows confirmed against the node go under /orphans.
        let mut orphans: Vec<Value> = Vec::new();
        if matches!(
            req.route,
            RouteKind::Account | RouteKind::Events | RouteKind::V1Txs
        ) && sa == 200
            && sb == 200
        {
            orphans = self.oracle_orphans(&mut va, &mut vb).await;
        }
        // chainweb-data's /txs/recent can list a tx in an orphaned block (seen live:
        // qTm3DYap... chain 14 height 7395953, chainweb-data block G33Rgc_P beside the canonical
        // rt2smpaG it also holds; the node and compat rt2smpaG). A row both sides list by
        // request key in another block goes under /orphans, A's row in its place, when the
        // node's canonical block there is A's.
        if req.route == RouteKind::Recent
            && tb.kind == crate::config::TargetKind::ChainwebData
            && sa == 200
            && sb == 200
        {
            orphans.extend(self.recent_orphans(&va, &mut vb).await);
        }
        // chainweb-data's transfers table misses some rows whose TRANSFER event it does hold
        // (seen live on chain 0 around heights 1.59M-1.69M). Rows only A lists, when B lists
        // no row of its own, are checked against B's own /txs/tx (at most GAP_CHECKS calls);
        // confirmed ones leave the diff as the visible hit /~cwd-transfers-gap.
        let mut gap_rows: Vec<Value> = Vec::new();
        if req.route == RouteKind::Account && sa == 200 && sb == 200 {
            let only_a = only_in_first(&va, &vb);
            if !only_a.is_empty()
                && only_a.len() <= GAP_CHECKS
                && only_in_first(&vb, &va).is_empty()
            {
                let mut all = true;
                for r in &only_a {
                    if !self.cwd_has_transfer(req, tb, r).await {
                        all = false;
                        break;
                    }
                }
                if all {
                    if let Value::Array(xa) = &mut va {
                        for r in &only_a {
                            if let Some(i) = xa.iter().position(|x| x == r) {
                                xa.remove(i);
                            }
                        }
                    }
                    gap_rows = only_a;
                }
            }
        }
        // kda-txs leaves out some rows of an account (seen live: 28481ce2... chain 0 height
        // 1288814, xwallet-xchain-gas chain 5 height 7386167, canonical blocks). /v1/txs rows
        // only A lists, when kda-txs lists no row of its own, are checked against
        // chainweb-data's /txs/tx: same block and an event naming the account. Confirmed rows
        // leave the diff as the visible hit /~kdatxs-gap.
        let mut kt_gap: Vec<Value> = Vec::new();
        if req.route == RouteKind::V1Txs
            && tb.kind == crate::config::TargetKind::KdaTxs
            && sa == 200
            && sb == 200
        {
            let only_a = only_in_first(&va, &vb);
            if !only_a.is_empty()
                && only_a.len() <= GAP_CHECKS
                && only_in_first(&vb, &va).is_empty()
            {
                let account = crate::via_cwd::parse(&req.path_and_query).account;
                let mut all = true;
                for r in &only_a {
                    if !self.cwd_names_account(req, r, &account).await {
                        all = false;
                        break;
                    }
                }
                if all {
                    if let Value::Array(xa) = &mut va {
                        for r in &only_a {
                            if let Some(i) = xa.iter().position(|x| x == r) {
                                xa.remove(i);
                            }
                        }
                    }
                    kt_gap = only_a;
                }
            }
        }
        // chainweb-data lacks the events of whole height ranges (seen live: no coin.TRANSFER
        // at 7392632-7392963 with its tip at 7395650, so its pages skip them). A rows at a
        // height above B's lowest where B lists no row at all are checked on the node: a user
        // tx in A's block with the row's event (/poll), a coinbase row in the node's canonical
        // block. When all agree they leave the diff as the visible hit /~cwd-events-missing-node
        // and, both pages full, both sides are cut at the higher of their lowest heights (R3E-14).
        let mut ev_gap: Vec<Value> = Vec::new();
        if req.route == RouteKind::Events
            && tb.kind == crate::config::TargetKind::ChainwebData
            && sa == 200
            && sb == 200
        {
            let b_heights: std::collections::HashSet<u64> = match &vb {
                Value::Array(xb) => xb.iter().filter_map(row_height).collect(),
                _ => Default::default(),
            };
            let b_min = min_height(&vb);
            let gap: Vec<Value> = match &va {
                Value::Array(xa) => xa
                    .iter()
                    .filter(|r| {
                        row_height(r)
                            .is_some_and(|h| b_min.is_none_or(|m| h > m) && !b_heights.contains(&h))
                    })
                    .cloned()
                    .collect(),
                _ => Vec::new(),
            };
            if !gap.is_empty()
                && gap.len() <= NODE_GAP_CHECKS
                && self.node_confirms_events(req, &gap).await
            {
                let a_min0 = min_height(&va);
                if let Value::Array(xa) = &mut va {
                    xa.retain(|r| !gap.contains(r));
                }
                if full_a0 && full_b0 {
                    if let Some(cut) = a_min0.max(b_min) {
                        drop_at_or_below(&mut va, cut);
                        drop_at_or_below(&mut vb, cut);
                    }
                }
                ev_gap = gap;
            }
        }
        // Ruling R3E-14 (owner): where the node agrees with A, an oracle that differs is at
        // fault. Seen live: chainweb-data answers short account pages without a Chainweb-Next
        // (7590fd61... chain 0: 7 rows of 50; 6d87fd6e... heights 1151496-1153496: no rows, A
        // 35 rows in node block 6tpjetrL), and kda-txs answers a varying subset of /v1/txs
        // (acc28032.../coin/250: 63, then 54, 404, 47 rows; A 125). When both sides held their
        // full limit they are first cut at the higher of their lowest heights (rows below it
        // cannot be compared). Then, with no row only B lists, every row only A lists is polled
        // on the node: when each is in A's block and has an event naming the account, those
        // rows move to the visible hit /~cwd-missing-node or /~kdatxs-missing-node.
        let mut node_gap: Vec<Value> = Vec::new();
        let mut window_cut = 0usize;
        let node_gap_pair = matches!(
            (req.route, tb.kind),
            (RouteKind::Account, crate::config::TargetKind::ChainwebData)
                | (RouteKind::V1Txs, crate::config::TargetKind::KdaTxs)
        );
        if node_gap_pair && sa == 200 && sb == 200 && gap_rows.is_empty() && kt_gap.is_empty() {
            let (mut ca, mut cb) = (va.clone(), vb.clone());
            if full_a0 && full_b0 {
                if let (Some(x), Some(y)) = (min_height(&ca), min_height(&cb)) {
                    drop_at_or_below(&mut ca, x.max(y));
                    drop_at_or_below(&mut cb, x.max(y));
                }
            }
            // Rows match by (requestKey, blockHash, idx): value diffs of a shared row (F1, F2,
            // ...) stay diffs of their own and do not block this rule.
            let only_a = only_keys_in_first(&ca, &cb);
            let only_b = only_keys_in_first(&cb, &ca);
            let cut_rows = (len_of(&va) + len_of(&vb)) - (len_of(&ca) + len_of(&cb));
            if only_b.is_empty()
                && only_a.len() <= NODE_GAP_CHECKS
                && (cut_rows > 0 || !only_a.is_empty())
            {
                let account = query_account(req);
                let ok = only_a.is_empty()
                    || (!account.is_empty() && self.node_confirms(req, &only_a, &account).await);
                if ok {
                    if let Value::Array(xa) = &mut ca {
                        for r in &only_a {
                            if let Some(i) = xa.iter().position(|x| x == r) {
                                xa.remove(i);
                            }
                        }
                    }
                    window_cut = cut_rows;
                    va = ca;
                    vb = cb;
                    node_gap = only_a;
                }
            }
        }
        // chainweb-data keeps rows of orphaned blocks: on /txs/txs it lists them beside the
        // canonical row, and its /txs/tx answers 404 when a request key has several rows.
        // A's row must be one of B's (the node pair checks that A's block is canonical); B's
        // other rows go under /orphans (allowlist `orphan-rows-txs`).
        // A B row in the node's canonical block is no orphan: it stays a row A lacks (F4, seen
        // live: genesis request keys on every chain, chainweb-data listing all 20 rows and
        // compat one).
        if req.route == RouteKind::Txs && sa == 200 && sb == 200 {
            let mut other = Vec::new();
            if let (Value::Array(xa), Value::Array(xb)) = (&va, &mut vb) {
                let hashes: Vec<&Value> = xa.iter().filter_map(|r| r.get("blockHash")).collect();
                if !xa.is_empty() && xb.len() > xa.len() {
                    let (keep, o): (Vec<Value>, Vec<Value>) = xb
                        .drain(..)
                        .partition(|r| r.get("blockHash").is_some_and(|h| hashes.contains(&h)));
                    *xb = keep;
                    other = o;
                }
            }
            if !other.is_empty() {
                let (orph, canon) = self.split_by_node(other).await;
                orphans = orph;
                if !canon.is_empty() {
                    if let Value::Array(xb) = &mut vb {
                        xb.extend(canon);
                    }
                    normalize(&req.route, &mut vb);
                }
            }
        }
        let orphan_tx = if req.route == RouteKind::Tx && sa == 200 && sb == 404 {
            self.cwd_orphan_rows(req, tb, &va).await
        } else {
            None
        };
        // Genesis txs: compat serves them on /txs/tx and /txs/txs as chainweb-data does (G1,
        // F4), so a status difference there is a diff like any other.
        // The status difference is part of the explained orphan case.
        let status_explained = orphan_tx.is_some();
        // kda-txs writes decimals as JSON doubles (seen live: kdswap-gas-payer event amounts
        // whose exact decimal has more digits than an f64 holds); compare at f64 precision
        // (only here: the row rules above read heights and chains as integers).
        if tb.kind == crate::config::TargetKind::KdaTxs {
            numbers_as_f64(&mut va);
            numbers_as_f64(&mut vb);
        }
        // kda-txs answers a row's events differently from one fetch to the next (seen live:
        // d763d377.../coin/50, e4f0a329.../coin/50, the gas and main TRANSFER from/to and
        // amounts, gone on a refetch). Rows both sides list whose events differ are polled
        // on the node: when the node's events in A's block equal A's for every such row,
        // B's events are replaced by A's under the visible hit /~kdatxs-events-node (R3E-15).
        let mut ev_node: Vec<Value> = Vec::new();
        if req.route == RouteKind::V1Txs
            && tb.kind == crate::config::TargetKind::KdaTxs
            && sa == 200
            && sb == 200
        {
            ev_node = self.events_node_tiebreak(req, &va, &mut vb).await;
        }
        // /txs/search against chainweb-data: rows one side lacks, settled on the node
        // (R-P5P9-48).
        let search_rows = if req.route == RouteKind::Search
            && tb.kind == crate::config::TargetKind::ChainwebData
            && sa == 200
            && sb == 200
        {
            let full = |v: &Value| search_page_full(&req.path_and_query, len_of(v), page_cut);
            let full = (full(&va), full(&vb));
            self.search_oracle_rows(req, tb, &mut va, &mut vb, full)
                .await
        } else {
            SearchRows::default()
        };
        let (mut d, skew) = if let Some(rows) = orphan_tx {
            (
                vec![DiffEntry {
                    path: "/orphans".into(),
                    kind: DiffKind::Missing,
                    a: None,
                    b: Some(Value::Array(rows)),
                }],
                false,
            )
        } else if req.route == RouteKind::Recent {
            recent_diff(&va, &vb)
        } else if req.route == RouteKind::Search
            && tb.kind == crate::config::TargetKind::ChainwebData
            && req.path_and_query.contains("pactid=")
        {
            // chainweb-data orders pactid rows by (success, height) only; Postgres leaves ties
            // in any order (seen both ways live). Compare ties by request key on both sides.
            let key = |r: &Value| {
                (
                    r.get("result").and_then(Value::as_str) != Some("TxSucceeded"),
                    std::cmp::Reverse(r.get("height").and_then(Value::as_u64)),
                    r.get("requestKey")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                )
            };
            let mut reordered = false;
            for v in [&mut va, &mut vb] {
                if let Value::Array(x) = v {
                    let before = x.clone();
                    x.sort_by_key(key);
                    reordered |= *x != before;
                }
            }
            let mut d = diff_route(&req.route, &va, &vb);
            if reordered {
                d.push(DiffEntry {
                    path: PACTID_TIE_ORDER_PATH.into(),
                    kind: DiffKind::Changed,
                    a: None,
                    b: None,
                });
            }
            (d, false)
        } else if req.route == RouteKind::Search
            && tb.kind == crate::config::TargetKind::ChainwebData
            && req.path_and_query.contains("search=")
        {
            // S-3: chainweb-data also matches `cont` txs through their initial tx's code
            // (`code` null, `initialCode` set there); kda-indexer matches a tx's own exec code only. Drop those
            // rows, cut our page to the rows left, and report the visible hit.
            let mut dropped = 0usize;
            if let Value::Array(xb) = &mut vb {
                let before = xb.len();
                xb.retain(|r| {
                    !(r.get("code").is_some_and(Value::is_null)
                        && r.get("initialCode").is_some_and(|c| !c.is_null()))
                });
                dropped = before - xb.len();
                let n = xb.len();
                if dropped > 0 {
                    if let Value::Array(xa) = &mut va {
                        xa.truncate(n);
                    }
                }
            }
            let mut d = diff_route(&req.route, &va, &vb);
            if dropped > 0 {
                d.push(DiffEntry {
                    path: SEARCH_CONT_HISTORY_PATH.into(),
                    kind: DiffKind::Changed,
                    a: None,
                    b: Some(json!(dropped)),
                });
            }
            (d, false)
        } else {
            (diff_route(&req.route, &va, &vb), false)
        };
        // /txs/recent against chainweb-data: a continuation whose first step sits in a height
        // chainweb-data has not filled yet shows `initialCode`/`previousSteps` null there (seen
        // live on fresh cross-chain txs; filled later). Reported under the oracle-lag hit.
        if req.route == RouteKind::Recent && tb.kind == crate::config::TargetKind::ChainwebData {
            for e in d.iter_mut() {
                if (e.path.ends_with("/initialCode") || e.path.ends_with("/previousSteps"))
                    && e.b.as_ref().is_none_or(Value::is_null)
                {
                    e.path = format!("{ORACLE_LAG_PATH}{}", e.path);
                }
            }
        }
        if !node_gap.is_empty() || window_cut > 0 {
            let keys: Vec<Value> = node_gap
                .iter()
                .take(10)
                .map(|r| json!([r.get("chain"), r.get("height"), r.get("requestKey")]))
                .collect();
            let path = if tb.kind == crate::config::TargetKind::KdaTxs {
                KDATXS_MISSING_NODE_PATH
            } else {
                CWD_MISSING_NODE_PATH
            };
            d.push(DiffEntry {
                path: path.into(),
                kind: DiffKind::Extra,
                a: Some(json!({"rows": node_gap.len(), "window_cut": window_cut, "first": keys})),
                b: None,
            });
        }
        if !ev_gap.is_empty() {
            let first: Vec<Value> = ev_gap
                .iter()
                .take(10)
                .map(|r| json!([r.get("chain"), r.get("height"), r.get("requestKey")]))
                .collect();
            d.push(DiffEntry {
                path: CWD_EVENTS_GAP_PATH.into(),
                kind: DiffKind::Extra,
                a: Some(json!({"rows": ev_gap.len(), "first": first})),
                b: None,
            });
        }
        if !ev_node.is_empty() {
            d.push(DiffEntry {
                path: KDATXS_EVENTS_NODE_PATH.into(),
                kind: DiffKind::Changed,
                a: Some(json!({"rows": ev_node.len(), "first": &ev_node[..ev_node.len().min(10)]})),
                b: None,
            });
        }
        if !kt_gap.is_empty() {
            d.push(DiffEntry {
                path: KDATXS_GAP_PATH.into(),
                kind: DiffKind::Extra,
                a: Some(Value::Array(kt_gap)),
                b: None,
            });
        }
        if cursor_skip > 0 {
            d.push(DiffEntry {
                path: CWD_CURSOR_SKIP_PATH.into(),
                kind: DiffKind::Changed,
                a: Some(json!(cursor_skip)),
                b: None,
            });
        }
        if lag_rows > 0 && !replica {
            d.push(DiffEntry {
                path: ORACLE_LAG_PATH.into(),
                kind: DiffKind::Changed,
                a: Some(json!(lag_rows)),
                b: None,
            });
        }
        d.extend(coins_lag);
        if let Some((x, y)) = supply_lag {
            d.push(DiffEntry {
                path: format!("{ORACLE_LAG_PATH}/coinsInCirculation"),
                kind: DiffKind::Changed,
                a: Some(x),
                b: Some(y),
            });
        }
        if limit_edge > 0 {
            d.push(DiffEntry {
                path: LIMIT_EDGE_PATH.into(),
                kind: DiffKind::Changed,
                a: Some(json!(limit_edge)),
                b: None,
            });
        }
        if let Some(e) = extra {
            d.push(e);
        }
        if event_order > 0 {
            d.push(DiffEntry {
                path: EVENT_ORDER_PATH.into(),
                kind: DiffKind::Changed,
                a: Some(json!(event_order)),
                b: Some(json!(event_order)),
            });
        }
        if !gap_rows.is_empty() {
            d.push(DiffEntry {
                path: CWD_TRANSFERS_GAP_PATH.into(),
                kind: DiffKind::Extra,
                a: Some(Value::Array(gap_rows)),
                b: None,
            });
        }
        d.extend(search_rows.hits());
        if !orphans.is_empty() {
            d.push(DiffEntry {
                path: "/orphans".into(),
                kind: DiffKind::Missing,
                a: None,
                b: Some(Value::Array(orphans)),
            });
        }
        if pa.boundary_dups + pb.boundary_dups > 0 {
            d.push(DiffEntry {
                path: PAGE_DUP_PATH.into(),
                kind: DiffKind::Changed,
                a: Some(json!(pa.boundary_dups)),
                b: Some(json!(pb.boundary_dups)),
            });
        }
        if page_cut && !replica {
            d.push(DiffEntry {
                path: PAGE_CUT_PATH.into(),
                kind: DiffKind::Changed,
                a: Some(json!(pa.pages.len())),
                b: Some(json!(pb.pages.len())),
            });
        }
        if sa != sb && !status_explained {
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
        let curl_b = format!("curl -s {}", sh_url("B", &req.path_and_query));
        let b_body = trim_body(&pb.pages[0].body);
        let mut o = self.finish(
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
        );
        // Items on one side only (the sides read slightly different tips) are not a diff.
        if skew && o.class == "match" {
            o.class = "skew".into();
        }
        o
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
        // A pair of two kda-indexer targets (primary and replica) has no allowlist: the
        // entries explain differences from chainweb-data, the node or kda-txs, never between
        // two copies of this indexer. Only the route-intrinsic `/stats` tolerances apply
        // (the sides read different tips); they make the outcome `skew`.
        let replica_pair = ta.kind == crate::config::TargetKind::KdaIndexerCompat
            && self.targets.get(b_name).map(|t| t.kind)
                == Some(crate::config::TargetKind::KdaIndexerCompat);
        let mut replica_skew = false;
        let (unexplained, ids) = if replica_pair {
            let (rest, skew) = split_replica_skew(d);
            replica_skew = skew;
            (rest, Vec::new())
        } else {
            let a_headers: Vec<(String, String)> =
                pa.pages.iter().flat_map(|c| c.headers.clone()).collect();
            self.allow
                .classify_request(&req.route, &req.path_and_query, &a_headers, &d)
        };
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
        } else if replica_skew {
            "skew"
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

    /// The balances pair (plan Task 12, H-1): A is `/api/v1/accounts/{a}/balances?token&chain`
    /// on kda-indexer native, B is `({token}.details (read-msg "a"))` on the node's `/local`
    /// (the same read `kdx_node::local` makes). Balances compare as decimal values, guards as
    /// JSON. A difference on a row that is `stale`, or whose `as_of_height` is more than 2 below
    /// the node's read height, is retried once after 60 s and then reported as `skew`.
    async fn balances_vs_local(&self, req: &Request, ta: &Target) -> Outcome {
        let Ok(u) = url::Url::parse(&format!("http://h{}", req.path_and_query)) else {
            return Self::simple("bad-request");
        };
        let account = u
            .path_segments()
            .and_then(|mut s| s.nth(3).map(str::to_owned))
            .map(|a| percent_decode(&a))
            .unwrap_or_default();
        let q: std::collections::HashMap<String, String> = u
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let (Some(token), Some(Ok(chain))) =
            (q.get("token"), q.get("chain").map(|c| c.parse::<u8>()))
        else {
            return Self::simple("bad-request");
        };
        let local = kdx_node::LocalClient::new(
            self.node.clone(),
            kdx_node::LocalOpts {
                rps: self.cfg.node_max_rps.max(1),
                concurrency: 1,
            },
        );
        let mut attempt = 0;
        loop {
            attempt += 1;
            let pa = match self.fetch_a(req, ta).await {
                Ok(p) => p,
                Err(e) => return self.a_error(req, ta, "node_local", None, &format!("{e:#}")),
            };
            if pa.pages[0].status >= 500 {
                return self.a_error(req, ta, "node_local", Some(&pa), "5xx");
            }
            let rows = pa
                .items
                .get("items")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let row = rows.iter().find(|r| {
                r.get("module").and_then(Value::as_str) == Some(token.as_str())
                    && r.get("chain").and_then(Value::as_u64) == Some(u64::from(chain))
            });
            let b = local.details(chain, token, &account).await;
            let b_text = match &b {
                Ok(kdx_node::Local::Found { value, height }) => {
                    format!("{{\"balance\":{:?},\"height\":{height}}}", value.balance)
                }
                Ok(kdx_node::Local::Absent) => "absent".to_string(),
                // Pact failures and node errors give no answer to compare (a token module
                // without `details`): not a diff.
                Err(e) => {
                    return self.b_error(
                        req,
                        ta,
                        "node_local",
                        Some(&pa),
                        &format!("{e}"),
                        String::new(),
                    )
                }
            };
            let (diff, uncertain) = match &b {
                Ok(l) => balance_diff(row, l, &|h| self.graph.chains_at(h).len()),
                Err(_) => (Vec::new(), false),
            };
            if diff.is_empty() {
                return Self::simple("match");
            }
            if uncertain && attempt == 1 {
                tokio::time::sleep(Duration::from_secs(60)).await;
                continue;
            }
            if uncertain {
                let rec = self.record(
                    req,
                    ta,
                    "node_local",
                    Some(&pa),
                    None,
                    b_text,
                    diff,
                    "skew",
                    &[],
                    String::new(),
                    Some("balance differs from /local past the skew allowance".into()),
                );
                return Outcome {
                    class: "skew".into(),
                    record: Some(rec),
                };
            }
            return self.finish(
                req,
                ta,
                "node_local",
                &pa,
                None,
                b_text,
                diff,
                false,
                String::new(),
                None,
            );
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

    /// `rk` is validated as base64url by the caller; everything is shell-quoted anyway.
    fn node_curl_poll(&self, chain: u8, rk: &str) -> String {
        let body = serde_json::json!({ "requestKeys": [rk] }).to_string();
        format!(
            "curl -s -X POST -H 'content-type: application/json' -d {} {}",
            sh_quote(&body),
            sh_url(
                "B",
                &format!(
                    "/chainweb/0.0/{}/chain/{chain}/pact/api/v1/poll",
                    self.cfg.network
                )
            )
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
        if !crate::gen::is_request_key(rk) {
            return Self::simple("bad-request-key");
        }
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
            Err(e) => return self.b_error(req, ta, "node", Some(pa), &format!("{e:#}"), curl_b),
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
        // No height in the poll answer: nothing to anchor the oracle to (never default to 0).
        let Some(height) = entry
            .pointer("/metaData/blockHeight")
            .and_then(Value::as_u64)
        else {
            return Self::simple("no-oracle");
        };
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
        // /poll reports the block the node saw the tx in; if that is not the canonical header
        // at the height any more, the tx was reorged.
        let poll_hash = entry.pointer("/metaData/blockHash").and_then(Value::as_str);
        let canon_hash = header.get("hash").and_then(Value::as_str);
        if poll_hash.is_some() && poll_hash != canon_hash {
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
                Some(format!(
                    "poll blockHash {} is not the canonical block {} at height {height}",
                    poll_hash.unwrap_or_default(),
                    canon_hash.unwrap_or_default()
                )),
            );
            return Outcome {
                class: "reorg".into(),
                record: Some(rec),
            };
        }
        let tx = match self.payload_tx(chain, &header, rk).await {
            Ok(t) => t,
            Err(e) => return self.b_error(req, ta, "node", Some(pa), &format!("{e:#}"), curl_b),
        };
        let mut vb = oracle::from_poll(&entry, &header, tx.as_ref());
        let block_hash = vb.get("blockHash").cloned().unwrap_or(Value::Null);
        // A side: the canonical row (for /txs/txs, other rows go under /orphans; copies of a
        // genesis tx in other chains' genesis blocks are not orphans, F4).
        let mut va = match (&req.route, a_body) {
            (RouteKind::Txs, Value::Array(rows)) => {
                let (canon, orphans): (Vec<&Value>, Vec<&Value>) = rows
                    .iter()
                    .filter(|r| !self.genesis_copy(r, chain))
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
            // A does not have the tx (or only orphan rows). A genesis tx whose request key A
            // stores on several chains is a 404 on /txs/tx, as in chainweb-data (F4): checked
            // on A's /txs/txs, it gets its own path so the allowlist can name it.
            let genesis = height == 0 || !self.graph.chains_at(height - 1).contains(&chain);
            let multi = genesis
                && req.route == RouteKind::Tx
                && self.a_genesis_multi(req, ta, rk, chain, &block_hash).await;
            vec![DiffEntry {
                path: if multi {
                    GENESIS_MULTI_PATH.into()
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
        let b = crate::gen::random::decode_checked(header, &payload)?;
        Ok(b.txs.into_iter().find(|t| t.hash.to_b64url() == rk))
    }

    /// Whether chainweb-data's own `/txs/tx` for the row's request key, in the row's block,
    /// carries the row's `{token}.TRANSFER` event from `fromAccount` to `toAccount`.
    /// chainweb-data's `/txs/tx` for `row`'s request key is in the same block and has an
    /// event whose params name `account`.
    async fn cwd_names_account(&self, req: &Request, row: &Value, account: &str) -> bool {
        let Some(cwd) = self
            .targets
            .values()
            .find(|t| t.kind == crate::config::TargetKind::ChainwebData)
        else {
            return false;
        };
        let Some(rk) = row.get("requestKey").and_then(Value::as_str) else {
            return false;
        };
        self.count_request(req.route.as_str(), &cwd.name, 1);
        let Ok(page) = cwd.get(&format!("/txs/tx?requestkey={rk}")).await else {
            return false;
        };
        let Some(tx) = page.json().filter(|_| page.status == 200) else {
            return false;
        };
        if tx.get("blockHash") != row.get("blockHash") {
            return false;
        }
        tx.get("events")
            .and_then(Value::as_array)
            .is_some_and(|evs| {
                evs.iter().any(|e| {
                    e.get("params")
                        .and_then(Value::as_array)
                        .is_some_and(|p| p.iter().any(|x| x.as_str() == Some(account)))
                })
            })
    }

    /// Whether the node's `/poll` puts every row's request key in the row's block, with an
    /// event whose params name `account` (ruling R3E-14). Coinbase rows are not confirmed.
    async fn node_confirms(&self, req: &Request, rows: &[Value], account: &str) -> bool {
        let mut by_chain: HashMap<u8, Vec<(String, String)>> = HashMap::new();
        for r in rows {
            let s = |k: &str| r.get(k).and_then(Value::as_str).map(str::to_string);
            let (Some(rk), Some(bh), Some(c)) = (
                s("requestKey"),
                s("blockHash"),
                r.get("chain").and_then(Value::as_u64),
            ) else {
                return false;
            };
            if rk == "<coinbase>" {
                return false;
            }
            let Ok(c) = u8::try_from(c) else {
                return false;
            };
            by_chain.entry(c).or_default().push((rk, bh));
        }
        for (c, keys) in by_chain {
            for chunk in keys.chunks(NODE_POLL_CHUNK) {
                let rks: Vec<String> = chunk.iter().map(|(rk, _)| rk.clone()).collect();
                self.count_request(req.route.as_str(), "node", 1);
                let Ok(res) = self.node.poll(c, &rks).await else {
                    return false;
                };
                for (rk, bh) in chunk {
                    let Some(v) = res.get(rk) else {
                        return false;
                    };
                    let in_block = v.pointer("/metaData/blockHash").and_then(Value::as_str)
                        == Some(bh.as_str());
                    let names = v
                        .get("events")
                        .and_then(Value::as_array)
                        .is_some_and(|evs| {
                            evs.iter().any(|e| {
                                e.get("params")
                                    .and_then(Value::as_array)
                                    .is_some_and(|p| p.iter().any(|x| x.as_str() == Some(account)))
                            })
                        });
                    if !(in_block && names) {
                        return false;
                    }
                }
            }
        }
        true
    }

    async fn cwd_has_transfer(&self, req: &Request, tb: &Target, row: &Value) -> bool {
        if tb.kind != crate::config::TargetKind::ChainwebData {
            return false;
        }
        let s = |k: &str| row.get(k).and_then(Value::as_str);
        let (Some(rk), Some(token)) = (s("requestKey"), s("token")) else {
            return false;
        };
        if rk == "<coinbase>" {
            return false;
        }
        self.count_request(req.route.as_str(), &tb.name, 1);
        let Ok(page) = tb.get(&format!("/txs/tx?requestkey={rk}")).await else {
            return false;
        };
        let Some(tx) = page.json().filter(|_| page.status == 200) else {
            return false;
        };
        if tx.get("blockHash") != row.get("blockHash") {
            return false;
        }
        let name = format!("{token}.TRANSFER");
        let accts = |v: Option<&Value>| match v {
            Some(Value::String(a)) => a.clone(),
            _ => String::new(),
        };
        tx.get("events")
            .and_then(Value::as_array)
            .is_some_and(|evs| {
                evs.iter().any(|e| {
                    let p = e.get("params").and_then(Value::as_array);
                    e.get("name").and_then(Value::as_str) == Some(name.as_str())
                        && p.is_some_and(|p| {
                            accts(p.first()) == accts(row.get("fromAccount"))
                                && accts(p.get(1)) == accts(row.get("toAccount"))
                        })
                })
            })
    }

    /// Moves B rows that list one of A's request keys in another block out of `vb` when the
    /// node confirms A's block as canonical at that chain and height; an A row left without
    /// a B counterpart by that (B lists only the orphan copy) leaves `va` as well. Returns
    /// the moved B rows.
    /// chainweb-data's tip: the max height on its `/txs/recent`, read at most once a minute.
    async fn oracle_tip(&self, tb: &Target) -> Option<u64> {
        if let Some((t, h)) = self.oracle_tips.lock().expect("tips").get(&tb.name) {
            if t.elapsed() < ORACLE_TIP_TTL {
                return Some(*h);
            }
        }
        self.count_request("oracle-tip", &tb.name, 1);
        let cap = tb.get("/txs/recent").await.ok()?;
        if cap.status != 200 {
            return None;
        }
        let h = cap
            .json()?
            .as_array()?
            .iter()
            .filter_map(row_height)
            .max()?;
        self.oracle_tips
            .lock()
            .expect("tips")
            .insert(tb.name.clone(), (std::time::Instant::now(), h));
        Some(h)
    }

    pub(crate) async fn oracle_orphans(&self, va: &mut Value, vb: &mut Value) -> Vec<Value> {
        let (Value::Array(xa), Value::Array(xb)) = (&mut *va, &mut *vb) else {
            return Vec::new();
        };
        let s = |r: &Value, k: &str| r.get(k).and_then(Value::as_str).map(str::to_string);
        let mut a_block: HashMap<String, (String, u8, u64)> = HashMap::new();
        // A's block per (chain, height): oracle rows at an A height in another block (coinbase
        // rows carry no request key to match) are orphan candidates too.
        let mut a_at: HashMap<(u8, u64), String> = HashMap::new();
        for r in xa.iter() {
            if let (Some(rk), Some(bh), Some(c), Some(h)) = (
                s(r, "requestKey"),
                s(r, "blockHash"),
                r.get("chain").and_then(Value::as_u64),
                r.get("height").and_then(Value::as_u64),
            ) {
                a_at.insert((c as u8, h), bh.clone());
                if rk != "<coinbase>" {
                    a_block.insert(rk, (bh, c as u8, h));
                }
            }
        }
        // Canonical hash per (chain, height), shared by both rules; at most ORPHAN_CHECKS
        // lookups per compare.
        let mut canon_at: HashMap<(u8, u64), Option<String>> = HashMap::new();
        let mut checks = 0usize;
        let mut moved = Vec::new();
        let mut i = 0;
        while i < xb.len() {
            let r = &xb[i];
            let at = (
                r.get("chain").and_then(Value::as_u64).map(|c| c as u8),
                r.get("height").and_then(Value::as_u64),
            );
            let hit = match (s(r, "requestKey"), s(r, "blockHash")) {
                (Some(rk), Some(bh)) => a_block
                    .get(&rk)
                    .filter(|(abh, _, _)| *abh != bh)
                    .cloned()
                    .or_else(|| match at {
                        (Some(c), Some(h)) => a_at
                            .get(&(c, h))
                            .filter(|abh| **abh != bh)
                            .map(|abh| (abh.clone(), c, h)),
                        _ => None,
                    }),
                _ => None,
            };
            let Some((abh, c, h)) = hit else {
                i += 1;
                continue;
            };
            let ok = self
                .canon_cached(&mut canon_at, &mut checks, c, h)
                .await
                .is_some_and(|ch| ch == Some(abh));
            if ok {
                moved.push(xb.remove(i));
            } else {
                i += 1;
            }
        }
        // B rows (coinbase or not) at a (chain, height) where A lists no row: an orphan when
        // the node's canonical block there is another one (seen live: 99cb7008...?chain=1,
        // chainweb-data coinbase row in T6y_F81b at chain 1 height 7392440, canonical
        // 3J36InYO, whose coinbase went to another miner; k:b649fd12...?token=coin, a failed
        // tx's gas TRANSFER in ytgyQjrM at chain 0 height 6894865, canonical TDOizl_M, request
        // key unknown to the node's /poll on every chain).
        let mut j = 0;
        while j < xb.len() {
            let r = &xb[j];
            let (Some(c), Some(h), Some(bh)) = (
                r.get("chain").and_then(Value::as_u64).map(|c| c as u8),
                r.get("height").and_then(Value::as_u64),
                s(r, "blockHash"),
            ) else {
                j += 1;
                continue;
            };
            if a_at.contains_key(&(c, h)) {
                j += 1;
                continue;
            }
            let canon_hash = self
                .canon_cached(&mut canon_at, &mut checks, c, h)
                .await
                .flatten();
            if canon_hash.is_some_and(|ch| ch != bh) {
                moved.push(xb.remove(j));
            } else {
                j += 1;
            }
        }
        // A rows whose only B copy was an orphan.
        for o in &moved {
            let rk = s(o, "requestKey");
            if rk.as_deref() == Some("<coinbase>") {
                continue;
            }
            let still = xb.iter().any(|r| s(r, "requestKey") == rk);
            if !still {
                xa.retain(|r| s(r, "requestKey") != rk);
            }
        }
        moved
    }

    /// The node's canonical block hash at `(c, h)` (`header/branch` up to the cut, never the
    /// plain header range, which also returns orphans), from `cache`, from the cache shared by
    /// all compares, or one lookup while fewer than ORPHAN_CHECKS were made; `None` when the
    /// budget is spent. The node client applies `node_max_rps`.
    async fn canon_cached(
        &self,
        cache: &mut HashMap<(u8, u64), Option<String>>,
        checks: &mut usize,
        c: u8,
        h: u64,
    ) -> Option<Option<String>> {
        if let Some(v) = cache.get(&(c, h)) {
            return Some(v.clone());
        }
        let known = self.canon.lock().expect("canon").get(&(c, h)).cloned();
        if let Some(v) = known {
            cache.insert((c, h), Some(v.clone()));
            return Some(Some(v));
        }
        if *checks >= ORPHAN_CHECKS {
            return None;
        }
        *checks += 1;
        self.count_request("orphan-check", "node", 1);
        let v = canonical_header(&self.node, &self.tips, c, h)
            .await
            .ok()
            .flatten()
            .and_then(|hd| hd.get("hash").and_then(Value::as_str).map(str::to_string));
        if let Some(hash) = &v {
            if self
                .tips
                .cut_height(c)
                .is_some_and(|top| h.saturating_add(CANON_CACHE_DEPTH) <= top)
            {
                let mut g = self.canon.lock().expect("canon");
                if g.len() >= CANON_CACHE_MAX {
                    g.clear();
                }
                g.insert((c, h), hash.clone());
            }
        }
        cache.insert((c, h), v.clone());
        Some(v)
    }

    /// `/txs/search` against chainweb-data (R-P5P9-48), rows one side lists and the other
    /// lacks (by request key and block). First, when a side held a full page, rows at or past
    /// its last row's place in the answer order leave both sides (the next rows of that side
    /// were not fetched). Then:
    /// - a chainweb-data row leaves `vb` as an orphan when the node's canonical block at its
    ///   (chain, height), on the cut's branch, is another one (seen live in run 6E-3: 23
    ///   records, e.g. chain 2 height 7399318, chainweb-data block oC2lnmOL, canonical
    ///   OHz-ZmnP). `search=` rows chainweb-data matched through a `cont` tx's initial code
    ///   are left to S-3.
    /// - an A row above chainweb-data's tip is oracle lag (seen live: 5unn... chain 0, B's
    ///   newest row 60-150 heights below A's); below it, oracle gap when the node's `/poll`
    ///   puts the tx in A's block and that block is canonical (the 2026-11-10 hole).
    ///
    /// Rows the node cannot settle stay diffs.
    async fn search_oracle_rows(
        &self,
        req: &Request,
        tb: &Target,
        va: &mut Value,
        vb: &mut Value,
        (full_a, full_b): (bool, bool),
    ) -> SearchRows {
        let mut out = SearchRows::default();
        let (Value::Array(xa), Value::Array(xb)) = (&mut *va, &mut *vb) else {
            return out;
        };
        let pactid = req.path_and_query.contains("pactid=");
        let place = |r: &Value| search_place(pactid, r);
        let bound = [(full_a, xa.last()), (full_b, xb.last())]
            .into_iter()
            .filter_map(|(full, last)| if full { last.map(place) } else { None })
            .min();
        if let Some(bound) = bound {
            let (na, nb) = (xa.len(), xb.len());
            xa.retain(|r| place(r) < bound);
            xb.retain(|r| place(r) < bound);
            out.a_cut = na - xa.len();
            out.b_cut = nb - xb.len();
        }
        let s = |r: &Value, k: &str| r.get(k).and_then(Value::as_str).map(str::to_string);
        let key = |r: &Value| (s(r, "requestKey"), s(r, "blockHash"));
        let cont_history = |r: &Value| {
            req.path_and_query.contains("search=")
                && r.get("code").is_some_and(Value::is_null)
                && r.get("initialCode").is_some_and(|c| !c.is_null())
        };
        let mut cache: HashMap<(u8, u64), Option<String>> = HashMap::new();
        let mut checks = 0usize;
        let a_keys: std::collections::HashSet<_> = xa.iter().map(key).collect();
        let mut i = 0;
        while i < xb.len() {
            let r = &xb[i];
            let at = (
                row_chain(r),
                r.get("height").and_then(Value::as_u64),
                s(r, "blockHash"),
            );
            let (Some(c), Some(h), Some(bh)) = at else {
                i += 1;
                continue;
            };
            if a_keys.contains(&key(r)) || cont_history(r) {
                i += 1;
                continue;
            }
            let canon = self
                .canon_cached(&mut cache, &mut checks, c, h)
                .await
                .flatten();
            if canon.is_some_and(|ch| ch != bh) {
                out.orphans.push(xb.remove(i));
            } else {
                i += 1;
            }
        }
        let b_keys: std::collections::HashSet<_> = xb.iter().map(key).collect();
        let only_a: Vec<usize> = (0..xa.len())
            .filter(|&i| !b_keys.contains(&key(&xa[i])))
            .collect();
        if only_a.is_empty() {
            return out;
        }
        out.b_tip = self.oracle_tip(tb).await;
        let mut lag = Vec::new();
        let mut asked = Vec::new();
        for i in only_a {
            let h = xa[i].get("height").and_then(Value::as_u64);
            if h.zip(out.b_tip).is_some_and(|(h, t)| h > t) {
                lag.push(i);
            } else {
                asked.push(i);
            }
        }
        let rows: Vec<&Value> = asked.iter().map(|&i| &xa[i]).collect();
        let ok = self
            .node_in_canonical_block(req, &rows, &mut cache, &mut checks)
            .await;
        let gap: Vec<usize> = asked
            .iter()
            .zip(ok)
            .filter_map(|(&i, ok)| ok.then_some(i))
            .collect();
        let mut gone: Vec<(usize, bool)> = lag
            .into_iter()
            .map(|i| (i, true))
            .chain(gap.into_iter().map(|i| (i, false)))
            .collect();
        gone.sort_unstable();
        for &(i, is_lag) in gone.iter().rev() {
            let r = xa.remove(i);
            if is_lag {
                out.lag.insert(0, r);
            } else {
                out.gap.insert(0, r);
            }
        }
        out
    }

    /// Per row: the node's `/poll` puts its request key in the row's block, and that block is
    /// the canonical one at the row's (chain, height) on the cut's branch. At most
    /// NODE_GAP_CHECKS rows are asked, in `/poll` calls of NODE_POLL_CHUNK; the others, and
    /// rows the node does not answer, are not confirmed.
    async fn node_in_canonical_block(
        &self,
        req: &Request,
        rows: &[&Value],
        cache: &mut HashMap<(u8, u64), Option<String>>,
        checks: &mut usize,
    ) -> Vec<bool> {
        let s = |r: &Value, k: &str| r.get(k).and_then(Value::as_str).map(str::to_string);
        let mut ok = vec![false; rows.len()];
        let mut by_chain: std::collections::BTreeMap<u8, Vec<usize>> = Default::default();
        for (i, r) in rows.iter().enumerate().take(NODE_GAP_CHECKS) {
            if let (Some(c), Some(_), Some(_)) =
                (row_chain(r), s(r, "requestKey"), s(r, "blockHash"))
            {
                by_chain.entry(c).or_default().push(i);
            }
        }
        for (c, idx) in by_chain {
            for chunk in idx.chunks(NODE_POLL_CHUNK) {
                let rks: Vec<String> = chunk
                    .iter()
                    .filter_map(|&i| s(rows[i], "requestKey"))
                    .collect();
                self.count_request(req.route.as_str(), "node", 1);
                let Ok(res) = self.node.poll(c, &rks).await else {
                    continue;
                };
                for &i in chunk {
                    let r = rows[i];
                    let (Some(rk), Some(bh), Some(h)) = (
                        s(r, "requestKey"),
                        s(r, "blockHash"),
                        r.get("height").and_then(Value::as_u64),
                    ) else {
                        continue;
                    };
                    let polled = res
                        .get(&rk)
                        .and_then(|v| v.pointer("/metaData/blockHash"))
                        .and_then(Value::as_str);
                    if polled != Some(bh.as_str()) {
                        continue;
                    }
                    let canon = self.canon_cached(cache, checks, c, h).await.flatten();
                    ok[i] = canon.as_deref() == Some(bh.as_str());
                }
            }
        }
        ok
    }

    /// Splits oracle rows into (orphans, canonical): a row is canonical when the node's
    /// canonical block at its (chain, height) is the row's block. Rows the node cannot
    /// confirm either way stay orphans.
    async fn split_by_node(&self, rows: Vec<Value>) -> (Vec<Value>, Vec<Value>) {
        let mut cache = HashMap::new();
        let mut checks = 0usize;
        let (mut orph, mut canon) = (Vec::new(), Vec::new());
        for r in rows {
            let at = (
                r.get("chain")
                    .and_then(Value::as_u64)
                    .and_then(|c| u8::try_from(c).ok()),
                r.get("height").and_then(Value::as_u64),
                r.get("blockHash").and_then(Value::as_str),
            );
            let is_canon = match at {
                (Some(c), Some(h), Some(bh)) => self
                    .canon_cached(&mut cache, &mut checks, c, h)
                    .await
                    .flatten()
                    .is_some_and(|ch| ch == bh),
                _ => false,
            };
            if is_canon {
                canon.push(r);
            } else {
                orph.push(r);
            }
        }
        (orph, canon)
    }

    /// A row of another chain's genesis block: a copy of a genesis tx whose request key
    /// repeats across chains (F4), not an orphan of `chain`.
    fn genesis_copy(&self, r: &Value, chain: u8) -> bool {
        r.get("chain").and_then(Value::as_u64) != Some(u64::from(chain)) && self.is_genesis_row(r)
    }

    /// A's `/txs/txs` lists `rk` in the canonical genesis block of `chain` and in at least
    /// one other chain's genesis block (F4: then its `/txs/tx` answers 404, as
    /// chainweb-data's does for a request key with several rows).
    async fn a_genesis_multi(
        &self,
        req: &Request,
        ta: &Target,
        rk: &str,
        chain: u8,
        block_hash: &Value,
    ) -> bool {
        self.count_request(req.route.as_str(), &ta.name, 1);
        let Ok(page) = ta.get(&format!("/txs/txs?requestkey={rk}")).await else {
            return false;
        };
        let Some(Value::Array(rows)) = page.json() else {
            return false;
        };
        page.status == 200
            && rows.len() >= 2
            && rows.iter().all(|r| self.is_genesis_row(r))
            && rows.iter().any(|r| {
                r.get("chain").and_then(Value::as_u64) == Some(u64::from(chain))
                    && r.get("blockHash") == Some(block_hash)
            })
    }

    /// Node tie-break of per-row events against kda-txs (R3E-15). Rows both sides list (by
    /// request key, block and idx) whose `events` differ are polled on the node (at most
    /// EVENT_NODE_CHECKS rows). When for every such row the node has the tx in A's block
    /// with A's events, B's events are set to A's and `[chain, height, requestKey]` of each
    /// row is returned; otherwise nothing changes.
    async fn events_node_tiebreak(&self, req: &Request, va: &Value, vb: &mut Value) -> Vec<Value> {
        let (Value::Array(xa), Value::Array(xb)) = (va, &mut *vb) else {
            return Vec::new();
        };
        let mut a_by: HashMap<RowKey, &Value> = HashMap::new();
        for r in xa {
            a_by.entry(row_key(r)).or_insert(r);
        }
        let mut cand: Vec<(usize, &Value)> = Vec::new();
        for (i, rb) in xb.iter().enumerate() {
            if let Some(ra) = a_by.get(&row_key(rb)) {
                if ra.get("events") != rb.get("events") {
                    cand.push((i, ra));
                }
            }
        }
        if cand.is_empty() || cand.len() > EVENT_NODE_CHECKS {
            return Vec::new();
        }
        let s = |r: &Value, k: &str| r.get(k).and_then(Value::as_str).map(str::to_string);
        let mut by_chain: HashMap<u8, Vec<String>> = HashMap::new();
        for (_, ra) in &cand {
            let (Some(rk), Some(c)) = (s(ra, "requestKey"), row_chain(ra)) else {
                return Vec::new();
            };
            by_chain.entry(c).or_default().push(rk);
        }
        let mut node: HashMap<String, Value> = HashMap::new();
        for (c, rks) in by_chain {
            for chunk in rks.chunks(NODE_POLL_CHUNK) {
                self.count_request(req.route.as_str(), "node", 1);
                let Ok(res) = self.node.poll(c, chunk).await else {
                    return Vec::new();
                };
                for rk in chunk {
                    let Some(v) = res.get(rk) else {
                        return Vec::new();
                    };
                    node.insert(rk.clone(), v.clone());
                }
            }
        }
        for (_, ra) in &cand {
            let rk = s(ra, "requestKey").unwrap_or_default();
            let Some(n) = node.get(&rk) else {
                return Vec::new();
            };
            let in_block = n.pointer("/metaData/blockHash").and_then(Value::as_str)
                == s(ra, "blockHash").as_deref();
            let mut ne = oracle::v1txs_events(n);
            numbers_as_f64(&mut ne);
            let mut ae = ra.get("events").cloned().unwrap_or(Value::Null);
            numbers_as_f64(&mut ae);
            if !in_block || !same_events(&ae, &ne) {
                return Vec::new();
            }
        }
        let mut out = Vec::new();
        let fixes: Vec<(usize, Value)> = cand
            .iter()
            .map(|(i, ra)| (*i, ra.get("events").cloned().unwrap_or(Value::Null)))
            .collect();
        for (i, ev) in fixes {
            let r = &mut xb[i];
            out.push(json!([
                r.get("chain"),
                r.get("height"),
                r.get("requestKey")
            ]));
            if let Some(m) = r.as_object_mut() {
                m.insert("events".into(), ev);
            }
        }
        out
    }

    /// Event rows (`/txs/events` shape) confirmed on the node: a user tx is in the row's
    /// block on `/poll` with an event of the row's name and params; a coinbase row's block is
    /// the node's canonical block at its (chain, height).
    async fn node_confirms_events(&self, req: &Request, rows: &[Value]) -> bool {
        let s = |r: &Value, k: &str| r.get(k).and_then(Value::as_str).map(str::to_string);
        let mut by_chain: HashMap<u8, Vec<&Value>> = HashMap::new();
        let mut cache = HashMap::new();
        let mut checks = 0usize;
        for r in rows {
            let (Some(rk), Some(bh), Some(c), Some(h)) = (
                s(r, "requestKey"),
                s(r, "blockHash"),
                row_chain(r),
                row_height(r),
            ) else {
                return false;
            };
            if rk == "<coinbase>" {
                let canon = self.canon_cached(&mut cache, &mut checks, c, h).await;
                if canon.flatten().as_deref() != Some(bh.as_str()) {
                    return false;
                }
            } else {
                by_chain.entry(c).or_default().push(r);
            }
        }
        for (c, rs) in by_chain {
            let mut rks: Vec<String> = rs.iter().filter_map(|r| s(r, "requestKey")).collect();
            rks.sort();
            rks.dedup();
            let mut res = serde_json::Map::new();
            for chunk in rks.chunks(NODE_POLL_CHUNK) {
                self.count_request(req.route.as_str(), "node", 1);
                let Ok(m) = self.node.poll(c, chunk).await else {
                    return false;
                };
                res.extend(m);
            }
            for r in rs {
                let rk = s(r, "requestKey").unwrap_or_default();
                let Some(v) = res.get(&rk) else {
                    return false;
                };
                if v.pointer("/metaData/blockHash").and_then(Value::as_str)
                    != s(r, "blockHash").as_deref()
                {
                    return false;
                }
                let mut want = json!({"name": r.get("name"), "params": r.get("params")});
                numbers_as_f64(&mut want);
                let mut evs = oracle::v1txs_events(v);
                numbers_as_f64(&mut evs);
                if !evs.as_array().is_some_and(|a| a.contains(&want)) {
                    return false;
                }
            }
        }
        true
    }

    /// `/txs/recent` rows of B whose request key A lists in another block, when the node's
    /// canonical block at A's (chain, height) is A's: each is replaced by A's row and returned.
    async fn recent_orphans(&self, va: &Value, vb: &mut Value) -> Vec<Value> {
        let (Value::Array(xa), Value::Array(xb)) = (va, &mut *vb) else {
            return Vec::new();
        };
        let s = |r: &Value, k: &str| r.get(k).and_then(Value::as_str).map(str::to_string);
        let mut cache = HashMap::new();
        let mut checks = 0usize;
        let mut moved = Vec::new();
        for rb in xb.iter_mut() {
            let Some(ra) = xa.iter().find(|ra| {
                s(ra, "requestKey").is_some() && s(ra, "requestKey") == s(rb, "requestKey")
            }) else {
                continue;
            };
            let (Some(abh), Some(bbh), Some(c), Some(h)) = (
                s(ra, "blockHash"),
                s(rb, "blockHash"),
                row_chain(ra),
                row_height(ra),
            ) else {
                continue;
            };
            if abh == bbh {
                continue;
            }
            let canon = self
                .canon_cached(&mut cache, &mut checks, c, h)
                .await
                .flatten();
            if canon.as_deref() == Some(abh.as_str()) {
                moved.push(std::mem::replace(rb, ra.clone()));
            }
        }
        moved
    }

    /// A row of a genesis block: height 0, or the first height of a chain added later.
    fn is_genesis_row(&self, r: &Value) -> bool {
        let h = r.get("height").and_then(Value::as_u64);
        let c = r.get("chain").and_then(Value::as_u64);
        match (h, c) {
            (Some(0), Some(_)) => true,
            (Some(h), Some(c)) => {
                u8::try_from(c).is_ok_and(|c| !self.graph.chains_at(h - 1).contains(&c))
            }
            _ => false,
        }
    }

    /// chainweb-data's `/txs/tx` answers 404 when it holds several rows for a request key
    /// (orphaned blocks). One extra `/txs/txs` call on B: when it lists A's block plus other
    /// blocks, those other rows are returned (they go under `/orphans`).
    async fn cwd_orphan_rows(&self, req: &Request, tb: &Target, va: &Value) -> Option<Vec<Value>> {
        if tb.kind != crate::config::TargetKind::ChainwebData {
            return None;
        }
        let a_hash = va.get("blockHash")?.clone();
        let (_, q) = req.path_and_query.split_once('?')?;
        let rk = q.split('&').find_map(|p| p.strip_prefix("requestkey="))?;
        self.count_request(req.route.as_str(), &tb.name, 1);
        let page = tb.get(&format!("/txs/txs?requestkey={rk}")).await.ok()?;
        let Some(Value::Array(rows)) = page.json() else {
            return None;
        };
        if page.status != 200 || rows.len() < 2 {
            return None;
        }
        let (mine, other): (Vec<Value>, Vec<Value>) = rows
            .into_iter()
            .partition(|r| r.get("blockHash") == Some(&a_hash));
        if mine.is_empty() || other.is_empty() {
            return None;
        }
        // A row in the node's canonical block is no orphan: A should list it too (F4).
        let (orph, canon) = self.split_by_node(other).await;
        canon.is_empty().then_some(orph)
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
        let body = format!("{{\"lower\":[],\"upper\":[\"<cut hash of chain {chain}>\"]}}");
        let curl_b = format!(
            "curl -s -X POST -H 'accept: application/json;blockheader-encoding=object' -H 'content-type: application/json' -d {} {}",
            sh_quote(&body),
            sh_url(
                "B",
                &format!(
                    "/chainweb/0.0/{}/chain/{chain}/header/branch?minheight={height}&maxheight={height}",
                    self.cfg.network
                )
            )
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
                let (canon, orphans): (Vec<&Value>, Vec<&Value>) = rows
                    .iter()
                    .filter(|r| !self.genesis_copy(r, chain))
                    .partition(|r| r.get("blockHash") == bh.as_ref());
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
        // Genesis blocks included: compat serves their txs on /txs/txs (G1).
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
    // The GraphQL lanes are awaited by `finish`, so their last steps land before the final
    // summary flush (R-P5P9-58).
    let gql: Vec<tokio::task::JoinHandle<()>> = if ctx.cfg.has_graphql_pairs() {
        vec![tokio::spawn(crate::graphql::run_loop(ctx.clone()))]
    } else {
        Vec::new()
    };
    let rest = ctx
        .cfg
        .pairs
        .iter()
        .any(|(a, _)| ctx.targets.get(a).is_some_and(|t| !t.kind.is_graphql()));
    if !rest {
        // GraphQL pairs only: no REST generator or workers; tips and summaries as usual.
        let c = ctx.clone();
        tokio::spawn(async move {
            let mut t_tip = tokio::time::interval(TIP_REFRESH);
            let mut t_sum = tokio::time::interval(SUMMARY_FLUSH);
            t_tip.tick().await;
            loop {
                tokio::select! {
                    _ = t_tip.tick() => {
                        if let Err(e) = c.refresh_tips().await {
                            tracing::warn!(error = %e, "tip refresh");
                        }
                    }
                    _ = t_sum.tick() => c.flush_summary(),
                }
            }
        });
        shutdown_signal(max_secs).await;
        let (tx, _rx) = mpsc::channel::<Request>(1);
        return finish(ctx, tx, gql).await;
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
    if let Some(secs) = ctx.cfg.coins_every_secs {
        tokio::spawn(coins_cadence(tx.downgrade(), Duration::from_secs(secs)));
    }
    let rx = Arc::new(tokio::sync::Mutex::new(rx));
    let mut workers = gql;
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

/// `coins_every_secs` (R-P5P9-57): one `/coins` request into the REST pipeline per period, so
/// the `/coins` minimum fits a gate window without a separate `once --path /coins` loop. The
/// weak sender lets the pipeline close at shutdown.
async fn coins_cadence(tx: mpsc::WeakSender<Request>, every: Duration) {
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let Some(tx) = tx.upgrade() else { break };
        let q = Request::new("/coins".into(), crate::gen::Origin::Random, None);
        if tx.send(q).await.is_err() {
            break;
        }
    }
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

/// Percent-decoded path segment.
fn percent_decode(s: &str) -> String {
    url::form_urlencoded::parse(s.replace('+', "%2B").as_bytes())
        .next()
        .map(|(k, v)| {
            if v.is_empty() {
                k.into_owned()
            } else {
                format!("{k}={v}")
            }
        })
        .unwrap_or_default()
}

/// Equal as decimals: `1`, `1.0` and `1.00` are the same balance.
/// The balances pair's compare of kda-indexer's row for (token, chain) with the node's
/// `details` answer: the diff entries and whether they may be a race (a missing or extra row,
/// a guard change, or a balance on a `stale` row or one read 3+ heights below the node's).
/// A's balance row against the node's `/local`: the diff and whether it may be a race (the
/// pair then retries once, then counts it as skew). `chains_at`: the chain count at a height.
fn balance_diff(
    row: Option<&Value>,
    b: &kdx_node::Local<kdx_node::Details>,
    chains_at: &dyn Fn(u64) -> usize,
) -> (Vec<DiffEntry>, bool) {
    let (mut diff, mut uncertain) = (Vec::new(), false);
    let mut push = |path: &str, a: Option<Value>, b: Option<Value>| {
        diff.push(DiffEntry {
            path: path.into(),
            kind: DiffKind::Changed,
            a,
            b,
        });
    };
    match b {
        kdx_node::Local::Found { value, height } => match row {
            None => {
                push("/row", None, Some(Value::String(value.balance.clone())));
                uncertain = true;
            }
            Some(r) => {
                let bal = r.get("balance").and_then(Value::as_str).unwrap_or("");
                if !same_decimal(bal, &value.balance) {
                    push(
                        "/balance",
                        Some(Value::String(bal.into())),
                        Some(Value::String(value.balance.clone())),
                    );
                    let as_of = r.get("as_of_height").and_then(Value::as_u64).unwrap_or(0);
                    uncertain = r.get("stale").and_then(Value::as_bool) == Some(true)
                        || as_of + 2 < *height
                        || (r.get("module").and_then(Value::as_str) == Some("coin")
                            && coinbase_skew(bal, as_of, &value.balance, *height, chains_at));
                }
                if r.get("guard") != Some(&value.guard) {
                    push("/guard", r.get("guard").cloned(), Some(value.guard.clone()));
                    uncertain = true;
                }
            }
        },
        kdx_node::Local::Absent => {
            if let Some(r) = row {
                push("/row", r.get("balance").cloned(), None);
                uncertain = true;
            }
        }
    }
    (diff, uncertain)
}

/// Heights between the two reads that [`coinbase_skew`] considers at most.
const COINBASE_SKEW_HEIGHTS: u64 = 20;

/// Whether two coin balances of one account, read at heights `ours_h` and `theirs_h`, differ
/// by the coinbase of some of the blocks between them (R-P5P9-48): a miner's balance grows by
/// one block reward per block it mines, so the read at the higher height is higher by k
/// rewards of those heights (1 <= k <= the gap). Seen live in run 6E-3: k:e7f7130f...
/// chain 12, 32186.381987524380 at 7400410 against the node's 32187.286033524380 at
/// 7400412, one coinbase of 0.904046.
fn coinbase_skew(
    ours: &str,
    ours_h: u64,
    theirs: &str,
    theirs_h: u64,
    chains_at: &dyn Fn(u64) -> usize,
) -> bool {
    use crate::rewards::{block_reward, fixed};
    let (Some(x), Some(y)) = (fixed(ours), fixed(theirs)) else {
        return false;
    };
    let (lo, hi, d) = if theirs_h > ours_h {
        (ours_h, theirs_h, y - x)
    } else {
        (theirs_h, ours_h, x - y)
    };
    if d <= 0 || hi - lo > COINBASE_SKEW_HEIGHTS {
        return false;
    }
    let mut sums = std::collections::BTreeSet::from([0i128]);
    for h in lo + 1..=hi {
        let Some(r) = block_reward(h, chains_at(h)) else {
            return false;
        };
        let next: Vec<i128> = sums.iter().map(|s| s + r).collect();
        sums.extend(next);
    }
    sums.contains(&d)
}

fn same_decimal(a: &str, b: &str) -> bool {
    fn norm(s: &str) -> String {
        let s = s.trim();
        match s.split_once('.') {
            Some((i, f)) => match f.trim_end_matches('0') {
                "" => i.to_owned(),
                f => format!("{i}.{f}"),
            },
            None => s.to_owned(),
        }
    }
    norm(a) == norm(b)
}

#[cfg(test)]
mod mock_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// R-P5P9-10 #1: the balances pair's compare.
    #[test]
    fn balances_pair_compare() {
        use kdx_node::{Details, Local};
        let guard = serde_json::json!({"keys": ["ab"], "pred": "keys-all"});
        let found = |bal: &str, height| Local::Found {
            value: Details {
                balance: bal.into(),
                guard: guard.clone(),
            },
            height,
        };
        let row = |bal: &str, as_of: u64, stale: bool| {
            serde_json::json!({"module": "coin", "chain": 1, "balance": bal,
                "guard": guard, "as_of_height": as_of, "stale": stale})
        };
        let c20 = |_: u64| 20usize;
        // Same decimal in another spelling: a match.
        let r = row("1.50", 100, false);
        assert_eq!(
            super::balance_diff(Some(&r), &found("1.5", 101), &c20),
            (vec![], false)
        );
        // A fresh row with another balance: a sure diff.
        let (d, unsure) = super::balance_diff(Some(&r), &found("2.0", 101), &c20);
        assert_eq!(
            (d.len(), d[0].path.as_str(), unsure),
            (1, "/balance", false)
        );
        // A stale row, or one read 3 heights below the node: maybe a race.
        assert!(super::balance_diff(Some(&row("1.5", 100, true)), &found("2.0", 101), &c20).1);
        assert!(super::balance_diff(Some(&r), &found("2.0", 103), &c20).1);
        // Guard change, missing row, extra row.
        let other = serde_json::json!({"module": "coin", "chain": 1, "balance": "1.5",
            "guard": {"keys": ["cd"], "pred": "keys-all"}, "as_of_height": 100});
        let (d, unsure) = super::balance_diff(Some(&other), &found("1.5", 101), &c20);
        assert_eq!((d[0].path.as_str(), unsure), ("/guard", true));
        let (d, unsure) = super::balance_diff(None, &found("1.5", 101), &c20);
        assert_eq!((d[0].path.as_str(), unsure), ("/row", true));
        let (d, unsure) = super::balance_diff(Some(&r), &Local::Absent, &c20);
        assert_eq!(
            (d[0].path.as_str(), d[0].b.is_none(), unsure),
            ("/row", true, true)
        );
        assert_eq!(
            super::balance_diff(None, &Local::Absent, &c20),
            (vec![], false)
        );
    }

    /// R-P5P9-48 item 3: /coins past the relative 1e-6 is chainweb-data's tip lag only within
    /// the coinbase minted between the two tips (18.08092 KDA per height at 7.40M).
    #[test]
    fn coins_tolerance_scales_with_the_tip_gap() {
        // Run 6E-3, 04:43:29Z: A - B = 2,621.7 KDA, chainweb-data ~145 heights behind.
        let a: Value = serde_json::from_str("379699987.13176").unwrap();
        let b: Value = serde_json::from_str("379697365.43176").unwrap();
        let hit = coins_tip_lag(&a, &b, 7_400_100, 7_399_955).expect("145 heights");
        assert_eq!(hit.path, "/~oracle-lag/coins");
        let (left, ids) = crate::allowlist::Allowlist::load(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml"),
        )
        .unwrap()
        .classify_request(&RouteKind::Coins, "/coins", &[], &[hit]);
        assert!(left.is_empty() && ids == vec!["oracle-tip-lag".to_string()]);
        // No gap, or a gap whose coinbase (10 heights, 181 KDA) is too small: a diff.
        assert!(coins_tip_lag(&a, &b, 7_400_100, 7_400_100).is_none());
        assert!(coins_tip_lag(&a, &b, 7_400_100, 7_400_090).is_none());
        assert!(coins_tip_lag(&a, &b, 0, 7_399_955).is_none());
        // The side behind has the higher supply: a diff.
        assert!(coins_tip_lag(&b, &a, 7_400_100, 7_399_955).is_none());
        // chainweb-data ahead of A by 145 heights, its supply higher by as much: the gap.
        assert!(coins_tip_lag(&b, &a, 7_399_955, 7_400_100).is_some());
        // coins_tolerance_is_numeric's far value (9,987 KDA) stays a diff at 145 heights.
        let far: Value = serde_json::from_str("379690000.0").unwrap();
        assert!(coins_tip_lag(&a, &far, 7_400_100, 7_399_955).is_none());
        // Within the relative tolerance the diff code's own hit applies, not this one.
        let near: Value = serde_json::from_str("379699932.889").unwrap();
        assert!(coins_tip_lag(&a, &near, 7_400_100, 7_399_955).is_none());
    }

    /// R-P5P9-48 item 5: a balance diff equal to coinbases of the heights between A's read
    /// and the node's is tip skew (the run 6E-3 record: 0.904046 over 2 heights).
    #[test]
    fn balances_coinbase_skew() {
        use kdx_node::{Details, Local};
        let guard = serde_json::json!({"keys": ["ab"], "pred": "keys-all"});
        let node = |bal: &str, height| Local::Found {
            value: Details {
                balance: bal.into(),
                guard: guard.clone(),
            },
            height,
        };
        let ours = |module: &str, bal: &str, as_of: u64| {
            serde_json::json!({"module": module, "chain": 12, "balance": bal,
                "guard": guard, "as_of_height": as_of, "stale": false})
        };
        let graph = |h: u64| if h >= 852_054 { 20 } else { 10 };
        let unsure = |row: Value, n| balance_diff(Some(&row), &n, &graph);
        // k:e7f7130f... chain 12, 07:38:17Z: one coinbase of 0.904046 over 2 heights.
        let (d, skew) = unsure(
            ours("coin", "32186.381987524380", 7_400_410),
            node("32187.286033524380", 7_400_412),
        );
        assert_eq!((d.len(), d[0].path.as_str(), skew), (1, "/balance", true));
        // Two coinbases over 2 heights: skew. Three, or one off by 1e-6, or the node lower
        // while ahead: a sure diff.
        assert!(coinbase_skew(
            "10.0",
            7_400_410,
            "11.808092",
            7_400_412,
            &graph
        ));
        assert!(!coinbase_skew(
            "10.0",
            7_400_410,
            "12.712138",
            7_400_412,
            &graph
        ));
        assert!(!coinbase_skew(
            "10.0",
            7_400_410,
            "10.904047",
            7_400_412,
            &graph
        ));
        assert!(!coinbase_skew(
            "10.904046",
            7_400_410,
            "10.0",
            7_400_412,
            &graph
        ));
        // The node behind A: A higher by one coinbase is skew too.
        assert!(coinbase_skew(
            "10.904046",
            7_400_412,
            "10.0",
            7_400_411,
            &graph
        ));
        // Same height, or another token: no coinbase between.
        assert!(!coinbase_skew(
            "10.0",
            7_400_412,
            "10.904046",
            7_400_412,
            &graph
        ));
        let (_, skew) = unsure(
            ours("free.token", "32186.381987524380", 7_400_410),
            node("32187.286033524380", 7_400_412),
        );
        assert!(!skew);
        // Before the 20-chain graph the reward is split over 10 chains.
        assert!(coinbase_skew("1.0", 100, "3.304523", 101, &graph));
    }

    #[test]
    fn only_keys_ignores_value_diffs() {
        let a = json!([{"requestKey": "x", "blockHash": "h", "gasLimit": null},
                       {"requestKey": "y", "blockHash": "h"}]);
        let b = json!([{"requestKey": "x", "blockHash": "h", "gasLimit": 600},
                       {"requestKey": "y", "blockHash": "o"}]);
        let o = only_keys_in_first(&a, &b);
        assert_eq!(o.len(), 1);
        assert_eq!(o[0]["requestKey"], "y");
    }

    #[test]
    fn query_account_reads_both_routes() {
        let r = |p: &str| Request::new(p.to_string(), crate::gen::Origin::Replay, None);
        assert_eq!(
            query_account(&r("/txs/account/k%3Aab12?chain=0&limit=50")),
            "k:ab12"
        );
        assert_eq!(query_account(&r("/v1/txs/k:ab12/coin/10")), "k:ab12");
        assert_eq!(query_account(&r("/txs/recent")), "");
    }

    #[test]
    fn lag_and_limit_helpers() {
        let mut v = json!([{"height": 9}, {"height": 7}, {"height": 5}, {"height": 5}]);
        assert_eq!(drop_above(&mut v, 7), 1);
        assert_eq!(min_height(&v), Some(5));
        drop_at_or_below(&mut v, 5);
        assert_eq!(len_of(&v), 1);
        assert_eq!(
            query_limit("/txs/account/k:a?token=coin&limit=10"),
            Some(10)
        );
        assert_eq!(query_limit("/txs/account/k:a"), None);
        assert_eq!(num_value(&json!("1.5")), Some(1.5));
    }

    /// I1: a replica pair keeps the `/stats` tolerances (as `skew`) and nothing else, and its
    /// list clamp leaves a margin below the lower tip, so a chain trailing its side's tip
    /// does not show as missing top rows.
    #[test]
    fn replica_pair_tolerances_and_clamp() {
        // Two stores a few blocks apart: count and supply move inside the tolerances.
        let a = json!({"transactionCount": 1_000_000, "coinsInCirculation": 300_000_000.0});
        let b = json!({"transactionCount": 1_000_040, "coinsInCirculation": 300_000_093.0});
        let (rest, skew) = split_replica_skew(diff_route(&RouteKind::Stats, &a, &b));
        assert!(rest.is_empty(), "{rest:?}");
        assert!(skew);
        // Outside the tolerance it stays an unexplained diff.
        let c = json!({"transactionCount": 1_000_000, "coinsInCirculation": 300_500_000.0});
        let (rest, skew) = split_replica_skew(diff_route(&RouteKind::Stats, &a, &c));
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].path, "/coinsInCirculation");
        assert!(!skew);
        // Equal sides: a match, not skew.
        let (rest, skew) = split_replica_skew(diff_route(&RouteKind::Stats, &a, &a));
        assert!(rest.is_empty() && !skew);
        // Any other path is kept (no oracle allowlist entry applies to the pair).
        let (rest, skew) = split_replica_skew(vec![DiffEntry {
            path: ORACLE_LAG_PATH.into(),
            kind: DiffKind::Changed,
            a: None,
            b: None,
        }]);
        assert_eq!(rest.len(), 1);
        assert!(!skew);

        // List clamp: A's tip 100 on chain 0; B's tip 100 on chain 0, but B's chain 1 is
        // still at 97. Rows at 98..=100 on chain 1 exist on A only.
        let rows = |c1_top: u64| {
            let mut v = Vec::new();
            for h in (90..=100).rev() {
                v.push(json!({"chain": 0, "height": h}));
                if h <= c1_top {
                    v.push(json!({"chain": 1, "height": h}));
                }
            }
            Value::Array(v)
        };
        let (mut va, mut vb) = (rows(100), rows(97));
        // Both tips read 100 (B's chain 0 is at its tip), so the old clamp was 100.
        let naive = 100u64;
        let (mut na, mut nb) = (va.clone(), vb.clone());
        drop_above(&mut na, naive);
        drop_above(&mut nb, naive);
        assert_ne!(na, nb, "the old clamp at min(tipA, tipB) leaves a gap");
        let clamp = replica_clamp(100, 100);
        assert_eq!(clamp, 95);
        drop_above(&mut va, clamp);
        drop_above(&mut vb, clamp);
        assert_eq!(va, vb);
        assert_eq!(replica_clamp(3, 9), 0);
    }

    /// Event tie-break helpers: rows match by (requestKey, blockHash, idx); event lists
    /// compare as multisets at f64 precision; the node projection is the /v1/txs shape.
    #[test]
    fn event_tiebreak_helpers() {
        let a = json!({"requestKey": "r", "blockHash": "b", "events": []});
        let b = json!({"requestKey": "r", "blockHash": "b", "events": [1]});
        assert_eq!(row_key(&a), row_key(&b));
        assert_ne!(
            row_key(&a),
            row_key(&json!({"requestKey": "r", "blockHash": "c"}))
        );
        let x = json!([{"name": "coin.TRANSFER", "params": ["a", "b", 1.5]},
                       {"name": "coin.TRANSFER", "params": ["a", "m", 0.1]}]);
        let y = json!([{"name": "coin.TRANSFER", "params": ["a", "m", 0.1]},
                       {"name": "coin.TRANSFER", "params": ["a", "b", 1.5]}]);
        assert!(same_events(&x, &y));
        // The tie-break runs after numbers_as_f64 (seen live: chain 2.0 made it give up).
        let mut f = json!({"chain": 7, "height": 5});
        numbers_as_f64(&mut f);
        assert_eq!(row_chain(&f), Some(7));
        assert_eq!(row_chain(&json!({"chain": 19})), Some(19));
        assert_eq!(row_chain(&json!({"chain": 1.5})), None);
        // Numeric meta strings project as numbers (F3 tx -rSHnkpP...).
        assert_eq!(crate::oracle::meta_num(json!("0.00000001")), json!(1e-8));
        assert_eq!(crate::oracle::meta_num(json!("600")), json!(600));
        assert_eq!(crate::oracle::meta_num(json!(1e-8)), json!(1e-8));
        assert_eq!(crate::oracle::meta_num(json!("abc")), json!("abc"));
        let z = json!([{"name": "coin.TRANSFER", "params": ["a", "m", 1.5]},
                       {"name": "coin.TRANSFER", "params": ["a", "b", 0.1]}]);
        assert!(!same_events(&x, &z));
        let poll = json!({"events": [
            {"name": "TRANSFER", "module": {"namespace": null, "name": "coin"},
             "moduleHash": "h", "params": ["a", "b", 1.5]},
            {"name": "X", "module": {"namespace": "n", "name": "m"}, "moduleHash": "h"}
        ]});
        assert_eq!(
            crate::oracle::v1txs_events(&poll),
            json!([{"name": "coin.TRANSFER", "params": ["a", "b", 1.5]},
                   {"name": "n.m.X", "params": []}])
        );
    }
}
