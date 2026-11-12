//! GraphQL targets of the harness (plan Task 7): operation documents (captured client
//! operations loaded by path, our own written from the schema), variable sampling from random
//! canonical blocks, the GraphQL normaliser and the compare of the GraphQL adapter (A) with
//! the reference server (B), the node (`/poll`, `/header`, `/local` details) or the native
//! balances API.
//!
//! Routes are named `graphql:<operation file stem>` (captured stems carry the document hash,
//! because clients reuse operation names for different documents).

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde_json::{json, Map, Value};

use crate::config::{CompareConfig, TargetKind};
use crate::diff::{decimal_canon, diff};
use crate::gen::random::RandomGen;
use crate::gen::Tips;
use crate::normalize::{iso_eq_coarse, iso_micros, parse_iso, RouteKind};
use crate::report::{iso_secs, now_secs, sh_quote, trim_body, DiffEntry, DiffKind, DiffRecord};
use crate::run::{Ctx, Outcome};
use crate::target::Target;

/// Root fields the adapter answers with `NOT_SUPPORTED` (spec section 5.3; plan Q9).
pub const NOT_SUPPORTED_ROOTS: &[&str] = &["pactQuery", "lastTokenPriceInKda", "gasLimitEstimate"];
/// Root fields whose answer depends on the current tip: never sent to the oracle (GQ-16).
const TIP_ROOTS: &[&str] = &[
    "lastBlockHeight",
    "networkInfo",
    "completedBlockHeights",
    "blocksFromDepth",
    "graphConfiguration",
    "newBlocks",
    "newBlocksFromDepth",
    "balance",
];
/// Visible allowlist hits (`compare/allowlist.toml`).
pub const PAGE_CUT_PATH: &str = "/~graphql-page-cut";
pub const TOTALCOUNT_CAP_PATH: &str = "/~graphql-totalcount-cap";
pub const ORACLE_STALL_PATH: &str = "/~graphql-oracle-stall";
/// Pause after this many consecutive oracle timeouts, and for how long (GQ-16).
const ORACLE_TIMEOUTS_BEFORE_PAUSE: u32 = 3;
const ORACLE_PAUSE: Duration = Duration::from_secs(600);
/// Pause before the one retry of a tip-relative node balance diff.
const BALANCE_RETRY: Duration = Duration::from_secs(15);
/// Samples taken per drawn block window.
const SAMPLES_PER_BLOCK: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    /// Compared with the oracle at heights up to `max_height`.
    Oracle,
    /// Depends on the tip: not compared with the oracle.
    Tip,
    /// Node pairs (and, for balances, the native API pair).
    NodeTx,
    NodeBlock,
    NodeBalance,
    /// Expected to answer `NOT_SUPPORTED` (checked on A alone).
    NotSupported,
    /// Subscription document (only `ws-soak` uses subscriptions).
    Subscription,
}

#[derive(Debug, Clone)]
pub struct VarDef {
    pub name: String,
    pub required: bool,
}

#[derive(Debug, Clone)]
pub struct GqlOp {
    /// File stem; the route is `graphql:<stem>`.
    pub stem: String,
    pub query: String,
    pub vars: Vec<VarDef>,
    /// Example variables from the capture (`null`s and page sizes are kept as they are).
    pub example: Map<String, Value>,
    pub kind: OpKind,
    pub roots: Vec<String>,
    pub captured: bool,
}

impl GqlOp {
    pub fn route(&self) -> String {
        format!("graphql:{}", self.stem)
    }

    fn has_var(&self, n: &str) -> bool {
        self.vars.iter().any(|v| v.name == n)
    }
}

/// Parses an operation document into variables and root fields. Comments (`#`) are removed.
pub fn parse_op(stem: &str, text: &str, captured: bool) -> anyhow::Result<GqlOp> {
    let mut directive = None;
    let mut query = String::new();
    for line in text.lines() {
        let t = line.trim_start();
        if let Some(c) = t.strip_prefix('#') {
            if let Some(k) = c.trim().strip_prefix("kdx:") {
                directive = Some(k.trim().to_string());
            }
            continue;
        }
        query.push_str(line);
        query.push('\n');
    }
    let query = query.trim().to_string();
    anyhow::ensure!(!query.is_empty(), "{stem}: empty document");
    let is_sub = query.trim_start().starts_with("subscription");
    // Variable definitions: the first `(` before the first `{`.
    let brace = query.find('{').context("no selection set")?;
    let mut vars = Vec::new();
    if let Some(open) = query[..brace].find('(') {
        let mut depth = 0;
        let mut end = None;
        for (i, ch) in query[open..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(open + i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let end = end.context("unbalanced variable definitions")?;
        for part in query[open + 1..end].split(',') {
            let part = part.trim();
            let Some(rest) = part.strip_prefix('$') else {
                continue;
            };
            let (name, ty) = rest.split_once(':').context("variable without a type")?;
            let ty = ty.split('=').next().unwrap_or("").trim();
            vars.push(VarDef {
                name: name.trim().to_string(),
                required: ty.ends_with('!'),
            });
        }
    }
    let roots = root_fields(&query);
    let kind = match directive.as_deref() {
        Some("oracle") => OpKind::Oracle,
        Some("tip") => OpKind::Tip,
        Some("node-tx") => OpKind::NodeTx,
        Some("node-block") => OpKind::NodeBlock,
        Some("node-balance") => OpKind::NodeBalance,
        Some(other) => anyhow::bail!("{stem}: unknown directive {other:?}"),
        None if is_sub => OpKind::Subscription,
        None if roots
            .iter()
            .any(|r| NOT_SUPPORTED_ROOTS.contains(&r.as_str())) =>
        {
            OpKind::NotSupported
        }
        None => {
            let bounded = vars.iter().any(|v| {
                let n = v.name.to_ascii_lowercase();
                [
                    "height",
                    "hash",
                    "requestkey",
                    "accountname",
                    "publickey",
                    "tokenid",
                ]
                .iter()
                .any(|k| n.contains(k))
            });
            if bounded && !roots.iter().any(|r| TIP_ROOTS.contains(&r.as_str())) {
                OpKind::Oracle
            } else {
                OpKind::Tip
            }
        }
    };
    Ok(GqlOp {
        stem: stem.to_string(),
        query,
        vars,
        example: Map::new(),
        kind,
        roots,
        captured,
    })
}

/// Names of the fields selected directly under the operation's selection set.
fn root_fields(query: &str) -> Vec<String> {
    let mut out = Vec::new();
    let (mut depth, mut paren) = (0i32, 0i32);
    let mut chars = query.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        match c {
            '{' => depth += 1,
            '}' => depth -= 1,
            '(' => paren += 1,
            ')' => paren -= 1,
            c if (c.is_ascii_alphabetic() || c == '_') && depth == 1 && paren == 0 => {
                let mut j = i + c.len_utf8();
                while let Some(&(k, d)) = chars.peek() {
                    if d.is_ascii_alphanumeric() || d == '_' {
                        j = k + d.len_utf8();
                        chars.next();
                    } else {
                        break;
                    }
                }
                let word = &query[i..j];
                // `alias: field`: the word after the colon is the field.
                let after = query[j..].trim_start();
                if !after.starts_with(':') {
                    out.push(word.to_string());
                }
            }
            _ => {}
        }
    }
    out
}

/// The `## example variables` JSON object of a capture's `.txt` file.
fn example_vars(txt: &str) -> Map<String, Value> {
    let Some(i) = txt.find("## example variables") else {
        return Map::new();
    };
    let rest = &txt[i..];
    let Some(start) = rest.find('{') else {
        return Map::new();
    };
    let mut de = serde_json::Deserializer::from_str(&rest[start..]).into_iter::<Value>();
    match de.next() {
        Some(Ok(Value::Object(m))) => m,
        _ => Map::new(),
    }
}

/// Loads every `*.graphql` of `dir`. Captured documents (`dir` = `ops_dir`) read their example
/// variables from the sibling `*.txt`.
pub fn load_dir(dir: &Path, captured: bool, skip: &[String]) -> anyhow::Result<Vec<GqlOp>> {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "graphql"))
        .collect();
    names.sort();
    let mut out = Vec::new();
    for p in names {
        let stem = p
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        let display = stem
            .rsplit_once('-')
            .filter(|(_, h)| h.len() == 8 && h.bytes().all(|b| b.is_ascii_hexdigit()))
            .map_or(stem.as_str(), |(n, _)| n);
        if skip.iter().any(|s| s == display || *s == stem) {
            continue;
        }
        let text =
            std::fs::read_to_string(&p).with_context(|| format!("reading {}", p.display()))?;
        let mut op = parse_op(&stem, &text, captured).with_context(|| p.display().to_string())?;
        if captured {
            if let Ok(t) = std::fs::read_to_string(p.with_extension("txt")) {
                op.example = example_vars(&t);
            }
        }
        out.push(op);
    }
    Ok(out)
}

/// What a sampled canonical transaction offers to the variables of an operation.
#[derive(Debug, Clone)]
pub struct Sample {
    pub chain: u8,
    pub height: u64,
    pub block_hash: String,
    pub request_key: String,
    pub accounts: Vec<String>,
    /// The canonical header as the node served it.
    pub header: Value,
    /// `meta` of the command (sender, gas, ttl, creation time).
    pub meta: Value,
}

impl Sample {
    fn public_key(&self) -> Option<String> {
        self.accounts.iter().find_map(|a| {
            a.strip_prefix("k:")
                .filter(|k| k.len() == 64)
                .map(str::to_string)
        })
    }
}

/// Builds the variables of `op` for `sample`; `None` when a required one has no value.
pub fn vars_for(op: &GqlOp, s: &Sample, rng: &mut StdRng) -> Option<Map<String, Value>> {
    let mut m = op.example.clone();
    let acct = s
        .accounts
        .get(rng.random_range(0..s.accounts.len().max(1)))
        .cloned();
    let page_default = |name: &str, m: &Map<String, Value>| -> Value {
        match name {
            "first" => m.get("first").cloned().unwrap_or(json!(10)),
            _ => m.get(name).cloned().unwrap_or(Value::Null),
        }
    };
    for v in &op.vars {
        let n = v.name.as_str();
        let val: Option<Value> = match n {
            "startHeight" | "minHeight" | "height" | "blockHeight" => Some(json!(s.height)),
            "endHeight" | "maxHeight" => Some(json!(s.height + rng.random_range(0..4u64))),
            "chainId" => Some(json!(s.chain.to_string())),
            "chainIds" => Some(json!([s.chain.to_string()])),
            "hash" | "blockHash" => Some(json!(s.block_hash)),
            "requestKey" => Some(json!(s.request_key)),
            "accountName" | "account" => acct.as_ref().map(|a| json!(a)),
            "publicKey" => s.public_key().map(|k| json!(k)),
            "fungibleName" | "moduleName" | "module" => Some(json!("coin")),
            "qualifiedEventName" => Some(json!("coin.TRANSFER")),
            "minimumDepth" => Some(json!(0)),
            "blockId" => Some(json!(base64_std(
                format!("Block:{}", s.block_hash).as_bytes()
            ))),
            "first" | "last" | "after" | "before" => Some(page_default(n, &m)),
            _ => m.get(n).cloned(),
        };
        match val {
            Some(x) => {
                m.insert(n.to_string(), x);
            }
            None if v.required => return None,
            None => {
                m.insert(n.to_string(), Value::Null);
            }
        }
    }
    m.retain(|k, _| op.has_var(k));
    Some(m)
}

fn base64_std(b: &[u8]) -> String {
    use std::fmt::Write as _;
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for c in b.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        let _ = write!(
            out,
            "{}{}",
            T[(n >> 18) as usize & 63] as char,
            T[(n >> 12) as usize & 63] as char
        );
        out.push(if c.len() > 1 {
            T[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if c.len() > 2 {
            T[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Draws random canonical blocks and turns their user txs into [`Sample`]s.
pub struct Sampler {
    gen: RandomGen,
    cache: Vec<Sample>,
    cap: Option<u64>,
    rng: StdRng,
}

impl Sampler {
    pub fn new(gen: RandomGen, cap: Option<u64>) -> Sampler {
        Sampler {
            gen,
            cache: Vec::new(),
            cap,
            rng: StdRng::from_rng(&mut rand::rng()),
        }
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<Sample>> {
        if self.cache.len() < 2 {
            for (header, b) in self.gen.draw_blocks(self.cap).await? {
                let hash = header
                    .get("hash")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                for _ in 0..SAMPLES_PER_BLOCK.min(b.txs.len()) {
                    let tx = &b.txs[self.rng.random_range(0..b.txs.len())];
                    let mut accounts = Vec::new();
                    if let Some(s) = tx.cmd_parsed.as_ref().and_then(crate::gen::cmd_sender) {
                        accounts.push(s.to_string());
                    }
                    for ev in tx
                        .output
                        .get("events")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        for p in ev
                            .get("params")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .take(2)
                        {
                            if let Some(a) = p.as_str().filter(|a| a.len() >= 3) {
                                if !accounts.iter().any(|x| x == a) {
                                    accounts.push(a.to_string());
                                }
                            }
                        }
                    }
                    self.cache.push(Sample {
                        chain: b.header.chain_id,
                        height: b.header.height,
                        block_hash: hash.to_string(),
                        request_key: tx.hash.to_b64url(),
                        accounts,
                        header: header.clone(),
                        meta: tx
                            .cmd_parsed
                            .as_ref()
                            .and_then(|c| c.get("meta").cloned())
                            .unwrap_or(Value::Null),
                    });
                }
            }
        }
        if self.cache.is_empty() {
            return Ok(None);
        }
        let i = self.rng.random_range(0..self.cache.len());
        Ok(Some(self.cache.swap_remove(i)))
    }
}

/// GraphQL state of a [`Ctx`]: the loaded operations, the sampler and the oracle guard.
pub struct GqlState {
    pub ops: Vec<Arc<GqlOp>>,
    pub sampler: tokio::sync::Mutex<Option<Sampler>>,
    rng: Mutex<StdRng>,
    cursor: AtomicUsize,
    acct_ok: Mutex<HashMap<String, bool>>,
    oracle_timeouts: AtomicU32,
    oracle_pause_until: Mutex<Option<Instant>>,
}

impl GqlState {
    pub fn new(cfg: &CompareConfig) -> anyhow::Result<GqlState> {
        let mut ops = Vec::new();
        if cfg.has_graphql_pairs() {
            let g = &cfg.graphql;
            if g.own_ops_dir.is_dir() {
                ops.extend(load_dir(&g.own_ops_dir, false, &g.skip_ops)?);
            }
            if let Some(d) = &g.ops_dir {
                ops.extend(load_dir(d, true, &g.skip_ops)?);
            }
            ops.retain(|o| o.kind != OpKind::Subscription);
        }
        Ok(GqlState {
            ops: ops.into_iter().map(Arc::new).collect(),
            sampler: tokio::sync::Mutex::new(None),
            rng: Mutex::new(StdRng::from_rng(&mut rand::rng())),
            cursor: AtomicUsize::new(0),
            acct_ok: Mutex::new(HashMap::new()),
            oracle_timeouts: AtomicU32::new(0),
            oracle_pause_until: Mutex::new(None),
        })
    }

    fn oracle_paused(&self) -> bool {
        let mut g = self.oracle_pause_until.lock().expect("pause");
        match *g {
            Some(t) if t > Instant::now() => true,
            Some(_) => {
                *g = None;
                false
            }
            None => false,
        }
    }

    fn note_oracle(&self, timed_out: bool) {
        if timed_out {
            let n = self.oracle_timeouts.fetch_add(1, Ordering::Relaxed) + 1;
            if n >= ORACLE_TIMEOUTS_BEFORE_PAUSE {
                tracing::warn!("oracle timed out {n} times in a row: pausing it for 10 minutes");
                *self.oracle_pause_until.lock().expect("pause") =
                    Some(Instant::now() + ORACLE_PAUSE);
                self.oracle_timeouts.store(0, Ordering::Relaxed);
            }
        } else {
            self.oracle_timeouts.store(0, Ordering::Relaxed);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Normalisation

/// Whole numbers at or above this magnitude may be a number on one side and a string on the
/// other (BigInt); below it a number/string difference is a real format diff.
const BIG: u128 = 1 << 53;

fn canon_number(t: &str) -> Value {
    match decimal_canon(t) {
        Some((neg, d, e)) => Value::String(format!("#{}{d}e{e}", if neg { "-" } else { "" })),
        None => Value::String(t.to_string()),
    }
}

fn sort_key(node: &Value) -> Vec<String> {
    let get = |p: &[&str]| -> String {
        p.iter()
            .find_map(|x| node.pointer(x))
            .map(|v| v.to_string())
            .unwrap_or_default()
    };
    vec![
        get(&["/creationTime", "/cmd/meta/creationTime"]),
        get(&["/height", "/result/height"]),
        get(&["/orderIndex"]),
    ]
}

/// Normalises one GraphQL response body in place (see the plan's Task 7 list): cursors dropped,
/// `edges` equal in sort-key ties compared as sets, numbers by exact decimal value, `totalCount`
/// capped, errors reduced to their codes.
pub fn normalize_gql(v: &mut Value, count_cap: u64) {
    match v {
        Value::Object(m) => {
            for k in [
                "cursor",
                "startCursor",
                "endCursor",
                "extensions",
                "stacktrace",
            ] {
                m.remove(k);
            }
            if let Some(Value::Array(errs)) = m.get_mut("errors") {
                let mut codes: Vec<Value> = errs
                    .iter()
                    .map(|e| {
                        let code = e
                            .pointer("/extensions/code")
                            .or_else(|| e.get("code"))
                            .cloned()
                            .unwrap_or(Value::Null);
                        json!({ "code": code })
                    })
                    .collect();
                codes.sort_by_key(|c| c.to_string());
                *errs = codes;
            }
            for (k, x) in m.iter_mut() {
                if k == "totalCount" {
                    if let Some(n) = x.as_u64() {
                        *x = json!(n.min(count_cap));
                        continue;
                    }
                }
                normalize_gql(x, count_cap);
            }
            if let Some(Value::Array(edges)) = m.get_mut("edges") {
                sort_ties(edges);
            }
        }
        Value::Array(a) => {
            for x in a.iter_mut() {
                normalize_gql(x, count_cap);
            }
        }
        Value::Number(n) => *v = canon_number(&n.to_string()),
        Value::String(s)
            if s.len() >= 16
                && s.bytes().all(|b| b.is_ascii_digit())
                && s.parse::<u128>().is_ok_and(|n| n >= BIG) =>
        {
            *v = canon_number(&s.clone());
        }
        _ => {}
    }
}

/// Within runs of equal sort keys, orders edges by their canonical text.
fn sort_ties(edges: &mut [Value]) {
    let keys: Vec<Vec<String>> = edges
        .iter()
        .map(|e| e.get("node").map(sort_key).unwrap_or_default())
        .collect();
    let mut i = 0;
    while i < edges.len() {
        let mut j = i + 1;
        while j < edges.len() && keys[j] == keys[i] {
            j += 1;
        }
        if j - i > 1 {
            edges[i..j].sort_by_key(|e| e.to_string());
        }
        i = j;
    }
}

// ---------------------------------------------------------------------------------------------
// Pages

/// The first root field of `data` that is a connection (`edges` and `pageInfo`).
fn root_connection(data: &mut Value) -> Option<&mut Value> {
    data.as_object_mut()?
        .values_mut()
        .find(|v| v.get("edges").is_some_and(Value::is_array) && v.get("pageInfo").is_some())
}

fn edge_height(e: &Value) -> Option<u64> {
    e.pointer("/node/height")
        .or_else(|| e.pointer("/node/result/height"))
        .and_then(Value::as_u64)
}

/// Drops rows above `cap` from the root connection; returns how many.
fn drop_above(data: &mut Value, cap: u64) -> usize {
    let Some(c) = root_connection(data) else {
        return 0;
    };
    let Some(edges) = c.get_mut("edges").and_then(Value::as_array_mut) else {
        return 0;
    };
    let n = edges.len();
    edges.retain(|e| edge_height(e).is_none_or(|h| h <= cap));
    n - edges.len()
}

fn edges_len(data: &mut Value) -> usize {
    root_connection(data)
        .and_then(|c| c.get("edges").and_then(Value::as_array).map(Vec::len))
        .unwrap_or(0)
}

fn has_next(data: &mut Value) -> bool {
    root_connection(data)
        .and_then(|c| c.pointer("/pageInfo/hasNextPage").and_then(Value::as_bool))
        .unwrap_or(false)
}

fn truncate_edges(data: &mut Value, n: usize) {
    if let Some(c) = root_connection(data) {
        if let Some(e) = c.get_mut("edges").and_then(Value::as_array_mut) {
            e.truncate(n);
        }
        if let Some(p) = c.get_mut("pageInfo").and_then(Value::as_object_mut) {
            p.insert("hasNextPage".into(), json!(false));
        }
    }
}

/// An answer: the response (pages merged), the HTTP statuses and a verdict on transport.
pub struct Answer {
    pub body: Value,
    pub status: u16,
    pub raw: String,
    pub ms: Vec<u64>,
}

impl Answer {
    fn error_codes(&self) -> Vec<String> {
        self.body
            .get("errors")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|e| {
                e.pointer("/extensions/code")
                    .or_else(|| e.get("code"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect()
    }
}

fn body_of(op: &GqlOp, vars: &Map<String, Value>) -> Value {
    json!({ "query": op.query, "variables": vars })
}

fn curl_of(op: &GqlOp, vars: &Map<String, Value>, var: &str) -> String {
    format!(
        "curl -sk -H 'content-type: application/json' -d {} \"${var}\"",
        sh_quote(&body_of(op, vars).to_string())
    )
}

/// Prepares and diffs the two answers of an oracle pair: rows above `max` dropped, pages cut to
/// their common prefix, `totalCount` capped; the visible allowlist hits are added to the diff.
pub fn compare_answers(a: &mut Answer, b: &mut Answer, max: u64, cap: u64) -> Vec<DiffEntry> {
    let mut hits: Vec<DiffEntry> = Vec::new();
    let hit = |p: &str| DiffEntry {
        path: p.into(),
        kind: DiffKind::Changed,
        a: None,
        b: None,
    };
    // Rows above the oracle's stall height exist only on A.
    let dropped = a.body.get_mut("data").map_or(0, |d| drop_above(d, max))
        + b.body.get_mut("data").map_or(0, |d| drop_above(d, max));
    if dropped > 0 {
        hits.push(hit(ORACLE_STALL_PATH));
    }
    // Pages cut at a different size: compare the common prefix.
    if let (Some(da), Some(db)) = (a.body.get_mut("data"), b.body.get_mut("data")) {
        let (la, lb) = (edges_len(da), edges_len(db));
        if la != lb {
            let (more_a, more_b) = (has_next(da), has_next(db));
            if (la < lb && more_a) || (lb < la && more_b) {
                truncate_edges(da, la.min(lb));
                truncate_edges(db, la.min(lb));
                hits.push(hit(PAGE_CUT_PATH));
            }
        }
    }
    if let (Some(x), Some(y)) = (total_count(&a.body), total_count(&b.body)) {
        if x != y && x.min(y) >= cap {
            hits.push(hit(TOTALCOUNT_CAP_PATH));
        }
    }
    let (mut na, mut nb) = (a.body.clone(), b.body.clone());
    normalize_gql(&mut na, cap);
    normalize_gql(&mut nb, cap);
    let mut d = diff(&na, &nb);
    d.extend(hits);
    d
}

/// Sends `op` and follows `hasNextPage`/`after` up to `depth` more pages (the root
/// connection's edges are concatenated; the last page's `pageInfo` is kept).
pub async fn fetch_pages(
    t: &Target,
    op: &GqlOp,
    vars: &Map<String, Value>,
    depth: u32,
) -> anyhow::Result<Answer> {
    let mut vars = vars.clone();
    let mut ms: Vec<u64> = Vec::new();
    let cap = t.post_json(&body_of(op, &vars)).await?;
    ms.push(cap.ms);
    let status = cap.status;
    let raw = trim_body(&cap.body);
    let mut merged: Value = serde_json::from_slice(&cap.body)
        .with_context(|| format!("{}: HTTP {status}, body is not JSON: {raw}", t.name))?;
    let can_page = op.has_var("after") && vars.get("first").is_some_and(|f| !f.is_null());
    let mut pages = 1;
    while can_page && pages <= depth && merged.get("data").is_some_and(|d| !d.is_null()) {
        let data = merged.get_mut("data").expect("data");
        if !has_next(data) {
            break;
        }
        let Some(end) = root_connection(data)
            .and_then(|c| c.pointer("/pageInfo/endCursor").and_then(Value::as_str))
            .map(str::to_string)
        else {
            break;
        };
        vars.insert("after".into(), json!(end));
        let c = t.post_json(&body_of(op, &vars)).await?;
        ms.push(c.ms);
        let mut next: Value = serde_json::from_slice(&c.body).context("page is not JSON")?;
        let Some(nd) = next.get_mut("data").filter(|d| !d.is_null()) else {
            break;
        };
        let page_info = root_connection(nd).and_then(|c| c.get("pageInfo").cloned());
        let more = root_connection(nd)
            .and_then(|c| c.get_mut("edges"))
            .map(Value::take);
        let (Some(Value::Array(more)), Some(pi)) = (more, page_info) else {
            break;
        };
        let data = merged.get_mut("data").expect("data");
        if let Some(c) = root_connection(data) {
            if let Some(e) = c.get_mut("edges").and_then(Value::as_array_mut) {
                e.extend(more);
            }
            c["pageInfo"] = pi;
        }
        pages += 1;
    }
    Ok(Answer {
        body: merged,
        status,
        raw,
        ms,
    })
}

impl Ctx {
    fn gql_count(&self, route: &str, target: &str, ms: &[u64]) {
        let mut s = self.summary.lock().expect("summary");
        for m in ms {
            self.metrics.request(route, target);
            s.request(route, target);
            s.latency(route, target, *m);
        }
    }

    /// [`fetch_pages`] with the request and latency counts of the summary.
    pub async fn gql_fetch(
        &self,
        t: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
        depth: u32,
    ) -> anyhow::Result<Answer> {
        let a = fetch_pages(t, op, vars, depth).await?;
        self.gql_count(&op.route(), &t.name, &a.ms);
        Ok(a)
    }

    /// Whether the newest posting of `account` on A is at or below `max_height` (GQ-16: an
    /// account whose newest rows are above the oracle's stall height is not comparable).
    async fn account_below(&self, a: &Target, account: &str, max_height: u64) -> bool {
        if let Some(r) = self.gql.acct_ok.lock().expect("acct").get(account) {
            return *r;
        }
        let q = "query($a: String) { transactions(accountName: $a, first: 1) { edges { node { result { ... on TransactionResult { height } } } } } }";
        let body = json!({ "query": q, "variables": { "a": account } });
        let ok = match a.post_json(&body).await {
            Ok(c) => serde_json::from_slice::<Value>(&c.body)
                .ok()
                .and_then(|v| {
                    v.pointer("/data/transactions/edges/0/node/result/height")
                        .and_then(Value::as_u64)
                })
                .is_some_and(|h| h <= max_height),
            Err(_) => false,
        };
        self.gql
            .acct_ok
            .lock()
            .expect("acct")
            .insert(account.to_string(), ok);
        ok
    }

    async fn gql_sample(&self) -> anyhow::Result<Option<Sample>> {
        let mut g = self.gql.sampler.lock().await;
        if g.is_none() {
            let cap = self
                .cfg
                .targets
                .values()
                .filter(|t| t.kind == TargetKind::GraphqlOracle)
                .filter_map(|t| t.max_height)
                .min();
            let gen = RandomGen::new(
                self.node.clone(),
                self.graph.clone(),
                self.tips.clone(),
                self.cfg.min_depth,
            );
            *g = Some(Sampler::new(gen, cap));
        }
        g.as_mut().expect("sampler").next().await
    }

    fn gql_vars(&self, op: &GqlOp, s: &Sample) -> Option<Map<String, Value>> {
        let mut rng = self.gql.rng.lock().expect("rng");
        vars_for(op, s, &mut rng)
    }

    /// The (pair index, operation) combinations the configured pairs can compare.
    pub fn gql_jobs(&self) -> Vec<(usize, Arc<GqlOp>)> {
        let mut jobs: Vec<(usize, Arc<GqlOp>)> = Vec::new();
        for (pi, (a, b)) in self.cfg.pairs.iter().enumerate() {
            let (Some(ta), bk) = (self.targets.get(a), self.targets.get(b).map(|t| t.kind)) else {
                continue;
            };
            if ta.kind != TargetKind::KdaIndexerGraphql {
                continue;
            }
            for op in &self.gql.ops {
                let fit = matches!(
                    (b.as_str(), bk, op.kind),
                    (
                        _,
                        Some(TargetKind::GraphqlOracle),
                        OpKind::Oracle | OpKind::NotSupported
                    ) | (
                        "node",
                        _,
                        OpKind::NodeTx | OpKind::NodeBlock | OpKind::NodeBalance
                    ) | (_, Some(TargetKind::KdaIndexerNative), OpKind::NodeBalance)
                );
                if fit {
                    jobs.push((pi, op.clone()));
                }
            }
        }
        jobs
    }

    /// One compare round: the next (pair, operation) in turn, with a fresh sample.
    pub async fn gql_step(&self) -> anyhow::Result<()> {
        let jobs = self.gql_jobs();
        if jobs.is_empty() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            return Ok(());
        }
        let i = self.gql.cursor.fetch_add(1, Ordering::Relaxed) % jobs.len();
        let (pi, op) = &jobs[i];
        let (a, b) = &self.cfg.pairs[*pi];
        let ta = &self.targets[a];
        let tb = self.targets.get(b);
        if tb.is_some_and(|t| t.kind == TargetKind::GraphqlOracle) && self.gql.oracle_paused() {
            tokio::time::sleep(Duration::from_secs(5)).await;
            return Ok(());
        }
        let Some(sample) = self.gql_sample().await? else {
            self.count_class("graphql", "gen-error");
            return Ok(());
        };
        let Some(vars) = self.gql_vars(op, &sample) else {
            self.count_class(&op.route(), "skipped-vars");
            return Ok(());
        };
        let route = op.route();
        let o = match (op.kind, tb) {
            (OpKind::NotSupported, _) => self.gql_not_supported(ta, op, &vars).await,
            (OpKind::Oracle, Some(tb)) => {
                let max = self.cfg.targets[&tb.name].max_height.unwrap_or(u64::MAX);
                if let Some(acct) = vars.get("accountName").and_then(Value::as_str) {
                    if !self.account_below(ta, acct, max).await {
                        self.count_class(&route, "skipped-account-newer");
                        return Ok(());
                    }
                }
                self.gql_vs_oracle(ta, tb, op, &vars, max).await
            }
            (OpKind::NodeTx, None) => self.gql_node_tx(ta, op, &vars, &sample).await,
            (OpKind::NodeBlock, None) => self.gql_node_block(ta, op, &vars, &sample).await,
            (OpKind::NodeBalance, None) => self.gql_node_balance(ta, op, &vars, &sample).await,
            (OpKind::NodeBalance, Some(tb)) => self.gql_native_balance(ta, tb, op, &vars).await,
            _ => return Ok(()),
        };
        let pair_b = b.as_str();
        {
            let mut s = self.summary.lock().expect("summary");
            s.pair(&route, a, pair_b, &o.class);
            if let Some(rec) = &o.record {
                if let Some(ids) = &rec.allowlisted {
                    for id in ids.split(',') {
                        s.allowlist_hit(id);
                    }
                }
                if rec.class == "diff" {
                    s.unexplained(&route, &rec.diff);
                }
            }
        }
        self.count_class(&route, &o.class);
        if let Some(rec) = &o.record {
            if let Err(e) = self.log.append(rec) {
                tracing::warn!(error = %e, "diff log append");
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn gql_record(
        &self,
        op: &GqlOp,
        vars: &Map<String, Value>,
        a: &Target,
        b_name: &str,
        a_ans: Option<&Answer>,
        b_status: Option<u16>,
        b_body: &str,
        diff: Vec<DiffEntry>,
        class: &str,
        allowlisted: &[String],
        note: Option<String>,
    ) -> Outcome {
        let rec = DiffRecord {
            ts: iso_secs(now_secs()),
            route: op.route(),
            request: format!("{} {}", op.stem, Value::Object(vars.clone())),
            a_target: a.name.clone(),
            b_target: b_name.to_string(),
            a_status: a_ans.map(|x| x.status),
            b_status,
            a_body_trimmed: a_ans.map(|x| x.raw.clone()).unwrap_or_default(),
            b_body_trimmed: trim_body(b_body.as_bytes()),
            diff,
            class: class.to_string(),
            allowlisted: (!allowlisted.is_empty()).then(|| allowlisted.join(",")),
            curl: format!("{} ; {}", curl_of(op, vars, "A"), curl_of(op, vars, "B")),
            note,
        };
        Outcome {
            class: class.to_string(),
            record: Some(rec),
        }
    }

    fn gql_a_error(
        &self,
        op: &GqlOp,
        vars: &Map<String, Value>,
        a: &Target,
        b: &str,
        msg: &str,
    ) -> Outcome {
        self.gql_record(
            op,
            vars,
            a,
            b,
            None,
            None,
            "",
            Vec::new(),
            "a-error",
            &[],
            Some(msg.into()),
        )
    }

    /// GraphQL vs the reference server (oracle) at heights up to `max`.
    async fn gql_vs_oracle(
        &self,
        ta: &Target,
        tb: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
        max: u64,
    ) -> Outcome {
        let depth = self.cfg.follow_depth;
        let mut a = match self.gql_fetch(ta, op, vars, depth).await {
            Ok(a) => a,
            Err(e) => return self.gql_a_error(op, vars, ta, &tb.name, &format!("{e:#}")),
        };
        if a.status >= 500 {
            return self.gql_a_error(op, vars, ta, &tb.name, &format!("HTTP {}", a.status));
        }
        let mut b = match self.gql_fetch(tb, op, vars, depth).await {
            Ok(b) => {
                self.gql.note_oracle(false);
                b
            }
            Err(e) => {
                let timed_out = e.chain().any(|c| {
                    c.downcast_ref::<reqwest::Error>()
                        .is_some_and(reqwest::Error::is_timeout)
                });
                self.gql.note_oracle(timed_out);
                return self.gql_record(
                    op,
                    vars,
                    ta,
                    &tb.name,
                    Some(&a),
                    None,
                    "",
                    Vec::new(),
                    "b-error",
                    &[],
                    Some(format!("{e:#}")),
                );
            }
        };
        // Oracle server faults are its own (R3E-14 material), not a compare.
        if b.status >= 500 || b.error_codes().iter().any(|c| c == "INTERNAL_SERVER_ERROR") {
            return self.gql_record(
                op,
                vars,
                ta,
                &tb.name,
                Some(&a),
                Some(b.status),
                &b.raw,
                Vec::new(),
                "b-error",
                &[],
                Some("oracle server error".into()),
            );
        }
        let d = compare_answers(&mut a, &mut b, max, self.cfg.graphql.total_count_cap);
        self.gql_classify(op, vars, ta, &tb.name, &a, b.status, &b.raw, d)
    }

    #[allow(clippy::too_many_arguments)]
    fn gql_classify(
        &self,
        op: &GqlOp,
        vars: &Map<String, Value>,
        ta: &Target,
        b_name: &str,
        a: &Answer,
        b_status: u16,
        b_raw: &str,
        d: Vec<DiffEntry>,
    ) -> Outcome {
        if d.is_empty() {
            return Outcome {
                class: "match".into(),
                record: None,
            };
        }
        let (unexplained, ids) = self.allow.classify_request(
            &RouteKind::Other,
            &format!("graphql:{}", op.stem),
            &[],
            &d,
        );
        let class = if unexplained.is_empty() {
            "allowlisted"
        } else {
            "diff"
        };
        let shown: Vec<DiffEntry> = if class == "diff" { unexplained } else { d };
        self.gql_record(
            op,
            vars,
            ta,
            b_name,
            Some(a),
            Some(b_status),
            b_raw,
            shown,
            class,
            &ids,
            None,
        )
    }

    /// `pactQuery` and friends answer `NOT_SUPPORTED` (checked on A alone).
    async fn gql_not_supported(
        &self,
        ta: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
    ) -> Outcome {
        let a = match self.gql_fetch(ta, op, vars, 0).await {
            Ok(a) => a,
            Err(e) => return self.gql_a_error(op, vars, ta, "expected", &format!("{e:#}")),
        };
        if a.error_codes().iter().any(|c| c == "NOT_SUPPORTED") {
            return Outcome {
                class: "match".into(),
                record: None,
            };
        }
        let d = vec![DiffEntry {
            path: "/errors/0/code".into(),
            kind: DiffKind::Missing,
            a: None,
            b: Some(json!("NOT_SUPPORTED")),
        }];
        self.gql_record(
            op,
            vars,
            ta,
            "expected",
            Some(&a),
            None,
            "",
            d,
            "diff",
            &[],
            None,
        )
    }

    async fn gql_a_data(
        &self,
        ta: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
    ) -> Result<Value, Outcome> {
        match self.gql_fetch(ta, op, vars, 0).await {
            Ok(a) if a.status < 500 && a.body.get("errors").is_none() => Ok(a.body["data"].clone()),
            Ok(a) => Err(self.gql_record(
                op,
                vars,
                ta,
                "node",
                Some(&a),
                None,
                "",
                Vec::new(),
                "a-error",
                &[],
                Some(format!("HTTP {} errors {:?}", a.status, a.error_codes())),
            )),
            Err(e) => Err(self.gql_a_error(op, vars, ta, "node", &format!("{e:#}"))),
        }
    }

    /// GraphQL transaction fields against `/poll` and the command the node served in the block.
    async fn gql_node_tx(
        &self,
        ta: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
        s: &Sample,
    ) -> Outcome {
        let data = match self.gql_a_data(ta, op, vars).await {
            Ok(d) => d,
            Err(o) => return o,
        };
        let polled = match self
            .node
            .poll(s.chain, std::slice::from_ref(&s.request_key))
            .await
        {
            Ok(p) => p,
            Err(e) => {
                return self.gql_record(
                    op,
                    vars,
                    ta,
                    "node",
                    None,
                    None,
                    "",
                    Vec::new(),
                    "b-error",
                    &[],
                    Some(e.to_string()),
                )
            }
        };
        let Some(p) = polled.get(&s.request_key) else {
            return self.gql_record(
                op,
                vars,
                ta,
                "node",
                None,
                None,
                "",
                Vec::new(),
                "b-error",
                &[],
                Some("poll: not found".into()),
            );
        };
        let t = &data["transaction"];
        let ok = p.pointer("/result/status").and_then(Value::as_str) == Some("success");
        let mut got = json!({
            "hash": t["hash"],
            "chain": t.pointer("/cmd/meta/chainId"),
            "sender": t.pointer("/cmd/meta/sender"),
            "gas": text_decimal(t.pointer("/result/gas")),
            "height": t.pointer("/result/height"),
            "blockHash": t.pointer("/result/block/hash"),
            "blockChain": t.pointer("/result/block/chainId"),
            "failed": t.pointer("/result/badResult").map(|b| !b.is_null()),
        });
        let mut want = json!({
            "hash": s.request_key,
            "chain": s.chain,
            "sender": s.meta.get("sender"),
            "gas": text_decimal(p.get("gas")),
            "height": p.pointer("/metaData/blockHeight"),
            "blockHash": p.pointer("/metaData/blockHash"),
            "blockChain": s.chain,
            "failed": !ok,
        });
        for v in [&mut got, &mut want] {
            normalize_gql(v, u64::MAX);
        }
        let d = diff(&got, &want);
        self.gql_node_outcome(op, vars, ta, d, &want)
    }

    fn gql_node_outcome(
        &self,
        op: &GqlOp,
        vars: &Map<String, Value>,
        ta: &Target,
        d: Vec<DiffEntry>,
        want: &Value,
    ) -> Outcome {
        if d.is_empty() {
            return Outcome {
                class: "match".into(),
                record: None,
            };
        }
        let dummy = Answer {
            body: Value::Null,
            status: 200,
            raw: String::new(),
            ms: Vec::new(),
        };
        let (unexplained, ids) = self.allow.classify_request(
            &RouteKind::Other,
            &format!("graphql:{}", op.stem),
            &[],
            &d,
        );
        let class = if unexplained.is_empty() {
            "allowlisted"
        } else {
            "diff"
        };
        let shown = if unexplained.is_empty() {
            d
        } else {
            unexplained
        };
        self.gql_record(
            op,
            vars,
            ta,
            "node",
            Some(&dummy),
            Some(200),
            &want.to_string(),
            shown,
            class,
            &ids,
            None,
        )
    }

    /// GraphQL block fields against the canonical header the node served.
    async fn gql_node_block(
        &self,
        ta: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
        s: &Sample,
    ) -> Outcome {
        let data = match self.gql_a_data(ta, op, vars).await {
            Ok(d) => d,
            Err(o) => return o,
        };
        let b = &data["block"];
        let h = &s.header;
        let time_ok = |actual: Option<&Value>, micros: Option<&Value>| -> bool {
            let (Some(a), Some(m)) = (
                actual.and_then(Value::as_str),
                micros.and_then(Value::as_u64),
            ) else {
                return false;
            };
            match (parse_iso(a), parse_iso(&iso_micros(m))) {
                (Some(x), Some(y)) => iso_eq_coarse(&x, &y),
                _ => false,
            }
        };
        let mut got = json!({
            "hash": b["hash"], "height": b["height"], "chainId": text_decimal(b.get("chainId")),
            "payloadHash": b["payloadHash"], "parent": b.pointer("/parent/hash"),
            "nonce": b["nonce"], "weight": b["weight"], "target": b["target"],
            "creationTime": time_ok(b.get("creationTime"), h.get("creationTime")),
            "epoch": time_ok(b.get("epoch"), h.get("epochStart")),
        });
        let mut want = json!({
            "hash": h["hash"], "height": h["height"], "chainId": text_decimal(h.get("chainId")),
            "payloadHash": h["payloadHash"], "parent": h["parent"],
            "nonce": h["nonce"], "weight": h["weight"], "target": h["target"],
            "creationTime": true, "epoch": true,
        });
        for v in [&mut got, &mut want] {
            normalize_gql(v, u64::MAX);
        }
        let d = diff(&got, &want);
        self.gql_node_outcome(op, vars, ta, d, &want)
    }

    /// GraphQL chain balances and guards against `/local` `details` (H-1), retried once after a
    /// pause because both sides read a moving tip.
    async fn gql_node_balance(
        &self,
        ta: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
        _s: &Sample,
    ) -> Outcome {
        let local = kdx_node::LocalClient::new(
            self.node.clone(),
            kdx_node::LocalOpts {
                rps: self.cfg.node_max_rps.max(1),
                concurrency: 1,
            },
        );
        let account = vars
            .get("accountName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut last: Vec<DiffEntry> = Vec::new();
        for attempt in 0..2 {
            let data = match self.gql_a_data(ta, op, vars).await {
                Ok(d) => d,
                Err(o) => return o,
            };
            let rows = data
                .pointer("/fungibleAccount/chainAccounts")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut d = Vec::new();
            for r in rows.iter().take(3) {
                let Some(chain) = text_decimal(r.get("chainId")).and_then(|c| c.parse::<u8>().ok())
                else {
                    continue;
                };
                let det = match local.details(chain, "coin", &account).await {
                    Ok(kdx_node::Local::Found { value, .. }) => value,
                    Ok(kdx_node::Local::Absent) => {
                        d.push(DiffEntry {
                            path: format!("/chain{chain}"),
                            kind: DiffKind::Extra,
                            a: Some(r.clone()),
                            b: None,
                        });
                        continue;
                    }
                    Err(e) => {
                        return self.gql_record(
                            op,
                            vars,
                            ta,
                            "node",
                            None,
                            None,
                            "",
                            Vec::new(),
                            "b-error",
                            &[],
                            Some(e.to_string()),
                        );
                    }
                };
                if !same_decimal(
                    &text_decimal(r.get("balance")).unwrap_or_default(),
                    &det.balance,
                ) {
                    d.push(DiffEntry {
                        path: format!("/chain{chain}/balance"),
                        kind: DiffKind::Changed,
                        a: r.get("balance").cloned(),
                        b: Some(json!(det.balance)),
                    });
                }
                if let Some(g) = r.get("guard").filter(|g| g.get("keys").is_some()) {
                    let keys = |v: &Value| {
                        let mut k: Vec<String> = v
                            .get("keys")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(|x| x.as_str().map(str::to_string))
                            .collect();
                        k.sort();
                        k
                    };
                    let pred = |v: &Value| v.get("predicate").or_else(|| v.get("pred")).cloned();
                    if keys(g) != keys(&det.guard) || pred(g) != pred(&det.guard) {
                        d.push(DiffEntry {
                            path: format!("/chain{chain}/guard"),
                            kind: DiffKind::Changed,
                            a: Some(g.clone()),
                            b: Some(det.guard.clone()),
                        });
                    }
                }
            }
            if d.is_empty() {
                return Outcome {
                    class: "match".into(),
                    record: None,
                };
            }
            last = d;
            if attempt == 0 {
                tokio::time::sleep(BALANCE_RETRY).await;
            }
        }
        self.gql_node_outcome(op, vars, ta, last, &Value::Null)
    }

    /// The internal pair: GraphQL chain balances against the native balances API.
    async fn gql_native_balance(
        &self,
        ta: &Target,
        tb: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
    ) -> Outcome {
        let data = match self.gql_a_data(ta, op, vars).await {
            Ok(d) => d,
            Err(o) => return o,
        };
        let account = vars
            .get("accountName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut d = Vec::new();
        for r in data
            .pointer("/fungibleAccount/chainAccounts")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .take(3)
        {
            let Some(chain) = text_decimal(r.get("chainId")) else {
                continue;
            };
            let path = format!(
                "/api/v1/accounts/{}/balances?token=coin&chain={chain}",
                crate::gen::enc_segment(account)
            );
            let cap = match tb.get(&path).await {
                Ok(c) if c.status == 200 => c,
                Ok(c) => {
                    return self.gql_record(
                        op,
                        vars,
                        ta,
                        &tb.name,
                        None,
                        Some(c.status),
                        &trim_body(&c.body),
                        Vec::new(),
                        "b-error",
                        &[],
                        None,
                    )
                }
                Err(e) => {
                    return self.gql_record(
                        op,
                        vars,
                        ta,
                        &tb.name,
                        None,
                        None,
                        "",
                        Vec::new(),
                        "b-error",
                        &[],
                        Some(format!("{e:#}")),
                    )
                }
            };
            self.gql_count(&op.route(), &tb.name, &[cap.ms]);
            let row = cap
                .json()
                .and_then(|v| v.get("items").and_then(Value::as_array).cloned())
                .and_then(|rows| {
                    rows.into_iter().find(|x| {
                        x.get("module").and_then(Value::as_str) == Some("coin")
                            && x.get("chain")
                                .and_then(Value::as_u64)
                                .map(|c| c.to_string())
                                == Some(chain.clone())
                    })
                });
            let want = row
                .as_ref()
                .and_then(|x| text_decimal(x.get("balance")))
                .unwrap_or_default();
            if !same_decimal(&text_decimal(r.get("balance")).unwrap_or_default(), &want) {
                d.push(DiffEntry {
                    path: format!("/chain{chain}/balance"),
                    kind: DiffKind::Changed,
                    a: r.get("balance").cloned(),
                    b: Some(json!(want)),
                });
            }
        }
        self.gql_node_outcome(op, vars, ta, d, &Value::Null)
    }
}

fn total_count(v: &Value) -> Option<u64> {
    match v {
        Value::Object(m) => m
            .get("totalCount")
            .and_then(Value::as_u64)
            .or_else(|| m.values().find_map(total_count)),
        _ => None,
    }
}

/// A scalar's text: a string as is, a number by its shortest decimal text.
fn text_decimal(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn same_decimal(a: &str, b: &str) -> bool {
    match (decimal_canon(a), decimal_canon(b)) {
        (Some(x), Some(y)) => x == y,
        _ => a == b,
    }
}

/// `run` loop: one task taking GraphQL steps until `stopping`.
pub async fn run_loop(ctx: Arc<Ctx>) {
    // Every (pair, operation) shows in the summary from the start, so a gate sees an operation
    // that never got a compare as "0 compared" instead of not at all.
    {
        let mut s = ctx.summary.lock().expect("summary");
        for (pi, op) in ctx.gql_jobs() {
            let (a, b) = &ctx.cfg.pairs[pi];
            s.pair(&op.route(), a, b, "planned");
        }
    }
    while !ctx.stopping.load(Ordering::Relaxed) {
        if let Err(e) = ctx.gql_step().await {
            tracing::warn!(error = %format!("{e:#}"), "graphql step");
            ctx.count_class("graphql", "gen-error");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

/// `once --graphql <stem>`: one compare of that operation per configured pair.
pub async fn once(cfg: CompareConfig, stem: &str) -> anyhow::Result<Vec<Outcome>> {
    let ctx = Ctx::new(cfg).await?;
    let op = ctx
        .gql
        .ops
        .iter()
        .find(|o| o.stem == stem)
        .cloned()
        .with_context(|| format!("no operation {stem:?} among {} loaded", ctx.gql.ops.len()))?;
    let mut out = Vec::new();
    for (a, b) in &ctx.cfg.pairs {
        let Some(ta) = ctx
            .targets
            .get(a)
            .filter(|t| t.kind == TargetKind::KdaIndexerGraphql)
        else {
            continue;
        };
        let tb = ctx.targets.get(b);
        let sample = ctx.gql_sample().await?.context("no sample")?;
        let vars = ctx
            .gql_vars(&op, &sample)
            .context("variables unavailable")?;
        let o = match (op.kind, tb) {
            (OpKind::NotSupported, _) => ctx.gql_not_supported(ta, &op, &vars).await,
            (OpKind::Oracle, Some(tb)) => {
                let max = ctx.cfg.targets[&tb.name].max_height.unwrap_or(u64::MAX);
                ctx.gql_vs_oracle(ta, tb, &op, &vars, max).await
            }
            (OpKind::NodeTx, None) => ctx.gql_node_tx(ta, &op, &vars, &sample).await,
            (OpKind::NodeBlock, None) => ctx.gql_node_block(ta, &op, &vars, &sample).await,
            (OpKind::NodeBalance, None) => ctx.gql_node_balance(ta, &op, &vars, &sample).await,
            (OpKind::NodeBalance, Some(tb)) => ctx.gql_native_balance(ta, tb, &op, &vars).await,
            _ => continue,
        };
        out.push(o);
    }
    Ok(out)
}

/// `gen --source graphql`: prints `count` sampled (operation, variables) draws.
pub async fn gen_debug(cfg: &CompareConfig, count: usize, print: bool) -> anyhow::Result<()> {
    let node = cfg.node_client()?;
    let graph = node.chain_graph().await?;
    let tips = Arc::new(Tips::default());
    tips.refresh(&node, None).await?;
    let state = GqlState::new(cfg)?;
    anyhow::ensure!(
        !state.ops.is_empty(),
        "no GraphQL operations loaded (graphql pairs configured?)"
    );
    let cap = cfg
        .targets
        .values()
        .filter(|t| t.kind == TargetKind::GraphqlOracle)
        .filter_map(|t| t.max_height)
        .min();
    let mut sampler = Sampler::new(RandomGen::new(node, graph, tips, cfg.min_depth), cap);
    let mut rng = StdRng::from_rng(&mut rand::rng());
    let mut by_op: BTreeMap<String, usize> = BTreeMap::new();
    let mut n = 0;
    while n < count {
        let Some(s) = sampler.next().await? else {
            continue;
        };
        let op = &state.ops[n % state.ops.len()];
        n += 1;
        let Some(v) = vars_for(op, &s, &mut rng) else {
            *by_op
                .entry(format!("{} (no variables)", op.route()))
                .or_default() += 1;
            continue;
        };
        *by_op.entry(op.route()).or_default() += 1;
        if print {
            println!("{:<44} {:?} {}", op.route(), op.kind, Value::Object(v));
        }
    }
    println!("{n} draws by operation {by_op:?}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(v: Value) -> Value {
        let mut v = v;
        normalize_gql(&mut v, 10_000);
        v
    }

    #[test]
    fn parse_variables_and_roots() {
        let op = parse_op(
            "BlocksByHeight-7d3a4973",
            "# kdx-compare note\nquery X($startHeight: Int!, $chainIds: [String!], $first: Int = 5) {\n blocksFromHeight(startHeight: $startHeight, chainIds: $chainIds, first: $first) { edges { node { hash } } }\n lastBlockHeight\n}",
            true,
        )
        .unwrap();
        let names: Vec<_> = op
            .vars
            .iter()
            .map(|v| (v.name.as_str(), v.required))
            .collect();
        assert_eq!(
            names,
            [("startHeight", true), ("chainIds", false), ("first", false)]
        );
        assert_eq!(op.roots, ["blocksFromHeight", "lastBlockHeight"]);
        // The tip root makes the document tip-relative even though it has a height variable.
        assert_eq!(op.kind, OpKind::Tip);
        let ok = parse_op(
            "a",
            "query($requestKey: String!) { transaction(requestKey: $requestKey) { hash } }",
            false,
        )
        .unwrap();
        assert_eq!(ok.kind, OpKind::Oracle);
        let ns = parse_op(
            "p",
            "query($q: [String!]!) { pactQuery(pactQuery: $q) { chainId } }",
            true,
        )
        .unwrap();
        assert_eq!(ns.kind, OpKind::NotSupported);
        let sub = parse_op("s", "subscription { newBlocks { hash } }", true).unwrap();
        assert_eq!(sub.kind, OpKind::Subscription);
    }

    #[test]
    fn cursors_dropped_and_ties_are_sets() {
        let a = json!({"edges": [
            {"cursor": "x", "node": {"height": 5, "hash": "b"}},
            {"cursor": "y", "node": {"height": 5, "hash": "a"}},
            {"cursor": "z", "node": {"height": 4, "hash": "c"}}],
            "pageInfo": {"startCursor": "1", "endCursor": "2", "hasNextPage": false}});
        let b = json!({"edges": [
            {"cursor": "p", "node": {"height": 5, "hash": "a"}},
            {"cursor": "q", "node": {"height": 5, "hash": "b"}},
            {"cursor": "r", "node": {"height": 4, "hash": "c"}}],
            "pageInfo": {"startCursor": "7", "endCursor": "8", "hasNextPage": false}});
        assert!(diff(&norm(a), &norm(b)).is_empty());
        // Different sort keys are not a tie: the order matters.
        let c = json!({"edges": [{"node": {"height": 4, "hash": "c"}}, {"node": {"height": 5, "hash": "a"}}]});
        let d = json!({"edges": [{"node": {"height": 5, "hash": "a"}}, {"node": {"height": 4, "hash": "c"}}]});
        assert!(!diff(&norm(c), &norm(d)).is_empty());
    }

    #[test]
    fn numbers_by_decimal_value_and_big_ints_either_way() {
        assert!(diff(&norm(json!({"x": 9.99})), &norm(json!({"x": 9.990}))).is_empty());
        assert!(diff(
            &norm(json!({"d": 32876630560156508u64})),
            &norm(json!({"d": "32876630560156508"}))
        )
        .is_empty());
        // A small number against a string is a format difference.
        assert!(!diff(&norm(json!({"g": 600})), &norm(json!({"g": "600"}))).is_empty());
        assert!(!diff(&norm(json!({"g": 600})), &norm(json!({"g": 601}))).is_empty());
    }

    #[test]
    fn total_count_is_capped_and_errors_reduce_to_codes() {
        assert!(diff(
            &norm(json!({"totalCount": 15000})),
            &norm(json!({"totalCount": 10000}))
        )
        .is_empty());
        assert!(!diff(
            &norm(json!({"totalCount": 9})),
            &norm(json!({"totalCount": 10}))
        )
        .is_empty());
        let a = json!({"errors": [{"message": "x", "extensions": {"code": "BAD_USER_INPUT"}}]});
        let b = json!({"errors": [{"message": "y", "code": "BAD_USER_INPUT"}]});
        assert!(diff(&norm(a), &norm(b)).is_empty());
    }

    #[test]
    fn rows_above_the_cap_are_dropped() {
        let mut d = json!({"c": {"edges": [{"node": {"height": 9}}, {"node": {"result": {"height": 3}}}], "pageInfo": {"hasNextPage": true}}});
        assert_eq!(drop_above(&mut d, 5), 1);
        assert_eq!(edges_len(&mut d), 1);
    }

    #[test]
    fn variables_follow_the_sample() {
        let op = parse_op(
            "t",
            "query($accountName: String, $startHeight: Int!, $chainIds: [String!], $first: Int, $after: String) { transactions(accountName: $accountName) { totalCount } }",
            false,
        )
        .unwrap();
        let s = Sample {
            chain: 3,
            height: 100,
            block_hash: "H".into(),
            request_key: "R".into(),
            accounts: vec!["k:a".into()],
            header: Value::Null,
            meta: Value::Null,
        };
        let mut rng = StdRng::seed_from_u64(1);
        let v = vars_for(&op, &s, &mut rng).unwrap();
        assert_eq!(v["accountName"], "k:a");
        assert_eq!(v["startHeight"], 100);
        assert_eq!(v["chainIds"], json!(["3"]));
        assert_eq!(v["first"], 10);
        assert!(v["after"].is_null());
        let none = Sample {
            accounts: vec![],
            ..s
        };
        let strict = parse_op(
            "u",
            "query($accountName: String!) { fungibleAccount(accountName: $accountName) { id } }",
            false,
        )
        .unwrap();
        assert!(vars_for(&strict, &none, &mut rng).is_none());
    }

    #[test]
    fn our_own_operations_load_with_the_right_kinds() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/graphql");
        let ops = load_dir(&dir, false, &[]).unwrap();
        let kind = |s: &str| ops.iter().find(|o| o.stem == s).map(|o| o.kind);
        assert_eq!(kind("NodeTx"), Some(OpKind::NodeTx));
        assert_eq!(kind("NodeBlock"), Some(OpKind::NodeBlock));
        assert_eq!(kind("NodeBalance"), Some(OpKind::NodeBalance));
        assert_eq!(kind("QLastBlockHeight"), Some(OpKind::Tip));
        // One per v1 root field with inputs: all of them are height-bounded oracle documents.
        let roots: std::collections::BTreeSet<_> = ops
            .iter()
            .filter(|o| o.kind == OpKind::Oracle)
            .flat_map(|o| o.roots.clone())
            .collect();
        for r in [
            "block",
            "blocksFromHeight",
            "transaction",
            "transactions",
            "transactionsByPublicKey",
            "transfers",
            "events",
            "fungibleAccount",
            "fungibleChainAccount",
            "fungibleChainAccounts",
            "fungibleAccountsByPublicKey",
            "nonFungibleAccount",
            "node",
        ] {
            assert!(roots.contains(r), "no own operation for {r}");
        }
        assert!(ops.iter().all(|o| o.kind != OpKind::Subscription));
    }

    #[test]
    fn captured_example_variables_and_names() {
        let t = "# x\n\n## query\nquery A { a }\n\n## example variables\n{\"accountName\":\"k:<account>\",\"first\":10,\"last\":null}\n\n## response shape\n{ \"data\": {} }";
        let m = example_vars(t);
        assert_eq!(m["first"], 10);
        assert!(m["last"].is_null());
        assert_eq!(m.len(), 3);
    }

    /// A mock GraphQL server whose `blocksFromHeight` connection has 7 rows served 3 at a time,
    /// with opaque cursors; `skew` makes it answer 2 rows per page and hides the last one.
    async fn mock(skew: bool) -> String {
        use axum::{routing::post, Json, Router};
        let app = Router::new().route(
            "/graphql",
            post(move |Json(req): Json<Value>| async move {
                let after = req["variables"]["after"]
                    .as_str()
                    .and_then(|c| c.strip_prefix("c"))
                    .and_then(|n| n.parse::<usize>().ok())
                    .unwrap_or(0);
                let size = if skew { 2 } else { 3 };
                let total = 7;
                let end = (after + size).min(total);
                let edges: Vec<Value> = (after..end)
                    .map(|i| json!({"cursor": format!("c{}", i + 1), "node": {"height": 100 + i, "hash": format!("h{i}")}}))
                    .collect();
                Json(json!({"data": {"blocksFromHeight": {"edges": edges,
                    "pageInfo": {"hasNextPage": end < total, "endCursor": format!("c{end}")}}}}))
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        format!("http://{addr}/graphql")
    }

    fn target(url: &str, name: &str) -> Target {
        Target::new(
            name,
            url.parse().unwrap(),
            TargetKind::KdaIndexerGraphql,
            Duration::from_secs(5),
            crate::target::Limiter::new(1000.0),
            1 << 20,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn pages_are_followed_and_a_page_size_cut_is_an_allowlisted_hit() {
        let op = parse_op(
            "QBlocksFromHeight",
            "query($startHeight: Int!, $first: Int, $after: String) { blocksFromHeight(startHeight: $startHeight, first: $first, after: $after) { edges { cursor node { height hash } } pageInfo { hasNextPage endCursor } } }",
            false,
        )
        .unwrap();
        let mut vars = Map::new();
        vars.insert("startHeight".into(), json!(100));
        vars.insert("first".into(), json!(3));
        vars.insert("after".into(), Value::Null);
        let (ta, tb) = (
            target(&mock(false).await, "a"),
            target(&mock(true).await, "b"),
        );
        // Depth 1 follows one more page: 3 + 3 rows on A, 2 + 2 on B.
        let mut a = fetch_pages(&ta, &op, &vars, 1).await.unwrap();
        let mut b = fetch_pages(&tb, &op, &vars, 1).await.unwrap();
        assert_eq!(edges_len(&mut a.body["data"]), 6);
        assert_eq!(edges_len(&mut b.body["data"]), 4);
        assert_eq!(a.ms.len(), 2);
        // The longer side is cut to the shorter one; the only entry is the visible hit.
        let d = compare_answers(&mut a, &mut b, u64::MAX, 10_000);
        assert_eq!(
            d.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(),
            [PAGE_CUT_PATH]
        );
        // Everything followed: both have all 7 rows and match without any hit.
        let mut a = fetch_pages(&ta, &op, &vars, 5).await.unwrap();
        let mut b = fetch_pages(&tb, &op, &vars, 5).await.unwrap();
        assert!(compare_answers(&mut a, &mut b, u64::MAX, 10_000).is_empty());
        // Rows above the oracle's ceiling are dropped from both sides and recorded.
        let mut a = fetch_pages(&ta, &op, &vars, 5).await.unwrap();
        let mut b = fetch_pages(&tb, &op, &vars, 5).await.unwrap();
        let d = compare_answers(&mut a, &mut b, 103, 10_000);
        assert_eq!(
            d.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(),
            [ORACLE_STALL_PATH]
        );
        assert_eq!(edges_len(&mut a.body["data"]), 4);
    }

    #[test]
    fn the_shipped_allowlist_covers_exactly_the_visible_graphql_hits() {
        let allow = crate::allowlist::Allowlist::load(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../compare/allowlist.toml")
                .as_path(),
        )
        .unwrap();
        let entry = |path: &str, kind| DiffEntry {
            path: path.into(),
            kind,
            a: None,
            b: None,
        };
        let hits = [PAGE_CUT_PATH, TOTALCOUNT_CAP_PATH, ORACLE_STALL_PATH]
            .map(|p| entry(p, DiffKind::Changed));
        let (un, ids) = allow.classify_request(&RouteKind::Other, "graphql:QBlock", &[], &hits);
        assert!(un.is_empty(), "{un:?}");
        assert_eq!(ids.len(), 3);
        // A real difference on a GraphQL request is not allowlisted.
        let real = [entry("/data/block/hash", DiffKind::Changed)];
        let (un, _) = allow.classify_request(&RouteKind::Other, "graphql:QBlock", &[], &real);
        assert_eq!(un.len(), 1);
    }

    #[test]
    fn base64_matches_the_relay_id_form() {
        assert_eq!(base64_std(b"Block:S6LBhIyO"), "QmxvY2s6UzZMQmhJeU8=");
    }
}
