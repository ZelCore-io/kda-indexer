//! R-P5P9-62: `kda-compare referee`, the CG-3T method as a harness subcommand, so the FV
//! daily read needs no ad hoc scripts.
//!
//! It reads stored diff logs (`diffs-YYYY-MM-DD.jsonl`) after a run and takes every GraphQL
//! record the run left unexplained (class `diff`). For each oracle-pair record it re-runs, on
//! the stored answers, the live compare's own rules ([`Ctx::gql_rules`], with the document):
//! the row rules, H8, the `payload.data` matcher, H7 and the diff with the ruled field
//! matchers (H9 among them), then classifies with the allowlist as the run does. It never
//! asks the oracle: H6 (finishing cut nested lists) needs it, so the stored record's H6
//! entries (a harness limit, an unfinished list) are carried into the result and keep the
//! record unexplained (R-P5P9-64 M-1).
//!
//! Nothing is dropped from the verdict (R-P5P9-64 I-4): a line that is not a diff record,
//! a GraphQL diff record whose B target this config does not know as an oracle, a record
//! that cannot be re-run and every record after the read cap is spent (M-8) are listed as
//! not refereed with the reason, and count in the exit code.
//!
//! Node reads are single-row, paced by the node client's limiter at `rps` reads a second
//! (header-by-hash reads included, R-P5P9-65 (2)), and capped at `max_reads` for the whole
//! run (plus H7's cap per compare): the node's chain graph at the start, the row rules', H8's,
//! the `payload.data` matcher's and H7's. A record the cap leaves unsettled stays
//! unexplained and says so. The output counts the classes and lists every record still
//! unexplained with its evidence.

use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use anyhow::Context;
use serde::Serialize;
use serde_json::{json, Map, Value};

use crate::config::{CompareConfig, TargetKind};
use crate::graphql::{Answer, H6};
use crate::normalize::RouteKind;
use crate::report::{DiffEntry, DiffRecord};
use crate::run::Ctx;

/// `by_class` of the records listed as not refereed for want of reads (R-P5P9-64 M-8).
pub const UNREAD: &str = "not refereed: unread, the node read cap is reached";

/// Defaults of the FV daily read (R-P5P9-61 ruling (4)).
pub const DEFAULT_MAX_READS: usize = 6_000;
pub const DEFAULT_RPS: u32 = 10;
/// Unexplained diff entries listed per record at most (the count is always given).
pub const MAX_SHOWN: usize = 50;

pub struct Opts {
    pub logs: Vec<PathBuf>,
    pub max_reads: usize,
    pub rps: u32,
}

#[derive(Debug, Default, Serialize)]
pub struct Report {
    /// Non-empty lines read from the logs.
    pub records: usize,
    /// GraphQL records the run left unexplained (class `diff`): the ones taken up.
    pub refereed: usize,
    pub explained: usize,
    pub unexplained: usize,
    /// Lines and taken-up records that could not be re-run (a line that is not a diff
    /// record, a B target that is not an oracle here, a body the log cut, an operation not
    /// loaded, the read cap spent, …); they stay unexplained, each listed with its reason.
    /// `explained + unexplained + not_refereed` = `refereed` + the lines that did not parse.
    pub not_refereed: usize,
    /// Per class: each allowlist id among the explained records, `unexplained: <kind>`
    /// (`h7-row`: a row the node settles against ours; `h6-unfinished`: a cut nested list
    /// the live run could not page to its end, e.g. the oracle's `Block.events` cursor;
    /// `harness-limit`: one it left unread for its page budget or `MAX_PAGES`;
    /// `h7-unsettled`: a read failed, a cap was reached or an identity was undecidable;
    /// `other`: any other difference) and `not refereed: <why>`.
    pub by_class: BTreeMap<String, usize>,
    pub node_reads: usize,
    pub node_read_cap: usize,
    /// Every record still unexplained, with its evidence.
    pub still_unexplained: Vec<Value>,
}

impl Report {
    /// The verdict: true only when every record taken up was explained and every line read
    /// (R-P5P9-64 I-4); `kda-compare referee` exits non-zero otherwise.
    pub fn passed(&self) -> bool {
        self.unexplained == 0 && self.not_refereed == 0
    }
}

/// What the rules make of one stored record.
struct Settled {
    unexplained: Vec<DiffEntry>,
    ids: Vec<String>,
    note: Option<String>,
    /// The hits that carry evidence.
    hits: Vec<DiffEntry>,
}

/// Re-referees the stored records of `opts.logs` (see the module docs).
pub async fn run(mut cfg: CompareConfig, opts: &Opts) -> anyhow::Result<Report> {
    // The node client's own limiter paces every read.
    cfg.node_max_rps = opts.rps.max(1);
    let ctx = Ctx::new(cfg).await?;
    // `Ctx::new` read the node's chain graph: one read of the cap (R-P5P9-64 M-8).
    ctx.gql
        .node_budget
        .store(opts.max_reads.saturating_sub(1), Ordering::Relaxed);
    let mut rep = Report {
        node_read_cap: opts.max_reads,
        ..Report::default()
    };
    let count = |rep: &mut Report, class: String| *rep.by_class.entry(class).or_default() += 1;
    let not_refereed = |rep: &mut Report, mut v: Value, class: &str, why: String| {
        rep.not_refereed += 1;
        count(rep, class.to_string());
        v["class"] = json!("not refereed");
        v["why"] = json!(why);
        rep.still_unexplained.push(v);
    };
    for path in &opts.logs {
        let f = std::fs::File::open(path).with_context(|| format!("reading {}", path.display()))?;
        for (n, line) in std::io::BufReader::new(f).lines().enumerate() {
            let line = line.with_context(|| format!("reading {}", path.display()))?;
            if line.trim().is_empty() {
                continue;
            }
            rep.records += 1;
            let at = format!("{}:{}", path.display(), n + 1);
            // R-P5P9-64 I-4: a line that does not parse (a cut append, a disk that filled)
            // may be a diff record: listed, never dropped.
            let rec = match serde_json::from_str::<DiffRecord>(&line) {
                Ok(rec) => rec,
                Err(e) => {
                    let why = format!("the line is not a diff record: {e}");
                    let class = "not refereed: not a diff record";
                    not_refereed(&mut rep, json!({ "at": at }), class, why);
                    continue;
                }
            };
            if rec.class != "diff" || !rec.route.starts_with("graphql:") {
                continue;
            }
            rep.refereed += 1;
            let head = json!({"at": at, "ts": rec.ts, "request": rec.request,
                "a": rec.a_target, "b": rec.b_target});
            let oracle = match ctx.targets.get(&rec.b_target) {
                None => Err(format!("B target {} is not configured here", rec.b_target)),
                Some(t) if t.kind != TargetKind::GraphqlOracle => Err(format!(
                    "B target {} is not a GraphQL oracle here (only oracle pairs are refereed)",
                    rec.b_target
                )),
                Some(_) => Ok(()),
            };
            if let Err(why) = oracle {
                let class = "not refereed: B is not an oracle in this config";
                not_refereed(&mut rep, head, class, why);
                continue;
            }
            // R-P5P9-64 M-8: once the cap is spent, every record left is unread.
            if ctx.gql.node_budget.load(Ordering::Relaxed) == 0 {
                let why = format!("the cap of {} node reads is spent", opts.max_reads);
                not_refereed(&mut rep, head, UNREAD, why);
                continue;
            }
            match settle(&ctx, &rec).await {
                Err(why) => {
                    let class = format!("not refereed: {why}");
                    not_refereed(&mut rep, head, &class, why);
                }
                Ok(s) if s.unexplained.is_empty() => {
                    rep.explained += 1;
                    for id in s.ids {
                        count(&mut rep, id);
                    }
                }
                Ok(s) => {
                    rep.unexplained += 1;
                    let has = |p: fn(&str) -> bool| s.unexplained.iter().any(|e| p(&e.path));
                    let kind = if has(|p| p == crate::graphql_window::ROW_PATH) {
                        "h7-row"
                    } else if has(|p| p == crate::graphql_scope::UNFINISHED_PATH) {
                        "h6-unfinished"
                    } else if has(|p| p.starts_with("/~harness-limit/")) {
                        "harness-limit"
                    } else if s.note.as_deref().is_some_and(|n| n.starts_with("H7 ")) {
                        "h7-unsettled"
                    } else {
                        "other"
                    };
                    count(&mut rep, format!("unexplained: {kind}"));
                    let mut v = head;
                    v["class"] = json!(kind);
                    v["note"] = json!(s.note);
                    v["unexplainedCount"] = json!(s.unexplained.len());
                    v["unexplained"] =
                        json!(s.unexplained.iter().take(MAX_SHOWN).collect::<Vec<_>>());
                    v["explainedBy"] = json!(s.ids);
                    v["evidence"] = json!(s.hits);
                    rep.still_unexplained.push(v);
                }
            }
        }
    }
    rep.node_reads = opts
        .max_reads
        .saturating_sub(ctx.gql.node_budget.load(Ordering::Relaxed));
    Ok(rep)
}

/// One stored record through the rules; `Err` when it cannot be re-run.
async fn settle(ctx: &Ctx, rec: &DiffRecord) -> Result<Settled, String> {
    if rec.body_cap.is_some() {
        return Err("a body was cut by the log's cap".into());
    }
    let (stem, vars) = rec
        .request
        .split_once(' ')
        .ok_or("no variables in the request")?;
    let vars: Map<String, Value> =
        serde_json::from_str(vars).map_err(|e| format!("the variables are not JSON: {e}"))?;
    let op = ctx
        .gql
        .ops
        .iter()
        .find(|o| o.stem == stem)
        .ok_or_else(|| format!("operation {stem} is not loaded"))?;
    let ta = ctx
        .targets
        .get(&rec.a_target)
        .ok_or("ours' target is not configured")?;
    // H7's short-page check needs the pages ours was fetched in (R-P5P9-64 I-3).
    let pages = rec
        .a_pages
        .ok_or("the record does not say how many pages ours was fetched in")?;
    let answer = |text: &str, status: Option<u16>, pages: usize| -> Result<Answer, String> {
        Ok(Answer {
            body: serde_json::from_str(text).map_err(|e| format!("a body is not JSON: {e}"))?,
            status: status.unwrap_or(200),
            raw: String::new(),
            ms: vec![0; pages],
        })
    };
    let mut a = answer(&rec.a_body_trimmed, rec.a_status, pages)?;
    let mut b = answer(&rec.b_body_trimmed, rec.b_status, 0)?;
    let max = ctx
        .cfg
        .targets
        .get(&rec.b_target)
        .and_then(|t| t.max_height)
        .unwrap_or(u64::MAX);
    // The live compare's rules with the document (R-P5P9-65 (1)); H6 from the stored diff.
    let (d, note) = ctx
        .gql_rules(ta, H6::Stored(&rec.diff), op, &vars, &mut a, &mut b, max)
        .await;
    let (unexplained, ids) = ctx
        .allow
        .classify_request(&RouteKind::Other, &rec.route, &[], &d);
    let hits = d
        .into_iter()
        .filter(|e| e.path.starts_with("/~") && (e.a.is_some() || e.b.is_some()))
        .collect();
    Ok(Settled {
        unexplained,
        ids,
        note,
        hits,
    })
}
