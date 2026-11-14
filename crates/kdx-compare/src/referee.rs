//! R-P5P9-62: `kda-compare referee`, the CG-3T method as a harness subcommand, so the FV
//! daily read needs no ad hoc scripts.
//!
//! It reads stored diff logs (`diffs-YYYY-MM-DD.jsonl`) after a run and takes every GraphQL
//! oracle-pair record the run left unexplained (class `diff`). For each it re-runs, on the
//! stored answers, the row rules and the node-refereed rules: H8, the `payload.data`
//! matcher, H7 and the diff with the ruled field matchers (H9 among them), then classifies
//! with the allowlist as the run does. It never asks the oracle: H6 (finishing cut nested
//! lists) needs the oracle, so a record that needed it stays unexplained here.
//!
//! Node reads are single-row, paced by the node client's limiter at `rps` reads a second,
//! and capped at `max_reads` for the whole run (plus H7's cap per compare); a record the cap
//! leaves unsettled stays unexplained and says so. The output counts the classes and lists
//! every record still unexplained with its evidence.

use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use anyhow::Context;
use serde::Serialize;
use serde_json::{json, Map, Value};

use crate::config::{CompareConfig, TargetKind};
use crate::graphql::Answer;
use crate::normalize::RouteKind;
use crate::report::{DiffEntry, DiffKind, DiffRecord};
use crate::run::Ctx;

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
    /// Records read from the logs.
    pub records: usize,
    /// GraphQL oracle-pair records the run left unexplained: the ones refereed.
    pub refereed: usize,
    pub explained: usize,
    pub unexplained: usize,
    /// Refereed records that could not be re-run (a body the log cut, an operation not
    /// loaded, …); they stay unexplained.
    pub not_refereed: usize,
    /// Per class: each allowlist id among the explained records, `unexplained: <kind>`
    /// (`h7-row`: a row the node settles against ours; `h7-unsettled`: a read failed, a cap
    /// was reached or an identity was undecidable; `other`: any other difference) and
    /// `not refereed: <why>`.
    pub by_class: BTreeMap<String, usize>,
    pub node_reads: usize,
    pub node_read_cap: usize,
    /// Every record still unexplained, with its evidence.
    pub still_unexplained: Vec<Value>,
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
    ctx.gql.node_budget.store(opts.max_reads, Ordering::Relaxed);
    let mut rep = Report {
        node_read_cap: opts.max_reads,
        ..Report::default()
    };
    let count = |rep: &mut Report, class: String| *rep.by_class.entry(class).or_default() += 1;
    for path in &opts.logs {
        let f = std::fs::File::open(path).with_context(|| format!("reading {}", path.display()))?;
        for line in std::io::BufReader::new(f).lines() {
            let line = line.with_context(|| format!("reading {}", path.display()))?;
            if line.trim().is_empty() {
                continue;
            }
            rep.records += 1;
            let Ok(rec) = serde_json::from_str::<DiffRecord>(&line) else {
                count(&mut rep, "skipped: not a diff record".into());
                continue;
            };
            let oracle = ctx
                .targets
                .get(&rec.b_target)
                .is_some_and(|t| t.kind == TargetKind::GraphqlOracle);
            if rec.class != "diff" || !rec.route.starts_with("graphql:") || !oracle {
                continue;
            }
            rep.refereed += 1;
            let head = json!({"ts": rec.ts, "request": rec.request, "a": rec.a_target,
                "b": rec.b_target});
            match settle(&ctx, &rec).await {
                Err(why) => {
                    rep.not_refereed += 1;
                    count(&mut rep, format!("not refereed: {why}"));
                    let mut v = head;
                    v["class"] = json!("not refereed");
                    v["why"] = json!(why);
                    rep.still_unexplained.push(v);
                }
                Ok(s) if s.unexplained.is_empty() => {
                    rep.explained += 1;
                    for id in s.ids {
                        count(&mut rep, id);
                    }
                }
                Ok(s) => {
                    rep.unexplained += 1;
                    let kind = if s
                        .unexplained
                        .iter()
                        .any(|e| e.path == crate::graphql_window::ROW_PATH)
                    {
                        "h7-row"
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
    let answer = |text: &str, status: Option<u16>| -> Result<Answer, String> {
        Ok(Answer {
            body: serde_json::from_str(text).map_err(|e| format!("a body is not JSON: {e}"))?,
            status: status.unwrap_or(200),
            raw: String::new(),
            ms: Vec::new(),
        })
    };
    let mut a = answer(&rec.a_body_trimmed, rec.a_status)?;
    let mut b = answer(&rec.b_body_trimmed, rec.b_status)?;
    let max = ctx
        .cfg
        .targets
        .get(&rec.b_target)
        .and_then(|t| t.max_height)
        .unwrap_or(u64::MAX);
    let root_scoped = crate::graphql_scope::root_scoped(&op.roots, &vars);
    let rows = ctx.gql_row_rules(ta, &vars, &mut a, &mut b).await;
    let (mut d, note) = ctx
        .gql_settle(ta, &vars, &mut a, &mut b, max, root_scoped)
        .await;
    d.extend(rows.into_iter().map(|p| DiffEntry {
        path: p.into(),
        kind: DiffKind::Changed,
        a: None,
        b: None,
    }));
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
