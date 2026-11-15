//! R-P5P9-62: `kda-compare referee`, the CG-3T method as a harness subcommand, so the FV
//! daily read needs no ad hoc scripts.
//!
//! It reads stored diff logs (`diffs-YYYY-MM-DD.jsonl`) after a run and takes every GraphQL
//! record the run left unexplained (class `diff`). For each oracle-pair record it re-runs, on
//! the stored answers, the live compare's own rules ([`Ctx::gql_rules`], with the document):
//! the row rules, H8, the `payload.data` matcher, H7 and the diff with the ruled field
//! matchers (H9 among them), then classifies with the allowlist as the run does. It does
//! not finish cut nested lists: H6's stored entries (a harness limit, an unfinished list)
//! are carried into the result and keep the record unexplained (R-P5P9-64 M-1). The one
//! oracle read is R-P5P9-86 TC's count walk over the frozen oracle (R-P5P9-87).
//!
//! R-P5P9-86 with R-P5P9-87, the REST records: an `account` record against chainweb-data
//! and a `v1txs` record against kda-txs are settled by their stored forms
//! ([`Ctx::rest_stored`]): the rules combined row by row, and the v1txs edge.
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
    /// GraphQL, REST account and /v1/txs records the run left unexplained (class `diff`):
    /// the ones taken up.
    pub refereed: usize,
    pub explained: usize,
    pub unexplained: usize,
    /// R-P5P9-86 (P, Q): records whose every difference a KNOWN-GAP entry matches. They are
    /// never explained; the FV gate counts them per entry against its baseline.
    pub known_gap: usize,
    /// Lines and taken-up records that could not be re-run (a line that is not a diff
    /// record, a B target that is not an oracle here, a body the log cut, an operation not
    /// loaded, the read cap spent, …); they stay unexplained, each listed with its reason.
    /// `explained + unexplained + known_gap + not_refereed` = `refereed` + the lines that did
    /// not parse.
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
    /// R-P5P9-86 (P, Q): a known gap is neither (as a live `known-gap` record, which the
    /// referee does not take up); the FV gate reads its count against the entry's baseline.
    pub fn passed(&self) -> bool {
        self.unexplained == 0 && self.not_refereed == 0
    }
}

/// What the rules make of one stored record.
struct Settled {
    unexplained: Vec<DiffEntry>,
    ids: Vec<String>,
    /// The KNOWN-GAP entries the record hit (R-P5P9-86 P, Q).
    known_ids: Vec<String>,
    note: Option<String>,
    /// The hits that carry evidence.
    hits: Vec<DiffEntry>,
}

/// Re-referees the stored records of `opts.logs` (see the module docs).
pub async fn run(mut cfg: CompareConfig, opts: &Opts) -> anyhow::Result<Report> {
    // The node client's own limiter paces every read.
    cfg.node_max_rps = opts.rps.max(1);
    // R-P5P9-92 (9): ours' and the oracle's reads (the count walk's pages) are paced at the
    // same rate at most, and each takes one read of `max_reads`.
    for t in cfg.targets.values_mut() {
        let cap = f64::from(opts.rps.max(1));
        t.rate_rps = Some(t.rate_rps.map_or(cap, |r| r.min(cap)));
    }
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
        let mut reader = std::io::BufReader::new(f);
        let mut buf = Vec::new();
        let mut n = 0;
        loop {
            // R-P5P9-67 m-4: lines are read as bytes; one that is not UTF-8 is listed.
            buf.clear();
            let got = reader
                .read_until(b'\n', &mut buf)
                .with_context(|| format!("reading {}", path.display()))?;
            if got == 0 {
                break;
            }
            n += 1;
            let at = format!("{}:{n}", path.display());
            let line = match std::str::from_utf8(&buf) {
                Ok(l) => l,
                Err(e) => {
                    rep.records += 1;
                    let why = format!("the line is not UTF-8 ({e}), so not a diff record");
                    let class = "not refereed: not a diff record";
                    not_refereed(&mut rep, json!({ "at": at }), class, why);
                    continue;
                }
            };
            if line.trim().is_empty() {
                continue;
            }
            rep.records += 1;
            // R-P5P9-64 I-4: a line that does not parse (a cut append, a disk that filled)
            // may be a diff record: listed, never dropped.
            let rec = match serde_json::from_str::<DiffRecord>(line) {
                Ok(rec) => rec,
                Err(e) => {
                    let why = format!("the line is not a diff record: {e}");
                    let class = "not refereed: not a diff record";
                    not_refereed(&mut rep, json!({ "at": at }), class, why);
                    continue;
                }
            };
            // R-P5P9-86 with R-P5P9-87: the REST account and /v1/txs records too.
            let rest = matches!(rec.route.as_str(), "account" | "v1txs");
            if rec.class != "diff" || !(rest || rec.route.starts_with("graphql:")) {
                continue;
            }
            rep.refereed += 1;
            let head = json!({"at": at, "ts": rec.ts, "route": rec.route,
                "request": rec.request, "a": rec.a_target, "b": rec.b_target});
            if rest {
                if ctx.gql.node_budget.load(Ordering::Relaxed) == 0 {
                    let why = format!("the cap of {} node reads is spent", opts.max_reads);
                    not_refereed(&mut rep, head, UNREAD, why);
                    continue;
                }
                match ctx.rest_stored(&rec).await {
                    Err(why) => {
                        let class = format!("not refereed: {why}");
                        not_refereed(&mut rep, head, &class, why);
                    }
                    Ok((un, ids, _)) if un.is_empty() => {
                        rep.explained += 1;
                        for id in ids {
                            count(&mut rep, id);
                        }
                    }
                    Ok((un, ids, note)) => {
                        rep.unexplained += 1;
                        count(&mut rep, format!("unexplained: rest-{}", rec.route));
                        let mut v = head;
                        v["class"] = json!(format!("rest-{}", rec.route));
                        v["note"] = json!(note);
                        v["unexplainedCount"] = json!(un.len());
                        v["unexplained"] = json!(un.iter().take(MAX_SHOWN).collect::<Vec<_>>());
                        v["explainedBy"] = json!(ids);
                        rep.still_unexplained.push(v);
                    }
                }
                continue;
            }
            // R-P5P9-86 S′ with R-P5P9-87: a NodeBalance record against ours' native API
            // (labelled `node` before S′) is settled by its stored form.
            if native_op(&ctx, &rec).is_some() {
                match settle_native(&ctx, &rec).await {
                    Err(why) => {
                        let class = format!("not refereed: {why}");
                        not_refereed(&mut rep, head, &class, why);
                    }
                    Ok((d, ids)) if d.is_empty() => {
                        rep.explained += 1;
                        for id in ids {
                            count(&mut rep, id);
                        }
                    }
                    Ok((d, ids)) => {
                        rep.unexplained += 1;
                        count(&mut rep, "unexplained: native-skew".into());
                        let mut v = head;
                        v["class"] = json!("native-skew");
                        v["unexplainedCount"] = json!(d.len());
                        v["unexplained"] = json!(d);
                        v["explainedBy"] = json!(ids);
                        rep.still_unexplained.push(v);
                    }
                }
                continue;
            }
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
            // R-P5P9-67 I-A (b), m-3: a follow-page fault of ours or the merged-bytes cap is a
            // fact of the fetch, with no pages to re-run: it stays unexplained.
            // R-P5P9-70 (m-3): so is a first-page fault of ours.
            let stop = rec.diff.iter().find(|e| {
                e.path == crate::graphql::FOLLOW_PAGE_FAULT_PATH
                    || e.path == crate::graphql::FIRST_PAGE_FAULT_PATH
                    || e.path == crate::graphql::MERGED_BYTES_PATH
            });
            if let Some(e) = stop {
                // R-P5P9-86 P: a fault on a heavy account P lists is a known gap, read with
                // the live run's request text (`graphql:<operation> <variables>`).
                let c = ctx.allow.classify_known(
                    &RouteKind::Other,
                    &format!("graphql:{}", rec.request),
                    &[],
                    &rec.diff,
                );
                if c.unexplained.is_empty() && !c.known_ids.is_empty() {
                    rep.known_gap += 1;
                    for id in c.known_ids {
                        count(&mut rep, format!("known-gap: {id}"));
                    }
                    continue;
                }
                let kind = match e.path.as_str() {
                    crate::graphql::FOLLOW_PAGE_FAULT_PATH => "follow-page-fault",
                    crate::graphql::FIRST_PAGE_FAULT_PATH => "first-page-fault",
                    _ => "harness-limit",
                };
                rep.unexplained += 1;
                count(&mut rep, format!("unexplained: {kind}"));
                let mut v = head;
                v["class"] = json!(kind);
                v["note"] = json!(rec.note);
                v["unexplainedCount"] = json!(rec.diff.len());
                v["unexplained"] = json!(rec.diff.iter().take(MAX_SHOWN).collect::<Vec<_>>());
                rep.still_unexplained.push(v);
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
                Ok(s) if s.unexplained.is_empty() && !s.known_ids.is_empty() => {
                    rep.known_gap += 1;
                    for id in s.known_ids {
                        count(&mut rep, format!("known-gap: {id}"));
                    }
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
    // R-P5P9-87 TC: the record's height (ours' highest stored row), before the rules pair
    // the rows. Ours' rows above it are newer than the record.
    let record_height = crate::graphql_window::highest_row(&a.body).unwrap_or(max);
    // The live compare's rules with the document (R-P5P9-65 (1)); H6 from the stored diff.
    let (mut d, mut note) = ctx
        .gql_rules(ta, H6::Stored(&rec.diff), op, &vars, &mut a, &mut b, max)
        .await;
    // R-P5P9-86 TC with R-P5P9-87: a count-only record walks ours bounded at the record's
    // height, and the oracle as it is now (frozen); ours' rows between the oracle's height
    // and the record's are polled node-canonical (R-P5P9-92 (2)).
    if let Some(tb) = ctx.targets.get(&rec.b_target) {
        ctx.gql_count_walk(ta, tb, op, &vars, &mut d, &mut note, max, record_height)
            .await;
    }
    // R-P5P9-86 (P, Q): the live run's request text, so the KNOWN-GAP entries scoped by
    // operation and account read as they do live.
    let c = ctx.allow.classify_known(
        &RouteKind::Other,
        &format!("graphql:{}", rec.request),
        &[],
        &d,
    );
    let (ids, known_ids) = (c.ids, c.known_ids);
    // A KNOWN-GAP hit is never explained: with another difference it stays unexplained.
    let unexplained = if c.unexplained.is_empty() {
        c.unexplained
    } else {
        [c.unexplained, c.known].concat()
    };
    let hits = d
        .into_iter()
        .filter(|e| e.path.starts_with("/~") && (e.a.is_some() || e.b.is_some()))
        .collect();
    Ok(Settled {
        unexplained,
        ids,
        known_ids,
        note,
        hits,
    })
}

/// The NodeBalance operation of a stored record whose B is not an oracle (R-P5P9-86 S′).
fn native_op<'a>(ctx: &'a Ctx, rec: &DiffRecord) -> Option<&'a crate::graphql::GqlOp> {
    let stem = rec.request.split_once(' ')?.0;
    let op = ctx.gql.ops.iter().find(|o| o.stem == stem)?;
    let oracle = ctx
        .targets
        .get(&rec.b_target)
        .is_some_and(|t| t.kind == TargetKind::GraphqlOracle);
    (op.kind == crate::graphql::OpKind::NodeBalance && !oracle).then_some(op)
}

/// Unix seconds of a record's `ts` (`YYYY-MM-DDTHH:MM:SS[.fff]Z`).
fn ts_secs(ts: &str) -> Option<u64> {
    let (d, t) = ts.strip_suffix('Z')?.split_once('T')?;
    let n = |x: &str| x.parse::<i64>().ok();
    let mut dp = d.split('-');
    let (y, m, dd) = (n(dp.next()?)?, n(dp.next()?)?, n(dp.next()?)?);
    let mut tp = t.split(':');
    let (hh, mm) = (n(tp.next()?)?, n(tp.next()?)?);
    let ss = n(tp.next()?.split('.').next()?)?;
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + dd - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    u64::try_from(days * 86_400 + hh * 3_600 + mm * 60 + ss).ok()
}

/// R-P5P9-86 S′, the stored form: every entry is a `/chain<N>/balance` gap that
/// [`Ctx::native_skew_stored`] settles; any other entry stays unexplained. Returns the
/// unexplained entries and the explaining ids.
async fn settle_native(
    ctx: &Ctx,
    rec: &DiffRecord,
) -> Result<(Vec<DiffEntry>, Vec<String>), String> {
    let vars: Map<String, Value> = rec
        .request
        .split_once(' ')
        .and_then(|(_, v)| serde_json::from_str(v).ok())
        .ok_or("the variables are not JSON")?;
    let account = vars
        .get("accountName")
        .and_then(Value::as_str)
        .ok_or("no accountName")?;
    let ts = ts_secs(&rec.ts).ok_or("the record's time does not read")?;
    let mut d = Vec::new();
    for e in &rec.diff {
        let chain = e
            .path
            .strip_prefix("/chain")
            .and_then(|p| p.strip_suffix("/balance"))
            .and_then(|c| c.parse::<u8>().ok());
        let dec = |v: &Option<Value>| v.as_ref().and_then(Value::as_str).map(str::to_string);
        let settled = match (chain, dec(&e.a), dec(&e.b)) {
            (Some(c), Some(x), Some(y)) => ctx.native_skew_stored(c, account, &x, &y, ts).await,
            _ => Err("not a balance gap".into()),
        };
        match settled {
            Ok(ev) => d.push(DiffEntry {
                path: crate::graphql::NATIVE_SKEW_PATH.into(),
                kind: crate::report::DiffKind::Changed,
                a: Some(json!({"stored": ev, "gap": [e.a, e.b]})),
                b: None,
            }),
            Err(why) => {
                let mut x = e.clone();
                x.b = Some(json!({"native": e.b, "why": why}));
                d.push(x);
            }
        }
    }
    let req = format!("graphql:{}", rec.request);
    let (un, ids) = ctx.allow.classify_request(&RouteKind::Other, &req, &[], &d);
    Ok((un, ids))
}
