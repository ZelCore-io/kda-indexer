//! `kda-compare summary --since <iso> [--routes must|should|all] --gate small|full`: the
//! P3 exit gate (and its P5 small and full forms). Reads the
//! ten-minute buckets the `run` loop writes into `summary-<date>.json` (flushed every minute
//! and on shutdown), merges the buckets that start at or after `--since`, and prints one row
//! per route and pair. Exit status 0 iff every must route is green.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use anyhow::Context;

use crate::normalize::parse_iso;
use crate::report::{date_of, lat_upper_ms, DailySummary, Window};

/// Must routes and the number of compared requests each needs at the full gate (the final
/// validation: 500/200/50).
pub const MUST: &[(&str, u64)] = &[
    ("account", 500),
    ("events", 500),
    ("tx", 500),
    ("txs", 500),
    ("v1txs", 200),
    ("recent", 50),
    ("stats", 50),
];

/// Must routes at the small gate (R3E-13): what a phase gate asks for.
pub const MUST_SMALL: &[(&str, u64)] = &[
    ("account", 100),
    ("events", 100),
    ("tx", 100),
    ("txs", 100),
    ("v1txs", 50),
    ("recent", 20),
    ("stats", 20),
];

/// Should routes and their minimums (registered at P5; P6 fills the routes). Same for both
/// gate sizes.
pub const SHOULD: &[(&str, u64)] = &[("search", 50), ("coins", 20), ("balances", 200)];

/// Gate size: `small` (R3E-13, every phase) or `full` (the final validation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateSize {
    Small,
    Full,
}

impl GateSize {
    pub fn parse(s: &str) -> anyhow::Result<GateSize> {
        match s {
            "small" => Ok(GateSize::Small),
            "full" => Ok(GateSize::Full),
            _ => anyhow::bail!("--gate must be `small` or `full`"),
        }
    }
}

/// Which routes the verdict covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteSet {
    Must,
    Should,
    /// The GraphQL routes (`graphql:<operation>`).
    Graphql,
    /// Must and should routes, plus the GraphQL routes when the window has any.
    All,
}

impl RouteSet {
    pub fn parse(s: &str) -> anyhow::Result<RouteSet> {
        match s {
            "must" => Ok(RouteSet::Must),
            "should" => Ok(RouteSet::Should),
            "graphql" => Ok(RouteSet::Graphql),
            "all" => Ok(RouteSet::All),
            _ => anyhow::bail!("--routes must be `must`, `should`, `graphql` or `all`"),
        }
    }

    /// The (route, minimum compared) list the verdict requires.
    pub fn required(self, size: GateSize) -> Vec<(&'static str, u64)> {
        let must = match size {
            GateSize::Small => MUST_SMALL,
            GateSize::Full => MUST,
        };
        match self {
            RouteSet::Must => must.to_vec(),
            RouteSet::Should => SHOULD.to_vec(),
            RouteSet::Graphql => Vec::new(),
            RouteSet::All => must.iter().chain(SHOULD).copied().collect(),
        }
    }
}

/// Classes that count as a request compared (the two sides answered and were diffed).
const COMPARED: &[&str] = &["match", "diff", "allowlisted", "legacy-cmd", "skew"];

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    pub route: String,
    pub a: String,
    pub b: String,
    pub compared: u64,
    pub unexplained: u64,
    pub allowlisted: u64,
    pub skew: u64,
    pub errors_a: u64,
    pub errors_b: u64,
    /// Oracle server faults (HTTP 5xx, `INTERNAL_SERVER_ERROR`): a subset of `errors_b`.
    pub oracle_faults: u64,
    pub p50_a: Option<f64>,
    pub p99_a: Option<f64>,
    pub p50_b: Option<f64>,
    pub p99_b: Option<f64>,
}

/// `YYYY-MM-DDTHH:MM:SSZ` or `YYYY-MM-DD` as seconds since the epoch.
pub fn parse_since(s: &str) -> anyhow::Result<u64> {
    let full = if s.len() == 10 {
        format!("{s}T00:00:00Z")
    } else {
        s.to_string()
    };
    let t = parse_iso(&full)
        .with_context(|| format!("--since {s:?}: expected YYYY-MM-DDTHH:MM:SSZ"))?;
    u64::try_from(t.secs).context("--since before 1970")
}

/// Merges every bucket starting at or after `since_secs` from the summary files in `out_dir`.
pub fn load(out_dir: &Path, since_secs: u64) -> anyhow::Result<Window> {
    let first_date = date_of(since_secs);
    let mut files: Vec<_> = std::fs::read_dir(out_dir)
        .with_context(|| format!("reading {}", out_dir.display()))?
        .filter_map(Result::ok)
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            let date = name.strip_prefix("summary-")?.strip_suffix(".json")?;
            (date >= first_date.as_str()).then(|| (date.to_string(), e.path()))
        })
        .collect();
    files.sort();
    let mut merged = Window::default();
    for (_, path) in files {
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let day: DailySummary =
            serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        for (start, w) in day.windows {
            let secs = parse_iso(&start).map(|t| t.secs).unwrap_or(-1);
            if secs < 0 || (secs as u64) < since_secs {
                continue;
            }
            for (k, classes) in w.pairs {
                let dst = merged.pairs.entry(k).or_default();
                for (c, n) in classes {
                    *dst.entry(c).or_default() += n;
                }
            }
            for (k, h) in w.lat {
                let dst = merged.lat.entry(k).or_default();
                for (b, n) in h {
                    *dst.entry(b).or_default() += n;
                }
            }
        }
    }
    Ok(merged)
}

/// Percentile of a histogram: the upper edge of the bucket that holds the `q`-th request.
pub fn percentile(h: &BTreeMap<u16, u64>, q: f64) -> Option<f64> {
    let total: u64 = h.values().sum();
    if total == 0 {
        return None;
    }
    let rank = ((total as f64) * q).ceil().max(1.0) as u64;
    let mut seen = 0;
    for (b, n) in h {
        seen += n;
        if seen >= rank {
            return Some(lat_upper_ms(*b));
        }
    }
    h.keys().next_back().map(|b| lat_upper_ms(*b))
}

/// One row per `route|a|b`, route order as in the file (alphabetical).
pub fn rows(w: &Window) -> Vec<Row> {
    let mut out = Vec::new();
    for (key, classes) in &w.pairs {
        let mut it = key.splitn(3, '|');
        let (Some(route), Some(a), Some(b)) = (it.next(), it.next(), it.next()) else {
            continue;
        };
        let n = |c: &str| classes.get(c).copied().unwrap_or(0);
        let lat = |target: &str| w.lat.get(&format!("{route}|{target}"));
        let mut row = Row {
            route: route.into(),
            a: a.into(),
            b: b.into(),
            compared: COMPARED.iter().map(|c| n(c)).sum(),
            unexplained: n("diff"),
            allowlisted: n("allowlisted") + n("legacy-cmd"),
            skew: n("skew"),
            errors_a: n("a-error"),
            errors_b: n("b-error") + n("oracle-fault"),
            oracle_faults: n("oracle-fault"),
            ..Default::default()
        };
        if let Some(h) = lat(a) {
            row.p50_a = percentile(h, 0.5);
            row.p99_a = percentile(h, 0.99);
        }
        if b != "node" {
            if let Some(h) = lat(b) {
                row.p50_b = percentile(h, 0.5);
                row.p99_b = percentile(h, 0.99);
            }
        }
        out.push(row);
    }
    out
}

/// The gate: every required route has at least its minimum compared (best pair) and no
/// unexplained diff in any pair. Returns the verdict and one line per failing route.
pub fn gate(rows: &[Row], required: &[(&str, u64)]) -> (bool, Vec<String>) {
    let mut why = Vec::new();
    for (route, min) in required {
        let of_route = rows.iter().filter(|r| r.route == *route);
        let compared = of_route.clone().map(|r| r.compared).max().unwrap_or(0);
        let diffs: u64 = of_route.map(|r| r.unexplained).sum();
        if compared < *min {
            why.push(format!("{route}: {compared} compared, needs {min}"));
        }
        if diffs > 0 {
            why.push(format!("{route}: {diffs} unexplained diffs"));
        }
    }
    (why.is_empty(), why)
}

fn ms(v: Option<f64>) -> String {
    v.map_or_else(|| "-".into(), |x| format!("{x:.0}"))
}

/// The table (the required routes only, or every route with `all_rows`) and the verdict;
/// `label` names the gate size and route set on the verdict line.
pub fn render(
    rows: &[Row],
    required: &[(&str, u64)],
    all_rows: bool,
    label: &str,
) -> (String, bool) {
    let is_must = |r: &str| required.iter().any(|(m, _)| *m == r);
    let mut s = String::new();
    let _ = writeln!(
        s,
        "{:<8} {:<22} {:>9} {:>6} {:>6} {:>5} {:>6} {:>6} {:>17} {:>17}",
        "route",
        "pair",
        "compared",
        "diffs",
        "allow",
        "skew",
        "errA",
        "errB",
        "A p50/p99 ms",
        "B p50/p99 ms"
    );
    for r in rows.iter().filter(|r| all_rows || is_must(&r.route)) {
        let _ = writeln!(
            s,
            "{:<8} {:<22} {:>9} {:>6} {:>6} {:>5} {:>6} {:>6} {:>17} {:>17}",
            r.route,
            format!("{}|{}", r.a, r.b),
            r.compared,
            r.unexplained,
            r.allowlisted,
            r.skew,
            r.errors_a,
            r.errors_b,
            format!("{}/{}", ms(r.p50_a), ms(r.p99_a)),
            format!("{}/{}", ms(r.p50_b), ms(r.p99_b)),
        );
    }
    let (green, why) = gate(rows, required);
    if green {
        let _ = writeln!(
            s,
            "GREEN ({label}): every required route is at its minimum with 0 unexplained diffs"
        );
    } else {
        let _ = writeln!(s, "NOT GREEN ({label}):");
        for w in why {
            let _ = writeln!(s, "  {w}");
        }
    }
    let _ = writeln!(
        s,
        "not covered by this verdict: reorg-probe failures and lag (separate gate steps)"
    );
    s.push_str(&render_a_errors(rows));
    (s, green)
}

/// R-P5P9-67 I-A (b): the `a-error` count of every route and pair with one, required or not
/// (read each day, not gated; a follow-page fault of ours is a diff, not an error).
pub fn render_a_errors(rows: &[Row]) -> String {
    let with: Vec<&Row> = rows.iter().filter(|r| r.errors_a > 0).collect();
    if with.is_empty() {
        return "a-error per route: none\n".into();
    }
    let mut s = String::from("a-error per route (read, not gated):\n");
    for r in with {
        let _ = writeln!(s, "  {} {}|{}: {}", r.route, r.a, r.b, r.errors_a);
    }
    s
}

/// Node-pair routes of the GraphQL gate and their minimums (small, full).
pub const GRAPHQL_NODE_ROUTES: &[(&str, u64, u64)] = &[
    ("graphql:NodeTx", 50, 500),
    ("graphql:NodeBlock", 50, 500),
    ("graphql:NodeBalance", 50, 2_000),
];
/// Per operation against the reference server (or the internal pair), and in total.
pub const GRAPHQL_PER_OP: (u64, u64) = (20, 100);
pub const GRAPHQL_TOTAL: (u64, u64) = (200, 2_000);

/// The node-pair route that covers an operation, by what its stem names: transactions,
/// transfers and events by `NodeTx`; accounts and balances by `NodeBalance`; blocks and
/// heights by `NodeBlock` (the transaction words win, so `QTransactionsByBlock` is `NodeTx`).
/// `None` for the node routes themselves and for operations nothing covers.
pub fn node_cover(route: &str) -> Option<&'static str> {
    let stem = route.strip_prefix("graphql:")?.to_ascii_lowercase();
    if stem.starts_with("node") {
        return None;
    }
    let has = |ws: &[&str]| ws.iter().any(|w| stem.contains(w));
    if has(&["transaction", "transfer", "event"]) {
        Some("graphql:NodeTx")
    } else if has(&["account", "balance"]) {
        Some("graphql:NodeBalance")
    } else if has(&["block", "height"]) {
        Some("graphql:NodeBlock")
    } else {
        None
    }
}

/// Rows with the oracle's own faults counted as "node-checked": where the oracle answered an
/// operation with a server fault (`oracle-fault`: it fails on `networkInfo`,
/// `completedBlockHeights`, some `blocksFromHeight` selections) and the node pair that covers
/// it reached its minimum without an unexplained diff, each fault counts as a compare. An
/// oracle that is merely unreachable or slow (`b-error`) does not.
fn node_checked(rows: &[Row], size: GateSize) -> Vec<Row> {
    let node_green = |n: &str| {
        let of = || rows.iter().filter(|r| r.route == n);
        let min = GRAPHQL_NODE_ROUTES
            .iter()
            .find(|x| x.0 == n)
            .map(|x| if size == GateSize::Small { x.1 } else { x.2 })
            .unwrap_or(u64::MAX);
        of().map(|r| r.compared).max().unwrap_or(0) >= min && of().all(|r| r.unexplained == 0)
    };
    rows.iter()
        .map(|r| {
            let mut r = r.clone();
            if r.b != "node" && r.oracle_faults > 0 && node_cover(&r.route).is_some_and(node_green)
            {
                r.compared += r.oracle_faults;
            }
            r
        })
        .collect()
}

/// The GraphQL gate (plan Task 7): every operation seen in the window has at least its minimum
/// compared, the node pairs reach theirs, the total is reached, and nothing is unexplained.
pub fn graphql_gate(rows: &[Row], size: GateSize) -> (bool, Vec<String>) {
    let rows = &node_checked(rows, size)[..];
    let pick = |s: (u64, u64)| if size == GateSize::Small { s.0 } else { s.1 };
    let mut routes: Vec<&str> = rows
        .iter()
        .filter(|r| r.route.starts_with("graphql:"))
        .map(|r| r.route.as_str())
        .collect();
    routes.sort_unstable();
    routes.dedup();
    let is_node = |r: &str| GRAPHQL_NODE_ROUTES.iter().any(|(n, _, _)| *n == r);
    let mut required: Vec<(&str, u64)> = Vec::new();
    for r in &routes {
        if !is_node(r) {
            required.push((r, pick(GRAPHQL_PER_OP)));
        }
    }
    for (n, s, f) in GRAPHQL_NODE_ROUTES {
        required.push((n, if size == GateSize::Small { *s } else { *f }));
    }
    let (_, mut why) = gate(rows, &required);
    let total: u64 = routes
        .iter()
        .filter(|r| !is_node(r))
        .map(|r| {
            rows.iter()
                .filter(|x| x.route == **r)
                .map(|x| x.compared)
                .max()
                .unwrap_or(0)
        })
        .sum();
    if total < pick(GRAPHQL_TOTAL) {
        why.push(format!(
            "graphql total: {total} compared, needs {}",
            pick(GRAPHQL_TOTAL)
        ));
    }
    (why.is_empty(), why)
}

/// `summary` command: prints the table; returns whether the gate is green.
pub fn run(out_dir: &Path, since: &str, routes: &str, gate: &str) -> anyhow::Result<bool> {
    let set = RouteSet::parse(routes)?;
    let size = GateSize::parse(gate)?;
    let since_secs = parse_since(since)?;
    let w = load(out_dir, since_secs)?;
    let unpaired = load_unpaired(out_dir, since_secs)?;
    let label = format!("gate {gate}, routes {routes}");
    let all_rows = rows(&w);
    let with_graphql = set == RouteSet::Graphql
        || (set == RouteSet::All && all_rows.iter().any(|r| r.route.starts_with("graphql:")));
    let required = set.required(size);
    let (mut text, mut green) = if set == RouteSet::Graphql {
        (render(&all_rows, &[], true, &label).0, true)
    } else {
        render(&all_rows, &required, routes == "all", &label)
    };
    if with_graphql {
        let (ok, why) = graphql_gate(&all_rows, size);
        text.push_str(&format!(
            "graphql gate {gate}: {}\n",
            if ok { "GREEN" } else { "RED" }
        ));
        for w in why {
            text.push_str(&format!("  {w}\n"));
        }
        green &= ok;
        text.push_str(&render_unpaired(&unpaired));
    }
    print!("{text}");
    Ok(green)
}

/// The union of the summaries' unpaired captured documents since `since_secs` (by day).
fn load_unpaired(out_dir: &Path, since_secs: u64) -> anyhow::Result<BTreeMap<String, String>> {
    let first_date = date_of(since_secs);
    let mut out = BTreeMap::new();
    for e in std::fs::read_dir(out_dir)
        .with_context(|| format!("reading {}", out_dir.display()))?
        .filter_map(Result::ok)
    {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some(date) = name
            .strip_prefix("summary-")
            .and_then(|n| n.strip_suffix(".json"))
        else {
            continue;
        };
        if date < first_date.as_str() {
            continue;
        }
        let text = std::fs::read_to_string(e.path())?;
        if let Ok(day) = serde_json::from_str::<DailySummary>(&text) {
            out.extend(day.graphql_unpaired);
        }
    }
    Ok(out)
}

/// R-P5P9-57 coverage: the captured documents the harness never pairs, by kind (tip and
/// subscription kinds are covered by the soak and the node pairs; unsupported ones go on
/// the gap list).
pub fn render_unpaired(unpaired: &BTreeMap<String, String>) -> String {
    let mut by_kind: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (stem, kind) in unpaired {
        by_kind
            .entry(kind.as_str())
            .or_default()
            .push(stem.as_str());
    }
    let mut text = format!(
        "graphql captured documents never paired: {}\n",
        unpaired.len()
    );
    for kind in ["tip", "subscription", "unsupported"] {
        let stems = by_kind.remove(kind).unwrap_or_default();
        text.push_str(&format!(
            "  {kind} ({}): {}\n",
            stems.len(),
            stems.join(" ")
        ));
    }
    for (kind, stems) in by_kind {
        text.push_str(&format!(
            "  {kind} ({}): {}\n",
            stems.len(),
            stems.join(" ")
        ));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::lat_bucket;

    fn window(pairs: &[(&str, &[(&str, u64)])]) -> Window {
        let mut w = Window::default();
        for (k, cs) in pairs {
            let m = w.pairs.entry((*k).into()).or_default();
            for (c, n) in *cs {
                m.insert((*c).into(), *n);
            }
        }
        w
    }

    #[test]
    fn gate_needs_minimums_and_no_diffs() {
        let mut pairs: Vec<(String, Vec<(&str, u64)>)> = MUST
            .iter()
            .map(|(r, min)| (format!("{r}|compat|cwd"), vec![("match", *min)]))
            .collect();
        let w = |p: &[(String, Vec<(&str, u64)>)]| {
            let refs: Vec<(&str, &[(&str, u64)])> =
                p.iter().map(|(k, v)| (k.as_str(), v.as_slice())).collect();
            window(&refs)
        };
        let (g, why) = gate(&rows(&w(&pairs)), MUST);
        assert!(g, "{why:?}");
        // one under its minimum
        pairs[4].1 = vec![("match", 199)];
        let (g, why) = gate(&rows(&w(&pairs)), MUST);
        assert!(!g);
        assert_eq!(why, vec!["v1txs: 199 compared, needs 200".to_string()]);
        // an unexplained diff, even with plenty compared
        pairs[4].1 = vec![("match", 300), ("diff", 1)];
        let (g, why) = gate(&rows(&w(&pairs)), MUST);
        assert!(!g);
        assert_eq!(why, vec!["v1txs: 1 unexplained diffs".to_string()]);
        // a missing route
        pairs.remove(0);
        let (g, _) = gate(&rows(&w(&pairs)), MUST);
        assert!(!g);
        // an empty window is never green
        assert!(!gate(&[], MUST).0);
    }

    #[test]
    fn errors_and_latency_columns() {
        let mut w = window(&[(
            "tx|compat|cwd",
            &[
                ("match", 8),
                ("diff", 1),
                ("allowlisted", 2),
                ("a-error", 3),
                ("b-error", 4),
            ],
        )]);
        let h = w.lat.entry("tx|compat".into()).or_default();
        for ms in [10, 10, 10, 10, 1000] {
            *h.entry(lat_bucket(ms)).or_default() += 1;
        }
        let r = rows(&w);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].compared, 11);
        assert_eq!(r[0].unexplained, 1);
        assert_eq!(r[0].errors_a, 3);
        assert_eq!(r[0].errors_b, 4);
        let p50 = r[0].p50_a.unwrap();
        assert!((10.0..=13.0).contains(&p50), "{p50}");
        let p99 = r[0].p99_a.unwrap();
        assert!((1000.0..=1250.0).contains(&p99), "{p99}");
        assert!(r[0].p50_b.is_none());
    }

    #[test]
    fn since_parses() {
        assert_eq!(parse_since("1970-01-02").unwrap(), 86_400);
        assert_eq!(parse_since("1970-01-01T01:00:00Z").unwrap(), 3600);
        assert!(parse_since("yesterday").is_err());
    }

    #[test]
    fn small_full_and_should_sets() {
        let mk = |req: &[(&str, u64)], delta: i64| -> Vec<Row> {
            req.iter()
                .map(|(r, min)| Row {
                    route: (*r).into(),
                    a: "compat".into(),
                    b: "cwd".into(),
                    compared: (*min as i64 + delta) as u64,
                    ..Default::default()
                })
                .collect()
        };
        let small = RouteSet::Must.required(GateSize::Small);
        let full = RouteSet::Must.required(GateSize::Full);
        assert_eq!(small[0], ("account", 100));
        assert_eq!(full[0], ("account", 500));
        // A window that meets the small minimums is green at small, not at full.
        let w = mk(&small, 0);
        assert!(gate(&w, &small).0);
        let (g, why) = gate(&w, &full);
        assert!(!g);
        assert!(
            why.contains(&"account: 100 compared, needs 500".to_string()),
            "{why:?}"
        );
        // Should routes are a separate set: must rows say nothing about them.
        let should = RouteSet::Should.required(GateSize::Small);
        assert_eq!(
            should,
            vec![("search", 50), ("coins", 20), ("balances", 200)]
        );
        assert!(gate(&w, &should).1.len() == 3);
        assert!(gate(&mk(&should, 0), &should).0);
        let all = RouteSet::All.required(GateSize::Small);
        assert_eq!(all.len(), small.len() + should.len());
        // A diff on a should route fails the should set but not the must set.
        let mut rows = mk(&small, 0);
        rows.extend(mk(&should, 0));
        rows.last_mut().unwrap().unexplained = 1;
        assert!(gate(&rows, &small).0);
        assert!(!gate(&rows, &should).0);
        assert!(!gate(&rows, &all).0);
        assert!(RouteSet::parse("x").is_err() && GateSize::parse("x").is_err());
    }

    fn grow(route: &str, b: &str, compared: u64, unexplained: u64) -> Row {
        Row {
            route: route.into(),
            a: "gql".into(),
            b: b.into(),
            compared,
            unexplained,
            ..Default::default()
        }
    }

    #[test]
    fn graphql_gate_small_and_full() {
        let mut rows = vec![
            grow("graphql:QBlock", "oracle", 20, 0),
            grow("graphql:QEvents", "oracle", 25, 0),
        ];
        for _ in 0..8 {
            rows.push(grow("graphql:QX", "oracle", 20, 0));
        }
        rows.extend([
            grow("graphql:NodeTx", "node", 50, 0),
            grow("graphql:NodeBlock", "node", 50, 0),
            grow("graphql:NodeBalance", "node", 50, 0),
        ]);
        // 20 + 25 + 20 = 65 per-operation compares: below the 200 total.
        let (ok, why) = graphql_gate(&rows, GateSize::Small);
        assert!(
            !ok && why.iter().any(|w| w.starts_with("graphql total")),
            "{why:?}"
        );
        for i in 0..8 {
            rows.push(grow(&format!("graphql:Op{i}"), "oracle", 25, 0));
        }
        let (ok, why) = graphql_gate(&rows, GateSize::Small);
        assert!(ok, "{why:?}");
        // The full gate asks 100 per operation and 500/500/2,000 on the node pairs.
        let (ok, why) = graphql_gate(&rows, GateSize::Full);
        assert!(!ok && why.len() > 5, "{why:?}");
        // An unexplained diff fails the small gate, and so does a missing node route.
        rows.push(grow("graphql:QBlock", "oracle", 0, 1));
        assert!(!graphql_gate(&rows, GateSize::Small).0);
        rows.pop();
        rows.retain(|r| r.route != "graphql:NodeBalance");
        let (ok, why) = graphql_gate(&rows, GateSize::Small);
        assert!(
            !ok && why.iter().any(|w| w.starts_with("graphql:NodeBalance")),
            "{why:?}"
        );
    }

    #[test]
    fn oracle_faults_are_node_checked_when_a_node_pair_covers_the_operation() {
        let fault = |route: &str, compared: u64, faults: u64| Row {
            oracle_faults: faults,
            errors_b: faults,
            ..grow(route, "oracle", compared, 0)
        };
        let nodes = |n: u64| {
            vec![
                grow("graphql:NodeTx", "node", n, 0),
                grow("graphql:NodeBlock", "node", n, 0),
                grow("graphql:NodeBalance", "node", n, 0),
            ]
        };
        let mut rows = vec![
            // The oracle fails every blocksFromHeight call: 0 compared, 25 faults.
            fault("graphql:QBlocksFromHeight", 0, 25),
            // An oracle that was merely unreachable: still 0 compared.
            Row {
                errors_b: 25,
                ..grow("graphql:QBlock", "oracle", 0, 0)
            },
        ];
        for i in 0..10 {
            rows.push(grow(&format!("graphql:Op{i}"), "oracle", 25, 0));
        }
        rows.extend(nodes(50));
        let (ok, why) = graphql_gate(&rows, GateSize::Small);
        assert!(!ok, "{why:?}");
        // Faults count for the covered operation, not for the merely unreachable one.
        assert!(
            !why.iter()
                .any(|w| w.starts_with("graphql:QBlocksFromHeight")),
            "{why:?}"
        );
        assert!(
            why.iter().any(|w| w.starts_with("graphql:QBlock:")),
            "{why:?}"
        );
        // No cover without the node pair reaching its minimum.
        let mut rows2 = rows.clone();
        rows2.retain(|r| r.route != "graphql:NodeBlock");
        rows2.push(grow("graphql:NodeBlock", "node", 10, 0));
        let (_, why) = graphql_gate(&rows2, GateSize::Small);
        assert!(
            why.iter()
                .any(|w| w.starts_with("graphql:QBlocksFromHeight")),
            "{why:?}"
        );
        // Nor when the covering node pair has an unexplained diff.
        let mut rows3 = rows.clone();
        rows3.push(grow("graphql:NodeBlock", "node", 0, 1));
        let (_, why) = graphql_gate(&rows3, GateSize::Small);
        assert!(
            why.iter()
                .any(|w| w.starts_with("graphql:QBlocksFromHeight")),
            "{why:?}"
        );
        // Mapping by stem.
        assert_eq!(
            node_cover("graphql:QTransactionsByBlock"),
            Some("graphql:NodeTx")
        );
        assert_eq!(
            node_cover("graphql:QFungibleAccount"),
            Some("graphql:NodeBalance")
        );
        assert_eq!(
            node_cover("graphql:QBlocksFromHeight"),
            Some("graphql:NodeBlock")
        );
        assert_eq!(node_cover("graphql:QNetworkInfo"), None);
        assert_eq!(node_cover("graphql:NodeBlock"), None);
    }

    #[test]
    fn route_set_graphql_parses() {
        assert_eq!(RouteSet::parse("graphql").unwrap(), RouteSet::Graphql);
        assert!(RouteSet::Graphql.required(GateSize::Small).is_empty());
    }

    /// R-P5P9-67 I-A (b): every render lists the `a-error` counts per route and pair, the
    /// routes outside the required set included (read daily, not passed silently).
    #[test]
    fn a_errors_are_listed_per_route_on_every_render() {
        let row = |route: &str, errors_a: u64| Row {
            route: route.into(),
            a: "a".into(),
            b: "b".into(),
            errors_a,
            ..Default::default()
        };
        let rows = vec![row("graphql:QX", 3), row("account", 0)];
        let (text, _) = render(&rows, MUST, false, "gate full");
        assert!(
            text.contains("a-error per route") && text.contains("graphql:QX a|b: 3"),
            "{text}"
        );
        assert!(!text.contains("account a|b: 0"), "{text}");
        let (text, _) = render(&[row("account", 0)], MUST, false, "gate full");
        assert!(text.contains("a-error per route: none"), "{text}");
    }
}
