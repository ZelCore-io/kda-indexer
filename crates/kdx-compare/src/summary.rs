//! `kda-compare summary --since <iso> [--routes must|should|all] [--gate small|full]`: the
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
    All,
}

impl RouteSet {
    pub fn parse(s: &str) -> anyhow::Result<RouteSet> {
        match s {
            "must" => Ok(RouteSet::Must),
            "should" => Ok(RouteSet::Should),
            "all" => Ok(RouteSet::All),
            _ => anyhow::bail!("--routes must be `must`, `should` or `all`"),
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
            errors_b: n("b-error"),
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

/// The table (the required routes only, or every route with `all_rows`) and the verdict.
pub fn render(rows: &[Row], required: &[(&str, u64)], all_rows: bool) -> (String, bool) {
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
            "GREEN: every required route is at its minimum with 0 unexplained diffs"
        );
    } else {
        let _ = writeln!(s, "NOT GREEN:");
        for w in why {
            let _ = writeln!(s, "  {w}");
        }
    }
    (s, green)
}

/// `summary` command: prints the table; returns whether the gate is green.
pub fn run(out_dir: &Path, since: &str, routes: &str, gate: &str) -> anyhow::Result<bool> {
    let set = RouteSet::parse(routes)?;
    let size = GateSize::parse(gate)?;
    let since_secs = parse_since(since)?;
    let w = load(out_dir, since_secs)?;
    let (text, green) = render(&rows(&w), &set.required(size), routes == "all");
    print!("{text}");
    Ok(green)
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
}
