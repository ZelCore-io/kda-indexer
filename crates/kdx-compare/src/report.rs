//! JSONL diff log and the daily summary (spec section 7.4). Files live under `out_dir`.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Body bytes kept per side in an explained diff record (R-P5P9-60 H3: 1 MiB, was 4 KB). The
/// request, the curl line and the note keep as much; each is cut by its JSON-escaped length
/// ([`DiffRecord::bounded`]).
pub const BODY_TRIM: usize = 1 << 20;
/// The existing 8 MiB body limit (`[node] max_body_bytes`, the targets' response cap): an
/// unexplained record (class `diff`) keeps both bodies whole up to this many JSON-escaped
/// bytes for the two together (fv-checklist T-item, R-P5P9-61), and says so when it cuts.
pub const BODY_CAP: usize = 8 << 20;
/// JSON-escaped bytes kept per diff entry value; a bigger value is replaced by a marked
/// string cut to this length (the bodies still carry it, up to [`BODY_TRIM`]).
pub const ENTRY_VALUE_TRIM: usize = 32 << 10;
/// JSON-escaped bytes kept of a diff entry path and of each short field.
pub const SHORT_TRIM: usize = 4 << 10;
/// Upper bound of one diff log line: five long strings, at most `MAX_ENTRIES` entries of a
/// path and two values, and the short fields with the JSON punctuation around them.
pub const MAX_RECORD_BYTES: usize = (BODY_CAP + 4)
    + 3 * (BODY_TRIM + 2)
    + crate::diff::MAX_ENTRIES * (SHORT_TRIM + 2 * (ENTRY_VALUE_TRIM + 2) + 64)
    + 9 * (SHORT_TRIM + 2)
    + 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiffKind {
    /// Present in B, absent in A.
    Missing,
    /// Present in A, absent in B.
    Extra,
    Changed,
    /// Same path, different JSON types.
    Type,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiffEntry {
    /// JSON pointer.
    pub path: String,
    pub kind: DiffKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub a: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub b: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffRecord {
    pub ts: String,
    pub route: String,
    pub request: String,
    pub a_target: String,
    pub b_target: String,
    pub a_status: Option<u16>,
    pub b_status: Option<u16>,
    pub a_body_trimmed: String,
    pub b_body_trimmed: String,
    pub diff: Vec<DiffEntry>,
    pub class: String,
    pub allowlisted: Option<String>,
    /// curl repro for A and B, relative to `$A` and `$B`.
    pub curl: String,
    /// Free-form detail (e.g. the legacy-cmd rehash result).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Set when the log cut a body: which cap applied and how much of each side it kept
    /// ([`DiffRecord::bounded`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_cap: Option<String>,
}

/// POSIX shell single-quoting: the result is one literal word whatever `s` holds (quotes,
/// `$(...)`, backticks, newlines). Used for the copy-paste curl lines in the diff log, whose
/// paths can come from untrusted replay logs.
pub fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// `"$VAR"'<quoted suffix>'`: the base URL variable expands, the rest stays literal.
pub fn sh_url(var: &str, suffix: &str) -> String {
    format!("\"${var}\"{}", sh_quote(suffix))
}

/// The length of `s` once JSON-escaped (without the quotes).
pub fn escaped_len(s: &str) -> usize {
    s.chars()
        .map(|c| match c {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
            c if (c as u32) < 0x20 => 6,
            c => c.len_utf8(),
        })
        .sum()
}

/// The two bodies of a record as the log keeps them, and the note when a cap cut them: an
/// unexplained record keeps both whole up to [`BODY_CAP`] escaped bytes together (a side
/// under half keeps all of it and the other takes the rest), any other record
/// [`BODY_TRIM`] a side.
fn bound_bodies(a: &str, b: &str, unexplained: bool) -> (String, String, Option<String>) {
    let (ea, eb) = (escaped_len(a), escaped_len(b));
    let half = BODY_CAP / 2;
    let (ka, kb, cap) = if !unexplained {
        let cut = ea > BODY_TRIM || eb > BODY_TRIM;
        (
            BODY_TRIM,
            BODY_TRIM,
            cut.then_some("1 MiB a body (explained record)"),
        )
    } else if ea + eb <= BODY_CAP {
        (ea, eb, None)
    } else if eb <= half {
        (
            BODY_CAP - eb,
            eb,
            Some("the 8 MiB body limit for both bodies"),
        )
    } else if ea <= half {
        (
            ea,
            BODY_CAP - ea,
            Some("the 8 MiB body limit for both bodies"),
        )
    } else {
        (half, half, Some("the 8 MiB body limit for both bodies"))
    };
    let (ta, tb) = (trim_escaped(a, ka), trim_escaped(b, kb));
    let note = cap.map(|c| {
        format!(
            "bodies cut at {c}: a kept {} of {ea} escaped bytes, b kept {} of {eb}",
            escaped_len(&ta),
            escaped_len(&tb)
        )
    });
    (ta, tb, note)
}

/// `s` cut to at most `max` bytes once JSON-escaped (on a char boundary).
pub fn trim_escaped(s: &str, max: usize) -> String {
    let mut n = 0;
    for (i, c) in s.char_indices() {
        let w = match c {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
            c if (c as u32) < 0x20 => 6,
            c => c.len_utf8(),
        };
        if n + w > max {
            return s[..i].to_string();
        }
        n += w;
    }
    s.to_string()
}

/// A diff entry value cut to [`ENTRY_VALUE_TRIM`] escaped bytes: kept when it fits,
/// otherwise a string `<clipped N bytes> <its text, cut>`.
fn bound_value(v: Option<Value>) -> Option<Value> {
    let v = v?;
    let text = v.to_string();
    if text.len() <= ENTRY_VALUE_TRIM {
        return Some(v);
    }
    let mark = format!("<clipped {} bytes> ", text.len());
    let body = trim_escaped(&text, ENTRY_VALUE_TRIM - mark.len());
    Some(Value::String(mark + &body))
}

impl DiffRecord {
    /// The record with every field bounded (R-P5P9-60 H3): its line is at most
    /// [`MAX_RECORD_BYTES`] whatever the bodies and the diff hold.
    pub fn bounded(&self) -> DiffRecord {
        let long = |s: &str| trim_escaped(s, BODY_TRIM);
        let short = |s: &str| trim_escaped(s, SHORT_TRIM);
        let (a_body, b_body, cut) = bound_bodies(
            &self.a_body_trimmed,
            &self.b_body_trimmed,
            self.class == "diff",
        );
        let body_cap = match (&self.body_cap, cut) {
            (Some(x), Some(y)) => Some(format!("{x}; {y}")),
            (x, y) => x.clone().or(y),
        };
        DiffRecord {
            ts: short(&self.ts),
            route: short(&self.route),
            request: long(&self.request),
            a_target: short(&self.a_target),
            b_target: short(&self.b_target),
            a_status: self.a_status,
            b_status: self.b_status,
            a_body_trimmed: a_body,
            b_body_trimmed: b_body,
            diff: self
                .diff
                .iter()
                .take(crate::diff::MAX_ENTRIES)
                .map(|e| DiffEntry {
                    path: short(&e.path),
                    kind: e.kind,
                    a: bound_value(e.a.clone()),
                    b: bound_value(e.b.clone()),
                })
                .collect(),
            class: short(&self.class),
            allowlisted: self.allowlisted.as_deref().map(short),
            curl: long(&self.curl),
            note: self.note.as_deref().map(long),
            body_cap: body_cap.as_deref().map(short),
        }
    }
}

/// A body as text, whole up to [`BODY_CAP`] bytes (the log applies its caps when it writes).
pub fn trim_body(b: &[u8]) -> String {
    let s = String::from_utf8_lossy(&b[..b.len().min(BODY_CAP)]);
    s.into_owned()
}

/// Seconds since the Unix epoch, now.
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Civil date (y, m, d) from days since 1970-01-01 (Howard Hinnant's algorithm).
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Days since 1970-01-01 for a civil date.
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let m = i64::from(m);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub fn date_of(secs: u64) -> String {
    let (y, m, d) = civil_from_days((secs / 86_400) as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

/// `YYYY-MM-DDTHH:MM:SSZ` for `secs`.
pub fn iso_secs(secs: u64) -> String {
    let rem = secs % 86_400;
    format!(
        "{}T{:02}:{:02}:{:02}Z",
        date_of(secs),
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Append-only JSONL log, one file per UTC day.
pub struct DiffLog {
    out_dir: PathBuf,
}

impl DiffLog {
    pub fn new(out_dir: &Path) -> anyhow::Result<DiffLog> {
        std::fs::create_dir_all(out_dir)
            .with_context(|| format!("creating {}", out_dir.display()))?;
        Ok(DiffLog {
            out_dir: out_dir.to_path_buf(),
        })
    }

    pub fn path_for(&self, date: &str) -> PathBuf {
        self.out_dir.join(format!("diffs-{date}.jsonl"))
    }

    pub fn append(&self, rec: &DiffRecord) -> anyhow::Result<()> {
        let path = self.path_for(&date_of(now_secs()));
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        let mut line = serde_json::to_vec(&rec.bounded())?;
        line.push(b'\n');
        f.write_all(&line)?;
        Ok(())
    }
}

/// Counts per route and class for one UTC day.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DailySummary {
    pub date: String,
    pub started: String,
    pub updated: String,
    /// route -> target -> requests.
    pub requests: BTreeMap<String, BTreeMap<String, u64>>,
    /// route -> class -> count.
    pub classes: BTreeMap<String, BTreeMap<String, u64>>,
    /// allowlist id -> matched diff records.
    pub allowlist_hits: BTreeMap<String, u64>,
    /// Unexplained diff paths (normalised: array indexes replaced by `*`) -> count.
    pub unexplained_paths: BTreeMap<String, u64>,
    /// Ten-minute buckets (key: the bucket start, `YYYY-MM-DDTHH:MM:SSZ`) for `kda-compare
    /// summary --since`.
    #[serde(default)]
    pub windows: BTreeMap<String, Window>,
    /// Captured GraphQL documents no pair compares: stem -> `tip`, `subscription` or
    /// `unsupported` (R-P5P9-57 coverage list).
    #[serde(default)]
    pub graphql_unpaired: BTreeMap<String, String>,
}

/// Seconds per `DailySummary.windows` bucket.
pub const WINDOW_SECS: u64 = 600;

/// What one ten-minute bucket counted.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Window {
    /// `route|a|b` -> outcome class -> count (one count per request through the pair).
    pub pairs: BTreeMap<String, BTreeMap<String, u64>>,
    /// `route|target` -> latency histogram (see `lat_bucket`) -> HTTP exchanges.
    pub lat: BTreeMap<String, BTreeMap<u16, u64>>,
}

/// Latency histogram bucket: log scale, ratio 1.2 (percentiles are good to about 20%).
pub fn lat_bucket(ms: u64) -> u16 {
    ((ms.max(1) as f64).ln() / 1.2f64.ln()).floor() as u16
}

/// Upper edge of a `lat_bucket`, in ms.
pub fn lat_upper_ms(b: u16) -> f64 {
    1.2f64.powi(i32::from(b) + 1)
}

impl DailySummary {
    pub fn new() -> DailySummary {
        let now = now_secs();
        DailySummary {
            date: date_of(now),
            started: iso_secs(now),
            updated: iso_secs(now),
            ..Default::default()
        }
    }

    /// Today's summary file when there is one (a restarted process keeps counting into it, so
    /// `kda-compare summary --since` sees the whole day), else a fresh summary.
    pub fn load_or_new(out_dir: &Path) -> DailySummary {
        let today = date_of(now_secs());
        let old = std::fs::read_to_string(Self::path(out_dir, &today))
            .ok()
            .and_then(|t| serde_json::from_str::<DailySummary>(&t).ok())
            .filter(|s| s.date == today);
        match old {
            Some(s) => s,
            None => DailySummary::new(),
        }
    }

    pub fn request(&mut self, route: &str, target: &str) {
        *self
            .requests
            .entry(route.to_string())
            .or_default()
            .entry(target.to_string())
            .or_default() += 1;
    }

    pub fn class(&mut self, route: &str, class: &str) {
        *self
            .classes
            .entry(route.to_string())
            .or_default()
            .entry(class.to_string())
            .or_default() += 1;
    }

    fn window(&mut self) -> &mut Window {
        let now = now_secs();
        let key = iso_secs(now - now % WINDOW_SECS);
        self.windows.entry(key).or_default()
    }

    /// One request through the pair `(a, b)` ended in `class`.
    pub fn pair(&mut self, route: &str, a: &str, b: &str, class: &str) {
        *self
            .window()
            .pairs
            .entry(format!("{route}|{a}|{b}"))
            .or_default()
            .entry(class.to_string())
            .or_default() += 1;
    }

    /// One HTTP exchange with `target` on `route` took `ms`.
    pub fn latency(&mut self, route: &str, target: &str, ms: u64) {
        *self
            .window()
            .lat
            .entry(format!("{route}|{target}"))
            .or_default()
            .entry(lat_bucket(ms))
            .or_default() += 1;
    }

    pub fn allowlist_hit(&mut self, id: &str) {
        *self.allowlist_hits.entry(id.to_string()).or_default() += 1;
    }

    pub fn unexplained(&mut self, route: &str, entries: &[DiffEntry]) {
        let mut seen = std::collections::BTreeSet::new();
        for e in entries {
            let p = format!("{route} {} {:?}", generic_path(&e.path), e.kind);
            if seen.insert(p.clone()) {
                *self.unexplained_paths.entry(p).or_default() += 1;
            }
        }
    }

    pub fn path(out_dir: &Path, date: &str) -> PathBuf {
        out_dir.join(format!("summary-{date}.json"))
    }

    /// Writes the summary file for `self.date`. If the UTC day changed, returns a fresh
    /// summary for the new day (the caller replaces `self` with it).
    pub fn flush(&mut self, out_dir: &Path) -> anyhow::Result<Option<DailySummary>> {
        let now = now_secs();
        self.updated = iso_secs(now);
        let path = Self::path(out_dir, &self.date);
        std::fs::write(&path, serde_json::to_vec_pretty(self)?)
            .with_context(|| format!("writing {}", path.display()))?;
        if date_of(now) != self.date {
            return Ok(Some(DailySummary::new()));
        }
        Ok(None)
    }
}

/// `/3/events/12/params` -> `/*/events/*/params`.
pub fn generic_path(p: &str) -> String {
    p.split('/')
        .map(|s| {
            if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
                "*"
            } else {
                s
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sh_quote_hostile_path() {
        let hostile = "/txs/tx?requestkey=x\"; rm -rf ~; echo $(id) `id` 'q' \n$HOME";
        let q = sh_quote(hostile);
        assert_eq!(
            q,
            "'/txs/tx?requestkey=x\"; rm -rf ~; echo $(id) `id` '\\''q'\\'' \n$HOME'"
        );
        // Every single quote in the output is either a delimiter or part of the '\'' escape.
        assert!(q.starts_with('\'') && q.ends_with('\''));
        assert_eq!(sh_quote("it's"), "'it'\\''s'");
        assert_eq!(sh_url("A", "/x?a=1&b=2"), "\"$A\"'/x?a=1&b=2'");
        // Round-trip through a real shell when one is available.
        if let Ok(out) = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(format!("printf %s {q}"))
            .output()
        {
            assert_eq!(String::from_utf8_lossy(&out.stdout), hostile);
        }
    }

    fn rec(body: String, request: String, diff: Vec<DiffEntry>) -> DiffRecord {
        DiffRecord {
            ts: "2026-11-14T00:00:00Z".into(),
            route: "graphql:QBlock".into(),
            request,
            a_target: "gql".into(),
            b_target: "oracle".into(),
            a_status: Some(200),
            b_status: Some(200),
            a_body_trimmed: body.clone(),
            b_body_trimmed: body.clone(),
            diff,
            class: "diff".into(),
            allowlisted: None,
            curl: body.clone(),
            note: Some(body),
            body_cap: None,
        }
    }

    /// fv-checklist T-item (R-P5P9-61): an unexplained record keeps both bodies whole up to
    /// the 8 MiB body limit for the two together, and says so when that cap cuts them; an
    /// explained record keeps 1 MiB a side, as before, and says so too.
    #[test]
    fn unexplained_records_keep_full_bodies_to_the_8_mib_cap() {
        let body = |n: usize| format!("{{\"data\":\"{}\"}}", "x".repeat(n));
        let back = |r: &DiffRecord| -> DiffRecord { serde_json::from_slice(&line_of(r)).unwrap() };
        assert_eq!(
            trim_body(body(3 << 20).as_bytes()).len(),
            body(3 << 20).len()
        );
        // 3 MiB and 2 MiB: both whole, parseable, no cap note.
        let mut r = rec(body(3 << 20), "Q {}".into(), vec![]);
        r.b_body_trimmed = body(2 << 20);
        let got = back(&r);
        assert_eq!(got.a_body_trimmed, r.a_body_trimmed);
        assert_eq!(got.b_body_trimmed, r.b_body_trimmed);
        assert!(serde_json::from_str::<Value>(&got.a_body_trimmed).is_ok());
        assert_eq!(got.body_cap, None);
        // 6 MiB and 6 MiB: 8 MiB for the two, half each, and the record says so.
        r.a_body_trimmed = body(6 << 20);
        r.b_body_trimmed = body(6 << 20);
        let got = back(&r);
        let (la, lb) = (
            escaped_len(&got.a_body_trimmed),
            escaped_len(&got.b_body_trimmed),
        );
        assert!(la == 4 << 20 && lb == 4 << 20, "{la} {lb}");
        let note = got.body_cap.unwrap();
        assert!(
            note.contains("8 MiB") && note.contains(&format!("of {}", escaped_len(&body(6 << 20)))),
            "{note}"
        );
        // A small side stays whole; the other (8 MiB, a merged paged answer) takes the rest.
        r.a_body_trimmed = body(1 << 10);
        r.b_body_trimmed = body(8 << 20);
        let got = back(&r);
        assert_eq!(got.a_body_trimmed, r.a_body_trimmed);
        assert_eq!(
            escaped_len(&got.b_body_trimmed),
            (8 << 20) - escaped_len(&r.a_body_trimmed)
        );
        // An explained record keeps 1 MiB a side, and says so.
        r.class = "allowlisted".into();
        r.a_body_trimmed = body(3 << 20);
        let got = back(&r);
        assert_eq!(escaped_len(&got.a_body_trimmed), BODY_TRIM);
        assert!(got.body_cap.unwrap().contains("1 MiB"));
    }

    fn line_of(r: &DiffRecord) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let log = DiffLog::new(dir.path()).unwrap();
        log.append(r).unwrap();
        let p = log.path_for(&date_of(now_secs()));
        std::fs::read(p).unwrap()
    }

    /// R-P5P9-60 H3: a body up to 1 MiB is kept whole (was cut at 4 KB), and the record line
    /// stays bounded whatever the bodies, request and diff values hold (with full bodies,
    /// R-P5P9-61, the bound is about 14.4 MiB).
    #[test]
    fn h3_records_keep_bodies_to_1_mib_and_stay_bounded() {
        // A 900 KB JSON body (the class CG-2 cut at 4 KB) is kept whole and parses.
        let rows: Vec<String> = (0..9000)
            .map(|i| format!("{{\"node\":{{\"id\":\"{i:090}\"}}}}"))
            .collect();
        let body = format!("{{\"data\":{{\"edges\":[{}]}}}}", rows.join(","));
        assert!(body.len() > 900_000 && body.len() < BODY_TRIM);
        let b = trim_body(body.as_bytes());
        let line = line_of(&rec(b, "QBlock {}".into(), vec![]));
        let back: DiffRecord = serde_json::from_slice(&line).unwrap();
        assert_eq!(back.a_body_trimmed, body);
        assert!(serde_json::from_str::<Value>(&back.b_body_trimmed).is_ok());
        // The worst case: 3 MiB of quotes and control bytes in every long field, 60 entries
        // with 1 MiB values and long paths. The line is bounded and still one JSON record.
        let worst: String = "\"\u{1}\\x".repeat(3 << 18);
        let big = Value::String("\"".repeat(1 << 20));
        let entries: Vec<DiffEntry> = (0..60)
            .map(|i| DiffEntry {
                path: format!("/{}{i}", "a/".repeat(5000)),
                kind: DiffKind::Changed,
                a: Some(big.clone()),
                b: Some(json!({"edges": [big.clone()]})),
            })
            .collect();
        let line = line_of(&rec(worst.clone(), worst, entries));
        assert!(
            line.len() <= MAX_RECORD_BYTES,
            "{} > {MAX_RECORD_BYTES}",
            line.len()
        );
        const { assert!(MAX_RECORD_BYTES < 16 << 20) };
        let back: DiffRecord = serde_json::from_slice(&line).unwrap();
        assert_eq!(back.diff.len(), crate::diff::MAX_ENTRIES);
        let a = back.diff[0].a.as_ref().and_then(Value::as_str).unwrap();
        assert!(a.starts_with("<clipped "), "{}", &a[..40]);
        // Small values are kept as they are.
        assert_eq!(bound_value(Some(json!(5))), Some(json!(5)));
    }

    /// Log growth on a CG-sized run (fv-checklist T-item; run by hand with `--ignored
    /// --nocapture`): 4,000 records whose bodies follow an assumed mix (72% about 3 KB a
    /// side, 20% 40 KB, 7% 400 KB, 1% 6 MiB), written once as unexplained records (full
    /// bodies, 8 MiB cap) and once under the 1 MiB-a-side trim they had before.
    #[test]
    #[ignore]
    fn log_growth_cg_sized_run() {
        let size = |i: usize| match i % 100 {
            0 => 6 << 20,
            1..=7 => 400 << 10,
            8..=27 => 40 << 10,
            _ => 3 << 10,
        };
        let mut out = Vec::new();
        for class in ["diff", "allowlisted"] {
            let dir = tempfile::tempdir().unwrap();
            let log = DiffLog::new(dir.path()).unwrap();
            for i in 0..4000 {
                let body = format!("{{\"data\":\"{}\"}}", "x".repeat(size(i)));
                let mut r = rec(body.clone(), format!("Q{i} {{}}"), vec![]);
                r.curl = "curl -s \"$A\"'/graphql' ; curl -s \"$B\"'/graphql'".into();
                r.note = None;
                r.class = class.into();
                log.append(&r).unwrap();
            }
            let n = std::fs::metadata(log.path_for(&date_of(now_secs())))
                .unwrap()
                .len();
            out.push((class, n));
        }
        for (class, n) in out {
            println!(
                "{class}: {n} bytes ({:.1} MiB)",
                n as f64 / (1u64 << 20) as f64
            );
        }
    }

    #[test]
    fn trim_escaped_counts_escapes_and_keeps_chars_whole() {
        assert_eq!(trim_escaped("abc", 3), "abc");
        assert_eq!(trim_escaped("a\"b", 2), "a");
        assert_eq!(trim_escaped("a\u{1}b", 6), "a");
        assert_eq!(trim_escaped("aé", 2), "a");
        assert_eq!(trim_escaped("aé", 3), "aé");
    }
}
