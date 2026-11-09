//! JSONL diff log and the daily summary (spec section 7.4). Files live under `out_dir`.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Body bytes kept per side in a diff record.
pub const BODY_TRIM: usize = 4096;

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

pub fn trim_body(b: &[u8]) -> String {
    let s = String::from_utf8_lossy(&b[..b.len().min(BODY_TRIM)]);
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
        let mut line = serde_json::to_vec(rec)?;
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
    use super::{sh_quote, sh_url};

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
}
