//! Accepted differences (`compare/allowlist.toml`, spec section 7.3).

use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

use crate::normalize::RouteKind;
use crate::report::{DiffEntry, DiffKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Condition {
    /// Only in A (`DiffKind::Extra`).
    AExtra,
    /// Only in B (`DiffKind::Missing`).
    BExtra,
    /// Value or type changed.
    Changed,
    Any,
}

impl Condition {
    fn matches(self, k: DiffKind) -> bool {
        match self {
            Condition::AExtra => k == DiffKind::Extra,
            Condition::BExtra => k == DiffKind::Missing,
            Condition::Changed => matches!(k, DiffKind::Changed | DiffKind::Type),
            Condition::Any => true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub id: String,
    /// `RouteKind::as_str()` name, or `"*"`.
    pub route: String,
    /// `*` matches inside one path segment, `**` across segments.
    pub json_path_glob: String,
    pub condition: Condition,
    /// Optional: the request (path and query) must contain this text.
    #[serde(default)]
    pub request_contains: Option<String>,
    pub reason: String,
    pub owner_note: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    #[serde(default, rename = "entry")]
    entries: Vec<Entry>,
}

#[derive(Debug, Clone, Default)]
pub struct Allowlist {
    pub entries: Vec<Entry>,
}

impl Allowlist {
    pub fn load(path: &Path) -> anyhow::Result<Allowlist> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let f: File =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        let mut ids = std::collections::HashSet::new();
        for e in &f.entries {
            anyhow::ensure!(ids.insert(e.id.clone()), "duplicate allowlist id {}", e.id);
        }
        Ok(Allowlist { entries: f.entries })
    }

    /// Splits `diffs` into unexplained entries and the ids of the entries that matched.
    pub fn classify(
        &self,
        route: &RouteKind,
        diffs: &[DiffEntry],
    ) -> (Vec<DiffEntry>, Vec<String>) {
        self.classify_request(route, "", diffs)
    }

    pub fn classify_request(
        &self,
        route: &RouteKind,
        request: &str,
        diffs: &[DiffEntry],
    ) -> (Vec<DiffEntry>, Vec<String>) {
        let mut unexplained = Vec::new();
        let mut ids: Vec<String> = Vec::new();
        for d in diffs {
            let hit = self.entries.iter().find(|e| {
                (e.route == "*" || e.route == route.as_str())
                    && e.condition.matches(d.kind)
                    && e.request_contains
                        .as_deref()
                        .is_none_or(|s| request.contains(s))
                    && glob(&e.json_path_glob, &d.path)
            });
            match hit {
                Some(e) => {
                    if !ids.contains(&e.id) {
                        ids.push(e.id.clone());
                    }
                }
                None => unexplained.push(d.clone()),
            }
        }
        (unexplained, ids)
    }
}

/// Glob over a JSON pointer: `**` matches anything, `*` anything but `/`.
pub fn glob(pat: &str, s: &str) -> bool {
    fn m(p: &[u8], s: &[u8]) -> bool {
        match p.first() {
            None => s.is_empty(),
            Some(b'*') if p.get(1) == Some(&b'*') => (0..=s.len()).any(|i| m(&p[2..], &s[i..])),
            Some(b'*') => {
                let lim = s.iter().position(|&c| c == b'/').unwrap_or(s.len());
                (0..=lim).any(|i| m(&p[1..], &s[i..]))
            }
            Some(&c) => s.first() == Some(&c) && m(&p[1..], &s[1..]),
        }
    }
    m(pat.as_bytes(), s.as_bytes())
}
