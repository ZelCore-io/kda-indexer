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

/// An entry's standing (R-P5P9-66 concern 8). Both match the same way; the FV preconditions
/// need every entry ACCEPTED, and the controller flips a PROPOSED entry by a ruling once CG-4
/// shows its hits are explained by its rule and nothing more.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Status {
    Proposed,
    Accepted,
    /// R-P5P9-75 (P, Q): a gap the controller accepted for G4. Matched like the others, but a
    /// hit is **never** explained: `classify_request` leaves it unexplained, and
    /// `classify_known` returns it apart, as a `known-gap` record that the summary counts per
    /// entry and day and gates against `baseline_per_hour` × 1.5.
    #[serde(rename = "KNOWN-GAP")]
    KnownGap,
}

/// Why an entry is accepted (H2 item 6, owner R-P5P9-162): the docs list every accepted
/// difference with this class and the ruling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AcceptClass {
    /// Stock is wrong and the node (or stock's own error) shows it.
    StockWrongNodeEvidence,
    /// A difference documented as intended (the docs state it).
    IntendedDocumented,
    /// A feature the indexer does not offer, by design.
    UnsupportedByDesign,
    /// The two reads saw different tips (bounded window).
    TipTiming,
    /// Stock contradicts itself (its paging, cursor or count, or a re-fetch); no node read.
    StockSelfInconsistent,
    /// Stock lags or stopped (a configured cut, late fill); no node read.
    StockStale,
    /// A comparison convention of the harness, listed in docs/methodology.md.
    HarnessConvention,
    /// Our own defect, tracked as a known gap (never intended).
    OurDefect,
}

impl AcceptClass {
    pub fn as_str(self) -> &'static str {
        match self {
            AcceptClass::StockWrongNodeEvidence => "stock-wrong-node-evidence",
            AcceptClass::IntendedDocumented => "intended-documented",
            AcceptClass::UnsupportedByDesign => "unsupported-by-design",
            AcceptClass::TipTiming => "tip-timing",
            AcceptClass::StockSelfInconsistent => "stock-self-inconsistent",
            AcceptClass::StockStale => "stock-stale",
            AcceptClass::HarnessConvention => "harness-convention",
            AcceptClass::OurDefect => "our-defect",
        }
    }
}

/// One row of the table the docs list: id, reason, accept class, ruling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub id: String,
    pub reason: String,
    pub accept_class: &'static str,
    pub ruling: String,
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
    /// Optional: kda-indexer's answer (A, any page) must carry this response header
    /// (name, any case), e.g. `x-search-truncated` for a cut search.
    #[serde(default)]
    pub a_header: Option<String>,
    /// Optional (R-P5P9-75 P, Q): the request must contain one of these texts. A GraphQL
    /// request reads `graphql:<operation> <variables as JSON>`.
    #[serde(default)]
    pub request_any: Vec<String>,
    /// Optional (R-P5P9-75 P): the diff entry's A value, as JSON text, must contain this text.
    #[serde(default)]
    pub a_value_contains: Option<String>,
    /// KNOWN-GAP only, and required there: the CG-4 per-hour baseline of its records. The gate
    /// allows at most 1.5 × this per hour of the window.
    #[serde(default)]
    pub baseline_per_hour: Option<f64>,
    /// Required: PROPOSED, ACCEPTED or KNOWN-GAP.
    pub status: Status,
    /// Required: why the entry is accepted (see [`AcceptClass`]).
    pub accept_class: AcceptClass,
    pub reason: String,
    /// Required: the ruling (or, for an owner-accepted entry, the owner note) that admits it.
    pub ruling: String,
    pub owner_note: String,
    /// Owner H4 rule: the doc anchor (`docs/<file>.md#<anchor>`) that documents the
    /// difference, or the documented stock-fault class backed by per-record node evidence.
    /// An entry without one is **open**: it matches nothing, in the live and oracle harness
    /// as in the endpoint suite (H5 a), so its differences are reported.
    #[serde(default)]
    pub doc: Option<String>,
    /// H5 (b): for an accepted `stock-wrong-node-evidence` entry, the harness function and the
    /// per-record node read that gate its hit (`<module>::<fn>: <read>`).
    #[serde(default)]
    pub node_read: Option<String>,
}

impl Entry {
    /// The entry may explain a difference (it has a doc anchor). KNOWN-GAP entries are
    /// matched apart and never explain.
    pub fn is_open(&self) -> bool {
        self.status != Status::KnownGap && self.doc.is_none()
    }

    /// Owner rule (H5 a): the entry may explain a difference. An open entry, our own defect
    /// and a KNOWN-GAP entry explain nothing, so they match nothing either.
    pub fn explains(&self) -> bool {
        !self.is_open()
            && self.status != Status::KnownGap
            && self.accept_class != AcceptClass::OurDefect
    }
}

/// `docs/<file>.md#<anchor>` and the heading slug exists in that file (GitHub slug rules).
pub fn doc_anchor_ok(root: &Path, doc: &str) -> bool {
    let Some((file, anchor)) = doc.split_once('#') else {
        return false;
    };
    if !file.starts_with("docs/") || !file.ends_with(".md") || anchor.is_empty() {
        return false;
    }
    let Ok(text) = std::fs::read_to_string(root.join(file)) else {
        return false;
    };
    text.lines()
        .filter_map(|l| l.strip_prefix('#'))
        .map(|h| {
            h.trim_start_matches('#')
                .trim()
                .to_lowercase()
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == ' ' || *c == '-' || *c == '_')
                .map(|c| if c == ' ' { '-' } else { c })
                .collect::<String>()
        })
        .any(|slug| slug == anchor)
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
            anyhow::ensure!(
                !e.reason.trim().is_empty() && !e.ruling.trim().is_empty(),
                "allowlist entry {} needs a reason and a ruling",
                e.id
            );
            anyhow::ensure!(
                (e.status == Status::KnownGap) == e.baseline_per_hour.is_some()
                    && e.baseline_per_hour
                        .is_none_or(|b| b.is_finite() && b >= 0.0),
                "allowlist entry {}: a KNOWN-GAP entry needs baseline_per_hour (>= 0), and only \
                 a KNOWN-GAP entry takes one",
                e.id
            );
        }
        Ok(Allowlist { entries: f.entries })
    }

    /// The entry `id` is open (no doc anchor, not a KNOWN-GAP).
    pub fn is_open_id(&self, id: &str) -> bool {
        self.entries.iter().any(|e| e.id == id && e.is_open())
    }

    /// Every entry as a docs row: id, reason, accept class, ruling.
    pub fn table(&self) -> Vec<TableRow> {
        self.entries
            .iter()
            .map(|e| TableRow {
                id: e.id.clone(),
                reason: e.reason.clone(),
                accept_class: e.accept_class.as_str(),
                ruling: e.ruling.clone(),
            })
            .collect()
    }

    /// Splits `diffs` into unexplained entries and the ids of the entries that matched.
    pub fn classify(
        &self,
        route: &RouteKind,
        diffs: &[DiffEntry],
    ) -> (Vec<DiffEntry>, Vec<String>) {
        self.classify_request(route, "", &[], diffs)
    }

    /// `a_headers`: the response headers of A's pages. A KNOWN-GAP hit is never explained:
    /// it stays in the unexplained list here (use [`Allowlist::classify_known`] to see it).
    pub fn classify_request(
        &self,
        route: &RouteKind,
        request: &str,
        a_headers: &[(String, String)],
        diffs: &[DiffEntry],
    ) -> (Vec<DiffEntry>, Vec<String>) {
        let mut unexplained = Vec::new();
        let mut ids: Vec<String> = Vec::new();
        for d in diffs {
            match self.hit(route, request, a_headers, d) {
                Some(e) if e.status != Status::KnownGap => push_id(&mut ids, &e.id),
                _ => unexplained.push(d.clone()),
            }
        }
        (unexplained, ids)
    }

    /// [`Allowlist::classify_request`] with the KNOWN-GAP hits apart: they are neither
    /// explained nor unexplained, and their record is a `known-gap` one.
    pub fn classify_known(
        &self,
        route: &RouteKind,
        request: &str,
        a_headers: &[(String, String)],
        diffs: &[DiffEntry],
    ) -> Classified {
        let mut c = Classified::default();
        for d in diffs {
            match self.hit(route, request, a_headers, d) {
                Some(e) if e.status == Status::KnownGap => {
                    push_id(&mut c.known_ids, &e.id);
                    c.known.push(d.clone());
                }
                Some(e) => push_id(&mut c.ids, &e.id),
                None => c.unexplained.push(d.clone()),
            }
        }
        c
    }

    /// The CG-4 per-hour baseline of a KNOWN-GAP entry.
    pub fn baseline(&self, id: &str) -> Option<f64> {
        self.entries
            .iter()
            .find(|e| e.id == id)
            .and_then(|e| e.baseline_per_hour)
    }

    /// The explaining entry that matches `d` (H5 a: only those match). H6 (S7): the entry
    /// that takes `d` is the first fitting one whose glob names the path exactly, else the
    /// first fitting one; when that entry explains nothing (open, our defect, KNOWN-GAP) `d`
    /// stays unexplained, so a broad `/**` entry never picks up an open entry's hit.
    fn hit(
        &self,
        route: &RouteKind,
        request: &str,
        a_headers: &[(String, String)],
        d: &DiffEntry,
    ) -> Option<&Entry> {
        self.taker(route, request, a_headers, d)
            .filter(|e| e.explains())
    }

    /// The entry that takes `d`: specific (no `*` in the glob) before broad, then file order.
    fn taker(
        &self,
        route: &RouteKind,
        request: &str,
        a_headers: &[(String, String)],
        d: &DiffEntry,
    ) -> Option<&Entry> {
        let fit = |e: &&Entry| fits(e, route, request, a_headers, d);
        self.entries
            .iter()
            .filter(|e| !e.json_path_glob.contains('*'))
            .find(fit)
            .or_else(|| self.entries.iter().find(fit))
    }

    /// The ids of the non-explaining entries (open, our-defect, KNOWN-GAP) whose pattern fits
    /// a difference of `diffs`: shown beside a reported diff, never explaining it.
    pub fn unexplaining_ids(
        &self,
        route: &RouteKind,
        request: &str,
        a_headers: &[(String, String)],
        diffs: &[DiffEntry],
    ) -> Vec<String> {
        let mut ids = Vec::new();
        for d in diffs {
            if let Some(e) = self
                .taker(route, request, a_headers, d)
                .filter(|e| !e.explains())
            {
                push_id(&mut ids, &e.id);
            }
        }
        ids
    }
}

/// `e`'s pattern fits `d` (route, condition, request texts, A value and header, path glob).
fn fits(
    e: &Entry,
    route: &RouteKind,
    request: &str,
    a_headers: &[(String, String)],
    d: &DiffEntry,
) -> bool {
    // A streamed `/v1/txs` hash or row difference is a diff whatever the allowlist
    // says (R-P5P9-52 concern 5): a request-scoped `/**` entry must not explain it.
    if [crate::streamed::HASH_PATH, crate::streamed::ROW_PATH]
        .iter()
        .any(|p| d.path.starts_with(p))
    {
        return false;
    }
    (e.route == "*" || e.route == route.as_str())
        && e.condition.matches(d.kind)
        && e.request_contains
            .as_deref()
            .is_none_or(|s| request.contains(s))
        && (e.request_any.is_empty() || e.request_any.iter().any(|s| request.contains(s)))
        && e.a_value_contains
            .as_deref()
            .is_none_or(|t| d.a.as_ref().is_some_and(|v| v.to_string().contains(t)))
        && e.a_header
            .as_deref()
            .is_none_or(|h| a_headers.iter().any(|(k, _)| k.eq_ignore_ascii_case(h)))
        && glob(&e.json_path_glob, &d.path)
}

fn push_id(ids: &mut Vec<String>, id: &str) {
    if !ids.iter().any(|i| i == id) {
        ids.push(id.to_string());
    }
}

/// [`Allowlist::classify_known`]'s split of one record's diff entries.
#[derive(Debug, Clone, Default)]
pub struct Classified {
    pub unexplained: Vec<DiffEntry>,
    /// The explaining entries that matched.
    pub ids: Vec<String>,
    /// Entries a KNOWN-GAP entry matched: never explained.
    pub known: Vec<DiffEntry>,
    pub known_ids: Vec<String>,
}

impl Classified {
    /// The record's class and the entries it shows: `diff` (the unexplained, then the known
    /// gaps), `known-gap` (every entry) or `allowlisted` (every entry).
    pub fn class(&self, all: &[DiffEntry]) -> (&'static str, Vec<DiffEntry>) {
        if !self.unexplained.is_empty() {
            let mut d = self.unexplained.clone();
            d.extend(self.known.iter().cloned());
            ("diff", d)
        } else if !self.known.is_empty() {
            ("known-gap", all.to_vec())
        } else {
            ("allowlisted", all.to_vec())
        }
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

#[cfg(test)]
mod tests {
    use super::Allowlist;
    use crate::diff::diff_route;
    use crate::normalize::RouteKind;

    fn shipped() -> Allowlist {
        let p =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
        Allowlist::load(&p).unwrap()
    }

    /// R-P5P9-66 concern 8: every shipped entry carries `status` (PROPOSED or ACCEPTED),
    /// `reason` and `ruling`, read from the file itself so a missing key is seen even if the
    /// loader defaulted it.
    #[test]
    fn every_shipped_entry_carries_status_reason_and_ruling() {
        let p =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/allowlist.toml");
        let raw: toml::Value = toml::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap();
        let entries = raw["entry"].as_array().expect("entries");
        assert!(entries.len() > 40, "{} entries", entries.len());
        let mut lacking = Vec::new();
        for e in entries {
            let id = e["id"].as_str().unwrap_or("?");
            for k in ["status", "reason", "ruling"] {
                let v = e.get(k).and_then(toml::Value::as_str).unwrap_or("");
                if v.trim().is_empty() {
                    lacking.push(format!("{id}: {k}"));
                }
            }
            let st = e.get("status").and_then(toml::Value::as_str).unwrap_or("");
            if !["PROPOSED", "ACCEPTED", "KNOWN-GAP"].contains(&st) && !st.is_empty() {
                lacking.push(format!("{id}: status {st}"));
            }
        }
        assert!(lacking.is_empty(), "entries lacking a field: {lacking:?}");
        // The loader agrees: the shipped file loads with a status on every entry.
        let al = shipped();
        assert_eq!(al.entries.len(), entries.len());
        // Only entries the owner accepted, or the controller flipped by a ruling on a run's
        // evidence (R-P5P9-75, CG-4, or R-P5P9-86 / R-P5P9-103, CG-5, with its example records, or
        // fix3e S2 on the O1 diagnosis), are ACCEPTED.
        for e in &al.entries {
            if e.status == super::Status::Accepted {
                assert!(
                    e.owner_note.contains("by the owner")
                        || e.owner_note.contains("owner ruling")
                        || ((e.ruling.contains("R-P5P9-75, CG-4")
                            || e.ruling.contains("R-P5P9-86, CG-5")
                            || e.ruling.contains("R-P5P9-103, CG-5"))
                            && (e.owner_note.contains('#') || e.owner_note.contains("zero hits")))
                        // The controller's FIX-3 part E ruling S2, on the O1 diagnosis.
                        || (e.ruling.contains("fix3e S2") && e.reason.contains("o1o2 diagnosis")),
                    "{} is ACCEPTED without an owner acceptance or a flip ruling with evidence",
                    e.id
                );
            }
        }
    }

    /// An entry without `status` or `ruling`, with an empty one, or with a status other than
    /// R-P5P9-86 (allowlist): the nine entries checked against the node on CG-5 are
    /// ACCEPTED and cite the ruling; the `graphql-h7-window-referee` text names every
    /// unindexed one-sided row except E's and M's, not only D's and F's.
    #[test]
    fn e3_the_r86_flips_are_accepted_and_cite_the_ruling() {
        let al = shipped();
        for id in [
            "graphql-r75-exec-payload-data",
            "graphql-r75-payload-rollback",
            "graphql-r75-oracle-counts-heights",
            "graphql-r75-signer-address",
            "graphql-r75-nested-order-index",
            "kdatxs-missing-node",
            "kdatxs-events-node",
            "oracle-orphan",
            "oracle-gap",
        ] {
            let e = al.entries.iter().find(|e| e.id == id).expect(id);
            assert_eq!(e.status, super::Status::Accepted, "{id}");
            assert!(e.ruling.contains("R-P5P9-86"), "{id}: {}", e.ruling);
            assert!(e.owner_note.contains('#'), "{id}: {}", e.owner_note);
        }
        let w = al
            .entries
            .iter()
            .find(|e| e.id == "graphql-h7-window-referee")
            .unwrap();
        assert!(w.reason.contains("except E's and M's rows"), "{}", w.reason);
        assert!(w.reason.contains("not only R-P5P9-75 D's"), "{}", w.reason);
    }

    /// PROPOSED or ACCEPTED does not load.
    #[test]
    fn an_entry_without_a_status_or_ruling_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let load = |extra: &str| {
            let p = dir.path().join("a.toml");
            std::fs::write(
                &p,
                format!(
                    "[[entry]]\nid = \"x\"\nroute = \"*\"\njson_path_glob = \"/x\"\n\
                     condition = \"any\"\nreason = \"r\"\nowner_note = \"n\"\n\
                     accept_class = \"intended-documented\"\n{extra}"
                ),
            )
            .unwrap();
            Allowlist::load(&p)
        };
        assert!(load("status = \"PROPOSED\"\nruling = \"R-1\"\n").is_ok());
        assert!(load("status = \"ACCEPTED\"\nruling = \"R-1\"\n").is_ok());
        assert!(load("ruling = \"R-1\"\n").is_err(), "no status");
        assert!(load("status = \"PROPOSED\"\n").is_err(), "no ruling");
        assert!(
            load("status = \"PROPOSED\"\nruling = \" \"\n").is_err(),
            "empty ruling"
        );
        assert!(
            load("status = \"MAYBE\"\nruling = \"R-1\"\n").is_err(),
            "unknown status"
        );
    }

    /// H2 item 6 (owner R-P5P9-162): every entry carries an accept class, so the docs can
    /// list every accepted difference with why it is accepted. A missing or unknown class
    /// does not load.
    #[test]
    fn an_entry_without_an_accept_class_is_refused() {
        use super::AcceptClass;
        let dir = tempfile::tempdir().unwrap();
        let load = |extra: &str| {
            let p = dir.path().join("c.toml");
            std::fs::write(
                &p,
                format!(
                    "[[entry]]\nid = \"x\"\nroute = \"*\"\njson_path_glob = \"/x\"\n\
                     condition = \"any\"\nreason = \"r\"\nowner_note = \"n\"\n\
                     status = \"PROPOSED\"\nruling = \"R-1\"\n{extra}"
                ),
            )
            .unwrap();
            Allowlist::load(&p)
        };
        for (text, class) in [
            (
                "stock-wrong-node-evidence",
                AcceptClass::StockWrongNodeEvidence,
            ),
            ("intended-documented", AcceptClass::IntendedDocumented),
            ("unsupported-by-design", AcceptClass::UnsupportedByDesign),
            ("tip-timing", AcceptClass::TipTiming),
        ] {
            let al = load(&format!("accept_class = \"{text}\"\n")).unwrap();
            assert_eq!(al.entries[0].accept_class, class);
            assert_eq!(class.as_str(), text);
        }
        assert!(load("").is_err(), "no accept class");
        assert!(
            load("accept_class = \"whatever\"\n").is_err(),
            "unknown class"
        );
    }

    /// The shipped file: every entry has a class, and the table the docs list has id,
    /// reason, class and ruling for each.
    #[test]
    fn the_shipped_table_lists_id_reason_class_and_ruling_for_every_entry() {
        let al = shipped();
        let rows = al.table();
        assert_eq!(rows.len(), al.entries.len());
        for r in &rows {
            assert!(
                !r.id.is_empty() && !r.reason.trim().is_empty() && !r.ruling.trim().is_empty(),
                "{}",
                r.id
            );
        }
        assert!(rows.iter().any(|r| r.accept_class == "tip-timing"));
        assert!(rows
            .iter()
            .any(|r| r.accept_class == "unsupported-by-design"));
        assert!(rows
            .iter()
            .any(|r| r.accept_class == "stock-wrong-node-evidence"));
        assert!(rows.iter().any(|r| r.accept_class == "intended-documented"));
    }

    /// H4 (review S3-S5): the class says what evidence admits an entry. Stock-wrong only with a
    /// node read, stock contradicting itself or lagging has its own class, harness comparison
    /// conventions are listed in docs/methodology.md, and our own defect is never "intended".
    #[test]
    fn h4_accept_classes_match_the_evidence() {
        use super::{AcceptClass, Entry};
        let al = shipped();
        let class = |id: &str| {
            al.entries
                .iter()
                .find(|e| e.id == id)
                .unwrap_or_else(|| panic!("{id}"))
                .accept_class
                .as_str()
        };
        for id in [
            "cwd-throttled-page",
            "cwd-page-boundary-dup",
            "oracle-unstable",
            "oracle-limit-edge",
            "cwd-cursor-coinbase-skip",
            "graphql-r60-count-self-consistent",
        ] {
            assert_eq!(class(id), "stock-self-inconsistent", "{id}");
        }
        for id in ["graphql-oracle-stall", "oracle-tip-lag"] {
            assert_eq!(class(id), "stock-stale", "{id}");
        }
        assert_eq!(class("stats-count-age-lag"), "intended-documented");
        assert_eq!(class("known-gap-q-missing-chain-rows"), "our-defect");
        let docs = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/methodology.md"),
        )
        .unwrap();
        let conventions: Vec<&Entry> = al
            .entries
            .iter()
            .filter(|e| e.accept_class == AcceptClass::HarnessConvention)
            .collect();
        assert!(conventions.len() >= 11);
        for e in conventions {
            assert!(
                docs.contains(&format!("`{}`", e.id)),
                "{} is not listed in docs/methodology.md",
                e.id
            );
        }
        let page = al
            .entries
            .iter()
            .find(|e| e.id == "graphql-page-size")
            .unwrap();
        assert!(
            page.reason.contains("1,000") && !page.reason.contains("100 items"),
            "stale: {}",
            page.reason
        );
        for e in &al.entries {
            assert!(
                !e.ruling.starts_with("No ruling found") || e.ruling.contains("node's /poll"),
                "{}: no ruling and no evidence",
                e.id
            );
        }
        for (text, c) in [
            (
                "stock-self-inconsistent",
                AcceptClass::StockSelfInconsistent,
            ),
            ("stock-stale", AcceptClass::StockStale),
            ("harness-convention", AcceptClass::HarnessConvention),
            ("our-defect", AcceptClass::OurDefect),
        ] {
            assert_eq!(c.as_str(), text);
        }
    }

    /// Owner H4 rule: an entry accepts a difference only with a valid doc anchor; any other
    /// (non-KNOWN-GAP) entry is open and explains nothing.
    #[test]
    fn every_accepting_entry_has_a_valid_doc_anchor() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let al = shipped();
        let (mut acc, mut open, mut gap) = (0, 0, 0);
        for e in &al.entries {
            match (&e.doc, e.status) {
                (_, super::Status::KnownGap) => gap += 1,
                (Some(d), _) => {
                    assert!(
                        super::doc_anchor_ok(&root, d),
                        "{}: bad doc anchor {d}",
                        e.id
                    );
                    acc += 1;
                }
                (None, _) => {
                    assert!(e.is_open());
                    open += 1;
                }
            }
        }
        assert_eq!(acc + open + gap, al.entries.len());
        assert!(!super::doc_anchor_ok(
            &root,
            "docs/comparison.md#no-such-heading"
        ));
        assert!(!super::doc_anchor_ok(&root, "README.md#x"));
        let e = al
            .entries
            .iter()
            .find(|e| e.id == "oracle-unstable")
            .unwrap();
        assert!(e.is_open());
        assert!(al.is_open_id("oracle-unstable") && !al.is_open_id("oracle-orphan"));
    }

    /// Owner rule (H5 a): an open entry, an our-defect entry and a KNOWN-GAP entry match
    /// nothing in the live and oracle harness either; their diffs stay reported diffs.
    #[test]
    fn h5_open_our_defect_and_known_gap_entries_match_nothing() {
        use super::{AcceptClass, Status};
        use crate::report::{DiffEntry, DiffKind};
        let al = shipped();
        let one = |path: &str, kind: DiffKind| {
            vec![DiffEntry {
                path: path.into(),
                kind,
                a: Some(serde_json::json!(1)),
                b: Some(serde_json::json!(2)),
            }]
        };
        // oracle-unstable (open), known-gap-q (our-defect, KNOWN-GAP), known-gap-p (KNOWN-GAP).
        for (route, path, req) in [
            (RouteKind::V1Txs, "/~oracle-unstable", ""),
            (
                RouteKind::Other,
                "/~known-gap-missing-chain-rows",
                "graphql:QFungibleAccount {}",
            ),
            (
                RouteKind::Other,
                "/~graphql-follow-page-fault",
                "graphql:Q {\"a\":\"k:465174a5\"}",
            ),
        ] {
            let mut d = one(path, DiffKind::Changed);
            d[0].a = Some(serde_json::json!({"why": "request deadline exceeded"}));
            let c = al.classify_known(&route, req, &[], &d);
            assert_eq!(c.unexplained.len(), 1, "{path}");
            assert!(c.ids.is_empty() && c.known.is_empty(), "{path}");
            assert_eq!(c.class(&d).0, "diff", "{path}");
        }
        for e in &al.entries {
            if e.is_open()
                || e.accept_class == AcceptClass::OurDefect
                || e.status == Status::KnownGap
            {
                assert!(!e.explains(), "{}", e.id);
            } else {
                assert!(e.explains(), "{}", e.id);
            }
        }
        // A documented entry still explains.
        let d = one("/~oracle-orphan", DiffKind::Missing);
        let (left, ids) = al.classify(&RouteKind::Search, &d);
        assert!(left.is_empty() && ids == vec!["oracle-orphan".to_string()]);
    }

    /// H5 (b): a stock fault is accepted only with per-record node evidence in code. Each
    /// accepted `stock-wrong-node-evidence` entry names the node read in `node_read` (the
    /// harness function and the read), and the audit's unconfirmable entries are open.
    #[test]
    fn h5_stock_faults_name_their_per_record_node_read() {
        use super::{AcceptClass, Entry, Status};
        let al = shipped();
        let mut accepted = 0;
        for e in &al.entries {
            if e.accept_class == AcceptClass::StockWrongNodeEvidence && !e.is_open() {
                let n = e.node_read.as_deref().unwrap_or("");
                // Owner ruling 2026-11-19: the two failed-tx entries are accepted on the node
                // as the reference, without a per-record read in code.
                let owner = e
                    .ruling
                    .contains("Owner 2026-11-19: accepted as stock fault");
                assert!(
                    owner || (n.contains("node") && n.contains("::")),
                    "{}: no node read cited",
                    e.id
                );
                accepted += 1;
            }
        }
        assert_eq!(accepted, 26);
        for id in ["failed-tx-callstack", "failed-tx-error-info"] {
            assert!(!al.is_open_id(id), "{id}: owner ruling 2026-11-19");
        }
        for id in [
            "cwd-hole-tx-au4cjml8",
            "cwd-hole-txs-au4cjml8",
            "cwd-hole-events-7392263",
            "kdatxs-gap-cont-d7bf62",
            "oracle-event-order",
            "graphql-r58-oracle-empty",
            "graphql-r60-oracle-strips-quotes",
        ] {
            assert!(al.is_open_id(id), "{id} has no per-record node read");
        }
        let g = al
            .entries
            .iter()
            .find(|e| e.id == "genesis-multi-chain-tx")
            .unwrap();
        assert_eq!(g.accept_class, AcceptClass::IntendedDocumented);
        let n = |pred: &dyn Fn(&Entry) -> bool| al.entries.iter().filter(|e| pred(e)).count();
        // H6: + 5 harness conventions anchored (S4, R-P5P9-171/172) + iso-same-instant (S5).
        assert_eq!(n(&|e| e.explains()), 48, "accepted");
        assert_eq!(n(&|e| e.is_open()), 28, "open");
        assert_eq!(n(&|e| e.status == Status::KnownGap), 2, "known gaps");
    }

    /// I-4: a `search=` diff is excused only when kda-indexer said it cut the answer.
    #[test]
    fn search_window_needs_a_truncated_answer() {
        let al = shipped();
        let a = serde_json::json!([{"requestKey": "x", "height": 10}]);
        let b = serde_json::json!([{"requestKey": "y", "height": 10}]);
        let d = diff_route(&RouteKind::Search, &a, &b);
        assert!(!d.is_empty());
        let req = "/txs/search?search=coin.transfer&minheight=1&maxheight=99&limit=50";
        let (left, ids) = al.classify_request(&RouteKind::Search, req, &[], &d);
        assert_eq!(left.len(), d.len(), "a plain search diff is unexplained");
        assert!(ids.is_empty());
        // R-P5P9-77 item 3: `search-window` is back (PROPOSED; CG-4 could not log REST
        // hits): a cut answer's diff is its visible hit.
        let cut = [("x-search-truncated".to_string(), "true".to_string())];
        let (left, ids) = al.classify_request(&RouteKind::Search, req, &cut, &d);
        assert!(left.is_empty());
        assert_eq!(ids, vec!["search-window".to_string()]);
    }

    /// R-P5P9-48 item 4: `pactid-cap` matches only an answer carrying X-Search-Truncated (the
    /// 1,000-tx cap cut the walk); an uncapped pactid answer is compared in full.
    #[test]
    fn pactid_cap_needs_the_header() {
        let al = shipped();
        let a = serde_json::json!([{"requestKey": "x", "height": 10}]);
        let b = serde_json::json!([{"requestKey": "y", "height": 10}]);
        let d = diff_route(&RouteKind::Search, &a, &b);
        let req = "/txs/search?pactid=wfSVxyz";
        let (left, ids) = al.classify_request(&RouteKind::Search, req, &[], &d);
        assert_eq!(
            left.len(),
            d.len(),
            "an uncapped pactid diff is unexplained"
        );
        assert!(ids.is_empty());
        let other = [("content-type".to_string(), "application/json".to_string())];
        let (left, _) = al.classify_request(&RouteKind::Search, req, &other, &d);
        assert_eq!(left.len(), d.len());
        let cut = [("x-search-truncated".to_string(), "true".to_string())];
        let (left, ids) = al.classify_request(&RouteKind::Search, req, &cut, &d);
        assert!(left.is_empty());
        assert_eq!(ids, vec!["pactid-cap".to_string()]);
    }

    /// R-P5P9-10 #3: /coins within 1e-6 relative is a visible allowlist hit; beyond it, a diff.
    #[test]
    fn coins_tolerance_is_numeric() {
        let al = shipped();
        let a: serde_json::Value = serde_json::from_str("379699987.13176").unwrap();
        let near: serde_json::Value = serde_json::from_str("379699932.889").unwrap();
        let far: serde_json::Value = serde_json::from_str("379690000.0").unwrap();
        let d = diff_route(&RouteKind::Coins, &a, &near);
        let (left, ids) = al.classify_request(&RouteKind::Coins, "/coins", &[], &d);
        // H5 (a): coins-supply-tolerance is open; it matches nothing.
        assert!(left.len() == 1 && ids.is_empty());
        let d = diff_route(&RouteKind::Coins, &a, &far);
        let (left, ids) = al.classify_request(&RouteKind::Coins, "/coins", &[], &d);
        assert_eq!(left.len(), 1);
        assert!(ids.is_empty());
        assert!(diff_route(&RouteKind::Coins, &a, &a).is_empty());
    }
    /// R-P5P9-47: the run 6E-3 chainweb-data hole and kda-txs gap entries are tied to their
    /// requests; a genesis search (the 6E-F defect) and other requests stay unexplained.
    #[test]
    fn run_6e3_entries_are_scoped() {
        let al = shipped();
        let a = serde_json::json!([{"requestKey": "x", "height": 10}]);
        let b = serde_json::json!([{"requestKey": "y", "height": 10}]);
        let class = |route: RouteKind, req: &str| {
            let d = diff_route(&route, &a, &b);
            assert!(!d.is_empty());
            let (left, ids) = al.classify_request(&route, req, &[], &d);
            (left.len(), ids)
        };
        let miss = |route: RouteKind, req: &str| {
            let (left, ids) = class(route, req);
            assert!(left > 0 && ids.is_empty(), "{req} must stay unexplained");
        };
        // R-P5P9-77 item 3: the four one-offs are back (PROPOSED), each on its own request.
        let hit = |route: RouteKind, req: &str, id: &str| {
            let (left, ids) = class(route, req);
            assert_eq!(
                (left, ids),
                if al.is_open_id(id) {
                    (1, vec![])
                } else {
                    (0, vec![id.to_string()])
                },
                "{req} should be {id}"
            );
        };
        hit(
            RouteKind::Tx,
            "/txs/tx?requestkey=Au4cjML8abc",
            "cwd-hole-tx-au4cjml8",
        );
        hit(
            RouteKind::Txs,
            "/txs/txs?requestkey=Au4cjML8abc",
            "cwd-hole-txs-au4cjml8",
        );
        miss(RouteKind::Tx, "/txs/tx?requestkey=Other");
        hit(
            RouteKind::Events,
            "/txs/events?modulename=coin&name=TRANSFER_XCHAIN&minheight=7392263&maxheight=7392463",
            "cwd-hole-events-7392263",
        );
        miss(RouteKind::Events, "/txs/events?minheight=7392264");
        hit(
            RouteKind::V1Txs,
            "/v1/txs/k:d7bf62aa/coin/250",
            "kdatxs-gap-cont-d7bf62",
        );
        // R-P5P9-49: the one-off balances and search entries are gone; the coinbase skew rule
        // and the oracle-orphan, -lag and -gap classes cover them, so a diff on those
        // requests stays unexplained here. A pactid diff is unexplained unless the answer
        // says the cap cut it (pactid_cap_needs_the_header).
        miss(
            RouteKind::Balances,
            "/api/v1/accounts/k:e7f7130fab/balances?token=coin&chain=12",
        );
        miss(RouteKind::Balances, "/api/v1/accounts/k:other/balances");
        miss(RouteKind::Search, "/txs/search?pactid=Au4cjML8x");
        miss(RouteKind::Search, "/txs/search?pactid=5unnQ");
        miss(
            RouteKind::Search,
            "/txs/search?pactid=X4ltrWEU0sBD6h9w1hoo0Rq0IZAkYYpMQryxE9gNycI",
        );
        miss(RouteKind::Search, "/txs/search?pactid=wfSVxyz");
        // The genesis defect must stay visible: a plain keyset search is not allowlisted.
        miss(
            RouteKind::Search,
            "/txs/search?search=keyset+%22kad-ops-20%22%29&minheight=852004&maxheight=852104",
        );
    }

    /// R-P5P9-77 item 3: CG-4 did not log allowlisted REST records, so a REST entry's zero
    /// hits are no evidence. Every REST entry D2 removed for zero hits (its zero-hit table and
    /// the four one-offs) is back, PROPOSED, with the note that CG-5 decides.
    #[test]
    fn rest_zero_hit_removals_are_restored_as_proposed() {
        let al = shipped();
        for id in [
            "stats-estimate",
            "cwd-page-boundary-dup",
            "oracle-unstable",
            "kdatxs-gap",
            "search-window",
            "kdatxs-gap-cont-d7bf62",
            "cwd-hole-tx-au4cjml8",
            "cwd-hole-txs-au4cjml8",
            "cwd-hole-events-7392263",
        ] {
            let e = al
                .entries
                .iter()
                .find(|e| e.id == id)
                .unwrap_or_else(|| panic!("{id} is not restored"));
            assert_eq!(e.status, super::Status::Proposed, "{id}");
            assert!(
                e.owner_note
                    .contains("CG-4 could not log REST hits; CG-5 decides")
                    && e.ruling.contains("R-P5P9-77"),
                "{id}: {} / {}",
                e.owner_note,
                e.ruling
            );
        }
        // stats-estimate's rule is relative (1% of the count), with no per-block bound, so
        // x-count-age does not widen it; a count within 1% is its visible hit again.
        let a = serde_json::json!({"transactionCount": 1000});
        let near = serde_json::json!({"transactionCount": 1010});
        let d = diff_route(&RouteKind::Stats, &a, &near);
        let (left, ids) = al.classify_request(&RouteKind::Stats, "/stats", &[], &d);
        // H5 (a): stats-estimate (a 1 % tolerance) is open; it matches nothing.
        assert_eq!(left.len(), 1, "{d:?}");
        assert!(ids.is_empty());
    }

    /// R-P5P9-75 (P, Q): a KNOWN-GAP entry loads only with `baseline_per_hour`, and only it
    /// takes one. Its hit is never explained: `classify_request` leaves it unexplained, and
    /// `classify_known` returns it apart with a `known-gap` class.
    #[test]
    fn known_gap_is_never_explained() {
        use super::Status;
        use crate::report::{DiffEntry, DiffKind};
        let dir = tempfile::tempdir().unwrap();
        let load = |extra: &str| {
            let p = dir.path().join("k.toml");
            std::fs::write(
                &p,
                format!(
                    "[[entry]]\nid = \"k\"\nroute = \"*\"\njson_path_glob = \"/x\"\n\
                     condition = \"any\"\nreason = \"r\"\nruling = \"R-1\"\nowner_note = \"n\"\n\
                     accept_class = \"intended-documented\"\n{extra}"
                ),
            )
            .unwrap();
            Allowlist::load(&p)
        };
        let k = load("status = \"KNOWN-GAP\"\nbaseline_per_hour = 2.0\n").unwrap();
        assert_eq!(k.entries[0].status, Status::KnownGap);
        assert!(load("status = \"KNOWN-GAP\"\n").is_err(), "no baseline");
        assert!(load("status = \"ACCEPTED\"\nbaseline_per_hour = 1.0\n").is_err());
        assert!(load("status = \"KNOWN-GAP\"\nbaseline_per_hour = -1.0\n").is_err());
        let d = [DiffEntry {
            path: "/x".into(),
            kind: DiffKind::Changed,
            a: None,
            b: None,
        }];
        let (un, ids) = k.classify_request(&RouteKind::Other, "graphql:Q {}", &[], &d);
        assert_eq!((un.len(), ids.len()), (1, 0), "never explained");
        let c = k.classify_known(&RouteKind::Other, "graphql:Q {}", &[], &d);
        // H5 (a): a KNOWN-GAP entry matches nothing; its record is a plain diff.
        assert!(c.unexplained.len() == 1 && c.ids.is_empty());
        assert!(c.known_ids.is_empty());
        assert_eq!(c.class(&d).0, "diff");
        assert_eq!(
            k.unexplaining_ids(&RouteKind::Other, "graphql:Q {}", &[], &d),
            vec!["k"]
        );
        assert_eq!(k.baseline("k"), Some(2.0));
    }

    /// R-P5P9-75 P and Q, the shipped entries: P only on the measured heavy accounts and a
    /// "request deadline exceeded" page fault; Q only on the four account operations.
    #[test]
    fn shipped_known_gaps_are_scoped() {
        use crate::report::{DiffEntry, DiffKind};
        let al = shipped();
        let fault = |path: &str, why: &str| {
            vec![DiffEntry {
                path: path.into(),
                kind: DiffKind::Changed,
                a: Some(serde_json::json!({"page": 3, "status": 200, "why": why})),
                b: None,
            }]
        };
        let deadline = r#"errors: [{"message":"request deadline exceeded"}]"#;
        let req = |stem: &str, acct: &str| format!("graphql:{stem} {{\"accountName\":\"{acct}\"}}");
        let known = |r: &str, d: &[DiffEntry]| {
            let c = al.classify_known(&RouteKind::Other, r, &[], d);
            (c.class(d).0, c.known_ids)
        };
        let d = fault("/~graphql-follow-page-fault", deadline);
        let full = format!("k:465174a5{}", "0".repeat(56));
        assert_eq!(
            known(&req("QTransactionsByAccount", &full), &d),
            ("diff", vec![])
        );
        let d1 = fault("/~graphql-first-page-fault", deadline);
        // H5 (a): an open, our-defect or KNOWN-GAP entry matches nothing; the diff is reported.
        assert_eq!(known(&req("Transactions", "b581bc62ff"), &d1).0, "diff");
        // Another account (CG-4 #293's), another fault text: diffs.
        assert_eq!(known(&req("Transactions", "k:58ddb72d00"), &d1).0, "diff");
        let other = fault("/~graphql-follow-page-fault", "HTTP 500");
        assert_eq!(
            known(&req("QTransactionsByAccount", &full), &other).0,
            "diff"
        );
        // The key must start the value: a key inside another value is not the account.
        assert_eq!(
            known(&req("QTransactionsByAccount", "k:00465174a5"), &d).0,
            "diff"
        );
        // Q: the marker on the four operations is a known gap, elsewhere a diff.
        let q = vec![DiffEntry {
            path: crate::graphql_norm::MISSING_CHAIN_ROWS_PATH.into(),
            kind: DiffKind::Missing,
            a: None,
            b: Some(serde_json::json!({"list": "/data/x", "missing": [["1", null]]})),
        }];
        // The four operations' names come from the entry (no capture stem in the sources).
        let qe = al
            .entries
            .iter()
            .find(|e| e.id == "known-gap-q-missing-chain-rows")
            .unwrap();
        assert_eq!(qe.request_any.len(), 4);
        for r in &qe.request_any {
            let stem = r.trim_start_matches("graphql:").trim_end();
            // H5 (a): an open, our-defect or KNOWN-GAP entry matches nothing; the diff is reported.
            assert_eq!(known(&req(stem, "k:a"), &q).0, "diff", "{stem}");
        }
        assert_eq!(
            known(&req("QFungibleAccountsByPublicKey", "k:a"), &q).0,
            "diff"
        );
        assert_eq!(known(&req("QFungibleChainAccounts", "k:a"), &q).0, "diff");
        // A known gap with another unexplained entry in the record: a diff.
        let mut both = q.clone();
        both.push(DiffEntry {
            path: "/data/fungibleAccount/totalBalance".into(),
            kind: DiffKind::Changed,
            a: Some(serde_json::json!("#1e0")),
            b: Some(serde_json::json!("#2e0")),
        });
        assert_eq!(known(&req("QFungibleAccount", "k:a"), &both).0, "diff");
    }

    /// R-P5P9-86 P: the two heavy accounts CG-5 measured (#520, #977) are P's, by their full
    /// key; a key that only starts like one of them, or another fault, stays a diff.
    #[test]
    fn e2_p_the_cg5_heavy_accounts_are_known_gaps() {
        use crate::report::{DiffEntry, DiffKind};
        let al = shipped();
        let fault = |path: &str, why: &str| {
            vec![DiffEntry {
                path: path.into(),
                kind: DiffKind::Changed,
                a: Some(serde_json::json!({"page": 2, "status": 200, "why": why})),
                b: None,
            }]
        };
        let deadline = r#"errors: [{"message":"request deadline exceeded"}]"#;
        let req = |stem: &str, acct: &str| format!("graphql:{stem} {{\"accountName\":\"{acct}\"}}");
        let known = |r: &str, d: &[DiffEntry]| {
            let c = al.classify_known(&RouteKind::Other, r, &[], d);
            (c.class(d).0, c.known_ids)
        };
        let k520 = "af7943004d0b08fe5402c23ec317f7a8d1bdd6ce4cc52414efcf8dbfa68ca256";
        let k977 = "k:22fbc926b497bf3ecc7199b923039e74cdeda12726e1dc4f443953007cf8cc43";
        let follow = fault("/~graphql-follow-page-fault", deadline);
        let first = fault("/~graphql-first-page-fault", deadline);
        assert_eq!(
            known(&req("QTransactionsByAccount", k520), &follow),
            ("diff", vec![])
        );
        assert_eq!(known(&req("Transactions", k977), &first), ("diff", vec![]));
        // The full key only: a longer value that starts with it is another account.
        let longer = format!("{k977}00");
        assert_eq!(known(&req("Transactions", &longer), &first).0, "diff");
        // Another fault on the same account stays a diff.
        let other = fault("/~graphql-follow-page-fault", "HTTP 500");
        assert_eq!(
            known(&req("QTransactionsByAccount", k520), &other).0,
            "diff"
        );
    }

    fn entry(path: &str, kind: crate::report::DiffKind, a: serde_json::Value) -> Vec<crate::report::DiffEntry> {
        vec![crate::report::DiffEntry {
            path: path.into(),
            kind,
            a: Some(a),
            b: Some(serde_json::json!("stock")),
        }]
    }

    /// H6 (S3): the failed-tx entries cover only the owner ruling's scope: stock holding a
    /// callStack ours lacks, and ours answering the node's `<toplevel>:0` info.
    #[test]
    fn h6_failed_tx_entries_scoped_to_the_owner_ruling() {
        use crate::report::DiffKind;
        let al = shipped();
        let r = RouteKind::Tx;
        let cs = |kind, path: &str| al.classify(&r, &entry(path, kind, serde_json::json!([1]))).0.len();
        assert_eq!(cs(DiffKind::Missing, "/result/error/callStack"), 0);
        assert_eq!(cs(DiffKind::Extra, "/result/error/callStack"), 1);
        assert_eq!(cs(DiffKind::Changed, "/result/error/callStack/0"), 1);
        let info = |a: &str| {
            al.classify(&r, &entry("/result/error/info", DiffKind::Changed, serde_json::json!(a)))
                .0
                .len()
        };
        assert_eq!(info("<toplevel>:0"), 0);
        assert_eq!(info("<interactive>:0:5"), 1);
        assert_eq!(info("x <toplevel>:0"), 1);
    }

    /// H6 (S6): event-search-window explains only whole rows one side lists.
    #[test]
    fn h6_event_search_window_explains_only_the_window() {
        use crate::report::DiffKind;
        let al = shipped();
        let rq = "/txs/events?search=transfer&limit=50";
        let row = entry("/7", DiffKind::Missing, serde_json::json!({}));
        assert!(al.classify_known(&RouteKind::Events, rq, &[], &row).unexplained.is_empty());
        let field = entry("/3/params/2", DiffKind::Changed, serde_json::json!(1.5));
        assert_eq!(al.classify_known(&RouteKind::Events, rq, &[], &field).unexplained.len(), 1);
    }

    /// H7 (SHOULD-3, R-P5P9-173): the window is ours', so it explains only rows stock lists and
    /// ours does not; a row only ours lists, or a row null on one side, stays a diff.
    #[test]
    fn h7_event_search_window_explains_only_stock_extra_rows() {
        use crate::report::{DiffEntry, DiffKind};
        let al = shipped();
        let rq = "/txs/events?search=transfer&limit=50";
        let un = |d: &[DiffEntry]| al.classify_known(&RouteKind::Events, rq, &[], d).unexplained.len();
        assert_eq!(un(&entry("/7", DiffKind::Missing, serde_json::json!({}))), 0);
        let extra = [DiffEntry {
            path: "/7".into(),
            kind: DiffKind::Extra,
            a: Some(serde_json::json!({})),
            b: None,
        }];
        assert_eq!(un(&extra), 1);
        let null_row = [DiffEntry {
            path: "/7".into(),
            kind: DiffKind::Type,
            a: Some(serde_json::Value::Null),
            b: Some(serde_json::json!({})),
        }];
        assert_eq!(un(&null_row), 1);
    }

    /// H7 (SHOULD-2): the iso-same-instant hit carries the field path and still explains.
    #[test]
    fn h7_iso_same_instant_hit_with_field_path_explains() {
        let al = shipped();
        let d = entry(
            &format!("{}/data/block/creationTime", crate::diff::ISO_SAME_INSTANT_PATH),
            crate::report::DiffKind::Changed,
            serde_json::json!("2024-01-01T00:00:00Z"),
        );
        assert!(al.classify_known(&RouteKind::Tx, "", &[], &d).unexplained.is_empty());
    }

    /// H6 (S7): an open entry that names a hit path exactly takes it before a broad accepted
    /// `/**` entry, so the hit stays unexplained (and is named as open).
    #[test]
    fn h6_specific_open_entry_beats_broad_accepted_entry() {
        use crate::report::DiffKind;
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("a.toml");
        std::fs::write(
            &f,
            "[[entry]]\nid = \"broad\"\nroute = \"*\"\njson_path_glob = \"/**\"\ncondition = \"any\"\n\
             reason = \"r\"\nstatus = \"ACCEPTED\"\naccept_class = \"intended-documented\"\nruling = \"r\"\nowner_note = \"n\"\n\
             doc = \"docs/comparison.md#intended-differences\"\n\
             [[entry]]\nid = \"open-one\"\nroute = \"*\"\njson_path_glob = \"/~event-order\"\ncondition = \"any\"\n\
             reason = \"r\"\nstatus = \"PROPOSED\"\naccept_class = \"stock-wrong-node-evidence\"\nruling = \"r\"\nowner_note = \"n\"\n",
        )
        .unwrap();
        let al = Allowlist::load(&f).unwrap();
        let d = entry("/~event-order", DiffKind::Changed, serde_json::json!(1));
        let c = al.classify_known(&RouteKind::Search, "/txs/search?search=x", &[], &d);
        assert_eq!(c.unexplained.len(), 1);
        assert!(c.ids.is_empty());
        assert_eq!(
            al.unexplaining_ids(&RouteKind::Search, "", &[], &d),
            vec!["open-one".to_string()]
        );
        // the broad entry still explains what no specific entry names
        let d = entry("/0/x", DiffKind::Changed, serde_json::json!(1));
        assert!(al.classify_known(&RouteKind::Search, "", &[], &d).unexplained.is_empty());
    }

    /// H6 (S4, S5): the harness conventions of docs/methodology.md that tolerate no value
    /// difference have a doc anchor and explain; the ones that do (f64 floats, the per-field
    /// normalisers, the coinbase key form) stay open.
    #[test]
    fn h6_conventions_anchored_unless_they_tolerate_a_value() {
        let al = shipped();
        let by = |id: &str| al.entries.iter().find(|e| e.id == id).unwrap();
        for id in [
            "graphql-r75-nested-multiset",
            "graphql-h6-prefix",
            "graphql-r60-scoped-list",
            "graphql-count-walk",
            "rest-compose",
            "iso-same-instant",
        ] {
            assert!(by(id).explains(), "{id}");
            assert_eq!(by(id).accept_class, super::AcceptClass::HarnessConvention, "{id}");
        }
        for id in [
            "graphql-r57-float",
            "graphql-r57-field",
            "graphql-coinbase-oracle-key-form",
            "graphql-r75-coinbase-request-key",
            "graphql-native-miner-skew",
        ] {
            assert!(by(id).is_open(), "{id}");
        }
        let d = entry(
            &format!("{}/t", crate::diff::ISO_SAME_INSTANT_PATH),
            crate::report::DiffKind::Changed,
            serde_json::json!("2024-01-01T00:00:00Z"),
        );
        assert!(al.classify_known(&RouteKind::Tx, "", &[], &d).unexplained.is_empty());
    }
}
