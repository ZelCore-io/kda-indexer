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
    /// Required: PROPOSED or ACCEPTED.
    pub status: Status,
    pub reason: String,
    /// Required: the ruling (or, for an owner-accepted entry, the owner note) that admits it.
    pub ruling: String,
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
            anyhow::ensure!(
                !e.reason.trim().is_empty() && !e.ruling.trim().is_empty(),
                "allowlist entry {} needs a reason and a ruling",
                e.id
            );
        }
        Ok(Allowlist { entries: f.entries })
    }

    /// Splits `diffs` into unexplained entries and the ids of the entries that matched.
    pub fn classify(
        &self,
        route: &RouteKind,
        diffs: &[DiffEntry],
    ) -> (Vec<DiffEntry>, Vec<String>) {
        self.classify_request(route, "", &[], diffs)
    }

    /// `a_headers`: the response headers of A's pages.
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
            let hit = self.entries.iter().find(|e| {
                (e.route == "*" || e.route == route.as_str())
                    && e.condition.matches(d.kind)
                    && e.request_contains
                        .as_deref()
                        .is_none_or(|s| request.contains(s))
                    && e.a_header
                        .as_deref()
                        .is_none_or(|h| a_headers.iter().any(|(k, _)| k.eq_ignore_ascii_case(h)))
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
            if !["PROPOSED", "ACCEPTED"].contains(&st) && !st.is_empty() {
                lacking.push(format!("{id}: status {st}"));
            }
        }
        assert!(lacking.is_empty(), "entries lacking a field: {lacking:?}");
        // The loader agrees: the shipped file loads with a status on every entry.
        let al = shipped();
        assert_eq!(al.entries.len(), entries.len());
        // Only entries whose owner note says the owner accepted them are ACCEPTED.
        for e in &al.entries {
            if e.status == super::Status::Accepted {
                assert!(
                    e.owner_note.contains("by the owner") || e.owner_note.contains("owner ruling"),
                    "{} is ACCEPTED without an owner acceptance in its note",
                    e.id
                );
            }
        }
    }

    /// An entry without `status` or `ruling`, with an empty one, or with a status other than
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
                     condition = \"any\"\nreason = \"r\"\nowner_note = \"n\"\n{extra}"
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
        assert!(left.is_empty() && ids == vec!["coins-supply-tolerance".to_string()]);
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
        let hit = |route: RouteKind, req: &str, id: &str| {
            let (left, ids) = class(route, req);
            assert_eq!(
                (left, ids),
                (0, vec![id.to_string()]),
                "{req} should be {id}"
            );
        };
        let miss = |route: RouteKind, req: &str| {
            let (left, ids) = class(route, req);
            assert!(left > 0 && ids.is_empty(), "{req} must stay unexplained");
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
}
