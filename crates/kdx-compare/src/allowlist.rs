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
    /// Optional: kda-indexer's answer (A, any page) must carry this response header
    /// (name, any case), e.g. `x-search-truncated` for a cut search.
    #[serde(default)]
    pub a_header: Option<String>,
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
    /// R-P5P9-47: the run 6E-3 oracle defects are tied to their requests; a genesis search
    /// (the 6E-F defect) and other requests stay unexplained.
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
        hit(
            RouteKind::Balances,
            "/api/v1/accounts/k:e7f7130fab/balances?token=coin&chain=12",
            "balances-tip-skew-e7f7130f",
        );
        miss(RouteKind::Balances, "/api/v1/accounts/k:other/balances");
        // search: pact-specific classes keep their own hits; any other pactid diff is
        // unexplained unless the answer says the cap cut it (pactid_cap_needs_the_header).
        hit(
            RouteKind::Search,
            "/txs/search?pactid=Au4cjML8x",
            "cwd-search-hole-au4cjml8",
        );
        hit(
            RouteKind::Search,
            "/txs/search?pactid=5unnQ",
            "cwd-search-behind-5unn",
        );
        miss(RouteKind::Search, "/txs/search?pactid=wfSVxyz");
        // The genesis defect must stay visible: a plain keyset search is not allowlisted.
        miss(
            RouteKind::Search,
            "/txs/search?search=keyset+%22kad-ops-20%22%29&minheight=852004&maxheight=852104",
        );
    }
}
