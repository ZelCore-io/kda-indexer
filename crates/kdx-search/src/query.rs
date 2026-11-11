//! Candidate query (plan Task 8 `Searcher::candidates`, S-4): the AND of the 3-grams of the
//! pattern's literal runs (at least 3 chars each) as a prefilter, then the stored code of each
//! prefilter hit verified with the LIKE matcher. Bounded by `max_candidates` prefilter hits
//! and a deadline checked every 256 hits.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

use kdx_types::like::Like;
use tantivy::query::{BooleanQuery, EnableScoring, Occur, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Value};
use tantivy::{DocSet, IndexReader, ReloadPolicy, TantivyDocument, Term, TERMINATED};

use crate::manifest::SearchManifest;
use crate::schema::{open, Fields};

/// Most 3-grams one query ANDs (any subset of a pattern's grams is still a superset filter).
pub const MAX_GRAMS: usize = 64;
/// Deadline check period, in prefilter hits.
const DEADLINE_EVERY: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    /// The pattern has no literal run of 3 or more chars: the index cannot narrow it (compat
    /// runs the bounded scan, native answers 400; S-4).
    #[error("no_index_path: the pattern has no literal run of 3 or more characters")]
    NoIndexPath,
    #[error("timeout: the search deadline passed")]
    Timeout,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<tantivy::TantivyError> for SearchError {
    fn from(e: tantivy::TantivyError) -> Self {
        SearchError::Other(e.into())
    }
}

impl From<std::io::Error> for SearchError {
    fn from(e: std::io::Error) -> Self {
        SearchError::Other(e.into())
    }
}

#[derive(Debug, Default, Clone)]
pub struct Candidates {
    /// Verified bodies: (CodeHash, first_h, last_h).
    pub hashes: Vec<([u8; 16], u64, u64)>,
    /// More than `max_candidates` prefilter hits: the rest were not examined.
    pub truncated: bool,
    /// Prefilter hits examined.
    pub examined: usize,
}

/// The distinct 3-grams of the pattern's literal runs, at most [`MAX_GRAMS`] (None: no run
/// of 3 or more chars). Every string the pattern matches contains each run, hence each gram.
pub fn grams(p: &Like) -> Option<Vec<String>> {
    let mut out = BTreeSet::new();
    for run in p.literal_runs() {
        let cs: Vec<char> = run.chars().collect();
        for w in cs.windows(3) {
            out.insert(w.iter().collect::<String>());
        }
    }
    if out.is_empty() {
        return None;
    }
    // Keep the longest-reaching spread when capping: every k-th gram in order.
    let v: Vec<String> = out.into_iter().collect();
    if v.len() <= MAX_GRAMS {
        return Some(v);
    }
    let step = v.len().div_ceil(MAX_GRAMS);
    Some(v.into_iter().step_by(step).collect())
}

/// Read side of `search/` (reloads on the writer's commits).
pub struct Searcher {
    reader: IndexReader,
    fields: Fields,
    pub manifest: SearchManifest,
    pub max_candidates: usize,
}

impl Searcher {
    /// None when `dir` holds no built index (no `manifest.json`).
    pub fn open(dir: &Path, max_candidates: usize) -> anyhow::Result<Option<Searcher>> {
        let Some(manifest) = SearchManifest::load(dir)? else {
            return Ok(None);
        };
        let (index, fields) = open(dir)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;
        Ok(Some(Searcher {
            reader,
            fields,
            manifest,
            max_candidates,
        }))
    }

    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    pub fn candidates(&self, p: &Like, deadline: Instant) -> Result<Candidates, SearchError> {
        let grams = grams(p).ok_or(SearchError::NoIndexPath)?;
        let f = self.fields;
        let clauses: Vec<(Occur, Box<dyn Query>)> = grams
            .iter()
            .map(|g| {
                let q: Box<dyn Query> = Box::new(TermQuery::new(
                    Term::from_field_text(f.code, g),
                    IndexRecordOption::Basic,
                ));
                (Occur::Must, q)
            })
            .collect();
        let q = BooleanQuery::new(clauses);
        let searcher = self.reader.searcher();
        let w = q.weight(EnableScoring::disabled_from_searcher(&searcher))?;
        let mut out = Candidates::default();
        'segments: for seg in searcher.segment_readers() {
            let mut sc = w.scorer(seg, 1.0)?;
            let store = seg.get_store_reader(8)?;
            let first_h = seg.fast_fields().u64("first_h")?;
            let last_h = seg.fast_fields().u64("last_h")?;
            let alive = seg.alive_bitset();
            let mut doc = sc.doc();
            while doc != TERMINATED {
                if alive.is_none_or(|a| a.is_alive(doc)) {
                    if out.examined >= self.max_candidates {
                        out.truncated = true;
                        break 'segments;
                    }
                    out.examined += 1;
                    if out.examined.is_multiple_of(DEADLINE_EVERY) && Instant::now() >= deadline {
                        return Err(SearchError::Timeout);
                    }
                    let d: TantivyDocument = store.get(doc)?;
                    let code = d.get_first(f.code).and_then(|v| v.as_str()).unwrap_or("");
                    if p.matches(code) {
                        let hb = d.get_first(f.hash).and_then(|v| v.as_bytes());
                        let Some(hash) = hb.and_then(|b| <[u8; 16]>::try_from(b).ok()) else {
                            return Err(
                                anyhow::anyhow!("search doc {doc} has no 16-byte hash").into()
                            );
                        };
                        out.hashes.push((
                            hash,
                            first_h.first(doc).unwrap_or(0),
                            last_h.first(doc).unwrap_or(u64::MAX),
                        ));
                    }
                }
                doc = sc.advance();
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use kdx_types::like::Like;

    use super::{grams, SearchError, Searcher};
    use crate::manifest::{SearchManifest, FORMAT};
    use crate::schema::{create, to_doc, writer, Doc};

    fn doc(code: &str, h: u64) -> Doc {
        Doc {
            hash: kdx_types::blake2b_128(code.as_bytes()),
            code: code.into(),
            first_h: h,
            last_h: h + 10,
        }
    }

    #[test]
    fn grams_cover_every_literal_run() {
        assert_eq!(grams(&Like::new("abcd")).unwrap(), vec!["abc", "bcd"]);
        assert_eq!(grams(&Like::new("ab%cde")).unwrap(), vec!["cde"]);
        assert!(grams(&Like::new("ab%cd_e")).is_none());
        let long: String = (0..300)
            .map(|i| char::from(b'a' + (i % 26) as u8))
            .collect();
        assert!(grams(&Like::new(&long)).unwrap().len() <= super::MAX_GRAMS);
    }

    #[test]
    fn candidates_prefilter_then_verify() {
        let dir = tempfile::tempdir().unwrap();
        let (index, f) = create(dir.path()).unwrap();
        let mut w = writer(&index, 1, 32).unwrap();
        let codes = [
            "(coin.transfer \"alice\" \"bob\" 1.0)",
            "(coin.transfer-create \"bob\" \"carol\" (read-keyset \"ks\") 2.5)",
            "(module foo GOV (defun bar () 1))",
            "(free.wiz.mint \"é€\" 3)",
        ];
        for (i, c) in codes.iter().enumerate() {
            w.add_document(to_doc(&f, &doc(c, i as u64 * 100))).unwrap();
        }
        w.commit().unwrap();
        SearchManifest {
            format: FORMAT,
            tantivy: crate::TANTIVY_VERSION.into(),
            mode: "full".into(),
            fallback_min_len: 0,
            built_through_height: 300,
            rebuilt_through_height: Some(300),
            docs: 4,
            bytes: 0,
            built_at: 1,
        }
        .commit(dir.path())
        .unwrap();
        let s = Searcher::open(dir.path(), 100).unwrap().unwrap();
        let far = Instant::now() + Duration::from_secs(10);
        let hits = |p: &str| {
            let mut v: Vec<u64> = s
                .candidates(&Like::new(p), far)
                .unwrap()
                .hashes
                .iter()
                .map(|x| x.1)
                .collect();
            v.sort();
            v
        };
        assert_eq!(hits("coin.transfer"), vec![0, 100]);
        assert_eq!(hits("transfer \"al"), vec![0]);
        // The 3-grams of both runs are present in doc 100, the order check rejects doc 0.
        assert_eq!(hits("carol%2.5"), vec![100]);
        assert_eq!(hits("(module foo"), vec![200]);
        assert_eq!(hits("\"é€\""), vec![300]);
        assert!(hits("transfer-crate").is_empty());
        assert!(matches!(
            s.candidates(&Like::new("co%in"), far),
            Err(SearchError::NoIndexPath)
        ));
        // Cap: one prefilter hit examined, the rest truncated.
        let s1 = Searcher::open(dir.path(), 1).unwrap().unwrap();
        let c = s1.candidates(&Like::new("coin."), far).unwrap();
        assert!(c.truncated && c.examined == 1);
        // The live file set of a snapshot.
        let sf = crate::snapshot::live_files(dir.path()).unwrap().unwrap();
        assert!(sf.files.iter().any(|f| f.ends_with(".store")));
        assert!(!sf
            .files
            .iter()
            .any(|f| f == "meta.json" || f == "manifest.json"));
    }
}
