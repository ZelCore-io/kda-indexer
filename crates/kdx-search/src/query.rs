//! Candidate query (plan Task 8 `Searcher::candidates`, S-4): the AND of the 3-grams of the
//! pattern's literal runs (at least 3 chars each) as a prefilter, then the stored code of each
//! prefilter hit verified with the LIKE matcher.
//!
//! The prefilter walk reads only fast fields: it drops docs that cannot have a tx in the
//! query's height window ([`Window`]) and keeps the `max_candidates` newest of the rest (by
//! `last_h`, then `first_h`). Those are verified newest first, so a cap or a deadline cuts the
//! oldest bodies, not an arbitrary subset (tantivy's doc order is roughly oldest range first).
//! The walk gets 3/5 of the time to the deadline (checked every 4096 docs); verification
//! checks it every 16 docs and every 256 KiB of verified code.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
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
/// Deadline check period of the prefilter walk, in docs.
const WALK_EVERY: usize = 4096;
/// Deadline check period of verification, in docs and in verified code bytes.
const VERIFY_EVERY: usize = 16;
const VERIFY_BYTES: usize = 256 << 10;

/// The heights a query asks for: `lo..=hi`, and `upper_from`, the first height of readers'
/// upper expansion window (`min(rebuilt_through_height + 1, hot floor)`; 0 without the
/// field). A doc's txs lie in `first_h..=last_h` plus every height from `upper_from` on, and
/// never below `first_h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub lo: u64,
    pub hi: u64,
    pub upper_from: u64,
}

impl Window {
    /// Every height.
    pub const ALL: Window = Window {
        lo: 0,
        hi: u64::MAX,
        upper_from: 0,
    };

    /// False when no tx of a doc with these heights can lie in the window.
    pub fn admits(&self, first_h: u64, last_h: u64) -> bool {
        first_h <= self.hi && (self.hi >= self.upper_from || last_h >= self.lo)
    }
}

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
    /// Verified bodies: (CodeHash, first_h, last_h), newest first.
    pub hashes: Vec<([u8; 16], u64, u64)>,
    /// More than `max_candidates` prefilter hits in the window, or the deadline cut the walk
    /// or the verification: older bodies were not examined.
    pub truncated: bool,
    /// Prefilter hits verified.
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

    /// Verified bodies for `p` that can have a tx in `w`, newest first (see the module doc).
    /// `Timeout` when the deadline passed before any body was verified.
    pub fn candidates(
        &self,
        p: &Like,
        w: Window,
        deadline: Instant,
    ) -> Result<Candidates, SearchError> {
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
        let wt = q.weight(EnableScoring::disabled_from_searcher(&searcher))?;
        let start = Instant::now();
        let walk_until = start + deadline.saturating_duration_since(start) * 3 / 5;
        let k = self.max_candidates.max(1);
        // Min-heap of the k newest admitted docs: (last_h, first_h, segment, doc).
        let mut top: BinaryHeap<Reverse<(u64, u64, u32, u32)>> = BinaryHeap::with_capacity(k);
        let (mut admitted, mut walked) = (0usize, 0usize);
        let mut out = Candidates::default();
        let segs = searcher.segment_readers();
        'segments: for (ord, seg) in segs.iter().enumerate() {
            let mut sc = wt.scorer(seg, 1.0)?;
            let first_h = seg.fast_fields().u64("first_h")?;
            let last_h = seg.fast_fields().u64("last_h")?;
            let alive = seg.alive_bitset();
            let mut doc = sc.doc();
            while doc != TERMINATED {
                if alive.is_none_or(|a| a.is_alive(doc)) {
                    walked += 1;
                    if walked.is_multiple_of(WALK_EVERY) && Instant::now() >= walk_until {
                        out.truncated = true;
                        break 'segments;
                    }
                    let fh = first_h.first(doc).unwrap_or(0);
                    let lh = last_h.first(doc).unwrap_or(u64::MAX);
                    if w.admits(fh, lh) {
                        admitted += 1;
                        let key = (lh, fh, ord as u32, doc);
                        if top.len() < k {
                            top.push(Reverse(key));
                        } else if top.peek().is_some_and(|Reverse(m)| key > *m) {
                            top.pop();
                            top.push(Reverse(key));
                        }
                    }
                }
                doc = sc.advance();
            }
        }
        out.truncated |= admitted > k;
        let mut cands: Vec<(u64, u64, u32, u32)> = top.into_iter().map(|Reverse(x)| x).collect();
        cands.sort_unstable_by(|a, b| b.cmp(a));
        let mut stores: Vec<Option<tantivy::store::StoreReader>> =
            (0..segs.len()).map(|_| None).collect();
        let mut bytes = 0usize;
        for (i, &(lh, fh, ord, doc)) in cands.iter().enumerate() {
            if i.is_multiple_of(VERIFY_EVERY) || bytes >= VERIFY_BYTES {
                bytes = 0;
                if Instant::now() >= deadline {
                    out.truncated = true;
                    break;
                }
            }
            out.examined += 1;
            let store = match &mut stores[ord as usize] {
                Some(s) => s,
                slot => slot.insert(segs[ord as usize].get_store_reader(8)?),
            };
            let d: TantivyDocument = store.get(doc)?;
            let code = d.get_first(f.code).and_then(|v| v.as_str()).unwrap_or("");
            bytes += code.len();
            if p.matches(code) {
                let hb = d.get_first(f.hash).and_then(|v| v.as_bytes());
                let Some(hash) = hb.and_then(|b| <[u8; 16]>::try_from(b).ok()) else {
                    return Err(anyhow::anyhow!("search doc {doc} has no 16-byte hash").into());
                };
                out.hashes.push((hash, fh, lh));
            }
        }
        if out.truncated && out.examined == 0 && Instant::now() >= deadline {
            return Err(SearchError::Timeout);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use kdx_types::like::Like;

    use super::{grams, SearchError, Searcher, Window};
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
                .candidates(&Like::new(p), Window::ALL, far)
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
            s.candidates(&Like::new("co%in"), Window::ALL, far),
            Err(SearchError::NoIndexPath)
        ));
        // Cap: one prefilter hit examined, the rest truncated.
        let s1 = Searcher::open(dir.path(), 1).unwrap().unwrap();
        let c = s1
            .candidates(&Like::new("coin."), Window::ALL, far)
            .unwrap();
        assert!(c.truncated && c.examined == 1);
        // The newest of the two is kept.
        assert_eq!(c.hashes[0].1, 100);
        // The live file set of a snapshot.
        let sf = crate::snapshot::live_files(dir.path()).unwrap().unwrap();
        assert!(sf.files.iter().any(|f| f.ends_with(".store")));
        assert!(!sf
            .files
            .iter()
            .any(|f| f == "meta.json" || f == "manifest.json"));
    }

    fn manifest(dir: &std::path::Path) {
        SearchManifest {
            format: FORMAT,
            tantivy: crate::TANTIVY_VERSION.into(),
            mode: "full".into(),
            fallback_min_len: 0,
            built_through_height: 1_000,
            rebuilt_through_height: Some(1_000),
            docs: 0,
            bytes: 0,
            built_at: 1,
        }
        .commit(dir)
        .unwrap();
    }

    /// I-3: more matching bodies than `max_candidates`, written oldest first over several
    /// segments: the cap keeps the newest, and a narrow window near the tip finds its bodies.
    #[test]
    fn the_cap_keeps_the_newest_bodies_and_windows_prune() {
        let dir = tempfile::tempdir().unwrap();
        let (index, f) = create(dir.path()).unwrap();
        let mut w = writer(&index, 1, 32).unwrap();
        for i in 0..50u64 {
            let d = Doc {
                hash: kdx_types::blake2b_128(format!("b{i}").as_bytes()),
                code: format!("(coin.transfer \"a{i}\" \"b\" 1.0)"),
                first_h: i * 10,
                last_h: i * 10 + 5,
            };
            w.add_document(to_doc(&f, &d)).unwrap();
            if i % 7 == 6 {
                w.commit().unwrap();
            }
        }
        w.commit().unwrap();
        manifest(dir.path());
        let s = Searcher::open(dir.path(), 10).unwrap().unwrap();
        let far = Instant::now() + Duration::from_secs(10);
        let lows = |c: &super::Candidates| c.hashes.iter().map(|x| x.1).collect::<Vec<_>>();
        let p = Like::new("(coin.transfer \"");
        let c = s.candidates(&p, Window::ALL, far).unwrap();
        assert!(c.truncated);
        assert_eq!(lows(&c), (40..50).rev().map(|i| i * 10).collect::<Vec<_>>());
        // ±30 heights around 460, below the upper window: only bodies seen there.
        let near = Window {
            lo: 430,
            hi: 490,
            upper_from: 1_001,
        };
        let c = s.candidates(&p, near, far).unwrap();
        assert!(!c.truncated);
        assert_eq!(lows(&c), vec![490, 480, 470, 460, 450, 440, 430]);
        // A window inside the upper window admits every body first seen at or below it.
        let upper = Window {
            lo: 1_100,
            hi: 1_150,
            upper_from: 1_001,
        };
        let c = s.candidates(&p, upper, far).unwrap();
        assert!(c.truncated && c.hashes.len() == 10 && c.hashes[0].1 == 490);
        // Nothing before 120 was first seen at 130 or later.
        let early = Window {
            lo: 0,
            hi: 120,
            upper_from: 0,
        };
        assert_eq!(
            lows(&s.candidates(&p, early, far).unwrap()),
            vec![120, 110, 100, 90, 80, 70, 60, 50, 40, 30]
        );
        // M-2: a passed deadline stops verification at once.
        let past = Instant::now() - Duration::from_millis(1);
        assert!(matches!(
            s.candidates(&p, Window::ALL, past),
            Err(SearchError::Timeout)
        ));
    }
}
