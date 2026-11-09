//! Replay of real traffic (spec section 7.2 item 1): weighted by observed counts.
//!
//! Input TSV: `count\tMETHOD\tpath?query`, GET only. `import` builds it from HAProxy HTTP log
//! lines, keeping only the request line of chainweb-data routes (spec section 5.2): client
//! addresses, headers and timings are never written.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use anyhow::Context;
use rand::rngs::StdRng;
use rand::RngExt;

use super::{
    is_clean_path, is_request_key, rng, Generator, OracleRef, Origin, Request, CHAIN_FROM_A,
};
use crate::normalize::RouteKind;

pub struct ReplayGen {
    entries: Vec<(u64, String)>,
    /// Cumulative counts, for weighted sampling.
    cum: Vec<u64>,
    rng: StdRng,
}

impl ReplayGen {
    pub fn load(path: &Path) -> anyhow::Result<ReplayGen> {
        let f = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let mut entries = Vec::new();
        for (i, line) in BufReader::new(f).lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            let mut it = line.splitn(3, '\t');
            let (Some(c), Some(m), Some(p)) = (it.next(), it.next(), it.next()) else {
                anyhow::bail!("{}:{}: expected count, method, path", path.display(), i + 1);
            };
            let count: u64 = c
                .parse()
                .with_context(|| format!("{}:{}: bad count", path.display(), i + 1))?;
            if m != "GET" || !is_clean_path(p) || count == 0 {
                continue;
            }
            entries.push((count, p.to_string()));
        }
        anyhow::ensure!(!entries.is_empty(), "{}: no GET entries", path.display());
        let mut cum = Vec::with_capacity(entries.len());
        let mut acc: u64 = 0;
        for (c, _) in &entries {
            acc = acc
                .checked_add(*c)
                .with_context(|| format!("{}: total count overflows u64", path.display()))?;
            cum.push(acc);
        }
        Ok(ReplayGen {
            entries,
            cum,
            rng: rng(),
        })
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// The `requestkey` query value, if any.
fn request_key(path: &str) -> Option<String> {
    let q = path.split_once('?')?.1;
    q.split('&')
        .find_map(|kv| kv.strip_prefix("requestkey="))
        .filter(|rk| is_request_key(rk))
        .map(str::to_string)
}

impl Generator for ReplayGen {
    async fn next(&mut self) -> anyhow::Result<Vec<Request>> {
        let total = *self.cum.last().expect("non-empty");
        let x = self.rng.random_range(0..total);
        let i = self.cum.partition_point(|&c| c <= x);
        let path = self.entries[i].1.clone();
        let oracle = match RouteKind::from_path(&path) {
            RouteKind::Tx | RouteKind::Txs => request_key(&path).map(|rk| OracleRef::Poll {
                chain: CHAIN_FROM_A,
                request_key: rk,
            }),
            _ => None,
        };
        Ok(vec![Request::new(path, Origin::Replay, oracle)])
    }
}

/// Other chainweb-data routes kept by `import` besides those `RouteKind` knows.
const EXTRA_ROUTES: &[&str] = &["/richlist.csv", "/cwd-spec", "/cwd-spec.json"];

/// The `path?query` of a GET request line inside an HAProxy HTTP log line, if it is a
/// spec section 5.2 route. Absolute-form URIs are reduced to their path.
pub fn extract(line: &str) -> Option<String> {
    // The request line is the last quoted field ("GET /x HTTP/1.1").
    let end = line.rfind('"')?;
    let start = line[..end].rfind('"')?;
    let req = &line[start + 1..end];
    let mut parts = req.split(' ');
    let method = parts.next()?;
    let mut uri = parts.next()?;
    if method != "GET" {
        return None;
    }
    if let Some(rest) = uri
        .strip_prefix("http://")
        .or_else(|| uri.strip_prefix("https://"))
    {
        uri = &rest[rest.find('/')?..];
    }
    // Untrusted input: no whitespace or control characters anywhere in the path.
    if !is_clean_path(uri) {
        return None;
    }
    let path = uri.split('?').next()?;
    let keep = RouteKind::from_path(uri) != RouteKind::Other || EXTRA_ROUTES.contains(&path);
    keep.then(|| uri.to_string())
}

/// Aggregates an HAProxy log into the replay TSV (most frequent first). Returns
/// `(lines read, requests kept, distinct paths)`.
pub fn import(haproxy_log: &Path, out: &Path) -> anyhow::Result<(u64, u64, usize)> {
    let f = std::fs::File::open(haproxy_log)
        .with_context(|| format!("opening {}", haproxy_log.display()))?;
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let (mut lines, mut kept) = (0u64, 0u64);
    for line in BufReader::new(f).lines() {
        let line = line?;
        lines += 1;
        if let Some(p) = extract(&line) {
            kept += 1;
            *counts.entry(p).or_default() += 1;
        }
    }
    let mut rows: Vec<(u64, String)> = counts.into_iter().map(|(p, c)| (c, p)).collect();
    rows.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    if let Some(dir) = out.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut w = std::io::BufWriter::new(
        std::fs::File::create(out).with_context(|| format!("creating {}", out.display()))?,
    );
    for (c, p) in &rows {
        writeln!(w, "{c}\tGET\t{p}")?;
    }
    w.flush()?;
    Ok((lines, kept, rows.len()))
}
