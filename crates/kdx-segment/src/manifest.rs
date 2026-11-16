//! `manifest.json`: the list of committed ranges/steps (spec §3.4). A range's files exist on
//! disk before it is listed here; only listed ranges are read.

use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::files::replace_file;

pub const MANIFEST: &str = "manifest.json";
/// Segment format version (manifest and every `range.toml`). Version 2 adds the `pact`
/// posting family and chainweb-data's transfer rule (`docs/format.md`).
pub const FORMAT_VERSION: u8 = 2;

/// The error for a segments dir (manifest or `range.toml`) in another format version.
pub fn format_refusal(found: u64) -> String {
    format!(
        "store format v{found}, this build reads v{FORMAT_VERSION}: rebuild it \
         (docs/operations.md#format-upgrade)"
    )
}

/// Longest `network` a manifest may hold (chainweb ids are about ten characters).
pub const MAX_NETWORK_LEN: usize = 64;

/// Refuses a `network` that is not a plain name: it is the first part of every segment file
/// name (`files::file_name`), so `/`, `\`, `..`, NUL or a leading `.` would let a hostile
/// `manifest.json` name a file outside `segments/`. Allowed: 1 to [`MAX_NETWORK_LEN`]
/// characters of `[A-Za-z0-9._-]`, not starting with `.`, without `..`.
pub fn check_network(network: &str) -> Result<()> {
    let plain = !network.is_empty()
        && network.len() <= MAX_NETWORK_LEN
        && !network.starts_with('.')
        && !network.contains("..")
        && network
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if !plain {
        let shown: String = network.chars().take(MAX_NETWORK_LEN + 1).collect();
        bail!(
            "manifest: network {shown:?} is not a plain name (1 to {MAX_NETWORK_LEN} of \
             A-Z a-z 0-9 . _ -, no leading '.', no '..')"
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RangeKind {
    /// 10,000-height freeze step (level 9).
    Step,
    /// 100,000-height range (backfill or merge, level 19).
    Range,
}

impl RangeKind {
    pub fn name(self) -> &'static str {
        match self {
            RangeKind::Step => "step",
            RangeKind::Range => "range",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeEntry {
    pub kind: RangeKind,
    /// First height (inclusive).
    pub start: u32,
    /// End height (exclusive).
    pub end: u32,
    pub first_txnum: u64,
    pub tx_count: u64,
    pub block_count: u64,
}

impl RangeEntry {
    pub fn end_txnum(&self) -> u64 {
        // Saturating: a hostile manifest.json must not panic here; `validate` refuses the
        // overflow before any entry is used.
        self.first_txnum.saturating_add(self.tx_count)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u8,
    pub network: String,
    /// Contiguous, ascending by height and TxNum.
    pub entries: Vec<RangeEntry>,
    /// End height (exclusive) of the frozen store = last entry's `end` (0 when empty).
    pub frontier: u32,
    /// TxNum the next step/range starts at.
    pub next_txnum: u64,
}

impl Manifest {
    pub fn new(network: &str) -> Manifest {
        Manifest {
            format_version: FORMAT_VERSION,
            network: network.to_string(),
            entries: Vec::new(),
            frontier: 0,
            next_txnum: 0,
        }
    }

    /// Load `dir/manifest.json`; `Ok(None)` when it does not exist yet.
    pub fn load(dir: &Path) -> Result<Option<Manifest>> {
        let p = dir.join(MANIFEST);
        let b = match std::fs::read(&p) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {}", p.display())),
        };
        Manifest::parse(&b)
            .with_context(|| format!("parse {}", p.display()))
            .map(Some)
    }

    /// Parse and validate `manifest.json` bytes (refuses another `format_version`).
    pub fn parse(b: &[u8]) -> Result<Manifest> {
        let m: Manifest = serde_json::from_slice(b).context("manifest json")?;
        if m.format_version != FORMAT_VERSION {
            bail!(format_refusal(u64::from(m.format_version)));
        }
        m.validate()?;
        Ok(m)
    }

    /// Every file the manifest references (all kinds of every range or step, side files of
    /// the nippy-jar containers included), without the manifest itself.
    pub fn files(&self) -> Vec<std::path::PathBuf> {
        self.entries
            .iter()
            .flat_map(|e| crate::files::range_file_names(&self.network, e.start, e.end))
            .map(std::path::PathBuf::from)
            .collect()
    }

    /// `network` is a plain name ([`check_network`]); entries contiguous in heights and
    /// TxNums; frontier and next_txnum match the last entry.
    pub fn validate(&self) -> Result<()> {
        check_network(&self.network)?;
        let mut h = self.entries.first().map(|e| e.start).unwrap_or(0);
        let mut t = self.entries.first().map(|e| e.first_txnum).unwrap_or(0);
        for e in &self.entries {
            if e.first_txnum.checked_add(e.tx_count).is_none() {
                bail!(
                    "manifest: range {}..{} first_txnum {} + tx_count {} overflows",
                    e.start,
                    e.end,
                    e.first_txnum,
                    e.tx_count
                );
            }
            if e.start != h || e.end <= e.start {
                bail!(
                    "manifest: range {}..{} not contiguous at {h}",
                    e.start,
                    e.end
                );
            }
            if e.first_txnum != t {
                bail!(
                    "manifest: range {}..{} first_txnum {} expected {t}",
                    e.start,
                    e.end,
                    e.first_txnum
                );
            }
            h = e.end;
            t = e.end_txnum();
        }
        if !self.entries.is_empty() && (self.frontier != h || self.next_txnum != t) {
            bail!(
                "manifest: frontier/next_txnum {}/{} but entries end at {h}/{t}",
                self.frontier,
                self.next_txnum
            );
        }
        Ok(())
    }

    /// Append one entry (must continue the last one) and update frontier/next_txnum.
    pub fn push(&mut self, e: RangeEntry) -> Result<()> {
        if let Some(l) = self.entries.last() {
            if e.start != l.end || e.first_txnum != l.end_txnum() {
                bail!(
                    "manifest: {}..{} does not continue {}..{}",
                    e.start,
                    e.end,
                    l.start,
                    l.end
                );
            }
        }
        self.frontier = e.end;
        self.next_txnum = e.end_txnum();
        self.entries.push(e);
        Ok(())
    }

    /// Replace the entries covering exactly `merged.start..merged.end` (a merge of steps) with
    /// `merged`, in memory; the caller then commits.
    pub fn replace_span(&mut self, merged: RangeEntry) -> Result<Vec<RangeEntry>> {
        let i = self
            .entries
            .iter()
            .position(|e| e.start == merged.start)
            .context("merge start not in manifest")?;
        let j = self
            .entries
            .iter()
            .position(|e| e.end == merged.end)
            .context("merge end not in manifest")?;
        let old: Vec<RangeEntry> = self.entries[i..=j].to_vec();
        let txs: u64 = old.iter().map(|e| e.tx_count).sum();
        if old[0].first_txnum != merged.first_txnum || txs != merged.tx_count {
            bail!("merge counts differ from the replaced entries");
        }
        self.entries.splice(i..=j, [merged]);
        self.validate()?;
        Ok(old)
    }

    /// Atomically replace `dir/manifest.json` (temp + fsync + rename + dir fsync).
    pub fn commit(&self, dir: &Path) -> Result<()> {
        self.validate()?;
        let b = serde_json::to_vec_pretty(self)?;
        replace_file(&dir.join(MANIFEST), &b)
    }

    /// Entry holding TxNum `t`.
    pub fn entry_of_txnum(&self, t: u64) -> Option<usize> {
        let i = self.entries.partition_point(|e| e.end_txnum() <= t);
        (i < self.entries.len() && self.entries[i].first_txnum <= t).then_some(i)
    }

    /// Entry holding height `h`.
    pub fn entry_of_height(&self, h: u32) -> Option<usize> {
        let i = self.entries.partition_point(|e| e.end <= h);
        (i < self.entries.len() && self.entries[i].start <= h).then_some(i)
    }
}
