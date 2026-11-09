//! Canonical header walk (P0 ruling: the plain header range also returns orphans).
//!
//! Two ways to name the canonical branch for `min..=max`:
//! - **cut**: `header/branch` with the current cut hash as `upper`. The node walks down from
//!   the cut, so deep history costs a long walk per page.
//! - **anchor**: plain `header?minheight=max+1&maxheight=max+64` (orphans included), take
//!   any header at the highest returned height and follow parent links down to `max+1`;
//!   below `min_tip − F` orphan branches are far shorter than 64, so that header is
//!   canonical. Its hash is the `upper` of a short `header/branch` walk.
//!
//! `Auto` starts with `cut`, times the first walk and switches to `anchor` for good when it
//! took more than [`SLOW_WALK`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use kdx_node::{Cut, NodeClient};
use serde_json::Value;

/// A cut-mode walk slower than this switches `Auto` to anchors.
pub const SLOW_WALK: Duration = Duration::from_secs(5);
/// Heights above `max` read to find the anchor.
pub const ANCHOR_SPAN: u64 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderMode {
    Auto,
    Cut,
    Anchor,
}

impl std::str::FromStr for HeaderMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<HeaderMode> {
        Ok(match s {
            "auto" => HeaderMode::Auto,
            "cut" => HeaderMode::Cut,
            "anchor" => HeaderMode::Anchor,
            _ => bail!("header mode must be auto, cut or anchor"),
        })
    }
}

const M_UNDECIDED: u8 = 0;
const M_CUT: u8 = 1;
const M_ANCHOR: u8 = 2;

/// Mode state shared by all fetch tasks, plus timings for the report.
pub struct Walker {
    mode: AtomicU8,
    auto: bool,
    cut: Cut,
    timing: std::sync::Mutex<WalkTiming>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct WalkTiming {
    pub cut_walks: u64,
    pub cut_secs: f64,
    pub cut_max_secs: f64,
    pub anchor_walks: u64,
    /// Anchor lookup (plain header range + parent walk).
    pub anchor_lookup_secs: f64,
    /// The short `header/branch` walk below the anchor.
    pub anchor_branch_secs: f64,
    pub anchor_max_secs: f64,
    pub switched_to_anchor: bool,
}

impl Walker {
    pub fn new(mode: HeaderMode, cut: Cut) -> Walker {
        let m = match mode {
            HeaderMode::Auto => M_UNDECIDED,
            HeaderMode::Cut => M_CUT,
            HeaderMode::Anchor => M_ANCHOR,
        };
        Walker {
            mode: AtomicU8::new(m),
            auto: mode == HeaderMode::Auto,
            cut,
            timing: Default::default(),
        }
    }

    pub fn mode(&self) -> &'static str {
        match self.mode.load(Ordering::Relaxed) {
            M_ANCHOR => "anchor",
            M_CUT => "cut",
            _ => "auto (undecided)",
        }
    }

    pub fn timing(&self) -> WalkTiming {
        self.timing.lock().unwrap().clone()
    }

    /// Canonical headers of `chain` at `min..=max`, ascending, one per height.
    pub async fn canonical_headers(
        &self,
        node: &NodeClient,
        chain: u8,
        min: u64,
        max: u64,
    ) -> Result<Vec<Value>> {
        if self.mode.load(Ordering::Relaxed) != M_ANCHOR {
            let cut_hash = &self
                .cut
                .hashes
                .get(&chain)
                .with_context(|| format!("chain {chain} not in the cut"))?
                .hash;
            let t = Instant::now();
            let hs = node.header_branch_all(chain, cut_hash, min, max).await?;
            let secs = t.elapsed().as_secs_f64();
            {
                let mut tm = self.timing.lock().unwrap();
                tm.cut_walks += 1;
                tm.cut_secs += secs;
                tm.cut_max_secs = tm.cut_max_secs.max(secs);
            }
            if self.auto && self.mode.load(Ordering::Relaxed) == M_UNDECIDED {
                let slow = secs > SLOW_WALK.as_secs_f64();
                self.mode
                    .store(if slow { M_ANCHOR } else { M_CUT }, Ordering::Relaxed);
                if slow {
                    self.timing.lock().unwrap().switched_to_anchor = true;
                }
                tracing::info!(
                    chain,
                    min,
                    max,
                    secs,
                    mode = if slow { "anchor" } else { "cut" },
                    "header walk: first cut-mode walk timed"
                );
            }
            return Ok(hs);
        }
        let t = Instant::now();
        let upper = anchor(node, chain, max + 1).await?;
        let lookup = t.elapsed().as_secs_f64();
        let t = Instant::now();
        let hs = node.header_branch_all(chain, &upper, min, max).await?;
        let branch = t.elapsed().as_secs_f64();
        let mut tm = self.timing.lock().unwrap();
        tm.anchor_walks += 1;
        tm.anchor_lookup_secs += lookup;
        tm.anchor_branch_secs += branch;
        tm.anchor_max_secs = tm.anchor_max_secs.max(lookup + branch);
        Ok(hs)
    }
}

/// Hash of the canonical header of `chain` at `at`, found by walking parent links down from
/// the highest header in `at..=at+63`.
pub async fn anchor(node: &NodeClient, chain: u8, at: u64) -> Result<String> {
    let hs = node
        .headers_all(chain, at, at + ANCHOR_SPAN - 1)
        .await
        .with_context(|| format!("anchor headers chain {chain} at {at}"))?;
    let mut by_hash: HashMap<&str, (u64, &str)> = HashMap::with_capacity(hs.len());
    let mut top: Option<(u64, &str)> = None;
    for h in &hs {
        let hash = h
            .get("hash")
            .and_then(Value::as_str)
            .context("header hash")?;
        let parent = h
            .get("parent")
            .and_then(Value::as_str)
            .context("header parent")?;
        let height = h
            .get("height")
            .and_then(Value::as_u64)
            .context("header height")?;
        by_hash.insert(hash, (height, parent));
        if top.is_none_or(|(th, _)| height > th) {
            top = Some((height, hash));
        }
    }
    let (mut height, mut hash) =
        top.with_context(|| format!("no headers at {at}.. on chain {chain}"))?;
    if height < at + ANCHOR_SPAN / 2 {
        bail!(
            "anchor chain {chain}: highest header at {height}, need at least {} above {at}",
            ANCHOR_SPAN / 2
        );
    }
    while height > at {
        let (_, parent) = by_hash[hash];
        let (ph, _) = *by_hash
            .get(parent)
            .with_context(|| format!("anchor chain {chain}: parent of height {height} missing"))?;
        if ph + 1 != height {
            bail!("anchor chain {chain}: parent height {ph} below {height}");
        }
        height = ph;
        hash = parent;
    }
    Ok(hash.to_string())
}
