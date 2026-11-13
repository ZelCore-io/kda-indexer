//! Request generators (spec section 7.2): replay, random sampling and edge seeds.

pub mod random;
pub mod replay;
pub mod seeds;

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use kdx_node::NodeClient;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde::Serialize;
use serde_json::Value;

use crate::config::Mix;
use crate::normalize::RouteKind;
use crate::target::Target;

/// `OracleRef::Poll.chain` value meaning "take the chain from A's response" (replay requests
/// carry a request key but no chain).
pub const CHAIN_FROM_A: u8 = u8::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Origin {
    Replay,
    Random,
    Seed,
}

impl Origin {
    pub fn as_str(self) -> &'static str {
        match self {
            Origin::Replay => "replay",
            Origin::Random => "random",
            Origin::Seed => "seed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum OracleRef {
    Poll {
        chain: u8,
        request_key: String,
    },
    Header {
        chain: u8,
        height: u64,
        hash: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct Request {
    pub route: RouteKind,
    pub path_and_query: String,
    pub origin: Origin,
    pub oracle: Option<OracleRef>,
}

impl Request {
    pub fn new(path_and_query: String, origin: Origin, oracle: Option<OracleRef>) -> Request {
        Request {
            route: RouteKind::from_path(&path_and_query),
            path_and_query,
            origin,
            oracle,
        }
    }
}

pub trait Generator {
    fn next(&mut self) -> impl Future<Output = anyhow::Result<Vec<Request>>> + Send;
}

/// Tip heights both sides have, refreshed by the caller (every 60 s in `run`).
#[derive(Debug, Default)]
pub struct Tips {
    node_min: AtomicU64,
    a_tip: AtomicU64,
    cut: Mutex<BTreeMap<u8, (u64, String)>>,
}

impl Tips {
    /// Reads the node `/cut` and A's `/txs/recent` (max height seen there).
    pub async fn refresh(&self, node: &NodeClient, a: Option<&Target>) -> anyhow::Result<()> {
        let cut = node.cut().await?;
        let min = cut.hashes.values().map(|e| e.height).min().unwrap_or(0);
        self.node_min.store(min, Ordering::Relaxed);
        {
            let mut c = self.cut.lock().expect("tips poisoned");
            c.clear();
            for (ch, e) in &cut.hashes {
                c.insert(*ch, (e.height, e.hash.clone()));
            }
        }
        if let Some(t) = a {
            match t.get("/txs/recent").await {
                Ok(cap) if cap.status == 200 => {
                    let max = cap
                        .json()
                        .and_then(|v| {
                            v.as_array()?
                                .iter()
                                .filter_map(|x| x.get("height")?.as_u64())
                                .max()
                        })
                        .unwrap_or(0);
                    if max > 0 {
                        self.a_tip.store(max, Ordering::Relaxed);
                    }
                }
                Ok(cap) => tracing::warn!(status = cap.status, "A /txs/recent for tip"),
                Err(e) => tracing::warn!(error = %format!("{e:#}"), "A /txs/recent for tip"),
            }
        }
        Ok(())
    }

    /// `min(tip of A, lowest node chain tip)`; the node tip alone when A's is unknown.
    pub fn min_tip(&self) -> u64 {
        let n = self.node_min.load(Ordering::Relaxed);
        match self.a_tip.load(Ordering::Relaxed) {
            0 => n,
            a => a.min(n),
        }
    }

    pub fn a_tip(&self) -> u64 {
        self.a_tip.load(Ordering::Relaxed)
    }

    /// The chain's cut hash (canonical branch tip).
    pub fn cut_hash(&self, chain: u8) -> Option<String> {
        self.cut
            .lock()
            .expect("tips poisoned")
            .get(&chain)
            .map(|(_, h)| h.clone())
    }

    /// The chain's height in the cut.
    pub fn cut_height(&self, chain: u8) -> Option<u64> {
        self.cut
            .lock()
            .expect("tips poisoned")
            .get(&chain)
            .map(|(h, _)| *h)
    }
}

pub fn rng() -> StdRng {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(7);
    StdRng::seed_from_u64(seed)
}

/// Percent-encodes one path segment (keeps unreserved characters and `:`).
pub fn enc_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~:".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Percent-encodes a query value.
pub fn enc_query(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

/// Draws from replay, random and seeds by `mix` weights; a missing source's weight is
/// redistributed over the others.
pub struct Mixer {
    pub random: Option<random::RandomGen>,
    pub seeds: Option<seeds::SeedGen>,
    pub replay: Option<replay::ReplayGen>,
    pub mix: Mix,
    rng: StdRng,
}

impl Mixer {
    pub fn new(
        random: Option<random::RandomGen>,
        seeds: Option<seeds::SeedGen>,
        replay: Option<replay::ReplayGen>,
        mix: Mix,
    ) -> Mixer {
        Mixer {
            random,
            seeds,
            replay,
            mix,
            rng: rng(),
        }
    }

    /// Empty blocks the random and seed generators skipped so far.
    pub fn empty_blocks(&self) -> u64 {
        self.random.as_ref().map_or(0, |r| r.empty_blocks)
            + self.seeds.as_ref().map_or(0, |s| s.empty_blocks)
    }
}

impl Generator for Mixer {
    async fn next(&mut self) -> anyhow::Result<Vec<Request>> {
        let w = [
            if self.replay.is_some() {
                self.mix.replay
            } else {
                0.0
            },
            if self.random.is_some() {
                self.mix.random
            } else {
                0.0
            },
            if self.seeds.is_some() {
                self.mix.seeds
            } else {
                0.0
            },
        ];
        let total: f64 = w.iter().sum();
        anyhow::ensure!(
            total > 0.0,
            "no request source available (replay, random, seeds)"
        );
        let mut x = self.rng.random_range(0.0..total);
        let mut pick = 0;
        for (i, wi) in w.iter().enumerate() {
            if *wi > 0.0 {
                pick = i;
                if x < *wi {
                    break;
                }
                x -= wi;
            }
        }
        match pick {
            0 => self.replay.as_mut().expect("replay").next().await,
            1 => self.random.as_mut().expect("random").next().await,
            _ => self.seeds.as_mut().expect("seeds").next().await,
        }
    }
}

/// A request key as it appears on the wire: 43 unpadded base64url characters.
pub fn is_request_key(s: &str) -> bool {
    s.len() == 43
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Path-and-query safe to replay: starts with `/`, no whitespace or control characters.
pub fn is_clean_path(p: &str) -> bool {
    p.starts_with('/') && !p.chars().any(|c| c.is_whitespace() || c.is_control())
}

/// `meta.sender` of a parsed cmd.
pub fn cmd_sender(cmd: &Value) -> Option<&str> {
    cmd.get("meta")?.get("sender")?.as_str()
}
