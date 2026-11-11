//! Fixed edge seeds (spec section 7.2 item 3), cycled in file order.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use kdx_node::NodeClient;
use rand::rngs::StdRng;
use serde::Deserialize;

use super::random::{block_requests, v1txs_request, BlockDraw};
use super::{enc_query, enc_segment, rng, Generator, OracleRef, Origin, Request, Tips};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeedKind {
    Height,
    RequestKey,
    Account,
    PactId,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum SeedValue {
    Int(u64),
    Text(String),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Seed {
    pub kind: SeedKind,
    #[serde(default)]
    pub chain: Option<u8>,
    pub value: SeedValue,
    /// Account seeds: one non-`coin` token module to ask about besides `coin`.
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub note: String,
}

impl Seed {
    /// Placeholders (`value` starting with `TODO`) are skipped.
    pub fn is_placeholder(&self) -> bool {
        matches!(&self.value, SeedValue::Text(s) if s.starts_with("TODO"))
    }

    fn text(&self) -> String {
        match &self.value {
            SeedValue::Int(n) => n.to_string(),
            SeedValue::Text(s) => s.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    #[serde(default, rename = "seed")]
    seeds: Vec<Seed>,
}

pub struct SeedGen {
    pub seeds: Vec<Seed>,
    pos: usize,
    node: Option<(NodeClient, Arc<Tips>)>,
    rng: StdRng,
    pub empty_blocks: u64,
}

impl SeedGen {
    pub fn load(path: &Path) -> anyhow::Result<SeedGen> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let f: File =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        let seeds: Vec<Seed> = f
            .seeds
            .into_iter()
            .filter(|s| !s.is_placeholder())
            .collect();
        Ok(SeedGen {
            seeds,
            pos: 0,
            node: None,
            rng: rng(),
            empty_blocks: 0,
        })
    }

    /// Height seeds need the node; without it they are skipped.
    pub fn with_node(mut self, node: NodeClient, tips: Arc<Tips>) -> SeedGen {
        self.node = Some((node, tips));
        self
    }

    async fn requests(&mut self, s: &Seed) -> anyhow::Result<Vec<Request>> {
        let o = Origin::Seed;
        Ok(match s.kind {
            SeedKind::Height => {
                let (Some((node, tips)), SeedValue::Int(h), Some(c)) =
                    (&self.node, &s.value, s.chain)
                else {
                    return Ok(Vec::new());
                };
                match block_requests(node, tips, c, *h, o, &mut self.rng).await? {
                    BlockDraw::Empty => {
                        self.empty_blocks += 1;
                        Vec::new()
                    }
                    BlockDraw::Requests(r) => r,
                }
            }
            SeedKind::RequestKey => {
                let rk = s.text();
                if !super::is_request_key(&rk) {
                    tracing::warn!(%rk, "seed request key is not 43-char base64url; skipped");
                    return Ok(Vec::new());
                }
                let oracle = Some(OracleRef::Poll {
                    chain: s.chain.unwrap_or(super::CHAIN_FROM_A),
                    request_key: rk.clone(),
                });
                vec![
                    Request::new(format!("/txs/tx?requestkey={rk}"), o, oracle.clone()),
                    Request::new(format!("/txs/txs?requestkey={rk}"), o, oracle),
                ]
            }
            SeedKind::Account => {
                let mut q = "limit=10".to_string();
                if let Some(c) = s.chain {
                    q.push_str(&format!("&chain={c}"));
                }
                let acct = s.text();
                let mut reqs = vec![
                    Request::new(format!("/txs/account/{}?{q}", enc_segment(&acct)), o, None),
                    v1txs_request(&acct, "coin", 50, o),
                ];
                if let Some(t) = &s.token {
                    reqs.push(Request::new(
                        format!(
                            "/txs/account/{}?{q}&token={}",
                            enc_segment(&acct),
                            enc_query(t)
                        ),
                        o,
                        None,
                    ));
                    reqs.push(v1txs_request(&acct, t, 50, o));
                }
                reqs
            }
            SeedKind::PactId => vec![Request::new(
                format!("/txs/search?pactid={}", enc_query(&s.text())),
                o,
                None,
            )],
        })
    }
}

impl Generator for SeedGen {
    async fn next(&mut self) -> anyhow::Result<Vec<Request>> {
        // One pass at most: skip seeds that yield nothing (empty blocks, no node).
        for _ in 0..self.seeds.len() {
            let s = self.seeds[self.pos].clone();
            self.pos = (self.pos + 1) % self.seeds.len();
            let r = self.requests(&s).await?;
            if !r.is_empty() {
                return Ok(r);
            }
        }
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod p5_seed_tests {
    use super::*;

    /// The shipped seed file holds the P5 seeds: five WIZ_TRANSFER accounts asked with
    /// `token=free.wiz-arena` and five cross-chain pact ids.
    #[test]
    fn mainnet_seed_file_has_the_p5_seeds() {
        let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/seeds/mainnet01.toml");
        let g = SeedGen::load(&p).unwrap();
        let wiz = g
            .seeds
            .iter()
            .filter(|s| s.kind == SeedKind::Account && s.token.as_deref() == Some("free.wiz-arena"))
            .count();
        let pacts = g
            .seeds
            .iter()
            .filter(|s| s.kind == SeedKind::PactId)
            .count();
        assert_eq!(wiz, 5);
        assert_eq!(pacts, 5);
    }
}
