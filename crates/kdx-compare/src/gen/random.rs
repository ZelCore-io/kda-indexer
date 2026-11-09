//! Random sampling (spec section 7.2 item 2): a random canonical block below
//! `min(tip) - min_depth`, one random user tx in it, and the requests derived from that tx.

use std::sync::Arc;

use anyhow::Context;
use kdx_decode::build::{parse_event, transfer_accounts};
use kdx_decode::DecodedBlock;
use kdx_node::{ChainGraph, NodeClient};
use rand::rngs::StdRng;
use rand::RngExt;
use serde_json::Value;

use super::{cmd_sender, enc_query, enc_segment, rng, Generator, OracleRef, Origin, Request, Tips};

/// Windows per `next()` before giving up (most blocks are empty).
const MAX_DRAWS: u32 = 30;
/// Heights per draw: one `header/branch` and one payload batch for the whole window, then one
/// non-empty block of the window is picked uniformly.
const WINDOW: u64 = 20;
/// At most this many accounts and event kinds per sampled tx.
const MAX_ACCOUNTS: usize = 3;
const MAX_EVENTS: usize = 3;
/// `/txs/events` requests are bounded to `height +- EVENTS_WINDOW` so chainweb-data does not
/// scan its whole events table.
const EVENTS_WINDOW: u64 = 100;
const ACCOUNT_WINDOW: u64 = 1000;

pub struct RandomGen {
    node: NodeClient,
    graph: ChainGraph,
    tips: Arc<Tips>,
    min_depth: u64,
    rng: StdRng,
    pub empty_blocks: u64,
    pub drawn_blocks: u64,
}

pub enum BlockDraw {
    Empty,
    Requests(Vec<Request>),
}

/// The canonical header at `(chain, height)`: `header/branch` with the chain's cut hash as
/// `upper` (the plain header range also returns orphans).
pub async fn canonical_header(
    node: &NodeClient,
    tips: &Tips,
    chain: u8,
    height: u64,
) -> anyhow::Result<Option<Value>> {
    let upper = tips
        .cut_hash(chain)
        .with_context(|| format!("no cut hash for chain {chain}"))?;
    let page = node
        .header_branch(chain, &[], &[upper], Some(height), Some(height))
        .await?;
    Ok(page
        .items
        .into_iter()
        .find(|h| h.get("height").and_then(Value::as_u64) == Some(height)))
}

/// Canonical header plus decoded payload at `(chain, height)`.
pub async fn fetch_block(
    node: &NodeClient,
    tips: &Tips,
    chain: u8,
    height: u64,
) -> anyhow::Result<Option<(Value, DecodedBlock)>> {
    let Some(header) = canonical_header(node, tips, chain, height).await? else {
        return Ok(None);
    };
    let ph = header
        .get("payloadHash")
        .and_then(Value::as_str)
        .context("header without payloadHash")?
        .to_string();
    let payload = node.payload_with_outputs(chain, &ph).await?;
    let b = kdx_decode::decode_block(&header, &payload)
        .map_err(|e| anyhow::anyhow!("decode {chain}/{height}: {e}"))?;
    Ok(Some((header, b)))
}

/// Requests derived from one random user tx of the block at `(chain, height)`.
pub async fn block_requests(
    node: &NodeClient,
    tips: &Tips,
    chain: u8,
    height: u64,
    origin: Origin,
    rng: &mut StdRng,
) -> anyhow::Result<BlockDraw> {
    let Some((header, b)) = fetch_block(node, tips, chain, height).await? else {
        return Ok(BlockDraw::Empty);
    };
    if b.txs.is_empty() {
        return Ok(BlockDraw::Empty);
    }
    let tx = &b.txs[rng.random_range(0..b.txs.len())];
    let hash = header
        .get("hash")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Ok(BlockDraw::Requests(tx_requests(
        tx, chain, height, &hash, origin, rng,
    )))
}

pub fn tx_requests(
    tx: &kdx_decode::DecodedTx,
    chain: u8,
    height: u64,
    block_hash: &str,
    origin: Origin,
    rng: &mut StdRng,
) -> Vec<Request> {
    let rk = tx.hash.to_b64url();
    let mut out = vec![
        Request::new(
            format!("/txs/tx?requestkey={rk}"),
            origin,
            Some(OracleRef::Poll {
                chain,
                request_key: rk.clone(),
            }),
        ),
        Request::new(
            format!("/txs/txs?requestkey={rk}"),
            origin,
            Some(OracleRef::Header {
                chain,
                height,
                hash: block_hash.to_string(),
            }),
        ),
    ];
    // Accounts: sender, then fungible TRANSFER from/to (token = the event's module).
    let mut accounts: Vec<(String, String)> = Vec::new();
    if let Some(s) = cmd_sender(&tx.cmd_parsed) {
        accounts.push((s.to_string(), "coin".to_string()));
    }
    let mut events: Vec<(String, String)> = Vec::new();
    if let Some(evs) = tx.output.get("events").and_then(Value::as_array) {
        for e in evs {
            let Some(p) = parse_event(e) else { continue };
            if let Some((from, to)) = transfer_accounts(&p) {
                for a in [from, to] {
                    accounts.push((a.to_string(), p.module.clone()));
                }
            }
            events.push((p.module.clone(), p.name.to_string()));
        }
    }
    let mut seen = std::collections::HashSet::new();
    accounts.retain(|(a, t)| !a.is_empty() && seen.insert((a.clone(), t.clone())));
    for (acct, token) in accounts.into_iter().take(MAX_ACCOUNTS) {
        let mut q = Vec::new();
        if token != "coin" || rng.random_bool(0.5) {
            q.push(format!("token={}", enc_query(&token)));
        }
        if rng.random_bool(0.5) {
            q.push(format!("chain={chain}"));
        }
        q.push(format!("limit={}", [10, 50, 250][rng.random_range(0..3)]));
        if rng.random_bool(0.5) {
            q.push(format!(
                "minheight={}&maxheight={}",
                height.saturating_sub(ACCOUNT_WINDOW),
                height + ACCOUNT_WINDOW
            ));
        }
        out.push(Request::new(
            format!("/txs/account/{}?{}", enc_segment(&acct), q.join("&")),
            origin,
            None,
        ));
    }
    let mut seen = std::collections::HashSet::new();
    events.retain(|e| seen.insert(e.clone()));
    for (module, name) in events.into_iter().take(MAX_EVENTS) {
        out.push(Request::new(
            format!(
                "/txs/events?modulename={}&name={}&minheight={}&maxheight={}",
                enc_query(&module),
                enc_query(&name),
                height.saturating_sub(EVENTS_WINDOW),
                height + EVENTS_WINDOW
            ),
            origin,
            None,
        ));
    }
    // Defpact id: a `cont` tx's pactId, or the continuation this tx started.
    let pact_id = tx
        .cmd_parsed
        .pointer("/payload/cont/pactId")
        .or_else(|| tx.output.pointer("/continuation/pactId"))
        .and_then(Value::as_str);
    if let Some(p) = pact_id {
        out.push(Request::new(
            format!("/txs/search?pactid={}", enc_query(p)),
            origin,
            None,
        ));
    }
    out
}

impl RandomGen {
    pub fn new(node: NodeClient, graph: ChainGraph, tips: Arc<Tips>, min_depth: u64) -> RandomGen {
        RandomGen {
            node,
            graph,
            tips,
            min_depth,
            rng: rng(),
            empty_blocks: 0,
            drawn_blocks: 0,
        }
    }
}

impl RandomGen {
    /// One window of canonical blocks on one chain; returns the non-empty ones.
    async fn draw_window(
        &mut self,
        chain: u8,
        lo: u64,
        hi: u64,
    ) -> anyhow::Result<Vec<(Value, DecodedBlock)>> {
        let upper = self
            .tips
            .cut_hash(chain)
            .with_context(|| format!("no cut hash for chain {chain}"))?;
        let page = self
            .node
            .header_branch(chain, &[], &[upper], Some(lo), Some(hi))
            .await?;
        let headers = page.items;
        let hashes: Vec<String> = headers
            .iter()
            .filter_map(|h| h.get("payloadHash")?.as_str().map(str::to_string))
            .collect();
        let payloads = self.node.payloads_with_outputs(chain, &hashes).await?;
        let mut out = Vec::new();
        for h in headers {
            let Some(p) = h
                .get("payloadHash")
                .and_then(Value::as_str)
                .and_then(|ph| payloads.get(ph))
            else {
                continue;
            };
            let b = kdx_decode::decode_block(&h, p)
                .map_err(|e| anyhow::anyhow!("decode chain {chain}: {e}"))?;
            self.drawn_blocks += 1;
            if b.txs.is_empty() {
                self.empty_blocks += 1;
            } else {
                out.push((h, b));
            }
        }
        Ok(out)
    }
}

impl Generator for RandomGen {
    async fn next(&mut self) -> anyhow::Result<Vec<Request>> {
        let top = self.tips.min_tip().saturating_sub(self.min_depth);
        anyhow::ensure!(top > WINDOW, "tips not known yet");
        for _ in 0..MAX_DRAWS {
            let h = self.rng.random_range(0..=top - WINDOW + 1);
            let chains = self.graph.chains_at(h);
            if chains.is_empty() {
                continue;
            }
            let c = chains[self.rng.random_range(0..chains.len())];
            // Stay inside one chain graph (chains 10-19 start at the graph change).
            let hi = (h + WINDOW - 1).min(top);
            let full = self.draw_window(c, h, hi).await?;
            if full.is_empty() {
                continue;
            }
            let (header, b) = &full[self.rng.random_range(0..full.len())];
            let tx = &b.txs[self.rng.random_range(0..b.txs.len())];
            let hash = header
                .get("hash")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            return Ok(tx_requests(
                tx,
                c,
                b.header.height,
                &hash,
                Origin::Random,
                &mut self.rng,
            ));
        }
        Ok(Vec::new())
    }
}
