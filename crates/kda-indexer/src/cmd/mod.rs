//! CLI subcommand implementations.

use std::time::Duration;

use anyhow::Context;
use kdx_node::{NodeClient, NodeClientConfig};

use crate::config::Config;

pub mod probe;

/// Build the node client from config; `max_rps` and `node_concurrency` come from `[ingest]`.
pub fn node_client(cfg: &Config) -> anyhow::Result<NodeClient> {
    let p2p_cert_sha256 = cfg.node.p2p_cert_sha256.as_deref().map(kdx_node::parse_sha256_hex).transpose().context("node.p2p_cert_sha256")?;
    let ncfg = NodeClientConfig {
        base: cfg.node.url.clone(),
        network: cfg.network.clone(),
        timeout: Duration::from_millis(cfg.node.timeout_ms),
        max_rps: cfg.ingest.max_rps,
        concurrency: cfg.ingest.node_concurrency,
        p2p_url: cfg.node.p2p_url.clone(),
        p2p_cert_sha256,
    };
    Ok(NodeClient::new(&ncfg)?)
}
