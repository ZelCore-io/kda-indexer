use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

pub type Result<T> = std::result::Result<T, NodeError>;

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("http {status}: {body_prefix}")]
    Http { status: u16, body_prefix: String },
    #[error("request timed out")]
    Timeout,
    #[error("transport error: {0}")]
    Transport(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("payload {hash} missing from node response")]
    Missing { hash: String },
    #[error("client config error: {0}")]
    Config(String),
    #[error("response body exceeds {limit} bytes")]
    TooLarge { limit: usize },
}

/// `GET /info`.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// `nodePackageVersion`, e.g. "3.2".
    pub node_version: String,
    /// `nodeVersion`, the chainweb network id, e.g. "mainnet01".
    pub chainweb_version: String,
    /// Ascending by start height: (first height of the graph, chain ids ascending).
    pub graph_history: Vec<(u64, Vec<u8>)>,
    pub raw: Value,
}

#[derive(Debug, Clone)]
pub struct CutEntry {
    pub height: u64,
    pub hash: String,
}

/// `GET /cut`.
#[derive(Debug, Clone)]
pub struct Cut {
    pub hashes: BTreeMap<u8, CutEntry>,
    pub height: u64,
    pub weight: String,
}

/// Paged response (`limit`, `items`, optional `next` token).
#[derive(Debug, Clone)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub limit: u32,
    pub next: Option<String>,
}

/// One row of `NodeClient::stats()`.
#[derive(Debug, Clone)]
pub struct StatRow {
    pub endpoint: String,
    /// HTTP status, 0 for transport errors and timeouts.
    pub status: u16,
    pub count: u64,
    pub total_secs: f64,
}

/// `[node]` config section (spec section 6.5), shared by kda-indexer and kdx-compare.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub url: Url,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p2p_url: Option<Url>,
    /// Hex SHA-256 of the node's self-signed leaf certificate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p2p_cert_sha256: Option<String>,
    #[serde(default = "d_timeout_ms")]
    pub timeout_ms: u64,
    /// Largest accepted response body after decompression; a bigger one fails the request.
    #[serde(default = "d_max_body_bytes")]
    pub max_body_bytes: usize,
    /// Largest accepted server-sent event (the SSE buffer cap); a bigger one ends the stream.
    #[serde(default = "d_max_sse_event_bytes")]
    pub max_sse_event_bytes: usize,
}

fn d_timeout_ms() -> u64 {
    30_000
}

pub const DEFAULT_MAX_BODY_BYTES: usize = 64 << 20;
pub const DEFAULT_MAX_SSE_EVENT_BYTES: usize = 1 << 20;

fn d_max_body_bytes() -> usize {
    DEFAULT_MAX_BODY_BYTES
}

fn d_max_sse_event_bytes() -> usize {
    DEFAULT_MAX_SSE_EVENT_BYTES
}
