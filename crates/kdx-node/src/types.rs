use std::collections::BTreeMap;

use serde_json::Value;

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
