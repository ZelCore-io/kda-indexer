//! CLI subcommand implementations.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use kdx_node::{NodeClient, NodeClientConfig};

use crate::config::Config;

pub mod backfill;
pub mod dump;
pub mod frames;
pub mod hot;
pub mod inspect;
pub mod measure;
pub mod notify;
pub mod probe;
pub mod reconcile;
pub mod run;
pub mod search;
pub mod segments;
pub mod serve;
pub mod snapshot;
pub mod unwind;
pub mod verify;
pub mod viewcheck;

/// Build the node client from config; `max_rps` and `node_concurrency` come from `[ingest]`.
pub fn node_client(cfg: &Config) -> anyhow::Result<NodeClient> {
    let p2p_cert_sha256 = cfg
        .node
        .p2p_cert_sha256
        .as_deref()
        .map(kdx_node::parse_sha256_hex)
        .transpose()
        .context("node.p2p_cert_sha256")?;
    let ncfg = NodeClientConfig {
        base: cfg.node.url.clone(),
        network: cfg.network.clone(),
        timeout: Duration::from_millis(cfg.node.timeout_ms),
        max_rps: cfg.ingest.max_rps,
        concurrency: cfg.ingest.node_concurrency,
        p2p_url: cfg.node.p2p_url.clone(),
        p2p_cert_sha256,
        max_body_bytes: cfg.node.max_body_bytes,
        max_sse_event_bytes: cfg.node.max_sse_event_bytes,
    };
    Ok(NodeClient::new(&ncfg)?)
}

/// The node client of the balance reconciler: `[balances] local_rps` and
/// `local_concurrency` instead of the `[ingest]` budget (B-1).
pub fn local_node_client(cfg: &Config) -> anyhow::Result<NodeClient> {
    let mut c = cfg.clone();
    c.ingest.max_rps = cfg.balances.local_rps;
    c.ingest.node_concurrency = cfg.balances.local_concurrency.max(1) as usize;
    node_client(&c)
}

/// Take the exclusive lock on `<data_dir>/LOCK` for a writing command (`run`, `tail`,
/// `backfill`, `unwind`). Held until the returned file is dropped; the OS releases it when
/// the process exits. Read-only commands do not take it.
pub fn lock_data_dir(data_dir: &Path) -> anyhow::Result<File> {
    std::fs::create_dir_all(data_dir).with_context(|| format!("create {}", data_dir.display()))?;
    let p = data_dir.join("LOCK");
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&p)
        .with_context(|| format!("open {}", p.display()))?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(TryLockError::WouldBlock) => anyhow::bail!(
            "{} is in use by another kda-indexer process (run, tail, backfill or unwind); stop it first",
            data_dir.display()
        ),
        Err(TryLockError::Error(e)) => Err(e).with_context(|| format!("lock {}", p.display())),
    }
}
