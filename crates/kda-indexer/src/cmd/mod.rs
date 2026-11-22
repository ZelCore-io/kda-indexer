//! CLI subcommand implementations.

use std::fs::{File, TryLockError};
use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use kdx_node::{NodeClient, NodeClientConfig};

use crate::config::Config;

pub mod backfill;
pub mod dump;
pub mod frames;
pub mod graphql;
pub mod hot;
pub mod inspect;
pub mod measure;
pub mod notify;
pub mod probe;
pub mod rebuild;
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
/// `backfill`, `unwind`, `rebuild-range`, `reconcile-balances`, `segments add-index`,
/// `hot drop-family`). Held until the returned file is dropped; the OS releases it when
/// the process exits. Read-only commands do not take it.
pub fn lock_data_dir(data_dir: &Path) -> anyhow::Result<File> {
    std::fs::create_dir_all(data_dir).with_context(|| format!("create {}", data_dir.display()))?;
    let p = data_dir.join("LOCK");
    // F-44: a symlink planted at `LOCK` is not followed (a root-run command would otherwise
    // create or lock its target).
    let f = kdx_segment::files::open_lock_file(&p)?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(TryLockError::WouldBlock) => anyhow::bail!(
            "{} is in use by another kda-indexer process (run, tail, backfill, unwind, \
             rebuild-range, reconcile-balances, segments add-index or hot drop-family); stop it \
             first",
            data_dir.display()
        ),
        Err(TryLockError::Error(e)) => Err(e).with_context(|| format!("lock {}", p.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::lock_data_dir;

    /// F-44: a symlink planted at `<data_dir>/LOCK` is not followed (its target is neither
    /// created nor locked); the command fails instead.
    #[test]
    fn a_symlinked_lock_file_is_not_followed() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let target = tmp.path().join("elsewhere");
        std::os::unix::fs::symlink(&target, data.join("LOCK")).unwrap();
        assert!(lock_data_dir(&data).is_err());
        assert!(!target.exists(), "the symlink target was created");
    }

    #[test]
    fn a_plain_lock_file_is_taken_once() {
        let tmp = tempfile::tempdir().unwrap();
        let l = lock_data_dir(tmp.path()).unwrap();
        assert!(lock_data_dir(tmp.path()).is_err());
        drop(l);
        assert!(lock_data_dir(tmp.path()).is_ok());
    }
}
