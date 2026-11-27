//! Shared ingest context: node client, hot store, dictionary, segment and ETL dirs, graph.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use kdx_hot::env::meta;
use kdx_hot::{Geometry, HotDict, HotStore, Txn};
use kdx_node::{ChainGraph, NodeClient};

/// The values of `Config` that ingest needs (kdx-ingest cannot depend on the binary's
/// `Config`; `kda_indexer::cmd::backfill` fills this from it).
#[derive(Debug, Clone)]
pub struct IngestOpts {
    pub network: String,
    pub data_dir: PathBuf,
    pub geometry: Geometry,
    pub finality_depth: u64,
    pub batch_size: u32,
    pub ranges_in_flight: u32,
    pub range_heights: u64,
    pub step_heights: u64,
    pub min_free_gb: u64,
    /// zstd level for ranges and for (tail-end) steps.
    pub range_level: i32,
    pub step_level: i32,
    pub dict_samples: u32,
    /// Buffered bytes of the raw spool per range before a sorted run is spilled.
    pub etl_mem_mb: u64,
    /// Cap for (ranges fetching + the range being built) × spool buffer + writer estimate.
    pub mem_cap_mb: u64,
    /// Tail writer: commit after this many ms or this many blocks, whichever comes first.
    pub commit_ms: u64,
    pub commit_blocks: u32,
    /// `/cut` poll interval of the tail (catches gaps and SSE loss).
    pub cut_poll_secs: u64,
    /// Dev: followers stop at this height (bounded tail runs and replay checks).
    pub until: Option<u32>,
    /// Test fixtures only: write segments the way a build before FIX-2 did (no `sndr.pst`,
    /// no `kf.fil`), the input of `segments add-index` tests. Always false in production.
    pub legacy_segments: bool,
}

impl IngestOpts {
    /// The writer options of every range/step this process writes at zstd `level`.
    pub fn write_opts(&self, level: i32) -> kdx_segment::WriteOpts {
        let mut w = kdx_segment::WriteOpts::new(level, self.dict_samples);
        w.optional_files = !self.legacy_segments;
        w
    }
}

pub struct Ctx {
    pub opts: IngestOpts,
    pub node: NodeClient,
    /// One MDBX environment per process: shared with the API and every other `Ctx`.
    pub hot: Arc<HotStore>,
    pub dict: Arc<Mutex<HotDict>>,
    pub segdir: PathBuf,
    pub etl: PathBuf,
    pub graph: ChainGraph,
}

/// `<data_dir>/segments`, `<data_dir>/hot`, `<data_dir>/etl` (spec §6.6).
pub fn segments_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("segments")
}
pub fn hot_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("hot")
}
pub fn etl_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("etl")
}

impl Ctx {
    /// Open the stores and check the node's network and chain graph against `Meta`
    /// (written on first open, refused on mismatch).
    pub async fn open(opts: IngestOpts, node: NodeClient) -> Result<Ctx> {
        let hot = Arc::new(HotStore::open(&hot_dir(&opts.data_dir), opts.geometry)?);
        Ctx::open_with(opts, node, hot).await
    }

    /// Like [`Ctx::open`] over an already opened hot store (`run` opens it once and shares it
    /// between backfill, tail and API: opening one MDBX env twice in a process is not allowed).
    pub async fn open_with(opts: IngestOpts, node: NodeClient, hot: Arc<HotStore>) -> Result<Ctx> {
        let info = node.info().await.context("node /info")?;
        if info.chainweb_version != opts.network {
            bail!(
                "node network {:?} != config network {:?}",
                info.chainweb_version,
                opts.network
            );
        }
        let graph = ChainGraph::from_info(&info);
        let mut hist = info.graph_history.clone();
        hist.sort_by_key(|(h, _)| *h);
        let graph_json = serde_json::to_vec(&hist)?;

        let segdir = segments_dir(&opts.data_dir);
        let etl = etl_dir(&opts.data_dir);
        std::fs::create_dir_all(&segdir)?;
        std::fs::create_dir_all(&etl)?;
        {
            let mut txn = hot.rw()?;
            match txn.meta(meta::NETWORK)? {
                None => txn.set_meta(meta::NETWORK, opts.network.as_bytes())?,
                Some(n) if n == opts.network.as_bytes() => {}
                Some(n) => bail!(
                    "hot store holds network {:?}, config says {:?}",
                    String::from_utf8_lossy(&n),
                    opts.network
                ),
            }
            match txn.meta(meta::CHAIN_GRAPH)? {
                None => txn.set_meta(meta::CHAIN_GRAPH, &graph_json)?,
                Some(g) if g == graph_json => {}
                Some(g) => bail!(
                    "hot store chain graph {} differs from the node's {}",
                    String::from_utf8_lossy(&g),
                    String::from_utf8_lossy(&graph_json)
                ),
            }
            txn.commit()?;
        }
        let dict = HotDict::load(&hot.ro()?)?;
        Ok(Ctx {
            opts,
            node,
            hot,
            dict: Arc::new(Mutex::new(dict)),
            segdir,
            etl,
            graph,
        })
    }

    /// Replace the in-memory dictionary with the persisted one. Called after a write txn that
    /// may have run `HotDict::persist` failed: the names it drained from `pending` never
    /// reached MDBX, so ids must not be handed out from memory (review M3). A failed reload is
    /// an error the caller returns (F-52): going on with the drained in-memory dictionary
    /// would hand out ids that are not in `DictRev`.
    pub fn reload_dict(&self, dict: &mut HotDict) -> Result<()> {
        reload_dict(&self.hot, dict)
    }
}

/// A failed reload of the dictionary after a failed write txn (F-52): the in-memory
/// dictionary may hand out ids that are not in `DictRev`, so the process must stop.
#[derive(Debug)]
pub struct DictReloadFailed(anyhow::Error);

impl std::fmt::Display for DictReloadFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("reloading the dictionary after a failed write txn")
    }
}

impl std::error::Error for DictReloadFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Replace `dict` with the one persisted in `hot` (see [`Ctx::reload_dict`]). A failure is a
/// [`DictReloadFailed`]: the caller returns it, and a background writer that would go on
/// stops the process ([`stop_on_reload_failure`]).
pub fn reload_dict(hot: &HotStore, dict: &mut HotDict) -> Result<()> {
    *dict = hot
        .ro()
        .and_then(|t| HotDict::load(&t))
        .map_err(|e| anyhow::Error::new(DictReloadFailed(e)))?;
    Ok(())
}

/// Whether `e` carries a [`DictReloadFailed`].
pub fn is_reload_failure(e: &anyhow::Error) -> bool {
    e.chain().any(|c| c.is::<DictReloadFailed>())
}

/// The process's stop token, registered by the command that runs the background writers.
static PROCESS_STOP: std::sync::OnceLock<tokio_util::sync::CancellationToken> =
    std::sync::OnceLock::new();

/// Register the process's stop token: a background writer whose dictionary reload failed
/// cancels it ([`stop_on_reload_failure`]). The first registration stays.
pub fn set_process_stop(token: tokio_util::sync::CancellationToken) {
    let _ = PROCESS_STOP.set(token);
}

/// For a background writer that logs its errors and goes on: when `e` is a failed reload
/// (F-52), log it at ERROR and stop the process (the registered token, else `fallback`), and
/// answer `true`, so the writer stops too.
pub fn stop_on_reload_failure(
    e: &anyhow::Error,
    fallback: &tokio_util::sync::CancellationToken,
) -> bool {
    if !is_reload_failure(e) {
        return false;
    }
    tracing::error!(
        error = format!("{e:#}"),
        "dictionary reload failed after a failed write txn; stopping the process (F-52)"
    );
    PROCESS_STOP.get().unwrap_or(fallback).cancel();
    fallback.cancel();
    true
}

#[cfg(test)]
mod reload_tests {
    use super::*;

    /// A failed reload stops the process token; any other error leaves it running.
    #[test]
    fn a_failed_reload_stops_the_process() {
        let stop = tokio_util::sync::CancellationToken::new();
        let other = anyhow::anyhow!("write failed");
        assert!(!stop_on_reload_failure(&other, &stop));
        assert!(!stop.is_cancelled());
        let e =
            anyhow::Error::new(DictReloadFailed(anyhow::anyhow!("no readers"))).context("marks");
        assert!(is_reload_failure(&e));
        assert!(stop_on_reload_failure(&e, &stop));
        assert!(stop.is_cancelled());
    }
}
