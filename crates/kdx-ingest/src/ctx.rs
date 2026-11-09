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
}

pub struct Ctx {
    pub opts: IngestOpts,
    pub node: NodeClient,
    pub hot: HotStore,
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
        let hot = HotStore::open(&hot_dir(&opts.data_dir), opts.geometry)?;
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
    /// reached MDBX, so ids must not be handed out from memory (review M3).
    pub fn reload_dict(&self, dict: &mut HotDict) {
        match self.hot.ro().and_then(|t| HotDict::load(&t)) {
            Ok(d) => *dict = d,
            Err(e) => tracing::error!(error = %e, "reloading the dictionary failed"),
        }
    }
}
