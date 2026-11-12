//! Shared harness of the stateful equivalence tests (Task 14). Also included by
//! `crates/kda-indexer/tests/unwind_cli.rs` through `#[path]`.
//!
//! The pool is 24 consecutive mainnet blocks of chain 0 (heights 3,125,000..3,125,023, 0-21
//! user txs each), captured as served by a node. Tests place pool blocks at synthetic heights
//! on a single-chain graph: the header's `height`, `hash` and `parent` are rewritten, the
//! payload is kept, so every block still passes the payload Merkle check (the header hash is
//! never recomputed, ruling R-6). Block `i` of the pool is placed at most once per branch.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kdx_decode::{DecodedBlock, VerifyReport};
use kdx_hot::{Geometry, HotDict, HotStore, Table, Txn};
use kdx_ingest::apply::{self, Batch, Fork};
use kdx_ingest::{pipeline, Ctx, IngestOpts};
use kdx_node::{ChainGraph, NodeClient, NodeClientConfig, NodeInfo};
use kdx_types::hash::{b64url_decode, b64url_encode};
use serde_json::Value;

pub const POOL_LEN: usize = 24;

pub fn pool() -> Vec<Value> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../kdx-ingest/tests/fixtures/chain0_3125000.json.zst"
    );
    let f = std::fs::File::open(path).expect("fixture");
    let mut s = Vec::new();
    zstd::Decoder::new(f).unwrap().read_to_end(&mut s).unwrap();
    let v: Vec<Value> = serde_json::from_slice(&s).unwrap();
    assert_eq!(v.len(), POOL_LEN);
    v
}

/// Synthetic header hash of `(height, branch)`.
pub fn hh(height: u32, branch: u8) -> String {
    let mut b = [0u8; 32];
    b[0] = branch;
    b[1..5].copy_from_slice(&height.to_be_bytes());
    b[31] = 0x6b;
    b64url_encode(&b)
}

/// One block on a branch: pool block `i` at `height` with branch id `br`, whose parent is
/// `(height − 1, parent_br)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Place {
    pub pool: usize,
    pub height: u32,
    pub br: u8,
    pub parent_br: u8,
}

pub fn prepare(pool: &[Value], p: Place) -> (DecodedBlock, VerifyReport) {
    let mut h = pool[p.pool]["header"].clone();
    h["height"] = Value::from(p.height);
    h["hash"] = Value::from(hh(p.height, p.br));
    h["parent"] = Value::from(hh(p.height.wrapping_sub(1), p.parent_br));
    let r = pipeline::prepare_value(0, p.height, &h, &pool[p.pool]["payload"]).unwrap();
    assert!(r.1.merkle_verified(), "pool {} must verify", p.pool);
    r
}

/// A linear branch: pool blocks `ids` at `from..`, all with branch id `br`; the first one's
/// parent has branch id `parent_br`.
pub fn branch(ids: &[usize], from: u32, br: u8, parent_br: u8) -> Vec<Place> {
    ids.iter()
        .enumerate()
        .map(|(i, &pool)| Place {
            pool,
            height: from + i as u32,
            br,
            parent_br: if i == 0 { parent_br } else { br },
        })
        .collect()
}

pub fn opts(dir: &Path, step: u64, range: u64, finality: u64) -> IngestOpts {
    IngestOpts {
        network: "mainnet01".into(),
        data_dir: dir.to_path_buf(),
        geometry: Geometry::default(),
        finality_depth: finality,
        batch_size: 50,
        ranges_in_flight: 1,
        range_heights: range,
        step_heights: step,
        min_free_gb: 0,
        range_level: 3,
        step_level: 3,
        dict_samples: 0,
        etl_mem_mb: 16,
        mem_cap_mb: 256,
        commit_ms: 1000,
        commit_blocks: 10,
        cut_poll_secs: 10,
        until: None,
    }
}

/// A single-chain (chain 0) context on `<dir>/hot`, `<dir>/segments`, `<dir>/etl`. The node
/// client points at a closed port: nothing here talks to a node.
pub fn ctx(dir: &Path, step: u64, range: u64, finality: u64) -> Ctx {
    let hot = Arc::new(HotStore::open(&dir.join("hot"), Geometry::default()).unwrap());
    let node = NodeClient::new(&NodeClientConfig {
        base: "http://127.0.0.1:9".parse().unwrap(),
        network: "mainnet01".into(),
        timeout: Duration::from_secs(1),
        max_rps: 0,
        concurrency: 1,
        p2p_url: None,
        p2p_cert_sha256: None,
        max_body_bytes: 1 << 20,
        max_sse_event_bytes: 1 << 20,
    })
    .unwrap();
    let info = NodeInfo {
        node_version: String::new(),
        chainweb_version: "mainnet01".into(),
        graph_history: vec![(0, vec![0])],
        raw: Value::Null,
    };
    std::fs::create_dir_all(dir.join("segments")).unwrap();
    std::fs::create_dir_all(dir.join("etl")).unwrap();
    Ctx {
        opts: opts(dir, step, range, finality),
        node,
        dict: Arc::new(Mutex::new(HotDict::load(&hot.ro().unwrap()).unwrap())),
        hot,
        segdir: dir.join("segments"),
        etl: dir.join("etl"),
        graph: ChainGraph::from_info(&info),
    }
}

/// Apply `places` (ascending, linear) through the tail writer, `chunk` blocks per commit.
pub fn apply_linear(ctx: &Ctx, pool: &[Value], places: &[Place], chunk: usize) {
    for c in places.chunks(chunk.max(1)) {
        let blocks = c.iter().map(|p| prepare(pool, *p)).collect();
        apply::commit(
            ctx,
            &[Batch {
                chain: 0,
                fork: None,
                blocks,
                halted: false,
                halt_reason: "",
            }],
        )
        .unwrap();
    }
}

/// The follower's reorg batch: unwind to `fork_height` (kept), then apply `places`.
pub fn apply_fork(ctx: &Ctx, pool: &[Value], fork_height: u32, depth: u32, places: &[Place]) {
    let blocks = places.iter().map(|p| prepare(pool, *p)).collect();
    apply::commit(
        ctx,
        &[Batch {
            chain: 0,
            fork: Some(Fork {
                height: fork_height,
                depth,
            }),
            blocks,
            halted: false,
            halt_reason: "",
        }],
    )
    .unwrap();
}

/// `hot stats --digest` per table (blake3 over every (key, value) row, as in
/// `kda-indexer hot stats --digest`), with the replay-neutral exclusions spelled out:
/// - `Meta` and the additive tables (balances, NFT, marks) have no digest there either;
/// - dictionary tables are returned separately by [`dict_rows`]: names interned by undone
///   blocks stay by design (review M1), so they are compared as a subset;
/// - `Counters` rows at 0 (left by an undo) and the reorg bookkeeping counters (`reorg*`).
pub fn digest(hot: &HotStore) -> BTreeMap<String, (usize, String)> {
    let ro = hot.ro().unwrap();
    let mut out = BTreeMap::new();
    for t in Table::all() {
        if t == Table::Meta || t.is_additive() || t.name().starts_with("Dict") {
            continue;
        }
        let mut h = blake3::Hasher::new();
        let mut n = 0usize;
        for (k, v) in ro.scan(t, &[], |_| true).unwrap() {
            if t == Table::Counters && (v.iter().all(|&x| x == 0) || k.starts_with(b"reorg")) {
                continue;
            }
            h.update(&(k.len() as u32).to_be_bytes());
            h.update(&k);
            h.update(&(v.len() as u32).to_be_bytes());
            h.update(&v);
            n += 1;
        }
        out.insert(t.name(), (n, h.finalize().to_hex().to_string()));
    }
    out
}

/// Every dictionary row as (table, key, value).
pub fn dict_rows(hot: &HotStore) -> BTreeSet<(String, Vec<u8>, Vec<u8>)> {
    let ro = hot.ro().unwrap();
    let mut out = BTreeSet::new();
    for t in Table::all() {
        if t.name().starts_with("Dict") {
            for (k, v) in ro.scan(t, &[], |_| true).unwrap() {
                out.insert((t.name(), k, v));
            }
        }
    }
    out
}

/// Assert `a` and `b` hold the same replayed state: equal digests, and the dictionary of the
/// shorter history is a subset (same ids) of the longer one's.
pub fn assert_same_state(longer: &HotStore, shorter: &HotStore, what: &str) {
    let (da, db) = (digest(longer), digest(shorter));
    for (t, a) in &da {
        assert_eq!(Some(a), db.get(t), "{what}: table {t} differs");
    }
    let (ra, rb) = (dict_rows(longer), dict_rows(shorter));
    let missing: Vec<_> = rb.difference(&ra).take(3).collect();
    assert!(
        missing.is_empty(),
        "{what}: dictionary rows lost: {missing:?}"
    );
}

/// Fungible TRANSFER sides of a pool block, read straight from the served JSON (not through
/// the decoder or the index): `(account, module)` for each non-empty sender/receiver of a
/// 3-parameter `TRANSFER` event in the coinbase and every tx output.
pub fn transfer_sides(pool: &[Value], i: usize) -> BTreeSet<(String, String)> {
    let p = &pool[i]["payload"];
    let mut outs = vec![p["coinbase"].as_str().unwrap().to_owned()];
    for t in p["transactions"].as_array().unwrap() {
        outs.push(t[1].as_str().unwrap().to_owned());
    }
    let mut s = BTreeSet::new();
    for o in outs {
        let bytes = b64url_decode(&o).unwrap();
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        for ev in v["events"].as_array().into_iter().flatten() {
            let params = ev["params"].as_array();
            if ev["name"] != "TRANSFER" || params.map(Vec::len) != Some(3) {
                continue;
            }
            let params = params.unwrap();
            let m = &ev["module"];
            let module = match m["namespace"].as_str() {
                Some(ns) => format!("{ns}.{}", m["name"].as_str().unwrap()),
                None => m["name"].as_str().unwrap().to_owned(),
            };
            for side in &params[..2] {
                if let Some(a) = side.as_str().filter(|a| !a.is_empty()) {
                    s.insert((a.to_owned(), module.clone()));
                }
            }
        }
    }
    s
}

/// Small deterministic generator (xorshift64*), so a failing case prints its seed.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }

    pub fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            let j = self.range(0, i as u64) as usize;
            v.swap(i, j);
        }
    }
}

/// Id-free view of the hot blocks: per block (height, header hash, request keys in order),
/// after checking that its stored records re-render to the node's Merkle roots through the
/// store's own dictionary (`check_records`). Two stores whose dictionaries assign other ids
/// to the same names compare equal here.
pub fn rendered_blocks(hot: &HotStore) -> Vec<(u32, Vec<u8>, Vec<Vec<u8>>)> {
    use kdx_types::record::{decode_block, decode_tx};
    let ro = hot.ro().unwrap();
    let dict = HotDict::load(&ro).unwrap();
    let mut txs: BTreeMap<(u32, u8), Vec<kdx_types::record::TxRecord>> = BTreeMap::new();
    for (k, v) in ro.scan(Table::TailTxs, &[], |_| true).unwrap() {
        let key = kdx_types::TxKey::from_be_bytes(k.as_slice().try_into().unwrap());
        txs.entry((key.height(), key.chain()))
            .or_default()
            .push(decode_tx(&v).unwrap());
    }
    let mut out = Vec::new();
    for (_, v) in ro.scan(Table::Headers, &[], |_| true).unwrap() {
        let b = decode_block(&v).unwrap();
        let h = u32::try_from(b.height).unwrap();
        let t = txs.remove(&(h, b.chain)).unwrap_or_default();
        let r = kdx_decode::merkle::check_records(&b, &t, &dict).unwrap();
        assert!(
            r.is_verified(),
            "height {h}: stored records do not re-render: {r:?}"
        );
        let rks = t
            .iter()
            .map(|t| t.request_key.map(|h| h.0.to_vec()).unwrap_or_default())
            .collect();
        out.push((h, b.hash.0.to_vec(), rks));
    }
    assert!(
        txs.is_empty(),
        "tx records without a header: {:?}",
        txs.keys()
    );
    out
}

/// The same state modulo dictionary ids: equal row counts in every compared table, equal
/// digests of the id-free tables, and equal [`rendered_blocks`].
pub fn assert_same_state_modulo_ids(a: &HotStore, b: &HotStore, what: &str) {
    let (da, db) = (digest(a), digest(b));
    for (t, (n, h)) in &da {
        let (m, g) = &db[t];
        assert_eq!(n, m, "{what}: table {t} row count");
        if ["TailRk", "HeaderByHash", "StageCheckpoint", "Counters"].contains(&t.as_str()) {
            assert_eq!(h, g, "{what}: id-free table {t} differs");
        }
    }
    assert_eq!(
        rendered_blocks(a),
        rendered_blocks(b),
        "{what}: rendered blocks"
    );
}
