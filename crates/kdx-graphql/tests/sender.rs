//! FIX-2 T5: `transactions(accountName)` (with and without `fungibleName`) and REST
//! `/accounts/{a}/txs?role=sender` read the `sndr` postings when the view is sender-ready, and
//! answer exactly what the old path (the account's `acct`/`xfer` postings filtered on the
//! record's `meta.sender`) answers: edges, cursors, `pageInfo`, `totalCount`, REST items and
//! `next_cursor`, for every account and argument combination of the fixture, in every store
//! layout (hot only, frozen only, split, right after a freeze, after a merge, after a reorg,
//! and the not-ready layouts that fall back to the old path).
//!
//! The fixture's accounts: a heavy receiver that also sends, an account that sends and
//! receives, a plain sender that also pays gas for others, a 256-byte sender (posted), a
//! 257-byte one (never posted), and an unknown name. Txs include cross-chain halves, a
//! coinbase paying the heavy receiver, a cmd that does not parse and a non-string sender.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use kdx_api::native::Q;
use kdx_api::{Api, ApiSettings, ApiState, Store, TipWatch};
use kdx_decode::{decode_block, index_block, DecodedBlock, IndexedBlock, VerifyReport};
use kdx_graphql::{build_schema, GraphqlSettings, KdxSchema};
use kdx_hot::balances::{bal_key, BalanceRow};
use kdx_hot::{HotDict, Table};
use kdx_segment::{Manifest, RangeKind, RangeWriter, WriteOpts};
use kdx_types::hash::b64url_encode;
use kdx_types::{blake2b_256, DictKind, DictMut};
use serde_json::{json, Value};

const HEAVY: &str = "k:heavy";
const BOTH: &str = "k:both";
const ALICE: &str = "k:alice";
const NOBODY: &str = "k:nobody";

fn long(n: usize, c: char) -> String {
    format!("k:{}", c.to_string().repeat(n - 2))
}

/// One user tx: its `meta.sender` (any JSON), its events, and whether its cmd is unparsable.
#[derive(Clone)]
struct TxSpec {
    sender: Value,
    events: Vec<Value>,
    unparsed: bool,
}

fn ev(ns: Option<&str>, module: &str, name: &str, params: Value) -> Value {
    json!({"params": params, "name": name,
           "module": {"namespace": ns, "name": module},
           "moduleHash": b64url_encode(&[11; 32])})
}

fn coin(from: &str, to: &str, amt: f64) -> Value {
    ev(None, "coin", "TRANSFER", json!([from, to, amt]))
}

fn kdx(from: &str, to: &str, amt: f64) -> Value {
    ev(Some("kaddex"), "kdx", "TRANSFER", json!([from, to, amt]))
}

fn tx(sender: impl Into<Value>, events: Vec<Value>) -> TxSpec {
    TxSpec {
        sender: sender.into(),
        events,
        unparsed: false,
    }
}

/// The user txs of block `(c, h)` on fork `fork` (fork 1 replaces the tip blocks of chain 0 in
/// the reorg layout, with other senders).
fn txs_of(c: u8, h: u32, fork: u8) -> Vec<TxSpec> {
    let l256 = long(256, 'a');
    let l257 = long(257, 'b');
    let first = if fork == 0 { ALICE } else { BOTH };
    let mut v = vec![
        // The heavy receiver receives in every block.
        tx(first, vec![coin(first, HEAVY, 1.0)]),
        tx(ALICE, vec![coin(ALICE, HEAVY, 2.0)]),
    ];
    if h.is_multiple_of(2) {
        v.push(tx(
            HEAVY,
            vec![coin(HEAVY, BOTH, 1.5), kdx(HEAVY, ALICE, 3.0)],
        ));
    } else {
        // Alice pays gas for a transfer between the other two.
        v.push(tx(ALICE, vec![kdx(HEAVY, BOTH, 0.5)]));
    }
    if h.is_multiple_of(3) {
        v.push(tx(BOTH, vec![coin(BOTH, HEAVY, 0.25)]));
        v.push(tx(ALICE, vec![coin(ALICE, BOTH, 0.75)]));
    }
    if h % 4 == 1 {
        v.push(TxSpec {
            sender: json!(HEAVY),
            events: vec![coin(HEAVY, ALICE, 9.0)],
            unparsed: true,
        });
    }
    if h % 5 == 2 {
        v.push(tx(l256.as_str(), vec![coin(&l256, HEAVY, 1.0)]));
        v.push(tx(l257.as_str(), vec![coin(&l257, HEAVY, 1.0)]));
    }
    // Cross-chain halves: out of chain 0, into chain 1 (the cont is paid by Both).
    if c == 0 && h % 3 == 1 {
        v.push(tx(HEAVY, vec![coin(HEAVY, "", 1.0)]));
    }
    if c == 1 && h % 3 == 2 {
        v.push(tx(BOTH, vec![coin("", HEAVY, 1.0)]));
    }
    if h == 5 {
        v.push(tx(42, vec![coin(HEAVY, ALICE, 1.0)]));
    }
    v
}

/// A distinct hash per `(tag, chain, height, fork)`.
fn hash_of(tag: u8, c: u8, h: u32, fork: u8) -> [u8; 32] {
    let mut b = [tag; 32];
    b[0] = 0x80 | c;
    b[1..5].copy_from_slice(&h.to_be_bytes());
    b[5] = fork;
    b
}

/// Block `(c, h)` of `fork` with a coinbase paying the heavy receiver and `txs`.
fn decoded(c: u8, h: u32, fork: u8, specs: &[TxSpec]) -> DecodedBlock {
    let mut txs = Vec::new();
    for (i, s) in specs.iter().enumerate() {
        let cmd = if s.unparsed {
            format!(
                "{{\"meta\": {{\"sender\": {}}} not json {c} {h} {i}",
                s.sender
            )
        } else {
            json!({
                "payload": {"exec": {"data": null, "code": format!("(tx {c} {h} {i} {fork})")}},
                "meta": {"sender": s.sender, "chainId": c.to_string(),
                         "creationTime": 1_700_000_000u64 + u64::from(h),
                         "gasLimit": 1000, "gasPrice": 1e-8, "ttl": 600},
                "signers": [{"pubKey": "k1"}],
                "nonce": format!("n-{c}-{h}-{i}-{fork}"),
                "networkId": "mainnet01",
            })
            .to_string()
        };
        let rk = blake2b_256(cmd.as_bytes()).to_b64url();
        let t =
            serde_json::to_vec(&json!({"hash": rk, "sigs": [{"sig": "ab"}], "cmd": cmd})).unwrap();
        let out = serde_json::to_vec(&json!({"gas": 1,
            "result": {"status": "success", "data": 1}, "reqKey": rk, "logs": null,
            "events": s.events, "metaData": null, "continuation": null, "txId": null}))
        .unwrap();
        txs.push(json!([b64url_encode(&t), b64url_encode(&out)]));
    }
    let cb = serde_json::to_vec(
        &json!({"gas": 0, "result": {"status": "success", "data": "ok"},
        "reqKey": b64url_encode(&hash_of(0xCB, c, h, fork)), "logs": null,
        "events": [coin("", HEAVY, 2.0)],
        "metaData": null, "continuation": null, "txId": null}),
    )
    .unwrap();
    let miner = format!(r#"{{"account":"{HEAVY}","predicate":"keys-all","public-keys":["m"]}}"#);
    let payload_hash = b64url_encode(&hash_of(0xA4, c, h, fork));
    let parent_fork = if fork > 0 && h > REORG_FROM + 1 {
        fork
    } else {
        0
    };
    let header = json!({
        "nonce": "1", "creationTime": 1_700_000_000_000_000u64 + u64::from(h) * 30_000_000,
        "parent": b64url_encode(&hash_of(0xB0, c, h.saturating_sub(1), parent_fork)),
        "adjacents": {"5": b64url_encode(&hash_of(0xAD, c, h, fork))},
        "target": b64url_encode(&[0xff; 32]), "payloadHash": payload_hash, "chainId": c,
        "weight": b64url_encode(&[1; 32]), "height": h, "chainwebVersion": "mainnet01",
        "epochStart": 1, "featureFlags": 0, "hash": b64url_encode(&hash_of(0xB0, c, h, fork))
    });
    let payload = json!({
        "transactions": txs, "minerData": b64url_encode(miner.as_bytes()),
        "transactionsHash": b64url_encode(&hash_of(0xA7, c, h, fork)),
        "outputsHash": b64url_encode(&hash_of(0xA8, c, h, fork)),
        "payloadHash": payload_hash, "coinbase": b64url_encode(&cb),
    });
    decode_block(&header, &payload).expect("block decodes")
}

/// Heights `0..TOP` on chains 0 and 1.
const TOP: u32 = 14;
/// The split layouts freeze heights below this.
const FRONTIER: u32 = 8;
/// The reorg layout replaces chain 0's blocks above this height.
const REORG_FROM: u32 = 11;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Layout {
    /// Every block in the hot store.
    HotOnly,
    /// Every block in two frozen steps.
    FrozenOnly,
    /// Two frozen steps below `FRONTIER`, the rest hot.
    Split,
    /// Every block applied to the hot store, the ones below `FRONTIER` then frozen; the
    /// frozen blocks' hot rows are still there (the freezer deletes them after the manifest
    /// commit).
    AfterFreeze,
    /// One merged range below `FRONTIER`, the rest hot.
    Merged,
    /// `Split`, then chain 0 rolled back above `REORG_FROM` and fork 1 applied there.
    Reorg,
    /// `Split` with one step written before FIX-2 (no `sndr.pst`): not ready, old path.
    StepWithoutSndr,
    /// `Split` with a hot store whose `sndr_hot` is not set: not ready, old path.
    HotNotReady,
}

const LAYOUTS: [Layout; 8] = [
    Layout::HotOnly,
    Layout::FrozenOnly,
    Layout::Split,
    Layout::AfterFreeze,
    Layout::Merged,
    Layout::Reorg,
    Layout::StepWithoutSndr,
    Layout::HotNotReady,
];

struct Fx {
    _dir: tempfile::TempDir,
    store: Arc<Store>,
    state: Arc<ApiState>,
    schema: KdxSchema,
}

fn settings() -> ApiSettings {
    ApiSettings {
        network: "mainnet01".into(),
        deadline: Duration::from_secs(30),
        max_in_flight: 16,
        max_body_bytes: 8 << 20,
        v1txs_max_body_bytes: 8 << 20,
        rate_per_ip: 1_000_000,
        rate_burst: 1_000_000,
        trusted_proxies: Vec::new(),
        read_threads: 2,
        ready_max_lag: 10,
        max_event_lists: 64,
    }
}

/// Strip `sndr.pst` (file, `files` and `postings.sndr`) from a written range.
fn strip_sndr(segdir: &std::path::Path, e: &kdx_segment::RangeEntry) {
    use kdx_segment::files::{file_name, FileKind};
    let toml = segdir.join(file_name("mainnet01", FileKind::Range, e.start, e.end));
    let mut info = kdx_segment::RangeInfo::load(&toml).unwrap();
    let n = file_name("mainnet01", FileKind::Sndr, e.start, e.end);
    assert!(info.files.remove(&n).is_some(), "{n} listed");
    std::fs::remove_file(segdir.join(&n)).unwrap();
    assert!(info.postings.remove("sndr").is_some());
    std::fs::write(&toml, toml::to_string(&info).unwrap()).unwrap();
}

/// Write `ranges` (each `(kind, start, end, blocks)`) as the store's segments.
fn write_segments(
    dir: &std::path::Path,
    ranges: Vec<(RangeKind, u32, u32, Vec<IndexedBlock>)>,
) -> (Vec<kdx_segment::RangeEntry>, u64) {
    let segdir = dir.join("segments");
    let mut m = Manifest::new("mainnet01");
    let mut first_txnum = 0;
    let mut entries = Vec::new();
    for (kind, start, end, blocks) in ranges {
        let mut w = RangeWriter::create(
            &segdir,
            &dir.join(format!("etl-{start}")),
            "mainnet01",
            kind,
            start,
            end,
            first_txnum,
            WriteOpts::new(1, 0),
        )
        .unwrap();
        for b in &blocks {
            w.push(b).unwrap();
        }
        let e = w.finish().unwrap();
        first_txnum = e.end_txnum();
        m.push(e.clone()).unwrap();
        entries.push(e);
    }
    m.commit(&segdir).unwrap();
    (entries, first_txnum)
}

/// `(c, h)` of every fixture block, ascending by height then chain.
fn places(top: u32) -> Vec<(u8, u32)> {
    (0..top).flat_map(|h| [(0, h), (1, h)]).collect()
}

fn fixture(layout: Layout) -> Fx {
    build(layout, txs_of, TOP, FRONTIER)
}

fn build(layout: Layout, txs: impl Fn(u8, u32, u8) -> Vec<TxSpec>, top: u32, frontier: u32) -> Fx {
    let d = tempfile::tempdir().unwrap();
    let g = kdx_hot::Geometry {
        max_gb: 1,
        growth_mb: 16,
    };
    let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap());
    let mut t = hot.rw().unwrap();
    let mut dict = HotDict::load(&t).unwrap();
    let index = |c: u8, h: u32, fork: u8, dict: &mut HotDict| -> IndexedBlock {
        let b = decoded(c, h, fork, &txs(c, h, fork));
        index_block(&b, &VerifyReport::default(), dict).expect("block indexes")
    };
    let frontier = match layout {
        Layout::HotOnly => 0,
        Layout::FrozenOnly => top,
        _ => frontier,
    };
    let half = frontier / 2;
    let all = places(top);
    let frozen: Vec<(u8, u32)> = all.iter().copied().filter(|p| p.1 < frontier).collect();
    let hot_blocks: Vec<(u8, u32)> = if layout == Layout::AfterFreeze {
        all.clone()
    } else {
        all.iter().copied().filter(|p| p.1 >= frontier).collect()
    };
    // The tail first when it holds frozen heights too (the names are interned in apply order).
    if layout == Layout::AfterFreeze {
        for &(c, h) in &hot_blocks {
            let ib = index(c, h, 0, &mut dict);
            kdx_hot::tail::apply(&mut t, &ib, &mut dict).unwrap();
        }
    }
    let seg_blocks = |lo: u32, hi: u32, dict: &mut HotDict| -> Vec<IndexedBlock> {
        frozen
            .iter()
            .filter(|p| (lo..hi).contains(&p.1))
            .map(|&(c, h)| index(c, h, 0, dict))
            .collect()
    };
    let ranges = if frontier == 0 {
        vec![]
    } else if layout == Layout::Merged {
        vec![(
            RangeKind::Range,
            0,
            frontier,
            seg_blocks(0, frontier, &mut dict),
        )]
    } else {
        vec![
            (RangeKind::Step, 0, half, seg_blocks(0, half, &mut dict)),
            (
                RangeKind::Step,
                half,
                frontier,
                seg_blocks(half, frontier, &mut dict),
            ),
        ]
    };
    if !ranges.is_empty() {
        let (entries, next) = write_segments(d.path(), ranges);
        if layout == Layout::StepWithoutSndr {
            strip_sndr(&d.path().join("segments"), &entries[0]);
        }
        t.set_frozen_frontier(frontier).unwrap();
        t.set_next_txnum(next).unwrap();
    }
    if layout != Layout::AfterFreeze {
        for &(c, h) in &hot_blocks {
            let ib = index(c, h, 0, &mut dict);
            kdx_hot::tail::apply(&mut t, &ib, &mut dict).unwrap();
        }
    }
    if layout == Layout::Reorg {
        kdx_hot::tail::unwind_to(&mut t, 0, REORG_FROM).unwrap();
        for h in REORG_FROM + 1..top {
            let ib = index(0, h, 1, &mut dict);
            kdx_hot::tail::apply(&mut t, &ib, &mut dict).unwrap();
        }
    }
    // Coin balances, so `fungibleChainAccount` resolves for the named accounts.
    let coin_id = dict.intern(DictKind::Module, "coin");
    for a in [HEAVY, BOTH, ALICE] {
        let id = dict.intern(DictKind::Account, a);
        for c in [0u8, 1] {
            let row = BalanceRow {
                absent: false,
                balance: "1.0".into(),
                guard: [0; 16],
                as_of: 1,
                reconciled_at: 1,
                unverified: false,
            };
            t.put(Table::Balances, &bal_key(id, coin_id, c), &row.encode())
                .unwrap();
        }
    }
    dict.persist(&mut t).unwrap();
    if layout == Layout::HotNotReady {
        t.del(
            kdx_hot::Table::Meta,
            kdx_hot::env::meta::SNDR_HOT.as_bytes(),
        )
        .unwrap();
    } else {
        t.set_meta_u64(kdx_hot::env::meta::SNDR_HOT, 1).unwrap();
    }
    t.commit().unwrap();

    let store = Arc::new(Store::open(hot, d.path()).unwrap());
    let state = Api::new(store.clone(), settings(), TipWatch::default()).state();
    let gs = GraphqlSettings {
        count_budget: Duration::from_secs(30),
        ..GraphqlSettings::default()
    };
    Fx {
        _dir: d,
        store,
        schema: build_schema(state.clone(), gs, None),
        state,
    }
}

/// Whether a fresh view of `fx` would take the sender path.
fn ready(fx: &Fx) -> bool {
    let v = fx
        .store
        .view(Instant::now() + Duration::from_secs(5))
        .unwrap();
    v.sender_ready().unwrap()
}

async fn exec(s: &KdxSchema, q: &str) -> Value {
    let r = s.execute(q).await;
    serde_json::to_value(&r).unwrap()
}

/// `q` answered by the old path and by the sender path.
async fn both(fx: &Fx, q: &str) -> (Value, Value) {
    fx.store.set_sender_index(false);
    let old = exec(&fx.schema, q).await;
    fx.store.set_sender_index(true);
    let new = exec(&fx.schema, q).await;
    (old, new)
}

const SEL: &str = "totalCount pageInfo { hasNextPage hasPreviousPage startCursor endCursor } \
                   edges { cursor node { hash } }";

fn tx_query(args: &str) -> String {
    format!("{{ transactions({args}) {{ {SEL} }} }}")
}

fn conn(v: &Value) -> &Value {
    &v["data"]["transactions"]
}

fn accounts() -> Vec<String> {
    vec![
        HEAVY.into(),
        BOTH.into(),
        ALICE.into(),
        long(256, 'a'),
        long(257, 'b'),
        NOBODY.into(),
    ]
}

/// Every argument combination (besides the account and the page) the fixture is checked with.
fn filters() -> Vec<String> {
    let mut out = Vec::new();
    for fung in [None, Some("coin"), Some("kaddex.kdx"), Some("free.none")] {
        for chain in [None, Some("0"), Some("1")] {
            for min_h in [None, Some(3)] {
                for max_h in [None, Some(11)] {
                    for depth in [None, Some(3)] {
                        let mut a = Vec::new();
                        if let Some(f) = fung {
                            a.push(format!("fungibleName: \"{f}\""));
                        }
                        if let Some(c) = chain {
                            a.push(format!("chainId: \"{c}\""));
                        }
                        if let Some(m) = min_h {
                            a.push(format!("minHeight: {m}"));
                        }
                        if let Some(m) = max_h {
                            a.push(format!("maxHeight: {m}"));
                        }
                        if let Some(m) = depth {
                            a.push(format!("minimumDepth: {m}"));
                        }
                        out.push(a.join(" "));
                    }
                }
            }
        }
    }
    out
}

/// Walk the list page by page (`first`/`after` or `last`/`before`), checking each page on
/// both paths; returns the number of pages.
async fn walk(fx: &Fx, base: &str, n: usize, back: bool) -> usize {
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let page = match (&cursor, back) {
            (None, false) => format!("first: {n}"),
            (Some(c), false) => format!("first: {n} after: \"{c}\""),
            (None, true) => format!("last: {n}"),
            (Some(c), true) => format!("last: {n} before: \"{c}\""),
        };
        let q = tx_query(&format!("{base} {page}"));
        let (old, new) = both(fx, &q).await;
        assert_eq!(old, new, "{q}");
        assert!(old["errors"].is_null(), "{q}: {old}");
        pages += 1;
        let pi = &conn(&new)["pageInfo"];
        let (more, next) = if back {
            (pi["hasPreviousPage"].as_bool().unwrap(), &pi["startCursor"])
        } else {
            (pi["hasNextPage"].as_bool().unwrap(), &pi["endCursor"])
        };
        if !more {
            return pages;
        }
        cursor = Some(next.as_str().unwrap().to_owned());
        assert!(pages < 100, "{q}: runaway walk");
    }
}

/// Every account × every filter combination: the big page (and its `totalCount`) is the same
/// on both paths; for the combinations without chain/depth filters, every forward and
/// backward page too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graphql_sender_lists_equal_the_old_path_in_every_layout() {
    for layout in LAYOUTS {
        let fx = fixture(layout);
        let expect_ready = !matches!(layout, Layout::StepWithoutSndr | Layout::HotNotReady);
        assert_eq!(ready(&fx), expect_ready, "{layout:?}");
        let mut nonempty = 0;
        for a in accounts() {
            for f in filters() {
                let base = format!("accountName: \"{a}\" {f}");
                let q = tx_query(&format!("{base} first: 100"));
                let (old, new) = both(&fx, &q).await;
                assert_eq!(old, new, "{layout:?} {q}");
                assert!(old["errors"].is_null(), "{layout:?} {q}: {old}");
                let n = conn(&new)["edges"].as_array().unwrap().len();
                assert_eq!(conn(&new)["totalCount"], json!(n), "{layout:?} {q}");
                if n > 0 {
                    nonempty += 1;
                }
                if !f.contains("chainId") && !f.contains("minimumDepth") && n > 0 {
                    walk(&fx, &base, 4, false).await;
                    walk(&fx, &base, 3, true).await;
                }
            }
        }
        // `FungibleChainAccount.transactions` is a sender list too.
        for a in [HEAVY, BOTH, ALICE] {
            for c in ["0", "1"] {
                for page in ["first: 100", "first: 2", "last: 3"] {
                    let q = format!(
                        "{{ fungibleChainAccount(accountName: \"{a}\" chainId: \"{c}\") \
                         {{ transactions({page}) {{ {SEL} }} }} }}"
                    );
                    let (old, new) = both(&fx, &q).await;
                    assert_eq!(old, new, "{layout:?} {q}");
                    let txs = &new["data"]["fungibleChainAccount"]["transactions"];
                    assert!(txs["edges"].is_array(), "{layout:?} {q}: {new}");
                    if page == "first: 100" && a != BOTH {
                        assert!(!txs["edges"].as_array().unwrap().is_empty(), "{q}");
                    }
                }
            }
        }
        // The fixture is not vacuous: most combinations list something.
        assert!(
            nonempty >= 100,
            "{layout:?}: only {nonempty} lists had edges"
        );
    }
}

/// The sender lists hold exactly the fixture's sent txs: for the heavy receiver, its own
/// sends only (no receive, no unparsed cmd, no coinbase, no tx it only appears in).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graphql_sender_list_is_the_sent_txs() {
    let fx = fixture(Layout::Split);
    let q = format!("{{ transactions(accountName: \"{HEAVY}\" first: 100) {{ edges {{ node {{ cmd {{ meta {{ sender }} }} }} }} }} }}");
    let v = exec(&fx.schema, &q).await;
    let edges = conn(&v)["edges"].as_array().unwrap();
    // Sends of HEAVY per (c, h): h even (one) and chain 0 with h % 3 == 1 (cross-chain out).
    let want = places(TOP)
        .iter()
        .map(|&(c, h)| usize::from(h % 2 == 0) + usize::from(c == 0 && h % 3 == 1))
        .sum::<usize>();
    assert_eq!(edges.len(), want, "{v}");
    for e in edges {
        assert_eq!(e["node"]["cmd"]["meta"]["sender"], json!(HEAVY));
    }
    // The 257-byte sender is never posted, so neither path lists it; the 256-byte one is.
    for (a, some) in [(long(256, 'a'), true), (long(257, 'b'), false)] {
        let q = tx_query(&format!("accountName: \"{a}\" first: 100"));
        let (old, new) = both(&fx, &q).await;
        assert_eq!(old, new);
        assert_eq!(!conn(&new)["edges"].as_array().unwrap().is_empty(), some);
    }
}

/// REST `role=sender`, every account and filter (chain, min/max height, both orders), paged
/// by `limit` 3 to the end: the same pages on both paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_role_sender_equals_the_old_path_in_every_layout() {
    for layout in LAYOUTS {
        let fx = fixture(layout);
        for a in accounts() {
            for chain in [None, Some(0u8), Some(1)] {
                for (minh, maxh) in [(None, None), (Some(3), Some(11))] {
                    for order in ["desc", "asc"] {
                        let base = Q {
                            role: Some("sender".into()),
                            limit: Some(3),
                            chain,
                            minheight: minh,
                            maxheight: maxh,
                            order: Some(order.into()),
                            ..Q::default()
                        };
                        fx.store.set_sender_index(false);
                        let old = rest_walk(&fx.state, &a, &base).await;
                        fx.store.set_sender_index(true);
                        let new = rest_walk(&fx.state, &a, &base).await;
                        assert_eq!(old, new, "{layout:?} {a} {chain:?} {minh:?} {order}");
                    }
                }
            }
        }
    }
}

/// One REST response: status and body.
async fn rest(st: &Arc<ApiState>, account: &str, q: Q) -> (u16, Value) {
    let r = kdx_api::native::accounts::txs(
        State(st.clone()),
        Ok(Path(account.to_owned())),
        Ok(Query(q)),
    )
    .await;
    let status = r.status().as_u16();
    let b = axum::body::to_bytes(r.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

fn clone_q(q: &Q) -> Q {
    Q {
        role: q.role.clone(),
        limit: q.limit,
        cursor: q.cursor.clone(),
        chain: q.chain,
        minheight: q.minheight,
        maxheight: q.maxheight,
        order: q.order.clone(),
        ..Q::default()
    }
}

/// Every page of a REST list, following `next_cursor`.
async fn rest_walk(st: &Arc<ApiState>, account: &str, base: &Q) -> Vec<(u16, Value)> {
    let mut out = Vec::new();
    let mut cursor = None;
    loop {
        let mut q = clone_q(base);
        q.cursor = cursor;
        let (s, mut v) = rest(st, account, q).await;
        let next = v["next_cursor"].as_str().map(str::to_owned);
        if let Some(o) = v.as_object_mut() {
            // `as_of` is the request's own clock.
            o.remove("as_of");
        }
        out.push((s, v));
        match next {
            Some(c) if out.len() < 200 => cursor = Some(c),
            _ => return out,
        }
    }
}

/// A receive-heavy account (thousands of received txs above its one sent tx): the old path
/// runs out of the 1 s scan window (`TIMEOUT`, no data), the sender path answers its tx. REST
/// `role=sender` on the old path answers an empty page with a continuation cursor; the
/// sender path answers the row at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receive_heavy_account_times_out_on_the_old_path_and_answers_on_the_new() {
    const PER_BLOCK: u32 = 1_000;
    const BLOCKS: u32 = 24;
    let txs = |c: u8, h: u32, _f: u8| -> Vec<TxSpec> {
        if h == 0 {
            return if c == 0 {
                vec![tx(HEAVY, vec![coin(HEAVY, ALICE, 1.0)])]
            } else {
                vec![]
            };
        }
        (0..PER_BLOCK)
            .map(|i| tx(ALICE, vec![coin(ALICE, HEAVY, f64::from(i) + 0.5)]))
            .collect()
    };
    // Frozen: one tx of Heavy's at height 0, then BLOCKS × 2 chains × PER_BLOCK receives.
    let fx = build(Layout::FrozenOnly, txs, BLOCKS + 1, BLOCKS + 1);
    assert!(ready(&fx));
    let q = tx_query(&format!("accountName: \"{HEAVY}\" first: 5"));
    let (old, new) = both(&fx, &q).await;
    // The old path decodes ~6,000 records a second in a debug build (`cargo test`), so its
    // walk over the 48,000 receives needs ~8 s and stops at the 1 s window. An optimised
    // build may get through them; it must then answer what the new path answers.
    if cfg!(debug_assertions) || !old["errors"].is_null() {
        assert_eq!(
            old["errors"][0]["extensions"]["code"],
            json!("TIMEOUT"),
            "{old}"
        );
        assert!(old["data"]["transactions"].is_null(), "{old}");
    } else {
        assert_eq!(old, new);
    }
    assert!(new["errors"].is_null(), "{new}");
    assert_eq!(conn(&new)["edges"].as_array().unwrap().len(), 1, "{new}");
    assert_eq!(conn(&new)["totalCount"], json!(1));
    assert_eq!(conn(&new)["pageInfo"]["hasNextPage"], json!(false));

    let q = Q {
        role: Some("sender".into()),
        limit: Some(5),
        ..Q::default()
    };
    fx.store.set_sender_index(false);
    let (s, old) = rest(&fx.state, HEAVY, clone_q(&q)).await;
    assert_eq!(s, 200);
    assert!(old["items"].as_array().unwrap().is_empty(), "{old}");
    assert!(old["next_cursor"].is_string(), "{old}");
    fx.store.set_sender_index(true);
    let (s, new) = rest(&fx.state, HEAVY, q).await;
    assert_eq!(s, 200);
    assert_eq!(new["items"].as_array().unwrap().len(), 1, "{new}");
    assert!(new["next_cursor"].is_null(), "{new}");
}
