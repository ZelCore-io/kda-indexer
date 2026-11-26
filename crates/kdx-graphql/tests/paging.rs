//! R-P5P9-62 (FIX-PJ): joining the cursor pages of a tx-keyed list gives the rows of one big
//! page, in the same order, with none lost and none repeated, also when a page ends inside a
//! transaction that has more rows.
//!
//! The store holds frozen blocks (segments, below [`FRONTIER`]) and hot blocks above it, on two
//! chains. Each user tx has several transfers and events at non-contiguous positions (the live
//! case: orderIndex 4, 1 and 0 of one tx). Each list is paged forward (`first`/`after`) and
//! backward (`last`/`before`) at every page size from 1 to one past its length; every page's
//! `pageInfo` and `totalCount` are checked against the big page.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kdx_api::{Api, ApiSettings, Store, TipWatch};
use kdx_decode::{decode_block, index_block, DecodedBlock, IndexedBlock, VerifyReport};
use kdx_graphql::{build_schema, GraphqlSettings, KdxSchema};
use kdx_hot::balances::{bal_key, BalanceRow};
use kdx_hot::{HotDict, Table};
use kdx_segment::{Manifest, RangeKind, RangeWriter, WriteOpts};
use kdx_types::hash::b64url_encode;
use kdx_types::{blake2b_256, DictKind, DictMut, TxKey};
use serde_json::{json, Value};

/// Heights below this are frozen (in segments), the rest hot.
const FRONTIER: u32 = 10;
/// The frozen blocks are written as two step ranges, `[0, FIRST_RANGE_END)` and
/// `[FIRST_RANGE_END, FRONTIER)`.
const FIRST_RANGE_END: u32 = 5;
const ALICE: &str = "k:alice";
/// An account whose every `coin` row the node holds at 0.
const DRAINED: &str = "k:drained";
const BOB: &str = "k:bob";
/// An account whose only stored row the node reported absent.
const GONE: &str = "k:gone";
const CAROL: &str = "k:carol";
const ALICE_KEY: &str = "alice-key";
const NFT_LEDGER: (&str, &str) = ("marmalade-v2", "ledger");
/// The big page: every list of the fixture is shorter.
const BIG: usize = 100;

/// What one user tx emits (all txs are sent and signed by Alice).
#[derive(Debug, Clone, Copy)]
enum Shape {
    /// Five fungible transfers touching Alice at positions 0, 1, 3, 4, 5 (two modules) and a
    /// non-transfer event at 2.
    Many,
    /// Two coin transfers (0, 4) around three NFT transfers (1, 2, 3).
    Nft,
    /// One coin transfer.
    One,
    /// One `coin.NOTE` event whose only param is Carol's name: not a principal and not a
    /// transfer side, so the tx has no `acct` posting of Carol's (audit F-27).
    Note,
}

/// The fixture's blocks: `(chain, height, user txs)`. Heights below [`FRONTIER`] are frozen.
const BLOCKS: &[(u8, u32, &[Shape])] = &[
    // The last tx of range 1 has several rows: a cursor in it bounds at the start of range 2.
    (0, 4, &[Shape::One, Shape::Many]),
    (0, 6, &[Shape::Nft, Shape::Many]),
    (1, 6, &[Shape::Many]),
    // The top frozen height: a cursor here bounds at `FRONTIER` (the `next_txnum` arm).
    (1, 9, &[Shape::Many, Shape::Nft]),
    // Hot, exactly at the frontier.
    (0, FRONTIER, &[Shape::Many]),
    (0, 11, &[Shape::Many, Shape::Nft]),
    (0, 12, &[Shape::Many]),
    (1, 12, &[Shape::One, Shape::Many]),
    // Hot: a tx naming Carol in a non-transfer event only (audit F-27).
    (1, 11, &[Shape::Note]),
];

fn ev(ns: Option<&str>, module: &str, name: &str, params: Value) -> Value {
    json!({"params": params, "name": name,
           "module": {"namespace": ns, "name": module},
           "moduleHash": b64url_encode(&[11; 32])})
}

fn events(s: Shape) -> Vec<Value> {
    let coin = |p: Value| ev(None, "coin", "TRANSFER", p);
    let nft = |p: Value| ev(Some(NFT_LEDGER.0), NFT_LEDGER.1, "TRANSFER", p);
    match s {
        Shape::Many => vec![
            coin(json!([ALICE, BOB, 1.5])),
            coin(json!([BOB, ALICE, 2.5])),
            ev(None, "coin", "NOTE", json!([ALICE])),
            ev(
                Some("kaddex"),
                "kdx",
                "TRANSFER",
                json!([ALICE, CAROL, 3.5]),
            ),
            coin(json!([ALICE, CAROL, 4.5])),
            coin(json!([CAROL, ALICE, 5.5])),
        ],
        Shape::Nft => vec![
            coin(json!([ALICE, BOB, 1.5])),
            nft(json!(["t:1", ALICE, BOB, 1.0])),
            nft(json!(["t:2", BOB, ALICE, 1.0])),
            nft(json!(["t:3", ALICE, CAROL, 1.0])),
            coin(json!([BOB, ALICE, 2.5])),
        ],
        Shape::One => vec![coin(json!([ALICE, BOB, 1.5]))],
        Shape::Note => vec![ev(None, "coin", "NOTE", json!([CAROL]))],
    }
}

/// A distinct 32-byte hash per `(tag, chain, height)`; its first 4 bytes (the cursor's block
/// fingerprint) differ between blocks.
fn hash_of(tag: u8, c: u8, h: u32) -> [u8; 32] {
    let mut b = [tag; 32];
    b[0] = 0x80 | c;
    b[1] = h as u8;
    b
}

fn block_hash(c: u8, h: u32) -> String {
    b64url_encode(&hash_of(0xB0, c, h))
}

/// Block `(c, h)` with a coinbase and `shapes` as its user txs, and their request keys.
fn decoded(c: u8, h: u32, shapes: &[Shape]) -> (DecodedBlock, Vec<String>) {
    let mut txs = Vec::new();
    let mut rks = Vec::new();
    for (i, s) in shapes.iter().enumerate() {
        let ct = 1_700_000_000u64 + u64::from(h);
        let cmd = json!({
            "payload": {"exec": {"data": null, "code": format!("(tx {c} {h} {i})")}},
            "meta": {"sender": ALICE, "chainId": c.to_string(), "creationTime": ct,
                     "gasLimit": 1000, "gasPrice": 1e-8, "ttl": 600},
            "signers": [{"pubKey": ALICE_KEY}],
            "nonce": format!("n-{c}-{h}-{i}"),
            "networkId": "mainnet01",
        })
        .to_string();
        let rk = blake2b_256(cmd.as_bytes()).to_b64url();
        let tx =
            serde_json::to_vec(&json!({"hash": rk, "sigs": [{"sig": "ab"}], "cmd": cmd})).unwrap();
        let out = serde_json::to_vec(&json!({"gas": 1,
            "result": {"status": "success", "data": 1}, "reqKey": rk, "logs": null,
            "events": events(*s), "metaData": null, "continuation": null, "txId": null}))
        .unwrap();
        txs.push(json!([b64url_encode(&tx), b64url_encode(&out)]));
        rks.push(rk);
    }
    let cb = serde_json::to_vec(
        &json!({"gas": 0, "result": {"status": "success", "data": "ok"},
        "reqKey": b64url_encode(&hash_of(0xCB, c, h)), "logs": null,
        "events": [ev(None, "coin", "COINBASE", json!(["k:miner", 2.5]))],
        "metaData": null, "continuation": null, "txId": null}),
    )
    .unwrap();
    let miner = br#"{"account":"k:miner","predicate":"keys-all","public-keys":["m"]}"#;
    let payload_hash = b64url_encode(&hash_of(0xA4, c, h));
    let header = json!({
        "nonce": "1", "creationTime": 1_700_000_000_000_000u64 + u64::from(h) * 30_000_000,
        "parent": b64url_encode(&hash_of(0xB0, c, h.saturating_sub(1))),
        "adjacents": {"5": b64url_encode(&hash_of(0xAD, c, h))},
        "target": b64url_encode(&[0xff; 32]), "payloadHash": payload_hash, "chainId": c,
        "weight": b64url_encode(&[1; 32]), "height": h, "chainwebVersion": "mainnet01",
        "epochStart": 1, "featureFlags": 0, "hash": block_hash(c, h)
    });
    let payload = json!({
        "transactions": txs, "minerData": b64url_encode(miner),
        "transactionsHash": b64url_encode(&hash_of(0xA7, c, h)),
        "outputsHash": b64url_encode(&hash_of(0xA8, c, h)),
        "payloadHash": payload_hash, "coinbase": b64url_encode(&cb),
    });
    (decode_block(&header, &payload).expect("block decodes"), rks)
}

struct Fx {
    _dir: tempfile::TempDir,
    schema: KdxSchema,
    store: Arc<Store>,
    /// User tx request keys by block, in position order.
    rks: HashMap<(u8, u32), Vec<String>>,
}

/// The store: [`BLOCKS`] below [`FRONTIER`] in two frozen step ranges (as the freezer writes
/// them), the rest applied to the hot store by the tail writer; Alice holds coin on both
/// chains and NFTs on chain 0, so her account nodes resolve.
fn fixture() -> Fx {
    fixture_with(GraphqlSettings::default())
}

/// [`fixture`] with the GraphQL settings of the test.
fn fixture_with(gs: GraphqlSettings) -> Fx {
    fixture_big(gs, 0)
}

/// [`fixture_with`] plus `extra` hot blocks on chain 0 above the fixture's tip, one
/// `Shape::One` tx of Alice's each.
fn fixture_big(gs: GraphqlSettings, extra: u32) -> Fx {
    let d = tempfile::tempdir().unwrap();
    let g = kdx_hot::Geometry {
        max_gb: 1,
        growth_mb: 16,
    };
    let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap());
    let mut t = hot.rw().unwrap();
    let mut dict = HotDict::load(&t).unwrap();
    let mut rks = HashMap::new();
    let mut index = |c: u8, h: u32, shapes: &[Shape], dict: &mut HotDict| -> IndexedBlock {
        let (b, keys) = decoded(c, h, shapes);
        rks.insert((c, h), keys);
        index_block(&b, &VerifyReport::default(), dict).expect("block indexes")
    };
    let segdir = d.path().join("segments");
    // Two step ranges, so that a cursor in the last tx of range 1 bounds at the first height
    // of range 2 (`FIRST_RANGE_END`), and one cursor bounds at `FRONTIER`.
    let mut m = Manifest::new("mainnet01");
    let mut first_txnum = 0;
    for (start, end) in [(0, FIRST_RANGE_END), (FIRST_RANGE_END, FRONTIER)] {
        let mut w = RangeWriter::create(
            &segdir,
            &d.path().join(format!("etl-{start}")),
            "mainnet01",
            RangeKind::Step,
            start,
            end,
            first_txnum,
            WriteOpts::new(1, 0),
        )
        .unwrap();
        for &(c, h, shapes) in BLOCKS.iter().filter(|b| (start..end).contains(&b.1)) {
            w.push(&index(c, h, shapes, &mut dict)).unwrap();
        }
        let entry = w.finish().unwrap();
        first_txnum = entry.end_txnum();
        m.push(entry).unwrap();
    }
    m.commit(&segdir).unwrap();
    t.set_frozen_frontier(FRONTIER).unwrap();
    t.set_next_txnum(first_txnum).unwrap();
    for &(c, h, shapes) in BLOCKS.iter().filter(|b| b.1 >= FRONTIER) {
        let ib = index(c, h, shapes, &mut dict);
        kdx_hot::tail::apply(&mut t, &ib, &mut dict).unwrap();
    }
    for h in 13..13 + extra {
        let ib = index(0, h, &[Shape::One], &mut dict);
        kdx_hot::tail::apply(&mut t, &ib, &mut dict).unwrap();
    }
    let alice = dict.intern(DictKind::Account, ALICE);
    let coin = dict.intern(DictKind::Module, "coin");
    let ledger = dict.intern(
        DictKind::Module,
        &format!("{}.{}", NFT_LEDGER.0, NFT_LEDGER.1),
    );
    dict.persist(&mut t).unwrap();
    for c in [0u8, 1] {
        let row = BalanceRow {
            absent: false,
            balance: "1.0".into(),
            guard: [0; 16],
            as_of: 12,
            reconciled_at: 12,
            unverified: false,
        };
        t.put(Table::Balances, &bal_key(alice, coin, c), &row.encode())
            .unwrap();
    }
    // Rows the node holds at 0: alice's coin on chain 2, every row of `DRAINED`, Bob's coin on
    // chain 0 (Bob has transfers) and the miner's coin on chain 0. They exist on chain, so
    // they are listed at 0, as stock lists them.
    let drained = dict.intern(DictKind::Account, DRAINED);
    let bob = dict.intern(DictKind::Account, BOB);
    let miner = dict.intern(DictKind::Account, "k:miner");
    dict.persist(&mut t).unwrap();
    for (a, c, bal) in [
        (alice, 2u8, "0.0"),
        (drained, 0, "0.000000000000"),
        (drained, 1, "0"),
        (bob, 0, "0.0"),
        (miner, 0, "0.0"),
    ] {
        let row = BalanceRow {
            absent: false,
            balance: bal.into(),
            guard: [0; 16],
            as_of: 12,
            reconciled_at: 12,
            unverified: false,
        };
        t.put(Table::Balances, &bal_key(a, coin, c), &row.encode())
            .unwrap();
    }
    // Carol's rows for `balance(...)`'s TRANSFER rule: Carol has coin and kaddex.kdx
    // transfers on chain 0 and coin transfers on chain 1, none on chains 2 to 4. Rows at 0
    // without a transfer on their chain were written by something else (create-account,
    // rotate): stock's `balance(...)` leaves them out.
    let carol = dict.intern(DictKind::Account, CAROL);
    let kdx = dict.intern(DictKind::Module, "kaddex.kdx");
    dict.persist(&mut t).unwrap();
    for (m, c, bal) in [
        (coin, 0u8, "0.0"),
        (coin, 1, "2.0"),
        (coin, 2, "0.0"),
        (coin, 3, "1.0"),
        (coin, 4, "0.0"),
        (kdx, 0, "0.0"),
        (kdx, 2, "0.0"),
    ] {
        let row = BalanceRow {
            absent: false,
            balance: bal.into(),
            guard: [0; 16],
            as_of: 12,
            reconciled_at: 12,
            unverified: false,
        };
        t.put(Table::Balances, &bal_key(carol, m, c), &row.encode())
            .unwrap();
    }
    // Rows the node reported absent (no row on chain): alice's coin on chain 3, `GONE`'s only.
    let gone = dict.intern(DictKind::Account, GONE);
    dict.persist(&mut t).unwrap();
    for (a, c) in [(alice, 3u8), (gone, 0)] {
        let row = BalanceRow {
            absent: true,
            balance: String::new(),
            guard: [0; 16],
            as_of: 12,
            reconciled_at: 12,
            unverified: false,
        };
        t.put(Table::Balances, &bal_key(a, coin, c), &row.encode())
            .unwrap();
    }
    kdx_hot::nft::upsert_event(&mut t, alice, "t:2", 0, ledger, 12, "1.0").unwrap();
    // Every tx carries its `sndr` posting (the tail wrote them, the steps hold `sndr.pst`):
    // sender lists read the sender index (FIX-2).
    t.set_meta_u64(kdx_hot::env::meta::SNDR_HOT, 1).unwrap();
    t.commit().unwrap();

    let store = Arc::new(Store::open(hot, d.path()).unwrap());
    let settings = ApiSettings {
        network: "mainnet01".into(),
        deadline: Duration::from_secs(5),
        max_in_flight: 16,
        max_body_bytes: 8 << 20,
        v1txs_max_body_bytes: 8 << 20,
        v1txs_deadline: std::time::Duration::from_secs(30),
        rate_per_ip: 20,
        rate_burst: 40,
        trusted_proxies: Vec::new(),
        read_threads: 2,
        ready_max_lag: 10,
        max_event_lists: 64,
    };
    let st = Api::new(store.clone(), settings, TipWatch::default()).state();
    Fx {
        _dir: d,
        schema: build_schema(st, gs, None),
        store,
        rks,
    }
}

/// One cursor-paged list: `query` holds `ARGS` (the page arguments) and `SEL` (the connection
/// selection); `path` leads from `data` to the connection.
struct List {
    query: String,
    path: Vec<&'static str>,
    node: &'static str,
    /// The connection serves `totalCount`.
    total: bool,
}

const TRANSFER: &str = "id requestKey orderIndex amount";
const EVENT: &str = "id requestKey orderIndex qualifiedName";
const TX: &str = "hash";

fn list(query: impl Into<String>, path: &[&'static str], node: &'static str, total: bool) -> List {
    List {
        query: query.into(),
        path: path.to_vec(),
        node,
        total,
    }
}

impl List {
    fn text(&self, args: &str) -> String {
        let total = if self.total { "totalCount " } else { "" };
        let sel = format!(
            "{{ {total}pageInfo {{ hasNextPage hasPreviousPage startCursor endCursor }} \
             edges {{ cursor node {{ {} }} }} }}",
            self.node
        );
        format!(
            "{{ {} }}",
            self.query.replace("ARGS", args).replace("SEL", &sel)
        )
    }
}

async fn run(s: &KdxSchema, q: &str) -> Value {
    let r = s.execute(q).await;
    assert!(r.errors.is_empty(), "{q}: {:?}", r.errors);
    serde_json::to_value(&r.data).unwrap()
}

fn conn<'v>(v: &'v Value, path: &[&str]) -> &'v Value {
    path.iter().fold(v, |v, p| &v[*p])
}

/// The big page: the list's rows (each `{cursor, node}`), checked to be the whole list, with
/// no row twice and `totalCount` equal to its length.
async fn big_page(s: &KdxSchema, l: &List) -> Vec<Value> {
    let q = l.text(&format!("first: {BIG}"));
    let v = run(s, &q).await;
    let c = conn(&v, &l.path);
    let rows = c["edges"].as_array().unwrap_or_else(|| panic!("{q}: {v}"));
    assert!(rows.len() < BIG, "{q}: the fixture list must fit one page");
    assert_eq!(c["pageInfo"]["hasNextPage"], false, "{q}");
    if l.total {
        assert_eq!(c["totalCount"], rows.len(), "{q}");
    }
    let ids: HashSet<String> = rows.iter().map(|r| r["node"].to_string()).collect();
    assert_eq!(ids.len(), rows.len(), "{q}: the big page repeats a row");
    rows.clone()
}

/// One page's `pageInfo`/`totalCount` against the big page; `Err` names what is wrong.
fn check_page(
    l: &List,
    c: &Value,
    edges: &[Value],
    had_cursor: bool,
    (next, prev): (bool, bool),
    full: usize,
) -> Result<(), String> {
    let info = &c["pageInfo"];
    let first = edges.first().map_or(Value::Null, |e| e["cursor"].clone());
    let last = edges.last().map_or(Value::Null, |e| e["cursor"].clone());
    let want = json!({"startCursor": first, "endCursor": last,
                      "hasNextPage": next, "hasPreviousPage": prev});
    let got = json!({"startCursor": info["startCursor"], "endCursor": info["endCursor"],
                     "hasNextPage": info["hasNextPage"],
                     "hasPreviousPage": info["hasPreviousPage"]});
    if got != want {
        return Err(format!(
            "pageInfo {got} != {want} (cursor given: {had_cursor})"
        ));
    }
    if l.total && c["totalCount"] != full {
        return Err(format!("totalCount {} != {full}", c["totalCount"]));
    }
    Ok(())
}

/// Page `l` forward by `k` (`first: k`, then `after: endCursor`) and join the pages.
async fn forward(s: &KdxSchema, l: &List, k: usize, full: usize) -> (Vec<Value>, Vec<String>) {
    let mut rows: Vec<Value> = Vec::new();
    let mut bad = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..=full + 1 {
        let args = match &after {
            None => format!("first: {k}"),
            Some(c) => format!("first: {k}, after: \"{c}\""),
        };
        let q = l.text(&args);
        let v = run(s, &q).await;
        let c = conn(&v, &l.path);
        let edges = c["edges"].as_array().cloned().unwrap_or_default();
        let more = c["pageInfo"]["hasNextPage"].as_bool().unwrap_or(false);
        rows.extend(edges.iter().cloned());
        let want_more = rows.len() < full;
        if let Err(e) = check_page(
            l,
            c,
            &edges,
            after.is_some(),
            (want_more, after.is_some()),
            full,
        ) {
            bad.push(format!("{q}: {e}"));
        }
        if !more || edges.is_empty() {
            return (rows, bad);
        }
        after = c["pageInfo"]["endCursor"].as_str().map(str::to_owned);
    }
    bad.push(format!("k={k}: more pages than rows"));
    (rows, bad)
}

/// Page `l` backward by `k` (`last: k`, then `before: startCursor`) and join the pages.
async fn backward(s: &KdxSchema, l: &List, k: usize, full: usize) -> (Vec<Value>, Vec<String>) {
    let mut rows: Vec<Value> = Vec::new();
    let mut bad = Vec::new();
    let mut before: Option<String> = None;
    for _ in 0..=full + 1 {
        let args = match &before {
            None => format!("last: {k}"),
            Some(c) => format!("last: {k}, before: \"{c}\""),
        };
        let q = l.text(&args);
        let v = run(s, &q).await;
        let c = conn(&v, &l.path);
        let edges = c["edges"].as_array().cloned().unwrap_or_default();
        let more = c["pageInfo"]["hasPreviousPage"].as_bool().unwrap_or(false);
        rows.splice(0..0, edges.iter().cloned());
        let want_more = rows.len() < full;
        if let Err(e) = check_page(
            l,
            c,
            &edges,
            before.is_some(),
            (before.is_some(), want_more),
            full,
        ) {
            bad.push(format!("{q}: {e}"));
        }
        if !more || edges.is_empty() {
            return (rows, bad);
        }
        before = c["pageInfo"]["startCursor"].as_str().map(str::to_owned);
    }
    bad.push(format!("k={k}: more pages than rows"));
    (rows, bad)
}

/// `(requestKey, orderIndex)` of each row, for readable diffs.
fn ids(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .map(|r| {
            let n = &r["node"];
            match n.get("orderIndex") {
                Some(o) => format!("{}#{o}", n["requestKey"].as_str().unwrap_or("?")),
                None => n["hash"].as_str().unwrap_or("?").to_owned(),
            }
        })
        .collect()
}

/// Join the pages of `l` at each size in `ks` (forward, and backward when `back`) and compare
/// with the big page: same rows (cursor and node), same order, no repeats, and every page's
/// `pageInfo` and `totalCount` right. Returns the failures.
async fn joins(s: &KdxSchema, l: &List, ks: &[usize], back: bool) -> Vec<String> {
    let full = big_page(s, l).await;
    let mut fails = Vec::new();
    for &k in ks {
        let mut dirs = vec![("forward", forward(s, l, k, full.len()).await)];
        if back {
            dirs.push(("backward", backward(s, l, k, full.len()).await));
        }
        for (dir, (rows, bad)) in dirs {
            if rows != full {
                let (got, want) = (ids(&rows), ids(&full));
                let missing: Vec<&String> = want.iter().filter(|r| !got.contains(r)).collect();
                let extra = got.len() + missing.len() - want.len();
                fails.push(format!(
                    "{} {dir} k={k}: joined {} rows, big page {}; missing {missing:?}, \
                     {extra} extra (none of either: the order differs)",
                    l.query,
                    got.len(),
                    want.len(),
                ));
            }
            fails.extend(bad.into_iter().map(|b| format!("  pageInfo: {b}")));
        }
    }
    fails
}

/// The RED of R-P5P9-62: Alice's transfers paged by 1, 2 and 3 lose no row of a frozen tx
/// whose rows straddle a page boundary (the live `k:d665bcfe…` c2 case).
#[tokio::test]
async fn account_transfers_keep_a_tx_s_rows_across_a_page_boundary() {
    let fx = fixture();
    let l = list(
        format!("transfers(accountName: \"{ALICE}\", ARGS) SEL"),
        &["transfers"],
        TRANSFER,
        true,
    );
    let fails = joins(&fx.schema, &l, &[1, 2, 3], false).await;
    assert!(fails.is_empty(), "{}", fails.join("\n"));
}

/// Every cursor-paged tx-keyed list of the schema (the FIX-PJ sweep), at every page size and
/// both directions. Lists of one row per tx are in the sweep too: they pass unchanged.
#[tokio::test]
async fn every_tx_list_joins_its_pages_at_every_size() {
    let fx = fixture();
    let frozen_rk = &fx.rks[&(0, 6)][1];
    let hot_rk = &fx.rks[&(0, 11)][0];
    let frozen_block = format!("hash: \"{}\"", block_hash(0, 6));
    let hot_block = format!("hash: \"{}\"", block_hash(0, 11));
    let a = format!("accountName: \"{ALICE}\"");
    let a0 = format!("{a}, chainId: \"0\"");
    let coin_transfer = "qualifiedEventName: \"coin.TRANSFER\"";
    let starts = r#"parametersFilter: "{\"array_starts_with\": \"k:alice\"}""#;
    // A root list: `field(args, <page arguments>)`.
    let root = |field: &'static str, args: &str, node: &'static str, total: bool| {
        let sep = if args.is_empty() { "" } else { ", " };
        list(
            format!("{field}({args}{sep}ARGS) SEL"),
            &[field],
            node,
            total,
        )
    };
    // A list nested in an object: `parent(args) { field(<page arguments>) }`.
    let nested =
        |parent: &'static str, args: &str, field: &'static str, node: &'static str, total: bool| {
            let q = format!("{parent}({args}) {{ {field}(ARGS) SEL }}");
            list(q, &[parent, field], node, total)
        };
    // A list of one tx's rows: `transaction(requestKey) { result { field } }`.
    let in_tx = |rk: &str, field: &'static str, node: &'static str, total: bool| {
        let q = format!(
            "transaction(requestKey: \"{rk}\") {{ result {{ ... on TransactionResult \
             {{ {field}(ARGS) SEL }} }} }}"
        );
        list(q, &["transaction", "result", field], node, total)
    };
    let by = |k: &str, v: &str| format!("{k}: \"{v}\"");
    let lists = vec![
        // Root `transfers`.
        root("transfers", &a, TRANSFER, true),
        root(
            "transfers",
            &format!("{a}, fungibleName: \"coin\""),
            TRANSFER,
            true,
        ),
        root("transfers", &a0, TRANSFER, true),
        root("transfers", "fungibleName: \"coin\"", TRANSFER, true),
        root("transfers", &format!("{a}, isNFT: true"), TRANSFER, true),
        root("transfers", &by("requestKey", frozen_rk), TRANSFER, true),
        root(
            "transfers",
            &by("blockHash", &block_hash(0, 6)),
            TRANSFER,
            true,
        ),
        root(
            "transfers",
            &by("blockHash", &block_hash(0, 11)),
            TRANSFER,
            true,
        ),
        root("transfers", "", TRANSFER, true),
        // Root `events`.
        root("events", coin_transfer, EVENT, true),
        root("events", &format!("{coin_transfer}, {starts}"), EVENT, true),
        root(
            "events",
            &format!("{coin_transfer}, minHeight: 4, maxHeight: 11"),
            EVENT,
            true,
        ),
        root("events", "moduleName: \"coin\"", EVENT, true),
        root(
            "events",
            &format!("{coin_transfer}, {}", by("requestKey", frozen_rk)),
            EVENT,
            true,
        ),
        root(
            "events",
            &format!("{coin_transfer}, {}", by("blockHash", &block_hash(0, 6))),
            EVENT,
            true,
        ),
        // Root `transactions` and `transactionsByPublicKey` (one row per tx).
        root("transactions", &a, TX, true),
        root("transactions", "fungibleName: \"coin\"", TX, true),
        root(
            "transactions",
            &format!("{a}, fungibleName: \"coin\""),
            TX,
            true,
        ),
        root(
            "transactions",
            &by("blockHash", &block_hash(0, 6)),
            TX,
            true,
        ),
        root("transactions", &by("requestKey", frozen_rk), TX, true),
        root("transactions", "", TX, true),
        // `isCoinbase: true` adds the coinbases to the same lists (G4 class 1).
        root("transactions", "isCoinbase: true", TX, true),
        root(
            "transactions",
            "isCoinbase: true, chainId: \"0\", minHeight: 4, maxHeight: 11",
            TX,
            true,
        ),
        root(
            "transactions",
            &format!("isCoinbase: true, {}", by("blockHash", &block_hash(0, 6))),
            TX,
            true,
        ),
        root(
            "transactionsByPublicKey",
            &by("publicKey", ALICE_KEY),
            TX,
            true,
        ),
        // Nested in a block.
        nested("block", &frozen_block, "transactions", TX, true),
        nested("block", &frozen_block, "events", EVENT, true),
        nested("block", &hot_block, "events", EVENT, true),
        // Nested in an account.
        nested("fungibleAccount", &a, "transfers", TRANSFER, false),
        nested("fungibleAccount", &a, "transactions", TX, true),
        nested("fungibleChainAccount", &a0, "transfers", TRANSFER, false),
        nested("fungibleChainAccount", &a0, "transactions", TX, true),
        nested("nonFungibleAccount", &a, "transactions", TX, true),
        nested("nonFungibleChainAccount", &a0, "transactions", TX, true),
        // Nested in one tx.
        in_tx(frozen_rk, "transfers", TRANSFER, false),
        in_tx(frozen_rk, "events", EVENT, true),
        in_tx(hot_rk, "events", EVENT, true),
    ];
    let mut summary = Vec::new();
    let mut fails = Vec::new();
    for l in &lists {
        let n = big_page(&fx.schema, l).await.len();
        assert!(n > 0, "{}: empty in the fixture", l.query);
        let ks: Vec<usize> = (1..=n + 1).collect();
        let f = joins(&fx.schema, l, &ks, true).await;
        if !f.is_empty() {
            summary.push(format!("{} rows {n}: {} failures", l.query, f.len()));
        }
        fails.extend(f);
    }
    fails.truncate(40);
    assert!(
        summary.is_empty(),
        "lists that lose, repeat or misplace rows:\n{}\nfirst failures:\n{}",
        summary.join("\n"),
        fails.join("\n")
    );
}

/// The big page keeps the list order (R3E-8, R-P5P9-19): height descending, then node order
/// inside a height (chain, then position in the block, descending), then the row's position
/// in the tx, descending.
#[tokio::test]
async fn big_page_order_is_height_then_node_order_then_sub_row() {
    let fx = fixture();
    let mut blocks: Vec<&(u8, u32, &[Shape])> = BLOCKS.iter().collect();
    blocks.sort_by_key(|b| std::cmp::Reverse((b.1, b.0)));
    // `rows(shape)`: the positions of the listed rows in a tx of that shape, ascending.
    let want = |rows: fn(Shape) -> &'static [u32]| -> Vec<String> {
        let mut out = Vec::new();
        for &&(c, h, shapes) in &blocks {
            for (i, s) in shapes.iter().enumerate().rev() {
                for p in rows(*s).iter().rev() {
                    out.push(format!("{}#{p}", fx.rks[&(c, h)][i]));
                }
            }
        }
        out
    };
    // Alice's fungible transfers.
    let l = list(
        format!("transfers(accountName: \"{ALICE}\", ARGS) SEL"),
        &["transfers"],
        TRANSFER,
        true,
    );
    assert_eq!(
        ids(&big_page(&fx.schema, &l).await),
        want(|s| match s {
            Shape::Many => &[0, 1, 3, 4, 5],
            Shape::Nft => &[0, 4],
            Shape::One => &[0],
            Shape::Note => &[],
        })
    );
    // The coin.TRANSFER events.
    let l = list(
        "events(qualifiedEventName: \"coin.TRANSFER\", ARGS) SEL",
        &["events"],
        EVENT,
        true,
    );
    assert_eq!(
        ids(&big_page(&fx.schema, &l).await),
        want(|s| match s {
            Shape::Many => &[0, 1, 4, 5],
            Shape::Nft => &[0, 4],
            Shape::One => &[0],
            Shape::Note => &[],
        })
    );
}

/// Cursor compatibility: the cursor format is unchanged (version 1: tx key, the row's
/// position in the tx, block fingerprint), so a cursor issued before the fix resumes at its
/// row; a damaged one is refused with the existing `invalid cursor` error.
#[tokio::test]
async fn cursors_keep_their_format_and_bad_ones_are_refused() {
    let fx = fixture();
    let q = format!(
        "{{ transfers(accountName: \"{ALICE}\", first: 2) \
         {{ pageInfo {{ endCursor }} edges {{ node {{ requestKey orderIndex }} }} }} }}"
    );
    let v = run(&fx.schema, &q).await;
    let end = v["transfers"]["pageInfo"]["endCursor"].as_str().unwrap();
    let c = kdx_api::Cursor::decode(end).unwrap();
    let last = &v["transfers"]["edges"][1]["node"];
    assert_eq!(
        i64::from(c.sub),
        last["orderIndex"].as_i64().unwrap(),
        "{v}"
    );
    // The same cursor rebuilt field by field, as the code before the fix encoded it.
    let rebuilt = kdx_api::Cursor {
        key: c.key,
        sub: c.sub,
        fp: c.fp,
    }
    .encode();
    assert_eq!(rebuilt, end);
    for bad in [&end[1..], "AAAA", "not a cursor"] {
        let q = format!(
            "{{ transfers(accountName: \"{ALICE}\", first: 2, after: \"{bad}\") \
             {{ edges {{ cursor }} }} }}"
        );
        let r = fx.schema.execute(q.as_str()).await;
        let msg = r
            .errors
            .first()
            .map(|e| e.message.clone())
            .unwrap_or_default();
        assert!(msg.contains("invalid cursor"), "{bad}: {:?}", r.errors);
    }
}

/// The cursor of Alice's transfer at order index 5 of the first user tx of block 6 on chain 1
/// (`6/1/1`, sub 5), as a version-1 cursor minted before the fix encoded it: version 1, tx key
/// (height 6, chain 1, position 1), sub 5, fingerprint of that block's hash. The fixture's keys
/// and hashes are fixed, so the text is too; it does not come from the code under test.
const GOLDEN_CURSOR: &str = "AQAAAAYBAAEAAAWBBrCw";

/// A cursor minted before the fix resumes at its row: the rest of its tx (orders 4, 3, 1), in
/// a frozen block. The code now mints the same text for that row.
#[tokio::test]
async fn a_cursor_minted_before_the_fix_resumes_at_its_row() {
    let fx = fixture();
    let rk = &fx.rks[&(1, 6)][0];
    let page = |args: &str| {
        format!(
            "{{ transfers(accountName: \"{ALICE}\", {args}) \
             {{ edges {{ cursor node {{ requestKey orderIndex }} }} }} }}"
        )
    };
    // Today's code mints the golden text for that row.
    let v = run(&fx.schema, &page("first: 100")).await;
    let minted = v["transfers"]["edges"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["node"]["requestKey"] == rk.as_str() && e["node"]["orderIndex"] == 5)
        .unwrap_or_else(|| panic!("{v}"));
    assert_eq!(minted["cursor"], GOLDEN_CURSOR);
    // `after:` the golden cursor gives the next rows of the same tx.
    let v = run(
        &fx.schema,
        &page(&format!("first: 3, after: \"{GOLDEN_CURSOR}\"")),
    )
    .await;
    let rows = v["transfers"]["edges"].as_array().unwrap();
    let got: Vec<String> = rows
        .iter()
        .map(|e| format!("{}#{}", e["node"]["requestKey"], e["node"]["orderIndex"]))
        .collect();
    let want: Vec<String> = [4, 3, 1].iter().map(|o| format!("\"{rk}\"#{o}")).collect();
    assert_eq!(got, want, "{v}");
}

/// Well-formed cursors that no page minted: the fingerprint of a stored block (so the reorg
/// check passes) with a key or sub the list never issues. Each answers at most `n` rows and
/// no error, except `cursor invalidated by reorg` where the block is not the stored one.
#[tokio::test]
async fn crafted_well_formed_cursors_answer_at_most_a_page() {
    let fx = fixture();
    // The fingerprint of block `(c, h)`: the first 4 bytes of its hash (`hash_of`).
    let fp = |c: u8, h: u32| u32::from_be_bytes([0x80 | c, h as u8, 0xB0, 0xB0]);
    let at = TxKey::new(6, 1, 1);
    let crafted: Vec<(&str, kdx_api::Cursor)> = vec![
        (
            "sub past the end of the tx",
            kdx_api::Cursor {
                key: at,
                sub: 99,
                fp: fp(1, 6),
            },
        ),
        (
            "sub u16::MAX",
            kdx_api::Cursor {
                key: at,
                sub: u16::MAX,
                fp: fp(1, 6),
            },
        ),
        (
            "pos past the block",
            kdx_api::Cursor {
                key: TxKey::new(6, 1, 50),
                sub: 0,
                fp: fp(1, 6),
            },
        ),
        (
            "non-zero low byte",
            kdx_api::Cursor {
                key: TxKey(at.0 | 0x01),
                sub: 3,
                fp: fp(1, 6),
            },
        ),
        (
            "non-zero low byte, hot block",
            kdx_api::Cursor {
                key: TxKey(TxKey::new(11, 0, 1).0 | 0xFF),
                sub: 3,
                fp: fp(0, 11),
            },
        ),
        (
            "fp 0 at an empty position",
            kdx_api::Cursor {
                key: TxKey::new(7, 0, 0),
                sub: 0,
                fp: 0,
            },
        ),
        (
            "fp 0 at a stored block (reorg)",
            kdx_api::Cursor {
                key: at,
                sub: 1,
                fp: 0,
            },
        ),
        (
            "key u64::MAX",
            kdx_api::Cursor {
                key: TxKey(u64::MAX),
                sub: u16::MAX,
                fp: 0,
            },
        ),
        (
            "key u64::MAX, fp u32::MAX",
            kdx_api::Cursor {
                key: TxKey(u64::MAX),
                sub: u16::MAX,
                fp: u32::MAX,
            },
        ),
        (
            "key 0",
            kdx_api::Cursor {
                key: TxKey(0),
                sub: 0,
                fp: 0,
            },
        ),
    ];
    let n = 3;
    let (mut bad, mut answered, mut reorged) = (Vec::new(), 0, 0);
    for field in [
        "transfers(accountName: \"k:alice\"",
        "events(qualifiedEventName: \"coin.TRANSFER\"",
        "transactions(accountName: \"k:alice\"",
    ] {
        for (what, c) in &crafted {
            let tok = c.encode();
            for args in [
                format!("first: {n}, after: \"{tok}\""),
                format!("last: {n}, before: \"{tok}\""),
            ] {
                let q = format!("{{ {field}, {args}) {{ edges {{ cursor }} }} }}");
                let r = fx.schema.execute(q.as_str()).await;
                let name = field.split('(').next().unwrap();
                if let Some(e) = r.errors.first() {
                    if !e.message.contains(kdx_api::cursor::REORGED) {
                        bad.push(format!("{what}, {args}: {name}: error {}", e.message));
                    }
                    reorged += 1;
                    continue;
                }
                answered += 1;
                let v = serde_json::to_value(&r.data).unwrap();
                let rows = v[name]["edges"].as_array().map_or(0, Vec::len);
                if rows > n {
                    bad.push(format!("{what}, {args}: {name}: {rows} rows for n = {n}"));
                }
            }
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
    // Both outcomes occur, so the table is not vacuous.
    assert!(
        answered > 0 && reorged > 0,
        "{answered} answered, {reorged} reorged"
    );
}

/// `totalCount` is exact, as stock's (GQ-10, owner ruling 2026-11-26): no 10,000 cap and no
/// count budget. It runs to the request deadline; a count the deadline cuts answers a
/// `COUNT_TIMEOUT` error at the field, never a number.
mod exact_count {
    use super::*;

    /// Lists whose count is checked against their full walk on the long fixture: the account
    /// lists the old budget cut, with the key filters (chain, heights) and row rules.
    const LISTS: [&str; 8] = [
        "transfers(accountName: \"k:alice\", ARGS)",
        "transfers(accountName: \"k:alice\", fungibleName: \"coin\", ARGS)",
        "transfers(accountName: \"k:alice\", chainId: \"0\", ARGS)",
        "transactions(accountName: \"k:alice\", ARGS)",
        "transactions(accountName: \"k:alice\", minHeight: 100, maxHeight: 5000, ARGS)",
        "transactions(accountName: \"k:alice\", isCoinbase: true, ARGS)",
        "transactionsByPublicKey(publicKey: \"alice-key\", ARGS)",
        "events(qualifiedEventName: \"coin.TRANSFER\", ARGS)",
    ];

    fn field(l: &str) -> &str {
        &l[..l.find('(').unwrap()]
    }

    async fn total(s: &KdxSchema, l: &str) -> i64 {
        let q = format!("{{ {} {{ totalCount }} }}", l.replace("ARGS", "first: 1"));
        run(s, &q).await[field(l)]["totalCount"].as_i64().unwrap()
    }

    /// Rows of the full walk of `l`, pages of 1,000.
    async fn walk(s: &KdxSchema, l: &str) -> i64 {
        let (mut n, mut after) = (0i64, None::<String>);
        loop {
            let args = match &after {
                None => "first: 1000".to_string(),
                Some(c) => format!("first: 1000, after: \"{c}\""),
            };
            let q = format!(
                "{{ {} {{ pageInfo {{ hasNextPage endCursor }} edges {{ cursor }} }} }}",
                l.replace("ARGS", &args)
            );
            let v = run(s, &q).await;
            let c = &v[field(l)];
            n += c["edges"].as_array().unwrap().len() as i64;
            if c["pageInfo"]["hasNextPage"] != true {
                return n;
            }
            after = c["pageInfo"]["endCursor"].as_str().map(str::to_owned);
        }
    }

    /// Above the old 10,000 cap the count is the true one: the long account's transfers and
    /// sends are counted in full, and each equals its full walk.
    #[tokio::test]
    async fn a_count_above_ten_thousand_is_exact() {
        let fx = fixture_big(GraphqlSettings::default(), 10_050);
        for l in &LISTS[..2] {
            let n = total(&fx.schema, l).await;
            assert!(n > 10_000, "{l}: {n} is above the old cap");
            assert_eq!(n, walk(&fx.schema, l).await, "{l}");
        }
    }

    /// Every list's count equals the rows its full walk lists, filters included, on a long
    /// account (thousands of hot txs beside the frozen ranges).
    #[tokio::test]
    async fn the_count_equals_the_full_walk_with_filters() {
        let fx = fixture_big(GraphqlSettings::default(), 3_000);
        for l in LISTS {
            let n = total(&fx.schema, l).await;
            assert!(n > 0, "{l}: the fixture list is not empty");
            assert_eq!(n, walk(&fx.schema, l).await, "{l}");
        }
    }

    /// After a reorg rolls blocks back (and other blocks take their place) every count is
    /// still the length of its full walk, and it moved with the rollback.
    #[tokio::test]
    async fn the_count_follows_a_reorg_rollback() {
        let fx = fixture_big(GraphqlSettings::default(), 400);
        let before = total(&fx.schema, LISTS[0]).await;
        let mut t = fx.store.hot.rw().unwrap();
        let undone = kdx_hot::tail::unwind_to(&mut t, 0, 300).unwrap();
        assert!(undone > 0);
        let mut dict = HotDict::load(&t).unwrap();
        // The new fork: half as many blocks, each with a `Many` tx (five transfers).
        for h in 301..301 + undone / 2 {
            let (b, _) = decoded(0, h, &[Shape::Many]);
            let ib = index_block(&b, &VerifyReport::default(), &mut dict).unwrap();
            kdx_hot::tail::apply(&mut t, &ib, &mut dict).unwrap();
        }
        dict.persist(&mut t).unwrap();
        t.commit().unwrap();
        let after = total(&fx.schema, LISTS[0]).await;
        assert_ne!(
            after, before,
            "the rollback changed the account's transfers"
        );
        for l in LISTS {
            assert_eq!(total(&fx.schema, l).await, walk(&fx.schema, l).await, "{l}");
        }
    }

    /// A count the deadline cuts is a `COUNT_TIMEOUT` error at `totalCount`, never a number
    /// and never a response flag. The same count with time is exact.
    #[tokio::test]
    async fn a_count_cut_by_the_deadline_is_an_error() {
        let fx = fixture_big(GraphqlSettings::default(), 2_000);
        for l in LISTS {
            let q = format!("{{ {} {{ totalCount }} }}", l.replace("ARGS", "first: 1"));
            let req = async_graphql::Request::new(q.clone()).data(kdx_graphql::ReqCtx {
                deadline: std::time::Instant::now(),
            });
            let r = fx.schema.execute(req).await;
            assert!(!r.errors.is_empty(), "{q}: a cut count must fail");
            let codes: Vec<String> = r
                .errors
                .iter()
                .filter_map(|e| e.extensions.as_ref()?.get("code").map(|c| c.to_string()))
                .collect();
            assert!(
                codes
                    .iter()
                    .all(|c| c.contains("COUNT_TIMEOUT") || c.contains("TIMEOUT")),
                "{q}: {codes:?}"
            );
            let d = serde_json::to_value(&r.data).unwrap();
            assert!(
                d.get(field(l))
                    .is_none_or(|c| c.is_null() || c["totalCount"].is_null()),
                "{q}: no number is answered: {d}"
            );
            assert!(r.extensions.is_empty(), "{q}: {:?}", r.extensions);
        }
        // A spent deadline on a list with a page still serves no partial count.
        let q = "{ transfers(accountName: \"k:alice\", first: 2) { totalCount edges { cursor } } }";
        let req = async_graphql::Request::new(q).data(kdx_graphql::ReqCtx {
            deadline: std::time::Instant::now(),
        });
        let r = fx.schema.execute(req).await;
        assert!(!r.errors.is_empty());
        let d = serde_json::to_value(&r.data).unwrap();
        assert!(
            d["transfers"].is_null() || d["transfers"]["totalCount"].is_null(),
            "{d}"
        );
    }

    /// Timing of exact counts on a long account, run by hand (`--ignored --nocapture`, best in
    /// release): `KDX_COUNT_BENCH_TXS` extra txs (default 50,000). Prints the time and answer
    /// of each list's `totalCount`, cold (first) and warm.
    #[tokio::test]
    #[ignore = "timing run"]
    async fn timing_of_exact_counts_on_a_long_account() {
        let n: u32 = std::env::var("KDX_COUNT_BENCH_TXS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50_000);
        let fx = fixture_big(GraphqlSettings::default(), n);
        for l in LISTS {
            for round in 0..2 {
                let t = Instant::now();
                let rows = total(&fx.schema, l).await;
                println!(
                    "{l:<80} round {round}: {:>8.1} ms, totalCount {rows}",
                    t.elapsed().as_secs_f64() * 1e3
                );
            }
        }
    }
}

/// FIX-3 part C: no page cap (stock has none; its complexity gate bounds `first`): a page of
/// 500 answers 500 rows, and an absent `first`/`last` is stock's 20.
#[tokio::test]
async fn pages_are_served_in_full_and_default_to_20() {
    let fx = fixture_big(GraphqlSettings::default(), 600);
    let q = |args: &str| {
        format!(
            "{{ transactions(accountName: \"{ALICE}\"{args}) {{ pageInfo {{ hasNextPage }} edges {{ node {{ hash }} }} }} }}"
        )
    };
    let v = run(&fx.schema, &q(", first: 500")).await;
    let c = &v["transactions"];
    assert_eq!(c["edges"].as_array().map(Vec::len), Some(500), "{v}");
    assert_eq!(c["pageInfo"]["hasNextPage"], true);
    let v = run(&fx.schema, &q(", last: 500")).await;
    assert_eq!(
        v["transactions"]["edges"].as_array().map(Vec::len),
        Some(500)
    );
    let v = run(&fx.schema, &q("")).await;
    assert_eq!(
        v["transactions"]["edges"].as_array().map(Vec::len),
        Some(20)
    );
    // A block list too (it had a wide clamp of 200).
    let v = run(
        &fx.schema,
        "{ blocksFromHeight(startHeight: 0, chainIds: [\"0\"], first: 500) { edges { node { height } } } }",
    )
    .await;
    assert_eq!(
        v["blocksFromHeight"]["edges"].as_array().map(Vec::len),
        Some(500),
        "{v}"
    );
}

/// Runs `q` with `vars` and answers the whole response (data and errors).
async fn run_vars(s: &KdxSchema, q: &str, vars: Value) -> Value {
    let r = s
        .execute(
            async_graphql::Request::new(q).variables(async_graphql::Variables::from_json(vars)),
        )
        .await;
    serde_json::to_value(&r).unwrap()
}

fn chain_ids_of(v: &Value) -> Vec<String> {
    v["data"]["fungibleChainAccounts"]
        .as_array()
        .unwrap_or_else(|| panic!("a list: {v}"))
        .iter()
        .map(|r| r["chainId"].as_str().unwrap().to_owned())
        .collect()
}

/// FIX-4 item 1: `fungibleChainAccounts` with `chainIds` absent (a declared variable sent
/// without a value) or `[]` lists every chain's row, and no rows is `[]`, never `null`.
#[tokio::test]
async fn fungible_chain_accounts_list_every_chain_and_never_null() {
    let fx = fixture();
    let q = "query($a: String!, $c: [String!]) { fungibleChainAccounts(accountName: $a, chainIds: $c) { chainId balance } }";
    for vars in [
        json!({"a": ALICE}),
        json!({"a": ALICE, "c": null}),
        json!({"a": ALICE, "c": []}),
    ] {
        let v = run_vars(&fx.schema, q, vars.clone()).await;
        assert!(v.get("errors").is_none(), "{vars}: {v}");
        assert_eq!(chain_ids_of(&v), ["0", "1", "2"], "{vars}: {v}");
    }
    let v = run(
        &fx.schema,
        &format!("{{ fungibleChainAccounts(accountName: \"{ALICE}\") {{ chainId }} }}"),
    )
    .await;
    assert_eq!(v["fungibleChainAccounts"].as_array().map(Vec::len), Some(3));
    // One chain asked: its row only.
    let v = run_vars(&fx.schema, q, json!({"a": ALICE, "c": ["1"]})).await;
    assert_eq!(chain_ids_of(&v), ["1"], "{v}");
    // No row on the asked chain, or an unknown account: an empty list.
    for vars in [
        json!({"a": ALICE, "c": ["13"]}),
        json!({"a": "k:nobody"}),
        json!({"a": "k:nobody", "c": []}),
    ] {
        let v = run_vars(&fx.schema, q, vars.clone()).await;
        assert!(v.get("errors").is_none(), "{vars}: {v}");
        assert_eq!(v["data"]["fungibleChainAccounts"], json!([]), "{vars}: {v}");
    }
    // `fungibleChainAccount` on a chain without a row stays `null`.
    let v = run(
        &fx.schema,
        &format!(
            "{{ fungibleChainAccount(accountName: \"{ALICE}\", chainId: \"5\") {{ chainId }} }}"
        ),
    )
    .await;
    assert_eq!(v["fungibleChainAccount"], Value::Null, "{v}");
    let v = run(
        &fx.schema,
        &format!(
            "{{ fungibleChainAccount(accountName: \"{ALICE}\", chainId: \"1\") {{ chainId }} }}"
        ),
    )
    .await;
    assert_eq!(v["fungibleChainAccount"]["chainId"], "1", "{v}");
}

/// Zero rows (owner ruling 2026-11-26, stock compatibility): a row that exists on chain is
/// listed at any balance, 0 included, as stock does: in `chainAccounts`,
/// `fungibleChainAccounts`, `fungibleChainAccount` and `balance(...)`. Only a row the node
/// reported absent (no row on chain) is left out.
#[tokio::test]
async fn rows_on_chain_are_listed_at_any_balance() {
    let fx = fixture();
    let chains_of = |v: &Value| -> Vec<String> {
        v.as_array()
            .unwrap()
            .iter()
            .map(|r| r["chainId"].as_str().unwrap().to_owned())
            .collect()
    };
    let v = run(
        &fx.schema,
        &format!(
            "{{ fungibleAccount(accountName: \"{ALICE}\") {{ totalBalance chainAccounts {{ chainId balance }} }} }}"
        ),
    )
    .await;
    assert_eq!(
        chains_of(&v["fungibleAccount"]["chainAccounts"]),
        ["0", "1", "2"],
        "{v}"
    );
    assert_eq!(
        v["fungibleAccount"]["chainAccounts"][2]["balance"],
        json!(0.0),
        "{v}"
    );
    assert_eq!(v["fungibleAccount"]["totalBalance"], json!(2), "{v}");
    let v = run(
        &fx.schema,
        &format!("{{ fungibleChainAccounts(accountName: \"{ALICE}\") {{ chainId }} }}"),
    )
    .await;
    assert_eq!(
        chains_of(&v["fungibleChainAccounts"]),
        ["0", "1", "2"],
        "{v}"
    );
    let v = run(
        &fx.schema,
        &format!(
            "{{ fungibleChainAccount(accountName: \"{ALICE}\", chainId: \"2\") {{ chainId balance }} }}"
        ),
    )
    .await;
    assert_eq!(
        v["fungibleChainAccount"],
        json!({"chainId": "2", "balance": 0.0}),
        "{v}"
    );
    // A row the node reported absent is not on chain: never listed.
    let v = run(
        &fx.schema,
        &format!(
            "{{ fungibleChainAccount(accountName: \"{ALICE}\", chainId: \"3\") {{ chainId }} }}"
        ),
    )
    .await;
    assert_eq!(v["fungibleChainAccount"], Value::Null, "{v}");
    let v = run(
        &fx.schema,
        &format!(
            "{{ balance(accountName: \"{ALICE}\") {{ edges {{ node {{ module chainId }} }} }} }}"
        ),
    )
    .await;
    let rows: Vec<String> = v["balance"]["edges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            format!(
                "{}:{}",
                e["node"]["module"].as_str().unwrap(),
                e["node"]["chainId"]
            )
        })
        .collect();
    // `balance(...)` lists alice's chain-2 row at 0 only if a TRANSFER touched it there;
    // none did (stock's rule), while `fungibleAccount` above lists it.
    assert_eq!(rows, ["coin:\"0\"", "coin:\"1\""], "{v}");
    // `fungibleAccount` lists every row at 0 (funded and drained, or created and never
    // funded).
    let v = run(
        &fx.schema,
        &format!("{{ fungibleAccount(accountName: \"{DRAINED}\") {{ accountName totalBalance chainAccounts {{ chainId }} }} }}"),
    )
    .await;
    assert_eq!(
        chains_of(&v["fungibleAccount"]["chainAccounts"]),
        ["0", "1"],
        "{v}"
    );
    assert_eq!(v["fungibleAccount"]["totalBalance"], json!(0), "{v}");
    let v = run(
        &fx.schema,
        &format!("{{ fungibleChainAccounts(accountName: \"{DRAINED}\") {{ chainId }} }}"),
    )
    .await;
    assert_eq!(chains_of(&v["fungibleChainAccounts"]), ["0", "1"], "{v}");
    let v = run(
        &fx.schema,
        &format!("{{ balance(accountName: \"{DRAINED}\") {{ edges {{ cursor }} }} }}"),
    )
    .await;
    // No TRANSFER touched `DRAINED`'s rows: `balance(...)` lists none of them.
    assert_eq!(
        v["balance"]["edges"].as_array().map(Vec::len),
        Some(0),
        "{v}"
    );
    // An account whose only row is absent does not exist on chain: `null`.
    let v = run(
        &fx.schema,
        &format!("{{ fungibleAccount(accountName: \"{GONE}\") {{ accountName }} }}"),
    )
    .await;
    assert_eq!(v["fungibleAccount"], Value::Null, "{v}");
}

/// `balance(accountName:)` rows of `account` as `module:chain=balance`, in served order.
async fn balance_rows(schema: &KdxSchema, account: &str, args: &str) -> (Vec<String>, Value) {
    let v = run(
        schema,
        &format!(
            "{{ balance(accountName: \"{account}\"{args}) {{ edges {{ cursor node {{ module chainId balance }} }} pageInfo {{ hasNextPage hasPreviousPage }} }} }}"
        ),
    )
    .await;
    let rows = v["balance"]["edges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            let n = &e["node"];
            format!(
                "{}:{}={}",
                n["module"].as_str().unwrap(),
                n["chainId"].as_str().unwrap(),
                n["balance"].as_str().unwrap()
            )
        })
        .collect();
    (rows, v)
}

/// PR #47 I-1: stock's `balance(...)` lists every row above 0 and a row at 0 only when a
/// TRANSFER event touched it, i.e. the account's transfer list in that module has an entry on
/// that chain. A zero row written by create-account or rotate (no transfer there) is left out;
/// `fungibleAccount` still lists it. Pages walk the same filtered rows.
#[tokio::test]
async fn balance_lists_zero_rows_only_where_a_transfer_touched_them() {
    let fx = fixture();
    let (rows, v) = balance_rows(&fx.schema, CAROL, "").await;
    assert_eq!(
        rows,
        [
            // at 0, with coin transfers on chain 0: listed
            "coin:0=0",
            // above 0, with transfers
            "coin:1=2",
            // above 0, no transfer on chain 3: listed (only zero rows are checked)
            "coin:3=1",
            // at 0, with kaddex.kdx transfers on chain 0: listed (the module's own list)
            "kaddex.kdx:0=0",
            // coin:2, coin:4 and kaddex.kdx:2 are at 0 with no transfer there: left out
        ],
        "{v}"
    );
    // `fungibleAccount` (coin) keeps every on-chain row, at any balance.
    let v = run(
        &fx.schema,
        &format!(
            "{{ fungibleAccount(accountName: \"{CAROL}\") {{ chainAccounts {{ chainId }} }} }}"
        ),
    )
    .await;
    let chains: Vec<&str> = v["fungibleAccount"]["chainAccounts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["chainId"].as_str().unwrap())
        .collect();
    assert_eq!(chains, ["0", "1", "2", "3", "4"], "{v}");
    // A chain filter applies the same rule.
    let (rows, v) = balance_rows(&fx.schema, CAROL, ", chainIds: [\"2\", \"3\"]").await;
    assert_eq!(rows, ["coin:3=1"], "{v}");
    let (rows, v) = balance_rows(&fx.schema, CAROL, ", module: \"kaddex.kdx\"").await;
    assert_eq!(rows, ["kaddex.kdx:0=0"], "{v}");
    // Paging forward one row at a time and backward one row at a time walks exactly the
    // listed rows; a left-out row never takes a page slot or flips `hasNextPage`.
    let all = [
        "coin:0=0".to_owned(),
        "coin:1=2".to_owned(),
        "coin:3=1".to_owned(),
        "kaddex.kdx:0=0".to_owned(),
    ];
    for (dir, cur_arg, more) in [
        ("first", "after", "hasNextPage"),
        ("last", "before", "hasPreviousPage"),
    ] {
        let mut seen = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let args = match &cursor {
                None => format!(", {dir}: 1"),
                Some(c) => format!(", {dir}: 1, {cur_arg}: \"{c}\""),
            };
            let (rows, v) = balance_rows(&fx.schema, CAROL, &args).await;
            assert_eq!(rows.len(), 1, "{dir}: {v}");
            seen.push(rows[0].clone());
            cursor = v["balance"]["edges"][0]["cursor"]
                .as_str()
                .map(str::to_owned);
            if !v["balance"]["pageInfo"][more].as_bool().unwrap() {
                break;
            }
            assert!(seen.len() < 10, "{dir}: no end");
        }
        if dir == "last" {
            seen.reverse();
        }
        assert_eq!(seen, all, "{dir}");
    }
}

/// The transfer probe of a zero row is bounded: `balance_touch_keys` transfer keys of one
/// (account, module), newest first. When the cap ends the probe before every zero row's chain
/// was found, the rows still unresolved are kept (listed, the safe side: the row exists on
/// chain) and counted in `kdx_graphql_balance_touch_capped`.
#[tokio::test]
async fn a_capped_transfer_probe_keeps_the_zero_rows_it_could_not_resolve() {
    let fx = fixture_with(GraphqlSettings {
        balance_touch_keys: 1,
        ..GraphqlSettings::default()
    });
    let before = kdx_metrics::get().graphql_balance_touch_capped.get();
    let (rows, v) = balance_rows(&fx.schema, CAROL, "").await;
    // Carol's newest coin and kaddex.kdx transfer keys are both on chain 1 (height 12): one
    // key resolves none of the five zero rows, so all five are kept and counted.
    assert_eq!(
        rows,
        [
            "coin:0=0",
            "coin:1=2",
            "coin:2=0",
            "coin:3=1",
            "coin:4=0",
            "kaddex.kdx:0=0",
            "kaddex.kdx:2=0",
        ],
        "{v}"
    );
    let after = kdx_metrics::get().graphql_balance_touch_capped.get();
    assert!(after >= before + 5, "{before} -> {after}");
    // A cap large enough to read every key gives the exact answer.
    let fx = fixture_with(GraphqlSettings {
        balance_touch_keys: 1000,
        ..GraphqlSettings::default()
    });
    let (rows, v) = balance_rows(&fx.schema, CAROL, "").await;
    assert_eq!(rows.len(), 4, "{v}");
}

/// I-1 (PR #44 review): an account drained to 0 everywhere still resolves through
/// `fungibleAccount` and `node(id:)` (`FungibleAccount` and `FungibleChainAccount` ids), with
/// its rows at 0, so its `transfers` and `transactions` stay reachable. An account with no
/// stored row at all is still `null`.
#[tokio::test]
async fn a_drained_account_keeps_its_history() {
    let fx = fixture();
    let sel = "accountName totalBalance chainAccounts { chainId } transfers(first: 50) { edges { cursor } } transactions(first: 50) { edges { cursor } }";
    let check = |v: &Value, label: &str| {
        assert_eq!(
            v["chainAccounts"],
            json!([{"chainId": "0"}]),
            "{label}: {v}"
        );
        assert_eq!(v["totalBalance"], json!(0), "{label}: {v}");
        let transfers = v["transfers"]["edges"].as_array().expect(label);
        assert!(!transfers.is_empty(), "{label}: {v}");
        assert!(v["transactions"]["edges"].is_array(), "{label}: {v}");
    };
    let v = run(
        &fx.schema,
        &format!("{{ fungibleAccount(accountName: \"{BOB}\") {{ {sel} }} }}"),
    )
    .await;
    check(&v["fungibleAccount"], "fungibleAccount");
    let id = kdx_graphql::ids::encode(&kdx_graphql::ids::NodeKey::FungibleAccount {
        fungible: "coin".into(),
        account: BOB.into(),
    });
    let v = run(
        &fx.schema,
        &format!("{{ node(id: \"{id}\") {{ ... on FungibleAccount {{ {sel} }} }} }}"),
    )
    .await;
    check(&v["node"], "node FungibleAccount");
    // The chain account at 0 resolves by id, with its transfers.
    let id = kdx_graphql::ids::encode(&kdx_graphql::ids::NodeKey::FungibleChainAccount {
        chain: "0".into(),
        fungible: "coin".into(),
        account: BOB.into(),
    });
    let v = run(
        &fx.schema,
        &format!("{{ node(id: \"{id}\") {{ ... on FungibleChainAccount {{ chainId balance transfers(first: 50) {{ edges {{ cursor }} }} transactions(first: 50) {{ edges {{ cursor }} }} }} }} }}"),
    )
    .await;
    assert_eq!(v["node"]["chainId"], "0", "{v}");
    assert_eq!(v["node"]["balance"], json!(0.0), "{v}");
    assert!(
        !v["node"]["transfers"]["edges"]
            .as_array()
            .unwrap()
            .is_empty(),
        "{v}"
    );
    assert!(v["node"]["transactions"]["edges"].is_array(), "{v}");
    // No stored row on that chain, or none at all: `null`.
    for (chain, account) in [("1", BOB), ("0", "k:nobody")] {
        let id = kdx_graphql::ids::encode(&kdx_graphql::ids::NodeKey::FungibleChainAccount {
            chain: chain.into(),
            fungible: "coin".into(),
            account: account.into(),
        });
        let v = run(
            &fx.schema,
            &format!("{{ node(id: \"{id}\") {{ ... on FungibleChainAccount {{ chainId }} }} }}"),
        )
        .await;
        assert_eq!(v["node"], Value::Null, "{chain} {account}: {v}");
    }
    let v = run(
        &fx.schema,
        "{ fungibleAccount(accountName: \"k:nobody\") { accountName } }",
    )
    .await;
    assert_eq!(v["fungibleAccount"], Value::Null, "{v}");
}

/// The review's minor 6: `Block.minerAccount` serves the miner's row though it is at 0.
#[tokio::test]
async fn miner_account_is_served_at_zero() {
    let fx = fixture();
    let v = run(
        &fx.schema,
        &format!(
            "{{ block(hash: \"{}\") {{ minerAccount {{ accountName chainId balance }} }} }}",
            block_hash(0, 12)
        ),
    )
    .await;
    assert_eq!(
        v["block"]["minerAccount"],
        json!({"accountName": "k:miner", "chainId": "0", "balance": 0.0}),
        "{v}"
    );
}

/// FIX-4 item 2: `chainIds: []` on the block lists means every chain, as no `chainIds` does.
#[tokio::test]
async fn empty_chain_ids_on_block_lists_mean_every_chain() {
    let fx = fixture();
    for (field, args) in [
        ("blocksFromHeight", "startHeight: 0"),
        ("blocksFromDepth", "minimumDepth: 0"),
        (
            "completedBlockHeights",
            "completedHeights: false, heightCount: 100",
        ),
    ] {
        let q = |chains: &str| {
            format!(
                "{{ {field}({args}{chains}, first: 50) {{ edges {{ node {{ chainId height }} }} }} }}"
            )
        };
        let all = run(&fx.schema, &q("")).await;
        let empty = run(&fx.schema, &q(", chainIds: []")).await;
        let n = all[field]["edges"].as_array().map_or(0, Vec::len);
        assert!(n > 1, "{field}: {all}");
        let chains: HashSet<String> = all[field]["edges"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["node"]["chainId"].to_string())
            .collect();
        assert!(chains.len() > 1, "{field}: {all}");
        assert_eq!(empty, all, "{field}");
    }
}

/// The ids of the coinbase row and the user tx rows of block `(c, h)` in node order: the
/// coinbase first, then the user txs in payload order, each tx's rows by ascending position.
fn node_order(
    fx: &Fx,
    c: u8,
    h: u32,
    rows: fn(Shape) -> &'static [u32],
    coinbase: bool,
) -> Vec<String> {
    let shapes = BLOCKS.iter().find(|b| (b.0, b.1) == (c, h)).unwrap().2;
    let mut out = Vec::new();
    if coinbase {
        out.push(format!("{}#0", b64url_encode(&hash_of(0xCB, c, h))));
    }
    for (i, s) in shapes.iter().enumerate() {
        for p in rows(*s) {
            out.push(format!("{}#{p}", fx.rks[&(c, h)][i]));
        }
    }
    out
}

/// FIX-4 item 6: a list of one block (`Block.events`, `Block.transactions`, a root list with
/// `blockHash`) is in the node's order: the coinbase (first leaf of the payload's
/// `outputsHash`, executed first) and then the user txs in payload order, rows of a tx by
/// ascending position. Backward pages hold the same order.
#[tokio::test]
async fn one_block_lists_are_in_node_order() {
    let fx = fixture();
    fn every_event(s: Shape) -> &'static [u32] {
        match s {
            Shape::Many => &[0, 1, 2, 3, 4, 5],
            Shape::Nft => &[0, 1, 2, 3, 4],
            Shape::One | Shape::Note => &[0],
        }
    }
    fn coin_transfers(s: Shape) -> &'static [u32] {
        match s {
            Shape::Many => &[0, 1, 4, 5],
            Shape::Nft => &[0, 4],
            Shape::One => &[0],
            Shape::Note => &[],
        }
    }
    // A frozen block and a hot one.
    for (c, h) in [(0u8, 6u32), (0, 11), (1, 12)] {
        let bh = block_hash(c, h);
        let hash = format!("hash: \"{bh}\"");
        let events = list(
            format!("block({hash}) {{ events(ARGS) SEL }}"),
            &["block", "events"],
            EVENT,
            true,
        );
        let want = node_order(&fx, c, h, every_event, true);
        assert_eq!(
            ids(&big_page(&fx.schema, &events).await),
            want,
            "Block.events {c}/{h}"
        );
        // The last page of 3 is the list's last 3 rows, in the same order.
        let v = run(&fx.schema, &events.text("last: 3")).await;
        let tail = ids(conn(&v, &events.path)["edges"].as_array().unwrap());
        assert_eq!(tail, want[want.len() - 3..], "Block.events last 3, {c}/{h}");

        let txs = list(
            format!("block({hash}) {{ transactions(ARGS) SEL }}"),
            &["block", "transactions"],
            TX,
            true,
        );
        let shapes = BLOCKS.iter().find(|b| (b.0, b.1) == (c, h)).unwrap().2;
        let got: Vec<String> = big_page(&fx.schema, &txs)
            .await
            .iter()
            .map(|r| r["node"]["hash"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            got,
            fx.rks[&(c, h)][..shapes.len()],
            "Block.transactions {c}/{h}"
        );
        let got: Vec<String> = big_page(
            &fx.schema,
            &list(
                format!("transactions(blockHash: \"{bh}\", ARGS) SEL"),
                &["transactions"],
                TX,
                true,
            ),
        )
        .await
        .iter()
        .map(|r| r["node"]["hash"].as_str().unwrap().to_owned())
        .collect();
        assert_eq!(got, fx.rks[&(c, h)], "transactions(blockHash) {c}/{h}");

        let root = list(
            format!("events(qualifiedEventName: \"coin.COINBASE\", blockHash: \"{bh}\", ARGS) SEL"),
            &["events"],
            EVENT,
            true,
        );
        assert_eq!(
            ids(&big_page(&fx.schema, &root).await),
            node_order(&fx, c, h, |_| &[], true),
            "events(coin.COINBASE, blockHash) {c}/{h}"
        );
        let root = list(
            format!("events(qualifiedEventName: \"coin.TRANSFER\", blockHash: \"{bh}\", ARGS) SEL"),
            &["events"],
            EVENT,
            true,
        );
        assert_eq!(
            ids(&big_page(&fx.schema, &root).await),
            node_order(&fx, c, h, coin_transfers, false),
            "events(coin.TRANSFER, blockHash) {c}/{h}"
        );
        let root = list(
            format!("transfers(blockHash: \"{bh}\", ARGS) SEL"),
            &["transfers"],
            TRANSFER,
            true,
        );
        let got = ids(&big_page(&fx.schema, &root).await);
        let mut sorted = got.clone();
        let pos = |id: &String| want.iter().position(|w| w == id).unwrap();
        sorted.sort_by_key(pos);
        assert_eq!(got, sorted, "transfers(blockHash) {c}/{h} in node order");
    }
}

/// FIX-4 item 5: an id that cannot be read is `null` (stock's answer), not an error; `nodes`
/// answers `null` in its place and resolves the others.
#[tokio::test]
async fn unreadable_node_ids_are_null() {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use kdx_graphql::ids::{encode, NodeKey};
    let fx = fixture();
    let good = encode(&NodeKey::Block {
        hash: block_hash(0, 6),
    });
    let legacy = STANDARD.encode(format!("[\"Block\",\"{}\"]", block_hash(0, 6)));
    for bad in ["AAAA", "not base64!", "", legacy.as_str(), "QmxvY2s6eA=="] {
        let v = run(&fx.schema, &format!("{{ node(id: \"{bad}\") {{ id }} }}")).await;
        assert_eq!(v, json!({"node": null}), "{bad}");
    }
    let v = run(
        &fx.schema,
        &format!("{{ nodes(ids: [\"{good}\", \"AAAA\", \"{legacy}\"]) {{ id ... on Block {{ height }} }} }}"),
    )
    .await;
    assert_eq!(v, json!({"nodes": [{"id": good, "height": 6}, null, null]}),);
}

/// FIX-4 item 4: `gasLimitEstimate(input: [])` runs no Pact: an empty list, as stock answers.
/// Any input is still `NOT_SUPPORTED`.
#[tokio::test]
async fn gas_limit_estimate_of_no_input_is_an_empty_list() {
    let fx = fixture();
    let v = run(&fx.schema, "{ gasLimitEstimate(input: []) { amount } }").await;
    assert_eq!(v, json!({"gasLimitEstimate": []}));
    let r = fx
        .schema
        .execute("{ gasLimitEstimate(input: [\"{}\"]) { amount } }")
        .await;
    let code = r.errors[0]
        .extensions
        .as_ref()
        .and_then(|x| x.get("code"))
        .cloned();
    assert_eq!(code, Some(async_graphql::Value::from("NOT_SUPPORTED")));
}

/// FIX-4 part B (B3): a `fungibleName` with surrounding spaces names the same module, as in
/// stock (`"coin "` answered the `coin` row there, `null` here); a name of spaces alone is
/// still `BAD_USER_INPUT`.
#[tokio::test]
async fn fungible_name_is_trimmed() {
    let f = fixture();
    let fields = [
        format!("fungibleAccount(accountName: \"{ALICE}\", FN) {{ accountName fungibleName totalBalance chainAccounts {{ chainId balance }} }}"),
        format!("fungibleChainAccount(accountName: \"{ALICE}\", chainId: \"0\", FN) {{ chainId balance fungibleName }}"),
        format!("fungibleChainAccounts(accountName: \"{ALICE}\", FN) {{ chainId balance }}"),
        format!("fungibleAccountsByPublicKey(publicKey: \"{ALICE_KEY}\", FN) {{ accountName }}"),
        format!("fungibleChainAccountsByPublicKey(publicKey: \"{ALICE_KEY}\", chainId: \"0\", FN) {{ accountName }}"),
        format!("transfers(accountName: \"{ALICE}\", FN, first: 50) {{ totalCount edges {{ node {{ requestKey orderIndex amount }} }} }}"),
        "transfers(FN, first: 50) { edges { node { requestKey orderIndex } } }".to_owned(),
    ];
    // FIX-4 part C (M-4): with `blockHash` or `requestKey` too, a name of spaces alone is
    // still refused, not an empty list.
    let rk = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    for src in [
        format!("blockHash: \"{}\"", block_hash(0, 6)),
        format!("requestKey: \"{rk}\""),
    ] {
        let r = f
            .schema
            .execute(format!("{{ transfers({src}, fungibleName: \"  \") {{ edges {{ node {{ requestKey }} }} }} }}"))
            .await;
        let code = r
            .errors
            .first()
            .and_then(|e| e.extensions.as_ref()?.get("code").cloned());
        assert_eq!(
            code,
            Some(async_graphql::Value::String("BAD_USER_INPUT".into())),
            "{src}: {:?}",
            r.errors
        );
    }
    for field in &fields {
        let q = |name: &str| {
            format!(
                "{{ {} }}",
                field.replace("FN", &format!("fungibleName: \"{name}\""))
            )
        };
        let want = run(&f.schema, &q("coin")).await;
        assert!(
            !want.as_object().unwrap().values().any(Value::is_null),
            "{field}: {want}"
        );
        for name in ["coin ", " coin", "\tcoin  "] {
            let r = f.schema.execute(q(name)).await;
            assert!(r.errors.is_empty(), "{field} {name:?}: {:?}", r.errors);
            assert_eq!(
                serde_json::to_value(&r.data).unwrap(),
                want,
                "{field} {name:?}"
            );
        }
        let r = f.schema.execute(q("  ")).await;
        let code = r
            .errors
            .first()
            .and_then(|e| e.extensions.as_ref()?.get("code").cloned());
        assert_eq!(
            code,
            Some(async_graphql::Value::String("BAD_USER_INPUT".into())),
            "{field}: {:?}",
            r.errors
        );
    }
}

/// Suite run 4: `fungibleChainAccounts` lists its rows in the order of the requested
/// `chainIds` (stock's order), not ascending by chain; a repeated chain lists once, at its
/// first place.
#[tokio::test]
async fn fungible_chain_accounts_follow_the_requested_chain_order() {
    let fx = fixture();
    let q = "query($a: String!, $c: [String!]) { fungibleChainAccounts(accountName: $a, chainIds: $c) { chainId } }";
    for (c, want) in [
        (json!(["1", "0"]), vec!["1", "0"]),
        (json!(["0", "1"]), vec!["0", "1"]),
        (json!(["1", "7", "0", "1"]), vec!["1", "0"]),
    ] {
        let v = run_vars(&fx.schema, q, json!({"a": ALICE, "c": c})).await;
        assert!(v.get("errors").is_none(), "{c}: {v}");
        assert_eq!(chain_ids_of(&v), want, "{c}: {v}");
    }
}

/// Suite run 4: an integer id beyond i64 is an unreadable id like any other: `null` for
/// `node` and a `null` entry in `nodes`, with no error (stock answers 200 `null`).
#[tokio::test]
async fn an_integer_id_beyond_i64_reads_as_null() {
    let fx = fixture();
    let big = serde_json::Number::from(9_223_372_036_854_775_808u64);
    let q = "query($id: ID!, $ids: [ID!]!) { node(id: $id) { __typename } nodes(ids: $ids) { __typename } }";
    for id in [
        json!(big),
        json!(u64::MAX),
        json!(-5),
        json!("9223372036854775808"),
    ] {
        let vars = json!({"id": id, "ids": [id, "x"]});
        let v = run_vars(&fx.schema, q, vars.clone()).await;
        assert!(v.get("errors").is_none(), "{vars}: {v}");
        assert_eq!(v["data"]["node"], Value::Null, "{vars}: {v}");
        assert_eq!(v["data"]["nodes"], json!([null, null]), "{vars}: {v}");
    }
    let v = run_vars(
        &fx.schema,
        "{ node(id: 9223372036854775808) { id } nodes(ids: [9223372036854775808, 3]) { id } }",
        json!({}),
    )
    .await;
    assert!(v.get("errors").is_none(), "{v}");
    assert_eq!(v["data"]["nodes"], json!([null, null]), "{v}");
    // Not an integer (a fraction, a bool): still refused.
    for id in [json!(1.5), json!(true)] {
        let v = run_vars(&fx.schema, q, json!({"id": id, "ids": []})).await;
        assert!(v.get("errors").is_some(), "{id}: {v}");
    }
}

/// Suite run 4 (decided: ours stays): an inverted range, `startHeight` = `endHeight` + 1,
/// is an empty list on every chain, not an error (stock answers INTERNAL_SERVER_ERROR
/// "Start height cannot be greater than end height").
#[tokio::test]
async fn blocks_from_height_with_an_inverted_range_is_empty() {
    let fx = fixture();
    for (start, end) in [(7, 6), (11, 10), (1, 0)] {
        for chains in ["", ", chainIds: [\"0\"]", ", chainIds: [\"0\", \"1\"]"] {
            let q = format!(
                "{{ blocksFromHeight(startHeight: {start}, endHeight: {end}{chains}) {{ edges {{ node {{ height }} }} }} }}"
            );
            let v = run_vars(&fx.schema, &q, json!({})).await;
            assert!(v.get("errors").is_none(), "{q}: {v}");
            assert_eq!(
                v["data"]["blocksFromHeight"]["edges"],
                json!([]),
                "{q}: {v}"
            );
        }
    }
    // The same heights, not inverted, hold a block (the fixture has chain 0 at height 6).
    let v = run_vars(
        &fx.schema,
        "{ blocksFromHeight(startHeight: 6, endHeight: 6, chainIds: [\"0\"]) { edges { node { height } } } }",
        json!({}),
    )
    .await;
    assert_eq!(
        v["data"]["blocksFromHeight"]["edges"]
            .as_array()
            .map(Vec::len),
        Some(1),
        "{v}"
    );
}

/// Audit PR-3: GraphQL resolver fixes (F-27, F-28, F-30, F-50, F-51).
mod audit_pr3 {
    use super::*;

    fn code(r: &async_graphql::Response) -> Option<async_graphql::Value> {
        r.errors
            .first()
            .and_then(|e| e.extensions.as_ref()?.get("code").cloned())
    }

    fn bad_input() -> Option<async_graphql::Value> {
        Some(async_graphql::Value::from("BAD_USER_INPUT"))
    }

    fn edges<'v>(v: &'v Value, field: &str) -> &'v Vec<Value> {
        v[field]["edges"].as_array().unwrap()
    }

    /// F-27: `events(parametersFilter)` whose first value is a string in the account
    /// dictionary but not a principal finds a non-`TRANSFER` event naming it, although the
    /// tx has no `acct` posting of that string. A `TRANSFER` filter on the same name keeps
    /// the account's postings (option B) and still lists its transfers.
    #[tokio::test]
    async fn a_non_principal_first_param_of_a_non_transfer_event_is_listed() {
        let fx = fixture();
        let q = |qual: &str| {
            format!(
                r#"{{ events(qualifiedEventName: "{qual}", parametersFilter: "{{\"array_starts_with\": \"{CAROL}\"}}", first: 50) {{ totalCount edges {{ node {{ requestKey }} }} }} }}"#
            )
        };
        let v = run(&fx.schema, &q("coin.NOTE")).await;
        let rks: Vec<&str> = edges(&v, "events")
            .iter()
            .map(|e| e["node"]["requestKey"].as_str().unwrap())
            .collect();
        assert_eq!(rks, [fx.rks[&(1, 11)][0].as_str()], "{v}");
        assert_eq!(v["events"]["totalCount"], 1, "{v}");
        let v = run(&fx.schema, &q("coin.TRANSFER")).await;
        assert!(!edges(&v, "events").is_empty(), "{v}");
    }

    /// F-30 (stock ANDs every supplied filter): `requestKey` with `blockHash` lists the key's
    /// copy in that block only; another block's hash lists nothing, and a malformed one is
    /// `BAD_USER_INPUT`, on `transactions`, `transfers` and `events`.
    #[tokio::test]
    async fn request_key_and_block_hash_both_filter() {
        let fx = fixture();
        let rk = &fx.rks[&(0, 6)][1];
        for (field, extra, sel) in [
            ("transactions", "", "hash"),
            ("transfers", "", "requestKey orderIndex"),
            (
                "events",
                ", qualifiedEventName: \"coin.TRANSFER\"",
                "requestKey orderIndex",
            ),
        ] {
            let q = |bh: Option<&str>| {
                let bh = bh.map_or(String::new(), |h| format!(", blockHash: \"{h}\""));
                format!(
                    "{{ {field}(requestKey: \"{rk}\"{bh}{extra}, first: 50) \
                     {{ totalCount edges {{ node {{ {sel} }} }} }} }}"
                )
            };
            let alone = run(&fx.schema, &q(None)).await;
            assert!(!edges(&alone, field).is_empty(), "{field}: {alone}");
            let right = run(&fx.schema, &q(Some(&block_hash(0, 6)))).await;
            assert_eq!(right, alone, "{field}: the key's own block");
            let wrong = run(&fx.schema, &q(Some(&block_hash(0, 11)))).await;
            assert!(edges(&wrong, field).is_empty(), "{field}: {wrong}");
            assert_eq!(wrong[field]["totalCount"], 0, "{field}: {wrong}");
            let r = fx.schema.execute(q(Some("not-a-hash"))).await;
            assert_eq!(code(&r), bad_input(), "{field}: {:?}", r.errors);
        }
    }

    /// The hashes `transactions(<src>, fungibleName: <m>)` lists (its `totalCount` agrees).
    async fn hashes(s: &KdxSchema, src: String, m: &str) -> Vec<String> {
        let v = run(
            s,
            &format!(
                "{{ transactions({src}, fungibleName: \"{m}\", first: 50) \
                 {{ totalCount edges {{ node {{ hash }} }} }} }}"
            ),
        )
        .await;
        let hs: Vec<String> = edges(&v, "transactions")
            .iter()
            .map(|e| e["node"]["hash"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(v["transactions"]["totalCount"], hs.len(), "{v}");
        hs
    }

    /// F-30: `transactions(fungibleName)` beside `requestKey` or `blockHash` keeps only the
    /// txs with an event of that module (stock: `EXISTS` an event with `module` = the name).
    #[tokio::test]
    async fn fungible_name_filters_transactions_of_a_key_or_a_block() {
        let fx = fixture();
        // Block (0, 4): a `One` tx (coin only), then a `Many` tx (coin and kaddex.kdx).
        let [one, many] = [&fx.rks[&(0, 4)][0], &fx.rks[&(0, 4)][1]];
        let rk = |k: &str| format!("requestKey: \"{k}\"");
        assert_eq!(hashes(&fx.schema, rk(one), "coin").await, [one.as_str()]);
        assert!(hashes(&fx.schema, rk(one), "kaddex.kdx").await.is_empty());
        assert_eq!(
            hashes(&fx.schema, rk(many), "kaddex.kdx").await,
            [many.as_str()]
        );
        let bh = format!("blockHash: \"{}\"", block_hash(0, 4));
        assert_eq!(
            hashes(&fx.schema, bh.clone(), "kaddex.kdx").await,
            [many.as_str()]
        );
        assert_eq!(hashes(&fx.schema, bh.clone(), "coin").await.len(), 2);
        assert!(hashes(&fx.schema, bh, "nobody.token").await.is_empty());
    }

    /// PR-4 (a), stock parity: `transactions(accountName | chainId, fungibleName)` keeps the
    /// txs with ANY event of that module (stock: `EXISTS` an event with `module` = the name),
    /// not only those with a transfer of it: block (1, 11)'s tx, sent by Alice, emits only
    /// `coin.NOTE`.
    #[tokio::test]
    async fn fungible_name_keeps_txs_with_any_event_of_the_module() {
        let fx = fixture();
        let note = fx.rks[&(1, 11)][0].clone();
        for src in [
            format!("accountName: \"{ALICE}\""),
            format!("accountName: \"{ALICE}\", chainId: \"1\""),
        ] {
            let hs = hashes(&fx.schema, src.clone(), "coin").await;
            assert!(hs.contains(&note), "{src}: {hs:?}");
            // A module the tx has no event of still drops it.
            let hs = hashes(&fx.schema, src.clone(), "kaddex.kdx").await;
            assert!(!hs.contains(&note), "{src}: {hs:?}");
            assert!(!hs.is_empty(), "{src}");
        }
        // Without an account, key or block the source stays the full-history
        // `<module>.TRANSFER` postings (a deliberate gap with stock, see docs/graphql.md):
        // the NOTE-only tx is not listed there.
        for src in ["chainId: \"1\"", "minHeight: 11, maxHeight: 11"] {
            let hs = hashes(&fx.schema, src.to_owned(), "coin").await;
            assert!(!hs.contains(&note), "{src}: {hs:?}");
            assert!(!hs.is_empty(), "{src}");
        }
        // Over the old sender path too (`sender_index = false`).
        fx.store.set_sender_index(false);
        let hs = hashes(&fx.schema, format!("accountName: \"{ALICE}\""), "coin").await;
        assert!(hs.contains(&note), "{hs:?}");
    }

    /// PR-4 (a), controller ruling: `transactions(fungibleName)` without an account, key or
    /// block reads the full-history `<module>.TRANSFER` postings, so a transfer more than
    /// `MAX_HEIGHTS` below the chain's tip is still listed (a heights walk would stop above
    /// it).
    #[tokio::test]
    async fn fungible_name_alone_reaches_transfers_below_the_height_walk() {
        let extra = kdx_graphql::txlist::MAX_HEIGHTS + 10;
        let fx = fixture_big(GraphqlSettings::default(), extra);
        let old = fx.rks[&(0, 4)][0].clone();
        let v = run(
            &fx.schema,
            "{ transactions(chainId: \"0\", fungibleName: \"coin\", last: 3) { edges { node { hash } } } }",
        )
        .await;
        let hs: Vec<&str> = edges(&v, "transactions")
            .iter()
            .map(|e| e["node"]["hash"].as_str().unwrap())
            .collect();
        assert!(hs.contains(&old.as_str()), "{v}");
    }

    /// F-30: the `isNFT` account path checks `fungibleName` as every other path does.
    #[tokio::test]
    async fn nft_transfers_check_the_fungible_name() {
        let fx = fixture();
        let r = fx
            .schema
            .execute(format!(
                "{{ transfers(accountName: \"{ALICE}\", isNFT: true, fungibleName: \"  \") \
                 {{ edges {{ node {{ requestKey }} }} }} }}"
            ))
            .await;
        assert_eq!(code(&r), bad_input(), "{:?}", r.errors);
    }

    /// F-50, GQ-10: a block's `totalCount` (counted with the page) that the deadline cuts is
    /// a `COUNT_TIMEOUT` error, as a deferred count is, not a partial number or an exact 0.
    #[tokio::test]
    async fn a_bounded_count_cut_by_the_deadline_is_an_error() {
        let fx = fixture();
        let q = format!(
            "{{ transfers(blockHash: \"{}\") {{ totalCount }} }}",
            block_hash(0, 6)
        );
        let req = async_graphql::Request::new(q.clone()).data(kdx_graphql::ReqCtx {
            deadline: std::time::Instant::now(),
        });
        let r = fx.schema.execute(req).await;
        assert!(!r.errors.is_empty(), "a cut count is an error");
        assert!(r.extensions.is_empty(), "{:?}", r.extensions);
        // With time, the same count is exact.
        let v = run(&fx.schema, &q).await;
        assert!(v["transfers"]["totalCount"].as_i64().unwrap() > 0);
    }

    /// F-51: the id of a `transfers(isNFT: true)` node resolves through `node(id)` to the
    /// same transfer.
    #[tokio::test]
    async fn nft_transfer_ids_resolve_through_node() {
        let fx = fixture();
        let sel = "id requestKey orderIndex moduleName senderAccount receiverAccount amount";
        let v = run(
            &fx.schema,
            &format!(
                "{{ transfers(accountName: \"{ALICE}\", isNFT: true, first: 50) \
                 {{ edges {{ node {{ {sel} }} }} }} }}"
            ),
        )
        .await;
        let list = edges(&v, "transfers");
        assert!(!list.is_empty(), "{v}");
        for e in list {
            let n = run(
                &fx.schema,
                &format!(
                    "{{ node(id: {}) {{ ... on Transfer {{ {sel} }} }} }}",
                    e["node"]["id"]
                ),
            )
            .await;
            assert_eq!(n["node"], e["node"], "{n}");
        }
    }

    /// F-28: `balance(chainIds: [])` lists every chain (as `fungibleChainAccounts` does), more
    /// than 100 chain ids are refused, and the page follows the cursor rules of every other
    /// connection (`after` with `first: 0` serves no row).
    #[tokio::test]
    async fn balance_reads_chain_ids_and_pages_as_other_connections() {
        let fx = fixture();
        let q = |args: &str| {
            format!(
                "{{ balance(accountName: \"{ALICE}\"{args}) \
                 {{ edges {{ cursor node {{ chainId module }} }} pageInfo {{ hasNextPage }} }} }}"
            )
        };
        let all = run(&fx.schema, &q("")).await;
        assert_eq!(edges(&all, "balance").len(), 2, "{all}");
        // The stored texts `1.0` and `0.0` are served in stock's layout (`balance_text`).
        let texts = run(
            &fx.schema,
            &format!(
                "{{ balance(accountName: \"{ALICE}\") {{ edges {{ node {{ balance }} }} }} }}"
            ),
        )
        .await;
        let got: Vec<&Value> = edges(&texts, "balance")
            .iter()
            .map(|e| &e["node"]["balance"])
            .collect();
        assert_eq!(got, [&json!("1"), &json!("1")], "{texts}");
        assert_eq!(run(&fx.schema, &q(", chainIds: []")).await, all);
        let many = format!(", chainIds: [{}]", vec!["\"0\""; 101].join(", "));
        let r = fx.schema.execute(q(&many)).await;
        assert_eq!(code(&r), bad_input(), "{:?}", r.errors);
        let first = edges(&all, "balance")[0]["cursor"].as_str().unwrap();
        let v = run(&fx.schema, &q(&format!(", after: \"{first}\", first: 0"))).await;
        assert!(edges(&v, "balance").is_empty(), "{v}");
    }
}

/// The real-traffic `Transactions` operation (G4 replay corpus, case gql.transactions:1000002,
/// 45,584 requests in two days), verbatim: one chain at one height, `isCoinbase: true`.
const COINBASE_OP: &str = r#"
  query Transactions(
    $first: Int, $last: Int, $after: String, $before: String, $chainId: String,
    $isCoinbase: Boolean, $minHeight: Int, $maxHeight: Int
  ) {
    transactions(
      first: $first, last: $last, after: $after, before: $before, chainId: $chainId,
      isCoinbase: $isCoinbase, minHeight: $minHeight, maxHeight: $maxHeight
    ) {
      totalCount
      pageInfo { endCursor hasNextPage hasPreviousPage startCursor }
      edges {
        node {
          hash
          cmd {
            meta { gasLimit chainId creationTime gasPrice sender }
            payload { ... on ExecutionPayload { code } }
          }
          result {
            ... on TransactionResult { badResult gas block { height canonical } }
          }
        }
        cursor
      }
    }
  }
"#;

/// G4 class 1: `transactions(isCoinbase: true)` lists the coinbase as a `Transaction`
/// beside the user txs, as stock answers it: first in its block (stock orders by creation
/// time, and the coinbase has the block's), then the user txs in the list's existing order
/// (R-P5P9-151: reverse node order). The coinbase row is as stock's: the node's coinbase
/// `reqKey` as `hash`, sender `coinbase`, gas limit, price and gas 0, code `{}`, the block's
/// time in whole seconds. `false` keeps user txs only.
#[tokio::test]
async fn is_coinbase_true_lists_the_coinbase_as_stock_does() {
    let fx = fixture();
    let vars = |cb: Option<bool>| {
        json!({"chainId": "0", "first": 25, "isCoinbase": cb, "last": null,
               "minHeight": 11, "maxHeight": 11})
    };
    let v = run_vars(&fx.schema, COINBASE_OP, vars(Some(true))).await;
    assert!(v.get("errors").is_none(), "{v}");
    let t = &v["data"]["transactions"];
    let edges = t["edges"].as_array().unwrap();
    let users = &fx.rks[&(0, 11)];
    assert_eq!(edges.len(), 1 + users.len(), "{t}");
    assert_eq!(t["totalCount"], json!(1 + users.len()));
    let cb = &edges[0]["node"];
    assert_eq!(
        cb,
        &json!({
            "hash": b64url_encode(&hash_of(0xCB, 0, 11)),
            "cmd": {
                "meta": {"gasLimit": 0, "chainId": 0,
                         "creationTime": "2023-11-14T22:18:50.000Z",
                         "gasPrice": "0", "sender": "coinbase"},
                "payload": {"code": "{}"}
            },
            "result": {"badResult": null, "gas": "0",
                       "block": {"height": 11, "canonical": true}}
        })
    );
    // The user txs follow, in reverse node order, unchanged.
    let hashes: Vec<&str> = edges[1..]
        .iter()
        .map(|e| e["node"]["hash"].as_str().unwrap())
        .collect();
    let mut want: Vec<&str> = users.iter().map(String::as_str).collect();
    want.reverse();
    assert_eq!(hashes, want);

    for cb in [Some(false), None] {
        let v = run_vars(&fx.schema, COINBASE_OP, vars(cb)).await;
        assert!(v.get("errors").is_none(), "{v}");
        let t = &v["data"]["transactions"];
        assert_eq!(t["edges"].as_array().unwrap().len(), users.len(), "{t}");
        assert_eq!(t["totalCount"], json!(users.len()));
    }
}

/// A coinbase `Transaction`'s other fields: stock's defaults for a tx with no command
/// (`transaction-schema-validator.ts`): no signers or sigs, empty nonce, the network id,
/// data `{}`, ttl 0; its result is the stored coinbase output.
#[tokio::test]
async fn a_listed_coinbase_has_stock_s_command_defaults() {
    let fx = fixture();
    // A list of one block is in node order: the coinbase first.
    let q = r#"{ transactions(isCoinbase: true, blockHash: "BH", first: 1) {
        edges { node { sigs { sig } cmd { nonce networkId signers { pubkey }
          meta { ttl } payload { ... on ExecutionPayload { data } } }
          result { ... on TransactionResult { goodResult eventCount
            events { edges { node { qualifiedName } } } } } } } } }"#;
    let v = run(&fx.schema, &q.replace("BH", &block_hash(1, 9))).await;
    assert_eq!(
        v["transactions"]["edges"][0]["node"],
        json!({"sigs": [],
               "cmd": {"nonce": "", "networkId": "mainnet01", "signers": [],
                       "meta": {"ttl": "0"}, "payload": {"data": "{}"}},
               "result": {"goodResult": "\"ok\"", "eventCount": 1,
                          "events": {"edges": [{"node": {"qualifiedName": "coin.COINBASE"}}]}}})
    );
}

/// A coinbase row's `cmd.meta` as stock recorded it (G4 replay run 2, `gql.transactions:1000002`,
/// all 5,193 `isCoinbase: true` answers have the same types). Stock sends the coinbase
/// `gasLimit` as the JSON number `0`, while every user tx's is a string. `gasPrice` is the
/// string `"0"` and `chainId` a number, like user rows.
const STOCK_COINBASE_META: &str = r#"{"gasLimit": 0, "chainId": 1,
    "creationTime": "2021-12-09T03:03:45.000Z", "gasPrice": "0", "sender": "coinbase"}"#;

/// G4 run 2: a coinbase row's meta has the JSON types of stock's recorded answer (`gasLimit` is
/// the number `0`), and user rows keep the string `gasLimit`.
#[tokio::test]
async fn a_listed_coinbase_meta_has_stock_s_json_types() {
    let fx = fixture();
    let q = r#"{ transactions(isCoinbase: true, blockHash: "BH", first: 100) {
        edges { node { cmd { meta { chainId creationTime gasLimit gasPrice sender } } } } } }"#;
    let v = run(&fx.schema, &q.replace("BH", &block_hash(1, 9))).await;
    let edges = v["transactions"]["edges"].as_array().unwrap();
    let meta = |i: usize| &edges[i]["node"]["cmd"]["meta"];
    let stock: Value = serde_json::from_str(STOCK_COINBASE_META).unwrap();
    assert_eq!(meta(0)["sender"], json!("coinbase"));
    assert_eq!(meta(0)["gasLimit"], stock["gasLimit"]);
    for (k, want) in stock.as_object().unwrap() {
        let kind = |x: &Value| std::mem::discriminant(x);
        assert_eq!(kind(&meta(0)[k]), kind(want), "coinbase meta.{k}");
    }
    // The block has user txs too: their gasLimit stays a string.
    assert!(edges.len() > 1, "fixture block has user txs");
    for i in 1..edges.len() {
        assert!(meta(i)["gasLimit"].is_string(), "user row {i}");
    }
}

/// Request keys of the G4 replay corpus (gql.transaction:1000002 and :1000003): the
/// base64-wrapped coinbase keys stock lists as `hash` (60 characters: a JSON-quoted hash
/// text; 58: a bare one). Neither decodes to a 32-byte hash.
const WRAPPED_RKS: [&str; 2] = [
    "ImZBSmFxWUlGYlhqU1RVUllQOWNrdEFnczFxUl8zbHhPREsxUEFPOHZjcUki",
    "NHloUkNldlQ2TDQxX01oY2k1VWhHWVRxRmxOWFpWR1VycXQyR2VYcGJEYw",
];

/// G4 class 2: a request key that is no hash finds nothing, as in stock (its lookups compare
/// the text with the stored key): `transaction` answers `null` and the lists by request key
/// answer no rows, all without an error (stock: `{"transaction": null}`, 17,227 real
/// requests). Other argument checks still refuse.
#[tokio::test]
async fn a_request_key_that_is_no_hash_finds_nothing_without_an_error() {
    let fx = fixture();
    let one = "query T($requestKey: String!) { transaction(requestKey: $requestKey) { hash } }";
    for rk in WRAPPED_RKS
        .iter()
        .copied()
        .chain(["short", "", &"A".repeat(200)])
    {
        let v = run_vars(&fx.schema, one, json!({"requestKey": rk})).await;
        assert_eq!(v, json!({"data": {"transaction": null}}), "{rk}");
        for q in [
            "transactions(requestKey: $rk) { totalCount edges { node { hash } } }",
            "transfers(requestKey: $rk) { totalCount edges { node { amount } } }",
            "events(qualifiedEventName: \"coin.TRANSFER\", requestKey: $rk) \
             { totalCount edges { node { requestKey } } }",
        ] {
            let field = &q[..q.find('(').unwrap()];
            let v = run_vars(
                &fx.schema,
                &format!("query L($rk: String) {{ {q} }}"),
                json!({"rk": rk}),
            )
            .await;
            assert_eq!(
                v,
                json!({"data": {field: {"totalCount": 0, "edges": []}}}),
                "{field} {rk}"
            );
        }
    }
    // A malformed `blockHash` beside it is still refused, as before.
    let v = run_vars(
        &fx.schema,
        "query T($rk: String!) { transaction(requestKey: $rk, blockHash: \"x\") { hash } }",
        json!({"rk": WRAPPED_RKS[0]}),
    )
    .await;
    assert!(
        v["errors"][0]["message"]
            .as_str()
            .unwrap()
            .contains("blockHash"),
        "{v}"
    );
}

/// Controller ruling on PR #30 (review N1): in a `transactions(isCoinbase: true)` list the
/// coinbase is the first row of its block, as stock orders it, so kadscan's `first: 25` page of
/// a block with 25 or more user txs still shows it. The user rows keep the order and the cursors
/// of the list without the flag. Every cursor resumes at the next row both ways (`after` with
/// `first`, `before` with `last`).
#[tokio::test]
async fn is_coinbase_true_puts_the_coinbase_first_in_its_block() {
    let fx = fixture();
    let win = "chainId: \"0\", minHeight: 4, maxHeight: 11";
    let q = |args: &str| {
        format!(
            "{{ transactions({args}) {{ pageInfo {{ hasNextPage hasPreviousPage }} \
             edges {{ cursor node {{ hash }} }} }} }}"
        )
    };
    let rows = |v: &Value| -> Vec<(String, String)> {
        v["transactions"]["edges"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| {
                (
                    e["node"]["hash"].as_str().unwrap().to_string(),
                    e["cursor"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    };
    // Expected: newest block first; in each, the coinbase, then the user txs newest first.
    let mut want = Vec::new();
    for h in (4..=11).rev() {
        let Some(users) = fx.rks.get(&(0, h)) else {
            continue;
        };
        want.push(b64url_encode(&hash_of(0xCB, 0, h)));
        want.extend(users.iter().rev().cloned());
    }
    let all = rows(
        &run(
            &fx.schema,
            &q(&format!("isCoinbase: true, {win}, first: 1000")),
        )
        .await,
    );
    let hashes: Vec<String> = all.iter().map(|r| r.0.clone()).collect();
    assert_eq!(hashes, want);

    // The user rows are the list without the flag, with the same cursors.
    let plain = rows(&run(&fx.schema, &q(&format!("{win}, first: 1000"))).await);
    let users: Vec<(String, String)> = all
        .iter()
        .filter(|r| plain.iter().any(|p| p.0 == r.0))
        .cloned()
        .collect();
    assert_eq!(users, plain);
    assert!(users.len() < all.len());

    // kadscan's operation on one block with a page smaller than the block: the coinbase first.
    let one = "isCoinbase: true, chainId: \"0\", minHeight: 11, maxHeight: 11";
    let n = fx.rks[&(0, 11)].len();
    assert!(n >= 2, "block 0/11 needs user txs");
    let v = run(&fx.schema, &q(&format!("{one}, first: 1"))).await;
    assert_eq!(rows(&v)[0].0, b64url_encode(&hash_of(0xCB, 0, 11)));
    assert_eq!(v["transactions"]["pageInfo"]["hasNextPage"], json!(true));
    let v = run(&fx.schema, &q(&format!("{one}, last: 1"))).await;
    assert_eq!(rows(&v)[0].0, fx.rks[&(0, 11)][0]);

    // Every cursor resumes at the next row in both directions.
    for (i, (_, c)) in all.iter().enumerate() {
        let after = rows(
            &run(
                &fx.schema,
                &q(&format!(
                    "isCoinbase: true, {win}, first: 1000, after: \"{c}\""
                )),
            )
            .await,
        );
        assert_eq!(after, all[i + 1..], "after row {i}");
        let before = rows(
            &run(
                &fx.schema,
                &q(&format!(
                    "isCoinbase: true, {win}, last: 1000, before: \"{c}\""
                )),
            )
            .await,
        );
        assert_eq!(before, all[..i], "before row {i}");
        let v = run(
            &fx.schema,
            &q(&format!(
                "isCoinbase: true, {win}, last: 1, before: \"{c}\""
            )),
        )
        .await;
        assert_eq!(
            rows(&v),
            all[i.saturating_sub(1)..i],
            "last 1 before row {i}"
        );
    }
}

/// The posting lists of the fixture (account, transfer, fungible, event and public-key
/// sources), with key filters (`chainId`, heights) and row filters (`fungibleName`, the
/// sender rule) that drop rows the walk reads.
fn posting_lists() -> Vec<List> {
    let a = format!("accountName: \"{ALICE}\"");
    let a0 = format!("{a}, chainId: \"0\"");
    let coin_transfer = "qualifiedEventName: \"coin.TRANSFER\"";
    let root = |field: &'static str, args: &str, node: &'static str, total: bool| {
        list(format!("{field}({args}, ARGS) SEL"), &[field], node, total)
    };
    let nested =
        |parent: &'static str, args: &str, field: &'static str, node: &'static str, total: bool| {
            let q = format!("{parent}({args}) {{ {field}(ARGS) SEL }}");
            list(q, &[parent, field], node, total)
        };
    vec![
        root("transfers", &a, TRANSFER, true),
        root(
            "transfers",
            &format!("{a}, fungibleName: \"coin\""),
            TRANSFER,
            true,
        ),
        root("transfers", &a0, TRANSFER, true),
        root(
            "transactions",
            &format!("{a}, minHeight: 6, maxHeight: 11"),
            TX,
            true,
        ),
        root("transfers", "fungibleName: \"coin\"", TRANSFER, true),
        root("events", coin_transfer, EVENT, true),
        root(
            "events",
            &format!("{coin_transfer}, minHeight: 4, maxHeight: 11"),
            EVENT,
            true,
        ),
        root("transactions", &a, TX, true),
        root(
            "transactions",
            &format!("{a}, fungibleName: \"coin\""),
            TX,
            true,
        ),
        root(
            "transactionsByPublicKey",
            &format!("publicKey: \"{ALICE_KEY}\""),
            TX,
            true,
        ),
        nested("fungibleAccount", &a, "transfers", TRANSFER, false),
        nested("fungibleAccount", &a, "transactions", TX, true),
        nested("fungibleChainAccount", &a0, "transfers", TRANSFER, false),
        nested("fungibleChainAccount", &a0, "transactions", TX, true),
        nested("nonFungibleAccount", &a, "transactions", TX, true),
    ]
}

/// gql.transfers:1000551: `last: N` on a posting list reads the list from its oldest row, so
/// a list with more txs than the `last`/`before` key cap answers it (it answered `LIMIT`:
/// the page collected every key down from the newest first). Here the cap is 2 txs and every
/// list is longer: `last: 1` is the oldest row, and every backward page join equals the big
/// page with the right `pageInfo`.
#[tokio::test]
async fn last_pages_of_posting_lists_read_from_the_oldest_row_past_the_key_cap() {
    let fx = fixture_with(GraphqlSettings {
        back_keys: 2,
        ..GraphqlSettings::default()
    });
    let mut fails = Vec::new();
    for l in &posting_lists() {
        let full = big_page(&fx.schema, l).await;
        let txs: HashSet<&Value> = full.iter().map(|r| &r["node"]["requestKey"]).collect();
        assert!(
            txs.len() > 2 || l.node == TX && full.len() > 2,
            "{}: the list must hold more txs than the cap",
            l.query
        );
        let v = run(&fx.schema, &l.text("last: 1")).await;
        let c = conn(&v, &l.path);
        let edges = c["edges"].as_array().cloned().unwrap_or_default();
        assert_eq!(edges, full[full.len() - 1..], "{}: last: 1", l.query);
        if let Err(e) = check_page(l, c, &edges, false, (false, true), full.len()) {
            fails.push(format!("{} last: 1: {e}", l.query));
        }
        let ks: Vec<usize> = (1..=full.len() + 1).collect();
        fails.extend(joins(&fx.schema, l, &ks, true).await);
    }
    fails.truncate(40);
    assert!(fails.is_empty(), "{}", fails.join("\n"));
}

/// `last: N, before: C` on a posting list is the N rows above C in the big page (the
/// first/after walk, unchanged), for every cursor C and N from 1 to 3, with the right
/// `pageInfo`. Below the cap this is what the newest-first key collection answered.
#[tokio::test]
async fn last_before_on_posting_lists_is_the_rows_above_the_cursor() {
    for gs in [
        GraphqlSettings::default(),
        GraphqlSettings {
            back_keys: 2,
            ..GraphqlSettings::default()
        },
    ] {
        let fx = fixture_with(gs);
        let mut fails = Vec::new();
        for l in &posting_lists() {
            let full = big_page(&fx.schema, l).await;
            for (i, row) in full.iter().enumerate() {
                let c = row["cursor"].as_str().unwrap();
                for n in 1..=3usize {
                    let v = run(&fx.schema, &l.text(&format!("last: {n}, before: \"{c}\""))).await;
                    let cn = conn(&v, &l.path);
                    let edges = cn["edges"].as_array().cloned().unwrap_or_default();
                    let want = &full[i.saturating_sub(n)..i];
                    if edges != want {
                        fails.push(format!(
                            "{} last {n} before row {i}: {:?} != {:?}",
                            l.query,
                            ids(&edges),
                            ids(want)
                        ));
                    }
                    if let Err(e) = check_page(l, cn, &edges, true, (true, i > n), full.len()) {
                        fails.push(format!("{} last {n} before row {i}: {e}", l.query));
                    }
                }
            }
        }
        fails.truncate(40);
        assert!(fails.is_empty(), "{}", fails.join("\n"));
    }
}

/// Sources with no oldest-first walk keep the key cap: `isCoinbase: true` (coinbase-first
/// order) and the height walk answer `LIMIT` once `last` reaches more txs than the cap.
#[tokio::test]
async fn last_pages_of_other_sources_keep_the_key_cap() {
    let fx = fixture_with(GraphqlSettings {
        back_keys: 2,
        ..GraphqlSettings::default()
    });
    for args in ["isCoinbase: true, last: 1", "last: 1"] {
        let q = format!("{{ transactions({args}) {{ edges {{ node {{ hash }} }} }} }}");
        let r = fx.schema.execute(q.as_str()).await;
        let e = r.errors.first().unwrap_or_else(|| panic!("{q}: no error"));
        assert!(
            e.message.contains("last/before reaches more than 2 txs"),
            "{q}: {e:?}"
        );
        let code = e
            .extensions
            .as_ref()
            .and_then(|x| x.get("code"))
            .map(|c| c.to_string());
        assert_eq!(code.as_deref(), Some("\"LIMIT\""), "{q}: {e:?}");
    }
}

/// PR #48 part 2: stored per-key row counts (`kdx_hot::rowcount`) built by
/// `reconcile-counts` (`kdx_ingest::row_counts`) and kept by the tail answer `totalCount` of
/// the plain posting-keyed lists, equal to the full walk; filters they do not hold walk.
mod stored_count {
    use super::*;
    use kdx_hot::rowcount::{self, Tally};
    use kdx_ingest::row_counts;

    /// Lists the stored counts answer (no filter, `chainId`, `isCoinbase`), over several kinds,
    /// both chains, frozen and hot rows, coinbase rows and a module with only 4-param
    /// `TRANSFER` events (no transfer row).
    const STORED: &[&str] = &[
        "transfers(accountName: \"k:alice\", ARGS)",
        "transfers(accountName: \"k:alice\", chainId: \"1\", ARGS)",
        "transfers(accountName: \"k:carol\", ARGS)",
        "transfers(accountName: \"k:alice\", fungibleName: \"coin\", ARGS)",
        "transfers(accountName: \"k:carol\", fungibleName: \"kaddex.kdx\", ARGS)",
        "transfers(fungibleName: \"coin\", ARGS)",
        "transfers(fungibleName: \"kaddex.kdx\", chainId: \"0\", ARGS)",
        "transfers(fungibleName: \"marmalade-v2.ledger\", ARGS)",
        "transactions(accountName: \"k:alice\", ARGS)",
        "transactions(accountName: \"k:alice\", chainId: \"0\", ARGS)",
        "transactions(accountName: \"k:alice\", isCoinbase: true, ARGS)",
        "transactions(fungibleName: \"coin\", ARGS)",
        "transactions(fungibleName: \"coin\", isCoinbase: true, ARGS)",
        "transactionsByPublicKey(publicKey: \"alice-key\", ARGS)",
        "events(qualifiedEventName: \"coin.TRANSFER\", ARGS)",
        "events(qualifiedEventName: \"coin.NOTE\", chainId: \"1\", ARGS)",
        "events(qualifiedEventName: \"coin.COINBASE\", ARGS)",
    ];

    /// Lists that keep the walk: heights, depth, `fungibleName` on an account's txs, event
    /// parameter / order filters, NFT transfers.
    const WALKED: &[&str] = &[
        "transactions(accountName: \"k:alice\", minHeight: 5, ARGS)",
        "transactions(accountName: \"k:alice\", minimumDepth: 1, ARGS)",
        "transactions(accountName: \"k:alice\", fungibleName: \"coin\", ARGS)",
        "events(qualifiedEventName: \"coin.TRANSFER\", minHeight: 5, ARGS)",
        "events(qualifiedEventName: \"coin.TRANSFER\", orderIndex: 0, ARGS)",
        "transfers(accountName: \"k:alice\", isNFT: true, ARGS)",
    ];

    fn field(l: &str) -> &str {
        &l[..l.find('(').unwrap()]
    }

    async fn total(s: &KdxSchema, l: &str) -> i64 {
        let q = format!("{{ {} {{ totalCount }} }}", l.replace("ARGS", "first: 1"));
        let v = run(s, &q).await;
        v[field(l)]["totalCount"]
            .as_i64()
            .unwrap_or_else(|| panic!("{q}: {v}"))
    }

    async fn walk(s: &KdxSchema, l: &str) -> i64 {
        let q = format!(
            "{{ {} {{ edges {{ cursor }} }} }}",
            l.replace("ARGS", "first: 1000")
        );
        let v = run(s, &q).await;
        v[field(l)]["edges"].as_array().unwrap().len() as i64
    }

    /// `totalCount` of `l` with the request deadline already spent: `Some` only when no walk
    /// ran (the stored count answered).
    async fn total_spent(s: &KdxSchema, l: &str) -> Option<i64> {
        let q = format!("{{ {} {{ totalCount }} }}", l.replace("ARGS", "first: 1"));
        let req = async_graphql::Request::new(q).data(kdx_graphql::ReqCtx {
            deadline: Instant::now(),
        });
        let r = s.execute(req).await;
        let d = serde_json::to_value(&r.data).unwrap();
        d.get(field(l))
            .and_then(|c| c.get("totalCount"))
            .and_then(Value::as_i64)
    }

    fn reconcile(fx: &Fx) -> row_counts::Report {
        let r =
            row_counts::reconcile(&fx.store.segdir, &fx.store.hot, 2, false, &|| false).unwrap();
        assert!(r.ready && r.unrendered == 0, "{r:?}");
        r
    }

    fn stored(fx: &Fx) -> Tally {
        rowcount::read_all(&fx.store.hot.ro().unwrap()).unwrap()
    }

    fn recount(fx: &Fx) -> Tally {
        row_counts::tally(&fx.store.segdir, &fx.store.hot, 3, &|| false)
            .unwrap()
            .0
    }

    fn ready(fx: &Fx) -> bool {
        rowcount::ready(&fx.store.hot.ro().unwrap()).unwrap()
    }

    /// Every stored-count list equals its walk; every walk-only list still walks.
    async fn check_lists(fx: &Fx) {
        for l in STORED.iter().chain(WALKED) {
            assert_eq!(total(&fx.schema, l).await, walk(&fx.schema, l).await, "{l}");
        }
    }

    /// The backfill builds exactly the counts ingest maintains: every fixture block applied by
    /// the tail alone (in the fixture's interning order, so ids match) gives the table the
    /// backfill builds from the fixture's frozen ranges and hot blocks.
    #[test]
    fn backfill_equals_ingest() {
        let fx = fixture();
        let built = recount(&fx);
        assert!(built.0.len() > 20, "a real fixture: {}", built.0.len());
        let kinds: HashSet<u8> = built.0.keys().map(|k| k.0 as u8).collect();
        assert_eq!(kinds.len(), 7, "every kind is exercised: {kinds:?}");
        let chains: HashSet<u8> = built.0.keys().map(|k| k.2).collect();
        assert_eq!(chains, HashSet::from([0, 1]));

        let d = tempfile::tempdir().unwrap();
        let hot = kdx_hot::HotStore::open(d.path(), kdx_hot::Geometry::default()).unwrap();
        let mut t = hot.rw().unwrap();
        let mut dict = HotDict::load(&t).unwrap();
        let order = BLOCKS
            .iter()
            .filter(|b| b.1 < FIRST_RANGE_END)
            .chain(
                BLOCKS
                    .iter()
                    .filter(|b| (FIRST_RANGE_END..FRONTIER).contains(&b.1)),
            )
            .chain(BLOCKS.iter().filter(|b| b.1 >= FRONTIER));
        for &(c, h, shapes) in order {
            let (b, _) = decoded(c, h, shapes);
            let ib = index_block(&b, &VerifyReport::default(), &mut dict).unwrap();
            kdx_hot::tail::apply(&mut t, &ib, &mut dict).unwrap();
        }
        let ingest = rowcount::read_all(&t).unwrap();
        // Ingest keeps the table but never marks it ready: only the backfill does.
        assert!(!rowcount::ready(&t).unwrap());
        t.commit().unwrap();
        assert_eq!(ingest, built);
    }

    /// Stored counts equal the full walk on every plain list, and the backfill is idempotent.
    #[tokio::test]
    async fn stored_counts_equal_the_walk() {
        let fx = fixture();
        let walked: Vec<i64> = {
            let mut v = Vec::new();
            for l in STORED {
                v.push(walk(&fx.schema, l).await);
            }
            v
        };
        assert!(!ready(&fx));
        let r = reconcile(&fx);
        assert!(r.frozen_txs > 0 && r.hot_txs > 0, "{r:?}");
        for (l, w) in STORED.iter().zip(&walked) {
            assert_eq!(total(&fx.schema, l).await, *w, "{l}");
            assert_eq!(total_spent(&fx.schema, l).await, Some(*w), "{l}: stored");
        }
        assert!(walked.contains(&0) && walked.iter().any(|&w| w > 10));
        let once = stored(&fx);
        reconcile(&fx);
        assert_eq!(stored(&fx), once, "idempotent");
        check_lists(&fx).await;
    }

    /// Counts stay right through a reorg: unwind two hot blocks of chain 0, apply a different
    /// fork, with the marker set (stored counts answer) and unset (the walk answers; the
    /// table still follows, and a backfill marks it ready again).
    #[tokio::test]
    async fn counts_follow_a_reorg_with_and_without_the_marker() {
        let fx = fixture();
        reconcile(&fx);
        let reorg = |chain: u8, to: u32, fork: &[(u32, &[Shape])]| {
            let mut t = fx.store.hot.rw().unwrap();
            let mut dict = HotDict::load(&t).unwrap();
            assert!(kdx_hot::tail::unwind_to(&mut t, chain, to).unwrap() > 0);
            for &(h, shapes) in fork {
                let (mut b, _) = decoded(chain, h, shapes);
                // Another fork: another block hash.
                b.header.hash = kdx_types::Hash32(hash_of(0xC0, chain, h));
                let ib = index_block(&b, &VerifyReport::default(), &mut dict).unwrap();
                kdx_hot::tail::apply(&mut t, &ib, &mut dict).unwrap();
            }
            dict.persist(&mut t).unwrap();
            t.commit().unwrap();
        };
        let before = stored(&fx);
        reorg(0, FRONTIER, &[(11, &[Shape::One, Shape::Nft, Shape::Many])]);
        assert!(ready(&fx), "a reorg keeps the marker");
        assert_ne!(stored(&fx), before, "the reorg changed counts");
        assert_eq!(stored(&fx), recount(&fx), "marker set: table = recount");
        check_lists(&fx).await;
        for l in STORED {
            assert!(total_spent(&fx.schema, l).await.is_some(), "{l}: stored");
        }

        // Marker unset: the walk answers; the tail still keeps the table exact.
        let mut t = fx.store.hot.rw().unwrap();
        assert!(rowcount::clear_ready(&mut t).unwrap());
        t.commit().unwrap();
        reorg(1, 11, &[(12, &[Shape::Note]), (13, &[Shape::Many])]);
        assert!(!ready(&fx));
        assert_eq!(stored(&fx), recount(&fx), "marker unset: table = recount");
        check_lists(&fx).await;
        reconcile(&fx);
        check_lists(&fx).await;
    }

    /// A count answered from storage on a list far longer than the walk can finish in the
    /// deadline: the request deadline is already spent, the walk cannot read one key, the
    /// stored count still answers, and equals the walk under a normal deadline.
    #[tokio::test]
    async fn a_stored_count_answers_where_the_walk_cannot() {
        let fx = fixture_big(GraphqlSettings::default(), 3_000);
        let l = "transfers(accountName: \"k:alice\", ARGS)";
        let walked = total(&fx.schema, l).await;
        assert!(walked > 3_000);
        assert_eq!(
            total_spent(&fx.schema, l).await,
            None,
            "no marker: the walk is cut"
        );
        reconcile(&fx);
        assert_eq!(total_spent(&fx.schema, l).await, Some(walked));
        let e = "events(qualifiedEventName: \"coin.TRANSFER\", ARGS)";
        assert_eq!(
            total_spent(&fx.schema, e).await,
            Some(walk_all(&fx.schema, e).await)
        );
        // Walk-only lists (each over thousands of keys here) still walk: a spent deadline
        // gives no number.
        for l in WALKED {
            assert_eq!(total_spent(&fx.schema, l).await, None, "{l}");
        }
    }

    /// Rows of the full walk of `l`, pages of 1,000.
    async fn walk_all(s: &KdxSchema, l: &str) -> i64 {
        let (mut n, mut after) = (0i64, None::<String>);
        loop {
            let args = match &after {
                None => "first: 1000".to_string(),
                Some(c) => format!("first: 1000, after: \"{c}\""),
            };
            let q = format!(
                "{{ {} {{ pageInfo {{ hasNextPage endCursor }} edges {{ cursor }} }} }}",
                l.replace("ARGS", &args)
            );
            let v = run(s, &q).await;
            let c = &v[field(l)];
            n += c["edges"].as_array().unwrap().len() as i64;
            if c["pageInfo"]["hasNextPage"] != true {
                return n;
            }
            after = c["pageInfo"]["endCursor"].as_str().map(str::to_owned);
        }
    }

    /// A mainnet-like block for the backfill timing: a coinbase paying one of 50 miners
    /// (`coin.TRANSFER ["", miner, reward]`) and `users` coin transfers, each from a sender out
    /// of 2M accounts (its own signer key) to another, with the gas payment to the miner: two
    /// `coin.TRANSFER` events per user tx, as most mainnet txs.
    fn mainnet_like(c: u8, h: u32, users: usize) -> DecodedBlock {
        let acct = |x: u64| {
            format!(
                "k:{:016x}",
                x.wrapping_mul(0x9E37_79B9_7F4A_7C15) % 2_000_000
            )
        };
        let miner = format!("k:miner{}", (u64::from(h) * 7 + u64::from(c)) % 50);
        let mut txs = Vec::new();
        for i in 0..users {
            let seed = (u64::from(h) << 20) ^ (u64::from(c) << 12) ^ i as u64;
            let (from, to) = (acct(seed), acct(seed ^ 0xABCD_EF01));
            let ct = 1_700_000_000u64 + u64::from(h);
            let cmd = json!({
                "payload": {"exec": {"data": null, "code": format!("(coin.transfer \"{from}\" \"{to}\" 1.0)")}},
                "meta": {"sender": from, "chainId": c.to_string(), "creationTime": ct,
                         "gasLimit": 1000, "gasPrice": 1e-8, "ttl": 600},
                "signers": [{"pubKey": format!("{:064x}", seed % 2_000_000)}],
                "nonce": format!("n-{c}-{h}-{i}"),
                "networkId": "mainnet01",
            })
            .to_string();
            let rk = blake2b_256(cmd.as_bytes()).to_b64url();
            let tx = serde_json::to_vec(&json!({"hash": rk, "sigs": [{"sig": "ab"}], "cmd": cmd}))
                .unwrap();
            let evs = [
                ev(None, "coin", "TRANSFER", json!([from, miner, 0.0000115])),
                ev(None, "coin", "TRANSFER", json!([from, to, 1.0])),
            ];
            let out = serde_json::to_vec(&json!({"gas": 1150,
                "result": {"status": "success", "data": "Write succeeded"}, "reqKey": rk,
                "logs": b64url_encode(&hash_of(0x10, c, h)), "events": evs,
                "metaData": null, "continuation": null, "txId": 1234567}))
            .unwrap();
            txs.push(json!([b64url_encode(&tx), b64url_encode(&out)]));
        }
        let cb = serde_json::to_vec(
            &json!({"gas": 0, "result": {"status": "success", "data": "Write succeeded"},
            "reqKey": b64url_encode(&hash_of(0xCB, c, h)), "logs": null,
            "events": [ev(None, "coin", "TRANSFER", json!(["", miner, 2.304523]))],
            "metaData": null, "continuation": null, "txId": 1234566}),
        )
        .unwrap();
        let md = format!(r#"{{"account":"{miner}","predicate":"keys-all","public-keys":["m"]}}"#);
        let payload_hash = b64url_encode(&hash_of(0xA4, c, h));
        let header = json!({
            "nonce": "1", "creationTime": 1_700_000_000_000_000u64 + u64::from(h) * 30_000_000,
            "parent": b64url_encode(&hash_of(0xB0, c, h.saturating_sub(1))),
            "adjacents": {"5": b64url_encode(&hash_of(0xAD, c, h))},
            "target": b64url_encode(&[0xff; 32]), "payloadHash": payload_hash, "chainId": c,
            "weight": b64url_encode(&[1; 32]), "height": h, "chainwebVersion": "mainnet01",
            "epochStart": 1, "featureFlags": 0, "hash": block_hash(c, h)
        });
        let payload = json!({
            "transactions": txs, "minerData": b64url_encode(md.as_bytes()),
            "transactionsHash": b64url_encode(&hash_of(0xA7, c, h)),
            "outputsHash": b64url_encode(&hash_of(0xA8, c, h)),
            "payloadHash": payload_hash, "coinbase": b64url_encode(&cb),
        });
        decode_block(&header, &payload).expect("block decodes")
    }

    /// Backfill rate, run by hand (`--release -- --ignored --nocapture`): a store of
    /// `KDX_COUNT_BENCH_BLOCKS` frozen mainnet-like blocks (default 100,000) on 20 chains,
    /// alternating 1 and 2 user txs (about 40% coinbase records, as mainnet), counted on 1, 2
    /// and 6 threads (KDA-03's default `history_walk_threads`).
    #[test]
    #[ignore = "timing run"]
    fn timing_of_the_backfill() {
        let n: u32 = std::env::var("KDX_COUNT_BENCH_BLOCKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100_000);
        let d = tempfile::tempdir().unwrap();
        let g = kdx_hot::Geometry {
            max_gb: 4,
            growth_mb: 64,
        };
        let hot = kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap();
        let mut t = hot.rw().unwrap();
        let mut dict = HotDict::load(&t).unwrap();
        let segdir = d.path().join("segments");
        let end = n.div_ceil(20);
        let mut w = RangeWriter::create(
            &segdir,
            &d.path().join("etl-0"),
            "mainnet01",
            RangeKind::Step,
            0,
            end,
            0,
            WriteOpts::new(1, 0),
        )
        .unwrap();
        for h in 0..end {
            for c in 0..20u8 {
                let b = mainnet_like(c, h, 1 + (h as usize + c as usize) % 2);
                w.push(&index_block(&b, &VerifyReport::default(), &mut dict).unwrap())
                    .unwrap();
            }
        }
        let entry = w.finish().unwrap();
        let mut m = Manifest::new("mainnet01");
        let next = entry.end_txnum();
        m.push(entry).unwrap();
        m.commit(&segdir).unwrap();
        t.set_frozen_frontier(end).unwrap();
        t.set_next_txnum(next).unwrap();
        dict.persist(&mut t).unwrap();
        t.commit().unwrap();
        for threads in [1usize, 2, 6] {
            let (_, r) = row_counts::tally(&segdir, &hot, threads, &|| false).unwrap();
            println!(
                "threads {threads}: {} records in {:.2} s = {:.0} records/s ({} keys)",
                r.frozen_txs + r.hot_txs,
                r.count_secs,
                r.rate,
                r.keys
            );
        }
    }
}
