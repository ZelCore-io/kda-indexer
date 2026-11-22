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
use std::time::Duration;

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
const BOB: &str = "k:bob";
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

/// The count budget (`count_budget`): a `totalCount` walk that runs out of it answers the
/// rows counted so far, flagged in the response `extensions`; one that finishes in time is
/// exact and unflagged.
mod count_budget {
    use super::*;

    const QUERIES: [&str; 2] = [
        "{ transfers(accountName: \"k:alice\", first: 1) { totalCount } }",
        "{ transactions(accountName: \"k:alice\", first: 1) { totalCount } }",
    ];

    async fn total(s: &KdxSchema, q: &str) -> (i64, bool) {
        let r = s.execute(q).await;
        assert!(r.errors.is_empty(), "{q}: {:?}", r.errors);
        let d = serde_json::to_value(&r.data).unwrap();
        let n = d.as_object().unwrap().values().next().unwrap()["totalCount"]
            .as_i64()
            .unwrap();
        (n, r.extensions.contains_key("totalCountLowerBound"))
    }

    /// An account with a few postings is counted exactly under the default budget, with no flag.
    #[tokio::test]
    async fn a_small_account_is_counted_exactly_and_unflagged() {
        let fx = fixture();
        for q in QUERIES {
            let (n, flagged) = total(&fx.schema, q).await;
            assert!(n > 1, "{q}: the fixture account has several rows");
            assert!(!flagged, "{q}");
        }
    }

    /// A budget that is spent before the walk reads a key stops it at once: a flagged lower
    /// bound that never exceeds the true count (the page itself is still served). A sender
    /// list read from the sender index (FIX-2) is a key-only walk that reads the clock every
    /// 64 keys, so the fixture's few keys are counted exactly even with a spent budget.
    #[tokio::test]
    async fn a_spent_budget_answers_a_flagged_lower_bound() {
        let exact = fixture();
        let cut = fixture_with(GraphqlSettings {
            count_budget: Duration::ZERO,
            ..GraphqlSettings::default()
        });
        let (full, _) = total(&exact.schema, QUERIES[1]).await;
        assert_eq!(total(&cut.schema, QUERIES[1]).await, (full, false));
        for q in &QUERIES[..1] {
            let (full, _) = total(&exact.schema, q).await;
            let (n, flagged) = total(&cut.schema, q).await;
            assert!(flagged, "{q}: a cut count carries totalCountLowerBound");
            assert!(
                (0..=full).contains(&n),
                "{q}: {n} is a lower bound of {full}"
            );
            assert!(n < full, "{q}: nothing is counted past a spent budget");
        }
        let page = run(
            &cut.schema,
            "{ transfers(accountName: \"k:alice\", first: 2) { totalCount edges { cursor } } }",
        )
        .await;
        assert_eq!(page["transfers"]["edges"].as_array().unwrap().len(), 2);
    }

    /// FIX-2 fix 6: the old decode path (`sender_index = false`, or a store add-index has not
    /// finished) stays live, and there a sender list decodes every record of the account's
    /// `acct` list: a spent budget answers a flagged lower bound below the true count.
    #[tokio::test]
    async fn a_spent_budget_cuts_the_old_sender_path() {
        let exact = fixture();
        let cut = fixture_with(GraphqlSettings {
            count_budget: Duration::ZERO,
            ..GraphqlSettings::default()
        });
        let q = QUERIES[1];
        // The sender index answers the same spent-budget count exactly (see above) ...
        let (full, _) = total(&exact.schema, q).await;
        assert_eq!(total(&cut.schema, q).await, (full, false));
        // ... and the old path, switched on for both stores, is cut.
        for fx in [&exact, &cut] {
            fx.store.set_sender_index(false);
        }
        let (full, flagged) = total(&exact.schema, q).await;
        assert!(
            full > 1 && !flagged,
            "{q}: exact on the old path with a budget"
        );
        let (n, flagged) = total(&cut.schema, q).await;
        assert!(flagged, "{q}: a cut count carries totalCountLowerBound");
        assert!(n < full, "{q}: {n} is below {full} past a spent budget");
    }

    /// The budget bounds an account's walk only: an event-name or public-key count with a spent
    /// budget is exact and unflagged, as before.
    #[tokio::test]
    async fn a_non_account_count_ignores_the_budget() {
        let exact = fixture();
        let cut = fixture_with(GraphqlSettings {
            count_budget: Duration::ZERO,
            ..GraphqlSettings::default()
        });
        for q in [
            "{ events(qualifiedEventName: \"coin.TRANSFER\", first: 1) { totalCount } }".to_string(),
            format!(
                "{{ transactionsByPublicKey(publicKey: \"{ALICE_KEY}\", first: 1) {{ totalCount }} }}"
            ),
        ] {
            let (full, _) = total(&exact.schema, &q).await;
            let (n, flagged) = total(&cut.schema, &q).await;
            assert!(full > 1, "{q}: the fixture has several rows");
            assert_eq!((n, flagged), (full, false), "{q}");
        }
    }

    /// A cut count is never below the edges served in the same response.
    #[tokio::test]
    async fn a_cut_count_covers_the_edges_served() {
        let cut = fixture_with(GraphqlSettings {
            count_budget: Duration::ZERO,
            ..GraphqlSettings::default()
        });
        for field in ["transfers", "transactions"] {
            let d = run(
                &cut.schema,
                &format!(
                    "{{ {field}(accountName: \"k:alice\", first: 3) {{ totalCount edges {{ cursor }} }} }}"
                ),
            )
            .await;
            let edges = d[field]["edges"].as_array().unwrap().len() as i64;
            let n = d[field]["totalCount"].as_i64().unwrap();
            assert!(edges > 1, "{field}: several edges are served");
            assert!(n >= edges, "{field}: totalCount {n} < {edges} edges served");
        }
    }

    /// A walk cut partway answers a count above zero and below the true one, flagged. The
    /// account has thousands of postings and the budget is a few ms: the walk takes far longer
    /// (debug or release) yet gets past its first records.
    #[tokio::test]
    async fn a_walk_cut_partway_is_a_flagged_partial_count() {
        let n_extra = 8000;
        let exact = fixture_big(
            GraphqlSettings {
                count_budget: Duration::from_secs(60),
                ..GraphqlSettings::default()
            },
            n_extra,
        );
        let cut = fixture_big(
            GraphqlSettings {
                count_budget: Duration::from_millis(5),
                ..GraphqlSettings::default()
            },
            n_extra,
        );
        for q in QUERIES {
            let (full, flagged) = total(&exact.schema, q).await;
            assert!(full > i64::from(n_extra), "{q}");
            assert!(!flagged, "{q}");
            let (n, flagged) = total(&cut.schema, q).await;
            assert!(flagged, "{q}: a cut count carries totalCountLowerBound");
            assert!(n > 1 && n < full, "{q}: {n} is partway to {full}");
        }
    }

    /// A block's or a request key's list is counted with its page, exactly, whatever the budget.
    #[tokio::test]
    async fn a_block_list_ignores_the_budget() {
        let cut = fixture_with(GraphqlSettings {
            count_budget: Duration::ZERO,
            ..GraphqlSettings::default()
        });
        let q = format!(
            "{{ transfers(blockHash: \"{}\", first: 1) {{ totalCount }} }}",
            block_hash(0, 6)
        );
        let (n, flagged) = total(&cut.schema, &q).await;
        assert!(n > 0 && !flagged);
    }

    /// Timing of the count walk on a long account, run by hand (`--ignored --nocapture`, best
    /// in release): `KDX_COUNT_BENCH_TXS` extra txs (default 6000). Prints the time and answer
    /// of `totalCount` with the budget off (a minute, so half the 5 s deadline decides: the
    /// walk as it was) and with the default 200 ms budget and a 10 ms one (the fixture's hot records decode in
    /// microseconds, far faster than cold segment records, so only a tight budget cuts it).
    #[tokio::test]
    #[ignore = "timing run"]
    async fn timing_of_the_count_walk_on_a_long_account() {
        let n: u32 = std::env::var("KDX_COUNT_BENCH_TXS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(6000);
        let default = GraphqlSettings::default();
        for (label, budget) in [
            ("before (half the deadline)", Duration::from_secs(60)),
            ("after (200 ms budget)", default.count_budget),
            ("after (10 ms budget)", Duration::from_millis(10)),
        ] {
            let fx = fixture_big(
                GraphqlSettings {
                    count_budget: budget,
                    ..default.clone()
                },
                n,
            );
            for q in QUERIES {
                for round in 0..3 {
                    let t = std::time::Instant::now();
                    let (rows, flagged) = total(&fx.schema, q).await;
                    println!(
                        "{label}: {} round {round}: {:>7.1} ms, totalCount {rows}, lower bound {flagged}",
                        &q[2..14],
                        t.elapsed().as_secs_f64() * 1e3
                    );
                }
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
        assert_eq!(chain_ids_of(&v), ["0", "1"], "{vars}: {v}");
    }
    let v = run(
        &fx.schema,
        &format!("{{ fungibleChainAccounts(accountName: \"{ALICE}\") {{ chainId }} }}"),
    )
    .await;
    assert_eq!(v["fungibleChainAccounts"].as_array().map(Vec::len), Some(2));
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

    /// F-50: a block's `totalCount` (counted with the page) that the deadline cuts is a
    /// flagged lower bound, as a deferred count is, not an exact 0.
    #[tokio::test]
    async fn a_bounded_count_cut_by_the_deadline_is_flagged() {
        let fx = fixture();
        let q = format!(
            "{{ transfers(blockHash: \"{}\") {{ totalCount }} }}",
            block_hash(0, 6)
        );
        let req = async_graphql::Request::new(q).data(kdx_graphql::ReqCtx {
            deadline: std::time::Instant::now(),
        });
        let r = fx.schema.execute(req).await;
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert!(
            r.extensions.contains_key("totalCountLowerBound"),
            "{:?}",
            r.extensions
        );
        // With time, the same count is exact and unflagged.
        let r = fx
            .schema
            .execute(format!(
                "{{ transfers(blockHash: \"{}\") {{ totalCount }} }}",
                block_hash(0, 6)
            ))
            .await;
        assert!(!r.extensions.contains_key("totalCountLowerBound"));
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
        assert_eq!(run(&fx.schema, &q(", chainIds: []")).await, all);
        let many = format!(", chainIds: [{}]", vec!["\"0\""; 101].join(", "));
        let r = fx.schema.execute(q(&many)).await;
        assert_eq!(code(&r), bad_input(), "{:?}", r.errors);
        let first = edges(&all, "balance")[0]["cursor"].as_str().unwrap();
        let v = run(&fx.schema, &q(&format!(", after: \"{first}\", first: 0"))).await;
        assert!(edges(&v, "balance").is_empty(), "{v}");
    }
}
