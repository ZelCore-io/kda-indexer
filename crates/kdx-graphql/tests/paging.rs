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
        "reqKey": b64url_encode(&hash_of(0xCB, c, h)), "logs": null, "events": [],
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
    /// User tx request keys by block, in position order.
    rks: HashMap<(u8, u32), Vec<String>>,
}

/// The store: [`BLOCKS`] below [`FRONTIER`] in two frozen step ranges (as the freezer writes
/// them), the rest applied to the hot store by the tail writer; Alice holds coin on both
/// chains and NFTs on chain 0, so her account nodes resolve.
fn fixture() -> Fx {
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
    t.commit().unwrap();

    let store = Arc::new(Store::open(hot, d.path()).unwrap());
    let settings = ApiSettings {
        network: "mainnet01".into(),
        deadline: Duration::from_secs(5),
        max_in_flight: 16,
        max_body_bytes: 8 << 20,
        v1txs_max_body_bytes: 8 << 20,
        rate_per_ip: 20,
        rate_burst: 40,
        trusted_proxies: Vec::new(),
        read_threads: 2,
        ready_max_lag: 10,
        max_event_lists: 64,
    };
    let st = Api::new(store, settings, TipWatch::default()).state();
    Fx {
        _dir: d,
        schema: build_schema(st, GraphqlSettings::default(), None),
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
