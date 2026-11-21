//! D1: the balance seed's receivers pass marks `(receiver, coin, chain)` for accounts no event
//! names before coin v3, from frozen and hot txs; it runs once per store (`Meta
//! balances_seed_receivers`), resumes at its cursor, and picks up txs stored below the cutoff
//! after it ran.

mod common;

use std::sync::Arc;

use common::*;
use kdx_decode::receivers::record_receivers;
use kdx_decode::{decode_block, verify_block, VerifyReport};
use kdx_hot::balances::{self, meta};
use kdx_hot::{Table, Txn};
use kdx_ingest::apply::{self, Batch};
use kdx_ingest::seed_receivers::{self, pending};
use kdx_segment::Segments;
use kdx_types::hash::b64url_encode;
use kdx_types::{blake2b_256, Dict, DictKind, MemDict};
use serde_json::{json, Value};

fn h(n: u8) -> String {
    b64url_encode(&[n; 32])
}

const LEGACY: &str = "sasha-legacy";
const XRECV: &str = "k:3333333333333333333333333333333333333333333333333333333333333333";
const SENDER: &str = "k:4444444444444444444444444444444444444444444444444444444444444444";
const PK: &str = "2222222222222222222222222222222222222222222222222222222222222222";

fn user_tx(cmd: Value, cont: Value) -> (Vec<u8>, Vec<u8>) {
    let cmd = cmd.to_string();
    let rk = blake2b_256(cmd.as_bytes()).to_b64url();
    let tx = serde_json::to_vec(&json!({"hash": rk, "sigs": [], "cmd": cmd})).unwrap();
    let out = serde_json::to_vec(
        &json!({"gas": 1, "result": {"status": "success", "data": "Write succeeded"},
        "reqKey": rk, "logs": h(12), "events": [], "metaData": null,
        "continuation": cont, "txId": 7}),
    )
    .unwrap();
    (tx, out)
}

/// A chain-0 block at `height` with the two event-less repro shapes: a pre-event
/// `coin.transfer-create` to [`LEGACY`] and a cross-chain finish to [`XRECV`].
fn repro_block(height: u32) -> (kdx_decode::DecodedBlock, VerifyReport) {
    let exec = json!({"payload": {"exec": {"code":
        format!(r#"(coin.transfer-create "{SENDER}" "{LEGACY}" (read-keyset "ks") 2.0)"#),
        "data": {"ks": {"keys": [PK], "pred": "keys-all"}}}},
        "meta": {"sender": SENDER}, "signers": [], "nonce": format!("e{height}")});
    let cont_cmd = json!({"payload": {"cont": {"pactId": h(30), "step": 1, "rollback": false,
        "data": {}, "proof": "cHJvb2Y"}}, "meta": {"sender": "free-x-chain-gas"},
        "signers": [], "nonce": format!("c{height}")});
    let cont = json!({"pactId": h(30), "step": 1, "stepCount": 2, "executed": null,
        "stepHasRollback": false, "yield": null,
        "continuation": {"def": "coin.transfer-crosschain", "args": ["k:src", XRECV,
            {"keys": [PK], "pred": "keys-all"}, "0", 1.0]}});
    let txs: Vec<Value> = [user_tx(exec, Value::Null), user_tx(cont_cmd, cont)]
        .into_iter()
        .map(|(t, o)| json!([b64url_encode(&t), b64url_encode(&o)]))
        .collect();
    let cb = serde_json::to_vec(
        &json!({"gas": 0, "result": {"status": "success", "data": "ok"},
        "reqKey": h(9), "logs": null, "events": [], "metaData": null,
        "continuation": null, "txId": null}),
    )
    .unwrap();
    let miner = br#"{"account":"k:m","predicate":"keys-all","public-keys":["m"]}"#;
    let header = json!({
        "nonce": "1", "creationTime": 1, "parent": hh(height - 1, 0), "adjacents": {},
        "target": h(3), "payloadHash": h(4), "chainId": 0, "weight": h(5), "height": height,
        "chainwebVersion": "mainnet01", "epochStart": 1, "featureFlags": 0,
        "hash": hh(height, 0)
    });
    let payload = json!({
        "transactions": txs, "minerData": b64url_encode(miner),
        "transactionsHash": h(7), "outputsHash": h(8), "payloadHash": h(4),
        "coinbase": b64url_encode(&cb),
    });
    let b = decode_block(&header, &payload).expect("block decodes");
    verify_block(&b, &b.header.payload_hash).expect("consistency checks");
    (b, VerifyReport::default())
}

fn apply_repro(c: &kdx_ingest::Ctx, height: u32) {
    apply::commit(
        c,
        &[Batch {
            chain: 0,
            fork: None,
            blocks: vec![repro_block(height)],
            halted: false,
            halt_reason: "",
        }],
    )
    .unwrap();
}

/// Pool blocks at heights 0..24 (0..20 frozen), the repro block at 24 (hot).
fn store() -> (tempfile::TempDir, kdx_ingest::Ctx) {
    let pool = pool();
    let all: Vec<usize> = (0..POOL_LEN).collect();
    let d = tempfile::tempdir().unwrap();
    let c = ctx(d.path(), 2, 1000, 2);
    apply_linear(&c, &pool, &branch(&all, 0, 0, 0), 5);
    while kdx_ingest::freezer::tick(&c).unwrap().is_some() {}
    apply_repro(&c, 24);
    (d, c)
}

fn dirty(c: &kdx_ingest::Ctx, name: &str) -> bool {
    let txn = c.hot.ro().unwrap();
    let coin = balances::coin_module(&txn).unwrap().unwrap();
    let d = kdx_hot::HotDict::load(&txn).unwrap();
    let Some(a) = d.find(DictKind::Account, name) else {
        return false;
    };
    balances::is_dirty(&txn, a, coin, 0).unwrap()
}

/// Every receiver of the frozen user txs, as the pass must derive them.
fn frozen_receivers(c: &kdx_ingest::Ctx) -> Vec<String> {
    let dict: Arc<MemDict> = Arc::new(
        kdx_hot::HotDict::load(&c.hot.ro().unwrap())
            .unwrap()
            .mem()
            .clone(),
    );
    let segs = Segments::open_without_filters(&c.segdir, dict.clone()).unwrap();
    let mut v = Vec::new();
    for t in 0..segs.manifest.next_txnum {
        if let Some(r) = segs.tx(t).unwrap() {
            v.extend(record_receivers(&r, &*dict).unwrap());
        }
    }
    v
}

#[test]
fn the_pass_marks_event_less_receivers_once() {
    let (_d, c) = store();
    assert!(!dirty(&c, LEGACY) && !dirty(&c, XRECV));
    assert!(pending(&c.hot.ro().unwrap()).unwrap().is_some());
    let expected = frozen_receivers(&c);
    assert!(!expected.is_empty(), "the pool's coin txs name accounts");

    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &|| false).unwrap();
    assert!(!rep.stopped);
    assert!(rep.frozen_txs > 0 && rep.hot_txs >= 2, "{rep:?}");
    assert!(dirty(&c, LEGACY), "pre-event transfer-create receiver");
    assert!(dirty(&c, XRECV), "cross-chain finish receiver");
    for a in &expected {
        assert!(dirty(&c, a), "frozen receiver {a}");
    }
    let txn = c.hot.ro().unwrap();
    assert_eq!(
        txn.meta(meta::BALANCES_SEED_RECEIVERS).unwrap().as_deref(),
        Some(&25u32.to_be_bytes()[..])
    );
    assert!(pending(&txn).unwrap().is_none());
    drop(txn);

    // Gated: a second call walks nothing.
    let again = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &|| false).unwrap();
    assert_eq!((again.hot_txs, again.frozen_txs), (0, 0), "{again:?}");

    // A block stored below the cutoff afterwards (a tail behind coin v3) is walked alone.
    apply_repro(&c, 25);
    assert_eq!(pending(&c.hot.ro().unwrap()).unwrap(), Some((25, 26)));
    let more = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &|| false).unwrap();
    assert_eq!((more.hot_txs, more.frozen_txs), (2, 0), "{more:?}");
}

#[test]
fn a_stopped_pass_resumes_and_marks_the_same_rows() {
    let (_d, full) = store();
    seed_receivers::run(&full.hot, &full.dict, &full.segdir, &|| false).unwrap();

    let (_d2, c) = store();
    let mut stops = 0;
    loop {
        let calls = std::cell::Cell::new(0);
        // Stop at the second poll: each run makes a little progress.
        let rep = seed_receivers::run_with(&c.hot, &c.dict, &c.segdir, 7, &|| {
            calls.set(calls.get() + 1);
            calls.get() > 1
        })
        .unwrap();
        if !rep.stopped {
            break;
        }
        stops += 1;
        assert!(pending(&c.hot.ro().unwrap()).unwrap().is_some());
        assert!(stops < 1000);
    }
    assert!(stops > 1, "the pass must have been stopped and resumed");
    let rows = |c: &kdx_ingest::Ctx| {
        let txn = c.hot.ro().unwrap();
        let d = kdx_hot::HotDict::load(&txn).unwrap();
        let mut v: Vec<(String, u8)> = txn
            .scan(Table::BalanceDirty, &[], |_| true)
            .unwrap()
            .into_iter()
            .map(|(k, _)| {
                let (a, _, ch) = balances::parse_bal_key(&k).unwrap();
                (d.name(DictKind::Account, a).unwrap().to_owned(), ch)
            })
            .collect();
        v.sort();
        v
    };
    assert_eq!(rows(&c), rows(&full));
}
