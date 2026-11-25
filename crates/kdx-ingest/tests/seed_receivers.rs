//! D1: the balance seed's receivers pass marks `(receiver, coin, chain)` for accounts no event
//! names before coin v3, from frozen and hot txs; it runs once per store (`Meta
//! balances_seed_receivers`), resumes at its cursor, and picks up txs stored below the cutoff
//! after it ran.

mod common;

use std::sync::Arc;

use common::*;
use kdx_decode::receivers::record_receivers;
use kdx_decode::receivers::MAINNET01;
use kdx_decode::{decode_block, verify_block, VerifyReport};
use kdx_hot::balances::{self, meta};
use kdx_hot::{Table, Txn};
use kdx_ingest::apply::{self, Batch};
use kdx_ingest::seed_receivers::{self, Opts, TX_CHUNK};
use kdx_segment::Segments;
use kdx_types::hash::b64url_encode;
use kdx_types::{blake2b_256, Dict, DictKind, DictMut, MemDict};
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
    // The pass's own marks only (the tail marks D3 rows itself since fix round 1).
    clear_marks(&c);
    (d, c)
}

fn pending(txn: &impl Txn) -> anyhow::Result<Option<(u32, u32)>> {
    seed_receivers::pending(txn, MAINNET01)
}

fn opts(chunk: usize) -> Opts {
    Opts {
        chunk,
        ..Opts::default()
    }
}

fn run(c: &kdx_ingest::Ctx) -> seed_receivers::ReceiversReport {
    seed_receivers::run(&c.hot, &c.dict, &c.segdir, &Opts::default(), &|| false).unwrap()
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
            v.extend(record_receivers(&r, &*dict, MAINNET01).unwrap());
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

    let rep = run(&c);
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
    let again = run(&c);
    assert_eq!((again.hot_txs, again.frozen_txs), (0, 0), "{again:?}");

    // A block stored below the cutoff afterwards (a tail behind coin v3) is walked alone.
    apply_repro(&c, 25);
    assert_eq!(pending(&c.hot.ro().unwrap()).unwrap(), Some((25, 26)));
    let more = run(&c);
    assert_eq!((more.hot_txs, more.frozen_txs), (2, 0), "{more:?}");
}

#[test]
fn a_stopped_pass_resumes_and_marks_the_same_rows() {
    let (_d, full) = store();
    run(&full);

    let (_d2, c) = store();
    let mut stops = 0;
    loop {
        let calls = std::cell::Cell::new(0);
        // Stop at the second poll: each run makes a little progress.
        let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &opts(7), &|| {
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

/// Give `name` a verified `Balances` row on chain 0 (interning it as a posting would have).
fn put_row(c: &kdx_ingest::Ctx, name: &str) {
    put_row_with(c, name, false);
}

/// Like [`put_row`], with the row's `unverified` flag (I-5) set as given.
fn put_row_with(c: &kdx_ingest::Ctx, name: &str, unverified: bool) {
    let mut d = c.dict.lock().unwrap();
    let mut txn = c.hot.rw().unwrap();
    let a = d.intern(DictKind::Account, name);
    d.persist(&mut txn).unwrap();
    let coin = balances::coin_module(&txn).unwrap().unwrap();
    let row = balances::BalanceRow {
        absent: false,
        balance: "2.0".into(),
        guard: [1; 16],
        as_of: 24,
        reconciled_at: 24,
        unverified,
    };
    txn.put(
        Table::Balances,
        &balances::bal_key(a, coin, 0),
        &row.encode(),
    )
    .unwrap();
    txn.commit().unwrap();
}

#[test]
fn keys_that_already_have_a_balances_row_are_not_marked() {
    // M2 (b): a row the tail keeps fresh is correct; only absent keys are marked.
    let (_d, c) = store();
    put_row(&c, LEGACY);
    let rep = run(&c);
    assert!(!dirty(&c, LEGACY), "LEGACY has a row: not re-read");
    assert!(dirty(&c, XRECV), "XRECV has no row: marked");
    assert_eq!(rep.skipped_existing, 1, "{rep:?}");
}

#[test]
fn absent_keys_are_marked() {
    // N2: no Balances row at all: both receivers are marked, nothing is skipped.
    let (_d, c) = store();
    let rep = run(&c);
    assert!(dirty(&c, LEGACY) && dirty(&c, XRECV));
    assert_eq!(rep.skipped_existing, 0, "{rep:?}");
}

#[test]
fn an_unverified_row_is_marked_like_an_absent_key() {
    // N1/N2: the last re-read of an unverified row failed; the tail will not refresh it, so
    // the pass marks it. A verified row is still skipped.
    let (_d, c) = store();
    put_row_with(&c, LEGACY, true);
    put_row(&c, XRECV);
    let rep = run(&c);
    assert!(dirty(&c, LEGACY), "unverified row: marked");
    assert!(!dirty(&c, XRECV), "verified row: skipped");
    assert_eq!(rep.skipped_existing, 1, "{rep:?}");
}

#[test]
fn a_dry_run_counts_skipped_keys_like_a_real_run() {
    // N2: a verified row is `skipped_existing`, an unverified one is a would-be mark.
    let setup = || {
        let (d, c) = store();
        put_row(&c, LEGACY);
        put_row_with(&c, XRECV, true);
        (d, c)
    };
    let (_d, real) = setup();
    let want = run(&real);
    assert_eq!(want.skipped_existing, 1, "{want:?}");

    let (_d2, c) = setup();
    let rows0 = dirty_rows(&c);
    let o = Opts {
        dry_run: true,
        ..Opts::default()
    };
    let got = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| false).unwrap();
    assert!(got.dry_run);
    assert_eq!(got.skipped_existing, 1, "{got:?}");
    assert_eq!(
        got.dirty_inserted, want.dirty_inserted,
        "{got:?} vs {want:?}"
    );
    assert_eq!(dirty_rows(&c), rows0, "a dry run writes no marks");
}

fn dirty_rows(c: &kdx_ingest::Ctx) -> usize {
    balances::dirty_count(&c.hot.ro().unwrap()).unwrap()
}

#[test]
fn a_dry_run_counts_without_writing() {
    let (_d, real) = store();
    let want = run(&real);
    assert!(want.dirty_inserted > 0 && want.new_accounts > 0, "{want:?}");

    let (_d2, c) = store();
    let accounts = |c: &kdx_ingest::Ctx| {
        kdx_hot::HotDict::load(&c.hot.ro().unwrap())
            .unwrap()
            .len(DictKind::Account)
    };
    let (rows0, names0) = (dirty_rows(&c), accounts(&c));
    let o = Opts {
        dry_run: true,
        ..Opts::default()
    };
    let got = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| false).unwrap();
    assert!(got.dry_run);
    assert_eq!(
        got.dirty_inserted, want.dirty_inserted,
        "{got:?} vs {want:?}"
    );
    assert_eq!(got.new_accounts, want.new_accounts, "{got:?} vs {want:?}");
    assert_eq!(got.skipped_existing, want.skipped_existing);
    // Nothing written: no marks, no names, no flag or cursor; the pass is still pending.
    assert_eq!((dirty_rows(&c), accounts(&c)), (rows0, names0));
    assert_eq!(c.dict.lock().unwrap().len(DictKind::Account), names0);
    let txn = c.hot.ro().unwrap();
    assert!(txn.meta(meta::BALANCES_SEED_RECEIVERS).unwrap().is_none());
    assert!(txn
        .meta(meta::BALANCES_SEED_RECEIVERS_CURSOR)
        .unwrap()
        .is_none());
    assert!(pending(&txn).unwrap().is_some());
}

#[test]
fn the_pass_refuses_a_store_without_a_tail() {
    // L3: names interned before a backfill resume could be truncated and their ids reused.
    let (_d, c) = store();
    let mut txn = c.hot.rw().unwrap();
    for ch in 0..kdx_segment::SLOT_WIDTH as u8 {
        kdx_hot::checkpoint::clear_checkpoint(&mut txn, kdx_hot::Stage::Tail, ch).unwrap();
    }
    txn.commit().unwrap();
    let rows0 = dirty_rows(&c);
    let e = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &Opts::default(), &|| false)
        .unwrap_err()
        .to_string();
    assert!(e.contains("no tail"), "{e}");
    assert_eq!(dirty_rows(&c), rows0, "nothing marked");
    assert!(!dirty(&c, LEGACY));
}

#[test]
fn the_pass_refuses_a_store_whose_backfill_is_incomplete() {
    // L3: MDBX ahead of the segment manifest (a range committed, its manifest not).
    let (_d, c) = store();
    let mut txn = c.hot.rw().unwrap();
    let f = txn.frozen_frontier().unwrap();
    txn.set_frozen_frontier(f + 1).unwrap();
    txn.commit().unwrap();
    let e = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &Opts::default(), &|| false)
        .unwrap_err()
        .to_string();
    assert!(e.contains("backfill"), "{e}");
}

#[test]
fn a_tx_whose_cmd_is_not_json_is_counted_and_kept_in_meta() {
    // L1: never silent; `--status` shows the stored count.
    let (_d, c) = store();
    let mut txn = c.hot.rw().unwrap();
    let (k, v) = txn
        .scan(Table::TailTxs, &[], |_| true)
        .unwrap()
        .into_iter()
        .find(|(k, _)| !kdx_types::TxKey::from_be_bytes(k[..8].try_into().unwrap()).is_coinbase())
        .unwrap();
    let mut r = kdx_types::record::decode_tx(&v).unwrap();
    r.cmd = Some(b"{not json".to_vec());
    let mut buf = Vec::new();
    kdx_types::record::encode_tx(&r, &mut buf);
    txn.put(Table::TailTxs, &k, &buf).unwrap();
    txn.commit().unwrap();
    let rep = run(&c);
    assert_eq!(rep.errors, 1, "{rep:?}");
    let txn = c.hot.ro().unwrap();
    assert_eq!(
        txn.meta(meta::BALANCES_SEED_RECEIVERS_ERRORS)
            .unwrap()
            .as_deref(),
        Some(&1u64.to_be_bytes()[..])
    );
}

// ---------------------------------------------------------------------------------------------
// D3: the token receivers pass (`Rules::D3`).
// ---------------------------------------------------------------------------------------------

const INJ: &str = "4069e72f62deb9dde8ee0686ee2908580131128b0a350e23890e9b46e61f166d";
const WRECV: &str = "ZELCORE WIZA-KDA";
const UNWRAP_ACC: &str = "k:5555555555555555555555555555555555555555555555555555555555555555";
const SKDX_ACC: &str = "k:6666666666666666666666666666666666666666666666666666666666666666";
/// Shaped like an injected miner account, but only ever a tx sender: never a coinbase miner.
const FAKE_KEY: &str = "7777777777777777777777777777777777777777777777777777777777777777";
const OZIL: &str = "n_95d7fe012aa7e05c187b3fc8c605ff3b1a2c521d.MesutÖzilDönerKebabMerkel42Inu";

fn event(module: &str, name: &str, params: Value) -> Value {
    let (ns, n) = module
        .split_once('.')
        .map_or((Value::Null, module), |(a, b)| (a.into(), b));
    json!({"params": params, "name": name, "module": {"namespace": ns, "name": n},
        "moduleHash": h(40)})
}

fn d3_tx(n: u32, cmd: Value, events: Value, cont: Value) -> Value {
    let mut cmd = cmd;
    cmd["nonce"] = json!(format!("d3-{n}"));
    cmd["signers"] = json!([]);
    let cmd = cmd.to_string();
    let rk = blake2b_256(cmd.as_bytes()).to_b64url();
    let tx = serde_json::to_vec(&json!({"hash": rk, "sigs": [], "cmd": cmd})).unwrap();
    let out = serde_json::to_vec(
        &json!({"gas": 1, "result": {"status": "success", "data": "Write succeeded"},
        "reqKey": rk, "logs": h(12), "events": events, "metaData": null,
        "continuation": cont, "txId": 7}),
    )
    .unwrap();
    json!([b64url_encode(&tx), b64url_encode(&out)])
}

fn xchain_finish(n: u32, module: &str, receiver: &str) -> Value {
    let cmd = json!({"payload": {"cont": {"pactId": h(50 + n as u8), "step": 1,
        "rollback": false, "data": {}, "proof": "cHJvb2Y"}}, "meta": {"sender": "gas-station"}});
    let cont = json!({"pactId": h(50 + n as u8), "step": 1, "stepCount": 2, "executed": null,
        "stepHasRollback": false, "yield": null,
        "continuation": {"def": format!("{module}.transfer-crosschain"),
            "args": [SENDER, receiver, {"keys": [PK], "pred": "keys-all"}, "0", 1.0]}});
    d3_tx(n, cmd, json!([]), cont)
}

/// A chain-0 block at `height`: an injected miner account (credits [`INJ`]), a `free.wiza`
/// cross-chain finish to [`WRECV`], a `kaddex.kdx` TRANSFER (so the token is tracked) with an
/// UNWRAP to [`UNWRAP_ACC`], an alchemist WRAP minting `kaddex.skdx` to [`SKDX_ACC`], and a
/// cross-chain finish of a module whose name `/local` cannot parse ([`OZIL`]).
fn d3_block(height: u32) -> (kdx_decode::DecodedBlock, VerifyReport) {
    let skdx = json!({"refSpec": [{"namespace": null, "name": "fungible-v2"}],
        "refName": {"namespace": "kaddex", "name": "skdx"}});
    let fake = format!(r#"k:f" (read-keyset "miner-keyset") 1.0)(coin.coinbase "{FAKE_KEY}"#);
    let staking = json!({"payload": {"exec": {"code": "(kaddex.staking.claim)", "data": {}}},
        "meta": {"sender": fake}});
    let evs = json!([
        event("kaddex.kdx", "TRANSFER", json!([SENDER, "k:kdx-pool", 1.0])),
        event(
            "kaddex.kdx",
            "UNWRAP",
            json!(["kdx-staking", UNWRAP_ACC, 2.0])
        ),
        event(
            "kaddex.alchemist",
            "WRAP",
            json!([skdx, 1.0, SKDX_ACC, SKDX_ACC])
        ),
    ]);
    let txs = vec![
        xchain_finish(1, "free.wiza", WRECV),
        d3_tx(2, staking, evs, Value::Null),
        xchain_finish(3, OZIL, SKDX_ACC),
    ];
    let cb = serde_json::to_vec(
        &json!({"gas": 0, "result": {"status": "success", "data": "ok"},
        "reqKey": h(9), "logs": null, "events": [], "metaData": null,
        "continuation": null, "txId": null}),
    )
    .unwrap();
    let account = format!(r#"k:m" (read-keyset "miner-keyset") 1.0)(coin.coinbase "{INJ}"#);
    let miner = serde_json::to_vec(
        &json!({"account": account, "predicate": "keys-all", "public-keys": ["m"]}),
    )
    .unwrap();
    let header = json!({
        "nonce": "1", "creationTime": 1, "parent": hh(height - 1, 0), "adjacents": {},
        "target": h(3), "payloadHash": h(4), "chainId": 0, "weight": h(5), "height": height,
        "chainwebVersion": "mainnet01", "epochStart": 1, "featureFlags": 0,
        "hash": hh(height, 0)
    });
    let payload = json!({
        "transactions": txs, "minerData": b64url_encode(&miner),
        "transactionsHash": h(7), "outputsHash": h(8), "payloadHash": h(4),
        "coinbase": b64url_encode(&cb),
    });
    let b = decode_block(&header, &payload).expect("block decodes");
    verify_block(&b, &b.header.payload_hash).expect("consistency checks");
    (b, VerifyReport::default())
}

fn d3_store() -> (tempfile::TempDir, kdx_ingest::Ctx) {
    let (d, c) = store();
    apply::commit(
        &c,
        &[Batch {
            chain: 0,
            fork: None,
            blocks: vec![d3_block(25)],
            halted: false,
            halt_reason: "",
        }],
    )
    .unwrap();
    clear_marks(&c);
    (d, c)
}

fn d3_opts(dry_run: bool) -> Opts {
    Opts {
        rules: seed_receivers::Rules::D3,
        dry_run,
        ..Opts::default()
    }
}

fn dirty_in(c: &kdx_ingest::Ctx, name: &str, module: &str) -> bool {
    dirty_on(c, name, module, 0)
}

fn dirty_on(c: &kdx_ingest::Ctx, name: &str, module: &str, chain: u8) -> bool {
    let txn = c.hot.ro().unwrap();
    let d = kdx_hot::HotDict::load(&txn).unwrap();
    let (Some(a), Some(m)) = (
        d.find(DictKind::Account, name),
        d.find(DictKind::Module, module),
    ) else {
        return false;
    };
    balances::is_dirty(&txn, a, m, chain).unwrap()
}

fn tracked(c: &kdx_ingest::Ctx, module: &str) -> bool {
    let txn = c.hot.ro().unwrap();
    let d = kdx_hot::HotDict::load(&txn).unwrap();
    d.find(DictKind::Module, module)
        .is_some_and(|m| balances::token_row(&txn, m).unwrap().is_some())
}

#[test]
fn the_token_pass_marks_credits_no_transfer_names_and_discovers_tokens() {
    let (_d, c) = d3_store();
    let rows = [
        (INJ, "coin"),
        (WRECV, "free.wiza"),
        (UNWRAP_ACC, "kaddex.kdx"),
        (SKDX_ACC, "kaddex.skdx"),
    ];
    for (a, m) in rows {
        assert!(!dirty_in(&c, a, m), "{a} {m} dirty before the pass");
    }
    assert!(tracked(&c, "kaddex.kdx") && !tracked(&c, "kaddex.skdx"));

    // The dry run counts what the real run marks, and writes nothing.
    let dry = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(true), &|| false).unwrap();
    for (a, m) in rows {
        assert!(!dirty_in(&c, a, m), "dry run marked {a} {m}");
    }
    assert!(!tracked(&c, "kaddex.skdx"));
    assert!(c
        .hot
        .ro()
        .unwrap()
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)
        .unwrap()
        .is_none());

    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    assert_eq!(rep.errors, 0, "{rep:?}");
    for (a, m) in rows {
        assert!(dirty_in(&c, a, m), "{a} {m} not marked: {rep:?}");
    }
    // Discovered modules are only queued; the node's describe-module check tracks them
    // (review M1).
    assert!(!tracked(&c, "kaddex.skdx") && !tracked(&c, "free.wiza"));
    let q = balances::discovered_queue(&c.hot.ro().unwrap(), 100).unwrap();
    let names: Vec<&str> = q.iter().map(|(m, _, _)| m.as_str()).collect();
    assert!(names.contains(&"kaddex.skdx"), "{q:?}");
    assert!(names.contains(&"free.wiza"), "{q:?}");
    let yes = vec![Some(true); q.len()];
    seed_receivers::check_discovered(&c.hot, &c.dict, &q, &yes).unwrap();
    assert!(
        tracked(&c, "kaddex.skdx"),
        "fungible-v2 modref discovers the token"
    );
    assert!(
        tracked(&c, "free.wiza"),
        "a token's cross-chain defpact discovers it"
    );
    assert!(
        !tracked(&c, OZIL),
        "a module name /local cannot parse is never read"
    );
    assert!(!dirty_in(&c, SKDX_ACC, OZIL));
    assert_eq!(
        (dry.dirty_inserted, dry.new_accounts, dry.tokens_noted),
        (rep.dirty_inserted, rep.new_accounts, rep.tokens_noted),
        "dry {dry:?} real {rep:?}"
    );
    assert_eq!(rep.injected_miners, 1, "{rep:?}");
    // The miner's credit is marked on its block's chain only; a non-miner name of the same
    // shape gives nothing.
    for ch in 1..20u8 {
        assert!(!dirty_on(&c, INJ, "coin", ch), "chain {ch}");
    }
    assert!(!dirty_in(&c, FAKE_KEY, "coin"));

    let txn = c.hot.ro().unwrap();
    assert_eq!(
        txn.meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)
            .unwrap()
            .as_deref(),
        Some(&26u32.to_be_bytes()[..])
    );
    assert!(seed_receivers::pending_for(&txn, &d3_opts(false))
        .unwrap()
        .is_none());
    // The D1 flag is separate: D3 does not mark D1 done, nor the reverse.
    assert!(txn.meta(meta::BALANCES_SEED_RECEIVERS).unwrap().is_none());
    drop(txn);
    let again =
        seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    assert_eq!((again.hot_txs, again.frozen_txs), (0, 0), "{again:?}");
}

#[test]
fn a_token_at_its_budget_is_dropped_and_counted_and_the_pass_completes() {
    let (_d, c) = d3_store();
    let o = Opts {
        max_event_candidates: 1,
        ..d3_opts(false)
    };
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| false).unwrap();
    // kaddex.kdx's UNWRAP names two strings: one is over its budget of 1.
    assert_eq!(rep.dropped_tokens.get("kaddex.kdx"), Some(&1), "{rep:?}");
    assert!(
        !rep.dropped_tokens.contains_key("kaddex.alchemist"),
        "{rep:?}"
    );
    // Emitter B (kaddex.alchemist) is not starved by A, and uncapped rules still run.
    assert!(dirty_in(&c, SKDX_ACC, "kaddex.skdx"), "{rep:?}");
    assert!(dirty_in(&c, WRECV, "free.wiza") && dirty_in(&c, INJ, "coin"));
    // Done anyway: flag set, not pending, the drop in Meta for `--status`.
    let txn = c.hot.ro().unwrap();
    assert!(txn
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)
        .unwrap()
        .is_some());
    assert!(seed_receivers::pending_for(&txn, &o).unwrap().is_none());
    let d = txn
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS_DROPPED)
        .unwrap()
        .expect("drops recorded");
    assert!(String::from_utf8_lossy(&d).contains("kaddex.kdx"));
}

#[test]
fn a_full_untracked_bucket_drops_counts_and_still_completes() {
    let (_d, c) = d3_store();
    let o = Opts {
        max_untracked_candidates: 0,
        ..d3_opts(false)
    };
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| false).unwrap();
    // kaddex.alchemist (untracked) names kaddex.skdx rows: dropped, counted.
    assert!(rep.dropped_untracked >= 1, "{rep:?}");
    assert!(!dirty_in(&c, SKDX_ACC, "kaddex.skdx"));
    // Tracked tokens are unaffected, and the pass is done.
    assert!(rep.dropped_tokens.is_empty(), "{rep:?}");
    assert!(dirty_in(&c, UNWRAP_ACC, "kaddex.kdx"));
    let txn = c.hot.ro().unwrap();
    assert!(txn
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)
        .unwrap()
        .is_some());
    let d = txn
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS_DROPPED)
        .unwrap()
        .expect("drops recorded");
    let d: Value = serde_json::from_slice(&d).unwrap();
    assert_eq!(d["untracked"].as_u64(), Some(rep.dropped_untracked));
}

#[test]
fn the_global_ceiling_drops_only_untracked_candidates_and_completes() {
    let (_d, c) = d3_store();
    let o = Opts {
        max_total_candidates: 0,
        ..d3_opts(false)
    };
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| false).unwrap();
    assert!(
        rep.dropped_ceiling >= 1 && rep.dropped_tokens.is_empty(),
        "{rep:?}"
    );
    // The untracked emitter's row is dropped; a tracked token's row never is.
    assert!(!dirty_in(&c, SKDX_ACC, "kaddex.skdx"));
    assert!(dirty_in(&c, UNWRAP_ACC, "kaddex.kdx"), "{rep:?}");
    let txn = c.hot.ro().unwrap();
    assert!(txn
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)
        .unwrap()
        .is_some());
}

#[test]
fn a_resumed_token_pass_keeps_its_cumulative_drop_counts() {
    let o = |chunk| Opts {
        chunk,
        max_untracked_candidates: 0,
        ..d3_opts(false)
    };
    let (_d, full) = d3_store();
    let whole =
        seed_receivers::run(&full.hot, &full.dict, &full.segdir, &o(TX_CHUNK), &|| false).unwrap();
    let (_d2, c) = d3_store();
    let mut stops = 0;
    let last = loop {
        let calls = std::cell::Cell::new(0);
        let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o(7), &|| {
            calls.set(calls.get() + 1);
            calls.get() > 1
        })
        .unwrap();
        if !rep.stopped {
            break rep;
        }
        stops += 1;
        assert!(stops < 1000);
    };
    assert!(stops > 1, "the pass must have been stopped and resumed");
    assert!(whole.dropped_untracked >= 1, "{whole:?}");
    assert_eq!(
        (
            last.dropped_untracked,
            last.dropped_ceiling,
            last.event_candidates
        ),
        (
            whole.dropped_untracked,
            whole.dropped_ceiling,
            whole.event_candidates
        ),
        "resumed {last:?} whole {whole:?}"
    );
    // The stored drop counts match (`tracked_bound` may differ: a resumed run starts with the
    // tokens earlier chunks noted).
    let d = |c: &kdx_ingest::Ctx| {
        let v = c
            .hot
            .ro()
            .unwrap()
            .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS_DROPPED)
            .unwrap()
            .expect("drops recorded");
        let v: Value = serde_json::from_slice(&v).unwrap();
        (v["untracked"].as_u64(), v["ceiling"].as_u64())
    };
    assert_eq!(d(&c), d(&full));
}

// ---------------------------------------------------------------------------------------------
// Fix round 1 (review H1, H2, M3): the pass on a store whose tail marked no D3 rows.
// ---------------------------------------------------------------------------------------------

/// Drop every dirty and seed mark, like a store whose tail predates the live D3 marking.
fn clear_marks(c: &kdx_ingest::Ctx) {
    let mut txn = c.hot.rw().unwrap();
    for t in [Table::BalanceDirty, Table::BalanceSeed] {
        for (k, _) in txn.scan(t, &[], |_| true).unwrap() {
            txn.del(t, &k).unwrap();
        }
    }
    txn.commit().unwrap();
}

/// A verified `Balances` row of `(name, module, 0)` read at `as_of`.
fn put_row_at(c: &kdx_ingest::Ctx, name: &str, module: &str, as_of: u32) {
    let mut d = c.dict.lock().unwrap();
    let mut txn = c.hot.rw().unwrap();
    let a = d.intern(DictKind::Account, name);
    let m = d.intern(DictKind::Module, module);
    d.persist(&mut txn).unwrap();
    let row = balances::BalanceRow {
        absent: false,
        balance: "2.0".into(),
        guard: [1; 16],
        as_of,
        reconciled_at: 1,
        unverified: false,
    };
    txn.put(Table::Balances, &balances::bal_key(a, m, 0), &row.encode())
        .unwrap();
    txn.commit().unwrap();
}

#[test]
fn a_verified_row_older_than_the_credit_is_marked_and_a_newer_one_skipped() {
    let (_d, c) = d3_store();
    clear_marks(&c);
    // The UNWRAP credit is at height 25: a row read at 10 is stale, one read at 100 is not.
    put_row_at(&c, UNWRAP_ACC, "kaddex.kdx", 10);
    put_row_at(&c, SKDX_ACC, "kaddex.skdx", 100);
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    assert!(dirty_in(&c, UNWRAP_ACC, "kaddex.kdx"), "{rep:?}");
    assert!(!dirty_in(&c, SKDX_ACC, "kaddex.skdx"), "{rep:?}");
    assert!(rep.skipped_existing >= 1, "{rep:?}");
    // The mark carries the credit's height, like a posting's.
    let txn = c.hot.ro().unwrap();
    let d = kdx_hot::HotDict::load(&txn).unwrap();
    let k = balances::bal_key(
        d.find(DictKind::Account, UNWRAP_ACC).unwrap(),
        d.find(DictKind::Module, "kaddex.kdx").unwrap(),
        0,
    );
    let m = txn
        .get(Table::BalanceDirty, &k)
        .unwrap()
        .expect("tail-style mark");
    assert_eq!(balances::dirty_height(&m), Some(25));
}

/// A chain-0 block at `height` with `n` txs of a hostile token `free.spam`: each has a TRANSFER
/// (so the token is tracked) and a MINT naming the same 64 accounts.
fn spam_block(height: u32, n: u32) -> (kdx_decode::DecodedBlock, VerifyReport) {
    let names: Vec<Value> = (0..64).map(|i| json!(format!("k:spam-{i:02}"))).collect();
    let txs: Vec<Value> = (0..n)
        .map(|i| {
            let cmd = json!({"payload": {"exec": {"code": "(free.spam.go)", "data": {}}},
                "meta": {"sender": SENDER}});
            let evs = json!([
                event("free.spam", "TRANSFER", json!([SENDER, "k:spam-pool", 1.0])),
                event("free.spam", "MINT", json!(names.clone())),
            ]);
            d3_tx(1000 + i, cmd, evs, Value::Null)
        })
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

#[test]
fn a_hostile_token_repeating_strings_cannot_exhaust_its_budget_or_block_the_pass() {
    let (_d, c) = d3_store();
    apply::commit(
        &c,
        &[Batch {
            chain: 0,
            fork: None,
            blocks: vec![spam_block(26, 500)],
            halted: false,
            halt_reason: "",
        }],
    )
    .unwrap();
    clear_marks(&c);
    // 500 txs x 64 repeated names = 32,000 raw takes, but only 64 distinct rows.
    let o = Opts {
        max_event_candidates: 1_000,
        ..d3_opts(false)
    };
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| false).unwrap();
    assert!(dirty_in(&c, "k:spam-63", "free.spam"), "{rep:?}");
    assert!(dirty_in(&c, UNWRAP_ACC, "kaddex.kdx"), "{rep:?}");
    let txn = c.hot.ro().unwrap();
    assert!(
        txn.meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)
            .unwrap()
            .is_some(),
        "the pass completes: {rep:?}"
    );
}

#[test]
fn a_token_over_its_distinct_row_budget_is_dropped_alone_and_the_pass_completes() {
    let (_d, c) = d3_store();
    apply::commit(
        &c,
        &[Batch {
            chain: 0,
            fork: None,
            blocks: vec![spam_block(26, 3)],
            halted: false,
            halt_reason: "",
        }],
    )
    .unwrap();
    clear_marks(&c);
    // free.spam names 64 distinct rows (and kaddex.kdx 2): a budget of 10 drops free.spam only.
    let o = Opts {
        max_event_candidates: 10,
        ..d3_opts(false)
    };
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| false).unwrap();
    assert!(dirty_in(&c, UNWRAP_ACC, "kaddex.kdx"), "{rep:?}");
    assert!(!dirty_in(&c, "k:spam-63", "free.spam"), "{rep:?}");
    let txn = c.hot.ro().unwrap();
    assert!(
        txn.meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)
            .unwrap()
            .is_some(),
        "the pass completes: {rep:?}"
    );
    let d = txn
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS_DROPPED)
        .unwrap()
        .expect("drops recorded");
    let d: Value = serde_json::from_slice(&d).unwrap();
    // Drops count takes (a repeat of a dropped row counts again; keeping them distinct would
    // need memory an attacker controls): per tx the 54 MINT names over the 10 taken. The exec's
    // strings are charged to the tx sender, not to free.spam.
    assert_eq!(d["tokens"]["free.spam"].as_u64(), Some(3 * 54), "{d}");
    assert!(d["tokens"].get("kaddex.kdx").is_none(), "{d}");
}

#[test]
fn a_resumed_pass_ignores_a_stale_cap_hit_list() {
    let (_d, c) = d3_store();
    let o = Opts {
        chunk: 7,
        ..d3_opts(false)
    };
    let calls = std::cell::Cell::new(0);
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| {
        calls.set(calls.get() + 1);
        calls.get() > 1
    })
    .unwrap();
    assert!(rep.stopped, "{rep:?}");
    // A list left by an earlier pass under another cap.
    let mut txn = c.hot.rw().unwrap();
    txn.set_meta(
        "balances_seed_token_receivers_cap_hit",
        br#"{"cap":1,"emitters":["kaddex.kdx"]}"#,
    )
    .unwrap();
    txn.commit().unwrap();
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| false).unwrap();
    assert!(!rep.stopped, "{rep:?}");
    let txn = c.hot.ro().unwrap();
    assert!(
        txn.meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)
            .unwrap()
            .is_some(),
        "{rep:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// Fix round 1: live tail marks (ruling R-P5P9-192), discovery (M1), history walk (M2).
// ---------------------------------------------------------------------------------------------

/// A chain-0 block at `height` with the given txs (from [`d3_tx`] / [`xchain_finish`]).
fn block_of(height: u32, txs: Vec<Value>) -> (kdx_decode::DecodedBlock, VerifyReport) {
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

fn commit_block(c: &kdx_ingest::Ctx, b: (kdx_decode::DecodedBlock, VerifyReport)) {
    apply::commit(
        c,
        &[Batch {
            chain: 0,
            fork: None,
            blocks: vec![b],
            halted: false,
            halt_reason: "",
        }],
    )
    .unwrap();
}

/// The D3 rows the tail marked for block (0, `height`), from the record its undo reads.
fn tail_d3_rows(c: &kdx_ingest::Ctx, height: u32) -> std::collections::BTreeSet<(u32, u32)> {
    let txn = c.hot.ro().unwrap();
    let v = txn
        .meta(&balances::d3_undo_key(0, height))
        .unwrap()
        .unwrap_or_default();
    v.chunks_exact(16)
        .map(|e| {
            (
                u32::from_be_bytes(e[..4].try_into().unwrap()),
                u32::from_be_bytes(e[4..8].try_into().unwrap()),
            )
        })
        .collect()
}

/// The D3 rows the seed pass derives for the stored user txs of block (0, `height`) whose
/// names the dictionary holds, and how many it derives with a name it does not hold (the tail
/// defers those to the hourly pass).
fn pass_d3_rows(c: &kdx_ingest::Ctx, height: u32) -> (std::collections::BTreeSet<(u32, u32)>, u64) {
    let txn = c.hot.ro().unwrap();
    let d = kdx_hot::HotDict::load(&txn).unwrap();
    let tracked = |m: &str| {
        d.find(DictKind::Module, m)
            .and_then(|id| balances::token_row(&txn, id).unwrap())
            .is_some_and(|t| t.kind != balances::KIND_NOT_FUNGIBLE)
    };
    let mut out = std::collections::BTreeSet::new();
    let mut unknown = 0u64;
    let first = kdx_types::TxKey::new(height, 0, 0).to_be_bytes();
    for (k, v) in txn
        .scan(Table::TailTxs, &first, |k| {
            kdx_types::TxKey::from_be_bytes(k[..8].try_into().unwrap()).height() == height
        })
        .unwrap()
    {
        let r = kdx_types::record::decode_tx(&v).unwrap();
        let t = kdx_decode::receivers::record_token_receivers(&r, d.mem(), MAINNET01, &tracked)
            .unwrap();
        let _ = k;
        for ((a, m), _) in t.all_pairs() {
            let coin = balances::coin_module(&txn).unwrap();
            let mid = if m == "coin" {
                coin
            } else {
                d.find(DictKind::Module, m)
            };
            match (d.find(DictKind::Account, a), mid) {
                (Some(a), Some(m)) => {
                    out.insert((a, m));
                }
                _ => unknown += 1,
            }
        }
    }
    (out, unknown)
}

#[test]
fn the_tail_marks_the_same_d3_rows_as_the_pass_and_a_reorg_marks_them_again() {
    let (_d, c) = store();
    commit_block(&c, d3_block(25));
    let tail = tail_d3_rows(&c, 25);
    let (pass, unknown) = pass_d3_rows(&c, 25);
    assert!(!tail.is_empty());
    assert_eq!(tail, pass);
    // Rows with a name no posting interned (here the pre-fork exec strings) wait for the pass.
    assert!(unknown > 0);
    // The credits of the fixture, marked at the block's height with no verified-row skip.
    for (a, m) in [
        (WRECV, "free.wiza"),
        (UNWRAP_ACC, "kaddex.kdx"),
        (SKDX_ACC, "kaddex.skdx"),
    ] {
        assert!(dirty_in(&c, a, m), "{a} {m}");
    }
    // A reorg that undoes block 25 marks the rows again, like undone postings.
    clear_marks(&c);
    let mut txn = c.hot.rw().unwrap();
    kdx_hot::tail::unwind_to(&mut txn, 0, 24).unwrap();
    txn.commit().unwrap();
    for (a, m) in [
        (WRECV, "free.wiza"),
        (UNWRAP_ACC, "kaddex.kdx"),
        (SKDX_ACC, "kaddex.skdx"),
    ] {
        assert!(dirty_in(&c, a, m), "undo: {a} {m}");
    }
}

#[test]
fn the_tail_caps_one_emitter_per_block_and_still_applies_it() {
    let (_d, c) = store();
    // 40 txs x 64 distinct names of one token: 2,560 rows, over the per-block cap.
    let txs: Vec<Value> = (0..40u32)
        .map(|i| {
            let names: Vec<Value> = (0..64)
                .map(|j| json!(format!("k:{:064x}", i * 64 + j)))
                .collect();
            let cmd = json!({"payload": {"exec": {"code": "(free.spam.go)", "data": {}}},
                "meta": {"sender": SENDER}});
            let evs = json!([
                event("free.spam", "TRANSFER", json!([SENDER, "k:spam-pool", 1.0])),
                event("free.spam", "MINT", json!(names)),
            ]);
            d3_tx(3000 + i, cmd, evs, Value::Null)
        })
        .collect();
    commit_block(&c, block_of(25, txs));
    // Each tx charges free.spam its 64 MINT names (the exec's strings go to the sender's key).
    // Below the events fork the code naming free.spam also marks (sender, free.spam) by rule
    // 2b, once per block, charged to the sender's key: one row beyond the emitter's cap.
    let cap = kdx_hot::tail::D3_ROWS_PER_EMITTER_PER_BLOCK as usize;
    let rows = tail_d3_rows(&c, 25);
    assert!(
        rows.len() <= cap + 1 && rows.len() >= cap - 32,
        "{}",
        rows.len()
    );
    let dropped = c
        .hot
        .ro()
        .unwrap()
        .counter(kdx_hot::balances::counters::D3_TAIL_DROPPED)
        .unwrap();
    assert_eq!(dropped as usize, 40 * 64 - cap);
    assert!(c
        .hot
        .ro()
        .unwrap()
        .get(Table::Headers, &kdx_hot::tables::block_key(0, 25))
        .unwrap()
        .is_some());
}

#[test]
fn a_spoofed_modref_is_never_tracked_and_discovery_is_capped_per_namespace() {
    let (_d, c) = store();
    // 100 txs: kaddex.alchemist events naming 4 modules each (kaddex.f<i>-<j>), none real.
    let txs: Vec<Value> = (0..100u32)
        .map(|i| {
            let refs: Vec<Value> = (0..4)
                .map(|j| {
                    json!({"refSpec": [{"namespace": null, "name": "fungible-v2"}],
                    "refName": {"namespace": "kaddex", "name": format!("f{i}-{j}")}})
                })
                .collect();
            let cmd = json!({"payload": {"exec": {"code": "(kaddex.alchemist.go)", "data": {}}},
                "meta": {"sender": SENDER}});
            let evs = json!([event("kaddex.alchemist", "WRAP", json!([refs, SKDX_ACC]))]);
            d3_tx(4000 + i, cmd, evs, Value::Null)
        })
        .collect();
    commit_block(&c, block_of(25, txs));
    let after_tail = balances::discovered_queue(&c.hot.ro().unwrap(), 10_000)
        .unwrap()
        .len();
    // One namespace holds at most a few queue entries (security review).
    assert_eq!(after_tail, balances::D3_DISCOVER_PER_NAMESPACE);
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    assert!(
        rep.tokens_noted as usize <= seed_receivers::MAX_DISCOVERED_PER_NAMESPACE,
        "{rep:?}"
    );
    assert_eq!(
        rep.discovered_namespace_dropped as usize,
        400 - seed_receivers::MAX_DISCOVERED_PER_NAMESPACE,
        "{rep:?}"
    );
    let q = balances::discovered_queue(&c.hot.ro().unwrap(), 10_000).unwrap();
    assert_eq!(q.len(), balances::D3_DISCOVER_PER_NAMESPACE);
    // The node says none of them is a module: nothing is tracked and the queue empties.
    let no = vec![Some(false); q.len()];
    let (t, r) = seed_receivers::check_discovered(&c.hot, &c.dict, &q, &no).unwrap();
    assert_eq!((t, r as usize), (0, q.len()));
    assert!(!tracked(&c, "kaddex.f0-0"));
    assert!(balances::discovered_queue(&c.hot.ro().unwrap(), 10)
        .unwrap()
        .is_empty());
}

const LATE_ACC: &str = "k:8888888888888888888888888888888888888888888888888888888888888888";

#[test]
fn a_token_tracked_after_the_pass_gets_its_history_walked() {
    let (_d, c) = d3_store();
    // Height 26: free.late mints to LATE_ACC with no TRANSFER; free.late is not tracked yet.
    let cmd = json!({"payload": {"exec": {"code": "(free.late.mint)", "data": {}}},
        "meta": {"sender": SENDER}});
    let evs = json!([event("free.late", "MINT", json!([LATE_ACC, 5.0]))]);
    commit_block(&c, block_of(26, vec![d3_tx(5000, cmd, evs, Value::Null)]));
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    assert_eq!(rep.to, 27, "{rep:?}");
    assert!(!dirty_in(&c, LATE_ACC, "free.late"));
    // free.late becomes tracked later (its first TRANSFER, or a confirmed discovery).
    let q = vec![("free.late".to_owned(), 0u8, 26u32)];
    seed_receivers::check_discovered(&c.hot, &c.dict, &q, &[Some(true)]).unwrap();
    assert_eq!(
        balances::rewalk_pending(&c.hot.ro().unwrap())
            .unwrap()
            .len(),
        1
    );
    // The next pass (new heights) also walks free.late's history below its done height.
    commit_block(&c, block_of(27, vec![xchain_finish(9, "coin", SENDER)]));
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    assert_eq!((rep.from, rep.rewalk_tokens), (27, 1), "{rep:?}");
    assert!(dirty_in(&c, LATE_ACC, "free.late"), "{rep:?}");
    assert!(balances::rewalk_pending(&c.hot.ro().unwrap())
        .unwrap()
        .is_empty());
}

// ---------------------------------------------------------------------------------------------
// Fix round 1, security review: exec rows are charged to the sender; discovery per namespace.
// ---------------------------------------------------------------------------------------------

#[test]
fn junk_execs_calling_a_token_do_not_spend_its_event_budget() {
    let (_d, c) = d3_store();
    // 20 txs call free.wiza with 64 junk literals each and emit nothing of it; then a tx
    // where free.wiza mints to LATE_ACC (non-TRANSFER event, charged to free.wiza).
    let mut txs: Vec<Value> = (0..20u32)
        .map(|i| {
            let lits: Vec<String> = (0..64)
                .map(|j| format!("\"k:{:064x}\"", i * 64 + j))
                .collect();
            let code = format!("(free.wiza.details {})", lits.join(" "));
            let cmd = json!({"payload": {"exec": {"code": code, "data": {}}},
                "meta": {"sender": "k:attacker"}});
            d3_tx(6000 + i, cmd, json!([]), Value::Null)
        })
        .collect();
    let cmd = json!({"payload": {"exec": {"code": "(free.wiza.mint)", "data": {}}},
        "meta": {"sender": SENDER}});
    let evs = json!([
        event("free.wiza", "TRANSFER", json!([SENDER, "k:wiza-pool", 1.0])),
        event("free.wiza", "MINT", json!([LATE_ACC, 5.0])),
    ]);
    txs.push(d3_tx(6100, cmd, evs, Value::Null));
    commit_block(&c, block_of(26, txs));
    clear_marks(&c);
    let o = Opts {
        max_event_candidates: 10,
        ..d3_opts(false)
    };
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| false).unwrap();
    assert!(dirty_in(&c, LATE_ACC, "free.wiza"), "{rep:?}");
    assert!(!rep.dropped_tokens.contains_key("free.wiza"), "{rep:?}");
}

#[test]
fn a_thousand_junk_namespaces_cannot_stop_a_real_tokens_discovery() {
    let (_d, c) = store();
    let modref = |ns: &str, n: &str| {
        json!({"refSpec": [{"namespace": null, "name": "fungible-v2"}],
            "refName": {"namespace": ns, "name": n}})
    };
    let mut txs: Vec<Value> = (0..1000u32)
        .map(|i| {
            let ns = format!("n_{i:040x}");
            let cmd = json!({"payload": {"exec": {"code": "(x)", "data": {}}},
                "meta": {"sender": SENDER}});
            let evs = json!([event(
                &format!("{ns}.junk"),
                "WRAP",
                json!([modref(&ns, "tok"), modref(&ns, "tok2"), SKDX_ACC])
            )]);
            d3_tx(7000 + i, cmd, evs, Value::Null)
        })
        .collect();
    let cmd = json!({"payload": {"exec": {"code": "(kaddex.alchemist.wrap)", "data": {}}},
        "meta": {"sender": SENDER}});
    let evs = json!([event(
        "kaddex.alchemist",
        "WRAP",
        json!([modref("kaddex", "skdx"), 1.0, SKDX_ACC])
    )]);
    txs.push(d3_tx(8000, cmd, evs, Value::Null));
    commit_block(&c, block_of(25, txs));
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    let q = balances::discovered_queue(&c.hot.ro().unwrap(), 100_000).unwrap();
    assert!(q.iter().any(|(m, _, _)| m == "kaddex.skdx"), "{rep:?}");
}

/// A tx whose `emitter` event names `fungible-v2` modrefs to `targets` (and [`SKDX_ACC`]).
fn modref_tx(n: u32, emitter: &str, targets: &[&str]) -> Value {
    let refs: Vec<Value> = targets
        .iter()
        .map(|t| {
            let (ns, name) = t.split_once('.').unwrap();
            json!({"refSpec": [{"namespace": null, "name": "fungible-v2"}],
                "refName": {"namespace": ns, "name": name}})
        })
        .collect();
    let cmd = json!({"payload": {"exec": {"code": "(x)", "data": {}}},
        "meta": {"sender": SENDER}});
    let evs = json!([event(emitter, "WRAP", json!([refs, SKDX_ACC]))]);
    d3_tx(n, cmd, evs, Value::Null)
}

#[test]
fn junk_free_modules_cannot_starve_a_real_free_token() {
    let (_d, c) = store();
    // Ten junk free.* modules each name themselves, then free.real does.
    let mut txs: Vec<Value> = (0..10u32)
        .map(|i| {
            let m = format!("free.junk{i}");
            modref_tx(9000 + i, &m, &[&m])
        })
        .collect();
    txs.push(modref_tx(9100, "free.real", &["free.real"]));
    commit_block(&c, block_of(25, txs));
    seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    let q = balances::discovered_queue(&c.hot.ro().unwrap(), 10_000).unwrap();
    assert!(q.iter().any(|(m, _, _)| m == "free.real"), "{q:?}");
}

#[test]
fn a_discovery_dropped_at_a_quota_is_requeued_by_the_next_pass() {
    let (_d, c) = store();
    // Six kaddex modules in one block: four fit the namespace quota, two are kept pending.
    let names: Vec<String> = (0..6).map(|i| format!("kaddex.f{i}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    commit_block(
        &c,
        block_of(
            25,
            vec![
                modref_tx(9200, "kaddex.alchemist", &refs[..3]),
                modref_tx(9201, "kaddex.alchemist", &refs[3..]),
            ],
        ),
    );
    seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    let q = balances::discovered_queue(&c.hot.ro().unwrap(), 100).unwrap();
    assert_eq!(q.len(), balances::D3_DISCOVER_PER_NAMESPACE);
    let pending = balances::discover_pending(&c.hot.ro().unwrap(), 100).unwrap();
    assert_eq!(pending.len(), 2, "{pending:?}");
    // The node rejects the four; the next pass re-queues the two first.
    let no = vec![Some(false); q.len()];
    seed_receivers::check_discovered(&c.hot, &c.dict, &q, &no).unwrap();
    commit_block(&c, block_of(26, vec![xchain_finish(9, "coin", SENDER)]));
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    assert_eq!(rep.discovered_requeued, 2, "{rep:?}");
    let q = balances::discovered_queue(&c.hot.ro().unwrap(), 100).unwrap();
    let mut got: Vec<&str> = q.iter().map(|(m, _, _)| m.as_str()).collect();
    got.sort();
    let mut want: Vec<&str> = pending.iter().map(|(m, _)| m.as_str()).collect();
    want.sort();
    assert_eq!(got, want);
    assert!(balances::discover_pending(&c.hot.ro().unwrap(), 100)
        .unwrap()
        .is_empty());
}

// ---------------------------------------------------------------------------------------------
// Fix round 2 (ruling R-P5P9-195).
// ---------------------------------------------------------------------------------------------

/// Re-review N1: a discovered module name past the key limit (an `n_x.tok` event naming a
/// 3,000-char `n_x` module in a modref-shaped object) neither halts the tail nor the pass: it
/// is dropped and counted.
#[test]
fn a_long_discovered_module_name_halts_neither_the_tail_nor_the_pass() {
    let (_d, c) = d3_store();
    let modref = json!({"refSpec": [{"namespace": null, "name": "fungible-v2"}],
        "refName": {"namespace": "n_x", "name": "a".repeat(3000)}});
    let cmd = json!({"payload": {"exec": {"code": "(n_x.tok.mint)", "data": {}}},
        "meta": {"sender": SENDER}});
    let evs = json!([event("n_x.tok", "MINT", json!([modref, LATE_ACC, 1.0]))]);
    commit_block(&c, block_of(26, vec![d3_tx(6000, cmd, evs, Value::Null)]));
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false)
        .expect("the pass must not fail");
    assert_eq!(rep.to, 27, "{rep:?}");
    assert!(rep.discovered_too_long >= 1, "{rep:?}");
    let txn = c.hot.ro().unwrap();
    assert!(balances::discovered_queue(&txn, 100)
        .unwrap()
        .iter()
        .all(|(m, _, _)| m.len() <= kdx_decode::receivers::MAX_MODULE_LEN));
}

/// Review (a): `requeue_pending` resumes after the last key it scanned, so entries the queue
/// keeps refusing (one namespace over its quota, sorting first) cannot freeze the rest.
#[test]
fn refused_pending_entries_cannot_freeze_the_rest_of_the_list() {
    let (_d, c) = d3_store();
    let mut txn = c.hot.rw().unwrap();
    // 12 entries of one attacker namespace on chain 00 sort before the real one on chain 01.
    for i in 0..12 {
        balances::pend_discovered(&mut txn, &format!("n_a.junk{i:02}"), 0, 5).unwrap();
    }
    balances::pend_discovered(&mut txn, "n_real.tok", 1, 5).unwrap();
    txn.commit().unwrap();
    let mut moved_real = false;
    for _ in 0..5 {
        let mut txn = c.hot.rw().unwrap();
        balances::requeue_pending(&mut txn, 4).unwrap();
        txn.commit().unwrap();
        moved_real |= balances::discovered_queue(&c.hot.ro().unwrap(), 100)
            .unwrap()
            .iter()
            .any(|(m, _, _)| m == "n_real.tok");
    }
    assert!(moved_real, "the real entry never left the pending list");
}

/// Re-review N2: the history walk of a newly tracked token runs in bounded slices whose
/// cursor is persisted with their marks; a new walker resumes at that cursor, not at 0.
#[test]
fn a_queued_history_walk_progresses_in_slices_and_resumes_from_its_cursor() {
    let (_d, c) = d3_store();
    let cmd = json!({"payload": {"exec": {"code": "(free.late.mint)", "data": {}}},
        "meta": {"sender": SENDER}});
    let evs = json!([event("free.late", "MINT", json!([LATE_ACC, 5.0]))]);
    commit_block(&c, block_of(26, vec![d3_tx(5000, cmd, evs, Value::Null)]));
    let mut o = d3_opts(false);
    o.history_inline = false;
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| false).unwrap();
    assert_eq!(rep.to, 27, "{rep:?}");
    let q = vec![("free.late".to_owned(), 0u8, 26u32)];
    seed_receivers::check_discovered(&c.hot, &c.dict, &q, &[Some(true)]).unwrap();
    assert_eq!(
        balances::rewalk_pending(&c.hot.ro().unwrap())
            .unwrap()
            .len(),
        1
    );

    let mut w = seed_receivers::history_begin(&c.hot, &c.dict, &o)
        .unwrap()
        .expect("a walk is pending");
    let s = w.step(&c.hot, &c.dict, &c.segdir, &o, 1).unwrap();
    assert!(!s.done && s.txs <= 1, "{s:?}");
    let at = w.position();
    drop(w);
    // A new walker (e.g. after a restart) resumes where the last slice left off.
    let mut w = seed_receivers::history_begin(&c.hot, &c.dict, &o)
        .unwrap()
        .expect("still pending");
    assert_eq!(w.position(), at);
    let mut slices = 1;
    loop {
        let s = w.step(&c.hot, &c.dict, &c.segdir, &o, 1).unwrap();
        slices += 1;
        assert!(s.txs <= 1, "{s:?}");
        if s.done {
            break;
        }
        assert!(slices < 10_000, "the walk never ends");
    }
    assert!(slices > 2, "{slices}");
    assert!(dirty_in(&c, LATE_ACC, "free.late"));
    assert!(balances::rewalk_pending(&c.hot.ro().unwrap())
        .unwrap()
        .is_empty());
    assert!(seed_receivers::history_begin(&c.hot, &c.dict, &o)
        .unwrap()
        .is_none());
}

/// Re-review N2 / review M2: only a token first seen below the D3 done height needs a
/// history walk; one first seen at or above it is covered by the tail and the next pass.
#[test]
fn a_token_first_seen_above_the_d3_flag_queues_no_walk() {
    let (_d, c) = d3_store();
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    let done = rep.to;
    let q = vec![("free.above".to_owned(), 0u8, done)];
    seed_receivers::check_discovered(&c.hot, &c.dict, &q, &[Some(true)]).unwrap();
    assert!(balances::rewalk_pending(&c.hot.ro().unwrap())
        .unwrap()
        .is_empty());
    let q = vec![("free.below".to_owned(), 0u8, done - 1)];
    seed_receivers::check_discovered(&c.hot, &c.dict, &q, &[Some(true)]).unwrap();
    assert_eq!(
        balances::rewalk_pending(&c.hot.ro().unwrap())
            .unwrap()
            .len(),
        1
    );
}

// ---------------------------------------------------------------------------------------------
// R-G4-01 (b): a tracked token passed as a bare module reference (Hyperlane `mailbox.dispatch`).
// ---------------------------------------------------------------------------------------------

const KBNS: &str = "n_e5950000000000000000000000000000000000ab";

/// G4 row #1 (chain 2, 7,432,682, here chain 0): `(<ns>.mailbox.dispatch <ns>.kb-USDC 1
/// "<remote recipient>" 97.841352)` burns the sender's kb-USDC with no kb-USDC event. The tail
/// and the hourly pass mark (sender, kb-USDC); a crafted tx over the per-tx cap is counted.
#[test]
fn a_bridge_dispatch_marks_the_sender_in_the_tail_and_the_pass_and_counts_the_cap() {
    let (_d, c) = store();
    let usdc = format!("{KBNS}.kb-USDC");
    // Height 25: a kb-USDC TRANSFER tracks the token and names the sender.
    let cmd = json!({"payload": {"exec": {"code": format!("({usdc}.transfer \"{SENDER}\" \"k:pool\" 1.0)"),
        "data": {}}}, "meta": {"sender": SENDER}});
    let evs = json!([event(&usdc, "TRANSFER", json!([SENDER, "k:pool", 1.0]))]);
    commit_block(&c, block_of(25, vec![d3_tx(7000, cmd, evs, Value::Null)]));
    assert!(tracked(&c, &usdc));
    clear_marks(&c);
    // Height 26: the dispatch (events of coin and the mailbox only).
    let code = format!(
        "({KBNS}.mailbox.dispatch {usdc} 1 \"0x00000000000000000000000000c0ffee00000000000000000000000000aa\" 97.841352)"
    );
    let cmd = json!({"payload": {"exec": {"code": code, "data": {}}}, "meta": {"sender": SENDER}});
    let evs = json!([
        event("coin", "TRANSFER", json!([SENDER, "k:gas", 0.01])),
        event(
            &format!("{KBNS}.mailbox"),
            "DISPATCH",
            json!([1, "router", 1, "0xaa"])
        ),
    ]);
    commit_block(&c, block_of(26, vec![d3_tx(7001, cmd, evs, Value::Null)]));
    assert!(dirty_in(&c, SENDER, &usdc), "tail");
    // The hourly pass derives the same row from the stored record.
    clear_marks(&c);
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    assert!(dirty_in(&c, SENDER, &usdc), "pass {rep:?}");
    assert_eq!(rep.modref_capped, 0);
    // Height 27: a crafted tx naming the token by reference with 40 strings: capped, counted.
    let mut code = format!("({KBNS}.mailbox.dispatch {usdc}");
    for j in 0..40 {
        code.push_str(&format!(" \"k:{j:04}\""));
    }
    code.push(')');
    let cmd = json!({"payload": {"exec": {"code": code, "data": {}}}, "meta": {"sender": SENDER}});
    commit_block(
        &c,
        block_of(27, vec![d3_tx(7002, cmd, json!([]), Value::Null)]),
    );
    let capped = c
        .hot
        .ro()
        .unwrap()
        .counter(kdx_hot::balances::counters::D3_MODREF_CAPPED)
        .unwrap();
    let cap = kdx_decode::receivers::MAX_MODREF_PAIRS_PER_TX as u64;
    assert_eq!(capped, 41 - cap, "sender + 40 strings over a cap of {cap}");
    // The pass counts the same rows for the same tx (review M5).
    clear_marks(&c);
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    assert_eq!(rep.modref_capped, 41 - cap, "pass {rep:?}");
}
