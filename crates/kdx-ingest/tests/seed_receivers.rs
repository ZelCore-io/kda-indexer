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
use kdx_ingest::seed_receivers::{self, Opts};
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
fn the_token_pass_budgets_the_cap_per_emitter_and_stays_pending_on_a_hit() {
    let (_d, c) = d3_store();
    let o = Opts {
        max_event_candidates: 1,
        ..d3_opts(false)
    };
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| false).unwrap();
    // kaddex.kdx's UNWRAP names two strings: its budget of 1 is hit.
    assert!(rep.cap_hit.contains(&"kaddex.kdx".to_owned()), "{rep:?}");
    assert!(
        !rep.cap_hit.contains(&"kaddex.alchemist".to_owned()),
        "{rep:?}"
    );
    // Emitter B (kaddex.alchemist) is not starved by A, and uncapped rules still run.
    assert!(dirty_in(&c, SKDX_ACC, "kaddex.skdx"), "{rep:?}");
    assert!(dirty_in(&c, WRECV, "free.wiza") && dirty_in(&c, INJ, "coin"));
    // Not done: no flag, still pending, the reason in Meta for `--status`.
    let txn = c.hot.ro().unwrap();
    assert!(txn
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)
        .unwrap()
        .is_none());
    assert!(seed_receivers::pending_for(&txn, &o).unwrap().is_some());
    let why = txn
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS_CAP_HIT)
        .unwrap()
        .expect("cap hit recorded");
    assert!(String::from_utf8_lossy(&why).contains("kaddex.kdx"));
    drop(txn);
    // A rerun with a raised cap completes and clears the reason.
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &d3_opts(false), &|| false).unwrap();
    assert!(rep.cap_hit.is_empty(), "{rep:?}");
    let txn = c.hot.ro().unwrap();
    assert!(txn
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)
        .unwrap()
        .is_some());
    assert!(txn
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS_CAP_HIT)
        .unwrap()
        .is_none());
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
    assert!(rep.cap_hit.is_empty(), "{rep:?}");
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
fn the_global_ceiling_drops_and_completes() {
    let (_d, c) = d3_store();
    let o = Opts {
        max_total_candidates: 0,
        ..d3_opts(false)
    };
    let rep = seed_receivers::run(&c.hot, &c.dict, &c.segdir, &o, &|| false).unwrap();
    assert!(
        rep.dropped_ceiling >= 1 && rep.cap_hit.is_empty(),
        "{rep:?}"
    );
    let txn = c.hot.ro().unwrap();
    assert!(txn
        .meta(meta::BALANCES_SEED_TOKEN_RECEIVERS)
        .unwrap()
        .is_some());
}
