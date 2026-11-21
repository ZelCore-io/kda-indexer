//! D1: balance receivers that no event names (before coin v3). The seed-only extraction
//! `receivers::balance_receivers` / `record_receivers`; postings, transfers and tx lists do
//! not change (`index_block` is untouched, checked at the end).
//!
//! Shapes follow the three mainnet repro txs (account names replaced):
//! - FA:1143, chain 3, height 716,372: `exec` `(coin.transfer-create "s" "r" (read-keyset ..) x)`,
//!   `events: []`;
//! - FA:4476, chain 9, height 870,129: `exec` `(coin.transfer ..)`, `events: []`;
//! - a `cont` step 1 of `coin.transfer-crosschain` at 1,462,957 on chain 6, gas payer
//!   `free-x-chain-gas`, receiver only in `continuation.continuation.args`, `events: []`.

use kdx_decode::receivers::{account_like, balance_receivers, record_receivers, COIN_V3_HEIGHT};
use kdx_decode::{decode_block, index_block, verify_block, DecodedBlock, Family, VerifyReport};
use kdx_types::hash::b64url_encode;
use kdx_types::{blake2b_256, Dict, DictKind, MemDict};
use serde_json::{json, Value};

fn h(n: u8) -> String {
    b64url_encode(&[n; 32])
}

const SENDER: &str = "k:1111111111111111111111111111111111111111111111111111111111111111";
const LEGACY: &str = "sasha-legacy";
const PK: &str = "2222222222222222222222222222222222222222222222222222222222222222";

fn exec(code: &str, data: Value) -> Value {
    json!({"payload": {"exec": {"code": code, "data": data}},
           "meta": {"sender": SENDER, "chainId": "3", "gasLimit": 600, "gasPrice": 1e-5,
                    "ttl": 28800, "creationTime": 1},
           "signers": [{"pubKey": "1".repeat(64)}], "nonce": "n", "networkId": "mainnet01"})
}

fn xchain_cmd() -> Value {
    json!({"payload": {"cont": {"pactId": h(30), "step": 1, "rollback": false,
           "data": {}, "proof": "cHJvb2Y"}},
           "meta": {"sender": "free-x-chain-gas", "chainId": "6", "gasLimit": 750,
                    "gasPrice": 1e-8, "ttl": 28800, "creationTime": 1},
           "signers": [], "nonce": "n", "networkId": "mainnet01"})
}

fn xchain_cont(step: u64, def: &str, receiver: &str) -> Value {
    json!({"pactId": h(30), "step": step, "stepCount": 2, "executed": null,
           "stepHasRollback": false, "yield": null,
           "continuation": {"def": def, "args": [SENDER, receiver,
               {"keys": [PK], "pred": "keys-all"}, "6", 1.0]}})
}

fn sorted(v: Vec<String>) -> Vec<String> {
    let mut v = v;
    v.sort_unstable();
    v
}

fn strs(v: &[&str]) -> Vec<String> {
    let mut v: Vec<String> = v.iter().map(|s| (*s).to_owned()).collect();
    v.sort_unstable();
    v
}

#[test]
fn pre_event_transfer_create_names_the_receiver_in_the_code() {
    // FA:1143 shape: receiver only as a code literal; the keyset name "ks" is too short.
    let cmd = exec(
        &format!(r#"(coin.transfer-create "{SENDER}" "{LEGACY}" (read-keyset "ks") 2.0)"#),
        json!({"ks": {"keys": [PK], "pred": "keys-all"}}),
    );
    let got = sorted(balance_receivers(&cmd, None, 716_372));
    // Data strings: the pubkey (a legacy account can be named by its key) and the predicate.
    assert_eq!(got, strs(&[SENDER, LEGACY, PK, "keys-all"]));
}

#[test]
fn pre_event_transfer_names_the_receiver_in_the_data() {
    // FA:4476 shape, with the accounts passed through `read-msg`.
    let to = format!("w:{}:keys-all", "x".repeat(43));
    let cmd = exec(
        r#"(coin.transfer (read-msg "from") (read-msg 'to) 2.19651)"#,
        json!({"from": SENDER, "to": to, "n": 3}),
    );
    let got = balance_receivers(&cmd, None, 870_129);
    assert!(got.contains(&to), "{got:?}");
    assert!(got.contains(&SENDER.to_owned()), "{got:?}");
    // Code literals inside the coin call (`"from"`, `'to` is 2 chars) are candidates too.
    assert!(got.contains(&"from".to_owned()), "{got:?}");
    assert!(!got.contains(&"to".to_owned()), "{got:?}");
}

#[test]
fn only_literals_inside_coin_calls_count() {
    let cmd = exec(
        r#"; (coin.transfer "in-comment" "x" 1.0)
           (free.other.pay "outside-acct" 1.0)
           (coin.transfer "esc\"aped" "r:ns.guard" 1.0)
           (let ((a "after-call")) a)"#,
        Value::Null,
    );
    let got = sorted(balance_receivers(&cmd, None, 10));
    assert_eq!(got, strs(&["esc\"aped", "r:ns.guard"]));
}

#[test]
fn coin_v3_and_later_execs_give_nothing() {
    let cmd = exec(
        r#"(coin.transfer "alice" "bobby" 1.0)"#,
        json!({"x": "carol"}),
    );
    assert_eq!(balance_receivers(&cmd, None, COIN_V3_HEIGHT - 1).len(), 3);
    assert!(balance_receivers(&cmd, None, COIN_V3_HEIGHT).is_empty());
}

#[test]
fn crosschain_finish_gives_the_receiver_from_the_continuation_args() {
    let cont = xchain_cont(1, "coin.transfer-crosschain", LEGACY);
    assert_eq!(
        balance_receivers(&xchain_cmd(), Some(&cont), 1_462_957),
        strs(&[LEGACY])
    );
    // At any height: a finish after coin v3 names the receiver in events too (harmless).
    assert_eq!(
        balance_receivers(&xchain_cmd(), Some(&cont), COIN_V3_HEIGHT + 5),
        strs(&[LEGACY])
    );
    // Step 0 (the debit on the source chain) and other defpacts give nothing.
    let burn = xchain_cont(0, "coin.transfer-crosschain", LEGACY);
    assert!(balance_receivers(&xchain_cmd(), Some(&burn), 1_462_957).is_empty());
    let other = xchain_cont(1, "free.bridge.move", LEGACY);
    assert!(balance_receivers(&xchain_cmd(), Some(&other), 1_462_957).is_empty());
    assert!(balance_receivers(&xchain_cmd(), None, 1_462_957).is_empty());
}

#[test]
fn account_like_follows_the_coin_length_rule() {
    assert!(!account_like("ab"));
    assert!(account_like("abc"));
    assert!(account_like(&"é".repeat(128))); // 128 chars, 256 bytes
    assert!(account_like(&"a".repeat(256)));
    assert!(!account_like(&"a".repeat(257)));
    assert!(!account_like(&"é".repeat(129))); // over the indexed byte limit
}

/// A block at `height` on chain 6 with one user tx: `cmd`, `events: []`, and `continuation`.
fn block(cmd: &Value, cont: Value, height: u64) -> DecodedBlock {
    let cmd = cmd.to_string();
    let rk = blake2b_256(cmd.as_bytes()).to_b64url();
    let tx = serde_json::to_vec(&json!({"hash": rk, "sigs": [], "cmd": cmd})).unwrap();
    let out = serde_json::to_vec(
        &json!({"gas": 1, "result": {"status": "success", "data": "Write succeeded"},
        "reqKey": rk, "logs": h(12), "events": [], "metaData": null,
        "continuation": cont, "txId": 7}),
    )
    .unwrap();
    let cb = serde_json::to_vec(
        &json!({"gas": 0, "result": {"status": "success", "data": "ok"},
        "reqKey": h(9), "logs": null, "events": [], "metaData": null,
        "continuation": null, "txId": null}),
    )
    .unwrap();
    let miner = br#"{"account":"k:m","predicate":"keys-all","public-keys":["m"]}"#;
    let header = json!({
        "nonce": "1", "creationTime": 1, "parent": h(1), "adjacents": {"1": h(2)},
        "target": h(3), "payloadHash": h(4), "chainId": 6, "weight": h(5), "height": height,
        "chainwebVersion": "mainnet01", "epochStart": 1, "featureFlags": 0, "hash": h(6)
    });
    let payload = json!({
        "transactions": [[b64url_encode(&tx), b64url_encode(&out)]],
        "minerData": b64url_encode(miner),
        "transactionsHash": h(7), "outputsHash": h(8), "payloadHash": h(4),
        "coinbase": b64url_encode(&cb),
    });
    let b = decode_block(&header, &payload).expect("block decodes");
    verify_block(&b, &b.header.payload_hash).expect("consistency checks");
    b
}

/// Acct posting names of the user tx.
fn acct_names(ib: &kdx_decode::IndexedBlock, dict: &MemDict) -> Vec<String> {
    let mut v: Vec<String> = ib.idx[1]
        .postings
        .iter()
        .filter(|(f, _)| *f == Family::Acct)
        .map(|(_, e)| dict.name(DictKind::Account, *e as u32).unwrap().to_owned())
        .collect();
    v.sort();
    v
}

#[test]
fn stored_records_give_the_same_receivers_and_postings_do_not_change() {
    // Crosschain finish, read back from the stored record (continuation in the value codec).
    let mut dict = MemDict::new();
    let b = block(
        &xchain_cmd(),
        xchain_cont(1, "coin.transfer-crosschain", LEGACY),
        1_462_957,
    );
    let ib = index_block(&b, &VerifyReport::default(), &mut dict).expect("indexes");
    assert_eq!(
        record_receivers(&ib.txs[1], &dict).unwrap(),
        strs(&[LEGACY])
    );
    assert!(record_receivers(&ib.txs[0], &dict).unwrap().is_empty()); // coinbase
                                                                      // The receiver still gets no posting: tx lists and transfers are unchanged.
    assert_eq!(acct_names(&ib, &dict), vec!["free-x-chain-gas".to_owned()]);
    assert!(ib.idx[1].postings.iter().all(|(f, _)| *f != Family::Xfer));

    // Pre-event exec, from the stored record.
    let mut dict = MemDict::new();
    let cmd = exec(
        &format!(r#"(coin.transfer-create "{SENDER}" "{LEGACY}" (read-keyset "ks") 2.0)"#),
        json!({"ks": {"keys": [PK], "pred": "keys-all"}}),
    );
    let b = block(&cmd, Value::Null, 716_372);
    let ib = index_block(&b, &VerifyReport::default(), &mut dict).expect("indexes");
    let got = sorted(record_receivers(&ib.txs[1], &dict).unwrap());
    assert_eq!(got, strs(&[SENDER, PK, "keys-all", LEGACY]));
    assert_eq!(acct_names(&ib, &dict), vec![SENDER.to_owned()]);
}
