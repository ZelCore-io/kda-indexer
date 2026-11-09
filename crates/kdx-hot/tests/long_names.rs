//! A chain-data name past the MDBX key limit must not stop ingest (review H2): a 5 KB
//! TRANSFER side and a 5 KB module name go through `index_block` and `tail::apply`.

use kdx_decode::{decode_block, index_block, VerifyReport};
use kdx_hot::dict::HotDict;
use kdx_hot::env::{Geometry, HotStore, Txn};
use kdx_types::hash::b64url_encode;
use kdx_types::{blake2b_256, Dict, DictKind};
use serde_json::json;

fn h(n: u8) -> String {
    b64url_encode(&[n; 32])
}

#[test]
fn over_long_names_do_not_halt_the_tail() {
    let long_acct = "a".repeat(5000);
    let long_mod = "m".repeat(5000);
    let ev = json!({"params": [long_acct, "k:ok", 1.0], "name": "TRANSFER",
        "module": {"namespace": null, "name": long_mod}, "moduleHash": h(11)});
    let cmd =
        r#"{"payload":{"exec":{"data":null,"code":"1"}},"meta":{"sender":"k:s"},"nonce":"n"}"#;
    let rk = blake2b_256(cmd.as_bytes()).to_b64url();
    let tx = serde_json::to_vec(&json!({"hash": rk, "sigs": [{"sig": "ab"}], "cmd": cmd})).unwrap();
    let out = serde_json::to_vec(
        &json!({"gas": 1, "result": {"status": "success", "data": 1},
        "reqKey": rk, "logs": null, "events": [ev], "metaData": null,
        "continuation": null, "txId": null}),
    )
    .unwrap();
    let cb = serde_json::to_vec(
        &json!({"gas": 0, "result": {"status": "success", "data": "ok"},
        "reqKey": h(9), "logs": null, "events": [], "metaData": null,
        "continuation": null, "txId": null}),
    )
    .unwrap();
    let header = json!({
        "nonce": "1", "creationTime": 1, "parent": h(1), "adjacents": {"1": h(2)},
        "target": h(3), "payloadHash": h(4), "chainId": 0, "weight": h(5), "height": 5,
        "chainwebVersion": "mainnet01", "epochStart": 1, "featureFlags": 0, "hash": h(6)
    });
    let payload = json!({
        "transactions": [[b64url_encode(&tx), b64url_encode(&out)]],
        "minerData": b64url_encode(br#"{"account":"k:m"}"#),
        "transactionsHash": h(7), "outputsHash": h(8), "payloadHash": h(4),
        "coinbase": b64url_encode(&cb),
    });
    let b = decode_block(&header, &payload).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let store = HotStore::open(
        dir.path(),
        Geometry {
            max_gb: 1,
            growth_mb: 16,
        },
    )
    .unwrap();
    let mut dict = HotDict::load(&store.ro().unwrap()).unwrap();
    let ib = index_block(&b, &VerifyReport::default(), &mut dict).unwrap();
    assert_eq!(ib.unindexed_accounts, 1);
    let mut txn = store.rw().unwrap();
    kdx_hot::tail::apply(&mut txn, &ib, &mut dict).expect("apply must not fail");
    txn.commit().expect("commit must not fail");

    let ro = store.ro().unwrap();
    let back = HotDict::load(&ro).unwrap();
    assert!(back.find(DictKind::Module, &long_mod).is_some());
    assert!(back.find(DictKind::Account, &long_acct).is_none());
    assert_eq!(
        ro.counter(kdx_hot::tail::counters::UNINDEXED_ACCOUNTS)
            .unwrap(),
        1
    );
}
