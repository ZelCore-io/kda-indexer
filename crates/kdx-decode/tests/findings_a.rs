//! Finding fixtures A (plan Task 13, T-3) at the decode level: R-4 (the transfer rule of
//! chainweb-data's `mkTransferRows` and its six amount forms, `WIZ_TRANSFER` included) and
//! R-P5P9-3 (`acct` postings only from an event named exactly `TRANSFER`). Input: an event as
//! the node serves it; expected: the record's `transfers` list and the postings.

use kdx_decode::{decode_block, index_block, verify_block, DecodedBlock, Family, VerifyReport};
use kdx_types::hash::b64url_encode;
use kdx_types::{blake2b_256, Dict, DictKind, MemDict};
use serde_json::{json, Value};

fn h(n: u8) -> String {
    b64url_encode(&[n; 32])
}

fn event(module: &str, name: &str, params: Value) -> Value {
    json!({"params": params, "name": name,
           "module": {"namespace": null, "name": module}, "moduleHash": h(11)})
}

/// A block with one user tx (sender `k:<s>`) whose output carries `events`.
fn block(events: Value) -> DecodedBlock {
    let sender = format!("k:{}", "5".repeat(64));
    let cmd = format!(
        r#"{{"payload":{{"exec":{{"data":null,"code":"1"}}}},"meta":{{"sender":"{sender}"}},"nonce":"n"}}"#
    );
    let rk = blake2b_256(cmd.as_bytes()).to_b64url();
    let tx = serde_json::to_vec(&json!({"hash": rk, "sigs": [{"sig": "ab"}], "cmd": cmd})).unwrap();
    let out = serde_json::to_vec(
        &json!({"gas": 1, "result": {"status": "success", "data": 1},
        "reqKey": rk, "logs": null, "events": events, "metaData": null,
        "continuation": null, "txId": null}),
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
        "target": h(3), "payloadHash": h(4), "chainId": 0, "weight": h(5), "height": 5,
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

/// Postings of the user tx, `(family, account name)` for the account families.
fn postings(b: &DecodedBlock, fam: Family, dict: &mut MemDict) -> Vec<u64> {
    let ib = index_block(b, &VerifyReport::default(), dict).expect("indexes");
    ib.idx[1]
        .postings
        .iter()
        .filter(|(f, _)| *f == fam)
        .map(|(_, e)| *e)
        .collect()
}

fn transfers_of(b: &DecodedBlock) -> Vec<u16> {
    let (_, trs) =
        kdx_decode::build_records(b, &VerifyReport::default(), &mut MemDict::new()).unwrap();
    trs[1].transfers.clone()
}

/// R-4: every amount form `getAmount` reads makes a transfer row; anything else does not.
#[test]
fn r4_six_amount_forms_make_a_transfer_row() {
    let (a, b) = ("alice", "bob");
    let forms = [
        json!(1.5),
        json!({"decimal": 1.5}),
        json!({"decimal": "1.5"}),
        json!({"int": 7}),
        json!({"int": "7"}),
        json!("1.5"),
    ];
    for amount in forms {
        let blk = block(json!([event("coin", "TRANSFER", json!([a, b, amount]))]));
        assert_eq!(transfers_of(&blk), vec![0], "amount {amount}");
    }
    // Not amounts: no row.
    for amount in [
        json!("abc"),
        json!(null),
        json!({"decimal": "x"}),
        json!([1]),
        json!(true),
    ] {
        let blk = block(json!([event("coin", "TRANSFER", json!([a, b, amount]))]));
        assert!(transfers_of(&blk).is_empty(), "amount {amount}");
    }
    // Two params or four params: no row (four is the poly-fungible form).
    for params in [json!([a, b]), json!([a, b, 1.0, 2.0])] {
        let blk = block(json!([event("coin", "TRANSFER", params.clone())]));
        assert!(transfers_of(&blk).is_empty(), "params {params}");
    }
}

/// R-4: the suffix rule. `wiz-arena.WIZ_TRANSFER` (from, to, amount) has three params, so it
/// is a transfer row and an `xfer` posting (keyed by its module); a name that does not end in
/// `TRANSFER`, or an `(id, from, to)` shape whose third param is no amount, is not.
#[test]
fn r4_wiz_transfer_is_a_transfer_row_and_an_xfer_posting() {
    let blk = block(json!([event(
        "wiz-arena",
        "WIZ_TRANSFER",
        json!(["alice", "bob", 12.0])
    )]));
    assert_eq!(transfers_of(&blk), vec![0]);
    assert!(!postings(&blk, Family::Xfer, &mut MemDict::new()).is_empty());

    for ev in [
        event("coin", "TRANSFERRED", json!(["alice", "bob", 1.0])),
        event("wiz-arena", "WIZ_TRANSFER", json!(["12", "alice", "bob"])),
    ] {
        let blk = block(json!([ev]));
        assert!(transfers_of(&blk).is_empty());
        assert!(postings(&blk, Family::Xfer, &mut MemDict::new()).is_empty());
    }
}

/// R-P5P9-3: `acct` postings keep the v1 rule. The WIZ_TRANSFER "sides" are an NFT id and a
/// receiver; neither becomes an account row. Only an event named exactly `TRANSFER` posts its
/// sides.
#[test]
fn r_p5p9_3_acct_postings_come_from_transfer_only() {
    let blk = block(json!([event(
        "wiz-arena",
        "WIZ_TRANSFER",
        json!(["12", "alice-x", "bob-x"])
    )]));
    let mut dict = MemDict::new();
    let names = acct_names(&blk, &mut dict);
    for n in ["12", "alice-x", "bob-x"] {
        assert!(
            !names.contains(&n.to_owned()),
            "{n} must not be an account: {names:?}"
        );
    }
    // The same params under the exact name TRANSFER do post both sides.
    let blk = block(json!([event(
        "coin",
        "TRANSFER",
        json!(["alice-x", "bob-x", 1.0])
    )]));
    let names = acct_names(&blk, &mut MemDict::new());
    assert!(names.contains(&"alice-x".to_owned()) && names.contains(&"bob-x".to_owned()));
}

fn acct_names(b: &DecodedBlock, dict: &mut MemDict) -> Vec<String> {
    let mut v: Vec<String> = postings(b, Family::Acct, dict)
        .into_iter()
        .map(|e| dict.name(DictKind::Account, e as u32).unwrap().to_owned())
        .collect();
    v.sort();
    v
}
