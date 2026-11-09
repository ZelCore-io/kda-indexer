//! Hostile user content must never fail a whole block (review C1/H1), and every record must
//! render the node's Merkle leaf bytes exactly (H2).

use kdx_decode::{
    build_records, decode_block, render_miner_bytes, render_output_bytes, render_tx_bytes,
    verify_block, DecodedBlock,
};
use kdx_types::hash::b64url_encode;
use kdx_types::record::{decode_block as dec_block_rec, decode_tx, encode_block, encode_tx};
use kdx_types::{blake2b_256, MemDict};
use serde_json::{json, Value};

fn h(n: u8) -> String {
    b64url_encode(&[n; 32])
}

const OK_OUT_TAIL: &str =
    r#""logs":null,"events":[],"metaData":null,"continuation":null,"txId":null}"#;

fn output(rk: &str, data: &str) -> String {
    format!(
        r#"{{"gas":1,"result":{{"status":"success","data":{data}}},"reqKey":"{rk}",{OK_OUT_TAIL}"#
    )
}

/// A block with one user tx whose `cmd` env data and output data are the given raw JSON texts.
fn block(cmd_data: &str, out_data: &str, miner: &str) -> (Value, Value, Vec<u8>, Vec<u8>) {
    let cmd = format!(
        r#"{{"payload":{{"exec":{{"data":{cmd_data},"code":"1"}}}},"meta":{{"sender":"k:a"}},"nonce":"n"}}"#
    );
    let rk = blake2b_256(cmd.as_bytes()).to_b64url();
    let tx = serde_json::to_vec(&json!({"hash": rk, "sigs": [{"sig": "ab"}], "cmd": cmd})).unwrap();
    let out = output(&rk, out_data).into_bytes();
    let cb = output(&h(9), "\"ok\"").into_bytes();
    let header = json!({
        "nonce": "1", "creationTime": 1, "parent": h(1), "adjacents": {"1": h(2)},
        "target": h(3), "payloadHash": h(4), "chainId": 0, "weight": h(5), "height": 5,
        "chainwebVersion": "mainnet01", "epochStart": 1, "featureFlags": 0, "hash": h(6)
    });
    let payload = json!({
        "transactions": [[b64url_encode(&tx), b64url_encode(&out)]],
        "minerData": b64url_encode(miner.as_bytes()),
        "transactionsHash": h(7), "outputsHash": h(8), "payloadHash": h(4),
        "coinbase": b64url_encode(&cb),
    });
    (header, payload, tx, out)
}

/// Decode, verify, build, encode/decode the records and check the exact leaf bytes.
fn roundtrip(header: &Value, payload: &Value, tx: &[u8], out: &[u8]) -> DecodedBlock {
    let b = decode_block(header, payload).expect("block must decode");
    verify_block(&b, &b.header.payload_hash).expect("consistency checks");
    let mut dict = MemDict::new();
    let (br, trs) = build_records(&b, &mut dict).expect("records");
    let mut buf = Vec::new();
    encode_block(&br, &mut buf);
    let br2 = dec_block_rec(&buf).unwrap();
    assert_eq!(br2, br);
    assert!(!br2.merkle_verified);
    assert_eq!(render_miner_bytes(&br2, &dict).unwrap(), b.miner_bytes);
    let mut recs = Vec::new();
    for r in &trs {
        let mut buf = Vec::new();
        encode_tx(r, &mut buf);
        let r2 = decode_tx(&buf).unwrap();
        assert_eq!(&r2, r);
        recs.push(r2);
    }
    assert_eq!(
        render_output_bytes(&recs[0], &dict).unwrap(),
        b.coinbase_bytes
    );
    assert_eq!(render_tx_bytes(&recs[1], &dict).unwrap().unwrap(), tx);
    assert_eq!(render_output_bytes(&recs[1], &dict).unwrap(), out);
    assert_eq!(recs[1].cmd_unparsed, b.txs[0].flags.cmd_unparsed);
    b
}

const MINER: &str = r#"{"account":"k:m","predicate":"keys-all","public-keys":["m"]}"#;

#[test]
fn ordinary_block_needs_no_raw_slot() {
    let (hd, p, tx, out) = block(r#"{"x":1.0}"#, r#"{"y":[1,"a"]}"#, MINER);
    let b = roundtrip(&hd, &p, &tx, &out);
    assert_eq!(b.txs[0].flags, Default::default());
    let mut dict = MemDict::new();
    let (br, trs) = build_records(&b, &mut dict).unwrap();
    assert!(br.raw_miner.is_none());
    assert!(trs
        .iter()
        .all(|r| r.raw_tx.is_none() && r.raw_output.is_none()));
}

#[test]
fn magic_number_key_in_cmd_does_not_fail_the_block() {
    for data in [
        r#"{"$serde_json::private::Number":"x"}"#,
        r#"{"$serde_json::private::Number":"1.5"}"#,
    ] {
        let (hd, p, tx, out) = block(data, "1", MINER);
        let b = roundtrip(&hd, &p, &tx, &out);
        assert!(b.txs[0].flags.cmd_unparsed);
        assert!(b.txs[0].cmd_parsed.is_none());
    }
}

#[test]
fn magic_number_key_in_output_is_kept_as_raw_bytes() {
    for data in [
        r#"{"$serde_json::private::Number":"x"}"#,
        r#"{"$serde_json::private::Number":"1.5"}"#,
    ] {
        let (hd, p, tx, out) = block("1", data, MINER);
        let b = roundtrip(&hd, &p, &tx, &out);
        assert!(b.txs[0].flags.output_unrepresentable);
        // reqKey is still recovered and checked.
        assert!(b.txs[0].out_req_key.is_some());
    }
}

#[test]
fn deep_nesting_and_lone_surrogates_do_not_fail_the_block() {
    let deep = format!("{}1{}", "[".repeat(300), "]".repeat(300));
    let (hd, p, tx, out) = block(&deep, &deep, MINER);
    let b = roundtrip(&hd, &p, &tx, &out);
    assert!(b.txs[0].flags.cmd_unparsed);
    assert!(b.txs[0].flags.output_unrepresentable);

    let (hd, p, tx, out) = block(r#""\ud800""#, r#""\udc00x""#, MINER);
    let b = roundtrip(&hd, &p, &tx, &out);
    assert!(b.txs[0].flags.cmd_unparsed);
    assert!(b.txs[0].flags.output_unrepresentable);
}

#[test]
fn escape_forms_and_odd_miner_are_byte_exact() {
    // aeson writes \u0008 where serde_json writes \b; the raw slot keeps the node's form.
    let (hd, p, tx, out) = block("1", r#""a\u0008b\/c""#, r#"{"account":"\u006b:m"}"#);
    let b = roundtrip(&hd, &p, &tx, &out);
    let mut dict = MemDict::new();
    let (br, trs) = build_records(&b, &mut dict).unwrap();
    assert!(
        br.raw_miner.is_some(),
        "{:?}",
        String::from_utf8_lossy(&b.miner_bytes)
    );
    assert!(trs[1].raw_output.is_some());
    // Unrepresentable minerData is kept too.
    let (hd, p, tx, out) = block("1", "1", r#"{"$serde_json::private::Number":"2"}"#);
    let b = roundtrip(&hd, &p, &tx, &out);
    assert!(b.miner_unrepresentable);
}
