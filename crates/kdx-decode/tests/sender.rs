//! The `sndr` posting family (FIX-2): one posting per user tx whose cmd parses and whose
//! `meta.sender` is an indexable account, exactly where that sender gets its `acct` posting.

use kdx_decode::index::{indexed_sender, sender_of};
use kdx_decode::{decode_block, index_block, verify_block, DecodedBlock, Family, VerifyReport};
use kdx_types::hash::b64url_encode;
use kdx_types::{blake2b_256, Dict, DictKind, MemDict};
use serde_json::{json, Value};

fn h(n: u8) -> String {
    b64url_encode(&[n; 32])
}

/// A block with one user tx whose signed `cmd` string is `cmd` and whose output names
/// `receiver` in a coin TRANSFER from `k:payer`.
fn block(cmd: &str, receiver: &str) -> DecodedBlock {
    let rk = blake2b_256(cmd.as_bytes()).to_b64url();
    let tx = serde_json::to_vec(&json!({"hash": rk, "sigs": [{"sig": "ab"}], "cmd": cmd})).unwrap();
    let events = json!([{"params": ["k:payer", receiver, 1.5], "name": "TRANSFER",
        "module": {"namespace": null, "name": "coin"}, "moduleHash": h(11)}]);
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

fn cmd_with_sender(sender: Value) -> String {
    json!({"payload": {"exec": {"data": null, "code": "1"}},
           "meta": {"sender": sender}, "nonce": "n"})
    .to_string()
}

/// (family, account name) of every acct/sndr posting of the user tx, plus the record's
/// `indexed_sender`.
fn postings(b: &DecodedBlock) -> (Vec<(Family, String)>, Option<String>) {
    let mut dict = MemDict::new();
    let ib = index_block(b, &VerifyReport::default(), &mut dict).expect("indexes");
    let v = ib.idx[1]
        .postings
        .iter()
        .filter(|(f, _)| matches!(f, Family::Acct | Family::Sndr))
        .map(|(f, e)| {
            let n = dict.name(DictKind::Account, *e as u32).unwrap().to_owned();
            (*f, n)
        })
        .collect();
    // The coinbase record never has a sender.
    assert_eq!(indexed_sender(&ib.txs[0]).unwrap(), None);
    (v, indexed_sender(&ib.txs[1]).unwrap())
}

#[test]
fn family_six_is_sndr() {
    assert_eq!(Family::Sndr as u8, 6);
    assert_eq!(Family::ALL.len(), 7);
    assert_eq!(Family::from_u8(6), Some(Family::Sndr));
    assert_eq!(Family::from_u8(7), None);
    assert_eq!(Family::Sndr.name(), "sndr");
    for (i, f) in Family::ALL.into_iter().enumerate() {
        assert_eq!(f as usize, i);
    }
}

#[test]
fn sender_of_reads_meta_sender_strings_only() {
    assert_eq!(
        sender_of(&json!({"meta": {"sender": "alice"}})),
        Some("alice")
    );
    assert_eq!(sender_of(&json!({"meta": {"sender": 5}})), None);
    assert_eq!(sender_of(&json!({"meta": {}})), None);
    assert_eq!(sender_of(&json!({"sender": "alice"})), None);
    assert_eq!(sender_of(&json!("alice")), None);
}

#[test]
fn the_sender_gets_a_sndr_posting_next_to_its_acct_posting() {
    let sender = format!("k:{}", "5".repeat(64));
    let b = block(&cmd_with_sender(Value::from(sender.clone())), "k:recv");
    let (p, s) = postings(&b);
    assert!(p.contains(&(Family::Acct, sender.clone())), "{p:?}");
    assert!(p.contains(&(Family::Sndr, sender.clone())), "{p:?}");
    // Receivers and payers of events get acct postings only.
    assert!(p.contains(&(Family::Acct, "k:recv".into())), "{p:?}");
    let sndr: Vec<_> = p.iter().filter(|(f, _)| *f == Family::Sndr).collect();
    assert_eq!(sndr.len(), 1, "{p:?}");
    assert_eq!(s.as_deref(), Some(sender.as_str()));
}

#[test]
fn no_sndr_posting_without_an_indexable_sender() {
    // Sender not a string, over-long, or empty: no sndr posting (and no acct for it).
    for sender in [json!(7), Value::from("x".repeat(257)), Value::from("")] {
        let b = block(&cmd_with_sender(sender.clone()), "k:recv");
        let (p, s) = postings(&b);
        assert!(p.iter().all(|(f, _)| *f != Family::Sndr), "{sender}: {p:?}");
        assert_eq!(s, None, "{sender}");
    }
    // A 256-byte sender is still an account.
    let long = "y".repeat(256);
    let b = block(&cmd_with_sender(Value::from(long.clone())), "k:recv");
    let (p, s) = postings(&b);
    assert!(p.contains(&(Family::Sndr, long.clone())), "{p:?}");
    assert_eq!(s, Some(long));
}

#[test]
fn an_unparsed_cmd_gets_no_sndr_posting() {
    // Not JSON at all: the record is `cmd_unparsed`, only output postings exist.
    let b = block(r#"{"meta":{"sender":"k:abc"}"#, "k:recv");
    let (p, s) = postings(&b);
    assert!(p.iter().all(|(f, _)| *f != Family::Sndr), "{p:?}");
    assert_eq!(s, None);
}

#[test]
fn sndr_is_a_subset_of_acct_on_every_tx() {
    for sender in ["k:a", "alice", "w:abc:keys-all"] {
        let b = block(&cmd_with_sender(Value::from(sender)), "k:recv");
        let (p, _) = postings(&b);
        for (f, n) in &p {
            if *f == Family::Sndr {
                assert!(p.contains(&(Family::Acct, n.clone())), "{p:?}");
            }
        }
    }
}
