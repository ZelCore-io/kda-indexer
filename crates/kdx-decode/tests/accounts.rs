//! Acct postings are a pure function of the block (review H1) and over-long account strings
//! are never interned (review H2).

use kdx_decode::{decode_block, index_block, verify_block, DecodedBlock, Family, VerifyReport};
use kdx_types::hash::b64url_encode;
use kdx_types::{blake2b_256, Dict, DictKind, DictMut, MemDict};
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

/// Acct posting names of the user tx.
fn acct_names(b: &DecodedBlock, dict: &mut MemDict) -> Vec<String> {
    let ib = index_block(b, &VerifyReport::default(), dict).expect("indexes");
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
fn crosschain_receiver_seen_only_as_event_param_is_posted() {
    let from = format!("k:{}", "a".repeat(64));
    let to = format!("k:{}", "b".repeat(64));
    let b = block(json!([event(
        "coin",
        "TRANSFER_XCHAIN",
        json!([from, to, 1.5, "2"])
    )]));
    let mut fresh = MemDict::new();
    let names = acct_names(&b, &mut fresh);
    assert!(names.contains(&to), "receiver must be posted: {names:?}");
    assert!(names.contains(&from));
    // Same posting set whatever the dictionary already held (order independence).
    let mut warm = MemDict::new();
    warm.intern(DictKind::Account, &to);
    warm.intern(DictKind::Account, "k:unrelated");
    assert_eq!(acct_names(&b, &mut warm), names);
}

#[test]
fn principals_at_any_depth_and_poly_fungible_sides_are_posted() {
    let w = format!("w:{}:keys-all", "A".repeat(43));
    let b = block(json!([
        event("m", "RECONCILE", json!([{"x": {"y": [w]}}, "alice-vanity"])),
        event("ledger", "TRANSFER", json!(["t:id", "bob", "carol", 1.0])),
    ]));
    let names = acct_names(&b, &mut MemDict::new());
    assert!(names.contains(&w));
    assert!(names.contains(&"bob".to_owned()) && names.contains(&"carol".to_owned()));
    // A non-principal name that only appears in other params gets no posting.
    assert!(!names.contains(&"alice-vanity".to_owned()));
}

#[test]
fn over_long_account_is_not_interned() {
    let long = "x".repeat(5000);
    let b = block(json!([event(
        "spam",
        "TRANSFER",
        json!([long, "k:ok", 1.0])
    )]));
    let mut dict = MemDict::new();
    let ib = index_block(&b, &VerifyReport::default(), &mut dict).expect("indexes");
    assert_eq!(ib.unindexed_accounts, 1);
    assert!(dict.find(DictKind::Account, &long).is_none());
    assert!(dict.find(DictKind::Account, "k:ok").is_some());
}

#[test]
fn principal_grammar() {
    use kdx_decode::build::is_principal;
    assert!(is_principal(&format!("k:{}", "0f".repeat(32))));
    assert!(!is_principal(&format!("k:{}", "0F".repeat(32))));
    assert!(!is_principal("k:abc"));
    assert!(is_principal(&format!("c:{}", "_".repeat(43))));
    assert!(is_principal("r:ns.ks") && is_principal("m:free.mod"));
    assert!(is_principal(&format!("u:free.m.f:{}", "a".repeat(43))));
    assert!(is_principal(&format!("p:{}:free.m.f", "a".repeat(43))));
    assert!(!is_principal("alice") && !is_principal("r:") && !is_principal("x:y"));
}

/// Review M1: the `xfer`-only sides (a suffix-TRANSFER's `(id, from, rational)` params, the
/// non-principal names of an `X_TRANSFER`) are interned by `intern_block`, in the documented
/// order, before the records are encoded: `extract` interns nothing new.
#[test]
fn xfer_only_sides_are_interned_before_encoding() {
    let to = format!("k:{}", "c".repeat(64));
    let evs = json!([
        event("wiz", "WIZ_TRANSFER", json!(["nft-17", to, 1.5])),
        event("tok", "X_TRANSFER", json!(["alice", "bob", 1.0])),
    ]);
    let b = block(evs);
    let mut dict = MemDict::new();
    kdx_decode::build::intern_block(&b, &mut dict);
    let sender = format!("k:{}", "5".repeat(64));
    // Miner, sender, then per event: `event_accounts` (principals), then the xfer-only sides.
    assert_eq!(
        dict.names(DictKind::Account),
        &["k:m", &sender, &to, "nft-17", "alice", "bob"]
    );
    let before: Vec<usize> = DictKind::ALL.iter().map(|&k| dict.len(k)).collect();
    let ib = index_block(&b, &VerifyReport::default(), &mut dict).expect("indexes");
    let after: Vec<usize> = DictKind::ALL.iter().map(|&k| dict.len(k)).collect();
    assert_eq!(before, after, "extract interned a name intern_block missed");
    let xfer = ib.idx[1]
        .postings
        .iter()
        .filter(|(f, _)| *f == Family::Xfer)
        .count();
    assert_eq!(xfer, 4);
}

/// Review M1: record bytes and postings of a block are the same whether the block is the
/// first to mention its xfer-only names (backfill) or is applied again with those names
/// already interned (the tail after a reorg undo keeps its names; the freezer and merge copy
/// the tail's record bytes as they are).
#[test]
fn record_bytes_do_not_depend_on_prior_interning() {
    let evs = json!([
        event("wiz", "WIZ_TRANSFER", json!(["nft-17", "k:ok", 1.5])),
        event("tok", "X_TRANSFER", json!(["alice", "bob", 1.0])),
    ]);
    let b = block(evs);
    let bytes = |dict: &mut MemDict| {
        let ib = index_block(&b, &VerifyReport::default(), dict).expect("indexes");
        let mut txs = Vec::new();
        for t in &ib.txs {
            kdx_types::record::encode_tx(t, &mut txs);
        }
        let mut blk = Vec::new();
        kdx_types::record::encode_block(&ib.block, &mut blk);
        (blk, txs, ib.idx)
    };
    // Backfill: a fresh dictionary, the block is the first mention.
    let backfill = bytes(&mut MemDict::new());
    // Tail: the block applied, undone by a reorg (names stay), then applied again.
    let mut tail = MemDict::new();
    let first = bytes(&mut tail);
    let again = bytes(&mut tail);
    assert_eq!(first, backfill);
    assert_eq!(again, backfill);
}
