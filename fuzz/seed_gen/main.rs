//! Writes the seed corpora: `gen_seeds OUT_DIR` creates `OUT_DIR/<target>/<seed>` from records
//! and files our own writers produce and from requests we write. Nothing is fetched.

use std::fs;
use std::path::{Path, PathBuf};

use kdx_decode::{build_records, decode_block, verify_block, Family};
use kdx_segment::keys::write_keys;
use kdx_segment::phf::{write_index, IndexKey};
use kdx_segment::postings::{encode_list, PostingsWriter};
use kdx_segment::{Manifest, RangeEntry, RangeKind};
use kdx_types::hash::{b64url_encode, blake2b_256};
use kdx_types::record::{encode_block, encode_tx};
use kdx_types::value::to_vec;
use kdx_types::{DictKind, DictMut, Hash32, MemDict};
use serde_json::json;

struct Out(PathBuf);

impl Out {
    fn put(&self, target: &str, name: &str, bytes: &[u8]) {
        let d = self.0.join(target);
        fs::create_dir_all(&d).expect("mkdir");
        fs::write(d.join(name), bytes).expect("write seed");
    }
}

fn main() {
    let out = Out(PathBuf::from(
        std::env::args().nth(1).expect("usage: gen_seeds OUT_DIR"),
    ));
    payload(&out);
    values(&out);
    segment_files(&out);
    manifests(&out);
    tokens(&out);
    small(&out);
}

fn payload(out: &Out) {
    let datas = [
        r#"{"k":1}"#,
        r#"[1,2,{"a":"b"}]"#,
        r#"{"big":123456789012345678901234567890,"f":1.50}"#,
        r#"{"a":"\ud800","b":"\u00e9"}"#,
    ];
    for (i, data) in datas.iter().enumerate() {
        let cmd = kdx_fuzz::cmd_json(data);
        let rk = blake2b_256(cmd.as_bytes()).to_b64url();
        let o = kdx_fuzz::output_json(&rk, data);
        let miner = r#"{"account":"k:m","predicate":"keys-all","public-keys":["m"]}"#;
        let mut seed = vec![1u8];
        seed.extend_from_slice(cmd.as_bytes());
        seed.push(0);
        seed.extend_from_slice(o.as_bytes());
        seed.push(0);
        seed.extend_from_slice(miner.as_bytes());
        out.put("payload_decode", &format!("parts-{i}"), &seed);

        let (header, payload) =
            kdx_fuzz::block_from_parts(cmd.as_bytes(), o.as_bytes(), miner.as_bytes());
        let doc = json!({"header": header, "payload": payload});
        out.put(
            "payload_decode",
            &format!("block-{i}"),
            doc.to_string().as_bytes(),
        );

        // Records of the same block.
        let b = decode_block(&header, &payload).expect("seed block decodes");
        let report = verify_block(&b, &b.header.payload_hash).unwrap_or_default();
        let mut dict = MemDict::new();
        let (br, txs) = build_records(&b, &report, &mut dict).expect("records");
        let mut buf = Vec::new();
        encode_block(&br, &mut buf);
        out.put("record_decode", &format!("block-{i}"), &buf);
        for (j, t) in txs.iter().enumerate() {
            let mut buf = Vec::new();
            encode_tx(t, &mut buf);
            out.put("record_decode", &format!("tx-{i}-{j}"), &buf);
        }
    }
}

fn values(out: &Out) {
    let mut d = MemDict::new();
    for k in DictKind::ALL {
        for n in ["k:a", "coin", "TRANSFER", "x", "y"] {
            d.intern(k, n);
        }
    }
    let vs = [
        json!(null),
        json!({"a": [1, 2.5, "k:a", true], "b": {"c": "x"}}),
        json!({"amount": {"decimal": "12.5"}, "n": 12345678901234567890u64}),
        json!(["k:a", "coin", "y", {"": ""}]),
    ];
    for (i, v) in vs.iter().enumerate() {
        out.put("value_decode", &format!("v{i}"), &to_vec(v, &d));
    }
}

fn tmp(name: &str) -> PathBuf {
    kdx_fuzz::scratch(name)
}

fn segment_files(out: &Out) {
    let hdr = |range: u64, bound: u32| {
        let mut h = range.to_le_bytes().to_vec();
        h.extend_from_slice(&bound.to_le_bytes());
        h
    };
    // Posting lists in each encoding the writer picks.
    let lists: [Vec<u32>; 5] = [
        vec![],
        vec![5],
        vec![1, 3, 7, 200, 9000],
        (0..5000).step_by(3).collect(),
        (0..70_000).filter(|v| v % 5 != 0).collect(),
    ];
    for (i, l) in lists.iter().enumerate() {
        let mut b = Vec::new();
        encode_list(l, 100_000, &mut b).expect("encode list");
        let mut seed = hdr(100_000, 1_000);
        seed.extend_from_slice(&b);
        out.put("postings_list", &format!("list-{i}"), &seed);
    }
    // A whole postings file.
    let p = tmp("seed-postings");
    let mut w =
        PostingsWriter::create(&p, Family::from_u8(0).expect("family"), 100_000).expect("create");
    for (e, l) in lists.iter().enumerate().skip(1) {
        w.push(10 + e as u64, l).expect("push");
    }
    w.finish().expect("finish");
    let mut seed = hdr(100_000, 70_000);
    seed.extend_from_slice(&fs::read(&p).expect("read"));
    out.put("postings_list", "file", &seed);
    let _ = fs::remove_file(&p);

    // keys.ef
    for (i, counts) in [vec![3u32, 0, 7, 1], vec![1; 40], vec![100, 5000, 0, 2]]
        .iter()
        .enumerate()
    {
        let p = tmp("seed-keys");
        write_keys(&p, 2, counts).expect("write keys");
        out.put(
            "ef_keys",
            &format!("keys-{i}"),
            &fs::read(&p).expect("read"),
        );
        let _ = fs::remove_file(&p);
    }

    // Indexes: PtrHash (enough keys) and sorted.
    for (i, (n, sorted)) in [(3usize, true), (300, false), (5000, false)]
        .iter()
        .enumerate()
    {
        let keys: Vec<IndexKey> = (0..*n)
            .map(|j| IndexKey::new(&Hash32(blake2b_256(&j.to_le_bytes()).0), j as u32))
            .collect();
        let p = tmp("seed-index");
        write_index(&p, keys, *sorted).expect("write index");
        let mut seed = blake2b_256(b"probe").0.to_vec();
        seed.extend_from_slice(&fs::read(&p).expect("read"));
        out.put("phf_open", &format!("index-{i}"), &seed);
        let _ = fs::remove_file(&p);
    }
}

fn manifests(out: &Out) {
    let mut m = Manifest::new("mainnet01");
    m.push(RangeEntry {
        kind: RangeKind::Step,
        start: 0,
        end: 10_000,
        first_txnum: 0,
        tx_count: 5000,
        block_count: 10_000,
    })
    .expect("push");
    m.push(RangeEntry {
        kind: RangeKind::Step,
        start: 10_000,
        end: 20_000,
        first_txnum: 5000,
        tx_count: 100,
        block_count: 10_000,
    })
    .expect("push");
    out.put(
        "manifest_parse",
        "manifest.json",
        &serde_json::to_vec_pretty(&m).expect("json"),
    );
    out.put(
        "manifest_parse",
        "range.toml",
        b"format_version = 2\nnetwork = \"mainnet01\"\nkind = \"step\"\nstart = 0\nend = 10000\nfirst_txnum = 0\ntx_count = 5\nblock_count = 10000\nslot_width = 2\nblock_records = 10000\nlevel = 9\ntxs_dict_id = 0\n",
    );
    out.put(
        "manifest_parse",
        "search.json",
        br#"{"format":1,"built_at":1700000000}"#,
    );
}

fn tokens(out: &Out) {
    // Cursor: version 1 | tx key u64 BE | sub u16 | fingerprint u32.
    let mut c = vec![1u8];
    c.extend_from_slice(&0x0000_0001_0000_0042u64.to_be_bytes());
    c.extend_from_slice(&3u16.to_be_bytes());
    c.extend_from_slice(&0xdead_beefu32.to_be_bytes());
    out.put("api_tokens", "cursor", b64url_encode(&c).as_bytes());
    let mut b = vec![0xB1u8];
    b.extend_from_slice(&4_000_000u64.to_be_bytes());
    b.push(7);
    out.put("api_tokens", "block-cursor", b64url_encode(&b).as_bytes());
    use base64::Engine as _;
    for (i, id) in [
        "Block:abc",
        "FungibleAccount:[\"coin\",\"k:a\"]",
        "FungibleChainAccount:[\"3\",\"coin\",\"k:a\"]",
        "Transaction:[\"hash\",\"rk\"]",
        "Signer:[\"rk\",\"1\"]",
    ]
    .iter()
    .enumerate()
    {
        out.put(
            "api_tokens",
            &format!("id-{i}"),
            base64::engine::general_purpose::STANDARD
                .encode(id)
                .as_bytes(),
        );
    }
}

fn small(out: &Out) {
    for (i, s) in [
        &b"%a%b\x00xxaxxbxx"[..],
        b"transfer\x00(coin.transfer \"a\" \"b\" 1.0)",
        b"a_c\x00abc",
        b"\\%x\x0050%x",
        b"\xc3\xa9%\x00caf\xc3\xa9",
    ]
    .iter()
    .enumerate()
    {
        out.put("like_match", &format!("p{i}"), s);
    }
    for (i, s) in [
        &b"\x00203.0.113.7"[..],
        b"\x0010.1.1.1, 203.0.113.7, 10.0.0.2",
        b"\x00::ffff:203.0.113.7",
        b"\x0110.2.3.4\n198.51.100.5, 10.9.9.9",
        b"\x00[2001:db8::1]:4000",
        b"\x02not-an-ip",
    ]
    .iter()
    .enumerate()
    {
        out.put("xff_parse", &format!("x{i}"), s);
    }
    let q = |s: serde_json::Value| s.to_string();
    let bodies = [
        q(json!({"query": "{ lastBlockHeight }"})),
        q(json!({"query": "{ graphConfiguration { version } networkInfo { networkHost } }"})),
        q(
            json!({"query": "query($n: Int, $c: String) { blocksFromHeight(startHeight: 100, first: $n, after: $c) { pageInfo { hasNextPage endCursor } edges { cursor node { hash chainId height } } } }", "variables": {"n": 5, "c": null}}),
        ),
        q(
            json!({"query": "query($a: String!) { fungibleAccount(accountName: $a, fungibleName: \"coin\") { accountName totalBalance chainAccounts { chainId balance } } }", "variables": {"a": "k:abc"}}),
        ),
        q(
            json!({"query": "{ transactions(first: 3, minimumDepth: 2) { edges { node { hash result { ... on TransactionResult { goodResult badResult } } } } } }"}),
        ),
        q(
            json!({"query": "{ events(qualifiedEventName: \"coin.TRANSFER\", first: 2) { edges { node { moduleName name parameters } } } }"}),
        ),
        q(
            json!({"query": "query($id: ID!) { node(id: $id) { id __typename } }", "variables": {"id": "QmxvY2s6YWJj"}}),
        ),
        q(
            json!([{"query": "{ lastBlockHeight }"}, {"query": "{ __typename }", "operationName": null}]),
        ),
        q(
            json!({"query": "{ __schema { queryType { name } types { name kind fields { name } } } }"}),
        ),
        q(
            json!({"query": "{ a: lastBlockHeight b: lastBlockHeight ...F } fragment F on Query { lastBlockHeight }"}),
        ),
    ];
    for (i, b) in bodies.iter().enumerate() {
        out.put("graphql_request", &format!("req-{i}"), b.as_bytes());
    }
    let _ = Path::new("");
}
