//! Shared helpers of the fuzz targets and the seed generator.

use std::path::PathBuf;

use kdx_types::hash::{b64url_encode, blake2b_256};
use serde_json::{json, Value};

const OK_OUT_TAIL: &str =
    r#""logs":null,"events":[],"metaData":null,"continuation":null,"txId":null}"#;

fn h(n: u8) -> String {
    b64url_encode(&[n; 32])
}

/// A node-style output JSON text for request key `rk` with `data` as the result data.
pub fn output_json(rk: &str, data: &str) -> String {
    format!(
        r#"{{"gas":1,"result":{{"status":"success","data":{data}}},"reqKey":"{rk}",{OK_OUT_TAIL}"#
    )
}

/// A `cmd` JSON text with `data` as the exec env data.
pub fn cmd_json(data: &str) -> String {
    format!(
        r#"{{"payload":{{"exec":{{"data":{data},"code":"(+ 1 2)"}}}},"meta":{{"sender":"k:a"}},"nonce":"n"}}"#
    )
}

/// A block (header, payload) with one user tx built from the raw `cmd` and `output` JSON
/// texts and a miner-data text. Hashes the fuzzer cannot know are filler; the decoder does
/// not check them (the verifier does).
pub fn block_from_parts(cmd: &[u8], output: &[u8], miner: &[u8]) -> (Value, Value) {
    let rk = blake2b_256(cmd).to_b64url();
    let tx_json = format!(
        r#"{{"hash":"{rk}","sigs":[{{"sig":"ab"}}],"cmd":{}}}"#,
        serde_json::to_string(&String::from_utf8_lossy(cmd)).unwrap_or_default()
    );
    let cb = output_json(&h(9), "\"ok\"");
    let header = json!({
        "nonce": "1", "creationTime": 1, "parent": h(1), "adjacents": {"1": h(2)},
        "target": h(3), "payloadHash": h(4), "chainId": 0, "weight": h(5), "height": 5,
        "chainwebVersion": "mainnet01", "epochStart": 1, "featureFlags": 0, "hash": h(6)
    });
    let payload = json!({
        "transactions": [[b64url_encode(tx_json.as_bytes()), b64url_encode(output)]],
        "minerData": b64url_encode(miner),
        "transactionsHash": h(7), "outputsHash": h(8), "payloadHash": h(4),
        "coinbase": b64url_encode(cb.as_bytes()),
    });
    (header, payload)
}

/// A per-process scratch file path (three targets can run at once; names do not clash).
pub fn scratch(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("kdx-fuzz-{}-{name}", std::process::id()))
}
