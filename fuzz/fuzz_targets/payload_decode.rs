#![no_main]
//! Node payload and output JSON through decode, verify, build and the record round trip.
//!
//! Input forms: a JSON object `{"header":…,"payload":…}` (the block as the node serves it), or
//! `0x01 cmd-json 0x00 output-json 0x00 miner-json`, which wraps the three texts into a block
//! so the fuzzer mutates the inner JSON directly instead of its base64.

use kdx_decode::json::parse_untrusted;
use kdx_decode::{decode_block, index_block, verify_block, VerifyReport};
use kdx_types::record::{decode_tx, encode_tx};
use kdx_types::MemDict;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let (header, payload) = if data.first() == Some(&1) {
        let mut parts = data[1..].splitn(3, |&b| b == 0);
        let cmd = parts.next().unwrap_or(&[]);
        let out = parts.next().unwrap_or(&[]);
        let miner = parts.next().unwrap_or(&[]);
        kdx_fuzz::block_from_parts(cmd, out, miner)
    } else {
        let Ok(v) = parse_untrusted(data) else { return };
        match (v.get("header"), v.get("payload")) {
            (Some(h), Some(p)) => (h.clone(), p.clone()),
            _ => (v.clone(), v),
        }
    };
    let Ok(b) = decode_block(&header, &payload) else {
        return;
    };
    let report =
        verify_block(&b, &b.header.payload_hash).unwrap_or_else(|_| VerifyReport::default());
    let mut dict = MemDict::new();
    let Ok(_) = index_block(&b, &report, &mut dict) else {
        return;
    };
    // Records built from any decoded block must survive their own codec.
    if let Ok((_, txs)) = kdx_decode::build_records(&b, &report, &mut dict) {
        for r in &txs {
            let mut buf = Vec::new();
            encode_tx(r, &mut buf);
            let r2 = decode_tx(&buf).expect("a record we encoded must decode");
            assert_eq!(&r2, r);
            let _ = kdx_decode::render_tx_bytes(r, &dict);
            let _ = kdx_decode::render_output_bytes(r, &dict);
        }
    }
});
