#![no_main]
//! Replica files come from another host: arbitrary bytes as a tx or block record.

use kdx_types::record::{decode_block, decode_block_with, decode_tx, encode_block, encode_tx};
use kdx_types::{Hash32, MemDict};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let dict = MemDict::new();
    if let Ok(r) = decode_tx(data) {
        let mut buf = Vec::new();
        encode_tx(&r, &mut buf);
        let r2 = decode_tx(&buf).expect("re-encoded record decodes");
        assert_eq!(r2, r);
        let _ = kdx_decode::render_tx_bytes(&r, &dict);
        let _ = kdx_decode::render_output_bytes(&r, &dict);
    }
    if let Ok(b) = decode_block(data) {
        let mut buf = Vec::new();
        encode_block(&b, &mut buf);
        let _ = decode_block(&buf);
        let _ = kdx_decode::render::render_header(&b);
        let _ = kdx_decode::render_miner_bytes(&b, &dict);
    }
    let resolve = |c: u8, h: u64| Some(Hash32([c ^ (h as u8); 32]));
    let _ = decode_block_with(data, &resolve);
});
