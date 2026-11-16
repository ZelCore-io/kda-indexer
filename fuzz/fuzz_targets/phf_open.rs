#![no_main]
//! Saved index parts (PtrHash and sorted): open, then look keys up (M5). The first 32 bytes
//! of the input are the key looked up; the rest is the file.

use kdx_segment::phf::{Index, KeyIndex};
use kdx_types::Hash32;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 32 {
        return;
    }
    let key = Hash32(data[..32].try_into().unwrap());
    let path = kdx_fuzz::scratch("index");
    if std::fs::write(&path, &data[32..]).is_err() {
        return;
    }
    let Ok(ix) = Index::open(&path) else { return };
    let _ = (ix.kind(), ix.len(), ix.is_empty());
    let mut out = Vec::new();
    ix.candidates(&key, &mut out);
    let _ = ix.get(&key);
    for b in 0..4u8 {
        let mut k = key;
        k.0[0] = b;
        let _ = ix.get(&k);
    }
});
