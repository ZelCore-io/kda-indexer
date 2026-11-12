#![no_main]
//! The dictionary-tagged msgpack `Value` codec.

use kdx_types::value::{decode, encode, from_slice};
use kdx_types::{DictKind, DictMut, MemDict};
use libfuzzer_sys::fuzz_target;

fn dict() -> MemDict {
    let mut d = MemDict::new();
    for k in DictKind::ALL {
        for n in ["k:a", "coin", "TRANSFER", "x", "y"] {
            d.intern(k, n);
        }
    }
    d
}

fuzz_target!(|data: &[u8]| {
    let d = dict();
    if let Ok(v) = from_slice(data, &d) {
        let mut out = Vec::new();
        encode(&v, &d, &mut out);
        let _ = from_slice(&out, &d);
    }
    let mut cur = data;
    let _ = decode(&mut cur, &d);
});
