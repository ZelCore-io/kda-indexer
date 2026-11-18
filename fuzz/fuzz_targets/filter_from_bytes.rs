#![no_main]
//! Key filter bytes (`kf.fil` and one filter section): decode, then query. Anything accepted
//! must answer without panicking, and a whole file must re-encode to the same bytes.

use kdx_segment::filter::{decode_pair, encode_pair, KeyFilter};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(f) = KeyFilter::from_bytes(data) {
        let _ = (f.len(), f.is_empty(), f.bytes());
        for k in [0u64, 1, 0xdead_beef, u64::MAX] {
            let _ = f.contains(k);
        }
        assert_eq!(f.to_bytes(), data);
    }
    if let Ok((bh, rk)) = decode_pair(data) {
        for k in [0u64, 1, 0xdead_beef, u64::MAX] {
            let _ = (bh.contains(k), rk.contains(k));
        }
        assert_eq!(encode_pair(&bh, &rk), data);
    }
});
