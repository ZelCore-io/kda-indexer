#![no_main]
//! Posting lists: the list decoder, the Roaring view and the lazy walk, and a whole postings
//! file. Input: `u64 range_txs | u32 bound | bytes`.
//!
//! A crash inside `sucds` (`Vec::with_capacity` on a damaged Elias-Fano length) is the known
//! finding for the P9 fix round (R-P5P9-25); it aborts with an allocation failure.

use std::sync::Arc;

use kdx_segment::postings::{decode_list, Postings};
use kdx_segment::RoaringView;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 12 {
        return;
    }
    let _range_txs = u64::from_le_bytes(data[..8].try_into().unwrap());
    let bound = u32::from_le_bytes(data[8..12].try_into().unwrap());
    let body = &data[12..];

    let _ = decode_list(body);

    let backing: Arc<dyn AsRef<[u8]> + Send + Sync> = Arc::new(body.to_vec());
    if let Ok(v) = RoaringView::parse(backing, 0, body.len()) {
        let _ = (v.len(), v.is_empty());
    }

    // A whole postings file: open, decode every list, walk the lazy iterator.
    let path = kdx_fuzz::scratch("postings");
    if std::fs::write(&path, body).is_err() {
        return;
    }
    if let Ok(p) = Postings::open(&path, 1_000) {
        let n = p.entities().min(64);
        for i in 0..n {
            if let Ok(full) = p.list_at(i) {
                let _ = p.lazy_walk_matches(i, &full, u64::from(bound));
            }
            let e = p.entity_at(i);
            for max in [None, Some(bound)] {
                if let Ok(Some(it)) = p.list_le(e, max) {
                    for _ in it.take(256) {}
                }
            }
        }
        let _ = p.list(u64::from(bound));
    }
});
