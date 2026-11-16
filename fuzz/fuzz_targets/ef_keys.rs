#![no_main]
//! `keys.ef`: the Elias-Fano slot table and its lookups.
//!
//! A crash inside `sucds` (`Vec::with_capacity` on a damaged length) is the known finding for
//! the P9 fix round (R-P5P9-25).

use kdx_segment::keys::Keys;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let path = kdx_fuzz::scratch("keys.ef");
    if std::fs::write(&path, data).is_err() {
        return;
    }
    let Ok(k) = Keys::open(&path) else { return };
    let slots = k.slots();
    for s in (0..slots.min(64)).chain(slots.saturating_sub(2)..slots + 2) {
        let _ = (k.first(s), k.count(s));
    }
    let total = k.tx_count();
    for t in [
        0,
        1,
        total / 2,
        total.saturating_sub(1),
        total,
        total + 1,
        u64::MAX,
    ] {
        let _ = k.slot_of(t);
    }
});
