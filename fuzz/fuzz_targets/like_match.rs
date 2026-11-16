#![no_main]
//! The LIKE matcher and the 3-gram split. Input: `pattern 0x00 text`. A pathological pattern
//! shows up as a libFuzzer timeout (`-timeout=2`).

use kdx_search::grams;
use kdx_types::like::Like;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let (p, t) = match data.iter().position(|&b| b == 0) {
        Some(i) => (&data[..i], &data[i + 1..]),
        None => (data, &data[..0]),
    };
    let (p, t) = (String::from_utf8_lossy(p), String::from_utf8_lossy(t));
    let like = Like::new(&p);
    let m = like.matches(&t);
    let _ = like.literal_runs();
    if let Some(gs) = grams(&like) {
        // Every string the pattern matches contains each of its grams.
        if m {
            for g in &gs {
                assert!(t.contains(g.as_str()));
            }
        }
    }
});
