//! `reconcile-counts` memory per key (PR #48 review I-3): the live heap of a [`Tally`] and the
//! peak while the write turns it into sorted runs one kind at a time, counted by a wrapping
//! allocator. A measurement, not a check: `cargo test -p kdx-hot --test tally_mem --
//! --ignored --nocapture`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use kdx_decode::rowcount::CountKind;
use kdx_hot::rowcount::Tally;

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let n = LIVE.fetch_add(l.size(), Ordering::Relaxed) + l.size();
        PEAK.fetch_max(n, Ordering::Relaxed);
        // SAFETY: forwards the caller's layout to the system allocator.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        // SAFETY: `p` was allocated by `alloc` above with the same layout.
        unsafe { System.dealloc(p, l) }
    }
}

#[global_allocator]
static A: Counting = Counting;

#[test]
#[ignore = "measurement"]
fn tally_bytes_per_key() {
    // A mainnet-like spread of keys over the kinds (accounts dominate).
    let share = [30u64, 30, 2, 2, 1, 20, 15];
    let n: u64 = 2_000_000;
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let mut t = Tally::default();
    for (k, s) in CountKind::ALL.iter().zip(share) {
        for e in 0..n * s / 100 {
            t.insert((*k, e, (e % 20) as u8), (e, 0));
        }
    }
    let keys = t.len() as f64;
    let held = (LIVE.load(Ordering::Relaxed) - base) as f64;
    let grow = (PEAK.load(Ordering::Relaxed) - base) as f64;
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    let mut written = 0usize;
    for k in CountKind::ALL {
        let run = t.take_sorted(k);
        written += run.len();
    }
    let peak = (PEAK.load(Ordering::Relaxed) - base) as f64;
    assert_eq!(written as f64, keys);
    println!(
        "keys {keys}: tally {:.1} B/key, peak while counting (map growth) {:.1} B/key, peak while writing {:.1} B/key",
        held / keys,
        grow / keys,
        peak / keys
    );
}
