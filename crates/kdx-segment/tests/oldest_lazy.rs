//! N-01: the oldest-first walk of a Roaring list is lazy. Taking a few entries from a list
//! of millions allocates a bounded amount (no whole-list `Vec`), and a seek to a minimum
//! value starts there without walking the entries below it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use kdx_decode::Family;
use kdx_segment::postings::{Postings, PostingsWriter};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let n = LIVE.fetch_add(l.size(), Ordering::SeqCst) + l.size();
        PEAK.fetch_max(n, Ordering::SeqCst);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::SeqCst);
        unsafe { System.dealloc(p, l) }
    }
}

#[global_allocator]
static A: Counting = Counting;

/// Bytes allocated above the live level at the start of `f`, at the peak of `f`.
fn peak_during<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let start = LIVE.load(Ordering::SeqCst);
    PEAK.store(start, Ordering::SeqCst);
    let out = f();
    (out, PEAK.load(Ordering::SeqCst).saturating_sub(start))
}

#[test]
fn oldest_first_on_a_big_roaring_list_is_lazy_and_seeks() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("acct.pst");
    // 4M entries, every other value of an 8M range: bitmap containers (Roaring encoding).
    let range: u64 = 8 << 20;
    let vals: Vec<u32> = (0..(range as u32)).step_by(2).collect();
    let mut w = PostingsWriter::create(&p, Family::Acct, range).unwrap();
    w.push(7, &vals).unwrap();
    w.finish().unwrap();
    drop(vals);
    let pf = Postings::open(&p, 100).unwrap();

    // Oldest first: the first 10 entries, a bounded allocation (the eager walk built a
    // 32 MB Vec<u64> of the whole list).
    let (got, peak) = peak_during(|| {
        pf.list(7)
            .unwrap()
            .oldest_first()
            .take(10)
            .collect::<Vec<_>>()
    });
    assert_eq!(got, (0..10u64).map(|i| 100 + 2 * i).collect::<Vec<_>>());
    assert!(peak < 1 << 20, "oldest_first allocated {peak} bytes");

    // Under a bound: still ascending, still capped at the bound.
    let got: Vec<u64> = pf
        .list_le(7, Some(21))
        .unwrap()
        .unwrap()
        .oldest_first()
        .collect();
    assert_eq!(got, (0..=10u64).map(|i| 100 + 2 * i).collect::<Vec<_>>());

    // Seek: from a minimum value, inside a container and at a container edge.
    for min in [0u32, 1, 4_000_001, 65_536, 131_071, 8_388_606, 8_388_607] {
        let (got, peak) = peak_during(|| {
            pf.list(7)
                .unwrap()
                .oldest_from(Some(min))
                .take(3)
                .collect::<Vec<_>>()
        });
        let want: Vec<u64> = (u64::from(min)..range)
            .filter(|v| v % 2 == 0)
            .take(3)
            .map(|v| 100 + v)
            .collect();
        assert_eq!(got, want, "min {min}");
        assert!(peak < 1 << 20, "oldest_from({min}) allocated {peak} bytes");
    }
    assert_eq!(
        pf.list(7).unwrap().oldest_from(Some(u32::MAX)).count(),
        0,
        "a seek past the end"
    );
}
