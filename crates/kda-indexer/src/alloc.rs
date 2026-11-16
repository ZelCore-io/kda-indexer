//! Allocator statistics for `/metrics` (jemalloc `stats.*`, via `tikv-jemalloc-ctl`).
//!
//! The binary installs jemalloc as the global allocator (`main.rs`), configured through the
//! embedded `malloc_conf` there. This module only reads its counters, every 10 s, into the
//! `kdx_alloc_*` gauges. Without jemalloc (other targets) it does nothing.

use tokio_util::sync::CancellationToken;

/// How often the gauges are refreshed.
pub const REFRESH: std::time::Duration = std::time::Duration::from_secs(10);

/// One reading of the allocator counters, in bytes.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Debug, Clone, Copy)]
pub struct Stats {
    pub allocated: usize,
    pub active: usize,
    pub resident: usize,
    pub retained: usize,
    pub mapped: usize,
}

/// Advance the stats epoch, then read the five counters. Any failed read is an error and no
/// partial reading is returned.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn read() -> Result<Stats, tikv_jemalloc_ctl::Error> {
    use tikv_jemalloc_ctl::{epoch, stats};
    // jemalloc caches its counters; they only move when the epoch advances.
    epoch::advance()?;
    Ok(Stats {
        allocated: stats::allocated::read()?,
        active: stats::active::read()?,
        resident: stats::resident::read()?,
        retained: stats::retained::read()?,
        mapped: stats::mapped::read()?,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn gauge(v: usize) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Read the counters once and store them in the gauges. On error the gauges stay as they
/// were.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn refresh() -> Result<(), tikv_jemalloc_ctl::Error> {
    apply(read)
}

/// Store the reading of `read` in the gauges; an `Err` changes nothing.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn apply<E>(read: impl FnOnce() -> Result<Stats, E>) -> Result<(), E> {
    let s = read()?;
    let m = kdx_metrics::get();
    m.alloc_allocated_bytes.set(gauge(s.allocated));
    m.alloc_active_bytes.set(gauge(s.active));
    m.alloc_resident_bytes.set(gauge(s.resident));
    m.alloc_retained_bytes.set(gauge(s.retained));
    m.alloc_mapped_bytes.set(gauge(s.mapped));
    Ok(())
}

/// Refresh the gauges every [`REFRESH`] until `stop`. A failed read logs once at warn (again
/// only after a success in between) and never ends the loop or the process.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub async fn stats_loop(stop: CancellationToken) {
    let mut failed = false;
    loop {
        match refresh() {
            Ok(()) => failed = false,
            Err(e) => {
                if !failed {
                    tracing::warn!(error = %e, "alloc: stats read failed (gauges keep their last values)");
                }
                failed = true;
            }
        }
        tokio::select! {
            () = stop.cancelled() => break,
            () = tokio::time::sleep(REFRESH) => {}
        }
    }
}

/// Start the refresh task (`run` and `serve`).
pub fn spawn_stats(stop: CancellationToken) {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    tokio::spawn(stats_loop(stop));
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    drop(stop);
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;

    /// A failed read leaves the gauges as they were.
    #[test]
    fn failed_read_keeps_gauges() {
        let m = kdx_metrics::get();
        let s = Stats {
            allocated: 1,
            active: 2,
            resident: 3,
            retained: 4,
            mapped: 5,
        };
        apply::<()>(|| Ok(s)).unwrap();
        assert_eq!(m.alloc_resident_bytes.get(), 3);
        assert!(apply(|| Err("boom")).is_err());
        assert_eq!(m.alloc_allocated_bytes.get(), 1);
        assert_eq!(m.alloc_active_bytes.get(), 2);
        assert_eq!(m.alloc_resident_bytes.get(), 3);
        assert_eq!(m.alloc_retained_bytes.get(), 4);
        assert_eq!(m.alloc_mapped_bytes.get(), 5);
    }
}
