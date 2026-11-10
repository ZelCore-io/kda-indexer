//! Free-space check and process memory readings.

use std::ffi::CString;
use std::path::Path;

use anyhow::{bail, Context, Result};

/// Free bytes for unprivileged writers on the filesystem holding `path` (statvfs).
pub fn free_bytes(path: &Path) -> Result<u64> {
    let c = CString::new(path.as_os_str().as_encoded_bytes()).context("path with NUL")?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path and `st` a writable statvfs.
    let rc = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("statvfs {}", path.display()));
    }
    #[allow(clippy::unnecessary_cast)]
    Ok(st.f_bavail as u64 * st.f_frsize as u64)
}

/// Refuse to write when less than `min_gb` GiB is free under `path`.
pub fn ensure_free(path: &Path, min_gb: u64) -> Result<()> {
    let free = free_bytes(path)?;
    if free < min_gb << 30 {
        bail!(
            "only {:.1} GiB free under {} (backfill.min_free_gb = {min_gb})",
            free as f64 / (1u64 << 30) as f64,
            path.display()
        );
    }
    Ok(())
}

/// Peak resident set size of this process so far, in MiB (getrusage `ru_maxrss`: KiB on
/// Linux, bytes on macOS).
pub fn peak_rss_mb() -> u64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `ru` is a writable rusage.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
        return 0;
    }
    #[allow(clippy::unnecessary_cast)]
    let v = ru.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        v >> 20
    } else {
        v >> 10
    }
}

/// Anonymous resident memory in MiB (Linux `RssAnon` of `/proc/self/status`; `None`
/// elsewhere). File-backed pages (MDBX and segment mmaps) are left out: the kernel drops
/// those under pressure, so they are not what `mem_cap_mb` bounds.
pub fn rss_anon_mb() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_rss_anon_kb(&s).map(|kb| kb >> 10)
}

fn parse_rss_anon_kb(status: &str) -> Option<u64> {
    let l = status.lines().find(|l| l.starts_with("RssAnon:"))?;
    l.split_whitespace().nth(1)?.parse().ok()
}

/// Current resident set size in MiB (Linux `/proc/self/statm`; `None` elsewhere).
pub fn rss_mb() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = s.split_whitespace().nth(1)?.parse().ok()?;
    // SAFETY: sysconf has no preconditions.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    Some((pages * u64::try_from(page).ok()?) >> 20)
}

#[cfg(test)]
mod rss_tests {
    #[test]
    fn rss_anon_line() {
        let s = "VmRSS:\t 13000000 kB\nRssAnon:\t12582912 kB\nRssFile:\t 1000 kB\n";
        assert_eq!(super::parse_rss_anon_kb(s), Some(12_582_912));
        assert_eq!(super::parse_rss_anon_kb("VmRSS: 1 kB"), None);
    }
}
