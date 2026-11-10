//! Periodic gauge sampling for `/metrics`: per-chain tips and lag, the frozen frontier, the
//! dictionary snapshot size (every 10 s) and storage sizes (every 60 s). Runs in `serve` and in
//! `run`. Label values are chain numbers and segment file kinds only.

use std::ffi::CString;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use kdx_segment::files::FileKind;
use tokio_util::sync::CancellationToken;

use crate::server::ApiState;
use crate::status::Status;

const TIPS_EVERY: Duration = Duration::from_secs(10);
const STORAGE_EVERY: u32 = 6;

pub async fn run(st: Arc<ApiState>, shutdown: CancellationToken) {
    let mut n = 0u32;
    loop {
        sample_tips(&st).await;
        if n.is_multiple_of(STORAGE_EVERY) {
            let segdir = st.store.segdir.clone();
            let _ = tokio::task::spawn_blocking(move || sample_storage(&segdir)).await;
        }
        n = n.wrapping_add(1);
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(TIPS_EVERY) => {}
        }
    }
}

async fn sample_tips(st: &Arc<ApiState>) {
    let m = kdx_metrics::get();
    m.dict_snapshot_names.set(i64::try_from(st.store.dict_names()).unwrap_or(i64::MAX));
    match Status::load(st).await {
        Ok(s) => {
            m.frozen_frontier_height.set(i64::from(s.seg_frontier));
            for c in &s.chains {
                m.chain(c.chain, c.node_height, u64::from(c.height), c.halted);
            }
        }
        Err(e) => tracing::debug!(error = %e, "metrics: tip sampling failed"),
    }
}

/// Sizes of `mdbx.dat`, per segment file kind and the free bytes of the data volume.
pub fn sample_storage(segdir: &Path) {
    let m = kdx_metrics::get();
    let data_dir = segdir.parent().unwrap_or(segdir);
    if let Ok(md) = std::fs::metadata(data_dir.join("hot").join("mdbx.dat")) {
        m.hot_store_bytes.set(i64::try_from(md.len()).unwrap_or(i64::MAX));
    }
    let mut by_kind = [0u64; FileKind::ALL.len() + 1];
    if let Ok(rd) = std::fs::read_dir(segdir) {
        for e in rd.flatten() {
            let name = e.file_name();
            let Some(name) = name.to_str() else { continue };
            let Ok(md) = e.metadata() else { continue };
            if !md.is_file() {
                continue;
            }
            let kind = name
                .split('-')
                .nth(1)
                .and_then(|k| FileKind::ALL.iter().position(|f| f.name() == k))
                .unwrap_or(FileKind::ALL.len());
            by_kind[kind] += md.len();
        }
    }
    for (i, b) in by_kind.iter().enumerate() {
        let name = FileKind::ALL.get(i).map_or("other", |k| k.name());
        m.segment_kind_bytes(name, *b);
    }
    if let Some(free) = free_bytes(data_dir) {
        m.disk_free_bytes.set(i64::try_from(free).unwrap_or(i64::MAX));
    }
}

fn free_bytes(path: &Path) -> Option<u64> {
    let c = CString::new(path.as_os_str().as_encoded_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path and `st` a writable statvfs.
    if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
        return None;
    }
    #[allow(clippy::unnecessary_cast)]
    Some(st.f_bavail as u64 * st.f_frsize as u64)
}
