//! `rebuild-range` through the CLI (Task 16 Step 2): the process is killed (SIGKILL) at each
//! phase of the swap; every time the store still opens, passes `segments check --deep` and
//! holds the same content. A clean run afterwards recovers and swaps. Each run compresses at
//! a zstd level other than the stored files' (I-2: alternating 1 and 5, the freeze uses 3),
//! so every rebuilt file differs from the one it replaces and a mixed old/new set fails the
//! per-file blake3 of `range.toml` in `segments check --deep`. The node is the local stand-in
//! of the shared harness.

#[path = "../../kdx-ingest/tests/common/mod.rs"]
mod common;

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use common::*;

/// The packaged example config pointed at the stand-in node, step files at zstd `level`.
fn config(dir: &Path, base: &str, level: i32) -> PathBuf {
    let src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../packaging/config/mainnet01.toml"
    ))
    .unwrap();
    let p = dir.join(format!("config-{level}.toml"));
    let cfg = src.replace("http://127.0.0.1:31351", base);
    std::fs::write(&p, format!("{cfg}\n[freezer]\nzstd_level = {level}\n")).unwrap();
    p
}

fn rebuild(cfg: &Path, data: &Path, start: u32, sleep_at: Option<&str>) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_kda-indexer"));
    c.arg("--config")
        .arg(cfg)
        .args(["rebuild-range", "--header-mode", "cut", "--step"])
        .arg(start.to_string())
        .arg("--data-dir")
        .arg(data)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(p) = sleep_at {
        c.env("KDX_DEBUG_SLEEP_AT", format!("{p}:120"));
    }
    c
}

/// Content digest of every segment entry (the hot dictionary opened read-only).
fn segment_content(data: &Path) -> String {
    let hs = kdx_hot::HotStore::open_ro(&data.join("hot")).unwrap();
    let dict = kdx_hot::HotDict::load(&hs.ro().unwrap()).unwrap();
    let seg = data.join("segments");
    let m = kdx_segment::Manifest::load(&seg).unwrap().unwrap();
    let mut d = kdx_segment::content::ContentDigest::new();
    for e in &m.entries {
        let r = kdx_segment::RangeReader::open(&seg, e, &m.network).unwrap();
        kdx_segment::content::feed(&r, &dict, &mut d).unwrap();
    }
    d.finish().blake3
}

fn check_deep(data: &Path, what: &str) {
    let rep = kdx_segment::check(&data.join("segments"), true).unwrap();
    assert!(rep.ok(), "{what}: {rep:?}");
}

#[test]
fn rebuild_range_killed_at_each_phase_leaves_a_checkable_store() {
    let pool = pool();
    let places = places_for(&[0, 1], 12);
    let base = fake_node(&pool, &places);
    let d = tempfile::tempdir().unwrap();
    let data = d.path().join("data");
    let start = {
        let c = freeze_store(&data, &pool, &base, &places, &[1, 0]);
        kdx_segment::Manifest::load(&c.segdir)
            .unwrap()
            .unwrap()
            .entries[2]
            .start
    };
    let before = segment_content(&data);
    let cfgs = [config(d.path(), &base, 1), config(d.path(), &base, 5)];
    for (i, point) in [
        "rebuild-after-build",
        "rebuild-before-rename",
        "rebuild-after-rename",
        "rebuild-mid-swap",
        "rebuild-before-cleanup",
    ]
    .into_iter()
    .enumerate()
    {
        let mut child = rebuild(&cfgs[i % 2], &data, start, Some(point))
            .spawn()
            .unwrap();
        let err = BufReader::new(child.stderr.take().unwrap());
        let mut log = Vec::new();
        let mut reached = false;
        for line in err.lines() {
            let line = line.unwrap();
            reached = line.contains("KDX_DEBUG_SLEEP_AT=");
            log.push(line);
            if reached {
                break;
            }
        }
        child.kill().unwrap(); // SIGKILL
        child.wait().unwrap();
        assert!(reached, "{point} never reached: {log:#?}");
        check_deep(&data, point);
        assert_eq!(segment_content(&data), before, "{point}: content");
    }
    // The last kill left a finished swap at level 1 behind its journal; level 5 differs.
    let out = rebuild(&cfgs[1], &data, start, None).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rep: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(rep["swapped"], true, "{rep}");
    assert_eq!(rep["content_equal"], true, "{rep}");
    assert_eq!(
        rep["bytes_equal"], false,
        "the rebuild must not be byte-equal: {rep}"
    );
    let (old, new) = (&rep["old_blake3"], &rep["new_blake3"]);
    // zstd-compressed files and `range.toml` differ (posting and index files are not
    // compressed and may stay equal: a mix of equal bytes cannot be wrong).
    let mut differ = 0;
    for (n, h) in new.as_object().unwrap() {
        if n.ends_with(".seg") || n.ends_with(".toml") {
            assert_ne!(&old[n], h, "{n}: rebuilt bytes equal the stored ones");
            differ += 1;
        }
    }
    assert!(differ >= 3, "{new}");
    check_deep(&data, "after the clean run");
    assert_eq!(segment_content(&data), before);
    let seg = data.join("segments");
    let left: Vec<_> = std::fs::read_dir(&seg)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("rebuild.") || n.starts_with(".swap-"))
        .collect();
    // m8: the recovery of the last kill finished that swap a second time.
    assert!(left.is_empty(), "{left:?}");
}
