#![no_main]
//! Files that come from another host: the segments `manifest.json`, `range.toml` and the
//! search `manifest.json`. (`SNAPSHOT.json` is written only; nothing parses it.)

use kdx_search::SearchManifest;
use kdx_segment::writer::RangeInfo;
use kdx_segment::Manifest;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(m) = Manifest::parse(data) {
        let _ = m.files();
        let _ = m.validate();
        for t in [0, 1, u64::MAX] {
            let _ = m.entry_of_txnum(t);
        }
        for h in [0, 1, u32::MAX] {
            let _ = m.entry_of_height(h);
        }
    }
    let dir = std::env::temp_dir().join(format!("kdx-fuzz-{}-manifest", std::process::id()));
    if std::fs::create_dir_all(&dir).is_err() {
        return;
    }
    if std::fs::write(dir.join("range.toml"), data).is_ok() {
        let _ = RangeInfo::load(&dir.join("range.toml"));
    }
    if std::fs::write(dir.join("manifest.json"), data).is_ok() {
        if let Ok(Some(m)) = SearchManifest::load(&dir) {
            let _ = m.mode();
        }
    }
});
