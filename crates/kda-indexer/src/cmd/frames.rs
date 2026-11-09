//! `--records-out` file format shared by `dump`, `measure` and `rk-bench`.
//!
//! A file is a sequence of frames `(kind u8, len u32 little-endian, record bytes)` in TxKey
//! order (height, chain, position). Each block contributes one block frame followed by its
//! transaction frames (coinbase first). The `MemDict` that the record ids refer to is stored
//! next to it as `<stem>.dict.json`.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use kdx_types::MemDict;

/// `BlockRecord`.
pub const KIND_BLOCK: u8 = 0;
/// Coinbase `TxRecord` (position 0).
pub const KIND_COINBASE: u8 = 1;
/// User `TxRecord` whose `cmd` hashes to its `hash`.
pub const KIND_USER: u8 = 2;
/// User `TxRecord` whose served `cmd` does not hash to its `hash` (legacy re-encoded cmd).
pub const KIND_USER_LEGACY: u8 = 3;

pub fn put_frame<W: Write>(w: &mut W, kind: u8, rec: &[u8]) -> std::io::Result<()> {
    w.write_all(&[kind])?;
    w.write_all(&(rec.len() as u32).to_le_bytes())?;
    w.write_all(rec)
}

/// All frames of a file as `(kind, record bytes)`.
pub fn read_frames(path: &Path) -> anyhow::Result<Vec<(u8, Vec<u8>)>> {
    let mut data = Vec::new();
    std::fs::File::open(path)
        .with_context(|| format!("opening {}", path.display()))?
        .read_to_end(&mut data)?;
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < data.len() {
        if i + 5 > data.len() {
            bail!("{}: truncated frame header at byte {i}", path.display());
        }
        let kind = data[i];
        let len = u32::from_le_bytes(data[i + 1..i + 5].try_into().unwrap()) as usize;
        i += 5;
        if i + len > data.len() {
            bail!("{}: truncated frame body at byte {i}", path.display());
        }
        out.push((kind, data[i..i + len].to_vec()));
        i += len;
    }
    Ok(out)
}

/// `records-recent.bin` -> `records-recent.dict.json`.
pub fn dict_path(records: &Path) -> PathBuf {
    records.with_extension("dict.json")
}

pub fn load_dict(records: &Path) -> anyhow::Result<MemDict> {
    let p = dict_path(records);
    let b = std::fs::read(&p).with_context(|| format!("reading {}", p.display()))?;
    Ok(serde_json::from_slice(&b)?)
}
