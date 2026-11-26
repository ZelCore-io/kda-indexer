//! Status of the one-off fills `kda-indexer run` starts (the DEX history, the swap prices, the
//! token chain list): `Meta fill_failed/<name>` holds the last error (at most
//! [`MAX_ERROR_LEN`] bytes) while a fill is failing and retrying, and is removed when it
//! succeeds. Readers answer a distinct "fill failed" message while it is set, instead of the
//! "being filled" one, so a fill stuck on a deterministic error is visible from outside.

use anyhow::Result;

use crate::env::{RwTxn, Txn};
use crate::tables::Table;

/// Meta key prefix of a failing fill's last error.
pub const FAILED_PREFIX: &str = "fill_failed/";
/// The Sushi DEX history fill (`kdx_ingest::dex_fill`).
pub const DEX: &str = "dex";
/// The swap price fill (`kdx_ingest::price_fill`).
pub const PRICES: &str = "prices";
/// The token chain list build (`token_chains::fill`).
pub const TOKEN_CHAINS: &str = "token_chains";
/// Longest stored error text.
pub const MAX_ERROR_LEN: usize = 512;

fn key(name: &str) -> String {
    format!("{FAILED_PREFIX}{name}")
}

/// Record that `name` failed with `error` (cut at a char boundary below [`MAX_ERROR_LEN`]).
pub fn set_failed(txn: &mut RwTxn<'_>, name: &str, error: &str) -> Result<()> {
    let mut end = error.len().min(MAX_ERROR_LEN);
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    txn.set_meta(&key(name), &error.as_bytes()[..end])
}

/// `name` succeeded (or is not failing).
pub fn clear_failed(txn: &mut RwTxn<'_>, name: &str) -> Result<()> {
    txn.del(Table::Meta, key(name).as_bytes())?;
    Ok(())
}

/// The last error of a failing fill.
pub fn failed(txn: &impl Txn, name: &str) -> Result<Option<String>> {
    Ok(txn
        .meta(&key(name))?
        .map(|v| String::from_utf8_lossy(&v).into_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Geometry, HotStore};

    #[test]
    fn set_cut_and_clear() {
        let dir = tempfile::tempdir().unwrap();
        let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        assert_eq!(failed(&txn, DEX).unwrap(), None);
        let long = "é".repeat(MAX_ERROR_LEN);
        set_failed(&mut txn, DEX, &long).unwrap();
        let got = failed(&txn, DEX).unwrap().unwrap();
        assert!(got.len() <= MAX_ERROR_LEN && got.chars().all(|c| c == 'é'));
        assert_eq!(failed(&txn, PRICES).unwrap(), None);
        clear_failed(&mut txn, DEX).unwrap();
        assert_eq!(failed(&txn, DEX).unwrap(), None);
    }
}
