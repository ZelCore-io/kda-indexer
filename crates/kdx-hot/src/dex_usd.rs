//! `DexUsd`: the KDA/USD rate per hour, for the USD fields of the Sushi DEX surface (stock-gap
//! plan B3). Key: hour u32 big-endian (unix seconds / 3,600). Value: the rate as canonical
//! decimal text (`Amount::to_text`), at most [`MAX_RATE_TEXT`] bytes.
//!
//! The rows come from the DIA feed (`kdx_ingest::dia`), one per hour in which a poll passed
//! validation (the last good value of that hour). Ingest never writes this table and unwind
//! never touches it: a rate is off-chain and does not depend on the chain's branch.
//!
//! History: DIA keeps no history for KDA (`assetChartPoints` answers an empty series), so
//! hours before the feed started have no row. A reader takes the newest row at or before the
//! hour; for an hour before the first row it takes the first row (the nearest later value).
//! Stock priced its whole history at the rate of its own ingest/backfill moment, which is
//! also a later value, so clients see the same kind of number.

use anyhow::Result;

use crate::dex::Amount;
use crate::env::{RwTxn, Txn};
use crate::tables::Table;

/// Largest stored rate text read back (anything longer is ignored as damaged).
pub const MAX_RATE_TEXT: usize = 64;

pub fn key(hour: u32) -> [u8; 4] {
    hour.to_be_bytes()
}

/// Store the rate of `hour` (replaces the hour's earlier value).
pub fn put(txn: &mut RwTxn<'_>, hour: u32, rate: &Amount) -> Result<()> {
    txn.put(Table::DexUsd, &key(hour), rate.to_text().as_bytes())
}

fn decode(k: &[u8], v: &[u8]) -> Option<(u32, Amount)> {
    let hour = u32::from_be_bytes(k.try_into().ok()?);
    if v.len() > MAX_RATE_TEXT {
        return None;
    }
    let a = Amount::parse_stored(std::str::from_utf8(v).ok()?)?;
    (!a.is_zero() && !a.is_negative()).then_some((hour, a))
}

/// The newest stored rate (hour, rate), if any.
pub fn latest(txn: &impl Txn) -> Result<Option<(u32, Amount)>> {
    if !txn.has_table(Table::DexUsd) {
        return Ok(None);
    }
    for row in txn.scan_rev_prefix(Table::DexUsd, &[], &[])?.take(16) {
        let (k, v) = row?;
        if let Some(r) = decode(&k, &v) {
            return Ok(Some(r));
        }
    }
    Ok(None)
}

/// The rates a reader of hours `from..=to` needs: every row in the range, the newest row
/// before it and the first row after it (so [`Rates::at`] can answer any hour of the range).
/// `cont` is called per row; reading stops (with what it has) when it answers false.
pub fn load(txn: &impl Txn, from: u32, to: u32, cont: &mut dyn FnMut() -> bool) -> Result<Rates> {
    let mut rows = Vec::new();
    if !txn.has_table(Table::DexUsd) {
        return Ok(Rates { rows });
    }
    for row in txn
        .scan_rev_prefix(Table::DexUsd, &[], &key(from))?
        .take(16)
    {
        let (k, v) = row?;
        if let Some(r) = decode(&k, &v) {
            rows.push(r);
            break;
        }
    }
    let mut past = 0usize;
    let mut stop = false;
    let got = txn.scan(Table::DexUsd, &key(from), |k| {
        if stop || !cont() {
            return false;
        }
        if k.len() == 4 && u32::from_be_bytes([k[0], k[1], k[2], k[3]]) > to {
            // Keep one row past the range (the nearest later value), then stop.
            past += 1;
            if past > 1 {
                stop = true;
                return false;
            }
        }
        true
    })?;
    rows.extend(got.iter().filter_map(|(k, v)| decode(k, v)));
    rows.sort_by_key(|r| r.0);
    rows.dedup_by_key(|r| r.0);
    Ok(Rates { rows })
}

/// Hourly rates, ascending by hour.
#[derive(Debug, Clone, Default)]
pub struct Rates {
    pub rows: Vec<(u32, Amount)>,
}

impl Rates {
    /// The rate of `hour`: the newest row at or before it, else the first row after it
    /// (module doc), else `None` (no rate was ever stored: USD values answer 0, as stock does
    /// when it has no KDA/USD price).
    pub fn at(&self, hour: u32) -> Option<&Amount> {
        let i = self.rows.partition_point(|r| r.0 <= hour);
        if i > 0 {
            Some(&self.rows[i - 1].1)
        } else {
            self.rows.first().map(|r| &r.1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::{Geometry, HotStore};

    #[test]
    fn floor_then_nearest_later_and_bounded_load() {
        let d = tempfile::tempdir().unwrap();
        let hot = HotStore::open(
            &d.path().join("hot"),
            Geometry {
                max_gb: 1,
                growth_mb: 16,
            },
        )
        .unwrap();
        {
            let mut w = hot.rw().unwrap();
            for (h, r) in [(100, "0.5"), (110, "0.6"), (120, "0.7"), (130, "0.8")] {
                put(&mut w, h, &Amount::parse(r).unwrap()).unwrap();
            }
            // A damaged row is skipped.
            w.put(Table::DexUsd, &key(125), b"-1").unwrap();
            w.commit().unwrap();
        }
        let ro = hot.ro().unwrap();
        let all = load(&ro, 0, u32::MAX, &mut || true).unwrap();
        assert_eq!(
            all.at(50).unwrap().to_text(),
            "0.5",
            "before the first: first"
        );
        assert_eq!(all.at(100).unwrap().to_text(), "0.5");
        assert_eq!(all.at(119).unwrap().to_text(), "0.6");
        assert_eq!(all.at(126).unwrap().to_text(), "0.7", "damaged row ignored");
        assert_eq!(all.at(9999).unwrap().to_text(), "0.8");
        let some = load(&ro, 111, 112, &mut || true).unwrap();
        let hours: Vec<u32> = some.rows.iter().map(|r| r.0).collect();
        assert_eq!(
            hours,
            vec![110, 120],
            "floor row and one row past the range"
        );
        assert_eq!(latest(&ro).unwrap().unwrap().0, 130);
        assert!(Rates::default().at(5).is_none());
    }
}
