//! Circulating supply of KDA, the formula and tables of chainweb-data's `Chainweb.Coins`
//! (BSD-3, see `THIRD_PARTY_NOTICES`): mining rewards by block height plus the token
//! allocations released by date. Money is exact decimal arithmetic (fixed point, 12 digits)
//! and becomes a double at the very end.
//!
//! The allocation file is read the way chainweb-data reads it: its first line is taken as
//! a header and skipped, although it holds a row. The reproduced total depends on it.

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

const MINER_REWARDS: &str = include_str!("../data/miner_rewards.csv");
const TOKEN_PAYMENTS: &str = include_str!("../data/token_payments.csv");

/// Fixed-point scale: 10^12.
const SCALE_DIGITS: usize = 12;
const SCALE: i128 = 1_000_000_000_000;

/// A non-negative decimal literal (`23.04523`, `10000000`) in fixed point.
fn dec(s: &str) -> Option<i128> {
    let s = s.trim();
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    if int.is_empty() && frac.is_empty() || frac.len() > SCALE_DIGITS {
        return None;
    }
    if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
        return None;
    }
    let i: i128 = if int.is_empty() { 0 } else { int.parse().ok()? };
    let mut f: i128 = if frac.is_empty() {
        0
    } else {
        frac.parse().ok()?
    };
    for _ in frac.len()..SCALE_DIGITS {
        f *= 10;
    }
    Some(i * SCALE + f)
}

/// `%FT%TZ` as Unix seconds.
fn utc_time(s: &str) -> Option<i64> {
    let b = s.trim().as_bytes();
    if b.len() != 20 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[19] != b'Z' {
        return None;
    }
    let n = |r: std::ops::Range<usize>| -> Option<i64> {
        std::str::from_utf8(&b[r]).ok()?.parse().ok()
    };
    let (y, m, d) = (n(0..4)?, n(5..7)?, n(8..10)?);
    let (hh, mm, ss) = (n(11..13)?, n(14..16)?, n(17..19)?);
    // days from civil (Howard Hinnant)
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + hh * 3600 + mm * 60 + ss)
}

struct Tables {
    /// `(from height, total mined before it, reward per height from it)`, ascending.
    rewards: Vec<(u64, i128, i128)>,
    /// `(release time, cumulative allocation from it)`, ascending.
    allocations: Vec<(i64, i128)>,
}

fn tables() -> &'static Tables {
    static T: OnceLock<Tables> = OnceLock::new();
    T.get_or_init(|| {
        // minerRewardMap: height thresholds to rewards (a later row of a height wins).
        let mut map: BTreeMap<u64, i128> = BTreeMap::new();
        for line in MINER_REWARDS.lines().filter(|l| !l.trim().is_empty()) {
            let (h, r) = line.split_once(',').expect("miner_rewards.csv row");
            map.insert(
                h.trim().parse().expect("miner_rewards.csv height"),
                dec(r).expect("miner_rewards.csv reward"),
            );
        }
        // cumulativeRewards: reward `r` applies from the previous threshold up to `h`.
        let mut rewards = Vec::with_capacity(map.len() + 1);
        let (mut last, mut total) = (0u64, 0i128);
        for (h, r) in map {
            rewards.push((last, total, r));
            total += i128::from(h - last) * r;
            last = h;
        }
        rewards.push((last, total, 0));
        let mut m: BTreeMap<u64, (i128, i128)> = BTreeMap::new();
        for (from, t, r) in rewards {
            m.insert(from, (t, r));
        }
        let rewards = m.into_iter().map(|(h, (t, r))| (h, t, r)).collect();

        // cumulativeAllocations: consecutive rows of one time form a section.
        let mut rows: Vec<(i64, i128)> = Vec::new();
        for line in TOKEN_PAYMENTS
            .lines()
            .skip(1)
            .filter(|l| !l.trim().is_empty())
        {
            let f: Vec<&str> = line.split(',').collect();
            assert!(f.len() == 5, "token_payments.csv row");
            rows.push((
                utc_time(f[1]).expect("token_payments.csv time"),
                dec(f[3]).expect("token_payments.csv amount"),
            ));
        }
        let mut cum: BTreeMap<i64, i128> = BTreeMap::new();
        let mut total = 0i128;
        let mut i = 0;
        while i < rows.len() {
            let t = rows[i].0;
            while i < rows.len() && rows[i].0 == t {
                total += rows[i].1;
                i += 1;
            }
            cum.insert(t, total);
        }
        Tables {
            rewards,
            allocations: cum.into_iter().collect(),
        }
    })
}

/// Coins mined up to `height` (`getTotalMiningRewards`), fixed point.
fn mined(height: u64) -> i128 {
    let t = &tables().rewards;
    let i = t.partition_point(|e| e.0 <= height).saturating_sub(1);
    let (last, total, reward) = t[i];
    total + i128::from(height - last) * reward
}

/// Allocations released at `unix` seconds (`getTotalAllocations`), fixed point.
fn allocated(unix: i64) -> i128 {
    let t = &tables().allocations;
    let i = t.partition_point(|e| e.0 <= unix);
    if i == 0 {
        0
    } else {
        t[i - 1].1
    }
}

/// Circulating supply at block `height` and time `now` (`getCirculatingCoins`).
pub fn circulating(height: u64, now: SystemTime) -> f64 {
    let unix = match now.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(_) => 0,
    };
    let v = mined(height) + allocated(unix);
    // Exact decimal text, so the double is correctly rounded.
    format!("{}.{:012}", v / SCALE, v % SCALE)
        .parse()
        .unwrap_or(f64::NAN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsing() {
        assert_eq!(dec("23.04523"), Some(23_045_230_000_000));
        assert_eq!(dec("10000000"), Some(10_000_000 * SCALE));
        assert_eq!(dec("x"), None);
        assert_eq!(utc_time("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(utc_time("2019-12-01T00:00:00Z"), Some(1_575_158_400));
    }

    #[test]
    fn tables_load() {
        // Height 0 mined nothing; before the first release nothing is allocated.
        assert_eq!(mined(0), 0);
        assert_eq!(allocated(0), 0);
        assert!(circulating(7_000_000, SystemTime::now()) > 3.0e8);
    }
}
