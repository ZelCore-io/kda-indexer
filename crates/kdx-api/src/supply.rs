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
    /// `(from height, total mined before it, reward per height from it, the total's places)`,
    /// ascending. Places are Data.Decimal's digits after the point (see `circulating_text`).
    rewards: Vec<(u64, i128, i128, u8)>,
    /// `(release time, cumulative allocation from it, its places)`, ascending.
    allocations: Vec<(i64, i128, u8)>,
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
        let (mut last, mut total, mut places) = (0u64, 0i128, 0u8);
        for (h, r) in map {
            rewards.push((last, total, r, places));
            let p = i128::from(h - last) * r;
            total += p;
            places = places.max(norm_places(p));
            last = h;
        }
        rewards.push((last, total, 0, places));
        let mut m: BTreeMap<u64, (i128, i128, u8)> = BTreeMap::new();
        for (from, t, r, p) in rewards {
            m.insert(from, (t, r, p));
        }
        let rewards = m.into_iter().map(|(h, (t, r, p))| (h, t, r, p)).collect();

        // cumulativeAllocations: consecutive rows of one time form a section.
        let mut rows: Vec<(i64, i128, u8)> = Vec::new();
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
                written_places(f[3]),
            ));
        }
        let mut cum: BTreeMap<i64, (i128, u8)> = BTreeMap::new();
        let (mut total, mut places) = (0i128, 0u8);
        let mut i = 0;
        while i < rows.len() {
            let t = rows[i].0;
            while i < rows.len() && rows[i].0 == t {
                total += rows[i].1;
                places = places.max(rows[i].2);
                i += 1;
            }
            cum.insert(t, (total, places));
        }
        Tables {
            rewards,
            allocations: cum.into_iter().map(|(t, (v, p))| (t, v, p)).collect(),
        }
    })
}

/// Coins mined up to `height` (`getTotalMiningRewards`), fixed point, with its places.
fn mined_places(height: u64) -> (i128, u8) {
    let t = &tables().rewards;
    let i = t.partition_point(|e| e.0 <= height).saturating_sub(1);
    let (last, total, reward, places) = t[i];
    let p = i128::from(height - last) * reward;
    (total + p, places.max(norm_places(p)))
}

fn mined(height: u64) -> i128 {
    mined_places(height).0
}

/// Allocations released at `unix` seconds (`getTotalAllocations`), fixed point, with its
/// places.
fn allocated_places(unix: i64) -> (i128, u8) {
    let t = &tables().allocations;
    let i = t.partition_point(|e| e.0 <= unix);
    if i == 0 {
        (0, 0)
    } else {
        (t[i - 1].1, t[i - 1].2)
    }
}

fn allocated(unix: i64) -> i128 {
    allocated_places(unix).0
}

/// Digits after the point of a CSV literal (`read` of a Data.Decimal keeps them).
fn written_places(s: &str) -> u8 {
    s.trim().split_once('.').map_or(0, |(_, f)| f.len() as u8)
}

/// Places of a normalized product (Data.Decimal's `*` drops trailing zeros).
fn norm_places(mut v: i128) -> u8 {
    if v == 0 {
        return 0;
    }
    let mut p = SCALE_DIGITS as u8;
    while p > 0 && v % 10 == 0 {
        v /= 10;
        p -= 1;
    }
    p
}

fn unix_secs(now: SystemTime) -> i64 {
    match now.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(_) => 0,
    }
}

/// chainweb-data's `/coins` text: `show` of the Haskell `Decimal` that `getCirculatingCoins`
/// returns (ruling C-1). Data.Decimal prints exactly its number of places: a sum keeps the
/// larger count of its terms, a product is normalized (trailing zeros dropped), and a CSV
/// literal keeps the places it was written with.
pub fn circulating_text(height: u64, now: SystemTime) -> String {
    let (m, mp) = mined_places(height);
    let (a, ap) = allocated_places(unix_secs(now));
    let v = m + a;
    let p = mp.max(ap).min(SCALE_DIGITS as u8) as usize;
    let int = v / SCALE;
    if p == 0 {
        return int.to_string();
    }
    let frac = (v % SCALE) / 10i128.pow((SCALE_DIGITS - p) as u32);
    format!("{int}.{frac:0p$}")
}

/// Circulating supply at block `height` and time `now` (`getCirculatingCoins`).
pub fn circulating(height: u64, now: SystemTime) -> f64 {
    let v = mined(height) + allocated(unix_secs(now));
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

    /// chainweb-data's `/coins` answered `379699932.889` and, a minute later,
    /// `379699987.13176` on 2026-11-11 (live): heights 7,396,275 and 7,396,278 (18.08092 per
    /// height). The first shows the places rule (3, not the 5 of its neighbours).
    #[test]
    fn coins_text_matches_chainweb_data() {
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1794431615);
        assert_eq!(circulating_text(7_396_275, t), "379699932.889");
        assert_eq!(circulating_text(7_396_278, t), "379699987.13176");
        assert_eq!(norm_places(1_500_000_000_000), 1);
        assert_eq!(norm_places(0), 0);
        assert_eq!(written_places("16000.50"), 2);
    }
}
