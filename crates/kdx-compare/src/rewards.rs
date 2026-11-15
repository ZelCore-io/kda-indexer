//! Coinbase rewards by block height, from the miner reward table that chainweb-data's supply
//! also reads (see THIRD_PARTY_NOTICES). The harness uses them for tip gaps (R-P5P9-48):
//! `/coins` against a chainweb-data that lags, and a balance read a few heights apart from the
//! node's `/local`.
//!
//! The table is packaged with the harness (R-P5P9-49 concern 5): `data/miner_rewards.csv` in
//! this crate, a copy of the served `crates/kdx-api/data/miner_rewards.csv` that a test keeps
//! equal. It is compiled in, so the installed binary reads no file.

use std::collections::BTreeMap;
use std::sync::OnceLock;

const MINER_REWARDS: &str = include_str!("../data/miner_rewards.csv");

/// Fixed point: units per KDA (coin's 12 decimal places).
pub const SCALE: i128 = 1_000_000_000_000;

/// A non-negative decimal literal in fixed point; `None` past 12 places.
pub fn fixed(s: &str) -> Option<i128> {
    let s = s.trim();
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    if (int.is_empty() && frac.is_empty())
        || frac.len() > 12
        || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let i: i128 = if int.is_empty() { 0 } else { int.parse().ok()? };
    let f: i128 = if frac.is_empty() {
        0
    } else {
        frac.parse().ok()?
    };
    Some(i.checked_mul(SCALE)? + f * 10i128.pow(12 - frac.len() as u32))
}

/// Threshold height -> reward per height of all chains together, in fixed point. A reward
/// applies to the heights up to and including its threshold (chainweb's `lookupGE`; a later
/// row of the same height wins).
fn table() -> &'static BTreeMap<u64, i128> {
    static T: OnceLock<BTreeMap<u64, i128>> = OnceLock::new();
    T.get_or_init(|| {
        MINER_REWARDS
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let (h, r) = l.split_once(',').expect("miner_rewards.csv row");
                (
                    h.trim().parse().expect("miner_rewards.csv height"),
                    fixed(r).expect("miner_rewards.csv reward"),
                )
            })
            .collect()
    })
}

/// Reward per height of all chains together at `h`.
pub fn height_reward(h: u64) -> Option<i128> {
    table().range(h..).next().map(|(_, r)| *r)
}

/// The coinbase of one block at `h` on a graph of `chains` chains: the height's reward over
/// the chain count, rounded half-even to 8 places (chainweb's `minerReward`).
pub fn block_reward(h: u64, chains: usize) -> Option<i128> {
    let m = height_reward(h)?;
    let n = i128::try_from(chains).ok().filter(|n| *n > 0)?;
    let q = 10_000 * n;
    let (d, r) = (m / q, m % q);
    let up = 2 * r > q || (2 * r == q && d % 2 == 1);
    Some((d + i128::from(up)) * 10_000)
}

/// The coinbase minted at heights `lo+1..=hi` on all chains together, in KDA.
pub fn minted_between(lo: u64, hi: u64) -> f64 {
    let mut total: i128 = 0;
    let mut from = lo.saturating_add(1);
    while from <= hi {
        let Some((&t, &r)) = table().range(from..).next() else {
            break;
        };
        let to = t.min(hi);
        total += i128::from(to - from + 1) * r;
        from = to + 1;
    }
    total as f64 / SCALE as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// R-P5P9-49 concern 5 (FV T-3): the table is packaged with the harness. The embedded
    /// file lives in this crate (`data/miner_rewards.csv`), not by a path into another crate,
    /// and it is byte for byte the table the served `/coins` supply reads.
    #[test]
    fn the_reward_table_is_packaged_in_the_harness_crate() {
        let own = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("data/miner_rewards.csv");
        let packaged = std::fs::read_to_string(&own)
            .unwrap_or_else(|e| panic!("{} is not in the package: {e}", own.display()));
        assert_eq!(
            MINER_REWARDS, packaged,
            "the embedded table is the packaged file"
        );
        let src = include_str!("rewards.rs");
        let include = src
            .lines()
            .find(|l| l.starts_with("const MINER_REWARDS"))
            .expect("the include line");
        assert!(
            include.contains("include_str!(\"../data/miner_rewards.csv\")"),
            "the table is embedded from the crate's own data dir: {include}"
        );
        let served = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../kdx-api/data/miner_rewards.csv");
        let served = std::fs::read_to_string(&served)
            .unwrap_or_else(|e| panic!("reading the served table {}: {e}", served.display()));
        assert_eq!(
            packaged, served,
            "the harness copy drifted from the served table"
        );
    }

    #[test]
    fn rewards_at_the_run_6e_heights() {
        // 7358401..=7446000: 18.08092 KDA per height, 0.904046 per block on 20 chains.
        assert_eq!(height_reward(7_400_412), fixed("18.08092"));
        assert_eq!(block_reward(7_400_412, 20), fixed("0.904046"));
        assert_eq!(height_reward(7_358_400), fixed("18.13322"));
        let m = minted_between(7_400_410, 7_400_412);
        assert!((m - 2.0 * 18.08092).abs() < 1e-9, "{m}");
        // Across a threshold: one height at 18.13322, one at 18.08092.
        let m = minted_between(7_358_399, 7_358_401);
        assert!((m - (18.13322 + 18.08092)).abs() < 1e-9, "{m}");
        assert_eq!(minted_between(5, 5), 0.0);
        assert_eq!(fixed("1.5"), Some(SCALE + SCALE / 2));
        assert_eq!(fixed("1.0000000000001"), None);
        assert_eq!(fixed("-1"), None);
    }
}
