//! Shared property-test harness (included by path from each crate's `tests/props.rs`).
//!
//! `props!` turns each `fn name(arg in strategy, ..) { body }` into a module `name` with two
//! tests: `name::default` (256 cases, runs in `cargo test`) and `name::long` (10,000 cases,
//! ignored; run with `cargo test --workspace -- --ignored long`). The body uses
//! `prop_assert!`-style macros and may `return Err(..)`.
//!
//! Seeds are fixed (T-1): each property runs from a seed derived from its module path, so a
//! verdict is the same on every run and machine. To run other cases on demand, set
//! `PROPTEST_RNG_SEED=<u64>`; that seed then replaces the derived one for every property
//! (`PROPTEST_RNG_SEED=7 cargo test -p kdx-types --test props -- --ignored long`).
#![allow(dead_code, unused_imports, unused_macros)]

use proptest::test_runner::{Config, RngSeed, TestRunner};

pub use proptest::prelude::*;

pub const DEFAULT_CASES: u32 = 256;
pub const LONG_CASES: u32 = 10_000;

/// The seed of property `name`: `PROPTEST_RNG_SEED` when set, else FNV-1a 64 of the name.
pub fn seed_of(name: &str) -> u64 {
    if let Some(s) = std::env::var("PROPTEST_RNG_SEED")
        .ok()
        .and_then(|v| v.trim().parse().ok())
    {
        return s;
    }
    name.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

pub fn run<S: Strategy>(
    name: &str,
    cases: u32,
    strategy: S,
    f: impl Fn(S::Value) -> Result<(), TestCaseError>,
) {
    let seed = seed_of(name);
    let mut runner = TestRunner::new(Config {
        cases,
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(seed),
        ..Config::default()
    });
    if let Err(e) = runner.run(&strategy, f) {
        panic!("{name} (seed {seed}): {e}");
    }
}

macro_rules! props {
    ($( fn $name:ident($($arg:pat in $strat:expr),+ $(,)?) $body:block )*) => {
        $(
            mod $name {
                #[allow(unused_imports)]
                use super::*;
                fn check(cases: u32) {
                    $crate::prop::run(module_path!(), cases, ($($strat,)+), |($($arg,)+)| {
                        $body
                        #[allow(unreachable_code)]
                        Ok(())
                    });
                }
                #[test]
                fn default() {
                    check($crate::prop::DEFAULT_CASES)
                }
                #[test]
                #[ignore = "long mode: 10,000 cases"]
                fn long() {
                    check($crate::prop::LONG_CASES)
                }
            }
        )*
    };
}

/// `N` arbitrary bytes.
pub fn arr<const N: usize>() -> impl Strategy<Value = [u8; N]> {
    proptest::collection::vec(any::<u8>(), N).prop_map(|v| v.try_into().expect("N bytes"))
}
