//! Shared property-test harness (included by path from each crate's `tests/props.rs`).
//!
//! `props!` turns each `fn name(arg in strategy, ..) { body }` into a module `name` with two
//! tests: `name::default` (256 cases, runs in `cargo test`) and `name::long` (10,000 cases,
//! ignored; run with `cargo test --workspace -- --ignored long`). The body uses
//! `prop_assert!`-style macros and may `return Err(..)`.
#![allow(dead_code, unused_imports, unused_macros)]

use proptest::test_runner::{Config, TestRunner};

pub use proptest::prelude::*;

pub const DEFAULT_CASES: u32 = 256;
pub const LONG_CASES: u32 = 10_000;

pub fn run<S: Strategy>(
    cases: u32,
    strategy: S,
    f: impl Fn(S::Value) -> Result<(), TestCaseError>,
) {
    let mut runner = TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    });
    if let Err(e) = runner.run(&strategy, f) {
        panic!("{e}");
    }
}

macro_rules! props {
    ($( fn $name:ident($($arg:pat in $strat:expr),+ $(,)?) $body:block )*) => {
        $(
            mod $name {
                #[allow(unused_imports)]
                use super::*;
                fn check(cases: u32) {
                    $crate::prop::run(cases, ($($strat,)+), |($($arg,)+)| {
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
