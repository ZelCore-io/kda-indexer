//! FIX-3 part E (B1): the read budget's safety ceiling sits at least 10x above the budget of
//! every real document in the repository (the compare suite's documents), priced with no
//! variables bound, so every page variable costs the most the resolver serves.
//!
//! `KDX_BUDGET_DOCS=<dir> cargo test -p kdx-graphql --test budget_ceiling -- --nocapture`
//! prices another directory of operation documents (an explorer's) the same way and prints
//! each cost: the calibration the ceiling was set from.

use std::path::{Path, PathBuf};

use async_graphql::Variables;
use kdx_graphql::limits::{budget, BUDGET_CEILING};
use kdx_graphql::GraphqlSettings;

fn docs(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "graphql" || x == "gql"))
        .collect();
    v.sort();
    v
}

/// Each document's budget with no variables bound, largest first.
fn costs(dir: &Path) -> Vec<(String, u64)> {
    let s = GraphqlSettings::default();
    let mut out: Vec<(String, u64)> = docs(dir)
        .into_iter()
        .map(|p| {
            let text = std::fs::read_to_string(&p).unwrap();
            let doc = async_graphql::parser::parse_query(&text).unwrap();
            let c = budget(&doc, &Variables::default(), &s)
                .unwrap_or_else(|e| panic!("{}: {e:?}", p.display()))
                .complexity;
            (p.file_name().unwrap().to_string_lossy().into_owned(), c)
        })
        .collect();
    out.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    out
}

#[test]
fn the_ceiling_is_ten_times_every_real_document() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare/graphql");
    let all = costs(&dir);
    assert!(all.len() >= 20, "{all:?}");
    for (name, c) in &all {
        assert!(
            c.saturating_mul(10) <= BUDGET_CEILING,
            "{name}: budget {c}, ceiling {BUDGET_CEILING}"
        );
    }
    if let Some(d) = std::env::var_os("KDX_BUDGET_DOCS") {
        let extra = costs(Path::new(&d));
        for (name, c) in &extra {
            println!("{c}\t{name}");
        }
        for (name, c) in &extra {
            assert!(
                c.saturating_mul(10) <= BUDGET_CEILING,
                "{name}: budget {c}, ceiling {BUDGET_CEILING}"
            );
        }
    }
    for (name, c) in &all {
        println!("{c}\t{name}");
    }
}
