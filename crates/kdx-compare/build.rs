//! Embeds the short git sha (`KDX_GIT_SHA`) that the harness logs at start and its summary
//! prints (R-P5P9-88 (3a)); "unknown" outside a checkout. R-P5P9-95 m-13: a tree with
//! changes against `HEAD` adds `-dirty`, and the git paths are asked of git, so a worktree
//! (whose `.git` is a file) reruns this too ([`build_git`], tested with the crate).

#[path = "build_git.rs"]
mod build_git;

use std::path::Path;

fn main() {
    let here = Path::new(".");
    let sha = std::env::var("KDX_GIT_SHA")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| build_git::sha(here));
    println!(
        "cargo:rustc-env=KDX_GIT_SHA={}",
        sha.as_deref().unwrap_or("unknown")
    );
    println!("cargo:rerun-if-env-changed=KDX_GIT_SHA");
    for path in build_git::watched(here) {
        println!("cargo:rerun-if-changed={path}");
    }
    // A change to a tracked file under the workspace's sources makes the tree dirty.
    for p in ["../../crates", "../../compare", "../../Cargo.lock"] {
        println!("cargo:rerun-if-changed={p}");
    }
}
