//! Embeds the short git sha (`KDX_GIT_SHA`) that the harness logs at start and its summary
//! prints (R-P5P9-88 (3a)); "unknown" outside a checkout. R-P5P9-95 m-13: a tree with
//! changes against `HEAD` adds `-dirty`, and the git paths are asked of git, so a worktree
//! (whose `.git` is a file) reruns this too.

use std::process::Command;

fn git(args: &[&str]) -> Option<std::process::Output> {
    Command::new("git").args(args).output().ok()
}

fn main() {
    let sha = std::env::var("KDX_GIT_SHA")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            let out = git(&["rev-parse", "--short=12", "HEAD"])?;
            let sha = out
                .status
                .success()
                .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
                .filter(|s| !s.is_empty())?;
            let clean = git(&["diff", "--quiet", "HEAD"]).is_some_and(|o| o.status.success());
            Some(if clean { sha } else { format!("{sha}-dirty") })
        });
    println!(
        "cargo:rustc-env=KDX_GIT_SHA={}",
        sha.as_deref().unwrap_or("unknown")
    );
    println!("cargo:rerun-if-env-changed=KDX_GIT_SHA");
    for p in ["HEAD", "index", "refs/heads"] {
        if let Some(o) = git(&["rev-parse", "--git-path", p]) {
            if o.status.success() {
                let path = String::from_utf8_lossy(&o.stdout).trim().to_string();
                println!("cargo:rerun-if-changed={path}");
            }
        }
    }
    // A change to a tracked file under the workspace's sources makes the tree dirty.
    for p in ["../../crates", "../../compare", "../../Cargo.lock"] {
        println!("cargo:rerun-if-changed={p}");
    }
}
