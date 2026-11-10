//! Embeds the short git sha (`KDX_GIT_SHA`) for `kdx_build_info`; "unknown" outside a checkout.

use std::process::Command;

fn main() {
    let sha = std::env::var("KDX_GIT_SHA").ok().filter(|s| !s.is_empty()).or_else(|| {
        let out = Command::new("git")
            .args(["rev-parse", "--short=12", "HEAD"])
            .output()
            .ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
            .filter(|s| !s.is_empty())
    });
    println!("cargo:rustc-env=KDX_GIT_SHA={}", sha.as_deref().unwrap_or("unknown"));
    println!("cargo:rerun-if-env-changed=KDX_GIT_SHA");
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs/heads");
}
