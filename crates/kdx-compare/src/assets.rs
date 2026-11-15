//! R-P5P9-88 (3a), FVH-E3.4: the harness's `compare/` assets and build. At start the harness
//! logs a hash of the allowlist and the GraphQL documents it loads, with its build sha;
//! each day's summary keeps them and `kda-compare summary` prints them, so the FV setup can
//! check that the host runs F's assets and build.

use std::path::Path;

use anyhow::Context;
use sha2::{Digest, Sha256};

/// The build's short git sha (`build.rs`); "unknown" outside a checkout.
pub const BUILD_SHA: &str = env!("KDX_GIT_SHA");

/// SHA-256 (hex) over the allowlist file and every `*.graphql` file directly in `ops_dir`
/// (when it is a directory), in file-name order, each as its name, a NUL, its bytes and a
/// NUL. A changed, added, removed or renamed document changes it.
pub fn assets_hash(allowlist: &Path, ops_dir: &Path) -> anyhow::Result<String> {
    let mut h = Sha256::new();
    let mut add = |name: &str, path: &Path| -> anyhow::Result<()> {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        h.update(name.as_bytes());
        h.update([0]);
        h.update(&bytes);
        h.update([0]);
        Ok(())
    };
    add("allowlist.toml", allowlist)?;
    if ops_dir.is_dir() {
        let mut docs: Vec<(String, std::path::PathBuf)> = std::fs::read_dir(ops_dir)
            .with_context(|| format!("reading {}", ops_dir.display()))?
            .filter_map(Result::ok)
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                name.ends_with(".graphql").then(|| (name, e.path()))
            })
            .collect();
        docs.sort();
        for (name, path) in docs {
            add(&format!("graphql/{name}"), &path)?;
        }
    }
    Ok(h.finalize().iter().map(|b| format!("{b:02x}")).collect())
}

/// The line the harness logs and the summary keeps: `assets <hash> build <sha>`.
pub fn assets_line(allowlist: &Path, ops_dir: &Path) -> anyhow::Result<String> {
    Ok(format!(
        "assets {} build {BUILD_SHA}",
        assets_hash(allowlist, ops_dir)?
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hash is stable over the same files, and changes with the allowlist, a document's
    /// bytes, an added document and a renamed one; files that are not documents are left out.
    #[test]
    fn the_assets_hash_follows_every_asset() {
        let dir = tempfile::tempdir().unwrap();
        let (allow, ops) = (
            dir.path().join("allowlist.toml"),
            dir.path().join("graphql"),
        );
        std::fs::create_dir_all(&ops).unwrap();
        std::fs::write(&allow, "a").unwrap();
        std::fs::write(ops.join("Q1.graphql"), "query Q1 { x }").unwrap();
        let h = || assets_hash(&allow, &ops).unwrap();
        let h0 = h();
        assert_eq!(h0.len(), 64);
        assert_eq!(h(), h0);
        std::fs::write(ops.join("notes.txt"), "not a document").unwrap();
        assert_eq!(h(), h0);
        std::fs::write(&allow, "b").unwrap();
        let h1 = h();
        assert_ne!(h1, h0);
        std::fs::write(ops.join("Q1.graphql"), "query Q1 { y }").unwrap();
        let h2 = h();
        assert_ne!(h2, h1);
        std::fs::write(ops.join("Q2.graphql"), "query Q2 { x }").unwrap();
        let h3 = h();
        assert_ne!(h3, h2);
        std::fs::rename(ops.join("Q2.graphql"), ops.join("Q3.graphql")).unwrap();
        assert_ne!(h(), h3);
        let line = assets_line(&allow, &ops).unwrap();
        assert!(line.starts_with("assets ") && line.contains(&format!(" build {BUILD_SHA}")));
        assert!(!BUILD_SHA.is_empty());
    }

    /// The shipped `compare/` assets hash without error.
    #[test]
    fn the_shipped_assets_hash() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compare");
        let h = assets_hash(&root.join("allowlist.toml"), &root.join("graphql")).unwrap();
        assert_eq!(h.len(), 64);
    }
}
