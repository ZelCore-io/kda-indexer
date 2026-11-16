//! The build sha and the git files `build.rs` watches (R-P5P9-95 m-13), shared with the
//! crate's tests (R-P5P9-96 (4)).

use std::path::Path;
use std::process::Command;

fn git(dir: &Path, args: &[&str]) -> Option<std::process::Output> {
    Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .ok()
}

/// The short sha of `HEAD` at `dir`, with `-dirty` when a tracked file differs from `HEAD`;
/// `None` outside a checkout.
pub fn sha(dir: &Path) -> Option<String> {
    let out = git(dir, &["rev-parse", "--short=12", "HEAD"])?;
    let sha = out
        .status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|s| !s.is_empty())?;
    let clean = git(dir, &["diff", "--quiet", "HEAD"]).is_some_and(|o| o.status.success());
    Some(if clean { sha } else { format!("{sha}-dirty") })
}

/// `HEAD`, the index and the branch refs as git names them from `dir` (relative to `dir` or
/// absolute). In a worktree `.git` is a file, so these are asked of git, never assumed.
pub fn watched(dir: &Path) -> Vec<String> {
    ["HEAD", "index", "refs/heads"]
        .into_iter()
        .filter_map(|p| git(dir, &["rev-parse", "--git-path", p]))
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(dir: &Path, args: &[&str]) {
        let ok = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args([
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "-c",
                "core.hooksPath=/dev/null",
            ])
            .args(args)
            .output()
            .unwrap();
        assert!(ok.status.success(), "git {args:?}: {ok:?}");
    }

    /// R-P5P9-96 (4), m-13: an edit to a tracked file marks the sha `-dirty`, and in a
    /// worktree (whose `.git` is a file, so `.git/HEAD` names nothing) the watched files are
    /// the worktree's own and exist.
    #[test]
    fn g_m13_the_build_sha_is_dirty_and_a_worktree_is_watched() {
        let tmp = tempfile::tempdir().unwrap();
        let main = tmp.path().join("main");
        std::fs::create_dir(&main).unwrap();
        run(&main, &["init", "-q"]);
        std::fs::write(main.join("f.txt"), "1\n").unwrap();
        run(&main, &["add", "f.txt"]);
        run(&main, &["commit", "-q", "-m", "one"]);
        let clean = sha(&main).unwrap();
        assert!(!clean.ends_with("-dirty"), "{clean}");
        std::fs::write(main.join("f.txt"), "2\n").unwrap();
        assert_eq!(sha(&main), Some(format!("{clean}-dirty")));
        run(&main, &["checkout", "-q", "f.txt"]);
        let wt = tmp.path().join("wt");
        run(
            &main,
            &["worktree", "add", "-q", "-b", "w", wt.to_str().unwrap()],
        );
        assert!(wt.join(".git").is_file());
        assert!(!wt.join(".git/HEAD").exists());
        let w = watched(&wt);
        assert_eq!(w.len(), 3, "{w:?}");
        for p in &w {
            assert!(wt.join(p).exists(), "{p} {w:?}");
        }
        let head = wt.join(&w[0]);
        assert_ne!(head, main.join(".git/HEAD"), "{w:?}");
        assert_eq!(sha(&wt), Some(clean.clone()));
        std::fs::write(wt.join("f.txt"), "3\n").unwrap();
        assert_eq!(sha(&wt), Some(format!("{clean}-dirty")));
    }
}
