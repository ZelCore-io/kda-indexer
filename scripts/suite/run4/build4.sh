#!/bin/bash
# H7 (SHOULD-4b, R-P5P9-173): builds tools4/kda-compare-h5 from a clean checkout of one commit
# of the local repository copy (a detached git worktree; no network), under nice -n 10, and
# writes kda-compare-h5.sha256 and kda-compare-h5.commit from that checkout's
# `git rev-parse HEAD`, so the stamp is never written by hand. The commit must be the one
# stage4.sh stamped in tools4/COMMIT; run4/verify4.sh then checks both.
# Usage: build4.sh <commit> [suite dir]  (REPO: the repository copy, default: the one around
# the current directory; CARGO: the cargo to run, default cargo)
set -eu
fail() { echo "build4: $*" >&2; exit 1; }
REPO="${REPO:-$(git rev-parse --show-toplevel)}"
C=$(git -C "$REPO" rev-parse --verify "${1:?commit}^{commit}") || fail "no commit ${1}"
S="${2:-${SUITE_DIR:?pass the suite dir or set SUITE_DIR}}"
T="$S/tools4"
[ "$(cat "$T/COMMIT" 2>/dev/null)" = "$C" ] || fail "tools4/COMMIT is not $C (stage4.sh that commit first)"
W=$(mktemp -d "${TMPDIR:-/tmp}/build4.XXXXXX")
cleanup() { git -C "$REPO" worktree remove --force "$W/src" >/dev/null 2>&1 || true; rm -rf "$W"; git -C "$REPO" worktree prune; }
trap cleanup EXIT
git -C "$REPO" worktree add --detach "$W/src" "$C" >/dev/null 2>&1 || fail "cannot check out $C"
cd "$W/src"
H=$(git rev-parse HEAD)
[ "$H" = "$C" ] || fail "checkout is at $H, not $C"
[ -z "$(git status --porcelain --untracked-files=all)" ] || fail "checkout at $C is not clean"
nice -n 10 "${CARGO:-cargo}" build --release -p kdx-compare --bin kda-compare
B="${CARGO_TARGET_DIR:-$W/src/target}/release/kda-compare"
[ -f "$B" ] || fail "no $B after the build"
[ -z "$(git status --porcelain --untracked-files=no)" ] || fail "the build changed tracked files at $C"
cp "$B" "$T/kda-compare-h5.new" && chmod 755 "$T/kda-compare-h5.new" && mv "$T/kda-compare-h5.new" "$T/kda-compare-h5"
(cd "$T" && sha256sum kda-compare-h5 > kda-compare-h5.sha256)
echo "$H" > "$T/kda-compare-h5.commit"
echo "build4: tools4/kda-compare-h5 at $H ($(cut -c1-16 "$T/kda-compare-h5.sha256"))"
