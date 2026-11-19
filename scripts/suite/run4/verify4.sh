#!/bin/bash
# H6 (S8): refuses run 4 unless tools4 and the pair-diff binary come from the reviewed fix-round
# commit. Called by start4.sh and launch.sh (so the installed unit checks too). stage4.sh writes
# the commit into EXPECTED_COMMIT below and stamps tools4; the binary is built separately:
#   git checkout <EXPECTED_COMMIT> && cargo build --release -p kdx-compare --bin kda-compare
#   cp target/release/kda-compare tools4/kda-compare-h5
#   (cd tools4 && sha256sum kda-compare-h5 > kda-compare-h5.sha256 && echo <EXPECTED_COMMIT> > kda-compare-h5.commit)
set -u
S="${SUITE_DIR:?set SUITE_DIR}"
EXPECTED_COMMIT="@COMMIT@"
T="$S/tools4"
fail() { echo "verify4: $*" >&2; exit 1; }
case "$EXPECTED_COMMIT" in *@*|"") fail "run4/verify4.sh was not staged by stage4.sh (no commit)";; esac
[ "$(cat "$T/COMMIT" 2>/dev/null)" = "$EXPECTED_COMMIT" ] || fail "tools4/COMMIT is not $EXPECTED_COMMIT"
(cd "$T" && sha256sum --quiet -c SHA256SUMS) || fail "tools4 files differ from tools4/SHA256SUMS ($EXPECTED_COMMIT)"
[ -x "$T/kda-compare-h5" ] || fail "missing tools4/kda-compare-h5 (build it from $EXPECTED_COMMIT, see the header)"
(cd "$T" && sha256sum --quiet -c kda-compare-h5.sha256) || fail "tools4/kda-compare-h5 does not match tools4/kda-compare-h5.sha256"
[ "$(cat "$T/kda-compare-h5.commit" 2>/dev/null)" = "$EXPECTED_COMMIT" ] \
  || fail "tools4/kda-compare-h5.commit is not $EXPECTED_COMMIT (binary built from another commit)"
grep -q 'kda_compare *= *"\?tools4/kda-compare-h5"\?' "$S/run4/suite4.toml" || fail "run4/suite4.toml does not use tools4/kda-compare-h5"
echo "verify4: tools4 and kda-compare-h5 at $EXPECTED_COMMIT ($(cut -c1-16 "$T/kda-compare-h5.sha256"))"
