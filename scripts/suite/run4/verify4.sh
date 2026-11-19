#!/bin/bash
# H6 (S8): refuses run 4 unless tools4 and the pair-diff binary come from the reviewed fix-round
# commit. Called by start4.sh and launch.sh (so the installed unit checks too). stage4.sh writes
# the commit into EXPECTED_COMMIT below and stamps tools4; the binary is built by
# run4/build4.sh <EXPECTED_COMMIT> (H7 SHOULD-4b): a clean checkout at that commit, with the
# sha256 and the commit stamp written from that checkout's `git rev-parse HEAD`.
# H7 (SHOULD-4c, R-P5P9-173): the pair-diff config named by run4/suite4.toml ([diff] config)
# must load tools4/allowlist.toml, and the sha256 of its [graphql] field_rules is pinned in
# run4/fv-compare.pin at the first verify; a later verify with other field_rules refuses.
# H8 (R-P5P9-174): the pin covers [graphql] total_count_cap with field_rules (absent = default).
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
FV=$(python3 -c '
import hashlib, json, os, sys, tomllib
s = sys.argv[1]
c = tomllib.load(open(os.path.join(s, "run4/suite4.toml"), "rb")).get("diff", {}).get("config")
if not c:
    sys.exit("run4/suite4.toml names no [diff] config")
fv = tomllib.load(open(os.path.join(s, c), "rb"))
al = fv.get("allowlist", "compare/allowlist.toml")
if os.path.realpath(os.path.join(s, al)) != os.path.realpath(os.path.join(s, "tools4/allowlist.toml")):
    sys.exit("%s: pair-diff would load allowlist %s, not tools4/allowlist.toml" % (c, al))
g = fv.get("graphql", {})
fr = g.get("field_rules", [])
cap = g.get("total_count_cap")         # absent: the compiled default (config.rs)
h = hashlib.sha256(json.dumps({"field_rules": fr, "total_count_cap": cap}, sort_keys=True,
                              separators=(",", ":")).encode()).hexdigest()
print("%s field_rules+total_count_cap sha256 %s (%d rules, total_count_cap %s)"
      % (c, h, len(fr), "default" if cap is None else cap))
' "$S" 2>&1) || fail "$FV"
P="$S/run4/fv-compare.pin"
if [ -f "$P" ]; then
  [ "$(cat "$P")" = "$FV" ] || fail "pair-diff config changed since the pin: now '$FV', pinned '$(cat "$P")'"
else
  echo "$FV" > "$P"
fi
echo "verify4: tools4 and kda-compare-h5 at $EXPECTED_COMMIT ($(cut -c1-16 "$T/kda-compare-h5.sha256")); $FV"
