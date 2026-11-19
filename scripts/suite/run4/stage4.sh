#!/bin/bash
# H6 (S8): stages run 4's tools4/ and run4/ scripts from one commit of this repository into
# OUT (then copied to the host's SUITE_DIR; suite4.toml, suite4.env, fv-compare.toml and cases4
# stay as staged on the host). Writes tools4/COMMIT, tools4/SHA256SUMS and the commit into
# run4/verify4.sh. Usage: stage4.sh <commit> <out dir>
set -eu
C=$(git rev-parse --verify "${1:?commit}^{commit}")
OUT="${2:?out dir}"
mkdir -p "$OUT/tools4" "$OUT/run4"
for f in allow.py endpoints.py gen.py nodecheck.py run.py select3.py stress.py; do
  git show "$C:scripts/suite/$f" > "$OUT/tools4/$f"
done
git show "$C:compare/allowlist.toml" > "$OUT/tools4/allowlist.toml"
for f in check.sh guard.sh launch.sh start4.sh verify4.sh kdx-suite4.service kdx-suite4-guard.service; do
  git show "$C:scripts/suite/run4/$f" > "$OUT/run4/$f"
done
sed -i.bak "s/^EXPECTED_COMMIT=\"@COMMIT@\"$/EXPECTED_COMMIT=\"$C\"/" "$OUT/run4/verify4.sh" && rm "$OUT/run4/verify4.sh.bak"
grep -q "^EXPECTED_COMMIT=\"$C\"$" "$OUT/run4/verify4.sh"
echo "$C" > "$OUT/tools4/COMMIT"
(cd "$OUT/tools4" && sha256sum allow.py endpoints.py gen.py nodecheck.py run.py select3.py stress.py allowlist.toml COMMIT > SHA256SUMS)
echo "staged $C in $OUT; build tools4/kda-compare-h5 from it (run4/verify4.sh header)"
