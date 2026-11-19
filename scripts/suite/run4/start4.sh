#!/bin/bash
# Starts suite run 4 and its guard as transient units, as run 3 was started. NOT run by the
# harness round that prepared it: the owner starts it. Needs tools4/, cases4/, run4/suite4.toml.
set -e
S="${SUITE_DIR:?set SUITE_DIR}"
cd "$S"
# H6 (S8): tools4 and the pair-diff binary must come from the reviewed fix-round commit
# (tools4/COMMIT, tools4/SHA256SUMS, sha256sum of tools4/kda-compare-h5 and its commit stamp).
# Build: run4/build4.sh <commit> (cargo build --release -p kdx-compare --bin kda-compare from a
# clean checkout at that commit; H7 SHOULD-4b), and fv-compare.toml must load tools4/allowlist.toml.
/bin/bash "$S/run4/verify4.sh"
systemctl stop kdx-suite4-guard 2>/dev/null || true
systemd-run --unit=kdx-suite4 --collect -E SUITE_DIR="$S" -p Nice=10 -p MemoryMax=4G --working-directory="$S" /bin/bash "$S/run4/launch.sh"
sleep 3
systemd-run --unit=kdx-suite4-guard --collect -E SUITE_DIR="$S" -E OURS_UNIT="${OURS_UNIT:?set OURS_UNIT}" -E FV_UNITS="${FV_UNITS:-}" -E LEAK_FILE="${LEAK_FILE:-}" /bin/bash "$S/run4/guard.sh"
sleep 2
systemctl is-active kdx-suite4 kdx-suite4-guard
