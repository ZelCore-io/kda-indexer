#!/bin/bash
# Starts suite run 4 and its guard as transient units, as run 3 was started. NOT run by the
# harness round that prepared it: the owner starts it. Needs tools4/, cases4/, run4/suite4.toml.
set -e
S="${SUITE_DIR:?set SUITE_DIR}"
cd "$S"
# H5: pair-diff must be this branch's build (open entries match nothing); run 3's binary is older.
if [ ! -x "$S/tools4/kda-compare-h5" ]; then
  echo "missing tools4/kda-compare-h5: build test/suite after run 3 ends:" >&2
  echo "  cargo build --release -p kdx-compare --bin kda-compare, then copy it to tools4/kda-compare-h5" >&2
  exit 1
fi
systemctl stop kdx-suite4-guard 2>/dev/null || true
systemd-run --unit=kdx-suite4 --collect -E SUITE_DIR="$S" -p Nice=10 -p MemoryMax=4G --working-directory="$S" /bin/bash "$S/run4/launch.sh"
sleep 3
systemd-run --unit=kdx-suite4-guard --collect -E SUITE_DIR="$S" -E OURS_UNIT="${OURS_UNIT:?set OURS_UNIT}" -E FV_UNITS="${FV_UNITS:-}" -E LEAK_FILE="${LEAK_FILE:-}" /bin/bash "$S/run4/guard.sh"
sleep 2
systemctl is-active kdx-suite4 kdx-suite4-guard
