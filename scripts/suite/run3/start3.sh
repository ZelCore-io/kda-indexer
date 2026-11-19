#!/bin/bash
# Starts suite run 3 and its guard, as run 2's restart2.sh did (transient units). NOT run by the
# harness round that prepared it: the owner starts it. Needs tools3/, cases3/, suite3.toml.
set -e
S="${SUITE_DIR:-/root/kdx-suite}"
cd "$S"
systemctl stop kdx-suite3-guard 2>/dev/null || true
systemd-run --unit=kdx-suite3 --collect -p Nice=10 -p MemoryMax=4G --working-directory="$S" /bin/bash "$S/run3/launch.sh"
sleep 3
systemd-run --unit=kdx-suite3-guard --collect /bin/bash "$S/run3/guard.sh"
sleep 2
systemctl is-active kdx-suite3 kdx-suite3-guard
