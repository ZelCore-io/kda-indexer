#!/bin/bash
# Guard for suite run 4, as run 3's: stops kdx-suite4 (only) if ours restarted, a unit in
# FV_UNITS is down, or the leak sample grew > 15 % over its value at guard start. Exits when
# kdx-suite4 is inactive. SUITE_DIR and OURS_UNIT are required; FV_UNITS and LEAK_FILE optional
# (see check.sh).
set -u
S="${SUITE_DIR:?set SUITE_DIR}"
O="${OURS_UNIT:?set OURS_UNIT}"
log=$S/run4/guard.log
base=""
[ -n "${LEAK_FILE:-}" ] && base=$(tail -1 "$LEAK_FILE" | awk "{print \$2}")
echo "$(date -u +%FT%TZ) guard start, leak base ${base:-none}" >> $log
while systemctl is-active -q kdx-suite4; do
  nr=$(systemctl show "$O" -p NRestarts --value)
  why=""
  if [ "$nr" != "0" ] || ! systemctl is-active -q "$O"; then why="ours NRestarts=$nr"; fi
  for u in ${FV_UNITS:-}; do systemctl is-active -q $u || why="$why $u down"; done
  if [ -n "$base" ]; then
    cur=$(tail -1 "$LEAK_FILE" | awk "{print \$2}")
    if [ -n "$cur" ] && [ "$cur" -gt $(( base + base * 15 / 100 )) ]; then why="$why leak $cur > base $base +15%"; fi
  fi
  if [ -n "$why" ]; then systemctl stop kdx-suite4; echo "$(date -u +%FT%TZ) ALERT: $why; kdx-suite4 stopped" >> $log; exit 1; fi
  sleep 20
done
echo "$(date -u +%FT%TZ) kdx-suite4 inactive; guard exits" >> $log
