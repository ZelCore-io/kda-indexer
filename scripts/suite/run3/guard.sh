#!/bin/bash
# Guard for suite run 3, as run 2's: stops kdx-suite3 (only) if ours restarted, an FV unit is
# down, or the leak sample grew > 15 % over its value at guard start. Exits when kdx-suite3 is
# inactive.
S="${SUITE_DIR:-/root/kdx-suite}"
log=$S/run3/guard.log
base=$(tail -1 /root/kdx-fv/leak-samples.txt | awk "{print \$2}")
echo "$(date -u +%FT%TZ) guard start, leak base $base" >> $log
while systemctl is-active -q kdx-suite3; do
  nr=$(systemctl show kda-indexer-mainnet01 -p NRestarts --value)
  why=""
  if [ "$nr" != "0" ] || ! systemctl is-active -q kda-indexer-mainnet01; then why="ours NRestarts=$nr"; fi
  for u in kdx-fv-harness.scope kdx-fv6 kdx-fv6-run kdx-fv6-pair-run; do systemctl is-active -q $u || why="$why $u down"; done
  cur=$(tail -1 /root/kdx-fv/leak-samples.txt | awk "{print \$2}")
  if [ -n "$cur" ] && [ "$cur" -gt $(( base + base * 15 / 100 )) ]; then why="$why leak $cur > base $base +15%"; fi
  if [ -n "$why" ]; then systemctl stop kdx-suite3; echo "$(date -u +%FT%TZ) ALERT: $why; kdx-suite3 stopped" >> $log; exit 1; fi
  sleep 20
done
echo "$(date -u +%FT%TZ) kdx-suite3 inactive; guard exits" >> $log
