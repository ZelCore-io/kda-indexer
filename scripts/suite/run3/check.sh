#!/bin/bash
# bounded health check of suite run 3. Host layout and unit names come from the environment:
#   SUITE_DIR   host directory with tools3/, cases3/ and run3/ (required)
#   OURS_UNIT   the indexer's systemd unit (required)
#   FV_UNITS    space-separated units that must stay active (optional)
#   LEAK_FILE   memory-sample file, last field 2 is the value (optional)
set -u
S="${SUITE_DIR:?set SUITE_DIR}"
O="${OURS_UNIT:?set OURS_UNIT}"
date -u +%FT%TZ
echo "suite3: $(systemctl show kdx-suite3 -p ActiveState -p SubState -p Result 2>/dev/null | tr "\n" " ")"
P=$(systemctl show "$O" -p MainPID --value)
echo "ours NRestarts=$(systemctl show "$O" -p NRestarts --value) active=$(systemctl is-active "$O") RSS_MB=$(ps -o rss= -p $P | awk "{print int(\$1/1024)}")"
[ -n "${FV_UNITS:-}" ] && echo "fv units: $(systemctl is-active $FV_UNITS 2>&1 | tr "\n" " ")"
echo "rows: results $(wc -l < $S/run3/results.jsonl 2>/dev/null) sample $(wc -l < $S/run3/sample.jsonl 2>/dev/null)"
grep -v "^throttle" $S/run3/run.log | tail -4 | cut -c1-200
echo "throttle events: $(grep -c "^throttle" $S/run3/run.log); last: $(grep "^throttle" $S/run3/run.log | tail -2 | cut -c10-220 | tr "\n" "|")"
echo "ours 429 rows: $(grep -c "\"st\":429" $S/run3/results.jsonl)"
echo "rows with tip: $(grep -c "\"tip\":" $S/run3/results.jsonl) with evidence: $(grep -c "\"evidence\":" $S/run3/results.jsonl)"
[ -n "${LEAK_FILE:-}" ] && echo "leak: $(tail -3 "$LEAK_FILE" | tr "\n" " ")"
echo "load: $(cat /proc/loadavg)"
