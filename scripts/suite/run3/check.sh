#!/bin/bash
# bounded health check of suite run 3
S="${SUITE_DIR:-/root/kdx-suite}"
date -u +%FT%TZ
echo "suite3: $(systemctl show kdx-suite3 -p ActiveState -p SubState -p Result 2>/dev/null | tr "\n" " ")"
P=$(systemctl show kda-indexer-mainnet01 -p MainPID --value)
echo "ours NRestarts=$(systemctl show kda-indexer-mainnet01 -p NRestarts --value) active=$(systemctl is-active kda-indexer-mainnet01) RSS_MB=$(ps -o rss= -p $P | awk "{print int(\$1/1024)}")"
echo "fv units: $(systemctl is-active kdx-fv-harness.scope kdx-fv6 kdx-fv6-run kdx-fv6-pair-run kdx-fv-replay-f3d1.timer kdx-fv-replay-f3d2.timer 2>&1 | tr "\n" " ")"
echo "rows: results $(wc -l < $S/run3/results.jsonl 2>/dev/null) sample $(wc -l < $S/run3/sample.jsonl 2>/dev/null)"
grep -v "^throttle" $S/run3/run.log | tail -4 | cut -c1-200
echo "throttle events: $(grep -c "^throttle" $S/run3/run.log); last: $(grep "^throttle" $S/run3/run.log | tail -2 | cut -c10-220 | tr "\n" "|")"
echo "ours 429 rows: $(grep -c "\"st\":429" $S/run3/results.jsonl)"
echo "rows with tip: $(grep -c "\"tip\":" $S/run3/results.jsonl) with evidence: $(grep -c "\"evidence\":" $S/run3/results.jsonl)"
echo "leak: $(tail -3 /root/kdx-fv/leak-samples.txt | tr "\n" " ")"
echo "load: $(cat /proc/loadavg)"
