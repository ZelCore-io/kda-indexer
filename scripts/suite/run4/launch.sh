#!/bin/bash
# Suite run 4: the run, then its report. Started by kdx-suite4.service or start4.sh (never by
# hand during another run). SUITE_DIR is the host directory with tools4/, cases4/ and run4/.
# Stock heavy lanes stop taking new cases after STOCK_HEAVY_HOURS (default 34); cases are
# ordered run-2 differences first, so a stop leaves the fresh sample short, not the differences.
set -u
cd "${SUITE_DIR:?set SUITE_DIR}"
/bin/bash run4/verify4.sh >> run4/run.log 2>&1 || exit 1      # H6 (S8): tools4 and binary at the fix-round commit
date -u +%FT%TZ >> run4/start.time
nice -n 10 python3 -u tools4/run.py run --config run4/suite4.toml --cases cases4 --out run4 \
  --sample-flag --heavy-sample 0 --stock-heavy-hours "${STOCK_HEAVY_HOURS:-34}" >> run4/run.log 2>&1
rc=$?
date -u +%FT%TZ > run4/end.time
python3 tools4/run.py report --out run4 >> run4/run.log 2>&1
exit $rc
