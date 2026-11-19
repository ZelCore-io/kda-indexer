#!/bin/bash
# Suite run 3: the run, then its report. Started by kdx-suite3.service or start3.sh (never by
# hand during another run). SUITE_DIR is the host directory with tools3/, cases3/ and run3/.
# Stock heavy lanes stop taking new cases after STOCK_HEAVY_HOURS (default 11); cases are
# ordered run-2 differences first, so a stop leaves the fresh sample short, not the differences.
set -u
cd "${SUITE_DIR:?set SUITE_DIR}"
date -u +%FT%TZ >> run3/start.time
nice -n 10 python3 -u tools3/run.py run --config suite3.toml --cases cases3 --out run3 \
  --sample-flag --heavy-sample 0 --stock-heavy-hours "${STOCK_HEAVY_HOURS:-11}" >> run3/run.log 2>&1
rc=$?
date -u +%FT%TZ > run3/end.time
python3 tools3/run.py report --out run3 >> run3/run.log 2>&1
exit $rc
