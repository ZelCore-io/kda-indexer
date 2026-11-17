# Build and package kda-indexer on the target host (spec 6.2). No Docker, no cross build.
#   make build     release binaries (kda-indexer, kda-compare)
#   make deb       build + .deb in target/debian/   (needs: cargo install cargo-deb --locked)
#   make docs-charts  render docs/data/benchmarks.json to docs/img/*.svg (python3, stdlib only)
#   make docs-check   check docs links, quoted numbers and forbidden strings, run the tools' tests
#
# Run it at low priority so chainweb-node is not disturbed:
#   nice -n 19 ionice -c3 make deb

CARGO ?= cargo
PYTHON ?= python3

.PHONY: build deb clean docs-charts docs-check

build:
	$(CARGO) build --release --locked -p kda-indexer -p kdx-compare

deb: build
	$(CARGO) deb -p kda-indexer --no-build
	@ls -1 target/debian/kda-indexer_*.deb

clean:
	$(CARGO) clean

docs-charts:
	$(PYTHON) scripts/docs_charts.py

docs-check:
	$(PYTHON) -m unittest discover -s scripts/tests
	$(PYTHON) scripts/docs_check.py .
