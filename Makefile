# Build and package kda-indexer on the target host (spec 6.2). No Docker, no cross build.
#   make build     release binaries (kda-indexer, kda-compare)
#   make deb       build + .deb in target/debian/   (needs: cargo install cargo-deb --locked)
#
# Run it at low priority so chainweb-node is not disturbed:
#   nice -n 19 ionice -c3 make deb

CARGO ?= cargo

.PHONY: build deb clean

build:
	$(CARGO) build --release --locked -p kda-indexer -p kdx-compare

deb: build
	$(CARGO) deb -p kda-indexer --no-build
	@ls -1 target/debian/kda-indexer_*.deb

clean:
	$(CARGO) clean
