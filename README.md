# kda-indexer

A compact, fast indexer for the [Kadena](https://github.com/kda-community) chainweb blockchain.

It indexes all 20 chains from a chainweb-node: blocks, full transactions, results, events and transfers. It serves them through:

- a native HTTP API;
- a REST API compatible with chainweb-data (`/txs/*`);
- a GraphQL API on its own port.

Status: early development. Nothing here is ready for production yet.

## Goals

- Full transaction history for all 20 chains, in a small fraction of the disk space today's Postgres-based indexers need.
- Fast initial sync: chains are ingested in parallel and history is written as immutable, compressed segments.
- Runs as a native service next to chainweb-node: a single binary installed as a Debian package with a systemd unit.

## Building

```sh
cargo build --release
./target/release/kda-indexer --config packaging/config/mainnet01.toml.example config check
```

The pinned toolchain in `rust-toolchain.toml` is installed automatically by `rustup`. Copy `packaging/config/mainnet01.toml.example` and adjust the node URL and listen addresses for your host.

## Installing

Build a Debian package on the host and install it: [docs/install.md](docs/install.md). The package ships a hardened systemd unit (`kda-indexer-<network>`, binary and config under `/opt/coins/kda-indexer/<network>/`, data under `/opt/coins/data/kadena/kda-indexer/<network>/`), a config with loopback listeners and the licence notices.

Operating notes (systemd, the data-dir lock, moving a data dir, read-only `serve`, metrics and alerts, recovering a halted chain): [docs/operations.md](docs/operations.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this work, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
