# Installing kda-indexer

kda-indexer is built on the host that runs it and installed as a Debian package. It runs as a
systemd service, one instance per network (`kda-indexer@mainnet01`), next to a chainweb-node
whose service API it reads.

The package installs the service **stopped and disabled**. Nothing starts until you say so.

## Requirements

- Debian or Ubuntu with systemd; x86-64 (a package built on one host installs on others of
  the same architecture, the build uses a generic target CPU).
- A chainweb-node with its service API reachable from the host (default
  `http://127.0.0.1:31351`).
- A data volume with room for the store. Mainnet needs a few hundred GB; the indexer refuses
  to start a backfill with less than `backfill.min_free_gb` (default 60) free.
- Build tools (below). They are not needed at run time.

## Build the package

```sh
sudo apt install build-essential clang libclang-dev pkg-config git curl   # libmdbx C build and bindgen
curl https://sh.rustup.rs -sSf | sh                                      # then open a new shell
cargo install cargo-deb --locked
git clone https://github.com/ZelCore-io/kda-indexer.git
cd kda-indexer
nice -n 19 ionice -c3 make deb
```

- `rustup` installs the toolchain pinned in `rust-toolchain.toml` on the first build.
- `nice -n 19 ionice -c3` keeps the build from disturbing a chainweb-node on the same host.
  The build uses thin LTO and one codegen unit, so expect a while; a first build is the long one.
- The package is `target/debian/kda-indexer_<version>_amd64.deb`.

## Install

```sh
sudo dpkg -i target/debian/kda-indexer_<version>_amd64.deb
```

The package:

- installs `/usr/bin/kda-indexer` and `/usr/bin/kda-compare` (the comparison harness);
- creates the system user and group `kda-indexer` (no shell, no home) if they are missing;
- installs the units `kda-indexer@.service` and `kda-compare@.service`;
- installs the config `/etc/kda-indexer/mainnet01.toml` (mode 0640, group `kda-indexer`);
- creates `/var/lib/kda-indexer` (if absent) and the empty data dir
  `/var/lib/kda-indexer/mainnet01` (owner `kda-indexer`);
- puts the documentation, licences, notices and example configs in
  `/usr/share/doc/kda-indexer/`.

It does not enable or start anything, and it never changes ownership of an existing
`/var/lib/kda-indexer` tree.

## Configure

Edit `/etc/kda-indexer/mainnet01.toml`. As shipped it passes `config check` and every listener
binds `127.0.0.1`:

| Key | Shipped | Notes |
|---|---|---|
| `network` | `mainnet01` | |
| `data_dir` | `/var/lib/kda-indexer/mainnet01` | must be writable by the unit (see the sandbox note below) |
| `node.url` | `http://127.0.0.1:31351` | the chainweb-node service API |
| `api.native` | `127.0.0.1:31361` | loopback or a private address only; the server refuses anything else |
| `api.compat` | `127.0.0.1:31362` | chainweb-data compatible routes |
| `api.admin` | `127.0.0.1:31369` | `/healthz`, `/readyz`, `/status`, `/metrics`; loopback or private only |

All keys, with their defaults, are in `/usr/share/doc/kda-indexer/examples/mainnet01.toml.example`.
The config is a conffile: `dpkg` keeps your edits on upgrade.

Check it before the first start:

```sh
sudo -u kda-indexer kda-indexer --config /etc/kda-indexer/mainnet01.toml config check
```

The `kda-indexer` user can read the config because the file is group-readable by it. An error
names the key; `<placeholder>` values (as in the examples) are refused on purpose.

## First start

```sh
sudo systemctl start kda-indexer@mainnet01
journalctl -u kda-indexer@mainnet01 -f
systemctl status kda-indexer@mainnet01        # STATUS shows "backfill r/n" or "tail lag N"
curl -s http://127.0.0.1:31369/status
```

On an empty data dir the service backfills the whole history (days for mainnet) and then
follows the tip. The unit reports `READY` as soon as the API is bound, so the start does not
time out during a long backfill; `/readyz` answers 503 until the indexer has caught up.

When it behaves, start it at boot:

```sh
sudo systemctl enable kda-indexer@mainnet01
```

### Backfill on a busy host

The unit's seccomp filter stops the process from changing its own priority. For a first sync
on a host that also serves other things, lower the priority from outside with a drop-in:

```sh
sudo systemctl edit kda-indexer@mainnet01     # paste examples/backfill.conf, save
sudo systemctl restart kda-indexer@mainnet01
```

Remove the drop-in when the backfill has finished.

### Memory limits

The unit ships `MemoryHigh=12G` and `MemoryMax=16G`, sized for a 23 GiB host. Memory-mapped
segment pages are reclaimable page cache and do not count against the hard limit like heap.
Set your own values in a drop-in (`examples/memory.conf`).

### The sandbox and the data dir

The unit is sandboxed (`ProtectSystem=strict`). It may write only to
`/var/lib/kda-indexer/<instance>` and, if it exists, the node_exporter textfile directory.
`data_dir` must therefore be inside `/var/lib/kda-indexer/<instance>`. If your store sits
somewhere else, either move it (see "Moving an existing data dir into an instance dir" in
[operations.md](operations.md)) or keep it where it is with a drop-in that opens that path:
`examples/datadir-volume-root.conf` shows the case of a store at the root of a volume mounted
at `/var/lib/kda-indexer`.

## Metrics

Prometheus text is served on the admin port: `http://127.0.0.1:31369/metrics`. To also write
the file node_exporter's textfile collector reads, set in the config

```toml
[metrics]
textfile = "/var/lib/node_exporter/textfile_collector/kda_indexer.prom"
```

and give the service write access to that directory (it writes a temporary file and renames
it, so node_exporter never sees a partial file):

```sh
sudo chgrp kda-indexer /var/lib/node_exporter/textfile_collector
sudo chmod g+w /var/lib/node_exporter/textfile_collector
```

If the directory is not writable the indexer logs once and keeps serving `/metrics` on the
admin port. See [operations.md](operations.md) for the metric list and suggested alerts.

## Comparison harness (optional)

`kda-compare@<name>` replays requests against the indexer and a reference and reports
differences. It is not enabled by the package.

```sh
sudo install -d -o kda-indexer -g kda-indexer -m 0750 /var/lib/kda-indexer/compare-mainnet01
sudo install -m 0640 -g kda-indexer /usr/share/doc/kda-indexer/examples/compare.toml.example \
    /etc/kda-indexer/compare-mainnet01.toml
sudoedit /etc/kda-indexer/compare-mainnet01.toml
sudo systemctl start kda-compare@mainnet01
```

## Upgrade

Build the new package and `dpkg -i` it. The config and your drop-ins stay. Instances that were
running are restarted (`try-restart`) and resume where they stopped; stopped instances stay
stopped. A new data-format version is never migrated in place; see [format.md](format.md) and
[operations.md](operations.md).

## Uninstall

```sh
sudo systemctl stop 'kda-indexer@*' 'kda-compare@*'      # the package also does this on removal
sudo apt remove kda-indexer        # keeps /etc/kda-indexer/mainnet01.toml
sudo apt purge kda-indexer         # also removes the config
```

Neither command deletes `/var/lib/kda-indexer` (the store takes days to rebuild) or the
`kda-indexer` user. To remove them as well, after you have checked that you want to:

```sh
sudo rm -rf /var/lib/kda-indexer
sudo deluser --system kda-indexer
```
