# Installing kda-indexer

kda-indexer is built on the host that runs it and installed as a Debian package. It runs as a
systemd service, one unit per network (`kda-indexer-mainnet01`), next to a chainweb-node
whose service API it reads. The layout follows Blockbook's:

| What | Where |
|---|---|
| binary | `/opt/coins/kda-indexer/<network>/bin/kda-indexer` |
| config | `/opt/coins/kda-indexer/<network>/config/<network>.toml` |
| data | `/opt/coins/data/kadena/kda-indexer/<network>/` |
| unit | `kda-indexer-<network>.service` |
| user | `kda-indexer` |

A fresh install **enables the service at boot but does not start it**: check the config, then
start it.

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

- installs `/opt/coins/kda-indexer/mainnet01/bin/kda-indexer` and `/usr/bin/kda-compare`
  (the comparison harness);
- creates the system user and group `kda-indexer` (no shell, no home) if they are missing;
- installs the units `kda-indexer-mainnet01.service` and `kda-compare@.service`;
- installs the config `/opt/coins/kda-indexer/mainnet01/config/mainnet01.toml` (mode 0640,
  group `kda-indexer`);
- creates `/opt/coins/data/kadena/kda-indexer` (if absent) and the empty data dir
  `/opt/coins/data/kadena/kda-indexer/mainnet01` (owner `kda-indexer`);
- enables `kda-indexer-mainnet01.service` at boot (fresh install only), without starting it;
- puts the documentation, licences, notices and example configs in
  `/usr/share/doc/kda-indexer/`.

It never changes ownership of an existing `/opt/coins/data/kadena/kda-indexer` tree, so a
dedicated volume can be mounted there before or after the install (with the service
stopped).

**The volume root must be traversable by the service user.** A freshly formatted or
copied-over volume often has its root at `0700` or `0750` `root:root`; the service user
(`kda-indexer`) then cannot reach `<volume>/mainnet01` even though that directory is its own,
and the unit fails to open the store. Set the mode of the mounted volume root to `0755`
(`chmod 0755 /opt/coins/data/kadena/kda-indexer`) before the first start. The package never
changes the mode of an existing directory; instead the postinst checks, as the service user,
that every directory down to the data dir can be entered and that the data dir is writable,
and stops with a message naming the directory and the fix if not. Re-run
`dpkg --configure kda-indexer` after fixing it.

## Configure

Edit `/opt/coins/kda-indexer/mainnet01/config/mainnet01.toml`. As shipped it passes `config check` and every listener
binds `127.0.0.1`:

| Key | Shipped | Notes |
|---|---|---|
| `network` | `mainnet01` | |
| `data_dir` | `/opt/coins/data/kadena/kda-indexer/mainnet01` | must be writable by the unit (see the sandbox note below) |
| `node.url` | `http://127.0.0.1:31351` | the chainweb-node service API |
| `api.native` | `127.0.0.1:31361` | loopback or a private address only; the server refuses anything else |
| `api.compat` | `127.0.0.1:31362` | chainweb-data compatible routes |
| `api.admin` | `127.0.0.1:31369` | `/healthz`, `/readyz`, `/status`, `/metrics`; loopback or private only |

All keys, with their defaults, are in `/usr/share/doc/kda-indexer/examples/mainnet01.toml.example`.
The config is a conffile: `dpkg` keeps your edits on upgrade.

Check it before the first start:

```sh
sudo -u kda-indexer kda-indexer --config /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml config check
```

The `kda-indexer` user can read the config because the file is group-readable by it. An error
names the key; `<placeholder>` values (as in the examples) are refused on purpose.

## First start

```sh
sudo systemctl start kda-indexer-mainnet01
journalctl -u kda-indexer-mainnet01 -f
systemctl status kda-indexer-mainnet01        # STATUS shows "backfill r/n" or "tail lag N"
curl -s http://127.0.0.1:31369/status
```

On an empty data dir the service backfills the whole history (days for mainnet) and then
follows the tip. The unit reports `READY` as soon as the API is bound, so the start does not
time out during a long backfill; `/readyz` answers 503 until the indexer has caught up.

The package has already enabled the unit, so it comes back after a reboot.

### Another network

The package ships the `mainnet01` unit and config. For another network (for example
testnet), copy both and replace `mainnet01` in them:

```sh
sudo install -d -m 0755 /opt/coins/kda-indexer/testnet/bin
sudo install -d -m 0750 -g kda-indexer /opt/coins/kda-indexer/testnet/config
sudo install -m 0755 /opt/coins/kda-indexer/mainnet01/bin/kda-indexer /opt/coins/kda-indexer/testnet/bin/
sudo install -m 0640 -g kda-indexer /usr/share/doc/kda-indexer/examples/testnet.toml.example \
    /opt/coins/kda-indexer/testnet/config/testnet.toml
sudo install -d -m 0750 -o kda-indexer -g kda-indexer /opt/coins/data/kadena/kda-indexer/testnet
sed 's/mainnet01/testnet/g' /lib/systemd/system/kda-indexer-mainnet01.service \
    | sudo tee /etc/systemd/system/kda-indexer-testnet.service >/dev/null
sudo systemctl daemon-reload && sudo systemctl enable kda-indexer-testnet
```

A package upgrade replaces only the mainnet01 binary; copy it again for other networks.

### Backfill on a busy host

The unit's seccomp filter stops the process from changing its own priority. For a first sync
on a host that also serves other things, lower the priority from outside with a drop-in:

```sh
sudo systemctl edit kda-indexer-mainnet01     # paste examples/backfill.conf, save
sudo systemctl restart kda-indexer-mainnet01
```

Remove the drop-in when the backfill has finished.

### Memory limits

The unit ships `MemoryHigh=12G` and `MemoryMax=16G`, sized for a 23 GiB host. Memory-mapped
segment pages are reclaimable page cache and do not count against the hard limit like heap.
Set your own values in a drop-in (`examples/memory.conf`).

### The sandbox and the data dir

The unit is sandboxed (`ProtectSystem=strict`). It may write only to
`/opt/coins/data/kadena/kda-indexer/<network>` and, if it exists, the node_exporter textfile
directory. `data_dir` must therefore be inside that directory. If your store sits
somewhere else, either move it (see "Moving an existing data dir into an instance dir" in
[operations.md](operations.md)) or keep it where it is with a drop-in that opens that path:
`examples/datadir-volume-root.conf` shows the case of a store at the root of a volume mounted
at `/opt/coins/data/kadena/kda-indexer`.

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
differences. It is not enabled by the package, and keeps its own paths under
`/etc/kda-indexer` and `/var/lib/kda-indexer`.

```sh
sudo install -d -m 0750 -g kda-indexer /etc/kda-indexer
sudo install -d -o kda-indexer -g kda-indexer -m 0750 /var/lib/kda-indexer/compare-mainnet01
sudo install -m 0640 -g kda-indexer /usr/share/doc/kda-indexer/examples/compare.toml.example \
    /etc/kda-indexer/compare-mainnet01.toml
sudoedit /etc/kda-indexer/compare-mainnet01.toml
sudo systemctl start kda-compare@mainnet01
```

## A second host from a snapshot

A second host (a read replica, or a standby) starts from a snapshot of a running one
instead of a full backfill. On the source host (the unit keeps running), snapshot onto the
**same volume** as the data dir, without `--copy`: the segment files are hard-linked, so the
snapshot takes almost no space there and `rsync` reads from it. Do not use `--copy` on the
same volume; it writes a second full copy of every segment file to the source volume (on
shared storage that is the allocation risk "Before any big write" in
[operations.md](operations.md) warns about). Use `--copy` only to write the snapshot to
another local filesystem.

```sh
sudo -u kda-indexer kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml \
    snapshot --out /opt/coins/data/kadena/kda-indexer/snapshot-copy
```

On the new host, install the package as above, but leave the data dir empty, stop the unit,
and copy the snapshot in (the directory must end up owned by `kda-indexer`):

```sh
rsync -aH --delete source-host:/opt/coins/data/kadena/kda-indexer/snapshot-copy/ \
    /opt/coins/data/kadena/kda-indexer/mainnet01/
sudo chown -R kda-indexer:kda-indexer /opt/coins/data/kadena/kda-indexer/mainnet01
sudo systemctl start kda-indexer-mainnet01
```

The service resumes the tail from the checkpoints in the snapshot and reaches lag 0 in
minutes to hours depending on the snapshot's age. Use `serve` instead of `run` for a replica
that must not ingest. Delete `snapshot-copy` on the source host when the copy is done. The
node, config and ports of the new host are its own. See "Snapshots and replicas" in
[operations.md](operations.md).

## Upgrade

Build the new package and `dpkg -i` it. The config and your drop-ins stay. Instances that were
running are restarted (`try-restart`) and resume where they stopped; a stopped service stays
stopped. A new data-format version is never migrated in place; see [format.md](format.md) and
[operations.md](operations.md).

## Uninstall

```sh
sudo systemctl stop kda-indexer-mainnet01 'kda-compare@*'   # the package also does this on removal
sudo apt remove kda-indexer        # keeps the config
sudo apt purge kda-indexer         # also removes the config
```

Neither command deletes `/opt/coins/data/kadena/kda-indexer` (the store takes days to rebuild) or the
`kda-indexer` user. To remove them as well, after you have checked that you want to:

```sh
sudo rm -rf /opt/coins/data/kadena/kda-indexer
sudo deluser --system kda-indexer
```
