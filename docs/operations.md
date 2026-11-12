# Operating kda-indexer

## Commands that write

`run`, `backfill` and `unwind` (and the hidden `tail`) write to the data directory. Each one
takes an exclusive lock on `<data_dir>/LOCK` and exits at once with an error if another
kda-indexer process holds it. Stop the service (`systemctl stop kda-indexer-<network>`) before running
`backfill` or `unwind` by hand. Read-only commands (`inspect`, `segments`, `hot stats`) do
not take the lock and can run next to the service.

## Halted chains

The tail follows reorgs up to `ingest.finality_depth` blocks deep. When a chain's fork is
deeper than that, lies below the frozen frontier, or is at a height whose stored hash is not
known, the tail **halts that chain**: it logs an ERROR with the fork height and depth, stops
following the chain and sets the hot counter `halted.<chain>`. The other chains keep
running, but the freezer stops for all chains (it freezes only heights that every chain has
passed), so the hot store grows until the halt is cleared. On restart a halted chain stays
skipped, with an ERROR line.

To recover:

1. Stop the indexer.
2. Find the chain and the fork height in the ERROR line (`hot stats` prints every counter with its value, one `counter <name> <n>` line each).
3. If the stored branch must go, unwind it and clear the halt in one step:

   ```sh
   kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml unwind --chain 3 --to 7390500 --clear-halt
   ```

   `--to` must be at or above `frozen_frontier − 1`; frozen heights are never unwound. To
   only clear the halt (for example after the node has settled on the stored branch), leave
   out `--to`:

   ```sh
   kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml unwind --chain 3 --clear-halt
   ```

   `--clear-halt` deletes `halted.<chain>` and its reason counter. It refuses when the chain's hot state is
   inconsistent: the `Tail` checkpoint has no stored header or a different hash, the tip has
   no undo record, or headers are stored above the checkpoint.
4. Start the indexer. The tail resumes the chain from its stored tip (or from the `Headers`
   checkpoint at `frozen_frontier − 1` when every hot block was unwound) and follows the
   node's current branch.

## Running under systemd

The package installs `kda-indexer-mainnet01.service`, one unit per network (Blockbook
layout): binary and config under `/opt/coins/kda-indexer/<network>/` (`bin/`, `config/`),
data under `/opt/coins/data/kadena/kda-indexer/<network>/` (see [install.md](install.md)).

```sh
systemctl start|stop|restart kda-indexer-mainnet01
systemctl status kda-indexer-mainnet01         # STATUS: "backfill r/n" or "tail lag N"
journalctl -u kda-indexer-mainnet01 -f
```

- **Readiness.** The unit is `Type=notify`. The service sends `READY=1` as soon as the store is
  open and the API is bound, before backfill, so a long first sync never trips the start
  timeout. Use `/readyz` (admin port) to know when it has caught up: 503 while backfilling,
  while any chain is halted, or while any chain lags more than `api.ready_max_lag` blocks.
  Lag is judged against the last node tips for up to 10 minutes after the node stops
  answering, so a node restart does not take the indexer out of the pool.
- **Watchdog.** `WatchdogSec=120`. A dedicated thread pings systemd only while ingest and the
  API runtime are making progress: backfill and the range writer beat per block and per
  postings file, the tail writer on every commit and every 5 s when idle, the freezer per
  block pushed, the API every 5 s. The tail and the freezer count separately, so a busy tail
  cannot hide a stuck freezer. If a component is silent for more than half the watchdog
  period, the thread logs one ERROR naming it and stops pinging; systemd then kills and
  restarts the unit (`Restart=on-failure`). `kda-indexer serve` has no ingest and watches only
  the API.
- **Stop.** SIGTERM finishes the current commit, then exits 0. `TimeoutStopSec=120` leaves room
  for a freezer step that is writing. A step that needs longer is killed by systemd; the
  next start removes its unfinished files and redoes it (the freezer is crash-safe).
- **Priority and memory.** The seccomp filter blocks changing priority from inside the
  process, so use drop-ins: `examples/backfill.conf` (idle IO, nice 10) while backfilling,
  `examples/memory.conf` for per-host limits.
- **Enabled at boot, not started.** A fresh install enables the unit but does not start it;
  start it once the config checks out. An upgrade restarts it only if it was running.

## Moving an existing data dir into a network dir

The unit writes to `/opt/coins/data/kadena/kda-indexer/<network>`. A store built by hand, for example by
`kda-indexer run` at the top of a volume mounted at `/opt/coins/data/kadena/kda-indexer`, has `hot/`,
`segments/` and so on directly in that directory. To make it match the unit, move it one level
down. This is a rename on the same filesystem, so it takes seconds; it still needs the service
stopped and the lock released.

1. Stop whatever writes the store (`systemctl stop kda-indexer-<network>` or the process
   started by hand) and wait for it to exit. Nothing may hold `<data_dir>/LOCK`.
2. Create the target and move every top-level entry of the store into it:

   ```sh
   cd /opt/coins/data/kadena/kda-indexer
   sudo mkdir -p mainnet01
   sudo sh -c 'for f in .[!.]* *; do [ "$f" = mainnet01 ] || [ ! -e "$f" ] || mv "$f" mainnet01/; done'
   ```

   Anything else that lives on the volume (for example `lost+found`) moves too; leave it out
   by editing the loop if that matters to you.
3. Give the service ownership of the store:

   ```sh
   sudo chown -R kda-indexer:kda-indexer /opt/coins/data/kadena/kda-indexer/mainnet01
   ```

4. Check `data_dir = "/opt/coins/data/kadena/kda-indexer/mainnet01"` in `/opt/coins/kda-indexer/mainnet01/config/mainnet01.toml`,
   then `systemctl start kda-indexer-mainnet01`. The log shows the resume point (manifest
   frontier and the tail's stored tips).
5. Read the `run: plan` line in `journalctl`: it must show the old `frontier` and
   `backfill=false` (a store that was found empty would show `frontier=0` and `backfill=true`;
   stop at once if you see that and check the path).

**Alternative, no move.** Keep the store where it is: set `data_dir = "/opt/coins/data/kadena/kda-indexer"`
and install the drop-in `examples/datadir-volume-root.conf`. It replaces the unit's
`ReadWritePaths` so the sandbox opens that directory. Ownership still has to be
`kda-indexer`.

## Format upgrade

A release that changes the store format (see the version and "Changes from version 1" at
the top of [format.md](format.md)) cannot read the old store. It refuses to start with:

```
store format v1, this build reads v2: rebuild it (docs/operations.md#format-upgrade)
```

There is no in-place migration: the new store is a full backfill (about 9–10 hours on
mainnet01). Do the "Before any big write" check first.

1. Stop the unit: `systemctl stop kda-indexer-mainnet01`. Record what you want to keep
   from the old store (`hot stats`, `segments list --json`, sizes).
2. Keep or delete the old data dir. In a fleet where other hosts serve traffic, keeping it
   (renamed, for example `mainnet01.v1`) allows a rollback to the old package; on a single
   host whose store nothing consumes yet, delete it so the volume does not hold both:

   ```sh
   sudo rm -rf /opt/coins/data/kadena/kda-indexer/mainnet01
   sudo install -d -o kda-indexer -g kda-indexer /opt/coins/data/kadena/kda-indexer/mainnet01
   ```

3. On thin-provisioned or Ceph-backed volumes, return the freed blocks:
   `sudo fstrim -v /opt/coins/data/kadena/kda-indexer`.
4. Install the new package (`dpkg -i`), which keeps the conffile.
5. Start the unit: `systemctl start kda-indexer-mainnet01`. On the empty data dir it runs a
   full backfill, then tails; `systemctl status` shows `backfill r/n` while it runs.

## Read-only `serve`

`kda-indexer serve` runs the APIs over a data dir that another process writes (a read replica,
or a look at a live store). It opens MDBX read-only, takes no lock and does no ingest.

```sh
kda-indexer --config /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml serve [--data-dir DIR]
```

- Give it its own listeners (`api.*` in a separate config) so it does not clash with the
  writer's ports.
- It polls the node's `/cut` every 10 s for the node tips; `/readyz` is 503 while those are
  unknown or older than 60 s.
- It reports `READY=1` and the watchdog works as for `run`, if you wrap it in a unit.
- A reader never holds an MDBX read transaction longer than one request, so it does not stop
  the writer from reusing pages.

### Merkle halts

The tail also halts a chain when a block's payload does not verify against its Merkle roots
after one refetch (spec §2.2 check 3; `format.md`, "Payload Merkle verification"). The ERROR
line has `reason=merkle` and the computed and claimed roots. The counters are
`halted.<chain>` and `halted.<chain>.merkle` (the other reasons are `reorg` and `poison`).
The blocks before the bad one are committed; the bad one is not.

A mismatch on a modern block means the node served bytes that do not match what it
committed to, or that the indexer's decode is wrong. Do not just clear it:

1. Check the node first: fetch `payload/<payloadHash>/outputs` for that chain again and run
   `kda-indexer dump --from H --to H --chains C --merkle` on the height. It is read-only.
2. If the block now verifies, clear the halt (`unwind --chain C --clear-halt`) and start.
   The tail refetches from the stored tip.
3. If it still fails, keep the chain halted and report it with the dump output.

Backfill does not halt. A mismatch there leaves the block's `merkle_verified` bit clear and
adds a line to `<data_dir>/merkle-unverified.jsonl`. `kda-indexer verify merkle --sample N`
(or `--all`) re-checks stored blocks at any time, read-only, beside a running indexer.

## Metrics and alerts

`/metrics` on the admin port (OpenMetrics text); the same families are written to the
node_exporter textfile when `metrics.textfile` is set (see [install.md](install.md)).

| Metric | Labels |
|---|---|
| `kdx_build_info` | version, git sha, network |
| `kdx_node_tip_height`, `kdx_indexed_tip_height`, `kdx_chain_lag_blocks` | chain |
| `kdx_chain_halted` | chain |
| `kdx_frozen_frontier_height` | |
| `kdx_ingest_blocks_total`, `kdx_ingest_txs_total` | mode (backfill, tail) |
| `kdx_stage_duration_seconds` | stage |
| `kdx_backfill_blocks_per_second`, `kdx_backfill_next_height`, `kdx_backfill_end_height` | |
| `kdx_reorgs_total`, `kdx_reorg_depth` | chain |
| `kdx_node_requests_total`, `kdx_node_request_seconds` | endpoint, status |
| `kdx_hot_store_bytes`, `kdx_segment_bytes` | kind |
| `kdx_disk_free_bytes` | |
| `kdx_mdbx_commit_seconds` | |
| `kdx_http_requests_total`, `kdx_http_request_seconds` | api, route, status |
| `kdx_freezer_runs_total`, `kdx_freezer_seconds` | op (freeze, merge) |
| `kdx_read_view_seconds` | phase (open, held) |
| `kdx_merkle_blocks_total` | result (verified, mismatch_legacy, mismatch) |

Alerts to set up in your own alerting:

- chain lag above 20 blocks for 5 minutes on any chain (`kdx_chain_lag_blocks`);
- any `kdx_chain_halted` equal to 1 (see "Halted chains" above);
- any increase of `kdx_merkle_blocks_total` with `result` other than `verified`
  (`mismatch_legacy` or `mismatch`: every era must verify);
- `kdx_disk_free_bytes` below 15% of the volume;
- a reorg deeper than 50 (`kdx_reorg_depth`);
- the comparison harness reporting unexplained differences over an hour;
- the unit restarting (`NRestarts` or a `systemd` failed-unit alert): the watchdog restarts a
  hung process and logs which component was silent.

## Snapshots and replicas

`kda-indexer snapshot --out DIR` writes a consistent copy of a data dir. It only reads, takes
no lock and can run beside the live `run` (and beside the freezer and merges). The copy is a
data dir: `serve --data-dir DIR` answers on it, and `run` on it resumes the tail from the
checkpoints it holds.

```sh
kda-indexer --config /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml \
    snapshot --out /opt/coins/data/kadena/kda-indexer/mainnet01.replica [--copy]
```

- `DIR` must not exist or be empty, and must not lie inside the source data dir. Without
  `--copy` it must be on the same filesystem as the source: the segment files are
  **hard-linked** (they are immutable), so the snapshot costs almost no extra space until a
  merge replaces files in the source. With `--copy` they are copied and `fsync`ed.
- Order: the command begins a fresh read transaction on the hot store, reads
  `segments/manifest.json`, checks that the transaction sees every dictionary name the
  manifest's segments refer to, writes a **compacted** copy of the hot store from that
  transaction into `DIR/hot` and closes the transaction, then links every file the manifest
  names and writes the manifest bytes. If the name check fails (a backfill range or a freeze
  committed between the two reads), the transaction and the manifest are taken again after a
  one-second pause (up to 30 reads, then the command fails and asks to retry). If a merge
  removes a step file before it is linked, the transaction, the manifest and the hot copy are
  taken again and the missing links are redone (up to five reads). Files of an earlier read
  that the final manifest no longer names are removed.
- A refused `--out` (inside the data dir, not empty) creates nothing; after any later failure
  the command removes what it created, so the same `--out` can be used again.
- Never copied: `etl/`, `LOCK`, `merkle-unverified.jsonl`.
- `DIR/SNAPSHOT.json` records the format versions, network, frozen frontier, `next_txnum`, the
  tail checkpoint per chain, the manifest's blake3, the binary version and the creation time
  (no paths).
- The read transaction stays open for the hot copy only (minutes on a large store), not for
  the segment links or copies, so the source writer cannot recycle the pages that transaction
  still sees for that long. Do not start a snapshot when the volume is short of free space;
  run the checks of "Before any big write" first.

A local replica:

```sh
kda-indexer --config .../mainnet01.toml snapshot --out /opt/coins/data/kadena/kda-indexer/replica
kda-indexer --config .../replica.toml serve        # replica.toml: data_dir of the snapshot, own api.* ports
```

To seed another host, take a hard-linked snapshot on the source volume (no `--copy`: on the
same volume `--copy` writes a second full copy of every segment file there; keep it for
another local filesystem), then
`rsync -aH SNAPSHOT_DIR/ other-host:/opt/coins/data/kadena/kda-indexer/mainnet01/` (`-H` keeps
the hard links that exist inside the copy). See "A second host from a snapshot" in
[install.md](install.md). A `run` on the copy catches up from the snapshot's checkpoints: it
follows the node block by block and resolves duplicate hot rows below the frozen frontier at
start, as after a crash during a freeze. A snapshot that is older than the node's history is
not a problem; one that is older than the tail can follow quickly is just slower to catch up.

## Measured mainnet backfill

One full format-v2 backfill of mainnet01 (20 chains, heights 0 to 7,390,000: 139,279,460
blocks and 369,200,367 transactions), from an empty store. Setup: one range in flight, the
`examples/backfill.conf` drop-in (idle IO, nice 10), `etl_mem_mb = 512`, `mem_cap_mb = 8192`,
`MemoryHigh=10G`, on a 24 GiB host that also ran the chainweb-node it read from (local service
API, 8 cores) and a local data volume. Use these numbers to size a host, not as a promise.

| Measure | Value |
|---|---|
| Wall time | 31,419 s (8 h 44 min) |
| Overall rate | about 4,430 blocks/s, 11,750 txs/s |
| Peak process memory (VmHWM) | 9.8 GiB (10,037 MiB; peak sampled RssAnon 9.45 GiB) |
| Peak cgroup memory (page cache included) | 10.0 GiB |
| Spool (`etl/`) peak | 19.7 GB |
| Volume used at the peak | 129.0 GB (`etl/` included) |
| Store when done | 125.6 GB of segments, 0.5 GB hot store, empty `etl/` |
| Node container CPU, sampled every 5 min | up to 361% (more than 300% in 11 of 105 samples) |
| Time to tail lag 0 after the post-backfill restart | under 10 s |

Rate by density band (blocks/s and txs/s are the band's blocks and txs over the summed wall time
of its ranges; the dense eras are CPU-bound in decode and index, the sparse ones wait on the node):

| Heights | Blocks | Txs | Blocks/s | Txs/s |
|---|---|---|---|---|
| 0 to 0.85M (10 chains) | 8.0M | 8.1M | 5,770 | 5,870 |
| 0.85M to 2.0M | 22.0M | 22.7M | 6,210 | 6,420 |
| 2.0M to 3.4M | 28.0M | 37.9M | 5,360 | 7,260 |
| 3.4M to 3.7M | 6.0M | 11.2M | 4,700 | 8,760 |
| 3.7M to 5.3M (dense) | 32.0M | 166.1M | 3,000 | 15,570 |
| 5.3M to 7.39M | 41.8M | 121.6M | 4,610 | 13,410 |

Stage time, summed over the 82 ranges (the stages overlap, so the sum is more than the wall
time): fetch 26,429 s, decode and index 13,545 s, finish 8,344 s, commit 70 s.

Size per component of the finished store:

| Component | Files | Bytes |
|---|---|---|
| `txs.seg` | 82 | 95.96 GB |
| `blocks.seg` | 82 | 21.66 GB |
| `rk.idx` | 82 | 1.93 GB |
| `code.pst` | 82 | 2.55 GB |
| `bh.idx` | 82 | 1.17 GB |
| `xfer.pst` | 82 | 0.81 GB |
| `acct.pst` | 82 | 0.75 GB |
| `pk.pst` | 82 | 0.40 GB |
| `evt.pst` | 82 | 0.18 GB |
| `keys.ef`, `pact.pst`, `txs.off`, `blocks.conf`, `blocks.off`, `txs.conf`, `range.toml` | 82 each | 0.19 GB together |

The largest single `txs.seg` is 4.79 GB (a 100k-height range in the densest era); two ranges
are above 4 GB. Memory for a backfill peaks in the dense eras: `ranges_in_flight = 1` and a
`MemoryHigh` of at least peak plus 2 GB (`MemoryMax` plus 4 GB) is the setting this host needed.
Every block of the backfill was Merkle-verified as it was written (139,423,890 verified, none
unverified), and a later `verify merkle --all` over the whole store, one process per 250k-height
era and four at a time next to the running service, rechecked 139,427,260 blocks (139,279,460
frozen, 147,800 hot) in 84 minutes: every one `ok`, none `ok_unflagged`, `bad_flagged`,
`bad_legacy`, `bad_other` or errored, in every era. Peak process memory of one run was about
2 GB.

## Before any big write

Before a backfill, a merge of many ranges, a rebuild or a replica copy on a host with shared
storage, check the free space of the volume and, on Ceph-backed volumes, `ceph osd df`: do
not start if any OSD of the pool is above 75%.
