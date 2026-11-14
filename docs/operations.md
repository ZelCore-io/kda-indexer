# Operating kda-indexer

## Commands that write

`run`, `backfill`, `rebuild-range` and `unwind` (and the hidden `tail`) write to the data directory. Each one
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

## Behind a load balancer

To serve the compat routes through HAProxy next to chainweb-data, and move them one route at a
time with a rollback for each step, see [load-balancer.md](load-balancer.md). In short: bind
the compat, graphql and admin listeners to loopback plus the host's private address, set
`api.trusted_proxies` to the balancer's address, and let the balancer check `/readyz` on the
admin port. A host that is not ready leaves the pool, and its routes fall back to
chainweb-data. Drain the server on the balancer before a planned restart.

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

- A `serve` fixes its table list when it opens the store: one started before a P6 `run`
  created the additive tables (balances, keys, tokens, NFTs) keeps answering them as empty.
  Restart `serve` (on replicas too) once after the first P6 `run` start, and after any release
  that adds a table (M-12).

## Snapshots and replicas

`kda-indexer snapshot --out DIR` writes a consistent copy of a data dir. It only reads the
source, does not take the data dir lock and can run beside the live `run` (and beside the
freezer and merges); it holds the swap locks (`segments.swap.lock`, `search.swap.lock`) only
while it places files. The copy is a data dir: `serve --data-dir DIR` answers on it, and `run`
on it resumes the tail from the checkpoints it holds.

```sh
sudo install -d -o kda-indexer -g kda-indexer -m 0750 /opt/coins/data/kadena/kda-indexer/mainnet01.replica
sudo -u kda-indexer kda-indexer --config /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml \
    snapshot --out /opt/coins/data/kadena/kda-indexer/mainnet01.replica [--copy]
```

- Run it as the service user (`sudo -u kda-indexer`), the owner of the data dir. The volume
  root is owned by root (see [install.md](install.md)), so that user cannot create `DIR`
  there: root creates the empty `DIR` owned by `kda-indexer` first (the `install -d` line),
  and does nothing else. The command refuses to run as root on a data dir root does not own,
  before it opens or creates anything: as root it would read and write, by path, in a tree
  the service user controls. Run it as root only on a data dir whose whole tree root owns:
  as root it also refuses when `segments/`, or `DIR` if it exists, is not root-owned, and
  names each such path.
- Every file the manifest (or `search/meta.json`) lists must be a regular file. A symbolic
  link at a listed file's name, or anything else that is not a regular file, makes the
  command fail: with `--copy` the file is opened with `O_NOFOLLOW` and checked on the open
  descriptor, and the copy is created with `O_CREAT | O_EXCL | O_NOFOLLOW`; without `--copy`
  the hard link is made without following a symlink and must be the file checked before it.
  The swap lock files are opened with `O_NOFOLLOW` too: a symlink at `segments.swap.lock` or
  `search.swap.lock` is never followed and makes the command fail (without the lock a swap
  already in progress could pair files from both sides); remove it as the data dir's owner
  and run again. Any other failure to open a lock file (a read-only data dir where it cannot
  be created) is logged as a warning and the snapshot runs without that lock. Directories above a listed file (`segments/`, `search/`, a `rebuild.<S>/` of a
  pending swap) are opened as their paths resolve. The manifest's `network` must be a plain
  name (`A-Z a-z 0-9 . _ -`, no leading `.`, no `..`, at most 64 characters), so a listed
  name never points outside `segments/`.

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
- `search/` (when the source has one) comes last: its `manifest.json` and tantivy's
  `meta.json` are read once and the files of the segments `meta.json` lists are linked (or
  copied); a file a commit or merge removed first restarts this step (up to five reads).
  The copy's tail stage re-reads from the copied `built_through_height`.
- Never copied: `etl/`, `LOCK`, `merkle-unverified.jsonl`, `search.new/`, `search.lock`.
- `DIR/SNAPSHOT.json` records the format versions, network, frozen frontier, `next_txnum`, the
  tail checkpoint per chain, the manifest's blake3, the binary version and the creation time
  (no paths).
- The read transaction stays open for the hot copy only (minutes on a large store), not for
  the segment links or copies, so the source writer cannot recycle the pages that transaction
  still sees for that long. Do not start a snapshot when the volume is short of free space;
  run the checks of "Before any big write" first.

A local replica:

```sh
sudo install -d -o kda-indexer -g kda-indexer -m 0750 /opt/coins/data/kadena/kda-indexer/replica
sudo -u kda-indexer kda-indexer --config .../mainnet01.toml snapshot --out /opt/coins/data/kadena/kda-indexer/replica
sudo -u kda-indexer kda-indexer --config .../replica.toml segments check
sudo -u kda-indexer kda-indexer --config .../replica.toml serve   # replica.toml: data_dir of the snapshot, own api.* ports
```

Restoring a data dir (a snapshot, a copy on another host, a backup put back) always runs
`segments check` on it before its first `serve` or `run`. The check verifies the manifest and
the size and blake3 of every file each `range.toml` lists, so a copy that lost a file in
transit, or paired the old and new files of a range that was swapped while it was taken, is
refused before it answers anything. Start the service only when it passes.

To seed another host, take a hard-linked snapshot on the source volume (no `--copy`: on the
same volume `--copy` writes a second full copy of every segment file there; keep it for
another local filesystem), then
`rsync -aH SNAPSHOT_DIR/ other-host:/opt/coins/data/kadena/kda-indexer/mainnet01/` (`-H` keeps
the hard links that exist inside the copy). See "A second host from a snapshot" in
[install.md](install.md). A `run` on the copy catches up from the snapshot's checkpoints: it
follows the node block by block and resolves duplicate hot rows below the frozen frontier at
start, as after a crash during a freeze. A snapshot that is older than the node's history is
not a problem; one that is older than the tail can follow quickly is just slower to catch up.

## Code search index

`run` keeps `search/` (spec §3.8) itself: when `search/manifest.json` is missing and backfill
is done it builds the index in the background (status line `search rebuild d/n`), then its
tail stage adds the body of every new exec tx it has not indexed yet, committing every
`[search] commit_secs`. `serve` only reads it. Search failures are logged and retried after
a minute; they never stop ingest.

```sh
kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml search rebuild --dry-run
kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml search rebuild [--mode full|fallback]
kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml search query 'coin.transfer'
```

- `--dry-run` measures the exact number of distinct bodies (census), samples 1% of them per
  range (average length, module share), builds a trial index of the first 1,000,000 bodies
  (`--trial-docs`) in `search.dryrun/` (removed afterwards) and prints the projected size and
  the S-1 decision: `full` when the projection is at most 40 GB, else `fallback` with the
  smallest `fallback_min_len` whose projection is at most 30 GB. Set `[search] mode` and
  `fallback_min_len` from it.
- `search rebuild` is read-only on `hot/` and `segments/`, so it can run beside the service
  (as the service user, `nice -n 19 ionice -c3` on a busy host). It holds
  `segments.swap.lock` shared while it runs, so a `rebuild-range` waits for it (see
  "Rebuilding a range or step"). It writes `search.new/`, then
  swaps it in; the running `run` notices the new `built_at` and reopens. One rebuild at a
  time (`search.lock`). It is a big write: run "Before any big write" first.
- `[search] build_threads` and `heap_mb` set the rebuild's threads and tantivy writer heap;
  the tail stage uses one thread and 64 MB.
- Periodic rebuild: `run` rebuilds the index in-process when it is `rebuild_days` old or when
  readers' upper expansion window (tip − `rebuilt_through_height` in `search/manifest.json`)
  reaches `rebuild_window_heights`, whichever comes first (defaults 7 days, 30,000 heights;
  checked every 10 minutes, at most one attempt an hour). It keeps the live index's mode
  (or an explicit `mode = "full"` / `fallback` with `fallback_min_len`) and the configured
  threads and heap, so plan for the rebuild's memory beside the service. The tail stage keeps
  serving the live index meanwhile; the swap and every stage commit take
  `search.swap.lock`, so they never interleave. `periodic_rebuild = false` turns it off
  (then run `search rebuild` by hand when hits get slow).
- Alert when `kdx_search_built_through_height` trails the tip by more than 20 heights for 15
  minutes (a failing or backed-off tail stage; its errors are in the log). Meanwhile compat
  `/txs/search?search=` runs the bounded scan and native `/search/code` answers
  `partial: true` (`kdx_search_rejected_total{reason="lag"}` counts it).
- Deleting `search/` and restarting the service rebuilds it in the background; nothing else
  depends on it.

## Balances

`run` keeps the balance tables itself (spec §3.9, stage 8): when `Meta balances_seeded` is
absent and backfill is done it runs the seed, then a reconciler reads every dirty row from the
node's `/local` and writes the balance, guard and key rows. `serve` only reads them. Turn it off
with `[balances] enabled = false`.

- **Seed** (no node calls): marks every known `(account, module, chain)` dirty: both sides of
  every `TRANSFER` (`xfer` postings) on each chain they touch, and every account seen in any
  event (`acct` postings) for `coin` on each chain it touches. It walks the hot tail, then the
  segments, and writes 100,000 rows per commit with a resume cursor, so a restart continues
  where it stopped. Mainnet: roughly 1–2M rows.
- **Reconcile**: up to 256 dirty rows per batch, chosen fresh for every batch: tail marks first
  (`coin` first, then at most `[balances] module_share` keys per other module, round-robin),
  the seed backlog (`BalanceSeed`, `coin` first) only behind them; through a node client of its
  own at `[balances] local_rps` calls per second (default 50) with `local_concurrency` in
  flight (default 8). Each call is `({module}.details (read-msg "a"))` with the account in the
  request data, gas limit 150,000; keyset references are resolved with `describe-keyset`.
  Mainnet first pass: about 6–12 h at 50/s; everything else runs meanwhile.
- **Marks**: every tail block marks the balances it may change (`TRANSFER` sides, every account
  of its events for `coin`), and a reorg's undo marks the unwound blocks' rows again. A row
  that is dirty is served with `stale: true` until it is re-read.
- **Errors**: a node error (timeout, connection, HTTP status) keeps the row dirty and moves it
  to the back of the queue; the reconciler backs off (up to 60 s) only when node errors are at
  least half of a batch, and a row that fails 5 times in a row is parked (`result="parked"`,
  mark dropped until its next tail mark). It never becomes a zero balance. A Pact failure (for
  example a token module without a `details` function) or an unusable answer (not JSON, too
  large, no decimal balance) drops the mark it read (a newer mark survives) and keeps any stored row, flagged
  unverified (served `stale: true`), with no back-off. A module name that fails the safety
  check is skipped without a call. A node outage parks nothing: retry counts only run in
  batches that do not back off.
- **Trust**: a guard adds the account to `/keys/{pk}/accounts` only for `coin` and for a module
  whose `describe-module` lists `fungible-v2` on that chain (checked once per module and chain
  through `/local`, re-checked every `[balances] recheck_hours`, default 24). Any other module's
  balances are stored and served, but its guard indexes no key, and its keyset references are
  not resolved.
- **By-key listing** (SEC-2): `GET /keys/{pk}/accounts` lists only the modules in
  `[balances] key_modules` (default `["coin"]`; each entry must be `name` or
  `namespace.name`). A confirmed `fungible-v2` module still only proves the interface: its
  `details` can report any balance under any key. Another module's rows are listed only when
  a client asks with `?token=<module>`, and only on chains where that module is a confirmed
  `fungible-v2`. Each row carries `trusted` (the module is in `key_modules`). Adding a module
  to `key_modules` takes a restart of `serve`/`run`; the stored rows do not change.
- **Not fungible**: a module whose `details` calls all fail in 3 batches in a row is marked
  not fungible: the tail stops marking it, its dirty rows are dropped without a call
  (`result="not_fungible"`) and `/tokens` hides it until the re-check `recheck_hours` later.
- **Caps**: an untrusted module gets at most `[balances] module_rows` rows (default 1,000,000;
  then the tail marks no new key of it) and an account at most `account_rows` untrusted rows
  (default 1,000); `coin` and confirmed `fungible-v2` rows are always kept
  (`result="capped"`).
- **Marks off**: with `[balances] enabled = false` the tail writes no marks. Each chain keeps a
  watermark of the last block applied with marks; when the tail applies a block past it
  (balances were off, or an older binary ran on the store) the reconciler re-runs the seed, so
  rows changed meanwhile are re-read (a full pass behind the tail marks).

```sh
kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml reconcile-balances --status
# service stopped (both take the data dir lock):
kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml reconcile-balances --seed
kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml reconcile-balances --all
```

- `--status`: dirty rows, balance/absent/guard/key/token counts, seed state (`seeded_height`,
  `seed_in_progress`), the reconciler's rate, ETA and heartbeat age. Read-only; runs beside the
  service.
- `--seed`: runs (or resumes) the seed walk now; the next `run` reconciles what it marked.
- `--all`: marks every stored balance row dirty, so the next `run` re-reads them all.
- `--nft-fill` (service stopped): runs the NFT history fill again (below), then drains the
  marks it made with `get-balance`. `--status` also prints `nft_rows`, `nft_dirty_rows` and
  `nft_filled_height`.

### NFTs

The same stage keeps `NftBalances` (spec §3.9). Once per store (`Meta nft_filled`), before the
balance seed, it walks the `evt` postings of every event name of a trusted ledger (`[nft] ledgers`, default `marmalade.ledger` and `marmalade-v2.ledger`;
hot tail first, then the segments) and applies each tx's events: a v2-shaped `RECONCILE` sets
both parties' balance, other ledger events mark `NftDirty`. The fill makes no node calls and
takes seconds. After that the tail applies the same rules to every block, a reorg's undo marks
the rows of the unwound txs dirty, and the reconciler reads `NftDirty` rows with
`({ledger}.get-balance (read-msg "t") (read-msg "a"))` (same `/local` budget and safety rules
as balances) between its balance batches. `kda-indexer inspect nft-check --count 20
--compare-node` samples stored rows and compares them with the node.

For GraphQL (GQ-15) the reconciler also fills `NftDetails`: each loop walks up to 4,096
`NftBalances` rows from a cursor that wraps, and makes at most 32 calls in the same `/local`
budget. The calls are `({ledger}.details (read-msg "t") (read-msg "a"))` for a positive holding's
guard and `({ledger}.get-token-info (read-msg "t"))` for its token's info. A row is read again
once its balance row moves. An unusable answer is stored as failed until then, with no back-off.
A node error stores nothing and, when it is the majority of a step's calls, the cursor stays.
`reconcile-balances --status` prints `nft_details_rows`. On a store from before P7 the first `run` creates the
table, and the first laps fill it; until then `guard`/`info` answer `NOT_AVAILABLE`. A `serve`
opened before that `run` sees the table only after a restart, as with the P6 tables.

- The fill records the ledger set it walked (`Meta nft_filled_ledgers`). A ledger added to
  `[nft] ledgers` later is walked on the next `run` start: only that ledger's events, from its
  postings (hot tail and segments), with no node calls. `reconcile-balances --nft-fill` walks
  every trusted ledger again.
- A failed fill (for example one undecodable record) is logged (`nft: history fill failed`)
  and retried an hour later; the balance seed and reconciler run meanwhile.
- `n_4e470a97222514a8662dd1219000a0431451b0ee.ledger` (a v2-shaped ledger in a principal
  namespace, seen on chain 1) is not trusted by default: it is not the `marmalade-v2`
  deployment. Add it to `[nft] ledgers` to index it; its rows keep its own ledger id.
- Metrics: `kdx_balance_dirty_rows`, `kdx_balance_reconcile_total{result="ok|absent|error|skipped|parked|capped|not_fungible"}`
  and `kdx_balance_reconcile_seconds`. A dirty count that only grows while the rate is zero
  means the node is refusing `/local` calls (see the log line `balances: node errors`).

## Rebuilding a range or step

`kda-indexer rebuild-range --step S` (or `--range S` for a 100k range) refetches the heights of
the manifest entry that starts at `S` from the node and swaps the rebuilt files in. Use it to
repair a file that `segments check` reports (blake3 or size), and for deep-reorg recovery.
Stop the service first: it takes the data dir lock.

- The rebuild is built in `segments/rebuild.<S>/` with the backfill writer. A payload Merkle
  mismatch aborts it (legacy-era mismatches are stored unverified, as in a backfill).
- The tx and block counts and the first TxNum must equal the manifest entry, or nothing is
  swapped. The content digest (decoded records, names resolved) of the stored and rebuilt
  files is compared, and the blake3 of every stored and rebuilt file is logged. A store
  rebuilt with its own dictionary gives equal bytes; across stores only the content is
  comparable (`segments digest --content`).
- The rebuilt range must link to its neighbours: the first block of each chain must name the
  stored last block before the range as its parent, and the stored first block after it (the
  next entry, or the hot store) must name the rebuilt last block. Otherwise nothing is
  swapped; a reorg that spans two steps needs both rebuilt, starting with the later one only
  after the earlier one links.
- When the content differs (or the stored range was unreadable), the rebuild marks dirty the
  balance rows of every account and module in the transfers of both the old and the new
  content, and marks the search index stale from `S` (`stale_from_height` in
  `search/manifest.json`). Until a search rebuild runs, native `/search/code` answers
  `partial: true` and compat `/txs/search` scans instead of trusting the index; the periodic
  rebuild treats the mark as due, or run `kda-indexer search rebuild`. The report's
  `dirty_marks` and `search_marked_stale` say what was done.
- `--dry-run` builds and compares, prints the report and removes the rebuild. It writes
  nothing to the hot store, but like every writer start it first finishes or drops other
  ranges' interrupted swaps.
- The swap is crash-safe at every point: before the journal `rebuild.<S>/rebuild.json`
  exists the old files stay; after it, readers use the rebuilt set, and the next writer start
  (`run`, `backfill`, `rebuild-range`) finishes the swap. `segments check` passes in between.
- Restart `serve` after every `rebuild-range` that swapped (`"swapped": true`). A running
  `serve` keeps the files and indexes it opened before the swap and opens others lazily, so it
  can pair an old index with a new file of the swapped range (wrong rows or decode errors).
- `snapshot` and the swap exclude each other through `segments.swap.lock` in the data dir: a
  snapshot waits for a swap in progress, and a swap waits for a snapshot to place its
  segment files.
- A `search rebuild` started by hand holds `segments.swap.lock` shared from start to end. A
  `rebuild-range` started meanwhile blocks at its start, before it fetches anything, until
  that search rebuild finishes, which can take hours on mainnet: run the two one after the
  other. If a search rebuild starts while the range is being fetched and built, the
  `rebuild-range` waits again before its dirty and stale marks and the swap. Each wait logs
  an info line `rebuild: waiting for segments.swap.lock ...` when it begins and
  `rebuild: got segments.swap.lock after N s` when it ends. The background search rebuild of
  `run` does not take this lock (it cannot overlap a `rebuild-range`, which needs the data
  dir lock).
- `--step S` works only where the manifest still lists that 10k step. Once its steps are
  merged into a 100k range, `rebuild-range` matches an entry by its exact start and kind, so
  `--step` inside the range fails and `--range S` (the whole range) is the only option for
  those heights. A range rebuild refetches 100,000 heights of 20 chains (2M blocks): at the
  backfill rates of "Measured mainnet backfill" below, about 6 minutes in the sparse eras and
  about 11 minutes in the dense 3.7M to 5.3M era, plus the digest compare. Until the swap, the
  rebuilt files sit next to the old ones in `segments/rebuild.<S>/`: the largest mainnet
  range has a 4.8 GB `txs.seg` (B10; the next is 4.3 GB), so check free space first.

Compare stores with `segments digest --content` (per entry, plus a total that does not depend
on the range/step split) and `hot stats --digest` (name-resolved: records, postings by name,
dictionaries as name sets). Between two stores built separately, only these `hot stats
--digest` lines must match: `records`, `TailPostings`, `TailRk`, `HeaderByHash`,
`StageCheckpoint` and `Counters`. The `Undo` line hashes raw bytes that hold entity ids, which
differ whenever the stores assigned ids in another order, and the `Dict*` name sets differ when
one store interned names from blocks that were later orphaned.

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

Size and memory notes: the two 100k ranges whose `txs.seg` is above 4 GB are heights 4.4M to 4.5M
(4.79 GB) and 4.5M to 4.6M (4.30 GB); they verify and read normally, and the range size stays
100k. ≥ 32 GiB RAM recommended for a full backfill beside a node (this 24 GiB host swapped in the
dense eras).

## Before any big write

Before a backfill, a merge of many ranges, a rebuild or a replica copy on a host with shared
storage, check the free space of the volume and, on Ceph-backed volumes, `ceph osd df`: do
not start if any OSD of the pool is above 75%.

## Fuzzing

`fuzz/` holds cargo-fuzz targets for everything that parses bytes it does not control: `payload_decode` (node payload and output JSON), `record_decode`, `value_decode`, `postings_list`, `ef_keys`, `phf_open`, `manifest_parse` (`manifest.json`, `range.toml`, the search `manifest.json`), `api_tokens` (cursors and Relay ids), `like_match`, `xff_parse` and `graphql_request` (a POST body against an empty store). The crate has its own workspace and a nightly toolchain, so the default build does not see it. Build with `cd fuzz && cargo +nightly fuzz build`; `cargo run --bin gen_seeds -- DIR` writes seed corpora to `DIR/<target>/` from our own writers (nothing is fetched); then run `cargo +nightly fuzz run TARGET DIR/TARGET -- -max_len=4096 -timeout=2`. Corpora and artifacts stay out of the repository. A minimised crash input becomes a regression test in the crate it hit.
