# Operating kda-indexer

## Commands that write

`run`, `backfill`, `rebuild-range`, `unwind` and `segments add-index` (and the hidden `tail` and `hot drop-family`) write to the data directory. Each one
takes an exclusive lock on `<data_dir>/LOCK` and exits at once with an error if another
kda-indexer process holds it. Stop the service (`systemctl stop kda-indexer-<network>`) before running
`backfill`, `unwind` or `segments add-index` by hand. Read-only commands (`inspect`,
`segments list|check|digest`, `hot stats`) do
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

## Adding the sender index and key filters to an existing store

A store written before FIX-2 has no `sndr.pst` (the sender index) and no `kf.fil` (the
in-memory key filters) in its ranges and steps, and its hot tail has no `sndr` rows. It runs
as before: `transactions(accountName)` keeps today's scan with its 1 s cap and every lookup
probes every range. `segments add-index` adds the missing pieces once, with the service
stopped:

```sh
systemctl stop kda-indexer-mainnet01
kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml segments add-index --jobs 4 > add-index.json
systemctl start kda-indexer-mainnet01
```

- It takes the data-dir lock and `segments.swap.lock` (a snapshot placing files makes it
  wait, with a log line). It first finishes or drops any interrupted swap, as every writer
  start does.
- Picked: every manifest entry whose `sndr.pst` is not listed in its `range.toml` or listed
  but missing, and every entry whose `kf.fil` is not listed, missing or not trusted (a
  rollback's old binary deletes both files at startup while `range.toml` keeps listing them).
  Only the missing files are built.
- Per entry: it walks `txs.seg` and `blocks.seg` once in order, writes the missing files and a
  new `range.toml` into `segments/rebuild.<start>/`, checks every sender list against the
  `acct` list of the same account and every key against the new filter, then swaps them in
  with the `rebuild-range` swap (journal, renames, `range.toml` last). Only renames: hard
  links to the old files (a replica, a snapshot) keep the old inodes. The bytes equal what
  the writer would have written for the range.
- `--jobs N` (default 4) entries run in parallel, each independent. Memory per job is about
  8 B per sender posting plus 8 B per request key and block hash of the entry, plus the
  filter under construction (the largest mainnet entry, 14.3 M txs, stays under 0.6 GB).
- Then the hot pass (one write txn): a `sndr` row and its undo op for every hot tx that
  lacks one, and `Meta sndr_hot = 1`. It runs even when the key is already set (it adds
  nothing to a complete tail and takes seconds), so it also repairs a tail an older binary
  extended (see the rollback section below).
- The dictionary is only read. A sender that is an indexable account but has no id fails
  that entry (nothing swapped, exit 1); the other entries still complete.
- `--only sndr` / `--only filter` builds one of the two (`--only filter` skips the hot pass).
  `--dry-run` builds and checks every entry in `<data_dir>/etl/add-index-dry.<start>/`,
  removes it and reports sizes; it swaps nothing, writes nothing to the hot store and takes
  no swap lock (it still takes the data-dir lock).

**Time.** One INFO line per entry gives its txs, seconds and an ETA (the first finished
entry's seconds per tx × the txs left ÷ jobs). Expect roughly 12.5 µs per tx of CPU, so
`txs ÷ jobs × 12.5 µs`: for mainnet's 370 M txs about 20 min at `--jobs 4`. Reading is
sequential and not the limit. If the first entry's rate points much higher, stop it.

**Resumable.** Stopping it (Ctrl-C, kill, a crash) at any point is safe: an entry is either
untouched, or its committed swap is finished by the next writer start (`run`,
`rebuild-range`, `add-index` itself). Run the same command again; it skips the complete
entries and does nothing on a finished store. The JSON report lists per entry `txs`,
`senders`, `sndr_bytes`, `filter_bytes`, `seconds`, the hot pass and the totals; the exit
code is 1 if an entry or the hot pass failed.

Check afterwards with `segments check --deep`. It verifies `sndr ⊆ acct`, that the sender
index is complete (every tx with an indexed sender has its `sndr` posting and, with the hot
dictionary, each `sndr` list is exactly the txs its account sent), and every key against its
filter. The sender index is used for queries only once every range has its `sndr` lists and
the hot pass has set `Meta sndr_hot` (`hot stats` shows it; the `kdx_sender_index_ready`
gauge is 1 from the start).

**A damaged `sndr.pst`.** A listed `sndr.pst` whose header does not decode fails the whole
store open (`run` and `serve` exit, naming the range): the sender lists must not
silently lose a range. Recovery, with the service stopped: delete that `sndr.pst` and run
`segments add-index` again (a listed-but-missing file is rebuilt). A damaged or missing
`kf.fil` only turns that range's filter off (WARN, `kdx_key_filter_untrusted`); the
same rerun rebuilds it. If the writer cannot build a filter at all, it writes the step or
range without `kf.fil` (WARN) rather than stall the freeze or merge.

## Rolling back to a build before FIX-2

From FIX-2 on, the tail writes the `sndr` posting family (family 6, the txs an account sent;
see [format.md](format.md)) into the hot store, and every range gets `sndr.pst` and `kf.fil`.
`hot drop-family sndr` is **required** before installing an older package. Offline, with the
new binary:

```sh
systemctl stop kda-indexer-mainnet01
kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml hot drop-family sndr
```

It takes the data-dir lock and runs one write txn: it deletes every `TailPostings` row of
family 6, strips the matching `UndoOp::Posting` ops from the `Undo` rows (so an unwind by the
old binary finds nothing it does not know) and deletes `Meta sndr_hot`. It prints the counts.
Only `sndr` can be dropped; the required families are refused. A data dir without a hot
store is refused (exit 1); nothing is created. Then install the old package and start the
unit.

The older binary reads the segments as before. At its first start it deletes `sndr.pst` and
`kf.fil` (its cleanup does not know them) while `range.toml` keeps listing them, so its
`segments check` reports them missing. That is expected.

**If `drop-family` was skipped.** The old binary's freezer stops with `posting family` at the
first family-6 row: nothing is frozen and the hot tail keeps growing. The tail rows it writes
have no `sndr` postings while `Meta sndr_hot` still says 1, so the sender index has a gap. Run
`hot drop-family sndr` with the new binary to get the old one freezing again, or upgrade
again.

**Going forward again** (either way): install the new package and run `segments add-index`
before starting the unit. It rebuilds the deleted `sndr.pst` and `kf.fil`, and its hot pass
(which always runs) adds the `sndr` rows any tail block lacks. The new freezer never copies
`sndr` postings from hot rows: it derives them from the records, so a step frozen over a
gappy tail is still complete. `segments check --deep` then proves the index complete.

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

## `/stats` freshness

The compat `/stats` (and GraphQL `networkInfo`) never counts on the request. A background task
started with the server recounts the user transactions, starting each count 2 minutes after the
previous one ended (counts never overlap), and stops with it: an in-progress count is abandoned
on shutdown, so the exit does not wait for the walk. Otherwise a count runs to completion,
however long it takes. A request answers the last count with `X-Count-Age: <seconds>`
since that count started (the snapshot the count read), so the header stays under the 2 minute
interval plus one count time when refreshes succeed and grows when they do not. There is no age after which the count is
refused.

- A refresh that takes over 60 s logs one `stats: count refresh was slow` WARN with its
  duration when it ends; the age grows meanwhile and requests keep the last count. A refresh
  that fails keeps the last count and logs one `stats: count refresh failed` WARN per 10
  minutes. A repeated slow WARN means cold pages: check the disk and the `read_threads` load.
  A request that finds the count older than 6 minutes (3 intervals) logs one
  `stats: the count is stale` WARN per 10 minutes: a count is stuck or failing.
- Right after a start there is no count yet. A request waits up to the API deadline, then
  answers `503` with `Retry-After: 5`; GraphQL `networkInfo` answers the same error at once.
  Both clear when the first count finishes, which takes as long as one walk of the hot window.
  A failed first count is retried after 5 s (later counts after 2 minutes).
- The transaction count in a `/stats` answer can be up to the refresh interval plus one count
  behind the store (`X-Count-Age` measures from the start of that count); the supply is computed
  per request.

## Swagger UI (`/docs`)

The native and compat listeners serve Swagger UI at `/docs`, with the OpenAPI documents of both
APIs at `/docs/openapi.json` and `/docs/openapi-compat.json`. Everything is embedded in the
binary (no CDN, nothing fetched at runtime) and read-only. `[api] docs_enabled = false` turns it
off: `/docs` and every file under it answer 404 (a restart is needed). The admin listener
(`/healthz`, `/readyz`, `/metrics`) and the GraphQL listener never serve it. Behind a load
balancer, route `/docs` the way you route the listener's other paths, or leave it unrouted so the
public name does not expose it; the native listener is internal by rule. The page costs about
2 MB of response bytes on the first load (cached for a day by the browser) and counts against the
per-client rate limit like any request. The embedded UI is swagger-ui-dist 5.34.1 (Apache-2.0,
see `THIRD_PARTY_NOTICES`).

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
| `kdx_segment_optional_missing_total` | file (sndr, kf): a file `range.toml` lists is missing at range open |
| `kdx_key_filter_untrusted_total` | none: a `kf.fil` failed its checks at open; the range is probed without it |
| `kdx_key_filter_bytes` | none: key filter memory of the newest segment view |
| `kdx_sender_index_ready` | none: 1 when sender lists read the `sndr` postings (every range has `sndr.pst`, hot `sndr_hot = 1`), 0 while they use the old scan; `[graphql] sender_index = false` forces the old scan whatever this shows |
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
  as root it also refuses when `segments/`, `hot/`, `search/`, or `DIR` if it exists (even
  empty), is not root-owned, and names each such path.
- Every file the manifest (or `search/meta.json`) lists must be a regular file. A symbolic
  link at a listed file's name, or anything else that is not a regular file, makes the
  command fail: with `--copy` the file is opened with `O_NOFOLLOW` and checked on the open
  descriptor, and the copy is created with `O_CREAT | O_EXCL | O_NOFOLLOW`; without `--copy`
  the hard link is made without following a symlink and must be the file checked before it.
  The swap lock files are opened with `O_NOFOLLOW` too: a symlink at `segments.swap.lock` or
  `search.swap.lock` is never followed and makes the command fail (without the lock a swap
  already in progress could pair files from both sides); remove it as the data dir's owner
  and run again. Any other failure to open a lock file (a read-only data dir where it cannot
  be created) is logged as a warning and the snapshot runs without that lock. Only the
  symlink form is closed this way: a race that remains (a directory planted at the lock
  path, or the lock file renamed away) needs write access as the service user, so it crosses
  no privilege boundary, and `segments check` on restore catches a mixed set. Directories
  above a listed file (`segments/`, `search/`, a `rebuild.<S>/` of a pending swap) are opened
  as their paths resolve. The manifest's `network` must be a plain name (`A-Z a-z 0-9 . _ -`,
  no leading `.`, no `..`, at most 64 characters), so a listed name never points outside
  `segments/`.

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

Snapshots taken before the audit fix F-02 (PR-5) lack the optional files of every range that
has them (`*.sndr.pst`, the sender postings, and `*.kf.fil`, the block-hash and request-key
filters): the snapshot placed them and then deleted them again. `segments check` on such a copy
fails on those files. Take a new snapshot with a fixed binary, or keep the copy and rebuild the
files there with `segments add-index` (see above), then run `segments check` again. A host
already restored from such a snapshot serves sender lists by the slow fallback until then.

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
- **Branch check**: an answer clears a row's mark only when it was read on the block the
  indexer holds at that height: the answer's `metaData.prevBlockHash` must equal the stored
  header's hash at its `as_of`. Right after a quick reorg flip-and-back the node can still
  answer from the orphan's state at the same height; such an answer (or one read above the
  indexer's tail) is stored flagged unverified (served `stale: true`) and the row stays queued
  at the back. When the indexer holds another header at that height (a definite other
  branch) and already has a row, that row (balance, guard, public-key index) is kept and only
  flagged unverified. Such answers count as `result="other_branch"` (in addition to `ok`).
  When every answer of a batch was off-branch, the reconciler waits 2 s before the next
  batch; it never stops re-reading. It logs a WARN when answers carry no
  `metaData.prevBlockHash` (at most once a minute: a node or Pact change makes every row
  unverifiable), and when off-branch answers are more than half of at least 50 `ok` answers
  in a 60 s window (node off our cut, a balancer in front of several nodes, or a lagging
  tail). An "absent" answer carries no block metadata and cannot be checked: a row the
  node reports absent from an orphan's state is stored as absent until the account's next
  posting on that chain or `reconcile-balances --all`.
  NFT rows (`get-balance`, `NftDirty`) get the same check: an off-branch answer keeps the
  mark (served `stale: true`) and does not replace a row already held. The NFT holding guards
  and token info (`NftDetails`, GraphQL only) are still checked by height alone (TODO D2):
  they are re-read whenever the balance row's `as_of` moves, and rarely differ between
  sibling blocks.
- **Errors**: a node error (timeout, connection, HTTP status) keeps the row dirty and moves it
  to the back of the queue; the reconciler backs off (up to 60 s) only when node errors are at
  least half of a batch, and a row that fails 5 times in a row is parked (`result="parked"`,
  mark dropped until its next tail mark). It never becomes a zero balance. A Pact failure (for
  example a token module without a `details` function) drops the mark it read (a newer mark
  survives) and keeps any stored row, flagged unverified (served `stale: true`), with no
  back-off. An unusable answer (not JSON, too large, no decimal balance) never drops the mark:
  it is logged (`balances: details answer unusable; key kept dirty`), counted
  (`result="invalid"`) and moved to the back of the queue, and it does not count against the
  module's fungible check. A batch of only unusable answers pauses the next one for 30 s.
  Balance text is accepted up to 1,024 bytes: Pact decimals carry up to 255 places, so a
  full-precision balance (for example `free.wiza`, about 260 bytes) is stored exactly. A module name that fails the safety
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
  (default 1,000); `coin` rows are always kept (`result="capped"`). A confirmed `fungible-v2`
  module other than `coin` is not counted against an account's cap but has its own row cap,
  `fungible_module_rows` (default 1,262,460: 10x the largest mainnet token, `free.finux` with
  126,246 rows in Nov 2026), since anyone can deploy one. `--status` shows both caps,
  `largest_module_rows` and `capped_modules`; a module at its cap is logged at WARN. As with
  `module_rows`, the capped flag is sticky: raising the cap later does not resume the module
  (no command clears the flag yet).
- **Guard limits**: a `details` guard or `describe-keyset` answer over 4096 bytes of JSON is
  stored as null (the balance row is still written) and counted in `--status`
  `guards_capped`. A keyset of more than 64 keys, or with a key over 256 bytes, gives no
  `KeyAccounts` rows (WebAuthn keys reach 191 bytes on mainnet).
- **Marks off**: with `[balances] enabled = false` the tail writes no marks. Each chain keeps a
  watermark of the last block applied with marks; when the tail applies a block past it
  (balances were off, or an older binary ran on the store) the reconciler re-runs the seed, so
  rows changed meanwhile are re-read (a full pass behind the tail marks).

```sh
kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml reconcile-balances --status
kda-indexer -c /opt/coins/kda-indexer/mainnet01/config/mainnet01.toml reconcile-balances --seed --dry-run
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

#### Receivers pass (D1)

Before coin v3 some KDA credits named the receiver in no event, so no posting marks its row.
After the seed, once per store (`Meta balances_seed_receivers`), the receivers pass walks every
stored user tx below the network's coin v3 height and marks `(receiver, coin, chain)` dirty:
below the Pact events fork, every account-like string in `exec.data` and every string literal
in `exec.code`; at any height, the receiver of a `coin.transfer-crosschain` finish. A key that
already has a `Balances` row is skipped (the tail keeps it correct). Fork heights are per
network; a network this build has no heights for uses mainnet01's and logs a warning.

- **It blocks the reconcile loop while it walks the segments**: the tail's dirty rows wait until
  it ends (minutes to a few hours on mainnet, plus one `/local` read per marked row afterwards).
  Its progress is logged every 1M txs.
- Count it first, read-only (no lock, beside the service):
  `reconcile-balances --seed --dry-run` prints `candidates`, `new_accounts`, `skipped_existing`
  and `dirty_inserted` (rows it would mark). The posting seed does not run.
- Then run it at a quiet hour (the service runs it by itself), or with the service stopped:
  `reconcile-balances --seed`.
- It refuses a store with no tail yet or an incomplete backfill (the service postpones it to the
  hourly recheck).
- `errors` must be 0. Each unreadable tx is logged with block height, chain and request key;
  `--seed` exits non-zero when any was found and `--status` prints `seed_receivers_errors`
  (with `seed_receivers_height` and `seed_receivers_in_progress`).
- To force a rerun, delete Meta `balances_seed_receivers`.

#### Token receivers pass (D3)

Some credits name the receiver in no TRANSFER event, for coin and for tokens alike, so no
posting marks its row. Since fix round 1 the **tail marks them itself**, in the block's txn,
with the same rules as the pass (one shared extraction, `kdx_decode::receivers::d3_candidates`
then `d3_filter`): each row is marked at the tx's height through the postings' mark path, with
no verified-row skip, and a reorg's undo marks it again like an undone posting. The tail keeps
the per-tx caps below and allows one emitter at most 1,024 rows per block; more are dropped and
counted (counter `d3_tail_dropped`), and the block is applied regardless. The tail interns no
names: decode interns the event accounts (postings), a cross-chain defpact's module and account,
and the modules a tx may put forward as tokens, so backfill and tail build the same dictionary.
A row whose account or module is still unknown (for example a string of an `exec`) is left to
the hourly pass, which interns it.

The token receivers pass marks the same rows for stored history, with its own flag
(`Meta balances_seed_token_receivers`), cursor and error count, and the same mechanics as D1
(resumable, `stop` polled per chunk, refused until the backfill is complete and the tail has
started). It walks every stored height, not only those below coin v3. Each row is marked at the
height of the latest tx that names it, like a posting; a verified `Balances` row is skipped only
when it was read at or above that height (`as_of`), so a row read before the credit is re-read.
After the first walk it is pending again whenever the tail grew, and the service's hourly
recheck walks only the new heights (a safety net now that the tail marks). Candidates:

- an injected early miner account (a dictionary name holding a `"`, mainnet 2020): the
  accounts its coinbase really credited, as coin rows on that block's chain only;
- a cross-chain defpact of any module (`<module>.transfer-crosschain`): step 1 credits
  `args[1]`, step 0 debits `args[0]`. Tokens emit no TRANSFER for either;
- an `exec` below the events fork: the D1 strings plus every string of `result.data`, for coin
  and for each tracked token the code calls; from the events fork on, the same strings for a
  tracked token the code calls only when the tx has no event of that token;
- an event of a tracked token that is not a well-formed TRANSFER (`WRAP`, `UNWRAP`, `MINT`,
  ...): its string params, for that token;
- an event param holding a `fungible-v2` module reference: the event's string params, for the
  referenced module, when the event's module is that module, a module of the same governed
  namespace (not `free` or `user`), or a tracked token naming another tracked token.

Token discovery: a cross-chain defpact's module, and a modref named by its own module or a
module of the same governed namespace (`kaddex.skdx` is only ever minted by
`kaddex.alchemist`, so no TRANSFER ever made it a token), are put forward as tokens, at most 4
per tx, 16 per tail block, and per pass 8,192 with at most 4 per namespace (anyone can create
`n_` namespaces; a flood of them must not crowd out a real token). A plain object renders exactly
like a modref, so a name is only queued (`Meta balances_d3_discover/<ns>/<chain>/<module>`, at
most 4 per namespace and 16,384 in all). In the open namespaces (`free`, `user`) the quota is
per module, so junk modules there cannot starve a real one. A discovery dropped at any quota is
counted (`d3_discover_dropped`) and kept in `Meta balances_d3_discover_pending/<chain>/<module>`
(at most 1,000,000; beyond that only counted, `d3_discover_lost`); the next pass re-queues these
first, so nothing is lost for good. Each re-queue scans from after the last pending key it
scanned (`Meta balances_d3_discover_pending_from`) and wraps around, so entries the queue keeps
refusing cannot hold back the rest. A discovered name longer than 256 bytes is dropped and
counted (`d3_discover_too_long`, and `discovered_too_long` in the pass's dropped report): it
could never be an MDBX key, and real module names are far shorter. The reconciler checks 4 queued names per
round with `describe-module` on that chain; a module that lists `fungible-v2` becomes a tracked
token, anything else is dropped. Note: a namespace whose user guard lets anyone deploy behaves
like `free`; add it to `OPEN_NAMESPACES` if one is found.

History of newly tracked tokens: when a module gets a `Tokens` row (first TRANSFER in the
tail, a confirmed discovery, or a reconciler check) whose first height is below the D3 done
height, it is queued in `Meta balances_d3_rewalk/<ModuleId>`: a credit below that height may
predate its tracking. A token first seen at or above the done height is covered by the tail
marks and the next pass, so it queues no walk. The walk reads the D3 rules for the queued tokens
only over heights `[0, done)`. In the service it runs in slices of 50,000 txs, one per
reconcile round, so tail marks, NFT steps and dirty rows keep flowing; each slice commits its
marks with the walk's cursor (`Meta balances_d3_rewalk_walk`: done height, tokens, next TxNum
or hot key, budget counters), so a restart resumes there. A new walk starts at most once a day
(`balances_d3_rewalk_at`); tokens queued during a walk wait for the next one. The CLI walks to
the end on every run.

Modules whose name fails the `/local` module check (B-1, for example a non-ASCII name that
Pact 5 cannot parse) are never marked, queued or tracked: the node API cannot read them.

Only successful txs give candidates. The injected-miner rule reads the block's miner data of
coinbases below the coinbase injection fix (mainnet 121,452), on that block's chain only.
Rows from events and from an exec's token calls are capped per tx: events give at most 64
rows plus 4 for each event with a candidate, never more than 1,024 (a batch tx such as a
kaddex staking-migration WRAP tx credits 45 accounts in one tx), at most 64 strings from one
event; an exec gives at most 64 (events are taken first). Amount params (`{"decimal": ..}`,
`{"int": ..}`, plain number strings) are never candidates. Then per pass a budget of distinct rows per tracked token (2,000,000,
`KDX_D3_MAX_EVENT_CANDIDATES` for the CLI; a repeated row costs nothing), one shared bucket for
all untracked emitters (2,000,000) and a global ceiling (20,000,000). Rows from an `exec`'s token
calls are charged to the tx's sender (100,000 per sender, 2,000,000 per pass), never to the
called token, so junk calls cannot spend a token's budget; the tail keys its per-block cap the
same way. Over a budget the excess is
dropped and counted for that token or bucket only, and the pass completes and advances for
everyone: `--status` shows `seed_token_receivers_dropped` (`tokens`: drops per token,
`untracked`, `ceiling`, `exec_sender`, `exec_ceiling`, `discovered`, `discovered_namespace`). A genuine token with drops needs a rerun with a raised
budget (delete the flag first). The budget counters persist with the cursor, so a resumed pass
does not grant a token its budget twice.

`reconcile-balances --seed` runs D1 and then D3. `--seed --dry-run` opens the store read-only
and prints both reports, `receivers` and `token_receivers`; `dirty_inserted` is the number of
node reads the pass would cause. `--status` shows `seed_token_receivers_height`,
`seed_token_receivers_in_progress`, `seed_token_receivers_errors`, `d3_discover_queued`,
`d3_discover_pending` and `d3_rewalk_pending`. To force a rerun, delete `Meta balances_seed_token_receivers`.

Run the first pass with the service stopped, or restart the service in a quiet window: the
service runs it inline at start (about 2.4 h on mainnet) and the reconcile loop waits for it.
After the repair, the verify scan must show 0 missing rows, including indirect token credits
with no event of the token after the events fork (another module crediting it, review L4).

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
- Metrics: `kdx_balance_dirty_rows`, `kdx_balance_reconcile_total{result="ok|other_branch|absent|error|invalid|skipped|parked|capped|not_fungible"}`
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
- Restart `serve` after every `rebuild-range` that swapped (`"swapped": true`) and after every
  `segments add-index`. A running `serve` keeps the files and indexes it opened before the
  swap and opens others lazily, so it can pair an old index with a new file of the swapped
  range (wrong rows or decode errors). It does not reopen a range by itself, even when the
  manifest is unchanged. A missed restart shows in the log: once a minute `serve` compares
  each range's `range.toml` with the one it opened and logs an ERROR
  `serve: range swapped on disk after serve opened it (rebuild-range or segments add-index);
  restart serve` with the range, once per range. Alert on it, or restart `serve` as part of
  every `rebuild-range`. `run` needs no restart: it holds the data dir lock, so neither
  command can run beside it.
- A reader that opens a range at the moment its swap renames the files can, rarely, fail the
  open or read a mix of old and new files of that range. The restart above covers it too: open
  no new `serve` during a swap, and restart running ones after it. (A per-open side choice and
  retry would close this; it is not built while the restart rule stands.)
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

`fuzz/` holds cargo-fuzz targets for everything that parses bytes it does not control: `payload_decode` (node payload and output JSON), `record_decode`, `value_decode`, `postings_list`, `ef_keys`, `phf_open`, `filter_from_bytes` (the key-filter file), `manifest_parse` (`manifest.json`, `range.toml`, the search `manifest.json`), `api_tokens` (cursors and Relay ids), `like_match`, `xff_parse` and `graphql_request` (a POST body against an empty store). The crate has its own workspace and a nightly toolchain, so the default build does not see it. Build with `cd fuzz && cargo +nightly fuzz build`; `cargo run --bin gen_seeds -- DIR` writes seed corpora to `DIR/<target>/` from our own writers (nothing is fetched); then run `cargo +nightly fuzz run TARGET DIR/TARGET -- -max_len=4096 -timeout=2`. Corpora and artifacts stay out of the repository. A minimised crash input becomes a regression test in the crate it hit.
