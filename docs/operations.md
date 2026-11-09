# Operating kda-indexer

## Commands that write

`run`, `backfill` and `unwind` (and the hidden `tail`) write to the data directory. Each one
takes an exclusive lock on `<data_dir>/LOCK` and exits at once with an error if another
kda-indexer process holds it. Stop the service (`systemctl stop kda-indexer`) before running
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
2. Find the chain and the fork height in the ERROR line (`hot stats` shows the counters).
3. If the stored branch must go, unwind it and clear the halt in one step:

   ```sh
   kda-indexer -c /etc/kda-indexer/config.toml unwind --chain 3 --to 7390500 --clear-halt
   ```

   `--to` must be at or above `frozen_frontier − 1`; frozen heights are never unwound. To
   only clear the halt (for example after the node has settled on the stored branch), leave
   out `--to`:

   ```sh
   kda-indexer -c /etc/kda-indexer/config.toml unwind --chain 3 --clear-halt
   ```

   `--clear-halt` deletes `halted.<chain>` only. It refuses when the chain's hot state is
   inconsistent: the `Tail` checkpoint has no stored header or a different hash, the tip has
   no undo record, or headers are stored above the checkpoint.
4. Start the indexer. The tail resumes the chain from its stored tip (or from the `Headers`
   checkpoint at `frozen_frontier − 1` when every hot block was unwound) and follows the
   node's current branch.
