# kda-indexer on-disk format, version 1

This document describes the files kda-indexer writes for frozen history: segment files,
their indexes and the record encoding inside them. Everything here is format version 1.
The data directory of one network looks like this:

```
<data_dir>/
  hot/            MDBX environment: dictionaries, live tail, undo, checkpoints (mutable)
  segments/       immutable range and step files + manifest.json
  etl/            temporary spool files while a range is being built (deleted afterwards)
```

Segments refer to names (accounts, modules, event names, public keys, module hashes) by
the dense integer ids stored in the `hot/` dictionaries. A `segments/` directory is only
readable together with the `hot/` store it was written with; copy both together.

Integers are little-endian unless stated otherwise. `varint` is unsigned LEB128.
`bytes` is a varint length followed by that many raw bytes.

## Identifiers

| Id | Definition |
|---|---|
| TxKey | u64 `height (32 bits) | chain (8) | position (16) | reserved (8)`; position 0 is the coinbase, 1..n the user txs in payload order. Big-endian when stored as bytes. |
| TxNum | Dense u64 sequence over frozen txs in TxKey order, coinbase included, starting at 0. |
| BlockOrd | `height × 20 + chain`. Inside a range the slot is `BlockOrd − start × 20`. |
| AccountId, ModuleId, EventNameId, PubKeyId, ModuleHash id | u32, dense from 0, in first-seen order; never reused or deleted. |
| CodeHash | blake2b-128 of an `exec` code body. |

Every name is interned when a block is indexed, before its records are encoded. An account
string is interned only when it is 1..=256 bytes (`coin` allows at most 256 characters);
longer strings are not accounts and get no id or posting. The `unindexed_accounts` counter
in the hot store's `Counters` counts them. In the hot store, `DictFwd` is keyed by the name,
or by `0xFF ‖ blake2b-256(name)` when the name is longer than 480 bytes, so no key reaches
the MDBX key-size limit.

Before the chain-graph change (height 852,054 on mainnet01) only 10 chains exist, so slots
of the missing chains are empty. Empty slots are allowed anywhere.

## Ranges, steps and the manifest

History is cut into height ranges `[start, end)` that cover all chains. A **range** spans
100,000 heights (written by the backfill, or by merging steps, zstd level 19). A **step**
spans 10,000 heights (written by the freezer from the hot tail, zstd level 9). Both use the
same files; `range.toml` says which kind it is.

`segments/manifest.json`:

```json
{
  "format_version": 1,
  "network": "mainnet01",
  "entries": [
    { "kind": "range", "start": 0, "end": 100000,
      "first_txnum": 0, "tx_count": 1234567, "block_count": 1000000 }
  ],
  "frontier": 100000,
  "next_txnum": 1234567
}
```

Entries are contiguous in heights and in TxNums. `frontier` is the last entry's `end`;
`next_txnum` is the TxNum the next step or range starts at. Only listed entries are read;
files of unlisted ranges are leftovers and are deleted or rebuilt.

## File names

`{network}-{file}-{start:09}-{end:09}.{ext}`, `end` exclusive, for example
`mainnet01-txs-007300000-007400000.seg`.

| file | ext | contents |
|---|---|---|
| `txs` | `.seg` (+ `.off`, `.conf`) | tx records in TxNum order |
| `blocks` | `.seg` (+ `.off`, `.conf`) | block records in BlockOrd order (present slots only) |
| `keys` | `.ef` | first TxNum of every slot |
| `rk` | `.idx` | request key → tx |
| `bh` | `.idx` | block hash → slot |
| `acct`, `xfer`, `pk`, `evt`, `code` | `.pst` | posting lists |
| `range` | `.toml` | range header with blake3 of every other file |

## Write ordering

1. Records are encoded and spooled to `etl/` (zstd level 1), sampled for the dictionaries.
2. All files are written into a temporary directory `segments/.tmp-{network}-{start}-{end}/`
   through a 16 MB buffered writer and `fsync`ed once each. `range.toml` is written last,
   after blake3 of every other file is known.
3. Every file is renamed into `segments/`, `range.toml` last; the directory is `fsync`ed.
4. `manifest.json` is replaced atomically (temp name, `fsync`, rename, directory `fsync`).

No file is modified after its rename. A crash before step 4 leaves files that the manifest
does not list; `segments check` reports them as orphans and the next build of that range
replaces them.

### Backfill commit order and resume

A backfill range commits in this order:

1. `RangeWriter::finish`: steps 1–3 above (files renamed into `segments/`, directory fsynced).
2. One MDBX write txn in the hot store: the dictionary names first interned by this range,
   the `Headers` stage checkpoint per chain (last height and hash of the range),
   `next_txnum`, `frozen_frontier`, and `Meta dict_lens@{end}` (dictionary size per kind
   after the range).
3. Step 4 above: `manifest.json` replaced.

The manifest is the source of truth. On start, backfill:

- deletes everything under `etl/` (raw spools and posting runs are never reused);
- deletes every file and temp directory in `segments/` that the manifest does not list;
- when the hot store's `frozen_frontier`/`next_txnum` or `Headers` checkpoints differ from
  the manifest, resets them to the manifest frontier (checkpoints re-derived from the last
  range's block hashes); if the hot store was ahead (a crash between steps 2 and 3) and
  holds no tail data, the names above `dict_lens@{frontier}` are dropped, so the range is
  rebuilt with the same ids and the same bytes;
- continues at `manifest.frontier`.

### Tail and freezer commit order

The live tail never writes segments. Its single writer commits one MDBX txn per
`tail.commit_ms` / `tail.commit_blocks`: names interned in block-apply order, the block rows
(`Headers`, `HeaderByHash`, `TailTxs`, `TailRk`, `TailPostings`, `Defpacts`, counters), one
`Undo` row per block and the chain's `Tail` checkpoint. A reorg unwinds to the fork point
and applies the new branch in the same txn.

The freezer moves the step `frontier..frontier+step_heights` once every chain's stored tip
is at least `finality_depth` above its end:

1. The step is written from the hot rows (`RangeWriter`, steps 1–3 above, `freezer.zstd_level`).
2. `manifest.json` is replaced (step 4 above).
3. One MDBX txn deletes the step's `Headers`, `HeaderByHash`, `TailTxs`, `TailRk` (rows
   owned by those txs), `TailPostings` and `Undo` rows, and sets `frozen_frontier`,
   `next_txnum`, the `Headers` checkpoints (hashes at `end − 1`) and `dict_lens@{end}`.

On start, files the manifest does not list are deleted, and if the manifest frontier is
above the hot `frozen_frontier` (a crash between 2 and 3) step 3 is redone. TxNums are
assigned at freeze time in (height, chain, pos) order, so a tx has exactly one TxNum and is
never both hot and frozen after step 3.

A merge rewrites ten consecutive steps that cover one aligned `range_heights` span as one
range (`backfill.zstd_level`, new dictionaries; records, TxNums and postings unchanged),
replaces the ten entries in `manifest.json`, then deletes the step files. A crash before
the manifest leaves an unlisted range (deleted on start); after it, unlisted steps (deleted
on start).

Raw spool: `etl/raw-{start}/raw-NNNNN.run`, sorted runs of `(height u32 BE, chain u8)` →
`[header, payload]` JSON, zstd level 1, k-way merged in (height, chain) order.

## `range.toml`

```toml
format_version = 1
network = "mainnet01"
kind = "range"            # or "step"
start = 7300000
end = 7400000
first_txnum = 0
tx_count = 0
block_count = 0
slot_width = 20
block_records = 64
level = 19
txs_dict_id = 0           # zstd dictionary ids (0 = no dictionary)
txs_dict_bytes = 65536
blocks_dict_id = 0
blocks_dict_bytes = 65536
rk_index = "ptrhash"      # or "sorted"
bh_index = "ptrhash"
rk_keys = 0
bh_keys = 0
cmd_reencoded = 0         # user txs whose served cmd does not hash to the request key

[postings.acct]           # one table per family
entities = 0
entries = 0

[files."mainnet01-txs-007300000-007400000.seg"]   # one table per file
bytes = 0
blake3 = "<hex>"
```

## `txs.seg` and `blocks.seg`

Both are [nippy-jar](https://github.com/paradigmxyz/reth/tree/main/crates/storage/nippy-jar)
containers (reth) with one column: the data file `.seg`, the offsets file `.off` (one
offset per row plus the end, preceded by one byte giving the offset width) and the
configuration file `.conf` (bincode; holds the user header below). nippy-jar compression is
not used: each row is one zstd frame compressed by kda-indexer.

**Row.** Up to 64 consecutive records (`block_records`). Decompressed:

```
u32      n                      records in this row (64 except in the last row)
u32 × (n + 1)  offsets          record i = bytes [off[i], off[i+1]) after the table
...      record bytes, back to back
```

Record `i` of the file is record `i mod 64` of row `i / 64`.

**Compression.** zstd at the range's level with a 64 KB dictionary trained on up to 20,000
records of that same file kind and range (never reused across ranges). If training is not
possible (very small ranges) the dictionary is empty and rows are compressed without one.

**User header** (`.conf`):

| field | meaning |
|---|---|
| `kind` | `txs` or `blocks` |
| `records` | record count |
| `block_records` | 64 |
| `level` | zstd level |
| `dict`, `dict_id` | the zstd dictionary and its id |
| `max_raw_row` | largest decompressed row in bytes |
| `present` | `blocks` only: one bit per slot (LSB first); record i is the i-th set slot |
| `slots` | `blocks` only: `(end − start) × 20` |

`txs.seg` holds every tx of the range in TxNum order: per block the coinbase (position 0),
then the user txs. `blocks.seg` holds one record per present slot in slot order.

## Record encoding

Every record starts with the format version byte `1`.

### Tx record

```text
u8      version = 1
u8      flags   b0 request_key  b1 cmd  b2 logs  b3 pact_tx_id
                b4 metadata     b5 continuation  b6 extra  b7 cb_req_key
u8      bits    b0 status_ok  b1 raw_tx  b2 raw_output  b3 cmd_unparsed
                b4 cmd_reencoded  (b5..b7 zero)
u8      present output-envelope keys present in the CommandResult, bit i
                = gas result reqKey logs events metaData continuation txId (0xff = all)
[8]     key     TxKey, big-endian
[32]    request_key                        if flags.b0 (user txs)
cb_req_key                                 if flags.b7 (coinbase)
        u8 form: 0 Plain [32] | 1 B64Text [32] | 2 B64Quoted [32] | 3 Other bytes(utf-8)
bytes   cmd     original signed cmd string  if flags.b1
varint  n_sigs, then per sig: u8 tag 0 = Ed25519 [64] | 1 = Other bytes(utf-8)
bytes   result  value of result.data (success) or result.error (failure)
varint  gas
[32]    logs                               if flags.b2 (absent = JSON null)
varint  pact_tx_id                         if flags.b3 (absent = JSON null)
bytes   metadata value                     if flags.b4 (absent = JSON null)
bytes   continuation value                 if flags.b5 (absent = JSON null)
varint  n_events, then per event: varint ModuleId, varint EventNameId,
        varint ModuleHash id, bytes params value
varint  n_transfers, then varint event position each (fungible TRANSFER events)
bytes   extra value (a map)                if flags.b6
bytes   raw_tx      original tx object bytes   if bits.b1
bytes   raw_output  original output bytes      if bits.b2
```

- `cb_req_key` forms: `Plain` = base64url of the hash, `B64Text` = base64url of the utf-8 of
  that text, `B64Quoted` = the same with surrounding quotes, `Other` = any other string.
- `raw_tx` / `raw_output` hold the exact bytes the node served, present only when rendering
  the typed fields does not reproduce them byte for byte; a reader returns them verbatim.
- `cmd_unparsed`: the `cmd` is stored verbatim but is not representable JSON.
- `cmd_reencoded`: the node served a `cmd` that does not hash to the request key (seen on
  many old txs); the served bytes are stored as is.
- `extra` keeps fields that do not fit their typed slot (unexpected envelope keys or values,
  non-standard `sigs` entries); its entries win over the typed fields when rendering.

### Block record

```text
u8      version = 1
u8      flags   b0 merkle_verified  b1 raw_miner  b2 adj_refs  (b3..b7 zero)
[32]    hash, [32] parent
u8      chain
varint  height
[32]    weight, [32] target            (little-endian bytes as on the wire)
[8]     nonce, big-endian u64
varint  creation_time, epoch_start, feature_flags
[32]    payload_hash, [32] transactions_hash, [32] outputs_hash
varint  n_adjacents, then per entry: u8 chain, then
        [32] hash                       if not flags.b2 (wire order kept)
        u8 form, [32] hash if form 0    if flags.b2
bytes   chainweb_version (utf-8)
varint  miner_account (AccountId; 0xffffffff = no account string)
bytes   miner_data value (the whole minerData object)
bytes   raw_miner  original minerData bytes  if flags.b1
```

**Adjacent refs.** Segment block records always set `adj_refs`. Form 1 (a 1-byte ref) means
the adjacent parent hash equals the stored hash of block (adjacent chain, height − 1) in
the **same range**; form 0 is a raw hash. The first height of a range is always written in
raw form, so a range decodes without its neighbours. The hot store writes the plain form
(b2 clear).

`merkle_verified` stays false until the payload Merkle roots have been recomputed from the
leaves; until then the payload is consistent with its header but not proven.

### Value codec

`value` fields are MessagePack with two extension types: ext 1 holds the exact text of a
JSON number (so `1.0` and big integers round-trip), ext 2 holds a varint AccountId for a
string that is an interned account name. Maps keep their original key order.

## `keys.ef`

```
[4]  "KDXK"   u8 version = 1   u32 slot_width   u64 slots
sucds EliasFano of slots + 1 values
```

Value `s` is the first TxNum of slot `s` relative to the range's `first_txnum`; the last
value is the range's tx count. An empty slot repeats the next slot's value, so its tx count
is 0. `TxNum → TxKey`: rank gives the slot, the TxNum minus the slot's first gives the
position. `TxKey → TxNum`: select on the slot plus the position.

## `rk.idx` and `bh.idx`

`rk.idx` maps request keys to the tx's TxNum offset inside the range; `bh.idx` maps block
hashes to the slot. Keys are reduced to their first 8 bytes (big-endian u64, the
"prefix"); bytes 8..12 are a 4-byte fingerprint. Every hit is confirmed against the full
32-byte key in the record.

```
[4]  "KDXH"   u8 version = 1   u8 kind   u64 n
```

- kind 0, **PtrHash**: `f64 alpha`, `f64 lambda`, `u64 seed`, `u64 pilots_len`, pilots
  bytes, `u64 remap_len`, remap `u32` each, zero padding to a multiple of 4, then `n` slots
  of `(u32 offset, u32 fingerprint)` indexed by the minimal perfect hash of the prefix
  ([PtrHash](https://github.com/RagnarGrootKoerkamp/ptrhash) with the `Linear` bucket
  function, FxHash, single part, remapped to `[0, n)`).
- kind 1, **sorted**: `n` × (`[8]` prefix big-endian, `[5]` offset big-endian) ascending by
  prefix, then offset; binary search; equal prefixes are allowed.

The sorted form is the fallback: it is written when two keys of the range share a prefix,
when PtrHash construction fails, or when forced for testing (`KDX_DEBUG_RK_FALLBACK=1`).
`range.toml` records which form each index uses.

PtrHash construction is deterministic: the global seed sequence is fixed upstream and the
pilot search uses a fixed per-part seed (a change in the vendored `ptr_hash`), so two builds
of the same keys give byte-identical files.

## Posting files (`.pst`)

One file per family per range. Families and their entity keys:

| family byte | file | entity |
|---|---|---|
| 0 | `xfer` | `AccountId << 32 | ModuleId`: txs where the account sends or receives a fungible TRANSFER of that module |
| 1 | `acct` | AccountId: txs whose sender, signer `addr` or coinbase miner is the account, or one of whose events names it (see below) |
| 2 | `pk` | PubKeyId: txs signed by the key |
| 3 | `evt` | EventNameId, or `1 << 32 | ModuleId` for "any event of the module" |
| 4 | `code` | first 8 bytes of the CodeHash, big-endian (verify the full hash on read) |

An event names an account when the account is a side of a fungible `TRANSFER (from, to,
amount)`, a poly-fungible `TRANSFER (id, from, to, amount)` or a `TRANSFER_XCHAIN` /
`TRANSFER_XCHAIN_RECD (from, to, amount, chain)`, or when it is a syntactically valid Pact
principal (`k:`, `w:`, `r:`, `u:`, `m:`, `p:`, `c:`) anywhere in the event's params. All of
these are interned before encoding, so the `acct` posting set is a pure function of the
block: backfill, tail, replay and rebuild give the same set. A non-principal ("vanity")
account name that appears only in the params of other events gets no posting.

```
[4]  "KDXP"   u8 version = 1   u8 family   u64 range_txs
lists area      one list per entity, in entity order
entities        n × u64, ascending
offsets         sucds EliasFano of n + 1 list start offsets (relative to the lists area)
trailer         u64 n, u64 lists_len, u64 offsets_len
```

A list holds TxNums relative to the range's `first_txnum`, ascending. Its first byte picks
the encoding by length:

| tag | when | layout |
|---|---|---|
| 0 | length ≤ 16 | varint length, varint first value, varint deltas |
| 1 | length ≤ max(range_txs / 32, 16) | varint length, sucds EliasFano (universe = range_txs) |
| 2 | denser | Roaring bitmap (standard portable serialization) |

Readers page newest first: ranges from newest to oldest, each list from its last value.

## Lookups

- **TxNum → record**: the manifest gives the range (binary search over `first_txnum`), then
  `txs.seg` record `TxNum − first_txnum`.
- **Request key → tx**: the hot tail first, then `rk.idx` of each range, newest first.
- **Block (chain, height) → record**: the range by height, the slot, the `present` bit and
  its rank give the `blocks.seg` record; adjacent refs resolve through the same range.
- **Block hash → block**: the hot tail first, then `bh.idx` of each range, newest first.

## Checking

`kda-indexer segments check` verifies the manifest (contiguous heights and TxNums), the
size and blake3 of every file listed in each `range.toml`, and reports orphan files.
`--deep` also decodes every record, checks every tx's TxKey against `keys.ef`, resolves
every request key and block hash through the indexes, decodes every adjacent ref and every
posting list.
