# Native API v1

**Internal and unstable.** The native API binds a loopback or private address only (the server refuses anything else) and may change freely until a public hostname is decided. The machine-readable form is [`openapi.json`](openapi.json); regenerate it with `kda-indexer openapi > docs/openapi.json`.

All routes are `GET` under `/api/v1`. JSON field names are snake_case. Errors are plain text with a status code: 400 (bad parameter, bad cursor), 404, 429 (per-IP rate), 503 (too many requests in flight, or a response over the 8 MB cap), 504 (the 5 s deadline). The listener on `[api] native` has no CORS headers.

## Conventions

- **Lists** answer `{ "items": [...], "next_cursor": string|null, "as_of": {...} }`. Single resources are objects with an extra `as_of` field.
- **`as_of`** is `{ "indexed_tip_min": <lowest indexed tip over all chains>, "frozen_frontier": <first height not yet in segments> }`.
- **Parameters** (every list): `limit` (1 to 100, default 20), `cursor`, `order` (`desc` newest first, default; `asc`), `minheight` and `maxheight` (inclusive), `chain` (0 to 19). An unknown value is a 400.
- **Cursors** are opaque. They encode the last row returned and a fingerprint of its block, so they stay valid when that block moves from the hot store into a segment. A cursor whose block was reorged away answers `400 cursor invalidated by reorg`; restart the walk without a cursor.
- **Short pages.** A filtered list (`role=sender`, `events` with `module`+`name`+`param`, `transfers`) examines at most 5,000 txs per request. When that cap is hit before `limit` rows are found, the page is shorter and `next_cursor` is set. Follow `next_cursor` until it is `null`; an empty page with a cursor is normal.
- **`order=asc`** on account, key and event lists materialises at most 200,000 postings of the window, then pages through them; a larger window answers `400` and asks for a narrower `minheight`/`maxheight`.
- **Order inside a tx.** Several rows of one tx (transfers, events) are always in event order, in both directions.
- **Genesis blocks** are served here (the compat API hides them).
- **Coinbase** txs have `request_key: null` and `kind: "coinbase"`.
- Account names are 1 to 256 bytes.

## Routes

| Route | Returns |
|---|---|
| `GET /status` | Network, per-chain node and indexed tip, lag, halted chains, frozen frontier. |
| `GET /stats` | Exact `user_tx_count` and `block_count`; per-chain frozen counts (`per_chain_ready` turns true once the background count of the frozen ranges finished). Circulating supply is added with the compat `/stats` work. |
| `GET /blocks?minheight&maxheight&chains` | Blocks by height window, ordered by (height, chain); `chains=0,3` or `chain=`. A request walks at most 20,000 heights, then answers with a cursor. |
| `GET /blocks/{hash}` | Header, miner, `tx_count` (user txs), `confirmations` (tip block = 1), `coinbase` (a full tx object). |
| `GET /chains/{chain}/blocks/{height}` | Same, by height on the canonical chain. |
| `GET /blocks/{hash}/txs` | User txs of the block as summaries, payload order (`order=desc` reverses). |
| `GET /txs/{requestKey}?include=proof` | Full tx (below). `include=proof` adds the SPV proof of a continuation tx. |
| `GET /accounts/{account}/transfers?token&chain` | Transfers of `token` (default `coin`) with the account on either side. A transfer to oneself is one row. Cross-chain halves carry `cross_chain_account` and `cross_chain_id`, resolved from the same tx's `TRANSFER_XCHAIN` and `X_RESUME` events. |
| `GET /accounts/{account}/txs?role=any\|sender` | Tx summaries touching the account (any appearance, including coinbase rewards), or sent by it (meta sender). |
| `GET /keys/{pubkey}/txs` | Tx summaries signed by the key. |
| `GET /events?module&name&chain&param0..param9` | Events with exact module and name, and exact string match of parameter N. `name` needs `module`, or is written qualified (`coin.TRANSFER`). At least one of `module`, `name`, `param0..9` is required; a `param` filter alone needs `module` or `name`. |

### Reserved for P6

`/txs/{requestKey}/defpact`, `/accounts/{account}/balances`, `/keys/{pubkey}/accounts`, `/tokens`, `/tokens/{module}`, `/search/code`, `/search/events` and `/nft/accounts/{account}` answer `501 {"error":"not implemented until P6"}`.

## Objects

**Tx summary** (list rows): `request_key`, `chain`, `height`, `block_hash`, `block_time`, `tx_index`, `kind` (`exec`, `cont`, `coinbase`, `unknown`), `status` (`success`, `failure`), `gas`, `sender`, `code` (first 256 bytes of the exec code) and `code_truncated`.

**Tx** (`/txs/{requestKey}`): the summary context plus `result` (the data on success, the error on failure), `pact_tx_id`, `logs`, `metadata`, `continuation`, `events` (`idx`, `name`, `qualified_name`, `module`, `module_hash`, `params`), `transfers` (`idx`, `token`, `from`, `to`, `amount` as a decimal string, `cross_chain_account`, `cross_chain_id`), `sigs`, and `command`:

- exec: `code`, `data`;
- cont: `pact_id`, `step`, `rollback`, `data`, and `proof` (only with `include=proof`; otherwise `has_proof`);
- both: `nonce`, `network_id`, `sender`, `gas_limit`, `gas_price`, `ttl`, `creation_time`, `signers` (`public_key`, `scheme`, `address`, `capabilities` as `{name, args}`).

**Transfer row** (`/accounts/{a}/transfers`): the tx context fields (`request_key`, `chain`, `height`, `block_hash`, `block_time`, `tx_index`) plus the transfer fields above.

**Event row** (`/events`): the tx context fields plus `idx`, `name`, `qualified_name`, `module`, `module_hash`, `params`.

**Block**: `hash`, `chain`, `height`, `parent`, `time` (RFC 3339), `creation_time` (microseconds), `adjacents`, `weight`, `target`, `nonce`, `epoch_start`, `feature_flags`, `payload_hash`, `transactions_hash`, `outputs_hash`, `chainweb_version`, `miner` (`account`, `data`), `tx_count`, `confirmations`, `merkle_verified`.

## Consistency

Every request reads one snapshot of the hot store and the segment set, so a tx is never listed twice nor torn across a freeze or a merge (see `docs/format.md`, "Readers"). A tx may briefly be missing while a block is being applied.
