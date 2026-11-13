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
| `GET /accounts/{account}/balances?token&chain&cursor&limit` | Current balances from `/local` reconciles: `items` of `{module, chain, balance, guard, as_of_height, stale}`. `balance` is the decimal text the node printed; `as_of_height` is the node's tip height that read saw; `stale: true` means a later block (or a reorg) touched the row and a re-read is queued. Accounts the node reports absent on a chain are left out. A key waiting for its first read is listed with `balance`, `guard` and `as_of_height` null and `stale: true`; `stale` is also true when the last re-read after a change failed. Rows of modules marked not fungible are left out. Paged by (module, chain): `limit` 1..=100 (default 100), `cursor`; a page may hold fewer items than `limit`. |
| `GET /keys/{pubkey}/accounts?cursor&limit` | Accounts whose guard holds the key (inline keysets, and keyset references resolved with `describe-keyset`; user, capability and module guards give none): `items` of `{account, modules: [{module, chain, balance, as_of_height, stale}]}`, paged by account. Only rows of `coin` and of modules whose `describe-module` lists `fungible-v2` on that chain count: any other module's `details` reports whatever guard its author wrote, so it cannot add an account to a key. |
| `GET /tokens?cursor&limit` | Fungible tokens (modules seen emitting `TRANSFER`): `items` of `{module, first_height, holders, holders_approx: true}`. `holders` counts reconciled rows above zero. Paged by module name (`limit` 1..=100, default 20). Modules whose `details` calls keep failing (not fungible) are left out until their re-check. |
| `GET /tokens/{module}` | One token, same fields; `404` for a module never seen in a `TRANSFER` or marked not fungible. |
| `GET /nft/accounts/{account}?chain&cursor&limit` | NFTs the account holds, balances above zero: `items` of `{token_id, ledger, chain, balance, as_of_height, source, stale}`, paged by token and chain. A *ledger* is a trusted marmalade ledger module (`[nft] ledgers`, default `marmalade.ledger` and `marmalade-v2.ledger`). `source` is `event` (the balance after a ledger `RECONCILE` event) or `local` (read with `get-balance`, used for rows a reorg touched and for ledgers that emit no `RECONCILE`); `stale: true` means a re-read is queued. Accounts that are not in the account dictionary (non-principal names never seen as a sender or a `TRANSFER` party) are not listed. |

A store written before the balance tables existed (served read-only) answers the balance, key and token routes with empty lists (format rule A-1 in `format.md`).

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

## Search, `/coins` and defpact (P6)

Every search route runs under one gate: at most `search.concurrency` (4) searches at once (`503` beyond) and `search.per_client` (1) per client, keyed like the rate limiter (`429` beyond). Patterns are SQL `LIKE` patterns matched anywhere in the text (`%`, `_`, `\` escapes), at most 256 characters and 8 `%`. Refusals and cut answers count in `kdx_search_rejected_total{reason}` (`too_short`, `busy`, `client_busy`, `timeout`, `cap`); `kdx_search_docs` and `kdx_search_built_through_height` describe the code index served.

- Compat `GET /txs/search?search=…` (chainweb-data `searchTxs`): `[TxSummary]` of txs whose own exec code contains the pattern, height desc then request key desc, `limit` 10 (max 250), `offset` up to 10,000 or `next`, `Chainweb-Next`. A pattern with a literal run of 3 characters uses the code index (when it holds every body the pattern can match); otherwise a bounded newest-first scan (`search.max_scan_heights`, `search.timeout_ms`). A cut answer carries `X-Search-Truncated: true` and, when there is more, `Chainweb-Next`. Genesis txs are listed, as chainweb-data does (the other compat list routes hide them). chainweb-data also matches `cont` txs through their initial code; kda-indexer does not.
- Compat `GET /txs/search?pactid=…`: the `cont` txs of the defpact, successful first, then height desc (ties request key ascending), at most 50, from the newest 1,000 txs of the pact (cap: an old successful continuation of a pact with more than 1,000 txs may not appear, while chainweb-data lists it first; such an answer carries `X-Search-Truncated: true`, an answer the cap did not cut has no such header). Neither parameter answers `404 You must specify a search string`, both `400`.
- Compat `GET /coins`: circulating KDA at the highest stored height, as chainweb-data prints it (`text/plain`); `404 Server database is empty` outside mainnet01.
- Native `GET /api/v1/search/code?q=…`: txs whose exec code contains `q` (needs `search.min_len` non-wildcard characters and a 3-character run), from the code index only (`503` when none is built); `{items, next_cursor, truncated, partial, as_of}`. `partial: true` means fallback mode may hold bodies too short for this pattern, or the index trails the data tip by more than 20 heights (newer bodies may be missing; compat `/txs/search?search=` then runs its bounded scan instead).
- Native `GET /api/v1/search/events?q=…`: events whose qualified name or parameter text contains `q`, the compat bounded scan in the native shape; `{items, next_cursor, truncated, as_of}`.
- Native `GET /api/v1/txs/{request_key}/defpact`: `{pact_id, items: [{request_key, chain, height, step, rollback, success}], truncated, as_of}`, ascending, for the pact the tx starts or continues (at most 1,000 txs, the newest).
- Native `GET /api/v1/txs/{request_key}`: a request key stored in several genesis blocks answers every copy as `{items, next_cursor: null, as_of}`.
