# GraphQL

`kda-indexer` serves Kadena's GraphQL schema (the schema of `@kadena/graph`) on its own
listener, `api.graphql` (default `127.0.0.1:31363`). `run` and `serve` both bind it.

- `POST /graphql`: a JSON request `{"query", "variables", "operationName"}`, or a batch (a JSON
  array of them).
- `GET /graphql?query=...&variables=...&operationName=...` (GraphQL over HTTP).
- The websocket upgrade on the same path carries subscriptions. Until they are served, an
  upgrade request answers a GraphQL error with `extensions.code = "NOT_SUPPORTED"`.

The listener sits behind the same limits as the compat API (per-IP rate, in-flight cap,
`trusted_proxies`, the request deadline and the 8 MB response cap) and answers CORS from any
origin. There is no playground or GraphiQL; introspection is on (`graphql.introspection`).

`kda-indexer graphql print-schema` prints the served schema. `kda-indexer graphql check-ops DIR`
validates the operation documents (`*.graphql`) in a directory against the schema and the
limits without running them, and prints `in-scope`, `invalid: ...` or `limit: ...` for each.
Neither needs a config.

## Schema

The schema is Kadena's `generated-schema.graphql`, vendored unchanged in
`crates/kdx-graphql/schema/kadena-graph.graphql` (kadena.js commit
`1b05828f9c1cff7ccd8f7665c577a0a9e599aa2b`, BSD-3-Clause, see `THIRD_PARTY_NOTICES`). Type,
field and argument names, argument defaults, nullability, interfaces, unions and deprecations
are identical, plus the ZelCore extensions below; a test compares the two and pins every
extension line (`crates/kdx-graphql/tests/extensions.txt`). Descriptions are not reproduced.

Differences from Kadena graph:

- `pactQuery` and `gasLimitEstimate` are in the schema but answer `NOT_SUPPORTED`: the indexer
  never runs Pact code for clients.
- `Transaction.result` is never a `TransactionMempoolInfo`: the indexer has no mempool, and a
  transaction that is not indexed is `null`.
- Page sizes: the schema text says 500 by default. Here a connection returns 20 items when
  `first`/`last` are omitted, and larger values are clamped to 100 (not refused);
  `pageInfo.hasNextPage` stays correct. The block lists (`blocksFromHeight`, `blocksFromDepth`,
  `completedBlockHeights`) and account balances (`balance`) clamp at 200 (twice `max_page`):
  kadscan's home list asks for 120 blocks and its assets tab for 200 balances.
- The schema has no "as of height" field, so GraphQL answers do not carry one (the native API
  does).
- Fields not served yet answer `NOT_SUPPORTED` with the rest of the operation intact.

## Errors

Errors carry `extensions.code`:

| Code | Meaning |
|---|---|
| `NOT_SUPPORTED` | a schema field this indexer does not serve |
| `BAD_USER_INPUT` | a bad argument, cursor or request body |
| `LIMIT` | a resource limit; `extensions.limit` names it (table below) |
| `TIMEOUT` | the request deadline passed |
| `COUNT_TIMEOUT` | reserved; a count cut short answers a lower bound instead (see `totalCount`) |
| `UNSUPPORTED_FILTER` | an `events` `parametersFilter` form that is not served |
| `NOT_AVAILABLE` | stored data the field needs is missing: an NFT holding's `guard` or `info` not read yet, or the node had no usable answer. The rest of the item answers |
| `INTERNAL` | an internal error (logged, never detailed in the answer) |

## Limits

All in `[graphql]` (defaults shown). A refusal answers a GraphQL error with
`extensions.code = "LIMIT"` and counts `kdx_graphql_rejected_total{reason}`.

| Limit | Default | `extensions.limit` |
|---|---|---|
| query depth | `max_depth = 10` | `depth` |
| introspection depth (`__schema`, `__type`; exempt from `max_depth`) | `introspection_depth = 14` | `introspection_depth` |
| query complexity | `max_complexity = 2000` | `complexity` |
| operations per batch | `max_batch = 10` | `batch` |
| query text | `max_query_bytes = 32768` | `query_size` |
| variables (JSON) | `max_variables_bytes = 65536` | `variables_size` |
| request body | 131072 bytes (HTTP 413) | `body_size` |
| page size | `default_page = 20`, `max_page = 100` (clamped); block lists and `balance` clamp at `2 × max_page` | — |
| request deadline | `api.deadline_ms` (5 s), shared by a batch | `TIMEOUT` code |

Rate: a request takes one token of the per-IP rate limit (`api.rate_per_ip`); a batch of N
operations takes N.

Complexity (field types from the schema; aliases change nothing): each field costs 1
(`__typename` 0, and a connection's `edges` and their `node` 0). A connection or list field
multiplies what it selects by the page size it will return, each item at least 1: of a
connection, only its `edges` (`totalCount` and `pageInfo` are answered once; a connection
that selects neither `edges` nor `pageInfo` reads no page); of a list (`newBlocks`, the
`transactions` subscription), all of it, sized by `quantity` when given. A list's
`totalCount` that is a count walk (`transactions`, `events`, `transfers`,
`transactionsByPublicKey` and the account `transactions`) costs `max_page`, and an
operation may run at most 2 of them, outer multipliers included (`complexity` otherwise). A
block's `transactions` and a tx's own `totalCount`s are counted with their rows. A block's
`events` `totalCount` decodes every tx of the block, so it costs the default page per block
(not a count walk). `fungibleAccountsByPublicKey` and `fungibleChainAccountsByPublicKey` are
sized at 100 accounts, the most they return. A subscription's `quantity` is clamped to
`max_page`, and the `transactions` subscription without one is sized at 20.

Depth counts nested fields; fragments are expanded where they are spread, so fragments,
inline fragments and aliases do not get around depth or complexity. Every operation in a
document is checked, not only the one that runs.

The document is parsed and walked by these limits before anything else looks at it, and that
walk stops at 20,000 fields after fragment expansion (`LIMIT`, `limit: complexity`, "query has
more than 20000 fields after fragment expansion"). A fragment bomb (each fragment spreading the
one before it twice) is refused in about a millisecond, over HTTP and over the websocket. At
most 8 directives are allowed on one field, fragment, fragment spread or inline fragment
(`LIMIT`, `limit: directives`).

A `crossChainTransfer` inside another `crossChainTransfer` costs 10 per item, the price of the
store read it would need. It is answered from the pair already loaded and reads nothing.

List pages (`transactions`, `transfers`, `events` and the other lists) share the API's heavy
read slots (half the read threads). Each client key (an IPv4 address or an IPv6 /64, as the
rate limiter keys it) holds at most one of them. Up to 3 more of its heavy reads wait behind
that one. Past that, one is refused at once with `LIMIT`. One page scans for at most 1 s; a
sparse or empty match then ends with `TIMEOUT`: narrow the filter.

Complexity: each field costs 1 (`__typename` 0). A field that returns a page multiplies the
cost of its selection by the page size it returns (`first`/`last` clamped to `max_page`, or
`2 × max_page` on the block lists and `balance`, or
`default_page` when omitted; this applies to `events`, `transactions`, `transfers`,
`blocksFromHeight`, `blocksFromDepth`, `completedBlockHeights`, `transactionsByPublicKey`).
`nodes(ids)` multiplies by the number of ids, per-chain account lists by the number of chains
(20, or `chainIds`), `nonFungibleTokenBalances` by `max_page`. Example:
`blocksFromHeight(startHeight: 0, first: 100) { edges { node { hash height } } }` costs
1 + 100 × (1 + 1 + 2) = 401.

A page size bound to a variable is costed at the variable's value, else at its default in the
operation, else (no value, no default) at `max_page`, never at `default_page`.

The walker fails closed (SR7-3): a `totalCount` is cheap only under the connection types on its
cheap list (a block's or tx's own rows, block ranges, account transfers, balances). Under any
other connection, or under a parent the walker cannot resolve (an interface or unknown type
condition, a fragment on one), it is a deferred count walk costing `max_page` and counting
toward the two per operation. A field the walker cannot resolve costs the page maximum with
its subtree priced per item.

Introspection (`__schema`, `__type`) is counted on its own budget of 100,000 per operation
(`extensions.limit = "introspection_complexity"`): its list fields multiply their selection by
the schema's averages (`types` 90, `fields` 4, `args` 1, `inputFields` 1, `enumValues` 2,
`interfaces` 1, `possibleTypes` 2, `directives` 4). The graphql-js introspection query costs
about 32,000 and is 14 deep. A list field may not repeat on one introspection path
(`__schema { types { fields { type { fields … } } } }` is refused as
`introspection_nesting`).

### Websockets

`GET /graphql` with `Upgrade: websocket` and `Sec-WebSocket-Protocol: graphql-transport-ws`
(or the legacy `graphql-ws`). Each limit below closes or refuses as shown and counts
`kdx_graphql_rejected_total{reason}`:

| Limit | Default | On refusal |
|---|---|---|
| connections per client IP (an IPv6 client counts by its /64) | `ws_conns_per_ip = 10` | upgrade answers 429 (`ws_connections_per_ip`) |
| connections per IPv6 /48 (all its /64s together) | `ws_conns_per_48 = 30` | upgrade answers 429 (`ws_connections_per_ip`) |
| connections per process | `ws_conns_max = 1000` | upgrade answers 429 (`ws_connections`) |
| subscriptions per connection | `ws_subs_per_conn = 20` | the subscription gets a `LIMIT` error (`ws_subscriptions_per_connection`), the connection stays |
| subscriptions per client IP (IPv4 address or IPv6 /64), over all its connections | `ws_subs_per_ip = 40` | `LIMIT` error (`ws_subscriptions_per_client`), the connection stays |
| subscriptions per IPv6 /48 | `ws_subs_per_48 = 120` | `LIMIT` error (`ws_subscriptions_per_client`), the connection stays |
| subscriptions per process | `ws_subs_max = 2000` | `LIMIT` error (`ws_subscriptions`) |
| `connection_init` | within `ws_init_secs = 10` | close 4408 (`ws_init_timeout`) |
| no subscription open | for `ws_idle_secs = 600` | close 4000 (`ws_idle`) |
| client message or frame | 64 KB | close 1009 (`ws_message`); a malformed frame closes 1002 |
| client messages | one per-IP rate token each (pings too) | close 4429 (`ws_rate`) |
| a client that does not read | 30 s per server message | close 4008 (`ws_slow_reader`) |
| a chain whose feed read fails | 30 ticks in a row | that chain alone is skipped each tick (a WARN line with the chain and its failure count; the other chains keep delivering), then resumes from its tip with a gap |
| a subscriber behind the feed | 1,024 feed items | one `INTERNAL` error; the subscription goes on from the feed's newest item |
| a query or mutation over the socket | subscriptions only | `NOT_SUPPORTED` error, nothing runs (`ws_query`); send queries over HTTP |
| one subscription message | the HTTP request deadline, shared by all its resolvers | `TIMEOUT` error for that message |

The per-client and per-/48 caps keep a few clients from holding every process slot: a
kadscan page holds a handful of subscriptions on one connection, so one client fits several
open pages under `ws_subs_per_ip`. An IPv4 client is capped per address only; an IPv6 client
by its /64 and, with the other /64s of its /48, by the /48 caps.

Depth, complexity and the size limits apply to subscription documents as to queries. Every
slot is released when its subscription or connection ends; `kdx_graphql_subscriptions` is the
number open.

## Blocks and network

- `Block` fields come from the stored header. `powHash` is recomputed from the binary header
  (`docs/format.md`, "Derived header fields") and rendered as chainweb-data does: lowercase
  hex of the hash read as a big-endian number. `target` and `weight` are decimal integers
  (chainweb-data's `numeric` columns); `difficulty` is (2^256 − 1) / target; `nonce` and
  `flags` are `Decimal` numbers; `epoch` and `creationTime` are `DateTime`s.
- `neighbors` lists the adjacent blocks ascending by chain id. `parent` is `null` for a
  genesis block. `minerAccount` is the miner's current `coin` row on the block's chain; a
  miner without a row is a field error.
- Block lists (`blocksFromHeight`, `blocksFromDepth`, `completedBlockHeights`) are ordered by
  height descending, then chain ascending; forward and backward pages hold the same blocks.
  `blocksFromDepth` lists blocks at least `minimumDepth` below their chain's stored tip.
  `completedBlockHeights` takes the newest `heightCount` (at most 100) heights: with
  `completedHeights: true` (default) only heights at or below every requested chain's tip.
- `Block.transactions` lists the block's user txs (coinbase is not a `Transaction`);
  `Block.events` lists every event of the block, coinbase included. Both page with the
  defaults above (20, at most 100), not the 500 the schema text mentions.
- `lastBlockHeight` is the highest stored tip.
- `networkInfo`: `networkId` from `network`, `networkHost` from `graphql.network_host`,
  `apiVersion` `"0.0"`, `transactionCount` and `coinsInCirculation` as `/stats`;
  `totalDifficulty` is the sum over chains of the tip block's `weight`, `networkHashRate` the
  sum over chains of the tip block's difficulty divided by 30 s.
- `node(id)`/`nodes(ids)` (at most 100) resolve every type; a malformed id is
  `BAD_USER_INPUT`, an object not stored is `null`. Coinbase events and transfers resolve
  through their block.

## Subscriptions

Subscriptions read a feed that polls the stored chain tips every `feed_poll_ms` (1,000), so
`serve` and replicas serve them as `run` does.

- A message never carries more items than its complexity priced: a catch-up is split over
  several messages.
- `newBlocks(chainIds)`: each message lists new blocks of the subscribed chains, at most one
  per subscribed chain. After a reorg the replacing blocks are sent again.
- `newBlocksFromDepth(chainIds, minimumDepth)`: a block is sent once its chain's tip is
  `minimumDepth` above it, starting from the subscription's start; only blocks the store
  still holds at their height (checked on release); at most one per subscribed chain per
  message.
- `events(qualifiedEventName, chainId, minimumDepth, parametersFilter)`: the matching events
  of each new (or deep enough) block, at most the default page (20) per message, the rest in
  the next messages; `parametersFilter` takes the forms `events` takes.
- `transaction(requestKey, chainId)`: one message when the tx is (or becomes) indexed, then
  the subscription completes; `TIMEOUT` after `ws_tx_wait_secs`.
- More than 200 new blocks on a chain in one poll (a catch-up), or a reorg deeper than 64
  blocks, is a gap: subscribers of that chain get the blocks before it, then one `INTERNAL`
  error, and stay subscribed.

## Ids and cursors

Node ids (`id`, `node(id:)`, `nodes(ids:)`) use Kadena graph's global id format: standard base64
of `<TypeName>:<key>`. The key is the block hash for `Block`, the account name for
`NonFungibleAccount`, and otherwise a compact JSON array of the natural key in this order:

| Type | Key |
|---|---|
| `Transaction` | `[blockHash, requestKey]` |
| `Event` | `["blockHash", "orderIndex", "requestKey"]` (the order index as a string) |
| `Transfer` | `[blockHash, "chainId", "orderIndex", moduleHash, requestKey]` (chain and order index as strings) |
| `Signer` | `[requestKey, "orderIndex"]` (the index as a string) |
| `FungibleAccount` | `[fungibleName, accountName]` |
| `FungibleChainAccount` | `[chainId, fungibleName, accountName]` |
| `NonFungibleChainAccount` | `[chainId, accountName]` |
| `NonFungibleTokenBalance` | `[tokenId, accountName, chainId]` |

The numeric key elements are JSON strings (`["hash","1","rk"]`) because that is how the deployed
oracle writes them (R-P5P9-19); decoding takes a number or a numeric string, so ids of either
form resolve.

Cursors are opaque. Transaction, event and transfer cursors are the same tokens as the native
API's (a transaction position plus a block fingerprint), so a cursor taken from one instance
pages on another, and a cursor whose block was reorged away is refused.

## Scalars

| Scalar | Format |
|---|---|
| `BigInt` | a JSON number while it is a safe integer (below 2^53), else a string |
| `DateTime` | `YYYY-MM-DDTHH:MM:SS.mmmZ` (UTC, milliseconds) |
| `Decimal` | a JSON number with the exact decimal digits (no float rounding) |

### Wire formats that follow the oracle (R-P5P9-19)

The deployed oracle serializes several fields differently from the scalar the schema declares.
Where the kadscan client parses the field, the wire form follows the oracle; the schema text
stays Kadena's (the parity test sees the same types, so there is nothing to pin in
`tests/extensions.txt`). Deviations from the scalar formats above:

| Field | Declared | Wire form |
|---|---|---|
| `TransactionMeta.gasLimit`, `.ttl` | `BigInt` | string (`"600"`) |
| `TransactionMeta.gasPrice` | `Float` | string, plain notation (`"0.00000001"`) |
| `TransactionResult.gas`, `.transactionId` | `BigInt` | string (`"236"`) |
| `Transfer.amount` | `Decimal` | string, plain notation (`"0.00000236"`) |
| `Block.nonce` | `Decimal` | string |
| `Block.difficulty` | `BigInt` | always a JSON number, also above 2^53 |
| `FungibleChainAccount.chainId` | `String` | string (already) |

Everything else keeps the scalar rules (`Block.height`, `chainId`, `orderIndex`, `result.height`
stay numbers; `totalBalance` and `balance` stay numbers). Error codes ride in
`extensions.code` (GraphQL spec) and are copied to a top-level `code` on each error, the place
the oracle puts them.

## Transactions, events and transfers

- **Lists.** `transactions`, `transactionsByPublicKey`, `transfers`, `events` and the account
  connections walk the same posting lists as the native API. Order: newest first by
  (height, chain, position in the block); rows of one tx (events, transfers) newest first too,
  by descending event position (order index 1 before 0), as the deployed oracle lists them.
  `first`/`after` pages walk down from the newest row; `last`/`before` pages first collect
  the keys between the cursor and the newest row (keys only, at most 200,000, else `LIMIT`).
  Every walk checks the request deadline every 64 keys and answers `TIMEOUT` past it.
- **Cursors** are the native API's tokens (tx key, row position and a fingerprint of the
  row's block): portable between instances, refused with `BAD_USER_INPUT` after a reorg of
  that block.
- **Coinbase.** Coinbase txs are not `Transaction`s (as in Kadena graph): transaction lists
  leave them out, and `Event.transaction`/`Transfer.transaction` of a coinbase row is `null`.
  Their events and transfers are listed, with request key `<coinbase>` and order index 0
  (the compat rendering).
- **`transactions` sources**: `requestKey`, else `blockHash`, else `accountName` with
  `fungibleName` (txs with a transfer of that token on the account's side), `accountName`
  alone (every tx touching the account), `fungibleName` alone (txs with a `<token>.TRANSFER`
  event); with none of them, every user tx from the tip down (at most 20,000 heights), as
  a latest-transactions list asks. `chainId`, `minHeight`, `maxHeight` and
  `minimumDepth` (height ≤ the chain's tip − depth) filter any source.
- **`transfers`** use the same sources; rows are the tx's transfers of `fungibleName` with
  `accountName` on either side when given. `crossChainTransfer` is the other half of a
  cross-chain transfer (the `cont` step of the pact, or the tx that started it), `null`
  while that half is not indexed.
- **`events`**: `qualifiedEventName` is required. `parametersFilter` accepts only
  `{"array_starts_with": <value or array>}` (the event's first parameters equal the value or
  the array's values); anything else is `UNSUPPORTED_FILTER`.
- **`transaction(requestKey)`**: `null` when not indexed; a request key stored in several
  genesis blocks answers `BAD_USER_INPUT` unless `blockHash` picks one copy.
- **`totalCount`** is computed only when selected: a count walk of at most 10,000 rows
  within half of the remaining deadline. Larger lists show 10,000. When the time share runs
  out, `totalCount` answers the rows counted so far (a lower bound), the edges are kept, and
  the response carries `"extensions": {"totalCountLowerBound": true}` (R-P5P9-14).
- **Text forms.** `goodResult`, `badResult`, `continuation`, `metadata`, `Event.parameters`,
  payload `data` and capability `args` are compact JSON text of the node's values (numbers
  keep their exact text). `Transfer.amount` is the decimal of the event's amount parameter.

## Accounts

- `fungibleAccount`, `fungibleChainAccount(s)` and `…ByPublicKey` read the reconciled
  balance rows (spec §3.9); resolvers never call the node. Stale rows are served; chains
  without a row are absent from `chainAccounts`; with no row the account is `null`.
- `totalBalance` is the exact decimal sum of the chain balances; `balance` is the stored
  text as a float.
- Guards: an inline keyset or a resolved keyset reference is a `KeysetGuard` (`raw` is the
  stored guard), a user guard is a `UserGuard`, anything else a `RawGuard`.
- `…ByPublicKey` lists at most 100 accounts, only rows whose guard holds the key, of `coin`
  (the default) or of a module confirmed `fungible-v2` on that chain. Only the
  `[balances] key_modules` (default `coin`) are governance-trusted. Any module can implement
  `fungible-v2` and report any balance under any key. A `fungibleName` outside `key_modules`
  is answered as the caller's own choice. Even a `coin` row proves only that the account's guard
  holds the key, not who owns the account: anyone can create an account guarded by someone
  else's key without their signature.
- `FungibleAccount.transactions`: txs with a transfer of the token on the account's side;
  `FungibleChainAccount.transactions`: txs on that chain the account sent (`meta.sender`),
  as the schema says.
- `nonFungibleAccount`/`nonFungibleChainAccount` list positive NFT holdings (at most
  `max_page`); `version` is `v1` (`marmalade.ledger`), `v2` (`marmalade-v2.ledger`) or the
  ledger name.
- `NonFungibleTokenBalance.guard` and `.info` come from the `NftDetails` table. The reconciler
  fills it with `({ledger}.details (read-msg "t") (read-msg "a"))` and
  `({ledger}.get-token-info (read-msg "t"))` (GQ-15); resolvers never call the node.
  - Details read before the holding's balance last changed are served, as stale balances are.
  - `info` is `null` with no error for a ledger without `get-token-info` (`marmalade.ledger`).
    `supply` is truncated toward zero, like `balance`.
  - A holding whose details are not read yet, or whose node answer was absent or unusable, gets
    a `NOT_AVAILABLE` error on that field only. `info` is then `null`. `guard` is non-null, so
    it answers an empty `RawGuard` (`raw: ""`, no keys) beside the error: the account and its
    other holdings still answer.

## ZelCore extensions

Additive fields and arguments that kadscan's operations use (R-P5P9-16), built clean-room on
our store; they are not in Kadena's schema. Every Kadena document stays valid: the one
relaxed argument (`events.qualifiedEventName`, now nullable) accepts what `String!` did.

| Extension | Answer |
|---|---|
| `Block.canonical: Boolean!` | whether the block is the stored block at its chain and height. The store keeps the canonical chain only, so a block read in the same request is `true`; one held across a reorg (a subscription) may not be. One read per block |
| `Block.coinbase: String!` | the coinbase output as compact JSON text (`gas`, `logs`, `txId`, `events`, `reqKey`, `result`, `metaData`, `continuation`). One read per block |
| `Transaction.orphanedTransactions: [Transaction]` | always `[]`: the store keeps no orphaned blocks |
| `QueryBlocksFromDepthConnection.totalCount` | blocks in the whole list, from the per-chain height ranges (store floor to tip minus `minimumDepth`); exact without a walk, since blocks are dense |
| `TransactionResultTransfersConnection.totalCount` | the tx's transfer count |
| `transactions(isCoinbase: Boolean)` | `false` or absent: user txs (as without it). `true`: `NOT_SUPPORTED` (coinbase txs are not `Transaction`s here; read `Block.coinbase`) |
| `transfers(isNFT: Boolean)` | `true`: NFT transfers instead of fungible ones, the four-parameter `TRANSFER (id sender receiver amount)` events of the NFT ledgers (`[nft] ledgers`). An account's come from its `acct` postings; with neither account, module, block nor request key, heights are walked down from the tip (as `transactions` without a selective argument). `crossChainTransfer` is `null` for them |
| `Transfer.tokenId: String` | the NFT transfer's token id; `null` for a fungible transfer |
| `events(moduleName: String)` | events of that module. `qualifiedEventName` or `moduleName` is required (`BAD_USER_INPUT` otherwise); with both, both filter. `moduleName` alone has no posting list: unless `blockHash` or `requestKey` picks the txs, heights are walked down from the tip (at most 20,000 heights, within the deadline) |
| `NetworkInfo.countersOfEachChain: [CountersOfEachChain!]!` | per chain, ascending: `chainId`, `blocksCount` (genesis to stored tip). `transactionCount: Int` and `totalGasUsedInKda: String` answer `null` with `NOT_AVAILABLE`: the store keeps no all-time per-chain tx or gas counter |
| `balance(accountName, module, chainIds, first/last/after/before): QueryBalanceConnection!` | an account's present fungible balances, one `BalanceNode {accountName module chainId balance}` per module and chain, listed by module name then chain, filtered by `module` and `chainIds`; at most 4,096 rows per account (`LIMIT` above). Without `accountName` (a module's holders): `NOT_SUPPORTED`, as the store keeps no holders index |
| `transactionsByPactCode(pactCode, first/after): QueryTransactionsByPactCodeConnection!` | txs whose own exec code contains `pactCode` (`%`/`_` wildcards), newest first, through the `/txs/search` engine: its pattern checks, per-client search gate, code index or bounded newest-first scan, caps and time budget. Rows are `TransactionSummary {requestKey height chainId canonical creationTime badResult sender gas gasLimit gasPrice code}`. Forward paging only (`last`/`before`: `BAD_USER_INPUT`). The search gate counts per client
address (HTTP, or the websocket's accepted connection); an operation without a known address
is refused (`LIMIT`). A page the search cut short carries `"extensions": {"searchTruncated": true}` and `pageInfo.endCursor` resumes it. `NOT_SUPPORTED` when the server has no search context |
| `lastTokenPriceInKda(moduleName): Decimal` | `NOT_SUPPORTED`: no price data |
| `Subscription.newBlocks(quantity: Int)` | at most `quantity` (1 to 100, clamped to `max_page`) blocks per message, the newest of the batch; absent keeps Kadena's behaviour |
| `Subscription.transactions(quantity: Int = 20): [Transaction!]` | the user txs of newly committed blocks on every chain, newest first, at most `quantity` (1 to 100, clamped to `max_page`) per message; a message per feed batch that has any |

`pactQuery` stays `NOT_SUPPORTED`.

## Fields compared with the node, not an oracle

Tip-relative answers change from second to second, so the comparison harness checks them
against the node or the native API: balances and guards, `networkInfo`, `lastBlockHeight`,
`completedBlockHeights`, `blocksFromDepth` and subscriptions.
