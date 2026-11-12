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
are identical; a test compares the two. Descriptions are not reproduced.

Differences from Kadena graph:

- `pactQuery` and `gasLimitEstimate` are in the schema but answer `NOT_SUPPORTED`: the indexer
  never runs Pact code for clients.
- `Transaction.result` is never a `TransactionMempoolInfo`: the indexer has no mempool, and a
  transaction that is not indexed is `null`.
- Page sizes: the schema text says 500 by default. Here a connection returns 20 items when
  `first`/`last` are omitted, and larger values are clamped to 100 (not refused);
  `pageInfo.hasNextPage` stays correct.
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
| `COUNT_TIMEOUT` | `totalCount` could not be counted in time; the edges are still returned |
| `UNSUPPORTED_FILTER` | an `events` `parametersFilter` form that is not served |
| `INTERNAL` | an internal error (logged, never detailed in the answer) |

## Limits

All in `[graphql]` (defaults shown). A refusal answers a GraphQL error with
`extensions.code = "LIMIT"` and counts `kdx_graphql_rejected_total{reason}`.

| Limit | Default | `extensions.limit` |
|---|---|---|
| query depth | `max_depth = 10` | `depth` |
| introspection depth (`__schema`, `__type`; exempt from `max_depth`) | `introspection_depth = 15` | `introspection_depth` |
| query complexity | `max_complexity = 2000` | `complexity` |
| operations per batch | `max_batch = 10` | `batch` |
| query text | `max_query_bytes = 32768` | `query_size` |
| variables (JSON) | `max_variables_bytes = 65536` | `variables_size` |
| request body | 131072 bytes (HTTP 413) | `body_size` |
| page size | `default_page = 20`, `max_page = 100` (clamped) | — |
| request deadline | `api.deadline_ms` (5 s), shared by a batch | `TIMEOUT` code |

Rate: a request takes one token of the per-IP rate limit (`api.rate_per_ip`); a batch of N
operations takes N.

Depth counts nested fields; fragments are expanded where they are spread, so fragments,
inline fragments and aliases do not get around depth or complexity. Every operation in a
document is checked, not only the one that runs.

Complexity: each field costs 1 (`__typename` 0). A field that returns a page multiplies the
cost of its selection by the page size it returns (`first`/`last` clamped to `max_page`, or
`default_page` when omitted; this applies to `events`, `transactions`, `transfers`,
`blocksFromHeight`, `blocksFromDepth`, `completedBlockHeights`, `transactionsByPublicKey`).
`nodes(ids)` multiplies by the number of ids, per-chain account lists by the number of chains
(20, or `chainIds`), `nonFungibleTokenBalances` by `max_page`. Example:
`blocksFromHeight(startHeight: 0, first: 100) { edges { node { hash height } } }` costs
1 + 100 × (1 + 1 + 2) = 401.

Websocket limits (`ws_*`) apply once subscriptions are served.

## Ids and cursors

Node ids (`id`, `node(id:)`, `nodes(ids:)`) use Kadena graph's global id format: standard base64
of `<TypeName>:<key>`. The key is the block hash for `Block`, the account name for
`NonFungibleAccount`, and otherwise a compact JSON array of the natural key in this order:

| Type | Key |
|---|---|
| `Transaction` | `[blockHash, requestKey]` |
| `Event` | `[blockHash, orderIndex, requestKey]` |
| `Transfer` | `[blockHash, chainId, orderIndex, moduleHash, requestKey]` |
| `Signer` | `[requestKey, orderIndex]` |
| `FungibleAccount` | `[fungibleName, accountName]` |
| `FungibleChainAccount` | `[chainId, fungibleName, accountName]` |
| `NonFungibleChainAccount` | `[chainId, accountName]` |
| `NonFungibleTokenBalance` | `[tokenId, accountName, chainId]` |

Cursors are opaque. Transaction, event and transfer cursors are the same tokens as the native
API's (a transaction position plus a block fingerprint), so a cursor taken from one instance
pages on another, and a cursor whose block was reorged away is refused.

## Scalars

| Scalar | Format |
|---|---|
| `BigInt` | a JSON number while it is a safe integer (below 2^53), else a string |
| `DateTime` | `YYYY-MM-DDTHH:MM:SS.mmmZ` (UTC, milliseconds) |
| `Decimal` | a JSON number with the exact decimal digits (no float rounding) |

## Fields compared with the node, not an oracle

Tip-relative answers change from second to second, so the comparison harness checks them
against the node or the native API: balances and guards, `networkInfo`, `lastBlockHeight`,
`completedBlockHeights`, `blocksFromDepth` and subscriptions.
