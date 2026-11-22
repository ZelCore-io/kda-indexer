# GraphQL

`kda-indexer` serves Kadena's GraphQL schema (the schema of `@kadena/graph`) on its own
listener, `api.graphql` (default `127.0.0.1:31363`). `run` and `serve` both bind it.

- `POST /graphql`: a JSON request `{"query", "variables", "operationName"}`, or a batch (a JSON
  array of them).
- `GET /graphql?query=...&variables=...&operationName=...` (GraphQL over HTTP).
- The websocket upgrade on the same path carries subscriptions (see [Subscriptions](#subscriptions)).

The listener sits behind the same limits as the compat API (per-IP rate, in-flight cap,
`trusted_proxies`, the request deadline and the 8 MB response cap) and answers CORS from any
origin. There is no playground or GraphiQL; introspection is on (`graphql.introspection`).

`kda-indexer graphql print-schema` prints the served schema. `kda-indexer graphql check-ops DIR`
validates the operation documents (`*.graphql`) in a directory against the schema and the
limits without running them, and prints `in-scope`, `invalid: ...` or `limit: ...` for each
(`limit: page` for a page over 1,000 rows, which only the resolver refuses).
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
  never runs Pact code for clients. `gasLimitEstimate(input: [])` runs no Pact and answers
  `[]`, as the stock indexer does.
- `Transaction.result` is never a `TransactionMempoolInfo`: the indexer has no mempool, and a
  transaction that is not indexed is `null`.
- Page sizes: as in the stock indexer, a connection returns 20 items when `first`/`last` are
  omitted (or 0), and any larger page the complexity limit admits is answered in full up to
  the 1,000-row safety cap (see Limits). `pageInfo.hasNextPage` stays correct.
- The schema has no "as of height" field, so GraphQL answers do not carry one (the native API
  does).
- Fields not served yet answer `NOT_SUPPORTED` with the rest of the operation intact.
- Row order within a block (R-P5P9-151). A newest-first list that spans several blocks
  (`transactions`, `events`, `transfers` without `blockHash`, the account connections) lists
  the rows of one block in reverse node order, so the coinbase comes last; the stock indexer
  orders those rows by creation time. A list of one block (`blockHash` without `requestKey`,
  the `Block` connections) is in the node's order, coinbase first, as the node and the stock
  indexer list it.

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
| `INTERNAL_SERVER_ERROR` | the stock indexer's code, kept where ours answers as stock does: the complexity refusal (Limits), and a request refused before any field runs because a variable is missing or its value does not coerce to its type. Such a request answers HTTP 500 with no `data` key, as stock answers it; in a batch the status stays 200. Also `transaction(minimumDepth:)` outside 1..=100, with stock's message (`[ERROR][GRAPHQL][VALID_RANGE] Minimum depth must not be lower than 1.` or `… higher than 100.`), `transaction: null` and HTTP 200 |
| `GRAPHQL_PARSE_FAILED` | the document does not parse: HTTP 400 with no `data` key, as stock (Apollo Server 4) answers it. The message is ours (async-graphql's parser), not graphql-js's |
| `GRAPHQL_VALIDATION_FAILED` | the document fails validation (an unknown field, a literal of the wrong type, an unused variable, the depth limit): HTTP 400 with no `data` key, as stock answers it. A document error, the depth refusal included, is answered alone, without the variable errors of the same request, as Apollo validates before it reads variables. Messages are async-graphql's. One case differs: an argument that mixes a variable with a bad literal (`nodes(ids: [$a, 1])`) is taken as a variable error (500 `INTERNAL_SERVER_ERROR`), where Apollo answers 400 |

A request refused before anything ran (a parse, validation or limit refusal, or a variable
error) has no `data` key, as in stock and the GraphQL spec; an error a field raised keeps
`data`. In a batch each item follows this rule and the status stays 200.

## Limits

Depth and complexity are the same as the stock indexer's (hack-a-chain-software/indexer-kadena
at 90a4be1, `server.ts`), ported from the libraries it uses, with the same thresholds, the
same counting and the same refusal text, code and HTTP status. They are fixed, not settings.

| Limit | Value | Refusal (message; top-level `code`; HTTP) | `extensions.limit` |
|---|---|---|---|
| complexity (HTTP and websocket) | 4000 | `[ERROR][GRAPHQL][VALID_RANGE] Sorry, too complicated query! Exceeded the maximum allowed complexity.`; `INTERNAL_SERVER_ERROR`; 500. Over the websocket: `Sorry, too complicated query! Exceeded the maximum allowed complexity.` | `complexity` |
| depth (HTTP only, as stock) | 9 | `'<operation name or (anonymous)>' exceeds operation depth limits.`; `GRAPHQL_VALIDATION_FAILED`; 400 | `depth` |

Complexity is `graphql-query-complexity` 1.0.0 as stock configures it. A field with stock's
`@complexity(value, multipliers)` directive costs `(value + cost of its selection) × Π
multipliers`; a multiplier is the named argument's value (a list counts its length), and is
ignored when the argument is absent or null: `transactions { … }` without `first`/`last` is
multiplied by 1 although it returns 20. Variables and their defaults count, and so do the
schema's argument defaults (a subscription's `quantity` is 20). Every other field, leaves
included, costs the cost of its selection (leaves 0). Over an interface or union the most
expensive possible type counts. `@skip`/`@include` drop what they drop. The operation priced
is the one that runs (the named one, or the only one). Stock skips the check over HTTP for any
operation named `IntrospectionQuery`; this indexer skips it only when that operation selects
introspection fields alone, so `query IntrospectionQuery { transactions(first: 5000) … }` is
refused here and answered by stock (a safety difference; introspection costs 0 anyway). A
negative `first`/`last` multiplies by 0 here and no field costs less than 0, so a negative page
cannot cancel an expensive sibling (stock multiplies the negative in; the negative page itself
is refused here, see "Pages"). The values
are stock's own, generated into `crates/kdx-graphql/src/stock_schema.rs` from stock's
`schema.graphql` by `scripts/stock-schema/extract.js`. A ZelCore field stock does not have is
priced as stock prices its lists, `@complexity(value: 1, multipliers: ["first", "last"])`.
Stock's own pagination serves `first: 0` (or `last: 0`) without a cursor as the default page of
20, but its gate multiplies by the argument's value, 0: such a page and everything nested in it
cost 0 there, at every level. This indexer keeps that price at the gate (parity) and bounds the
rows with its read budget instead (below). One case differs (audit F-04): a zero multiplier
next to a non-zero one of the same field is ignored, so `first: 1000, last: 0` (served as
1,000 rows) is priced at 1,000, not 0 as stock prices it; such a page nested under another
page can now be refused with the complexity error where stock answers it. `first: 0` alone,
or both zero, is still 0.
Examples: four aliased `transactions(first: 1000) { edges { node { hash } } }` cost 4000 and are
answered, a fifth page of 1 is refused; `transactions(first: 4001)` is refused; the explorer's transactions-by-account page at `first: 100` costs
(1 + 1 for `result` + 1 for its `block`) × 100 = 300.

Depth is `@graphile/depth-limit` 0.3.1 with `maxDepth: 9` and its other defaults. Only a field
with a selection set adds 1 (leaves and fragments add nothing). Introspection fields are
counted apart (at most 12), list wrappers along a path are summed (at most 4, introspection 3),
and one field coordinate (`Block.parent`) may nest at most twice on a path; introspection
coordinates have graphile's own table (`__Type.ofType` 8, `__Type.fields` 1, ...). Every
operation of the document is checked. The full introspection document (graphql-js 16's
`getIntrospectionQuery()` included) reaches introspection depth 13 through
`fields { args { type { ofType … } } }`, so stock refuses it over HTTP and so does this
indexer, with stock's error. Workaround (stock and ours alike): send a shallower introspection
document. Dropping `args { ...InputValue }` from the `fields` selection, or cutting the
`TypeRef` fragment's `ofType` chain to 7 levels, keeps it within 12; the websocket runs no depth
rule. `scripts/stock-schema/oracle.js` runs stock's own libraries on stock's schema, to check
a document against them.

Stock's order holds: other validation errors come with the depth error; the complexity check
runs only on a valid document. A batch answers HTTP 200 with each answer's errors.

The other limits are this indexer's own, in `[graphql]` (defaults shown). A refusal answers
`extensions.code = "LIMIT"`. Every refusal counts `kdx_graphql_rejected_total{reason}`.

| Limit | Default | `extensions.limit` |
|---|---|---|
| operations per batch | `max_batch = 10` | `batch` |
| query text | `max_query_bytes = 32768` | `query_size` |
| variables (JSON) | `max_variables_bytes = 65536` | `variables_size` |
| request body | 131072 bytes (HTTP 413) | `body_size` |
| fields after fragment expansion | 20,000 | `complexity` ("query has more than 20000 fields after fragment expansion") |
| read budget of a request (a batch: the sum of its operations) | 1,100,000 (fixed) | `budget` ("query reads too much: budget N exceeds 1100000") |
| directives on one field, fragment or spread | 8 | `directives` |
| introspection cost per operation | 100,000 | `introspection_complexity` |
| deferred `totalCount` walks per operation (outer multipliers included) | 2 | `counts` ("more than 2 list totalCounts in one operation") |
| rows of one page: `first`/`last`, a subscription's `quantity` | 1,000 (fixed) | `page` ("a page holds at most 1000 rows (first, last and quantity above it are refused)") |
| NFT holdings of one account | 10,000 (fixed) | `nft_holdings`: partial data, the first 10,000 are answered with this error at the list field ("holdings cut at 10000: the account holds more") |
| page size | `default_page = 20` when `first`/`last` are omitted or 0 | — |
| request deadline | `api.deadline_ms` (5 s), shared by a batch | `TIMEOUT` code |
| `totalCount` walk | half the remaining deadline; an account's transactions/transfers also `count_budget_ms = 200` (1..=60000) if sooner | none: a lower bound is answered (see `totalCount`) |

Rate: a request takes one token of the per-IP rate limit (`api.rate_per_ip`); a batch of N
operations takes N.

The document is parsed and walked before anything else looks at it, and that walk stops at
20,000 fields after fragment expansion. A fragment bomb (each fragment spreading the one before
it twice) is refused in about a millisecond, over HTTP and over the websocket. Stock has no
such bound (its complexity walk expands every spread), nor the directive cap nor the
introspection cost cap.

These refusals are safety, not parity: each is a document stock answers slowly, times out on,
runs out of memory on, or that only an attack sends. Stock has none of them:
- 20,000 fields after fragment expansion, 8 directives, the introspection cost of 100,000, the
  parser nesting of 64, and the query, variables and body size caps (attack shapes);
- more than 2 deferred `totalCount` walks in one operation (each walks a whole account list);
- a request (a batch: all its operations together) whose read budget is over 1,100,000 rows,
  for example many aliases of `transactions(first: 0) { … events(first: 0) … }`, which stock's
  gate prices at 0 while each serves 20 × 20 rows;
- a page, or a subscription `quantity`, over 1,000 rows, where stock's gate passes it (any
  `first` from 1,001 to 4,000 on a page of leaves): stock answers such a page slowly or times
  out. An account's NFT holdings, which take no page argument, are returned in full like
  stock's up to 10,000; above that the first 10,000 come with the `nft_holdings` error, so a
  cut list never looks complete;
- `query IntrospectionQuery` carrying real fields is priced (above).

### Pages

Read as stock's `getPaginationParams` reads them: `first` and `last` count by truthiness, so
`first: 0` or `last: 0` without a cursor is the default page of 20 (forward), as in stock, and
`first: 0, last: 5` is the last 5. With a cursor, the count goes with it as given (`after` with
`first: 0` is an empty page; stock fails on that one when rows follow). A negative `first` or
`last` is `BAD_USER_INPUT` ("first and last must not be negative"). Stock passes it into its
SQL limit: `-1` answers an empty page and below `-1` a database error per resolver, with no
fixed shape to copy, so ours keeps one clear refusal. `first` with `last` (both non-zero),
`after` with `last` and `before` with `first` are `BAD_USER_INPUT`, the cases stock refuses.

The walk also prices the document with this indexer's own cost model: a read budget, about the
rows a request reads. It prices a page as served, with the same function the resolvers size a
page with: as asked up to 1,000; absent, `first: 0` or `last: 0` without a cursor at
`default_page` (20), at every nesting level; with a cursor the count as given (`after` with
`first: 0` costs 0). A page variable bound to nothing, a subscription's `quantity` and the ids
of `nodes` are priced at the most served (1,000). An account's NFT holdings are read when the
account resolves, so `nonFungibleAccount`, `nonFungibleChainAccount` and every NFT id given to
`node`/`nodes` (or an id only a variable gives) cost 10,000 rows (one details read each)
whatever is selected, and `nonFungibleTokenBalances` 10,000 times its selection's cost (each
row at least 1) for the rows it answers (audit F-05: about 110 selected fields per holding
fit under the ceiling). A request above the safety ceiling of 1,100,000 is refused with `budget`
before anything runs; a batch counts the sum of its operations. The ceiling is 11 times the
largest budget of the explorer operation documents in [data/explorer-ops.md](data/explorer-ops.md)
and the compare suite's documents, priced with no variables bound, so every page variable at
1,000 (the largest is kadscan's `blockTransactions-d6646ba8`, 100,101; the largest at its
real page sizes is far below). `crates/kdx-graphql/tests/budget_ceiling.rs` checks the suite's
documents and prices any directory of documents (`KDX_BUDGET_DOCS`). One request's text is
also bounded (32 KB), so the ceiling binds mostly on batches and on NFT holdings. Apart from
the ceiling, only the count-walk bound above refuses; the read slots, the 1 s page scan and the
request deadline bound the work below it. `max_complexity`, `max_depth`,
`introspection_depth` and `max_page` only feed the legacy model.

A `crossChainTransfer` inside another `crossChainTransfer` costs 10 per item, the price of the
store read it would need. It is answered from the pair already loaded and reads nothing.

List pages (`transactions`, `transfers`, `events` and the other lists) share the API's heavy
read slots (half the read threads). Each client key (an IPv4 address or an IPv6 /64, as the
rate limiter keys it) holds at most one of them. Up to 3 more of its heavy reads wait behind
that one. Past that, one is refused at once with `LIMIT`. One page scans for at most 1 s; a
sparse or empty match then ends with `TIMEOUT`: narrow the filter. A connection nested in one
block (`Block.transactions`, `Block.events`), and the root `transactions`, `events` and
`transfers` with a `blockHash` (and no `requestKey`), are not heavy reads: each reads only that
block, by key, on a plain read slot, so one request over many blocks gets every block's page
(R-P5P9-60). `minimumDepth` (it reads the chain tips) and, on `transfers`, a selected
`crossChainTransfer` (it reads the pact's other chain) keep the heavy read for that call. A transaction's own `events` and `transfers` read nothing; they page the
transaction already loaded.

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
| client messages | one per-IP rate token each (pings too); the opening burst below may borrow | close 4429 (`ws_rate`) |
| a client that does not read | 30 s per server message | close 4008 (`ws_slow_reader`) |
| a chain whose feed read fails | 30 ticks in a row | that chain alone is skipped each tick (a WARN line with the chain and its failure count; the other chains keep delivering), then resumes from its tip with a gap |
| a subscriber behind the feed | 1,024 feed items, or behind blocks the ring had to drop to stay within `feed_ring_mb = 64` MB of decoded blocks | one `INTERNAL` error; the subscription goes on from the feed's newest item |
| a query or mutation over the socket | subscriptions only | `NOT_SUPPORTED` error, nothing runs (`ws_query`); send queries over HTTP |
| one subscription message | the HTTP request deadline, shared by all its resolvers | `TIMEOUT` error for that message |

Opening burst: a connection's first `ws_subs_per_conn + 2` messages (`connection_init`, a
full set of subscribes and one more, which the per-connection cap refuses with `LIMIT`) may
borrow up to that many tokens of the client's future refill when its bucket is empty. A
client that fills a connection's subscriptions at once is therefore not closed for rate, even
right after its other connections or HTTP requests used its burst. The borrow is one floor
per client, not a grant per connection: the client's sustained rate stays `api.rate_per_ip`,
its largest burst grows by at most `ws_subs_per_conn + 2`, the refill repays borrowed tokens
before an HTTP request or a message outside an opening burst gets one, and a flood is still
closed 4429.

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
  hex of the hash read as a big-endian number. `target` and `weight` are the node's header
  form, base64url of the 32 little-endian bytes (the oracle agrees; R-P5P9-57 F1);
  `difficulty` is (2^256 − 1) / target; `nonce` and
  `flags` are `Decimal`s sent as strings (the oracle's form); `epoch` and `creationTime` are
  `DateTime`s.
- `neighbors` lists the adjacent blocks ascending by chain id. `parent` is `null` for a
  genesis block. `minerAccount` is the miner's current `coin` row on the block's chain; a
  miner without a row is a field error.
- `blocksFromHeight` with `startHeight` above `endHeight` is an empty list (stock errors).
- Block lists are ordered by height, then chain ascending: `blocksFromHeight` ascending by
  height, as the oracle lists it (R-P5P9-57 M2a); `blocksFromDepth` and
  `completedBlockHeights` descending. `chainIds` absent or `[]` means every chain. Forward
  and backward pages hold the same blocks. The
  order of blocks within one height is a contract choice (R3E-8).
  `blocksFromDepth` lists blocks at least `minimumDepth` below their chain's stored tip.
  `completedBlockHeights` takes the newest `heightCount` (at most 100) heights: with
  `completedHeights: true` (default) only heights at or below every requested chain's tip.
- `Block.transactions` lists the block's user txs (coinbase is not a `Transaction`);
  `Block.events` lists every event of the block, coinbase included. Both page with the
  defaults above (20, at most 100), not the 500 the schema text mentions. Both are in the
  node's order: the coinbase first (the first leaf of the payload's `outputsHash`, executed
  before the user txs), then the user txs in payload order, each tx's events by ascending
  order index. Root lists with `blockHash` (and no `requestKey`) list their one block in
  the same order.
- `lastBlockHeight` is the highest stored tip.
- `networkInfo`: `networkId` from `network`, `networkHost` from `graphql.network_host`,
  `apiVersion` `"0.0"`, `transactionCount` (the last background count, as `/stats`; an error before the first one) and `coinsInCirculation` as `/stats`;
  `totalDifficulty` is the sum over chains of the tip block's `weight`, `networkHashRate` the
  sum over chains of the tip block's difficulty divided by 30 s.
- `node(id)`/`nodes(ids)` (at most 100) resolve every type. An id that cannot be read (not
  base64 of `<Type>:<key>`, an unknown type, or a malformed key) is `null`, as the stock
  indexer answers, and so is an object not stored; `nodes` answers `null` in that id's place.
  A well-formed id whose lookup is ambiguous (a `Signer` of a genesis tx stored on several
  chains) keeps its `BAD_USER_INPUT` error. An integer id of any size is an id like a string
  (so `9223372036854775808` is `null`, not an error); a fraction or a boolean is refused.
  Coinbase events and transfers resolve through their block. The id of an NFT transfer
  (`transfers(isNFT: true)`) resolves to that transfer (audit F-51).

## Subscriptions

Subscriptions read a feed that polls the stored chain tips every `feed_poll_ms` (1,000), so
`serve` and replicas serve them as `run` does.

The feed decodes each new block's txs once and shares them with every subscriber through a
broadcast ring of 1,024 items. The ring is byte-bounded (`feed_ring_mb`, 64 MB by default):
before a block would take the decoded blocks it holds past the budget, the feed pushes the
oldest blocks out of the ring with empty items. Only a subscriber that has not read those
blocks yet gets the lag error; a subscriber that keeps up skips the empty items and loses
nothing. Full blocks of maximum-size txs therefore cannot pin more than the budget (plus one
block over the budget, alone) behind a client that stops reading. `kdx_graphql_feed_ring_bytes`
is the current estimate.

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
| `Transfer.amount` | `Decimal` | string, plain notation without trailing zeros (`"0.00000236"`, `"26.57382"`) |
| `Transfer.creationTime` | `DateTime` | the tx's `meta.creationTime` (whole seconds); a coinbase transfer has its block's time cut to whole seconds |
| `ExecutionPayload.code` | `String` | the code JSON-quoted (`"\"(coin.transfer …)\""`) |
| `Signer.scheme` | `String` | `""` when the signer names no scheme, else the command's scheme |
| `Event.parameterText` | `String!` | numbers in JavaScript layout (`0.00000236`, `1e-7`); `parameters` keeps the node's numbers |
| `Block.nonce`, `Block.flags` | `Decimal` | string |
| `Block.difficulty` | `BigInt` | always a JSON number, also above 2^53 |
| `FungibleChainAccount.chainId` | `String` | string (already) |

A union or interface field (`TransactionCommand.payload`, `Transaction.result`, the `guard`
fields, `node` and `nodes`) whose fragments select none of the value's fields answers `{}`, as
the oracle does (`payload { ... on ExecutionPayload { code } }` on a continuation), not
`null`; a missing value is still `null`.

Everything else keeps the scalar rules (`Block.height`, `chainId`, `orderIndex`, `result.height`
stay numbers; `totalBalance` and `balance` stay numbers). Error codes ride in
`extensions.code` (GraphQL spec) and are copied to a top-level `code` on each error, the place
the oracle puts them.

## Transactions, events and transfers

- **Lists.** `transactions`, `transactionsByPublicKey`, `transfers`, `events` and the account
  connections walk the same posting lists as the native API. Order: newest first by
  (height, chain, position in the block); rows of one tx (events, transfers) newest first too,
  by descending event position (order index 1 before 0), as the deployed oracle lists them.
  The exception is a list of one block (`blockHash` without `requestKey`, and the `Block`
  connections): it is in the node's order, coinbase first (see "Blocks and network").
  `first`/`after` pages walk down from the newest row; `last`/`before` pages first collect
  the keys between the cursor and the newest row (keys only, at most 200,000, else `LIMIT`).
  Every walk checks the request deadline every 64 keys and answers `TIMEOUT` past it.
- **Cursors** are the native API's tokens (tx key, row position and a fingerprint of the
  row's block): portable between instances, refused with `BAD_USER_INPUT` after a reorg of
  that block.
- **Coinbase.** Coinbase txs are not `Transaction`s (as in Kadena graph): transaction lists
  leave them out, and `Event.transaction`/`Transfer.transaction` of a coinbase row is `null`.
  Their events and transfers are listed with order index 0 and the node's coinbase request
  key (the `reqKey` of the stored coinbase output, as the oracle lists it; R-P5P9-57 F4).
  Event and Transfer ids carry that key; `node(id)` still accepts ids with `<coinbase>`.
- **`transactions` sources**: `requestKey`, else `blockHash`, else `accountName` with
  `fungibleName` (txs with a transfer of that token on the account's side), `accountName`
  alone (txs touching the account), `fungibleName` alone (txs with a `<token>.TRANSFER`
  event); with none of them, every user tx from the tip down (at most 20,000 heights), as
  a latest-transactions list asks. `chainId`, `minHeight`, `maxHeight` and
  `minimumDepth` (height ≤ the chain's tip − depth) filter any source. With `accountName`,
  only the txs the account sent (`meta.sender`) are listed and counted, as the oracle does
  (GQ-19, R-P5P9-57 M1).
- **Sender lists and the sender index (FIX-2).** `transactions(accountName)`,
  `transactions(accountName, fungibleName)` and `FungibleChainAccount.transactions` read the
  account's `sndr` postings (with `fungibleName`, a merge of its `sndr` and `xfer` postings)
  once the sender index is complete: every segment range has its `sndr.pst` and the hot
  store has `sndr_hot = 1` (gauge `kdx_sender_index_ready`). No record is decoded to pick
  the rows, so a receive-heavy account answers as fast as any other, and `totalCount` is a
  key-only walk. Until then (a store before `segments add-index`) the old path serves: the
  account's `acct` (or `xfer`) postings filtered on each record's `meta.sender`, which a
  receive-heavy account can run past the 1 s scan window (`TIMEOUT`, no data). Both paths
  answer the same edges, cursors, `pageInfo` and `totalCount`. `[graphql] sender_index =
  false` forces the old path (default `true`; used to compare the two on a host); it also
  applies to REST `role=sender`. With `requestKey` or `blockHash` the sender filter runs on
  that key's or block's txs, as before.
- **Combined filters (audit F-30).** Every supplied filter applies, as stock ANDs them in
  its SQL (indexer-kadena `90a4be1`: `transaction-query-builder.ts`,
  `transfer-db-repository.ts`, `event-query-builder.ts`). `requestKey` picks the source,
  and a `blockHash` beside it keeps only the key's copy in that block (another block's hash
  lists nothing; a malformed one is `BAD_USER_INPUT`), on `transactions`, `transfers` and
  `events`. On `transactions`, `fungibleName` beside `requestKey` or `blockHash` keeps the
  txs with an event of that module (stock's `EXISTS` on `Events.module`); with any other
  source the posting list already holds only that token's txs. `transfers(isNFT: true,
  accountName)` checks `fungibleName` as every other path does.
- **`transfers`** use the same sources; rows are the tx's transfers of `fungibleName` with
  `accountName` on either side when given. `crossChainTransfer` is the other half of a
  cross-chain transfer (the `cont` step of the pact, or the tx that started it), `null`
  while that half is not indexed. The outgoing half reads the pact's steps oldest first and
  stops at the one that succeeded, so failed retries after it cost nothing; a lookup reads
  at most 1,000 steps, and one request at most 2,000 for all its `crossChainTransfer`
  fields, past which a lookup answers `null`. Anyone can post failed `cont` txs naming a
  pending pact (it costs only gas): more than 1,000 of them before the one that succeeds make
  the outgoing link `null` here where stock links it. Griefing only, bounded, not fixed.
- **`events`**: `qualifiedEventName` is required. `parametersFilter` accepts only
  `{"array_starts_with": <value or array>}` (the event's first parameters equal the value or
  the array's values); anything else is `UNSUPPORTED_FILTER`. When the first value is a
  string, the account's `acct` postings are walked (fast) only if they hold every match: the
  string is a Pact principal (posted wherever it appears), or the event is named `TRANSFER`
  (a non-principal name is posted as a transfer side). Otherwise the event name's postings
  are walked and filtered on the parameters (audit F-27, option B). Known gap: a
  poly-fungible `TRANSFER (id, from, to, amount)` whose token `id` equals a non-principal
  string, and does not also name it as a side, is not listed for that string.
- **`transaction(requestKey)`**: `null` when not indexed; a request key stored in several
  genesis blocks answers `BAD_USER_INPUT` unless `blockHash` picks one copy.
- **`totalCount`** is computed only when selected: a count walk of at most 10,000 rows
  within half of the remaining deadline. An account's `transactions` or `transfers` list is
  bounded further by `count_budget_ms` (200 ms by default, counted from when the read
  starts), whichever is sooner; other lists keep the half-deadline limit. Larger lists show 10,000. Cold records decode
  at about 1 ms each, so an account with more than a few hundred rows to decode answers a
  lower bound. A sender list served from the sender index counts keys only, no decode. When the time share runs out, `totalCount` answers the rows counted so far (a
  lower bound, never below the edges served), the edges are kept, and the response carries
  `"extensions": {"totalCountLowerBound": true}` (R-P5P9-14). A block's or a request key's
  list counts with its page within the scan window; when the deadline cuts that count it is
  flagged the same way, never below the edges served (audit F-50).
- **Text forms.** `goodResult`, `badResult`, `continuation`, `metadata`, `Event.parameters`,
  payload `data` and capability `args` are compact JSON text of the node's values (numbers
  keep their exact text). `Transfer.amount` is the decimal of the event's amount parameter.

## Accounts

- `fungibleAccount`, `fungibleChainAccount(s)` and `…ByPublicKey` read the reconciled
  balance rows (spec §3.9); resolvers never call the node. Stale rows are served; chains
  without a row are absent from `chainAccounts`; with no row the account is `null`.
  `fungibleChainAccounts` with `chainIds` absent (also a declared variable sent without a
  value) or `[]` lists every chain's row, and answers `[]`, not `null`, when there is none;
  with `chainIds` set the rows come in the order asked (each chain once), as stock lists them;
  `fungibleChainAccount` on a chain without a row is `null`.
- `fungibleName` is trimmed before use, here and on the transaction and transfer lists, as
  stock reads it: `"coin "` names `coin`. A name of spaces alone is `BAD_USER_INPUT`.
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
  The 100 are counted after the module filter: accounts of other modules guarded by the same
  key (spam needs no signature) never push the asked module's accounts out. The walk over the
  key's accounts stops at 100 matches or after its 1 s scan window; when it stops with
  accounts of the key left unread, the response carries `"extensions": {"truncated": true}`.
- `FungibleAccount.transactions`: txs with a transfer of the token on the account's side;
  `FungibleChainAccount.transactions`: txs on that chain the account sent (`meta.sender`),
  as the schema says.
- `nonFungibleAccount`/`nonFungibleChainAccount` list positive NFT holdings (all of them, as
  stock, up to the 10,000-row safety cap, past which the first 10,000 come with a
  `nft_holdings` `LIMIT` error); `version` is `v1` (`marmalade.ledger`), `v2` (`marmalade-v2.ledger`) or the
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
| `balance(accountName, module, chainIds, first/last/after/before): QueryBalanceConnection!` | an account's present fungible balances, one `BalanceNode {accountName module chainId balance}` per module and chain, listed by module name then chain, filtered by `module` and `chainIds` (at most 100, each once; `[]` means every chain); pages as every other connection (`first: 0` with `after` serves no row); at most 4,096 rows per account (`LIMIT` above). Without `accountName` (a module's holders): `NOT_SUPPORTED`, as the store keeps no holders index |
| `transactionsByPactCode(pactCode, first/after): QueryTransactionsByPactCodeConnection!` | txs whose own exec code contains `pactCode` (`%`/`_` wildcards), newest first, through the `/txs/search` engine: its pattern checks, per-client search gate, code index or bounded newest-first scan, caps and time budget. Rows are `TransactionSummary {requestKey height chainId canonical creationTime badResult sender gas gasLimit gasPrice code}`. Forward paging only (`last`/`before`: `BAD_USER_INPUT`). The search gate counts per client
address (HTTP, or the websocket's accepted connection); an operation without a known address
is refused (`LIMIT`). A page the search cut short carries `"extensions": {"searchTruncated": true}` and `pageInfo.endCursor` resumes it. `NOT_SUPPORTED` when the server has no search context |
| `lastTokenPriceInKda(moduleName): Decimal` | `NOT_SUPPORTED`: no price data |
| `Subscription.newBlocks(quantity: Int)` | at most `quantity` (1 to 1,000; above it the `page` refusal) blocks per message, the newest of the batch; absent keeps Kadena's behaviour |
| `Subscription.transactions(quantity: Int = 20): [Transaction!]` | the user txs of newly committed blocks on every chain, newest first, at most `quantity` (1 to 1,000; above it the `page` refusal) per message; a message per feed batch that has any |

`pactQuery` stays `NOT_SUPPORTED`.

## Fields compared with the node, not an oracle

Tip-relative answers change from second to second, so the comparison harness checks them
against the node or the native API: balances and guards, `networkInfo`, `lastBlockHeight`,
`completedBlockHeights`, `blocksFromDepth` and subscriptions.
