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

- `pactQuery` and `gasLimitEstimate` run client Pact through the node's `/local`, as the stock
  indexer does (owner decision 2026-11-26, which replaces the earlier "never runs Pact for
  clients" rule). See [Pact passthrough](#pact-passthrough). The passthrough is off by
  default (`[graphql.pact] enabled = false`), and then both answer `NOT_SUPPORTED`. An
  operator turns it on with `enabled = true`. `gasLimitEstimate(input: [])` runs no Pact and answers
  `[]`, as the stock indexer does.
- `Transaction.result` is never a `TransactionMempoolInfo`: the indexer has no mempool, and a
  transaction that is not indexed is `null`.
- Page sizes: as in the stock indexer, a connection returns 20 items when `first`/`last` are
  omitted (or 0), and any larger page the complexity limit admits is answered in full up to
  the 1,000-row safety cap (see Limits). `pageInfo.hasNextPage` stays correct.
- The schema has no "as of height" field, so GraphQL answers do not carry one (the native API
  does).
- Fields not served yet answer `NOT_SUPPORTED` with the rest of the operation intact.
- Balances of `lago.kwUSDC`, `lago.USD2` and `lago.kwBTC` (R-G4-02). The 489 funded rows on
  chains 0–2 sit in the modules' old `token-table`, but the live modules are empty stubs with
  no `get-balance`, so no node API can read them. They are excluded from balances, as are the
  modules whose name the node cannot parse (non-ASCII): no row is served for them, where stock
  may list one (it listed `lago.kwUSDC` at 0).
- `balance(...)` lists fungible balances only (R-G4-03). Stock's `balance(...)` with no
  `module` also lists marmalade ledger rows with balance `"0"`, one per token id. Ours lists
  fungible balances only; NFT holdings come from `nonFungibleAccount` and
  `nonFungibleChainAccount`.
- Row order within a block (R-P5P9-151). A newest-first list that spans several blocks
  (`transactions`, `events`, `transfers` without `blockHash`, the account connections) lists
  the rows of one block in reverse node order; the stock indexer orders those rows by creation
  time. Such lists have no coinbase rows, except `transactions(isCoinbase: true)`: there the
  coinbase is the first row of its block, as stock lists it (its creation time is the
  block's), then the user txs in reverse node order. So a page of one block that is smaller
  than the block (kadscan asks `first: 25`) still shows the coinbase. A coinbase row's cursor
  sorts above every user tx of its block; user rows keep the cursors of the list without the
  flag. A list of one block (`blockHash` without `requestKey`, the `Block` connections) is in
  the node's order, coinbase first, as the node lists it. The stock indexer's order there is
  not the node's: in the G4 replay of real traffic, the first page of a nested
  `Block.transactions` list of a block with more transactions than the page held a different
  subset of the block on stock than on ours (51 records), and ours follows the node. For
  blocks with more transactions than one page, only the first page was compared in G4 (515
  records: the 51 above, and 464 whose lists never finished); the rest of those lists was not
  compared with stock. A full page-walk comparison is planned.
- Root fields of the stock indexer that are not in Kadena's schema are not served: the DEX
  fields `pools`, `dexMetrics` and `tokenPrices` (one real request asked for them in the G4
  replay) are unknown fields: the document answers `GRAPHQL_VALIDATION_FAILED` (HTTP 400, see
  [Errors](#errors)), where stock answers them. Introspection
  (`__schema`) lists Kadena's schema plus the ZelCore extensions below, so its answer differs
  from stock's, whose type list has those fields and lacks ours. The DEX price that kadscan
  reads is served as `lastTokenPriceInKda` (see [Token price](#token-price)).

## Errors

Errors carry `extensions.code`:

| Code | Meaning |
|---|---|
| `NOT_SUPPORTED` | a schema field this indexer does not serve |
| `BAD_USER_INPUT` | a bad argument, cursor or request body |
| `LIMIT` | a resource limit; `extensions.limit` names it (table below) |
| `TIMEOUT` | the request deadline passed |
| `COUNT_TIMEOUT` | `totalCount` could not be counted exactly before the request deadline; the field answers no number (see `totalCount`) |
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
| depth (HTTP and websocket; stock: HTTP only) | 9 | `'<operation name or (anonymous)>' exceeds operation depth limits.`; `GRAPHQL_VALIDATION_FAILED`; 400. Over the websocket the same message comes as the subscription's error | `depth` |

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
`TypeRef` fragment's `ofType` chain to 7 levels, keeps it within 12. Stock runs no depth rule
over the websocket; this indexer runs the same rule there (audit F-07), since a subscription
re-runs for every block, so a subscription stock opens at depth 10 or more is refused here. `scripts/stock-schema/oracle.js` runs stock's own libraries on stock's schema, to check
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
| `totalCount` walk | the request deadline (`api.deadline_ms`); no row cap | `COUNT_TIMEOUT` at the field (see `totalCount`) |

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
(or the legacy `graphql-ws`). The subprotocol is chosen once, over every offered value, in the
server's order (`graphql-transport-ws` first) and exact case; the answer's
`Sec-WebSocket-Protocol` names the one the session speaks (audit F-26). An offer of neither
name in that case (`GRAPHQL-WS` included) answers 400. Each limit below closes or refuses as shown and counts
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
| no frame from the client (the server pings every 30 s; a pong counts) | 90 s (fixed) | close 4000 (`ws_silent`) |
| client message or frame | 64 KB | close 1009 (`ws_message`); a malformed frame closes 1002 |
| client messages | one per-IP rate token each (protocol `ping` messages and the client's websocket Ping frames too; a Pong answering a server ping takes none, any other Pong takes one); the opening burst below may borrow | close 4429 (`ws_rate`) |
| a client that does not read | 30 s per server message or ping | close 4008 (`ws_slow_reader`) |
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

Depth, complexity and the size limits apply to subscription documents as to queries. A
socket's operations run as requests of its client, so their heavy reads count against that
client's one heavy slot as its HTTP requests' do (audit F-06). Every
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
- `Block.transactions` lists the block's user txs (a coinbase is a `Transaction` only in
  `transactions(isCoinbase: true)`);
  `Block.events` lists every event of the block, coinbase included. Both page with the
  defaults above (20, at most 100), not the 500 the schema text mentions. Both are in the
  node's order: the coinbase first (the first leaf of the payload's `outputsHash`, executed
  before the user txs), then the user txs in payload order, each tx's events by ascending
  order index. Root lists with `blockHash` (and no `requestKey`) list their one block in
  the same order. The stock indexer orders a block's transactions otherwise, so the first
  page of a block larger than the page can hold other rows there (seen in the 51
  paged blocks compared in G4) (see Row order under
  [Schema](#schema)).
- **Orphan blocks.** `blocksFromHeight` lists the canonical blocks only (the store keeps no
  others, and every block checked in the G4 replay matched the node's canonical header: 63,582 ok, 0 bad). The stock
  indexer also lists non-canonical blocks (60 records in the G4 replay): the same chain and
  height twice (an orphan next to the canonical block), or an orphan that takes a slot of a
  20-row page so that another chain's block drops out of it. Its `canonical: false` rows are
  not in ours' answer.
- **Stock crash.** Some `blocksFromHeight` documents make the stock indexer answer
  `INTERNAL_SERVER_ERROR` with `base64url.replace is not a function` (7 records in the G4
  replay). Ours answers them with the blocks.
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

**At the cutover from the stock indexer.** Stock's cursors are another format
(`base64("<creationTime>:<id>")`), and ours cannot read them. A client that holds a stock
cursor when its traffic moves to kda-indexer sends it once in `after`/`before` and gets
`BAD_USER_INPUT` `invalid cursor` (57 such requests in the G4 replay, all in a nested
`Block.transactions(after:)`); it restarts from the first page and pages on with ours'
cursors. The REST `next` tokens have the same one-time transient (see
[load-balancer.md](load-balancer.md#4-apply-check-roll-back)).

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
| `TransactionMeta.gasLimit` of a coinbase row (`transactions(isCoinbase: true)`) | `BigInt` | the JSON number `0`, as stock (a stock type quirk; user rows stay strings) |
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
  `transactions(isCoinbase: true)` lists each block's coinbase before the block's user txs
  (see Row order).
  `first`/`after` pages walk down from the newest row. `last`/`before` pages of a posting
  list (an account, an account with `fungibleName`, `fungibleName` alone, an event name, a
  public key, and the account connections) read that list oldest first, from the cursor or
  the oldest row, and stop at the page, so `last: N` costs N rows plus the rows the filters
  drop. The other sources, `transactions(isCoinbase: true)` and a list with no account, key,
  event name or `fungibleName` argument (the height walk, `events(moduleName)` alone too),
  first collect the keys between the cursor and the newest row (keys only, at most 200,000,
  else `LIMIT`; stock answers those).
  Every walk checks the request deadline every 64 keys and answers `TIMEOUT` past it.
- **Cursors** are the native API's tokens (tx key, row position and a fingerprint of the
  row's block): portable between instances, refused with `BAD_USER_INPUT` after a reorg of
  that block.
- **Coinbase.** Coinbase txs are not `Transaction`s (as in Kadena graph): transaction lists
  leave them out, except `transactions(isCoinbase: true)` (see the extensions table).
  `Event.transaction`/`Transfer.transaction` of a coinbase row is `null`, also when reached
  from a listed coinbase's `result.events` or `result.transfers`, and `node(id)` with a listed
  coinbase's `Transaction` id answers `null` (stock most likely resolves both).
  Their events and transfers are listed with order index 0 and the node's coinbase request
  key (the `reqKey` of the stored coinbase output, as the oracle lists it; R-P5P9-57 F4).
  Event and Transfer ids carry that key; `node(id)` still accepts ids with `<coinbase>`.
- **`transactions` sources**: `requestKey`, else `blockHash`, else `accountName` (txs
  touching the account), else `fungibleName` (txs with a `<token>.TRANSFER` event, all
  history); with none of them, every user tx from the tip down (at most 20,000 heights), as
  a latest-transactions list asks. Beside `accountName`, `requestKey` or `blockHash`,
  `fungibleName` keeps the txs with any event of that module, as stock does (`EXISTS` on
  `Events.module`, indexer-kadena `90a4be1`). **Known difference (deliberate):**
  `fungibleName` without those (alone, or with `chainId`, heights or `minimumDepth`) lists
  the txs with a transfer of the module, where stock lists the txs with any event of it.
  Matching stock there would need a per-module posting index; until one exists, a walk
  over every tx would lose full-history coverage, so the transfer postings stay. `chainId`, `minHeight`, `maxHeight` and
  `minimumDepth` (height ≤ the chain's tip − depth) filter any source. With `accountName`,
  only the txs the account sent (`meta.sender`) are listed and counted, as the oracle does
  (GQ-19, R-P5P9-57 M1).
- **Sender lists and the sender index (FIX-2).** `transactions(accountName)`,
  `transactions(accountName, fungibleName)` and `FungibleChainAccount.transactions` read the
  account's `sndr` postings (`transactions` with `fungibleName` keeps those with an event of
  that module; `FungibleChainAccount.transactions` is unchanged by this)
  once the sender index is complete: every segment range has its `sndr.pst` and the hot
  store has `sndr_hot = 1` (gauge `kdx_sender_index_ready`). No record is decoded to pick
  the rows, so a receive-heavy account answers as fast as any other, and `totalCount` is a
  key-only walk. Until then (a store before `segments add-index`) the old path serves: the
  account's `acct` postings filtered on each record's `meta.sender`, which a
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
  `events`. On `transactions`, `fungibleName` keeps the txs with an event of that module
  (stock's `EXISTS` on `Events.module`) when the source is `accountName`, `requestKey` or
  `blockHash`; with `fungibleName` as the only source it keeps txs with a transfer of that
  module (see the known difference above). `transfers(isNFT: true,
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
  genesis blocks answers `BAD_USER_INPUT` unless `blockHash` picks one copy. A
  `requestKey` that is not a hash (for example the base64-wrapped coinbase key stock lists
  as a coinbase `hash`) finds nothing, as in stock, which compares the text with its stored
  keys: `transaction` answers `null` and `transactions`, `transfers` and `events` with that
  `requestKey` list no rows, all without an error. A coinbase is never answered by
  `transaction(requestKey)`, in stock or here. Other bad arguments beside it (a malformed
  `blockHash`, `minimumDepth` out of range) keep their errors.
- **`totalCount`** is exact, as stock's (GQ-10, owner ruling 2026-11-26): it is the number
  of rows the full walk of the list returns, with every filter applied, and has no cap. It
  is computed only when selected: from the stored per-key counts where they hold the list
  (below), else by a count walk that runs to the request deadline (`api.deadline_ms`, 5 s).
  A walk the deadline cuts answers a `COUNT_TIMEOUT` error at
  `totalCount` (the error has the field's path; the field is left out of the connection
  object, which is `null` when `totalCount` was its only field; the rest of the response
  stays); it never answers a
  partial number and sets no response flag. A block's or a request key's list counts with
  its page, under the same rule (audit F-50). There is no `totalCountLowerBound` flag and no
  `count_budget_ms` setting any more (an old config file that sets it still loads; it is
  ignored).
  - *Key-only counts* (no record is decoded): `transactions(accountName)` and
    `FungibleChainAccount.transactions` served from the sender index,
    `transactionsByPublicKey`, `transactions(isCoinbase: true)` beside them, and these with
    `chainId`, `minHeight`, `maxHeight` or `minimumDepth`. Measured at about 17 million keys
    a second (50,014 keys in 3 ms, release build, warm), they finish well inside the
    deadline for any list the store holds.
  - *Decoding counts* (each tx's record is read to count its rows): `transfers` and `events`
    lists (an account, `fungibleName`, `qualifiedEventName`, `parametersFilter`,
    `orderIndex`), `transactions(accountName, fungibleName)`, NFT filters, and sender lists
    on the old path. They read frozen records ahead 256 at a time. Measured at about 220,000
    rows a second on warm records (50,048 transfer rows in about 230 ms); cold records on the
    production store counted about 12,500 rows a second (2,507 rows in 200 ms). **Known
    difference:** a decoding list with more rows than the deadline can read cold (roughly
    50,000 rows and up, fewer on a busy host; for example `events(qualifiedEventName:
    "coin.TRANSFER")` over all heights, or the transfers of an exchange account) answers
    `COUNT_TIMEOUT` where stock answers a number, unless the stored counts answer it (next
    item).
  - *Stored counts* (no walk; PR #48): a table of rows per (list kind, key, chain), user and
    coinbase rows apart, kept by the tail at each block and each reorg and built once by
    `kda-indexer reconcile-counts` ([operations.md](operations.md#stored-totalcount-counts)).
    While it is marked ready, these lists answer `totalCount` from it, at any length, as a
    sum over the chains (or one chain's row):
    - `transfers(accountName)` and `FungibleChainAccount.transfers`; `transfers(accountName,
      fungibleName)`; `transfers(fungibleName)`;
    - `transactions(accountName)` and `FungibleChainAccount.transactions` (sender index);
      `transactions(fungibleName)`; `transactionsByPublicKey`;
    - `events(qualifiedEventName)`;
    - each with `chainId`, and the transaction lists with `isCoinbase: true`.

    These keep the walk (and its `COUNT_TIMEOUT` on very long lists): `minHeight`,
    `maxHeight`, `minimumDepth`; `transactions(accountName, fungibleName)` (the module filter
    reads each record); `events` with `parametersFilter`, `orderIndex` or `moduleName`;
    `transfers(isNFT: true)`; sender lists on the old path (no sender index); lists without
    a selective argument (the height walk); a block's or a request key's list (counted with
    the page). Per-height buckets were left out: a height range would still walk its two edge
    buckets, which on the lists that need them (a popular event) is the same cut. While the
    counts are not ready (never built, or cleared by `backfill`, `rebuild-range`,
    `segments add-index` or `hot drop-family`), every list walks as above: slower, never
    wrong.
- **Text forms.** `goodResult`, `badResult`, `continuation`, `metadata`, `Event.parameters`,
  payload `data` and capability `args` are compact JSON text of the node's values (numbers
  keep their exact text). `Transfer.amount` is the decimal of the event's amount parameter.
  The keys of a JSON object inside these texts may come in another order than in the stock
  indexer's text; the values are the same.

### Transaction fields where stock differs from the node

Ours serves a transaction's command and result as the node holds them. The G4 replay of real
`transaction` and `blocksFromHeight` requests found these fields where the stock indexer's
value is not the node's. Each class was checked against the node's command or output for
sampled request keys (most of them per record by the comparison harness), and the node
agreed with ours every time:

| Field | Ours (the node's value) | Stock indexer |
|---|---|---|
| `cmd.nonce` holding double quotes | the command's nonce as it is | one layer of JSON quoting removed from the whole nonce, or every double quote inside it removed |
| `cmd.signers[].scheme` | `""` when the signer names no scheme, else the command's scheme | `""` also when the command names one |
| `cmd.signers[].address` | the signer's `addr` from the command | `null` (51 records) |
| `cmd.signers` order | the command's order (`orderIndex` 0, 1, …) | at times another order (1 record listed 1, 0); the same signers as a set |
| `payload.pactId` of a continuation | the command's pact id | `null` |
| `payload.rollback` of a continuation | the command's `rollback` | another value (196 records) |
| exec `payload.data` | the command's data | another value (71 records) |
| continuation `payload.data` that the node holds as `null` | `"null"` | `"{}"` |
| `result.transactionId` of a failed tx | `null` (the node has no tx id for it) | `"0"` |
| `result.goodResult` when the node's result data is the empty string | `"\"\""` | `null` |
| numbers inside `goodResult`, `continuation`, `Event.parameters` and capability `args` | the node's decimal text | another text for the same value (more or fewer fraction digits); equal as decimals |
| `result.badResult` of a failed tx | the node's stored error: no `callStack`, `info` as the node serves it (`<toplevel>:0`) | the error as produced at execution time: with a `callStack`, and a source position in `info` (117 records) |

The failed-tx class is accepted by owner ruling with the node as the reference; the others
were read on the node per record or on samples when each rule was made. Floats are listed
under [Scalars](#scalars), and decimal text under the compat notes of [api.md](api.md#compat-notes).

## Accounts

- `fungibleAccount`, `fungibleChainAccount(s)` and `…ByPublicKey` read the reconciled
  balance rows (spec §3.9); resolvers never call the node. Stale rows are served; chains
  without a row are absent from `chainAccounts`; with no row the account is `null`.
  `fungibleChainAccounts` with `chainIds` absent (also a declared variable sent without a
  value) or `[]` lists every chain's row, and answers `[]`, not `null`, when there is none;
  with `chainIds` set the rows come in the order asked (each chain once), as stock lists them;
  `fungibleChainAccount` on a chain without a row is `null`.
- **Zero rows follow stock's two rules.**
  - `fungibleAccount` lists every row that exists on chain, at any balance: a row the node
    holds is listed whatever its balance, 0 included, whether the account was funded once and
    drained or created and never funded on that chain; only a chain where the node has no row
    (reported absent) is left out. The same holds for `chainAccounts`,
    `fungibleChainAccounts`, `fungibleChainAccount`, the `…ByPublicKey` lists, `node(id:)` for
    `FungibleAccount` and `FungibleChainAccount` ids and `Block.minerAccount`.
    `fungibleAccount` is non-null for every account that exists on chain (`totalBalance` 0
    when every row is at 0), so its `transfers` and `transactions` stay reachable after it is
    drained. The native `/accounts/{account}/balances` and `/keys/{pubkey}/accounts` follow
    this rule too ([api.md](api.md)).
  - `balance(...)` lists every row above 0, plus the rows at 0 that a TRANSFER event touched:
    the account's transfer list in that module (what `transfers(accountName, fungibleName,
    chainId)` reads) has an entry on the row's chain. A row at 0 that only `create-account`,
    `rotate` or a token's own `create-account` wrote is left out, as stock leaves it out.
    This is worked out when the query runs, from the transfer postings; nothing is stored.
    Our transfer postings come only from node events, which start at the events fork
    (1,138,000 on mainnet), as stock's TRANSFER rows do, so a row drained before the fork is
    not counted as touched in either. The check reads at most 10,000 transfer keys per
    account and module, newest first, and at most 40,000 for all modules of one request;
    it stops once every zero row's chain is found. A zero row still unresolved at either
    cap is listed (it exists on chain) and counted in
    `kdx_graphql_balance_touch_capped_total`. The request deadline ends the check with a
    deadline error, never with a cap. Pages, `chainIds` and `module` walk only the
    rows this rule keeps; the 4,096-row cap counts every stored row of the account, as before.
  Rows come from the reconciler's reads of the accounts ingest sees named: transfer sides,
  senders, signers, event principals, the D3 receivers, and since the zero-rows fix the
  account a `coin.create-account` call creates (D3 rule 2c: the code literals, then the
  `exec.data` strings, at most 64; never `result.data` or a keyset predicate). A store whose
  D3 pass ran before that rule gets those rows from `reconcile-balances --rewalk coin`
  ([operations.md](operations.md)). An account created inside another module's code, with
  no event or exec string naming it, is still not seen.
  Measured against stock (WP-L, post-#44 replays): stock's `fungibleAccount` lists every row
  the node holds, created-and-never-funded ones included; stock's `balance(...)` lists the
  zero rows a TRANSFER event touched (190 of 190) and none of the node's zero rows without
  one (0 of 13).
- **Funded rows stock lacks.** The stock indexer misses some non-zero balance rows that the
  node holds and ours lists, mostly token credits that no TRANSFER event names (for example
  `kaddex.skdx`, `kdlaunch.token`, `mok.token`, `free.kishu-ken`; 17 records in the G4 replay,
  each sampled row read on the node with `get-balance` and equal to ours). See
  [operations.md](operations.md#token-receivers-pass-d3) for how ours finds those receivers.
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
| `transactions(isCoinbase: Boolean)` | `false` or absent: user txs (as without it). `true`: the same list with the coinbases added, as stock (it drops its `sender != 'coinbase'` filter; nothing else changes, so beside `accountName` it lists the account's txs only). A coinbase row answers as stock's: `hash` is the node's coinbase `reqKey` text, `meta.sender` `coinbase`, `chainId` its chain, `creationTime` its block's time in whole seconds, `gasLimit`, `gasPrice`, `ttl` and `gas` 0 (`meta.gasLimit` is the JSON number `0`, not the string `"0"`: a type quirk of stock, whose coinbase rows answer a number while every user tx's `gasLimit` is a string; `gasPrice` and `gas` stay strings, and `ttl` stays a string, as stock's coinbase `ttl` was never recorded), `nonce` empty, no signers or sigs, payload code and data `{}`, `networkId` the configured network; its result, events and transfers are the stored coinbase output. Order: the coinbase is the first row of its block (see Row order), so kadscan's `first: 25` page of one block shows it even when the block has 25 or more user txs; user rows keep the order and cursors of the list without the flag. Paging, `totalCount` (an exact count, coinbases included; stock's is an estimate from its height counters) and cost caps are those of the list without it |
| `transfers(isNFT: Boolean)` | `true`: NFT transfers instead of fungible ones, the four-parameter `TRANSFER (id sender receiver amount)` events of the NFT ledgers (`[nft] ledgers`). An account's come from its `acct` postings; with neither account, module, block nor request key, heights are walked down from the tip (as `transactions` without a selective argument). `crossChainTransfer` is `null` for them |
| `Transfer.tokenId: String` | the NFT transfer's token id; `null` for a fungible transfer |
| `events(moduleName: String)` | events of that module. `qualifiedEventName` or `moduleName` is required (`BAD_USER_INPUT` otherwise); with both, both filter. `moduleName` alone has no posting list: unless `blockHash` or `requestKey` picks the txs, heights are walked down from the tip (at most 20,000 heights, within the deadline) |
| `NetworkInfo.countersOfEachChain: [CountersOfEachChain!]!` | per chain, ascending: `chainId`, `blocksCount` (genesis to stored tip). `transactionCount: Int` is the chain's user txs (coinbases excluded, as stock's `canonicalTransactions`). `totalGasUsedInKda: String` is Σ `gas × meta.gasPrice` over those txs, printed as stock prints its `DECIMAL(20,10)` column: rounded half-up to exactly 10 decimals, trailing zeros kept (`"205.8782168640"`). Both come from the hot `ChainCounters` table, read in the same snapshot as the tips; a price that is missing or malformed adds 0. Ours is an exact decimal that a reorg reduces; stock's is a float sum that keeps an orphaned block's gas (see [comparison.md](comparison.md#intended-differences)). While the table is not ready (an older store before `kda-indexer reconcile-counts`, or after a command that clears its marker) both fields answer `null` with `NOT_AVAILABLE` |
| `balance(accountName, module, chainIds, first/last/after/before): QueryBalanceConnection!` | an account's present fungible balances, one `BalanceNode {accountName module chainId balance}` per module and chain (`balance` is a string in stock's layout, JavaScript `Number.prototype.toString` over the exact digits the node gave: `0.002101`, not `2.101e-3`; `6184.514948` and `110970`, without trailing zeros; exponent notation below 1e-6 and from 1e21, as `1e-12` and `1.5e+21`), listed by module name then chain, filtered by `module` and `chainIds` (at most 100, each once; `[]` means every chain); pages as every other connection (`first: 0` with `after` serves no row); at most 4,096 rows per account (`LIMIT` above). Without `accountName` (a module's holders): `NOT_SUPPORTED`, as the store keeps no holders index |
| `transactionsByPactCode(pactCode, first/after): QueryTransactionsByPactCodeConnection!` | txs whose own exec code contains `pactCode` (`%`/`_` wildcards), newest first, through the `/txs/search` engine: its pattern checks, per-client search gate, code index or bounded newest-first scan, caps and time budget. Rows are `TransactionSummary {requestKey height chainId canonical creationTime badResult sender gas gasLimit gasPrice code}`. Forward paging only (`last`/`before`: `BAD_USER_INPUT`). The search gate counts per client
address (HTTP, or the websocket's accepted connection); an operation without a known address
is refused (`LIMIT`). A page the search cut short carries `"extensions": {"searchTruncated": true}` and `pageInfo.endCursor` resumes it. `NOT_SUPPORTED` when the server has no search context |
| `lastTokenPriceInKda(moduleName): Decimal` | the price in KDA of the newest DEX swap that names `moduleName`, as stock computes it (see [Token price](#token-price)); `null` when no stored swap matches |
| `Subscription.newBlocks(quantity: Int)` | at most `quantity` (1 to 1,000; above it the `page` refusal) blocks per message, the newest of the batch; absent keeps Kadena's behaviour |
| `Subscription.transactions(quantity: Int = 20): [Transaction!]` | the user txs of newly committed blocks on every chain, newest first, at most `quantity` (1 to 1,000; above it the `page` refusal) per message; a message per feed batch that has any |

### Pact passthrough

`pactQuery` and `gasLimitEstimate` send client Pact to the node's
`POST /chainweb/0.0/<network>/chain/<c>/pact/api/v1/local`, on `node.url` and `network` from the
config. The client never chooses the URL: the chain must be a canonical chain number of the
network (`"0"` to `"19"`), and a client `networkId` is dropped, as stock's parser drops it.
Nothing is ever sent to `/send`; `/local` executes against a snapshot and commits nothing.

Answers match stock's, field for field (fixtures captured from stock in
`crates/kdx-graphql/tests/data/pact/`):

- `pactQuery`: each item is an unsigned `/local` call (preflight and signature verification
  off, gas limit 2,500, sender `""`, ttl 28,800, as `@kadena/client` sends it). `status`
  `success` carries `result`, the `JSON.stringify` text of the Pact value as JavaScript decodes
  it (`1.0` reads `1`, `1.0e-8` reads `1e-8`, integers `{"int":3}`, long decimals
  `{"decimal":"..."}`). `error` carries the Pact error message, or `{}` when the node gave no
  Pact error (an unknown chain, an HTTP error). After 10 s an item answers `status` `timeout`
  with stock's text. Items run in parallel; an item never fails the field.
- `gasLimitEstimate`: stock's six input shapes and flags. `code` builds an unsigned command
  (gas limit 10,000, no `networkId`, preflight off); `stringified-command` (preflight on) and
  `full-transaction` (preflight and signature verification on) send the client's command text
  and signatures as they are. `full-command`, `partial-command` and `payload` need `payload` and
  `meta` as strings in stock's schema, so stock can never build a valid command from them and
  answers its node error; we answer the same text without calling the node. Errors fail the
  whole field (`data: null`, `code` `INTERNAL_SERVER_ERROR`) with stock's texts: the first
  input that is not JSON or fails the schema, an unknown shape, then
  `[ERROR][GAS_API_GATEWAY][ESTIMATE_GAS] Chainweb Node was unable to estimate the gas limit`
  for anything the node refused.

Limits (`[graphql.pact]`; stock has none). Each refusal is a stock-style message with `code`
`INTERNAL_SERVER_ERROR`, and counts in `kdx_graphql_pact{field,outcome}` and
`kdx_graphql_rejected{reason}`:

| Setting | Default | When hit |
|---|---|---|
| `max_items` | 20 | items in one request, all fields, aliases and batch operations together: the field fails with `[ERROR][RAW_QUERY][LIMIT] At most 20 Pact queries and gas estimates per request` (`[GAS]` for `gasLimitEstimate`) |
| `enabled` | false | off: both fields answer `NOT_SUPPORTED`. Operators turn it on |
| `per_ip_rps`, `per_ip_burst` | 5, 20 | per client, keyed as the API's rate limit (an IPv4 address, a v4-mapped address unmapped, or an IPv6 /64; under key pressure a new key draws on its /24 or /48 overflow bucket, and eviction never refills a drained key), one token per item: `[ERROR][RAW_QUERY][RATE_LIMIT] Too many Pact queries from this client, retry later` |
| `max_in_flight` | 8 | node calls at once; items wait for a slot inside their timeout. At most twice as many requests may be inside the passthrough; past that a request is refused at once with `[ERROR][RAW_QUERY][BUSY] The Pact query service is busy, retry later` |
| `timeout_ms` | 10,000 | per item, slot wait included: stock's `timeout` item; a gas estimate fails with the node error. Only a request in which one of the fields actually runs may run this long (plus 1 s) past `api.deadline_ms`; the body read and every other request keep `api.deadline_ms` |
| `max_input_bytes` | 65,536 | `code` plus `data` of one item: `[ERROR][RAW_QUERY][LIMIT] Pact code and data exceed N bytes` (the item, not the field); one gas input: `[ERROR][GAS][LIMIT] Input exceeds N bytes`. The request's `max_variables_bytes` and query caps apply first |
| `max_gas_limit` | 150,000 | `meta.gasLimit` of a client command (`stringified-command`, `full-transaction`): `[ERROR][GAS][LIMIT] gasLimit above the server cap of N`, no node call. Our own commands ask for 2,500 or 10,000 |
| (duplicate keys) | refused | a client command (`stringified-command`, `full-transaction`) that repeats a JSON key anywhere: `[ERROR][GAS][INPUT] The command holds a duplicate JSON key`, no node call (the node's decoder might read the other copy) |
| `max_response_bytes` | 1 MiB | the node's answer: `[ERROR][RAW_QUERY][LIMIT] The node's answer exceeds N bytes`; a gas estimate fails with the node error |
| `node_rps` | 16 | node calls per second across the process (0 = none); calls past it wait inside their timeout |
| (chains) | the network's | a `chainId` must be a canonical number below the network's chain count, taken from the store's chain graph, else the network (20); otherwise no node call |

The passthrough has its own node client, so it neither shares nor drains ingest's node budget.

### Token price

`lastTokenPriceInKda(moduleName)` follows stock (indexer-kadena `90a4be1`,
`last-token-price-in-kda-resolver.ts`). Stock takes the newest transaction
(`ORDER BY creationtime DESC, id DESC`) whose result text contains `moduleName`
(`LIKE '%' || moduleName || '%'`). When `result.data[0].token` and `result.data[1].token` are
set, it answers `kda / token`: `kda` is the amount of element 0 if element 0's token is `coin`,
otherwise the amount of element 1, and `token` is the other one. Otherwise it answers `null`.
The division is decimal.js `div` (20 significant digits, half up) and then `toFixed(12)` (half
up), sent as a string with its trailing zeros (`"0.042190643290"`). A JSON-number amount goes
through a JavaScript double first; a `{"decimal": "..."}` amount is exact. A zero token amount
gives `"Infinity"` or `"NaN"`, as stock does.

We keep the newest swap per chain and token in the hot table `TokenSwaps`
([format.md](format.md)) instead of scanning every result. A swap is a successful tx that
emits a trusted DEX swap event (Kaddex/eckoDEX `kaddex.exchange.SWAP`, KDSwap
`kdlaunch.kdswap-exchange.SWAP` or Sushi
`n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange.SWAP`, params
`[sender, receiver, in, token-in, out, token-out]`)
and whose `result.data` elements 0 and 1 equal one of those events' `(token-in, in)` and
`(token-out, out)`: tokens compared by the module reference's qualified name, amounts as
numbers. The result is the value of the tx's last expression, which the sender writes; the
event can only be emitted by the DEX module, so it is what authenticates the two legs. Token
names must be valid Pact qualified names (at most 256 bytes, ASCII letters, digits and
`%#+-_&$@<>=^?*!|/~.`). Exactly one of the two legs must be `coin`, and the row is keyed under
the other token: a token-to-token swap has no price in KDA and stores nothing, and `coin` is
never a key. Each further token of the list (at most 8 tokens in all) is stored only when a trusted
SWAP event of the same tx trades it against `coin`, and then with the legs of that event
(its token-in, amount-in, token-out, amount-out), never the result's legs: a routed swap names
three or more tokens, one per hop, and each routed token is priced by its own hop. Stock keys
the first hop's row under every token by text, so a routed token (such as `kb-USDC`) can differ
from stock's answer; no compared module is served from a routed row.
Sushi is trusted because it is a live DEX with the same event layout, and Pact lets only that
module emit events under its name, so trust stays scoped to that one namespaced module. It is
needed for parity: a tx such as the newest `free.wiza` arbitrage (a Kaddex hop, then a Sushi
hop) has the Sushi hop as its result, which only the Sushi event backs. The tail keeps the
table current, and a one-off fill reads the history ([operations.md](operations.md#token-swaps)).
A lookup applies stock's `LIKE` pattern (`%` and `_` wildcards, `\` escape, case-sensitive) to
the stored token names and takes the newest match by (`meta.creationTime`, height, chain,
position). The pattern is user input, so the lookup is bounded: a pattern longer than 256
bytes, or with more than 8 unescaped wildcards, answers `null`; it reads at most 20,000 rows;
the matching work of the whole lookup (not per row) is capped, and past the row or work cap
the lookup matches the exact name only, with one point read per chain (20); the request
deadline is checked inside the scan. The resolver runs as a heavy read, so it shares the
heavy-read slots and cannot exhaust the general read pool. Amounts with more than 80 significant digits or an
exponent beyond ±1000, and prices with more than 400 integer digits, answer `null`.

Rulings, where stock's text scan cannot be kept exactly:

- **Trusted DEXes only, legs checked against their events.** Stock reads the price from any
  result text, which the tx sender writes, so anyone could set a price. We count a swap only
  when the result's first two legs repeat a `SWAP` event of the same tx from one of three
  trusted modules: Kaddex/eckoDEX (`kaddex.exchange`), KDSwap (`kdlaunch.kdswap-exchange`) and
  Sushi (`n_bd19ba92f0449d4422e620740759c9e94cacdb37.sushi-exchange`). Event names are
  namespaced by the emitting module, and only that module can emit them. Sushi is included
  because it is a live DEX with the same event layout and the newest `free.wiza` swap tx is an
  arbitrage (a Kaddex hop, then a Sushi hop) whose result is the Sushi hop; without it we could
  not match stock there. Trust is scoped to that one namespaced module. Adding a DEX is a code
  change (`DEX_SWAP_EVENTS`).
- **Only KDA pairs.** Stock treats element 1 as KDA when element 0 is not `coin`, so a
  token-to-token swap gets a nonsense price. We store a swap only when exactly one of its two
  legs is `coin`, keyed under the other token (a routed token is stored only with the legs of its own
  trusted `coin` hop); any other swap stores nothing, and a stored row with no `coin` leg
  answers `null`.
- **A genuine swap can still move the price.** The answer is the last trade, as in stock; a
  trade in a thin pool on a trusted DEX sets it like any other.
- **Swaps only.** Stock matches any result text. If a newer non-swap tx mentions the module
  (a failed swap's error, a `create-pair` result), stock answers `null` and we answer the
  last swap's price. Our answer is the more useful one, and stock's full-table scan timed out on
  35 of the 49 corpus requests.
- **Tie order.** Stock breaks equal `creationtime` values by its row id (insertion order). We
  use height, then chain, then position.
- **Canonical blocks only.** Stock keeps orphaned blocks' txs in its scan; we do not.
- **Unreadable amounts.** An amount decimal.js would refuse (an object such as `{"int": 5}`)
  makes stock fail the request; we store no swap for it (its legs cannot be checked), so the
  answer comes from an older swap or is `null`.
- **`coin` gets no special case.** It matches every swap with KDA, so it answers the newest
  swap's price, as stock's text match would when that swap is the newest matching tx.

Checked against the 2026-11-24 G4 replay: stock answered 14 of the 49 distinct corpus requests
(the other 35 timed out at its load balancer). We match all 14 to the last digit, `free.wiza` included through the Sushi event
(`crates/kdx-graphql/tests/prices.rs`, with the newest swaps and their SWAP events read from
mainnet in `tests/data/token-swaps-2026-11-24.jsonl`); all 14 are `coin` pairs.

## Fields compared with the node, not an oracle

Tip-relative answers change from second to second, so the comparison harness checks them
against the node or the native API: balances and guards, `networkInfo`, `lastBlockHeight`,
`completedBlockHeights`, `blocksFromDepth` and subscriptions.
