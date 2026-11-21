# Real-traffic corpus, 2026-11-14 .. 2026-11-21 (anonymized)

Read requests that real clients sent to the production Kadena indexer services, deduplicated
with a count per distinct request, and anonymized (see below). Use it to replay a realistic
request mix against kda-indexer, either through the endpoint suite or through kda-compare.

## Window and sources

| Class | Service | Window (UTC) | Source |
|---|---|---|---|
| `graphql` | GraphQL indexer (`POST /graphql`) | 2026-11-16 13:11 .. 2026-11-18 13:19 | the temporary 48 h POST-body capture. Bodies exist only for that window. |
| `cwdata-rest` | chainweb-data REST (`/txs/*`, `/coins`, `/stats`) | 2026-11-14 00:00 .. 2026-11-21 ~19:45 | load-balancer access logs (request line only) |
| `kda-txs` | `/v1/txs/{account}/{token}/{limit}` | 2026-11-14 00:00 .. 2026-11-21 ~19:45 | service access log (request line only) |

Not included: GraphQL subscriptions (`/wss/graphql`, no frames logged), GraphQL outside the
capture window (no body logged), chainweb node API traffic (not an indexer query), and any
POST other than GraphQL (no body logged).

## Files

- `corpus.jsonl.gz`: every distinct request, one JSON object per line:
  `{"id", "ep", "class", "count", "days", "req"}`.
  - `ep`: endpoint name in the suite's naming (`gql.<root field>`, `compat.<route>`).
  - `count`: how many times the request was seen in the window. `days`: on how many distinct
    days it was seen. There are no timestamps.
  - `req`: `{"query", "variables", "operationName"}` for GraphQL; `{"path"}` (path and query
    string) for REST.
- `cases/<ep>.jsonl.gz`: the same requests split per endpoint, in the endpoint suite's case
  format: `{"id", "ep", "cls": "real", "count", "req"}`, with `req` = `{"query", "variables", "op"}`
  for GraphQL and `{"path"}` for REST. The suite ignores `count`.
- `cwdata-replay.tsv`: the chainweb-data REST requests in kda-compare's replay format
  (`count<TAB>GET<TAB>path?query`, `#` lines are comments).

## Counts

| Class | Raw requests | Kept | Distinct (deduplicated) |
|---|---:|---:|---:|
| `graphql` | 302,212 | 236,683 | 91,721 |
| `cwdata-rest` | 106,527 | 106,515 | 88 |
| `kda-txs` | 74,379 | 28,322 | 1,598 |
| **Total** | **483,118** | **371,520** | **93,407** |

Dropped, by reason:

| Reason | Requests |
|---|---:|
| GraphQL from internal sources (our own comparison and suite runs), identified by joining each captured body to its load-balancer log line | 65,480 |
| kda-txs uptime monitor (health checks) | 38,784 |
| kda-txs scanner probes and non-`/v1/txs` paths | 7,261 |
| GraphQL documents with inline string literals (ad-hoc Pact code and account names written into the query text) | 20 |
| Malformed account names (GraphQL 20, REST 8) | 28 |
| Malformed request keys (GraphQL 4, REST 5) | 9 |
| REST free-text search terms that are not an identifier | 4 |
| GraphQL documents with comments | 3 |
| Other REST shapes (unknown route or parameter, malformed `/v1/txs`) | 7 |
| Unparsable capture lines | 2 |

Distinct requests per endpoint (requests in parentheses):

- GraphQL: `blocksFromHeight` 67,965 (77,853), `transaction` 16,845 (34,399), `transactions` 5,207
  (45,596), `transfers` 554 (619), `nonFungibleAccount` 354 (374), `fungibleAccount` 353 (373),
  `lastBlockHeight` 352 (77,064), `lastTokenPriceInKda` 49 (242), `pactQuery` 26 (97, all
  `describe-module`), `blocksFromDepth` 5, `__schema` 4 (15), `networkInfo` 4 (24),
  `completedBlockHeights` 1 (17), `events` 1 (4), `fungibleChainAccount` 1.
- chainweb-data: `account` 52 (69,725), `recent` 1 (36,720; a fixed poller), `search` 22 (40),
  `stats`/`stats.json` 3 (20), plus 10 single requests to other paths (`/txs/health`,
  `/txs/docs`, ...), kept as real client behaviour (they return 404).
- kda-txs: `v1txs` 1,598 (28,322).

Size: 4.2 MB compressed. Nothing was sampled.

## Anonymization

Done on the processing host before anything left it; the substitution table stays there and
is not in this repository.

- No client IPs, user agents, headers, cookies, auth, response data or timestamps (only a count
  of distinct days per request). No infrastructure host names or addresses.
- Every user identifier was replaced by a deterministic substitute of the same kind, drawn from
  public chain data (transactions sampled from our own indexer over random chains and heights
  1.5M..7.42M), never from the logs. The same raw value always gets the same substitute, and two
  different raw values never share one, so the request patterns (repeats, polling, paging)
  survive:
  - public keys, and `k:` accounts through their key (a `k:<key>` account and a `publicKey`
    variable for the same key stay consistent);
  - named, `c:`, `w:` and `r:` accounts;
  - request keys and transaction hashes (including the base64-wrapped form some clients send,
    which keeps its wrapping);
  - block hashes and `Block:` node ids.
- Kept as is: chain ids, heights, limits, offsets, numeric block-list cursors, module and token
  names (`coin`, `runonflux.flux`, ...), event names, GraphQL documents and operation names.
- Requests carrying anything that could not be classified (free text, inline literals in the
  GraphQL document, credential-like query parameters, opaque cursors) were dropped, not edited.
- Substitutes exclude every identifier-shaped token that appears anywhere in the raw logs, so no
  raw identifier can reappear by chance.

Privacy check run on the result: 0 IPv4/IPv6 addresses, 0 internal addresses or host names,
0 timestamps; 0 of the 17,949 identifier tokens in the corpus (64-hex keys, 43-char request keys
and block hashes, including base64-wrapped ones) occur in the raw logs; 0 `k:` accounts and 0
named accounts overlap the raw sets; every identifier token comes from the public substitute pool.

Substituted accounts and keys are real, active chain entities, but an account substitute may
hold a different token set from the original. Request-key substitutes are real transactions on
some chain. Expect some answers that differ in content from what the original client saw.

## Replaying it

Endpoint suite (`scripts/suite/`, branch `test/suite`):

```
python3 scripts/suite/run.py run --config suite.toml \
    --cases tests/corpus/traffic-2026-11/cases --out <out dir> \
    --endpoints gql.transaction gql.transactions gql.blocksFromHeight compat.account compat.v1txs
```

`load_cases` opens `<cases>/<ep>.jsonl.gz` for each endpoint it is given, so pass the endpoint
names from the list above (endpoints not in `endpoints.ENDPOINTS` are ignored). To weight by real
frequency, repeat or sample cases by `count` before the run; the runner itself sends each case once.

kda-compare (REST, weighted by `count`): set `replay = "tests/corpus/traffic-2026-11/cwdata-replay.tsv"`
in the kda-compare config and give `mix.replay` a weight. Only GET entries are read, as in an
imported HAProxy replay file.

For anything else, read `corpus.jsonl.gz` directly: GraphQL entries are ready-to-POST bodies
(`{"query", "variables", "operationName"}` from `req`), REST entries are paths relative to the
service root.
