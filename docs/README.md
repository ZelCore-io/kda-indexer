# kda-indexer documentation

kda-indexer indexes the full history of all 20 Kadena chains from a chainweb-node and serves
it through a native API, a chainweb-data compatible REST API and a GraphQL API. Start with the
overview; the rest of this page lists every document with one line on what it covers.

Status: validated against chainweb-data and the GraphQL indexer on mainnet; a 72-hour final
validation is in progress (it ends 2026-11-19 17:05Z); not yet serving production traffic.

## Understand it

| Page | What it covers |
|---|---|
| [overview.md](overview.md) | What it indexes and serves, who it is for, current status, and what it does not do. |
| [architecture.md](architecture.md) | Components and data flow from the node to the APIs, reorg handling, the data directory. |
| [comparison.md](comparison.md) | Disk, response time, correctness, sync and memory compared with chainweb-data and the GraphQL indexer, as change rates with charts. |
| [methodology.md](methodology.md) | How it was built and how its answers were checked against chainweb-data, the GraphQL indexer and the node. |
| [explorer.md](explorer.md) | Pointing a Kadena explorer front end at the GraphQL API: what works, what differs, what is not supported. |

## Use it

| Page | What it covers |
|---|---|
| [install.md](install.md) | Building the Debian package, installing, configuring and starting the service. |
| [operations.md](operations.md) | Running it: systemd, halted chains, read-only `serve`, metrics and alerts, snapshots and replicas, rebuilds, the measured mainnet backfill. |
| [load-balancer.md](load-balancer.md) | Serving the compat routes through HAProxy next to chainweb-data, one route at a time, with a rollback for each step. |

## Reference

| Page | What it covers |
|---|---|
| [api.md](api.md) | The native API (`/api/v1`): conventions, routes, objects, search and defpact routes. Internal and unstable. |
| [openapi.json](openapi.json) | Machine-readable OpenAPI description of the native API. |
| [graphql.md](graphql.md) | The GraphQL API: schema, errors, limits, websockets and subscriptions, differences from Kadena's graph. |
| [format.md](format.md) | The on-disk format (version 2): segments, indexes, posting lists, record encoding, write ordering, checks. |

## Data behind the numbers

Every number quoted on these pages comes from [data/benchmarks.json](data/benchmarks.json),
with a source line saying what was measured, how, when and on what hardware class. The charts
in [img/](img/) are generated from that file with `make docs-charts`; `make docs-check` checks
that the pages quote the same values.
