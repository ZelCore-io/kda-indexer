# Overview

kda-indexer is a single Rust binary that follows a chainweb-node over HTTP and indexes the full
history of all 20 Kadena chains. It serves that history through a native API, a REST API
compatible with chainweb-data and the kda-txs `/v1/txs` route, and Kadena's GraphQL schema.

Status: validated against chainweb-data and the GraphQL indexer on mainnet; a 72-hour final
validation is in progress (it ends 2026-11-19 17:05Z); not yet serving production traffic.

## What it indexes

Everything a Kadena transaction carries is stored on the first pass:

- **Blocks**: headers, miner, coinbase, and derived header fields.
- **Transactions**: the command (code or continuation, data, signers with capabilities,
  metadata, nonce, network id), signatures, the result (status, data or error), gas, logs hash,
  Pact tx id and continuation state. Coinbase transactions are stored and indexed like the
  others, so miner rewards appear in account history.
- **Events** with their parameters, and **transfers** (chainweb-data's rule: an event whose
  name ends in `TRANSFER` with `from`, `to` and an amount), including both halves of a
  cross-chain transfer.
- **Continuations (defpacts)**: every step of a pact, and the SPV proof a continuation carries.
- **Balances, guards, tokens and NFTs**: read from the node's `/local` (the node executes
  Pact; kda-indexer does not) and kept current as new blocks touch them.

History is indexed by account, public key, event name, module, request key, block hash and
pactId, plus a code search index over distinct `exec` code bodies.

## What it serves

One process, separate listeners (mainnet defaults, all on loopback unless configured otherwise):

| Listener | Default | Contents |
|---|---|---|
| Native API | `127.0.0.1:31361` | `/api/v1/...`: blocks, transactions, account and key history, events, balances, tokens, NFTs, code search, defpacts. Binds loopback or private addresses only. See [api.md](api.md). |
| Compat API | `127.0.0.1:31362` | chainweb-data routes `/txs/account`, `/txs/events`, `/txs/recent`, `/txs/search`, `/txs/tx`, `/txs/txs`, `/stats`, `/coins`, and the kda-txs route `/v1/txs/...`. Compatible with chainweb-data's `/txs` routes, with documented differences (see [api.md](api.md)). |
| GraphQL | `127.0.0.1:31363` | `POST`/`GET /graphql` with Kadena's schema, vendored, plus a few pinned extensions; subscriptions over websocket. See [graphql.md](graphql.md). |
| Admin | `127.0.0.1:31369` | `/healthz`, `/readyz`, `/status`, `/metrics` (Prometheus). |

`kda-indexer run` ingests and serves. `kda-indexer serve` serves a data directory read-only (a
replica, or a copy made with `kda-indexer snapshot`) without ingesting. See
[operations.md](operations.md).

## Who it is for

Anyone who runs a chainweb-node and needs transaction history behind an API: wallets,
explorers, exchanges and node operators. It is built to replace a chainweb-data instance (Haskell and
Postgres) and a GraphQL indexer with one service that installs as a Debian package with a
systemd unit, next to the node it reads. It does not replace the node: `/send`, `/local`,
`/poll`, `/listen`, `/cut` and P2P traffic stay on chainweb-node.

On mainnet the whole data directory is <!--m:disk-total-vs-cwd-->140.2 GB<!--/m--> (interim),
against <!--m:disk-total-vs-cwd-->1,568.6 GB<!--/m--> for chainweb-data on the same host:
<!--m:disk-total-vs-cwd-->11.2× smaller (−91%)<!--/m-->. Response times, correctness and the
places where it is slower are in [comparison.md](comparison.md).

## What it does not do

- **No mempool.** Only transactions in blocks are indexed. A GraphQL `Transaction.result` is
  never a `TransactionMempoolInfo`; a transaction that is not indexed is `null`.
- **No Pact execution.** It decodes what the node already executed. GraphQL `pactQuery` and
  `gasLimitEstimate` answer `NOT_SUPPORTED`; balances come from the node's `/local`.
- **No orphaned transactions.** Only the canonical chain is kept. A reorg is unwound and the
  new branch applied; cursors into a reorged block answer `400 cursor invalidated by reorg`.
- **No unbounded queries.** Every request has a page cap, a 5 s deadline and an index behind
  it:
  - compat API: `limit` default 10, at most 250; `offset` at most 10,000 (use `next` beyond);
  - native API: `limit` 1 to 100, default 20; filtered lists examine at most 5,000 txs per
    request and may return short pages with a cursor;
  - GraphQL: 20 items by default and a 1,000-row safety cap per page, with the stock
    indexer's depth and complexity limits, plus batch and size limits;
  - code search: SQL `LIKE` patterns of at most 256 characters; patterns that cannot use the
    code index run a bounded newest-first scan with a 2 s timeout and may return a cut answer
    (`X-Search-Truncated: true`).
- **No price data or analytics.** GraphQL `lastTokenPriceInKda` answers `NOT_SUPPORTED`.
- **Reorgs deeper than `ingest.finality_depth`** (1,000 blocks by default) halt that chain
  for an operator to resolve; the other chains keep running
  ([operations.md](operations.md#halted-chains)).
- **GraphQL account tabs are slow today.** The account Transactions and Transfers tabs of an
  explorer take about 2.5 s because of the `totalCount` walk; a bounded count is fixed, not
  released yet ([explorer.md](explorer.md#speed)).
- **One known NFT issue.** Some early marmalade v1 tokens (ids without the `t:` prefix) are
  missing from `nonFungibleAccount`; fixed, not released yet
  ([explorer.md](explorer.md#what-works-what-differs-what-is-not-supported)).
- **The native API is internal.** It refuses public bind addresses and may change until a
  public hostname is decided.

For the full list of GraphQL differences, see [graphql.md](graphql.md); for using it behind
an explorer, see [explorer.md](explorer.md).
