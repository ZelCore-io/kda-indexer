# Explorer operations checked against kda-indexer

Generated for [explorer.md](../explorer.md). `kda-indexer graphql check-ops DIR` validates each
operation document against the served schema and the default `[graphql]` limits without running
it: `in-scope` (valid, within the limits), `invalid: …` or `limit: …`. No variables are bound, so
a page size given only by a variable is costed at the largest page (100).

Speed: loopback on the validation host, at least 100 samples per operation with 100 distinct
variable sets, 2026-11-17, build 0b5cb03 (interim). Blank: not measured.

## kadscan

kadscan's source is not public; these are the documents its web app sends.

| Operation | What it reads | check-ops | p50 (interim) | p99 (interim) | Note |
|---|---|---|---:|---:|---|
| `AverageGasPrice-4237b410` | recent transactions, gas prices | in-scope |  |  |  |
| `Balance-e6a60502` | balances of a module, all holders | in-scope |  |  | without `accountName` (all holders of a module) `balance` answers `NOT_SUPPORTED`: no holders index |
| `Block-f6483b86` | block by hash | in-scope | <!--m:exp-block-f6483b86-p50-->4.6 ms<!--/m--> | <!--m:exp-block-f6483b86-p99-->32.7 ms<!--/m--> |  |
| `Blocks-34be02c6` | blocks list (`blocksFromDepth`) | limit: complexity |  |  | refused only when the page variables are absent (costed at the maximum page); passes with `first` 25, refused at 50 |
| `BlocksByHeight-7d3a4973` | blocks in a height range, with gas prices | in-scope | <!--m:exp-blocksbyheight-7d3a4973-p50-->11.8 ms<!--/m--> | <!--m:exp-blocksbyheight-7d3a4973-p99-->110.0 ms<!--/m--> |  |
| `BlocksFromHeight-2b764a6f` | blocks in a height range | in-scope | <!--m:exp-blocksfromheight-2b764a6f-p50-->2.8 ms<!--/m--> | <!--m:exp-blocksfromheight-2b764a6f-p99-->5.1 ms<!--/m--> |  |
| `BlocksFromHeight-c137684a` | block heights per chain | in-scope | <!--m:exp-blocksfromheight-c137684a-p50-->1.4 ms<!--/m--> | <!--m:exp-blocksfromheight-c137684a-p99-->4.2 ms<!--/m--> |  |
| `ChainsGas-4ccfcce3` | per-chain gas counters | in-scope |  |  | `totalGasUsedInKda` answers `null` with `NOT_AVAILABLE`: no all-time gas counter |
| `CoinsInCirculation-74ce39b0` | coins in circulation | in-scope |  |  |  |
| `Events-618c9a0e` | events of a module | in-scope |  |  |  |
| `FungibleAccount-57a45a6a` | account total balance | in-scope | <!--m:exp-fungibleaccount-57a45a6a-p50-->2.8 ms<!--/m--> | <!--m:exp-fungibleaccount-57a45a6a-p99-->11.5 ms<!--/m--> |  |
| `FungibleAccount-78665e6d` | account balances and guards per chain | in-scope | <!--m:exp-fungibleaccount-78665e6d-p50-->2.4 ms<!--/m--> | <!--m:exp-fungibleaccount-78665e6d-p99-->8.4 ms<!--/m--> |  |
| `GetRelatedTransaction-4b2a4e83` | transaction detail (related tx) | in-scope | <!--m:exp-getrelatedtransaction-4b2a4e83-p50-->13.9 ms<!--/m--> | <!--m:exp-getrelatedtransaction-4b2a4e83-p99-->38.2 ms<!--/m--> |  |
| `HomeBlockList-4f4aae87` | home: new blocks (subscription) | in-scope |  |  |  |
| `HomeBlockListInit-33e14dda` | home: first blocks | in-scope |  |  |  |
| `HomeTxList-0ae076ad` | home: new transactions (subscription) | in-scope |  |  |  |
| `HomeTxListInit-ec030d30` | home: first transactions | in-scope |  |  |  |
| `NetworkHashRate-a7d45475` | network hash rate | in-scope |  |  |  |
| `NetworkInfo-e5e8483b` | network info | in-scope |  |  |  |
| `NewBlocks-a61c93fc` | new blocks (subscription) | in-scope |  |  |  |
| `NonFungibleAccount-f77b27d1` | account NFTs | in-scope | <!--m:exp-nonfungibleaccount-f77b27d1-p50-->1.1 ms<!--/m--> | <!--m:exp-nonfungibleaccount-f77b27d1-p99-->2.9 ms<!--/m--> | works, with one known issue: some early marmalade v1 tokens (ids without the `t:` prefix) are missing from `nonFungibleAccount`; fixed, waiting for release |
| `PactContract-504515c9` | read a contract value (`pactQuery`) | in-scope |  |  | runs, but `pactQuery` answers `NOT_SUPPORTED`: the indexer never runs Pact code |
| `Query-3ef4ad06` | token price (`lastTokenPriceInKda`) | in-scope |  |  | works: stock's last-swap price (14 of 14 recorded stock answers matched) |
| `Query-63662f60` | transaction status | in-scope | <!--m:exp-query-63662f60-p50-->15.8 ms<!--/m--> | <!--m:exp-query-63662f60-p99-->41.1 ms<!--/m--> |  |
| `Query-75ee2f0a` | last block height | in-scope |  |  |  |
| `Query-d3953b76` | account assets (`balance`) | in-scope |  |  |  |
| `Subscription-59b669f2` | new transactions (subscription) | in-scope |  |  |  |
| `TotalDifficulty-2a35fbde` | total difficulty | in-scope |  |  |  |
| `Transaction-d62bf364` | transaction page | limit: complexity | <!--m:exp-transaction-d62bf364-p50-->11.7 ms<!--/m--> | <!--m:exp-transaction-d62bf364-p99-->38.1 ms<!--/m--> | refused only when the page variables are absent; passes with `first` 50; 0 refusals over 100 real variable sets |
| `Transactions-2a9fca9d` | transactions of a token | in-scope |  |  |  |
| `Transactions-9e58b95d` | transactions list | in-scope | <!--m:exp-transactions-9e58b95d-p50-->3.7 ms<!--/m--> | <!--m:exp-transactions-9e58b95d-p99-->41.5 ms<!--/m--> |  |
| `Transactions-bca501b0` | account page: Transactions tab | limit: complexity | <!--m:exp-transactions-bca501b0-p50-->2,476.9 ms<!--/m--> | <!--m:exp-transactions-bca501b0-p99-->2,559.7 ms<!--/m--> | refused only when the page variables are absent; no refusal over 100 real variable sets. Slow: the `totalCount` walk, see the speed table |
| `TransactionsByPactCode-2205c440` | search by code | in-scope |  |  |  |
| `TransactionsByPactCode-32b094e9` | search by code | in-scope |  |  |  |
| `Transfers-5cf774de` | transfers of a token (amounts) | in-scope |  |  |  |
| `Transfers-825fa035` | account page: Transfers tab | in-scope | <!--m:exp-transfers-825fa035-p50-->2,474.0 ms<!--/m--> | <!--m:exp-transfers-825fa035-p99-->2,578.6 ms<!--/m--> | slow: `totalCount` walk, see the speed table |
| `Transfers-8332ebc2` | transfers of a token | in-scope |  |  |  |
| `Transfers-8c8a0f56` | account transfers (short list) | in-scope | <!--m:exp-transfers-8c8a0f56-p50-->6.1 ms<!--/m--> | <!--m:exp-transfers-8c8a0f56-p99-->188.6 ms<!--/m--> |  |
| `Transfers-f3be2db0` | account page: Transfers tab (fungible or NFT) | in-scope | <!--m:exp-transfers-f3be2db0-p50-->2,473.1 ms<!--/m--> | <!--m:exp-transfers-f3be2db0-p99-->2,560.7 ms<!--/m--> | slow: `totalCount` walk, see the speed table |
| `blockTransactions-d6646ba8` | transactions of blocks in a height range | limit: complexity | <!--m:exp-blocktransactions-d6646ba8-p50-->23.0 ms<!--/m--> | <!--m:exp-blocktransactions-d6646ba8-p99-->195.6 ms<!--/m--> | refused only when the page variables are absent; 0 refusals over 100 real variable sets |
| `blocksFromHeight-18247276` | block page (full header) | in-scope | <!--m:exp-blocksfromheight-18247276-p50-->12.4 ms<!--/m--> | <!--m:exp-blocksfromheight-18247276-p99-->171.4 ms<!--/m--> |  |

## Kadena explorer

From the public source (`packages/apps/explorer` in kadena.js, commit `35b4c805`): the documents
its code generator emits, plus one instance of the events query it builds at run time.

| Operation | check-ops | Note |
|---|---|---|
| `account` | in-scope |  |
| `accountTransactions` | in-scope |  |
| `accountTransfers` | in-scope |  |
| `block` | in-scope |  |
| `blockTransactions` | in-scope |  |
| `blocksFromHeight` | in-scope |  |
| `completedBlockHeights` | in-scope |  |
| `coreAccount` | in-scope |  |
| `coreBlocksFromHeight` | in-scope |  |
| `events` | in-scope |  |
| `eventsPerChain-20chains` | limit: complexity | the per-chain events query built at run time, with all 20 chains ticked: refused (about 100 per chain); passes with up to 19 chains. With no chain ticked the explorer sends `events` instead |
| `lastBlockHeight` | in-scope |  |
| `networkInfo` | in-scope |  |
| `newBlocks` | in-scope |  |
| `transaction` | in-scope |  |
| `transactionRequestKey` | in-scope |  |
| `transactions` | limit: complexity | generated by the explorer's code generator but used by no page; passes with `first` 50 |
