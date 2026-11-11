//! OpenAPI 3.1 document of the native API, generated from the handler annotations
//! (`kda-indexer openapi > docs/openapi.json`).

use utoipa::OpenApi;

use crate::native::{accounts, balances, blocks, events, search, stats, tokens, txs};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "kda-indexer native API",
        version = "1",
        description = "Internal and unstable until a public hostname is decided. JSON in snake_case. \
List routes take limit (1..=100, default 20), an opaque cursor, order (desc default, asc), minheight, \
maxheight and chain, and answer {items, next_cursor, as_of}. A filtered page can be shorter than \
limit while next_cursor is set: follow it until it is null. A cursor whose block was reorged away \
answers 400 'cursor invalidated by reorg'. Balances and tokens come from /local reconciles: every \
balance row carries as_of_height and stale. Search routes run under a shared gate (503 when \
search.concurrency searches run, 429 when the client already runs search.per_client) and answer \
truncated: true when their deadline or a cap cut them. A route reserved for later answers 501 \
{\"error\": \"not implemented until P6\"}: /nft/accounts/{account}."
    ),
    paths(
        crate::status::handler,
        stats::stats,
        blocks::list,
        blocks::by_hash,
        blocks::by_height,
        blocks::txs,
        txs::get_tx,
        txs::defpact,
        accounts::transfers,
        accounts::txs,
        accounts::key_txs,
        balances::account_balances,
        balances::key_accounts,
        tokens::list,
        tokens::get,
        events::list,
        search::code,
        search::events,
    )
)]
pub struct ApiDoc;

/// The document as pretty JSON.
pub fn json() -> String {
    ApiDoc::openapi().to_pretty_json().unwrap_or_default()
}
