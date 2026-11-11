//! OpenAPI 3.1 document of the native API, generated from the handler annotations
//! (`kda-indexer openapi > docs/openapi.json`).

use utoipa::OpenApi;

use crate::native::{accounts, blocks, events, stats, txs};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "kda-indexer native API",
        version = "1",
        description = "Internal and unstable until a public hostname is decided. JSON in snake_case. \
List routes take limit (1..=100, default 20), an opaque cursor, order (desc default, asc), minheight, \
maxheight and chain, and answer {items, next_cursor, as_of}. A filtered page can be shorter than \
limit while next_cursor is set: follow it until it is null. A cursor whose block was reorged away \
answers 400 'cursor invalidated by reorg'. Routes reserved for P6 answer 501 {\"error\": \"not implemented until P6\"}: \
/txs/{requestKey}/defpact, /accounts/{account}/balances, /keys/{pubkey}/accounts, /tokens, /tokens/{module}, \
/search/code, /search/events, /nft/accounts/{account}."
    ),
    paths(
        crate::status::handler,
        stats::stats,
        blocks::list,
        blocks::by_hash,
        blocks::by_height,
        blocks::txs,
        txs::get_tx,
        accounts::transfers,
        accounts::txs,
        accounts::key_txs,
        events::list,
    )
)]
pub struct ApiDoc;

/// The document as pretty JSON.
pub fn json() -> String {
    ApiDoc::openapi().to_pretty_json().unwrap_or_default()
}
