//! `node(id)` and `nodes(ids)` (Task 3): decode a Relay id (`ids.rs`) and load the object
//! through the same paths as the root queries. An id that does not decode is
//! `BAD_USER_INPUT`; an object that is not stored is `null`.

use async_graphql::{Context, Result, ID};

use super::{
    Event, NonFungibleAccount, NonFungibleChainAccount, NonFungibleTokenBalance, Signer,
    Transaction, Transfer,
};
use std::sync::Arc;

use kdx_types::Hash32;

use crate::error::{bad_input, from_api};
use crate::gql;
use crate::ids::{decode, NodeKey};
use crate::txlist::chain_arg;
use crate::txlist::{hash_arg, load, Blocks, TxData};
use crate::types::Node;

/// Most ids one `nodes` call takes.
pub const MAX_NODES: usize = 100;

async fn tx(
    ctx: &Context<'_>,
    block_hash: Option<String>,
    rk: String,
) -> Result<Option<Transaction>> {
    super::tx::query_transaction(ctx, block_hash, None, rk).await
}

/// The tx an event or transfer id names: a user tx by request key, or the block's coinbase
/// (whose rows carry the coinbase request-key text, R7D-3).
async fn row_tx(ctx: &Context<'_>, block_hash: String, rk: String) -> Result<Option<Arc<TxData>>> {
    if Hash32::from_b64url(&rk).is_ok() {
        return Ok(tx(ctx, Some(block_hash), rk).await?.map(|t| t.d));
    }
    let bh = hash_arg("blockHash", &block_hash)?;
    gql(ctx)
        .state()?
        .read(move |view| {
            let Some((c, h, _)) = view.block_by_hash(&bh)? else {
                return Ok(None);
            };
            let mut blocks = Blocks::default();
            for rec in view.block_txs(c, h)? {
                if rec.key.is_coinbase() {
                    let d = load(view, rec.key, Some(rec), &mut blocks)?;
                    return Ok(d.filter(|d| d.rk() == rk));
                }
            }
            Ok(None)
        })
        .await
        .map_err(from_api)
}

pub async fn node(ctx: &Context<'_>, id: &ID) -> Result<Option<Node>> {
    let key = decode(id.as_str()).map_err(|_| bad_input("invalid id"))?;
    Ok(match key {
        NodeKey::Block { hash } => super::block::query_block(ctx, &hash)
            .await?
            .map(Node::Block),
        NodeKey::Transaction {
            block_hash,
            request_key,
        } => tx(ctx, Some(block_hash), request_key)
            .await?
            .map(Node::Transaction),
        NodeKey::Event {
            block_hash,
            order_index,
            request_key,
        } => row_tx(ctx, block_hash, request_key).await?.and_then(|d| {
            (0..d.evs.len())
                .map(|i| Event::new(d.clone(), i))
                .find(|e| e.ord_idx() == order_index)
                .map(Node::Event)
        }),
        NodeKey::Transfer {
            block_hash,
            chain,
            order_index,
            module_hash,
            request_key,
        } => row_tx(ctx, block_hash, request_key).await?.and_then(|d| {
            (0..d.trs.len())
                .map(|i| Transfer::new(d.clone(), i, None))
                .find(|x| {
                    x.ord_idx() == order_index
                        && i64::from(x.d.key().chain()) == chain
                        && x.mod_hash() == module_hash
                })
                .map(Node::Transfer)
        }),
        NodeKey::Signer {
            request_key,
            order_index,
        } => tx(ctx, None, request_key.clone()).await?.and_then(|t| {
            let i = usize::try_from(order_index).ok()?;
            let s =
                t.d.cmd
                    .as_ref()?
                    .get("signers")?
                    .as_array()?
                    .get(i)?
                    .clone();
            Some(Node::Signer(Signer::of(&request_key, i, &s)))
        }),
        NodeKey::FungibleAccount { fungible, account } => {
            super::account::fungible_account(ctx, &account, Some(&fungible))
                .await?
                .map(Node::FungibleAccount)
        }
        NodeKey::FungibleChainAccount {
            chain,
            fungible,
            account,
        } => super::account::fungible_chain_accounts(ctx, &account, &[chain], Some(&fungible))
            .await?
            .and_then(|v| v.into_iter().next())
            .map(Node::FungibleChainAccount),
        NodeKey::NonFungibleAccount { account } => super::nft::non_fungible(ctx, &account, None)
            .await?
            .map(|(account, items)| {
                Node::NonFungibleAccount(NonFungibleAccount { account, items })
            }),
        NodeKey::NonFungibleChainAccount { chain, account } => {
            let c = chain_arg(&chain)?;
            super::nft::non_fungible(ctx, &account, Some(c))
                .await?
                .map(|(account, items)| {
                    Node::NonFungibleChainAccount(NonFungibleChainAccount {
                        account,
                        chain: c,
                        items,
                    })
                })
        }
        NodeKey::NonFungibleTokenBalance {
            token_id,
            account,
            chain,
        } => {
            let c = chain_arg(&chain)?;
            super::nft::non_fungible(ctx, &account, Some(c))
                .await?
                .and_then(|(account, items)| {
                    let item = items.into_iter().find(|i| i.token_id == token_id)?;
                    Some(Node::NonFungibleTokenBalance(NonFungibleTokenBalance {
                        account,
                        item,
                    }))
                })
        }
    })
}

pub async fn nodes(ctx: &Context<'_>, ids: &[ID]) -> Result<Vec<Option<Node>>> {
    if ids.len() > MAX_NODES {
        return Err(bad_input(format!("at most {MAX_NODES} ids")));
    }
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        out.push(node(ctx, id).await?);
    }
    Ok(out)
}
