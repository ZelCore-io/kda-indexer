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
/// (whose rows carry the node's coinbase `reqKey`, R-P5P9-57 F4; ids issued before it carry
/// compat's `<coinbase>`, still accepted).
async fn row_tx(ctx: &Context<'_>, block_hash: String, rk: String) -> Result<Option<Arc<TxData>>> {
    if Hash32::from_b64url(&rk).is_ok() {
        if let Some(t) = tx(ctx, Some(block_hash.clone()), rk.clone()).await? {
            return Ok(Some(t.d));
        }
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
                    return Ok(d.filter(|d| {
                        d.rk() == rk || rk == kdx_api::compat::shapes::COINBASE_RK_TEXT
                    }));
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

#[cfg(test)]
mod tests {
    use async_graphql::{Request, Variables};
    use kdx_decode::{IndexedBlock, TxIndex};
    use kdx_types::record::{BlockRecord, TxRecord};
    use kdx_types::{Hash32, TxKey};
    use serde_json::{json, Value};

    use crate::ids::{encode, NodeKey};
    use crate::testutil;

    /// A coinbase `reqKey` as the node sends it on Pact 5 (base64url of the parent hash
    /// text, h5801243 c4): not a 32-byte hash.
    const CB_RK: &str = "aVdla04wempYOThOVHN6UXlPV3hXRmM1azVzODc1SDYtYmpSTFdNOUgyMA";
    const BH: Hash32 = Hash32([42; 32]);
    const USER_RK: Hash32 = Hash32([5; 32]);

    fn out(rk: &str) -> Value {
        json!({
            "reqKey": rk,
            "result": {"status": "success", "data": "ok"},
            "events": [{"name": "TRANSFER", "module": {"name": "coin", "namespace": null},
                "moduleHash": "mh", "params": ["", "miner", 1.0]}]
        })
    }

    fn rec(pos: u16, rk: Option<Hash32>, out: &Value, cmd: Option<Value>) -> TxRecord {
        TxRecord {
            key: TxKey::new(10, 0, pos),
            request_key: rk,
            status_ok: true,
            cmd: cmd.map(|c| serde_json::to_vec(&c).unwrap()),
            raw_output: Some(serde_json::to_vec(out).unwrap()),
            ..TxRecord::default()
        }
    }

    /// Block (chain 0, height 10): a coinbase whose output carries `CB_RK`, and one user tx.
    fn fixture() -> testutil::Fixture {
        let f = testutil::store();
        let cmd = json!({"meta": {"chainId": "0", "creationTime": 1, "sender": "a"},
            "payload": {"exec": {"code": "1", "data": {}}}, "signers": [], "nonce": "n",
            "networkId": "mainnet01"});
        let txs = vec![
            rec(0, None, &out(CB_RK), None),
            rec(1, Some(USER_RK), &out(&USER_RK.to_b64url()), Some(cmd)),
        ];
        let idx = txs
            .iter()
            .map(|r| TxIndex {
                key: r.key,
                postings: Vec::new(),
                pact_id: None,
                code_hash: None,
            })
            .collect();
        f.apply_block(&IndexedBlock {
            block: BlockRecord {
                hash: BH,
                chain: 0,
                height: 10,
                ..BlockRecord::default()
            },
            txs,
            idx,
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        });
        f
    }

    fn event_id(rk: &str) -> String {
        encode(&NodeKey::Event {
            block_hash: BH.to_b64url(),
            order_index: 0,
            request_key: rk.to_owned(),
        })
    }

    /// `node(id)` for an event id: its `requestKey`, or null.
    async fn node_rk(f: &testutil::Fixture, id: &str) -> Value {
        let schema = crate::build_schema(f.st.clone(), crate::GraphqlSettings::default(), None);
        let q = "query($id: ID!) { node(id: $id) { ... on Event { requestKey orderIndex } } }";
        let r = schema
            .execute(Request::new(q).variables(Variables::from_json(json!({"id": id}))))
            .await;
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        r.data.into_json().unwrap()["node"].clone()
    }

    /// Review of the F4 change (Important 2): coinbase event ids resolve by the node's
    /// coinbase reqKey (new ids) and by compat's `<coinbase>` (ids issued before it); a
    /// user-tx id resolves its tx, and one that misses falls through to the block's
    /// coinbase, which does not match it, so it answers null.
    #[tokio::test]
    async fn node_resolves_coinbase_and_user_tx_event_ids() {
        let f = fixture();
        // A new coinbase id: the node's reqKey.
        let v = node_rk(&f, &event_id(CB_RK)).await;
        assert_eq!(v, json!({"requestKey": CB_RK, "orderIndex": 0}));
        // An old coinbase id: `<coinbase>` still names the block's coinbase.
        let v = node_rk(&f, &event_id(kdx_api::compat::shapes::COINBASE_RK_TEXT)).await;
        assert_eq!(v, json!({"requestKey": CB_RK, "orderIndex": 0}));
        // A user-tx id resolves its own tx.
        let v = node_rk(&f, &event_id(&USER_RK.to_b64url())).await;
        assert_eq!(
            v,
            json!({"requestKey": USER_RK.to_b64url(), "orderIndex": 0})
        );
        // A user-tx id that misses: not this block's tx, and not its coinbase.
        let miss = Hash32([6; 32]).to_b64url();
        assert_eq!(node_rk(&f, &event_id(&miss)).await, Value::Null);
        // An id whose text is neither a hash nor the coinbase's reqKey.
        assert_eq!(node_rk(&f, &event_id("not-a-key")).await, Value::Null);
    }
}
