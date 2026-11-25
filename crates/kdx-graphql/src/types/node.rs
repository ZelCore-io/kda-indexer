//! `node(id)` and `nodes(ids)` (Task 3): decode a Relay id (`ids.rs`) and load the object
//! through the same paths as the root queries. An id that cannot be read (it does not
//! decode, or its key is not a well-formed key of its type) is `null`, as stock answers
//! (FIX-4 item 5); so is an object that is not stored.

use std::borrow::Cow;

use async_graphql::registry::{MetaType, Registry};
use async_graphql::{Context, InputType, InputValueError, InputValueResult, Result, Value, ID};

use super::{
    Event, NonFungibleAccount, NonFungibleChainAccount, NonFungibleTokenBalance, Signer,
    Transaction, Transfer,
};
use std::sync::Arc;

use kdx_types::Hash32;

use crate::error::{bad_input, from_api, Code};
use crate::gql;
use crate::ids::{decode, NodeKey};
use crate::txlist::chain_arg;
use crate::txlist::{hash_arg, load, Blocks, TxData};
use crate::types::Node;

/// Most ids one `nodes` call takes.
pub const MAX_NODES: usize = 100;

/// A `node` / `nodes` id argument: the GraphQL `ID` scalar, but taking any whole number, as
/// stock does (graphql-js: a string or an integer). async-graphql's own `ID` refuses an
/// integer that does not fit an i64, which answered a client's `9223372036854775808` with a
/// 500 where stock answers `null`. The schema still names the type `ID`.
pub struct AnyId(pub ID);

/// Whether `v` is an `ID` value: a string, or a whole number of any size.
fn is_id_value(v: &Value) -> bool {
    match v {
        Value::String(_) => true,
        Value::Number(n) => {
            n.is_i64()
                || n.is_u64()
                || n.as_f64()
                    .is_some_and(|f| f.is_finite() && f.fract() == 0.0)
        }
        _ => false,
    }
}

impl InputType for AnyId {
    type RawValueType = ID;

    fn type_name() -> Cow<'static, str> {
        <ID as InputType>::type_name()
    }

    fn create_type_info(registry: &mut Registry) -> String {
        let name = <ID as InputType>::create_type_info(registry);
        // Widen the registered scalar's validity check (the document validation runs it on
        // literals and on variable values).
        if let Some(MetaType::Scalar { is_valid, .. }) = registry.types.get_mut("ID") {
            *is_valid = Some(std::sync::Arc::new(is_id_value));
        }
        name
    }

    fn parse(value: Option<Value>) -> InputValueResult<Self> {
        match value {
            Some(Value::Number(n)) if is_id_value(&Value::Number(n.clone())) => {
                Ok(AnyId(ID(n.to_string())))
            }
            v => <ID as InputType>::parse(v)
                .map(AnyId)
                .map_err(InputValueError::propagate),
        }
    }

    fn to_value(&self) -> Value {
        self.0.to_value()
    }

    fn as_raw_value(&self) -> Option<&ID> {
        Some(&self.0)
    }
}

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
    let Ok(key) = decode(id.as_str()) else {
        return Ok(None);
    };
    unreadable_is_null(lookup(ctx, key).await)
}

/// The id is the only input: a key the lookup refuses as malformed (`BAD_USER_INPUT`) is an
/// unreadable id, `null`. A lookup that finds several copies of a tx is an answer about
/// stored data, not about the id: it keeps its error (FIX-4 part C, M-3).
fn unreadable_is_null<T>(r: Result<Option<T>>) -> Result<Option<T>> {
    match r {
        Err(e) if is_bad_input(&e) && e.message != super::tx::MANY_COPIES => Ok(None),
        r => r,
    }
}

fn is_bad_input(e: &async_graphql::Error) -> bool {
    e.extensions
        .as_ref()
        .and_then(|x| x.get("code"))
        .is_some_and(|c| *c == async_graphql::Value::from(Code::BadUserInput.as_str()))
}

async fn lookup(ctx: &Context<'_>, key: NodeKey) -> Result<Option<Node>> {
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
            // Fungible transfers, then the NFT transfers `transfers(isNFT: true)` lists under
            // the same id form (audit F-51).
            let fungible = (0..d.trs.len()).map(|i| Transfer::new(d.clone(), i, None));
            let nfts = (0..d.nfts.len()).map(|i| Transfer::nft(d.clone(), i));
            fungible
                .chain(nfts)
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
        } => super::account::fungible_chain_account_node(ctx, &account, &chain, &fungible)
            .await?
            .map(Node::FungibleChainAccount),
        NodeKey::NonFungibleAccount { account } => super::nft::non_fungible(ctx, &account, None)
            .await?
            .map(|(account, items, cut)| {
                Node::NonFungibleAccount(NonFungibleAccount {
                    account,
                    items,
                    cut,
                })
            }),
        NodeKey::NonFungibleChainAccount { chain, account } => {
            let c = chain_arg(&chain)?;
            super::nft::non_fungible(ctx, &account, Some(c))
                .await?
                .map(|(account, items, cut)| {
                    Node::NonFungibleChainAccount(NonFungibleChainAccount {
                        account,
                        chain: c,
                        items,
                        cut,
                    })
                })
        }
        NodeKey::NonFungibleTokenBalance {
            token_id,
            account,
            chain,
        } => {
            let c = chain_arg(&chain)?;
            match super::nft::non_fungible(ctx, &account, Some(c)).await? {
                None => None,
                Some((account, items, cut)) => {
                    match items.into_iter().find(|i| i.token_id == token_id) {
                        Some(item) => {
                            Some(Node::NonFungibleTokenBalance(NonFungibleTokenBalance {
                                account,
                                item,
                            }))
                        }
                        // Past the cap: unknown, not absent (part E, S1).
                        None if cut => return Err(super::nft::holdings_cut()),
                        None => None,
                    }
                }
            }
        }
    })
}

pub async fn nodes(ctx: &Context<'_>, ids: &[AnyId]) -> Result<Vec<Option<Node>>> {
    if ids.len() > MAX_NODES {
        return Err(bad_input(format!("at most {MAX_NODES} ids")));
    }
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        out.push(node(ctx, &id.0).await?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{bad_input, is_bad_input, unreadable_is_null};
    use async_graphql::{Request, Result, Variables};
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
                d3: Default::default(),
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

    /// FIX-4 part C (M-3): only a refused key is an unreadable id (`null`); a lookup that
    /// finds the object ambiguous (a genesis tx stored on several chains, named by a `Signer`
    /// id without a block) keeps its `BAD_USER_INPUT` error.
    #[test]
    fn only_a_refused_key_answers_null() {
        let malformed: Result<Option<()>> = Err(bad_input("blockHash: not a hash"));
        assert!(matches!(unreadable_is_null(malformed), Ok(None)));
        let many: Result<Option<()>> = Err(bad_input(super::super::tx::MANY_COPIES));
        let e = unreadable_is_null(many).unwrap_err();
        assert_eq!(e.message, super::super::tx::MANY_COPIES);
        assert!(is_bad_input(&e));
        let other: Result<Option<()>> = Err(async_graphql::Error::new("internal"));
        assert!(unreadable_is_null(other).is_err());
        assert!(matches!(unreadable_is_null(Ok(Some(()))), Ok(Some(()))));
    }
}
