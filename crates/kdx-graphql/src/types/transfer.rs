//! `Transfer` (Task 4): a transfer of a loaded tx (chainweb-data's `mkTransfer` rule, as
//! compat). `crossChainTransfer` is preloaded with the page when the operation selects it,
//! else read on demand.

use std::sync::Arc;

use async_graphql::{Context, Object, Result, ID};
use serde_json::Value;

use super::{Block, Transaction};
use crate::error::from_api;
use crate::gql;
use crate::ids::{encode, NodeKey};
use crate::scalars::{BigInt, DateTime, Decimal};
use crate::txlist::{block_node, xchain_of, Blocks, TxData, XHalf};
use kdx_api::compat::shapes;

#[derive(Debug, Clone)]
pub struct Transfer {
    pub d: Arc<TxData>,
    /// Index in `d.trs`.
    pub i: usize,
    /// The preloaded other half (`Some(None)`: there is none).
    pub xchain: Option<XHalf>,
    /// An NFT transfer: `i` indexes `d.nfts` (ZelCore `transfers(isNFT: true)`).
    pub nft: bool,
}

impl Transfer {
    pub fn new(d: Arc<TxData>, i: usize, xchain: Option<XHalf>) -> Transfer {
        Transfer {
            d,
            i,
            xchain,
            nft: false,
        }
    }
    /// The NFT transfer at `nfts[i]` of `d`.
    pub fn nft(d: Arc<TxData>, i: usize) -> Transfer {
        Transfer {
            d,
            i,
            xchain: Some(None),
            nft: true,
        }
    }
    fn t(&self) -> &shapes::Transfer {
        if self.nft {
            &self.d.nfts[self.i].tr
        } else {
            &self.d.trs[self.i]
        }
    }
    pub(crate) fn ord_idx(&self) -> i64 {
        shapes::ev_idx(self.d.key(), self.t().pos)
    }
    pub(crate) fn mod_hash(&self) -> String {
        self.d
            .evs
            .get(self.t().pos)
            .map(|e| e.module_hash.clone())
            .unwrap_or_default()
    }
}

/// The exact amount text of a transfer: the decimal of the event's third parameter, else
/// chainweb-data's rendering.
fn amount(d: &TxData, t: &shapes::Transfer) -> Decimal {
    let p = d.evs.get(t.pos).and_then(|e| e.params.get(2));
    let text = match p {
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(Value::Object(o)) => o.get("decimal").or_else(|| o.get("int")).map(|v| match v {
            Value::String(s) => s.clone(),
            v => v.to_string(),
        }),
        _ => None,
    };
    text.as_deref()
        .and_then(Decimal::new)
        .or_else(|| Decimal::new(&t.amount))
        .unwrap_or_else(|| Decimal::new("0").expect("0 is a decimal"))
}

#[Object]
impl Transfer {
    async fn amount(&self) -> Decimal {
        if self.nft {
            let t = self.t();
            return Decimal::new(&t.amount)
                .unwrap_or_else(|| Decimal::new("0").expect("0 is a decimal"))
                .into_text();
        }
        amount(&self.d, self.t()).into_text()
    }
    /// ZelCore extension: the token id of an NFT transfer; `null` for a fungible one.
    async fn token_id(&self) -> Option<String> {
        self.nft.then(|| self.d.nfts[self.i].token_id.clone())
    }
    async fn block(&self) -> Block {
        block_node(&self.d.block)
    }
    #[graphql(deprecation = "Use `block.hash` field instead.")]
    async fn block_hash(&self) -> String {
        self.d.block_hash()
    }
    #[graphql(deprecation = "Use `block.chainId` field instead.")]
    async fn chain_id(&self) -> BigInt {
        BigInt::new(i128::from(self.d.key().chain()))
    }
    async fn creation_time(&self) -> DateTime {
        DateTime(self.d.block.creation_time as i64)
    }
    async fn cross_chain_transfer(&self, ctx: &Context<'_>) -> Result<Option<Transfer>> {
        let half = match &self.xchain {
            Some(x) => x.clone(),
            None => {
                let (d, i) = (self.d.clone(), self.i);
                gql(ctx)
                    .state()?
                    .read(move |view| xchain_of(view, &d, i, &mut Blocks::default()))
                    .await
                    .map_err(from_api)?
            }
        };
        // The other half's other half is this transfer (M-5): a nested
        // `crossChainTransfer` answers from it, with no store read.
        let me = Some((self.d.clone(), self.i));
        Ok(half.map(|(d, i)| Transfer::new(d, i, Some(me))))
    }
    #[graphql(deprecation = "Use `block.height` field instead.")]
    async fn height(&self) -> BigInt {
        BigInt::new(i128::from(self.d.key().height()))
    }
    pub async fn id(&self) -> Result<ID> {
        Ok(ID(encode(&NodeKey::Transfer {
            block_hash: self.d.block_hash(),
            chain: i64::from(self.d.key().chain()),
            order_index: self.ord_idx(),
            module_hash: self.mod_hash(),
            request_key: self.d.rk(),
        })))
    }
    async fn module_hash(&self) -> String {
        self.mod_hash()
    }
    async fn module_name(&self) -> String {
        self.t().token.clone()
    }
    async fn order_index(&self) -> BigInt {
        BigInt::new(i128::from(self.ord_idx()))
    }
    async fn receiver_account(&self) -> String {
        self.t().to.clone()
    }
    async fn request_key(&self) -> String {
        self.d.rk()
    }
    async fn sender_account(&self) -> String {
        self.t().from.clone()
    }
    async fn transaction(&self) -> Option<Transaction> {
        Transaction::new(self.d.clone())
    }
}
