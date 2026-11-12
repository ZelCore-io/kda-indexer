//! `Event` (Task 4): one event of a loaded tx, as chainweb-data stores it (coinbase events
//! carry request key `cb` and order index 0, as compat).

use std::sync::Arc;

use async_graphql::{Object, Result, ID};

use super::{Block, Transaction};
use crate::ids::{encode, NodeKey};
use crate::scalars::BigInt;
use crate::txlist::{block_node, TxData};
use crate::types::tx::json_text;
use kdx_api::compat::shapes::{self, Ev};

#[derive(Debug, Clone)]
pub struct Event {
    pub d: Arc<TxData>,
    /// Index in `d.evs`.
    pub i: usize,
}

impl Event {
    pub fn new(d: Arc<TxData>, i: usize) -> Event {
        Event { d, i }
    }
    fn ev(&self) -> &Ev {
        &self.d.evs[self.i]
    }
    fn ord_idx(&self) -> i64 {
        shapes::ev_idx(self.d.key(), self.ev().pos)
    }
}

#[Object]
impl Event {
    async fn block(&self) -> Block {
        block_node(&self.d.block)
    }
    async fn chain_id(&self) -> BigInt {
        BigInt(i128::from(self.d.key().chain()))
    }
    async fn height(&self) -> BigInt {
        BigInt(i128::from(self.d.key().height()))
    }
    pub async fn id(&self) -> Result<ID> {
        Ok(ID(encode(&NodeKey::Event {
            block_hash: self.d.block_hash(),
            order_index: self.ord_idx(),
            request_key: self.d.rk(),
        })))
    }
    async fn module_name(&self) -> String {
        self.ev().module.clone()
    }
    async fn name(&self) -> String {
        let e = self.ev();
        e.qual_name
            .strip_prefix(&e.module)
            .and_then(|s| s.strip_prefix('.'))
            .unwrap_or(&e.qual_name)
            .to_owned()
    }
    async fn order_index(&self) -> BigInt {
        BigInt(i128::from(self.ord_idx()))
    }
    async fn parameter_text(&self) -> String {
        shapes::param_text(&self.ev().params)
    }
    async fn parameters(&self) -> Option<String> {
        Some(json_text(&self.ev().params))
    }
    async fn qualified_name(&self) -> String {
        self.ev().qual_name.clone()
    }
    async fn request_key(&self) -> String {
        self.d.rk()
    }
    async fn transaction(&self) -> Option<Transaction> {
        Transaction::new(self.d.clone())
    }
}
