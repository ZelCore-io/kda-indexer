//! `Event` (Task 4): one event of a loaded tx, as chainweb-data stores it (coinbase events
//! carry the node's coinbase request key and order index 0, R-P5P9-57 F4).

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
    pub(crate) fn ord_idx(&self) -> i64 {
        shapes::ev_idx(self.d.key(), self.ev().pos)
    }
}

#[Object]
impl Event {
    async fn block(&self) -> Block {
        block_node(&self.d.block)
    }
    async fn chain_id(&self) -> BigInt {
        BigInt::new(i128::from(self.d.key().chain()))
    }
    async fn height(&self) -> BigInt {
        BigInt::new(i128::from(self.d.key().height()))
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
        BigInt::new(i128::from(self.ord_idx()))
    }
    /// The parameters as aeson text with the oracle's number layout (`0.00000236`, not
    /// `2.36e-6`): kadscan reads this field (R-P5P9-57 F9). `parameters` keeps the node's
    /// numbers (no captured kadscan document requests it).
    async fn parameter_text(&self) -> String {
        let mut b = Vec::new();
        shapes::write_aeson_with(&self.ev().params, &mut b, &crate::scalars::js_number);
        String::from_utf8(b).unwrap_or_default()
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
