//! Transactions, their command, payloads, results and signers (Task 4).
//!
//! Every field answers from the [`TxData`] loaded with the page (no store access), except
//! the cross-chain half of a transfer that was not preloaded.

use std::sync::Arc;

use async_graphql::{Context, Object, Result, SimpleObject, Union, ID};
use serde_json::Value;

use super::{
    Block, Event, TransactionResultEventsConnection, TransactionResultEventsConnectionEdge,
    TransactionResultTransfersConnection, TransactionResultTransfersConnectionEdge, Transfer,
};
use super::{
    QueryEventsConnection, QueryEventsConnectionEdge, QueryTransactionsByPublicKeyConnection,
    QueryTransactionsByPublicKeyConnectionEdge, QueryTransactionsConnection,
    QueryTransactionsConnectionEdge, QueryTransfersConnection, QueryTransfersConnectionEdge,
};
use crate::connection::{PageArgs, Total};
use crate::error::{bad_input, from_api};
use crate::error::{err, Code};
use crate::gql;
use crate::ids::{encode, NodeKey};
use crate::scalars::{BigInt, DateTime};
use crate::txlist::{
    block_node, chain_arg, hash_arg, height_arg, list, load, name_arg, page_in_tx, Blocks, Item,
    KeyFilter, ListPage, ListSpec, ParamFilter, Rows, SourceSpec, TxData,
};

/// A user transaction (coinbase txs are not listed as transactions, as in Kadena graph).
#[derive(Debug, Clone)]
pub struct Transaction {
    pub d: Arc<TxData>,
}

/// Compact JSON text of a value (numbers keep their exact text).
pub fn json_text(v: &Value) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "null".into())
}

/// An integer field that may be a number, a numeric string or `{"int": n}`.
pub fn int_of(v: Option<&Value>) -> i128 {
    match v {
        Some(Value::Number(n)) => n
            .to_string()
            .parse::<i128>()
            .ok()
            .or_else(|| n.as_f64().map(|f| f as i128))
            .unwrap_or(0),
        Some(Value::String(s)) => s.trim().parse::<i128>().unwrap_or(0),
        Some(Value::Object(o)) => int_of(o.get("int")),
        _ => 0,
    }
}

fn float_of(v: Option<&Value>) -> f64 {
    match v {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        Some(Value::String(s)) => s.trim().parse().unwrap_or(0.0),
        Some(Value::Object(o)) => float_of(o.get("decimal").or_else(|| o.get("int"))),
        _ => 0.0,
    }
}

fn str_of(v: Option<&Value>) -> String {
    v.and_then(Value::as_str).unwrap_or_default().to_owned()
}

impl Transaction {
    pub fn new(d: Arc<TxData>) -> Option<Transaction> {
        (!d.is_coinbase()).then_some(Transaction { d })
    }
}

#[Object]
impl Transaction {
    async fn cmd(&self) -> Result<TransactionCommand> {
        match &self.d.cmd {
            Some(_) => Ok(TransactionCommand { d: self.d.clone() }),
            None => Err(err(
                Code::Internal,
                "the command of this tx is not readable",
            )),
        }
    }
    async fn hash(&self) -> String {
        self.d.rk()
    }
    pub async fn id(&self) -> Result<ID> {
        Ok(ID(encode(&NodeKey::Transaction {
            block_hash: self.d.block_hash(),
            request_key: self.d.rk(),
        })))
    }
    async fn result(&self) -> TransactionInfo {
        TransactionInfo::TransactionResult(TransactionResult { d: self.d.clone() })
    }
    async fn sigs(&self) -> Vec<TransactionSignature> {
        self.d
            .rec
            .sigs
            .iter()
            .map(|s| TransactionSignature { sig: s.to_wire() })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct TransactionCommand {
    d: Arc<TxData>,
}

impl TransactionCommand {
    fn cmd(&self) -> &Value {
        self.d.cmd.as_ref().unwrap_or(&Value::Null)
    }
}

#[Object]
impl TransactionCommand {
    async fn meta(&self) -> TransactionMeta {
        let m = self.cmd().get("meta");
        let g = |k: &str| m.and_then(|m| m.get(k));
        TransactionMeta {
            chain_id: BigInt(int_of(g("chainId"))),
            creation_time: DateTime((float_of(g("creationTime")) * 1e6) as i64),
            gas_limit: BigInt(int_of(g("gasLimit"))),
            gas_price: float_of(g("gasPrice")),
            sender: str_of(g("sender")),
            ttl: BigInt(int_of(g("ttl"))),
        }
    }
    async fn network_id(&self) -> String {
        str_of(self.cmd().get("networkId"))
    }
    async fn nonce(&self) -> String {
        str_of(self.cmd().get("nonce"))
    }
    async fn payload(&self) -> TransactionPayload {
        let p = self.cmd().get("payload");
        if let Some(c) = p.and_then(|p| p.get("cont")) {
            return TransactionPayload::ContinuationPayload(ContinuationPayload {
                data: json_text(c.get("data").unwrap_or(&Value::Null)),
                pact_id: c.get("pactId").and_then(Value::as_str).map(str::to_owned),
                proof: c.get("proof").and_then(Value::as_str).map(str::to_owned),
                rollback: c.get("rollback").and_then(Value::as_bool),
                step: c.get("step").and_then(Value::as_i64).map(|s| s as i32),
            });
        }
        let e = p.and_then(|p| p.get("exec"));
        TransactionPayload::ExecutionPayload(ExecutionPayload {
            code: e
                .and_then(|e| e.get("code"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            data: json_text(e.and_then(|e| e.get("data")).unwrap_or(&Value::Null)),
        })
    }
    async fn signers(&self) -> Vec<Signer> {
        let rk = self.d.rk();
        self.cmd()
            .get("signers")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .enumerate()
                    .map(|(i, s)| Signer::of(&rk, i, s))
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct TransactionMeta {
    pub chain_id: BigInt,
    pub creation_time: DateTime,
    pub gas_limit: BigInt,
    pub gas_price: f64,
    pub sender: String,
    pub ttl: BigInt,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ContinuationPayload {
    pub data: String,
    pub pact_id: Option<String>,
    pub proof: Option<String>,
    pub rollback: Option<bool>,
    pub step: Option<i32>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ExecutionPayload {
    pub code: Option<String>,
    pub data: String,
}

#[derive(Debug, Clone, Union)]
pub enum TransactionPayload {
    ContinuationPayload(ContinuationPayload),
    ExecutionPayload(ExecutionPayload),
}

/// Never returned (GQ-3): the indexer has no mempool. Present for schema parity.
#[derive(Debug, Clone, SimpleObject)]
pub struct TransactionMempoolInfo {
    pub status: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TransactionResult {
    d: Arc<TxData>,
}

impl TransactionResult {
    fn ok(&self) -> bool {
        self.d.out.pointer("/result/status").and_then(Value::as_str) == Some("success")
    }
}

#[Object]
impl TransactionResult {
    async fn bad_result(&self) -> Option<String> {
        (!self.ok()).then(|| json_text(self.d.out.pointer("/result/error").unwrap_or(&Value::Null)))
    }
    async fn block(&self) -> Block {
        block_node(&self.d.block)
    }
    async fn continuation(&self) -> Option<String> {
        self.d
            .out
            .get("continuation")
            .filter(|c| !c.is_null())
            .map(json_text)
    }
    async fn event_count(&self) -> Option<BigInt> {
        Some(BigInt(self.d.evs.len() as i128))
    }
    async fn events(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<TransactionResultEventsConnection> {
        let page = PageArgs::new(after, before, first, last).parse(&gql(ctx).settings)?;
        let rows = (0..self.d.evs.len())
            .map(|i| (self.d.evs[i].pos.min(u16::MAX as usize) as u16, i))
            .collect();
        let (edges, page_info) = page_in_tx(&self.d, rows, &page)?;
        Ok(TransactionResultEventsConnection {
            edges: edges
                .into_iter()
                .map(|(cursor, i)| TransactionResultEventsConnectionEdge {
                    cursor,
                    node: Event::new(self.d.clone(), i),
                })
                .collect(),
            page_info,
            total: Total::Exact(self.d.evs.len() as u64),
        })
    }
    async fn gas(&self) -> BigInt {
        BigInt(int_of(self.d.out.get("gas")))
    }
    async fn good_result(&self) -> Option<String> {
        self.ok()
            .then(|| json_text(self.d.out.pointer("/result/data").unwrap_or(&Value::Null)))
    }
    #[graphql(deprecation = "Use `block.height` instead.")]
    async fn height(&self) -> BigInt {
        BigInt(i128::from(self.d.key().height()))
    }
    async fn logs(&self) -> Option<String> {
        self.d
            .out
            .get("logs")
            .and_then(Value::as_str)
            .map(str::to_owned)
    }
    #[graphql(deprecation = "Not used.")]
    async fn metadata(&self) -> String {
        json_text(self.d.out.get("metaData").unwrap_or(&Value::Null))
    }
    async fn transaction_id(&self) -> Option<BigInt> {
        self.d
            .out
            .get("txId")
            .filter(|v| !v.is_null())
            .map(|v| BigInt(int_of(Some(v))))
    }
    async fn transfers(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<TransactionResultTransfersConnection> {
        let page = PageArgs::new(after, before, first, last).parse(&gql(ctx).settings)?;
        let rows = (0..self.d.trs.len())
            .map(|i| (self.d.trs[i].pos.min(u16::MAX as usize) as u16, i))
            .collect();
        let (edges, page_info) = page_in_tx(&self.d, rows, &page)?;
        Ok(TransactionResultTransfersConnection {
            edges: edges
                .into_iter()
                .map(|(cursor, i)| TransactionResultTransfersConnectionEdge {
                    cursor,
                    node: Transfer::new(self.d.clone(), i, None),
                })
                .collect(),
            page_info,
        })
    }
}

#[derive(Debug, Clone, Union)]
pub enum TransactionInfo {
    TransactionMempoolInfo(TransactionMempoolInfo),
    TransactionResult(TransactionResult),
}

#[derive(Debug, Clone, SimpleObject)]
pub struct TransactionSignature {
    pub sig: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct TransactionCapability {
    pub args: String,
    pub name: String,
}

/// A signer of the command, at `order_index` in its `signers` list.
#[derive(Debug, Clone, Default)]
pub struct Signer {
    pub request_key: String,
    pub order_index: usize,
    pub pubkey: String,
    pub address: Option<String>,
    pub scheme: Option<String>,
    pub clist: Vec<TransactionCapability>,
}

impl Signer {
    fn of(rk: &str, i: usize, s: &Value) -> Signer {
        Signer {
            request_key: rk.to_owned(),
            order_index: i,
            pubkey: str_of(s.get("pubKey")),
            address: s.get("addr").and_then(Value::as_str).map(str::to_owned),
            scheme: s.get("scheme").and_then(Value::as_str).map(str::to_owned),
            clist: s
                .get("clist")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .map(|c| TransactionCapability {
                            name: str_of(c.get("name")),
                            args: json_text(c.get("args").unwrap_or(&Value::Array(vec![]))),
                        })
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

#[Object]
impl Signer {
    async fn address(&self) -> Option<String> {
        self.address.clone()
    }
    async fn clist(&self) -> Vec<TransactionCapability> {
        self.clist.clone()
    }
    pub async fn id(&self) -> Result<ID> {
        Ok(ID(encode(&NodeKey::Signer {
            request_key: self.request_key.clone(),
            order_index: self.order_index as i64,
        })))
    }
    async fn order_index(&self) -> Option<i32> {
        Some(self.order_index as i32)
    }
    async fn pubkey(&self) -> String {
        self.pubkey.clone()
    }
    async fn scheme(&self) -> Option<String> {
        self.scheme.clone()
    }
}

// ---------------------------------------------------------------------------------------
// Query roots (Task 4)

fn txs(
    p: ListPage,
) -> (
    Vec<(String, Transaction)>,
    crate::connection::PageInfo,
    Total,
) {
    let edges = p
        .edges
        .into_iter()
        .filter_map(|(c, it)| match it {
            Item::Tx(d) => Transaction::new(d).map(|t| (c, t)),
            _ => None,
        })
        .collect();
    (edges, p.info, p.total)
}

/// `transaction(requestKey, blockHash, minimumDepth)`: `null` when not indexed; several
/// copies (genesis, R-P5P9-5) without a `blockHash` that picks one are `BAD_USER_INPUT`.
pub async fn query_transaction(
    ctx: &Context<'_>,
    block_hash: Option<String>,
    minimum_depth: Option<i32>,
    request_key: String,
) -> Result<Option<Transaction>> {
    let rk = hash_arg("requestKey", &request_key)?;
    let bh = block_hash.map(|h| hash_arg("blockHash", &h)).transpose()?;
    let depth = height_arg("minimumDepth", minimum_depth)?;
    let found = gql(ctx)
        .state()?
        .read(move |view| {
            let tips = match depth {
                Some(_) => view.chain_tips()?,
                None => vec![],
            };
            let mut blocks = Blocks::default();
            let mut out = Vec::new();
            for (k, r) in view.txs_by_rk(&rk)? {
                if k.is_coinbase() {
                    continue;
                }
                if let Some(dp) = depth {
                    let max = tips
                        .iter()
                        .find(|(c, _, _)| *c == k.chain())
                        .and_then(|(_, h, _)| h.checked_sub(dp));
                    if max.is_none_or(|m| k.height() > m) {
                        continue;
                    }
                }
                if let Some(d) = load(view, k, Some(r), &mut blocks)? {
                    if bh.is_none_or(|h| d.block.hash == h) {
                        out.push(d);
                    }
                }
            }
            Ok(out)
        })
        .await
        .map_err(from_api)?;
    match found.len() {
        0 => Ok(None),
        1 => Ok(Transaction::new(found[0].clone())),
        _ => Err(bad_input(
            "multiple transactions found: pass blockHash to pick one",
        )),
    }
}

/// The source of a `transactions`/`transfers` list (GQ-19, GQ-20).
fn list_source(
    account: Option<&str>,
    fungible: Option<&str>,
    block_hash: Option<&str>,
    request_key: Option<&str>,
) -> Result<SourceSpec> {
    Ok(if let Some(rk) = request_key {
        SourceSpec::Rk(hash_arg("requestKey", rk)?)
    } else if let Some(bh) = block_hash {
        SourceSpec::Block(hash_arg("blockHash", bh)?)
    } else {
        match (account, fungible) {
            (Some(a), Some(m)) => SourceSpec::Xfer {
                account: name_arg("accountName", a)?.to_owned(),
                module: name_arg("fungibleName", m)?.to_owned(),
            },
            (Some(a), None) => SourceSpec::Acct(name_arg("accountName", a)?.to_owned()),
            (None, Some(m)) => {
                SourceSpec::EvtName(format!("{}.TRANSFER", name_arg("fungibleName", m)?))
            }
            (None, None) => SourceSpec::Heights,
        }
    })
}

fn key_filter(
    chain_id: Option<&str>,
    min_height: Option<i32>,
    max_height: Option<i32>,
    minimum_depth: Option<i32>,
    user_only: bool,
) -> Result<KeyFilter> {
    Ok(KeyFilter {
        chain: chain_id.map(chain_arg).transpose()?,
        min_h: height_arg("minHeight", min_height)?,
        max_h: height_arg("maxHeight", max_height)?,
        depth: height_arg("minimumDepth", minimum_depth)?,
        user_only,
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn query_transactions(
    ctx: &Context<'_>,
    account_name: Option<String>,
    after: Option<String>,
    before: Option<String>,
    block_hash: Option<String>,
    chain_id: Option<String>,
    first: Option<i32>,
    fungible_name: Option<String>,
    last: Option<i32>,
    max_height: Option<i32>,
    min_height: Option<i32>,
    minimum_depth: Option<i32>,
    request_key: Option<String>,
) -> Result<QueryTransactionsConnection> {
    let spec = ListSpec {
        source: list_source(
            account_name.as_deref(),
            fungible_name.as_deref(),
            block_hash.as_deref(),
            request_key.as_deref(),
        )?,
        filter: key_filter(
            chain_id.as_deref(),
            min_height,
            max_height,
            minimum_depth,
            true,
        )?,
        rows: Rows::Txs {
            sender: None,
            nft: false,
        },
    };
    let (edges, page_info, total) =
        txs(list(ctx, spec, PageArgs::new(after, before, first, last)).await?);
    Ok(QueryTransactionsConnection {
        edges: edges
            .into_iter()
            .map(|(cursor, node)| QueryTransactionsConnectionEdge { cursor, node })
            .collect(),
        page_info,
        total,
    })
}

pub async fn query_transactions_by_public_key(
    ctx: &Context<'_>,
    after: Option<String>,
    before: Option<String>,
    first: Option<i32>,
    last: Option<i32>,
    public_key: String,
) -> Result<QueryTransactionsByPublicKeyConnection> {
    let spec = ListSpec {
        source: SourceSpec::Pk(name_arg("publicKey", &public_key)?.to_owned()),
        filter: KeyFilter {
            user_only: true,
            ..KeyFilter::default()
        },
        rows: Rows::Txs {
            sender: None,
            nft: false,
        },
    };
    let (edges, page_info, total) =
        txs(list(ctx, spec, PageArgs::new(after, before, first, last)).await?);
    Ok(QueryTransactionsByPublicKeyConnection {
        edges: edges
            .into_iter()
            .map(|(cursor, node)| QueryTransactionsByPublicKeyConnectionEdge { cursor, node })
            .collect(),
        page_info,
        total,
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn query_transfers(
    ctx: &Context<'_>,
    account_name: Option<String>,
    after: Option<String>,
    before: Option<String>,
    block_hash: Option<String>,
    chain_id: Option<String>,
    first: Option<i32>,
    fungible_name: Option<String>,
    last: Option<i32>,
    request_key: Option<String>,
) -> Result<QueryTransfersConnection> {
    let spec = ListSpec {
        source: list_source(
            account_name.as_deref(),
            fungible_name.as_deref(),
            block_hash.as_deref(),
            request_key.as_deref(),
        )?,
        filter: key_filter(chain_id.as_deref(), None, None, None, false)?,
        rows: Rows::Transfers {
            module: fungible_name,
            account: account_name,
        },
    };
    let p = list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
    Ok(QueryTransfersConnection {
        edges: p
            .edges
            .into_iter()
            .filter_map(|(cursor, it)| match it {
                Item::Transfer(d, i, x) => Some(QueryTransfersConnectionEdge {
                    cursor,
                    node: Transfer::new(d, i, x),
                }),
                _ => None,
            })
            .collect(),
        page_info: p.info,
        total: p.total,
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn query_events(
    ctx: &Context<'_>,
    after: Option<String>,
    before: Option<String>,
    block_hash: Option<String>,
    chain_id: Option<String>,
    first: Option<i32>,
    last: Option<i32>,
    max_height: Option<i32>,
    min_height: Option<i32>,
    minimum_depth: Option<i32>,
    order_index: Option<i32>,
    parameters_filter: Option<String>,
    qualified_event_name: String,
    request_key: Option<String>,
) -> Result<QueryEventsConnection> {
    let qual = name_arg("qualifiedEventName", &qualified_event_name)?.to_owned();
    let params = parameters_filter
        .as_deref()
        .map(ParamFilter::parse)
        .transpose()?;
    let source = if let Some(rk) = &request_key {
        SourceSpec::Rk(hash_arg("requestKey", rk)?)
    } else if let Some(bh) = &block_hash {
        SourceSpec::Block(hash_arg("blockHash", bh)?)
    } else if let Some(a) = params.as_ref().and_then(|p| p.first_str()) {
        SourceSpec::AcctOrEvt {
            account: a.to_owned(),
            qual: qual.clone(),
        }
    } else {
        SourceSpec::EvtName(qual.clone())
    };
    let spec = ListSpec {
        source,
        filter: key_filter(
            chain_id.as_deref(),
            min_height,
            max_height,
            minimum_depth,
            false,
        )?,
        rows: Rows::Events {
            qual,
            order_index: order_index.map(i64::from),
            params,
        },
    };
    let p = list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
    Ok(QueryEventsConnection {
        edges: p
            .edges
            .into_iter()
            .filter_map(|(cursor, it)| match it {
                Item::Event(d, i) => Some(QueryEventsConnectionEdge {
                    cursor,
                    node: Event::new(d, i),
                }),
                _ => None,
            })
            .collect(),
        page_info: p.info,
        total: p.total,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn number_forms() {
        assert_eq!(int_of(Some(&json!("19"))), 19);
        assert_eq!(int_of(Some(&json!(1500))), 1500);
        assert_eq!(int_of(Some(&json!({"int": 7}))), 7);
        assert_eq!(int_of(None), 0);
        assert_eq!(float_of(Some(&json!(1.0e-8))), 1.0e-8);
        assert_eq!(float_of(Some(&json!({"decimal": "0.5"}))), 0.5);
        assert_eq!(json_text(&json!({"a": [1, "x"]})), r#"{"a":[1,"x"]}"#);
    }

    #[test]
    fn signer_shape() {
        let s = Signer::of(
            "rk",
            1,
            &json!({"pubKey": "ab", "scheme": "ED25519",
                    "clist": [{"name": "coin.GAS", "args": []},
                              {"name": "coin.TRANSFER", "args": ["k:a", "k:b", 1.5]}]}),
        );
        assert_eq!((s.pubkey.as_str(), s.order_index), ("ab", 1));
        assert_eq!(s.address, None);
        assert_eq!(s.clist[1].args, r#"["k:a","k:b",1.5]"#);
        assert_eq!(s.clist[0].args, "[]");
    }
}
