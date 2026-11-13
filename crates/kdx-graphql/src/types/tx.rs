//! Transactions, their command, payloads, results and signers (Task 4).
//!
//! Every field answers from the [`TxData`] loaded with the page (no store access), except
//! the cross-chain half of a transfer that was not preloaded.

use std::sync::Arc;

use async_graphql::{Context, Object, Result, SimpleObject, Union, ID};
use serde_json::Value;

use super::{
    Block, Event, Sel, TransactionResultEventsConnection, TransactionResultEventsConnectionEdge,
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
use crate::scalars::{BigInt, DateTime, FloatText};
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
    /// ZelCore extension: copies of this transaction in orphaned blocks. The store keeps
    /// the canonical chain only (a reorg replaces blocks), so this is always empty.
    async fn orphaned_transactions(&self) -> Option<Vec<Option<Transaction>>> {
        Some(Vec::new())
    }
    pub async fn id(&self) -> Result<ID> {
        Ok(ID(encode(&NodeKey::Transaction {
            block_hash: self.d.block_hash(),
            request_key: self.d.rk(),
        })))
    }
    async fn result(&self) -> Sel<TransactionInfo> {
        Sel(TransactionInfo::TransactionResult(TransactionResult {
            d: self.d.clone(),
        }))
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

    fn payload_value(&self) -> TransactionPayload {
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
            // JSON-quoted, as the oracle sends it and kadscan reads it (R-P5P9-57 F6).
            code: e
                .and_then(|e| e.get("code"))
                .filter(|c| c.is_string())
                .map(json_text),
            data: json_text(e.and_then(|e| e.get("data")).unwrap_or(&Value::Null)),
        })
    }
}

#[Object]
impl TransactionCommand {
    async fn meta(&self) -> TransactionMeta {
        let m = self.cmd().get("meta");
        let g = |k: &str| m.and_then(|m| m.get(k));
        TransactionMeta {
            chain_id: BigInt::new(int_of(g("chainId"))),
            creation_time: DateTime((float_of(g("creationTime")) * 1e6) as i64),
            gas_limit: BigInt::text(int_of(g("gasLimit"))),
            gas_price: FloatText(format!("{}", float_of(g("gasPrice")))),
            sender: str_of(g("sender")),
            ttl: BigInt::text(int_of(g("ttl"))),
        }
    }
    async fn network_id(&self) -> String {
        str_of(self.cmd().get("networkId"))
    }
    async fn nonce(&self) -> String {
        str_of(self.cmd().get("nonce"))
    }
    async fn payload(&self) -> Sel<TransactionPayload> {
        Sel(self.payload_value())
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
    pub gas_price: FloatText,
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
        Some(BigInt::new(self.d.evs.len() as i128))
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
        BigInt::text(int_of(self.d.out.get("gas")))
    }
    async fn good_result(&self) -> Option<String> {
        self.ok()
            .then(|| json_text(self.d.out.pointer("/result/data").unwrap_or(&Value::Null)))
    }
    #[graphql(deprecation = "Use `block.height` instead.")]
    async fn height(&self) -> BigInt {
        BigInt::new(i128::from(self.d.key().height()))
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
            .map(|v| BigInt::text(int_of(Some(v))))
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
            total: Total::Exact(self.d.trs.len() as u64),
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
    pub(crate) fn of(rk: &str, i: usize, s: &Value) -> Signer {
        Signer {
            request_key: rk.to_owned(),
            order_index: i,
            pubkey: str_of(s.get("pubKey")),
            address: s.get("addr").and_then(Value::as_str).map(str::to_owned),
            // "" when the signer names no scheme, as the oracle sends it; a scheme the
            // command names is kept (the node's value, R-P5P9-57 F8).
            scheme: Some(
                s.get("scheme")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            ),
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

/// The list behind `transactions(...)`. GQ-19 (R-P5P9-57 M1): `accountName` lists the txs
/// the account sent (`meta.sender`), as the oracle does, filtered on top of the postings.
fn transactions_spec(
    account_name: Option<&str>,
    block_hash: Option<&str>,
    chain_id: Option<&str>,
    fungible_name: Option<&str>,
    (min_height, max_height, minimum_depth): (Option<i32>, Option<i32>, Option<i32>),
    request_key: Option<&str>,
) -> Result<ListSpec> {
    Ok(ListSpec {
        source: list_source(account_name, fungible_name, block_hash, request_key)?,
        filter: key_filter(chain_id, min_height, max_height, minimum_depth, true)?,
        rows: Rows::Txs {
            sender: account_name.map(str::to_owned),
            nft: false,
        },
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
    is_coinbase: Option<bool>,
) -> Result<QueryTransactionsConnection> {
    if is_coinbase == Some(true) {
        // Coinbase txs are not `Transaction`s here (R7D-3); `Block.coinbase` has them.
        return Err(crate::error::not_supported(
            "transactions(isCoinbase: true) (use Block.coinbase)",
        ));
    }
    let spec = transactions_spec(
        account_name.as_deref(),
        block_hash.as_deref(),
        chain_id.as_deref(),
        fungible_name.as_deref(),
        (min_height, max_height, minimum_depth),
        request_key.as_deref(),
    )?;
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
    is_nft: Option<bool>,
) -> Result<QueryTransfersConnection> {
    let nft = is_nft == Some(true);
    let source = match (nft, &account_name) {
        // NFT moves have no `xfer` postings: an account's come from its `acct` postings.
        (true, Some(a)) if request_key.is_none() && block_hash.is_none() => {
            SourceSpec::Acct(name_arg("accountName", a)?.to_owned())
        }
        _ => list_source(
            account_name.as_deref(),
            fungible_name.as_deref(),
            block_hash.as_deref(),
            request_key.as_deref(),
        )?,
    };
    let spec = ListSpec {
        source,
        filter: key_filter(chain_id.as_deref(), None, None, None, false)?,
        rows: Rows::Transfers {
            module: fungible_name,
            account: account_name,
            nft,
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
                Item::Nft(d, i) => Some(QueryTransfersConnectionEdge {
                    cursor,
                    node: Transfer::nft(d, i),
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
    qualified_event_name: Option<String>,
    request_key: Option<String>,
    module_name: Option<String>,
) -> Result<QueryEventsConnection> {
    let qual = qualified_event_name
        .as_deref()
        .map(|q| name_arg("qualifiedEventName", q).map(str::to_owned))
        .transpose()?;
    let module = module_name
        .as_deref()
        .map(|m| name_arg("moduleName", m).map(str::to_owned))
        .transpose()?;
    if qual.is_none() && module.is_none() {
        return Err(bad_input("events needs qualifiedEventName or moduleName"));
    }
    let params = parameters_filter
        .as_deref()
        .map(ParamFilter::parse)
        .transpose()?;
    let source = if let Some(rk) = &request_key {
        SourceSpec::Rk(hash_arg("requestKey", rk)?)
    } else if let Some(bh) = &block_hash {
        SourceSpec::Block(hash_arg("blockHash", bh)?)
    } else if let Some(q) = &qual {
        match params.as_ref().and_then(|p| p.first_str()) {
            Some(a) => SourceSpec::AcctOrEvt {
                account: a.to_owned(),
                qual: q.clone(),
            },
            None => SourceSpec::EvtName(q.clone()),
        }
    } else {
        // `moduleName` alone (ZelCore extension): no module posting family, so heights are
        // walked down from the tip, as a list without a selective argument (GQ-20).
        SourceSpec::Heights
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
            module,
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

// ---------------------------------------------------------------------------------------
// ZelCore extension: transactionsByPactCode (R-P5P9-16, on the P6 bounded search)

/// One row of `transactionsByPactCode` (ZelCore extension): a flat summary of a user tx.
#[derive(Debug, Clone)]
pub struct TransactionSummary {
    pub d: Arc<TxData>,
}

#[Object]
impl TransactionSummary {
    async fn request_key(&self) -> String {
        self.d.rk()
    }
    async fn height(&self) -> BigInt {
        BigInt::new(i128::from(self.d.key().height()))
    }
    async fn chain_id(&self) -> BigInt {
        BigInt::new(i128::from(self.d.key().chain()))
    }
    /// Whether the row's block is the stored block at its height, as `Block.canonical` (M9).
    async fn canonical(&self, ctx: &Context<'_>) -> Result<bool> {
        super::block::is_stored(ctx, &self.d.block).await
    }
    async fn creation_time(&self) -> DateTime {
        let m = self.d.cmd.as_ref().and_then(|c| c.get("meta"));
        DateTime((float_of(m.and_then(|m| m.get("creationTime"))) * 1e6) as i64)
    }
    async fn bad_result(&self) -> Option<String> {
        (self.d.out.pointer("/result/status").and_then(Value::as_str) != Some("success"))
            .then(|| json_text(self.d.out.pointer("/result/error").unwrap_or(&Value::Null)))
    }
    async fn sender(&self) -> String {
        self.d.sender().unwrap_or_default().to_owned()
    }
    async fn gas(&self) -> String {
        int_of(self.d.out.get("gas")).to_string()
    }
    async fn gas_limit(&self) -> String {
        int_of(
            self.d
                .cmd
                .as_ref()
                .and_then(|c| c.pointer("/meta/gasLimit")),
        )
        .to_string()
    }
    async fn gas_price(&self) -> String {
        number_text(
            self.d
                .cmd
                .as_ref()
                .and_then(|c| c.pointer("/meta/gasPrice")),
        )
    }
    async fn code(&self) -> Option<String> {
        self.d
            .cmd
            .as_ref()
            .and_then(|c| c.pointer("/payload/exec/code"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    }
}

/// A search-gate permit for the client at `ip`. An operation whose client address is not
/// known is refused: sharing one bucket among such clients would defeat the per-client limit.
pub fn search_permit(
    gate: &Arc<kdx_api::search::SearchGate>,
    ip: Option<std::net::IpAddr>,
) -> Result<kdx_api::search::SearchPermit> {
    let ip = ip.ok_or_else(|| {
        err(
            Code::Limit,
            "search needs a known client address (HTTP or an accepted websocket)",
        )
    })?;
    gate.try_acquire(ip).map_err(|_| {
        err(
            Code::Limit,
            "too many searches in flight: try again shortly",
        )
    })
}

/// A number's exact text (a string or `{"decimal": ..}` unwrapped); "0" when absent.
fn number_text(v: Option<&Value>) -> String {
    match v {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) => s.trim().to_owned(),
        Some(Value::Object(o)) => number_text(o.get("decimal").or_else(|| o.get("int"))),
        _ => "0".to_owned(),
    }
}

/// `transactionsByPactCode(pactCode)`: the txs whose own exec code contains `pactCode`
/// (`%`/`_` wildcards), newest first, through the P6 search (`/txs/search`): its pattern
/// checks, per-client gate, code index or bounded newest-first scan, candidate caps and time
/// budget. Forward paging only. A page the search cut short is flagged `searchTruncated`
/// in the response `extensions`, and `pageInfo.endCursor` resumes it.
pub async fn query_transactions_by_pact_code(
    ctx: &Context<'_>,
    after: Option<String>,
    before: Option<String>,
    first: Option<i32>,
    last: Option<i32>,
    pact_code: String,
) -> Result<super::QueryTransactionsByPactCodeConnection> {
    use crate::connection::PageCursor;
    use kdx_api::compat::Paging;
    let g = gql(ctx);
    let Some(search) = g.search.clone() else {
        return Err(crate::error::not_supported(
            "transactionsByPactCode (no search index)",
        ));
    };
    if last.is_some() || before.is_some() {
        return Err(bad_input(
            "transactionsByPactCode pages forward only (first/after)",
        ));
    }
    kdx_api::search::check_pattern("pactCode", &pact_code).map_err(bad_input)?;
    let page = PageArgs::new(after, None, first, None).parse(&g.settings)?;
    let next = match page.cursor {
        None => None,
        Some(PageCursor::Tx(c)) => Some(c),
        Some(PageCursor::Block(_)) => return Err(bad_input("invalid cursor")),
    };
    let had_cursor = next.is_some();
    let permit = search_permit(&search.gate, crate::client_ip(ctx))?;
    let n = page.n;
    let (rows, resume, truncated) = g
        .state()?
        .clone()
        .read_heavy(move |view| {
            let _permit = permit;
            let pg = Paging {
                limit: n,
                offset: 0,
                next,
                min_height: None,
                max_height: None,
                empty: n == 0,
            };
            let p = kdx_api::compat::search::search_page(view, &search, &pact_code, &pg)?;
            let mut blocks = Blocks::default();
            let mut rows = Vec::new();
            for r in &p.rows {
                let rk = r.get("requestKey").and_then(Value::as_str).unwrap_or("");
                let Ok(rk) = kdx_types::Hash32::from_b64url(rk) else {
                    continue;
                };
                let chain = r.get("chain").and_then(Value::as_u64);
                let height = r.get("height").and_then(Value::as_u64);
                let hit = view.txs_by_rk(&rk)?.into_iter().find(|(k, _)| {
                    !k.is_coinbase()
                        && chain.is_none_or(|c| u64::from(k.chain()) == c)
                        && height.is_none_or(|h| u64::from(k.height()) == h)
                });
                if let Some((k, rec)) = hit {
                    if let Some(d) = load(view, k, Some(rec), &mut blocks)? {
                        rows.push(d);
                    }
                }
            }
            Ok((rows, p.next, p.truncated))
        })
        .await
        .map_err(from_api)?;
    if truncated {
        crate::flags::mark(ctx, crate::flags::Flag::SearchTruncated);
    }
    let edges: Vec<super::QueryTransactionsByPactCodeConnectionEdge> = rows
        .into_iter()
        .map(|d| super::QueryTransactionsByPactCodeConnectionEdge {
            cursor: d.cursor(0).encode(),
            node: TransactionSummary { d },
        })
        .collect();
    let page_info = crate::connection::PageInfo {
        start_cursor: edges.first().map(|e| e.cursor.clone()),
        end_cursor: resume
            .map(|c| c.encode())
            .or_else(|| edges.last().map(|e| e.cursor.clone())),
        has_next_page: resume.is_some(),
        has_previous_page: had_cursor,
    };
    Ok(super::QueryTransactionsByPactCodeConnection { edges, page_info })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kdx_api::compat::shapes::{self, Ev};
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

    #[test]
    fn search_permits_are_per_client_and_need_an_address() {
        use std::net::{IpAddr, Ipv4Addr};
        let gate = Arc::new(kdx_api::search::SearchGate::new(8, 1));
        let a = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
        let b = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1));
        let pa = search_permit(&gate, Some(a)).expect("first client");
        let _pb = search_permit(&gate, Some(b)).expect("a second client has its own bucket");
        assert!(
            search_permit(&gate, Some(a)).is_err(),
            "per-client limit holds"
        );
        assert!(search_permit(&gate, None).is_err(), "no address: refused");
        drop(pa);
        assert!(search_permit(&gate, Some(a)).is_ok(), "released on drop");
    }

    /// Wire formats of the deployed oracle (R-P5P9-19): `gasLimit`, `ttl`, `gas`, `transactionId`,
    /// `gasPrice` and `Transfer.amount` are strings, `Block.difficulty` a number above 2^53,
    /// `Block.nonce` a string, Event/Transfer/Signer ids carry string indexes.
    #[tokio::test]
    async fn oracle_wire_formats() {
        use crate::types::block::Block;
        use crate::types::event::Event;
        use crate::types::transfer::Transfer;
        use async_graphql::{EmptyMutation, EmptySubscription, Object, Schema};
        use base64::Engine;
        use kdx_types::record::BlockRecord;
        use kdx_types::{Hash32, TxKey};

        let block = Arc::new(BlockRecord {
            hash: Hash32([7; 32]),
            parent: Hash32([0; 32]),
            adjacents: Vec::new(),
            chain: 1,
            height: 5,
            weight: [0; 32],
            // difficulty = (2^256 - 1) / target = 2^56 + 1 or so: above 2^53.
            target: {
                let mut t = [0u8; 32];
                t[31 - 6] = 0x01; // little-endian 2^200 -> a target of 2^200
                t
            },
            nonce: u64::MAX,
            creation_time: 1_700_000_000_000_000,
            epoch_start: 1_700_000_000_000_000,
            feature_flags: 0,
            payload_hash: Hash32([0; 32]),
            transactions_hash: Hash32([0; 32]),
            outputs_hash: Hash32([0; 32]),
            chainweb_version: "mainnet01".into(),
            miner_account: 0,
            miner_data: Vec::new(),
            raw_miner: None,
            merkle_verified: true,
        });
        let d = Arc::new(TxData {
            rec: kdx_types::record::TxRecord {
                key: TxKey::new(5, 1, 0),
                request_key: Some(Hash32([9; 32])),
                ..Default::default()
            },
            block: block.clone(),
            out: json!({"gas": 236, "txId": 21663490}),
            cmd: Some(json!({"meta": {"chainId": "1", "gasLimit": 600, "ttl": 600,
                "gasPrice": 1.0e-8, "creationTime": 1.7e9, "sender": "k:a"},
                "signers": [{"pubKey": "ab"}]})),
            evs: vec![Ev {
                pos: 1,
                qual_name: "coin.TRANSFER".into(),
                module: "coin".into(),
                module_hash: String::new(),
                params: json!(["a", "b", 2.36e-6]),
            }],
            trs: vec![shapes::Transfer {
                pos: 0,
                token: "coin".into(),
                from: "a".into(),
                to: "b".into(),
                amount: "0.00000236".into(),
            }],
            nfts: Vec::new(),
        });
        struct Q(Arc<TxData>, Arc<BlockRecord>);
        #[Object]
        impl Q {
            async fn cmd(&self) -> TransactionCommand {
                TransactionCommand { d: self.0.clone() }
            }
            async fn result(&self) -> TransactionResult {
                TransactionResult { d: self.0.clone() }
            }
            async fn transfer(&self) -> Transfer {
                Transfer::new(self.0.clone(), 0, None)
            }
            async fn event(&self) -> Event {
                Event::new(self.0.clone(), 0)
            }
            async fn block(&self) -> Block {
                Block::new(self.1.clone())
            }
        }
        let schema = Schema::new(Q(d, block), EmptyMutation, EmptySubscription);
        let r = schema
            .execute(
                "{ cmd { meta { chainId gasLimit ttl gasPrice } signers { id } }
                   result { gas transactionId }
                   transfer { amount id }
                   event { id orderIndex }
                   block { difficulty nonce } }",
            )
            .await;
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        let v = serde_json::to_value(&r.data).unwrap();
        // Strings: gasLimit, ttl, gasPrice, gas, transactionId, Transfer.amount.
        assert_eq!(v["cmd"]["meta"]["gasLimit"], json!("600"));
        assert_eq!(v["cmd"]["meta"]["ttl"], json!("600"));
        assert_eq!(v["cmd"]["meta"]["gasPrice"], json!("0.00000001"));
        assert_eq!(v["result"]["gas"], json!("236"));
        assert_eq!(v["result"]["transactionId"], json!("21663490"));
        assert_eq!(v["transfer"]["amount"], json!("0.00000236"));
        // Numbers stay numbers: meta.chainId, Event.orderIndex.
        assert_eq!(v["cmd"]["meta"]["chainId"], json!(1));
        assert!(v["event"]["orderIndex"].is_number());
        // Block.difficulty: a number even above 2^53; nonce: a string.
        assert!(v["block"]["difficulty"].is_number(), "{v}");
        assert!(v["block"]["difficulty"].as_u64().unwrap() > (1 << 53));
        assert_eq!(v["block"]["nonce"], json!(u64::MAX.to_string()));
        // Ids: string indexes.
        let dec = |id: &Value| {
            String::from_utf8(
                base64::engine::general_purpose::STANDARD
                    .decode(id.as_str().unwrap())
                    .unwrap(),
            )
            .unwrap()
        };
        assert!(dec(&v["event"]["id"]).starts_with("Event:["), "{v}");
        assert!(
            dec(&v["event"]["id"]).contains(r#"","0","#),
            "{}",
            dec(&v["event"]["id"])
        );
        let t = dec(&v["transfer"]["id"]);
        assert!(t.contains(r#","1","0","#), "{t}");
        let sg = dec(&v["cmd"]["signers"][0]["id"]);
        assert!(sg.ends_with(r#","0"]"#), "{sg}");
    }
}

#[cfg(test)]
mod canonical_tests {
    use async_graphql::{EmptyMutation, EmptySubscription, Schema};
    use kdx_types::record::TxRecord;
    use kdx_types::TxKey;

    use super::*;
    use crate::testutil;
    use crate::GraphqlSettings;

    struct Q {
        d: Arc<TxData>,
    }

    #[Object]
    impl Q {
        async fn s(&self) -> TransactionSummary {
            TransactionSummary { d: self.d.clone() }
        }
    }

    /// M9: a summary is canonical only while its block is the stored one at its height.
    #[tokio::test]
    async fn summary_canonical_follows_the_stored_block() {
        let f = testutil::store();
        f.apply(0, 5, 1);
        for (fork, want) in [(1u8, true), (0, false)] {
            let d = Arc::new(TxData {
                rec: TxRecord {
                    key: TxKey::new(5, 0, 1),
                    ..TxRecord::default()
                },
                block: testutil::feed_block(0, 5, fork).block.clone(),
                out: Value::Null,
                cmd: None,
                evs: Vec::new(),
                trs: Vec::new(),
                nfts: Vec::new(),
            });
            let settings = GraphqlSettings::default();
            let schema = Schema::build(Q { d }, EmptyMutation, EmptySubscription)
                .data(crate::Gql {
                    state: Some(f.st.clone()),
                    ws: crate::ws::WsState::new(&settings),
                    settings,
                    feed: None,
                    search: None,
                })
                .finish();
            let r = schema.execute("{ s { canonical } }").await;
            assert!(r.errors.is_empty(), "{:?}", r.errors);
            let v = r.data.into_json().unwrap();
            assert_eq!(v["s"]["canonical"], want, "fork {fork}");
        }
    }
}

/// R-P5P9-57 (CG fix round): the served formats and values the gate's diff classes fixed.
#[cfg(test)]
mod cgfix_tests {
    use async_graphql::{EmptyMutation, EmptySubscription, Object, Schema};
    use base64::Engine;
    use kdx_api::compat::shapes::{self, Ev};
    use kdx_types::record::{BlockRecord, TxRecord};
    use kdx_types::{Hash32, TxKey};
    use serde_json::json;

    use super::*;
    use crate::types::block::Block;
    use crate::types::event::Event;
    use crate::types::transfer::Transfer;

    /// A mainnet header's `weight` and `target` (chain 4, h5801238): the node's base64url
    /// and the decimal the build before this fix served.
    const WEIGHT_B64: &str = "yjE9L07e77qFMAEAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const WEIGHT_DEC: &str = "1438066297983613385126346";
    const TARGET_B64: &str = "TBokDqB9G-OMpihJATl6slilqwREufm4FgAAAAAAAAA";
    const TARGET_DEC: &str = "142631823828797198123037655622276534200691233580490055490124";
    /// A coinbase's `reqKey` as the node sends it (h5801243 c4, Pact 5: base64url of the
    /// parent hash text) and as the oracle lists it.
    const CB_RK: &str = "aVdla04wempYOThOVHN6UXlPV3hXRmM1azVzODc1SDYtYmpSTFdNOUgyMA";

    fn le32(b64: &str) -> [u8; 32] {
        kdx_types::hash::b64url_decode(b64)
            .unwrap()
            .try_into()
            .unwrap()
    }

    fn block() -> Arc<BlockRecord> {
        Arc::new(BlockRecord {
            hash: Hash32([7; 32]),
            chain: 4,
            height: 5_801_243,
            weight: le32(WEIGHT_B64),
            target: le32(TARGET_B64),
            creation_time: 1_746_518_323_432_000,
            ..BlockRecord::default()
        })
    }

    fn tx(pos: u16, out: Value, cmd: Option<Value>, params: Value) -> Arc<TxData> {
        let rec = TxRecord {
            key: TxKey::new(5_801_243, 4, pos),
            request_key: (pos > 0).then_some(Hash32([9; 32])),
            ..Default::default()
        };
        Arc::new(TxData {
            rec,
            block: block(),
            out,
            cmd,
            evs: vec![Ev {
                pos: 0,
                qual_name: "coin.TRANSFER".into(),
                module: "coin".into(),
                module_hash: "mh".into(),
                params,
            }],
            trs: vec![shapes::Transfer {
                pos: 0,
                token: "coin".into(),
                from: "a".into(),
                to: "b".into(),
                amount: "26.57382".into(),
            }],
            nfts: Vec::new(),
        })
    }

    struct Q(Arc<TxData>);
    #[Object]
    impl Q {
        async fn cmd(&self) -> TransactionCommand {
            TransactionCommand { d: self.0.clone() }
        }
        async fn transfer(&self) -> Transfer {
            Transfer::new(self.0.clone(), 0, None)
        }
        async fn event(&self) -> Event {
            Event::new(self.0.clone(), 0)
        }
        async fn block(&self) -> Block {
            Block::new(self.0.block.clone())
        }
        async fn tx(&self) -> Option<Transaction> {
            Transaction::new(self.0.clone())
        }
        async fn guard(&self) -> Sel<crate::types::IGuard> {
            Sel(crate::types::guard::map_guard(
                &json!({"fun": "f", "args": ["a"]}),
                None,
            ))
        }
        async fn no_guard(&self) -> Option<Sel<crate::types::IGuard>> {
            None
        }
    }

    async fn run(d: Arc<TxData>, q: &str) -> Value {
        let r = Schema::new(Q(d), EmptyMutation, EmptySubscription)
            .execute(q)
            .await;
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        serde_json::to_value(&r.data).unwrap()
    }

    fn id_text(v: &Value) -> String {
        let b = base64::engine::general_purpose::STANDARD
            .decode(v.as_str().unwrap())
            .unwrap();
        String::from_utf8(b).unwrap()
    }

    /// F1, F3, F5, F6, F8, F9 and `Transfer.amount` on a user tx.
    #[tokio::test]
    async fn user_tx_formats() {
        let cmd = json!({
            "nonce": "\"2024-06-11T12:52:52.004Z\"",
            "meta": {"chainId": "4", "creationTime": 1718110252, "sender": "a"},
            "payload": {"exec": {"code": "(coin.transfer \"a\" \"b\" 1.0)\n", "data": {}}},
            "signers": [{"pubKey": "ab"}, {"pubKey": "cd", "scheme": "ED25519"}]
        });
        let params: Value =
            serde_json::from_str(r#"["a","b",26.57382000000000000000000000,2.36e-6,1.0e-7]"#)
                .unwrap();
        let d = tx(
            1,
            json!({"result": {"status": "success", "data": ""}}),
            Some(cmd),
            params,
        );
        let v = run(
            d,
            "{ cmd { nonce payload { ... on ExecutionPayload { code } } signers { scheme } }
               transfer { creationTime amount requestKey }
               event { parameterText parameters requestKey }
               block { weight target } }",
        )
        .await;
        // F1: the node's base64url, the same numbers as the old decimals.
        assert_eq!(v["block"]["weight"], json!(WEIGHT_B64));
        assert_eq!(v["block"]["target"], json!(TARGET_B64));
        assert_eq!(
            kdx_decode::header::le256_decimal(&le32(WEIGHT_B64)),
            WEIGHT_DEC
        );
        assert_eq!(
            kdx_decode::header::le256_decimal(&le32(TARGET_B64)),
            TARGET_DEC
        );
        // F3: the tx's meta creationTime (whole seconds), not the block time.
        assert_eq!(
            v["transfer"]["creationTime"],
            json!("2024-06-11T12:50:52.000Z")
        );
        // F5 (rule a): the nonce is the command's string as the node holds it.
        assert_eq!(v["cmd"]["nonce"], json!("\"2024-06-11T12:52:52.004Z\""));
        // F6: the code JSON-quoted, as the oracle sends it.
        assert_eq!(
            v["cmd"]["payload"]["code"],
            json!(r#""(coin.transfer \"a\" \"b\" 1.0)\n""#)
        );
        // F8: "" when the signer names no scheme; a named scheme is kept (rule a).
        assert_eq!(
            v["cmd"]["signers"],
            json!([{"scheme": ""}, {"scheme": "ED25519"}])
        );
        // F9: parameterText in the oracle's number layout; parameters keeps the node's.
        assert_eq!(
            v["event"]["parameterText"],
            json!(r#"["a","b",26.57382,0.00000236,1e-7]"#)
        );
        assert_eq!(
            v["event"]["parameters"],
            json!(r#"["a","b",26.57382000000000000000000000,2.36e-6,1.0e-7]"#)
        );
        // Transfer.amount without trailing zeros.
        assert_eq!(v["transfer"]["amount"], json!("26.57382"));
    }

    /// F4: coinbase rows carry the node's coinbase reqKey, and their ids follow it.
    #[tokio::test]
    async fn coinbase_rows_carry_the_node_req_key() {
        let out = json!({"reqKey": CB_RK, "result": {"status": "success", "data": "ok"}});
        let d = tx(0, out, None, json!(["", "miner", 1.0]));
        let v = run(
            d,
            "{ transfer { requestKey id creationTime } event { requestKey id } }",
        )
        .await;
        assert_eq!(v["transfer"]["requestKey"], json!(CB_RK));
        assert_eq!(v["event"]["requestKey"], json!(CB_RK));
        let t = id_text(&v["transfer"]["id"]);
        assert!(t.ends_with(&format!(r#","{CB_RK}"]"#)), "{t}");
        let e = id_text(&v["event"]["id"]);
        assert!(e.ends_with(&format!(r#","0","{CB_RK}"]"#)), "{e}");
        // No command: the block's time.
        assert_eq!(
            v["transfer"]["creationTime"],
            json!("2025-05-06T07:58:43.432Z")
        );
        // An output without a reqKey keeps compat's text.
        let d = tx(0, json!({}), None, json!(["", "miner", 1.0]));
        let v = run(d, "{ transfer { requestKey } }").await;
        assert_eq!(v["transfer"]["requestKey"], json!("<coinbase>"));
    }

    /// F10 `cmd.payload` null vs `{}` (rule b): a union or interface whose fragments select
    /// none of its fields answers `{}`, as the oracle does; a matching fragment is unchanged
    /// and a missing value stays `null`.
    #[tokio::test]
    async fn unmatched_fragments_answer_an_empty_object() {
        let cont = json!({"payload": {"cont": {"pactId": "p", "step": 1, "rollback": false,
            "data": {}, "proof": null}}});
        let out = json!({"result": {"status": "success", "data": ""}});
        let d = tx(1, out, Some(cont), json!([]));
        let v = run(
            d.clone(),
            "{ cmd { payload { ... on ExecutionPayload { code } } }
               tx { result { ... on TransactionMempoolInfo { status } } }
               guard { ... on KeysetGuard { keys } }
               noGuard { ... on KeysetGuard { keys } } }",
        )
        .await;
        assert_eq!(v["cmd"]["payload"], json!({}));
        assert_eq!(v["tx"]["result"], json!({}));
        assert_eq!(v["guard"], json!({}));
        assert_eq!(v["noGuard"], Value::Null);
        let v = run(
            d,
            "{ cmd { payload { __typename ... on ContinuationPayload { pactId step } } }
               tx { result { __typename } }
               guard { __typename ... on UserGuard { fun } } }",
        )
        .await;
        assert_eq!(
            v["cmd"]["payload"],
            json!({"__typename": "ContinuationPayload", "pactId": "p", "step": 1})
        );
        assert_eq!(
            v["tx"]["result"],
            json!({"__typename": "TransactionResult"})
        );
        assert_eq!(v["guard"], json!({"__typename": "UserGuard", "fun": "f"}));
    }

    /// M1 (GQ-19): `transactions(accountName)` keeps only the account's own txs.
    #[test]
    fn account_transactions_are_sender_rows() {
        let spec = transactions_spec(Some("k:a"), None, Some("2"), None, (None, None, None), None)
            .unwrap();
        let Rows::Txs { sender, nft } = &spec.rows else {
            panic!("tx rows")
        };
        assert_eq!((sender.as_deref(), *nft), (Some("k:a"), false));
        let with_token = transactions_spec(
            Some("k:a"),
            None,
            None,
            Some("coin"),
            (None, None, None),
            None,
        )
        .unwrap();
        assert!(matches!(&with_token.rows, Rows::Txs { sender: Some(s), .. } if s == "k:a"));
        let by_block = transactions_spec(
            None,
            Some("lYOEd2WyPiTx_bfiTIbwRH2N4beFtFtQuNRZA2umjyo"),
            None,
            None,
            (None, None, None),
            None,
        )
        .unwrap();
        assert!(matches!(&by_block.rows, Rows::Txs { sender: None, .. }));
        // The row filter: a tx another account sent is dropped, the account's own is kept.
        let mine = tx(
            1,
            json!({}),
            Some(json!({"meta": {"sender": "k:a"}})),
            json!([]),
        );
        let other = tx(
            1,
            json!({}),
            Some(json!({"meta": {"sender": "k:b"}})),
            json!([]),
        );
        assert_eq!(spec.rows.rows(&mine).len(), 1);
        assert!(spec.rows.rows(&other).is_empty());
    }
}
