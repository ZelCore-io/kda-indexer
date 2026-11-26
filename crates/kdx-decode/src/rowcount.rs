//! The list rows one tx adds to the stored per-key counts (`kdx_hot::rowcount`, PR #48 part 2).
//!
//! A GraphQL list walks one posting list and turns each tx into rows (`kdx-graphql`
//! `txlist::Rows::rows`), so its exact `totalCount` is the sum, over the txs **posted under
//! the key**, of the rows each tx yields. [`tx_counts`] computes that sum's terms for one
//! record:
//!
//! - which keys the tx is posted under, by the index's own rules (`index::extract`:
//!   [`parse_event`], [`transfer_accounts`], [`event_accounts`], the sender and the signers);
//! - how many rows the tx yields on each such key, by the list's row rule over the rendered
//!   output ([`crate::rows::events`] and [`crate::rows::transfers`], the code the walk runs).
//!
//! So a stored count equals the walk by construction, edge cases included: a `WIZ_TRANSFER`
//! row counts for an account only when the tx is in the account's `acct` postings for another
//! reason, a transfer whose amount `show_scientific` cannot render has its posting and no row.
//! The one input the record lacks is the block's miner account (an `acct` posting of the
//! coinbase); the coinbase's own `coin.TRANSFER` names the miner, so no row is lost.

use std::collections::BTreeSet;

use kdx_types::record::TxRecord;
use kdx_types::{Dict, DictKind};
use serde_json::Value;

use crate::build::{account_ok, event_accounts, parse_event, transfer_accounts};
use crate::index::xfer_entity;
use crate::json::parse_untrusted;
use crate::render::{render_output, RenderError};
use crate::rows::{events, transfers};

/// What a stored count counts. The discriminant is the first key byte of `RowCounts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CountKind {
    /// Transfer rows with the account on a side, in txs with its `acct` posting
    /// (`transfers(accountName)`). Entity: AccountId.
    AcctXfer = 0,
    /// Transfer rows of the module with the account on a side, in txs with the `xfer`
    /// posting (`transfers(accountName, fungibleName)`, `FungibleChainAccount.transfers`).
    /// Entity: `xfer_entity(account, module)`.
    Xfer = 1,
    /// Events of the name, in txs with its `evt` posting (`events(qualifiedEventName)`).
    /// Entity: EventNameId.
    EvtRows = 2,
    /// Txs with the `evt` posting (`transactions(fungibleName)`, the `<m>.TRANSFER` key).
    /// Entity: EventNameId.
    EvtTxs = 3,
    /// Transfer rows of module `m` in txs with the `<m>.TRANSFER` `evt` posting
    /// (`transfers(fungibleName)`). Entity: EventNameId of `<m>.TRANSFER`.
    TokenXfer = 4,
    /// Txs with the `sndr` posting (`transactions(accountName)`). Entity: AccountId.
    Sndr = 5,
    /// Txs with the `pk` posting (`transactionsByPublicKey`). Entity: PubKeyId.
    Pk = 6,
}

impl CountKind {
    pub const ALL: [CountKind; 7] = [
        CountKind::AcctXfer,
        CountKind::Xfer,
        CountKind::EvtRows,
        CountKind::EvtTxs,
        CountKind::TokenXfer,
        CountKind::Sndr,
        CountKind::Pk,
    ];
    pub fn from_u8(b: u8) -> Option<CountKind> {
        CountKind::ALL.get(usize::from(b)).copied()
    }
    pub fn name(self) -> &'static str {
        match self {
            CountKind::AcctXfer => "acct_xfer",
            CountKind::Xfer => "xfer",
            CountKind::EvtRows => "evt_rows",
            CountKind::EvtTxs => "evt_txs",
            CountKind::TokenXfer => "token_xfer",
            CountKind::Sndr => "sndr",
            CountKind::Pk => "pk",
        }
    }
}

/// One term: `rows` rows of `kind` under `entity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowTerm {
    pub kind: CountKind,
    pub entity: u64,
    pub rows: u64,
}

/// The rows `r` adds to each stored count (zero terms left out), in a stable order. Names
/// the dictionary lacks have no posting, so they get no term. An output that does not render
/// is an error: the walk cannot list that tx either.
pub fn tx_counts(r: &TxRecord, dict: &dyn Dict) -> Result<Vec<RowTerm>, RenderError> {
    let out = render_output(r, dict)?;
    let evs = events(&out);
    let trs = transfers(&evs);

    // Posting keys, as `index::extract` derives them.
    let mut acct: BTreeSet<&str> = BTreeSet::new();
    let mut xfer: BTreeSet<(&str, String)> = BTreeSet::new();
    let mut evt: BTreeSet<String> = BTreeSet::new();
    if let Some(raw) = out.get("events").and_then(Value::as_array) {
        for e in raw {
            if let Some(ev) = parse_event(e) {
                if let Some((from, to)) = transfer_accounts(&ev) {
                    for a in [from, to] {
                        if account_ok(a) {
                            xfer.insert((a, ev.module.clone()));
                        }
                    }
                }
                evt.insert(ev.event_name);
            }
            let mut v = Vec::new();
            event_accounts(e, &mut v);
            acct.extend(v.into_iter().filter(|a| account_ok(a)));
        }
    }
    let cmd = match (&r.cmd, r.key.pos() == 0 || r.cmd_unparsed) {
        (Some(c), false) => parse_untrusted(c).ok(),
        _ => None,
    };
    let mut sender = None;
    let mut pks: BTreeSet<&str> = BTreeSet::new();
    if let Some(c) = &cmd {
        if let Some(s) = crate::index::sender_of(c).filter(|s| account_ok(s)) {
            sender = Some(s);
            acct.insert(s);
        }
        for s in c
            .get("signers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(pk) = s.get("pubKey").and_then(Value::as_str) {
                pks.insert(pk);
            }
            if let Some(a) = s
                .get("addr")
                .and_then(Value::as_str)
                .filter(|a| account_ok(a))
            {
                acct.insert(a);
            }
        }
    }

    let mut t = Vec::new();
    let mut push = |kind, entity, rows: usize| {
        if rows > 0 {
            t.push(RowTerm {
                kind,
                entity,
                rows: rows as u64,
            });
        }
    };
    let side = |a: &str, x: &crate::rows::Transfer| x.from == a || x.to == a;
    for a in &acct {
        if let Some(id) = dict.find(DictKind::Account, a) {
            push(
                CountKind::AcctXfer,
                u64::from(id),
                trs.iter().filter(|x| side(a, x)).count(),
            );
        }
    }
    for (a, m) in &xfer {
        if let (Some(ai), Some(mi)) = (
            dict.find(DictKind::Account, a),
            dict.find(DictKind::Module, m),
        ) {
            push(
                CountKind::Xfer,
                xfer_entity(ai, mi),
                trs.iter().filter(|x| x.token == *m && side(a, x)).count(),
            );
        }
    }
    for q in &evt {
        let Some(id) = dict.find(DictKind::EventName, q) else {
            continue;
        };
        let id = u64::from(id);
        push(
            CountKind::EvtRows,
            id,
            evs.iter().filter(|e| e.qual_name == *q).count(),
        );
        push(CountKind::EvtTxs, id, 1);
        if let Some(m) = q.strip_suffix(".TRANSFER") {
            push(
                CountKind::TokenXfer,
                id,
                trs.iter().filter(|x| x.token == m).count(),
            );
        }
    }
    if let Some(id) = sender.and_then(|s| dict.find(DictKind::Account, s)) {
        push(CountKind::Sndr, u64::from(id), 1);
    }
    for pk in &pks {
        if let Some(id) = dict.find(DictKind::PubKey, pk) {
            push(CountKind::Pk, u64::from(id), 1);
        }
    }
    t.sort_unstable();
    Ok(t)
}
