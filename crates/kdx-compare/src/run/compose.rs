//! R-P5P9-86 on the REST oracle pairs (account against chainweb-data, /v1/txs against
//! kda-txs): the rules combined row by row, and the /v1/txs recent-window edge. Each has a
//! stored-record form for `kda-compare referee` (R-P5P9-87, [`Ctx::rest_stored`]).

use super::*;
use crate::config::TargetKind;

/// R-P5P9-86: the record's one-sided rows, each explained by one rule; lists each rule and
/// its row count (allowlist `rest-compose`).
pub(crate) const REST_COMPOSE_PATH: &str = "/~rest-compose";
/// A coinbase row only ours lists, which the node's canonical block pays the account.
pub(crate) const CWD_MISSING_COINBASE_PATH: &str = "/~cwd-missing-node-coinbase";
/// Oracle-only rows at or below ours' lowest height while ours held its limit.
pub(crate) const OURS_LIMIT_EDGE_PATH: &str = "/~ours-limit-edge";
/// R-P5P9-86 v1txs: kda-txs behind at the recent-window edge.
pub(crate) const KDATXS_EDGE_LAG_PATH: &str = "/~kdatxs-edge-lag";

/// The v1txs edge's re-read delay (R-P5P9-86: 60 s).
#[cfg(not(test))]
const V1TXS_EDGE_REREAD: Duration = Duration::from_secs(60);
#[cfg(test)]
const V1TXS_EDGE_REREAD: Duration = Duration::from_millis(10);

fn max_height(v: &Value) -> Option<u64> {
    v.as_array()?.iter().filter_map(row_height).max()
}

/// R-P5P9-86 v1txs: the two pages do not overlap in height and the oracle's rows end lower
/// (both sides hold rows).
pub(crate) fn edge_shape(va: &Value, vb: &Value) -> bool {
    matches!((min_height(va), max_height(vb)), (Some(lo_a), Some(hi_b)) if hi_b < lo_a)
}

fn remove_rows(v: &mut Value, rows: &[Value]) {
    if let Value::Array(x) = v {
        for r in rows {
            if let Some(i) = x.iter().position(|y| y == r) {
                x.remove(i);
            }
        }
    }
}

fn first_keys(rows: &[Value]) -> Vec<Value> {
    rows.iter()
        .take(10)
        .map(|r| json!([r.get("chain"), r.get("height"), r.get("requestKey")]))
        .collect()
}

/// Two reads of one /v1/txs pair compared on their height overlap, as the compare does
/// (normalized, numbers at f64 precision, a tx's events as a multiset). `Err`: no overlap,
/// or a difference there.
fn overlap_agrees(route: &RouteKind, a: &Value, b: &Value) -> Result<usize, String> {
    let (mut a, mut b) = (a.clone(), b.clone());
    normalize(route, &mut a);
    normalize(route, &mut b);
    // Heights are read before the f64 numbers (a height then reads as a float).
    let (Some(lo), Some(hi)) = (
        min_height(&a).max(min_height(&b)),
        max_height(&a).min(max_height(&b)),
    ) else {
        return Err("a side lists no row".into());
    };
    if lo > hi {
        return Err(format!("the reads do not overlap in height ({lo} > {hi})"));
    }
    for v in [&mut a, &mut b] {
        if let Value::Array(x) = v {
            x.retain(|r| row_height(r).is_some_and(|h| (lo..=hi).contains(&h)));
        }
    }
    numbers_as_f64(&mut a);
    numbers_as_f64(&mut b);
    events_as_multiset(&mut a, &mut b);
    let d = diff_route(route, &a, &b);
    if d.is_empty() {
        Ok(len_of(&a))
    } else {
        Err(format!(
            "the reads differ on the overlap {lo}..={hi} ({} entries)",
            d.len()
        ))
    }
}

impl Ctx {
    /// R-P5P9-86 (REST account records where several rules apply): every one-sided row of
    /// `va` (ours) and `vb` (chainweb-data or kda-txs) is explained by exactly one rule's own
    /// check, or nothing moves (`Err`, with why):
    /// - a user row only ours lists: in ours' block on the node's `/poll`, with an event
    ///   naming the account (cwd-missing-node / kdatxs-missing-node);
    /// - a coinbase row only ours lists: the node's canonical block at its height is the
    ///   row's and pays the account the row's amount (cwd-missing-node-coinbase);
    /// - a row only the oracle lists: at or below ours' lowest height while ours held its
    ///   limit (`full_a`; ours-limit-edge). `lowest_a` is the lowest height of ours' page as
    ///   fetched, before any rule dropped rows: a row a rule cut from ours' bottom never
    ///   makes an oracle-only row above it ours' limit.
    ///
    /// Those rows leave both sides; shared rows' field differences stay for their own rules.
    /// Returns the hits, with `/~rest-compose` listing each rule and its rows.
    pub(crate) async fn rest_compose(
        &self,
        req: &Request,
        kdatxs: bool,
        va: &mut Value,
        vb: &mut Value,
        full_a: bool,
        lowest_a: Option<u64>,
    ) -> Result<Vec<DiffEntry>, String> {
        let only_a = only_keys_in_first(va, vb);
        let only_b = only_keys_in_first(vb, va);
        if only_a.is_empty() && only_b.is_empty() {
            return Err("no one-sided row".into());
        }
        let lowest = lowest_a;
        for r in &only_b {
            let h = row_height(r);
            if !(full_a && matches!((h, lowest), (Some(h), Some(m)) if h <= m)) {
                return Err(format!(
                    "an oracle-only row at height {h:?} is not at or below ours' lowest \
                     {lowest:?} with ours' limit held ({full_a})"
                ));
            }
        }
        if only_a.len() > NODE_GAP_CHECKS {
            return Err(format!(
                "{} rows only ours lists, over {NODE_GAP_CHECKS}",
                only_a.len()
            ));
        }
        let account = query_account(req);
        if account.is_empty() && !only_a.is_empty() {
            return Err("no account in the request".into());
        }
        let (cbs, users): (Vec<Value>, Vec<Value>) = only_a
            .iter()
            .cloned()
            .partition(|r| r.get("requestKey").and_then(Value::as_str) == Some("<coinbase>"));
        if !users.is_empty() && !self.node_confirms(req, &users, &account).await {
            return Err(
                "the node does not put every user row only ours lists in its block with an \
                 event naming the account"
                    .into(),
            );
        }
        if kdatxs && !cbs.is_empty() {
            return Err("a coinbase row only ours lists on /v1/txs".into());
        }
        for r in &cbs {
            self.node_pays_coinbase(req, r, &account).await?;
        }
        remove_rows(va, &only_a);
        remove_rows(vb, &only_b);
        let node_path = if kdatxs {
            KDATXS_MISSING_NODE_PATH
        } else {
            CWD_MISSING_NODE_PATH
        };
        let mut rules = serde_json::Map::new();
        let mut out = Vec::new();
        let mut add = |path: &str, id: &str, rows: &[Value], kind: DiffKind, ours: bool| {
            if rows.is_empty() {
                return;
            }
            rules.insert(id.into(), json!(rows.len()));
            let ev = Some(
                json!({"rows": rows.len(), "window_cut": 0, "first": first_keys(rows),
                "lowest": lowest}),
            );
            out.push(DiffEntry {
                path: path.into(),
                kind,
                a: if ours { ev.clone() } else { None },
                b: if ours { None } else { ev },
            });
        };
        add(
            node_path,
            node_path.trim_start_matches("/~"),
            &users,
            DiffKind::Extra,
            true,
        );
        add(
            CWD_MISSING_COINBASE_PATH,
            "cwd-missing-node-coinbase",
            &cbs,
            DiffKind::Extra,
            true,
        );
        add(
            OURS_LIMIT_EDGE_PATH,
            "ours-limit-edge",
            &only_b,
            DiffKind::Missing,
            false,
        );
        out.push(DiffEntry {
            path: REST_COMPOSE_PATH.into(),
            kind: DiffKind::Changed,
            a: Some(json!({ "rules": rules })),
            b: None,
        });
        Ok(out)
    }

    /// A coinbase row only ours lists: the node's canonical block at its (chain, height), on
    /// the cut's branch, is the row's, and its coinbase output pays `account` exactly the
    /// row's amount (two node reads, within the run's cap).
    async fn node_pays_coinbase(
        &self,
        req: &Request,
        r: &Value,
        account: &str,
    ) -> Result<(), String> {
        let (Some(c), Some(h), Some(bh)) = (
            row_chain(r),
            row_height(r),
            r.get("blockHash").and_then(Value::as_str),
        ) else {
            return Err("a coinbase row without its chain, height or block".into());
        };
        if !self.gql.take_node_read() {
            return Err("the node read cap is reached".into());
        }
        self.count_request(req.route.as_str(), "node", 1);
        let hd = canonical_header(&self.node, &self.tips, c, h)
            .await
            .map_err(|e| format!("header read failed: {e:#}"))?
            .ok_or_else(|| format!("no canonical header at chain {c} height {h}"))?;
        if hd.get("hash").and_then(Value::as_str) != Some(bh) {
            return Err(format!(
                "the node's canonical block at chain {c} height {h} is not the row's {bh}"
            ));
        }
        let ph = hd
            .get("payloadHash")
            .and_then(Value::as_str)
            .ok_or("a header without payloadHash")?;
        if !self.gql.take_node_read() {
            return Err("the node read cap is reached".into());
        }
        self.count_request(req.route.as_str(), "node", 1);
        let payload = self
            .node
            .payload_with_outputs(c, ph)
            .await
            .map_err(|e| format!("payload read failed: {e:#}"))?;
        let paid = crate::graphql_window::coinbase_paid(&payload, account).ok_or_else(|| {
            format!("the coinbase at chain {c} height {h} does not pay {account}")
        })?;
        let amount = match r.get("amount") {
            Some(Value::String(s)) => crate::rewards::fixed(s),
            Some(Value::Number(n)) => crate::rewards::fixed(&n.to_string()),
            _ => None,
        }
        .ok_or("the row's amount does not read")?;
        if paid != amount {
            return Err(format!(
                "the coinbase at chain {c} height {h} pays another amount"
            ));
        }
        Ok(())
    }

    /// Every row is node-canonical: the node's `/poll` puts its request key in its block, and
    /// that block is the node's canonical one at its (chain, height) on the cut's branch.
    async fn rows_node_canonical(&self, req: &Request, rows: &[&Value]) -> Result<(), String> {
        let s = |r: &Value, k: &str| r.get(k).and_then(Value::as_str).map(str::to_string);
        let mut by_chain: HashMap<u8, Vec<(String, String, u64)>> = HashMap::new();
        for r in rows {
            let (Some(rk), Some(bh), Some(c), Some(h)) = (
                s(r, "requestKey"),
                s(r, "blockHash"),
                row_chain(r),
                row_height(r),
            ) else {
                return Err("a row without its request key, block, chain or height".into());
            };
            by_chain.entry(c).or_default().push((rk, bh, h));
        }
        if rows.len() > NODE_GAP_CHECKS {
            return Err(format!("{} rows, over {NODE_GAP_CHECKS}", rows.len()));
        }
        let (mut cache, mut checks) = (HashMap::new(), 0usize);
        for (c, keys) in by_chain {
            for chunk in keys.chunks(NODE_POLL_CHUNK) {
                if !self.gql.take_node_read() {
                    return Err("the node read cap is reached".into());
                }
                self.count_request(req.route.as_str(), "node", 1);
                let rks: Vec<String> = chunk.iter().map(|k| k.0.clone()).collect();
                let res = self
                    .node
                    .poll(c, &rks)
                    .await
                    .map_err(|e| format!("/poll failed: {e:#}"))?;
                for (rk, bh, h) in chunk {
                    let placed = res
                        .get(rk)
                        .and_then(|v| v.pointer("/metaData/blockHash"))
                        .and_then(Value::as_str);
                    if placed != Some(bh.as_str()) {
                        return Err(format!("the node does not put {rk} in block {bh}"));
                    }
                    let canon = self.canon_cached(&mut cache, &mut checks, c, *h).await;
                    if canon.flatten().as_deref() != Some(bh.as_str()) {
                        return Err(format!("block {bh} is not canonical on the node"));
                    }
                }
            }
        }
        Ok(())
    }

    /// R-P5P9-86 v1txs, live: the pages do not overlap and kda-txs' rows end lower, so both
    /// sides are read once more after 60 s. Oracle lag when the re-read agrees on its
    /// overlap, or shows the same shape with kda-txs further on and every row of the first
    /// reads (`va`, `vb`) node-canonical. `Err`: why not.
    pub(crate) async fn v1txs_edge_live(
        &self,
        req: &Request,
        ta: &Target,
        tb: &Target,
        va: &Value,
        vb: &Value,
    ) -> Result<Value, String> {
        tokio::time::sleep(V1TXS_EDGE_REREAD).await;
        let read = |p: anyhow::Result<Paged>| -> Result<Value, String> {
            let p = p.map_err(|e| format!("the re-read failed: {e:#}"))?;
            if p.pages.first().map(|c| c.status) != Some(200) {
                return Err("the re-read did not answer 200".into());
            }
            Ok(p.items)
        };
        let ra = read(self.fetch_a(req, ta).await)?;
        let rb = read(self.fetch_a(req, tb).await)?;
        match overlap_agrees(&req.route, &ra, &rb) {
            Ok(n) => Ok(json!({"reread": "agrees on the overlap", "rows": n})),
            Err(why) => {
                let further = max_height(&rb) > max_height(vb);
                if !(edge_shape(&ra, &rb) && further) {
                    return Err(format!("{why}; nor the same shape with kda-txs further on"));
                }
                let rows: Vec<&Value> = [va, vb]
                    .into_iter()
                    .filter_map(Value::as_array)
                    .flatten()
                    .collect();
                self.rows_node_canonical(req, &rows).await?;
                Ok(json!({"reread": "the same shape, kda-txs further on",
                    "oracleTop": [max_height(vb), max_height(&rb)], "nodeCanonical": rows.len()}))
            }
        }
    }

    /// R-P5P9-86 v1txs with R-P5P9-87, the stored form: every stored row of both sides is
    /// node-canonical, and a read of both sides now agrees on its overlap.
    async fn v1txs_edge_stored(
        &self,
        req: &Request,
        ta: &Target,
        tb: &Target,
        va: &Value,
        vb: &Value,
    ) -> Result<Value, String> {
        let rows: Vec<&Value> = [va, vb]
            .into_iter()
            .filter_map(Value::as_array)
            .flatten()
            .collect();
        self.rows_node_canonical(req, &rows).await?;
        let read = |p: anyhow::Result<Paged>| -> Result<Value, String> {
            let p = p.map_err(|e| format!("the read now failed: {e:#}"))?;
            if p.pages.first().map(|c| c.status) != Some(200) {
                return Err("the read now did not answer 200".into());
            }
            Ok(p.items)
        };
        let ra = read(self.fetch_a(req, ta).await)?;
        let rb = read(self.fetch_a(req, tb).await)?;
        let n = overlap_agrees(&req.route, &ra, &rb)?;
        Ok(
            json!({"stored": "every row node-canonical", "nodeCanonical": rows.len(),
            "readNow": "agrees on the overlap", "rows": n}),
        )
    }

    /// R-P5P9-86 with R-P5P9-87: a stored REST record (an account pair against
    /// chainweb-data, a /v1/txs pair against kda-txs) settled by its stored form. A /v1/txs
    /// record of the edge shape is settled by [`Ctx::v1txs_edge_stored`]; any other record by
    /// the stored rows: normalized, the node's orphans moved out, the rows combined by
    /// [`Ctx::rest_compose`] (ours full when it holds the request's limit), the rest diffed
    /// and classified. There is no live window drop: a recent row the oracle lacks must pass
    /// the node check. Returns the unexplained entries, the explaining ids and a note;
    /// `Err` when the record has no stored form.
    pub(crate) async fn rest_stored(
        &self,
        rec: &DiffRecord,
    ) -> Result<(Vec<DiffEntry>, Vec<String>, Option<String>), String> {
        if rec.body_cap.is_some() {
            return Err("a body was cut by the log's cap".into());
        }
        let route = match rec.route.as_str() {
            "account" => RouteKind::Account,
            "v1txs" => RouteKind::V1Txs,
            r => return Err(format!("route {r} has no stored form")),
        };
        let (Some(ta), Some(tb)) = (
            self.targets.get(&rec.a_target),
            self.targets.get(&rec.b_target),
        ) else {
            return Err("a target is not configured here".into());
        };
        let kdatxs = tb.kind == TargetKind::KdaTxs;
        if !matches!(
            (route, tb.kind),
            (RouteKind::Account, TargetKind::ChainwebData) | (RouteKind::V1Txs, TargetKind::KdaTxs)
        ) {
            return Err(format!(
                "B target {} is not this route's oracle (chainweb-data or kda-txs)",
                rec.b_target
            ));
        }
        if rec.a_status != Some(200) || rec.b_status != Some(200) {
            return Err("a side did not answer 200".into());
        }
        let parse = |t: &str| {
            serde_json::from_str::<Value>(t)
                .ok()
                .filter(Value::is_array)
                .ok_or("a stored body is not a row list")
        };
        let (raw_a, raw_b) = (parse(&rec.a_body_trimmed)?, parse(&rec.b_body_trimmed)?);
        let req = Request {
            route,
            path_and_query: rec.request.clone(),
            origin: crate::gen::Origin::Replay,
            oracle: None,
        };
        let classify = |d: &[DiffEntry]| self.allow.classify_request(&route, &rec.request, &[], d);
        let mut why = Vec::new();
        if route == RouteKind::V1Txs && edge_shape(&raw_a, &raw_b) {
            match self.v1txs_edge_stored(&req, ta, tb, &raw_a, &raw_b).await {
                Ok(ev) => {
                    let d = [DiffEntry {
                        path: KDATXS_EDGE_LAG_PATH.into(),
                        kind: DiffKind::Changed,
                        a: Some(ev),
                        b: None,
                    }];
                    let (un, ids) = classify(&d);
                    return Ok((un, ids, None));
                }
                Err(w) => why.push(format!("v1txs edge: {w}")),
            }
        }
        let (mut va, mut vb) = (raw_a.clone(), raw_b.clone());
        normalize(&route, &mut va);
        normalize(&route, &mut vb);
        let orphans = self.oracle_orphans(&mut va, &mut vb).await;
        let limit = if route == RouteKind::V1Txs {
            crate::via_cwd::parse(&rec.request)
                .limit
                .ok()
                .map(|l| l as usize)
        } else {
            query_limit(&rec.request)
        };
        let full_a = limit.is_some_and(|l| len_of(&raw_a) >= l);
        let lowest = min_height(&raw_a);
        let mut d = match self
            .rest_compose(&req, kdatxs, &mut va, &mut vb, full_a, lowest)
            .await
        {
            Ok(h) => h,
            Err(w) => {
                why.push(format!("rows: {w}"));
                Vec::new()
            }
        };
        if kdatxs {
            numbers_as_f64(&mut va);
            numbers_as_f64(&mut vb);
        }
        let order = if route == RouteKind::V1Txs {
            events_as_multiset(&mut va, &mut vb)
        } else {
            0
        };
        d.extend(diff_route(&route, &va, &vb));
        if order > 0 {
            d.push(DiffEntry {
                path: EVENT_ORDER_PATH.into(),
                kind: DiffKind::Changed,
                a: Some(json!(order)),
                b: Some(json!(order)),
            });
        }
        if !orphans.is_empty() {
            d.push(DiffEntry {
                path: "/orphans".into(),
                kind: DiffKind::Missing,
                a: None,
                b: Some(Value::Array(orphans)),
            });
        }
        let (un, ids) = classify(&d);
        Ok((un, ids, (!why.is_empty()).then(|| why.join("; "))))
    }
}
