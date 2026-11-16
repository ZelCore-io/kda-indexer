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

/// A REST account row whose sender is its receiver (a non-empty account).
fn is_self_transfer(r: &Value) -> bool {
    let acct = |k: &str| r.get(k).and_then(Value::as_str).filter(|s| !s.is_empty());
    acct("fromAccount").is_some() && acct("fromAccount") == acct("toAccount")
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
/// R-P5P9-95 I-3: or (`whole`) on every row of both reads.
fn reads_agree(route: &RouteKind, a: &Value, b: &Value, whole: bool) -> Result<usize, String> {
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
    if lo > hi && !whole {
        return Err(format!("the reads do not overlap in height ({lo} > {hi})"));
    }
    if !whole {
        for v in [&mut a, &mut b] {
            if let Value::Array(x) = v {
                x.retain(|r| row_height(r).is_some_and(|h| (lo..=hi).contains(&h)));
            }
        }
    }
    numbers_as_f64(&mut a);
    numbers_as_f64(&mut b);
    events_as_multiset(&mut a, &mut b);
    let d = diff_route(route, &a, &b);
    if d.is_empty() {
        Ok(len_of(&a))
    } else if whole {
        Err(format!("the re-reads differ ({} entries)", d.len()))
    } else {
        Err(format!(
            "the reads differ on the overlap {lo}..={hi} ({} entries)",
            d.len()
        ))
    }
}

/// R-P5P9-95 I-3, live: the re-reads (`ra` ours, `rb` kda-txs) agree only when no row only
/// kda-txs listed in the first reads (`va`, `vb`) is still listed by kda-txs' re-read while
/// ours' re-read lacks it; and, when kda-txs' re-read reaches ours' top, on every row (else
/// on their height overlap).
fn reread_agrees(
    route: &RouteKind,
    (va, vb): (&Value, &Value),
    ra: &Value,
    rb: &Value,
) -> Result<usize, String> {
    let listed = |v: &Value, k: &RowKey| {
        v.as_array()
            .is_some_and(|x| x.iter().any(|r| row_key(r) == *k))
    };
    let still = only_keys_in_first(vb, va)
        .iter()
        .filter(|r| {
            let k = row_key(r);
            listed(rb, &k) && !listed(ra, &k)
        })
        .count();
    if still > 0 {
        return Err(format!(
            "{still} rows only kda-txs listed at first are still in kda-txs' re-read and not \
             in ours'"
        ));
    }
    reads_agree(route, ra, rb, max_height(rb) >= max_height(ra))
}

/// RFC 3986 percent-encoding of a path segment (unreserved characters and `:` kept).
fn pct_segment(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b':' => {
                (b as char).to_string()
            }
            b => format!("%{b:02X}"),
        })
        .collect()
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn rest_compose(
        &self,
        req: &Request,
        kdatxs: bool,
        va: &mut Value,
        vb: &mut Value,
        full_a: bool,
        lowest_a: Option<u64>,
        horizon: Option<u64>,
    ) -> Result<Vec<DiffEntry>, String> {
        let only_a = only_keys_in_first(va, vb);
        let only_b = only_keys_in_first(vb, va);
        if only_a.is_empty() && only_b.is_empty() {
            return Err("no one-sided row".into());
        }
        // R-P5P9-95 m-6: a row only ours lists that ours lists more than once settles nothing
        // (each copy would pass the node check on its own).
        // R-P5P9-105: a self-transfer (from = to, the same requestKey and idx) is listed
        // twice by contract, as sender and as receiver (cwd does the same), so its two copies
        // count as two rows. A third copy, or two of any other row, is a duplicate.
        let mut copies: HashMap<RowKey, (usize, bool)> = HashMap::new();
        for r in va.as_array().into_iter().flatten() {
            let c = copies.entry(row_key(r)).or_insert((0, true));
            c.0 += 1;
            c.1 &= is_self_transfer(r);
        }
        let allowed = |(n, selfx): (usize, bool)| n <= if selfx { 2 } else { 1 };
        if only_a
            .iter()
            .any(|r| copies.get(&row_key(r)).is_some_and(|&c| !allowed(c)))
        {
            return Err("a row only ours lists is listed more than once".into());
        }
        let lowest = lowest_a;
        let mut floor: Vec<Value> = Vec::new();
        for r in &only_b {
            let h = row_height(r);
            // R-P5P9-95 m-6: below ours' lowest height by position; at it, only inside the
            // lag horizon (`horizon`: ours' tip less 1,440; `None`: never).
            let placed = match (h, lowest) {
                (Some(h), Some(m)) if h < m => true,
                (Some(h), Some(m)) if h == m => horizon.is_some_and(|f| h >= f),
                _ => false,
            };
            // R-P5P9-105 (#1966): at ours' own floor height, with ours full, the row counts
            // as below the cut when the node places it canonical at that height (below).
            let at_floor = full_a && !placed && h.is_some() && h == lowest;
            if at_floor {
                floor.push(r.clone());
                continue;
            }
            if !(full_a && placed) {
                return Err(format!(
                    "an oracle-only row at height {h:?} is not below ours' lowest {lowest:?} \
                     (or at it inside the lag horizon from {horizon:?}) with ours' limit held \
                     ({full_a})"
                ));
            }
        }
        if !floor.is_empty() {
            self.node_places_at_floor(req, &floor)
                .await
                .map_err(|w| format!("an oracle-only row at ours' floor height {lowest:?}: {w}"))?;
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

    /// R-P5P9-105 (REST #1966): each oracle-only row at ours' floor height (at most
    /// `FLOOR_CHECKS` live, `REFEREE_FLOOR_CHECKS` in the referee, both in `graphql_window`)
    /// is canonical there on the node: its `/poll` entry is in the row's block at the row's
    /// height. One read of the run's budget per chain chunk.
    async fn node_places_at_floor(&self, req: &Request, rows: &[Value]) -> Result<(), String> {
        let cap = self
            .gql
            .floor_checks
            .load(std::sync::atomic::Ordering::Relaxed);
        if rows.len() > cap {
            return Err(format!("{} rows, over {cap}", rows.len()));
        }
        let mut by_chain: HashMap<u8, Vec<(String, String, u64)>> = HashMap::new();
        for r in rows {
            let s = |k: &str| r.get(k).and_then(Value::as_str).map(str::to_string);
            let (Some(rk), Some(bh), Some(c), Some(h)) =
                (s("requestKey"), s("blockHash"), row_chain(r), row_height(r))
            else {
                return Err("a row without its request key, block, chain or height".into());
            };
            if rk == "<coinbase>" {
                return Err("a coinbase row has no /poll entry".into());
            }
            by_chain.entry(c).or_default().push((rk, bh, h));
        }
        for (c, keys) in by_chain {
            for chunk in keys.chunks(NODE_POLL_CHUNK) {
                if !self.gql.take_node_read() {
                    return Err("the node read cap is reached".into());
                }
                self.count_request(req.route.as_str(), "node", 1);
                let rks: Vec<String> = chunk.iter().map(|(rk, _, _)| rk.clone()).collect();
                let res = self
                    .node
                    .poll(c, &rks)
                    .await
                    .map_err(|e| format!("poll failed: {e:#}"))?;
                for (rk, bh, h) in chunk {
                    let v = res
                        .get(rk)
                        .ok_or_else(|| format!("the node does not hold {rk}"))?;
                    let at = (
                        v.pointer("/metaData/blockHash").and_then(Value::as_str),
                        v.pointer("/metaData/blockHeight").and_then(Value::as_u64),
                    );
                    if at != (Some(bh.as_str()), Some(*h)) {
                        return Err(format!(
                            "the node places {rk} at {at:?}, not the row's block at {h}"
                        ));
                    }
                }
            }
        }
        Ok(())
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
        match reread_agrees(&req.route, (va, vb), &ra, &rb) {
            Ok(n) => Ok(json!({"reread": "agrees", "rows": n})),
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

    /// R-P5P9-92 (9), R-P5P9-93: the stored form's read of `t` now, one read of the run's
    /// budget (the referee's `--max-reads`) per page; past it, why, its own reason.
    async fn read_now_charged(&self, req: &Request, t: &Target) -> Result<(Value, bool), String> {
        let cap = || {
            format!(
                "the run's read cap is reached before the re-read of {}",
                t.name
            )
        };
        if !self.gql.take_node_read() {
            return Err(cap());
        }
        let p = self
            .fetch_a(req, t)
            .await
            .map_err(|e| format!("the read now failed: {e:#}"))?;
        for _ in 1..p.pages.len() {
            if !self.gql.take_node_read() {
                return Err(cap());
            }
        }
        if p.pages.first().map(|c| c.status) != Some(200) {
            return Err("the read now did not answer 200".into());
        }
        Ok((p.items, p.truncated))
    }

    /// R-P5P9-86 v1txs with R-P5P9-87, the stored form: every stored row of both sides is
    /// node-canonical. R-P5P9-95 I-3: and ours' window floor at the record's height (ours'
    /// highest stored row) lies above every kda-txs-only row: ours' newest `limit` transfer
    /// rows, coinbases included, read now from ours' `/txs/account` bounded by `maxheight`
    /// (coin only). Ours with fewer rows has no floor, and a read the API does not bound by
    /// height settles nothing.
    async fn v1txs_edge_stored(
        &self,
        req: &Request,
        ta: &Target,
        va: &Value,
        vb: &Value,
    ) -> Result<Value, String> {
        let rows: Vec<&Value> = [va, vb]
            .into_iter()
            .filter_map(Value::as_array)
            .flatten()
            .collect();
        self.rows_node_canonical(req, &rows).await?;
        let p = crate::via_cwd::parse(&req.path_and_query);
        let limit = p.limit.map_err(|_| "the request's limit does not read")?;
        if p.namespace != "coin" {
            return Err(format!(
                "ours' window floor is read for coin only, not {}",
                p.namespace
            ));
        }
        let top = max_height(va).ok_or("ours' stored page lists no row")?;
        let floor_req = Request {
            route: RouteKind::Account,
            path_and_query: format!(
                "/txs/account/{}?token=coin&limit={limit}&maxheight={top}",
                pct_segment(&p.account)
            ),
            origin: crate::gen::Origin::Replay,
            oracle: None,
        };
        let (now, cut) = self.read_now_charged(&floor_req, ta).await?;
        let mut hs = Vec::new();
        for r in now.as_array().into_iter().flatten() {
            match row_height(r) {
                Some(h) if h <= top => hs.push(h),
                Some(h) => {
                    return Err(format!(
                        "ours' API did not bound the read at the record's height {top} (a row \
                         at {h})"
                    ))
                }
                None => return Err("a row of ours' window read has no height".into()),
            }
        }
        hs.sort_unstable_by(|x, y| y.cmp(x));
        let Some(&floor) = usize::try_from(limit)
            .ok()
            .and_then(|l| hs.get(l.checked_sub(1)?))
        else {
            return Err(format!(
                "ours lists {} of {limit} rows up to the record's height{}: no window floor",
                hs.len(),
                if cut { " (the read was cut)" } else { "" }
            ));
        };
        let only_b = only_keys_in_first(vb, va);
        if let Some(r) = only_b
            .iter()
            .find(|r| row_height(r).is_none_or(|h| h >= floor))
        {
            return Err(format!(
                "a kda-txs-only row at {:?} is not below ours' window floor {floor}",
                row_height(r)
            ));
        }
        Ok(
            json!({"stored": "every row node-canonical", "nodeCanonical": rows.len(),
            "oursWindowFloor": floor, "recordHeight": top, "kdatxsOnly": only_b.len()}),
        )
    }

    /// R-P5P9-86 with R-P5P9-87: a stored REST record (an account pair against
    /// chainweb-data, a /v1/txs pair against kda-txs) settled by its stored form. A /v1/txs
    /// record of the edge shape is settled by [`Ctx::v1txs_edge_stored`]; any other record by
    /// the stored rows: normalized, the node's orphans moved out, the rows combined by
    /// [`Ctx::rest_compose`] (ours full when it holds the request's limit; the lag horizon
    /// from the record's tip of ours, R-P5P9-96 (2a)), the rest diffed and classified. There is no live window drop: a recent row the oracle lacks must pass
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
            match self.v1txs_edge_stored(&req, ta, &raw_a, &raw_b).await {
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
        // R-P5P9-96 (2a): a record that carries ours' tip and ours' lowest listed height
        // bounds an oracle-only row at ours' lowest height by the lag horizon from that tip,
        // as live does. Without them (CG-5's records), or with a lowest height that is not
        // the stored page's, there is no horizon and such a row stays unexplained.
        let horizon = match (rec.a_tip_height, rec.a_lowest_height) {
            (Some(t), Some(m)) if Some(m) == lowest && t > ORACLE_HORIZON => {
                Some(t - ORACLE_HORIZON)
            }
            _ => None,
        };
        let mut d = match self
            .rest_compose(&req, kdatxs, &mut va, &mut vb, full_a, lowest, horizon)
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
