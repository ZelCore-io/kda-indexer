"""Node-backed checks for answers that have no stock verdict (stdlib only).

Block ranges (blocksFromHeight, blocksFromDepth, completedBlockHeights): every hash is the
node's canonical header at its chain and height, the rows sit inside the requested window,
and the order is the documented one. Transaction and transfer lists: the rows exist on the
node (`/poll` on their chain, in the block ours names), `transactions(accountName)` lists
only txs the account sent, no row repeats, and `totalCount` equals the rows when the page
holds the whole list. The pure functions take the node's answers as arguments; `Node`
fetches them under its own rate cap (one read per chain at most, cached).
"""

import collections
import json
import re
import threading
import time
import urllib.parse

import allow

BRANCH_MAX = 360          # header/branch rows per read
SAMPLE_ROWS = 10          # rows per answer checked against /poll
MAX_CHAINS = 4            # chains per answer checked
TIP_DEPTH = 10            # a block this close to the tip may be reorged between two reads
TIP_SLACK = 3             # blocks the tip may move between ours' answer and the node read
RK_RE = re.compile(r"[A-Za-z0-9_-]{43}")   # a request key (pseudo keys like <coinbase> are not polled)


def _get(d, *path):
    for k in path:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def _edges(ep, body):
    field = ep.split(".", 1)[1]
    conn = _get(body, "data", field)
    if isinstance(conn, dict):
        return [e.get("node") for e in conn.get("edges") or [] if isinstance(e, dict)], conn
    if isinstance(body, list):
        return body, None
    if isinstance(body, dict) and isinstance(body.get("items"), list):
        return body["items"], None
    return [], None


def _chain(v):
    try:
        return int(str(v))
    except (TypeError, ValueError):
        return None


def block_rows(ep, body):
    """[(chain, height, hash or None)] in answer order."""
    nodes, _ = _edges(ep, body)
    out = []
    for n in nodes:
        if isinstance(n, dict) and isinstance(n.get("height"), int) and _chain(n.get("chainId")) is not None:
            out.append((_chain(n["chainId"]), n["height"], n.get("hash")))
    return out


def check_block_range(ep, v, rows, canon, tips):
    """Problems with a block-range answer. `canon`: {(chain, height): hash} from the node;
    `tips`: {chain: node tip height} read after ours answered."""
    p = []
    for c, h, x in rows:
        if x is not None and (c, h) in canon and canon[(c, h)] != x and h > tips.get(c, h) - TIP_DEPTH:
            p.append("tip-race")
            break
    for c, h, x in rows:
        if x is not None and (c, h) in canon and canon[(c, h)] != x and h <= tips.get(c, h) - TIP_DEPTH:
            p.append("not-canonical")
            break
    if any(h > tips.get(c, h) + TIP_SLACK for c, h, _ in rows):
        p.append("above-tip")
    hs = [h for _, h, _ in rows]
    if ep == "gql.blocksFromHeight":
        start, end = v.get("startHeight"), v.get("endHeight")
        if isinstance(start, int) and any(h < start for h in hs):
            p.append("below-start")
        if isinstance(end, int) and any(h > end for h in hs):
            p.append("above-end")
        if any(b < a for a, b in zip(hs, hs[1:])):
            p.append("order")
    elif ep == "gql.blocksFromDepth":
        d = v.get("minimumDepth")
        if isinstance(d, int) and d >= 0 and any(h > tips.get(c, h) - d + TIP_SLACK for c, h, _ in rows):
            p.append("window")
        if any(b > a for a, b in zip(hs, hs[1:])):
            p.append("order")
    if len(set((c, h) for c, h, _ in rows)) != len(rows):
        p.append("duplicate")
    return p


def failures(p):
    """The problems that fail a check: `tip-race` (a mismatch within TIP_DEPTH of the tip, a
    reorg between the two reads) is recorded but is not a failure."""
    return [x for x in p if x != "tip-race"]


def orphan_evidence(rows, canon):
    """For rows only stock lists (`[(chain, height, block hash)]`) against the node's canonical
    hashes `{(chain, height): hash}`: how many sit in a block the node does not hold as canonical
    (orphans) and how many could not be resolved. Evidence for the orphan-count rule."""
    orphans = unresolved = 0
    for c, h, x in rows:
        if (c, h) not in canon:
            unresolved += 1
        elif canon[(c, h)] != x:
            orphans += 1
    return {"stock_orphans": orphans, "unresolved": unresolved}


def _rk(n):
    return n.get("requestKey") or n.get("request_key") or (n.get("hash") if "cmd" in n or "result" in n else None)


def tx_rows(ep, body):
    """[{rk, chain, block, sender}] for transaction, transfer and event lists."""
    nodes, _ = _edges(ep, body)
    out = []
    for n in nodes:
        if not isinstance(n, dict):
            continue
        rk = _rk(n)
        if not isinstance(rk, str):
            continue
        chain = _chain(n.get("chainId", n.get("chain", _get(n, "cmd", "meta", "chainId"))))
        block = n.get("blockHash") or n.get("block_hash") or _get(n, "result", "block", "hash") or _get(n, "block", "hash")
        out.append({"rk": rk, "chain": chain, "block": block, "sender": _get(n, "cmd", "meta", "sender"),
                    "order": n.get("orderIndex", n.get("idx")),
                    "from": n.get("fromAccount"), "to": n.get("toAccount")})
    return out


def sample_rows(rows, n=SAMPLE_ROWS):
    """The first and last n/2 rows (deterministic)."""
    if len(rows) <= n:
        return list(rows)
    return list(rows[: n // 2]) + list(rows[-(n - n // 2):])


def check_tx_list(ep, v, body, rows, polled):
    """Problems with a tx/transfer list. `polled`: {(chain, rk): poll metaData or None (not
    on node)} for the rows that were checked; rows not in `polled` were not checked. Genesis
    request keys repeat across chains, so a height-0 answer is not compared by block."""
    p = []
    for r in rows:
        m = polled.get((r["chain"], r["rk"]), False)
        if m is False:
            continue
        if m is None:
            p.append("missing-on-node")
            break
        if r["block"] and m.get("blockHash") and m.get("blockHeight") != 0 and m["blockHash"] != r["block"]:
            p.append("block-differs")
            break
    if ep == "gql.transactions" and v.get("accountName") and not v.get("fungibleName"):
        if any(r["sender"] is not None and r["sender"] != v["accountName"] for r in rows):
            p.append("sender")
    # Duplicates only where a row is identified: a tx by (rk, block), a transfer or event by
    # (rk, block, orderIndex). A min selection without those fields is not checked.
    tx_list = ep in ("gql.transactions", "gql.transactionsByPublicKey", "compat.txs")
    keyed = [r for r in rows if r["block"] and (tx_list or r["order"] is not None)]
    # An account-filtered transfer list holds a self-transfer twice, once as sender and once as
    # receiver (as chainweb-data does, rule R-P5P9-105): two copies of a row with from == to
    # pass there; a third copy, or two copies of any other row, fail.
    by_account = ep == "compat.account" or (ep == "gql.transfers" and v.get("accountName"))
    seen = collections.Counter((r["rk"], r["block"], r["order"]) for r in keyed)
    selfish = {(r["rk"], r["block"], r["order"]) for r in keyed
               if by_account and r["from"] is not None and r["from"] == r["to"]}
    if any(n > (2 if k in selfish else 1) for k, n in seen.items()):
        p.append("duplicate")
    _, conn = _edges(ep, body)
    if isinstance(conn, dict) and isinstance(conn.get("totalCount"), int):
        pi = conn.get("pageInfo") or {}
        whole = pi.get("hasNextPage") is False and pi.get("hasPreviousPage") is False \
            and not v.get("after") and not v.get("before")
        if whole and conn["totalCount"] != len(conn.get("edges") or []):
            p.append("totalCount")
    return p


class Bucket:
    def __init__(self, rps):
        self.gap = 1.0 / rps
        self.next = time.monotonic()
        self.lock = threading.Lock()

    def try_take(self, max_wait):
        with self.lock:
            now = time.monotonic()
            t = max(now, self.next)
            if t - now > max_wait:
                return False
            self.next = t + self.gap
        if t > now:
            time.sleep(t - now)
        return True


class Node:
    """The chainweb node's service API under a read cap (`rps`, default 20)."""

    def __init__(self, http, url, network, rps=20, max_wait=2.0):
        self.http, self.base = http, url.rstrip("/") + "/chainweb/0.0/" + network
        self.bucket = Bucket(rps)
        self.max_wait = max_wait
        self.lock = threading.Lock()
        self.cut, self.cut_t = None, 0
        self.canon = collections.OrderedDict()
        self.recent = {}
        self.reads = 0
        self.tip_log = collections.deque(maxlen=4000)   # (wall time, highest chain height)

    def _call(self, path, body=None, accept="application/json"):
        if not self.bucket.try_take(self.max_wait):
            raise Budget()
        self.reads += 1
        r = self.http.call(self.base, path=path, body=body, accept=accept)
        if r["status"] != 200:
            raise NodeError("%s %d" % (path.split("?")[0][-30:], r["status"]))
        return json.loads(r["raw"])

    def tips(self, fresh=False):
        with self.lock:
            cached = not fresh and self.cut is not None and time.monotonic() - self.cut_t < 10
        if not cached:
            c = self._call("/cut")
            with self.lock:
                self.cut, self.cut_t = c["hashes"], time.monotonic()
                self.tip_log.append((time.time(), max(v["height"] for v in c["hashes"].values())))
        return {int(k): v["height"] for k, v in self.cut.items()}, {int(k): v["hash"] for k, v in self.cut.items()}

    def tip_pair(self, t0):
        """{"ours", "stock"}: the highest chain height at about `t0` (ours' read, from the log of
        cut reads, at most TIP_SAMPLE_AGE old) and now, from a fresh cut (stock's answer has
        just come back). None when either is missing; recorded in the row for the tip-timing
        rule."""
        try:
            h0 = allow.tip_before(list(self.tip_log), t0)
            if h0 is None:
                return None
            tips, _ = self.tips(fresh=True)
            return {"ours": h0, "stock": max(tips.values())}
        except Exception:  # noqa: BLE001 - the tip is evidence only; no budget, no record
            return None

    def sample_tips(self, period=10.0):
        """Keeps the tip log fresh: one cut read per `period` seconds, in a daemon thread."""
        def loop():
            while True:
                try:
                    self.tips(fresh=True)
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(period)
        threading.Thread(target=loop, daemon=True).start()

    def canonical(self, chain, lo, hi):
        tips, hashes = self.tips()
        need = [h for h in range(lo, hi + 1) if (chain, h) not in self.canon]
        if need:
            q = urllib.parse.urlencode({"minheight": min(need), "maxheight": max(need), "limit": BRANCH_MAX})
            ans = self._call("/chain/%d/header/branch?%s" % (chain, q), body={"lower": [], "upper": [hashes[chain]]},
                             accept="application/json;blockheader-encoding=object")
            with self.lock:
                for it in ans.get("items") or []:
                    if it["height"] <= tips[chain] - 10:
                        self.canon[(chain, it["height"])] = it["hash"]
                    else:
                        self.canon.pop((chain, it["height"]), None)
                    self.recent[(chain, it["height"])] = it["hash"]
                if len(self.recent) > 50000:
                    self.recent.clear()
                while len(self.canon) > 200000:
                    self.canon.popitem(last=False)
        out = {}
        for h in range(lo, hi + 1):
            x = self.canon.get((chain, h)) or self.recent.get((chain, h))
            if x:
                out[(chain, h)] = x
        return out

    def poll(self, chain, rks):
        ans = self._call("/chain/%d/pact/api/v1/poll" % chain, body={"requestKeys": rks})
        return {(chain, rk): (ans[rk].get("metaData") or {}) if rk in ans else None for rk in rks}

    def check(self, ep, v, body):
        """{"ok", "why", "rows"} or None when the answer has nothing the node can check."""
        try:
            if ep in ("gql.blocksFromHeight", "gql.blocksFromDepth", "gql.completedBlockHeights"):
                rows = block_rows(ep, body)
                if not rows:
                    return None
                tips, _ = self.tips()
                canon = {}
                by = collections.defaultdict(list)
                for c, h, x in rows:
                    by[c].append(h)
                for c in sorted(by)[:MAX_CHAINS]:
                    lo, hi = min(by[c]), max(by[c])
                    if hi - lo < BRANCH_MAX and any(x for cc, _, x in rows if cc == c):
                        canon.update(self.canonical(c, lo, hi))
                p = check_block_range(ep, v, rows, canon, tips)
                out = {"ok": not failures(p), "why": p or None, "rows": len(rows), "canon": len(canon)}
                if "tip-race" in p:
                    out["tip"] = max(tips.values())    # recorded with the rows' heights in the answer
                return out
            rows = tx_rows(ep, body)
            if not rows:
                return None
            polled = {}
            by = collections.defaultdict(list)
            for r in sample_rows(rows):
                if r["chain"] is not None and RK_RE.fullmatch(r["rk"]):
                    by[r["chain"]].append(r["rk"])
            for c in sorted(by)[:3]:
                polled.update(self.poll(c, sorted(set(by[c]))))
            p = check_tx_list(ep, v, body, rows, polled)
            return {"ok": not failures(p), "why": p or None, "rows": len(rows), "polled": len(polled)}
        except Budget:
            return {"ok": None, "why": ["skipped:node-budget"]}
        except Exception as e:  # noqa: BLE001 - a node failure is recorded, never a verdict
            return {"ok": None, "why": ["node-error:" + (type(e).__name__ + " " + str(e))[:80]]}


class Budget(Exception):
    pass


class NodeError(Exception):
    pass
