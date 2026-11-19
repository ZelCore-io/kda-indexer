"""Node-backed checks for answers that have no stock verdict (stdlib only).

Block ranges (blocksFromHeight, blocksFromDepth, completedBlockHeights): every hash is the
node's canonical header at its chain and height, the rows sit inside the requested window,
and the order is the documented one. Transaction and transfer lists: the rows exist on the
node (`/poll` on their chain, in the block ours names), `transactions(accountName)` lists
only txs the account sent, no row repeats, and `totalCount` equals the rows when the page
holds the whole list. The pure functions take the node's answers as arguments; `Node`
fetches them under its own rate cap (one read per chain at most, cached).
"""

import bisect
import collections
import contextlib
import json
import re
import threading
import time
import urllib.parse

import allow
import endpoints

BRANCH_MAX = 360          # header/branch rows per read
SAMPLE_ROWS = 10          # rows per answer checked against /poll
MAX_CHAINS = 4            # chains per answer checked
TIP_DEPTH = 10            # a block this close to the tip may be reorged between two reads
TIP_SLACK = 3             # blocks the tip may move between ours' answer and the node read
TIP_ROW_AGE = 5           # seconds: a tip sample older than this is not a reading at that time
SETTLE_S = 30             # seconds before the re-read of a near-tip mismatch (run-2 triage class 14)
EVIDENCE_EPS = {"gql.transactions", "gql.transactionsByPublicKey", "gql.transfers"}
EVIDENCE_PAGE = 100       # rows per evidence page
EVIDENCE_PAGES = 5        # pages per side: a longer list is not completed (evidence incomplete)
EVIDENCE_READS = 20       # node reads per case for the extra rows
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
    if any(c not in tips for c, _, _ in rows):
        p.append("no-tip")                 # a chain missing from the cut: nothing to judge by
    near = near_tip_mismatches(rows, canon, tips)
    if near:
        p.append("tip-race")
    for c, h, x in rows:
        if c in tips and x is not None and (c, h) in canon and canon[(c, h)] != x and h <= tips[c] - TIP_DEPTH:
            p.append("not-canonical")
            break
    if any(c in tips and h > tips[c] + TIP_SLACK for c, h, _ in rows):
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


def near_tip_mismatches(rows, canon, tips):
    """[(chain, height, ours' hash)] that differ from the node within TIP_DEPTH of the tip."""
    return [(c, h, x) for c, h, x in rows
            if c in tips and x is not None and (c, h) in canon and canon[(c, h)] != x and h > tips[c] - TIP_DEPTH]


def settle(near, first, second):
    """H4 (B4, run-2 triage class 14): a near-tip mismatch is a tip race only when a fresh re-read
    settles it: the node now holds ours' hash, or its own hash at that height changed between the
    two reads (a reorg). `first`/`second`: {(chain, height): hash} of the two reads. Returns
    "tip-race-settled", "not-canonical" (the node still holds the same other block) or
    "tip-race" (the re-read did not cover it: unsettled)."""
    out = "tip-race-settled"
    for c, h, x in near:
        y = second.get((c, h))
        if y is None:
            return "tip-race"
        if y != x and y == first.get((c, h)):
            out = "not-canonical"
    return out


def failures(p):
    """The problems that fail a check. Only `tip-race-settled` (a near-tip mismatch a fresh
    re-read settled) is not one; an unsettled `tip-race` fails (H4, B4)."""
    return [x for x in p if x != "tip-race-settled"]


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
    ks = [(r["rk"], r["block"], r["order"]) for r in keyed]
    seen = collections.Counter(ks)
    selfish = {(r["rk"], r["block"], r["order"]) for r in keyed
               if by_account and r["from"] is not None and r["from"] == r["to"]}
    # the two copies of a self-transfer sit next to each other (M5)
    apart = {k for k in selfish if seen[k] == 2 and
             (lambda ix: ix[1] - ix[0] != 1)([i for i, y in enumerate(ks) if y == k])}
    if any(n > (2 if k in selfish else 1) for k, n in seen.items()) or apart:
        p.append("duplicate")
    _, conn = _edges(ep, body)
    if isinstance(conn, dict) and isinstance(conn.get("totalCount"), int):
        pi = conn.get("pageInfo") or {}
        whole = pi.get("hasNextPage") is False and pi.get("hasPreviousPage") is False \
            and not v.get("after") and not v.get("before")
        if whole and conn["totalCount"] != len(conn.get("edges") or []):
            p.append("totalCount")
    return p


class TipLog:
    """Time-ordered `(wall time, height)` samples, bounded; `at(t)` is the newest sample at or
    before `t` when it is fresh enough. One cut (or lastBlockHeight) read per second fills it, so
    the runner reads the tip for a request from here and never per request."""

    def __init__(self, maxlen=900):
        self.maxlen = maxlen
        self.ts, self.hs = [], []
        self.lock = threading.Lock()

    def add(self, t, h):
        with self.lock:
            i = bisect.bisect_right(self.ts, t)
            self.ts.insert(i, t)
            self.hs.insert(i, h)
            while len(self.ts) > self.maxlen:
                self.ts.pop(0)
                self.hs.pop(0)

    def at(self, t, max_age=TIP_ROW_AGE):
        with self.lock:
            i = bisect.bisect_right(self.ts, t)
            if i == 0 or t - self.ts[i - 1] > max_age:
                return None
            return self.hs[i - 1]

    def __len__(self):
        return len(self.ts)

    def __getitem__(self, i):
        with self.lock:
            return (self.ts[i], self.hs[i])

    def __iter__(self):
        with self.lock:
            return iter(list(zip(self.ts, self.hs)))


def needs_evidence(ep, entries):
    """A list diff where stock has more rows than ours: a `totalCount` that is higher on stock
    or edges only stock has (kind Missing)."""
    if ep not in EVIDENCE_EPS:
        return False
    root = ep.split(".", 1)[1]
    for e in entries or []:
        if e.get("path") == "/data/%s/totalCount" % root and e.get("kind") == "Changed":
            a, b = allow._num(e.get("a")), allow._num(e.get("b"))
            if a is not None and b is not None and b > a:
                return True
        elif e.get("kind") == "Missing" and re.fullmatch(r"/data/%s/edges/\d+" % root, e.get("path", "")):
            return True
    return False


# The selection that carries a row's block: where it sits is what the node is asked about.
EVIDENCE_SEL = {
    "transactions": "hash cmd { meta { chainId } } result { ... on TransactionResult { height block { hash } } }",
    "transactionsByPublicKey": "hash cmd { meta { chainId } } result { ... on TransactionResult { height block { hash } } }",
    "transfers": "requestKey orderIndex chainId height blockHash",
}


def evidence_doc(ep):
    field = ep.split(".", 1)[1]
    vdefs, args = endpoints.GQL[field][0], endpoints.GQL[field][1]
    return ("query E_%s(%s) { %s(%s) { pageInfo { hasNextPage endCursor } edges { cursor node { %s } } } }"
            % (field, vdefs, field, args, EVIDENCE_SEL[field]))


def evidence_vars(v):
    """The case's filters with the paging replaced by one full page."""
    out = {k: x for k, x in (v or {}).items() if k not in ("first", "last", "after", "before")}
    out["first"] = EVIDENCE_PAGE
    return out


def evidence_rows(ep, body):
    """[{key, chain, height, block}] of an evidence page: `key` identifies the row (request key or
    hash, block, order index)."""
    nodes, _ = _edges(ep, body)
    out = []
    for n in nodes:
        if not isinstance(n, dict):
            continue
        if ep == "gql.transfers":
            rk, block, order = n.get("requestKey"), n.get("blockHash"), n.get("orderIndex")
            chain, height = _chain(n.get("chainId")), n.get("height")
        else:
            res = n.get("result") if isinstance(n.get("result"), dict) else {}
            rk, block, order = n.get("hash"), _get(res, "block", "hash"), None
            chain, height = _chain(_get(n, "cmd", "meta", "chainId")), res.get("height")
        out.append({"key": (rk, block, order), "chain": chain, "height": height, "block": block})
    return out


def page_rows(fetch, ep, vars_, max_pages=EVIDENCE_PAGES):
    """Pages `fetch(vars)` (an answer body or None) from the first page: (rows, complete, pages).
    Complete means the last page said there is no next one; a failing page or the page cap leaves
    it incomplete."""
    v = dict(vars_)
    rows, pages = [], 0
    field = ep.split(".", 1)[1]
    while pages < max_pages:
        body = fetch(dict(v))
        conn = _get(body, "data", field) if isinstance(body, dict) else None
        if not isinstance(conn, dict):
            return rows, False, pages
        pages += 1
        rows += evidence_rows(ep, body)
        pi = conn.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            return rows, True, pages
        if not pi.get("endCursor"):
            return rows, False, pages
        v["after"] = pi["endCursor"]
    return rows, False, pages


def extra_rows(ours, stock):
    """The rows only stock lists (a multiset difference by row key)."""
    have = collections.Counter(r["key"] for r in ours)
    out = []
    for r in stock:
        if have[r["key"]] > 0:
            have[r["key"]] -= 1
        else:
            out.append(r)
    return out


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

    def __init__(self, http, url, network, rps=20, max_wait=2.0, settle_s=SETTLE_S):
        self.http, self.base = http, url.rstrip("/") + "/chainweb/0.0/" + network
        self.bucket = Bucket(rps)
        self.max_wait = max_wait
        self.settle_s = settle_s
        self.lock = threading.Lock()
        self.cut, self.cut_t = None, 0
        self.canon = collections.OrderedDict()
        self.recent = {}
        self.reads = 0
        self.tip_log = TipLog()          # (wall time, highest chain height), one cut read a second
        self.tl = threading.local()      # the per-case read cap of `read_cap`

    def _call(self, path, body=None, accept="application/json"):
        cap = getattr(self.tl, "cap", None)
        if cap is not None and self.tl.used >= cap:
            raise Budget()
        if not self.bucket.try_take(self.max_wait):
            raise Budget()
        if cap is not None:
            self.tl.used += 1
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
                self.tip_log.add(time.time(), max(v["height"] for v in c["hashes"].values()))
        return {int(k): v["height"] for k, v in self.cut.items()}, {int(k): v["hash"] for k, v in self.cut.items()}

    def tip_pair(self, t0, t1=None):
        """{"ours", "stock"}: the highest chain height at `t0` (ours' read) and at `t1` (stock's
        answer, default now), from the log of cut reads, each at most TIP_ROW_AGE old. No node
        read: the sampler keeps the log. None when either is missing; recorded in the row for
        the tip-timing rule."""
        h0 = self.tip_log.at(t0, TIP_ROW_AGE)
        h1 = self.tip_log.at(time.time() if t1 is None else t1, TIP_ROW_AGE)
        return None if h0 is None or h1 is None else {"ours": h0, "stock": h1}

    def sample_tips(self, period=1.0):
        """Keeps the tip log fresh: one cut read per `period` seconds, in a daemon thread."""
        def loop():
            while True:
                try:
                    self.tips(fresh=True)
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(period)
        threading.Thread(target=loop, daemon=True).start()

    @contextlib.contextmanager
    def read_cap(self, n):
        """At most `n` node reads from this thread inside the block (cache hits are free); the
        next one raises Budget. Yields the counter (`.used`)."""
        self.tl.cap, self.tl.used = n, 0
        try:
            yield self.tl
        finally:
            self.tl.cap = None

    def evidence(self, rows, max_reads=EVIDENCE_READS):
        """Node evidence for rows only stock lists (`extra_rows`): how many sit in a block the node
        does not hold as canonical (`stock_orphans`), how many could not be resolved (`unresolved`:
        near the tip, no height, or the read bound), the rows asked and the reads used. One read
        per chain covers a height span under BRANCH_MAX; wider spans read per height."""
        out = {"stock_orphans": 0, "unresolved": 0, "extra": len(rows), "reads": 0, "forks_confirmed": 0,
               "forks": []}
        canon = {}
        with self.read_cap(max_reads) as cap:
            try:
                tips, _ = self.tips()
                by = collections.defaultdict(set)
                for r in rows:
                    c, h = r.get("chain"), r.get("height")
                    if c is not None and isinstance(h, int) and h <= tips.get(c, -1) - TIP_DEPTH:
                        by[c].add(h)
                for c in sorted(by):
                    hs = sorted(by[c])
                    spans = [(hs[0], hs[-1])] if hs[-1] - hs[0] < BRANCH_MAX else [(h, h) for h in hs]
                    for lo, hi in spans:
                        canon.update(self.canonical(c, lo, hi))
            except (Budget, NodeError, ValueError, KeyError):
                pass            # what was read stays; the rest is unresolved
            # H4 (S1): a stock-only row counts as an orphan only when the node holds its block
            # as a fork header at that height; otherwise it is unresolved.
            for r in rows:
                x = canon.get((r.get("chain"), r.get("height")))
                if x is None or r.get("block") is None:
                    out["unresolved"] += 1
                elif x != r["block"]:
                    try:
                        ok = self.fork_header(r["chain"], r["block"], r["height"])
                    except Budget:
                        ok = None
                    # H5 (c): each fork-header read, kept for an offline re-referee.
                    out["forks"].append([r["chain"], r["height"], r["block"], x, ok])
                    if ok:
                        out["stock_orphans"] += 1
                        out["forks_confirmed"] += 1
                    else:
                        out["unresolved"] += 1
            out["reads"] = cap.used
        return out

    def canonical(self, chain, lo, hi, fresh=False):
        """{(chain, height): hash} on the node's current branch. `fresh`: read the node again for
        every height (the settle re-read), with a fresh cut, and answer from that read only."""
        tips, hashes = self.tips(fresh=fresh)
        need = [h for h in range(lo, hi + 1) if fresh or (chain, h) not in self.canon]
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
        if fresh:
            return {(chain, it["height"]): it["hash"] for it in ans.get("items") or [] if lo <= it["height"] <= hi}
        out = {}
        for h in range(lo, hi + 1):
            x = self.canon.get((chain, h)) or self.recent.get((chain, h))
            if x:
                out[(chain, h)] = x
        return out

    def settle_race(self, near, first):
        """Waits `settle_s`, re-reads the cut and the branch at each near-tip mismatch, and
        settles it (`settle`). A failed re-read leaves it `tip-race` (a failure). Returns
        (result, evidence): the evidence is this call's own (H6 B2: the Node is shared by every
        worker thread, so nothing per record is kept on it)."""
        time.sleep(self.settle_s)
        second = {}
        res = None
        try:
            by = collections.defaultdict(list)
            for c, h, _ in near:
                by[c].append(h)
            for c, hs in by.items():
                second.update(self.canonical(c, min(hs), max(hs), fresh=True))
        except (Budget, NodeError, ValueError, KeyError):
            res = "tip-race"
        if res is None:
            res = settle(near, first, second)
        # H5 (c): both reads are kept with the record, so `settle` can be re-applied offline.
        ev = {"near": [[c, h, x] for c, h, x in near],
              "first": [[c, h, first.get((c, h))] for c, h, _ in near],
              "second": [[c, h, second.get((c, h))] for c, h, _ in near],
              "wait_s": self.settle_s, "result": res}
        return res, ev

    def fork_header(self, chain, block, height):
        """True when the node holds `block` as a header at `height` on `chain` (a fork block it
        saw), False when it does not know it."""
        try:
            h = self._call("/chain/%d/header/%s" % (chain, urllib.parse.quote(str(block), safe="")),
                           accept="application/json;blockheader-encoding=object")
        except NodeError:
            return False
        return isinstance(h, dict) and h.get("height") == height

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
                    res, out["settle"] = self.settle_race(near_tip_mismatches(rows, canon, tips), canon)
                    p[p.index("tip-race")] = res
                    if p.count("not-canonical") > 1:
                        p.remove("not-canonical")
                    out.update(ok=not failures(p), why=p)
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
