#!/usr/bin/env python3
"""Endpoint suite, runner and report (stdlib only).

  run.py run --config suite.toml --cases DIR --out DIR [--ours-only] [--per-endpoint N]
             [--endpoints EP ...] [--workers 320] [--heavy-sample 2000]
             [--stock-heavy-hours 10.5] [--import-from OLD/results.jsonl]
      Coverage split (run 2): light endpoints run every case against stock. Heavy endpoints
      (endpoints.HEAVY_ENDPOINTS) run every case on ours with node-backed checks
      (nodecheck.py) and cross-checks against ours' other APIs; a stratified sample of
      `--heavy-sample` cases per heavy endpoint runs against stock in its own lane
      (heavy_concurrency per host, results in DIR/sample.jsonl), until
      `--stock-heavy-hours`. The suite allowlist (allow.py) re-classifies the diffs the
      run-1 triage found DOCUMENTED or STOCK-WRONG.
      Sends every case to ours and to its stock side, and has `kda-compare pair-diff`
      (the live harness's normalisers, field rules and allowlist) give the verdict. Native
      routes have no stock: their answers are checked for the documented invariants, and
      `native.tx` / `native.block_by_height` are cross-checked against ours' other APIs.
      Resumable: cases already in DIR/results.jsonl are skipped. Rate caps per backend come
      from the config; known-heavy GraphQL fields run one at a time per stock host. A stock
      failure that looks like a crash (connection refused or reset, 502, 503) is recorded in
      DIR/stock-events.jsonl and that endpoint and class are skipped on that host from then
      on. `--ours-only` is the dry run: no stock request, no verdict, invariants only.

  run.py estimate --config suite.toml --cases DIR
      Request totals per backend and the expected run time, from the caps and the latency
      assumptions in the config.

  run.py report --out DIR
      DIR/report.md and DIR/summary.json: per endpoint, cases run, equal / allowlisted /
      known-gap / diff / status mismatch / stock error / ours error / not run, and latency
      p50/p99 on both sides; the open diffs with example case ids (host-only: they hold
      real data).

The config (TOML, host-only; see suite.example.toml) names the URLs, caps and the
kda-compare binary and config.
"""

import argparse
import collections
import concurrent.futures as cf
import gzip
import http.client
import json
import os
import re
import ssl
import subprocess
import sys
import threading
import time
import tomllib
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import allow  # noqa: E402
import endpoints as E  # noqa: E402
import nodecheck  # noqa: E402

CRASH_STATUS = {502, 503}
# Load signals for the throttle: no answer, or a gateway/overload status. A fast 500 with a
# GraphQL body (stock's INTERNAL_SERVER_ERROR on bad input) is an answer, not load.
LOAD_ERR = {0, 502, 503, 504}
MSG_LEN = 160
ACCEPT = "application/json, text/plain;q=0.9, */*;q=0.8"


def load_error(st):
    return st in LOAD_ERR
# Ours runs every search under one gate: `search.concurrency` (4) at once, one per client
# (docs/api.md). Search cases hold one of `[ours] search_sources` (4 addresses) while in flight.
# Event queries with a parameter filter go through the same gate (seen in run 2: a 429 on
# compat /txs/events), so they hold a search source too; so does /v1/txs (ours 503 in 0 ms
# when the gate is full).
SEARCH_EPS = {"compat.search", "compat.pactid", "native.search_code", "native.search_events",
              "compat.events", "native.events", "compat.v1txs"}


class Bucket:
    """At most `rps` starts per second (evenly spaced)."""

    def __init__(self, rps):
        self.gap = 1.0 / rps
        self.next = time.monotonic()
        self.lock = threading.Lock()

    def take(self):
        with self.lock:
            now = time.monotonic()
            t = max(now, self.next)
            self.next = t + self.gap
        if t > now:
            time.sleep(t - now)


class Http:
    def __init__(self, timeout, sources=None, insecure=False):
        self.timeout = timeout
        self.sources = sources or [None]
        self.i = 0
        self.lock = threading.Lock()
        self.ctx = ssl._create_unverified_context() if insecure else None

    def call(self, url, path=None, body=None, src=None, accept=ACCEPT):
        u = urllib.parse.urlsplit(url)
        with self.lock:
            if src is None:
                src = self.sources[self.i % len(self.sources)]
            self.i += 1
        kw = {"timeout": self.timeout}
        if src:
            kw["source_address"] = (src, 0)
        if u.scheme == "https":
            c = http.client.HTTPSConnection(u.hostname, u.port or 443, context=self.ctx, **kw)
        else:
            c = http.client.HTTPConnection(u.hostname, u.port or 80, **kw)
        target = (u.path.rstrip("/") + path) if path is not None else (u.path or "/")
        t = time.monotonic()
        try:
            if body is None:
                c.request("GET", target, headers={"accept": accept})
            else:
                c.request("POST", target, body=json.dumps(body).encode(),
                          headers={"content-type": "application/json", "accept": accept})
            resp = c.getresponse()
            raw = resp.read()
            ms = int((time.monotonic() - t) * 1000)
            return {"status": resp.status, "raw": raw, "ms": ms,
                    "hdr": {k.lower(): v for k, v in resp.getheaders()}}
        except Exception as e:  # noqa: BLE001 - every transport failure is a recorded outcome
            return {"status": 0, "raw": b"", "ms": int((time.monotonic() - t) * 1000),
                    "err": type(e).__name__ + ": " + str(e)[:200]}
        finally:
            c.close()


def parse(raw):
    try:
        return json.loads(raw)
    except Exception:
        return raw.decode("utf-8", "replace")[:100000]


TIP_RE = re.compile(r"@tip([+-]\d+)?")


def resolve(v, tip):
    if isinstance(v, str):
        m = TIP_RE.fullmatch(v)
        if m:
            return tip + int(m.group(1) or 0)
        return TIP_RE.sub(lambda m: str(tip + int(m.group(1) or 0)), v) if "@tip" in v else v
    if isinstance(v, list):
        return [resolve(x, tip) for x in v]
    if isinstance(v, dict):
        return {k: resolve(x, tip) for k, x in v.items()}
    return v


def gql_errors(body):
    """(codes, messages) of a GraphQL answer's errors; messages cut to MSG_LEN."""
    if not isinstance(body, dict):
        return [], []
    codes, msgs = [], []
    for e in body.get("errors") or []:
        if isinstance(e, dict):
            codes.append(str((e.get("extensions") or {}).get("code") or e.get("code") or "ERROR"))
            msgs.append(str(e.get("message", ""))[:MSG_LEN])
    return codes, msgs


def gql_error_codes(body):
    return gql_errors(body)[0]


def side_msgs(st, raw, body):
    """Error messages worth keeping for one side: GraphQL error messages, else the start of a
    non-200 body. Truncated."""
    _, msgs = gql_errors(body)
    if msgs:
        return msgs[:3]
    if st != 200 and raw:
        return [raw[:MSG_LEN].decode("utf-8", "replace")]
    return []


# ---------------------------------------------------------------------------------------
# Native invariants (no stock exists)

LIST_ROUTES = {"blocks", "block_txs", "acct_transfers", "acct_txs", "acct_balances", "key_txs",
               "key_accounts", "events", "tokens", "nft", "search_code", "search_events"}


def native_check(ep, cls, path, st, body):
    """Problems with one native answer; `[]` when it keeps the documented contract."""
    route = ep.split(".", 1)[1]
    q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(path).query))
    p = []
    if st >= 500 and st not in (503, 504):
        p.append("5xx:%d" % st)
    if st == 0:
        p.append("transport")
    if st in (503, 504):
        p.append("overload:%d" % st)
    if st != 200:
        if cls != "invalid" and st not in (400, 404) and not p:
            p.append("status:%d" % st)
        return p
    if cls == "invalid":
        p.append("note:invalid-accepted")
    if not isinstance(body, dict):
        return p + ["not-json-object"]
    if "as_of" not in body and route not in ("status", "stats"):
        p.append("no-as_of")
    if route in LIST_ROUTES:
        items = body.get("items")
        if not isinstance(items, list):
            return p + ["no-items"]
        if "next_cursor" not in body:
            p.append("no-next_cursor")
        try:
            lim = int(q.get("limit", "0"))
        except ValueError:
            lim = 0
        if 1 <= lim <= 100 and len(items) > lim:
            p.append("over-limit:%d>%d" % (len(items), lim))
        hs = [i.get("height") for i in items if isinstance(i, dict) and isinstance(i.get("height"), int)]
        order = q.get("order", "desc")
        if route not in ("blocks", "block_txs", "acct_balances", "key_accounts", "tokens", "nft") and len(hs) > 1:
            bad = any(b > a for a, b in zip(hs, hs[1:])) if order == "desc" else any(b < a for a, b in zip(hs, hs[1:]))
            if bad:
                p.append("order")
        if "chain" in q and q["chain"].isdigit():
            if any(isinstance(i, dict) and "chain" in i and str(i["chain"]) != q["chain"] for i in items):
                p.append("chain-filter")
        mn, mx = q.get("minheight"), q.get("maxheight")
        if mn and mn.lstrip("-").isdigit() and any(h < int(mn) for h in hs):
            p.append("below-minheight")
        if mx and mx.lstrip("-").isdigit() and any(h > int(mx) for h in hs):
            p.append("above-maxheight")
    return p


# ---------------------------------------------------------------------------------------

class PairDiff:
    def __init__(self, binary, config):
        self.p = subprocess.Popen([binary, "--config", config, "pair-diff"], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, text=True, bufsize=1)
        self.lock = threading.Lock()

    def __call__(self, rec):
        with self.lock:
            self.p.stdin.write(json.dumps(rec) + "\n")
            self.p.stdin.flush()
            return json.loads(self.p.stdout.readline())


def load_cases(cases_dir, eps, per):
    """Round-robin over endpoints so a partial run covers every endpoint evenly."""
    its = []
    for ep in eps:
        fn = os.path.join(cases_dir, ep + ".jsonl.gz")
        if os.path.exists(fn):
            its.append(gzip.open(fn, "rt"))
    n = collections.Counter()
    while its:
        for f in list(its):
            l = f.readline()
            if not l:
                its.remove(f)
                continue
            c = json.loads(l)
            if per and n[c["ep"]] >= per:
                its.remove(f)
                continue
            n[c["ep"]] += 1
            yield c


def heavy_case(ep, req):
    """Known-heavy on stock (transactions by account, NFT accounts): limited by concurrency
    (`heavy_concurrency` in flight per host), outside the rate target."""
    if ep.startswith("gql.nonFungible"):
        return True
    v = req.get("variables") or {}
    if ep in ("gql.transactions", "gql.transfers") and v.get("accountName"):
        return True
    if ep == "gql.transactionsByPublicKey":
        return True
    return ep in ("compat.account", "compat.v1txs")


def crash_like(r):
    e = r.get("err") or ""
    return r["status"] in CRASH_STATUS or (r["status"] == 0 and any(
        w in e for w in ("Refused", "Reset", "RemoteDisconnected", "BrokenPipe")))


class Gate:
    """At most `limit` in flight; the limit can change while waiting."""

    def __init__(self, limit):
        self.limit, self.used = limit, 0
        self.cv = threading.Condition()

    def __enter__(self):
        with self.cv:
            while self.used >= self.limit:
                self.cv.wait(1)
            self.used += 1

    def __exit__(self, *a):
        with self.cv:
            self.used -= 1
            self.cv.notify_all()

    def set(self, n):
        with self.cv:
            self.limit = n
            self.cv.notify_all()


def judge(win, err_limit, p99_limit):
    """(bad, error share, p99 ms) of a window of (t, load error, ms). `p99_limit` 0: off."""
    if not win:
        return False, 0.0, None
    er = sum(e for _, e, _ in win) / len(win)
    p99 = pct([m for _, _, m in win], .99)
    return er > err_limit or (p99_limit > 0 and p99 > p99_limit), er, p99


class StockHost:
    """One stock backend with an adaptive throttle per class.

    Light cases take the rate bucket; heavy cases a slot of the heavy gate. Each class has
    its own 60 s window and is judged on its own cases only, every 10 s:
      - light: load errors (no answer, 502/503/504) over `err_limit` (2%) or p99 over
        `p99_limit_s` (5 s) halve the rate (at most once a minute); a healthy window at
        least `ramp_every_s` (120 s) after the last change adds `ramp_step` (5) r/s, up to
        the configured rate.
      - heavy: load errors over `heavy_err_limit` (default 1.0: a heavy timeout is expected
        and judged on its own, never against the light rate) or p99 over
        `heavy_p99_limit_s` (default 0: off) halve the heavy slots (at least 1); a healthy
        window adds one slot back, up to `heavy_concurrency`.
    A crash (connection refused or reset, 502, 503) halves the class it happened in and
    pauses the host: the cases in flight are recorded, a probe asks the host every 15 s,
    and once it answers the paused cases are retried once."""

    def __init__(self, name, kind, c, http, log, start=True):
        self.name, self.kind, self.c, self.http, self.log = name, kind, c, http, log
        self.url = c["url"]
        self.target = float(c.get("rps", 35))
        self.rate = self.target
        self.bucket = Bucket(self.rate)
        self.heavy_target = int(c.get("heavy_concurrency", 4))
        self.heavy_limit = self.heavy_target
        self.heavy = Gate(self.heavy_limit)
        self.light = threading.Semaphore(c.get("concurrency", 64))
        self.win = {"light": collections.deque(), "heavy": collections.deque()}
        self.inflight = set()
        self.healthy = threading.Event()
        self.healthy.set()
        self.lock = threading.Lock()
        self.changed = self.heavy_changed = time.monotonic()
        self.crashed = {"light": False, "heavy": False}
        self.err_limit = c.get("err_limit", 0.02)
        self.p99_limit = c.get("p99_limit_s", 5.0) * 1000
        self.heavy_err_limit = c.get("heavy_err_limit", 1.0)
        self.heavy_p99_limit = c.get("heavy_p99_limit_s", 0) * 1000
        self.ramp_every = c.get("ramp_every_s", 120)
        self.ramp_step = c.get("ramp_step", 5)
        self.max_height = c.get("max_height") or None
        self.timeout = c.get("timeout_s")
        if start:
            threading.Thread(target=self.judge_loop, daemon=True).start()

    def note(self, what, **kw):
        rec = dict(ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), host=self.name, event=what,
                   rate=round(self.rate, 1), heavy=self.heavy_limit, **kw)
        self.log.write(json.dumps(rec) + "\n")
        print("throttle " + json.dumps(rec), flush=True)

    def set_rate(self, r, why):
        self.rate = max(1.0, min(self.target, r))
        self.bucket.gap = 1.0 / self.rate
        self.changed = time.monotonic()
        self.note("rate", cls="light", why=why)

    def set_heavy(self, n, why):
        self.heavy_limit = max(1, min(self.heavy_target, n))
        self.heavy.set(self.heavy_limit)
        self.heavy_changed = time.monotonic()
        self.note("rate", cls="heavy", why=why)

    def send(self, req):
        h = self.http
        if self.timeout and self.timeout != h.timeout:
            h = Http(self.timeout, insecure=bool(h.ctx))
        if self.kind == "gql":
            return h.call(self.url, body={"query": req["query"], "variables": req["variables"]})
        return h.call(self.url, path=req["path"])

    def record(self, cls, t, status, ms):
        with self.lock:
            self.win[cls].append((t, load_error(status), ms))
            if status in CRASH_STATUS:
                self.crashed[cls] = True

    def call(self, cid, req, heavy):
        cls = "heavy" if heavy else "light"
        for attempt in (0, 1):
            self.healthy.wait()
            with (self.heavy if heavy else self.light):
                if not heavy:
                    self.bucket.take()
                with self.lock:
                    self.inflight.add(cid)
                r = self.send(req)
                with self.lock:
                    self.inflight.discard(cid)
                self.record(cls, time.monotonic(), r["status"], r["ms"])
            r["host"], r["attempt"] = self.name, attempt
            if not crash_like(r):
                return r
            self.crash(cid, r, cls)
        return r

    def crash(self, cid, r, cls):
        with self.lock:
            self.crashed[cls] = True
            if not self.healthy.is_set():
                return
            self.healthy.clear()
            ids = sorted(self.inflight | {cid})
        self.note("crash", cls=cls, status=r["status"], err=r.get("err"), inflight=ids)
        threading.Thread(target=self.probe, daemon=True).start()

    def probe(self):
        q = {"query": "{ lastBlockHeight }", "variables": {}} if self.kind == "gql" else {"path": self.c.get("probe_path", "/stats")}
        while True:
            time.sleep(15)
            r = self.send(q)
            if r["status"] == 200:
                self.note("healthy-again")
                self.healthy.set()
                return

    def judge_once(self, now):
        with self.lock:
            for w in self.win.values():
                while w and w[0][0] < now - 60:
                    w.popleft()
            lw, hw = list(self.win["light"]), list(self.win["heavy"])
            crashed, self.crashed = self.crashed, {"light": False, "heavy": False}
        bad, er, p99 = judge(lw, self.err_limit, self.p99_limit)
        if (bad or crashed["light"]) and now - self.changed >= 60:
            self.set_rate(self.rate / 2, "crash" if crashed["light"] else "light errors %.1f%% p99 %s ms" % (er * 100, p99))
        elif lw and not bad and not crashed["light"] and self.rate < self.target and now - self.changed >= self.ramp_every:
            self.set_rate(self.rate + self.ramp_step, "healthy")
        hbad, her, hp99 = judge(hw, self.heavy_err_limit, self.heavy_p99_limit)
        if crashed["heavy"] or (hbad and hw):
            if now - self.heavy_changed >= 60 or crashed["heavy"]:
                self.set_heavy(self.heavy_limit // 2, "crash" if crashed["heavy"] else "heavy errors %.1f%% p99 %s ms" % (her * 100, hp99))
        elif hw and self.heavy_limit < self.heavy_target and now - self.heavy_changed >= self.ramp_every:
            self.set_heavy(self.heavy_limit + 1, "healthy")

    def judge_loop(self):
        while True:
            time.sleep(10)
            if self.healthy.is_set():
                self.judge_once(time.monotonic())


def ours_sources(o, rps):
    """Source addresses for ours so that no source passes ours' per-client limit
    (`per_ip_limit`, ours' `rate_per_ip`): at most half of it per source. Listed `sources`
    must be enough; else `source_count` (or the minimum, at least 16) loopback addresses
    from `source_base` + `source_start` are used."""
    per = float(o.get("per_ip_limit", 20)) * 0.5
    need = max(1, int(-(-rps // per)))
    if o.get("sources") and not o.get("source_count"):
        if len(o["sources"]) < need:
            raise SystemExit("ours: %d sources for %s r/s; ours allows %s r/s per source, need >= %d"
                             % (len(o["sources"]), rps, o.get("per_ip_limit", 20), need))
        return list(o["sources"])
    n = max(need, int(o.get("source_count", 16)))
    base, start = o.get("source_base", "127.0.0."), int(o.get("source_start", 150))
    if start + n > 255:
        raise SystemExit("ours: source range too long")
    return ["%s%d" % (base, start + i) for i in range(n)]


NODE_EPS = {"gql.blocksFromHeight", "gql.blocksFromDepth", "gql.completedBlockHeights",
            "gql.transactions", "gql.transactionsByPublicKey", "gql.transfers", "gql.events",
            "compat.account", "compat.txs", "compat.events"}


def normalize_pair(ep, ab, sb):
    """Harness-side normalisation before the diff: fungibleChainAccounts compares as a set
    (ours lists chains ascending, stock's order is unspecified; triage 5)."""
    if ep == "gql.fungibleChainAccounts":
        for b in (ab, sb):
            xs = ((b or {}).get("data") or {}).get("fungibleChainAccounts") if isinstance(b, dict) else None
            if isinstance(xs, list):
                xs.sort(key=lambda x: str((x or {}).get("chainId", "")).zfill(3) if isinstance(x, dict) else "")
    return ab, sb


def verdict_class(ep, cls, v, ours_codes, vars_, node):
    """Class and allowlist ids after the suite allowlist (allow.py)."""
    c = v.get("class")
    ents = v.get("entries") or []
    if any(e.get("path") == "/~graphql-oracle-stall" for e in ents):
        return "stock-unavailable", []
    if c in ("diff", "status"):
        k, ids = allow.classify({"ep": ep, "cls": cls, "ours_codes": ours_codes, "entries": ents,
                                 "vars": vars_, "node": node})
        if k != "diff":
            return k, ids
    return c, []


class Runner:
    def __init__(self, cfg, a):
        self.cfg, self.a = cfg, a
        o = cfg["ours"]
        st = cfg.get("stock", {})
        # Ours is paired with stock: by default it carries the stock side's total rate.
        paired = sum(x.get("rps", 35) for x in st.get("oracle", [])) + sum(
            st[k].get("rps", 30) for k in ("cwd", "kdatxs") if k in st)
        rps = o.get("rps") or paired or 100
        self.ours = Http(o.get("timeout_s", 30), ours_sources(o, rps))
        self.ours_bucket = Bucket(rps)
        os.makedirs(a.out, exist_ok=True)
        fn = os.path.join(a.out, "results.jsonl")
        if getattr(a, "import_from", None) and not os.path.exists(fn):
            n = import_rows(a.import_from, fn)
            print("imported %d rows from %s" % (n, a.import_from), flush=True)
        self.res = open(fn, "a", buffering=1)
        self.sample = open(os.path.join(a.out, "sample.jsonl"), "a", buffering=1)
        self.events = open(os.path.join(a.out, "stock-events.jsonl"), "a", buffering=1)
        # The stock GraphQL hosts are reached by address (certificate for a hostname): opt-in.
        http = Http(st.get("timeout_s", 180), insecure=st.get("insecure_tls", False))
        self.oracles = [] if a.ours_only else [StockHost(x["name"], "gql", x, http, self.events)
                                               for x in st.get("oracle", [])]
        self.rest_stock = {} if a.ours_only else {k: StockHost(k, "rest", st[k], http, self.events)
                                                  for k in ("cwd", "kdatxs") if k in st}
        self.pd = None if a.ours_only else PairDiff(cfg["diff"]["kda_compare"], cfg["diff"]["config"])
        nd = cfg.get("node") or {}
        self.node = nodecheck.Node(Http(nd.get("timeout_s", 30)), nd["url"], nd.get("network", "mainnet01"),
                                   min(float(nd.get("rps", 20)), 20.0),
                                   float(nd.get("max_wait_s", 30))) if nd.get("url") else None
        self.tip = None
        import queue
        self.search_src = queue.Queue()
        for x in o.get("search_sources") or ["127.0.0.%d" % i for i in (210, 211, 212, 213)]:
            self.search_src.put(x)
        print("ours: %s r/s over %d sources; node checks %s" % (rps, len(self.ours.sources),
                                                              "on" if self.node else "off"), flush=True)

    def ours_call(self, fam, req, src=None):
        self.ours_bucket.take()
        o = self.cfg["ours"]
        if fam == "graphql":
            return self.ours.call(o["graphql"], body={"query": req["query"], "variables": req["variables"]}, src=src)
        base = o["native"] if fam == "native" else o["compat"]
        return self.ours.call(base, path=req["path"], src=src)

    def ours_tip(self):
        r = self.ours.call(self.cfg["ours"]["graphql"], body={"query": "{ lastBlockHeight }"})
        return int(json.loads(r["raw"])["data"]["lastBlockHeight"])

    def stock_host(self, case, stock):
        if stock == "oracle" and self.oracles:
            return self.oracles[int(case["id"].rsplit(":", 1)[1]) % len(self.oracles)]
        return self.rest_stock.get(stock)

    def stock_call(self, case, fam, stock, req, heavy):
        ep = case["ep"]
        if ep in E.STOCK_SKIP:
            return None, "not-run: " + E.STOCK_SKIP[ep]
        h = self.stock_host(case, stock)
        if h is None:
            return None, "not-run: no stock configured"
        return h.call(case["id"], req, heavy), None

    def one(self, case, lane=None):
        """One case. `lane`: None (main pass), "sample" (heavy endpoint against stock, in a
        stock lane) or "light" (a light endpoint run in a stock lane)."""
        ep = case["ep"]
        fam, stock = ep.split(".")[0], dict((e[0], e[2]) for e in E.ENDPOINTS)[ep]
        fam = {"gql": "graphql"}.get(fam, fam)
        maxh = None
        tip = self.tip
        if fam == "graphql" and self.oracles:
            maxh = min((x.max_height or tip) for x in self.oracles)
            tip = min(tip, maxh)
        req = resolve(case["req"], tip)
        if ep in SEARCH_EPS:
            src = self.search_src.get()
            try:
                a = self.ours_call(fam, req, src)
            finally:
                self.search_src.put(src)
        else:
            a = self.ours_call(fam, req)
        ab = parse(a["raw"])
        row = {"id": case["id"], "ep": ep, "cls": case["cls"], "t": int(time.time()),
               "ours": {"st": a["status"], "ms": a["ms"], "b": len(a["raw"]), "err": a.get("err")}}
        codes = []
        if fam == "graphql":
            codes = gql_error_codes(ab)
            row["ours"]["gql_errors"] = codes[:3]
            if any(c in ("GRAPHQL_PARSE_FAILED", "GRAPHQL_VALIDATION_FAILED") for c in codes) \
                    and case["cls"] not in ("invalid",):
                row["ours"]["note"] = "document-rejected"
        m = side_msgs(a["status"], a["raw"], ab)
        if m:
            row["ours"]["msg"] = m
        vars_ = req.get("variables") or {}
        heavy_ep = ep in E.HEAVY_ENDPOINTS
        if fam == "native":
            row["cov"] = "native"
            row["inv"] = native_check(ep, case["cls"], req["path"], a["status"], ab)
            row["cross"] = self.cross(ep, req, a["status"], ab)
        elif heavy_ep and lane is None:
            row["cov"] = "node"
            if self.node and a["status"] == 200 and ep in NODE_EPS:
                row["node"] = self.node.check(ep, vars_, ab)
            row["cross"] = self.cross(ep, req, a["status"], ab)
        elif not self.a.ours_only:
            row["cov"] = "stock-sample" if lane == "sample" else "stock"
            s, why = self.stock_call(case, fam, stock, req, heavy=lane is not None)
            if s is None:
                row["stock"] = {"not_run": why}
            else:
                sb = parse(s["raw"])
                row["stock"] = {"host": s["host"], "st": s["status"], "ms": s["ms"], "err": s.get("err"),
                                "retry": s.get("attempt", 0)}
                if fam == "graphql":
                    row["stock"]["gql_errors"] = gql_error_codes(sb)[:3]
                sm = side_msgs(s["status"], s["raw"], sb)
                if sm:
                    row["stock"]["msg"] = sm
                if a["status"] == 429:
                    row["v"] = {"class": "ours-429"}
                elif load_error(s["status"]):
                    row["v"] = {"class": "stock-unavailable"}
                elif s["status"] and a["status"]:
                    ab, sb = normalize_pair(ep, ab, sb)
                    rq = req["path"] if fam != "graphql" else "%s %s" % (req.get("op", "op"), json.dumps(req["variables"]))
                    v = self.pd({"id": case["id"], "kind": "graphql" if fam == "graphql" else "rest",
                                 "request": rq, "max": maxh,
                                 "ours": {"status": a["status"], "body": ab},
                                 "stock": {"status": s["status"], "body": sb}})
                    node = None
                    if v.get("class") in ("diff", "status") and self.node and ep in NODE_EPS and a["status"] == 200:
                        node = row["node"] = self.node.check(ep, vars_, ab)
                    k, ids = verdict_class(ep, case["cls"], v, codes, vars_, node)
                    row["v"] = {"class": k, "ids": (v.get("ids") or []) + ids, "known": v.get("known_ids"),
                                "entries": v.get("entries")}
        (self.sample if lane == "sample" else self.res).write(json.dumps(row, separators=(",", ":")) + "\n")
        return row

    def cross(self, ep, req, st, body):
        """Cross-check of an answer against ours' other APIs (same fact, one more request)."""
        if st != 200 or not isinstance(body, (dict, list)):
            return None
        q = urllib.parse.quote
        if ep == "native.tx" and isinstance(body, dict) and body.get("request_key"):
            r = self.ours_call("compat", {"path": "/txs/tx?requestkey=" + q(body["request_key"])})
            c = parse(r["raw"])
            if r["status"] != 200 or not isinstance(c, dict):
                return {"ok": body.get("height") == 0, "why": "compat %d" % r["status"]}
            ok = c.get("height") == body.get("height") and c.get("blockHash") == body.get("block_hash")
            return {"ok": ok, "why": None if ok else "height/blockHash differ"}
        if ep == "native.block_by_height" and isinstance(body, dict) and body.get("hash"):
            r = self.ours_call("native", {"path": "/api/v1/blocks/" + q(body["hash"], safe="")})
            c = parse(r["raw"])
            ok = r["status"] == 200 and isinstance(c, dict) and all(
                c.get(k) == body.get(k) for k in ("hash", "height", "chain", "parent", "payload_hash"))
            return {"ok": ok, "why": None if ok else "by-hash answer differs (%d)" % r["status"]}
        tx = blk = None
        d = body.get("data") if isinstance(body, dict) else None
        if ep == "gql.transaction" and isinstance(d, dict) and isinstance(d.get("transaction"), dict):
            t = d["transaction"]
            res = t.get("result") or {}
            if t.get("hash") and isinstance(res.get("block"), dict) and res["block"].get("hash"):
                tx = (t["hash"], res["block"]["hash"], res.get("height", res["block"].get("height")))
        elif ep == "compat.tx" and isinstance(body, dict) and body.get("requestKey") and body.get("blockHash"):
            tx = (body["requestKey"], body["blockHash"], body.get("height"))
        elif ep == "gql.block" and isinstance(d, dict) and isinstance(d.get("block"), dict):
            blk = d["block"]
        elif ep in ("gql.blocksFromHeight", "gql.blocksFromDepth") and isinstance(d, dict):
            edges = ((d.get(ep[4:]) or {}).get("edges") or [])
            blk = (edges[0] or {}).get("node") if edges and isinstance(edges[0], dict) else None
        if tx:
            r = self.ours_call("native", {"path": "/api/v1/txs/" + q(tx[0], safe="")})
            c = parse(r["raw"])
            ok = r["status"] == 200 and isinstance(c, dict) and c.get("block_hash") == tx[1] and \
                (tx[2] is None or c.get("height") == tx[2])
            return {"ok": ok, "with": "native /txs", "why": None if ok else "native tx differs (%d)" % r["status"]}
        if isinstance(blk, dict) and blk.get("hash") and isinstance(blk.get("height"), int):
            r = self.ours_call("native", {"path": "/api/v1/blocks/" + q(blk["hash"], safe="")})
            c = parse(r["raw"])
            ok = r["status"] == 200 and isinstance(c, dict) and c.get("height") == blk["height"] and (
                blk.get("chainId") is None or str(c.get("chain")) == str(blk["chainId"]))
            return {"ok": ok, "with": "native /blocks", "why": None if ok else "native block differs (%d)" % r["status"]}
        return None

    def done_ids(self, name):
        done = set()
        fn = os.path.join(self.a.out, name)
        if os.path.exists(fn):
            for l in open(fn):
                try:
                    done.add(json.loads(l)["id"])
                except Exception:
                    pass
        return done

    def pool(self, cases, workers, lane, deadline=None, name="main"):
        """Run `cases` on `workers` threads; stops submitting at `deadline` (monotonic)."""
        n = skipped = 0
        gate = threading.Semaphore(workers * 2)
        with cf.ThreadPoolExecutor(workers) as ex:
            for c in cases:
                if deadline and time.monotonic() > deadline:
                    skipped += 1
                    continue
                gate.acquire()
                f = ex.submit(self.one, c, lane)
                f.add_done_callback(lambda _f: gate.release())
                n += 1
                if n % 1000 == 0:
                    print("%s submitted %d" % (name, n), flush=True)
        print("%s done %d (past deadline: %d)" % (name, n, skipped), flush=True)

    def run(self):
        done = self.done_ids("results.jsonl")
        sdone = self.done_ids("sample.jsonl")
        eps = self.a.endpoints or [e[0] for e in E.ENDPOINTS]
        self.tip = self.ours_tip()
        print("tip=%d resume_skip=%d sample_skip=%d" % (self.tip, len(done), len(sdone)), flush=True)
        deadline = time.monotonic() + self.a.stock_heavy_hours * 3600
        lanes = []
        if not self.a.ours_only:
            heavy = [e for e in eps if e in E.HEAVY_ENDPOINTS and e not in E.STOCK_SKIP]
            for i, h in enumerate(self.oracles):
                hv = [e for e in heavy if e.startswith("gql.")]
                cs = host_cases(load_cases(self.a.cases, hv, self.a.heavy_sample), i, len(self.oracles), sdone)
                lanes.append((h, cs, "sample", deadline))
                lt = [e for e in eps if e in E.LANE_LIGHT and e.startswith("gql.")]
                if lt:
                    cs2 = host_cases(load_cases(self.a.cases, lt, self.a.per_endpoint), i, len(self.oracles), done)
                    lanes.append((h, cs2, "light", deadline))
            for k, h in self.rest_stock.items():
                hv = [e for e in heavy if e.startswith("compat.") and dict((x[0], x[2]) for x in E.ENDPOINTS)[e] == k]
                cs = (c for c in load_cases(self.a.cases, hv, self.a.heavy_sample) if c["id"] not in sdone)
                lanes.append((h, cs, "sample", deadline))
                lt = [e for e in eps if e in E.LANE_LIGHT and dict((x[0], x[2]) for x in E.ENDPOINTS)[e] == k]
                if lt:
                    cs2 = (c for c in load_cases(self.a.cases, lt, self.a.per_endpoint) if c["id"] not in done)
                    lanes.append((h, cs2, "light", deadline))
        threads = []
        for h, cs, kind, dl in lanes:
            t = threading.Thread(target=self.pool, args=(cs, h.heavy_target, kind, dl, "%s-%s" % (h.name, kind)))
            t.start()
            threads.append(t)
        main_eps = [e for e in eps if self.a.ours_only or e not in E.LANE_LIGHT]
        self.pool((c for c in load_cases(self.a.cases, main_eps, self.a.per_endpoint) if c["id"] not in done),
                  self.a.workers, None, None, "main")
        for t in threads:
            t.join()
        print("all done", flush=True)


def host_cases(cases, i, n, done):
    """The cases of stock host `i` of `n` (split by case number), minus `done`."""
    for c in cases:
        if c["id"] not in done and int(c["id"].rsplit(":", 1)[1]) % n == i:
            yield c


def import_rows(src, dst):
    """Rows of an earlier run whose coverage class matches run 2: light endpoints paired with
    stock and native routes, minus the harness artifacts (ours 429, generated node ids,
    cross-check routes, which run again). Re-classified with the suite allowlist."""
    n = 0
    with open(dst, "w") as out:
        for l in open(src):
            try:
                r = json.loads(l)
            except Exception:
                continue
            ep = r["ep"]
            if ep in E.HEAVY_ENDPOINTS or ep in E.NATIVE_CROSS or r["ours"].get("st") == 429:
                continue
            if ep in ("gql.node", "gql.nodes") and r["cls"] != "real":
                continue
            r["from"] = "run1"
            r["cov"] = "native" if ep.startswith("native.") else "stock"
            s = r.get("stock") or {}
            v = r.get("v")
            if s.get("host") and load_error(s.get("st", 0)):
                r["v"] = {"class": "stock-unavailable"}
            elif v:
                k, ids = verdict_class(ep, r["cls"], v, r["ours"].get("gql_errors") or [], {}, None)
                v["class"], v["ids"] = k, (v.get("ids") or []) + ids
            out.write(json.dumps(r, separators=(",", ":")) + "\n")
            n += 1
    return n


# ---------------------------------------------------------------------------------------

def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def report(out):
    """DIR/report.md and DIR/summary.json from results.jsonl (main pass) and sample.jsonl
    (heavy endpoints against stock). Per endpoint and coverage source: cases, equal,
    allowlisted, known-gap, diff, status, stock unavailable, errors on each side, node and
    cross-checks, p50/p99 on both sides; then the open diff classes with example ids."""
    agg = collections.defaultdict(collections.Counter)
    lat = collections.defaultdict(lambda: {"ours": [], "stock": []})
    classes = collections.defaultdict(lambda: [0, []])
    allow_ids = collections.Counter()
    probs = collections.defaultdict(collections.Counter)
    hourly = collections.defaultdict(collections.Counter)
    node_why = collections.Counter()
    cov_tot = collections.defaultdict(collections.Counter)
    for fname in ("results.jsonl", "sample.jsonl"):
        path = os.path.join(out, fname)
        if not os.path.exists(path):
            continue
        for l in open(path):
            try:
                r = json.loads(l)
            except Exception:
                continue
            ep = r["ep"]
            cov = r.get("cov") or ("native" if ep.startswith("native.") else "stock")
            key = (ep, cov)
            hr = time.strftime("%Y-%m-%dT%H", time.gmtime(r.get("t", 0)))
            if r.get("from") is None:
                hourly[hr]["ours"] += 1 + (r.get("cross") is not None)
                if r.get("stock") and r["stock"].get("host"):
                    hourly[hr][r["stock"]["host"]] += 1 + r["stock"].get("retry", 0)
            c = agg[key]
            c["run"] += 1
            o = r["ours"]
            lat[key]["ours"].append(o["ms"])
            c["ours_err"] += o["st"] == 0 or o["st"] >= 500
            c["ours_429"] += o["st"] == 429
            if o.get("note"):
                probs[ep][o["note"]] += 1
            inv = [p for p in r.get("inv") or [] if not p.startswith("note")]
            for p in r.get("inv") or []:
                probs[ep][p.split(":")[0] if not p.startswith("note") else p] += 1
            if cov == "native":
                c["inv_clean"] += not [p for p in inv if not p.startswith("overload")]
            x = r.get("cross")
            if x is not None:
                c["cross_ok" if x["ok"] else "cross_bad"] += 1
                cov_tot["cross-check"]["ok" if x["ok"] else "bad"] += 1
                if not x["ok"] and len(classes[(ep, "cross", x.get("why") or "?")][1]) < 3:
                    classes[(ep, "cross", x.get("why") or "?")][1].append(r["id"])
                if not x["ok"]:
                    classes[(ep, "cross", x.get("why") or "?")][0] += 1
            nd = r.get("node")
            if nd is not None and cov == "node":
                k = {True: "node_ok", False: "node_bad"}.get(nd.get("ok"), "node_skip")
                c[k] += 1
                cov_tot["node"][k] += 1
                for w in nd.get("why") or []:
                    node_why[(ep, w.split(":")[0] if nd.get("ok") is None else w)] += 1
                if nd.get("ok") is False:
                    ck = (ep, "node", ",".join(nd.get("why") or []))
                    classes[ck][0] += 1
                    if len(classes[ck][1]) < 3:
                        classes[ck][1].append(r["id"])
            s = r.get("stock")
            if s is None:
                continue
            if "not_run" in s:
                c["not_run"] += 1
                continue
            lat[key]["stock"].append(s["ms"])
            c["stock_err"] += s["st"] == 0 or s["st"] >= 500
            v = r.get("v")
            if v:
                k = v["class"]
                c[k] += 1
                cov_tot[cov][k] += 1
                for i in v.get("ids") or []:
                    allow_ids[i] += 1
                if k in ("diff", "status"):
                    pats = sorted({"%s %s" % (re.sub(r"/\d+", "/N", e.get("path", "?")), e.get("kind"))
                                   for e in (v.get("entries") or [])[:4]})
                    ck = (ep, k, " + ".join(pats[:3]))
                    classes[ck][0] += 1
                    if len(classes[ck][1]) < 3:
                        classes[ck][1].append(r["id"])
    cols = ["run", "equal", "allowlisted", "known-gap", "diff", "status", "stock-unavailable",
            "stock_err", "ours_err", "ours_429", "not_run", "node_ok", "node_bad", "node_skip",
            "cross_ok", "cross_bad", "inv_clean"]
    lines = ["# Endpoint suite report", "",
             "Coverage: `stock` = paired with stock (light endpoints, all cases), `stock-sample` = heavy "
             "endpoint sample against stock, `node` = heavy endpoint on ours with node-backed checks and "
             "cross-checks, `native` = no stock (invariants and cross-checks).", "",
             "| endpoint | cov | " + " | ".join(cols) + " | ours p50/p99 ms | stock p50/p99 ms |",
             "|---" * (len(cols) + 4) + "|"]
    summ = {}
    for key in sorted(agg):
        c = agg[key]
        lo, ls = lat[key]["ours"], lat[key]["stock"]
        row = {k: c.get(k, 0) for k in cols}
        row.update({"ours_p50": pct(lo, .5), "ours_p99": pct(lo, .99), "stock_p50": pct(ls, .5),
                    "stock_p99": pct(ls, .99)})
        summ["%s|%s" % key] = row
        lines.append("| %s | %s | %s | %s/%s | %s/%s |" % (key[0], key[1], " | ".join(str(row[k]) for k in cols),
                                                         row["ours_p50"], row["ours_p99"],
                                                         row["stock_p50"], row["stock_p99"]))
    lines += ["", "## Per coverage source", ""]
    for cov in sorted(cov_tot):
        t = cov_tot[cov]
        if cov in ("node",):
            n = t["node_ok"] + t["node_bad"]
            lines.append("- node: %d checked, %d ok (%.2f%%), %d bad, %d skipped" % (
                n, t["node_ok"], 100.0 * t["node_ok"] / max(n, 1), t["node_bad"], t["node_skip"]))
        elif cov == "cross-check":
            n = t["ok"] + t["bad"]
            lines.append("- cross-check: %d, %d ok (%.2f%%), %d bad" % (n, t["ok"], 100.0 * t["ok"] / max(n, 1), t["bad"]))
        else:
            n = sum(t[k] for k in ("equal", "allowlisted", "known-gap", "diff", "status"))
            lines.append("- %s: %d verdicts, %d equal (%.2f%%), %d allowlisted, %d known-gap, %d diff, %d status; "
                         "%d stock unavailable" % (cov, n, t["equal"], 100.0 * t["equal"] / max(n, 1), t["allowlisted"],
                                                   t["known-gap"], t["diff"], t["status"], t["stock-unavailable"]))
    lines += ["", "## Allowlist hits (suite rules and harness ids)", ""]
    for k, n in allow_ids.most_common():
        lines.append("- %s: %d" % (k, n))
    lines += ["", "## Node checks not done or failed (endpoint, reason)", ""]
    for (ep, w), n in node_why.most_common(40):
        lines.append("- %s %s: %d" % (ep, w, n))
    backs = sorted({b for h in hourly.values() for b in h})
    lines += ["", "## Achieved r/s per backend per hour (UTC; partial hours divide by 3600 too)", "",
              "| hour | " + " | ".join(backs) + " |", "|---" * (len(backs) + 1) + "|"]
    for hr in sorted(hourly):
        lines.append("| %s | %s |" % (hr, " | ".join("%.1f" % (hourly[hr][b] / 3600) for b in backs)))
    lines += ["", "## Ours problems (invariants, rejected documents)", ""]
    for ep in sorted(probs):
        lines.append("- %s: %s" % (ep, dict(probs[ep])))
    lines += ["", "## Open diff classes (endpoint, kind, path pattern: cases, example ids)", ""]
    for (ep, k, pat), (n, ids) in sorted(classes.items(), key=lambda x: -x[1][0]):
        lines.append("- %s %s `%s`: %d (%s)" % (ep, k, pat, n, ", ".join(ids)))
    open(os.path.join(out, "report.md"), "w").write("\n".join(lines) + "\n")
    json.dump({"endpoints": summ, "coverage": {k: dict(v) for k, v in cov_tot.items()},
               "allow": dict(allow_ids), "hourly": {h: dict(v) for h, v in hourly.items()},
               "classes": [[ep, k, pat, n, ids] for (ep, k, pat), (n, ids) in classes.items()]},
              open(os.path.join(out, "summary.json"), "w"), indent=1)
    print("\n".join(lines[:6]))


def estimate(cfg, cases_dir):
    """Requests per backend and hours at the configured rates. Light stock cases run at
    min(rps, concurrency / latency) per host; heavy ones at heavy_concurrency / latency per
    host, beside the light ones. Ours carries every case (plus native cross-checks) at its
    paired rate. Latencies come from `[estimate]`."""
    req, heavy = collections.Counter(), collections.Counter()
    for ep in E.ENDPOINTS:
        fn = os.path.join(cases_dir, ep[0] + ".jsonl.gz")
        if not os.path.exists(fn):
            continue
        for l in gzip.open(fn, "rt"):
            c = json.loads(l)
            req["ours"] += 1 + (ep[0] in E.NATIVE_CROSS)
            req["ours_search"] += ep[0] in SEARCH_EPS
            if ep[2] in ("oracle", "cwd", "kdatxs") and ep[0] not in E.STOCK_SKIP:
                req[ep[2]] += 1
                heavy[ep[2]] += heavy_case(ep[0], c["req"])
    st, lat = cfg.get("stock", {}), cfg.get("estimate", {})
    o = st.get("oracle", [])
    tables = {"oracle": o, "cwd": [st["cwd"]] if "cwd" in st else [], "kdatxs": [st["kdatxs"]] if "kdatxs" in st else []}
    secs = {}
    for b, hosts in tables.items():
        if not req[b]:
            continue
        hosts = hosts or [{}]
        ls, hs = lat.get(b + "_light_s", lat.get(b + "_s", 0.6)), lat.get(b + "_heavy_s", 3.0)
        light = sum(min(h.get("rps", 35), h.get("concurrency", 64) / ls) for h in hosts)
        hv = sum(h.get("heavy_concurrency", 4) / hs for h in hosts)
        secs[b] = max((req[b] - heavy[b]) / light, heavy[b] / hv)
    paired = sum(x.get("rps", 35) for x in o) + sum(st[k].get("rps", 30) for k in ("cwd", "kdatxs") if k in st)
    ours_rps = cfg["ours"].get("rps") or paired or 100
    secs["ours"] = req["ours"] / ours_rps
    srch = sum(1 for e in E.ENDPOINTS if e[0] in SEARCH_EPS) and req["ours_search"]
    secs["ours_search"] = srch * lat.get("ours_search_s", 1.5) / 4
    total_stock_rps = {b: round((req[b] - heavy[b]) / secs[b], 1) for b in tables if b in secs}
    out = {"requests": dict(req), "heavy": dict(heavy), "ours_rps": ours_rps,
           "hours": {k: round(v / 3600, 2) for k, v in secs.items()},
           "wall_hours_min": round(max(secs.values()) / 3600, 2),
           "light_rps_per_backend": total_stock_rps}
    print(json.dumps(out, indent=1))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--config", required=True)
    r.add_argument("--cases", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--ours-only", action="store_true")
    r.add_argument("--per-endpoint", type=int, default=0)
    r.add_argument("--endpoints", nargs="*")
    r.add_argument("--workers", type=int, default=320)
    r.add_argument("--heavy-sample", type=int, default=2000, help="stock sample per heavy endpoint")
    r.add_argument("--stock-heavy-hours", type=float, default=10.5,
                   help="stop sending new heavy sample cases to stock after this many hours")
    r.add_argument("--import-from", help="results.jsonl of an earlier run (see import_rows)")
    e = sub.add_parser("estimate")
    e.add_argument("--config", required=True)
    e.add_argument("--cases", required=True)
    p = sub.add_parser("report")
    p.add_argument("--out", required=True)
    a = ap.parse_args()
    if a.cmd == "run":
        Runner(tomllib.load(open(a.config, "rb")), a).run()
    elif a.cmd == "estimate":
        estimate(tomllib.load(open(a.config, "rb")), a.cases)
    else:
        report(a.out)


if __name__ == "__main__":
    main()
