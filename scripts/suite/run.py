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

      Worker errors (R-P5P9-180): a case whose worker raises is written as a record with verdict
      `harness-error` (id, ep, exception type and message), logged, and counted; resume runs it
      again. More than 1% of a lane's cases in harness-error (rolling window of 200, from 100
      cases, or over the whole lane) stops the run (exit 2). Each lane prints and appends to
      DIR/lanes.jsonl `submitted = written + harness-error`; `report` reconciles them.

  run.py estimate --config suite.toml --cases DIR [--heavy-sample N | --sample-flag]
      Request totals per backend and the expected hours per lane, from the caps and the latency
      assumptions in the config ([estimate], and [estimate.ep] per endpoint in seconds).

  Run 3 (H3): the case list comes from select3.py (run-2 differences first, then a fresh sample);
      `run --sample-flag` sends a heavy endpoint's flagged cases only to stock. Every stock-paired
      row carries `tip` (the node's tip at ours' read and at stock's answer, for the tip-timing
      rule) and `idx` (ours' and the stock host's own lastBlockHeight), read from logs filled once
      per `--tip-period` seconds, never per request. A list diff where stock has more rows
      (totalCount or edges) gets node evidence for the extra rows (`node.stock_orphans`, at most
      20 node reads per case; orphan rule). `smoke` runs named cases under a hard request budget.

  run.py smoke --config suite.toml --cases DIR --out DIR --ids ID ... --budget N --budget-file F
      Real cases through the real runner, every request (ours, stock, node, tip samplers)
      counted against N across invocations; stops at N.

  run.py rereferee --out DIR --cases DIR --md FILE
      Re-classifies the stored diff and status records of DIR with the current allowlist (no
      request is sent) and writes the aggregate counts, unexplained before and after.

  run.py report --out DIR
      DIR/report.md and DIR/summary.json: per endpoint, cases run, equal / allowlisted /
      known-gap / diff / status mismatch / stock error / ours error / not run, and latency
      p50/p99 on both sides; the open diffs with example case ids (host-only: they hold
      real data).

The config (TOML, host-only; see suite.example.toml) names the URLs, caps and the
kda-compare binary and config.
"""

import argparse
import base64
import collections
import decimal
import concurrent.futures as cf
import gzip
import http.client
import json
import math
import os
import re
import ssl
import subprocess
import sys
import threading
import time
import tomllib
import urllib.parse
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import allow  # noqa: E402
import endpoints as E  # noqa: E402
import nodecheck  # noqa: E402
import pagewalk  # noqa: E402

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


class BudgetExhausted(Exception):
    pass


class RequestBudget:
    """A hard cap on the requests this process sends to anything (smoke runs): `limit` in all,
    counted across invocations through `fn` (a file holding the count). No limit: no check."""

    def __init__(self, limit, fn):
        self.limit, self.fn = limit, fn
        self.lock = threading.Lock()
        self.used = 0
        if fn and os.path.exists(fn):
            try:
                self.used = int(open(fn).read().strip() or 0)
            except ValueError:
                self.used = 0

    def take(self):
        if self.limit is None:
            return
        with self.lock:
            if self.used >= self.limit:
                raise BudgetExhausted("request budget %d used" % self.limit)
            self.used += 1
            if self.fn:
                with open(self.fn, "w") as f:
                    f.write(str(self.used))


REQ_BUDGET = RequestBudget(None, None)


class Http:
    def __init__(self, timeout, sources=None, insecure=False):
        self.timeout = timeout
        self.sources = sources or [None]
        self.i = 0
        self.lock = threading.Lock()
        self.ctx = ssl._create_unverified_context() if insecure else None

    def call(self, url, path=None, body=None, src=None, accept=ACCEPT):
        REQ_BUDGET.take()
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

class OnePerKey:
    """At most one task in flight per key (the tip sampler's reads, one per host)."""

    def __init__(self):
        self.busy, self.lock = set(), threading.Lock()

    def start(self, k):
        with self.lock:
            if k in self.busy:
                return False
            self.busy.add(k)
            return True

    def done(self, k):
        with self.lock:
            self.busy.discard(k)


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


def load_cases(cases_dir, eps, per, only_flag=None):
    """Round-robin over endpoints so a partial run covers every endpoint evenly. `only_flag`: only
    the cases that carry that flag (run 3: `sample` marks the cases that also go to stock)."""
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
            if only_flag and not c.get(only_flag):
                continue
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


# Fix round 1 (I1): the 504 guard's defaults; it is on unless a host sets guard_504_max = 0.
G504_DEFAULTS = {"guard_504_max": 5, "guard_504_window_s": 60, "guard_504_action": "backoff",
                 "guard_504_backoff_s": 300, "guard_504_max_trips": 3}


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
        # WP-H1 504 guard (replaces the manual watcher), on by default: `guard_504_max` 504s within
        # `guard_504_window_s` either stop the run ("stop") or pause this host for
        # `guard_504_backoff_s` ("backoff"); the `guard_504_max_trips`-th trip stops the run.
        # An explicit guard_504_max = 0 turns it off.
        self.g504_max = int(c.get("guard_504_max", G504_DEFAULTS["guard_504_max"]))
        self.g504_window = float(c.get("guard_504_window_s", G504_DEFAULTS["guard_504_window_s"]))
        self.g504_action = c.get("guard_504_action", G504_DEFAULTS["guard_504_action"])
        self.g504_backoff = float(c.get("guard_504_backoff_s", G504_DEFAULTS["guard_504_backoff_s"]))
        self.g504_max_trips = int(c.get("guard_504_max_trips", G504_DEFAULTS["guard_504_max_trips"]))
        if self.g504_action not in ("stop", "backoff"):
            raise ValueError("%s: guard_504_action must be stop or backoff" % name)
        self.g504 = collections.deque()
        self.g504_trips = 0
        # Re-review I6: 504s that land while this host is backing off are answers to requests
        # sent before the trip; they are not counted, so one burst is one trip.
        self.g504_until = float("-inf")
        self.on_stop = None             # set by the Runner: ends the run with a message
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
        self.guard_504(t, status)

    def guard_504(self, t, status):
        """WP-H1: counts stock 504s in a sliding window; returns the action taken ("stop",
        "backoff") or None."""
        if not self.g504_max:
            return None
        with self.lock:
            while self.g504 and self.g504[0] < t - self.g504_window:
                self.g504.popleft()
            if status != 504 or t < self.g504_until:
                return None
            self.g504.append(t)
            if len(self.g504) < self.g504_max:
                return None
            n = len(self.g504)
            self.g504.clear()
            self.g504_trips += 1
            stop = self.g504_action == "stop" or self.g504_trips >= self.g504_max_trips
            if not stop:
                self.g504_until = t + self.g504_backoff
        msg = "%s: %d stock 504s within %ds (guard_504_max %d)" % (self.name, n, self.g504_window, self.g504_max)
        if stop:
            self.note("504-guard-stop", n=n)
            if self.on_stop:
                self.on_stop(msg)
            return "stop"
        self.note("504-guard-backoff", n=n, trip=self.g504_trips, pause_s=self.g504_backoff)
        self.healthy.clear()

        def resume():
            time.sleep(self.g504_backoff)
            self.note("504-guard-resume")
            self.healthy.set()
        threading.Thread(target=resume, daemon=True).start()
        return "backoff"

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


def _data(b):
    return b.get("data") if isinstance(b, dict) and isinstance(b.get("data"), dict) else None


def _exact(x):
    """The exact decimal text of a balance (a JSON number or a decimal string); else the value."""
    if isinstance(x, bool) or x is None:
        return x
    try:
        d = decimal.Decimal(str(x))
    except (decimal.InvalidOperation, ValueError):
        return x
    if not d.is_finite():
        return x
    t = format(d.normalize(), "f")
    return "0" if t in ("-0", "0") else t


def _keyed(rows, key):
    """{key: row} of a list; a repeated key gets `#2`, `#3`... so no row is lost."""
    out, seen = {}, collections.Counter()
    for r in rows:
        k = key(r)
        seen[k] += 1
        out[k if seen[k] == 1 else "%s#%d" % (k, seen[k])] = r
    return out


def _balance_map(b):
    """WP-H1: `balance(...)` edges as {"chainId|module": node}, the balance as exact decimal text in
    `balanceExact`."""
    conn = (_data(b) or {}).get("balance")
    if not isinstance(conn, dict) or not isinstance(conn.get("edges"), list):
        return None
    if not all(isinstance(e, dict) and isinstance(e.get("node"), dict) for e in conn["edges"]):
        return None
    for e in conn["edges"]:
        # Renamed so the f64 tolerance pair-diff gives a `balance` leaf (r57-float) cannot apply.
        if "balance" in e["node"]:
            raw = e["node"].pop("balance")
            e["node"]["balanceExact"] = _exact(raw)
            e["_raw"] = raw
    conn["edges"] = _keyed(conn["edges"], lambda e: "%s|%s" % (e["node"].get("chainId"), e["node"].get("module")))
    return set(conn["edges"])


def _balance_text(ab, sb):
    """Fix round 1 (M1): where both sides give the same exact balance in other text ("1.10" and
    "1.1", a number and a string), both keep the raw value in `balanceText`, so the record shows
    the difference a client sees; returns its `balance-text:<chainId|module>` labels (unaccepted)."""
    ea = ab["data"]["balance"]["edges"] if ab is not None else {}
    eb = sb["data"]["balance"]["edges"] if sb is not None else {}
    labels = []
    for k in sorted(set(ea) & set(eb)):
        xa, xb = ea[k], eb[k]
        if "_raw" in xa and "_raw" in xb and xa["node"].get("balanceExact") == xb["node"].get("balanceExact") \
                and json.dumps(xa["_raw"]) != json.dumps(xb["_raw"]):
            xa["node"]["balanceText"], xb["node"]["balanceText"] = xa["_raw"], xb["_raw"]
            labels.append("balance-text:%s" % k)
    for e in list(ea.values()) + list(eb.values()):
        e.pop("_raw", None)
    return labels


def _v1txs_map(b):
    """WP-H1: a `/v1/txs` list as {requestKey: row} (the service's cache `_id` dropped)."""
    if not isinstance(b, list) or not all(isinstance(r, dict) for r in b):
        return None
    for r in b:
        r.pop("_id", None)
    return _keyed(b, lambda r: str(r.get("requestKey")))


def _connections(v, path=""):
    """(path, connection) for every GraphQL connection (a dict with an `edges` list) in a body."""
    if isinstance(v, dict):
        if isinstance(v.get("edges"), list):
            yield path, v
        for k, x in v.items():
            yield from _connections(x, path + "/" + str(k))
    elif isinstance(v, list):
        for i, x in enumerate(v):
            yield from _connections(x, "%s/%d" % (path, i))


def page_cut(body, side):
    """WP-H1: where only part of a list was answered: a connection with `hasNextPage` true, or a
    `totalCount` above the rows it lists. Each item names the side and the connection's path."""
    out = []
    for path, c in _connections(body):
        pi = c.get("pageInfo") if isinstance(c.get("pageInfo"), dict) else {}
        tc = c.get("totalCount")
        if pi.get("hasNextPage") is True:
            out.append("%s:%s hasNextPage" % (side, path))
        elif isinstance(tc, int) and not isinstance(tc, bool) and tc > len(c["edges"]):
            out.append("%s:%s totalCount %d > %d rows" % (side, path, tc, len(c["edges"])))
    return out


def _tx_key(e):
    n = e.get("node") if isinstance(e, dict) else None
    rk = n.get("hash", n.get("requestKey")) if isinstance(n, dict) else None
    return (str(rk), json.dumps(e, sort_keys=True))


def _proven_whole(c):
    """A connection that shows it lists every row: `hasNextPage` false, or a `totalCount` equal to
    its rows."""
    pi = c.get("pageInfo") if isinstance(c.get("pageInfo"), dict) else {}
    tc = c.get("totalCount")
    return pi.get("hasNextPage") is False or \
        (isinstance(tc, int) and not isinstance(tc, bool) and tc == len(c["edges"]))


def _nested_block_txs(b):
    """The nested `transactions` connections of the blocks in a GraphQL answer (not a root list)."""
    return [(p, c) for p, c in _connections(b) if p.endswith("/transactions") and p != "/data/transactions"]


def normalize_pair(ep, ab, sb):
    """Harness-side alignment before the diff; returns (ours, stock, info). info["cut"] names every
    list answered in part (the record is then `incomplete` at best); info["one_side"] names rows only
    one side lists (`one-side:<key>`), for the node check.
    - fungibleChainAccounts compares as a set (ours lists chains ascending, stock's order is
      unspecified; triage 5);
    - `balance(...)` edges compare as a map (chainId, module) -> exact decimal (WP-H1);
    - `/v1/txs` lists compare row by row, aligned by request key (WP-H1);
    - a block's nested `transactions` compares as a multiset by request key when both sides list
      the whole block; a cut one is left as it is and the record is incomplete (WP-H1)."""
    info = {"cut": [], "one_side": [], "labels": []}
    if ep == "gql.fungibleChainAccounts":
        for b in (ab, sb):
            xs = (_data(b) or {}).get("fungibleChainAccounts")
            if isinstance(xs, list):
                xs.sort(key=lambda x: str((x or {}).get("chainId", "")).zfill(3) if isinstance(x, dict) else "")
    info["cut"] = page_cut(ab, "ours") + page_cut(sb, "stock")
    one = None
    if ep == "compat.v1txs":
        ka, kb = _v1txs_map(ab), _v1txs_map(sb)
        if ka is not None and kb is not None:
            ab, sb, one = ka, kb, (set(ka), set(kb))
    elif isinstance(_data(ab), dict) and isinstance(_data(sb), dict) and \
            "balance" in _data(ab) and "balance" in _data(sb):
        ka, kb = _balance_map(ab), _balance_map(sb)
        if ka is not None and kb is not None:
            one = (ka, kb)
        info["labels"] = _balance_text(ab if ka is not None else None, sb if kb is not None else None)
    if one:
        info["one_side"] = ["one-side:%s" % k for k in sorted(one[0] ^ one[1])]
    # Fix round 1 (I3): a nested list compares as a multiset only when both sides prove it whole;
    # one that may be a first page (no hasNextPage false, no totalCount equal to its rows) is cut.
    na, nb = dict(_nested_block_txs(ab)), dict(_nested_block_txs(sb))
    for side, n in (("ours", na), ("stock", nb)):
        for path in sorted(n):
            if not _proven_whole(n[path]) and not any(c.startswith("%s:%s " % (side, path)) for c in info["cut"]):
                info["cut"].append("%s:%s unproven" % (side, path))
    for path in set(na) & set(nb):
        if _proven_whole(na[path]) and _proven_whole(nb[path]):
            na[path]["edges"].sort(key=_tx_key)
            nb[path]["edges"].sort(key=_tx_key)
    return ab, sb, info


def tip_fields(node, idx, host, t0, t1):
    """The tip evidence of one stock-paired call, read from the sampled logs (no request): `tip`
    is the node's highest chain height at ours' read (`t0`) and at stock's answer (`t1`) for the
    tip-timing rule; `idx` is ours' and the stock host's own lastBlockHeight at those times."""
    out = {}
    p = node.tip_pair(t0, t1) if node else None
    if p:
        out["tip"] = p
    i = {}
    for key, name, t in (("ours", "ours", t0), ("stock", host, t1)):
        lg = idx.get(name)
        h = lg.at(t, nodecheck.TIP_ROW_AGE) if lg is not None else None
        if h is not None:
            i[key] = h
    if i:
        out["idx"] = i
    return out


# Harness hits that mean only part of a list was compared (a common prefix after a page cut,
# a capped count, a size limit): such a record is `incomplete`, never equal or accepted.
PARTIAL_HITS = {"/~graphql-page-cut", "/~graphql-totalcount-cap", "/~harness-limit/graphql-merged-bytes",
                "/~graphql-short-page"}
# WP-H1: a nested list pair-diff could not finish (graphql_scope UNFINISHED_PATH, with a `#why`
# suffix). Never known or accepted: alone it makes the record `incomplete`, never `diff`-free.
UNFINISHED_HIT = "/~graphql-h6-unfinished"


def _unfinished(e):
    p = e.get("path") or ""
    return p == UNFINISHED_HIT or p.startswith(UNFINISHED_HIT + "#")


def rule_entries(v):
    """(entries, complete) the suite rules judge: what pair-diff's allowlist left, uncut (`left`,
    H6 S1) when it was kept; else the display list, complete only when `left_total` says nothing
    was cut. An H5 record (`all` was the left list, `total` its length) and an H4 one (`total`
    only) are read as they were written; a record from before H4 is judged by allow.complete."""
    if isinstance(v.get("left"), list):
        return v["left"], len(v["left"]) == v.get("left_total", len(v["left"]))
    if isinstance(v.get("left_total"), int):
        ents = v.get("entries") or []
        return ents, v["left_total"] <= len(ents)
    if isinstance(v.get("all"), list):
        return v["all"], len(v["all"]) == v.get("total", len(v["all"]))
    ents = v.get("entries") or []
    if isinstance(v.get("total"), int):
        return ents, v["total"] <= len(ents)
    return ents, None


def stored_verdict(v, k, ids):
    """The `v` a record stores: display entries (at most MAX_SHOWN), the total, and the full list
    (H5 c: no cap), so a later re-referee sees every difference."""
    out = {"class": k, "ids": (v.get("ids") or []) + ids, "known": v.get("known_ids"), "open": v.get("open_ids") or [],
           "entries": (v.get("entries") or [])[:allow.MAX_SHOWN]}
    for k2 in ("cut", "one_side", "labels"):    # WP-H1: why incomplete; rows one side lists; text labels
        if v.get(k2):
            out[k2] = v[k2]
    for k in ("total", "left_total"):
        if isinstance(v.get(k), int):
            out[k] = v[k]
    for k in ("all", "left"):
        if isinstance(v.get(k), list):
            out[k] = v[k]
    ents, done = rule_entries(out)
    out["complete"] = bool(done) if done is not None else allow.complete({"entries": ents})
    return out


def total_count(body):
    """The first connection's totalCount of a GraphQL answer, else None."""
    if isinstance(body, dict) and isinstance(body.get("data"), dict):
        for x in body["data"].values():
            if isinstance(x, dict) and isinstance(x.get("totalCount"), int):
                return x["totalCount"]
    return None


def side_counts(ab, sb, ev):
    """H5 (c): per side, the rows compared, the answer's totalCount and the pages fetched (one per
    side for the compared answer; the orphan-evidence paging adds `evidence_pages`)."""
    out = {}
    for i, (k, body) in enumerate((("ours", ab), ("stock", sb))):
        out[k] = {"rows": count_rows(body), "totalCount": total_count(body), "pages": 1}
        if ev and isinstance(ev.get("ev_pages"), list):
            out[k]["evidence_pages"] = ev["ev_pages"][i]
    return out


def pack_bodies(ours_raw, stock_raw):
    """Both raw response bodies, zlib-compressed and base64 text (H5 c: re-referee input).
    A body is bytes (what Http.call returns), str or None (R-P5P9-180: bytes used to raise here)."""
    def enc(t):
        if t is None:
            t = b""
        elif isinstance(t, str):
            t = t.encode("utf-8")
        return base64.b64encode(zlib.compress(bytes(t), 9)).decode("ascii")
    return {"enc": "zlib+base64", "ours": enc(ours_raw), "stock": enc(stock_raw)}


def unpack_bodies(b):
    """(ours_raw, stock_raw) of pack_bodies, as text (bytes that are not UTF-8 decode with U+FFFD)."""
    def dec(t):
        return zlib.decompress(base64.b64decode(t)).decode("utf-8", "replace")
    return dec(b["ours"]), dec(b["stock"])


# H5 (c): what a non-equal stock-paired record must hold to be re-refereed offline.
EVIDENCE_KEYS = ("complete", "all", "tip")


def keep_evidence(row, ours_raw, stock_raw, ab, sb, ev):
    """Adds the re-referee evidence to a stock-paired record that is not `equal`: both raw bodies
    and the per-side counts (the full list, `complete`, tip, settle and fork reads are already in
    `v`, `tip` and `node`)."""
    if (row.get("v") or {}).get("class") in (None, "equal"):
        return
    row["counts"] = side_counts(ab, sb, ev)
    row["bodies"] = pack_bodies(ours_raw, stock_raw)


def missing_evidence(row):
    """The H5 (c) fields a non-equal stock-paired record lacks (empty: re-refereeable)."""
    v = row.get("v") or {}
    miss = [k for k in EVIDENCE_KEYS if (k in ("complete", "all") and k not in v) or (k == "tip" and k not in row)]
    return miss + [k for k in ("counts", "bodies") if k not in row]


def needs_kept_evidence(row):
    """A record keep_evidence had to complete: pair-diff ran on it (`rows`) and it is not equal."""
    return "rows" in row and (row.get("v") or {}).get("class") not in (None, "equal")


def evidence_gaps(out):
    """H6 (S9): Counter of the missing H5 (c) fields over the non-equal pair-diffed records of
    DIR/results.jsonl and sample.jsonl, plus `records` (how many lack any) and `checked`."""
    c = collections.Counter()
    for fname in ("results.jsonl", "sample.jsonl"):
        path = os.path.join(out, fname)
        if not os.path.exists(path):
            continue
        for l in open(path):
            try:
                r = json.loads(l)
            except Exception:
                continue
            if not needs_kept_evidence(r):
                continue
            c["checked"] += 1
            miss = missing_evidence(r)
            if miss:
                c["records"] += 1
                for k in miss:
                    c[k] += 1
    return c


def disk_estimate(rows, ratio=0.25):
    """Bytes run 4 adds for the H5 (c) bodies: every non-equal stock-paired record keeps both
    bodies, compressed at `ratio` and base64'd (4/3)."""
    raw = n = 0
    for r in rows:
        if (r.get("v") or {}).get("class") in (None, "equal") or "stock" not in r:
            continue
        n += 1
        raw += (r.get("ours") or {}).get("b") or 0
        raw += (r.get("stock") or {}).get("b") or 0
    return {"non_equal": n, "raw_bytes": raw, "body_bytes": math.ceil(raw * ratio * 4 / 3)}


def count_rows(body):
    """Rows of a list answer (GraphQL connection edges, a REST list or `items`), else 1."""
    if isinstance(body, list):
        return len(body)
    if isinstance(body, dict):
        if isinstance(body.get("items"), list):
            return len(body["items"])
        for x in (body.get("data") or {}).values() if isinstance(body.get("data"), dict) else []:
            if isinstance(x, dict) and isinstance(x.get("edges"), list):
                return len(x["edges"])
    return 1


STALL_HIT = "/~graphql-oracle-stall"


def stall_class(ep, cls, c, ents, done, ours_codes, vars_, node, tip, query, idx):
    """H8 (R-P5P9-174): the bin of a record with an oracle-stall hit. The stall hit and the
    partial-compare markers are not differences; every other entry pair-diff left must be
    explained by a suite rule for the record to be `stock-unavailable`. Any unexplained one (or a
    left list that was cut) makes it `diff` (`status` where pair-diff said so): a real difference
    is never binned as stock unavailable."""
    full = done if done is not None else allow.complete({"entries": ents})
    rest = [e for e in ents if e.get("path") != STALL_HIT and e.get("path") not in PARTIAL_HITS]
    if not full:
        return ("status" if c == "status" else "diff"), []
    if not rest:
        return "stock-unavailable", []
    k, ids = allow.classify({"ep": ep, "cls": cls, "ours_codes": ours_codes, "entries": rest, "vars": vars_,
                             "node": node, "tip": tip, "query": query, "idx": idx, "complete": True})
    if k in ("allowlisted", allow.UNAVAILABLE):
        return "stock-unavailable", ids
    return ("status" if c == "status" else "diff"), []


def verdict_class(ep, cls, v, ours_codes, vars_, node, tip=None, query=None, idx=None, cut=None):
    """Class and allowlist ids after the suite allowlist (allow.py). `tip`: the node's highest
    height at ours' read and at stock's ({"ours", "stock"}), for the tip-timing rule; `idx`: ours'
    and stock's own lastBlockHeight; `query`: the case's GraphQL document; `cut`: the lists
    normalize_pair found answered in part (else the record's stored `cut`)."""
    c = v.get("class")
    ents, done = rule_entries(v)
    # H7 (SHOULD-1, R-P5P9-173): the partial-compare and stall hits are looked for in every
    # entry (`all`), also the ones the Rust allowlist explained and dropped from `left`.
    seen = v["all"] if isinstance(v.get("all"), list) else ents
    # WP-H1: a page cut, hasNextPage on either side, or an unfinished nested list: incomplete.
    partial = bool(cut if cut is not None else v.get("cut")) or \
        any(e.get("path") in PARTIAL_HITS or _unfinished(e) for e in seen)
    unfinished = [e for e in ents if _unfinished(e)]
    # Fix round 1 (I2): differing HTTP statuses stay `status`, whatever else the entries say.
    if c == "diff" and unfinished and done is not False:
        rest = [e for e in ents if not _unfinished(e) and e.get("path") not in PARTIAL_HITS]
        if not rest:
            return "incomplete", []
        sub = dict(v, entries=rest, left=rest, left_total=len(rest))
        sub.pop("all", None)
        k, ids = verdict_class(ep, cls, sub, ours_codes, vars_, node, tip, query, idx, cut=["unfinished"])
        return ("incomplete", []) if k in ("incomplete", "allowlisted", "known-gap", "equal") else (k, ids)
    if any(e.get("path") == STALL_HIT for e in seen):
        return stall_class(ep, cls, c, ents, done, ours_codes, vars_, node, tip, query, idx)
    if c in ("equal", "allowlisted", "known-gap") and partial:
        return "incomplete", []         # owner H4 rule 1: only part of the list was compared
    if c == "allowlisted" and v.get("open_ids"):
        c = "diff"                      # owner H4 rule 3: an open (undocumented) entry accepts nothing
    if c in ("diff", "status"):
        ctx = {"ep": ep, "cls": cls, "ours_codes": ours_codes, "entries": ents, "vars": vars_, "node": node,
               "tip": tip, "query": query, "idx": idx}
        if done is not None:
            ctx["complete"] = done
        k, ids = allow.classify(ctx)
        if k in ("allowlisted", "known-gap") and partial:
            return "incomplete", []     # H7 SHOULD-1: incomplete is never accepted
        if k != "diff":
            return k, ids
    return c, []


# R-P5P9-180: a worker exception is a recorded outcome, never a silent drop.
HARNESS_ERR_LIMIT = 0.01        # stop the run above this share of a lane's cases ...
HARNESS_ERR_WINDOW = 200        # ... over the last this many completed cases of the lane ...
HARNESS_ERR_MIN = 100           # ... once at least this many are in the window
HARNESS_ERR_CLASS = "harness-error"


def is_harness_error(r):
    return (r.get("v") or {}).get("class") == HARNESS_ERR_CLASS


def case_cov(ep, lane, ours_only=False):
    """The coverage label Runner.one gives a case (None: it sets none, ours-only light case)."""
    if ep.split(".")[0] == "native":
        return "native"
    if ep in E.HEAVY_ENDPOINTS and lane is None:
        return "node"
    if not ours_only:
        return "stock-sample" if lane == "sample" else "stock"
    return None


def harness_error_row(case, lane, exc, ours_only=False):
    """The record of a case whose worker raised: id, endpoint, class, the exception type and
    message (cut at 300 chars). No request, no response."""
    ep = case.get("ep") or "?"
    row = {"id": case.get("id"), "ep": ep, "cls": case.get("cls"), "t": int(time.time()),
           "lane": lane or "main", "v": {"class": HARNESS_ERR_CLASS},
           "harness_error": {"type": type(exc).__name__, "msg": str(exc)[:300]}}
    cov = case_cov(ep, lane, ours_only)
    if cov:
        row["cov"] = cov
    return row


class LaneTally:
    """Per-lane count of cases submitted, written and failed in the worker, with the rolling
    window the harness-error stop rule reads. Thread-safe."""

    def __init__(self, name, limit=HARNESS_ERR_LIMIT, window=HARNESS_ERR_WINDOW, min_window=HARNESS_ERR_MIN):
        self.name, self.limit, self.min_window = name, limit, min_window
        self.lock = threading.Lock()
        self.submitted = self.written = self.errors = self.lost = 0
        self.recent = collections.deque(maxlen=window)

    def submit(self):
        with self.lock:
            self.submitted += 1

    def finish(self, ok):
        """A worker ended (`ok`: its record was written as a result). The stop message when the
        window rate is over the limit, else None."""
        with self.lock:
            if ok:
                self.written += 1
            else:
                self.errors += 1
            self.recent.append(0 if ok else 1)
            n, bad = len(self.recent), sum(self.recent)
            if n >= self.min_window and bad / n > self.limit:
                return ("lane %s: %d harness errors in the last %d cases (%.1f%% > %.0f%%)"
                        % (self.name, bad, n, 100.0 * bad / n, 100.0 * self.limit))
        return None

    def lose(self):
        """A worker raised and its error record could not be written either (or the future carried
        an exception that escaped the handler)."""
        with self.lock:
            self.lost += 1

    def reconciles(self):
        with self.lock:
            return self.submitted == self.written + self.errors + self.lost

    def final(self):
        """The stop message when the lane as a whole is over the limit or has a case unaccounted for."""
        with self.lock:
            if self.lost or self.submitted != self.written + self.errors + self.lost:
                return "lane %s: %d cases unaccounted for (submitted %d, written %d, harness-error %d)" % (
                    self.name, self.submitted - self.written - self.errors, self.submitted, self.written,
                    self.errors)
            if self.submitted and self.errors / self.submitted > self.limit:
                return "lane %s: %d of %d cases ended in harness-error (%.1f%% > %.0f%%)" % (
                    self.name, self.errors, self.submitted, 100.0 * self.errors / self.submitted,
                    100.0 * self.limit)
        return None

    def line(self):
        with self.lock:
            return "lane %s: submitted %d = written %d + harness-error %d + lost %d%s" % (
                self.name, self.submitted, self.written, self.errors, self.lost,
                "" if self.submitted == self.written + self.errors + self.lost else "  RECONCILIATION FAILED")

    def as_json(self):
        with self.lock:
            return {"lane": self.name, "submitted": self.submitted, "written": self.written,
                    "harness_error": self.errors, "lost": self.lost, "t": int(time.time())}


class Runner:
    def __init__(self, cfg, a):
        self.cfg, self.a = cfg, a
        self.abort, self.abort_msg = threading.Event(), None
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
        for h in self.oracles + list(self.rest_stock.values()):
            h.on_stop = self.stop_run
        self.pd = None if a.ours_only else PairDiff(cfg["diff"]["kda_compare"], cfg["diff"]["config"])
        nd = cfg.get("node") or {}
        self.node = nodecheck.Node(Http(nd.get("timeout_s", 30)), nd["url"], nd.get("network", "mainnet01"),
                                   min(float(nd.get("rps", 20)), 30.0),
                                   float(nd.get("max_wait_s", 30)),
                                   float(nd.get("settle_s", nodecheck.SETTLE_S))) if nd.get("url") else None
        self.tip_period = float(getattr(a, "tip_period", 1.0) or 1.0)
        self.idx = {"ours": nodecheck.TipLog()}
        self.tip_reads = OnePerKey()
        for h in self.oracles:
            self.idx[h.name] = nodecheck.TipLog()
        if self.node:
            self.node.sample_tips(self.tip_period)      # the node tip log: tip-timing rule, every paired row
        if not a.ours_only:
            threading.Thread(target=self.sample_indexer_tips, daemon=True).start()
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

    def sample_indexer_tips(self):
        """ours' and each stock GraphQL host's own lastBlockHeight, one read per `tip_period` each,
        through the same rate buckets (a read takes a slot); the logs back `tip_fields`. Never
        feeds the throttle windows. A failed read leaves a gap."""
        q = {"query": "{ lastBlockHeight }", "variables": {}}
        hosts = [("ours", None)] + [(h.name, h) for h in self.oracles]

        def one_read(name, h):
            if not self.tip_reads.start(name):
                return                      # the previous read of this host is still in flight (M1)
            try:
                if h is None:
                    r = self.ours_call("graphql", q)
                else:
                    if not h.healthy.is_set():
                        return
                    h.bucket.take()
                    r = h.send(q)
                if r["status"] == 200:
                    self.idx[name].add(time.time(), int(json.loads(r["raw"])["data"]["lastBlockHeight"]))
            except BudgetExhausted:
                raise
            except Exception:  # noqa: BLE001 - tip evidence only
                pass
            finally:
                self.tip_reads.done(name)
        while True:
            for name, h in hosts:
                threading.Thread(target=one_read, args=(name, h), daemon=True).start()
            time.sleep(self.tip_period)

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
        t_ours = time.time()
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
                t_stock = time.time()
                sb = parse(s["raw"])
                row["stock"] = {"host": s["host"], "st": s["status"], "ms": s["ms"], "err": s.get("err"),
                                "retry": s.get("attempt", 0), "b": len(s["raw"] or "")}
                row.update(tip_fields(self.node, self.idx, s["host"], t_ours, t_stock))
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
                    ab, sb, aligned = normalize_pair(ep, ab, sb)
                    row["rows"] = [count_rows(ab), count_rows(sb)]      # rows compared (ours, stock)
                    rq = req["path"] if fam != "graphql" else "%s %s" % (req.get("op", "op"), json.dumps(req["variables"]))
                    v = self.pd({"id": case["id"], "kind": "graphql" if fam == "graphql" else "rest",
                                 "request": rq, "max": maxh, "full": True,
                                 "ours": {"status": a["status"], "body": ab},
                                 "stock": {"status": s["status"], "body": sb}})
                    node = None
                    if v.get("class") in ("diff", "status") and self.node and ep in NODE_EPS and a["status"] == 200:
                        node = row["node"] = self.node.check(ep, vars_, ab)
                    if v.get("class") in ("diff", "status") and self.node and ep in nodecheck.EVIDENCE_EPS \
                            and a["status"] == 200 and nodecheck.needs_evidence(ep, rule_entries(v)[0]):
                        ev = self.orphan_evidence(case, vars_, self.stock_host(case, stock))
                        if ev:
                            node = row["node"] = dict(node or {}, **ev)
                    k, ids = verdict_class(ep, case["cls"], v, codes, vars_, node, row.get("tip"),
                                           req.get("query"), row.get("idx"), cut=aligned["cut"])
                    row["v"] = stored_verdict(dict(v, **{x: aligned[x] for x in ("cut", "one_side", "labels") if aligned[x]}),
                                              k, ids)
                    keep_evidence(row, a["raw"], s["raw"], ab, sb, node)
        (self.sample if lane == "sample" else self.res).write(json.dumps(row, separators=(",", ":")) + "\n")
        return row

    def orphan_evidence(self, case, vars_, host):
        """For a list where stock counts or lists more than ours: page both sides with the evidence
        query (rows with their block), take the rows only stock lists and ask the node about them
        (nodecheck.Node.evidence, at most EVIDENCE_READS reads). Without both lists complete (the
        page cap, a failed page) there is no `stock_orphans` and the rule does not fire."""
        ep = case["ep"]
        doc = nodecheck.evidence_doc(ep)
        v = nodecheck.evidence_vars(vars_)

        def fetch_ours(vv):
            r = self.ours_call("graphql", {"query": doc, "variables": vv})
            return parse(r["raw"]) if r["status"] == 200 else None

        def fetch_stock(vv):
            r = host.call(case["id"] + ":evidence", {"query": doc, "variables": vv}, True)
            return parse(r["raw"]) if r["status"] == 200 else None
        try:
            o_rows, o_done, o_pages = nodecheck.page_rows(fetch_ours, ep, v)
            s_rows, s_done, s_pages = nodecheck.page_rows(fetch_stock, ep, v) if host and o_done else ([], False, 0)
        except BudgetExhausted:
            raise
        except Exception as e:  # noqa: BLE001 - evidence only; the verdict stays what pair-diff gave
            return {"evidence": "error:" + type(e).__name__}
        info = {"ev_pages": [o_pages, s_pages]}
        if not (o_done and s_done):
            return dict(info, evidence="incomplete")
        extra = nodecheck.extra_rows(o_rows, s_rows)
        ours_only = len(nodecheck.extra_rows(s_rows, o_rows))       # rows only ours lists (H4, S1)
        if not extra:
            return dict(info, evidence="no-extra-rows", extra=0, ours_only=ours_only)
        return dict(info, evidence="node", ours_only=ours_only, **self.node.evidence(extra, nodecheck.EVIDENCE_READS))

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

    def flag(self):
        return "sample" if getattr(self.a, "sample_flag", False) else None

    def done_ids(self, name):
        """Ids with a written result. A harness-error record is not a result: resume runs it again."""
        done = set()
        fn = os.path.join(self.a.out, name)
        if os.path.exists(fn):
            for l in open(fn):
                try:
                    r = json.loads(l)
                    if not is_harness_error(r):
                        done.add(r["id"])
                except Exception:
                    pass
        return done

    def stop_run(self, msg):
        """Ends the run: no further case is submitted, the message is printed and run() returns 2."""
        if not self.abort.is_set():
            self.abort_msg = msg
            print("STOPPING THE RUN: %s" % msg, flush=True)
        self.abort.set()

    def safe_one(self, case, lane, tally):
        """`one` for a pool worker: an exception becomes a harness-error record (results.jsonl, or
        sample.jsonl for the sample lane), a line in the log and a count in the lane tally."""
        ok = True
        try:
            self.one(case, lane)
        except Exception as e:  # noqa: BLE001 - every worker failure is a recorded outcome
            ok = False
            print("HARNESS-ERROR %s %s lane=%s %s: %s" % (case.get("id"), case.get("ep"), lane or "main",
                                                          type(e).__name__, str(e)[:300]), flush=True)
            try:
                row = harness_error_row(case, lane, e, getattr(self.a, "ours_only", False))
                (self.sample if lane == "sample" else self.res).write(json.dumps(row, separators=(",", ":")) + "\n")
            except Exception as we:  # noqa: BLE001
                tally.lose()
                print("HARNESS-ERROR-UNRECORDED %s: %s: %s" % (case.get("id"), type(we).__name__, str(we)[:200]),
                      flush=True)
        msg = tally.finish(ok)
        if msg:
            self.stop_run(msg)

    def landed(self, f, tally, gate):
        """Done callback of every submitted case: frees the gate and reads the future, so an
        exception that escaped safe_one is counted and stops the run instead of vanishing."""
        gate.release()
        e = f.exception()
        if e is not None:
            tally.lose()
            print("HARNESS-ERROR-ESCAPED %s: %s" % (type(e).__name__, str(e)[:300]), flush=True)
            self.stop_run("lane %s: a worker exception escaped the handler (%s)" % (tally.name, type(e).__name__))

    def pool(self, cases, workers, lane, deadline=None, name="main"):
        """Run `cases` on `workers` threads; stops submitting at `deadline` (monotonic) or when the
        run is stopped. Prints and appends (DIR/lanes.jsonl) the lane's reconciliation: submitted
        = written + harness-error. A failure of the lane itself stops the run."""
        tally = LaneTally(name)
        n = skipped = 0
        try:
            gate = threading.Semaphore(workers * 2)
            with cf.ThreadPoolExecutor(workers) as ex:
                for c in cases:
                    if self.abort.is_set():
                        break
                    if deadline and time.monotonic() > deadline:
                        skipped += 1
                        continue
                    gate.acquire()
                    tally.submit()
                    f = ex.submit(self.safe_one, c, lane, tally)
                    f.add_done_callback(lambda _f: self.landed(_f, tally, gate))
                    n += 1
                    if n % 1000 == 0:
                        print("%s submitted %d" % (name, n), flush=True)
        except Exception as e:  # noqa: BLE001 - the lane itself (case source, executor) failed
            self.stop_run("lane %s failed: %s: %s" % (name, type(e).__name__, str(e)[:300]))
        fin = tally.final()
        if fin:
            self.stop_run(fin)
        print("%s done %d (past deadline: %d%s)" % (name, n, skipped, ", run stopped" if self.abort.is_set() else ""),
              flush=True)
        print(tally.line(), flush=True)
        try:
            with open(os.path.join(self.a.out, "lanes.jsonl"), "a") as lf:
                lf.write(json.dumps(tally.as_json(), separators=(",", ":")) + "\n")
        except OSError as e:
            print("lanes.jsonl not written: %s" % e, flush=True)

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
                cs = host_cases(load_cases(self.a.cases, hv, self.a.heavy_sample, self.flag()), i, len(self.oracles), sdone)
                lanes.append((h, cs, "sample", deadline))
                lt = [e for e in eps if e in E.LANE_LIGHT and e.startswith("gql.")]
                if lt:
                    cs2 = host_cases(load_cases(self.a.cases, lt, self.a.per_endpoint), i, len(self.oracles), done)
                    lanes.append((h, cs2, "light", deadline))
            for k, h in self.rest_stock.items():
                hv = [e for e in heavy if e.startswith("compat.") and dict((x[0], x[2]) for x in E.ENDPOINTS)[e] == k]
                cs = (c for c in load_cases(self.a.cases, hv, self.a.heavy_sample, self.flag()) if c["id"] not in sdone)
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
        if self.abort.is_set():
            print("STOPPED: %s" % self.abort_msg, flush=True)
            return 2
        print("all done", flush=True)
        return 0


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


def read_lanes(out):
    """The per-lane counts DIR/lanes.jsonl holds (one line per lane per invocation)."""
    fn = os.path.join(out, "lanes.jsonl")
    rows = []
    if os.path.exists(fn):
        for l in open(fn):
            try:
                rows.append(json.loads(l))
            except Exception:
                pass
    return rows


def reconciles(ln):
    return ln.get("submitted", 0) == ln.get("written", 0) + ln.get("harness_error", 0) + ln.get("lost", 0)


def reconcile_line(ln):
    return "%s: submitted %d = written %d + harness-error %d + lost %d%s" % (
        ln.get("lane"), ln.get("submitted", 0), ln.get("written", 0), ln.get("harness_error", 0),
        ln.get("lost", 0), "" if reconciles(ln) else "  RECONCILIATION FAILED")


def report(out):
    """DIR/report.md and DIR/summary.json from results.jsonl (main pass) and sample.jsonl
    (heavy endpoints against stock). Per endpoint and coverage source: cases, equal,
    allowlisted, known-gap, diff, status, stock unavailable, errors on each side, node and
    cross-checks, p50/p99 on both sides; then the open diff classes with example ids."""
    agg = collections.defaultdict(collections.Counter)
    lat = collections.defaultdict(lambda: {"ours": [], "stock": []})
    classes = collections.defaultdict(lambda: [0, []])
    allow_ids = collections.Counter()
    rows_cmp = collections.Counter()      # rows compared (ours + stock), per verdict class
    probs = collections.defaultdict(collections.Counter)
    hourly = collections.defaultdict(collections.Counter)
    node_why = collections.Counter()
    cov_tot = collections.defaultdict(collections.Counter)
    herr, good_ids, written = [], collections.defaultdict(set), collections.Counter()
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
            if is_harness_error(r):
                herr.append((fname, key, r))        # counted below, once its id is known to have no result
                continue
            good_ids[fname].add(r["id"])
            written[fname] += 1
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
                rows_cmp[k] += sum(x for x in (r.get("rows") or []) if isinstance(x, int))
                for i in v.get("ids") or []:
                    allow_ids[i] += 1
                if k in ("diff", "status"):
                    pats = sorted({"%s %s" % (re.sub(r"/\d+", "/N", e.get("path", "?")), e.get("kind"))
                                   for e in (v.get("entries") or [])[:4]})
                    ck = (ep, k, " + ".join(pats[:3]))
                    classes[ck][0] += 1
                    if len(classes[ck][1]) < 3:
                        classes[ck][1].append(r["id"])
    # R-P5P9-180: a harness-error record is a case that has no result; a later result for the same id
    # (a resumed run) settles it, and it is then counted as resolved, not open.
    open_err, resolved_err = {}, 0
    for fname, key, r in herr:
        if r["id"] in good_ids[fname]:
            resolved_err += 1
        else:
            open_err[(fname, r["id"])] = (key, r)
    err_by_ep = collections.Counter()
    for (fname, _), (key, r) in open_err.items():
        agg[key]["harness-error"] += 1
        err_by_ep[(r["ep"], (r.get("harness_error") or {}).get("type", "?"))] += 1
    cols = ["run", "equal", "allowlisted", "known-gap", "diff", "status", "incomplete", "stock-unavailable",
            "harness-error", "stock_err", "ours_err", "ours_429", "not_run", "node_ok", "node_bad", "node_skip",
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
            n = sum(t[k] for k in ("equal", "allowlisted", "known-gap", "diff", "status", "incomplete"))
            lines.append("- %s: %d verdicts, %d equal (%.2f%%), %d allowlisted, %d known-gap, %d diff, %d status, "
                         "%d incomplete; %d stock unavailable" % (
                             cov, n, t["equal"], 100.0 * t["equal"] / max(n, 1), t["allowlisted"], t["known-gap"],
                             t["diff"], t["status"], t["incomplete"], t["stock-unavailable"]))
    lanes = read_lanes(out)
    lines += ["", "## Harness errors and reconciliation (R-P5P9-180)", "",
              "- open harness-error cases (no result written): %d; resolved by a later result: %d" % (
                  len(open_err), resolved_err),
              "- records written: results.jsonl %d, sample.jsonl %d" % (written["results.jsonl"], written["sample.jsonl"])]
    for (ep, typ), n in sorted(err_by_ep.items()):
        lines.append("- harness-error %s %s: %d" % (ep, typ, n))
    lines += ["", "Per lane, last runs (submitted = written + harness-error):", ""]
    for ln in lanes:
        lines.append("- " + reconcile_line(ln))
    if not lanes:
        lines.append("- no lanes.jsonl: this run did not record per-lane counts")
    tot = collections.Counter()
    for ln in lanes:
        for k in ("submitted", "written", "harness_error", "lost"):
            tot[k] += ln.get(k, 0)
    if lanes:
        lines.append("- total: " + reconcile_line(dict(tot, lane="all lanes")))
    gaps = evidence_gaps(out)
    lines += ["", "## Re-referee evidence (H5 c, H6 S9)", "",
              "- non-equal paired records: %d; lacking evidence: %d%s" % (
                  gaps["checked"], gaps["records"],
                  " (%s)" % ", ".join("%s %d" % (k, n) for k, n in sorted(gaps.items())
                                      if k not in ("checked", "records")) if gaps["records"] else "")]
    lines += ["", "## Rows compared (ours + stock rows of every paired answer, by verdict)", ""]
    for k, n in sorted(rows_cmp.items()):
        lines.append("- %s: %d rows" % (k, n))
    lines.append("- total: %d rows" % sum(rows_cmp.values()))
    lines += ["", "## Allowlist hits (suite rules and harness ids)", ""]
    for k, n in allow_ids.most_common():
        lines.append("- %s: %d" % (k, n))
    by_class = collections.Counter()
    acc = {x["id"]: x["accept_class"] for x in allow.table()}
    for k, n in allow_ids.items():
        by_class[acc.get(k, "harness-id (compare/allowlist.toml)")] += n
    lines += ["", "## Allowlist hits by accept class", ""]
    for k, n in by_class.most_common():
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
               "allow": dict(allow_ids),
               "harness_errors": {"open": len(open_err), "resolved": resolved_err,
                                  "by_endpoint": {"%s|%s" % k: n for k, n in err_by_ep.items()},
                                  "lanes": lanes, "reconciled": all(reconciles(x) for x in lanes)},
               "evidence_gaps": dict(gaps), "hourly": {h: dict(v) for h, v in hourly.items()},
               "classes": [[ep, k, pat, n, ids] for (ep, k, pat), (n, ids) in classes.items()]},
              open(os.path.join(out, "summary.json"), "w"), indent=1)
    print("\n".join(lines[:6]))
    print("harness-error open %d, resolved %d; lanes reconciled: %s" % (
        len(open_err), resolved_err, "yes" if all(reconciles(x) for x in lanes) else "NO"))


SUITE_RULE_IDS = {r.id for r in allow.RULES} | {"accepted-tags"}    # and the H2 id the tag rules replaced


def rereferee_rows(rows, vars_of, query_of=None):
    """Re-classifies stored records with the current allowlist (allow.py), no request sent.
    `rows`: iterable of (source, record); `vars_of(id)`: the case's GraphQL variables or None;
    `query_of(id)`: its GraphQL document or None. Records stored as diff or status are judged,
    and so are records a suite rule accepted (their ids name one): the rule may be stricter now
    (H4), so they are judged again from pair-diff's own class (`status` where the HTTP statuses
    differ, else `diff`) and counted in `reopened` when no rule accepts them any more. The node
    check, tip pair and indexer heights the run stored on the record are the evidence."""
    query_of = query_of or (lambda i: None)
    before, after = collections.Counter(), collections.Counter()
    moved, left, reopened = collections.Counter(), collections.Counter(), collections.Counter()
    incomplete = collections.Counter()      # WP-H1: only part compared; never counted as explained
    origin = collections.defaultdict(lambda: [0, 0])      # rows imported from run 1 vs run 2's own
    for src, r in rows:
        v = dict(r.get("v") or {})
        suite_ids = [i for i in v.get("ids") or [] if i in SUITE_RULE_IDS]
        was = v.get("class")
        if was in ("allowlisted", "known-gap") and suite_ids:
            if not rule_entries(v)[0]:
                continue            # pair-diff's allowlist explained it all (an id both lists share)
            same = (r.get("ours") or {}).get("st") == (r.get("stock") or {}).get("st")
            v["class"] = "diff" if same else "status"
            v["ids"] = [i for i in v.get("ids") or [] if i not in SUITE_RULE_IDS]
        elif was not in ("diff", "status"):
            continue
        k, ids = verdict_class(r["ep"], r.get("cls"), v, (r.get("ours") or {}).get("gql_errors") or [],
                               vars_of(r["id"]) or {}, r.get("node"), r.get("tip"), query_of(r["id"]), r.get("idx"))
        if was not in ("diff", "status"):
            if k in ("diff", "status"):
                reopened[(r["ep"], ",".join(suite_ids))] += 1
                after[src] += 1
                left[r["ep"]] += 1
            elif k == "incomplete":
                incomplete[r["ep"]] += 1
            continue
        before[src] += 1
        og = origin[r.get("from") or "run2"]
        og[0] += 1
        if k in ("diff", "status"):
            after[src] += 1
            left[r["ep"]] += 1
            og[1] += 1
        elif k == "incomplete":
            incomplete[r["ep"]] += 1
        else:
            moved[(r["ep"], ",".join(ids), k)] += 1
    return {"before": dict(before), "after": dict(after), "moved": dict(moved), "left": dict(left),
            "incomplete": dict(incomplete), "total_incomplete": sum(incomplete.values()),
            "origin": dict(origin), "reopened": dict(reopened), "total_before": sum(before.values()),
            "total_after": sum(after.values())}


def rereferee_md(out, name):
    """Aggregate counts only: endpoints and rule ids, never a case id or a request."""
    lines = ["# Re-referee of %s with the current harness rules" % name, "",
             "Stored records only (diff and status rows of results.jsonl and sample.jsonl); no request sent. "
             "The rules read the node check and the tip pair stored on each record.", "",
             "- unexplained before: %d" % out["total_before"],
             "- unexplained after: %d" % out["total_after"],
             "- incomplete (only part compared; neither explained nor unexplained): %d" % out.get("total_incomplete", 0),
             "",
             "| source | before | after |", "|---|---|---|"]
    for src in sorted(out["before"]):
        lines.append("| %s | %d | %d |" % (src, out["before"][src], out["after"].get(src, 0)))
    lines += ["", "| origin | before | after |", "|---|---|---|"]
    for og, (b, a) in sorted(out["origin"].items()):
        lines.append("| %s | %d | %d |" % ("imported from run 1" if og == "run1" else "run 2's own rows", b, a))
    lines += ["", "## Moved to explained (endpoint, rule, class: rows)", ""]
    for (ep, ids, k), n in sorted(out["moved"].items(), key=lambda x: -x[1]):
        lines.append("- %s, %s, %s: %d" % (ep, ids, k, n))
    if not out["moved"]:
        lines.append("- none")
    lines += ["", "## Accepted before, open under the current rules (endpoint, rule: rows)", ""]
    for (ep, ids), n in sorted(out.get("reopened", {}).items(), key=lambda x: -x[1]):
        lines.append("- %s, %s: %d" % (ep, ids, n))
    if not out.get("reopened"):
        lines.append("- none")
    lines += ["", "## Incomplete, by endpoint (a page cut, hasNextPage, an unfinished nested list)", ""]
    for ep, n in sorted(out.get("incomplete", {}).items(), key=lambda x: -x[1]):
        lines.append("- %s: %d" % (ep, n))
    if not out.get("incomplete"):
        lines.append("- none")
    lines += ["", "## Still unexplained, by endpoint", ""]
    for ep, n in sorted(out["left"].items(), key=lambda x: -x[1]):
        lines.append("- %s: %d" % (ep, n))
    return "\n".join(lines) + "\n"


def rereferee(out, cases_dir, md_path):
    """run.py rereferee: DIR/results.jsonl and DIR/sample.jsonl re-classified, written to md_path."""
    want = collections.defaultdict(set)
    stored = []
    for fname, src in (("results.jsonl", "results"), ("sample.jsonl", "sample")):
        path = os.path.join(out, fname)
        if not os.path.exists(path):
            continue
        for l in open(path):
            if not any(x in l for x in ('"class":"diff"', '"class":"status"', '"class":"allowlisted"',
                                        '"class":"known-gap"')):
                continue
            try:
                r = json.loads(l)
            except Exception:
                continue
            v = r.get("v") or {}
            if v.get("class") in ("diff", "status") or any(i in SUITE_RULE_IDS for i in v.get("ids") or []):
                stored.append((src, r))
                want[r["ep"]].add(r["id"])
    vars_by, query_by = {}, {}
    for ep, ids in want.items():
        fn = os.path.join(cases_dir, ep + ".jsonl.gz")
        if not os.path.exists(fn):
            continue
        for l in gzip.open(fn, "rt"):
            c = json.loads(l)
            if c["id"] in ids:
                vars_by[c["id"]] = (c.get("req") or {}).get("variables") or {}
                query_by[c["id"]] = (c.get("req") or {}).get("query")
    res = rereferee_rows(stored, vars_by.get, query_by.get)
    with open(md_path, "w") as f:
        f.write(rereferee_md(res, os.path.basename(os.path.abspath(out))))
    json.dump({k: v for k, v in res.items() if k not in ("moved", "reopened")}, open(md_path + ".json", "w"))
    print("unexplained before %d after %d; incomplete %d" % (res["total_before"], res["total_after"],
                                                            res["total_incomplete"]))


def count_cases(cases_dir, heavy_sample=2000, sample_flag=False):
    """{ep: {"n": cases, "stock": cases sent to stock}}: light endpoints send every case; a heavy
    endpoint its flagged cases (`sample_flag`) or the first `heavy_sample`."""
    out = {}
    for ep in E.ENDPOINTS:
        fn = os.path.join(cases_dir, ep[0] + ".jsonl.gz")
        if not os.path.exists(fn):
            continue
        n = flagged = 0
        for l in gzip.open(fn, "rt"):
            n += 1
            if sample_flag and '"sample":true' in l:
                flagged += 1
        if ep[2] not in ("oracle", "cwd", "kdatxs") or ep[0] in E.STOCK_SKIP:
            s = 0
        elif ep[0] in E.HEAVY_ENDPOINTS:
            s = flagged if sample_flag else min(n, heavy_sample)
        else:
            s = n
        out[ep[0]] = {"n": n, "stock": s}
    return out


def estimate_model(cfg, counts):
    """Hours per lane from `{ep: {"n", "stock"}}`. Light stock cases run at min(rps, concurrency /
    latency) per host; heavy-lane cases (heavy endpoints' sample, LANE_LIGHT endpoints) at
    heavy_concurrency / latency per host, beside the light ones; ours carries every case (plus
    native cross-checks) at its rate. Latencies: `[estimate]` per backend, `[estimate.ep]` per
    endpoint (seconds, e.g. observed in run 2). Wall time is the slowest lane."""
    st, lat = cfg.get("stock", {}), cfg.get("estimate", {})
    per_ep = lat.get("ep", {})
    tables = {"oracle": st.get("oracle", []), "cwd": [st["cwd"]] if "cwd" in st else [],
              "kdatxs": [st["kdatxs"]] if "kdatxs" in st else []}
    host_of = {e[0]: e[2] for e in E.ENDPOINTS}
    work = collections.defaultdict(lambda: {"n": 0, "secs": 0.0})
    ours_n = search_n = 0
    for ep, c in counts.items():
        ours_n += c["n"] * (1 + (ep in E.NATIVE_CROSS))
        search_n += c["n"] * (ep in SEARCH_EPS)
        b = host_of.get(ep)
        if not c["stock"] or b not in tables:
            continue
        heavy = ep in E.HEAVY_ENDPOINTS or ep in E.LANE_LIGHT
        ls = per_ep.get(ep, lat.get(b + "_heavy_s", 3.0) if heavy else lat.get(b + "_light_s", lat.get(b + "_s", 0.6)))
        w = work[(b, "heavy" if heavy else "light")]
        w["n"] += c["stock"]
        w["secs"] += c["stock"] * ls
    lanes = {}
    for (b, kind), w in work.items():
        hosts = tables[b] or [{}]
        if kind == "light":
            conc = sum(h.get("concurrency", 64) for h in hosts)
            rps = sum(h.get("rps", 35) for h in hosts)
            lanes["%s-light" % b] = max(w["secs"] / conc, w["n"] / rps)
        else:
            lanes["%s-heavy" % b] = w["secs"] / sum(h.get("heavy_concurrency", 4) for h in hosts)
    paired = sum(x.get("rps", 35) for x in tables["oracle"]) + sum(st[k].get("rps", 30) for k in ("cwd", "kdatxs") if k in st)
    ours_rps = cfg["ours"].get("rps") or paired or 100
    lanes["ours"] = ours_n / ours_rps
    lanes["ours-search"] = search_n * lat.get("ours_search_s", 1.5) / 4
    hours = {k: round(v / 3600, 3) for k, v in lanes.items()}
    return {"requests": {"ours": ours_n, "stock": {k: sum(c["stock"] for e, c in counts.items() if host_of.get(e) == k)
                                                   for k in tables}},
            "ours_rps": ours_rps, "lane_hours": hours, "wall_hours": max(hours.values())}


def estimate(cfg, cases_dir, heavy_sample=2000, sample_flag=False):
    out = estimate_model(cfg, count_cases(cases_dir, heavy_sample, sample_flag))
    print(json.dumps(out, indent=1))
    return out


def lane_of(ep):
    """The lane `one()` runs an endpoint in when it is sent to stock."""
    if ep in E.HEAVY_ENDPOINTS:
        return "sample"
    return "light" if ep in E.LANE_LIGHT else None


def smoke(cfg, a):
    """A handful of cases through the real runner under a hard request budget (all requests to
    ours, stock and the node count, across invocations): validates the tip and evidence
    fetching. Prints one summary line per case; rows go to DIR/results.jsonl and sample.jsonl."""
    global REQ_BUDGET
    REQ_BUDGET = RequestBudget(a.budget, a.budget_file)
    a.ours_only = False
    want = list(a.ids)
    cases = {}
    for ep in sorted({i.rsplit(":", 1)[0] for i in want}):
        for l in gzip.open(os.path.join(a.cases, ep + ".jsonl.gz"), "rt"):
            c = json.loads(l)
            if c["id"] in want:
                cases[c["id"]] = c
    rn = Runner(cfg, a)
    try:
        rn.tip = rn.ours_tip()
        time.sleep(a.settle)
        for i in want:
            c = cases[i]
            row = rn.one(c, lane_of(c["ep"]))
            v, nd = row.get("v") or {}, row.get("node") or {}
            print(json.dumps({"id": i, "class": v.get("class"), "ids": v.get("ids"), "ours": row["ours"].get("st"),
                              "stock": (row.get("stock") or {}).get("st"), "tip": row.get("tip"), "idx": row.get("idx"),
                              "node": {k: nd[k] for k in ("ok", "evidence", "stock_orphans", "unresolved", "extra",
                                                          "reads", "ev_pages") if k in nd}}), flush=True)
    except BudgetExhausted as e:
        print("STOPPED: %s" % e, flush=True)
    print("requests used: %d of %s" % (REQ_BUDGET.used, a.budget), flush=True)
    os._exit(0)


def read_walk_cases(path, ids=None):
    """Page-walk cases: a .jsonl or .jsonl.gz of suite cases ({"id", "ep", "req": {"query",
    "variables"}}, optional "walk": {"path", "cursor_var"}), GraphQL only; `ids` keeps those."""
    op = gzip.open if path.endswith(".gz") else open
    out = []
    for line in op(path, "rt"):
        if not line.strip():
            continue
        c = json.loads(line)
        if ids and c["id"] not in ids:
            continue
        if not c.get("ep", "").startswith("gql.") or not (c.get("req") or {}).get("query"):
            raise ValueError("page-walk takes GraphQL cases only: %s" % c.get("id"))
        out.append(c)
    return out


def page_walk(cfg, a):
    """run.py page-walk: each case walked to its end on ours and on its stock host with each side's own
    cursors (pagewalk.py), at most --max-pages pages and --rps requests per second per side; rows
    compared as sets plus totalCount; one-side rows read on the node when [node] is configured.
    Writes DIR/pagewalk.jsonl (one record per case, row keys only) and prints the status counts.
    The stock host's 504 guard applies: a stop ends the walk."""
    global REQ_BUDGET
    if a.budget:
        if not a.budget_file:
            raise SystemExit("page-walk: --budget needs --budget-file")
        REQ_BUDGET = RequestBudget(a.budget, a.budget_file)
    cases = read_walk_cases(a.cases, set(a.ids or []))
    o, st, nd = cfg["ours"], cfg.get("stock", {}), cfg.get("node") or {}
    ours = Http(o.get("timeout_s", 30))
    os.makedirs(a.out, exist_ok=True)
    log = open(os.path.join(a.out, "stock-events.jsonl"), "a", buffering=1)
    http = Http(st.get("timeout_s", 180), insecure=st.get("insecure_tls", False))
    hosts = [StockHost(x["name"], "gql", x, http, log, start=False) for x in st.get("oracle", [])]
    if not hosts:
        raise SystemExit("page-walk: no [[stock.oracle]] host in the config")
    stop = threading.Event()
    for h in hosts:
        h.on_stop = lambda msg: (print("STOPPING THE WALK: %s" % msg, flush=True), stop.set())
    node = nodecheck.Node(Http(nd.get("timeout_s", 30)), nd["url"], nd.get("network", "mainnet01"),
                          min(float(nd.get("rps", 20)), 30.0), float(nd.get("max_wait_s", 30))) if nd.get("url") else None

    def fetch_ours(q, v):
        r = ours.call(o["graphql"], body={"query": q, "variables": v})
        return r["status"], parse(r["raw"])

    # Fix round 1 (I4): one pacer per side for the whole walk, so the per-side rate holds across cases.
    pacers = (pagewalk.Pacer(a.rps), pagewalk.Pacer(a.rps))
    with open(os.path.join(a.out, "pagewalk.jsonl"), "a", buffering=1) as f:
        counts = walk_cases(cases, hosts, fetch_ours, lambda h: walk_stock_fetch(h, stop), node, a.max_pages,
                            pacers, stop, lambda rec: f.write(json.dumps(rec, separators=(",", ":")) + "\n"))
    print("page-walk: %s" % json.dumps(dict(counts), sort_keys=True), flush=True)
    return 2 if stop.is_set() else 0


def walk_stock_fetch(h, stop, poll_s=1.0):
    """The page-walk's stock fetch for host `h` (Fix round 1, I5): it waits while the host's 504
    guard backs off (`healthy` cleared), sends nothing once the walk is stopped (0, None), and
    feeds every answer to the guard."""
    def fetch(q, v):
        while not h.healthy.wait(poll_s):
            if stop.is_set():
                return 0, None
        if stop.is_set():
            return 0, None
        r = h.send({"query": q, "variables": v})
        h.guard_504(time.monotonic(), r["status"])
        return r["status"], parse(r["raw"])
    return fetch


def walk_cases(cases, hosts, fetch_ours, fetch_stock_of, node, max_pages, pacers, stop, write):
    """Walks each case (pagewalk.run_case) with the same two pacers, case `gql.X:<n>` on host
    `n % hosts`; `write(rec)` each record. Ends at a stop or an exhausted budget. Returns the
    status counts."""
    counts = collections.Counter()
    for i, c in enumerate(cases):
        if stop.is_set():
            break
        try:
            n = int(c["id"].rsplit(":", 1)[1])
        except (IndexError, ValueError):
            n = i
        h = hosts[n % len(hosts)]
        try:
            rec = pagewalk.run_case(c, fetch_ours, fetch_stock_of(h), node, max_pages, pacers)
        except BudgetExhausted as e:
            print("STOPPED: %s" % e, flush=True)
            break
        rec["host"] = h.name
        write(rec)
        counts[rec["status"]] += 1
        print(json.dumps({"id": rec["id"], "status": rec["status"], "pages": rec["pages"], "rows": rec["rows"],
                          "totalCount": rec["totalCount"],
                          "one_side": [len(rec["one_side"]["ours"]), len(rec["one_side"]["stock"])]}), flush=True)
    return counts


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
    r.add_argument("--sample-flag", action="store_true",
                   help="heavy endpoints: only the cases flagged `sample` go to stock (run 3 case list)")
    r.add_argument("--tip-period", type=float, default=1.0, help="seconds between tip reads (node, ours, stock)")
    e = sub.add_parser("estimate")
    e.add_argument("--config", required=True)
    e.add_argument("--cases", required=True)
    e.add_argument("--heavy-sample", type=int, default=2000)
    e.add_argument("--sample-flag", action="store_true")
    sm = sub.add_parser("smoke")
    sm.add_argument("--config", required=True)
    sm.add_argument("--cases", required=True)
    sm.add_argument("--out", required=True)
    sm.add_argument("--ids", nargs="+", required=True)
    sm.add_argument("--budget", type=int, required=True, help="hard cap on all requests, across invocations")
    sm.add_argument("--budget-file", required=True)
    sm.add_argument("--tip-period", type=float, default=5.0)
    sm.add_argument("--settle", type=float, default=7.0, help="seconds of tip sampling before the first case")
    p = sub.add_parser("report")
    p.add_argument("--out", required=True)
    eg = sub.add_parser("evidence-gaps", help="count the non-equal records that lack re-referee evidence")
    eg.add_argument("--out", required=True)
    rr = sub.add_parser("rereferee")
    rr.add_argument("--out", required=True, help="a run directory (results.jsonl, sample.jsonl)")
    rr.add_argument("--cases", required=True)
    rr.add_argument("--md", required=True, help="the markdown file to write (aggregate counts only)")
    pw = sub.add_parser("page-walk", help="walk GraphQL lists to their end on both sides (WP-H1)")
    pw.add_argument("--config", required=True)
    pw.add_argument("--cases", required=True, help=".jsonl or .jsonl.gz of GraphQL suite cases")
    pw.add_argument("--out", required=True)
    pw.add_argument("--ids", nargs="*", help="only these case ids")
    pw.add_argument("--max-pages", type=int, default=pagewalk.MAX_PAGES, help="pages per side (at most 50)")
    pw.add_argument("--rps", type=float, default=pagewalk.RPS, help="requests per second per side (at most 2)")
    pw.add_argument("--budget", type=int, default=0, help="hard cap on all requests (0: none)")
    pw.add_argument("--budget-file", default=None)
    a = ap.parse_args()
    if a.cmd == "page-walk":
        sys.exit(page_walk(tomllib.load(open(a.config, "rb")), a))
    elif a.cmd == "run":
        sys.exit(Runner(tomllib.load(open(a.config, "rb")), a).run())
    elif a.cmd == "estimate":
        estimate(tomllib.load(open(a.config, "rb")), a.cases, a.heavy_sample, a.sample_flag)
    elif a.cmd == "smoke":
        smoke(tomllib.load(open(a.config, "rb")), a)
    elif a.cmd == "rereferee":
        rereferee(a.out, a.cases, a.md)
    elif a.cmd == "evidence-gaps":
        g = evidence_gaps(a.out)
        print("evidence gaps: %d of %d non-equal paired records %s" % (
            g["records"], g["checked"], json.dumps({k: n for k, n in sorted(g.items()) if k not in ("checked", "records")})))
    else:
        report(a.out)


if __name__ == "__main__":
    main()
