#!/usr/bin/env python3
"""Endpoint suite, runner and report (stdlib only).

  run.py run --config suite.toml --cases DIR --out DIR [--ours-only] [--per-endpoint N]
             [--endpoints EP ...] [--workers 64]
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
import endpoints as E  # noqa: E402

CRASH_STATUS = {502, 503}
# Ours runs every search under one gate: `search.concurrency` (4) at once, one per client
# (docs/api.md). Search cases hold one of `[ours] search_sources` (4 addresses) while in flight.
SEARCH_EPS = {"compat.search", "compat.pactid", "native.search_code", "native.search_events"}


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

    def call(self, url, path=None, body=None, src=None):
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
                c.request("GET", target, headers={"accept": "application/json"})
            else:
                c.request("POST", target, body=json.dumps(body).encode(),
                          headers={"content-type": "application/json"})
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


def gql_error_codes(body):
    if not isinstance(body, dict):
        return []
    out = []
    for e in body.get("errors") or []:
        if isinstance(e, dict):
            out.append(str((e.get("extensions") or {}).get("code") or "ERROR"))
    return out


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


class StockHost:
    """One stock backend with its adaptive throttle.

    Light cases take the rate bucket; heavy cases only a concurrency slot. Every 10 s the
    last 60 s are judged: errors over `err_limit` (2%) or p99 over `p99_limit_s` (5 s), or a
    crash, halve the rate (at most once per window); a healthy window at least
    `ramp_every_s` (120 s) after the last change adds `ramp_step` (5) r/s, up to the
    configured rate. A crash pauses the host: the cases in flight are recorded, a probe
    asks the host every 15 s, and once it answers the paused cases are retried once."""

    def __init__(self, name, kind, c, http, log):
        self.name, self.kind, self.c, self.http, self.log = name, kind, c, http, log
        self.url = c["url"]
        self.target = float(c.get("rps", 35))
        self.rate = self.target
        self.bucket = Bucket(self.rate)
        self.heavy = threading.Semaphore(c.get("heavy_concurrency", 4))
        self.light = threading.Semaphore(c.get("concurrency", 64))
        self.win = collections.deque()
        self.inflight = set()
        self.healthy = threading.Event()
        self.healthy.set()
        self.lock = threading.Lock()
        self.changed = time.monotonic()
        self.crashed = False
        self.err_limit = c.get("err_limit", 0.02)
        self.p99_limit = c.get("p99_limit_s", 5.0) * 1000
        self.ramp_every = c.get("ramp_every_s", 120)
        self.ramp_step = c.get("ramp_step", 5)
        self.max_height = c.get("max_height") or None
        threading.Thread(target=self.judge, daemon=True).start()

    def note(self, what, **kw):
        rec = dict(ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), host=self.name, event=what,
                   rate=round(self.rate, 1), **kw)
        self.log.write(json.dumps(rec) + "\n")
        print("throttle " + json.dumps(rec), flush=True)

    def set_rate(self, r, why):
        self.rate = max(1.0, min(self.target, r))
        self.bucket.gap = 1.0 / self.rate
        self.changed = time.monotonic()
        self.note("rate", why=why)

    def send(self, req):
        if self.kind == "gql":
            return self.http.call(self.url, body={"query": req["query"], "variables": req["variables"]})
        return self.http.call(self.url, path=req["path"])

    def call(self, cid, req, heavy):
        for attempt in (0, 1):
            self.healthy.wait()
            sem = self.heavy if heavy else self.light
            with sem:
                if not heavy:
                    self.bucket.take()
                with self.lock:
                    self.inflight.add(cid)
                r = self.send(req)
                with self.lock:
                    self.inflight.discard(cid)
                    self.win.append((time.monotonic(), r["status"] == 0 or r["status"] >= 500, r["ms"]))
            r["host"], r["attempt"] = self.name, attempt
            if not crash_like(r):
                return r
            self.crash(cid, r)
        return r

    def crash(self, cid, r):
        with self.lock:
            if not self.healthy.is_set():
                return
            self.healthy.clear()
            ids = sorted(self.inflight | {cid})
            self.crashed = True
        self.note("crash", status=r["status"], err=r.get("err"), inflight=ids)
        self.set_rate(self.rate / 2, "crash")
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

    def judge(self):
        while True:
            time.sleep(10)
            now = time.monotonic()
            with self.lock:
                while self.win and self.win[0][0] < now - 60:
                    self.win.popleft()
                w = list(self.win)
                crashed, self.crashed = self.crashed, False
            if not w or not self.healthy.is_set():
                continue
            er = sum(e for _, e, _ in w) / len(w)
            p99 = pct([m for _, _, m in w], .99)
            if (er > self.err_limit or p99 > self.p99_limit) and now - self.changed >= 60:
                self.set_rate(self.rate / 2, "errors %.1f%% p99 %d ms" % (er * 100, p99))
            elif not crashed and er <= self.err_limit and p99 <= self.p99_limit \
                    and self.rate < self.target and now - self.changed >= self.ramp_every:
                self.set_rate(self.rate + self.ramp_step, "healthy")


class Runner:
    def __init__(self, cfg, a):
        self.cfg, self.a = cfg, a
        o = cfg["ours"]
        srcs = o.get("sources") or []
        self.ours = Http(o.get("timeout_s", 30), srcs)
        st = cfg.get("stock", {})
        # Ours is paired with stock: by default it carries the stock side's total rate.
        paired = sum(x.get("rps", 35) for x in st.get("oracle", [])) + sum(
            st[k].get("rps", 30) for k in ("cwd", "kdatxs") if k in st)
        self.ours_bucket = Bucket(o.get("rps") or paired or 100)
        os.makedirs(a.out, exist_ok=True)
        self.res = open(os.path.join(a.out, "results.jsonl"), "a", buffering=1)
        self.events = open(os.path.join(a.out, "stock-events.jsonl"), "a", buffering=1)
        # The stock GraphQL hosts are reached by address (certificate for a hostname): opt-in.
        http = Http(st.get("timeout_s", 180), insecure=st.get("insecure_tls", False))
        self.oracles = [] if a.ours_only else [StockHost(x["name"], "gql", x, http, self.events)
                                               for x in st.get("oracle", [])]
        self.rest_stock = {} if a.ours_only else {k: StockHost(k, "rest", st[k], http, self.events)
                                                  for k in ("cwd", "kdatxs") if k in st}
        self.pd = None if a.ours_only else PairDiff(cfg["diff"]["kda_compare"], cfg["diff"]["config"])
        self.tip = None
        import queue
        self.search_src = queue.Queue()
        for x in o.get("search_sources") or ["127.0.0.%d" % i for i in (210, 211, 212, 213)]:
            self.search_src.put(x)

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

    def stock_call(self, case, fam, stock, req):
        ep = case["ep"]
        if ep in E.STOCK_SKIP:
            return None, "not-run: " + E.STOCK_SKIP[ep]
        if stock == "oracle" and self.oracles:
            h = self.oracles[int(case["id"].rsplit(":", 1)[1]) % len(self.oracles)]
        elif stock in self.rest_stock:
            h = self.rest_stock[stock]
        else:
            return None, "not-run: no stock configured"
        return h.call(case["id"], req, heavy_case(ep, req)), None

    def one(self, case):
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
        if fam == "graphql":
            codes = gql_error_codes(ab)
            row["ours"]["gql_errors"] = codes[:3]
            if any(c in ("GRAPHQL_PARSE_FAILED", "GRAPHQL_VALIDATION_FAILED") for c in codes) \
                    and case["cls"] not in ("invalid",):
                row["ours"]["note"] = "document-rejected"
        if fam == "native":
            row["inv"] = native_check(ep, case["cls"], req["path"], a["status"], ab)
            row["cross"] = self.cross(ep, req, a["status"], ab)
        elif not self.a.ours_only:
            s, why = self.stock_call(case, fam, stock, req)
            if s is None:
                row["stock"] = {"not_run": why}
            else:
                row["stock"] = {"host": s["host"], "st": s["status"], "ms": s["ms"], "err": s.get("err"),
                                "retry": s.get("attempt", 0)}
                if s["status"] and a["status"]:
                    rq = req["path"] if fam != "graphql" else "%s %s" % (req.get("op", "op"), json.dumps(req["variables"]))
                    v = self.pd({"id": case["id"], "kind": "graphql" if fam == "graphql" else "rest",
                                 "request": rq, "max": maxh,
                                 "ours": {"status": a["status"], "body": ab},
                                 "stock": {"status": s["status"], "body": parse(s["raw"])}})
                    row["v"] = {"class": v.get("class"), "ids": v.get("ids"), "known": v.get("known_ids"),
                                "entries": v.get("entries")}
        self.res.write(json.dumps(row, separators=(",", ":")) + "\n")
        return row

    def cross(self, ep, req, st, body):
        """Cross-check of a native answer against ours' other APIs (same fact)."""
        if st != 200 or not isinstance(body, dict):
            return None
        if ep == "native.tx" and body.get("request_key"):
            r = self.ours_call("compat", {"path": "/txs/tx?requestkey=" + urllib.parse.quote(body["request_key"])})
            c = parse(r["raw"])
            if r["status"] != 200 or not isinstance(c, dict):
                return {"ok": body.get("height") == 0, "why": "compat %d" % r["status"]}
            ok = c.get("height") == body.get("height") and c.get("blockHash") == body.get("block_hash")
            return {"ok": ok, "why": None if ok else "height/blockHash differ"}
        if ep == "native.block_by_height" and body.get("hash"):
            r = self.ours_call("native", {"path": "/api/v1/blocks/" + urllib.parse.quote(body["hash"], safe="")})
            c = parse(r["raw"])
            ok = r["status"] == 200 and isinstance(c, dict) and all(
                c.get(k) == body.get(k) for k in ("hash", "height", "chain", "parent", "payload_hash"))
            return {"ok": ok, "why": None if ok else "by-hash answer differs (%d)" % r["status"]}
        return None

    def run(self):
        done = set()
        fn = os.path.join(self.a.out, "results.jsonl")
        if os.path.exists(fn):
            for l in open(fn):
                try:
                    done.add(json.loads(l)["id"])
                except Exception:
                    pass
        eps = self.a.endpoints or [e[0] for e in E.ENDPOINTS]
        self.tip = self.ours_tip()
        print("tip=%d resume_skip=%d" % (self.tip, len(done)), flush=True)
        n = 0
        gate = threading.Semaphore(self.a.workers * 2)
        with cf.ThreadPoolExecutor(self.a.workers) as ex:
            for c in load_cases(self.a.cases, eps, self.a.per_endpoint):
                if c["id"] in done:
                    continue
                gate.acquire()
                f = ex.submit(self.one, c)
                f.add_done_callback(lambda _f: gate.release())
                n += 1
                if n % 1000 == 0:
                    print("submitted %d" % n, flush=True)
        print("done %d" % n, flush=True)


# ---------------------------------------------------------------------------------------

def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def report(out):
    agg = collections.defaultdict(lambda: collections.Counter())
    lat = collections.defaultdict(lambda: {"ours": [], "stock": []})
    diffs = collections.defaultdict(lambda: collections.defaultdict(list))
    probs = collections.defaultdict(collections.Counter)
    hourly = collections.defaultdict(collections.Counter)
    for l in open(os.path.join(out, "results.jsonl")):
        try:
            r = json.loads(l)
        except Exception:
            continue
        ep = r["ep"]
        hr = time.strftime("%Y-%m-%dT%H", time.gmtime(r.get("t", 0)))
        hourly[hr]["ours"] += 1 + (r.get("cross") is not None)
        if r.get("stock") and r["stock"].get("host"):
            hourly[hr][r["stock"]["host"]] += 1 + r["stock"].get("retry", 0)
        c = agg[ep]
        c["run"] += 1
        o = r["ours"]
        lat[ep]["ours"].append(o["ms"])
        ours_err = o["st"] == 0 or o["st"] >= 500
        c["ours_err"] += ours_err
        if o.get("note"):
            probs[ep][o["note"]] += 1
        for p in r.get("inv") or []:
            probs[ep][p.split(":")[0] if not p.startswith("note") else p] += 1
        x = r.get("cross")
        if x is not None:
            c["cross_ok" if x["ok"] else "cross_bad"] += 1
        s = r.get("stock")
        if s is None:
            continue
        if "not_run" in s:
            c["not_run"] += 1
            continue
        lat[ep]["stock"].append(s["ms"])
        if s["st"] == 0 or s["st"] >= 500:
            c["stock_err"] += 1
        v = r.get("v")
        if v:
            c[v["class"]] += 1
            if v["class"] in ("diff", "status"):
                for e in (v.get("entries") or [])[:3]:
                    key = re.sub(r"/\d+", "/N", e.get("path", "?"))
                    if len(diffs[ep][key]) < 3:
                        diffs[ep][key].append(r["id"])
    cols = ["run", "equal", "allowlisted", "known-gap", "diff", "status", "stock_err", "ours_err",
            "not_run", "cross_ok", "cross_bad"]
    lines = ["# Endpoint suite report", "",
             "| endpoint | " + " | ".join(cols) + " | ours p50/p99 ms | stock p50/p99 ms |",
             "|---" * (len(cols) + 3) + "|"]
    summ = {}
    for ep in sorted(agg):
        c = agg[ep]
        lo, ls = lat[ep]["ours"], lat[ep]["stock"]
        row = {k: c.get(k, 0) for k in cols}
        row.update({"ours_p50": pct(lo, .5), "ours_p99": pct(lo, .99), "stock_p50": pct(ls, .5),
                    "stock_p99": pct(ls, .99), "problems": dict(probs[ep])})
        summ[ep] = row
        lines.append("| %s | %s | %s/%s | %s/%s |" % (ep, " | ".join(str(row[k]) for k in cols),
                                                     row["ours_p50"], row["ours_p99"],
                                                     row["stock_p50"], row["stock_p99"]))
    backs = sorted({b for h in hourly.values() for b in h})
    lines += ["", "## Achieved r/s per backend per hour (UTC; partial hours divide by 3600 too)", "",
              "| hour | " + " | ".join(backs) + " |", "|---" * (len(backs) + 1) + "|"]
    for hr in sorted(hourly):
        lines.append("| %s | %s |" % (hr, " | ".join("%.1f" % (hourly[hr][b] / 3600) for b in backs)))
    lines += ["", "## Ours problems (invariants, rejected documents)", ""]
    for ep in sorted(probs):
        lines.append("- %s: %s" % (ep, dict(probs[ep])))
    lines += ["", "## Open diffs (path pattern: example case ids)", ""]
    for ep in sorted(diffs):
        for k, ids in sorted(diffs[ep].items()):
            lines.append("- %s `%s`: %s" % (ep, k, ", ".join(ids)))
    open(os.path.join(out, "report.md"), "w").write("\n".join(lines) + "\n")
    json.dump({"endpoints": summ, "hourly": {h: dict(v) for h, v in hourly.items()}},
              open(os.path.join(out, "summary.json"), "w"), indent=1)
    print("\n".join(lines[:len(summ) + 4]))


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
