#!/usr/bin/env python3
"""Real-traffic stress test of ours: a stepped ramp to find the capacity knee (stdlib only).

  stress.py --config suite.toml --out DIR [--rest-jsonl F | --rest-tsv F] [--gqlcap LOG ...]
            [--steps 25,50,100,200,400,800] [--step-secs 300] [--procs 4] [--plan]
            [--smoke N --smoke-rps R]

The mix replays the real requests: REST GETs weighted by their observed counts, GraphQL
POST bodies from the capture weighted by occurrence, and the GraphQL share of the total
taken from the two sources' observed rates (or `--gql-share`). Requests leave from many
loopback source addresses (`[ours] stress_sources`) so the per-IP limiter sees many clients.

Each step is open loop: requests start on schedule whatever the answers do. A step stops
early (and the ramp ends) when, after 30 s, the errors (transport, 5xx, 429) pass 1% or p99
passes 2 s, or the in-flight backlog passes 10 s of load. During the run the service's RSS
and NRestarts are sampled every 5 s (`[ours] unit`; read-only `systemctl show`).

The knee is the highest step that reached 95% of its rate with p99 < 500 ms, errors
< 0.1% and no restart. DIR gets steps.json (per step and route p50/p95/p99, errors,
achieved rate, RSS, restarts), watch.tsv and report.md. Ours only: never point it at a
stock or production service. `--plan` prints the mix and the schedule and sends nothing;
`--smoke N` sends N requests at `--smoke-rps` as a check of the driver.
"""

import argparse
import collections
import gzip
import http.client
import json
import multiprocessing as mp
import os
import random
import re
import subprocess
import sys
import threading
import time
import tomllib
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

ERR_P99_MS, ERR_RATE = 2000, 0.01
KNEE_P99_MS, KNEE_ERR, KNEE_ACHIEVED = 500, 0.001, 0.95


def rest_route(p):
    m = re.match(r"^/(v1/txs|txs/[a-z]+|coins|stats|api/v1/[a-z]+)", p)
    return (m.group(1) if m else "other").replace("txs/", "").replace("v1/txs", "v1txs")


def gql_route(q):
    m = re.search(r"\{\s*(?:\w+\s*:\s*)?(\w+)", q or "")
    return "gql:" + (m.group(1) if m else "?")


def build_mix(a):
    rest, rest_w, rest_span = [], [], None
    if a.rest_jsonl:
        first, last = None, None
        for l in open(a.rest_jsonl):
            d = json.loads(l)
            if d.get("method") != "GET":
                continue
            path = d["path"] + (("?" + d["query"]) if d.get("query") else "")
            rest.append(path)
            rest_w.append(int(d.get("count") or 1))
            first = min(first or d["first"], d["first"])
            last = max(last or d["last"], d["last"])
        if first and last:
            ts = [time.mktime(time.strptime(x, "%Y-%m-%dT%H:%M:%SZ")) for x in (first, last)]
            rest_span = max(1.0, ts[1] - ts[0])
    elif a.rest_tsv:
        for l in open(a.rest_tsv):
            parts = l.rstrip("\n").split("\t")
            if len(parts) == 3 and parts[1] == "GET":
                rest.append(parts[2])
                rest_w.append(int(parts[0]) if parts[0].isdigit() else 1)
    gq = collections.Counter()
    gfirst, glast, glines = None, None, 0
    for fn in a.gqlcap or []:
        for l in (gzip.open(fn, "rt", errors="replace") if fn.endswith(".gz") else open(fn, errors="replace")):
            m = re.search(r"(\d\d/\w{3}/\d{4}:\d\d:\d\d:\d\d)\.\d+ \d+ \d+ \d+ cl=(\d+) bl=(\d+) body=\"(.*)\"\s*$", l)
            if not m or m.group(2) != m.group(3):
                continue
            glines += 1
            t = time.mktime(time.strptime(m.group(1), "%d/%b/%Y:%H:%M:%S"))
            gfirst, glast = min(gfirst or t, t), max(glast or t, t)
            try:
                body = json.loads('"%s"' % m.group(4))
                one = json.loads(body)
            except Exception:
                continue
            if isinstance(one, dict) and str(one.get("query", "")).lstrip().startswith("subscription"):
                continue
            gq[body] += 1
    gql, gql_w = list(gq), list(gq.values())
    share = a.gql_share
    if share is None:
        rr = sum(rest_w) / rest_span if rest_span else None
        gr = glines / max(1.0, (glast or 0) - (gfirst or 0)) if glines else None
        share = (gr / (gr + rr)) if rr and gr else (0.5 if gql and rest else (1.0 if gql else 0.0))
    return {"rest": rest, "rest_w": rest_w, "gql": gql, "gql_w": gql_w, "gql_share": share}


def worker(cfg, mix, rate, secs, seed, srcs, q, stop):
    """One process: `rate` r/s for `secs`, results to `q` in one-second batches."""
    o = cfg["ours"]
    rnd = random.Random(seed)
    batch, lock = [], threading.Lock()
    inflight = [0]

    def send(i):
        if rnd.random() < mix["gql_share"] and mix["gql"]:
            body = rnd.choices(mix["gql"], mix["gql_w"])[0]
            url, path, route = o["graphql"], None, gql_route(json.loads(body).get("query"))
        else:
            path = rnd.choices(mix["rest"], mix["rest_w"])[0]
            url, body, route = (o["native"] if path.startswith("/api/") else o["compat"]), None, rest_route(path)
        u = urllib.parse.urlsplit(url)
        c = http.client.HTTPConnection(u.hostname, u.port, timeout=o.get("timeout_s", 30),
                                       source_address=(srcs[i % len(srcs)], 0))
        t = time.monotonic()
        st = 0
        try:
            if body is None:
                c.request("GET", u.path.rstrip("/") + path)
            else:
                c.request("POST", u.path or "/", body=body.encode(), headers={"content-type": "application/json"})
            r = c.getresponse()
            r.read()
            st = r.status
        except Exception:
            st = 0
        finally:
            c.close()
        with lock:
            batch.append((route, st, int((time.monotonic() - t) * 1000)))
            inflight[0] -= 1

    def flush():
        while not stop.is_set():
            time.sleep(1)
            with lock:
                b, batch[:] = list(batch), []
                n = inflight[0]
            q.put(("rows", b, (seed, n)))
    threading.Thread(target=flush, daemon=True).start()
    gap = 1.0 / rate
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=min(2000, max(16, int(rate * 4)))) as ex:
        i = 0
        while not stop.is_set():
            at = t0 + i * gap
            if at - t0 >= secs:
                break
            d = at - time.monotonic()
            if d > 0:
                time.sleep(d)
            with lock:
                inflight[0] += 1
            ex.submit(send, i)
            i += 1
    time.sleep(1.2)
    with lock:
        q.put(("rows", list(batch), (seed, inflight[0])))
    q.put(("end", i, None))


def watch(cfg, out, stop, samples):
    unit = cfg["ours"].get("unit")
    f = open(os.path.join(out, "watch.tsv"), "a", buffering=1)
    while not stop.is_set():
        rss, nr = None, None
        if unit:
            try:
                o = subprocess.run(["systemctl", "show", "-p", "MainPID", "-p", "NRestarts", unit],
                                   capture_output=True, text=True, timeout=10).stdout
                kv = dict(x.split("=", 1) for x in o.split() if "=" in x)
                nr = int(kv.get("NRestarts", "0"))
                for l in open("/proc/%s/status" % kv.get("MainPID", "0")):
                    if l.startswith("VmRSS:"):
                        rss = int(l.split()[1]) // 1024
            except Exception:
                pass
        samples.append((time.time(), rss, nr))
        f.write("%s\t%s\t%s\n" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), rss, nr))
        stop.wait(5)


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))]


def is_err(st):
    return st == 0 or st >= 500 or st == 429


def run_step(cfg, mix, rate, secs, procs, srcs, seed):
    q, stop = mp.Queue(), mp.Event()
    per = rate / procs
    ps = [mp.Process(target=worker, args=(cfg, mix, per, secs, seed * 100 + k, srcs[k::procs] or srcs, q, stop))
          for k in range(procs)]
    for p in ps:
        p.start()
    rows, ended, sent, t0, why = [], 0, 0, time.monotonic(), None
    backlog = 0
    inflight = {}
    while ended < procs:
        try:
            kind, a, b = q.get(timeout=5)
        except Exception:
            continue
        if kind == "end":
            ended += 1
            sent += a
            continue
        rows.extend(a)
        inflight[b[0]] = b[1]
        backlog = sum(inflight.values())
        el = time.monotonic() - t0
        if el >= 30 and not stop.is_set() and rows:
            recent = rows[-int(rate * 30):]
            er = sum(is_err(s) for _, s, _ in recent) / len(recent)
            p99 = pct([ms for _, _, ms in recent], .99)
            if er > ERR_RATE:
                why = "errors %.2f%%" % (er * 100)
            elif p99 > ERR_P99_MS:
                why = "p99 %d ms" % p99
            elif backlog > rate * 10:
                why = "backlog %d" % backlog
            if why:
                stop.set()
    stop.set()
    for p in ps:
        p.join(30)
    el = time.monotonic() - t0
    by = collections.defaultdict(list)
    for r, s, ms in rows:
        by[r].append((s, ms))
    routes = {}
    for r, xs in sorted(by.items()):
        ms = [m for _, m in xs]
        routes[r] = {"n": len(xs), "err": sum(is_err(s) for s, _ in xs), "p50": pct(ms, .5),
                     "p95": pct(ms, .95), "p99": pct(ms, .99),
                     "codes": dict(collections.Counter(s for s, _ in xs))}
    allms = [m for _, _, m in rows]
    return {"rate": rate, "secs": round(el, 1), "sent": sent, "done": len(rows),
            "achieved": round(len(rows) / max(el, 1e-9), 1),
            "err_rate": (sum(is_err(s) for _, s, _ in rows) / len(rows)) if rows else None,
            "p50": pct(allms, .5), "p95": pct(allms, .95), "p99": pct(allms, .99),
            "stopped_early": why, "routes": routes}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--rest-jsonl")
    ap.add_argument("--rest-tsv")
    ap.add_argument("--gqlcap", nargs="*")
    ap.add_argument("--gql-share", type=float)
    ap.add_argument("--steps", default="25,50,100,200,400,800")
    ap.add_argument("--step-secs", type=int, default=300)
    ap.add_argument("--procs", type=int, default=4)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--plan", action="store_true")
    ap.add_argument("--smoke", type=int)
    ap.add_argument("--smoke-rps", type=float, default=5.0)
    a = ap.parse_args()
    cfg = tomllib.load(open(a.config, "rb"))
    os.makedirs(a.out, exist_ok=True)
    mix = build_mix(a)
    srcs = cfg["ours"].get("stress_sources") or ["127.0.0.%d" % i for i in range(10, 138)]
    steps = [int(x) for x in a.steps.split(",")]
    plan = {"rest_paths": len(mix["rest"]), "rest_weight": sum(mix["rest_w"]),
            "gql_bodies": len(mix["gql"]), "gql_weight": sum(mix["gql_w"]),
            "gql_share": round(mix["gql_share"], 3), "sources": len(srcs), "steps": steps,
            "step_secs": a.step_secs,
            "max_requests": sum(steps) * a.step_secs,
            "max_minutes": len(steps) * a.step_secs / 60}
    print(json.dumps(plan, indent=1), flush=True)
    if a.plan:
        return
    stop = threading.Event()
    samples = []
    threading.Thread(target=watch, args=(cfg, a.out, stop, samples), daemon=True).start()
    results = []
    sched = [(a.smoke_rps, a.smoke / a.smoke_rps)] if a.smoke else [(s, a.step_secs) for s in steps]
    for k, (rate, secs) in enumerate(sched):
        t0 = time.time()
        r = run_step(cfg, mix, rate, secs, 1 if a.smoke else a.procs, srcs, a.seed + k)
        win = [s for s in samples if s[0] >= t0]
        r["rss_mb_max"] = max((s[1] for s in win if s[1]), default=None)
        nrs = [s[2] for s in samples if s[2] is not None]
        r["nrestarts"] = nrs[-1] if nrs else None
        r["restarted"] = len(set(s[2] for s in win if s[2] is not None)) > 1
        results.append(r)
        print(json.dumps({k: v for k, v in r.items() if k != "routes"}), flush=True)
        json.dump({"plan": plan, "steps": results}, open(os.path.join(a.out, "steps.json"), "w"), indent=1)
        if r["stopped_early"] or r["restarted"]:
            break
    stop.set()
    knee = None
    for r in results:
        if (r["achieved"] >= KNEE_ACHIEVED * r["rate"] and r["p99"] is not None and r["p99"] < KNEE_P99_MS
                and (r["err_rate"] or 0) < KNEE_ERR and not r["restarted"] and not r["stopped_early"]):
            knee = r["rate"]
    lines = ["# Stress report", "", "Knee (max sustained r/s, p99 < 500 ms, errors < 0.1%%): **%s**" % knee, "",
             "| step r/s | achieved | done | err %% | p50 | p95 | p99 | RSS MB max | restarts | stopped |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for r in results:
        lines.append("| %s | %s | %s | %.3f | %s | %s | %s | %s | %s | %s |" % (
            r["rate"], r["achieved"], r["done"], 100 * (r["err_rate"] or 0), r["p50"], r["p95"], r["p99"],
            r["rss_mb_max"], r["nrestarts"], r["stopped_early"] or ""))
    lines += ["", "## Per route and step (p50/p95/p99 ms, n, errors)", ""]
    for r in results:
        for rt, x in r["routes"].items():
            lines.append("- %s r/s %s: %s/%s/%s n=%d err=%d" % (r["rate"], rt, x["p50"], x["p95"], x["p99"], x["n"], x["err"]))
    open(os.path.join(a.out, "report.md"), "w").write("\n".join(lines) + "\n")
    print("knee=%s" % knee)


if __name__ == "__main__":
    main()
