#!/usr/bin/env python3
"""Hourly answer check after a cutover: replay a sample of the requests kda-indexer served
in the last hour against the stock services and the node, and check that the indexer follows
the chain. Python 3.11+ standard library only.

  hourly_check.py --config hourly.toml [--hour YYYY-MM-DDTHH] [--dry-run]

Inputs: the indexer's request log (`[api.request_log]`, operations.md "Request log"), the
`kda-compare` binary with a compare config whose pairs and targets name the stock services,
the indexer's native and admin listeners, and the node.

Steps:
  1. Read the hour's lines from the log and its rotations (`<log>`, `<log>.1`, ...).
  2. Sample: one request of every distinct query shape (route, query-parameter names; for
     GraphQL the operation's document and variable names), then up to `per_route` per route,
     at most `budget` in all. Identical requests are replayed once.
  3. Replay the sample through `kda-compare replay-batch`: every configured pair (ours against
     chainweb-data, kda-txs, the GraphQL indexer and the node), with the harness's normalisers,
     allowlist, node oracles (tx existence by /poll, block hashes, balances) and its per-target
     rate caps. A request that differs is asked again once after `recheck_delay_s`.
  4. Chain follow: our tip per chain (native `/status`) against the node's cut (height, lag, and
     the node's hash at our height), halted chains, restarts, 5xx and the balance drain from
     `/metrics`, compared with the previous hour's state.
  5. Print a compact summary and write `<out_dir>/hourly-<hour>.json`. Exit 0 = pass, 1 = fail,
     2 = the check itself could not run.

Classes (the G4 triage classes): equal; (a) allowlisted (a documented, accepted difference);
(d) known-gap; (c) timing: a tip race or a balance row read while the drain had it stale
(the harness's skew/reorg rules), or a difference the re-check no longer shows; stock-unavailable
(the stock side failed: not counted against us); ours-error; unexplained (open, the (b)
candidates). The replay happens after the request was served (up to an hour later), so it
judges the answer for the same request now, not the bytes served; tip-dependent answers move in
between, which is what the timing class and the re-check absorb.

Pass = no unexplained difference and no ours-error, every chain's lag within `ready_max_lag`
(now and in every logged request of the hour), no halted chain or hash mismatch, no restart,
and no route above `max_5xx_share` of 5xx answers in the hour.
"""

import argparse
import calendar
import collections
import hashlib
import json
import os
import random
import re
import subprocess
import sys
import time
import tomllib
import urllib.parse
import urllib.request

UA = "hourly-check/1.0"
DEFAULTS = {
    "log": "",
    "compare_bin": "kda-compare",
    "compare_config": "",
    "native_url": "http://127.0.0.1:31361",
    "admin_url": "http://127.0.0.1:31369",
    "node_url": "http://127.0.0.1:31351",
    "network": "mainnet01",
    "unit": "",
    "out_dir": ".",
    "budget": 2000,
    "per_route": 300,
    "concurrency": 2,
    "deadline_minutes": 50,
    "recheck_delay_s": 30,
    "ready_max_lag": 20,
    "max_5xx_share": 0.005,
    "min_5xx": 2,
}

# Harness outcome classes (kda-compare) -> hourly classes. Anything else is a difference.
EQUAL = {"match"}
ALLOWED = {"allowlisted"}
GAP = {"known-gap"}
TIMING = {"skew", "reorg", "oracle-tip"}
STOCK = {"b-error"}
NOT_COMPARED = {"no-oracle", "bad-request", "a-paused", "gen-error"}
OURS = {"a-error"}
# Worst first: an item takes the worst class of its pairs.
ORDER = ["ours-error", "unexplained", "timing", "known-gap", "allowlisted", "equal",
         "stock-unavailable", "not-compared"]
LETTER = {"allowlisted": "a", "known-gap": "d", "timing": "c", "unexplained": "b?"}


def hour_window(hour):
    """(start_ms, end_ms, label) of `YYYY-MM-DDTHH` (UTC), or of the last full hour."""
    if hour:
        t = calendar.timegm(time.strptime(hour, "%Y-%m-%dT%H"))
    else:
        t = (int(time.time()) // 3600 - 1) * 3600
    return t * 1000, (t + 3600) * 1000, time.strftime("%Y-%m-%dT%H", time.gmtime(t))


def log_files(path):
    """The current log and its rotations that exist, oldest first."""
    out, i = [], 1
    while os.path.exists("%s.%d" % (path, i)):
        out.append("%s.%d" % (path, i))
        i += 1
    out.reverse()
    if os.path.exists(path):
        out.append(path)
    return out


def read_window(paths, start_ms, end_ms, counts):
    for p in paths:
        with open(p, encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    e = json.loads(line)
                except ValueError:
                    counts["bad-line"] += 1
                    continue
                if start_ms <= e.get("t", 0) < end_ms:
                    yield e


def gql_request(e):
    """(query, variables, operationName) of a logged GraphQL request, or (None, reason)."""
    if e.get("method") == "POST":
        if e.get("gql_cut"):
            return None, "body-cut"
        g = e.get("gql")
        if isinstance(g, list):
            return None, "batch"
        if not isinstance(g, dict) or not isinstance(g.get("query"), str):
            return None, "no-body"
        return (g["query"], g.get("variables") or {}, g.get("operationName")), None
    q = urllib.parse.parse_qs(urllib.parse.urlsplit(e.get("uri", "")).query)
    if "query" not in q:
        return None, "no-query"
    try:
        v = json.loads(q["variables"][0]) if "variables" in q else {}
    except ValueError:
        return None, "bad-variables"
    return (q["query"][0], v if isinstance(v, dict) else {}, (q.get("operationName") or [None])[0]), None


def doc_key(query):
    return hashlib.sha1(" ".join(query.split()).encode()).hexdigest()[:10]


def route_of(e):
    """(route key, shape, replay item without id) of a log line, or (route key, None, reason)."""
    listener = e.get("listener")
    if listener == "graphql":
        r, why = gql_request(e)
        if r is None:
            return "graphql", None, why
        query, variables, op = r
        root = re.search(r"\{\s*([A-Za-z_]\w*)", query)
        rkey = "graphql:" + (root.group(1) if root else "?")
        shape = "%s|%s|%s|%s" % (rkey, op or "", doc_key(query), ",".join(sorted(variables)))
        return rkey, shape, {"query": query, "variables": variables}
    if listener != "compat":
        return str(listener), None, "listener"
    if e.get("method") not in ("GET", "HEAD"):
        return "compat:" + (e.get("route") or "?"), None, "method"
    if e.get("uri_cut"):
        return "compat:" + (e.get("route") or "?"), None, "uri-cut"
    u = urllib.parse.urlsplit(e["uri"])
    route = e.get("route") or u.path
    keys = sorted({k for k, _ in urllib.parse.parse_qsl(u.query, keep_blank_values=True)})
    return "compat:" + route, "compat:%s?%s" % (route, ",".join(keys)), {"path": e["uri"]}


def sample(entries, budget, per_route, seed):
    """Every distinct shape once (most frequent first), then up to `per_route` per route, at
    most `budget`; identical requests once. Returns (items, routes, skipped reasons)."""
    by_shape = collections.defaultdict(list)
    routes = collections.Counter()
    skipped = collections.Counter()
    for e in entries:
        rkey, shape, it = route_of(e)
        routes[rkey] += 1
        if shape is None:
            skipped["%s %s" % (rkey, it)] += 1
            continue
        by_shape[shape].append((rkey, it))
    rng = random.Random(seed)
    chosen, seen, per = [], set(), collections.Counter()

    def take(rkey, it):
        k = json.dumps(it, sort_keys=True)
        if k in seen or len(chosen) >= budget:
            return False
        seen.add(k)
        per[rkey] += 1
        chosen.append(dict(it, id="%d" % len(chosen), route=rkey))
        return True

    for shape in sorted(by_shape, key=lambda s: (-len(by_shape[s]), s)):
        rkey, it = rng.choice(by_shape[shape])
        take(rkey, it)
    pool = collections.defaultdict(list)
    for shape, xs in by_shape.items():
        for rkey, it in xs:
            pool[rkey].append(it)
    for rkey in sorted(pool):
        xs = pool[rkey]
        rng.shuffle(xs)
        for it in xs:
            if per[rkey] >= per_route or len(chosen) >= budget:
                break
            take(rkey, it)
    return chosen, routes, skipped


def item_class(line):
    """The hourly class of one `replay-batch` verdict line."""
    if line.get("skip"):
        return "not-compared"
    classes = set()
    for o in line.get("outcomes") or []:
        c = o.get("class")
        if c in EQUAL:
            classes.add("equal")
        elif c in ALLOWED:
            classes.add("allowlisted")
        elif c in GAP:
            classes.add("known-gap")
        elif c in TIMING:
            classes.add("timing")
        elif c in STOCK:
            classes.add("stock-unavailable")
        elif c in NOT_COMPARED:
            classes.add("not-compared")
        elif c in OURS:
            classes.add("ours-error")
        else:
            classes.add("unexplained")
    for c in ORDER:
        if c in classes:
            return c
    return "not-compared"


def signature(rkey, line):
    """Route plus the JSON paths that differed: what makes a difference 'new'."""
    paths = set()
    for o in line.get("outcomes") or []:
        for d in ((o.get("record") or {}).get("diff") or [])[:6]:
            paths.add(re.sub(r"/\d+", "/N", str(d.get("path", "?"))) + " " + str(d.get("kind", "")))
        if o.get("class") not in EQUAL | ALLOWED:
            paths.add("class " + str(o.get("class")))
    return rkey + " :: " + " + ".join(sorted(paths))


def replay(cfg, items):
    """Runs `kda-compare replay-batch` on `items`: {id: verdict line}. Stops feeding new work
    at the deadline by sending only what fits: the caller passes items in priority order."""
    cmd = [cfg["compare_bin"], "--config", cfg["compare_config"], "replay-batch",
           "--concurrency", str(cfg["concurrency"])]
    inp = "".join(json.dumps({k: v for k, v in it.items() if k != "route"}) + "\n" for it in items)
    try:
        p = subprocess.run(cmd, input=inp, capture_output=True, text=True,
                           timeout=cfg["deadline_minutes"] * 60)
    except subprocess.TimeoutExpired as ex:
        out = ex.stdout.decode() if isinstance(ex.stdout, bytes) else (ex.stdout or "")
        return parse_verdicts(out), "deadline"
    if p.returncode != 0:
        return parse_verdicts(p.stdout), "kda-compare exit %d: %s" % (p.returncode, p.stderr.strip()[-300:])
    return parse_verdicts(p.stdout), None


def parse_verdicts(text):
    out = {}
    for line in text.splitlines():
        try:
            v = json.loads(line)
        except ValueError:
            continue
        if isinstance(v, dict) and "id" in v:
            out[str(v["id"])] = v
    return out


def get_json(url, accept="application/json", timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": accept})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def get_text(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()


METRIC = re.compile(r'^([a-zA-Z_:][\w:]*)(?:\{(.*)\})?\s+(\S+)')


def parse_metrics(text):
    """[(name, {label: value}, float)] of an OpenMetrics text."""
    out = []
    for line in text.splitlines():
        m = METRIC.match(line)
        if not m or line.startswith("#"):
            continue
        labels = dict(re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', m.group(2) or ""))
        try:
            out.append((m.group(1), labels, float(m.group(3))))
        except ValueError:
            pass
    return out


def metrics_facts(samples):
    f = {"http_5xx": collections.Counter(), "http_total": collections.Counter(),
         "halted": [], "lag": {}, "dirty_rows": None, "reqlog": {}}
    for name, lab, v in samples:
        if name == "kdx_http_requests_total" and lab.get("api") in ("compat", "graphql"):
            k = "%s %s" % (lab["api"], lab.get("route"))
            f["http_total"][k] += v
            if lab.get("status", "").startswith("5"):
                f["http_5xx"][k] += v
        elif name == "kdx_chain_halted" and v:
            f["halted"].append(lab.get("chain"))
        elif name == "kdx_chain_lag_blocks":
            f["lag"][lab.get("chain")] = v
        elif name == "kdx_balance_dirty_rows":
            f["dirty_rows"] = (f["dirty_rows"] or 0) + v
        elif name == "kdx_request_log_lines_total":
            f["reqlog"][lab.get("result")] = v
    return f


def chain_follow(cfg):
    """Ours (native /status) against the node's cut, per chain: lag and hash at our height."""
    res = {"chains": {}, "errors": []}
    try:
        st = get_json(cfg["native_url"] + "/status")
        cut = get_json("%s/chainweb/0.0/%s/cut" % (cfg["node_url"], cfg["network"]))
    except Exception as ex:  # noqa: BLE001
        res["errors"].append("status/cut: %s" % ex)
        return res
    node = {str(c): v for c, v in (cut.get("hashes") or {}).items()}
    for c in st.get("chains") or []:
        ch = str(c.get("chain"))
        n = node.get(ch) or {}
        ours_h, ours_hash = c.get("height"), c.get("hash")
        row = {"ours": ours_h, "node": n.get("height"), "halted": bool(c.get("halted"))}
        if ours_h is not None and n.get("height") is not None:
            row["lag"] = n["height"] - ours_h
            if ours_hash and n["height"] == ours_h:
                row["hash_ok"] = ours_hash == n.get("hash")
            elif ours_hash and n["height"] > ours_h:
                try:
                    h = get_json("%s/chainweb/0.0/%s/chain/%s/header?minheight=%d&maxheight=%d"
                                 % (cfg["node_url"], cfg["network"], ch, ours_h, ours_h),
                                 accept="application/json;blockheader-encoding=object")
                    row["hash_ok"] = ours_hash in {x.get("hash") for x in h.get("items") or []}
                except Exception as ex:  # noqa: BLE001
                    res["errors"].append("header %s@%s: %s" % (ch, ours_h, ex))
        res["chains"][ch] = row
    return res


def unit_state(unit):
    if not unit:
        return None
    try:
        out = subprocess.run(["systemctl", "show", unit, "-p", "NRestarts", "-p", "MainPID",
                              "-p", "ActiveState"], capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    return dict(x.split("=", 1) for x in out.splitlines() if "=" in x)


def served_stats(entries):
    by = collections.defaultdict(lambda: {"n": 0, "5xx": 0, "429": 0, "ms": []})
    lag = None
    for e in entries:
        rkey = "%s %s" % (e.get("listener"), e.get("route") or "unmatched")
        s = by[rkey]
        s["n"] += 1
        st = int(e.get("status", 0))
        s["5xx"] += 500 <= st < 600
        s["429"] += st == 429
        s["ms"].append(float(e.get("ms", 0)))
        if e.get("lag_max") is not None:
            lag = max(lag or 0, e["lag_max"])
    out = {}
    for k, s in by.items():
        ms = sorted(s["ms"])
        out[k] = {"n": s["n"], "5xx": s["5xx"], "429": s["429"],
                  "p50_ms": ms[len(ms) // 2] if ms else None,
                  "p99_ms": ms[min(len(ms) - 1, int(len(ms) * 0.99))] if ms else None}
    return out, lag


def verdict(rep, cfg):
    """(pass, reasons) of a report."""
    why = []
    c = rep["replay"]["classes"]
    if c.get("unexplained"):
        why.append("%d unexplained difference(s)" % c["unexplained"])
    if c.get("ours-error"):
        why.append("%d ours-error" % c["ours-error"])
    if rep["replay"].get("error"):
        why.append("replay: " + rep["replay"]["error"])
    lim = cfg["ready_max_lag"]
    for ch, r in rep["follow"]["chains"].items():
        if r.get("lag") is not None and r["lag"] > lim:
            why.append("chain %s lag %d > %d" % (ch, r["lag"], lim))
        if r.get("hash_ok") is False:
            why.append("chain %s hash differs from the node at height %s" % (ch, r["ours"]))
        if r.get("halted"):
            why.append("chain %s halted" % ch)
    if rep["follow"]["errors"]:
        why.append("follow check incomplete: " + "; ".join(rep["follow"]["errors"])[:300])
    if rep["served_lag_max"] is not None and rep["served_lag_max"] > lim:
        why.append("served with lag %d > %d during the hour" % (rep["served_lag_max"], lim))
    if rep.get("restarts"):
        why.append("restart: " + rep["restarts"])
    for k, s in rep["served"].items():
        if s["5xx"] >= cfg["min_5xx"] and s["5xx"] > cfg["max_5xx_share"] * s["n"]:
            why.append("%s: %d of %d answers 5xx" % (k, s["5xx"], s["n"]))
    if rep["lines"] == 0:
        why.append("no request log lines in the hour (log off, wrong path, or no traffic)")
    return not why, why


def run(cfg, label, start_ms, end_ms, dry):
    counts = collections.Counter()
    entries = list(read_window(log_files(cfg["log"]), start_ms, end_ms, counts))
    items, routes, skipped = sample(entries, cfg["budget"], cfg["per_route"], label)
    served, lag = served_stats(entries)
    rep = {"hour": label, "lines": len(entries), "bad_lines": counts["bad-line"],
           "routes": dict(routes), "skipped": dict(skipped), "sampled": len(items),
           "served": served, "served_lag_max": lag,
           "replay": {"classes": {}, "per_route": {}, "examples": [], "not_run": 0}}
    state_path = os.path.join(cfg["out_dir"], "hourly-state.json")
    try:
        with open(state_path) as f:
            state = json.load(f)
    except (OSError, ValueError):
        state = {}
    seen = set(state.get("signatures", []))
    if not dry and items:
        got, err = replay(cfg, items)
        diffs = [it for it in items if it["id"] in got and item_class(got[it["id"]]) == "unexplained"]
        if diffs:
            time.sleep(cfg["recheck_delay_s"])
            again, _ = replay(cfg, diffs)
            for it in diffs:
                v = again.get(it["id"])
                if v is not None and item_class(v) != "unexplained":
                    got[it["id"]] = dict(got[it["id"]], recheck=item_class(v))
        cls = collections.Counter()
        per = collections.defaultdict(collections.Counter)
        for it in items:
            v = got.get(it["id"])
            c = "not-run" if v is None else item_class(v)
            if v is not None and v.get("recheck"):
                c = "timing"
            cls[c] += 1
            per[it["route"]][c] += 1
            if c == "unexplained":
                sig = signature(it["route"], v)
                new = sig not in seen
                seen.add(sig)
                if len(rep["replay"]["examples"]) < 20:
                    rec = next((o.get("record") for o in v.get("outcomes") or [] if o.get("record")), {}) or {}
                    rep["replay"]["examples"].append({
                        "route": it["route"], "new": new, "signature": sig,
                        "request": it.get("path") or (it.get("query", "")[:200] + " " + json.dumps(it.get("variables"))[:200]),
                        "pair": "%s/%s" % (rec.get("a_target"), rec.get("b_target")),
                        "diff": json.dumps(rec.get("diff"))[:600]})
        rep["replay"]["classes"] = dict(cls)
        rep["replay"]["per_route"] = {k: dict(v) for k, v in per.items()}
        rep["replay"]["not_run"] = cls.get("not-run", 0)
        if err and err != "deadline":
            rep["replay"]["error"] = err
        elif err == "deadline":
            rep["replay"]["deadline_hit"] = True
    rep["follow"] = chain_follow(cfg)
    try:
        m = metrics_facts(parse_metrics(get_text(cfg["admin_url"] + "/metrics")))
    except Exception as ex:  # noqa: BLE001
        m = None
        rep["follow"]["errors"].append("metrics: %s" % ex)
    unit = unit_state(cfg["unit"])
    prev = state.get("last") or {}
    if m:
        rep["metrics"] = {"halted": m["halted"], "max_lag": max(m["lag"].values(), default=None),
                          "dirty_rows": m["dirty_rows"],
                          "dirty_rows_delta": (m["dirty_rows"] - prev["dirty_rows"])
                          if m["dirty_rows"] is not None and prev.get("dirty_rows") is not None else None,
                          "request_log": m["reqlog"],
                          "http_5xx_delta": {k: v - prev.get("http_5xx", {}).get(k, 0)
                                             for k, v in m["http_5xx"].items()
                                             if v - prev.get("http_5xx", {}).get(k, 0) > 0}}
        for ch in m["halted"]:
            rep["follow"]["chains"].setdefault(str(ch), {})["halted"] = True
    rep["unit"] = unit
    if unit and prev.get("unit"):
        p = prev["unit"]
        if unit.get("NRestarts") != p.get("NRestarts") or unit.get("MainPID") != p.get("MainPID"):
            rep["restarts"] = "NRestarts %s -> %s, MainPID %s -> %s" % (
                p.get("NRestarts"), unit.get("NRestarts"), p.get("MainPID"), unit.get("MainPID"))
    if unit and unit.get("ActiveState") not in (None, "active"):
        rep["restarts"] = "unit %s" % unit.get("ActiveState")
    rep["pass"], rep["reasons"] = verdict(rep, cfg)
    if not dry:
        state = {"signatures": sorted(seen)[-5000:],
                 "last": {"hour": label, "unit": unit,
                          "dirty_rows": m["dirty_rows"] if m else None,
                          "http_5xx": dict(m["http_5xx"]) if m else {}}}
        with open(state_path + ".tmp", "w") as f:
            json.dump(state, f)
        os.replace(state_path + ".tmp", state_path)
    return rep


def summary_text(rep):
    out = ["hour %s  %s  lines %d  sampled %d  not-run %d" % (
        rep["hour"], "PASS" if rep["pass"] else "FAIL", rep["lines"], rep["sampled"],
        rep["replay"].get("not_run", 0))]
    c = rep["replay"]["classes"]
    out.append("classes " + " ".join("%s=%d" % (k, c[k]) for k in ORDER + ["not-run"] if c.get(k))
               + "  (G4: " + " ".join("%s=%d" % (LETTER[k], c[k]) for k in LETTER if c.get(k)) + ")")
    for r, pc in sorted(rep["replay"]["per_route"].items()):
        bad = pc.get("unexplained", 0) + pc.get("ours-error", 0)
        out.append("  %-4s %-40s %s" % ("ok" if not bad else "FAIL", r,
                                        " ".join("%s=%d" % (k, v) for k, v in sorted(pc.items()))))
    lags = [r.get("lag") for r in rep["follow"]["chains"].values() if r.get("lag") is not None]
    out.append("follow: chains %d  max lag %s  served lag max %s  hash mismatches %d  halted %s  restarts %s" % (
        len(rep["follow"]["chains"]), max(lags, default=None), rep["served_lag_max"],
        sum(1 for r in rep["follow"]["chains"].values() if r.get("hash_ok") is False),
        (rep.get("metrics") or {}).get("halted"), rep.get("restarts") or "none"))
    if rep.get("metrics"):
        mm = rep["metrics"]
        out.append("metrics: drain dirty rows %s (delta %s)  5xx delta %s  request log %s" % (
            mm["dirty_rows"], mm["dirty_rows_delta"], mm["http_5xx_delta"] or "{}", mm["request_log"]))
    for e in rep["replay"]["examples"][:5]:
        out.append("  %s %s [%s] %s :: %s" % ("NEW" if e["new"] else "seen", e["route"], e["pair"],
                                             e["request"][:160], e["diff"][:200]))
    for r in rep["reasons"]:
        out.append("  reason: " + r)
    return "\n".join(out)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--config", required=True)
    ap.add_argument("--hour", help="YYYY-MM-DDTHH (UTC); default: the last full hour")
    ap.add_argument("--dry-run", action="store_true", help="sample and follow checks only, no replay")
    a = ap.parse_args(argv)
    with open(a.config, "rb") as f:
        user = tomllib.load(f)
    unknown = set(user) - set(DEFAULTS)
    if unknown:
        print("unknown config keys: %s" % ", ".join(sorted(unknown)), file=sys.stderr)
        return 2
    cfg = dict(DEFAULTS, **user)
    if not cfg["log"] or (not a.dry_run and not cfg["compare_config"]):
        print("config needs log and compare_config", file=sys.stderr)
        return 2
    start, end, label = hour_window(a.hour)
    os.makedirs(cfg["out_dir"], exist_ok=True)
    rep = run(cfg, label, start, end, a.dry_run)
    with open(os.path.join(cfg["out_dir"], "hourly-%s.json" % label), "w") as f:
        json.dump(rep, f, indent=1, sort_keys=True)
    print(summary_text(rep))
    return 0 if rep["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
