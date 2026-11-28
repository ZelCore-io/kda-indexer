#!/usr/bin/env python3
"""Hourly answer check after a cutover: replay a sample of the requests kda-indexer served
in the last hour against the stock services and the node, and check that the indexer follows
the chain. Python 3.11+ standard library only.

  hourly_check.py --config hourly.toml [--hour YYYY-MM-DDTHH] [--dry-run]

Inputs: the indexer's request log (`[api.request_log]`, operations.md "Request log"), the
`kda-compare` binary with a compare config whose pairs and targets name the stock services,
the indexer's native and admin listeners, and the node.

Steps:
  1. Read the hour's lines from the log and its rotations (`<log>`, `<log>.1`, ...); a file
     last written before the hour began is not opened. Lines with `"local": true` (a loopback
     client: this script's own replays) are skipped and counted as `skipped_local`.
  2. Sample: one request of every distinct query shape (route, query-parameter names; for
     GraphQL the operation's document and variable names), then up to `per_route` per route,
     at most `budget` in all. Identical requests are replayed once. Only GET/HEAD compat
     requests and GraphQL documents whose every operation is a query are replayed (a mutation
     or subscription anywhere in the document is skipped); `operationName` is kept.
  3. Replay the sample through `kda-compare replay-batch`: every configured pair (ours against
     chainweb-data, kda-txs, the GraphQL indexer and the node), with the harness's normalisers,
     allowlist, node oracles (tx existence by /poll, block hashes, balances) and its per-target
     rate caps. A request that differs is asked again once after `recheck_delay_s`. The whole
     run, re-check included, has one deadline (`deadline_minutes`), and one run at a time
     holds `<out_dir>/hourly.lock`, so the stock rate caps hold across the run.
  4. Chain follow: our tip per chain (native `/status`) against the node's cut (height, lag, and
     the node's hash at our height), halted chains, restarts, 5xx and the balance drain from
     `/metrics`, compared with the previous hour's state.
  5. Print a compact summary and write `<out_dir>/hourly-<hour>.json` (mode 0640). Exit 0 =
     pass, 1 = fail, 2 = the check itself could not run (or another run holds the lock:
     "busy"), 3 = inconclusive (too few requests compared; not a pass).

Classes (the G4 triage classes): equal; legacy-equal (kda-compare's legacy-cmd with no
difference: the node re-encoded an old tx's cmd, every compared field agrees); (a) allowlisted (a documented, accepted difference);
(d) known-gap; (c) timing: a tip race or a balance row read while the drain had it stale
(the harness's skew/reorg rules), or a difference the re-check finds equal or allowlisted
(a re-check that answers ours-error counts as ours-error; any other re-check keeps the
difference unexplained); stock-unavailable
(the stock side failed: not counted against us); ours-error; unexplained (open, the (b)
candidates). The replay happens after the request was served (up to an hour later), so it
judges the answer for the same request now, not the bytes served; tip-dependent answers move in
between, which is what the timing class and the re-check absorb.

Pass = no unexplained difference and no ours-error, every chain's lag within `ready_max_lag`
(now and in every logged request of the hour), no halted chain or hash mismatch, no restart,
and no route above `max_5xx_share` of 5xx answers in the hour. Inconclusive = no failure, but
fewer requests compared (every class but stock-unavailable, not-compared and not-run) than
`min_compared_share` of the sample or `min_compared`, whichever is higher: stock down all
hour, or the deadline hit before the answers came back.
"""

import argparse
import calendar
import collections
import fcntl
import hashlib
import json
import math
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
    "min_compared_share": 0.6,
    "min_compared": 200,
}
SIGNATURES_KEPT = 5000

# Harness outcome classes (kda-compare) -> hourly classes. Anything else is a difference.
EQUAL = {"match"}
ALLOWED = {"allowlisted"}
GAP = {"known-gap"}
TIMING = {"skew", "reorg", "oracle-tip"}
STOCK = {"b-error"}
NOT_COMPARED = {"no-oracle", "bad-request", "a-paused", "gen-error"}
OURS = {"a-error"}
# kda-compare's `legacy-cmd`: the node serves a re-encoded cmd for an old tx (it does not hash
# to the request key), so the outcome is flagged even when every projected field agrees. With
# no difference recorded it is `legacy-equal`; with any difference it stays unexplained.
LEGACY = {"legacy-cmd"}
# Worst first: an item takes the worst class of its pairs.
ORDER = ["ours-error", "unexplained", "timing", "known-gap", "allowlisted", "legacy-equal", "equal",
         "stock-unavailable", "not-compared"]
LETTER = {"allowlisted": "a", "known-gap": "d", "timing": "c", "unexplained": "b?"}
# Classes of an item whose answers were compared (a difference was compared too).
COMPARED = ("equal", "legacy-equal", "allowlisted", "known-gap", "timing", "unexplained",
            "ours-error")
# A re-check that answers one of these turns a difference into (c) timing.
RECHECK_CLEARS = ("equal", "allowlisted")
EXIT = {"PASS": 0, "FAIL": 1, "INCONCLUSIVE": 3}


def write_private(path, text):
    """Writes `path` with mode 0640: reports and state hold request paths and variables."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o640)
    with os.fdopen(fd, "w") as f:
        os.fchmod(f.fileno(), 0o640)
        f.write(text)


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
        try:
            if os.path.getmtime(p) * 1000 < start_ms:
                continue  # last written before the hour began: none of its lines are in it
        except OSError:
            continue
        with open(p, encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    e = json.loads(line)
                except ValueError:
                    counts["bad-line"] += 1
                    continue
                if start_ms <= e.get("t", 0) < end_ms:
                    if e.get("local") is True:
                        counts["skipped-local"] += 1  # loopback: this script's own replays
                        continue
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


def operation_types(query):
    """Types of the top-level definitions of a GraphQL document, in order (`query`,
    `mutation`, `subscription`, `fragment`; a `{` shorthand is a query). Comments and strings
    are skipped, so neither a `# kdx:` line nor a string can hide or fake a definition."""
    out, depth, paren, expect, i, n = [], 0, 0, True, 0, len(query)
    while i < n:
        c = query[i]
        if c == "#":
            j = query.find("\n", i)
            i = n if j < 0 else j + 1
            continue
        if c == '"':
            if query.startswith('"""', i):
                j = query.find('"""', i + 3)
                while j > 0 and query[j - 1] == "\\":
                    j = query.find('"""', j + 3)
                i = n if j < 0 else j + 3
            else:
                j = i + 1
                while j < n and query[j] not in '"\n':
                    j += 2 if query[j] == "\\" else 1
                i = j + 1
            continue
        if c == "(":
            paren += 1
        elif c == ")":
            paren -= 1
        elif c == "{" and paren == 0:
            if depth == 0 and expect:
                out.append("query")
                expect = False
            depth += 1
        elif c == "}" and paren == 0:
            depth -= 1
            if depth == 0:
                expect = True
        elif (c.isalpha() or c == "_") and depth == 0 and paren == 0:
            j = i
            while j < n and (query[j].isalnum() or query[j] == "_"):
                j += 1
            if expect:
                out.append(query[i:j])
                expect = False
            i = j
            continue
        i += 1
    return out


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
        if any(t not in ("query", "fragment") for t in operation_types(query)):
            return "graphql", None, "not-query"
        root = re.search(r"\{\s*([A-Za-z_]\w*)", query)
        rkey = "graphql:" + (root.group(1) if root else "?")
        shape = "%s|%s|%s|%s" % (rkey, op or "", doc_key(query), ",".join(sorted(variables)))
        it = {"query": query, "variables": variables}
        if op:
            it["operationName"] = op
        return rkey, shape, it
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
        elif c in LEGACY and no_difference(o):
            classes.add("legacy-equal")
        else:
            classes.add("unexplained")
    for c in ORDER:
        if c in classes:
            return c
    return "not-compared"


def no_difference(outcome):
    """True only when the outcome's record says, with an empty list, that nothing differed (a
    missing record or diff is not proof of equality)."""
    rec = outcome.get("record")
    return isinstance(rec, dict) and rec.get("diff") == []


def signature(rkey, line):
    """Route plus the JSON paths that differed: what makes a difference 'new'."""
    paths = set()
    for o in line.get("outcomes") or []:
        for d in ((o.get("record") or {}).get("diff") or [])[:6]:
            paths.add(re.sub(r"/\d+", "/N", str(d.get("path", "?"))) + " " + str(d.get("kind", "")))
        if o.get("class") not in EQUAL | ALLOWED:
            paths.add("class " + str(o.get("class")))
    return rkey + " :: " + " + ".join(sorted(paths))


def replay(cfg, items, deadline):
    """Runs `kda-compare replay-batch` on `items`: {id: verdict line}, with the time left
    before `deadline` (time.monotonic()); what has not answered by then is not-run."""
    left = deadline - time.monotonic()
    if left <= 0:
        return {}, "deadline"
    cmd = [cfg["compare_bin"], "--config", cfg["compare_config"], "replay-batch",
           "--concurrency", str(cfg["concurrency"])]
    inp = "".join(json.dumps({k: v for k, v in it.items() if k != "route"}) + "\n" for it in items)
    try:
        p = subprocess.run(cmd, input=inp, capture_output=True, text=True,
                           timeout=left)
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
    """(status, reasons) of a report: PASS, FAIL, or INCONCLUSIVE (no failure, but too few
    requests compared)."""
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
    if why:
        return "FAIL", why
    if not rep["replay"].get("dry_run"):
        compared = sum(c.get(k, 0) for k in COMPARED)
        need = max(math.ceil(cfg["min_compared_share"] * rep.get("sampled", 0)), cfg["min_compared"])
        if compared < need:
            return "INCONCLUSIVE", [
                "%d of %d sampled requests compared, below the floor of %d (stock-unavailable %d, "
                "not-compared %d, not-run %d%s)" % (
                    compared, rep.get("sampled", 0), need, c.get("stock-unavailable", 0),
                    c.get("not-compared", 0), c.get("not-run", 0),
                    ", deadline hit" if rep["replay"].get("deadline_hit") else "")]
    return "PASS", why


def run(cfg, label, start_ms, end_ms, dry, deadline=None):
    """One hour's check. `deadline` (time.monotonic()) bounds the replay and the re-check
    together; default `deadline_minutes` from now."""
    if deadline is None:
        deadline = time.monotonic() + cfg["deadline_minutes"] * 60
    counts = collections.Counter()
    entries = list(read_window(log_files(cfg["log"]), start_ms, end_ms, counts))
    items, routes, skipped = sample(entries, cfg["budget"], cfg["per_route"], label)
    served, lag = served_stats(entries)
    rep = {"hour": label, "lines": len(entries), "bad_lines": counts["bad-line"],
           "skipped_local": counts["skipped-local"],
           "routes": dict(routes), "skipped": dict(skipped), "sampled": len(items),
           "served": served, "served_lag_max": lag,
           "replay": {"classes": {}, "per_route": {}, "examples": [], "not_run": 0}}
    state_path = os.path.join(cfg["out_dir"], "hourly-state.json")
    try:
        with open(state_path) as f:
            state = json.load(f)
    except (OSError, ValueError):
        state = {}
    # Oldest first; a signature seen again moves to the end, so the newest are kept.
    seen = dict.fromkeys(s for s in state.get("signatures", []) if isinstance(s, str))
    if dry:
        rep["replay"]["dry_run"] = True
    if not dry and items:
        got, err = replay(cfg, items, deadline)
        diffs = [it for it in items if it["id"] in got and item_class(got[it["id"]]) == "unexplained"]
        if diffs:
            time.sleep(max(0.0, min(cfg["recheck_delay_s"], deadline - time.monotonic())))
            again, err2 = replay(cfg, diffs, deadline)
            err = err or err2
            for it in diffs:
                v = again.get(it["id"])
                if v is not None:
                    got[it["id"]] = dict(got[it["id"]], recheck=item_class(v))
        cls = collections.Counter()
        per = collections.defaultdict(collections.Counter)
        for it in items:
            v = got.get(it["id"])
            c = "not-run" if v is None else item_class(v)
            r = v.get("recheck") if v is not None else None
            if r in RECHECK_CLEARS:
                c = "timing"
            elif r == "ours-error":
                c = "ours-error"
            cls[c] += 1
            per[it["route"]][c] += 1
            if c == "unexplained":
                sig = signature(it["route"], v)
                new = sig not in seen
                seen.pop(sig, None)
                seen[sig] = True
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
    rep["status"], rep["reasons"] = verdict(rep, cfg)
    rep["pass"] = rep["status"] == "PASS"
    if not dry:
        state = {"signatures": list(seen)[-SIGNATURES_KEPT:],
                 "last": {"hour": label, "unit": unit,
                          "dirty_rows": m["dirty_rows"] if m else None,
                          "http_5xx": dict(m["http_5xx"]) if m else {}}}
        write_private(state_path + ".tmp", json.dumps(state))
        os.replace(state_path + ".tmp", state_path)
    return rep


def summary_text(rep):
    out = ["hour %s  %s  lines %d  sampled %d  not-run %d  skipped_local %d" % (
        rep["hour"], rep["status"], rep["lines"], rep["sampled"],
        rep["replay"].get("not_run", 0), rep.get("skipped_local", 0))]
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
    deadline = time.monotonic() + cfg["deadline_minutes"] * 60
    start, end, label = hour_window(a.hour)
    os.makedirs(cfg["out_dir"], exist_ok=True)
    lock_path = os.path.join(cfg["out_dir"], "hourly.lock")
    lock = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o640)
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            print("busy: another hourly check holds %s" % lock_path, file=sys.stderr)
            return 2
        rep = run(cfg, label, start, end, a.dry_run, deadline)
        write_private(os.path.join(cfg["out_dir"], "hourly-%s.json" % label),
                      json.dumps(rep, indent=1, sort_keys=True))
    finally:
        os.close(lock)
    print(summary_text(rep))
    return EXIT[rep["status"]]


if __name__ == "__main__":
    sys.exit(main())
