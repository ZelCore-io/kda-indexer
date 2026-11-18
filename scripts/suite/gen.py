#!/usr/bin/env python3
"""Endpoint suite, case generator (stdlib only).

Two steps, both offline (no request is sent):

  gen.py harvest --out pool.json [--traffic TSV] [--gqlcap LOG ...] [--diffs JSONL[.zst] ...]
                 [--seeds TOML] [--ops-dir DIR ...]
      Builds the entity pool from files already on the host: the real REST traffic list,
      the GraphQL body capture, the compare harness's diff logs (requests and trimmed
      answers of both sides) and the seed list. Accounts are stratified by how often they
      appear (top 100, then ten deciles), by role (receive-only), by name form (k:, w:, r:,
      non-standard) plus never-seen names made up at generation time. Request keys and block
      hashes carry tags (failed, cont, xchain, coinbase, genesis, legacy, fork, orphan, ...)
      where the source shows them.

  gen.py cases --pool pool.json --out DIR [--per-endpoint 20000] [--seed 1]
      Writes DIR/<endpoint>.jsonl.gz (one case per line: id, ep, cls, req) and
      DIR/manifest.json (counts per endpoint and class, and the classes that fell back
      because the pool had nothing for them). Same pool and seed give the same cases.

The pool and the cases hold real data: keep them on the host, never commit them.
"""

import argparse
import collections
import gzip
import json
import os
import random
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import endpoints as E  # noqa: E402

RK_RE = r"[A-Za-z0-9_-]{43}"
KEYS_RK = {"requestKey", "request_key", "requestkey", "reqKey"}
KEYS_BLOCK = {"blockHash", "block_hash", "blockhash"}
KEYS_SENDER = {"sender", "senderAccount", "fromAccount", "from", "from_acct"}
KEYS_RECV = {"receiverAccount", "toAccount", "to", "receiver", "to_acct"}
KEYS_ACCT = {"account", "accountName", "account_name", "minerAccount"} | KEYS_SENDER | KEYS_RECV
KEYS_KEY = {"publicKey", "public_key", "pubkey", "pubKey"}
KEYS_MOD = {"moduleName", "module", "fungibleName", "token", "modulename", "namespace"}
KEYS_EVENT = {"qualifiedEventName", "qualifiedName", "qualified_name"}
KEYS_PACT = {"pactId", "pact_id", "pactid"}
KEYS_NFT = {"tokenId", "token_id"}
TEXT_RES = {
    "rk": re.compile(r'"(?:requestKey|request_key|reqKey)"\s*:\s*"(%s)"' % RK_RE),
    "block": re.compile(r'"(?:blockHash|block_hash|hash)"\s*:\s*"(%s)"' % RK_RE),
    "acct": re.compile(r'"(?:account|accountName|sender|senderAccount|receiverAccount|fromAccount|toAccount)"\s*:\s*"([^"\\]{1,256})"'),
    "key": re.compile(r'"(?:publicKey|public_key|pubkey)"\s*:\s*"([0-9a-f]{64})"'),
    "mod": re.compile(r'"(?:moduleName|fungibleName|module)"\s*:\s*"([A-Za-z0-9_.-]{2,80})"'),
    "event": re.compile(r'"(?:qualifiedName|qualifiedEventName|qualified_name)"\s*:\s*"([A-Za-z0-9_.-]{3,120})"'),
    "pact": re.compile(r'"(?:pactId|pact_id)"\s*:\s*"(%s)"' % RK_RE),
    "nft": re.compile(r'"(?:tokenId|token_id)"\s*:\s*"([^"\\]{1,200})"'),
    "failed": re.compile(r'"(?:requestKey|request_key|hash)"\s*:\s*"(%s)"[^{}]{0,1500}?(?:"result"\s*:\s*"Failure"|"badResult"\s*:\s*"|"status"\s*:\s*"failure")' % RK_RE),
    "height": re.compile(r'"(?:chain|chainId)"\s*:\s*"?(\d{1,2})"?[^{}]{0,300}?"(?:height|blockHeight)"\s*:\s*(\d+)'),
}
ROOT_RE = re.compile(r"\{\s*(?:(\w+)\s*:\s*)?(\w+)")


class Pool:
    def __init__(self):
        self.acct = collections.Counter()
        self.senders, self.receivers = set(), set()
        self.keys = collections.Counter()
        self.mods = collections.Counter()
        self.events = collections.Counter()
        self.rks = collections.defaultdict(set)      # tag -> rks
        self.blocks = collections.defaultdict(set)   # tag -> hashes
        self.heights = set()
        self.pacts, self.nfts = set(), set()
        self.rest_real = collections.defaultdict(set)
        self.gql_real = collections.defaultdict(list)
        self.search_terms = set()
        self.tip = 0

    # -- entity intake --------------------------------------------------------------
    def account(self, a, role=None):
        if not isinstance(a, str) or not 1 <= len(a.encode()) <= 256:
            return
        self.acct[a] += 1
        if role == "s":
            self.senders.add(a)
        elif role == "r":
            self.receivers.add(a)
        if a.startswith("k:") and len(a) == 66:
            self.keys[a[2:]] += 1

    def walk(self, v, depth=0):
        """Tags entities in a parsed answer or variable map."""
        if depth > 40:
            return
        if isinstance(v, list):
            for x in v:
                self.walk(x, depth + 1)
            return
        if not isinstance(v, dict):
            return
        txt = None
        h = v.get("height", v.get("blockHeight"))
        ch = v.get("chain", v.get("chainId"))
        for k, x in v.items():
            if isinstance(x, (dict, list)):
                self.walk(x, depth + 1)
                continue
            if not isinstance(x, str):
                continue
            if k in KEYS_RK and re.fullmatch(RK_RE, x):
                tags = ["any"]
                txt = txt or json.dumps(v)[:20000]
                if v.get("badResult") or v.get("result") == "Failure" or v.get("status") == "failure":
                    tags.append("failed")
                if v.get("kind") == "cont" or v.get("continuation") or '"step"' in txt:
                    tags.append("cont")
                if "XCHAIN" in txt or "X_RESUME" in txt:
                    tags.append("xchain")
                if v.get("kind") == "coinbase":
                    tags.append("coinbase")
                if isinstance(h, int):
                    tags.append("genesis" if h == 0 else "old" if h < 1000000 else
                                "recent" if self.tip and h > self.tip - 100000 else "any")
                for t in tags:
                    self.rks[t].add(x)
            elif k in KEYS_BLOCK and re.fullmatch(RK_RE, x):
                self.block(x, h)
            elif k == "hash" and re.fullmatch(RK_RE, x) and ("parent" in v or "powHash" in v or "adjacents" in v):
                self.block(x, h)
            elif k in KEYS_SENDER:
                self.account(x, "s")
            elif k in KEYS_RECV:
                self.account(x, "r")
            elif k in KEYS_ACCT:
                self.account(x)
            elif k in KEYS_KEY and re.fullmatch(r"[0-9a-f]{64}", x):
                self.keys[x] += 1
            elif k in KEYS_MOD and re.fullmatch(r"[A-Za-z0-9_.-]{2,80}", x):
                self.mods[x] += 1
            elif k in KEYS_EVENT and "." in x:
                self.events[x] += 1
            elif k in KEYS_PACT and re.fullmatch(RK_RE, x):
                self.pacts.add(x)
            elif k in KEYS_NFT:
                self.nfts.add(x)
        if isinstance(h, int) and ch is not None and str(ch).isdigit():
            self.heights.add((int(ch), h))

    def block(self, x, h):
        self.blocks["any"].add(x)
        if isinstance(h, int):
            if h == 0:
                self.blocks["genesis"].add(x)
            elif any(abs(h - f) <= 2 for f in E.FORKS):
                self.blocks["fork"].add(x)
            elif self.tip and h > self.tip - 100000:
                self.blocks["recent"].add(x)

    def text(self, s):
        """Regex intake for trimmed (unparseable) answers."""
        for x in TEXT_RES["rk"].findall(s):
            self.rks["any"].add(x)
        for x in TEXT_RES["failed"].findall(s):
            self.rks["failed"].add(x)
        for x in TEXT_RES["block"].findall(s):
            self.blocks["any"].add(x)
        for x in TEXT_RES["acct"].findall(s):
            self.account(x)
        for x in TEXT_RES["key"].findall(s):
            self.keys[x] += 1
        for x in TEXT_RES["mod"].findall(s):
            self.mods[x] += 1
        for x in TEXT_RES["event"].findall(s):
            if "." in x:
                self.events[x] += 1
        for x in TEXT_RES["pact"].findall(s):
            self.pacts.add(x)
        for x in TEXT_RES["nft"].findall(s):
            self.nfts.add(x)
        for c, h in TEXT_RES["height"].findall(s):
            self.heights.add((int(c), int(h)))

    def body(self, s):
        try:
            self.walk(json.loads(s))
        except Exception:
            self.text(s)

    # -- request intake -------------------------------------------------------------
    def rest(self, path):
        from urllib.parse import parse_qs, unquote, urlsplit
        u = urlsplit(path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        segs = [unquote(s) for s in u.path.split("/") if s]
        route = None
        if u.path.startswith("/txs/"):
            route = segs[1] if len(segs) > 1 else None
            if route == "search" and "pactid" in q:
                route = "pactid"
            if route == "account" and len(segs) > 2:
                self.account(segs[2])
        elif u.path.startswith("/v1/txs"):
            route = "v1txs"
            if len(segs) > 2:
                self.account(segs[2])
            if len(segs) > 3:
                self.mods[segs[3]] += 1
        elif u.path in ("/coins", "/stats"):
            route = u.path[1:]
        if route in ("tx", "txs", "account", "events", "recent", "search", "pactid", "coins", "stats", "v1txs"):
            self.rest_real[route].add(path)
        for k in ("requestkey",):
            if k in q and re.fullmatch(RK_RE, q[k]):
                self.rks["any"].add(q[k])
        if "search" in q:
            self.search_terms.add(q["search"])
        if "pactid" in q and re.fullmatch(RK_RE, q["pactid"]):
            self.pacts.add(q["pactid"])
        for k in ("token", "modulename"):
            if k in q:
                self.mods[q[k]] += 1
        if "param" in q:
            self.account(q["param"])

    def gql(self, query, variables, op=None):
        m = ROOT_RE.search(query[query.find("{"):] if "{" in query else "")
        if not m or query.lstrip().startswith("subscription"):
            return
        field = m.group(2)
        if field not in E.GQL:
            return
        if len(self.gql_real[field]) < 2000:
            self.gql_real[field].append({"query": query, "variables": variables or {}, "op": op or "real"})
        if isinstance(variables, dict):
            self.walk(variables)

    # -- output ---------------------------------------------------------------------
    def strata(self):
        by = collections.defaultdict(list)
        ranked = [a for a, _ in self.acct.most_common()]
        top = ranked[:100]
        by["top100"] = top
        rest = list(reversed(ranked[100:]))  # least seen first
        n = len(rest)
        for i in range(10):
            by["d%d" % (i + 1)] = rest[i * n // 10:(i + 1) * n // 10]
        by["receive_only"] = sorted(self.receivers - self.senders)
        for a in ranked:
            if a.startswith("k:"):
                by["k"].append(a)
            elif a.startswith("w:"):
                by["w"].append(a)
            elif a.startswith("r:"):
                by["r"].append(a)
            else:
                by["nonstandard"].append(a)
        return {k: v[:50000] for k, v in by.items()}

    def dump(self):
        return {
            "tip": self.tip,
            "accounts_by": self.strata(),
            "keys": [k for k, _ in self.keys.most_common(50000)],
            "modules": [m for m, _ in self.mods.most_common(2000)],
            "events": [e for e, _ in self.events.most_common(2000)],
            "rks_by": {k: sample(v, 100000) for k, v in self.rks.items()},
            "blocks_by": {k: sample(v, 100000) for k, v in self.blocks.items()},
            "heights": sample(self.heights, 200000),
            "pact_ids": sorted(self.pacts),
            "nft_ids": sorted(self.nfts),
            "rest_real": {k: sorted(v) for k, v in self.rest_real.items()},
            "gql_real": dict(self.gql_real),
            "search_terms": sorted(self.search_terms),
        }


def sample(xs, n):
    """At most n of xs, a seeded uniform sample (not the first n in sort order)."""
    xs = sorted(xs)
    return xs if len(xs) <= n else sorted(random.Random(0).sample(xs, n))


def lines(path):
    if path.endswith(".zst"):
        p = subprocess.Popen(["zstd", "-dc", path], stdout=subprocess.PIPE, text=True,
                             errors="replace")
        yield from p.stdout
        p.wait()
    elif path.endswith(".gz"):
        with gzip.open(path, "rt", errors="replace") as f:
            yield from f
    else:
        with open(path, errors="replace") as f:
            yield from f


def load_ops(dirs):
    ops = {}
    for d in dirs or []:
        for fn in os.listdir(d):
            if fn.endswith(".graphql"):
                ops[fn[:-8]] = "\n".join(
                    l for l in open(os.path.join(d, fn)).read().splitlines() if not l.startswith("#"))
    return ops


def harvest(a):
    p = Pool()
    ops = load_ops(a.ops_dir)
    for fn in a.diffs or []:  # first pass: the tip (for the recent tags)
        for l in lines(fn):
            m = re.search(r'"a_tip_height":\s*"?(\d+)', l)
            if m:
                p.tip = max(p.tip, int(m.group(1)))
    if a.traffic:
        for l in lines(a.traffic):
            parts = l.rstrip("\n").split("\t")
            if len(parts) == 3 and parts[1] == "GET":
                p.rest(parts[2])
    for fn in a.gqlcap or []:
        for l in lines(fn):
            m = re.search(r'cl=(\d+) bl=(\d+) body="(.*)"\s*$', l)
            if not m or m.group(1) != m.group(2):
                continue
            try:
                b = json.loads(json.loads('"%s"' % m.group(3)))
            except Exception:
                continue
            for one in (b if isinstance(b, list) else [b]):
                if isinstance(one, dict) and isinstance(one.get("query"), str):
                    p.gql(one["query"], one.get("variables"), one.get("operationName"))
    for fn in a.diffs or []:
        for l in lines(fn):
            try:
                d = json.loads(l)
            except Exception:
                continue
            rq = d.get("request") or ""
            if rq.startswith("/"):
                p.rest(rq)
            elif str(d.get("route", "")).startswith("graphql:"):
                name = d["route"][8:]
                opn, _, var = rq.partition(" ")
                try:
                    vs = json.loads(var) if var else {}
                except Exception:
                    vs = {}
                if name in ops:
                    p.gql(ops[name], vs, name)
                else:
                    p.walk(vs)
            for k in ("a_body_trimmed", "b_body_trimmed"):
                if isinstance(d.get(k), str):
                    p.body(d[k])
    if a.seeds:
        import tomllib
        for s in tomllib.load(open(a.seeds, "rb")).get("seed", []):
            note = (s.get("note") or "").lower()
            v = s.get("value")
            if not isinstance(v, (str, int)) or str(v).startswith("TODO"):
                continue  # placeholder seed
            if s.get("kind") == "account":
                p.account(v)
            elif s.get("kind") == "request_key":
                p.rks["any"].add(v)
                for tag, words in (("failed", ["fail"]), ("legacy", ["legacy"]),
                                   ("cont", ["cont", "defpact"]), ("xchain", ["xchain", "cross"]),
                                   ("coinbase", ["coinbase"])):
                    if any(w in note for w in words):
                        p.rks[tag].add(v)
            elif s.get("kind") == "pact_id":
                p.pacts.add(v)
            elif s.get("kind") == "height" and str(v).isdigit():
                p.heights.add((int(s.get("chain", 0)), int(v)))
            elif s.get("kind") == "block_hash":
                p.blocks["orphan" if "orphan" in note else "any"].add(v)
    pool = p.dump()
    with open(a.out, "w") as f:
        json.dump(pool, f)
    print(json.dumps(summary(pool), indent=1))


def summary(pool):
    return {
        "tip": pool["tip"],
        "accounts": {k: len(v) for k, v in pool["accounts_by"].items()},
        "keys": len(pool["keys"]), "modules": len(pool["modules"]), "events": len(pool["events"]),
        "rks": {k: len(v) for k, v in pool["rks_by"].items()},
        "blocks": {k: len(v) for k, v in pool["blocks_by"].items()},
        "heights": len(pool["heights"]), "pact_ids": len(pool["pact_ids"]),
        "nft_ids": len(pool["nft_ids"]),
        "rest_real": {k: len(v) for k, v in pool["rest_real"].items()},
        "gql_real": {k: len(v) for k, v in pool["gql_real"].items()},
        "search_terms": len(pool["search_terms"]),
    }


def real_backed(ep, pool):
    name, fam = ep[0], ep[1]
    if fam == "graphql":
        return bool((pool.get("gql_real") or {}).get(name[4:]))
    if fam == "compat":
        return bool((pool.get("rest_real") or {}).get(name[7:]))
    return False


def generate(pool, out, per, seed, only=None):
    os.makedirs(out, exist_ok=True)
    man = {"seed": seed, "per_endpoint": per, "pool": summary(pool), "endpoints": {}}
    for ep in E.ENDPOINTS:
        name, fam, stock, build, classes, tpl = ep
        if only and name not in only:
            continue
        r = random.Random("%s:%s" % (seed, name))
        names = [c for c, _ in classes]
        weights = [w for _, w in classes]
        counts = collections.Counter()
        backed = real_backed(ep, pool)
        with gzip.open(os.path.join(out, name + ".jsonl.gz"), "wt") as f:
            for i in range(per):
                # Stratified: every class gets its share in a fixed interleave, not a draw.
                cls = names[_slot(i, weights)]
                label = cls if (cls != "real" or backed) else "real>entity"
                req = build(cls, r, pool)
                counts[label] += 1
                f.write(json.dumps({"id": "%s:%d" % (name, i), "ep": name, "cls": label,
                                    "req": req}, separators=(",", ":")) + "\n")
        man["endpoints"][name] = {"family": fam, "stock": stock, "route": tpl,
                                  "classes": dict(counts),
                                  "stock_skip": E.STOCK_SKIP.get(name)}
    with open(os.path.join(out, "manifest.json"), "w") as f:
        json.dump(man, f, indent=1)
    return man


def _slot(i, weights):
    """Deterministic stratified class for case i: a low-discrepancy walk over the weights."""
    x = (i * 0.6180339887498949) % 1.0
    acc = 0.0
    total = sum(weights)
    for k, w in enumerate(weights):
        acc += w / total
        if x < acc:
            return k
    return len(weights) - 1


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    h = sub.add_parser("harvest")
    h.add_argument("--out", required=True)
    h.add_argument("--traffic")
    h.add_argument("--gqlcap", nargs="*")
    h.add_argument("--diffs", nargs="*")
    h.add_argument("--seeds")
    h.add_argument("--ops-dir", nargs="*")
    c = sub.add_parser("cases")
    c.add_argument("--pool", required=True)
    c.add_argument("--out", required=True)
    c.add_argument("--per-endpoint", type=int, default=20000)
    c.add_argument("--seed", default="1")
    c.add_argument("--only", nargs="*")
    a = ap.parse_args()
    if a.cmd == "harvest":
        harvest(a)
    else:
        man = generate(json.load(open(a.pool)), a.out, a.per_endpoint, a.seed, a.only)
        tot = sum(sum(e["classes"].values()) for e in man["endpoints"].values())
        print("endpoints=%d cases=%d" % (len(man["endpoints"]), tot))


if __name__ == "__main__":
    main()
