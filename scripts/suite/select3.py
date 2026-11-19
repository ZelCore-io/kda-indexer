#!/usr/bin/env python3
"""Run-3 case list (stdlib only; offline, no request is sent).

  select3.py --run2 DIR --cases2 DIR --fresh DIR --out DIR

Builds DIR/<endpoint>.jsonl.gz for `run.py run` from three sources, in this order per endpoint
(a run that stops at its deadline has covered the run-2 differences first):

  1. every case of run 2 (DIR2/results.jsonl, sample.jsonl; cases2 files) whose verdict was
     `diff`, `status` or `stock-unavailable` on a light endpoint, and `diff` or `status` in the
     stock sample of a heavy endpoint (flagged `sample`: it runs against stock again; the stock
     timeouts of the heavy sample are not repeated), and every case whose node check failed
     (`node.ok` false, any endpoint: sel run2-node-fail; H4 S2). Ids stay run 2's, so a record
     compares to its run-2 verdict by id. A picked id missing from the cases2 file is an error.
  2. a fresh sample from `gen.py cases --seed N` (FRESH_PER_EP per endpoint) for the endpoints
     where FIX-4 changed behaviour (FRESH_FULL), ids renumbered from FRESH_BASE;
  3. for the other GraphQL endpoints, the fresh `invalid` and `edge` cases (error
     mapping, FIX-4 item 3), ERR_MAP_EPS.

Heavy endpoints run on ours with the node-backed checks for every case; the cases flagged
`sample` also run against stock (`run.py run --sample-flag`); every fresh gql.transaction case
with a minimumDepth is flagged, none is dropped by the per-endpoint cap. Each line carries `sel`
(run2-diff, run2-status, run2-stock-unavailable, run2-node-fail, fresh). DIR/manifest.json holds counts only.
"""

import argparse
import collections
import gzip
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import endpoints as E  # noqa: E402

FRESH_PER_EP = 20000
FRESH_BASE = 1000000          # fresh case numbers start here: no clash with run 2's ids
SAMPLE_PER_EP = 650           # stock sample per heavy endpoint's fresh cases (run 2's size)
FRESH_FULL = {
    "gql.fungibleChainAccounts", "gql.gasLimitEstimate", "gql.node", "gql.nodes", "gql.transaction",
    "compat.v1txs", "gql.blocksFromDepth", "gql.blocksFromHeight", "gql.completedBlockHeights",
}
# Error mapping touches every GraphQL endpoint: the others get the fresh error-class cases.
ERR_MAP_EPS = sorted(e[0] for e in E.ENDPOINTS if e[1] == "graphql" and e[0] not in FRESH_FULL
                     and e[0] not in E.STOCK_SKIP)
ERR_CLASSES = {"invalid", "edge"}
RUN2_CLASSES = {"diff", "status", "stock-unavailable"}
HEAVY_CLASSES = {"diff", "status"}


def pick(rows, heavy):
    """{ep: [(id, why, sample)]} in first-seen order. `rows`: (source, row), source "results" or
    "sample". A heavy endpoint contributes its stock-sample diffs and status rows only (the
    results rows of a heavy endpoint are ours-only); a light endpoint any diff, status or
    stock-unavailable row. Any row whose node check failed is picked as `node-fail` (not flagged
    for stock when it is a heavy endpoint's ours-only row). A later row of the same case replaces
    an earlier one."""
    last = collections.OrderedDict()
    for src, r in rows:
        ep, c = r.get("ep"), (r.get("v") or {}).get("class")
        if ep is None:
            continue
        nf = (r.get("node") or {}).get("ok") is False
        if ep in heavy and src == "sample" and c in HEAVY_CLASSES:
            last[r["id"]] = (ep, c, True, True)
        elif ep not in heavy and c in RUN2_CLASSES:
            last[r["id"]] = (ep, c, True, False)
        elif nf:
            last[r["id"]] = (ep, "node-fail", True, ep in heavy and src == "sample")
        elif c is not None and (src == "sample" or ep not in heavy):
            last[r["id"]] = (ep, c, False, ep in heavy)
    out = collections.OrderedDict()
    for i, (ep, c, ok, hv) in last.items():
        if ok:
            out.setdefault(ep, []).append((i, c, hv))
    return out


def _read(fn):
    if not os.path.exists(fn):
        return
    for l in gzip.open(fn, "rt"):
        yield json.loads(l)


def _num(i):
    return int(i.rsplit(":", 1)[1])


def build(picks, cases2, fresh, out, fresh_full=None, err_eps=None, sample_per_ep=SAMPLE_PER_EP):
    """Writes the case files; returns the manifest (counts per endpoint and source)."""
    fresh_full = FRESH_FULL if fresh_full is None else fresh_full
    err_eps = set(ERR_MAP_EPS if err_eps is None else err_eps)
    os.makedirs(out, exist_ok=True)
    man = {"endpoints": {}, "fresh_per_ep": FRESH_PER_EP}
    for ep in sorted(set(picks) | set(fresh_full) | err_eps):
        n = collections.Counter()
        lines = []
        want = {i: (why, sample) for i, why, sample in picks.get(ep, [])}
        found = set()
        for c in _read(os.path.join(cases2, ep + ".jsonl.gz")):       # case order, not verdict order
            if c["id"] in want:
                found.add(c["id"])
                why, sample = want[c["id"]]
                c["sel"] = "run2-" + why
                if sample:
                    c["sample"] = True
                lines.append(c)
                n[c["sel"]] += 1
        if len(found) != len(want):
            raise ValueError("%s: %d picked case(s) not in the cases2 file" % (ep, len(want) - len(found)))
        fc = list(_read(os.path.join(fresh, ep + ".jsonl.gz"))) if (ep in fresh_full or ep in err_eps) else []
        if ep in err_eps and ep not in fresh_full:
            fc = [c for c in fc if c["cls"] in ERR_CLASSES]
        heavy = ep in E.HEAVY_ENDPOINTS
        flagged, deep = set(), set()
        if ep == "gql.transaction":
            deep = {i for i, c in enumerate(fc) if (c["req"].get("variables") or {}).get("minimumDepth") is not None}
        if heavy and fc:
            step = max(1, len(fc) // max(1, sample_per_ep))
            flagged = set(range(0, min(len(fc), FRESH_PER_EP), step)[:sample_per_ep]) if sample_per_ep else set()
            flagged |= deep
        keep = [k for k in range(len(fc)) if k < FRESH_PER_EP or k in deep]
        for k in keep:
            c = fc[k]
            c["id"] = "%s:%d" % (ep, FRESH_BASE + _num(c["id"]))
            c["sel"] = "fresh"
            if k in flagged:
                c["sample"] = True
            lines.append(c)
            n["fresh"] += 1
        with gzip.open(os.path.join(out, ep + ".jsonl.gz"), "wt") as f:
            for c in lines:
                f.write(json.dumps(c, separators=(",", ":")) + "\n")
        man["endpoints"][ep] = dict(n)
        if any(c.get("sample") for c in lines):
            man["endpoints"][ep]["stock-sample"] = sum(1 for c in lines if c.get("sample"))
    return man


def load_rows(run2):
    for src, name in (("results", "results.jsonl"), ("sample", "sample.jsonl")):
        fn = os.path.join(run2, name)
        if not os.path.exists(fn):
            continue
        for l in open(fn):
            try:
                yield src, json.loads(l)
            except ValueError:
                pass


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run2", required=True)
    ap.add_argument("--cases2", required=True)
    ap.add_argument("--fresh", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    picks = pick(load_rows(a.run2), E.HEAVY_ENDPOINTS)
    man = build(picks, a.cases2, a.fresh, a.out)
    json.dump(man, open(os.path.join(a.out, "manifest.json"), "w"), indent=1, sort_keys=True)
    tot = collections.Counter()
    for m in man["endpoints"].values():
        tot.update(m)
    print(json.dumps(dict(tot), sort_keys=True))


if __name__ == "__main__":
    main()
