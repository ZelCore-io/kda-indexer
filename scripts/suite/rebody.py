#!/usr/bin/env python3
"""WP-H2: re-referee stored stock-paired records from the bodies they kept, with the current compare.

`run.py rereferee` re-judges a record's stored diff entries only. Records stored before WP-H1 were
compared by position, so their `balance(...)` and `/v1/txs` entries say nothing about the set
compare. This pass runs the whole current chain again on the two kept bodies (no request is
sent to stock, ours or the node):

    unpack_bodies -> normalize_pair -> kda-compare pair-diff -> verdict_class -> stored_verdict

The node check, tip pair and indexer heights stored on the record are the rule evidence, as in
the run. Records without kept bodies (stored equal) cannot be re-run and are counted apart.

    python3 scripts/suite/rebody.py --run DIR --cases DIR --kda-compare BIN --config TOML --out DIR

Writes DIR_OUT/rebody.jsonl (one line per re-run record: old and new class, the shapes the new
compare touches, cut, one-side rows, labels and the paths left unexplained) and
DIR_OUT/rebody-summary.json (counts per endpoint). Input files are only read.
"""
import argparse
import collections
import gzip
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run  # noqa: E402

# Report buckets of a verdict class.
BUCKET = {"equal": "equal", "allowlisted": "known", "known-gap": "known", "incomplete": "incomplete",
          "diff": "diff", "status": "status", "stock-unavailable": "stock-unavailable", "ours-429": "ours-429"}
BALANCE_TEXT_RE = re.compile(r"/balanceText$")
MAX_PATHS = 50


def shapes(ep, ab, sb):
    """Which parts of the WP-H1 compare a body pair touches (read before normalize_pair)."""
    out = []
    if ep == "compat.v1txs" and isinstance(ab, list) and isinstance(sb, list):
        out.append("v1txs")
    if "balance" in (run._data(ab) or {}) and "balance" in (run._data(sb) or {}):
        out.append("balance")
    if run._nested_block_txs(ab) or run._nested_block_txs(sb):
        out.append("nested-tx")
    if run.page_cut(ab, "ours") or run.page_cut(sb, "stock"):
        out.append("page-cut")
    return out


def bucket(k, v):
    """The report bucket: a diff whose only unexplained entries are `balanceText` is
    `balance-text` (WP-H1 M1: same exact value, other JSON text; not accepted anywhere)."""
    b = BUCKET.get(k, k)
    if b == "diff":
        ents = run.rule_entries(v)[0]
        if ents and all(BALANCE_TEXT_RE.search(e.get("path") or "") for e in ents):
            return "balance-text"
    return b


def _parse(text):
    return run.parse(text.encode("utf-8"))


def rejudge(rec, case, pd):
    """The record judged again from its kept bodies: (new stored `v`, shapes), or None when it
    cannot be (no bodies, no case, or a side the run did not pair-diff)."""
    if not rec.get("bodies") or case is None:
        return None
    ours, stock = rec.get("ours") or {}, rec.get("stock") or {}
    if ours.get("st") == 429 or run.load_error(stock.get("st")) or not (ours.get("st") and stock.get("st")):
        return None
    ep = rec["ep"]
    fam = "graphql" if ep.startswith("gql.") else "rest"
    tip = (rec.get("tip") or {}).get("ours") or (rec.get("idx") or {}).get("ours")
    req = run.resolve(case["req"], tip) if tip else case["req"]
    ours_raw, stock_raw = run.unpack_bodies(rec["bodies"])
    ab, sb = _parse(ours_raw), _parse(stock_raw)
    codes = run.gql_error_codes(ab) if fam == "graphql" else []
    sh = shapes(ep, ab, sb)
    ab, sb, aligned = run.normalize_pair(ep, ab, sb, run.page_limit(ep, req))
    rq = req["path"] if fam != "graphql" else run.gql_request(req)
    v = pd({"id": rec["id"], "kind": fam, "request": rq, "max": tip, "full": True,
            "ours": {"status": ours["st"], "body": ab}, "stock": {"status": stock["st"], "body": sb}})
    k, ids = run.verdict_class(ep, rec.get("cls"), v, codes, req.get("variables") or {}, rec.get("node"),
                               rec.get("tip"), req.get("query"), rec.get("idx"), cut=aligned["cut"])
    nv = run.stored_verdict(dict(v, **{x: aligned[x] for x in ("cut", "one_side", "labels") if aligned[x]}), k, ids)
    return nv, sh


def out_line(rec, nv, sh):
    old = rec.get("v") or {}
    ents = run.rule_entries(nv)[0]
    return {"id": rec["id"], "ep": rec["ep"], "old": old.get("class"), "old_bucket": bucket(old.get("class"), old),
            "old_ids": old.get("ids") or [], "new": nv["class"], "new_bucket": bucket(nv["class"], nv),
            "ids": nv.get("ids") or [], "shapes": sh, "cut": nv.get("cut") or [],
            "one_side": nv.get("one_side") or [], "labels": nv.get("labels") or [],
            "left": [e.get("path") for e in ents][:MAX_PATHS], "left_total": len(ents)}


def summarize(lines, skipped):
    """Per endpoint: old bucket counts, new bucket counts, transitions, and per shape."""
    per = collections.defaultdict(lambda: {"old": collections.Counter(), "new": collections.Counter(),
                                           "moves": collections.Counter()})
    by_shape = collections.defaultdict(lambda: collections.Counter())
    for ln in lines:
        p = per[ln["ep"]]
        p["old"][ln["old_bucket"]] += 1
        p["new"][ln["new_bucket"]] += 1
        p["moves"]["%s->%s" % (ln["old_bucket"], ln["new_bucket"])] += 1
        for s in ln["shapes"] or ["other"]:
            by_shape[s]["%s->%s" % (ln["old_bucket"], ln["new_bucket"])] += 1
    return {"endpoints": {ep: {k: dict(c) for k, c in p.items()} for ep, p in sorted(per.items())},
            "shapes": {s: dict(c) for s, c in sorted(by_shape.items())},
            "equal_to_diff": [ln["id"] for ln in lines if ln["old_bucket"] == "equal"
                              and ln["new_bucket"] in ("diff", "status", "balance-text")],
            "skipped": {k: dict(c) for k, c in skipped.items()}}


def load_cases(cases_dir, ids_by_ep):
    out = {}
    for ep, ids in ids_by_ep.items():
        fn = os.path.join(cases_dir, ep + ".jsonl.gz")
        if not os.path.exists(fn):
            continue
        for ln in gzip.open(fn, "rt"):
            c = json.loads(ln)
            if c["id"] in ids:
                out[c["id"]] = c
    return out


def stored(run_dir):
    for fname in ("results.jsonl", "sample.jsonl"):
        path = os.path.join(run_dir, fname)
        if os.path.exists(path):
            for ln in open(path):
                r = json.loads(ln)
                if r.get("v"):
                    yield r


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--run", required=True, help="run dir with results.jsonl and sample.jsonl (read only)")
    ap.add_argument("--cases", required=True, help="cases dir (<ep>.jsonl.gz)")
    ap.add_argument("--kda-compare", required=True)
    ap.add_argument("--config", required=True, help="kda-compare config (the run's [diff] config)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--eps", nargs="*", help="only these endpoints")
    a = ap.parse_args(argv)
    os.makedirs(a.out, exist_ok=True)
    recs, want = [], collections.defaultdict(set)
    skipped = collections.defaultdict(collections.Counter)
    for r in stored(a.run):
        if a.eps and r["ep"] not in a.eps:
            continue
        if not r.get("bodies"):
            skipped[r["ep"]]["no-bodies:%s" % r["v"].get("class")] += 1
            continue
        recs.append(r)
        want[r["ep"]].add(r["id"])
    cases = load_cases(a.cases, want)
    pd = run.PairDiff(a.kda_compare, a.config)
    lines = []
    with open(os.path.join(a.out, "rebody.jsonl"), "w") as f:
        for r in recs:
            got = rejudge(r, cases.get(r["id"]), pd)
            if got is None:
                skipped[r["ep"]]["not-rejudged:%s" % r["v"].get("class")] += 1
                continue
            ln = out_line(r, *got)
            lines.append(ln)
            f.write(json.dumps(ln, separators=(",", ":")) + "\n")
    s = summarize(lines, skipped)
    json.dump(s, open(os.path.join(a.out, "rebody-summary.json"), "w"), indent=1, sort_keys=True)
    print("re-run %d records; equal->diff %d; skipped %d" % (len(lines), len(s["equal_to_diff"]),
                                                            sum(sum(c.values()) for c in skipped.values())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
