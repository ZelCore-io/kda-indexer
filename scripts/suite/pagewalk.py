"""Page-walk mode (WP-H1): a GraphQL list walked to its end on each side, compared as a set.

A paired compare sees only the page the client asked for. Here each side is walked from the
first page with its own cursors (a stock cursor means nothing to ours, and the other way round),
at most MAX_PAGES pages and RPS requests per second per side. The combined rows of the two sides
are compared as multisets by row key, plus `totalCount`. A side that did not reach its last page
(the page cap, an error, a missing cursor) makes the record `incomplete`, never equal. Rows only
one side lists are named by key and, when a node is configured, read on the node
(nodecheck.Node.evidence for rows that carry chain, height and block).

Used by `run.py page-walk` (see its --help); stdlib only.
"""

import collections
import json
import time

MAX_PAGES = 50          # pages per side, at most
RPS = 2.0               # requests per second per side, at most
KEY_ORDER = (("hash",), ("requestKey", "orderIndex", "chainId"), ("chainId", "module"), ("id",))


class Pacer:
    """At most `rps` requests per second for one side (a fixed gap between two requests)."""

    def __init__(self, rps=RPS, clock=time.monotonic, sleep=time.sleep):
        if not 0 < rps <= RPS:
            raise ValueError("page-walk rate must be in (0, %s] requests per second per side" % RPS)
        self.gap, self.clock, self.sleep, self.next = 1.0 / rps, clock, sleep, None

    def wait(self):
        t = self.clock()
        if self.next is not None and self.next > t:
            self.sleep(self.next - t)
            t = self.next
        self.next = t + self.gap


def conn_at(data, path):
    """The connection at a dotted path under `data` (`transactions`, `block.transactions`); a
    numeric segment indexes a list."""
    x = data
    for seg in path.split("."):
        if isinstance(x, list) and seg.isdigit() and int(seg) < len(x):
            x = x[int(seg)]
        elif isinstance(x, dict):
            x = x.get(seg)
        else:
            return None
    return x if isinstance(x, dict) else None


def default_path(ep, data=None):
    """The walked connection: the endpoint's root field (`gql.transfers` -> `transfers`), else the
    only key of the answer's `data`."""
    if ep and ep.startswith("gql."):
        return ep[4:]
    if isinstance(data, dict) and len(data) == 1:
        return next(iter(data))
    return None


def walk(fetch, query, variables, path, max_pages=MAX_PAGES, pacer=None, cursor_var="after"):
    """Walks one side. `fetch(query, vars)` -> (status, body). The walk starts at the first page
    (the case's `after`, `before` and `last` are dropped; its `first` is the page size) and follows
    `pageInfo.endCursor` in `cursor_var` until `hasNextPage` is false.
    Returns {"rows", "totalCount", "pages", "done", "why"}; `done` only when the last page said
    there is no next one."""
    if not 0 < max_pages <= MAX_PAGES:
        raise ValueError("page-walk page cap must be in 1..%d" % MAX_PAGES)
    pacer = pacer or Pacer()
    v = {k: x for k, x in (variables or {}).items() if k not in ("last", "before", cursor_var)}
    out = {"rows": [], "totalCount": None, "pages": 0, "done": False, "why": None, "counts": []}
    while out["pages"] < max_pages:
        pacer.wait()
        st, body = fetch(query, dict(v))
        out["pages"] += 1
        if st != 200 or not isinstance(body, dict):
            out["why"] = "status %s" % st
            return out
        if body.get("errors"):
            out["why"] = "errors"
            return out
        data = body.get("data")
        conn = conn_at(data, path or default_path(None, data) or "") if isinstance(data, dict) else None
        if conn is None or not isinstance(conn.get("edges"), list):
            out["why"] = "no connection at %s" % path
            return out
        tc = conn.get("totalCount")
        out["counts"].append(tc)
        if out["totalCount"] is None:
            out["totalCount"] = tc
        out["rows"] += [e.get("node") if isinstance(e, dict) else e for e in conn["edges"]]
        pi = conn.get("pageInfo")
        if not isinstance(pi, dict) or "hasNextPage" not in pi:
            out["why"] = "no pageInfo.hasNextPage selected"
            return out
        if not pi["hasNextPage"]:
            out["done"] = True
            return out
        if not pi.get("endCursor"):
            out["why"] = "no endCursor"
            return out
        v[cursor_var] = pi["endCursor"]
    out["why"] = "page-cap"
    return out


def row_key(n):
    """A row's key: the first full set of KEY_ORDER fields it selects, else its whole text."""
    if isinstance(n, dict):
        for fields in KEY_ORDER:
            if fields[0] in n:
                return "|".join(str(n.get(f)) for f in fields if f in n)
    return json.dumps(n, sort_keys=True)


def compare(a, b):
    """Two walks (ours, stock) as multisets by key: rows only one side lists, rows whose content
    differs under one key, both totalCounts. `status`: incomplete when either walk did not finish,
    else diff on any difference (rows or counts), else equal."""
    ka = collections.defaultdict(list)
    kb = collections.defaultdict(list)
    for n in a["rows"]:
        ka[row_key(n)].append(json.dumps(n, sort_keys=True))
    for n in b["rows"]:
        kb[row_key(n)].append(json.dumps(n, sort_keys=True))
    one = {"ours": [], "stock": []}
    changed = []
    for k in sorted(set(ka) | set(kb)):
        xa, xb = collections.Counter(ka.get(k, [])), collections.Counter(kb.get(k, []))
        if xa == xb:
            continue
        na, nb = len(ka.get(k, [])), len(kb.get(k, []))
        if na > nb:
            one["ours"] += [k] * (na - nb)
        elif nb > na:
            one["stock"] += [k] * (nb - na)
        else:
            changed.append(k)
    counts = [a.get("totalCount"), b.get("totalCount")]
    rows = [len(a["rows"]), len(b["rows"])]
    if not (a["done"] and b["done"]):
        status = "incomplete"
    elif one["ours"] or one["stock"] or changed or counts[0] != counts[1]:
        status = "diff"
    else:
        status = "equal"
    rows_by_key = {"ours": {row_key(n): n for n in a["rows"]}, "stock": {row_key(n): n for n in b["rows"]}}
    return {"status": status, "totalCount": counts, "rows": rows, "one_side": one, "changed": changed,
            "count_moved": [len(set(x.get("counts") or [])) > 1 for x in (a, b)],
            "why": [a.get("why"), b.get("why")], "_rows": rows_by_key}


def _get(n, *path):
    for p in path:
        n = n.get(p) if isinstance(n, dict) else None
    return n


def node_row(n):
    """{key, chain, height, block} of a row for nodecheck.Node.evidence, else None (no node path)."""
    chain = n.get("chainId", _get(n, "cmd", "meta", "chainId")) if isinstance(n, dict) else None
    height = n.get("height", _get(n, "result", "height")) if isinstance(n, dict) else None
    block = n.get("blockHash", _get(n, "result", "block", "hash")) if isinstance(n, dict) else None
    try:
        chain = int(chain)
    except (TypeError, ValueError):
        return None
    if not isinstance(height, int) or not block:
        return None
    return {"key": row_key(n), "chain": chain, "height": height, "block": block}


def node_check(node, cmp, max_reads=20):
    """One-side rows read on the node, per side: the node's canonical header at each row's chain and
    height against the row's block. None without a node; a side with no row the node can place
    reports how many rows had no node path."""
    if node is None:
        return None
    out = {}
    for side in ("ours", "stock"):
        keys = cmp["one_side"][side]
        if not keys:
            continue
        rows = [node_row(cmp["_rows"][side].get(k)) for k in keys]
        have = [r for r in rows if r]
        res = node.evidence(have, max_reads) if have else {}
        out[side] = dict(res, no_node_path=len(rows) - len(have))
    return out


def run_case(case, fetch_ours, fetch_stock, node, max_pages=MAX_PAGES, pacers=None):
    """One case walked on both sides; the record run.py page-walk writes (no rows, keys only)."""
    req = case.get("req") or {}
    walk_cfg = case.get("walk") or {}
    path = walk_cfg.get("path") or default_path(case.get("ep"))
    cv = walk_cfg.get("cursor_var", "after")
    pacers = pacers or (Pacer(), Pacer())
    a = walk(fetch_ours, req["query"], req.get("variables") or {}, path, max_pages, pacers[0], cv)
    b = walk(fetch_stock, req["query"], req.get("variables") or {}, path, max_pages, pacers[1], cv)
    c = compare(a, b)
    rec = {"id": case.get("id"), "ep": case.get("ep"), "path": path, "status": c["status"],
           "pages": [a["pages"], b["pages"]], "rows": c["rows"], "totalCount": c["totalCount"],
           "one_side": c["one_side"], "changed": c["changed"], "why": c["why"], "count_moved": c["count_moved"]}
    nd = node_check(node, c)
    if nd is not None:
        rec["node"] = nd
    return rec
