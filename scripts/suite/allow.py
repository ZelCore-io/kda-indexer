"""Suite allowlist: the run-1 triage's DOCUMENTED and STOCK-WRONG classes (stdlib only).

`kda-compare pair-diff` gives the first verdict with the live harness's allowlist. The
suite then looks at the diff entries that are left: a row whose every entry is explained
by a rule below becomes `allowlisted`, with the rule ids (a row an our-defect rule touches
stays `diff`, H6 B1; one whose stock side gave nothing to compare is `stock-unavailable`, S2). A rule never explains an entry on its own say-so where the triage used
the node as referee: those rules need the row's node check to have passed.

Each rule names its verdict and reason (suite-run1-triage.md sections in brackets), and
carries an accept class and a ruling (H2, owner R-P5P9-162): `table()` lists every rule with
id, reason, accept class and ruling, so the docs can say why each difference is accepted.
OURS-BUG classes have no rule here: they stay open diffs until ours is fixed.
"""

import base64
import collections
import decimal
import json
import re

# Why a rule accepts a difference (the same classes as `accept_class` in compare/allowlist.toml).
ACCEPT_CLASSES = ("stock-wrong-node-evidence", "intended-documented", "unsupported-by-design", "tip-timing",
                  "stock-self-inconsistent", "stock-stale", "harness-convention", "our-defect",
                  "stock-unavailable")
# H6 (S2): a rule of this class accepts nothing. Where it explains every difference left, stock
# gave no value to compare (an error, a -1 sentinel) and the record is `stock-unavailable`.
UNAVAILABLE = "stock-unavailable"
TIP_WINDOW = 4          # heights the tip may move between ours' read and stock's (about 2 minutes)
MAX_SHOWN = 8           # kda-compare pair-diff's display cut (crates/kdx-compare/src/pair.rs MAX_SHOWN)
CHAINS = 20
MAX_BLOCK_REWARD = 2.5  # KDA a block can mint at most (above the highest reward of the emission schedule)


class Rule:
    def __init__(self, id, verdict, reason, eps, fn, node=False, classes=None):
        self.id, self.verdict, self.reason, self.eps, self.fn = id, verdict, reason, eps, fn
        self.node, self.classes = node, classes
        self.accept, self.ruling = META[id]       # a rule without META is refused at import
        assert self.accept in ACCEPT_CLASSES, id
        self.doc = DOC.get(id, _DIFF % "intended-differences" if id.startswith("tag:") else None)

    def explains(self, ctx):
        """Indices of the entries this rule explains (empty: none)."""
        if self.eps and ctx["ep"] not in self.eps:
            return set()
        if self.classes and ctx["cls"] not in self.classes:
            return set()
        if self.node and not node_passed(ctx.get("node")):
            return set()
        if self.doc is None and self.id != "known-gap-missing-chain-rows":
            return set()                      # owner H4 rule 3: undocumented, never accepted
        return self.fn(ctx) or set()


def node_passed(node):
    """The row's node check passed. A near-tip mismatch that no fresh re-read settled (`tip-race`,
    every record before H4) is a failed check, whatever `ok` says."""
    node = node or {}
    return node.get("ok") is True and "tip-race" not in (node.get("why") or [])


def complete(ctx):
    """Every difference of the row is in ctx["entries"]. A stored record without the flag holds
    pair-diff's display list: MAX_SHOWN entries there may be a cut list."""
    c = ctx.get("complete")
    if c is not None:
        return bool(c)
    return len(ctx.get("entries") or []) < MAX_SHOWN


# id -> (accept class, ruling). The ruling names where the difference was accepted.
_T1 = "Suite run-1 triage, section %s; owner R-P5P9-162 (list every difference with why)."
_T2 = "Suite run-2 triage, %s; owner R-P5P9-162 (list every difference with why)."
META = {
    "pactQuery-not-supported": ("unsupported-by-design", _T1 % "1 (docs/graphql.md: NOT_SUPPORTED)"),
    "gasLimitEstimate-not-supported": ("unsupported-by-design", _T1 % "4 (docs/graphql.md: NOT_SUPPORTED)"),
    "networkInfo-totalGasUsedInKda": ("unsupported-by-design", _T1 % "2 (docs table: NOT_AVAILABLE)"),
    "networkInfo-stock-error": (UNAVAILABLE, _T1 % "2 (stock's own INTERNAL_SERVER_ERROR); H6 S2, R-P5P9-172: "
                                "no node read, so the record is stock-unavailable, never allowlisted"),
    "networkInfo-totalDifficulty": (UNAVAILABLE, _T1 % "2 (stock answers -1); H6 S2, R-P5P9-172: no node read, "
                                    "so the record is stock-unavailable, never allowlisted"),
    "networkInfo-coins-stale": ("stock-wrong-node-evidence", _T1 % "2 (stock's value lags; H4: the node tip "
                                "sampled at stock's answer is above stock's own lastBlockHeight)"),
    "completedBlockHeights-stock-error": ("stock-wrong-node-evidence", _T1 % "3 (node check passes on the row)"),
    "bad-input-vs-stock-error": ("intended-documented", _T1 % "7b (docs Pages)"),
    "ours-refuses-lenient-input": ("intended-documented", _T1 % "7c and 1 (docs: lenient input is refused)"),
    "node-undecodable-id": ("intended-documented", _T1 % "1 (docs Blocks and network)"),
    "guard-keyset-reference": ("intended-documented", _T1 % "5 and 7d (docs Accounts)"),
    "known-gap-missing-chain-rows": ("our-defect", _T1 % "7d (known gap Q: OUR DEFECT, gated by the baseline)"),
    "lastBlockHeight-tip-race": ("tip-timing", _T1 % "7d (up to 3 blocks between the two calls); H6 S2: only "
                                 "where the node tip recorded with the row moved the same way, at least as far"),
    "multi-block-order": ("intended-documented", _T2 % "order class (owner-accepted documented difference, docs/graphql.md; "
                          "H4: same rows on both pages at the same heights)"),
    "totalcount-stock-orphans": ("stock-wrong-node-evidence", _T2 % "orphan-count rule (H2 item 3; H4: every stock-only "
                                 "row is a fork header the node holds, exactly the gap)"),
    "failed-tx-callstack": ("stock-wrong-node-evidence",
                            "Owner ruling 2026-11-19 (R-P5P9-170): accepted as a stock fault, the node's stored "
                            "/poll output is the reference; same id as compare/allowlist.toml failed-tx-callstack. "
                            "WP-H1: the GraphQL badResult form, no longer filed under graphql-r57-json-object."),
    "tip-timing": ("tip-timing", _T2 % "tip-staleness timing (H2 item 5; H4: every differing row lies between the two "
                   "observed tips)"),
}


_DIFF = "docs/comparison.md#%s"
DOC = {
    "pactQuery-not-supported": "docs/graphql.md#errors",
    "gasLimitEstimate-not-supported": "docs/graphql.md#errors",
    "networkInfo-totalGasUsedInKda": "docs/graphql.md#errors",
    "bad-input-vs-stock-error": "docs/graphql.md#errors",
    "ours-refuses-lenient-input": "docs/graphql.md#errors",
    "node-undecodable-id": "docs/graphql.md#errors",
    "guard-keyset-reference": "docs/graphql.md#accounts",
    "multi-block-order": _DIFF % "endpoint-suite-rules",
    "tip-timing": _DIFF % "endpoint-suite-rules",
    "lastBlockHeight-tip-race": _DIFF % "endpoint-suite-rules",
    "networkInfo-stock-error": _DIFF % "stock-faults-the-node-disproves",
    "networkInfo-totalDifficulty": _DIFF % "stock-faults-the-node-disproves",
    "networkInfo-coins-stale": _DIFF % "stock-faults-the-node-disproves",
    "completedBlockHeights-stock-error": _DIFF % "stock-faults-the-node-disproves",
    "totalcount-stock-orphans": _DIFF % "stock-faults-the-node-disproves",
    "failed-tx-callstack": _DIFF % "endpoint-suite-rules",
    "known-gap-missing-chain-rows": None,          # our defect: a known gap, never accepted
}


def _codes(ctx):
    return ctx.get("ours_codes") or []


def _all(ctx, pred):
    """Every entry matches `pred`: all indices, else none."""
    ents = ctx["entries"]
    return set(range(len(ents))) if ents and all(pred(e) for e in ents) else set()


def _ise_on_stock(e):
    return "INTERNAL_SERVER_ERROR" in str(e.get("b", ""))


def _data_or_errors(e):
    return e["path"] in ("/data", "/errors") or re.fullmatch(r"/data/\w+", e["path"]) is not None


def _num(s):
    """The diff's number text (`#<digits>e<exp>` or plain) as an exact decimal (never a float:
    owner H4 rule, number parity)."""
    if isinstance(s, bool):
        return None
    try:
        d = decimal.Decimal(str(s).lstrip("#"))
    except (decimal.InvalidOperation, ValueError):
        return None
    return d if d.is_finite() else None


def _not_supported(ctx):
    if _codes(ctx) and all(c == "NOT_SUPPORTED" for c in _codes(ctx)):
        return _all(ctx, lambda e: _data_or_errors(e) or e["path"].startswith("/errors/"))
    return set()


def _gas_not_supported(ctx):
    # Only where stock fails too (INTERNAL_SERVER_ERROR on every input). `input: []`, where
    # stock answers `[]`, is OURS-BUG-4 and stays open.
    if not (_codes(ctx) and all(c == "NOT_SUPPORTED" for c in _codes(ctx))):
        return set()
    if not any(_ise_on_stock(e) for e in ctx["entries"]):
        return set()
    return _all(ctx, lambda e: _data_or_errors(e) or e["path"].startswith("/errors/"))


def _gas_used(ctx):
    if any(c != "NOT_AVAILABLE" for c in _codes(ctx)):
        return set()
    return {i for i, e in enumerate(ctx["entries"])
            if re.fullmatch(r"/data/networkInfo/countersOfEachChain/\d+/totalGasUsedInKda", e["path"])
            or (e["path"] == "/errors" and e["kind"] == "Extra")}


def _network_info_ise(ctx):
    if _codes(ctx):
        return set()
    return _all(ctx, lambda e: (e["path"] == "/data/networkInfo" and e["kind"] == "Type" and str(e.get("b")) == "None")
                or (e["path"] == "/errors" and e["kind"] == "Missing" and _ise_on_stock(e)))


def _total_difficulty(ctx):
    return {i for i, e in enumerate(ctx["entries"])
            if e["path"] == "/data/networkInfo/totalDifficulty" and _num(e.get("b")) == -1}


def _coins_stale(ctx):
    # Stock's value lags. Explained only where the node shows stock behind: the node tip sampled
    # at stock's answer (tip.stock) is above stock's own lastBlockHeight (idx.stock), ours' own
    # height is ahead of stock's, and ours' excess is at most what the missing heights can mint
    # (CHAINS blocks per height, MAX_BLOCK_REWARD each).
    t, x = ctx.get("tip") or {}, ctx.get("idx") or {}
    ns, so, oo = t.get("stock"), x.get("stock"), x.get("ours")
    if not all(isinstance(v, int) for v in (ns, so, oo)) or not (ns > so and oo > so):
        return set()
    bound = (oo - so) * CHAINS * decimal.Decimal(str(MAX_BLOCK_REWARD))
    out = set()
    for i, e in enumerate(ctx["entries"]):
        if e["path"] == "/data/networkInfo/coinsInCirculation" and e["kind"] == "Changed":
            a, b = _num(e.get("a")), _num(e.get("b"))
            if a is not None and b is not None and 0 < a - b <= bound:
                out.add(i)
    return out


def _cbh_ise(ctx):
    ch = (ctx.get("vars") or {}).get("chainIds", None)
    if ch not in (None, []):
        return set()
    return _network_like_ise(ctx, "/data")


def _network_like_ise(ctx, data_path):
    if _codes(ctx):
        return set()
    return _all(ctx, lambda e: (e["path"] == data_path and e["kind"] == "Type" and str(e.get("b")) == "None")
                or (e["path"] == "/errors" and e["kind"] == "Missing" and _ise_on_stock(e)))


def _bad_input_vs_ise(ctx):
    if not (_codes(ctx) and all(c == "BAD_USER_INPUT" for c in _codes(ctx))):
        return set()
    if not any(e["path"].startswith("/errors/") and e["path"].endswith("/code") and _ise_on_stock(e)
               for e in ctx["entries"]):
        return set()
    return _all(ctx, lambda e: _data_or_errors(e) or e["path"].startswith("/errors/") or e["path"].startswith("/data/"))


def _ours_refuses(ctx):
    # 7c lenient input and node/nodes ids stock cannot read: ours BAD_USER_INPUT, stock
    # answers (null or data) without an error.
    if not (_codes(ctx) and all(c == "BAD_USER_INPUT" for c in _codes(ctx))):
        return set()
    return _all(ctx, lambda e: (e["path"] == "/data" and str(e.get("a")) == "None")
                or (e["path"] == "/errors" and e["kind"] == "Extra"))


def _undecodable(i):
    """True when a node id is not Kadena's `<Type>:<key>` layout in standard base64."""
    try:
        raw = base64.b64decode(str(i), validate=True).decode()
    except Exception:
        return True
    return re.fullmatch(r"[A-Za-z]+:.+", raw, re.S) is None


def _node_bad_id(ctx):
    v = ctx.get("vars") or {}
    ids = v.get("ids") if ctx["ep"] == "gql.nodes" else [v.get("id")]
    if not isinstance(ids, list) or not any(_undecodable(i) for i in ids):
        return set()
    return _ours_refuses(ctx)


def _keyset_guard(ctx):
    out = set()
    ents = ctx["entries"]
    for i, e in enumerate(ents):
        m = re.fullmatch(r"(.*/guard)/(__typename|keys|predicate)", e["path"])
        if not m:
            continue
        base = m.group(1)
        if any(x["path"] == base + "/__typename" and str(x.get("a")) == "KeysetGuard" and str(x.get("b")) == "RawGuard"
               for x in ents):
            out.add(i)
    return out


# Visible harness hits already accepted in compare/allowlist.toml, relayed with that entry's own
# accept class (the test reads the TOML file and compares).
TAG_CLASS = {
    "graphql-r57-json-object": "harness-convention",
    "graphql-r58-count-lower-bound": "intended-documented",
    "graphql-totalcount-cap": "intended-documented",
    "graphql-r57-height-ties": "intended-documented",
}
TAGS_ALLOWED = tuple(TAG_CLASS)
# H3: the visible hit an entry's glob names where it is not the entry id (the tie-cut hit of
# graphql-r57-height-ties never matched its tag before).
TAG_HIT = {"graphql-r57-height-ties": "graphql-r57-height-tie-cut"}
for _t, _c in TAG_CLASS.items():
    META["tag:" + _t] = (_c, _T1 % "7d (tag accepted in compare/allowlist.toml as %s, relayed with its class)" % _t)


def _tag(tag):
    def fn(ctx):
        return {i for i, e in enumerate(ctx["entries"]) if e["path"].lstrip("/~") == tag}
    return fn


def _gap_tag(ctx):
    return {i for i, e in enumerate(ctx["entries"]) if e["path"] == "/~known-gap-missing-chain-rows"}


NODE_INFO = "<toplevel>:0"      # the node's stored `info` of a failed tx


def _json_obj(x):
    if not isinstance(x, str) or not x.lstrip().startswith("{"):
        return None
    try:
        v = json.loads(x, parse_float=decimal.Decimal)
    except ValueError:
        return None
    return v if isinstance(v, dict) else None


def _failed_tx_callstack(ctx):
    """WP-H1: a failed tx's GraphQL `badResult` (a JSON text) where stock holds a `callStack` ours
    lacks; an `info` difference is part of it only where ours answers the node form exactly. Every
    other key must be equal. Ours holding a callStack, or any other difference, stays a diff."""
    out = set()
    for i, e in enumerate(ctx["entries"]):
        if not e["path"].endswith("/badResult") or e.get("kind") != "Changed":
            continue
        a, b = _json_obj(e.get("a")), _json_obj(e.get("b"))
        if a is None or b is None or "callStack" in a or "callStack" not in b:
            continue
        b = {k: x for k, x in b.items() if k != "callStack"}
        if a.get("info") != b.get("info"):
            if a.get("info") != NODE_INFO or "info" not in b:
                continue
            a = {k: x for k, x in a.items() if k != "info"}
            b = {k: x for k, x in b.items() if k != "info"}
        if a == b:
            out.add(i)
    return out


def _tip_race(ctx):
    # H6 (S2): node tip evidence stored with the record. The node tip at ours' read (tip.ours)
    # and at stock's (tip.stock) moved by t; the two answers differ by d with the same sign,
    # |d| <= |t| and |d| <= 3. No recorded tip, or a tip that did not move that way: a diff.
    t = ctx.get("tip") or {}
    lo, hi = t.get("ours"), t.get("stock")
    if not all(isinstance(x, int) and not isinstance(x, bool) for x in (lo, hi)) or lo == hi:
        return set()
    out = set()
    for i, e in enumerate(ctx["entries"]):
        if e["path"] == "/data/lastBlockHeight" and e["kind"] == "Changed":
            a, b = _num(e.get("a")), _num(e.get("b"))
            if a is not None and b is not None and a != b and (b > a) == (hi > lo) \
                    and abs(b - a) <= min(3, abs(hi - lo)):
                out.add(i)
    return out


ORDER_EPS = {"gql.events": "events", "gql.transactions": "transactions", "gql.transfers": "transfers"}
ORDER_ID_FIELDS = {"id", "hash", "requestKey", "cursor"}
HEIGHT_SEL = re.compile(r"\bheight\b")


def _single_block(v):
    """A list bound to one block: FIX-4 lists it in node order, so a diff there stays open."""
    if v.get("blockHash") or v.get("requestKey"):
        return True
    lo, hi = v.get("minHeight"), v.get("maxHeight")
    return bool(v.get("chainId")) and lo is not None and lo == hi


def _leaf(field):
    return field.rsplit("/", 1)[-1]


def edge_table(ctx, root):
    """Positional list entries of the row: ({pos: {field: entry}}, {pos: whole-edge entry},
    [other entries]). `field` is the path below `edges/<pos>/` (`node/requestKey`, `cursor`)."""
    pat = re.compile(r"/data/%s/edges/(\d+)(?:/(.+))?" % re.escape(root))
    fields, whole, other = collections.defaultdict(dict), {}, []
    for e in ctx["entries"]:
        m = pat.fullmatch(e["path"])
        if not m:
            other.append(e)
        elif m.group(2) is None:
            whole[int(m.group(1))] = e
        else:
            fields[int(m.group(1))][m.group(2)] = e
    return fields, whole, other


def _multi_block_order(ctx):
    # Multi-block newest-first lists keep node order inside a block (coinbase last); stock orders
    # by tx creationTime. Explained only where (H4, B2) the entries are all the differences, the
    # query selects the height, and the two pages hold the same rows at the same heights: no
    # height or chain differs at any position, the changed positions hold the same multiset of
    # row ids on both sides, and every field that differs on a moved row has the same value on
    # the row's other position. A count, a page-info change or a whole edge keeps the row open.
    # Pages cut inside their last block (different rows of that block) stay open: the stored
    # entries do not carry the unchanged heights that would locate the cut.
    root = ORDER_EPS.get(ctx["ep"])
    if root is None or _single_block(ctx.get("vars") or {}) or not complete(ctx):
        return set()
    if not HEIGHT_SEL.search(ctx.get("query") or ""):
        return set()
    fields, whole, other = edge_table(ctx, root)
    if other or whole or not fields:
        return set()
    for f in fields.values():
        for k, e in f.items():
            if e["kind"] != "Changed" or _leaf(k) in ("height", "chainId"):
                return set()
    keys = sorted(set.intersection(*(set(k for k in f if _leaf(k) in ORDER_ID_FIELDS) for f in fields.values())))
    if not keys:
        return set()

    def ident(pos, side):
        return tuple(json.dumps(fields[pos][k].get(side), sort_keys=True) for k in keys)
    a_at = {pos: ident(pos, "a") for pos in fields}
    b_at = {pos: ident(pos, "b") for pos in fields}
    if collections.Counter(a_at.values()) != collections.Counter(b_at.values()):
        return set()
    if len(set(a_at.values())) != len(a_at):
        return set()                     # a row twice on one page: no one-to-one match
    where_b = {v: pos for pos, v in b_at.items()}
    for i, ident_a in a_at.items():
        j = where_b[ident_a]
        for k in set(fields[i]) | set(fields[j]):
            if k not in fields[i] or k not in fields[j] or fields[i][k].get("a") != fields[j][k].get("b"):
                return set()
    return set(range(len(ctx["entries"])))


def _int_field(ctx, k):
    v = (ctx.get("node") or {}).get(k)
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def _totalcount_stock_orphans(ctx):
    # Stock counts orphan copies. Explained only on exact accounting (H4, S1): the evidence lists
    # were complete, every row only stock lists (`extra`) is a row in a non-canonical block
    # whose fork header the node holds (`stock_orphans` == `forks_confirmed`), none is
    # unresolved, ours lists no row stock lacks, and the count gap is exactly that number.
    n = _int_field(ctx, "stock_orphans")
    if n is None or n <= 0:
        return set()
    if _int_field(ctx, "extra") != n or _int_field(ctx, "forks_confirmed") != n or _int_field(ctx, "unresolved") != 0:
        return set()
    oo = (ctx.get("node") or {}).get("ours_only")
    if oo not in (None, 0):
        return set()
    out = set()
    for i, e in enumerate(ctx["entries"]):
        if e["kind"] != "Changed" or not re.fullmatch(r"/data/\w+/totalCount", e["path"]):
            return set()
        a, b = _num(e.get("a")), _num(e.get("b"))
        if a is None or b is None or b - a != n:
            return set()
        out.add(i)
    return out


TIP_EPS = {"gql.blocksFromDepth", "gql.blocksFromHeight", "gql.transfers", "gql.transactions", "gql.events",
           "gql.lastBlockHeight"}
TIP_ANCHORS = ("blockHash", "requestKey", "maxHeight", "endHeight", "after", "before")
NEWEST_FIRST = {"gql.blocksFromDepth", "gql.transfers", "gql.transactions", "gql.events"}


def _height_in(v):
    """The `height` found in an edge object (any depth), as a number."""
    if isinstance(v, dict):
        for k, x in v.items():
            if k == "height":
                return _num(x)
        for x in v.values():
            h = _height_in(x)
            if h is not None:
                return h
    return None


def _tip_timing(ctx):
    # H4 (B1): the node tip moved by 1..TIP_WINDOW heights between ours' read and stock's
    # (ctx["tip"] = {"ours": lo, "stock": hi}, recorded in the row), on a list that follows the
    # tip, and every difference is a row between the two tips:
    # - lastBlockHeight: stock higher than ours by at most hi - lo;
    # - newest-first lists: stock's first k rows are new (their height in (lo, hi], minus
    #   minimumDepth for blocksFromDepth; ours' row there not above lo), every later position
    #   holds ours' row k places up (each differing field equals ours' value there), at least
    #   one such shifted row proves it, and a totalCount gap is exactly k;
    # - blocksFromHeight (ascending): only rows stock appends at the end, each between the tips.
    # Only with every difference known (`complete`).
    t = ctx.get("tip") or {}
    lo, hi = t.get("ours"), t.get("stock")
    if ctx["ep"] not in TIP_EPS or not isinstance(lo, int) or not isinstance(hi, int) or not 1 <= hi - lo <= TIP_WINDOW:
        return set()
    if not complete(ctx):
        return set()
    v = ctx.get("vars") or {}
    ents = ctx["entries"]
    if ctx["ep"] == "gql.lastBlockHeight":
        out = set()
        for i, e in enumerate(ents):
            a, b = _num(e.get("a")), _num(e.get("b"))
            if e["path"] != "/data/lastBlockHeight" or e["kind"] != "Changed" or a is None or b is None \
                    or not 0 < b - a <= hi - lo:
                return set()
            out.add(i)
        return out
    if ctx["ep"] != "gql.blocksFromDepth" and any(v.get(k) not in (None, "") for k in TIP_ANCHORS):
        return set()
    d = v.get("minimumDepth") if ctx["ep"] == "gql.blocksFromDepth" else 0
    d = d if isinstance(d, int) and d >= 0 else 0
    lo, hi = lo - d, hi - d
    root = ctx["ep"].split(".", 1)[1]
    fields, whole, other = edge_table(ctx, root)
    count = None
    for e in other:
        if e["path"] == "/data/%s/totalCount" % root and e["kind"] == "Changed":
            a, b = _num(e.get("a")), _num(e.get("b"))
            if a is None or b is None:
                return set()
            count = b - a
        elif not (re.fullmatch(r"/data/%s/pageInfo/(startCursor|endCursor)" % root, e["path"]) and e["kind"] == "Changed"):
            return set()
    if ctx["ep"] == "gql.blocksFromHeight":
        if fields or not whole:
            return set()
        for e in whole.values():
            h = _height_in(e.get("b"))
            if e["kind"] != "Missing" or h is None or not lo < h <= hi:
                return set()
        if count is not None and count != len(whole):
            return set()
        return set(range(len(ents)))
    if ctx["ep"] not in NEWEST_FIRST or whole or not fields:
        return set()
    k = 0
    while k in fields:
        he = next((e for f, e in fields[k].items() if _leaf(f) == "height"), None)
        hb = _num(he.get("b")) if he is not None and he["kind"] == "Changed" else None
        ha = _num(he.get("a")) if he is not None else None
        if hb is None or not lo < hb <= hi or (ha is not None and ha > lo):
            break
        k += 1
    last = max(fields)
    if k == 0 or last < k:
        return set()
    for j in range(k, last + 1):
        f = fields.get(j)
        if not f or not any(_leaf(x) in ORDER_ID_FIELDS for x in f):
            return set()
        for x, e in f.items():
            up = fields.get(j - k, {}).get(x)
            if e["kind"] != "Changed" or up is None or up.get("a") != e.get("b"):
                return set()
    if count is not None and count != k:
        return set()
    return set(range(len(ents)))


BAD = ("edge", "invalid")

RULES = [
    Rule("pactQuery-not-supported", "DOCUMENTED",
         "docs/graphql.md Schema: pactQuery answers NOT_SUPPORTED on ours [triage 1].",
         {"gql.pactQuery"}, _not_supported),
    Rule("gasLimitEstimate-not-supported", "DOCUMENTED",
         "docs/graphql.md: gasLimitEstimate answers NOT_SUPPORTED; stock fails with INTERNAL_SERVER_ERROR "
         "on every input, so there is no answer to copy. input: [] stays open (OURS-BUG-4) [triage 4].",
         {"gql.gasLimitEstimate"}, _gas_not_supported),
    Rule("networkInfo-totalGasUsedInKda", "DOCUMENTED",
         "countersOfEachChain.totalGasUsedInKda answers NOT_AVAILABLE on ours (docs table) [triage 2].",
         {"gql.networkInfo"}, _gas_used),
    Rule("networkInfo-stock-error", "STOCK-WRONG",
         "Stock answers networkInfo null + INTERNAL_SERVER_ERROR for every selection except totalDifficulty, "
         "coinsInCirculation and countersOfEachChain, even networkId alone (a constant) [triage 2].",
         {"gql.networkInfo"}, _network_info_ise),
    Rule("networkInfo-totalDifficulty", "STOCK-WRONG",
         "Stock totalDifficulty is -1 [triage 2].", {"gql.networkInfo"}, _total_difficulty),
    Rule("networkInfo-coins-stale", "STOCK-WRONG",
         "Stock coinsInCirculation lags the mining rate; explained only where ours is ahead [triage 2].",
         {"gql.networkInfo"}, _coins_stale),
    Rule("completedBlockHeights-stock-error", "STOCK-WRONG",
         "Stock answers INTERNAL_SERVER_ERROR in < 15 ms for every query with chainIds absent or []; "
         "explained only where this row's node check passed (canonical hashes, heights <= tip) [triage 3].",
         {"gql.completedBlockHeights"}, _cbh_ise, node=True),
    Rule("bad-input-vs-stock-error", "DOCUMENTED",
         "Both refuse a malformed hash, cursor or negative first/last: ours BAD_USER_INPUT names the "
         "input, stock crashes with INTERNAL_SERVER_ERROR (docs Pages) [triage 7b].",
         None, _bad_input_vs_ise, classes=BAD),
    Rule("ours-refuses-lenient-input", "DOCUMENTED",
         "Ours refuses a non-canonical chain id, an empty or oversized name, a bad cursor or an id it "
         "cannot decode with BAD_USER_INPUT; stock coerces it (JavaScript Number()) or answers null "
         "[triage 7c, 1]. Edge and invalid classes only.",
         None, _ours_refuses, classes=BAD),
    Rule("node-undecodable-id", "DOCUMENTED",
         "node/nodes with an id that does not decode to <Type>:<key>: ours BAD_USER_INPUT (docs Blocks and "
         "network), stock null; any case class, checked on the request's ids [triage 1].",
         {"gql.node", "gql.nodes"}, _node_bad_id),
    Rule("guard-keyset-reference", "DOCUMENTED",
         "A resolved r: keyset reference is a KeysetGuard on ours, a RawGuard on stock (docs Accounts) "
         "[triage 5, 7d].", None, _keyset_guard),
    Rule("known-gap-missing-chain-rows", "KNOWN",
         "Zero-balance chain rows the node holds but ours never saw a TRANSFER for (known gap) [triage 7d].",
         None, _gap_tag),
] + [Rule("tag:" + t, "KNOWN", "Harness hit already accepted in compare/allowlist.toml (entry %s), with that entry's "
              "accept class %s [triage 7d]." % (t, c), None, _tag(TAG_HIT.get(t, t))) for t, c in TAG_CLASS.items()] + [
    Rule("multi-block-order", "DOCUMENTED",
         "Multi-block newest-first lists (events, transactions, transfers) list a block's rows in node "
         "order, a coinbase after its block's txs; stock orders by tx creationTime. A page then holds other "
         "rows at the same positions. Single-block lists are node order on both sides since FIX-4. "
         "Owner-accepted documented difference (docs/graphql.md); needs the node check to pass [run-2 triage].",
         set(ORDER_EPS), _multi_block_order, node=True),
    Rule("totalcount-stock-orphans", "STOCK-WRONG",
         "Stock's totalCount counts orphan copies (transactionsByPublicKey and the other lists). Explained "
         "only where the node confirms that at least as many of the extra rows are in non-canonical blocks "
         "as the gap; otherwise a diff [H2 item 3].",
         None, _totalcount_stock_orphans, node=True),
    Rule("failed-tx-callstack", "STOCK-WRONG",
         "A failed tx's badResult: stock keeps the Pact error as produced at execution time (a callStack, a "
         "source position in info); the node serves the stored output, which ours answers. Only a callStack "
         "stock has and ours lacks, and an info where ours answers the node's <toplevel>:0, are accepted "
         "[owner ruling 2026-11-19; G4 audit 2.3].",
         None, _failed_tx_callstack),
    Rule("tip-timing", "HARNESS",
         "The node tip moved by 1 to TIP_WINDOW heights between ours' read and stock's read, on a list that "
         "follows the tip; both heights are recorded in the row [H2 item 5].",
         TIP_EPS, _tip_timing),
    Rule("lastBlockHeight-tip-race", "HARNESS",
         "Neither side is pinned for lastBlockHeight; a difference of up to 3 blocks is the tip moving "
         "between the two calls, shown by the node tip recorded at each read [triage 7d, H6 S2].",
         {"gql.lastBlockHeight"}, _tip_race),
]


def table():
    """Every rule as a docs row: id, reason, accept class, ruling."""
    return [{"id": r.id, "reason": r.reason, "accept_class": r.accept, "ruling": r.ruling, "doc": r.doc} for r in RULES]


def classify(ctx):
    """(class, rule ids) for a row with entries left after pair-diff. ctx: ep, cls,
    ours_codes, entries, vars, node, tip."""
    ents = ctx.get("entries") or []
    if not ents or not complete(ctx):
        return "diff", []          # H4 (B3): a row whose differences are not all known is never accepted
    left = set(range(len(ents)))
    used = []
    for r in RULES:
        x = r.explains(ctx) & left
        if x:
            left -= x
            used.append(r.id)
    if left:
        return "diff", []
    # H6 (B1): an our-defect rule never takes part in `allowlisted` or `known-gap`: any use makes
    # the row a diff, as pair-diff classes it (H5 a; its `open` ids name the known gap).
    if any(META[i][0] == "our-defect" for i in used):
        return "diff", []
    if any(META[i][0] == UNAVAILABLE for i in used):
        return UNAVAILABLE, used        # H6 (S2): stock gave nothing to compare; never accepted
    return "allowlisted", used
