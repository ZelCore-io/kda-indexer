"""Suite allowlist: the run-1 triage's DOCUMENTED and STOCK-WRONG classes (stdlib only).

`kda-compare pair-diff` gives the first verdict with the live harness's allowlist. The
suite then looks at the diff entries that are left: a row whose every entry is explained
by a rule below becomes `allowlisted` (or `known-gap` when a known-gap tag explains it),
with the rule ids. A rule never explains an entry on its own say-so where the triage used
the node as referee: those rules need the row's node check to have passed.

Each rule names its verdict and reason (suite-run1-triage.md sections in brackets), and
carries an accept class and a ruling (H2, owner R-P5P9-162): `table()` lists every rule with
id, reason, accept class and ruling, so the docs can say why each difference is accepted.
OURS-BUG classes have no rule here: they stay open diffs until ours is fixed.
"""

import base64
import collections
import re

# Why a rule accepts a difference (the same four classes as `accept_class` in
# compare/allowlist.toml).
ACCEPT_CLASSES = ("stock-wrong-node-evidence", "intended-documented", "unsupported-by-design", "tip-timing")
TIP_WINDOW = 4          # heights the tip may move between ours' read and stock's (about 2 minutes)
TIP_SAMPLE_AGE = 30     # seconds: a tip sample older than this is not a reading at that time


class Rule:
    def __init__(self, id, verdict, reason, eps, fn, node=False, classes=None):
        self.id, self.verdict, self.reason, self.eps, self.fn = id, verdict, reason, eps, fn
        self.node, self.classes = node, classes
        self.accept, self.ruling = META[id]       # a rule without META is refused at import
        assert self.accept in ACCEPT_CLASSES, id

    def explains(self, ctx):
        """Indices of the entries this rule explains (empty: none)."""
        if self.eps and ctx["ep"] not in self.eps:
            return set()
        if self.classes and ctx["cls"] not in self.classes:
            return set()
        if self.node and not (ctx.get("node") or {}).get("ok"):
            return set()
        return self.fn(ctx) or set()


# id -> (accept class, ruling). The ruling names where the difference was accepted.
_T1 = "Suite run-1 triage, section %s; owner R-P5P9-162 (list every difference with why)."
_T2 = "Suite run-2 triage, %s; owner R-P5P9-162 (list every difference with why)."
META = {
    "pactQuery-not-supported": ("unsupported-by-design", _T1 % "1 (docs/graphql.md: NOT_SUPPORTED)"),
    "gasLimitEstimate-not-supported": ("unsupported-by-design", _T1 % "4 (docs/graphql.md: NOT_SUPPORTED)"),
    "networkInfo-totalGasUsedInKda": ("unsupported-by-design", _T1 % "2 (docs table: NOT_AVAILABLE)"),
    "networkInfo-stock-error": ("stock-wrong-node-evidence", _T1 % "2 (stock's own INTERNAL_SERVER_ERROR)"),
    "networkInfo-totalDifficulty": ("stock-wrong-node-evidence", _T1 % "2 (stock answers -1)"),
    "networkInfo-coins-stale": ("stock-wrong-node-evidence", _T1 % "2 (stock's value lags)"),
    "completedBlockHeights-stock-error": ("stock-wrong-node-evidence", _T1 % "3 (node check passes on the row)"),
    "bad-input-vs-stock-error": ("intended-documented", _T1 % "7b (docs Pages)"),
    "ours-refuses-lenient-input": ("intended-documented", _T1 % "7c and 1 (docs: lenient input is refused)"),
    "node-undecodable-id": ("intended-documented", _T1 % "1 (docs Blocks and network)"),
    "guard-keyset-reference": ("intended-documented", _T1 % "5 and 7d (docs Accounts)"),
    "accepted-tags": ("intended-documented", _T1 % "7d (tags already accepted in compare/allowlist.toml)"),
    "known-gap-missing-chain-rows": ("intended-documented", _T1 % "7d (known gap Q, gated by the baseline)"),
    "lastBlockHeight-tip-race": ("tip-timing", _T1 % "7d (up to 3 blocks between the two calls)"),
    "multi-block-order": ("intended-documented", _T2 % "order class (owner-accepted documented difference, docs/graphql.md)"),
    "totalcount-stock-orphans": ("stock-wrong-node-evidence", _T2 % "orphan-count rule (H2 item 3, node confirms the orphans)"),
    "tip-timing": ("tip-timing", _T2 % "tip-staleness timing (H2 item 5, bounded window, both heights recorded)"),
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
    """The diff's number text (`#<digits>e<exp>` or plain) as a float."""
    try:
        return float(str(s).lstrip("#"))
    except ValueError:
        return None


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
    # Stock's value lags (about 3 KDA per 108 s against the ~0.6 KDA/s mining rate):
    # explained only where ours is ahead.
    out = set()
    for i, e in enumerate(ctx["entries"]):
        if e["path"] == "/data/networkInfo/coinsInCirculation" and e["kind"] == "Changed":
            a, b = _num(e.get("a")), _num(e.get("b"))
            if a is not None and b is not None and a > b:
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


TAGS_ALLOWED = ("graphql-r57-float", "graphql-r57-json-object", "graphql-r58-count-lower-bound",
                "graphql-totalcount-cap", "graphql-r57-height-tie-cut", "graphql-r57-height-ties")


def _tags(ctx):
    return {i for i, e in enumerate(ctx["entries"]) if e["path"].lstrip("/~") in TAGS_ALLOWED}


def _gap_tag(ctx):
    return {i for i, e in enumerate(ctx["entries"]) if e["path"] == "/~known-gap-missing-chain-rows"}


def _tip_race(ctx):
    out = set()
    for i, e in enumerate(ctx["entries"]):
        if e["path"] == "/data/lastBlockHeight":
            a, b = _num(e.get("a")), _num(e.get("b"))
            if a is not None and b is not None and abs(a - b) <= 3:
                out.add(i)
    return out


ORDER_EPS = {"gql.events": "events", "gql.transactions": "transactions", "gql.transfers": "transfers"}
ORDER_ID_FIELDS = {"id", "hash", "requestKey", "cursor"}


def _single_block(v):
    """A list bound to one block: FIX-4 lists it in node order, so a diff there stays open."""
    if v.get("blockHash") or v.get("requestKey"):
        return True
    lo, hi = v.get("minHeight"), v.get("maxHeight")
    return bool(v.get("chainId")) and lo is not None and lo == hi


def _multi_block_order(ctx):
    # Multi-block newest-first lists keep node order inside a block (coinbase last); stock orders
    # by tx creationTime. The page then holds different rows at the same positions. Explained
    # only where the node passed ours' rows, the list spans blocks, and at every changed edge
    # the row itself differs (id, hash, request key or cursor), nothing else: a field that
    # differs on the same row, a count, or a page-info change keeps the row open.
    root = ORDER_EPS.get(ctx["ep"])
    if root is None or _single_block(ctx.get("vars") or {}):
        return set()
    pat = re.compile(r"/data/%s/edges/(\d+)/(?:node/(.+)|(cursor))" % root)
    seen = collections.defaultdict(set)
    for e in ctx["entries"]:
        m = pat.fullmatch(e["path"])
        if not m or e["kind"] != "Changed":
            return set()
        seen[m.group(1)].add(m.group(3) or m.group(2))
    if not seen or any(not (ORDER_ID_FIELDS & f) for f in seen.values()):
        return set()
    return set(range(len(ctx["entries"])))


def _totalcount_stock_orphans(ctx):
    # Stock counts orphan copies. Explained only where the node confirmed that many of the
    # extra rows sit in non-canonical blocks (`node.stock_orphans`, nodecheck.orphan_evidence);
    # without that evidence the count stays a diff.
    n = (ctx.get("node") or {}).get("stock_orphans")
    if not isinstance(n, int):
        return set()
    out = set()
    for i, e in enumerate(ctx["entries"]):
        if e["kind"] != "Changed" or not re.fullmatch(r"/data/\w+/totalCount", e["path"]):
            return set()
        a, b = _num(e.get("a")), _num(e.get("b"))
        if a is None or b is None or not 0 < b - a <= n:
            return set()
        out.add(i)
    return out


TIP_EPS = {"gql.blocksFromDepth", "gql.blocksFromHeight", "gql.transfers", "gql.transactions", "gql.events",
           "gql.lastBlockHeight"}
TIP_ANCHORS = ("blockHash", "requestKey", "maxHeight", "endHeight", "after", "before")


def tip_before(log, t):
    """The newest tip sample `(time, height)` at or before `t`, if it is fresh enough."""
    best = None
    for ts, h in log:
        if ts <= t and (best is None or ts >= best[0]):
            best = (ts, h)
    return best[1] if best and t - best[0] <= TIP_SAMPLE_AGE else None


def _tip_timing(ctx):
    # The node's tip moved by 1..TIP_WINDOW heights between ours' read and stock's
    # (ctx["tip"] = {"ours": h, "stock": h}, both recorded in the row), on a list that follows
    # the tip (no height bound, cursor or block pin). Only /data entries; an error or a status
    # difference is not timing.
    t = ctx.get("tip") or {}
    a, b = t.get("ours"), t.get("stock")
    if ctx["ep"] not in TIP_EPS or not isinstance(a, int) or not isinstance(b, int) or not 1 <= b - a <= TIP_WINDOW:
        return set()
    v = ctx.get("vars") or {}
    if ctx["ep"] != "gql.blocksFromDepth" and any(v.get(k) not in (None, "") for k in TIP_ANCHORS):
        return set()
    return _all(ctx, lambda e: e["path"].startswith("/data"))


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
    Rule("accepted-tags", "KNOWN",
         "Already-accepted harness classes: r57 float and JSON-object text, r58 count lower bound, the "
         "totalCount cap and the height-tie cut [triage 7d].", None, _tags),
    Rule("known-gap-missing-chain-rows", "KNOWN",
         "Zero-balance chain rows the node holds but ours never saw a TRANSFER for (known gap) [triage 7d].",
         None, _gap_tag),
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
    Rule("tip-timing", "HARNESS",
         "The node tip moved by 1 to TIP_WINDOW heights between ours' read and stock's read, on a list that "
         "follows the tip; both heights are recorded in the row [H2 item 5].",
         TIP_EPS, _tip_timing),
    Rule("lastBlockHeight-tip-race", "HARNESS",
         "Neither side is pinned for lastBlockHeight; a difference of up to 3 blocks is the tip moving "
         "between the two calls [triage 7d].", {"gql.lastBlockHeight"}, _tip_race),
]


def table():
    """Every rule as a docs row: id, reason, accept class, ruling."""
    return [{"id": r.id, "reason": r.reason, "accept_class": r.accept, "ruling": r.ruling} for r in RULES]


def classify(ctx):
    """(class, rule ids) for a row with entries left after pair-diff. ctx: ep, cls,
    ours_codes, entries, vars, node, tip."""
    ents = ctx.get("entries") or []
    if not ents:
        return "diff", []
    left = set(range(len(ents)))
    used = []
    for r in RULES:
        x = r.explains(ctx) & left
        if x:
            left -= x
            used.append(r.id)
    if left:
        return "diff", []
    return ("known-gap" if used == ["known-gap-missing-chain-rows"] else "allowlisted"), used
