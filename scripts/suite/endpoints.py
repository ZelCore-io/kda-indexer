"""Endpoint inventory and case builders of the endpoint suite (stdlib only).

Every endpoint has a family (`graphql`, `compat`, `native`), a stock side (`oracle`: the stock
GraphQL indexer, `cwd`: chainweb-data, `kdatxs`: the stock `/v1/txs` service, `internal`:
no stock exists, checked for invariants and against ours' other APIs), and weighted case
classes. A class builder gets a seeded `random.Random` and the entity pool and returns the
request: `{"path": ...}` for REST, `{"query", "variables", "op"}` for GraphQL.

Symbolic heights `@tip`, `@tip+N`, `@tip-N` are resolved by the runner when the case is
sent (the tip moves between generation and the run). Nothing here holds real data: real
values come from the pool, which is harvested on the host and never committed.
"""

import base64
import json
import urllib.parse

CHAINS = [str(c) for c in range(20)]
BAD_CHAINS = ["-1", "20", "99", "a", "", " 0", "00", "1e1", "9223372036854775808", "0x1"]
# Chain-graph change (10 -> 20 chains) and candidate fork-activation heights of mainnet01,
# probed at h-1, h, h+1. Exactness does not matter for a comparison: both sides answer the
# same height.
FORKS = [121452, 140808, 328000, 448501, 449940, 452820, 530500, 752214, 771000, 852054,
         1138000, 1213180, 1722500, 2162000, 2334500, 2965885, 3125000, 3250348, 3346400,
         3789100, 4052000, 4294900, 4577530, 4819246, 5060000]
TIP_HEIGHTS = ["@tip", "@tip-1", "@tip-10", "@tip+1", "@tip+2", "@tip+1000"]
SILLY_HEIGHTS = [-1, -2147483648, 2147483647, 2147483648, 9007199254740993, 10**9]
BAD_STRINGS = ["", " ", "ü✓☃", "\u0000", "a" * 257, "a" * 10000, "' OR 1=1 --",
               "%", "_", "\\", "null", "../../x", "%00", "<script>", "퟿" * 3, "k:",
               "w:", "r:", "k:" + "Z" * 64, "k:" + "0" * 63, "k:" + "0" * 65, "k:" + "A" * 64]
BAD_RKS = ["", "x", "A" * 42, "A" * 44, "A" * 43 + "=", "+/" * 21 + "A", "0" * 64, "a" * 10000,
           "ü" * 43, "%2F" * 15]
BAD_CURSORS = ["", "x", "AAAA", "%%%", "☃", "A" * 10000,
               base64.b64encode(b'{"h":-1}').decode(), base64.b64encode(b"\xff" * 40).decode(),
               base64.urlsafe_b64encode(json.dumps({"height": 2**70}).encode()).decode()]
PAGE_SIZES = [0, 1, 2, 10, 20, 50, 99, 100, 101, 1000, -1, 2147483648]


def tipish(r):
    return r.choice(TIP_HEIGHTS)


def height(r, pool, kind=None):
    """A height drawn over the height strata: genesis, forks +-1, tip region, real, silly."""
    k = kind or r.choices(["genesis", "fork", "tip", "real", "silly"], [1, 3, 2, 4, 1])[0]
    if k == "genesis":
        return r.choice([0, 1, 2])
    if k == "fork":
        return r.choice(FORKS) + r.choice([-1, 0, 1])
    if k == "tip":
        return tipish(r)
    if k == "silly":
        return r.choice(SILLY_HEIGHTS)
    hs = pool.get("heights") or [[0, 1000000]]
    return r.choice(hs)[1]


def pick(r, pool, key, fallback):
    xs = pool.get(key) or []
    return r.choice(xs) if xs else fallback(r)


def rand_hex(r, n):
    return "".join(r.choice("0123456789abcdef") for _ in range(n))


def rand_b64u(r, n=43):
    return "".join(r.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
                   for _ in range(n))


# Account strata: the pool holds `accounts_by` = {stratum: [names]} (see gen.harvest).
ACCOUNT_STRATA = ["top100", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "d10",
                  "receive_only", "k", "w", "r", "nonstandard", "never_seen"]


def account(r, pool, stratum=None):
    s = stratum or r.choice(ACCOUNT_STRATA)
    if s == "never_seen":
        return r.choice(["k:" + rand_hex(r, 64), "w:" + rand_b64u(r) + ":keys-all",
                         "suite-never-" + rand_hex(r, 12)])
    xs = (pool.get("accounts_by") or {}).get(s) or []
    if not xs:
        xs = [a for v in (pool.get("accounts_by") or {}).values() for a in v] or ["coin"]
    return r.choice(xs)


def rk(r, pool, tag=None):
    by = pool.get("rks_by") or {}
    xs = by.get(tag) if tag else None
    if not xs:
        xs = [x for v in by.values() for x in v] or [rand_b64u(r)]
    return r.choice(xs)


RK_TAGS = ["any", "failed", "cont", "xchain", "coinbase", "genesis", "legacy", "old", "recent"]


def block_hash(r, pool, tag=None):
    by = pool.get("blocks_by") or {}
    if tag == "unknown":
        return rand_b64u(r)
    xs = by.get(tag) if tag else None
    if not xs:
        xs = [x for v in by.values() for x in v] or [rand_b64u(r)]
    return r.choice(xs)


def pubkey(r, pool):
    if r.random() < 0.1:
        return rand_hex(r, 64)
    return pick(r, pool, "keys", lambda r: rand_hex(r, 64))


def module(r, pool):
    if r.random() < 0.08:
        return r.choice(["coin", "fungible-v2", "free.no-such-module", "COIN", "coin "])
    return pick(r, pool, "modules", lambda r: "coin")


def event_name(r, pool):
    return pick(r, pool, "events", lambda r: "coin.TRANSFER")


def nft_id(r, pool):
    return pick(r, pool, "nft_ids", lambda r: "t:" + rand_b64u(r))


def pact_id(r, pool):
    return pick(r, pool, "pact_ids", lambda r: rand_b64u(r))


def bad(r):
    return r.choice(BAD_STRINGS)


# ---------------------------------------------------------------------------------------
# GraphQL documents: every argument is a variable, so a case is a variable map. `min` and
# `full` selection sets; `full` follows the harness's own operations (compare/graphql).

TX_FULL = ("id hash cmd { nonce networkId meta { chainId creationTime gasLimit gasPrice sender ttl }"
           " signers { id pubkey scheme clist { name args } } } result { ... on TransactionResult {"
           " badResult goodResult gas continuation transactionId height block { id hash height chainId }"
           " events(first: 20) { totalCount edges { node { id name qualifiedName orderIndex parameters } } }"
           " transfers(first: 20) { totalCount edges { node { id amount creationTime orderIndex"
           " senderAccount receiverAccount moduleName chainId blockHash requestKey } } } } }")
BLOCK_FULL = ("id hash height chainId creationTime difficulty weight nonce epoch powHash target"
              " payloadHash parent { hash } transactions(first: 5) { totalCount pageInfo {"
              " hasNextPage hasPreviousPage } edges { cursor node { id hash } } } events(first: 5)"
              " { totalCount edges { node { id name qualifiedName orderIndex } } }")
PAGE = "totalCount pageInfo { hasNextPage hasPreviousPage startCursor endCursor }"
PAGE_NC = "pageInfo { hasNextPage hasPreviousPage startCursor endCursor }"
GUARD = "guard { __typename ... on KeysetGuard { keys predicate } }"
TRANSFER = ("id amount creationTime orderIndex senderAccount receiverAccount moduleName chainId"
            " height blockHash requestKey")

# field -> (variable definitions, argument list, min selection, full selection)
GQL = {
    "block": ("$hash: String!", "hash: $hash", "hash height chainId", BLOCK_FULL),
    "blocksFromDepth": (
        "$minimumDepth: Int!, $chainIds: [String!], $first: Int, $last: Int, $after: String, $before: String",
        "minimumDepth: $minimumDepth, chainIds: $chainIds, first: $first, last: $last, after: $after, before: $before",
        "edges { node { hash } }",
        PAGE_NC + " edges { cursor node { hash height chainId creationTime parent { hash } } }"),
    "blocksFromHeight": (
        "$startHeight: Int!, $endHeight: Int, $chainIds: [String!], $first: Int, $last: Int, $after: String, $before: String",
        "startHeight: $startHeight, endHeight: $endHeight, chainIds: $chainIds, first: $first, last: $last, after: $after, before: $before",
        "edges { node { hash } }",
        PAGE_NC + " edges { cursor node { hash height chainId creationTime weight parent { hash } } }"),
    "completedBlockHeights": (
        "$chainIds: [String!], $heightCount: Int, $completedHeights: Boolean, $first: Int, $last: Int, $after: String, $before: String",
        "chainIds: $chainIds, heightCount: $heightCount, completedHeights: $completedHeights, first: $first, last: $last, after: $after, before: $before",
        "edges { node { height chainId } }",
        PAGE_NC + " edges { cursor node { height chainId hash } }"),
    "events": (
        "$qualifiedEventName: String!, $chainId: String, $minHeight: Int, $maxHeight: Int, $minimumDepth: Int, $blockHash: String, $requestKey: String, $orderIndex: Int, $parametersFilter: String, $first: Int, $last: Int, $after: String, $before: String",
        "qualifiedEventName: $qualifiedEventName, chainId: $chainId, minHeight: $minHeight, maxHeight: $maxHeight, minimumDepth: $minimumDepth, blockHash: $blockHash, requestKey: $requestKey, orderIndex: $orderIndex, parametersFilter: $parametersFilter, first: $first, last: $last, after: $after, before: $before",
        "edges { node { requestKey orderIndex } }",
        PAGE + " edges { cursor node { id name qualifiedName orderIndex parameters block { hash height } requestKey } }"),
    "fungibleAccount": (
        "$accountName: String!, $fungibleName: String", "accountName: $accountName, fungibleName: $fungibleName",
        "accountName totalBalance",
        "id accountName fungibleName totalBalance chainAccounts { id chainId balance " + GUARD + " }"),
    "fungibleAccountsByPublicKey": (
        "$publicKey: String!, $fungibleName: String", "publicKey: $publicKey, fungibleName: $fungibleName",
        "accountName", "id accountName fungibleName totalBalance"),
    "fungibleChainAccount": (
        "$accountName: String!, $chainId: String!, $fungibleName: String",
        "accountName: $accountName, chainId: $chainId, fungibleName: $fungibleName",
        "balance", "id chainId accountName balance " + GUARD),
    "fungibleChainAccounts": (
        "$accountName: String!, $chainIds: [String!], $fungibleName: String",
        "accountName: $accountName, chainIds: $chainIds, fungibleName: $fungibleName",
        "chainId balance", "id chainId accountName fungibleName balance " + GUARD),
    "fungibleChainAccountsByPublicKey": (
        "$publicKey: String!, $chainId: String!, $fungibleName: String",
        "publicKey: $publicKey, chainId: $chainId, fungibleName: $fungibleName",
        "accountName", "id chainId accountName fungibleName balance " + GUARD),
    "gasLimitEstimate": ("$input: [String!]!", "input: $input", "amount",
                         "amount inputType transaction usedPreflight usedSignatureVerification"),
    "graphConfiguration": ("", "", "minimumBlockHeight", "minimumBlockHeight version"),
    "lastBlockHeight": ("", "", "", ""),
    "networkInfo": ("", "", "networkId",
                    "apiVersion coinsInCirculation networkHashRate networkHost networkId totalDifficulty transactionCount"),
    "node": ("$id: ID!", "id: $id", "__typename id",
             "__typename id ... on Block { hash height chainId } ... on Transaction { hash }"
             " ... on FungibleAccount { accountName totalBalance } ... on Event { qualifiedName }"),
    "nodes": ("$ids: [ID!]!", "ids: $ids", "__typename id",
              "__typename id ... on Block { hash height chainId } ... on Transaction { hash }"),
    "nonFungibleAccount": ("$accountName: String!", "accountName: $accountName", "accountName",
                           "id accountName chainAccounts { id chainId accountName }"
                           " nonFungibleTokenBalances { tokenId chainId balance version }"),
    "nonFungibleChainAccount": (
        "$accountName: String!, $chainId: String!", "accountName: $accountName, chainId: $chainId",
        "accountName", "id accountName chainId nonFungibleTokenBalances { id tokenId chainId balance version }"),
    "pactQuery": ("$pactQuery: [PactQuery!]!", "pactQuery: $pactQuery", "status",
                  "chainId code error result status"),
    "transaction": ("$requestKey: String!, $blockHash: String, $minimumDepth: Int",
                    "requestKey: $requestKey, blockHash: $blockHash, minimumDepth: $minimumDepth",
                    "hash", TX_FULL),
    "transactions": (
        "$accountName: String, $blockHash: String, $chainId: String, $fungibleName: String, $requestKey: String, $minHeight: Int, $maxHeight: Int, $minimumDepth: Int, $first: Int, $last: Int, $after: String, $before: String",
        "accountName: $accountName, blockHash: $blockHash, chainId: $chainId, fungibleName: $fungibleName, requestKey: $requestKey, minHeight: $minHeight, maxHeight: $maxHeight, minimumDepth: $minimumDepth, first: $first, last: $last, after: $after, before: $before",
        "edges { node { hash } }",
        PAGE + " edges { cursor node { id hash cmd { meta { chainId sender creationTime } } result { ... on TransactionResult { height badResult block { hash } } } } }"),
    "transactionsByPublicKey": (
        "$publicKey: String!, $first: Int, $last: Int, $after: String, $before: String",
        "publicKey: $publicKey, first: $first, last: $last, after: $after, before: $before",
        "edges { node { hash } }",
        PAGE + " edges { cursor node { id hash cmd { meta { chainId sender } } } }"),
    "transfers": (
        "$accountName: String, $blockHash: String, $chainId: String, $fungibleName: String, $requestKey: String, $first: Int, $last: Int, $after: String, $before: String",
        "accountName: $accountName, blockHash: $blockHash, chainId: $chainId, fungibleName: $fungibleName, requestKey: $requestKey, first: $first, last: $last, after: $after, before: $before",
        "edges { node { requestKey amount } }",
        PAGE + " edges { cursor node { " + TRANSFER + " } }"),
}


def gql_doc(field, full):
    vdefs, args, mn, fl = GQL[field]
    sel = fl if full else mn
    op = "S_" + field
    head = "query %s%s" % (op, "(%s)" % vdefs if vdefs else "")
    call = field + ("(%s)" % args if args else "")
    return op, "%s { %s%s }" % (head, call, " { %s }" % sel if sel else "")


def paging(r, v, conn=True):
    """Paging edges: first/last/before/after including garbage and both directions."""
    k = r.choice(["first", "last", "first_after", "last_before", "none", "both", "garbage"])
    if k in ("first", "first_after", "both"):
        v["first"] = r.choice(PAGE_SIZES)
    if k in ("last", "last_before", "both"):
        v["last"] = r.choice(PAGE_SIZES)
    if k == "first_after":
        v["after"] = r.choice(BAD_CURSORS)
    if k == "last_before":
        v["before"] = r.choice(BAD_CURSORS)
    if k == "garbage":
        v[r.choice(["after", "before"])] = r.choice(BAD_CURSORS)
        v["first"] = r.choice([1, 10, 100])
    return v


def chains_list(r):
    k = r.random()
    if k < 0.4:
        return [r.choice(CHAINS)]
    if k < 0.6:
        return r.sample(CHAINS, r.randint(2, 20))
    if k < 0.75:
        return None
    if k < 0.85:
        return []
    return [r.choice(BAD_CHAINS)]


def g_entity(field, r, pool):
    """Valid-looking variables for `field` over the entity strata."""
    v = {}
    if field == "block":
        v["hash"] = block_hash(r, pool, r.choice(["any", "genesis", "fork", "orphan", "recent", "unknown"]))
    elif field == "blocksFromDepth":
        v["minimumDepth"] = r.choice([0, 1, 2, 6, 10, 100, 1000, 20000, 10**6])
        v["chainIds"] = chains_list(r)
        v["first"] = r.choice([1, 5, 20, 50, 100])
    elif field == "blocksFromHeight":
        h = height(r, pool)
        v["startHeight"] = h
        if r.random() < 0.6 and isinstance(h, int):
            v["endHeight"] = h + r.choice([0, 1, 5, 100, 20000, -1])
        v["chainIds"] = chains_list(r)
        v["first"] = r.choice([1, 5, 20, 50, 100])
    elif field == "completedBlockHeights":
        v["chainIds"] = chains_list(r)
        v["heightCount"] = r.choice([1, 2, 3, 10, 50, 100])
        v["completedHeights"] = r.choice([True, False, None])
        v["first"] = r.choice([1, 5, 20])
    elif field == "events":
        v["qualifiedEventName"] = event_name(r, pool)
        if r.random() < 0.5:
            v["chainId"] = r.choice(CHAINS)
        if r.random() < 0.4:
            lo = height(r, pool, "real")
            v["minHeight"] = lo
            v["maxHeight"] = lo + r.choice([0, 1, 100, 10000])
        if r.random() < 0.15:
            v["requestKey"] = rk(r, pool)
        if r.random() < 0.1:
            v["blockHash"] = block_hash(r, pool)
        if r.random() < 0.1:
            v["parametersFilter"] = json.dumps({"array_starts_with": account(r, pool)})
        v["first"] = r.choice([1, 10, 20, 50, 100])
    elif field in ("fungibleAccount", "nonFungibleAccount"):
        v["accountName"] = account(r, pool)
        if field == "fungibleAccount" and r.random() < 0.3:
            v["fungibleName"] = module(r, pool)
    elif field == "fungibleAccountsByPublicKey":
        v["publicKey"] = pubkey(r, pool)
        if r.random() < 0.3:
            v["fungibleName"] = module(r, pool)
    elif field in ("fungibleChainAccount", "nonFungibleChainAccount"):
        v["accountName"] = account(r, pool)
        v["chainId"] = r.choice(CHAINS)
        if field == "fungibleChainAccount" and r.random() < 0.3:
            v["fungibleName"] = module(r, pool)
    elif field == "fungibleChainAccounts":
        v["accountName"] = account(r, pool)
        c = chains_list(r)
        if c is not None:
            v["chainIds"] = c
        if r.random() < 0.3:
            v["fungibleName"] = module(r, pool)
    elif field == "fungibleChainAccountsByPublicKey":
        v["publicKey"] = pubkey(r, pool)
        v["chainId"] = r.choice(CHAINS)
    elif field == "gasLimitEstimate":
        code = r.choice(["(+ 1 2)", "(coin.details \"%s\")" % account(r, pool), "(free.x)", "1"])
        cmd = {"code": code, "meta": {"chainId": r.choice(CHAINS)}}
        v["input"] = [json.dumps(cmd)]
    elif field in ("graphConfiguration", "lastBlockHeight", "networkInfo"):
        pass
    elif field in ("node", "nodes"):
        ids = [node_id(r, pool) for _ in range(r.choice([1, 1, 2, 5, 20]))]
        if field == "node":
            v["id"] = ids[0]
        else:
            v["ids"] = ids
    elif field == "pactQuery":
        a = account(r, pool)
        code = r.choice(['(coin.get-balance "%s")' % a, '(coin.details "%s")' % a, "(+ 1 2)",
                         "(describe-module \"coin\")", '(coin.get-balance "suite-never")'])
        v["pactQuery"] = [{"chainId": r.choice(CHAINS), "code": code}]
    elif field == "transaction":
        v["requestKey"] = rk(r, pool, r.choice(RK_TAGS))
        if r.random() < 0.2:
            v["blockHash"] = block_hash(r, pool)
        if r.random() < 0.1:
            v["minimumDepth"] = r.choice([0, 1, 6, 10**6])
    elif field in ("transactions", "transfers"):
        k = r.choice(["account", "account_chain", "block", "rk", "account_module", "heights"])
        if k.startswith("account"):
            v["accountName"] = account(r, pool)
        if k == "account_chain":
            v["chainId"] = r.choice(CHAINS)
        if k == "account_module":
            v["fungibleName"] = module(r, pool)
        if k == "block":
            v["blockHash"] = block_hash(r, pool)
        if k == "rk":
            v["requestKey"] = rk(r, pool, r.choice(RK_TAGS))
        if k == "heights" and field == "transactions":
            v["accountName"] = account(r, pool)
            lo = height(r, pool, "real")
            v["minHeight"], v["maxHeight"] = lo, lo + r.choice([0, 10, 1000])
        v["first"] = r.choice([1, 10, 20, 50, 100])
    elif field == "transactionsByPublicKey":
        v["publicKey"] = pubkey(r, pool)
        v["first"] = r.choice([1, 10, 20, 50])
    return v


def node_id(r, pool):
    k = r.choice(["block", "tx", "account", "bad"])
    if k == "block":
        raw = '["Block","%s"]' % block_hash(r, pool)
    elif k == "tx":
        raw = '["Transaction","%s","%s"]' % (block_hash(r, pool), rk(r, pool))
    elif k == "account":
        raw = '["FungibleAccount","coin","%s"]' % account(r, pool)
    else:
        return r.choice(BAD_CURSORS)
    return base64.b64encode(raw.encode()).decode()


PAGED_GQL = {"blocksFromDepth", "blocksFromHeight", "completedBlockHeights", "events",
             "transactions", "transactionsByPublicKey", "transfers"}
REQUIRED_STR = {"block": ["hash"], "events": ["qualifiedEventName"],
                "fungibleAccount": ["accountName"], "fungibleAccountsByPublicKey": ["publicKey"],
                "fungibleChainAccount": ["accountName", "chainId"],
                "fungibleChainAccounts": ["accountName"],
                "fungibleChainAccountsByPublicKey": ["publicKey", "chainId"],
                "node": ["id"], "nonFungibleAccount": ["accountName"],
                "nonFungibleChainAccount": ["accountName", "chainId"],
                "transaction": ["requestKey"], "transactions": ["accountName", "requestKey"],
                "transactionsByPublicKey": ["publicKey"], "transfers": ["accountName", "requestKey"]}


def g_invalid(field, r, pool):
    v = g_entity(field, r, pool)
    keys = REQUIRED_STR.get(field)
    k = r.random()
    if keys and k < 0.6:
        key = r.choice(keys)
        v[key] = r.choice(BAD_RKS if key == "requestKey" else BAD_STRINGS)
    elif k < 0.75 and v:
        v[r.choice(sorted(v))] = r.choice([None, -1, 2**63, "x", [], {}, True])  # wrong type
    elif k < 0.85:
        v = {}  # required variables missing
    else:
        for key in ("minHeight", "maxHeight", "startHeight", "endHeight", "minimumDepth"):
            if key in GQL[field][0]:
                v[key] = r.choice(SILLY_HEIGHTS)
    return v


def g_edge(field, r, pool):
    v = g_entity(field, r, pool)
    if field in PAGED_GQL:
        for k in ("first", "last", "after", "before"):
            v.pop(k, None)
        paging(r, v)
    if field in ("blocksFromHeight",):
        v["startHeight"] = height(r, pool, r.choice(["genesis", "fork", "tip", "silly"]))
    if field in ("events", "transactions") and r.random() < 0.5:
        v["minHeight"] = height(r, pool, r.choice(["genesis", "fork", "tip"]))
    if "chainId" in v or field in ("fungibleChainAccount", "nonFungibleChainAccount",
                                   "fungibleChainAccountsByPublicKey"):
        v["chainId"] = r.choice(CHAINS + BAD_CHAINS[:3])
    if field == "transaction":
        v["requestKey"] = rk(r, pool, r.choice(["genesis", "coinbase", "cont", "failed", "xchain", "legacy"]))
    if field == "block":
        v["hash"] = block_hash(r, pool, r.choice(["genesis", "orphan", "fork", "unknown"]))
    return v


def g_real(field, r, pool):
    caps = (pool.get("gql_real") or {}).get(field) or []
    if not caps:
        return None
    c = r.choice(caps)
    return {"query": c["query"], "variables": c.get("variables") or {}, "op": c.get("op") or "real"}


def gql_builder(field):
    def build(cls, r, pool):
        if cls == "real":
            got = g_real(field, r, pool)
            if got:
                return got
            cls = "entity"
        fn = {"entity": g_entity, "edge": g_edge, "invalid": g_invalid, "shape": g_entity}[cls]
        v = fn(field, r, pool)
        full = cls != "shape" and r.random() < 0.7
        op, q = gql_doc(field, full)
        if cls == "shape" and GQL[field][2]:
            # Root alias plus __typename: the same data under another response shape.
            q = q.replace("{ %s" % field, "{ a1: %s" % field, 1).replace(
                "{ " + GQL[field][2], "{ __typename " + GQL[field][2], 1)
        return {"query": q, "variables": {k: x for k, x in v.items() if x is not None}, "op": op}
    return build


GQL_CLASSES = [("real", 0.15), ("entity", 0.45), ("edge", 0.2), ("invalid", 0.15), ("shape", 0.05)]
# Classes the stock indexer runs one at a time (known heavy).
HEAVY = {"transactions", "transfers", "events", "transactionsByPublicKey", "fungibleAccount",
         "fungibleChainAccounts", "completedBlockHeights", "blocksFromHeight"}


# ---------------------------------------------------------------------------------------
# REST helpers

def qs(**kw):
    items = [(k.rstrip("_"), v) for k, v in kw.items() if v is not None]
    return ("?" + urllib.parse.urlencode(items, quote_via=urllib.parse.quote)) if items else ""


def seg(s):
    return urllib.parse.quote(str(s), safe="")


def win(r, pool):
    if r.random() < 0.5:
        return None, None
    lo = height(r, pool, r.choice(["genesis", "fork", "real", "tip"]))
    if not isinstance(lo, int):
        return lo, None
    return lo, lo + r.choice([0, 1, 100, 10000, -5])


def compat_builder(route):
    def build(cls, r, pool):
        if cls == "real":
            xs = (pool.get("rest_real") or {}).get(route) or []
            if xs:
                return {"path": r.choice(xs)}
            cls = "entity"
        inv = cls == "invalid"
        edge = cls == "edge"
        lim = r.choice(PAGE_SIZES + [250, 251]) if edge else r.choice([None, 10, 20, 50, 100])
        off = r.choice([None, 0, 1, 100, 9999, 10000, 10001, -1]) if edge else None
        nxt = r.choice(BAD_CURSORS) if edge and r.random() < 0.3 else None
        if route in ("tx", "txs"):
            key = r.choice(BAD_RKS) if inv else rk(r, pool, r.choice(RK_TAGS))
            return {"path": "/txs/%s%s" % (route, qs(requestkey=key))}
        if route == "account":
            a = bad(r) if inv else account(r, pool)
            lo, hi = win(r, pool)
            tok = module(r, pool) if r.random() < 0.3 else None
            ch = r.choice(CHAINS + (BAD_CHAINS if inv or edge else [])) if r.random() < 0.4 else None
            return {"path": "/txs/account/%s%s" % (seg(a), qs(token=tok, chain=ch, minheight=lo,
                                                                maxheight=hi, limit=lim,
                                                                offset=off, next=nxt))}
        if route == "events":
            k = r.choice(["name", "module", "search", "param", "rk"])
            p = {}
            if k == "name":
                p["name"] = event_name(r, pool).split(".")[-1]
                p["modulename"] = ".".join(event_name(r, pool).split(".")[:-1]) or None
            elif k == "module":
                p["modulename"] = module(r, pool)
            elif k == "search":
                p["search"] = bad(r) if inv else event_name(r, pool)
            elif k == "param":
                p["param"] = account(r, pool)
                p["name"] = "TRANSFER"
            else:
                p["requestkey"] = r.choice(BAD_RKS) if inv else rk(r, pool)
            lo, hi = win(r, pool)
            return {"path": "/txs/events" + qs(minheight=lo, maxheight=hi, limit=lim, offset=off,
                                               next=nxt, **p)}
        if route == "recent":
            junk = r.choice([None, "x", "1"]) if edge or inv else None
            return {"path": "/txs/recent" + qs(junk=junk)}
        if route == "search":
            terms = pool.get("search_terms") or ["coin.transfer"]
            t = bad(r) if inv else r.choice(terms + [module(r, pool), account(r, pool),
                                                    "%" + module(r, pool) + "%", "a_b", "ab", "%%"])
            return {"path": "/txs/search" + qs(search=t, limit=lim, offset=off, next=nxt)}
        if route == "pactid":
            p = r.choice(BAD_RKS) if inv else pact_id(r, pool)
            both = r.random() < 0.05
            return {"path": "/txs/search" + qs(pactid=p, search="coin" if both else None)}
        if route in ("coins", "stats"):
            junk = r.choice([None, "x"]) if edge or inv else None
            return {"path": "/" + route + qs(junk=junk)}
        if route == "v1txs":
            a = bad(r) if inv else account(r, pool)
            ns = module(r, pool) if r.random() < 0.4 else "coin"
            n = r.choice([0, 1, 25, 50, 2050, 10000, 10001, -1, "1.5", "1e3", "NaN", "x"]) if edge or inv \
                else r.choice([1, 10, 25, 50, 100])
            form = r.choice(["path3", "path2", "path1", "query"])
            if form == "path3":
                return {"path": "/v1/txs/%s/%s/%s" % (seg(a), seg(ns), seg(n))}
            if form == "path2":
                return {"path": "/v1/txs/%s/%s%s" % (seg(a), seg(ns), qs(limit=n))}
            if form == "path1":
                return {"path": "/v1/txs/%s%s" % (seg(a), qs(namespace=ns, limit=n))}
            return {"path": "/v1/txs" + qs(account=a, namespace=ns, limit=n)}
        raise KeyError(route)
    return build


def native_builder(route):
    def build(cls, r, pool):
        inv = cls == "invalid"
        edge = cls == "edge"
        p = {}
        if edge or r.random() < 0.5:
            p["limit"] = r.choice(PAGE_SIZES) if edge else r.choice([1, 5, 20, 100])
        if edge and r.random() < 0.4:
            p["cursor"] = r.choice(BAD_CURSORS)
        if r.random() < 0.3:
            p["order"] = r.choice(["asc", "desc"] + (["up", ""] if edge or inv else []))
        if r.random() < 0.3:
            lo, hi = win(r, pool)
            p["minheight"], p["maxheight"] = lo, hi
        if r.random() < 0.3:
            p["chain"] = r.choice(CHAINS + (BAD_CHAINS if edge or inv else []))
        a = bad(r) if inv else account(r, pool)
        key = (r.choice(BAD_STRINGS + ["0" * 63, "G" * 64]) if inv else pubkey(r, pool))
        rkey = r.choice(BAD_RKS) if inv else rk(r, pool, r.choice(RK_TAGS))
        bh = r.choice(BAD_RKS) if inv else block_hash(r, pool, r.choice(["any", "genesis", "orphan", "fork", "recent", "unknown"]))
        if route == "status" or route == "stats":
            return {"path": "/api/v1/" + route + (qs(junk="x") if inv else "")}
        if route == "blocks":
            q = {k: p.get(k) for k in ("limit", "cursor", "order")}
            lo = height(r, pool)
            q["minheight"] = lo
            q["maxheight"] = (lo + r.choice([0, 1, 10, 20000, 20001, -1])) if isinstance(lo, int) else None
            if r.random() < 0.5:
                q["chains"] = ",".join(r.sample(CHAINS, r.randint(1, 4))) if not inv else r.choice(BAD_CHAINS)
            return {"path": "/api/v1/blocks" + qs(**q)}
        if route == "block":
            return {"path": "/api/v1/blocks/" + seg(bh)}
        if route == "block_txs":
            return {"path": "/api/v1/blocks/%s/txs%s" % (seg(bh), qs(limit=p.get("limit"), order=p.get("order"), cursor=p.get("cursor")))}
        if route == "block_by_height":
            ch = r.choice(BAD_CHAINS) if inv else r.choice(CHAINS)
            return {"path": "/api/v1/chains/%s/blocks/%s" % (seg(ch), seg(height(r, pool)))}
        if route == "tx":
            return {"path": "/api/v1/txs/" + seg(rkey) + ("?include=proof" if r.random() < 0.2 else "")}
        if route == "defpact":
            return {"path": "/api/v1/txs/%s/defpact" % seg(rkey if inv else rk(r, pool, r.choice(["cont", "xchain", "any"])))}
        if route in ("acct_transfers", "acct_txs", "acct_balances"):
            sub = {"acct_transfers": "transfers", "acct_txs": "txs", "acct_balances": "balances"}[route]
            if route == "acct_txs" and r.random() < 0.4:
                p["role"] = r.choice(["any", "sender"] + (["x"] if inv else []))
            if route != "acct_txs" and r.random() < 0.3:
                p["token"] = module(r, pool)
            if route == "acct_balances":
                p = {k: p.get(k) for k in ("limit", "cursor", "chain", "token")}
            return {"path": "/api/v1/accounts/%s/%s%s" % (seg(a), sub, qs(**p))}
        if route in ("key_txs", "key_accounts"):
            sub = "txs" if route == "key_txs" else "accounts"
            if route == "key_accounts":
                p = {k: p.get(k) for k in ("limit", "cursor")}
                if r.random() < 0.3:
                    p["token"] = module(r, pool)
            return {"path": "/api/v1/keys/%s/%s%s" % (seg(key), sub, qs(**p))}
        if route == "events":
            ev = event_name(r, pool)
            mod, _, name = ev.rpartition(".")
            k = r.choice(["mod_name", "qualified", "param", "module", "none"] if inv or edge
                         else ["mod_name", "qualified", "param", "module"])
            if k == "mod_name":
                p["module"], p["name"] = mod, name
            elif k == "qualified":
                p["name"] = ev
            elif k == "param":
                p["module"], p["name"] = mod, name
                p["param%d" % r.choice([0, 0, 1, 2, 9, 10])] = account(r, pool)
            elif k == "module":
                p["module"] = mod
            return {"path": "/api/v1/events" + qs(**p)}
        if route == "tokens":
            return {"path": "/api/v1/tokens" + qs(limit=p.get("limit"), cursor=p.get("cursor"))}
        if route == "token":
            m = bad(r) if inv else module(r, pool)
            return {"path": "/api/v1/tokens/" + seg(m)}
        if route == "nft":
            return {"path": "/api/v1/nft/accounts/%s%s" % (seg(a), qs(chain=p.get("chain"), limit=p.get("limit"), cursor=p.get("cursor")))}
        if route == "search_code":
            terms = pool.get("search_terms") or ["coin.transfer"]
            t = bad(r) if inv else r.choice(terms + [module(r, pool), "%%%", "ab", "a" * 300])
            return {"path": "/api/v1/search/code" + qs(q=t, limit=p.get("limit"), cursor=p.get("cursor"))}
        if route == "search_events":
            t = bad(r) if inv else r.choice([event_name(r, pool), account(r, pool), module(r, pool), "TRANSFER"])
            return {"path": "/api/v1/search/events" + qs(q=t, limit=p.get("limit"), cursor=p.get("cursor"))}
        raise KeyError(route)
    return build


REST_CLASSES = [("real", 0.15), ("entity", 0.55), ("edge", 0.15), ("invalid", 0.15)]
NATIVE_CLASSES = [("entity", 0.65), ("edge", 0.2), ("invalid", 0.15)]

# name, family, stock, builder, classes, route template (docs)
ENDPOINTS = []
for _f in GQL:
    ENDPOINTS.append(("gql." + _f, "graphql", "oracle", gql_builder(_f), GQL_CLASSES, _f))
for _r, _tpl, _stock in [("tx", "/txs/tx", "cwd"), ("txs", "/txs/txs", "cwd"),
                         ("account", "/txs/account/{account}", "cwd"),
                         ("events", "/txs/events", "cwd"), ("recent", "/txs/recent", "cwd"),
                         ("search", "/txs/search?search=", "cwd"),
                         ("pactid", "/txs/search?pactid=", "cwd"), ("coins", "/coins", "cwd"),
                         ("stats", "/stats", "cwd"),
                         ("v1txs", "/v1/txs/{account}/{namespace}/{limit}", "kdatxs")]:
    ENDPOINTS.append(("compat." + _r, "compat", _stock, compat_builder(_r), REST_CLASSES, _tpl))
for _r, _tpl in [("status", "/status"), ("stats", "/stats"), ("blocks", "/blocks"),
                 ("block", "/blocks/{hash}"), ("block_txs", "/blocks/{hash}/txs"),
                 ("block_by_height", "/chains/{chain}/blocks/{height}"),
                 ("tx", "/txs/{request_key}"), ("defpact", "/txs/{request_key}/defpact"),
                 ("acct_transfers", "/accounts/{account}/transfers"),
                 ("acct_txs", "/accounts/{account}/txs"),
                 ("acct_balances", "/accounts/{account}/balances"),
                 ("key_txs", "/keys/{pubkey}/txs"), ("key_accounts", "/keys/{pubkey}/accounts"),
                 ("events", "/events"), ("tokens", "/tokens"), ("token", "/tokens/{module}"),
                 ("nft", "/nft/accounts/{account}"), ("search_code", "/search/code"),
                 ("search_events", "/search/events")]:
    ENDPOINTS.append(("native." + _r, "native", "internal", native_builder(_r), NATIVE_CLASSES,
                      "/api/v1" + _tpl))

# Stock-side exceptions: never sent to stock, recorded as "not run".
STOCK_SKIP = {"gql.nonFungibleAccount": "crashes stock (known crasher QNonFungibleAccount)"}
# Native routes with a cross-check against ours' other APIs (one extra request to ours).
NATIVE_CROSS = {"native.tx": "compat /txs/tx", "native.block_by_height": "native /blocks/{hash}"}
