"""Endpoint suite tools (scripts/suite): inventory, deterministic stratified cases, invariants.

Synthetic pool only: the real pool is harvested on the host and never committed.
"""

import gzip
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "suite"))
import endpoints as E  # noqa: E402
import gen  # noqa: E402
import run  # noqa: E402
import allow  # noqa: E402
import nodecheck as N  # noqa: E402
import select3  # noqa: E402
import base64  # noqa: E402
import random  # noqa: E402

POOL = {
    "tip": 1000,
    "accounts_by": {"top100": ["k:" + "a" * 64], "d1": ["alice"], "k": ["k:" + "a" * 64],
                    "w": ["w:abc:keys-all"], "r": ["r:ns.ks"], "nonstandard": ["alice"],
                    "receive_only": ["bob"]},
    "keys": ["a" * 64], "modules": ["coin", "free.tok"], "events": ["coin.TRANSFER"],
    "rks_by": {"any": ["R" * 43], "failed": ["F" * 43]},
    "blocks_by": {"any": ["B" * 43], "genesis": ["G" * 43]},
    "heights": [[0, 10], [3, 500]], "pact_ids": ["P" * 43], "nft_ids": ["t:" + "N" * 43],
    "rest_real": {"tx": ["/txs/tx?requestkey=" + "R" * 43]},
    "gql_real": {"lastBlockHeight": [{"query": "query Q { lastBlockHeight }", "variables": {}}]},
    "search_terms": ["coin.transfer"],
}


class Inventory(unittest.TestCase):
    def test_counts(self):
        fam = [e[1] for e in E.ENDPOINTS]
        self.assertEqual((fam.count("graphql"), fam.count("compat"), fam.count("native")), (23, 10, 19))
        self.assertEqual(len({e[0] for e in E.ENDPOINTS}), len(E.ENDPOINTS))

    def test_openapi_covered(self):
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
        with open(os.path.join(root, "docs", "openapi.json")) as f:
            paths = json.load(f)["paths"]
        routes = {e[5] for e in E.ENDPOINTS if e[1] == "native"}
        for p in paths:
            self.assertIn(p.replace("{request_key}", "{request_key}"), routes, p)

    def test_documents_are_well_formed(self):
        for f in E.GQL:
            for full in (False, True):
                _, q = E.gql_doc(f, full)
                self.assertEqual(q.count("{"), q.count("}"), q)


class Cases(unittest.TestCase):
    def gen(self, d, seed):
        return gen.generate(POOL, d, 400, seed)

    def test_deterministic_and_stratified(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            m1, m2 = self.gen(a, "1"), self.gen(b, "1")
            self.assertEqual(m1["endpoints"], m2["endpoints"])
            for ep in m1["endpoints"]:
                x = gzip.open(os.path.join(a, ep + ".jsonl.gz")).read()
                self.assertEqual(x, gzip.open(os.path.join(b, ep + ".jsonl.gz")).read(), ep)
            for ep, m in m1["endpoints"].items():
                self.assertEqual(sum(m["classes"].values()), 400)
                self.assertGreaterEqual(len(m["classes"]), 3, ep)
            self.assertIn("real", m1["endpoints"]["compat.tx"]["classes"])
            self.assertIn("real>entity", m1["endpoints"]["compat.account"]["classes"])
            self.assertEqual(m1["endpoints"]["gql.nonFungibleAccount"]["stock_skip"] is not None, True)

    def test_other_seed_differs(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            gen.generate(POOL, a, 50, "1", ["compat.account"])
            gen.generate(POOL, b, 50, "2", ["compat.account"])
            fa = gzip.open(os.path.join(a, "compat.account.jsonl.gz")).read()
            self.assertNotEqual(fa, gzip.open(os.path.join(b, "compat.account.jsonl.gz")).read())


class Runner(unittest.TestCase):
    def test_tip_resolution(self):
        self.assertEqual(run.resolve({"a": "@tip+1", "b": ["@tip-2"], "p": "/x?h=@tip"}, 100),
                         {"a": 101, "b": [98], "p": "/x?h=100"})

    def test_native_invariants(self):
        ok = {"items": [{"height": 5, "chain": 1}, {"height": 4, "chain": 1}], "next_cursor": None,
              "as_of": {}}
        self.assertEqual(run.native_check("native.acct_txs", "entity", "/a?limit=2&chain=1", 200, ok), [])
        bad = {"items": [{"height": 4, "chain": 2}, {"height": 5, "chain": 1}], "next_cursor": None,
               "as_of": {}}
        p = run.native_check("native.acct_txs", "entity", "/a?limit=1&chain=1", 200, bad)
        self.assertIn("order", p)
        self.assertIn("chain-filter", p)
        self.assertTrue(any(x.startswith("over-limit") for x in p))
        self.assertEqual(run.native_check("native.tx", "entity", "/t", 500, "x"), ["5xx:500"])
        self.assertEqual(run.native_check("native.tx", "invalid", "/t", 400, "x"), [])


class NodeIds(unittest.TestCase):
    def test_kadena_layout(self):
        seen = set()
        r = random.Random(7)
        for _ in range(400):
            i = E.node_id(r, POOL)
            try:
                raw = base64.b64decode(i, validate=True).decode()
            except Exception:
                continue
            t, _, key = raw.partition(":")
            seen.add(t)
            if t == "Block":
                self.assertEqual(key, "B" * 43 if key.startswith("B") else key)
                self.assertFalse(key.startswith("["))
            elif t in ("Transaction", "FungibleAccount"):
                self.assertEqual(len(json.loads(key)), 2)
                self.assertNotIn(" ", key)
        self.assertTrue({"Block", "Transaction", "FungibleAccount"} <= seen)
        self.assertEqual(base64.b64decode(E.encode_id("Block", "abc")), b"Block:abc")
        self.assertEqual(base64.b64decode(E.encode_id("Transaction", ["h", "r"])), b'Transaction:["h","r"]')


class Throttle(unittest.TestCase):
    def host(self):
        return run.StockHost("s", "gql", {"url": "http://x", "rps": 35, "heavy_concurrency": 4}, None,
                             open(os.devnull, "w"), start=False)

    def test_heavy_timeouts_do_not_touch_light(self):
        h = self.host()
        now = 1000.0
        h.changed = h.heavy_changed = now - 500
        for i in range(50):
            h.record("heavy", now - 1, 0, 30000)
            h.record("light", now - 1, 200, 10)
        h.judge_once(now)
        self.assertEqual(h.rate, 35)

    def test_light_p99_uses_light_only(self):
        h = self.host()
        now = 1000.0
        h.changed = now - 500
        for i in range(100):
            h.record("light", now - 1, 200, 9000 if i < 5 else 10)
        h.judge_once(now)
        self.assertLess(h.rate, 35)

    def test_fast_500_is_not_load(self):
        self.assertFalse(run.load_error(500))
        self.assertTrue(run.load_error(504))
        self.assertTrue(run.load_error(0))

    def test_heavy_crash_lowers_heavy_only(self):
        h = self.host()
        now = 1000.0
        h.changed = h.heavy_changed = now - 500
        for i in range(20):
            h.record("heavy", now - 1, 502, 5)
        h.judge_once(now)
        self.assertEqual(h.rate, 35)
        self.assertLess(h.heavy_limit, 4)


class OursSources(unittest.TestCase):
    def test_enough_sources(self):
        srcs = run.ours_sources({"rps": 130, "per_ip_limit": 20, "source_base": "127.0.0.", "source_start": 60}, 130)
        self.assertGreaterEqual(len(srcs), 13)
        self.assertEqual(len(set(srcs)), len(srcs))
        self.assertLessEqual(130 / len(srcs), 10)

    def test_listed_sources_too_few(self):
        with self.assertRaises(SystemExit):
            run.ours_sources({"sources": ["127.0.0.200"], "per_ip_limit": 20}, 130)


class Messages(unittest.TestCase):
    def test_gql_messages_truncated(self):
        b = {"errors": [{"message": "x" * 500, "extensions": {"code": "BAD_USER_INPUT"}}, {"message": "m"}]}
        codes, msgs = run.gql_errors(b)
        self.assertEqual(codes, ["BAD_USER_INPUT", "ERROR"])
        self.assertEqual(len(msgs[0]), run.MSG_LEN)
        self.assertEqual(run.side_msgs(502, b"<html>bad gateway</html>", None), ["<html>bad gateway</html>"])


def E_(path, kind, a=None, b=None):
    e = {"path": path, "kind": kind}
    if a is not None:
        e["a"] = a
    if b is not None:
        e["b"] = b
    return e


class Allowlist(unittest.TestCase):
    def cls(self, ep, entries, codes=(), c="entity", vars_=None, node=None):
        return allow.classify({"ep": ep, "cls": c, "ours_codes": list(codes), "entries": entries,
                               "vars": vars_ or {}, "node": node})

    def test_documented_and_stock_wrong(self):
        self.assertEqual(self.cls("gql.pactQuery", [E_("/data", "Type", "None", "{}"), E_("/errors", "Extra", "[]")],
                                  ["NOT_SUPPORTED"])[1], ["pactQuery-not-supported"])
        self.assertEqual(self.cls("gql.gasLimitEstimate", [E_("/errors/0/code", "Changed", "NOT_SUPPORTED", "INTERNAL_SERVER_ERROR")],
                                  ["NOT_SUPPORTED"])[0], "allowlisted")
        self.assertEqual(self.cls("gql.networkInfo", [E_("/data/networkInfo", "Type", "{}", "None"),
                                                      E_("/errors", "Missing", None, "[{'code': 'INTERNAL_SERVER_ERROR'}]")])[0], "allowlisted")
        self.assertEqual(self.cls("gql.networkInfo", [E_("/data/networkInfo/countersOfEachChain/3/totalGasUsedInKda", "Missing", None, "1.5")],
                                  ["NOT_AVAILABLE"])[0], "allowlisted")
        self.assertEqual(self.cls("gql.networkInfo", [E_("/data/networkInfo/coinsInCirculation", "Changed", "#38e1", "#37e1")])[0], "allowlisted")
        self.assertEqual(self.cls("gql.networkInfo", [E_("/data/networkInfo/coinsInCirculation", "Changed", "#36e1", "#37e1")])[0], "diff")
        self.assertEqual(self.cls("gql.block", [E_("/data", "Type", "None", "{'block': None}"),
                                                E_("/errors/0/code", "Changed", "BAD_USER_INPUT", "INTERNAL_SERVER_ERROR")],
                                  ["BAD_USER_INPUT"], "edge")[0], "allowlisted")
        self.assertEqual(self.cls("gql.fungibleChainAccount", [E_("/data", "Type", "None", "{}"), E_("/errors", "Extra", "[]")],
                                  ["BAD_USER_INPUT"], "invalid")[0], "allowlisted")
        # the same refusal on an entity case stays open
        self.assertEqual(self.cls("gql.fungibleChainAccount", [E_("/data", "Type", "None", "{}"), E_("/errors", "Extra", "[]")],
                                  ["BAD_USER_INPUT"], "entity")[0], "diff")
        self.assertEqual(self.cls("gql.fungibleAccount", [E_("/data/fungibleAccount/chainAccounts/0/guard/__typename", "Changed", "KeysetGuard", "RawGuard"),
                                                          E_("/data/fungibleAccount/chainAccounts/0/guard/keys", "Extra", "[]")])[0], "allowlisted")
        self.assertEqual(self.cls("gql.block", [E_("/~graphql-r57-float", "Changed")])[0], "allowlisted")
        self.assertEqual(self.cls("gql.fungibleAccount", [E_("/~known-gap-missing-chain-rows", "Missing")])[0], "known-gap")

    def test_node_condition(self):
        ent = [E_("/data", "Type", "{}", "None"), E_("/errors", "Missing", None, "[{'code': 'INTERNAL_SERVER_ERROR'}]")]
        self.assertEqual(self.cls("gql.completedBlockHeights", ent, vars_={"chainIds": []}, node={"ok": True})[0], "allowlisted")
        self.assertEqual(self.cls("gql.completedBlockHeights", ent, vars_={"chainIds": []}, node=None)[0], "diff")
        self.assertEqual(self.cls("gql.completedBlockHeights", ent, vars_={"chainIds": ["4"]}, node={"ok": True})[0], "diff")

    def test_node_undecodable_id(self):
        ent = [E_("/data", "Type", "None", "{'node': None}"), E_("/errors", "Extra", "[]")]
        self.assertEqual(self.cls("gql.node", ent, ["BAD_USER_INPUT"], "entity", {"id": "AAAA"})[0], "allowlisted")
        good = E.encode_id("Block", "abc")
        self.assertEqual(self.cls("gql.node", ent, ["BAD_USER_INPUT"], "entity", {"id": good})[0], "diff")

    def test_ours_bugs_stay_open(self):
        self.assertEqual(self.cls("gql.fungibleChainAccounts", [E_("/data/fungibleChainAccounts", "Type", "None", "[]")])[0], "diff")
        self.assertEqual(self.cls("gql.gasLimitEstimate", [E_("/data", "Type", "None", "{'gasLimitEstimate': []}"),
                                                           E_("/errors", "Extra", "[{'code': 'NOT_SUPPORTED'}]")],
                                  ["NOT_SUPPORTED"])[0], "diff")

    def test_every_entry_has_reason(self):
        for r in allow.RULES:
            self.assertIn(r.verdict, ("DOCUMENTED", "STOCK-WRONG", "HARNESS", "KNOWN"))
            self.assertTrue(len(r.reason) > 20, r.id)

    def test_every_rule_has_id_reason_accept_class_and_ruling(self):
        # H2 item 6 (owner R-P5P9-162): the docs list every accepted difference with why.
        ids = [r.id for r in allow.RULES]
        self.assertEqual(len(ids), len(set(ids)))
        for r in allow.RULES:
            self.assertIn(r.accept, allow.ACCEPT_CLASSES, r.id)
            self.assertTrue(len(r.ruling.strip()) > 10, r.id)
        rows = allow.table()
        self.assertEqual([x["id"] for x in rows], ids)
        for x in rows:
            self.assertEqual(sorted(x), ["accept_class", "id", "reason", "ruling"])
        self.assertEqual({x["accept_class"] for x in rows} - set(allow.ACCEPT_CLASSES), set())
        by = {x["id"]: x["accept_class"] for x in rows}
        self.assertEqual(by["pactQuery-not-supported"], "unsupported-by-design")
        self.assertEqual(by["lastBlockHeight-tip-race"], "tip-timing")
        self.assertEqual(by["networkInfo-stock-error"], "stock-wrong-node-evidence")
        self.assertEqual(by["ours-refuses-lenient-input"], "intended-documented")

    def test_transaction_min_depth_has_no_rule(self):
        # H2 item 1: refused like stock after FIX-4 -> `equal`; nothing here explains it.
        ent = [E_("/data/transaction", "Type", "{}", "None"), E_("/errors", "Missing", None, "[{'code': 'INTERNAL_SERVER_ERROR'}]")]
        self.assertEqual(self.cls("gql.transaction", ent, c="edge", vars_={"minimumDepth": 101})[0], "diff")
        self.assertFalse([r for r in allow.RULES if r.eps and "gql.transaction" in r.eps])

    def order_ents(self, root="events", n=2, extra=()):
        out = []
        for i in range(n):
            out += [E_("/data/%s/edges/%d/node/id" % (root, i), "Changed", "x%d" % i, "y%d" % i),
                    E_("/data/%s/edges/%d/node/requestKey" % (root, i), "Changed", "x%d" % i, "y%d" % i)]
        return out + list(extra)

    def test_multi_block_order(self):
        ok = {"ok": True}
        v = {"qualifiedEventName": "coin.TRANSFER", "chainId": "19", "minHeight": 10, "maxHeight": 11, "first": 10}
        k, ids = self.cls("gql.events", self.order_ents(), c="real", vars_=v, node=ok)
        self.assertEqual((k, ids), ("allowlisted", ["multi-block-order"]))
        k, ids = self.cls("gql.transactions", self.order_ents("transactions"), c="real", vars_={"first": 10}, node=ok)
        self.assertEqual(ids, ["multi-block-order"])
        # the node must have passed for this row
        self.assertEqual(self.cls("gql.events", self.order_ents(), c="real", vars_=v, node=None)[0], "diff")
        self.assertEqual(self.cls("gql.events", self.order_ents(), c="real", vars_=v, node={"ok": False})[0], "diff")
        # a single-block list is in node order since FIX-4: a diff there stays open
        for one in ({"blockHash": "B" * 43}, {"requestKey": "R" * 43},
                    {"chainId": "19", "minHeight": 10, "maxHeight": 10}):
            self.assertEqual(self.cls("gql.events", self.order_ents(), c="real", vars_=dict(v, **one), node=ok)[0], "diff")
        # a row whose identity is the same but a field differs is a real diff
        field = [E_("/data/events/edges/0/node/parameters", "Changed", "[1]", "[2]")]
        self.assertEqual(self.cls("gql.events", field, c="real", vars_=v, node=ok)[0], "diff")
        # an identity change at one edge only, with a field change at another: still open
        mixed = self.order_ents(n=1, extra=[E_("/data/events/edges/1/node/parameters", "Changed", "[1]", "[2]")])
        self.assertEqual(self.cls("gql.events", mixed, c="real", vars_=v, node=ok)[0], "diff")
        # a count or a page-info difference is not an order difference
        cnt = self.order_ents(extra=[E_("/data/events/totalCount", "Changed", 3, 4)])
        self.assertEqual(self.cls("gql.events", cnt, c="real", vars_=v, node=ok)[0], "diff")
        # other endpoints are not covered
        self.assertEqual(self.cls("gql.blocksFromHeight", self.order_ents("blocksFromHeight"), c="real", vars_=v, node=ok)[0], "diff")

    def test_stock_orphan_count(self):
        ent = [E_("/data/transactionsByPublicKey/totalCount", "Changed", 1825, 1827)]
        ep = "gql.transactionsByPublicKey"
        self.assertEqual(self.cls(ep, ent, node={"ok": True, "stock_orphans": 2}), ("allowlisted", ["totalcount-stock-orphans"]))
        self.assertEqual(self.cls(ep, ent, node={"ok": True, "stock_orphans": 3})[1], ["totalcount-stock-orphans"])
        # fewer confirmed orphans than the gap, no evidence, or the node check failed: a diff
        self.assertEqual(self.cls(ep, ent, node={"ok": True, "stock_orphans": 1})[0], "diff")
        self.assertEqual(self.cls(ep, ent, node={"ok": True})[0], "diff")
        self.assertEqual(self.cls(ep, ent, node=None)[0], "diff")
        self.assertEqual(self.cls(ep, ent, node={"ok": False, "stock_orphans": 5})[0], "diff")
        # ours above stock is never an orphan count
        self.assertEqual(self.cls(ep, [E_("/data/transactionsByPublicKey/totalCount", "Changed", 1827, 1825)],
                                  node={"ok": True, "stock_orphans": 9})[0], "diff")
        # only the count is explained
        both = ent + [E_("/data/transactionsByPublicKey/edges/0/node/hash", "Changed", "a", "b")]
        self.assertEqual(self.cls(ep, both, node={"ok": True, "stock_orphans": 2})[0], "diff")

    def test_tip_timing(self):
        ent = [E_("/data/blocksFromDepth/edges/1/node/hash", "Changed", "a", "b"),
               E_("/data/blocksFromDepth/edges/2/node/hash", "Changed", "b", "c")]

        def go(ep, ents, tip, vars_=None):
            return allow.classify({"ep": ep, "cls": "entity", "ours_codes": [], "entries": ents, "vars": vars_ or {},
                                   "node": None, "tip": tip})
        self.assertEqual(go("gql.blocksFromDepth", ent, {"ours": 100, "stock": 101}), ("allowlisted", ["tip-timing"]))
        self.assertEqual(go("gql.blocksFromDepth", ent, {"ours": 100, "stock": 100 + allow.TIP_WINDOW})[1], ["tip-timing"])
        # the window is bounded, and unrecorded or unmoved heights explain nothing
        self.assertEqual(go("gql.blocksFromDepth", ent, {"ours": 100, "stock": 101 + allow.TIP_WINDOW})[0], "diff")
        self.assertEqual(go("gql.blocksFromDepth", ent, {"ours": 100, "stock": 100})[0], "diff")
        self.assertEqual(go("gql.blocksFromDepth", ent, {"ours": None, "stock": 101})[0], "diff")
        self.assertEqual(go("gql.blocksFromDepth", ent, None)[0], "diff")
        # a list pinned to a height window or a cursor does not move with the tip
        lst = [E_("/data/transfers/edges/0/node/requestKey", "Changed", "a", "b")]
        self.assertEqual(go("gql.transfers", lst, {"ours": 5, "stock": 6}, {"first": 1})[0], "allowlisted")
        self.assertEqual(go("gql.transfers", lst, {"ours": 5, "stock": 6}, {"first": 1, "maxHeight": 3})[0], "diff")
        self.assertEqual(go("gql.transfers", lst, {"ours": 5, "stock": 6}, {"first": 1, "after": "c"})[0], "diff")
        # errors and status differences are not timing
        err = [E_("/errors/0/code", "Changed", "A", "B")]
        self.assertEqual(go("gql.blocksFromDepth", err, {"ours": 5, "stock": 6})[0], "diff")
        # other endpoints are not tip-anchored
        self.assertEqual(go("gql.fungibleAccount", ent, {"ours": 5, "stock": 6})[0], "diff")

    def test_tip_pair_from_samples(self):
        log = [(100.0, 50), (110.0, 51), (125.0, 52)]
        self.assertEqual(allow.tip_before(log, 112.0), 51)
        self.assertEqual(allow.tip_before(log, 99.0), None)
        # a sample older than the freshness bound is not a reading at that time
        self.assertEqual(allow.tip_before(log, 125.0 + allow.TIP_SAMPLE_AGE + 1), None)


def blk(h, c, x):
    return {"node": {"hash": x, "height": h, "chainId": str(c)}}


class NodeChecks(unittest.TestCase):
    def test_block_range(self):
        canon = {(0, 10): "a", (0, 11): "b", (1, 10): "c"}
        body = {"data": {"blocksFromHeight": {"edges": [blk(10, 0, "a"), blk(10, 1, "c"), blk(11, 0, "b")]}}}
        rows = N.block_rows("gql.blocksFromHeight", body)
        self.assertEqual(N.check_block_range("gql.blocksFromHeight", {"startHeight": 10}, rows, canon, {0: 20, 1: 20}), [])
        bad = {"data": {"blocksFromHeight": {"edges": [blk(11, 0, "b"), blk(10, 0, "x")]}}}
        p = N.check_block_range("gql.blocksFromHeight", {"startHeight": 11}, N.block_rows("gql.blocksFromHeight", bad), canon, {0: 20})
        self.assertIn("order", p)
        self.assertIn("not-canonical", p)
        self.assertIn("below-start", p)
        d = {"data": {"blocksFromDepth": {"edges": [blk(11, 0, "b"), blk(10, 0, "a")]}}}
        self.assertEqual(N.check_block_range("gql.blocksFromDepth", {"minimumDepth": 5},
                                             N.block_rows("gql.blocksFromDepth", d), canon, {0: 16}), [])
        self.assertIn("window", N.check_block_range("gql.blocksFromDepth", {"minimumDepth": 9},
                                                     N.block_rows("gql.blocksFromDepth", d), canon, {0: 16}))

    def test_tx_list(self):
        def tx(rk, c, s, bh):
            return {"node": {"hash": rk, "cmd": {"meta": {"chainId": c, "sender": s}}, "result": {"block": {"hash": bh}}}}
        body = {"data": {"transactions": {"totalCount": 2, "pageInfo": {"hasNextPage": False, "hasPreviousPage": False},
                                          "edges": [tx("r1", "0", "alice", "B1"), tx("r2", "1", "alice", "B2")]}}}
        rows = N.tx_rows("gql.transactions", body)
        polled = {(0, "r1"): {"blockHash": "B1"}, (1, "r2"): {"blockHash": "B2"}}
        self.assertEqual(N.check_tx_list("gql.transactions", {"accountName": "alice"}, body, rows, polled), [])
        body["data"]["transactions"]["edges"].append(tx("r1", "0", "bob", "B1"))
        polled2 = {(0, "r1"): {"blockHash": "B1"}, (1, "r2"): None}
        p = N.check_tx_list("gql.transactions", {"accountName": "alice"}, body, N.tx_rows("gql.transactions", body), polled2)
        for k in ("missing-on-node", "sender", "duplicate", "totalCount"):
            self.assertIn(k, p)
        self.assertEqual(N.sample_rows(list(range(40)), 10), [0, 1, 2, 3, 4, 35, 36, 37, 38, 39])
        # genesis keys repeat across chains: a height-0 poll answer is not compared by block
        g = {"data": {"transactions": {"edges": [tx("r1", "0", "alice", "G5")]}}}
        self.assertEqual(N.check_tx_list("gql.transactions", {}, g, N.tx_rows("gql.transactions", g),
                                         {(0, "r1"): {"blockHash": "G0", "blockHeight": 0}}), [])
        # two transfers of one tx in a min selection are not duplicates
        t = {"data": {"transfers": {"edges": [{"node": {"requestKey": "r1"}}, {"node": {"requestKey": "r1"}}]}}}
        self.assertEqual(N.check_tx_list("gql.transfers", {}, t, N.tx_rows("gql.transfers", t), {}), [])

    def test_self_transfer_listed_twice(self):
        # run-2 triage class 14: /txs/account lists a self-transfer as sender and as receiver
        def row(rk, frm, to, order=3):
            return {"node": {"requestKey": rk, "chainId": "14", "blockHash": "B", "orderIndex": order,
                             "fromAccount": frm, "toAccount": to}}

        def chk(ep, edges, v=None):
            body = {"data": {"transfers": {"edges": edges}}} if ep == "gql.transfers" else [e["node"] for e in edges]
            return N.check_tx_list(ep, v or {}, body, N.tx_rows(ep, body), {})
        s = row("r1", "alice", "alice")
        self.assertEqual(chk("gql.transfers", [s, s], {"accountName": "alice"}), [])
        self.assertIn("duplicate", chk("gql.transfers", [s, s, s], {"accountName": "alice"}))
        t = row("r2", "alice", "bob")
        self.assertIn("duplicate", chk("gql.transfers", [t, t], {"accountName": "alice"}))
        # only on the account-filtered lists
        self.assertIn("duplicate", chk("gql.transfers", [s, s]))
        body = [{"requestKey": "r1", "chain": 14, "blockHash": "B", "idx": 3, "fromAccount": "alice", "toAccount": "alice"}] * 2
        self.assertEqual(N.check_tx_list("compat.account", {}, body, N.tx_rows("compat.account", body), {}), [])
        self.assertIn("duplicate", N.check_tx_list("compat.account", {}, body * 2, N.tx_rows("compat.account", body * 2), {}))
        body2 = [dict(body[0], toAccount="bob")] * 2
        self.assertIn("duplicate", N.check_tx_list("compat.account", {}, body2, N.tx_rows("compat.account", body2), {}))

    def test_tip_race_is_not_a_failure(self):
        # a mismatch within TIP_DEPTH of the tip can be a reorg between the two reads
        canon = {(2, 98): "n98", (2, 50): "n50"}
        near = [(2, 98, "o98")]
        p = N.check_block_range("gql.blocksFromHeight", {}, near, canon, {2: 100})
        self.assertEqual(p, ["tip-race"])
        self.assertEqual(N.failures(p), [])
        far = [(2, 50, "o50")]
        p = N.check_block_range("gql.blocksFromHeight", {}, far, canon, {2: 100})
        self.assertEqual(p, ["not-canonical"])
        self.assertEqual(N.failures(p), ["not-canonical"])
        # both kinds: the deep one still fails
        p = N.check_block_range("gql.blocksFromHeight", {}, far + near, canon, {2: 100})
        self.assertEqual(N.failures(p), ["not-canonical"])

    def test_tip_pair_reads_the_log_only(self):
        class Http:
            def __init__(self):
                self.calls = 0

            def call(self, base, path="", body=None, accept=None):
                self.calls += 1
                return {"status": 200, "raw": json.dumps({"hashes": {"0": {"height": 100 + self.calls, "hash": "x"}}})}
        h = Http()
        n = N.Node(h, "http://node", "mainnet01", rps=1000)
        n.tips(fresh=True)                          # a sample: logged at the current time
        t0 = n.tip_log[-1][0]
        self.assertEqual(n.tip_log[-1][1], 101)
        n.tip_log.add(t0 + 1.0, 102)
        n.tip_log.add(t0 + 2.0, 104)
        calls = h.calls
        self.assertEqual(n.tip_pair(t0 + 0.5, t0 + 2.5), {"ours": 101, "stock": 104})
        self.assertEqual(h.calls, calls)            # per request: no node read
        # no sample before ours' read, or none fresh enough: nothing recorded
        self.assertIsNone(n.tip_pair(t0 - 1000, t0 + 2.5))
        self.assertIsNone(n.tip_pair(t0 + 0.5, t0 + 2.0 + N.TIP_ROW_AGE + 1))

    def test_tip_log_lookup(self):
        lg = N.TipLog(maxlen=3)
        for t, h in [(100.0, 50), (101.0, 51), (103.0, 52)]:
            lg.add(t, h)
        self.assertEqual(lg.at(101.5, 5), 51)
        self.assertEqual(lg.at(103.0, 5), 52)
        self.assertIsNone(lg.at(99.0, 5))
        self.assertIsNone(lg.at(110.0, 5))
        lg.add(104.0, 53)                           # the oldest sample falls out
        self.assertEqual(len(lg), 3)
        self.assertIsNone(lg.at(100.5, 5))

    def test_orphan_evidence(self):
        canon = {(1, 10): "good", (1, 11): "ok2"}
        rows = [(1, 10, "orphan"), (1, 11, "ok2"), (1, 12, "x")]
        self.assertEqual(N.orphan_evidence(rows, canon), {"stock_orphans": 1, "unresolved": 1})
        self.assertEqual(N.orphan_evidence([], canon), {"stock_orphans": 0, "unresolved": 0})

    def test_tip_slack(self):
        rows = [(0, 12, None)]
        self.assertEqual(N.check_block_range("gql.completedBlockHeights", {}, rows, {}, {0: 10}), [])
        self.assertIn("above-tip", N.check_block_range("gql.completedBlockHeights", {}, rows, {}, {0: 8}))


class Rereferee(unittest.TestCase):
    """H2 part B: stored records re-classified with the current allowlist; no request is sent."""

    def rec(self, id, ep, klass, ents, node=None, tip=None, st=200, codes=()):
        r = {"id": id, "ep": ep, "cls": "real", "t": 1, "cov": "stock-sample",
             "ours": {"st": 200, "ms": 1, "gql_errors": list(codes)}, "stock": {"host": "h", "st": st, "ms": 1},
             "v": {"class": klass, "ids": [], "known": [], "entries": ents}}
        if node is not None:
            r["node"] = node
        if tip is not None:
            r["tip"] = tip
        return r

    def test_counts_before_and_after(self):
        order = [E_("/data/events/edges/0/node/id", "Changed", "a", "b"),
                 E_("/data/events/edges/0/node/requestKey", "Changed", "a", "b")]
        field = [E_("/data/events/edges/0/node/parameters", "Changed", "1", "2")]
        rows = [
            ("sample", self.rec("gql.events:5", "gql.events", "diff", order, node={"ok": True})),
            ("sample", self.rec("gql.events:6", "gql.events", "diff", field, node={"ok": True})),
            ("results", self.rec("gql.events:7", "gql.events", "status", order, node={"ok": True}, st=500)),
            ("results", self.rec("gql.events:8", "gql.events", "equal", [])),
            ("results", self.rec("gql.events:9", "gql.events", "allowlisted", order)),
        ]
        vars_of = {"gql.events:5": {"first": 10}, "gql.events:6": {"first": 10}, "gql.events:7": {"first": 10},
                   "gql.events:8": {}, "gql.events:9": {}}
        out = run.rereferee_rows(rows, vars_of.get)
        self.assertEqual(out["before"], {"sample": 2, "results": 1})
        self.assertEqual(out["after"], {"sample": 1})
        self.assertEqual(out["total_before"], 3)
        self.assertEqual(out["total_after"], 1)
        self.assertEqual(out["moved"], {("gql.events", "multi-block-order", "allowlisted"): 2})
        self.assertEqual(out["left"], {"gql.events": 1})
        self.assertEqual(out["origin"], {"run2": [3, 1]})
        md = run.rereferee_md(out, "run2")
        self.assertIn("unexplained before: 3", md)
        self.assertIn("unexplained after: 1", md)
        self.assertIn("multi-block-order", md)
        self.assertNotIn("gql.events:5", md)       # aggregate counts only: no case ids, no request contents

    def test_stored_tip_and_node_evidence_are_used(self):
        ent = [E_("/data/blocksFromDepth/edges/1/node/hash", "Changed", "a", "b")]
        rows = [("sample", self.rec("a:1", "gql.blocksFromDepth", "diff", ent, tip={"ours": 5, "stock": 6})),
                ("sample", self.rec("a:2", "gql.blocksFromDepth", "diff", ent))]
        rows[1][1]["from"] = "run1"
        out = run.rereferee_rows(rows, lambda i: {})
        self.assertEqual((out["total_before"], out["total_after"]), (2, 1))
        self.assertEqual(out["origin"], {"run2": [1, 0], "run1": [1, 1]})


class Import(unittest.TestCase):
    def test_filters_and_reclassifies(self):
        rows = [
            {"id": "gql.pactQuery:1", "ep": "gql.pactQuery", "cls": "edge", "ours": {"st": 200, "gql_errors": ["NOT_SUPPORTED"]},
             "stock": {"host": "a", "st": 200}, "v": {"class": "diff", "entries": [E_("/data", "Type", "None", "{}"), E_("/errors", "Extra", "[]")]}},
            {"id": "gql.lastBlockHeight:2", "ep": "gql.lastBlockHeight", "cls": "edge", "ours": {"st": 429}, "stock": {"host": "a", "st": 200}},
            {"id": "gql.node:3", "ep": "gql.node", "cls": "entity", "ours": {"st": 200}, "stock": {"host": "a", "st": 200}},
            {"id": "gql.transfers:4", "ep": "gql.transfers", "cls": "entity", "ours": {"st": 200}, "stock": {"host": "a", "st": 504}},
            {"id": "native.tx:5", "ep": "native.tx", "cls": "entity", "ours": {"st": 200}},
            {"id": "gql.fungibleAccount:6", "ep": "gql.fungibleAccount", "cls": "entity", "ours": {"st": 200},
             "stock": {"host": "a", "st": 504}, "v": {"class": "status", "entries": []}},
            {"id": "native.blocks:7", "ep": "native.blocks", "cls": "entity", "ours": {"st": 200}, "inv": []},
        ]
        with tempfile.TemporaryDirectory() as d:
            src, dst = os.path.join(d, "a"), os.path.join(d, "b")
            with open(src, "w") as f:
                f.write("\n".join(json.dumps(r) for r in rows) + "\n")
            self.assertEqual(run.import_rows(src, dst), 3)
            got = {r["id"]: r for r in map(json.loads, open(dst))}
        self.assertEqual(set(got), {"gql.pactQuery:1", "gql.fungibleAccount:6", "native.blocks:7"})
        self.assertEqual(got["gql.pactQuery:1"]["v"]["class"], "allowlisted")
        self.assertEqual(got["gql.fungibleAccount:6"]["v"]["class"], "stock-unavailable")
        self.assertEqual(got["native.blocks:7"]["cov"], "native")

    def test_host_split(self):
        cs = [{"id": "gql.block:%d" % i} for i in range(6)]
        lanes = [[c["id"] for c in run.host_cases(iter(cs), i, 2, {"gql.block:4"})] for i in (0, 1)]
        self.assertEqual(lanes, [["gql.block:0", "gql.block:2"], ["gql.block:1", "gql.block:3", "gql.block:5"]])

    def test_chain_accounts_as_set(self):
        a = {"data": {"fungibleChainAccounts": [{"chainId": "10"}, {"chainId": "2"}]}}
        b = {"data": {"fungibleChainAccounts": [{"chainId": "2"}, {"chainId": "10"}]}}
        a, b = run.normalize_pair("gql.fungibleChainAccounts", a, b)
        self.assertEqual(a, b)


class FakeNodeHttp:
    """A chainweb node: /cut and header/branch for chains whose blocks are 'canon<height>'."""

    def __init__(self, tip=1000, chains=(1, 2)):
        self.tip, self.chains, self.calls = tip, chains, []

    def call(self, base, path="", body=None, accept=None):
        self.calls.append(path)
        if path.startswith("/cut"):
            return {"status": 200, "raw": json.dumps({"hashes": {str(c): {"height": self.tip, "hash": "tip%d" % c}
                                                                 for c in self.chains}})}
        if "/header/branch" in path:
            q = dict(x.split("=") for x in path.split("?")[1].split("&"))
            lo, hi = int(q["minheight"]), int(q["maxheight"])
            return {"status": 200, "raw": json.dumps({"items": [{"height": h, "hash": "canon%d" % h}
                                                                for h in range(lo, hi + 1)]})}
        return {"status": 404, "raw": b""}


class Evidence(unittest.TestCase):
    """H3 item 1: the runner records the tip at both reads for every stock-paired call and
    fetches the node evidence for rows only stock lists, within a read bound per case."""

    def test_tip_fields_for_every_paired_row(self):
        node = N.Node(FakeNodeHttp(), "http://node", "mainnet01", rps=1000)
        for t, h in [(100.0, 50), (101.0, 51), (102.0, 53)]:
            node.tip_log.add(t, h)
        idx = {"ours": N.TipLog(), "h1": N.TipLog()}
        idx["ours"].add(100.0, 49)
        idx["h1"].add(101.0, 48)
        got = run.tip_fields(node, idx, "h1", 100.5, 102.5)
        self.assertEqual(got, {"tip": {"ours": 50, "stock": 53}, "idx": {"ours": 49, "stock": 48}})
        # indexer samples are evidence too: missing ones are left out, the node tip stays
        self.assertEqual(run.tip_fields(node, {}, "h1", 100.5, 102.5), {"tip": {"ours": 50, "stock": 53}})
        # no node sample in range: only the indexer heights
        self.assertEqual(run.tip_fields(node, idx, "h1", 10.0, 102.5), {"idx": {"stock": 48}})
        self.assertEqual(run.tip_fields(None, idx, "h1", 100.5, 102.5), {"idx": {"ours": 49, "stock": 48}})

    def test_trigger_is_a_count_gap_with_stock_ahead(self):
        ep = "gql.transactionsByPublicKey"
        gap = E_("/data/transactionsByPublicKey/totalCount", "Changed", 5, 7)
        self.assertTrue(N.needs_evidence(ep, [gap]))
        self.assertTrue(N.needs_evidence(ep, [E_("/data/transactionsByPublicKey/edges/3", "Missing", None, "{}")]))
        self.assertFalse(N.needs_evidence(ep, [E_("/data/transactionsByPublicKey/totalCount", "Changed", 7, 5)]))
        self.assertFalse(N.needs_evidence(ep, [E_("/data/transactionsByPublicKey/edges/0/node/hash", "Changed", "a", "b")]))
        self.assertFalse(N.needs_evidence("gql.fungibleAccount", [gap]))
        self.assertFalse(N.needs_evidence(ep, []))

    def test_evidence_query_and_rows(self):
        v = N.evidence_vars({"publicKey": "k", "first": 20, "after": "c", "last": 1, "before": "d", "chainId": "3"})
        self.assertEqual(v, {"publicKey": "k", "chainId": "3", "first": N.EVIDENCE_PAGE})
        for ep in ("gql.transactions", "gql.transactionsByPublicKey", "gql.transfers"):
            doc = N.evidence_doc(ep)
            self.assertIn("$first: Int", doc)
            self.assertIn("$after: String", doc)
            self.assertIn("pageInfo", doc)
            self.assertIn("block", doc.lower())
        body = {"data": {"transactions": {"edges": [
            {"cursor": "c1", "node": {"hash": "H1", "cmd": {"meta": {"chainId": "7"}},
                                       "result": {"height": 90, "block": {"hash": "B1"}}}}]}}}
        self.assertEqual(N.evidence_rows("gql.transactions", body),
                         [{"key": ("H1", "B1", None), "chain": 7, "height": 90, "block": "B1"}])
        body = {"data": {"transfers": {"edges": [
            {"node": {"requestKey": "R", "orderIndex": 2, "chainId": "4", "height": 80, "blockHash": "B2"}}]}}}
        self.assertEqual(N.evidence_rows("gql.transfers", body),
                         [{"key": ("R", "B2", 2), "chain": 4, "height": 80, "block": "B2"}])

    def test_extra_rows_are_a_multiset_difference(self):
        r = lambda k, b: {"key": (k, b, None), "chain": 1, "height": 5, "block": b}  # noqa: E731
        ours = [r("a", "B1"), r("b", "B2")]
        stock = [r("a", "B1"), r("b", "B2"), r("b", "B2x"), r("a", "B1")]
        self.assertEqual([x["key"] for x in N.extra_rows(ours, stock)], [("b", "B2x", None), ("a", "B1", None)])
        self.assertEqual(N.extra_rows(stock, ours), [])

    def test_paging_stops_at_the_cap(self):
        def fetch(vars_):
            after = vars_.get("after")
            row = {None: "a", "c1": "b", "c2": "c"}[after]
            nxt = {None: "c1", "c1": "c2", "c2": None}[after]
            return {"data": {"transactions": {"pageInfo": {"hasNextPage": nxt is not None, "endCursor": nxt},
                                              "edges": [{"cursor": row, "node": {"hash": row, "cmd": {"meta": {"chainId": "1"}},
                                                         "result": {"height": 1, "block": {"hash": "B" + row}}}}]}}}
        rows, complete, n = N.page_rows(fetch, "gql.transactions", {"first": 3}, max_pages=5)
        self.assertEqual((len(rows), complete, n), (3, True, 3))
        rows, complete, n = N.page_rows(fetch, "gql.transactions", {"first": 3}, max_pages=2)
        self.assertEqual((len(rows), complete, n), (2, False, 2))
        # a failing page is incomplete, never a guess
        rows, complete, n = N.page_rows(lambda v: None, "gql.transactions", {}, max_pages=2)
        self.assertEqual((rows, complete), ([], False))

    def test_node_evidence_counts_orphans_within_the_read_bound(self):
        h = FakeNodeHttp(tip=1000, chains=(1, 2, 3))
        n = N.Node(h, "http://node", "mainnet01", rps=1000)
        rows = [{"key": ("r1", "canon500", None), "chain": 1, "height": 500, "block": "canon500"},   # canonical
                {"key": ("r2", "orphanX", None), "chain": 1, "height": 501, "block": "orphanX"},      # orphan
                {"key": ("r3", "orphanY", None), "chain": 2, "height": 700, "block": "orphanY"},      # orphan
                {"key": ("r4", "tipblk", None), "chain": 3, "height": 995, "block": "tipblk"}]        # near the tip
        ev = n.evidence(rows, max_reads=20)
        self.assertEqual((ev["stock_orphans"], ev["unresolved"], ev["extra"]), (2, 1, 4))
        self.assertLessEqual(ev["reads"], 20)
        self.assertEqual(ev["reads"], len([c for c in h.calls if "/header/branch" in c or c.startswith("/cut")]))

    def test_node_evidence_never_exceeds_the_cap(self):
        h = FakeNodeHttp(tip=100000, chains=tuple(range(20)))
        n = N.Node(h, "http://node", "mainnet01", rps=1000)
        # 30 rows on 20 chains, far apart: one read per chain at most, and the cap stops the rest
        rows = [{"key": ("r%d" % i, "bad", None), "chain": i % 20, "height": 1000 + 1000 * i, "block": "bad"}
                for i in range(30)]
        ev = n.evidence(rows, max_reads=5)
        self.assertLessEqual(ev["reads"], 5)
        self.assertEqual(ev["stock_orphans"] + ev["unresolved"], 30)
        self.assertGreater(ev["unresolved"], 0)

    def test_the_orphan_rule_fires_on_collected_evidence(self):
        n = N.Node(FakeNodeHttp(tip=1000), "http://node", "mainnet01", rps=1000)
        rows = [{"key": ("r%d" % i, "bad", None), "chain": 1, "height": 500 + i, "block": "bad"} for i in range(2)]
        ev = n.evidence(rows)
        ent = [E_("/data/transactionsByPublicKey/totalCount", "Changed", 1825, 1827)]
        ctx = {"ep": "gql.transactionsByPublicKey", "cls": "entity", "ours_codes": [], "entries": ent, "vars": {},
               "node": dict({"ok": True}, **ev)}
        self.assertEqual(allow.classify(ctx), ("allowlisted", ["totalcount-stock-orphans"]))
        ctx["node"] = dict({"ok": True}, **n.evidence(rows[:1]))      # the gap is 2, one orphan found
        self.assertEqual(allow.classify(ctx)[0], "diff")


class OrphanEvidenceFlow(unittest.TestCase):
    """The runner's evidence step end to end, with ours, stock and the node stubbed."""

    def page(self, rows, more=False):
        edges = [{"cursor": "c", "node": {"hash": h, "cmd": {"meta": {"chainId": "1"}},
                                          "result": {"height": ht, "block": {"hash": b}}}} for h, ht, b in rows]
        return {"status": 200, "raw": json.dumps({"data": {"transactionsByPublicKey": {
            "pageInfo": {"hasNextPage": more, "endCursor": "e"}, "edges": edges}}}).encode()}

    def go(self, ours_rows, stock_rows):
        sent = []
        stub = type("R", (), {})()
        stub.node = N.Node(FakeNodeHttp(tip=1000), "http://node", "mainnet01", rps=1000)
        stub.ours_call = lambda fam, req: sent.append(("ours", req["variables"])) or self.page(ours_rows)
        host = type("H", (), {"call": lambda h, cid, req, heavy: sent.append(("stock", req["variables"])) or self.page(stock_rows)})()
        case = {"id": "gql.transactionsByPublicKey:7", "ep": "gql.transactionsByPublicKey"}
        ev = run.Runner.orphan_evidence(stub, case, {"publicKey": "k", "first": 20}, host)
        return ev, sent

    def test_extra_stock_rows_in_orphan_blocks_are_counted(self):
        ours = [("t1", 500, "canon500"), ("t2", 600, "canon600")]
        stock = ours + [("t1", 500, "orphan1"), ("t2", 600, "orphan2")]
        ev, sent = self.go(ours, stock)
        self.assertEqual((ev["evidence"], ev["stock_orphans"], ev["unresolved"], ev["extra"]), ("node", 2, 0, 2))
        self.assertLessEqual(ev["reads"], nodecheck_reads())
        self.assertEqual(sent[0], ("ours", {"publicKey": "k", "first": N.EVIDENCE_PAGE}))
        self.assertEqual({w for w, _ in sent}, {"ours", "stock"})

    def test_equal_lists_have_no_extra_rows(self):
        rows = [("t1", 500, "canon500")]
        ev, _ = self.go(rows, rows)
        self.assertEqual((ev["evidence"], ev["extra"]), ("no-extra-rows", 0))
        self.assertNotIn("stock_orphans", ev)

    def test_an_incomplete_list_gives_no_orphan_count(self):
        stub = type("R", (), {})()
        stub.node = N.Node(FakeNodeHttp(), "http://node", "mainnet01", rps=1000)
        stub.ours_call = lambda fam, req: self.page([("t1", 500, "canon500")], more=True)   # never ends
        host = type("H", (), {"call": lambda h, cid, req, heavy: self.page([])})()
        ev = run.Runner.orphan_evidence(stub, {"id": "x:1", "ep": "gql.transactionsByPublicKey"}, {}, host)
        self.assertEqual(ev["evidence"], "incomplete")
        self.assertNotIn("stock_orphans", ev)


def nodecheck_reads():
    return N.EVIDENCE_READS


class PactAndGasStatus(unittest.TestCase):
    """H3 item 2: the 4,476 run-2 status rows are variable-coercion refusals (`pactQuery: true`,
    `input: -1`, an absent variable). The pre-FIX-4 build answered HTTP 200 with the codeless
    variable error (code ERROR) and `data`; stock answers HTTP 500 INTERNAL_SERVER_ERROR. The
    NOT_SUPPORTED rules are about a call that reached the resolver, so they correctly do not
    cover the old shape; FIX-4 item 3 gives the 500 shape, which the pair-diff calls equal."""

    ent = [E_("/data", "Extra", None, None), E_("/errors/0/code", "Type", None, "INTERNAL_SERVER_ERROR")]

    def test_pre_fix4_shape_stays_open(self):
        for ep in ("gql.pactQuery", "gql.gasLimitEstimate"):
            got = allow.classify({"ep": ep, "cls": "invalid", "ours_codes": ["ERROR"], "entries": self.ent,
                                  "vars": {}, "node": None})
            self.assertEqual(got, ("diff", []), ep)

    def test_not_supported_rules_still_cover_a_call_that_reached_the_resolver(self):
        ent = [E_("/data", "Type", "None", "{}"), E_("/errors", "Extra", "[]")]
        self.assertEqual(allow.classify({"ep": "gql.pactQuery", "cls": "real", "ours_codes": ["NOT_SUPPORTED"],
                                         "entries": ent, "vars": {}, "node": None})[0], "allowlisted")

    def test_gas_limit_empty_input_is_not_allowlisted(self):
        # `input: []`: stock answers [], and with FIX-4 ours does too, so a row left over with
        # NOT_SUPPORTED on ours stays open (no stock error to explain it).
        ent = [E_("/data", "Type", "None", "{}"), E_("/errors", "Extra", "[]")]
        self.assertEqual(allow.classify({"ep": "gql.gasLimitEstimate", "cls": "invalid", "ours_codes": ["NOT_SUPPORTED"],
                                         "entries": ent, "vars": {"input": []}, "node": None})[0], "diff")

    def test_status_rows_are_selected_for_run_3(self):
        rows = [("results", {"id": "gql.pactQuery:134", "ep": "gql.pactQuery", "v": {"class": "status"}})]
        got = select3.pick(rows, heavy=set())
        self.assertEqual(got["gql.pactQuery"], [("gql.pactQuery:134", "status", False)])


class Select3(unittest.TestCase):
    """H3 item 3: the run-3 case list."""

    def rows(self):
        def r(i, ep, c):
            return {"id": i, "ep": ep, "v": {"class": c}}
        return [
            ("results", r("gql.node:1", "gql.node", "diff")),
            ("results", r("gql.node:2", "gql.node", "equal")),
            ("results", r("gql.node:3", "gql.node", "stock-unavailable")),
            ("results", r("gql.node:4", "gql.node", "allowlisted")),
            ("results", r("gql.node:5", "gql.node", "known-gap")),
            ("results", r("gql.pactQuery:9", "gql.pactQuery", "status")),
            ("results", r("gql.node:1", "gql.node", "diff")),                       # a row twice: one case
            ("results", {"id": "native.tx:1", "ep": "native.tx", "cov": "native"}),   # no verdict
            ("sample", r("gql.transfers:7", "gql.transfers", "diff")),
            ("sample", r("gql.transfers:8", "gql.transfers", "stock-unavailable")),   # heavy timeout: not selected
            ("sample", r("gql.transfers:9", "gql.transfers", "status")),
            ("results", r("gql.transfers:10", "gql.transfers", "diff")),              # heavy ep in results: ours-only row
        ]

    def test_pick_by_verdict_and_coverage(self):
        got = select3.pick(self.rows(), heavy={"gql.transfers"})
        self.assertEqual(got["gql.node"], [("gql.node:1", "diff", False), ("gql.node:3", "stock-unavailable", False)])
        self.assertEqual(got["gql.pactQuery"], [("gql.pactQuery:9", "status", False)])
        # a heavy endpoint: only the stock-sample diffs and status rows, run again against stock
        self.assertEqual(got["gql.transfers"], [("gql.transfers:7", "diff", True), ("gql.transfers:9", "status", True)])
        self.assertNotIn("native.tx", got)

    def mk(self, d, ep, n, cls="real", extra=None):
        os.makedirs(d, exist_ok=True)
        with gzip.open(os.path.join(d, ep + ".jsonl.gz"), "wt") as f:
            for i in range(n):
                req = {"variables": dict(extra(i) if extra else {})}
                f.write(json.dumps({"id": "%s:%d" % (ep, i), "ep": ep, "cls": cls(i) if callable(cls) else cls,
                                    "req": req}) + "\n")

    def test_build_orders_run2_cases_first_and_renumbers_fresh(self):
        with tempfile.TemporaryDirectory() as d:
            run2, fresh, out = (os.path.join(d, x) for x in ("run2", "fresh", "out"))
            self.mk(run2, "gql.node", 6)
            self.mk(fresh, "gql.node", 4)
            picks = {"gql.node": [("gql.node:4", "diff", False), ("gql.node:1", "stock-unavailable", False)]}
            man = select3.build(picks, run2, fresh, out, fresh_full={"gql.node"}, err_eps=set())
            cs = [json.loads(l) for l in gzip.open(os.path.join(out, "gql.node.jsonl.gz"), "rt")]
        self.assertEqual([c["id"] for c in cs[:2]], ["gql.node:1", "gql.node:4"])           # case order, not verdict order
        self.assertEqual([c["sel"] for c in cs[:2]], ["run2-stock-unavailable", "run2-diff"])
        fresh_ids = [c["id"] for c in cs[2:]]
        self.assertEqual(fresh_ids, ["gql.node:%d" % (select3.FRESH_BASE + i) for i in range(4)])
        self.assertTrue(all(c["sel"] == "fresh" for c in cs[2:]))
        self.assertEqual(len(set(c["id"] for c in cs)), 6)
        self.assertEqual(man["endpoints"]["gql.node"], {"run2-diff": 1, "run2-stock-unavailable": 1, "fresh": 4})

    def test_fresh_error_mapping_takes_invalid_and_edge_classes_only(self):
        with tempfile.TemporaryDirectory() as d:
            run2, fresh, out = (os.path.join(d, x) for x in ("run2", "fresh", "out"))
            cl = ["real", "invalid", "edge", "entity", "shape", "invalid"]
            self.mk(fresh, "gql.networkInfo", 6, cls=lambda i: cl[i])
            man = select3.build({}, run2, fresh, out, fresh_full=set(), err_eps={"gql.networkInfo"})
            cs = [json.loads(l) for l in gzip.open(os.path.join(out, "gql.networkInfo.jsonl.gz"), "rt")]
        self.assertEqual([c["cls"] for c in cs], ["invalid", "edge", "invalid"])
        self.assertEqual(man["endpoints"]["gql.networkInfo"], {"fresh": 3})

    def test_heavy_fresh_cases_flag_a_stock_sample(self):
        with tempfile.TemporaryDirectory() as d:
            run2, fresh, out = (os.path.join(d, x) for x in ("run2", "fresh", "out"))
            self.mk(fresh, "gql.blocksFromDepth", 100)
            select3.build({}, run2, fresh, out, fresh_full={"gql.blocksFromDepth"}, err_eps=set(), sample_per_ep=10)
            cs = [json.loads(l) for l in gzip.open(os.path.join(out, "gql.blocksFromDepth.jsonl.gz"), "rt")]
        self.assertEqual(len(cs), 100)
        flagged = [c for c in cs if c.get("sample")]
        self.assertEqual(len(flagged), 10)
        self.assertEqual([c["id"] for c in flagged][:3], ["gql.blocksFromDepth:%d" % (select3.FRESH_BASE + i) for i in (0, 10, 20)])

    def test_transaction_minimum_depth_cases_are_always_sampled(self):
        with tempfile.TemporaryDirectory() as d:
            run2, fresh, out = (os.path.join(d, x) for x in ("run2", "fresh", "out"))
            self.mk(fresh, "gql.transaction", 50, extra=lambda i: {"minimumDepth": 101} if i in (7, 33) else {})
            select3.build({}, run2, fresh, out, fresh_full={"gql.transaction"}, err_eps=set(), sample_per_ep=4)
            cs = [json.loads(l) for l in gzip.open(os.path.join(out, "gql.transaction.jsonl.gz"), "rt")]
        flagged = {c["id"] for c in cs if c.get("sample")}
        self.assertTrue({"gql.transaction:%d" % (select3.FRESH_BASE + i) for i in (7, 33)} <= flagged)

    def test_run_plan_covers_every_fix4_group(self):
        eps = select3.FRESH_FULL
        for ep in ("gql.fungibleChainAccounts", "gql.gasLimitEstimate", "gql.node", "gql.nodes", "gql.transaction",
                   "compat.v1txs", "gql.blocksFromDepth", "gql.blocksFromHeight", "gql.completedBlockHeights"):
            self.assertIn(ep, eps)
        self.assertEqual(select3.FRESH_PER_EP, 20000)
        for ep in select3.ERR_MAP_EPS:                  # error mapping: the other GraphQL endpoints
            self.assertTrue(ep.startswith("gql."))
            self.assertNotIn(ep, eps)


class SampleLane(unittest.TestCase):
    def test_sample_flag_selects_the_stock_lane(self):
        with tempfile.TemporaryDirectory() as d:
            with gzip.open(os.path.join(d, "gql.transfers.jsonl.gz"), "wt") as f:
                for i in range(6):
                    f.write(json.dumps({"id": "gql.transfers:%d" % i, "ep": "gql.transfers", "cls": "real",
                                        "req": {}, "sample": i % 3 == 0}) + "\n")
            every = [c["id"] for c in run.load_cases(d, ["gql.transfers"], 0)]
            flagged = [c["id"] for c in run.load_cases(d, ["gql.transfers"], 0, only_flag="sample")]
        self.assertEqual(len(every), 6)
        self.assertEqual(flagged, ["gql.transfers:0", "gql.transfers:3"])


class Budget(unittest.TestCase):
    def test_smoke_budget_is_cumulative_and_hard(self):
        with tempfile.TemporaryDirectory() as d:
            fn = os.path.join(d, "count")
            b = run.RequestBudget(3, fn)
            b.take()
            b.take()
            self.assertEqual(run.RequestBudget(3, fn).used, 2)       # a second invocation sees the first's count
            b2 = run.RequestBudget(3, fn)
            b2.take()
            with self.assertRaises(run.BudgetExhausted):
                b2.take()
            self.assertEqual(int(open(fn).read()), 3)               # the refused call is not counted

    def test_no_budget_means_no_limit(self):
        b = run.RequestBudget(None, None)
        for _ in range(100):
            b.take()


class Estimate(unittest.TestCase):
    def test_per_endpoint_latency_drives_the_stock_lanes(self):
        cfg = {"stock": {"oracle": [{"rps": 35, "concurrency": 64, "heavy_concurrency": 4},
                                    {"rps": 35, "concurrency": 64, "heavy_concurrency": 4}]},
               "ours": {"rps": 130}, "estimate": {"oracle_light_s": 0.6, "oracle_heavy_s": 3.0,
                                                  "ep": {"gql.nodes": 15.0}}}
        counts = {"gql.nodes": {"n": 8000, "stock": 8000}, "gql.pactQuery": {"n": 7000, "stock": 7000},
                  "gql.transfers": {"n": 20000, "stock": 600}}
        out = run.estimate_model(cfg, counts)
        # nodes is a heavy-lane endpoint: 8000 * 15 s over 2 hosts * 4 slots; transfers' sample too
        self.assertAlmostEqual(out["lane_hours"]["oracle-heavy"], (8000 * 15.0 + 600 * 3.0) / 8 / 3600, places=2)
        # ours carries every case at its rate
        self.assertAlmostEqual(out["lane_hours"]["ours"], 35000 / 130 / 3600, places=2)
        self.assertEqual(out["wall_hours"], max(out["lane_hours"].values()))

if __name__ == "__main__":
    unittest.main()
