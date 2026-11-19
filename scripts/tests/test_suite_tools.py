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

    def test_ours_bugs_stay_open(self):
        self.assertEqual(self.cls("gql.fungibleChainAccounts", [E_("/data/fungibleChainAccounts", "Type", "None", "[]")])[0], "diff")
        self.assertEqual(self.cls("gql.gasLimitEstimate", [E_("/data", "Type", "None", "{'gasLimitEstimate': []}"),
                                                           E_("/errors", "Extra", "[{'code': 'NOT_SUPPORTED'}]")],
                                  ["NOT_SUPPORTED"])[0], "diff")

    def test_every_entry_has_reason(self):
        for r in allow.RULES:
            self.assertIn(r.verdict, ("DOCUMENTED", "STOCK-WRONG", "HARNESS", "KNOWN"))
            self.assertTrue(len(r.reason) > 20, r.id)


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
        polled = {"r1": {"blockHash": "B1"}, "r2": {"blockHash": "B2"}}
        self.assertEqual(N.check_tx_list("gql.transactions", {"accountName": "alice"}, body, rows, polled), [])
        body["data"]["transactions"]["edges"].append(tx("r1", "0", "bob", "B1"))
        polled2 = {"r1": {"blockHash": "B1"}, "r2": None}
        p = N.check_tx_list("gql.transactions", {"accountName": "alice"}, body, N.tx_rows("gql.transactions", body), polled2)
        for k in ("missing-on-node", "sender", "duplicate", "totalCount"):
            self.assertIn(k, p)
        self.assertEqual(N.sample_rows(list(range(40)), 10), [0, 1, 2, 3, 4, 35, 36, 37, 38, 39])


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

    def test_chain_accounts_as_set(self):
        a = {"data": {"fungibleChainAccounts": [{"chainId": "10"}, {"chainId": "2"}]}}
        b = {"data": {"fungibleChainAccounts": [{"chainId": "2"}, {"chainId": "10"}]}}
        a, b = run.normalize_pair("gql.fungibleChainAccounts", a, b)
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
