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


if __name__ == "__main__":
    unittest.main()
