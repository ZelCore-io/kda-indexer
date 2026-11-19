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
                                                      E_("/errors", "Missing", None, "[{'code': 'INTERNAL_SERVER_ERROR'}]")])[0], "stock-unavailable")
        self.assertEqual(self.cls("gql.networkInfo", [E_("/data/networkInfo/countersOfEachChain/3/totalGasUsedInKda", "Missing", None, "1.5")],
                                  ["NOT_AVAILABLE"])[0], "allowlisted")
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
        self.assertEqual(self.cls("gql.block", [E_("/~graphql-r57-json-object", "Changed")])[0], "allowlisted")
        # owner H4: the f64 tolerance is undocumented, so open
        self.assertEqual(self.cls("gql.block", [E_("/~graphql-r57-float", "Changed")])[0], "diff")
        # H6 B1: our defect is never accepted; pair-diff classes the marker diff, so does the suite
        self.assertEqual(self.cls("gql.fungibleAccount", [E_("/~known-gap-missing-chain-rows", "Missing")])[0], "diff")

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
            self.assertEqual(sorted(x), ["accept_class", "doc", "id", "reason", "ruling"])
        self.assertEqual({x["accept_class"] for x in rows} - set(allow.ACCEPT_CLASSES), set())
        by = {x["id"]: x["accept_class"] for x in rows}
        self.assertEqual(by["pactQuery-not-supported"], "unsupported-by-design")
        self.assertEqual(by["lastBlockHeight-tip-race"], "tip-timing")
        self.assertEqual(by["networkInfo-stock-error"], "stock-unavailable")      # H6 S2
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
        self.assertEqual(N.failures(p), ["tip-race"])      # H4 B4: only a settled re-read passes
        far = [(2, 50, "o50")]
        p = N.check_block_range("gql.blocksFromHeight", {}, far, canon, {2: 100})
        self.assertEqual(p, ["not-canonical"])
        self.assertEqual(N.failures(p), ["not-canonical"])
        # both kinds: the deep one still fails
        p = N.check_block_range("gql.blocksFromHeight", {}, far + near, canon, {2: 100})
        self.assertEqual(N.failures(p), ["tip-race", "not-canonical"])

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
        order = [E_("/data/events/edges/0/node/requestKey", "Changed", "a", "b"),
                 E_("/data/events/edges/1/node/requestKey", "Changed", "b", "a")]
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
        out = run.rereferee_rows(rows, vars_of.get, lambda i: QH)
        self.assertEqual(out["before"], {"sample": 2, "results": 1})
        self.assertEqual(out["after"], {"sample": 1})
        self.assertEqual(out["total_before"], 3)
        self.assertEqual(out["total_after"], 1)
        self.assertEqual(out["moved"], {("gql.events", "multi-block-order", "allowlisted"): 2})
        self.assertEqual(out["left"], {"gql.events": 1})
        self.assertEqual(out["origin"], {"run2": [3, 1]})
        self.assertEqual(out["reopened"], {})
        md = run.rereferee_md(out, "run2")
        self.assertIn("unexplained before: 3", md)
        self.assertIn("unexplained after: 1", md)
        self.assertIn("multi-block-order", md)
        self.assertNotIn("gql.events:5", md)       # aggregate counts only: no case ids, no request contents

    def test_stored_tip_and_node_evidence_are_used(self):
        ent = shifted("blocksFromDepth", 1, 3, [6], old_h=4, field="node/hash")
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
        self.forks = {}           # block hash -> height of fork headers the node holds

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
        if "/header/" in path:
            x = path.rsplit("/", 1)[1].split("?")[0]
            if x in self.forks:
                return {"status": 200, "raw": json.dumps({"height": self.forks[x], "hash": x})}
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
        h.forks = {"orphanX": 501, "orphanY": 700}
        n = N.Node(h, "http://node", "mainnet01", rps=1000)
        rows = [{"key": ("r1", "canon500", None), "chain": 1, "height": 500, "block": "canon500"},   # canonical
                {"key": ("r2", "orphanX", None), "chain": 1, "height": 501, "block": "orphanX"},      # orphan
                {"key": ("r3", "orphanY", None), "chain": 2, "height": 700, "block": "orphanY"},      # orphan
                {"key": ("r4", "tipblk", None), "chain": 3, "height": 995, "block": "tipblk"}]        # near the tip
        ev = n.evidence(rows, max_reads=20)
        self.assertEqual((ev["stock_orphans"], ev["unresolved"], ev["extra"]), (2, 1, 4))
        self.assertLessEqual(ev["reads"], 20)
        self.assertEqual(ev["reads"], len([c for c in h.calls if "/header/" in c or c.startswith("/cut")]))

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
        h = FakeNodeHttp(tip=1000)
        h.forks = {"bad0": 500, "bad1": 501}
        n = N.Node(h, "http://node", "mainnet01", rps=1000)
        rows = [{"key": ("r%d" % i, "bad%d" % i, None), "chain": 1, "height": 500 + i, "block": "bad%d" % i}
                for i in range(2)]
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
        h = FakeNodeHttp(tip=1000)
        h.forks = {"orphan1": 500, "orphan2": 600}
        stub.node = N.Node(h, "http://node", "mainnet01", rps=1000)
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
        self.assertEqual((ev["forks_confirmed"], ev["ours_only"]), (2, 0))
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


def ED(root, pos, field, a, b, kind="Changed"):
    """One positional list entry: /data/<root>/edges/<pos>/<field>."""
    return E_("/data/%s/edges/%d/%s" % (root, pos, field), kind, a, b)


QH = "query Q { x { edges { node { requestKey height chainId } } } }"


def H4ctx(ep, ents, tip=None, vars_=None, node=None, complete=True, query=QH, idx=None):
    ctx = {"ep": ep, "cls": "real", "ours_codes": [], "entries": ents, "vars": vars_ or {}, "node": node, "tip": tip}
    if complete is not None:
        ctx["complete"] = complete
    if query is not None:
        ctx["query"] = query
    if idx is not None:
        ctx["idx"] = idx
    return ctx


def H4(*a, **k):
    return allow.classify(H4ctx(*a, **k))


def shifted(root, k, n, new_heights, old_h=50, field="node/requestKey"):
    """Stock = k new head rows (heights new_heights) then ours' first n-k rows; ours = r0..r(n-1)
    all at height old_h. Positional entries as pair-diff gives them."""
    ours = ["r%d" % i for i in range(n)]
    stock = ["n%d" % i for i in range(k)] + ours[:n - k]
    out = []
    for j in range(n):
        out.append(ED(root, j, field, ours[j], stock[j]))
        if j < k:
            out.append(ED(root, j, "node/height", old_h, new_heights[j]))
    return out


class H4Complete(unittest.TestCase):
    """B3: a row whose entries are not all the differences is never accepted by a suite rule."""

    def test_incomplete_entries_are_never_accepted(self):
        ent = [E_("/~graphql-r57-json-object", "Changed")]
        self.assertEqual(H4("gql.block", ent)[0], "allowlisted")
        self.assertEqual(H4("gql.block", ent, complete=False)[0], "diff")
        # a stored record without the flag: MAX_SHOWN entries is pair-diff's display cut
        full = [E_("/~graphql-r57-json-object", "Changed")] * allow.MAX_SHOWN
        self.assertEqual(H4("gql.block", full, complete=None)[0], "diff")
        self.assertEqual(H4("gql.block", full[:-1], complete=None)[0], "allowlisted")

    def test_verdict_class_uses_the_full_list_and_the_total(self):
        many = [E_("/~graphql-r57-json-object", "Changed")] * 12
        v = {"class": "diff", "entries": many[:8], "total": 12, "all": many}
        self.assertEqual(run.verdict_class("gql.block", "real", v, [], {}, None)[0], "allowlisted")
        # display list only (the full list was not kept): not all differences are known
        v = {"class": "diff", "entries": many[:8], "total": 12}
        self.assertEqual(run.verdict_class("gql.block", "real", v, [], {}, None)[0], "diff")
        v = {"class": "diff", "entries": many[:3], "total": 3}
        self.assertEqual(run.verdict_class("gql.block", "real", v, [], {}, None)[0], "allowlisted")

    def test_stored_verdict_keeps_display_and_full_list_apart(self):
        many = [E_("/x/%d" % i, "Changed") for i in range(12)]
        st = run.stored_verdict({"class": "diff", "ids": [], "known_ids": [], "entries": many[:8], "total": 12,
                                 "all": many}, "diff", [])
        self.assertEqual(len(st["entries"]), 8)
        self.assertEqual(st["total"], 12)
        self.assertEqual(len(st["all"]), 12)
        # H5 (c): no cap; a long list is kept whole and the record stays complete.
        huge = [E_("/x/%d" % i, "Changed") for i in range(201)]
        st = run.stored_verdict({"class": "diff", "entries": huge[:8], "total": len(huge), "all": huge}, "diff", [])
        self.assertEqual(len(st["all"]), 201)
        self.assertTrue(st["complete"])


class H4TipTiming(unittest.TestCase):
    """B1: a difference is tip timing only where every differing value is a row between the two
    observed tips, in the right direction, and the rest of the page is the same rows shifted."""

    T = {"ours": 100, "stock": 102}

    def test_new_head_rows_between_the_tips_are_timing(self):
        ents = shifted("transfers", 2, 5, [102, 101])
        self.assertEqual(H4("gql.transfers", ents, self.T, {"first": 5}), ("allowlisted", ["tip-timing"]))
        cnt = ents + [E_("/data/transfers/totalCount", "Changed", 10, 12)]
        self.assertEqual(H4("gql.transfers", cnt, self.T, {"first": 5})[0], "allowlisted")

    def test_rows_outside_the_window_stay_open(self):
        for hs in ([100, 99], [103, 101]):         # at/below ours' tip, above stock's tip
            self.assertEqual(H4("gql.transfers", shifted("transfers", 2, 5, hs), self.T, {"first": 5})[0], "diff")
        # ours read later than stock (or the tip did not move): never timing
        ents = shifted("transfers", 1, 5, [101])
        self.assertEqual(H4("gql.transfers", ents, {"ours": 102, "stock": 100}, {"first": 5})[0], "diff")
        self.assertEqual(H4("gql.transfers", ents, {"ours": 101, "stock": 101}, {"first": 5})[0], "diff")

    def test_the_count_gap_must_equal_the_new_rows(self):
        ents = shifted("transfers", 2, 5, [102, 101]) + [E_("/data/transfers/totalCount", "Changed", 10, 15)]
        self.assertEqual(H4("gql.transfers", ents, self.T, {"first": 5})[0], "diff")

    def test_a_missing_deep_row_or_a_changed_field_is_not_timing(self):
        ents = shifted("transfers", 1, 5, [101])
        # stock's row 3 is not ours' row 2: a row in the middle is missing
        bad = [e for e in ents if e["path"] != "/data/transfers/edges/3/node/requestKey"]
        bad.append(ED("transfers", 3, "node/requestKey", "r3", "zz"))
        self.assertEqual(H4("gql.transfers", bad, self.T, {"first": 5})[0], "diff")
        # a field that differs on a shared (shifted) row
        amt = ents + [ED("transfers", 2, "node/amount", "1.0", "2.0")]
        self.assertEqual(H4("gql.transfers", amt, self.T, {"first": 5})[0], "diff")
        # a whole page of new rows proves no shift: open
        self.assertEqual(H4("gql.transfers", shifted("transfers", 2, 2, [101, 102]), self.T, {"first": 2})[0], "diff")
        # an unexplained page-info change
        pi = ents + [E_("/data/transfers/pageInfo/hasNextPage", "Changed", True, False)]
        self.assertEqual(H4("gql.transfers", pi, self.T, {"first": 5})[0], "diff")

    def test_last_block_height_is_bounded_by_the_tip_gap(self):
        e = [E_("/data/lastBlockHeight", "Changed", 100, 102)]
        self.assertEqual(H4("gql.lastBlockHeight", e, self.T)[1], ["tip-timing"])
        far = [E_("/data/lastBlockHeight", "Changed", 50, 100)]
        self.assertEqual(H4("gql.lastBlockHeight", far, {"ours": 100, "stock": 101})[0], "diff")
        self.assertNotIn("tip-timing", H4("gql.lastBlockHeight", [E_("/data/lastBlockHeight", "Changed", 102, 100)],
                                          self.T)[1])

    def test_blocks_from_height_appends_and_depth_offsets(self):
        tail = [E_("/data/blocksFromHeight/edges/3", "Missing", None, {"node": {"height": 101, "hash": "h"}})]
        self.assertEqual(H4("gql.blocksFromHeight", tail, self.T, {"startHeight": 90})[1], ["tip-timing"])
        old = [E_("/data/blocksFromHeight/edges/3", "Missing", None, {"node": {"height": 99, "hash": "h"}})]
        self.assertEqual(H4("gql.blocksFromHeight", old, self.T, {"startHeight": 90})[0], "diff")
        extra = [E_("/data/blocksFromHeight/edges/3", "Extra", {"node": {"height": 101}}, None)]
        self.assertEqual(H4("gql.blocksFromHeight", extra, self.T, {"startHeight": 90})[0], "diff")
        # blocksFromDepth: the window is the two tips minus the depth
        d = shifted("blocksFromDepth", 1, 4, [92], field="node/hash")
        self.assertEqual(H4("gql.blocksFromDepth", d, self.T, {"minimumDepth": 9})[1], ["tip-timing"])
        self.assertEqual(H4("gql.blocksFromDepth", d, self.T, {"minimumDepth": 0})[0], "diff")

    def test_incomplete_or_anchored_lists_stay_open(self):
        ents = shifted("transfers", 1, 5, [101])
        self.assertEqual(H4("gql.transfers", ents, self.T, {"first": 5}, complete=False)[0], "diff")
        self.assertEqual(H4("gql.transfers", ents, self.T, {"first": 5, "maxHeight": 101})[0], "diff")


class H4Order(unittest.TestCase):
    """B2: order-only means the same rows on both pages, at the same heights."""

    OK = {"ok": True}
    V = {"first": 10}

    def swap(self, root="events"):
        return [ED(root, 0, "node/requestKey", "x", "y"), ED(root, 1, "node/requestKey", "y", "x")]

    def test_a_reorder_inside_a_height_is_order(self):
        self.assertEqual(H4("gql.events", self.swap(), node=self.OK, vars_=self.V), ("allowlisted", ["multi-block-order"]))
        self.assertEqual(H4("gql.transactions", self.swap("transactions"), node=self.OK, vars_=self.V)[1],
                         ["multi-block-order"])

    def test_a_shifted_or_foreign_page_is_not_order(self):
        shift = [ED("events", 0, "node/requestKey", "r1", "r2"), ED("events", 1, "node/requestKey", "r2", "r3")]
        self.assertEqual(H4("gql.events", shift, node=self.OK, vars_=self.V)[0], "diff")
        foreign = [ED("events", 0, "node/requestKey", "a", "p"), ED("events", 1, "node/requestKey", "b", "q")]
        self.assertEqual(H4("gql.events", foreign, node=self.OK, vars_=self.V)[0], "diff")

    def test_heights_must_agree_and_be_selected(self):
        h = self.swap() + [ED("events", 0, "node/height", 5, 6)]
        self.assertEqual(H4("gql.events", h, node=self.OK, vars_=self.V)[0], "diff")
        c = self.swap() + [ED("events", 1, "node/chainId", "1", "2")]
        self.assertEqual(H4("gql.events", c, node=self.OK, vars_=self.V)[0], "diff")
        self.assertEqual(H4("gql.events", self.swap(), node=self.OK, vars_=self.V,
                            query="query Q { events { edges { node { requestKey } } } }")[0], "diff")
        self.assertEqual(H4("gql.events", self.swap(), node=self.OK, vars_=self.V, query=None)[0], "diff")

    def test_a_moved_row_with_a_changed_field_is_not_order(self):
        e = self.swap() + [ED("events", 0, "node/amount", "1", "3"), ED("events", 1, "node/amount", "2", "1")]
        self.assertEqual(H4("gql.events", e, node=self.OK, vars_=self.V)[0], "diff")
        ok = self.swap() + [ED("events", 0, "node/amount", "1", "2"), ED("events", 1, "node/amount", "2", "1")]
        self.assertEqual(H4("gql.events", ok, node=self.OK, vars_=self.V)[0], "allowlisted")

    def test_incomplete_or_without_a_passed_node_check(self):
        self.assertEqual(H4("gql.events", self.swap(), node=self.OK, vars_=self.V, complete=False)[0], "diff")
        self.assertEqual(H4("gql.events", self.swap(), node=None, vars_=self.V)[0], "diff")
        # B4: an unsettled near-tip mismatch is a failed node check
        self.assertEqual(H4("gql.events", self.swap(), node={"ok": True, "why": ["tip-race"]}, vars_=self.V)[0], "diff")
        self.assertEqual(H4("gql.events", self.swap(), node={"ok": True, "why": ["tip-race-settled"]},
                            vars_=self.V)[0], "allowlisted")


class H4Orphans(unittest.TestCase):
    """S1: exact accounting of the rows only stock lists."""

    EP = "gql.transactionsByPublicKey"
    ENT = [E_("/data/transactionsByPublicKey/totalCount", "Changed", 1825, 1827)]

    def node(self, **k):
        n = {"ok": True, "stock_orphans": 2, "extra": 2, "unresolved": 0, "forks_confirmed": 2, "ours_only": 0}
        n.update(k)
        return n

    def test_exact_accounting(self):
        self.assertEqual(H4(self.EP, self.ENT, node=self.node()), ("allowlisted", ["totalcount-stock-orphans"]))
        for bad in ({"extra": 3}, {"unresolved": 1}, {"stock_orphans": 3, "forks_confirmed": 3}, {"ours_only": 1},
                    {"forks_confirmed": 1}, {"forks_confirmed": None}, {"ok": False}, {"why": ["tip-race"]}):
            self.assertEqual(H4(self.EP, self.ENT, node=self.node(**bad))[0], "diff", bad)

    def test_fork_headers_are_confirmed_on_the_node(self):
        h = FakeNodeHttp(tip=1000)
        h.forks = {"forkA": 500}
        n = N.Node(h, "http://node", "mainnet01", rps=1000)
        rows = [{"key": ("r1", "forkA", None), "chain": 1, "height": 500, "block": "forkA"},
                {"key": ("r2", "ghost", None), "chain": 1, "height": 501, "block": "ghost"}]
        ev = n.evidence(rows)
        self.assertEqual((ev["stock_orphans"], ev["forks_confirmed"], ev["unresolved"]), (1, 1, 1))


class H4TipRace(unittest.TestCase):
    """B4: a near-tip hash mismatch is settled by a fresh re-read, or it fails the check."""

    def test_unsettled_tip_race_fails(self):
        self.assertEqual(N.failures(["tip-race"]), ["tip-race"])
        self.assertEqual(N.failures(["tip-race-settled"]), [])
        # a chain missing from the cut is no tip race
        p = N.check_block_range("gql.blocksFromHeight", {}, [(7, 98, "o98")], {(7, 98): "n98"}, {2: 100})
        self.assertIn("no-tip", p)
        self.assertNotIn("tip-race", p)

    def body(self, h, x):
        return {"data": {"blocksFromHeight": {"edges": [blk(h, 1, x)]}}}

    def test_reread_settles_or_fails(self):
        class Reorg(FakeNodeHttp):
            def call(self, base, path="", body=None, accept=None):
                r = super().call(base, path, body, accept)
                if "/header/branch" in path and sum("/header/branch" in c for c in self.calls) > 1:
                    r = {"status": 200, "raw": r["raw"].replace("canon995", "ours995")}
                return r
        n = N.Node(Reorg(tip=1000, chains=(1,)), "http://node", "mainnet01", rps=1000, settle_s=0)
        got = n.check("gql.blocksFromHeight", {}, self.body(995, "ours995"))
        self.assertEqual((got["ok"], got["why"]), (True, ["tip-race-settled"]))
        n = N.Node(FakeNodeHttp(tip=1000, chains=(1,)), "http://node", "mainnet01", rps=1000, settle_s=0)
        got = n.check("gql.blocksFromHeight", {}, self.body(995, "ours995"))
        self.assertFalse(got["ok"])
        self.assertIn("not-canonical", got["why"])


class H4StockStale(unittest.TestCase):
    """S6: networkInfo coins lag explained only with the node tip above stock's own height."""

    E = [E_("/data/networkInfo/coinsInCirculation", "Changed", "#1010", "#1000")]

    def test_node_tip_and_stock_height(self):
        ok = H4("gql.networkInfo", self.E, tip={"ours": 100, "stock": 100}, idx={"ours": 100, "stock": 98})
        self.assertEqual(ok, ("allowlisted", ["networkInfo-coins-stale"]))
        for tip, idx in ((None, {"ours": 100, "stock": 98}), ({"ours": 100, "stock": 100}, None),
                         ({"ours": 100, "stock": 98}, {"ours": 100, "stock": 98}),       # node not above stock
                         ({"ours": 100, "stock": 100}, {"ours": 98, "stock": 98})):      # no lag
            self.assertEqual(H4("gql.networkInfo", self.E, tip=tip, idx=idx)[0], "diff", (tip, idx))
        huge = [E_("/data/networkInfo/coinsInCirculation", "Changed", "#99999", "#1000")]
        self.assertEqual(H4("gql.networkInfo", huge, tip={"ours": 100, "stock": 100}, idx={"ours": 100, "stock": 98})[0],
                         "diff")

    def test_tag_rules_carry_the_toml_class(self):
        import re as _re
        toml = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "compare", "allowlist.toml")).read()
        cls = dict(_re.findall(r'id = "([^"]+)"[^\[]*?accept_class = "([^"]+)"', toml))
        by = {x["id"]: x["accept_class"] for x in allow.table()}
        self.assertNotIn("accepted-tags", by)
        for tag in allow.TAGS_ALLOWED:
            self.assertIn(tag, cls, tag)
            self.assertEqual(by["tag:" + tag], cls[tag], tag)
        self.assertEqual(by["known-gap-missing-chain-rows"], "our-defect")
        self.assertEqual(by["networkInfo-coins-stale"], "stock-wrong-node-evidence")
        self.assertFalse(hasattr(allow, "TIP_SAMPLE_AGE") or hasattr(allow, "tip_before"))   # M3


class H4Rereferee(unittest.TestCase):
    """Rows a suite rule accepted before are judged again: a stricter rule re-opens them."""

    def test_accepted_rows_are_rejudged(self):
        old = [E_("/data/blocksFromDepth/edges/1/node/hash", "Changed", "a", "b")]
        r = {"id": "gql.blocksFromDepth:1", "ep": "gql.blocksFromDepth", "cls": "real", "ours": {"st": 200},
             "stock": {"st": 200}, "tip": {"ours": 5, "stock": 6},
             "v": {"class": "allowlisted", "ids": ["tip-timing"], "known": [], "entries": old}}
        keep = {"id": "gql.block:2", "ep": "gql.block", "cls": "real", "ours": {"st": 200}, "stock": {"st": 200},
                "v": {"class": "allowlisted", "ids": ["graphql-r57-json-object"], "known": [],
                      "entries": [E_("/x", "Changed")]}}
        out = run.rereferee_rows([("sample", r), ("results", keep)], lambda i: {})
        self.assertEqual(out["reopened"], {("gql.blocksFromDepth", "tip-timing"): 1})
        self.assertEqual(out["total_after"], 1)


class H4Select(unittest.TestCase):
    def test_failed_node_checks_are_selected(self):
        rows = [("results", {"id": "gql.blocksFromDepth:4", "ep": "gql.blocksFromDepth", "node": {"ok": False}}),
                ("results", {"id": "compat.account:3", "ep": "compat.account", "v": {"class": "equal"},
                             "node": {"ok": False, "why": ["duplicate"]}}),
                ("results", {"id": "gql.blocksFromDepth:5", "ep": "gql.blocksFromDepth", "node": {"ok": True}})]
        got = select3.pick(rows, heavy={"gql.blocksFromDepth"})
        self.assertEqual(got["gql.blocksFromDepth"], [("gql.blocksFromDepth:4", "node-fail", False)])
        self.assertEqual(got["compat.account"], [("compat.account:3", "node-fail", False)])

    def test_a_picked_id_missing_from_the_cases_fails(self):
        with tempfile.TemporaryDirectory() as d:
            run2, fresh, out = (os.path.join(d, x) for x in ("run2", "fresh", "out"))
            Select3.mk(None, run2, "gql.node", 2)
            with self.assertRaises(ValueError):
                select3.build({"gql.node": [("gql.node:9", "diff", False)]}, run2, fresh, out, fresh_full=set(),
                              err_eps=set())

    def test_every_minimum_depth_case_goes_to_stock(self):
        old = select3.FRESH_PER_EP
        select3.FRESH_PER_EP = 10
        try:
            with tempfile.TemporaryDirectory() as d:
                run2, fresh, out = (os.path.join(d, x) for x in ("run2", "fresh", "out"))
                Select3.mk(None, fresh, "gql.transaction", 50, extra=lambda i: {"minimumDepth": 101} if i % 2 else {})
                select3.build({}, run2, fresh, out, fresh_full={"gql.transaction"}, err_eps=set(), sample_per_ep=2)
                cs = [json.loads(l) for l in gzip.open(os.path.join(out, "gql.transaction.jsonl.gz"), "rt")]
        finally:
            select3.FRESH_PER_EP = old
        deep = {c["id"] for c in cs if c["req"]["variables"].get("minimumDepth") is not None}
        self.assertEqual(len(deep), 25)                       # none dropped by the per-endpoint cap
        self.assertTrue(deep <= {c["id"] for c in cs if c.get("sample")})


class H4Minor(unittest.TestCase):
    def test_self_transfer_copies_must_be_adjacent(self):
        def row(rk, frm, to, order=3):
            return {"node": {"requestKey": rk, "chainId": "14", "blockHash": "B", "orderIndex": order,
                             "fromAccount": frm, "toAccount": to}}
        s, o = row("r1", "alice", "alice"), row("r9", "alice", "bob", 1)
        body = {"data": {"transfers": {"edges": [s, o, s]}}}
        p = N.check_tx_list("gql.transfers", {"accountName": "alice"}, body, N.tx_rows("gql.transfers", body), {})
        self.assertIn("duplicate", p)

    def test_one_read_in_flight_per_host(self):
        f = run.OnePerKey()
        self.assertTrue(f.start("h"))
        self.assertFalse(f.start("h"))
        self.assertTrue(f.start("ours"))
        f.done("h")
        self.assertTrue(f.start("h"))

    def test_run3_scripts_name_no_host_layout(self):
        d = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "suite", "run3")
        for fn in os.listdir(d):
            t = open(os.path.join(d, fn)).read()
            for bad in ("/root/", "kda-indexer-mainnet01", "kdx-fv"):
                self.assertNotIn(bad, t, fn)

    def test_run4_scripts_use_run4_paths_and_34_heavy_hours(self):
        """H5 (d): run 4 runs tools4 on cases4 into run4, its config inside run4/, stock heavy
        lanes 34 h by default; no host layout and nothing of run 3's."""
        d = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "suite", "run4")
        names = sorted(os.listdir(d))
        self.assertEqual(names, ["build4.sh", "check.sh", "guard.sh", "kdx-suite4-guard.service", "kdx-suite4.service",
                                 "launch.sh", "stage4.sh", "start4.sh", "verify4.sh"])
        for fn in names:
            t = open(os.path.join(d, fn)).read()
            for bad in ("/root/", "kda-indexer-mainnet01", "kdx-fv", "run3", "tools3", "cases3", "suite3",
                        "pkill", "pgrep"):
                self.assertNotIn(bad, t, fn)
        launch = open(os.path.join(d, "launch.sh")).read()
        self.assertIn("STOCK_HEAVY_HOURS:-34", launch)
        self.assertIn("tools4/run.py run --config run4/suite4.toml --cases cases4 --out run4", launch)
        self.assertIn('[ -x "$T/kda-compare-h5" ]', open(os.path.join(d, "verify4.sh")).read())     # H6 S8



def _slugs(path):
    out = set()
    for l in open(path):
        if l.startswith("#"):
            h = l.lstrip("#").strip().lower()
            out.add("".join(c for c in h if c.isalnum() or c in " -_").replace(" ", "-"))
    return out


class H4OwnerRules(unittest.TestCase):
    """Owner H4 rules: documented differences only, exact numbers, every row accounted for."""

    ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")

    def test_every_suite_rule_has_a_valid_doc_anchor(self):
        for r in allow.RULES:
            if r.id == "known-gap-missing-chain-rows":
                self.assertIsNone(r.doc)            # our defect: a known gap, never accepted
                continue
            self.assertIsNotNone(r.doc, r.id)
            f, a = r.doc.split("#")
            self.assertTrue(f.startswith("docs/") and a in _slugs(os.path.join(self.ROOT, f)), r.doc)

    def test_numbers_are_exact_decimals(self):
        self.assertEqual(allow._num("#1.00000000000000001"), allow._num("1.00000000000000001"))
        self.assertNotEqual(allow._num("1.00000000000000001"), allow._num("1"))
        self.assertIsNone(allow._num(True))

    def test_a_partial_comparison_is_incomplete(self):
        v = {"class": "allowlisted", "entries": [E_("/~graphql-page-cut", "Changed")], "total": 1}
        self.assertEqual(run.verdict_class("gql.transfers", "real", v, [], {}, None)[0], "incomplete")
        self.assertEqual(run.verdict_class("gql.transfers", "real", {"class": "equal", "entries": []}, [], {}, None)[0],
                         "equal")
        st = run.stored_verdict({"class": "diff", "entries": [E_("/x", "Changed")], "total": 1,
                                 "all": [E_("/x", "Changed")]}, "diff", [])
        self.assertIs(st["complete"], True)

    def test_an_open_entry_accepts_nothing(self):
        v = {"class": "allowlisted", "ids": ["oracle-unstable"], "open_ids": ["oracle-unstable"],
             "entries": [E_("/0/x", "Changed")], "total": 1}
        self.assertEqual(run.verdict_class("compat.v1txs", "real", v, [], {}, None)[0], "diff")

    def test_rows_are_counted(self):
        self.assertEqual(run.count_rows({"data": {"transfers": {"edges": [{}, {}, {}]}}}), 3)
        self.assertEqual(run.count_rows([1, 2]), 2)
        self.assertEqual(run.count_rows({"data": {"block": {}}}), 1)


class H5Rereferee(unittest.TestCase):
    """H5 (c): a stock-paired record that is not equal keeps everything a later rule change needs
    to re-judge it offline: the full diff list (no cap), row counts per side, the completeness
    flag, the tip, settle and fork-header evidence, and both response bodies."""

    def test_full_list_is_kept_without_a_cap(self):
        ents = [{"path": "/x/%d" % i, "kind": "changed", "a": i, "b": i + 1} for i in range(450)]
        v = {"class": "diff", "entries": ents[:8], "total": 450, "all": ents}
        out = run.stored_verdict(v, "diff", [])
        self.assertEqual(len(out["all"]), 450)
        self.assertTrue(out["complete"])
        self.assertEqual(run.verdict_class("gql.x", "valid", v, [], {}, None)[0], "diff")

    def test_side_counts(self):
        ours = {"data": {"transactions": {"totalCount": 7, "edges": [{}, {}, {}]}}}
        stock = {"data": {"transactions": {"totalCount": 9, "edges": [{}, {}]}}}
        c = run.side_counts(ours, stock, None)
        self.assertEqual(c, {"ours": {"rows": 3, "totalCount": 7, "pages": 1},
                             "stock": {"rows": 2, "totalCount": 9, "pages": 1}})
        c = run.side_counts([1, 2], [1], {"ev_pages": [3, 4]})
        self.assertEqual(c["ours"], {"rows": 2, "totalCount": None, "pages": 1, "evidence_pages": 3})
        self.assertEqual(c["stock"]["evidence_pages"], 4)

    def test_bodies_round_trip(self):
        a, s = '{"data":{"x":1}}', '{"data":{"x":"2"}}' * 50
        b = run.pack_bodies(a, s)
        self.assertEqual(b["enc"], "zlib+base64")
        self.assertEqual(run.unpack_bodies(b), (a, s))
        self.assertEqual(run.unpack_bodies(run.pack_bodies(None, "")), ("", ""))

    def test_keep_evidence_only_for_non_equal_paired_records(self):
        row = {"v": {"class": "equal"}, "rows": [1, 1]}
        run.keep_evidence(row, "a", "b", {}, {}, None)
        self.assertNotIn("bodies", row)
        row = {"v": {"class": "diff", "complete": True, "all": []}, "tip": {"ours": 1, "stock": 2},
               "node": {"settle": {"near": []}, "forks": []}}
        run.keep_evidence(row, '{"a":1}', '{"a":2}', {"a": 1}, {"a": 2}, None)
        self.assertEqual(run.unpack_bodies(row["bodies"]), ('{"a":1}', '{"a":2}'))
        self.assertEqual(row["counts"]["ours"]["rows"], 1)
        self.assertEqual(run.missing_evidence(row), [])
        bare = {"v": {"class": "diff"}, "bodies": row["bodies"], "counts": row["counts"]}
        self.assertEqual(run.missing_evidence(bare), ["complete", "all", "tip"])

    def test_settle_and_fork_reads_are_recorded(self):
        n = N.Node.__new__(N.Node)
        n.settle_s = 0
        n.canonical = lambda c, lo, hi, fresh=False: {(c, h): "B%d" % h for h in range(lo, hi + 1)}
        res, ev = n.settle_race([(1, 10, "X")], {(1, 10): "A"})
        self.assertEqual(res, "tip-race-settled")
        self.assertEqual(ev, {"near": [[1, 10, "X"]], "first": [[1, 10, "A"]],
                              "second": [[1, 10, "B10"]], "wait_s": 0, "result": "tip-race-settled"})
        self.assertEqual(N.settle([(1, 10, "X")], {(1, 10): "A"}, {(1, 10): "B10"}), "tip-race-settled")

    def test_disk_estimate(self):
        est = run.disk_estimate([{"v": {"class": "diff"}, "ours": {"b": 1000}, "stock": {"b": 3000}},
                                 {"v": {"class": "equal"}, "ours": {"b": 99}}], ratio=0.25)
        self.assertEqual(est["non_equal"], 1)
        self.assertEqual(est["body_bytes"], int(4000 * 0.25 * 4 / 3) + 1)



class H6Fixes(unittest.TestCase):
    """H6 (R-P5P9-172): the suite-h5-review findings, each with the case that failed before."""

    ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
    GAP = E_("/~known-gap-missing-chain-rows", "Missing")

    def test_b1_our_defect_never_makes_a_row_allowlisted(self):
        tip = {"ours": 100, "stock": 101}
        mixed = [self.GAP, E_("/data/lastBlockHeight", "Changed", 100, 101)]
        self.assertEqual(H4("gql.lastBlockHeight", mixed, tip=tip), ("diff", []))
        self.assertEqual(H4("gql.fungibleAccount", [self.GAP]), ("diff", []))
        # the other rule alone still accepts its entry
        self.assertEqual(H4("gql.lastBlockHeight", mixed[1:], tip=tip)[0], "allowlisted")

    def test_b2_settle_evidence_is_per_call_across_threads(self):
        import threading
        n = N.Node.__new__(N.Node)
        n.settle_s = 0
        gate = threading.Barrier(2)

        def canonical(c, lo, hi, fresh=False):
            gate.wait(timeout=5)            # both calls are in flight before either returns
            return {(c, h): "C%d-%d" % (c, h) for h in range(lo, hi + 1)}
        n.canonical = canonical
        got = {}

        def go(c):
            got[c] = n.settle_race([(c, 10 * c, "X%d" % c)], {(c, 10 * c): "F%d" % c})
        ts = [threading.Thread(target=go, args=(c,)) for c in (1, 2)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(10)
        for c in (1, 2):
            res, ev = got[c]
            self.assertEqual(ev["near"], [[c, 10 * c, "X%d" % c]])
            self.assertEqual(ev["first"], [[c, 10 * c, "F%d" % c]])
            self.assertEqual(ev["second"], [[c, 10 * c, "C%d-%d" % (c, 10 * c)]])
        self.assertFalse(hasattr(n, "last_settle"))

    def test_s1_rules_judge_the_left_list_and_all_keeps_every_entry(self):
        explained = E_("/result/error/callStack", "Missing", None, "[]")
        real = E_("/data/x", "Changed", 1, 2)
        v = {"class": "diff", "entries": [real], "total": 2, "left_total": 1, "all": [explained, real], "left": [real]}
        self.assertEqual(run.rule_entries(v), ([real], True))
        out = run.stored_verdict(v, "diff", [])
        self.assertEqual((out["all"], out["left"], out["total"], out["left_total"]), ([explained, real], [real], 2, 1))
        self.assertTrue(out["complete"])
        # without the lists: the display list, complete only when nothing of `left` was cut
        self.assertEqual(run.rule_entries({"entries": [real], "total": 9, "left_total": 1}), ([real], True))
        self.assertEqual(run.rule_entries({"entries": [real], "total": 9, "left_total": 2}), ([real], False))
        # an H5 record (`all` was the left list) reads as it was written
        self.assertEqual(run.rule_entries({"entries": [real], "total": 1, "all": [real]}), ([real], True))

    def test_s2_stock_errors_are_stock_unavailable_never_allowlisted(self):
        ise = [E_("/data/networkInfo", "Type", "{}", "None"),
               E_("/errors", "Missing", None, "[{'code': 'INTERNAL_SERVER_ERROR'}]")]
        self.assertEqual(H4("gql.networkInfo", ise)[0], "stock-unavailable")
        td = [E_("/data/networkInfo/totalDifficulty", "Changed", "#5e3", "-1")]
        self.assertEqual(H4("gql.networkInfo", td), ("stock-unavailable", ["networkInfo-totalDifficulty"]))
        # an entry no rule explains beside it keeps the row a diff
        self.assertEqual(H4("gql.networkInfo", td + [E_("/data/networkInfo/networkId", "Changed", "a", "b")])[0], "diff")
        by = {x["id"]: x["accept_class"] for x in allow.table()}
        for i in ("networkInfo-stock-error", "networkInfo-totalDifficulty"):
            self.assertEqual(by[i], "stock-unavailable")

    def test_s2_tip_race_needs_the_recorded_node_tip(self):
        e = [E_("/data/lastBlockHeight", "Changed", 102, 100)]          # ours ahead by 2
        self.assertEqual(H4("gql.lastBlockHeight", e)[0], "diff")       # no tip recorded
        self.assertEqual(H4("gql.lastBlockHeight", e, tip={"ours": 100, "stock": 100})[0], "diff")
        self.assertEqual(H4("gql.lastBlockHeight", e, tip={"ours": 102, "stock": 101})[0], "diff")   # moved 1 < 2
        self.assertEqual(H4("gql.lastBlockHeight", e, tip={"ours": 100, "stock": 103})[0], "diff")   # wrong way
        self.assertEqual(H4("gql.lastBlockHeight", e, tip={"ours": 103, "stock": 101}),
                         ("allowlisted", ["lastBlockHeight-tip-race"]))

    def test_s9_report_and_check_count_records_without_evidence(self):
        d = tempfile.mkdtemp()
        ok = {"id": "a", "ep": "gql.block", "ours": {"st": 200, "ms": 1}, "stock": {"st": 200, "ms": 1, "host": "h"},
              "rows": [1, 1], "tip": {"ours": 1, "stock": 1}, "counts": {}, "bodies": {},
              "v": {"class": "diff", "complete": True, "all": []}}
        bare = dict(ok, id="b", v={"class": "diff"})
        del bare["tip"]
        eq = dict(ok, id="c", v={"class": "equal"})
        del eq["bodies"]
        with open(os.path.join(d, "results.jsonl"), "w") as f:
            for r in (ok, bare, eq):
                f.write(json.dumps(r) + "\n")
        g = run.evidence_gaps(d)
        self.assertEqual((g["checked"], g["records"], g["tip"], g["all"], g["complete"]), (2, 1, 1, 1, 1))
        run.report(d)
        self.assertIn("lacking evidence: 1", open(os.path.join(d, "report.md")).read())
        self.assertEqual(json.load(open(os.path.join(d, "summary.json")))["evidence_gaps"]["records"], 1)
        chk = open(os.path.join(self.ROOT, "scripts", "suite", "run4", "check.sh")).read()
        self.assertIn("tools4/run.py evidence-gaps --out $S/run4", chk)

    def test_s8_start4_checks_binary_hash_and_tools_commit(self):
        import shutil
        import subprocess
        r4 = os.path.join(self.ROOT, "scripts", "suite", "run4")
        st, la, sg = (open(os.path.join(r4, f)).read() for f in ("start4.sh", "launch.sh", "stage4.sh"))
        self.assertIn('/bin/bash "$S/run4/verify4.sh"', st)
        self.assertIn("run4/verify4.sh", la.split("python3")[0])            # before the run starts
        self.assertIn("cargo build --release -p kdx-compare --bin kda-compare", st)
        self.assertIn("SHA256SUMS", sg)
        if shutil.which("sha256sum") is None:
            self.skipTest("no sha256sum")
        C = "c" * 40
        d = tempfile.mkdtemp()
        os.makedirs(os.path.join(d, "tools4"))
        os.makedirs(os.path.join(d, "run4"))
        v = open(os.path.join(r4, "verify4.sh")).read()
        self.assertIn('EXPECTED_COMMIT="@COMMIT@"', v)
        open(os.path.join(d, "run4", "suite4.toml"), "w").write(
            '[diff]\nkda_compare = "tools4/kda-compare-h5"\nconfig = "run4/fv.toml"\n')
        open(os.path.join(d, "run4", "fv.toml"), "w").write('allowlist = "tools4/allowlist.toml"\n[node]\n')
        t = os.path.join(d, "tools4")

        def w(name, text):
            open(os.path.join(t, name), "w").write(text)

        def verify(staged=True):
            open(os.path.join(d, "run4", "verify4.sh"), "w").write(v.replace("@COMMIT@", C) if staged else v)
            return subprocess.run(["/bin/bash", os.path.join(d, "run4", "verify4.sh")], env=dict(os.environ, SUITE_DIR=d),
                                  capture_output=True, text=True).returncode
        w("run.py", "x")
        w("COMMIT", C + "\n")
        subprocess.run("sha256sum run.py COMMIT > SHA256SUMS", shell=True, cwd=t, check=True)
        w("kda-compare-h5", "bin")
        os.chmod(os.path.join(t, "kda-compare-h5"), 0o755)
        subprocess.run("sha256sum kda-compare-h5 > kda-compare-h5.sha256", shell=True, cwd=t, check=True)
        w("kda-compare-h5.commit", C + "\n")
        self.assertEqual(verify(), 0)
        self.assertNotEqual(verify(staged=False), 0)                       # not staged: no commit
        w("kda-compare-h5.commit", "d" * 40 + "\n")
        self.assertNotEqual(verify(), 0)                                   # binary from another commit
        w("kda-compare-h5.commit", C + "\n")
        w("kda-compare-h5", "other bin")
        self.assertNotEqual(verify(), 0)                                   # binary replaced
        subprocess.run("sha256sum kda-compare-h5 > kda-compare-h5.sha256", shell=True, cwd=t, check=True)
        self.assertEqual(verify(), 0)
        w("run.py", "y")
        self.assertNotEqual(verify(), 0)                                   # tools4 file from another commit


class H7Fixes(unittest.TestCase):
    """H7 (R-P5P9-173): suite-h6-rereview SHOULD-1 and SHOULD-4 (S8 part)."""

    ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
    R4 = os.path.join(ROOT, "scripts", "suite", "run4")

    def test_should1_partial_hit_the_rust_allowlist_explained_is_still_seen(self):
        cap = E_("/~graphql-totalcount-cap", "Changed", 1, 2)
        lbh = E_("/data/lastBlockHeight", "Changed", 100, 101)
        tip = {"ours": 5, "stock": 6}
        v = {"class": "diff", "left": [lbh], "left_total": 1, "all": [cap, lbh], "total": 2}
        self.assertEqual(run.verdict_class("gql.lastBlockHeight", "real", v, [], {}, None, tip=tip)[0], "incomplete")
        self.assertEqual(run.verdict_class("gql.lastBlockHeight", "real", dict(v, **{"class": "allowlisted"}), [], {},
                                           None, tip=tip)[0], "incomplete")
        # without the cap the same record is accepted by the tip rule, and a real diff stays a diff
        self.assertEqual(run.verdict_class("gql.lastBlockHeight", "real", dict(v, all=[lbh], total=1), [], {}, None,
                                           tip=tip)[0], "allowlisted")
        real = E_("/data/x", "Changed", 1, 2)
        self.assertEqual(run.verdict_class("gql.x", "real", {"class": "diff", "left": [real], "all": [cap, real]},
                                           [], {}, None)[0], "diff")
        # the oracle stall the Rust allowlist explained is seen too
        stall = E_("/~graphql-oracle-stall", "Changed", 1, 2)
        self.assertEqual(run.verdict_class("gql.x", "real", {"class": "diff", "left": [real], "all": [stall, real]},
                                           [], {}, None)[0], "stock-unavailable")

    def _suite(self, fv_allowlist='allowlist = "tools4/allowlist.toml"\n', rules='[{ field = "cmd.nonce", rule = "json_unquote" }]'):
        import subprocess
        C = "c" * 40
        d = tempfile.mkdtemp()
        t = os.path.join(d, "tools4")
        os.makedirs(t)
        os.makedirs(os.path.join(d, "run4"))
        open(os.path.join(d, "run4", "suite4.toml"), "w").write(
            '[diff]\nkda_compare = "tools4/kda-compare-h5"\nconfig = "run4/fv-compare.toml"\n')
        self._fv(d, fv_allowlist, rules)
        for n, x in (("run.py", "x"), ("allowlist.toml", "a"), ("COMMIT", C + "\n"), ("kda-compare-h5", "bin"),
                     ("kda-compare-h5.commit", C + "\n")):
            open(os.path.join(t, n), "w").write(x)
        os.chmod(os.path.join(t, "kda-compare-h5"), 0o755)
        subprocess.run("sha256sum run.py allowlist.toml COMMIT > SHA256SUMS && sha256sum kda-compare-h5 > kda-compare-h5.sha256",
                       shell=True, cwd=t, check=True)
        open(os.path.join(d, "run4", "verify4.sh"), "w").write(
            open(os.path.join(self.R4, "verify4.sh")).read().replace("@COMMIT@", C))
        return d

    def _fv(self, d, fv_allowlist, rules):
        open(os.path.join(d, "run4", "fv-compare.toml"), "w").write(
            fv_allowlist + "[graphql]\nfield_rules = %s\n[node]\n" % rules)

    def _verify(self, d):
        import subprocess
        return subprocess.run(["/bin/bash", os.path.join(d, "run4", "verify4.sh")], env=dict(os.environ, SUITE_DIR=d),
                              capture_output=True, text=True)

    def test_should4c_verify4_needs_the_tools4_allowlist_and_pins_field_rules(self):
        import shutil
        if shutil.which("sha256sum") is None:
            self.skipTest("no sha256sum")
        d = self._suite()
        r = self._verify(d)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("field_rules", r.stdout)
        self.assertEqual(self._verify(d).returncode, 0)                      # pinned, unchanged
        self._fv(d, 'allowlist = "tools4/allowlist.toml"\n', "[]")
        self.assertNotEqual(self._verify(d).returncode, 0)                   # field_rules changed after the pin
        for fv in ('', 'allowlist = "compare/allowlist.toml"\n', 'allowlist = "/elsewhere/allowlist.toml"\n'):
            d = self._suite(fv_allowlist=fv)
            r = self._verify(d)
            self.assertNotEqual(r.returncode, 0, fv)                         # pair-diff would load another allowlist
            self.assertIn("allowlist", r.stderr)
        d = self._suite()
        open(os.path.join(d, "run4", "suite4.toml"), "w").write('[diff]\nkda_compare = "tools4/kda-compare-h5"\n')
        self.assertNotEqual(self._verify(d).returncode, 0)                   # no pair-diff config named

    def test_should4b_build4_builds_from_a_clean_checkout_and_stamps_head(self):
        import shutil
        import subprocess
        if shutil.which("sha256sum") is None or shutil.which("git") is None:
            self.skipTest("no sha256sum or git")
        b4 = os.path.join(self.R4, "build4.sh")
        self.assertIn("nice -n 10", open(b4).read())
        self.assertIn("build --release -p kdx-compare --bin kda-compare", open(b4).read())
        repo = tempfile.mkdtemp()
        g = lambda *a: subprocess.run(["git", "-C", repo] + list(a), check=True, capture_output=True, text=True).stdout.strip()
        g("init", "-q")
        g("config", "user.email", "t@example.invalid")
        g("config", "user.name", "t")
        open(os.path.join(repo, "src.txt"), "w").write("one\n")
        g("add", "src.txt")
        g("commit", "-qm", "one")
        c1 = g("rev-parse", "HEAD")
        open(os.path.join(repo, "src.txt"), "w").write("two\n")
        g("commit", "-qam", "two")
        open(os.path.join(repo, "src.txt"), "w").write("dirty\n")          # uncommitted change in the copy
        fake = os.path.join(tempfile.mkdtemp(), "cargo")
        open(fake, "w").write('#!/bin/bash\nmkdir -p target/release && cp src.txt target/release/kda-compare\n')
        os.chmod(fake, 0o755)
        s = tempfile.mkdtemp()
        os.makedirs(os.path.join(s, "tools4"))
        open(os.path.join(s, "tools4", "COMMIT"), "w").write(c1 + "\n")
        env = dict(os.environ, REPO=repo, CARGO=fake)
        env.pop("CARGO_TARGET_DIR", None)
        r = subprocess.run(["/bin/bash", b4, c1[:12], s], env=env, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        t = os.path.join(s, "tools4")
        self.assertEqual(open(os.path.join(t, "kda-compare-h5")).read(), "one\n")    # the commit, not the tree
        self.assertEqual(open(os.path.join(t, "kda-compare-h5.commit")).read().strip(), c1)
        self.assertEqual(subprocess.run("sha256sum -c --quiet kda-compare-h5.sha256", shell=True, cwd=t).returncode, 0)
        self.assertEqual(g("worktree", "list").count("\n"), 0)                # the checkout is removed
        r = subprocess.run(["/bin/bash", b4, "HEAD", s], env=env, capture_output=True, text=True)
        self.assertNotEqual(r.returncode, 0)                                   # not the staged tools4/COMMIT
        st, sg = (open(os.path.join(self.R4, f)).read() for f in ("start4.sh", "stage4.sh"))
        self.assertIn("build4.sh", sg)
        self.assertIn("run4/build4.sh", open(os.path.join(self.R4, "verify4.sh")).read())


if __name__ == "__main__":
    unittest.main()
