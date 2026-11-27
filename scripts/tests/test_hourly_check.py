"""Tests for scripts/hourly_check.py (stdlib unittest, no network).

Run: python3 -m unittest discover -s scripts/tests
"""

import json
import os
import stat
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import hourly_check as H  # noqa: E402

T0 = 1_795_780_800_000  # 2026-11-27T12:00:00Z


def rest(uri, route, t=T0 + 1000, status=200, method="GET", **kw):
    return dict(t=t, listener="compat", method=method, uri=uri, route=route, status=status, ms=3.0, **kw)


def gql(query, variables=None, op=None, t=T0 + 1000, **kw):
    g = {"query": query}
    if variables is not None:
        g["variables"] = variables
    if op:
        g["operationName"] = op
    return dict(t=t, listener="graphql", method="POST", uri="/graphql", route="/graphql",
                status=200, ms=5.0, gql=g, **kw)


def verdict_line(i, *classes, skip=None):
    return {"id": str(i), "skip": skip, "outcomes": [{"class": c, "allowlist": []} for c in classes]}


class WindowAndFiles(unittest.TestCase):
    def test_hour_window(self):
        s, e, label = H.hour_window("2026-11-27T12")
        self.assertEqual((s, e, label), (T0, T0 + 3_600_000, "2026-11-27T12"))

    def test_rotations_oldest_first_and_window_filter(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "r.jsonl")
            for name, t in ((p + ".2", T0 - 10), (p + ".1", T0 + 5), (p, T0 + 3_600_000)):
                with open(name, "w") as f:
                    f.write(json.dumps(rest("/stats", "/stats", t=t)) + "\nnot json\n")
            self.assertEqual(H.log_files(p), [p + ".2", p + ".1", p])
            c = H.collections.Counter()
            got = list(H.read_window(H.log_files(p), T0, T0 + 3_600_000, c))
            self.assertEqual([e["t"] for e in got], [T0 + 5])
            self.assertEqual(c["bad-line"], 3)


class Shapes(unittest.TestCase):
    def test_rest_shape_is_route_and_param_names(self):
        a = H.route_of(rest("/txs/account/k:a?limit=5&offset=0", "/txs/account/{account}"))
        b = H.route_of(rest("/txs/account/k:b?offset=10&limit=50", "/txs/account/{account}"))
        self.assertEqual(a[1], b[1])
        self.assertEqual(a[2], {"path": "/txs/account/k:a?limit=5&offset=0"})
        self.assertEqual(H.route_of(rest("/txs/recent", "/txs/recent", method="POST"))[2], "method")

    def test_graphql_shape_and_skips(self):
        q = "query Q($h: Int!) { block(height: $h) { hash } }"
        a = H.route_of(gql(q, {"h": 1}, "Q"))
        b = H.route_of(gql("query  Q($h: Int!) {\n block(height: $h) { hash } }", {"h": 2}, "Q"))
        self.assertEqual(a[0], "graphql:block")
        self.assertEqual(a[1], b[1], "whitespace does not make a new shape")
        self.assertEqual(a[2], {"query": q, "variables": {"h": 1}})
        cut = gql(q)
        cut.pop("gql")
        cut["gql_cut"] = True
        self.assertEqual(H.route_of(cut)[2], "body-cut")
        batch = gql(q)
        batch["gql"] = [{"query": q}]
        self.assertEqual(H.route_of(batch)[2], "batch")
        get = dict(t=T0, listener="graphql", method="GET", route="/graphql", status=200, ms=1,
                   uri="/graphql?query=%7B%20lastBlockHeight%20%7D")
        self.assertEqual(H.route_of(get)[2], {"query": "{ lastBlockHeight }", "variables": {}})


class Sampling(unittest.TestCase):
    def test_every_shape_then_per_route_cap_and_budget(self):
        es = [rest("/txs/account/k:%d?limit=5" % i, "/txs/account/{account}") for i in range(50)]
        es += [rest("/txs/account/k:x?minheight=%d" % i, "/txs/account/{account}") for i in range(3)]
        es += [rest("/stats", "/stats")] * 10
        es += [gql("{ lastBlockHeight }")]
        items, routes, _ = H.sample(es, budget=100, per_route=10, seed="h")
        by = H.collections.Counter(it["route"] for it in items)
        self.assertEqual(by["compat:/txs/account/{account}"], 10)
        self.assertEqual(by["compat:/stats"], 1, "identical requests once")
        self.assertEqual(by["graphql:lastBlockHeight"], 1)
        self.assertTrue(any("minheight" in it.get("path", "") for it in items), "rare shape kept")
        self.assertEqual(routes["compat:/txs/account/{account}"], 53)
        small, _, _ = H.sample(es, budget=3, per_route=10, seed="h")
        self.assertEqual(len(small), 3)
        self.assertEqual(len({it["id"] for it in items}), len(items))
        again, _, _ = H.sample(es, budget=100, per_route=10, seed="h")
        self.assertEqual(items, again, "same hour, same sample")


class Classes(unittest.TestCase):
    def test_mapping_worst_wins_and_unknown_fails_closed(self):
        self.assertEqual(H.item_class(verdict_line(1, "match", "allowlisted")), "allowlisted")
        self.assertEqual(H.item_class(verdict_line(1, "match", "skew")), "timing")
        self.assertEqual(H.item_class(verdict_line(1, "match", "diff")), "unexplained")
        self.assertEqual(H.item_class(verdict_line(1, "legacy-cmd")), "unexplained")
        self.assertEqual(H.item_class(verdict_line(1, "something-new")), "unexplained")
        self.assertEqual(H.item_class(verdict_line(1, "b-error")), "stock-unavailable")
        self.assertEqual(H.item_class(verdict_line(1, "a-error", "diff")), "ours-error")
        self.assertEqual(H.item_class(verdict_line(1, skip="tip-only")), "not-compared")
        self.assertEqual(H.item_class(verdict_line(1, "known-gap", "match")), "known-gap")

    def test_signature_ignores_numbers_in_paths(self):
        a = {"outcomes": [{"class": "diff", "record": {"diff": [{"path": "/rows/3/amount", "kind": "Changed"}]}}]}
        b = {"outcomes": [{"class": "diff", "record": {"diff": [{"path": "/rows/7/amount", "kind": "Changed"}]}}]}
        self.assertEqual(H.signature("r", a), H.signature("r", b))


class Metrics(unittest.TestCase):
    def test_parse_and_facts(self):
        text = "\n".join([
            "# TYPE kdx_http_requests counter",
            'kdx_http_requests_total{api="compat",route="/stats",status="200"} 10',
            'kdx_http_requests_total{api="compat",route="/stats",status="503"} 2',
            'kdx_http_requests_total{api="native",route="/status",status="500"} 9',
            'kdx_chain_halted{chain="3"} 1',
            'kdx_chain_halted{chain="4"} 0',
            'kdx_chain_lag_blocks{chain="0"} 2',
            'kdx_balance_dirty_rows 1234',
            'kdx_request_log_lines_total{result="dropped"} 5',
        ])
        f = H.metrics_facts(H.parse_metrics(text))
        self.assertEqual(f["http_5xx"], {"compat /stats": 2})
        self.assertEqual(f["halted"], ["3"])
        self.assertEqual(f["lag"], {"0": 2})
        self.assertEqual(f["dirty_rows"], 1234)
        self.assertEqual(f["reqlog"], {"dropped": 5})


def base_report(**kw):
    rep = {"replay": {"classes": {"equal": 5}}, "follow": {"chains": {"0": {"ours": 10, "lag": 1, "hash_ok": True}},
                                                         "errors": []},
           "served_lag_max": 2, "served": {"compat /stats": {"n": 1000, "5xx": 1}}, "lines": 10}
    rep.update(kw)
    return rep


class Verdict(unittest.TestCase):
    cfg = dict(H.DEFAULTS)

    def test_pass(self):
        ok, why = H.verdict(base_report(), self.cfg)
        self.assertTrue(ok, why)

    def test_each_failure(self):
        cases = [
            base_report(replay={"classes": {"unexplained": 1}}),
            base_report(replay={"classes": {"ours-error": 1}}),
            base_report(follow={"chains": {"0": {"ours": 1, "lag": 21}}, "errors": []}),
            base_report(follow={"chains": {"0": {"ours": 1, "lag": 0, "hash_ok": False}}, "errors": []}),
            base_report(follow={"chains": {"0": {"ours": 1, "halted": True}}, "errors": []}),
            base_report(follow={"chains": {}, "errors": ["status/cut: refused"]}),
            base_report(served_lag_max=25),
            base_report(restarts="NRestarts 0 -> 1"),
            base_report(served={"compat /stats": {"n": 100, "5xx": 2}}),
            base_report(lines=0),
        ]
        for rep in cases:
            ok, why = H.verdict(rep, self.cfg)
            self.assertFalse(ok, rep)
            self.assertTrue(why)

    def test_stock_unavailable_and_timing_pass(self):
        ok, _ = H.verdict(base_report(replay={"classes": {"stock-unavailable": 3, "timing": 2, "allowlisted": 4}}),
                          self.cfg)
        self.assertTrue(ok)


FAKE = r'''#!/usr/bin/env python3
import json, os, sys
state = os.environ["FAKE_STATE"]
n = int(open(state).read()) if os.path.exists(state) else 0
open(state, "w").write(str(n + 1))
for line in sys.stdin:
    it = json.loads(line)
    cls = "match"
    if "stats" in it.get("path", ""):
        cls = "diff" if n == 0 else "match"   # differs once, equal on the re-check
    if "recent" in it.get("path", ""):
        cls = "diff"                          # differs every time
    rec = {"a_target": "compat", "b_target": "cwd", "diff": [{"path": "/x", "kind": "Changed"}]}
    out = {"id": it["id"], "skip": None, "outcomes": [{"class": cls, "allowlist": []}]}
    if cls != "match":
        out["outcomes"][0]["record"] = rec
    print(json.dumps(out))
'''


class EndToEnd(unittest.TestCase):
    def test_replay_recheck_and_report(self):
        with tempfile.TemporaryDirectory() as d:
            log = os.path.join(d, "r.jsonl")
            with open(log, "w") as f:
                for e in (rest("/stats", "/stats"), rest("/txs/recent", "/txs/recent"),
                          rest("/txs/tx?requestkey=abc", "/txs/tx")):
                    f.write(json.dumps(e) + "\n")
            fake = os.path.join(d, "fake-compare")
            with open(fake, "w") as f:
                f.write(FAKE)
            os.chmod(fake, os.stat(fake).st_mode | stat.S_IEXEC)
            os.environ["FAKE_STATE"] = os.path.join(d, "n")
            cfg = dict(H.DEFAULTS, log=log, compare_bin=fake, compare_config="x.toml", out_dir=d,
                       recheck_delay_s=0, native_url="http://127.0.0.1:9", admin_url="http://127.0.0.1:9",
                       node_url="http://127.0.0.1:9")
            rep = H.run(cfg, "2026-11-27T12", T0, T0 + 3_600_000, False)
            self.assertEqual(rep["replay"]["classes"], {"equal": 1, "timing": 1, "unexplained": 1})
            self.assertEqual(rep["replay"]["per_route"]["compat:/txs/recent"], {"unexplained": 1})
            self.assertTrue(rep["replay"]["examples"][0]["new"])
            self.assertFalse(rep["pass"])
            self.assertIn("FAIL", H.summary_text(rep))
            # The next hour, the same difference is not new.
            os.remove(os.environ["FAKE_STATE"])
            rep2 = H.run(cfg, "2026-11-27T12", T0, T0 + 3_600_000, False)
            self.assertFalse(rep2["replay"]["examples"][0]["new"])


if __name__ == "__main__":
    unittest.main()
