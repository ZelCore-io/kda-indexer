"""Tests for scripts/hourly_check.py (stdlib unittest, no network).

Run: python3 -m unittest discover -s scripts/tests
"""

import contextlib
import fcntl
import io
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

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
            for name, t in ((p + ".3", T0 + 7), (p + ".2", T0 - 10), (p + ".1", T0 + 5),
                            (p, T0 + 3_600_000)):
                with open(name, "w") as f:
                    f.write(json.dumps(rest("/stats", "/stats", t=t)) + "\nnot json\n")
            # `.3` was last written before the hour began, so it is not opened (its in-window
            # line is a plant to prove that).
            old = T0 / 1000 - 60
            os.utime(p + ".3", (old, old))
            self.assertEqual(H.log_files(p), [p + ".3", p + ".2", p + ".1", p])
            c = H.collections.Counter()
            got = list(H.read_window(H.log_files(p), T0, T0 + 3_600_000, c))
            self.assertEqual([e["t"] for e in got], [T0 + 5])
            self.assertEqual(c["bad-line"], 3)

    def test_local_lines_are_skipped_and_counted(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "r.jsonl")
            lines = [rest("/stats", "/stats", t=T0 + 1),
                     rest("/stats", "/stats", t=T0 + 2, local=True),
                     rest("/txs/recent", "/txs/recent", t=T0 + 3, local=False),
                     rest("/stats", "/stats", t=T0 + 3_600_001, local=True)]  # outside the hour
            with open(p, "w") as f:
                f.write("".join(json.dumps(e) + "\n" for e in lines))
            os.utime(p, (T0 / 1000 + 60, T0 / 1000 + 60))
            c = H.collections.Counter()
            got = list(H.read_window(H.log_files(p), T0, T0 + 3_600_000, c))
            self.assertEqual([e["t"] for e in got], [T0 + 1, T0 + 3])
            self.assertEqual(c["skipped-local"], 1)


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
        self.assertEqual(a[2], {"query": q, "variables": {"h": 1}, "operationName": "Q"})
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

    def test_only_query_documents_are_replayed(self):
        q = "query Q($h: Int!) { block(height: $h) { hash } }"
        for doc in ("mutation M { transfer { ok } }",
                    "# kdx:oracle\nmutation M { x }",
                    "subscription S { newBlocks { hash } }",
                    "query A { lastBlockHeight } mutation B { x }",
                    "query A { lastBlockHeight }\nsubscription B { events { name } }",
                    "fragment F on Block { hash } mutation M { x }"):
            for op in (None, "A", "B"):
                self.assertEqual(H.route_of(gql(doc, op=op))[2], "not-query", doc)
        get = dict(t=T0, listener="graphql", method="GET", route="/graphql", status=200, ms=1,
                   uri="/graphql?query=mutation%20M%20%7B%20x%20%7D")
        self.assertEqual(H.route_of(get)[2], "not-query")
        # Strings, comments and argument objects do not fake a definition.
        for doc in ('query A { a(s: "mutation { x }") }', "# mutation M { x }\n{ a }",
                    "query Q($x: In = {a: 1}) { b }", '"""mutation""" query A { a }'):
            self.assertEqual(H.operation_types(doc), ["query"], doc)

    def test_operation_name_is_kept(self):
        multi = "query A { lastBlockHeight } query B($h: Int!) { block(height: $h) { hash } }"
        it = H.route_of(gql(multi, {"h": 1}, "B"))[2]
        self.assertEqual(it, {"query": multi, "variables": {"h": 1}, "operationName": "B"})
        get = dict(t=T0, listener="graphql", method="GET", route="/graphql", status=200, ms=1,
                   uri="/graphql?query=query%20A%20%7B%20a%20%7D&operationName=A")
        self.assertEqual(H.route_of(get)[2]["operationName"], "A")
        items, _, _ = H.sample([gql(multi, {"h": 1}, "B")], budget=10, per_route=10, seed="h")
        self.assertEqual(items[0]["operationName"], "B")


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
        self.assertEqual(H.item_class(verdict_line(1, "legacy-cmd")), "unexplained",
                         "no record: not proof of equality")
        self.assertEqual(H.item_class(verdict_line(1, "something-new")), "unexplained")
        self.assertEqual(H.item_class(verdict_line(1, "b-error")), "stock-unavailable")
        self.assertEqual(H.item_class(verdict_line(1, "a-error", "diff")), "ours-error")
        self.assertEqual(H.item_class(verdict_line(1, skip="tip-only")), "not-compared")
        self.assertEqual(H.item_class(verdict_line(1, "known-gap", "match")), "known-gap")

    def test_legacy_cmd_without_differences_is_legacy_equal(self):
        # 2026-11-28T09: two old /txs/txs?requestkey= replays came back legacy-cmd on the node
        # pair with diff [] (every field equal) and failed the hour as unexplained.
        def legacy(diff, *others):
            line = verdict_line(1, *others)
            line["outcomes"].append({"class": "legacy-cmd", "allowlist": [],
                                     "record": {"a_target": "compat", "b_target": "node", "diff": diff}})
            return line
        self.assertEqual(H.item_class(legacy([], "match", "match")), "legacy-equal")
        self.assertEqual(H.item_class(legacy([])), "legacy-equal")
        # Any difference stays unexplained; so does a worse class of another pair.
        self.assertEqual(H.item_class(legacy([{"path": "/0/code", "kind": "Changed"}], "match")),
                         "unexplained")
        self.assertEqual(H.item_class(legacy([], "diff")), "unexplained")
        self.assertEqual(H.item_class(legacy([], "a-error")), "ours-error")
        self.assertEqual(H.item_class(legacy(None, "match")), "unexplained")
        self.assertIn("legacy-equal", H.COMPARED)

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
    rep = {"replay": {"classes": {"equal": 300}}, "follow": {"chains": {"0": {"ours": 10, "lag": 1, "hash_ok": True}},
                                                           "errors": []},
           "served_lag_max": 2, "served": {"compat /stats": {"n": 1000, "5xx": 1}}, "lines": 10,
           "sampled": 300}
    rep.update(kw)
    return rep


class Verdict(unittest.TestCase):
    cfg = dict(H.DEFAULTS)

    def test_pass(self):
        status, why = H.verdict(base_report(), self.cfg)
        self.assertEqual(status, "PASS", why)

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
            status, why = H.verdict(rep, self.cfg)
            self.assertEqual(status, "FAIL", rep)
            self.assertTrue(why)

    def test_enough_compared_passes_with_some_stock_unavailable(self):
        rep = base_report(sampled=320, replay={"classes": {"stock-unavailable": 10, "timing": 100,
                                                           "allowlisted": 150, "equal": 60}})
        status, why = H.verdict(rep, self.cfg)
        self.assertEqual(status, "PASS", why)

    def test_too_few_compared_is_inconclusive(self):
        cases = [
            # stock down all hour
            base_report(replay={"classes": {"stock-unavailable": 300}}),
            # the deadline hit before any answer came back
            base_report(replay={"classes": {"not-run": 300}, "deadline_hit": True}),
            # under 60% of the sample compared
            base_report(sampled=500, replay={"classes": {"equal": 250, "not-compared": 250}}),
            # under the absolute floor of 200
            base_report(sampled=150, replay={"classes": {"equal": 150}}),
        ]
        for rep in cases:
            status, why = H.verdict(rep, self.cfg)
            self.assertEqual(status, "INCONCLUSIVE", rep)
            self.assertIn("below the floor", why[0])
        self.assertEqual(H.EXIT["INCONCLUSIVE"], 3)
        # A failure wins over too few compared.
        status, _ = H.verdict(base_report(replay={"classes": {"stock-unavailable": 299, "unexplained": 1}}),
                              self.cfg)
        self.assertEqual(status, "FAIL")
        # A dry run compares nothing by design.
        status, _ = H.verdict(base_report(replay={"classes": {}, "dry_run": True}), self.cfg)
        self.assertEqual(status, "PASS")


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
    if "events" in it.get("path", ""):
        cls = "diff" if n == 0 else "a-error"  # differs, then ours fails on the re-check
    if "search" in it.get("path", ""):
        cls = "diff" if n == 0 else "b-error"  # differs, then stock fails on the re-check
    rec = {"a_target": "compat", "b_target": "cwd", "diff": [{"path": "/x", "kind": "Changed"}]}
    out = {"id": it["id"], "skip": None, "outcomes": [{"class": cls, "allowlist": []}]}
    if cls != "match":
        out["outcomes"][0]["record"] = rec
    print(json.dumps(out))
'''


class EndToEnd(unittest.TestCase):
    def setup(self, d, *extra):
        log = os.path.join(d, "r.jsonl")
        with open(log, "w") as f:
            for e in (rest("/stats", "/stats"), rest("/txs/recent", "/txs/recent"),
                      rest("/txs/tx?requestkey=abc", "/txs/tx")) + extra:
                f.write(json.dumps(e) + "\n")
        os.utime(log, (T0 / 1000 + 60, T0 / 1000 + 60))
        fake = os.path.join(d, "fake-compare")
        with open(fake, "w") as f:
            f.write(FAKE)
        os.chmod(fake, os.stat(fake).st_mode | stat.S_IEXEC)
        return dict(H.DEFAULTS, log=log, compare_bin=fake, compare_config="x.toml", out_dir=d,
                    recheck_delay_s=0, native_url="http://127.0.0.1:9", admin_url="http://127.0.0.1:9",
                    node_url="http://127.0.0.1:9")

    def test_local_lines_never_reach_the_sample_or_report(self):
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.dict(os.environ, {"FAKE_STATE": os.path.join(d, "n")}):
            cfg = self.setup(d, rest("/txs/events", "/txs/events", local=True),
                             rest("/txs/search", "/txs/search", local=True))
            rep = H.run(cfg, "2026-11-27T12", T0, T0 + 3_600_000, True)
            self.assertEqual((rep["lines"], rep["sampled"], rep["skipped_local"]), (3, 3, 2))
            self.assertIn("skipped_local 2", H.summary_text(dict(rep, status="PASS", reasons=[])))

    def test_replay_recheck_and_report(self):
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.dict(os.environ, {"FAKE_STATE": os.path.join(d, "n")}):
            cfg = self.setup(d, rest("/txs/events", "/txs/events"), rest("/txs/search", "/txs/search"))
            rep = H.run(cfg, "2026-11-27T12", T0, T0 + 3_600_000, False)
            # stats: equal on the re-check -> timing; events: ours-error on the re-check ->
            # ours-error; search: stock failed on the re-check -> still unexplained.
            self.assertEqual(rep["replay"]["classes"],
                             {"equal": 1, "timing": 1, "unexplained": 2, "ours-error": 1})
            self.assertEqual(rep["replay"]["per_route"]["compat:/txs/recent"], {"unexplained": 1})
            self.assertEqual(rep["replay"]["per_route"]["compat:/txs/events"], {"ours-error": 1})
            self.assertEqual(rep["replay"]["per_route"]["compat:/txs/search"], {"unexplained": 1})
            self.assertTrue(rep["replay"]["examples"][0]["new"])
            self.assertEqual(rep["status"], "FAIL")
            self.assertFalse(rep["pass"])
            self.assertIn("FAIL", H.summary_text(rep))
            state = os.path.join(d, "hourly-state.json")
            self.assertEqual(stat.S_IMODE(os.stat(state).st_mode), 0o640)
            # The next hour, the same difference is not new.
            os.remove(os.environ["FAKE_STATE"])
            rep2 = H.run(cfg, "2026-11-27T12", T0, T0 + 3_600_000, False)
            self.assertFalse(rep2["replay"]["examples"][0]["new"])

    def test_newest_signatures_are_kept(self):
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.dict(os.environ, {"FAKE_STATE": os.path.join(d, "n")}):
            cfg = self.setup(d)
            old = ["zzz%05d" % i for i in range(H.SIGNATURES_KEPT)]
            with open(os.path.join(d, "hourly-state.json"), "w") as f:
                json.dump({"signatures": old}, f)
            rep = H.run(cfg, "2026-11-27T12", T0, T0 + 3_600_000, False)
            sig = rep["replay"]["examples"][0]["signature"]
            with open(os.path.join(d, "hourly-state.json")) as f:
                kept = json.load(f)["signatures"]
            self.assertEqual(len(kept), H.SIGNATURES_KEPT)
            self.assertEqual(kept[-1], sig, "the newest signature is kept although it sorts first")
            self.assertNotIn("zzz00000", kept, "the oldest goes")

    def test_recheck_shares_the_one_deadline(self):
        clock, timeouts = [0.0], []

        def fake_run(cmd, input, capture_output, text, timeout):  # noqa: A002
            timeouts.append(timeout)
            clock[0] += 2000
            out = "".join(json.dumps(verdict_line(json.loads(x)["id"], "diff")) + "\n"
                          for x in input.splitlines())
            return subprocess.CompletedProcess(cmd, 0, out, "")

        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(H.time, "monotonic", lambda: clock[0]), \
                mock.patch.object(H.subprocess, "run", fake_run), \
                mock.patch.object(H.time, "sleep", lambda s: None):
            cfg = self.setup(d)
            rep = H.run(cfg, "2026-11-27T12", T0, T0 + 3_600_000, False)
            self.assertEqual(timeouts, [3000, 1000], "the re-check gets only what is left of 50 min")
            clock[0], timeouts[:] = 0.0, []
            cfg["deadline_minutes"] = 30
            rep = H.run(cfg, "2026-11-27T12", T0, T0 + 3_600_000, False)
            self.assertEqual(timeouts, [1800], "no time left: no re-check")
            self.assertTrue(rep["replay"]["deadline_hit"])
            self.assertEqual(rep["replay"]["classes"], {"unexplained": 3})

    def test_main_writes_0640_and_a_second_run_is_busy(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = self.setup(d)
            conf = os.path.join(d, "hourly.toml")
            with open(conf, "w") as f:
                f.write('log = "%s"\nout_dir = "%s"\nnative_url = "http://127.0.0.1:9"\n'
                        'admin_url = "http://127.0.0.1:9"\nnode_url = "http://127.0.0.1:9"\n'
                        % (cfg["log"], d))
            with contextlib.redirect_stdout(io.StringIO()):
                code = H.main(["--config", conf, "--hour", "2026-11-27T12", "--dry-run"])
            self.assertEqual(code, 1, "the follow check cannot reach the services")
            report = os.path.join(d, "hourly-2026-11-27T12.json")
            self.assertEqual(stat.S_IMODE(os.stat(report).st_mode), 0o640)
            fd = os.open(os.path.join(d, "hourly.lock"), os.O_RDWR)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                err = io.StringIO()
                with contextlib.redirect_stderr(err):
                    code = H.main(["--config", conf, "--hour", "2026-11-27T12", "--dry-run"])
                self.assertEqual(code, 2)
                self.assertIn("busy", err.getvalue())
            finally:
                os.close(fd)


if __name__ == "__main__":
    unittest.main()
