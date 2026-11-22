"""Tests for scripts/docs_charts.py and scripts/docs_check.py (stdlib unittest).

Run: python3 -m unittest discover scripts/tests
"""

import contextlib
import io
import json
import os
import re
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import docs_charts  # noqa: E402
import docs_check  # noqa: E402


def metric(mid, ours, baseline, unit="ms", better="lower", **kw):
    m = {
        "id": mid,
        "group": "latency",
        "label": mid,
        "unit": unit,
        "ours": ours,
        "baseline": baseline,
        "baseline_name": "chainweb-data",
        "better": better,
        "interim": True,
        "source": "test",
        "date": "2026-11-17",
    }
    m.update(kw)
    return m


def check(root):
    with contextlib.redirect_stdout(io.StringIO()):
        return docs_check.main(root)


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


class ChangeTests(unittest.TestCase):
    def test_change_lower_is_better(self):
        c = docs_charts.change(metric("a", 55, 709, unit="ms"))
        self.assertAlmostEqual(c["factor"], 12.9, places=1)
        self.assertEqual(c["direction"], "better")
        self.assertEqual(c["text"], "−654 ms (12.9× faster, −92%)")
        c = docs_charts.change(metric("b", 55, 709, unit="GB"))
        self.assertEqual(c["text"], "12.9× smaller (−92%)")
        c = docs_charts.change(metric("c", 140.2, 1568.6, unit="GB"))
        self.assertEqual(c["text"], "11.2× smaller (−91%)")
        self.assertLess(c["pct"], 0)

    def test_change_regression(self):
        c = docs_charts.change(metric("a", 9.4, 4.0, unit="ms"))
        self.assertEqual(c["direction"], "worse")
        self.assertEqual(c["text"], "+5.4 ms (2.4× slower, +135%)")
        c = docs_charts.change(metric("b", 9.4, 4.0, unit="rows"))
        self.assertEqual(c["text"], "2.4× more (+135%)")
        c = docs_charts.change(metric("c", 1.17, 0.8, unit="GB"))
        self.assertEqual(c["text"], "1.5× larger (+46%)")

    def test_change_same(self):
        c = docs_charts.change(metric("a", 101, 100))
        self.assertEqual(c["direction"], "same")
        c = docs_charts.change(metric("b", 99, 100, better="equal", unit="txs"))
        self.assertEqual(c["direction"], "same")

    def test_change_small_factor_is_percent_only(self):
        c = docs_charts.change(metric("a", 95, 100, unit="ms"))
        self.assertEqual(c["text"], "−5 ms (5% faster)")
        c = docs_charts.change(metric("b", 108, 100, unit="ms"))
        self.assertEqual(c["text"], "+8 ms (8% slower)")

    def test_change_boundary_at_two_percent(self):
        self.assertEqual(docs_charts.change(metric("a", 102, 100, unit="ms"))["direction"], "worse")
        self.assertEqual(docs_charts.change(metric("b", 98, 100, unit="ms"))["direction"], "better")
        self.assertEqual(docs_charts.change(metric("c", 98, 100, unit="ms"))["text"], "−2 ms (2% faster)")
        self.assertEqual(docs_charts.change(metric("d", 101.99, 100, unit="ms"))["direction"], "same")
        self.assertEqual(docs_charts.change(metric("e", 98.01, 100, unit="ms"))["direction"], "same")

    def test_change_histogram_edges_are_rounded_and_prefixed(self):
        src = "values are histogram bucket upper edges (about ±20%)"
        c = docs_charts.change(metric("a", 38, 709, unit="ms", source=src))
        self.assertEqual(c["text"], "−670 ms (about 19× faster, −95%)")
        c = docs_charts.change(metric("b", 2540, 709, unit="ms", source=src))
        self.assertEqual(c["text"], "+1.8 s (about 3.6× slower, +260%)")
        c = docs_charts.change(metric("c", 22, 5266, unit="ms", note="bucket edges"))
        self.assertEqual(c["text"], "−5.2 s (about 240× faster, −99%)")
        c = docs_charts.change(metric("d", 1, 5, unit="ms", source=src))
        self.assertEqual(c["text"], "−4 ms (about 5× faster, −80%)")
        # exact data are not rounded or prefixed
        c = docs_charts.change(metric("e", 38, 709, unit="ms"))
        self.assertEqual(c["text"], "−671 ms (18.7× faster, −95%)")

    def test_latency_change_leads_with_absolute_time(self):
        src = "values are histogram bucket upper edges (about ±20%)"
        c = docs_charts.change(metric("a", 6, 95, unit="ms", source=src))
        self.assertEqual(c["text"], "−89 ms (about 16× faster, −94%)")
        self.assertIsNone(c["class"])
        c = docs_charts.change(metric("b", 2540, 709, unit="ms", source=src))
        self.assertEqual(c["text"], "+1.8 s (about 3.6× slower, +260%)")
        c = docs_charts.change(metric("c", 492, 46956, unit="ms", source=src))
        self.assertEqual(c["text"], "−46 s (about 95× faster, −99%)")
        # exact data keep their precision
        c = docs_charts.change(metric("d", 12.5, 3.5, unit="ms"))
        self.assertEqual(c["text"], "+9 ms (3.6× slower, +257%)")
        c = docs_charts.change(metric("e", 108, 100, unit="ms"))
        self.assertEqual(c["text"], "+8 ms (8% slower)")
        c = docs_charts.change(metric("f", 2.5, 1.0, unit="s"))
        self.assertEqual(c["text"], "+1.5 s (2.5× slower, +150%)")
        # sizes and counts are unchanged
        c = docs_charts.change(metric("g", 140.2, 1568.6, unit="GB"))
        self.assertEqual(c["text"], "11.2× smaller (−91%)")
        self.assertIsNone(c["class"])

    def test_latency_class(self):
        def cls(ours, base, mid="x-p50", unit="ms"):
            return docs_charts.change(metric(mid, ours, base, unit=unit))["class"]
        self.assertEqual(cls(27, 15), "negligible")          # +12 ms
        self.assertEqual(cls(34.9, 15), "negligible")        # +19.9 ms
        self.assertEqual(cls(35, 15), "noticeable")          # +20 ms
        self.assertEqual(cls(215, 15), "noticeable")         # +200 ms
        self.assertEqual(cls(216, 15), "user-visible")       # +201 ms
        self.assertEqual(cls(1.3, 1.0, unit="s"), "user-visible")  # +300 ms
        # a p99 over 1.5 s is user-visible whatever was added
        self.assertEqual(cls(1600, 1550, mid="x-p99"), "user-visible")
        self.assertEqual(cls(1600, 1550, mid="x-p50"), "noticeable")
        self.assertEqual(docs_charts.change(metric("y", 1600, 1550, label="q p99"))["class"], "user-visible")
        # faster, same or unmeasured: no class
        self.assertIsNone(cls(5, 15))
        self.assertIsNone(cls(15, 15))
        self.assertIsNone(docs_charts.change(metric("z", 15, None))["class"])

    def test_render_shows_latency_class(self):
        m = metric("slow-p99", 2540, 709, unit="ms")
        svg = docs_charts.render({"id": "c", "title": "t", "metrics": ["slow-p99"], "scale": "auto"},
                                 {"slow-p99": m})
        self.assertIn("· user-visible", svg)

    def test_fmt_factor(self):
        self.assertEqual(docs_charts.fmt_factor(99.96), "100")
        self.assertEqual(docs_charts.fmt_factor(99.94), "99.9")
        self.assertEqual(docs_charts.fmt_factor(12.94), "12.9")
        self.assertEqual(docs_charts.fmt_factor(1234.6), "1,235")

    def test_change_without_baseline(self):
        c = docs_charts.change(metric("a", 31419, None, unit="s"))
        self.assertEqual(c["direction"], "none")
        self.assertIsNone(c["factor"])

    def test_change_zero(self):
        c = docs_charts.change(metric("a", 0, 784, unit="rows"))
        self.assertEqual(c["direction"], "better")
        self.assertEqual(c["text"], "100% fewer")

    def test_render_skips_unpublished_metrics(self):
        metrics = {"a": metric("a", 5, 9), "b": metric("b", 6, 8, publish=False)}
        chart = {"id": "t", "file": "t.svg", "title": "T", "metrics": ["a", "b"], "scale": "auto"}
        svg = docs_charts.render(chart, metrics)
        self.assertIn('id="a"', svg)
        self.assertNotIn('id="b"', svg)

    def test_main_refuses_unplain_ids_and_file_names(self):
        # F-59: a quote in an id would break out of the SVG attribute; a path in `file`
        # would write outside the output dir.
        root = tempfile.mkdtemp()
        for chart, mid in (({"id": 'c" onload="x', "file": "c.svg"}, "a"),
                           ({"id": "c", "file": "../escape.svg"}, "a"),
                           ({"id": "c", "file": "c.svg"}, 'a"b')):
            chart.update({"title": "C", "metrics": [mid], "scale": "linear"})
            data = {"metrics": [metric(mid, 1, 2)], "charts": [chart]}
            write(os.path.join(root, "d.json"), json.dumps(data))
            with self.assertRaises(SystemExit):
                docs_charts.main(os.path.join(root, "d.json"), os.path.join(root, "out"))
        self.assertFalse(os.path.exists(os.path.join(root, "escape.svg")))

    def test_main_rejects_mixed_units(self):
        root = tempfile.mkdtemp()
        data = {"metrics": [metric("a", 1, 2, unit="rows"), metric("b", 1, 2, unit="blocks")],
                "charts": [{"id": "c", "file": "c.svg", "title": "C", "metrics": ["a", "b"], "scale": "linear"}]}
        write(os.path.join(root, "d.json"), json.dumps(data))
        with self.assertRaises(SystemExit):
            docs_charts.main(os.path.join(root, "d.json"), os.path.join(root, "out"))


class RenderTests(unittest.TestCase):
    def setUp(self):
        self.metrics = {
            "a": metric("a", 18, 56348),
            "b": metric("b", 22, 409),
        }
        self.chart = {"id": "t", "file": "t.svg", "title": "T", "metrics": ["a", "b"], "scale": "auto"}

    def test_log_scale_auto(self):
        svg = docs_charts.render(self.chart, self.metrics)
        self.assertIn('<g class="axis log"', svg)
        lin = dict(self.chart, scale="linear")
        self.assertIn('<g class="axis linear"', docs_charts.render(lin, self.metrics))
        small = {"a": metric("a", 5, 9), "b": metric("b", 6, 8)}
        self.assertIn('<g class="axis linear"', docs_charts.render(self.chart, small))

    def test_svg_has_dark_mode_block(self):
        svg = docs_charts.render(self.chart, self.metrics)
        self.assertIn("prefers-color-scheme: dark", svg)
        outside = re.sub(r"<style>.*?</style>", "", svg, flags=re.S)
        self.assertNotIn('fill="#', outside)
        self.assertNotIn('stroke="#', outside)
        self.assertNotIn("style=", outside)
        self.assertIn("<title", svg)
        self.assertIn("<desc", svg)

    def test_render_deterministic(self):
        self.assertEqual(
            docs_charts.render(self.chart, self.metrics),
            docs_charts.render(self.chart, self.metrics),
        )

    def test_render_handles_null_and_zero(self):
        m = {"a": metric("a", 0, 784, unit="rows"), "b": metric("b", 5, None)}
        svg = docs_charts.render(dict(self.chart, scale="auto"), m)
        self.assertIn("not measured", svg)


class CheckTests(unittest.TestCase):
    def tree(self, page, metrics=None, charts=None):
        root = tempfile.mkdtemp()
        data = {
            "metrics": metrics if metrics is not None else [metric("lat", 55, 709)],
            "charts": charts if charts is not None else [],
        }
        write(os.path.join(root, "docs", "data", "benchmarks.json"), json.dumps(data))
        write(os.path.join(root, "docs", "other.md"), "# other\n")
        write(os.path.join(root, "docs", "my file.md"), "# spaced\n")
        write(os.path.join(root, "README.md"), "See [docs](docs/README.md).\n")
        write(os.path.join(root, "docs", "README.md"), page)
        return root

    def test_clean_tree_passes(self):
        root = self.tree("[o](other.md) <!--m:lat-->55 ms<!--/m--> <!--m:lat-->55<!--/m--> "
                         "<!--m:lat-->709 ms<!--/m--> <!--m:lat-->−654 ms (12.9× faster, −92%)<!--/m-->\n")
        self.assertEqual(check(root), 0)

    def test_relative_links_resolve(self):
        root = self.tree("[bad](missing.md)\n")
        self.assertNotEqual(check(root), 0)
        root = self.tree("[case](Other.md)\n")
        self.assertNotEqual(check(root), 0)
        root = self.tree("![img](img/none.svg)\n")
        self.assertNotEqual(check(root), 0)

    def test_angle_bracket_and_single_quote_links(self):
        self.assertEqual(check(self.tree("[ok](<my file.md>)\n")), 0)
        self.assertEqual(check(self.tree("[ok](<my file.md> \"title\")\n")), 0)
        self.assertNotEqual(check(self.tree("[bad](<no such file.md>)\n")), 0)
        self.assertNotEqual(check(self.tree("[bad](<missing.md> 'title')\n")), 0)
        self.assertEqual(check(self.tree("<a href='other.md'>ok</a>\n")), 0)
        self.assertNotEqual(check(self.tree("<a href='missing.md'>bad</a>\n")), 0)
        self.assertNotEqual(check(self.tree("<img src='img/none.svg'>\n")), 0)
        self.assertNotEqual(check(self.tree('<img src="img/none.svg">\n')), 0)
        self.assertNotEqual(check(self.tree("[ref]: <missing.md>\n")), 0)

    def test_page_numbers_match_data(self):
        root = self.tree("<!--m:lat-->56 ms<!--/m-->\n")
        self.assertNotEqual(check(root), 0)
        root = self.tree("<!--m:nope-->55<!--/m-->\n")
        self.assertNotEqual(check(root), 0)

    def test_change_tag_needs_full_text(self):
        # wrong direction word
        self.assertNotEqual(check(self.tree("<!--m:lat-->12.9× slower (−92%)<!--/m-->\n")), 0)
        # wrong sign or percent
        self.assertNotEqual(check(self.tree("<!--m:lat-->−654 ms (12.9× faster, +92%)<!--/m-->\n")), 0)
        self.assertNotEqual(check(self.tree("<!--m:lat-->−654 ms (12.9× faster, −91%)<!--/m-->\n")), 0)
        # factor or percent alone are not the change text
        self.assertNotEqual(check(self.tree("<!--m:lat-->12.9×<!--/m-->\n")), 0)
        self.assertNotEqual(check(self.tree("<!--m:lat-->92%<!--/m-->\n")), 0)
        # right words, wrong number
        self.assertNotEqual(check(self.tree("<!--m:lat-->13.9× faster (−92%)<!--/m-->\n")), 0)
        # latency needs the absolute time, and the right one
        self.assertNotEqual(check(self.tree("<!--m:lat-->12.9× faster (−92%)<!--/m-->\n")), 0)
        self.assertNotEqual(check(self.tree("<!--m:lat-->−650 ms (12.9× faster, −92%)<!--/m-->\n")), 0)
        self.assertEqual(check(self.tree("<!--m:lat-->−654 ms (12.9× faster, −92%)<!--/m-->\n")), 0)

    def test_raw_value_units_and_bare_numbers(self):
        # wrong unit
        self.assertNotEqual(check(self.tree("<!--m:lat-->55 s<!--/m-->\n")), 0)
        self.assertNotEqual(check(self.tree("<!--m:lat-->55 GB<!--/m-->\n")), 0)
        # a bare change factor or percent is not a raw value
        self.assertNotEqual(check(self.tree("<!--m:lat-->12.9<!--/m-->\n")), 0)
        self.assertNotEqual(check(self.tree("<!--m:lat-->92<!--/m-->\n")), 0)
        # a bare ours or baseline value is accepted
        self.assertEqual(check(self.tree("<!--m:lat-->709<!--/m-->\n")), 0)

    def test_latency_class_tag(self):
        metrics = [metric("lat", 55, 709), metric("slow", 2540, 709)]
        self.assertEqual(check(self.tree("<!--m:slow-->user-visible<!--/m-->\n", metrics)), 0)
        self.assertNotEqual(check(self.tree("<!--m:slow-->negligible<!--/m-->\n", metrics)), 0)
        # a faster metric has no class
        self.assertNotEqual(check(self.tree("<!--m:lat-->negligible<!--/m-->\n", metrics)), 0)

    def test_unpublished_metric_cannot_be_tagged(self):
        metrics = [metric("lat", 55, 709), metric("wip", 182, None, unit="records", publish=False)]
        self.assertEqual(check(self.tree("<!--m:lat-->55 ms<!--/m-->\n", metrics)), 0)
        self.assertNotEqual(check(self.tree("<!--m:wip-->182 records<!--/m-->\n", metrics)), 0)

    def test_forbidden_strings(self):
        root = self.tree("runs on " + "KDA" + "-03\n")
        self.assertNotEqual(check(root), 0)
        root = self.tree("mail someone" + "@" + "example.org\n")
        self.assertNotEqual(check(root), 0)
        root = self.tree("fine\n")
        write(os.path.join(root, "docs", "specs", "x.md"), "KDA" + "-03\n")
        self.assertEqual(check(root), 0)


class DataValidationTests(unittest.TestCase):
    def chart(self, ids, **kw):
        c = {"id": "c", "file": "c.svg", "title": "C", "metrics": ids, "scale": "auto"}
        c.update(kw)
        return c

    def problems(self, metrics, charts=()):
        return docs_check.validate_data({"metrics": metrics, "charts": list(charts)})

    def test_valid_data(self):
        ms = [metric("a", 1, 2), metric("b", 1, None, publish=False, note="n")]
        self.assertEqual(self.problems(ms, [self.chart(["a", "b"])]), [])

    def test_required_fields(self):
        for key in ("id", "group", "label", "unit", "ours", "baseline", "baseline_name",
                    "better", "interim", "source", "date"):
            m = metric("a", 1, 2)
            del m[key]
            self.assertTrue(self.problems([m]), key)

    def test_field_types(self):
        for key, bad in (("interim", "yes"), ("interim", 1), ("ours", "5"), ("ours", True),
                         ("baseline", "x"), ("better", "fastest"), ("group", "misc"),
                         ("date", "17 Nov 2026"), ("unit", ""), ("note", 3), ("publish", "no"),
                         ("baseline_name", "somebody")):
            m = metric("a", 1, 2)
            m[key] = bad
            self.assertTrue(self.problems([m]), (key, bad))

    def test_process_counts_have_no_baseline(self):
        m = metric("a", 752, None, unit="tests", better="equal", group="process", baseline_name="none")
        self.assertEqual(self.problems([m]), [])

    def test_duplicate_ids(self):
        self.assertTrue(self.problems([metric("a", 1, 2), metric("a", 3, 4)]))

    def test_chart_rules(self):
        ms = [metric("a", 1, 2, unit="rows"), metric("b", 1, 2, unit="blocks")]
        self.assertTrue(self.problems(ms, [self.chart(["a", "b"])]))       # two units
        self.assertEqual(self.problems(ms, [self.chart(["a"])]), [])
        self.assertTrue(self.problems(ms, [self.chart(["a", "zzz"])]))     # unknown metric
        self.assertTrue(self.problems(ms, [self.chart(["a"], scale="cubic")]))
        self.assertTrue(self.problems(ms, [self.chart(["a"], file="c.png")]))
        self.assertTrue(self.problems(ms, [self.chart([])]))

    def test_ids_and_file_names_are_plain(self):
        # F-59: ids land in SVG attributes and `file` is joined to the output dir.
        for bad in ('a"b', "a b", "<x>", "a'b"):
            self.assertTrue(self.problems([metric("a", 1, 2)], [self.chart(["a"], id=bad)]), bad)
            self.assertTrue(self.problems([metric(bad, 1, 2)]), bad)
        for bad in ("../x.svg", "sub/x.svg", "/tmp/x.svg"):
            self.assertTrue(self.problems([metric("a", 1, 2)], [self.chart(["a"], file=bad)]), bad)

    def test_unpublished_metric_does_not_count_for_units(self):
        ms = [metric("a", 1, 2, unit="rows"), metric("b", 1, 2, unit="blocks", publish=False)]
        self.assertEqual(self.problems(ms, [self.chart(["a", "b"])]), [])

    def test_check_fails_on_invalid_data_file(self):
        root = tempfile.mkdtemp()
        bad = metric("a", 1, 2)
        bad["interim"] = "true"
        write(os.path.join(root, "docs", "data", "benchmarks.json"), json.dumps({"metrics": [bad], "charts": []}))
        self.assertNotEqual(check(root), 0)

    def test_shipped_data_is_valid(self):
        path = os.path.join(os.path.dirname(os.path.dirname(HERE)), "docs", "data", "benchmarks.json")
        with open(path, encoding="utf-8") as f:
            self.assertEqual(docs_check.validate_data(json.load(f)), [])


if __name__ == "__main__":
    unittest.main()
