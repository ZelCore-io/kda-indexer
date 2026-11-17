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
        self.assertEqual(c["text"], "12.9× faster")
        c = docs_charts.change(metric("b", 55, 709, unit="GB"))
        self.assertEqual(c["text"], "12.9× smaller")
        self.assertLess(c["pct"], 0)

    def test_change_regression(self):
        c = docs_charts.change(metric("a", 9.4, 4.0, unit="ms"))
        self.assertEqual(c["direction"], "worse")
        self.assertEqual(c["text"], "2.4× slower")
        c = docs_charts.change(metric("b", 9.4, 4.0, unit="rows"))
        self.assertEqual(c["text"], "2.4× more")

    def test_change_same(self):
        c = docs_charts.change(metric("a", 101, 100))
        self.assertEqual(c["direction"], "same")
        c = docs_charts.change(metric("b", 99, 100, better="equal", unit="txs"))
        self.assertEqual(c["direction"], "same")

    def test_change_without_baseline(self):
        c = docs_charts.change(metric("a", 31419, None, unit="s"))
        self.assertEqual(c["direction"], "none")
        self.assertIsNone(c["factor"])

    def test_change_zero(self):
        c = docs_charts.change(metric("a", 0, 784, unit="rows"))
        self.assertEqual(c["direction"], "better")
        self.assertEqual(c["text"], "100% fewer")


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
    def tree(self, page):
        root = tempfile.mkdtemp()
        data = {
            "metrics": [metric("lat", 55, 709)],
            "charts": [],
        }
        write(os.path.join(root, "docs", "data", "benchmarks.json"), json.dumps(data))
        write(os.path.join(root, "docs", "other.md"), "# other\n")
        write(os.path.join(root, "README.md"), "See [docs](docs/README.md).\n")
        write(os.path.join(root, "docs", "README.md"), page)
        return root

    def test_clean_tree_passes(self):
        root = self.tree("[o](other.md) <!--m:lat-->55 ms<!--/m--> <!--m:lat-->12.9× faster<!--/m-->\n")
        self.assertEqual(check(root), 0)

    def test_relative_links_resolve(self):
        root = self.tree("[bad](missing.md)\n")
        self.assertNotEqual(check(root), 0)
        root = self.tree("[case](Other.md)\n")
        self.assertNotEqual(check(root), 0)
        root = self.tree("![img](img/none.svg)\n")
        self.assertNotEqual(check(root), 0)

    def test_page_numbers_match_data(self):
        root = self.tree("<!--m:lat-->56 ms<!--/m-->\n")
        self.assertNotEqual(check(root), 0)
        root = self.tree("<!--m:nope-->55<!--/m-->\n")
        self.assertNotEqual(check(root), 0)

    def test_forbidden_strings(self):
        root = self.tree("runs on " + "KDA" + "-03\n")
        self.assertNotEqual(check(root), 0)
        root = self.tree("mail someone" + "@" + "example.org\n")
        self.assertNotEqual(check(root), 0)
        root = self.tree("fine\n")
        write(os.path.join(root, "docs", "specs", "x.md"), "KDA" + "-03\n")
        self.assertEqual(check(root), 0)


if __name__ == "__main__":
    unittest.main()
