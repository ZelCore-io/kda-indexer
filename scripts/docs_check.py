#!/usr/bin/env python3
"""Check the public docs: relative links, quoted numbers, forbidden strings.

Python 3 standard library only.

Checks
  links      every relative Markdown link, image and <img src>/<a href> in
             README.md and docs/**/*.md resolves to a file in the repo, with
             the exact case GitHub will use.
  data       docs/data/benchmarks.json has every required field with the
             right type, unique ids, charts that name real metrics and use
             one unit each.
  numbers    every value tagged in a page as <!--m:ID-->value<!--/m--> matches
             docs/data/benchmarks.json. A tag holding a x or a % must equal
             the metric's full change text (factor, direction word and
             percent). Any other tag is a raw value: a number equal, at the
             precision shown, to ours or baseline, optionally followed by the
             metric's unit. A slower latency metric may also tag its class
             (negligible, noticeable, user-visible). Metrics with
             "publish": false must not be tagged.
  forbidden  README.md and docs/** hold no private infrastructure strings or
             email addresses. docs/specs, docs/plans and docs/internal are
             skipped by every check.

Usage: python3 scripts/docs_check.py [repo_root]   (exit 0 = ok)
"""

import json
import math
import os
import re
import sys
from urllib.parse import unquote

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import docs_charts  # noqa: E402

SKIP_DIRS = ("docs/specs", "docs/plans", "docs/internal")

# Spelled in pieces so this file does not itself contain them.
FORBIDDEN = [
    "10." + "100.",
    "FX" + "-",
    "KDA" + "-0",
    "KDA" + "INDEXER",
    "KDA" + "LB",
    "/" + "Users/",
    "/" + "Volumes/",
]
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")

FENCE = re.compile(r"^(```|~~~).*?^\1[^\n]*$", re.S | re.M)
INLINE_CODE = re.compile(r"`[^`\n]*`")
# Each pattern puts the target in one of its groups; _target_group picks the one that matched.
MD_LINK = re.compile(r"!?\[(?:[^\[\]]|\[[^\]]*\])*\]\(\s*(?:<([^>\n]*)>|([^)\s]+))"
                     r"(?:\s+(?:\"[^\"]*\"|'[^']*'))?\s*\)")
REF_DEF = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*(?:<([^>\n]*)>|(\S+))", re.M)
HTML_REF = re.compile(r"<(?:img|a|source)\b[^>]*?\b(?:src|href|srcset)\s*=\s*"
                      r"(?:\"([^\"]+)\"|'([^']+)'|([^\s\"'>]+))", re.I)
TAG = re.compile(r"<!--m:([A-Za-z0-9_.-]+)-->(.*?)<!--/m-->", re.S)
NUMBER = re.compile(r"-?\d[\d,]*(?:\.\d+)?|-?\.\d+")


def _rel(root, path):
    return os.path.relpath(path, root).replace(os.sep, "/")


def _skipped(rel):
    return any(rel == d or rel.startswith(d + "/") for d in SKIP_DIRS)


def scope_files(root):
    """README.md plus every file under docs/, minus the skipped dirs."""
    files = []
    readme = os.path.join(root, "README.md")
    if os.path.isfile(readme):
        files.append(readme)
    docs = os.path.join(root, "docs")
    for d, dirs, names in os.walk(docs):
        dirs[:] = sorted(x for x in dirs if not _skipped(_rel(root, os.path.join(d, x))))
        for n in sorted(names):
            files.append(os.path.join(d, n))
    return files


def _line(text, pos):
    return text.count("\n", 0, pos) + 1


def _exists_exact(root, rel):
    """True if rel (a '/'-separated path under root) exists with exact case."""
    cur = root
    for part in [p for p in rel.split("/") if p not in ("", ".")]:
        try:
            entries = os.listdir(cur)
        except OSError:
            return False
        if part not in entries:
            return False
        cur = os.path.join(cur, part)
    return True


def _blank_code(text):
    """Replace code blocks and inline code with spaces, keeping offsets."""
    text = FENCE.sub(lambda m: re.sub(r"[^\n]", " ", m.group(0)), text)
    return INLINE_CODE.sub(lambda m: " " * len(m.group(0)), text)


def check_links(root, path, text, errors):
    body = _blank_code(text)
    targets = []
    for rx in (MD_LINK, REF_DEF, HTML_REF):
        for m in rx.finditer(body):
            g = next(i for i in range(1, (rx.groups or 0) + 1) if m.group(i) is not None)
            targets.append((m.start(g), m.group(g)))
    base = os.path.dirname(path)
    for pos, target in sorted(targets):
        t = target.strip()
        if not t or t.startswith("#") or re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", t) or t.startswith("//"):
            continue
        t = unquote(t.split("#", 1)[0].split("?", 1)[0])
        if not t:
            continue
        if t.startswith("/"):
            full = os.path.normpath(os.path.join(root, t.lstrip("/")))
        else:
            full = os.path.normpath(os.path.join(base, t))
        rel = os.path.relpath(full, root).replace(os.sep, "/")
        where = "{}:{}".format(_rel(root, path), _line(text, pos))
        if rel.startswith(".."):
            errors.append("{}: links: {} points outside the repository".format(where, target))
        elif not _exists_exact(root, rel):
            errors.append("{}: links: {} does not resolve (missing or wrong case)".format(where, target))


def _matches(shown, value):
    """shown is the text of a number as printed; value the data value."""
    if value is None or isinstance(value, bool) or not math.isfinite(value):
        return False
    s = shown.replace(",", "")
    decimals = len(s.split(".", 1)[1]) if "." in s else 0
    return abs(round(abs(value), decimals) - abs(float(s))) < 10 ** -(decimals + 6)


RAW = re.compile(r"^(-?\d[\d,]*(?:\.\d+)?|-?\.\d+)\s*(.*)$", re.S)


def check_numbers(root, path, text, metrics, errors):
    for m in TAG.finditer(text):
        mid = m.group(1)
        shown = " ".join(m.group(2).split())
        where = "{}:{}".format(_rel(root, path), _line(text, m.start()))
        metric = metrics.get(mid)
        if metric is None:
            errors.append("{}: numbers: unknown metric id {!r}".format(where, mid))
            continue
        if not docs_charts.published(metric):
            errors.append("{}: numbers: metric {!r} is not published (\"publish\": false)".format(where, mid))
            continue
        c = docs_charts.change(metric)
        if shown == c["text"] or (shown == "not measured" and c["direction"] == "none"):
            continue
        if c.get("class") and shown == c["class"]:
            continue
        if "\u00d7" in shown or "%" in shown:
            errors.append("{}: numbers: {!r} for {} is not the change text {!r}".format(
                where, shown, mid, c["text"]))
            continue
        raw = RAW.match(shown)
        if not raw:
            errors.append("{}: numbers: {!r} for {} is neither a raw value nor the change text {!r}".format(
                where, shown, mid, c["text"]))
            continue
        number, unit = raw.group(1), raw.group(2).strip()
        if unit and unit != metric.get("unit"):
            errors.append("{}: numbers: {!r} for {} has unit {!r}, the data say {!r}".format(
                where, shown, mid, unit, metric.get("unit")))
            continue
        if not any(_matches(number, v) for v in (metric.get("ours"), metric.get("baseline"))):
            errors.append("{}: numbers: {!r} for {} matches neither ours={} nor baseline={}".format(
                where, shown, mid, metric.get("ours"), metric.get("baseline")))


GROUPS = {"disk", "latency", "correctness", "sync", "resources", "graphql", "process"}
BASELINES = {"chainweb-data", "GraphQL indexer", "node", "/v1/txs service", "none"}
BETTER = {"lower", "higher", "equal"}
SCALES = {"auto", "linear", "log"}
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def validate_data(data):
    """Return a list of problems in a parsed benchmarks.json (empty = valid)."""
    errs = []
    if not isinstance(data, dict):
        return ["top level must be an object"]
    metrics, charts = data.get("metrics"), data.get("charts")
    if not isinstance(metrics, list):
        errs.append("metrics must be a list")
        metrics = []
    if not isinstance(charts, list):
        errs.append("charts must be a list")
        charts = []
    by_id = {}
    for i, m in enumerate(metrics):
        if not isinstance(m, dict):
            errs.append("metrics[{}] must be an object".format(i))
            continue
        mid = m.get("id")
        tag = "metric {!r}".format(mid) if isinstance(mid, str) and mid else "metrics[{}]".format(i)

        def need(key, kind, ok=None, _m=m, _tag=tag):
            if key not in _m:
                errs.append("{}: missing {}".format(_tag, key))
                return False
            if not kind(_m[key]):
                errs.append("{}: {} has the wrong type or value ({!r})".format(_tag, key, _m[key]))
                return False
            return True

        text = lambda v: isinstance(v, str) and v.strip() != ""  # noqa: E731
        need("id", text)
        if isinstance(mid, str):
            if mid in by_id:
                errs.append("{}: duplicate id".format(tag))
            by_id[mid] = m
        if need("group", lambda v: v in GROUPS) is False:
            pass
        need("label", text)
        need("unit", text)
        for side in ("ours", "baseline"):
            need(side, lambda v: v is None or _is_number(v))
        need("baseline_name", lambda v: v in BASELINES)
        need("better", lambda v: v in BETTER)
        need("interim", lambda v: isinstance(v, bool))
        need("source", text)
        need("date", lambda v: isinstance(v, str) and bool(DATE.match(v)))
        if "note" in m and not isinstance(m["note"], str):
            errs.append("{}: note must be a string".format(tag))
        if "publish" in m and not isinstance(m["publish"], bool):
            errs.append("{}: publish must be a boolean".format(tag))
    seen = set()
    for i, c in enumerate(charts):
        if not isinstance(c, dict):
            errs.append("charts[{}] must be an object".format(i))
            continue
        cid = c.get("id")
        tag = "chart {!r}".format(cid) if isinstance(cid, str) and cid else "charts[{}]".format(i)
        for key in ("id", "file", "title"):
            if not isinstance(c.get(key), str) or not c.get(key).strip():
                errs.append("{}: missing or invalid {}".format(tag, key))
        if isinstance(c.get("file"), str) and not c["file"].endswith(".svg"):
            errs.append("{}: file must end in .svg".format(tag))
        if cid in seen:
            errs.append("{}: duplicate id".format(tag))
        seen.add(cid)
        if c.get("scale") not in SCALES:
            errs.append("{}: scale must be one of auto, linear, log".format(tag))
        ids = c.get("metrics")
        if not isinstance(ids, list) or not ids or not all(isinstance(x, str) for x in ids):
            errs.append("{}: metrics must be a non-empty list of ids".format(tag))
            continue
        unknown = [x for x in ids if x not in by_id]
        if unknown:
            errs.append("{}: unknown metrics {}".format(tag, unknown))
        units = sorted({by_id[x].get("unit") for x in ids
                        if x in by_id and docs_charts.published(by_id[x]) and isinstance(by_id[x].get("unit"), str)})
        if len(units) > 1:
            errs.append("{}: mixes units {}; one unit per chart".format(tag, units))
    return errs


def check_forbidden(root, path, text, errors):
    for s in FORBIDDEN:
        i = text.find(s)
        if i >= 0:
            errors.append("{}:{}: forbidden: {!r}".format(_rel(root, path), _line(text, i), s))
    for m in EMAIL.finditer(text):
        errors.append("{}:{}: forbidden: email address".format(_rel(root, path), _line(text, m.start())))


def main(root):
    root = os.path.abspath(root)
    errors = []
    data_path = os.path.join(root, "docs", "data", "benchmarks.json")
    metrics = {}
    rel_data = "docs/data/benchmarks.json"
    if not os.path.isfile(data_path):
        errors.append("{}: data: file is missing".format(rel_data))
    else:
        try:
            with open(data_path, encoding="utf-8") as f:
                data = json.load(f)
        except ValueError as e:
            errors.append("{}: data: not valid JSON ({})".format(rel_data, e))
        else:
            problems = validate_data(data)
            errors.extend("{}: data: {}".format(rel_data, p) for p in problems)
            if not problems:
                metrics = {m["id"]: m for m in data["metrics"]}
    for path in scope_files(root):
        try:
            with open(path, encoding="utf-8") as f:
                text = f.read()
        except (UnicodeDecodeError, OSError):
            continue  # binary file
        check_forbidden(root, path, text, errors)
        if path.endswith(".md"):
            check_links(root, path, text, errors)
            check_numbers(root, path, text, metrics, errors)
    for e in errors:
        print(e)
    if errors:
        print("docs-check: {} problem(s)".format(len(errors)))
        return 1
    print("docs-check: ok")
    return 0


if __name__ == "__main__":
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else here))
