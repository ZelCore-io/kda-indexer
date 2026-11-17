#!/usr/bin/env python3
"""Check the public docs: relative links, quoted numbers, forbidden strings.

Python 3 standard library only.

Checks
  links      every relative Markdown link, image and <img src>/<a href> in
             README.md and docs/**/*.md resolves to a file in the repo, with
             the exact case GitHub will use.
  numbers    every value tagged in a page as <!--m:ID-->value<!--/m--> matches
             docs/data/benchmarks.json: the metric's change text, or a number
             equal (at the precision shown) to ours, baseline, the change
             factor or the change percent.
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
MD_LINK = re.compile(r"!?\[(?:[^\[\]]|\[[^\]]*\])*\]\(\s*<?([^)\s>]+)>?(?:\s+(?:\"[^\"]*\"|'[^']*'))?\s*\)")
REF_DEF = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*<?(\S+?)>?(?:\s|$)", re.M)
HTML_REF = re.compile(r"<(?:img|a|source)\b[^>]*?\b(?:src|href|srcset)\s*=\s*\"([^\"]+)\"", re.I)
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
            targets.append((m.start(1), m.group(1)))
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


def check_numbers(root, path, text, metrics, errors):
    for m in TAG.finditer(text):
        mid, shown = m.group(1), m.group(2).strip()
        where = "{}:{}".format(_rel(root, path), _line(text, m.start()))
        metric = metrics.get(mid)
        if metric is None:
            errors.append("{}: numbers: unknown metric id {!r}".format(where, mid))
            continue
        c = docs_charts.change(metric)
        if shown == c["text"] or (shown == "not measured" and c["direction"] == "none"):
            continue
        if c["text"] and c["text"] in shown:
            continue
        num = NUMBER.search(shown)
        candidates = [metric.get("ours"), metric.get("baseline"), c["factor"],
                      None if c["pct"] is None else abs(c["pct"])]
        if num and any(_matches(num.group(0), v) for v in candidates):
            continue
        errors.append("{}: numbers: {!r} for {} matches none of ours={}, baseline={}, change={!r}".format(
            where, shown, mid, metric.get("ours"), metric.get("baseline"), c["text"]))


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
    if os.path.isfile(data_path):
        try:
            with open(data_path, encoding="utf-8") as f:
                metrics = {m["id"]: m for m in json.load(f)["metrics"]}
        except (ValueError, KeyError) as e:
            errors.append("docs/data/benchmarks.json: numbers: unreadable ({})".format(e))
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
