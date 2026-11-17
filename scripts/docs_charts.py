#!/usr/bin/env python3
"""Render docs/data/benchmarks.json into SVG bar charts in docs/img/.

Python 3 standard library only. The output is deterministic: the same data
gives byte-identical files. Every colour lives in the embedded <style> block
and is redefined under `prefers-color-scheme: dark`.

Usage: python3 scripts/docs_charts.py [data.json] [out_dir]
"""

import json
import math
import os
import sys
from xml.sax.saxutils import escape

TIME_UNITS = {"ms", "s", "min", "h"}
SIZE_UNITS = {"B", "KB", "MB", "GB", "TB", "KiB", "MiB", "GiB", "TiB"}
SAME_PCT = 2.0

WIDTH = 880
X0 = 260  # left edge of the plot area (labels sit to the left of it)
PLOT_W = 380
BAR_H = 12
BAR_GAP = 3
GROUP_GAP = 14
TOP = 84

STYLE = """
svg{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif}
.bg{fill:#ffffff}
.title{fill:#1f2328;font-size:16px;font-weight:600}
.sub,.tick,.muted{fill:#59636e;font-size:11px}
.label{fill:#1f2328;font-size:12px}
.val{fill:#1f2328;font-size:11px}
.grid{stroke:#d8dee4;stroke-width:1}
.ours.better{fill:#1a7f37}
.ours.worse{fill:#cf222e}
.ours.same,.ours.none{fill:#57606a}
.base{fill:#afb8c1}
.up{fill:#1a7f37}
.down{fill:#cf222e}
@media (prefers-color-scheme: dark){
.bg{fill:#0d1117}
.title,.label,.val{fill:#e6edf3}
.sub,.tick,.muted{fill:#9198a1}
.grid{stroke:#30363d}
.ours.better{fill:#3fb950}
.ours.worse{fill:#f85149}
.ours.same,.ours.none{fill:#9198a1}
.base{fill:#484f58}
.up{fill:#3fb950}
.down{fill:#f85149}
}
"""


def fmt_num(v):
    """1568.6 -> '1,568.6'; 229926073 -> '229,926,073'; 9.45 -> '9.45'."""
    if v is None:
        return "not measured"
    if float(v).is_integer():
        return "{:,}".format(int(v))
    s = "{:,.3f}".format(v).rstrip("0").rstrip(".")
    return s


def fmt_factor(f):
    if f >= 100:
        return "{:,}".format(int(round(f)))
    return "{:.1f}".format(f)


def _words(unit):
    """(word when ours is the smaller value, word when ours is the larger)."""
    if unit in TIME_UNITS:
        return "faster", "slower"
    if unit in SIZE_UNITS:
        return "smaller", "larger"
    if unit.endswith("/s"):
        return "lower", "higher"
    return "fewer", "more"


def change(metric):
    """Change of ours against the baseline.

    Returns {"factor", "pct", "direction", "text"}. direction is "better",
    "worse", "same" (|pct| < 2) or "none" (a side is not measured).
    """
    ours, base = metric.get("ours"), metric.get("baseline")
    if ours is None or base is None:
        return {"factor": None, "pct": None, "direction": "none", "text": "not measured"}
    if base == 0:
        if ours == 0:
            return {"factor": 1.0, "pct": 0.0, "direction": "same", "text": "same"}
        return {"factor": None, "pct": None, "direction": "worse" if metric["better"] != "higher" else "better",
                "text": "{} vs none".format(fmt_num(ours))}
    pct = (ours - base) / base * 100.0
    lo, hi = min(ours, base), max(ours, base)
    factor = hi / lo if lo > 0 else None
    smaller_word, larger_word = _words(metric.get("unit", ""))
    word = smaller_word if ours < base else larger_word

    better = metric.get("better", "lower")
    if abs(pct) < SAME_PCT:
        direction = "same"
    elif better == "lower":
        direction = "better" if ours < base else "worse"
    elif better == "higher":
        direction = "better" if ours > base else "worse"
    else:  # equal: any real difference counts against us
        direction = "worse"

    if direction == "same":
        text = "about the same" if ours == base else "about the same ({:.1f}% {})".format(abs(pct), word)
    elif factor is not None and factor >= 1.1:
        text = "{}× {}".format(fmt_factor(factor), word)
    else:
        text = "{:.0f}% {}".format(abs(pct), word)
    return {"factor": factor, "pct": pct, "direction": direction, "text": text}


def _scale(chart, values):
    pos = [v for v in values if v is not None and v > 0]
    mode = chart.get("scale", "auto")
    if mode == "auto":
        mode = "log" if pos and max(pos) / min(pos) > 10 else "linear"
    if mode == "log":
        if not pos:
            pos = [1]
        lo = 10 ** math.floor(math.log10(min(pos)))
        if min(pos) / lo < 1.5:  # keep the smallest bar visible
            lo /= 10
        hi = 10 ** math.ceil(math.log10(max(pos)))
        if hi <= lo:
            hi = lo * 10
        span = math.log10(hi) - math.log10(lo)

        def x(v):
            if v is None or v <= 0:
                return 0.0
            return PLOT_W * (math.log10(v) - math.log10(lo)) / span

        ticks = []
        t = lo
        while t <= hi * 1.0001:
            ticks.append(t)
            t *= 10
        return "log", x, ticks
    top = max([v for v in values if v is not None] + [0])
    if top <= 0:
        top = 1
    raw = top / 5
    mag = 10 ** math.floor(math.log10(raw))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= raw)
    hi = math.ceil(top / step) * step

    def x(v):
        if v is None or v <= 0:
            return 0.0
        return PLOT_W * v / hi

    ticks = [i * step for i in range(int(round(hi / step)) + 1)]
    return "linear", x, ticks


def _tick_label(t):
    if t >= 1_000_000_000:
        return fmt_num(t / 1e9) + "G"
    if t >= 1_000_000:
        return fmt_num(t / 1e6) + "M"
    if t >= 10_000:
        return fmt_num(t / 1e3) + "k"
    return fmt_num(round(t, 6))


def render(chart, metrics):
    """Return the SVG text for one chart."""
    rows = [metrics[mid] for mid in chart["metrics"]]
    values = []
    for m in rows:
        values += [m.get("ours"), m.get("baseline")]
    mode, x, ticks = _scale(chart, values)
    units = sorted({m["unit"] for m in rows})
    interim = any(m.get("interim") for m in rows)
    group_h = 2 * BAR_H + BAR_GAP + GROUP_GAP
    plot_h = len(rows) * group_h
    height = TOP + plot_h + 34

    out = []
    a = out.append
    title = chart["title"]
    sub = "{} · {} scale · ▲ better ▼ worse than the baseline".format(", ".join(units), mode)
    if interim:
        sub += " · interim numbers"
    desc_parts = []
    for m in rows:
        c = change(m)
        def val(v, unit=m["unit"]):
            return "not measured" if v is None else "{} {}".format(fmt_num(v), unit)
        desc_parts.append("{}: kda-indexer {}, {} {}{}".format(
            m["label"], val(m.get("ours")), m["baseline_name"], val(m.get("baseline")),
            "" if c["direction"] == "none" else ", " + c["text"]))
    cid = escape(chart["id"])
    a('<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
      'role="img" aria-labelledby="{c}-t {c}-d">'.format(w=WIDTH, h=height, c=cid))
    a('<title id="{}-t">{}</title>'.format(cid, escape(title)))
    a('<desc id="{}-d">{}</desc>'.format(cid, escape("; ".join(desc_parts) + ".")))
    a("<style>{}</style>".format(STYLE))
    a('<rect class="bg" x="0" y="0" width="{}" height="{}" rx="6"/>'.format(WIDTH, height))
    a('<text class="title" x="16" y="28">{}</text>'.format(escape(title)))
    a('<text class="sub" x="16" y="48">{}</text>'.format(escape(sub)))
    # legend
    ly = 62
    lx = 16
    present = {change(m)["direction"] for m in rows}
    legend = [("ours " + d, t) for d, t in (("better", "kda-indexer, better"), ("worse", "kda-indexer, worse"),
                                             ("same", "kda-indexer, about the same"),
                                             ("none", "kda-indexer, no baseline")) if d in present]
    if any(m.get("baseline") is not None for m in rows):
        legend.append(("base", "baseline"))
    for cls, text in legend:
        a('<rect class="{}" x="{}" y="{}" width="10" height="10" rx="2"/>'.format(cls, lx, ly))
        a('<text class="muted" x="{}" y="{}">{}</text>'.format(lx + 14, ly + 9, escape(text)))
        lx += 24 + int(len(text) * 6.2)

    # axis and grid
    a('<g class="axis {}">'.format(mode))
    for t in ticks:
        gx = X0 + x(t) if t > 0 else X0
        a('<line class="grid" x1="{:.1f}" y1="{}" x2="{:.1f}" y2="{}"/>'.format(gx, TOP - 6, gx, TOP + plot_h - 6))
        a('<text class="tick" x="{:.1f}" y="{}" text-anchor="middle">{}</text>'.format(
            gx, TOP + plot_h + 10, escape(_tick_label(t))))
    a("</g>")

    for i, m in enumerate(rows):
        y = TOP + i * group_h
        c = change(m)
        a('<g class="metric" id="{}">'.format(escape(m["id"])))
        a('<text class="label" x="{}" y="{}" text-anchor="end">{}</text>'.format(
            X0 - 10, y + BAR_H + 4, escape(m["label"])))
        # ours
        ow = x(m.get("ours"))
        a('<rect class="ours {}" x="{}" y="{}" width="{:.1f}" height="{}" rx="2"/>'.format(
            c["direction"], X0, y, ow, BAR_H))
        glyph, gcls = {"better": ("▲", "up"), "worse": ("▼", "down")}.get(c["direction"], ("", ""))
        tx = X0 + ow + 6
        text = "{} {}".format(fmt_num(m.get("ours")), m["unit"]) if m.get("ours") is not None else "not measured"
        a('<text class="val" x="{:.1f}" y="{}">{}'.format(tx, y + BAR_H - 2, escape(text)))
        if c["direction"] != "none":
            if glyph:
                a(' <tspan class="{}">{}</tspan>'.format(gcls, glyph))
            a(" {}".format(escape(c["text"])))
        a("</text>")
        # baseline
        by = y + BAR_H + BAR_GAP
        if m.get("baseline") is None:
            a('<text class="muted" x="{}" y="{}">{}: not measured</text>'.format(
                X0, by + BAR_H - 2, escape(m["baseline_name"])))
        else:
            bw = x(m["baseline"])
            a('<rect class="base" x="{}" y="{}" width="{:.1f}" height="{}" rx="2"/>'.format(X0, by, bw, BAR_H))
            a('<text class="muted" x="{:.1f}" y="{}">{} {} · {}</text>'.format(
                X0 + bw + 6, by + BAR_H - 2, escape(fmt_num(m["baseline"])), escape(m["unit"]),
                escape(m["baseline_name"])))
        a("</g>")
    a("</svg>")
    return "\n".join(out) + "\n"


def main(data_path, out_dir):
    """Render every chart in data_path into out_dir; return the written paths."""
    with open(data_path, encoding="utf-8") as f:
        data = json.load(f)
    metrics = {m["id"]: m for m in data["metrics"]}
    if len(metrics) != len(data["metrics"]):
        raise SystemExit("duplicate metric id in {}".format(data_path))
    os.makedirs(out_dir, exist_ok=True)
    written = []
    for chart in data["charts"]:
        missing = [mid for mid in chart["metrics"] if mid not in metrics]
        if missing:
            raise SystemExit("chart {}: unknown metrics {}".format(chart["id"], missing))
        path = os.path.join(out_dir, chart["file"])
        svg = render(chart, metrics)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(svg)
        written.append(path)
    return written


if __name__ == "__main__":
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data = sys.argv[1] if len(sys.argv) > 1 else os.path.join(root, "docs", "data", "benchmarks.json")
    out = sys.argv[2] if len(sys.argv) > 2 else os.path.join(root, "docs", "img")
    for p in main(data, out):
        print(os.path.relpath(p, root))
