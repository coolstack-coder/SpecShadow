"""Draw the README / post figures from experiments/results*.json.

Outputs plain SVG (paper style) into docs/. No dependencies beyond the
standard library.
"""
import json
import os
import sys
from collections import defaultdict
from xml.sax.saxutils import escape

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXP = os.path.join(ROOT, "experiments")
DOCS = os.path.join(ROOT, "docs")

PAPER, INK, MUTE, RULE, RED, FILL = "#fbfaf6", "#141414", "#5c5c5c", "#d9d6cc", "#b42318", "#efece3"
SANS = "Helvetica Neue, Helvetica, Arial, sans-serif"
MONO = "Menlo, SF Mono, Consolas, monospace"


def load():
    """results.json is the main run; results-sweep.json and results-suite.json,
    when present, replace its clock sweep and AegisDB-suite sections."""
    def read(name):
        p = os.path.join(EXP, name)
        return json.load(open(p)) if os.path.exists(p) else {}
    main, sweep, suite_run = read("results.json"), read("results-sweep.json"), read("results-suite.json")
    runs = main.get("runs") or []
    if sweep.get("runs"):
        runs = [r for r in runs if r["group"] != "sweep"] + sweep["runs"]
    suite = {s["bug"]: s for s in (suite_run.get("aegis_suite") or main.get("aegis_suite") or [])}
    # repeated runs of AegisDB's suite: {"bug": {"runs": n, "failed": k, "tests": [...]}}
    rep = read("suite-repeat.json")
    for bug, v in rep.items():
        suite[bug] = {"bug": bug, "runs": v["runs"], "failed": v["failed"], "tests": v["tests"],
                      "passed": v["failed"] == 0}
    return runs, suite


class SVG:
    def __init__(self, w, h):
        self.w, self.h, self.parts = w, h, []

    def add(self, s):
        self.parts.append(s)

    def text(self, x, y, s, size=16, fill=INK, weight="normal", anchor="start", font=SANS):
        self.add(f'<text x="{x}" y="{y}" font-family="{font}" font-size="{size}" fill="{fill}" '
                 f'font-weight="{weight}" text-anchor="{anchor}">{escape(str(s))}</text>')

    def rect(self, x, y, w, h, fill="none", stroke=INK, sw=1.5, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.add(f'<rect x="{x}" y="{y}" width="{max(w,0)}" height="{max(h,0)}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{d}/>')

    def line(self, x1, y1, x2, y2, stroke=INK, sw=1.5, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.add(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{stroke}" stroke-width="{sw}"{d}/>')

    def save(self, name):
        os.makedirs(DOCS, exist_ok=True)
        body = "\n".join(self.parts)
        svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" '
               f'viewBox="0 0 {self.w} {self.h}"><rect width="100%" height="100%" fill="{PAPER}"/>\n{body}\n</svg>\n')
        open(os.path.join(DOCS, name), "w").write(svg)
        print("wrote docs/" + name)


BUG_LABEL = {
    "double-vote": "votes twice in one term",
    "no-up-to-date": "votes for a stale log",
    "commit-old-term": "commits old-term entries (fig. 8)",
    "commit-overrun": "follower commits past the match",
    "long-lease": "lease 2x election timeout",
    "stale-term": "accepts an old leader's append",
    "vote-not-synced": "votes before the vote is on disk",
}


def fig_detection(runs, suite):
    bugs = [b for b in BUG_LABEL if any(r["group"] == "bugs" and r.get("bug") == b for r in runs)]
    if not bugs:
        return
    W, row = 980, 58
    H = 150 + row * len(bugs) + 70
    s = SVG(W, H)
    s.text(30, 44, "Seven planted raft bugs: who notices?", 26, weight="bold")
    s.text(30, 72, "AegisDB's own 9 raft tests vs. SpecShadow on 20 random chaos runs per bug", 15, MUTE)
    x_suite, x_fired, x_bar = 360, 520, 640
    s.text(30, 118, "planted bug", 14, MUTE, font=MONO)
    s.text(x_suite, 118, "AegisDB tests fail", 14, MUTE, font=MONO)
    s.text(x_fired, 118, "bug fired", 14, MUTE, font=MONO)
    s.text(x_bar, 118, "caught by SpecShadow", 14, MUTE, font=MONO)
    s.line(30, 130, W - 30, 130)
    for k, b in enumerate(bugs):
        y = 130 + row * k
        rs = [r for r in runs if r["group"] == "bugs" and r.get("bug") == b]
        caught = sum(1 for r in rs if not r["ok"] and not r.get("error"))
        fired = sum(1 for r in rs if r.get("bug_fired", 0) > 0)
        rules = defaultdict(int)
        for r in rs:
            if not r["ok"] and r.get("rule"):
                rules[r["rule"]] += 1
        s.text(30, y + 26, b, 16, weight="bold", font=MONO)
        s.text(30, y + 46, BUG_LABEL[b], 13, MUTE)
        su = suite.get(b)
        if su and "runs" in su:
            s.text(x_suite, y + 34, f"{su['failed']}/{su['runs']} runs", 16,
                   RED if su["failed"] == 0 else INK, "bold", font=MONO)
        elif su:
            s.text(x_suite, y + 34, "missed" if su["passed"] else "caught", 16,
                   RED if su["passed"] else INK, "bold", font=MONO)
        has_fired = any("bug_fired" in r for r in rs)
        s.text(x_fired, y + 34, f"{fired}/{len(rs)}" if has_fired else "-", 16, INK, font=MONO)
        bw = 220
        s.rect(x_bar, y + 16, bw, 22, fill="white", stroke=INK, sw=1)
        s.rect(x_bar, y + 16, bw * caught / max(len(rs), 1), 22, fill=INK, stroke=INK, sw=1)
        s.text(x_bar + bw + 12, y + 33, f"{caught}/{len(rs)}", 16, weight="bold", font=MONO)
        if rules:
            top = max(rules, key=rules.get)
            s.text(x_bar, y + 54, top, 12, MUTE, font=MONO)
        s.line(30, y + row, W - 30, y + row, RULE, 1)
    s.text(30, H - 26, "\"bug fired\" = runs where the buggy line actually changed what a node did.", 13, MUTE)
    s.save("fig-detection.svg")


def fig_clock(runs):
    sw = [r for r in runs if r["group"] == "sweep" and not r.get("error")]
    if not sw:
        return
    by = defaultdict(list)
    for r in sw:
        by[round(r["rate"], 2)].append(r)
    rates = sorted(by, reverse=True)
    W, H = 980, 560
    s = SVG(W, H)
    s.text(30, 44, "Slow the leader's clock, and lease reads go stale", 26, weight="bold")
    s.text(30, 72, "AegisDB, unmodified. Leader partitioned every 0.5 s while its clock runs at the given speed.", 15, MUTE)
    x0, x1, y0, y1 = 110, W - 60, 440, 120
    s.line(x0, y0, x1, y0)
    s.line(x0, y0, x0, y1)
    for p in (0, 25, 50, 75, 100):
        y = y0 - (y0 - y1) * p / 100
        s.line(x0 - 6, y, x0, y)
        s.text(x0 - 12, y + 5, f"{p}%", 13, MUTE, anchor="end", font=MONO)
        if p:
            s.line(x0, y, x1, y, RULE, 1)
    lo, hi = 0.25, 1.02
    X = lambda r: x1 - (x1 - x0) * (hi - r) / (hi - lo)
    # theory: lease (0.75 ET of the leader's clock) outlives the earliest new
    # election (1 ET of the followers' clocks) once the leader runs below 0.75x
    xt = X(0.75)
    s.line(xt, y1 - 10, xt, y0, RED, 1.5, "6 5")
    s.text(xt + 8, y1 + 4, "worst case: unsafe below 0.75x", 13, RED, font=MONO)
    pts = []
    for r in rates:
        rs = by[r]
        frac = 100 * sum(1 for x in rs if x["stale"] > 0) / len(rs)
        pts.append((X(r), y0 - (y0 - y1) * frac / 100, r, frac, rs))
    s.add('<polyline fill="none" stroke="#141414" stroke-width="3" points="' +
          " ".join(f"{x:.1f},{y:.1f}" for x, y, *_ in pts) + '"/>')
    for x, y, r, frac, rs in pts:
        s.add(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6" fill="{RED if frac else INK}"/>')
        s.text(x, y0 + 26, f"{r:g}x", 13, MUTE, anchor="middle", font=MONO)
        stale = sum(x2["stale"] for x2 in rs)
        reads = sum(x2["reads"] for x2 in rs)
        if stale:
            s.text(x, y - 14, f"{stale}", 12, RED, anchor="middle", font=MONO)
    s.text((x0 + x1) / 2, y0 + 56, "leader clock speed (1x = real time)", 14, anchor="middle")
    s.text(x0 - 60, y1 - 22, "runs with at least one stale read", 13, MUTE)
    n = len(by[rates[0]])
    s.text(30, H - 24, f"{n} runs per speed, 3 s each. Red numbers: stale reads found in those runs (TLC stops at the first; the count is from replaying the same rule).", 12.5, MUTE)
    s.save("fig-clock-rate.svg")


def fig_pause(runs):
    groups = [("correct", "normal clock", "chaos, 20 runs"),
              ("pause", "read stalls after lease check", "3x election timeout"),
              ("freeze", "leader's clock stops", "suspended VM")]
    data = []
    for g, label, sub in groups:
        rs = [r for r in runs if r["group"] == g and not r.get("error")]
        if rs:
            data.append((label, sub, sum(1 for r in rs if not r["ok"]), len(rs),
                         sum(r["stale"] for r in rs)))
    if not data:
        return
    W, H = 980, 420
    s = SVG(W, H)
    s.text(30, 44, "A paused process is safe. A stopped clock is not.", 26, weight="bold")
    s.text(30, 72, "Runs where TLC found a linearizability violation (StaleRead), AegisDB unmodified", 15, MUTE)
    y = 120
    for label, sub, bad, n, stale in data:
        s.text(30, y + 26, label, 18, weight="bold")
        s.text(30, y + 48, sub, 13, MUTE, font=MONO)
        bw = 420
        s.rect(430, y + 10, bw, 34, fill="white", stroke=INK, sw=1)
        s.rect(430, y + 10, bw * bad / n, 34, fill=RED if bad else INK, stroke=INK, sw=1)
        s.text(430 + bw + 14, y + 34, f"{bad}/{n}", 18, RED if bad else INK, "bold", font=MONO)
        y += 86
    s.save("fig-pause-vs-freeze.svg")


def fig_architecture():
    W, H = 1100, 330
    s = SVG(W, H)
    s.add('<defs><marker id="a" markerWidth="12" markerHeight="12" refX="11" refY="6" orient="auto">'
          '<path d="M0,0 L12,6 L0,12" fill="none" stroke="#141414" stroke-width="2"/></marker></defs>')
    boxes = [
        (20, "AegisDB raft", ["real Go code", "3 nodes, one process", "faults + clock control"]),
        (290, "trace", ["one event per", "state change", "global causal order"]),
        (560, "TLA+ spec", ["TraceRaft.tla", "13 actions", "27 safety rules"]),
        (830, "TLC", ["replays every step", "first broken rule", "-> event + Go line"]),
    ]
    for x, title, lines in boxes:
        s.rect(x, 80, 220, 170, fill="white", stroke=INK, sw=2.5)
        s.text(x + 110, 120, title, 22, weight="bold", anchor="middle")
        for k, l in enumerate(lines):
            s.text(x + 110, 160 + 26 * k, l, 15, MUTE, anchor="middle", font=MONO)
    for x in (246, 516, 786):
        s.add(f'<line x1="{x}" y1="165" x2="{x+36}" y2="165" stroke="#141414" stroke-width="2.5" marker-end="url(#a)"/>')
    s.text(20, 44, "SpecShadow: check what the code actually did, step by step", 22, weight="bold")
    s.text(20, 296, "Tests check outcomes. Trace validation checks every intermediate step against the protocol.", 15, MUTE)
    s.save("fig-architecture.svg")


if __name__ == "__main__":
    runs, suite = load()
    fig_architecture()
    fig_detection(runs, suite)
    fig_clock(runs)
    fig_pause(runs)
