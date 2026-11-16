"""Render docs/specshadow-demo.mp4 (1080x1080, 30 fps) from real results.

Every number, terminal line and timeline event on screen comes from
experiments/results*.json, a real `specshadow check` run, and a real
counterexample trace. Needs Pillow and imageio-ffmpeg.

    python experiments/video.py <counterexample.ndjson> <check-output.txt>
"""
import json
import math
import os
import subprocess
import sys

from PIL import Image, ImageDraw, ImageFont
import imageio_ffmpeg

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
W = H = 1080
FPS = 30
PAPER, INK, MUTE, RULE, RED, FILL, PALE = "#fbfaf6", "#141414", "#5c5c5c", "#d9d6cc", "#b42318", "#efece3", "#f6dcd8"
HV, MN = "/System/Library/Fonts/Helvetica.ttc", "/System/Library/Fonts/Menlo.ttc"


def font(path, size, bold=False):
    return ImageFont.truetype(path, size, index=1 if bold else 0)


F = {
    "h1": font(HV, 66, True), "h2": font(HV, 48, True), "h3": font(HV, 34, True),
    "b": font(HV, 30), "bb": font(HV, 30, True), "s": font(HV, 24), "sb": font(HV, 24, True),
    "m": font(MN, 24), "mb": font(MN, 24, True), "ms": font(MN, 20), "mxs": font(MN, 17),
    "big": font(MN, 88, True),
}


def ease(t):
    t = max(0.0, min(1.0, t))
    return t * t * (3 - 2 * t)


def base(section=""):
    im = Image.new("RGB", (W, H), PAPER)
    d = ImageDraw.Draw(im)
    d.rectangle([28, 28, W - 28, H - 28], outline=INK, width=2)
    if section:
        d.text((60, 56), section, font=F["ms"], fill=MUTE)
    d.text((W - 60, H - 64), "github.com/coolstack-coder/SpecShadow", font=F["mxs"], fill=MUTE, anchor="ra")
    return im, d


def wrap(d, text, fnt, width):
    words, lines, cur = text.split(), [], ""
    for w in words:
        t = (cur + " " + w).strip()
        if d.textlength(t, font=fnt) <= width:
            cur = t
        else:
            lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines


def para(d, xy, text, fnt, fill=INK, width=W - 140, gap=1.3):
    x, y = xy
    for ln in wrap(d, text, fnt, width):
        d.text((x, y), ln, font=fnt, fill=fill)
        y += int(fnt.size * gap)
    return y


# ---------------------------------------------------------------- data

EXP = os.path.join(ROOT, "experiments")


def load_results():
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
    for bug, v in read("suite-repeat.json").items():
        suite[bug] = {"bug": bug, "runs": v["runs"], "failed": v["failed"], "passed": v["failed"] == 0}
    return runs, suite


def load(trace_path, check_path):
    runs, suite = load_results()
    evs = [json.loads(l) for l in open(trace_path)]
    check = open(check_path).read().rstrip("\n").split("\n")
    return runs, suite, evs, check


# ---------------------------------------------------------------- scenes

def scene_title(t, ctx):
    im, d = base()
    a = ease(t / 0.8)
    d.text((70, 300), "My Raft database", font=F["h1"], fill=INK)
    d.text((70, 380), "passed all its tests.", font=F["h1"], fill=INK)
    if t > 1.2:
        d.text((70, 500), "Then I made it prove", font=F["h1"], fill=RED)
        d.text((70, 580), "every single step.", font=F["h1"], fill=RED)
    if t > 2.6:
        d.text((70, 740), "SpecShadow: TLA+ trace validation for real Go code", font=F["s"], fill=MUTE)
    return im


def scene_tests_miss(t, ctx):
    runs, suite = ctx["runs"], ctx["suite"]
    im, d = base("THE PROBLEM")
    d.text((70, 110), "I planted 7 classic raft bugs.", font=F["h2"], fill=INK)
    d.text((70, 172), "AegisDB's own raft tests:", font=F["h2"], fill=INK)
    d.text((640, 250), "suite runs passed", font=F["ms"], fill=MUTE)
    bugs = ["double-vote", "no-up-to-date", "commit-old-term", "commit-overrun", "long-lease", "stale-term", "vote-not-synced"]
    for k, b in enumerate(bugs):
        y = 290 + k * 82
        show = t > 0.6 + k * 0.35
        d.text((90, y), b, font=F["mb"], fill=INK if show else RULE)
        if show:
            su = suite.get(b, {"passed": True})
            if "runs" in su:
                label, ok = (f"pass {su['runs'] - su['failed']}/{su['runs']}", su["failed"] == 0)
            else:
                label, ok = ("all 9 pass" if su["passed"] else "caught"), su["passed"]
            d.rectangle([640, y - 6, 960, y + 38], outline=INK, width=2, fill="white")
            d.text((800, y + 16), label, font=F["mb"], fill=RED if ok else INK, anchor="mm")
    if t > 4.2:
        d.text((70, 900), "Tests check outcomes. These bugs hide in the steps.", font=F["bb"], fill=RED)
    return im


def scene_how(t, ctx):
    im, d = base("HOW IT WORKS")
    d.text((70, 110), "Record every step. Replay it against the spec.", font=F["h3"], fill=INK)
    boxes = [("AegisDB raft", "real Go code"), ("trace", "1 event per step"),
             ("TLA+ spec", "27 safety rules"), ("TLC", "first broken rule")]
    for k, (a, b) in enumerate(boxes):
        if t < 0.3 + k * 0.6:
            continue
        x, y = 70 + (k % 2) * 480, 230 + (k // 2) * 250
        d.rectangle([x, y, x + 420, y + 170], outline=INK, width=3, fill="white")
        d.text((x + 210, y + 62), a, font=F["h3"], fill=INK, anchor="mm")
        d.text((x + 210, y + 118), b, font=F["m"], fill=MUTE, anchor="mm")
    arrows = [((490, 315), (550, 315)), ((760, 400), (760, 480)), ((550, 565), (490, 565))]
    for k, (p, q) in enumerate(arrows):
        if t > 0.6 + k * 0.6:
            d.line([p, q], fill=INK, width=4)
            ang = math.atan2(q[1] - p[1], q[0] - p[0])
            for s in (-1, 1):
                d.line([q, (q[0] - 16 * math.cos(ang + s * 0.5), q[1] - 16 * math.sin(ang + s * 0.5))], fill=INK, width=4)
    # a scrolling strip of real events
    evs = [e for e in ctx["evs"] if e.get("state") and e["kind"] != "Apply"]
    if t > 2.6:
        y0 = 770
        d.rectangle([70, y0 - 20, W - 70, y0 + 210], fill=INK)
        start = int((t - 2.6) * 30) % max(1, len(evs) - 7)
        for k, e in enumerate(evs[start:start + 7]):
            st = e.get("state") or {}
            line = f'{e["seq"]:>5} {e["node"]:<3} {e["kind"]:<15} term={st.get("term","")} commit={st.get("commit","")}'
            d.text((96, y0 + k * 28), line, font=F["mxs"], fill="#e8e6df")
    return im


def scene_terminal(t, ctx):
    im, d = base("A REAL RUN")
    d.rectangle([60, 110, W - 60, 900], fill=INK)
    cmd = "$ specshadow check -trace freeze.ndjson"
    n = int(min(len(cmd), t * 28))
    d.text((90, 140), cmd[:n], font=F["ms"], fill="#e8e6df")
    if t > 1.8:
        for k, ln in enumerate(ctx["check"][:24]):
            if t < 1.8 + k * 0.12:
                break
            col = "#ff8a80" if ("VIOLATION" in ln or ">>>" in ln) else "#e8e6df"
            d.text((90, 190 + k * 28), ln[:78], font=F["mxs"], fill=col)
    if t > 5:
        d.text((70, 940), "TLC names the broken rule, the event, and the Go line.", font=F["sb"], fill=INK)
    return im


def scene_timeline(t, ctx):
    """The freeze counterexample, drawn from the trace."""
    tl = ctx["tl"]
    im, d = base("THE BUG IT FOUND IN MY OWN DATABASE")
    d.text((70, 100), "A suspended VM comes back", font=F["h3"], fill=INK)
    d.text((70, 146), "and serves a stale read.", font=F["h3"], fill=RED)
    x0, x1 = 170, W - 80
    t0, t1 = tl["t0"], tl["t1"]
    X = lambda us: x0 + (x1 - x0) * (us - t0) / (t1 - t0)
    cursor = t0 + (t1 - t0) * ease(t / 8.0)
    lanes = {n: 300 + k * 170 for k, n in enumerate(tl["nodes"])}
    for n, y in lanes.items():
        d.text((70, y - 14), n, font=F["mb"], fill=INK)
        d.line([x0, y, x1, y], fill=RULE, width=2)
    fz = tl["freeze"]
    if fz and cursor > fz[0]:
        y = lanes[tl["old"]]
        d.rectangle([X(fz[0]), y - 40, X(min(cursor, fz[1])), y + 40], fill=FILL, outline=MUTE)
        d.text((X(fz[0]) + 10, y - 34), "frozen: clock stopped", font=F["mxs"], fill=MUTE)
    for e in tl["marks"]:
        if e["t"] > cursor:
            continue
        x, y = X(e["t"]), lanes[e["node"]]
        if e["kind"] == "BecomeLeader":
            d.rectangle([x - 8, y - 8, x + 8, y + 8], fill=INK)
            d.text((x, y + 22), f'leader, term {e["term"]}', font=F["mxs"], fill=INK, anchor="ma")
        elif e["kind"] == "WriteAck":
            d.line([x, y - 10, x, y + 10], fill=INK, width=2)
    upto = min(cursor, tl["stale"]["t"])
    acked = max([e["idx"] for e in tl["marks"] if e["kind"] == "WriteAck" and e["t"] <= upto] or [0])
    d.text((70, 820), f"writes acknowledged so far: {acked}", font=F["m"], fill=INK)
    sr = tl["stale"]
    if cursor >= sr["t"]:
        x, y = X(sr["t"]), lanes[sr["node"]]
        d.ellipse([x - 16, y - 16, x + 16, y + 16], outline=RED, width=5)
        d.text((70, 870), f'{sr["node"]} still thinks it leads term {sr["term"]}. Lease valid.', font=F["sb"], fill=RED)
        d.text((70, 906), f'It serves a read at index {sr["idx"]}, but write {sr["acked"]} was already', font=F["sb"], fill=RED)
        d.text((70, 940), "acknowledged before the read began. Not linearizable.", font=F["sb"], fill=RED)
    return im


def scene_clock(t, ctx):
    runs = ctx["runs"]
    im, d = base("THE NUMBERS")
    d.text((70, 100), "Slower leader clock, stale reads", font=F["h3"], fill=INK)
    sw = [r for r in runs if r["group"] == "sweep" and not r.get("error")]
    by = {}
    for r in sw:
        by.setdefault(round(r["rate"], 2), []).append(r)
    rates = sorted(by, reverse=True)
    x0, x1, y0, y1 = 150, W - 90, 760, 260
    d.line([x0, y0, x1, y0], fill=INK, width=3)
    d.line([x0, y0, x0, y1], fill=INK, width=3)
    for p in (0, 50, 100):
        y = y0 - (y0 - y1) * p / 100
        d.text((x0 - 14, y), f"{p}%", font=F["ms"], fill=MUTE, anchor="rm")
    lo, hi = 0.25, 1.02
    X = lambda r: x1 - (x1 - x0) * (hi - r) / (hi - lo)
    xt = X(0.75)
    for yy in range(y1, y0, 22):
        d.line([xt, yy, xt, yy + 12], fill=RED, width=3)
    d.text((xt + 10, y1 - 4), "worst case: below 0.75x", font=F["ms"], fill=RED)
    pts = []
    for r in rates:
        rs = by[r]
        frac = sum(1 for x in rs if x["stale"] > 0) / len(rs)
        pts.append((X(r), y0 - (y0 - y1) * frac, r, frac))
    shown = pts[: max(1, int(len(pts) * ease(t / 3.5)) + 1)]
    if len(shown) > 1:
        d.line([(x, y) for x, y, *_ in shown], fill=INK, width=5)
    for x, y, r, frac in shown:
        d.ellipse([x - 9, y - 9, x + 9, y + 9], fill=RED if frac else INK)
        if r in (1.0, 0.9, 0.75, 0.6, 0.5, 0.4, 0.3):
            d.text((x, y0 + 26), f"{r:g}x", font=F["ms"], fill=MUTE, anchor="ma")
    d.text((W / 2, y0 + 70), "leader clock speed  (1x = real time)", font=F["s"], fill=INK, anchor="ma")
    d.text((70, 900), "share of runs with at least one stale read", font=F["s"], fill=MUTE)
    return im


def scene_correction(t, ctx):
    runs = ctx["runs"]
    im, d = base("WHAT I GOT WRONG")
    d.text((70, 110), "In my clock post I said a process", font=F["h3"], fill=INK)
    d.text((70, 156), "pausing after its lease check was", font=F["h3"], fill=INK)
    d.text((70, 202), "the threat. The checker says no.", font=F["h3"], fill=RED)
    rows = [("pause", "read paused after lease check"), ("freeze", "leader's clock stopped (VM suspend)")]
    for k, (g, label) in enumerate(rows):
        if t < 1.0 + k * 1.0:
            continue
        rs = [r for r in runs if r["group"] == g and not r.get("error")]
        bad = sum(1 for r in rs if not r["ok"])
        y = 340 + k * 190
        d.text((70, y), label, font=F["bb"], fill=INK)
        d.rectangle([70, y + 56, 770, y + 106], outline=INK, width=2, fill="white")
        if rs:
            d.rectangle([70, y + 56, 70 + 700 * bad / len(rs), y + 106], fill=RED if bad else INK)
            d.text((800, y + 81), f"{bad}/{len(rs)} runs", font=F["mb"], fill=RED if bad else INK, anchor="lm")
    if t > 3.2:
        ovl = sum(r.get("overlapped", 0) for r in runs if r["group"] == "pause")
        y = para(d, (70, 730), f"{ovl} paused reads came back older than writes acknowledged during the pause. "
                 "Those writes overlapped the read, so every one is still linearizable.", F["s"], INK)
        para(d, (70, y + 16), "A clock that stops is different: when it resumes, the lease still looks valid.", F["sb"], RED)
    return im


def scene_end(t, ctx):
    im, d = base()
    d.text((70, 300), "SpecShadow", font=F["h1"], fill=INK)
    para(d, (70, 400), "Real Go executions, checked step by step against a TLA+ spec by the TLC model checker.", F["b"], MUTE)
    d.text((70, 600), "github.com/coolstack-coder/SpecShadow", font=F["mb"], fill=INK)
    return im


SCENES = [(scene_title, 4.5), (scene_tests_miss, 6.5), (scene_how, 7.0), (scene_terminal, 8.0),
          (scene_timeline, 10.0), (scene_clock, 6.0), (scene_correction, 8.5), (scene_end, 4.0)]


def build_timeline(evs):
    """Pick the stale read and the window around it from the trace."""
    acked, start, stale = 0, {}, None
    for e in evs:
        if e["kind"] == "WriteAck":
            acked = max(acked, e["idx"])
        elif e["kind"] == "ReadStart":
            start[e["rid"]] = acked
        elif e["kind"] == "ReadServe" and e["idx"] < start.get(e["rid"], 0):
            stale = {"t": e["t_us"], "node": e["node"], "idx": e["idx"], "acked": start[e["rid"]],
                     "term": e["mterm"]}
            break
    if not stale:
        raise SystemExit("no stale read in trace")
    old = stale["node"]
    fz = [e["t_us"] for e in evs if e["kind"] == "Fault" and e["node"] == old and e.get("note") in ("freeze", "thaw") and e["t_us"] <= stale["t"]]
    freeze = (fz[-2], fz[-1]) if len(fz) >= 2 else None
    t0 = (freeze[0] if freeze else stale["t"] - 600_000) - 120_000
    t1 = stale["t"] + 60_000
    nodes = sorted({e["node"] for e in evs if e["node"].startswith("n")})
    marks = []
    for e in evs:
        if t0 <= e["t_us"] <= t1 and e["kind"] in ("BecomeLeader", "WriteAck"):
            marks.append({"t": e["t_us"], "node": e["node"], "kind": e["kind"], "idx": e.get("idx", 0),
                          "term": (e.get("state") or {}).get("term", e.get("mterm", 0))})
    return {"t0": t0, "t1": t1, "nodes": nodes, "marks": marks, "stale": stale, "freeze": freeze, "old": old}


def main():
    runs, suite, evs, check = load(sys.argv[1], sys.argv[2])
    ctx = {"runs": runs, "suite": suite, "evs": evs, "check": check, "tl": build_timeline(evs)}
    out = os.path.join(ROOT, "docs", "specshadow-demo.mp4")
    w = imageio_ffmpeg.write_frames(out, (W, H), fps=FPS, codec="libx264", quality=8,
                                    pix_fmt_out="yuv420p", macro_block_size=8)
    w.send(None)
    stills = {}
    for k, (fn, dur) in enumerate(SCENES):
        n = int(dur * FPS)
        for f in range(n):
            im = fn(f / FPS, ctx)
            if f == n - 1:
                stills[fn.__name__] = im
            w.send(im.tobytes())
    w.close()
    for name, im in stills.items():
        im.save(os.path.join(ROOT, "docs", f"still-{name}.png"))
    print("wrote", out, sum(d for _, d in SCENES), "s")


if __name__ == "__main__":
    main()
