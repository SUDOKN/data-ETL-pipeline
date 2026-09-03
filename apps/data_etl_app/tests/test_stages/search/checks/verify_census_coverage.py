"""Check every judgment file against its packet: did the judge code EVERY form?

A judge that quietly skips forms produces a file that parses, merges, and
reports a precision number computed on a subset it chose itself. That failure
is invisible downstream, so it is checked here before any merge.

Usage: .venv/bin/python checks/verify_census_coverage.py --run <run_id>
"""
from __future__ import annotations
import argparse, json, re, sys
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from paths import run_dir  # noqa: E402

WIN_RE = re.compile(r"^## Window (\d+:\d+)", re.M)
# A form is printed in single quotes, or in DOUBLE quotes when it contains an
# apostrophe (exporter behaviour). Matching only the first drops every
# possessive - 47 forms on run 20260901T013332, silently.
FORM_RE = re.compile(r"""^\s*\d+\.\s+(?:'(?P<sq>.*)'|"(?P<dq>.*)")\s*$""", re.M)


def forms_in(text: str) -> list[str]:
    return [m.group("sq") if m.group("sq") is not None else m.group("dq")
            for m in FORM_RE.finditer(text)]


def packet_forms(text: str) -> Counter:
    """(window, form) -> count, read from the packet's numbered lists."""
    out: Counter = Counter()
    chunks = WIN_RE.split(text)
    for i in range(1, len(chunks), 2):
        window, body = chunks[i], chunks[i + 1]
        body = body.split("### The window text", 1)[0]
        for form in forms_in(body):
            out[(window, form)] += 1
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    args = ap.parse_args()
    rd = run_dir(args.run)
    packets = rd / "raw" / "judge_packets"
    # A part file's packet lives only in the split directory; look there too so
    # a mid-census check does not report every in-flight part as "NO PACKET".
    split = rd / "raw" / "judge_packets_split"
    judgments = rd / "judgments"
    rows, bad = [], 0
    for jf in sorted(judgments.glob("*.jsonl")):
        if jf.name.endswith(".judge2.jsonl"):
            continue
        unit = jf.stem
        pf = packets / f"{unit}.md"
        if not pf.is_file():
            pf = split / f"{unit}.md"
        if not pf.is_file():
            print(f"{unit}: NO PACKET"); bad += 1; continue
        expected = packet_forms(pf.read_text(encoding="utf-8"))
        got: Counter = Counter()
        misses = 0
        for line in jf.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("type") == "miss":
                misses += 1
                continue
            got[(rec.get("window"), rec.get("form"))] += 1
        missing = sum((expected - got).values())
        extra = sum((got - expected).values())
        status = "ok" if not missing and not extra else "MISMATCH"
        if status != "ok":
            bad += 1
        rows.append((unit, sum(expected.values()), sum(got.values()),
                     missing, extra, misses, status))
    w = max((len(r[0]) for r in rows), default=10)
    print(f"{'unit':{w}s} {'packet':>7s} {'coded':>7s} {'missing':>8s} {'extra':>6s} {'misses':>7s}  status")
    for r in rows:
        print(f"{r[0]:{w}s} {r[1]:7d} {r[2]:7d} {r[3]:8d} {r[4]:6d} {r[5]:7d}  {r[6]}")
    print(f"\n{len(rows)} unit(s) checked, {bad} with problems")
    raise SystemExit(1 if bad else 0)


if __name__ == "__main__":
    main()
