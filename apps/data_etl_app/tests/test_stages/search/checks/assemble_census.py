"""Reconcile part-files into one judgment file per (subject, field).

Oversized packets are judged in parts (see split_judge_packets.py), so the
judgments directory holds a mix: whole-unit files from agents that managed the
job in one pass, and `<unit>__partNN.jsonl` sets from those that did not.
merge_judgments.py splits a filename on `__` to find its scorecard, so a part
file would resolve to the field `products__part01` and be silently skipped —
its judgments lost with only a warning.

This assembles the two shapes into one file per unit, preferring whichever
source actually covers the packet:

  * a whole-unit file that covers every form wins outright;
  * otherwise the parts are concatenated, and the unit file (a truncated
    partial) is discarded rather than merged with them - mixing the two would
    double-count forms both happened to reach.

Part files are moved under judgments/parts/ once folded, so the merger's
non-recursive glob no longer sees them.

Usage: .venv/bin/python checks/assemble_census.py --run <run_id> [--apply]
"""
from __future__ import annotations
import argparse, json, re, sys
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from paths import run_dir  # noqa: E402
from split_judge_packets import WIN_SPLIT_RE, forms_in  # noqa: E402


def packet_forms(path: Path) -> Counter:
    c: Counter = Counter()
    for block in WIN_SPLIT_RE.split(path.read_text(encoding="utf-8"))[1:]:
        m = re.match(r"## Window (\d+:\d+)", block)
        if m is None:
            continue
        window = m.group(1)
        for form in forms_in(block.split("### The window text", 1)[0]):
            c[(window, form)] += 1
    return c


def read_records(path: Path) -> list[dict]:
    out = []
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            print(f"  ! unparseable line in {path.name}", file=sys.stderr)
    return out


def coded(records: list[dict]) -> Counter:
    c: Counter = Counter()
    for r in records:
        if r.get("type") != "miss":
            c[(r.get("window"), r.get("form"))] += 1
    return c


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--apply", action="store_true", help="write files (default: report only)")
    args = ap.parse_args()
    rd = run_dir(args.run)
    packets, jdir = rd / "raw" / "judge_packets", rd / "judgments"
    parts_dir = jdir / "parts"
    rows, complete = [], 0
    for pf in sorted(packets.glob("*.md")):
        unit = pf.stem
        want = packet_forms(pf)
        unit_recs = read_records(jdir / f"{unit}.jsonl")
        part_files = sorted(jdir.glob(f"{unit}__part*.jsonl"))
        part_recs: list[dict] = []
        for p in part_files:
            part_recs.extend(read_records(p))
        unit_missing = sum((want - coded(unit_recs)).values())
        part_missing = sum((want - coded(part_recs)).values())
        if unit_recs and unit_missing == 0:
            source, chosen, missing = "unit", unit_recs, 0
        elif part_files and part_missing <= unit_missing:
            source, chosen, missing = "parts", part_recs, part_missing
        elif unit_recs:
            source, chosen, missing = "unit", unit_recs, unit_missing
        else:
            source, chosen, missing = "none", [], sum(want.values())
        # A packet with zero forms is trivially complete once a judge has
        # attended it (miss sweep done, file written) - lucasmilhaupt equipments
        # returned nothing, so its packet lists no forms and its judgment file
        # may legitimately be empty.
        attended = bool(chosen) or (jdir / f"{unit}.jsonl").is_file() or bool(part_files)
        if missing == 0 and attended:
            complete += 1
        rows.append((unit, sum(want.values()), sum(coded(chosen).values()),
                     missing, source, len(part_files)))
        if args.apply and source == "parts" and chosen:
            (jdir / f"{unit}.jsonl").write_text(
                "\n".join(json.dumps(r, ensure_ascii=False) for r in chosen) + "\n",
                encoding="utf-8")
    if args.apply and any(jdir.glob("*__part*.jsonl")):
        parts_dir.mkdir(exist_ok=True)
        for p in sorted(jdir.glob("*__part*.jsonl")):
            p.rename(parts_dir / p.name)
    w = max((len(r[0]) for r in rows), default=10)
    print(f"{'unit':{w}s} {'packet':>7s} {'coded':>7s} {'missing':>8s} {'source':>7s} {'parts':>6s}")
    for r in rows:
        flag = "" if r[3] == 0 else "  <-- INCOMPLETE"
        print(f"{r[0]:{w}s} {r[1]:7d} {r[2]:7d} {r[3]:8d} {r[4]:>7s} {r[5]:6d}{flag}")
    print(f"\n{complete}/{len(rows)} units complete"
          + ("  (applied)" if args.apply else "  (dry run - pass --apply to write)"))


if __name__ == "__main__":
    main()
