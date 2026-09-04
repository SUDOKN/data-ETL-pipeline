"""Cancel phantom misses on split windows before any miss count is quoted.

A judge holding one slice of a window's forms cannot see what sibling parts
cover (the 09-01 merge-checklist item 7 hazard, realized at scale in this
run: acimachine products part03 declared 137 misses on a window whose other
150 forms lived in part02). This pass pools every part's form strings per
(unit, window) and re-tests each miss entity against the pooled forms.

A miss is CANCELLED when its entity matches a pooled form of the same
window: exact case-insensitive equality, or containment either way after
whitespace normalization (the same leniency judges applied within their own
slices). Everything else SURVIVES. Cancelled misses are annotated in place
(`"reconciled": "covered-by-sibling"`, with the covering form and part) so
merge_judgments.py and census_report.py can filter on it; nothing is
deleted. A summary lands beside the judgments dir.

Usage: .venv/bin/python checks/reconcile_split_misses.py --run <run_id> [--apply]
Without --apply it only prints the would-be cancellations.
"""

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
RUNS = HERE.parent / "history" / "runs"


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip().lower()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    jdir = RUNS / args.run / "judgments"
    files = sorted(jdir.glob("*.jsonl"))
    if not files:
        raise SystemExit(f"no judgment files under {jdir}")

    # unit = "<subject>__<field>"; part files share the unit prefix.
    def unit_of(path: Path) -> str:
        stem = path.stem
        return re.sub(r"__part\d+$", "", stem)

    # Pool forms per (unit, window) across all parts, remembering the part.
    pooled: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    rows_by_file: dict[Path, list[dict]] = {}
    for f in files:
        if f.stem.endswith(".judge2"):
            continue
        rows = []
        for line in f.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        rows_by_file[f] = rows
        u = unit_of(f)
        for r in rows:
            if r.get("type") != "miss" and "form" in r:
                pooled[(u, str(r.get("window", "")))].append(
                    (norm(str(r["form"])), f.stem)
                )

    cancelled = surviving = 0
    per_unit: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    changed_files = set()
    for f, rows in rows_by_file.items():
        u = unit_of(f)
        multi_part = sum(1 for g in rows_by_file if unit_of(g) == u) > 1
        for r in rows:
            if r.get("type") != "miss":
                continue
            ent = norm(str(r.get("entity", "")))
            if not ent:
                continue
            hit = None
            for form_n, part in pooled.get((u, str(r.get("window", ""))), []):
                # A sibling's form cancels; the same file's forms were already
                # visible to the judge, so only count cross-part coverage.
                if part == f.stem and multi_part:
                    continue
                if ent == form_n or ent in form_n or form_n in ent:
                    hit = (form_n, part)
                    break
            if hit:
                cancelled += 1
                per_unit[u][0] += 1
                if args.apply and r.get("reconciled") != "covered-by-sibling":
                    r["reconciled"] = "covered-by-sibling"
                    r["covered_by_form"] = hit[0]
                    r["covered_in_part"] = hit[1]
                    changed_files.add(f)
            else:
                surviving += 1
                per_unit[u][1] += 1

    if args.apply:
        for f in changed_files:
            f.write_text(
                "\n".join(json.dumps(r, ensure_ascii=False) for r in rows_by_file[f]) + "\n"
            )
        summary = {
            "run": args.run,
            "cancelled_covered_by_sibling": cancelled,
            "surviving_misses": surviving,
            "per_unit": {u: {"cancelled": c, "surviving": s} for u, (c, s) in sorted(per_unit.items())},
        }
        out = RUNS / args.run / "raw" / "miss_reconciliation.json"
        out.write_text(json.dumps(summary, indent=1))
        print(f"applied: {cancelled} cancelled, {surviving} surviving -> {out}")
    else:
        print(f"dry-run: would cancel {cancelled}, leaving {surviving} surviving")
        for u, (c, s) in sorted(per_unit.items(), key=lambda kv: -kv[1][0])[:15]:
            if c:
                print(f"  {u:55s} cancel {c:4d} keep {s:4d}")


if __name__ == "__main__":
    main()
