"""Fold judge-agent output into the run's scorecards (RUNBOOK step 5).

Judges write one JSONL file per (subject, field) into
history/runs/<run>/judgments/:

    <subject_slug>__<field>.jsonl            the primary judge
    <subject_slug>__<field>.judge2.jsonl     the independent double-judge

Two record shapes:

    {"window": "0:23771", "form": "CNC machining", "code": "P",
     "actor": "own", "evidence_kind": "prose", "note": ""}
    {"type": "miss", "window": "0:23771", "entity": "Zeiss CONTURA G2 CMM",
     "quote": "...", "suggested_forms": ["Zeiss CONTURA G2 CMM"]}

Precision is reported ONLY beside the agreement floor: a judged delta smaller
than the rate at which two judges disagree is not a result. Where no
double-judge file exists the floor is null and the scorecard says so.

Usage: .venv/bin/python merge_judgments.py --run 20260825T194457
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from paths import run_dir  # noqa: E402
from scorecard import append_history  # noqa: E402

# Every field's code table maps onto this rollup so fields stay comparable.
ROLLUP = {
    "V": "in_field", "B": "in_field",
    "P": "adjacent_field", "M": "adjacent_field", "C": "adjacent_field",
    "S": "adjacent_field", "D": "adjacent_field", "E": "adjacent_field",
    "N": "adjacent_field", "L": "adjacent_field", "I": "adjacent_field",
    "O": "adjacent_field", "T": "adjacent_field", "W": "adjacent_field",
    "Z": "adjacent_field",
    "G": "generic",
    "U": "junk",
}


_BOUNDS_RE = re.compile(r"\d+:\d+")


def normalize_window(value: Any) -> Any:
    """Reduce a window key to its bare `start:end` bounds.

    Judges have written the window both ways — `"0:23771"` and the full packet
    heading `"Window 0:23771 (chunk 0:96974)"`. Both name the same window, but
    an un-normalized key silently makes two judges share ZERO keys, which reads
    as "no agreement data" rather than as a format mismatch. Normalizing here
    keeps the agreement floor computable whichever way an agent writes it."""
    if not isinstance(value, str):
        return value
    match = _BOUNDS_RE.search(value)
    return match.group() if match else value


def read_judgments(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    forms: list[dict[str, Any]] = []
    misses: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        record = json.loads(line)
        record["window"] = normalize_window(record.get("window"))
        if record.get("type") == "miss":
            misses.append(record)
        elif record.get("code"):
            forms.append(record)
    return forms, misses


def summarize(forms: list[dict[str, Any]], misses: list[dict[str, Any]]) -> dict[str, Any]:
    codes = Counter(r["code"] for r in forms)
    rollup = Counter(ROLLUP.get(r["code"], "junk") for r in forms)
    judged = len(forms)
    actors = Counter(r.get("actor") for r in forms if r.get("actor"))
    wrong_actor = sum(
        count for actor, count in actors.items()
        if actor in {"client", "supplier", "lab", "parent_sibling", "reseller_inventory"}
    )
    return {
        "forms_judged": judged,
        "codes": dict(sorted(codes.items())),
        "rollup": dict(sorted(rollup.items())),
        "precision_in_field": round(rollup["in_field"] / judged, 4) if judged else None,
        "junk_rate": round(rollup["junk"] / judged, 4) if judged else None,
        "actors": dict(sorted(actors.items())),
        "wrong_actor_share": round(wrong_actor / judged, 4) if judged else None,
        "misses": misses,
        "miss_count": len(misses),
    }


def agreement(primary: list[dict[str, Any]], second: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Percent agreement on the shared rollup over forms BOTH judges coded."""
    if not second:
        return None
    key = lambda r: (r.get("window"), r.get("form"))  # noqa: E731
    a = {key(r): ROLLUP.get(r["code"], "junk") for r in primary}
    b = {key(r): ROLLUP.get(r["code"], "junk") for r in second}
    shared = set(a) & set(b)
    if not shared:
        return {"compared": 0, "agreement": None}
    same = sum(1 for k in shared if a[k] == b[k])
    disagreements = [
        {"window": k[0], "form": k[1], "primary": a[k], "second": b[k]}
        for k in sorted(shared, key=lambda x: (str(x[0]), str(x[1])))
        if a[k] != b[k]
    ]
    return {
        "compared": len(shared),
        "agreement": round(same / len(shared), 4),
        "disagreements": disagreements[:25],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    args = parser.parse_args()

    directory = run_dir(args.run) / "judgments"
    if not directory.is_dir():
        print(f"no judgments directory at {directory}", file=sys.stderr)
        raise SystemExit(1)

    merged = 0
    report_rows: list[str] = []
    for path in sorted(directory.glob("*.jsonl")):
        if path.name.endswith(".judge2.jsonl"):
            continue
        stem = path.stem  # <subject_slug>__<field>
        slug, _, field = stem.partition("__")
        forms, misses = read_judgments(path)
        summary = summarize(forms, misses)
        second_path = path.with_name(f"{stem}.judge2.jsonl")
        second_forms: list[dict[str, Any]] = []
        if second_path.is_file():
            second_forms, _ = read_judgments(second_path)
        summary["judge_agreement"] = agreement(forms, second_forms)

        scorecard_path = run_dir(args.run) / f"scorecard_{slug}_{field}.json"
        if not scorecard_path.is_file():
            print(f"  ! no scorecard for {slug}/{field}, skipping", file=sys.stderr)
            continue
        scorecard = json.loads(scorecard_path.read_text())
        scorecard["metrics"]["judged"] = summary
        scorecard_path.write_text(json.dumps(scorecard, indent=2, ensure_ascii=False) + "\n")
        append_history(scorecard)
        merged += 1

        floor = summary["judge_agreement"]
        floor_text = (
            f"{floor['agreement']:.0%} over {floor['compared']}" if floor and floor.get("agreement") is not None
            else "not double-judged"
        )
        report_rows.append(
            f"| {slug} | {field} | {summary['forms_judged']} | "
            f"{summary['precision_in_field']:.0%} | {summary['junk_rate']:.0%} | "
            f"{summary['wrong_actor_share']:.0%} | {summary['miss_count']} | {floor_text} |"
            if summary["forms_judged"] else
            f"| {slug} | {field} | 0 | — | — | — | {summary['miss_count']} | {floor_text} |"
        )

    if report_rows:
        out = run_dir(args.run) / "JUDGED.md"
        header = [
            f"# Judged census — run {args.run}",
            "",
            "Precision is `in_field / judged` on the shared rollup. Read every",
            "number beside its agreement floor: a difference smaller than the rate",
            "at which two judges disagree is not a result.",
            "",
            "| subject | field | judged | precision | junk | wrong-actor | misses | agreement floor |",
            "|---|---|---|---|---|---|---|---|",
        ]
        out.write_text("\n".join(header + report_rows) + "\n", encoding="utf-8")
        print(f"merged {merged} judged sets; wrote {out}")


if __name__ == "__main__":
    main()
