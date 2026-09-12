"""Coverage check for a run's judge fan-out (RUNBOOK step 4, the check done inline on
2026-09-11 for run 20260911T003500, made reusable): every planned record of
``pending/judged/*__*.json`` has exactly one well-formed verdict row under ``verdicts/``,
every row carries J1–J7 and a model-stamped ``judge`` string, and nothing extra was judged.

.venv/bin/python checks/verdict_coverage.py --run <run_id> [--jobs]
``--jobs`` also lists which job files (pending/judged/jobs/J*.md) still have items whose out
file is missing or short, so a fan-out can be resumed job by job.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import scorecard  # type: ignore[no-redef]
else:
    from . import scorecard

DIMS = ("J1", "J2", "J3", "J4", "J5", "J6", "J7")


def planned_keys(run_dir: Path) -> dict[str, tuple[str, str]]:
    out: dict[str, tuple[str, str]] = {}
    for path in sorted((run_dir / "pending" / "judged").glob("*__*.json")):
        order = json.loads(path.read_text(encoding="utf-8"))
        for r in order["records"]:
            out[r["content_key"]] = (order["subject"], order["field"])
    return out


def verdict_rows(run_dir: Path) -> tuple[dict[str, dict[str, Any]], int, int, list[str]]:
    rows: dict[str, dict[str, Any]] = {}
    malformed = duplicates = 0
    bad_schema: list[str] = []
    for path in sorted((run_dir / "verdicts").glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError:
                malformed += 1
                continue
            key = row.get("content_key")
            if not key:
                bad_schema.append(f"{path.name}: row without content_key")
                continue
            if key in rows:
                duplicates += 1
                continue
            checks = row.get("checks") or {}
            missing_dims = [d for d in DIMS if d not in checks or "verdict" not in (checks.get(d) or {})]
            if missing_dims:
                bad_schema.append(f"{path.name}: {key} missing {missing_dims}")
            if not re.match(r"^agent:[a-z0-9.-]+", str(row.get("judge", ""))):
                bad_schema.append(f"{path.name}: {key} judge string not model-stamped: {row.get('judge')!r}")
            rows[key] = row
    return rows, malformed, duplicates, bad_schema


def job_status(run_dir: Path, rows: dict[str, dict[str, Any]]) -> list[str]:
    pending: list[str] = []
    for job in sorted((run_dir / "pending" / "judged" / "jobs").glob("J*.md")):
        short: list[str] = []
        for line in job.read_text(encoding="utf-8").splitlines():
            if not line.startswith("{"):
                continue
            item = json.loads(line)
            order = json.loads(Path(item["order"]).read_text(encoding="utf-8"))
            keys = [r["content_key"] for r in order["records"][item["start"] : item["end"]]]
            got = sum(k in rows for k in keys)
            if got < len(keys):
                short.append(f"{item['subject']}/{item['field']} part{item['part']}: {got}/{len(keys)}")
        if short:
            pending.append(f"{job.name}: " + "; ".join(short))
    return pending


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", required=True)
    parser.add_argument("--jobs", action="store_true")
    args = parser.parse_args()
    run_dir = scorecard.history_dir(args.run)
    planned = planned_keys(run_dir)
    rows, malformed, duplicates, bad_schema = verdict_rows(run_dir)
    missing = [k for k in planned if k not in rows]
    extra = [k for k in rows if k not in planned]
    by_field = Counter(planned[k][1] for k in missing)
    summary = {
        "planned": len(planned),
        "rows": len(rows),
        "malformed": malformed,
        "duplicates": duplicates,
        "missing": len(missing),
        "missing_by_field": dict(by_field),
        "extra": len(extra),
        "bad_schema": len(bad_schema),
        "fails_any": sum(any((rows[k]["checks"].get(d) or {}).get("verdict") == "fail" for d in DIMS) for k in rows),
    }
    print(json.dumps(summary, indent=1))
    for line in bad_schema[:20]:
        print("  bad:", line)
    if args.jobs:
        for line in job_status(run_dir, rows):
            print("  pending:", line)


if __name__ == "__main__":
    main()
