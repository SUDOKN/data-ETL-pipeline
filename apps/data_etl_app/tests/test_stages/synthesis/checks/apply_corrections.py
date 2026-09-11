"""RUNBOOK step 5, second half — merge verifier corrections into the verdict files.

  .venv/bin/python checks/apply_corrections.py --run <run_id> [--dry-run]

Reads ``history/runs/<run>/verify/corrections_*.jsonl`` (full replacement
rows written by the verifier agents, ``verified: true``) and rewrites each
verdict file in ``history/runs/<run>/verdicts/`` with the corrected row in
place of the judge's row, matched by ``content_key``. A correction whose key
is not found in any verdict file is reported and skipped. Prints what
changed per dimension (verdict and severity transitions) so the report can
say how many rows verification moved — the step is a measurement.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import scorecard  # type: ignore[no-redef]
else:
    from . import scorecard

DIMS = ["J1", "J2", "J3", "J4", "J5", "J6", "J7"]


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                rows.append(json.loads(line))
            except ValueError:
                continue
    return rows


def apply(run_id: str, dry_run: bool = False) -> dict[str, Any]:
    run_dir = scorecard.history_dir(run_id)
    corrections: dict[str, dict[str, Any]] = {}
    for path in sorted((run_dir / "verify").glob("corrections_*.jsonl")):
        for row in _load_jsonl(path):
            corrections[row["content_key"]] = row
    transitions: Counter = Counter()
    changed_rows = 0
    applied = 0
    files_touched = 0
    for path in sorted((run_dir / "verdicts").glob("*.jsonl")):
        rows = _load_jsonl(path)
        touched = False
        for i, row in enumerate(rows):
            content_key = row.get("content_key")
            fix = corrections.get(content_key) if isinstance(content_key, str) else None
            if fix is None:
                continue
            applied += 1
            moved = False
            for d in DIMS:
                before = (row.get("checks") or {}).get(d) or {}
                after = (fix.get("checks") or {}).get(d) or {}
                if (before.get("verdict"), before.get("severity")) != (after.get("verdict"), after.get("severity")):
                    transitions[f"{d}: {before.get('verdict')}/{before.get('severity')} -> {after.get('verdict')}/{after.get('severity')}"] += 1
                    moved = True
            if moved:
                changed_rows += 1
            fix = dict(fix)
            fix["verified"] = True
            rows[i] = fix
            touched = True
        if touched:
            files_touched += 1
            if not dry_run:
                path.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8")
    unmatched = len(corrections) - applied
    return {
        "corrections": len(corrections),
        "applied": applied,
        "unmatched": unmatched,
        "rows_changed": changed_rows,
        "files_touched": files_touched,
        "dry_run": dry_run,
        "transitions": dict(transitions.most_common()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(json.dumps(apply(args.run, args.dry_run), indent=1))


if __name__ == "__main__":
    main()
