"""Cut pending work orders into judge slices at request boundaries.

RUNBOOK step 4: one agent per work order, split above ~250 records into
slices of ~150–200, and NEVER split a ``request_custom_id`` across agents —
the co-packed context is what J3 needs. This tool does the arithmetic once
and writes it down, so every agent prompt names an exact, reproducible range:

  history/runs/<run_id>/pending/SLICES.json
    [{"subject", "field", "part", "start", "end", "records", "requests",
      "path"}]  — ``start``/``end`` are 0-based half-open indexes into the
      work order's ``records`` array.

Run:  .venv/bin/python checks/slices.py --run <run_id> [--target 175] [--max 250]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ""):  # runnable as a script
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import scorecard  # type: ignore[no-redef]
else:
    from . import scorecard


def cut(records: list[dict[str, Any]], target: int, max_records: int) -> list[tuple[int, int]]:
    """(start, end) ranges over ``records`` that never split a request.

    Records are grouped by ``request_custom_id`` in order of first appearance;
    a slice closes when adding the next request would push it past ``target``
    and it already holds something. One request larger than ``max_records`` is
    a slice of its own (a 292-snippet group rides alone in its request; a
    request is at most ~50 snippets of records otherwise).
    """
    if not records:
        return []
    if len(records) <= max_records:
        return [(0, len(records))]
    runs: list[tuple[int, int]] = []  # contiguous request runs
    start = 0
    for i in range(1, len(records) + 1):
        if i == len(records) or records[i].get("request_custom_id") != records[start].get(
            "request_custom_id"
        ):
            runs.append((start, i))
            start = i
    slices: list[tuple[int, int]] = []
    open_start, open_len = runs[0][0], 0
    for run_start, run_end in runs:
        run_len = run_end - run_start
        if open_len and open_len + run_len > target:
            slices.append((open_start, run_start))
            open_start, open_len = run_start, 0
        open_len += run_len
    slices.append((open_start, runs[-1][1]))
    return slices


def build(run_id: str, target: int, max_records: int) -> list[dict[str, Any]]:
    pending_dir = scorecard.history_dir(run_id) / "pending"
    out: list[dict[str, Any]] = []
    for path in sorted(pending_dir.glob("*__*.json")):
        order = json.loads(path.read_text(encoding="utf-8"))
        records = order.get("records") or []
        ranges = cut(records, target, max_records)
        for part, (start, end) in enumerate(ranges, start=1):
            out.append(
                {
                    "subject": order["subject"],
                    "field": order["field"],
                    "part": part,
                    "parts": len(ranges),
                    "start": start,
                    "end": end,
                    "records": end - start,
                    "requests": len(
                        {r.get("request_custom_id") for r in records[start:end]}
                    ),
                    "chunks": sorted({r["chunk_bounds"] for r in records[start:end]}),
                    "path": str(path),
                }
            )
    (pending_dir / "SLICES.json").write_text(
        json.dumps(out, indent=1), encoding="utf-8"
    )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--target", type=int, default=175)
    parser.add_argument("--max", dest="max_records", type=int, default=250)
    args = parser.parse_args()
    slices = build(args.run, args.target, args.max_records)
    print(
        json.dumps(
            {
                "slices": len(slices),
                "records": sum(s["records"] for s in slices),
                "largest": max((s["records"] for s in slices), default=0),
                "work_orders": len({(s["subject"], s["field"]) for s in slices}),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
