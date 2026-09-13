"""Per-request clustering of judged fails — the per-request MODE readout.

  .venv/bin/python checks/request_mode_readout.py --run <run_id> [--subject <s>]...

gpt-4.1 at temperature 0 lands a whole synthesis request in one reading
(design doc §23: the same 39-record request comes back all laundered or all
hedged). This readout makes that visible in the VERDICTS: it joins each judged
record (``history/runs/<run>/verdicts/*.jsonl``, corrected rows) to the request
it rode in (``request_custom_id`` from ``pending/judged/*.json``) and, over
requests with at least two judged records, prints how many failing records sit
in a request where the MAJORITY of judged records fail, how many requests fail
whole, and how many are mixed — for any-fail and for majors. Measured
2026-09-13 (mathewsco + tanfel): 22% of failing records in majority-failing
requests on run 20260912T191548 (cap 50), 51% on the A/A draw 20260912T225723,
81% on the cap-10 run 20260913T023316 (18 whole-request fails) — the coin is
per request whatever its size. Read-only; no gate.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import paired_readout, scorecard  # type: ignore[no-redef]
else:
    from . import paired_readout, scorecard


def request_of_records(run_id: str) -> tuple[dict[str, str], dict[str, tuple[str, str]]]:
    """``content_key → request_custom_id`` and ``content_key → (subject, field)``
    from the run's judged work orders (the thinned ``pending/*.json`` lose
    judged records after finalize; ``pending/judged/*.json`` keep them)."""
    by_key: dict[str, str] = {}
    meta: dict[str, tuple[str, str]] = {}
    judged_dir = scorecard.history_dir(run_id) / "pending" / "judged"
    for path in sorted(judged_dir.glob("*__*.json")):
        order = json.loads(path.read_text(encoding="utf-8"))
        for record in order.get("records") or []:
            key = record.get("content_key")
            if not key:
                continue
            by_key[key] = record.get("request_custom_id") or ""
            meta[key] = (order["subject"], order["field"])
    return by_key, meta


def readout(run_id: str, subjects: tuple[str, ...]) -> None:
    by_key, meta = request_of_records(run_id)
    verdicts = {row["content_key"]: row for row in paired_readout.load_run_verdicts(run_id).values()}
    for label, fn in (("any-fail", paired_readout.any_fail), ("major", paired_readout.any_major)):
        groups: dict[tuple[tuple[str, str], str], list[bool]] = defaultdict(list)
        for key, row in verdicts.items():
            if key not in by_key:
                continue
            if subjects and meta[key][0] not in subjects:
                continue
            groups[(meta[key], by_key[key])].append(fn(row))
        multi = {k: v for k, v in groups.items() if len(v) >= 2}
        failing = sum(sum(v) for v in multi.values())
        in_majority = sum(sum(v) for v in multi.values() if sum(v) * 2 > len(v))
        whole = sum(1 for v in multi.values() if all(v))
        mixed = sum(1 for v in multi.values() if any(v) and not all(v))
        share = f"{100 * in_majority / failing:.0f}%" if failing else "n/a"
        print(
            f"{run_id} {label:8s}: requests with >=2 judged records {len(multi)}, failing records "
            f"{failing}, in majority-failing requests {in_majority} ({share}), whole-request fails "
            f"{whole}, mixed requests {mixed}"
        )
        per_field: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
        for (m, _req), v in multi.items():
            per_field[m].append((len(v), sum(v)))
        for m, rows in sorted(per_field.items()):
            fails = sum(b for _a, b in rows)
            if not fails:
                continue
            worst = sorted(((a, b) for a, b in rows if b), key=lambda t: (-t[1], -t[0]))[:12]
            print(f"    {m[0]}/{m[1]}: {len(rows)} requests, {fails} failing; (judged, failing) per failing request {worst}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--subject", action="append", default=[], help="restrict to a subject (repeatable)")
    args = parser.parse_args()
    readout(args.run, tuple(args.subject))


if __name__ == "__main__":
    main()
