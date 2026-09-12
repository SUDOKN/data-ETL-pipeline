"""RUNBOOK step 5 — build verification packets from a run's verdict files.

  .venv/bin/python checks/verify_packets.py --run <run_id> [--passes 5] [--per-packet 120]

For every verdict file: every J3 fail, every ``major`` fail, and N random
passes are re-read against the work order before ingestion. This tool joins
each such verdict row to its record (paragraph, evidence, locations, chunk
text path) from the judged work orders and writes
``history/runs/<run>/verify/packet_<k>.json`` plus ``VERIFY_INDEX.json`` so a
verifier agent gets everything it needs in one file and writes corrections
as full replacement rows to ``history/runs/<run>/verify/corrections_<k>.jsonl``
(``verified: true`` on every row it re-read, corrected or not).
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import scorecard  # type: ignore[no-redef]
else:
    from . import scorecard

DIMS = ["J1", "J2", "J3", "J4", "J5", "J6", "J7"]


def _severity(row: dict[str, Any]) -> tuple[bool, bool, bool]:
    checks = row.get("checks") or {}
    fails = {d: checks.get(d) or {} for d in DIMS if d in checks}
    j3 = (fails.get("J3") or {}).get("verdict") == "fail"
    major = any(c.get("verdict") == "fail" and c.get("severity") == "major" for c in fails.values())
    any_fail = any(c.get("verdict") == "fail" for c in fails.values())
    return j3, major, any_fail


def build(
    run_id: str,
    passes: int,
    per_packet: int,
    seed: int = 20260911,
    calibration_files: tuple[str, ...] = (),
    only: tuple[str, ...] = (),
    first_packet: int = 1,
) -> dict[str, Any]:
    """``calibration_files``: verdict-file basenames whose EVERY J2/J6 fail
    (not only the majors) joins the packets under the reason
    ``"calibration: document rule"`` — for slices a judge flagged as
    turning on one calibration question.

    ``only``: verdict-file basename prefixes; when given, ONLY those files are
    packeted and the existing packets are kept, numbered from
    ``first_packet`` — for verdicts added to an already-verified run (2026-09-12:
    a subject's two fields re-run and judged after the run's verification)."""
    run_dir = scorecard.history_dir(run_id)
    orders: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for path in (run_dir / "pending" / "judged").glob("*__*.json"):
        order = json.loads(path.read_text(encoding="utf-8"))
        orders[(order["subject"], order["field"])] = {
            **{f"{r['chunk_bounds']}:{r['group_id']}": {**r, "chunk_text_path": (order.get("chunk_texts") or {}).get(r["chunk_bounds"])} for r in order["records"]},
        }
    rng = random.Random(seed)
    items: list[dict[str, Any]] = []
    summary: dict[str, Any] = {"files": 0, "rows": 0, "j3_fails": 0, "majors": 0, "passes_sampled": 0}
    for path in sorted((run_dir / "verdicts").glob("*.jsonl")):
        if only and not path.name.startswith(only):
            continue
        rows: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                rows.append(json.loads(line))
            except ValueError:
                continue
        summary["files"] += 1
        summary["rows"] += len(rows)
        chosen: list[tuple[str, dict[str, Any]]] = []
        passing: list[dict[str, Any]] = []
        calibration = path.name in calibration_files
        for row in rows:
            j3, major, any_fail = _severity(row)
            checks = row.get("checks") or {}
            j2_or_j6 = any((checks.get(d) or {}).get("verdict") == "fail" for d in ("J2", "J6"))
            if j3:
                chosen.append(("J3 fail", row)); summary["j3_fails"] += 1
            elif major:
                chosen.append(("major fail", row)); summary["majors"] += 1
            elif calibration and j2_or_j6:
                chosen.append(("calibration: document rule", row))
                summary["calibration"] = summary.get("calibration", 0) + 1
            elif not any_fail:
                passing.append(row)
        sample = rng.sample(passing, min(passes, len(passing)))
        summary["passes_sampled"] += len(sample)
        chosen.extend(("random pass", row) for row in sample)
        for reason, row in chosen:
            record = orders.get((row["subject"], row["field"]), {}).get(f"{row['chunk_bounds']}:{row['group_id']}")
            items.append({"reason": reason, "verdict_file": str(path), "verdict": row, "record": record})
    out_dir = run_dir / "verify"
    out_dir.mkdir(exist_ok=True)
    if not only:
        for old in out_dir.glob("packet_*.json"):
            old.unlink()
    packets = []
    for k in range(0, len(items), per_packet):
        number = first_packet + k // per_packet
        packet_path = out_dir / f"packet_{number:02d}.json"
        packet_path.write_text(json.dumps(items[k:k + per_packet], indent=1, ensure_ascii=False, default=str), encoding="utf-8")
        packets.append({"path": str(packet_path), "items": len(items[k:k + per_packet]),
                        "corrections": str(out_dir / f"corrections_{number:02d}.jsonl")})
    summary["items"] = len(items)
    summary["packets"] = packets
    summary["only"] = list(only)
    index_name = "VERIFY_INDEX.json" if not only else f"VERIFY_INDEX_{first_packet:02d}.json"
    (out_dir / index_name).write_text(json.dumps(summary, indent=1), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--passes", type=int, default=5)
    parser.add_argument("--per-packet", type=int, default=120)
    parser.add_argument(
        "--calibration-file",
        action="append",
        default=[],
        help="verdict file basename whose every J2/J6 fail is verified (repeatable)",
    )
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        help="verdict-file basename prefix to packet (repeatable); keeps existing packets",
    )
    parser.add_argument(
        "--first-packet", type=int, default=1, help="number of the first packet written"
    )
    args = parser.parse_args()
    summary = build(
        args.run,
        args.passes,
        args.per_packet,
        calibration_files=tuple(args.calibration_file),
        only=tuple(args.only),
        first_packet=args.first_packet,
    )
    print(json.dumps({k: v for k, v in summary.items() if k != "packets"} | {"packets": len(summary["packets"])}, indent=1))


if __name__ == "__main__":
    main()
