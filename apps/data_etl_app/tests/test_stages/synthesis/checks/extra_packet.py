"""Build ONE extra verification packet from named verdict rows (RUNBOOK step 5).

  .venv/bin/python checks/extra_packet.py <run_id> <packet_number> <verdict_file_basename> <content_key>...

``verify_packets.py`` samples J3 fails, majors, calibration fails and random
passes; a row it would not sample — typically a PASS the judge later flagged
as wrong in its reply (an appended file is never rewritten) — is verified by
naming its content keys here. Writes ``history/runs/<run>/verify/
packet_<NN>.json`` in the same item shape ({reason, verdict_file, verdict,
record}, the record from the judged work order with the chunk text path),
numbered AFTER the built packets (``verify_packets.py`` without ``--only``
deletes existing packets, so build the main packets first); the verifier
writes ``corrections_<NN>.jsonl`` and ``apply_corrections.py`` merges it like
any other. Written 2026-09-13 for run 20260913T170246 (four steelcraft
conformity rows, all four verified J2+J6 major)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import scorecard  # type: ignore[no-redef]
else:
    from . import scorecard


def build(run_id: str, number: int, verdict_file: str, keys: list[str]) -> Path:
    run_dir = scorecard.history_dir(run_id)
    vpath = run_dir / "verdicts" / verdict_file
    rows: dict[str, dict[str, Any]] = {}
    for line in vpath.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        rows[row["content_key"]] = row
    subject_field = verdict_file.split("__part")[0]
    order = json.loads(
        (run_dir / "pending" / "judged" / f"{subject_field}.json").read_text(encoding="utf-8")
    )
    records = {r["content_key"]: r for r in order["records"]}
    items: list[dict[str, Any]] = []
    for key in keys:
        if key not in rows or key not in records:
            raise SystemExit(f"{key}: in verdicts {key in rows}, in work order {key in records}")
        record = dict(records[key])
        record["chunk_text"] = (order.get("chunk_texts") or {}).get(record.get("chunk_bounds"))
        items.append(
            {
                "reason": "judge self-flagged pass",
                "verdict_file": str(vpath),
                "verdict": rows[key],
                "record": record,
            }
        )
    out = run_dir / "verify" / f"packet_{number:02d}.json"
    out.write_text(json.dumps(items, ensure_ascii=False, indent=1), encoding="utf-8")
    return out


def main() -> None:
    if len(sys.argv) < 5:
        raise SystemExit(__doc__)
    run_id, number, verdict_file, *keys = sys.argv[1:]
    out = build(run_id, int(number), verdict_file, keys)
    print(f"wrote {out} with {len(keys)} item(s); corrections expected at {out.parent / out.name.replace('packet_', 'corrections_').replace('.json', '.jsonl')}")


if __name__ == "__main__":
    main()
