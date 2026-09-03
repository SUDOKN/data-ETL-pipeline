"""Mechanical validation of diff-set judgment files (RUNBOOK step 5's first half).

Checks every history/runs/<run>/diffset_judgments/*.jsonl against
raw/diffset_packets/diffset_index.json:

  - every line parses, has a known type, and uses the allowed vocabulary
    (codes per field, verdicts per type, actors, evidence kinds);
  - once a unit's every part has a judgment file, its per-type row counts
    match what the packets asked for (forms == unmatched, checks == listed);
  - duplicate rows (same type + key) are flagged.

Exit code 1 on any problem, so it can gate the merge.

Usage: .venv/bin/python validate_diffset_judgments.py --run 20260828T163338
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from paths import run_dir, subject_slug  # noqa: E402

FIELD_CODES = {
    "products": set("VBPMGCSDU"),
    "equipments": set("VBDPTSGU"),
    "process_caps": set("VBLIZMEGU"),
    "material_caps": set("VBPNCGU"),
    "industries": set("VBOWGU"),
    "conformity_attestations": set("VBDZGU"),
}
ACTORS = {"own", "client", "supplier", "lab", "parent_sibling", "reseller_inventory", "unclear"}
EVIDENCE = {"prose", "list", "table", "title", "footer", "other"}
VERDICTS = {
    "candidate_check": {"confirm", "dispute", "not_found"},
    "miss_validation": {"valid_miss", "entry_wrong"},
    "credit_check": {"true_credit", "false_credit"},
}
_PART_RE = re.compile(r"\.part\d+$")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    args = parser.parse_args()
    rdir = run_dir(args.run)
    index = json.loads(
        (rdir / "raw" / "diffset_packets" / "diffset_index.json").read_text(encoding="utf-8")
    )
    jdir = rdir / "diffset_judgments"
    problems: list[str] = []
    unit_counts: dict[tuple[str, str], Counter[str]] = {}
    files_by_unit: dict[tuple[str, str], set[str]] = {}

    for path in sorted(jdir.glob("*.jsonl")):
        if path.name.endswith(".judge2.jsonl"):
            stem = path.name[: -len(".judge2.jsonl")]
        else:
            stem = path.stem
        unit_stem = _PART_RE.sub("", stem)
        slug, _, field = unit_stem.partition("__")
        codes_ok = FIELD_CODES.get(field)
        if codes_ok is None:
            problems.append(f"{path.name}: unknown field {field!r}")
            continue
        is_double = path.name.endswith(".judge2.jsonl")
        key = (slug, field)
        if not is_double:
            files_by_unit.setdefault(key, set()).add(stem)
        seen: set[tuple[str, str, str]] = set()
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                problems.append(f"{path.name}:{n}: unparseable ({exc})")
                continue
            kind = row.get("type") or ("form" if row.get("code") else None)
            if kind == "form":
                code = str(row.get("code", ""))
                if code not in codes_ok:
                    problems.append(f"{path.name}:{n}: code {code!r} not in {field} table")
                actor = row.get("actor")
                if actor and str(actor) not in ACTORS:
                    problems.append(f"{path.name}:{n}: unknown actor {actor!r}")
                ek = row.get("evidence_kind")
                if ek and str(ek) not in EVIDENCE:
                    problems.append(f"{path.name}:{n}: unknown evidence_kind {ek!r}")
                # The stage can return the SAME form string twice in one window
                # (each occurrence is listed and judged separately), so form
                # rows are exempt from the duplicate check; the unit-level
                # count check still catches over/under-writing.
                dedup = None
            elif kind in VERDICTS:
                verdict = str(row.get("verdict", ""))
                if verdict not in VERDICTS[kind]:
                    problems.append(f"{path.name}:{n}: verdict {verdict!r} invalid for {kind}")
                if kind == "miss_validation" and verdict == "entry_wrong":
                    if row.get("suggested_status") not in ("disputed", "retired"):
                        problems.append(
                            f"{path.name}:{n}: entry_wrong without suggested_status disputed|retired"
                        )
                anchor = str(row.get("entry_id")) if kind != "credit_check" else (
                    f"{row.get('entry_id')}|{row.get('form')}|{row.get('window')}"
                )
                dedup = (kind, anchor, "")
            else:
                problems.append(f"{path.name}:{n}: unknown row type {row.get('type')!r}")
                continue
            if dedup is not None:
                if dedup in seen:
                    problems.append(f"{path.name}:{n}: duplicate row {dedup}")
                seen.add(dedup)
            if not is_double:
                unit_counts.setdefault(key, Counter())[kind] += 1

    complete = incomplete = 0
    for unit in index["units"]:
        slug = subject_slug(unit["subject"])
        key = (slug, unit["field"])
        expected_files = {Path(f).stem for f in unit["packets"]}
        have = files_by_unit.get(key, set())
        if not expected_files:
            continue
        if have < expected_files:
            incomplete += 1
            continue
        complete += 1
        counts = unit_counts.get(key, Counter())
        expect = {
            "form": unit["unmatched"],
            "candidate_check": unit["candidate_checks"],
            "miss_validation": unit["miss_validations"],
            "credit_check": unit["credit_samples"],
        }
        for kind, want in expect.items():
            got = counts.get(kind, 0)
            if got != want:
                problems.append(
                    f"{slug}/{unit['field']}: {kind} rows {got} != {want} the packets listed"
                )

    print(f"units complete: {complete}, awaiting parts: {incomplete}, problems: {len(problems)}")
    for p in problems[:60]:
        print(" !", p)
    if len(problems) > 60:
        print(f"   ... and {len(problems) - 60} more")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
