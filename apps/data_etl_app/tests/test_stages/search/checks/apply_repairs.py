"""Fold a repair agent's verdicts back into the markdown expectation set.

`port_corpus.py` carries over every entry whose evidence still occurs in the new
text and holds the rest out in a repair worklist (see
`expectations_markdown/REPAIR_BRIEF.md`). An agent reads the new text and
returns one verdict per held-out entry. This script writes those verdicts in.

Held-out entries were never written to the YAML, so a `reanchor` INSERTS the
entry (with its original id, name, actor and notes, plus the agent's new
evidence and forms) and a `retire` inserts it with `status: retired` as a
record. `dispute` inserts it with `status: disputed`, which keeps the argument
on file without gating recall.

Entries are inserted in document order -- by the offset of their first evidence
quote -- so the YAML keeps following the site the way `build_expectations.py`
leaves it.

INPUT -- one JSON object per line:

    {"id": "...", "field": "products", "verdict": "reanchor",
     "acceptable_forms": [...], "evidence": [{"quote": "..."}], "notes": "..."}
    {"id": "...", "field": "products", "verdict": "retire", "reason": "..."}

Usage:
    .venv/bin/python checks/apply_repairs.py --slug lucasmilhaupt_com \\
        --worklist <repair_*.jsonl> --verdicts <agent output>.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date as date_cls
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from _shared.text_matching import flexible_pattern, normalize_spaces  # noqa: E402
from paths import EXPECTATIONS_DIR, REPO_ROOT, SEARCH_FIELDS  # noqa: E402

MAX_QUOTE_LENGTH = 200
VALID_VERDICTS = {"reanchor", "retire", "dispute"}
ENTRY_KEY_ORDER = (
    "id", "name", "status", "acceptable_forms", "evidence",
    "actor", "notes", "provenance",
)


def _occurs(form: str, normalized_text: str, *, case_sensitive: bool = True) -> bool:
    if len(form.strip()) <= 3:
        case_sensitive = True
    pattern = flexible_pattern(form, case_sensitive=case_sensitive)
    return bool(pattern and pattern.search(normalized_text))


def _offset_of(quote: str, normalized_text: str) -> int | None:
    pattern = flexible_pattern(quote, case_sensitive=True)
    if pattern is None:
        return None
    match = pattern.search(normalized_text)
    return match.start() if match else None


def _ordered(entry: dict[str, Any]) -> dict[str, Any]:
    rest = dict(entry)
    out = {k: rest.pop(k) for k in ENTRY_KEY_ORDER if k in rest}
    out.update(rest)
    return out


def apply_repairs(
    slug: str, worklist: Path, verdicts_path: Path, *, today: str, dry_run: bool
) -> int:
    held = {}
    for line in worklist.read_text().splitlines():
        line = line.strip()
        if line:
            row = json.loads(line)
            held[str(row["id"])] = row

    verdicts: dict[str, dict[str, Any]] = {}
    complaints: list[str] = []
    for number, line in enumerate(verdicts_path.read_text().splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("//"):
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            complaints.append(f"line {number}: unparseable JSON ({exc})")
            continue
        eid = str(row.get("id", ""))
        if eid not in held:
            complaints.append(f"line {number}: id {eid!r} is not in the worklist")
            continue
        if row.get("verdict") not in VALID_VERDICTS:
            complaints.append(f"line {number}: bad verdict {row.get('verdict')!r}")
            continue
        if eid in verdicts:
            complaints.append(f"line {number}: duplicate verdict for {eid}")
            continue
        verdicts[eid] = row

    missing = sorted(set(held) - set(verdicts))
    if missing:
        complaints.append(
            f"{len(missing)} worklist id(s) got no verdict: {missing[:5]}"
            + (" ..." if len(missing) > 5 else "")
        )

    subject_dir = EXPECTATIONS_DIR / slug
    by_field: dict[str, list[dict[str, Any]]] = {f: [] for f in SEARCH_FIELDS}
    counts = {"reanchor": 0, "retire": 0, "dispute": 0}

    for eid, verdict_row in verdicts.items():
        source = held[eid]
        field_name = source["field"]
        verdict = verdict_row["verdict"]
        entry: dict[str, Any] = {
            "id": eid,
            "name": source.get("name", ""),
            "acceptable_forms": list(
                verdict_row.get("acceptable_forms") or source.get("acceptable_forms") or []
            ),
        }
        if source.get("actor"):
            entry["actor"] = source["actor"]

        if verdict == "retire":
            entry["status"] = "retired"
            # A retired entry keeps its LEGACY evidence: the validator exempts
            # retired rows from the quote contract precisely so a judgment can
            # be recorded about text that no longer exists.
            entry["evidence"] = [
                {"quote": q} for q in (source.get("legacy_evidence") or [])
            ][:1]
            note = verdict_row.get("reason") or verdict_row.get("notes") or ""
            entry["notes"] = (
                f"retired at the markdown cutover: {note}" if note
                else "retired at the markdown cutover"
            )
        else:
            entry["status"] = "disputed" if verdict == "dispute" else "confirmed"
            entry["evidence"] = [
                {"quote": str(e.get("quote", ""))}
                for e in (verdict_row.get("evidence") or [])
            ]
            if verdict_row.get("notes"):
                entry["notes"] = verdict_row["notes"]
            elif source.get("notes"):
                entry["notes"] = source["notes"]

        # A re-anchored entry is the agent's word, not the seed's; it re-enters
        # as `confirmed` only after the verification pass, so mark it candidate.
        if verdict == "reanchor":
            entry["status"] = "candidate"

        entry["provenance"] = [
            {"action": "added", "by": "corpus-seed", "date": "2026-08-27",
             "source": "legacy-corpus"},
            {"action": verdict, "by": f"repair-agent-{slug.replace('_', '-')}",
             "date": today, "source": "markdown-cutover"},
        ]
        by_field[field_name].append(entry)
        counts[verdict] += 1

    written = 0
    for field_name in SEARCH_FIELDS:
        incoming = by_field[field_name]
        if not incoming:
            continue
        path = subject_dir / f"{field_name}.yaml"
        if not path.is_file():
            complaints.append(f"{field_name}: no ported file to repair into")
            continue
        payload = yaml.safe_load(path.read_text()) or {}
        text_path = REPO_ROOT / payload["snapshot"]["file"]
        normalized = normalize_spaces(text_path.read_text(errors="replace"))

        # Idempotent by entry id. A repair pass is routinely re-run after an
        # agent fixes the rows this script complained about, and without this
        # the second run appends a DUPLICATE id -- which the loader reports as
        # a schema error, so the retry would break the file it was fixing.
        already = {str(e.get("id")) for e in (payload.get("entries") or [])}
        replayed = [e for e in incoming if e["id"] in already]
        if replayed:
            print(f"  {field_name:<26} {len(replayed)} already applied, skipped")
        incoming = [e for e in incoming if e["id"] not in already]
        if not incoming:
            continue

        # Every re-anchored entry is held to the SAME two contracts the
        # validator errors on, here rather than three steps later: its quote
        # must occur verbatim in the new text, and one of its forms must be
        # covered by that quote. An entry failing either is left out with a
        # complaint -- writing it would put a known-broken row in the eval set.
        usable: list[dict[str, Any]] = []
        for entry in incoming:
            if entry["status"] == "retired":
                usable.append(entry)
                continue
            forms = [str(f) for f in entry["acceptable_forms"]]
            good: list[dict[str, Any]] = []
            for evidence in entry["evidence"]:
                quote = evidence["quote"]
                if len(quote) > MAX_QUOTE_LENGTH:
                    complaints.append(f"{entry['id']}: quote over {MAX_QUOTE_LENGTH} chars")
                    continue
                if not _occurs(quote, normalized, case_sensitive=True):
                    complaints.append(
                        f"{entry['id']}: quote not found in the new text: {quote[:60]!r}"
                    )
                    continue
                record: dict[str, Any] = {"quote": quote}
                offset = _offset_of(quote, normalized)
                if offset is not None:
                    record["approx_offset"] = offset
                good.append(record)
            if not good:
                complaints.append(f"{entry['id']}: no surviving evidence; left out")
                continue
            if not any(
                _occurs(form, normalize_spaces(e["quote"]), case_sensitive=False)
                for e in good for form in forms
            ):
                complaints.append(
                    f"{entry['id']}: no acceptable_form is covered by its own "
                    f"evidence; left out"
                )
                continue
            entry["evidence"] = good
            usable.append(entry)

        entries = list(payload.get("entries") or []) + [_ordered(e) for e in usable]

        def sort_key(entry: dict[str, Any]) -> int:
            offsets = [
                q["approx_offset"] for q in entry.get("evidence", [])
                if q.get("approx_offset") is not None
            ]
            return min(offsets) if offsets else len(normalized)

        payload["entries"] = sorted(entries, key=sort_key)
        payload["eval_set_version"] = int(payload.get("eval_set_version") or 1) + 1
        if not dry_run:
            path.write_text(
                yaml.safe_dump(payload, sort_keys=False, allow_unicode=True, width=110)
            )
        written += 1
        print(f"  {field_name:<26} +{len(usable)} repaired ({len(entries)} total)")

    print(
        f"\n{slug}: {counts['reanchor']} re-anchored, {counts['dispute']} disputed, "
        f"{counts['retire']} retired across {written} file(s)"
    )
    if complaints:
        print(f"\n{len(complaints)} complaint(s):")
        for message in complaints:
            print(f"  - {message}")
    return 1 if complaints else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slug", required=True)
    parser.add_argument("--worklist", required=True, type=Path)
    parser.add_argument("--verdicts", required=True, type=Path)
    parser.add_argument("--date", default=date_cls.today().isoformat())
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return apply_repairs(
        args.slug, args.worklist, args.verdicts, today=args.date, dry_run=args.dry_run
    )


if __name__ == "__main__":
    raise SystemExit(main())
