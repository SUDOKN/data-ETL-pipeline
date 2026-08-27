"""Apply a verification pass's verdicts to a subject's expectation files.

Promotion to `confirmed` is the only status that gates recall, so it must be a
recorded act with an author and a date, never a silent edit. This script is that
act: it reads the verifying agent's JSONL, applies each verdict, appends a
provenance row naming the verifier, and bumps `eval_set_version`.

Verdicts (see VERIFY_BRIEF.md):
    confirm  -> status: confirmed
    dispute  -> status: disputed  (never gates; `reason` kept in notes)
    retire   -> status: retired   (kept, never deleted)
    amend    -> corrected forms/quote applied, then confirmed

`miss` rows are appended as NEW `candidate` entries: the verifying pass is their
first reading, so finding them does not confirm them.

An entry with no verdict is left exactly as it was and reported at the end.
Silence is not consent - an unjudged entry stays `candidate` and stops gating,
and this script says so out loud rather than letting it pass unnoticed.

Usage:
    .venv/bin/python checks/apply_verifications.py --slug tanfel_com \
        --jsonl <path>.jsonl --by verify-agent-tanfel [--date 2026-08-27]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from _shared.text_matching import flexible_pattern, normalize_spaces  # noqa: E402
from paths import EXPECTATIONS_DIR, REPO_ROOT, SEARCH_FIELDS  # noqa: E402

VERDICT_STATUS = {
    "confirm": "confirmed",
    "dispute": "disputed",
    "retire": "retired",
    "amend": "confirmed",
}


def _offset_of(quote: str, normalized_text: str) -> int | None:
    """Takes ALREADY-normalized text; normalization is length-preserving so the
    offset indexes the raw text too. See build_expectations for why it is not
    normalized per quote."""
    pattern = flexible_pattern(quote, case_sensitive=True)
    if pattern is None:
        return None
    match = pattern.search(normalized_text)
    return match.start() if match else None


def apply(slug: str, jsonl_path: Path, by: str, date: str) -> int:
    subject_dir = EXPECTATIONS_DIR / slug
    if not subject_dir.is_dir():
        raise SystemExit(f"no expectations for {slug!r}")

    verdicts: dict[str, dict[str, Any]] = {}
    misses: list[dict[str, Any]] = []
    complaints: list[str] = []
    rejected_misses: list[str] = []
    for line_number, raw in enumerate(jsonl_path.read_text().splitlines(), 1):
        raw = raw.strip()
        if not raw or raw.startswith("//"):
            continue
        try:
            row = json.loads(raw)
        except json.JSONDecodeError as exc:
            complaints.append(f"line {line_number}: unparseable JSON ({exc})")
            continue
        if row.get("type") == "miss":
            misses.append(row)
            continue
        entry_id = row.get("id")
        verdict = row.get("verdict")
        if not entry_id or verdict not in VERDICT_STATUS:
            complaints.append(f"line {line_number}: bad verdict row {row!r}")
            continue
        if entry_id in verdicts:
            complaints.append(f"line {line_number}: duplicate verdict for {entry_id}")
        verdicts[entry_id] = row

    applied = {name: 0 for name in VERDICT_STATUS}
    unjudged: list[str] = []
    added = 0
    seen_ids: set[str] = set()

    for field_name in SEARCH_FIELDS:
        path = subject_dir / f"{field_name}.yaml"
        if not path.is_file():
            continue
        payload = yaml.safe_load(path.read_text()) or {}
        text_rel = (payload.get("snapshot") or {}).get("file", "")
        text = (REPO_ROOT / text_rel).read_text(errors="replace") if text_rel else ""
        normalized_text = normalize_spaces(text)
        entries = list(payload.get("entries") or [])
        touched = False

        for entry in entries:
            entry_id = str(entry.get("id"))
            seen_ids.add(entry_id)
            row = verdicts.get(entry_id)
            if row is None:
                if entry.get("status") == "candidate":
                    unjudged.append(entry_id)
                continue
            verdict = row["verdict"]
            if verdict == "amend":
                if row.get("corrected_forms"):
                    entry["acceptable_forms"] = list(row["corrected_forms"])
                if row.get("corrected_quote"):
                    quote = row["corrected_quote"]
                    record: dict[str, Any] = {"quote": quote}
                    offset = _offset_of(quote, normalized_text)
                    if offset is None:
                        complaints.append(
                            f"{entry_id}: corrected_quote absent from text: {quote[:60]!r}"
                        )
                    else:
                        record["approx_offset"] = offset
                    # APPEND, never replace. Recall is window-scoped: an entry
                    # counts against a run only if one of its quotes falls in a
                    # window that run searched, so every quote dropped here is
                    # window coverage silently lost. A quote that fails to cover
                    # a form is useless, not harmful - the fix is to add a good
                    # one beside it. Pass `replace_evidence: true` to drop the
                    # old quotes deliberately, when one is actually wrong.
                    existing_quotes = [] if row.get("replace_evidence") else list(
                        entry.get("evidence") or []
                    )
                    if not any(
                        e.get("quote") == quote for e in existing_quotes
                    ):
                        existing_quotes.append(record)
                    entry["evidence"] = existing_quotes
            entry["status"] = VERDICT_STATUS[verdict]
            provenance_row: dict[str, Any] = {
                "action": {"confirm": "verified", "amend": "amended",
                           "dispute": "disputed", "retire": "retired"}[verdict],
                "by": by,
                "date": date,
            }
            if row.get("reason"):
                provenance_row["reason"] = row["reason"]
            entry.setdefault("provenance", []).append(provenance_row)
            if row.get("reason") and verdict in {"dispute", "retire"}:
                existing_note = entry.get("notes", "")
                entry["notes"] = (
                    f"{existing_note} | {row['reason']}" if existing_note else row["reason"]
                )
            applied[verdict] += 1
            touched = True

        field_misses = [m for m in misses if m.get("field") == field_name]
        if field_misses:
            next_index = len(entries) + 1
            for miss in field_misses:
                evidence = []
                for item in miss.get("evidence") or []:
                    quote = item.get("quote", "") if isinstance(item, dict) else str(item)
                    record = {"quote": quote}
                    offset = _offset_of(quote, normalized_text)
                    if offset is None:
                        complaints.append(
                            f"{slug}/{field_name} miss {miss.get('name')!r}: "
                            f"quote absent from text: {quote[:60]!r}"
                        )
                    else:
                        record["approx_offset"] = offset
                    evidence.append(record)
                # A miss whose quote is not in the text cannot be an entry: the
                # quote IS the evidence, and validate_expectations would fail it
                # immediately. Verifiers occasionally paraphrase or elide one.
                # Refusing it here keeps a broken entry out of the eval set
                # instead of adding it and failing validation afterwards.
                if evidence and not any("approx_offset" in e for e in evidence):
                    rejected_misses.append(
                        f"{field_name}: {miss.get('name')!r} (no verbatim quote)"
                    )
                    continue
                new_entry: dict[str, Any] = {
                    "id": f"{slug}-{field_name}-{next_index:04d}",
                    "name": miss.get("name", ""),
                    "status": "candidate",
                    "acceptable_forms": list(miss.get("acceptable_forms") or []),
                    "evidence": evidence,
                }
                if miss.get("actor") and miss["actor"] != "own":
                    new_entry["actor"] = miss["actor"]
                if miss.get("reason"):
                    new_entry["notes"] = miss["reason"]
                new_entry["provenance"] = [
                    {"action": "added", "by": by, "date": date,
                     "source": "verification-pass-miss"}
                ]
                entries.append(new_entry)
                next_index += 1
                added += 1
                touched = True

        if touched:
            payload["entries"] = entries
            payload["eval_set_version"] = int(payload.get("eval_set_version") or 1) + 1
            path.write_text(
                yaml.safe_dump(payload, sort_keys=False, allow_unicode=True, width=110)
            )

    orphans = sorted(set(verdicts) - seen_ids)
    print(f"{slug}: " + ", ".join(f"{n} {v}" for v, n in applied.items()))
    print(f"  {added} miss entries added as candidate")
    if rejected_misses:
        print(f"  {len(rejected_misses)} miss entries REJECTED (quote not verbatim):")
        for message in rejected_misses:
            print(f"    - {message}")
    if unjudged:
        print(f"  {len(unjudged)} entries left UNJUDGED (still candidate, not gating):")
        for entry_id in unjudged[:20]:
            print(f"    - {entry_id}")
        if len(unjudged) > 20:
            print(f"    ... and {len(unjudged) - 20} more")
    if orphans:
        print(f"  {len(orphans)} verdicts for ids that do not exist: {orphans[:10]}")
    for message in complaints:
        print(f"  COMPLAINT {message}")
    return 1 if complaints or orphans else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slug", required=True)
    parser.add_argument("--jsonl", required=True, type=Path)
    parser.add_argument("--by", required=True, help="verifier name for provenance")
    parser.add_argument("--date", default="2026-08-27")
    args = parser.parse_args()
    return apply(args.slug, args.jsonl, args.by, args.date)


if __name__ == "__main__":
    raise SystemExit(main())
