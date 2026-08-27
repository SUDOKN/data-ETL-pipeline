"""The cumulative verdict ledger — the "exhaustive + cache" policy.

Every agent judgment of a record is stored once, keyed by CONTENT: the
paragraph, the evidence it was written from, the prompt version that produced
it, and the taxonomy version that judged it. Replayed records are byte-identical
across runs, so a later run re-judges only what actually changed; the ledger
accumulates into a growing ground-truth bank.

One JSONL file per (subject, field) under history/judgments/. A verdict row:

  {"content_key": "...", "subject": "...", "field": "...",
   "group_id": "...", "chunk_bounds": "...", "focal_form": "...",
   "taxonomy_version": "...", "pv": "...", "evidence_sha256": "...",
   "first_judged_run": "...", "judge": "agent:<short description>",
   "checks": {"J1": {"verdict": "pass|fail|unclear", "severity": "minor|major",
                     "note": "...", "quote": "..."}, ...},
   "verified": false}

``checks`` carries only the dimensions TAXONOMY.md defines for the field;
``verified: true`` marks rows a human/session re-checked against the dumps.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Optional

from . import loading, pull

LEDGER_ROOT = loading.EVAL_ROOT / "history" / "judgments"


def content_key(
    group_id: str,
    synthesis: str,
    evidence_sha256: Optional[str],
    pv: Optional[str],
    taxonomy_version: str,
) -> str:
    payload = "\x1f".join(
        [group_id, synthesis, evidence_sha256 or "-", pv or "-", taxonomy_version]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def _ledger_path(subject: str, field_name: str) -> Path:
    return LEDGER_ROOT / f"{subject}__{field_name}.jsonl"


def load_ledger(subject: str, field_name: str) -> dict[str, dict[str, Any]]:
    path = _ledger_path(subject, field_name)
    if not path.is_file():
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        rows[row["content_key"]] = row  # last write wins
    return rows


def append_verdicts(
    subject: str, field_name: str, rows: Iterable[dict[str, Any]]
) -> int:
    LEDGER_ROOT.mkdir(parents=True, exist_ok=True)
    path = _ledger_path(subject, field_name)
    existing = load_ledger(subject, field_name)
    added = 0
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            if row["content_key"] in existing:
                continue
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            existing[row["content_key"]] = row
            added += 1
    return added


def key_for_record(
    record: loading.SynthRecord,
    evidence_index: Optional[dict[str, Any]],
    pv: Optional[str],
    taxonomy_version: str,
) -> Optional[str]:
    if not record.synthesis:
        return None
    ev = (
        pull.evidence_for(evidence_index, record)
        if evidence_index is not None
        else None
    )
    return content_key(
        record.group_id,
        record.synthesis,
        (ev or {}).get("evidence_sha256"),
        pv,
        taxonomy_version,
    )


def split_cached_pending(
    records: list[loading.SynthRecord],
    evidence_index: Optional[dict[str, Any]],
    pv: Optional[str],
    taxonomy_version: str,
    ledger: dict[str, dict[str, Any]],
) -> tuple[list[tuple[loading.SynthRecord, str]], list[tuple[loading.SynthRecord, str]]]:
    """(cached, pending) as (record, content_key) pairs; unsynthesized records
    are neither (there is nothing to judge)."""
    cached: list[tuple[loading.SynthRecord, str]] = []
    pending: list[tuple[loading.SynthRecord, str]] = []
    for record in records:
        key = key_for_record(record, evidence_index, pv, taxonomy_version)
        if key is None:
            continue
        (cached if key in ledger else pending).append((record, key))
    return cached, pending


def judged_rates(
    records_with_keys: list[tuple[loading.SynthRecord, str]],
    ledger: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Per-dimension fail rates over the judged population of THIS run,
    reading cached verdicts. Denominator = records with a verdict carrying
    that dimension."""
    counts: dict[str, dict[str, int]] = {}
    for _record, key in records_with_keys:
        row = ledger.get(key)
        if not row:
            continue
        for dim, verdict in (row.get("checks") or {}).items():
            slot = counts.setdefault(dim, {"judged": 0, "fail": 0, "unclear": 0})
            slot["judged"] += 1
            outcome = (verdict or {}).get("verdict")
            if outcome == "fail":
                slot["fail"] += 1
            elif outcome == "unclear":
                slot["unclear"] += 1
    return {
        dim: {
            **slot,
            "fail_rate": round(slot["fail"] / slot["judged"], 4)
            if slot["judged"]
            else None,
        }
        for dim, slot in sorted(counts.items())
    }
