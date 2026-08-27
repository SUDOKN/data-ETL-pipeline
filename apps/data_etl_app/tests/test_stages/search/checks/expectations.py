"""Load and validate the evolving expectation set (see EXPECTATIONS_SCHEMA.md)."""

from __future__ import annotations

import sys
from dataclasses import dataclass, field as dc_field
from pathlib import Path
from typing import Any, Optional

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
from paths import EXPECTATIONS_DIR, subject_slug  # noqa: E402

REQUIRED_ENTRY_KEYS = {"id", "name", "status", "acceptable_forms", "evidence", "provenance"}
VALID_STATUSES = {"candidate", "confirmed", "disputed", "retired"}


@dataclass
class Expectations:
    subject: str
    field: str
    path: str
    eval_set_version: int = 0
    expected_empty: bool = False
    entries: list[dict[str, Any]] = dc_field(default_factory=list)
    false_friends: list[dict[str, Any]] = dc_field(default_factory=list)
    problems: list[str] = dc_field(default_factory=list)  # schema violations found on load

    def entries_with_status(self, *statuses: str) -> list[dict[str, Any]]:
        return [e for e in self.entries if e.get("status") in statuses]


def load_expectations(subject: str, field_name: str) -> Optional[Expectations]:
    path = EXPECTATIONS_DIR / subject_slug(subject) / f"{field_name}.yaml"
    if not path.is_file():
        return None
    payload = yaml.safe_load(path.read_text()) or {}
    exp = Expectations(
        subject=subject,
        field=field_name,
        path=str(path),
        eval_set_version=int(payload.get("eval_set_version") or 0),
        expected_empty=bool(payload.get("expected_empty", False)),
        entries=list(payload.get("entries") or []),
        false_friends=list(payload.get("false_friends") or []),
    )
    seen_ids: set[str] = set()
    for entry in exp.entries:
        missing = REQUIRED_ENTRY_KEYS - set(entry)
        if missing:
            exp.problems.append(f"{entry.get('id', '<no id>')}: missing {sorted(missing)}")
        if entry.get("status") not in VALID_STATUSES:
            exp.problems.append(f"{entry.get('id')}: bad status {entry.get('status')!r}")
        eid = entry.get("id")
        if eid in seen_ids:
            exp.problems.append(f"duplicate id {eid}")
        if isinstance(eid, str):
            seen_ids.add(eid)
    return exp
