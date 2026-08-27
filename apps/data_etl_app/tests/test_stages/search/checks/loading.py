"""Load one run's search-stage material into per-(subject, field) window
records the metric battery consumes.

Sources, in order of authority:
- the run's extraction dumps (request usage/timing; `phrases` and `sub_bounds`
  on dumps written after 2026-08-26);
- the pulled Mongo request file (wire user messages — the exact text each
  window read — plus response content for dumps that predate the phrases
  block). See pull.py; the pulled file is never committed (it holds site text).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from masking import scan_domain, wire_text_from_user_message  # noqa: E402
from paths import DUMP_ROOT, SEARCH_FIELDS  # noqa: E402

SEARCH_ID_RE = re.compile(
    r"^(?P<subject>[^>]+)>(?P<field>[^>]+)>llm_search>chunk>(?P<chunk>[0-9:]+)"
    r">sub>(?P<sub>[0-9:]+)>(?P<segment>.+)$"
)
RECURSIVE_ID_RE = re.compile(
    r"^(?P<subject>[^>]+)>(?P<field>[^>]+)>llm_recursive_search>round>(?P<round>\d+)"
    r">chunk>(?P<chunk>[0-9:]+)>sub>(?P<sub>[0-9:]+)>(?P<segment>.+)$"
)
PV_RE = re.compile(r"\|pv=([^|>]+)")
CAP_RE = re.compile(r"max_completion_tokens=(\d+)")


@dataclass
class WindowRecord:
    """One search request: a sub-window of a chunk, its phrases, its wire text."""

    subject: str
    field: str
    chunk_bounds: str
    sub_bounds: str
    custom_id: str
    round_index: Optional[int] = None  # None = first search; 0.. = recursive
    phrases: Optional[list[str]] = None  # None = response unavailable
    phrases_note: Optional[str] = None
    wire_text: Optional[str] = None
    domain: Optional[str] = None  # scan-domain-masked wire text
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    created_at: Optional[str] = None
    finish_reason: Optional[str] = None
    unparseable_content: bool = False

    @property
    def completion_cap(self) -> Optional[int]:
        m = CAP_RE.search(self.custom_id)
        return int(m.group(1)) if m else None

    @property
    def prompt_version(self) -> Optional[str]:
        m = PV_RE.search(self.custom_id)
        return m.group(1) if m else None


@dataclass
class FieldRun:
    """Everything the battery needs for one (subject, field) of one run."""

    run_id: str
    subject: str
    field: str
    windows: list[WindowRecord] = dc_field(default_factory=list)
    metadata: dict[str, Any] = dc_field(default_factory=dict)  # run header slice
    scraped_text: dict[str, Any] = dc_field(default_factory=dict)
    dump_path: Optional[str] = None

    @property
    def chunk_bounds(self) -> list[str]:
        seen: list[str] = []
        for w in self.windows:
            if w.chunk_bounds not in seen:
                seen.append(w.chunk_bounds)
        return seen


def _parse_custom_id(custom_id: str) -> Optional[dict[str, Any]]:
    m = SEARCH_ID_RE.match(custom_id)
    if m:
        return {**m.groupdict(), "round": None}
    m = RECURSIVE_ID_RE.match(custom_id)
    if m:
        d = m.groupdict()
        d["round"] = int(d["round"])
        return d
    return None


def load_run(run_id: str, pulled_file: Optional[Path] = None) -> list[FieldRun]:
    """Assemble FieldRuns for every search field with a dump in this run.

    ``pulled_file`` is pull.py's output; without it, wire texts are absent and
    phrases exist only if the dump carries them (dumps written after
    2026-08-26). The battery degrades explicitly, never silently: metrics that
    need what is missing report ``skipped_missing_input``.
    """
    dump_dir = DUMP_ROOT / run_id
    if not dump_dir.is_dir():
        raise FileNotFoundError(f"no dump directory {dump_dir}")

    pulled: dict[str, dict[str, Any]] = {}
    if pulled_file is not None and Path(pulled_file).is_file():
        for doc in json.loads(Path(pulled_file).read_text()):
            cid = doc.get("custom_id")
            if cid:
                pulled[cid] = doc

    field_runs: list[FieldRun] = []
    for dump_path in sorted(dump_dir.glob("*.json")):
        payload = json.loads(dump_path.read_text())
        fname = payload.get("field_type")
        subject = payload.get("subject_unique_id")
        if fname not in SEARCH_FIELDS or not subject:
            continue
        run_block = payload.get("run", {}) or {}
        meta = (run_block.get("extraction_metadata") or {})
        fr = FieldRun(
            run_id=run_id,
            subject=subject,
            field=fname,
            metadata=meta,
            scraped_text=run_block.get("scraped_text") or {},
            dump_path=str(dump_path),
        )
        for chunk_bounds, chunk in (payload.get("chunks") or {}).items():
            requests = (chunk or {}).get("requests") or {}
            entries = list(requests.get("llm_phrase_search") or [])
            recursive = requests.get("llm_phrase_recursive_search") or {}
            for sub_rounds in recursive.values():
                entries.extend(sub_rounds)
            for entry in entries:
                record = _window_from_entry(entry, pulled)
                if record is not None:
                    fr.windows.append(record)
        fr.windows.sort(
            key=lambda w: (
                int(w.chunk_bounds.split(":")[0]),
                int(w.sub_bounds.split(":")[0]),
                -1 if w.round_index is None else w.round_index,
            )
        )
        field_runs.append(fr)
    return field_runs


def _window_from_entry(
    entry: dict[str, Any], pulled: dict[str, dict[str, Any]]
) -> Optional[WindowRecord]:
    custom_id = entry.get("custom_id")
    if not isinstance(custom_id, str):
        return None
    parsed = _parse_custom_id(custom_id)
    if parsed is None:
        return None
    record = WindowRecord(
        subject=parsed["subject"],
        field=parsed["field"],
        chunk_bounds=parsed["chunk"],
        sub_bounds=parsed["sub"],
        custom_id=custom_id,
        round_index=parsed["round"],
        input_tokens=entry.get("input_tokens"),
        output_tokens=entry.get("output_tokens"),
        created_at=entry.get("created_at"),
        finish_reason=entry.get("finish_reason"),
    )
    phrases = entry.get("phrases")
    if isinstance(phrases, list):
        record.phrases = [p for p in phrases if isinstance(p, str)]
    record.phrases_note = entry.get("phrases_note")

    doc = pulled.get(custom_id)
    if doc:
        user_message = doc.get("user_message")
        if isinstance(user_message, str):
            record.wire_text = wire_text_from_user_message(user_message)
            record.domain = scan_domain(record.wire_text)
        if record.finish_reason is None and doc.get("finish_reason"):
            record.finish_reason = doc["finish_reason"]
        if record.phrases is None:
            content = doc.get("content")
            if isinstance(content, str):
                try:
                    payload = json.loads(content)
                    plist = (
                        payload.get("phrases")
                        if isinstance(payload, dict)
                        else None
                    )
                except ValueError:
                    plist = None
                if isinstance(plist, list) and all(
                    isinstance(p, str) for p in plist
                ):
                    record.phrases = plist
                else:
                    record.unparseable_content = True
    return record


def run_started_at(run_id: str) -> datetime:
    return datetime.strptime(run_id, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)


def is_live(record: WindowRecord, run_id: str, tolerance_seconds: int = 600) -> Optional[bool]:
    """True = dispatched by THIS run; False = replayed from an earlier run.
    None = no created_at to judge by. created_at is stamped at orchestrator
    run start for requests the run creates, so proximity to the run id is the
    discriminator (replays keep their original timestamps)."""
    if not record.created_at:
        return None
    try:
        created = datetime.fromisoformat(record.created_at)
    except ValueError:
        return None
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    delta = abs((created - run_started_at(run_id)).total_seconds())
    return delta <= tolerance_seconds
