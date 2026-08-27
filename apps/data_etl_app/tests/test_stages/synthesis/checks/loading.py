"""Dump discovery and record loading for the synthesis evaluation.

Encodes the measurement traps the run evidence established (README §traps):
the pairing key always carries the chunk bounds (242 group_ids appear in both
chunks of a field; a bounds-less key silently drops records), contract_products
is a byte-copy of products through synthesis and must not be double counted,
and full-run dumps before 2026-08-26 carry the synthesis only on group rows
(no ``synthesis`` block), so both shapes load here.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field as dc_field
from pathlib import Path
from typing import Any, Iterator, Optional

REPO_ROOT = Path(__file__).resolve().parents[6]
DUMPS_ROOT = REPO_ROOT / "packages" / "logs" / "extraction_dumps"
EVAL_ROOT = Path(__file__).resolve().parents[1]

# The seven fields with a synthesis stage. contract_products shares every
# phrase stage with products through synthesis (one LLM call, two dump files),
# so synthesis metrics count it once, under products.
SYNTHESIS_FIELDS = [
    "products",
    "contract_products",
    "equipments",
    "industries",
    "conformity_attestations",
    "material_caps",
    "process_caps",
]
SHARED_DUPLICATE = "contract_products"

# subject_name as synthesis prompts received it (PipelineContext.subject_name =
# business_desc.result.name). The dump does not record it; business_desc dumps
# only exist on the run that first extracted the field. Extend on each new
# subject's first run (README §protocol step 2).
KNOWN_SUBJECT_NAMES = {
    "alecmfg_com": "Alec Model",
    "steelcraft_com": "Steelcraft",
}


@dataclass
class SynthRecord:
    """One synthesis record, keyed the only safe way: with chunk bounds."""

    subject: str  # dump-safe, e.g. "alecmfg_com"
    field: str
    chunk_bounds: str
    group_id: str
    focal_form: Optional[str]
    synthesis: Optional[str]
    key: Optional[str] = None
    forms: list[str] = dc_field(default_factory=list)
    status: Optional[str] = None  # row status (full run) or synthesis status
    entries: Optional[int] = None  # distinct snippets sent (synthesis block only)
    mention_count: Optional[int] = None
    raw_row: dict[str, Any] = dc_field(default_factory=dict)

    @property
    def pair_key(self) -> tuple[str, str, str, str]:
        return (self.subject, self.field, self.chunk_bounds, self.group_id)


def list_runs() -> list[str]:
    """Run ids on disk, oldest first (dir names sort lexicographically)."""
    if not DUMPS_ROOT.is_dir():
        return []
    return sorted(p.name for p in DUMPS_ROOT.iterdir() if p.is_dir())


def latest_run() -> Optional[str]:
    runs = list_runs()
    return runs[-1] if runs else None


def run_dir(run_id: str) -> Path:
    return DUMPS_ROOT / run_id


def dump_files(run_id: str) -> dict[tuple[str, str], Path]:
    """(subject, field) -> dump path for the run's synthesis-bearing fields.

    Filenames are ``{subject}__{field}[__partial].json``; field names contain
    single underscores only, so the double-underscore split is safe.
    """
    out: dict[tuple[str, str], Path] = {}
    for path in sorted(run_dir(run_id).glob("*.json")):
        parts = path.stem.split("__")
        if len(parts) < 2:
            continue
        subject, field_name = parts[0], parts[1]
        if field_name in SYNTHESIS_FIELDS:
            out[(subject, field_name)] = path
    return out


def load_dump(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def iter_records(
    subject: str, field_name: str, dump: dict[str, Any]
) -> Iterator[SynthRecord]:
    """Yield every synthesis record of one dump, preferring the synthesis
    block (partial dumps; full dumps since the 2026-08-26 instrument fix) and
    falling back to the group rows a pre-fix full run carries."""
    for chunk_bounds, chunk in (dump.get("chunks") or {}).items():
        block = chunk.get("synthesis")
        if block and block.get("records"):
            for row in block["records"]:
                yield SynthRecord(
                    subject=subject,
                    field=field_name,
                    chunk_bounds=chunk_bounds,
                    group_id=row["group_id"],
                    focal_form=row.get("focal_form"),
                    synthesis=row.get("synthesis"),
                    key=row.get("key"),
                    forms=list(row.get("forms") or []),
                    status=row.get("status"),
                    entries=row.get("entries"),
                    mention_count=row.get("mention_count"),
                    raw_row=row,
                )
            continue
        for row in chunk.get("rows") or []:
            record = row.get("record")
            if record is None and "group_id" not in row:
                continue  # v2-era phrase rows have no group spine
            yield SynthRecord(
                subject=subject,
                field=field_name,
                chunk_bounds=chunk_bounds,
                group_id=row.get("group_id", ""),
                focal_form=(record or {}).get("focal_form") or row.get("focal_form"),
                synthesis=(record or {}).get("synthesis"),
                key=row.get("key"),
                forms=list(row.get("forms") or []),
                status=row.get("status"),
                mention_count=row.get("mention_count"),
                raw_row=row,
            )


def has_synthesis_block(dump: dict[str, Any]) -> bool:
    return any(
        (chunk.get("synthesis") or {}).get("records")
        for chunk in (dump.get("chunks") or {}).values()
    )


def synthesis_requests(dump: dict[str, Any]) -> list[dict[str, Any]]:
    """Every synthesis request entry across the dump's chunks (list-shaped
    stage; excludes nothing — the caller filters dummies by ``note``)."""
    out: list[dict[str, Any]] = []
    for chunk in (dump.get("chunks") or {}).values():
        stage = (chunk.get("requests") or {}).get("llm_phrase_synthesis") or []
        out.extend(stage)
    return out


def synthesis_pv(dump: dict[str, Any]) -> Optional[str]:
    """The prompt version the header claims for the synthesis stage."""
    meta = ((dump.get("run") or {}).get("extraction_metadata") or {}).get(
        "llm_phrase_synthesis"
    ) or {}
    return meta.get("prompt_version_id")


def pv_of_custom_id(custom_id: str) -> Optional[str]:
    for part in custom_id.split("|"):
        if part.startswith("pv="):
            return part[3:]
    return None


def ud_of_custom_id(custom_id: str) -> Optional[str]:
    for part in custom_id.split("|"):
        if part.startswith("ud="):
            return part[3:]
    return None


def subject_display_name(subject: str, run_id: str) -> Optional[str]:
    """The subject_name the prompts saw: the known map, else the run's (or any
    earlier run's) business_desc dump."""
    if subject in KNOWN_SUBJECT_NAMES:
        return KNOWN_SUBJECT_NAMES[subject]
    for candidate_run in reversed([r for r in list_runs() if r <= run_id]):
        for suffix in ("", "__partial"):
            path = run_dir(candidate_run) / f"{subject}__business_desc{suffix}.json"
            if path.is_file():
                dump = load_dump(path)
                for chunk in (dump.get("chunks") or {}).values():
                    name = ((chunk.get("result") or {}) or {}).get("name")
                    if name:
                        return name
    return None
