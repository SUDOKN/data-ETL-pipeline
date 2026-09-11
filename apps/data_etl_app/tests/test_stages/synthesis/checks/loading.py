"""Dump discovery and record loading for the synthesis evaluation.

Encodes the measurement traps the run evidence established (README §traps):
the pairing key always carries the chunk bounds (242 group_ids appear in both
chunks of a field; a bounds-less key silently drops records), contract_products
is a byte-copy of products through synthesis and must not be double counted,
and full-run dumps before 2026-08-26 carry the synthesis only on group rows
(no ``synthesis`` block), so both shapes load here.

Wire shape since 2026-09-05 (location-stage merge, flattened): a synthesis
record's evidence is ``snippets: [str]`` — bare verbatim passages, no
per-snippet location. Location is CODE on the fold (``fold.groups[].mentions[]
.location``: the nearest Markdown heading above the mention, or its table's
header row) and never reaches the model; the harness reads it off the fold
block as a watch number and as pointers for judges (``fold_locations``).
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

# The synthesis request stage and its under-answer retry stage, as the dump's
# ``requests`` map names them. A retried record's accepted paragraph came from
# the retry request, so both stages are part of the evidence trail.
SYNTHESIS_STAGE = "llm_phrase_synthesis"
SYNTHESIS_RETRY_STAGE = "llm_phrase_synthesis_retry"

# subject_name as synthesis prompts received it (PipelineContext.subject_name =
# business_desc.result.name). Since 2026-09-05 the evidence snapshot records
# the name straight off the wire (pull.load_subject_names) and is preferred;
# this map and the business_desc dumps are the fallbacks for older runs.
KNOWN_SUBJECT_NAMES = {
    "alecmfg_com": "Alec Model",
    "steelcraft_com": "Steelcraft",
}


def safe_subject(subject_unique_id: str) -> str:
    """The dump's own subject-to-filename rule (core's ``_safe_path_segment``):
    alphanumerics, ``-`` and ``_`` survive, everything else becomes ``_``.
    ``anchor-mfg.com`` -> ``anchor-mfg_com`` (NOT ``anchor_mfg_com``)."""
    return "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in subject_unique_id
    )


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
    snippets: Optional[int] = None  # distinct snippets sent (synthesis block only)
    mention_count: Optional[int] = None
    retried: Optional[bool] = None  # answered by the under-answer retry pass
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
                snippets = row.get("snippets")
                if snippets is None:
                    snippets = row.get("entries")  # dumps before 2026-09-05
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
                    snippets=snippets,
                    mention_count=row.get("mention_count"),
                    retried=row.get("retried"),
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


def synthesis_requests(
    dump: dict[str, Any], include_retries: bool = True
) -> list[dict[str, Any]]:
    """Every synthesis request entry across the dump's chunks — the group
    requests and, by default, the under-answer retry requests too (list-shaped
    stages; excludes nothing — the caller filters dummies by ``note``)."""
    stages = [SYNTHESIS_STAGE] + ([SYNTHESIS_RETRY_STAGE] if include_retries else [])
    out: list[dict[str, Any]] = []
    for chunk in (dump.get("chunks") or {}).values():
        requests = chunk.get("requests") or {}
        for stage in stages:
            out.extend(requests.get(stage) or [])
    return out


def fold_location_summary(dump: dict[str, Any]) -> dict[str, Any]:
    """Code-located mentions across the dump's fold blocks — a WATCH number.

    ``mentions_located`` counts mentions whose ``locate_context`` found a
    heading or table header row above them; the rest fell to
    ``DEFAULT_LOCATION``. The model never sees either (it reads the chunk
    text), so coverage is a property of the corpus + locator, not of the
    synthesis — tracked, never gated.
    """
    mentions = 0
    located = 0
    chunks_with_fold = 0
    for chunk in (dump.get("chunks") or {}).values():
        summary = (chunk.get("fold") or {}).get("summary") or {}
        if not summary:
            continue
        chunks_with_fold += 1
        mentions += summary.get("mentions") or 0
        located += summary.get("mentions_located") or 0
    return {
        "chunks_with_fold": chunks_with_fold,
        "mentions": mentions,
        "located": located,
        "coverage": round(located / mentions, 4) if mentions else None,
    }


def fold_locations(dump: dict[str, Any]) -> dict[tuple[str, str], list[str]]:
    """(chunk_bounds, group_id) -> the distinct code-derived locations of the
    group's mentions, in first-seen order (``None`` = unlocated, dropped).

    These are POINTERS for a judge into the chunk text (the heading or table
    header the mention sits under), not evidence the model was shown.
    """
    out: dict[tuple[str, str], list[str]] = {}
    for chunk_bounds, chunk in (dump.get("chunks") or {}).items():
        for group in ((chunk.get("fold") or {}).get("groups") or []):
            seen: list[str] = []
            for mention in group.get("mentions") or []:
                location = mention.get("location")
                if location and location not in seen:
                    seen.append(location)
            out[(chunk_bounds, group.get("group_id", ""))] = seen
    return out


def fold_group_forms(dump: dict[str, Any]) -> dict[str, dict[str, list[str]]]:
    """``{chunk_bounds: {group_id: member forms}}`` for every SYNTHESIZED
    group of the fold block — the groups that became records (status
    ``ok``: not empty, not collapsed), the same set ``FoldResult.
    synthesis_records`` yields in core."""
    out: dict[str, dict[str, list[str]]] = {}
    for chunk_bounds, chunk in (dump.get("chunks") or {}).items():
        groups = ((chunk.get("fold") or {}).get("groups")) or []
        out[chunk_bounds] = {
            g["group_id"]: [f for f in (g.get("forms") or []) if f and f.strip()]
            for g in groups
            if g.get("group_id") and g.get("status") not in ("no_mentions", "collapsed")
        }
    return out


def fold_sibling_forms(dump: dict[str, Any]) -> dict[tuple[str, str], list[str]]:
    """``(chunk_bounds, group_id) -> the member forms of every OTHER
    synthesized group of that chunk`` — the sibling forms core's designation
    trigger cuts its spans at (``sibling_forms_by_record`` in the synthesis
    node service does the same over the live fold)."""
    out: dict[tuple[str, str], list[str]] = {}
    for chunk_bounds, groups in fold_group_forms(dump).items():
        for group_id in groups:
            out[(chunk_bounds, group_id)] = [
                form
                for other_id, forms in groups.items()
                if other_id != group_id
                for form in forms
            ]
    return out


def synthesis_pv(dump: dict[str, Any]) -> Optional[str]:
    """The prompt version the header claims for the synthesis stage."""
    meta = ((dump.get("run") or {}).get("extraction_metadata") or {}).get(
        SYNTHESIS_STAGE
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
    earlier run's) business_desc dump. (run_eval consults the evidence
    snapshot's wire-recorded names before falling back to this.)"""
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
