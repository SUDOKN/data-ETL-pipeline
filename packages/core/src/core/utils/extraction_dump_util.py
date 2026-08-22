"""The extraction dumps: one diagnostic file per (subject, field) per run.

Every field writes one — multi-stage fields as per-RECORD rows, single-stage
fields (binary classification, addresses, business description) as the parsed
result per chunk — with a shared provenance header and per-request token usage.
Multi-stage row anatomy is documented below; the rest of this docstring is
about those rows.

One row per RECORD the relationship stage deposed, the empty-mentions ones
included: they are the honest not-found branch and their absence downstream
must read as "never asked", not as silence. Each row joins every later stage
through the record_id, with the phrase alongside for the reader.

Row anatomy:

- ``status`` — the branch the record actually took. Deliberately redundant with
  the other fields (a second witness): ``_record_status_from_fields`` re-derives
  it from the row alone and a disagreement is logged as an error, so a future
  bug that makes the fields lie gets caught by the dump instead of shipping.
    * ``no_mentions``    — the honest not-found branch; the record skipped
                           grounding AND screening code-side (fork F9).
    * ``no_candidates``  — every grounding pass declined the record: asked,
                           found nothing, said why.
    * ``screened_out``   — grounding enumerated candidates and screening
                           rejected every one.
    * ``grounded``       — at least one candidate passed screening.
    * ``screening_dropped`` — candidates exist but no screening verdict does.
                           Defensive: the two-axis hold should make this
                           unreachable; until then it must not masquerade as
                           ``screened_out``.
- ``search_round`` / ``provenance`` — where the record's phrase came from.
  Round 0 is reserved for brute-search survivors, so an unmatched phrase is
  ``search_round: null, provenance: "unmatched"`` rather than a fake round 0.
- ``in_vocab_grounding`` / ``oov_grounding`` / ``freehand_grounding`` — the
  enumeration passes: ``{"tags": {...}}`` or ``{"declined": <explanation>}``.
  A pass that never ran has its key OMITTED (the OOV pass is run config), so
  "never asked" and "asked, found nothing" stay distinguishable.
- ``lvl_by_lvl_itps`` — the descent trail (concepts only): every tag sits at an
  explicit level. Each node carries:
    * ``origin`` — which stage produced its rules (``initial_grounding`` /
      ``recursive_grounding`` / ``both``), derived from which rule maps are
      non-empty.
    * ``source`` — how the node entered the dump (``iterative_tagging`` today;
      the slot survives so synthesized rows stay distinguishable if one ever
      returns).
    * ``in_vocab`` — whether ``group_id`` resolves in the ontology.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional

from requests.structures import CaseInsensitiveDict

from llm_providers.models.llm_model import NO_MODEL

from core.models.extraction_schemas.grounding import (
    RecordGroundingEntry,
    RecordGroundingResults,
)
from core.models.extraction_schemas.iterative_tagging import (
    PhraseTrail,
)
from core.models.extraction_schemas.relationship import (
    MaskedLLMPhraseRelationshipResults,
    MaskedPhraseRelationshipRecord,
)
from core.models.extraction_schemas.screening import (
    CandidateScreeningVerdict,
    RecordScreeningResults,
)
from core.models.extraction_schemas.search import LLMSearchResults
from core.models.field_types import ExtractionFieldType
from core.models.skos_concept import Concept
from core.utils.subject_name_lint import count_own_name_hits

logger = logging.getLogger(__name__)

_SOURCE_TRAIL = "iterative_tagging"


def _provenance(
    phrase: str, search_rounds: dict[int, LLMSearchResults]
) -> tuple[Optional[int], str]:
    """Earliest search round carrying *phrase*, as (round, provenance label)."""
    for round_index in sorted(search_rounds.keys()):
        if phrase in search_rounds[round_index]:
            if round_index == 0:
                return 0, "brute"
            return round_index, f"llm_round_{round_index}"
    return None, "unmatched"


def _origin(direct: dict, iterative: dict) -> str:
    if direct and iterative:
        return "both"
    if iterative:
        return "recursive_grounding"
    return "initial_grounding"


def _sorted_levels(
    levels: dict[int, list[dict[str, object]]],
) -> dict[int, list[dict[str, object]]]:
    """Stable output: levels ascending, nodes ordered by (group, parent)."""
    return {
        lvl: sorted(
            levels[lvl],
            key=lambda node: (str(node["group_id"]), str(node["parent_group_id"])),
        )
        for lvl in sorted(levels.keys())
    }


def _grounding_dump(entry: Optional[RecordGroundingEntry]) -> Optional[dict[str, Any]]:
    """One pass's verdict for a record: tags, a declination, or None when the
    pass never saw the record (a no-mentions record is never sent)."""
    if entry is None:
        return None
    if entry.tags:
        return {
            "tags": {
                tag: [rule.model_dump(mode="json") for rule in applied_rules]
                for tag, applied_rules in entry.tags.items()
            }
        }
    return {"declined": entry.explanation}


def _screening_dump(
    verdicts: Optional[dict[str, CandidateScreeningVerdict]],
) -> Optional[dict[str, Any]]:
    if verdicts is None:
        return None
    return {
        candidate: verdict.model_dump(mode="json")
        for candidate, verdict in verdicts.items()
    }


def _record_candidates(*groundings: Optional[RecordGroundingEntry]) -> set[str]:
    candidates: set[str] = set()
    for entry in groundings:
        if entry is not None:
            candidates.update(entry.tags)
    return candidates


def _record_status(
    record: MaskedPhraseRelationshipRecord,
    candidates: set[str],
    verdicts: Optional[dict[str, CandidateScreeningVerdict]],
) -> str:
    if not record.record.mentions:
        return "no_mentions"
    if not candidates:
        return "no_candidates"
    if verdicts is None:
        return "screening_dropped"
    if any(verdict.passed for verdict in verdicts.values()):
        return "grounded"
    return "screened_out"


def _record_status_from_fields(row: dict[str, Any]) -> str:
    """The second witness: re-derive ``status`` from the row's own fields."""
    record = row.get("record")
    if isinstance(record, dict) and not record.get("mentions"):
        return "no_mentions"
    tags: set[str] = set()
    for key in ("in_vocab_grounding", "oov_grounding", "freehand_grounding"):
        pass_dump = row.get(key)
        if isinstance(pass_dump, dict):
            tags.update(pass_dump.get("tags", {}))
    if not tags:
        return "no_candidates"
    screening = row.get("screening")
    if not isinstance(screening, dict):
        return "screening_dropped"
    if any(
        isinstance(verdict, dict) and verdict.get("passed")
        for verdict in screening.values()
    ):
        return "grounded"
    return "screened_out"


def _record_own_name_hits(record: MaskedPhraseRelationshipRecord) -> str:
    """Every model-authored text on the record, joined for the own-name lint."""
    parts = [record.record.synthesis]
    for mention in record.record.mentions:
        parts.append(mention.account)
    return "\n".join(part for part in parts if part)


def _base_record_row(
    *,
    record_id: str,
    entry: MaskedPhraseRelationshipRecord,
    search_rounds: dict[int, LLMSearchResults],
    relationship_repairs: dict[str, str],
    subject_name: Optional[str],
) -> dict[str, Any]:
    search_round, provenance = _provenance(entry.phrase, search_rounds)
    row: dict[str, object] = {
        "record_id": record_id,
        "phrase": entry.phrase,
        "search_round": search_round,
        "provenance": provenance,
        "record": entry.record.model_dump(mode="json"),
    }
    # Present ONLY when the model answered under a different string and the
    # reconciler rewrote it back. Relationship is the one stage left where a
    # repair is possible — every record-keyed stage downstream holds exactly.
    # Absent means "answered verbatim" — do not emit null, or every clean row
    # grows a field to say nothing happened.
    if entry.phrase in relationship_repairs:
        row["phrase_as_answered"] = relationship_repairs[entry.phrase]
    # Record-only lint of the relationship prompt's own-name ban. Present only
    # on violation, like the repair field: a clean row must not grow a field to
    # say nothing happened.
    if subject_name:
        own_name_hits = count_own_name_hits(
            _record_own_name_hits(entry), subject_name
        )
        if own_name_hits:
            row["record_own_name_hits"] = own_name_hits
    return row


def _check_row_status(row: dict[str, Any], status: str) -> None:
    rederived = _record_status_from_fields(row)
    if rederived != status:
        logger.error(
            f"extraction dump row inconsistency for record {row.get('record_id')!r}: "
            f"writer chose status {status!r} but the row's fields read as "
            f"{rederived!r}"
        )


def build_concept_record_rows(
    *,
    masked_flat: MaskedLLMPhraseRelationshipResults,
    in_vocab_flat: RecordGroundingResults,
    oov_flat: Optional[RecordGroundingResults],
    screening_flat: RecordScreeningResults,
    phrase_trails: list[PhraseTrail],
    search_rounds: dict[int, LLMSearchResults],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],
    relationship_repairs: Optional[dict[str, str]] = None,
    subject_name: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Concept-path rows: one per relationship record, the two grounding
    passes, the per-candidate screening verdicts, and the descent trail.

    ``oov_flat`` is None when the OOV pass never ran this run — its key is then
    omitted from every row, so "never asked" cannot read as "found nothing".
    """
    trails_by_record = {trail.phrase: trail for trail in phrase_trails}
    relationship_repairs = relationship_repairs or {}

    rows: list[dict[str, Any]] = []
    for record_id in sorted(masked_flat):
        entry = masked_flat[record_id]
        row = _base_record_row(
            record_id=record_id,
            entry=entry,
            search_rounds=search_rounds,
            relationship_repairs=relationship_repairs,
            subject_name=subject_name,
        )
        in_vocab_entry = in_vocab_flat.get(record_id)
        row["in_vocab_grounding"] = _grounding_dump(in_vocab_entry)
        oov_entry: Optional[RecordGroundingEntry] = None
        if oov_flat is not None:
            oov_entry = oov_flat.get(record_id)
            row["oov_grounding"] = _grounding_dump(oov_entry)
        verdicts = screening_flat.get(record_id)
        row["screening"] = _screening_dump(verdicts)

        trail = trails_by_record.get(record_id)
        levels: dict[int, list[dict[str, Any]]] = {}
        if trail is not None:
            for lvl, itps in trail.lvl_by_lvl_itps.items():
                for itp in itps:
                    node = itp.model_dump(mode="json")
                    node["origin"] = _origin(
                        itp.direct_og_tag_w_rules, itp.iterative_og_tag_w_rules
                    )
                    node["source"] = _SOURCE_TRAIL
                    node["in_vocab"] = itp.group_id in match_label_to_concept_map
                    levels.setdefault(lvl, []).append(node)
        if levels:
            row["lvl_by_lvl_itps"] = _sorted_levels(levels)

        status = _record_status(
            entry, _record_candidates(in_vocab_entry, oov_entry), verdicts
        )
        row["status"] = status
        _check_row_status(row, status)
        rows.append(row)

    return rows


def build_keyword_record_rows(
    *,
    masked_flat: MaskedLLMPhraseRelationshipResults,
    freehand_flat: RecordGroundingResults,
    screening_flat: RecordScreeningResults,
    search_rounds: dict[int, LLMSearchResults],
    relationship_repairs: Optional[dict[str, str]] = None,
    subject_name: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Keyword-path rows. No ontology and no descent, so no levels: the minted
    candidates sit in ``freehand_grounding`` and screening judges each one."""
    relationship_repairs = relationship_repairs or {}

    rows: list[dict[str, Any]] = []
    for record_id in sorted(masked_flat):
        entry = masked_flat[record_id]
        row = _base_record_row(
            record_id=record_id,
            entry=entry,
            search_rounds=search_rounds,
            relationship_repairs=relationship_repairs,
            subject_name=subject_name,
        )
        freehand_entry = freehand_flat.get(record_id)
        row["freehand_grounding"] = _grounding_dump(freehand_entry)
        verdicts = screening_flat.get(record_id)
        row["screening"] = _screening_dump(verdicts)

        status = _record_status(entry, _record_candidates(freehand_entry), verdicts)
        row["status"] = status
        _check_row_status(row, status)
        rows.append(row)

    return rows


def build_partial_record_rows(
    *,
    search_rounds: dict[int, LLMSearchResults],
    masked_flat: Optional[MaskedLLMPhraseRelationshipResults],
    grounding_by_stage: dict[str, Optional[RecordGroundingResults]],
    screening_flat: Optional[RecordScreeningResults],
    relationship_repairs: Optional[dict[str, str]] = None,
    subject_name: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Rows for a run that stopped before reconcile.

    Separate from the two full builders rather than a flag on them, because
    ``status`` cannot be derived here: its values are statements about a chain
    that finished, and a stopped run has no business claiming any of them.
    Rows carry the stage fields and let the reader draw the conclusion.

    A stage that ran but had nothing for a record emits an explicit ``null``; a
    stage that never ran has its key OMITTED. The distinction is the whole
    point of a partial dump — "grounding declined it" and "grounding never
    happened" must not read the same.

    A run stopped before relationship has no records at all, so it falls back
    to one row per searched phrase instead of an empty file.
    """
    relationship_repairs = relationship_repairs or {}
    if masked_flat is None:
        phrases: set[str] = set()
        for round_phrases in search_rounds.values():
            phrases |= set(round_phrases)
        rows: list[dict[str, Any]] = []
        for phrase in sorted(phrases):
            search_round, provenance = _provenance(phrase, search_rounds)
            rows.append(
                {
                    "phrase": phrase,
                    "search_round": search_round,
                    "provenance": provenance,
                }
            )
        return rows

    rows = []
    for record_id in sorted(masked_flat):
        entry = masked_flat[record_id]
        row = _base_record_row(
            record_id=record_id,
            entry=entry,
            search_rounds=search_rounds,
            relationship_repairs=relationship_repairs,
            subject_name=subject_name,
        )
        for stage_key, grounding_flat in grounding_by_stage.items():
            if grounding_flat is None:
                continue
            row[stage_key] = _grounding_dump(grounding_flat.get(record_id))
        if screening_flat is not None:
            row["screening"] = _screening_dump(screening_flat.get(record_id))
        rows.append(row)

    return rows


def _safe_path_segment(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in value
    )


# The descent tree, which is neither a single id nor a flat list.
_TAGGING_TREE_FIELD = "llm_phrase_recursive_tagging_reqs"
_RECURSIVE_SEARCH_FIELD = "llm_phrase_recursive_search_req_ids"
# The single-stage bundle's one request field. It predates the ``_req_id``
# naming convention the suffix sweep below reads, and renaming it would break
# loading every persisted deferred document, so it is special-cased instead.
_SINGLE_STAGE_REQUEST_FIELD = "llm_request_id"
_PARTIAL_RUN_NOTE = (
    "Stage-gated run: it stopped at the first disabled stage, so no final result "
    "was written to the subject. A stage absent from a row's keys did not run; an "
    "explicit null means it ran and had nothing for that phrase."
)


def jsonable_result(result: object) -> object:
    """A parsed stage result as plain JSON data, whatever model shape it has."""
    if hasattr(result, "model_dump"):
        return result.model_dump(mode="json")
    if isinstance(result, list):
        return [jsonable_result(item) for item in result]
    return result


def _to_utc(value: Optional[datetime]) -> Optional[datetime]:
    """Normalize before any subtraction: docs loaded from Mongo carry naive
    UTC datetimes, ones still in memory carry aware ones, and mixing the two
    in arithmetic raises."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _request_entry(
    custom_id: str, completed_requests: Mapping[str, object]
) -> dict[str, object]:
    """One request with the token usage and timing its stored response reports.

    Null tokens never mean zero: ``synthetic_response`` marks a dummy the
    pipeline fabricated for a skipped path (its stored 1/1 usage is fiction, no
    API call happened), and ``usage_unavailable`` marks a request the caller's
    completed map cannot answer for. Both are excluded from the rollups.

    Timing semantics: ``turnaround_seconds`` is deferral→completion — for batch
    runs that includes sitting in Mongo until the batch was uploaded plus
    OpenAI's queue, NOT model speed, which the Batch API does not report. The
    two latency fields are real model-side measurements and exist only on
    eagerly dispatched requests; like the repair fields on rows, they are
    present only when something was actually measured.
    """
    entry: dict[str, object] = {
        "custom_id": custom_id,
        "input_tokens": None,
        "output_tokens": None,
    }
    request_doc = completed_requests.get(custom_id)
    created_at = _to_utc(getattr(request_doc, "created_at", None))
    if created_at is not None:
        entry["created_at"] = created_at.isoformat()
    response = getattr(request_doc, "response", None)
    if response is None:
        entry["note"] = "usage_unavailable"
        return entry
    chat_completion = response.chat_completion_result
    if chat_completion.model == NO_MODEL.name:
        entry["note"] = "synthetic_response"
        return entry
    entry["input_tokens"] = chat_completion.usage.prompt_tokens
    entry["output_tokens"] = chat_completion.usage.completion_tokens
    completed_at = _to_utc(chat_completion.created)
    entry["completed_at"] = completed_at.isoformat()
    if created_at is not None:
        entry["turnaround_seconds"] = round(
            (completed_at - created_at).total_seconds()
        )
    client_latency_ms = getattr(response, "client_latency_ms", None)
    if client_latency_ms is not None:
        entry["client_latency_ms"] = client_latency_ms
    openai_processing_ms = getattr(response, "openai_processing_ms", None)
    if openai_processing_ms is not None:
        entry["openai_processing_ms"] = openai_processing_ms
    return entry


def build_chunk_requests(
    bundle: object, completed_requests: Mapping[str, object]
) -> dict[str, object]:
    """The batch requests that answered for this chunk, keyed by stage, each
    carrying the token usage off its stored response.

    Read off the bundle by field-name suffix rather than a hand-kept list, so a
    stage added later shows up without anyone remembering to add it here.

    Worth carrying even though the header already names the model and prompt pin:
    a custom_id ends in ``model|params|pv|gs``, so these ids are an independent
    witness to the header. If a dump ever claims one prompt version while its
    requests were filed under another, that disagreement is visible in the file
    instead of being something you have to go to Mongo to discover.
    """
    requests: dict[str, object] = {}
    for field_name in sorted(dir(bundle)):
        if field_name == _TAGGING_TREE_FIELD:
            # level -> the descend request per node at that level
            tagging_tree = getattr(bundle, field_name, None)
            if tagging_tree:
                requests["llm_phrase_recursive_tagging"] = {
                    level: [
                        _request_entry(request_id, completed_requests)
                        for request_id in sorted(
                            node.descend_req_id for node in nodes
                        )
                    ]
                    for level, nodes in sorted(tagging_tree.items())
                }
            continue
        if field_name == _RECURSIVE_SEARCH_FIELD:
            # sub-window bounds -> that sub-window's rounds, in round order
            recursive_rounds = getattr(bundle, field_name, None)
            if recursive_rounds:
                requests["llm_phrase_recursive_search"] = {
                    sub_bounds: [
                        _request_entry(request_id, completed_requests)
                        for request_id in round_req_ids
                    ]
                    for sub_bounds, round_req_ids in sorted(recursive_rounds.items())
                }
            continue
        if field_name == _SINGLE_STAGE_REQUEST_FIELD:
            request_id = getattr(bundle, field_name, None)
            if request_id is not None:
                requests["single_stage_extraction"] = [
                    _request_entry(request_id, completed_requests)
                ]
            continue
        if field_name.endswith("_req_id"):
            request_id = getattr(bundle, field_name, None)
            if request_id is not None:
                requests[field_name[: -len("_req_id")]] = [
                    _request_entry(request_id, completed_requests)
                ]
        elif field_name.endswith("_req_ids"):
            request_ids = getattr(bundle, field_name, None)
            if request_ids:
                requests[field_name[: -len("_req_ids")]] = [
                    _request_entry(request_id, completed_requests)
                    for request_id in request_ids
                ]
    return requests


def _stage_entry_lists(
    requests: dict[str, object],
) -> Iterator[tuple[str, list[dict[str, object]]]]:
    """Flatten a chunk's requests block to (stage, entries): the descent tree
    is the one stage whose value is level -> entries rather than a flat list."""
    for stage, value in requests.items():
        if isinstance(value, dict):
            yield stage, [
                entry for level_entries in value.values() for entry in level_entries
            ]
        else:
            yield stage, value


def _time_span(entries: list[dict[str, object]]) -> Optional[dict[str, object]]:
    """Earliest request creation to latest completion, with the bounds kept so
    a suspicious duration is checkable inside the file.

    Only entries carrying both timestamps qualify — a synthetic response's
    fabricated completion time must not stretch (or fake) a span. None when
    nothing qualifies: an absent span reads as "not measurable", where zero
    would read as "instant".
    """
    qualified = [
        entry
        for entry in entries
        if entry.get("created_at") and entry.get("completed_at")
    ]
    if not qualified:
        return None
    started_at = min(
        datetime.fromisoformat(str(entry["created_at"])) for entry in qualified
    )
    ended_at = max(
        datetime.fromisoformat(str(entry["completed_at"])) for entry in qualified
    )
    return {
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "seconds": round((ended_at - started_at).total_seconds()),
    }


def _token_usage_rollup(
    chunks: dict[str, dict[str, object]],
) -> dict[str, object]:
    """Sum per-request usage across every chunk, overall and per stage.

    Entries with null tokens (synthetic responses, unanswerable lookups) are
    excluded rather than counted as zero — the totals claim to be what the run
    actually consumed, and a silent zero would understate that claim invisibly.
    """
    overall_input = 0
    overall_output = 0
    by_stage: dict[str, dict[str, int]] = {}
    for chunk in chunks.values():
        for stage, entries in _stage_entry_lists(chunk["requests"]):
            for entry in entries:
                if entry["input_tokens"] is None:
                    continue
                stage_totals = by_stage.setdefault(
                    stage, {"input_tokens": 0, "output_tokens": 0}
                )
                stage_totals["input_tokens"] += entry["input_tokens"]
                stage_totals["output_tokens"] += entry["output_tokens"]
                overall_input += entry["input_tokens"]
                overall_output += entry["output_tokens"]
    return {
        "input_tokens": overall_input,
        "output_tokens": overall_output,
        "by_stage": {stage: by_stage[stage] for stage in sorted(by_stage)},
    }


def _time_span_rollup(
    chunks: dict[str, dict[str, object]],
) -> Optional[dict[str, object]]:
    """The run's wall-clock span across every chunk, with a per-stage
    breakdown. Stage spans are meaningful because stages are sequential — a
    stage's requests only exist once its predecessor parsed — so the gaps
    between them are real waiting, mostly batch turnaround."""
    stage_entries: dict[str, list[dict[str, object]]] = {}
    for chunk in chunks.values():
        for stage, entries in _stage_entry_lists(chunk["requests"]):
            stage_entries.setdefault(stage, []).extend(entries)
    overall = _time_span(
        [entry for entries in stage_entries.values() for entry in entries]
    )
    if overall is None:
        return None
    overall["by_stage"] = {
        stage: span
        for stage in sorted(stage_entries)
        if (span := _time_span(stage_entries[stage])) is not None
    }
    return overall


def build_run_provenance(
    *,
    metadata: object,
    scraped_text_file: object,
    partial: bool,
    stopped_at: Optional[str] = None,
    stages_run: Optional[list[str]] = None,
    stages_disabled: Optional[list[str]] = None,
) -> dict[str, object]:
    """Everything that decided what this dump contains, for partial and full alike.

    ``metadata`` is dumped whole rather than picked apart into the fields that
    seem to matter. Provenance that lists a chosen subset is provenance that goes
    stale the first time a stage gains a knob, and the reader has no way to tell
    an omitted field from an unset one. It is also the exact object every request
    custom_id was built from, so dumping it verbatim is what makes the ids below
    checkable.

    Safe to read at face value only because a resumed run must agree with the
    live configuration — see ``PrefillNode.raise_if_metadata_is_stale``. Without
    that invariant this block would describe the requests while the prompt bytes
    on the wire came from somewhere else.
    """
    provenance: dict[str, object] = {"partial": partial}
    if partial:
        provenance["stopped_at"] = stopped_at
        provenance["stages_run"] = stages_run
        provenance["stages_disabled"] = stages_disabled
        provenance["note"] = _PARTIAL_RUN_NOTE
    provenance["scraped_text"] = {
        "s3_version_id": scraped_text_file.s3_version_id,
        "num_tokens": scraped_text_file.num_tokens,
        "last_modified_on": scraped_text_file.last_modified_on.isoformat(),
    }
    provenance["extraction_metadata"] = metadata.model_dump(mode="json")
    return provenance


def write_extraction_dump(
    *,
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    timestamp: datetime,
    chunked_contents: dict[str, dict[str, object]],
    chunked_request_map: dict[str, object],
    completed_requests: Mapping[str, object],
    run_provenance: dict[str, object],
    name_suffix: str = "",
) -> None:
    """Write one field's dump, with the provenance that produced it.

    ``chunked_contents`` is what each chunk has to show, already keyed the way
    the reader should meet it: ``{"rows": [...]}`` for a phrase pipeline,
    ``{"result": ...}`` for a single-stage field. The writer adds the
    ``requests`` witness and wall-clock span to each chunk, and the token and
    time rollups to the header; it does not interpret the contents.

    ``completed_requests`` (custom_id -> stored batch request) is what prices
    the dump: per-request usage is read off each stored response, and requests
    it cannot answer for are marked rather than guessed.

    ``run_provenance`` is required rather than optional: it was optional until
    2026-08-13 and only the partial path passed it, so every full dump — the ones
    an evaluation actually reads — recorded nothing about which prompts, models
    or ontology produced it.
    """
    dump_root = Path(__file__).resolve().parents[4] / "logs" / "extraction_dumps"
    dump_dir = dump_root / timestamp.strftime("%Y%m%dT%H%M%S")
    dump_dir.mkdir(parents=True, exist_ok=True)

    dump_path = dump_dir / (
        f"{_safe_path_segment(subject_unique_id)}__"
        f"{_safe_path_segment(field_type.name)}{name_suffix}.json"
    )
    chunks: dict[str, dict[str, object]] = {}
    for chunk_bounds, contents in chunked_contents.items():
        requests = (
            build_chunk_requests(chunked_request_map[chunk_bounds], completed_requests)
            if chunk_bounds in chunked_request_map
            else {}
        )
        chunk: dict[str, object] = {"requests": requests}
        chunk_span = _time_span(
            [entry for _stage, entries in _stage_entry_lists(requests) for entry in entries]
        )
        if chunk_span is not None:
            chunk["time_span"] = chunk_span
        chunk.update(contents)
        chunks[chunk_bounds] = chunk

    # The rollups sit in the run header just before the (bulky) metadata block,
    # so the cost and duration of a run are readable without scrolling past it.
    token_usage = _token_usage_rollup(chunks)
    time_span = _time_span_rollup(chunks)
    run_with_usage: dict[str, object] = {}
    for key, value in run_provenance.items():
        if key == "extraction_metadata":
            run_with_usage["token_usage"] = token_usage
            if time_span is not None:
                run_with_usage["time_span"] = time_span
        run_with_usage[key] = value
    run_with_usage.setdefault("token_usage", token_usage)
    if time_span is not None:
        run_with_usage.setdefault("time_span", time_span)

    dump_payload: dict[str, object] = {
        "subject_unique_id": subject_unique_id,
        "field_type": field_type.name,
        "timestamp": timestamp.isoformat(),
        # Ahead of the chunks on purpose: a reader has to learn what produced
        # them — and, in a partial dump, which stages are missing — before
        # reading a single row, or an absent stage reads as an absent result.
        "run": run_with_usage,
        "chunks": chunks,
    }
    dump_path.write_text(json.dumps(dump_payload, indent=2), encoding="utf-8")
    logger.info("Saved extraction dump to %s", dump_path)
