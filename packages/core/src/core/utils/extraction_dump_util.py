"""The extraction dumps: one diagnostic file per (subject, field) per run.

Every field writes one — multi-stage fields as per-phrase rows, single-stage
fields (binary classification, addresses, business description) as the parsed
result per chunk — with a shared provenance header and per-request token usage.
Multi-stage row anatomy is documented below; the rest of this docstring is
about those rows.

One row per phrase the pipeline SCREENED (plus any phrase a grounding response
introduced that screening never saw — kept visible rather than dropped, since
that drift is itself a defect worth reading). The old dump emitted a row per
GROUNDED phrase, which made ``screening.passed`` a constant ``true`` and hid
exactly the phrases a reviewer most needs: the screened-out majority, the
out-of-vocab proposals, and the initial sentinel verdicts.

Row anatomy:

- ``status`` — the branch the phrase actually took. Deliberately redundant with
  the other fields (a second witness): ``_status_from_fields`` re-derives it
  from the row alone and a disagreement is logged as an error, so a future bug
  that makes the fields lie gets caught by the dump instead of shipping in it.
    * ``screened_out``   — failed relationship screening; no grounding fields.
    * ``grounded``       — at least one real tag survived: an iterative-tagging
                           node or an out-of-vocab proposal.
    * ``no_match``       — grounding saw it and every verdict was a stop
                           (sentinel / false child).
    * ``grounding_dropped`` — passed screening but no grounding response carries
                           it. Defensive: response-vs-input validation should
                           make this unreachable; until then it must not
                           masquerade as ``no_match``.
- ``search_round`` / ``provenance`` — where the phrase came from. Round 0 is
  reserved for brute-search survivors, so an unmatched phrase is
  ``search_round: null, provenance: "unmatched"`` rather than a fake round 0.
- ``lvl_by_lvl_itps`` — option-B shape: every tag sits at an explicit level.
  Tags with no tree position (initial-grounding sentinels and out-of-vocab
  proposals) sit at level 0 with ``parent_group_id: null``; iterative-tagging
  nodes keep their real position. Each node carries:
    * ``origin`` — which stage produced its rules (``initial_grounding`` /
      ``recursive_grounding`` / ``both``), derived from which rule maps are
      non-empty.
    * ``source`` — how the node entered the dump (``iterative_tagging`` or one
      of the synthesized kinds). ``origin`` says who spoke; ``source`` says why
      the row exists — a synthesized node and a trail node can be
      field-identical without it.
    * ``in_vocab`` — whether ``group_id`` resolves in the ontology.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Mapping, Optional

from requests.structures import CaseInsensitiveDict

from llm_providers.models.llm_model import NO_MODEL

from core.models.extraction_schemas.grounding import (
    PhraseToTagAndRulesMap,
    TagToAppliedRulesMap,
    is_sentinel_grounding_label,
)
from core.models.extraction_schemas.iterative_tagging import (
    PhraseTrail,
)
from core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.screening import (
    LiveScreeningResults,
)
from core.models.extraction_schemas.search import LLMSearchResults
from core.models.field_types import ExtractionFieldType
from core.models.skos_concept import Concept
from core.utils.subject_name_lint import count_own_name_hits

logger = logging.getLogger(__name__)

_SOURCE_TRAIL = "iterative_tagging"
# Defensive: an in-vocab direct tag whose concept has no real trail node to
# carry it. Every recognized tag descends now (the exact-label settle policy
# is gone), so this fires only for a concept the level walk stopped elsewhere
# (false child / sentinel kills its direct copies) or failed to seed — the tag
# still has to reach the dump either way.
_SOURCE_UNWALKED = "initial_grounding_unwalked"
_SOURCE_SENTINEL = "initial_grounding_sentinel"
_SOURCE_OOV = "initial_grounding_oov"


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


def _origin(
    direct: TagToAppliedRulesMap, iterative: TagToAppliedRulesMap
) -> str:
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


def _node_is_real(node: dict[str, object]) -> bool:
    """A node asserting a label that reaches results — not a stop record."""
    return node["stop_reason"] is None and not is_sentinel_grounding_label(
        str(node["group_id"])
    )


def _status_from_fields(
    screening_dump: Optional[dict[str, object]],
    levels: dict[int, list[dict[str, object]]],
) -> str:
    """The second witness: re-derive ``status`` from the row's own fields."""
    if screening_dump is not None and not screening_dump.get("passed"):
        return "screened_out"
    all_nodes = [node for nodes in levels.values() for node in nodes]
    if not all_nodes:
        return "grounding_dropped"
    real_nodes = [node for node in all_nodes if _node_is_real(node)]
    if not real_nodes:
        return "no_match"
    return "grounded"


def merge_stage_repairs(
    repairs_by_stage: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    """Invert ``{stage: {phrase: answered}}`` into ``{phrase: {stage: answered}}``.

    Per stage, not one flat map: more than one stage holds its response now, so
    the same phrase can be mis-echoed twice — screening under one drifted string
    and grounding under another. A flat map keeps whichever stage happened to be
    parsed last and loses the other without saying so, which is the exact class
    of silent loss this field exists to end. Stage keys are the row's own field
    names so a reader can line them up.
    """
    merged: dict[str, dict[str, str]] = {}
    for stage, repairs in repairs_by_stage.items():
        for phrase, answered in repairs.items():
            merged.setdefault(phrase, {})[stage] = answered
    return merged


def _finish_row(
    *,
    phrase: str,
    status: str,
    screening_dump: Optional[dict[str, object]],
    relationship: Optional[str],
    levels: dict[int, list[dict[str, object]]],
    search_rounds: dict[int, LLMSearchResults],
    repairs_flat: dict[str, dict[str, str]],
    subject_name: Optional[str] = None,
) -> dict[str, object]:
    rederived = _status_from_fields(screening_dump, levels)
    if rederived != status:
        logger.error(
            f"extraction dump row inconsistency for {phrase!r}: writer chose status "
            f"{status!r} but the row's fields read as {rederived!r}"
        )
    for nodes in levels.values():
        for node in nodes:
            # Rules from a descent imply a parent that descended: a rootless
            # node claiming recursive origin means the join above mislabeled it.
            if node["origin"] != "initial_grounding" and node["parent_group_id"] is None:
                logger.error(
                    f"extraction dump row inconsistency for {phrase!r}: node "
                    f"{node['group_id']!r} claims origin {node['origin']!r} "
                    f"with no parent"
                )
    search_round, provenance = _provenance(phrase, search_rounds)
    row: dict[str, object] = {
        "phrase": phrase,
        "status": status,
        "search_round": search_round,
        "provenance": provenance,
        "relationship": relationship,
        "screening": screening_dump,
    }
    # Present ONLY when the model answered under a different string and the
    # reconciler rewrote it back. Every other field on this row is keyed by the
    # sent phrase, which is what makes the stages joinable; without this the
    # substitution leaves no trace outside a log line and the trail reads as if
    # the model echoed cleanly. Absent means "answered verbatim" — do not emit
    # null, or every clean row grows a field to say nothing happened.
    if phrase in repairs_flat:
        row["phrase_as_answered"] = repairs_flat[phrase]
    # Record-only lint of the relationship prompt's own-name ban. Present only
    # on violation, like the repair field: a clean row must not grow a field to
    # say nothing happened.
    if subject_name and relationship:
        own_name_hits = count_own_name_hits(relationship, subject_name)
        if own_name_hits:
            row["relationship_own_name_hits"] = own_name_hits
    if screening_dump is None:
        # A grounding response introduced this phrase; screening never saw it.
        # Visible on purpose — response-vs-input validation is the real fix.
        row["note"] = "phrase_not_in_screening"
        logger.warning(
            f"phrase {phrase!r} appears in grounding output but was never screened"
        )
    if levels:
        row["lvl_by_lvl_itps"] = _sorted_levels(levels)
    return row


def build_concept_phrase_rows(
    *,
    screening_flat: LiveScreeningResults,
    relationship_flat: LLMPhraseRelationshipResults,
    initial_grounding_flat: PhraseToTagAndRulesMap,
    phrase_trails: list[PhraseTrail],
    search_rounds: dict[int, LLMSearchResults],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],
    repairs_flat: Optional[dict[str, dict[str, str]]] = None,
    subject_name: Optional[str] = None,
) -> list[dict[str, object]]:
    trails_by_phrase = {trail.phrase: trail for trail in phrase_trails}
    phrases = sorted(
        set(screening_flat)
        | set(initial_grounding_flat)
        | set(trails_by_phrase)
    )

    rows: list[dict[str, object]] = []
    for phrase in phrases:
        verdict = screening_flat.get(phrase)
        screening_dump = verdict.model_dump() if verdict is not None else None
        relationship = relationship_flat.get(phrase)

        if verdict is not None and not verdict.passed:
            if phrase in initial_grounding_flat or phrase in trails_by_phrase:
                logger.error(
                    f"screened-out phrase {phrase!r} still reached grounding — "
                    f"the screening filter upstream is leaking"
                )
            rows.append(
                _finish_row(
                    phrase=phrase,
                    status="screened_out",
                    screening_dump=screening_dump,
                    relationship=relationship,
                    levels={},
                    search_rounds=search_rounds,
                    repairs_flat=repairs_flat or {},
                    subject_name=subject_name,
                )
            )
            continue

        levels: dict[int, list[dict[str, object]]] = {}

        trail = trails_by_phrase.get(phrase)
        if trail is not None:
            for lvl, itps in trail.lvl_by_lvl_itps.items():
                for itp in itps:
                    node = itp.model_dump(mode="json")
                    node["origin"] = _origin(
                        itp.direct_og_tag_w_rules, itp.iterative_og_tag_w_rules
                    )
                    node["source"] = _SOURCE_TRAIL
                    node["in_vocab"] = (
                        itp.group_id in match_label_to_concept_map
                    )
                    levels.setdefault(lvl, []).append(node)

        # Initial-grounding tags the trail cannot carry, each at its level:
        # positionless tags (sentinels and out-of-vocab proposals) at level 0.
        trail_real_group_ids = {
            str(node["group_id"])
            for nodes in levels.values()
            for node in nodes
            if node["stop_reason"] is None
        }
        for tag, applied_rules in initial_grounding_flat.get(phrase, {}).items():
            concept = match_label_to_concept_map.get(tag)
            if concept is not None:
                if concept.name in trail_real_group_ids:
                    # The concept entered iterative tagging; the trail node
                    # above already carries this tag via the direct copy.
                    continue
                logger.warning(
                    f"phrase {phrase!r}: in-vocab tag {tag!r} has no real "
                    f"trail node for concept {concept.name!r} — every "
                    f"recognized tag should enter iterative tagging now"
                )
                levels.setdefault(concept.level, []).append(
                    {
                        "parent_group_id": None,
                        "group_id": concept.name,
                        "stop_reason": None,
                        "direct_og_tag_w_rules": {
                            tag: [rule.model_dump(mode="json") for rule in applied_rules]
                        },
                        "iterative_og_tag_w_rules": {},
                        "origin": "initial_grounding",
                        "source": _SOURCE_UNWALKED,
                        "in_vocab": True,
                    }
                )
                continue
            source = (
                _SOURCE_SENTINEL
                if is_sentinel_grounding_label(tag)
                else _SOURCE_OOV
            )
            levels.setdefault(0, []).append(
                {
                    "parent_group_id": None,
                    "group_id": tag,
                    "stop_reason": None,
                    "direct_og_tag_w_rules": {
                        tag: [rule.model_dump(mode="json") for rule in applied_rules]
                    },
                    "iterative_og_tag_w_rules": {},
                    "origin": "initial_grounding",
                    "source": source,
                    "in_vocab": False,
                }
            )

        all_nodes = [node for nodes in levels.values() for node in nodes]
        real_nodes = [node for node in all_nodes if _node_is_real(node)]
        if not all_nodes:
            status = "grounding_dropped"
            logger.warning(
                f"phrase {phrase!r} passed screening but no grounding response "
                f"carries it"
            )
        elif not real_nodes:
            status = "no_match"
        else:
            status = "grounded"

        rows.append(
            _finish_row(
                phrase=phrase,
                status=status,
                screening_dump=screening_dump,
                relationship=relationship,
                levels=levels,
                search_rounds=search_rounds,
                repairs_flat=repairs_flat or {},
                subject_name=subject_name,
            )
        )

    return rows


def build_keyword_phrase_rows(
    *,
    screening_flat: LiveScreeningResults,
    relationship_flat: LLMPhraseRelationshipResults,
    freehand_grounding_flat: PhraseToTagAndRulesMap,
    search_rounds: dict[int, LLMSearchResults],
    repairs_flat: Optional[dict[str, dict[str, str]]] = None,
    subject_name: Optional[str] = None,
) -> list[dict[str, object]]:
    """Keyword-path rows. No ontology, so no levels: the grounded categories sit
    in ``freehand_grounding`` and status distinguishes real categories from the
    sentinel escape hatch."""
    phrases = sorted(set(screening_flat) | set(freehand_grounding_flat))

    rows: list[dict[str, object]] = []
    for phrase in phrases:
        verdict = screening_flat.get(phrase)
        screening_dump = verdict.model_dump() if verdict is not None else None
        relationship = relationship_flat.get(phrase)

        if verdict is not None and not verdict.passed:
            if phrase in freehand_grounding_flat:
                logger.error(
                    f"screened-out phrase {phrase!r} still reached freehand "
                    f"grounding — the screening filter upstream is leaking"
                )
            row = _finish_row(
                phrase=phrase,
                status="screened_out",
                screening_dump=screening_dump,
                relationship=relationship,
                levels={},
                search_rounds=search_rounds,
                repairs_flat=repairs_flat or {},
                subject_name=subject_name,
            )
            rows.append(row)
            continue

        groundings = freehand_grounding_flat.get(phrase, {})
        real_tags = {
            tag for tag in groundings if not is_sentinel_grounding_label(tag)
        }
        if not groundings:
            status = "grounding_dropped"
            logger.warning(
                f"phrase {phrase!r} passed screening but no freehand grounding "
                f"response carries it"
            )
        elif not real_tags:
            status = "no_match"
        else:
            status = "grounded"

        search_round, provenance = _provenance(phrase, search_rounds)
        row = {
            "phrase": phrase,
            "status": status,
            "search_round": search_round,
            "provenance": provenance,
            "relationship": relationship,
            "screening": screening_dump,
            "freehand_grounding": {
                tag: [rule.model_dump(mode="json") for rule in applied_rules]
                for tag, applied_rules in groundings.items()
            },
        }
        # This branch builds its row inline rather than through _finish_row, so
        # the repair field has to be repeated here. Same rule: present only when
        # the model answered under a different string.
        if repairs_flat and phrase in repairs_flat:
            row["phrase_as_answered"] = repairs_flat[phrase]
        if subject_name and relationship:
            own_name_hits = count_own_name_hits(relationship, subject_name)
            if own_name_hits:
                row["relationship_own_name_hits"] = own_name_hits
        if screening_dump is None:
            row["note"] = "phrase_not_in_screening"
            logger.warning(
                f"phrase {phrase!r} appears in freehand grounding output but was "
                f"never screened"
            )
        rows.append(row)

    return rows


def build_partial_phrase_rows(
    *,
    search_rounds: dict[int, LLMSearchResults],
    relationship_flat: Optional[LLMPhraseRelationshipResults],
    screening_flat: Optional[LiveScreeningResults],
    grounding_flat: Optional[PhraseToTagAndRulesMap],
    grounding_stage: Optional[str] = None,
    repairs_flat: Optional[dict[str, dict[str, str]]] = None,
    subject_name: Optional[str] = None,
) -> list[dict[str, object]]:
    """Rows for a run that stopped before reconcile.

    Separate from the two full builders rather than a flag on them, because
    ``status`` cannot be derived here: ``grounded`` / ``no_match`` /
    ``screened_out`` are statements about a chain that finished, and a stopped
    run has no business claiming any of them. Rows carry the stage fields and
    let the reader draw the conclusion.

    A stage that ran but had nothing for a phrase emits an explicit ``null``; a
    stage that never ran has its key OMITTED. The distinction is the whole point
    of a partial dump — "screening rejected it" and "screening never happened"
    must not read the same.

    Driven by the union of every stage that ran, search rounds included, so a
    run stopped after search still dumps its phrases instead of an empty file.
    """
    phrases: set[str] = set()
    for round_phrases in search_rounds.values():
        phrases |= set(round_phrases)
    for stage_flat in (relationship_flat, screening_flat, grounding_flat):
        if stage_flat is not None:
            phrases |= set(stage_flat)

    repairs_flat = repairs_flat or {}
    rows: list[dict[str, object]] = []
    for phrase in sorted(phrases):
        search_round, provenance = _provenance(phrase, search_rounds)
        row: dict[str, object] = {
            "phrase": phrase,
            "search_round": search_round,
            "provenance": provenance,
        }
        if relationship_flat is not None:
            relationship = relationship_flat.get(phrase)
            row["relationship"] = relationship
            if subject_name and relationship:
                own_name_hits = count_own_name_hits(relationship, subject_name)
                if own_name_hits:
                    row["relationship_own_name_hits"] = own_name_hits
        if screening_flat is not None:
            verdict = screening_flat.get(phrase)
            row["screening"] = verdict.model_dump() if verdict is not None else None
        if grounding_flat is not None:
            groundings = grounding_flat.get(phrase)
            row[grounding_stage or "grounding"] = (
                {
                    tag: [rule.model_dump(mode="json") for rule in applied_rules]
                    for tag, applied_rules in groundings.items()
                }
                if groundings is not None
                else None
            )
        if phrase in repairs_flat:
            row["phrase_as_answered"] = repairs_flat[phrase]
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
