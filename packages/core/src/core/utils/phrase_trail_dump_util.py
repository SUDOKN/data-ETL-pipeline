"""Per-phrase diagnostic rows for the phrase-trail dumps.

One row per phrase the pipeline SCREENED (plus any phrase a grounding response
introduced that screening never saw — kept visible rather than dropped, since
that drift is itself a defect worth reading). The old dump emitted a row per
GROUNDED phrase, which made ``screening.passed`` a constant ``true`` and hid
exactly the phrases a reviewer most needs: the screened-out majority, the
exact-label settled concepts, the out-of-vocab proposals, and the initial
sentinel verdicts.

Row anatomy:

- ``status`` — the branch the phrase actually took. Deliberately redundant with
  the other fields (a second witness): ``_status_from_fields`` re-derives it
  from the row alone and a disagreement is logged as an error, so a future bug
  that makes the fields lie gets caught by the dump instead of shipping in it.
    * ``screened_out``   — failed relationship screening; no grounding fields.
    * ``grounded``       — at least one real tag survived: an iterative-tagging
                           node or an out-of-vocab proposal.
    * ``settled``        — its only real tags are exact-label concepts that
                           never entered iterative tagging (proven at their own
                           level, nowhere further to go).
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
  proposals) sit at level 0 with ``parent_group_id: null``; a settled concept
  sits at its own level; iterative-tagging nodes keep their real position.
  Each node carries:
    * ``origin`` — which stage produced its rules (``initial_grounding`` /
      ``recursive_grounding`` / ``both``), derived from which rule maps are
      non-empty.
    * ``source`` — how the node entered the dump (``iterative_tagging`` or one
      of the synthesized kinds). ``origin`` says who spoke; ``source`` says why
      the row exists — a settled node and a trail node can be field-identical
      without it, which would make ``status`` under-determined.
    * ``in_vocab`` — whether ``group_id`` resolves in the ontology.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from requests.structures import CaseInsensitiveDict

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

logger = logging.getLogger(__name__)

_SOURCE_TRAIL = "iterative_tagging"
_SOURCE_SETTLED = "initial_grounding_settled"
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
    if all(node["source"] == _SOURCE_SETTLED for node in real_nodes):
        return "settled"
    return "grounded"


def _finish_row(
    *,
    phrase: str,
    status: str,
    screening_dump: Optional[dict[str, object]],
    relationship: Optional[str],
    levels: dict[int, list[dict[str, object]]],
    search_rounds: dict[int, LLMSearchResults],
) -> dict[str, object]:
    rederived = _status_from_fields(screening_dump, levels)
    if rederived != status:
        logger.error(
            f"phrase trail row inconsistency for {phrase!r}: writer chose status "
            f"{status!r} but the row's fields read as {rederived!r}"
        )
    for nodes in levels.values():
        for node in nodes:
            # Rules from a descent imply a parent that descended: a rootless
            # node claiming recursive origin means the join above mislabeled it.
            if node["origin"] != "initial_grounding" and node["parent_group_id"] is None:
                logger.error(
                    f"phrase trail row inconsistency for {phrase!r}: node "
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
        # settled concepts at their own, positionless tags (sentinels and
        # out-of-vocab proposals) at level 0.
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
                        "source": _SOURCE_SETTLED,
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
        elif all(node["source"] == _SOURCE_SETTLED for node in real_nodes):
            status = "settled"
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
            )
        )

    return rows


def build_keyword_phrase_rows(
    *,
    screening_flat: LiveScreeningResults,
    relationship_flat: LLMPhraseRelationshipResults,
    freehand_grounding_flat: PhraseToTagAndRulesMap,
    search_rounds: dict[int, LLMSearchResults],
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
        if screening_dump is None:
            row["note"] = "phrase_not_in_screening"
            logger.warning(
                f"phrase {phrase!r} appears in freehand grounding output but was "
                f"never screened"
            )
        rows.append(row)

    return rows


def _safe_path_segment(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in value
    )


def write_phrase_trails_dump(
    *,
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    timestamp: datetime,
    chunked_phrase_trails: dict[str, list[dict[str, object]]],
) -> None:
    dump_root = Path(__file__).resolve().parents[4] / "logs" / "phrase_trails"
    dump_dir = dump_root / timestamp.strftime("%Y%m%dT%H%M%S")
    dump_dir.mkdir(parents=True, exist_ok=True)

    dump_path = dump_dir / (
        f"{_safe_path_segment(subject_unique_id)}__{_safe_path_segment(field_type.name)}.json"
    )
    dump_payload = {
        "subject_unique_id": subject_unique_id,
        "field_type": field_type.name,
        "timestamp": timestamp.isoformat(),
        "chunked_phrase_trails": chunked_phrase_trails,
    }
    dump_path.write_text(json.dumps(dump_payload, indent=2), encoding="utf-8")
    logger.info("Saved phrase trail dump to %s", dump_path)
