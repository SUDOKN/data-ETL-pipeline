from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, TypeVar

from core.models.extraction_schemas.grounding import (
    TagToAppliedRulesMap,
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
from core.models.field_types import ExtractionFieldType

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


def _resolve_phrase_value(
    partitioned: dict[int, dict[str, _T]],
    phrase: str,
) -> Optional[_T]:
    """Return *phrase*'s value from any round bucket, or None if absent.

    The search / relationship / screening / grounding phrase sets are produced
    by separate LLM calls and can diverge, so a phrase may not appear in every
    stage at the round computed from search results. Scan all rounds instead of
    indexing by a presumed round so a drifted phrase yields None rather than a
    KeyError that crashes the diagnostic dump.
    """
    for round_map in partitioned.values():
        if phrase in round_map:
            return round_map[phrase]
    return None


def _safe_path_segment(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in value
    )


def build_keyword_phrase_trail_entry(
    *,
    phrase: str,
    search_round: int,
    relationship_result: dict[int, LLMPhraseRelationshipResults],
    screening_result: dict[int, LiveScreeningResults],
    phrase_groundings: TagToAppliedRulesMap,
) -> dict[str, object]:
    screening = _resolve_phrase_value(screening_result, phrase)
    return {
        "phrase": phrase,
        "search_round": search_round,
        "relationship": _resolve_phrase_value(relationship_result, phrase),
        "screening": screening.model_dump() if screening is not None else None,
        # Dumped here rather than at write time because the dump is handed to
        # json.dumps, which cannot serialize the AppliedRule models.
        "freehand_grounding": {
            tag: [rule.model_dump(mode="json") for rule in applied_rules]
            for tag, applied_rules in phrase_groundings.items()
        },
    }


def build_concept_phrase_trail_entry(
    *,
    phrase_trail: PhraseTrail,
    search_round: int,
    relationship_result: dict[int, LLMPhraseRelationshipResults],
    screening_result: dict[int, LiveScreeningResults],
) -> dict[str, object]:
    phrase_trail_dump = phrase_trail.model_dump(mode="json")
    screening = _resolve_phrase_value(screening_result, phrase_trail.phrase)
    return {
        "phrase": phrase_trail.phrase,
        "search_round": search_round,
        "relationship": _resolve_phrase_value(relationship_result, phrase_trail.phrase),
        "screening": screening.model_dump() if screening is not None else None,
        "lvl_by_lvl_itps": phrase_trail_dump["lvl_by_lvl_itps"],
    }


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
