from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from core.models.extraction_schemas.iterative_tagging import PhraseTrail
from core.models.extraction_schemas.relationship import LLMPhraseRelationshipResults
from core.models.extraction_schemas.screening import LiveScreeningResults
from core.models.types_and_enums import LLMExtractedFieldTypeEnum

logger = logging.getLogger(__name__)


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
    phrase_groundings: dict[str, str],
) -> dict[str, object]:
    return {
        "phrase": phrase,
        "search_round": search_round,
        "relationship": relationship_result[search_round][phrase],
        "screening": screening_result[search_round][phrase].model_dump(),
        "freehand_grounding": phrase_groundings,
    }


def build_concept_phrase_trail_entry(
    *,
    phrase_trail: PhraseTrail,
    search_round: int,
    relationship_result: dict[int, LLMPhraseRelationshipResults],
    screening_result: dict[int, LiveScreeningResults],
) -> dict[str, object]:
    phrase_trail_dump = phrase_trail.model_dump(mode="json")
    return {
        "phrase": phrase_trail.phrase,
        "search_round": search_round,
        "relationship": relationship_result[search_round][phrase_trail.phrase],
        "screening": screening_result[search_round][phrase_trail.phrase].model_dump(),
        "lvl_by_lvl_itps": phrase_trail_dump["lvl_by_lvl_itps"],
    }


def write_phrase_trails_dump(
    *,
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    timestamp: datetime,
    chunked_phrase_trails: dict[str, list[dict[str, object]]],
) -> None:
    dump_root = Path(__file__).resolve().parents[4] / "logs" / "phrase_trails"
    dump_dir = dump_root / timestamp.strftime("%Y%m%dT%H%M%S")
    dump_dir.mkdir(parents=True, exist_ok=True)

    dump_path = dump_dir / (
        f"{_safe_path_segment(mfg_etld1)}__{_safe_path_segment(field_type.name)}.json"
    )
    dump_payload = {
        "mfg_etld1": mfg_etld1,
        "field_type": field_type.name,
        "timestamp": timestamp.isoformat(),
        "chunked_phrase_trails": chunked_phrase_trails,
    }
    dump_path.write_text(json.dumps(dump_payload, indent=2), encoding="utf-8")
    logger.info("Saved phrase trail dump to %s", dump_path)
