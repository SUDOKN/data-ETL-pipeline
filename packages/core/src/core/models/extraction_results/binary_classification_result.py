from typing import Optional

from pydantic import BaseModel, ConfigDict

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_results.single_stage_extraction_results import (
    SingleStageExtractionResults,
    SingleStageStats,
)


class BaseClassificationDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")
    answer: bool
    reason: str


class LLMBinaryClassification(BaseClassificationDecision):
    """The stored decision. Since the catalog conversion, ``answer`` is DERIVED
    from ``applied_rules`` at parse time (``passed_implied_by``) and ``reason`` is
    synthesized from the reported rules — the model reports neither. Both stay on
    the stored shape because every downstream reader (ground-truth surveys, the
    keyword API's rejection message) wants the conclusion, not the derivation.

    ``identified_entity`` and ``applied_rules`` default empty so documents
    written before the conversion still load.
    """

    confidence: int
    identified_entity: Optional[str] = None
    applied_rules: list[AppliedRule] = []


class BinaryClassificationStats(SingleStageStats[LLMBinaryClassification]):
    pass


BinaryClassificationStatsMap = dict[
    str, BinaryClassificationStats
]  # "0:1000" -> {answer, confidence, reason}


class BinaryClassificationResult(
    SingleStageExtractionResults[LLMBinaryClassification, LLMBinaryClassification]
):
    chunk_stats: BinaryClassificationStatsMap
