from pydantic import BaseModel

from core.models.extraction_results.single_stage_extraction_results import (
    SingleStageExtractionResults,
    SingleStageStats,
)


class BaseClassificationDecision(BaseModel):
    answer: bool
    reason: str


class LLMBinaryClassification(BaseClassificationDecision):
    confidence: int


class BinaryClassificationStats(SingleStageStats[LLMBinaryClassification]):
    pass


BinaryClassificationStatsMap = dict[
    str, BinaryClassificationStats
]  # "0:1000" -> {answer, confidence, reason}


class BinaryClassificationResult(
    SingleStageExtractionResults[LLMBinaryClassification, LLMBinaryClassification]
):
    chunk_stats: BinaryClassificationStatsMap
