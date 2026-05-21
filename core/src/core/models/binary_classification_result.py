from pydantic import BaseModel

from core.models.single_stage_extraction_results import (
    SingleStageExtractionResults,
    SingleStageStats,
)


class BinaryClassification(BaseModel):
    answer: bool
    confidence: int
    reason: str


class BinaryClassificationStats(SingleStageStats[BinaryClassification]):
    result: BinaryClassification


BinaryClassificationStatsMap = dict[
    str, BinaryClassificationStats
]  # "0:1000" -> {answer, confidence, reason}


class BinaryClassificationResult(
    SingleStageExtractionResults[BinaryClassification, BinaryClassification]
):
    result: BinaryClassification
    chunk_stats: BinaryClassificationStatsMap
