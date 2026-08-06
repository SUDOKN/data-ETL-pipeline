from pydantic import BaseModel, ConfigDict
from typing import Optional
from core.models.extraction_results.single_stage_extraction_results import (
    SingleStageExtractionResults,
    SingleStageStats,
)


class BusinessDescription(BaseModel):
    name: Optional[str]
    description: Optional[str]


class BusinessDescResponse(BusinessDescription):
    model_config = ConfigDict(extra="forbid")


class BusinessDescriptionExtractionStats(SingleStageStats[BusinessDescription]):
    result: BusinessDescription


BusinessDescriptionExtractionStatsMap = dict[str, BusinessDescriptionExtractionStats]
# "0:1000" -> {results: [{name, description}, ...]}
# although we expect only 1 result per chunk,
# we keep it as a list for consistency with other extraction types


class BusinessDescriptionExtractionResult(
    SingleStageExtractionResults[BusinessDescription, BusinessDescription]
):
    result: BusinessDescription
    chunk_stats: BusinessDescriptionExtractionStatsMap
