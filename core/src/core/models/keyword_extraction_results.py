from typing import Optional

from core.models.field_types import (
    LLMDistillationResults,
    S3FileVersionIDType,
)
from core.models.search_stage_results import (
    SearchStageMetadata,
    SearchStageResults,
    SearchStageExtractionStats,
)


class KeywordExtractionMetadata(SearchStageMetadata):
    distillation_prompt_version_id: Optional[S3FileVersionIDType]


class KeywordExtractionStats(SearchStageExtractionStats):
    llm_distillation: LLMDistillationResults


KeywordExtractionStatsMap = dict[str, KeywordExtractionStats]
"""
{
    "0:1000" : {
        results: ('keyword1', 'keyword2', ...), 
        llm_search: ('keyword1', 'keyword2', ...)
    },
    "750:1500": {
        ...
    }
    
}
"""


class KeywordExtractionResults(SearchStageResults):
    metadata: KeywordExtractionMetadata
    chunk_stats: KeywordExtractionStatsMap  # chunk map
