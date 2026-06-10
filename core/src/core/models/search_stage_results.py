from pydantic import BaseModel

from core.models.base_extraction import BaseStageMetadata, BaseExtractionStats
from core.models.field_types import (
    LLMSearchResults,
    S3FileVersionIDType,
)


class SearchStageMetadata(BaseStageMetadata):
    search_prompt_version_id: S3FileVersionIDType


class SearchStageExtractionStats(BaseExtractionStats):
    llm_search: LLMSearchResults  # identified by LLM


SearchStageStatsMap = dict[
    str, SearchStageExtractionStats
]  # "0:1000" -> {results: ('term1', 'term2', ...)}


class SearchStageResults(BaseModel):
    metadata: SearchStageMetadata
    results: set[str]
    chunk_stats: SearchStageStatsMap  # chunk map
