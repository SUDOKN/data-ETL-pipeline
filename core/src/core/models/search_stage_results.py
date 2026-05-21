from datetime import datetime
from pydantic import BaseModel

from core.models.field_types import (
    S3FileVersionIDType,
)


class SearchStageMetadata(BaseModel):
    model: str
    created_at: datetime
    search_prompt_version_id: S3FileVersionIDType


class SearchStageStats(BaseModel):
    results: set[str]


SearchStageStatsMap = dict[
    str, SearchStageStats
]  # "0:1000" -> {results: ('term1', 'term2', ...)}


class SearchStageResults(BaseModel):
    metadata: SearchStageMetadata
    results: set[str]
    chunk_stats: SearchStageStatsMap  # chunk map
