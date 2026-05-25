from datetime import datetime
from pydantic import BaseModel

from core.models.field_types import (
    S3FileVersionIDType,
)
from data_etl_app.models.chunking_strat import ChunkingStrategy
from open_ai_key_app.models.gpt_model_params import GPTModelParams


class SearchStageMetadata(BaseModel):
    model: str
    model_params: GPTModelParams
    created_at: datetime
    chunk_strat: ChunkingStrategy
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
