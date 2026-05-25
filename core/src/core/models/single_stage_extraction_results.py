from datetime import datetime
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from pydantic import BaseModel
from typing import Generic, TypeVar

ChunkExtractionResultT = TypeVar("ChunkExtractionResultT")
SingleStageExtractionResultT = TypeVar("SingleStageExtractionResultT")

from core.models.field_types import (
    S3FileVersionIDType,
)
from data_etl_app.models.chunking_strat import ChunkingStrategy


class SingleStageMetadata(BaseModel):
    model: str
    model_params: GPTModelParams
    created_at: datetime
    chunk_strat: ChunkingStrategy
    prompt_version_id: S3FileVersionIDType


class SingleStageStats(BaseModel, Generic[ChunkExtractionResultT]):
    result: ChunkExtractionResultT


SingleStageStatsMap = dict[
    str, SingleStageStats[ChunkExtractionResultT]
]  # "0:1000" -> {results: ('term1', 'term2', ...)}


class SingleStageExtractionResults(
    BaseModel, Generic[SingleStageExtractionResultT, ChunkExtractionResultT]
):
    metadata: SingleStageMetadata
    result: SingleStageExtractionResultT  # compiled from chunk-level results
    chunk_stats: SingleStageStatsMap  # chunk map
