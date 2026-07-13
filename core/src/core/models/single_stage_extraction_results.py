from __future__ import annotations

from pydantic import BaseModel
from typing import Generic, TypeVar

from core.models.llm_phrase_extraction_results import (
    BaseExtractionMetadata,
    ExtractionNodeMetadata,
)

ChunkExtractionResultT = TypeVar("ChunkExtractionResultT")
SingleStageExtractionResultT = TypeVar("SingleStageExtractionResultT")


class SingleStageStats(BaseModel, Generic[ChunkExtractionResultT]):
    result: ChunkExtractionResultT


class LLMSingleStageExtractionMetadata(BaseExtractionMetadata):
    single_stage: ExtractionNodeMetadata


class SingleStageExtractionResults(
    BaseModel, Generic[SingleStageExtractionResultT, ChunkExtractionResultT]
):
    metadata: LLMSingleStageExtractionMetadata
    result: SingleStageExtractionResultT  # compiled from chunk-level results
    chunk_stats: dict[str, SingleStageStats[ChunkExtractionResultT]]  # chunk map
