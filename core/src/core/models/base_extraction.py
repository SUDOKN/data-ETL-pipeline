from datetime import datetime
from pydantic import BaseModel

from data_etl_app.models.chunking_strat import ChunkingStrategy
from open_ai_key_app.models.gpt_model_params import GPTModelParams


class BaseStageMetadata(BaseModel):
    model: str
    model_params: GPTModelParams
    created_at: datetime
    chunk_strat: ChunkingStrategy


class BaseExtractionStats(BaseModel):
    results: set[str]


class BaseExtractionResults(BaseModel):
    metadata: BaseStageMetadata
    results: set[str]
    chunk_stats: dict[str, BaseExtractionStats]  # chunk bounds -> stats
