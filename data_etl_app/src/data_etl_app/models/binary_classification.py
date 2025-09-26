from datetime import datetime
from pydantic import BaseModel

from shared.models.field_types import (
    S3FileVersionIDType,
)


class ChunkBinaryClassificationResult(BaseModel):
    answer: bool
    confidence: int
    reason: str


class BinaryClassificationStats(BaseModel):
    prompt_version_id: S3FileVersionIDType
    final_chunk_key: str
    chunk_result_map: dict[str, ChunkBinaryClassificationResult]


class BinaryClassificationResult(BaseModel):
    evaluated_at: datetime
    answer: bool  # from final chunk
    confidence: int  # from final chunk
    reason: str  # from final chunk
    stats: BinaryClassificationStats
