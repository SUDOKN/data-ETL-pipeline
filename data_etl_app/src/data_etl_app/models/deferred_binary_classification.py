from pydantic import BaseModel

from core.models.field_types import (
    S3FileVersionIDType,
)
from open_ai_key_app.models.field_types import GPTBatchRequestMongoID


class DeferredBinaryClassificationStats(BaseModel):
    prompt_version_id: S3FileVersionIDType
    final_chunk_key: str
    chunk_batch_request_id_map: dict[str, GPTBatchRequestMongoID]


class DeferredBinaryClassification(BaseModel):
    deferred_stats: DeferredBinaryClassificationStats
