from pydantic import BaseModel

from core.models.field_types import (
    S3FileVersionIDType,
)
from open_ai_key_app.models.gpt_batch_request import GPTBatchRequest


class DeferredBinaryClassificationStats(BaseModel):
    prompt_version_id: S3FileVersionIDType
    final_chunk_key: str
    chunk_batch_request_map: dict[str, GPTBatchRequest]


class DeferredBinaryClassification(BaseModel):
    deferred_stats: DeferredBinaryClassificationStats
