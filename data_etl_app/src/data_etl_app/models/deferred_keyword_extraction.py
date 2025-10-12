from pydantic import BaseModel

from core.models.field_types import (
    S3FileVersionIDType,
)
from open_ai_key_app.models.gpt_batch_request import GPTBatchRequest


class DeferredKeywordExtractionStats(BaseModel):
    extract_prompt_version_id: S3FileVersionIDType
    chunk_batch_request_map: dict[str, GPTBatchRequest]


class DeferredKeywordExtraction(BaseModel):
    deferred_stats: DeferredKeywordExtractionStats
