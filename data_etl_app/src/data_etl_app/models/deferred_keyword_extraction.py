from pydantic import BaseModel

from core.models.field_types import (
    S3FileVersionIDType,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID


class DeferredKeywordExtractionStats(BaseModel):
    extract_prompt_version_id: S3FileVersionIDType
    chunk_batch_request_id_map: dict[str, GPTBatchRequestCustomID]


class DeferredKeywordExtraction(BaseModel):
    deferred_stats: DeferredKeywordExtractionStats
