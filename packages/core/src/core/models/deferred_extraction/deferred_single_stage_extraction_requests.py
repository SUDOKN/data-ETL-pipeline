from pydantic import BaseModel
from typing import Optional

from core.models.extraction_results.single_stage_extraction_results import (
    LLMSingleStageExtractionMetadata,
)
from llm_providers.field_types import BatchRequestIDType


class SingleStageExtractionRequestBundle(BaseModel):
    llm_request_id: Optional[BatchRequestIDType]


SingleStageExtractionRequestMap = dict[str, SingleStageExtractionRequestBundle]


class DeferredSingleStageExtractionRequests(BaseModel):
    metadata: LLMSingleStageExtractionMetadata
    chunked_request_map: SingleStageExtractionRequestMap
