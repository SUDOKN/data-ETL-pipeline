from pydantic import BaseModel
from typing import Optional

from core.models.single_stage_extraction_results import SingleStageMetadata
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID


class SingleStageExtractionRequestBundle(BaseModel):
    llm_request_id: Optional[GPTBatchRequestCustomID]


SingleStageExtractionRequestMap = dict[str, SingleStageExtractionRequestBundle]


class DeferredSingleStageExtractionRequests(BaseModel):
    metadata: SingleStageMetadata
    request_map: SingleStageExtractionRequestMap
