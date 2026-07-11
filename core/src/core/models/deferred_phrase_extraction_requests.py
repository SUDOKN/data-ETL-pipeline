from pydantic import BaseModel
from typing import Optional

from core.models.llm_phrase_extraction_results import (
    LLMPhraseExtractionMetadata,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID


class LLMPhraseExtractionRequestBundle(BaseModel):
    llm_phrase_search_req_id: Optional[GPTBatchRequestCustomID]
    llm_phrase_relationship_req_id: Optional[GPTBatchRequestCustomID]
    llm_phrase_relationship_screening_req_id: Optional[GPTBatchRequestCustomID]


LLMPhraseExtractionRequestMap = dict[str, LLMPhraseExtractionRequestBundle]


class DeferredLLMPhraseExtractionRequests(BaseModel):
    metadata: LLMPhraseExtractionMetadata
    chunked_request_map: LLMPhraseExtractionRequestMap
