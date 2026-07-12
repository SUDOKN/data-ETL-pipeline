from core.models.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionRequestBundle,
)
from core.models.keyword_extraction_results import (
    KeywordExtractionMetadata,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from typing import Optional


class KeywordExtractionRequestBundle(LLMPhraseExtractionRequestBundle):
    llm_phrase_freehand_grounding_req_id: Optional[GPTBatchRequestCustomID]


KeywordExtractionRequestMap = dict[str, KeywordExtractionRequestBundle]


class DeferredKeywordExtractionRequests(DeferredLLMPhraseExtractionRequests):
    metadata: KeywordExtractionMetadata
    chunked_request_map: KeywordExtractionRequestMap
