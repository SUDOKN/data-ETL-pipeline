from packages.core.src.core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionRequestBundle,
)
from packages.core.src.core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionMetadata,
)
from packages.llm_providers.src.llm_providers.field_types import BatchRequestIDType
from typing import Optional


class KeywordExtractionRequestBundle(LLMPhraseExtractionRequestBundle):
    llm_phrase_freehand_grounding_req_id: Optional[BatchRequestIDType]


KeywordExtractionRequestMap = dict[str, KeywordExtractionRequestBundle]


class DeferredKeywordExtractionRequests(DeferredLLMPhraseExtractionRequests):
    metadata: KeywordExtractionMetadata
    chunked_request_map: KeywordExtractionRequestMap
