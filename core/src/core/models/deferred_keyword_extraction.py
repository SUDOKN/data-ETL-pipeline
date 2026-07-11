from core.models.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionRequestBundle,
)
from core.models.keyword_extraction_results import (
    KeywordExtractionMetadata,
)


class KeywordExtractionRequestBundle(LLMPhraseExtractionRequestBundle):
    pass


KeywordExtractionRequestMap = dict[str, KeywordExtractionRequestBundle]


class DeferredKeywordExtractionRequests(DeferredLLMPhraseExtractionRequests):
    metadata: KeywordExtractionMetadata
    request_map: KeywordExtractionRequestMap
