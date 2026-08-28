from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionRequestBundle,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    KeywordExtractionMetadata,
)
from llm_providers.field_types import BatchRequestIDType
from pydantic import Field
from typing import Optional


class KeywordExtractionRequestBundle(LLMPhraseExtractionRequestBundle):
    # Ordered list of freehand-grounding groups (group 1 == index 0). Each group
    # covers at most `max_pairs_per_request` screened phrases for this chunk;
    # results are merged back together.
    llm_phrase_freehand_grounding_req_ids: list[BatchRequestIDType] = Field(
        default_factory=list
    )
    # Freehand-grounding under-answer retry (3.3, the synthesis stage's policy
    # ported by user decision 2026-08-24): None = not yet assessed; a list
    # (possibly empty) = assessed, these record ids came back unanswered and
    # the retry request set below re-asks exactly them, once.
    llm_phrase_freehand_grounding_retry_record_ids: Optional[list[str]] = None
    llm_phrase_freehand_grounding_retry_req_ids: list[BatchRequestIDType] = Field(
        default_factory=list
    )


KeywordExtractionRequestMap = dict[str, KeywordExtractionRequestBundle]


class DeferredKeywordExtractionRequests(DeferredLLMPhraseExtractionRequests):
    metadata: KeywordExtractionMetadata
    chunked_request_map: KeywordExtractionRequestMap
