from pydantic import BaseModel, Field
from typing import Optional

from core.models.extraction_results.llm_phrase_extraction_results import (
    LLMPhraseExtractionMetadata,
)
from llm_providers.field_types import BatchRequestIDType


class LLMPhraseExtractionRequestBundle(BaseModel):
    llm_phrase_search_req_id: Optional[BatchRequestIDType]
    # Ordered list of recursive search rounds (round 1 == index 0). Each round
    # re-searches the chunk while excluding the compounding union of phrases found
    # by the first search and all prior recursive rounds.
    llm_phrase_recursive_search_req_ids: list[BatchRequestIDType] = Field(
        default_factory=list
    )
    llm_phrase_relationship_req_id: Optional[BatchRequestIDType]
    # Ordered list of screening groups (group 1 == index 0). Each group covers at
    # most `max_pairs_per_request` phrase-relationship pairs from the upstream
    # relationship results for this chunk; results are merged back together.
    llm_phrase_relationship_screening_req_ids: list[BatchRequestIDType] = Field(
        default_factory=list
    )


LLMPhraseExtractionRequestMap = dict[str, LLMPhraseExtractionRequestBundle]


class DeferredLLMPhraseExtractionRequests(BaseModel):
    metadata: LLMPhraseExtractionMetadata
    chunked_request_map: LLMPhraseExtractionRequestMap
