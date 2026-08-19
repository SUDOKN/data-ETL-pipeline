from pydantic import BaseModel, Field

from core.models.extraction_results.llm_phrase_extraction_results import (
    LLMPhraseExtractionMetadata,
)
from llm_providers.field_types import BatchRequestIDType


class LLMPhraseExtractionRequestBundle(BaseModel):
    # Absolute "start:end" character bounds of the search sub-windows tiling this
    # chunk, in document order — the chunk's geometry under
    # ChunkingStrategy.search_divisor, computed once at prefill (where the text
    # is) and read by the search stages (which embed ids without it). One entry
    # equal to the chunk's own bounds when search_divisor == 1.
    search_sub_bounds: list[str] = Field(default_factory=list)
    # One first-search request per sub-window, index-aligned with
    # search_sub_bounds.
    llm_phrase_search_req_ids: list[BatchRequestIDType] = Field(default_factory=list)
    # Per sub-window (keyed by its search_sub_bounds entry): the ordered list of
    # recursive search rounds (round 1 == index 0). Each round re-searches the
    # sub-window while excluding the compounding union of phrases found there by
    # the first search and all prior recursive rounds. Sub-windows converge
    # independently, so the lists are ragged.
    llm_phrase_recursive_search_req_ids: dict[str, list[BatchRequestIDType]] = Field(
        default_factory=dict
    )
    # Ordered list of relationship groups (group 1 == index 0). Each group covers
    # at most `max_phrases_per_request` of the chunk's candidate phrases; every
    # group sees the same full chunk text, and results are merged back together.
    llm_phrase_relationship_req_ids: list[BatchRequestIDType] = Field(
        default_factory=list
    )
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
