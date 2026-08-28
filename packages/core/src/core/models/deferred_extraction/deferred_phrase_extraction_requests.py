from typing import Optional

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
    # v3 mention collection (PIPELINE_V3_PLAN.md D4–D7, as amended 2026-08-22).
    # Per sub-window (keyed by its search_sub_bounds entry): the ordered list of
    # mention-location request groups (group 1 == index 0). Mentions are
    # collected in CODE from the window text and the window's sent forms; each
    # group asks the LLM for the location of at most `max_mentions_per_request`
    # of the window's distinct snippets, against the same window text; the fold
    # merges the groups back. A window with no mentions gets one dummy request.
    llm_phrase_mention_req_ids: dict[str, list[BatchRequestIDType]] = Field(
        default_factory=dict
    )
    # Per sub-window: the forms the mention stage was handed for it — the CHUNK's
    # search ∪ brute casings, filtered to the forms that OCCUR in the sub-window
    # (user decision 2026-08-22), sorted — written when the group ids are embedded.
    # Stored so the fold can re-collect from the text + these forms alone — the
    # request carries snippets, not forms (user decision), and the fold has no
    # search map.
    llm_phrase_mention_sent_forms: dict[str, list[str]] = Field(default_factory=dict)
    # Location-stage under-answer policy (user decision 2026-08-22): once a
    # sub-window's group requests are complete it is ASSESSED — the mention ids
    # the model left undescribed are stored here (an empty list = assessed, none
    # missing; a key absent = not yet assessed) and, when any are missing, ONE
    # retry pass asks for those items again (split by `max_mentions_per_request`
    # into the request ids below, parallel to the stored ids' grouping). The fold
    # reads the retry answers after the groups'; nothing is retried twice.
    llm_phrase_mention_retry_mention_ids: dict[str, list[str]] = Field(default_factory=dict)
    llm_phrase_mention_retry_req_ids: dict[str, list[BatchRequestIDType]] = Field(
        default_factory=dict
    )
    # v3 synthesis (PIPELINE_V3_PLAN.md D15 as amended 2026-08-22, D16; Phase
    # 3.2). Per CHUNK (this bundle): the ordered list of synthesis request
    # groups (group 1 == index 0). The chunk's fold records — one per non-empty
    # group, focal form + entries — are packed in bundle order into requests of
    # at most `max_entries_per_request` entries, a record never split; a chunk
    # with no records gets one dummy request. Records are recomputed from the
    # text + the stored mention state (the fold is deterministic), so nothing
    # but the ids is stored.
    llm_phrase_synthesis_req_ids: list[BatchRequestIDType] = Field(default_factory=list)
    # Under-answer policy (user decision 2026-08-22, same family as the Location
    # stage's): once the chunk's group requests are complete it is ASSESSED —
    # the record ids no answer synthesized are stored here (an empty list =
    # assessed, none missing; None = not yet assessed) and, when any are
    # missing, ONE retry pass re-asks for just those records (packed the same
    # way into the ids below). The result reads group answers first, then the
    # retry's; what is still missing after that is reported, never retried twice.
    llm_phrase_synthesis_retry_record_ids: Optional[list[str]] = None
    llm_phrase_synthesis_retry_req_ids: list[BatchRequestIDType] = Field(default_factory=list)
    # v2 relationship (out of every chain since v3 3.1; retired at 3.3).
    # Ordered list of relationship groups (group 1 == index 0). Each group covers
    # at most `max_phrases_per_request` of the chunk's candidate phrases; every
    # group sees the same full chunk text, and results are merged back together.
    llm_phrase_relationship_req_ids: list[BatchRequestIDType] = Field(
        default_factory=list
    )
    # Ordered list of screening groups (group 1 == index 0). Each group covers at
    # most `max_pairs_per_request` phrase-relationship pairs from the upstream
    # relationship results for this chunk; results are merged back together.
    # Screening under-answer retry (3.3, the synthesis stage's policy ported by
    # user decision 2026-08-24): None = not yet assessed; a list (possibly
    # empty) = assessed, these record ids came back unanswered and the retry
    # request set below re-asks exactly them, once.
    llm_phrase_relationship_screening_retry_record_ids: Optional[list[str]] = None
    llm_phrase_relationship_screening_retry_req_ids: list[BatchRequestIDType] = Field(
        default_factory=list
    )
    llm_phrase_relationship_screening_req_ids: list[BatchRequestIDType] = Field(
        default_factory=list
    )


LLMPhraseExtractionRequestMap = dict[str, LLMPhraseExtractionRequestBundle]


class DeferredLLMPhraseExtractionRequests(BaseModel):
    metadata: LLMPhraseExtractionMetadata
    chunked_request_map: LLMPhraseExtractionRequestMap
