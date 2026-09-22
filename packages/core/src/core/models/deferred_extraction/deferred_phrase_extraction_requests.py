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
    # The OPT-IN second search pass over the same sub-windows (2026-09-03,
    # Phase B retry-and-union; user decision: DEFAULT OFF, enabled per run via
    # the node's `search_union_pass` knob): index-aligned like the first, same
    # window text, its own request ids (`>pass>2>` in the id). When present,
    # the chunk view unions the passes — the measured A/A churn means a second
    # identical read adds ~11-13% distinct forms, and a window whose first
    # pass degenerated (repetition loop) is carried by its second. Empty on
    # default-off runs and pre-cutover docs; whatever a union-enabled run
    # embedded stays honored on every later read.
    llm_phrase_search_pass2_req_ids: list[BatchRequestIDType] = Field(default_factory=list)
    # Per sub-window (keyed by its search_sub_bounds entry): the ordered list of
    # recursive search rounds (round 1 == index 0). Each round re-searches the
    # sub-window while excluding the compounding union of phrases found there by
    # the first search and all prior recursive rounds. Sub-windows converge
    # independently, so the lists are ragged.
    llm_phrase_recursive_search_req_ids: dict[str, list[BatchRequestIDType]] = Field(
        default_factory=dict
    )
    # v3 mention-collection LLM stage (out of every chain since the
    # location-stage merge, 2026-09-03 — synthesis absorbed the location task).
    # The two request-id fields and the retry list stay so stored pre-merge
    # documents load and keep their provenance, the same posture as the
    # relationship fields below; no node writes or reads them any more.
    llm_phrase_mention_req_ids: dict[str, list[BatchRequestIDType]] = Field(
        default_factory=dict
    )
    # Per sub-window: the sent forms — the SUBJECT's search ∪ brute casings,
    # filtered to the forms that OCCUR in the sub-window (user decisions
    # 2026-08-22 / 2026-09-02), sorted. LIVE: written by the synthesis node's
    # embed pass (pass 0, ported from the retired mention node — the field
    # keeps its historical name so stored documents stay valid); the fold
    # re-collects from the text + these forms alone.
    llm_phrase_mention_sent_forms: dict[str, list[str]] = Field(default_factory=dict)
    # Retired with the mention stage (see llm_phrase_mention_req_ids).
    llm_phrase_mention_retry_mention_ids: dict[str, list[str]] = Field(default_factory=dict)
    llm_phrase_mention_retry_req_ids: dict[str, list[BatchRequestIDType]] = Field(
        default_factory=dict
    )
    # v3 synthesis (PIPELINE_V3_PLAN.md D15 as amended 2026-08-22, D16; Phase
    # 3.2; merged with the location task 2026-09-03). Per CHUNK (this bundle):
    # the ordered list of synthesis request groups (group 1 == index 0). The
    # chunk's fold records — one per non-empty group, focal form + snippets —
    # are packed in bundle order into requests of at most
    # `max_entries_per_request` snippets, a record never split; a chunk with no
    # records gets one dummy request. Records are recomputed from the text +
    # the stored sent forms (the fold is pure code), so nothing but the ids is
    # stored.
    llm_phrase_synthesis_req_ids: list[BatchRequestIDType] = Field(default_factory=list)
    # Under-answer policy (user decision 2026-08-22): once the chunk's group
    # requests are complete it is ASSESSED — the record ids no answer
    # synthesized, the answered ids whose synthesis dropped designations
    # (2026-09-02) are stored here (an empty list = assessed, none missing;
    # None = not yet assessed) and, when any exist, ONE retry pass re-asks for
    # just those records (packed the same way into the ids below). The result
    # reads group answers first, then the retry's; what is still missing after
    # that is reported, never retried twice.
    llm_phrase_synthesis_retry_record_ids: Optional[list[str]] = None
    llm_phrase_synthesis_retry_req_ids: list[BatchRequestIDType] = Field(default_factory=list)
    # Step 2 unit screening (2026-09-21): wave → the wave's request ids. A
    # keyword field has one wave (its freehand candidates); a concept field
    # has one per vocabulary depth, issued by the descent loop (design draft
    # §6.3). No under-answer retry: the parser holds each response to the
    # request's own units and records exactly, and a breach goes to the
    # parse-error re-dispatch. Defaulted so every stored bundle loads.
    llm_phrase_unit_screening_req_ids: dict[int, list[BatchRequestIDType]] = Field(
        default_factory=dict
    )
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
