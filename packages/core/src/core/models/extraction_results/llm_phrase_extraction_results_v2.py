"""Pipeline v2's stored stats and metadata shapes (PIPELINE_V2_PLAN.md).

These are THE canonical shapes since the phase-2.9 flip: the v1 stats/metadata
classes are deleted, and only the per-stage node-metadata classes (which never
changed shape) still live in the older modules. Folding these definitions back
into those files under unsuffixed names is deferred cosmetic work — the V2
suffix is load-bearing nowhere.

Two deliberate departures from v1:

- **Keywords adopt ``ConceptsFound``** with ``in_vocab`` empty by construction
  (keyword fields have no vocabulary), so all seven phrase fields share one
  results shape and one reader.
- **One stats-map field name for both families** (``chunked_extraction_stats``)
  — the v1 concept/keyword naming split (``chunked_extraction_stats`` vs
  ``chunk_stats``) bought nothing and cost every consumer a branch.

The upstream-content digest (fork F12) is NOT here: it rides request-id
CONSTRUCTION (the ``|ud=`` segment every downstream ``get_request_custom_id``
appends), which knows the upstream results — node metadata is fixed before
they exist.
"""

from __future__ import annotations

from typing import Optional, TypeVar

from pydantic import BaseModel

from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
    ConceptsFound,
)
from core.models.extraction_results.keyword_extraction_results import (
    BatchedFreehandGroundingNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    AggregationFoldMetadata,
    BaseExtractionMetadata,
    BatchedMentionCollectionNodeMetadata,
    BatchedSynthesisNodeMetadata,
    BatchedRelationshipNodeMetadata,
    BatchedScreeningNodeMetadata,
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
)
from core.models.extraction_schemas.grounding import RecordGroundingResults
from core.models.extraction_schemas.iterative_tagging import (
    IterativeGroundingResult,
)
from core.models.extraction_schemas.relationship import (
    MaskedLLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.screening import RecordScreeningResults
from core.models.extraction_schemas.search import LLMSearchResults


_T = TypeVar("_T")


def partition_records_by_search_round(
    flat_results: dict[str, _T],
    phrase_by_record_id: dict[str, str],
    search_rounds: dict[int, "LLMSearchResults"],
) -> dict[int, dict[str, _T]]:
    """Assign each RECORD-keyed result to its phrase's earliest search round.

    The v2 twin of ``partition_by_search_round``: stage results are keyed by
    record_id while search rounds speak phrases, so the split reads each
    record's phrase through the masked relationship join
    (``phrase_by_record_id``). A record whose phrase is absent from every round
    falls back to round 0, the same defensive default as v1; round 0 stays
    present even when empty so consumers can index it unconditionally.
    """
    phrase_to_round: dict[str, int] = {}
    for round_idx in sorted(search_rounds.keys()):
        for phrase in search_rounds[round_idx]:
            if phrase not in phrase_to_round:
                phrase_to_round[phrase] = round_idx

    rounds: dict[int, dict[str, _T]] = {}
    for record_id, value in flat_results.items():
        phrase = phrase_by_record_id.get(record_id)
        round_idx = phrase_to_round.get(phrase, 0) if phrase is not None else 0
        rounds.setdefault(round_idx, {})[record_id] = value
    rounds.setdefault(0, {})
    return rounds


class InitialGroundingStats(BaseModel):
    """The two-pass grounding block. ``out_of_vocab`` waits on ``in_vocab`` for
    concepts and stays empty when the OOV pass is toggled off — which is run
    config reflected in metadata (``llm_phrase_oov_grounding: None``), never a
    StageToggle (those are hard-stop by locked decision)."""

    # round → {record_id: entry} — each record assigned to its phrase's earliest
    # search round; a declined record keeps its explanation (see
    # RecordGroundingEntry).
    in_vocab: dict[int, RecordGroundingResults]
    out_of_vocab: dict[int, RecordGroundingResults]


class LLMPhraseExtractionStatsV2(BaseModel):
    # round -> phrases found independently that round (NOT cumulative); round 0
    # reserved for brute survivors, empty for keyword fields.
    llm_phrase_search: dict[int, LLMSearchResults]
    # round → {record_id: {phrase, record}} — the masked relationship results;
    # the id→phrase join every later stage reads through lives HERE.
    llm_phrase_relationship: dict[int, MaskedLLMPhraseRelationshipResults]
    # round → {record_id: {candidate: verdict}} — every candidate judged.
    llm_phrase_screening: dict[int, RecordScreeningResults]


class ConceptExtractionStatsV2(LLMPhraseExtractionStatsV2):
    # in_vocab = passed screening then deepened by recursive descent
    # (post-recursive); out_of_vocab = OOV candidates that passed screening.
    results: ConceptsFound
    brute_search: set[str]  # regex search survivors
    llm_phrase_initial_grounding: InitialGroundingStats
    llm_phrase_recursive_grounding: IterativeGroundingResult


class KeywordExtractionStatsV2(LLMPhraseExtractionStatsV2):
    # Uniform shape: in_vocab is empty by construction (no vocabulary).
    results: ConceptsFound
    # round → {record_id: entry} — minted candidates as the entry's tags.
    llm_phrase_freehand_grounding: dict[int, RecordGroundingResults]


ConceptExtractionStatsMapV2 = dict[str, ConceptExtractionStatsV2]  # "0:1000" -> stats
KeywordExtractionStatsMapV2 = dict[str, KeywordExtractionStatsV2]


class LLMPhraseExtractionMetadataV2(BaseExtractionMetadata):
    llm_phrase_search: ExtractionNodeMetadata
    llm_phrase_recursive_search: RecursiveSearchNodeMetadata
    # v2 relationship — out of every chain since v3 3.1 (2026-08-21); still
    # required here because it is part of every stored run identity, until 3.3
    # retires the stage. No v3 node reads it.
    llm_phrase_relationship: BatchedRelationshipNodeMetadata
    llm_phrase_relationship_screening: BatchedScreeningNodeMetadata
    # v3 (PIPELINE_V3_PLAN.md Phase 3.1): the mention collector and the
    # aggregation fold's identity. Optional so every stored v2 document still
    # loads; a v3 chain always sets both, and the prefill staleness check turns
    # None-vs-set into the standard re-defer.
    llm_phrase_mention_collection: Optional[BatchedMentionCollectionNodeMetadata] = None
    aggregation_fold: Optional[AggregationFoldMetadata] = None
    # v3 Phase 3.2: the synthesis stage's identity (cap + location arm). Same
    # Optional-for-loading, always-set-by-the-factory contract as the two above.
    llm_phrase_synthesis: Optional[BatchedSynthesisNodeMetadata] = None


class ConceptExtractionMetadataV2(LLMPhraseExtractionMetadataV2):
    # In-vocab pass. Reuses the batched grounding node shape: the group cap
    # decides which records share a request, exactly as before.
    llm_phrase_initial_grounding: BatchedInitialGroundingNodeMetadata
    # None = the OOV discovery pass is off for this run. Optional HERE is what
    # makes a run without it a distinct run identity while old and new metadata
    # both load.
    llm_phrase_oov_grounding: Optional[BatchedInitialGroundingNodeMetadata] = None
    llm_phrase_recursive_grounding: ExtractionNodeMetadata


class KeywordExtractionMetadataV2(LLMPhraseExtractionMetadataV2):
    llm_phrase_freehand_grounding: BatchedFreehandGroundingNodeMetadata


class ConceptExtractionResultsV2(BaseModel):
    metadata: ConceptExtractionMetadataV2
    results: ConceptsFound
    chunked_extraction_stats: ConceptExtractionStatsMapV2


class KeywordExtractionResultsV2(BaseModel):
    metadata: KeywordExtractionMetadataV2
    results: ConceptsFound
    chunked_extraction_stats: KeywordExtractionStatsMapV2
