"""Pipeline v2's stored stats and metadata shapes (PIPELINE_V2_PLAN.md).

Standalone rather than inheriting the v1 classes: the shared stage fields
changed type (relationship is masked and record-keyed, screening is
per-candidate), so there is no common base left to share. The v1 modules stay
untouched until the phase-2 cutover retires them; these fold back into the
canonical files then.

Two deliberate departures from v1:

- **Keywords adopt ``ConceptsFound``** with ``in_vocab`` empty by construction
  (keyword fields have no vocabulary), so all seven phrase fields share one
  results shape and one reader.
- **One stats-map field name for both families** (``chunked_extraction_stats``)
  — the v1 concept/keyword naming split (``chunked_extraction_stats`` vs
  ``chunk_stats``) bought nothing and cost every consumer a branch.

The upstream-content digest (fork F12) is NOT here: it rides request-id
CONSTRUCTION, which knows the upstream results, not node metadata, which is
fixed before they exist. It lands with the phase-2.9 embedding rework.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel

from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
    ConceptsFound,
)
from core.models.extraction_results.keyword_extraction_results import (
    BatchedFreehandGroundingNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    BaseExtractionMetadata,
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
    llm_phrase_relationship: BatchedRelationshipNodeMetadata
    llm_phrase_relationship_screening: BatchedScreeningNodeMetadata


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
