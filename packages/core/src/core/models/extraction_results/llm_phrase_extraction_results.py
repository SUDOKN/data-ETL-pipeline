"""The stored stats and metadata shapes of the phrase pipeline.

These are THE canonical shapes, and the only ones: the v1 stats/metadata
classes were deleted at the phase-2.9 flip (PIPELINE_V2_PLAN.md), and the
per-stage node-metadata classes they shared a file with now live in
``extraction_node_metadata`` under the name that describes what they are.

The ``V2`` suffix these classes carried until 2026-08-27 is gone with them.
It marked a migration that finished, and once nothing on the other side of it
survived, the suffix only implied a v1 the reader would go looking for. Version
talk belongs in the notes below, where it explains a decision — not in a name.

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

from pydantic import BaseModel, Field

from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
    ConceptsFound,
)
from core.models.extraction_results.keyword_extraction_results import (
    BatchedFreehandGroundingNodeMetadata,
)
from core.models.extraction_results.extraction_node_metadata import (
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
from core.models.extraction_schemas.run_provenance import RunProvenance
from core.models.extraction_schemas.stored_fold import StoredFold
from core.models.extraction_schemas.iterative_tagging import (
    IterativeGroundingResult,
)
from core.models.extraction_schemas.relationship import (
    MaskedLLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.synthesis import GroupRecords
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


class LLMPhraseExtractionStats(BaseModel):
    # round -> phrases found independently that round (NOT cumulative); round 0
    # reserved for brute survivors, empty for keyword fields.
    llm_phrase_search: dict[int, LLMSearchResults]
    # v3 (2026-08-27): the chunk's aggregation fold, persisted. Which mention
    # landed in which group is rule-dependent intermediary data no stored
    # result reproduces — recomputing it after a rules change answers a
    # different question. Offsets, not passages; see `stored_fold`. Inline here
    # (as well as in the run-keyed `extraction_runs` collection) so reading one
    # manufacturer needs no second lookup.
    aggregation_fold: StoredFold
    # v2's relationship stage, RETIRED by v3 (3.3): populated only on results
    # written before the re-key; new runs leave it empty. round →
    # {record_id: {phrase, record}}.
    llm_phrase_relationship: dict[int, MaskedLLMPhraseRelationshipResults] = Field(
        default_factory=dict
    )
    # v3 (3.3, D16): round → {group_id: {focal_form, synthesis}} — the
    # per-group records every downstream verdict keys against; the
    # id→focal-form join every later stage reads through lives HERE. Empty on
    # results written before the re-key.
    llm_phrase_synthesis: dict[int, GroupRecords] = Field(default_factory=dict)
    # round → {record_id: {candidate: verdict}} — every candidate judged.
    llm_phrase_screening: dict[int, RecordScreeningResults]


class ConceptExtractionStats(LLMPhraseExtractionStats):
    # in_vocab = passed screening then deepened by recursive descent
    # (post-recursive); out_of_vocab = OOV candidates that passed screening.
    results: ConceptsFound
    brute_search: set[str]  # regex search survivors
    llm_phrase_initial_grounding: InitialGroundingStats
    llm_phrase_recursive_grounding: IterativeGroundingResult


class KeywordExtractionStats(LLMPhraseExtractionStats):
    # Uniform shape: in_vocab is empty by construction (no vocabulary).
    results: ConceptsFound
    # round → {record_id: entry} — minted candidates as the entry's tags.
    llm_phrase_freehand_grounding: dict[int, RecordGroundingResults]


ConceptExtractionStatsMap = dict[str, ConceptExtractionStats]  # "0:1000" -> stats
KeywordExtractionStatsMap = dict[str, KeywordExtractionStats]


class LLMPhraseExtractionMetadata(BaseExtractionMetadata):
    llm_phrase_search: ExtractionNodeMetadata
    llm_phrase_recursive_search: RecursiveSearchNodeMetadata
    # v2 relationship — RETIRED at v3 3.3 (out of every chain since 3.1).
    # Optional so every stored pre-3.3 run identity still loads; new runs
    # carry None and no node reads it.
    llm_phrase_relationship: Optional[BatchedRelationshipNodeMetadata] = None
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


class ConceptExtractionMetadata(LLMPhraseExtractionMetadata):
    # In-vocab pass. Reuses the batched grounding node shape: the group cap
    # decides which records share a request, exactly as before.
    llm_phrase_initial_grounding: BatchedInitialGroundingNodeMetadata
    # None = the OOV discovery pass is off for this run. Optional HERE is what
    # makes a run without it a distinct run identity while old and new metadata
    # both load.
    llm_phrase_oov_grounding: Optional[BatchedInitialGroundingNodeMetadata] = None
    llm_phrase_recursive_grounding: ExtractionNodeMetadata


class KeywordExtractionMetadata(LLMPhraseExtractionMetadata):
    llm_phrase_freehand_grounding: BatchedFreehandGroundingNodeMetadata


class ConceptExtractionResults(BaseModel):
    metadata: ConceptExtractionMetadata
    # BESIDE metadata, never inside it: the prefill staleness check compares
    # stored metadata against the live configuration, and a run timestamp in
    # there would make every resume read as drift.
    run_provenance: RunProvenance
    results: ConceptsFound
    chunked_extraction_stats: ConceptExtractionStatsMap


class KeywordExtractionResults(BaseModel):
    metadata: KeywordExtractionMetadata
    run_provenance: RunProvenance
    results: ConceptsFound
    chunked_extraction_stats: KeywordExtractionStatsMap
