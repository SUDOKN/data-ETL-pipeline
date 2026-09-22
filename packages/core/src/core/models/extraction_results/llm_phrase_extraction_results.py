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
    BatchedSynthesisNodeMetadata,
    BatchedRelationshipNodeMetadata,
    BatchedScreeningNodeMetadata,
    DescentNodeMetadata,
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
)
from core.models.extraction_schemas.descent import DescentTrail
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
    # RETIRED by Step 2 (2026-09-22): unit screening replaces it; defaulted so
    # results stored by earlier runs load and Step 2 runs leave it empty.
    llm_phrase_screening: dict[int, RecordScreeningResults] = Field(default_factory=dict)
    # Step 2 unit screening (design draft §5, 2026-09-21): the same per-record,
    # per-candidate verdicts, now carrying the evidence distance, the failed
    # rule and the quote (``CandidateScreeningVerdict``). For concept fields
    # this is the pre-descent wave (the labels grounding matched directly);
    # descent's per-wave verdicts live in ``llm_phrase_descent_screening``.
    # Defaulted so results stored before the stage existed load unchanged;
    # ``llm_phrase_screening`` stays required until the cutover retires it.
    llm_phrase_unit_screening: dict[int, RecordScreeningResults] = Field(
        default_factory=dict
    )


class ConceptExtractionStats(LLMPhraseExtractionStats):
    # in_vocab = passed screening then deepened by recursive descent
    # (post-recursive); out_of_vocab = OOV candidates that passed screening.
    results: ConceptsFound
    brute_search: set[str]  # regex search survivors
    # RETIRED by Step 2 (2026-09-22): the one call (``llm_phrase_grounding``)
    # and the wave descent (``llm_phrase_descent_trail``) replace these two;
    # defaulted so results stored by earlier runs load.
    llm_phrase_initial_grounding: Optional[InitialGroundingStats] = None
    llm_phrase_recursive_grounding: IterativeGroundingResult = Field(default_factory=dict)
    # --- Step 2 (2026-09-21), additive beside the blocks above until the
    # cutover retires them; every field defaulted so stored results load. ---
    # The ONE grounding call (design draft §3): ``in_vocab`` = the vocabulary
    # labels it matched per record, each tag carrying the record's quote as its
    # evidence rule; ``out_of_vocab`` = the proposals — from the same call, and
    # from the proposal pass when it ran. The two-bucket type is reused because
    # the buckets mean the same thing; only the number of requests changed.
    llm_phrase_grounding: Optional[InitialGroundingStats] = None
    # The proposal pass's own trail (run flag; §54.2): the records the grounding
    # call left with no label, read again — matched after all, proposed, or
    # confirmed as nothing — so the census can tell which call produced what.
    llm_phrase_proposal: dict[int, RecordGroundingResults] = Field(
        default_factory=dict
    )
    # Descent in depth waves (§6.3): depth → the screening verdicts of that
    # wave's group (descent-reached pairs ∪ direct matches at that depth); the
    # groups themselves stay in ``llm_phrase_recursive_grounding``'s type.
    llm_phrase_descent_screening: dict[int, RecordScreeningResults] = Field(
        default_factory=dict
    )
    # The leaf step (§6.5): accepted leaf label → per-record proposals kept
    # aside (the dump's copy of what ``VocabularyCandidate`` stores).
    llm_phrase_leaf_step: dict[str, RecordGroundingResults] = Field(
        default_factory=dict
    )
    # The whole descent, as the wave loop stored it (2026-09-22): every depth
    # wave's units, verdicts, descent answers, leaf answers and false children,
    # then the proposal wave. The two blocks above are views the reconcile
    # step fills from it.
    llm_phrase_descent_trail: Optional[DescentTrail] = None


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
    # Step 2 unit screening (2026-09-21): units of one label and the records
    # grounding matched on, ≤ ``max_pairs_per_request`` DISTINCT records per
    # request (D8). Optional beside the stage it replaces until the cutover;
    # the factory sets it for a Step 2 run and the prefill staleness check
    # turns None-vs-set into the standard re-defer.
    llm_phrase_unit_screening: Optional[BatchedScreeningNodeMetadata] = None
    # v3 (PIPELINE_V3_PLAN.md Phase 3.1): the aggregation fold's identity
    # (which since 2026-09-03 also carries the snippet-radius clip dial — the
    # mention-collection LLM stage and its metadata were retired when the
    # synthesis stage absorbed the location task). Optional so every stored v2
    # document still loads; a v3 chain always sets it, and the prefill
    # staleness check turns None-vs-set into the standard re-defer.
    aggregation_fold: Optional[AggregationFoldMetadata] = None
    # v3 Phase 3.2: the synthesis stage's identity. Same Optional-for-loading,
    # always-set-by-the-factory contract as the one above.
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
    # --- Step 2 (2026-09-21), additive until the cutover retires the three
    # above. Same Optional-for-loading, set-by-the-factory contract. ---
    # The one grounding call (initial + OOV merged, D5): the group cap decides
    # which records share a request, as before.
    llm_phrase_grounding: Optional[BatchedInitialGroundingNodeMetadata] = None
    # The proposal pass for records the call left with no label; None = the
    # pass is OFF for this run (the run flag, the same convention as
    # ``llm_phrase_oov_grounding``).
    llm_phrase_proposal: Optional[BatchedInitialGroundingNodeMetadata] = None
    # Descent in depth waves, with the leaf-step flag.
    llm_phrase_descent: Optional[DescentNodeMetadata] = None


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
