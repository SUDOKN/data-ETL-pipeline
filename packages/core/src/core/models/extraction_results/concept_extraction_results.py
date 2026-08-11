from __future__ import annotations
from pydantic import BaseModel

from core.models.extraction_results.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
    LLMPhraseExtractionStats,
    LLMPhraseExtractionMetadata,
)
from core.models.extraction_schemas.grounding import (
    PhraseToTagAndRulesMap,
)
from core.models.extraction_schemas.iterative_tagging import (
    IterativeGroundingResult,
)


class ConceptsFound(BaseModel):
    in_vocab: set[str]
    out_of_vocab: set[str]


class ConceptExtractionStats(LLMPhraseExtractionStats):
    results: ConceptsFound
    brute_search: set[str]  # regex search
    # round → {phrase: {tag: [applied_rule, ...]}} — each phrase assigned to its earliest search round
    llm_phrase_initial_grounding: dict[int, PhraseToTagAndRulesMap]
    llm_phrase_recursive_grounding: (
        IterativeGroundingResult  # level by level organized nodes
    )


ConceptExtractionStatsMap = dict[
    str, ConceptExtractionStats
]  # "0:1000" -> {results, brute, identified, phrase_relationship, mapping, unmapped_llm}


class BatchedInitialGroundingNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on phrase pairs per initial-grounding request. The screened
    # out-of-vocab phrases for a chunk are split into
    # ceil(num_pairs / max_pairs_per_request) groups, each grounded independently
    # and merged back into one flat result.
    max_pairs_per_request: int


class ConceptExtractionMetadata(LLMPhraseExtractionMetadata):
    llm_phrase_initial_grounding: BatchedInitialGroundingNodeMetadata
    llm_phrase_recursive_grounding: ExtractionNodeMetadata


class ConceptExtractionResults(BaseModel):
    metadata: ConceptExtractionMetadata
    results: ConceptsFound
    chunked_extraction_stats: ConceptExtractionStatsMap
