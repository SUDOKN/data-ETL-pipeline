from __future__ import annotations
from pydantic import BaseModel

from core.models.extraction_results.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
    LLMPhraseExtractionStats,
    LLMPhraseExtractionMetadata,
)
from core.models.field_types import (
    PhraseToTagAndReasonMap,
    IterativeGroundingResult,
)


class ConceptsFound(BaseModel):
    in_vocab: set[str]
    out_of_vocab: set[str]


class ConceptExtractionStats(LLMPhraseExtractionStats):
    results: ConceptsFound
    brute_search: set[str]  # regex search
    llm_phrase_initial_grounding: PhraseToTagAndReasonMap
    llm_phrase_recursive_grounding: (
        IterativeGroundingResult  # level by level organized nodes
    )


ConceptExtractionStatsMap = dict[
    str, ConceptExtractionStats
]  # "0:1000" -> {results, brute, identified, phrase_relationship, mapping, unmapped_llm}


class ConceptExtractionMetadata(LLMPhraseExtractionMetadata):
    llm_phrase_initial_grounding: ExtractionNodeMetadata
    llm_phrase_recursive_grounding: ExtractionNodeMetadata


class ConceptExtractionResults(BaseModel):
    metadata: ConceptExtractionMetadata
    results: ConceptsFound
    chunked_extraction_stats: ConceptExtractionStatsMap
