from typing import Optional


from core.models.search_stage_results import (
    SearchStageMetadata,
    SearchStageResults,
    SearchStageStats,
)
from core.models.field_types import (
    LLMEvidenceResults,
    RawLLMMappingResult,
    LLMSearchResults,
    S3FileVersionIDType,
    OntologyVersionIDType,
)


class ConceptExtractionMetadata(SearchStageMetadata):
    ontology_version_id: OntologyVersionIDType
    evidence_prompt_version_id: Optional[S3FileVersionIDType]
    mapping_prompt_version_id: Optional[S3FileVersionIDType]


class ConceptExtractionStats(SearchStageStats):
    brute_search: set[str]  # regex search
    llm_search: LLMSearchResults  # identified by LLM
    llm_evidence: LLMEvidenceResults  # for each term in [brute | identified]
    llm_mapping: RawLLMMappingResult
    unmapped: set[str]


ConceptExtractionStatsMap = dict[
    str, ConceptExtractionStats
]  # "0:1000" -> {results, brute, identified, evidence, mapping, unmapped_llm}


class ConceptExtractionResults(SearchStageResults):
    metadata: ConceptExtractionMetadata
    chunk_stats: ConceptExtractionStatsMap
