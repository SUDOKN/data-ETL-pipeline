from typing import Optional


from core.models.search_stage_results import (
    SearchStageMetadata,
    SearchStageResults,
    SearchStageExtractionStats,
)
from core.models.field_types import (
    LLMDistillationResults,
    RawLLMMappingResult,
    S3FileVersionIDType,
    OntologyVersionIDType,
)


class ConceptExtractionMetadata(SearchStageMetadata):
    ontology_version_id: OntologyVersionIDType
    distillation_prompt_version_id: Optional[S3FileVersionIDType]
    mapping_prompt_version_id: Optional[S3FileVersionIDType]


class ConceptExtractionStats(SearchStageExtractionStats):
    brute_search: set[str]  # regex search
    llm_distillation: LLMDistillationResults  # for each term in [brute | identified]
    llm_mapping: RawLLMMappingResult
    unmapped: set[str]


ConceptExtractionStatsMap = dict[
    str, ConceptExtractionStats
]  # "0:1000" -> {results, brute, identified, distillation, mapping, unmapped_llm}


class ConceptExtractionResults(SearchStageResults):
    metadata: ConceptExtractionMetadata
    chunk_stats: ConceptExtractionStatsMap
