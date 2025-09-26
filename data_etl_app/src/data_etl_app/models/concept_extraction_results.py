from datetime import datetime
from pydantic import BaseModel, ValidationInfo, field_validator

from shared.models.field_types import (
    LLMMappingType,
    S3FileVersionIDType,
    OntologyVersionIDType,
)


class ConceptSearchChunkStats(BaseModel):
    results: set[str]  # results from brute and llm search, maybe empty
    brute: set[str]
    llm: set[str]  # TODO: check if orphan llm is also present in the text
    mapping: LLMMappingType
    unmapped_llm: set[str]  # unmapped unknowns from llm search


# "0:1000" -> { results, brute, llm, mapping, unmapped_llm }
ConceptSearchChunkMap = dict[str, ConceptSearchChunkStats]


class ConceptExtractionStats(BaseModel):
    extract_prompt_version_id: S3FileVersionIDType
    map_prompt_version_id: S3FileVersionIDType
    ontology_version_id: OntologyVersionIDType
    mapping: LLMMappingType  # all chunk mappings combined
    chunked_stats: ConceptSearchChunkMap
    unmapped_llm: list[str]

    @field_validator("chunked_stats")
    @classmethod
    def validate_chunk_map_keys(cls, v: ConceptSearchChunkMap, info: ValidationInfo):
        for key in v.keys():
            parts = key.split(":")
            if len(parts) != 2:
                raise ValueError(f"Invalid chunk_bounds key format: '{key}'")
            try:
                start = int(parts[0])
                _ = int(parts[1])
            except ValueError:
                raise ValueError(f"chunk_bounds key must contain integers: '{key}'")
            if start < 0:
                raise ValueError(f"chunk_bounds start must be >= 0: '{key}'")
        return v


class ConceptExtractionResults(BaseModel):
    extracted_at: datetime
    results: list[str]  # final results
    stats: ConceptExtractionStats
