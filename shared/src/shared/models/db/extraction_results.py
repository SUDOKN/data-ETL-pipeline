from datetime import datetime
from pydantic import BaseModel, ValidationInfo, field_validator

from shared.models.types import LLMMappingType, OntologyVersionIDType


class ChunkSearchStats(BaseModel):
    results: set[str]  # results from brute and llm search, maybe empty
    brute: set[str]
    llm: set[str]  # TODO: check if orphan llm is also present in the text
    mapping: LLMMappingType
    unmapped_llm: set[str]  # unmapped unknowns from llm search


# key: chunk_bounds, e.g. "0:1000"
SearchResult = dict[str, ChunkSearchStats]


class ExtractionStats(BaseModel):
    ontology_version_id: OntologyVersionIDType
    mapping: LLMMappingType
    search: SearchResult
    unmapped_llm: list[str]

    @field_validator("search")
    @classmethod
    def validate_search_keys(cls, v: SearchResult, info: ValidationInfo):
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


class ExtractionResults(BaseModel):
    extracted_at: datetime
    results: list[str]  # final results
    stats: ExtractionStats
