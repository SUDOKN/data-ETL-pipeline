from pydantic import BaseModel
from typing import TypedDict

from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.chunk_search_stats import (
    ChunkSearchStats,
    ChunkSearchStatsJSONSerialized,
)
from data_etl_app.models.db.extraction_stats import (
    ExtractionStatsJSONSerialized,
)


class ExtractionStats(TypedDict):
    search: dict[str, ChunkSearchStats]  # str is chunk index boundary, e.g. "0:100"
    mapping: dict[Concept, list[str]]
    unmapped_brute: set[Concept]
    unmapped_llm: set[str]


class ExtractionStatsJSONSerialized(TypedDict):
    mapping: dict[str, list[str]]
    search: dict[str, ChunkSearchStatsJSONSerialized]
    unmapped_brute: list[str]
    unmapped_llm: list[str]


class ExtractionStats_DBModel(BaseModel):
    mapping: dict[str, list[str]]
    search: dict[str, ChunkSearchStatsJSONSerialized]
    unmapped_brute: list[str]
    unmapped_llm: list[str]

    @classmethod
    def from_dict(cls, extracted_stats: ExtractionStatsJSONSerialized):
        return cls(
            mapping=extracted_stats["mapping"],
            search=extracted_stats["search"],
            unmapped_brute=list(extracted_stats["unmapped_brute"]),
            unmapped_llm=list(extracted_stats["unmapped_llm"]),
        )
