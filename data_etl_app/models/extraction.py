from pydantic import BaseModel
from models.extractor import (
    ChunkSearchStatsJSONSerialized,
    ExtractionResult,
    ExtractionResultsStatsJSONSerialized,
)
from data_etl_app.services.extractor import search_result_to_json_serializable


class ExtractionStats(BaseModel):
    mapping: dict[str, list[str]]
    search: dict[str, ChunkSearchStatsJSONSerialized]
    unmapped_brute: list[str]
    unmapped_llm: list[str]

    @classmethod
    def from_dict(cls, extracted_stats: ExtractionResultsStatsJSONSerialized):
        return cls(
            mapping=extracted_stats["mapping"],
            search=extracted_stats["search"],
            unmapped_brute=list(extracted_stats["unmapped_brute"]),
            unmapped_llm=list(extracted_stats["unmapped_llm"]),
        )


class ExtractedResults(BaseModel):
    results: list[str]
    stats: ExtractionStats

    @classmethod
    def from_dict(cls, extracted_results: ExtractionResult):
        json_serialized_results = search_result_to_json_serializable(extracted_results)
        return cls(
            results=json_serialized_results["results"],
            stats=ExtractionStats.from_dict(json_serialized_results["stats"]),
        )
