from pydantic import BaseModel
from typing import List, Dict
from models.extractor import (
    ExtractionResult,
    ExtractionResultsStatsJSONSerialized,
)
from services.extractor import search_result_to_json_serializable

class ExtractionStats(BaseModel):
    mapping: Dict[str, List[str]]
    brute_search: List[str]
    llm_search: List[str]
    unmapped_brute: List[str]
    unmapped_llm: List[str]

    @classmethod
    def from_dict(cls, extracted_stats: ExtractionResultsStatsJSONSerialized):
        return cls(
            mapping=extracted_stats["mapping"],
            brute_search=list(extracted_stats["brute_search"]),
            llm_search=list(extracted_stats["llm_search"]),
            unmapped_brute=list(extracted_stats["unmapped_brute"]),
            unmapped_llm=list(extracted_stats["unmapped_llm"]),
        )


class ExtractedResults(BaseModel):
    results: List[str]
    stats: ExtractionStats

    @classmethod
    def from_dict(cls, extracted_results: ExtractionResult):
        return cls(
            results=list(extracted_results["results"]),
            stats=ExtractionStats.from_dict(
                search_result_to_json_serializable(extracted_results)["stats"]
            ),
        )