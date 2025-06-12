from pydantic import BaseModel
from typing import TypedDict

from data_etl_app.models.db.extraction_stats import (
    ExtractionStats,
    ExtractionStatsJSONSerialized,
    ExtractionStats_DBModel,
)


class ExtractionResult(TypedDict):
    results: set[str]
    stats: ExtractionStats


class ExtractionResultJSONSerialized(TypedDict):
    results: list[str]
    stats: ExtractionStatsJSONSerialized


class ExtractionResults_DBModel(BaseModel):
    results: list[str]
    stats: ExtractionStats_DBModel

    @classmethod
    def from_dict(cls, extracted_results: ExtractionResult):
        json_serialized_results = _get_json_serializable_extraction_result(
            extracted_results
        )
        return cls(
            results=json_serialized_results["results"],
            stats=ExtractionStats_DBModel.from_dict(json_serialized_results["stats"]),
        )


def _get_json_serializable_extraction_result(
    search_result: ExtractionResult,
) -> ExtractionResultJSONSerialized:
    mapping = {str(k): v for k, v in search_result["stats"]["mapping"].items()}
    return {
        "results": list(search_result["results"]),
        "stats": {
            "search": {
                chunk_bounds: {
                    "human": list(v["human"] or []),
                    "brute": [str(c) for c in v["brute"]],
                    "llm": list(v["llm"]),
                }
                for chunk_bounds, v in search_result["stats"]["search"].items()
            },
            "mapping": mapping,
            "unmapped_brute": [
                str(k) for k in search_result["stats"]["unmapped_brute"]
            ],
            "unmapped_llm": list(search_result["stats"]["unmapped_llm"]),
        },
    }
