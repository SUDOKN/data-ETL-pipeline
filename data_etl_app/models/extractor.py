from typing import TypedDict
from models.skos_concept import Concept


class ExtractionResultsStats(TypedDict):
    mapping: dict[Concept, list[str]]
    brute_search: set[Concept]
    llm_search: set[str]
    unmapped_brute: set[Concept]
    unmapped_llm: set[str]


class ExtractionResultsStatsJSONSerialized(TypedDict):
    mapping: dict[str, list[str]]
    brute_search: list[str]
    llm_search: list[str]
    unmapped_brute: list[str]
    unmapped_llm: list[str]


class ExtractionResult(TypedDict):
    results: set[str]
    stats: ExtractionResultsStats


class ExtractionResultJSONSerialized(TypedDict):
    results: list[str]
    stats: ExtractionResultsStatsJSONSerialized
