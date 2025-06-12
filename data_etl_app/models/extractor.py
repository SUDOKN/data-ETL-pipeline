from typing import TypedDict
from models.skos_concept import Concept


class ChunkSearchStats(TypedDict):
    human: set[str]
    brute: set[Concept]
    llm: set[str]


class ChunkSearchStatsJSONSerialized(TypedDict):
    human: list[str]
    brute: list[str]
    llm: list[str]


class ExtractionResultsStats(TypedDict):
    search: dict[str, ChunkSearchStats]  # str is chunk index boundary, e.g. "0:100"
    mapping: dict[Concept, list[str]]
    unmapped_brute: set[Concept]
    unmapped_llm: set[str]


class ExtractionResultsStatsJSONSerialized(TypedDict):
    search: dict[str, ChunkSearchStatsJSONSerialized]
    mapping: dict[str, list[str]]
    unmapped_brute: list[str]
    unmapped_llm: list[str]


class ExtractionResult(TypedDict):
    results: set[str]
    stats: ExtractionResultsStats


class ExtractionResultJSONSerialized(TypedDict):
    results: list[str]
    stats: ExtractionResultsStatsJSONSerialized
