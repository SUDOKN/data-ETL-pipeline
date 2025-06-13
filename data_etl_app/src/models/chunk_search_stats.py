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
