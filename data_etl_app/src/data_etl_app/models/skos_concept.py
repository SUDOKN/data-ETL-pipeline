from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time

from functools import cached_property
import json
import logging
from rdflib import URIRef

from typing_extensions import TypedDict

logger = logging.getLogger(__name__)


class ConceptNode(TypedDict):
    name: str
    uri: URIRef
    altLabels: list[str]
    definition: str
    children: list[ConceptNode]
    childrenCount: int


class Concept:
    def __init__(
        self,
        name: str,
        uri: URIRef,
        altLabels: list[str],
        ancestors: list[str],
        definition: str,
    ) -> None:
        self.name = name
        self.uri = uri
        self.altLabels = altLabels
        self.ancestors = ancestors
        self.definition = definition

    @cached_property
    def matchLabels(self) -> set[str]:
        return set([self.name] + self.altLabels)

    def __hash__(self):
        return hash(self.name)

    # DO NOT MODIFY
    def __str__(self) -> str:
        return f"{self.name}"

    def __repr__(self) -> str:
        return f"Concept(name={self.name}, uri={self.uri}, altLabels={self.altLabels}, ancestors={self.ancestors}, definition={self.definition})"


class ConceptJSONEncoder(json.JSONEncoder):
    def default(self, o: object) -> object:
        if isinstance(o, Concept):
            d: dict[str, object] = {
                "name": o.name,
                "altLabels": o.altLabels,
                "ancestors": o.ancestors,
                "definition": o.definition,
            }

            return d
        return super().default(o)
