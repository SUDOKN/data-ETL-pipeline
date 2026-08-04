from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time

from functools import cached_property
import json
import logging
from rdflib import URIRef
from pydantic import BaseModel, field_validator

from typing_extensions import TypedDict

logger = logging.getLogger(__name__)


class ConceptNode(TypedDict):
    name: str
    uri: URIRef
    level: int
    altLabels: list[str]
    definition: str
    children: list[ConceptNode]
    num_children: int


class Concept(BaseModel):
    name: str
    uri: str
    level: int
    altLabels: list[str]
    ancestors: list[str]
    children: list[str]
    definition: str

    @field_validator("definition", mode="before")
    @classmethod
    def normalize_definition_whitespace(cls, value: object) -> object:
        if isinstance(value, str):
            return " ".join(value.split())
        return value

    @cached_property
    def matchLabels(self) -> set[str]:
        return set([self.name] + self.altLabels)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Concept):
            return NotImplemented
        return self.__hash__() == other.__hash__()

    # DO NOT MODIFY
    def __str__(self) -> str:
        return f"{self.name}"

    def __repr__(self) -> str:
        return f"Concept(name={self.name}, uri={self.uri}, level={self.level}, altLabels={self.altLabels}, ancestors={self.ancestors}, definition={self.definition})"


class ConceptJSONEncoder(json.JSONEncoder):
    def default(self, o: object) -> object:
        if isinstance(o, Concept):
            full_definition = o.definition
            if o.altLabels:
                full_definition += f" It is also known as {', '.join(o.altLabels)}."
            if o.children:
                full_definition += f" Some subclasses include {', '.join(o.children)}."
            d: dict[str, object] = {
                "name": o.name,
                "definition": full_definition,
            }

            return d
        return super().default(o)
