from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time
from functools import cached_property
import json

from typing import TypedDict, Optional

class ConceptNode(TypedDict):
    name: str
    altLabels: list[str]
    children: list[ConceptNode]
    ancestors: Optional[list[str]]  # no quotes needed
    antiLabels: Optional[list[str]]  # no quotes needed


class Concept:
    def __init__(
        self,
        name: str,
        altLabels: list[str],
        ancestors: Optional[list[str]] = None,
        antiLabels: Optional[list[str]] = None,
    ) -> None:
        self._name = name
        self._altLabels = altLabels
        self._ancestors = ancestors
        self._antiLabels = antiLabels

    @property
    def name(self) -> str:
        return self._name

    @property
    def altLabels(self) -> list[str]:
        return self._altLabels

    @property
    def ancestors(self) -> Optional[list[str]]:
        return self._ancestors

    @property
    def antiLabels(self) -> Optional[list[str]]:
        return self._antiLabels

    @cached_property
    def matchLabels(self) -> set[str]:
        return set([self.name] + self.altLabels)

    def __hash__(self):
        return hash(self.name)

    def __str__(self) -> str:
        return f"{self.name}"

    def __repr__(self) -> str:
        return f"Concept(name={self.name}, altLabels={self.altLabels}, ancestors={self.ancestors}, antiLabels={self.antiLabels})"


class ConceptJSONEncoder(json.JSONEncoder):
    def default(self, o: object) -> object:
        # print(f"o:{o}")
        # print(f"o type:{type(o)}")
        # print(f"o.__class__:{o.__class__}")
        # print(f"o.__class__.__name__:{o.__class__.__name__}")
        # print(f"isinstance(o, Concept):{isinstance(o, Concept)}")
        if isinstance(o, Concept):
            d: dict[str, object] = {
                "name": o.name,
                "altLabels": o.altLabels,
            }
            if o.ancestors is not None:
                d["ancestors"] = o.ancestors
            if o.antiLabels is not None:
                d["antiLabels"] = o.antiLabels
            return d
        return super().default(o)

