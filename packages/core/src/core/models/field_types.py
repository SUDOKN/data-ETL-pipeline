"""Structural types describing what `core` requires of an app-declared extractable field.

`core` deliberately knows nothing about *which* fields exist — the consuming app declares
them (typically as an ``Enum``) and only has to satisfy these protocols.
"""

from typing import Protocol, TypeVar, runtime_checkable


@runtime_checkable
class ExtractionFieldType(Protocol):
    """Identity of one extractable field.

    ``name`` is used as the attribute key on the (deferred) extraction subject and is
    embedded in batch-request custom IDs, so it must be stable across runs.
    """

    @property
    def name(self) -> str: ...

    def __hash__(self) -> int: ...


@runtime_checkable
class ConceptFieldType(ExtractionFieldType, Protocol):
    """An extractable field whose vocabulary is grounded in an ontology subtree."""

    @property
    def base_uri(self) -> str: ...

    @property
    def recursive_grounding_placeholders(self) -> tuple[str, str]:
        """``(parent_stem, child_stem)`` substituted into the recursive grounding prompt."""
        ...


LLMExtractedFieldTypeVar = TypeVar(
    "LLMExtractedFieldTypeVar", bound=ExtractionFieldType
)

SingleStageFieldTypeVar = TypeVar("SingleStageFieldTypeVar", bound=ExtractionFieldType)

ConceptFieldTypeVar = TypeVar("ConceptFieldTypeVar", bound=ConceptFieldType)
