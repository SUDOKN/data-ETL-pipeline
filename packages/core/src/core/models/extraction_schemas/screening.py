from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict

from core.models.extraction_schemas.applied_rule import AppliedRule

# Stage 3
LLMScreeningResults = dict[str, str]  # To only keep the relationships we care about


class ScreeningVerdict(BaseModel):
    """Structured verdict from live GPT phrase_relationship_screening (strict json_schema
    output). Distinct from LLMScreeningResults, which is the legacy Yes—/No— string
    convention still used by human-corrected ground-truth data.

    ``passed`` is DERIVED, not reported: the model returns only its rules, and
    ``passed_implied_by`` reads the verdict off them at parse time. It is stored
    because every downstream reader wants the answer rather than the derivation.

    ``applied_rules`` is the model's own report, checked against the catalog that
    asked for it but otherwise stored as returned."""

    passed: bool
    identified_entity: Optional[str]
    applied_rules: list[AppliedRule]


LiveScreeningResults = dict[str, ScreeningVerdict]  # phrase -> verdict


class PhraseScreeningEntry(BaseModel):
    """What the model returns. No ``passed`` field: the rules ARE the decision
    procedure, so asking for a separate boolean only created a second channel that
    could contradict the first — and a contradiction failed the whole group request.
    The parser derives it instead (``passed_implied_by``)."""

    model_config = ConfigDict(extra="forbid")

    phrase: str
    # Nullable but required: a phrase that identified nothing still reports why.
    # Declared Optional with no default so it stays in the schema's `required`,
    # which OpenAI strict mode demands.
    identified_entity: Optional[str]
    applied_rules: list[AppliedRule]


class PhraseRelationshipScreeningResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    screenings: list[PhraseScreeningEntry]
