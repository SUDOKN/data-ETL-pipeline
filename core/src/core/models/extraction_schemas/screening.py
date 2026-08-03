from __future__ import annotations

from pydantic import BaseModel, ConfigDict

# Stage 3
LLMScreeningResults = dict[str, str]  # To only keep the relationships we care about


class ScreeningVerdict(BaseModel):
    """Structured verdict from live GPT phrase_relationship_screening (strict json_schema
    output). Distinct from LLMScreeningResults, which is the legacy Yes—/No— string
    convention still used by human-corrected ground-truth data."""

    passed: bool
    reason: str


LiveScreeningResults = dict[str, ScreeningVerdict]  # phrase -> verdict


class PhraseScreeningEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    phrase: str
    passed: bool
    reason: str


class PhraseRelationshipScreeningResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    screenings: list[PhraseScreeningEntry]
