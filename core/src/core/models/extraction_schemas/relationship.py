from __future__ import annotations

from pydantic import BaseModel, ConfigDict

# Stage 2
LLMPhraseRelationshipResults = dict[
    str, str
]  # Describe relationship between phrases extracted and the subject, the manufacturer.


class PhraseRelationshipEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    phrase: str
    description: str


class PhraseRelationshipResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    relationships: list[PhraseRelationshipEntry]
