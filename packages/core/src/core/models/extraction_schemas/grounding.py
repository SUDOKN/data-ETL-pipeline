from __future__ import annotations

from pydantic import BaseModel, ConfigDict

# Stage 4
TagAndReasonMap = dict[str, str]
PhraseToTagAndReasonMap = dict[
    str, TagAndReasonMap  # { phrase -> {tag: reason}, ...}
]  # The tags phrases can be linked to, with the reason for the link.


class PhraseGroundingTag(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tag: str
    reason: str


class PhraseGroundingEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    phrase: str
    tags: list[PhraseGroundingTag]


class PhraseGroundingResponse(BaseModel):
    """Shared wire schema for freehand/initial/recursive grounding (identical shape)."""

    model_config = ConfigDict(extra="forbid")

    groundings: list[PhraseGroundingEntry]
