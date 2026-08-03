from __future__ import annotations

from pydantic import BaseModel, ConfigDict

# Stage 1
LLMSearchResults = set[str]
"""
Example:
[
    "Reciprocating Surface Grinders",
    "Rotary, Trunnion (Horizontal & Vertical) Transfer Machines",
    "CNC Lathes",
    "Arbor Presses"
]
"""


class PhraseSearchResponse(BaseModel):
    """Shared by both llm_search and llm_phrase_recursive_search (identical shape)."""

    model_config = ConfigDict(extra="forbid")

    phrases: list[str]
