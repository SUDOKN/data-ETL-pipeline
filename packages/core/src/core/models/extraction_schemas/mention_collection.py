"""The mention-collection record (PIPELINE_V3_PLAN.md D4–D7; LLM wire retired
2026-09-03 with the location-stage merge).

Mentions are collected by CODE (``core.utils.aggregation_fold.collect_window``):
every whole-word occurrence of every sent form in the window, casing-expanded,
clipped to the sentence or line that holds it, one item per DISTINCT snippet.
``mention_id`` is content-derived (``mention_id_for_snippet``), random-looking
on purpose, like ``record_id``.

The location wire that used to live here — one LLM request per (window, mention
group) asking where each passage sits — was retired when the synthesis stage
absorbed the location task: synthesis now sees the chunk's text and returns a
verbatim context line per entry (``core.models.extraction_schemas.synthesis``).
What remains is the record the fold and the dumps share.
"""

from __future__ import annotations

from pydantic import BaseModel


class MentionWireItem(BaseModel):
    """One distinct snippet of a window: its content-derived id and the
    verbatim passage. The fold's per-window collection unit."""

    mention_id: str
    mention: str
