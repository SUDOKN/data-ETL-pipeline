"""Wire contract of the v3 synthesis stage (PIPELINE_V3_PLAN.md D15, D16).

Synthesis is deposition-only: it never sees source text. A request carries
records — one per group the aggregation fold built — as an opaque ``record_id``
plus the group's ``focal_form`` and its ``entries``, each entry a
``{location, snippet}`` pair passed through unchanged from mention collection
(or ``{snippet}`` alone on the no-location A/B arm). The model describes the
focal entity using the entries as evidence (D15 as amended 2026-08-22: the
focal form is the one thing the model is told about the group — the most
frequent member form, chosen in code — and entries not about it are not
evidence); there are no dispositions and no split flag (D15 as amended
2026-08-21 — the sense burden moved to screening, D14).

NAMING
------
The wire label is ``record_id`` because that is the word the model is given;
its VALUE is the group's ``group_id = hash(normalized key)`` (D11), and code,
dumps and ground truth keep calling it ``group_id``. The per-form
``record_id = hash(form)`` that v2 minted, and that v3 keeps as the mention-GT
anchor, is a different key — do not conflate them (D16 naming note). This
module types the request side so that the renderer's callers spell the wire
the same way every time.

REQUEST AND RESPONSE SHAPES
---------------------------
Request: the two-block pattern — ``<<<RECORD_IDS`` as a bare array, then
``<<<RECORDS`` as an ARRAY of ``{record_id, entries}`` (array, not map, by user
decision; rendered by ``render_synthesis_record_blocks``). Response: an array
mirroring the request, ``{"syntheses": [{record_id, synthesis}]}`` — arrays
because the response format is OpenAI strict mode, which cannot express an
object keyed by ids, and because an array lets the hold COUNT exactly-once
where an object would collapse a duplicate key silently.

WHAT HOLDS THE RESPONSE
-----------------------
``hold_response_to_sent_record_ids`` — exact on ids, unknown id raises
(fabrication), missing ids raise or thin per the caller's ``on_missing``; the
policy for a synthesis that answers a fraction of its records is the
USER-OWNED under-answer decision (v3 plan, standing watch items) and is the
node's to choose in Phase 3, not this module's.
"""

from __future__ import annotations

import logging
from typing import Optional

from pydantic import BaseModel, ConfigDict, ValidationError

from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)

logger = logging.getLogger(__name__)


# --- request side -----------------------------------------------------------


class SynthesisEntry(BaseModel):
    """One entry of a record on the request wire: a mention's location and
    verbatim snippet, passed through from mention collection unchanged (same
    field names on purpose — one name per concept across stages). ``location``
    is None on the no-location A/B arm (user decision 2026-08-22) and is then
    LEFT OFF the wire — render entries with ``model_dump(exclude_none=True)``
    (``wire_dict`` below) so the model sees ``{snippet}`` and not a null."""

    location: Optional[str] = None
    snippet: str


class SynthesisRecordInput(BaseModel):
    """One record on the request wire: the opaque id the model echoes back, the
    focal form the record is about, and the entries it synthesizes from.
    ``record_id`` carries the group_id value."""

    record_id: str
    focal_form: str
    entries: list[SynthesisEntry]

    def wire_dict(self) -> dict:
        """The record as the request renders it: ``record_id``, ``focal_form``,
        ``entries`` — a missing location is absent, never ``null``. Also what
        the request's ``|ud=`` digest reads."""
        return self.model_dump(exclude_none=True)


# --- response wire ----------------------------------------------------------


class SynthesisEntryResponse(BaseModel):
    """Wire shape of one record's answer: the id echoed exactly as given, then
    its synthesis."""

    model_config = ConfigDict(extra="forbid")

    record_id: str
    synthesis: str


class SynthesesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    syntheses: list[SynthesisEntryResponse]


SYNTHESIS_RESPONSE_SCHEMA = build_gpt_response_format(
    SynthesesResponse, name="phrase_synthesis"
)

# What a dummy (no records) synthesis request answers with.
DUMMY_SYNTHESIS_RESPONSE_CONTENT = '{"syntheses": []}'


# --- stored -----------------------------------------------------------------

# group_id → synthesis text: the stage's parse-time result, keyed by the value
# that rode the wire as ``record_id``.
SynthesesByGroupId = dict[str, str]


def parse_synthesis_response(gpt_response: Optional[str]) -> SynthesesByGroupId:
    """The wire's synthesis entries as the group_id → synthesis map.

    Raises ``ValueError`` on an empty response, a response the strict schema
    would not have produced, or an id answered twice.
    """
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError("parse_synthesis_response: Empty or invalid response from GPT")

    try:
        parsed = SynthesesResponse.model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_synthesis_response: Invalid response from GPT:{gpt_response}"
        ) from e

    by_id: SynthesesByGroupId = {}
    for entry in parsed.syntheses:
        if entry.record_id in by_id:
            raise ValueError(
                f"parse_synthesis_response: Duplicate record_id "
                f"{entry.record_id!r} in synthesis response"
            )
        by_id[entry.record_id] = entry.synthesis
    return by_id
