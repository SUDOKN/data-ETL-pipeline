"""Wire contract of the v3 mention-collection stage — the LOCATION wire
(PIPELINE_V3_PLAN.md D4–D7, as amended 2026-08-22).

Mentions are collected by CODE (``core.utils.aggregation_fold.collect_window``):
every whole-word occurrence of every sent form in the window, casing-expanded,
clipped to the sentence or line that holds it. The one thing the LLM is asked
for is LOCATION — where each collected passage sits in the window, in its own
words — because that is the one thing that needs the container to be seen. (The
LLM collector that preceded this was measured to be a lossy Ctrl+F: the fold
already discarded any mention whose snippet did not hold the form verbatim, so
its accepted output was a subset of the scan, minus 17–19% it satisficed away.)

REQUEST. The window text, then two fenced blocks at the very bottom: the
mention ids as a bare array between ``<<<MENTION_IDS`` and ``MENTION_IDS>>>``,
then the mentions themselves between ``<<<MENTIONS`` and ``MENTIONS>>>`` as an
ARRAY of ``{mention_id, mention}`` — one entry per DISTINCT snippet of the
window (a line repeated fifty times is one entry). No forms ride the wire
(user decision): the role of a passage does not depend on which word in it we
care about. ``mention_id`` is content-derived (``mention_id_for_snippet``),
random-looking on purpose, like ``record_id``.

RESPONSE. ``{"mentions": [{mention_id, location}]}`` — an array because the
response format is OpenAI strict mode (no object keyed by ids), and because an
array lets the parse SEE a duplicate id where an object would swallow it.

WHAT HOLDS THE RESPONSE. ``hold_response_to_sent_mention_ids`` in
``phrase_blocks_contract`` — exact on ids, WARN-ONLY both ways: an id never
sent is dropped, a sent id with no answer is left absent and the fold gives
that mention a default location. Nothing raises: the LLM can no longer lose a
mention, only fail to colour it, and a raising hold would replay a
temperature-0 mis-echo to death.
"""

from __future__ import annotations

import logging
from typing import Optional

from pydantic import BaseModel, ConfigDict, ValidationError

from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)

logger = logging.getLogger(__name__)


# --- request side -------------------------------------------------------------


class MentionWireItem(BaseModel):
    """One entry of the request's ``<<<MENTIONS`` block: the id the model
    echoes back, and the verbatim passage it describes the location of."""

    mention_id: str
    mention: str


# --- response wire -----------------------------------------------------------


class MentionLocationEntry(BaseModel):
    """Wire shape of one answer: the id echoed exactly as given, then where that
    passage sits, in the reader's own words (its own wording and extent — no
    fixed form; user decision 2026-08-22)."""

    model_config = ConfigDict(extra="forbid")

    mention_id: str
    location: str


class MentionLocationsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mentions: list[MentionLocationEntry]


MENTION_COLLECTION_RESPONSE_SCHEMA = build_gpt_response_format(
    MentionLocationsResponse, name="phrase_mention_location"
)

# What a dummy (no mentions in this window) request answers with.
DUMMY_MENTION_COLLECTION_RESPONSE_CONTENT = '{"mentions": []}'


# --- stored -----------------------------------------------------------------

# mention_id → location: the stage's parse-time result per request, merged per
# window; the aggregation fold attaches each to every occurrence of the snippet.
LocationsByMentionId = dict[str, str]


def parse_mention_location_response(gpt_response: Optional[str]) -> LocationsByMentionId:
    """The wire's entries as the mention_id → location map.

    Raises ``ValueError`` on an empty response, a response the strict schema
    would not have produced, or an id answered twice — a duplicate is a
    contract breach the array shape lets us see, and the map must not pick one
    silently.
    """
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_mention_location_response: Empty or invalid response from GPT"
        )

    try:
        parsed = MentionLocationsResponse.model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_mention_location_response: Invalid response from GPT:{gpt_response}"
        ) from e

    by_id: LocationsByMentionId = {}
    for entry in parsed.mentions:
        if entry.mention_id in by_id:
            raise ValueError(
                f"parse_mention_location_response: Duplicate mention_id "
                f"{entry.mention_id!r} in mention-location response"
            )
        by_id[entry.mention_id] = entry.location
    return by_id
