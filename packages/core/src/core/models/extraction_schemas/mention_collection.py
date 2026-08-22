"""Wire contract of the v3 mention-collection stage (PIPELINE_V3_PLAN.md D4–D7).

The collector is handed a window of scraped text and the surface forms search
found in that same window (a bare ``<<<PHRASES`` block — no ids: the form is
the natural working object, and an id→form map is indirection with nothing to
buy at a stage that makes no verdict; D5/D6 as amended 2026-08-21). For each
form it reports every char-for-char, whole-word occurrence as a mention:
``{location, snippet}`` — a freehand location in the reader's words and the
verbatim passage. Sense-agnostic by contract: the snippet carries the sense,
the synthesis stage downstream weighs it.

WHY AN ARRAY AND NOT AN OBJECT KEYED BY FORM
--------------------------------------------
The prompt's natural output is ``{"<form>": [mentions]}``, but the response
format is OpenAI strict mode (``response_format_util``): every object must
carry ``additionalProperties: false``, so an object whose keys are the sent
forms is not expressible. The wire therefore carries an array of
``{form, mentions}`` entries — the same echo of the form, as a field instead of
a key — exactly how v2's record-keyed stages carry ``record_id``. The parse
rebuilds the keyed map and raises on a duplicate form, which an object would
have swallowed silently (last key wins).

WHAT HOLDS THE RESPONSE
-----------------------
``hold_response_to_sent_forms`` in ``phrase_blocks_contract`` — EXACT, no
reconciler. The v1 phrase reconciler casefolds and repairs by containment,
and v3 deliberately sends case variants as distinct forms (``Aluminum`` and
``aluminum`` ride separately, D5), so that reconciler would fuse them. A
response key that is not exactly a sent form is dropped and warned; the
mechanical floor scan (D7) is what turns the resulting hole into a visible
discrepancy. Nothing raises: at temperature 0 a deterministic mis-echo would
replay to death under a raising hold.

Stored identity is the form string itself; ``record_id_for_phrase(form)`` is
minted code-side at the aggregation fold as mention provenance (D11) and never
rides this wire.
"""

from __future__ import annotations

import logging
from typing import Optional

from pydantic import BaseModel, ConfigDict, ValidationError

from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)

logger = logging.getLogger(__name__)


# --- wire -------------------------------------------------------------------


class MentionEntry(BaseModel):
    """Wire shape of one occurrence of a form in the window."""

    model_config = ConfigDict(extra="forbid")

    # Where the occurrence sits, in the reader's own words (page by path or
    # heading + the governing heading/section, by stated preference). Advisory
    # colour only: the authoritative page is derived in code from the snippet's
    # position in the window text (D6 as amended).
    location: str
    # The passage holding the occurrence, copied character for character: the
    # whole sentence, or the whole line for non-sentence text. Verbatim is the
    # contract — the fold re-attributes every mention from this string (D8) and
    # the mention-level GT check is "snippet occurs in the window" (D18).
    snippet: str


class FormMentionsEntry(BaseModel):
    """Wire shape of one form's answer: the form echoed exactly as it was given,
    then its mentions in text order. Empty ``mentions`` is a legal answer (the
    form was sent but does not occur); the floor scan decides whether that is
    honest."""

    model_config = ConfigDict(extra="forbid")

    form: str
    mentions: list[MentionEntry]


class FormMentionsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    forms: list[FormMentionsEntry]


MENTION_COLLECTION_RESPONSE_SCHEMA = build_gpt_response_format(
    FormMentionsResponse, name="phrase_mention_collection"
)

# What a dummy (no forms in this window) mention-collection request answers with.
DUMMY_MENTION_COLLECTION_RESPONSE_CONTENT = '{"forms": []}'


# --- stored -----------------------------------------------------------------


class Mention(BaseModel):
    """Stored form of one occurrence."""

    location: str
    snippet: str


# form → its mentions in this window, in response order. The stage's parse-time
# result; the aggregation fold (Phase 2.3) re-keys from here.
MentionsByForm = dict[str, list[Mention]]


def parse_mention_collection_response(gpt_response: Optional[str]) -> MentionsByForm:
    """The wire's form entries as the form → mentions map.

    Raises ``ValueError`` on an empty response, a response the strict schema
    would not have produced, or a form answered twice — a duplicate is a
    contract breach the array shape lets us see, and the map must not pick one
    silently.
    """
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_mention_collection_response: Empty or invalid response from GPT"
        )

    try:
        parsed = FormMentionsResponse.model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_mention_collection_response: Invalid response from GPT:{gpt_response}"
        ) from e

    by_form: MentionsByForm = {}
    for entry in parsed.forms:
        if entry.form in by_form:
            raise ValueError(
                f"parse_mention_collection_response: Duplicate form "
                f"{entry.form!r} in mention-collection response"
            )
        by_form[entry.form] = [
            Mention(location=m.location, snippet=m.snippet) for m in entry.mentions
        ]
    return by_form
