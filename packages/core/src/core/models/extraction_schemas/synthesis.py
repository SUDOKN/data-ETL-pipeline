"""Wire contract of the v3 synthesis stage (PIPELINE_V3_PLAN.md D15, D16; the
location-stage merge of 2026-09-03 — D15's "text can be ADDED to its input
later" escape hatch, exercised).

A request carries the CHUNK'S TEXT (the 20k macro window every record's
mentions were folded from), then records — one per group the aggregation fold
built — as an opaque ``record_id`` plus the group's ``focal_form`` and its
``snippets``: bare verbatim strings from the fold, the record's distinct
passages in locked order (the ``{location, snippet}`` entry object died in two
steps — location with the location-stage merge, the one-key wrapper right
after it, both 2026-09-03; "entry" survives only in the stored metadata field
``max_entries_per_request``, whose name old run documents pin). The model
describes the focal entity using the snippets as evidence (D15 as amended
2026-08-22: the focal form is the one thing the model is told about the
group — the most frequent member form, chosen in code — and snippets not
about it are not evidence; the TEXT is not evidence either — it is there so
the model can see where each snippet sits); there are no dispositions and no
split flag (D15 as amended 2026-08-21 — the sense burden moved to screening,
D14).

The answer carries nothing but the synthesis. Where a snippet sits on its
page — the heading or table header row above it — is derived in CODE by the
aggregation fold (``aggregation_fold.locate_context``) and stored on the
mention as its ``location``. Until 2026-09-05 the model returned that per
snippet, as a verbatim quote (``snippet_contexts``, one per snippet by
index): a slot nothing downstream read, whose per-snippet enumeration invited
a repetition loop — one record with 65 snippets under one heading drew 4,850
copies of that heading and an answer cut off by the completion cap (run
20260904T184906). Dropped by user decision, together with the count-mismatch
retry class it needed.

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
Request: the chunk text, then the two-block pattern — ``<<<RECORD_IDS`` as a
bare array, then ``<<<RECORDS`` as an ARRAY of ``{record_id, focal_form,
snippets}`` (array, not map, by user decision; rendered by
``render_synthesis_record_blocks``). Response: an array mirroring the request,
``{"syntheses": [{record_id, doer, doer_name, capacity, dealing_words,
synthesis}]}`` — arrays because the response format is OpenAI strict mode,
which cannot express an object keyed by ids, and because an array lets the
hold COUNT exactly-once where an object would collapse a duplicate key
silently. The four LABELS (2026-09-13, the variance experiment — synthesis
design doc §31.3/§33) are decided BEFORE the paragraph: strict mode emits keys
in schema order, so the model commits to who the snippets give the doing to
(``doer``, four values) and to the manufacturer's own dealing in the capacity
the snippets fix (``capacity``, ten values), names the other party as the text
names it (``doer_name``) and copies the snippets' own words for the dealing
(``dealing_words``), and only then writes the synthesis. Measured motive:
gpt-4.1 at temperature 0 resolves exactly these two near-ties inside prose,
and lands a whole request in one reading or the other (three cap-50 draws of
the same subjects disagreed on 9.6% of records, only 8 of 1,449 failing in
every draw). The labels ride on the stored ``SynthesisAnswer`` and the dump;
the downstream ``GroupRecord`` does NOT carry them (grounding and screening
``model_dump()`` it into their requests — one change per run). A response the completion cap cut off is
SALVAGED down to its complete records (``_salvage_truncated_syntheses``, the
search stage's discipline): the record the cut fell in is simply missing, and
the node's retry pass re-asks it; a response that closed its JSON and still
fails the schema is not truncation and keeps raising.

WHAT HOLDS THE RESPONSE
-----------------------
``hold_response_to_sent_record_ids`` — exact on ids, unknown id raises
(fabrication), missing ids raise or thin per the caller's ``on_missing``; the
policy for a synthesis that answers a fraction of its records is the
USER-OWNED under-answer decision (v3 plan, standing watch items) and is the
node's to choose in Phase 3, not this module's.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Literal, Optional, get_args

from pydantic import BaseModel, ConfigDict, ValidationError

from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)

logger = logging.getLogger(__name__)


# --- request side -----------------------------------------------------------


class SynthesisRecordInput(BaseModel):
    """One record on the request wire: the opaque id the model echoes back, the
    focal form the record is about, and the verbatim snippets it synthesizes
    from — bare strings, the bundle's distinct passages in locked order.
    ``record_id`` carries the group_id value."""

    record_id: str
    focal_form: str
    snippets: list[str]

    def wire_dict(self) -> dict:
        """The record as the request renders it: ``record_id``, ``focal_form``,
        ``snippets``. Also what the request's ``|ud=`` digest reads."""
        return self.model_dump(exclude_none=True)


# --- response wire ----------------------------------------------------------


# The label vocabularies (2026-09-13). Generic on purpose — no field words —
# and spelled exactly as the statics' Output section lists them; the strict
# schema turns each Literal into an enum, so a value outside the list is a
# schema failure, never a stored answer.
Doer = Literal["the manufacturer", "another party", "nobody", "not shown"]
Capacity = Literal[
    "makes, performs, or provides it as its own",
    "works on it to another party's order or specification",
    "lists, carries, represents, resells, or distributes what another party makes or does",
    "services, tests, inspects, or installs it",
    "uses it as an input, tool, material, or machine",
    "supplies into or serves it",
    "holds, is certified to, or claims to meet it",
    "arranges for another party to perform it",
    "unstated",
    "none",
]
DOER_VALUES: tuple[str, ...] = get_args(Doer)
CAPACITY_VALUES: tuple[str, ...] = get_args(Capacity)


class SynthesisRecordResponse(BaseModel):
    """Wire shape of one record's answer, in the order the model writes it:
    the id echoed exactly as given, the four labels decided first (``doer``,
    ``doer_name``, ``capacity``, ``dealing_words`` — 2026-09-13), then the
    synthesis. Field order IS the schema's property order, which strict mode
    makes the generation order: keep the labels above ``synthesis``. See the
    module docstring for the per-snippet context quotes this carried until
    2026-09-05."""

    model_config = ConfigDict(extra="forbid")

    record_id: str
    doer: Doer
    doer_name: str
    capacity: Capacity
    dealing_words: str
    synthesis: str


class SynthesesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    syntheses: list[SynthesisRecordResponse]


SYNTHESIS_RESPONSE_SCHEMA = build_gpt_response_format(
    SynthesesResponse, name="phrase_synthesis"
)

# What a dummy (no records) synthesis request answers with.
DUMMY_SYNTHESIS_RESPONSE_CONTENT = '{"syntheses": []}'


# --- stored -----------------------------------------------------------------


class SynthesisAnswer(BaseModel):
    """One record's parsed answer: the synthesis text and, since 2026-09-13,
    the four labels the model decided before writing it. The labels are
    Optional here (not on the wire) so answers parsed from older responses and
    the harness's own fixtures still load; ``labels`` is None for those."""

    synthesis: str
    doer: Optional[str] = None
    doer_name: Optional[str] = None
    capacity: Optional[str] = None
    dealing_words: Optional[str] = None

    @property
    def labels(self) -> Optional[dict[str, str]]:
        """The four labels as one dict (the dump's ``labels`` row field), or
        None when the answer carries none."""
        if self.doer is None or self.capacity is None:
            return None
        return {
            "doer": self.doer,
            "doer_name": self.doer_name or "",
            "capacity": self.capacity,
            "dealing_words": self.dealing_words or "",
        }


# group_id → the record's answer: the stage's parse-time result, keyed by the
# value that rode the wire as ``record_id``.
SynthesesByGroupId = dict[str, SynthesisAnswer]


# The only shape worth salvaging: an object whose first key is `syntheses`,
# opening its array. Anything else that fails validation is a different failure.
_SYNTHESES_ARRAY_PREFIX = re.compile(r'\s*\{\s*"syntheses"\s*:\s*\[')
_RECORD_DECODER = json.JSONDecoder()


def _salvage_truncated_syntheses(
    gpt_response: str,
) -> Optional[list[SynthesisRecordResponse]]:
    """Recover the complete records of a ``{"syntheses": [...`` array the model
    left unterminated by running into its completion cap.

    Returns None unless the text is exactly that shape — the prefix, a run of
    complete record objects, then either the end of the text or an object cut
    mid-way. A response that closed its array failed validation for some other
    reason, and one holding non-record objects is not this schema's answer at
    all; neither is ours to repair, so both keep raising. Mirrors the search
    stage's ``_salvage_truncated_phrases`` (2026-08-16), which this stage
    lacked when a 20,000-token runaway cost a whole subject (2026-09-04).
    """
    prefix = _SYNTHESES_ARRAY_PREFIX.match(gpt_response)
    if not prefix:
        return None

    records: list[SynthesisRecordResponse] = []
    index = prefix.end()
    length = len(gpt_response)
    while True:
        while index < length and gpt_response[index] in ", \t\r\n":
            index += 1
        if index >= length:
            break  # ran out of text between records
        if gpt_response[index] != "{":
            return None  # a closing "]", or something that is not a record
        try:
            raw, index = _RECORD_DECODER.raw_decode(gpt_response, index)
        except ValueError:
            break  # the record itself was cut mid-way
        try:
            records.append(SynthesisRecordResponse.model_validate(raw))
        except ValidationError:
            return None  # a complete object that is not a record: not truncation
    return records or None


def parse_synthesis_response(gpt_response: Optional[str]) -> SynthesesByGroupId:
    """The wire's answered records as the group_id → answer map.

    Raises ``ValueError`` on an empty response or a response the strict schema
    would not have produced (unless it is a truncated one that salvages — see
    ``_salvage_truncated_syntheses``).

    An id answered more than once is DROPPED with every answer it got, logged,
    and left for the node's retry pass (2026-09-12): a repeated id means the
    model confused ids, so one of its paragraphs belongs to a sibling whose
    own id went unanswered, and there is no telling which. Dropping both
    makes the repeated id and the overwritten sibling ``missing`` together,
    and the retry re-asks exactly those. Raising here instead re-asked the
    whole request under the parse-error cap and, when the model repeated the
    defect, cost a subject two fields (run 20260911T223222).
    """
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError("parse_synthesis_response: Empty or invalid response from GPT")

    try:
        answered_records = SynthesesResponse.model_validate_json(gpt_response).syntheses
    except ValidationError as e:
        salvaged = _salvage_truncated_syntheses(gpt_response)
        if salvaged is None:
            raise ValueError(
                f"parse_synthesis_response: Invalid response from GPT:{gpt_response}"
            ) from e
        logger.error(
            f"parse_synthesis_response: truncated response ({len(gpt_response)} chars) "
            f"— salvaged {len(salvaged)} complete record(s); the cut record is left "
            f"unanswered for the retry pass"
        )
        answered_records = salvaged

    answers_by_id: dict[str, list[SynthesisRecordResponse]] = {}
    for answered in answered_records:
        answers_by_id.setdefault(answered.record_id, []).append(answered)
    repeated = [rid for rid, answers in answers_by_id.items() if len(answers) > 1]
    if repeated:
        logger.error(
            f"parse_synthesis_response: {len(repeated)} record id(s) answered more than "
            f"once — {repeated}; every answer for them is dropped and left for the "
            f"retry pass (the response answered {len(answered_records)} records under "
            f"{len(answers_by_id)} distinct ids)"
        )
    return {
        rid: SynthesisAnswer(
            synthesis=answers[0].synthesis,
            doer=answers[0].doer,
            doer_name=answers[0].doer_name,
            capacity=answers[0].capacity,
            dealing_words=answers[0].dealing_words,
        )
        for rid, answers in answers_by_id.items()
        if len(answers) == 1
    }


# --- downstream (D16) -------------------------------------------------------


class GroupRecord(BaseModel):
    """One per-group record as every stage downstream of synthesis consumes it
    (D16): the group's focal form and the synthesis written for it. The key it
    travels under is the ``group_id`` — already opaque (``hash(normalized
    key)``, D11), so the key-masking discipline the v2 ``record_id`` provided
    comes for free: the model never sees a candidate-looking label as the
    grouping key, while the record's own fields carry the wording grounding
    needs (user decision 2026-08-24: focal form + synthesis, no member forms,
    no entries — the synthesis IS the evidence digest)."""

    focal_form: str
    synthesis: str


# group_id → the group's downstream record.
GroupRecords = dict[str, GroupRecord]
