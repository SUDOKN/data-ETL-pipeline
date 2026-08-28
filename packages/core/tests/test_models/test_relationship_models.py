"""Phase 0.2 of pipeline v2 (PIPELINE_V2_PLAN.md): the relationship stage's v2
wire and stored types — records with mentions + synthesis, and the masked
(record_id-keyed) stored map."""

import pytest
from pydantic import ValidationError

from core.models.extraction_schemas.relationship import (
    MaskedLLMPhraseRelationshipResults,
    MaskedPhraseRelationshipRecord,
    PhraseMention,
    PhraseRecordsResponse,
    PhraseRelationshipRecord,
)
from core.models.extraction_schemas.response_format_util import (
    assert_strict_schema_supported,
    build_gpt_response_format,
)

FOUND_AND_NOT_FOUND = """
{"records": [
  {"phrase": "aerospace",
   "mentions": [
     {"form": "Aerospace",
      "page": "/industries",
      "account": "Heading: 'Aerospace' introduces a list of sectors [the manufacturer] serves."},
     {"form": "aero-space",
      "page": "unknown",
      "account": "Prose: 'our aero-space work' — the text began mid-page, no URL line visible."}
   ],
   "synthesis": "The manufacturer presents aerospace as a sector it serves."},
  {"phrase": "medical devices",
   "mentions": [],
   "synthesis": "The phrase was not found in the text."}
]}
"""


def test_wire_round_trip_including_not_found_branch():
    parsed = PhraseRecordsResponse.model_validate_json(FOUND_AND_NOT_FOUND)
    assert [entry.phrase for entry in parsed.records] == [
        "aerospace",
        "medical devices",
    ]
    first = parsed.records[0]
    assert [mention.form for mention in first.mentions] == ["Aerospace", "aero-space"]
    assert first.mentions[1].page == "unknown"
    # The honest not-found branch: mentions empty, synthesis still present.
    assert parsed.records[1].mentions == []
    assert parsed.records[1].synthesis


@pytest.mark.parametrize(
    "payload",
    [
        # Extra key at any level must be rejected (extra="forbid" everywhere).
        '{"records": [], "extra": 1}',
        '{"records": [{"phrase": "x", "mentions": [], "synthesis": "s", "note": ""}]}',
        '{"records": [{"phrase": "x", "mentions": [{"form": "f", "page": "/", "account": "a", "why": ""}], "synthesis": "s"}]}',
        # Every field is required — a mention without a page is malformed, not partial.
        '{"records": [{"phrase": "x", "mentions": [{"form": "f", "account": "a"}], "synthesis": "s"}]}',
        '{"records": [{"phrase": "x", "synthesis": "s"}]}',
    ],
)
def test_wire_rejects_extras_and_missing_fields(payload: str):
    with pytest.raises(ValidationError):
        PhraseRecordsResponse.model_validate_json(payload)


def test_wire_schema_is_strict_mode_supported():
    response_format = build_gpt_response_format(
        PhraseRecordsResponse, name="phrase_relationship_records"
    )
    assert_strict_schema_supported(
        response_format, where="relationship v2 wire schema"
    )


def test_masked_map_persists_the_id_to_phrase_join():
    record = PhraseRelationshipRecord(
        mentions=[PhraseMention(form="Aerospace", page="/", account="a")],
        synthesis="s",
    )
    masked: MaskedLLMPhraseRelationshipResults = {
        "r7k3f9x2": MaskedPhraseRelationshipRecord(phrase="aerospace", record=record)
    }
    entry = masked["r7k3f9x2"]
    # The phrase rides inside the entry: the join survives storage without any
    # side table, and the record itself is carried unaltered.
    assert entry.phrase == "aerospace"
    assert entry.record.mentions[0].form == "Aerospace"
