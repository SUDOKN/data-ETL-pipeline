"""Phase 2.1 of pipeline v2 (PIPELINE_V2_PLAN.md): the v2 relationship parse —
records in, held to the sent phrases, masked out — and the F9 evidence filter."""

import json

import pytest

from core.services.phrase_blocks_contract import (
    MissingResponsePhrases,
    hold_response_to_sent_phrases,
    render_phrases_block,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service_v2 import (
    DUMMY_RECORDS_RESPONSE_CONTENT,
    LLM_PHRASE_RELATIONSHIP_RESPONSE_SCHEMA_V2,
    parse_llm_phrase_relationship_records,
    records_with_mentions,
)
from core.models.extraction_schemas.response_format_util import (
    assert_strict_schema_supported,
)
from core.utils.record_id_util import mask_relationship_records, record_id_for_phrase


def _response(records: list[dict]) -> str:
    return json.dumps({"records": records})


FOUND = {
    "phrase": "aerospace",
    "mentions": [
        {"form": "Aerospace", "page": "/industries", "account": "Heading: 'Aerospace'."},
        {"form": "aero-space", "page": "unknown", "account": "Prose mention."},
    ],
    "synthesis": "The manufacturer serves aerospace.",
}
NOT_FOUND = {
    "phrase": "medical devices",
    "mentions": [],
    "synthesis": "The phrase was not found in the text.",
}


def test_parse_converts_wire_records_to_the_stored_map():
    parsed = parse_llm_phrase_relationship_records(_response([FOUND, NOT_FOUND]))
    assert set(parsed) == {"aerospace", "medical devices"}
    record = parsed["aerospace"]
    assert [m.form for m in record.mentions] == ["Aerospace", "aero-space"]
    assert record.mentions[1].page == "unknown"
    assert record.synthesis == "The manufacturer serves aerospace."
    assert parsed["medical devices"].mentions == []


def test_duplicate_phrases_and_malformed_responses_raise():
    with pytest.raises(ValueError, match="Duplicate phrase"):
        parse_llm_phrase_relationship_records(_response([FOUND, FOUND]))
    with pytest.raises(ValueError, match="Invalid response"):
        parse_llm_phrase_relationship_records('{"relationships": []}')
    with pytest.raises(ValueError, match="Empty or invalid"):
        parse_llm_phrase_relationship_records(None)


def test_the_dummy_content_parses_to_an_empty_map():
    assert parse_llm_phrase_relationship_records(DUMMY_RECORDS_RESPONSE_CONTENT) == {}


def test_the_v2_response_schema_is_strict_mode_supported():
    assert_strict_schema_supported(
        LLM_PHRASE_RELATIONSHIP_RESPONSE_SCHEMA_V2, where="relationship v2"
    )


def test_the_parse_result_holds_to_the_sent_phrases_axis():
    """Relationship keeps the phrase fence — identity is set here, masking comes
    after the hold. A production-shaped context exercises the same reader the
    node uses."""
    user_message = (
        "the name of the manufacturer in question: X\n\n"
        "scraped text from their website:\nAerospace things.\n\n"
        f"{render_phrases_block(['aerospace', 'medical devices'])}"
    )
    parsed = parse_llm_phrase_relationship_records(_response([FOUND, NOT_FOUND]))
    held = hold_response_to_sent_phrases(
        user_message=user_message,
        response_by_phrase=parsed,
        where="test",
        on_missing="raise",
    )
    assert set(held) == {"aerospace", "medical devices"}

    with pytest.raises(MissingResponsePhrases):
        hold_response_to_sent_phrases(
            user_message=user_message,
            response_by_phrase=parse_llm_phrase_relationship_records(
                _response([FOUND])
            ),
            where="test",
            on_missing="raise",
        )


def test_masking_then_filtering_drops_only_the_evidence_free_records():
    parsed = parse_llm_phrase_relationship_records(_response([FOUND, NOT_FOUND]))
    masked = mask_relationship_records(parsed)
    assert set(masked) == {
        record_id_for_phrase("aerospace"),
        record_id_for_phrase("medical devices"),
    }

    with_evidence = records_with_mentions(masked)
    assert set(with_evidence) == {record_id_for_phrase("aerospace")}
    # The stored map is untouched — the filter shapes downstream input only.
    assert record_id_for_phrase("medical devices") in masked
