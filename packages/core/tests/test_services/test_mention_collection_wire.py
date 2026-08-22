"""Phase 1.4 of pipeline v3 (PIPELINE_V3_PLAN.md): the mention-collection wire —
strict schema, parse, and the EXACT forms hold (D4–D7, D5/D6 as amended)."""

import json
import logging

import pytest

from core.models.extraction_schemas.mention_collection import (
    DUMMY_MENTION_COLLECTION_RESPONSE_CONTENT,
    MENTION_COLLECTION_RESPONSE_SCHEMA,
    Mention,
    parse_mention_collection_response,
)
from core.models.extraction_schemas.response_format_util import (
    assert_strict_schema_supported,
)
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_forms,
    render_phrases_block,
)

# The example block the static prompt shows the model, placeholders filled.
PROMPT_EXAMPLE = {
    "forms": [
        {
            "form": "Aluminum",
            "mentions": [
                {"location": "/materials page, under the 'Alloys' heading",
                 "snippet": "We machine Aluminum 6061-T6 and 7075 to tight tolerances."},
                {"location": "site-wide footer menu",
                 "snippet": "Aluminum | Brass | Steel"},
            ],
        },
        {"form": "aluminum", "mentions": [
            {"location": "homepage, hero paragraph",
             "snippet": "From aluminum prototypes to production runs."}]},
        {"form": "Lead Time", "mentions": []},
    ]
}


def _message(forms: list[str]) -> str:
    return f"the text\n\nscraped text...\n\n{render_phrases_block(forms)}"


def test_schema_is_one_strict_mode_accepts():
    assert_strict_schema_supported(
        MENTION_COLLECTION_RESPONSE_SCHEMA, where="phrase_mention_collection"
    )


def test_prompt_example_decodes_and_parses_in_response_order():
    by_form = parse_mention_collection_response(json.dumps(PROMPT_EXAMPLE))
    assert list(by_form) == ["Aluminum", "aluminum", "Lead Time"]
    assert by_form["Aluminum"][1] == Mention(
        location="site-wide footer menu", snippet="Aluminum | Brass | Steel"
    )
    # Case variants are DISTINCT forms on this wire (D5).
    assert by_form["aluminum"] != by_form["Aluminum"]
    # An empty mentions array is a legal answer; the floor scan judges it.
    assert by_form["Lead Time"] == []


def test_dummy_content_parses_to_nothing():
    assert parse_mention_collection_response(DUMMY_MENTION_COLLECTION_RESPONSE_CONTENT) == {}


@pytest.mark.parametrize("bad", [None, ""])
def test_empty_response_raises(bad):
    with pytest.raises(ValueError, match="Empty or invalid"):
        parse_mention_collection_response(bad)


def test_whitespace_only_response_is_invalid_not_empty():
    # Truthy but not JSON: the schema branch, same as the relationship parse.
    with pytest.raises(ValueError, match="Invalid response"):
        parse_mention_collection_response("   ")


def test_duplicate_form_raises_rather_than_picking_one():
    dup = {"forms": [
        {"form": "Brass", "mentions": []},
        {"form": "Brass", "mentions": [{"location": "x", "snippet": "Brass fittings"}]},
    ]}
    with pytest.raises(ValueError, match="Duplicate form 'Brass'"):
        parse_mention_collection_response(json.dumps(dup))


def test_extra_keys_and_missing_fields_are_rejected():
    # The old object-keyed shape must NOT validate: the wire is the array.
    with pytest.raises(ValueError, match="Invalid response"):
        parse_mention_collection_response(json.dumps({"mentions": {"Brass": []}}))
    # A mention missing its snippet is not a mention.
    with pytest.raises(ValueError, match="Invalid response"):
        parse_mention_collection_response(json.dumps(
            {"forms": [{"form": "Brass", "mentions": [{"location": "x"}]}]}))
    # Extra keys are forbidden at every level (the form-echo diagnostic arm,
    # if it ever ships, is a schema change, not a tolerated extra).
    with pytest.raises(ValueError, match="Invalid response"):
        parse_mention_collection_response(json.dumps(
            {"forms": [{"form": "Brass", "mentions": [], "page": "/"}]}))


def test_exact_hold_keeps_case_variants_distinct_and_preserves_sent_order(caplog):
    """The v1 reconciler casefolds; on v3's payload that would fuse `Aluminum`
    and `aluminum`. The forms hold is exact, so both survive, in sent order."""
    sent = ["aluminum", "Lead Time", "Aluminum"]
    response = {"Aluminum": ["a"], "aluminum": ["b"], "Lead Time": []}
    with caplog.at_level(logging.WARNING):
        held = hold_response_to_sent_forms(
            user_message=_message(sent), response_by_form=response, where="t"
        )
    assert list(held) == sent
    assert held["Aluminum"] == ["a"] and held["aluminum"] == ["b"]
    assert not caplog.records


def test_exact_hold_drops_unknown_keys_and_leaves_missing_absent(caplog):
    sent = ["Aluminum", "Brass"]
    # `aluminum` was NOT sent: a mis-echo of casing. Exact hold drops it — no
    # repair onto `Aluminum` — and `Brass` was never answered.
    response = {"aluminum": ["x"], "Lead": ["y"]}
    with caplog.at_level(logging.WARNING):
        held = hold_response_to_sent_forms(
            user_message=_message(sent), response_by_form=response, where="t"
        )
    assert held == {}
    messages = [r.getMessage() for r in caplog.records]
    assert any("dropping 2 response form(s)" in m for m in messages)
    assert any("nothing came back for ['Aluminum', 'Brass']" in m for m in messages)
    # Missing stays ABSENT (never-answered), never filled with [] (answered-none).
    assert "Brass" not in held


def test_hold_is_a_no_op_without_a_phrases_block():
    response = {"anything": []}
    assert hold_response_to_sent_forms(
        user_message="no block here", response_by_form=response, where="t"
    ) is response
