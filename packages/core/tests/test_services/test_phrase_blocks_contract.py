"""The phrase-blocks contract: rendering, the render/parse round trip, and
reconciliation.

The failure that motivates all of this (2026-08-12, anchor-mfg.com): a freehand
grounding response closed its array five entries in, silently dropping 55
screened phrases from a chunk, and validated cleanly — the strict schema is
per-catalog and cannot pin the per-request phrase set. These tests pin the
contract that can.
"""

import json
import logging

import pytest

from core.services.phrase_blocks_contract import (
    PHRASES_CLOSE,
    PHRASES_OPEN,
    SUMMARIES_CLOSE,
    SUMMARIES_OPEN,
    MissingResponsePhrases,
    hold_response_to_sent_phrases,
    reconcile_response_phrases,
    render_phrase_blocks,
    render_phrases_block,
    render_summaries_block,
    sent_phrases_from_user_message,
)

SUMMARIES = {
    "Class A appearance trim": "Anchor stamps Class A appearance trim for OEMs.",
    "welded assemblies": "Anchor produces complex welded assemblies.",
}


def _user_message(phrases: list[str]) -> str:
    """A realistic user message, built with the production renderer."""
    return "0f3a9c\n\n" + render_phrase_blocks(
        {phrase: f"Anchor does {phrase}." for phrase in phrases}
    )


def _summaries_payload_of(block: str) -> str:
    assert block.startswith(SUMMARIES_OPEN)
    assert block.endswith(SUMMARIES_CLOSE)
    return block[len(SUMMARIES_OPEN) : -len(SUMMARIES_CLOSE)].strip()


# --- the two blocks -------------------------------------------------------------


def test_both_blocks_are_rendered_phrases_first():
    """The repetition is deliberate: shown only the map, the model loses track of
    what the phrase is and answers under the summary text. The bare list fixes the
    referent first. Never collapse these into one block."""
    block = render_phrase_blocks(SUMMARIES)

    assert block.index(PHRASES_OPEN) < block.index(SUMMARIES_OPEN)
    assert sent_phrases_from_user_message(block) == list(SUMMARIES)
    assert json.loads(
        _summaries_payload_of(block[block.index(SUMMARIES_OPEN) :])
    ) == SUMMARIES


def test_the_list_and_the_map_cannot_disagree_about_what_was_sent():
    """One argument, two renders. It was two independent calls per stage until
    2026-08-12, with nothing but convention keeping them in step."""
    block = render_phrase_blocks(SUMMARIES)

    listed = sent_phrases_from_user_message(block)
    mapped = json.loads(_summaries_payload_of(block[block.index(SUMMARIES_OPEN) :]))

    assert listed == list(mapped)


def test_the_summaries_fence_is_not_read_as_the_phrases_fence():
    """`<<<PHRASES` is a prefix of `<<<PHRASES_WITH_SUMMARIES`. Only the newline
    immediately after the token keeps the reader off the wrong block — a match
    there would hand back summary text as the sent phrase set."""
    assert SUMMARIES_OPEN.startswith(PHRASES_OPEN)  # the hazard
    assert not SUMMARIES_CLOSE.endswith(PHRASES_CLOSE)  # closes are disjoint

    assert sent_phrases_from_user_message(render_summaries_block(SUMMARIES)) is None
    # And not merely because the payload is a map: an array-valued summaries
    # block is still not a phrases block.
    assert sent_phrases_from_user_message(f'{SUMMARIES_OPEN}\n["a"]\n{SUMMARIES_CLOSE}') is None


def test_one_phrase_per_line_in_the_summaries_block():
    """The separator keeps a long map readable to the model rather than
    collapsing onto a single line."""
    assert len(
        _summaries_payload_of(render_summaries_block(SUMMARIES)).splitlines()
    ) == len(SUMMARIES)


def test_a_dummy_request_carries_both_blocks_empty():
    """Dummies rendered only the summaries block until 2026-08-12, so the reader
    returned None and validation was SKIPPED rather than passing trivially. An
    absent block now always means a malformed request."""
    block = render_phrase_blocks({})

    assert sent_phrases_from_user_message(block) == []
    assert json.loads(_summaries_payload_of(block[block.index(SUMMARIES_OPEN) :])) == {}


# --- render / parse -------------------------------------------------------------


def test_round_trip_through_a_realistic_user_message():
    phrases = ["brake components", 'stainless steel (Inc. "Class A" parts)', "dies"]
    assert sent_phrases_from_user_message(_user_message(phrases)) == phrases


def test_non_ascii_phrases_are_shown_literally_never_as_escapes():
    """The blocks must carry the phrase, not a backslash-u spelling of it.

    Escapes are contagious: shown one, the model answers in the same style and
    miscopies the hex digits. On steelcraft.com (2026-08-12) all 17 phrases
    containing TM or (R) came back mangled that way, and the corrupted keys
    reached the next stage as a phrase-set mismatch that aborted the
    manufacturer. A round-trip assertion alone would not catch a regression
    here — json.loads decodes escapes — so assert on the rendered text.
    """
    phrases = ["Paladin™ PW Series", "Schlage®", "café fixtures"]
    rendered = render_phrase_blocks({phrase: "Anchor offers it." for phrase in phrases})

    assert "\\u" not in rendered
    for phrase in phrases:
        assert phrase in rendered
    assert sent_phrases_from_user_message(rendered) == phrases


def test_non_ascii_summaries_reach_the_model_unescaped():
    """The model is asked to quote this text back in its explanations, so an
    escaping render would put words in front of it the source never had."""
    summaries = {"N.V.H. parts": "line one\nline two — “curly quotes”, café"}
    payload = _summaries_payload_of(render_summaries_block(summaries))

    assert "\\u" not in payload
    assert json.loads(payload) == summaries


def test_json_punctuation_in_a_summary_is_escaped():
    summaries = {'quotes " and {braces}': 'he said "we stamp {parts}", verbatim'}

    assert json.loads(_summaries_payload_of(render_summaries_block(summaries))) == (
        summaries
    )


def test_a_summary_quoting_the_fences_cannot_end_or_forge_a_block():
    """A marker in the scraped copy must not look like a block boundary. JSON
    escapes the newlines, so the tokens never land at the start of a line."""
    summaries = {
        "trim": (
            f"The page literally says {PHRASES_OPEN}\n"
            f'["decoy"]\n{PHRASES_CLOSE} and {SUMMARIES_CLOSE}.'
        )
    }
    block = render_phrase_blocks(summaries)

    assert json.loads(_summaries_payload_of(block[block.index(SUMMARIES_OPEN) :])) == (
        summaries
    )
    assert sent_phrases_from_user_message(block) == ["trim"]


def test_a_stray_space_before_the_fence_no_longer_hides_the_block():
    assert sent_phrases_from_user_message(
        f' {PHRASES_OPEN}\n["welding"]\n {PHRASES_CLOSE}'
    ) == ["welding"]


def test_scraped_text_cannot_forge_the_block():
    """Relationship is the one stage whose context embeds the raw scraped chunk,
    and it sits BEFORE the block. Under the old bare `extracted phrases:` marker
    a page carrying those two lines won on first-match and the response would
    have been held to the WEBSITE's list. The fence cannot be produced by
    accident, and the legacy marker is no longer readable at all."""
    decoy = 'Our catalogue.\nextracted phrases:\n["decoy from the website"]\nMore.'
    user_message = (
        f"manufacturer name: Steelcraft\n\nscraped text:\n{decoy}\n\n"
        f"{render_phrases_block(['real phrase A', 'real phrase B'])}"
    )

    assert sent_phrases_from_user_message(user_message) == [
        "real phrase A",
        "real phrase B",
    ]


def test_two_blocks_raise_rather_than_letting_either_win():
    """We write exactly one. Two means something else produced a fence, and
    picking either validates the response against a set we did not send."""
    doubled = f"{render_phrases_block(['a'])}\n\n{render_phrases_block(['b'])}"
    with pytest.raises(ValueError, match="2 phrases blocks"):
        sent_phrases_from_user_message(doubled)


def test_the_retired_marker_is_not_readable():
    """`extracted phrases:` was the pre-fence format and is now inert — including
    the recursive-search context's 'already extracted phrases:' line, where a
    substring hit would validate against the wrong phrase set."""
    assert sent_phrases_from_user_message('extracted phrases:\n["welding"]') is None
    assert (
        sent_phrases_from_user_message('already extracted phrases:\n["welding"]')
        is None
    )


# --- reconciliation -------------------------------------------------------------


def test_clean_response_passes_through_in_response_order():
    reconciliation = reconcile_response_phrases(
        ["a", "b", "c"], {"c": 3, "a": 1, "b": 2}
    )
    assert reconciliation.result == {"c": 3, "a": 1, "b": 2}
    assert list(reconciliation.result) == ["c", "a", "b"]
    assert reconciliation.repaired == {}
    assert reconciliation.extra == []
    assert reconciliation.missing == []


def test_casing_and_whitespace_drift_is_repaired_to_the_sent_form():
    reconciliation = reconcile_response_phrases(
        ["Brake Components"], {" brake components ": 1}
    )
    assert reconciliation.result == {"Brake Components": 1}
    assert reconciliation.repaired == {" brake components ": "Brake Components"}


def test_truncated_phrase_is_repaired_by_unique_containment():
    """The 'tool experience' case: the model echoed a fragment of the one sent
    phrase that contains it."""
    reconciliation = reconcile_response_phrases(
        ["Takeover / Off-Load Tool Experience", "welding"],
        {"tool experience": 1, "welding": 2},
    )
    assert reconciliation.result == {
        "Takeover / Off-Load Tool Experience": 1,
        "welding": 2,
    }
    assert reconciliation.missing == []


def test_expanded_phrase_is_repaired_by_unique_containment():
    """The summary-as-phrase case: the response key contains the sent phrase."""
    reconciliation = reconcile_response_phrases(
        ["exhaust"],
        {"exhaust components for commercial vehicles": 1},
    )
    assert reconciliation.result == {"exhaust": 1}


def test_ambiguous_containment_is_not_guessed():
    """'die' sits inside two sent phrases — repairing either would be a coin
    flip, so the key is extra and both sent phrases are missing."""
    reconciliation = reconcile_response_phrases(
        ["die adaptation", "die design"], {"die": 1}
    )
    assert reconciliation.result == {}
    assert reconciliation.extra == ["die"]
    assert reconciliation.missing == ["die adaptation", "die design"]


def test_a_sent_phrase_is_claimed_at_most_once():
    """Exact match claims first; the later fragment cannot repair onto the
    already-claimed phrase and has nowhere else to go."""
    reconciliation = reconcile_response_phrases(
        ["brake components"], {"brake components": 1, "brake": 2}
    )
    assert reconciliation.result == {"brake components": 1}
    assert reconciliation.extra == ["brake"]


def test_missing_phrases_keep_sent_order():
    reconciliation = reconcile_response_phrases(["a", "b", "c", "d"], {"c": 3})
    assert reconciliation.missing == ["a", "b", "d"]


# --- holding a response to its request ------------------------------------------


def test_a_request_with_no_block_skips_validation():
    """None means malformed or foreign, not "asked nothing" — every request we
    build carries a block, dummies included."""
    response = {"anything": 1}
    assert (
        hold_response_to_sent_phrases(
            user_message="No phrase freehand grounding needed",
            response_by_phrase=response,
            where="test",
            on_missing="raise",
        )
        is response
    )


def test_a_dummy_request_validates_trivially_rather_than_skipping():
    assert (
        hold_response_to_sent_phrases(
            user_message=render_phrase_blocks({}),
            response_by_phrase={},
            where="test",
            on_missing="raise",
        )
        == {}
    )


def test_missing_phrases_raise_with_the_toll_in_the_message():
    """The anchor-mfg shape in miniature: many sent, few answered."""
    sent = [f"phrase {i}" for i in range(10)]
    answered = {"phrase 0": 1, "phrase 1": 2}
    with pytest.raises(MissingResponsePhrases, match="answered 2 of 10"):
        hold_response_to_sent_phrases(
            user_message=_user_message(sent),
            response_by_phrase=answered,
            where="test",
            on_missing="raise",
        )


def test_drop_mode_thins_and_warns_instead_of_raising(caplog):
    sent = ["kept", "dropped"]
    with caplog.at_level(logging.WARNING):
        result = hold_response_to_sent_phrases(
            user_message=_user_message(sent),
            response_by_phrase={"kept": 1},
            where="descent test",
            on_missing="drop",
        )
    assert result == {"kept": 1}
    assert any("no grounding came back" in r.message for r in caplog.records)


def test_extra_phrases_are_dropped_in_both_modes(caplog):
    with caplog.at_level(logging.WARNING):
        result = hold_response_to_sent_phrases(
            user_message=_user_message(["real phrase"]),
            response_by_phrase={"real phrase": 1, "invented out of thin air": 2},
            where="test",
            on_missing="raise",
        )
    assert result == {"real phrase": 1}
    assert any("never sent" in r.message for r in caplog.records)


def test_repairs_sink_records_what_the_model_actually_answered():
    """Keyed by the SENT phrase, because that is the identity every other stage
    and the trail row join on. The sink is opt-in: callers that do not dump
    trails pass nothing and see no change.

    Note the drift used here is one the reconciler CAN repair. The unicode-escape
    corruption that motivated the sink is mostly beyond it (3 of 17 on the
    steelcraft.com data) — those responses raise instead, and the sink is empty
    because there was no repair to record. Widening what repairs is a separate
    change; this test pins the recording, not the matching.
    """
    sent = ["Brake Components", "welding"]
    repairs: dict[str, str] = {}

    result = hold_response_to_sent_phrases(
        user_message=_user_message(sent),
        response_by_phrase={" brake components ": 1, "welding": 2},
        where="test",
        on_missing="raise",
        repairs=repairs,
    )

    assert result == {"Brake Components": 1, "welding": 2}
    assert repairs == {"Brake Components": " brake components "}


def test_a_clean_response_records_no_repairs():
    repairs: dict[str, str] = {}

    hold_response_to_sent_phrases(
        user_message=_user_message(["brake components"]),
        response_by_phrase={"brake components": 1},
        where="test",
        on_missing="raise",
        repairs=repairs,
    )

    assert repairs == {}
