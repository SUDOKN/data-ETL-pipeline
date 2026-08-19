"""Reading a search / recursive-search response that the model got wrong.

The case these are written from: a `material_caps` recursive round on
alecmfg.com (2026-08-16) answered 51 real phrases and then repeated
"precision CNC machining for precision robotics" 2,453 times until it hit the
20,000-token completion cap and truncated mid-string. Every phrase it had
actually found was in the valid prefix.
"""

import json
import logging

import pytest

from core.services.pipeline_nodes.multi_stage.llm_phrase_search_node_service import (
    SearchResponseParseError,
    parse_error_record,
    parse_llm_search_response,
)


def _truncate_at_completion_cap(phrases: list[str], cap_chars: int) -> str:
    """The response as the API returns it when the model runs out of tokens:
    valid JSON up to a point, then nothing — no closing quote, no `]`, no `}`."""
    return json.dumps({"phrases": phrases})[:cap_chars]


class TestWellFormedResponses:
    def test_returns_the_unique_phrases(self):
        response = json.dumps({"phrases": ["6061-T6 aluminum", "bar stock"]})

        assert parse_llm_search_response(response) == {"6061-T6 aluminum", "bar stock"}

    def test_empty_array_is_an_empty_result_not_an_error(self):
        # The recursive prompt sanctions this: "It is normal for nothing to
        # remain ... an empty array is the expected result in that case."
        assert parse_llm_search_response(json.dumps({"phrases": []})) == set()

    def test_duplicates_collapse(self):
        response = json.dumps({"phrases": ["bar stock", "bar stock"]})

        assert parse_llm_search_response(response) == {"bar stock"}

    @pytest.mark.parametrize("response", [None, ""])
    def test_no_response_at_all_raises(self, response):
        with pytest.raises(SearchResponseParseError):
            parse_llm_search_response(response)


class TestTruncatedResponses:
    def test_salvages_the_complete_elements(self):
        full = _truncate_at_completion_cap(["one", "two", "three"], cap_chars=1000)
        truncated = full[: full.index('"three"') + len('"thr')]

        assert parse_llm_search_response(truncated) == {"one", "two"}

    def test_salvages_the_runaway_round(self):
        # The shape of the real failure: a real prefix, then one phrase over and
        # over, cut mid-string by the cap.
        real = [f"phrase {i}" for i in range(51)]
        runaway = real + ["precision CNC machining for precision robotics"] * 2453
        response = _truncate_at_completion_cap(runaway, cap_chars=20_000)

        parsed = parse_llm_search_response(response)

        assert set(real) <= parsed
        # The repeated phrase survives as one member, like any other duplicate.
        assert len(parsed) == len(real) + 1

    def test_element_cut_exactly_after_its_closing_quote_still_salvages(self):
        full = _truncate_at_completion_cap(["one", "two"], cap_chars=1000)
        truncated = full[: full.index('"two"') + len('"two"')]

        assert parse_llm_search_response(truncated) == {"one", "two"}

    def test_truncated_before_the_first_element_raises(self):
        # Nothing was salvaged, so there is nothing to report as a round.
        with pytest.raises(SearchResponseParseError):
            parse_llm_search_response('{"phrases": [')

    def test_escapes_survive_salvage(self):
        full = _truncate_at_completion_cap(['1/2" bar stock', "next"], cap_chars=1000)
        truncated = full[: full.index('"next"') + len('"ne')]

        assert parse_llm_search_response(truncated) == {'1/2" bar stock'}


class TestUnsalvageableResponses:
    @pytest.mark.parametrize(
        "response",
        [
            "I could not find any phrases in this text.",
            '{"phrase": ["singular key"]}',
            '{"phrases": [{"phrase": "an object"}',
            '{"phrases": ["a", "b"], "notes": "extra key"}',
            '{"phrases": "not an array"}',
        ],
    )
    def test_raises_rather_than_guessing(self, response):
        with pytest.raises(SearchResponseParseError):
            parse_llm_search_response(response)

    def test_error_carries_the_whole_response_for_the_record_but_not_the_message(self):
        response = "not json at all " * 500

        with pytest.raises(SearchResponseParseError) as raised:
            parse_llm_search_response(response, where=" for req-1")

        error = raised.value
        assert response not in str(error)
        assert "req-1" in str(error)
        assert str(len(response)) in str(error)
        assert response in parse_error_record(error)

    def test_parse_error_record_falls_back_to_the_message(self):
        assert parse_error_record(ValueError("something else")) == "something else"


class TestRepetitionReport:
    def test_reports_a_repetitive_response(self, caplog):
        phrases = ["one", "two"] + ["looping phrase"] * 100

        with caplog.at_level(logging.WARNING):
            parse_llm_search_response(json.dumps({"phrases": phrases}))

        assert "looping phrase" in caplog.text
        assert "102 phrases, 3 unique" in caplog.text

    def test_quiet_on_an_ordinary_over_long_answer(self, caplog):
        # 422 elements / 302 unique — the process_caps round on the same
        # sub-window as the runaway. Verbose, but not a loop.
        phrases = [f"phrase {i}" for i in range(302)] + [
            f"phrase {i}" for i in range(120)
        ]

        with caplog.at_level(logging.WARNING):
            parse_llm_search_response(json.dumps({"phrases": phrases}))

        assert caplog.text == ""
