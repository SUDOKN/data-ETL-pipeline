"""Phase 3.1 of pipeline v3 (PIPELINE_V3_PLAN.md D4–D7, D8/D9/D19 at the fold):
the mention-collection node service — window-local forms (search ∪ recursive ∪
brute casings), form groups, the request context and its exact hold, the parse,
and the per-chunk aggregation fold with inherited pages."""

import json
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest

from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
)
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
)
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_forms,
    sent_phrases_from_user_message,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_mention_collection_node_service import (
    brute_casings_in_window,
    fold_verb_fold_of,
    get_chunk_fold,
    get_window_forms,
    get_window_mentions,
    parse_mention_group_result,
    render_mention_collection_context,
    split_into_form_groups,
    window_bounds,
    window_text_of,
)
from core.utils.aggregation_fold import BUNDLE_STATUS_NO_MENTIONS, BUNDLE_STATUS_OK
from core.utils.floor_scan import preceding_page_of

SEP = "#" * 50
SUBJECT = "acme.example"
T0 = datetime(2026, 8, 21, 12, 0, 0)


class _Field:
    name = "material_caps"


def _request(result_json: str, user_message: str = "") -> Any:
    """Only what the service reads off a completed GPTBatchRequest."""
    return SimpleNamespace(
        response=SimpleNamespace(result=result_json),
        request=SimpleNamespace(body=SimpleNamespace(user_message=lambda: user_message)),
    )


def _search(phrases: list[str]) -> Any:
    return _request(json.dumps({"phrases": phrases}))


def _mention_response(forms: dict[str, list[tuple[str, str]]]) -> str:
    return json.dumps(
        {
            "forms": [
                {
                    "form": form,
                    "mentions": [{"location": loc, "snippet": snip} for loc, snip in ms],
                }
                for form, ms in forms.items()
            ]
        }
    )


# ---------------------------------------------------------------------------
# Forms
# ---------------------------------------------------------------------------


def test_brute_casings_are_the_windows_own_spellings_whole_word_headers_excluded():
    window = (
        f"{SEP}\nhttps://acme.example/aluminum\n\n"
        "aluminum, ALUMINUM and Aluminum parts. Leader in lead. Al 6061.\n"
    )
    assert brute_casings_in_window(window, {"Aluminum", "Lead", "Al"}) == [
        "ALUMINUM", "Al", "Aluminum", "aluminum", "lead",
    ]
    # the URL line's 'aluminum' is a header, not text; 'Leader' is not 'Lead';
    # the short label 'Al' stays exact (no 'al' from 'aluminum')
    assert brute_casings_in_window("nothing here", {"Aluminum"}) == []
    assert brute_casings_in_window("x", set()) == []


def test_split_into_form_groups_mirrors_the_relationship_grouping_contract():
    assert split_into_form_groups([], 30) == [[]]
    assert [len(g) for g in split_into_form_groups([f"f{i}" for i in range(7)], 3)] == [3, 3, 1]
    assert [len(g) for g in split_into_form_groups([f"f{i}" for i in range(6)], 3)] == [3, 3]


@pytest.mark.asyncio
async def test_window_forms_are_window_local_exact_strings_sorted_with_brute_casings():
    bundle = ConceptExtractionRequestBundle(
        brute={"Aluminum"},
        brute_by_sub_bounds={"0:100": ["ALUMINUM", "aluminum"], "100:200": []},
        search_sub_bounds=["0:100", "100:200"],
        llm_phrase_search_req_ids=["s0", "s1"],
        llm_phrase_recursive_search_req_ids={"0:100": ["r0a", "r0b"]},
        llm_phrase_recursive_tagging_reqs=None,
    )
    search_map = {"s0": _search(["Brass", "aluminum", "Aluminum"]), "s1": _search(["Steel"])}
    recursive_map = {"r0a": _search(["die-casting", "Brass"]), "r0b": _search([" ", ""])}
    forms = await get_window_forms(
        subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds="0:200",
        sub_bounds="0:100", extraction_bundle=bundle,
        llm_phrase_search_gpt_request_map=search_map,
        llm_phrase_recursive_search_gpt_request_map=recursive_map, timestamp=T0,
    )
    # case variants ride separately (D5); brute casings join; blanks drop; sorted
    assert forms == ["ALUMINUM", "Aluminum", "Brass", "aluminum", "die-casting"]
    other = await get_window_forms(
        subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds="0:200",
        sub_bounds="100:200", extraction_bundle=bundle,
        llm_phrase_search_gpt_request_map=search_map,
        llm_phrase_recursive_search_gpt_request_map=recursive_map, timestamp=T0,
    )
    assert other == ["Steel"]  # never unioned across windows


@pytest.mark.asyncio
async def test_window_forms_reject_an_unknown_sub_window():
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=["0:100"], llm_phrase_search_req_ids=["s0"]
    )
    with pytest.raises(ValueError, match="not one of chunk"):
        await get_window_forms(
            subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds="0:100",
            sub_bounds="5:9", extraction_bundle=bundle,
            llm_phrase_search_gpt_request_map={"s0": _search([])},
            llm_phrase_recursive_search_gpt_request_map={}, timestamp=T0,
        )


# ---------------------------------------------------------------------------
# Request context and the exact hold
# ---------------------------------------------------------------------------


def test_context_ends_in_the_fence_the_hold_reads_back():
    context = render_mention_collection_context("We stock Brass.", ["Brass", "brass"])
    assert context.startswith("text scraped from a manufacturer's website:\nWe stock Brass.")
    assert sent_phrases_from_user_message(context) == ["Brass", "brass"]
    held = hold_response_to_sent_forms(
        user_message=context,
        response_by_form={"brass": [], "Brass": [], "BRASS": []},
        where="test",
    )
    assert list(held) == ["Brass", "brass"]  # sent order; unknown casing dropped, no fuse


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parse_group_returns_sent_forms_and_the_held_answer():
    context = render_mention_collection_context("Brass here. Steel too.", ["Brass", "Steel"])
    req = _request(
        _mention_response({"Brass": [("top", "Brass here.")], "Bronze": [("x", "y")]}),
        context,
    )
    sent, held = await parse_mention_group_result(
        subject_unique_id=SUBJECT, field_type=_Field(), group_req_id="m0",
        completed_request_map={"m0": req}, timestamp=T0,
    )
    assert sent == ["Brass", "Steel"]
    assert list(held) == ["Brass"]  # Bronze dropped (not sent); Steel absent (never answered)
    assert held["Brass"][0].snippet == "Brass here."


@pytest.mark.asyncio
async def test_parse_group_handles_the_dummy_answer_and_missing_requests():
    sent, held = await parse_mention_group_result(
        subject_unique_id=SUBJECT, field_type=_Field(), group_req_id="d0",
        completed_request_map={"d0": _request('{"forms": []}', "No mention collection needed")},
        timestamp=T0,
    )
    assert (sent, held) == ([], {})
    with pytest.raises(ValueError, match="missing GPTBatchRequest"):
        await parse_mention_group_result(
            subject_unique_id=SUBJECT, field_type=_Field(), group_req_id="nope",
            completed_request_map={}, timestamp=T0,
        )


@pytest.mark.asyncio
async def test_window_mentions_merge_groups_in_order():
    c0 = render_mention_collection_context("Brass. Steel.", ["Brass"])
    c1 = render_mention_collection_context("Brass. Steel.", ["Steel"])
    completed = {
        "g0": _request(_mention_response({"Brass": [("p", "Brass.")]}), c0),
        "g1": _request(_mention_response({"Steel": [("p", "Steel.")]}), c1),
    }
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=["0:13"], llm_phrase_mention_req_ids={"0:13": ["g0", "g1"]}
    )
    sent, mentions = await get_window_mentions(
        subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds="0:13",
        sub_bounds="0:13", extraction_bundle=bundle, completed_request_map=completed,
        timestamp=T0,
    )
    assert sent == ["Brass", "Steel"] and set(mentions) == {"Brass", "Steel"}


# ---------------------------------------------------------------------------
# The per-chunk fold
# ---------------------------------------------------------------------------

PAGE_A = "https://acme.example/materials"
PAGE_B = "https://acme.example/about"
SUBJECT_TEXT = (
    f"{SEP}\n{PAGE_A}\n\n"
    "We stock Aluminum and Brass.\n"          # window 0
    "aluminum alloys ship daily.\n"          # window 1 starts here: mid-page, inherits PAGE_A
    f"{SEP}\n{PAGE_B}\n\n"
    "Lead-free solder only. Aluminum | Brass | Steel\n"
)


def _bounds_of(line: str) -> int:
    return SUBJECT_TEXT.index(line)


@pytest.mark.asyncio
async def test_chunk_fold_groups_across_windows_and_inherits_the_page():
    split = _bounds_of("aluminum alloys")
    sub0, sub1 = f"0:{split}", f"{split}:{len(SUBJECT_TEXT)}"
    w0, w1 = window_text_of(SUBJECT_TEXT, sub0), window_text_of(SUBJECT_TEXT, sub1)
    assert window_bounds(sub1) == (split, len(SUBJECT_TEXT))
    assert preceding_page_of(SUBJECT_TEXT, split) == PAGE_A

    c0 = render_mention_collection_context(w0, ["Aluminum", "Brass"])
    c1 = render_mention_collection_context(w1, ["Aluminum", "Brass", "Lead", "aluminum"])
    completed = {
        "m0": _request(
            _mention_response({
                "Aluminum": [("materials intro", "We stock Aluminum and Brass.")],
                "Brass": [],  # lazy: the fold re-attributes from Aluminum's snippet
            }),
            c0,
        ),
        "m1": _request(
            _mention_response({
                "aluminum": [("materials, line 2", "aluminum alloys ship daily.")],
                "Aluminum": [("about footer", "Aluminum | Brass | Steel")],
                "Brass": [("about footer", "Aluminum | Brass | Steel")],
                "Lead": [],  # satisficed: 'Lead-free solder only.' goes unreported
            }),
            c1,
        ),
    }
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[sub0, sub1],
        llm_phrase_mention_req_ids={sub0: ["m0"], sub1: ["m1"]},
    )
    result = await get_chunk_fold(
        subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds=f"0:{len(SUBJECT_TEXT)}",
        extraction_bundle=bundle, completed_request_map=completed, timestamp=T0,
        subject_text=SUBJECT_TEXT, verb_fold=True,
    )
    by_key = {b.key: b for b in result.bundles}
    assert set(by_key) == {"aluminum", "brass", "lead"}
    assert by_key["aluminum"].forms == ("Aluminum", "aluminum")
    assert [(m.window_index, m.form, m.page) for m in by_key["aluminum"].mentions] == [
        (0, "Aluminum", PAGE_A),
        (1, "aluminum", PAGE_A),   # mid-page window inherited the page
        (1, "Aluminum", PAGE_B),
    ]
    assert [(m.window_index, m.page) for m in by_key["brass"].mentions] == [
        (0, PAGE_A),
        (1, PAGE_B),
    ]
    assert by_key["brass"].status == BUNDLE_STATUS_OK
    assert by_key["lead"].status == BUNDLE_STATUS_NO_MENTIONS
    # the hold: window 1 owes one 'Lead' occurrence the collector never reported
    assert [w.window_id for w in result.windows] == [sub0, sub1]
    assert not result.windows[0].has_discrepancy
    assert [(f, len(o)) for f, o in result.windows[1].unaccounted.items() if o] == [("Lead", 1)]
    assert result.verb_fold is True
    assert [r.record_id for r in result.synthesis_records()] == [
        by_key["aluminum"].group_id, by_key["brass"].group_id,
    ]


@pytest.mark.asyncio
async def test_chunk_fold_requires_the_geometry():
    with pytest.raises(ValueError, match="no search_sub_bounds"):
        await get_chunk_fold(
            subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds="0:1",
            extraction_bundle=LLMPhraseExtractionRequestBundle(),
            completed_request_map={}, timestamp=T0, subject_text="x", verb_fold=False,
        )


def test_fold_verb_fold_reads_off_any_metadata_shape():
    assert fold_verb_fold_of(SimpleNamespace(aggregation_fold=SimpleNamespace(verb_fold=True)))
    assert not fold_verb_fold_of(SimpleNamespace(aggregation_fold=None))
    assert not fold_verb_fold_of(object())
