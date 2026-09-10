"""The aggregation-fold service (the mechanical survivor of the retired
mention-collection node service, 2026-09-03): per-window form harvest
(search ∪ recursive ∪ brute casings), the SUBJECT pool filtered to what occurs
in each sub-window, the code collection per sub-window, and the per-chunk
aggregation fold — pure code over (text, stored forms, knobs) — with inherited
pages and stored forms. The LLM location wire these helpers used to feed died
with the location-stage merge; its tests died with it."""

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
from core.services.pipeline_nodes.multi_stage.aggregation_fold_service import (
    brute_casings_in_window,
    collect_sub_window,
    fold_snippet_radius_of,
    fold_verb_fold_of,
    forms_occurring_in_window,
    get_chunk_fold,
    get_chunk_forms,
    get_window_forms,
    stored_window_forms,
    window_bounds,
    window_text_of,
)
from core.utils.aggregation_fold import BUNDLE_STATUS_OK
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
    # the per-window HARVEST stays window-local; pooling is get_chunk_forms
    assert other == ["Steel"]
    pooled = await get_chunk_forms(
        subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds="0:200",
        extraction_bundle=bundle,
        llm_phrase_search_gpt_request_map=search_map,
        llm_phrase_recursive_search_gpt_request_map=recursive_map, timestamp=T0,
    )
    assert pooled == ["ALUMINUM", "Aluminum", "Brass", "Steel", "aluminum", "die-casting"]


def test_forms_occurring_in_window_keeps_only_what_the_scan_can_anchor():
    text = (
        f"{SEP}\nhttps://acme.example/steel-doors\n\n"
        "We stock Brass and aluminum.\n"
        f"{SEP}\nhttps://acme.example/privacy-policy\n\nSteel cookies.\n"
    )
    sub = f"0:{len(text)}"
    # 'Aluminum' is found through its casing (tier 2); 'Steel' occurs only in a URL
    # line and on an excluded page -> not sent; 'Lead' occurs nowhere; blanks drop
    assert forms_occurring_in_window(text, sub, ["Lead", "Steel", "Aluminum", "Brass", " "]) == [
        "Aluminum", "Brass",
    ]
    assert forms_occurring_in_window(text, sub, []) == []
    # a mid-page window inherits its page: the head of a window cut inside an EXCLUDED
    # page is masked up to the next URL line, so a form found only there is not sent
    head = f"{SEP}\nhttps://acme.example/cookie-policy\n\nbanner\n"
    body = f"{SEP}\nhttps://acme.example/brass\n\nBrass here.\n"
    window = f"{len(head) - len('banner\\n')}:{len(head + body)}"
    assert forms_occurring_in_window(head + body, window, ["banner", "Brass"]) == ["Brass"]
    # and a window lying entirely inside an excluded page yields nothing at all
    inside = f"{len(head) - len('banner\\n')}:{len(head) + 12}"
    assert forms_occurring_in_window(head + "Brass here.\n", inside, ["Brass"]) == []


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


def test_stored_forms_are_required_not_guessed():
    bundle = LLMPhraseExtractionRequestBundle(llm_phrase_mention_sent_forms={"0:9": ["Brass"]})
    assert stored_window_forms(SUBJECT, _Field(), "0:9", "0:9", bundle) == ["Brass"]
    with pytest.raises(ValueError, match="re-deferred"):
        stored_window_forms(SUBJECT, _Field(), "0:9", "9:18", bundle)


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------


def test_collect_sub_window_is_pure_in_text_and_forms_and_inherits_the_page():
    text = f"{SEP}\nhttps://acme.example/a\n\nBrass parts.\nbrass too.\n"
    split = text.index("brass too")
    c = collect_sub_window(text, f"{split}:{len(text)}", ["Brass"])
    assert [(m.form, m.page) for m in c.mentions] == [("brass", "https://acme.example/a")]
    assert c == collect_sub_window(text, f"{split}:{len(text)}", ["Brass"])


# ---------------------------------------------------------------------------
# The chunk fold — pure code over (text, stored forms, knobs)
# ---------------------------------------------------------------------------

PAGE_A = "https://acme.example/materials"
PAGE_B = "https://acme.example/about"
PAGE_P = "https://acme.example/privacy-policy"
SUBJECT_TEXT = (
    f"{SEP}\n{PAGE_A}\n\n"
    "We stock Aluminum and Brass.\n"          # window 0
    "aluminum alloys ship daily.\n"          # window 1 starts here: mid-page, inherits PAGE_A
    f"{SEP}\n{PAGE_B}\n\n"
    "Lead-free solder only. Aluminum | Brass | Steel\n"
    f"{SEP}\n{PAGE_P}\n\n"
    "We use Aluminum cookies.\n"
)
SPLIT = SUBJECT_TEXT.index("aluminum alloys")
SUB0, SUB1 = f"0:{SPLIT}", f"{SPLIT}:{len(SUBJECT_TEXT)}"


def test_chunk_fold_groups_across_windows_and_inherits_the_page():
    assert window_text_of(SUBJECT_TEXT, SUB1).startswith("aluminum alloys")
    assert window_bounds(SUB1) == (SPLIT, len(SUBJECT_TEXT))
    assert preceding_page_of(SUBJECT_TEXT, SPLIT) == PAGE_A

    forms0, forms1 = ["Aluminum", "Brass"], ["Aluminum", "Brass", "Lead", "aluminum"]
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[SUB0, SUB1],
        llm_phrase_mention_sent_forms={SUB0: forms0, SUB1: forms1},
    )
    result = get_chunk_fold(
        subject_unique_id=SUBJECT, field_type=_Field(),
        chunk_bounds=f"0:{len(SUBJECT_TEXT)}", extraction_bundle=bundle,
        subject_text=SUBJECT_TEXT, verb_fold=True,
    )
    by_key = {b.key: b for b in result.bundles}
    assert set(by_key) == {"aluminum", "brass", "lead"}
    assert by_key["aluminum"].forms == ("Aluminum", "aluminum")
    assert [(m.window_index, m.form, m.page) for m in by_key["aluminum"].mentions] == [
        (0, "Aluminum", PAGE_A),
        (1, "aluminum", PAGE_A),  # mid-page window inherited the page
        (1, "Aluminum", PAGE_B),
    ]
    assert [(m.window_index, m.page) for m in by_key["brass"].mentions] == [(0, PAGE_A), (1, PAGE_B)]
    assert by_key["brass"].status == BUNDLE_STATUS_OK
    (lead,) = by_key["lead"].mentions
    assert lead.snippet == "Lead-free solder only."
    assert by_key["lead"].status == BUNDLE_STATUS_OK
    assert [w.window_id for w in result.windows] == [SUB0, SUB1]
    assert result.windows[1].collection.excluded_pages == [PAGE_P]
    assert result.verb_fold is True
    assert [r.record_id for r in result.synthesis_records()] == [
        by_key["aluminum"].group_id, by_key["brass"].group_id, by_key["lead"].group_id,
    ]
    # snippets are bare verbatim strings since the location-stage merge
    (lead_record,) = [r for r in result.synthesis_records() if r.record_id == by_key["lead"].group_id]
    assert lead_record.snippets == ["Lead-free solder only."]
    # pure: the same inputs fold to the same result
    assert get_chunk_fold(
        subject_unique_id=SUBJECT, field_type=_Field(),
        chunk_bounds=f"0:{len(SUBJECT_TEXT)}", extraction_bundle=bundle,
        subject_text=SUBJECT_TEXT, verb_fold=True,
    ) == result


def test_chunk_fold_requires_the_geometry():
    with pytest.raises(ValueError, match="no search_sub_bounds"):
        get_chunk_fold(
            subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds="0:1",
            extraction_bundle=LLMPhraseExtractionRequestBundle(),
            subject_text="x", verb_fold=False,
        )


def test_chunk_fold_with_no_stored_forms_says_re_defer():
    bundle = LLMPhraseExtractionRequestBundle(search_sub_bounds=[SUB0])
    with pytest.raises(ValueError, match="re-deferred"):
        get_chunk_fold(
            subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds="0:1",
            extraction_bundle=bundle, subject_text=SUBJECT_TEXT, verb_fold=False,
        )


def test_fold_knobs_read_off_any_metadata_shape():
    assert fold_verb_fold_of(SimpleNamespace(aggregation_fold=SimpleNamespace(verb_fold=True)))
    assert not fold_verb_fold_of(SimpleNamespace(aggregation_fold=None))
    assert not fold_verb_fold_of(object())
    # snippet_radius moved to the fold metadata with the location-stage merge
    assert fold_snippet_radius_of(
        SimpleNamespace(aggregation_fold=SimpleNamespace(snippet_radius=2))
    ) == 2
    assert fold_snippet_radius_of(SimpleNamespace(aggregation_fold=None)) == 0
    assert fold_snippet_radius_of(object()) == 0
