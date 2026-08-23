"""Phase 3.1 of pipeline v3 as amended 2026-08-22 (PIPELINE_V3_PLAN.md D4–D7,
D8/D9/D19 at the fold): the mention-collection node service — per-window form
harvest (search ∪ recursive ∪ brute casings), the CHUNK pool filtered to what
occurs in each sub-window, the code collection per sub-window, item groups, the
Location request context and its hold, the parse, the retry pass (under-answer
policy), and the per-chunk aggregation fold with inherited pages and stored
forms."""

import json
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
)
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
)
from core.models.extraction_schemas.mention_collection import MentionWireItem
from core.services.phrase_blocks_contract import (
    sent_mention_ids_from_user_message,
    sent_mentions_from_user_message,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_mention_collection_node_service import (
    brute_casings_in_window,
    collect_sub_window,
    create_missing_mention_collection_requests,
    fold_verb_fold_of,
    forms_occurring_in_window,
    get_chunk_fold,
    get_chunk_forms,
    get_window_forms,
    get_window_locations,
    group_digest_payload,
    parse_mention_group_result,
    render_mention_location_context,
    retry_items_of_window,
    split_into_item_groups,
    stored_window_forms,
    window_bounds,
    window_text_of,
)
from core.utils.aggregation_fold import (
    BUNDLE_STATUS_OK,
    DEFAULT_LOCATION,
)
from core.utils.floor_scan import EXCLUDED_PAGE_MARKER, preceding_page_of

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


def _location_response(by_id: dict[str, str]) -> str:
    return json.dumps({"mentions": [{"mention_id": i, "location": loc} for i, loc in by_id.items()]})


def _items(*pairs: tuple[str, str]) -> list[MentionWireItem]:
    return [MentionWireItem(mention_id=i, mention=m) for i, m in pairs]


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
# Collection + groups
# ---------------------------------------------------------------------------


def test_split_into_item_groups_mirrors_the_grouping_contract():
    assert split_into_item_groups([], 30) == [[]]
    seven = _items(*[(f"m{i}", f"s{i}") for i in range(7)])
    assert [len(g) for g in split_into_item_groups(seven, 3)] == [3, 3, 1]
    assert [len(g) for g in split_into_item_groups(seven[:6], 3)] == [3, 3]
    assert group_digest_payload(seven[:2]) == [["m0", "s0"], ["m1", "s1"]]


def test_collect_sub_window_is_pure_in_text_and_forms_and_inherits_the_page():
    text = f"{SEP}\nhttps://acme.example/a\n\nBrass parts.\nbrass too.\n"
    split = text.index("brass too")
    c = collect_sub_window(text, f"{split}:{len(text)}", ["Brass"])
    assert [(m.form, m.page) for m in c.mentions] == [("brass", "https://acme.example/a")]
    assert c == collect_sub_window(text, f"{split}:{len(text)}", ["Brass"])


# ---------------------------------------------------------------------------
# Request context and the hold
# ---------------------------------------------------------------------------


def test_context_ends_in_the_blocks_the_hold_reads_back():
    items = _items(("m1", "We stock Brass."), ("m2", "Brass | Steel"))
    context = render_mention_location_context("We stock Brass.\nBrass | Steel", items)
    assert context.startswith("text scraped from a manufacturer's website:\nWe stock Brass.")
    assert sent_mention_ids_from_user_message(context) == ["m1", "m2"]
    assert sent_mentions_from_user_message(context) == [i.model_dump() for i in items]


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parse_group_returns_sent_ids_and_the_held_answer():
    context = render_mention_location_context("Brass here. Steel too.", _items(("m1", "Brass here."), ("m2", "Steel too.")))
    req = _request(_location_response({"m1": "top of page", "m9": "never sent"}), context)
    sent, held, unknown = await parse_mention_group_result(
        subject_unique_id=SUBJECT, field_type=_Field(), group_req_id="g0",
        completed_request_map={"g0": req}, timestamp=T0,
    )
    assert sent == ["m1", "m2"]
    assert held == {"m1": "top of page"}  # m9 dropped (not sent); m2 absent (never answered)
    assert unknown == ["m9"]  # reported, so the dump can say WHY a window came back short


@pytest.mark.asyncio
async def test_parse_group_handles_the_dummy_answer_and_missing_requests():
    sent, held, unknown = await parse_mention_group_result(
        subject_unique_id=SUBJECT, field_type=_Field(), group_req_id="d0",
        completed_request_map={"d0": _request('{"mentions": []}', "No mention location needed")},
        timestamp=T0,
    )
    assert (sent, held, unknown) == ([], {}, [])
    with pytest.raises(ValueError, match="missing GPTBatchRequest"):
        await parse_mention_group_result(
            subject_unique_id=SUBJECT, field_type=_Field(), group_req_id="nope",
            completed_request_map={}, timestamp=T0,
        )


@pytest.mark.asyncio
async def test_window_locations_merge_groups_in_order():
    c0 = render_mention_location_context("Brass. Steel.", _items(("m1", "Brass.")))
    c1 = render_mention_location_context("Brass. Steel.", _items(("m2", "Steel.")))
    completed = {
        "g0": _request(_location_response({"m1": "first"}), c0),
        "g1": _request(_location_response({"m2": "second"}), c1),
    }
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=["0:13"], llm_phrase_mention_req_ids={"0:13": ["g0", "g1"]}
    )
    answer = await get_window_locations(
        subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds="0:13",
        sub_bounds="0:13", extraction_bundle=bundle, completed_request_map=completed,
        timestamp=T0,
    )
    assert answer.sent_ids == ["m1", "m2"] and answer.locations == {"m1": "first", "m2": "second"}
    assert answer.missing_ids == [] and answer.retried_mention_ids == []
    assert answer.unknown_answer_ids == []


@pytest.mark.asyncio
async def test_window_locations_read_the_retry_after_the_groups():
    c0 = render_mention_location_context("Brass. Steel.", _items(("m1", "Brass."), ("m2", "Steel.")))
    cr = render_mention_location_context("Brass. Steel.", _items(("m2", "Steel.")))
    completed = {
        # the first pass answered one of two and echoed an id that was never sent
        "g0": _request(_location_response({"m1": "first", "zz": "never sent"}), c0),
        "r0": _request(_location_response({"m2": "from the retry"}), cr),
    }
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=["0:13"],
        llm_phrase_mention_req_ids={"0:13": ["g0"]},
        llm_phrase_mention_retry_mention_ids={"0:13": ["m2"]},
        llm_phrase_mention_retry_req_ids={"0:13": ["r0"]},
    )
    kwargs: dict[str, Any] = dict(
        subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds="0:13", sub_bounds="0:13",
        extraction_bundle=bundle, completed_request_map=completed, timestamp=T0,
    )
    first_pass = await get_window_locations(**kwargs, include_retry=False)
    assert first_pass.missing_ids == ["m2"] and first_pass.unknown_answer_ids == ["zz"]
    assert first_pass.retried_mention_ids == []
    merged = await get_window_locations(**kwargs)
    assert merged.locations == {"m1": "first", "m2": "from the retry"}
    assert merged.missing_ids == [] and merged.retried_mention_ids == ["m2"]
    assert merged.unknown_answer_ids == ["zz"]


# ---------------------------------------------------------------------------
# Request creation
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


@pytest.fixture(autouse=True, scope="module")
def offline_gpt_batch_request_settings():
    """Request creation constructs a ``GPTBatchRequest`` Document; validating one
    needs only its ``_document_settings`` (the synchronous half of
    ``init_beanie``), so populate that and nothing else — the same offline
    pattern the app tests' conftest uses for every app document."""
    from beanie.odm.settings.document import DocumentSettings
    from llm_providers.db_models.gpt_batch_request import GPTBatchRequest

    settings_class = getattr(GPTBatchRequest, "Settings")
    settings_vars = {a: getattr(settings_class, a) for a in dir(settings_class) if not a.startswith("__")}
    GPTBatchRequest._document_settings = DocumentSettings(**settings_vars)


_PARAMS = GPTModelParams(
    max_completion_tokens=1000,
    response_format={"type": "json_object"},
    temperature=0.0,
    top_p=1.0,
    presence_penalty=0.0,
    frequency_penalty=0.0,
)
_MODEL = LLM_Model(name="gpt-4.1", max_context_tokens=128000)
_PROMPT = Prompt(text="PROMPT", s3_version_id="v1", name="p", num_tokens=2)


@pytest.mark.asyncio
async def test_requests_are_built_from_the_stored_forms_and_omit_excluded_pages():
    forms1 = ["Aluminum", "Brass", "Lead", "aluminum"]
    c1 = collect_sub_window(SUBJECT_TEXT, SUB1, forms1)
    assert len(c1.items) == 3  # 'aluminum alloys…', 'Lead-free solder only.', the footer; privacy page excluded
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[SUB0, SUB1],
        llm_phrase_mention_req_ids={SUB0: ["m0"], SUB1: ["m1a", "m1b"]},
        llm_phrase_mention_sent_forms={SUB0: ["Titanium"], SUB1: forms1},
    )
    requests = await create_missing_mention_collection_requests(
        subject_unique_id=SUBJECT, field_type=_Field(), chunked_request_map={"0:999": bundle},
        missing_request_ids={"m0", "m1a", "m1b"}, subject_text=SUBJECT_TEXT,
        phrase_mention_collection_prompt=_PROMPT, timestamp=T0,
        llm_model=_MODEL, model_params=_PARAMS,
        max_mentions_per_request=2, eager=True,
    )
    by_id = {r.request.custom_id: r for r in requests}
    assert set(by_id) == {"m0", "m1a", "m1b"}
    # window 0 has no mentions of 'Titanium' -> dummy, pre-answered
    dummy_response = by_id["m0"].response
    assert dummy_response is not None and dummy_response.result is not None
    assert '"mentions": []' in dummy_response.result
    # window 1: two groups of the three items, the same window text, excluded page omitted
    msg_a, msg_b = by_id["m1a"].request.body.user_message(), by_id["m1b"].request.body.user_message()
    assert sent_mention_ids_from_user_message(msg_a) == [i.mention_id for i in c1.items[:2]]
    assert sent_mention_ids_from_user_message(msg_b) == [i.mention_id for i in c1.items[2:]]
    for msg in (msg_a, msg_b):
        assert "Lead-free solder only." in msg and EXCLUDED_PAGE_MARKER in msg and "cookies" not in msg
        assert "<<<PHRASES" not in msg  # no forms on the wire


@pytest.mark.asyncio
async def test_retry_requests_carry_only_the_stored_missing_items():
    forms1 = ["Aluminum", "Brass", "Lead", "aluminum"]
    c1 = collect_sub_window(SUBJECT_TEXT, SUB1, forms1)
    alloys, solder, footer = c1.items
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[SUB1],
        llm_phrase_mention_req_ids={SUB1: ["m1a", "m1b"]},
        llm_phrase_mention_sent_forms={SUB1: forms1},
        llm_phrase_mention_retry_mention_ids={SUB1: [solder.mention_id, footer.mention_id]},
        llm_phrase_mention_retry_req_ids={SUB1: ["r1a"]},
    )
    requests = await create_missing_mention_collection_requests(
        subject_unique_id=SUBJECT, field_type=_Field(), chunked_request_map={"0:999": bundle},
        missing_request_ids={"r1a"}, subject_text=SUBJECT_TEXT,
        phrase_mention_collection_prompt=_PROMPT, timestamp=T0,
        llm_model=_MODEL, model_params=_PARAMS,
        max_mentions_per_request=2, eager=True,
    )
    assert [r.request.custom_id for r in requests] == ["r1a"]  # the groups were not asked for
    msg = requests[0].request.body.user_message()
    assert sent_mention_ids_from_user_message(msg) == [solder.mention_id, footer.mention_id]
    assert alloys.mention in msg  # the same window text travels with the retry
    # the retry items are taken from the collection, in window order
    wanted = {footer.mention_id, solder.mention_id}
    assert [i.mention_id for i in retry_items_of_window(SUBJECT, _Field(), SUB1, c1, wanted)] == [
        solder.mention_id, footer.mention_id,
    ]
    with pytest.raises(ValueError, match="not among the window's collected items"):
        retry_items_of_window(SUBJECT, _Field(), SUB1, c1, ["m-not-there"])
    # a retry grouping that no longer matches the stored ids is refused, not guessed
    bundle.llm_phrase_mention_retry_req_ids[SUB1] = ["r1a", "r1b"]
    with pytest.raises(ValueError, match="retry group count"):
        await create_missing_mention_collection_requests(
            subject_unique_id=SUBJECT, field_type=_Field(), chunked_request_map={"0:999": bundle},
            missing_request_ids={"r1a"}, subject_text=SUBJECT_TEXT,
            phrase_mention_collection_prompt=_PROMPT, timestamp=T0,
            llm_model=_MODEL, model_params=_PARAMS,
            max_mentions_per_request=2, eager=True,
        )


@pytest.mark.asyncio
async def test_request_creation_refuses_a_group_count_that_drifted():
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[SUB1],
        llm_phrase_mention_req_ids={SUB1: ["only-one"]},
        llm_phrase_mention_sent_forms={SUB1: ["Aluminum", "Brass", "Lead"]},
    )
    with pytest.raises(ValueError, match="does not match the computed count"):
        await create_missing_mention_collection_requests(
            subject_unique_id=SUBJECT, field_type=_Field(), chunked_request_map={"0:999": bundle},
            missing_request_ids={"only-one"}, subject_text=SUBJECT_TEXT,
            phrase_mention_collection_prompt=_PROMPT, timestamp=T0,
            llm_model=_MODEL, model_params=_PARAMS,
            max_mentions_per_request=1, eager=True,
        )


# ---------------------------------------------------------------------------
# The per-chunk fold
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chunk_fold_groups_across_windows_inherits_the_page_and_defaults_missing_locations():
    w0, w1 = window_text_of(SUBJECT_TEXT, SUB0), window_text_of(SUBJECT_TEXT, SUB1)
    assert window_bounds(SUB1) == (SPLIT, len(SUBJECT_TEXT))
    assert preceding_page_of(SUBJECT_TEXT, SPLIT) == PAGE_A

    forms0, forms1 = ["Aluminum", "Brass"], ["Aluminum", "Brass", "Lead", "aluminum"]
    c0, c1 = collect_sub_window(SUBJECT_TEXT, SUB0, forms0), collect_sub_window(SUBJECT_TEXT, SUB1, forms1)
    intro, (alloys, solder, footer) = c0.items[0], c1.items
    completed = {
        "m0": _request(_location_response({intro.mention_id: "materials intro"}),
                       render_mention_location_context(w0, c0.items)),
        "m1": _request(_location_response({footer.mention_id: "about footer", alloys.mention_id: "materials, line 2"}),
                       render_mention_location_context(w1, c1.items)),  # 'Lead-free…' left undescribed
    }
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[SUB0, SUB1],
        llm_phrase_mention_req_ids={SUB0: ["m0"], SUB1: ["m1"]},
        llm_phrase_mention_sent_forms={SUB0: forms0, SUB1: forms1},
    )
    result = await get_chunk_fold(
        subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds=f"0:{len(SUBJECT_TEXT)}",
        extraction_bundle=bundle, completed_request_map=completed, timestamp=T0,
        subject_text=SUBJECT_TEXT, verb_fold=True,
    )
    by_key = {b.key: b for b in result.bundles}
    assert set(by_key) == {"aluminum", "brass", "lead"}
    assert by_key["aluminum"].forms == ("Aluminum", "aluminum")
    assert [(m.window_index, m.form, m.page, m.location) for m in by_key["aluminum"].mentions] == [
        (0, "Aluminum", PAGE_A, "materials intro"),
        (1, "aluminum", PAGE_A, "materials, line 2"),  # mid-page window inherited the page
        (1, "Aluminum", PAGE_B, "about footer"),
    ]
    assert [(m.window_index, m.page) for m in by_key["brass"].mentions] == [(0, PAGE_A), (1, PAGE_B)]
    assert by_key["brass"].status == BUNDLE_STATUS_OK
    # code collected 'Lead-free solder only.' though the model never described it
    (lead,) = by_key["lead"].mentions
    assert lead.snippet == "Lead-free solder only." and lead.location == DEFAULT_LOCATION
    assert by_key["lead"].status == BUNDLE_STATUS_OK
    assert [w.window_id for w in result.windows] == [SUB0, SUB1]
    assert result.windows[0].not_described == []
    assert result.windows[1].not_described == [solder.mention_id]
    assert result.windows[1].retried == [] and result.windows[1].unknown_answer_ids == []
    assert result.windows[1].collection.excluded_pages == [PAGE_P]

    # the retry pass fills what the groups left out, and the fold says so
    completed["r1"] = _request(
        _location_response({solder.mention_id: "about, first line"}),
        render_mention_location_context(w1, [solder]),
    )
    bundle.llm_phrase_mention_retry_mention_ids[SUB1] = [solder.mention_id]
    bundle.llm_phrase_mention_retry_req_ids[SUB1] = ["r1"]
    retried = await get_chunk_fold(
        subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds=f"0:{len(SUBJECT_TEXT)}",
        extraction_bundle=bundle, completed_request_map=completed, timestamp=T0,
        subject_text=SUBJECT_TEXT, verb_fold=True,
    )
    (lead_again,) = {b.key: b for b in retried.bundles}["lead"].mentions
    assert lead_again.location == "about, first line" and lead_again.location_source == "llm"
    assert retried.windows[1].not_described == []
    assert retried.windows[1].retried == [solder.mention_id]
    assert result.verb_fold is True
    assert [r.record_id for r in result.synthesis_records()] == [
        by_key["aluminum"].group_id, by_key["brass"].group_id, by_key["lead"].group_id,
    ]


@pytest.mark.asyncio
async def test_chunk_fold_requires_the_geometry():
    with pytest.raises(ValueError, match="no search_sub_bounds"):
        await get_chunk_fold(
            subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds="0:1",
            extraction_bundle=LLMPhraseExtractionRequestBundle(),
            completed_request_map={}, timestamp=T0, subject_text="x", verb_fold=False,
        )


@pytest.mark.asyncio
async def test_chunk_fold_with_no_stored_forms_says_re_defer():
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[SUB0], llm_phrase_mention_req_ids={SUB0: ["d0"]}
    )
    with pytest.raises(ValueError, match="re-deferred"):
        await get_chunk_fold(
            subject_unique_id=SUBJECT, field_type=_Field(), chunk_bounds="0:1",
            extraction_bundle=bundle,
            completed_request_map={"d0": _request('{"mentions": []}', "dummy")},
            timestamp=T0, subject_text=SUBJECT_TEXT, verb_fold=False,
        )


def test_fold_verb_fold_reads_off_any_metadata_shape():
    assert fold_verb_fold_of(SimpleNamespace(aggregation_fold=SimpleNamespace(verb_fold=True)))
    assert not fold_verb_fold_of(SimpleNamespace(aggregation_fold=None))
    assert not fold_verb_fold_of(object())
