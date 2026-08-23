"""Phase 3.1 of pipeline v3 (amended 2026-08-22): the fold as a dump reader
meets it — code-collected mentions with their location source, the per-window
collection + Location-stage coverage — and the request witness for the
per-sub-window mention requests."""

from types import SimpleNamespace
from typing import cast

from core.utils.aggregation_fold import DEFAULT_LOCATION, WindowInput, collect_window, fold_document
from core.utils.extraction_dump_util import build_chunk_requests
from core.utils.fold_dump_util import build_fold_dump

SEP = "#" * 50
WINDOW = (
    f"{SEP}\nhttps://acme.example/materials\n\n"
    "We stock Aluminum and Brass. aluminum alloys ship daily.\n"
    f"{SEP}\nhttps://acme.example/privacy-policy\n\nBrass cookies.\n"
)


def test_fold_dump_carries_groups_forms_mentions_and_the_window_report():
    items = collect_window(WINDOW, ["Aluminum", "Brass"]).items
    intro = items[0]
    locations = {intro.mention_id: "materials page, intro sentence"}
    result = fold_document([WindowInput(WINDOW, ["Aluminum", "Brass"], locations, window_id="0:99")])
    dump = build_fold_dump(result, subject_name="Acme")

    assert dump["normalizer_version"] == result.normalizer_version
    assert dump["summary"]["groups"] == 2 and dump["summary"]["empty_groups"] == 0
    assert dump["summary"]["mentions"] == 3  # Aluminum, Brass (intro), aluminum (rescued casing)
    assert dump["summary"]["distinct_snippets"] == 2
    assert dump["summary"]["described"] == 1 and dump["summary"]["not_described"] == 1
    assert dump["summary"]["windows_with_undescribed"] == 1
    assert dump["summary"]["discovered_casings"] == 1
    assert dump["summary"]["windows_with_excluded_pages"] == 1
    groups = {g["key"]: g for g in dump["groups"]}
    assert groups["aluminum"]["forms"] == ["Aluminum", "aluminum"] and groups["aluminum"]["status"] == "ok"
    assert groups["aluminum"]["distinct_snippets"] == 2
    first, rescued = groups["aluminum"]["mentions"]
    assert first["form"] == "Aluminum" and first["location"] == "materials page, intro sentence"
    assert first["location_source"] == "llm" and first["mention_id"] == intro.mention_id
    assert first["page"] == "https://acme.example/materials" and first["window"] == 0
    assert "sent_form" not in first
    assert rescued["form"] == "aluminum" and rescued["sent_form"] == "Aluminum"
    assert rescued["location"] == DEFAULT_LOCATION and rescued["location_source"] == "none"
    (window,) = dump["windows"]
    assert window["sub_bounds"] == "0:99" and window["sent_forms"] == 2
    assert window["forms_with_hits"] == 2 and window["zero_hit_forms"] == []
    assert window["mentions"] == 3 and window["distinct_snippets"] == 2
    assert window["described"] == 1 and window["not_described"] == [items[1].mention_id]
    assert window["discovered_casings"] == {"Aluminum": ["aluminum"]}
    assert window["excluded_pages"] == ["https://acme.example/privacy-policy"]


def test_fold_dump_marks_empty_groups_and_zero_hit_forms():
    result = fold_document([WindowInput(WINDOW, ["Steel", "Titanium"], {})])
    dump = build_fold_dump(result)
    assert dump["summary"]["empty_groups"] == 2 and dump["summary"]["mentions"] == 0
    assert dump["summary"]["zero_hit_forms"] == 2
    assert dump["windows"][0]["zero_hit_forms"] == ["Steel", "Titanium"]
    assert {g["status"] for g in dump["groups"]} == {"no_mentions"}


def test_chunk_requests_witness_lists_mention_groups_per_sub_window():
    bundle = SimpleNamespace(
        llm_phrase_search_req_ids=["s0"],
        llm_phrase_mention_req_ids={"100:200": ["m1a"], "0:100": ["m0a", "m0b"]},
    )
    requests = build_chunk_requests(bundle, completed_requests={})
    by_sub_window = cast(dict, requests["llm_phrase_mention_collection"])
    assert list(by_sub_window) == ["0:100", "100:200"]
    assert len(by_sub_window["0:100"]) == 2
    assert "llm_phrase_search" in requests
