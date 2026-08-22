"""Phase 3.1 of pipeline v3: the fold as a dump reader meets it, and the
request witness for the per-sub-window mention requests."""

from types import SimpleNamespace
from typing import cast

from core.models.extraction_schemas.mention_collection import Mention
from core.utils.aggregation_fold import WindowInput, fold_document
from core.utils.extraction_dump_util import build_chunk_requests
from core.utils.fold_dump_util import build_fold_dump

SEP = "#" * 50
WINDOW = (
    f"{SEP}\nhttps://acme.example/materials\n\n"
    "We stock Aluminum and Brass. aluminum alloys ship daily.\n"
)


def test_fold_dump_carries_groups_forms_mentions_and_the_window_hold():
    answer = {"Aluminum": [Mention(location="intro", snippet="We stock Aluminum and Brass.")]}
    result = fold_document([WindowInput(WINDOW, ["Aluminum", "Brass"], answer, window_id="0:99")])
    dump = build_fold_dump(result, subject_name="Acme")

    assert dump["normalizer_version"] == result.normalizer_version
    assert dump["summary"]["groups"] == 2 and dump["summary"]["empty_groups"] == 0
    assert dump["summary"]["mentions"] == 2  # Brass re-attributed from Aluminum's snippet
    # no rekey REPORT: the collector's own form was among the snippet's owners;
    # the Brass mention still says which form it was filed under
    assert dump["summary"]["unaccounted"] == 0 and dump["summary"]["rekeyed"] == 0
    groups = {g["key"]: g for g in dump["groups"]}
    assert groups["aluminum"]["forms"] == ["Aluminum"] and groups["aluminum"]["status"] == "ok"
    brass = groups["brass"]["mentions"][0]
    assert brass["form"] == "Brass" and brass["reported_form"] == "Aluminum"
    assert brass["page"] == "https://acme.example/materials" and brass["window"] == 0
    assert "reported_form" not in groups["aluminum"]["mentions"][0]
    (window,) = dump["windows"]
    assert window["sub_bounds"] == "0:99" and window["sent_forms"] == 2
    assert window["unaccounted"] == {} and window["rekeyed"] == []
    # tier-2 discovery: the casing no sent form covered
    assert window["discovered_casings"] == {"Aluminum": ["aluminum"]}


def test_fold_dump_marks_empty_groups_and_missed_obligations():
    result = fold_document([WindowInput(WINDOW, ["Brass", "Steel"], {})])
    dump = build_fold_dump(result)
    assert dump["summary"]["empty_groups"] == 2
    assert dump["summary"]["unaccounted"] == 1 and dump["summary"]["windows_with_discrepancy"] == 1
    brass_at = WINDOW.index("Brass")
    assert dump["windows"][0]["unaccounted"] == {"Brass": [[brass_at, brass_at + 5]]}
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
