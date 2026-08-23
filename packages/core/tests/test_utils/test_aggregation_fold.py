"""Pipeline v3's aggregation fold as amended 2026-08-22 (PIPELINE_V3_PLAN.md D7,
D8, D9, D11, D19): CODE collects — casing-expanded whole-word occurrences,
longest-span containment, sentence/line clip, distinct-snippet wire items —
the Location stage's answer is attached per mention id, bundles ride in locked
order, synthesis sees each distinct snippet once."""

import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from core.services.phrase_blocks_contract import render_synthesis_record_blocks
from core.utils.aggregation_fold import (
    BUNDLE_STATUS_NO_MENTIONS,
    BUNDLE_STATUS_OK,
    DEFAULT_LOCATION,
    LOCATION_SOURCE_LLM,
    LOCATION_SOURCE_NONE,
    FoldResult,
    WindowInput,
    collect_window,
    fold_document,
    fold_window,
)
from core.utils.floor_scan import EXCLUDED_PAGE_MARKER, floor_scan, omit_excluded_pages
from core.utils.form_normalizer import NORMALIZER_VERSION, group_id_for_key, normalize
from core.utils.record_id_util import (
    MENTION_ID_PREFIX,
    MentionIdCollisionError,
    mention_id_for_snippet,
    record_id_for_phrase,
)

SEP = "#" * 50

# ---------------------------------------------------------------------------
# Golden case: the prototype scenario of PIPELINE_V3_WALKTHROUGH_2_2.md §6,
# now collected by code
# ---------------------------------------------------------------------------

WINDOW = (
    f"{SEP}\nhttps://acme.example/materials\n\n"
    "We stock Aluminum and Brass. Sample Lead Time: 2 weeks.\n"
    "aluminum alloys ship daily.\n"
    f"{SEP}\nhttps://acme.example/about\n\n"
    "Lead-free solder only. Aluminum | Brass | Steel\n"
)
SENT = ["Aluminum", "aluminum", "Brass", "Lead", "Lead Time", "Sample Lead Time", "die-casting"]
INTRO = "We stock Aluminum and Brass."
MATERIALS, ABOUT = "https://acme.example/materials", "https://acme.example/about"


def _at(text: str, needle: str, nth: int = 0) -> int:
    pos = -1
    for _ in range(nth + 1):
        pos = text.index(needle, pos + 1)
    return pos


def _golden(locations=None) -> FoldResult:
    return fold_document([WindowInput(WINDOW, SENT, locations or {})])


def test_golden_collection_is_every_owning_occurrence_in_text_order():
    c = collect_window(WINDOW, SENT)
    got = [(m.form, m.start, m.snippet) for m in c.mentions]
    assert got == [
        ("Aluminum", _at(WINDOW, "Aluminum"), INTRO),
        ("Brass", _at(WINDOW, "Brass"), INTRO),
        ("Sample Lead Time", _at(WINDOW, "Sample Lead Time"), "Sample Lead Time: 2 weeks."),
        ("aluminum", _at(WINDOW, "aluminum alloys"), "aluminum alloys ship daily."),
        ("Lead", _at(WINDOW, "Lead-free"), "Lead-free solder only."),
        ("Aluminum", _at(WINDOW, "Aluminum", 1), "Aluminum | Brass | Steel"),
        ("Brass", _at(WINDOW, "Brass", 1), "Aluminum | Brass | Steel"),
    ]
    # containment: `Lead` and `Lead Time` inside "Sample Lead Time" own nothing there
    assert all(m.form != "Lead Time" for m in c.mentions)
    assert c.zero_hit_forms == ["die-casting"]
    assert sorted(c.forms_with_hits) == sorted(set(SENT) - {"die-casting"})
    # pages are code-derived from the occurrence offset
    assert [m.page for m in c.mentions] == [MATERIALS] * 4 + [ABOUT] * 3


def test_golden_wire_items_are_distinct_snippets_in_first_occurrence_order():
    c = collect_window(WINDOW, SENT)
    assert [i.mention for i in c.items] == [
        INTRO,
        "Sample Lead Time: 2 weeks.",
        "aluminum alloys ship daily.",
        "Lead-free solder only.",
        "Aluminum | Brass | Steel",
    ]
    assert all(i.mention_id == mention_id_for_snippet(i.mention) for i in c.items)
    assert all(i.mention_id.startswith(MENTION_ID_PREFIX) and len(i.mention_id) == 8 for i in c.items)
    assert len({i.mention_id for i in c.items}) == len(c.items)
    # every mention carries the id of its snippet's item
    ids = {i.mention_id for i in c.items}
    assert all(m.mention_id in ids for m in c.mentions)


def test_golden_bundles_keys_forms_and_order():
    result = _golden()
    assert result.normalizer_version == NORMALIZER_VERSION
    assert [b.key for b in result.bundles] == [
        "aluminum",  # first mention: window 0, the intro
        "brass",
        "sample lead time",
        "lead",
        "die casting",  # empties last, by key
        "lead time",
    ]
    by_key = {b.key: b for b in result.bundles}
    assert by_key["aluminum"].forms == ("Aluminum", "aluminum")
    assert by_key["aluminum"].group_id == group_id_for_key("aluminum")
    assert by_key["lead time"].status == BUNDLE_STATUS_NO_MENTIONS  # swallowed by containment
    assert by_key["die casting"].status == BUNDLE_STATUS_NO_MENTIONS  # search false positive
    assert by_key["lead"].status == BUNDLE_STATUS_OK
    assert [m.record_id for m in by_key["aluminum"].mentions] == [
        record_id_for_phrase("Aluminum"),
        record_id_for_phrase("aluminum"),
        record_id_for_phrase("Aluminum"),
    ]


def test_golden_locations_attach_per_snippet_and_default_when_absent():
    c = collect_window(WINDOW, SENT)
    intro_id = c.items[0].mention_id
    result = _golden({intro_id: "materials page, first sentence of the intro"})
    (w,) = result.windows
    assert w.described == [intro_id]
    assert w.not_described == [i.mention_id for i in c.items[1:]]
    assert w.has_undescribed
    by_key = {b.key: b for b in result.bundles}
    aluminum, brass = by_key["aluminum"].mentions, by_key["brass"].mentions
    # both owners of the intro sentence take its one location
    assert aluminum[0].location == brass[0].location == "materials page, first sentence of the intro"
    assert aluminum[0].location_source == LOCATION_SOURCE_LLM
    assert aluminum[1].location == DEFAULT_LOCATION
    assert aluminum[1].location_source == LOCATION_SOURCE_NONE


def test_golden_synthesis_entries_are_distinct_snippets_and_skip_empty_bundles():
    result = _golden()
    by_key = {b.key: b for b in result.bundles}
    entries = by_key["aluminum"].synthesis_entries()
    assert [e.snippet for e in entries] == [INTRO, "aluminum alloys ship daily.", "Aluminum | Brass | Steel"]
    records = result.synthesis_records()
    assert [r.record_id for r in records] == [b.group_id for b in result.bundles if not b.is_empty]
    rendered = render_synthesis_record_blocks([r.model_dump() for r in records])
    assert "<<<RECORD_IDS" in rendered and INTRO in rendered and "die casting" not in rendered


def test_synthesis_entries_dedupe_repeated_lines_across_windows():
    footer = f"{SEP}\nhttps://acme.example/p1\n\nAluminum | Brass\n"
    result = fold_document(
        [WindowInput(footer, ["Aluminum"], {}, window_id="a"), WindowInput(footer, ["Aluminum"], {}, window_id="b")]
    )
    (b,) = result.bundles
    assert len(b.mentions) == 2  # both occurrences stay on the bundle
    assert len(b.synthesis_entries()) == 1  # synthesis sees the line once


# ---------------------------------------------------------------------------
# Collection rules
# ---------------------------------------------------------------------------


def test_casing_rescue_collects_unsent_casings_into_the_family_group():
    result = fold_document([WindowInput(WINDOW, ["Aluminum"], {})])
    (b,) = [x for x in result.bundles if x.key == "aluminum"]
    assert b.forms == ("Aluminum", "aluminum")  # the discovered casing is a member form
    rescued = [m for m in b.mentions if m.form == "aluminum"]
    assert len(rescued) == 1 and rescued[0].sent_form == "Aluminum" and rescued[0].is_discovered_casing
    assert rescued[0].record_id == record_id_for_phrase("aluminum")
    assert result.windows[0].collection.discovered_casings == {"Aluminum": ["aluminum"]}


def test_two_sent_casings_of_one_string_yield_one_mention_per_span():
    c = collect_window("Aluminum parts. aluminum too.", ["Aluminum", "aluminum"])
    assert [(m.form, m.sent_form) for m in c.mentions] == [("Aluminum", "Aluminum"), ("aluminum", "aluminum")]


def test_short_forms_stay_exact_case():
    c = collect_window("AL and Al and al.", ["Al"])
    assert [(m.form, m.start) for m in c.mentions] == [("Al", 7)]


def test_fragment_inside_a_fuller_span_is_not_collected_partial_overlap_is():
    c = collect_window("Sample Lead Time is short. Lead Time-free zone.", ["Lead", "Lead Time", "Sample Lead Time"])
    assert [m.form for m in c.mentions] == ["Sample Lead Time", "Lead Time"]
    c = collect_window("red hot steel", ["red hot", "hot steel"])
    assert [m.form for m in c.mentions] == ["red hot", "hot steel"]


def test_clip_is_the_sentence_within_the_line_or_the_whole_line():
    text = "Intro here. We machine Aluminum daily! Next sentence.\nAluminum | Brass | Steel\n"
    c = collect_window(text, ["Aluminum"])
    assert [m.snippet for m in c.mentions] == ["We machine Aluminum daily!", "Aluminum | Brass | Steel"]
    for m in c.mentions:
        assert text[m.snippet_start : m.snippet_start + len(m.snippet)] == m.snippet
        assert m.snippet_start <= m.start and m.end <= m.snippet_start + len(m.snippet)


def test_clip_never_cuts_inside_the_occurrence():
    text = "Made by Acme Inc. Steel and more. tail"
    c = collect_window(text, ["Inc. Steel"])
    (m,) = c.mentions
    assert "Inc. Steel" in m.snippet


def test_two_owners_in_one_sentence_are_two_mentions_one_item():
    c = collect_window("We stock Aluminum and Brass.", ["Aluminum", "Brass"])
    assert len(c.mentions) == 2 and len(c.items) == 1
    assert c.mentions[0].mention_id == c.mentions[1].mention_id


def test_excluded_pages_are_not_collected_and_are_reported():
    text = (
        f"{SEP}\nhttps://acme.example/about\n\nWe machine Aluminum.\n"
        f"{SEP}\nhttps://acme.example/privacy-policy\n\nAluminum cookies here.\n"
        f"{SEP}\nhttps://acme.example/products\n\nAluminum parts.\n"
    )
    c = collect_window(text, ["Aluminum"])
    assert [m.page for m in c.mentions] == ["https://acme.example/about", "https://acme.example/products"]
    assert c.excluded_pages == ["https://acme.example/privacy-policy"]
    wire = omit_excluded_pages(text)
    assert "cookies" not in wire and EXCLUDED_PAGE_MARKER in wire and "Aluminum parts." in wire


def test_window_head_inherits_the_preceding_page():
    text = "tail of a page with Aluminum\n" + f"{SEP}\nhttps://acme.example/next\n\nAluminum again\n"
    c = collect_window(text, ["Aluminum"], preceding_page="https://acme.example/prev")
    assert [m.page for m in c.mentions] == ["https://acme.example/prev", "https://acme.example/next"]
    c = collect_window(text, ["Aluminum"], preceding_page="https://acme.example/privacy")
    assert [m.page for m in c.mentions] == ["https://acme.example/next"]


def test_sent_forms_are_window_local_and_blank_forms_are_ignored():
    a = WindowInput(f"{SEP}\nhttps://x/a\n\nAluminum here\n", ["Aluminum", " "], {})
    b = WindowInput(f"{SEP}\nhttps://x/b\n\nAluminum there\n", ["Brass"], {})
    result = fold_document([a, b])
    by_key = {x.key: x for x in result.bundles}
    assert len(by_key["aluminum"].mentions) == 1  # b's Aluminum was never sent for b
    assert by_key["brass"].is_empty
    assert fold_document([]).bundles == []


def test_verb_fold_is_a_dial_applied_to_the_keys():
    text = f"{SEP}\nhttps://x/a\n\nCNC milled parts; CNC milling too.\n"
    off = fold_document([WindowInput(text, ["CNC milled", "CNC milling"], {})], verb_fold=False)
    on = fold_document([WindowInput(text, ["CNC milled", "CNC milling"], {})], verb_fold=True)
    assert len(off.bundles) == 2 and len(on.bundles) == 1
    assert on.bundles[0].key == normalize("CNC milled", verb_fold=True)


def test_mention_id_collision_error_is_the_documented_remedy():
    assert issubclass(MentionIdCollisionError, ValueError)


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

_word = st.text(alphabet=st.characters(whitelist_categories=("Ll", "Lu"), max_codepoint=0x24F), min_size=1, max_size=6)
_form = st.one_of(_word, st.builds(lambda a, b: f"{a} {b}", _word, _word), st.sampled_from(["6061-T6", "Lead", "Lead Time", "Sample Lead Time", "Al"]))
_line = st.lists(st.one_of(_word, _form), min_size=0, max_size=8).map(" ".join)
_text = st.lists(_line, min_size=1, max_size=6).map("\n".join)
_forms = st.lists(_form, min_size=1, max_size=6)


@settings(max_examples=200, deadline=None)
@given(_text, _forms, st.randoms())
def test_collection_is_independent_of_sent_order_and_idempotent(text, forms, rng):
    a = collect_window(text, forms)
    shuffled = list(forms)
    rng.shuffle(shuffled)
    b = collect_window(text, shuffled)
    assert [(m.start, m.end, m.form, m.snippet) for m in a.mentions] == [
        (m.start, m.end, m.form, m.snippet) for m in b.mentions
    ]
    assert [i.mention_id for i in a.items] == [i.mention_id for i in b.items]


@settings(max_examples=200, deadline=None)
@given(_text, _forms)
def test_collection_invariants(text, forms):
    c = collect_window(text, forms)
    scan = floor_scan(text, sorted({f for f in forms if f.strip()}))
    spans = set()
    for m in c.mentions:
        # the form is the text at its span, found by its sent form's scan, whole-word
        assert text[m.start : m.end] == m.form
        assert (m.start, m.end) in {(o.start, o.end) for o in scan.tier2[m.sent_form]}
        # the snippet holds the occurrence and sits where it says
        assert text[m.snippet_start : m.snippet_start + len(m.snippet)] == m.snippet
        assert m.snippet_start <= m.start and m.end <= m.snippet_start + len(m.snippet)
        assert "\n" not in m.snippet
        assert m.mention_id == mention_id_for_snippet(m.snippet)
        # no two mentions share a span; no mention sits strictly inside another's span
        assert (m.start, m.end) not in spans
        spans.add((m.start, m.end))
    for a in c.mentions:
        for b in c.mentions:
            assert not (a.start <= b.start and b.end <= a.end and (a.start, a.end) != (b.start, b.end))
    # items are the distinct snippets, in first-occurrence order, ids unique
    first_seen: list[str] = []
    for m in c.mentions:
        if m.snippet not in first_seen:
            first_seen.append(m.snippet)
    assert [i.mention for i in c.items] == first_seen
    assert len({i.mention_id for i in c.items}) == len(c.items)
    # every tier-2 hit of a sent form is either collected or inside a collected span
    for form, occs in scan.tier2.items():
        for o in occs:
            assert any(m.start <= o.start and o.end <= m.end for m in c.mentions)


@settings(max_examples=100, deadline=None)
@given(_text, _forms, st.booleans())
def test_fold_invariants(text, forms, verb_fold):
    result = fold_document([WindowInput(text, forms, {})], verb_fold=verb_fold)
    keys = [b.key for b in result.bundles]
    assert len(set(keys)) == len(keys)
    for b in result.bundles:
        assert all(normalize(f, verb_fold=verb_fold) == b.key for f in b.forms)
        assert list(b.mentions) == sorted(b.mentions)
        assert all(m.location == DEFAULT_LOCATION for m in b.mentions)
        assert len(b.synthesis_entries()) == len({m.snippet for m in b.mentions})
    filled = [b for b in result.bundles if not b.is_empty]
    assert result.bundles[: len(filled)] == filled  # empties last
    assert [r.record_id for r in result.synthesis_records()] == [b.group_id for b in filled]
    (w,) = result.windows
    assert w.described == [] and w.not_described == [i.mention_id for i in w.items]
