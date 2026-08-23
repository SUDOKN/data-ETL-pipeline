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


# ---------------------------------------------------------------------------
# Snippet radius (user knob, 2026-08-22) and the focal form / location arm
# (D15 as amended 2026-08-22)
# ---------------------------------------------------------------------------


def _snippets(text, forms, radius):
    return [m.snippet for m in collect_window(text, forms, snippet_radius=radius).mentions]


def test_radius_zero_is_the_legacy_clip_byte_for_byte():
    c0 = collect_window(WINDOW, SENT)
    c1 = collect_window(WINDOW, SENT, snippet_radius=0)
    assert [(m.start, m.snippet, m.snippet_start, m.mention_id) for m in c0.mentions] == [
        (m.start, m.snippet, m.snippet_start, m.mention_id) for m in c1.mentions
    ]
    assert [i.mention_id for i in c0.items] == [i.mention_id for i in c1.items]


def test_radius_widens_by_sentence_units_within_the_line():
    text = "A one. B two Aluminum here. C three. D four.\n"
    assert _snippets(text, ["Aluminum"], 0) == ["B two Aluminum here."]
    assert _snippets(text, ["Aluminum"], 1) == ["A one. B two Aluminum here. C three."]
    assert _snippets(text, ["Aluminum"], 2) == ["A one. B two Aluminum here. C three. D four."]
    assert _snippets(text, ["Aluminum"], 9) == ["A one. B two Aluminum here. C three. D four."]


def test_radius_crosses_line_breaks_as_unit_breaks_and_skips_blank_lines():
    text = "Line one.\nAluminum line.\nLine three.\n"
    assert _snippets(text, ["Aluminum"], 1) == ["Line one.\nAluminum line.\nLine three."]
    spaced = "First.\n\nAluminum.\n\nLast.\n"
    assert _snippets(spaced, ["Aluminum"], 1) == ["First.\n\nAluminum.\n\nLast."]
    # a multi-line snippet still sits where it says it does, and hashes as itself
    c = collect_window(spaced, ["Aluminum"], snippet_radius=1)
    (m,) = c.mentions
    assert spaced[m.snippet_start : m.snippet_start + len(m.snippet)] == m.snippet
    assert m.mention_id == mention_id_for_snippet(m.snippet)


def test_radius_never_crosses_or_includes_a_page_boundary_line():
    text = (
        f"{SEP}\nhttps://acme.example/a\nIntro.\nAluminum here.\n"
        f"{SEP}\nhttps://acme.example/b\nOther page.\nMore.\n"
    )
    assert _snippets(text, ["Aluminum"], 5) == ["Intro.\nAluminum here."]
    # a hit at the top of a page looks down only as far as the next boundary
    assert _snippets(text, ["Other"], 5) == ["Other page.\nMore."]


def test_radius_completes_an_occurrence_that_spans_a_sentence_break():
    text = "Made by Acme Inc. Steel and more. tail"
    assert _snippets(text, ["Inc. Steel"], 0) == ["Made by Acme Inc. Steel"]  # legacy
    assert _snippets(text, ["Inc. Steel"], 1) == ["Made by Acme Inc. Steel and more. tail"]


def test_negative_radius_is_refused_and_radius_is_recorded_on_the_fold():
    with pytest.raises(ValueError, match="snippet_radius"):
        collect_window("Aluminum.", ["Aluminum"], snippet_radius=-1)
    result = fold_document([WindowInput(WINDOW, SENT, {})], snippet_radius=1)
    assert result.snippet_radius == 1
    assert fold_document([WindowInput(WINDOW, SENT, {})]).snippet_radius == 0


@settings(max_examples=150, deadline=None)
@given(_text, _forms, st.integers(min_value=1, max_value=3))
def test_radius_invariants(text, forms, radius):
    wide = collect_window(text, forms, snippet_radius=radius)
    tight = collect_window(text, forms)
    # same occurrences in the same order; every snippet holds its occurrence and
    # contains the radius-0 snippet; id = hash(snippet)
    assert [(m.start, m.end, m.form) for m in wide.mentions] == [
        (m.start, m.end, m.form) for m in tight.mentions
    ]
    for w, t in zip(wide.mentions, tight.mentions, strict=True):
        assert text[w.snippet_start : w.snippet_start + len(w.snippet)] == w.snippet
        assert w.snippet_start <= w.start and w.end <= w.snippet_start + len(w.snippet)
        assert t.snippet in w.snippet
        assert w.mention_id == mention_id_for_snippet(w.snippet)


def test_focal_form_is_the_most_frequent_member_form_ties_to_the_earliest():
    result = _golden()
    by_key = {b.key: b for b in result.bundles}
    assert by_key["aluminum"].forms == ("Aluminum", "aluminum")
    assert by_key["aluminum"].focal_form == "Aluminum"  # 2 mentions vs 1
    assert by_key["brass"].focal_form == "Brass"
    # a tie goes to the form whose first mention comes first in locked order
    tie = fold_document([WindowInput("We use steel. Steel is strong.", ["steel"], {})])
    (b,) = [b for b in tie.bundles if not b.is_empty]
    assert b.forms == ("Steel", "steel") and b.focal_form == "steel"
    # an empty bundle has no focal form and no record
    empty = by_key[normalize("die-casting")]
    assert empty.focal_form is None
    with pytest.raises(ValueError, match="no mentions"):
        empty.synthesis_record()


def test_synthesis_records_carry_the_focal_form_and_honour_the_location_arm():
    result = _golden({mention_id_for_snippet(INTRO): "the intro line"})
    with_loc = result.synthesis_records()
    without = result.synthesis_records(include_location=False)
    assert [r.record_id for r in with_loc] == [r.record_id for r in without]
    assert all(r.focal_form for r in with_loc)
    first = with_loc[0]
    assert first.focal_form == "Aluminum"
    assert first.entries[0].location == "the intro line"
    assert all(e.location is None for r in without for e in r.entries)
    assert "location" not in without[0].wire_dict()["entries"][0]
    assert with_loc[0].wire_dict()["entries"][0]["location"] == "the intro line"
    # the wire dicts render (the synthesis request's two blocks) on both arms
    assert render_synthesis_record_blocks([r.wire_dict() for r in with_loc])
    assert render_synthesis_record_blocks([r.wire_dict() for r in without])
