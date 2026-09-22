"""Pipeline v3's aggregation fold (PIPELINE_V3_PLAN.md D7, D8, D9, D11, D19,
D21): CODE collects — casing-expanded whole-word occurrences, DECENTRALIZED so
nesting never suppresses (D8 reversed 2026-08-27), sentence/line clip,
distinct-snippet wire items — the Location stage's answer is attached per
mention id, bundles ride in locked order, synthesis sees each distinct snippet
once, and a compound that is a mere coordination of siblings collapses (D21)."""

import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from core.services.phrase_blocks_contract import render_synthesis_record_blocks
from core.utils.aggregation_fold import (
    BUNDLE_STATUS_COLLAPSED,
    BUNDLE_STATUS_NO_MENTIONS,
    BUNDLE_STATUS_OK,
    DEFAULT_LOCATION,
    LOCATION_SOURCE_LLM,
    LOCATION_SOURCE_NONE,
    FoldResult,
    WindowInput,
    collect_window,
    coordination_segments,
    fold_document,
    fold_window,
)
from core.utils.floor_scan import EXCLUDED_PAGE_MARKER, floor_scan, omit_excluded_pages
from core.utils.form_normalizer import NORMALIZER_VERSION, normalize
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


def _golden() -> FoldResult:
    return fold_document([WindowInput(WINDOW, SENT)])


def test_golden_collection_is_every_occurrence_in_text_order():
    c = collect_window(WINDOW, SENT)
    got = [(m.form, m.start, m.snippet) for m in c.mentions]
    lead_time_snippet = "Sample Lead Time: 2 weeks."
    assert got == [
        ("Aluminum", _at(WINDOW, "Aluminum"), INTRO),
        ("Brass", _at(WINDOW, "Brass"), INTRO),
        # DECENTRALIZED (D8 reversed 2026-08-27): the enclosing form first, then
        # every form nested inside it — each is a mention of its own group.
        ("Sample Lead Time", _at(WINDOW, "Sample Lead Time"), lead_time_snippet),
        ("Lead Time", _at(WINDOW, "Lead Time"), lead_time_snippet),
        ("Lead", _at(WINDOW, "Lead Time"), lead_time_snippet),
        ("aluminum", _at(WINDOW, "aluminum alloys"), "aluminum alloys ship daily."),
        ("Lead", _at(WINDOW, "Lead-free"), "Lead-free solder only."),
        ("Aluminum", _at(WINDOW, "Aluminum", 1), "Aluminum | Brass | Steel"),
        ("Brass", _at(WINDOW, "Brass", 1), "Aluminum | Brass | Steel"),
    ]
    assert sorted(c.forms_with_hits) == sorted(set(SENT) - {"die-casting"})
    # pages are code-derived from the occurrence offset
    assert [m.page for m in c.mentions] == [MATERIALS] * 6 + [ABOUT] * 3


def test_nesting_does_not_suppress_but_a_span_is_collected_once():
    """D8 reversed: `Lead` inside "Sample Lead Time" is a mention of `Lead`.
    Two sent casings of one string still yield ONE mention for the span."""
    c = collect_window("Sample Lead Time", ["Sample Lead Time", "Lead Time", "Lead"])
    assert [(m.form, m.start) for m in c.mentions] == [
        ("Sample Lead Time", 0),
        ("Lead Time", 7),
        ("Lead", 7),
    ]
    # one span, two sent casings -> one mention (`by_span` dedupe survives)
    same_span = collect_window("We stock ALUMINUM.", ["ALUMINUM", "aluminum"])
    assert len(same_span.mentions) == 1


def test_nesting_does_not_change_the_location_wire():
    """The restored mentions share their owner's snippet, so the distinct-snippet
    items — and therefore every mention_id and the request digest — are
    unchanged by decentralization. This is what lets stored location answers
    replay instead of re-dispatching."""
    c = collect_window(WINDOW, SENT)
    assert [i.mention for i in c.items] == [
        INTRO,
        "Sample Lead Time: 2 weeks.",
        "aluminum alloys ship daily.",
        "Lead-free solder only.",
        "Aluminum | Brass | Steel",
    ]


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
        # `lead` and `lead time` both start at "Lead Time" inside it; the
        # shorter span sorts first (bundles order by first mention's end).
        "lead",
        "lead time",
        "die casting",  # empties last, by key
    ]
    by_key = {b.key: b for b in result.bundles}
    assert by_key["aluminum"].forms == ("Aluminum", "aluminum")
    # group ids are the bundles' positions in words (2026-09-22), empties last
    assert [b.group_id for b in result.bundles] == [
        "record-one", "record-two", "record-three", "record-four", "record-five", "record-six",
    ]
    assert by_key["aluminum"].group_id == "record-one" and by_key["die casting"].group_id == "record-six"
    # `lead time` is no longer swallowed — it holds the mention nested in
    # "Sample Lead Time" (D8 reversed); only a form occurring NOWHERE is empty.
    assert by_key["lead time"].status == BUNDLE_STATUS_OK
    assert by_key["die casting"].status == BUNDLE_STATUS_NO_MENTIONS  # search false positive
    assert by_key["lead"].status == BUNDLE_STATUS_OK
    assert [m.record_id for m in by_key["aluminum"].mentions] == [
        record_id_for_phrase("Aluminum"),
        record_id_for_phrase("aluminum"),
        record_id_for_phrase("Aluminum"),
    ]


def test_golden_snippet_sharing_across_owners():
    """Both owners of one sentence share its snippet and mention_id — the
    identity the post-synthesis context join keys by (per bundle, per
    snippet), now that the fold itself carries no locations."""
    by_key = {b.key: b for b in _golden().bundles}
    aluminum, brass = by_key["aluminum"].mentions, by_key["brass"].mentions
    assert aluminum[0].snippet == brass[0].snippet == INTRO
    assert aluminum[0].mention_id == brass[0].mention_id


def test_golden_synthesis_snippets_are_distinct_and_skip_empty_bundles():
    result = _golden()
    by_key = {b.key: b for b in result.bundles}
    assert by_key["aluminum"].distinct_snippets() == [
        INTRO, "aluminum alloys ship daily.", "Aluminum | Brass | Steel",
    ]
    records = result.synthesis_records()
    assert [r.record_id for r in records] == [b.group_id for b in result.bundles if not b.is_empty]
    rendered = render_synthesis_record_blocks([r.model_dump() for r in records])
    assert "<<<RECORDS" in rendered and "RECORD_IDS" not in rendered and INTRO in rendered and "die casting" not in rendered


def test_synthesis_snippets_dedupe_repeated_lines_across_windows():
    footer = f"{SEP}\nhttps://acme.example/p1\n\nAluminum | Brass\n"
    result = fold_document(
        [WindowInput(footer, ["Aluminum"], window_id="a"), WindowInput(footer, ["Aluminum"], window_id="b")]
    )
    (b,) = result.bundles
    assert len(b.mentions) == 2  # both occurrences stay on the bundle
    assert len(b.distinct_snippets()) == 1  # synthesis sees the line once


# ---------------------------------------------------------------------------
# Collection rules
# ---------------------------------------------------------------------------


def test_casing_rescue_collects_unsent_casings_into_the_family_group():
    result = fold_document([WindowInput(WINDOW, ["Aluminum"])])
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


def test_every_nested_and_overlapping_span_is_collected():
    """D8 reversed 2026-08-27: a fragment inside a fuller span IS collected —
    enclosing span first, then the spans nested in it, then the next position."""
    c = collect_window(
        "Sample Lead Time is short. Lead Time-free zone.",
        ["Lead", "Lead Time", "Sample Lead Time"],
    )
    assert [m.form for m in c.mentions] == [
        "Sample Lead Time",
        "Lead Time",
        "Lead",
        "Lead Time",
        "Lead",
    ]
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
    a = WindowInput(f"{SEP}\nhttps://x/a\n\nAluminum here\n", ["Aluminum", " "])
    b = WindowInput(f"{SEP}\nhttps://x/b\n\nAluminum there\n", ["Brass"])
    result = fold_document([a, b])
    by_key = {x.key: x for x in result.bundles}
    assert len(by_key["aluminum"].mentions) == 1  # b's Aluminum was never sent for b
    assert by_key["brass"].is_empty
    assert fold_document([]).bundles == []


def test_verb_fold_is_a_dial_applied_to_the_keys():
    text = f"{SEP}\nhttps://x/a\n\nCNC milled parts; CNC milling too.\n"
    off = fold_document([WindowInput(text, ["CNC milled", "CNC milling"])], verb_fold=False)
    on = fold_document([WindowInput(text, ["CNC milled", "CNC milling"])], verb_fold=True)
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
        # no two mentions share a span (D8 reversed: nesting is allowed and
        # expected, but a SPAN is still collected exactly once)
        assert (m.start, m.end) not in spans
        spans.add((m.start, m.end))
    # items are the distinct snippets, in first-occurrence order, ids unique
    first_seen: list[str] = []
    for m in c.mentions:
        if m.snippet not in first_seen:
            first_seen.append(m.snippet)
    assert [i.mention for i in c.items] == first_seen
    assert len({i.mention_id for i in c.items}) == len(c.items)
    # D8 reversed: EVERY tier-2 hit of a sent form is now collected in its own
    # right — no hit is represented only by an enclosing span any more.
    for form, occs in scan.tier2.items():
        for o in occs:
            assert (o.start, o.end) in spans


@settings(max_examples=100, deadline=None)
@given(_text, _forms, st.booleans())
def test_fold_invariants(text, forms, verb_fold):
    result = fold_document([WindowInput(text, forms)], verb_fold=verb_fold)
    keys = [b.key for b in result.bundles]
    assert len(set(keys)) == len(keys)
    for b in result.bundles:
        assert all(normalize(f, verb_fold=verb_fold) == b.key for f in b.forms)
        assert list(b.mentions) == sorted(b.mentions)
        assert len(b.distinct_snippets()) == len({m.snippet for m in b.mentions})
    filled = [b for b in result.bundles if not b.is_empty]
    assert result.bundles[: len(filled)] == filled  # empties last
    assert [r.record_id for r in result.synthesis_records()] == [b.group_id for b in filled]


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
    result = fold_document([WindowInput(WINDOW, SENT)], snippet_radius=1)
    assert result.snippet_radius == 1
    assert fold_document([WindowInput(WINDOW, SENT)]).snippet_radius == 0


# ---------------------------------------------------------------------------
# Markdown-shaped text (2026-08-28, the scraper's markdown_v1 cutover): the
# clip is line-oriented and needs no mode flag — markers stay in snippets as
# location signals; decoration lines (pipe-table separator rows, dividers)
# are not units (module docstring, B)
# ---------------------------------------------------------------------------


def test_markdown_markers_stay_in_line_shaped_snippets():
    text = "## Certifications\n- Swiss machining\n| Haas | 3 |\n"
    assert _snippets(text, ["Certifications"], 0) == ["## Certifications"]
    assert _snippets(text, ["Swiss machining"], 0) == ["- Swiss machining"]
    assert _snippets(text, ["Haas"], 0) == ["| Haas | 3 |"]


def test_pipe_separator_row_is_not_a_unit_so_radius_reaches_the_header_row():
    table = "| Machine | Qty |\n|---|---|\n| Haas | 3 |\n| Mazak | 2 |\n"
    # radius 1 around the first DATA row: one unit up is the HEADER row — the
    # |---| separator between them consumes no radius (it is inside the slice,
    # a well-formed mini table, but not a unit)
    (snippet,) = _snippets(table, ["Haas"], 1)
    assert snippet == "| Machine | Qty |\n|---|---|\n| Haas | 3 |\n| Mazak | 2 |"


def test_divider_runs_in_legacy_text_are_not_units_either():
    text = "Steel plate stock.\n-----\nCut to size.\n"
    # shape-neutral: the same rule spares legacy divider lines from radius
    assert _snippets(text, ["Steel plate"], 1) == ["Steel plate stock.\n-----\nCut to size."]


def test_no_occurrence_ever_sits_on_a_decoration_line():
    # the scan matches word text; a separator row has none, so the unit lookup
    # (_unit_index_at) never has to answer for a decoration offset
    c = collect_window("| Alpha |\n|---|\n| Beta |\n", ["Alpha", "Beta"], snippet_radius=2)
    assert [m.snippet for m in c.mentions] == ["| Alpha |\n|---|\n| Beta |"] * 2


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
    tie = fold_document([WindowInput("We use steel. Steel is strong.", ["steel"])])
    (b,) = [b for b in tie.bundles if not b.is_empty]
    assert b.forms == ("Steel", "steel") and b.focal_form == "steel"
    # an empty bundle has no focal form and no record
    empty = by_key[normalize("die-casting")]
    assert empty.focal_form is None
    with pytest.raises(ValueError, match="no mentions"):
        empty.synthesis_record()


def test_synthesis_records_carry_the_focal_form_and_bare_snippets():
    records = _golden().synthesis_records()
    assert all(r.focal_form for r in records)
    first = records[0]
    assert first.focal_form == "Aluminum"
    assert first.snippets[0] == INTRO
    assert first.wire_dict()["snippets"][0] == INTRO
    # the wire dicts render (the synthesis request's two blocks)
    assert render_synthesis_record_blocks([r.wire_dict() for r in records])


# ---------------------------------------------------------------------------
# D21 — the compound collapse (2026-08-27). Sound only because mentions are
# decentralized: under containment a compound held its occurrences exclusively.
# ---------------------------------------------------------------------------

COORD_TEXT = (
    "We supply doors and frames to the trade.\n"
    "Our doors are steel. The frames are galvanized.\n"
)
COORD_FORMS = ["doors and frames", "doors", "frames"]


def _coord(**kwargs) -> FoldResult:
    return fold_document([WindowInput(COORD_TEXT, COORD_FORMS)], **kwargs)


def test_collapse_is_off_by_default():
    result = _coord()
    by_key = {b.key: b for b in result.bundles}
    assert by_key["door and frame"].status == BUNDLE_STATUS_OK
    assert by_key["door and frame"].collapsed_into == ()
    # off means the compound is still synthesized as its own record
    assert by_key["door and frame"].group_id in {
        r.record_id for r in result.synthesis_records()
    }


def test_collapse_skips_the_compound_and_keeps_it_visible():
    result = _coord(collapse_compounds=True)
    by_key = {b.key: b for b in result.bundles}
    compound = by_key["door and frame"]
    assert compound.status == BUNDLE_STATUS_COLLAPSED
    assert set(compound.collapsed_into) == {
        by_key["door"].group_id,
        by_key["frame"].group_id,
    }
    # kept in the fold, skipped by synthesis — the empty-bundle posture
    assert compound.mentions
    assert compound.group_id not in {r.record_id for r in result.synthesis_records()}
    assert {b.key for b in result.collapsed_bundles} == {"door and frame"}
    # the parts still carry the compound's evidence, so nothing is lost
    covered = {m.snippet for b in (by_key["door"], by_key["frame"]) for m in b.mentions}
    assert {m.snippet for m in compound.mentions} <= covered


def test_collapse_splits_the_surface_form_so_commas_survive_normalization():
    """`normalize` deletes commas, so an Oxford list must be split on the
    SURFACE form — splitting the key would mis-segment it as `oil gas`."""
    assert coordination_segments("Oil, Gas, and Petroleum") == ["Oil", "Gas", "Petroleum"]
    assert coordination_segments("Oil & Gas") == ["Oil", "Gas"]
    assert normalize("Oil, Gas, and Petroleum") == "oil gas and petroleum"
    # not coordinations
    assert coordination_segments("stainless steel") is None
    assert coordination_segments("and") is None
    assert coordination_segments("Design/Build") is None


def test_g2_a_part_that_is_not_a_sibling_group_blocks_the_collapse():
    """Shared-head coordination: "commercial and institutional buildings" splits
    to `commercial` | `institutional building`, and bare `commercial` is not a
    group — so the compound survives."""
    text = "We serve commercial and institutional buildings and institutional buildings alike."
    result = fold_document(
        [WindowInput(text, ["commercial and institutional buildings", "institutional buildings"])],
        collapse_compounds=True,
    )
    by_key = {b.key: b for b in result.bundles}
    assert by_key["commercial and institutional building"].status == BUNDLE_STATUS_OK


def test_g4_a_part_with_no_standing_outside_the_compound_blocks_the_collapse():
    """`fire rated door` occurs ONLY inside "fire rated doors and frames", so it
    is a fragment, not a sibling entity — the measured steelcraft case."""
    text = "We list fire rated doors and frames.\nOur frames ship daily.\n"
    result = fold_document(
        [WindowInput(text, ["fire rated doors and frames", "fire rated doors", "frames"])],
        collapse_compounds=True,
    )
    by_key = {b.key: b for b in result.bundles}
    assert by_key["fire rated door and frame"].status == BUNDLE_STATUS_OK
    assert by_key["fire rated door and frame"].collapsed_into == ()


def test_g3_a_compound_holding_evidence_no_part_covers_blocks_the_collapse():
    """The parts' groups exist by KEY but none of their surface forms occurs
    inside the compound's occurrence — the plurals `doors`/`frames` do not
    match the singular "Door and Frame" — so the compound holds a snippet no
    part covers and collapsing it would destroy evidence."""
    text = "We supply Door and Frame units.\ndoors and frames are stocked.\n"
    result = fold_document(
        [WindowInput(text, ["Door and Frame", "doors", "frames"])],
        collapse_compounds=True,
    )
    by_key = {b.key: b for b in result.bundles}
    compound = by_key["door and frame"]
    covered = {m.snippet for b in (by_key["door"], by_key["frame"]) for m in b.mentions}
    assert {m.snippet for m in compound.mentions} - covered  # uncovered evidence
    assert compound.status == BUNDLE_STATUS_OK
    assert compound.collapsed_into == ()


def test_collapse_is_recorded_as_run_identity_on_the_result():
    assert _coord(collapse_compounds=True).collapse_compounds is True
    assert _coord().collapse_compounds is False
