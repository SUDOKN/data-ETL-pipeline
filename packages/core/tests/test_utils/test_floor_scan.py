"""Phase 2.2 of pipeline v3 (PIPELINE_V3_PLAN.md D7): the word-boundary matcher,
the masked scan domain, page geometry, and the two scan tiers."""

import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from core.utils.floor_scan import (
    SHORT_FORM_MAX_LENGTH,
    Occurrence,
    PageSpan,
    find_form_occurrences,
    floor_scan,
    is_short_form,
    mask_page_headers,
    page_at,
    page_spans,
)

SEP = "#" * 50
WINDOW = (
    f"{SEP}\n"
    "https://acme.example/materials/lead\n"
    "\n"
    "Sample Lead Time: 2 weeks. We machine Lead-free solder and lead alloys.\n"
    "Leader in leading edge work. 6061-T6 aluminum, not 16061-T6.\n"
    "CNC/Manual Machining; C++ tooling; Al 6061 vs AL and alloy. SS 304 steel.\n"
    f"{SEP}\n"
    "https://acme.example/about\n"
    "\n"
    "304SS stainless. Lead paint removed. Visit /materials/lead for more.\n"
)


# ---------------------------------------------------------------------------
# Word boundaries for technical terms
# ---------------------------------------------------------------------------


def _hits(text, form, cs=True):
    return [text[o.start:o.end] for o in find_form_occurrences(text, form, case_sensitive=cs)]


def test_lead_never_matches_leader_or_leading():
    text = "Lead Time. Lead-free. Leader. leading. lead."
    assert _hits(text, "Lead") == ["Lead", "Lead"]
    assert _hits(text, "Lead", cs=False) == ["Lead", "Lead", "lead"]


@pytest.mark.parametrize(
    "text, form, expected",
    [
        ("6061-T6 aluminum, not 16061-T6.", "6061-T6", 1),
        ("CNC/Manual Machining", "CNC", 1),
        ("CNC/Manual Machining", "Manual Machining", 1),
        ("C++ tooling and C+++", "C++", 2),   # no right guard: the form ends in '+'
        ("304SS stainless and SS 304", "SS", 1),  # 304SS: preceded by a word char
        ("steel. steel, steel; (steel)", "steel", 4),
        ("manufacturer's parts", "manufacturer", 1),
        ("Aluminum Alloy", "Al", 0),
        ("Al 6061 and Al-Mg", "Al", 2),
        ("façade façades", "façade", 1),      # Unicode word chars guard too
        ("", "steel", 0),
        ("steel", "", 0),
    ],
)
def test_boundary_rules(text, form, expected):
    assert len(find_form_occurrences(text, form)) == expected


def test_occurrences_are_non_overlapping_and_in_order():
    assert find_form_occurrences("a a a a", "a a") == [Occurrence(0, 3), Occurrence(4, 7)]


# ---------------------------------------------------------------------------
# Scan domain: page headers are not text
# ---------------------------------------------------------------------------


def test_mask_preserves_length_and_offsets():
    masked = mask_page_headers(WINDOW)
    assert len(masked) == len(WINDOW)
    assert "https://" not in masked and "#" not in masked
    # Every non-header character sits at the same offset.
    i = WINDOW.index("Sample Lead Time")
    assert masked[i : i + len("Sample Lead Time")] == "Sample Lead Time"
    assert masked.count("\n") == WINDOW.count("\n")


def test_a_form_that_occurs_only_in_a_url_line_has_no_hits():
    window = f"{SEP}\nhttps://acme.example/services/die-casting\n\nWe do other things.\n"
    scan = floor_scan(window, ["die-casting", "casting", "acme"])
    assert scan.tier1 == {"die-casting": [], "casting": [], "acme": []}
    assert scan.tier2 == scan.tier1


def test_path_like_text_in_the_body_is_still_text():
    scan = floor_scan(WINDOW, ["/materials/lead"])
    # The body line "Visit /materials/lead for more." counts; the URL line does not.
    assert len(scan.tier1["/materials/lead"]) == 1


# ---------------------------------------------------------------------------
# Page geometry
# ---------------------------------------------------------------------------


def test_page_spans_tile_the_window_and_attribute_offsets():
    spans = page_spans(WINDOW)
    assert [s.url for s in spans] == [None, "https://acme.example/materials/lead", "https://acme.example/about"]
    assert spans[0].start == 0 and spans[-1].end == len(WINDOW)
    for a, b in zip(spans, spans[1:]):
        assert a.end == b.start
    assert page_at(spans, WINDOW.index("Sample Lead Time")) == "https://acme.example/materials/lead"
    assert page_at(spans, WINDOW.index("304SS")) == "https://acme.example/about"
    assert page_at(spans, 0) is None  # the separator line precedes the first URL


def test_mid_page_start_inherits_the_preceding_page_when_given():
    tail = "continued text here.\n" + SEP + "\nhttps://acme.example/next\n\nmore.\n"
    # A page starts at its URL line; the separator line before it still belongs
    # to the preceding span (it is masked out of the scan domain either way).
    assert page_spans(tail)[0] == PageSpan(None, 0, tail.index("https://"))
    assert page_spans(tail, preceding_page="https://acme.example/prev")[0].url == "https://acme.example/prev"


def test_window_without_any_url_is_one_page_of_unknown_or_inherited():
    assert page_spans("just text") == [PageSpan(None, 0, 9)]
    assert page_spans("just text", preceding_page="p")[0].url == "p"
    assert page_spans("") == [PageSpan(None, 0, 0)]


# ---------------------------------------------------------------------------
# The two tiers and the short-form policy
# ---------------------------------------------------------------------------


def test_tiers_on_the_window():
    scan = floor_scan(WINDOW, ["Lead", "lead", "Al", "SS", "aluminum", "Leader"])
    t1 = {f: len(v) for f, v in scan.tier1.items()}
    t2 = {f: len(v) for f, v in scan.tier2.items()}
    # "lead" exact: "lead alloys" and the BODY path "/materials/lead" (a word after '/').
    assert t1 == {"Lead": 3, "lead": 2, "Al": 1, "SS": 1, "aluminum": 1, "Leader": 1}
    # Tier 2 adds the other casings for long forms only: 3 + 2 either way.
    assert t2["Lead"] == 5 and t2["lead"] == 5
    assert t2["Al"] == 1 and t2["SS"] == 1  # AL, 304SS never join: short forms stay exact
    assert scan.short_forms == ["Al", "SS"]
    assert scan.page_of(scan.tier1["Lead"][0]) == "https://acme.example/materials/lead"
    assert scan.page_of(scan.tier1["Lead"][2]) == "https://acme.example/about"


def test_short_form_threshold():
    assert SHORT_FORM_MAX_LENGTH == 3
    assert is_short_form("SS") and is_short_form("CNC") and not is_short_form("HVAC")


def test_repeated_forms_are_scanned_once_and_order_is_sent_order():
    scan = floor_scan("a b c", ["c", "a", "c"])
    assert list(scan.tier1) == ["c", "a"]


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

_letters = st.text(alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), max_codepoint=0x24F), min_size=1, max_size=8)
_token = st.one_of(_letters, st.sampled_from(["6061-T6", "C++", "CNC/Manual", "Lead-free", "304SS", "Al"]))
_text = st.lists(_token, min_size=0, max_size=12).map(" ".join)


@settings(max_examples=300, deadline=None)
@given(_text, _token)
def test_every_hit_is_a_whole_word_exact_substring(text, form):
    for o in find_form_occurrences(text, form):
        assert text[o.start:o.end] == form
        if re.match(r"\w", form[0]) and o.start > 0:
            assert not re.match(r"\w", text[o.start - 1])
        if re.match(r"\w", form[-1]) and o.end < len(text):
            assert not re.match(r"\w", text[o.end])


@settings(max_examples=300, deadline=None)
@given(_text, _token)
def test_tier2_contains_tier1_and_matches_casefold(text, form):
    scan = floor_scan(text, [form])
    t1, t2 = scan.tier1[form], scan.tier2[form]
    assert set(t1) <= set(t2)
    for o in t2:
        assert text[o.start:o.end].casefold() == form.casefold()
    if is_short_form(form):
        assert t1 == t2
    assert t2 == sorted(t2)
    for a, b in zip(t2, t2[1:]):
        assert a.end <= b.start


@settings(max_examples=200, deadline=None)
@given(_text, _text, _token)
def test_a_form_surrounded_by_spaces_is_always_found(before, after, form):
    text = f"{before} {form} {after}"
    assert any(text[o.start:o.end] == form for o in find_form_occurrences(text, form))


@settings(max_examples=200, deadline=None)
@given(st.text(max_size=200))
def test_mask_is_length_preserving_and_page_spans_tile(text):
    assert len(mask_page_headers(text)) == len(text)
    spans = page_spans(text)
    assert spans[0].start == 0 and spans[-1].end == len(text)
    for a, b in zip(spans, spans[1:]):
        assert a.end == b.start
