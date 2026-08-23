"""Phase 2.2 of pipeline v3 (PIPELINE_V3_PLAN.md D7): the word-boundary matcher,
the masked scan domain, page geometry, and the two scan tiers."""

import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from core.utils.floor_scan import (
    CONTINUED_PAGE_MARKER,
    EXCLUDED_PAGE_MARKER,
    continued_page_header,
    excluded_page_spans,
    is_excluded_page,
    mask_excluded_pages,
    omit_excluded_pages,
    scan_domain,
    wire_window_text,
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
    assert [s.url for s in spans] == ["https://acme.example/materials/lead", "https://acme.example/about"]
    assert spans[0].start == 0 and spans[-1].end == len(WINDOW)
    for a, b in zip(spans, spans[1:]):
        assert a.end == b.start
    assert page_at(spans, WINDOW.index("Sample Lead Time")) == "https://acme.example/materials/lead"
    assert page_at(spans, WINDOW.index("304SS")) == "https://acme.example/about"
    # the separator line opens the page (N1)
    assert page_at(spans, 0) == "https://acme.example/materials/lead"


def test_mid_page_start_inherits_the_preceding_page_when_given():
    tail = "continued text here.\n" + SEP + "\nhttps://acme.example/next\n\nmore.\n"
    # A page starts at its separator line when one directly precedes its URL
    # line (2026-08-22, N1: page-aligned windows open on the separator), so the
    # inherited head ends there.
    assert page_spans(tail)[0] == PageSpan(None, 0, tail.index(SEP))
    assert page_spans(tail, preceding_page="https://acme.example/prev")[0].url == "https://acme.example/prev"
    # a bare URL line (no separator) still opens a page at the URL line
    bare = "head.\nhttps://acme.example/bare\n\nbody.\n"
    assert page_spans(bare) == [
        PageSpan(None, 0, 6),
        PageSpan("https://acme.example/bare", 6, len(bare)),
    ]
    # a separator NOT directly followed by a URL line is body, not a header
    loose = SEP + "\n\nhttps://acme.example/x\nbody\n"
    assert page_spans(loose)[0] == PageSpan(None, 0, len(SEP) + 2)


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


# ---------------------------------------------------------------------------
# Excluded pages (2026-08-22, proposal P3): legal boilerplate is not harvest
# material — masked from the scan, omitted from the wire, by one URL rule.
# ---------------------------------------------------------------------------

SEP = "#" * 50
LEGAL = (
    f"{SEP}\nhttps://acme.example/about\n\nWe machine Aluminum.\n"
    f"{SEP}\nhttps://www.acme.example/en/privacy-policy.html\n\nWe process orders and cookies. Aluminum too.\n"
    f"{SEP}\nhttps://acme.example/products\n\nAluminum parts.\n"
)


@pytest.mark.parametrize(
    "url, excluded",
    [
        ("https://www.steelcraft.com/en/privacy-policy.html", True),
        ("https://alecmfg.com/terms-and-conditions/", True),
        ("https://alecmfg.com/cookie-policy-eu/", True),
        ("https://alecmfg.com/wp-content/plugins/complianz-terms-conditions/download.php", True),
        ("https://acme.example/legal-notice", True),
        ("https://acme.example/impressum", True),
        ("https://acme.example/quality-policy", False),  # 'policy' alone is not a legal page
        ("https://termsmfg.com/products", False),  # the host is never matched
        ("https://acme.example/", False),
        (None, False),
    ],
)
def test_excluded_page_rule_matches_the_path_only(url, excluded):
    assert is_excluded_page(url) is excluded


def test_excluded_pages_are_masked_from_the_scan_and_reported():
    (span,) = excluded_page_spans(LEGAL)
    assert span.url == "https://www.acme.example/en/privacy-policy.html"
    assert LEGAL[span.start:span.end].startswith(
        f"{SEP}\nhttps://www.acme.example/en/privacy-policy.html"
    )
    assert len(mask_excluded_pages(LEGAL)) == len(LEGAL) and len(scan_domain(LEGAL)) == len(LEGAL)
    scan = floor_scan(LEGAL, ["Aluminum"])
    assert [scan.page_of(o) for o in scan.tier1["Aluminum"]] == [
        "https://acme.example/about", "https://acme.example/products",
    ]


def test_excluded_pages_are_omitted_from_the_wire_but_their_url_line_stays():
    wire = omit_excluded_pages(LEGAL)
    assert "cookies" not in wire and EXCLUDED_PAGE_MARKER in wire
    privacy_header = f"{SEP}\nhttps://www.acme.example/en/privacy-policy.html\n"
    assert privacy_header + EXCLUDED_PAGE_MARKER in wire
    assert "We machine Aluminum." in wire and "Aluminum parts." in wire
    assert omit_excluded_pages("no pages at all") == "no pages at all"
    # a bare-URL excluded page keeps its URL line
    bare = "https://acme.example/privacy\n\ncookies\nhttps://acme.example/ok\n\nbody\n"
    assert omit_excluded_pages(bare) == (
        f"https://acme.example/privacy\n{EXCLUDED_PAGE_MARKER}\nhttps://acme.example/ok\n\nbody\n"
    )


def test_window_head_inherited_from_an_excluded_page_is_excluded_too():
    text = "tail of the policy with Aluminum\n" + f"{SEP}\nhttps://acme.example/x\n\nAluminum body\n"
    scan = floor_scan(text, ["Aluminum"], preceding_page="https://acme.example/privacy")
    assert len(scan.tier1["Aluminum"]) == 1
    wire = omit_excluded_pages(text, preceding_page="https://acme.example/privacy")
    assert wire.startswith(EXCLUDED_PAGE_MARKER) and "tail of the policy" not in wire
    # wire_window_text derives the inherited page from the subject text itself
    subject = f"{SEP}\nhttps://acme.example/privacy\n\nlegal\n" + text
    start = subject.index("tail of the policy")
    assert wire_window_text(subject, start, len(subject)) == wire


# ---------------------------------------------------------------------------
# 2026-08-22 (N1): page-aligned windows and the continued-page header
# ---------------------------------------------------------------------------

PRIVACY = f"{SEP}\nhttps://acme.example/privacy\n\nlegal\n"
ABOUT = f"{SEP}\nhttps://acme.example/about\n\nfirst half.\nsecond half with Aluminum.\n"
PAGE_X = f"{SEP}\nhttps://acme.example/x\n\nbody\n"


def test_a_window_opening_on_a_separator_after_an_excluded_page_gets_no_spurious_marker():
    # Before N1 the separator line belonged to the PRECEDING page, so a window
    # that opened on one after an excluded page would have started with the
    # excluded-page marker for that single line.
    subject = PRIVACY + PAGE_X
    start = subject.index(SEP, 1)
    wire = wire_window_text(subject, start, len(subject))
    assert wire == subject[start:]
    assert EXCLUDED_PAGE_MARKER not in wire and CONTINUED_PAGE_MARKER not in wire


def test_a_mid_page_window_is_announced_with_the_inherited_page_header():
    subject = ABOUT + PAGE_X
    start = subject.index("second half")
    wire = wire_window_text(subject, start, len(subject))
    assert wire == continued_page_header("https://acme.example/about") + subject[start:]
    assert wire.startswith(
        f"{SEP}\nhttps://acme.example/about\n{CONTINUED_PAGE_MARKER}\n\nsecond half"
    )
    # the scan and the fold never see it: the window text is untouched
    scan = floor_scan(subject[start:], ["Aluminum"], preceding_page="https://acme.example/about")
    assert scan.page_of(scan.tier1["Aluminum"][0]) == "https://acme.example/about"


def test_no_header_is_injected_when_the_inherited_page_is_unknown_or_excluded_or_blank():
    # unknown: the document has no header before the window
    subject = "no header at all.\nmore text.\n"
    assert wire_window_text(subject, subject.index("more"), len(subject)) == "more text.\n"
    # excluded: omitted with its page (tested above), never announced
    subject = f"{SEP}\nhttps://acme.example/privacy\n\nlegal one.\nlegal two.\n" + PAGE_X
    wire = wire_window_text(subject, subject.index("legal two"), len(subject))
    assert wire.startswith(EXCLUDED_PAGE_MARKER) and CONTINUED_PAGE_MARKER not in wire
    # blank head: a window that opens on blank lines before a header
    subject = f"{SEP}\nhttps://acme.example/about\n\nbody\n\n\n" + PAGE_X
    start = subject.index("\n\n\n") + 1
    assert CONTINUED_PAGE_MARKER not in wire_window_text(subject, start, len(subject))


# --- excluded pages dropped BEFORE chunking (2026-08-23) -------------------------


def test_drop_excluded_pages_removes_the_whole_page_header_included():
    from core.utils.floor_scan import PAGE_EXCLUSION_VERSION, drop_excluded_pages

    result = drop_excluded_pages(LEGAL)
    assert result.version == PAGE_EXCLUSION_VERSION == "1"
    assert result.text == (
        f"{SEP}\nhttps://acme.example/about\n\nWe machine Aluminum.\n"
        f"{SEP}\nhttps://acme.example/products\n\nAluminum parts.\n"
    )
    assert "privacy-policy" not in result.text and EXCLUDED_PAGE_MARKER not in result.text
    (page,) = result.dropped
    assert page.url == "https://www.acme.example/en/privacy-policy.html"
    assert LEGAL[page.start : page.end].startswith(f"{SEP}\nhttps://www.acme.example/en/privacy-policy.html")
    assert result.chars_before == len(LEGAL)
    assert result.chars_removed == page.chars == page.end - page.start
    assert result.chars_after == len(result.text) == len(LEGAL) - page.chars
    # the trimmed text is stable under a second pass (no page is excluded twice)
    assert drop_excluded_pages(result.text).text == result.text
    assert drop_excluded_pages(result.text).dropped == ()


def test_drop_excluded_pages_keeps_text_untouched_when_nothing_is_excluded():
    from core.utils.floor_scan import drop_excluded_pages

    clean = f"{SEP}\nhttps://acme.example/about\n\nbody\n"
    result = drop_excluded_pages(clean)
    assert result.text is clean and result.dropped == () and result.chars_removed == 0
    # text before the first page header (unknown page) is never dropped
    headless = "orphan head line\n" + clean
    assert drop_excluded_pages(headless).text == headless


def test_drop_excluded_pages_handles_first_last_and_bare_url_pages():
    from core.utils.floor_scan import drop_excluded_pages

    text = (
        f"{SEP}\nhttps://acme.example/privacy\n\ncookies first\n"
        "https://acme.example/ok\n\nbody\n"  # bare-URL page, kept
        f"{SEP}\nhttps://acme.example/terms-of-use\n\nlegal last\n"
    )
    result = drop_excluded_pages(text)
    assert result.text == "https://acme.example/ok\n\nbody\n"
    assert [p.url for p in result.dropped] == [
        "https://acme.example/privacy", "https://acme.example/terms-of-use",
    ]
    dump = result.to_dump()
    assert dump["version"] == "1" and dump["chars_after"] == len(result.text)
    assert [p["url"] for p in dump["pages"]] == [p.url for p in result.dropped]
    assert dump["chars_before"] - dump["chars_removed"] == dump["chars_after"]
