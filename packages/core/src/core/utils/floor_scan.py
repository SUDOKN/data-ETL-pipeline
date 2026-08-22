"""The mechanical floor under mention collection (PIPELINE_V3_PLAN.md D7), and the
word-boundary occurrence matcher it is built on (also D8's re-attribution engine).

The LLM mention collector reports occurrences of sent forms in a window of
scraped text. Nothing about an LLM guarantees it reports all of them — v1
satisficed at ~1.7 mentions per phrase however many contexts existed — so every
window is also scanned mechanically, and the two are compared:

TIER 1 (HOLD): exact-case, whole-word occurrences of each sent form. Every hit
must be accounted for in the LLM's mentions or the window flags a discrepancy.
This is the satisficing tripwire, at zero ground-truth cost.

TIER 2 (DISCOVERY): the same scan case-insensitively. Hits tier 1 did not see
are casings (or near-forms) search never emitted — they feed the missed-form
surface. Short forms are the exception: ``Al``, ``SS``, ``ms`` lowercase are
English noise, so forms of ``SHORT_FORM_MAX_LENGTH`` characters or fewer stay
case-sensitive in tier 2 and are reported as flagged.

WORD BOUNDARIES, ALWAYS. Substring matching is the measured ``Lead`` failure
(52 hits, 0 real — ``leading``, ``Leader``). The rule here is tuned for
technical terms rather than prose: a guard is placed at an edge of the form
only when that edge IS a word character, so ``6061-T6`` matches in
"6061-T6 aluminum" and not in "16061-T6", ``CNC`` matches in "CNC/Manual",
``C++`` needs no right guard, and ``Lead`` matches "Lead Time" and "Lead-free"
but never "Leader". ``\\w`` is Unicode-aware, so accented letters are word
characters too.

SCAN DOMAIN = WINDOW TEXT MINUS PAGE HEADERS (user decision 2026-08-21, D2/D7
domain notes). The scraper renders every page as a ``#`` separator line, the
bare URL on its own line, a blank line, then the content; neither search nor
the collector treats those header lines as text, so the scan must not either or
every path hit is a phantom discrepancy. Headers are blanked with spaces,
LENGTH-PRESERVING, so every offset this module reports is an offset into the
original window — page attribution and snippet positions depend on that.

Everything here is pure and deterministic; the Phase 3 node decides what a
discrepancy does.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Optional

SHORT_FORM_MAX_LENGTH = 3  # D7's own number; settled 2.2

# A line that is nothing but a URL: the scraper's page header.
_URL_LINE_RE = re.compile(r"^[ \t]*https?://\S+[ \t]*$", re.MULTILINE)
# The scraper's page separator: a line of (at least ten) '#'.
_SEPARATOR_LINE_RE = re.compile(r"^[ \t]*#{10,}[ \t]*$", re.MULTILINE)


# --- scan domain + pages -------------------------------------------------------


def _blank(text: str, spans: Iterable[tuple[int, int]]) -> str:
    chars = list(text)
    for start, end in spans:
        for i in range(start, end):
            if chars[i] not in "\r\n":
                chars[i] = " "
    return "".join(chars)


def mask_page_headers(text: str) -> str:
    """*text* with every URL line and separator line replaced by spaces of the
    same length (newlines kept). ``len(result) == len(text)`` always."""
    spans = [m.span() for m in _URL_LINE_RE.finditer(text)]
    spans += [m.span() for m in _SEPARATOR_LINE_RE.finditer(text)]
    return _blank(text, spans)


@dataclass(frozen=True)
class PageSpan:
    """The stretch of a window that belongs to one page. ``url`` is None for
    text before the first URL line (a sub-window that starts mid-page) unless
    the caller supplied the inherited page."""

    url: Optional[str]
    start: int
    end: int


def page_spans(text: str, *, preceding_page: Optional[str] = None) -> list[PageSpan]:
    """Pages of *text*, in order, tiling ``[0, len(text))``. A page starts at
    its URL line and runs to the next URL line (or the end)."""
    urls = [(m.start(), m.group().strip()) for m in _URL_LINE_RE.finditer(text)]
    spans: list[PageSpan] = []
    cursor = 0
    current = preceding_page
    for start, url in urls:
        if start > cursor or (start == 0 and not spans and current is not None):
            spans.append(PageSpan(current, cursor, start))
        elif start > 0 and not spans:
            spans.append(PageSpan(current, cursor, start))
        cursor, current = start, url
    if cursor < len(text) or not spans:
        spans.append(PageSpan(current, cursor, len(text)))
    return spans


def preceding_page_of(text: str, offset: int) -> Optional[str]:
    """The url of the last URL line that starts before *offset* in *text*, or
    None — the page a sub-window cut at *offset* inherits (pass it as
    ``preceding_page``). Sub-window bounds respect line boundaries, so *offset*
    is a line start and never splits a URL line."""
    last: Optional[str] = None
    for match in _URL_LINE_RE.finditer(text, 0, offset):
        last = match.group().strip()
    return last


def page_at(spans: list[PageSpan], offset: int) -> Optional[str]:
    """The url of the page holding *offset*, or None when unknown."""
    for span in spans:
        if span.start <= offset < span.end:
            return span.url
    return spans[-1].url if spans and offset >= spans[-1].end else None


# --- occurrences -------------------------------------------------------------


@dataclass(frozen=True, order=True)
class Occurrence:
    start: int
    end: int


def _is_word_char(ch: str) -> bool:
    return bool(re.match(r"\w", ch))


def form_pattern(form: str, *, case_sensitive: bool) -> re.Pattern[str]:
    """The compiled whole-word pattern for *form* (see module docstring)."""
    left = r"(?<!\w)" if _is_word_char(form[0]) else ""
    right = r"(?!\w)" if _is_word_char(form[-1]) else ""
    flags = 0 if case_sensitive else re.IGNORECASE
    return re.compile(left + re.escape(form) + right, flags)


def find_form_occurrences(
    text: str, form: str, *, case_sensitive: bool = True
) -> list[Occurrence]:
    """Non-overlapping whole-word occurrences of *form* in *text*, in order.
    The empty form occurs nowhere."""
    if not form:
        return []
    return [Occurrence(m.start(), m.end()) for m in form_pattern(form, case_sensitive=case_sensitive).finditer(text)]


def is_short_form(form: str) -> bool:
    return len(form) <= SHORT_FORM_MAX_LENGTH


# --- the scan ----------------------------------------------------------------


@dataclass
class FloorScan:
    """What the mechanical scan found in one window. ``tier1`` and ``tier2`` are
    keyed by sent form, in sent order; tier 2 ⊇ tier 1 per form, and equals it
    for short forms (listed in ``short_forms``). ``pages`` is the window's page
    geometry for attributing any offset."""

    tier1: dict[str, list[Occurrence]] = field(default_factory=dict)
    tier2: dict[str, list[Occurrence]] = field(default_factory=dict)
    short_forms: list[str] = field(default_factory=list)
    pages: list[PageSpan] = field(default_factory=list)

    def page_of(self, occurrence: Occurrence) -> Optional[str]:
        return page_at(self.pages, occurrence.start)


def floor_scan(
    window_text: str,
    forms: Iterable[str],
    *,
    preceding_page: Optional[str] = None,
) -> FloorScan:
    """Scan *window_text* for every form, both tiers, over the masked domain."""
    domain = mask_page_headers(window_text)
    scan = FloorScan(pages=page_spans(window_text, preceding_page=preceding_page))
    for form in forms:
        if form in scan.tier1:
            continue  # forms are a set on the wire; a repeat adds nothing
        exact = find_form_occurrences(domain, form, case_sensitive=True)
        scan.tier1[form] = exact
        if is_short_form(form):
            scan.short_forms.append(form)
            scan.tier2[form] = list(exact)
        else:
            scan.tier2[form] = find_form_occurrences(domain, form, case_sensitive=False)
    return scan
