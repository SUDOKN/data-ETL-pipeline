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

SCAN DOMAIN = WINDOW TEXT MINUS PAGE HEADERS MINUS EXCLUDED PAGES (user
decisions 2026-08-21 and 2026-08-22, D2/D7 domain notes). The scraper renders
every page as a ``#`` separator line, the bare URL on its own line, a blank
line, then the content; neither search nor the collector treats those header
lines as text, so the scan must not either or every path hit is a phantom
discrepancy. EXCLUDED PAGES (2026-08-22, proposal P3 accepted): pages whose URL
path says they are legal boilerplate — privacy, cookie, terms, legal,
disclaimer, gdpr, imprint — are not harvest material at all: on run
20260822T195947 steelcraft's privacy policy alone (35k chars) put 103 forms
into the fold that occur on no other page and cost ~80k prompt tokens. Those
pages are masked here (so no mention is ever collected on them) and OMITTED
from the text the search and location requests send (``omit_excluded_pages``),
both from the one URL rule. Headers and excluded pages are blanked with spaces,
LENGTH-PRESERVING, so every offset this module reports is an offset into the
original window — page attribution and snippet positions depend on that.
PAGE ALIGNMENT (2026-08-22, proposal N1 accepted): a page starts at its
separator line (``is_page_header_line``), the chunkers close chunks and
sub-windows before one (``ChunkingStrategy.align_to_page_headers``), so a
window opens mid-page only when a single page outgrows the limit — and then
``wire_window_text`` announces the inherited page (``continued_page_header``).

Everything here is pure and deterministic; the Phase 3 node decides what a
discrepancy does.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

SHORT_FORM_MAX_LENGTH = 3  # D7's own number; settled 2.2

# A line that is nothing but a URL: the scraper's page header.
_URL_LINE_RE = re.compile(r"^[ \t]*https?://\S+[ \t]*$", re.MULTILINE)
# The scraper's page separator: a line of (at least ten) '#'.
_SEPARATOR_LINE_RE = re.compile(r"^[ \t]*#{10,}[ \t]*$", re.MULTILINE)


def is_page_header_line(line: str) -> bool:
    """Whether *line* (a raw line, line ending included) is the separator line
    the scraper writes at the top of every page — the line a page-aligned
    chunk starts on (``ChunkingStrategy.align_to_page_headers``; 2026-08-22,
    proposal N1)."""
    return bool(_SEPARATOR_LINE_RE.fullmatch(line.rstrip("\r\n")))


def is_page_barrier_line(line: str) -> bool:
    """Whether *line* (a raw line, line ending included) is a page BOUNDARY
    line — the scraper's separator or a URL-only page header. Neither is text:
    the scan masks both (``mask_page_headers``), and the snippet clip with a
    radius > 0 (``aggregation_fold``, user knob 2026-08-22) never extends a
    snippet across one, so context never leaks in from a neighbouring page."""
    stripped = line.rstrip("\r\n")
    return bool(_SEPARATOR_LINE_RE.fullmatch(stripped) or _URL_LINE_RE.fullmatch(stripped))


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


# --- excluded pages ---------------------------------------------------------------

# Matched against the URL's PATH (host excluded): a manufacturer's domain may
# carry any word, its legal pages carry these.
_EXCLUDED_PAGE_PATH_RE = re.compile(
    r"privacy|cookie|terms|legal|disclaimer|gdpr|imprint|impressum", re.IGNORECASE
)
EXCLUDED_PAGE_MARKER = "[page content omitted]"
# Identity of the exclusion RULE as a text-preparation step (2026-08-23): the phrase
# pipelines drop excluded pages from the text BEFORE chunking (see
# ``drop_excluded_pages``), so chunk and window bounds are offsets into the
# trimmed text. Bump when ``_EXCLUDED_PAGE_PATH_RE`` or the drop semantics change:
# the version is stored on ``ChunkingStrategy`` and a mismatch fails the resume.
PAGE_EXCLUSION_VERSION = "1"


def _url_path(url: str) -> str:
    without_scheme = url.split("://", 1)[-1]
    slash = without_scheme.find("/")
    return "" if slash == -1 else without_scheme[slash:]


def is_excluded_page(url: Optional[str]) -> bool:
    """Whether the page at *url* is legal boilerplate the pipeline excludes
    (see the module docstring). None — an unknown page — is never excluded."""
    return bool(url) and bool(_EXCLUDED_PAGE_PATH_RE.search(_url_path(url or "")))


def excluded_page_spans(
    text: str, *, preceding_page: Optional[str] = None
) -> list["PageSpan"]:
    """The stretches of *text* that belong to excluded pages — including a
    window head that inherits an excluded *preceding_page*."""
    return [span for span in page_spans(text, preceding_page=preceding_page) if is_excluded_page(span.url)]


def mask_excluded_pages(text: str, *, preceding_page: Optional[str] = None) -> str:
    """*text* with every excluded page blanked to spaces, length-preserving
    (newlines kept); the page's own URL line is blanked with it."""
    return _blank(text, [(s.start, s.end) for s in excluded_page_spans(text, preceding_page=preceding_page)])


@dataclass(frozen=True)
class DroppedPage:
    """One excluded page removed by ``drop_excluded_pages``; offsets are into
    the ORIGINAL text, ``chars`` is its length including its header lines."""

    url: str
    start: int
    end: int

    @property
    def chars(self) -> int:
        return self.end - self.start


@dataclass(frozen=True)
class PageExclusion:
    """The outcome of ``drop_excluded_pages`` over one subject's text: what the
    phrase pipelines chunk (``text``) and what was taken out of it."""

    version: str
    text: str
    dropped: tuple[DroppedPage, ...]
    chars_before: int

    @property
    def chars_removed(self) -> int:
        return sum(page.chars for page in self.dropped)

    @property
    def chars_after(self) -> int:
        return len(self.text)

    def to_dump(self) -> dict[str, Any]:
        """The dump's ``run.scraped_text.excluded_pages`` block: the rule version
        and every page removed, so a too-broad rule stays visible."""
        return {
            "version": self.version,
            "chars_before": self.chars_before,
            "chars_removed": self.chars_removed,
            "chars_after": self.chars_after,
            "pages": [
                {"url": p.url, "start": p.start, "end": p.end, "chars": p.chars}
                for p in self.dropped
            ],
        }


def drop_excluded_pages(text: str) -> PageExclusion:
    """*text* with every excluded page REMOVED outright — its separator line, its
    URL line and its body, no marker left behind — for the phrase pipelines to
    chunk (2026-08-23, user decision). Measured on run 20260823T034518: with the
    pages merely blanked on the wire, steelcraft's privacy policy (6,585 tok,
    oversize) and terms of use still consumed budget, and page alignment left a
    third of chunk 1 idle because the privacy page would not fit after it — 47%
    of the 40k-token budget produced nothing, real-content coverage 80% → 64%.
    Removing the pages before chunking returns that budget to real pages.

    Text before the first page header (unknown page) is kept. Pure and
    deterministic: the same text and rule version always give the same result,
    which is what lets a resumed run re-derive the trimmed text. Offsets in
    ``dropped`` are into the original *text*; every offset downstream (chunk and
    window bounds, spans, dump pages) is into the RETURNED text."""
    kept: list[str] = []
    dropped: list[DroppedPage] = []
    for span in page_spans(text):
        if is_excluded_page(span.url):
            dropped.append(DroppedPage(url=span.url or "", start=span.start, end=span.end))
        else:
            kept.append(text[span.start : span.end])
    return PageExclusion(
        version=PAGE_EXCLUSION_VERSION,
        text="".join(kept) if dropped else text,
        dropped=tuple(dropped),
        chars_before=len(text),
    )


def scan_domain(text: str, *, preceding_page: Optional[str] = None) -> str:
    """What the scan sweeps: *text* minus page headers minus excluded pages.
    Length-preserving. Both span sets are found on the ORIGINAL text (an
    excluded page is recognised by its URL line, which the header mask blanks)."""
    spans = [m.span() for m in _URL_LINE_RE.finditer(text)]
    spans += [m.span() for m in _SEPARATOR_LINE_RE.finditer(text)]
    spans += [(s.start, s.end) for s in excluded_page_spans(text, preceding_page=preceding_page)]
    return _blank(text, spans)


# The header ``wire_window_text`` prepends to a window that opens mid-page
# (2026-08-22, proposal N1): the inherited page's URL under a separator line
# shaped like the scraper's, so the model sees which page the head belongs to
# instead of prose under the request nonce — and a line saying the page began
# earlier, so it does not take the head for the page's start. Wire only: the
# scan and the fold never see it.
CONTINUED_PAGE_MARKER = "[this page began before the text shown; its opening is not included]"
_WIRE_SEPARATOR_LINE = "#" * 50


def continued_page_header(url: str) -> str:
    return f"{_WIRE_SEPARATOR_LINE}\n{url}\n{CONTINUED_PAGE_MARKER}\n\n"


def _inherited_head(text: str) -> str:
    """The stretch of *text* before its first page header — the part a
    mid-page window inherits from the preceding page ('' when the window opens
    on a header or holds no header at all)."""
    first = next(iter(_page_headers(text)), None)
    return text[: first[0]] if first is not None else ""


def wire_window_text(subject_text: str, start: int, end: int) -> str:
    """The text of ``subject_text[start:end]`` as a request sends it: excluded
    pages omitted, the page a mid-page window inherits taken into account —
    omitted with its page when that page is excluded, otherwise announced by
    ``continued_page_header`` so the model knows where the head belongs."""
    text = subject_text[start:end]
    preceding_page = preceding_page_of(subject_text, start)
    wire = omit_excluded_pages(text, preceding_page=preceding_page)
    head = _inherited_head(text)
    if preceding_page and head.strip() and not is_excluded_page(preceding_page):
        return continued_page_header(preceding_page) + wire
    return wire


def omit_excluded_pages(text: str, *, preceding_page: Optional[str] = None) -> str:
    """*text* for the WIRE: every excluded page's body replaced by its URL line
    (kept, so the model sees a page was there) and ``EXCLUDED_PAGE_MARKER``; a
    window head inherited from an excluded page becomes the marker alone. NOT
    length-preserving — offsets into the result are meaningless; the scan and
    the fold work on the original text."""
    spans = excluded_page_spans(text, preceding_page=preceding_page)
    if not spans:
        return text
    out: list[str] = []
    cursor = 0
    for span in spans:
        out.append(text[cursor:span.start])
        body = text[span.start:span.end]
        header = _PAGE_HEADER_RE.match(body)
        if header:  # the page's own header lines stay: separator (if any) + URL
            out.append(f"{header.group().strip()}\n{EXCLUDED_PAGE_MARKER}\n")
        else:
            out.append(f"{EXCLUDED_PAGE_MARKER}\n")
        cursor = span.end
    out.append(text[cursor:])
    return "".join(out)


@dataclass(frozen=True)
class PageSpan:
    """The stretch of a window that belongs to one page. ``url`` is None for
    text before the first page header (a sub-window that starts mid-page)
    unless the caller supplied the inherited page."""

    url: Optional[str]
    start: int
    end: int


# A page header as the scraper writes it: the separator line (optional — a
# bare URL line still opens a page) directly followed by the URL line.
_PAGE_HEADER_RE = re.compile(
    r"(?:[ \t]*#{10,}[ \t]*\r?\n)?[ \t]*https?://\S+[ \t]*(?=\r?\n|$)"
)


def _page_headers(text: str) -> Iterable[tuple[int, str]]:
    """``(start, url)`` of every page header of *text*, in order. A page starts
    at its separator line when one directly precedes its URL line (so a
    page-aligned window opens on the separator), else at the URL line."""
    separator_ends = {m.end(): m.start() for m in _SEPARATOR_LINE_RE.finditer(text)}
    for m in _URL_LINE_RE.finditer(text):
        start = m.start()
        for newline in ("\n", "\r\n"):
            before = start - len(newline)
            if before in separator_ends and text[before:start] == newline:
                start = separator_ends[before]
                break
        yield start, m.group().strip()


def page_spans(text: str, *, preceding_page: Optional[str] = None) -> list[PageSpan]:
    """Pages of *text*, in order, tiling ``[0, len(text))``. A page starts at
    its header (separator line + URL line, or the bare URL line) and runs to
    the next header (or the end)."""
    spans: list[PageSpan] = []
    cursor = 0
    current = preceding_page
    for start, url in _page_headers(text):
        if start > cursor:
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
    """Scan *window_text* for every form, both tiers, over the masked domain
    (headers and excluded pages blanked; *preceding_page* decides whether the
    window's head belongs to an excluded page)."""
    domain = scan_domain(window_text, preceding_page=preceding_page)
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
