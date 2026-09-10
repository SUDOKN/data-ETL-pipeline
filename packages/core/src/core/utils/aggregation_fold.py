"""The v3 aggregation fold (PIPELINE_V3_PLAN.md D7, D8, D9, D11, D19 — as amended
2026-08-22): the pure code between search and synthesis, and the CODE HALF of
mention collection.

Until 2026-08-22 an LLM collector reported each form's occurrences and this
fold re-derived everything from its verbatim snippets. Measured on two runs,
the collector's accepted output was a strict subset of the mechanical scan
(the fold discarded any snippet that did not hold the form verbatim) minus the
17–19% of occurrences it satisficed away — deterministically skipping repeated
listing-title lines for common words. So code now collects, and the LLM's one
remaining job is LOCATION (``core.models.extraction_schemas.mention_collection``).
Everything below is pure and deterministic:

A. COLLECT. The owners of a window's text are the tier-2 floor-scan hits of
   every sent form (``floor_scan``: whole-word, case-insensitive for forms
   longer than ``SHORT_FORM_MAX_LENGTH``, over the window minus page headers
   and excluded pages). MENTIONS ARE DECENTRALIZED (D8, reversed 2026-08-27 by
   user decision on measurement): nesting does NOT suppress — a hit inside a
   longer hit is still a mention of its own form, so "doors and frames" is a
   mention of ``door``, of ``frame`` AND of ``doors and frames``, each in its
   own bundle with its own focal form. The rule this replaced (longest-span
   containment: ``Lead`` and ``Lead Time`` inside "Sample Lead Time" owning
   nothing) was measured to suppress 34% of all occurrences and to be the sole
   cause of every empty bundle in the corpus — ``ISO 9001`` hidden inside
   ``ISO 9001:2015``, ``Oil & Gas`` inside "U.S. Oil & Gas Client", ``ASME``
   inside "ASME-certified welders" — each recorded as a searched form that was
   never found, and skipped by synthesis. One hit is one mention; its ``form``
   is the text at the span (a sent form, or a casing of one the scan discovered
   — "casing rescue", the mechanical replacement for what recursive search used
   to supply), ``sent_form`` the form whose scan found it. A SPAN is still
   collected once: two sent casings of one string find the same spans and yield
   one mention per span, not two.

B. CLIP. The snippet is the sentence holding the occurrence within its line,
   or the whole line where the line has no sentence punctuation (menus,
   headings, list entries). Measured 2026-08-22 on run 20260822T195947: median
   112 chars, max 623, against the collector's 138 / 1,973. SNIPPET RADIUS
   (user knob, 2026-08-22): ``snippet_radius = r > 0`` widens the clip to the
   whole sentence unit(s) holding the occurrence plus ``r`` sentence units on
   each side, where a unit is a sentence within a line (the whole line when it
   has no sentence break), line breaks count as unit breaks, blank lines are
   skipped, and a page boundary line (separator / URL header,
   ``floor_scan.is_page_barrier_line``) is a hard stop that is never crossed
   or included. ``r = 0`` is byte-identical to the clip above (the golden
   behaviour every stored mention id rests on). The snippet hash is the
   mention id, so the radius is part of the mention stage's request identity.

   MARKDOWN TEXTS (2026-08-28, the scraper's markdown_v1 cutover): the clip
   needs no mode flag — it is line-oriented and Markdown is a line-oriented
   format, so a bullet, a heading or a pipe-table row is simply a line-shaped
   unit, and its ``- `` / ``## `` / ``|`` markers stay IN the snippet
   deliberately (verbatim text, and a location signal: the marker tells the
   synthesis model it is reading a list entry / heading / table row). The one
   markdown-specific rule: DECORATION lines — a pipe table's ``|---|``
   separator row, legacy ``-----`` dividers (``_DECORATION_LINE_RE``) — are
   not units, so a radius clip skips them exactly like blank lines and a
   radius-1 clip around a table data row reaches the header row.

C. WIRE (2026-09-03, the location-stage merge). The window's DISTINCT snippets,
   in first-occurrence order, are what the synthesis records send —
   ``mention_id = hash(snippet)`` is still every snippet's identity. There is
   no separate Location stage any more: the synthesis stage receives the
   chunk's text alongside its records and reads where each snippet sits for
   itself. The fold is fully mechanical.

D. LOCATION (2026-09-05, user decision). Each mention's ``location`` is
   derived in CODE at collection time — ``_Lines.locate_context``: a pipe-table
   row takes its table's header row (the topmost pipe row of its run, when a
   ``|---|`` separator follows it); every other snippet takes the nearest
   Markdown heading STRICTLY ABOVE its first line, table rows and ordinary
   lines passed over; a page boundary line is never crossed, and None means
   the page block has nothing above the snippet (the stored fold writes
   ``DEFAULT_LOCATION``).
   Between the merge and this date the synthesis model returned one verbatim
   context line PER SNIPPET instead: a slot nothing downstream read (grounding
   and screening consume the synthesis alone), that needed its own count-
   mismatch retry class, and whose enumeration invited a repetition loop — a
   65-snippet record under one heading drew 4,850 copies of that heading and
   a truncated answer (run 20260904T184906). Measured on run 20260905T014738
   before dropping it: this rule agreed with the model's line on 87% of
   snippets; the rest were the model preferring a page title over a scraper
   artifact heading (``## Intro``) or attributing nav boilerplate differently.
   A recurring snippet now gets the heading of EACH occurrence (14 of 15
   recurring snippets in that run sat under different headings), where the
   per-distinct-snippet quote could give it only one.

E. GROUP + BUNDLE. Forms bucket by ``normalize()`` (D9/D10: a dict, global
   scope — the union of every window's sent forms AND the casings the scan
   discovered, so a rescued casing sits in its family's group);
   ``group_id = hash(key)`` is the synthesis ``record_id`` (D11/D16); per-form
   ``record_id = hash(form)`` rides on every mention as provenance and the
   mention-GT anchor. Mentions inside a bundle are in LOCKED order — window
   index, then offset — so the synthesis input and its ``|ud=`` digest depend
   on nothing but the text; bundles are ordered by their first mention (empty
   bundles last, by key). The page on a mention is CODE-DERIVED from the
   occurrence offset (D6). EMPTY BUNDLES ARE KEPT (status ``no_mentions``): a
   sent form with no occurrence in its window — now only a search false
   positive, since containment no longer swallows anything — is skipped by
   synthesis but stays visible.

E2. COLLAPSE (D21, the ``collapse_compounds`` dial). A group whose surface form
   is nothing but a coordination of sibling groups that already hold every one
   of its mentions keeps its bundle and its dump row under status
   ``collapsed`` (with ``collapsed_into``), and is skipped by synthesis. Only
   sound because of A's decentralization: under containment a compound held its
   occurrences exclusively, so collapsing it destroyed evidence. See
   ``collapsible_groups`` for the split rule and its three guards.

F. SYNTHESIS SNIPPETS. A record sends its bundle's DISTINCT snippets in
   locked order, as bare verbatim strings (user decision 2026-08-22: a
   repeated line reaches synthesis once; the per-occurrence mentions stay on
   the bundle for the dump and ground truth. The ``{location, snippet}`` entry
   object died in two steps, both 2026-09-03: location with the location-stage
   merge — the synthesis model reads position from the chunk text itself —
   and the one-key wrapper right after it). Each record
   also carries the bundle's FOCAL FORM (D15 as amended 2026-08-22): the most
   frequent member form by mention count, ties broken by earliest first mention
   in locked order — code-chosen, deterministic, what synthesis is told the
   record is about.
"""

from __future__ import annotations

import bisect
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from typing import Optional, Sequence

from core.models.extraction_schemas.mention_collection import MentionWireItem
from core.models.extraction_schemas.synthesis import SynthesisRecordInput
from core.utils.floor_scan import (
    FloorScan,
    Occurrence,
    excluded_page_spans,
    floor_scan,
    is_page_barrier_line,
    page_at,
)
from core.utils.form_normalizer import NORMALIZER_VERSION, assign_group_ids, normalize
from core.utils.record_id_util import (
    MentionIdCollisionError,
    mention_id_for_snippet,
    record_id_for_phrase,
)

BUNDLE_STATUS_OK = "ok"
BUNDLE_STATUS_NO_MENTIONS = "no_mentions"
# A compound that is nothing but a coordination of sibling groups already
# holding every one of its mentions (D21). Kept in `bundles`, skipped by
# `synthesis_records()` — the same posture as an empty bundle.
BUNDLE_STATUS_COLLAPSED = "collapsed"

# D21's separators, matched on the SURFACE form. Never on the normalization key:
# `normalize` deletes commas and turns slashes into spaces, so the key of
# "Oil, Gas, and Petroleum" is "oil gas and petroleum" and would mis-split as
# `oil gas` | `petroleum`. A slash is deliberately NOT a separator here
# ("CNC/Manual", "ISO 9001/14001" are ambiguous).
_COORDINATION_SPLIT = re.compile(r"\s*,\s*|\s*&\s*|\b(?:and|or)\b", re.IGNORECASE)

# What a stored mention carries when the fold found no heading or table header
# above its snippet within the page block (module docstring, D). The field is
# called "location" for continuity with the retired location stage's prose
# register; its sources: "code" since 2026-09-05, "llm" for documents written
# between the location-stage merge and that date (the model's per-snippet
# quote), "none" for the default.
DEFAULT_LOCATION = "(location not described)"
LOCATION_SOURCE_CODE = "code"
LOCATION_SOURCE_LLM = "llm"
LOCATION_SOURCE_NONE = "none"

# A Markdown heading line: one to six '#' then a space and a word character.
# The scraper's page separator (ten or more '#', nothing else) does not match.
_HEADING_LINE_RE = re.compile(r"[ \t]*#{1,6}[ \t]+\S")


# --- input ---------------------------------------------------------------------


@dataclass
class WindowInput:
    """One window: its text and the forms search found in it (window-local,
    D2/D5). ``preceding_page`` is the page a sub-window cut mid-page inherits
    (see ``floor_scan.page_spans``); ``window_id`` is a free label for dumps — a
    window's POSITION in the document is its index in the sequence given to
    ``fold_document``."""

    text: str
    sent_forms: Sequence[str]
    preceding_page: Optional[str] = None
    window_id: Optional[str] = None


# --- A–C: the collection ------------------------------------------------------


@dataclass(frozen=True, order=True)
class CollectedMention:
    """One occurrence of one form, as code collected it."""

    start: int
    end: int
    form: str  # the text at [start, end)
    sent_form: str  # the sent form whose scan found it
    page: Optional[str]
    snippet: str
    snippet_start: int
    mention_id: str
    location: Optional[str]  # module docstring, D

    @property
    def occurrence(self) -> Occurrence:
        return Occurrence(self.start, self.end)


@dataclass
class WindowCollection:
    """What code collected in one window: the mentions in text order, the
    distinct-snippet wire items in first-occurrence order, the scan they rest
    on, and what the scan found beyond the sent forms."""

    text: str
    sent_forms: list[str]
    scan: FloorScan
    mentions: list[CollectedMention]
    items: list[MentionWireItem]
    # per sent form: the casings of it that occur in the window but were never
    # sent (tier 2 beyond tier 1) — each is now a collected form in its own right
    discovered_casings: dict[str, list[str]]
    excluded_pages: list[str]

    @property
    def collected_forms(self) -> list[str]:
        """Every distinct form that occurs: sent forms with hits plus the
        discovered casings, in first-occurrence order."""
        seen: dict[str, None] = {}
        for m in self.mentions:
            seen.setdefault(m.form, None)
        return list(seen)

    @property
    def forms_with_hits(self) -> list[str]:
        return [f for f in self.sent_forms if self.scan.tier2.get(f)]

    @property
    def zero_hit_forms(self) -> list[str]:
        return [f for f in self.sent_forms if not self.scan.tier2.get(f)]


@dataclass(frozen=True)
class _Hit:
    start: int
    end: int
    form: str  # text at the span
    sent_form: str


def _owning_hits(text: str, scan: FloorScan, sent_forms: Sequence[str]) -> list[_Hit]:
    """Tier-2 hits of every sent form, one per distinct span (several sent forms
    can find the same span — casings of one string), by position.

    NESTING DOES NOT SUPPRESS (D8 reversed 2026-08-27). A hit inside a longer
    hit is still a hit: "doors and frames" is a mention of ``door``, of
    ``frame`` AND of ``doors and frames``, and each is its own bundle. Sorted
    so an enclosing hit always precedes the hits inside it, which is what keeps
    the wire items (first-occurrence order over distinct snippets) stable
    against this change.
    """
    by_span: dict[tuple[int, int], str] = {}
    for sent_form in sent_forms:
        for o in scan.tier2.get(sent_form, []):
            span = (o.start, o.end)
            # The sent form that matches the span's text exactly names the hit;
            # otherwise the first (sent order) casing that found it.
            if span not in by_span or text[o.start:o.end] == sent_form:
                by_span[span] = sent_form
    return sorted(
        (_Hit(s, e, text[s:e], sf) for (s, e), sf in by_span.items()),
        key=lambda h: (h.start, -(h.end - h.start), h.form),
    )


_SENTENCE_BREAK = re.compile(r"(?<=[.!?])\s+")

# A DECORATION line (2026-08-28, with the scraper's markdown_v1 cutover —
# ``scraper.utils.html_to_markdown``): only pipes/colons/dashes/whitespace with
# at least one dash. Markdown pipe tables carry a ``|---|---|`` separator row
# under the header row, and legacy text has its own ``-----`` divider lines;
# neither is prose. Shape-neutral by construction: any line with a word
# character stays a unit. Decoration lines are not units — like blank lines
# they are skipped and do NOT break the page block — so a ``radius > 0`` clip
# never wastes a context unit on one, and a radius-1 clip around a table DATA
# row reaches the HEADER row across the separator (the mitigation for header
# rows orphaned from their data by window splits). No occurrence can sit on
# one (the scan only hits word text), so ``_unit_index_at`` is unaffected.
_DECORATION_LINE_RE = re.compile(r"[ \t|:-]*-[ \t|:-]*")


@dataclass(frozen=True)
class _Unit:
    """One sentence unit of a window (step B): absolute ``[start, end)``, and
    the index of the page block it sits in (barrier lines split blocks)."""

    start: int
    end: int
    block: int


class _Lines:
    """Line geometry of a window, for clipping."""

    def __init__(self, text: str):
        self.text = text
        self.lines = text.split("\n")
        self.starts = [0]
        for line in self.lines:
            self.starts.append(self.starts[-1] + len(line) + 1)
        self._units: Optional[list[_Unit]] = None
        self._unit_starts: Optional[list[int]] = None

    @staticmethod
    def _cuts(line: str) -> list[int]:
        return [0] + [m.end() for m in _SENTENCE_BREAK.finditer(line)] + [len(line)]

    def clip(self, start: int, end: int, *, radius: int = 0) -> tuple[str, int]:
        """The sentence holding ``[start, end)`` within its line — the whole
        line where the line has no sentence break — stripped, with its absolute
        offset. Never cuts inside the occurrence. With ``radius > 0``, the
        whole unit(s) holding the occurrence plus ``radius`` units each side,
        within the occurrence's page block (see the module docstring, B)."""
        if radius > 0:
            return self._clip_with_radius(start, end, radius)
        li = bisect.bisect_right(self.starts, start) - 1
        line_start = self.starts[li]
        line = self.lines[li]
        rel_start, rel_end = start - line_start, min(end, line_start + len(line)) - line_start
        cuts = self._cuts(line)
        sb = max(c for c in cuts if c <= rel_start)
        se = min([c for c in cuts if c > rel_start] or [len(line)])
        se = max(se, rel_end)
        piece = line[sb:se]
        stripped = piece.strip()
        lead = len(piece) - len(piece.lstrip())
        return stripped, line_start + sb + lead

    def units(self) -> list[_Unit]:
        """Every sentence unit of the window in text order. Barrier lines are
        not units and advance the block index; blank lines and decoration
        lines (``_DECORATION_LINE_RE`` — pipe-table separator rows, divider
        runs) are neither units nor block breaks."""
        if self._units is None:
            units: list[_Unit] = []
            block = 0
            for li, line in enumerate(self.lines):
                if is_page_barrier_line(line):
                    block += 1
                    continue
                if not line.strip():
                    continue
                if _DECORATION_LINE_RE.fullmatch(line):
                    continue
                line_start = self.starts[li]
                cuts = self._cuts(line)
                for a, b in zip(cuts, cuts[1:], strict=False):
                    if line[a:b].strip():
                        units.append(_Unit(line_start + a, line_start + b, block))
            self._units = units
            self._unit_starts = [u.start for u in units]
        return self._units

    def _unit_index_at(self, offset: int) -> int:
        units = self.units()
        assert self._unit_starts is not None
        i = bisect.bisect_right(self._unit_starts, offset) - 1
        if i < 0 or not (units[i].start <= offset < units[i].end):
            raise ValueError(
                f"offset {offset} is not inside any sentence unit (a barrier or blank "
                f"line); occurrences never are — the scan masks those lines."
            )
        return i

    def locate_context(self, snippet_start: int) -> Optional[str]:
        """The line that introduces the passage starting at ``snippet_start``
        (module docstring, D), STRICTLY ABOVE the snippet's first line and
        within its page block; None when the block has none above it.

        A snippet that is a pipe-table row takes its TABLE'S HEADER ROW — the
        topmost row of the run of pipe rows it sits in, when that row is
        followed by a decoration row (``|---|---|``); a headerless table, or
        the header row itself as the snippet, falls through to the heading
        search above the table. Every other snippet takes the nearest Markdown
        heading above it; table rows above prose are passed over (a table does
        not introduce the paragraph under it), as are blank, decoration and
        ordinary lines. HTML-island tables (``<tr>`` lines) carry no separator
        marker and take the nearest heading like prose.
        """
        li = bisect.bisect_right(self.starts, snippet_start) - 1
        search_from = li
        if self._is_pipe_row(li):
            top = li
            while top > 0 and (
                self._is_pipe_row(top - 1) or _DECORATION_LINE_RE.fullmatch(self.lines[top - 1])
            ):
                top -= 1
            if top < li and self._is_pipe_row(top) and self._is_table_header_row(top):
                return self.lines[top].strip()
            search_from = top
        for j in range(search_from - 1, -1, -1):
            line = self.lines[j]
            if is_page_barrier_line(line):
                return None
            if _HEADING_LINE_RE.match(line):
                return line.strip()
        return None

    def _is_pipe_row(self, li: int) -> bool:
        line = self.lines[li]
        return line.lstrip().startswith("|") and not _DECORATION_LINE_RE.fullmatch(line)

    def _is_table_header_row(self, li: int) -> bool:
        """Whether pipe row *li* is a header: the next non-blank line is a
        decoration (separator) row."""
        for k in range(li + 1, len(self.lines)):
            if not self.lines[k].strip():
                continue
            return bool(_DECORATION_LINE_RE.fullmatch(self.lines[k]))
        return False

    def _clip_with_radius(self, start: int, end: int, radius: int) -> tuple[str, int]:
        units = self.units()
        i = self._unit_index_at(start)
        j = self._unit_index_at(end - 1) if end > start else i
        block = units[i].block
        lo, hi = i, j
        while lo > 0 and i - lo < radius and units[lo - 1].block == block:
            lo -= 1
        while hi + 1 < len(units) and hi - j < radius and units[hi + 1].block == block:
            hi += 1
        piece = self.text[units[lo].start : units[hi].end]
        stripped = piece.strip()
        lead = len(piece) - len(piece.lstrip())
        return stripped, units[lo].start + lead


def collect_window(
    text: str,
    sent_forms: Sequence[str],
    *,
    preceding_page: Optional[str] = None,
    snippet_radius: int = 0,
) -> WindowCollection:
    """Steps A–C over one window. Pure; independent of the order of
    ``sent_forms``. Blank forms are ignored (they can anchor nothing).
    ``snippet_radius`` is the clip's context dial (module docstring, B; 0 =
    the sentence-within-line clip).

    Raises ``MentionIdCollisionError`` should two distinct snippets of the
    window hash to one mention id.
    """
    if snippet_radius < 0:
        raise ValueError(f"snippet_radius must be >= 0, got {snippet_radius}")
    forms = sorted({f for f in sent_forms if f.strip()})
    scan = floor_scan(text, forms, preceding_page=preceding_page)
    lines = _Lines(text)

    mentions: list[CollectedMention] = []
    snippet_of_id: dict[str, str] = {}
    items: list[MentionWireItem] = []
    for h in _owning_hits(text, scan, forms):
        snippet, snippet_start = lines.clip(h.start, h.end, radius=snippet_radius)
        mention_id = mention_id_for_snippet(snippet)
        known = snippet_of_id.get(mention_id)
        if known is None:
            snippet_of_id[mention_id] = snippet
            items.append(MentionWireItem(mention_id=mention_id, mention=snippet))
        elif known != snippet:
            raise MentionIdCollisionError(
                f"mention id {mention_id!r} is shared by two distinct snippets: "
                f"{known!r} and {snippet!r}. The id is deterministic, so this pair "
                f"collides on every run; raise RECORD_ID_BODY_LENGTH."
            )
        mentions.append(
            CollectedMention(
                start=h.start,
                end=h.end,
                form=h.form,
                sent_form=h.sent_form,
                page=page_at(scan.pages, h.start),
                snippet=snippet,
                snippet_start=snippet_start,
                mention_id=mention_id,
                location=lines.locate_context(snippet_start),
            )
        )

    sent_set = set(forms)
    discovered: dict[str, list[str]] = {}
    for form, hits in scan.tier2.items():
        casings = sorted({text[o.start:o.end] for o in hits} - sent_set)
        if casings:
            discovered[form] = casings

    return WindowCollection(
        text=text,
        sent_forms=forms,
        scan=scan,
        mentions=mentions,
        items=items,
        discovered_casings=discovered,
        excluded_pages=[
            s.url for s in excluded_page_spans(text, preceding_page=preceding_page) if s.url
        ],
    )


# --- output: mentions and bundles ---------------------------------------------


@dataclass(frozen=True, order=True)
class FoldedMention:
    """One occurrence of one form, as the fold collected it — fully mechanical
    since the location-stage merge (2026-09-03).

    ``(window_index, start, end)`` is the occurrence's position in the
    document (the LOCKED order); ``form`` is the text at ``[start, end)`` — a
    sent form or a discovered casing of one; ``record_id = hash(form)`` is its
    provenance key (D11); ``page`` is code-derived from ``start``; ``snippet``
    is the clipped passage holding the occurrence, at ``snippet_start``;
    ``mention_id = hash(snippet)`` is the snippet's identity; ``sent_form`` is
    the sent form whose scan found the occurrence; ``location`` is the
    code-derived context line above the snippet (module docstring, D; None
    where the page block has none).
    """

    window_index: int
    start: int
    end: int
    form: str
    record_id: str
    page: Optional[str]
    snippet: str
    snippet_start: int
    mention_id: str
    sent_form: str
    location: Optional[str] = None

    @property
    def occurrence(self) -> Occurrence:
        return Occurrence(self.start, self.end)

    @property
    def is_discovered_casing(self) -> bool:
        return self.form != self.sent_form




@dataclass(frozen=True)
class MentionBundle:
    """One group: its id, key, member forms (sorted), and mentions in locked
    order. ``group_id`` is what rides the synthesis wire as ``record_id``.

    ``collapsed_into`` is D21: the sibling group ids this bundle is a mere
    coordination of. Non-empty means the bundle keeps its mentions and its
    dump row but is not synthesized — the parts carry its evidence.
    """

    group_id: str
    key: str
    forms: tuple[str, ...]
    mentions: tuple[FoldedMention, ...]
    collapsed_into: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.mentions

    @property
    def is_collapsed(self) -> bool:
        return bool(self.collapsed_into)

    @property
    def status(self) -> str:
        if self.is_empty:
            return BUNDLE_STATUS_NO_MENTIONS
        return BUNDLE_STATUS_COLLAPSED if self.is_collapsed else BUNDLE_STATUS_OK

    @property
    def focal_form(self) -> Optional[str]:
        """The form synthesis is told the record is about (D15 as amended
        2026-08-22): the most frequent member form by mention count — the
        text at the span, so a discovered casing counts as its own form — ties
        broken by the earliest first mention in locked order (then lexically,
        which cannot happen: two forms cannot share a first mention). None for
        an empty bundle."""
        if not self.mentions:
            return None
        counts = Counter(m.form for m in self.mentions)
        first_index: dict[str, int] = {}
        for index, m in enumerate(self.mentions):
            first_index.setdefault(m.form, index)
        return min(counts, key=lambda f: (-counts[f], first_index[f], f))

    def distinct_snippets(self) -> list[str]:
        """Step F: the bundle's distinct snippets in locked order — what the
        synthesis record sends."""
        snippets: list[str] = []
        seen: set[str] = set()
        for m in self.mentions:
            if m.snippet in seen:
                continue
            seen.add(m.snippet)
            snippets.append(m.snippet)
        return snippets

    def synthesis_record(self) -> SynthesisRecordInput:
        """The record this bundle sends to synthesis. Raises for an empty
        bundle — it has no focal form and nothing to synthesize; callers go
        through ``FoldResult.synthesis_records``, which skips empties."""
        focal = self.focal_form
        if focal is None:
            raise ValueError(
                f"bundle {self.group_id!r} ({self.key!r}) has no mentions and no synthesis record"
            )
        return SynthesisRecordInput(
            record_id=self.group_id,
            focal_form=focal,
            snippets=self.distinct_snippets(),
        )


# --- output: per-window report ---------------------------------------------------


@dataclass
class WindowFold:
    """What the fold made of one window: the collection it rests on and the
    mentions in locked order."""

    window_index: int
    window_id: Optional[str]
    collection: WindowCollection
    mentions: list[FoldedMention]

    @property
    def sent_forms(self) -> list[str]:
        return self.collection.sent_forms

    @property
    def items(self) -> list[MentionWireItem]:
        return self.collection.items


@dataclass
class FoldResult:
    """The document-level fold: bundles (first-mention order, empties last) and
    the per-window reports, under the normalizer version that keyed them."""

    bundles: list[MentionBundle]
    windows: list[WindowFold]
    verb_fold: bool
    normalizer_version: str = NORMALIZER_VERSION
    snippet_radius: int = 0
    collapse_compounds: bool = False

    def bundle(self, group_id: str) -> Optional[MentionBundle]:
        for b in self.bundles:
            if b.group_id == group_id:
                return b
        return None

    @property
    def empty_bundles(self) -> list[MentionBundle]:
        return [b for b in self.bundles if b.is_empty]

    @property
    def collapsed_bundles(self) -> list[MentionBundle]:
        return [b for b in self.bundles if b.is_collapsed]

    def synthesis_records(self) -> list[SynthesisRecordInput]:
        """The synthesis request side: one record per bundle that has something
        to synthesize, in bundle order, each naming its focal form. Skipped:
        EMPTY bundles (nothing to synthesize) and COLLAPSED ones (D21 — their
        parts carry every mention). Both remain in ``bundles`` for the dump."""
        return [
            b.synthesis_record()
            for b in self.bundles
            if not b.is_empty and not b.is_collapsed
        ]


# --- the per-window fold -----------------------------------------------------------


def fold_window(
    window: WindowInput, *, window_index: int = 0, snippet_radius: int = 0
) -> WindowFold:
    """Steps A–C over one window. Pure; independent of the order of
    ``sent_forms``."""
    collection = collect_window(
        window.text,
        window.sent_forms,
        preceding_page=window.preceding_page,
        snippet_radius=snippet_radius,
    )
    mentions = [
        FoldedMention(
            window_index=window_index,
            start=m.start,
            end=m.end,
            form=m.form,
            record_id=record_id_for_phrase(m.form),
            page=m.page,
            snippet=m.snippet,
            snippet_start=m.snippet_start,
            mention_id=m.mention_id,
            sent_form=m.sent_form,
            location=m.location,
        )
        for m in collection.mentions
    ]
    return WindowFold(
        window_index=window_index,
        window_id=window.window_id,
        collection=collection,
        mentions=sorted(mentions),
    )


# --- E2: the compound collapse (D21) --------------------------------------------------


def coordination_segments(form: str) -> Optional[list[str]]:
    """*form* split into its coordination segments, or None when it is not a
    coordination at all.

    Splits the SURFACE form — see ``_COORDINATION_SPLIT`` for why never the key.
    Returns None unless the split yields at least two segments that each still
    have a non-empty normalization key, so "and doors", "R & " and a form whose
    only separator sits at an edge are not coordinations.
    """
    parts = [p.strip().strip(",;") for p in _COORDINATION_SPLIT.split(form)]
    parts = [p for p in parts if p.strip()]
    if len(parts) < 2:
        return None
    return parts


def _encloses(outer: FoldedMention, inner: FoldedMention) -> bool:
    return (
        outer.window_index == inner.window_index
        and outer.start <= inner.start
        and inner.end <= outer.end
    )


def collapsible_groups(
    bundles: Sequence[MentionBundle], *, verb_fold: bool = False
) -> dict[str, tuple[str, ...]]:
    """``group_id -> the sibling group ids it collapses into`` (D21).

    A compound collapses only when all three guards pass, in this order:

    G2 — every segment's normalized key is a DIFFERENT existing bundle in this
         fold's scope (the 20k chunk). This is what stops a shared-head
         coordination: "Commercial and Institutional Buildings" splits to
         ``commercial`` | ``institutional building`` and bare ``commercial``
         is not a group.
    G4 — INDEPENDENT STANDING: every part has at least one mention that no
         mention of the compound encloses. Without it a fragment that only ever
         occurs inside the compound would count as a sibling — the measured
         "wood or steel stud anchors" (= "(wood or steel) stud anchors") and
         "fire-rated doors and frames" cases.
    G3 — EVIDENCE COVERAGE: every snippet of the compound appears among its
         parts' snippets. The direct statement of "offers no unique mentions",
         and the guard that is only satisfiable once mentions are decentralized
         (D8 reversed) — under containment the compound held its occurrences
         exclusively and this could never pass.

    A part that is itself a collapse candidate disqualifies the collapse, so
    collapses never chain. Pure and deterministic.
    """
    by_key = {b.key: b for b in bundles}
    candidate_ids: set[str] = set()
    segments_of: dict[str, list[MentionBundle]] = {}

    for bundle in bundles:
        if bundle.is_empty:
            continue  # nothing to collapse, and nothing to cover
        for form in bundle.forms:
            parts = coordination_segments(form)
            if parts is None:
                continue
            keys = [normalize(p, verb_fold=verb_fold) for p in parts]
            if any(not k or k == bundle.key for k in keys) or len(set(keys)) < 2:
                continue
            if not all(k in by_key for k in keys):  # G2
                continue
            part_bundles = [by_key[k] for k in dict.fromkeys(keys)]
            if any(p.is_empty for p in part_bundles):
                continue
            candidate_ids.add(bundle.group_id)
            segments_of[bundle.group_id] = part_bundles
            break

    collapsed: dict[str, tuple[str, ...]] = {}
    for bundle in bundles:
        part_bundles = segments_of.get(bundle.group_id)
        if part_bundles is None:
            continue
        # no chains: a part that is itself collapsing cannot carry the evidence
        if any(p.group_id in candidate_ids for p in part_bundles):
            continue
        # G4 — independent standing
        if not all(
            any(
                not any(_encloses(outer, inner) for outer in bundle.mentions)
                for inner in part.mentions
            )
            for part in part_bundles
        ):
            continue
        # G3 — evidence coverage
        covered: set[str] = set()
        for part in part_bundles:
            covered.update(m.snippet for m in part.mentions)
        if {m.snippet for m in bundle.mentions} - covered:
            continue
        collapsed[bundle.group_id] = tuple(p.group_id for p in part_bundles)
    return collapsed


# --- the document fold ------------------------------------------------------------------


def fold_document(
    windows: Sequence[WindowInput],
    *,
    verb_fold: bool = False,
    snippet_radius: int = 0,
    collapse_compounds: bool = False,
) -> FoldResult:
    """Steps A–C over every window, then E over the union of sent and
    collected forms, then E2 (D21) when *collapse_compounds*. ``snippet_radius``
    is the clip dial (B), uniform over the document — it is run identity, not a
    per-window fact; so is *collapse_compounds*, which decides which groups
    reach synthesis at all.

    Raises ``GroupIdCollisionError`` (from ``assign_group_ids``) should two
    distinct keys ever hash to one id.
    """
    folds = [
        fold_window(w, window_index=i, snippet_radius=snippet_radius)
        for i, w in enumerate(windows)
    ]

    key_of: dict[str, str] = {}
    for fold in folds:
        for form in list(fold.collection.sent_forms) + fold.collection.collected_forms:
            if form not in key_of:
                key_of[form] = normalize(form, verb_fold=verb_fold)
    group_ids = assign_group_ids(key_of.values())

    forms_by_key: dict[str, set[str]] = defaultdict(set)
    for form, key in key_of.items():
        forms_by_key[key].add(form)
    mentions_by_key: dict[str, list[FoldedMention]] = defaultdict(list)
    for fold in folds:
        for m in fold.mentions:
            mentions_by_key[key_of[m.form]].append(m)

    bundles = [
        MentionBundle(
            group_id=group_ids[key],
            key=key,
            forms=tuple(sorted(forms)),
            mentions=tuple(sorted(mentions_by_key.get(key, []))),
        )
        for key, forms in forms_by_key.items()
    ]
    if collapse_compounds:
        collapsed = collapsible_groups(bundles, verb_fold=verb_fold)
        if collapsed:
            bundles = [
                (
                    replace(b, collapsed_into=collapsed[b.group_id])
                    if b.group_id in collapsed
                    else b
                )
                for b in bundles
            ]

    filled = sorted(
        (b for b in bundles if not b.is_empty),
        key=lambda b: (b.mentions[0].window_index, b.mentions[0].start, b.mentions[0].end, b.key),
    )
    empty = sorted((b for b in bundles if b.is_empty), key=lambda b: b.key)
    return FoldResult(
        bundles=filled + empty,
        windows=folds,
        verb_fold=verb_fold,
        snippet_radius=snippet_radius,
        collapse_compounds=collapse_compounds,
    )
