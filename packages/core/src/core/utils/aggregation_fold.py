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

C. WIRE. The window's DISTINCT snippets, in first-occurrence order, are the
   Location request's items ``{mention_id, mention}`` — 2,880 items for 4,907
   occurrences on that run. ``mention_id = hash(snippet)``.

D. LOCATE. The Location stage's held answer is a ``mention_id → location`` map;
   every occurrence of a snippet takes its location. A snippet the model did
   not describe keeps the mention (it is a fact of the text) under
   ``DEFAULT_LOCATION`` and is reported — the model can no longer lose a
   mention, only fail to colour it.

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

F. SYNTHESIS ENTRIES. A bundle's entries are its DISTINCT snippets in locked
   order, each with the location of its first occurrence (user decision
   2026-08-22: a repeated line reaches synthesis once; the per-occurrence
   mentions stay on the bundle for the dump and ground truth). The synthesis
   A/B (user decision 2026-08-22) has an arm WITHOUT locations:
   ``include_location=False`` builds entries of the snippet alone. Each record
   also carries the bundle's FOCAL FORM (D15 as amended 2026-08-22): the most
   frequent member form by mention count, ties broken by earliest first mention
   in locked order — code-chosen, deterministic, what synthesis is told the
   record is about.
"""

from __future__ import annotations

import bisect
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from typing import Mapping, Optional, Sequence

from core.models.extraction_schemas.mention_collection import MentionWireItem
from core.models.extraction_schemas.synthesis import SynthesisEntry, SynthesisRecordInput
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

# What a mention carries when the Location stage did not describe its snippet.
DEFAULT_LOCATION = "(location not described)"
LOCATION_SOURCE_LLM = "llm"
LOCATION_SOURCE_NONE = "none"


# --- input ---------------------------------------------------------------------


@dataclass
class WindowInput:
    """One window: its text, the forms search found in it (window-local, D2/D5),
    and the Location stage's held answer for it (``mention_id → location``,
    merged across the window's request groups; empty when the stage has not
    answered). ``preceding_page`` is the page a sub-window cut mid-page inherits
    (see ``floor_scan.page_spans``); ``window_id`` is a free label for dumps — a
    window's POSITION in the document is its index in the sequence given to
    ``fold_document``."""

    text: str
    sent_forms: Sequence[str]
    locations_by_mention_id: Mapping[str, str] = field(default_factory=dict)
    preceding_page: Optional[str] = None
    window_id: Optional[str] = None
    # Location-stage diagnostics the fold only carries (dump-visible): the
    # mention ids a retry pass re-asked for, and ids the model answered that were
    # never sent (dropped by the hold).
    retried_mention_ids: Sequence[str] = ()
    unknown_answer_ids: Sequence[str] = ()


# --- A–C: the collection ------------------------------------------------------


@dataclass(frozen=True, order=True)
class CollectedMention:
    """One occurrence of one form, as code collected it (before location)."""

    start: int
    end: int
    form: str  # the text at [start, end)
    sent_form: str  # the sent form whose scan found it
    page: Optional[str]
    snippet: str
    snippet_start: int
    mention_id: str

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
        not units and advance the block index; blank lines are neither."""
        if self._units is None:
            units: list[_Unit] = []
            block = 0
            for li, line in enumerate(self.lines):
                if is_page_barrier_line(line):
                    block += 1
                    continue
                if not line.strip():
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
    """One occurrence of one form, located.

    ``(window_index, start, end)`` is the occurrence's position in the
    document (the LOCKED order); ``form`` is the text at ``[start, end)`` — a
    sent form or a discovered casing of one; ``record_id = hash(form)`` is its
    provenance key (D11); ``page`` is code-derived from ``start``; ``snippet``
    is the clipped passage holding the occurrence, at ``snippet_start``;
    ``mention_id = hash(snippet)`` is the Location wire key; ``location`` is
    the Location stage's description (``location_source`` says whether the
    model gave it or the fold defaulted it); ``sent_form`` is the sent form
    whose scan found the occurrence.
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
    location: str
    location_source: str
    sent_form: str

    @property
    def occurrence(self) -> Occurrence:
        return Occurrence(self.start, self.end)

    @property
    def is_discovered_casing(self) -> bool:
        return self.form != self.sent_form

    def synthesis_entry(self, *, include_location: bool = True) -> SynthesisEntry:
        """The entry this mention contributes (step F). ``include_location=False``
        is the no-location A/B arm: the snippet alone."""
        return SynthesisEntry(
            location=self.location if include_location else None, snippet=self.snippet
        )


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

    def synthesis_entries(self, *, include_location: bool = True) -> list[SynthesisEntry]:
        """Step F: distinct snippets in locked order, each under the location
        of its first occurrence (or no location at all on the A/B arm)."""
        entries: list[SynthesisEntry] = []
        seen: set[str] = set()
        for m in self.mentions:
            if m.snippet in seen:
                continue
            seen.add(m.snippet)
            entries.append(m.synthesis_entry(include_location=include_location))
        return entries

    def synthesis_record(self, *, include_location: bool = True) -> SynthesisRecordInput:
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
            entries=self.synthesis_entries(include_location=include_location),
        )


# --- output: per-window report ---------------------------------------------------


@dataclass
class WindowFold:
    """What the fold made of one window: the collection it rests on, the
    located mentions (locked order), and the Location stage's coverage —
    ``described`` ids answered, ``not_described`` ids the model left out."""

    window_index: int
    window_id: Optional[str]
    collection: WindowCollection
    mentions: list[FoldedMention]
    described: list[str] = field(default_factory=list)
    not_described: list[str] = field(default_factory=list)
    retried: list[str] = field(default_factory=list)
    unknown_answer_ids: list[str] = field(default_factory=list)

    @property
    def sent_forms(self) -> list[str]:
        return self.collection.sent_forms

    @property
    def items(self) -> list[MentionWireItem]:
        return self.collection.items

    @property
    def has_undescribed(self) -> bool:
        return bool(self.not_described)


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

    def synthesis_records(self, *, include_location: bool = True) -> list[SynthesisRecordInput]:
        """The synthesis request side: one record per bundle that has something
        to synthesize, in bundle order, each naming its focal form. Skipped:
        EMPTY bundles (nothing to synthesize) and COLLAPSED ones (D21 — their
        parts carry every mention). Both remain in ``bundles`` for the dump."""
        return [
            b.synthesis_record(include_location=include_location)
            for b in self.bundles
            if not b.is_empty and not b.is_collapsed
        ]


# --- the per-window fold -----------------------------------------------------------


def fold_window(
    window: WindowInput, *, window_index: int = 0, snippet_radius: int = 0
) -> WindowFold:
    """Steps A–D over one window. Pure; independent of the order of
    ``sent_forms`` and of the keys of ``locations_by_mention_id``."""
    collection = collect_window(
        window.text,
        window.sent_forms,
        preceding_page=window.preceding_page,
        snippet_radius=snippet_radius,
    )
    mentions: list[FoldedMention] = []
    for m in collection.mentions:
        location = window.locations_by_mention_id.get(m.mention_id)
        mentions.append(
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
                location=DEFAULT_LOCATION if location is None else location,
                location_source=LOCATION_SOURCE_NONE if location is None else LOCATION_SOURCE_LLM,
                sent_form=m.sent_form,
            )
        )
    sent_ids = [item.mention_id for item in collection.items]
    return WindowFold(
        window_index=window_index,
        window_id=window.window_id,
        collection=collection,
        mentions=sorted(mentions),
        described=[i for i in sent_ids if i in window.locations_by_mention_id],
        not_described=[i for i in sent_ids if i not in window.locations_by_mention_id],
        retried=list(window.retried_mention_ids),
        unknown_answer_ids=list(window.unknown_answer_ids),
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
    """Steps A–D over every window, then E over the union of sent and
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
