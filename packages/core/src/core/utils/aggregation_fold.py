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
   and excluded pages) after LONGEST-SPAN CONTAINMENT: a hit strictly inside a
   longer hit of any form is not an owner — ``Lead`` and ``Lead Time`` inside
   "Sample Lead Time" belong to ``Sample Lead Time`` (D8). One owner is one
   mention; its ``form`` is the text at the span (a sent form, or a casing of
   one the scan discovered — "casing rescue", the mechanical replacement for
   what recursive search used to supply), ``sent_form`` the form whose scan
   found it. Two sent casings of one string find the same spans and yield one
   mention per span, not two.

B. CLIP. The snippet is the sentence holding the occurrence within its line,
   or the whole line where the line has no sentence punctuation (menus,
   headings, list entries). Measured 2026-08-22 on run 20260822T195947: median
   112 chars, max 623, against the collector's 138 / 1,973.

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
   sent form with no occurrence in its window — a search false positive, or
   one swallowed by containment — is skipped by synthesis but stays visible.

F. SYNTHESIS ENTRIES. A bundle's entries are its DISTINCT snippets in locked
   order, each with the location of its first occurrence (user decision
   2026-08-22: a repeated line reaches synthesis once; the per-occurrence
   mentions stay on the bundle for the dump and ground truth).
"""

from __future__ import annotations

import bisect
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Mapping, Optional, Sequence

from core.models.extraction_schemas.mention_collection import MentionWireItem
from core.models.extraction_schemas.synthesis import SynthesisEntry, SynthesisRecordInput
from core.utils.floor_scan import (
    FloorScan,
    Occurrence,
    excluded_page_spans,
    floor_scan,
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
    can find the same span — casings of one string), minus those strictly
    contained in a longer hit (D8's longest-span containment), by position."""
    by_span: dict[tuple[int, int], str] = {}
    for sent_form in sent_forms:
        for o in scan.tier2.get(sent_form, []):
            span = (o.start, o.end)
            # The sent form that matches the span's text exactly names the hit;
            # otherwise the first (sent order) casing that found it.
            if span not in by_span or text[o.start:o.end] == sent_form:
                by_span[span] = sent_form
    hits = sorted(
        (_Hit(s, e, text[s:e], sf) for (s, e), sf in by_span.items()),
        key=lambda h: (h.start, -(h.end - h.start), h.form),
    )
    owners: list[_Hit] = []
    reach = -1  # furthest end among kept hits, all of which start at or before h
    for h in hits:
        # Kept hits start ≤ h.start (sorted); one reaching to or past h.end
        # contains h. Equal spans collapsed above, so "reaches at least as far"
        # is "strictly longer".
        if h.end <= reach:
            continue
        owners.append(h)
        reach = h.end
    return owners


_SENTENCE_BREAK = re.compile(r"(?<=[.!?])\s+")


class _Lines:
    """Line geometry of a window, for clipping."""

    def __init__(self, text: str):
        self.lines = text.split("\n")
        self.starts = [0]
        for line in self.lines:
            self.starts.append(self.starts[-1] + len(line) + 1)

    def clip(self, start: int, end: int) -> tuple[str, int]:
        """The sentence holding ``[start, end)`` within its line — the whole
        line where the line has no sentence break — stripped, with its absolute
        offset. Never cuts inside the occurrence."""
        li = bisect.bisect_right(self.starts, start) - 1
        line_start = self.starts[li]
        line = self.lines[li]
        rel_start, rel_end = start - line_start, min(end, line_start + len(line)) - line_start
        cuts = [0] + [m.end() for m in _SENTENCE_BREAK.finditer(line)] + [len(line)]
        sb = max(c for c in cuts if c <= rel_start)
        se = min([c for c in cuts if c > rel_start] or [len(line)])
        se = max(se, rel_end)
        piece = line[sb:se]
        stripped = piece.strip()
        lead = len(piece) - len(piece.lstrip())
        return stripped, line_start + sb + lead


def collect_window(
    text: str,
    sent_forms: Sequence[str],
    *,
    preceding_page: Optional[str] = None,
) -> WindowCollection:
    """Steps A–C over one window. Pure; independent of the order of
    ``sent_forms``. Blank forms are ignored (they can anchor nothing).

    Raises ``MentionIdCollisionError`` should two distinct snippets of the
    window hash to one mention id.
    """
    forms = sorted({f for f in sent_forms if f.strip()})
    scan = floor_scan(text, forms, preceding_page=preceding_page)
    lines = _Lines(text)

    mentions: list[CollectedMention] = []
    snippet_of_id: dict[str, str] = {}
    items: list[MentionWireItem] = []
    for h in _owning_hits(text, scan, forms):
        snippet, snippet_start = lines.clip(h.start, h.end)
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

    def synthesis_entry(self) -> SynthesisEntry:
        return SynthesisEntry(location=self.location, snippet=self.snippet)


@dataclass(frozen=True)
class MentionBundle:
    """One group: its id, key, member forms (sorted), and mentions in locked
    order. ``group_id`` is what rides the synthesis wire as ``record_id``."""

    group_id: str
    key: str
    forms: tuple[str, ...]
    mentions: tuple[FoldedMention, ...]

    @property
    def is_empty(self) -> bool:
        return not self.mentions

    @property
    def status(self) -> str:
        return BUNDLE_STATUS_NO_MENTIONS if self.is_empty else BUNDLE_STATUS_OK

    def synthesis_entries(self) -> list[SynthesisEntry]:
        """Step F: distinct snippets in locked order, each under the location
        of its first occurrence."""
        entries: list[SynthesisEntry] = []
        seen: set[str] = set()
        for m in self.mentions:
            if m.snippet in seen:
                continue
            seen.add(m.snippet)
            entries.append(m.synthesis_entry())
        return entries

    def synthesis_record(self) -> SynthesisRecordInput:
        return SynthesisRecordInput(record_id=self.group_id, entries=self.synthesis_entries())


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

    def bundle(self, group_id: str) -> Optional[MentionBundle]:
        for b in self.bundles:
            if b.group_id == group_id:
                return b
        return None

    @property
    def empty_bundles(self) -> list[MentionBundle]:
        return [b for b in self.bundles if b.is_empty]

    def synthesis_records(self) -> list[SynthesisRecordInput]:
        """The synthesis request side: one record per NON-EMPTY bundle, in
        bundle order. Empty bundles are skipped (nothing to synthesize) but
        remain in ``bundles`` for the dump."""
        return [b.synthesis_record() for b in self.bundles if not b.is_empty]


# --- the per-window fold -----------------------------------------------------------


def fold_window(window: WindowInput, *, window_index: int = 0) -> WindowFold:
    """Steps A–D over one window. Pure; independent of the order of
    ``sent_forms`` and of the keys of ``locations_by_mention_id``."""
    collection = collect_window(
        window.text, window.sent_forms, preceding_page=window.preceding_page
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


# --- the document fold ------------------------------------------------------------------


def fold_document(
    windows: Sequence[WindowInput],
    *,
    verb_fold: bool = False,
) -> FoldResult:
    """Steps A–D over every window, then E over the union of sent and
    collected forms.

    Raises ``GroupIdCollisionError`` (from ``assign_group_ids``) should two
    distinct keys ever hash to one id.
    """
    folds = [fold_window(w, window_index=i) for i, w in enumerate(windows)]

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
    filled = sorted(
        (b for b in bundles if not b.is_empty),
        key=lambda b: (b.mentions[0].window_index, b.mentions[0].start, b.mentions[0].end, b.key),
    )
    empty = sorted((b for b in bundles if b.is_empty), key=lambda b: b.key)
    return FoldResult(bundles=filled + empty, windows=folds, verb_fold=verb_fold)
