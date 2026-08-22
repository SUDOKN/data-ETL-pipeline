"""The v3 aggregation fold (PIPELINE_V3_PLAN.md D8, D9, D11, D19): the pure code
between mention collection and synthesis.

The LLM mention collector answers per window: for each sent form, a list of
``{location, snippet}`` mentions. That answer is ADVISORY. Everything below
treats the verbatim snippet as the only fact in it and re-derives the rest
mechanically, so that model case-sloppiness, a sentence filed under one form
when it holds several, and a sub-form claimed for a fuller span's spot are all
repaired without a reconciler and without any further LLM call:

A. LOCATE. Each snippet is found in the window by exact substring. A snippet
   that occurs at several positions yields one candidate per position (the
   collector's advisory location cannot pick one, and the tier-1 hold wants
   every position accounted for). A snippet that does not occur verbatim is
   reported as ``unlocated`` and anchors nothing — it is the mention-GT
   failure of D18, made visible here.

B. RE-ATTRIBUTE. The owners of a window's text are the tier-1 floor-scan hits
   (``floor_scan``: exact-case, whole-word, over the window minus its page
   headers) after LONGEST-MATCH CONTAINMENT: a hit strictly inside a longer hit
   of any sent form is not an owner — ``Lead`` and ``Lead Time`` inside
   "Sample Lead Time" belong to ``Sample Lead Time`` (D8). A located snippet is
   a mention of EVERY owner inside its span, whatever form the collector filed
   it under. Attributing from the scan rather than by re-matching forms inside
   the snippet string keeps the word-boundary guard honest at the snippet's
   edges (a snippet cut mid-token would otherwise pass the guard) and makes B
   and E one computation.

C. DEDUP (D19's residual rule, settled here). The unit is the OCCURRENCE: key
   ``(group key, occurrence span)``; when several snippets cover one spot the
   LONGER snippet is kept (ties broken lexically so the result is independent
   of the collector's answer order). Two genuine occurrences in one sentence
   stay two mentions; two extents of one spot collapse to one.

D. GROUP + BUNDLE. Forms bucket by ``normalize()`` (D9/D10: a dict, global
   scope — all windows of the document, the union of every window's sent
   forms); ``group_id = hash(key)`` is the synthesis ``record_id`` (D11/D16);
   per-form ``record_id = hash(form)`` rides on every mention as provenance
   and the mention-GT anchor. Mentions inside a bundle are in LOCKED order —
   window index, then offset (== page order then position) — so the synthesis
   input and its ``|ud=`` digest do not depend on the collector's answer order
   or batching; bundles themselves are ordered by their first mention (empty
   bundles last, by key). The page on a mention is CODE-DERIVED from the
   occurrence offset (D6 as amended); the collector's location string rides
   along as colour. EMPTY BUNDLES ARE KEPT (status ``no_mentions``): a form
   with no mention after the fold — a search false positive, or one swallowed
   by containment — is skipped by synthesis (``FoldResult.synthesis_records``)
   but stays dump-visible.

E. HOLD. The tier-1 obligations of a window are the owners of B; every one
   must lie inside some located snippet or it is ``unaccounted`` — the
   window's discrepancy record (D7). Applying containment to the scan's hits
   as well is what keeps a fuller-span form from raising phantom
   discrepancies for its sub-forms.

Everything here is pure and deterministic; what a discrepancy does, and how
bundles are batched onto the synthesis wire, is the Phase 3 node's business.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Mapping, Optional, Sequence

from core.models.extraction_schemas.mention_collection import Mention
from core.models.extraction_schemas.synthesis import SynthesisEntry, SynthesisRecordInput
from core.utils.floor_scan import FloorScan, Occurrence, floor_scan, page_at
from core.utils.form_normalizer import NORMALIZER_VERSION, assign_group_ids, normalize
from core.utils.record_id_util import record_id_for_phrase

BUNDLE_STATUS_OK = "ok"
BUNDLE_STATUS_NO_MENTIONS = "no_mentions"


# --- input ---------------------------------------------------------------------


@dataclass
class WindowInput:
    """One window as the collector saw it, plus its held answer.

    ``sent_forms`` are the forms the collector was asked about in THIS window
    (window-local, D2/D5). ``mentions_by_form`` is the collector's answer after
    ``hold_response_to_sent_forms`` — keyed by the form it filed each mention
    under, which the fold treats as advisory. ``preceding_page`` is the page a
    sub-window cut mid-page inherits (see ``floor_scan.page_spans``);
    ``window_id`` is a free label for dumps — a window's POSITION in the
    document is its index in the sequence given to ``fold_document``.
    """

    text: str
    sent_forms: Sequence[str]
    mentions_by_form: Mapping[str, Sequence[Mention]]
    preceding_page: Optional[str] = None
    window_id: Optional[str] = None


# --- output: mentions and bundles ---------------------------------------------


@dataclass(frozen=True, order=True)
class FoldedMention:
    """One occurrence of one form, as the fold attributed it.

    ``(window_index, start, end)`` is the occurrence's position in the
    document (the LOCKED order); ``form`` is the sent form that occurs exactly
    at ``[start, end)``; ``record_id = hash(form)`` is its provenance key
    (D11); ``page`` is code-derived from ``start``; ``snippet`` is the
    collector's verbatim passage holding the occurrence, located at
    ``snippet_start``; ``location`` is the collector's advisory colour;
    ``reported_form`` is the form the collector filed the snippet under (equal
    to ``form`` unless the fold re-keyed it).
    """

    window_index: int
    start: int
    end: int
    form: str
    record_id: str
    page: Optional[str]
    snippet: str
    snippet_start: int
    location: str
    reported_form: str

    @property
    def occurrence(self) -> Occurrence:
        return Occurrence(self.start, self.end)

    @property
    def rekeyed(self) -> bool:
        return self.reported_form != self.form

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

    def synthesis_record(self) -> SynthesisRecordInput:
        return SynthesisRecordInput(
            record_id=self.group_id,
            entries=[m.synthesis_entry() for m in self.mentions],
        )


# --- output: per-window report ---------------------------------------------------


@dataclass(frozen=True)
class SnippetReport:
    """A collector mention the fold could not turn into an occurrence:
    ``unlocated`` (the snippet is not a verbatim substring of the window) or
    ``unanchored`` (located at ``position``, but no owning occurrence of any
    sent form lies inside it — an anaphoric sentence, or a fragment)."""

    reported_form: str
    location: str
    snippet: str
    position: Optional[int] = None


@dataclass(frozen=True)
class RekeyReport:
    """A located snippet whose filed form was not among the owners inside it —
    the collector's attribution was repaired from the text."""

    reported_form: str
    snippet: str
    position: int
    attributed_forms: tuple[str, ...]


@dataclass
class WindowFold:
    """What the fold made of one window: the kept mentions (locked order), the
    scan it rests on, the tier-1 hold (``obligations`` per sent form after
    containment; ``unaccounted`` = the discrepancy record), and what it set
    aside. ``candidates`` counts (form, occurrence) rows before D19's dedup."""

    window_index: int
    window_id: Optional[str]
    mentions: list[FoldedMention]
    scan: FloorScan
    obligations: dict[str, list[Occurrence]]
    unaccounted: dict[str, list[Occurrence]]
    unlocated: list[SnippetReport] = field(default_factory=list)
    unanchored: list[SnippetReport] = field(default_factory=list)
    rekeyed: list[RekeyReport] = field(default_factory=list)
    candidates: int = 0

    @property
    def unaccounted_count(self) -> int:
        return sum(len(v) for v in self.unaccounted.values())

    @property
    def has_discrepancy(self) -> bool:
        return self.unaccounted_count > 0


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


# --- B: owners -------------------------------------------------------------------


@dataclass(frozen=True)
class _Hit:
    start: int
    end: int
    form: str


def _owning_hits(scan: FloorScan) -> list[_Hit]:
    """Tier-1 hits of every sent form, minus those strictly contained in a
    longer hit of any form (D8's longest-match containment), by position."""
    hits = sorted(
        (_Hit(o.start, o.end, form) for form, occs in scan.tier1.items() for o in occs),
        key=lambda h: (h.start, -(h.end - h.start), h.form),
    )
    owners: list[_Hit] = []
    reach = -1  # furthest end among kept hits, all of which start at or before h
    for h in hits:
        # Kept hits start ≤ h.start (sorted); one reaching to or past h.end
        # contains h. Equal spans cannot occur across distinct forms (the same
        # text is the same form) nor within one (per-form hits never overlap),
        # so "reaches at least as far" is "strictly longer".
        if h.end <= reach:
            continue
        owners.append(h)
        reach = h.end
    return owners


def obligations_by_form(scan: FloorScan) -> dict[str, list[Occurrence]]:
    """Tier-1 hold obligations per sent form (sent order): the scan's hits
    after containment. Public because the hold is meaningful on its own."""
    by_form: dict[str, list[Occurrence]] = {form: [] for form in scan.tier1}
    for h in _owning_hits(scan):
        by_form[h.form].append(Occurrence(h.start, h.end))
    return by_form


# --- A: locate --------------------------------------------------------------------


def _positions(text: str, snippet: str) -> list[int]:
    """Every start offset at which *snippet* occurs in *text* (the empty
    snippet occurs nowhere)."""
    if not snippet:
        return []
    out: list[int] = []
    i = text.find(snippet)
    while i != -1:
        out.append(i)
        i = text.find(snippet, i + 1)
    return out


# --- the per-window fold -----------------------------------------------------------


def _candidate_rank(m: FoldedMention) -> tuple[int, str, int, str, str]:
    """D19: the longer snippet wins. Among equal snippets the copy the collector
    filed under this very form is preferred (so ``reported_form`` reads
    truthfully), then a lexical order — never the collector's answer order."""
    own_filing = 0 if m.reported_form == m.form else 1
    return (-len(m.snippet), m.snippet, own_filing, m.location, m.reported_form)


def fold_window(
    window: WindowInput,
    *,
    window_index: int = 0,
    verb_fold: bool = False,
) -> WindowFold:
    """Steps A–C and E over one window. Pure; independent of the order of
    ``sent_forms``, of the keys of ``mentions_by_form`` and of the mentions
    under each key. Blank forms are ignored (they can anchor nothing)."""
    sent_forms = [f for f in window.sent_forms if f.strip()]
    scan = floor_scan(window.text, sent_forms, preceding_page=window.preceding_page)
    owners = _owning_hits(scan)
    obligations = obligations_by_form(scan)

    fold = WindowFold(
        window_index=window_index,
        window_id=window.window_id,
        mentions=[],
        scan=scan,
        obligations=obligations,
        unaccounted={form: [] for form in obligations},
    )

    by_occurrence: dict[tuple[str, int, int], FoldedMention] = {}
    for reported_form, mentions in window.mentions_by_form.items():
        for mention in mentions:
            positions = _positions(window.text, mention.snippet)
            if not positions:
                fold.unlocated.append(
                    SnippetReport(reported_form, mention.location, mention.snippet)
                )
                continue
            for pos in positions:
                end = pos + len(mention.snippet)
                inside = [h for h in owners if pos <= h.start and h.end <= end]
                if not inside:
                    fold.unanchored.append(
                        SnippetReport(reported_form, mention.location, mention.snippet, pos)
                    )
                    continue
                if reported_form not in {h.form for h in inside}:
                    fold.rekeyed.append(
                        RekeyReport(
                            reported_form,
                            mention.snippet,
                            pos,
                            tuple(sorted({h.form for h in inside})),
                        )
                    )
                for h in inside:
                    fold.candidates += 1
                    candidate = FoldedMention(
                        window_index=window_index,
                        start=h.start,
                        end=h.end,
                        form=h.form,
                        record_id=record_id_for_phrase(h.form),
                        page=page_at(scan.pages, h.start),
                        snippet=mention.snippet,
                        snippet_start=pos,
                        location=mention.location,
                        reported_form=reported_form,
                    )
                    key = (normalize(h.form, verb_fold=verb_fold), h.start, h.end)
                    incumbent = by_occurrence.get(key)
                    if incumbent is None or _candidate_rank(candidate) < _candidate_rank(incumbent):
                        by_occurrence[key] = candidate

    fold.mentions = sorted(by_occurrence.values())
    covered = {(m.form, m.start, m.end) for m in fold.mentions}
    for form, occs in obligations.items():
        fold.unaccounted[form] = [o for o in occs if (form, o.start, o.end) not in covered]
    return fold


# --- the document fold ------------------------------------------------------------------


def fold_document(
    windows: Sequence[WindowInput],
    *,
    verb_fold: bool = False,
) -> FoldResult:
    """Steps A–E over every window, then D over the union of sent forms.

    Raises ``GroupIdCollisionError`` (from ``assign_group_ids``) should two
    distinct keys ever hash to one id.
    """
    folds = [
        fold_window(w, window_index=i, verb_fold=verb_fold) for i, w in enumerate(windows)
    ]

    key_of: dict[str, str] = {}
    for w in windows:
        for form in w.sent_forms:
            if form.strip() and form not in key_of:
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
