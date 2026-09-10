"""The aggregation fold as a dump reader meets it (PIPELINE_V3_PLAN.md Phase
3.1, amended 2026-08-22; location coverage left with the retired location
stage, 2026-09-03 — per-mention context now rides the synthesis dump).

One block per chunk: a summary, the bundles (one entry per group — member forms
visible inline, so a wrong merge is human-visible at a glance — with mentions
in locked order), and the per-window report: what code collected (occurrences,
distinct snippets, forms with and without hits, casings the scan discovered,
pages it excluded).

A group that collapsed (D21) carries ``collapsed_into`` and status
``collapsed``; the summary separates the three populations a reader must not
confuse — ``groups`` (all of them), ``empty_groups`` and ``collapsed_groups``
(both skipped by synthesis, for different reasons), and ``synthesized_groups``
(what actually reached the LLM).
"""

from __future__ import annotations

from typing import Any, Mapping, Optional

from core.models.extraction_schemas.stored_fold import (
    WindowBoundsMismatch,
    window_base_offset,
)
from core.utils.aggregation_fold import FoldResult, FoldedMention, MentionBundle, WindowFold
from core.utils.subject_name_lint import count_own_name_hits


def _mention_row(m: FoldedMention, base: Optional[int] = None) -> dict[str, Any]:
    row: dict[str, Any] = {
        "form": m.form,
        "record_id": m.record_id,
        "window": m.window_index,
        "span": [m.start, m.end],
        "page": m.page,
        "mention_id": m.mention_id,
        "snippet": m.snippet,
        # The code-derived context line above the snippet (aggregation_fold
        # docstring, D); null where the page block has none.
        "location": m.location,
    }
    if m.is_discovered_casing:
        row["sent_form"] = m.sent_form
    # The document-absolute span, when the window's base offset is known — the
    # join between this row and the same mention in `StoredFold`, which stores
    # only absolute offsets. Absent rather than null when the base could not be
    # read, so a present `doc_span` always means something.
    if base is not None:
        row["doc_span"] = [base + m.start, base + m.end]
    return row


def _bundle_row(
    b: MentionBundle,
    subject_name: Optional[str],
    bases: Optional[Mapping[int, int]] = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "group_id": b.group_id,
        "key": b.key,
        "forms": list(b.forms),
        "status": b.status,
        "mention_count": len(b.mentions),
        "distinct_snippets": len(b.distinct_snippets()),
        "mentions": [
            _mention_row(m, bases.get(m.window_index) if bases else None)
            for m in b.mentions
        ],
    }
    # Present only when it happened, like every lint field: the sibling groups
    # this compound collapsed into (D21).
    if b.is_collapsed:
        row["collapsed_into"] = list(b.collapsed_into)
    if subject_name:
        hits = sum(count_own_name_hits(m.snippet, subject_name) for m in b.mentions)
        if hits:
            row["own_name_hits_in_snippets"] = hits
    return row


def _window_row(w: WindowFold) -> dict[str, Any]:
    c = w.collection
    return {
        "window": w.window_index,
        "sub_bounds": w.window_id,
        "sent_forms": len(c.sent_forms),
        "forms_with_hits": len(c.forms_with_hits),
        "zero_hit_forms": list(c.zero_hit_forms),
        "mentions": len(w.mentions),
        "distinct_snippets": len(c.items),
        "discovered_casings": dict(c.discovered_casings),
        "short_forms": list(c.scan.short_forms),
        "excluded_pages": list(c.excluded_pages),
    }


def _window_bases(result: FoldResult) -> dict[int, int]:
    """Each window's document-absolute base, best effort.

    The dump is a witness, not a result: a window whose id is not the
    sub-window bounds simply gets no absolute spans, where the stored fold —
    which has nothing but offsets — refuses to be built at all.
    """
    bases: dict[int, int] = {}
    for window in result.windows:
        try:
            bases[window.window_index] = window_base_offset(window)
        except WindowBoundsMismatch:
            continue
    return bases


def build_fold_dump(result: FoldResult, *, subject_name: Optional[str] = None) -> dict[str, Any]:
    windows = result.windows
    bases = _window_bases(result)
    # Entries are what synthesis actually reads, so their distribution is the
    # standing watch on decentralization's known cost (D8 reversed 2026-08-27):
    # a generic head noun now inherits every specific mention, and `door` went
    # 60 -> 157 entries on the measured run. Uncapped by decision; watched here.
    entry_counts = sorted(
        (len(b.distinct_snippets()) for b in result.bundles if not b.is_empty),
        reverse=True,
    )
    return {
        "normalizer_version": result.normalizer_version,
        "verb_fold": result.verb_fold,
        "collapse_compounds": result.collapse_compounds,
        "summary": {
            "groups": len(result.bundles),
            "empty_groups": len(result.empty_bundles),
            "collapsed_groups": len(result.collapsed_bundles),
            "synthesized_groups": len(result.synthesis_records()),
            "max_entries_in_a_group": entry_counts[0] if entry_counts else 0,
            "groups_over_50_entries": sum(1 for n in entry_counts if n > 50),
            "mentions": sum(len(b.mentions) for b in result.bundles),
            "mentions_located": sum(
                1 for b in result.bundles for m in b.mentions if m.location is not None
            ),
            "windows": len(windows),
            "distinct_snippets": sum(len(w.collection.items) for w in windows),
            "zero_hit_forms": sum(len(w.collection.zero_hit_forms) for w in windows),
            "discovered_casings": sum(
                len(c) for w in windows for c in w.collection.discovered_casings.values()
            ),
            "windows_with_excluded_pages": sum(
                1 for w in windows if w.collection.excluded_pages
            ),
        },
        "groups": [_bundle_row(b, subject_name, bases) for b in result.bundles],
        "windows": [_window_row(w) for w in windows],
    }
