"""The aggregation fold as a dump reader meets it (PIPELINE_V3_PLAN.md Phase
3.1, amended 2026-08-22; the group-aware dump proper is Phase 4.1).

One block per chunk: a summary, the bundles (one entry per group — member forms
visible inline, so a wrong merge is human-visible at a glance — with mentions
in locked order, each saying where its location came from), and the per-window
report: what code collected (occurrences, distinct snippets, forms with and
without hits, casings the scan discovered, pages it excluded) and what the
Location stage covered (``described`` / ``not_described`` ids, the ids a retry
pass re-asked for, answer ids that were never sent).

A group that collapsed (D21) carries ``collapsed_into`` and status
``collapsed``; the summary separates the three populations a reader must not
confuse — ``groups`` (all of them), ``empty_groups`` and ``collapsed_groups``
(both skipped by synthesis, for different reasons), and ``synthesized_groups``
(what actually reached the LLM).
"""

from __future__ import annotations

from typing import Any, Optional

from core.utils.aggregation_fold import FoldResult, FoldedMention, MentionBundle, WindowFold
from core.utils.subject_name_lint import count_own_name_hits


def _mention_row(m: FoldedMention) -> dict[str, Any]:
    row: dict[str, Any] = {
        "form": m.form,
        "record_id": m.record_id,
        "window": m.window_index,
        "span": [m.start, m.end],
        "page": m.page,
        "mention_id": m.mention_id,
        "location": m.location,
        "location_source": m.location_source,
        "snippet": m.snippet,
    }
    if m.is_discovered_casing:
        row["sent_form"] = m.sent_form
    return row


def _bundle_row(b: MentionBundle, subject_name: Optional[str]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "group_id": b.group_id,
        "key": b.key,
        "forms": list(b.forms),
        "status": b.status,
        "mention_count": len(b.mentions),
        "distinct_snippets": len(b.synthesis_entries()),
        "mentions": [_mention_row(m) for m in b.mentions],
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
        "described": len(w.described),
        "not_described": list(w.not_described),
        "retried": list(w.retried),
        "unknown_answer_ids": list(w.unknown_answer_ids),
        "discovered_casings": dict(c.discovered_casings),
        "short_forms": list(c.scan.short_forms),
        "excluded_pages": list(c.excluded_pages),
    }


def build_fold_dump(result: FoldResult, *, subject_name: Optional[str] = None) -> dict[str, Any]:
    windows = result.windows
    # Entries are what synthesis actually reads, so their distribution is the
    # standing watch on decentralization's known cost (D8 reversed 2026-08-27):
    # a generic head noun now inherits every specific mention, and `door` went
    # 60 -> 157 entries on the measured run. Uncapped by decision; watched here.
    entry_counts = sorted(
        (len(b.synthesis_entries()) for b in result.bundles if not b.is_empty),
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
            "windows": len(windows),
            "distinct_snippets": sum(len(w.collection.items) for w in windows),
            "described": sum(len(w.described) for w in windows),
            "not_described": sum(len(w.not_described) for w in windows),
            "windows_with_undescribed": sum(1 for w in windows if w.has_undescribed),
            "retried": sum(len(w.retried) for w in windows),
            "windows_retried": sum(1 for w in windows if w.retried),
            "unknown_answer_ids": sum(len(w.unknown_answer_ids) for w in windows),
            "zero_hit_forms": sum(len(w.collection.zero_hit_forms) for w in windows),
            "discovered_casings": sum(
                len(c) for w in windows for c in w.collection.discovered_casings.values()
            ),
            "windows_with_excluded_pages": sum(
                1 for w in windows if w.collection.excluded_pages
            ),
        },
        "groups": [_bundle_row(b, subject_name) for b in result.bundles],
        "windows": [_window_row(w) for w in windows],
    }
