"""The aggregation fold as a dump reader meets it (PIPELINE_V3_PLAN.md Phase
3.1; the group-aware dump proper is Phase 4.1).

One block per chunk: a summary, the bundles (one entry per group — member forms
visible inline, so a wrong merge is human-visible at a glance — with mentions
in locked order), and the per-window hold: what the collector owed (tier-1
obligations after containment), what it missed (``unaccounted``), what it
reported that anchored nothing (``unlocated``/``unanchored``), what the fold
re-keyed, and the tier-2 discovery surface (casings no sent form covered).
"""

from __future__ import annotations

from typing import Any, Optional

from core.utils.aggregation_fold import FoldResult, FoldedMention, MentionBundle, WindowFold
from core.utils.floor_scan import Occurrence
from core.utils.subject_name_lint import count_own_name_hits


def _span(o: Occurrence) -> list[int]:
    return [o.start, o.end]


def _mention_row(m: FoldedMention) -> dict[str, Any]:
    row: dict[str, Any] = {
        "form": m.form,
        "record_id": m.record_id,
        "window": m.window_index,
        "span": [m.start, m.end],
        "page": m.page,
        "location": m.location,
        "snippet": m.snippet,
    }
    if m.rekeyed:
        row["reported_form"] = m.reported_form
    return row


def _bundle_row(b: MentionBundle, subject_name: Optional[str]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "group_id": b.group_id,
        "key": b.key,
        "forms": list(b.forms),
        "status": b.status,
        "mention_count": len(b.mentions),
        "mentions": [_mention_row(m) for m in b.mentions],
    }
    if subject_name:
        hits = sum(count_own_name_hits(m.snippet, subject_name) for m in b.mentions)
        if hits:
            row["own_name_hits_in_snippets"] = hits
    return row


def _window_row(w: WindowFold) -> dict[str, Any]:
    return {
        "window": w.window_index,
        "sub_bounds": w.window_id,
        "sent_forms": len(w.obligations),
        "mentions": len(w.mentions),
        "candidates": w.candidates,
        "obligations": sum(len(v) for v in w.obligations.values()),
        "unaccounted_count": w.unaccounted_count,
        "unaccounted": {
            form: [_span(o) for o in occs] for form, occs in w.unaccounted.items() if occs
        },
        "unlocated": [
            {"reported_form": r.reported_form, "location": r.location, "snippet": r.snippet}
            for r in w.unlocated
        ],
        "unanchored": [
            {
                "reported_form": r.reported_form,
                "location": r.location,
                "snippet": r.snippet,
                "position": r.position,
            }
            for r in w.unanchored
        ],
        "rekeyed": [
            {
                "reported_form": r.reported_form,
                "position": r.position,
                "attributed_forms": list(r.attributed_forms),
                "snippet": r.snippet,
            }
            for r in w.rekeyed
        ],
        "short_forms": list(w.scan.short_forms),
        "discovered_casings": dict(w.discovered_casings),
    }


def build_fold_dump(result: FoldResult, *, subject_name: Optional[str] = None) -> dict[str, Any]:
    windows = result.windows
    return {
        "normalizer_version": result.normalizer_version,
        "verb_fold": result.verb_fold,
        "summary": {
            "groups": len(result.bundles),
            "empty_groups": len(result.empty_bundles),
            "mentions": sum(len(b.mentions) for b in result.bundles),
            "windows": len(windows),
            "candidates": sum(w.candidates for w in windows),
            "obligations": sum(
                len(v) for w in windows for v in w.obligations.values()
            ),
            "unaccounted": sum(w.unaccounted_count for w in windows),
            "windows_with_discrepancy": sum(1 for w in windows if w.has_discrepancy),
            "unlocated": sum(len(w.unlocated) for w in windows),
            "unanchored": sum(len(w.unanchored) for w in windows),
            "rekeyed": sum(len(w.rekeyed) for w in windows),
            "discovered_casings": sum(
                len(c) for w in windows for c in w.discovered_casings.values()
            ),
        },
        "groups": [_bundle_row(b, subject_name) for b in result.bundles],
        "windows": [_window_row(w) for w in windows],
    }
