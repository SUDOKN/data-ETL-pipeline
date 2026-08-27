"""Pinned-version lint recompute.

Dump-stored lint counters are not comparable across lint-code versions (the
13→3 focal-form incident), so the harness recomputes every lint itself with
the CURRENT core implementations and stamps their source hashes into each
scorecard. Two scorecards agree on method only when their lint_versions agree.
"""

from __future__ import annotations

import hashlib
import inspect
from collections import defaultdict
from typing import Any, Iterable, Optional

from core.utils import focal_form_lint as _ffl
from core.utils import subject_name_lint as _snl
from core.utils.focal_form_lint import focal_form_absent, is_entity_shaped
from core.utils.subject_name_lint import count_own_name_hits

__all__ = [
    "focal_form_absent",
    "is_entity_shaped",
    "count_own_name_hits",
    "lint_versions",
    "identical_synthesis_clusters",
]


def _module_hash(module: object) -> str:
    source = inspect.getsource(module)  # type: ignore[arg-type]
    return hashlib.sha256(source.encode("utf-8")).hexdigest()[:12]


def lint_versions() -> dict[str, str]:
    return {
        "focal_form_lint": _module_hash(_ffl),
        "subject_name_lint": _module_hash(_snl),
    }


def identical_synthesis_clusters(
    rows: Iterable[tuple[str, Optional[str], Optional[str]]],
) -> list[dict[str, Any]]:
    """The identical-synthesis tripwire over ACTUAL request membership.

    ``rows`` = (group_id, synthesis, request_custom_id) triples; membership
    comes from the evidence snapshot (which request really carried the record),
    not a ``pack_records`` recompute. Returns one cluster per (request,
    synthesis string) shared by ≥2 records. An identical string on two records
    of one request is a collapse; most are accurate-but-undifferentiated
    composites — only a record whose own name is absent is a defect, and the
    focal-form lint catches exactly that subclass.
    """
    by_request_synthesis: dict[tuple[str, str], list[str]] = defaultdict(list)
    for group_id, synthesis, request_custom_id in rows:
        if not synthesis or not request_custom_id:
            continue
        by_request_synthesis[(request_custom_id, synthesis)].append(group_id)
    clusters = []
    for (request_custom_id, synthesis), group_ids in sorted(
        by_request_synthesis.items()
    ):
        if len(group_ids) >= 2:
            clusters.append(
                {
                    "request_custom_id": request_custom_id,
                    "group_ids": sorted(group_ids),
                    "synthesis_head": synthesis[:160],
                }
            )
    return clusters
