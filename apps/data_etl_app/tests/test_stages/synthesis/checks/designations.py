"""Designation preservation (added 2026-09-05 with the wire port).

The six synthesis statics carry one rule the harness had no number for:

    "Where collected snippets carry specific designations of a general entity,
    preserve each specific in the synthesized record; never collapse them into
    the general term. ... a synthesis that names fewer designations than the
    snippets carry is wrong, and phrases like among others, various models or
    grades, or numerous codes are never a substitute for the designations
    themselves."

The pipeline enforces that rule mechanically: ``core.utils.designation_tokens``
is the conservation check behind the stage's under-enumeration RETRY
(``designation_coverage`` / ``resolve_under_enumeration`` in the synthesis
node service — records whose first answer drops designations are re-asked and
the better-covering answer is kept). This module recomputes that check with
the CURRENT core implementation over the ACCEPTED paragraphs — the same
pinned-lint convention as ``focal_form_lint`` (its source hash is stamped via
``lints.lint_versions``) — so the number reported is the residue AFTER the
pipeline's own retry-and-resolve.

It is still a WATCH number, never a gate: core's tiers are precision-first
(letter+digit tokens, ALL-CAPS/kind-word pairs, digits right after the focal
form), a designation in a snippet the model rightly set aside as not about the
focal entity counts as a drop here, and bare-number grades outside the focal
span are invisible. Every dropped token is handed to the judge in the work
order; J2's designation clause decides. The collapse-phrase scan is the
harness's own nomination for the "never a substitute" half of the rule.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Optional

from core.utils.designation_tokens import designation_tokens, missing_designations

__all__ = ["designation_tokens", "missing_designations", "dropped_designations", "report"]

# Substitutes the statics forbid when designations were available.
_COLLAPSE_RE = re.compile(
    r"\b(?:among others|and others|and more|and similar|and other [a-z]+|"
    r"various (?:models|grades|codes|types|sizes|series|standards|designations|"
    r"parts|products|alloys|materials|specifications)|"
    r"numerous (?:models|grades|codes|types|standards|parts|products|alloys)|"
    r"a (?:wide |broad |full )?(?:range|variety|number|selection) of|etc\.?)\b",
    re.I,
)


def dropped_designations(
    snippets: Iterable[str], synthesis: str, focal_form: Optional[str] = None
) -> dict[str, Any]:
    """Which of the snippets' designation tokens the synthesis does not carry
    (core's conservation check, verbatim).

    Returns {"tokens": [all, sorted], "dropped": [missing, sorted],
    "collapse_phrases": [...]} — ``collapse_phrases`` lists the forbidden-
    substitute phrases found in the synthesis, reported only when the snippets
    carried designations at all.
    """
    snippets = list(snippets)
    demanded: set[str] = set()
    for text in snippets:
        demanded |= designation_tokens(text, focal_form=focal_form)
    dropped = missing_designations(snippets, synthesis or "", focal_form=focal_form)
    collapse = _COLLAPSE_RE.findall(synthesis or "") if demanded else []
    return {"tokens": sorted(demanded), "dropped": sorted(dropped), "collapse_phrases": collapse}


def report(
    records: Iterable[Any], evidence_index: Optional[dict[str, Any]], evidence_for
) -> dict[str, Any]:
    """The per-field summary + per-record detail over synthesized records.

    ``evidence_for(index, record)`` resolves a record's snapshot entry (kept as
    a parameter so this module stays import-free of the snapshot code).
    """
    per_record: dict[tuple, dict[str, Any]] = {}
    records_with = 0
    tokens_total = 0
    tokens_preserved = 0
    records_fully = 0
    records_collapsing = 0
    if evidence_index is None:
        return {"summary": None, "per_record": per_record}
    for record in records:
        if not record.synthesis:
            continue
        ev = evidence_for(evidence_index, record)
        if ev is None:
            continue
        detail = dropped_designations(
            ev.get("snippets") or [], record.synthesis, focal_form=record.focal_form
        )
        if not detail["tokens"]:
            continue
        per_record[record.pair_key] = detail
        records_with += 1
        tokens_total += len(detail["tokens"])
        tokens_preserved += len(detail["tokens"]) - len(detail["dropped"])
        if not detail["dropped"]:
            records_fully += 1
        if detail["collapse_phrases"]:
            records_collapsing += 1
    summary = {
        "records_with_designations": records_with,
        "tokens": tokens_total,
        "tokens_preserved": tokens_preserved,
        "token_rate": round(tokens_preserved / tokens_total, 4) if tokens_total else None,
        "records_fully_preserved": records_fully,
        "record_rate": round(records_fully / records_with, 4) if records_with else None,
        "records_with_collapse_phrase": records_collapsing,
    }
    return {"summary": summary, "per_record": per_record}
