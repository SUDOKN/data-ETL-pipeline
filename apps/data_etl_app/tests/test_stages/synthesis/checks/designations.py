"""Designation preservation (added 2026-09-05 with the wire port; scoped to
the focal-form rule 2026-09-10).

The six synthesis statics carry one rule the harness has a number for — the
focal-form designation paragraph (decision D2, 2026-09-10):

    "Where the focal_form itself carries a designation — a grade, a model
    code, a standard's number — keep it verbatim; where a snippet writes such
    a designation directly with the focal entity as its own (the entity's own
    grades, codes, or numbers), keep those verbatim too. Designations, names,
    and details that a snippet attaches to other entities — neighbours in the
    same list, table cell, or sentence — are not part of this record ..."

The pipeline enforces the first half mechanically: ``core.utils.designation_
tokens`` is the conservation check behind the stage's under-enumeration RETRY
(``designation_coverage`` / ``resolve_under_enumeration`` in the synthesis
node service — a record whose first answer drops a designation it OWNS is
re-asked, and the better-covering answer is kept, the first on ties). A
record owns the designations inside its focal form plus those written
directly beside it in a snippet, the spans cut at the chunk's SIBLING FORMS
(the member forms of the chunk's other synthesized groups, read here off the
dump's fold block). This module recomputes that check with the CURRENT core
implementation over the ACCEPTED paragraphs — the same pinned-lint convention
as ``focal_form_lint`` (its source hash is stamped via ``lints.lint_versions``)
— so the number reported is the residue AFTER the pipeline's own
retry-and-resolve.

Two numbers come out of it, both WATCH numbers, never gates:

- ``designation_preservation`` — tokens the record owns that the paragraph
  carries (the old proxy, now over the scoped demand; before 2026-09-10 it
  demanded every token in every snippet and 82.8% of those belonged to other
  records, which is why it read 61.7% then);
- ``own_designation_drops`` — records whose FOCAL FORM's own tokens are
  missing from the paragraph (baseline 0 on run 20260905T213127 under every
  trigger variant; a non-zero value is the first sign the new paragraph
  costs recall).

Every dropped token is handed to the judge in the work order; J2's
designation clause (own designations only) decides. The collapse-phrase scan
is the harness's own nomination for the paragraph's "coverage of what the
snippets say about the focal entity" sentence.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Mapping, Optional, Sequence

from core.utils.designation_tokens import (
    designation_shaped_tokens,
    designation_tokens,
    missing_designations,
    own_tokens,
)

__all__ = [
    "designation_shaped_tokens",
    "designation_tokens",
    "missing_designations",
    "own_tokens",
    "dropped_designations",
    "report",
]

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
    snippets: Iterable[str],
    synthesis: str,
    focal_form: str,
    sibling_forms: Sequence[str] = (),
) -> dict[str, Any]:
    """Which of the designations the record OWNS the synthesis does not carry
    (core's conservation check, verbatim).

    Returns ``{"tokens": [all demanded, sorted], "dropped": [missing, sorted],
    "own_tokens": [the focal form's own, sorted], "own_dropped": [missing
    among those, sorted], "collapse_phrases": [...]}`` — ``collapse_phrases``
    lists the forbidden-substitute phrases found in the synthesis, reported
    only when the record owned designations at all.
    """
    snippets = list(snippets)
    demanded: set[str] = set(own_tokens(focal_form))
    for text in snippets:
        demanded |= designation_tokens(text, focal_form=focal_form, sibling_forms=sibling_forms)
    dropped = missing_designations(
        snippets, synthesis or "", focal_form=focal_form, sibling_forms=sibling_forms
    )
    own = own_tokens(focal_form)
    collapse = _COLLAPSE_RE.findall(synthesis or "") if demanded else []
    return {
        "tokens": sorted(demanded),
        "dropped": sorted(dropped),
        "own_tokens": sorted(own),
        "own_dropped": sorted(own & dropped),
        "collapse_phrases": collapse,
    }


def report(
    records: Iterable[Any],
    evidence_index: Optional[dict[str, Any]],
    evidence_for,
    sibling_forms: Optional[Mapping[tuple[str, str], Sequence[str]]] = None,
) -> dict[str, Any]:
    """The per-field summary + per-record detail over synthesized records.

    ``evidence_for(index, record)`` resolves a record's snapshot entry (kept as
    a parameter so this module stays import-free of the snapshot code).
    ``sibling_forms`` maps ``(chunk_bounds, group_id)`` to the member forms of
    the chunk's other synthesized groups (``loading.fold_sibling_forms``);
    without it the spans are cut only by the text's own punctuation, which
    over-demands.
    """
    per_record: dict[tuple, dict[str, Any]] = {}
    records_with = 0
    tokens_total = 0
    tokens_preserved = 0
    records_fully = 0
    records_collapsing = 0
    records_with_own = 0
    own_tokens_total = 0
    own_dropped_total = 0
    records_own_dropped = 0
    if evidence_index is None:
        return {"summary": None, "per_record": per_record}
    for record in records:
        if not record.synthesis or not record.focal_form:
            continue
        ev = evidence_for(evidence_index, record)
        if ev is None:
            continue
        siblings = (sibling_forms or {}).get((record.chunk_bounds, record.group_id)) or ()
        detail = dropped_designations(
            ev.get("snippets") or [],
            record.synthesis,
            focal_form=record.focal_form,
            sibling_forms=siblings,
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
        if detail["own_tokens"]:
            records_with_own += 1
            own_tokens_total += len(detail["own_tokens"])
            own_dropped_total += len(detail["own_dropped"])
            if detail["own_dropped"]:
                records_own_dropped += 1
    summary = {
        "records_with_designations": records_with,
        "tokens": tokens_total,
        "tokens_preserved": tokens_preserved,
        "token_rate": round(tokens_preserved / tokens_total, 4) if tokens_total else None,
        "records_fully_preserved": records_fully,
        "record_rate": round(records_fully / records_with, 4) if records_with else None,
        "records_with_collapse_phrase": records_collapsing,
        # WATCH: the focal form's own tokens (baseline 0 dropped, run 20260905T213127).
        "records_with_own_tokens": records_with_own,
        "own_tokens": own_tokens_total,
        "own_tokens_dropped": own_dropped_total,
        "records_with_own_drop": records_own_dropped,
    }
    return {"summary": summary, "per_record": per_record}
