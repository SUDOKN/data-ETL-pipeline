"""Pure derivations between the phrase pipeline's stages.

Each stage's input is a function of upstream STORED shapes and nothing else, so
these live apart from the node services: the orchestration calls them, the
reconcile node calls them, and the tests pin them without any request
machinery.

Introduced as the v2 halves of PIPELINE_V2_PLAN.md phases 2.7/2.8, alongside the
descent loop and reconcile node that consume them. The module was called
``pipeline_v2_derivations`` until 2026-08-27; there is no other set of stage
derivations for the name to distinguish it from.
"""

from __future__ import annotations

from typing import Optional

from core.models.deferred_extraction.deferred_concept_extraction import (
    TaggingResult,
)
from core.models.extraction_schemas.grounding import RecordGroundingResults
from core.models.extraction_schemas.screening import RecordScreeningResults


def candidates_for_screening(
    in_vocab: RecordGroundingResults,
    out_of_vocab: Optional[RecordGroundingResults] = None,
) -> dict[str, list[str]]:
    """Each record's candidate list for the screening stage: its in-vocab tags
    plus its OOV discoveries, casefold-deduped preferring the in-vocab spelling
    (an OOV mint that restates a vocabulary label IS that label — one verdict,
    under the canonical name). Records that yielded nothing anywhere are absent:
    they have nothing to screen."""
    candidates: dict[str, list[str]] = {}
    folded_by_record: dict[str, set[str]] = {}

    def add(record_id: str, label: str) -> None:
        folded = label.casefold()
        seen = folded_by_record.setdefault(record_id, set())
        if folded in seen:
            return
        seen.add(folded)
        candidates.setdefault(record_id, []).append(label)

    for record_id, entry in in_vocab.items():
        for label in entry.tags:
            add(record_id, label)
    for record_id, entry in (out_of_vocab or {}).items():
        for label in entry.tags:
            add(record_id, label)

    return {record_id: sorted(labels) for record_id, labels in candidates.items()}


def passed_candidates_by_record(
    screening: RecordScreeningResults,
) -> dict[str, set[str]]:
    """Each record's candidates that passed screening; records where nothing
    passed are absent."""
    passed: dict[str, set[str]] = {}
    for record_id, verdicts in screening.items():
        survivors = {
            candidate for candidate, verdict in verdicts.items() if verdict.passed
        }
        if survivors:
            passed[record_id] = survivors
    return passed


def descent_seed_tagging_results(
    in_vocab: RecordGroundingResults,
    screening: RecordScreeningResults,
) -> list[TaggingResult]:
    """The recursive stage's entry set: every PASSED in-vocab candidate, grouped
    by tag across records — the same tag-major shape the v1 descent machinery
    consumes, with record ids where phrases used to be. Sorted by tag so the
    seed is deterministic across runs."""
    passed = passed_candidates_by_record(screening)
    by_tag: dict[str, dict[str, list]] = {}
    for record_id, entry in in_vocab.items():
        surviving = passed.get(record_id, set())
        for tag, rules in entry.tags.items():
            if tag not in surviving:
                continue
            by_tag.setdefault(tag, {})[record_id] = rules

    return [
        TaggingResult(group_id=tag, phrase_rules_map=by_record)
        for tag, by_record in sorted(by_tag.items())
    ]


def candidates_that_passed(
    grounding: RecordGroundingResults,
    screening: RecordScreeningResults,
) -> set[str]:
    """The grounding stage's labels that passed screening on at least one
    record. One qualifying record suffices, as in v1 — a label rejected on one
    record and proven on another is proven. Serves the OOV results bucket and
    the keyword (freehand) results alike."""
    passed = passed_candidates_by_record(screening)
    survivors: set[str] = set()
    for record_id, entry in grounding.items():
        surviving = passed.get(record_id, set())
        survivors.update(tag for tag in entry.tags if tag in surviving)
    return survivors
