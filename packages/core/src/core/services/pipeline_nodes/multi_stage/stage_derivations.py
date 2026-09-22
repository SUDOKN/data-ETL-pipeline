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

from typing import Callable, Mapping, Optional, Sequence

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


def units_for_screening(
    *results: RecordGroundingResults,
    fold: Optional[Callable[[str], str]] = None,
) -> dict[str, list[str]]:
    """Step 2 (user decision 2026-09-21): grounding answers record by record,
    and the records tagged to the same label are regrouped HERE into one
    screening unit — ``label -> sorted record ids`` over every map given
    (vocabulary matches and proposals alike). Labels are casefold-deduped,
    the first spelling seen kept, so a proposal minted under two casings on
    two records is one unit. Records that yielded nothing are absent.

    ``fold`` (ruling 25, 2026-09-21) maps a label to the name it is a unit
    under before grouping — the concept fields pass the vocabulary's
    alias-to-name map, so a record tagged under an other name ("Assembly")
    and one tagged under the name ("Joining") make ONE unit, never the
    "alias twins" the census counted; a label the fold does not know (a
    proposal) is returned as given."""
    by_label: dict[str, set[str]] = {}
    spelling: dict[str, str] = {}
    for result_map in results:
        for record_id, entry in result_map.items():
            for label in entry.tags:
                name = fold(label) if fold is not None else label
                folded = name.casefold()
                spelling.setdefault(folded, name)
                by_label.setdefault(folded, set()).add(record_id)
    return {
        spelling[folded]: sorted(record_ids)
        for folded, record_ids in sorted(by_label.items(), key=lambda kv: spelling[kv[0]].casefold())
    }


UnitGroup = list[tuple[str, list[str]]]


def pack_units(units: Mapping[str, Sequence[str]], max_records: int) -> list[UnitGroup]:
    """Split one wave's units into request groups, each holding at most
    ``max_records`` DISTINCT records (D8: the cap is records, because the
    records are what the request renders and the model reads).

    Deterministic: units in label order, packed first-fit into the open
    group; a unit whose records exceed the cap is cut into SUB-UNITS of at
    most the cap (same label, disjoint records), and two sub-units of one
    label never share a request — the parser holds each request to its own
    units by label, and the wave's verdicts union per record afterwards.
    An empty unit map is one empty group (the zero-units chunk sends its
    single pre-answered dummy, as the record stages do)."""
    if max_records < 1:
        raise ValueError("pack_units: max_records must be >= 1")
    pieces: list[tuple[str, list[str]]] = []
    for label in sorted(units, key=lambda s: s.casefold()):
        ids = sorted(set(units[label]))
        if not ids:
            continue
        for i in range(0, len(ids), max_records):
            pieces.append((label, ids[i : i + max_records]))
    groups: list[UnitGroup] = []
    group_records: list[set[str]] = []
    group_labels: list[set[str]] = []
    for label, ids in pieces:
        placed = False
        for g, (records, labels) in enumerate(zip(group_records, group_labels)):
            if label in labels:
                continue  # a sub-unit of this label is already here
            if len(records | set(ids)) <= max_records:
                groups[g].append((label, ids))
                records.update(ids)
                labels.add(label)
                placed = True
                break
        if not placed:
            groups.append([(label, ids)])
            group_records.append(set(ids))
            group_labels.append({label})
    return groups or [[]]


# --- Step 2 descent in depth waves (2026-09-22) -------------------------------


def wave_group(
    direct: Mapping[str, Sequence[str]],
    reached: Mapping[str, Sequence[str]],
    failed_by_record: Mapping[str, set[str]],
    ancestors_of: Callable[[str], Sequence[str]],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """One depth wave's units: the labels matched directly at this depth and
    the ones descent reached from the wave before, deduplicated by (label,
    record); minus every pair whose label has an ancestor that FAILED
    screening on that same record (V13, applied structurally: a failed line
    is never re-litigated further down). Returns ``(units, removed)``, both
    label → sorted record ids."""
    merged: dict[str, set[str]] = {}
    for source in (direct, reached):
        for label, ids in source.items():
            merged.setdefault(label, set()).update(ids)
    units: dict[str, list[str]] = {}
    removed: dict[str, list[str]] = {}
    for label in sorted(merged, key=lambda s: s.casefold()):
        ancestors = set(ancestors_of(label))
        keep, drop = [], []
        for rid in sorted(merged[label]):
            (drop if ancestors & failed_by_record.get(rid, set()) else keep).append(rid)
        if keep:
            units[label] = keep
        if drop:
            removed[label] = drop
    return units, removed


def deepest_accepted_labels(
    accepted_by_record: Mapping[str, Sequence[str]],
    ancestors_of: Callable[[str], Sequence[str]],
) -> dict[str, list[str]]:
    """The reconcile rule (V11 as decided 2026-09-22): per record, an accepted
    label is dropped when one of its DESCENDANTS was accepted on the same
    record — the deepest accepted label replaces its ancestors, each on its
    own passed verdict; a parent whose children were rejected, or never
    screened, stands."""
    out: dict[str, list[str]] = {}
    for rid, labels in accepted_by_record.items():
        accepted = set(labels)
        covered = {a for label in accepted for a in ancestors_of(label)}
        kept = sorted(label for label in accepted if label not in covered)
        if kept:
            out[rid] = kept
    return out
