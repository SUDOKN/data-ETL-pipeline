"""The Step 2 concept dump (cutover 6c, 2026-09-22): the rows and the chunk
block that show the descent LEVEL BY LEVEL, as the user required — a reader
can follow a record from the label the grounding call gave it, wave by wave
(its verdict, what the descent under it reached, proposed or declined, the
false children, the pairs removed under a failed ancestor), down to the
deepest accepted label the reconcile step shipped, and then through the
proposal wave.

Two views of one trail: the ROW is per record (every wave's nodes for that
record), the CHUNK BLOCK is per wave (every unit, verdict and answer of that
wave across records). Both are rendered from the stored ``DescentTrail`` and
the reconcile decision, never recomputed from the model's answers.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Mapping, Optional

from core.models.extraction_schemas.descent import DescentTrail, WaveTrail
from core.models.extraction_schemas.grounding import (
    RecordGroundingEntry,
    RecordGroundingResults,
)
from core.models.extraction_schemas.screening import CandidateScreeningVerdict
from core.models.extraction_schemas.search import LLMSearchResults
from core.utils.extraction_dump_util import (
    _base_group_row,
    _grounding_dump,
    _group_record_status,
    _screening_dump,
)

if TYPE_CHECKING:
    from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
        ChunkSynthesisResult,
    )

logger = logging.getLogger(__name__)


def _quote_of_tags(entry: Optional[RecordGroundingEntry]) -> dict[str, str]:
    """label → the quote it carries (the evidence condition's explanation)."""
    if entry is None:
        return {}
    out: dict[str, str] = {}
    for label, rules in entry.tags.items():
        out[label] = next((r.explanation for r in rules if r.outcome in ("satisfied", "unverified") and r.explanation), "")
    return out


def _verdict_dump(verdict: CandidateScreeningVerdict) -> dict[str, Any]:
    dump: dict[str, Any] = {"passed": verdict.passed}
    if verdict.evidence:
        dump["evidence"] = verdict.evidence
    if verdict.failed_rule:
        dump["failed_rule"] = verdict.failed_rule
    if verdict.quote:
        dump["quote"] = verdict.quote
    return dump


def _answer_dump(entry: RecordGroundingEntry, vocabulary_children: Optional[set[str]] = None) -> dict[str, Any]:
    """One record's descent (or leaf) answer: the children reached and the
    proposals, each with its quote, or the declination."""
    if not entry.tags:
        return {"declined": entry.explanation}
    quotes = _quote_of_tags(entry)
    if vocabulary_children is None:
        return {"answered": quotes}
    reached = {label: q for label, q in quotes.items() if label in vocabulary_children}
    proposed = {label: q for label, q in quotes.items() if label not in vocabulary_children}
    dump: dict[str, Any] = {}
    if reached:
        dump["reached"] = reached
    if proposed:
        dump["proposed"] = proposed
    return dump


def _record_wave_nodes(wave: WaveTrail, record_id: str, children_of: Mapping[str, set[str]]) -> list[dict[str, Any]]:
    """The wave's nodes for one record: each unit the record was screened
    under (verdict, then what the descent or leaf step under it did), and
    each pair removed before screening under a failed ancestor."""
    nodes: list[dict[str, Any]] = []
    verdicts = wave.screening.get(record_id, {})
    for label in sorted(wave.units):
        if record_id not in wave.units[label]:
            continue
        node: dict[str, Any] = {"label": label}
        verdict = verdicts.get(label)
        node["verdict"] = _verdict_dump(verdict) if verdict is not None else None
        descent = wave.descent.get(label, {}).get(record_id)
        if descent is not None:
            node["descent"] = _answer_dump(descent, children_of.get(label, set()))
        leaf = wave.leaf.get(label, {}).get(record_id)
        if leaf is not None:
            node["leaf_step"] = _answer_dump(leaf, set())
        false_children = wave.false_children.get(label, {}).get(record_id)
        if false_children:
            node["false_children"] = list(false_children)
        nodes.append(node)
    for label in sorted(wave.removed_under_failed_ancestor):
        if record_id in wave.removed_under_failed_ancestor[label]:
            nodes.append({"label": label, "removed_under_failed_ancestor": True})
    return nodes


def _children_reached_by_parent(trail: DescentTrail) -> dict[str, set[str]]:
    """parent → every vocabulary label a descent under it reached (the units
    of the next wave that came from it), so an answer's tags can be told
    apart as reached children vs proposals without the vocabulary in hand."""
    proposals = {label.casefold() for label in trail.proposal_units}
    out: dict[str, set[str]] = {}
    for wave in trail.waves.values():
        for parent, per_record in wave.descent.items():
            for entry in per_record.values():
                out.setdefault(parent, set()).update(label for label in entry.tags if label.casefold() not in proposals)
    return out


def _merged_verdicts(trail: DescentTrail, record_id: str) -> Optional[dict[str, CandidateScreeningVerdict]]:
    merged: dict[str, CandidateScreeningVerdict] = {}
    seen = False
    for wave in sorted(trail.waves):
        by_candidate = trail.waves[wave].screening.get(record_id)
        if by_candidate is not None:
            seen = True
            merged.update(by_candidate)
    by_candidate = trail.proposal_screening.get(record_id)
    if by_candidate is not None:
        seen = True
        merged.update(by_candidate)
    return merged if seen else None


def _status_from_step2_fields(row: dict[str, Any]) -> str:
    """The second witness for Step 2 rows, from the row's own fields alone."""
    if not row.get("mention_count"):
        return "no_mentions"
    record = row.get("record")
    if not isinstance(record, dict) or not record.get("synthesis"):
        return "not_synthesized"
    tags: set[str] = set()
    for key in ("grounding", "proposals"):
        pass_dump = row.get(key)
        if isinstance(pass_dump, dict):
            tags.update(pass_dump.get("tags", {}))
    if not tags:
        return "no_candidates"
    verdicts: list[dict[str, Any]] = []
    for nodes in (row.get("descent_levels") or {}).values():
        verdicts.extend(node["verdict"] for node in nodes if isinstance(node.get("verdict"), dict))
    proposal_wave = row.get("proposal_wave")
    if isinstance(proposal_wave, dict):
        verdicts.extend(v for v in proposal_wave.values() if isinstance(v, dict))
    if not verdicts and row.get("proposal_wave") is None and not row.get("descent_levels"):
        return "screening_dropped"
    if any(v.get("passed") for v in verdicts):
        return "grounded"
    return "screened_out"


def build_step2_concept_group_rows(
    *,
    synthesis_result: "ChunkSynthesisResult",
    grounding_flat: RecordGroundingResults,
    proposals_flat: RecordGroundingResults,
    proposal_pass_flat: Optional[RecordGroundingResults],
    trail: DescentTrail,
    shipped_by_record: Mapping[str, list[str]],
    search_rounds: dict[int, LLMSearchResults],
    subject_name: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Step 2 concept rows, one per GROUP the fold built: ``grounding`` (the
    one call's vocabulary matches, or its declination), ``proposal_pass``
    (present exactly when the pass ran), ``proposals`` (every label proposed
    for the record before descent, with quotes), ``descent_levels`` (wave →
    the record's nodes, level by level), ``proposal_wave`` (the record's
    proposal verdicts), ``shipped`` (what the reconcile step kept for the
    record), and ``status``."""
    focal_by_group = {r.record_id: r.focal_form for r in synthesis_result.records}
    children_of = _children_reached_by_parent(trail)

    rows: list[dict[str, Any]] = []
    for bundle in synthesis_result.fold.bundles:
        group_id = bundle.group_id
        synthesis = synthesis_result.synthesis_of(group_id)
        row = _base_group_row(
            group_id=group_id,
            focal_form=focal_by_group.get(group_id),
            forms=list(bundle.forms),
            key=bundle.key,
            mention_count=len(bundle.mentions),
            synthesis=synthesis,
            search_rounds=search_rounds,
            subject_name=subject_name,
        )
        grounding_entry = grounding_flat.get(group_id)
        row["grounding"] = _grounding_dump(grounding_entry)
        if proposal_pass_flat is not None:
            row["proposal_pass"] = _grounding_dump(proposal_pass_flat.get(group_id))
        proposals_entry = proposals_flat.get(group_id)
        row["proposals"] = _grounding_dump(proposals_entry)

        levels = {
            wave: _record_wave_nodes(trail.waves[wave], group_id, children_of)
            for wave in sorted(trail.waves)
        }
        row["descent_levels"] = {wave: nodes for wave, nodes in levels.items() if nodes}
        row["proposal_wave"] = _screening_dump(trail.proposal_screening.get(group_id))
        accepted_here = sorted(
            label for label, v in trail.proposal_screening.get(group_id, {}).items() if v.passed
        )
        row["shipped"] = {
            "in_vocab": list(shipped_by_record.get(group_id, [])),
            "out_of_vocab": accepted_here,
        }

        candidates: set[str] = set()
        for entry in (grounding_entry, proposals_entry):
            if entry is not None:
                candidates.update(entry.tags)
        status = _group_record_status(
            mention_count=len(bundle.mentions),
            synthesis=synthesis,
            candidates=candidates,
            verdicts=_merged_verdicts(trail, group_id),
        )
        row["status"] = status
        rederived = _status_from_step2_fields(row)
        if rederived != status:
            logger.error(
                f"extraction dump row inconsistency for group {group_id!r}: writer chose "
                f"status {status!r} but the row's fields read as {rederived!r}"
            )
        rows.append(row)
    return rows


def _verdict_summary(screening: Mapping[str, Mapping[str, CandidateScreeningVerdict]]) -> dict[str, dict[str, Any]]:
    """label → {accepted: [records], rejected: {record: failed rule}}."""
    out: dict[str, dict[str, Any]] = {}
    for rid in sorted(screening):
        for label, verdict in screening[rid].items():
            slot = out.setdefault(label, {"accepted": [], "rejected": {}})
            if verdict.passed:
                slot["accepted"].append(rid)
            else:
                slot["rejected"][rid] = verdict.failed_rule
    return {label: out[label] for label in sorted(out)}


def build_descent_trail_dump(
    trail: DescentTrail,
    shipped_by_record: Mapping[str, list[str]],
    out_of_vocab: list[str],
) -> dict[str, Any]:
    """The chunk's descent per LEVEL: for each wave its units, the pairs
    removed under a failed ancestor, the verdicts by label, every accepted
    parent's answers by record, the leaf steps and the false children; then
    the proposal wave; then what shipped."""
    children_of = _children_reached_by_parent(trail)
    waves: dict[int, dict[str, Any]] = {}
    for wave in sorted(trail.waves):
        wt = trail.waves[wave]
        block: dict[str, Any] = {
            "units": {label: list(wt.units[label]) for label in sorted(wt.units)},
            "verdicts": _verdict_summary(wt.screening),
        }
        if wt.removed_under_failed_ancestor:
            block["removed_under_failed_ancestor"] = {
                label: list(v) for label, v in sorted(wt.removed_under_failed_ancestor.items())
            }
        if wt.descent:
            block["descent"] = {
                parent: {rid: _answer_dump(entry, children_of.get(parent, set())) for rid, entry in sorted(per_record.items())}
                for parent, per_record in sorted(wt.descent.items())
            }
        if wt.leaf:
            block["leaf_step"] = {
                leaf: {rid: _answer_dump(entry, set()) for rid, entry in sorted(per_record.items())}
                for leaf, per_record in sorted(wt.leaf.items())
            }
        if wt.false_children:
            block["false_children"] = {
                parent: {rid: list(v) for rid, v in sorted(by_record.items())}
                for parent, by_record in sorted(wt.false_children.items())
            }
        waves[wave] = block
    proposal_wave = {
        label: {
            "sources": list(trail.proposal_sources.get(label, [])),
            "records": list(records),
            **_verdict_summary(trail.proposal_screening).get(label, {"accepted": [], "rejected": {}}),
        }
        for label, records in sorted(trail.proposal_units.items(), key=lambda kv: kv[0].casefold())
    }
    return {
        "max_depth": trail.max_depth,
        "waves": waves,
        "proposal_wave": proposal_wave,
        "shipped": {
            "in_vocab_by_record": {rid: list(labels) for rid, labels in sorted(shipped_by_record.items())},
            "out_of_vocab": list(out_of_vocab),
        },
    }
