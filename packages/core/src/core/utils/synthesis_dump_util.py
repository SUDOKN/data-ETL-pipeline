"""The synthesis stage as a dump reader meets it (PIPELINE_V3_PLAN.md Phase
3.2; the group-aware dump proper is Phase 4.1).

One block per chunk: the counts, then one row per record — the group's key and
member forms (so a wrong merge is visible next to what was written about it),
the focal form the model was told, how many snippets it saw, the synthesis it
returned (or why none), and the two lints over that synthesis — the own-name
count, and
whether an entity-shaped focal form went missing from the paragraph written for
it (``focal_form_lint``) — plus the ids the model answered that were never sent
and the ids a retry re-asked for.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Optional

from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    ChunkSynthesisResult,
    pack_records,
)
from core.utils.focal_form_lint import focal_form_absent
from core.utils.subject_name_lint import count_own_name_hits

STATUS_SYNTHESIZED = "synthesized"
STATUS_NOT_SYNTHESIZED = "not_synthesized"


def _identical_synthesis_groups_by_request(
    result: ChunkSynthesisResult, max_entries_per_request: int
) -> dict[str, int]:
    """The identical-synthesis tripwire (v3 plan §10, user decision
    2026-08-24): group_id → 0-based request index, for every record whose
    synthesis string is BYTE-IDENTICAL to another record's in the same
    first-pass request. A dump counter in the posture of the lints — measured
    at 12–23 same-request pairs per full run, almost all accurate composite
    sentences reused across co-packed records — NEVER a retry trigger.
    Request membership is recomputed through ``pack_records``, the same pure
    packing the node minted its ids from."""
    flagged: dict[str, int] = {}
    for request_index, group in enumerate(
        pack_records(result.records, max_entries_per_request)
    ):
        by_synthesis: dict[str, list[str]] = {}
        for record in group:
            synthesis = result.synthesis_of(record.record_id)
            if synthesis is None:
                continue
            by_synthesis.setdefault(synthesis, []).append(record.record_id)
        for record_ids in by_synthesis.values():
            if len(record_ids) < 2:
                continue
            for record_id in record_ids:
                flagged[record_id] = request_index
    return flagged


def build_synthesis_dump(
    result: ChunkSynthesisResult,
    *,
    subject_name: Optional[str] = None,
    max_entries_per_request: Optional[int] = None,
) -> dict[str, Any]:
    fold = result.fold
    bundle_of = {b.group_id: b for b in fold.bundles}
    identical_by_group: dict[str, int] = (
        _identical_synthesis_groups_by_request(result, max_entries_per_request)
        if max_entries_per_request is not None
        else {}
    )
    rows: list[dict[str, Any]] = []
    for record in result.records:
        bundle = bundle_of.get(record.record_id)
        synthesis = result.synthesis_of(record.record_id)
        row: dict[str, Any] = {
            "group_id": record.record_id,
            "key": bundle.key if bundle else None,
            "forms": list(bundle.forms) if bundle else [],
            "focal_form": record.focal_form,
            "snippets": len(record.snippets),
            "mention_count": len(bundle.mentions) if bundle else None,
            "status": STATUS_SYNTHESIZED if synthesis is not None else STATUS_NOT_SYNTHESIZED,
            "retried": record.record_id in result.answer.retried_record_ids,
            "synthesis": synthesis,
        }
        # The four labels the model decided before the paragraph (2026-09-13);
        # absent on answers that carry none (older runs, fixtures).
        record_answer = result.answer.syntheses.get(record.record_id)
        labels = record_answer.labels if record_answer is not None else None
        if labels is not None:
            row["labels"] = labels
        if subject_name and synthesis:
            hits = count_own_name_hits(synthesis, subject_name)
            if hits:
                row["own_name_hits_in_synthesis"] = hits
        if synthesis and focal_form_absent(
            synthesis, record.focal_form, row["forms"]
        ):
            row["focal_form_absent"] = True
        # Present only on violation, like every lint field.
        if record.record_id in identical_by_group:
            row["identical_synthesis_in_request"] = identical_by_group[
                record.record_id
            ]
        rows.append(row)
    syntheses = [r["synthesis"] for r in rows if r["synthesis"]]
    labelled = [r["labels"] for r in rows if "labels" in r]
    return {
        "summary": {
            "records": len(result.records),
            "snippets": sum(len(r.snippets) for r in result.records),
            "synthesized": sum(1 for r in rows if r["status"] == STATUS_SYNTHESIZED),
            "not_synthesized": list(result.not_synthesized),
            "retried": list(result.answer.retried_record_ids),
            "unknown_answer_ids": list(result.answer.unknown_answer_ids),
            "group_requests": result.group_request_count,
            "retry_requests": result.retry_request_count,
            "synthesis_chars": sum(len(s) for s in syntheses),
            "own_name_hits_in_syntheses": sum(
                r.get("own_name_hits_in_synthesis", 0) for r in rows
            ),
            "focal_form_absent_records": sum(
                1 for r in rows if r.get("focal_form_absent")
            ),
            # Records whose synthesis string is byte-identical to a co-packed
            # record's (the §10 tripwire); absent when the cap was not known.
            **(
                {
                    "identical_synthesis_records_in_request": sum(
                        1
                        for r in rows
                        if "identical_synthesis_in_request" in r
                    )
                }
                if max_entries_per_request is not None
                else {}
            ),
            # Label distributions (2026-09-13); present only when the answers
            # carry labels.
            **(
                {
                    "labelled_records": len(labelled),
                    "doer_counts": dict(Counter(l["doer"] for l in labelled)),
                    "capacity_counts": dict(Counter(l["capacity"] for l in labelled)),
                }
                if labelled
                else {}
            ),
        },
        "records": rows,
    }
