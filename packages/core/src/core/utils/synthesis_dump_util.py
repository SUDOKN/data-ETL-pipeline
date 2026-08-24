"""The synthesis stage as a dump reader meets it (PIPELINE_V3_PLAN.md Phase
3.2; the group-aware dump proper is Phase 4.1).

One block per chunk: the arm and counts, then one row per record — the group's
key and member forms (so a wrong merge is visible next to what was written
about it), the focal form the model was told, how many entries it saw, the
synthesis it returned (or why none), and the two lints over that synthesis — the
own-name count, and whether an entity-shaped focal form went missing from the
paragraph written for it (``focal_form_lint``) — plus the ids the model answered
that were never sent and the ids a retry re-asked for.
"""

from __future__ import annotations

from typing import Any, Optional

from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    ChunkSynthesisResult,
)
from core.utils.focal_form_lint import focal_form_absent
from core.utils.subject_name_lint import count_own_name_hits

STATUS_SYNTHESIZED = "synthesized"
STATUS_NOT_SYNTHESIZED = "not_synthesized"


def build_synthesis_dump(
    result: ChunkSynthesisResult, *, subject_name: Optional[str] = None
) -> dict[str, Any]:
    fold = result.fold
    bundle_of = {b.group_id: b for b in fold.bundles}
    rows: list[dict[str, Any]] = []
    for record in result.records:
        bundle = bundle_of.get(record.record_id)
        synthesis = result.synthesis_of(record.record_id)
        row: dict[str, Any] = {
            "group_id": record.record_id,
            "key": bundle.key if bundle else None,
            "forms": list(bundle.forms) if bundle else [],
            "focal_form": record.focal_form,
            "entries": len(record.entries),
            "mention_count": len(bundle.mentions) if bundle else None,
            "status": STATUS_SYNTHESIZED if synthesis is not None else STATUS_NOT_SYNTHESIZED,
            "retried": record.record_id in result.answer.retried_record_ids,
            "synthesis": synthesis,
        }
        if subject_name and synthesis:
            hits = count_own_name_hits(synthesis, subject_name)
            if hits:
                row["own_name_hits_in_synthesis"] = hits
        if synthesis and focal_form_absent(
            synthesis, record.focal_form, row["forms"]
        ):
            row["focal_form_absent"] = True
        rows.append(row)
    syntheses = [r["synthesis"] for r in rows if r["synthesis"]]
    return {
        "include_location": result.include_location,
        "summary": {
            "records": len(result.records),
            "entries": sum(len(r.entries) for r in result.records),
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
        },
        "records": rows,
    }
