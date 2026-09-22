"""The Step 2 descent stage's request and parse layer (2026-09-22).

A descent request carries an accepted parent's ACCEPTED records (record
blocks in the user context, subject-keyed) and, in the system text, the
descent prompt with its two per-field placeholders filled: the parent's
name, and the parent's children as the dash-line outline with definitions —
or an empty list for the leaf step. Its answer is the record-major structural
wire: per record the children matched (with the quote that supplies the
narrowing feature), sibling proposals, or the declination that lets the
parent stand. The parse holds options to the parent's children: a vocabulary
label named that is NOT a child is a false child, recorded and dropped (the
parent stands, user decision 2026-09-22); anything else off the list is a
proposal.
"""

from __future__ import annotations

import logging
import traceback
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error_capped,
)

from core.models.extraction_schemas.catalog_wire_schema import response_format_for
from core.models.extraction_schemas.grounding import (
    RecordGroundingEntry,
    RecordGroundingResults,
)
from core.models.extraction_schemas.synthesis import GroupRecords
from core.models.field_types import ConceptFieldType
from core.models.rule_catalog import STAGE_DESCENT, RuleCatalog
from core.models.skos_concept import Concept
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_record_ids,
    render_record_blocks,
    sent_record_ids_from_user_message,
    sent_records_from_user_message,
)
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    parse_record_grounding_structural_result,
    subject_keyed_payloads,
)
from core.services.rule_catalog_registry import get_rule_catalog
from core.utils.rdf_to_graph_util import render_concept_outline

logger = logging.getLogger(__name__)

DESCENT_LABEL = "descent"
NO_CHILDREN_LINE = "(the vocabulary holds nothing narrower)"


def descent_catalog_for(field_name: str) -> RuleCatalog:
    return get_rule_catalog(STAGE_DESCENT, field_name)


def descent_request_payload(
    group_records: GroupRecords, record_ids: Sequence[str], children: Sequence[Concept]
) -> dict[str, Any]:
    """What a descent request is built from and what its ``|ud=`` digest is
    over: the parent's accepted records (subject-keyed) and the children
    offered, by name."""
    missing = sorted(rid for rid in record_ids if rid not in group_records)
    if missing:
        raise ValueError(f"descent_request_payload: no group record for id(s) {missing}")
    records = subject_keyed_payloads({rid: group_records[rid].model_dump() for rid in sorted(set(record_ids))})
    return {"records": records, "children": sorted(c.name for c in children)}


def render_descent_prompt(
    prompt: Prompt, field_type: ConceptFieldType, parent: Concept, children: Sequence[Concept]
) -> str:
    """The system text: the descent prompt with the parent's name and the
    children's dash-line outline (definitions on their own lines) in place of
    the field's two placeholders; the leaf step lists nothing."""
    parent_stem, child_stem = field_type.recursive_grounding_placeholders
    outline = render_concept_outline(list(children), with_definitions=True) if children else NO_CHILDREN_LINE
    return prompt.text.replace(parent_stem, parent.name).replace(child_stem, outline)


def create_deferred_descent_gpt_request(
    *,
    deferred_at: datetime,
    subject_unique_id: str,
    field_type: ConceptFieldType,
    request_id: str,
    prompt: Prompt,
    parent: Concept,
    children: Sequence[Concept],
    payload: Mapping[str, Any],
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    return create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context=render_record_blocks(dict(payload["records"])),
        prompt_text=render_descent_prompt(prompt, field_type, parent, children),
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(response_format_for(descent_catalog_for(field_type.name))),
        batch_id="Eager" if eager else None,
    )


class DescentAnswer:
    """One descent request's parsed answer: per record the children matched
    (canonical names) with their rules, the proposals, the declinations, and
    the false children."""

    def __init__(self) -> None:
        self.reached: dict[str, RecordGroundingResults] = {}  # child → {record: entry(tags={child: rules})}
        self.proposals: dict[str, RecordGroundingResults] = {}  # proposal → {record: entry}
        self.declined: dict[str, str] = {}  # record → reason
        self.false_children: dict[str, list[str]] = {}  # record → labels
        self.answered: set[str] = set()


async def parse_descent_request(
    *,
    subject_unique_id: str,
    field_name: str,
    req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    children: Sequence[Concept],
    vocabulary_names: Mapping[str, str],
    timestamp: datetime,
) -> DescentAnswer:
    """Parse one descent (or leaf-step) request. Options are held to the
    parent's children by name or other name; a proposal that is a vocabulary
    label elsewhere is a FALSE CHILD (dropped, recorded); a record the answer
    left out is a declination without a reason (the parent stands)."""
    req_obj = completed_request_map.get(req_id)
    if not req_obj:
        raise ValueError(f"{DESCENT_LABEL}: Missing GPTBatchRequest for request ID {req_id} in {subject_unique_id}:{field_name}")
    elif not req_obj.response:
        raise ValueError(f"{DESCENT_LABEL}: GPTBatchRequest for request ID {req_id} has no response_blob in {subject_unique_id}:{field_name}")
    child_labels: list[str] = [label for c in children for label in [c.name, *c.altLabels]]
    child_name_of = {label.casefold(): c.name for c in children for label in [c.name, *c.altLabels]}
    try:
        user_message = req_obj.request.body.user_message()
        sent = sent_records_from_user_message(user_message)
        parsed = parse_record_grounding_structural_result(
            req_obj.response.result,
            catalog=descent_catalog_for(field_name),
            allowed_labels=child_labels,
            sent_records=sent if isinstance(sent, dict) else None,
        )
        held = hold_response_to_sent_record_ids(
            user_message=user_message, response_by_record_id=parsed,
            where=f"{subject_unique_id}:{field_name} {DESCENT_LABEL} {req_id}", on_missing="drop",
        )
        sent_ids = sent_record_ids_from_user_message(user_message) or []
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=req_obj, error_message=str(e), timestamp=timestamp, traceback_str=traceback.format_exc(),
        )
        logger.error(f"{DESCENT_LABEL}: Error parsing results for subject {subject_unique_id} from GPT response: {e}")
        raise

    answer = DescentAnswer()
    answer.answered = set(held)
    for rid, entry in held.items():
        if not entry.tags:
            answer.declined[rid] = entry.explanation or ""
            continue
        for label, rules in entry.tags.items():
            child = child_name_of.get(label.casefold())
            if child is not None:
                answer.reached.setdefault(child, {})[rid] = RecordGroundingEntry(tags={child: rules})
                continue
            elsewhere = vocabulary_names.get(label.casefold())
            if elsewhere is not None:
                answer.false_children.setdefault(rid, []).append(elsewhere)
                logger.warning(f"{DESCENT_LABEL}: record {rid} named {elsewhere!r}, a vocabulary label that is not a child here; recorded as a false child")
                continue
            answer.proposals.setdefault(label, {})[rid] = RecordGroundingEntry(tags={label: rules})
    for rid in sent_ids:
        if rid not in held:
            logger.warning(f"{DESCENT_LABEL}: record {rid} unanswered in {req_id}; the parent stands for it")
    return answer
