"""Pipeline v2's consolidated screening: every candidate judged, both axes held.

Candidates are SUPPLIED — grounding enumerated them — so this stage never
identifies anything: it judges the manufacturer's relationship with each
candidate against the record's mentions and synthesis, one verdict per
candidate. The v1 gate's no-candidate branch and ``identified_entity`` have no
counterpart here.

TWO AXES, BOTH EXACT. The id axis is held by the shared record hold; the
candidate axis is held against the request's OWN records payload — what this
request asked about each record is part of the request document, so the set
validated against is the set sent, by construction. Exact on both: ids and
candidates are supplied strings, so a response key that differs is corruption,
and matching it "helpfully" onto a neighbour is the silent misattribution the
whole record design exists to rule out.
"""

from __future__ import annotations

import logging
import traceback
from datetime import datetime
from typing import Any, Optional

from pydantic import ValidationError

from core.models.extraction_schemas.catalog_wire_schema import (
    flatten_rule_slots,
    response_format_for_v2,
    screening_response_model_v2,
)
from core.models.extraction_schemas.relationship import (
    MaskedLLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.screening import (
    CandidateScreeningVerdict,
    RecordScreeningResults,
)
from core.models.rule_catalog import RuleCatalog
from core.services.applied_rule_validation import (
    check_applied_rules,
    passed_implied_by,
    raise_for_violations,
)
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_record_ids,
    render_record_blocks,
    sent_records_from_user_message,
)
from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model, NO_MODEL
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error_capped,
)

logger = logging.getLogger(__name__)

# What a dummy (no candidates anywhere) screening request answers with.
DUMMY_SCREENINGS_RESPONSE_CONTENT = '{"screenings": []}'


def parse_record_screening_result(
    gpt_response: Optional[str],
    *,
    catalog: RuleCatalog,
) -> RecordScreeningResults:
    """One response's screenings as the stored record → candidate → verdict map.

    ``passed`` is derived by ``passed_implied_by``, never reported — the rules
    ARE the decision procedure, exactly as in v1. Violations are collected
    across the whole response and raised once; a candidate whose report failed
    its catalog contributes no verdict, and the raise discards the response.
    """
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_record_screening_result: Empty or invalid response from GPT"
        )

    try:
        parsed = screening_response_model_v2(catalog).model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_record_screening_result: Invalid response from GPT:{gpt_response}"
        ) from e

    results: RecordScreeningResults = {}
    violations: list[str] = []
    for entry in parsed.screenings:
        if entry.record_id in results:
            raise ValueError(
                f"parse_record_screening_result: Duplicate record id "
                f"{entry.record_id!r} in screenings response"
            )
        verdicts: dict[str, CandidateScreeningVerdict] = {}
        for unit in entry.candidates:
            if unit.candidate in verdicts:
                raise ValueError(
                    f"parse_record_screening_result: record {entry.record_id!r} "
                    f"judges candidate {unit.candidate!r} twice"
                )
            applied = flatten_rule_slots(catalog, unit)
            report = check_applied_rules(
                catalog=catalog,
                applied_rules=applied,
                where=f"record {entry.record_id} candidate {unit.candidate!r}",
            )
            if report.problems:
                violations.extend(report.problems)
                continue
            verdicts[unit.candidate] = CandidateScreeningVerdict(
                passed=passed_implied_by(catalog, applied),
                applied_rules=applied,
            )
        results[entry.record_id] = verdicts

    raise_for_violations(violations)
    return results


def hold_candidates_to_sent_records(
    *,
    user_message: str,
    held_results: RecordScreeningResults,
    where: str,
) -> None:
    """The candidate axis: every record's verdicts must cover exactly the
    candidates its request listed. Raises on either direction — an unjudged
    candidate is an unanswered question, and a verdict on a candidate nobody
    listed is a fabricated one."""
    sent_records = sent_records_from_user_message(user_message)
    if sent_records is None:
        return

    problems: list[str] = []
    for record_id, verdicts in held_results.items():
        sent_candidates = set(sent_records.get(record_id, {}).get("candidates", []))
        judged = set(verdicts)
        missing = sent_candidates - judged
        unsent = judged - sent_candidates
        if missing:
            problems.append(
                f"record {record_id}: no verdict came back for {sorted(missing)}"
            )
        if unsent:
            problems.append(
                f"record {record_id}: verdicts on candidate(s) never sent: "
                f"{sorted(unsent)}"
            )
    if problems:
        raise ValueError(f"{where}: " + "; ".join(problems))


async def parse_record_screening_group_result(
    *,
    subject_unique_id: str,
    field_name: str,
    catalog: RuleCatalog,
    group_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> RecordScreeningResults:
    """Parse one screening group request and hold it on both axes."""
    req_obj = completed_request_map.get(group_req_id)
    if not req_obj:
        raise ValueError(
            f"record_screening_v2: Missing GPTBatchRequest for request ID "
            f"{group_req_id} in {subject_unique_id}:{field_name}"
        )
    elif not req_obj.response:
        raise ValueError(
            f"record_screening_v2: GPTBatchRequest for request ID {group_req_id} "
            f"has no response_blob in {subject_unique_id}:{field_name}"
        )

    where = f"{subject_unique_id}:{field_name} record screening {group_req_id}"
    try:
        parsed = parse_record_screening_result(
            req_obj.response.result, catalog=catalog
        )
        user_message = req_obj.request.body.user_message()
        held = hold_response_to_sent_record_ids(
            user_message=user_message,
            response_by_record_id=parsed,
            where=where,
            on_missing="raise",
        )
        hold_candidates_to_sent_records(
            user_message=user_message, held_results=held, where=where
        )
        return held
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"record_screening_v2: Error parsing screening results for subject "
            f"{subject_unique_id} from GPT response: {e}"
        )
        raise


async def get_record_screening_result(
    *,
    subject_unique_id: str,
    field_name: str,
    chunk_bounds: str,
    catalog: RuleCatalog,
    group_req_ids: list[BatchRequestIDType],
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> RecordScreeningResults:
    """Merge screening verdicts across every group embedded for the chunk."""
    if not group_req_ids:
        raise ValueError(
            f"record_screening_v2: request id list is empty for chunk bounds "
            f"{chunk_bounds} in {subject_unique_id}:{field_name}"
        )

    merged: RecordScreeningResults = {}
    for group_req_id in group_req_ids:
        merged.update(
            await parse_record_screening_group_result(
                subject_unique_id=subject_unique_id,
                field_name=field_name,
                catalog=catalog,
                group_req_id=group_req_id,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
            )
        )
    return merged


def build_screening_payloads(
    masked: MaskedLLMPhraseRelationshipResults,
    candidates_by_record: dict[str, list[str]],
) -> dict[str, dict[str, Any]]:
    """The id → record payload a screening request renders: the evidence plus
    the candidates to judge. Only records WITH candidates are sent — a record
    grounding yielded nothing for has nothing to screen — and a candidate list
    for a record the relationship stage never produced is a pipeline bug."""
    payloads: dict[str, dict[str, Any]] = {}
    for record_id, candidates in candidates_by_record.items():
        entry = masked.get(record_id)
        if entry is None:
            raise ValueError(
                f"build_screening_payloads: candidates supplied for unknown "
                f"record id {record_id!r}"
            )
        if not candidates:
            continue
        payload: dict[str, Any] = entry.record.model_dump()
        payload["candidates"] = sorted(candidates)
        payloads[record_id] = payload
    return payloads


def create_deferred_record_screening_gpt_request(
    *,
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: str,
    prompt: Prompt,
    catalog: RuleCatalog,
    screening_payloads: dict[str, dict[str, Any]],
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    """One screening group request: deposition-only, records and candidates,
    never chunk text (fork F8)."""
    return create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context=render_record_blocks(screening_payloads),
        prompt_text=prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(response_format_for_v2(catalog)),
        batch_id="Eager" if eager else None,
    )


def create_dummy_completed_record_screening_batch_request(
    *,
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: BatchRequestIDType,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    note = (
        "No record screening needed - no record carried any candidate to judge."
    )
    base = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context=f"{note}\n{render_record_blocks({})}",
        prompt_text=note,
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_record_screening_batch_id",
    )
    base.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content=DUMMY_SCREENINGS_RESPONSE_CONTENT
        ),
    )
    return base
