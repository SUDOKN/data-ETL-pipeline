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

from dataclasses import dataclass, field as dataclass_field

import asyncio
import logging
import traceback
from datetime import datetime
from typing import Any, Optional

from pydantic import ValidationError

from core.models.extraction_schemas.catalog_wire_schema import (
    flatten_rule_slots,
    response_format_for,
    screening_response_model,
)
from core.models.extraction_schemas.screening import (
    CandidateScreeningVerdict,
    RecordScreeningResults,
)
from core.models.rule_catalog import STAGE_RELATIONSHIP_SCREENING, RuleCatalog
from core.services.applied_rule_validation import (
    check_applied_rules,
    passed_implied_by,
    raise_for_violations,
)
from core.models.extraction_schemas.synthesis import GroupRecords
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_record_ids,
    render_record_blocks,
    sent_record_ids_from_user_message,
    sent_records_from_user_message,
)
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    grouped_record_payloads,
)
from core.services.rule_catalog_registry import get_rule_catalog
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


def screening_catalog_for(field_name: str) -> RuleCatalog:
    """This stage's catalog for one field — the schema, the parse validation
    and the request's response_format all resolve through it."""
    return get_rule_catalog(STAGE_RELATIONSHIP_SCREENING, field_name)


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
        parsed = screening_response_model(catalog).model_validate_json(gpt_response)
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
) -> tuple[list[str], RecordScreeningResults]:
    """Parse one screening group request and hold it on both axes, as
    ``(sent record ids, held results)``. The id axis THINS on missing (3.3,
    the under-answer decision — user, 2026-08-24): unanswered records feed the
    node's retry assessment instead of failing the response. The candidate
    axis stays exact over the records that DID answer, and an id nobody sent
    still raises."""
    req_obj = completed_request_map.get(group_req_id)
    if not req_obj:
        raise ValueError(
            f"record_screening: Missing GPTBatchRequest for request ID "
            f"{group_req_id} in {subject_unique_id}:{field_name}"
        )
    elif not req_obj.response:
        raise ValueError(
            f"record_screening: GPTBatchRequest for request ID {group_req_id} "
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
            on_missing="drop",
        )
        hold_candidates_to_sent_records(
            user_message=user_message, held_results=held, where=where
        )
        return sent_record_ids_from_user_message(user_message) or [], held
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"record_screening: Error parsing screening results for subject "
            f"{subject_unique_id} from GPT response: {e}"
        )
        raise


@dataclass(frozen=True)
class ChunkScreeningAnswer:
    """ONE chunk's screening answer, merged across its group requests and
    (when included) the retry — the same shape the grounding stages use."""

    sent_ids: list[str]
    results: RecordScreeningResults
    retried_record_ids: list[str] = dataclass_field(default_factory=list)

    @property
    def missing_ids(self) -> list[str]:
        return [rid for rid in self.sent_ids if rid not in self.results]


async def get_chunk_record_screening_answer(
    *,
    subject_unique_id: str,
    field_name: str,
    chunk_bounds: str,
    catalog: RuleCatalog,
    group_req_ids: list[BatchRequestIDType],
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    retry_req_ids: Optional[list[BatchRequestIDType]] = None,
    retried_record_ids: Optional[list[str]] = None,
) -> ChunkScreeningAnswer:
    """Merge screening verdicts across every request embedded for the chunk —
    the groups, then the retry (``retry_req_ids=None``/empty reads the first
    pass alone, as the retry assessment does). A record answered in two
    requests raises."""
    if not group_req_ids:
        raise ValueError(
            f"record_screening: request id list is empty for chunk bounds "
            f"{chunk_bounds} in {subject_unique_id}:{field_name}"
        )

    sent_ids: list[str] = []
    sent_seen: set[str] = set()
    merged: RecordScreeningResults = {}
    for group_req_id in [*group_req_ids, *(retry_req_ids or [])]:
        req_sent_ids, held = await parse_record_screening_group_result(
            subject_unique_id=subject_unique_id,
            field_name=field_name,
            catalog=catalog,
            group_req_id=group_req_id,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
        )
        for rid in req_sent_ids:
            if rid not in sent_seen:
                sent_seen.add(rid)
                sent_ids.append(rid)
        for rid, verdicts in held.items():
            if rid in merged:
                raise ValueError(
                    f"record_screening: record id {rid!r} answered in two "
                    f"requests of chunk {chunk_bounds} in "
                    f"{subject_unique_id}:{field_name}"
                )
            merged[rid] = verdicts
    return ChunkScreeningAnswer(
        sent_ids=sent_ids,
        results=merged,
        retried_record_ids=list(retried_record_ids or []),
    )


async def get_record_screening_result(
    *,
    subject_unique_id: str,
    field_name: str,
    chunk_bounds: str,
    catalog: RuleCatalog,
    group_req_ids: list[BatchRequestIDType],
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    retry_req_ids: Optional[list[BatchRequestIDType]] = None,
) -> RecordScreeningResults:
    """The chunk's stored screening map: groups, then the retry. A record
    still unanswered after the retry is WARNED about and absent — its
    candidates get no verdict, which downstream reads as not-passed (fails
    closed) and the dump shows as ``screening_dropped``."""
    answer = await get_chunk_record_screening_answer(
        subject_unique_id=subject_unique_id,
        field_name=field_name,
        chunk_bounds=chunk_bounds,
        catalog=catalog,
        group_req_ids=group_req_ids,
        completed_request_map=completed_request_map,
        timestamp=timestamp,
        retry_req_ids=retry_req_ids,
    )
    if answer.missing_ids:
        logger.warning(
            f"record_screening: {len(answer.missing_ids)} record(s) of chunk "
            f"{chunk_bounds} in {subject_unique_id}:{field_name} still unanswered "
            f"after the retry pass: {answer.missing_ids}"
        )
    return answer.results


def build_screening_payloads(
    group_records: GroupRecords,
    candidates_by_record: dict[str, list[str]],
) -> dict[str, dict[str, Any]]:
    """The group_id → record payload a screening request renders (3.3, D16):
    the group's focal form and synthesis, plus the candidates to judge. Only
    records WITH candidates are sent — a record grounding yielded nothing for
    has nothing to screen — and a candidate list for a group the synthesis
    stage never produced is a pipeline bug."""
    payloads: dict[str, dict[str, Any]] = {}
    for group_id, candidates in candidates_by_record.items():
        record = group_records.get(group_id)
        if record is None:
            raise ValueError(
                f"build_screening_payloads: candidates supplied for unknown "
                f"group id {group_id!r}"
            )
        if not candidates:
            continue
        payload: dict[str, Any] = record.model_dump()
        payload["candidates"] = sorted(candidates)
        payloads[group_id] = payload
    return payloads


async def create_missing_record_screening_requests(
    *,
    subject_unique_id: str,
    field_name: str,
    chunk_payload_maps: dict[str, dict[str, dict[str, Any]]],
    group_req_ids_by_chunk: dict[str, list[BatchRequestIDType]],
    retry_req_ids_by_chunk: dict[str, list[BatchRequestIDType]],
    retry_record_ids_by_chunk: dict[str, list[str]],
    missing_req_ids: set[BatchRequestIDType],
    prompt: Prompt,
    catalog: RuleCatalog,
    subject_name: str,
    max_records_per_request: int,
    deferred_at: datetime,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
    BATCH_SIZE: int = 100,
) -> list[GPTBatchRequest]:
    """The screening create loop, mirroring the grounding one: the caller
    derives each chunk's full payload map (records + their candidate lists) and
    the split into groups happens here, through the same helper the
    id-embedding side used. The retry params carry the stage's under-answer
    retry (3.3 — the synthesis pattern)."""
    batch_requests: list[GPTBatchRequest] = []
    chunk_items = [
        (chunk_bounds, payloads)
        for chunk_bounds, payloads in chunk_payload_maps.items()
        if set(group_req_ids_by_chunk[chunk_bounds]) & missing_req_ids
        or set(retry_req_ids_by_chunk.get(chunk_bounds, [])) & missing_req_ids
    ]

    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        for chunk_bounds, payloads in batch:
            payload_groups = grouped_record_payloads(
                payloads, max_records_per_request
            )
            group_req_ids = group_req_ids_by_chunk[chunk_bounds]
            if len(group_req_ids) != len(payload_groups):
                raise ValueError(
                    f"record_screening: embedded group count "
                    f"({len(group_req_ids)}) does not match computed group count "
                    f"({len(payload_groups)}) for chunk bounds {chunk_bounds} in "
                    f"{subject_unique_id}:{field_name}. Group counts are computed "
                    f"once, upfront, from the same candidate sets, so this should "
                    f"not happen."
                )

            # The retry pass: the stored missing ids, packed like the first pass.
            retry_req_ids = retry_req_ids_by_chunk.get(chunk_bounds, [])
            if set(retry_req_ids) & missing_req_ids:
                retry_record_ids = retry_record_ids_by_chunk.get(chunk_bounds, [])
                unknown = [rid for rid in retry_record_ids if rid not in payloads]
                if unknown:
                    raise ValueError(
                        f"record_screening: stored retry record id(s) {unknown} of "
                        f"chunk {chunk_bounds} in {subject_unique_id}:{field_name} "
                        f"are not among the chunk's records; the upstream state "
                        f"changed under the stored request ids (re-defer)."
                    )
                retry_payloads = {rid: payloads[rid] for rid in retry_record_ids}
                retry_payload_groups = grouped_record_payloads(
                    retry_payloads, max_records_per_request
                )
                if not retry_payloads or len(retry_req_ids) != len(retry_payload_groups):
                    raise ValueError(
                        f"record_screening: embedded retry group count "
                        f"({len(retry_req_ids)}) does not match the computed count "
                        f"({len(retry_payload_groups)}, {len(retry_payloads)} "
                        f"record(s)) for chunk bounds {chunk_bounds} in "
                        f"{subject_unique_id}:{field_name}."
                    )
                for retry_group_index, retry_req_id in enumerate(retry_req_ids):
                    if retry_req_id not in missing_req_ids:
                        continue
                    logger.info(
                        f"record_screening: retrying "
                        f"{len(retry_payload_groups[retry_group_index])} unanswered "
                        f"record(s) for {subject_unique_id}:{field_name} chunk "
                        f"{chunk_bounds} retry group {retry_group_index}"
                    )
                    batch_requests.append(
                        create_deferred_record_screening_gpt_request(
                            deferred_at=deferred_at,
                            subject_unique_id=subject_unique_id,
                            request_id=retry_req_id,
                            prompt=prompt,
                            catalog=catalog,
                            subject_name=subject_name,
                            screening_payloads=retry_payload_groups[retry_group_index],
                            gpt_model=llm_model,
                            eager=eager,
                            model_params=model_params,
                        )
                    )

            for group_index, group_req_id in enumerate(group_req_ids):
                if group_req_id not in missing_req_ids:
                    continue

                payload_group = payload_groups[group_index]
                if not payload_group:
                    if len(payload_groups) != 1:
                        raise ValueError(
                            f"record_screening: unexpected empty record group at "
                            f"index {group_index} of {len(payload_groups)} groups "
                            f"for chunk bounds {chunk_bounds} in "
                            f"{subject_unique_id}:{field_name}. Only a single "
                            f"group should ever be empty (the zero-candidates "
                            f"case)."
                        )
                    logger.info(
                        f"record_screening: no record carried any candidate for "
                        f"{subject_unique_id}:{field_name} chunk {chunk_bounds}, "
                        f"creating dummy request"
                    )
                    batch_requests.append(
                        create_dummy_completed_record_screening_batch_request(
                            deferred_at=deferred_at,
                            subject_unique_id=subject_unique_id,
                            request_id=group_req_id,
                            model_params=model_params,
                            eager=eager,
                        )
                    )
                    continue

                batch_requests.append(
                    create_deferred_record_screening_gpt_request(
                        deferred_at=deferred_at,
                        subject_unique_id=subject_unique_id,
                        request_id=group_req_id,
                        prompt=prompt,
                        catalog=catalog,
                        subject_name=subject_name,
                        screening_payloads=payload_group,
                        gpt_model=llm_model,
                        eager=eager,
                        model_params=model_params,
                    )
                )

        await asyncio.sleep(0)

    return batch_requests


def create_deferred_record_screening_gpt_request(
    *,
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: str,
    prompt: Prompt,
    catalog: RuleCatalog,
    subject_name: str,
    screening_payloads: dict[str, dict[str, Any]],
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    """One screening group request: deposition-only, records and candidates,
    never chunk text (fork F8). The manufacturer's NAME rides at the top
    (3.3 rider, user decision: v3 syntheses name the subject — quoted headings
    inject it — so the judge must know whose record it is reading; the v2-era
    "never by name" sentence left the statics with the same edit)."""
    return create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context=(
            f"the name of the manufacturer in question: {subject_name}\n\n"
            f"{render_record_blocks(screening_payloads)}"
        ),
        prompt_text=prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(response_format_for(catalog)),
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
