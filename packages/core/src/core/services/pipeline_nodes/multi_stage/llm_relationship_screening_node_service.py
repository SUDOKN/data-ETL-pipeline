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
import json
import logging
import re
import traceback
from collections import Counter
from datetime import datetime
from typing import Any, Callable, Mapping, Optional, Sequence

from pydantic import ValidationError

from core.models.extraction_schemas.catalog_wire_schema import (
    flatten_rule_slots,
    response_format_for,
    screening_response_model,
    unit_screening_response_model,
)
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.utils.quote_check import quote_found_in_text
from core.models.extraction_schemas.screening import (
    CandidateScreeningVerdict,
    RecordScreeningResults,
)
from core.models.rule_catalog import STAGE_UNIT_SCREENING, RuleCatalog
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
    subject_keyed_payloads,
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


# --- Step 2: unit-major structural screening (2026-09-21) --------------------

UNITS_OPEN = "<<<UNITS"
UNITS_CLOSE = "UNITS>>>"


def render_units_block(units: Sequence[Mapping[str, Any]]) -> str:
    """The UNITS block of a unit-screening request: one JSON object per line
    (``option``, optional ``meaning``, ``records``) inside fences, so the
    parser can read back exactly what was asked."""
    lines = ",\n".join(json.dumps(dict(unit), ensure_ascii=False) for unit in units)
    return f"{UNITS_OPEN}\n[\n{lines}\n]\n{UNITS_CLOSE}"


def parse_unit_screening_result(
    gpt_response: Optional[str],
    *,
    catalog: RuleCatalog,
    units: Mapping[str, Sequence[str]],
    sent_records: Optional[Mapping[str, Mapping[str, Any]]],
) -> RecordScreeningResults:
    """One unit-major response as the stored record → candidate → verdict map.

    Holds (V7, design draft §5.2): every sent unit answered exactly once and
    no unit invented; per unit ``accepted`` ∪ ``not_accepted`` is exactly the
    unit's records, none in both. Any breach of THOSE fails the response (the
    node's parse-error path re-dispatches under its cap) — the candidate axis
    is exact, as today's. An accepted record's quote is checked against its
    record but never fails the response (2026-09-22, the first Step 2 run
    stopped both subjects on paraphrased quotes): a quote not found verbatim
    keeps the verdict with ``quote_verified=False``, the rule grounding
    already applies to its own quotes; an acceptance with no quote at all is
    kept and logged. ``passed`` is set from the list a record landed in; the
    evidence distance and the failed rule ride on the verdict as metadata;
    ``applied_rules`` carries the failed rule for a rejection and is empty
    for an acceptance.
    """
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError("parse_unit_screening_result: Empty or invalid response from GPT")
    try:
        parsed = unit_screening_response_model(catalog).model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(f"parse_unit_screening_result: Invalid response from GPT:{gpt_response}") from e

    expected = {option.casefold(): (option, set(ids)) for option, ids in units.items()}
    seen: Counter[str] = Counter()
    problems: list[str] = []
    results: RecordScreeningResults = {}
    for unit in parsed.screenings:
        key = unit.option.strip().casefold()
        seen[key] += 1
        if key not in expected:
            problems.append(f"unit never sent: {unit.option!r}")
            continue
        option, sent_ids = expected[key]
        accepted = {r.record_id for r in unit.accepted}
        rejected = {r.record_id for r in unit.not_accepted}
        if accepted & rejected:
            problems.append(f"unit {option!r}: records both accepted and not accepted: {sorted(accepted & rejected)}")
        answered = accepted | rejected
        if answered != sent_ids:
            problems.append(
                f"unit {option!r}: answered records {sorted(answered)} are not the sent "
                f"records {sorted(sent_ids)}"
            )
        if problems:
            continue
        for r in unit.accepted:
            verified: Optional[bool] = None
            if not r.quote.strip():
                logger.warning(f"unit screening: unit {option!r}: record {r.record_id} accepted with no quote; kept, unverified")
            else:
                record = sent_records.get(r.record_id) if sent_records is not None else None
                if record is not None:
                    phrase = record.get("subject", record.get("focal_form", "")) or ""
                    verified = quote_found_in_text(r.quote, str(phrase), str(record.get("synthesis", "") or ""))
                    if not verified:
                        logger.info(f"unit screening: unit {option!r}: record {r.record_id} accepted on a quote not found verbatim in the record; kept, marked unverified")
            results.setdefault(r.record_id, {})[option] = CandidateScreeningVerdict(
                passed=True, applied_rules=[], evidence=r.evidence, quote=r.quote, quote_verified=verified
            )
        for r in unit.not_accepted:
            results.setdefault(r.record_id, {})[option] = CandidateScreeningVerdict(
                passed=False,
                applied_rules=[AppliedRule(rule_id=r.failed_rule, outcome="failed", explanation=r.quote)],
                failed_rule=r.failed_rule,
                quote=r.quote,
            )
    for key, n in seen.items():
        if n > 1:
            problems.append(f"unit {expected[key][0] if key in expected else key!r} answered {n} times")
    for key, (option, _) in expected.items():
        if seen[key] == 0:
            problems.append(f"unit {option!r} never answered")
    if problems:
        raise ValueError("parse_unit_screening_result: " + "; ".join(problems))
    return results


# --- Step 2 unit screening (2026-09-21): the request / parse layer ------------
#
# One request = the manufacturer line, the records ONCE (subject-keyed), then a
# UNITS block: one candidate label per unit, its meaning when the vocabulary
# gives one, and the ids of the records grounding matched on. Requests are
# packed per chunk and per WAVE by ``stage_derivations.pack_units`` (≤ the
# record cap, sub-units of one label never sharing a request); a keyword field
# has one wave, a concept field one per vocabulary depth (the descent loop).
# No under-answer retry: the parser holds each response to the request's own
# units and records exactly (V7), and a breach goes to the parse-error
# re-dispatch under its cap.

UNIT_SCREENING_LABEL = "unit screening"
MANUFACTURER_LINE = "the name of the manufacturer in question: "
DUMMY_UNIT_SCREENINGS_RESPONSE_CONTENT = '{"screenings": []}'
_UNITS_RE = re.compile(re.escape(UNITS_OPEN) + r"\n(.*?)\n" + re.escape(UNITS_CLOSE), re.S)

# One request's payload — what the ``|ud=`` digest is over and what the
# request renders: the records it carries (subject-keyed) and its units.
UnitRequestPayload = dict[str, Any]


def unit_screening_catalog_for(field_name: str) -> RuleCatalog:
    return get_rule_catalog(STAGE_UNIT_SCREENING, field_name)


def build_unit_request_payload(
    group_records: GroupRecords,
    units: Sequence[tuple[str, Sequence[str]]],
    *,
    meaning_of: Callable[[str], Optional[str]],
) -> UnitRequestPayload:
    """``{"records": {id: {subject, synthesis}}, "units": [{option, meaning?,
    records}]}`` for one packed request group. The records are the union of
    the units' ids, each rendered once; a unit's ``meaning`` is the
    vocabulary's definition when ``meaning_of`` gives one (a proposal has
    none and carries no key). An id no group record carries is a pipeline
    bug: the units were built from those records."""
    record_ids: set[str] = set()
    rendered_units: list[dict[str, Any]] = []
    for option, ids in units:
        ids = sorted(ids)
        unit: dict[str, Any] = {"option": option}
        meaning = meaning_of(option)
        if meaning:
            unit["meaning"] = meaning
        unit["records"] = ids
        rendered_units.append(unit)
        record_ids.update(ids)
    unknown = sorted(rid for rid in record_ids if rid not in group_records)
    if unknown:
        raise ValueError(f"build_unit_request_payload: unit record id(s) {unknown} are not among the chunk's records")
    records = subject_keyed_payloads({rid: group_records[rid].model_dump() for rid in sorted(record_ids)})
    return {"records": records, "units": rendered_units}


def render_unit_screening_context(subject_name: str, payload: UnitRequestPayload) -> str:
    return (
        f"{MANUFACTURER_LINE}{subject_name}\n\n"
        f"{render_record_blocks(payload['records'])}\n\n"
        f"{render_units_block(payload['units'])}"
    )


def sent_units_from_user_message(user_message: str) -> Optional[list[dict[str, Any]]]:
    """The UNITS block a request carried, read back — what the parser holds
    the response to."""
    m = _UNITS_RE.search(user_message)
    return None if m is None else json.loads(m.group(1))


def create_deferred_unit_screening_gpt_request(
    *,
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: str,
    prompt: Prompt,
    catalog: RuleCatalog,
    subject_name: str,
    payload: UnitRequestPayload,
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    return create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context=render_unit_screening_context(subject_name, payload),
        prompt_text=prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(response_format_for(catalog)),
        batch_id="Eager" if eager else None,
    )


def create_dummy_completed_unit_screening_batch_request(
    *,
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: BatchRequestIDType,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    note = "No unit screening needed - no unit to judge in this wave."
    base = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context=f"{note}\n{render_record_blocks({})}\n\n{render_units_block([])}",
        prompt_text=note,
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_unit_screening_batch_id",
    )
    base.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content=DUMMY_UNIT_SCREENINGS_RESPONSE_CONTENT
        ),
    )
    return base


async def create_missing_unit_screening_requests(
    *,
    subject_unique_id: str,
    field_name: str,
    payloads_by_req_id: Mapping[BatchRequestIDType, UnitRequestPayload],
    missing_req_ids: set[BatchRequestIDType],
    prompt: Prompt,
    catalog: RuleCatalog,
    subject_name: str,
    deferred_at: datetime,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
) -> list[GPTBatchRequest]:
    """The unit-screening create loop: the caller derives each request's
    payload (the same packing the id-embedding side used, so the ids and
    the payloads always describe the same groups) and this creates the
    missing ones; an empty payload (the zero-units wave of a chunk) gets the
    pre-answered dummy."""
    batch_requests: list[GPTBatchRequest] = []
    for req_id in sorted(missing_req_ids):
        payload = payloads_by_req_id.get(req_id)
        if payload is None:
            raise ValueError(
                f"{UNIT_SCREENING_LABEL}: no payload for missing request id {req_id} "
                f"in {subject_unique_id}:{field_name}; the embedded ids and the derived "
                f"payloads disagree (re-defer)"
            )
        if not payload["units"]:
            batch_requests.append(
                create_dummy_completed_unit_screening_batch_request(
                    deferred_at=deferred_at, subject_unique_id=subject_unique_id,
                    request_id=req_id, model_params=model_params, eager=eager,
                )
            )
            continue
        batch_requests.append(
            create_deferred_unit_screening_gpt_request(
                deferred_at=deferred_at, subject_unique_id=subject_unique_id, request_id=req_id,
                prompt=prompt, catalog=catalog, subject_name=subject_name, payload=payload,
                gpt_model=llm_model, eager=eager, model_params=model_params,
            )
        )
        await asyncio.sleep(0)
    return batch_requests


async def parse_unit_screening_group_result(
    *,
    subject_unique_id: str,
    field_name: str,
    catalog: RuleCatalog,
    req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> RecordScreeningResults:
    """Parse one unit-screening request, held to the units and records it
    carried (read back off its own user message)."""
    req_obj = completed_request_map.get(req_id)
    if not req_obj:
        raise ValueError(f"{UNIT_SCREENING_LABEL}: Missing GPTBatchRequest for request ID {req_id} in {subject_unique_id}:{field_name}")
    elif not req_obj.response:
        raise ValueError(f"{UNIT_SCREENING_LABEL}: GPTBatchRequest for request ID {req_id} has no response_blob in {subject_unique_id}:{field_name}")
    try:
        user_message = req_obj.request.body.user_message()
        sent_units = sent_units_from_user_message(user_message)
        if sent_units is None:
            raise ValueError(f"{UNIT_SCREENING_LABEL}: request {req_id} carries no UNITS block")
        units = {u["option"]: list(u["records"]) for u in sent_units}
        if not units:
            return {}
        sent = sent_records_from_user_message(user_message)
        return parse_unit_screening_result(
            req_obj.response.result,
            catalog=catalog,
            units=units,
            sent_records=sent if isinstance(sent, dict) else None,
        )
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=req_obj, error_message=str(e), timestamp=timestamp, traceback_str=traceback.format_exc(),
        )
        logger.error(f"{UNIT_SCREENING_LABEL}: Error parsing results for subject {subject_unique_id} from GPT response: {e}")
        raise


async def get_unit_screening_result(
    *,
    subject_unique_id: str,
    field_name: str,
    catalog: RuleCatalog,
    req_ids: Sequence[BatchRequestIDType],
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> RecordScreeningResults:
    """One wave's verdicts, merged across its requests: record → candidate →
    verdict. Sub-units of one label sit in different requests over disjoint
    records, so their verdicts union per record; the same (record, candidate)
    answered twice is a pipeline bug and raises."""
    merged: RecordScreeningResults = {}
    for req_id in req_ids:
        held = await parse_unit_screening_group_result(
            subject_unique_id=subject_unique_id, field_name=field_name, catalog=catalog,
            req_id=req_id, completed_request_map=completed_request_map, timestamp=timestamp,
        )
        for rid, verdicts in held.items():
            slot = merged.setdefault(rid, {})
            for candidate, verdict in verdicts.items():
                if candidate in slot:
                    raise ValueError(
                        f"{UNIT_SCREENING_LABEL}: record {rid!r} judged twice for {candidate!r} "
                        f"in {subject_unique_id}:{field_name}"
                    )
                slot[candidate] = verdict
    return merged
