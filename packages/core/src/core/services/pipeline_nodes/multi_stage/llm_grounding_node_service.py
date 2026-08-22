"""Pipeline v2's grounding stages: in-vocab, OOV discovery, and keyword/freehand.

ONE service because the three stages differ only in what rides beside the
records (an options outline, the already-identified results, or nothing) and in
whether emitted labels are held to a vocabulary. Everything else — the record
blocks request shape, the record-keyed wire response, the flatten/validate
walk, the exact id-axis hold, the structural declination — is shared, and
sharing it here is what keeps the three stages' contracts from drifting apart.

Membership (fork F4): the in-vocab pass has no escape hatch, so an emitted
option that is not a vocabulary label is corruption by definition and fails the
response — the fake-OOV hole closed at the decoder's altitude. Casing drift
alone is repaired to the vocabulary's own spelling (the prompt says copy
verbatim; a re-cased copy is echo drift, not a different claim) and warned.
OOV and freehand pass no vocabulary: their candidates are minted by design,
and re-routing a minted label that happens to match the vocabulary is
reconcile's classification job, not a parse defect.
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import datetime
from typing import Any, Iterable, Optional

from pydantic import BaseModel, ValidationError

from core.models.extraction_schemas.catalog_wire_schema import (
    flatten_rule_slots,
    grounding_response_model,
    response_format_for,
)
from core.models.extraction_schemas.grounding import (
    RecordGroundingEntry,
    RecordGroundingResults,
    TagToAppliedRulesMap,
)
from core.models.extraction_schemas.relationship import (
    MaskedLLMPhraseRelationshipResults,
)
from core.models.rule_catalog import RuleCatalog
from core.services.applied_rule_validation import (
    check_applied_rules,
    raise_for_violations,
)
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_record_ids,
    render_record_blocks,
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

# What a dummy (no records to ground) grounding request answers with.
DUMMY_GROUNDINGS_RESPONSE_CONTENT = '{"groundings": []}'


def _unit_label(unit: BaseModel) -> str:
    """The emitted label, whichever unit key this stage's wire uses."""
    for key in ("option", "candidate"):
        label = getattr(unit, key, None)
        if label is not None:
            return label
    raise ValueError(f"grounding unit carries neither 'option' nor 'candidate': {unit!r}")


def parse_record_grounding_result(
    gpt_response: Optional[str],
    *,
    catalog: RuleCatalog,
    allowed_labels: Optional[Iterable[str]] = None,
) -> RecordGroundingResults:
    """One response's groundings as the stored record-keyed map.

    ``allowed_labels`` is the in-vocab pass's vocabulary (every matchLabel);
    None means labels are minted (OOV, freehand) and pass through as written.
    Violations are collected across the whole response and raised once, so a
    response is measured whole rather than up to its first defect.
    """
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_record_grounding_result: Empty or invalid response from GPT"
        )

    try:
        parsed = grounding_response_model(catalog).model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_record_grounding_result: Invalid response from GPT:{gpt_response}"
        ) from e

    canonical_by_folded = (
        {label.casefold(): label for label in allowed_labels}
        if allowed_labels is not None
        else None
    )

    staged: dict[str, tuple[TagToAppliedRulesMap, Optional[str]]] = {}
    violations: list[str] = []
    for entry in parsed.groundings:
        if entry.record_id in staged:
            raise ValueError(
                f"parse_record_grounding_result: Duplicate record id "
                f"{entry.record_id!r} in groundings response"
            )

        units = getattr(entry, "options", None)
        if units is None:
            units = getattr(entry, "candidates")

        tags: TagToAppliedRulesMap = {}
        for unit in units:
            label = _unit_label(unit)
            if canonical_by_folded is not None:
                canonical = canonical_by_folded.get(label.casefold())
                if canonical is None:
                    # The in-vocab contract: vocabulary or nothing. Anything
                    # else is a drifted or invented label — the exact string
                    # that used to be persisted as a fake ontology gap.
                    violations.append(
                        f"record {entry.record_id}: option {label!r} is not a "
                        f"vocabulary label; this pass chooses from the "
                        f"vocabulary or returns nothing"
                    )
                    continue
                if canonical != label:
                    logger.warning(
                        f"repaired option casing {label!r} -> {canonical!r} "
                        f"for record {entry.record_id}"
                    )
                label = canonical
            if label in tags:
                raise ValueError(
                    f"parse_record_grounding_result: record {entry.record_id!r} "
                    f"carries {label!r} twice"
                )
            applied = flatten_rule_slots(catalog, unit)
            report = check_applied_rules(
                catalog=catalog,
                applied_rules=applied,
                where=f"record {entry.record_id} unit {label!r}",
            )
            violations.extend(report.problems)
            tags[label] = applied

        explanation = entry.explanation
        if units:
            if explanation is not None:
                # Volunteered beside real units: harmless, but there is no
                # stored slot for it — the rules on each tag are the reasoning.
                logger.warning(
                    f"record {entry.record_id}: dropping explanation volunteered "
                    f"beside {len(units)} unit(s)"
                )
            explanation = None
        elif not (explanation and explanation.strip()):
            violations.append(
                f"record {entry.record_id}: yielded nothing and carries no "
                f"declination explanation — an empty record must say why"
            )
            continue

        staged[entry.record_id] = (tags, explanation)

    raise_for_violations(violations)

    return {
        record_id: RecordGroundingEntry(tags=tags, explanation=explanation)
        for record_id, (tags, explanation) in staged.items()
    }


async def parse_record_grounding_group_result(
    *,
    stage_label: str,
    subject_unique_id: str,
    field_name: str,
    catalog: RuleCatalog,
    group_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    allowed_labels: Optional[Iterable[str]] = None,
) -> RecordGroundingResults:
    """Parse one grounding group request and hold it to its sent record ids."""
    req_obj = completed_request_map.get(group_req_id)
    if not req_obj:
        raise ValueError(
            f"{stage_label}: Missing GPTBatchRequest for request ID "
            f"{group_req_id} in {subject_unique_id}:{field_name}"
        )
    elif not req_obj.response:
        raise ValueError(
            f"{stage_label}: GPTBatchRequest for request ID {group_req_id} has "
            f"no response_blob in {subject_unique_id}:{field_name}"
        )

    try:
        parsed = parse_record_grounding_result(
            req_obj.response.result,
            catalog=catalog,
            allowed_labels=allowed_labels,
        )
        return hold_response_to_sent_record_ids(
            user_message=req_obj.request.body.user_message(),
            response_by_record_id=parsed,
            where=f"{subject_unique_id}:{field_name} {stage_label} {group_req_id}",
            on_missing="raise",
        )
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"{stage_label}: Error parsing grounding results for subject "
            f"{subject_unique_id} from GPT response: {e}"
        )
        raise


async def get_record_grounding_result(
    *,
    stage_label: str,
    subject_unique_id: str,
    field_name: str,
    chunk_bounds: str,
    catalog: RuleCatalog,
    group_req_ids: list[BatchRequestIDType],
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    allowed_labels: Optional[Iterable[str]] = None,
) -> RecordGroundingResults:
    """Merge grounding entries across every group embedded for the chunk."""
    if not group_req_ids:
        raise ValueError(
            f"{stage_label}: request id list is empty for chunk bounds "
            f"{chunk_bounds} in {subject_unique_id}:{field_name}"
        )

    merged: RecordGroundingResults = {}
    for group_req_id in group_req_ids:
        merged.update(
            await parse_record_grounding_group_result(
                stage_label=stage_label,
                subject_unique_id=subject_unique_id,
                field_name=field_name,
                catalog=catalog,
                group_req_id=group_req_id,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
                allowed_labels=allowed_labels,
            )
        )
    return merged


def split_record_ids_into_groups(
    record_ids: list[str], group_size: int
) -> list[list[str]]:
    """Split an ORDERED record-id list into ordered groups of at most
    *group_size* ids. Always returns at least one (possibly empty) group so the
    single-dummy-request-per-chunk fallback keeps working when a chunk has no
    records at all. Callers sort before splitting: group membership is part of
    request identity, so it must not depend on dict iteration order."""
    if not record_ids:
        return [[]]
    return [
        record_ids[i : i + group_size]
        for i in range(0, len(record_ids), group_size)
    ]


def grouped_record_payloads(
    payloads: dict[str, dict[str, Any]], group_size: int
) -> list[dict[str, dict[str, Any]]]:
    """The chunk's payload map as ordered per-request groups (ids sorted).

    Both the id-embedding side (group count + the ``|ud=`` digest of each
    group's own payload) and the request-creation side derive their groups
    through here, so the two can never disagree about what a group contains.
    """
    groups = split_record_ids_into_groups(sorted(payloads), group_size)
    return [
        {record_id: payloads[record_id] for record_id in group} for group in groups
    ]


def build_record_payloads(
    masked: MaskedLLMPhraseRelationshipResults,
    *,
    already_identified: Optional[dict[str, list[str]]] = None,
) -> dict[str, dict[str, Any]]:
    """The id → record payload a grounding request renders into its blocks.

    The phrase itself is NOT in the payload — that is the mask (key-masking:
    the mention forms inside carry the wording, which grounding needs). The OOV
    pass adds each record's already-identified results, sorted for render
    stability.
    """
    payloads: dict[str, dict[str, Any]] = {}
    for record_id, entry in masked.items():
        payload: dict[str, Any] = entry.record.model_dump()
        if already_identified is not None:
            payload["already_identified"] = sorted(
                already_identified.get(record_id, [])
            )
        payloads[record_id] = payload
    return payloads


def create_deferred_record_grounding_gpt_request(
    *,
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: str,
    prompt: Prompt,
    catalog: RuleCatalog,
    record_payloads: dict[str, dict[str, Any]],
    options_section: Optional[str],
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    """One grounding group request. ``options_section`` is the pre-rendered
    outline block (heading included), or None for the freehand stage, which is
    sent no options at all."""
    context = render_record_blocks(record_payloads)
    if options_section is not None:
        context = f"{context}\n\n{options_section}"

    return create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context=context,
        prompt_text=prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(response_format_for(catalog)),
        batch_id="Eager" if eager else None,
    )


async def create_missing_record_grounding_requests(
    *,
    stage_label: str,
    subject_unique_id: str,
    field_name: str,
    chunk_payload_maps: dict[str, dict[str, dict[str, Any]]],
    group_req_ids_by_chunk: dict[str, list[BatchRequestIDType]],
    missing_req_ids: set[BatchRequestIDType],
    prompt: Prompt,
    catalog: RuleCatalog,
    options_section_by_chunk: dict[str, Optional[str]],
    max_records_per_request: int,
    deferred_at: datetime,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
    dummy_note: str,
    BATCH_SIZE: int = 100,
) -> list[GPTBatchRequest]:
    """The shared create loop for the record-grounding stages (in-vocab, OOV,
    freehand). ``chunk_payload_maps`` holds each chunk's FULL id → payload map
    (the caller derives it from upstream stored shapes); the split into groups
    happens here, through the same helper the id-embedding side used, so the
    embedded group count and digests always describe the groups actually sent.

    ``options_section_by_chunk`` is the pre-rendered outline block per chunk
    (None = freehand, which is sent no options). Per chunk rather than one
    value, because the OOV pass may one day scope its outline; today every
    chunk shares one rendered section.
    """
    batch_requests: list[GPTBatchRequest] = []
    chunk_items = [
        (chunk_bounds, payloads)
        for chunk_bounds, payloads in chunk_payload_maps.items()
        if set(group_req_ids_by_chunk[chunk_bounds]) & missing_req_ids
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
                    f"{stage_label}: embedded group count ({len(group_req_ids)}) "
                    f"does not match computed group count ({len(payload_groups)}) "
                    f"for chunk bounds {chunk_bounds} in "
                    f"{subject_unique_id}:{field_name}. Group counts are computed "
                    f"once, upfront, from the same upstream records, so this "
                    f"should not happen."
                )

            for group_index, group_req_id in enumerate(group_req_ids):
                if group_req_id not in missing_req_ids:
                    continue

                payload_group = payload_groups[group_index]
                if not payload_group:
                    if len(payload_groups) != 1:
                        raise ValueError(
                            f"{stage_label}: unexpected empty record group at "
                            f"index {group_index} of {len(payload_groups)} groups "
                            f"for chunk bounds {chunk_bounds} in "
                            f"{subject_unique_id}:{field_name}. Only a single "
                            f"group should ever be empty (the zero-records case)."
                        )
                    logger.info(
                        f"{stage_label}: no records to ground for "
                        f"{subject_unique_id}:{field_name} chunk {chunk_bounds}, "
                        f"creating dummy request"
                    )
                    batch_requests.append(
                        create_dummy_completed_record_grounding_batch_request(
                            deferred_at=deferred_at,
                            subject_unique_id=subject_unique_id,
                            request_id=group_req_id,
                            model_params=model_params,
                            eager=eager,
                            note=dummy_note,
                        )
                    )
                    continue

                batch_requests.append(
                    create_deferred_record_grounding_gpt_request(
                        deferred_at=deferred_at,
                        subject_unique_id=subject_unique_id,
                        request_id=group_req_id,
                        prompt=prompt,
                        catalog=catalog,
                        record_payloads=payload_group,
                        options_section=options_section_by_chunk[chunk_bounds],
                        gpt_model=llm_model,
                        eager=eager,
                        model_params=model_params,
                    )
                )

        await asyncio.sleep(0)

    return batch_requests


def create_dummy_completed_record_grounding_batch_request(
    *,
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: BatchRequestIDType,
    model_params: GPTModelParams,
    eager: bool,
    note: str,
) -> GPTBatchRequest:
    """A pre-answered request for a chunk with no records to ground. Still
    carries the record blocks, empty — an absent block has to stay an error."""
    base = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context=f"{note}\n{render_record_blocks({})}",
        prompt_text=note,
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_record_grounding_batch_id",
    )
    base.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content=DUMMY_GROUNDINGS_RESPONSE_CONTENT
        ),
    )
    return base
