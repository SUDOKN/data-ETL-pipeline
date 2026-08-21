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

import logging
import traceback
from datetime import datetime
from typing import Any, Iterable, Optional

from pydantic import BaseModel, ValidationError

from core.models.extraction_schemas.catalog_wire_schema import (
    flatten_rule_slots,
    grounding_response_model_v2,
    response_format_for_v2,
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
        parsed = grounding_response_model_v2(catalog).model_validate_json(gpt_response)
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
        model_params=model_params.with_response_format(response_format_for_v2(catalog)),
        batch_id="Eager" if eager else None,
    )


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
