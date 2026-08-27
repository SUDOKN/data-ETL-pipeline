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

from dataclasses import dataclass, field as dataclass_field

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
from core.models.extraction_schemas.synthesis import GroupRecords
from core.models.rule_catalog import RuleCatalog
from core.services.applied_rule_validation import (
    check_applied_rules,
    raise_for_violations,
)
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_record_ids,
    render_record_blocks,
    sent_record_ids_from_user_message,
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


def _quoted(labels: list[str]) -> str:
    """``'a', 'b'`` — the labels as they were written, for a log or a reason."""
    return ", ".join(repr(label) for label in labels)


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
    A label outside that vocabulary is DROPPED onto the entry's
    ``dropped_options`` (2026-08-25), not raised — see the drop site below.
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

    staged: dict[str, tuple[TagToAppliedRulesMap, Optional[str], list[str]]] = {}
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
        dropped: list[str] = []
        for unit in units:
            label = _unit_label(unit)
            if canonical_by_folded is not None:
                canonical = canonical_by_folded.get(label.casefold())
                if canonical is None:
                    # The in-vocab contract: vocabulary or nothing. Anything
                    # else is a drifted or invented label — the exact string
                    # that used to be persisted as a fake ontology gap.
                    #
                    # DROPPED, not raised (2026-08-25). Raising here failed the
                    # whole group request, and through the recursive loop the
                    # whole subject: run 20260824T190359 lost nine completed
                    # fields of steelcraft.com to one record answering
                    # 'Testing', and alecmfg.com to four answering RoHS/REACH.
                    # Neither is a vocabulary gap this pass can record — the
                    # OOV pass runs on every record and sees this one with the
                    # label absent from ``already_identified``, so the label
                    # is recorded there if it is real. The detection survives
                    # as ``dropped_options`` on the stored entry; what changes
                    # is only what the detection costs.
                    dropped.append(label)
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
        # Keyed on the tags KEPT, never on the units emitted. A record whose
        # only option was dropped emitted a unit but holds no tag, and the
        # stored entry's validator requires a declination explanation exactly
        # then — reading `units` here would null the explanation and fail
        # validation on the very records the drop above exists to rescue.
        if tags:
            if explanation is not None:
                # Volunteered beside real tags: harmless, but there is no
                # stored slot for it — the rules on each tag are the reasoning.
                logger.warning(
                    f"record {entry.record_id}: dropping explanation volunteered "
                    f"beside {len(tags)} tag(s)"
                )
            explanation = None
        elif not (explanation and explanation.strip()):
            if dropped:
                # Every option went out of vocabulary and the model offered no
                # declination of its own. The record still yields nothing, and
                # saying why is the invariant, so the drop itself is the reason.
                explanation = (
                    "No vocabulary option was chosen: this pass discarded "
                    f"{_quoted(dropped)}, which {'are' if len(dropped) > 1 else 'is'} "
                    "not in the vocabulary it must choose from."
                )
            else:
                violations.append(
                    f"record {entry.record_id}: yielded nothing and carries no "
                    f"declination explanation — an empty record must say why"
                )
                continue

        if dropped:
            logger.warning(
                f"record {entry.record_id}: dropped {len(dropped)} "
                f"non-vocabulary option(s) {_quoted(dropped)}; "
                f"{len(tags)} tag(s) kept"
            )

        staged[entry.record_id] = (tags, explanation, dropped)

    raise_for_violations(violations)

    return {
        record_id: RecordGroundingEntry(
            tags=tags, explanation=explanation, dropped_options=dropped
        )
        for record_id, (tags, explanation, dropped) in staged.items()
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
) -> tuple[list[str], RecordGroundingResults]:
    """Parse one grounding group request and hold it to its sent record ids,
    as ``(sent record ids, held results)``.

    The hold THINS on missing ids (3.3, the under-answer decision — user,
    2026-08-24): a response that validly answers a fraction of its records no
    longer fails; the ids left unanswered feed the node's retry assessment,
    exactly as the synthesis stage's do. An id nobody sent still raises —
    that is a fabricated verdict, not an under-answer."""
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
        user_message = req_obj.request.body.user_message()
        parsed = parse_record_grounding_result(
            req_obj.response.result,
            catalog=catalog,
            allowed_labels=allowed_labels,
        )
        held = hold_response_to_sent_record_ids(
            user_message=user_message,
            response_by_record_id=parsed,
            where=f"{subject_unique_id}:{field_name} {stage_label} {group_req_id}",
            on_missing="drop",
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
            f"{stage_label}: Error parsing grounding results for subject "
            f"{subject_unique_id} from GPT response: {e}"
        )
        raise


@dataclass(frozen=True)
class ChunkGroundingAnswer:
    """ONE chunk's grounding answer, merged across its group requests and
    (when included) the retry: the ids the requests sent, the held entries,
    and what is STILL missing — the retry assessment reads this with the
    retry excluded, the result with it included."""

    sent_ids: list[str]
    results: RecordGroundingResults
    retried_record_ids: list[str] = dataclass_field(default_factory=list)

    @property
    def missing_ids(self) -> list[str]:
        return [rid for rid in self.sent_ids if rid not in self.results]


async def get_chunk_record_grounding_answer(
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
    retry_req_ids: Optional[list[BatchRequestIDType]] = None,
    retried_record_ids: Optional[list[str]] = None,
) -> ChunkGroundingAnswer:
    """Merge grounding entries across every request embedded for the chunk —
    the group requests, then the retry's (pass ``retry_req_ids=None`` or empty
    to read the first pass alone, as the retry assessment does). A record
    answered in two requests raises: requests partition the chunk's records,
    so a duplicate is a pipeline bug, not model noise."""
    if not group_req_ids:
        raise ValueError(
            f"{stage_label}: request id list is empty for chunk bounds "
            f"{chunk_bounds} in {subject_unique_id}:{field_name}"
        )

    sent_ids: list[str] = []
    sent_seen: set[str] = set()
    merged: RecordGroundingResults = {}
    for group_req_id in [*group_req_ids, *(retry_req_ids or [])]:
        req_sent_ids, held = await parse_record_grounding_group_result(
            stage_label=stage_label,
            subject_unique_id=subject_unique_id,
            field_name=field_name,
            catalog=catalog,
            group_req_id=group_req_id,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
            allowed_labels=allowed_labels,
        )
        for rid in req_sent_ids:
            # A retry re-sends first-pass ids; sent_ids stays the union.
            if rid not in sent_seen:
                sent_seen.add(rid)
                sent_ids.append(rid)
        for rid, entry in held.items():
            if rid in merged:
                raise ValueError(
                    f"{stage_label}: record id {rid!r} answered in two requests "
                    f"of chunk {chunk_bounds} in {subject_unique_id}:{field_name}"
                )
            merged[rid] = entry
    return ChunkGroundingAnswer(
        sent_ids=sent_ids,
        results=merged,
        retried_record_ids=list(retried_record_ids or []),
    )


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
    retry_req_ids: Optional[list[BatchRequestIDType]] = None,
) -> RecordGroundingResults:
    """The chunk's stored grounding map: groups, then the retry. A record
    still unanswered after the retry is WARNED about and absent — downstream
    derivations treat an absent record as "nothing found here", and the dump's
    row for it shows the pass returning nothing (the under-answer decision:
    the subject no longer aborts)."""
    answer = await get_chunk_record_grounding_answer(
        stage_label=stage_label,
        subject_unique_id=subject_unique_id,
        field_name=field_name,
        chunk_bounds=chunk_bounds,
        catalog=catalog,
        group_req_ids=group_req_ids,
        completed_request_map=completed_request_map,
        timestamp=timestamp,
        allowed_labels=allowed_labels,
        retry_req_ids=retry_req_ids,
    )
    if answer.missing_ids:
        logger.warning(
            f"{stage_label}: {len(answer.missing_ids)} record(s) of chunk "
            f"{chunk_bounds} in {subject_unique_id}:{field_name} still unanswered "
            f"after the retry pass: {answer.missing_ids}"
        )
    return answer.results


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


def build_group_record_payloads(
    group_records: GroupRecords,
    *,
    already_identified: Optional[dict[str, list[str]]] = None,
) -> dict[str, dict[str, Any]]:
    """The group_id → record payload a grounding request renders into its
    blocks (3.3, D16): the group's focal form and its synthesis. The group_id
    key is already opaque (``hash(normalized key)``, D11), so the v2
    key-masking discipline holds with no masking step — the key never reads
    like a candidate label, while the record's fields carry the wording
    grounding needs. The OOV pass adds each record's already-identified
    results, sorted for render stability.
    """
    payloads: dict[str, dict[str, Any]] = {}
    for group_id, record in group_records.items():
        payload: dict[str, Any] = record.model_dump()
        if already_identified is not None:
            payload["already_identified"] = sorted(
                already_identified.get(group_id, [])
            )
        payloads[group_id] = payload
    return payloads


def retry_record_payloads(
    stage_label: str,
    subject_unique_id: str,
    field_name: str,
    chunk_bounds: str,
    payloads: dict[str, dict[str, Any]],
    retry_record_ids: list[str],
) -> dict[str, dict[str, Any]]:
    """The chunk's payloads restricted to the stored retry ids, for the retry
    request set. An id the payload map does not carry raises: the retry ids
    were computed from these very payloads, so a mismatch means the upstream
    state changed under the stored ids (re-defer)."""
    unknown = [rid for rid in retry_record_ids if rid not in payloads]
    if unknown:
        raise ValueError(
            f"{stage_label}: stored retry record id(s) {unknown} of chunk "
            f"{chunk_bounds} in {subject_unique_id}:{field_name} are not among "
            f"the chunk's records; the upstream state changed under the stored "
            f"request ids (re-defer)."
        )
    return {rid: payloads[rid] for rid in retry_record_ids}


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
    retry_req_ids_by_chunk: dict[str, list[BatchRequestIDType]],
    retry_record_ids_by_chunk: dict[str, list[str]],
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

    ``retry_req_ids_by_chunk`` / ``retry_record_ids_by_chunk`` carry the
    stage's under-answer retry (3.3 — the synthesis pattern): the stored
    missing record ids, re-packed through the same grouping helper.
    """
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
                    f"{stage_label}: embedded group count ({len(group_req_ids)}) "
                    f"does not match computed group count ({len(payload_groups)}) "
                    f"for chunk bounds {chunk_bounds} in "
                    f"{subject_unique_id}:{field_name}. Group counts are computed "
                    f"once, upfront, from the same upstream records, so this "
                    f"should not happen."
                )

            # The retry pass: the stored missing ids, packed like the first pass.
            retry_req_ids = retry_req_ids_by_chunk.get(chunk_bounds, [])
            if set(retry_req_ids) & missing_req_ids:
                retry_payloads = retry_record_payloads(
                    stage_label,
                    subject_unique_id,
                    field_name,
                    chunk_bounds,
                    payloads,
                    retry_record_ids_by_chunk.get(chunk_bounds, []),
                )
                retry_payload_groups = grouped_record_payloads(
                    retry_payloads, max_records_per_request
                )
                if not retry_payloads or len(retry_req_ids) != len(retry_payload_groups):
                    raise ValueError(
                        f"{stage_label}: embedded retry group count "
                        f"({len(retry_req_ids)}) does not match the computed count "
                        f"({len(retry_payload_groups)}, {len(retry_payloads)} record(s)) "
                        f"for chunk bounds {chunk_bounds} in "
                        f"{subject_unique_id}:{field_name}."
                    )
                for retry_group_index, retry_req_id in enumerate(retry_req_ids):
                    if retry_req_id not in missing_req_ids:
                        continue
                    logger.info(
                        f"{stage_label}: retrying "
                        f"{len(retry_payload_groups[retry_group_index])} unanswered "
                        f"record(s) for {subject_unique_id}:{field_name} chunk "
                        f"{chunk_bounds} retry group {retry_group_index}"
                    )
                    batch_requests.append(
                        create_deferred_record_grounding_gpt_request(
                            deferred_at=deferred_at,
                            subject_unique_id=subject_unique_id,
                            request_id=retry_req_id,
                            prompt=prompt,
                            catalog=catalog,
                            record_payloads=retry_payload_groups[retry_group_index],
                            options_section=options_section_by_chunk[chunk_bounds],
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
