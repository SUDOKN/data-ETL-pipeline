from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import datetime
from typing import Optional

from pydantic import ValidationError

from core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.catalog_wire_schema import (
    NO_CANDIDATE,
    flatten_rule_slots,
    response_format_for,
    screening_response_model,
)
from core.models.rule_catalog import STAGE_RELATIONSHIP_SCREENING
from core.services.applied_rule_validation import (
    passed_implied_by,
    check_applied_rules,
    raise_for_violations,
)
from core.services.rule_catalog_registry import get_rule_catalog
from core.models.extraction_schemas.screening import (
    LiveScreeningResults,
    ScreeningVerdict,
)
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import (
    LLM_Model,
    NO_MODEL,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestMap,
    LLMPhraseExtractionRequestBundle,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
    get_phrase_relationship_result,
)
from core.models.field_types import ExtractionFieldType
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_phrases,
    render_phrase_blocks,
)
from llm_providers.field_types import BatchRequestIDType

from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error_capped,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)

logger = logging.getLogger(__name__)


def get_screening_response_schema(field_type: ExtractionFieldType) -> dict:
    """This stage's strict ``response_format``, for one field type.

    Per catalog rather than a module constant, because the schema now names the
    catalog's own rule ids as required properties — which is what stops a response
    omitting one. See ``catalog_wire_schema``.
    """
    return response_format_for(
        get_rule_catalog(STAGE_RELATIONSHIP_SCREENING, field_type.name)
    )


def parse_llm_phrase_relationship_screening_result(
    gpt_response: Optional[str],
    field_type: ExtractionFieldType,
) -> LiveScreeningResults:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_phrase_relationship_screening_result: Empty or invalid response from GPT"
        )

    catalog = get_rule_catalog(STAGE_RELATIONSHIP_SCREENING, field_type.name)

    try:
        parsed = screening_response_model(catalog).model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_llm_phrase_relationship_screening_result: Invalid response from GPT:{gpt_response}"
        ) from e

    raw_gpt_phrase_relationship_screening_result: LiveScreeningResults = {}
    violations: list[str] = []
    for entry in parsed.screenings:
        if entry.phrase in raw_gpt_phrase_relationship_screening_result:
            raise ValueError(
                f"parse_llm_phrase_relationship_screening_result: Duplicate phrase {entry.phrase!r} in screenings response"
            )

        # The no-candidate branch (locked #21): nothing was identified, so the
        # conditions had no candidate to be about and there are no rules to report.
        # It is a branch of the wire union rather than an empty rule list, so a
        # response cannot half-take it the way one did on 2026-08-11 — reporting a
        # null entity AND a single failed condition, which matched neither shape.
        if entry.outcome == NO_CANDIDATE:
            raw_gpt_phrase_relationship_screening_result[entry.phrase] = (
                ScreeningVerdict(
                    passed=False,
                    identified_entity=None,
                    applied_rules=[],
                    no_candidate_explanation=entry.explanation,
                )
            )
            continue

        applied_rules = flatten_rule_slots(catalog, entry)
        report = check_applied_rules(
            catalog=catalog,
            applied_rules=applied_rules,
            where=f"phrase {entry.phrase!r}",
        )
        if report.problems:
            violations.extend(report.problems)
            # Keep walking so the error names every phrase that needs fixing — the
            # request is lost either way — but derive nothing from a report that
            # failed its own catalog.
            continue

        # Derived, never reported: the rules ARE the decision procedure.
        # ``identified_entity`` needs no pass/null cross-check any more — it is
        # non-null by construction on this branch.
        raw_gpt_phrase_relationship_screening_result[entry.phrase] = ScreeningVerdict(
            passed=passed_implied_by(catalog, applied_rules),
            identified_entity=entry.identified_entity,
            applied_rules=applied_rules,
        )

    raise_for_violations(violations)

    logger.debug(
        f"raw_gpt_phrase_relationship_screening_result:{raw_gpt_phrase_relationship_screening_result}"
    )

    return raw_gpt_phrase_relationship_screening_result


def get_verified_live_screening_results(
    live_screening_results: LiveScreeningResults,
) -> LiveScreeningResults:
    """Filter live (structured, boolean-verdict) screening results down to only the
    phrases that passed. Distinct from ground_truth_helper_util.get_verified_phrase_relationship_results,
    which operates on legacy human-corrected ground-truth data using the Yes—/No—
    string convention — do not merge these two paths."""
    return {
        phrase: verdict
        for phrase, verdict in live_screening_results.items()
        if verdict.passed
    }


async def parse_phrase_relationship_screening_group_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    group_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    repairs: Optional[dict[str, str]] = None,
) -> LiveScreeningResults:
    """Parse the screening verdicts returned by a single screening group request."""
    req_obj = completed_request_map.get(group_req_id)
    if not req_obj:
        raise ValueError(
            f"llm_phrase_relationship_screening_node.parse_batch_request_result: Missing GPTBatchRequest for phrase_relationship_screening request ID {group_req_id} in {subject_unique_id}:{field_type.name}"
        )
    elif not req_obj.response:
        raise ValueError(
            f"llm_phrase_relationship_screening_node.parse_batch_request_result: GPTBatchRequest for phrase_relationship_screening request ID {group_req_id} has no response_blob in {subject_unique_id}:{field_type.name}"
        )

    try:
        parsed_map = parse_llm_phrase_relationship_screening_result(
            gpt_response=req_obj.response.result,
            field_type=field_type,
        )
        # Screening was the one phrase stage that RENDERED the sent-phrases line
        # and never read it back, so a response that answered under a different
        # string passed schema validation and surfaced two nodes later as a
        # phrase-set mismatch in freehand grounding's embed — an abort with no
        # error recorded and therefore no re-dispatch, permanently stuck
        # (steelcraft.com, 2026-08-12). `raise` rather than `drop` because
        # screening is a TOTAL function: measured over that run, all 26 groups
        # returned a verdict for every phrase sent. It filters downstream in
        # get_verified_live_screening_results, never by omitting an entry. Safe
        # to re-dispatch: embed_request_ids skips chunks whose ids exist, so it
        # never wipes the error history the cap counts.
        return hold_response_to_sent_phrases(
            user_message=req_obj.request.body.user_message(),
            response_by_phrase=parsed_map,
            where=f"{subject_unique_id}:{field_type.name} relationship screening {group_req_id}",
            on_missing="raise",
            repairs=repairs,
        )
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"llm_phrase_relationship_screening_node.parse_batch_request_result: Error parsing phrase_relationship results for subject {subject_unique_id} from GPT response: {e}"
        )
        raise


async def get_phrase_relationship_screening_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    repairs: Optional[dict[str, str]] = None,
) -> LiveScreeningResults:
    """Merge screening verdicts across every group embedded for the chunk.

    ``repairs`` is forwarded to the per-group hold; see
    ``hold_response_to_sent_phrases``.
    """
    group_req_ids = extraction_bundle.llm_phrase_relationship_screening_req_ids
    if not group_req_ids:
        raise ValueError(
            f"llm_phrase_relationship_screening_node.parse_batch_request_result: phrase_relationship_screening_req_ids is empty for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )

    merged_results: LiveScreeningResults = {}
    for group_req_id in group_req_ids:
        merged_results.update(
            await parse_phrase_relationship_screening_group_result(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                group_req_id=group_req_id,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
                repairs=repairs,
            )
        )
    return merged_results


def _split_into_pair_groups(
    results: LLMPhraseRelationshipResults, group_size: int
) -> list[LLMPhraseRelationshipResults]:
    """Split an ordered phrase->relationship dict into ordered groups of at most
    *group_size* pairs. Always returns at least one (possibly empty) group so the
    existing single-dummy-request-per-chunk fallback keeps working when a chunk
    has no relationship pairs at all."""
    items = list(results.items())
    if not items:
        return [{}]
    return [dict(items[i : i + group_size]) for i in range(0, len(items), group_size)]


async def create_missing_phrase_relationship_screening_requests(
    subject_unique_id: str,
    subject_name: str,
    field_type: ExtractionFieldType,  # used for logging and debugging
    chunked_request_map: LLMPhraseExtractionRequestMap,
    missing_phrase_relationship_screening_req_ids: set[BatchRequestIDType],
    subject_text: str,
    phrase_relationship_screening_prompt: Prompt,
    llm_phrase_relationship_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    deferred_at: datetime,
    llm_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
    max_pairs_per_request: int,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_phrase_relationship_screening_requests: Generating GPTBatchRequests for {subject_unique_id}:{field_type}"
    )

    batch_requests: list[GPTBatchRequest] = []
    chunk_items = [
        (chunk_bounds, bundle)
        for chunk_bounds, bundle in chunked_request_map.items()
        if set(bundle.llm_phrase_relationship_screening_req_ids)
        & missing_phrase_relationship_screening_req_ids
    ]
    # Create lookup map: custom_id -> GPTBatchRequest
    if not llm_phrase_relationship_gpt_request_map:
        raise ValueError(
            f"create_missing_phrase_relationship_screening_requests: No completed GPTBatchRequests found for {subject_unique_id}:{field_type} in upstream_completed_batch_req_map"
        )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, extraction_bundle in batch:
            llm_phrase_relationships = await get_phrase_relationship_result(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                completed_request_map=llm_phrase_relationship_gpt_request_map,
                timestamp=deferred_at,
            )
            pair_groups = _split_into_pair_groups(
                llm_phrase_relationships, max_pairs_per_request
            )
            group_req_ids = extraction_bundle.llm_phrase_relationship_screening_req_ids
            if len(group_req_ids) != len(pair_groups):
                raise ValueError(
                    f"create_missing_phrase_relationship_screening_requests: embedded group count "
                    f"({len(group_req_ids)}) does not match computed group count ({len(pair_groups)}) "
                    f"for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type}. Group counts are "
                    f"computed once, upfront, from the same relationship results, so this should not happen."
                )

            start, end = int(chunk_bounds.split(":")[0]), int(
                chunk_bounds.split(":")[1]
            )

            for group_index, group_req_id in enumerate(group_req_ids):
                if group_req_id not in missing_phrase_relationship_screening_req_ids:
                    continue

                pair_group = pair_groups[group_index]
                if not pair_group:
                    # _split_into_pair_groups only ever produces an empty group as the
                    # sole element of a single-group list (zero relationship pairs total)
                    if len(pair_groups) != 1:
                        raise ValueError(
                            f"create_missing_phrase_relationship_screening_requests: unexpected empty "
                            f"pair group at index {group_index} of {len(pair_groups)} groups for chunk "
                            f"bounds {chunk_bounds} in {subject_unique_id}:{field_type}. Only a single group should "
                            f"ever be empty (the zero-relationship-pairs case)."
                        )
                    # add a dummy response blob with empty dict
                    logger.info(
                        f"No phrases found in text, for {subject_unique_id}:{field_type}, creating dummy phrase_relationship_screening request"
                    )
                    new_batch_request = _create_dummy_completed_phrase_relationships_screening_batch_request(
                        deferred_at=deferred_at,
                        subject_unique_id=subject_unique_id,
                        llm_phrase_relationship_screening_request_id=group_req_id,
                        model_params=model_params,
                        eager=eager,
                    )
                else:
                    logger.info(
                        f"Passing on candidates {pair_group} to phrase_relationship_screening phase for "
                        f"{subject_unique_id}:{field_type} chunk {chunk_bounds} group {group_index}"
                    )
                    new_batch_request = create_deferred_phrase_relationship_screening_gpt_request(
                        deferred_at=deferred_at,
                        subject_unique_id=subject_unique_id,
                        llm_phrase_relationship_screening_request_id=group_req_id,
                        subject_name=subject_name,
                        subject_text=subject_text[start:end],
                        field_type=field_type,
                        phrase_relationship_results=pair_group,
                        phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
                        eager=eager,
                        gpt_model=llm_model,
                        model_params=model_params,
                    )

                batch_requests.append(new_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {subject_unique_id}:{field_type}"
            )

    return batch_requests


def _create_dummy_completed_phrase_relationships_screening_batch_request(
    deferred_at: datetime,
    subject_unique_id: str,
    llm_phrase_relationship_screening_request_id: BatchRequestIDType,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    if llm_phrase_relationship_screening_request_id is None:
        raise ValueError(
            "_create_dummy_completed_phrase_relationships_screening_batch_request: llm_phrase_relationship_screening_request_id is None"
        )

    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_relationship_screening_request_id,
        # Still carries a block, empty — an absent one has to stay an error.
        context=(
            "No phrase relationship screening needed - no phrases found in text.\n"
            f"{render_phrase_blocks({})}"
        ),
        prompt_text="No phrase relationship screening needed - no phrases found in text by brute force or by LLM.",
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_phrase_relationship_batch_id",
    )

    base_gpt_batch_request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=llm_phrase_relationship_screening_request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content='{"screenings": []}'
        ),
    )

    return base_gpt_batch_request


def create_deferred_phrase_relationship_screening_gpt_request(
    deferred_at: datetime,
    subject_unique_id: str,
    llm_phrase_relationship_screening_request_id: str,
    subject_name: str,
    subject_text: str,
    field_type: ExtractionFieldType,
    phrase_relationship_results: LLMPhraseRelationshipResults,
    phrase_relationship_screening_prompt: Prompt,
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_phrase_relationship_screening_gpt_request: Generating GPTBatchRequest for {llm_phrase_relationship_screening_request_id}"
    )
    context = (
        # Keep the blocks at column 0. `_PHRASES_RE` tolerates leading spaces on
        # the fence lines now, but only because this stage once ended its prefix
        # with `\n\n ` and that single space hid the block from the reader for the
        # stage's whole life -- rendered every request, read back never.
        # f"Manufacturer name: {subject_name}\n\n"
        f"{render_phrase_blocks(phrase_relationship_results)}"
    )

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_relationship_screening_request_id,
        context=context,
        prompt_text=phrase_relationship_screening_prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(
            get_screening_response_schema(field_type)
        ),
        batch_id="Eager" if eager else None,
    )
    return gpt_batch_request
