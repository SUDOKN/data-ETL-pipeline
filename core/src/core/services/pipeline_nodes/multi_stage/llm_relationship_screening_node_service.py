from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime
from typing import Optional

from pydantic import ValidationError

from core.models.extraction_schemas.relationship import LLMPhraseRelationshipResults
from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)
from core.models.extraction_schemas.screening import (
    LiveScreeningResults,
    PhraseRelationshipScreeningResponse,
    ScreeningVerdict,
)
from core.models.file_objects.prompt import Prompt
from core.models.llm_model import LLM_Model, NO_MODEL
from core.models.batch_request_objects.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestMap,
    LLMPhraseExtractionRequestBundle,
)
from data_etl_app.models.pipeline_nodes.multi_stage.base.llm_phrase_relationship_node import (
    LLMPhraseRelationshipNode,
)
from core.models.types_and_enums import LLMExtractedFieldTypeEnum
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

from core.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error,
)
from core.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)

logger = logging.getLogger(__name__)


LLM_PHRASE_RELATIONSHIP_SCREENING_RESPONSE_SCHEMA = build_gpt_response_format(
    PhraseRelationshipScreeningResponse, name="phrase_relationship_screening_result"
)


def parse_llm_phrase_relationship_screening_result(
    gpt_response: Optional[str],
) -> LiveScreeningResults:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_phrase_relationship_screening_result: Empty or invalid response from GPT"
        )

    try:
        parsed = PhraseRelationshipScreeningResponse.model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_llm_phrase_relationship_screening_result: Invalid response from GPT:{gpt_response}"
        ) from e

    raw_gpt_phrase_relationship_screening_result: LiveScreeningResults = {}
    for entry in parsed.screenings:
        if entry.phrase in raw_gpt_phrase_relationship_screening_result:
            raise ValueError(
                f"parse_llm_phrase_relationship_screening_result: Duplicate phrase {entry.phrase!r} in screenings response"
            )
        raw_gpt_phrase_relationship_screening_result[entry.phrase] = ScreeningVerdict(
            passed=entry.passed, reason=entry.reason
        )

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
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    group_req_id: GPTBatchRequestCustomID,
    completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    timestamp: datetime,
) -> LiveScreeningResults:
    """Parse the screening verdicts returned by a single screening group request."""
    req_obj = completed_request_map.get(group_req_id)
    if not req_obj:
        raise ValueError(
            f"llm_phrase_relationship_screening_node.parse_batch_request_result: Missing GPTBatchRequest for phrase_relationship_screening request ID {group_req_id} in {mfg_etld1}:{field_type.name}"
        )
    elif not req_obj.response:
        raise ValueError(
            f"llm_phrase_relationship_screening_node.parse_batch_request_result: GPTBatchRequest for phrase_relationship_screening request ID {group_req_id} has no response_blob in {mfg_etld1}:{field_type.name}"
        )

    try:
        return parse_llm_phrase_relationship_screening_result(req_obj.response.result)
    except Exception as e:
        await record_response_parse_error(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"llm_phrase_relationship_screening_node.parse_batch_request_result: Error parsing phrase_relationship results for manufacturer {mfg_etld1} from GPT response: {e}"
        )
        raise


async def get_phrase_relationship_screening_result(
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    timestamp: datetime,
) -> LiveScreeningResults:
    """Merge screening verdicts across every group embedded for the chunk."""
    group_req_ids = extraction_bundle.llm_phrase_relationship_screening_req_ids
    if not group_req_ids:
        raise ValueError(
            f"llm_phrase_relationship_screening_node.parse_batch_request_result: phrase_relationship_screening_req_ids is empty for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
        )

    merged_results: LiveScreeningResults = {}
    for group_req_id in group_req_ids:
        merged_results.update(
            await parse_phrase_relationship_screening_group_result(
                mfg_etld1=mfg_etld1,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                group_req_id=group_req_id,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
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
    mfg_etld1: str,
    mfg_name: str,
    field_type: LLMExtractedFieldTypeEnum,  # used for logging and debugging
    chunked_request_map: LLMPhraseExtractionRequestMap,
    missing_phrase_relationship_screening_req_ids: set[GPTBatchRequestCustomID],
    mfg_text: str,
    phrase_relationship_screening_prompt: Prompt,
    llm_phrase_relationship_gpt_request_map: dict[
        GPTBatchRequestCustomID, GPTBatchRequest
    ],
    deferred_at: datetime,
    llm_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
    max_pairs_per_request: int,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_phrase_relationship_screening_requests: Generating GPTBatchRequests for {mfg_etld1}:{field_type}"
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
            f"create_missing_phrase_relationship_screening_requests: No completed GPTBatchRequests found for {mfg_etld1}:{field_type} in upstream_completed_batch_req_map"
        )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, extraction_bundle in batch:
            llm_phrase_relationships = await LLMPhraseRelationshipNode.get_result(
                mfg_etld1=mfg_etld1,
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
                    f"for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type}. Group counts are "
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
                            f"bounds {chunk_bounds} in {mfg_etld1}:{field_type}. Only a single group should "
                            f"ever be empty (the zero-relationship-pairs case)."
                        )
                    # add a dummy response blob with empty dict
                    logger.info(
                        f"No phrases found in text, for {mfg_etld1}:{field_type}, creating dummy phrase_relationship_screening request"
                    )
                    new_batch_request = _create_dummy_completed_phrase_relationships_screening_batch_request(
                        deferred_at=deferred_at,
                        etld1=mfg_etld1,
                        llm_phrase_relationship_screening_request_id=group_req_id,
                        model_params=model_params,
                        eager=eager,
                    )
                else:
                    logger.info(
                        f"Passing on candidates {pair_group} to phrase_relationship_screening phase for "
                        f"{mfg_etld1}:{field_type} chunk {chunk_bounds} group {group_index}"
                    )
                    new_batch_request = create_deferred_phrase_relationship_screening_gpt_request(
                        deferred_at=deferred_at,
                        etld1=mfg_etld1,
                        llm_phrase_relationship_screening_request_id=group_req_id,
                        mfg_name=mfg_name,
                        mfg_text=mfg_text[start:end],
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
                f"gpt request for {mfg_etld1}:{field_type}"
            )

    return batch_requests


def _create_dummy_completed_phrase_relationships_screening_batch_request(
    deferred_at: datetime,
    etld1: str,
    llm_phrase_relationship_screening_request_id: GPTBatchRequestCustomID,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    if llm_phrase_relationship_screening_request_id is None:
        raise ValueError(
            "_create_dummy_completed_phrase_relationships_screening_batch_request: llm_phrase_relationship_screening_request_id is None"
        )

    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_phrase_relationship_screening_request_id,
        context="No phrase relationship screening needed - no phrases found in text.",
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
    etld1: str,
    llm_phrase_relationship_screening_request_id: str,
    mfg_name: str,
    mfg_text: str,
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
        f"Manufacturer name: {mfg_name}\n\n "
        f"extracted phrases:\n{json.dumps(phrase_relationship_results)}"
    )

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_phrase_relationship_screening_request_id,
        context=context,
        prompt_text=phrase_relationship_screening_prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(
            LLM_PHRASE_RELATIONSHIP_SCREENING_RESPONSE_SCHEMA
        ),
        batch_id="Eager" if eager else None,
    )
    return gpt_batch_request
