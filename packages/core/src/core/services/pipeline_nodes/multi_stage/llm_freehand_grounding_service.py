from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime
from typing import Optional

from pydantic import ValidationError

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from core.models.deferred_extraction.deferred_keyword_extraction import (
    KeywordExtractionRequestBundle,
    KeywordExtractionRequestMap,
)
from core.models.extraction_schemas.grounding import (
    PhraseGroundingResponse,
    PhraseToTagAndReasonMap,
)
from core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from llm_providers.models.llm_model import (
    LLM_Model,
    NO_MODEL,
)
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error,
)
from core.models.types_and_enums import LLMExtractedFieldTypeEnum
from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
    get_phrase_relationship_result as parse_phrase_relationship_batch_req_result,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    get_phrase_relationship_screening_result as parse_relationship_screening_batch_req_result,
    get_verified_live_screening_results,
)
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)

logger = logging.getLogger(__name__)


LLM_PHRASE_FREEHAND_GROUNDING_RESPONSE_SCHEMA = build_gpt_response_format(
    PhraseGroundingResponse, name="phrase_freehand_grounding_result"
)


def parse_llm_phrase_freehand_grounding_result(
    gpt_response: Optional[str],
) -> PhraseToTagAndReasonMap:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_phrase_freehand_grounding_result: Empty or invalid response from GPT"
        )

    try:
        parsed = PhraseGroundingResponse.model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_llm_phrase_freehand_grounding_result: Invalid response from GPT:{gpt_response}"
        ) from e

    raw_llm_freehand_grounding_result: PhraseToTagAndReasonMap = {}
    for entry in parsed.groundings:
        if entry.phrase in raw_llm_freehand_grounding_result:
            raise ValueError(
                f"parse_llm_phrase_freehand_grounding_result: Duplicate phrase {entry.phrase!r} in groundings response"
            )
        raw_llm_freehand_grounding_result[entry.phrase] = {
            tag.tag: tag.reason for tag in entry.tags
        }

    return raw_llm_freehand_grounding_result


async def get_freehand_grounding_result(
    subject_unique_id: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    extraction_bundle: KeywordExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> PhraseToTagAndReasonMap:
    req_id = extraction_bundle.llm_phrase_freehand_grounding_req_id
    if not req_id:
        raise ValueError(
            f"phrase_freehand_grounding_node.parse_batch_request_result: phrase_freehand_grounding_request_id is None for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )

    req_obj = completed_request_map.get(req_id)
    if not req_obj:
        raise ValueError(
            f"phrase_freehand_grounding_node.parse_batch_request_result: Missing GPTBatchRequest for phrase_freehand_grounding request ID {req_id} in {subject_unique_id}:{field_type.name}"
        )
    if not req_obj.response:
        raise ValueError(
            f"phrase_freehand_grounding_node.parse_batch_request_result: GPTBatchRequest for phrase_freehand_grounding request ID {req_id} has no response_blob in {subject_unique_id}:{field_type.name}"
        )

    try:
        return parse_llm_phrase_freehand_grounding_result(req_obj.response.result)
    except Exception as e:
        await record_response_parse_error(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"phrase_freehand_grounding_node.parse_batch_request_result: Error parsing phrase_freehand_grounding results for manufacturer {subject_unique_id} from GPT response: {e}"
        )
        raise


async def create_missing_phrase_freehand_grounding_requests(
    subject_unique_id: str,
    subject_name: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunked_request_map: KeywordExtractionRequestMap,
    missing_phrase_freehand_grounding_req_ids: set[BatchRequestIDType],
    phrase_freehand_grounding_prompt: Prompt,
    llm_phrase_relationship_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    llm_phrase_screening_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    deferred_at: datetime,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_phrase_freehand_grounding_requests: Generating GPTBatchRequests for {subject_unique_id}:{field_type}"
    )

    if (
        not llm_phrase_relationship_gpt_request_map
        or not llm_phrase_screening_gpt_request_map
    ):
        raise ValueError(
            f"create_missing_phrase_freehand_grounding_requests: No completed GPTBatchRequests found for {subject_unique_id}:{field_type} in upstream maps."
        )

    batch_requests: list[GPTBatchRequest] = []
    chunk_items = list(
        {
            chunk_bounds: bundle
            for chunk_bounds, bundle in chunked_request_map.items()
            if bundle.llm_phrase_freehand_grounding_req_id
            in missing_phrase_freehand_grounding_req_ids
        }.items()
    )

    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        for chunk_bounds, extraction_bundle in batch:
            llm_phrase_relationship_results = (
                await parse_phrase_relationship_batch_req_result(
                    subject_unique_id=subject_unique_id,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=extraction_bundle,
                    completed_request_map=llm_phrase_relationship_gpt_request_map,
                    timestamp=deferred_at,
                )
            )
            llm_phrase_relationship_screening_results = (
                await parse_relationship_screening_batch_req_result(
                    subject_unique_id=subject_unique_id,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=extraction_bundle,
                    completed_request_map=llm_phrase_screening_gpt_request_map,
                    timestamp=deferred_at,
                )
            )

            left_only_phrases = (
                llm_phrase_relationship_results.keys()
                - llm_phrase_relationship_screening_results.keys()
            )
            if left_only_phrases:
                raise ValueError(
                    f"Phrases {left_only_phrases} found in relationship results:{extraction_bundle.llm_phrase_relationship_req_id} but not in screening results:{extraction_bundle.llm_phrase_relationship_screening_req_ids} for {subject_unique_id}:{field_type} chunk {chunk_bounds}"
                )
            right_only_phrases = (
                llm_phrase_relationship_screening_results.keys()
                - llm_phrase_relationship_results.keys()
            )
            if right_only_phrases:
                raise ValueError(
                    f"Phrases {right_only_phrases} found in screening results:{extraction_bundle.llm_phrase_relationship_screening_req_ids} but were never listed in relationship results:{extraction_bundle.llm_phrase_relationship_req_id} for {subject_unique_id}:{field_type} chunk {chunk_bounds}"
                )

            screened_phrases_w_reason = get_verified_live_screening_results(
                llm_phrase_relationship_screening_results
            )
            verified_phrases_w_og_summary: LLMPhraseRelationshipResults = {
                k: v
                for k, v in llm_phrase_relationship_results.items()
                if k in screened_phrases_w_reason
            }

            req_id = extraction_bundle.llm_phrase_freehand_grounding_req_id
            if not req_id:
                raise ValueError(
                    f"create_missing_phrase_freehand_grounding_requests: llm_phrase_freehand_grounding_req_id is None for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type}"
                )

            if not verified_phrases_w_og_summary:
                new_batch_request = (
                    _create_dummy_completed_phrase_freehand_grounding_batch_request(
                        deferred_at=deferred_at,
                        subject_unique_id=subject_unique_id,
                        llm_phrase_freehand_grounding_request_id=req_id,
                        model_params=model_params,
                        eager=eager,
                    )
                )
            else:
                new_batch_request = create_deferred_phrase_freehand_grounding_gpt_request(
                    deferred_at=deferred_at,
                    subject_unique_id=subject_unique_id,
                    llm_phrase_freehand_grounding_request_id=req_id,
                    phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
                    subject_name=subject_name,
                    verified_phrases_w_og_summary=verified_phrases_w_og_summary,
                    gpt_model=llm_model,
                    eager=eager,
                    model_params=model_params,
                )

            batch_requests.append(new_batch_request)

        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {subject_unique_id}:{field_type}"
            )

    return batch_requests


def _create_dummy_completed_phrase_freehand_grounding_batch_request(
    deferred_at: datetime,
    subject_unique_id: str,
    llm_phrase_freehand_grounding_request_id: BatchRequestIDType,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_freehand_grounding_request_id,
        context="No phrase freehand grounding needed - no phrases passed screening.",
        prompt_text="No phrase freehand grounding needed - no phrases passed screening.",
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_phrase_freehand_grounding_batch_id",
    )

    base_gpt_batch_request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=llm_phrase_freehand_grounding_request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content='{"groundings": []}'
        ),
    )

    return base_gpt_batch_request


def create_deferred_phrase_freehand_grounding_gpt_request(
    deferred_at: datetime,
    subject_unique_id: str,
    llm_phrase_freehand_grounding_request_id: BatchRequestIDType,
    phrase_freehand_grounding_prompt: Prompt,
    subject_name: str,
    verified_phrases_w_og_summary: LLMPhraseRelationshipResults,
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_phrase_freehand_grounding_gpt_request: Generating GPTBatchRequest for {llm_phrase_freehand_grounding_request_id}"
    )
    context = (
        # f"Manufacturer name: {subject_name}\n\n"
        f"screened product phrases and evidence:\n{json.dumps(verified_phrases_w_og_summary, indent=2)}"
    )

    return create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_freehand_grounding_request_id,
        context=context,
        prompt_text=phrase_freehand_grounding_prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(
            LLM_PHRASE_FREEHAND_GROUNDING_RESPONSE_SCHEMA
        ),
        batch_id="Eager" if eager else None,
    )
