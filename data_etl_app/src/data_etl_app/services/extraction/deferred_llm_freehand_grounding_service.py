from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime
from typing import Optional

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_extraction.deferred_keyword_extraction import (
    KeywordExtractionRequestBundle,
    KeywordExtractionRequestMap,
)
from core.models.field_types import (
    LLMFreehandGroundingResults,
    LLMPhraseRelationshipResults,
)
from core.models.batch_request_objects.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from core.models.llm_model import LLM_Model, NO_MODEL
from core.models.file_objects.prompt import Prompt
from core.services.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)
from core.services.gpt_batch_request_writes import record_response_parse_error
from data_etl_app.models.types_and_enums import LLMExtractedFieldTypeEnum
from data_etl_app.services.extraction.deferred_llm_phrase_relationship_node_service import (
    parse_batch_request_result as parse_phrase_relationship_batch_req_result,
)
from data_etl_app.services.extraction.deferred_llm_relationship_screening_node_service import (
    parse_batch_request_result as parse_relationship_screening_batch_req_result,
)
from data_etl_app.utils.ground_truth_helper_util import (
    get_verified_phrase_relationship_results,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from open_ai_key_app.models.gpt_model_params import GPTModelParams

logger = logging.getLogger(__name__)


def parse_llm_phrase_freehand_grounding_result(
    gpt_response: Optional[str],
) -> LLMFreehandGroundingResults:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_phrase_freehand_grounding_result: Empty or invalid response from GPT"
        )

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        raw_llm_freehand_grounding_result: dict[str, dict[str, str]] = json.loads(
            gpt_response
        )
        logger.debug(
            f"raw_llm_freehand_grounding_result:{json.dumps(raw_llm_freehand_grounding_result, indent=2)}"
        )
    except json.JSONDecodeError as e:
        raise ValueError(
            f"parse_llm_phrase_freehand_grounding_result: Invalid response from GPT:{gpt_response}"
        ) from e

    if not isinstance(raw_llm_freehand_grounding_result, dict):
        raise ValueError(
            "parse_llm_phrase_freehand_grounding_result: Expected raw_llm_freehand_grounding_result to be a dictionary"
        )

    return raw_llm_freehand_grounding_result


async def parse_batch_request_result(
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    extraction_bundle: KeywordExtractionRequestBundle,
    completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    deferred_at: datetime,
) -> LLMFreehandGroundingResults:
    req_id = extraction_bundle.llm_phrase_freehand_grounding_req_id
    if not req_id:
        raise ValueError(
            f"phrase_freehand_grounding_node.parse_batch_request_result: phrase_freehand_grounding_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
        )

    req_obj = completed_request_map.get(req_id)
    if not req_obj:
        raise ValueError(
            f"phrase_freehand_grounding_node.parse_batch_request_result: Missing GPTBatchRequest for phrase_freehand_grounding request ID {req_id} in {mfg_etld1}:{field_type.name}"
        )
    if not req_obj.response:
        raise ValueError(
            f"phrase_freehand_grounding_node.parse_batch_request_result: GPTBatchRequest for phrase_freehand_grounding request ID {req_id} has no response_blob in {mfg_etld1}:{field_type.name}"
        )

    try:
        return parse_llm_phrase_freehand_grounding_result(req_obj.response.result)
    except Exception as e:
        await record_response_parse_error(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=deferred_at,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"phrase_freehand_grounding_node.parse_batch_request_result: Error parsing phrase_freehand_grounding results for manufacturer {mfg_etld1} from GPT response: {e}"
        )
        raise


async def create_missing_phrase_freehand_grounding_requests(
    mfg_etld1: str,
    mfg_name: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunked_request_map: KeywordExtractionRequestMap,
    missing_phrase_freehand_grounding_req_ids: set[GPTBatchRequestCustomID],
    phrase_freehand_grounding_prompt: Prompt,
    llm_phrase_relationship_gpt_request_map: dict[
        GPTBatchRequestCustomID, GPTBatchRequest
    ],
    llm_phrase_screening_gpt_request_map: dict[
        GPTBatchRequestCustomID, GPTBatchRequest
    ],
    deferred_at: datetime,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_phrase_freehand_grounding_requests: Generating GPTBatchRequests for {mfg_etld1}:{field_type}"
    )

    if (
        not llm_phrase_relationship_gpt_request_map
        or not llm_phrase_screening_gpt_request_map
    ):
        raise ValueError(
            f"create_missing_phrase_freehand_grounding_requests: No completed GPTBatchRequests found for {mfg_etld1}:{field_type} in upstream maps."
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
                    mfg_etld1=mfg_etld1,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=extraction_bundle,
                    completed_request_map=llm_phrase_relationship_gpt_request_map,
                    deferred_at=deferred_at,
                )
            )
            llm_phrase_relationship_screening_results = (
                await parse_relationship_screening_batch_req_result(
                    mfg_etld1=mfg_etld1,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=extraction_bundle,
                    completed_request_map=llm_phrase_screening_gpt_request_map,
                    deferred_at=deferred_at,
                )
            )

            left_only_phrases = (
                llm_phrase_relationship_results.keys()
                - llm_phrase_relationship_screening_results.keys()
            )
            if left_only_phrases:
                raise ValueError(
                    f"Phrases {left_only_phrases} found in relationship results but not in screening results for {mfg_etld1}:{field_type} chunk {chunk_bounds}"
                )
            right_only_phrases = (
                llm_phrase_relationship_screening_results.keys()
                - llm_phrase_relationship_results.keys()
            )
            if right_only_phrases:
                raise ValueError(
                    f"Phrases {right_only_phrases} found in screening results but were never listed in relationship results for {mfg_etld1}:{field_type} chunk {chunk_bounds}"
                )

            screened_phrases_w_reason = get_verified_phrase_relationship_results(
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
                    f"create_missing_phrase_freehand_grounding_requests: llm_phrase_freehand_grounding_req_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type}"
                )

            if not verified_phrases_w_og_summary:
                new_batch_request = (
                    _create_dummy_completed_phrase_freehand_grounding_batch_request(
                        deferred_at=deferred_at,
                        etld1=mfg_etld1,
                        llm_phrase_freehand_grounding_request_id=req_id,
                        model_params=model_params,
                        eager=eager,
                    )
                )
            else:
                new_batch_request = create_deferred_phrase_freehand_grounding_gpt_request(
                    deferred_at=deferred_at,
                    etld1=mfg_etld1,
                    llm_phrase_freehand_grounding_request_id=req_id,
                    phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
                    mfg_name=mfg_name,
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
                f"gpt request for {mfg_etld1}:{field_type}"
            )

    return batch_requests


def _create_dummy_completed_phrase_freehand_grounding_batch_request(
    deferred_at: datetime,
    etld1: str,
    llm_phrase_freehand_grounding_request_id: GPTBatchRequestCustomID,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_phrase_freehand_grounding_request_id,
        context="No phrase freehand grounding needed - no phrases passed screening.",
        prompt=Prompt(
            name="dummy_phrase_freehand_grounding_prompt",
            text="No phrase freehand grounding needed - no phrases passed screening.",
            s3_version_id="dummy_s3_version_id",
            num_tokens=1,
        ),
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_phrase_freehand_grounding_batch_id",
    )

    base_gpt_batch_request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=llm_phrase_freehand_grounding_request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content="```json\n{}\n```"
        ),
    )

    return base_gpt_batch_request


def create_deferred_phrase_freehand_grounding_gpt_request(
    deferred_at: datetime,
    etld1: str,
    llm_phrase_freehand_grounding_request_id: GPTBatchRequestCustomID,
    phrase_freehand_grounding_prompt: Prompt,
    mfg_name: str,
    verified_phrases_w_og_summary: LLMPhraseRelationshipResults,
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_phrase_freehand_grounding_gpt_request: Generating GPTBatchRequest for {llm_phrase_freehand_grounding_request_id}"
    )
    context = (
        f"Manufacturer name: {mfg_name}\n\n"
        f"screened product phrases and evidence:\n{json.dumps(verified_phrases_w_og_summary, indent=2)}"
    )

    return create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_phrase_freehand_grounding_request_id,
        context=context,
        prompt=phrase_freehand_grounding_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
        batch_id="Eager" if eager else None,
    )
