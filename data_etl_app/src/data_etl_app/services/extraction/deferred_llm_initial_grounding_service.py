from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime
from typing import Optional

from core.models.field_types import (
    LLMGroundingResults,
    LLMPhraseRelationshipResults,
)
from core.models.prompt import Prompt
from core.models.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_concept_extraction import (
    ConceptExtractionRequestMap,
    ConceptExtractionRequestBundle,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
    LLMExtractedFieldTypeEnum,
)
from data_etl_app.utils.ground_truth_helper_util import (
    get_verified_phrase_relationship_results,
)
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from core.models.llm_model import NO_MODEL, LLM_Model

from core.services.gpt_batch_request_writes import record_response_parse_error
from core.services.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)
from data_etl_app.services.extraction.deferred_llm_phrase_relationship_node_service import (
    parse_batch_request_result as parse_phrase_relationhip_batch_req_result,
)
from data_etl_app.services.extraction.deferred_llm_relationship_screening_node_service import (
    parse_batch_request_result as parse_relationhip_screening_batch_req_result,
)

logger = logging.getLogger(__name__)


def parse_llm_phrase_initial_grounding_result(
    gpt_response: Optional[str],
) -> LLMGroundingResults:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_phrase_initial_grounding_result: Empty or invalid response from GPT"
        )

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        raw_llm_initial_grounding_result: dict[str, dict[str, str]] = json.loads(
            gpt_response
        )
        logger.debug(
            f"raw_llm_initial_grounding_result:{json.dumps(raw_llm_initial_grounding_result, indent=2)}"
        )
    except json.JSONDecodeError as e:
        raise ValueError(
            f"parse_llm_phrase_initial_grounding_result: Invalid response from GPT:{gpt_response}"
        ) from e

    if not isinstance(raw_llm_initial_grounding_result, dict):
        raise ValueError(
            "parse_llm_phrase_initial_grounding_result: Expected raw_llm_initial_grounding_result to be a dictionary"
        )

    logger.debug(f"raw_llm_initial_grounding_result:{raw_llm_initial_grounding_result}")

    return raw_llm_initial_grounding_result


async def parse_batch_request_result(
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    extraction_bundle: ConceptExtractionRequestBundle,
    completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    deferred_at: datetime,
) -> LLMGroundingResults:
    req_id = extraction_bundle.llm_phrase_initial_grounding_req_id
    if not req_id:
        raise ValueError(
            f"phrase_initial_grounding_node.parse_batch_request_result: phrase_initial_grounding_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
        )

    req_obj = completed_request_map.get(req_id)
    if not req_obj:
        raise ValueError(
            f"phrase_initial_grounding_node.parse_batch_request_result: Missing GPTBatchRequest for phrase_initial_grounding request ID {req_id} in {mfg_etld1}:{field_type.name}"
        )
    elif not req_obj.response:
        raise ValueError(
            f"phrase_initial_grounding_node.parse_batch_request_result: GPTBatchRequest for phrase_initial_grounding request ID {req_id} has no response_blob in {mfg_etld1}:{field_type.name}"
        )

    try:
        phrase_initial_grounding_results = parse_llm_phrase_initial_grounding_result(
            req_obj.response.result
        )
        return phrase_initial_grounding_results
    except Exception as e:
        await record_response_parse_error(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=deferred_at,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"phrase_initial_grounding_node.parse_batch_request_result: Error parsing phrase_initial_grounding results for manufacturer {mfg_etld1} from GPT response: {e}"
        )
        raise


async def create_missing_phrase_initial_grounding_requests(
    # used for logging and debugging
    mfg_etld1: str,
    mfg_name: str,
    field_type: ConceptTypeEnum,
    # context
    chunked_request_map: ConceptExtractionRequestMap,
    missing_phrase_initial_grounding_req_ids: set[GPTBatchRequestCustomID],
    phrase_initial_grounding_prompt: Prompt,
    llm_phrase_relationship_gpt_request_map: dict[
        GPTBatchRequestCustomID, GPTBatchRequest
    ],
    llm_phrase_screening_gpt_request_map: dict[
        GPTBatchRequestCustomID, GPTBatchRequest
    ],
    known_concepts: set[Concept],  # DO NOT MUTATE
    # metadata
    deferred_at: datetime,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_phrase_initial_grounding_requests: Generating GPTBatchRequests for {mfg_etld1}:{field_type}"
    )

    batch_requests: list[GPTBatchRequest] = []
    chunk_items = list(
        {
            chunk_bounds: bundle
            for chunk_bounds, bundle in chunked_request_map.items()
            if bundle.llm_phrase_initial_grounding_req_id
            in missing_phrase_initial_grounding_req_ids
            # because sometimes some breq docs may have already been created
        }.items()
    )

    if (
        not llm_phrase_relationship_gpt_request_map
        or not llm_phrase_screening_gpt_request_map
    ):
        raise ValueError(
            f"create_missing_phrase_initial_grounding_requests: No completed GPTBatchRequests found for {mfg_etld1}:{field_type} in llm_phrase_relationship and llm_phrase_screening gpt_request_maps."
        )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, extraction_bundle in batch:
            llm_phrase_relationship_results = (
                await parse_phrase_relationhip_batch_req_result(
                    mfg_etld1=mfg_etld1,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=extraction_bundle,
                    completed_request_map=llm_phrase_relationship_gpt_request_map,
                    deferred_at=deferred_at,
                )
            )

            llm_phrase_relationship_screening_results = (
                await parse_relationhip_screening_batch_req_result(
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

            # Discard phrases from llm_phrase_relationship_results which have been filtered out by the screening phase
            screened_phrases_w_reason = get_verified_phrase_relationship_results(
                llm_phrase_relationship_screening_results
            )
            verified_phrases_w_og_summary = {
                k: v
                for k, v in llm_phrase_relationship_results.items()
                if k in screened_phrases_w_reason
            }

            llm_phrase_initial_grounding_request_id = (
                extraction_bundle.llm_phrase_initial_grounding_req_id
            )
            if not llm_phrase_initial_grounding_request_id:
                raise ValueError(
                    f"create_missing_phrase_initial_grounding_requests: llm_phrase_initial_grounding_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type}"
                )
            if not verified_phrases_w_og_summary:
                # add a dummy response blob with empty dict
                logger.info(
                    f"No phrases found in text, for {mfg_etld1}:{field_type}, creating dummy initial grounding request"
                )
                dummy_batch_request = _create_dummy_completed_phrase_initial_grounding_batch_request(
                    deferred_at=deferred_at,
                    etld1=mfg_etld1,
                    llm_phrase_initial_grounding_request_id=llm_phrase_initial_grounding_request_id,
                    model_params=model_params,
                    eager=eager,
                )
                new_batch_request = dummy_batch_request
            else:
                logger.info(
                    f"Passing on candidates {verified_phrases_w_og_summary} to phrase_relationship phase for {mfg_etld1}:{field_type} chunk {chunk_bounds}"
                )
                llm_phrase_grounding_batch_request = create_deferred_phrase_initial_grounding_gpt_request(
                    deferred_at=deferred_at,
                    etld1=mfg_etld1,
                    llm_phrase_initial_grounding_request_id=llm_phrase_initial_grounding_request_id,
                    phrase_initial_grounding_prompt=phrase_initial_grounding_prompt,
                    field_type=field_type,
                    # context variables
                    mfg_name=mfg_name,
                    all_concepts=known_concepts,
                    verified_phrases_w_og_summary=verified_phrases_w_og_summary,
                    # model info
                    eager=eager,
                    gpt_model=llm_model,
                    model_params=model_params,
                )
                new_batch_request = llm_phrase_grounding_batch_request

            batch_requests.append(new_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {mfg_etld1}:{field_type}"
            )

    return batch_requests


def _create_dummy_completed_phrase_initial_grounding_batch_request(
    deferred_at: datetime,
    etld1: str,
    llm_phrase_initial_grounding_request_id: GPTBatchRequestCustomID,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    if llm_phrase_initial_grounding_request_id is None:
        raise ValueError(
            "_create_dummy_completed_phrase_initial_grounding_batch_request: llm_phrase_initial_grounding_request_id is None"
        )

    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_phrase_initial_grounding_request_id,
        context="No phrase relationship screening needed - no phrases found in text.",
        prompt=Prompt(
            name="dummy_phrase_relationship_screening_prompt",
            text="No phrase relationship screening needed - no phrases found in text by brute force or by LLM.",
            s3_version_id="dummy_s3_version_id",
            num_tokens=1,
        ),
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_phrase_relationship_batch_id",
    )

    base_gpt_batch_request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=llm_phrase_initial_grounding_request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content="```json\n{}\n```"
        ),
    )

    return base_gpt_batch_request


def create_deferred_phrase_initial_grounding_gpt_request(
    deferred_at: datetime,
    etld1: str,
    llm_phrase_initial_grounding_request_id: str,
    phrase_initial_grounding_prompt: Prompt,
    field_type: ConceptTypeEnum,
    # context
    mfg_name: str,
    all_concepts: set[Concept],
    verified_phrases_w_og_summary: LLMPhraseRelationshipResults,
    # model info
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_phrase_initial_grounding_gpt_request: Generating GPTBatchRequest for {llm_phrase_initial_grounding_request_id}"
    )
    all_concept_labels = [
        label for concept in all_concepts for label in concept.matchLabels
    ]

    context = f"Manufacturer name: {mfg_name}\n\n extracted phrases:\n{list(verified_phrases_w_og_summary)}\n\noptions of {field_type.name} to choose from:\n{all_concept_labels}"

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_phrase_initial_grounding_request_id,
        context=context,
        prompt=phrase_initial_grounding_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
        batch_id="Eager" if eager else None,
    )
    return gpt_batch_request
