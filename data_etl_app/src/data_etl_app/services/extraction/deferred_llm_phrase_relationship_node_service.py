from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime
from typing import Optional

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.field_types import LLMPhraseRelationshipResults, LLMSearchResults
from core.models.prompt import Prompt
from core.models.llm_model import LLM_Model, NO_MODEL
from core.models.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from core.models.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
    DeferredLLMPhraseExtractionRequests,
)
from core.models.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
    DeferredConceptExtractionRequests,
)

from data_etl_app.models.types_and_enums import LLMExtractedFieldTypeEnum
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

from core.services.gpt_batch_request_writes import record_response_parse_error
from core.services.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)
from data_etl_app.services.extraction.deferred_llm_phrase_recursive_search_node_service import (
    get_all_recursive_round_results,
)
from data_etl_app.services.extraction.deferred_llm_phrase_search_node_service import (
    parse_batch_request_result as parse_phrase_search_batch_req_result,
    parse_llm_search_response,
)

from data_etl_app.utils.ground_truth_helper_util import (
    merge_llm_and_brute_search_results,
)

logger = logging.getLogger(__name__)


def parse_llm_phrase_relationship_result(
    gpt_response: Optional[str],
) -> LLMPhraseRelationshipResults:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_phrase_relationship_result: Empty or invalid response from GPT"
        )

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        raw_gpt_phrase_relationship_result: dict[str, str] = json.loads(gpt_response)
        logger.debug(
            f"raw_gpt_phrase_relationship_result:{json.dumps(raw_gpt_phrase_relationship_result, indent=2)}"
        )
    except json.JSONDecodeError as e:
        raise ValueError(
            f"parse_llm_phrase_relationship_result: Invalid response from GPT:{gpt_response}"
        ) from e

    if not isinstance(raw_gpt_phrase_relationship_result, dict):
        raise ValueError(
            "parse_llm_phrase_relationship_result: Expected raw_gpt_phrase_relationship_result to be a dictionary"
        )

    logger.debug(
        f"raw_gpt_phrase_relationship_result:{raw_gpt_phrase_relationship_result}"
    )

    return raw_gpt_phrase_relationship_result


async def parse_batch_request_result(
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    deferred_at: datetime,
) -> LLMPhraseRelationshipResults:
    req_id = extraction_bundle.llm_phrase_relationship_req_id
    if not req_id:
        raise ValueError(
            f"phrase_relationship_node.parse_batch_request_result: phrase_relationship_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
        )

    req_obj = completed_request_map.get(req_id)
    if not req_obj:
        raise ValueError(
            f"phrase_relationship_node.parse_batch_request_result: Missing GPTBatchRequest for phrase_relationship request ID {req_id} in {mfg_etld1}:{field_type.name}"
        )
    elif not req_obj.response:
        raise ValueError(
            f"phrase_relationship_node.parse_batch_request_result: GPTBatchRequest for phrase_relationship request ID {req_id} has no response_blob in {mfg_etld1}:{field_type.name}"
        )

    try:
        phrase_relationship_results = parse_llm_phrase_relationship_result(
            req_obj.response.result
        )
        return phrase_relationship_results
    except Exception as e:
        await record_response_parse_error(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=deferred_at,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"phrase_relationship_node.parse_batch_request_result: Error parsing phrase_relationship results for manufacturer {mfg_etld1} from GPT response: {e}"
        )
        raise


async def create_missing_phrase_relationship_requests(
    mfg_etld1: str,
    mfg_name: str,
    field_type: LLMExtractedFieldTypeEnum,  # used for logging and debugging
    extraction_requests: (
        DeferredLLMPhraseExtractionRequests | DeferredConceptExtractionRequests
    ),
    missing_phrase_relationship_req_ids: set[GPTBatchRequestCustomID],
    mfg_text: str,
    phrase_relationship_prompt: Prompt,
    llm_phrase_search_gpt_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    deferred_at: datetime,
    llm_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
    llm_phrase_recursive_search_gpt_request_map: dict[
        GPTBatchRequestCustomID, GPTBatchRequest
    ],
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_phrase_relationship_requests: Generating GPTBatchRequests for {mfg_etld1}:{field_type}"
    )

    batch_requests: list[GPTBatchRequest] = []
    chunk_items = list(
        {
            chunk_bounds: bundle
            for chunk_bounds, bundle in extraction_requests.chunked_request_map.items()
            if bundle.llm_phrase_relationship_req_id
            in missing_phrase_relationship_req_ids
        }.items()
    )
    # Create lookup map: custom_id -> GPTBatchRequest
    if not llm_phrase_search_gpt_request_map:
        raise ValueError(
            f"create_missing_phrase_relationship_requests: No completed GPTBatchRequests found for {mfg_etld1}:{field_type} in upstream_completed_batch_req_map"
        )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, extraction_bundle in batch:
            # upstream phrase search results for this chunk
            llm_phrase_search_results = await parse_phrase_search_batch_req_result(
                mfg_etld1=mfg_etld1,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                all_phrase_search_req_responses_map=llm_phrase_search_gpt_request_map,
                deferred_at=deferred_at,
            )

            llm_phrase_recursive_search_results = get_all_recursive_round_results(
                extraction_bundle, llm_phrase_recursive_search_gpt_request_map
            )

            llm_phrase_search_results |= llm_phrase_recursive_search_results

            llm_phrase_relationship_request_id = (
                extraction_bundle.llm_phrase_relationship_req_id
            )
            if not llm_phrase_relationship_request_id:
                raise ValueError(
                    f"create_missing_phrase_relationship_requests: llm_phrase_relationship_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type}"
                )

            # Combine LLM search with brute force results (concept pipelines only) to get the
            # final search results for the chunk, which are used as context for phrase_relationship.
            # We include all brute force results as phrase_relationship candidates even if
            # they weren't identified by the LLM search, because the brute force results were
            # generated with high recall in mind and we don't want to miss out on any potential
            # phrase by filtering them with the LLM search results. Keyword pipelines have no
            # brute force phase
            all_search_results = merge_llm_and_brute_search_results(
                llm_search_results=llm_phrase_search_results,
                brute_search_results=(
                    extraction_bundle.brute
                    if type(extraction_bundle) == ConceptExtractionRequestBundle
                    else set()
                ),
            )

            if not all_search_results:
                # add a dummy response blob with empty dict
                logger.info(
                    f"No phrases found in text, for {mfg_etld1}:{field_type}, creating dummy phrase_relationship request"
                )
                dummy_batch_request = _create_dummy_completed_phrase_relationship_batch_request(
                    deferred_at=deferred_at,
                    etld1=mfg_etld1,
                    llm_phrase_relationship_request_id=llm_phrase_relationship_request_id,
                    model_params=model_params,
                    eager=eager,
                )
                new_batch_request = dummy_batch_request
            else:
                start, end = int(chunk_bounds.split(":")[0]), int(
                    chunk_bounds.split(":")[1]
                )
                logger.info(
                    f"Passing on candidates {all_search_results} to phrase_relationship phase for {mfg_etld1}:{field_type} chunk {chunk_bounds}"
                )
                phrase_relationship_batch_request = create_deferred_phrase_relationship_gpt_request(
                    deferred_at=deferred_at,
                    etld1=mfg_etld1,
                    llm_phrase_relationship_request_id=llm_phrase_relationship_request_id,
                    mfg_name=mfg_name,
                    mfg_text=mfg_text[start:end],
                    search_results=all_search_results,
                    phrase_relationship_prompt=phrase_relationship_prompt,
                    eager=eager,
                    gpt_model=llm_model,
                    model_params=model_params,
                )
                new_batch_request = phrase_relationship_batch_request

            batch_requests.append(new_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {mfg_etld1}:{field_type}"
            )

    return batch_requests


def _create_dummy_completed_phrase_relationship_batch_request(
    deferred_at: datetime,
    etld1: str,
    llm_phrase_relationship_request_id: GPTBatchRequestCustomID,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    if llm_phrase_relationship_request_id is None:
        raise ValueError(
            "_create_dummy_completed_phrase_relationship_batch_request: llm_phrase_relationship_request_id is None"
        )

    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_phrase_relationship_request_id,
        context="No phrase_relationship needed - no phrases found in text.",
        prompt=Prompt(
            name="dummy_phrase_relationship_prompt",
            text="No phrase relationships needed - no phrases found in text brute force or by LLM.",
            s3_version_id="dummy_s3_version_id",
            num_tokens=1,
        ),
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_phrase_relationship_batch_id",
    )

    base_gpt_batch_request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=llm_phrase_relationship_request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content="```json\n{}\n```"
        ),
    )

    return base_gpt_batch_request


def create_deferred_phrase_relationship_gpt_request(
    deferred_at: datetime,
    etld1: str,
    llm_phrase_relationship_request_id: str,
    mfg_name: str,
    mfg_text: str,
    search_results: LLMSearchResults,
    phrase_relationship_prompt: Prompt,
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_phrase_relationship_gpt_request: Generating GPTBatchRequest for {llm_phrase_relationship_request_id}"
    )
    context = f"manufacturer name: {mfg_name}\n\nscraped text:\n{mfg_text} \n\n extracted phrases:\n{list(search_results)}"

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_phrase_relationship_request_id,
        context=context,
        prompt=phrase_relationship_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
        batch_id="Eager" if eager else None,
    )
    return gpt_batch_request
