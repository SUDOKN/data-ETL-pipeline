import asyncio
import json
import logging
from datetime import datetime

from core.models.deferred_search_requests import DeferredSearchRequests
from core.models.prompt import Prompt
from core.models.db.gpt_batch_request import GPTBatchRequest
from data_etl_app.models.types_and_enums import (
    LLMExtractedFieldTypeEnum,
)


from core.services.gpt_batch_request_service import create_base_gpt_batch_request
from core.utils.str_util import make_json_array_parse_safe

logger = logging.getLogger(__name__)

from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from open_ai_key_app.models.gpt_model import (
    LLM_Model,
    ModelParameters,
    DefaultModelParameters,
)


def parse_llm_search_response(gpt_response: str) -> set[str]:
    if not gpt_response:
        logger.error(
            f"parse_llm_search_response: Invalid gpt_response:{gpt_response}, returning empty set"
        )
        return set()

    try:
        cleaned_response = make_json_array_parse_safe(gpt_response)
    except Exception as e:
        logger.error(
            (
                f"parse_llm_search_response: Failed to make_json_parse_safe GPT response: {e}\n",
                f"cleaned_response={gpt_response}, returning empty set",
            ),
            exc_info=True,
        )
        return set()

    try:
        llm_results: set[str] = set(json.loads(cleaned_response))
    except Exception as e:
        logger.error(
            (
                f"parse_llm_search_response: Failed to json.loads(cleaned_response): {e}\n",
                f"gpt_response={gpt_response}\n"
                f"cleaned_response={cleaned_response}, returning empty set",
            ),
            exc_info=True,
        )
        return set()

    logger.debug(f"llm_results:{llm_results}")

    return llm_results


async def create_missing_search_requests(
    deferred_at: datetime,
    field_type: LLMExtractedFieldTypeEnum,  # used for logging and debugging
    missing_search_req_ids: set[GPTBatchRequestCustomID],
    extraction_requests: DeferredSearchRequests,
    mfg_etld1: str,
    mfg_text: str,
    search_prompt: Prompt,
    llm_model: LLM_Model,
    model_params: ModelParameters = DefaultModelParameters,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:

    logger.info(
        f"create_missing_search_requests: Generating GPTBatchRequest for {mfg_etld1}:{field_type.name}"
    )

    batch_requests: list[GPTBatchRequest] = []
    chunk_items: list[tuple[GPTBatchRequestCustomID, str]] = []
    for (
        chunk_bounds,
        extraction_bundle,
    ) in extraction_requests.request_map.items():
        if extraction_bundle.llm_search_request_id in missing_search_req_ids:
            start = chunk_bounds.split(":")[0]
            end = chunk_bounds.split(":")[1]
            chunk_items.append(
                (
                    extraction_bundle.llm_search_request_id,
                    mfg_text[int(start) : int(end)],
                )
            )

    # Process chunks in batches to yield control periodically

    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for llm_search_request_id, chunk_text in batch:
            llm_batch_request = create_base_gpt_batch_request(
                deferred_at=deferred_at,
                custom_id=llm_search_request_id,
                context=chunk_text,
                prompt=search_prompt,
                gpt_model=llm_model,
                model_params=model_params,
            )

            batch_requests.append(llm_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {mfg_etld1}:{field_type}"
            )

    return batch_requests
