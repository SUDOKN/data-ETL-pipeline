import asyncio
import logging
import traceback
from datetime import datetime
from typing import Optional

from pydantic import ValidationError

from packages.llm_providers.src.llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from packages.core.src.core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)
from packages.core.src.core.models.extraction_schemas.search import (
    LLMSearchResults,
    PhraseSearchResponse,
)
from packages.llm_providers.src.llm_providers.models.file_objects.prompt import Prompt
from packages.llm_providers.src.llm_providers.models.llm_model import LLM_Model
from packages.core.src.core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestMap,
    LLMPhraseExtractionRequestBundle,
)
from packages.core.src.core.models.types_and_enums import (
    LLMExtractedFieldTypeEnum,
)
from packages.llm_providers.src.llm_providers.field_types import BatchRequestIDType
from packages.llm_providers.src.llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)

from packages.llm_providers.src.llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error,
)
from packages.llm_providers.src.llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
)

logger = logging.getLogger(__name__)


# Shared by both llm_search and llm_phrase_recursive_search (identical shape).
LLM_SEARCH_RESPONSE_SCHEMA = build_gpt_response_format(
    PhraseSearchResponse, name="phrase_search_result"
)


def parse_llm_search_response(gpt_response: Optional[str]) -> LLMSearchResults:
    if not gpt_response:
        raise ValueError(
            "parse_llm_search_response: Empty or invalid response from GPT"
        )

    try:
        parsed = PhraseSearchResponse.model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_llm_search_response: Invalid response from GPT:{gpt_response}"
        ) from e

    llm_results: set[str] = set(parsed.phrases)
    logger.debug(f"llm_results:{llm_results}")

    return llm_results


async def parse_batch_request_result(
    subject_unique_id: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    all_phrase_search_req_responses_map: dict[  # contains requests of this phase(which can be identified by its particular custom id) across all chunks
        BatchRequestIDType, GPTBatchRequest
    ],
    deferred_at: datetime,
) -> LLMSearchResults:
    llm_phrase_search_request_id = extraction_bundle.llm_phrase_search_req_id
    if not llm_phrase_search_request_id:
        raise ValueError(
            f"search_node.parse_batch_request_result: llm_phrase_search_request_id is None for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )

    llm_search_req = all_phrase_search_req_responses_map.get(
        llm_phrase_search_request_id
    )
    if not llm_search_req:
        raise ValueError(
            f"search_node.parse_batch_request_result: Missing GPTBatchRequest for search request ID {llm_phrase_search_request_id} in {subject_unique_id}:{field_type.name}"
        )
    elif not llm_search_req.response:
        raise ValueError(
            f"search_node.parse_batch_request_result: GPTBatchRequest for search request ID {llm_phrase_search_request_id} has no response_blob in {subject_unique_id}:{field_type.name}"
        )

    try:
        llm_search_results = parse_llm_search_response(llm_search_req.response.result)
        return llm_search_results
    except Exception as e:
        await record_response_parse_error(
            gpt_batch_request=llm_search_req,
            error_message=str(e),
            timestamp=deferred_at,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"search_node.parse_batch_request_result: Error parsing concept search results for manufacturer {subject_unique_id} from GPT response: {e}"
        )
        raise


async def create_missing_phrase_search_requests(
    deferred_at: datetime,
    field_type: LLMExtractedFieldTypeEnum,  # used for logging and debugging
    missing_search_req_ids: set[BatchRequestIDType],
    chunked_request_map: LLMPhraseExtractionRequestMap,
    subject_unique_id: str,
    mfg_text: str,
    search_prompt: Prompt,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:

    logger.info(
        f"create_missing_phrase_search_requests: Generating GPTBatchRequest for {subject_unique_id}:{field_type.name}"
    )

    batch_requests: list[GPTBatchRequest] = []
    chunk_items: list[tuple[BatchRequestIDType, str]] = []
    for (
        chunk_bounds,
        extraction_bundle,
    ) in chunked_request_map.items():
        if extraction_bundle.llm_phrase_search_req_id in missing_search_req_ids:
            start = chunk_bounds.split(":")[0]
            end = chunk_bounds.split(":")[1]
            chunk_items.append(
                (
                    extraction_bundle.llm_phrase_search_req_id,
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
                subject_unique_id=subject_unique_id,
                custom_id=llm_search_request_id,
                context=chunk_text,
                prompt_text=search_prompt.text,
                gpt_model=llm_model,
                model_params=model_params.with_response_format(
                    LLM_SEARCH_RESPONSE_SCHEMA
                ),
                batch_id="Eager" if eager else None,
            )

            batch_requests.append(llm_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {subject_unique_id}:{field_type} (Eager: {eager})"
            )

    return batch_requests
