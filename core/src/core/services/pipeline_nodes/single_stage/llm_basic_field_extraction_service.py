from datetime import datetime
import asyncio
import logging

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestMap,
)
from core.models.file_objects.prompt import Prompt

from core.services.gpt_batch_request_service import create_base_gpt_batch_request
from core.models.types_and_enums import (
    BasicFieldTypeEnum,
    BinaryClassificationTypeEnum,
)
from core.models.extraction_schemas.basic_fields import (
    AddressExtractionResponse,
    BinaryClassificationResponse,
    BusinessDescResponse,
)
from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from core.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams

logger = logging.getLogger(__name__)


# Each single-stage node owns its output schema; response_format is overridden
# per field_type via GPTModelParams.with_response_format() below.
ADDRESS_RESPONSE_SCHEMA = build_gpt_response_format(
    AddressExtractionResponse, name="address_extraction"
)

BUSINESS_DESC_RESPONSE_SCHEMA = build_gpt_response_format(
    BusinessDescResponse, name="business_desc_extraction"
)

BINARY_CLASSIFICATION_RESPONSE_SCHEMA = build_gpt_response_format(
    BinaryClassificationResponse, name="binary_classification"
)


def _response_schema_for_field_type(
    field_type: "BasicFieldTypeEnum | BinaryClassificationTypeEnum",
) -> dict:
    if field_type == BasicFieldTypeEnum.addresses:
        return ADDRESS_RESPONSE_SCHEMA
    if field_type == BasicFieldTypeEnum.business_desc:
        return BUSINESS_DESC_RESPONSE_SCHEMA
    return BINARY_CLASSIFICATION_RESPONSE_SCHEMA


async def create_missing_basic_extraction_requests(
    deferred_at: datetime,
    field_type: "BasicFieldTypeEnum | BinaryClassificationTypeEnum",  # used for logging and debugging
    missing_request_ids: set[GPTBatchRequestCustomID],
    chunked_request_map: SingleStageExtractionRequestMap,
    mfg_etld1: str,
    mfg_text: str,
    prompt: Prompt,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:

    logger.info(
        f"create_missing_basic_extraction_requests: Generating GPTBatchRequest for {mfg_etld1}:{field_type.name}"
    )

    request_model_params = model_params.with_response_format(
        _response_schema_for_field_type(field_type)
    )

    batch_requests: list[GPTBatchRequest] = []
    chunk_items: list[tuple[GPTBatchRequestCustomID, str]] = []
    for (
        chunk_bounds,
        extraction_bundle,
    ) in chunked_request_map.items():
        if extraction_bundle.llm_request_id in missing_request_ids:
            start = chunk_bounds.split(":")[0]
            end = chunk_bounds.split(":")[1]
            chunk_items.append(
                (
                    extraction_bundle.llm_request_id,
                    mfg_text[int(start) : int(end)],
                )
            )

    # Process chunks in batches to yield control periodically

    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for llm_request_id, chunk_text in batch:
            llm_batch_request = create_base_gpt_batch_request(
                deferred_at=deferred_at,
                etld1=mfg_etld1,
                custom_id=llm_request_id,
                context=chunk_text,
                prompt_text=prompt.text,
                gpt_model=llm_model,
                model_params=request_model_params,
                batch_id="Eager" if eager else None,
            )

            batch_requests.append(llm_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {mfg_etld1}:{field_type.name} (Eager: {eager})"
            )

    return batch_requests
