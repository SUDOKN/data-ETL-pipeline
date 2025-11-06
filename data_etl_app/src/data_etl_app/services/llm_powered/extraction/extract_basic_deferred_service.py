from datetime import datetime
import logging
from typing import Optional

from core.models.deferred_basic_extraction import DeferredBasicExtraction
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.services.gpt_batch_request_service import (
    create_gpt_batch_request,
)

from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)

from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)


async def find_addresses_from_first_chunk_deferred(
    deferred_at: datetime,
    deferred_address_extraction: Optional[DeferredBasicExtraction],
    mfg_etld1: str,
    mfg_text: str,
    keyword_label="addresses",
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> tuple[DeferredBasicExtraction, GPTBatchRequest]:
    prompt_service = await get_prompt_service()
    extract_address_prompt = prompt_service.extract_any_address

    if not deferred_address_extraction:
        chunks_map = await get_chunks_respecting_line_boundaries(
            mfg_text,
            gpt_model.max_context_tokens
            - extract_address_prompt.num_tokens
            - 5000,  # subtracting 5000 to leave room for last line in each chunk, otherwise _binary_classify_chunk gets > GPT_4o_mini.max_context_tokens
            max_chunks=1,  # Only generate 1 chunk
        )

        chunk = list(chunks_map.items())[0]
        custom_id = f"{mfg_etld1}>{keyword_label}>chunk>{chunk[0]}"
        deferred_address_extraction = DeferredBasicExtraction(
            gpt_request_id=custom_id,
            prompt_version_id=extract_address_prompt.s3_version_id,
        )
    else:
        # it will be assumed that GPTBatchRequest needs to be created again
        # AddressExtractionPhase is_deferred_mfg_missing_any_requests method needs to contain logic to verify the fact above
        if (
            deferred_address_extraction.prompt_version_id
            != extract_address_prompt.s3_version_id
        ):
            raise ValueError(
                f"find_addresses_from_first_chunk_deferred: Prompt version mismatch in deferred_address_extraction,"
                f" expected {extract_address_prompt.s3_version_id}, found {deferred_address_extraction.prompt_version_id}"
            )

        chunk_bounds = deferred_address_extraction.gpt_request_id.split(">chunk>")[1]
        start = chunk_bounds.split(":")[0]
        end = chunk_bounds.split(":")[1]
        chunk = (chunk_bounds, mfg_text[int(start) : int(end)])

    gpt_batch_request = create_gpt_batch_request(
        deferred_at=deferred_at,
        custom_id=deferred_address_extraction.gpt_request_id,
        text=chunk[1],
        prompt=extract_address_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )
    return deferred_address_extraction, gpt_batch_request


async def find_business_desc_using_only_first_chunk_deferred(
    deferred_at: datetime,
    deferred_business_desc_extraction: Optional[DeferredBasicExtraction],
    mfg_etld1: str,
    mfg_text: str,
    keyword_label="business_desc",
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> tuple[DeferredBasicExtraction, GPTBatchRequest]:
    logger.info(f"Finding business desc for {mfg_etld1} using only first chunk...")
    prompt_service = await get_prompt_service()
    find_business_desc_prompt = prompt_service.find_business_desc_prompt

    if not deferred_business_desc_extraction:
        chunks_map = await get_chunks_respecting_line_boundaries(
            mfg_text,
            gpt_model.max_context_tokens
            - find_business_desc_prompt.num_tokens
            - 5000,  # subtracting 5000 to leave room for last line in each chunk, otherwise _binary_classify_chunk gets > GPT_4o_mini.max_context_tokens
            max_chunks=1,  # Only generate 1 chunk
        )

        chunk = list(chunks_map.items())[0]
        custom_id = f"{mfg_etld1}>{keyword_label}>chunk>{chunk[0]}"
        deferred_business_desc_extraction = DeferredBasicExtraction(
            gpt_request_id=custom_id,
            prompt_version_id=find_business_desc_prompt.s3_version_id,
        )
    else:
        # User must ensure this extraction is really needed
        if (
            deferred_business_desc_extraction.prompt_version_id
            != find_business_desc_prompt.s3_version_id
        ):
            raise ValueError(
                f"find_business_desc_using_only_first_chunk_deferred: Prompt version mismatch in deferred_business_desc_extraction,"
            )
        chunk_bounds = deferred_business_desc_extraction.gpt_request_id.split(
            ">chunk>"
        )[1]
        start = chunk_bounds.split(":")[0]
        end = chunk_bounds.split(":")[1]
        chunk = (chunk_bounds, mfg_text[int(start) : int(end)])

    gpt_batch_request = create_gpt_batch_request(
        deferred_at=deferred_at,
        custom_id=deferred_business_desc_extraction.gpt_request_id,
        text=chunk[1],
        prompt=find_business_desc_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )

    return deferred_business_desc_extraction, gpt_batch_request
