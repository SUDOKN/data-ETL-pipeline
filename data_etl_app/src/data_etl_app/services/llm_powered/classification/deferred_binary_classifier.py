from datetime import datetime
import logging
from typing import Optional

from core.models.prompt import Prompt

from core.services.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    create_base_gpt_batch_request,
)
from core.models.db.gpt_batch_request import GPTBatchRequest
from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)

from data_etl_app.models.types_and_enums import BinaryClassificationTypeEnum
from core.models.deferred_binary_classification import (
    DeferredBinaryClassification,
)
from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)


async def binary_classify_deferred(
    deferred_at: datetime,
    deferred_binary_classification: Optional[DeferredBinaryClassification],
    mfg_etld1: str,
    mfg_text: str,
    classification_type: BinaryClassificationTypeEnum,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> tuple[DeferredBinaryClassification, GPTBatchRequest]:
    binary_classification_functions = {
        "is_manufacturer": is_company_a_manufacturer_deferred,
        "is_contract_manufacturer": is_contract_manufacturer_deferred,
        "is_product_manufacturer": is_product_manufacturer_deferred,
    }

    if classification_type.name not in binary_classification_functions:
        raise ValueError(f"Unsupported classification type: {classification_type.name}")

    return await binary_classification_functions[classification_type.name](
        deferred_at=deferred_at,
        deferred_binary_classification=deferred_binary_classification,
        mfg_etld1=mfg_etld1,
        mfg_text=mfg_text,
        gpt_model=gpt_model,
        model_params=model_params,
    )


async def is_company_a_manufacturer_deferred(
    deferred_at: datetime,
    deferred_binary_classification: Optional[DeferredBinaryClassification],
    mfg_etld1: str,
    mfg_text: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> tuple[DeferredBinaryClassification, GPTBatchRequest]:
    logger.info(f"is_company_a_manufacturer_deferred: Generating for {mfg_etld1}")
    prompt_service = await get_prompt_service()

    return await _binary_classify_using_only_first_chunk_deferred(
        deferred_at=deferred_at,
        deferred_binary_classification=deferred_binary_classification,
        mfg_etld1=mfg_etld1,
        keyword_label="is_manufacturer",
        mfg_text=mfg_text,
        binary_prompt=prompt_service.is_manufacturer_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )


async def is_contract_manufacturer_deferred(
    deferred_at: datetime,
    deferred_binary_classification: Optional[DeferredBinaryClassification],
    mfg_etld1: str,
    mfg_text: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> tuple[DeferredBinaryClassification, GPTBatchRequest]:
    logger.info(f"is_contract_manufacturer_deferred: Generating for {mfg_etld1}")
    prompt_service = await get_prompt_service()

    return await _binary_classify_using_only_first_chunk_deferred(
        deferred_at=deferred_at,
        deferred_binary_classification=deferred_binary_classification,
        mfg_etld1=mfg_etld1,
        keyword_label="is_contract_manufacturer",
        mfg_text=mfg_text,
        binary_prompt=prompt_service.is_contract_manufacturer_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )


async def is_product_manufacturer_deferred(
    deferred_at: datetime,
    deferred_binary_classification: Optional[DeferredBinaryClassification],
    mfg_etld1: str,
    mfg_text: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> tuple[DeferredBinaryClassification, GPTBatchRequest]:
    logger.info(f"is_product_manufacturer_deferred: Generating for {mfg_etld1}")
    prompt_service = await get_prompt_service()

    return await _binary_classify_using_only_first_chunk_deferred(
        deferred_at=deferred_at,
        deferred_binary_classification=deferred_binary_classification,
        mfg_etld1=mfg_etld1,
        keyword_label="is_product_manufacturer",
        mfg_text=mfg_text,
        binary_prompt=prompt_service.is_product_manufacturer_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )


async def _binary_classify_using_only_first_chunk_deferred(
    deferred_at: datetime,
    deferred_binary_classification: Optional[DeferredBinaryClassification],
    mfg_etld1: str,
    mfg_text: str,
    keyword_label: str,
    binary_prompt: Prompt,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> tuple[DeferredBinaryClassification, GPTBatchRequest]:

    if not deferred_binary_classification:
        chunks_map = await get_chunks_respecting_line_boundaries(
            text=mfg_text,
            soft_limit_tokens=(
                gpt_model.max_context_tokens - binary_prompt.num_tokens - 10_000
            ),  # subtracting 10000 to leave room for last line in each chunk, otherwise _binary_classify_chunk gets > GPT_4o_mini.max_context_tokens
            overlap_ratio=0,
            max_chunks=1,  # Only generate the first chunk
        )
        chunk = list(chunks_map.items())[0]
        custom_id = f"{mfg_etld1}>{keyword_label}>chunk>{chunk[0]}"
        deferred_binary_classification = DeferredBinaryClassification(
            prompt_version_id=binary_prompt.s3_version_id,
            final_chunk_key=chunk[0],
            chunk_request_id_map={chunk[0]: custom_id},
        )
    else:
        # it will be assumed that GPTBatchRequest needs to be created again
        # BinaryClassificationPhase is_deferred_mfg_missing_any_requests method needs to contain logic to verify the fact above
        if (
            deferred_binary_classification.prompt_version_id
            != binary_prompt.s3_version_id
        ):
            raise ValueError(
                f"_binary_classify_using_only_first_chunk_deferred: Prompt version mismatch in deferred_binary_classification,"
                f" expected {binary_prompt.s3_version_id}, found {deferred_binary_classification.prompt_version_id}"
            )

        chunk_bounds = deferred_binary_classification.final_chunk_key
        start = chunk_bounds.split(":")[0]
        end = chunk_bounds.split(":")[1]
        chunk = (chunk_bounds, mfg_text[int(start) : int(end)])

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        custom_id=deferred_binary_classification.chunk_request_id_map[
            deferred_binary_classification.final_chunk_key
        ],
        context=chunk[1],
        prompt=binary_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )
    return deferred_binary_classification, gpt_batch_request
