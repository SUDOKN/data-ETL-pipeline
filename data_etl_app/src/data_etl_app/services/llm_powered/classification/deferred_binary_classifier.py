from datetime import datetime
import logging

from core.models.prompt import Prompt

from open_ai_key_app.utils.token_util import num_tokens_from_string
from open_ai_key_app.utils.batch_gpt_util import (
    get_gpt_request_blob,
)
from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)
from open_ai_key_app.models.gpt_batch_request import GPTBatchRequest

from data_etl_app.models.deferred_binary_classification import (
    DeferredBinaryClassification,
    DeferredBinaryClassificationStats,
)
from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)


async def is_company_a_manufacturer_deferred(
    deferred_at: datetime,
    manufacturer_etld: str,
    mfg_txt: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> DeferredBinaryClassification:
    logger.info(
        f"is_company_a_manufacturer_deferred: Generating for {manufacturer_etld}"
    )
    prompt_service = await get_prompt_service()
    return _binary_classify_using_only_first_chunk_deferred(
        deferred_at=deferred_at,
        keyword_label="is_manufacturer",
        manufacturer_etld=manufacturer_etld,
        mfg_txt=mfg_txt,
        binary_prompt=prompt_service.is_manufacturer_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )


async def is_contract_manufacturer_deferred(
    deferred_at: datetime,
    manufacturer_etld: str,
    mfg_txt: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> DeferredBinaryClassification:
    logger.info(
        f"is_contract_manufacturer_deferred: Generating for {manufacturer_etld}"
    )
    prompt_service = await get_prompt_service()
    return _binary_classify_using_only_first_chunk_deferred(
        deferred_at=deferred_at,
        keyword_label="is_contract_manufacturer",
        manufacturer_etld=manufacturer_etld,
        mfg_txt=mfg_txt,
        binary_prompt=prompt_service.is_contract_manufacturer_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )


async def is_product_manufacturer_deferred(
    deferred_at: datetime,
    manufacturer_etld: str,
    mfg_txt: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> DeferredBinaryClassification:
    logger.info(f"is_product_manufacturer_deferred: Generating for {manufacturer_etld}")
    prompt_service = await get_prompt_service()
    return _binary_classify_using_only_first_chunk_deferred(
        deferred_at=deferred_at,
        keyword_label="is_product_manufacturer",
        manufacturer_etld=manufacturer_etld,
        mfg_txt=mfg_txt,
        binary_prompt=prompt_service.is_product_manufacturer_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )


def _binary_classify_using_only_first_chunk_deferred(
    deferred_at: datetime,
    keyword_label: str,
    manufacturer_etld: str,
    mfg_txt: str,
    binary_prompt: Prompt,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> DeferredBinaryClassification:
    chunks_map = get_chunks_respecting_line_boundaries(
        mfg_txt,
        gpt_model.max_context_tokens
        - binary_prompt.num_tokens
        - 5000,  # subtracting 5000 to leave room for last line in each chunk, otherwise _binary_classify_chunk gets > GPT_4o_mini.max_context_tokens
    )
    first_chunk_bounds = min(chunks_map.keys(), key=lambda k: int(k.split(":")[0]))
    first_chunk_text = chunks_map[first_chunk_bounds]
    logger.info(
        f"Using first chunk with key {first_chunk_bounds} for deferred binary classification with num_tokens {num_tokens_from_string(first_chunk_text)}."
    )
    chunk_batch_request_map = {
        first_chunk_bounds: _binary_classify_chunk_deferred(
            deferred_at=deferred_at,
            custom_id=f"{manufacturer_etld}>{keyword_label}>chunk{first_chunk_bounds}",
            chunk_txt=first_chunk_text,
            binary_prompt=binary_prompt,
            gpt_model=gpt_model,
            model_params=model_params,
        )
    }
    return DeferredBinaryClassification(
        deferred_stats=DeferredBinaryClassificationStats(
            prompt_version_id=binary_prompt.s3_version_id,
            final_chunk_key=first_chunk_bounds,
            chunk_batch_request_map=chunk_batch_request_map,
        )
    )


def _binary_classify_chunk_deferred(
    deferred_at: datetime,
    custom_id: str,
    chunk_txt: str,
    binary_prompt: Prompt,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> GPTBatchRequest:
    logger.info(
        f"_binary_classify_chunk_deferred: Generating GPTBatchRequest for {custom_id}"
    )

    return GPTBatchRequest(
        batch_id=None,
        request=get_gpt_request_blob(
            created_at=deferred_at,
            custom_id=custom_id,
            context=chunk_txt,
            prompt=binary_prompt.text,
            gpt_model=gpt_model,
            model_params=model_params,
        ),
        request_sent_at=None,
        response_blob=None,
        response_received_at=None,
    )
