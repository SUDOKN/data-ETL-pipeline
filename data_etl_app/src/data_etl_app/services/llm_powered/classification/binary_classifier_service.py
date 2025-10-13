from datetime import datetime
import json
import logging

from open_ai_key_app.utils.token_util import num_tokens_from_string
from open_ai_key_app.utils.ask_gpt_util import (
    ask_gpt_async,
)
from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)

from data_etl_app.models.binary_classification_result import (
    BinaryClassificationResult,
    BinaryClassificationStats,
    ChunkBinaryClassificationResult,
)
from core.models.prompt import Prompt

from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)


async def is_product_manufacturer(
    evaluated_at: datetime,
    manufacturer_etld: str,
    mfg_txt: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> BinaryClassificationResult:
    logger.debug(f"Checking if {manufacturer_etld} is a product manufacturer...")
    prompt_service = await get_prompt_service()
    return await _binary_classify_using_only_first_chunk(
        evaluated_at=evaluated_at,
        keyword_label="is_product_manufacturer",
        manufacturer_etld=manufacturer_etld,
        mfg_txt=mfg_txt,
        binary_prompt=prompt_service.is_product_manufacturer_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )


async def is_contract_manufacturer(
    evaluated_at: datetime,
    manufacturer_etld: str,
    mfg_txt: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> BinaryClassificationResult:
    logger.debug(f"Checking if {manufacturer_etld} is a contract manufacturer...")
    prompt_service = await get_prompt_service()
    return await _binary_classify_using_only_first_chunk(
        evaluated_at=evaluated_at,
        keyword_label="is_contract_manufacturer",
        manufacturer_etld=manufacturer_etld,
        mfg_txt=mfg_txt,
        binary_prompt=prompt_service.is_contract_manufacturer_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )


async def is_company_a_manufacturer(
    evaluated_at: datetime,
    manufacturer_etld: str,
    mfg_txt: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> BinaryClassificationResult:
    logger.debug(f"Checking if {manufacturer_etld} is a manufacturer...")
    prompt_service = await get_prompt_service()
    return await _binary_classify_using_only_first_chunk(
        evaluated_at=evaluated_at,
        keyword_label="is_manufacturer",
        manufacturer_etld=manufacturer_etld,
        mfg_txt=mfg_txt,
        binary_prompt=prompt_service.is_manufacturer_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )


async def _binary_classify_using_only_first_chunk(
    evaluated_at: datetime,
    keyword_label: str,
    manufacturer_etld: str,
    mfg_txt: str,
    binary_prompt: Prompt,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> BinaryClassificationResult:
    chunks_map = await get_chunks_respecting_line_boundaries(
        mfg_txt,
        gpt_model.max_context_tokens
        - binary_prompt.num_tokens
        - 5000,  # subtracting 5000 to leave room for last line in each chunk, otherwise _binary_classify_chunk gets > GPT_4o_mini.max_context_tokens
    )
    first_chunk_key = min(chunks_map.keys(), key=lambda k: int(k.split(":")[0]))
    first_chunk_text = chunks_map[first_chunk_key]
    logger.info(
        f"Using first chunk with key {first_chunk_key} for binary classification with num_tokens {num_tokens_from_string(first_chunk_text)}."
    )

    chunk_result = await _binary_classify_chunk(
        keyword_label,
        manufacturer_etld,
        first_chunk_text,
        binary_prompt.text,
        gpt_model,
        model_params,
    )
    chunk_result_map = {first_chunk_key: chunk_result}

    return BinaryClassificationResult(
        evaluated_at=evaluated_at,
        answer=chunk_result.answer,
        confidence=chunk_result.confidence,
        reason=chunk_result.reason,
        stats=BinaryClassificationStats(
            prompt_version_id=binary_prompt.s3_version_id,
            final_chunk_key=first_chunk_key,
            chunk_result_map=chunk_result_map,
        ),
    )


async def _binary_classify_chunk(
    keyword_label: str,
    manufacturer_etld: str,
    chunk_txt: str,
    binary_prompt: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> ChunkBinaryClassificationResult:
    chunk_tokens = num_tokens_from_string(chunk_txt)
    logger.debug(f"chunk_tokens: {chunk_tokens}")

    gpt_response = await ask_gpt_async(
        chunk_txt, binary_prompt, gpt_model, model_params
    )

    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            f"{manufacturer_etld}:{keyword_label} _binary_classify_chunk: Empty or invalid response from GPT"
        )

    logger.debug(f"classification gpt response:\n{gpt_response}")

    gpt_response = gpt_response.replace("```", "").replace("json", "")
    result = json.loads(gpt_response)
    if not result:
        raise ValueError(
            f"{manufacturer_etld}:{keyword_label} _binary_classify_chunk: Empty json result found in gpt_response"
        )

    return ChunkBinaryClassificationResult(**result)
