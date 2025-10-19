from datetime import datetime
import asyncio
import json
import logging

from core.models.db.manufacturer import Address
from core.models.prompt import Prompt

from open_ai_key_app.utils.ask_gpt_util import (
    num_tokens_from_string,
    ask_gpt_async,
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


async def extract_address_from_n_chunks(
    extraction_timestamp: datetime,
    mfg_etld1: str,
    mfg_text: str,
    n: int = 1,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> list[Address]:
    answer = []
    prompt_service = await get_prompt_service()
    extract_address_prompt = prompt_service.extract_any_address

    chunks_map = get_chunks_respecting_line_boundaries(
        mfg_text,
        gpt_model.max_context_tokens
        - extract_address_prompt.num_tokens
        - 5000,  # subtracting 5000 to leave room for last line in each chunk, otherwise _binary_classify_chunk gets > GPT_4o_mini.max_context_tokens
    )
    tasks = []

    for i, (_bounds, chunk_text) in enumerate(chunks_map.items()):
        if i >= n:
            break
        tasks.append(
            _extract_address_from_chunk(
                extraction_timestamp=extraction_timestamp,
                keyword_label="extract_address",
                mfg_etld1=mfg_etld1,
                chunk_text=chunk_text,
                extract_prompt=extract_address_prompt,
                gpt_model=gpt_model,
                model_params=model_params,
            )
        )
    answers: list[list[Address]] = await asyncio.gather(*tasks)
    for chunk_addresses in answers:
        answer.extend(chunk_addresses)

    return answer


async def _extract_address_from_chunk(
    extraction_timestamp: datetime,
    keyword_label: str,
    mfg_etld1: str,
    chunk_text: str,
    extract_prompt: Prompt,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> list[Address]:
    chunk_tokens = num_tokens_from_string(chunk_text)
    logger.debug(
        f"extraction_timestamp: {extraction_timestamp}, chunk_tokens: {chunk_tokens}"
    )

    gpt_response = await ask_gpt_async(
        chunk_text, extract_prompt.text, gpt_model, model_params
    )

    if not gpt_response:
        logger.error(
            f"extraction_timestamp: {extraction_timestamp}, Invalid gpt_response:{gpt_response}"
        )
        raise ValueError(
            f"{mfg_etld1}:{keyword_label} _extract_address_from_chunk: Empty or invalid response from GPT"
        )

    logger.debug(
        f"extraction_timestamp: {extraction_timestamp}, classification gpt response:\n{gpt_response}"
    )

    gpt_response = gpt_response.replace("```", "").replace("json", "")
    result = json.loads(gpt_response)
    if result is None:
        raise ValueError(
            f"extraction_timestamp: {extraction_timestamp}, {mfg_etld1}:{keyword_label} _extract_address_from_chunk: Empty json result found in gpt_response"
        )
    elif not isinstance(result, list):
        logger.info(
            f"extraction_timestamp: {extraction_timestamp}, {mfg_etld1}:{keyword_label} extracted non-list addresses {result} from chunk"
        )

    return [Address(**addr) for addr in result]
