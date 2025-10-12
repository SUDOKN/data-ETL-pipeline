from datetime import datetime
import asyncio
import json
import logging

from core.models.db.manufacturer import Address
from core.models.prompt import Prompt

from data_etl_app.services.llm_powered.extraction.extract_address_service import (
    _extract_address_from_chunk,
)
from open_ai_key_app.models.gpt_batch_request import GPTBatchRequest
from open_ai_key_app.utils.batch_gpt_util import get_gpt_request_blob
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

from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)


async def extract_address_from_n_chunks_deferred(
    deferred_at: datetime,
    mfg_etld1: str,
    mfg_text: str,
    keyword_label="addresses",
    n: int = 1,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> list[GPTBatchRequest]:
    prompt_service = await get_prompt_service()
    extract_address_prompt = prompt_service.extract_any_address

    chunks_map = get_chunks_respecting_line_boundaries(
        mfg_text,
        gpt_model.max_context_tokens
        - extract_address_prompt.num_tokens
        - 5000,  # subtracting 5000 to leave room for last line in each chunk, otherwise _binary_classify_chunk gets > GPT_4o_mini.max_context_tokens
    )

    gpt_batch_requests = []
    for i, (bounds, chunk_text) in enumerate(chunks_map.items()):
        if i >= n:
            break
        gpt_batch_requests.append(
            _extract_address_from_chunk_deferred(
                deferred_at=deferred_at,
                custom_id=f"{mfg_etld1}>{keyword_label}>chunk{bounds}",
                chunk_text=chunk_text,
                extract_prompt=extract_address_prompt,
                gpt_model=gpt_model,
                model_params=model_params,
            )
        )

    return gpt_batch_requests


def _extract_address_from_chunk_deferred(
    deferred_at: datetime,
    custom_id: str,
    chunk_text: str,
    extract_prompt: Prompt,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> GPTBatchRequest:
    logger.info(
        f"_extract_address_from_chunk_deferred: Generating GPTBatchRequest for {custom_id}"
    )

    return GPTBatchRequest(
        batch_id=None,
        request=get_gpt_request_blob(
            created_at=deferred_at,
            custom_id=custom_id,
            context=chunk_text,
            prompt=extract_prompt.text,
            gpt_model=gpt_model,
            model_params=model_params,
        ),
    )
