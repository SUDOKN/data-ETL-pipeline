from datetime import datetime
import json
import logging

from core.models.prompt import Prompt
from core.models.db.manufacturer import BusinessDescriptionResult

from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

from open_ai_key_app.models.gpt_batch_request import GPTBatchRequest
from open_ai_key_app.models.gpt_model import (
    DefaultModelParameters,
    GPT_4o_mini,
    GPTModel,
    ModelParameters,
)
from open_ai_key_app.utils.ask_gpt_util import (
    ask_gpt_async,
)
from open_ai_key_app.utils.batch_gpt_util import get_gpt_request_blob

logger = logging.getLogger(__name__)


async def find_business_desc_using_only_first_chunk_deferred(
    deferred_at: datetime,
    mfg_etld1: str,
    mfg_text: str,
    keyword_label="business_desc",
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> GPTBatchRequest:
    logger.info(f"Finding business desc for {mfg_etld1} using only first chunk...")
    prompt_service = await get_prompt_service()
    prompt = prompt_service.find_business_desc_prompt
    chunks_map = get_chunks_respecting_line_boundaries(
        mfg_text,
        gpt_model.max_context_tokens - prompt.num_tokens - 5000,
    )
    first_chunk_bounds = min(chunks_map.keys(), key=lambda k: int(k.split(":")[0]))
    first_chunk_text = chunks_map[first_chunk_bounds]
    custom_id = f"{mfg_etld1}>{keyword_label}>chunk{first_chunk_bounds}"

    return GPTBatchRequest(
        batch_id=None,
        request=get_gpt_request_blob(
            created_at=deferred_at,
            custom_id=custom_id,
            context=first_chunk_text,
            prompt=prompt.text,
            gpt_model=gpt_model,
            model_params=model_params,
        ),
    )


async def find_business_desc_using_only_first_chunk(
    mfg_etld1: str,
    mfg_text: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> BusinessDescriptionResult:
    logger.info(f"Finding business desc for {mfg_etld1} using only first chunk...")
    prompt_service = await get_prompt_service()
    prompt = prompt_service.find_business_desc_prompt
    chunks_map = get_chunks_respecting_line_boundaries(
        mfg_text,
        gpt_model.max_context_tokens - prompt.num_tokens - 5000,
    )
    first_chunk_key = min(chunks_map.keys(), key=lambda k: int(k.split(":")[0]))
    first_chunk_text = chunks_map[first_chunk_key]
    gpt_response = await ask_gpt_async(
        first_chunk_text, prompt.text, gpt_model, model_params
    )

    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError("find_business_desc: Empty or invalid response from GPT")

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        json_response = json.loads(gpt_response)
        business_name = json_response.get("name")
        business_desc = json_response.get("description")
    except:
        raise ValueError(
            f"find_business_desc: Invalid response from GPT:{gpt_response}"
        )

    logger.info(f"find_business_desc:`{business_name}`\n`{business_desc}`")

    return BusinessDescriptionResult(name=business_name, description=business_desc)


# LLM's independent search
async def llm_search(
    text: str,
    prompt: str,
    gpt_model: GPTModel,
    model_params: ModelParameters,
    num_passes: int = 1,
) -> set[str]:

    llm_results: set[str] = set()
    for _ in range(num_passes):
        gpt_response = await ask_gpt_async(text, prompt, gpt_model, model_params)

        if not gpt_response:
            logger.error(f"Invalid gpt_response:{gpt_response}")
            raise ValueError("llm_results: Empty or invalid response from GPT")

        try:
            gpt_response = gpt_response.replace("```", "").replace("json", "")
            new_extracted: set[str] = set(json.loads(gpt_response)) - llm_results
        except:
            raise ValueError(f"llm_results: Invalid response from GPT:{gpt_response}")

        logger.debug(
            f"llm_results new_extracted {len(new_extracted)}:{list(new_extracted)}"
        )

        llm_results = llm_results | new_extracted

    logger.debug(f"llm_results:{llm_results}")

    return llm_results


def llm_search_deferred(
    deferred_at: datetime,
    custom_id: str,
    text: str,
    prompt: Prompt,
    gpt_model: GPTModel,
    model_params: ModelParameters,
) -> GPTBatchRequest:
    custom_id = f"llm_search_deferred>{hash(text)}>{hash(prompt)}"
    return GPTBatchRequest(
        batch_id=None,
        request=get_gpt_request_blob(
            created_at=deferred_at,
            custom_id=custom_id,
            context=text,
            prompt=prompt.text,
            gpt_model=gpt_model,
            model_params=model_params,
        ),
    )
