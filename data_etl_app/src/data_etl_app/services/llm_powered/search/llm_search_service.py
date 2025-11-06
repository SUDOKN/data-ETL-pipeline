from datetime import datetime
import json
import logging
from typing import Optional

from core.models.prompt import Prompt

from core.models.db.gpt_batch_request import GPTBatchRequest
from open_ai_key_app.models.gpt_model import (
    GPTModel,
    ModelParameters,
)
from open_ai_key_app.utils.ask_gpt_util import (
    ask_gpt_async,
)
from open_ai_key_app.utils.batch_gpt_util import (
    get_gpt_request_blob,
)

logger = logging.getLogger(__name__)


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

        new_extracted: set[str] = parse_llm_search_response(gpt_response) - llm_results

        logger.debug(
            f"llm_results new_extracted {len(new_extracted)}:{list(new_extracted)}"
        )

        llm_results = llm_results | new_extracted

    logger.debug(f"llm_results:{llm_results}")

    return llm_results


def parse_llm_search_response(gpt_response: str) -> set[str]:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError("llm_results: Empty or invalid response from GPT")

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        llm_results: set[str] = set(json.loads(gpt_response))
    except:
        raise ValueError(f"llm_results: Invalid response from GPT:{gpt_response}")

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
    """
    Async version that runs CPU-intensive batch request generation in thread pool.

    This allows the event loop to yield control during tokenization, enabling
    concurrent processing of multiple manufacturers.
    """
    request_blob = get_gpt_request_blob(
        custom_id=custom_id,
        context=text,
        prompt=prompt.text,
        gpt_model=gpt_model,
        model_params=model_params,
    )

    gpt_batch_request = GPTBatchRequest(
        created_at=deferred_at,
        batch_id=None,
        request=request_blob,
    )

    return gpt_batch_request
