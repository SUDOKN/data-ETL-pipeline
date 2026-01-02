from datetime import datetime
import json
import logging
from typing import Optional

from core.models.prompt import Prompt

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.utils.str_util import make_json_array_parse_safe
from core.services.gpt_batch_request_service import create_base_gpt_batch_request
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
        logger.error(
            f"parse_llm_search_response: Invalid gpt_response:{gpt_response}, returning empty set"
        )
        return set()

    try:
        cleaned_response = make_json_array_parse_safe(gpt_response)
    except Exception as e:
        logger.error(
            (
                f"parse_llm_search_response: Failed to make_json_parse_safe GPT response: {e}\n",
                f"cleaned_response={gpt_response}, returning empty set",
            ),
            exc_info=True,
        )
        return set()

    try:
        llm_results: set[str] = set(json.loads(cleaned_response))
    except Exception as e:
        logger.error(
            (
                f"parse_llm_search_response: Failed to json.loads(cleaned_response): {e}\n",
                f"gpt_response={gpt_response}\n"
                f"cleaned_response={cleaned_response}, returning empty set",
            ),
            exc_info=True,
        )
        return set()

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

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        custom_id=custom_id,
        context=text,
        prompt=prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )

    return gpt_batch_request
