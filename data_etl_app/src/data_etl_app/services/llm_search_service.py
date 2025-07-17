import json
import logging

from open_ai_key_app.models.gpt_model import (
    GPTModel,
    ModelParameters,
)
from open_ai_key_app.utils.ask_gpt_util import (
    ask_gpt_async,
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
