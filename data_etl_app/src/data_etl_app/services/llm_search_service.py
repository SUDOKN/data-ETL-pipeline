import json

from open_ai_key_app.models.gpt_model import (
    GPTModel,
    ModelParameters,
)
from open_ai_key_app.utils.ask_gpt_util import (
    ask_gpt_async,
)


# LLM's independent search
async def llm_search(
    text: str,
    prompt: str,
    gpt_model: GPTModel,
    model_params: ModelParameters,
    num_passes: int = 1,
    debug: bool = False,
) -> set[str]:
    # print(f'prompt:{prompt}')
    llm_results: set[str] = set()
    for _ in range(num_passes):
        gpt_response = await ask_gpt_async(text, prompt, gpt_model, model_params)

        # if debug:
        #     print(f"llm_search gpt_response:{gpt_response}")

        if not gpt_response:
            print(f"Invalid gpt_response:{gpt_response}")
            raise ValueError("llm_results: Empty or invalid response from GPT")

        try:
            gpt_response = gpt_response.replace("```", "").replace("json", "")
            new_extracted: set[str] = set(json.loads(gpt_response)) - llm_results
        except:
            raise ValueError(f"llm_results: Invalid response from GPT:{gpt_response}")

        if debug:
            print(
                f"llm_results new_extracted {len(new_extracted)}:{list(new_extracted)}"
            )

        llm_results = llm_results | new_extracted

    # print(f'llm_results:{llm_results}')

    return llm_results
