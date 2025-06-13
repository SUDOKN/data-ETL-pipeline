import json

from open_ai_key_app.src.utils.ask_gpt import (
    num_tokens_from_string,
    ask_gpt_async,
)
from open_ai_key_app.src.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)
from models.db.binary_classifier_result import (
    BinaryClassifierResult,
)


async def binary_classifier(
    keyword_label: str,
    manufacturer_url: str,
    text: str,
    binary_prompt: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
    debug: bool = False,
) -> BinaryClassifierResult:
    text_tokens = num_tokens_from_string(text)
    if debug:
        print(f"text_tokens: {text_tokens}")

    gpt_response = await ask_gpt_async(text, binary_prompt, gpt_model, model_params)

    if not gpt_response:
        print(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            f"{manufacturer_url}:{keyword_label} llm_results: Empty or invalid response from GPT"
        )

    if debug:
        print(f"classification gpt response:\n{gpt_response}")

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        classification_result: BinaryClassifierResult = json.loads(gpt_response)
    except:
        raise ValueError(
            f"{manufacturer_url}:{keyword_label} binary_classifier: non-json result from GPT:{gpt_response}"
        )

    return classification_result
