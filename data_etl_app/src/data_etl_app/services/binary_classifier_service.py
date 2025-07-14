from datetime import datetime
import json

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
from shared.models.db.manufacturer import (
    BinaryClassifierResult,
)

from data_etl_app.services.prompt_service import prompt_service


async def is_product_manufacturer(
    timestamp: datetime, manufacturer_url: str, text: str, debug: bool = False
) -> BinaryClassifierResult:
    if debug:
        print(f"Checking if {manufacturer_url} is a product manufacturer...")
    result = await _binary_classify(
        evaluated_at=timestamp,
        keyword_label="is_product_manufacturer",
        manufacturer_url=manufacturer_url,
        text=text,
        binary_prompt=prompt_service.is_product_manufacturer_prompt,
        debug=debug,
    )
    return result


async def is_contract_manufacturer(
    timestamp: datetime, manufacturer_url: str, text: str, debug: bool = False
) -> BinaryClassifierResult:
    if debug:
        print(f"Checking if {manufacturer_url} is a contract manufacturer...")
    result = await _binary_classify(
        evaluated_at=timestamp,
        keyword_label="is_contract_manufacturer",
        manufacturer_url=manufacturer_url,
        text=text,
        binary_prompt=prompt_service.is_contract_manufacturer_prompt,
        debug=debug,
    )
    return result


async def _binary_classify(
    evaluated_at: datetime,
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

    gpt_response = await ask_gpt_async(
        text, binary_prompt, gpt_model, model_params, debug=debug
    )

    if not gpt_response:
        print(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            f"{manufacturer_url}:{keyword_label} _binary_classifier: Empty or invalid response from GPT"
        )

    if debug:
        print(f"classification gpt response:\n{gpt_response}")

    gpt_response = gpt_response.replace("```", "").replace("json", "")

    return BinaryClassifierResult(evaluated_at=evaluated_at, **json.loads(gpt_response))


# special case of binary classification as the llm also returns name
async def is_manufacturer(
    evaluated_at: datetime,
    keyword_label: str,
    manufacturer_url: str,
    text: str,
    binary_prompt: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
    debug: bool = False,
) -> tuple[str, BinaryClassifierResult]:
    text_tokens = num_tokens_from_string(text)
    if debug:
        print(f"text_tokens: {text_tokens}")

    gpt_response = await ask_gpt_async(
        text, binary_prompt, gpt_model, model_params, debug=debug
    )

    if not gpt_response:
        print(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            f"{manufacturer_url}:{keyword_label} llm_results: Empty or invalid response from GPT"
        )

    if debug:
        print(f"classification gpt response:\n{gpt_response}")

    gpt_response = gpt_response.replace("```", "").replace("json", "")
    result = json.loads(gpt_response)
    if not result or not result["name"]:
        raise ValueError(
            f"{manufacturer_url}:{keyword_label} llm_results: Empty or manufacturer name missing in response from GPT"
        )

    return result["name"], BinaryClassifierResult(evaluated_at=evaluated_at, **result)
