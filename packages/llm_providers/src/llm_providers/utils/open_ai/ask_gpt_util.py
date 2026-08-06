import logging
from typing import Optional

from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from llm_providers.models.llm_model import LLM_Model
from llm_providers.models.llm_model_params import (
    LLMModelParams,
)
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams


from llm_providers.utils.open_ai.gpt_batch_request_util import (
    build_response_from_chat_completion,
)
from llm_providers.utils.ask_llm_util import fetch_llm_chat_completion_result

logger = logging.getLogger(__name__)


async def ask_gpt(
    context: str,
    prompt: str,
    gpt_model: LLM_Model,
    model_params: LLMModelParams,
) -> Optional[str]:
    response = await fetch_llm_chat_completion_result(
        context=context,
        prompt=prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )
    return response.choices[0].message.content


async def fetch_gpt_batch_response(
    context: str,
    prompt: str,
    custom_id: str,
    batch_id: Optional[str],
    gpt_model: LLM_Model,
    model_params: GPTModelParams,
) -> GPTBatchResponse:
    if not batch_id:
        raise ValueError("batch_id must be provided for fetch_gpt_batch_response.")

    response = await fetch_llm_chat_completion_result(
        context=context,
        prompt=prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )

    response_blob = build_response_from_chat_completion(
        chat_completion_result=response,
        custom_id=custom_id,
        batch_id=batch_id,
    )

    return response_blob
