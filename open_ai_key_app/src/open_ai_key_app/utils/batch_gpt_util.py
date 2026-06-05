import asyncio
import logging
import litellm

from core.models.gpt_batch_request_blob import GPTBatchRequestBlob
from litellm_proxy_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import (
    GPTRequestBody,
    GPTModelParams,
    GPTModelParams,
)

from open_ai_key_app.utils.token_util import num_tokens_from_string

logger = logging.getLogger(__name__)


async def get_gpt_request_blob_async(
    custom_id: str,
    context: str,
    prompt: str,
    gpt_model: LLM_Model,
    model_params: GPTModelParams,
) -> GPTBatchRequestBlob:
    """
    Async version that runs CPU-intensive operations in thread pool.

    Tokenization is the main bottleneck (~70ms per call), so we run it
    in a thread pool to avoid blocking the event loop and allow other
    manufacturers to process concurrently.
    """
    logger.info(f"Generating GPT request blob for {custom_id}.")
    loop = asyncio.get_event_loop()

    # Run synchronous version in thread pool
    return await loop.run_in_executor(
        None,
        get_gpt_request_blob,
        custom_id,
        context,
        prompt,
        gpt_model,
        model_params,
    )


def get_gpt_request_blob(
    custom_id: str,
    context: str,
    prompt: str,
    gpt_model: LLM_Model,
    model_params: GPTModelParams,
) -> GPTBatchRequestBlob:
    """
    Synchronous version - called from thread pool in async context.

    Creates a GPT batch request blob with token counting and validation.
    """
    tokens_prompt = litellm.token_counter(model=gpt_model.model_name, text=prompt)
    tokens_context = litellm.token_counter(model=gpt_model.model_name, text=context)
    input_tokens = tokens_prompt + tokens_context
    tokens_needed = input_tokens + model_params.max_completion_tokens

    if tokens_needed > gpt_model.max_context_tokens:
        raise ValueError(
            f"Total tokens needed:{tokens_needed}=prompt:{tokens_prompt}+context:{tokens_context}+safe_completion:{model_params.max_completion_tokens} exceed max context tokens:{gpt_model.max_context_tokens}."
        )

    blob = GPTBatchRequestBlob(
        custom_id=custom_id,
        body=GPTRequestBody.model_validate(
            model_params.model_dump()
            | {
                "model": gpt_model.model_name,
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": context},
                ],
            }
        ),
        input_tokens=input_tokens,
    )

    return blob
