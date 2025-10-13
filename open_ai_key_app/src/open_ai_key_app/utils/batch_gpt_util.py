from datetime import datetime
import re
import asyncio
import logging
import tiktoken
from openai import AsyncOpenAI

from open_ai_key_app.utils.token_util import num_tokens_from_string
from open_ai_key_app.models.gpt_model import GPTModel, GPT_4o_mini, ModelParameters
from open_ai_key_app.models.gpt_batch_request_blob import (
    GPTBatchRequestBlob,
    GPTBatchRequestBlobBody,
)
from open_ai_key_app.services.openai_keypool_service import keypool

logger = logging.getLogger(__name__)


def get_gpt_request_blob(
    created_at: datetime,
    custom_id: str,
    context: str,
    prompt: str,
    gpt_model: GPTModel,
    model_params: ModelParameters,
) -> GPTBatchRequestBlob:
    logger.info(f"get_gpt_request_blob: Generating request blob for {custom_id}")
    tokens_prompt = num_tokens_from_string(prompt)
    tokens_context = num_tokens_from_string(context)
    max_response_tokens = (
        model_params.max_tokens
        if model_params.max_tokens
        else gpt_model.safe_completion_tokens
    )
    input_tokens = tokens_prompt + tokens_context
    tokens_needed = input_tokens + max_response_tokens

    if tokens_needed > gpt_model.max_context_tokens:
        raise ValueError(
            f"Total tokens needed:{tokens_needed} exceed max context tokens:{gpt_model.max_context_tokens}."
        )

    return GPTBatchRequestBlob(
        created_at=created_at,
        custom_id=custom_id,
        body=GPTBatchRequestBlobBody(
            model=gpt_model.model_name,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": context},
            ],
            input_tokens=input_tokens,
            max_tokens=max_response_tokens,
        ),
    )
