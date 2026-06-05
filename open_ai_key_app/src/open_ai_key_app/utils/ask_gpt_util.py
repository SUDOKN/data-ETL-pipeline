import litellm
import openai
import re
import asyncio
import logging
import time
import uuid
from typing import Optional
from openai.types.chat import ChatCompletion

from core.models.gpt_batch_response_blob import GPTBatchResponse
from litellm_proxy_app.models.llm_model import LLM_Model
from litellm_proxy_app.models.llm_model_params import LLMModelParams
from open_ai_key_app.models.gpt_model_params import GPTModelParams, GPTSyncRequestBody

from open_ai_key_app.services.openai_keypool_service import keypool
from data_etl_app.utils.gpt_batch_request_util import (
    build_response_from_chat_completion,
)

from data_etl_app.utils.gpt_batch_request_util import (
    build_response_from_chat_completion,
)
from litellm_proxy_app.utils.ask_llm_util import fetch_llm_chat_completion_result

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


async def fetch_gpt_chat_completion_result(
    context: str,
    prompt: str,
    gpt_model: LLM_Model,
    model_params: GPTModelParams,
) -> ChatCompletion:
    # Generate unique request ID for tracking
    request_id = str(uuid.uuid4())[:8]
    start_time = time.time()

    logger.info(
        f"[Request {request_id}] Starting fetch_gpt_chat_completion_result request"
    )

    tokens_prompt = litellm.token_counter(model=gpt_model.model_name, text=prompt)
    tokens_context = litellm.token_counter(model=gpt_model.model_name, text=context)
    tokens_needed = tokens_prompt + tokens_context + model_params.max_completion_tokens

    if tokens_needed > gpt_model.max_context_tokens:
        elapsed_time = time.time() - start_time
        logger.error(
            f"[Request {request_id}] Failed after {elapsed_time:.2f}s: "
            f"Total tokens needed:{tokens_needed} exceed max context tokens:{gpt_model.max_context_tokens}."
        )
        raise ValueError(
            f"Total tokens needed:{tokens_needed} exceed max context tokens:{gpt_model.max_context_tokens}."
        )

    logger.info(
        f"[Request {request_id}] context tokens: {tokens_context}, prompt tokens: {tokens_prompt}, "
        f"max response tokens: {model_params.max_completion_tokens}, total tokens needed: {tokens_needed}. "
        f"Attempting to borrow key from keypool."
    )

    key_borrow_time = time.time()
    key_name, api_key, lock_token = await keypool.borrow_key(
        tokens_needed, gpt_model.model_name
    )
    key_borrow_duration = time.time() - key_borrow_time

    try:
        logger.info(
            f"[Request {request_id}] Borrowed key '{key_name}' in {key_borrow_duration:.2f}s. "
            f"Using model '{gpt_model.model_name}'."
            # f"api_key: {api_key}"
        )

        api_call_start = time.time()
        openai.api_key = api_key

        sync_body = GPTSyncRequestBody(
            model=gpt_model.model_name,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"{context}"},
            ],
            max_completion_tokens=model_params.max_completion_tokens,
            temperature=model_params.temperature,
            top_p=model_params.top_p,
            presence_penalty=model_params.presence_penalty,
            frequency_penalty=model_params.frequency_penalty,
            seed=model_params.seed,
        )
        response = await asyncio.to_thread(
            openai.chat.completions.create,
            **sync_body.model_dump(exclude_none=True),
        )
        api_call_duration = time.time() - api_call_start
        total_duration = time.time() - start_time

        logger.info(
            f"[Request {request_id}] Success! API call took {api_call_duration:.2f}s, "
            f"total request time: {total_duration:.2f}s. "
            f"Received {len(response.choices)} choices from key '{key_name}'."
        )
        keypool.record_key_usage(api_key, tokens_needed, gpt_model.model_name)
        return response
    except Exception as e:
        # some errors look like
        # Error code: 429 - {'error': {'message': 'Rate limit reached for gpt-4o-mini in organization org-M5dkpWKwz4bw95SV04FgKdYV on tokens per min (TPM): Limit 200000, Used 130491, Requested 75418. Please try again in 1.772s. Visit https://platform.openai.com/account/rate-limits to learn more.', 'type': 'tokens', 'param': None, 'code': 'rate_limit_exceeded'}}
        # Error code: 429 - {'error': {'message': 'You exceeded your current quota, please check your plan and billing details. For more information on this error, read the docs: https://platform.openai.com/docs/guides/error-codes/api-errors.', 'type': 'insufficient_quota', 'param': None, 'code': 'insufficient_quota'}}
        # check if the error is due to quota exceeded
        error_msg = str(e)
        elapsed_time = time.time() - start_time
        logger.error(
            f"[Request {request_id}] Exception after {elapsed_time:.2f}s: {error_msg}"
        )
        if "You exceeded your current quota" in error_msg:
            logger.warning(
                f"[Request {request_id}] Quota exceeded for API key_name: {key_name}. Removing from pool."
            )
            keypool.mark_key_exhausted(api_key, error_msg, gpt_model.model_name)
            raise ValueError(f"Quota exceeded for API key: {key_name}.")

        # Handle rate limiting with suggested retry delay
        elif "rate limit reached" in error_msg or "Rate limit" in error_msg:
            match = re.search(r"Please try again in ([\d.]+)s", error_msg)
            if match:
                delay = float(match.group(1))
                logger.warning(
                    f"[Request {request_id}] Rate limit hit for key {key_name}. "
                    f"Marking as unavailable for {delay}s."
                )
                keypool.set_key_cooldown(api_key, delay, gpt_model.model_name)
            else:
                logger.warning(
                    f"[Request {request_id}] Rate limit hit for key {key_name}. "
                    f"Marking as unavailable for 5s by default."
                )
                keypool.set_key_cooldown(api_key, 5.0, gpt_model.model_name)  # Fallback

            # Do not retry, just fail and allow next request to pick a different key
            raise ValueError(f"Rate limit hit for API key: {key_name}.")
        else:
            raise e
    finally:
        logger.debug(f"[Request {request_id}] Returning key '{key_name}' to keypool.")
        keypool.return_key(api_key, lock_token, gpt_model.model_name)
