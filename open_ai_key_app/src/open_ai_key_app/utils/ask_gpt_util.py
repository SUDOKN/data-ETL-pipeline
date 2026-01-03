import openai
import re
import asyncio
import logging
import httpx
import time
import uuid

from open_ai_key_app.utils.token_util import num_tokens_from_string
from open_ai_key_app.models.gpt_model import GPTModel, GPT_4o_mini, ModelParameters
from core.models.gpt_batch_request_blob import GPTBatchRequestBlob
from open_ai_key_app.services.openai_keypool_service import keypool

logger = logging.getLogger(__name__)


# --- ask_gpt_async Function ---
# should ideally acquire lock before calling this function
async def ask_gpt_async(
    context: str,
    prompt: str,
    gpt_model: GPTModel,
    model_params: ModelParameters,
):
    # Generate unique request ID for tracking
    request_id = str(uuid.uuid4())[:8]
    start_time = time.time()

    logger.info(f"[Request {request_id}] Starting ask_gpt_async request")

    tokens_prompt = num_tokens_from_string(prompt)
    tokens_context = num_tokens_from_string(context)
    max_response_tokens = (
        model_params.max_tokens
        if model_params.max_tokens
        else gpt_model.safe_completion_tokens
    )
    tokens_needed = tokens_prompt + tokens_context + max_response_tokens

    if tokens_needed > gpt_model.max_context_tokens:
        elapsed_time = time.time() - start_time
        logger.error(
            f"[Request {request_id}] Failed after {elapsed_time:.2f}s: "
            f"Total tokens needed:{tokens_needed} exceed max context tokens:{gpt_model.max_context_tokens}."
        )
        raise ValueError(
            f"Total tokens needed:{tokens_needed} exceed max context tokens:{gpt_model.max_context_tokens}."
        )

    logger.debug(
        f"[Request {request_id}] context tokens: {tokens_context}, prompt tokens: {tokens_prompt}, "
        f"max response tokens: {max_response_tokens}, total tokens needed: {tokens_needed}. "
        f"Attempting to borrow key from keypool."
    )

    key_borrow_time = time.time()
    key_name, api_key, lock_token = await keypool.borrow_key(tokens_needed)
    key_borrow_duration = time.time() - key_borrow_time

    try:
        logger.info(
            f"[Request {request_id}] Borrowed key '{key_name}' in {key_borrow_duration:.2f}s. "
            f"Using model '{gpt_model.model_name}'."
            # f"api_key: {api_key}"
        )

        api_call_start = time.time()
        openai.api_key = api_key
        response = await asyncio.to_thread(
            openai.chat.completions.create,
            model=gpt_model.model_name,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": context},
            ],
            max_completion_tokens=max_response_tokens,
            temperature=model_params.temperature,
            top_p=model_params.top_p,
            presence_penalty=model_params.presence_penalty,
            frequency_penalty=model_params.frequency_penalty,
        )
        api_call_duration = time.time() - api_call_start
        total_duration = time.time() - start_time

        logger.info(
            f"[Request {request_id}] Success! API call took {api_call_duration:.2f}s, "
            f"total request time: {total_duration:.2f}s. "
            f"Received {len(response.choices)} choices from key '{key_name}'."
        )
        keypool.record_key_usage(api_key, tokens_needed)
        return response.choices[0].message.content
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
            keypool.mark_key_exhausted(api_key, error_msg)
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
                keypool.set_key_cooldown(api_key, delay)
            else:
                logger.warning(
                    f"[Request {request_id}] Rate limit hit for key {key_name}. "
                    f"Marking as unavailable for 5s by default."
                )
                keypool.set_key_cooldown(api_key, 5.0)  # Fallback

            # Do not retry, just fail and allow next request to pick a different key
            raise ValueError(f"Rate limit hit for API key: {key_name}.")
        else:
            raise e
    finally:
        logger.debug(f"[Request {request_id}] Returning key '{key_name}' to keypool.")
        keypool.return_key(api_key, lock_token)


# --- send_gpt_batch_request_sync Function ---
async def send_gpt_batch_request_sync(
    batch_request: GPTBatchRequestBlob,
) -> dict:
    """
    Send a GPT batch request synchronously using HTTP client with keypool management.

    Args:
        batch_request: GPTBatchRequestBlob containing the request details

    Returns:
        dict: The JSON response from OpenAI API

    Raises:
        httpx.HTTPStatusError: If the request fails
        ValueError: If quota exceeded or rate limit hit
    """
    # Generate unique request ID for tracking (use first 8 chars of custom_id + unique suffix)
    request_id = f"{batch_request.custom_id[:12]}_{str(uuid.uuid4())[:8]}"
    start_time = time.time()

    tokens_needed = batch_request.body.input_tokens + batch_request.body.max_tokens

    logger.info(
        f"[Request {request_id}] Starting batch request. Needs {tokens_needed} tokens. "
        f"Attempting to borrow key from keypool."
    )

    key_borrow_time = time.time()
    key_name, api_key, lock_token = await keypool.borrow_key(tokens_needed)
    key_borrow_duration = time.time() - key_borrow_time

    try:
        logger.info(
            f"[Request {request_id}] Borrowed key '{key_name}' in {key_borrow_duration:.2f}s."
        )

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        # Prepare the request body (exclude input_tokens as it's just for our tracking)
        request_body = {
            "model": batch_request.body.model,
            "messages": batch_request.body.messages,
            "max_tokens": batch_request.body.max_tokens,
        }

        logger.info(
            f"[Request {request_id}] Sending to model '{batch_request.body.model}' "
            f"with {batch_request.body.input_tokens} input tokens."
        )

        http_call_start = time.time()
        with httpx.Client(timeout=60.0) as client:
            response = client.post(
                f"https://api.openai.com{batch_request.url}",
                headers=headers,
                json=request_body,
            )
            response.raise_for_status()
            result = response.json()

            http_call_duration = time.time() - http_call_start
            total_duration = time.time() - start_time

            logger.info(
                f"[Request {request_id}] Success! HTTP call took {http_call_duration:.2f}s, "
                f"total request time: {total_duration:.2f}s."
            )
            keypool.record_key_usage(api_key, tokens_needed)
            return result

    except httpx.HTTPStatusError as e:
        error_msg = str(e)
        elapsed_time = time.time() - start_time
        logger.error(
            f"[Request {request_id}] HTTP error after {elapsed_time:.2f}s: {error_msg}"
        )

        # Try to parse error response
        try:
            error_data = e.response.json()
            logger.error(f"Error details: {error_data}")

            # Check for quota or rate limit errors in the response
            if "error" in error_data:
                error_message = error_data["error"].get("message", "")
                error_code = error_data["error"].get("code", "")

                if (
                    "quota" in error_message.lower()
                    or error_code == "insufficient_quota"
                ):
                    logger.warning(
                        f"[Request {request_id}] Quota exceeded for API key_name: {key_name}. "
                        f"Removing from pool."
                    )
                    keypool.mark_key_exhausted(api_key, error_message)
                    raise ValueError(f"Quota exceeded for API key: {key_name}.")

                elif (
                    "rate limit" in error_message.lower()
                    or error_code == "rate_limit_exceeded"
                ):
                    match = re.search(r"Please try again in ([\d.]+)s", error_message)
                    if match:
                        delay = float(match.group(1))
                        logger.warning(
                            f"[Request {request_id}] Rate limit hit for key {key_name}. "
                            f"Marking as unavailable for {delay}s."
                        )
                        keypool.set_key_cooldown(api_key, delay)
                    else:
                        logger.warning(
                            f"[Request {request_id}] Rate limit hit for key {key_name}. "
                            f"Marking as unavailable for 5s by default."
                        )
                        keypool.set_key_cooldown(api_key, 5.0)

                    raise ValueError(f"Rate limit hit for API key: {key_name}.")
        except ValueError:
            # Re-raise ValueError from quota/rate limit handling
            raise
        except Exception:
            pass

        raise
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(
            f"[Request {request_id}] Exception after {elapsed_time:.2f}s: {str(e)}"
        )
        raise
    finally:
        logger.debug(f"[Request {request_id}] Returning key '{key_name}' to keypool.")
        keypool.return_key(api_key, lock_token)
