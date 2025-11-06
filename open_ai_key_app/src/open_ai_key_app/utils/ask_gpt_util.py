import openai
import re
import asyncio
import logging
import httpx

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
    tokens_prompt = num_tokens_from_string(prompt)
    tokens_context = num_tokens_from_string(context)
    max_response_tokens = (
        model_params.max_tokens
        if model_params.max_tokens
        else gpt_model.safe_completion_tokens
    )
    tokens_needed = tokens_prompt + tokens_context + max_response_tokens

    if tokens_needed > gpt_model.max_context_tokens:
        raise ValueError(
            f"Total tokens needed:{tokens_needed} exceed max context tokens:{gpt_model.max_context_tokens}."
        )

    logger.debug(
        f"ask_gpt_async: context tokens: {tokens_context}, prompt tokens: {tokens_prompt}, "
        f"max response tokens: {max_response_tokens}, total tokens needed: {tokens_needed}. "
        f"\nAttempting to borrow key from keypool with {tokens_needed} tokens."
    )

    key_name, api_key, lock_token = await keypool.borrow_key(tokens_needed)
    try:
        logger.debug(
            f"ask_gpt_async: Using API key '{key_name}' for model '{gpt_model.model_name}'."
        )
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
        logger.debug(
            f"ask_gpt_async: Received response for key '{key_name}' with {len(response.choices)} choices."
        )
        keypool.record_key_usage(api_key, tokens_needed)
        return response.choices[0].message.content
    except Exception as e:
        # some errors look like
        # Error code: 429 - {'error': {'message': 'Rate limit reached for gpt-4o-mini in organization org-M5dkpWKwz4bw95SV04FgKdYV on tokens per min (TPM): Limit 200000, Used 130491, Requested 75418. Please try again in 1.772s. Visit https://platform.openai.com/account/rate-limits to learn more.', 'type': 'tokens', 'param': None, 'code': 'rate_limit_exceeded'}}
        # Error code: 429 - {'error': {'message': 'You exceeded your current quota, please check your plan and billing details. For more information on this error, read the docs: https://platform.openai.com/docs/guides/error-codes/api-errors.', 'type': 'insufficient_quota', 'param': None, 'code': 'insufficient_quota'}}
        # check if the error is due to quota exceeded
        error_msg = str(e)
        logger.error(f"ask_gpt_async exception occurred: {error_msg}")
        if "You exceeded your current quota" in error_msg:
            logger.warning(
                f"Quota exceeded for API key_name: {key_name}. Removing from pool."
            )
            keypool.mark_key_exhausted(api_key, error_msg)
            raise ValueError(f"Quota exceeded for API key: {key_name}.")

        # Handle rate limiting with suggested retry delay
        elif "rate limit reached" in error_msg or "Rate limit" in error_msg:
            match = re.search(r"Please try again in ([\d.]+)s", error_msg)
            if match:
                delay = float(match.group(1))
                logger.warning(
                    f"Rate limit hit for key {key_name}. Marking as unavailable for {delay}s."
                )
                keypool.set_key_cooldown(api_key, delay)
            else:
                logger.warning(
                    f"Rate limit hit for key {key_name}. Marking as unavailable for 5s by default."
                )
                keypool.set_key_cooldown(api_key, 5.0)  # Fallback

            # Do not retry, just fail and allow next request to pick a different key
            raise ValueError(f"Rate limit hit for API key: {key_name}.")
        else:
            raise e
    finally:
        logger.debug(f"ask_gpt_async: Returning key '{key_name}' to keypool.")
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
    tokens_needed = batch_request.body.input_tokens + batch_request.body.max_tokens

    logger.debug(
        f"send_gpt_batch_request_sync: Request '{batch_request.custom_id}' needs {tokens_needed} tokens. "
        f"Attempting to borrow key from keypool."
    )

    key_name, api_key, lock_token = await keypool.borrow_key(tokens_needed)

    try:
        logger.info(
            f"send_gpt_batch_request_sync: Using API key '{key_name}' for request '{batch_request.custom_id}'."
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
            f"send_gpt_batch_request_sync: Sending request '{batch_request.custom_id}' "
            f"to model '{batch_request.body.model}' with {batch_request.body.input_tokens} input tokens."
        )

        with httpx.Client(timeout=60.0) as client:
            response = client.post(
                f"https://api.openai.com{batch_request.url}",
                headers=headers,
                json=request_body,
            )
            response.raise_for_status()
            result = response.json()

            logger.debug(
                f"send_gpt_batch_request_sync: Received response for request '{batch_request.custom_id}'."
            )
            keypool.record_key_usage(api_key, tokens_needed)
            return result

    except httpx.HTTPStatusError as e:
        error_msg = str(e)
        logger.error(f"send_gpt_batch_request_sync HTTP error: {error_msg}")

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
                        f"Quota exceeded for API key_name: {key_name}. Removing from pool."
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
                            f"Rate limit hit for key {key_name}. Marking as unavailable for {delay}s."
                        )
                        keypool.set_key_cooldown(api_key, delay)
                    else:
                        logger.warning(
                            f"Rate limit hit for key {key_name}. Marking as unavailable for 5s by default."
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
        logger.error(f"send_gpt_batch_request_sync exception occurred: {str(e)}")
        raise
    finally:
        logger.debug(
            f"send_gpt_batch_request_sync: Returning key '{key_name}' to keypool."
        )
        keypool.return_key(api_key, lock_token)
