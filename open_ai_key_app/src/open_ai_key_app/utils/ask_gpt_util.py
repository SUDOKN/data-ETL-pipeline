import re
import asyncio
import logging

from openai import AsyncOpenAI

from open_ai_key_app.utils.token_util import num_tokens_from_string
from open_ai_key_app.models.gpt_model import GPTModel, ModelParameters
from open_ai_key_app.services.openai_keypool_service import keypool

logger = logging.getLogger(__name__)


# --- ask_gpt_async Function ---
# should ideally acquire lock before calling this function
async def ask_gpt_async(
    context: str,
    prompt: str,
    gpt_model: GPTModel,
    model_params: ModelParameters,
) -> str | None:
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
    accounted = False
    request_maybe_sent = False
    try:
        logger.debug(
            f"ask_gpt_async: Using API key '{key_name}' for model '{gpt_model.model_name}'."
        )
        # Use async client so cancellation aborts the HTTP request
        client = AsyncOpenAI(api_key=api_key, timeout=30.0)
        request_maybe_sent = True
        response = await client.chat.completions.create(
            model=gpt_model.model_name,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": context},
            ],
            max_tokens=max_response_tokens,
            temperature=model_params.temperature,
            top_p=model_params.top_p,
            presence_penalty=model_params.presence_penalty,
            frequency_penalty=model_params.frequency_penalty,
        )
        # Prefer exact billing when available
        used_tokens = None
        try:
            if hasattr(response, "usage") and response.usage is not None:
                # OpenAI returns usage with prompt_tokens, completion_tokens, total_tokens
                used_tokens = getattr(response.usage, "total_tokens", None)
        except Exception:
            used_tokens = None
        if used_tokens is None:
            used_tokens = tokens_needed

        keypool.record_key_usage(api_key, used_tokens)
        accounted = True
        logger.debug(
            f"ask_gpt_async: Received response for key '{key_name}' with {len(response.choices)} choices."
        )
        return response.choices[0].message.content
    except asyncio.CancelledError:
        # Peer task failed; this coroutine was cancelled while the background request keeps running.
        # Conservatively account tokens to avoid undercounting.
        if request_maybe_sent and not accounted:
            logger.info(
                f"ask_gpt_async: Cancelled while using key '{key_name}'. Conservatively recording usage: {tokens_needed} tokens."
            )
            keypool.record_key_usage(api_key, tokens_needed)
            accounted = True
        raise
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
            # Unknown error: if the request was likely dispatched, conservatively account
            if request_maybe_sent and not accounted:
                logger.warning(
                    f"ask_gpt_async: Unknown error after dispatch for key '{key_name}'. Conservatively recording usage: {tokens_needed} tokens."
                )
                keypool.record_key_usage(api_key, tokens_needed)
                accounted = True
            raise e
    finally:
        logger.debug(f"ask_gpt_async: Returning key '{key_name}' to keypool.")
        keypool.return_key(api_key, lock_token)
